# SPDX-License-Identifier: Apache-2.0
"""Management-IP failover performance spike (Phase 0 exit measurements).

Runs INSIDE the adapter container (so it reaches ``nso:8080`` + Vault and resolves
real NSO/NetBox creds via the adapter's own secrets provider — no secret ever passes
through the caller). It prints only timings + device names.

What it measures (the numbers that drive Phase-1 staggering, see the plan §0.7):

* reachable ``connect`` latency on a real, healthy device (the cheap steady-state probe);
* unreachable ``connect`` latency — both the adapter-capped cost (httpx ``failover_probe_timeout``,
  what the scheduler actually waits) and NSO's *true* connect-timeout (long timeout, the worst case);
* a full flip cycle (``set_address`` → ``disconnect`` → ``connect`` → revert);
* micro-op latency (``set_address`` / ``disconnect`` / ``get_address``);
* a REAL primary→OOB→primary failover of NetBox device 15 (always reverted in a ``finally``).

Safety: a throwaway device ``failover-spike-temp`` (TEST-NET 192.0.2.1) is created and deleted
for the unreachable/flip-mechanics timings; device 15's original address is captured up front and
restored unconditionally. ``connect`` is non-mutating to device config — only NSO's stored address
is touched, and only transiently.
"""

from __future__ import annotations

import asyncio
import statistics
import time

import httpx

from nso_adapter.config import get_config, get_env_settings
from nso_adapter.nso import actions
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider

NETBOX_DEVICE_ID = 15
TEMP_DEVICE = "failover-spike-temp"
TEMP_ADDR = "192.0.2.1"  # TEST-NET-1 (RFC 5737) — guaranteed unroutable
BOGUS_ADDR = "192.0.2.254"  # second unroutable addr, for the device-15 true-unreachable sample


def _host(cidr: str | None) -> str | None:
    return cidr.split("/")[0] if cidr else None


def _pct(samples: list[float], q: float) -> float:
    """Nearest-rank percentile (small-N friendly): q in [0,1]."""
    if not samples:
        return float("nan")
    s = sorted(samples)
    idx = min(len(s) - 1, max(0, round(q * (len(s) - 1))))
    return s[idx]


def _stat(label: str, samples: list[float]) -> str:
    if not samples:
        return f"  {label:<34} (no samples)"
    return (
        f"  {label:<34} n={len(samples)}  "
        f"min={min(samples):6.3f}s  median={statistics.median(samples):6.3f}s  "
        f"p95={_pct(samples, 0.95):6.3f}s  max={max(samples):6.3f}s"
    )


async def _timed(coro) -> tuple[float, bool, str]:
    """Await *coro*, return (elapsed_s, ok, detail). Never raises."""
    t0 = time.perf_counter()
    try:
        await coro
        return time.perf_counter() - t0, True, ""
    except Exception as exc:  # noqa: BLE001 — spike measures failures too
        return time.perf_counter() - t0, False, repr(exc)


async def _lookup_device15(cfg, provider) -> tuple[str | None, str | None]:
    """Resolve NetBox device 15 → (primary_host, oob_host) via the adapter's NetBox creds."""
    token = provider.get(cfg.netbox.api_token_ref)
    base = cfg.netbox.base_url.rstrip("/")
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(
            f"{base}/api/dcim/devices/{NETBOX_DEVICE_ID}/",
            headers={"Authorization": f"Token {token}"},
        )
        r.raise_for_status()
        d = r.json()
    primary = _host((d.get("primary_ip") or {}).get("address"))
    oob = _host((d.get("oob_ip") or {}).get("address"))
    print(f"[netbox] device {NETBOX_DEVICE_ID}: name={d.get('name')!r} primary={primary} oob={oob}")
    return primary, oob


def _match_nso_device(devices: list[dict], primary: str | None) -> dict | None:
    for d in devices:
        if primary and d.get("address") == primary:
            return d
    return None


def _ned_of(dev: dict) -> tuple[str, str]:
    """Return (ned_type, ned_id) from a list_devices device-type dict."""
    dt = dev.get("device-type") or {}
    for ned_type in ("cli", "netconf", "generic"):
        if ned_type in dt:
            return ned_type, dt[ned_type].get("ned-id", "")
    return "cli", ""


async def _delete_device(client: NsoClient, name: str) -> None:
    url = f"{client._base}/restconf/data/tailf-ncs:devices/device={name}"
    async with client._client() as c:
        resp = await c.delete(url)
        if resp.status_code not in (204, 404):
            resp.raise_for_status()


async def measure_reachable(client: NsoClient, name: str, timeout: float, n: int) -> list[float]:
    out: list[float] = []
    for _ in range(n):
        reachable, detail, elapsed = await actions.probe_reachable(client, name, timeout)
        out.append(elapsed)
        print(f"    reachable-connect {name}: {elapsed:.3f}s reachable={reachable}")
    return out


async def measure_unreachable(client: NsoClient, name: str, timeout: float, n: int, tag: str) -> list[float]:
    out: list[float] = []
    for _ in range(n):
        reachable, detail, elapsed = await actions.probe_reachable(client, name, timeout)
        out.append(elapsed)
        print(f"    {tag} {name} (timeout={timeout}s): {elapsed:.3f}s reachable={reachable}")
    return out


async def run() -> None:
    cfg = get_config()
    env = get_env_settings()
    provider = make_provider(cfg, env)
    inst = cfg.nso_instances[0]
    client = NsoClient(inst, provider.get(inst.username_ref), provider.get(inst.password_ref))
    probe_timeout = cfg.scheduler.failover_probe_timeout
    print(f"[cfg] instance={inst.name} base_url={inst.base_url} failover_probe_timeout={probe_timeout}s")

    primary, oob = await _lookup_device15(cfg, provider)
    devices = await client.list_devices()
    print(f"[nso] {len(devices)} devices in inventory")
    target = _match_nso_device(devices, primary)
    if target is None:
        print(f"[warn] no NSO device with address={primary}; inventory addresses:")
        for d in devices:
            print(f"        {d.get('name')} -> {d.get('address')}")
        print("[abort] cannot resolve device 15's NSO device; skipping device-bound measurements")
    else:
        print(f"[nso] device 15 → NSO device {target.get('name')!r} @ {target.get('address')}")

    results: dict[str, list[float]] = {}

    # ── Throwaway device: unreachable-connect + flip mechanics (no real device touched).
    ned_type, ned_id = _ned_of(target) if target else ("cli", "")
    authgroup = (target or {}).get("authgroup", "default")
    print(f"\n[temp] creating {TEMP_DEVICE} @ {TEMP_ADDR} (ned_type={ned_type} ned_id={ned_id} authgroup={authgroup})")
    temp_created = False
    try:
        await client.create_device(TEMP_DEVICE, TEMP_ADDR, ned_id, authgroup, ned_type=ned_type)
        temp_created = True
        await client.set_admin_state(TEMP_DEVICE, "unlocked")

        print("[temp] unreachable connect, adapter-capped (failover_probe_timeout):")
        results["unreach_capped_temp"] = await measure_unreachable(
            client, TEMP_DEVICE, probe_timeout, 3, "unreach-capped"
        )
        print("[temp] unreachable connect, NSO true timeout (long client timeout):")
        results["unreach_true_temp"] = await measure_unreachable(client, TEMP_DEVICE, 40.0, 1, "unreach-true")

        # Micro-ops on the temp device (CDB-local; representative regardless of device).
        e, _, _ = await _timed(client.set_address(TEMP_DEVICE, TEMP_ADDR))
        results.setdefault("op_set_address", []).append(e)
        e, _, _ = await _timed(client.disconnect(TEMP_DEVICE))
        results.setdefault("op_disconnect", []).append(e)
        e, _, _ = await _timed(client.get_address(TEMP_DEVICE))
        results.setdefault("op_get_address", []).append(e)
        print(_stat("op set_address", results["op_set_address"]))
        print(_stat("op disconnect", results["op_disconnect"]))
        print(_stat("op get_address", results["op_get_address"]))
    finally:
        if temp_created:
            await _delete_device(client, TEMP_DEVICE)
            print(f"[temp] deleted {TEMP_DEVICE}")

    if target is None:
        _report(results, probe_timeout)
        return

    name = target["name"]
    orig_addr = await client.get_address(name)
    print(f"\n[dev15] NSO device {name!r} original address = {orig_addr}")

    try:
        # Reachable connect on the live primary (cheap steady-state probe).
        print("[dev15] reachable connect on primary:")
        results["reach_connect_primary"] = await measure_reachable(client, name, probe_timeout, 5)

        # REAL failover: primary → OOB → connect (measures the full flip + OOB connect), then back. 2 round-trips.
        if oob:
            flip_samples: list[float] = []
            for i in range(2):
                t0 = time.perf_counter()
                await client.set_address(name, oob)
                await client.disconnect(name)
                reachable, detail, _ = await actions.probe_reachable(client, name, probe_timeout)
                flip = time.perf_counter() - t0
                flip_samples.append(flip)
                print(f"    [dev15] flip→OOB #{i + 1}: {flip:.3f}s reachable_on_oob={reachable}")
                # back to primary
                t0 = time.perf_counter()
                await client.set_address(name, orig_addr)
                await client.disconnect(name)
                reachable, detail, _ = await actions.probe_reachable(client, name, probe_timeout)
                back = time.perf_counter() - t0
                flip_samples.append(back)
                print(f"    [dev15] flip→primary #{i + 1}: {back:.3f}s reachable_on_primary={reachable}")
            results["flip_cycle"] = flip_samples
        else:
            print("[dev15] no OOB IP on device 15 — skipping real failover flip")

        # True worst-case unreachable on a TRUSTED device: point primary at a bogus addr so NSO
        # actually attempts TCP and blocks on its connect-timeout (host key already trusted).
        print("[dev15] true unreachable on trusted device (primary → bogus, long timeout):")
        await client.set_address(name, BOGUS_ADDR)
        await client.disconnect(name)
        results["unreach_true_live"] = await measure_unreachable(client, name, 40.0, 2, "unreach-trusted")
    finally:
        # Unconditional restore — device 15 must end on its original (primary) address, connected.
        await client.set_address(name, orig_addr)
        await client.disconnect(name)
        reachable, detail, _ = await actions.probe_reachable(client, name, probe_timeout)
        print(f"[dev15] RESTORED address={orig_addr} reachable={reachable}")

    _report(results, probe_timeout)


def _report(results: dict[str, list[float]], probe_timeout: float) -> None:
    print("\n" + "=" * 78)
    print("FAILOVER PERFORMANCE SPIKE — SUMMARY")
    print("=" * 78)
    print(_stat("reachable connect (primary)", results.get("reach_connect_primary", [])))
    print(_stat(f"unreachable connect capped@{probe_timeout}s", results.get("unreach_capped_temp", [])))
    print(_stat("unreachable connect NSO-true (temp)", results.get("unreach_true_temp", [])))
    print(_stat("unreachable connect NSO-true (trusted)", results.get("unreach_true_live", [])))
    print(_stat("full flip cycle (set+disc+connect)", results.get("flip_cycle", [])))
    print(_stat("op set_address", results.get("op_set_address", [])))
    print(_stat("op disconnect", results.get("op_disconnect", [])))
    print(_stat("op get_address", results.get("op_get_address", [])))
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(run())
