# SPDX-License-Identifier: Apache-2.0
"""Atomic-apply (Phase I3a) live-NSO spike.

Runs INSIDE the adapter container (reaches ``nso:8080`` + Vault, resolves real NSO
creds via the adapter's own secrets provider). Proves whether NSO RESTCONF can stage
TWO reconciler services (subinterface + interface IP) into ONE transaction and commit
them as ONE device transaction — the mechanism Phase I3a needs.

Subject: the sw01 greenfield ``ae99.999`` + IP ``33.1.1.1/24`` (NSO PROD-LAB03-CRPD-SW01).

Modes:
  recon  (default) — READ-ONLY. Enumerates transaction-related RESTCONF operations,
                     shows whether ae99.999 already exists, then dry-runs (``dry-run=native``,
                     no commit) subif-alone / ip-alone / COMBINED multi-module PATCH and
                     prints each device delta. Writes NOTHING to the device.
  commit          — REAL device write. Sends the combined multi-module PATCH for real
                    (one /restconf/data PATCH = one NSO transaction), then re-dry-runs to
                    verify nothing is left to push. ONLY run after explicit go-ahead.
  remove          — REAL device write. PUT-replaces both reconciler service instances back
                    to empty (drops ae99.999 + its IP), to clean up after a commit.

Usage:
    docker exec nso-adapter-nso-adapter-1 python /app/scripts/atomic_apply_spike.py [recon|commit|remove]
"""

from __future__ import annotations

import asyncio
import json
import sys

from nso_adapter.config import get_config, get_env_settings
from nso_adapter.nso.apply import _device_delta_from_dry_run
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider

DEV = "PROD-LAB03-CRPD-SW01"
PARENT = "ae99"
SUBIF = "ae99.999"
DOT1Q = 999
IPV4 = "33.1.1.1"
PLEN = 24

DATA = "/restconf/data"
SUBIF_PATH = "/restconf/data/subinterface-reconciler:subif-config"
IFACE_PATH = "/restconf/data/interface-reconciler:interface-config"
YANG_HDRS = {"Content-Type": "application/yang-data+json"}

_SUBIF_BODY = {
    "subinterface-reconciler:subif-config": [
        {
            "device": DEV,
            "interface": [
                {
                    "interface-name": SUBIF,
                    "parent-interface": PARENT,
                    "dot1q-vlan": DOT1Q,
                    "type": "subinterface",
                }
            ],
        }
    ]
}
_IFACE_BODY = {
    "interface-reconciler:interface-config": [
        {
            "device": DEV,
            "interface-name": SUBIF,
            "ipv4-address": [{"address": IPV4, "prefix-length": PLEN, "secondary": False}],
        }
    ]
}
# The combined body = both modules at the datastore root → ONE RESTCONF edit.
_COMBINED_BODY = {**_SUBIF_BODY, **_IFACE_BODY}


def _make_client() -> NsoClient:
    cfg = get_config()
    provider = make_provider(cfg, get_env_settings())
    inst = cfg.nso_instances[0]
    print(f"[cfg] instance={inst.name} base_url={inst.base_url} host_header={inst.host_header}")
    return NsoClient(inst, provider.get(inst.username_ref), provider.get(inst.password_ref))


def _q(url: str, *, dry_run: bool, reconcile: str = "keep-non-service-config") -> str:
    params = []
    if dry_run:
        params.append("dry-run=native")
    if reconcile:
        params.append(f"reconcile={reconcile}")
    return f"{url}?{'&'.join(params)}" if params else url


async def _dry_run(client: NsoClient, url: str, body: dict, label: str) -> None:
    payload = json.dumps(body)
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(_q(url, dry_run=True), content=payload, headers=YANG_HDRS)
    print(f"\n--- DRY-RUN [{label}] PATCH {url}  → HTTP {resp.status_code}")
    if resp.status_code not in (200, 201, 204):
        print(f"    ERROR body: {resp.text[:600]}")
        return
    try:
        delta = _device_delta_from_dry_run(resp.json(), DEV)
    except Exception as exc:  # noqa: BLE001
        print(f"    (unparseable dry-run body: {exc}); raw: {resp.text[:400]}")
        return
    if delta is None:
        print("    delta=INCONCLUSIVE (unexpected shape)")
    elif not delta.strip():
        print("    delta=EMPTY (NSO would push nothing)")
    else:
        print(f"    delta ({len(delta)} chars):\n{_indent(delta)}")


def _indent(text: str) -> str:
    return "\n".join("      " + ln for ln in text.splitlines())


async def _get(client: NsoClient, url: str, label: str) -> None:
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.get(url)
    print(f"\n--- GET [{label}] {url}  → HTTP {resp.status_code}")
    if resp.status_code == 404:
        print("    (404 — not present)")
        return
    print(f"    {resp.text[:800]}")


async def recon(client: NsoClient) -> None:
    print("\n========== RECON (read-only) ==========")

    # 1. Are tailf-netconf-transactions operations exposed over RESTCONF at all?
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.get(f"{client._base}/restconf/operations")
    print(f"\n--- GET /restconf/operations → HTTP {resp.status_code}")
    if resp.status_code == 200:
        try:
            ops = resp.json().get("ietf-restconf:operations", {})
            tx_ops = sorted(k for k in ops if "transaction" in k.lower() or "netconf-transactions" in k.lower())
            print(f"    transaction-related operations exposed: {tx_ops or 'NONE'}")
            print(f"    (total operations advertised: {len(ops)})")
        except Exception as exc:  # noqa: BLE001
            print(f"    (could not parse operations: {exc}); raw head: {resp.text[:300]}")
    else:
        print(f"    raw: {resp.text[:300]}")

    # 2. Does the greenfield subif already exist on the device CDB?
    ae99 = f"{client._base}/restconf/data/tailf-ncs:devices/device={DEV}/config/junos:configuration/interfaces/interface={PARENT}"
    await _get(client, ae99, "sw01 ae99 (junos cfg subtree)")

    # 3. Dry-run each scope alone, then combined. The ordering problem shows up here:
    #    ip-alone on a not-yet-existing unit vs. the combined single-transaction edit.
    await _dry_run(client, f"{client._base}{SUBIF_PATH}", _SUBIF_BODY, "subif ALONE")
    await _dry_run(client, f"{client._base}{IFACE_PATH}", _IFACE_BODY, "ip ALONE")
    await _dry_run(client, f"{client._base}{DATA}", _COMBINED_BODY, "COMBINED subif+ip (one /restconf/data edit)")

    print("\n========== RECON DONE — no device writes were made ==========")


async def commit(client: NsoClient) -> None:
    print("\n========== COMMIT (REAL device write) ==========")
    payload = json.dumps(_COMBINED_BODY)
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(_q(f"{client._base}{DATA}", dry_run=False), content=payload, headers=YANG_HDRS)
    print(f"--- PATCH {DATA} (combined, reconcile) → HTTP {resp.status_code}")
    if resp.status_code not in (200, 201, 204):
        print(f"    ERROR body: {resp.text[:800]}")
        return
    print("    committed OK (one /restconf/data edit = one NSO transaction)")
    # verify: nothing left to push
    await _dry_run(client, f"{client._base}{DATA}", _COMBINED_BODY, "VERIFY combined (post-commit)")


async def remove(client: NsoClient) -> None:
    print("\n========== REMOVE (REAL device write — cleanup) ==========")
    # Surgical: DELETE only the greenfield entries (NOT a whole-instance wipe), so other
    # adapter-managed interfaces/subifs on this device are untouched. interface-config is
    # keyed (device, interface-name); subif-config holds a nested interface list.
    targets = [
        f"{IFACE_PATH}={DEV},{SUBIF}",
        f"{SUBIF_PATH}={DEV}/interface={SUBIF}",
    ]
    for path in targets:
        url = f"{client._base}{path}"
        async with client._client(timeout=client._action_timeout) as c:
            resp = await c.delete(_q(url, dry_run=False))
        print(f"--- DELETE {path} → HTTP {resp.status_code}")
        if resp.status_code not in (200, 201, 204, 404):
            print(f"    ERROR body: {resp.text[:600]}")


async def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "recon"
    client = _make_client()
    if mode == "recon":
        await recon(client)
    elif mode == "commit":
        await commit(client)
    elif mode == "remove":
        await remove(client)
    else:
        print(f"unknown mode {mode!r}; use recon|commit|remove")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
