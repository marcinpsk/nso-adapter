# SPDX-License-Identifier: Apache-2.0
"""OpenAPI-truthfulness S5 — response_model serialization-cost spike.

Question (plan §S5 / F9): does the pydantic ``response_model`` validation we added
across every endpoint (S0–S4) impose a material cost on the two hot LIST endpoints
that can return thousands of rows — ``GET /interfaces`` and ``GET /route-policy`` —
and should those specific endpoints drop ``response_model`` (keeping the documented
``responses={200: {"model": X}}`` schema, skipping runtime validation)?

Method — two independent, corroborating measurements against the REAL app + a real
SQLite store seeded with maximal payloads (no NSO, no network):

  * End-to-end HTTP: identical app / DB, flip ONLY ``response_model`` and time real
    requests. The delta is the whole user-facing cost. The "WITHOUT" variant rebuilds
    the target route handler with ``response_field=None`` — exactly what
    ``response_model=None`` yields: FastAPI's ``use_dump_json`` fast path is disabled,
    so serialization falls back to ``jsonable_encoder`` (pure-Python) + the JSONResponse
    ``json.dumps`` render, instead of pydantic-core's Rust ``to_json``.
  * Isolated serialize: capture the handler's raw dict ONCE, then time only the two
    serialization paths (no DB, no ASGI) — the crisp, low-noise marginal number.

Both are apples-to-apples: ``body-parity`` asserts the two paths emit byte-identical
JSON, so this measures serialization strategy, not output shape.

Run (host, from the repo root)::

    uv run --native-tls python scripts/response_model_perf_spike.py

Prints only timings + payload sizes; the only writes are throwaway rows in a temp
SQLite file that is discarded on exit.

Result (2026-07-20, this host) — the hypothesis is REFUTED: ``response_model`` is not a
cost, it is a net win. With the default ``JSONResponse``, FastAPI's ``use_dump_json`` fast
path serializes via pydantic-core's Rust ``to_json``, which is ~3-4x faster than the
``jsonable_encoder`` + ``json.dumps`` fallback that ``response_model=None`` forces — even
though the fast path also validates. Isolated serialize medians: /interfaces 67.8 ms WITH
vs 246.5 ms WITHOUT; /route-policy 39.9 ms WITH vs 119.8 ms WITHOUT. End-to-end: dropping
``response_model`` is ~20-23% SLOWER on the maximal payloads. Decision: KEEP ``response_model``
on every endpoint (no ``response_model=None`` swap). See docs/response-model-perf-spike.md.
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

# Hermetic: never inherit an ambient dev DATABASE_URL (see tests/conftest.py).
os.environ.pop("DATABASE_URL", None)

VALID_TOKEN = "spike-bearer-token"  # noqa: S105 — throwaway local token
AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# Maximal payload sizes ("1000s of rows" per the plan).
N_IFACES, ATTRS_PER = 2000, 4
N_PL = N_CL = 400
N_AP = N_RM = 200
ENTRIES_PER = 8

ITERS, WARMUP = 40, 5
ISO_ITERS, ISO_WARMUP = 60, 5


def summ(samples: list[float]) -> str:
    p90 = sorted(samples)[int(len(samples) * 0.9)]
    return (
        f"min={min(samples):8.2f}  median={statistics.median(samples):8.2f}  "
        f"mean={statistics.mean(samples):8.2f}  p90={p90:8.2f}"
    )


def pct_slower(fast: float, slow: float) -> str:
    return f"{(slow - fast) / fast * 100:+.1f}%"


async def seed_interfaces() -> int:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        DbInterface,
        Device,
        InterfaceAttrState,
        InterfaceIntent,
        ManagedScope,
        SyncState,
    )

    ts = datetime(2026, 5, 20, 10, 0, 0)
    attr_names = ["description", "mtu", "admin_state", "ipv4", "ipv6", "vrf", "speed", "duplex"][:ATTRS_PER]
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name="bench-if", netbox_device_id=1)
        db.add(d)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        ifaces = [
            DbInterface(
                device_id=d.id,
                name=f"GigabitEthernet0/0/{i}.{i % 4000}",
                netbox_interface_id=10_000 + i,
                parent_binding=f"lag-{i % 64}",
                kind="logical",
                encap_tag=str(i % 4000),
                vrf=f"VRF-{i % 32}",
                service=f"EPIPE-{i}",
            )
            for i in range(N_IFACES)
        ]
        db.add_all(ifaces)
        await db.flush()
        attr_rows, intent_rows = [], []
        for iface in ifaces:
            for a in attr_names:
                attr_rows.append(
                    InterfaceAttrState(
                        interface_id=iface.id,
                        attribute=a,
                        nso_value=f"nso-{a}-value-for-{iface.name}",
                        netbox_value=f"nb-{a}-value",
                        sync_state=SyncState.apply_failed,
                    )
                )
                intent_rows.append(
                    InterfaceIntent(
                        interface_id=iface.id,
                        attribute=a,
                        intent_value=f"intent-{a}-value",
                        last_apply_at=ts,
                        last_apply_error={
                            "code": "nso_error",
                            "message": "commit rejected by device",
                            "detail": {"attr": a},
                        },
                    )
                )
        db.add_all(attr_rows)
        db.add_all(intent_rows)
        await db.commit()
        return d.id
    raise RuntimeError("no session")


async def seed_route_policy() -> int:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        Device,
        DeviceRoutePolicyASPath,
        DeviceRoutePolicyASPathEntry,
        DeviceRoutePolicyCommunityList,
        DeviceRoutePolicyCommunityListEntry,
        DeviceRoutePolicyPrefixList,
        DeviceRoutePolicyPrefixListEntry,
        DeviceRoutePolicyRouteMap,
        DeviceRoutePolicyRouteMapEntry,
        ManagedScope,
    )

    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name="bench-rp", netbox_device_id=2)
        db.add(d)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="route_policy"))

        pls = [
            DeviceRoutePolicyPrefixList(device_id=d.id, name=f"PL-{i:04d}", family=4, last_refreshed_at=ts)
            for i in range(N_PL)
        ]
        cls = [
            DeviceRoutePolicyCommunityList(
                device_id=d.id, name=f"CL-{i:04d}", invert_match=bool(i % 2), last_refreshed_at=ts
            )
            for i in range(N_CL)
        ]
        aps = [DeviceRoutePolicyASPath(device_id=d.id, name=f"AP-{i:04d}", last_refreshed_at=ts) for i in range(N_AP)]
        rms = [DeviceRoutePolicyRouteMap(device_id=d.id, name=f"RM-{i:04d}", last_refreshed_at=ts) for i in range(N_RM)]
        db.add_all(pls + cls + aps + rms)
        await db.flush()

        rows: list = []
        for pl in pls:
            for s in range(ENTRIES_PER):
                rows.append(
                    DeviceRoutePolicyPrefixListEntry(
                        prefix_list_id=pl.id,
                        sequence=(s + 1) * 10,
                        action="permit",
                        prefix=f"10.{s}.0.0/16",
                        ge=17,
                        le=24,
                    )
                )
        for cl in cls:
            for s in range(ENTRIES_PER):
                rows.append(
                    DeviceRoutePolicyCommunityListEntry(
                        community_list_id=cl.id, sequence=(s + 1) * 10, action="permit", community=f"64500:{s}"
                    )
                )
        for ap in aps:
            for s in range(ENTRIES_PER):
                rows.append(
                    DeviceRoutePolicyASPathEntry(
                        as_path_id=ap.id, sequence=(s + 1) * 10, action="permit", pattern=f"^64500_{s}_"
                    )
                )
        for rm in rms:
            for s in range(ENTRIES_PER):
                rows.append(
                    DeviceRoutePolicyRouteMapEntry(
                        route_map_id=rm.id,
                        sequence=(s + 1) * 10,
                        action="permit",
                        match_prefix_lists=[f"PL-{s:04d}"],
                        match_community_lists=[f"CL-{s:04d}"],
                        match_as_paths=[f"AP-{s:04d}"],
                        match_json='{"prefix": "PL-x", "local_preference": 200}',
                        set_json='{"local_preference": 200, "community": "64500:1"}',
                    )
                )
        db.add_all(rows)
        await db.commit()
        return d.id
    raise RuntimeError("no session")


def route_for(app, path: str, method: str = "GET"):
    for route in app.routes:
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
            return route
    raise RuntimeError(f"route not found: {method} {path}")


def strip_response_model(app, path: str, method: str = "GET") -> None:
    """Rebuild the target route handler with ``response_field=None`` (= ``response_model=None``)."""
    from fastapi.routing import request_response  # FastAPI's own — sets up fastapi_inner_astack

    route = route_for(app, path, method)
    route.response_field = None
    route.app = request_response(route.get_route_handler())


async def time_http(client, url: str) -> tuple[list[float], int, bytes]:
    r = None
    for _ in range(WARMUP):
        r = await client.get(url, headers=AUTH)
        assert r.status_code == 200, r.status_code
    samples = []
    for _ in range(ITERS):
        t0 = time.perf_counter()
        r = await client.get(url, headers=AUTH)
        samples.append((time.perf_counter() - t0) * 1000.0)
    return samples, r.status_code, r.content  # full body → true byte-parity, not just length


async def isolated_serialize(app, path: str, out) -> None:
    """Time ONLY serialization on the captured handler output — no DB, no ASGI."""
    from fastapi.encoders import jsonable_encoder
    from fastapi.routing import serialize_response

    route = route_for(app, path)
    field = route.response_field
    exclude_unset = bool(route.response_model_exclude_unset)

    async def with_path():
        return await serialize_response(
            field=field, response_content=out, exclude_unset=exclude_unset, is_coroutine=True, dump_json=True
        )

    def without_path():  # what response_model=None yields: jsonable_encoder + JSONResponse render
        return json.dumps(jsonable_encoder(out), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )

    for _ in range(ISO_WARMUP):
        await with_path()
        without_path()

    with_ms, without_ms = [], []
    wb = ob = b""
    for _ in range(ISO_ITERS):
        t0 = time.perf_counter()
        wb = await with_path()
        with_ms.append((time.perf_counter() - t0) * 1000.0)
        t0 = time.perf_counter()
        ob = without_path()
        without_ms.append((time.perf_counter() - t0) * 1000.0)

    print(f"  [isolated serialize, {ISO_ITERS} iters]  body-parity={wb == ob}  bytes={len(wb)}")
    print(f"    WITH   response_model (pydantic to_json)     {summ(with_ms)}")
    print(f"    WITHOUT response_model (jsonable+json.dumps) {summ(without_ms)}")
    delta = statistics.median(with_ms) - statistics.median(without_ms)
    print(
        f"    → response_model marginal serialize cost (median): {delta:+.2f} ms/req "
        f"({'FASTER' if delta < 0 else 'slower'} with response_model)"
    )


async def main() -> None:
    from httpx import ASGITransport, AsyncClient

    tmp = Path(tempfile.mkdtemp())
    cfg = tmp / "config.yaml"
    cfg.write_text(
        f"""
secrets:
  provider: local
nso_instances: []
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: sqlite+aiosqlite:///{tmp}/spike.db
"""
    )
    os.environ["CONFIG_FILE"] = str(cfg)
    os.environ["ADAPTER_TOKEN"] = VALID_TOKEN
    os.environ["NETBOX_TOKEN"] = "nb-spike-token"  # noqa: S105 — throwaway local token

    from nso_adapter.config import reset_config

    reset_config()
    from nso_adapter.main import create_app

    app = create_app()

    with (
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        async with app.router.lifespan_context(app):
            if_dev = await seed_interfaces()
            rp_dev = await seed_route_policy()
            if_url = f"/api/v1/devices/{if_dev}/interfaces"
            rp_url = f"/api/v1/devices/{rp_dev}/route-policy"
            if_tmpl = "/api/v1/devices/{device_id}/interfaces"
            rp_tmpl = "/api/v1/devices/{device_id}/route-policy"

            from nso_adapter.api.interfaces import list_interfaces
            from nso_adapter.api.route_policy import get_route_policy
            from nso_adapter.store.db import get_session

            if_out = rp_out = None
            async for db in get_session():
                if_out = await list_interfaces(if_dev, db)
                rp_out = await get_route_policy(rp_dev, db)
                break

            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                print("=" * 92)
                print(f"S5 response_model perf spike — {ITERS} HTTP iters ({WARMUP} warmup), per-request ms")
                print("=" * 92)

                on, code, body = await time_http(client, if_url)
                print(
                    f"\n### GET /interfaces — {N_IFACES} ifaces x {ATTRS_PER} attrs = "
                    f"{N_IFACES} + {N_IFACES * ATTRS_PER} models; body={len(body) / 1024:.0f} KiB; http={code}"
                )
                print(f"  WITH    response_model  {summ(on)}")
                await isolated_serialize(app, if_tmpl, if_out)
                strip_response_model(app, if_tmpl)
                off, code2, body2 = await time_http(client, if_url)
                print(f"  WITHOUT response_model  {summ(off)}")
                print(
                    f"  → end-to-end delta (median): WITHOUT is {pct_slower(statistics.median(on), statistics.median(off))} "
                    f"vs WITH  ·  byte-parity={body == body2}"
                )

                on2, code, body = await time_http(client, rp_url)
                nobj = N_PL + N_CL + N_AP + N_RM
                print(
                    f"\n### GET /route-policy — {nobj} objects x {ENTRIES_PER} entries = "
                    f"~{nobj} + {nobj * ENTRIES_PER} models; body={len(body) / 1024:.0f} KiB; http={code}"
                )
                print(f"  WITH    response_model  {summ(on2)}")
                await isolated_serialize(app, rp_tmpl, rp_out)
                strip_response_model(app, rp_tmpl)
                off2, code2, body2 = await time_http(client, rp_url)
                print(f"  WITHOUT response_model  {summ(off2)}")
                print(
                    f"  → end-to-end delta (median): WITHOUT is {pct_slower(statistics.median(on2), statistics.median(off2))} "
                    f"vs WITH  ·  byte-parity={body == body2}"
                )
                print("\n" + "=" * 92)


if __name__ == "__main__":
    asyncio.run(main())
