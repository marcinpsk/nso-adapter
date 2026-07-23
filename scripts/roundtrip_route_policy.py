# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <mazieba@libertyglobal.com>
"""Brownfield route-policy round-trip verifier.

Reads a device's route-policy via network-state-export (the read-mirror source),
feeds the same data back through the route-policy-reconciler as a ``dry-run=native``
intent push, and reports the southbound delta. A faithful brownfield round-trip
produces an EMPTY delta — anything else is a read/write fidelity gap.

Usage (inside the adapter container):
    python scripts/roundtrip_route_policy.py <device-name-or-ned-substring> [--show N]

No commit is ever issued (dry_run=True → NSO computes the delta then aborts).
"""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

from sqlalchemy import select

from nso_adapter.config import EnvSettings, get_config
from nso_adapter.core.importer import register_nso_client
from nso_adapter.nso.apply import apply_route_policy_config
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider
from nso_adapter.store.db import get_session, init_db
from nso_adapter.store.models import Device

# read-data key  ->  intent family
_FAMILY = {
    "prefix-list": "prefix_list",
    "community-list": "community_list",
    "as-path": "as_path",
    "route-map": "route_map",
}


def _intent_rows(read: dict) -> list:
    rows: list = []
    for key, family in _FAMILY.items():
        for obj in read.get(key, []) or []:
            rows.append(
                SimpleNamespace(
                    family=family,
                    name=obj.get("name"),
                    entries=obj.get("entry", []) or [],
                    invert_match=bool(obj.get("invert-match", False)),
                )
            )
    return rows


async def main() -> int:
    if len(sys.argv) < 2:
        print("usage: roundtrip_route_policy.py <device-or-ned-substr> [--show N]")
        return 2
    needle = sys.argv[1].lower()
    show = 0
    if "--show" in sys.argv:
        show = int(sys.argv[sys.argv.index("--show") + 1])

    cfg = get_config()
    env = EnvSettings()
    provider = make_provider(cfg, env)
    init_db(cfg.database_url)

    inst = cfg.nso_instances[0]
    client = NsoClient(inst, provider.get(inst.username_ref), provider.get(inst.password_ref))
    register_nso_client(inst.name, client)

    async for db in get_session():
        rows = (await db.execute(select(Device.nso_device_name, Device.ned_id))).all()
        break
    targets = [(n, ned) for n, ned in rows if n and (needle in n.lower() or (ned and needle in ned.lower()))]
    if not targets:
        print(f"no device matches {needle!r}")
        return 1

    overall_clean = True
    for device_name, ned_id in targets:
        read = await client.get_device_state_section(device_name, "route-policy")
        if not read:
            print(f"{device_name}: no route-policy read data")
            continue
        intent = _intent_rows(read)
        n_rm = sum(1 for r in intent if r.family == "route_map")
        delta = await apply_route_policy_config(client, device_name, intent, ned_id=ned_id, dry_run=True)
        if not delta or not delta.strip():
            print(f"{device_name} ({ned_id})  route-maps={n_rm}  ROUND-TRIP: EMPTY -> FULLY FAITHFUL")
        else:
            overall_clean = False
            lines = delta.splitlines()
            print(f"{device_name} ({ned_id})  route-maps={n_rm}  DELTA: {len(delta)} chars / {len(lines)} lines")
            if show:
                print("\n".join(lines[:show]))
                print("..." if len(lines) > show else "")
    return 0 if overall_clean else 3


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
