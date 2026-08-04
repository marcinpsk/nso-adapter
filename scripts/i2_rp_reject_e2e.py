# SPDX-License-Identifier: Apache-2.0
"""I2 reactive-capability live test: apply an IOS-XR route-map to a Junos device.

Pulls a real route-map off an IOS-XR device (lab01d-ra1.lab) and applies it to sw01
(Junos crpd, LAB01-CRPD-SW01) through the REAL adapter ``run_apply`` atomic path
(throwaway adapter device, only the one route-policy intent). The Junos commit rejects it
(the route-map references a prefix-list undefined on sw01), so the failure flows through
``_record_atomic_capability`` and the device_capability matrix learns the gap reactively —
the exact I2 behaviour the plugin NSO-tab panel then surfaces.

Reverts the throwaway DB rows + any service instance in a finally; LEAVES the recorded
capability rows (prints them) so they can be inspected / shown in the UI.

Run inside the adapter container:
  docker exec -e PYTHONPATH=/app nso-adapter-nso-adapter-1 \
    /opt/venv/bin/python /app/scripts/i2_rp_reject_e2e.py
"""

from __future__ import annotations

import os

os.environ["NSO_ADAPTER_ATOMIC_APPLY"] = "1"

import asyncio
import json
from datetime import UTC, datetime

from sqlalchemy import delete, select

from nso_adapter.config import get_config, get_env_settings
from nso_adapter.core.apply import run_apply
from nso_adapter.core.importer import register_nso_client
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider
from nso_adapter.store.db import get_session, init_db
from nso_adapter.store.models import (
    Device,
    DeviceCapability,
    Job,
    JobStatus,
    JobType,
    RoutePolicyObjectIntent,
)

NSO_NAME = "LAB01-CRPD-SW01"
JUNOS_NED = "juniper-junos-nc-4.19:juniper-junos-nc-4.19"
SRC = "lab01d-ra1.lab"
RM_NAME = "32_64_TEST_PERMIT_ALL"
RP_PATH = "/restconf/data/route-policy-reconciler:route-policy-config"


def _client() -> NsoClient:
    cfg = get_config()
    p = make_provider(cfg, get_env_settings())
    i = cfg.nso_instances[0]
    cl = NsoClient(i, p.get(i.username_ref), p.get(i.password_ref))
    register_nso_client(i.name, cl)
    return cl


async def main() -> None:
    init_db(get_config().database_url)
    c = _client()
    rp = await c.get_device_state_section(SRC, "route-policy")
    rm = next((x for x in (rp.get("route-map") or []) if x["name"] == RM_NAME), None)
    if rm is None:
        print(f"route-map {RM_NAME} not found on {SRC}")
        return
    print(f"source: route-map {rm['name']!r} from {SRC} ({len(rm['entry'])} entries)")

    dev_id = job_id = None
    try:
        async for db in get_session():
            d = Device(nso_instance="nso-dev", nso_device_name=NSO_NAME, netbox_device_id=987655, ned_id=JUNOS_NED)
            db.add(d)
            await db.flush()
            dev_id = d.id
            db.add(
                RoutePolicyObjectIntent(
                    device_id=d.id,
                    family="route_map",
                    name=rm["name"],
                    entries=rm["entry"],
                    accepted_at=datetime.now(UTC),
                )
            )
            j = Job(job_type=JobType.apply, device_id=d.id, status=JobStatus.queued)
            db.add(j)
            await db.commit()
            job_id = j.id
            break
        print(f"seeded throwaway device {dev_id} + route_map intent; job {job_id}")
        print(f"applying {RM_NAME} -> {NSO_NAME} (Junos) via run_apply ...")

        await run_apply(job_id, dev_id, force=True)

        async for db in get_session():
            job = await db.get(Job, job_id)
            print("\njob.status:", job.status)
            err = job.error or {}
            print("job.error.message:", err.get("message"))
            rows = (
                (await db.execute(select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == dev_id)))
                .scalars()
                .all()
            )
            for r in rows:
                lae = r.last_apply_error or {}
                print("rp intent last_apply_error:", json.dumps(lae)[:600])
            caps = (
                (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == JUNOS_NED))).scalars().all()
            )
            print("\nCAPABILITY recorded for", JUNOS_NED, ":")
            for cc in caps:
                print(f"   ({cc.scope}, {cc.status}, source={cc.source}) {(cc.detail or '')[:160]}")
            break
    finally:
        # revert: any rolled-back commit leaves no instance, but delete defensively
        async with c._client(timeout=120) as cl:
            r = await cl.delete(f"{c._base}{RP_PATH}={NSO_NAME}?reconcile=keep-non-service-config")
            print(f"\n[revert] delete route-policy svc instance -> HTTP {r.status_code}")
        async for db in get_session():
            if dev_id:
                await db.execute(delete(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == dev_id))
                if job_id:
                    jj = await db.get(Job, job_id)
                    if jj:
                        await db.delete(jj)
                dd = await db.get(Device, dev_id)
                if dd:
                    await db.delete(dd)
            await db.commit()
            break
        print("[revert] throwaway DB rows deleted; capability rows left for inspection")
        print("[revert] device in-sync:", await c.check_sync(NSO_NAME))


if __name__ == "__main__":
    asyncio.run(main())
