# SPDX-License-Identifier: Apache-2.0
"""I3b live end-to-end: drive the REAL adapter ``run_apply`` atomic path against sw01.

Isolated + revertible: a throwaway adapter device (mapped to NSO ``LAB01-CRPD-SW01``)
carries ONLY the greenfield ``ae99.999`` + ``198.18.1.1/24`` intent, so ``run_apply`` (with
``NSO_ADAPTER_ATOMIC_APPLY=1``) pushes exactly that in ONE atomic commit and nothing else.
Everything is reverted in a ``finally`` (device config via the reconciler-service teardown,
then the throwaway DB rows). The real sw01 adapter device (402) is never applied.

Run inside the adapter container:
  docker exec -e PYTHONPATH=/app nso-adapter-nso-adapter-1 \
    /opt/venv/bin/python /app/scripts/i3b_live_e2e.py
"""

from __future__ import annotations

import os

os.environ["NSO_ADAPTER_ATOMIC_APPLY"] = "1"  # force the atomic path for this run

import asyncio
import json
from datetime import datetime

from sqlalchemy import select

from nso_adapter.config import get_config, get_env_settings
from nso_adapter.core.apply import run_apply
from nso_adapter.core.importer import register_nso_client
from nso_adapter.nso.apply import atomic_apply_enabled
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider
from nso_adapter.store.db import get_session, init_db
from nso_adapter.store.models import (
    DbInterface,
    Device,
    InterfaceIpIntent,
    Job,
    JobStatus,
    JobType,
    SubinterfaceIntent,
)

NSO_NAME = "LAB01-CRPD-SW01"
IFACE = "ae99.999"
RC = "?reconcile=keep-non-service-config"
HDR = {"Content-Type": "application/yang-data+json"}


def _client() -> NsoClient:
    cfg = get_config()
    p = make_provider(cfg, get_env_settings())
    i = cfg.nso_instances[0]
    client = NsoClient(i, p.get(i.username_ref), p.get(i.password_ref))
    register_nso_client(i.name, client)  # run_apply resolves the client via the importer registry
    return client


async def _ae99(c: NsoClient) -> dict:
    async with c._client(timeout=120) as cl:
        r = await cl.get(
            f"{c._base}/restconf/data/tailf-ncs:devices/device={NSO_NAME}"
            f"/config/junos:configuration/interfaces/interface=ae99"
        )
        d = r.json().get("junos:interface", [{}])[0]
        unit999 = next((u for u in d.get("unit", []) if u.get("name") == "999"), None)
        return {
            "units": [u.get("name") for u in d.get("unit", [])],
            "encap": d.get("encapsulation"),
            "unit999": unit999,
        }


async def main() -> None:
    print("atomic_apply_enabled():", atomic_apply_enabled())
    init_db(get_config().database_url)  # standalone script: wire the session factory to the app's DB
    c = _client()
    temp_dev_id = iface_id = job_id = None
    try:
        async for db in get_session():
            dev = Device(nso_instance="nso-dev", nso_device_name=NSO_NAME, netbox_device_id=987654)
            db.add(dev)
            await db.flush()
            temp_dev_id = dev.id
            iface = DbInterface(device_id=dev.id, netbox_interface_id=987654, name=IFACE, kind="logical")
            db.add(iface)
            await db.flush()
            iface_id = iface.id
            db.add(
                SubinterfaceIntent(
                    device_id=dev.id,
                    interface_name=IFACE,
                    parent_interface="ae99",
                    dot1q_vlan=999,
                    sub_type="subinterface",
                    accepted_at=datetime.utcnow(),
                )
            )
            db.add(
                InterfaceIpIntent(
                    interface_id=iface.id,
                    address="198.18.1.1/24",
                    family="ipv4",
                    secondary=False,
                    accepted_at=datetime.utcnow(),
                )
            )
            job = Job(job_type=JobType.apply, device_id=dev.id, status=JobStatus.queued)
            db.add(job)
            await db.commit()
            job_id = job.id
            break
        print(f"seeded throwaway device id={temp_dev_id} iface id={iface_id} job id={job_id}")
        print("ae99 BEFORE:", await _ae99(c))

        # ── the real adapter apply worker (atomic path) ──
        await run_apply(job_id, temp_dev_id, force=True)

        async for db in get_session():
            job = await db.get(Job, job_id)
            print("job.status:", job.status)
            print("job.result:", job.result)
            subif = (
                (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == temp_dev_id)))
                .scalars()
                .all()
            )
            ip = (
                (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
                .scalars()
                .all()
            )
            print("subif rows applied:", [(r.last_apply_at is not None, r.last_apply_error) for r in subif])
            print("ip    rows applied:", [(r.last_apply_at is not None, r.last_apply_error) for r in ip])
            break

        after = await _ae99(c)
        print("ae99 AFTER:", after)
        print("check-sync:", await c.check_sync(NSO_NAME))
        landed = after["unit999"] is not None and "family" in (after["unit999"] or {})
        print("RESULT:", "PASS — ae99.999 + IP landed one-shot via run_apply" if landed else "FAIL")
    finally:
        # revert device config (teardown-fix makes the subif PUT-replace clean)
        async with c._client(timeout=180) as cl:
            r1 = await cl.delete(
                f"{c._base}/restconf/data/interface-reconciler:interface-config={NSO_NAME},{IFACE}{RC}"
            )
            body = json.dumps({"subinterface-reconciler:subif-config": [{"device": NSO_NAME}]})
            r2 = await cl.put(
                f"{c._base}/restconf/data/subinterface-reconciler:subif-config={NSO_NAME}{RC}",
                content=body,
                headers=HDR,
            )
            r3 = await cl.delete(f"{c._base}/restconf/data/subinterface-reconciler:subif-config={NSO_NAME}{RC}")
            print(f"[revert] del-ip={r1.status_code} put-subif-empty={r2.status_code} del-subif={r3.status_code}")
        # delete throwaway DB rows
        async for db in get_session():
            for r in (
                (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
                .scalars()
                .all()
            ):
                await db.delete(r)
            for r in (
                (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == temp_dev_id)))
                .scalars()
                .all()
            ):
                await db.delete(r)
            for obj in (
                await db.get(DbInterface, iface_id),
                await db.get(Job, job_id),
                await db.get(Device, temp_dev_id),
            ):
                if obj is not None:
                    await db.delete(obj)
            await db.commit()
            break
        print("[revert] throwaway rows deleted; ae99 NOW:", await _ae99(c), "check-sync:", await c.check_sync(NSO_NAME))


if __name__ == "__main__":
    asyncio.run(main())
