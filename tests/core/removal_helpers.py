# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared removal-job seed helpers."""

from __future__ import annotations

from sqlalchemy import update

from nso_adapter.core.generation import attach_to_job, create_reissue_generation
from nso_adapter.store.models import (
    Device,
    DeviceProjectionStream,
    GenerationMode,
    Job,
    JobStatus,
    JobType,
    StaticRouteTombstone,
)
from tests.conftest import session


async def seed_tomb(
    device_id: int,
    triple,
    *,
    job_id: int | None = None,
    route_id: int = 99,
    deployed_key=None,
    marking: str = "delete_origin",
) -> int:
    vrf, prefix, next_hop = triple
    async with session() as db:
        tomb = StaticRouteTombstone(
            device_id=device_id,
            route_id=route_id,
            vrf=vrf,
            prefix=prefix,
            next_hop=next_hop,
            deployed_key=deployed_key,
            marking=marking,
            job_id=job_id,
        )
        db.add(tomb)
        await db.commit()
        return tomb.id


async def authorize_static_route(device_id: int) -> None:
    """Freeze the device's current static-route state as its authorized fragment.

    What a real deletion push leaves behind: the rows and carriers it promoted, with the
    context and apply plan it froze. A removal generation composes that fragment, so a
    fixture that skips it produces a document with no section to operate on.
    """
    from nso_adapter.core.generation import lock_projection, note_write
    from nso_adapter.core.projection import freeze_fragment, snapshot_stream

    async with session() as db:
        await lock_projection(db, device_id)
        revision = await note_write(db, device_id, "static_route")
        device = await db.get(Device, device_id)
        fragment = await freeze_fragment(
            db, device, "static_route", await snapshot_stream(db, device_id, "static_route")
        )
        await db.execute(
            update(DeviceProjectionStream)
            .where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
            .values(authorized_revision=revision, authorized_document=fragment)
        )
        await db.commit()


async def seed_removal_job(device_id: int, context: dict, *, tombs: tuple[int, ...] = ()) -> int:
    """Seed a started removal job carrying the REISSUE generation enqueue_removal would give it.

    A removal job with no generation at all is not a state production can reach (#1522 §G1),
    and both runners now refuse it. Production writes the carriers first, creates the
    generation that SELECTS them by id, and stamps the job on them afterwards; the order is
    what lets the operation plane name the authority the job executes.
    """
    full_context = {"scope": "static_route", **context}
    await authorize_static_route(device_id)
    async with session() as db:
        generation = await create_reissue_generation(
            db,
            device_id,
            mode=GenerationMode.detach if full_context.get("detach") else GenerationMode.networked,
            removal_context=full_context,
            # Scope-qualified, like every real producer: the guard is device-wide now.
            allowed_removal_keys={"static_route": full_context["removed"]} if full_context.get("removed") else {},
            static_route_tombstone_ids=tuple(tombs),
        )
        # Started, at attempt 1: see seed_apply_job in test_static_route_put.
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=False,
            run_attempt=1,
            context=full_context,
        )
        db.add(job)
        await db.flush()
        await attach_to_job(db, generation, job)
        if tombs:
            await db.execute(
                update(StaticRouteTombstone).where(StaticRouteTombstone.id.in_(list(tombs))).values(job_id=job.id)
            )
        await db.commit()
        return job.id
