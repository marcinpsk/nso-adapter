# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S2: what offboard does to a device's settlement history.

Offboard does three things in one transaction: it bulk-terminalizes every QUEUED job of the
device, it NULLs ``device_id`` on EVERY job of the device (terminal ones included), and it
deletes the ``Device``. The middle step takes a device's already-sequenced history out of the
feed, and that is CORRECT — the device is gone and its plugin overlay goes with it.

Pinned so a later reader does not "repair" the null-out and resurrect a feed for a deleted
device. The partial unique index tolerates the detachment because NULLs are distinct.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from nso_adapter.core.claim import terminalize
from nso_adapter.store.models import Device, DeviceSettleCounter, Job, JobStatus, JobType
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def test_offboard_terminalizes_unsequenced_and_detaches_history(adapter_client):
    """S2.6 — the queued row terminalizes with NO sequence; the sequenced row keeps its own.

    Forbidden: the bulk write allocating (there is no execution to name, and the rows are
    about to be detached anyway), or the design "repairing" the ``device_id`` null-out so a
    deleted device keeps a feed nothing will ever consume.
    """
    from nso_adapter.core.onboarding import offboard_device

    device_id = await seed_device(nso_device_name="offboard-settle", netbox_device_id=8501)

    async with session() as db:
        history = Job(
            job_type=JobType.sync,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=True,
            run_attempt=1,
        )
        pending = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued, coalescible=True)
        db.add_all([history, pending])
        await db.commit()
        history_id, pending_id = history.id, pending.id

    async with session() as db:
        write = await terminalize(db, history_id, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
        await db.commit()
    assert write.settle_seq == 1

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))

    async with session() as db:
        history_row = await db.get(Job, history_id)
        pending_row = await db.get(Job, pending_id)
        counter = await db.scalar(
            sa.select(DeviceSettleCounter.last_seq).where(DeviceSettleCounter.device_id == device_id)
        )

    assert pending_row.status is JobStatus.failed
    assert pending_row.error["code"] == "device_offboarded"
    assert pending_row.settle_seq is None, "offboard's bulk write took a sequence"

    assert history_row.settle_seq == 1, "the already-allocated sequence was rewritten"
    assert history_row.device_id is None, "the detachment is the point: a deleted device has no feed"
    assert pending_row.device_id is None

    assert counter is None, "the counter did not cascade away with its device"
    async with session() as db:
        assert await db.get(Device, device_id) is None
