# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The per-device settle counter's lifecycle and allocation (Appendix S §3.3).

Three operations, deliberately separated by which transaction may run them:

* :func:`create_counter` runs in the SAME transaction as a ``Device`` insert. Both rows are
  new, so the FK check locks a row this transaction owns.
* :func:`ensure_settle_counters` is the repair path, and it runs outside any claim and
  outside any terminal transaction, where taking the device key-share lock is harmless. Its
  placement is load-bearing: a terminalization that finds no counter row RAISES, so it must
  precede every terminal recovery.
* :func:`allocate_settle_seq` runs inside the terminal transaction. It is a plain UPDATE of
  one counter row: it changes no FK column and therefore takes no lock on ``devices`` at
  all, which is what keeps the ``jobs -> devices`` edge out of the terminal path. A missing
  row raises :class:`MissingSettleCounter` and the whole terminal transaction aborts —
  recovery then decides. Creating it here is the deadlock this module exists to avoid.
"""

from __future__ import annotations

from typing import Any, cast

import structlog
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.models import Device, DeviceSettleCounter

logger = structlog.get_logger(__name__)


class MissingSettleCounter(RuntimeError):
    """A device-bound terminal write found no counter row to allocate from.

    Hard failure by design. The alternative — creating the row here — is what reintroduces
    the ``jobs -> devices`` lock edge that deadlocks against offboard.
    """


async def create_counter(db: AsyncSession, device_id: int) -> None:
    """Create the counter for a device being inserted. Caller commits.

    Called in the same transaction as the ``Device`` insert, at every site that inserts one.
    """
    await db.execute(pg_insert(DeviceSettleCounter).values(device_id=device_id, last_seq=0))


async def ensure_settle_counters() -> int:
    """Insert a counter row for every device missing one. Returns how many were created.

    The repair path for a device that predates the counter or that a future fourth insert
    site forgets. Its own transaction, no claim: the device key-share locks it takes are
    harmless here and would not be inside a terminal transaction.

    Row by row, under a SAVEPOINT each, and that is not a style choice. The insert validates
    its FK by waiting on any offboard holding that device ``FOR UPDATE``; when that offboard
    commits, the row is gone and the insert raises. As ONE statement that aborts every other
    device's repair too, and the exception then aborts the caller — which is a recovery tick
    (or a lifespan startup) whose remaining work would be skipped for the whole interval. One
    device vanishing under the sweep is normal, and it costs exactly that device's row.
    """
    from nso_adapter.store.db import get_session

    async for db in get_session():
        missing = (
            (
                await db.execute(
                    select(Device.id)
                    .outerjoin(DeviceSettleCounter, DeviceSettleCounter.device_id == Device.id)
                    .where(DeviceSettleCounter.device_id.is_(None))
                )
            )
            .scalars()
            .all()
        )
        created = 0
        for device_id in missing:
            try:
                async with db.begin_nested():
                    result = cast(
                        CursorResult[Any],
                        await db.execute(
                            pg_insert(DeviceSettleCounter)
                            .values(device_id=device_id, last_seq=0)
                            .on_conflict_do_nothing(index_elements=["device_id"])
                        ),
                    )
            except IntegrityError:
                # The device was offboarded while this insert waited on its row lock.
                logger.info("settle_counter.device_vanished", device_id=device_id)
                continue
            created += result.rowcount
        await db.commit()
        if created:
            logger.warning("settle_counter.repaired", created=created)
        return created
    return 0


async def allocate_settle_seq(db: AsyncSession, device_id: int) -> int:
    """Take this device's next settlement sequence. Caller commits.

    The UPDATE locks the counter row and holds it to COMMIT, so a rival on the same device
    blocks until this transaction ends and then reads the committed value: allocation order
    equals commit order PER DEVICE. Cross-device pairs interleave freely, which is allowed —
    the consumer's cursor is per device.

    Monotonicity is the contract; contiguity is not. The consumer walks rows, never values,
    so a gap would be invisible to it — and no consumer may assume there is none. (A
    rolled-back allocation is not a gap: the UPDATE rolls back with its transaction and the
    next caller receives the same value.)
    """
    seq = await db.scalar(
        sa_update(DeviceSettleCounter)
        .where(DeviceSettleCounter.device_id == device_id)
        .values(last_seq=DeviceSettleCounter.last_seq + 1)
        .returning(DeviceSettleCounter.last_seq)
        .execution_options(synchronize_session=False)
    )
    if seq is None:
        raise MissingSettleCounter(f"device {device_id} has no device_settle_counter row")
    return seq
