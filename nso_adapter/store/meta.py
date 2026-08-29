# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Store incarnation access (READSEM S4 D3).

:func:`ensure_store_meta` runs once at startup (after create_all / alembic) and caches
the singleton ``(incarnation, born)`` pair in-process — the pair cannot change while this
process lives (a rebuild is a new database and a new process), so API handlers read it
via :func:`get_store_incarnation` without a query.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from nso_adapter.store.db import session
from nso_adapter.store.models import StoreMeta

logger = structlog.get_logger(__name__)

_cached: tuple[str, datetime] | None = None


async def ensure_store_meta() -> tuple[str, datetime]:
    """Idempotently create the singleton row; cache and return ``(incarnation, born)``.

    Always re-reads the database (called once per startup): the cache serves request-time
    reads only and is OVERWRITTEN here, so a process that re-initialises against a
    different database (the per-test lifespan) never serves a stale pair. A concurrent
    first-insert (two processes starting against a fresh database) is resolved by the PK:
    the loser's INSERT violates ``id=1`` and re-reads the winner.
    """
    global _cached
    async with session() as db:
        row = (await db.execute(select(StoreMeta))).scalar_one_or_none()
        if row is None:
            row = StoreMeta(id=1, incarnation=str(uuid.uuid4()))
            db.add(row)
            try:
                await db.commit()
                await db.refresh(row)
                logger.info("store_meta.minted", incarnation=row.incarnation)
            except IntegrityError:
                await db.rollback()
                row = (await db.execute(select(StoreMeta))).scalar_one()
        _cached = (row.incarnation, row.born)
        return _cached
    raise RuntimeError("store_meta: no database session available")


def get_store_incarnation() -> tuple[str, datetime]:
    """Return the cached ``(incarnation, born)`` pair; requires :func:`ensure_store_meta` ran."""
    assert _cached is not None, "store incarnation not initialised — ensure_store_meta() at startup"
    return _cached
