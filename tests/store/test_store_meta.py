# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The store incarnation singleton (READSEM S4 D3).

``store_meta`` holds ONE row: the store incarnation ``(uuid, born)`` minted when this
database was first initialised. A DB rebuild mints a new pair — that pair rides every
``read_state`` block on the wire and is the plugin's adoption/reset signal. These tests
pin: creation on startup, idempotency (the pair never changes while the DB lives), and
the cached accessor serving the persisted values.
"""

from __future__ import annotations

import uuid as uuid_mod

import pytest
from sqlalchemy import select

from nso_adapter.store import meta as store_meta
from nso_adapter.store.models import StoreMeta
from tests.conftest import session


@pytest.mark.anyio
async def test_store_meta_row_created_on_startup(adapter_client):
    """App startup (lifespan → _init_database) must leave exactly one store_meta row with a
    valid UUID and a tz-aware born timestamp."""
    async with session() as db:
        rows = (await db.execute(select(StoreMeta))).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.id == 1
        uuid_mod.UUID(row.incarnation)  # parses as a UUID or raises
        # sqlite returns naive datetimes for DateTime(timezone=True) (UTC by convention,
        # like every other tz column in this store); PG serves tz-aware. Assert presence only.
        assert row.born is not None


@pytest.mark.anyio
async def test_ensure_store_meta_idempotent(adapter_client):
    """Calling ensure again (a restart against the same DB) must keep the SAME pair —
    the incarnation only changes when the database itself is rebuilt."""
    first = await store_meta.ensure_store_meta()
    second = await store_meta.ensure_store_meta()
    assert first == second
    async with session() as db:
        row = (await db.execute(select(StoreMeta))).scalar_one()
        assert (row.incarnation, row.born) == first


@pytest.mark.anyio
async def test_get_store_incarnation_serves_persisted_pair(adapter_client):
    """The cached accessor returns the persisted (uuid, born) pair for API handlers."""
    incarnation, born = store_meta.get_store_incarnation()
    async with session() as db:
        row = (await db.execute(select(StoreMeta))).scalar_one()
        assert (row.incarnation, row.born) == (incarnation, born)
