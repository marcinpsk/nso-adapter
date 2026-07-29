# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""``init_db`` accepts PostgreSQL URLs and nothing else.

The store is PostgreSQL-only by construction: advisory-lock family fences,
``ON CONFLICT … WHERE`` pointer upserts, REPEATABLE READ read snapshots and every
``timestamptz`` column assume it. Until #1329 the family fence carried a dialect check
that incidentally aborted a wrong-engine run — mid-refresh, after the app was already
serving. With that check gone the URL is rejected at bind time instead, so a
misconfiguration fails at startup rather than on the first write.
"""

from __future__ import annotations

import pytest

from nso_adapter.store import db as store_db

_RETIRED_URL = "sqlite+aiosqlite:///tmp/x.db"  # a rejection fixture; the store is never run against sqlite
_RETIRED_SCHEME = _RETIRED_URL.split("://", 1)[0]


@pytest.fixture(autouse=True)
def restore_store_globals():
    """init_db writes process globals; put back whatever the surrounding suite had."""
    engine, factory = store_db._engine, store_db._session_factory
    yield
    store_db._engine, store_db._session_factory = engine, factory


def test_init_db_rejects_a_non_postgresql_url():
    """The retired driver is refused outright, and the message names the offending scheme."""
    with pytest.raises(ValueError) as exc:
        store_db.init_db(_RETIRED_URL)
    assert _RETIRED_SCHEME in str(exc.value), "the error must name the scheme it rejected"


def test_init_db_rejects_a_bare_path_with_no_scheme():
    """A URL-shaped typo (no scheme at all) is rejected the same way, not silently accepted."""
    with pytest.raises(ValueError) as exc:
        store_db.init_db("/var/lib/adapter/store.db")
    assert "PostgreSQL" in str(exc.value)


def test_init_db_leaves_the_globals_unbound_when_it_rejects():
    """A rejected URL must not half-bind the process: no engine, no session factory."""
    store_db._engine = None
    store_db._session_factory = None
    with pytest.raises(ValueError):
        store_db.init_db(_RETIRED_URL)
    assert store_db.get_engine() is None
    assert store_db._session_factory is None


def test_init_db_binds_a_postgresql_url():
    """The supported scheme constructs an engine and a session factory."""
    store_db.init_db("postgresql+asyncpg://u:p@127.0.0.1:5432/adapter")
    engine = store_db.get_engine()
    assert engine is not None
    assert engine.dialect.name == "postgresql"
    assert store_db._session_factory is not None
