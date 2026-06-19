# SPDX-License-Identifier: Apache-2.0
"""SQLAlchemy async engine and session factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(database_url: str) -> None:
    global _engine, _session_factory
    if database_url.startswith("sqlite"):
        # SQLite (tests) — leave pooling at the driver default; sizing it risks "database is locked".
        _engine = create_async_engine(database_url, connect_args={"check_same_thread": False}, echo=False)
    else:
        # The failover base tick can hold one session per concurrent probe for the full
        # unreachable-probe timeout (~10s), and normal API/sync traffic shares this pool. Keep
        # it comfortably above failover_probe_concurrency (capped at 16 in api/config.py) so a
        # worst-case tick can't starve the pool: 20 + 10 overflow = 30, leaving 14 for everyone else.
        _engine = create_async_engine(database_url, pool_size=20, max_overflow=10, pool_pre_ping=True, echo=False)

    if database_url.startswith("sqlite"):

        @event.listens_for(_engine.sync_engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    assert _session_factory is not None, "DB not initialised — call init_db() first"
    async with _session_factory() as session:
        yield session


def get_engine():
    return _engine
