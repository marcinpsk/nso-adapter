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
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
    _engine = create_async_engine(database_url, connect_args=connect_args, echo=False)

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
