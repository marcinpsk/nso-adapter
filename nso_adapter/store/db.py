# SPDX-License-Identifier: Apache-2.0
"""SQLAlchemy async engine and session factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(database_url: str) -> None:
    """Bind the process-global engine and session factory to *database_url*.

    PostgreSQL only. The store's advisory-lock family fences, ``ON CONFLICT … WHERE``
    pointer upserts, per-request REPEATABLE READ snapshots and every ``timestamptz``
    column assume it, so any other scheme is refused HERE — a misconfiguration must fail
    at startup, not on the first write.
    """
    global _engine, _session_factory
    if not database_url.startswith("postgresql"):
        scheme = database_url.split("://", 1)[0] if "://" in database_url else database_url
        raise ValueError(
            f"database_url must be a PostgreSQL URL (postgresql+asyncpg://…); got {scheme!r}. "
            "The adapter store is PostgreSQL-only."
        )
    # The failover base tick can hold one session per concurrent probe for the full
    # unreachable-probe timeout (~10s), and normal API/sync traffic shares this pool. Keep
    # it comfortably above failover_probe_concurrency (capped at 16 in api/config.py) so a
    # worst-case tick can't starve the pool: 20 + 10 overflow = 30, leaving 14 for everyone else.
    _engine = create_async_engine(database_url, pool_size=20, max_overflow=10, pool_pre_ping=True, echo=False)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    assert _session_factory is not None, "DB not initialised — call init_db() first"
    async with _session_factory() as session:
        yield session


def get_engine():
    return _engine
