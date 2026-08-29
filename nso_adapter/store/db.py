# SPDX-License-Identifier: Apache-2.0
"""SQLAlchemy async engine and session factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, cast

from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.dml import UpdateBase

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None

# Server-enforced bounds for a transaction running inside a cancellation-absorbing span.
#
# Those spans are the reason the worker's cancel-drain can be bounded at all. Being
# "DB-only" does NOT make one bounded: the family fence takes
# ``SELECT pg_advisory_xact_lock(...)``, and PostgreSQL waits for a conflicting lock
# indefinitely by default. A client-side ``wait_for`` around it only reproduces the
# cancellation-completion problem one level down, so the bound has to come from the server.
#
# Sized BELOW the span's own child bound (absorb 5s + drain 2s = 7s), so the server always
# wins inside the span and the outer drain never waits on something the server would not
# have ended. Applied with SET LOCAL: scoped to the transaction, needing no reset before the
# connection returns to the pool.
ABSORBED_SPAN_LOCK_TIMEOUT_MS = 3_000
ABSORBED_SPAN_STATEMENT_TIMEOUT_MS = 4_000


async def execute_dml(db: AsyncSession, statement: UpdateBase) -> CursorResult[Any]:
    """Execute one DML statement and return its row-count-bearing result."""
    return cast(CursorResult[Any], await db.execute(statement))


async def apply_absorbed_span_bounds(db: AsyncSession) -> None:
    """Bound every lock wait and statement in this transaction, at the server.

    Call at the entry of any transaction reachable inside a cancellation-absorbing span.
    Idempotent and cheap — two SET LOCAL statements.
    """
    from sqlalchemy import text

    await db.execute(text(f"SET LOCAL lock_timeout = '{ABSORBED_SPAN_LOCK_TIMEOUT_MS}ms'"))
    await db.execute(text(f"SET LOCAL statement_timeout = '{ABSORBED_SPAN_STATEMENT_TIMEOUT_MS}ms'"))


def require_postgresql_url(database_url: str, *, label: str = "database_url") -> str:
    """Return *database_url* unchanged, or raise ``ValueError`` naming the rejected scheme.

    PostgreSQL only. The store's advisory-lock family fences, ``ON CONFLICT … WHERE``
    pointer upserts, per-request REPEATABLE READ snapshots and every ``timestamptz``
    column assume it. This is the ONE validator: both database entry points call it —
    :func:`init_db` for the app, and the migration runner for the container entrypoint,
    which reaches the database FIRST and would otherwise execute DDL on the wrong engine
    before failing partway through the chain.
    """
    if not database_url.startswith("postgresql"):
        scheme = database_url.split("://", 1)[0] if "://" in database_url else database_url
        raise ValueError(
            f"{label} must be a PostgreSQL URL (postgresql+asyncpg://…); got {scheme!r}. "
            "The adapter store is PostgreSQL-only."
        )
    return database_url


def init_db(database_url: str, *, application_name: str = "nso-adapter.store") -> None:
    """Bind the process-global engine and session factory to *database_url*.

    Refuses any non-PostgreSQL scheme up front, so a misconfiguration fails at startup
    rather than on the first write.
    """
    global _engine, _session_factory
    require_postgresql_url(database_url)
    # The failover base tick can hold one session per concurrent probe for the full
    # unreachable-probe timeout (~10s), and normal API/sync traffic shares this pool. Keep
    # it comfortably above failover_probe_concurrency (capped at 16 in api/config.py) so a
    # worst-case tick can't starve the pool: 20 + 10 overflow = 30, leaving 14 for everyone else.
    _engine = create_async_engine(
        database_url,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
        connect_args={"server_settings": {"application_name": application_name}},
    )
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    assert _session_factory is not None, "DB not initialised — call init_db() first"
    async with _session_factory() as session:
        yield session


def get_engine():
    return _engine
