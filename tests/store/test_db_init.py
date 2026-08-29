# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The two PostgreSQL-only entry points: ``init_db`` and the migration runner.

The store is PostgreSQL-only by construction: advisory-lock family fences,
``ON CONFLICT … WHERE`` pointer upserts, REPEATABLE READ read snapshots and every
``timestamptz`` column assume it. Until #1329 the family fence carried a dialect check
that incidentally aborted a wrong-engine run — mid-refresh, after the app was already
serving. With that check gone the URL is rejected at bind time instead.

``init_db`` alone is not enough: ``scripts/docker-entrypoint.sh`` runs
``python -m nso_adapter.db_migrate`` BEFORE the app ever calls ``init_db``, so a
wrong ``DATABASE_URL`` would reach alembic first and die partway through the chain,
having already executed DDL on the wrong engine. Both entry points share one validator.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from nso_adapter.store import db as store_db

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_ROOT = _REPO_ROOT / "nso_adapter"

_RETIRED_URL = "sqlite+aiosqlite:///tmp/x.db"  # a rejection fixture; the store is never run against sqlite
_RETIRED_SCHEME = _RETIRED_URL.split("://", 1)[0]
# The SYNC spelling of the same retired driver. Alembic builds sync engines, and this one
# is backed by the stdlib, so it stays installable-by-default even with the async driver
# uninstalled — i.e. it can really connect and really execute DDL. That is what makes the
# migration-runner test below a genuine tripwire rather than a "driver missing" accident.
_RETIRED_SYNC_SCHEME = _RETIRED_SCHEME.split("+")[0]


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


def test_internal_store_sessions_use_the_context_manager():
    """Internal callers must close sessions before they return or break."""
    offenders = []
    for path in sorted(_SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and ast.unparse(node.func).endswith("get_session"):
                offenders.append(f"{path.relative_to(_REPO_ROOT)}:{node.lineno}")

    assert not offenders, f"Internal code calls the FastAPI session dependency: {offenders}"


# ── the migration runner: the container entrypoint's FIRST database contact ──────────────


def test_db_migrate_rejects_a_non_postgresql_url_before_touching_the_database(tmp_path):
    """`python -m nso_adapter.db_migrate` — the literal entrypoint command — must refuse a
    wrong DATABASE_URL *before* alembic opens a connection.

    Driven as a subprocess so it exercises the real module-main path the container runs.
    The store file is the DDL tripwire: the retired driver CREATES it on connect, so its
    absence proves no engine was ever opened, let alone a migration executed.
    """
    store_file = tmp_path / "wrong-engine.db"
    proc = subprocess.run(
        [sys.executable, "-m", "nso_adapter.db_migrate"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "DATABASE_URL": f"{_RETIRED_SYNC_SCHEME}:///{store_file}"},
    )
    output = proc.stdout + proc.stderr

    assert proc.returncode != 0, f"the wrong engine was accepted:\n{output}"
    assert "ValueError" in output, f"expected the shared validator to raise:\n{output}"
    assert _RETIRED_SYNC_SCHEME in output, "the error must name the scheme it rejected"
    assert not store_file.exists(), "a database file was created — alembic connected before the check"
    assert "Running upgrade" not in output, f"alembic started the chain before the check:\n{output}"


def test_db_migrate_and_init_db_share_one_validator():
    """Both entry points must reject identically — two copies would drift."""
    from nso_adapter.store.db import require_postgresql_url

    with pytest.raises(ValueError) as via_helper:
        require_postgresql_url(_RETIRED_URL)
    with pytest.raises(ValueError) as via_init:
        store_db.init_db(_RETIRED_URL)
    assert str(via_helper.value) == str(via_init.value)
    assert require_postgresql_url("postgresql+asyncpg://u:p@h/db") == "postgresql+asyncpg://u:p@h/db"
