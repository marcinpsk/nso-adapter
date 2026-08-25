# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Contract tests for ``tests/conftest.py`` — its shared seeding helpers, and where fixtures live.

The helpers are used by hundreds of tests, so a helper that only works under the store's
CURRENT session settings turns one settings change into a suite-wide failure with no
bearing on the behaviour under test.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import get_args, get_type_hints

from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.store import db as store_db
from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import _client_backends, seed_device, session, start_job

_TESTS_ROOT = Path(__file__).resolve().parent


async def test_start_job_returns_the_attempt_under_an_expiring_session(store_engine, monkeypatch):
    """``start_job`` must not read the Job row after its own commit.

    The store factory sets ``expire_on_commit=False`` today. With expiry ON (SQLAlchemy's
    default), the committed instance is expired and reading ``run_attempt`` afterwards
    starts a lazy load outside the greenlet: ``MissingGreenlet``, not an attempt.
    """
    async with session() as db:
        job = Job(job_type=JobType.provision, device_id=None, status=JobStatus.queued, context={})
        db.add(job)
        await db.commit()
        job_id = job.id

    monkeypatch.setattr(store_db, "_session_factory", async_sessionmaker(store_engine, expire_on_commit=True))
    assert await start_job(job_id) == 1

    async with session() as db:
        started = await db.get(Job, job_id)
        assert started.status is JobStatus.running and started.run_attempt == 1


async def test_client_backend_diagnostics_exclude_query_text(store_engine, pg_admin):
    """A leaked session's last statement can contain a secret literal."""
    async with store_engine.connect() as leaked:
        await leaked.exec_driver_sql("SELECT 'review-secret-literal'")
        with pg_admin.connect() as admin:
            rows = _client_backends(admin, store_engine.url.database)

    assert rows
    assert all(len(row) == 2 for row in rows)
    assert "review-secret-literal" not in repr(rows)


def test_seed_device_type_contract_allows_no_netbox_identity():
    assert type(None) in get_args(get_type_hints(seed_device)["netbox_device_id"])


def _fixtures_defined_in(path: Path) -> list[str]:
    """Every ``@pytest.fixture``-decorated definition in *path*."""
    names: list[str] = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if isinstance(target, ast.Attribute) and target.attr == "fixture":
                names.append(node.name)
            elif isinstance(target, ast.Name) and target.id == "fixture":
                names.append(node.name)
    return names


def test_only_the_root_conftest_defines_fixtures():
    """A fixture below ``tests/conftest.py`` disappears on some argument orders.

    pytest parses a conftest's fixtures once, against the Directory node collected at that moment.
    An argument list that leaves the conftest's directory and comes back — ``pytest tests/core/a.py
    tests/b.py tests/core/c.py`` — rebuilds that node afterwards, and every fixture defined there
    resolves against the stale one: ``fixture 'x' not found`` at SETUP, only in subsets nobody runs
    deliberately. The root conftest's node is the one pytest never rebuilds, so fixtures live there.
    """
    offenders = {
        str(path.relative_to(_TESTS_ROOT.parent)): _fixtures_defined_in(path)
        for path in sorted(_TESTS_ROOT.rglob("conftest.py"))
        if path != _TESTS_ROOT / "conftest.py" and _fixtures_defined_in(path)
    }
    assert not offenders, (
        f"Fixtures defined in a non-root conftest: {offenders}\n\n"
        "Move them to tests/conftest.py — a subdirectory conftest's fixtures go missing when the "
        "argument list re-enters its directory."
    )
