# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared helpers for per-migration schema guards.

A migration guard needs a database of its own — the per-test template clone is
already at head, so it cannot be built at an earlier revision. Every helper here
addresses one migration by explicit revision id: naming ``head`` or ``-1`` makes a
guard silently wrong the moment a successor migration lands.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

import sqlalchemy as sa

from tests.conftest import _drop_database, _url_for

REPO_ROOT = Path(__file__).resolve().parents[2]
VERSIONS_DIR = REPO_ROOT / "alembic" / "versions"


def load_migration(filename: str):
    """Import a migration module by path. A missing file is the red evidence."""
    path = VERSIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(f"_migration_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def alembic(sync_url: str, *args: str) -> str:
    """Run alembic in a SUBPROCESS.

    In-process ``command.*`` runs ``alembic/env.py``, whose ``fileConfig`` call
    reconfigures the ROOT logger for the rest of the pytest session.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        env={**os.environ, "DATABASE_URL": sync_url},
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"alembic {args} failed:\n{proc.stdout.decode()}\n{proc.stderr.decode()}")
    return proc.stdout.decode()


def assert_single_head_containing(revision: str) -> None:
    """The revision graph has exactly one head and *revision* is in its ancestry.

    Asserted on the graph rather than on "is this the tip", so a legitimate successor
    migration does not break the guard. Membership in ``alembic heads`` would, and it
    never rejected a two-head split either — which is the failure that actually matters,
    because ``upgrade head`` refuses outright once the graph forks.

    ``ScriptDirectory`` reads alembic.ini and the versions directory only; it never runs
    ``env.py``.
    """
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(Config(str(REPO_ROOT / "alembic.ini")))
    heads = script.get_heads()
    assert len(heads) == 1, f"the revision graph has {len(heads)} heads: {heads}"
    ancestry = {rev.revision for rev in script.walk_revisions("base", heads[0])}
    assert revision in ancestry, f"{revision} is not an ancestor of head {heads[0]}"


@contextmanager
def private_database(pg_provisioner, tag: str):
    """A database of our own, at no particular revision."""
    name = f"nsoadp_{tag}_{uuid.uuid4().hex[:8]}"
    with pg_provisioner.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
    try:
        yield _url_for(name, driver="postgresql+psycopg2")
    finally:
        _drop_database(pg_provisioner, name, expect_clean=False)


@contextmanager
def engine_on(sync_url: str):
    engine = sa.create_engine(sync_url, poolclass=sa.pool.NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def index_predicates(engine, table: str) -> dict[str, tuple[tuple, bool, str | None]]:
    """``{name: (columns-or-expressions, unique, partial predicate)}``."""
    insp = sa.inspect(engine)
    return {
        i["name"]: (
            tuple(i.get("expressions") or i["column_names"]),
            i["unique"],
            (i.get("dialect_options") or {}).get("postgresql_where"),
        )
        for i in insp.get_indexes(table)
    }


_DELETE_RULES_SQL = sa.text(
    """
    SELECT kcu.column_name, rc.delete_rule
      FROM information_schema.referential_constraints rc
      JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = rc.constraint_name
       AND kcu.constraint_schema = rc.constraint_schema
     WHERE kcu.table_name = :table AND kcu.table_schema = 'public'
    """
)


def delete_rules(engine, table: str) -> dict[str, str]:
    """FK delete actions read from information_schema, not from the model.

    A DDL assertion against the model would pass against a restrictive FK that then
    makes offboard raise instead of removing the child rows.
    """
    with engine.connect() as conn:
        return {column: rule for column, rule in conn.execute(_DELETE_RULES_SQL, {"table": table})}
