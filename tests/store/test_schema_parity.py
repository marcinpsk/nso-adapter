# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Parity test: ``Base.metadata`` must match the alembic head.

The test suite builds its per-test database by cloning a template built with
``alembic upgrade head`` — the schema production runs. ``create_all`` therefore has
exactly one consumer left: this test, whose job is proving the two agree.

It creates two throwaway databases on the test server, builds one with ``create_all``
and one with ``alembic upgrade head``, diffs the reflected schema, and drops both.
It never skips: a schema divergence that only CI could see is the failure mode this
test exists to remove.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import inspect

from tests.conftest import _drop_database, _url_for

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _snapshot(engine) -> dict:
    insp = inspect(engine)
    snap: dict = {}
    for table in sorted(insp.get_table_names()):
        if table == "alembic_version":
            continue
        snap[table] = {
            # (type, nullable, server_default) — the server default is included so a
            # create_all-vs-alembic DEFAULT divergence is caught, not passed green.
            "cols": {c["name"]: (str(c["type"]), c["nullable"], c.get("default")) for c in insp.get_columns(table)},
            "pk": tuple(insp.get_pk_constraint(table)["constrained_columns"]),
            "fks": sorted(
                (tuple(f["constrained_columns"]), f["referred_table"], tuple(f["referred_columns"]))
                for f in insp.get_foreign_keys(table)
            ),
            "uqs": sorted(tuple(u["column_names"]) for u in insp.get_unique_constraints(table)),
            # The partial-index PREDICATE is part of the index: without it a
            # postgresql_where divergence between the model and the migration passes green.
            "ixs": sorted(
                (tuple(i["column_names"]), i["unique"], (i.get("dialect_options") or {}).get("postgresql_where"))
                for i in insp.get_indexes(table)
            ),
            # CHECK constraints compared by their reflected SQL text (names may be generated).
            "checks": sorted(c["sqltext"] for c in insp.get_check_constraints(table)),
        }
    # PostgreSQL ENUM types are schema-level, not per-table: compare their label sets so an
    # enum value-set divergence (a migration adding/renaming a member) is caught too.
    snap["__enums__"] = {e["name"]: tuple(e["labels"]) for e in insp.get_enums()}
    return snap


def _alembic_upgrade_head(db_url: str) -> None:
    """Run the production migration entry point in a SUBPROCESS.

    In-process ``command.upgrade`` runs ``alembic/env.py``, whose ``fileConfig`` call
    reconfigures the ROOT logger for the rest of the pytest session (N13a).
    """
    proc = subprocess.run(
        [sys.executable, "-m", "nso_adapter.db_migrate"],
        cwd=_REPO_ROOT,
        capture_output=True,
        env={**os.environ, "DATABASE_URL": db_url},
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"alembic upgrade head failed:\n{proc.stdout.decode()}\n{proc.stderr.decode()}")


def test_alembic_baseline_matches_create_all(pg_admin):
    suffix = uuid.uuid4().hex[:10]
    ca_db = f"parity_ca_{suffix}"
    al_db = f"parity_al_{suffix}"

    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{ca_db}"')
        conn.exec_driver_sql(f'CREATE DATABASE "{al_db}"')
    try:
        ca_url = _url_for(ca_db, driver="postgresql+psycopg2")
        al_url = _url_for(al_db, driver="postgresql+psycopg2")

        from nso_adapter.store.models import Base

        ca_engine = sa.create_engine(ca_url)
        Base.metadata.create_all(ca_engine)

        _alembic_upgrade_head(al_url)
        al_engine = sa.create_engine(al_url)

        ca_snap = _snapshot(ca_engine)
        al_snap = _snapshot(al_engine)
        ca_engine.dispose()
        al_engine.dispose()

        assert set(ca_snap) == set(al_snap), (
            f"table set differs: only_create_all={set(ca_snap) - set(al_snap)} "
            f"only_alembic={set(al_snap) - set(ca_snap)}"
        )
        for table in sorted(ca_snap):
            assert ca_snap[table] == al_snap[table], (
                f"schema mismatch in {table!r}:\n  create_all={ca_snap[table]}\n  alembic   ={al_snap[table]}"
            )
    finally:
        for dbname in (ca_db, al_db):
            _drop_database(pg_admin, dbname, expect_clean=False)
