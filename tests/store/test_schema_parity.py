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

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect

from tests.conftest import _drop_database, _url_for

_REPO_ROOT = Path(__file__).resolve().parents[2]


_DEFERRABILITY_SQL = sa.text(
    """
    SELECT c.conname, c.condeferrable, c.condeferred
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
     WHERE c.contype = 'u' AND n.nspname = 'public'
       AND t.relname = :table
    """
)
_JOB_TRIGGER_SQL = sa.text(
    """
    SELECT t.tgname, pg_get_triggerdef(t.oid, true), p.proname, p.prosrc
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_proc p ON p.oid = t.tgfoid
     WHERE n.nspname = 'public'
       AND c.relname = 'jobs'
       AND t.tgname = 'job_coalescible_immutable'
       AND NOT t.tgisinternal
    """
)


def _deferrability(conn, table: str) -> dict[str, tuple[bool, str]]:
    """Unique-constraint deferrability, which SQLAlchemy does not reflect."""
    return {
        name: (deferrable, "DEFERRED" if deferred else "IMMEDIATE")
        for name, deferrable, deferred in conn.execute(_DEFERRABILITY_SQL, {"table": table})
    }


def _snapshot(engine) -> dict:
    insp = inspect(engine)
    snap: dict = {}
    with engine.connect() as conn:
        for table in sorted(insp.get_table_names()):
            if table == "alembic_version":
                continue
            deferrability = _deferrability(conn, table)
            snap[table] = {
                # (type, nullable, server_default) — the server default is included so a
                # create_all-vs-alembic DEFAULT divergence is caught, not passed green.
                "cols": {c["name"]: (str(c["type"]), c["nullable"], c.get("default")) for c in insp.get_columns(table)},
                "pk": tuple(insp.get_pk_constraint(table)["constrained_columns"]),
                # ondelete normalized (upper-cased, None -> "NO ACTION"): alembic emitting a
                # restrictive FK where create_all emits CASCADE otherwise passes green, and
                # offboard then raises instead of removing the child rows.
                "fks": sorted(
                    (
                        tuple(f["constrained_columns"]),
                        f["referred_table"],
                        tuple(f["referred_columns"]),
                        ((f.get("options") or {}).get("ondelete") or "NO ACTION").upper(),
                    )
                    for f in insp.get_foreign_keys(table)
                ),
                # Deferrability is part of the constraint: an immediate twin of a
                # DEFERRABLE INITIALLY DEFERRED constraint rejects legal in-transaction swaps.
                "uqs": sorted(
                    (tuple(u["column_names"]), *deferrability.get(u["name"], (False, "IMMEDIATE")))
                    for u in insp.get_unique_constraints(table)
                ),
                # The partial-index PREDICATE is part of the index: without it a
                # postgresql_where divergence between the model and the migration passes green.
                # An EXPRESSION index reflects column_names as None entries, so a wrong-column
                # twin is invisible without the reflected expressions.
                "ixs": sorted(
                    (
                        tuple(i.get("expressions") or i["column_names"]),
                        i["unique"],
                        (i.get("dialect_options") or {}).get("postgresql_where"),
                    )
                    for i in insp.get_indexes(table)
                ),
                # CHECK constraints compared by their reflected SQL text (names may be generated).
                "checks": sorted(c["sqltext"] for c in insp.get_check_constraints(table)),
            }
        snap["__job_queue_class_trigger__"] = tuple(conn.execute(_JOB_TRIGGER_SQL).one())
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


def _assert_job_queue_class_schema(snapshot: dict) -> None:
    jobs = snapshot["jobs"]
    assert jobs["cols"]["coalescible"] == ("BOOLEAN", False, None)
    assert (
        ("device_id", "job_type"),
        True,
        "((status = 'queued'::jobstatus) AND coalescible)",
    ) in jobs["ixs"]
    checks = jobs["checks"]
    assert len(checks) == 4
    assert sum("removal" in condition and "coalescible" in condition for condition in checks) == 1
    assert sum("provision" in condition and "coalescible" in condition for condition in checks) == 1
    assert sum("provision" in condition and "device_id IS NULL" in condition for condition in checks) == 1
    assert sum("provision" in condition and "device_id IS NOT NULL" in condition for condition in checks) == 1
    trigger_name, trigger_definition, function_name, function_source = snapshot["__job_queue_class_trigger__"]
    assert trigger_name == "job_coalescible_immutable"
    assert "BEFORE UPDATE OF coalescible ON jobs" in trigger_definition
    assert "new.coalescible IS DISTINCT FROM old.coalescible" in trigger_definition
    assert function_name == "job_reject_coalescible_rewrite"
    assert "coalescible may not be updated" in function_source


def test_alembic_baseline_matches_create_all(pg_provisioner):
    suffix = uuid.uuid4().hex[:10]
    ca_db = f"parity_ca_{suffix}"
    al_db = f"parity_al_{suffix}"

    with pg_provisioner.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{ca_db}"')
        conn.exec_driver_sql(f'CREATE DATABASE "{al_db}"')
    ca_engine = None
    al_engine = None
    try:
        ca_url = _url_for(ca_db, driver="postgresql+psycopg2")
        al_url = _url_for(al_db, driver="postgresql+psycopg2")

        from nso_adapter.store.models import Base

        ca_engine = sa.create_engine(ca_url, connect_args={"application_name": "tests.schema_parity.create_all"})
        Base.metadata.create_all(ca_engine)

        _alembic_upgrade_head(al_url)
        al_engine = sa.create_engine(al_url, connect_args={"application_name": "tests.schema_parity.alembic"})

        ca_snap = _snapshot(ca_engine)
        al_snap = _snapshot(al_engine)

        assert set(ca_snap) == set(al_snap), (
            f"table set differs: only_create_all={set(ca_snap) - set(al_snap)} "
            f"only_alembic={set(al_snap) - set(ca_snap)}"
        )
        for table in sorted(ca_snap):
            assert ca_snap[table] == al_snap[table], (
                f"schema mismatch in {table!r}:\n  create_all={ca_snap[table]}\n  alembic   ={al_snap[table]}"
            )
        _assert_job_queue_class_schema(ca_snap)
    finally:
        if ca_engine is not None:
            ca_engine.dispose()
        if al_engine is not None:
            al_engine.dispose()
        for dbname in (ca_db, al_db):
            _drop_database(pg_provisioner, dbname, expect_clean=False)


def test_schema_parity_disposes_both_engines_when_snapshot_fails(pg_provisioner, monkeypatch):
    """A comparison failure must release both databases before cleanup starts."""
    from tests import conftest as root_conftest

    module = sys.modules[__name__]
    real_snapshot = _snapshot
    real_drop_database = _drop_database
    snapshot_calls = 0
    dropped_databases: list[str] = []

    def fail_after_snapshot(engine):
        nonlocal snapshot_calls
        snapshot_calls += 1
        snapshot = real_snapshot(engine)
        if snapshot_calls == 2:
            raise RuntimeError("schema comparison probe")
        return snapshot

    def require_clean_then_drop(provisioner, name: str, *, expect_clean: bool) -> None:
        dropped_databases.append(name)
        real_drop_database(provisioner, name, expect_clean=True)

    monkeypatch.setattr(module, "_snapshot", fail_after_snapshot)
    monkeypatch.setattr(module, "_drop_database", require_clean_then_drop)
    monkeypatch.setattr(root_conftest, "STRICT_TEARDOWN", True)

    with pytest.raises(RuntimeError, match="schema comparison probe"):
        test_alembic_baseline_matches_create_all(pg_provisioner)

    assert len(dropped_databases) == 2
