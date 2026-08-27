# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Discrimination tests for ``test_schema_parity._snapshot``.

The parity test is the only gate proving ``create_all ≡ alembic``. A field it does
not record is a divergence it passes green, so each field the snapshot claims to
cover needs a fixture where the two schemas differ in exactly that field and
nothing else. These three were blind before #1396 R1a: FK ``ondelete``, unique
constraint deferrability, and index expressions.
"""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa

from tests.conftest import _drop_database, _url_for
from tests.store.test_schema_parity import _snapshot


@pytest.fixture
def two_databases(pg_provisioner):
    """Two empty throwaway databases; yields a helper that snapshots arbitrary DDL."""
    suffix = uuid.uuid4().hex[:10]
    names = [f"snapdiff_a_{suffix}", f"snapdiff_b_{suffix}"]
    with pg_provisioner.connect() as conn:
        for name in names:
            conn.exec_driver_sql(f'CREATE DATABASE "{name}"')

    def snapshot_of(dbname: str, ddl: list[str]) -> dict:
        engine = sa.create_engine(_url_for(dbname, driver="postgresql+psycopg2"))
        try:
            with engine.begin() as conn:
                for stmt in ddl:
                    conn.exec_driver_sql(stmt)
            return _snapshot(engine)
        finally:
            engine.dispose()

    try:
        yield names, snapshot_of
    finally:
        for name in names:
            _drop_database(pg_provisioner, name, expect_clean=False)


_PARENT = "CREATE TABLE parent (id INTEGER PRIMARY KEY)"


def test_snapshot_discriminates_fk_ondelete(two_databases):
    """CASCADE vs the default restrictive action must not compare equal (B9).

    Alembic emitting a plain FK where ``create_all`` emits ``ON DELETE CASCADE``
    used to pass green — and offboard then raises instead of removing the child.
    """
    (db_a, db_b), snapshot_of = two_databases
    a = snapshot_of(
        db_a,
        [
            _PARENT,
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        ],
    )
    b = snapshot_of(
        db_b,
        [_PARENT, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))"],
    )
    assert a["child"]["fks"] != b["child"]["fks"]


def test_snapshot_normalizes_missing_ondelete(two_databases):
    """``None`` and an explicit ``NO ACTION`` are the same action, not a diff.

    Without the normalization the two build paths' reflection differences would
    produce spurious failures.
    """
    (db_a, db_b), snapshot_of = two_databases
    a = snapshot_of(
        db_a,
        [_PARENT, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))"],
    )
    b = snapshot_of(
        db_b,
        [
            _PARENT,
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE NO ACTION)",
        ],
    )
    assert a["child"]["fks"] == b["child"]["fks"]


def test_snapshot_discriminates_constraint_deferrability(two_databases):
    """``DEFERRABLE INITIALLY DEFERRED`` vs immediate must not compare equal.

    §3.7 mandates the deferrable identity constraint; an immediate twin rejects
    legal same-payload swaps and reclaims.
    """
    (db_a, db_b), snapshot_of = two_databases
    a = snapshot_of(
        db_a,
        [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, CONSTRAINT uq_t_k UNIQUE (k) DEFERRABLE INITIALLY DEFERRED)"
        ],
    )
    b = snapshot_of(db_b, ["CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, CONSTRAINT uq_t_k UNIQUE (k))"])
    assert a["t"]["uqs"] != b["t"]["uqs"]


def test_snapshot_discriminates_index_expressions(two_databases):
    """Two expression indexes over DIFFERENT expressions must not compare equal.

    An expression index reflects ``column_names`` as ``None`` entries, so a
    wrong-column twin was indistinguishable before the ``expressions`` fallback.
    """
    (db_a, db_b), snapshot_of = two_databases
    ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, ctx JSON)"
    a = snapshot_of(db_a, [ddl, "CREATE INDEX ix_t_expr ON t ((ctx->>'alpha'))"])
    b = snapshot_of(db_b, [ddl, "CREATE INDEX ix_t_expr ON t ((ctx->>'beta'))"])
    assert a["t"]["ixs"] != b["t"]["ixs"]
