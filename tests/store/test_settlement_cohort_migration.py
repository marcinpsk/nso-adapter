# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guards for marking-split deployment settlement cohorts."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "c8e2a4f91d63_deployment_generation_settlement_cohort.py"
_SEQUENCE = "deployment_generation_settlement_cohort_seq"


def _module():
    return load_migration(_MIGRATION)


def test_the_settlement_cohort_migration_chains_off_the_previous_head():
    module = _module()
    assert module.down_revision == "b7d5e2f18c40"
    assert_single_head_containing(module.revision)


def test_the_settlement_cohort_is_a_nullable_bigint_with_its_allocator(pg_admin):
    module = _module()
    with private_database(pg_admin, "settlecohort") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            column = {c["name"]: c for c in inspector.get_columns("deployment_generation")}["settlement_cohort"]
            assert isinstance(column["type"], sa.BigInteger)
            assert column["nullable"] is True
            assert column["default"] is None
            assert inspector.has_sequence(_SEQUENCE)


def test_the_settlement_cohort_migration_is_reversible(pg_admin):
    module = _module()
    with private_database(pg_admin, "settlecohortrev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            names = {c["name"] for c in inspector.get_columns("deployment_generation")}
            assert "settlement_cohort" not in names
            assert not inspector.has_sequence(_SEQUENCE)
        alembic(sync_url, "upgrade", module.revision)
