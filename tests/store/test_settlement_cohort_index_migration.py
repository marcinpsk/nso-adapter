# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guards for the global settlement-cohort lookup index."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "e4a8c2d1f6b9_generation_settlement_cohort_index.py"
_INDEX = "ix_generation_settlement_cohort"


def _module():
    return load_migration(_MIGRATION)


def _indexes(sync_url: str) -> dict[str, dict]:
    with engine_on(sync_url) as engine:
        return {index["name"]: index for index in sa.inspect(engine).get_indexes("deployment_generation")}


def test_the_settlement_cohort_index_migration_chains_off_the_previous_head():
    module = _module()
    assert module.down_revision == "c8e2a4f91d63"
    assert_single_head_containing(module.revision)


def test_the_settlement_cohort_index_leads_with_the_global_cohort(pg_admin):
    module = _module()
    with private_database(pg_admin, "settlecohortindex") as sync_url:
        alembic(sync_url, "upgrade", module.revision)

        index = _indexes(sync_url)[_INDEX]
        assert index["column_names"] == ["settlement_cohort"]
        assert "settlement_cohort IS NOT NULL" in str(index["dialect_options"]["postgresql_where"])


def test_the_settlement_cohort_index_migration_is_reversible(pg_admin):
    module = _module()
    with private_database(pg_admin, "settlecohortindexrev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        assert _INDEX not in _indexes(sync_url)
        alembic(sync_url, "upgrade", module.revision)
        assert _INDEX in _indexes(sync_url)
