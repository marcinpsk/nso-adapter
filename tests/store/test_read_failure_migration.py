# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guard for persisted read-failure classification."""

from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "f3b8d1e7a9c2_read_outcome_failures.py"


def _module():
    return load_migration(_MIGRATION)


def test_design_record_names_the_jsonb_whitelist_projection():
    design = (Path(__file__).parents[2] / "docs/design/redistribution-mixed-failure-metadata.md").read_text()
    mechanical_guard = design.split("### Mechanical guard", 1)[1].split("### Candidate shapes", 1)[0]

    assert "nullable `RefreshOutcome.read_failures` JSONB array" in mechanical_guard
    assert "dedicated nullable columns" not in mechanical_guard


def test_read_failure_column_upgrade_and_downgrade(pg_provisioner):
    module = _module()
    assert module.down_revision == "c7b4e1d90f28"
    assert_single_head_containing(module.revision)

    with private_database(pg_provisioner, "read_failure") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            before = {column["name"] for column in sa.inspect(engine).get_columns("refresh_outcome")}
            assert "read_failures" not in before

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            columns = {column["name"]: column for column in sa.inspect(engine).get_columns("refresh_outcome")}
            assert isinstance(columns["read_failures"]["type"], postgresql.JSONB)
            assert columns["read_failures"]["nullable"] is True

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            after = {column["name"] for column in sa.inspect(engine).get_columns("refresh_outcome")}
            assert after == before
