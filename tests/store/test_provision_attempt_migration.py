# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guards for the provision attempt id on jobs (#1732)."""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "d1a7c3e9f2b5_job_provision_attempt_id.py"
_INSERT_JOB = sa.text(
    "INSERT INTO jobs (job_type, status, coalescible, device_id, provision_attempt_id, created_at, updated_at) "
    "VALUES (CAST(:job_type AS jobtype), 'succeeded', false, NULL, :attempt, now(), now())"
)


def _module():
    return load_migration(_MIGRATION)


def test_the_provision_attempt_migration_chains_off_the_previous_head():
    module = _module()
    assert module.down_revision == "c6e8a1f42d90"
    assert_single_head_containing(module.revision)


def test_existing_provisions_upgrade_without_an_attempt(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "provattempt") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO jobs (job_type, status, coalescible, device_id, created_at, updated_at) "
                    "VALUES ('provision', 'succeeded', false, NULL, now(), now())"
                )
            )

        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine, engine.connect() as conn:
            assert conn.scalar(sa.text("SELECT provision_attempt_id FROM jobs")) is None


def test_the_attempt_id_is_unique_and_only_on_provisions(pg_provisioner):
    module = _module()
    attempt = uuid.uuid4()
    with private_database(pg_provisioner, "provattemptuq") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            with engine.begin() as conn:
                conn.execute(_INSERT_JOB, {"job_type": "provision", "attempt": attempt})
            with pytest.raises(sa.exc.IntegrityError, match="uq_job_provision_attempt_id"), engine.begin() as conn:
                conn.execute(_INSERT_JOB, {"job_type": "provision", "attempt": attempt})
            with pytest.raises(sa.exc.IntegrityError, match="ck_job_attempt_id_only_on_provision"):
                with engine.begin() as conn:
                    conn.execute(_INSERT_JOB, {"job_type": "sync", "attempt": uuid.uuid4()})


def test_the_provision_attempt_migration_is_reversible(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "provattemptrev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert "provision_attempt_id" not in {column["name"] for column in sa.inspect(engine).get_columns("jobs")}
        alembic(sync_url, "upgrade", module.revision)
