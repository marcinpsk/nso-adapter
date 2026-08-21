# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the deployment-generation immutability trigger."""

from __future__ import annotations

import os
import time
from concurrent.futures import Future, ThreadPoolExecutor

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, load_migration, private_database

_MIGRATION = "a4e1c7b09f52_deployment_generations.py"
_SETTLEMENT_COHORT_MIGRATION = "c8e2a4f91d63_deployment_generation_settlement_cohort.py"
_FUNCTION = "deployment_generation_reject_rewrite"
_FROZEN_FUNCTION_SOURCE = """
BEGIN
    IF NEW.device_id IS DISTINCT FROM OLD.device_id
        OR NEW.seq IS DISTINCT FROM OLD.seq
        OR NEW.mode IS DISTINCT FROM OLD.mode
        OR NEW.document::text IS DISTINCT FROM OLD.document::text
        OR NEW.digest IS DISTINCT FROM OLD.digest
        OR NEW.allowed_removal_keys::text IS DISTINCT FROM OLD.allowed_removal_keys::text
        OR NEW.source_push_seq::text IS DISTINCT FROM OLD.source_push_seq::text
        OR NEW.stream_revisions::text IS DISTINCT FROM OLD.stream_revisions::text
        OR NEW.removal_context::text IS DISTINCT FROM OLD.removal_context::text
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, allowed_removal_keys, source_push_seq, stream_revisions, removal_context, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
"""
_FROZEN_SETTLEMENT_COHORT_FUNCTION_SOURCE = """
BEGIN
    IF NEW.device_id IS DISTINCT FROM OLD.device_id
        OR NEW.seq IS DISTINCT FROM OLD.seq
        OR NEW.mode IS DISTINCT FROM OLD.mode
        OR NEW.document::text IS DISTINCT FROM OLD.document::text
        OR NEW.digest IS DISTINCT FROM OLD.digest
        OR NEW.allowed_removal_keys::text IS DISTINCT FROM OLD.allowed_removal_keys::text
        OR NEW.source_push_seq::text IS DISTINCT FROM OLD.source_push_seq::text
        OR NEW.stream_revisions::text IS DISTINCT FROM OLD.stream_revisions::text
        OR NEW.removal_context::text IS DISTINCT FROM OLD.removal_context::text
        OR NEW.settlement_cohort::text IS DISTINCT FROM OLD.settlement_cohort::text
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, allowed_removal_keys, source_push_seq, stream_revisions, removal_context, settlement_cohort, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
"""

_FUNCTION_SOURCE_SQL = sa.text(
    """
    SELECT p.prosrc
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'public'
       AND p.proname = :name
       AND pg_get_function_identity_arguments(p.oid) = ''
    """
)


def _installed_function_source(engine) -> str:
    with engine.connect() as conn:
        return conn.execute(_FUNCTION_SOURCE_SQL, {"name": _FUNCTION}).scalar_one()


def _function_source(statement: str) -> str:
    return statement.partition(" AS $$")[2].partition("$$ LANGUAGE plpgsql")[0]


def _wait_until_migration_reaches_jobs(engine, upgrade: Future[str]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            waiting = conn.execute(
                sa.text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM pg_locks AS lock
                          JOIN pg_class AS relation ON relation.oid = lock.relation
                          JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                         WHERE lock.database = (SELECT oid FROM pg_database WHERE datname = current_database())
                           AND namespace.nspname = 'public'
                           AND relation.relname = 'jobs'
                           AND NOT lock.granted
                    )
                    """
                )
            ).scalar_one()
        if waiting or upgrade.done():
            return
        time.sleep(0.05)
    raise AssertionError("the migration neither completed nor waited for the jobs table")


def test_upgrade_refuses_active_generationless_removals(pg_provisioner):
    """The operator must drain old removal work before generation execution starts."""
    module = load_migration(_MIGRATION)
    with private_database(pg_provisioner, "generation_removal_gate") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO jobs (job_type, status, context, created_at, updated_at) "
                    "VALUES ('removal', 'queued', CAST('{}' AS json), now(), now())"
                )
            )

        with pytest.raises(AssertionError, match="drain active removal jobs"):
            alembic(sync_url, "upgrade", module.revision)


def test_upgrade_serializes_removal_gate_against_a_concurrent_enqueue(pg_provisioner):
    """A removal committed during cutover must be visible to the quiescence gate."""
    module = load_migration(_MIGRATION)
    with private_database(pg_provisioner, "generation_removal_race") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.connect() as enqueue_conn:
            enqueue_tx = enqueue_conn.begin()
            enqueue_conn.execute(
                sa.text(
                    "INSERT INTO jobs (job_type, status, context, created_at, updated_at) "
                    "VALUES ('removal', 'queued', CAST('{}' AS json), now(), now())"
                )
            )

            with ThreadPoolExecutor(max_workers=1) as pool:
                upgrade = pool.submit(alembic, sync_url, "upgrade", module.revision)
                try:
                    _wait_until_migration_reaches_jobs(engine, upgrade)
                finally:
                    enqueue_tx.commit()

                try:
                    upgrade.result(timeout=30)
                except AssertionError as exc:
                    assert "drain active removal jobs" in str(exc)
                else:
                    with engine.connect() as conn:
                        ungoverned = conn.execute(
                            sa.text(
                                """
                                SELECT count(*)
                                  FROM jobs AS job
                                  LEFT JOIN deployment_generation AS generation ON generation.job_id = job.id
                                 WHERE job.job_type = 'removal'
                                   AND job.status IN ('queued', 'running')
                                   AND generation.id IS NULL
                                """
                            )
                        ).scalar_one()
                    pytest.fail(
                        f"migration committed across a concurrent enqueue and left {ungoverned} "
                        "active removal job(s) without a generation"
                    )


def test_historical_trigger_is_frozen_and_head_trigger_matches_live_ddl(pg_provisioner, tmp_path, monkeypatch):
    """The historical revision must render frozen SQL, not the live helper.

    The injected drifted_column discriminates where the helper reads the module
    tuple at call time; where the helper captured its default tuple before the
    injection, the live settlement_cohort column keeps the frozen-literal check
    discriminating. The marker only proves the subprocess loaded the injection.
    """
    from nso_adapter.store.ddl import generation_immutability_ddl

    module = load_migration(_MIGRATION)
    marker = tmp_path / "historical-sitecustomize-loaded"
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from pathlib import Path\n"
        "from nso_adapter.store import ddl\n"
        f"Path({str(marker)!r}).touch()\n"
        "ddl.GENERATION_IMMUTABLE_COLUMNS += ('drifted_column',)\n"
    )

    with private_database(pg_provisioner, "generation_ddl") as sync_url:
        old_pythonpath = os.environ.get("PYTHONPATH")
        pythonpath = str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
        monkeypatch.setenv("PYTHONPATH", pythonpath)
        alembic(sync_url, "upgrade", module.revision)
        assert marker.exists(), "the Alembic subprocess did not load sitecustomize"

        with engine_on(sync_url) as engine:
            historical_source = _installed_function_source(engine)
        assert historical_source == _FROZEN_FUNCTION_SOURCE
        assert "drifted_column" not in historical_source

        monkeypatch.delenv("PYTHONPATH", raising=False)
        if old_pythonpath is not None:
            monkeypatch.setenv("PYTHONPATH", old_pythonpath)

        alembic(sync_url, "upgrade", "head")

        with engine_on(sync_url) as engine:
            head_source = _installed_function_source(engine)
        assert head_source == _function_source(generation_immutability_ddl()[0])


def test_settlement_cohort_trigger_is_frozen_and_head_trigger_matches_live_ddl(pg_admin, tmp_path, monkeypatch):
    from nso_adapter.store.ddl import generation_immutability_ddl

    module = load_migration(_SETTLEMENT_COHORT_MIGRATION)
    marker = tmp_path / "sitecustomize-loaded"
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from pathlib import Path\n"
        "from nso_adapter.store import ddl\n"
        f"Path({str(marker)!r}).touch()\n"
        "ddl.GENERATION_IMMUTABLE_COLUMNS += ('drifted_column',)\n"
        "ddl.generation_immutability_ddl.__defaults__ = (ddl.GENERATION_IMMUTABLE_COLUMNS,)\n"
    )

    with private_database(pg_admin, "settlement_cohort_generation_ddl") as sync_url:
        old_pythonpath = os.environ.get("PYTHONPATH")
        pythonpath = str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
        monkeypatch.setenv("PYTHONPATH", pythonpath)
        alembic(sync_url, "upgrade", module.revision)
        assert marker.exists(), "the Alembic subprocess did not load the injected sitecustomize"

        with engine_on(sync_url) as engine:
            historical_source = _installed_function_source(engine)
        assert historical_source == _FROZEN_SETTLEMENT_COHORT_FUNCTION_SOURCE
        assert "drifted_column" not in historical_source

        monkeypatch.delenv("PYTHONPATH", raising=False)
        if old_pythonpath is not None:
            monkeypatch.setenv("PYTHONPATH", old_pythonpath)
        alembic(sync_url, "upgrade", "head")

        with engine_on(sync_url) as engine:
            head_source = _installed_function_source(engine)
        assert head_source == _function_source(generation_immutability_ddl()[0])
