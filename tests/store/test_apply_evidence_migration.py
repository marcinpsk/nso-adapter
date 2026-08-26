# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""A1 durable Apply evidence schema and retention behavior."""

from __future__ import annotations

import os
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    delete_rules,
    engine_on,
    index_predicates,
    load_migration,
    private_database,
)

_MIGRATION = "c6f1a8d2e4b7_apply_evidence.py"
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
        OR NEW.settlement_cohort::text IS DISTINCT FROM OLD.settlement_cohort::text
        OR NEW.apply_attempt_id IS DISTINCT FROM OLD.apply_attempt_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, allowed_removal_keys, source_push_seq, stream_revisions, removal_context, settlement_cohort, apply_attempt_id, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
"""
_FROZEN_PREVIOUS_FUNCTION_SOURCE = """
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


def _module():
    return load_migration(_MIGRATION)


def _installed_function_source(engine) -> str:
    with engine.connect() as conn:
        return conn.execute(_FUNCTION_SOURCE_SQL, {"name": _FUNCTION}).scalar_one()


def test_apply_evidence_trigger_is_frozen_for_upgrade_and_downgrade(pg_admin, tmp_path, monkeypatch):
    module = _module()
    marker = tmp_path / "sitecustomize-loaded"
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from pathlib import Path\n"
        "from nso_adapter.store import ddl\n"
        f"Path({str(marker)!r}).touch()\n"
        "ddl.GENERATION_IMMUTABLE_COLUMNS += ('drifted_column',)\n"
        "ddl.generation_immutability_ddl.__defaults__ = (ddl.GENERATION_IMMUTABLE_COLUMNS,)\n"
        "assert 'drifted_column' in ddl.generation_immutability_ddl()[0]\n"
    )

    with private_database(pg_admin, "apply_evidence_ddl") as sync_url:
        old_pythonpath = os.environ.get("PYTHONPATH")
        pythonpath = str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
        monkeypatch.setenv("PYTHONPATH", pythonpath)

        alembic(sync_url, "upgrade", module.revision)
        assert marker.exists(), "the Alembic subprocess did not load the injected sitecustomize"
        with engine_on(sync_url) as engine:
            assert _installed_function_source(engine) == _FROZEN_FUNCTION_SOURCE

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert _installed_function_source(engine) == _FROZEN_PREVIOUS_FUNCTION_SOURCE


def test_apply_evidence_schema_and_attempt_retention(pg_admin):
    module = _module()
    assert module.down_revision == "b9e3d7a1c5f2"
    assert_single_head_containing(module.revision)

    with private_database(pg_admin, "applyevidence") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            attempt_columns = {column["name"]: column for column in inspector.get_columns("deployment_apply_attempt")}
            generation_columns = {column["name"]: column for column in inspector.get_columns("deployment_generation")}

            assert set(attempt_columns) == {
                "id",
                "device_id",
                "selected",
                "admission_state",
                "http_status",
                "response",
                "created_at",
            }
            assert isinstance(attempt_columns["id"]["type"], postgresql.UUID)
            assert isinstance(attempt_columns["selected"]["type"], postgresql.JSONB)
            assert isinstance(attempt_columns["response"]["type"], postgresql.JSONB)
            assert attempt_columns["created_at"]["type"].timezone is True
            assert all(not column["nullable"] for column in attempt_columns.values())

            assert isinstance(generation_columns["apply_attempt_id"]["type"], postgresql.UUID)
            assert generation_columns["apply_attempt_id"]["nullable"] is True
            for name in ("carrier_job_result", "carrier_job_error"):
                assert isinstance(generation_columns[name]["type"], postgresql.JSONB)
                assert generation_columns[name]["nullable"] is True
            for name in ("carrier_job_id", "carrier_job_status"):
                assert generation_columns[name]["nullable"] is True

            assert delete_rules(engine, "deployment_apply_attempt")["device_id"] == "CASCADE"
            assert delete_rules(engine, "deployment_generation")["apply_attempt_id"] == "NO ACTION"
            attempt_fk = next(
                fk
                for fk in inspector.get_foreign_keys("deployment_generation")
                if fk["name"] == "fk_generation_apply_attempt"
            )
            assert attempt_fk["options"] == {
                "initially": "DEFERRED",
                "deferrable": True,
            }
            assert index_predicates(engine, "deployment_generation")["ix_generation_device_apply_attempt"] == (
                ("device_id", "apply_attempt_id"),
                False,
                None,
            )

            attempt_id = uuid.uuid4()
            with engine.begin() as conn:
                device_id = conn.exec_driver_sql(
                    "INSERT INTO devices "
                    "(nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                    "VALUES ('nso-dev', 'apply-evidence-retention', 'mapped', now(), now()) RETURNING id"
                ).scalar_one()
                conn.execute(
                    sa.text(
                        "INSERT INTO deployment_apply_attempt "
                        "(id, device_id, selected, admission_state, http_status, response, created_at) "
                        "VALUES (:id, :device_id, CAST(:selected AS jsonb), 'admitted', 202, "
                        "CAST(:response AS jsonb), now())"
                    ),
                    {
                        "id": attempt_id,
                        "device_id": device_id,
                        "selected": '{"vlan": 71}',
                        "response": '{"outcome": "promoted", "skipped": {}, "skipped_detail": null, "generations": []}',
                    },
                )
                conn.execute(
                    sa.text(
                        "INSERT INTO deployment_generation "
                        "(device_id, seq, mode, status, document, digest, allowed_removal_keys, "
                        "source_push_seq, stream_revisions, apply_attempt_id, attempts, created_at, updated_at) "
                        "VALUES (:device_id, 1, 'networked', 'settled', CAST('{}' AS json), :digest, "
                        "CAST('{}' AS json), CAST('{}' AS json), CAST('{}' AS json), :attempt_id, 0, now(), now())"
                    ),
                    {"device_id": device_id, "digest": "a" * 64, "attempt_id": attempt_id},
                )

            with pytest.raises(sa.exc.IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        sa.text("DELETE FROM deployment_apply_attempt WHERE id = :id"),
                        {"id": attempt_id},
                    )

            with engine.connect() as conn:
                assert (
                    conn.execute(
                        sa.text("SELECT count(*) FROM deployment_apply_attempt WHERE id = :id"),
                        {"id": attempt_id},
                    ).scalar_one()
                    == 1
                )


def test_generation_attempt_must_belong_to_the_same_device(pg_admin):
    module = _module()
    with private_database(pg_admin, "apply_evidence_device_fk") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            with pytest.raises(sa.exc.IntegrityError) as exc_info, engine.begin() as conn:
                attempt_device_id = conn.exec_driver_sql(
                    "INSERT INTO devices "
                    "(nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                    "VALUES ('nso-dev', 'attempt-device', 'mapped', now(), now()) RETURNING id"
                ).scalar_one()
                generation_device_id = conn.exec_driver_sql(
                    "INSERT INTO devices "
                    "(nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                    "VALUES ('nso-dev', 'generation-device', 'mapped', now(), now()) RETURNING id"
                ).scalar_one()
                attempt_id = uuid.uuid4()
                conn.execute(
                    sa.text(
                        "INSERT INTO deployment_apply_attempt "
                        "(id, device_id, selected, admission_state, http_status, response, created_at) "
                        "VALUES (:id, :device_id, CAST('{}' AS jsonb), 'admitted', 202, CAST('{}' AS jsonb), now())"
                    ),
                    {"id": attempt_id, "device_id": attempt_device_id},
                )
                conn.execute(
                    sa.text(
                        "INSERT INTO deployment_generation "
                        "(device_id, seq, mode, status, document, digest, allowed_removal_keys, "
                        "source_push_seq, stream_revisions, apply_attempt_id, attempts, created_at, updated_at) "
                        "VALUES (:device_id, 1, 'networked', 'settled', CAST('{}' AS json), :digest, "
                        "CAST('{}' AS json), CAST('{}' AS json), CAST('{}' AS json), :attempt_id, 0, now(), now())"
                    ),
                    {
                        "device_id": generation_device_id,
                        "digest": "b" * 64,
                        "attempt_id": attempt_id,
                    },
                )
            assert "fk_generation_apply_attempt" in str(exc_info.value)


def test_apply_evidence_migration_is_reversible(pg_admin):
    module = _module()
    with private_database(pg_admin, "applyevidencerev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            assert "deployment_apply_attempt" not in inspector.get_table_names()
            assert "apply_attempt_id" not in {
                column["name"] for column in inspector.get_columns("deployment_generation")
            }
        alembic(sync_url, "upgrade", module.revision)
