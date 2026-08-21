# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the deployment-generation immutability trigger."""

from __future__ import annotations

import os

import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, load_migration, private_database

_MIGRATION = "a4e1c7b09f52_deployment_generations.py"
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


def test_historical_trigger_is_frozen_and_head_trigger_matches_live_ddl(pg_admin, tmp_path, monkeypatch):
    from nso_adapter.store.ddl import generation_immutability_ddl

    module = load_migration(_MIGRATION)
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from nso_adapter.store import ddl\n"
        "if 'settlement_cohort' not in ddl.GENERATION_IMMUTABLE_COLUMNS:\n"
        "    ddl.GENERATION_IMMUTABLE_COLUMNS += ('settlement_cohort',)\n"
    )

    with private_database(pg_admin, "generation_ddl") as sync_url:
        old_pythonpath = os.environ.get("PYTHONPATH")
        pythonpath = str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
        monkeypatch.setenv("PYTHONPATH", pythonpath)
        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine:
            historical_source = _installed_function_source(engine)
        assert historical_source == _FROZEN_FUNCTION_SOURCE
        assert "settlement_cohort" not in historical_source

        monkeypatch.delenv("PYTHONPATH", raising=False)
        if old_pythonpath is not None:
            monkeypatch.setenv("PYTHONPATH", old_pythonpath)
        alembic(sync_url, "upgrade", "head")

        with engine_on(sync_url) as engine:
            head_source = _installed_function_source(engine)
        assert head_source == _function_source(generation_immutability_ddl()[0])
