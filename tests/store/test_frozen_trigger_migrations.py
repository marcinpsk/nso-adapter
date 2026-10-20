# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Trigger revisions install their frozen DDL, not the live store.ddl helpers."""

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, load_migration, private_database

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
_DRIFTED_HELPERS = (
    "observation_immutability_ddl",
    "execution_evidence_immutability_ddl",
    "generation_carrier_immutability_ddl",
    "job_terminal_evidence_immutability_ddl",
)


def _function_source(statement: str) -> str:
    return statement.partition(" AS $$")[2].partition("$$ LANGUAGE plpgsql")[0]


@pytest.mark.parametrize(
    ("migration", "constant", "functions"),
    [
        pytest.param(
            "8a86079e0dfe_store_immutable_read_observations.py",
            "_OBSERVATION_IMMUTABILITY_DDL",
            ("read_observation_reject_update",),
            id="read-observation",
        ),
        pytest.param(
            "c39b7d2e4a61_durable_execution_outcomes.py",
            "_EXECUTION_EVIDENCE_IMMUTABILITY_DDL",
            ("execution_attempt_reject_rewrite", "row_outcome_reject_update"),
            id="execution-evidence",
        ),
        pytest.param(
            "d8f2a6c1e904_terminal_carrier_evidence.py",
            "_GENERATION_CARRIER_IMMUTABILITY_DDL",
            ("deployment_generation_reject_carrier_rewrite",),
            id="generation-carrier",
        ),
        pytest.param(
            "d8f2a6c1e904_terminal_carrier_evidence.py",
            "_JOB_TERMINAL_EVIDENCE_IMMUTABILITY_DDL",
            ("job_reject_terminal_evidence_rewrite",),
            id="job-terminal-evidence",
        ),
    ],
)
def test_revision_installs_its_frozen_trigger_under_helper_drift(
    pg_provisioner, tmp_path, monkeypatch, migration, constant, functions
):
    module = load_migration(migration)
    frozen = {_function_source(statement) for statement in getattr(module, constant) if " AS $$" in statement}
    marker = tmp_path / "sitecustomize-loaded"
    (tmp_path / "sitecustomize.py").write_text(
        "from pathlib import Path\n"
        "from nso_adapter.store import ddl\n"
        f"for _name in {_DRIFTED_HELPERS!r}:\n"
        "    _live = getattr(ddl, _name)\n"
        "    setattr(ddl, _name, lambda _live=_live: tuple(s.replace('is immutable', 'is drifted') for s in _live()))\n"
        f"Path({str(marker)!r}).touch()\n"
    )
    old_pythonpath = os.environ.get("PYTHONPATH")
    monkeypatch.setenv(
        "PYTHONPATH", str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
    )

    with private_database(pg_provisioner, "frozen_trigger") as url:
        alembic(url, "upgrade", module.revision)
        assert marker.exists(), "the Alembic subprocess did not load sitecustomize"
        with engine_on(url) as engine, engine.connect() as conn:
            installed = {conn.execute(_FUNCTION_SOURCE_SQL, {"name": name}).scalar_one() for name in functions}

    assert installed == frozen
    assert not any("is drifted" in source for source in installed)
