# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Both terminal evidence guards follow the same migration lifecycle."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, load_migration, private_database


def test_terminal_carrier_migration_installs_and_removes_both_guards(pg_provisioner):
    migration = load_migration("d8f2a6c1e904_terminal_carrier_evidence.py")
    with private_database(pg_provisioner, "terminal_carrier_migration") as url:
        alembic(url, "upgrade", migration.down_revision)
        alembic(url, "upgrade", migration.revision)
        with engine_on(url) as engine:
            with engine.connect() as conn:
                assert (
                    conn.scalar(
                        sa.text(
                            "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname IN "
                            "('deployment_generation_carrier_immutable', 'job_terminal_evidence_immutable')"
                        )
                    )
                    == 2
                )
        alembic(url, "downgrade", migration.down_revision)
        with engine_on(url) as engine:
            with engine.connect() as conn:
                assert (
                    conn.scalar(
                        sa.text(
                            "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname IN "
                            "('deployment_generation_carrier_immutable', 'job_terminal_evidence_immutable')"
                        )
                    )
                    == 0
                )
                assert (
                    conn.scalar(
                        sa.text(
                            "SELECT count(*) FROM pg_proc WHERE proname IN "
                            "('deployment_generation_reject_carrier_rewrite', 'job_reject_terminal_evidence_rewrite')"
                        )
                    )
                    == 0
                )
        alembic(url, "upgrade", migration.revision)
