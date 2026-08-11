# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 O2b.1 — the receipt gains its third request-mode flag, reversibly.

The migration adds ``intent_push_receipt.backfill_only``. It chains off the deployment-
generation revision that introduced the table (the brief's ``c1b6e93a4d27`` predates that
landing and does not exist); the chain end is read from the module, so this guard and the
migration cannot drift apart.

Schema parity — ``create_all ≡ alembic`` — is proven by ``tests/store/test_schema_parity.py``
over the whole model set, so it is not restated here.
"""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "b7d5e2f18c40_receipt_backfill_only_mode.py"


def _module():
    return load_migration(_MIGRATION)


def test_the_migration_chains_off_the_deployment_generation_revision():
    """O2b.1 — one head, chained per §4.6: take the next revision off the CURRENT chain end."""
    module = _module()
    assert module.down_revision == "a4e1c7b09f52"
    assert_single_head_containing(module.revision)


def test_backfill_only_is_a_not_null_boolean_defaulting_to_false(pg_admin):
    """O2b.1 — every receipt admitted before the mode existed was an ordinary delivery."""
    module = _module()
    with private_database(pg_admin, "rcpbf") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            column = {c["name"]: c for c in sa.inspect(engine).get_columns("intent_push_receipt")}["backfill_only"]
            assert column["nullable"] is False
            assert "false" in str(column["default"]).lower()


def test_a_receipt_written_before_the_mode_existed_reads_as_an_ordinary_delivery(pg_admin):
    """O2b.1 — the default is asserted on a real pre-migration row, not on the DDL alone."""
    module = _module()
    with private_database(pg_admin, "rcpbfrow") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO devices (nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                "VALUES ('nso-dev', 'rcp-bf-mig', 'mapped', now(), now())"
            )
            conn.exec_driver_sql(
                "INSERT INTO intent_push_receipt "
                "(device_id, section, push_seq, request_digest, created_at, updated_at) "
                "SELECT id, 'static_route', 5, 'x', now(), now() FROM devices WHERE nso_device_name = 'rcp-bf-mig'"
            )

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine, engine.connect() as conn:
            assert conn.exec_driver_sql("SELECT backfill_only FROM intent_push_receipt").scalar() is False


def test_the_migration_is_reversible(pg_admin):
    """O2b.1 — irreversibility is the forbidden outcome."""
    module = _module()
    with private_database(pg_admin, "rcpbfrev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            names = {c["name"] for c in sa.inspect(engine).get_columns("intent_push_receipt")}
            assert "backfill_only" not in names
        alembic(sync_url, "upgrade", module.revision)
