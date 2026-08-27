# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the durable per-stream pending-clear carrier table."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    delete_rules,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "e3a7c9d1b504_stream_pending_clear.py"


def _module():
    return load_migration(_MIGRATION)


def test_stream_pending_clear_migration_chains_off_the_current_branch_head():
    module = _module()
    assert module.down_revision == "b7d5e2f18c40"
    assert_single_head_containing(module.revision)


def test_stream_pending_clear_table_shape(pg_provisioner):
    from nso_adapter.core.request_flags import PENDING_CLEAR_PROVENANCES

    module = _module()
    with private_database(pg_provisioner, "pending_clear") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            columns = {column["name"]: column for column in inspector.get_columns("stream_pending_clear")}

            assert set(columns) == {"id", "device_id", "stream", "provenance", "revision", "recorded_at"}
            assert inspector.get_pk_constraint("stream_pending_clear")["constrained_columns"] == ["id"]
            assert all(columns[name]["nullable"] is False for name in columns)
            assert isinstance(columns["revision"]["type"], sa.BigInteger)
            assert columns["recorded_at"]["type"].timezone is True

            unique_columns = {
                tuple(constraint["column_names"])
                for constraint in inspector.get_unique_constraints("stream_pending_clear")
            }
            assert ("device_id", "stream", "provenance") in unique_columns
            checks = [constraint["sqltext"] for constraint in inspector.get_check_constraints("stream_pending_clear")]
            assert any(all(repr(provenance) in check for provenance in PENDING_CLEAR_PROVENANCES) for check in checks)
            assert delete_rules(engine, "stream_pending_clear")["device_id"] == "CASCADE"


def test_stream_pending_clear_migration_is_reversible(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "pending_clear_down") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert "stream_pending_clear" not in sa.inspect(engine).get_table_names()

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            assert "stream_pending_clear" in sa.inspect(engine).get_table_names()


def test_stream_pending_clear_allows_only_one_provenance_per_stream(pg_sync_session):
    from nso_adapter.store.models import Device, StreamPendingClear

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="pending-clear-unique",
        netbox_device_id=14757,
    )
    pg_sync_session.add(device)
    pg_sync_session.flush()
    pg_sync_session.add_all(
        [
            StreamPendingClear(
                device_id=device.id,
                stream="ospf",
                provenance="store_only",
                revision=1,
            ),
            StreamPendingClear(
                device_id=device.id,
                stream="ospf",
                provenance="authorized",
                revision=2,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        pg_sync_session.flush()
