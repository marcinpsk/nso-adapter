# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Schema and constraint guards for the prepared projection slot (#1612)."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "b3d9f2a6c410_prepared_projection_slot.py"
_SLOT = ("prepared_revision", "prepared_tables", "prepared_deletions")
_CHECKS = {
    "ck_projection_stream_prepared_slot",
    "ck_projection_stream_prepared_revision",
}


def _module():
    return load_migration(_MIGRATION)


def _seed_stream(connection, *, desired_revision: int) -> int:
    device_id = connection.execute(
        sa.text(
            "INSERT INTO devices "
            "(nso_instance, nso_device_name, netbox_device_id, source_epoch, mapping_status, created_at, updated_at) "
            "VALUES ('nso-dev', 'migration-slot', 1612, 1, 'mapped', now(), now()) RETURNING id"
        )
    ).scalar_one()
    return connection.execute(
        sa.text(
            "INSERT INTO device_projection_stream "
            "(device_id, stream, desired_revision, authorized_revision, applied_revision, updated_at) "
            "VALUES (:device_id, 'lag', :desired, 0, 0, now()) RETURNING id"
        ),
        {"device_id": device_id, "desired": desired_revision},
    ).scalar_one()


def test_prepared_slot_migration_adds_three_nullable_columns_and_two_checks(pg_provisioner):
    module = _module()
    assert module.down_revision == "a5c7e9b1d3f6"
    assert_single_head_containing(module.revision)

    with private_database(pg_provisioner, "prepared_slot") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            existing = {column["name"] for column in sa.inspect(engine).get_columns("device_projection_stream")}
            assert existing.isdisjoint(_SLOT)
            with engine.begin() as connection:
                _seed_stream(connection, desired_revision=3)

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            columns = {column["name"]: column for column in inspector.get_columns("device_projection_stream")}
            assert set(_SLOT) <= set(columns)
            assert all(columns[name]["nullable"] for name in _SLOT)
            assert isinstance(columns["prepared_revision"]["type"], sa.BigInteger)
            assert _CHECKS <= {item["name"] for item in inspector.get_check_constraints("device_projection_stream")}
            with engine.connect() as connection:
                # The pre-existing row survives with an empty slot.
                assert connection.execute(
                    sa.text(
                        "SELECT prepared_revision, prepared_tables, prepared_deletions FROM device_projection_stream"
                    )
                ).one() == (None, None, None)

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            remaining = {column["name"] for column in sa.inspect(engine).get_columns("device_projection_stream")}
            assert remaining.isdisjoint(_SLOT)
            with engine.connect() as connection:
                assert connection.scalar(sa.text("SELECT count(*) FROM device_projection_stream")) == 1


@pytest.mark.parametrize(
    ("values", "constraint"),
    [
        pytest.param(
            {"prepared_revision": 1, "prepared_tables": None, "prepared_deletions": None},
            "ck_projection_stream_prepared_slot",
            id="revision-without-tables",
        ),
        pytest.param(
            {"prepared_revision": None, "prepared_tables": "{}", "prepared_deletions": "{}"},
            "ck_projection_stream_prepared_slot",
            id="tables-without-revision",
        ),
        pytest.param(
            {"prepared_revision": 1, "prepared_tables": "{}", "prepared_deletions": None},
            "ck_projection_stream_prepared_slot",
            id="deletions-missing",
        ),
        pytest.param(
            {"prepared_revision": 0, "prepared_tables": "{}", "prepared_deletions": "{}"},
            "ck_projection_stream_prepared_revision",
            id="revision-zero",
        ),
        pytest.param(
            {"prepared_revision": 4, "prepared_tables": "{}", "prepared_deletions": "{}"},
            "ck_projection_stream_prepared_revision",
            id="revision-past-desired",
        ),
    ],
)
def test_the_database_refuses_a_half_written_or_unreachable_slot(pg_provisioner, values, constraint):
    module = _module()
    with private_database(pg_provisioner, "prepared_slot_ck") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine, engine.begin() as connection:
            row_id = _seed_stream(connection, desired_revision=3)
            with pytest.raises(sa.exc.IntegrityError) as excinfo:
                connection.execute(
                    sa.text(
                        "UPDATE device_projection_stream SET prepared_revision = :prepared_revision, "
                        "prepared_tables = CAST(:prepared_tables AS json), "
                        "prepared_deletions = CAST(:prepared_deletions AS json) WHERE id = :row_id"
                    ),
                    {**values, "row_id": row_id},
                )
    assert constraint in str(excinfo.value)


def test_a_complete_slot_inside_the_desired_revision_is_accepted(pg_provisioner):
    module = _module()
    update = sa.text(
        "UPDATE device_projection_stream SET prepared_revision = 2, "
        "prepared_tables = CAST(:tables AS json), prepared_deletions = CAST(:deletions AS json) "
        "WHERE id = :row_id"
    )
    with private_database(pg_provisioner, "prepared_slot_ok") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            with engine.begin() as connection:
                row_id = _seed_stream(connection, desired_revision=3)
                connection.execute(
                    update,
                    {
                        "tables": '{"lag_bundle_intent": []}',
                        "deletions": '{"delete_origin": {}, "detach": {}, "owned_content": {}}',
                        "row_id": row_id,
                    },
                )
            with engine.connect() as connection:
                stored = connection.execute(
                    sa.text(
                        "SELECT prepared_revision, prepared_tables FROM device_projection_stream WHERE id = :row_id"
                    ),
                    {"row_id": row_id},
                ).one()
    assert stored == (2, {"lag_bundle_intent": []})
