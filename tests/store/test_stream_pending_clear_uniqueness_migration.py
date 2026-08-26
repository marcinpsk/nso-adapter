# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the one-pending-clear-row-per-stream migration."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "a6d4f2c8e1b3_stream_pending_clear_uniqueness.py"


def _module():
    return load_migration(_MIGRATION)


def _unique_constraints(engine) -> dict[str, tuple[str, ...]]:
    return {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in sa.inspect(engine).get_unique_constraints("stream_pending_clear")
    }


def _seed_pending_clear_rows(engine) -> None:
    with engine.begin() as conn:
        device_id = conn.execute(
            sa.text(
                "INSERT INTO devices "
                "(nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                "VALUES ('nso-dev', 'pending-clear-repair', 'mapped', now(), now()) RETURNING id"
            )
        ).scalar_one()
        conn.execute(
            sa.text(
                "INSERT INTO stream_pending_clear "
                "(device_id, stream, provenance, revision, recorded_at) "
                "VALUES (:device_id, :stream, :provenance, :revision, now())"
            ),
            [
                {"device_id": device_id, "stream": "isis", "provenance": "store_only", "revision": 1},
                {"device_id": device_id, "stream": "isis", "provenance": "authorized", "revision": 2},
                {"device_id": device_id, "stream": "ospf", "provenance": "store_only", "revision": 3},
            ],
        )


def test_stream_pending_clear_uniqueness_chains_off_the_table_migration():
    module = _module()
    assert module.down_revision == "e3a7c9d1b504"
    assert_single_head_containing(module.revision)


def test_stream_pending_clear_uniqueness_repairs_pairs_before_upgrade(pg_admin):
    module = _module()
    with private_database(pg_admin, "pending_clear_unique") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            _seed_pending_clear_rows(engine)

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            with engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT stream, provenance, revision FROM stream_pending_clear ORDER BY stream, provenance")
                ).all()
            assert rows == [("isis", "authorized", 2), ("ospf", "store_only", 3)]
            assert _unique_constraints(engine)["uq_stream_pending_clear"] == ("device_id", "stream")


def test_stream_pending_clear_uniqueness_downgrade_restores_provenance_identity(pg_admin):
    module = _module()
    with private_database(pg_admin, "pending_clear_unique_down") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert _unique_constraints(engine)["uq_stream_pending_clear"] == (
                "device_id",
                "stream",
                "provenance",
            )
