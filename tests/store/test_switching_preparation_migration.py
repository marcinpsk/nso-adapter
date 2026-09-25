# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Upgrade of a populated switching preparation slot."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, private_database


def test_upgrade_clears_old_slots_and_keeps_authorized_state(pg_provisioner):
    with private_database(pg_provisioner, "switching_prepared_slot") as sync_url:
        alembic(sync_url, "upgrade", "f3b8d1e7a9c2")
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = conn.execute(
                sa.text(
                    "INSERT INTO devices "
                    "(nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                    "VALUES ('nso-dev', 'migration-device', 'mapped', now(), now()) RETURNING id"
                )
            ).scalar_one()
            authorized = {"lag_bundle_intent": [{"name": "A", "lag_id": 1}]}
            for stream, populated in (("lag", True), ("switchport", False)):
                conn.execute(
                    sa.text(
                        "INSERT INTO device_projection_stream "
                        "(device_id, stream, desired_revision, authorized_revision, applied_revision, "
                        "authorized_document, prepared_revision, prepared_tables, prepared_deletions, updated_at) "
                        "VALUES (:device_id, :stream, 7, 5, 3, CAST(:authorized AS json), "
                        ":prepared_revision, CAST(:prepared_tables AS json), CAST(:prepared_deletions AS json), now())"
                    ),
                    {
                        "device_id": device_id,
                        "stream": stream,
                        "authorized": '{"lag_bundle_intent": [{"name": "A", "lag_id": 1}]}',
                        "prepared_revision": 7 if populated else None,
                        "prepared_tables": '{"lag_bundle_intent": [{"name": "B"}]}' if populated else None,
                        "prepared_deletions": '{"delete_origin": {}}' if populated else None,
                    },
                )

        alembic(sync_url, "upgrade", "head")
        with engine_on(sync_url) as engine, engine.connect() as conn:
            rows = (
                conn.execute(
                    sa.text(
                        "SELECT stream, desired_revision, authorized_revision, applied_revision, "
                        "authorized_document, prepared_revision, prepared_tables, prepared_deletions, "
                        "prepared_source_revision, prepared_source_digest "
                        "FROM device_projection_stream WHERE device_id = :device_id ORDER BY stream"
                    ),
                    {"device_id": device_id},
                )
                .mappings()
                .all()
            )
        assert [row["stream"] for row in rows] == ["lag", "switchport"]
        for row in rows:
            assert (row["desired_revision"], row["authorized_revision"], row["applied_revision"]) == (7, 5, 3)
            assert row["authorized_document"] == authorized
            assert (
                tuple(
                    row[field]
                    for field in (
                        "prepared_revision",
                        "prepared_tables",
                        "prepared_deletions",
                        "prepared_source_revision",
                        "prepared_source_digest",
                    )
                )
                == (None,) * 5
            )
