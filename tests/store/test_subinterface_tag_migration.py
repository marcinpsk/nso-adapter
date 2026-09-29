# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guards for required subinterface tags."""

from __future__ import annotations

import os
import subprocess
import sys

import sqlalchemy as sa

from tests.store.migration_harness import REPO_ROOT, alembic, engine_on, load_migration, private_database

MIGRATION = "e4b8c2d6f0a1_subinterface_tag_required.py"
PARENT = "d1a7c3e9f2b5"


def test_subinterface_tag_migration_requires_tags(pg_provisioner):
    migration = load_migration(MIGRATION)
    assert migration.down_revision == PARENT

    with private_database(pg_provisioner, "subif_tag") as url:
        alembic(url, "upgrade", PARENT)
        with engine_on(url) as engine, engine.begin() as conn:
            device_id = conn.execute(
                sa.text(
                    "INSERT INTO devices (nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
                    "VALUES ('test', 'subif-a', 'mapped', now(), now()) RETURNING id"
                )
            ).scalar_one()
            for table in ("device_subinterface", "subinterface_intent"):
                conn.execute(
                    sa.text(
                        f"INSERT INTO {table} (device_id, interface_name, dot1q_vlan, sub_type, refresh_source) "
                        "VALUES (:id, :name, NULL, 'subinterface', 'test')"
                        if table == "device_subinterface"
                        else f"INSERT INTO {table} (device_id, interface_name, dot1q_vlan, sub_type) "
                        "VALUES (:id, :name, NULL, 'subinterface')"
                    ),
                    {"id": device_id, "name": "if.100"},
                )

        for table in ("device_subinterface", "subinterface_intent"):
            proc = subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", migration.revision],
                cwd=REPO_ROOT,
                capture_output=True,
                env={**os.environ, "DATABASE_URL": url},
                check=False,
            )
            failure = proc.stderr.decode()
            assert proc.returncode != 0
            assert table in failure
            assert f"device_id={device_id}" in failure
            assert "interface_name=if.100" in failure
            with engine_on(url) as engine, engine.begin() as conn:
                assert conn.execute(sa.text(f"SELECT count(*) FROM {table} WHERE dot1q_vlan IS NULL")).scalar_one() == 1
                conn.execute(sa.text(f"DELETE FROM {table} WHERE dot1q_vlan IS NULL"))

        alembic(url, "upgrade", migration.revision)
        with engine_on(url) as engine:
            inspector = sa.inspect(engine)
            for table in ("device_subinterface", "subinterface_intent"):
                column = next(c for c in inspector.get_columns(table) if c["name"] == "dot1q_vlan")
                assert column["nullable"] is False
