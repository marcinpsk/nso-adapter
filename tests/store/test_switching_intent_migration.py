# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Schema and data-boundary guards for the switching intent migration."""

from __future__ import annotations

import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    delete_rules,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "a5c7e9b1d3f6_switching_intent_store.py"
_TABLES = {
    "lag_bundle_intent",
    "lag_member_intent",
    "switchport_intent",
    "switchport_tagged_vlan_intent",
}


def _module():
    return load_migration(_MIGRATION)


def test_switching_intent_migration_creates_empty_write_owned_tables(pg_provisioner):
    module = _module()
    assert module.down_revision == "c6f1a8d2e4b7"
    assert_single_head_containing(module.revision)

    with private_database(pg_provisioner, "switching_intent") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as connection:
            device_id = connection.execute(
                sa.text(
                    "INSERT INTO devices "
                    "(nso_instance, nso_device_name, netbox_device_id, source_epoch, mapping_status, created_at, updated_at) "
                    "VALUES ('nso-dev', 'migration-switch', 1612, 1, 'mapped', now(), now()) RETURNING id"
                )
            ).scalar_one()
            lag_bundle_id = connection.execute(
                sa.text(
                    "INSERT INTO lag_bundle_config "
                    "(device_id, name, lag_id, vpc_sensitive, refresh_source) "
                    "VALUES (:device_id, 'Port-channel1', 1, false, 'test') RETURNING id"
                ),
                {"device_id": device_id},
            ).scalar_one()
            connection.execute(
                sa.text(
                    "INSERT INTO lag_member_config (lag_bundle_id, interface_name) VALUES (:lag_bundle_id, 'Gi0/1')"
                ),
                {"lag_bundle_id": lag_bundle_id},
            )
            connection.execute(
                sa.text(
                    "INSERT INTO device_switchport (device_id, interface_name, refresh_source) "
                    "VALUES (:device_id, 'Gi0/2', 'test')"
                ),
                {"device_id": device_id},
            )

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            assert _TABLES <= set(inspector.get_table_names())
            with engine.connect() as connection:
                assert {table: connection.scalar(sa.text(f"SELECT count(*) FROM {table}")) for table in _TABLES} == {
                    table: 0 for table in _TABLES
                }
                assert connection.scalar(sa.text("SELECT count(*) FROM lag_bundle_config")) == 1
                assert connection.scalar(sa.text("SELECT count(*) FROM device_switchport")) == 1

            lag_columns = {column["name"]: column for column in inspector.get_columns("lag_bundle_intent")}
            member_columns = {column["name"] for column in inspector.get_columns("lag_member_intent")}
            switchport_columns = {column["name"] for column in inspector.get_columns("switchport_intent")}
            tagged_columns = {column["name"] for column in inspector.get_columns("switchport_tagged_vlan_intent")}
            assert isinstance(lag_columns["lag_id"]["type"], sa.BigInteger)
            assert lag_columns["lag_id"]["nullable"] is True
            assert lag_columns["accepted_at"]["type"].timezone is True
            assert lag_columns["accepted_at"]["nullable"] is False
            assert lag_columns["last_apply_at"]["type"].timezone is True
            assert member_columns == {"id", "lag_bundle_id", "interface_name", "mode", "port_priority"}
            assert switchport_columns == {
                "id",
                "device_id",
                "interface_name",
                "mode",
                "untagged_vlan",
                "accepted_at",
                "last_apply_at",
                "last_apply_error",
            }
            assert tagged_columns == {"id", "switchport_id", "vlan_id"}
            assert delete_rules(engine, "lag_bundle_intent") == {"device_id": "CASCADE"}
            assert delete_rules(engine, "lag_member_intent") == {"lag_bundle_id": "CASCADE"}
            assert delete_rules(engine, "switchport_intent") == {"device_id": "CASCADE"}
            assert delete_rules(engine, "switchport_tagged_vlan_intent") == {"switchport_id": "CASCADE"}

            uniques = {
                table: {tuple(item["column_names"]) for item in inspector.get_unique_constraints(table)}
                for table in _TABLES
            }
            assert uniques == {
                "lag_bundle_intent": {("device_id", "name")},
                "lag_member_intent": {("lag_bundle_id", "interface_name")},
                "switchport_intent": {("device_id", "interface_name")},
                "switchport_tagged_vlan_intent": {("switchport_id", "vlan_id")},
            }

            checks = {table: {item["name"] for item in inspector.get_check_constraints(table)} for table in _TABLES}
            assert checks["lag_bundle_intent"] == {
                "ck_lag_bundle_intent_admin_key_uint16",
                "ck_lag_bundle_intent_lag_id_uint32",
                "ck_lag_bundle_intent_min_links_uint16",
                "ck_lag_bundle_intent_system_priority_uint16",
            }
            assert checks["lag_member_intent"] == {"ck_lag_member_intent_port_priority_uint16"}
            assert checks["switchport_intent"] == {"ck_switchport_intent_untagged_vlan_uint16"}
            assert checks["switchport_tagged_vlan_intent"] == {"ck_switchport_tagged_vlan_intent_vlan_id_uint16"}

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert _TABLES.isdisjoint(sa.inspect(engine).get_table_names())
            with engine.connect() as connection:
                assert connection.scalar(sa.text("SELECT count(*) FROM lag_bundle_config")) == 1
                assert connection.scalar(sa.text("SELECT count(*) FROM device_switchport")) == 1
