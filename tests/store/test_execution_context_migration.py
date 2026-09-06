# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the one-shot data migration that stamps pre-contract fragments (#1663).

A fragment written before the execution-context contract carries tables only, so its
document would be refused at hydration. The migration attaches the context of every
section, the interface proof and the static-route apply plan, and nothing else.
"""

from __future__ import annotations

import json

import sqlalchemy as sa

from nso_adapter.core.projection import (
    EXECUTION_KEY,
    hydrate_interface_execution,
    projection_streams,
    section_context,
    stream_section,
)
from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan
from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "c7b4e1d90f28_execution_context_on_authorized_fragments.py"


def _module():
    return load_migration(_MIGRATION)


def _seed_device(connection, *, name: str, netbox_id: int, ned_id: str | None) -> int:
    return connection.execute(
        sa.text(
            "INSERT INTO devices "
            "(nso_instance, nso_device_name, netbox_device_id, source_epoch, mapping_status, ned_id, "
            " created_at, updated_at) "
            "VALUES ('nso-dev', :name, :netbox_id, 1, 'mapped', :ned_id, now(), now()) RETURNING id"
        ),
        {"name": name, "netbox_id": netbox_id, "ned_id": ned_id},
    ).scalar_one()


def _seed_stream(connection, device_id: int, stream: str, tables: dict) -> int:
    return connection.execute(
        sa.text(
            "INSERT INTO device_projection_stream "
            "(device_id, stream, desired_revision, authorized_revision, applied_revision, "
            " authorized_document, updated_at) "
            "VALUES (:device_id, :stream, 1, 1, 0, CAST(:document AS json), now()) RETURNING id"
        ),
        {"device_id": device_id, "stream": stream, "document": json.dumps(tables)},
    ).scalar_one()


def _fragment(connection, stream_id: int) -> dict:
    return connection.execute(
        sa.text("SELECT authorized_document FROM device_projection_stream WHERE id = :id"),
        {"id": stream_id},
    ).scalar_one()


def _empty_tables(stream: str) -> dict:
    from nso_adapter.core.projection import stream_tables

    return {table: [] for table in stream_tables(stream)}


def test_the_migration_stamps_a_context_on_every_section_and_leaves_the_schema_alone(pg_provisioner):
    module = _module()
    assert module.down_revision == "b3d9f2a6c410"
    assert_single_head_containing(module.revision)

    streams = sorted(projection_streams())
    with private_database(pg_provisioner, "execution_context") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            before = {column["name"] for column in sa.inspect(engine).get_columns("device_projection_stream")}
            with engine.begin() as connection:
                nokia = _seed_device(connection, name="mig-nokia", netbox_id=16631, ned_id="timos-nc-23.10")
                bare = _seed_device(connection, name="mig-bare", netbox_id=16632, ned_id=None)
                ids = {stream: _seed_stream(connection, nokia, stream, _empty_tables(stream)) for stream in streams}
                bare_id = _seed_stream(connection, bare, "vlan", _empty_tables("vlan"))

        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine:
            after = {column["name"] for column in sa.inspect(engine).get_columns("device_projection_stream")}
            assert after == before, "a data migration must not change the schema"
            with engine.begin() as connection:
                for stream in streams:
                    fragment = _fragment(connection, ids[stream])
                    section = stream_section(stream)
                    assert section_context({section: fragment}, section) == {
                        "ned_id": "timos-nc-23.10",
                        "dialect": "nokia_timos",
                    }
                    assert set(fragment) == {*_empty_tables(stream), EXECUTION_KEY}
                # A device with no NED id records an explicit null and the identity dialect.
                assert section_context({"vlan": _fragment(connection, bare_id)}, "vlan") == {
                    "ned_id": None,
                    "dialect": "identity",
                }


def test_the_migration_stamps_interface_proof_and_the_static_route_apply_plan(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "execution_context_proof") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            with engine.begin() as connection:
                device_id = _seed_device(connection, name="mig-proof", netbox_id=16633, ned_id="cisco-ios-cli-3.8")
                interface_id = connection.execute(
                    sa.text(
                        "INSERT INTO interfaces (device_id, name, kind, parent_binding, encap_tag, vrf, service) "
                        "VALUES (:device_id, 'Gi0/1', NULL, NULL, NULL, NULL, NULL) RETURNING id"
                    ),
                    {"device_id": device_id},
                ).scalar_one()
                for attribute, state in (("description", "in_sync"), ("enabled", "error")):
                    connection.execute(
                        sa.text(
                            "INSERT INTO interface_attr_state (interface_id, attribute, sync_state) "
                            "VALUES (:interface_id, :attribute, :state)"
                        ),
                        {"interface_id": interface_id, "attribute": attribute, "state": state},
                    )
                attributes = _seed_stream(
                    connection,
                    device_id,
                    "interface_config",
                    {
                        "interface_intent": [
                            {
                                "id": 11,
                                "interface_id": interface_id,
                                "attribute": "description",
                                "intent_value": "core",
                            },
                            {"id": 12, "interface_id": interface_id, "attribute": "enabled", "intent_value": "true"},
                        ]
                    },
                )
                addresses = _seed_stream(
                    connection,
                    device_id,
                    "ip",
                    {
                        "interface_ip_intent": [
                            {"id": 21, "interface_id": interface_id, "address": "192.0.2.1/24", "family": "ipv4"}
                        ]
                    },
                )
                routes = _seed_stream(
                    connection,
                    device_id,
                    "static_route",
                    {
                        "static_route_intent": [
                            {
                                "id": 31,
                                "device_id": device_id,
                                "route_id": 7,
                                "vrf": "",
                                "prefix": "198.18.0.0/24",
                                "next_hop": "198.18.1.1",
                            }
                        ],
                        "static_route_tombstone": [],
                    },
                )

        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine, engine.begin() as connection:
            attribute_fragment = _fragment(connection, attributes)
            address_fragment = _fragment(connection, addresses)
            route_fragment = _fragment(connection, routes)

    proof = attribute_fragment[EXECUTION_KEY]["proof"]
    assert proof["attribute_eligibility"] == {
        f"{interface_id}/description": True,
        f"{interface_id}/enabled": False,
    }
    assert proof["interfaces"][str(interface_id)]["name"] == "Gi0/1"
    # The ip lane contributes the interface record and no eligibility decision of its own.
    assert set(address_fragment[EXECUTION_KEY]["proof"]) == {"interfaces"}

    section = {
        **{table: rows for table, rows in attribute_fragment.items() if table != EXECUTION_KEY},
        **{table: rows for table, rows in address_fragment.items() if table != EXECUTION_KEY},
        EXECUTION_KEY: attribute_fragment[EXECUTION_KEY],
    }
    execution = hydrate_interface_execution({"interface_config": section})
    assert execution.eligible_attributes == frozenset({(interface_id, "description")})

    plan = hydrate_static_route_apply_plan({"static_route": route_fragment}, eligible_rows=[])
    assert plan.tombstone_ids == []
    assert route_fragment[EXECUTION_KEY]["proof"]["apply"]["mode"] in {"PATCH", "PUT"}


def test_the_downgrade_strips_execution_metadata_back_to_tables(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "execution_context_down") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as connection:
            device_id = _seed_device(connection, name="mig-down", netbox_id=16634, ned_id="cisco-ios-cli-3.8")
            stream_id = _seed_stream(connection, device_id, "vlan", _empty_tables("vlan"))

        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)

        with engine_on(sync_url) as engine, engine.begin() as connection:
            assert _fragment(connection, stream_id) == _empty_tables("vlan")
