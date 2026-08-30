# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Behavioral tests for durable LAG and switchport desired-state snapshots."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload

from nso_adapter.core.generation import lock_device_document
from nso_adapter.core.switching_intent import (
    LagBundleSnapshot,
    LagMemberSnapshot,
    SwitchportSnapshot,
    render_switching_sections,
    replace_lag_snapshot,
    replace_switchport_snapshot,
)
from nso_adapter.store.models import (
    DeviceGenerationCounter,
    LagBundleIntent,
    LagMemberIntent,
    SwitchportIntent,
    SwitchportTaggedVlanIntent,
)
from tests.conftest import seed_device, session


@pytest.mark.anyio
async def test_lag_replacement_rejects_duplicate_keys_without_mutating_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="lag-core-validation", netbox_device_id=1612)
    original = LagBundleSnapshot(name="Port-channel1", lag_id=1)
    async with session() as db:
        await replace_lag_snapshot(db, device_id, (original,))
        await db.commit()

    async with session() as db:
        with pytest.raises(ValueError, match="duplicate LAG bundle name"):
            await replace_lag_snapshot(
                db,
                device_id,
                (
                    LagBundleSnapshot(name="Port-channel2", lag_id=2),
                    LagBundleSnapshot(name="Port-channel2", lag_id=3),
                ),
            )
        await db.rollback()

    async with session() as db:
        rows = (await db.execute(select(LagBundleIntent).where(LagBundleIntent.device_id == device_id))).scalars().all()
    assert [(row.name, row.lag_id) for row in rows] == [("Port-channel1", 1)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("bundle", "message"),
    [
        (LagBundleSnapshot(name="Port-channel1", lag_id=True), "lag_id must be a uint32"),
        (LagBundleSnapshot(name="Port-channel1", lag_id=4294967296), "lag_id must be a uint32"),
        (LagBundleSnapshot(name="Port-channel1", min_links=-1), "min_links must be a uint16"),
        (
            LagBundleSnapshot(
                name="Port-channel1",
                members=(LagMemberSnapshot(interface_name="Gi0/1", port_priority=65536),),
            ),
            "port_priority must be a uint16",
        ),
    ],
)
async def test_lag_replacement_rejects_invalid_yang_values_before_mutation(adapter_client, bundle, message):
    device_id = await seed_device(nso_device_name=f"lag-core-range-{message}", netbox_device_id=None)
    async with session() as db:
        with pytest.raises(ValueError, match=message):
            await replace_lag_snapshot(db, device_id, (bundle,))
        await db.rollback()

    async with session() as db:
        rows = (await db.execute(select(LagBundleIntent).where(LagBundleIntent.device_id == device_id))).scalars().all()
    assert rows == []


@pytest.mark.anyio
async def test_lag_replacement_preserves_identity_and_only_clears_evidence_on_change(adapter_client):
    device_id = await seed_device(nso_device_name="lag-core-lifecycle", netbox_device_id=1613)
    original = LagBundleSnapshot(
        name="Port-channel1",
        lag_id=1,
        members=(LagMemberSnapshot(interface_name="Gi0/1", mode="active", port_priority=100),),
    )
    evidence_at = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        await replace_lag_snapshot(db, device_id, (original,))
        row = await db.scalar(
            select(LagBundleIntent)
            .where(LagBundleIntent.device_id == device_id)
            .options(selectinload(LagBundleIntent.members))
        )
        assert row is not None
        root_id = row.id
        member_id = row.members[0].id
        accepted_at = row.accepted_at
        row.last_apply_at = evidence_at
        row.last_apply_error = {"code": "previous_failure"}
        await db.commit()

    async with session() as db:
        await replace_lag_snapshot(db, device_id, (original,))
        await db.commit()
    async with session() as db:
        unchanged = await db.scalar(
            select(LagBundleIntent)
            .where(LagBundleIntent.device_id == device_id)
            .options(selectinload(LagBundleIntent.members))
        )
        assert unchanged is not None
        assert (unchanged.id, unchanged.members[0].id) == (root_id, member_id)
        assert unchanged.accepted_at == accepted_at
        assert unchanged.last_apply_at == evidence_at
        assert unchanged.last_apply_error == {"code": "previous_failure"}

    changed = LagBundleSnapshot(
        name="Port-channel1",
        lag_id=1,
        members=(LagMemberSnapshot(interface_name="Gi0/1", mode="active", port_priority=200),),
    )
    async with session() as db:
        await replace_lag_snapshot(db, device_id, (changed,))
        await db.commit()
    async with session() as db:
        row = (
            await db.execute(
                text(
                    "SELECT id, accepted_at, last_apply_at, last_apply_error, "
                    "last_apply_error IS NULL AS error_is_null "
                    "FROM lag_bundle_intent WHERE device_id = :device_id"
                ),
                {"device_id": device_id},
            )
        ).one()
        changed_member_id = await db.scalar(
            select(LagMemberIntent.id)
            .join(LagBundleIntent, LagBundleIntent.id == LagMemberIntent.lag_bundle_id)
            .where(LagBundleIntent.device_id == device_id)
        )

    assert row.id == root_id
    assert changed_member_id == member_id
    assert row.accepted_at != accepted_at
    assert row.last_apply_at is None
    assert row.last_apply_error is None
    assert row.error_is_null is True


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("interfaces", "message"),
    [
        (
            (SwitchportSnapshot(interface_name="Gi0/1"), SwitchportSnapshot(interface_name="Gi0/1")),
            "duplicate switchport interface_name",
        ),
        (
            (SwitchportSnapshot(interface_name="Gi0/1", tagged_vlans=(10, 10)),),
            "duplicate tagged VLAN",
        ),
        (
            (SwitchportSnapshot(interface_name="Gi0/1", untagged_vlan=True),),
            "untagged_vlan must be a uint16",
        ),
        (
            (SwitchportSnapshot(interface_name="Gi0/1", tagged_vlans=(65536,)),),
            "tagged VLAN must be a uint16",
        ),
    ],
)
async def test_switchport_replacement_rejects_invalid_graph_before_mutation(adapter_client, interfaces, message):
    device_id = await seed_device(nso_device_name=f"switchport-core-{message}", netbox_device_id=None)
    async with session() as db:
        with pytest.raises(ValueError, match=message):
            await replace_switchport_snapshot(db, device_id, interfaces)
        await db.rollback()

    async with session() as db:
        rows = (
            (await db.execute(select(SwitchportIntent).where(SwitchportIntent.device_id == device_id))).scalars().all()
        )
    assert rows == []


@pytest.mark.anyio
async def test_switching_renderer_is_canonical_and_omits_empty_values_and_families(adapter_client):
    device_id = await seed_device(nso_device_name="switching-render", netbox_device_id=1614)
    async with session() as db:
        await replace_lag_snapshot(
            db,
            device_id,
            (
                LagBundleSnapshot(name="Port-channel2", lag_id=7),
                LagBundleSnapshot(
                    name="Port-channel1",
                    lag_id=None,
                    members=(
                        LagMemberSnapshot(interface_name="Gi0/2"),
                        LagMemberSnapshot(interface_name="Gi0/1", mode="active"),
                    ),
                ),
            ),
        )
        await replace_switchport_snapshot(
            db,
            device_id,
            (
                SwitchportSnapshot(interface_name="Gi0/2", tagged_vlans=(30, 20)),
                SwitchportSnapshot(interface_name="Gi0/1", mode="access", untagged_vlan=10),
            ),
        )
        await db.commit()

    async with session() as db:
        await lock_device_document(db, device_id)
        rendered = await render_switching_sections(db, device_id)
        counter = await db.get(DeviceGenerationCounter, device_id)
        await db.rollback()

    assert rendered == {
        "lag": {
            "bundle": [
                {
                    "name": "Port-channel1",
                    "member": [
                        {"interface-name": "Gi0/1", "mode": "active"},
                        {"interface-name": "Gi0/2"},
                    ],
                },
                {"name": "Port-channel2", "lag-id": 7},
            ]
        },
        "switchport": {
            "interface": [
                {"interface-name": "Gi0/1", "mode": "access", "untagged-vlan": 10},
                {"interface-name": "Gi0/2", "tagged-vlan": [20, 30]},
            ]
        },
    }
    assert counter is None

    empty_device_id = await seed_device(nso_device_name="switching-render-empty", netbox_device_id=1615)
    async with session() as db:
        await lock_device_document(db, empty_device_id)
        assert await render_switching_sections(db, empty_device_id) == {}
        await db.rollback()


@pytest.mark.anyio
async def test_switchport_replacement_preserves_root_and_retained_tag_identity(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-core-lifecycle", netbox_device_id=1616)
    original = SwitchportSnapshot(interface_name="Gi0/1", mode="trunk", tagged_vlans=(10, 20))
    evidence_at = datetime(2026, 8, 2, tzinfo=UTC)
    async with session() as db:
        await replace_switchport_snapshot(db, device_id, (original,))
        row = await db.scalar(
            select(SwitchportIntent)
            .where(SwitchportIntent.device_id == device_id)
            .options(selectinload(SwitchportIntent.tagged_vlans))
        )
        assert row is not None
        root_id = row.id
        tag_ids = {tag.vlan_id: tag.id for tag in row.tagged_vlans}
        accepted_at = row.accepted_at
        row.last_apply_at = evidence_at
        row.last_apply_error = {"code": "previous_failure"}
        await db.commit()

    async with session() as db:
        await replace_switchport_snapshot(db, device_id, (original,))
        await db.commit()
    async with session() as db:
        unchanged = await db.scalar(
            select(SwitchportIntent)
            .where(SwitchportIntent.device_id == device_id)
            .options(selectinload(SwitchportIntent.tagged_vlans))
        )
        assert unchanged is not None
        assert unchanged.id == root_id
        assert {tag.vlan_id: tag.id for tag in unchanged.tagged_vlans} == tag_ids
        assert unchanged.accepted_at == accepted_at
        assert unchanged.last_apply_at == evidence_at
        assert unchanged.last_apply_error == {"code": "previous_failure"}

    changed = SwitchportSnapshot(interface_name="Gi0/1", mode="trunk", tagged_vlans=(20, 30))
    async with session() as db:
        await replace_switchport_snapshot(db, device_id, (changed,))
        await db.commit()
    async with session() as db:
        row = await db.scalar(
            select(SwitchportIntent)
            .where(SwitchportIntent.device_id == device_id)
            .options(selectinload(SwitchportIntent.tagged_vlans))
        )
        assert row is not None
        retained_tag_id = await db.scalar(
            select(SwitchportTaggedVlanIntent.id).where(
                SwitchportTaggedVlanIntent.switchport_id == row.id,
                SwitchportTaggedVlanIntent.vlan_id == 20,
            )
        )

    assert row.id == root_id
    assert retained_tag_id == tag_ids[20]
    assert row.accepted_at != accepted_at
    assert row.last_apply_at is None
    assert row.last_apply_error is None


@pytest.mark.anyio
async def test_replacements_keep_loaded_child_collections_current(adapter_client):
    device_id = await seed_device(nso_device_name="switching-loaded-collections", netbox_device_id=1617)
    async with session() as db:
        await replace_lag_snapshot(
            db,
            device_id,
            (
                LagBundleSnapshot(
                    name="Port-channel1",
                    lag_id=1,
                    members=(
                        LagMemberSnapshot(interface_name="Gi0/1"),
                        LagMemberSnapshot(interface_name="Gi0/2"),
                    ),
                ),
            ),
        )
        await replace_switchport_snapshot(
            db,
            device_id,
            (SwitchportSnapshot(interface_name="Gi0/3", mode="trunk", tagged_vlans=(10, 20)),),
        )
        lag_row = await db.scalar(
            select(LagBundleIntent)
            .where(LagBundleIntent.device_id == device_id)
            .options(selectinload(LagBundleIntent.members))
        )
        switchport_row = await db.scalar(
            select(SwitchportIntent)
            .where(SwitchportIntent.device_id == device_id)
            .options(selectinload(SwitchportIntent.tagged_vlans))
        )
        assert lag_row is not None
        assert switchport_row is not None

        await replace_lag_snapshot(
            db,
            device_id,
            (
                LagBundleSnapshot(
                    name="Port-channel1",
                    lag_id=1,
                    members=(
                        LagMemberSnapshot(interface_name="Gi0/2"),
                        LagMemberSnapshot(interface_name="Gi0/4"),
                    ),
                ),
            ),
        )
        await replace_switchport_snapshot(
            db,
            device_id,
            (SwitchportSnapshot(interface_name="Gi0/3", mode="trunk", tagged_vlans=(20, 30)),),
        )

        assert {member.interface_name for member in lag_row.members} == {"Gi0/2", "Gi0/4"}
        assert {tag.vlan_id for tag in switchport_row.tagged_vlans} == {20, 30}
        await db.rollback()


def test_obsolete_direct_nso_switching_paths_are_absent():
    repository = Path(__file__).resolve().parents[2]
    assert not (repository / "nso_adapter/core/lag_intent.py").exists()
    assert not (repository / "nso_adapter/core/switchport_intent.py").exists()

    apply_source = (repository / "nso_adapter/nso/apply.py").read_text()
    apply_tree = ast.parse(apply_source)
    function_names = {
        node.name for node in ast.walk(apply_tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert "apply_lag_config" not in function_names
    assert "apply_switchport_config" not in function_names
    assert "_LAG_SERVICE_PATH" not in apply_source
    assert "_SWITCHPORT_SERVICE_PATH" not in apply_source

    lag_api = (repository / "nso_adapter/api/lag_config.py").read_text()
    switchport_api = (repository / "nso_adapter/api/vlan.py").read_text()
    assert "get_nso_client" not in lag_api
    assert "get_nso_client" not in switchport_api
