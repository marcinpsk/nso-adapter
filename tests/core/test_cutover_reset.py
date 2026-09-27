# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The aggregate cutover retires adapter authority and inherited obligations."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from nso_adapter.core.projection import projection_streams
from tests.conftest import seed_device, session
from tests.core.test_static_route_plan import A, B, _plan, _seed_rows

pytestmark = pytest.mark.anyio


async def test_store_only_route_identity_edit_does_not_inherit_pre_cutover_retraction(adapter_client):
    from nso_adapter.core.cutover import deauthorize_for_cutover
    from nso_adapter.core.generation import note_write
    from nso_adapter.store.models import DeviceProjectionStream, StaticRouteIntent
    from tests.core.removal_helpers import authorize_static_route

    device_id = await seed_device(nso_device_name="cutover-route-edit", netbox_device_id=None)
    await _seed_rows(device_id, [{"triple": A, "route_id": 1, "deployed_key": list(A)}])
    await authorize_static_route(device_id)
    async with session() as db:
        await db.execute(
            sa.update(DeviceProjectionStream)
            .where(DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "static_route")
            .values(applied_revision=1)
        )
        await note_write(db, device_id, "static_route")
        await db.execute(
            sa.update(StaticRouteIntent)
            .where(StaticRouteIntent.device_id == device_id)
            .values(vrf=B[0], prefix=B[1], next_hop=B[2])
        )
        await db.commit()

    before, _ = await _plan(device_id)
    assert A in before.allowed

    async with session() as db:
        await deauthorize_for_cutover(db)
        await db.commit()

    after, _ = await _plan(device_id)
    assert A not in after.allowed


async def test_unparked_tombstone_is_retired_and_second_reset_changes_nothing(adapter_client):
    from nso_adapter.core.cutover import deauthorize_for_cutover, parked_static_route_carriers
    from nso_adapter.store.models import DeviceProjectionStream, StaticRouteTombstone
    from tests.core.removal_helpers import seed_tomb

    device_id = await seed_device(nso_device_name="cutover-repeat", netbox_device_id=None)
    await _seed_rows(device_id, [{"triple": A, "route_id": 1}])
    await seed_tomb(device_id, A, route_id=2)
    async with session() as db:
        assert await parked_static_route_carriers(db) == []
        first = await deauthorize_for_cutover(db)
        await db.commit()
    assert first.devices[0].tombstones == 1

    async with session() as db:
        second = await deauthorize_for_cutover(db)
        assert second.devices == ()
        assert await db.scalar(sa.select(sa.func.count()).select_from(StaticRouteTombstone)) == 0
        row = await db.scalar(sa.select(DeviceProjectionStream).where(DeviceProjectionStream.device_id == device_id))
        assert row.authorized_document is None
        await db.commit()


async def test_parked_carrier_refuses_reset_without_clearing_authority(adapter_client):
    from nso_adapter.core.cutover import CutoverBlocked, deauthorize_for_cutover
    from nso_adapter.store.models import DeviceProjectionStream, StaticRouteTombstone
    from tests.core.removal_helpers import seed_tomb

    device_id = await seed_device(nso_device_name="cutover-parked-reset", netbox_device_id=None)
    tombstone_id = await seed_tomb(device_id, A, route_id=1)
    async with session() as db:
        before = await db.scalar(
            sa.select(DeviceProjectionStream.authorized_document).where(
                DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "static_route"
            )
        )
        with pytest.raises(CutoverBlocked) as caught:
            await deauthorize_for_cutover(db)
        assert caught.value.parked[0].tombstone_id == tombstone_id
        await db.rollback()
    async with session() as db:
        assert (
            await db.scalar(
                sa.select(DeviceProjectionStream.authorized_document).where(
                    DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "static_route"
                )
            )
            == before
        )
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(StaticRouteTombstone)
                .where(StaticRouteTombstone.device_id == device_id)
            )
            == 1
        )


@pytest.mark.parametrize("blocker", ("failed", "outcome_unknown", "queued_job"))
async def test_executable_history_refuses_reset_without_writing_authority(adapter_client, blocker):
    from nso_adapter.core.cutover import CutoverStateBlocked, deauthorize_for_cutover
    from nso_adapter.store.models import (
        DeploymentGeneration,
        DeviceProjectionStream,
        GenerationMode,
        GenerationStatus,
        Job,
        JobStatus,
        JobType,
    )

    device_id = await seed_device(nso_device_name=f"cutover-blocked-{blocker}", netbox_device_id=None)
    async with session() as db:
        db.add(DeviceProjectionStream(device_id=device_id, stream="vlan", authorized_document={"vlan_intent": []}))
        if blocker == "queued_job":
            offender = Job(device_id=device_id, job_type=JobType.sync, status=JobStatus.queued, coalescible=False)
        else:
            offender = DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus[blocker],
                document={},
                digest="0" * 64,
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
            )
        db.add(offender)
        await db.commit()
        offender_id = offender.id

    async with session() as db:
        with pytest.raises(CutoverStateBlocked) as caught:
            await deauthorize_for_cutover(db)
        assert f"device {device_id}" in str(caught.value)
        assert f"{'job' if blocker == 'queued_job' else 'generation'} {offender_id}" in str(caught.value)
        assert blocker.replace("_job", "") in str(caught.value)
        await db.rollback()
    async with session() as db:
        assert await db.scalar(
            sa.select(DeviceProjectionStream.authorized_document).where(DeviceProjectionStream.device_id == device_id)
        ) == {"vlan_intent": []}


async def test_reset_drops_prepared_omission_before_next_document(adapter_client):
    from uuid import uuid4

    from nso_adapter.core.cutover import deauthorize_for_cutover
    from nso_adapter.core.generation import create_action_apply
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.core.switching_intent import LagBundleSnapshot, replace_lag_snapshot
    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="cutover-preparation", netbox_device_id=None)
    async with session() as db:
        await replace_lag_snapshot(
            db, device_id, (LagBundleSnapshot(name="A", lag_id=1),), source_revision=1, deleted_roots=[]
        )
        original = await snapshot_stream(db, device_id, "lag")
        await db.execute(
            sa.update(DeviceProjectionStream)
            .where(DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "lag")
            .values(authorized_document=original, authorized_revision=1, applied_revision=1)
        )
        await db.commit()
    async with session() as db:
        await replace_lag_snapshot(db, device_id, (), source_revision=2, deleted_roots=[])
        row = await db.scalar(sa.select(DeviceProjectionStream).where(DeviceProjectionStream.device_id == device_id))
        assert row.prepared_deletions["detach"]["lag_bundle_intent"][0]["name"] == "A"
        await db.commit()

    async with session() as db:
        await deauthorize_for_cutover(db)
        await db.commit()
    async with session() as db:
        row = await db.scalar(sa.select(DeviceProjectionStream).where(DeviceProjectionStream.device_id == device_id))
        assert (
            row.prepared_revision,
            row.prepared_tables,
            row.prepared_deletions,
            row.prepared_source_revision,
            row.prepared_source_digest,
        ) == (None, None, None, None, None)
        fresh = await replace_lag_snapshot(
            db, device_id, (LagBundleSnapshot(name="B", lag_id=2),), source_revision=3, deleted_roots=[]
        )
        await db.commit()
    async with session() as db:
        result = await create_action_apply(db, device_id, {"lag": fresh.selection_revision}, uuid4())
        assert result.generations
        assert all(
            {row["name"] for row in generation.document.get("lag", {}).get("lag_bundle_intent", [])} == {"B"}
            for generation in result.generations
        )
        await db.rollback()


async def test_store_only_pending_clear_no_longer_blocks_settlement_obligation(adapter_client):
    from nso_adapter.core.cutover import deauthorize_for_cutover
    from nso_adapter.core.generation import lock_projection
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.core.static_route_plan import pending_clear_fields
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="cutover-pending-clear", netbox_device_id=None)
    await _seed_rows(device_id, [{"triple": A, "route_id": 1}])
    async with session() as db:
        row = await db.scalar(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
        row.pending_clear = {"authorized": ["tag"], "store_only": ["metric"]}
        await db.commit()
    async with session() as db:
        row = await db.scalar(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
        assert pending_clear_fields(row.pending_clear) == {"tag", "metric"}
        await deauthorize_for_cutover(db)
        await db.commit()
    async with session() as db:
        row = await db.scalar(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
        assert row.pending_clear is None
        assert pending_clear_fields(row.pending_clear) == set()
        await lock_projection(db, device_id)
        fragment = await snapshot_stream(db, device_id, "static_route")
        assert pending_clear_fields(fragment["static_route_intent"][0]["pending_clear"]) == set()
        await db.rollback()


async def test_reset_clears_all_stream_authority_and_preserves_desired_state(adapter_client):
    from nso_adapter.core.cutover import deauthorize_for_cutover
    from nso_adapter.store.models import Base, DeviceProjectionStream, StreamPendingClear, VlanIntent

    device_id = await seed_device(nso_device_name="cutover-all-streams", netbox_device_id=None)
    second_device_id = await seed_device(nso_device_name="cutover-second-device", netbox_device_id=None)
    await _seed_rows(device_id, [{"triple": A, "route_id": 1}])
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=10, name="test-vlan"))
        db.add(VlanIntent(device_id=second_device_id, vlan_id=20, name="second-vlan"))
        db.add(StreamPendingClear(device_id=device_id, stream="vlan", provenance="store_only", revision=3))
        db.add(
            DeviceProjectionStream(
                device_id=second_device_id,
                stream="vlan",
                desired_revision=4,
                authorized_revision=4,
                applied_revision=4,
                authorized_document={"vlan_intent": []},
            )
        )
        for stream in projection_streams():
            db.add(
                DeviceProjectionStream(
                    device_id=device_id,
                    stream=stream,
                    desired_revision=3,
                    authorized_revision=2,
                    applied_revision=1,
                    source_push_seq=7,
                    authorized_document={"authorized": []},
                )
            )
        await db.commit()
    intent_tables = [table for table in Base.metadata.tables.values() if table.name.endswith("_intent")]
    async with session() as db:
        desired_counts = {
            table.name: await db.scalar(sa.select(sa.func.count()).select_from(table)) for table in intent_tables
        }
        revisions = {
            (device, stream): (desired, push_seq)
            for device, stream, desired, push_seq in (
                await db.execute(
                    sa.select(
                        DeviceProjectionStream.device_id,
                        DeviceProjectionStream.stream,
                        DeviceProjectionStream.desired_revision,
                        DeviceProjectionStream.source_push_seq,
                    )
                )
            ).all()
        }
        reset = await deauthorize_for_cutover(db)
        await db.commit()
    assert reset.devices[0].streams == len(projection_streams()) == 18
    assert reset.devices[0].stream_pending_clears == 1
    assert reset.devices[1].device_id == second_device_id
    assert reset.devices[1].streams == 1
    async with session() as db:
        assert await db.scalar(sa.select(sa.func.count()).select_from(StreamPendingClear)) == 0
        assert (
            await db.scalar(
                sa.text("SELECT count(*) FROM device_projection_stream WHERE authorized_document IS NOT NULL")
            )
            == 0
        )
        assert (
            await db.scalar(
                sa.text("SELECT count(*) FROM device_projection_stream WHERE authorized_document::text = '{}'")
            )
            == 0
        )
        assert {
            table.name: await db.scalar(sa.select(sa.func.count()).select_from(table)) for table in intent_tables
        } == desired_counts
        assert {
            (device, stream): (desired, push_seq)
            for device, stream, desired, push_seq in (
                await db.execute(
                    sa.select(
                        DeviceProjectionStream.device_id,
                        DeviceProjectionStream.stream,
                        DeviceProjectionStream.desired_revision,
                        DeviceProjectionStream.source_push_seq,
                    )
                )
            ).all()
        } == revisions


@pytest.mark.parametrize("unknown", ("table", "column"))
def test_schema_pin_refuses_unknown_device_table_or_column(unknown):
    from nso_adapter.core.cutover import CutoverSchemaBlocked, _refuse_unreviewed_schema
    from nso_adapter.store.models import Base

    metadata = sa.MetaData()
    for table in Base.metadata.tables.values():
        table.to_metadata(metadata)
    if unknown == "table":
        sa.Table("new_device_obligation", metadata, sa.Column("device_id", sa.Integer))
        name = "new_device_obligation.device_id"
    else:
        metadata.tables["device_projection_stream"].append_column(sa.Column("new_obligation", sa.Integer))
        name = "device_projection_stream.new_obligation"
    with pytest.raises(CutoverSchemaBlocked, match=name):
        _refuse_unreviewed_schema(metadata)
