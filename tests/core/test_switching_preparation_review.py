# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Committed source identity, authorization, and refusal atomicity for switching streams."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core.switching_intent import (
    LagBundleSnapshot,
    LagMemberSnapshot,
    SwitchingRefusal,
    SwitchingRequestRefused,
    SwitchportSnapshot,
    replace_lag_snapshot,
    replace_switchport_snapshot,
)
from nso_adapter.store.db import get_engine
from tests.conftest import seed_device, session
from tests.core.test_action_apply_promotion import (
    _SHAPE,
    _SWITCHING_STREAMS,
    AUTH,
    _authorize,
    _prepare,
    _stream,
)
from tests.core.test_projection_lock_order import _backend_pid, _wait_for_blocked_query

pytestmark = pytest.mark.anyio


async def _replace(
    db, device_id: int, stream: str, roots: dict[str, list[int]], *, source_revision: int, deleted_roots=()
):
    if stream == "lag":
        bundles = tuple(
            LagBundleSnapshot(
                name=name,
                lag_id=index,
                members=tuple(LagMemberSnapshot(interface_name=f"Gi0/{child}") for child in children),
            )
            for index, (name, children) in enumerate(roots.items(), 1)
        )
        return await replace_lag_snapshot(
            db, device_id, bundles, source_revision=source_revision, deleted_roots=deleted_roots
        )
    interfaces = tuple(
        SwitchportSnapshot(interface_name=name, tagged_vlans=tuple(children)) for name, children in roots.items()
    )
    return await replace_switchport_snapshot(
        db, device_id, interfaces, source_revision=source_revision, deleted_roots=deleted_roots
    )


async def _complete_state(db, device_id: int, stream: str) -> dict:
    shape = _SHAPE[stream]
    tables = (
        "device_projection_stream",
        shape["root_table"],
        shape["child_table"],
        "deployment_generation",
        "device_generation_counter",
    )
    statements = {
        "device_projection_stream": "SELECT * FROM device_projection_stream WHERE device_id = :device_id AND stream = :stream",
        shape["root_table"]: f"SELECT * FROM {shape['root_table']} WHERE device_id = :device_id ORDER BY id",
        shape["child_table"]: (
            f"SELECT child.* FROM {shape['child_table']} AS child "
            f"JOIN {shape['root_table']} AS root ON root.id = child."
            f"{'lag_bundle_id' if stream == 'lag' else 'switchport_id'} "
            "WHERE root.device_id = :device_id ORDER BY child.id"
        ),
        "deployment_generation": "SELECT count(*) AS count FROM deployment_generation WHERE device_id = :device_id",
        "device_generation_counter": "SELECT * FROM device_generation_counter WHERE device_id = :device_id",
    }
    state = {}
    for table in tables:
        result = await db.execute(sa.text(statements[table]), {"device_id": device_id, "stream": stream})
        state[table] = [dict(row) for row in result.mappings().all()]
    return state


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_preparation_waits_for_committed_apply_authorization(adapter_client, rival_engine, stream):
    from nso_adapter.core.generation import create_action_apply
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt

    device_id = await seed_device(nso_device_name=f"prepare-after-apply-{stream}", netbox_device_id=None)
    prepared = await _prepare(adapter_client, device_id, stream, {"A": [1]})
    revision = prepared.json()["selection_revision"]
    source_revision = (await _stream(device_id, stream)).prepared_source_revision
    attempt_id = uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {stream: revision}) is None
        await db.commit()

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as applying, rival() as preparing:
        applying_pid = await _backend_pid(applying)
        preparing_pid = await _backend_pid(preparing)
        await create_action_apply(applying, device_id, {stream: revision}, attempt_id)
        assert (await _complete_state(applying, device_id, stream))["device_projection_stream"][0][
            "authorized_revision"
        ] == revision

        competing = asyncio.create_task(
            _replace(preparing, device_id, stream, {}, source_revision=source_revision + 1, deleted_roots=["A"])
        )
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=applying_pid,
                waiter_pid=preparing_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not competing.done()
            await applying.commit()
            result = await asyncio.wait_for(competing, timeout=10)
            await preparing.commit()
        finally:
            if not competing.done():
                competing.cancel()
                await asyncio.gather(competing, return_exceptions=True)

    assert result.unauthorized_deleted_roots == []
    row = await _stream(device_id, stream)
    shape = _SHAPE[stream]
    assert [item[shape["root_field"]] for item in row.prepared_deletions["delete_origin"][shape["root_table"]]] == ["A"]
    assert len(row.prepared_deletions["delete_origin"][shape["child_table"]]) == 1


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_preparation_waits_for_committed_newer_source(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"prepare-after-newer-{stream}", netbox_device_id=None)
    await _prepare(adapter_client, device_id, stream, {"A": [1]})
    source_revision = (await _stream(device_id, stream)).prepared_source_revision
    shape = _SHAPE[stream]
    async with session() as newer:
        newer_pid = await _backend_pid(newer)
        await _replace(newer, device_id, stream, {"B": [2]}, source_revision=source_revision + 2)
        competing = asyncio.create_task(
            adapter_client.post(
                f"/api/v1/devices/{device_id}/{shape['path']}",
                json={**shape["body"]({"C": [3]}), "source_revision": source_revision + 1},
                headers=AUTH,
            )
        )
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=newer_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not competing.done()
            await newer.commit()
            response = await asyncio.wait_for(competing, timeout=10)
        finally:
            if not competing.done():
                competing.cancel()
                await asyncio.gather(competing, return_exceptions=True)

    assert response.status_code == 409, response.text
    assert response.json()["error"]["detail"] == {"reason": "stale_preparation"}
    row = await _stream(device_id, stream)
    assert row.prepared_source_revision == source_revision + 2


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
@pytest.mark.parametrize("reason", list(SwitchingRefusal)[2:])
async def test_refusal_preserves_complete_state_before_rollback_and_after_api(adapter_client, stream, reason):
    device_id = await seed_device(nso_device_name=f"refusal-state-{stream}-{reason.value}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1], "B": [2]})
    evidence_at = datetime(2026, 9, 1, tzinfo=UTC)
    async with session() as db:
        await db.execute(
            sa.text(
                f"UPDATE {_SHAPE[stream]['root_table']} SET last_apply_at = :evidence_at, "
                "last_apply_error = CAST(:error AS json) WHERE device_id = :device_id"
            ),
            {"evidence_at": evidence_at, "error": '{"code": "previous_failure"}', "device_id": device_id},
        )
        await db.execute(
            sa.text("UPDATE device_generation_counter SET last_seq = last_seq + 73 WHERE device_id = :device_id"),
            {"device_id": device_id},
        )
        await db.commit()
    current = await _stream(device_id, stream)
    source_revision = current.prepared_source_revision
    roots = {"A": [1], "B": [2]}
    deleted_roots = []
    if reason is SwitchingRefusal.stale_preparation:
        source_revision -= 1
    elif reason is SwitchingRefusal.revision_conflict:
        roots = {"A": [1]}
    elif reason is SwitchingRefusal.root_still_present:
        deleted_roots = ["A"]
        source_revision += 1
    else:
        deleted_roots = ["A", "A"]
        source_revision += 1

    async with session() as db:
        before = await _complete_state(db, device_id, stream)
        assert before["device_projection_stream"][0]["authorized_document"]
        assert before[_SHAPE[stream]["root_table"]]
        assert all(root["last_apply_at"] == evidence_at for root in before[_SHAPE[stream]["root_table"]])
        assert before[_SHAPE[stream]["child_table"]]
        assert before["deployment_generation"][0]["count"] > 0
        assert before["device_generation_counter"][0]["last_seq"] >= 73
        with pytest.raises(SwitchingRequestRefused) as refusal:
            await _replace(db, device_id, stream, roots, source_revision=source_revision, deleted_roots=deleted_roots)
        assert refusal.value.reason is reason
        assert await _complete_state(db, device_id, stream) == before
        await db.rollback()

    shape = _SHAPE[stream]
    body = {**shape["body"](roots, deleted_roots=deleted_roots), "source_revision": source_revision}
    response = await adapter_client.post(f"/api/v1/devices/{device_id}/{shape['path']}", json=body, headers=AUTH)
    assert response.status_code == (
        409 if reason in {SwitchingRefusal.stale_preparation, SwitchingRefusal.revision_conflict} else 422
    )
    assert response.json()["error"]["detail"] == {"reason": reason.value}
    async with session() as db:
        assert await _complete_state(db, device_id, stream) == before


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_equal_source_accepts_empty_null_omitted_and_identical_snapshots(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"same-source-unset-{stream}", netbox_device_id=None)
    shape = _SHAPE[stream]
    if stream == "lag":
        root = {"name": "A", "lag_id": 1, "timer": "", "members": [{"interface_name": "Gi0/1", "mode": ""}]}
        field = "bundles"
    else:
        root = {"interface_name": "A", "mode": "", "tagged_vlans": [1]}
        field = "interfaces"

    async def post(item):
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/{shape['path']}",
            json={field: [item], "source_revision": 40, "deleted_roots": []},
            headers=AUTH,
        )

    first = await post(root)
    assert first.status_code == 200, first.text
    first_revision = first.json()["selection_revision"]

    if stream == "lag":
        snapshot = LagBundleSnapshot(
            name="A", lag_id=1, timer="", members=(LagMemberSnapshot(interface_name="Gi0/1", mode=""),)
        )
        async with session() as db:
            await replace_lag_snapshot(db, device_id, (snapshot,), source_revision=40, deleted_roots=[])
            await db.commit()
    else:
        snapshot = SwitchportSnapshot(interface_name="A", mode="", tagged_vlans=(1,))
        async with session() as db:
            await replace_switchport_snapshot(db, device_id, (snapshot,), source_revision=40, deleted_roots=[])
            await db.commit()

    explicit_null = (
        {**root, "mode": None}
        if stream == "switchport"
        else {**root, "timer": None, "members": [{"interface_name": "Gi0/1", "mode": None}]}
    )
    omitted = {
        key: value for key, value in root.items() if key not in ({"mode"} if stream == "switchport" else {"timer"})
    }
    if stream == "lag":
        omitted["members"] = [{"interface_name": "Gi0/1"}]
    for item in (root, explicit_null, omitted):
        response = await post(item)
        assert response.status_code == 200, response.text
    row = await _stream(device_id, stream)
    assert row.prepared_revision == first_revision + 4


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_equal_source_accepts_reordered_roots_and_children_but_refuses_changed_snapshot(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"same-source-order-{stream}", netbox_device_id=None)
    shape = _SHAPE[stream]
    if stream == "lag":
        field = "bundles"
        roots = [
            {
                "name": "A",
                "lag_id": 1,
                "members": [{"interface_name": "Gi0/1"}, {"interface_name": "Gi0/2"}],
            },
            {"name": "B", "lag_id": 2, "members": [{"interface_name": "Gi0/3"}]},
        ]
        reordered_children = [
            {**roots[0], "members": list(reversed(roots[0]["members"]))},
            roots[1],
        ]
        changed = [{**roots[0], "lag_id": 3}, roots[1]]
    else:
        field = "interfaces"
        roots = [
            {"interface_name": "A", "tagged_vlans": [10, 20]},
            {"interface_name": "B", "tagged_vlans": [30]},
        ]
        reordered_children = [{**roots[0], "tagged_vlans": [20, 10]}, roots[1]]
        changed = [{**roots[0], "tagged_vlans": [10, 40]}, roots[1]]

    async def post(items):
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/{shape['path']}",
            json={field: items, "source_revision": 40, "deleted_roots": []},
            headers=AUTH,
        )

    first = await post(roots)
    assert first.status_code == 200, first.text
    for items in (list(reversed(roots)), reordered_children):
        response = await post(items)
        assert response.status_code == 200, response.text
    response = await post(changed)
    assert response.status_code == 409, response.text
    assert response.json()["error"]["detail"] == {"reason": "revision_conflict"}


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_core_equal_source_reordered_snapshot_keeps_digest(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"core-same-source-order-{stream}", netbox_device_id=None)
    if stream == "lag":
        items = (
            LagBundleSnapshot(name="A", lag_id=1, members=(LagMemberSnapshot("Gi0/1"), LagMemberSnapshot("Gi0/2"))),
            LagBundleSnapshot(name="B", lag_id=2, members=(LagMemberSnapshot("Gi0/3"), LagMemberSnapshot("Gi0/4"))),
        )
        replace = replace_lag_snapshot
        reordered = tuple(
            LagBundleSnapshot(name=item.name, lag_id=item.lag_id, members=tuple(reversed(item.members)))
            for item in reversed(items)
        )
    else:
        items = (
            SwitchportSnapshot(interface_name="A", tagged_vlans=(10, 20)),
            SwitchportSnapshot(interface_name="B", tagged_vlans=(30, 40)),
        )
        replace = replace_switchport_snapshot
        reordered = tuple(
            SwitchportSnapshot(interface_name=item.interface_name, tagged_vlans=tuple(reversed(item.tagged_vlans)))
            for item in reversed(items)
        )

    async with session() as db:
        await replace(db, device_id, items, source_revision=40, deleted_roots=[])
        await db.commit()
    first_digest = (await _stream(device_id, stream)).prepared_source_digest
    assert first_digest is not None

    async with session() as db:
        await replace(db, device_id, reordered, source_revision=40, deleted_roots=[])
        await db.commit()
    assert (await _stream(device_id, stream)).prepared_source_digest == first_digest
