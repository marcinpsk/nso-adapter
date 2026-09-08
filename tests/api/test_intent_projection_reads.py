# SPDX-License-Identifier: Apache-2.0
"""Boundary validation against the real store."""

import sqlalchemy as sa

from tests.conftest import seed_device


async def test_disabled_auto_apply_does_not_snapshot_for_admission(adapter_client):
    from nso_adapter.store.db import get_engine
    from tests.api.test_api_intent import AUTH
    from tests.conftest import push_seq
    from tests.core.test_generation_protocol import seed_settings

    device_id = await seed_device(nso_device_name="snapshot-disabled")
    await seed_settings(device_id, auto_apply=False)
    snapshots = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FROM interface_intent" in statement:
            snapshots.append(statement)

    engine = get_engine().sync_engine
    sa.event.listen(engine, "before_cursor_execute", observe)
    try:
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/intent", json={"attributes": []}, headers=AUTH | push_seq()
        )
    finally:
        sa.event.remove(engine, "before_cursor_execute", observe)
    assert response.status_code == 200, response.text
    assert len(snapshots) == 1, snapshots


async def _put_attrs(client, device_id: int, attributes: list[dict], *, query: str = ""):
    from tests.api.test_api_intent import AUTH
    from tests.conftest import push_seq

    return await client.put(
        f"/api/v1/devices/{device_id}/intent{query}",
        json={"attributes": attributes},
        headers=AUTH | push_seq(),
    )


async def _promotion_counts(device_id: int) -> tuple[int, int]:
    """How many deployment generations and jobs this device carries."""
    from nso_adapter.store.models import DeploymentGeneration, Job
    from tests.conftest import session

    async with session() as db:
        generations = await db.scalar(
            sa.select(sa.func.count())
            .select_from(DeploymentGeneration)
            .where(DeploymentGeneration.device_id == device_id)
        )
        jobs = await db.scalar(sa.select(sa.func.count()).select_from(Job).where(Job.device_id == device_id))
    return generations, jobs


async def _mirror(client, device_id: int) -> list[tuple[str, str]]:
    from tests.api.test_api_intent import AUTH

    response = await client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert response.status_code == 200, response.text
    return sorted((row["interface"], row["attribute"]) for row in response.json()["attributes"])


async def test_store_only_shrink_never_reaches_the_promotion_chain(adapter_client):
    """A store-only re-sync that drops an authorized attribute stores it and promotes nothing.

    The removal branch used to run under ``store_only`` and hit ``create_generation``, which
    refuses a store-only promotion outright, so a legitimate re-sync answered 500.
    """
    from nso_adapter.store.models import DeviceProjectionStream
    from tests.conftest import session
    from tests.core.test_generation_protocol import seed_settings

    device_id = await seed_device(nso_device_name="store-only-shrink")
    await seed_settings(device_id, auto_apply=True)

    first = await _put_attrs(
        adapter_client,
        device_id,
        [
            {"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "core link"},
            {"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "spare"},
        ],
    )
    assert first.status_code == 200, first.text
    async with session() as db:
        projection = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "interface_config",
            )
        )
        authorized = projection.authorized_document
    assert len(authorized["interface_intent"]) == 2, "setup broken: the first push authorized nothing"
    promoted = await _promotion_counts(device_id)

    # Mixed shrink: one attribute stays, one is omitted.
    kept = await _put_attrs(
        adapter_client,
        device_id,
        [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "core link"}],
        query="?store_only=true",
    )
    assert kept.status_code == 200, kept.text
    assert await _mirror(adapter_client, device_id) == [("GigabitEthernet0/1", "description")]
    assert await _promotion_counts(device_id) == promoted, "a store-only shrink promoted or enqueued"

    # Full shrink: every authorized attribute is omitted.
    emptied = await _put_attrs(adapter_client, device_id, [], query="?store_only=true")
    assert emptied.status_code == 200, emptied.text
    assert await _mirror(adapter_client, device_id) == []
    assert await _promotion_counts(device_id) == promoted, "a store-only shrink promoted or enqueued"

    async with session() as db:
        still = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "interface_config",
            )
        )
    assert still.authorized_document == authorized, "a store-only shrink authorized something"
