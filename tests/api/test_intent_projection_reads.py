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
