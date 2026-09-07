# SPDX-License-Identifier: Apache-2.0
"""Preview and commit resolve the same frozen execution policy."""

from unittest.mock import patch

import pytest

from nso_adapter.core.apply import collect_apply_diff
from tests.conftest import seed_device, session
from tests.core.removal_helpers import authorize_stream
from tests.core.test_action_apply_promotion import AUTH, _put_vlans
from tests.core.test_generation_protocol import job_row, run_head, seed_settings
from tests.core.test_static_route_put import A
from tests.core.test_static_route_removal import SrFake, sr_client
from tests.core.test_static_route_retention import _RICH_A, _carrier_for

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("operation", ["force", "detach", "retained"])
async def test_preview_and_commit_share_frozen_removal_policy(adapter_client, operation):
    from nso_adapter.core.removal import enqueue_removal

    device_id = await seed_device(nso_device_name="preview-policy", netbox_device_id=17314)
    await _carrier_for(device_id, A, route_id=1)
    await seed_settings(device_id, auto_apply=operation == "retained")
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    if operation != "retained":
        await authorize_stream(device_id, "vlan")
    if operation == "force":
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/actions/force-removal", json={"scope": "static_route"}, headers=AUTH
        )
        assert response.status_code == 202, response.text
    elif operation == "detach":
        async with session() as db:
            await enqueue_removal(db, device_id, "vlan", marking="detach", defer_retract=False, promotes=("vlan",))
            await db.commit()
    fake = SrFake("preview-policy", service=[_RICH_A])
    client = sr_client(fake)
    async with session() as db:
        with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
            await collect_apply_diff(db, device_id)
    preview = next(call for call in fake.calls if call["dry_run"] and call["body"])
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    commit = next(call for call in fake.calls if call["method"] == "put" and not call["dry_run"])
    assert preview["body"] == commit["body"]
    assert preview["no_networking"] == commit["no_networking"] == (operation == "detach")
    assert fake.sent_routes() == ([] if operation == "force" else [_RICH_A])
