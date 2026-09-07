# SPDX-License-Identifier: Apache-2.0
"""Aggregate collateral protection for prepared switching sections."""

from types import SimpleNamespace

import pytest

from tests.conftest import seed_device
from tests.core.test_action_apply_promotion import _put_vlans
from tests.core.test_generation_protocol import job_row, recorded_client, run_head, seed_settings

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("scope", "body"),
    [
        ("lag", {"bundle": [{"name": "Port-channel10", "member": [{"interface-name": "Ethernet1"}]}]}),
        ("switchport", {"interface": [{"interface-name": "Ethernet1", "tagged-vlan": [100]}]}),
    ],
)
async def test_unasserted_switching_root_blocks_unrelated_vlan_put(adapter_client, scope, body):
    device_id = await seed_device(nso_device_name="switching-collateral", netbox_device_id=17312)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    client, rec = recorded_client("switching-collateral")
    client.get_service_config.return_value = {scope: body}
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "failed", job.result
    assert not rec.documents


@pytest.mark.parametrize(
    ("scope", "wire", "label", "key", "body"),
    [
        (
            "lag",
            "lag-config",
            "lag_member_intent",
            ["Port-channel10", "Ethernet1"],
            {"bundle": [{"name": "Port-channel10", "member": [{"interface-name": "Ethernet1"}]}]},
        ),
        (
            "switchport",
            "switchport",
            "switchport_tagged_vlan_intent",
            ["Ethernet1", 100],
            {"interface": [{"interface-name": "Ethernet1", "tagged-vlan": [100]}]},
        ),
    ],
)
async def test_switching_child_guard_and_residue_use_authorized_identity(scope, wire, label, key, body):
    from nso_adapter.core.removal import _document_orphans, _residue_after_removal

    client, _ = recorded_client("switching-residue", device_state={wire: {"status": "ok", **body}})
    device = SimpleNamespace(nso_device_name="switching-residue")
    residue, unverifiable = await _residue_after_removal(client, device, scope, {"removed": {label: [key]}})
    assert residue == {label: [[str(part) for part in key]]}
    assert unverifiable == []
    retained_root = {
        scope: {
            next(iter(body)): [
                {k: v for k, v in next(iter(body.values()))[0].items() if k in {"name", "interface-name"}}
            ]
        }
    }
    assert _document_orphans({scope: body}, retained_root, {}) == {f"{scope}/{label}": [[str(p) for p in key]]}
    assert _document_orphans({scope: body}, retained_root, {scope: {label: [key]}}) == {}
