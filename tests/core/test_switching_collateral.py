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


#: Per prepared section: the read family, the child guard grain, and the identities the
#: fixtures below key by. The document and the export do NOT share a shape (``bundle`` vs
#: ``lag``, a ``tagged-vlan`` leaf-list vs a ``tagged-vlans`` range string), so a fixture
#: that replays the transmitted service body as reader data proves nothing.
_SHAPES = {
    "lag": {"wire": "lag-config", "label": "lag_member_intent", "root": "Port-channel10", "child": "Ethernet1"},
    "switchport": {"wire": "switchport", "label": "switchport_tagged_vlan_intent", "root": "Ethernet1", "child": 100},
}


def _service_body(scope: str, root: str, child) -> dict:
    """The section body one device-intent document carries for that root and child."""
    if scope == "lag":
        return {"bundle": [{"name": root, "member": [{"interface-name": child}]}]}
    return {"interface": [{"interface-name": root, "tagged-vlan": [child]}]}


def _export_body(scope: str, root: str, child) -> dict:
    """The same device state as network-state-export renders it."""
    if scope == "lag":
        return {"lag": [{"name": root, "member": [{"interface-name": child}]}]}
    return {"interface": [{"interface-name": root, "tagged-vlans": f"98,{child},1518-1519"}]}


@pytest.mark.parametrize("scope", ["lag", "switchport"])
async def test_switching_child_guard_and_residue_use_authorized_identity(scope):
    """The guard reads the service shape and the residue check the export shape.

    A surviving LAG member or tagged VLAN must be REPORTED: an empty intersection would
    read as a clean bill for config the removal never took off the device.
    """
    from nso_adapter.core.removal import _document_orphans, _residue_after_removal

    shape = _SHAPES[scope]
    label, root, child = shape["label"], shape["root"], shape["child"]
    key = [root, child]
    service = _service_body(scope, root, child)
    export = _export_body(scope, root, child)
    client, _ = recorded_client("switching-residue", device_state={shape["wire"]: {"status": "ok", **export}})
    device = SimpleNamespace(nso_device_name="switching-residue")
    residue, unverifiable = await _residue_after_removal(client, device, scope, {"removed": {label: [key]}})
    assert residue == {label: [[str(part) for part in key]]}
    assert unverifiable == []

    absent = "Ethernet9" if scope == "lag" else 4000
    gone, _ = await _residue_after_removal(client, device, scope, {"removed": {label: [[root, absent]]}})
    assert gone == {}, "a key the export does not carry is clean, not found"

    retained_root = {
        scope: {
            next(iter(service)): [
                {k: v for k, v in next(iter(service.values()))[0].items() if k in {"name", "interface-name"}}
            ]
        }
    }
    assert _document_orphans({scope: service}, retained_root, {}) == {f"{scope}/{label}": [[str(p) for p in key]]}
    assert _document_orphans({scope: service}, retained_root, {scope: {label: [key]}}) == {}


@pytest.mark.parametrize("scope", ["lag", "switchport"])
@pytest.mark.parametrize("remove_root", [False, True])
@pytest.mark.parametrize("survives", [False, True])
async def test_prepared_removal_worker_checks_root_and_child_residue(adapter_client, scope, remove_root, survives):
    from tests.core.test_action_apply_promotion import _apply, _prepare

    device_id = await seed_device(nso_device_name="prepared-residue", netbox_device_id=17319)
    await seed_settings(device_id, auto_apply=False)
    root = _SHAPES[scope]["root"]
    wire = _SHAPES[scope]["wire"]
    child = "Gi0/100" if scope == "lag" else 100  # what _prepare's {root: [100]} names
    revision = (await _prepare(adapter_client, device_id, scope, {root: [100]})).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {scope: revision})).status_code == 202
    device_state = {}
    client, rec = recorded_client("prepared-residue", device_state=device_state)
    assert (await job_row(await run_head(device_id, client))).status.value == "succeeded"
    previous = rec.documents[-1]
    client.get_service_config.return_value = previous
    # The far side of the writer answers in the EXPORT shape, never in the service shape
    # the document just transmitted.
    device_state[wire] = {"status": "ok", **(_export_body(scope, root, child) if survives else {})}
    response = await _prepare(
        adapter_client,
        device_id,
        scope,
        {} if remove_root else {root: []},
        deleted_roots=[root] if remove_root else [],
    )
    assert response.status_code == 200, response.text
    assert (await _apply(adapter_client, device_id, {scope: response.json()["selection_revision"]})).status_code == 202
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    assert job.result["residue_check"] == ("found" if survives else "clean")
