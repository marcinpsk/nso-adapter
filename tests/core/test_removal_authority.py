# SPDX-License-Identifier: Apache-2.0
"""Boundary validation against the real store."""

import pytest

from nso_adapter.core.removal import enqueue_removal, guard_allowed
from nso_adapter.store.models import DeploymentGeneration
from tests.conftest import seed_device, session


async def test_removal_admission_stores_scope_qualified_authority(adapter_client):
    from nso_adapter.core.generation import note_write
    from tests.core.test_generation_protocol import generations

    device_id = await seed_device(nso_device_name="qualified-removal")
    async with session() as db:
        await note_write(db, device_id, "vlan")
        await enqueue_removal(
            db,
            device_id,
            "vlan",
            marking="delete_origin",
            defer_retract=False,
            promotes=("vlan",),
            removed={"vlan": [[100]]},
        )
        await db.commit()
    generation = (await generations(device_id))[-1]
    assert generation.allowed_removal_keys == {"vlan": {"vlan": [[100]]}}
    assert guard_allowed(generation) == generation.allowed_removal_keys


def test_guard_refuses_unqualified_stored_authority():
    generation = DeploymentGeneration(allowed_removal_keys={"vlan": [[100]]})
    with pytest.raises(ValueError, match="scope-qualified"):
        guard_allowed(generation)


async def test_generation_refuses_unqualified_authority(adapter_client):
    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.store.models import GenerationMode

    device_id = await seed_device(nso_device_name="invalid-authority")
    async with session() as db:
        await note_write(db, device_id, "vlan")
        with pytest.raises(ValueError, match="scope-qualified"):
            await create_generation(
                db,
                device_id,
                streams=("vlan",),
                mode=GenerationMode.networked,
                allowed_removal_keys={"vlan": [[100]]},
            )


def test_static_route_reader_refuses_unqualified_authority():
    from nso_adapter.core.static_route_plan import _removal_keys

    with pytest.raises(ValueError, match="scope-qualified"):
        _removal_keys({"route": [["", "198.18.0.0/24", "198.18.1.1"]]})


def test_a_live_row_without_its_list_key_refuses_instead_of_reading_clean():
    """The orphan walk indexes live rows BY key, so a key-less row cannot be filtered out.

    Skipping it would take it out of the live set the body is subtracted from, and the guard
    would report a clean bill for a row the PUT retracts with no authority behind it. A key
    is mandatory in every RESTCONF list entry, so its absence is corrupt data, and the write
    fails closed on it.
    """
    from nso_adapter.core.removal import _document_orphans

    live = {"interface": {"interface": [{"interface-name": "Gi0/1", "ipv4-address": [{"address": "198.18.0.1"}]}]}}
    body = {"interface": {"interface": [{"interface-name": "Gi0/1", "ipv4-address": []}]}}
    assert _document_orphans(live, body, {}) == {"interface_config/ipv4-address": [["Gi0/1", "198.18.0.1"]]}

    keyless = {"interface": {"interface": [{"ipv4-address": [{"address": "198.18.0.1"}]}]}}
    with pytest.raises(KeyError):
        _document_orphans(keyless, body, {})
