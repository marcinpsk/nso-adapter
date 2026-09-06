# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The execution-context contract's five scenarios (#1663, #1522 memo A9).

One invariant, driven end to end: **a generation document's execution metadata matches
exactly the authorized fragments it composes.** Every case runs against the real API app
and the real store, executes through the REAL worker at the recorded RESTCONF boundary,
and asserts both what the document says and that it hydrates completely.

The assertions here are about the DOCUMENT and its hydration. The bodies the aggregate
sender derives from that document are C9's second slice; the per-section context reaches
the encoders with it.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from tests.api.test_static_route_identity import entry as route_entry
from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_action_apply_promotion import (
    _A,
    _apply,
    _generations,
    _put_routes,
    _put_vlans,
    _settle,
    _stream,
)
from tests.core.test_generation_protocol import recorded_client, run_head, seed_settings

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
_CISCO = "cisco-ios-cli-3.8"
_NOKIA = "timos-nc-23.10"


async def _set_ned(device_id: int, ned_id: str | None) -> None:
    from nso_adapter.store.models import Device

    async with session() as db:
        device = await db.get(Device, device_id)
        device.ned_id = ned_id
        await db.commit()


async def _seed_interface(device_id: int, name: str) -> int:
    from nso_adapter.store.models import DbInterface

    async with session() as db:
        iface = DbInterface(device_id=device_id, name=name)
        db.add(iface)
        await db.flush()
        iface_id = iface.id
        await db.commit()
        return iface_id


async def _put_attrs(client, device_id: int, attributes: list[dict], *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/intent{query}",
        json={"attributes": attributes},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_addresses(client, device_id: int, addresses: list[dict], *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/ip-intent{query}",
        json={"addresses": addresses},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_route_policy(client, device_id: int, members: list[str], *, seq: int, name: str = "RP-COMM"):
    return await client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        json={
            "objects": [
                {
                    "family": "community_list",
                    "name": name,
                    "entries": [
                        {"sequence": index, "action": "permit", "community": member}
                        for index, member in enumerate(members, start=1)
                    ],
                    "accepted": True,
                    "invert_match": False,
                }
            ]
        },
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _force_removal(client, device_id: int, scope: str):
    return await client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal",
        json={"scope": scope},
        headers=AUTH,
    )


async def _set_attribute_state(device_id: int, attribute: str, state) -> None:
    from nso_adapter.store.models import DbInterface, InterfaceAttrState

    async with session() as db:
        row = await db.scalar(
            sa.select(InterfaceAttrState)
            .join(DbInterface, DbInterface.id == InterfaceAttrState.interface_id)
            .where(DbInterface.device_id == device_id, InterfaceAttrState.attribute == attribute)
        )
        row.sync_state = state
        await db.commit()


async def _drain(device_id: int) -> None:
    """Terminalize whatever a runner queued behind it, so the next Apply is not refused."""
    from nso_adapter.core.generation import LIVE_JOB_STATUSES
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        for job in (
            (await db.execute(sa.select(Job).where(Job.device_id == device_id, Job.status.in_(LIVE_JOB_STATUSES))))
            .scalars()
            .all()
        ):
            job.status = JobStatus.succeeded
        await db.commit()


def _execution(generation, section: str) -> dict:
    return generation.document[section]["_execution"]


# ── Scenario 1 — a reissue re-asserts what it froze ──────────────────────────


async def test_scenario_1_a_reissue_carries_the_context_and_proof_its_authorization_froze(adapter_client):
    """Live eligibility, live rows and the device NED all move; the reissue moves with none.

    The untouched sections of a promotion-free reissue are the fragments their own
    authorizations froze, byte for byte, and they hydrate.
    """
    from nso_adapter.core.projection import hydrate_interface_execution
    from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan
    from nso_adapter.store.models import GenerationStatus, SyncState

    device_id = await seed_device(nso_device_name="ec-reissue", netbox_device_id=17001)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _CISCO)
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "core link"}],
            seq=1701,
        )
    ).status_code == 200
    assert (
        await _put_routes(adapter_client, device_id, [route_entry(_A, route_id=1, generation=1)], seq=1702)
    ).status_code == 200
    assert (await _put_vlans(adapter_client, device_id, [101], seq=1703)).status_code == 200
    assert (
        await _apply(adapter_client, device_id, {"interface_config": 1701, "static_route": 1702, "vlan": 1703})
    ).status_code == 202
    client, _ = recorded_client("ec-reissue")
    first_job = await run_head(device_id, client)
    assert first_job is not None
    await _settle(first_job, GenerationStatus.settled)
    baseline = (await _generations(device_id))[-1]
    frozen_interface = baseline.document["interface_config"]
    frozen_routes = baseline.document["static_route"]

    # Everything the old code re-derived at execution moves under the reissue's feet.
    await _set_attribute_state(device_id, "description", SyncState.error)
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "store only"}],
            seq=1704,
            query="?store_only=true",
        )
    ).status_code == 200
    assert (
        await _put_routes(
            adapter_client,
            device_id,
            [route_entry(_A, route_id=1, generation=2, metric=99)],
            seq=1705,
            query="?store_only=true",
        )
    ).status_code == 200
    await _set_ned(device_id, _NOKIA)

    assert (await _force_removal(adapter_client, device_id, "vlan")).status_code == 202

    reissue = (await _generations(device_id))[-1]
    assert reissue.id != baseline.id
    assert reissue.stream_revisions == {}
    assert reissue.document["interface_config"] == frozen_interface
    assert reissue.document["static_route"] == frozen_routes
    assert _execution(reissue, "interface_config")["context"] == {"ned_id": _CISCO, "dialect": "identity"}
    assert _execution(reissue, "static_route")["context"] == {"ned_id": _CISCO, "dialect": "identity"}
    eligibility = _execution(reissue, "interface_config")["proof"]["attribute_eligibility"]
    assert set(eligibility.values()) == {True}, "eligibility was re-resolved from now-ineligible live state"
    assert hydrate_interface_execution(reissue.document).eligible_attributes
    assert hydrate_static_route_apply_plan(reissue.document).mode in {"PATCH", "PUT"}
    for stream, revision in (("interface_config", 1), ("static_route", 1)):
        row = await _stream(device_id, stream)
        assert (row.authorized_revision, row.applied_revision) == (revision, revision)

    assert await run_head(device_id, recorded_client("ec-reissue")[0]) is not None


# ── Scenario 2 — a consumed carrier leaves no authority behind ───────────────


async def test_scenario_2_a_settled_carrier_is_pruned_from_the_fragment_and_every_later_document(adapter_client):
    """Real removal settlement consumes T; the next creation prunes it under the lock.

    The stored fragment is rewritten, so fragment and document agree literally rather than
    by exemption, and the already-immutable generation that named T keeps its bytes.
    """
    from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan, hydrate_static_route_removal_plan
    from tests.core.removal_helpers import seed_removal_job, seed_tomb
    from tests.core.test_static_route_put import A, B, seed_rows, wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client, tombstone_ids

    device_id = await seed_device(nso_device_name="ec-carrier", netbox_device_id=17002)
    await seed_settings(device_id, auto_apply=False)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await seed_tomb(device_id, A, route_id=1)
    job_id = await seed_removal_job(device_id, {}, tombs=(tomb,))

    fragment = (await _stream(device_id, "static_route")).authorized_document
    assert fragment["_execution"]["proof"]["apply"]["tombstone_ids"] == [tomb]
    removal = (await _generations(device_id))[-1]
    frozen_document, frozen_digest = removal.document, removal.digest
    assert hydrate_static_route_removal_plan(frozen_document).tombstone_ids == (tomb,)

    job = await run_removal_job(device_id, job_id, sr_client(SrFake("ec-carrier", service=[wire(A), wire(B)])))
    assert job.status.value == "succeeded"
    assert await tombstone_ids(device_id) == [], "real settlement did not consume the carrier"
    await _drain(device_id)

    # An unrelated authorization is the next document creation, so it prunes.
    assert (await _put_vlans(adapter_client, device_id, [202], seq=1710)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 1710})).status_code == 202

    pruned = (await _stream(device_id, "static_route")).authorized_document
    assert pruned["_execution"]["proof"]["apply"]["tombstone_ids"] == []
    assert pruned["static_route_tombstone"] == []
    later = (await _generations(device_id))[-1]
    assert hydrate_static_route_apply_plan(later.document).tombstone_ids == []

    stored = (await _generations(device_id))[0]
    assert (stored.document, stored.digest) == (frozen_document, frozen_digest), "an immutable document was rewritten"


async def test_scenario_2_an_operation_selecting_a_consumed_carrier_refuses_creation(adapter_client):
    """An INHERITED carrier that is gone was settled; an explicitly SELECTED one is a lie."""
    from nso_adapter.core.generation import CarrierGone, refresh_consumed_carriers
    from tests.core.removal_helpers import authorize_static_route
    from tests.core.test_static_route_put import B, seed_rows

    device_id = await seed_device(nso_device_name="ec-selected-carrier", netbox_device_id=17003)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await authorize_static_route(device_id)

    async with session() as db:
        with pytest.raises(CarrierGone, match="no longer exist"):
            await refresh_consumed_carriers(db, device_id, selected=(4242,))


# ── Scenario 3 — a section keeps its own context across a NED change ─────────


async def test_scenario_3_a_reissue_encodes_route_policy_under_the_context_it_was_authorized_with(adapter_client):
    """A device NED change reaches a section only when an operation reauthorizes it."""
    from nso_adapter.core.projection import section_context
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="ec-dialect", netbox_device_id=17004)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _NOKIA)
    assert (await _put_route_policy(adapter_client, device_id, ["large:64512:1:2"], seq=1720)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"route_policy": 1720})).status_code == 202
    first_job = await run_head(device_id, recorded_client("ec-dialect")[0])
    assert first_job is not None
    await _settle(first_job, GenerationStatus.settled)

    # A LATER section is authorized under a different context, so the document legally
    # carries two.
    await _set_ned(device_id, _CISCO)
    assert (await _put_vlans(adapter_client, device_id, [303], seq=1721)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 1721})).status_code == 202
    mixed = (await _generations(device_id))[-1]
    assert section_context(mixed.document, "route_policy") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    assert section_context(mixed.document, "vlan") == {"ned_id": _CISCO, "dialect": "identity"}

    second_job = await run_head(device_id, recorded_client("ec-dialect")[0])
    assert second_job is not None
    await _settle(second_job, GenerationStatus.settled)

    assert (await _force_removal(adapter_client, device_id, "vlan")).status_code == 202
    reissue = (await _generations(device_id))[-1]
    assert section_context(reissue.document, "route_policy") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    assert reissue.document["route_policy"] == mixed.document["route_policy"]


# ── Scenario 4 — H1: one push that adds, deletes and detaches ────────────────


async def test_scenario_4_the_networked_intermediate_keeps_the_retained_rows_own_decision(adapter_client):
    """The retained row's eligibility comes from the fragment it was retained FROM.

    Re-resolving it from live state would drop the description the intermediate document
    still has to carry, and only the detach final may retire it.
    """
    from nso_adapter.core.projection import hydrate_interface_execution
    from nso_adapter.store.models import GenerationMode, GenerationStatus, SyncState

    device_id = await seed_device(
        nso_device_name="ec-h1", netbox_device_id=17005, attributes=["description", "enabled"]
    )
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _CISCO)
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "core link"}],
            seq=1730,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1730})).status_code == 202
    first_job = await run_head(device_id, recorded_client("ec-h1")[0])
    assert first_job is not None
    await _settle(first_job, GenerationStatus.settled)

    # One push drops the description and adds an enabled row; the description's live state
    # then falls out of the eligible set.
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "enabled", "intent_value": True}],
            seq=1731,
        )
    ).status_code == 200
    await _set_attribute_state(device_id, "description", SyncState.error)
    assert (await _apply(adapter_client, device_id, {"interface_config": 1731})).status_code == 202

    generations = (await _generations(device_id))[1:]
    assert [generation.mode for generation in generations] == [GenerationMode.networked, GenerationMode.detach]
    assert len({generation.settlement_cohort for generation in generations}) == 1
    intermediate, final = generations
    intermediate_rows = intermediate.document["interface_config"]["interface_intent"]
    assert sorted(row["attribute"] for row in intermediate_rows) == ["description", "enabled"]
    eligibility = _execution(intermediate, "interface_config")["proof"]["attribute_eligibility"]
    description = next(row for row in intermediate_rows if row["attribute"] == "description")
    assert eligibility[f"{description['interface_id']}/description"] is True
    assert str(description["interface_id"]) in _execution(intermediate, "interface_config")["proof"]["interfaces"]
    assert [row["attribute"] for row in final.document["interface_config"]["interface_intent"]] == ["enabled"]
    assert (
        f"{description['interface_id']}/description"
        not in (_execution(final, "interface_config")["proof"]["attribute_eligibility"])
    )
    for generation in generations:
        assert hydrate_interface_execution(generation.document) is not None
    assert intermediate.digest != final.digest


# ── Scenario 5 — the sibling streams of a split section ──────────────────────


async def test_scenario_5a_an_ip_only_apply_keeps_the_owner_streams_context_and_decisions(adapter_client):
    """Case A. Only ``ip`` is reauthorized, so nothing about the attribute lane moves."""
    from nso_adapter.core.projection import hydrate_interface_execution, section_context
    from nso_adapter.store.models import GenerationStatus, SyncState

    device_id = await seed_device(nso_device_name="ec-split", netbox_device_id=17006)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _CISCO)
    eth0 = await _seed_interface(device_id, "GigabitEthernet0/1")
    eth1 = await _seed_interface(device_id, "GigabitEthernet0/2")
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "authorized"}],
            seq=1740,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1740})).status_code == 202
    first_job = await run_head(device_id, recorded_client("ec-split")[0])
    assert first_job is not None
    await _settle(first_job, GenerationStatus.settled)

    await _set_attribute_state(device_id, "description", SyncState.error)
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "store only"}],
            seq=1741,
            query="?store_only=true",
        )
    ).status_code == 200
    await _set_ned(device_id, _NOKIA)
    assert (
        await _put_addresses(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/2", "address": "192.0.2.1/24", "family": "ipv4"}],
            seq=1742,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1742})).status_code == 202

    generation = (await _generations(device_id))[-1]
    section = generation.document["interface_config"]
    (attribute,) = section["interface_intent"]
    assert attribute["intent_value"] == "authorized", "an unselected store-only value entered the document"
    assert section_context(generation.document, "interface_config") == {"ned_id": _CISCO, "dialect": "identity"}
    proof = section["_execution"]["proof"]
    assert proof["attribute_eligibility"] == {f"{eth0}/description": True}
    assert set(proof["interfaces"]) == {str(eth0), str(eth1)}
    assert hydrate_interface_execution(generation.document).eligible_attributes == frozenset({(eth0, "description")})
    assert (await _stream(device_id, "interface_config")).authorized_revision == 1

    # A following interface_config authorization moves the section's context.
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "moved"}],
            seq=1743,
        )
    ).status_code == 200
    await _settle((await _generations(device_id))[-1].job_id, GenerationStatus.settled)
    assert (await _apply(adapter_client, device_id, {"interface_config": 1743})).status_code == 202
    moved = (await _generations(device_id))[-1]
    assert section_context(moved.document, "interface_config") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}


async def test_scenario_5c_a_section_with_no_owner_fragment_takes_its_only_contributors_context(adapter_client):
    """Case C. ``ip`` first: the single contributing fragment supplies the section context."""
    from nso_adapter.core.projection import section_context

    device_id = await seed_device(nso_device_name="ec-split-owner-absent", netbox_device_id=17007)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _NOKIA)
    await _seed_interface(device_id, "GigabitEthernet0/1")
    assert (
        await _put_addresses(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "address": "192.0.2.5/24", "family": "ipv4"}],
            seq=1750,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1750})).status_code == 202

    generation = (await _generations(device_id))[-1]
    assert section_context(generation.document, "interface_config") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    assert "attribute_eligibility" not in generation.document["interface_config"]["_execution"]["proof"]


def test_scenario_5d_composition_refuses_a_fragment_whose_context_is_not_a_context():
    """Case D. A missing, mis-keyed or unregistered context is refused, naming the section."""
    from nso_adapter.core.generation import _compose_document

    good = {"context": {"ned_id": _CISCO, "dialect": "identity"}}
    assert _compose_document({"vlan": {"_execution": good}})["vlan"]["_execution"] == good
    with pytest.raises(ValueError, match="unfrozen fragment"):
        _compose_document({"vlan": {"vlan_intent": []}})
    with pytest.raises(ValueError, match="invalid execution context"):
        _compose_document({"vlan": {"_execution": {"context": {"ned_id": _CISCO}}}})
    with pytest.raises(ValueError, match="unregistered dialect"):
        _compose_document({"vlan": {"_execution": {"context": {"ned_id": _CISCO, "dialect": "junos"}}}})
    with pytest.raises(ValueError, match="operation plane"):
        _compose_document({"vlan": {"_execution": {**good, "operation": {}}}})


def test_scenario_5e_interface_records_merge_field_wise_and_refuse_a_conflict():
    """Case E. Owner-wins is right for the context and wrong for a row.

    The IP endpoint backfills ``parent_binding`` and ``encap_tag`` onto an interface an
    attribute fragment recorded while both were null; owner-wins would restore the nulls and
    the encoded address would lose its binding.
    """
    from nso_adapter.core.generation import _compose_document

    context = {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    bare = {
        "id": 7,
        "name": "1/1/1",
        "kind": None,
        "parent_binding": None,
        "encap_tag": None,
        "vrf": None,
        "service": None,
    }
    bound = {**bare, "parent_binding": "lag-99", "encap_tag": "99"}
    document = _compose_document(
        {
            "interface_config": {
                "interface_intent": [],
                "_execution": {"context": context, "proof": {"interfaces": {"7": bare}, "attribute_eligibility": {}}},
            },
            "ip": {
                "interface_ip_intent": [],
                "_execution": {"context": context, "proof": {"interfaces": {"7": bound}}},
            },
        }
    )
    assert document["interface_config"]["_execution"]["proof"]["interfaces"]["7"] == bound

    with pytest.raises(ValueError, match="conflicting 'parent_binding' values"):
        _compose_document(
            {
                "interface_config": {
                    "_execution": {
                        "context": context,
                        "proof": {"interfaces": {"7": {**bare, "parent_binding": "lag-1"}}},
                    },
                },
                "ip": {
                    "_execution": {"context": context, "proof": {"interfaces": {"7": bound}}},
                },
            }
        )


# ── the hydration half: every stored fact is checked BEFORE any device I/O ────


def test_hydration_refuses_a_section_that_never_recorded_a_context():
    """A pre-contract section must fail before the worker touches NSO, not at the encode site.

    The migration stamped the rows that existed when the contract landed. Anything that
    slipped past it still hydrates a valid proof, and hydration runs first, so the check
    belongs here as well as at the encoder.
    """
    from nso_adapter.core.projection import hydrate_interface_execution
    from nso_adapter.core.static_route_plan import (
        hydrate_static_route_apply_plan,
        hydrate_static_route_removal_plan,
    )

    interface = {
        "interface_config": {
            "interface_intent": [],
            "interface_ip_intent": [],
            "_execution": {"proof": {"interfaces": {}, "attribute_eligibility": {}}},
        }
    }
    with pytest.raises(ValueError, match="invalid execution context"):
        hydrate_interface_execution(interface)

    route = {
        "static_route": {
            "static_route_intent": [],
            "static_route_tombstone": [],
            "_execution": {
                "proof": {
                    "apply": {
                        "mode": "PATCH",
                        "row_ids": [],
                        "allowed_removal_keys": [],
                        "tombstone_ids": [],
                        "cas": [],
                        "tombstone_id_watermark": 0,
                    }
                },
                "operation": {
                    "removal": {
                        "authorized_removal_keys": [],
                        "claimed_keys": [],
                        "tombstone_ids": [],
                        "candidate_clears": [],
                        "reclaimed_keys": [],
                    }
                },
            },
        }
    }
    with pytest.raises(ValueError, match="invalid execution context"):
        hydrate_static_route_apply_plan(route)
    with pytest.raises(ValueError, match="invalid execution context"):
        hydrate_static_route_removal_plan(route)


def test_hydration_refuses_a_recorded_clear_the_documents_own_rows_do_not_describe():
    """A clear is discharged against the row it names, so it must name a row this document has.

    Checking the plane's outer keys only let a recorded clear name any row, any key and any
    field: settlement would retire an obligation over a leaf the document never authorized
    anyone to touch.
    """
    from nso_adapter.core.static_route_plan import hydrate_static_route_removal_plan

    row = {
        "id": 11,
        "vrf": "",
        "prefix": "198.18.0.0/24",
        "next_hop": "198.18.1.1",
        "metric": None,
        "pending_clear": {"authorized": ["metric"]},
    }

    def _document(clear):
        return {
            "static_route": {
                "static_route_intent": [row],
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {
                        "removal": {
                            "authorized_removal_keys": [],
                            "claimed_keys": [],
                            "tombstone_ids": [],
                            "candidate_clears": [clear],
                            "reclaimed_keys": [],
                        }
                    },
                },
            }
        }

    good = {"row_id": 11, "key": ["", "198.18.0.0/24", "198.18.1.1"], "fields": ["metric"]}
    assert hydrate_static_route_removal_plan(_document(good)).clears[0].row_id == 11

    with pytest.raises(ValueError, match="does not carry"):
        hydrate_static_route_removal_plan(_document({**good, "row_id": 12}))
    with pytest.raises(ValueError, match="not the"):
        hydrate_static_route_removal_plan(_document({**good, "key": ["", "198.18.9.0/24", "198.18.1.1"]}))
    with pytest.raises(ValueError, match="wire-unset"):
        hydrate_static_route_removal_plan(_document({**good, "fields": ["tag"]}))


def test_a_retained_interface_record_merges_with_the_desired_one():
    """The retained row keeps the binding its own authorization froze.

    An importer refresh can null ``parent_binding`` and ``encap_tag`` on the interface the
    desired fragment recorded. Skipping the source record whenever the desired proof already
    names the interface encoded the retained address without its binding.
    """
    from nso_adapter.core.projection import retained_proof

    bare = {
        "id": 7,
        "name": "1/1/1",
        "kind": None,
        "parent_binding": None,
        "encap_tag": None,
        "vrf": None,
        "service": None,
    }
    bound = {**bare, "parent_binding": "lag-99", "encap_tag": "99"}
    desired = {"interfaces": {"7": bare}, "attribute_eligibility": {"7/description": True}}
    source = {"interfaces": {"7": bound}, "attribute_eligibility": {"7/description": True}}
    retained = {"interface_intent": [{"interface_id": 7, "attribute": "description"}]}

    merged = retained_proof("interface_config", desired, source, retained)
    assert merged["interfaces"]["7"] == bound

    conflicting = {"interfaces": {"7": {**bare, "parent_binding": "lag-1"}}, "attribute_eligibility": {}}
    with pytest.raises(ValueError, match="conflicting 'parent_binding' values"):
        retained_proof("interface_config", conflicting, source, retained)
