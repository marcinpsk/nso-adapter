# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The execution-context contract's five scenarios (#1663, #1522 memo A9).

One invariant, driven end to end: **a generation document's execution metadata matches
exactly the authorized fragments it composes.** Every scenario runs against the real API app
and the real store, EXECUTES through the REAL worker, and asserts the device-intent document
the sender transmitted as well as what the stored document says and that it hydrates.

Executing is the point. A case that only reads the stored document stays green when the
sender ignores the frozen metadata and re-derives everything from live state, which is the
class of bug this contract exists to prevent.
"""

from __future__ import annotations

from copy import deepcopy

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


async def _execute(device_id: int, name: str, **kwargs):
    """Run this device's queued head through the real worker → its recorder.

    Every assertion about what reached NSO reads this recorder, so a scenario cannot pass by
    agreeing with the stored document while the sender transmitted something else.
    """
    from nso_adapter.store.models import Job, JobStatus

    client, recorder = recorded_client(name, **kwargs)
    job_id = await run_head(device_id, client)
    assert job_id is not None, "no job was queued for the generation under test"
    async with session() as db:
        job = await db.get(Job, job_id)
    assert job.status is JobStatus.succeeded, f"the worker failed the job under test: {job.error}"
    return recorder


async def _next_queued_type(device_id: int):
    """The job type at the head of this device's queue, or ``None`` when it is empty."""
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        return await db.scalar(
            sa.select(Job.job_type)
            .where(Job.device_id == device_id, Job.status == JobStatus.queued)
            .order_by(Job.id)
            .limit(1)
        )


async def _drain_writes(device_id: int, name: str, *, limit: int = 6) -> list[dict]:
    """Execute every queued job in order → the device-intent writes they transmitted.

    Each entry is ``{"instance": …, "no_networking": bool}``. A chain's links are asserted by
    what each one SENT, never by their queue order, which the planner owns.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    writes: list[dict] = []
    for _ in range(limit):
        if await _next_queued_type(device_id) not in (JobType.apply, JobType.removal):
            break  # the follow-up sync a detach queues is not part of the chain under test
        client, recorder = recorded_client(name)
        job_id = await run_head(device_id, client)
        if job_id is None:
            break
        async with session() as db:
            job = await db.get(Job, job_id)
        assert job.status is JobStatus.succeeded, f"the worker failed job {job_id}: {job.error}"
        for call in recorder.commits:
            instance = recorder._instance(call["body"])
            if instance is not None:
                writes.append({"instance": instance, "no_networking": "no-networking" in call["url"]})
    return writes


# ── Scenario 1 — a reissue re-asserts what it froze ──────────────────────────


async def test_scenario_1_a_reissue_carries_the_context_and_proof_its_authorization_froze(adapter_client):
    """Live eligibility, live rows and the device NED all move; the reissue moves with none.

    The untouched sections of a promotion-free reissue are the fragments their own
    authorizations froze, byte for byte, and they hydrate.
    """
    from nso_adapter.core.projection import hydrate_interface_execution
    from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan
    from nso_adapter.store.models import SyncState

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
    first = await _execute(device_id, "ec-reissue")
    baseline = (await _generations(device_id))[-1]
    frozen_interface = baseline.document["interface_config"]
    frozen_routes = baseline.document["static_route"]
    sent_first = first.documents[-1]

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

    # The point of the scenario: what the reissue TRANSMITS for the untouched families is the
    # bytes the first deployment sent, not what the live store and the live NED now say.
    second = await _execute(device_id, "ec-reissue")
    sent_second = second.documents[-1]
    assert sent_second["interface"] == sent_first["interface"]
    assert sent_second["static-route"] == sent_first["static-route"]
    assert sent_second["interface"]["interface"][0]["description"] == "core link"


# ── Scenario 2 — a consumed carrier leaves no authority behind ───────────────


@pytest.mark.parametrize("name_only", [False, True], ids=["no-authorization", "name-only"])
async def test_scenario_2_a_settled_carrier_is_pruned_from_the_fragment_and_every_later_document(
    adapter_client, name_only
):
    """Real removal settlement consumes T; the next creation prunes it under the lock.

    Driven through the intent API and the REAL worker, like every other scenario: a directly
    invoked runner proves the bookkeeping without proving that the admission which wrote the
    carrier and the worker which discharges it agree about it.

    The stored fragment is rewritten, so fragment and document agree literally rather than by
    exemption, and the already-immutable generation that named T keeps its bytes.
    """
    from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan, hydrate_static_route_removal_plan
    from tests.core.static_route_harness import K, S, retention_harness, route
    from tests.core.test_static_route_removal import tombstone_ids

    harness = await retention_harness(adapter_client)
    device_id = harness.device_id
    await _set_ned(device_id, _CISCO)
    await harness.push([route(K, route_id=1), route(S, route_id=2)])
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    await harness.drain()

    await harness.push([route(S, route_id=2)], removed=((1, K),))
    (tomb,) = await tombstone_ids(device_id)
    fragment = (await _stream(device_id, "static_route")).authorized_document
    assert fragment["_execution"]["proof"]["apply"]["tombstone_ids"] == [tomb]
    removal = (await _generations(device_id))[-1]
    frozen_document, frozen_digest = removal.document, removal.digest
    assert hydrate_static_route_removal_plan(frozen_document).tombstone_ids == (tomb,)

    await harness.run()
    assert await tombstone_ids(device_id) == [], "real settlement did not consume the carrier"
    assert harness.fake.sent_keys() == {S}, "the removal transmitted the key it was retracting"
    await harness.drain()

    if name_only:
        from tests.core.test_action_apply_promotion import _jobs

        before_generations = [g.id for g in await _generations(device_id)]
        before_jobs = [job.id for job in await _jobs(device_id)]
        previous = await _stream(device_id, "static_route")
        revision = previous.desired_revision + 1
        await harness.push([{**route(S, route_id=2), "name": "renamed survivor"}], store_only=True)
        staged = await _stream(device_id, "static_route")
        assert (staged.desired_revision, staged.authorized_revision, staged.applied_revision) == (
            revision,
            previous.authorized_revision,
            previous.applied_revision,
        )
        response = await _apply(adapter_client, device_id, {"static_route": harness.seq})
        assert response.status_code == 200, response.text
        assert response.json() == {
            "device_id": device_id,
            "outcome": "no_op",
            "selected": {"static_route": harness.seq},
            "skipped": {"static_route": "already_applied"},
            "skipped_detail": None,
            "generations": [],
        }
        assert [g.id for g in await _generations(device_id)] == before_generations
        assert [job.id for job in await _jobs(device_id)] == before_jobs
        promoted = await _stream(device_id, "static_route")
        assert (promoted.desired_revision, promoted.authorized_revision, promoted.applied_revision) == (
            revision,
            revision,
            revision,
        )
        assert promoted.authorized_document["_execution"]["proof"]["apply"]["tombstone_ids"] == []

    later = await harness.unrelated()
    pruned = (await _stream(device_id, "static_route")).authorized_document
    assert pruned["_execution"]["proof"]["apply"]["tombstone_ids"] == []
    assert pruned["static_route_tombstone"] == []
    plan = hydrate_static_route_apply_plan(later.document)
    assert plan.tombstone_ids == []
    assert K not in plan.allowed
    assert K not in {tuple(key) for key in pruned["_execution"]["proof"]["apply"]["allowed_removal_keys"]}

    stored = next(g for g in await _generations(device_id) if g.id == removal.id)
    assert (stored.document, stored.digest) == (frozen_document, frozen_digest), "an immutable document was rewritten"

    # EXECUTE the later document: a pruned carrier claims nothing, so the key it used to
    # claim is neither rendered nor retained.
    await harness.run()
    assert harness.fake.sent_keys() == {S}
    await harness.drain()

    assert (await _force_removal(adapter_client, device_id, "static_route")).status_code == 202
    reissue = (await _generations(device_id))[-1]
    assert reissue.stream_revisions == {}
    apply_plan = hydrate_static_route_apply_plan(reissue.document)
    removal_plan = hydrate_static_route_removal_plan(reissue.document)
    assert apply_plan.tombstone_ids == []
    assert K not in apply_plan.allowed
    assert removal_plan.tombstone_ids == ()
    assert K not in removal_plan.authorized
    assert reissue.document["static_route"]["static_route_tombstone"] == []
    await harness.run()
    assert harness.fake.sent_keys() == {S}
    assert all(entry["prefix"] != K[1] for entry in harness.fake.sent_routes())
    stored = next(g for g in await _generations(device_id) if g.id == removal.id)
    assert (stored.document, stored.digest) == (frozen_document, frozen_digest)


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

    device_id = await seed_device(nso_device_name="ec-dialect", netbox_device_id=17004)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _NOKIA)
    assert (await _put_route_policy(adapter_client, device_id, ["large:64512:1:2"], seq=1720)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"route_policy": 1720})).status_code == 202
    first = await _execute(device_id, "ec-dialect")
    # The frozen dialect is what spells the member: SR OS keeps an exact large community as
    # three keyword-less colon parts, and the canonical `large:` prefix would be rejected.
    assert first.container("route-policy")["community-list"][0]["entry"][0]["community"] == "64512:1:2"

    # A LATER section is authorized under a different context, so the document legally
    # carries two.
    await _set_ned(device_id, _CISCO)
    assert (await _put_vlans(adapter_client, device_id, [303], seq=1721)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 1721})).status_code == 202
    mixed = (await _generations(device_id))[-1]
    assert section_context(mixed.document, "route_policy") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    assert section_context(mixed.document, "vlan") == {"ned_id": _CISCO, "dialect": "identity"}
    second = await _execute(device_id, "ec-dialect")
    assert second.container("route-policy")["community-list"][0]["entry"][0]["community"] == "64512:1:2"

    assert (await _force_removal(adapter_client, device_id, "vlan")).status_code == 202
    reissue = (await _generations(device_id))[-1]
    assert section_context(reissue.document, "route_policy") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    assert reissue.document["route_policy"] == mixed.document["route_policy"]

    # EXECUTED with the device row now cisco: the members are still the Nokia spelling, so
    # the encode read the section's frozen dialect and not the live device.
    flush = await _execute(device_id, "ec-dialect")
    assert flush.container("route-policy")["community-list"][0]["entry"][0]["community"] == "64512:1:2"


# ── Scenario 4 — H1: one push that adds, deletes and detaches ────────────────


async def test_scenario_4_the_networked_intermediate_keeps_the_retained_rows_own_decision(adapter_client):
    """The retained row's eligibility comes from the fragment it was retained FROM.

    Re-resolving it from live state would drop the description the intermediate document
    still has to carry, and only the detach final may retire it.
    """
    from nso_adapter.core.projection import hydrate_interface_execution
    from nso_adapter.store.models import GenerationMode, SyncState

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
    await _execute(device_id, "ec-h1")

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

    # EXECUTED, every link of the chain. The networked writes still carry the description
    # they retained; only the no-networking write drops it, because nothing authorized
    # retracting it from the device.
    writes = await _drain_writes(device_id, "ec-h1")
    networked = [w for w in writes if not w["no_networking"]]
    detached = [w for w in writes if w["no_networking"]]
    assert networked and detached, f"expected a networked link and a detach link, got {writes}"
    for write in networked:
        entry = write["instance"]["interface"]["interface"][0]
        assert entry["description"] == "core link", "a networked link dropped the row it retained"
        assert entry["enabled"] is True
    for write in detached:
        entry = write["instance"]["interface"]["interface"][0]
        assert "description" not in entry, "the detach still asserted the description"
        assert entry["enabled"] is True


# ── Scenario 5 — the sibling streams of a split section ──────────────────────


async def test_scenario_5a_an_ip_only_apply_keeps_the_owner_streams_context_and_decisions(adapter_client):
    """Case A. Only ``ip`` is reauthorized, so nothing about the attribute lane moves."""
    from nso_adapter.core.projection import hydrate_interface_execution, section_context
    from nso_adapter.store.models import SyncState

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
    await _execute(device_id, "ec-split")

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

    # EXECUTED: the transmitted interface container carries the attribute half's AUTHORIZED
    # contribution beside the address the ip lane just authorized, and not the store-only
    # value the live row now holds.
    ip_only = await _execute(device_id, "ec-split")
    sent = {entry["interface-name"]: entry for entry in ip_only.container("interface")["interface"]}
    assert sent["GigabitEthernet0/1"]["description"] == "authorized"
    assert sent["GigabitEthernet0/2"]["ipv4-address"][0]["address"] == "192.0.2.1"
    # The section kept the cisco context through a NED change, so no Nokia routed leaf rides
    # either entry: the encode read the frozen record, never the live device row.
    assert not any("kind" in entry for entry in sent.values())

    # A following interface_config authorization moves the section's context.
    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": "moved"}],
            seq=1743,
        )
    ).status_code == 200
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

    # EXECUTED: a section whose owner stream never contributed still encodes and reaches the
    # wire, under the only context it has.
    recorder = await _execute(device_id, "ec-split-owner-absent")
    (entry,) = recorder.container("interface")["interface"]
    assert entry["interface-name"] == "GigabitEthernet0/1"
    assert entry["ipv4-address"] == [{"address": "192.0.2.5", "prefix-length": 24, "secondary": False}]
    assert "description" not in entry, "the absent owner contributed no attribute"


async def _revisions(device_id: int) -> set:
    """Every projection stream's authorized/applied revision, as one comparable snapshot."""
    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        rows = (
            await db.execute(
                sa.select(
                    DeviceProjectionStream.stream,
                    DeviceProjectionStream.authorized_revision,
                    DeviceProjectionStream.applied_revision,
                ).where(DeviceProjectionStream.device_id == device_id)
            )
        ).all()
    return set(rows)


async def test_scenario_5d_a_refused_composition_rolls_the_whole_transaction_back(adapter_client):
    """A composition refusal rolls back the Apply reservation inserted before composition."""
    from uuid import uuid4

    from nso_adapter.store.models import DeploymentApplyAttempt, DeviceProjectionStream
    from tests.core.test_action_apply_promotion import _put_snmp

    device_id = await seed_device(nso_device_name="ec-refusal-rollback", netbox_device_id=17009)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_snmp(adapter_client, device_id, ["ops"], seq=1779)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"snmp": 1779})).status_code == 202
    await _execute(device_id, "ec-refusal-rollback")
    assert (await _put_vlans(adapter_client, device_id, [401], seq=1780)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 1780})).status_code == 202
    await _execute(device_id, "ec-refusal-rollback")

    # An UNSELECTED stored fragment whose dialect names nothing registered: the composition
    # takes every authorized fragment, so the next Apply of another stream has to refuse.
    async with session() as db:
        row = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "snmp"
            )
        )
        document = deepcopy(row.authorized_document)
        document["_execution"]["context"]["dialect"] = "no-such-dialect"
        row.authorized_document = document
        await db.commit()

    before = await _generations(device_id)
    revisions = await _revisions(device_id)
    assert (await _put_vlans(adapter_client, device_id, [401, 402], seq=1781)).status_code == 200
    attempt_id = uuid4()
    response = await _apply(adapter_client, device_id, {"vlan": 1781}, attempt_id=attempt_id)
    assert response.status_code == 500, response.text

    async with session() as db:
        assert await db.get(DeploymentApplyAttempt, attempt_id) is None, "a refused Apply committed its reservation"
    assert [g.id for g in await _generations(device_id)] == [g.id for g in before], "a refused Apply left a generation"
    assert await _revisions(device_id) == revisions, "a refused Apply promoted a stream"


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


async def test_scenario_5e_a_backfilled_binding_survives_composition_and_reaches_the_wire(adapter_client):
    """Case E. Owner-wins is right for the context and wrong for a row.

    Driven by real rows, because that is where the loss happens: the ip endpoint backfills
    ``parent_binding`` and ``encap_tag`` onto an interface the attribute fragment recorded
    while both were null. Owner-wins would restore the nulls, and SR OS would be told to
    configure the address on an interface with no port binding.
    """
    from nso_adapter.store.models import DbInterface

    device_id = await seed_device(nso_device_name="ec-split-merge", netbox_device_id=17008)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _NOKIA)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="1/1/1:100", kind="logical")
        db.add(iface)
        await db.commit()

    assert (
        await _put_attrs(
            adapter_client,
            device_id,
            [{"interface": "1/1/1:100", "attribute": "description", "intent_value": "uplink"}],
            seq=1760,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1760})).status_code == 202
    await _execute(device_id, "ec-split-merge")
    frozen = (await _stream(device_id, "interface_config")).authorized_document
    (recorded,) = frozen["_execution"]["proof"]["interfaces"].values()
    assert recorded["parent_binding"] is None, "setup broken: the attribute lane must freeze the bare record"

    # The ip push backfills the binding, so the ip fragment records what the attribute
    # fragment could not have known.
    assert (
        await _put_addresses(
            adapter_client,
            device_id,
            [
                {
                    "interface": "1/1/1:100",
                    "address": "192.0.2.9/24",
                    "family": "ipv4",
                    "routed": True,
                    "parent_binding": "lag-99",
                    "encap_tag": "99",
                }
            ],
            seq=1761,
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1761})).status_code == 202

    generation = (await _generations(device_id))[-1]
    (merged,) = generation.document["interface_config"]["_execution"]["proof"]["interfaces"].values()
    assert (merged["parent_binding"], merged["encap_tag"]) == ("lag-99", "99")

    recorder = await _execute(device_id, "ec-split-merge")
    (entry,) = recorder.container("interface")["interface"]
    assert entry["description"] == "uplink"
    assert entry["parent-binding"] == "lag-99"
    assert entry["encap-tag"] == "99"


def test_scenario_5e_composition_refuses_two_different_non_null_interface_values():
    """The refusal half, which no single store can produce: two fragments, two bindings.

    The ip endpoint never clobbers a populated binding, so a conflict can only come from a
    document assembled out of fragments that disagree, and that must refuse rather than pick.
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


# ── the other split section: IS-IS ownership, and the one consumption choke point ──


async def test_the_isis_section_takes_its_owner_streams_context_and_keeps_both_lanes_rows(adapter_client):
    """The second split section, whose owner is ``isis`` and whose sibling is the flex-algo lane.

    Owner-wins is a property of the SECTION, so reauthorizing the owner is the one way to
    move it to a new NED. The sibling's ROWS are not owner-wins: they are the other lane's
    authorization and must still reach the wire.
    """
    from nso_adapter.core.projection import section_context

    device_id = await seed_device(nso_device_name="ec-isis-owner", netbox_device_id=17009)
    await seed_settings(device_id, auto_apply=False)
    await _set_ned(device_id, _NOKIA)
    assert (
        await adapter_client.put(
            f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
            json={"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
            headers=AUTH | {"X-Push-Seq": "1770"},
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"isis_flex_algo": 1770})).status_code == 202
    sibling_only = (await _generations(device_id))[-1]
    assert section_context(sibling_only.document, "isis") == {"ned_id": _NOKIA, "dialect": "nokia_timos"}
    await _execute(device_id, "ec-isis-owner")

    # The OWNER lane is authorized next, under a different device NED.
    await _set_ned(device_id, _CISCO)
    assert (
        await adapter_client.put(
            f"/api/v1/devices/{device_id}/isis-interface-intent",
            json={"interfaces": [{"interface_name": "GigabitEthernet0/1", "af": "ipv4", "process_tag": "1"}]},
            headers=AUTH | {"X-Push-Seq": "1771"},
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"isis": 1771})).status_code == 202

    composed = (await _generations(device_id))[-1]
    assert section_context(composed.document, "isis") == {"ned_id": _CISCO, "dialect": "identity"}, (
        "the composed section took the sibling lane's context instead of its owner's"
    )
    assert composed.document["isis"]["isis_flex_algo_intent"], "the sibling lane's rows left the document"

    recorder = await _execute(device_id, "ec-isis-owner")
    isis = recorder.container("isis")
    assert [entry["interface-name"] for entry in isis["interface-config"]] == ["GigabitEthernet0/1"]
    (process,) = isis["process-config"]
    assert [algo["algo-id"] for algo in process["flex-algo"]] == [128], "the sibling lane's rows left the wire"


def test_every_consumption_path_deletes_a_carrier_through_the_one_locking_choke_point():
    """One choke point, so the projection lock cannot be forgotten on a new consumption path.

    ``delete_tombstones`` takes the claim, then the projection lock, then the carrier rows.
    A second site issuing its own DELETE would consume a carrier a document creation holding
    that lock is in the middle of composing, which is the interleaving the order forbids.
    """
    import ast
    import pathlib as _pathlib

    root = _pathlib.Path(__file__).resolve().parents[2] / "nso_adapter"
    offenders: set[str] = set()
    for path in root.rglob("*.py"):
        if path.name == "tombstone_store.py":
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and (
                isinstance(node.func, ast.Name)
                and node.func.id == "delete"
                or isinstance(node.func, ast.Attribute)
                and node.func.attr == "delete"
            ):
                if any(isinstance(arg, ast.Name) and arg.id == "StaticRouteTombstone" for arg in node.args):
                    offenders.add(str(path.relative_to(root)))
    assert offenders == set(), f"carrier deletions outside the choke point: {sorted(offenders)}"

    source = ast.parse((root / "store" / "tombstone_store.py").read_text())
    body = next(
        node for node in ast.walk(source) if isinstance(node, ast.AsyncFunctionDef) and node.name == "delete_tombstones"
    )
    awaited = [
        node.func.id
        for node in ast.walk(body)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"lock_claim", "lock_projection"}
    ]
    assert awaited[:2] == ["lock_claim", "lock_projection"], f"the lock order is not claim then projection: {awaited}"


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


def test_hydration_refuses_two_eligibility_decisions_that_decode_to_one_attribute():
    """``"7/description"`` and ``"07/description"`` are two keys and one attribute.

    Comparing the DECODED set let the second silently overwrite the first: the sets matched,
    hydration passed, and the attribute went out ineligible with nothing to say so.
    """
    from nso_adapter.core.projection import hydrate_interface_execution

    iface = {
        "id": 7,
        "name": "Gi0/0",
        "kind": None,
        "parent_binding": None,
        "encap_tag": None,
        "vrf": None,
        "service": None,
    }
    document = {
        "interface_config": {
            "interface_intent": [{"id": 1, "interface_id": 7, "attribute": "description", "intent_value": "up"}],
            "_execution": {
                "context": {"ned_id": None, "dialect": "identity"},
                "proof": {
                    "interfaces": {"7": iface},
                    "attribute_eligibility": {"7/description": True, "07/description": False},
                },
            },
        }
    }
    with pytest.raises(ValueError, match="does not match its attribute rows"):
        hydrate_interface_execution(document)


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


@pytest.mark.parametrize("context", [None, {"ned_id": None, "dialect": "missing-dialect"}])
async def test_worker_validates_vlan_context_before_any_nso_io(adapter_client, context):
    import json
    from copy import deepcopy

    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import DeploymentGeneration
    from tests.core.test_generation_protocol import job_row

    device_id = await seed_device(nso_device_name="invalid-vlan-context", netbox_device_id=17318)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    async with session() as db:
        generation = await db.scalar(sa.select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id))
        document = deepcopy(generation.document)
        if context is None:
            document["vlan"]["_execution"].pop("context")
        else:
            document["vlan"]["_execution"]["context"] = context
        digest = digest_document(generation.mode, document, generation.allowed_removal_keys or {})
        await db.execute(sa.text("ALTER TABLE deployment_generation DISABLE TRIGGER deployment_generation_immutable"))
        await db.execute(
            sa.text("UPDATE deployment_generation SET document = CAST(:doc AS json), digest = :digest WHERE id = :gid"),
            {"doc": json.dumps(document), "digest": digest, "gid": generation.id},
        )
        await db.execute(sa.text("ALTER TABLE deployment_generation ENABLE TRIGGER deployment_generation_immutable"))
        await db.commit()
    client, rec = recorded_client("invalid-vlan-context")
    from unittest.mock import patch

    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert response.status_code == 200
    assert "preview unavailable" in response.json()["diffs"]["device_intent"]
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "failed"
    assert client.mock_calls == [], "context validation must precede every NSO call"
    assert rec.calls == []


def test_every_section_hydrator_refuses_missing_context():
    from nso_adapter.core.projection import hydrate_section, section_registry

    for section in section_registry():
        with pytest.raises(ValueError, match=section):
            hydrate_section({section: {}}, section)


async def test_scenario_2_a_creation_waits_for_a_consumption_and_names_no_consumed_carrier(
    adapter_client, rival_engine
):
    """The carrier lock with TWO real sessions: the creation waits, then names nothing gone.

    A document composed between another session's consumption and its commit would freeze a
    carrier id that is about to disappear, and the worker would then retain a key no carrier
    claims. The consuming session holds the device's projection lock across its whole pass,
    so the Apply blocks on it and composes only after the consumption has committed.
    """
    import asyncio
    from unittest.mock import patch

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from nso_adapter.core.static_route_reclaim import reclaim_one_device
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import StaticRouteTombstone
    from tests.core.removal_helpers import authorize_static_route, seed_tomb
    from tests.core.static_route_harness import K, S, retention_harness, route
    from tests.core.test_action_apply_promotion import _put_vlans
    from tests.core.test_projection_lock_order import _backend_pid, _wait_for_blocked_query
    from tests.core.test_static_route_reclaim import seed_succeeded_owner
    from tests.core.test_static_route_removal import sr_client, tombstone_ids

    harness = await retention_harness(adapter_client)
    device_id = harness.device_id
    await harness.push([route(S, route_id=2)])
    await harness.run()
    await harness.drain()
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, K, job_id=owner, route_id=1)
    await authorize_static_route(device_id)

    # The push is accepted first, so the Apply below is the one step that composes.
    harness.seq += 1
    assert (
        await _put_vlans(adapter_client, device_id, [909], seq=harness.seq, query="?store_only=true")
    ).status_code == 200

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as gate, rival() as consumer:
        gate_pid = await _backend_pid(gate)
        await gate.execute(sa.select(StaticRouteTombstone.id).where(StaticRouteTombstone.id == tomb).with_for_update())
        with patch("nso_adapter.core.importer.get_nso_client", return_value=sr_client(harness.fake)):
            consuming = asyncio.create_task(reclaim_one_device(device_id, db=consumer))
            creating = None
            try:
                consumer_pid = await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=gate_pid,
                    relation="static_route_tombstone",
                    fragments=("from static_route_tombstone", "for update"),
                )
                creating = asyncio.create_task(_apply(adapter_client, device_id, {"vlan": harness.seq}))
                await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=consumer_pid,
                    relation="devices",
                    fragments=("from devices", "for no key update"),
                )
                await gate.rollback()
                assert await asyncio.wait_for(consuming, timeout=10) == (1, 0)
                response = await asyncio.wait_for(creating, timeout=10)
            finally:
                await gate.rollback()
                for task in (consuming, creating):
                    if task is not None and not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    assert response.status_code == 202, response.text
    assert await tombstone_ids(device_id) == [], "the consumption did not commit"
    created = (await _generations(device_id))[-1]
    assert created.document["static_route"]["static_route_tombstone"] == [], "a document named a consumed carrier"
    assert created.document["static_route"]["_execution"]["proof"]["apply"]["tombstone_ids"] == []

    await harness.run()
    assert harness.fake.sent_keys() == {S}, "the worker retained a key no carrier claims"
