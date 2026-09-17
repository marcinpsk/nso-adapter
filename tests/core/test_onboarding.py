# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/onboarding.py — onboard_device, rekey_device, offboard_device, set_scope.

These tests exercise the DB-layer logic directly via the store's session context manager,
bypassing the HTTP layer.  The `adapter_client` fixture is still required to
ensure init_db() has run (creating schema) before any DB call.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select

from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, ManagedScope
from tests._secret_discipline import assert_text_free_of
from tests.conftest import session

# ── onboard_device ───────────────────────────────────────────────────────────


async def test_onboard_creates_device(adapter_client_with_nso):
    """onboard_device inserts a Device row and returns it with mapping_status=mapped."""
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.models import MappingStatus
    from tests._secret_discipline import assert_records_free_of

    async with session() as db:
        with capture_logs() as logs:
            device = await onboard_device(db, "nso-dev", "core-rtr-01", 42)
        assert device.id is not None
        assert device.nso_instance == "nso-dev"
        assert device.nso_device_name == "core-rtr-01"
        assert device.netbox_device_id == 42
        assert device.mapping_status == MappingStatus.mapped
    record = next(record for record in logs if record["event"] == "device.onboarded")
    assert record["device_id"] == device.id
    assert "nso_device" not in record
    assert_records_free_of([record], ["core-rtr-01"])


async def test_onboard_raises_for_unknown_instance(adapter_client):
    """onboard_device raises ValueError when NSO instance is not in config."""
    from nso_adapter.core.onboarding import onboard_device
    from tests._secret_discipline import assert_chain_free_of

    unknown_instance = "placeholder-unknown-onboard-instance"
    async with session() as db:
        with pytest.raises(ValueError, match="not found in config") as caught:
            await onboard_device(db, unknown_instance, "device-01", 99)

    assert_chain_free_of(caught.value, [unknown_instance])


async def test_claim_timeout_does_not_repeat_the_nso_device_name(adapter_client_with_nso, monkeypatch):
    from structlog.testing import capture_logs

    from nso_adapter.config import get_config
    from nso_adapter.core.claim import ClaimRegistration, ClaimUnavailableError, acquire_claim, release_claim
    from nso_adapter.core.onboarding import onboard_device
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of
    from tests.conftest import seed_device

    device_name = "placeholder-claimed-device"
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name=device_name, netbox_device_id=None)
    rival = await acquire_claim(device_id, "intent_put")
    assert rival is not None
    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.0)

    try:
        async with session() as db:
            with capture_logs() as logs, pytest.raises(ClaimUnavailableError) as caught:
                await onboard_device(db, "nso-dev", device_name, 99, reg=ClaimRegistration())
    finally:
        await release_claim(rival)

    assert_chain_free_of(caught.value, [device_name])
    record = next(record for record in logs if record["event"] == "device.mapping_claim_timeout")
    assert record["device_id"] == device_id
    assert "nso_device" not in record
    assert_records_free_of([record], [device_name])


async def test_onboard_raises_for_duplicate_netbox_id(adapter_client_with_nso):
    """onboard_device raises LookupError when netbox_device_id is already onboarded."""
    from nso_adapter.core.onboarding import onboard_device
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="existing-device", netbox_device_id=100)

    async with session() as db:
        with pytest.raises(LookupError, match="already onboarded"):
            await onboard_device(db, "nso-dev", "new-device", 100)


async def test_onboard_raises_for_duplicate_nso_device_name(adapter_client_with_nso):
    """onboard_device raises LookupError when (nso_instance, nso_device_name) is already onboarded
    to a DIFFERENT NetBox device — that is a genuine conflict; don't steal it."""
    from nso_adapter.core.onboarding import onboard_device
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="taken-name", netbox_device_id=200)

    async with session() as db:
        with pytest.raises(LookupError, match="already onboarded"):
            await onboard_device(db, "nso-dev", "taken-name", 201)


async def test_claimed_onboard_refusal_names_no_netbox_link(adapter_client_with_nso):
    """The provision path refuses under the claim, and that refusal reached the job result.

    Its message named the NetBox device the row is linked to, which the request never sent
    and the job record then persisted. The refusal states the reason; the link is logged.
    """
    from structlog.testing import capture_logs

    from nso_adapter.core.claim import ClaimRegistration
    from nso_adapter.core.onboarding import DeviceIdentityRefused, onboard_device
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="placeholder-claimed-node", netbox_device_id=46431)

    async with session() as db:
        with capture_logs() as logs, pytest.raises(DeviceIdentityRefused) as caught:
            await onboard_device(
                db,
                "nso-dev",
                "placeholder-claimed-node",
                46432,
                reg=ClaimRegistration(run_attempt=1),
            )

    assert_text_free_of(caught.value, ["46431"])  # first: the assertions below render the message
    assert str(caught.value) == "The NSO device is already onboarded to a different NetBox device"
    assert caught.value.reason == "onboarded_elsewhere"
    refused = [record for record in logs if record["event"] == "device.onboard_refused"]
    assert refused and refused[0]["linked_netbox_device_id"] == 46431


async def test_onboard_adopts_unlinked_existing_device(adapter_client_with_nso):
    """A device provisioned INTO NSO without a NetBox link (netbox_device_id IS NULL) must be
    ADOPTED when the operator later marks it managed: onboard_device fills the mapping in on the
    SAME row instead of raising a spurious 'already onboarded'. Regression — an unlinked leftover
    row silently blocked linking (409 -> plugin swallowed it), so the plugin's adapter_device_id
    stayed None and the device never onboarded (live: netbox device 23 / lab01c-ri6 vs the
    June-provisioned adapter device 343, netbox_device_id NULL)."""
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.models import MappingStatus
    from tests._secret_discipline import assert_records_free_of
    from tests.conftest import seed_device

    existing_id = await seed_device(nso_instance="nso-dev", nso_device_name="preprovisioned", netbox_device_id=None)

    async with session() as db:
        with capture_logs() as logs:
            device = await onboard_device(db, "nso-dev", "preprovisioned", 77)
        assert device.id == existing_id  # adopted the SAME row — not a second device
        assert device.netbox_device_id == 77
        assert device.mapping_status == MappingStatus.mapped

    record = next(record for record in logs if record["event"] == "device.adopted")
    assert record["device_id"] == existing_id
    assert record["netbox_device_id"] == 77
    assert "nso_device" not in record
    assert_records_free_of([record], ["preprovisioned"])

    # Exactly one row for that NSO node — adoption must not create a duplicate.
    async with session() as db:
        rows = (await db.execute(select(Device).where(Device.nso_device_name == "preprovisioned"))).scalars().all()
        assert len(rows) == 1
        assert rows[0].netbox_device_id == 77


async def test_onboard_existing_adoption_reports_a_late_netbox_conflict(adapter_client_with_nso):
    """A competing owner committed after the pre-check produces the public conflict."""
    from nso_adapter.core.onboarding import onboard_device
    from tests.conftest import seed_device

    existing_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="existing-adoption-winner",
        netbox_device_id=None,
    )
    owner_id = None
    async with session() as db:
        original_commit = db.commit

        async def commit_after_the_target_is_claimed():
            nonlocal owner_id
            owner_id = await seed_device(
                nso_instance="nso-dev",
                nso_device_name="existing-adoption-owner",
                netbox_device_id=78,
            )
            db.commit = original_commit
            await original_commit()

        db.commit = commit_after_the_target_is_claimed
        with pytest.raises(LookupError, match="NetBox device 78 is already onboarded") as caught:
            await onboard_device(db, "nso-dev", "existing-adoption-winner", 78)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert owner_id is not None
    async with session() as db:
        existing = await db.get(Device, existing_id)
        owner = await db.get(Device, owner_id)
        assert existing is not None and existing.netbox_device_id is None
        assert owner is not None and owner.netbox_device_id == 78


async def test_onboard_is_idempotent_for_same_link(adapter_client_with_nso):
    """Re-onboarding the same (instance, name) already linked to the SAME netbox_device_id returns
    the existing row (idempotent no-op), not a 409 — so a re-fired manage signal is safe."""
    from nso_adapter.core.onboarding import onboard_device
    from tests.conftest import seed_device

    existing_id = await seed_device(nso_instance="nso-dev", nso_device_name="already-linked", netbox_device_id=55)

    async with session() as db:
        device = await onboard_device(db, "nso-dev", "already-linked", 55)
        assert device.id == existing_id
        assert device.netbox_device_id == 55


async def test_onboard_resolves_a_lost_insert_race(adapter_client_with_nso):
    """A concurrent onboard that wins the insert is adopted, not duplicated.

    onboard_device checks identity with select-then-insert, so two callers can both find
    nothing and both insert. The duplicate would be permanent — the scope reconcile keys
    ownership by netbox_device_id and keeps every row it finds — so the DB constraints decide
    and the loser re-reads the winner. Simulated by committing the competing row between this
    caller's lookup and its own INSERT, which is exactly what the lost race looks like.

    Driven off ``flush``, not ``commit``: the insert is flushed there so the settle counter
    can name the device id (Appendix S §3.3). Injecting at commit would let this caller's
    INSERT land first, and the rival would then block on OUR uncommitted key instead of
    racing us to it.
    """
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import onboard_device
    from tests._secret_discipline import assert_records_free_of
    from tests.conftest import seed_device

    submitted_name = "placeholder-caller-raced-device"
    async with session() as db:
        original_flush = db.flush
        winner_id = {}

        async def flush_after_a_competing_insert():
            if not winner_id:
                winner_id["id"] = await seed_device(
                    nso_instance="nso-dev", nso_device_name=submitted_name, netbox_device_id=91
                )
            db.flush = original_flush
            await original_flush()

        db.flush = flush_after_a_competing_insert
        with capture_logs() as logs:
            device = await onboard_device(db, "nso-dev", submitted_name, 91)
        assert device.id == winner_id["id"]  # the winner's row, not a second one
    assert_records_free_of(logs, [submitted_name])

    async with session() as db:
        rows = (await db.execute(select(Device).where(Device.nso_device_name == submitted_name))).scalars().all()
        assert len(rows) == 1  # exactly one survivor


async def test_onboard_adopts_an_unlinked_lost_insert_winner(adapter_client_with_nso):
    """The losing insert must complete the requested link on an unlinked winner."""
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.models import MappingStatus
    from tests._secret_discipline import assert_records_free_of
    from tests.conftest import seed_device

    submitted_name = "placeholder-caller-unlinked-device"
    async with session() as db:
        original_flush = db.flush
        winner_id = {}

        async def flush_after_an_unlinked_competing_insert():
            if not winner_id:
                winner_id["id"] = await seed_device(
                    nso_instance="nso-dev",
                    nso_device_name=submitted_name,
                    netbox_device_id=None,
                )
            db.flush = original_flush
            await original_flush()

        db.flush = flush_after_an_unlinked_competing_insert
        with capture_logs() as logs:
            device = await onboard_device(db, "nso-dev", submitted_name, 190)

        assert device.id == winner_id["id"]
        assert device.netbox_device_id == 190
        assert device.mapping_status is MappingStatus.mapped
    assert_records_free_of(logs, [submitted_name])

    async with session() as db:
        rows = (await db.execute(select(Device).where(Device.nso_device_name == submitted_name))).scalars().all()
        assert len(rows) == 1
        assert rows[0].netbox_device_id == 190


async def test_onboard_reports_a_late_netbox_conflict_after_losing_the_insert(adapter_client_with_nso):
    """A target claimed after the adoption pre-check must retain the public conflict contract."""
    from nso_adapter.core.onboarding import onboard_device
    from tests.conftest import seed_device

    async with session() as db:
        original_flush = db.flush
        original_commit = db.commit
        winner_id = {}
        owner_id = {}

        async def flush_after_an_unlinked_competing_insert():
            if not winner_id:
                winner_id["id"] = await seed_device(
                    nso_instance="nso-dev",
                    nso_device_name="late-conflict-winner",
                    netbox_device_id=None,
                )
            db.flush = original_flush
            await original_flush()

        async def commit_after_the_target_is_claimed():
            if not owner_id:
                owner_id["id"] = await seed_device(
                    nso_instance="nso-dev",
                    nso_device_name="late-conflict-owner",
                    netbox_device_id=193,
                )
            db.commit = original_commit
            await original_commit()

        db.flush = flush_after_an_unlinked_competing_insert
        db.commit = commit_after_the_target_is_claimed
        with pytest.raises(LookupError, match="NetBox device 193 is already onboarded") as caught:
            await onboard_device(db, "nso-dev", "late-conflict-winner", 193)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None

    async with session() as db:
        winner = await db.get(Device, winner_id["id"])
        owner = await db.get(Device, owner_id["id"])
        assert winner is not None and winner.netbox_device_id is None
        assert owner is not None and owner.netbox_device_id == 193


async def test_onboard_same_identity_race_reports_identity_refusal(adapter_client_with_nso):
    """The losing request must report that the NSO identity belongs to another link."""
    from nso_adapter.core.onboarding import DeviceIdentityRefused, onboard_device
    from tests.conftest import seed_device

    async with session() as db:
        original_flush = db.flush
        winner_id = {}

        async def flush_after_a_competing_insert():
            if not winner_id:
                winner_id["id"] = await seed_device(
                    nso_instance="nso-dev",
                    nso_device_name="raced-identity",
                    netbox_device_id=192,
                )
            db.flush = original_flush
            await original_flush()

        db.flush = flush_after_a_competing_insert
        with pytest.raises(DeviceIdentityRefused) as caught:
            await onboard_device(db, "nso-dev", "raced-identity", 191)

    assert caught.value.reason == "onboarded_elsewhere"
    assert str(caught.value) == "The NSO device is already onboarded to a different NetBox device"


async def test_duplicate_nso_identity_is_rejected_by_the_database(adapter_client_with_nso):
    """The (nso_instance, nso_device_name) uniqueness is enforced in the DB, not just in code.

    onboard_device's own guard can be bypassed by a race; the constraint cannot.
    """
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import MappingStatus
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="dup-guard", netbox_device_id=93)

    with pytest.raises(IntegrityError):
        async with session() as db:
            db.add(
                Device(
                    nso_instance="nso-dev",
                    nso_device_name="dup-guard",
                    netbox_device_id=94,
                    mapping_status=MappingStatus.mapped,
                )
            )
            await db.commit()


async def test_duplicate_netbox_device_id_is_rejected_by_the_database(adapter_client_with_nso):
    """netbox_device_id is unique where non-null — two NSO nodes cannot claim one NetBox device."""
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import MappingStatus
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="nb-dup-a", netbox_device_id=95)

    with pytest.raises(IntegrityError):
        async with session() as db:
            db.add(
                Device(
                    nso_instance="nso-dev",
                    nso_device_name="nb-dup-b",
                    netbox_device_id=95,
                    mapping_status=MappingStatus.mapped,
                )
            )
            await db.commit()


async def test_unlinked_devices_may_coexist(adapter_client_with_nso):
    """The netbox_device_id index is PARTIAL: several unlinked leftovers are legitimate.

    A device provisioned into NSO without a NetBox link carries netbox_device_id NULL, and a
    plain unique index would have made the second one an error.
    """
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="unlinked-a", netbox_device_id=None)
    await seed_device(nso_instance="nso-dev", nso_device_name="unlinked-b", netbox_device_id=None)

    async with session() as db:
        rows = (await db.execute(select(Device).where(Device.netbox_device_id.is_(None)))).scalars().all()
        assert len(rows) >= 2


# ── rekey_device ─────────────────────────────────────────────────────────────


async def test_rekey_changes_device_name(adapter_client_with_nso):
    """rekey_device updates nso_device_name and resets sync metadata."""
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import rekey_device
    from tests._secret_discipline import assert_records_free_of
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="old-name", netbox_device_id=300)

    async with session() as db:
        device = await db.get(Device, device_id)
        device.ned_id = "old-ned"
        device.sw_version = "old-version"
        device.degraded_surfaces = ["ospf"]
        await db.commit()
        with capture_logs() as logs:
            updated = await rekey_device(db, device, nso_device_name="new-name")
        assert updated.nso_device_name == "new-name"
        assert updated.ned_id is None
        assert updated.sw_version is None
        assert updated.last_sync_at is None
        assert updated.degraded_surfaces is None
        assert updated.source_epoch == 2
    record = next(record for record in logs if record["event"] == "device.rekeyed")
    assert record["device_id"] == device_id
    assert "nso_device" not in record
    assert_records_free_of([record], ["new-name"])


async def test_rekey_reports_identity_refusal_when_the_target_is_claimed_after_the_precheck(
    adapter_client_with_nso,
):
    """The family fences lock (device_id, family); they do not serialize two devices on one identity.

    The pre-check and the commit are select-then-write, so a rival can claim the target pair in
    between and `uq_device_nso_identity` is what actually decides. Simulated by committing the
    competing row right after this caller's pre-check, which is exactly what losing looks like.
    """
    from nso_adapter.core.onboarding import DeviceIdentityRefused, rekey_device
    from tests._secret_discipline import assert_chain_free_of
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-loser", netbox_device_id=301)

    async with session() as db:
        device = await db.get(Device, device_id)
        original_execute = db.execute
        state = {"saw_precheck": False, "seeded": False}

        async def execute_racing_the_precheck(statement, *args, **kwargs):
            # Seed on the first statement AFTER the dup pre-check, so the pre-check misses the
            # rival and our own identity UPDATE is the one the constraint refuses.
            if state["saw_precheck"] and not state["seeded"]:
                state["seeded"] = True
                await seed_device(nso_instance="nso-dev", nso_device_name="rekey-winner", netbox_device_id=302)
            if "devices.id !=" in str(statement):
                state["saw_precheck"] = True
            return await original_execute(statement, *args, **kwargs)

        db.execute = execute_racing_the_precheck
        with pytest.raises(DeviceIdentityRefused) as caught:
            await rekey_device(db, device, nso_device_name="rekey-winner")

    assert state["seeded"], "the race was never injected; the test proves nothing"
    assert caught.value.reason == "identity_claimed"
    # The IntegrityError names the colliding key, so the refusal is raised OUTSIDE the handler:
    # chaining it would republish the device name through every sink that renders the chain.
    assert_chain_free_of(caught.value, ["rekey-winner", "uq_device_nso_identity"])


async def test_rekey_reraises_an_integrity_error_from_another_constraint(adapter_client_with_nso):
    """The try covers the teardown deletes too, so only the identity constraint means a lost race.

    A non-identity violation reported as `identity_claimed` would answer 409 and hide a real
    fault. The identity path itself is proven against the live constraint by the race test above.
    """
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.core.onboarding import rekey_device
    from tests.conftest import seed_device

    class _OtherConstraint(Exception):
        constraint_name = "uq_some_other_constraint"

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-other", netbox_device_id=303)

    async with session() as db:
        device = await db.get(Device, device_id)
        original_commit = db.commit

        async def commit_violating_another_constraint():
            db.commit = original_commit
            raise IntegrityError("UPDATE devices", {}, _OtherConstraint()) from _OtherConstraint()

        db.commit = commit_violating_another_constraint
        with pytest.raises(IntegrityError):
            await rekey_device(db, device, nso_device_name="rekey-other-target")


async def test_rekey_same_source_is_true_noop(adapter_client_with_nso):
    """An idempotent source PATCH preserves the generation and read publications."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.nso.read_outcome import Present
    from nso_adapter.store import outcome_store
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="same-source", netbox_device_id=306)
    async with session() as db:
        attempt = await outcome_store.record_read_outcome(db, device_id, "bfd", Present([]), refresh_source="poll")
        await outcome_store.record_result(
            db, attempt, result="replaced", succeeded=True, row_count=0, publish_payload=True
        )
        db.add(DbInterface(device_id=device_id, name="GE0/0"))
        await db.commit()
        device = await db.get(Device, device_id)
        updated = await rekey_device(
            db, device, nso_instance=device.nso_instance, nso_device_name=device.nso_device_name
        )
        assert updated.source_epoch == 1
        assert (await outcome_store.get_current_outcome(db, device_id, "bfd")).id == attempt
        assert await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id)) is not None


async def test_rekey_invalidates_all_read_publications(adapter_client_with_nso):
    """A real source change clears routing mirrors and every family pointer atomically."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.nso.read_outcome import Present
    from nso_adapter.store import outcome_store
    from nso_adapter.store.models import DeviceStaticRoute
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="old-source", netbox_device_id=307)
    async with session() as db:
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="198.18.20.0/24",
                next_hop="198.18.0.2",
                refresh_source="poll",
            )
        )
        attempt = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present([]), refresh_source="poll"
        )
        await outcome_store.record_result(
            db, attempt, result="replaced", succeeded=True, row_count=1, publish_payload=True
        )
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device, nso_device_name="new-source")
        assert updated.source_epoch == 2
        assert await db.scalar(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id)) is None
        assert await outcome_store.get_current_outcome(db, device_id, "static_route") is None


async def test_rekey_clears_interface_state(adapter_client_with_nso):
    """rekey_device deletes all interfaces and attr states for the device."""
    from nso_adapter.core.onboarding import rekey_device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="with-ifaces", netbox_device_id=301)

    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GE0/0")
        db.add(iface)
        await db.flush()
        state = InterfaceAttrState(interface_id=iface.id, attribute="description")
        db.add(state)
        await db.commit()

    async with session() as db:
        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="renamed")
        # interfaces should be gone
        result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
        assert result.scalars().all() == []


async def test_rekey_preserves_interface_intent_and_its_identity_anchor(adapter_client_with_nso):
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.models import InterfaceIntent
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-source", netbox_device_id=308)
    async with session() as db:
        iface = DbInterface(
            device_id=device_id,
            name="GE0/0",
            parent_binding="old-parent",
            kind="physical",
        )
        db.add(iface)
        await db.flush()
        db.add(InterfaceAttrState(interface_id=iface.id, attribute="description", nso_value="old"))
        db.add(
            InterfaceIntent(
                interface_id=iface.id,
                attribute="description",
                intent_value="operator-owned",
            )
        )
        await db.commit()

        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="replacement-source")

        kept_iface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id))
        assert kept_iface is not None
        assert kept_iface.name == "GE0/0"
        assert kept_iface.parent_binding is None
        assert kept_iface.kind is None
        intent = await db.scalar(select(InterfaceIntent).where(InterfaceIntent.interface_id == kept_iface.id))
        assert intent is not None
        assert intent.intent_value == "operator-owned"
        assert (
            await db.scalar(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == kept_iface.id)) is None
        )


async def test_rekey_preserves_ip_only_intent_and_its_identity_anchor(adapter_client_with_nso):
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.models import InterfaceIpIntent
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="ip-intent-source", netbox_device_id=310)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GE0/1")
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="198.18.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
            )
        )
        await db.commit()

        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="replacement-ip-source")

        kept_iface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id))
        assert kept_iface is not None
        intent = await db.scalar(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == kept_iface.id))
        assert intent is not None
        assert intent.address == "198.18.0.1/24"


async def test_old_source_sync_metadata_cannot_overwrite_rekey_reset(adapter_client_with_nso):
    from nso_adapter.core.importer import _publish_sync_metadata
    from nso_adapter.store.models import LastSyncStatus
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="metadata-source", netbox_device_id=309)
    async with session() as db:
        device = await db.get(Device, device_id)
        device.source_epoch = 2
        await db.commit()

        published = await _publish_sync_metadata(
            db,
            device_id,
            source_epoch=1,
            status=LastSyncStatus.succeeded,
            degraded_surfaces=None,
        )

        assert published is False
        await db.refresh(device)
        assert device.last_sync_at is None
        assert device.last_sync_status is None


async def test_rekey_raises_for_unknown_instance(adapter_client):
    """rekey_device raises ValueError when new NSO instance is not in config."""
    from nso_adapter.core.onboarding import rekey_device
    from tests._secret_discipline import assert_chain_free_of
    from tests.conftest import seed_device

    unknown_instance = "placeholder-unknown-rekey-instance"
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-inst", netbox_device_id=302)

    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(ValueError, match="not found in config") as caught:
            await rekey_device(db, device, nso_instance=unknown_instance)

    assert_chain_free_of(caught.value, [unknown_instance])


async def test_rekey_changes_nso_instance(adapter_client_with_nso):
    """rekey_device updates nso_instance when a valid new instance is provided."""
    from nso_adapter.core.onboarding import rekey_device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-inst-change", netbox_device_id=305)

    async with session() as db:
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device, nso_instance="nso-dev")  # same instance, valid
        assert updated.nso_instance == "nso-dev"

    """rekey_device with no fields provided returns the device unchanged."""
    from nso_adapter.core.onboarding import rekey_device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="unchanged", netbox_device_id=303)

    async with session() as db:
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device)
        assert updated.nso_device_name == "unchanged"


# ── offboard_device ──────────────────────────────────────────────────────────


async def test_offboard_removes_device(adapter_client_with_nso):
    """offboard_device deletes the device row from the DB."""
    from nso_adapter.core.onboarding import offboard_device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="to-offboard", netbox_device_id=400)

    async with session() as db:
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        # Confirm gone
        gone = await db.get(Device, device_id)
        assert gone is None


async def test_offboard_cascades_an_apply_stamped_generation(adapter_client_with_nso):
    from sqlalchemy import text

    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt, complete_apply_attempt
    from nso_adapter.store.models import DeploymentApplyAttempt, DeploymentGeneration, GenerationMode
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="offboard-apply-evidence",
        netbox_device_id=403,
    )
    attempt_id = uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {"vlan": 1}) is None
        await note_write(db, device_id, "vlan", push_seq=1)
        generation = await create_generation(
            db,
            device_id,
            streams=("vlan",),
            mode=GenerationMode.networked,
            document={},
            apply_attempt_id=attempt_id,
        )
        await complete_apply_attempt(
            db,
            attempt_id,
            admission_state="admitted",
            http_status=202,
            response={"generations": [{"generation_id": generation.id}]},
        )
        await db.commit()
        constraint = (
            await db.execute(
                text(
                    "SELECT confdeltype, condeferrable, condeferred "
                    "FROM pg_constraint "
                    "WHERE conname = 'fk_generation_apply_attempt'"
                )
            )
        ).one()

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))

    async with session() as db:
        assert await db.get(Device, device_id) is None
        assert await db.get(DeploymentApplyAttempt, attempt_id) is None
        assert (
            await db.scalar(select(DeploymentGeneration).where(DeploymentGeneration.apply_attempt_id == attempt_id))
            is None
        )
    assert constraint == (b"a", True, True)


async def test_offboard_cascades_interfaces_and_scope(adapter_client_with_nso):
    """offboard_device removes interfaces, attr states, and managed scope."""
    from nso_adapter.core.onboarding import offboard_device
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="offboard-with-data",
        netbox_device_id=401,
        attributes=["description"],
    )

    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GE0/0")
        db.add(iface)
        await db.flush()
        db.add(InterfaceAttrState(interface_id=iface.id, attribute="description"))
        await db.commit()

    async with session() as db:
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        # All related rows should be gone
        ifaces = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
        assert ifaces.scalars().all() == []
        scope = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
        assert scope.scalars().all() == []


async def test_offboard_cascades_mirror_rows_the_keeprows_change_relies_on(adapter_client_with_nso):
    """READSEM S5 (1327): device-absence now KEEPS mirror rows, so a device removed from NSO is
    cleaned up ONLY by offboard. Prove offboard removes the mirror for the families a bare 404 no
    longer clears — a pop family (static_route) + device_settings — so keep-rows can't strand
    immortal rows on a deleted device."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import DeviceSettings, DeviceStaticRoute
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="offboard-mirror",
        netbox_device_id=402,
        attributes=["description"],
    )

    async with session() as db:
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.9.0.0/16", next_hop="1.1.1.1"))
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    async with session() as db:
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        routes = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))
        assert routes.scalars().all() == [], "static_route rows orphaned after offboard"
        settings = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
        assert settings.scalars().all() == [], "device_settings row orphaned after offboard"
        scope = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
        assert scope.scalars().all() == []
        assert await db.get(Device, device_id) is None


# ── set_scope ────────────────────────────────────────────────────────────────


async def test_set_scope_adds_attributes(adapter_client_with_nso):
    """set_scope creates ManagedScope rows for each requested attribute."""
    from nso_adapter.core.onboarding import set_scope
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev", nso_device_name="scope-add", netbox_device_id=500, attributes=[]
    )

    async with session() as db:
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, ["description", "enabled"])
        attrs = {s.attribute for s in result}
        assert attrs == {"description", "enabled"}


async def test_set_scope_removes_old_attributes(adapter_client_with_nso):
    """set_scope removes rows that are no longer in the desired list."""
    from nso_adapter.core.onboarding import set_scope
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-remove",
        netbox_device_id=501,
        attributes=["description", "enabled"],
    )

    async with session() as db:
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, ["description"])  # remove "enabled"
        attrs = {s.attribute for s in result}
        assert attrs == {"description"}


async def test_set_scope_idempotent(adapter_client_with_nso):
    """set_scope with the same list twice leaves exactly that set of rows."""
    from nso_adapter.core.onboarding import set_scope
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-idempotent",
        netbox_device_id=502,
        attributes=["description"],
    )

    async with session() as db:
        device = await db.get(Device, device_id)
        r1 = await set_scope(db, device, ["description"])
        r2 = await set_scope(db, device, ["description"])
        assert {s.attribute for s in r1} == {s.attribute for s in r2} == {"description"}


async def test_set_scope_empty_list_clears_scope(adapter_client_with_nso):
    """set_scope with [] removes all managed attributes."""
    from nso_adapter.core.onboarding import set_scope
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-clear",
        netbox_device_id=503,
        attributes=["description"],
    )

    async with session() as db:
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, [])
        assert result == []


# ── _seed_onboarding_failover: the persisted step carries no store diagnostics ──


async def test_failover_seed_failure_step_classifies_the_store_error(adapter_client_with_nso, monkeypatch):
    """A failed seed reports the failure TYPE, never the driver's repr.

    The step is best-effort, so it is persisted and served rather than raised. A SQLAlchemy
    error repeats the statement it ran and the parameters it bound, and this row's parameters
    are the device's management addresses.
    """
    from nso_adapter.config import get_config
    from nso_adapter.core.onboarding import _seed_onboarding_failover

    monkeypatch.setattr(get_config().scheduler, "enable_failover", True)
    absent_device_id = 987654321  # no devices row, so the seed's INSERT violates its FK

    async with session() as db:
        step = await _seed_onboarding_failover(db, absent_device_id, "198.51.100.10", "203.0.113.10", "primary")

    assert step == {"step": "failover_seed", "status": "failed", "failure": "IntegrityError"}


async def test_failover_seed_success_step_is_unchanged(adapter_client_with_nso, monkeypatch):
    """The ok path still reports the address it seeded: only the failure branch changed."""
    from nso_adapter.config import get_config
    from nso_adapter.core.onboarding import _seed_onboarding_failover, onboard_device

    monkeypatch.setattr(get_config().scheduler, "enable_failover", True)

    async with session() as db:
        device = await onboard_device(db, "nso-dev", f"seed-{uuid4().hex[:8]}", int(uuid4().int % 10**8))
        await db.commit()
        step = await _seed_onboarding_failover(db, device.id, "198.51.100.10", "203.0.113.10", "oob")

    assert step == {"step": "failover_seed", "status": "ok", "detail": "oob"}


async def test_the_PROVISIONED_record_carries_step_names_and_statuses_but_no_step_DETAIL(
    adapter_client_with_nso,
):
    """`steps` is a collection, so the identifier policy has to reach inside it.

    Step details carry caller-supplied text — the requested admin-state, the derived
    device-type — and the failover-bootstrap branch renders both the primary and the OOB
    address. The terminal record is a diagnostic sink, so it gets the fixed step names and
    status classifications and nothing else; the caller still reads the full steps list off
    the job result, which is a response, not a sink.
    """
    from unittest.mock import AsyncMock, patch

    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.nso.client import NsoClient
    from tests._secret_discipline import assert_records_free_of

    submitted_name = "placeholder-caller-provisioned-device"
    submitted_admin_state = "placeholder-caller-admin-state"
    client = AsyncMock(spec=NsoClient)
    client.device_exists.return_value = False
    client.sync_from.return_value = True

    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async with session() as db:
            with capture_logs() as logs:
                result = await provision_nso_device(
                    db,
                    nso_instance="nso-dev",
                    device_name=submitted_name,
                    address="198.51.100.20",
                    ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                    authgroup="network",
                    netbox_device_id=int(uuid4().int % 10**8),
                    admin_state=submitted_admin_state,
                )

    assert result["ok"] is True
    provisioned = [record for record in logs if record["event"] == "device.provisioned"]
    assert len(provisioned) == 1
    assert provisioned[0]["steps"] == [
        {"step": "create", "status": "ok"},
        {"step": "admin_state", "status": "ok"},
        {"step": "fetch_host_keys", "status": "ok"},
        {"step": "sync_from", "status": "ok"},
        {"step": "adapter_mapping", "status": "ok"},
    ]
    # The TERMINAL record is this finding's scope. The six other `nso_device=` sites in this
    # module pre-date the stack on main and belong to the diagnostic-identity migration, which
    # needs the keyed reference to name a device that has no adapter row yet.
    assert_records_free_of(provisioned, [submitted_name, submitted_admin_state, "device-type=", "198.51.100.20"])
    assert provisioned[0]["device_id"] is not None, "the record must stay correlatable"
    # The response keeps what the sink drops.
    assert {"step": "admin_state", "status": "ok", "detail": submitted_admin_state} in result["steps"]


async def test_an_UNLINKED_provision_is_still_correlatable_without_a_device_id(adapter_client_with_nso):
    """A provision with no NetBox link creates no adapter row, so `device_id` is None.

    Dropping the submitted name would leave that record unaddressable, which is the failure mode
    the identity design warns about. The keyed `device_ref` names the pair without repeating it,
    and `job_id` correlates the record to the job that produced it.
    """
    import re
    from unittest.mock import AsyncMock, patch

    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.domain.diagnostics import DEVICE_REF_PATTERN
    from nso_adapter.nso.client import NsoClient
    from tests._secret_discipline import assert_records_free_of

    submitted_name = "placeholder-caller-unlinked-provision"
    client = AsyncMock(spec=NsoClient)
    client.device_exists.return_value = False
    client.sync_from.return_value = True

    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async with session() as db:
            with capture_logs() as logs:
                result = await provision_nso_device(
                    db,
                    nso_instance="nso-dev",
                    device_name=submitted_name,
                    address="198.51.100.22",
                    ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                    authgroup="network",
                    netbox_device_id=None,
                    job_id=4242,
                )

    assert result["ok"] is True
    assert result["device_id"] is None  # no NetBox link, so no adapter row
    record = next(r for r in logs if r["event"] == "device.provisioned")
    assert "device_id" not in record
    assert re.fullmatch(DEVICE_REF_PATTERN, record["device_ref"]), "the keyed reference carries it"
    assert record["job_id"] == 4242, "and the job correlates the record to what produced it"
    assert_records_free_of([record], [submitted_name])


async def test_a_NONFATAL_step_failure_keeps_its_CLASSIFICATION_in_the_record(adapter_client_with_nso):
    """sync-from is non-fatal, so its failure only ever surfaces through the terminal record.

    Dropping the whole step payload would make an auth failure and an outage read identically
    there. `failure` is authored by `failure_detail`, so it stays; `detail` is descriptive and
    does not.
    """
    from unittest.mock import AsyncMock, patch

    import httpx
    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.nso.client import NsoClient
    from tests._secret_discipline import assert_records_free_of

    submitted_name = "placeholder-caller-unsynced-device"
    request = httpx.Request("POST", "https://nso.invalid/restconf/placeholder-sync-url")
    client = AsyncMock(spec=NsoClient)
    client.device_exists.return_value = False
    client.sync_from.side_effect = httpx.HTTPStatusError(
        "placeholder-server-text", request=request, response=httpx.Response(401, request=request)
    )

    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async with session() as db:
            with capture_logs() as logs:
                result = await provision_nso_device(
                    db,
                    nso_instance="nso-dev",
                    device_name=submitted_name,
                    address="198.51.100.21",
                    ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                    authgroup="network",
                    netbox_device_id=int(uuid4().int % 10**8),
                )

    assert result["ok"] is True  # sync-from is non-fatal
    record = next(r for r in logs if r["event"] == "device.provisioned")
    sync_step = next(s for s in record["steps"] if s["step"] == "sync_from")
    assert sync_step == {"step": "sync_from", "status": "failed", "failure": "HTTPStatusError (HTTP 401)"}
    assert not any("detail" in step for step in record["steps"]), "descriptive detail is not a sink field"
    assert_records_free_of(
        [record], [submitted_name, "placeholder-server-text", "placeholder-sync-url", "device-type="]
    )


def test_every_production_provision_call_carries_the_job_correlator() -> None:
    """`device.provisioned` is addressable only through `device_id` or `job_id`.

    `device_id` is absent for a provision with no NetBox link, so `job_id` is the only
    correlator left on that path. Both default to `None` on the signature, which makes an
    unaddressable success record representable. The one production caller passes `job_id`;
    this pins that rather than rejecting a combination no caller can reach.
    """
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "nso_adapter"
    missing = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", None)
            if name != "provision_nso_device":
                continue
            if not any(keyword.arg == "job_id" for keyword in node.keywords):
                missing.append(f"{path.relative_to(root)}:{node.lineno}")

    assert missing == [], "a provision that reaches NSO must carry job_id, or its record is unaddressable"
