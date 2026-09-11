# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/apply.py — enqueue_apply and run_apply."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import enqueue_apply
from nso_adapter.core.apply import run_apply as _run_apply_worker
from nso_adapter.nso.apply import nokia_routed_kind
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.device_settle import create_counter
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    Job,
    JobStatus,
    JobType,
    SyncState,
)
from tests.conftest import attach_apply_generation, note_projection_write, session

# ── nokia_routed_kind (pure: derives SR OS router context from kind/service/vrf) ──


def _iface(kind, service="", vrf=""):
    return SimpleNamespace(kind=kind, service=service, vrf=vrf)


def test_nokia_routed_kind_none_for_non_routed_interfaces():
    assert nokia_routed_kind(_iface("physical")) is None
    assert nokia_routed_kind(_iface("lag")) is None


def test_nokia_routed_kind_base_when_no_service():
    assert nokia_routed_kind(_iface("loopback")) == "base"
    assert nokia_routed_kind(_iface("logical")) == "base"


def test_nokia_routed_kind_vprn_when_vrf_equals_service():
    assert nokia_routed_kind(_iface("logical", service="VPRN-A", vrf="VPRN-A")) == "vprn"


def test_nokia_routed_kind_ies_when_service_global_table_or_mismatched_vrf():
    assert nokia_routed_kind(_iface("logical", service="IES-1", vrf="")) == "ies"
    assert nokia_routed_kind(_iface("logical", service="SVC", vrf="other")) == "ies"


# ── _nokia_attr_kind (attribute-write context: routed kinds + lag) ────────────────


def test_nokia_attr_kind_lag_for_a_lag_interface():
    """A Nokia LAG's description/admin-state belong on `configure lag`, so the attribute-write
    context is 'lag' — routed-kind returns None for a lag (it never carries an IP)."""
    from nso_adapter.nso.apply import nokia_attr_kind

    assert nokia_attr_kind(_iface("lag")) == "lag"


def test_nokia_attr_kind_matches_routed_for_l3_and_ports():
    """For everything except a lag, the attribute context is the routed context: base/ies/vprn
    for L3 routed interfaces, None for a physical port (→ the legacy port path)."""
    from nso_adapter.nso.apply import nokia_attr_kind

    assert nokia_attr_kind(_iface("loopback")) == "base"
    assert nokia_attr_kind(_iface("logical", service="VPRN-A", vrf="VPRN-A")) == "vprn"
    assert nokia_attr_kind(_iface("logical", service="IES-1", vrf="")) == "ies"
    assert nokia_attr_kind(_iface("physical")) is None


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _seed_device(name: str = "test-rtr", netbox_id: int = 1) -> int:
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        await create_counter(db, d.id)
        await db.commit()
        await db.refresh(d)
        return d.id


async def _seed_apply_job(device_id: int, status: JobStatus = JobStatus.running) -> int:
    """Create a job row as the worker head leaves it.

    The local ``run_apply`` harness attaches a generation after each test finishes seeding
    intent. The real worker still receives only a generation-backed job.
    """
    async with session() as db:
        j = Job(
            job_type=JobType.apply,
            device_id=device_id,
            status=status,
            coalescible=True,
            run_attempt=1 if status is JobStatus.running else 0,
        )
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id


async def run_apply(job_id: int, device_id: int, force: bool = True, reg=None) -> None:
    """Attach the immutable fixture document, then invoke the real Apply worker."""
    async with session() as db:
        job = await db.get(Job, job_id)
    if job is not None:
        await attach_apply_generation(job_id, job.device_id)
    await _run_apply_worker(job_id=job_id, device_id=device_id, force=force, reg=reg)


#: The ONE sender every deployment goes through: a PUT of the device's whole document.
_SENDER = "nso_adapter.nso.apply.apply_device_intent"


def _nso_client(**attrs) -> AsyncMock:
    """A client whose ``device-intent`` instance is ABSENT.

    Nothing is retained and the device-wide collateral guard has no live row to block on, so a
    test that is not about retention or collateral sees exactly the send it seeded.
    """
    from nso_adapter.nso.client import ServiceInstanceState

    client = AsyncMock(spec=NsoClient)
    client.get_service_config = AsyncMock(return_value=None)
    client.service_instance_state = AsyncMock(return_value=ServiceInstanceState("absent", None))
    for name, value in attrs.items():
        setattr(client, name, value)
    return client


def sent_document(sender: AsyncMock, index: int = -1) -> dict:
    """The families one transmitted document carried: ``{YANG container: body}``."""
    call = sender.await_args_list[index]
    return call.args[2] if len(call.args) > 2 else call.kwargs["containers"]


def sent_list(sender: AsyncMock, container: str, label: str, index: int = -1) -> list:
    """One YANG list out of a transmitted family, empty when the family carried none."""
    return (sent_document(sender, index).get(container) or {}).get(label) or []


async def _seed_interface_with_intent(
    device_id: int,
    iface_name: str,
    attribute: str,
    intent_value: str,
    sync_state: SyncState,
    netbox_id: int = 100,
    **iface_fields,
) -> tuple[int, int]:
    """Create DbInterface + InterfaceAttrState + InterfaceIntent, return (iface_id, attr_id)."""
    async with session() as db:
        iface = DbInterface(
            device_id=device_id,
            netbox_interface_id=netbox_id,
            name=iface_name,
            **iface_fields,
        )
        db.add(iface)
        await db.flush()

        attr_state = InterfaceAttrState(
            interface_id=iface.id,
            attribute=attribute,
            sync_state=sync_state,
        )
        db.add(attr_state)

        intent = InterfaceIntent(
            interface_id=iface.id,
            attribute=attribute,
            intent_value=intent_value,
            accepted_at=datetime.now(UTC),
        )
        db.add(intent)
        await db.commit()
        await db.refresh(iface)
        await db.refresh(attr_state)
        return iface.id, attr_state.id


# ── the interface family's writer context reaches the wire (Finding C-drift) ──────
#
# The per-item attribute PATCH is gone with the reconcilers: description and admin state now
# ride the ONE interface entry of the device's document. The subject is unchanged, so these
# drive the real worker and read the transmitted container.


async def test_the_transmitted_interface_entry_carries_the_nokia_routed_context(adapter_client):
    """A Nokia routed interface is written as base|ies|vprn, not as a phantom port.

    The routed context decides WHERE the description lands on SR OS, so losing it writes
    ``configure port <logical-name>`` for an interface that has no port (Finding C-drift).
    """
    device_id = await _seed_device("ra1", 7001)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id,
        "CRPD-VPN:LO7",
        "description",
        "loopback for CRPD-VPN",
        SyncState.accepted,
        netbox_id=7001,
        kind="loopback",
        service="CRPD-VPN",
        vrf="CRPD-VPN",  # vrf == service ⇒ vprn
        parent_binding="lag-99",
        encap_tag="10",
    )

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    (entry,) = sent_list(sender, "interface", "interface")
    # RED against the aggregate encoder: the routed context is emitted only for an interface
    # that also carries an address, so an attribute-only interface loses it (reported).
    assert entry["kind"] == "vprn"
    assert entry["service"] == "CRPD-VPN"
    assert entry["parent-binding"] == "lag-99"
    assert entry["encap-tag"] == "10"
    assert entry["description"] == "loopback for CRPD-VPN"
    async with session() as db:
        rows = (await db.execute(select(InterfaceIntent))).scalars().all()
        assert all(row.last_apply_at is not None for row in rows), "the success stamp missed the live row"


async def test_the_transmitted_interface_entry_carries_lag_kind_for_a_nokia_lag(adapter_client):
    """A Nokia LAG is written as ``kind=lag``, so the description lands on ``configure lag``."""
    device_id = await _seed_device("ra2", 7003)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id, "lag-30", "description", "uplink bundle", SyncState.accepted, netbox_id=7003, kind="lag"
    )

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    (entry,) = sent_list(sender, "interface", "interface")
    # RED for the same reason as the vprn case above (reported).
    assert entry["kind"] == "lag"
    assert entry["description"] == "uplink bundle"
    assert "service" not in entry, "a LAG is not an IES/VPRN service"


async def test_the_transmitted_interface_entry_omits_the_routed_context_off_nokia(adapter_client):
    """An interface with no kind carries no routed context, so no false kind leaks to IOS."""
    device_id = await _seed_device("core-rtr-01", 7002)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id, "GigabitEthernet0/0", "enabled", "true", SyncState.accepted, netbox_id=7002
    )

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    (entry,) = sent_list(sender, "interface", "interface")
    assert entry["enabled"] is True
    assert "kind" not in entry and "service" not in entry
    assert "parent-binding" not in entry and "encap-tag" not in entry


# ── enqueue_apply ─────────────────────────────────────────────────────────────


async def test_enqueue_apply_creates_job(adapter_client):
    """enqueue_apply creates an Apply job when no queued Apply exists."""
    device_id = await _seed_device("rtr-a01", 101)
    async with session() as db:
        await note_projection_write(db, device_id, "vlan")
        job = await enqueue_apply(db, device_id=device_id, stream="vlan")
        assert job is not None
        assert job.job_type == JobType.apply
        assert job.status == JobStatus.queued


async def test_enqueue_apply_blocked_by_a_queued_apply(adapter_client):
    """enqueue_apply refuses only when a QUEUED apply already exists."""
    device_id = await _seed_device("rtr-a02", 102)
    await _seed_apply_job(device_id, JobStatus.queued)

    async with session() as db:
        await note_projection_write(db, device_id, "vlan")
        assert await enqueue_apply(db, device_id=device_id, stream="vlan") is None


async def test_enqueue_apply_admitted_while_an_apply_runs(adapter_client):
    """A running apply does not refuse its successor: the successor carries the newer
    intent, and the device claim is what serializes their execution."""
    device_id = await _seed_device("rtr-a02b", 103)
    await _seed_apply_job(device_id, JobStatus.running)

    async with session() as db:
        await note_projection_write(db, device_id, "vlan")
        assert await enqueue_apply(db, device_id=device_id, stream="vlan") is not None


# ── run_apply ─────────────────────────────────────────────────────────────────


async def test_run_apply_job_not_found(adapter_client, store_engine):
    """run_apply exits early when job_id doesn't exist in DB."""
    device_id = await _seed_device("rtr-a10", 110)
    # Should not raise — just log and return
    await run_apply(job_id=99999, device_id=device_id)
    assert store_engine.sync_engine.pool.checkedout() == 0


async def test_run_apply_refuses_a_job_without_a_generation_before_device_access(adapter_client):
    """A corrupt carrier must not deploy whatever happens to be in the live store."""
    device_id = await _seed_device("rtr-missing-generation", 109)
    async with session() as db:
        job = Job(
            job_type=JobType.apply,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=True,
            run_attempt=1,
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    client_requested = False

    def _unexpected_client(_instance):
        nonlocal client_requested
        client_requested = True
        raise AssertionError("an Apply without a generation must fail before device access")

    with patch("nso_adapter.core.importer.get_nso_client", _unexpected_client):
        await _run_apply_worker(job_id=job_id, device_id=device_id)

    assert not client_requested
    async with session() as db:
        failed = await db.get(Job, job_id)
        assert failed.status is JobStatus.failed
        assert failed.error == {
            "code": "apply_generation_missing",
            "message": f"Apply job {job_id} carries no generation to deploy.",
            "detail": {},
        }


async def test_run_apply_device_not_found(adapter_client):
    """run_apply marks job failed when device_id doesn't exist."""
    device_id = await _seed_device("rtr-a11", 111)
    job_id = await _seed_apply_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=KeyError("nso-dev")):
        await run_apply(job_id=job_id, device_id=99998)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


async def test_run_apply_nothing_eligible(adapter_client):
    """No eligible row anywhere: succeed with an all-zero result and touch no device.

    The counters are the registry's, so a family added later gets its zero for free instead
    of silently missing from the result the plugin reads.
    """
    from nso_adapter.core.apply import _result_keys

    device_id = await _seed_device("rtr-empty", 200)
    job_id = await _seed_apply_job(device_id)

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_not_awaited()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {f"{key}_count_by_outcome": {"in_sync": 0, "apply_failed": 0} for key in _result_keys()}


async def _set_sync_before_apply(device_id: int, value: bool) -> None:
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=False, sync_before_apply=value))
        await db.commit()
        return


async def test_run_apply_syncs_from_device_by_default(adapter_client):
    """With no DeviceSettings (or sync_before_apply on), run_apply sync-froms the device
    before pushing intent — clears the out-of-sync a prior timed-out commit can leave."""
    device_id = await _seed_device("rtr-sync-on", 130)
    job_id = await _seed_apply_job(device_id)

    mock_client = _nso_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.sync_from.assert_awaited_once_with("rtr-sync-on")


async def test_run_apply_skips_sync_from_when_disabled(adapter_client):
    """sync_before_apply=False (per-device toggle) skips the pre-apply sync-from — for
    NEDs that already sync on connect."""
    device_id = await _seed_device("rtr-sync-off", 131)
    await _set_sync_before_apply(device_id, False)
    job_id = await _seed_apply_job(device_id)

    mock_client = _nso_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.sync_from.assert_not_called()


async def test_run_apply_survives_sync_from_failure(adapter_client):
    """A failing pre-apply sync-from is best-effort — it must not fail the apply."""
    device_id = await _seed_device("rtr-sync-err", 132)
    job_id = await _seed_apply_job(device_id)

    mock_client = _nso_client()
    mock_client.sync_from.side_effect = RuntimeError("transport timeout")
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # nothing eligible, sync error swallowed


async def _preview_head(device_id: int) -> int:
    """Give the device an executable generation head — the document a preview is bound to."""
    job_id = await _seed_apply_job(device_id)
    await attach_apply_generation(job_id, device_id)
    return job_id


async def _seed_ospf(device_id: int, router_id: str = "1.1.1.1") -> None:
    from nso_adapter.store.models import OspfInstanceIntent

    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id=router_id, accepted_at=datetime.now(UTC))
        )
        await db.commit()


async def test_collect_apply_diff_returns_the_documents_delta(adapter_client):
    """One document is one transaction, so the preview is ONE dry-run and ONE delta."""
    from nso_adapter.core.apply import PREVIEW_KEY, collect_apply_diff

    device_id = await _seed_device("rtr-diff", 199)
    await _seed_ospf(device_id)
    await _preview_head(device_id)

    sender = AsyncMock(return_value="DEVICE NATIVE DELTA")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

    assert diffs == {PREVIEW_KEY: "DEVICE NATIVE DELTA"}
    assert sender.await_args.kwargs["dry_run"] is True, "a preview commits nothing"
    assert sent_list(sender, "ospf", "process-config")[0]["process-id"] == "1"


async def test_collect_apply_diff_empty_delta_is_omitted(adapter_client):
    """A device already holding its document previews nothing, not an empty string."""
    from nso_adapter.core.apply import collect_apply_diff

    device_id = await _seed_device("rtr-diff2", 198)
    await _seed_ospf(device_id)
    await _preview_head(device_id)

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, AsyncMock(return_value="   ")),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)
    assert diffs == {}


async def test_collect_apply_diff_outformat_cli_threads_the_format(adapter_client):
    """``outformat='cli'`` asks NSO for the NED-uniform tree diff the preview panel renders."""
    from nso_adapter.core.apply import PREVIEW_KEY, collect_apply_diff

    device_id = await _seed_device("rtr-diff-cli", 197)
    await _seed_ospf(device_id)
    await _preview_head(device_id)

    sender = AsyncMock(return_value="+ router ospf 1")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id, outformat="cli")

    assert diffs == {PREVIEW_KEY: "+ router ospf 1"}
    assert sender.await_args.kwargs["dry_run"] == "cli"


async def test_collect_apply_diff_previews_every_family_of_the_document(adapter_client):
    """The previewed body is the whole document: one dry-run carries every family at once."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-diff3", 196)
    await _seed_ospf(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
    await _preview_head(device_id)

    sender = AsyncMock(return_value="DELTA")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        async with session() as db:
            await collect_apply_diff(db, device_id)

    sender.assert_awaited_once()
    assert sent_list(sender, "ospf", "process-config")[0]["process-id"] == "1"
    assert sent_list(sender, "static-route", "route")[0]["prefix"] == "10.0.0.0/24"


async def test_collect_apply_diff_previews_the_document_not_the_live_store(adapter_client):
    """A store-only edit never reaches the device, so it must never reach the preview either.

    The preview is the dry-run OF THE DOCUMENT BEING COMMITTED (#1683): showing a live-store
    estimate would render a diff the commit cannot produce.
    """
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff-store-only", 195)
    await _seed_ospf(device_id, router_id="1.1.1.1")
    await _preview_head(device_id)
    async with session() as db:
        row = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalar_one()
        row.router_id = "9.9.9.9"
        await db.commit()

    sender = AsyncMock(return_value="DELTA")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        async with session() as db:
            await collect_apply_diff(db, device_id)

    assert sent_list(sender, "ospf", "process-config")[0]["router-id"] == "1.1.1.1"


async def test_collect_apply_diff_device_not_found(adapter_client):
    """A preview for an unknown device returns an empty mapping (no NSO call)."""
    from nso_adapter.core.apply import collect_apply_diff

    async with session() as db:
        diffs = await collect_apply_diff(db, 999999)
    assert diffs == {}


async def test_collect_apply_diff_without_a_generation_reports_unavailable(adapter_client):
    """UNAVAILABLE, never empty: an empty preview reads as "nothing to do" to the operator."""
    from nso_adapter.core.apply import PREVIEW_KEY, collect_apply_diff

    device_id = await _seed_device("rtr-diff-none", 194)
    await _seed_ospf(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

    assert list(diffs) == [PREVIEW_KEY]
    assert diffs[PREVIEW_KEY].startswith("!! preview unavailable")


async def test_collect_apply_diff_classifies_an_unexpected_dry_run_failure(adapter_client):
    """A preview reports the exception type without exposing its untrusted text."""
    from nso_adapter.core.apply import PREVIEW_KEY, collect_apply_diff

    device_id = await _seed_device("rtr-diff-boom", 193)
    await _seed_ospf(device_id)
    await _preview_head(device_id)
    secret = "placeholder-preview-secret"

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, AsyncMock(side_effect=RuntimeError(secret))),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

    assert diffs[PREVIEW_KEY].startswith("!! preview unavailable")
    assert "RuntimeError" in diffs[PREVIEW_KEY]
    assert secret not in diffs[PREVIEW_KEY]


async def test_run_apply_all_succeed(adapter_client):
    """run_apply marks job succeeded when all attributes apply successfully."""
    device_id = await _seed_device("rtr-a13", 113)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/0",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=200,
    )

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 1
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 0


async def test_run_apply_refreshes_mirror_and_notifies_plugin(adapter_client):
    """After a finalized Apply, run_apply re-reads the applied surfaces into the read-mirror and
    fires the plugin sync-complete callback, so a 'deploying' overlay row settles on the immediate
    post-apply reconcile instead of only on the next periodic sync (the route-policy rg03 race).

    Before this, Apply pushed config to NSO but never refreshed the cache-only GET endpoints or
    notified the plugin, so the plugin's presence-based settle read a stale mirror (applied object
    not yet present) and re-marked the row 'deploying' — settling only on the next 15-min sync."""
    from nso_adapter.bindings.netbox.client import NetboxClient
    from nso_adapter.core import importer as imp

    device_id = await _seed_device("rtr-settle", 321)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/0",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=400,
    )

    mock_client = _nso_client()
    # READSEM grain b: post-apply consumes the projected doc. Serve BOTH families' sections
    # with a row each so their materializers demonstrably run.
    mock_client.get_device_state_doc.return_value = {
        "device-name": "rtr-settle",
        "route-policy": {"status": "ok", "prefix-list": [{"name": "PL-SETTLE", "entry": []}]},
        "svi": {"status": "ok", "interface": [{"interface-name": "Vlan77"}]},
    }
    nb = AsyncMock(spec=NetboxClient)
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
            patch(_SENDER, new_callable=AsyncMock),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True)
    finally:
        imp._netbox_client = None

    # Both post-apply fan-outs (routing AND config) consumed the projected doc — one doc GET
    # each (codex S3-R2 F5's two-fan-out proof under grain b) ...
    assert mock_client.get_device_state_doc.await_count == 2, mock_client.get_device_state_doc.await_count
    # ... their sections actually materialized (the families that back a 'deploying' row) ...
    async with session() as db:
        from nso_adapter.store.models import DeviceRoutePolicyPrefixList, DeviceSvi

        pls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device_id)
                )
            )
            .scalars()
            .all()
        )
        svis = (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device_id))).scalars().all()
        assert [x.name for x in pls] == ["PL-SETTLE"]
        assert [x.interface_name for x in svis] == ["Vlan77"]
    # ... and the plugin was notified so its post-apply reconcile settles the deploying row.
    nb.notify_sync_complete.assert_awaited_once_with(321)


async def test_run_apply_post_refresh_failure_does_not_fail_job(adapter_client):
    """The post-apply refresh/notify is best-effort: the Apply job is already finalized, so a
    failure re-reading the mirror or notifying the plugin must NOT flip a succeeded job to failed
    (the periodic sync is the backstop)."""
    from nso_adapter.core import importer as imp

    device_id = await _seed_device("rtr-settle-fail", 322)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/1",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=401,
    )

    mock_client = _nso_client()
    imp._netbox_client = None  # get_netbox_client() -> None; helper must still not raise
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
        patch(
            "nso_adapter.core.importer.refresh_routing_surfaces_for_device",
            new_callable=AsyncMock,
            side_effect=RuntimeError("NSO unreachable"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # unchanged by the best-effort post-refresh


async def test_run_apply_partial_failure(adapter_client):
    """run_apply marks job failed when some attributes fail to apply."""
    from nso_adapter.nso.apply import NsoApplyError

    device_id = await _seed_device("rtr-a14", 114)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/1",
        attribute="description",
        intent_value="downlink",
        sync_state=SyncState.accepted,
        netbox_id=201,
    )

    mock_client = _nso_client()
    nso_err = NsoApplyError(code="nso_error", message="NSO rejected commit", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "nso_commit_failed"
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 1


async def test_run_apply_unexpected_exception_on_attribute(adapter_client):
    """run_apply handles unexpected (non-NsoApplyError) exceptions per-attribute."""
    device_id = await _seed_device("rtr-a15", 115)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/2",
        attribute="description",
        intent_value="mgmt",
        sync_state=SyncState.drifted,
        netbox_id=202,
    )

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=RuntimeError("unexpected internal error")),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert (
            "unexpected internal error" in str(job.result)
            or job.result["attribute_count_by_outcome"]["apply_failed"] == 1
        )


async def test_run_apply_does_not_reselect_recorded_interface_eligibility(adapter_client):
    """Execution uses the generation's eligibility record, not current worker flags."""
    device_id = await _seed_device("rtr-a16", 116)
    job_id = await _seed_apply_job(device_id)
    # Generation creation records in_sync as eligible. Worker flags cannot revise that fact.
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/3",
        attribute="description",
        intent_value="in-sync-iface",
        sync_state=SyncState.in_sync,
        netbox_id=203,
    )

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=False)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 1


async def test_run_apply_outer_exception(adapter_client):
    """run_apply marks job failed on an outer unexpected exception."""
    device_id = await _seed_device("rtr-a17", 117)
    job_id = await _seed_apply_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=RuntimeError("DB boom")):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "internal"


# ── IP intent apply pass ───────────────────────────────────────────────────


async def _seed_iface(device_id: int, iface_name: str) -> int:
    """Create a bare DbInterface row and return its id."""
    async with session() as db:
        iface = DbInterface(device_id=device_id, name=iface_name)
        db.add(iface)
        await db.flush()
        iface_id = iface.id
        await db.commit()
        return iface_id
    raise RuntimeError("unreachable")


async def _seed_ip_intent(
    interface_id: int,
    *,
    address: str,
    family: str = "ipv4",
    secondary: bool = False,
    vrf: str = "",
    accepted: bool = True,
) -> None:
    """Seed an InterfaceIpIntent row."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import InterfaceIpIntent

    async with session() as db:
        row = InterfaceIpIntent(
            interface_id=interface_id,
            address=address,
            vrf=vrf,
            family=family,
            secondary=secondary,
            accepted_at=datetime.now(UTC) if accepted else None,
        )
        db.add(row)
        await db.commit()


@pytest.mark.anyio
async def test_run_apply_ip_intent_success(adapter_client):
    """IP intent rows are applied and last_apply_at is set on success."""
    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-01", 201)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/1")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.0.1/24", family="ipv4")

    mock_nso = _nso_client()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        result = job.result
        assert result["ip_count_by_outcome"]["in_sync"] == 1
        assert result["ip_count_by_outcome"]["apply_failed"] == 0
        # Verify last_apply_at was stamped
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert ip_rows[0].last_apply_at is not None
        assert ip_rows[0].last_apply_error is None

    sender.assert_awaited_once()


@pytest.mark.anyio
async def test_run_apply_ip_intent_failure_marks_error(adapter_client):
    """When the document PUT is rejected, every address row records the error."""
    from sqlalchemy import select

    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-02", 202)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/2")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.1.1/30", family="ipv4")

    mock_nso = _nso_client()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch(
            _SENDER,
            new_callable=AsyncMock,
            side_effect=NsoApplyError("nso_patch_failed", "NSO returned 500"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["ip_count_by_outcome"]["apply_failed"] == 1
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert ip_rows[0].last_apply_error is not None
        assert ip_rows[0].last_apply_error["code"] == "nso_patch_failed"


@pytest.mark.anyio
async def test_run_apply_ip_intent_not_accepted_skipped(adapter_client):
    """IP rows without accepted_at are not eligible and not applied."""
    device_id = await _seed_device("rtr-ip-03", 203)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/3")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.2.1/24", family="ipv4", accepted=False)

    mock_nso = _nso_client()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_not_awaited()


@pytest.mark.anyio
async def test_run_apply_ip_already_applied_skipped_without_force(adapter_client):
    """IP rows with last_apply_at set and no error are skipped when force=False."""
    from datetime import UTC, datetime

    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-04", 204)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/4")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.3.1/24", family="ipv4", accepted=True)

    # Stamp last_apply_at to simulate already-applied
    async with session() as db:
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        rows[0].last_apply_at = datetime.now(UTC)
        rows[0].last_apply_error = None
        await db.commit()

    mock_nso = _nso_client()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=False)

    sender.assert_not_awaited()


async def test_run_apply_bgp_intent_does_not_crash_on_commit(adapter_client):
    """Regression: a dirty BgpRouterIntent must not crash the apply commit.

    The apply manually eager-loads BGP relationships (scopes/peers/afs). It used to write
    raw Python lists into __dict__, which bypasses SQLAlchemy instrumentation — so once the
    row was marked applied (dirty) the commit flush hit
    'list object has no attribute _sa_adapter' and aborted the ENTIRE job. set_committed_value
    instruments the collection, so flush sees committed (empty-history) state.
    """

    from nso_adapter.store.models import BgpRouterIntent

    device_id = await _seed_device("rtr-bgp-crash", 555)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(BgpRouterIntent(device_id=device_id, asn="65100", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["bgp_count_by_outcome"]["in_sync"] == 1


# ── one family at a time, through the ONE sender ──────────────────────────────
#
# Every family shares one shape now: the document's rows are encoded under the family's YANG
# container, the whole document is PUT once, and on success every row the body carried is
# stamped in_sync. The parametrized case locks that wiring (right container, right list, right
# result counter, rows stamped) for every single-list family; the cases below cover the
# multi-list families and the failure paths.


# (model_name, row kwargs, YANG container, YANG list, result-dict key)
_SCOPE_CASES = [
    (
        "StaticRouteIntent",
        dict(vrf="", prefix="10.9.0.0/24", next_hop="10.9.0.1"),
        "static-route",
        "route",
        "static_route",
    ),
    ("LoggingHostIntent", dict(address="10.9.0.99"), "logging", "host", "logging"),
    ("SviIntent", dict(interface_name="Vlan10", vlan_id=10), "svi", "interface", "svi"),
    (
        "SubinterfaceIntent",
        dict(interface_name="GigabitEthernet0/0.10"),
        "subinterface",
        "interface",
        "subinterface",
    ),
    ("VlanIntent", dict(vlan_id=20), "vlan", "vlan", "vlan"),
    ("BfdIntent", dict(interface_name="GigabitEthernet0/1"), "bfd", "interface", "bfd"),
    (
        "InterfaceMtuIntent",
        dict(interface_name="GigabitEthernet0/2", mtu=9000),
        "mtu",
        "interface",
        "interface_mtu",
    ),
    (
        "L2SapIntent",
        dict(service_name="EPIPE-1", service_type="epipe", sap_id="1/1/1"),
        "l2-sap",
        "sap",
        "l2_sap",
    ),
    (
        "SnmpCommunityIntent",
        dict(label="ro", vault_ref="network/netbox/snmp/community/ro#community", access="ro"),
        "snmp",
        "community",
        "snmp",
    ),
    ("OspfInstanceIntent", dict(process_id="1", router_id="9.9.9.9"), "ospf", "process-config", "ospf"),
    (
        "IsisInterfaceIntent",
        dict(interface_name="GigabitEthernet0/3", af="ipv4"),
        "isis",
        "interface-config",
        "isis",
    ),
]


@pytest.mark.parametrize("model_name, kwargs, container, label, result_key", _SCOPE_CASES)
async def test_run_apply_family_is_transmitted_and_stamped(
    adapter_client, model_name, kwargs, container, label, result_key
):
    """Each single-list family reaches the wire under its container and reports in_sync."""
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-{result_key}", 300)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    assert len(sent_list(sender, container, label)) == 1, sent_document(sender)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result[f"{result_key}_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        # the row was stamped applied
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_at is not None
        assert rows[0].last_apply_error is None


async def test_run_apply_logging_transmits_and_stamps_the_levels_singleton(adapter_client):
    """The accepted local-levels singleton rides the logging container and is stamped with it."""
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    device_id = await _seed_device("rtr-logging-lvl", 311)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(LoggingHostIntent(device_id=device_id, address="10.9.0.98", accepted_at=datetime.now(UTC)))
        db.add(LoggingLevelsIntent(device_id=device_id, console_severity="CRITICAL", accepted_at=datetime.now(UTC)))
        await db.commit()

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
        patch("nso_adapter.nso.apply.local_levels_write_enabled", return_value=True),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    logging_body = sent_document(sender)["logging"]
    assert logging_body["host"][0]["address"] == "10.9.0.98"
    assert logging_body["local-levels"]["console-severity"] == "CRITICAL"
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # both the host row and the levels singleton count and get stamped
        assert job.result["logging_count_by_outcome"] == {"in_sync": 2, "apply_failed": 0}
        row = (
            await db.execute(select(LoggingLevelsIntent).where(LoggingLevelsIntent.device_id == device_id))
        ).scalar_one()
        assert row.last_apply_at is not None
        assert row.last_apply_error is None


async def test_run_apply_logging_levels_only_is_eligible(adapter_client):
    """A levels-only accept (no host intent at all) must still make the job do work."""
    from nso_adapter.store.models import LoggingLevelsIntent

    device_id = await _seed_device("rtr-logging-lvl2", 312)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(LoggingLevelsIntent(device_id=device_id, monitor_severity="NOTICE", accepted_at=datetime.now(UTC)))
        await db.commit()

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
        patch("nso_adapter.nso.apply.local_levels_write_enabled", return_value=True),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["logging_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}


async def test_gated_local_levels_refuse_the_whole_send(adapter_client):
    """The deploy gate refuses the DOCUMENT, because one document is one transaction.

    Omitting the logging family would retract it and sending a host-only body would
    FASTMAP-retract the severities the device already holds, so neither weaker send exists:
    the job fails having transmitted nothing, and every family stays pending.
    """
    from nso_adapter.store.models import LoggingLevelsIntent, VlanIntent

    device_id = await _seed_device("rtr-logging-gated", 313)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(LoggingLevelsIntent(device_id=device_id, monitor_severity="NOTICE", accepted_at=datetime.now(UTC)))
        db.add(VlanIntent(device_id=device_id, vlan_id=77, accepted_at=datetime.now(UTC)))
        await db.commit()

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
        patch("nso_adapter.nso.apply.local_levels_write_enabled", return_value=False),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_not_awaited()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["logging_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        # the unrelated family never reached the device either, so it stays pending
        assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 0}
        row = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalar_one()
        assert row.last_apply_at is None and row.last_apply_error is None


async def test_run_apply_a_rejected_commit_fails_every_family_it_carried(adapter_client):
    """One transaction, one outcome: a rejected PUT landed NOTHING, so nothing is in_sync.

    The old per-scope path could fail one family and commit the rest. Under a full-document
    PUT that is a lie: the whole transaction rolled back, and claiming otherwise would settle
    rows the device never took.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import StaticRouteIntent, VlanIntent

    device_id = await _seed_device("rtr-sr-fail", 310)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.8.0.0/24", next_hop="10.8.0.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(VlanIntent(device_id=device_id, vlan_id=42, accepted_at=datetime.now(UTC)))
        await db.commit()

    nso_err = NsoApplyError(code="nso_error", message="route rejected", detail={"x": 1})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, AsyncMock(side_effect=nso_err)),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.error["code"] == "nso_commit_failed"
        items = job.error["detail"]["items"]
        assert {"type": "static_route", "error": "route rejected"} in items
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        assert rows[0].last_apply_error == {"code": "nso_error", "message": "route rejected", "detail": {"x": 1}}


async def test_run_apply_unexpected_send_exception_is_recorded_as_internal(adapter_client):
    """A non-NsoApplyError from the send is caught, recorded as 'internal', job failed.

    The exception TEXT never reaches the persisted error: a transport exception can carry a
    URL with credentials in it.
    """
    from nso_adapter.store.models import VlanIntent

    device_id = await _seed_device("rtr-vlan-boom", 311)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=42, accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=RuntimeError("kaboom")),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["vlan_count_by_outcome"]["apply_failed"] == 1
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_error["code"] == "internal"
        assert "kaboom" not in str(rows[0].last_apply_error), "exception text reached the persisted error"
        assert "kaboom" not in str(job.error), "exception text reached the persisted failure items"
        assert "RuntimeError" in rows[0].last_apply_error["message"]


# The IS-IS sub-collections (process/level/flex) are eligible on their OWN — a per-level
# knob can be the only accepted row on a device whose interfaces are already in sync. They
# were missing from run_apply's `any_eligible`, so the isis scope still PUSHED (its _Scope
# rows list includes them) while _finalize_job took the "nothing eligible" early-return:
# an all-zero SUCCESS for a commit the device had rejected, which the plugin then settled
# deploying -> in_sync. These lock each sub-collection as independently apply-worthy.
_ISIS_SUBSCOPE_CASES = [
    ("IsisProcessIntent", dict(process_tag="1", net="49.0001.0000.0000.0001.00")),
    ("IsisLevelIntent", dict(process_tag="1", level=2, wide_metrics_only=True)),
    ("IsisFlexAlgoIntent", dict(process_tag="1", algo_id=128)),
]


@pytest.mark.parametrize("model_name, kwargs", _ISIS_SUBSCOPE_CASES)
async def test_run_apply_isis_subscope_failure_fails_the_job(adapter_client, model_name, kwargs):
    """An IS-IS process/level/flex row is the ONLY eligible intent and the device rejects it.

    The job must FAIL. Before the fix it reported succeeded with all-zero counts.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-isis-sub-{model_name.lower()}", 330)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    nso_err = NsoApplyError(code="nso_error", message="level rejected", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["isis_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.error["code"] == "nso_commit_failed"
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_error["message"] == "level rejected"


@pytest.mark.parametrize("model_name, kwargs", _ISIS_SUBSCOPE_CASES)
async def test_run_apply_isis_subscope_success_is_counted(adapter_client, model_name, kwargs):
    """The same row applying cleanly must be COUNTED in_sync, not reported as nothing-eligible."""
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-isis-sub-ok-{model_name.lower()}", 331)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["isis_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_at is not None


async def test_run_apply_isis_applies_process_redist_and_flexalgo(adapter_client):
    """The IS-IS pass stamps interface + process + redistribute + flex-algo rows together."""
    from nso_adapter.store.models import (
        IsisFlexAlgoIntent,
        IsisInterfaceIntent,
        IsisProcessIntent,
        RedistributionIntent,
    )

    device_id = await _seed_device("rtr-isis-combo", 320)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            IsisInterfaceIntent(device_id=device_id, interface_name="Gi0/3", af="ipv4", accepted_at=datetime.now(UTC))
        )
        db.add(IsisProcessIntent(device_id=device_id, accepted_at=datetime.now(UTC)))
        db.add(IsisFlexAlgoIntent(device_id=device_id, algo_id=128, accepted_at=datetime.now(UTC)))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="isis",
                source_protocol="connected",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    isis = sent_document(sender)["isis"]
    assert len(isis["interface-config"]) == 1
    (process,) = isis["process-config"]
    assert len(process["redistribute"]) == 1
    assert len(process["flex-algo"]) == 1
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # in_sync counts every row across the four lists
        assert job.result["isis_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}


async def test_run_apply_ospf_applies_instance_interface_and_redist(adapter_client):
    """The OSPF pass applies process + interface + ospf-destined redistribution together."""
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

    device_id = await _seed_device("rtr-ospf-combo", 321)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/4", accepted_at=datetime.now(UTC)))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="ospf",
                source_protocol="static",
                accepted_at=datetime.now(UTC),
            )
        )
        # a bgp-destined redist row must NOT be swept into the ospf pass
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="bgp", source_protocol="static", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    ospf = sent_document(sender)["ospf"]
    assert len(ospf["interface-config"]) == 1
    redistributed = [entry for process in ospf["process-config"] for entry in process.get("redistribute", [])]
    assert len(redistributed) == 1  # only the ospf-destined row
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.result["ospf_count_by_outcome"] == {"in_sync": 3, "apply_failed": 0}


async def test_run_apply_snmp_applies_all_row_types(adapter_client):
    """The SNMP pass stamps communities, v3 users, hosts and the single system-info row."""
    from nso_adapter.store.models import (
        SnmpCommunityIntent,
        SnmpHostIntent,
        SnmpSystemInfoIntent,
        SnmpV3UserIntent,
    )

    device_id = await _seed_device("rtr-snmp-all", 322)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="ro",
                vault_ref="network/netbox/snmp/community/ro#community",
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(SnmpV3UserIntent(device_id=device_id, username="v3-test-group", accepted_at=datetime.now(UTC)))
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="10.7.0.5",
                version="v2c",
                notify_type="traps",
                community_or_user="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(SnmpSystemInfoIntent(device_id=device_id, location="rack-7", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock) as sender,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    snmp = sent_document(sender)["snmp"]
    assert len(snmp["community"]) == 1
    assert len(snmp["v3-user"]) == 1
    assert len(snmp["host"]) == 1
    assert snmp["location"] == "rack-7"
    async with session() as db:
        job = await db.get(Job, job_id)
        # 3 list rows + 1 system-info row
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}


async def test_run_apply_route_policy_failure_records_capability(adapter_client):
    """A route-policy NsoApplyError fails the job AND records a capability rejection.

    The device parser only rejects an unsupported construct on a real commit (dry-run
    renders it), so the accepted-half learns the (ned, sw) limitation here.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-fail", 323)
    # give the device a ned_id so apply_route_policy_config gets one
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id = "cisco-ios-cli-6.95"
        await db.commit()
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="ipv4",
                name="RM-IN",
                entries=[],
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    nso_err = NsoApplyError(code="nso_error", message="unsupported set community RM-IN", detail={})
    rec = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            return_value={"ned_id": "cisco-ios-cli-6.95", "sw_version": "15.5"},
        ),
        patch(
            "nso_adapter.core.capability.parse_rejected_construct",
            return_value=("route-policy", "RM-IN"),
        ),
        patch("nso_adapter.core.capability.record_capability_rejection", new=rec),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    rec.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}


async def test_run_apply_route_policy_capability_recording_is_best_effort(adapter_client):
    """If capability recording itself raises, the apply still fails cleanly (swallowed)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-err", 324)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-X", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = _nso_client()
    nso_err = NsoApplyError(code="nso_error", message="boom", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            side_effect=RuntimeError("capability backend down"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"]["apply_failed"] == 1


async def test_run_apply_route_policy_capability_skips_record_when_unparseable(adapter_client):
    """When the rejected construct can't be parsed (no name), no capability row is recorded."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-skip", 325)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-Y", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = _nso_client()
    nso_err = NsoApplyError(code="nso_error", message="opaque error", detail={})
    rec = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            return_value={"ned_id": "cisco-ios-cli-6.95", "sw_version": "15.5"},
        ),
        patch("nso_adapter.core.capability.parse_rejected_construct", return_value=(None, None)),
        patch("nso_adapter.core.capability.record_capability_rejection", new=rec),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    rec.assert_not_awaited()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


async def test_run_apply_ip_unexpected_exception(adapter_client):
    """A non-NsoApplyError from the IP pass is recorded as 'internal' and fails the job."""
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-boom", 326)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/9")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.6.0.1/24", family="ipv4")

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            _SENDER,
            new_callable=AsyncMock,
            side_effect=RuntimeError("transport exploded"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["ip_count_by_outcome"]["apply_failed"] == 1
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert rows[0].last_apply_error["code"] == "internal"
        assert "transport exploded" not in str(rows[0].last_apply_error), "exception text reached the persisted error"
        assert "transport exploded" not in str(job.error), "exception text reached the persisted failure items"
        assert "RuntimeError" in rows[0].last_apply_error["message"]


# ── one document, one transaction, one commit ─────────────────────────────────


async def _seed_subif_and_ip(device_id: int, iface_name: str = "ae99.999") -> int:
    """Seed a DbInterface + accepted SubinterfaceIntent (device-keyed) + accepted
    InterfaceIpIntent (interface-keyed) — the greenfield subif+IP pair. Returns iface_id."""
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async with session() as db:
        iface = DbInterface(device_id=device_id, netbox_interface_id=999, name=iface_name, kind="logical")
        db.add(iface)
        await db.flush()
        db.add(
            SubinterfaceIntent(
                device_id=device_id,
                interface_name=iface_name,
                parent_interface="ae99",
                dot1q_vlan=999,
                sub_type="subinterface",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="198.18.1.1/24",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
        return iface.id


async def _ip_and_subif_rows(device_id: int):
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async with session() as db:
        subif = (
            (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        ip = (await db.execute(select(InterfaceIpIntent))).scalars().all()
        return subif, ip


async def _seed_snmp_and_static_route(device_id: int) -> None:
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id, label="public", vault_ref="m/p#k", access="RO", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.9.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()
        return


async def test_the_document_carries_the_subif_and_its_address_in_one_commit(adapter_client):
    """A subinterface and the address on it land in ONE transaction, so FASTMAP orders them.

    The greenfield ordering dependency (an address on a unit the same push defines) is what
    the single transaction dissolves; two commits could not express it.
    """
    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    job_id = await _seed_apply_job(device_id)

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    (iface_entry,) = sent_list(sender, "interface", "interface")
    assert iface_entry["interface-name"] == "ae99.999"
    assert iface_entry["ipv4-address"][0]["address"] == "198.18.1.1"
    assert sent_list(sender, "subinterface", "interface")[0]["interface-name"] == "ae99.999"

    subif_rows, ip_rows = await _ip_and_subif_rows(device_id)
    assert all(r.last_apply_at is not None and r.last_apply_error is None for r in subif_rows + ip_rows)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded


async def test_the_document_carries_every_family_the_device_authorized(adapter_client):
    """One PUT, every family: a family the body omits is a family the PUT RETRACTS.

    So the send is never a subset of what the device holds, even when the job only settles
    the families its own generation promoted.
    """
    from nso_adapter.core.projection import section_registry

    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    await _seed_snmp_and_static_route(device_id)
    job_id = await _seed_apply_job(device_id)

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    sender.assert_awaited_once()
    containers = sent_document(sender)
    assert {"subinterface", "interface", "snmp", "static-route"} <= set(containers)
    # Every family the document carries reaches the wire, empty ones included: an absent
    # container owns nothing, which is not the same statement as an empty one.
    registry = section_registry()
    document_families = {registry[section].container for section in ("bgp", "vlan", "isis", "ospf", "logging")}
    assert document_families <= set(containers)
    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded


async def test_attributes_and_addresses_merge_into_one_interface_entry(adapter_client):
    """Attribute + address intent on ONE interface merge into ONE keyed entry.

    They share the interface-name key, so two list items would be a duplicate-key conflict.
    """
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/1", "description", "uplink", SyncState.accepted
    )
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.1/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    gi = [e for e in sent_list(sender, "interface", "interface") if e["interface-name"] == "Gi0/1"]
    assert len(gi) == 1  # ONE merged entry, not two
    assert gi[0]["description"] == "uplink"
    assert gi[0]["ipv4-address"][0]["address"] == "10.0.0.1"


async def test_a_rejected_commit_fails_all_families_with_localized_attribution(adapter_client):
    """A localized refusal fails every row in the rolled-back transaction."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    job_id = await _seed_apply_job(device_id)

    boom = AsyncMock(side_effect=NsoApplyError("nso_put_failed", "static route rejected"))

    async def _fake_localize(*_a, **_k):
        return {"static-route": "static route rejected"}, (None, None)

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, boom),
        patch("nso_adapter.core.apply._localize_document_failure", _fake_localize),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        sr = (await db.execute(select(StaticRouteIntent))).scalars().all()
        sc = (await db.execute(select(SnmpCommunityIntent))).scalars().all()
        assert sr and all(r.last_apply_error is not None for r in sr)  # offender → failed
        assert sc and all(r.last_apply_error is not None and r.last_apply_at is None for r in sc)
        assert (await db.get(Job, job_id)).status == JobStatus.failed


async def test_localisation_empties_one_family_at_a_time_and_records_its_capability(adapter_client):
    """The family whose REMOVAL lets the document compile is the offender, and it is recorded.

    The NED refusing a family it cannot compile is a real capability gap, so the matrix learns
    ``(ned, sw, static_route) = unsupported``; the family that compiled fine gets no row.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, SnmpCommunityIntent, StaticRouteIntent

    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:  # give the device a known (ned, sw) so no probe is needed
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError("nso_put_failed", "static route rejected by NED")
        # the localisation trials: only the document WITHOUT static-route compiles
        if "static-route" in containers:
            raise NsoApplyError("dry_run_rejected", "static route cannot compile on this NED")
        return "delta"

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, _sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "static_route" in by_scope and by_scope["static_route"].status == "unsupported"
        assert "snmp" not in by_scope  # compiled fine → not an offender → no capability row
        # Every transmitted family failed.
        sr = (await db.execute(select(StaticRouteIntent))).scalars().all()
        sc = (await db.execute(select(SnmpCommunityIntent))).scalars().all()
        assert all(r.last_apply_error is not None for r in sr)
        assert all(r.last_apply_error is not None and r.last_apply_at is None for r in sc)
        assert (await db.get(Job, job_id)).status == JobStatus.failed


async def test_a_refusal_that_names_its_family_skips_the_localisation_loop(adapter_client):
    """The aggregate names the offending family in its refusal, so no dry-run loop is needed.

    ``device-intent: refused [family=<container> ...]`` is the ratified refusal shape; reading
    it is exact, where emptying families one at a time is only a fallback.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, SnmpCommunityIntent

    device_id = await _seed_device(name="sw01-named-refusal")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    refusal = "device-intent: refused [family=snmp field=community]: no such construct"
    trials: list[set[str]] = []

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if dry_run:
            trials.append(set(containers))
            return "delta"
        raise NsoApplyError(
            "nso_put_failed",
            "commit rejected",
            detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": refusal}]}}},
        )

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, _sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    assert trials == [], "the refusal named its own family, so no localisation dry-run was needed"
    async with session() as db:
        caps = (await db.execute(select(DeviceCapability))).scalars().all()
        assert {c.scope for c in caps} == {"snmp"}
        comm = (await db.execute(select(SnmpCommunityIntent))).scalars().one()
        assert comm.last_apply_error is not None


async def test_a_rejected_interface_family_is_attributed_to_the_offending_half(adapter_client):
    """H2: a rejection naming the address node records ONLY ``interface_ip``.

    The interface container carries both halves, so coarse recording made the attribute half
    falsely warn that it is unsupported.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, InterfaceIpIntent

    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/1", "description", "uplink", SyncState.accepted
    )
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.1/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    # REAL sample shape (captured live on rg03): the 4xx names the ipv4-address node.
    reject_msg = (
        "invalid value for: prefix-length in /ir:interface-config[ir:device='sw01']"
        "[ir:interface-name='Gi0/1']/ir:ipv4-address[ir:address='10.0.0.1']/ir:prefix-length:"
        ' "99" is out of range.'
    )

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError(
                "nso_put_failed",
                reject_msg,
                detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": reject_msg}]}}},
            )
        if "interface" in containers:
            raise NsoApplyError("dry_run_rejected", reject_msg)
        return "delta"

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, _sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "interface_ip" in by_scope
        assert by_scope["interface_ip"].name == "ipv4-address"  # construct-named, not coarse
        assert "interface_attribute" not in by_scope  # the attribute half no longer falsely warns


async def test_an_unattributable_interface_rejection_records_both_halves(adapter_client):
    """When the rejection names no known construct, the fail-safe records BOTH halves coarse.

    Losing precision, never losing the record.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, InterfaceIpIntent

    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/2", "description", "uplink", SyncState.accepted
    )
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.5/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError("nso_put_failed", "opaque NED failure")
        if "interface" in containers:
            raise NsoApplyError("dry_run_rejected", "opaque NED failure")
        return "delta"

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, _sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "interface_ip" in by_scope and "interface_attribute" in by_scope  # coarse fallback


async def test_a_clean_commit_clears_the_stale_reactive_unsupported(adapter_client):
    """A clean commit proves every family in the document applies, so stale gaps are cleared.

    A probe cannot downgrade an apply-rejection, so without this the gap sticks forever. The
    scope set is the DOCUMENT's families now, not only the ones with rows; a fine-grained
    construct row is still never cleared.
    """
    from nso_adapter.store.models import Device, DeviceCapability

    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    ned = "cisco-ios-cli:cisco-ios"
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = ned, "15.7"
        # stale reactive rejections left by an earlier FAILED apply of these scopes
        db.add_all(
            [
                DeviceCapability(
                    ned_id=ned,
                    sw_version="15.7",
                    scope="snmp",
                    name="snmp",
                    status="unsupported",
                    detail="old error",
                    source="apply",
                ),
                DeviceCapability(
                    ned_id=ned,
                    sw_version="15.7",
                    scope="rm-set",
                    name="set extcommunity color",
                    status="unsupported",
                    detail="fine-grained",
                    source="apply",
                ),
            ]
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, AsyncMock(return_value=None)),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        by_key = {
            (c.scope, c.name): c
            for c in (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == ned))).scalars().all()
        }
        assert ("snmp", "snmp") not in by_key  # a family the commit carried → stale rejection cleared
        assert ("rm-set", "set extcommunity color") in by_key  # fine-grained → never cleared
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded


async def _seed_route_map_intent(device_id, ned_id):
    from nso_adapter.store.models import Device, RoutePolicyObjectIntent

    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = ned_id, ""
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="route_map",
                name="TEST-RM",
                entries=[{"sequence": 10, "action": "permit"}],
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_run_apply_misconfig_device_rejection_records_no_capability(adapter_client):
    """A generic device rejection that no per-scope dry-run localises — e.g. a route-map
    referencing a prefix-list not included in the push (a MISCONFIGURATION, not a NED limit) —
    must NOT record capability; that would be a false 'unsupported' verdict. The job still fails
    and last_apply_error carries the real device error. (The live IOS-XR-route-map→Junos case:
    'prefix-list referenced but not defined'.)"""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import DeviceCapability, RoutePolicyObjectIntent

    device_id = await _seed_device(name="sw01")
    await _seed_route_map_intent(device_id, "juniper-junos-nc-4.19:junos")
    job_id = await _seed_apply_job(device_id)

    device_err = "RPC error towards sw01: Policy error: PL-X prefix-list referenced (in term 10) but not defined"

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError(
                "nso_patch_failed",
                "NSO combined PATCH failed with status 400",
                detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": device_err}]}}},
            )
        return "rendered-delta"  # route-policy renders clean in dry-run → not localised

    mock_client = _nso_client()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch(_SENDER, _sender))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # NOT a capability gap
        rp = (await db.execute(select(RoutePolicyObjectIntent))).scalars().all()
        assert all(r.last_apply_error is not None for r in rp)  # but the apply did fail + recorded the error
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_transient_failure_records_no_capability(adapter_client):
    """A transport/internal failure (no device rejection) records NO capability — no false verdict."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import DeviceCapability

    device_id = await _seed_device(name="sw01")
    await _seed_route_map_intent(device_id, "juniper-junos-nc-4.19:junos")
    job_id = await _seed_apply_job(device_id)

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError("internal", "connection timed out")  # transport — no nso_error
        return "rendered-delta"

    mock_client = _nso_client()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch(_SENDER, _sender))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # nothing recorded
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_transient_during_localize_records_no_capability(adapter_client):
    """A transient transport error DURING per-scope localisation must NOT brand the scope
    'unsupported' — only a conclusive rejection is a capability signal (finding #10)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability

    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    device_err = "RPC error: something rejected"

    async def _sender(client, device_name, containers, *, dry_run=False, no_networking=False, strict=False):
        if not dry_run:
            raise NsoApplyError(
                "nso_patch_failed",
                "rejected",
                detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": device_err}]}}},
            )
        raise ConnectionError("transient blip during localisation")  # transport, not a conclusive reject

    mock_client = _nso_client()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch(_SENDER, _sender))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # no false 'unsupported'
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_marks_failed_even_when_session_poisoned(adapter_client, monkeypatch):
    """run_apply's failure handler must rollback the poisoned session before committing the
    failed-status, or the status commit itself throws and the job is stuck 'running' (#11)."""
    device_id = await _seed_device(name="rtr-poison")
    job_id = await _seed_apply_job(device_id)
    poison_reached = False

    async def _poison(db, job, job_id, device_id, force, *, reg):
        nonlocal poison_reached
        poison_reached = True
        # A real DB error (duplicate PK) puts the AsyncSession into a needs-rollback state,
        # exactly like a failed flush mid-apply; the failure handler must rollback first.
        db.add(
            Job(
                id=job_id,
                job_type=JobType.apply,
                device_id=device_id,
                status=JobStatus.queued,
                coalescible=True,
            )
        )
        await db.flush()  # IntegrityError → session poisoned; propagates to run_apply's handler

    with patch("nso_adapter.core.apply._execute_apply", _poison):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    assert poison_reached, "the test double must reach the real database error"
    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_a_body_build_failure_reverts_deploying(adapter_client):
    """A body-builder raising (a malformed address) must revert the attrs just marked deploying.

    Otherwise they are stuck deploying forever, since nothing else re-reads them (#12).
    """
    from nso_adapter.store.models import InterfaceAttrState

    device_id = await _seed_device(name="sw01")
    iface_id, attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/0", "description", "uplink", SyncState.accepted
    )
    await _seed_ip_intent(iface_id, address="10.0.0.1", accepted=True)  # malformed: no /prefix → build raises
    job_id = await _seed_apply_job(device_id)

    mock_client = _nso_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        attr_state = await db.get(InterfaceAttrState, attr_id)
        assert attr_state.sync_state != SyncState.deploying  # reverted, not stuck deploying
        assert attr_state.sync_state == SyncState.accepted


def test_capability_scopes_for_interface_config_covers_attribute_and_ip():
    """The interface container carries BOTH the attribute and IP scopes, so a rejection of it
    records capability under both — else a preflight for interface_attribute sees a false
    'fully supported' (#17). Every mapping comes off the registry."""
    from nso_adapter.core.apply import _capability_scopes_for

    assert _capability_scopes_for("interface") == ["interface_attribute", "interface_ip"]
    assert _capability_scopes_for("snmp") == ["snmp"]
    assert _capability_scopes_for("no-such-family") == []


async def test_an_apply_is_blocked_when_the_document_would_flush_a_live_orphan(adapter_client):
    """The collateral guard runs on an APPLY too, because one PUT makes every omission a retraction.

    A live VLAN the document neither renders nor is authorized to drop would be silently
    flushed off the device by an ordinary apply, which is the incident the guard exists for.
    """
    from nso_adapter.nso.client import ServiceInstanceState
    from nso_adapter.store.models import VlanIntent

    device_id = await _seed_device(name="sw01-collateral")
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=10, accepted_at=datetime.now(UTC)))
        await db.commit()

    live = {
        "device": "sw01-collateral",
        "vlan": {"vlan": [{"vlan-id": 10}, {"vlan-id": 999}]},  # 999 is nobody's intent
        "static-route": {"route": []},
    }
    client = _nso_client()
    client.service_instance_state = AsyncMock(return_value=ServiceInstanceState("present", live))

    sender = AsyncMock(return_value=None)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch(_SENDER, sender),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    committed = [call for call in sender.await_args_list if not call.kwargs.get("dry_run")]
    assert committed == [], "a blocked document must not be committed"
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        row = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalar_one()
        assert row.last_apply_error["code"] == "removal_blocked_collateral"
        assert row.last_apply_error["detail"]["orphans"] == {"vlan/vlan": [["999"]]}


async def test_a_failed_commit_marks_both_the_subif_and_its_address(adapter_client):
    """The commit is all-or-nothing: every subif AND address row records the error.

    They rolled back together, so a half-applied report would be a lie.
    """
    from nso_adapter.nso.apply import NsoApplyError

    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    job_id = await _seed_apply_job(device_id)

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_nso_client()),
        patch(_SENDER, AsyncMock(side_effect=NsoApplyError("nso_put_failed", "device said no"))),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    subif_rows, ip_rows = await _ip_and_subif_rows(device_id)
    assert all(r.last_apply_error is not None and r.last_apply_at is None for r in subif_rows + ip_rows)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


# ── #108: post-apply reader-compare (the #26 silent-drop class, caught in seconds) ──
#
# _verify_native_or_raise re-diffs the committed payload against the CDB SERVICE tree —
# both sides sit behind the same FASTMAP writer, so a writer that silently drops an
# object stays invisible (#26, proven live on rg03). The reader-compare closes that
# hole from the other side: after a scope's batch commit reports success, re-read the
# scope's device-state ACTION section (READSEM 1328) and require every intended key to
# be present. A missing key marks those rows apply_failed (retryable) and fails the JOB,
# so the plugin settles deploying→apply_failed on the immediate post-apply reconcile
# instead of waiting out stuck_deploying_grace_minutes.

# reader-compare reads the device-state-read ACTION now — a test seeds the post-commit
# device view as the action's certified output: {atomic, device-name, <wire>: <section>}.
# A method-level mock bypasses NsoClient certification (exercised end-to-end in
# tests/nso/test_device_state_client.py); the section still carries a terminal status.
_RC_WIRE = {
    "static_route": "static-route",
    "snmp": "snmp-config",
    "route_policy": "route-policy",
    "bgp": "bgp-config",
    "isis": "isis-interface",
}


def _rc_action(device_name: str, scope: str, section: dict) -> dict:
    """A certified device-state-read output carrying one scope's post-commit section."""
    return {"atomic": True, "device-name": device_name, _RC_WIRE[scope]: section}


async def test_run_apply_reader_compare_flags_silent_drop(adapter_client):
    """The #26 scenario: the static-route writer 'succeeds' but the device view never
    gains the route → the job FAILS and the row carries reader_compare_missing."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-drop", 401)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.26.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-drop", "static_route", {"status": "ok", "route": []}
    )  # commit "ok", key never landed
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.result["reader_compare"]["static_route"] == "missing"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error["code"] == "reader_compare_missing"
        assert "198.18.26.0/24" in row.last_apply_error["message"]


async def test_run_apply_reader_compare_ok_when_key_lands(adapter_client):
    """A landed key keeps the scope green and records reader_compare=ok."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-ok", 402)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.27.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-ok",
        "static_route",
        {"status": "ok", "route": [{"vrf": "", "prefix": "198.18.27.0/24", "next-hop": "10.0.0.1"}]},
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "ok"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None
        assert row.last_apply_at is not None


from tests.conftest import SNMP_COMMUNITY, SNMP_VAULT_REF, community_export_name  # noqa: E402


def _community_export_name(secret: str) -> str:
    """The export's community key: sha256(community string)[:16] — never the intent label."""
    import hashlib

    return hashlib.sha256(secret.encode()).hexdigest()[:16]


async def test_run_apply_reader_compare_does_not_fail_a_landed_community(adapter_client):
    """A community that DID land must not be re-flagged as a silent writer drop.

    The check keyed SnmpCommunityIntent by `label`, but network-state-export keys a
    community by sha256(community-string)[:16] — and the adapter never sees that string
    (it pushes a Vault triple; NSO resolves the secret). The sets could never intersect, so
    EVERY successful SNMP apply was stamped reader_compare_missing and failed, forever.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await _seed_device("rtr-rc-snmp", 404)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="prod-ro",
                vault_ref="network/netbox/snmp/community/prod-ro#community",
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    # The community IS on the device — under its hashed export identity.
    mock_client.get_device_state_section.return_value = {
        "status": "ok",
        "community": [{"name": _community_export_name("s3cr3t"), "access": "ro"}],
        "v3-user": [],
        "host": [],
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_still_catches_a_dropped_snmp_host(adapter_client):
    """Dropping the un-keyable community grain must not blunt the check: the
    address-keyed host and username-keyed v3-user are still verified."""
    from nso_adapter.store.models import SnmpHostIntent

    device_id = await _seed_device("rtr-rc-snmp-host", 405)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-snmp-host", "snmp", {"status": "ok", "community": [], "v3-user": [], "host": []}
    )  # never landed
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["snmp"] == "missing"
        row = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert row.last_apply_error["code"] == "reader_compare_missing"


async def test_run_apply_reader_compare_absent_reader_surface_is_not_a_drop(adapter_client):
    """A section the NED does not export (status=unsupported) means "unknown", not "the
    writer dropped everything".

    The device-state action declares status=unsupported for a family the NED has no export
    surface for — absence there proves nothing, so the scope stays "unknown" and green. The
    legacy None→{} coercion classified every intended key a silent writer drop and pinned the
    scope permanently apply_failed on a device where NSO had committed the intent; the
    envelope's status closes that blind spot.
    """
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-none", 406)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.29.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-none", "static_route", {"status": "unsupported"}
    )  # no export surface on this NED
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "unknown"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_empty_list_payload_is_still_a_drop(adapter_client):
    """A reader that DOES answer, with the scope's list empty, is a real silent drop —
    the export surface exists and reports nothing there. Must still fail."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-empty", 407)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.30.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-empty", "static_route", {"status": "ok", "route": []}
    )  # answered — and the route is NOT there
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["static_route"] == "missing"


async def _seed_nokia_device(name: str, netbox_id: int) -> int:
    """A device whose NED cannot hold every canonical community member (SR OS)."""
    async with session() as db:
        d = Device(
            nso_instance="nso-dev",
            nso_device_name=name,
            netbox_device_id=netbox_id,
            ned_id="timos-nc-23.10",
        )
        db.add(d)
        await db.flush()
        await create_counter(db, d.id)
        await db.commit()
        await db.refresh(d)
        return d.id


async def test_run_apply_reader_compare_skips_a_fully_unrepresentable_community_list(adapter_client):
    """An object the writer DELIBERATELY could not render must not be called a silent drop.

    apply_route_policy_config skips community members the NED cannot hold (`bandwidth:` has
    no SR OS policy keyword), so a community-list whose members are ALL unrepresentable
    emits {"name": …, "entry": []} — which has no renderable CLI form, never lands, and so
    never appears in the export. The PUT already reports these to the plugin via
    `unsupported_members` so it can mark them "unsupported on <ned>". Demanding the object
    be present anyway turned a known, deliberately-tolerated codec skip into a hard,
    permanently-recurring apply failure for the whole route_policy scope.
    """
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_nokia_device("rtr-rp-unsup", 408)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="community_list",
                name="CL-COLOR-ONLY",
                entries=[{"community": "bandwidth:64500:100"}],  # unrepresentable on SR OS
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    # The export answers — the object legitimately is not there, because nothing was rendered.
    mock_client.get_device_state_section.return_value = {
        "status": "ok",
        "community-list": [],
        "prefix-list": [],
        "route-map": [],
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["route_policy_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        row = (
            (await db.execute(select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_still_fails_a_representable_community_list(adapter_client):
    """A community-list the NED CAN hold, that did not land, is still a real silent drop."""
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_nokia_device("rtr-rp-real", 409)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="community_list",
                name="CL-STD",
                entries=[{"community": "64500:100"}],  # plain asn:val — SR OS takes it verbatim
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    # reader-compare reads the device-state ACTION; device-name is echoed by the real action but
    # ignored on this method mock (cert is exercised in test_device_state_client.py).
    mock_client.run_device_state_read.return_value = {
        "atomic": True,
        "device-name": None,
        "route-policy": {"status": "ok", "community-list": [], "prefix-list": [], "route-map": []},
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["route_policy"] == "missing"


async def test_run_apply_reader_compare_reader_error_is_nonfatal(adapter_client):
    """The check must never fail a good apply: a reader exception records 'error' and
    leaves the scope green (transparency without false alarms)."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-err", 403)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.28.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.side_effect = RuntimeError("reader down")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "error"


async def test_run_apply_reader_compare_bgp_checks_router_and_peers(adapter_client):
    """A BgpRouterIntent expands to its router asn AND every scope peer address — the
    reader nests router→scope→peer like the service. A dropped peer flags the row."""
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    device_id = await _seed_device("rtr-rc-bgp", 404)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        router = BgpRouterIntent(device_id=device_id, asn="65100", accepted_at=datetime.now(UTC))
        db.add(router)
        await db.flush()
        scope = BgpScopeIntent(router_id=router.id, vrf="")
        db.add(scope)
        await db.flush()
        db.add(BgpPeerIntent(scope_id=scope.id, peer_address="10.0.0.7"))
        db.add(BgpPeerIntent(scope_id=scope.id, peer_address="10.0.0.9"))
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-bgp",
        "bgp",
        {"status": "ok", "router": [{"asn": 65100, "scope": [{"vrf": "", "peer": [{"peer-address": "10.0.0.7"}]}]}]},
    )  # 10.0.0.9 silently dropped
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["bgp"] == "missing"
        assert job.result["bgp_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        router = (
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().one()
        )
        assert router.last_apply_error["code"] == "reader_compare_missing"
        assert "10.0.0.9" in router.last_apply_error["message"]


async def test_run_apply_reader_compare_isis_flags_only_missing_model(adapter_client):
    """isis mixes interface and process rows in one scope — only the model whose key
    is absent (here the process) is flagged; the landed interface row stays green."""
    from nso_adapter.store.models import IsisInterfaceIntent, IsisProcessIntent

    device_id = await _seed_device("rtr-rc-isis", 405)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            IsisInterfaceIntent(
                device_id=device_id, interface_name="ge-0/0/0", af="ipv4", accepted_at=datetime.now(UTC)
            )
        )
        db.add(IsisProcessIntent(device_id=device_id, process_tag="CORE", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-isis",
        "isis",
        {
            "status": "ok",
            "interface": [{"interface-name": "ge-0/0/0", "af": "ipv4"}],
            "process": [],  # process silently dropped
        },
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["isis"] == "missing"
        assert job.result["isis_count_by_outcome"] == {"in_sync": 1, "apply_failed": 1}
        iface_row = (
            (await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        proc_row = (
            (await db.execute(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert iface_row.last_apply_error is None
        assert proc_row.last_apply_error["code"] == "reader_compare_missing"


# ── CR-A17: a community that never LANDED is a silent drop like any other ─────────────────────
#
# The post-apply reader-compare exists to catch the #26 class: the commit reports success and the
# key never reaches the device. It covered every scope but the SNMP community — whose intent key is
# a label and whose export key is sha256(community-string)[:16], a digest of a secret the adapter
# never sees. So the row was simply left out of the check, and the one scope where a silent drop
# means a MISSING CREDENTIAL (monitoring goes blind, and nobody finds out until it matters) was the
# one scope the drop-detector did not cover.
#
# The adapter holds the vault_ref, so it can compute that digest itself.


async def _seed_community(device_id: int, *, label="prod-ro", vault_ref=SNMP_VAULT_REF) -> None:
    from nso_adapter.store.models import SnmpCommunityIntent

    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label=label,
                vault_ref=vault_ref,
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()


async def _apply_snmp(device_id: int, job_id: int, snmp_view: dict) -> Job:
    mock_client = _nso_client()
    section = {"status": "ok", **snmp_view}

    async def _read(device_name, families, *, timeout=None):
        # reader-compare reads the device-state ACTION; echo the requested device (as the real
        # action does) so the shape is faithful — cert itself is covered in test_device_state_client.py.
        return {"atomic": True, "device-name": device_name, "snmp-config": section}

    mock_client.run_device_state_read.side_effect = _read
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)
    async with session() as db:
        return await db.get(Job, job_id)


async def test_a_community_the_writer_SILENTLY_DROPPED_is_now_caught(adapter_client, vault):
    """The commit said success. The device has no such community. Monitoring is blind and NetBox
    says in_sync. This is exactly the #26 class the check was built for — it just could not see
    into this grain until it could resolve the secret.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    vault()
    device_id = await _seed_device("rtr-a17-drop", 431)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": []})

    assert job.status == JobStatus.failed
    assert job.result["reader_compare"]["snmp"] == "missing"
    async with session() as db:
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error["code"] == "reader_compare_missing"


async def test_a_community_that_DID_land_is_verified_green_not_merely_skipped(adapter_client, vault):
    """It used to pass this case by not looking. Now it looks, resolves the secret, matches the
    digest the device reports, and says ok — the difference between "we checked" and "we didn't".
    """
    vault()
    device_id = await _seed_device("rtr-a17-ok", 432)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(
        device_id,
        job_id,
        {"community": [{"name": community_export_name(SNMP_COMMUNITY), "access": "ro"}], "v3-user": [], "host": []},
    )

    assert job.status == JobStatus.succeeded
    assert job.result["reader_compare"]["snmp"] == "ok"
    assert job.result["snmp_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}


async def test_VAULT_DOWN_must_not_stamp_a_landed_community_apply_failed(adapter_client, vault):
    """Fail OPEN, and this is why it matters here more than on the removal side.

    Stamping `reader_compare_missing` because VAULT was unreachable would fail the apply, flip the
    row to apply_failed and pin the plugin's SNMP scope red — for a community sitting on the device
    exactly as intended. A check that cannot run must abstain, not accuse.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    vault(fail=True)
    device_id = await _seed_device("rtr-a17-vaultdown", 433)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(
        device_id,
        job_id,
        {"community": [{"name": community_export_name(SNMP_COMMUNITY)}], "v3-user": [], "host": []},
    )

    assert job.status == JobStatus.succeeded
    async with session() as db:
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None, "a Vault outage must never accuse the WRITER of dropping"


_LEAKY_REF = "placeholder-mount/placeholder-path#placeholder-key"
_LEAKY_COMPONENTS = [_LEAKY_REF, "placeholder-mount", "placeholder-path", "placeholder-key", "placeholder-secret"]


async def test_a_VAULT_OUTAGE_puts_no_part_of_the_REFERENCE_in_the_apply_logs(adapter_client):
    """The same sink on the apply side, where every verified apply reads Vault.

    The reference names a Vault mount, path and key, and the provider's exception can repeat the
    request and the payload. Only the community LABEL and the failure TYPE may be logged.
    """
    from structlog.testing import capture_logs

    from nso_adapter.core import snmp_verify
    from tests._secret_discipline import EchoingVault, assert_records_free_of

    provider = EchoingVault(_LEAKY_REF, "placeholder-secret")
    snmp_verify.register_secrets_provider(provider)
    try:
        device_id = await _seed_device("rtr-a17-vaultleak", 437)
        job_id = await _seed_apply_job(device_id)
        await _seed_community(device_id, vault_ref=_LEAKY_REF)
        with capture_logs() as logs:
            job = await _apply_snmp(
                device_id,
                job_id,
                {"community": [{"name": community_export_name(SNMP_COMMUNITY)}], "v3-user": [], "host": []},
            )
    finally:
        snmp_verify.register_secrets_provider(None)

    assert provider.reads == 1, "the outage was never reached"
    assert job.status == JobStatus.succeeded  # still fails open
    failed = [record for record in logs if record["event"] == "snmp_verify.vault_read_failed"]
    assert failed, "the failed read was not reported at all"
    assert failed[0]["label"] == "prod-ro", "the label is the half the operator needs"
    assert_records_free_of(logs, _LEAKY_COMPONENTS)
    assert failed[0]["exception_type"] == "RuntimeError"


async def test_a_dropped_HOST_is_still_caught_when_the_community_grain_goes_dark(adapter_client, vault):
    """One grain being unverifiable must not blunt the others — the address-keyed host still fails."""
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # community unverifiable
    device_id = await _seed_device("rtr-a17-mixed", 434)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": []})

    assert job.result["reader_compare"]["snmp"] == "missing"
    async with session() as db:
        host = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert host.last_apply_error["code"] == "reader_compare_missing"


# ── READSEM 1328 — the reader-compare/action-migration behaviours ─────────────────────────────


async def test_mixed_community_and_host_vault_down_is_PARTIAL_not_ok(adapter_client, vault):
    """r3-M2 (a PRE-EXISTING false-green fixed here): a Vault-unverifiable community alongside a
    host that DID land must NOT report a clean 'ok' after checking only the host. It reports
    'partial' and names the unchecked community — symmetric with the residue path's 'partial'.
    'missing' still beats 'partial', so this only fires when nothing checkable is actually absent.
    """
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # the community grain goes dark
    device_id = await _seed_device("rtr-a17-partial", 435)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    # the host IS on the device; the community cannot be re-keyed (Vault down)
    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": [{"address": "198.18.5.9"}]})

    assert job.status == JobStatus.succeeded  # partial is not a failure
    assert job.result["reader_compare"]["snmp"] == "partial"
    assert job.result["reader_compare_unverifiable"]["snmp"], "the unchecked community must be named"
    async with session() as db:
        host = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert host.last_apply_error is None  # the host landed and was verified


async def test_all_unverifiable_scope_runs_NO_action_and_is_unknown(adapter_client, vault):
    """r2-m3: a scope whose every expected key is Vault-unverifiable (a lone community, Vault down)
    must record 'unknown' WITHOUT ever running the (heavy) device-state action — there is nothing
    to look for on the device."""
    vault(fail=True)
    device_id = await _seed_device("rtr-a17-allunver", 436)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    mock_client = _nso_client()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.run_device_state_read.assert_not_awaited()  # never ran the action
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["reader_compare"]["snmp"] == "unknown"


async def test_reader_compare_batches_ONE_action_for_every_family(adapter_client):
    """One commit is ONE post-commit point, so the presence check runs ONE batched action.

    Each family is still classified independently: a landed route stays ok while a dropped
    host fails.
    """
    from nso_adapter.store.models import SnmpHostIntent, StaticRouteIntent

    device_id = await _seed_device(name="sw01-atomic-rc")
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.9.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="x",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    calls: list[list[str]] = []

    async def _read(device_name, families, *, timeout=None):
        calls.append(sorted(families))
        return {
            "atomic": True,
            "device-name": device_name,
            "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.9.9.0/24", "next-hop": "1.1.1.1"}]},
            "snmp-config": {"status": "ok", "community": [], "v3-user": [], "host": []},  # host dropped
        }

    mock_client = _nso_client()
    mock_client.run_device_state_read.side_effect = _read
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, AsyncMock(return_value=None)),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    assert len(calls) == 1, f"expected exactly one batched action, got {calls}"
    assert set(calls[0]) == {"static-route", "snmp-config"}
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.result["reader_compare"]["static_route"] == "ok"
        assert job.result["reader_compare"]["snmp"] == "missing"  # the host never landed
        assert job.status == JobStatus.failed


async def test_reader_compare_non_terminal_section_is_error(adapter_client):
    """A section carrying a NON-terminal status (a torn 'not-ready' the action should never emit)
    is classified 'error', never walked — the classifier must not treat it as present data. The
    real transport rejects such a response at certification (test_device_state_client.py); here the
    method mock bypasses cert to prove the classifier's own defence."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-notready", 441)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.41.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-notready", "static_route", {"status": "not-ready"}
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # error never fails a good apply
        assert job.result["reader_compare"]["static_route"] == "error"


# ── codex review (READSEM 1328): verifier robustness ──────────────────────────────────────────


async def test_reader_compare_malformed_ok_section_is_error_not_job_crash(adapter_client):
    """codex P2: a terminal 'ok' section whose nested data is malformed (route: [1] — an int where
    a keyed dict belongs) makes the walker raise. That raise must be contained to reader_compare=
    'error', never escape and turn a SUCCESSFUL device commit into an internal job failure."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-malformed", 442)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.42.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-malformed",
        "static_route",
        {"status": "ok", "route": [1]},  # int, not a keyed dict
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # the commit landed — a read-side glitch must not fail it
        assert job.result["reader_compare"]["static_route"] == "error"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None  # never accused of a silent drop


async def test_reader_compare_malformed_batched_section_is_error_not_job_crash(adapter_client):
    """codex P2: the batched classifier is guarded — a malformed section records 'error'.

    A read-side glitch must never turn a successful commit into a job failure.
    """
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device(name="sw01-atomic-malformed")
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.42.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    mock_client = _nso_client()
    mock_client.run_device_state_read.return_value = {
        "atomic": True,
        "device-name": "sw01-atomic-malformed",
        "static-route": {"status": "ok", "route": [1]},  # malformed
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, AsyncMock(return_value=None)),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["reader_compare"]["static_route"] == "error"


async def test_action_failure_preserves_unverifiable_labels(adapter_client, vault):
    """codex P3: when the action RAISES after translation already flagged a Vault-unverifiable
    community (a translatable host kept the scope checkable, so the action did run), the default
    path must still record reader_compare_unverifiable — symmetric with the atomic and residue
    paths — not drop it on the error branch."""
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # the community grain is unverifiable
    device_id = await _seed_device("rtr-a17-actfail", 443)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = _nso_client()
    mock_client.run_device_state_read.side_effect = RuntimeError("action exploded")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(_SENDER, new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # a read error never fails a good apply
        assert job.result["reader_compare"]["snmp"] == "error"
        assert job.result["reader_compare_unverifiable"]["snmp"], "the unverifiable community must survive the error"
