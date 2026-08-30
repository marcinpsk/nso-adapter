# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The deployment-generation protocol, driven end to end (#1558, #1522 §G1/§G2/§G5/§H2/§H4).

One vertical — the VLAN database — carried all the way through: a receipt-admitted intent
PUT over real HTTP, an atomic mutation that cannot commit without its generation, a
complete authorized document, execution of THAT document at the recorded RESTCONF
boundary, and an explicit retry of a blocked head.

Nothing here hand-drives the protocol. The mutations are real requests through the real
app, the runs go through ``worker._claim_next_job`` + ``worker._run_one_job``, and the
assertions are about the bytes that would reach NSO.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_VLAN_ROOT = "vlan-reconciler:vlan-config"
_SNMP_ROOT = "snmp-reconciler:snmp-config"


# ── the recorded RESTCONF boundary ───────────────────────────────────────────


class _Recorder:
    """Every request the apply hands to the RESTCONF pool, in order."""

    def __init__(self, device_name: str, *, fail_vlan: bool = False):
        self.device_name = device_name
        self.fail_vlan = fail_vlan
        self.calls: list[dict] = []

    async def _handle(self, method: str, url: str, content=None, headers=None):
        body = json.loads(content) if content else None
        dry = "dry-run=" in url
        self.calls.append({"method": method, "url": url, "body": body, "dry_run": dry})
        request = httpx.Request(method.upper(), url)
        if dry:
            return httpx.Response(
                200,
                request=request,
                json={"dry-run-result": {"native": {"device": [{"name": self.device_name, "data": ""}]}}},
            )
        if self.fail_vlan and _VLAN_ROOT in (body or {}):
            return httpx.Response(
                400,
                request=request,
                json={"errors": {"error": [{"error-message": "vlan commit rejected"}]}},
            )
        return httpx.Response(204, request=request, text="")

    @property
    def commits(self) -> list[dict]:
        return [c for c in self.calls if not c["dry_run"]]

    def bodies(self, root: str) -> list[dict]:
        return [c["body"] for c in self.commits if root in (c["body"] or {})]

    def vlan_ids(self) -> list[list[int]]:
        """Per real vlan-config commit, the vlan ids the body carried."""
        return [[entry["vlan-id"] for entry in body[_VLAN_ROOT][0]["vlan"]] for body in self.bodies(_VLAN_ROOT)]


def recorded_client(device_name: str, *, on_sync_from=None, fail_vlan: bool = False, device_state: dict | None = None):
    """A spec'd NsoClient whose RESTCONF boundary is recorded.

    *on_sync_from* runs when the apply takes its pre-apply sync — the window between the
    worker committing ``running`` and the runner reading the intent it pushes.

    *device_state* is what the post-commit ``device-state-read`` ACTION reports, keyed by the
    envelope wire name (``{"vlan-database": {"status": "ok", "vlan": [...]}}``). That action
    is the far side of the FASTMAP writer, so it is the only place a SILENTLY DROPPED key can
    be seen. Left out, the action answers nothing and every check classifies ``error``, which
    never fails an apply.
    """
    from nso_adapter.nso.client import NsoClient, ServiceInstanceState

    rec = _Recorder(device_name, fail_vlan=fail_vlan)
    http = AsyncMock()
    for method in ("get", "put", "patch", "post", "delete"):

        def _bind(m=method):
            async def _call(url, content=None, headers=None, **kwargs):
                return await rec._handle(m, url, content, headers)

            return _call

        getattr(http, method).side_effect = _bind()

    client = MagicMock(spec=NsoClient)
    client._base = "http://nso"
    client._action_timeout = 120.0
    client.service_instance_state = AsyncMock(return_value=ServiceInstanceState("absent", None))
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    client.get_service_config = AsyncMock(return_value=None)
    if device_state is None:
        client.run_device_state_read = AsyncMock(return_value={})
    else:

        async def _state(_device_name, wires, **_kwargs):
            return {wire: device_state.get(wire, {"status": "unsupported"}) for wire in wires}

        client.run_device_state_read = AsyncMock(side_effect=_state)

    async def _sync_from(*_args, **_kwargs):
        if on_sync_from is not None:
            await on_sync_from()

    client.sync_from = AsyncMock(side_effect=_sync_from)
    return client, rec


async def test_recorded_client_captures_delete_requests():
    """A reconciler DELETE is observable at the same boundary as every other verb."""
    client, recorder = recorded_client("gen-delete-recorder")

    async with client._client() as http:
        response = await http.delete("http://nso/restconf/data/example:item=1")

    assert response.status_code == 204
    assert [(call["method"], call["url"]) for call in recorder.calls] == [
        ("delete", "http://nso/restconf/data/example:item=1")
    ]


# ── helpers over the real app and the real worker ────────────────────────────


async def seed_settings(device_id: int, *, auto_apply: bool = True) -> None:
    from nso_adapter.store.models import DeviceSettings

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=auto_apply, sync_before_apply=True))
        await db.commit()


async def put_vlans(
    client, device_id: int, vids, *, seq: int | None = None, query: str = "", names=None, accepted=None
):
    """One real VLAN intent push. *accepted* pins a vid's ``accepted_at`` so a re-push can
    leave that row byte-identical — the plugin sends the stamp it holds, and a row whose
    authorization did not move is not a new intent."""
    headers = AUTH | push_seq(seq)
    names = names or {}
    accepted = accepted or {}
    entries = []
    for v in vids:
        entry = {"vlan_id": v, "name": names.get(v, "")}
        if v in accepted:
            entry["accepted_at"] = accepted[v]
        entries.append(entry)
    return await client.put(f"/api/v1/devices/{device_id}/vlan-intent{query}", json={"vlans": entries}, headers=headers)


async def put_snmp(client, device_id: int, labels, *, query: str = ""):
    body = {"communities": [{"label": label, "vault_ref": f"kv/snmp#{label}", "access": "ro"} for label in labels]}
    return await client.put(f"/api/v1/devices/{device_id}/snmp-intent{query}", json=body, headers=AUTH | push_seq())


# The four pushes that exercise the two SHARED document families: two endpoint streams each
# own part of ``interface_config`` and part of ``isis``.


async def put_interface_attrs(client, device_id: int, descriptions: dict[str, str], *, query: str = ""):
    body = {
        "attributes": [
            {"interface": name, "attribute": "description", "intent_value": value}
            for name, value in descriptions.items()
        ]
    }
    return await client.put(f"/api/v1/devices/{device_id}/intent{query}", json=body, headers=AUTH | push_seq())


async def put_ips(client, device_id: int, addresses: dict[str, str], *, query: str = ""):
    body = {
        "addresses": [{"interface": name, "address": address, "family": "ipv4"} for name, address in addresses.items()]
    }
    return await client.put(f"/api/v1/devices/{device_id}/ip-intent{query}", json=body, headers=AUTH | push_seq())


async def put_isis_interfaces(client, device_id: int, names, *, query: str = ""):
    body = {"interfaces": [{"interface_name": name, "af": "ipv4"} for name in names]}
    return await client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent{query}", json=body, headers=AUTH | push_seq()
    )


async def put_isis_flex_algos(client, device_id: int, algo_ids, *, query: str = ""):
    body = {"flex_algos": [{"process_tag": "", "algo_id": algo_id} for algo_id in algo_ids]}
    return await client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent{query}", json=body, headers=AUTH | push_seq()
    )


async def generations(device_id: int) -> list:
    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.select(DeploymentGeneration)
                    .where(DeploymentGeneration.device_id == device_id)
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )


async def stream_row(device_id: int, stream: str):
    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        return await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )


async def run_head(device_id: int, client):
    """Run this device's queued head through the REAL worker. Returns the job id, or None."""
    from nso_adapter.core import worker as worker_mod
    from nso_adapter.core.jobs import _JOB_RUNNERS

    claimed = await worker_mod._claim_next_job()
    if claimed is None:
        return None
    job_id, claimed_device, job_type, reg = claimed
    assert claimed_device == device_id
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
    ):
        await worker_mod._run_one_job(1, job_id, claimed_device, job_type, _JOB_RUNNERS[job_type], reg)
    return job_id


async def job_row(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return await db.get(Job, job_id)


# ── Finding 1 — the runner executes the document its job carries ─────────────


async def test_f1_a_successor_committed_mid_run_never_reaches_the_device(adapter_client):
    """The pushed body is generation 1's document, not whatever the store holds at read time.

    The successor commits in the exact window the worker opens: ``running`` is committed,
    the runner has not yet read the intent it pushes.
    """
    device_id = await seed_device(nso_device_name="gen-doc-exec", netbox_device_id=9801)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    async def successor():
        assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200

    client, rec = recorded_client("gen-doc-exec", on_sync_from=successor)
    job_id = await run_head(device_id, client)
    assert job_id is not None

    assert rec.vlan_ids() == [[10]], "the run deployed the successor's state under generation 1's identity"


async def test_f1_b_a_tampered_document_is_refused_rather_than_executed(adapter_client):
    """The digest is checked before execution: a rewritten document is not deployed.

    The trigger makes this unreachable from SQL, so the rewrite below disables it first —
    standing in for the paths a trigger cannot cover (a restore, a manual repair, a bug that
    bypasses the ORM). The digest is the last line and it has to hold on its own.
    """
    from nso_adapter.store.models import DeploymentGeneration

    device_id = await seed_device(nso_device_name="gen-doc-digest", netbox_device_id=9802)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    async with session() as db:
        generation = await db.scalar(sa.select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id))
        await db.execute(sa.text("ALTER TABLE deployment_generation DISABLE TRIGGER deployment_generation_immutable"))
        await db.execute(
            sa.text("UPDATE deployment_generation SET document = CAST(:doc AS json) WHERE id = :gid").bindparams(
                doc=json.dumps({"vlan": {"vlan_intent": [{"id": 1, "device_id": device_id, "vlan_id": 999}]}}),
                gid=generation.id,
            )
        )
        await db.execute(sa.text("ALTER TABLE deployment_generation ENABLE TRIGGER deployment_generation_immutable"))
        await db.commit()

    client, rec = recorded_client("gen-doc-digest")
    job_id = await run_head(device_id, client)
    assert job_id is not None
    job = await job_row(job_id)

    assert rec.vlan_ids() == [], "a document whose digest no longer matches was executed anyway"
    assert job.status.value == "failed"


# ── Finding 2 — mutation and generation commit together, or not at all ───────


async def test_f2_a_a_write_with_auto_apply_off_still_records_its_revision(adapter_client):
    """``desired_revision`` is what the STORE holds; it is not gated on auto-apply."""
    device_id = await seed_device(nso_device_name="gen-atomic-noauto", netbox_device_id=9803)
    await seed_settings(device_id, auto_apply=False)

    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    row = await stream_row(device_id, "vlan")
    assert row is not None, "an accepted write recorded no desired revision"
    assert row.desired_revision == 1
    assert row.authorized_revision == 0, "a device with auto-apply off promoted nothing"


async def test_f2_b_a_store_only_write_records_its_revision(adapter_client):
    device_id = await seed_device(nso_device_name="gen-atomic-storeonly", netbox_device_id=9804)
    await seed_settings(device_id, auto_apply=False)

    response = await put_vlans(adapter_client, device_id, [10], query="?store_only=true")
    assert response.status_code == 200
    assert response.json() == {"device_id": device_id, "count": 1, "removed": 0, "replaced": False}

    row = await stream_row(device_id, "vlan")
    assert row is not None, "a store-only write recorded no desired revision"
    assert row.desired_revision == 1
    assert row.authorized_revision == 0


async def test_f2_c_a_failed_removal_enqueue_rolls_the_mutation_back(adapter_client):
    """No crash boundary between the shrink and its generation: both land or neither does."""
    from nso_adapter.store.models import VlanIntent

    device_id = await seed_device(nso_device_name="gen-atomic-gap", netbox_device_id=9805)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200

    async def boom(*_args, **_kwargs):
        raise RuntimeError("crash in the gap")

    with patch("nso_adapter.core.removal.enqueue_removal", new=boom):
        resp = await put_vlans(adapter_client, device_id, [10])
    assert resp.status_code == 500, resp.text

    async with session() as db:
        remaining = sorted(
            (await db.execute(sa.select(VlanIntent.vlan_id).where(VlanIntent.device_id == device_id))).scalars().all()
        )
    assert remaining == [10, 20], "the shrink committed without the generation that authorizes it"


# ── Finding 3 — the document is the COMPLETE outbound device document ────────


async def test_f3_the_document_folds_the_last_authorized_state_of_every_section(adapter_client):
    """Authorized A1, store-only A2, then a networked B: B's document carries A1."""
    device_id = await seed_device(nso_device_name="gen-fold", netbox_device_id=9806)
    await seed_settings(device_id)

    assert (await put_snmp(adapter_client, device_id, ["A1"])).status_code == 200
    assert (await put_snmp(adapter_client, device_id, ["A2"], query="?store_only=true")).status_code == 200
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    chain = await generations(device_id)
    latest = chain[-1]
    assert "snmp" in latest.document, "the document omitted a section that is live on the device"
    labels = [row["label"] for row in latest.document["snmp"]["snmp_community_intent"]]
    assert labels == ["A1"], "the document carried an unauthorized store-only repair"
    assert [row["vlan_id"] for row in latest.document["vlan"]["vlan_intent"]] == [10]


# ── #1558 rework 3, finding 1 — a shared family's two lanes authorize separately ──
#
# Sixteen endpoint streams compose fourteen document sections: ``interface_config``/``ip``
# share one, ``isis``/``isis_flex_algo`` share the other. Promoting at SECTION grain made a
# normal push on either lane snapshot and authorize its sibling's un-promoted store-only
# state — the #103 leak the authorized document exists to close, reached sideways.


def _interface_rows(document: dict) -> list[tuple]:
    """The attribute lane's contribution. Absent means the lane authorized nothing."""
    tables = document.get("interface_config", {})
    return [(row["attribute"], row["intent_value"]) for row in tables.get("interface_intent", [])]


def _address_rows(document: dict) -> list[str]:
    tables = document.get("interface_config", {})
    return [row["address"] for row in tables.get("interface_ip_intent", [])]


async def test_f8_a_an_ip_push_does_not_authorize_a_store_only_interface_attribute(adapter_client):
    """Lane A (attributes) store-only, then a normal lane B (addresses) push.

    Both lanes land in the ``interface_config`` document, so one revision counter and one
    snapshot for the pair let B's promotion carry A's unauthorized repair to the device.
    """
    device_id = await seed_device(nso_device_name="gen-share-ip", netbox_device_id=9830)
    await seed_settings(device_id)

    resp = await put_interface_attrs(adapter_client, device_id, {"Gi0/1": "unauthorized"}, query="?store_only=true")
    assert resp.status_code == 200, resp.text
    assert await generations(device_id) == [], "a store-only write promoted"

    assert (await put_ips(adapter_client, device_id, {"Gi0/1": "10.0.0.1/24"})).status_code == 200

    (generation,) = await generations(device_id)
    assert sorted(generation.stream_revisions) == ["ip"], "the address push promoted the attribute lane too"
    assert _address_rows(generation.document) == ["10.0.0.1/24"]
    assert _interface_rows(generation.document) == [], (
        "the address push's document carried the sibling lane's store-only attribute"
    )
    assert (await stream_row(device_id, "interface_config")).authorized_revision == 0


async def test_f8_b_an_interface_attribute_push_does_not_authorize_a_store_only_ip(adapter_client):
    """The same pair in the other direction: store-only addresses, then a normal attribute push."""
    device_id = await seed_device(nso_device_name="gen-share-attr", netbox_device_id=9831)
    await seed_settings(device_id)

    resp = await put_ips(adapter_client, device_id, {"Gi0/1": "192.0.2.9/32"}, query="?store_only=true")
    assert resp.status_code == 200, resp.text
    assert await generations(device_id) == []

    assert (await put_interface_attrs(adapter_client, device_id, {"Gi0/1": "authorized"})).status_code == 200

    (generation,) = await generations(device_id)
    assert sorted(generation.stream_revisions) == ["interface_config"]
    assert _interface_rows(generation.document) == [("description", "authorized")]
    assert _address_rows(generation.document) == [], (
        "the attribute push's document carried the sibling lane's store-only address"
    )
    assert (await stream_row(device_id, "ip")).authorized_revision == 0


async def test_f8_c_an_isis_interface_push_does_not_authorize_a_store_only_flex_algo(adapter_client):
    """The IS-IS pair: store-only flex-algo, then a normal isis-interface push."""
    device_id = await seed_device(nso_device_name="gen-share-isis", netbox_device_id=9832)
    await seed_settings(device_id)

    resp = await put_isis_flex_algos(adapter_client, device_id, [128], query="?store_only=true")
    assert resp.status_code == 200, resp.text
    assert await generations(device_id) == []

    assert (await put_isis_interfaces(adapter_client, device_id, ["Gi0/1"])).status_code == 200

    (generation,) = await generations(device_id)
    assert sorted(generation.stream_revisions) == ["isis"]
    assert [row["interface_name"] for row in generation.document["isis"]["isis_interface_intent"]] == ["Gi0/1"]
    assert generation.document["isis"].get("isis_flex_algo_intent", []) == [], (
        "the isis-interface push's document carried the flex-algo lane's store-only state"
    )
    assert (await stream_row(device_id, "isis_flex_algo")).authorized_revision == 0


async def test_f8_d_a_flex_algo_push_does_not_authorize_a_store_only_isis_interface(adapter_client):
    """The IS-IS pair in the other direction."""
    device_id = await seed_device(nso_device_name="gen-share-flex", netbox_device_id=9833)
    await seed_settings(device_id)

    resp = await put_isis_interfaces(adapter_client, device_id, ["Gi0/9"], query="?store_only=true")
    assert resp.status_code == 200, resp.text
    assert await generations(device_id) == []

    assert (await put_isis_flex_algos(adapter_client, device_id, [128])).status_code == 200

    (generation,) = await generations(device_id)
    assert sorted(generation.stream_revisions) == ["isis_flex_algo"]
    assert [row["algo_id"] for row in generation.document["isis"]["isis_flex_algo_intent"]] == [128]
    assert generation.document["isis"].get("isis_interface_intent", []) == [], (
        "the flex-algo push's document carried the isis-interface lane's store-only state"
    )
    assert (await stream_row(device_id, "isis")).authorized_revision == 0


async def test_f8_e_a_force_removal_authorizes_nothing_outside_the_interfaces_it_flushes(adapter_client):
    """The operator's flush re-deploys ALREADY-AUTHORIZED state; it authorizes none of its own.

    The scope is per-interface — the job sends exactly the named instances — while a
    section-wide promotion covers every stream of the family, for every interface. So a
    store-only repair on Gi0/2's address lane became authorized, and then certified applied,
    by a force-removal of Gi0/1 that never sent it.
    """
    device_id = await seed_device(nso_device_name="gen-force-sibling", netbox_device_id=9834)
    await seed_settings(device_id, auto_apply=False)

    assert (await put_interface_attrs(adapter_client, device_id, {"Gi0/1": "managed"})).status_code == 200
    resp = await put_ips(adapter_client, device_id, {"Gi0/2": "198.51.100.2/24"}, query="?store_only=true")
    assert resp.status_code == 200, resp.text
    assert await generations(device_id) == []

    flush = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal",
        json={"scope": "interface_config", "interfaces": ["Gi0/1"]},
        headers=AUTH,
    )
    assert flush.status_code == 202, flush.text

    (generation,) = await generations(device_id)
    assert generation.stream_revisions == {}, "the operator's flush promoted the family's streams"
    sibling = await stream_row(device_id, "ip")
    assert sibling.desired_revision == 1, "the flush recorded a write on the sibling lane"
    assert sibling.authorized_revision == 0, "the flush authorized the sibling lane's store-only repair"

    client, rec = recorded_client("gen-force-sibling")
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert (await job_row(job_id)).status.value == "succeeded"
    # The interface-reconciler is keyed per interface, so the instance key is in the URL.
    assert [c["url"] for c in rec.commits if "Gi0%2F2" in c["url"]] == [], (
        "the flush sent the interface it was never given"
    )
    assert [c["url"] for c in rec.commits if "Gi0%2F1" in c["url"]] != [], "the flush sent nothing at all"

    sibling = await stream_row(device_id, "ip")
    assert sibling.authorized_revision == 0
    assert sibling.applied_revision == 0, "the flush certified a store-only repair it never sent"


# ── Finding 4 — receipt admission ────────────────────────────────────────────


async def test_f4_a_a_replayed_push_returns_the_stored_response_and_promotes_nothing(adapter_client):
    device_id = await seed_device(nso_device_name="gen-replay", netbox_device_id=9807)
    await seed_settings(device_id)

    first = await put_vlans(adapter_client, device_id, [10], seq=7)
    assert first.status_code == 200
    before = await generations(device_id)

    second = await put_vlans(adapter_client, device_id, [10], seq=7)
    assert second.status_code == 200
    assert second.json() == first.json(), "the replay did not return the stored response"

    after = await generations(device_id)
    assert [g.seq for g in after] == [g.seq for g in before], "the replay created a second generation"


async def test_f4_b_the_same_sequence_with_a_different_body_is_refused(adapter_client):
    device_id = await seed_device(nso_device_name="gen-reuse", netbox_device_id=9808)
    await seed_settings(device_id)

    assert (await put_vlans(adapter_client, device_id, [10], seq=7)).status_code == 200
    conflict = await put_vlans(adapter_client, device_id, [10, 20], seq=7)

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "sequence_reuse"
    assert len(await generations(device_id)) == 1
    # A refused delivery leaves nothing behind, not even a desired-revision bump.
    assert (await stream_row(device_id, "vlan")).desired_revision == 1


async def test_f4_c_a_lower_sequence_is_stale(adapter_client):
    device_id = await seed_device(nso_device_name="gen-stale", netbox_device_id=9809)
    await seed_settings(device_id)

    assert (await put_vlans(adapter_client, device_id, [10], seq=9)).status_code == 200
    stale = await put_vlans(adapter_client, device_id, [10, 20], seq=8)

    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "stale"
    assert len(await generations(device_id)) == 1
    assert (await stream_row(device_id, "vlan")).desired_revision == 1


@pytest.mark.parametrize(
    ("case_id", "raw"),
    list(enumerate(["abc", "", " ", "1.5", "0", "-1", str(2**63)])),
)
async def test_f4_d_a_malformed_or_out_of_domain_push_sequence_is_rejected(adapter_client, case_id, raw):
    device_id = await seed_device(nso_device_name=f"gen-badseq-{case_id}", netbox_device_id=None)
    await seed_settings(device_id)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": [{"vlan_id": 10, "name": ""}]},
        headers={**AUTH, "X-Push-Seq": raw},
    )

    assert resp.status_code == 422, f"{raw!r} was silently converted to an unkeyed write"
    assert resp.json()["error"]["code"] == "validation_error"
    assert await stream_row(device_id, "vlan") is None


async def test_f4_d_a_malformed_push_sequence_is_not_echoed(adapter_client):
    raw = "private-token-value"
    device_id = await seed_device(nso_device_name="gen-badseq-redacted", netbox_device_id=None)
    await seed_settings(device_id)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": []},
        headers={**AUTH, "X-Push-Seq": raw},
    )

    assert resp.status_code == 422
    assert resp.json() == {
        "error": {
            "code": "validation_error",
            "message": "Request validation failed",
            "detail": {
                "errors": [
                    {
                        "loc": ["header", "X-Push-Seq"],
                        "type": "int_parsing",
                        "msg": "Invalid value",
                    }
                ]
            },
        }
    }


async def test_f4_e_a_receipt_is_only_durable_when_its_mutation_commits(adapter_client):
    """The forbidden outcome: a receipt for an operation that rolled back."""
    from nso_adapter.core.receipt import latest_receipt

    device_id = await seed_device(nso_device_name="gen-receipt-atomic", netbox_device_id=9810)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10, 20], seq=3)).status_code == 200

    async def boom(*_args, **_kwargs):
        raise RuntimeError("crash in the gap")

    with patch("nso_adapter.core.removal.enqueue_removal", new=boom):
        resp = await put_vlans(adapter_client, device_id, [10], seq=4)
    assert resp.status_code == 500, resp.text

    async with session() as db:
        receipt = await latest_receipt(db, device_id, "vlan")
    assert receipt.push_seq == 3, "a rolled-back push left a receipt behind"


# ── Finding 5 — blocked-head retry admission ─────────────────────────────────


async def test_f5_a_a_failed_head_is_retried_with_the_same_document_and_digest(adapter_client):
    from nso_adapter.core.generation import retry_generation
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="gen-retry-failed", netbox_device_id=9811)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    failing, _rec = recorded_client("gen-retry-failed", fail_vlan=True)
    await run_head(device_id, failing)
    (head,) = await generations(device_id)
    assert head.status is GenerationStatus.failed

    assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200
    chain = await generations(device_id)
    assert len(chain) == 2

    async with session() as db:
        job = await retry_generation(db, chain[0].id)
        await db.commit()
    assert job is not None

    good, rec = recorded_client("gen-retry-failed")
    await run_head(device_id, good)

    assert rec.vlan_ids() == [[10]], "the retry did not re-send the blocked head's own document"
    reread = await generations(device_id)
    assert reread[0].status is GenerationStatus.settled
    assert reread[0].digest == chain[0].digest


async def test_f5_b_an_outcome_unknown_head_is_retriable(adapter_client):
    from nso_adapter.core.generation import recover_generations, retry_generation
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus

    device_id = await seed_device(nso_device_name="gen-retry-unknown", netbox_device_id=9812)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    # A process that died mid-run: the job is `running` with nothing running it.
    async with session() as db:
        job = await db.scalar(sa.select(Job).where(Job.device_id == device_id))
        job.status = JobStatus.running
        job.run_attempt = 1
        await db.commit()
    async with session() as db:
        await db.execute(
            sa.update(sa.table("deployment_generation", sa.column("device_id"), sa.column("status")))
            .where(sa.column("device_id") == device_id)
            .values(status="running")
        )
        await db.commit()
    async with session() as db:
        job = await db.scalar(sa.select(Job).where(Job.device_id == device_id))
        job.status = JobStatus.failed
        await db.commit()

    await recover_generations()
    (head,) = await generations(device_id)
    assert head.status is GenerationStatus.outcome_unknown

    async with session() as db:
        retried = await retry_generation(db, head.id)
        await db.commit()
    assert retried is not None

    client, rec = recorded_client("gen-retry-unknown")
    await run_head(device_id, client)
    assert rec.vlan_ids() == [[10]]
    assert (await generations(device_id))[0].status is GenerationStatus.settled


async def test_f5_c_a_detach_head_is_re_admittable(adapter_client):
    """A detach carries its removal context on the GENERATION, so a retry can rebuild its job."""
    from nso_adapter.core.generation import retry_generation
    from nso_adapter.store.models import GenerationMode, GenerationStatus, JobType

    device_id = await seed_device(nso_device_name="gen-retry-detach", netbox_device_id=9813)
    await seed_settings(device_id, auto_apply=False)
    assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200
    # An unmarked shrink: detach, not a networked retraction (#106).
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    chain = await generations(device_id)
    detach = next(g for g in chain if g.mode is GenerationMode.detach)
    assert detach.removal_context.get("detach") is True

    async with session() as db:
        await db.execute(
            sa.text("UPDATE deployment_generation SET status = 'failed', job_id = NULL WHERE id = :gid").bindparams(
                gid=detach.id
            )
        )
        await db.commit()

    async with session() as db:
        job = await retry_generation(db, detach.id)
        await db.commit()

    assert job is not None
    assert job.job_type is JobType.removal
    assert job.context.get("detach") is True
    assert (await generations(device_id))[-1].status is GenerationStatus.pending


# ── Finding 6: every device-writing producer attaches a generation ──────────


async def test_f6_a_manual_apply_creates_a_generation_from_authorized_state(adapter_client):
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode

    device_id = await seed_device(nso_device_name="gen-manual-apply", netbox_device_id=9814)
    await seed_settings(device_id, auto_apply=False)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200
    assert await generations(device_id) == []

    selected_seq = (await stream_row(device_id, "vlan")).source_push_seq
    resp = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"selected": {"vlan": selected_seq}},
        headers=AUTH,
    )
    assert resp.status_code == 202
    job_id = resp.json()["generations"][0]["job_id"]

    chain = await generations(device_id)
    assert len(chain) == 1, "the manual apply trigger enqueued a job with no generation"
    assert chain[0].mode is GenerationMode.networked
    async with session() as db:
        assert await db.scalar(
            sa.select(DeploymentGeneration.job_id).where(DeploymentGeneration.id == chain[0].id)
        ) == (job_id)
    assert [row["vlan_id"] for row in chain[0].document["vlan"]["vlan_intent"]] == [10]


async def test_f6_b_the_tombstone_sweeper_gives_its_job_a_generation(adapter_client):
    from nso_adapter.core.tombstone_sweep import sweep_tombstones
    from nso_adapter.store.models import GenerationMode, StaticRouteTombstone

    device_id = await seed_device(nso_device_name="gen-sweeper", netbox_device_id=9815)
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                vrf="",
                prefix="10.9.0.0/24",
                next_hop="192.0.2.9",
                marking="detach",
                route_id=4001,
            )
        )
        await db.commit()

    assert await sweep_tombstones() == 1

    chain = await generations(device_id)
    assert len(chain) == 1, "the sweeper enqueued a removal job with no generation"
    assert chain[0].mode is GenerationMode.detach
    assert chain[0].job_id is not None


async def test_f6_c_the_reclaimer_reissue_gives_its_job_a_generation(adapter_client):
    from nso_adapter.core.static_route_reclaim import reclaim_succeeded_tombstones
    from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteTombstone

    device_id = await seed_device(nso_device_name="gen-reclaim", netbox_device_id=9816)
    async with session() as db:
        # R1's handoff set: the owning removal job SUCCEEDED without proving anything.
        owner = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.succeeded, coalescible=False)
        db.add(owner)
        await db.flush()
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                vrf="",
                prefix="10.9.1.0/24",
                next_hop="192.0.2.10",
                marking="delete_origin",
                route_id=4002,
                job_id=owner.id,
            )
        )
        await db.commit()

    client, _rec = recorded_client("gen-reclaim")
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        _consumed, reissued = await reclaim_succeeded_tombstones()

    assert reissued == 1
    chain = await generations(device_id)
    assert len(chain) == 1, "the reclaimer reissued a removal job with no generation"
    assert chain[0].job_id is not None


async def test_f1_c_a_removal_replaces_with_its_generations_document(adapter_client):
    """The PUT-replace body is the detach generation's document, not the live store.

    A removal is a full-document write too, so a store-relative body would assert whatever
    the store drifted to after the generation was cut — under this generation's identity.
    """
    from nso_adapter.store.models import VlanIntent

    device_id = await seed_device(nso_device_name="gen-removal-doc", netbox_device_id=9817)
    await seed_settings(device_id, auto_apply=False)
    assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200
    # An unmarked shrink: a detach generation whose document is the post-shrink state.
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    # The store moves on after the generation was cut and before the job runs.
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=99, name="drift", accepted_at=sa.func.now()))
        await db.commit()

    client, rec = recorded_client("gen-removal-doc")
    assert await run_head(device_id, client) is not None

    assert rec.vlan_ids() == [[10]], "the removal asserted the drifted store instead of its own document"


# ── Finding 5 — the two operator exits from the barrier, over real HTTP ──────


async def _block_the_head(client, device_id: int, device_name: str):
    """Push, then fail the run at the device. Returns the now-blocked head."""
    assert (await put_vlans(client, device_id, [10])).status_code == 200
    failing, rec = recorded_client(device_name, fail_vlan=True)
    await run_head(device_id, failing)
    assert rec.bodies(_VLAN_ROOT), "the injected vlan rejection never fired"
    (head,) = await generations(device_id)
    return head


async def test_f5_d_the_retry_endpoint_re_admits_the_blocked_head(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="gen-retry-api", netbox_device_id=9818)
    await seed_settings(device_id)
    head = await _block_the_head(adapter_client, device_id, "gen-retry-api")
    assert head.status is GenerationStatus.failed

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/retry-generation", headers=AUTH)
    assert resp.status_code == 202
    assert resp.json() == {"job_id": (await generations(device_id))[0].job_id}

    good, rec = recorded_client("gen-retry-api")
    await run_head(device_id, good)
    assert rec.vlan_ids() == [[10]]
    assert (await generations(device_id))[0].status is GenerationStatus.settled


async def test_f5_e_the_abandon_endpoint_releases_the_successors(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="gen-abandon-api", netbox_device_id=9819)
    await seed_settings(device_id)
    await _block_the_head(adapter_client, device_id, "gen-abandon-api")
    assert (await put_vlans(adapter_client, device_id, [10, 20])).status_code == 200

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/abandon-generation", headers=AUTH)
    assert resp.status_code == 202

    chain = await generations(device_id)
    assert resp.json() == {"job_id": chain[1].job_id}
    assert chain[0].status is GenerationStatus.abandoned
    client, rec = recorded_client("gen-abandon-api")
    await run_head(device_id, client)
    assert rec.vlan_ids() == [[10, 20]], "the successor did not run once the head was abandoned"


@pytest.mark.parametrize("action", ["retry-generation", "abandon-generation"])
async def test_f5_f_the_barrier_controls_refuse_when_nothing_is_blocked(adapter_client, action):
    device_id = await seed_device(nso_device_name=f"gen-nothing-{action}", netbox_device_id=None)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/{action}", headers=AUTH)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"


@pytest.mark.parametrize("action", ["retry-generation", "abandon-generation"])
async def test_f5_g_an_outcome_unknown_head_answers_both_barrier_exits(adapter_client, action):
    """A blocked head is not only a ``failed`` one, and both exits must accept the other.

    Recovery never watched the run, so it can report ``outcome_unknown`` alone. An endpoint
    that keeps its own narrower idea of "blocked" leaves such a head with no exit at all.
    """
    from nso_adapter.core import worker as worker_mod
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await seed_device(nso_device_name=f"gen-unknown-{action}", netbox_device_id=None)
    await seed_settings(device_id)
    assert (await put_vlans(adapter_client, device_id, [10])).status_code == 200

    # The run starts and nobody observes its end: recovery re-dispositions the orphan.
    claimed = await worker_mod._claim_next_job()
    assert claimed is not None, "the push did not queue an apply"
    job_id, _device, _job_type, reg = claimed
    async with session() as db:
        await terminalize_running(db, job_id, status=JobStatus.failed, expected_attempt=reg.run_attempt)
        await db.commit()
    (head,) = await generations(device_id)
    assert head.status is GenerationStatus.outcome_unknown

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/{action}", headers=AUTH)
    assert resp.status_code == 202, resp.text
    assert set(resp.json()) == {"job_id"}
    if action == "retry-generation":
        assert resp.json()["job_id"] is not None
    else:
        assert resp.json()["job_id"] is None


# ── #1558 rework 2 — the stamp targets a deployment actually carried ──────────


async def vlan_rows(device_id: int) -> dict[int, tuple]:
    """Per vlan id: ``(name, last_apply_at is not None)`` — the intent and its bookkeeping."""
    from nso_adapter.store.models import VlanIntent

    async with session() as db:
        rows = (await db.execute(sa.select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
    return {row.vlan_id: (row.name, row.last_apply_at is not None) for row in rows}


async def test_f4_a_a_successors_changed_row_is_not_stamped_by_the_older_document(adapter_client):
    """A live row matched back by ``id`` alone can be a DIFFERENT intent by the time it is stamped.

    The successor commits in the run's own window and rewrites vlan 10's name. That row's id
    is in the executing document, but its content is not what was deployed — stamping it
    reports the successor's intent as applied by a write that never carried it.
    """
    device_id = await seed_device(nso_device_name="gen-stamp-changed", netbox_device_id=9820)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    first = await put_vlans(
        adapter_client, device_id, [10, 20], names={10: "before", 20: "kept"}, accepted={10: stamp, 20: stamp}
    )
    assert first.status_code == 200

    async def successor():
        # VLAN 20 is re-sent byte for byte; only VLAN 10 is a new intent.
        resp = await put_vlans(
            adapter_client, device_id, [10, 20], names={10: "after", 20: "kept"}, accepted={10: stamp, 20: stamp}
        )
        assert resp.status_code == 200

    client, rec = recorded_client("gen-stamp-changed", on_sync_from=successor)
    assert await run_head(device_id, client) is not None

    assert rec.vlan_ids() == [[10, 20]]
    rows = await vlan_rows(device_id)
    assert rows[10] == ("after", False), "the successor's changed VLAN was stamped by the older document"
    assert rows[20] == ("kept", True), "the unchanged VLAN the document carried was not stamped"


async def test_f5_a_atomic_mode_pushes_the_document_and_stamps_the_live_rows(adapter_client, monkeypatch):
    """The atomic path has the same two halves: the body is the document, the stamps are live.

    ``_run_atomic_apply`` used to be handed the hydrated rows for BOTH, so every stamp landed
    on a transient object no session flushes — the rows stayed pending for ever while the job
    reported success.
    """
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")

    device_id = await seed_device(nso_device_name="gen-atomic-doc", netbox_device_id=9821)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    first = await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    assert first.status_code == 200

    async def successor():
        resp = await put_vlans(
            adapter_client, device_id, [10, 20], names={10: "before"}, accepted={10: stamp, 20: stamp}
        )
        assert resp.status_code == 200

    client, rec = recorded_client("gen-atomic-doc", on_sync_from=successor)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert (await job_row(job_id)).status.value == "succeeded"

    assert rec.vlan_ids() == [[10]], "the atomic commit carried the successor's state"
    rows = await vlan_rows(device_id)
    assert rows[10] == ("before", True), "the atomic path stamped a transient hydrated row"
    assert rows[20] == (None, False), "a row the document never carried was stamped"


async def test_f5_b_atomic_mode_does_not_stamp_a_row_the_successor_changed(adapter_client, monkeypatch):
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")

    device_id = await seed_device(nso_device_name="gen-atomic-changed", netbox_device_id=9822)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    first = await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    assert first.status_code == 200

    async def successor():
        resp = await put_vlans(adapter_client, device_id, [10], names={10: "after"}, accepted={10: stamp})
        assert resp.status_code == 200

    client, rec = recorded_client("gen-atomic-changed", on_sync_from=successor)
    assert await run_head(device_id, client) is not None

    assert rec.vlan_ids() == [[10]]
    assert (await vlan_rows(device_id))[10] == ("after", False)


# ── #1558 rework 3, finding 2 — what is VERIFIED is what was SENT ────────────
#
# The stamp list is what the deployment may write bookkeeping onto; the push list is what it
# actually sent. They differ exactly when a successor rewrote a row in place, and verifying
# the STAMP list then looked for nothing at all — so NSO could silently drop the pushed VLAN,
# the job report success, and the generation settle and release its successors.

#: The device-state section shape the vlan reader-compare walks: an empty list is the device
#: view AFTER a silent writer drop — the commit reported success and the key never landed.
_VLAN_DROPPED = {"vlan-database": {"status": "ok", "vlan": []}}


async def _generation_statuses(device_id: int) -> list[str]:
    return [g.status.value for g in await generations(device_id)]


async def test_f9_a_a_dropped_key_fails_the_job_even_with_nothing_to_stamp(adapter_client):
    """Default path: the successor rewrote every carried row, so the stamp list is empty.

    The deployment still SENT vlan 10. The device view reports it absent, which is the #26
    silent-drop class — the job must fail and the generation must not settle, or the barrier
    releases successors on a deployment that never landed.
    """
    device_id = await seed_device(nso_device_name="gen-verify-sent", netbox_device_id=9840)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    assert (
        await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    ).status_code == 200

    async def successor():
        resp = await put_vlans(adapter_client, device_id, [10], names={10: "after"}, accepted={10: stamp})
        assert resp.status_code == 200

    client, rec = recorded_client("gen-verify-sent", on_sync_from=successor, device_state=_VLAN_DROPPED)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert rec.vlan_ids() == [[10]], "the run did not send generation 1's document"

    job = await job_row(job_id)
    assert job.status.value == "failed", "a silently dropped deployment reported success"
    assert (await _generation_statuses(device_id))[0] == "failed", (
        "a generation whose keys never landed settled and released its successors"
    )


async def test_f9_a_a_rejected_send_fails_the_job_even_with_nothing_to_stamp(adapter_client):
    """Default path: a rejected document still counts what it sent after a successor rewrite."""
    device_id = await seed_device(nso_device_name="gen-rejected-sent", netbox_device_id=9842)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    assert (
        await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    ).status_code == 200

    async def successor():
        resp = await put_vlans(adapter_client, device_id, [10], names={10: "after"}, accepted={10: stamp})
        assert resp.status_code == 200

    client, rec = recorded_client("gen-rejected-sent", on_sync_from=successor, fail_vlan=True)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert rec.vlan_ids() == [[10]], "the rejected send did not carry generation 1's document"

    job = await job_row(job_id)
    assert job.status.value == "failed", "a rejected deployment reported success"
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    assert (await _generation_statuses(device_id))[0] == "failed"


async def test_atomic_rejected_send_fails_the_job_even_with_nothing_to_stamp(adapter_client, monkeypatch):
    """Atomic failure accounting follows the sent document when no live row is stampable."""
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")

    device_id = await seed_device(nso_device_name="gen-atomic-rejected-sent", netbox_device_id=9861)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    assert (
        await put_vlans(
            adapter_client,
            device_id,
            [10],
            seq=98611,
            names={10: "before"},
            accepted={10: stamp},
        )
    ).status_code == 200

    async def successor():
        response = await put_vlans(
            adapter_client,
            device_id,
            [10],
            seq=98612,
            names={10: "after"},
            accepted={10: stamp},
        )
        assert response.status_code == 200

    client, rec = recorded_client("gen-atomic-rejected-sent", on_sync_from=successor, fail_vlan=True)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert rec.vlan_ids() == [[10]]

    job = await job_row(job_id)
    assert job.status.value == "failed", "a rejected atomic document reported success"
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    assert (await _generation_statuses(device_id))[0] == "failed"


async def test_f9_b_atomic_mode_verifies_the_document_it_sent(adapter_client, monkeypatch):
    """The atomic path replaces the staged rows with the stamp rows and verified THOSE."""
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")

    device_id = await seed_device(nso_device_name="gen-verify-atomic", netbox_device_id=9841)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    assert (
        await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    ).status_code == 200

    async def successor():
        resp = await put_vlans(adapter_client, device_id, [10], names={10: "after"}, accepted={10: stamp})
        assert resp.status_code == 200

    client, rec = recorded_client("gen-verify-atomic", on_sync_from=successor, device_state=_VLAN_DROPPED)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert rec.vlan_ids() == [[10]]

    job = await job_row(job_id)
    assert job.status.value == "failed", "a silently dropped atomic deployment reported success"
    assert (await _generation_statuses(device_id))[0] == "failed"


async def test_f9_c_a_present_key_still_settles_and_stamps_the_row_it_carried(adapter_client):
    """The other direction: the key IS on the device, so the deployment is proven and settles.

    Without this the fix could be "always fail when nothing was stamped", which would break
    every apply whose rows a successor legitimately superseded.
    """
    device_id = await seed_device(nso_device_name="gen-verify-present", netbox_device_id=9842)
    await seed_settings(device_id)
    stamp = "2026-08-01T00:00:00Z"
    assert (
        await put_vlans(adapter_client, device_id, [10], names={10: "before"}, accepted={10: stamp})
    ).status_code == 200

    async def successor():
        resp = await put_vlans(adapter_client, device_id, [10], names={10: "after"}, accepted={10: stamp})
        assert resp.status_code == 200

    present = {"vlan-database": {"status": "ok", "vlan": [{"vlan-id": 10}]}}
    client, _rec = recorded_client("gen-verify-present", on_sync_from=successor, device_state=present)
    job_id = await run_head(device_id, client)

    assert (await job_row(job_id)).status.value == "succeeded"
    assert (await _generation_statuses(device_id))[0] == "settled"
    # The successor's row is not stamped by a deployment that never carried its content.
    assert (await vlan_rows(device_id))[10] == ("after", False)
