# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Delivery refusals an operator can correct answer a typed 409 (#1915)."""

from copy import deepcopy

import pytest
from fastapi import Depends
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from nso_adapter.api.candidate_ingestion import CandidateIngestion
from nso_adapter.api.deps import verify_token
from nso_adapter.api.errors import DOMAIN_CONFLICTS, ERROR_CODES, ErrorEnvelope, domain_conflict
from nso_adapter.core.generation import ApplyUnexecutable
from nso_adapter.core.projection import InterfaceProofConflict
from nso_adapter.core.receipt import PromotionProvenanceUnexecutable
from nso_adapter.domain.asn import AsnRuleViolation
from nso_adapter.main import create_app
from nso_adapter.store.db import session
from nso_adapter.store.models import DbInterface
from tests._secret_discipline import assert_keys_absent, assert_text_free_of
from tests.conftest import AUTH, VALID_TOKEN
from tests.core.test_candidate_preflight import BODIES, preview, push, set_auto_apply
from tests.core.test_preflight_loop import authorize, setup
from tests.core.test_preflight_plan import database_state

pytestmark = pytest.mark.anyio


async def moved_parent_binding(api, monkeypatch):
    """Authorize an ip proof for lag-1, then move the stored interface to lag-2."""
    device, _, _, _ = await setup(api, monkeypatch)
    bound = deepcopy(BODIES["ip"])
    bound["addresses"][0]["parent_binding"] = "lag-1"
    assert (await push(api, device.id, "ip", bound, seq=41)).status_code == 200
    await authorize(api, device, {"ip": 41})
    async with session() as db:
        interface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
        interface.parent_binding = "lag-2"
        await db.commit()
    await set_auto_apply(device.id, True)
    return device


async def deliver_interface_config(api, device_id, seq):
    return await api.put(
        f"/api/v1/devices/{device_id}/intent",
        json=BODIES["interface_config"],
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def test_auto_apply_interface_proof_conflict_is_a_typed_409(adapter_client, monkeypatch):
    device = await moved_parent_binding(adapter_client, monkeypatch)
    before = await database_state()
    response = await deliver_interface_config(adapter_client, device.id, 50)
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "interface_proof_conflict"
    assert error["detail"] == {"streams": ["interface_config", "ip"], "field": "parent_binding"}
    assert "parent_binding" in error["message"]
    # The refusal rolls the whole delivery back, so a replay of the same sequence meets the same refusal.
    assert await database_state() == before
    replay = await deliver_interface_config(adapter_client, device.id, 50)
    assert replay.status_code == 409, replay.text
    assert replay.json() == response.json()


#: One instance of every mapped refusal. A new table entry without a sample fails both tests below.
SAMPLES = {
    ApplyUnexecutable: ApplyUnexecutable({"ip": "invalid_stored_address"}),
    AsnRuleViolation: AsnRuleViolation("bgp_router_intent", 7, "asn", "AS-PLACEHOLDER"),
    InterfaceProofConflict: InterfaceProofConflict("ip", "11", "parent_binding", "lag-1", "lag-2"),
    PromotionProvenanceUnexecutable: PromotionProvenanceUnexecutable("vlan"),
}


def test_every_domain_conflict_has_a_sample():
    assert set(SAMPLES) == set(DOMAIN_CONFLICTS)


@pytest.mark.parametrize("kind", SAMPLES, ids=lambda kind: kind.__name__)
async def test_every_domain_conflict_answers_a_typed_409_through_the_application(kind):
    app = create_app()
    app.state.adapter_token = VALID_TOKEN

    @app.get("/_test/domain-conflict", dependencies=[Depends(verify_token)])
    async def _refuse():
        raise SAMPLES[kind]

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/_test/domain-conflict", headers=AUTH)
    assert_text_free_of(response.text, ["AS-PLACEHOLDER"])
    assert response.status_code == 409, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code in ERROR_CODES - {"internal", "conflict"}
    assert response.json() == {"error": domain_conflict(SAMPLES[kind]).body["error"]}


@pytest.mark.parametrize("kind", SAMPLES, ids=lambda kind: kind.__name__)
async def test_every_domain_conflict_is_a_failed_delivery_probe(adapter_client, monkeypatch, kind):
    device, _, _, _ = await setup(adapter_client, monkeypatch)

    async def refuse(self, db):
        raise SAMPLES[kind]

    monkeypatch.setattr(CandidateIngestion, "delivery_effect", refuse)
    before = await database_state()
    response = await preview(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "unavailable", result
    assert result["error"] == {"code": "delivery_probe_failed"}
    assert_keys_absent(result, ["candidate_identity", "plan_digest", "delivery_effect"])
    assert await database_state() == before
