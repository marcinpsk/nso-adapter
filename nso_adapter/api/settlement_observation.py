# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""One coherent, read-only observation of a settlement obligation."""

from uuid import UUID

import anyio.to_thread
from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_read_db, verify_token
from nso_adapter.api.devices import (
    DeploymentEvidenceIn,
    DeviceDeploymentEvidenceOut,
    DeviceGenerationEvidenceOut,
    _device_generation_out,
    deployment_evidence,
)
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.execution import EvidenceEnvelope, evidence_digest, evidence_envelope
from nso_adapter.api.intent_receipts import IntentReceiptOut, _receipt_out
from nso_adapter.api.jobs import JobEvidenceOut, JobIdentityOut, _job_out
from nso_adapter.core.intent_protocol import INTENT_STREAMS
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DeploymentApplyAttempt,
    DeploymentGeneration,
    Device,
    IntentPushReceipt,
    Job,
    StoreMeta,
)

router = APIRouter(prefix="/api/v1/devices", tags=["devices"])


class SettlementObservationIn(DeploymentEvidenceIn):
    model_config = ConfigDict(extra="forbid")

    job_id: int = Field(gt=0, le=2**31 - 1, strict=True)
    after_settle_seq: int = Field(ge=0, le=2**63 - 1, strict=True)


class SettlementDeviceIdentityOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device_id: int
    netbox_device_id: int | None
    nso_instance: str
    nso_device_name: str


class ReceiptIdentityOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device_id: int
    section: str
    push_seq: int


class SettlementReceiptEvidenceOut(EvidenceEnvelope[ReceiptIdentityOut, IntentReceiptOut]):
    pass


class SettlementAbsenceOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: int | None
    apply_attempt_ids: list[UUID]
    receipt_sections: list[str]


class SettlementObservationOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: SettlementObservationIn
    store_incarnation: UUID
    device: SettlementDeviceIdentityOut
    job: JobEvidenceOut | None
    next_feed: JobIdentityOut | None
    generations: list[DeviceGenerationEvidenceOut]
    deployment: DeviceDeploymentEvidenceOut
    receipts: list[SettlementReceiptEvidenceOut]
    absent: SettlementAbsenceOut
    digest: str = Field(pattern="^[0-9a-f]{64}$")


@router.post(
    "/{device_id}/settlement-observation",
    dependencies=[Depends(verify_token)],
    response_model=SettlementObservationOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def settlement_observation(
    device_id: int,
    body: SettlementObservationIn,
    db: AsyncSession = Depends(get_read_db),
):
    """Observe one target and its actual next-feed proof without changing the store.

    The first SELECT acquires the snapshot. All projections use this session until the
    read dependency rolls it back. Missing records carry explicit absence markers.
    """
    body.apply_attempt_ids.sort()
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")
    meta = (await db.execute(select(StoreMeta))).scalar_one()
    job = await db.scalar(select(Job).where(Job.device_id == device_id, Job.id == body.job_id))
    next_feed = (
        (
            await db.execute(
                select(Job.id, Job.device_id, Job.settle_seq)
                .where(Job.device_id == device_id, Job.settle_seq > body.after_settle_seq)
                .order_by(Job.settle_seq)
                .limit(1)
            )
        )
        .mappings()
        .one_or_none()
    )
    deployment = await deployment_evidence(device_id, body, db)
    head = deployment["head"]
    historical_member = (
        select(ApplyExecutionAttempt.id)
        .where(
            ApplyExecutionAttempt.device_id == device_id,
            ApplyExecutionAttempt.job_correlation_id == body.job_id,
            or_(
                ApplyExecutionAttempt.generation_id == DeploymentGeneration.id,
                ApplyExecutionAttempt.generation_authorities.contains(
                    func.jsonb_build_array(func.jsonb_build_object("generation_id", DeploymentGeneration.id))
                ),
            ),
        )
        .correlate(DeploymentGeneration)
        .exists()
    )
    generation_rows = (
        await db.scalars(
            select(DeploymentGeneration)
            .where(
                DeploymentGeneration.device_id == device_id,
                or_(
                    historical_member,
                    DeploymentGeneration.job_id == body.job_id,
                    DeploymentGeneration.carrier_job_id == body.job_id,
                    DeploymentGeneration.apply_attempt_id.in_(body.apply_attempt_ids),
                    DeploymentGeneration.id == (head["identity"]["generation_id"] if head else None),
                ),
            )
            .order_by(DeploymentGeneration.seq)
        )
    ).all()
    receipts = (
        await db.scalars(
            select(IntentPushReceipt)
            .where(IntentPushReceipt.device_id == device_id)
            .order_by(IntentPushReceipt.section)
        )
    ).all()
    sections: set[str] = set()
    for generation in generation_rows:
        for mapping in (generation.source_push_seq, generation.stream_revisions):
            if isinstance(mapping, dict):
                sections.update(INTENT_STREAMS.intersection(mapping))
    selections = await db.scalars(
        select(DeploymentApplyAttempt.selected).where(
            DeploymentApplyAttempt.device_id == device_id,
            DeploymentApplyAttempt.id.in_(body.apply_attempt_ids),
        )
    )
    for selected in selections:
        if isinstance(selected, dict):
            sections.update(INTENT_STREAMS.intersection(selected))

    def build() -> dict:
        observed = {
            "request": body.model_dump(mode="json"),
            "store_incarnation": meta.incarnation,
            "device": {
                "device_id": device.id,
                "netbox_device_id": device.netbox_device_id,
                "nso_instance": device.nso_instance,
                "nso_device_name": device.nso_device_name,
            },
            "job": _job_out(job) if job is not None else None,
            "next_feed": dict(next_feed) if next_feed is not None else None,
            "generations": [_device_generation_out(row) for row in generation_rows],
            "deployment": deployment,
            "receipts": [
                evidence_envelope(
                    {"device_id": row.device_id, "section": row.section, "push_seq": row.push_seq},
                    {},
                    IntentReceiptOut,
                    construct=lambda candidate, row=row: candidate.update(_receipt_out(row)),
                )
                for row in receipts
            ],
            "absent": {
                "job_id": body.job_id if job is None else None,
                "apply_attempt_ids": deployment["unknown_apply_attempt_ids"],
                "receipt_sections": sorted(sections - {row.section for row in receipts}),
            },
        }
        return {**observed, "digest": evidence_digest(observed)}

    # Envelope digests and contract checks are CPU-bound: keep them off the event loop.
    return await anyio.to_thread.run_sync(build)
