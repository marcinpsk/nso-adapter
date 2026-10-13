# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Acknowledged origin history and exact historical joins, without live intent reads."""

from dataclasses import replace
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.apply import encode_section
from nso_adapter.core.projection import section_operation, stream_section
from nso_adapter.core.static_route_plan import CLEAR_WIRE_LEAF
from nso_adapter.core.static_route_reader import CertifiedSection
from nso_adapter.nso.origins import (
    OriginBinding,
    OriginIndex,
    OriginKey,
    RemovalAuthority,
    content_digest,
)
from nso_adapter.nso.refusal_protocol import SUBJECTS, row_ref
from nso_adapter.store.execution_evidence import origin_records, origins_from_records
from nso_adapter.store.models import ApplyExecutionAttempt, DeploymentGeneration, GenerationMode


def _fingerprints(entries: list[dict]) -> dict:
    result = {}
    for entry in entries:
        row = row_ref(
            "static-route",
            "static-route.route",
            (("route", (entry.get("vrf") or "", entry["prefix"], entry.get("next-hop") or "")),),
        )
        if row in result:
            raise ValueError("Certified route identities must be unique")
        result[row] = content_digest(entry)
    return result


async def record_acknowledged_origins(
    db: AsyncSession,
    attempt_id: int,
    job_id: int,
    run_attempt: int,
    claim_token: str,
    certified: CertifiedSection | None = None,
) -> None:
    """Checkpoint safe history after acknowledgement and certify exact route entry bytes."""
    from nso_adapter.store.execution_attempt_store import _owned_attempt

    attempt = await _owned_attempt(db, attempt_id, job_id, run_attempt, claim_token)
    if not attempt.commit_acknowledged or "origin_history" in attempt.response_classification:
        raise ValueError("Origin history requires an unrecorded acknowledged commit")
    generation = await db.get(DeploymentGeneration, attempt.generation_id)
    if generation is None:
        raise ValueError("Acknowledged history requires its frozen generation")
    routes = (
        encode_section(generation.document, "static_route").body.get("route", [])
        if "static_route" in generation.document
        else []
    )
    emitted = _fingerprints(routes)
    observed = {} if certified is None or certified.inconclusive else _fingerprints(certified.routes)
    origins = origins_from_records(attempt.safe_origins)
    history = OriginIndex()
    for key, binding in origins.bindings.items():
        if binding.provenance == "prior":
            continue
        fingerprint = None
        if key.family == "static-route":
            fingerprint = emitted.get(key.row) if binding.provenance == "current" else binding.entry_fingerprint
            if fingerprint is None or fingerprint != observed.get(key.row):
                continue
        history.bindings[key] = replace(binding, baseline_attempt_id=attempt.id, entry_fingerprint=fingerprint)
    attempt.response_classification = {**attempt.response_classification, "origin_history": origin_records(history)}
    await db.flush()


def _authorized_omission(generation: DeploymentGeneration, authority: RemovalAuthority) -> bool:
    if (
        authority.generation_id != generation.id
        or authority.frozen_revision != generation.stream_revisions.get(authority.stream)
        or generation.mode != GenerationMode.networked
    ):
        return False
    operation = section_operation(generation.document, stream_section(authority.stream))
    if authority.stream == "static_route":
        removal = operation.get("removal") or {}
        if authority.operation == "removal":
            return not authority.fields and any(
                row_ref("static-route", "static-route.route", (("route", tuple(key)),)) == authority.row
                for key in removal.get("authorized_removal_keys", [])
            )
        return bool(authority.fields) and any(
            row_ref("static-route", "static-route.route", (("route", tuple(item["key"])),)) == authority.row
            and set(authority.fields)
            <= {f"static-route.{field}" for field in item["fields"] if field in CLEAR_WIRE_LEAF}
            for item in removal.get("candidate_clears", [])
        )
    if (generation.removal_context or {}).get("scope") != stream_section(authority.stream):
        return False
    if authority.operation == "clear":
        if authority.stream in operation.get("immediate_clear_streams", []):
            return True
        ids = operation.get("pending_clear_ids", [])
        return bool(ids) and all(type(identity) is int and identity > 0 for identity in ids)
    subject = SUBJECTS[authority.row.subject]
    if not subject.path or authority.fields:
        return False
    labels = (generation.allowed_removal_keys or {}).get(stream_section(authority.stream), {})
    label = subject.id.rsplit(".", 1)[-1]
    for raw in labels.get(label, []):
        values = tuple(raw) if isinstance(raw, (list, tuple)) else (raw,)
        if len(values) != sum(len(step.keys) for step in subject.path):
            continue
        path, offset = [], 0
        for step in subject.path:
            path.append((step.list, values[offset : offset + len(step.keys)]))
            offset += len(step.keys)
        if row_ref(subject.family, subject.id, tuple(path)) == authority.row:
            return True
    return False


def _bind_prior(
    origins: OriginIndex,
    baseline: OriginIndex,
    family: str,
    row,
    provenance: Literal["retained", "prior"],
    *,
    fingerprint: str | None = None,
    authority: RemovalAuthority | None = None,
) -> None:
    fields = (
        authority.fields
        if authority is not None and authority.operation == "clear" and authority.fields
        else (None, *SUBJECTS[row.subject].fields)
    )
    for field in fields:
        key = OriginKey(family, row, field)
        prior = baseline.bindings.get(key)
        if (
            prior is None
            or (provenance == "retained" and prior.entry_fingerprint != fingerprint)
            or (authority is not None and prior.stream != authority.stream)
        ):
            continue
        # A current emitted field remains owned by its current producer.
        if key in origins.bindings:
            continue
        origins.unsupported.pop(key, None)
        origins.bindings[key] = replace(prior, provenance=provenance, authority=authority)


def _retained_family(origins: OriginIndex) -> None:
    if not origins.retained or any(
        OriginKey("static-route", contribution.row) not in origins.bindings for contribution in origins.retained
    ):
        return
    bindings = [
        binding
        for key, binding in origins.bindings.items()
        if key.family == "static-route" and key.row.subject == "static-route.route" and key.field is None
    ]
    logicals = tuple(dict.fromkeys(logical for binding in bindings for logical in binding.logical_rows))
    sources = tuple(dict.fromkeys(source for binding in bindings for source in binding.sources))
    key = OriginKey("static-route", row_ref("static-route", "static-route", ()))
    origins.unsupported.pop(key, None)
    origins.bindings[key] = OriginBinding("static_route", sources, "static-route", "current", logicals)


async def bind_historical_origins(
    db: AsyncSession,
    device_id: int,
    origins: OriginIndex,
    generation: DeploymentGeneration,
) -> None:
    """Join the latest acknowledged document to certified retention and immutable omissions."""
    frozen = await db.scalar(
        select(DeploymentGeneration)
        .where(DeploymentGeneration.id == generation.id, DeploymentGeneration.device_id == device_id)
        .execution_options(populate_existing=True)
    )
    if frozen is None:
        raise ValueError("Historical attribution requires a persisted device generation")
    attempt = await db.scalar(
        select(ApplyExecutionAttempt)
        .where(ApplyExecutionAttempt.device_id == device_id, ApplyExecutionAttempt.commit_acknowledged.is_(True))
        .order_by(ApplyExecutionAttempt.id.desc())
        .limit(1)
    )
    if attempt is None:
        return
    baseline = origins_from_records(attempt.response_classification.get("origin_history", []))
    for retained in origins.retained:
        _bind_prior(origins, baseline, "static-route", retained.row, "retained", fingerprint=retained.entry_fingerprint)
    _retained_family(origins)
    for omitted in origins.omitted:
        prior = baseline.bindings.get(OriginKey(omitted.family, omitted.row))
        authority = omitted.authority
        covers = authority is not None and (
            authority.row == omitted.row
            or (
                not authority.fields
                and prior is not None
                and bool(prior.logical_rows)
                and all(logical.carrier.root_row == authority.row for logical in prior.logical_rows)
            )
        )
        if authority is not None and covers and _authorized_omission(frozen, authority):
            _bind_prior(origins, baseline, omitted.family, omitted.row, "prior", authority=authority)


async def automatic_removal_reissue_blocked(db: AsyncSession, device_id: int, tombstone_id: int) -> bool:
    """Consult durable tombstone authority even after its original job was pruned."""
    attempts = await db.scalars(
        select(ApplyExecutionAttempt).where(
            ApplyExecutionAttempt.device_id == device_id,
            ApplyExecutionAttempt.removal_tombstone_ids.contains([tombstone_id]),
        )
    )
    for attempt in attempts:
        checkpoints = [value for key, value in attempt.response_classification.items() if key != "origin_history"]
        if any(checkpoint.get("kind") in {"refused", "unavailable", "protocol_fault"} for checkpoint in checkpoints):
            return True
        if not checkpoints or attempt.commit_acknowledged:
            return True
    return False
