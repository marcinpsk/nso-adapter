# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Public projections of frozen execution evidence."""

import hashlib
import json
import math
from datetime import UTC, datetime
from enum import Enum
from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from nso_adapter.api.timestamps import iso_z
from nso_adapter.nso.execution_contract import ContractError, validate_result


class _ClosedOut(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RowRefOut(_ClosedOut):
    subject: str
    key: str


class RelatedSiteOut(_ClosedOut):
    family: str
    row: RowRefOut | None
    field: str | None


class CatalogRefusalOut(_ClosedOut):
    family: str
    row: RowRefOut
    field: str | None
    reason: str
    message_id: str
    message: str
    related: RelatedSiteOut | None


class SettlementOwnerOut(_ClosedOut):
    stream: str
    row: RowRefOut
    source_push_seq: int | None
    frozen_revision: int | None
    accepted_at: str | None
    generation_id: int | None
    settlement_cohort: int | None


class LogicalIdentityOut(_ClosedOut):
    stream: str
    row: RowRefOut
    source_push_seq: int | None
    frozen_revision: int | None
    acceptance_identity: str | None
    generation_id: int | None
    settlement_cohort: int | None
    carrier: SettlementOwnerOut


class RowOutcomeOut(LogicalIdentityOut):
    disposition: Literal["in_sync", "refused", "not_applied", "unavailable"]
    refusals: list[CatalogRefusalOut]


class AttributedRefusalOut(CatalogRefusalOut):
    logical_rows: list[LogicalIdentityOut]
    provenance: Literal["current", "retained", "prior"]
    sender_phase: Literal["pre_commit_dry_run", "commit", "post_commit_verification"]


def evidence_projection(result: dict | None) -> dict:
    return {key: result.get(key) if result is not None else None for key in ("row_outcomes", "refusals")}


class DeploymentEvidenceInvariantError(RuntimeError):
    """The durable Apply response disagrees with its stamped generations."""


class EvidenceErrorOut(_ClosedOut):
    code: Literal["projection_construction", "schema_validation", "execution_contract", "attempt_generation_membership"]
    digest: str = Field(pattern="^[0-9a-f]{64}$")


IdentityT = TypeVar("IdentityT", bound=BaseModel)
ProjectionT = TypeVar("ProjectionT", bound=BaseModel)


# OpenGrep requires the TypeVar form for this model.
class EvidenceEnvelope(_ClosedOut, Generic[IdentityT, ProjectionT]):  # noqa: UP046
    identity: IdentityT
    projection: ProjectionT | None
    evidence_error: EvidenceErrorOut | None
    digest: str = Field(pattern="^[0-9a-f]{64}$")


# These lists represent sets of identified records; other lists retain their order.
_IDENTITY_COLLECTIONS = frozenset(
    {
        "row_outcomes",
        "refusals",
        "settlement_owners",
        "logical_rows",
        "generations",
        "attempts",
        "receipts",
        "generation_ids",
        "apply_attempt_ids",
        "unknown_apply_attempt_ids",
        "receipt_sections",
    }
)


def canonical_evidence(value, *, field: str = ""):
    """Preserve stored evidence, including values that cannot be normalized."""
    if isinstance(value, Enum):
        return canonical_evidence(value.value, field=field)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        try:
            if value.tzinfo is not None and value.utcoffset() is not None:
                return iso_z(value.astimezone(UTC))
        except (ValueError, OverflowError):
            pass
        return {"$datetime": value.isoformat()}
    if isinstance(value, str) and (field.endswith("_at") or field == "acceptance_identity"):
        try:
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if instant.tzinfo is not None and instant.utcoffset() is not None:
                return iso_z(instant.astimezone(UTC))
        except (ValueError, OverflowError):
            return value
    if isinstance(value, float) and not math.isfinite(value):
        return {"$float": str(value)}
    if isinstance(value, dict):
        return {key: canonical_evidence(item, field=key) for key, item in value.items()}
    if isinstance(value, list):
        items = [canonical_evidence(item) for item in value]
        return sorted(items, key=_canonical_json) if field in _IDENTITY_COLLECTIONS else items
    return value


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def evidence_digest(candidate) -> str:
    return hashlib.sha256(_canonical_json(canonical_evidence(candidate)).encode(errors="surrogatepass")).hexdigest()


def evidence_envelope(
    identity: dict, candidate: dict, schema: type[BaseModel], *, construct=None, validate=None, project=None
) -> dict:
    """Contain construction, digest, and validation in the identified item."""
    code = None
    projection = None
    try:
        try:
            if construct is not None:
                construct(candidate)
        finally:
            digest = evidence_digest(candidate)
        projection = schema.model_validate_json(
            json.dumps(candidate, default=canonical_evidence, allow_nan=False), strict=True
        ).model_dump(mode="json")
        if validate is not None:
            validate(projection)
        if project is not None:
            projection = project(projection)
    except ValidationError:
        code = "schema_validation"
    except ContractError:
        code = "execution_contract"
    except DeploymentEvidenceInvariantError:
        code = "attempt_generation_membership"
    except (AttributeError, ValueError, TypeError, KeyError, OverflowError):
        code = "projection_construction"
    return {
        "identity": identity,
        "projection": projection if code is None else None,
        "evidence_error": {"code": code, "digest": digest} if code is not None else None,
        "digest": digest,
    }


def validate_execution_result(result, *, required: bool) -> None:
    if required or result is not None:
        validate_result(result)
