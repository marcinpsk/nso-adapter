# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Public projections of frozen execution evidence."""

import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import MAX_EMAX, MIN_EMIN, Context, Decimal, localcontext
from enum import Enum
from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from nso_adapter.api.timestamps import iso_z
from nso_adapter.nso.execution_contract import ContractError, validate_result
from nso_adapter.store.json_codec import UnrepresentableJSONNumber


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


def _canonical_number(value: int | float | UnrepresentableJSONNumber) -> str:
    exponent_literal = "0"
    if isinstance(value, UnrepresentableJSONNumber):
        mantissa, _, exponent_literal = value.literal.lower().partition("e")
        number = Decimal(mantissa)
    else:
        number = Decimal.from_float(value) if isinstance(value, float) else Decimal(value)
    parts = number.as_tuple()
    assert isinstance(parts.exponent, int)
    digits = "".join(str(digit) for digit in parts.digits).rstrip("0")
    if not digits:
        return "0"
    offset = parts.exponent + len(parts.digits) - len(digits)
    # JSON exponents can exceed the exponent range of Decimal.
    precision = max(len(exponent_literal), len(str(offset))) + 1
    with localcontext(Context(prec=precision, Emax=MAX_EMAX, Emin=MIN_EMIN)):
        exponent = Decimal(exponent_literal or "0") + offset
    return f"{'-' if parts.sign else ''}{digits}e{exponent:f}"


def canonical_evidence(value):
    """Equal digests imply equal stored JSON values, with numbers compared by exact numeric value."""
    if isinstance(value, Enum):
        return canonical_evidence(value.value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        try:
            if value.tzinfo is not None and value.utcoffset() is not None:
                return iso_z(value.astimezone(UTC))
        except (ValueError, OverflowError):
            pass
        return {"$datetime": value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"$float": str(value)}
    if isinstance(value, UnrepresentableJSONNumber) or isinstance(value, (int, float)) and not isinstance(value, bool):
        return {"$json_number": _canonical_number(value)}
    if isinstance(value, dict):
        return {"$" + key if key.startswith("$") else key: canonical_evidence(item) for key, item in value.items()}
    if isinstance(value, list):
        return [canonical_evidence(item) for item in value]
    return value


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def evidence_digest(candidate) -> str:
    return hashlib.sha256(_canonical_json(canonical_evidence(candidate)).encode(errors="surrogatepass")).hexdigest()


def _projection_json_value(value):
    if isinstance(value, UnrepresentableJSONNumber):
        raise ValueError("Stored JSON number has no exact native representation")
    return canonical_evidence(value)


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
            json.dumps(candidate, default=_projection_json_value, allow_nan=False), strict=True
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
