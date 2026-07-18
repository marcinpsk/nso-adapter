# SPDX-License-Identifier: Apache-2.0
"""Standardised error responses matching api-contract.md error shape.

All non-2xx responses use:
  {"error": {"code": "snake_case", "message": "...", "detail": {}}}

The code vocabulary is a CLOSED set (`ERROR_CODES`): api_error() rejects unknown
codes at runtime (there is no static type-checker in CI, so the Literal alone
enforces nothing), and tests pin call sites ⊆ ERROR_CODES ⊆ api-contract.md.
Request-validation failures (FastAPI's RequestValidationError) are converted to
the same envelope by `validation_error_handler` — the framework's default
``{"detail": [...]}`` shape never reaches the wire.
"""

from __future__ import annotations

from typing import Literal

from fastapi import HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Closed set — every member documented in docs/api-contract.md (§ error body).
# Core codes first, then per-endpoint codes. Tests enforce both directions.
ERROR_CODES: frozenset[str] = frozenset(
    {
        # phase-1/2 core
        "unauthorized",
        "not_found",
        "validation_error",
        "nso_unreachable",
        "netbox_unreachable",
        "conflict",
        "internal",
        "not_implemented",
        "nso_commit_failed",
        # per-endpoint
        "ambiguous_device",
        "bad_request",
        "community_not_found",
        "harvest_unsupported_ned",
        "invalid_entries",
        "invalid_family",
        "invalid_name",
        "invalid_payload",
        "invalid_vault_ref",
        "no_ned_id",
        "no_nso_client",
        "nso_unavailable",
        "secrets_write_unsupported",
        "vault_error",
    }
)

ErrorCode = Literal[
    "unauthorized",
    "not_found",
    "validation_error",
    "nso_unreachable",
    "netbox_unreachable",
    "conflict",
    "internal",
    "not_implemented",
    "nso_commit_failed",
    "ambiguous_device",
    "bad_request",
    "community_not_found",
    "harvest_unsupported_ned",
    "invalid_entries",
    "invalid_family",
    "invalid_name",
    "invalid_payload",
    "invalid_vault_ref",
    "no_ned_id",
    "no_nso_client",
    "nso_unavailable",
    "secrets_write_unsupported",
    "vault_error",
]


class ErrorBody(BaseModel):
    """The inner object of the error envelope."""

    code: ErrorCode
    message: str
    detail: dict = {}


class ErrorEnvelope(BaseModel):
    """The one error shape every non-2xx response uses (api-contract.md)."""

    error: ErrorBody


class IntentApplyResult(BaseModel):
    """Shared 2xx body for the full-replace intent PUTs.

    Eight intent endpoints (static-route/vlan/svi/subinterface/l2-sap/logging/
    bfd/interface-mtu) return exactly this shape: how many rows the payload wrote
    (``count``), how many it dropped (``removed``), and whether a PUT-replace was
    enqueued to revert a removal/clear on the device (``replaced``). Endpoints
    with a different summary (bgp/ospf/isis/snmp/ip) keep their own inline model.
    """

    device_id: int
    count: int
    removed: int
    replaced: bool


class ApiError(HTTPException):
    """HTTPException whose detail IS the full response body (no FastAPI wrapping)."""


def api_error(
    status_code: int,
    code: ErrorCode,
    message: str,
    detail: dict | None = None,
) -> ApiError:
    if code not in ERROR_CODES:
        raise ValueError(f"unknown error code {code!r} — add it to ERROR_CODES and api-contract.md")
    return ApiError(
        status_code=status_code,
        detail={"error": {"code": code, "message": message, "detail": detail or {}}},
    )


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Convert FastAPI request-validation failures to the documented envelope.

    ``exc.errors()`` can carry non-JSON-native values (a validator's ValueError
    lands in ``ctx``) — encode through jsonable_encoder like FastAPI's default
    handler does, or a malformed request turns into a 500.
    """
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_error",
                "message": "Request validation failed",
                "detail": {"errors": jsonable_encoder(exc.errors())},
            }
        },
    )


# Reusable per-endpoint `responses=` fragments. Compose only the ones an
# operation can actually emit (its own api_error calls + its helpers' + its
# dependencies' + framework paths) — never a blanket router-level set.
_ENVELOPE_SCHEMA = {"model": ErrorEnvelope}

RESP_400 = {400: {**_ENVELOPE_SCHEMA, "description": "Bad request"}}
RESP_401 = {401: {**_ENVELOPE_SCHEMA, "description": "Missing or invalid bearer token"}}
RESP_404_DEVICE = {404: {**_ENVELOPE_SCHEMA, "description": "Device not found"}}
RESP_404 = {404: {**_ENVELOPE_SCHEMA, "description": "Not found"}}
RESP_409_ACTIVE_JOB = {409: {**_ENVELOPE_SCHEMA, "description": "A job is already running for this device"}}
RESP_409 = {409: {**_ENVELOPE_SCHEMA, "description": "Conflict"}}
RESP_422_VALIDATION = {422: {**_ENVELOPE_SCHEMA, "description": "Request validation failed (envelope shape)"}}
RESP_502_NSO = {502: {**_ENVELOPE_SCHEMA, "description": "NSO unreachable"}}
