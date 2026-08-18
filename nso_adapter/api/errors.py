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

import traceback
from typing import Literal

import structlog
from fastapi import HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

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
        # Receipt admission on a keyed intent push (#1522 §G2): the same X-Push-Seq
        # re-delivered with a different body, and a sequence older than the admitted one.
        "sequence_reuse",
        "stale",
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
    "sequence_reuse",
    "stale",
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

    Seven intent endpoints (vlan/svi/subinterface/l2-sap/logging/bfd/interface-mtu)
    return exactly this shape: how many rows the payload wrote (``count``), how many
    it dropped (``removed``), and whether a PUT-replace was enqueued to revert a
    removal/clear on the device (``replaced``). Endpoints with a different summary
    (bgp/ospf/isis/snmp/ip) keep their own inline model, and static-route EXTENDS this
    one with its per-route settlement echo (``StaticRouteIntentResult``).
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


def push_conflict_error(code: str, message: str, detail: dict | None = None) -> ApiError:
    """Map a receipt-admission refusal (#1522 §G2) onto the wire.

    Here, not at the endpoints: both codes are written out literally in ONE place, which is
    what keeps every code greppable (``test_call_site_codes_are_subset_of_error_codes``
    forbids a dynamic code argument) and what stops the twelve endpoints that join the
    protocol from each spelling the mapping out again.
    """
    if code == "sequence_reuse":
        return api_error(409, "sequence_reuse", message, detail)
    if code == "stale":
        return api_error(409, "stale", message, detail)
    raise ValueError(f"unknown push-admission code {code!r}")


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


async def projection_gone_handler(request: Request, exc: Exception) -> JSONResponse:
    """Answer a device offboarded UNDER a write with the same 404 the endpoints raise.

    Every accepted projection write takes the device's lock before it reads anything, and an
    offboard committing inside that window is a legitimate race, not a bug: the plugin's
    outbox retries a push while an operator removes the device. Mapped here, once, because it
    can surface from any of the sixteen intent PUTs and the two generation actions — each
    catching it locally is how the endpoints that did not came to answer 500.
    """
    return JSONResponse(
        status_code=404,
        content={"error": {"code": "not_found", "message": "Device not found", "detail": {}}},
    )


#: How many innermost traceback frames the unhandled-exception log keeps.
_WHERE_FRAMES = 5


def _where(exc: BaseException) -> list[str]:
    """Render the innermost frames as ``file:line in func`` — locations, nothing read from them.

    Never the source text, the exception args or the frame locals: those carry the very
    credential this path exists to keep out of the logs.
    """
    frames = traceback.extract_tb(exc.__traceback__)[-_WHERE_FRAMES:]
    return [f"{frame.filename}:{frame.lineno} in {frame.name}" for frame in frames]


def unhandled_exception_response(request: Request, exc: BaseException) -> JSONResponse:
    """Answer an unexpected failure with the documented envelope instead of a bare 500.

    The message is GENERIC on purpose and the exception reaches neither the body nor this
    log line: it routinely carries the credential, URL or row it failed on (an hvac error
    echoes the token path, a driver error the DSN), and nobody redacts a 500 body or a
    structured log field before it lands in an aggregator. The redacted ``where`` frames are
    the diagnostic remainder — see the middleware in ``main`` for why no traceback sink is
    left behind them.
    """
    logger.error(
        "api.unhandled_exception",
        exception_type=type(exc).__name__,
        method=request.method,
        path=request.url.path,
        where=_where(exc),
    )
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal", "message": "Internal server error", "detail": {}}},
    )


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Convert FastAPI request-validation failures to the documented envelope.

    ``exc.errors()`` can carry non-JSON-native values (a validator's ValueError
    lands in ``ctx``) — encode through jsonable_encoder like FastAPI's default
    handler does, or a malformed request turns into a 500.

    Pydantic also includes the submitted value under ``input``. Headers and query
    parameters can contain secrets, so the public boundary keeps the location and
    failure reason but never reflects that value.
    """
    errors = [{key: value for key, value in error.items() if key != "input"} for error in exc.errors()]
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_error",
                "message": "Request validation failed",
                "detail": {"errors": jsonable_encoder(errors)},
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
RESP_409_PUSH_SEQ = {
    409: {
        **_ENVELOPE_SCHEMA,
        "description": "X-Push-Seq reused with a different body or mode, or older than the admitted one",
    }
}
# One status, one description: an endpoint that can emit BOTH 409s must say so, or merging
# the two fragments silently drops whichever comes first. The second cause is the DEVICE
# CLAIM, not a running job: the static-route PUT refuses whenever any competing operation
# holds the device — an apply, a removal, a teardown, another intent push.
RESP_409_PUSH_SEQ_OR_DEVICE_BUSY = {
    409: {
        **_ENVELOPE_SCHEMA,
        "description": (
            "The device is busy with another operation, or X-Push-Seq was reused with a "
            "different body or mode / is older than the admitted one"
        ),
    }
}
RESP_422_VALIDATION = {422: {**_ENVELOPE_SCHEMA, "description": "Request validation failed (envelope shape)"}}
RESP_501 = {501: {**_ENVELOPE_SCHEMA, "description": "Not supported by the configured provider"}}
RESP_502_NSO = {502: {**_ENVELOPE_SCHEMA, "description": "NSO unreachable"}}
RESP_502 = {502: {**_ENVELOPE_SCHEMA, "description": "Upstream (NSO/Vault) operation failed"}}
