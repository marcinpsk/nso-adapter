# SPDX-License-Identifier: Apache-2.0
"""Standardised error responses matching api-contract.md error shape.

All non-2xx responses use:
  {"error": {"code": "snake_case", "message": "...", "detail": {}}}

The code vocabulary is a CLOSED set (`ERROR_CODES`): api_error() rejects unknown
codes at runtime (the Literal cannot validate dynamically constructed values),
and tests pin call sites ⊆ ERROR_CODES ⊆ api-contract.md.
Request-validation failures (FastAPI's RequestValidationError) are converted to
the same envelope by `validation_error_handler` — the framework's default
``{"detail": [...]}`` shape never reaches the wire.
"""

from __future__ import annotations

import traceback
from http import HTTPStatus
from typing import Any, Literal, get_args

import structlog
from fastapi import HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from nso_adapter.core.generation import ApplyUnexecutable
from nso_adapter.core.receipt import PromotionProvenanceUnexecutable

logger = structlog.get_logger(__name__)

# Closed set. Every member is documented in docs/api-contract.md (error body).
# Core codes come first, followed by per-endpoint codes.
ErrorCode = Literal[
    # phase-1/2 core
    "unauthorized",
    "not_found",
    "method_not_allowed",
    "validation_error",
    "nso_unreachable",
    "netbox_unreachable",
    "conflict",
    "internal",
    "not_implemented",
    "nso_commit_failed",
    "apply_unexecutable",
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
]

# Derive runtime validation from the OpenAPI/Pydantic enum.
ERROR_CODES: frozenset[str] = frozenset(get_args(ErrorCode))


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


class StoredIntentResult(BaseModel):
    """Truthful result for a claim-less full-snapshot store write (#1612).

    ``prepared`` says the snapshot is stored and SELECTABLE by a manual Apply; ``stored``
    says a store-only replacement landed and prepared nothing. Neither word claims
    authorization: only Apply authorizes, and only Apply reports deployment.
    ``selection_revision`` is the whole selection identity, and it is null for ``stored``.
    """

    status: Literal["prepared", "stored"]
    device_id: int
    stream: str
    count: int
    removed: int
    desired_revision: int
    selection_revision: int | None


class ApiError(HTTPException):
    """HTTPException whose detail IS the full response body (no FastAPI wrapping)."""


def api_error(
    status_code: int,
    code: ErrorCode,
    message: str,
    detail: dict | None = None,
) -> ApiError:
    if code not in ERROR_CODES:
        raise ValueError(f"unknown error code {code!r}: add it to ErrorCode and api-contract.md")
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


async def api_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ApiError)
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


async def framework_http_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Normalize framework-generated failures that occur outside endpoint handlers."""
    assert isinstance(exc, StarletteHTTPException)
    if exc.status_code == 400 and isinstance(exc.__cause__, ValueError):
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request body must contain valid JSON",
                    "detail": {},
                }
            },
            headers=exc.headers,
        )
    codes: dict[int, ErrorCode] = {
        401: "unauthorized",
        404: "not_found",
        405: "method_not_allowed",
        409: "conflict",
        422: "validation_error",
        501: "not_implemented",
    }
    code = codes.get(exc.status_code, "internal" if exc.status_code >= 500 else "bad_request")
    message = (
        "Internal server error" if exc.status_code >= 500 else str(exc.detail or HTTPStatus(exc.status_code).phrase)
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": code, "message": message, "detail": {}}},
        headers=exc.headers,
    )


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


async def apply_unexecutable_handler(request: Request, exc: Exception) -> JSONResponse:
    """Refuse generation creation when its selected projection cannot execute exactly."""
    assert isinstance(exc, ApplyUnexecutable)
    reasons = exc.reasons
    streams = sorted(reasons)
    return JSONResponse(
        status_code=409,
        content={
            "error": {
                "code": "apply_unexecutable",
                "message": f"Selected stream(s) cannot be applied faithfully: {', '.join(streams)}",
                "detail": {"streams": reasons},
            }
        },
    )


async def promotion_provenance_handler(request: Request, exc: Exception) -> JSONResponse:
    """Refuse a push that cannot execute deletion provenance from an earlier revision."""
    assert isinstance(exc, PromotionProvenanceUnexecutable)
    stream = exc.stream
    return await api_error_handler(
        request,
        api_error(
            409,
            "apply_unexecutable",
            str(exc),
            {"streams": {stream: "outstanding_deletion_provenance"}},
        ),
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


async def validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Convert FastAPI request-validation failures to the documented envelope.

    Pydantic can include the submitted value under ``input`` and a validator's
    exception under ``ctx`` and ``msg``. Any of them can contain secrets, so the
    public boundary keeps only the location and stable error type.
    """
    assert isinstance(exc, RequestValidationError)
    errors = [{"loc": error["loc"], "type": error["type"], "msg": "Invalid value"} for error in exc.errors()]
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
ResponseSpec = dict[int | str, dict[str, Any]]

_ENVELOPE_SCHEMA: dict[str, Any] = {"model": ErrorEnvelope}

RESP_400: ResponseSpec = {400: {**_ENVELOPE_SCHEMA, "description": "Bad request"}}
RESP_401: ResponseSpec = {401: {**_ENVELOPE_SCHEMA, "description": "Missing or invalid bearer token"}}
RESP_404_DEVICE: ResponseSpec = {404: {**_ENVELOPE_SCHEMA, "description": "Device not found"}}
RESP_404: ResponseSpec = {404: {**_ENVELOPE_SCHEMA, "description": "Not found"}}
RESP_409_QUEUED_ACTION: ResponseSpec = {
    409: {**_ENVELOPE_SCHEMA, "description": "A job of the requested type is already queued for this device"}
}
RESP_409_APPLY_CONFLICT: ResponseSpec = {
    409: {
        **_ENVELOPE_SCHEMA,
        "description": (
            "The Apply UUID identifies a different request, a job is already queued or running, "
            "or a selected stream cannot be applied faithfully"
        ),
    }
}
RESP_409: ResponseSpec = {409: {**_ENVELOPE_SCHEMA, "description": "Conflict"}}
RESP_409_PUSH_SEQ: ResponseSpec = {
    409: {
        **_ENVELOPE_SCHEMA,
        "description": "X-Push-Seq reused with a different body or mode, or older than the admitted one",
    }
}
# One status, one description: an endpoint that can emit BOTH 409s must say so, or merging
# the two fragments silently drops whichever comes first. The second cause is the DEVICE
# CLAIM, not a running job: the static-route PUT refuses whenever any competing operation
# holds the device — an apply, a removal, a teardown, another intent push.
RESP_409_PUSH_SEQ_OR_DEVICE_BUSY: ResponseSpec = {
    409: {
        **_ENVELOPE_SCHEMA,
        "description": (
            "The device is busy with another operation, or X-Push-Seq was reused with a "
            "different body or mode / is older than the admitted one"
        ),
    }
}
RESP_422_VALIDATION: ResponseSpec = {
    422: {**_ENVELOPE_SCHEMA, "description": "Request validation failed (envelope shape)"}
}
RESP_500_INTERNAL: ResponseSpec = {500: {**_ENVELOPE_SCHEMA, "description": "Internal adapter invariant failed"}}
RESP_501: ResponseSpec = {501: {**_ENVELOPE_SCHEMA, "description": "Not supported by the configured provider"}}
RESP_502_NSO: ResponseSpec = {502: {**_ENVELOPE_SCHEMA, "description": "NSO unreachable"}}
RESP_502: ResponseSpec = {502: {**_ENVELOPE_SCHEMA, "description": "Upstream (NSO/Vault) operation failed"}}
