# SPDX-License-Identifier: Apache-2.0
"""Standardised error responses matching api-contract.md error shape.

All non-2xx responses use:
  {"error": {"code": "snake_case", "message": "...", "detail": {}}}
"""
from __future__ import annotations

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


class ApiError(HTTPException):
    """HTTPException whose detail IS the full response body (no FastAPI wrapping)."""


def api_error(
    status_code: int,
    code: str,
    message: str,
    detail: dict | None = None,
) -> ApiError:
    return ApiError(
        status_code=status_code,
        detail={"error": {"code": code, "message": message, "detail": detail or {}}},
    )


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.detail)
