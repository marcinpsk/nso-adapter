# SPDX-License-Identifier: Apache-2.0
"""FastAPI dependency injectors."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import structlog
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import ApiError
from nso_adapter.store.db import get_session

logger = structlog.get_logger(__name__)


class _OptionalBearer(HTTPBearer):
    """HTTPBearer that returns None instead of raising when no credentials."""

    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        try:
            return await super().__call__(request)
        except HTTPException:  # pragma: no cover — auto_error=False means super() never raises
            return None  # pragma: no cover


bearer = _OptionalBearer(auto_error=False)

_UNAUTH = ApiError(
    status_code=401,
    detail={"error": {"code": "unauthorized", "message": "Missing or invalid bearer token", "detail": {}}},
)


async def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(bearer),
) -> str:
    if credentials is None:
        raise _UNAUTH
    expected: str = request.app.state.adapter_token
    if credentials.credentials != expected:
        raise _UNAUTH
    return credentials.credentials


async def get_db(session: AsyncSession = Depends(get_session)) -> AsyncGenerator[AsyncSession, None]:
    yield session
