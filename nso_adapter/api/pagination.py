# SPDX-License-Identifier: Apache-2.0
"""Shared fail-fast bounds for collection endpoints."""

from __future__ import annotations

from nso_adapter.api.errors import api_error

DEFAULT_PAGE = 100
LIMIT_MIN = 1
LIMIT_MAX = 500


def validate_page_limit(limit: int) -> int:
    """Return a valid page size. Refuse invalid requests instead of clamping them."""
    if not LIMIT_MIN <= limit <= LIMIT_MAX:
        raise api_error(422, "validation_error", f"limit must be between {LIMIT_MIN} and {LIMIT_MAX}: {limit}")
    return limit
