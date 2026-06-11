# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/removal.py — replace_on_removal."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nso_adapter.core.removal import replace_on_removal


@pytest.mark.asyncio
async def test_replace_on_removal_noop_when_nothing_removed():
    """No removals → no NSO call, returns False."""
    apply_callable = AsyncMock()
    result = await replace_on_removal(MagicMock(), SimpleNamespace(id=1), [], object, apply_callable)
    assert result is False
    apply_callable.assert_not_awaited()


@pytest.mark.asyncio
async def test_replace_on_removal_calls_apply_with_replace_true():
    """On removal, gathers remaining accepted rows and calls apply(..., replace=True)."""
    remaining = [SimpleNamespace(vlan_id=10)]

    scalars = MagicMock()
    scalars.all.return_value = remaining
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    db = MagicMock()
    db.execute = AsyncMock(return_value=result_obj)

    device = SimpleNamespace(id=1, nso_instance="nso-dev", nso_device_name="sw3")
    apply_callable = AsyncMock()
    from nso_adapter.store.models import VlanIntent as store_model

    with patch("nso_adapter.core.importer.get_nso_client", return_value="CLIENT") as gc:
        ok = await replace_on_removal(db, device, [3366], store_model, apply_callable)

    assert ok is True
    gc.assert_called_once_with("nso-dev")
    apply_callable.assert_awaited_once_with("CLIENT", "sw3", remaining, replace=True)


@pytest.mark.asyncio
async def test_replace_on_removal_swallows_errors():
    """A failed NSO replace is logged, not raised (request still succeeds)."""
    scalars = MagicMock()
    scalars.all.return_value = []
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    db = MagicMock()
    db.execute = AsyncMock(return_value=result_obj)
    device = SimpleNamespace(id=1, nso_instance="nso-dev", nso_device_name="sw3")
    from nso_adapter.store.models import VlanIntent as store_model

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=RuntimeError("not registered")):
        ok = await replace_on_removal(db, device, [1], store_model, AsyncMock())
    assert ok is False
