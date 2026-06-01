# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/state.py — sync_state status logic."""

from __future__ import annotations

from nso_adapter.core.sync_state import compute_drift, compute_sync_state
from nso_adapter.domain.models import Interface, InterfaceAttr
from nso_adapter.store.models import SyncState

# ── Phase 1 paths (intent_value=None) ────────────────────────────────────────


def test_phase1_unknown_when_netbox_value_is_none():
    assert compute_sync_state(None, None) == SyncState.unknown


def test_phase1_unknown_when_nso_value_is_none_and_no_netbox():
    assert compute_sync_state(None, None) == SyncState.unknown


def test_phase1_imported_when_values_match():
    assert compute_sync_state("hello", "hello") == SyncState.imported


def test_phase1_changed_when_values_differ():
    assert compute_sync_state("nso-val", "nb-val") == SyncState.changed


def test_phase1_unknown_when_netbox_none_nso_set():
    assert compute_sync_state("nso-val", None) == SyncState.unknown


# ── Phase 2 paths (intent_value is not None) ─────────────────────────────────


def test_phase2_in_sync_when_nso_matches_intent():
    assert compute_sync_state("deployed", "anything", intent_value="deployed") == SyncState.in_sync


def test_phase2_drifted_when_nso_differs_from_intent():
    assert compute_sync_state("different", "anything", intent_value="deployed") == SyncState.drifted


def test_phase2_in_sync_takes_priority_over_netbox():
    """Phase 2: only NSO vs intent matters; netbox_value is irrelevant."""
    assert compute_sync_state("intent-val", "nb-val", intent_value="intent-val") == SyncState.in_sync


# ── compute_drift ─────────────────────────────────────────────────────────────


def _make_interface(
    nso_desc: str | None,
    nb_desc: str | None,
    nso_enabled: bool | None = None,
    nb_enabled: bool | None = None,
) -> Interface:
    return Interface(
        name="Gi0/0",
        nso=InterfaceAttr(description=nso_desc, enabled=nso_enabled),
        netbox=InterfaceAttr(description=nb_desc, enabled=nb_enabled),
    )


def test_compute_drift_no_drift():
    iface = _make_interface("same", "same", True, True)
    result = compute_drift([iface])
    assert result[0].is_drifted is False


def test_compute_drift_description_differs():
    iface = _make_interface("nso-desc", "nb-desc")
    result = compute_drift([iface])
    assert result[0].is_drifted is True


def test_compute_drift_enabled_differs():
    iface = _make_interface(None, None, True, False)
    result = compute_drift([iface])
    assert result[0].is_drifted is True


def test_compute_drift_netbox_none_no_drift():
    """When netbox_value is None, we cannot compute drift; not drifted."""
    iface = _make_interface("nso-val", None)
    result = compute_drift([iface])
    assert result[0].is_drifted is False
