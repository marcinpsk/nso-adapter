# SPDX-License-Identifier: Apache-2.0
"""Sync-state checker — compares NSO vs NetBox interface attributes.

Phase 1 (no intent ownership):
  unknown  — not yet synced.
  imported — NetBox value matches last NSO import.
  changed  — NSO value now differs from NetBox (out-of-band device change).

Phase 2 (intent deployed via apply action):
  in_sync  — device matches deployed intent.
  drifted  — device has changed since intent was deployed.
"""

from __future__ import annotations

from nso_adapter.domain.models import Interface
from nso_adapter.store.models import SyncState


def compute_sync_state(
    nso_value: object,
    netbox_value: object,
    intent_value: object = None,
) -> SyncState:
    """Return per-attribute sync_state status.

    If *intent_value* is set (Phase 2 — intent has been deployed), use the
    Phase 2 states: ``in_sync`` / ``drifted``.
    Otherwise fall back to Phase 1 states: ``unknown`` / ``imported`` / ``changed``.
    """
    if intent_value is not None:
        # Phase 2: compare device (via NSO) against deployed intent
        if nso_value == intent_value:
            return SyncState.in_sync
        return SyncState.drifted

    # Phase 1: compare NSO against what we last wrote to NetBox
    if netbox_value is None:
        return SyncState.unknown
    if nso_value == netbox_value:
        return SyncState.imported
    return SyncState.changed


def compute_drift(interfaces: list[Interface]) -> list[Interface]:
    """Mark each Interface.is_drifted if NSO and NetBox attributes differ."""
    for iface in interfaces:
        drifted = False
        if iface.netbox.description is not None and iface.nso.description != iface.netbox.description:
            drifted = True
        if iface.netbox.enabled is not None and iface.nso.enabled != iface.netbox.enabled:
            drifted = True
        iface.is_drifted = drifted
    return interfaces
