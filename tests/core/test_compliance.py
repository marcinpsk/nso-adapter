# SPDX-License-Identifier: Apache-2.0
"""Tests for compliance drift checker."""

from nso_adapter.core.compliance import compute_compliance_status, compute_drift
from nso_adapter.domain.models import Interface, InterfaceAttr
from nso_adapter.store.models import ComplianceStatus


def _iface(desc_nso, desc_nb, enabled_nso, enabled_nb):
    return Interface(
        name="GigabitEthernet0/0",
        nso=InterfaceAttr(description=desc_nso, enabled=enabled_nso),
        netbox=InterfaceAttr(description=desc_nb, enabled=enabled_nb),
    )


def test_no_drift_when_equal():
    ifaces = compute_drift([_iface("link", "link", True, True)])
    assert not ifaces[0].is_drifted


def test_drift_on_description():
    ifaces = compute_drift([_iface("old", "new", True, True)])
    assert ifaces[0].is_drifted


def test_drift_on_enabled():
    ifaces = compute_drift([_iface("x", "x", True, False)])
    assert ifaces[0].is_drifted


def test_no_drift_when_netbox_attrs_none():
    """If NetBox has no data yet, treat as no-drift (nothing to compare)."""
    ifaces = compute_drift([_iface("desc", None, True, None)])
    assert not ifaces[0].is_drifted


# ── compute_compliance_status ─────────────────────────────────────────────────


def test_compliance_unknown_when_no_netbox():
    assert compute_compliance_status("uplink", None) == ComplianceStatus.unknown


def test_compliance_imported_when_equal():
    assert compute_compliance_status("uplink", "uplink") == ComplianceStatus.imported


def test_compliance_changed_when_different():
    assert compute_compliance_status("new-desc", "old-desc") == ComplianceStatus.changed


def test_compliance_unknown_for_none_nso_and_none_nb():
    assert compute_compliance_status(None, None) == ComplianceStatus.unknown


def test_compliance_changed_for_none_nso_with_existing_nb():
    assert compute_compliance_status(None, "old") == ComplianceStatus.changed
