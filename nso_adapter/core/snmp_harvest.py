# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Brownfield SNMP community harvest from NSO's device-config mirror.

On IOS/IOS-XE/IOS-XR/Junos the community string sits in plaintext in the device
config (the read mirror deliberately redacts it to ``sha256[:16]``). Harvest
reads ONLY the targeted per-NED community subtree (never the full device
config) and matches by fingerprint, so the operator can move a device-held
secret into Vault without ever typing it.

timos (Nokia SR OS) is EXCLUDED — live-confirmed (SR OS lab, 2026-07-06): the
config mirror stores communities as hash2-obfuscated blobs, never plaintext, so
neither harvest nor vault-vs-device value comparison is possible there.
SNMPv3 secrets are never harvestable (engine-ID-localized on every vendor).

The per-NED JSON shapes mirror network-state-export's device-verified readers
(``_ios_snmp_communities`` / ``_ios_xr_snmp_communities`` /
``_junos_snmp_communities``).
"""

from __future__ import annotations

from dataclasses import dataclass

from nso_adapter.secrets.refs import secret_fingerprint

# NED-id prefix → RESTCONF subpath of the community list under
# /restconf/data/tailf-ncs:devices/device=<name>/config/
_HARVEST_PATHS: dict[str, str] = {
    "cisco-ios-cli": "tailf-ned-cisco-ios:snmp-server/community",
    "cisco-iosxr-cli": "tailf-ned-cisco-ios-xr:snmp-server/community",
    "juniper-junos-nc": "junos:configuration/snmp/community",
    # ArcOS (live-verified ri6 2026-07-07): communities ride the openconfig-system
    # augment; the list-entry NAME is the plaintext secret, same as the cisco shape.
    "arcos-": "openconfig-system:system/arcos-openconfig-system-augments:snmp-server/communities/community",
}


@dataclass(frozen=True)
class HarvestedCommunity:
    secret: str
    access: str  # "RO" | "RW"
    acl: str | None


def harvest_subpath(ned_id: str) -> str | None:
    """Return the community-list RESTCONF subpath for *ned_id*, or None if unsupported."""
    for prefix, subpath in _HARVEST_PATHS.items():
        if ned_id.startswith(prefix):
            return subpath
    return None


def _entries(payload: dict) -> list[dict]:
    """Unwrap the (module-qualified) community list from a RESTCONF GET body."""
    if not isinstance(payload, dict):
        return []
    for key, value in payload.items():
        if key.split(":")[-1] == "community" and isinstance(value, list):
            return [e for e in value if isinstance(e, dict)]
    return []


def _extract_cisco(entry: dict) -> HarvestedCommunity | None:
    """IOS and IOS-XR: list key ``name`` IS the secret; RO/RW presence leaves.

    The ACL leaf is ``access-list-name`` on IOS and ``access-list`` on IOS-XR.
    """
    name = entry.get("name")
    if not isinstance(name, str) or not name:
        return None
    access = "RW" if "RW" in entry else "RO"
    acl = entry.get("access-list-name") or entry.get("access-list")
    return HarvestedCommunity(secret=name, access=access, acl=str(acl) if acl is not None else None)


def _extract_junos(entry: dict) -> HarvestedCommunity | None:
    """Junos ``snmp community``: key ``name`` IS the secret; authorization enum."""
    name = entry.get("name")
    if not isinstance(name, str) or not name:
        return None
    auth = str(entry.get("authorization") or "read-only")
    access = "RW" if "write" in auth.lower() else "RO"
    return HarvestedCommunity(secret=name, access=access, acl=None)


def find_community(ned_id: str, payload: dict, community_hash: str) -> HarvestedCommunity | None:
    """Return the community whose plaintext fingerprint equals *community_hash*.

    ArcOS deliberately shares the cisco extractor: its entry NAME is likewise the
    secret, and the platform has no RW/acl knobs, so the cisco fallbacks (RO, no
    acl) are exactly the arcos truth (pinned by the arcos test).
    """
    extract = _extract_junos if ned_id.startswith("juniper-junos-nc") else _extract_cisco
    for entry in _entries(payload):
        found = extract(entry)
        if found is not None and secret_fingerprint(found.secret) == community_hash:
            return found
    return None
