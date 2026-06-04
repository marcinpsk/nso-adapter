# SPDX-License-Identifier: Apache-2.0
"""NED helpers — platform family lookup and device-dict NED extraction."""

from __future__ import annotations

# Maps NED ID prefix → human-readable platform family string.
_NED_FAMILY_MAP: dict[str, str] = {
    "cisco-ios-cli": "ios",
    "cisco-iosxr-cli": "iosxr",
    "cisco-nx-cli": "nxos",
    "juniper-junos-nc": "junos",
    "juniper-junos-evo-nc": "junos",
}


def ned_family(ned_id: str) -> str | None:
    """Return the platform family string for *ned_id*, or ``None`` if unrecognised.

    Used to derive a short ``platform`` label from the NED ID returned by NSO.
    Examples: ``"cisco-ios-cli-6.95"`` → ``"ios"``,
    ``"juniper-junos-nc-4.1"`` → ``"junos"``, ``"nokia-sros-nc-22.10"`` → ``None``.
    A bare prefix with no version suffix (``"cisco-ios-cli"``) also matches.
    """
    for prefix, family in _NED_FAMILY_MAP.items():
        if ned_id.startswith(prefix):
            return family
    return None


_NED_DEVICE_TYPE_KEYS: tuple[str, ...] = ("cli", "netconf", "generic")


def _ned_oper_status(raw) -> str:
    """Normalise a packages/package oper-status into a short string.

    RESTCONF renders the oper-status choice as a container whose single present
    child names the state (e.g. ``{"up": [null]}`` / ``{"up": {}}``). Fall back
    to ``str`` for plain-leaf renderings, else ``"unknown"``.
    """
    if isinstance(raw, dict):
        for key in ("up", "error", "failed", "init"):
            if key in raw:
                return key
        keys = list(raw.keys())
        return keys[0] if keys else "unknown"
    return str(raw) if raw else "unknown"


def extract_ned_component(component):
    """Return ``(ned_id, device_meta)`` for a package's NED component, else None.

    A NED component carries a ``ned`` container with a cli/netconf/generic
    transport (each holding ``ned-id``) plus a ``device`` container (vendor /
    operating-system / product-family). Application/callback components have no
    ``ned`` → None, so callers can tell NEDs apart from service packages.
    """
    if not isinstance(component, list):
        return None
    for comp in component:
        if not isinstance(comp, dict):
            continue
        ned = comp.get("ned")
        if not isinstance(ned, dict):
            continue
        ned_id = None
        for key in _NED_DEVICE_TYPE_KEYS:
            sub = ned.get(key)
            if isinstance(sub, dict) and sub.get("ned-id"):
                ned_id = sub["ned-id"]
                break
        if ned_id is None:
            continue
        device_meta = ned.get("device") if isinstance(ned.get("device"), dict) else {}
        return ned_id, device_meta
    return None


def extract_ned_id_from_device_dict(device: dict) -> str | None:
    """Extract NED ID from a raw NSO device dict's ``device-type`` container.

    Handles three transport keys: ``cli``, ``netconf``, and ``generic``.
    Returns ``None`` when the device has no type information or when the
    sub-dict is unexpectedly ``null`` in the NSO RESTCONF payload.
    """
    device_type = device.get("device-type") or {}
    for key in _NED_DEVICE_TYPE_KEYS:
        if key in device_type:
            sub = device_type[key]
            return sub.get("ned-id") if isinstance(sub, dict) else None
    return None
