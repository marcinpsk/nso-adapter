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
