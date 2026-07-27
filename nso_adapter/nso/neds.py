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

# NSO encodes the southbound transport in the NED id's protocol token (the segment
# before the version): cisco-ios-cli-6.114 → cli; juniper-junos-nc-4.19 → netconf;
# foo-gen-1.0 → generic. Map each known token to the NSO ``device-type`` transport.
_NED_PROTO_TO_TRANSPORT: dict[str, str] = {
    "cli": "cli",
    "nc": "netconf",
    "netconf": "netconf",
    "gen": "generic",
    "generic": "generic",
    "snmp": "snmp",
}


def ned_transport(ned_id: str) -> str | None:
    """Return the NSO ``device-type`` transport encoded in *ned_id*, or None.

    The transport (cli / netconf / generic / snmp) is part of the NED id naming
    convention ``<vendor>-<os>-<transport>-<version>`` — e.g. ``juniper-junos-nc-4.19``
    is a NETCONF NED (``-nc-``), not a CLI one. Handles the doubled identityref form NSO
    RESTCONF returns (``juniper-junos-nc-4.19:juniper-junos-nc-4.19``).

    The transport is ANCHORED to the segment immediately before the version (or the last
    segment when there is no version suffix), so a transport-like token elsewhere in the
    id (a vendor/model segment such as ``foo-cli-bar-9.9``) cannot be mistaken for it.
    """
    segments = ned_id.split(":")[-1].split("-")
    if not segments:
        return None
    # A version suffix contains a digit (e.g. "6.114", "24.4"); when present the transport
    # is the segment before it, otherwise it is the final segment.
    has_version = any(ch.isdigit() for ch in segments[-1])
    idx = -2 if (has_version and len(segments) >= 2) else -1
    return _NED_PROTO_TO_TRANSPORT.get(segments[idx].lower())


def resolve_device_type(ned_id: str, requested: str | None = None) -> str:
    """Resolve the NSO ``device-type`` transport for onboarding *ned_id*.

    The transport is authoritatively encoded in the NED id, so it is derived from
    there. A *requested* value that contradicts a derivable transport raises
    ``ValueError`` — the guard that stops a NETCONF NED (``-nc-``) being onboarded
    as ``device-type cli`` (the bug that left lab01c-rd2.lab unable to sync).
    When the NED id carries no recognisable protocol token, fall back to *requested*
    (or ``"cli"``).
    """
    derived = ned_transport(ned_id)
    if derived is None:
        return requested or "cli"
    if requested is not None and requested != derived:
        raise ValueError(f"ned_type {requested!r} contradicts NED id {ned_id!r} (transport={derived!r})")
    return derived


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
