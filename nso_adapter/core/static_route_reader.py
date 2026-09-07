# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The ONE certified reader of a device's static-route service section (#1683).

Four sites take a certified static-route read: the sender's retention snapshot, the removal
body's ``current``, detach settlement's proof and the reclaimer's proof. Each of them passed
the LEGACY instance path and then indexed a TOP-LEVEL ``route`` list. The aggregate nests the
same routes one level down, under ``container static-route``
(``nso-packages/device-intent/src/yang/device-intent.yang``), and C9's cutover deletes the
legacy instances: afterwards those reads would certify ABSENCE while the aggregate owns the
key, so retention would omit it and a consumption proof would wrongly permit consumption.

Changing the URL alone does not fix it, because the nesting is wrong too. So the path and the
projection live HERE and nowhere else, and every consumer keeps the shape it already reads:
``{"route": [...]}``. The aggregate sender made that real: the path is now the one
``device-intent`` instance (``NsoClient.service_instance_state``) and the projection reads the
section out of its ``static-route`` container. A legacy-shaped answer at that path is refused
rather than read: certifying the family from a service the adapter no longer writes is exactly
the mistake this module exists to prevent.
"""

from __future__ import annotations

from typing import NamedTuple

import structlog

logger = structlog.get_logger(__name__)


class CertifiedSection(NamedTuple):
    """A certified verdict about the device's static-route section.

    ``present`` carries the section normalized to ``{"route": [...]}``. ``absent`` is
    CONCLUSIVE and means the certified instance carries no static-route entry, from a
    conclusive 404 and from an empty container alike. Anything uncertifiable is
    ``inconclusive`` and every consumer refuses on it.

    *instance* is the whole certified instance the same read saw, so the sender's collateral
    guard and the retained entries come from ONE read (#1396 R2 §4.1) instead of two that can
    disagree. It is ``None`` for a certified absence and for an uncertifiable read alike.
    """

    status: str
    entry: dict | None
    instance: dict | None = None

    @property
    def inconclusive(self) -> bool:
        return self.status == "inconclusive"

    @property
    def routes(self) -> list[dict]:
        """The section's entries, empty for a certified absence."""
        return list((self.entry or {}).get("route") or [])


async def certified_static_route_section(client, device) -> CertifiedSection:
    """Read THIS device's static-route section once, certified.

    The read may refuse a write and may supply the bytes of a key the caller's frozen plan
    already names; it may never add a key, authorize an omission or select a carrier.
    """
    state = await client.service_instance_state(device.nso_device_name)
    if state.inconclusive:
        return CertifiedSection("inconclusive", None)
    try:
        routes = _project(state.entry)
    except _Uncertifiable as exc:
        logger.warning("static_route.section_uncertifiable", device=device.nso_device_name, reason=str(exc))
        return CertifiedSection("inconclusive", None)
    if not routes:
        # An instance with no entry and no instance at all say the same thing, and the
        # consumers all treat "certified nothing here" alike.
        return CertifiedSection("absent", None, state.entry)
    return CertifiedSection("present", {"route": routes}, state.entry)


class _Uncertifiable(Exception):
    """The instance parsed, but its static-route section is not a shape this can certify."""


def _project(entry: dict | None) -> list[dict]:
    """Return the static-route entries of a service instance, whatever level they sit at.

    Every departure from the YANG shape raises: discarding it would report a malformed
    answer as certified ABSENCE, and a consumption proof would then consume a carrier whose
    key the service may still hold.
    """
    if entry is None:
        return []
    if not isinstance(entry, dict):
        raise _Uncertifiable(f"the instance is a {type(entry).__name__}, not an object")
    if "static-route" not in entry:
        if "route" in entry:
            # A LEGACY-shaped instance answering for the aggregate's path. Reading it would
            # certify the family from a service the adapter no longer writes, so refuse.
            raise _Uncertifiable("the instance carries a top-level route list, not a static-route container")
        return []
    section = entry["static-route"]
    if not isinstance(section, dict):
        raise _Uncertifiable(f"the static-route container is a {type(section).__name__}, not an object")
    if "route" not in section:
        return []
    routes = section["route"]
    if not isinstance(routes, list):
        raise _Uncertifiable(f"the route list is a {type(routes).__name__}, not a list")
    for route in routes:
        _certify_entry(route)
    return list(routes)


def _certify_entry(route: object) -> None:
    """Refuse a route entry that is not one keyed object. ``vrf`` and ``next-hop`` may be empty."""
    if not isinstance(route, dict):
        raise _Uncertifiable(f"a route entry is a {type(route).__name__}, not an object")
    prefix = route.get("prefix")
    if not isinstance(prefix, str) or not prefix:
        raise _Uncertifiable(f"a route entry carries no prefix key: {prefix!r}")
    for leaf in ("vrf", "next-hop"):
        value = route.get(leaf)
        if value is not None and not isinstance(value, str):
            raise _Uncertifiable(f"a route entry carries a non-string {leaf}: {value!r}")


__all__ = ["CertifiedSection", "certified_static_route_section"]
