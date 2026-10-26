# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Build export sections the way the exporter states collection coverage (#1833).

``exported`` gives the wire form a fake NSO serves; ``covered`` gives the decoded form a
fake client method or a direct ``Present`` outcome carries. Both read the vendored manifest.
"""

from __future__ import annotations

from copy import deepcopy
from unittest.mock import AsyncMock

from nso_adapter.nso.collection_coverage import FAMILIES, MARKER, decode_section

#: The section each redistribution component reads, by destination protocol.
REDIST_WIRES = {"bgp": "bgp-config", "isis": "isis-interface", "ospf": "ospf-config"}


def exported(family: str, payload: dict, *, cover_all: bool = True) -> dict:
    """Return *payload* with the coverage marker on every collection-bearing parent.

    ``cover_all`` states every collection covered, the meaning an omitted list had before
    the marker. Without it the marker names only the collections the payload carries, as
    the exporter does for a reader payload.
    """
    return _mark(deepcopy(payload), FAMILIES[family], cover_all)


def covered(family: str, payload: dict) -> dict:
    """Return the decoded section of a reader that covered every collection of *family*."""
    return decode_section(family, exported(family, payload))


def _mark(node: dict, collections: dict, cover_all: bool) -> dict:
    if not collections:
        return node
    for name, body in collections.items():
        if body["kind"] == "list":
            for entry in node.get(name) or []:
                _mark(entry, body["collections"], cover_all)
    node[MARKER] = [name for name in collections if cover_all or isinstance(node.get(name), list)]
    return node


def covered_sections(served: dict) -> dict:
    """Return *served* with each ``ok`` family section decoded as a reader that covered everything.

    For fakes that stand in for a client method: the real client returns decoded sections.
    """
    return {
        key: covered(key, value)
        if key in FAMILIES and isinstance(value, dict) and value.get("status") == "ok"
        else value
        for key, value in served.items()
    }


def serve_device_state(client, served: dict) -> None:
    """Configure the device-state action with decoded sections for the requested families."""

    async def _read(device_name, families, *, timeout=None):
        return {
            "atomic": True,
            "device-name": device_name,
            **covered_sections({wire: served.get(wire, {"status": "unsupported"}) for wire in families}),
        }

    client.run_device_state_read.side_effect = _read


def section_client() -> AsyncMock:
    """An NSO client stand-in whose ``get_device_state_section`` decodes its configured section.

    Set ``get_device_state_section.return_value`` to the reader payload, as before; the call
    returns what the real client returns for it. A configured ``side_effect`` replaces this.
    """
    client = AsyncMock()  # mock-ok: an export-section stand-in for the RESTCONF client

    def _decoded(_device_name, wire_family):
        return covered_sections({wire_family: client.get_device_state_section.return_value})[wire_family]

    client.get_device_state_section.side_effect = _decoded
    return client


def exported_sections(served: dict, *, cover_all: bool = True) -> dict:
    """Return *served* with each ``ok`` family section in wire form (see ``exported``)."""
    return {
        key: exported(key, value, cover_all=cover_all)
        if key in FAMILIES and isinstance(value, dict) and value.get("status") == "ok"
        else value
        for key, value in served.items()
    }
