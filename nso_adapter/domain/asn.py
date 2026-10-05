# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Canonical AS number identities."""

from nso_adapter.nso.neds import ned_family

_SOURCE_WITHOUT_AS_FAMILIES = frozenset({"junos", "timos"})
_NO_NED_CONTEXT = object()


def parse_asn(asn: object) -> int:
    """Return a uint32 AS number from asplain or asdot notation."""
    if not isinstance(asn, (str, int)) or isinstance(asn, bool):
        raise ValueError("BGP ASN is not a valid uint32 AS number")
    # RFC 5396 section 1 defines AS notation; section 2 defines the asplain and asdot+ formats.
    parts = str(asn).split(".")
    if len(parts) in (1, 2) and all(
        p.isascii() and p.isdecimal() and (p == "0" or not p.startswith("0")) for p in parts
    ):
        limit = 4294967295 if len(parts) == 1 else 65535
        try:
            values = [int(p) for p in parts]
        except ValueError:
            values = []
        if values and all(v <= limit for v in values):
            return values[0] if len(values) == 1 else values[0] * 65536 + values[1]
    raise ValueError("BGP ASN is not a valid uint32 AS number")


def redistribution_source_identity(protocol: str, reference: str) -> str:
    """Compare a BGP source instance by AS number and other instances by their reference."""
    return str(parse_asn(reference)) if protocol == "bgp" and reference != "" else reference


def checked_device_source_ref(value: object, table: str, row_id: object, field: str) -> str:
    """Return a device-read BGP source as asplain; Junos and TiMOS report a source without an AS as ""."""
    return "" if value == "" else str(checked_asn(value, table, row_id, field))


class AsnRuleViolation(Exception):
    """Refuse invalid AS content with its stored row or device-read location."""

    def __init__(self, table: str, row_id: object, field: str, value: object, *, other_row_id: object = None):
        message = f"{table} row {row_id!r} field {field}: AS value violates RFC 5396"
        self.table, self.row_id, self.field, self.value = table, row_id, field, value
        detail = {"table": table, "row_id": row_id, "field": field}
        if other_row_id is not None:
            detail["other_row_id"] = other_row_id
        super().__init__(message)
        self.error: dict = {"code": "asn_rule_violation", "message": message, "detail": detail}

    def include_degraded_surfaces(self, surfaces: list[str]) -> None:
        """Keep the complete failed-surface list without rejected AS content."""
        detail = self.error["detail"]
        detail["degraded_surfaces"] = sorted(set(detail.get("degraded_surfaces", [])) | set(surfaces))


class BgpSourceAsRequired(AsnRuleViolation):
    """Refuse a BGP source without an AS on a NED that requires one."""

    def __init__(self, ned_id: str | None):
        super().__init__("redistribution_intent", None, "source_ref", "")
        message = f"{ned_id or 'no learned NED'}: BGP redistribution requires the source AS"
        self.args = (message,)
        self.error = {"code": "bgp_source_as_required", "message": message, "detail": {"ned_id": ned_id}}


def asn_refusal_detail(exc: AsnRuleViolation) -> dict:
    """Return the authored refusal envelope without rejected AS content."""
    if type(exc) not in (AsnRuleViolation, BgpSourceAsRequired):
        raise TypeError("exc must be an AsnRuleViolation")
    return exc.error


def checked_asn(value: object, table: str, row_id: object, field: str) -> int:
    """Parse an AS value or refuse it at its content boundary."""
    try:
        return parse_asn(value)
    except ValueError:
        refused = AsnRuleViolation(table, row_id, field, value)
    raise refused


def checked_intent_source_ref(value: str, ned_id: str | None) -> str:
    """Return the canonical BGP source AS, or an empty source on Junos and TiMOS."""
    if value == "":
        if ned_family(ned_id or "") in _SOURCE_WITHOUT_AS_FAMILIES:
            return ""
        raise BgpSourceAsRequired(ned_id)
    return str(checked_asn(value, "redistribution_intent", None, "source_ref"))


def asn_row_identity(table: str, row: dict) -> tuple | None:
    """Validate AS fields and return a canonical root identity when applicable."""
    row_id = row.get("id")
    if table in {"bgp_router_intent", "device_bgp_router"}:
        return (checked_asn(row.get("asn"), table, row_id, "asn"),)
    if table in {"bgp_peer_intent", "device_bgp_peer", "device_bgp_peer_group"}:
        for field in ("remote_as", "local_as"):
            if row.get(field) is not None:
                checked_asn(row[field], table, row_id, field)
    if table in {"redistribution_intent", "device_redistribution"}:
        destination = row.get("dest_ref", "")
        source = row.get("source_ref", "")
        if row.get("dest_protocol") == "bgp":
            separator = ":" if table == "redistribution_intent" else "/"
            parts = destination.split(separator, 2)
            valid_lengths = (3,) if table == "redistribution_intent" else (2, 3)
            if len(parts) not in valid_lengths:
                raise AsnRuleViolation(table, row_id, "dest_ref", destination)
            destination = (checked_asn(parts[0], table, row_id, "dest_ref"), *parts[1:])
        if row.get("source_protocol") == "bgp":
            if table == "device_redistribution":
                source = checked_device_source_ref(source, table, row_id, "source_ref")
            elif source != "":
                source = checked_asn(source, table, row_id, "source_ref")
        return (row.get("dest_protocol"), destination, row.get("source_protocol"), source)
    return None


def validate_asn_rows(table: str, rows: list[dict], *, ned_id: object = _NO_NED_CONTEXT) -> None:
    """Refuse malformed values and colliding canonical AS identities."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        identity = asn_row_identity(table, row)
        if table == "redistribution_intent" and row.get("source_protocol") == "bgp" and ned_id is not _NO_NED_CONTEXT:
            if ned_id is not None and not isinstance(ned_id, str):
                raise TypeError("ned_id must be a string or None")
            checked_intent_source_ref(row.get("source_ref", ""), ned_id)
        if identity is None:
            continue
        previous = seen.get(identity)
        if previous is not None:
            if all(previous.get(field) == row.get(field) for field in ("asn", "dest_ref", "source_ref")):
                continue
            field = "asn" if "asn" in row else "dest_ref" if previous["dest_ref"] != row["dest_ref"] else "source_ref"
            raise AsnRuleViolation(table, row.get("id"), field, row.get(field), other_row_id=previous.get("id"))
        seen[identity] = row


def validate_source_as_numbers(entries: list[dict], table: str, row_id: object) -> None:
    """Refuse malformed or colliding device-read BGP sources within one destination."""
    seen: dict[int, object] = {}
    for entry in entries:
        value = entry.get("source-ref", "")
        if entry.get("source-protocol") != "bgp" or value == "":
            continue
        identity = checked_asn(value, table, row_id, "source-ref")
        if identity in seen and seen[identity] != value:
            raise AsnRuleViolation(table, row_id, "source-ref", value)
        seen[identity] = value
