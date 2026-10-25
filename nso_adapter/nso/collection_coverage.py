# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Decode the ``covered-collections`` marker of network-state-export sections (#1833).

RESTCONF omits an empty YANG list, so the wire alone cannot tell a collection the reader
read and found empty from one the reader did not read. The exporter states coverage per
parent (the section root and every collection-bearing list entry): the marker leaf-list
names the immediate child collections the reader covered.

``collection_manifest.json`` is a byte-identical copy of the manifest that the packages
generator writes from the export YANG, the one source for the collection tree. This module
is the one decoder every export consumer reads through:

* covered and omitted: restored as ``[]`` (complete, nothing configured);
* covered and present: kept, entries decoded recursively;
* not covered, omitted or present: removed (unavailable). The exporter serves a collection
  without a statement when its reader skipped configuration it does not export;
* an unknown or duplicate marker name, a marker on a parent that bears no collection, or a
  malformed covered value: :class:`CollectionCoverageError`.

The marker is transport metadata and never leaves this module. :func:`admit_coverage` is
the consumer side: a consumer names the collections it needs, and a ``Present`` read that
does not cover one becomes ``Unavailable(not_covered)``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path

import structlog

from nso_adapter.nso.read_outcome import Present, ReadOutcome, Unavailable, UnavailableReason

logger = structlog.get_logger(__name__)

MANIFEST_PATH = Path(__file__).with_name("collection_manifest.json")


class CollectionCoverageError(RuntimeError):
    """A device-state section breaks the collection coverage contract."""


def _load_collections(raw: object, where: str) -> dict[str, dict]:
    if not isinstance(raw, dict):
        raise CollectionCoverageError(f"manifest {where}: collections must be an object")
    out: dict[str, dict] = {}
    for name, body in raw.items():
        if not isinstance(body, dict) or body.get("kind") not in ("list", "leaf-list"):
            raise CollectionCoverageError(f"manifest {where}/{name}: kind must be list or leaf-list")
        if body["kind"] == "leaf-list":
            if set(body) != {"kind"}:
                raise CollectionCoverageError(f"manifest {where}/{name}: a leaf-list carries only its kind")
            out[name] = {"kind": "leaf-list"}
            continue
        keys = body.get("keys")
        if set(body) != {"kind", "keys", "collections"} or not isinstance(keys, list) or not keys:
            raise CollectionCoverageError(f"manifest {where}/{name}: a list carries kind, keys and collections")
        out[name] = {
            "kind": "list",
            "keys": tuple(keys),
            "collections": _load_collections(body["collections"], f"{where}/{name}"),
        }
    return out


def load_manifest(text: str) -> tuple[str, dict[str, dict[str, dict]]]:
    """Parse and validate manifest text into ``(marker, {family: collections})``."""
    raw = json.loads(text)
    if not isinstance(raw, dict) or set(raw) != {"marker", "families"}:
        raise CollectionCoverageError("manifest: want exactly the marker and families keys")
    marker, families = raw["marker"], raw["families"]
    if not isinstance(marker, str) or not marker or not isinstance(families, dict) or not families:
        raise CollectionCoverageError("manifest: the marker must be a name and families an object")
    out: dict[str, dict[str, dict]] = {}
    for family, body in families.items():
        if not isinstance(body, dict) or set(body) != {"collections"}:
            raise CollectionCoverageError(f"manifest {family}: want exactly the collections key")
        out[family] = _load_collections(body["collections"], family)
    return marker, out


MARKER, FAMILIES = load_manifest(MANIFEST_PATH.read_text(encoding="utf-8"))


def collection_paths(family: str) -> dict[str, str]:
    """Return ``{path: kind}`` for every collection of *family*; a path joins names with ``/``."""
    paths: dict[str, str] = {}

    def visit(collections: dict[str, dict], prefix: str) -> None:
        for name, body in collections.items():
            path = f"{prefix}{name}"
            paths[path] = body["kind"]
            if body["kind"] == "list":
                visit(body["collections"], f"{path}/")

    visit(_family(family), "")
    return paths


def validate_paths(family: str, paths: Iterable[str]) -> tuple[str, ...]:
    """Return *paths* as a tuple after checking that each names a collection of *family*."""
    declared = tuple(paths)
    known = collection_paths(family)
    unknown = sorted(set(declared) - set(known))
    if not declared or unknown:
        raise ValueError(f"{family}: collection paths must be non-empty manifest paths (unknown: {unknown})")
    return declared


def _family(family: str) -> dict[str, dict]:
    collections = FAMILIES.get(family)
    if collections is None:
        raise CollectionCoverageError(f"{family}: not a family of the collection manifest")
    return collections


def empty_section(family: str) -> dict:
    """Return the decoded section of a reader that covered every root collection and found none."""
    return {name: [] for name in _family(family)}


def decode_section(family: str, section: object) -> dict:
    """Return *section* with covered omitted collections restored and every marker removed."""
    if not isinstance(section, dict):
        raise CollectionCoverageError(f"{family}: a section must be an object")
    return _decode_parent(section, _family(family), family)


def decode_sections(families: Iterable[str], served: Mapping) -> dict:
    """Return a copy of *served* with each object section named in *families* decoded.

    A non-object section stays as served, and a section that breaks the coverage contract
    becomes None, so the caller's classifier names only that family malformed.
    """
    out = dict(served)
    for family in families:
        if isinstance(out.get(family), dict):
            try:
                out[family] = decode_section(family, out[family])
            except CollectionCoverageError:
                logger.warning("collection_coverage.section_rejected", family=family)
                out[family] = None
    return out


def _decode_parent(node: dict, collections: dict[str, dict], where: str) -> dict:
    covered = _covered(node, collections, where)
    out = {key: value for key, value in node.items() if key != MARKER}
    for key, value in out.items():
        if key not in collections:
            _refuse_marker(value, f"{where}/{key}")
    for name, body in collections.items():
        if name not in covered:
            out.pop(name, None)  # served without a statement: unavailable, never empty
            continue
        value = node.get(name, [])
        if not isinstance(value, list):
            raise CollectionCoverageError(f"{where}/{name}: a collection must be an array")
        if body["kind"] == "leaf-list":
            if any(item is None or isinstance(item, (dict, list)) for item in value):
                raise CollectionCoverageError(f"{where}/{name}: a leaf-list value must be a scalar")
            out[name] = list(value)
            continue
        out[name] = [_decode_entry(entry, body, f"{where}/{name}") for entry in value]
    return out


def _refuse_marker(value: object, where: str) -> None:
    """Refuse the marker below a value that is not a collection: only parents of collections carry it."""
    if isinstance(value, dict):
        if MARKER in value:
            raise CollectionCoverageError(f"{where}: the {MARKER} marker sits on a parent with no collection")
        for key, item in value.items():
            _refuse_marker(item, f"{where}/{key}")
    elif isinstance(value, list):
        for item in value:
            _refuse_marker(item, where)


def _decode_entry(entry: object, body: dict, where: str) -> dict:
    if not isinstance(entry, dict):
        raise CollectionCoverageError(f"{where}: a list entry must be an object")
    return _decode_parent(entry, body["collections"], where)


def _covered(node: dict, collections: dict[str, dict], where: str) -> set[str]:
    if MARKER not in node:
        return set()
    if not collections:
        raise CollectionCoverageError(f"{where}: the {MARKER} marker sits on a parent with no collection")
    names = node[MARKER]
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise CollectionCoverageError(f"{where}: the {MARKER} marker must be an array of names")
    if len(set(names)) != len(names):
        raise CollectionCoverageError(f"{where}: the {MARKER} marker repeats a name")
    if set(names) - set(collections):
        # The names are the server's own values; the parent path is ours and says enough.
        raise CollectionCoverageError(f"{where}: the {MARKER} marker names a collection this parent does not bear")
    return set(names)


def uncovered_paths(family: str, data: Mapping, paths: Iterable[str]) -> list[str]:
    """Return each of *paths* that some existing parent of *data* (a decoded section) does not cover.

    A covered collection is always present after :func:`decode_section`, so presence is
    coverage. A parent list with no entries needs no statement below it.
    """
    known = collection_paths(family)
    uncovered = []
    for path in paths:
        if path not in known:
            raise ValueError(f"{family}: {path} is not a manifest collection path")
        if not _path_covered(data, path.split("/")):
            uncovered.append(path)
    return uncovered


def _path_covered(node: Mapping, names: list[str]) -> bool:
    head, rest = names[0], names[1:]
    if head not in node:
        return False
    value = node[head]
    if not isinstance(value, list) or (rest and not all(isinstance(entry, dict) for entry in value)):
        raise CollectionCoverageError(f"{head}: a decoded collection must be an array of objects")
    return not rest or all(_path_covered(entry, rest) for entry in value)


def admit_coverage(outcome: ReadOutcome, family: str, requires: Iterable[str], *, consumer: str) -> ReadOutcome:
    """Return *outcome*, or ``Unavailable(not_covered)`` when a ``Present`` read misses a required collection."""
    if not isinstance(outcome, Present):
        return outcome
    missing = uncovered_paths(family, outcome.data, requires)
    if not missing:
        return outcome
    logger.info(f"{consumer}.refresh.not_covered", family=family, collections=missing)
    return Unavailable(UnavailableReason.not_covered)
