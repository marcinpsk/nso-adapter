# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Strict device projections used by family mirrors and observations."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import ClassVar, TypeVar

from pydantic import AliasGenerator, BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from nso_adapter.nso.shape import as_list, require_vlan_id, wire_int

# These fields belong to another projection of the same exported inventory.
EXCLUDED_WIRE_FIELDS: dict[str, tuple[str, ...]] = {}


class UnprojectableEntry(BaseModel):
    model_config = ConfigDict(strict=True)

    index: int
    reason: str


class ObservationCoverage(BaseModel):
    model_config = ConfigDict(strict=True)

    attributes: list[str]
    not_comparable: list[str] = Field(default_factory=list, exclude_if=lambda value: not value)


class DeviceEntry(BaseModel):
    model_config = ConfigDict(
        strict=True,
        alias_generator=AliasGenerator(validation_alias=lambda name: name.replace("_", "-")),
        populate_by_name=True,
    )

    present: list[str] = Field(default_factory=list)
    _source_index: int = PrivateAttr(default=0)
    identity: ClassVar[tuple[str, ...]] = ()
    identity_defaults: ClassVar[dict[str, str | int]] = {}
    children: ClassVar[dict[str, type[DeviceEntry]]] = {}
    containers: ClassVar[dict[str, type[DeviceEntry]]] = {}
    wire_aliases: ClassVar[dict[str, tuple[str, ...]]] = {}
    allow_empty_identity: ClassVar[tuple[str, ...]] = ("vrf", "process_tag", "next_hop", "source_ref")
    converters: ClassVar[dict[str, Callable[[object], object]]] = {}
    integers: ClassVar[tuple[str, ...]] = ()
    vlans: ClassVar[tuple[str, ...]] = ()
    credentials: ClassVar[tuple[str, ...]] = ()


ModelT = TypeVar("ModelT", bound=BaseModel)
EntryT = TypeVar("EntryT", bound=DeviceEntry)


def observation_payload(model: ModelT) -> ModelT:  # noqa: UP047
    """Copy a projection without retaining the mirror's credential values."""
    changes: dict[str, object] = {}
    for name in type(model).model_fields:
        value = getattr(model, name)
        if isinstance(model, DeviceEntry) and name in model.credentials:
            changes[name] = None
        elif isinstance(value, BaseModel):
            changes[name] = observation_payload(value)
        elif isinstance(value, list) and any(isinstance(item, BaseModel) for item in value):
            changes[name] = [observation_payload(item) if isinstance(item, BaseModel) else item for item in value]
    return model.model_copy(update=changes)


def entry_payload(entry: DeviceEntry) -> dict:
    """Return projected wire fields for a mirror, without presence metadata."""
    return {
        str(type(entry).model_fields[name].validation_alias or name): _wire_value(getattr(entry, name))
        for name in type(entry).model_fields
        if name != "present"
        and name in entry.model_fields_set
        and not any(
            name == f"{secret}_{suffix}" for secret in entry.credentials for suffix in ("present", "fingerprint")
        )
    }


def _wire_value(value):
    if isinstance(value, DeviceEntry):
        return entry_payload(value)
    if isinstance(value, list):
        if all(isinstance(item, DeviceEntry) for item in value):
            value = sorted(value, key=lambda item: item._source_index)
        return [_wire_value(item) for item in value]
    return value


def _entry_key(entry: DeviceEntry) -> tuple:
    values = [
        entry.identity_defaults.get(field) if getattr(entry, field) is None else getattr(entry, field)
        for field in entry.identity
    ]
    return tuple((value is not None, value) for value in values)


def _input_values(item: dict, model: type[DeviceEntry]) -> tuple[dict, list[str], list[str], list[str]]:
    values = {}
    conflicts = []
    supported = set(EXCLUDED_WIRE_FIELDS.get(model.__name__, ()))
    for name, field in model.model_fields.items():
        if name == "present" or any(
            name == f"{secret}_{suffix}" for secret in model.credentials for suffix in ("present", "fingerprint")
        ):
            continue
        wire = str(field.validation_alias or name)
        candidates = model.wire_aliases.get(name, (wire,))
        supported.update(candidates)
        aliases = [candidate for candidate in candidates if candidate in item]
        if aliases:
            values[name] = item[aliases[0]]
            if any(
                json.dumps(item[alias], sort_keys=True) != json.dumps(values[name], sort_keys=True)
                for alias in aliases[1:]
            ):
                conflicts.append(name)
    return values, sorted(values), conflicts, sorted(set(item) - supported)


def _project_children(values: dict, model: type[DeviceEntry], location: str, index: int) -> list[UnprojectableEntry]:
    invalid = []
    for name, child in model.children.items():
        if name in values and values[name] is not None:
            values[name], child_invalid = project_entries(values[name], child, f"{location}.{name}")
            invalid.extend(child_invalid)
    for name, child in model.containers.items():
        if name not in values or values[name] is None:
            continue
        if not isinstance(values[name], dict):
            invalid.append(UnprojectableEntry(index=index, reason=f"{location}.{name}: expected object"))
            values[name] = None
            continue
        entries, child_invalid = project_entries(values[name], child, f"{location}.{name}")
        invalid.extend(child_invalid)
        values[name] = entries[0] if entries else None
    return invalid


def _validate_entry(  # noqa: UP047
    values: dict, present: list[str], model: type[EntryT]
) -> tuple[EntryT | None, str | None]:
    try:
        for name, converter in model.converters.items():
            if name in values and values[name] is not None:
                values[name] = converter(values[name])
        for name in model.integers:
            if name in values and values[name] is not None:
                values[name] = wire_int(values[name])
        for name in model.vlans:
            if name in values and values[name] is not None:
                values[name] = require_vlan_id(wire_int(values[name]), "projection", "entry", name)
        entry = model.model_validate({**values, "present": present})
        if any(getattr(entry, key) == "" for key in model.identity if key not in model.allow_empty_identity):
            return None, "invalid identity"
    except ValidationError as exc:
        fields = sorted(
            {
                str(error["loc"][0]).replace("-", "_") if error["loc"] else "entry"
                for error in exc.errors(include_input=False)
            }
        )
        return None, "invalid " + ", ".join(fields)
    except (TypeError, ValueError):
        return None, "invalid integer or VLAN id"
    return entry, None


def project_entries(  # noqa: UP047
    raw: object, model: type[EntryT], path: str
) -> tuple[list[EntryT], list[UnprojectableEntry]]:
    """Project each keyed row and report invalid rows, including nested rows."""
    entries: list[EntryT] = []
    invalid: list[UnprojectableEntry] = []
    seen: set[tuple] = set()
    for index, item in enumerate(as_list(raw)):
        location = f"{path}[{index}]"
        if not isinstance(item, dict):
            invalid.append(UnprojectableEntry(index=index, reason=f"{location}: expected object"))
            continue
        values, present, conflicts, unsupported = _input_values(item, model)
        if unsupported:
            invalid.append(
                UnprojectableEntry(index=index, reason=f"{location}: unsupported fields: " + ", ".join(unsupported))
            )
        if conflicts:
            invalid.append(
                UnprojectableEntry(index=index, reason=f"{location}: conflicting aliases for " + ", ".join(conflicts))
            )
            continue
        invalid.extend(_project_children(values, model, location, index))
        entry, problem = _validate_entry(values, present, model)
        if entry is None:
            invalid.append(UnprojectableEntry(index=index, reason=f"{location}: {problem}"))
            continue
        entry._source_index = index
        key = _entry_key(entry)
        if key in seen:
            invalid.append(UnprojectableEntry(index=index, reason=f"{location}: duplicate identity"))
            continue
        seen.add(key)
        entries.append(entry)
    entries.sort(key=lambda entry: (_entry_key(entry), json.dumps(entry.model_dump(), sort_keys=True)))
    return entries, invalid


class VlanEntry(DeviceEntry):
    identity = ("vlan_id",)
    vlans = ("vlan_id",)

    vlan_id: int
    name: str | None = None


class VlanDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    vlans: list[VlanEntry] | None
    unprojectable: list[UnprojectableEntry]


def project_vlans(data: dict | None) -> VlanDocument:
    data = data or {}
    keys = [key for key in ("vlan", "vlans") if key in data]
    present = ["vlans"] if keys else []
    raw = data[keys[0]] if keys else []
    if any(json.dumps(data[key], sort_keys=True) != json.dumps(raw, sort_keys=True) for key in keys[1:]):
        invalid = [UnprojectableEntry(index=0, reason="vlan: conflicting collection aliases")]
        return VlanDocument(present=present, vlans=[], unprojectable=invalid)
    if raw is None:
        return VlanDocument(present=present, vlans=None, unprojectable=[])
    entries, invalid = project_entries(raw, VlanEntry, "vlan")
    return VlanDocument(present=present, vlans=entries, unprojectable=invalid)
