# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device projections shared by service mirrors and family observations."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nso_adapter.domain.read_projection import DeviceEntry, UnprojectableEntry, project_entries
from nso_adapter.secrets.refs import require_secret_fingerprint


class BfdEntry(DeviceEntry):
    identity = ("interface_name",)
    integers = ("min_tx", "min_rx", "multiplier")

    interface_name: str
    bound_port: str | None = None
    min_tx: int | None = None
    min_rx: int | None = None
    multiplier: int | None = None
    micro_bfd: bool | None = None
    enabled: bool | None = None


class BfdDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    interfaces: list[BfdEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_bfd(data: dict | None) -> BfdDocument:
    data = data or {}
    interfaces, invalid = project_entries(data.get("interface"), BfdEntry, "interface")
    return BfdDocument(
        interfaces=None if "interface" in data and data["interface"] is None else interfaces,
        present=["interfaces"] if "interface" in data else [],
        unprojectable=invalid,
    )


class L2SapEntry(DeviceEntry):
    identity = ("sap_id",)
    integers = ("outer_tag", "inner_tag")

    sap_id: str
    port: str | None = None
    outer_tag: int | None = None
    inner_tag: int | None = None


class L2ServiceEntry(DeviceEntry):
    identity = ("service_name",)
    integers = ("service_id",)
    children = {"saps": L2SapEntry}

    service_name: str
    service_type: str | None = None
    service_id: int | None = None
    saps: list[L2SapEntry] | None = Field(default=None, validation_alias="sap")


class L2ServiceDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    services: list[L2ServiceEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_l2_services(data: dict | None) -> L2ServiceDocument:
    data = data or {}
    services, invalid = project_entries(data.get("service"), L2ServiceEntry, "service")
    return L2ServiceDocument(
        services=None if "service" in data and data["service"] is None else services,
        present=["services"] if "service" in data else [],
        unprojectable=invalid,
    )


class LoggingHostEntry(DeviceEntry):
    identity = ("address",)
    integers = ("port",)

    address: str
    port: int | None = None
    severity: str | None = None
    facility: str | None = None
    transport: str | None = None
    vrf: str | None = None
    source: str | None = None


class LoggingLevelsEntry(DeviceEntry):
    console_severity: str | None = None
    monitor_severity: str | None = None
    module_severity: str | None = None


class LoggingDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    hosts: list[LoggingHostEntry] | None
    local_levels: LoggingLevelsEntry | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_logging(data: dict | None) -> LoggingDocument:
    data = data or {}
    hosts, invalid = project_entries(data.get("host"), LoggingHostEntry, "host")
    raw_levels = data.get("local-levels")
    levels, invalid_levels = project_entries(
        [raw_levels] if raw_levels is not None else None, LoggingLevelsEntry, "local-levels"
    )
    invalid.extend(invalid_levels)
    return LoggingDocument(
        hosts=None if "host" in data and data["host"] is None else hosts,
        local_levels=levels[0] if levels else None,
        present=sorted(name for wire, name in (("host", "hosts"), ("local-levels", "local_levels")) if wire in data),
        unprojectable=invalid,
    )


class SnmpCommunityEntry(DeviceEntry):
    identity = ("name",)

    name: str
    access: str | None = None
    acl: str | None = None
    has_secret: bool | None = None

    @field_validator("name")
    @classmethod
    def validate_fingerprint(cls, value: str) -> str:
        return require_secret_fingerprint(value)


class SnmpUserEntry(DeviceEntry):
    identity = ("username",)

    username: str
    has_auth_secret: bool | None = None
    has_priv_secret: bool | None = None


class SnmpHostEntry(DeviceEntry):
    identity = ("address",)
    integers = ("port",)

    address: str
    version: str | None = None
    notify_type: str | None = None
    port: int | None = None
    user: str | None = None

    @model_validator(mode="after")
    def validate_security_user(self) -> SnmpHostEntry:
        if self.user is not None and self.version != "3":
            raise ValueError("user requires SNMP version 3")
        return self


class SnmpSystemEntry(DeviceEntry):
    location: str | None = None
    contact: str | None = None


class SnmpDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    communities: list[SnmpCommunityEntry] | None
    users: list[SnmpUserEntry] | None
    hosts: list[SnmpHostEntry] | None
    system: SnmpSystemEntry
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_snmp(data: dict | None) -> SnmpDocument:
    data = data or {}
    communities, invalid = project_entries(data.get("community"), SnmpCommunityEntry, "community")
    users, invalid_users = project_entries(data.get("v3-user"), SnmpUserEntry, "v3-user")
    hosts, invalid_hosts = project_entries(data.get("host"), SnmpHostEntry, "host")
    system_data = {key: data[key] for key in ("location", "contact") if key in data}
    system, invalid_system = project_entries(system_data, SnmpSystemEntry, "system")
    invalid.extend([*invalid_users, *invalid_hosts, *invalid_system])
    return SnmpDocument(
        communities=None if "community" in data and data["community"] is None else communities,
        users=None if "v3-user" in data and data["v3-user"] is None else users,
        hosts=None if "host" in data and data["host"] is None else hosts,
        system=system[0] if system else SnmpSystemEntry(),
        present=sorted(
            name
            for wire, name in (("community", "communities"), ("v3-user", "users"), ("host", "hosts"))
            if wire in data
        ),
        unprojectable=invalid,
    )


class StaticRouteEntry(DeviceEntry):
    identity = ("vrf", "prefix", "next_hop")
    identity_defaults = {"vrf": "", "next_hop": ""}
    integers = ("metric", "tag")

    vrf: str | None = None
    prefix: str
    next_hop: str | None = None
    interface_next_hop: str | None = None
    next_hop_vrf: str | None = None
    metric: int | None = None
    permanent: bool | None = None
    tag: int | None = None
    name: str | None = None


class StaticRouteDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    routes: list[StaticRouteEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_static_routes(data: dict | None) -> StaticRouteDocument:
    data = data or {}
    routes, invalid = project_entries(data.get("route"), StaticRouteEntry, "route")
    return StaticRouteDocument(
        routes=None if "route" in data and data["route"] is None else routes,
        present=["routes"] if "route" in data else [],
        unprojectable=invalid,
    )
