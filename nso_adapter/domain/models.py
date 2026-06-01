# SPDX-License-Identifier: Apache-2.0
"""Domain models — pure dataclasses, no DB dependency."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class InterfaceAttr:
    description: str | None
    enabled: bool | None


@dataclass
class Interface:
    name: str
    nso: InterfaceAttr
    netbox: InterfaceAttr
    is_drifted: bool = False
    synced_at: datetime | None = None


@dataclass
class Device:
    id: int
    nso_instance: str
    nso_device_name: str
    netbox_device_id: int | None
    ned_id: str | None
    sync_status: str
    last_synced_at: datetime | None
    interfaces: list[Interface] = field(default_factory=list)
