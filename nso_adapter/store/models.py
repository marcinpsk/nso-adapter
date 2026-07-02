# SPDX-License-Identifier: Apache-2.0
"""ORM models for nso-adapter.

Schema aligns with docs/nso-adapter.md §5:
  device, managed_scope, interfaces, interface_attr_state, jobs
"""

from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class MappingStatus(str, enum.Enum):
    mapped = "mapped"
    unmatched_device = "unmatched_device"
    unmatched_interfaces = "unmatched_interfaces"


class LastSyncStatus(str, enum.Enum):
    succeeded = "succeeded"
    # Interface sync succeeded but one or more routing surfaces failed to read from NSO
    # (their last-known rows are kept, so they may be stale). See Device.degraded_surfaces.
    partial = "partial"
    failed = "failed"


class JobStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class JobType(str, enum.Enum):
    sync = "sync"
    # value MUST equal the name: Enum(JobType) persists the member NAME, so a divergent
    # value silently misses a raw-value filter/write or DataErrors (was "detect-drift").
    detect_drift = "detect_drift"
    connect = "connect"
    apply = "apply"  # Phase 2: push accepted intent to NSO
    removal = "removal"  # async PUT-replace to revert removed intent (see core/removal.py)
    provision = "provision"  # async device onboarding into NSO (see core/onboarding.provision_nso_device)


class SyncState(str, enum.Enum):
    # Phase 1 statuses (device-config-layer, no intent ownership)
    imported = "imported"  # NetBox value matches last import from NSO
    changed = "changed"  # NSO now reports a value differing from NetBox (out-of-band change)
    error = "error"  # could not be evaluated
    unknown = "unknown"  # not yet synced
    # Phase 2 statuses (intent ownership established via apply)
    accepted = "accepted"  # operator accepted value in plugin; not yet deployed to NSO
    deploying = "deploying"  # apply job is actively writing this attribute to NSO
    in_sync = "in_sync"  # device matches deployed intent
    apply_failed = "apply_failed"  # NSO commit failed; last intent value unchanged
    drifted = "drifted"  # device has changed since intent was deployed


class ActiveAddress(str, enum.Enum):
    """Which management address NSO is currently dialing for a device.

    Stored as a plain String column (the house style for incremental tables — only the
    baseline uses native PG enums), with these constants for code-level clarity.
    """

    primary = "primary"  # NSO is on the device's primary management IP
    oob = "oob"  # failed over to the out-of-band IP


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nso_instance: Mapped[str] = mapped_column(String(128))
    nso_device_name: Mapped[str] = mapped_column(String(256), index=True)
    netbox_device_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    ned_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    # Last platform version learned from a capability probe — lets the capability
    # cache resolve this device's (ned_id, sw_version) key WITHOUT a live probe.
    sw_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    mapping_status: Mapped[MappingStatus] = mapped_column(Enum(MappingStatus), default=MappingStatus.mapped)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sync_status: Mapped[LastSyncStatus | None] = mapped_column(Enum(LastSyncStatus), nullable=True)
    # When last_sync_status == partial: the routing surfaces that failed to read from NSO on the
    # last sync (e.g. ["bgp", "ospf"]). Their mirror rows may be stale. NULL when nothing degraded.
    degraded_surfaces: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    managed_scope: Mapped[list[ManagedScope]] = relationship(
        "ManagedScope", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    interfaces: Mapped[list[DbInterface]] = relationship(
        "DbInterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    jobs: Mapped[list[Job]] = relationship("Job", back_populates="device", lazy="raise")
    settings: Mapped[DeviceSettings | None] = relationship(
        "DeviceSettings", back_populates="device", uselist=False, cascade="all, delete-orphan", lazy="raise"
    )
    failover: Mapped[DeviceFailover | None] = relationship(
        "DeviceFailover", back_populates="device", uselist=False, cascade="all, delete-orphan", lazy="raise"
    )
    lag_interfaces: Mapped[list[LagInterface]] = relationship(
        "LagInterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    ip_addresses: Mapped[list[InterfaceIpAddress]] = relationship(
        "InterfaceIpAddress", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_communities: Mapped[list[SnmpCommunity]] = relationship(
        "SnmpCommunity", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_v3_users: Mapped[list[SnmpV3User]] = relationship(
        "SnmpV3User", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_hosts: Mapped[list[SnmpHost]] = relationship(
        "SnmpHost", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_system_info: Mapped[SnmpSystemInfo | None] = relationship(
        "SnmpSystemInfo", back_populates="device", uselist=False, cascade="all, delete-orphan", lazy="raise"
    )
    logging_hosts: Mapped[list[DeviceLoggingHost]] = relationship(
        "DeviceLoggingHost", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_community_intents: Mapped[list[SnmpCommunityIntent]] = relationship(
        "SnmpCommunityIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_v3_user_intents: Mapped[list[SnmpV3UserIntent]] = relationship(
        "SnmpV3UserIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_host_intents: Mapped[list[SnmpHostIntent]] = relationship(
        "SnmpHostIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    snmp_system_info_intent: Mapped[SnmpSystemInfoIntent | None] = relationship(
        "SnmpSystemInfoIntent", back_populates="device", uselist=False, cascade="all, delete-orphan", lazy="raise"
    )
    static_routes: Mapped[list[DeviceStaticRoute]] = relationship(
        "DeviceStaticRoute", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    l2_saps: Mapped[list[DeviceL2Sap]] = relationship(
        "DeviceL2Sap", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    l2_sap_intents: Mapped[list[L2SapIntent]] = relationship(
        "L2SapIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    vlan_database: Mapped[list[DeviceVlan]] = relationship(
        "DeviceVlan", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    vlan_intents: Mapped[list[VlanIntent]] = relationship(
        "VlanIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    switchports: Mapped[list[DeviceSwitchport]] = relationship(
        "DeviceSwitchport", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    static_route_intents: Mapped[list[StaticRouteIntent]] = relationship(
        "StaticRouteIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    logging_host_intents: Mapped[list[LoggingHostIntent]] = relationship(
        "LoggingHostIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    isis_processes: Mapped[list[DeviceIsisProcess]] = relationship(
        "DeviceIsisProcess", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    isis_interfaces: Mapped[list[DeviceIsisInterface]] = relationship(
        "DeviceIsisInterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    bfd_interfaces: Mapped[list[DeviceBfdInterface]] = relationship(
        "DeviceBfdInterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    bfd_intents: Mapped[list[BfdIntent]] = relationship(
        "BfdIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    svis: Mapped[list[DeviceSvi]] = relationship(
        "DeviceSvi", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    svi_intents: Mapped[list[SviIntent]] = relationship(
        "SviIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    subinterfaces: Mapped[list[DeviceSubinterface]] = relationship(
        "DeviceSubinterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    subinterface_intents: Mapped[list[SubinterfaceIntent]] = relationship(
        "SubinterfaceIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    interface_mtus: Mapped[list[DeviceInterfaceMtu]] = relationship(
        "DeviceInterfaceMtu", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    interface_mtu_intents: Mapped[list[InterfaceMtuIntent]] = relationship(
        "InterfaceMtuIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    isis_interface_intents: Mapped[list[IsisInterfaceIntent]] = relationship(
        "IsisInterfaceIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    isis_process_intents: Mapped[list[IsisProcessIntent]] = relationship(
        "IsisProcessIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    isis_flex_algo_intents: Mapped[list[IsisFlexAlgoIntent]] = relationship(
        "IsisFlexAlgoIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    bgp_routers: Mapped[list[DeviceBgpRouter]] = relationship(
        "DeviceBgpRouter", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    bgp_router_intents: Mapped[list[BgpRouterIntent]] = relationship(
        "BgpRouterIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    route_policy_object_intents: Mapped[list[RoutePolicyObjectIntent]] = relationship(
        "RoutePolicyObjectIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    ospf_instances: Mapped[list[DeviceOspfInstance]] = relationship(
        "DeviceOspfInstance", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    ospf_interfaces: Mapped[list[DeviceOspfInterface]] = relationship(
        "DeviceOspfInterface", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    ospf_instance_intents: Mapped[list[OspfInstanceIntent]] = relationship(
        "OspfInstanceIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    ospf_interface_intents: Mapped[list[OspfInterfaceIntent]] = relationship(
        "OspfInterfaceIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    redistributions: Mapped[list[DeviceRedistribution]] = relationship(
        "DeviceRedistribution", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )
    redistribution_intents: Mapped[list[RedistributionIntent]] = relationship(
        "RedistributionIntent", back_populates="device", cascade="all, delete-orphan", lazy="raise"
    )


class ManagedScope(Base):
    """One row per managed attribute per device (e.g. device_id=1, attribute='description')."""

    __tablename__ = "managed_scope"
    __table_args__ = (UniqueConstraint("device_id", "attribute"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(Integer, ForeignKey("devices.id"), index=True)
    attribute: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    device: Mapped[Device] = relationship("Device", back_populates="managed_scope")


class DbInterface(Base):
    """Interface identity record — one row per (device, interface name)."""

    __tablename__ = "interfaces"
    __table_args__ = (UniqueConstraint("device_id", "name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(Integer, ForeignKey("devices.id"), index=True)
    name: Mapped[str] = mapped_column(String(256))
    netbox_interface_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    nso_if_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    # M27R: first-class logical-interface modeling. Empty/NULL for physical ports
    # and for Cisco/Junos (which keep the flat interface=physical model).
    parent_binding: Mapped[str | None] = mapped_column(String(256), nullable=True)
    kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    encap_tag: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vrf: Mapped[str | None] = mapped_column(String(256), nullable=True)
    service: Mapped[str | None] = mapped_column(String(256), nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="interfaces")
    attr_states: Mapped[list[InterfaceAttrState]] = relationship(
        "InterfaceAttrState", back_populates="interface_obj", cascade="all, delete-orphan"
    )
    intent: Mapped[list[InterfaceIntent]] = relationship(
        "InterfaceIntent", back_populates="interface_obj", cascade="all, delete-orphan"
    )


class InterfaceAttrState(Base):
    """Per-attribute sync_state state — one row per (interface, attribute)."""

    __tablename__ = "interface_attr_state"
    __table_args__ = (UniqueConstraint("interface_id", "attribute"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    interface_id: Mapped[int] = mapped_column(Integer, ForeignKey("interfaces.id"), index=True)
    attribute: Mapped[str] = mapped_column(String(64))
    nso_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    netbox_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Deployed intent is the single source of truth in InterfaceIntent (read by the
    # importer to decide Phase 1 vs Phase 2). There is intentionally no intent_value
    # cache here — a second copy is what caused the Phase-2 split-brain.
    sync_state: Mapped[SyncState] = mapped_column(Enum(SyncState), default=SyncState.unknown)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    interface_obj: Mapped[DbInterface] = relationship("DbInterface", back_populates="attr_states")


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        # At most one active (queued/running) job per device for the enqueue_job-managed
        # types. Closes the enqueue_job TOCTOU: the check-then-insert can't materialise two
        # active rows even under concurrent schedulers/SSE/API. Exclusions:
        #   * removal jobs are intentionally per-scope (enqueue_removal queues one each for
        #     bgp/isis/snmp/… on the same device), so they must NOT collide;
        #   * provision jobs carry device_id=NULL (NULLs are distinct) and dedup by context.
        # Partial index → terminal (succeeded/failed) jobs never block a fresh enqueue.
        Index(
            "uq_job_active_per_device",
            "device_id",
            unique=True,
            sqlite_where=text("status IN ('queued', 'running') AND job_type <> 'removal'"),
            postgresql_where=text("status IN ('queued', 'running') AND job_type <> 'removal'"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_type: Mapped[JobType] = mapped_column(Enum(JobType))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.queued)
    device_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("devices.id"), nullable=True)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Phase 2: apply jobs snapshot interface_intent rows here at job start
    context: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    # Layer B (durable worker): set when a worker claims the job; heartbeat_at is
    # refreshed periodically while it runs so a crashed/hung job can be detected
    # and requeued (or failed) on the next startup.
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    device: Mapped[Device | None] = relationship("Device", back_populates="jobs")


class DeviceSettings(Base):
    """Per-device settings from the plugin's NSODeviceManagement model (Phase 2).

    Written by ``PUT /devices/{id}/scope`` (the ``auto_apply`` field) and
    refreshed by the scope reconciler.  Kept separate from ``Device`` so all
    plugin-sourced settings are obviously plugin-sourced.
    """

    __tablename__ = "device_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(Integer, ForeignKey("devices.id"), unique=True, index=True)
    auto_apply: Mapped[bool] = mapped_column(Boolean, default=False)
    # Sync-from the device before each apply to clear NSO/device out-of-sync (a timed-out
    # or partial commit leaves the CDB inconsistent and the next apply is refused). Default
    # on; can be disabled per device for NEDs that already sync-on-connect.
    sync_before_apply: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    device: Mapped[Device] = relationship("Device", back_populates="settings")


class DeviceFailover(Base):
    """Management-IP failover state for a device — one row per device.

    The primary/OOB IPs are plugin-sourced (from NetBox) and the rest is failover oper
    state maintained by the scheduler probe loop. Kept separate from ``Device`` so the
    plugin-sourced inputs and the failover bookkeeping stay obviously failover-scoped.
    See the mgmt-IP-failover plan: NSO probes reachability, the adapter switches the
    device address between primary and OOB with hysteresis.
    """

    __tablename__ = "device_failover"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), unique=True, index=True
    )
    # Plugin-sourced management addresses (NetBox primary_ip / oob_ip, host only).
    primary_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    oob_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Which address NSO is currently dialing (ActiveAddress value).
    active_address: Mapped[str] = mapped_column(
        String(16), default=ActiveAddress.primary.value, server_default=text("'primary'")
    )
    # Hysteresis counters (reset to 0 on any real state transition).
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    consecutive_successes: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    # Operator set the NSO address to something we don't manage → stop fighting them.
    manual_override: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    # Proactive fallback-health: is the OOB path known-good while we're on primary? (None=unknown)
    oob_healthy: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    oob_health_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_probe_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_probe_result: Mapped[str | None] = mapped_column(String(16), nullable=True)  # "ok" | "fail"
    last_switch_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Staggering bookkeeping — per-address due times so the fleet isn't probed in lockstep.
    next_primary_probe_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_oob_probe_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    device: Mapped[Device] = relationship("Device", back_populates="failover")


class FailoverConfig(Base):
    """Global mgmt-IP failover tuning — a one-row singleton (id=1), operator-editable.

    The plugin's ``NSOFailoverSettings`` singleton pushes these here; the base-tick reads
    them **live** each run, so a change takes effect on the next tick without rescheduling
    APScheduler (the per-device ``next_*_probe_at`` due-times make "reschedule" a config read).
    When no row exists, the scheduler falls back to the static ``SchedulerConfig`` defaults.
    Defaults below are the prod values confirmed by the perf spike (see docs/failover-perf-spike.md):
    probe concurrently (the load lever, since an unreachable connect blocks ~probe_timeout).
    """

    __tablename__ = "failover_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Operator live on/off (distinct from the deployment-level ``enable_failover`` static flag
    # that registers the base-tick job at all). When False the tick is a no-op.
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    primary_probe_interval: Mapped[int] = mapped_column(Integer, default=15, server_default=text("15"))  # minutes
    oob_probe_interval: Mapped[int] = mapped_column(Integer, default=360, server_default=text("360"))  # minutes (6h)
    failure_threshold: Mapped[int] = mapped_column(Integer, default=3, server_default=text("3"))
    success_threshold: Mapped[int] = mapped_column(Integer, default=5, server_default=text("5"))
    probe_timeout: Mapped[float] = mapped_column(Float, default=10.0, server_default=text("10.0"))  # seconds
    # Spike-derived: probe due devices concurrently under this cap so simultaneously-down
    # devices cost ceil(n/concurrency)·timeout, not n·timeout.
    probe_concurrency: Mapped[int] = mapped_column(Integer, default=8, server_default=text("8"))
    # Safety belt: at most this many disruptive flips (set_address+disconnect+connect) per tick.
    max_flips_per_tick: Mapped[int] = mapped_column(Integer, default=8, server_default=text("8"))
    sync_from_after_switch: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class InterfaceIntent(Base):
    """Intent mirror — one row per (interface, attribute), set by PUT /devices/{id}/intent.

    The plugin owns intent; this table is the adapter's mirror.  Apply workers
    read only this mirror, never the live plugin state.
    """

    __tablename__ = "interface_intent"
    __table_args__ = (UniqueConstraint("interface_id", "attribute"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    interface_id: Mapped[int] = mapped_column(Integer, ForeignKey("interfaces.id"), index=True)
    attribute: Mapped[str] = mapped_column(String(64))
    intent_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    interface_obj: Mapped[DbInterface] = relationship("DbInterface", back_populates="intent")


class LagInterface(Base):
    __tablename__ = "lag_interface"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    lag_id: Mapped[int] = mapped_column(Integer, nullable=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)
    __table_args__ = (UniqueConstraint("device_id", "name", name="uq_laginterface_device_name"),)

    device: Mapped[Device] = relationship("Device", back_populates="lag_interfaces")
    members: Mapped[list[LagMember]] = relationship(
        "LagMember", back_populates="lag_interface", cascade="all, delete-orphan"
    )


class LagMember(Base):
    __tablename__ = "lag_member"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lag_interface_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("lag_interface.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(256), nullable=False)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    __table_args__ = (UniqueConstraint("lag_interface_id", "interface_name", name="uq_lagmember_lag_iface"),)

    lag_interface: Mapped[LagInterface] = relationship("LagInterface", back_populates="members")


class LagBundleConfig(Base):
    """Read-mirror of LACP bundle configuration parameters from NSO."""

    __tablename__ = "lag_bundle_config"
    __table_args__ = (UniqueConstraint("device_id", "lag_id", name="uq_lag_bundle_config_device_lag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    lag_id: Mapped[int] = mapped_column(Integer, nullable=False)
    min_links: Mapped[int | None] = mapped_column(Integer, nullable=True)
    system_priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    system_id: Mapped[str | None] = mapped_column(String(17), nullable=True)
    timer: Mapped[str | None] = mapped_column(String(8), nullable=True)
    admin_key: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False, default="never")

    members: Mapped[list[LagMemberConfig]] = relationship(
        "LagMemberConfig", back_populates="bundle", cascade="all, delete-orphan", lazy="raise"
    )


class LagMemberConfig(Base):
    """Read-mirror of LACP member port parameters from NSO."""

    __tablename__ = "lag_member_config"
    __table_args__ = (UniqueConstraint("lag_bundle_id", "interface_name", name="uq_lag_member_config_bundle_iface"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lag_bundle_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("lag_bundle_config.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    port_priority: Mapped[int | None] = mapped_column(Integer, nullable=True)

    bundle: Mapped[LagBundleConfig] = relationship("LagBundleConfig", back_populates="members")


class DeviceVlan(Base):
    """Read mirror of a device's VLAN database. Full-replace per refresh."""

    __tablename__ = "device_vlan"
    __table_args__ = (UniqueConstraint("device_id", "vlan_id", name="uq_devicevlan_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vlan_id: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")
    device: Mapped[Device] = relationship("Device", back_populates="vlan_database")


class VlanIntent(Base):
    """Write-path intent for a VLAN-database entry (vid + name) accepted by the operator."""

    __tablename__ = "vlan_intent"
    __table_args__ = (UniqueConstraint("device_id", "vlan_id", name="uq_vlanintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vlan_id: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="vlan_intents")


class DeviceSwitchport(Base):
    """Read mirror of a device's per-interface L2 switchport state."""

    __tablename__ = "device_switchport"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_deviceswitchport_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    untagged_vlan_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("device_vlan.id", ondelete="SET NULL"), nullable=True
    )
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="switchports")
    untagged_vlan: Mapped[DeviceVlan | None] = relationship("DeviceVlan", lazy="raise")
    tagged_vlans: Mapped[list[DeviceVlan]] = relationship(
        "DeviceVlan", secondary="device_switchport_tagged_vlan", lazy="raise"
    )


class DeviceSwitchportTaggedVlan(Base):
    """Join table: tagged VLANs on a trunk switchport."""

    __tablename__ = "device_switchport_tagged_vlan"
    __table_args__ = (UniqueConstraint("switchport_id", "vlan_id", name="uq_swtaggedvlan_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    switchport_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_switchport.id", ondelete="CASCADE"), nullable=False
    )
    vlan_id: Mapped[int] = mapped_column(Integer, ForeignKey("device_vlan.id", ondelete="CASCADE"), nullable=False)


class InterfaceIpAddress(Base):
    """Read mirror of per-interface IP addresses from NSO.

    Full-replace on every refresh: all rows for a device are deleted then re-inserted.
    Keyed (device_id, interface_name, address, vrf) — one row per address per interface.
    Empty-string vrf means the global/default routing table.
    """

    __tablename__ = "interface_ip_address"
    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "interface_name",
            "address",
            "vrf",
            name="uq_ifipaddr_device_iface_addr_vrf",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(256), nullable=False)
    address: Mapped[str] = mapped_column(String(64), nullable=False)  # "ip/prefix-length"
    vrf: Mapped[str] = mapped_column(String(256), nullable=False, default="")  # "" = global
    family: Mapped[str] = mapped_column(String(8), nullable=False)  # "ipv4" | "ipv6"
    secondary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    bound_port: Mapped[str | None] = mapped_column(String(256), nullable=True, default=None)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="ip_addresses")


class InterfaceIpIntent(Base):
    """Write-path intent mirror for IP address configuration pushed by the plugin.

    Structured, NOT string-valued — unlike InterfaceIntent which stores generic
    attribute string values.  Apply pass runs separately from the attribute
    apply pass but shares the same per-device job lane (core/jobs.py:get_active_job).
    Keyed (interface_id, address, vrf).
    """

    __tablename__ = "interface_ip_intent"
    __table_args__ = (
        UniqueConstraint(
            "interface_id",
            "address",
            "vrf",
            name="uq_ifipintent_iface_addr_vrf",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    interface_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("interfaces.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str] = mapped_column(String(64), nullable=False)  # "ip/prefix-length"
    vrf: Mapped[str] = mapped_column(String(256), nullable=False, default="")  # "" = global
    family: Mapped[str] = mapped_column(String(8), nullable=False)  # "ipv4" | "ipv6" (derived)
    secondary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    interface_obj: Mapped[DbInterface] = relationship("DbInterface")


class SnmpCommunity(Base):
    """Read mirror of SNMP community entries from NSO oper-data.

    Full-replace on every refresh: all rows for a device are deleted then re-inserted.
    The community string is NEVER stored; community_hash is a SHA-256 opaque identifier.
    """

    __tablename__ = "snmp_community"
    __table_args__ = (UniqueConstraint("device_id", "community_hash", name="uq_snmpcommunity_device_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    community_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    access: Mapped[str] = mapped_column(String(4), nullable=False)  # "RO" | "RW"
    acl: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_communities")


class SnmpV3User(Base):
    """Read mirror of SNMPv3 user entries from NSO oper-data.

    Username is not a secret.  Passwords are never stored; has_auth_secret /
    has_priv_secret are boolean flags indicating Vault presence is required.
    """

    __tablename__ = "snmp_v3_user"
    __table_args__ = (UniqueConstraint("device_id", "username", name="uq_snmpv3user_device_username"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    username: Mapped[str] = mapped_column(String(256), nullable=False)
    has_auth_secret: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_priv_secret: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_v3_users")


class SnmpHost(Base):
    """Read mirror of SNMP trap/inform receiver entries from NSO oper-data.

    Only non-secret fields: address, version, notify_type, port.
    Community strings and usernames are never stored.
    """

    __tablename__ = "snmp_host"
    __table_args__ = (UniqueConstraint("device_id", "address", name="uq_snmphost_device_address"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[str | None] = mapped_column(String(8), nullable=True)  # "1" | "2c" | "3"
    notify_type: Mapped[str | None] = mapped_column(String(16), nullable=True)  # "trap" | "inform"
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_hosts")


class DeviceLoggingHost(Base):
    """Read mirror of a remote syslog server (logging host) from device config.

    Non-secret fields only: address, port, severity, facility, transport, vrf, source.
    """

    __tablename__ = "device_logging_host"
    __table_args__ = (UniqueConstraint("device_id", "address", name="uq_logginghost_device_address"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str] = mapped_column(String(256), nullable=False)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    severity: Mapped[str | None] = mapped_column(String(32), nullable=True)
    facility: Mapped[str | None] = mapped_column(String(32), nullable=True)
    transport: Mapped[str | None] = mapped_column(String(16), nullable=True)
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="logging_hosts")


class SnmpSystemInfo(Base):
    """Read mirror of SNMP sysLocation / sysContact per device.

    One row per device (unique on device_id).  Both fields are optional.
    """

    __tablename__ = "snmp_system_info"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, unique=True, index=True
    )
    location: Mapped[str | None] = mapped_column(String(256), nullable=True)
    contact: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_source: Mapped[str] = mapped_column(String(64), nullable=False)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_system_info")


# ---------------------------------------------------------------------------
# SNMP intent write-path models
# ---------------------------------------------------------------------------


class SnmpCommunityIntent(Base):
    """Write-path intent for an SNMP community entry accepted by the NetBox operator.

    The community string is NEVER stored; it is resolved from Vault at apply time
    using vault_ref (format: ``mount/path#key``).  The label is a human-readable
    identifier (NOT the community string) used as the SNMP service list key.
    """

    __tablename__ = "snmp_community_intent"
    __table_args__ = (UniqueConstraint("device_id", "label", name="uq_snmpcommintent_device_label"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    label: Mapped[str] = mapped_column(String(128), nullable=False)
    vault_ref: Mapped[str] = mapped_column(String(512), nullable=False)  # "mount/path#key"
    access: Mapped[str] = mapped_column(String(4), nullable=False)  # "RO" | "RW"
    acl: Mapped[str | None] = mapped_column(String(256), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_community_intents")


class SnmpV3UserIntent(Base):
    """Write-path intent for an SNMPv3 user accepted by the NetBox operator.

    Both auth and priv secrets are resolved from Vault at apply time.
    """

    __tablename__ = "snmp_v3_user_intent"
    __table_args__ = (UniqueConstraint("device_id", "username", name="uq_snmpv3userintent_device_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    username: Mapped[str] = mapped_column(String(128), nullable=False)
    auth_vault_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)  # "mount/path#key"
    priv_vault_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)  # "mount/path#key"
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_v3_user_intents")


class SnmpHostIntent(Base):
    """Write-path intent for an SNMP trap/inform host accepted by the NetBox operator."""

    __tablename__ = "snmp_host_intent"
    __table_args__ = (UniqueConstraint("device_id", "address", name="uq_snmphostintent_device_addr"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[str] = mapped_column(String(4), nullable=False)  # "1" | "2c" | "3"
    notify_type: Mapped[str] = mapped_column(String(8), nullable=False)  # "trap" | "inform"
    community_or_user: Mapped[str] = mapped_column(String(128), nullable=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_host_intents")


class SnmpSystemInfoIntent(Base):
    """Write-path intent for SNMP system location/contact accepted by the NetBox operator."""

    __tablename__ = "snmp_system_info_intent"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, unique=True, index=True
    )
    location: Mapped[str | None] = mapped_column(String(256), nullable=True)
    contact: Mapped[str | None] = mapped_column(String(256), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="snmp_system_info_intent")


class DeviceStaticRoute(Base):
    """Read mirror of a static route as exported by network-state-export."""

    __tablename__ = "device_static_route"
    __table_args__ = (UniqueConstraint("device_id", "vrf", "prefix", "next_hop", name="uq_devicestaticroute_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vrf: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    prefix: Mapped[str] = mapped_column(String(64), nullable=False)
    next_hop: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    interface_next_hop: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    permanent: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="static_routes")


class StaticRouteIntent(Base):
    """Write-path intent for a static route accepted by the NetBox operator."""

    __tablename__ = "static_route_intent"
    __table_args__ = (UniqueConstraint("device_id", "vrf", "prefix", "next_hop", name="uq_staticrouteintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vrf: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    prefix: Mapped[str] = mapped_column(String(64), nullable=False)
    next_hop: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    interface_next_hop: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    permanent: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="static_route_intents")


class DeviceSvi(Base):
    """Read mirror of one L3 VLAN interface (SVI / IRB) —, read-only.

    No IPs (those ride interface-ip). One row per (device, interface-name).
    """

    __tablename__ = "device_svi"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_devicesvi_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    vlan_id: Mapped[int] = mapped_column(Integer, nullable=False)
    svi_type: Mapped[str] = mapped_column(String(8), nullable=False, default="svi")  # svi | irb
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="svis")


class SviIntent(Base):
    """Write-path intent for an L3 VLAN interface (SVI / IRB) accepted by the operator."""

    __tablename__ = "svi_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_sviintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    vlan_id: Mapped[int] = mapped_column(Integer, nullable=False)
    svi_type: Mapped[str] = mapped_column(String(8), nullable=False, default="svi")  # svi | irb
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="svi_intents")


class DeviceSubinterface(Base):
    """Read mirror of one dot1q L3 subinterface —, read-only.

    No IPs (those ride interface-ip). One row per (device, interface-name). The
    dot1q tag is interface-local encapsulation, deliberately NOT a foreign key.
    """

    __tablename__ = "device_subinterface"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_devicesubif_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    parent_interface: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dot1q_vlan: Mapped[int | None] = mapped_column(Integer, nullable=True)  # interface-local 802.1q tag
    sub_type: Mapped[str] = mapped_column(String(16), nullable=False, default="subinterface")
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="subinterfaces")


class DeviceInterfaceMtu(Base):
    """Read mirror of one interface's MTU set — Phase 2b, read-only.

    One row per (device, interface-name) for any interface carrying at least one
    explicit MTU value. ``mtu`` is the L2 MTU; ``ip_mtu``/``mpls_mtu`` ride the
    plugin's NSOInterfaceMtuState overlay. ``bound_port`` carries the Nokia
    port↔router-interface binding so the plugin can correlate the L2 MTU.
    """

    __tablename__ = "device_interface_mtu"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_deviceifmtu_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ip_mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mpls_mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bound_port: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="interface_mtus")


class SubinterfaceIntent(Base):
    """Write-path intent for a dot1q L3 subinterface accepted by the operator."""

    __tablename__ = "subinterface_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_subifintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    parent_interface: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dot1q_vlan: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sub_type: Mapped[str] = mapped_column(String(16), nullable=False, default="subinterface")
    vrf: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="subinterface_intents")


class LoggingHostIntent(Base):
    """Write-path intent for a remote syslog server accepted by the NetBox operator."""

    __tablename__ = "logging_host_intent"
    __table_args__ = (UniqueConstraint("device_id", "address", name="uq_logginghostintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str] = mapped_column(String(256), nullable=False)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    severity: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    facility: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    transport: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    vrf: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    source: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="logging_host_intents")


class DeviceL2Sap(Base):
    """Read mirror of one Nokia L2 SAP (epipe/vpls service member) —, read-only.

    One flat row per SAP, carrying its parent service. The dot1q tag is per-SAP
    interface-local encap parsed from the sap-id ``port:tag[.inner]`` (not a device VLAN).
    """

    __tablename__ = "device_l2_sap"
    __table_args__ = (UniqueConstraint("device_id", "service_name", "sap_id", name="uq_devicel2sap_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    service_name: Mapped[str] = mapped_column(String(64), nullable=False)
    service_type: Mapped[str] = mapped_column(String(16), nullable=False)  # epipe | vpls
    service_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sap_id: Mapped[str] = mapped_column(String(64), nullable=False)
    port: Mapped[str] = mapped_column(String(64), nullable=False)
    outer_tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    inner_tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="l2_saps")


class L2SapIntent(Base):
    """Write-path intent for one Nokia L2 SAP accepted by the NetBox operator.

    Mirrors DeviceL2Sap's identity but carries the apply lifecycle. SAP-only:
    the apply path adds/adopts the SAP under an EXISTING epipe/vpls service.
    """

    __tablename__ = "l2_sap_intent"
    __table_args__ = (UniqueConstraint("device_id", "service_name", "sap_id", name="uq_l2sapintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    service_name: Mapped[str] = mapped_column(String(64), nullable=False)
    service_type: Mapped[str] = mapped_column(String(16), nullable=False)  # epipe | vpls
    sap_id: Mapped[str] = mapped_column(String(64), nullable=False)
    port: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    outer_tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    inner_tag: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="l2_sap_intents")


class DeviceIsisProcess(Base):
    """Read mirror of an IS-IS process as exported by network-state-export."""

    __tablename__ = "device_isis_process"
    __table_args__ = (UniqueConstraint("device_id", "process_tag", name="uq_deviceisisprocess_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    process_tag: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    net: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    metric_style: Mapped[str | None] = mapped_column(String(32), nullable=True)
    overload_bit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    area_auth_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    area_auth_present: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # Routing-protocol auth keys read from the device (not config-access creds).
    area_auth_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    domain_auth_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    domain_auth_present: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    domain_auth_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # cross-vendor instance scalars (read mirror of netbox_routing columns).
    spf_initial_wait: Mapped[int | None] = mapped_column(Integer, nullable=True)
    spf_max_wait: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_initial_wait: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_max_wait: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_lifetime: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_refresh_interval: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    overload_on_startup: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    overload_timeout: Mapped[int | None] = mapped_column(Integer, nullable=True)
    te_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    sr_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    sr_node_msd: Mapped[int | None] = mapped_column(Integer, nullable=True)
    distance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    maximum_paths: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reference_bandwidth: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # EAV long-tail mirror: {key: value} for ISISSettingChoices keys.
    settings: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # per-level child rows [{level, default-metric, wide-metrics-only,...}]
    # and the segment-routing object {enabled, prefix-sid-range, ...}.
    levels: Mapped[list | None] = mapped_column(JSON, nullable=True)
    segment_routing: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # flex-algo child rows [{algo-id, metric-type, priority,...}].
    flex_algos: Mapped[list | None] = mapped_column(JSON, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="isis_processes")


class DeviceIsisInterface(Base):
    """Read mirror of an IS-IS-enabled interface as exported by network-state-export."""

    __tablename__ = "device_isis_interface"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", "af", name="uq_deviceisisinterface_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    af: Mapped[str] = mapped_column(String(8), nullable=False)  # "ipv4" or "ipv6"
    process_tag: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    circuit_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    network_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Physical/LAG port a Nokia SR OS logical router-interface binds to (e.g. "lag-99:10").
    # Lets the plugin correlate a Nokia IS-IS interface to its NetBox dcim.Interface
    # (named by port-id). Absent (None) for Cisco/Junos and Nokia loopbacks.
    bound_port: Mapped[str | None] = mapped_column(String(256), nullable=True, default=None)
    # Per-interface IIH (hello) authentication, secret-safe: type normalised to
    # md5/text + a present flag. The key itself is never mirrored.
    hello_auth_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    hello_auth_present: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    bfd_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)  # BFD enabled for IS-IS on this iface
    # per-interface scalars (read mirror of netbox_routing columns).
    csnp_interval: Mapped[int | None] = mapped_column(Integer, nullable=True)
    retransmit_interval: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lsp_interval: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mesh_group: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # EAV long-tail mirror: {key: value} for ISISSettingChoices keys.
    settings: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # per-level child rows [{level, metric, hello-interval,...}].
    levels: Mapped[list | None] = mapped_column(JSON, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="isis_interfaces")


class DeviceBfdInterface(Base):
    """Read mirror of a BFD-configured interface (timers + micro-BFD) from network-state-export."""

    __tablename__ = "device_bfd_interface"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_devicebfdinterface_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    bound_port: Mapped[str | None] = mapped_column(String(256), nullable=True)
    min_tx: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_rx: Mapped[int | None] = mapped_column(Integer, nullable=True)
    multiplier: Mapped[int | None] = mapped_column(Integer, nullable=True)
    micro_bfd: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="bfd_interfaces")


class BfdIntent(Base):
    """Write-path intent for per-interface BFD timers accepted by the operator."""

    __tablename__ = "bfd_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_bfdintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    min_tx: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_rx: Mapped[int | None] = mapped_column(Integer, nullable=True)
    multiplier: Mapped[int | None] = mapped_column(Integer, nullable=True)
    micro_bfd: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="bfd_intents")


class InterfaceMtuIntent(Base):
    """Write-path intent for per-interface MTU accepted by the operator (Phase 2b)."""

    __tablename__ = "interface_mtu_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_ifmtuintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ip_mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mpls_mtu: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="interface_mtu_intents")


class IsisInterfaceIntent(Base):
    """Write-path intent for an IS-IS interface enablement accepted by the NetBox operator."""

    __tablename__ = "isis_interface_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", "af", name="uq_isisinterfaceintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    af: Mapped[str] = mapped_column(String(8), nullable=False)  # "ipv4" or "ipv6"
    process_tag: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    circuit_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    network_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="isis_interface_intents")


class IsisProcessIntent(Base):
    """Write-path intent for an IS-IS process accepted by the NetBox operator."""

    __tablename__ = "isis_process_intent"
    __table_args__ = (UniqueConstraint("device_id", "process_tag", name="uq_isisprocessintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    process_tag: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    net: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    metric_style: Mapped[str | None] = mapped_column(String(20), nullable=True)
    overload_bit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    area_auth_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    area_auth_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    domain_auth_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    domain_auth_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="isis_process_intents")


class IsisFlexAlgoIntent(Base):
    """Write-path intent for an IS-IS Flex-Algorithm definition accepted by the operator.

    Keyed by (device, process_tag, algo_id) independently of IsisProcessIntent so a
    flex-algo can be accepted/applied even when no other process-level config exists
    (e.g. IOS-XR, where process_tag is the 'router isis <tag>' instance name).
    """

    __tablename__ = "isis_flex_algo_intent"
    __table_args__ = (UniqueConstraint("device_id", "process_tag", "algo_id", name="uq_isisflexalgointent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    process_tag: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    algo_id: Mapped[int] = mapped_column(Integer, nullable=False)
    metric_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    admin_group_exclude: Mapped[str | None] = mapped_column(String(200), nullable=True)
    admin_group_include_any: Mapped[str | None] = mapped_column(String(200), nullable=True)
    admin_group_include_all: Mapped[str | None] = mapped_column(String(200), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="isis_flex_algo_intents")


class DeviceBgpRouter(Base):
    """Read mirror of a BGP router (ASN) as exported by network-state-export."""

    __tablename__ = "device_bgp_router"
    __table_args__ = (UniqueConstraint("device_id", "asn", name="uq_devicebgprouter_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    asn: Mapped[str] = mapped_column(String(32), nullable=False)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="bgp_routers")
    scopes: Mapped[list[DeviceBgpScope]] = relationship(
        "DeviceBgpScope", back_populates="router", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceBgpScope(Base):
    """Read mirror of a BGP VRF scope (per-router, per-VRF context)."""

    __tablename__ = "device_bgp_scope"
    __table_args__ = (UniqueConstraint("router_id", "vrf", name="uq_devicebgpscope_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    router_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_router.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vrf: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    router: Mapped[DeviceBgpRouter] = relationship("DeviceBgpRouter", back_populates="scopes")
    address_families: Mapped[list[DeviceBgpAddressFamily]] = relationship(
        "DeviceBgpAddressFamily", back_populates="scope", cascade="all, delete-orphan", lazy="raise"
    )
    peers: Mapped[list[DeviceBgpPeer]] = relationship(
        "DeviceBgpPeer", back_populates="scope", cascade="all, delete-orphan", lazy="raise"
    )
    peer_groups: Mapped[list[DeviceBgpPeerGroup]] = relationship(
        "DeviceBgpPeerGroup", back_populates="scope", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceBgpAddressFamily(Base):
    """Read mirror of a BGP address-family activated under a scope."""

    __tablename__ = "device_bgp_address_family"
    __table_args__ = (UniqueConstraint("scope_id", "af", name="uq_devicebgpaf_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_scope.id", ondelete="CASCADE"), nullable=False, index=True
    )
    af: Mapped[str] = mapped_column(String(32), nullable=False)  # e.g. "ipv4-unicast"

    scope: Mapped[DeviceBgpScope] = relationship("DeviceBgpScope", back_populates="address_families")


class DeviceBgpPeer(Base):
    """Read mirror of a BGP peer (neighbor) under a scope."""

    __tablename__ = "device_bgp_peer"
    __table_args__ = (UniqueConstraint("scope_id", "peer_address", name="uq_devicebgppeer_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_scope.id", ondelete="CASCADE"), nullable=False, index=True
    )
    peer_address: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    peer_group: Mapped[str | None] = mapped_column(String(128), nullable=True)
    remote_as: Mapped[str | None] = mapped_column(String(32), nullable=True)
    local_as: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ttl: Mapped[int | None] = mapped_column(Integer, nullable=True)
    password: Mapped[str | None] = mapped_column(String(256), nullable=True)  # plaintext by design
    source: Mapped[str | None] = mapped_column(String(256), nullable=True)  # update-source iface or local-address
    bfd_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)  # BFD fall-over on this peer

    scope: Mapped[DeviceBgpScope] = relationship("DeviceBgpScope", back_populates="peers")
    peer_address_families: Mapped[list[DeviceBgpPeerAddressFamily]] = relationship(
        "DeviceBgpPeerAddressFamily", back_populates="peer", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceBgpPeerAddressFamily(Base):
    """Read mirror of a BGP peer address-family activation."""

    __tablename__ = "device_bgp_peer_af"
    __table_args__ = (UniqueConstraint("peer_id", "af", name="uq_devicebgppeeraf_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    peer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_peer.id", ondelete="CASCADE"), nullable=False, index=True
    )
    af: Mapped[str] = mapped_column(String(32), nullable=False)  # e.g. "ipv4-unicast"
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # read-path: per-neighbor-AF policy references from device config.
    routemap_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    routemap_out: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_out: Mapped[str | None] = mapped_column(String(255), nullable=True)

    peer: Mapped[DeviceBgpPeer] = relationship("DeviceBgpPeer", back_populates="peer_address_families")


class DeviceBgpPeerGroup(Base):
    """Read mirror of a BGP peer-group / template object (shared by member peers).

    IOS ``neighbor-tag``, Junos/Nokia ``group``. Members reference it by name via
    DeviceBgpPeer.peer_group; this row carries the group's OWN config + per-AF
    policies so it can be modelled as a shared BGPPeerTemplate in NetBox.
    """

    __tablename__ = "device_bgp_peer_group"
    __table_args__ = (UniqueConstraint("scope_id", "name", name="uq_devicebgppeergroup_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_scope.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    remote_as: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source: Mapped[str | None] = mapped_column(String(256), nullable=True)

    scope: Mapped[DeviceBgpScope] = relationship("DeviceBgpScope", back_populates="peer_groups")
    address_families: Mapped[list[DeviceBgpPeerGroupAddressFamily]] = relationship(
        "DeviceBgpPeerGroupAddressFamily",
        back_populates="peer_group",
        cascade="all, delete-orphan",
        lazy="raise",
    )


class DeviceBgpPeerGroupAddressFamily(Base):
    """Read mirror of a per-AF policy object configured on a BGP peer-group."""

    __tablename__ = "device_bgp_peer_group_af"
    __table_args__ = (UniqueConstraint("peer_group_id", "af", name="uq_devicebgppeergroupaf_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    peer_group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_bgp_peer_group.id", ondelete="CASCADE"), nullable=False, index=True
    )
    af: Mapped[str] = mapped_column(String(32), nullable=False)  # e.g. "ipv4-unicast"
    routemap_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    routemap_out: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_out: Mapped[str | None] = mapped_column(String(255), nullable=True)

    peer_group: Mapped[DeviceBgpPeerGroup] = relationship("DeviceBgpPeerGroup", back_populates="address_families")


# ---------------------------------------------------------------------------
# BGP intent tables (write path)
# ---------------------------------------------------------------------------


class BgpRouterIntent(Base):
    """Write-path intent for a BGP router (ASN) accepted by the NetBox operator.

    Top-level unit: one row per (device, asn).  Status lifecycle tracked via
    accepted_at / last_apply_at / last_apply_error — same convention as
    IsisInterfaceIntent and StaticRouteIntent.
    """

    __tablename__ = "bgp_router_intent"
    __table_args__ = (UniqueConstraint("device_id", "asn", name="uq_bgprouterintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    asn: Mapped[str] = mapped_column(String(32), nullable=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="bgp_router_intents")
    scopes: Mapped[list[BgpScopeIntent]] = relationship(
        "BgpScopeIntent", back_populates="router", cascade="all, delete-orphan", lazy="raise"
    )


class BgpScopeIntent(Base):
    """Write-path intent for a BGP VRF scope."""

    __tablename__ = "bgp_scope_intent"
    __table_args__ = (UniqueConstraint("router_id", "vrf", name="uq_bgpscopeintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    router_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bgp_router_intent.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vrf: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    router: Mapped[BgpRouterIntent] = relationship("BgpRouterIntent", back_populates="scopes")
    address_families: Mapped[list[BgpAfIntent]] = relationship(
        "BgpAfIntent", back_populates="scope", cascade="all, delete-orphan", lazy="raise"
    )
    peers: Mapped[list[BgpPeerIntent]] = relationship(
        "BgpPeerIntent", back_populates="scope", cascade="all, delete-orphan", lazy="raise"
    )


class BgpAfIntent(Base):
    """Write-path intent for a BGP address-family activation under a scope."""

    __tablename__ = "bgp_af_intent"
    __table_args__ = (UniqueConstraint("scope_id", "af", name="uq_bgpafintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bgp_scope_intent.id", ondelete="CASCADE"), nullable=False, index=True
    )
    af: Mapped[str] = mapped_column(String(32), nullable=False)

    scope: Mapped[BgpScopeIntent] = relationship("BgpScopeIntent", back_populates="address_families")


class BgpPeerIntent(Base):
    """Write-path intent for a BGP neighbor under a scope."""

    __tablename__ = "bgp_peer_intent"
    __table_args__ = (UniqueConstraint("scope_id", "peer_address", name="uq_bgppeerintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bgp_scope_intent.id", ondelete="CASCADE"), nullable=False, index=True
    )
    peer_address: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    peer_group: Mapped[str | None] = mapped_column(String(128), nullable=True)
    remote_as: Mapped[str | None] = mapped_column(String(32), nullable=True)
    local_as: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ttl: Mapped[int | None] = mapped_column(Integer, nullable=True)
    password: Mapped[str | None] = mapped_column(String(256), nullable=True)  # plaintext by design
    # BGP session source: IOS/IOS-XR update-source interface, Junos/Nokia local-address IP.
    source: Mapped[str | None] = mapped_column(String(256), nullable=True)

    scope: Mapped[BgpScopeIntent] = relationship("BgpScopeIntent", back_populates="peers")
    peer_address_families: Mapped[list[BgpPeerAfIntent]] = relationship(
        "BgpPeerAfIntent", back_populates="peer", cascade="all, delete-orphan", lazy="raise"
    )


class BgpPeerAfIntent(Base):
    """Write-path intent for a BGP peer address-family activation."""

    __tablename__ = "bgp_peer_af_intent"
    __table_args__ = (UniqueConstraint("peer_id", "af", name="uq_bgppeerafintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    peer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bgp_peer_intent.id", ondelete="CASCADE"), nullable=False, index=True
    )
    af: Mapped[str] = mapped_column(String(32), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    routemap_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    routemap_out: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_in: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prefixlist_out: Mapped[str | None] = mapped_column(String(255), nullable=True)

    peer: Mapped[BgpPeerIntent] = relationship("BgpPeerIntent", back_populates="peer_address_families")


# ── Route-policy read-mirror (A2) ─────────────────────────────────────────


class DeviceRoutePolicyPrefixList(Base):
    """Read-mirror: one row per (device, prefix-list name)."""

    __tablename__ = "device_route_policy_prefix_list"
    __table_args__ = (UniqueConstraint("device_id", "name", name="uq_drppl_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    family: Mapped[int] = mapped_column(Integer, nullable=False)  # 4 or 6
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="poll")

    device: Mapped[Device] = relationship("Device")
    entries: Mapped[list[DeviceRoutePolicyPrefixListEntry]] = relationship(
        "DeviceRoutePolicyPrefixListEntry", back_populates="prefix_list", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceRoutePolicyPrefixListEntry(Base):
    """One sequence entry within a prefix-list."""

    __tablename__ = "device_route_policy_prefix_list_entry"
    __table_args__ = (UniqueConstraint("prefix_list_id", "sequence", name="uq_drpple_seq"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prefix_list_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_route_policy_prefix_list.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    prefix: Mapped[str] = mapped_column(String(64), nullable=False)
    ge: Mapped[int | None] = mapped_column(Integer, nullable=True)
    le: Mapped[int | None] = mapped_column(Integer, nullable=True)

    prefix_list: Mapped[DeviceRoutePolicyPrefixList] = relationship(
        "DeviceRoutePolicyPrefixList", back_populates="entries"
    )


class DeviceRoutePolicyCommunityList(Base):
    """Read-mirror: one row per (device, community-list name)."""

    __tablename__ = "device_route_policy_community_list"
    __table_args__ = (UniqueConstraint("device_id", "name", name="uq_drpcl_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    # Junos invert-match / Nokia "expression NOT (…)": the list matches routes
    # carrying NONE of its members. No native form on Cisco community-lists.
    invert_match: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="poll")

    device: Mapped[Device] = relationship("Device")
    entries: Mapped[list[DeviceRoutePolicyCommunityListEntry]] = relationship(
        "DeviceRoutePolicyCommunityListEntry",
        back_populates="community_list",
        cascade="all, delete-orphan",
        lazy="raise",
    )


class DeviceRoutePolicyCommunityListEntry(Base):
    """One entry within a community-list."""

    __tablename__ = "device_route_policy_community_list_entry"
    __table_args__ = (UniqueConstraint("community_list_id", "sequence", name="uq_drpcle_seq"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    community_list_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_route_policy_community_list.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    community: Mapped[str] = mapped_column(String(128), nullable=False)

    community_list: Mapped[DeviceRoutePolicyCommunityList] = relationship(
        "DeviceRoutePolicyCommunityList", back_populates="entries"
    )


class DeviceRoutePolicyASPath(Base):
    """Read-mirror: one row per (device, AS-path access-list name)."""

    __tablename__ = "device_route_policy_as_path"
    __table_args__ = (UniqueConstraint("device_id", "name", name="uq_drpap_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="poll")

    device: Mapped[Device] = relationship("Device")
    entries: Mapped[list[DeviceRoutePolicyASPathEntry]] = relationship(
        "DeviceRoutePolicyASPathEntry", back_populates="as_path", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceRoutePolicyASPathEntry(Base):
    """One entry within an AS-path access-list."""

    __tablename__ = "device_route_policy_as_path_entry"
    __table_args__ = (UniqueConstraint("as_path_id", "sequence", name="uq_drpape_seq"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_path_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_route_policy_as_path.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    pattern: Mapped[str] = mapped_column(Text, nullable=False)

    as_path: Mapped[DeviceRoutePolicyASPath] = relationship("DeviceRoutePolicyASPath", back_populates="entries")


class DeviceRoutePolicyRouteMap(Base):
    """Read-mirror: one row per (device, route-map name)."""

    __tablename__ = "device_route_policy_route_map"
    __table_args__ = (UniqueConstraint("device_id", "name", name="uq_drprm_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="poll")

    device: Mapped[Device] = relationship("Device")
    entries: Mapped[list[DeviceRoutePolicyRouteMapEntry]] = relationship(
        "DeviceRoutePolicyRouteMapEntry", back_populates="route_map", cascade="all, delete-orphan", lazy="raise"
    )


class DeviceRoutePolicyRouteMapEntry(Base):
    """One sequence/clause within a route-map."""

    __tablename__ = "device_route_policy_route_map_entry"
    __table_args__ = (UniqueConstraint("route_map_id", "sequence", name="uq_drprme_seq"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    route_map_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("device_route_policy_route_map.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    match_prefix_lists: Mapped[list | None] = mapped_column(JSON, nullable=True)  # list[str]
    match_community_lists: Mapped[list | None] = mapped_column(JSON, nullable=True)  # list[str]
    match_as_paths: Mapped[list | None] = mapped_column(JSON, nullable=True)  # list[str]
    match_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON blob
    set_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON blob

    route_map: Mapped[DeviceRoutePolicyRouteMap] = relationship("DeviceRoutePolicyRouteMap", back_populates="entries")


class RoutePolicyObjectIntent(Base):
    """Intent row per (device, family, object-name).

    Stores the desired state for one route-policy object on one device.
    ``entries`` is a JSON list shaped per the contract §3 (entries vary by
    family but are fully defined in docs/m17-route-policy-contract.md §2).
    """

    __tablename__ = "route_policy_object_intent"
    __table_args__ = (UniqueConstraint("device_id", "family", "name", name="uq_rpoi_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(Integer, ForeignKey("devices.id"), index=True)
    family: Mapped[str] = mapped_column(String(32))  # prefix_list / community_list / as_path / route_map
    name: Mapped[str] = mapped_column(String(255))
    entries: Mapped[dict | list] = mapped_column(JSON, nullable=False)
    # community_list only: Junos invert-match / Nokia "expression NOT (…)".
    invert_match: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="route_policy_object_intents")


# ──────────────────────────────────────────────────────────────────────────────
# OSPF models
# ──────────────────────────────────────────────────────────────────────────────


class DeviceOspfInstance(Base):
    """Read mirror of an OSPF process instance as exported by network-state-export."""

    __tablename__ = "device_ospf_instance"
    # vrf is part of the identity: the same process-id can run in multiple VRFs.
    __table_args__ = (UniqueConstraint("device_id", "process_id", "vrf", name="uq_deviceospfinstance_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    process_id: Mapped[str] = mapped_column(String(64), nullable=False)
    router_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vrf: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    # areas stored as JSON: [{area_id, area_type}]
    areas: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # OSPF process admin-state (Nokia SR OS 'admin-state enable'); None when the NED
    # has no explicit admin-state (process enabled by config presence).
    enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="ospf_instances")


class DeviceOspfInterface(Base):
    """Read mirror of an OSPF-enabled interface as exported by network-state-export."""

    __tablename__ = "device_ospf_interface"
    # process_id is part of the identity: one interface can be enabled under multiple
    # OSPF processes, so the same (device, interface) legitimately appears more than once.
    __table_args__ = (
        UniqueConstraint("device_id", "interface_name", "process_id", name="uq_deviceospfinterface_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    process_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    area_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    passive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost: Mapped[int | None] = mapped_column(Integer, nullable=True)
    network_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    auth_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    auth_present: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="ospf_interfaces")


class OspfInstanceIntent(Base):
    """Write-path intent for an OSPF process instance accepted by the NetBox operator."""

    __tablename__ = "ospf_instance_intent"
    __table_args__ = (UniqueConstraint("device_id", "process_id", name="uq_ospfinstanceintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    process_id: Mapped[str] = mapped_column(String(64), nullable=False)
    router_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    vrf: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    areas: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # OSPF process admin-state intent (Nokia SR OS 'admin-state enable').
    enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="ospf_instance_intents")


class OspfInterfaceIntent(Base):
    """Write-path intent for an OSPF interface accepted by the NetBox operator."""

    __tablename__ = "ospf_interface_intent"
    __table_args__ = (UniqueConstraint("device_id", "interface_name", name="uq_ospfinterfaceintent_identity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    process_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    area_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    passive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost: Mapped[int | None] = mapped_column(Integer, nullable=True)
    network_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    auth_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    auth_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="ospf_interface_intents")


# Redistribution ──────────────────────────────────────────────────────────


class DeviceRedistribution(Base):
    """Read mirror of one redistribute statement from network-state-export.

    Key: (device_id, dest_protocol, dest_ref, source_protocol, source_ref)
    dest_protocol: 'ospf' | 'isis' | 'bgp'
    dest_ref: process-id (str) for OSPF, area-tag for ISIS, '<asn>/<vrf>/<afi>' for BGP
    source_protocol: 'connected' | 'static' | 'ospf' | 'isis' | 'bgp' | 'eigrp'
    source_ref: '' for connected/static, instance id/asn/tag otherwise
    """

    __tablename__ = "device_redistribution"
    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "dest_protocol",
            "dest_ref",
            "source_protocol",
            "source_ref",
            name="uq_deviceredistribution_identity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dest_protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    dest_ref: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    source_protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    source_ref: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    route_map: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metric_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_source: Mapped[str] = mapped_column(String(32), nullable=False, default="never")

    device: Mapped[Device] = relationship("Device", back_populates="redistributions")


class RedistributionIntent(Base):
    """Write-path intent for a redistribution statement accepted by the NetBox operator."""

    __tablename__ = "redistribution_intent"
    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "dest_protocol",
            "dest_ref",
            "source_protocol",
            "source_ref",
            name="uq_redistributionintent_identity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dest_protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    dest_ref: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    source_protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    source_ref: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    route_map: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metric: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metric_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apply_error: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    device: Mapped[Device] = relationship("Device", back_populates="redistribution_intents")


class DeviceCapability(Base):
    """Route-policy capability matrix, keyed by ``(ned_id, sw_version)``.

    The compatibility cache that lets the plugin flag — at attach time — which parts of
    a route-map / community-list won't apply on a device, instead of the operator finding
    out only when it silently didn't land. Persisted so it survives an adapter restart.

    Two sources feed each per-element verdict:
      - ``source='probe'`` — the REPRESENTABLE half, from the NSO ``capability-probe``
        action (what the reconciler can model/send for this NED).
      - ``source='apply'`` — the ACCEPTED half, from a real ``apply_failed`` device-parser
        rejection (what the box actually takes at commit). Apply wins over probe.

    Keyed by ``(ned_id, sw_version)`` so 20 identical boxes share one verdict; a box on a
    different software version (or NED) falls into a different key and is re-checked.
    """

    __tablename__ = "device_capability"
    __table_args__ = (UniqueConstraint("ned_id", "sw_version", "scope", "name", name="uq_device_capability"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ned_id: Mapped[str] = mapped_column(String(256), index=True)
    sw_version: Mapped[str] = mapped_column(String(128), index=True)
    scope: Mapped[str] = mapped_column(String(32))  # community | rm-set | rm-match
    name: Mapped[str] = mapped_column(String(128))  # member-kind / construct
    status: Mapped[str] = mapped_column(String(16))  # native | translated | skipped | unsupported
    detail: Mapped[str] = mapped_column(String(256), default="")
    source: Mapped[str] = mapped_column(String(16))  # probe | apply
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
