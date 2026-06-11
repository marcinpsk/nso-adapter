# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""NSO reconcile-commit apply operations (Phase 2, M4/M5).

Uses NSO RESTCONF transactions with the ``reconcile`` commit option so that
the interface-reconciler service adopts pre-existing brownfield config instead
of creating conflicts.

Protocol summary:
1. POST  /restconf/data/tailf-ncs:devices/device={}/config  → ensure service exists
2. PATCH /restconf/data/interface-reconciler:interface-config  with intent
3. POST  /restconf/operations/tailf-netconf-transactions:commit with reconcile option

For simplicity in Phase 2 M5 we use the NSO RESTCONF PATCH directly on the
interface-reconciler service path with the native commit API.
"""

from __future__ import annotations

import json
import os

import structlog

from nso_adapter.nso.client import NsoClient

logger = structlog.get_logger(__name__)

# After a successful apply, re-issue the same intent as a native dry-run and
# assert NSO would push nothing further to the device. A non-empty device delta
# means the intent did not actually land (silently dropped/normalised by the
# NED, or rejected) — i.e. a false success. Toggle off with NSO_ADAPTER_VERIFY_APPLY=0.
VERIFY_AFTER_APPLY = os.environ.get("NSO_ADAPTER_VERIFY_APPLY", "1").strip().lower() not in ("0", "false", "no")

# RESTCONF path to the interface-reconciler service list
_SERVICE_PATH = "/restconf/data/interface-reconciler:interface-config"

# RESTCONF path to open a write transaction and commit with reconcile
_COMMIT_PATH = "/restconf/operations/tailf-netconf-transactions:commit"


class NsoApplyError(Exception):
    """Raised when a NSO commit fails for a specific attribute."""

    def __init__(self, code: str, message: str, detail: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


def _device_delta_from_dry_run(body: object, device_name: str) -> str | None:
    """Return the native device delta for *device_name* from a dry-run-result body.

    Returns the (possibly empty) southbound delta NSO would still push, or None
    when the response is not the expected ``dry-run-result`` shape — in which
    case verification is treated as inconclusive by the caller.

    Empty/absent ``native`` or no matching device entry both mean "no delta" ("").
    """
    if not isinstance(body, dict):
        return None
    result = body.get("dry-run-result")
    if not isinstance(result, dict):
        return None
    native = result.get("native")
    if native in (None, {}):
        return ""
    if not isinstance(native, dict):
        return None
    devices = native.get("device")
    if not devices:
        return ""
    if not isinstance(devices, list):
        return None
    for entry in devices:
        if isinstance(entry, dict) and entry.get("name") == device_name:
            return str(entry.get("data") or "")
    return ""


async def native_dry_run(
    client: NsoClient, url: str, payload: str, device_name: str, *, method: str = "patch"
) -> str | None:
    """Issue *payload* to *url* as a native dry-run (``?dry-run=native``) and return the delta.

    No commit happens — NSO computes the native device config the intent *would* push and
    returns it. The returned string is the device-native delta (``""`` = no change), or
    ``None`` when the dry-run was inconclusive (non-2xx / transport / unparseable / wrong
    shape). Same machinery as the post-apply verify guard, surfaced for the pre-apply preview.
    """
    dry_url = f"{url}?dry-run=native"
    try:
        async with client._client(timeout=client._action_timeout) as c:
            resp = await getattr(c, method)(
                dry_url,
                content=payload,
                headers={"Content-Type": "application/yang-data+json"},
            )
        if resp.status_code not in (200, 201, 204):
            return None
        body = resp.json()
    except Exception:  # network/transport/parse — inconclusive, never block
        return None
    return _device_delta_from_dry_run(body, device_name)


async def _verify_native_or_raise(
    client: NsoClient, url: str, payload: str, device_name: str, *, scope: str, method: str = "patch"
) -> None:
    """Re-issue *payload* as a native dry-run; raise if NSO would still change the device.

    Catches false successes: a 2xx apply whose intent NSO silently did not fully
    apply leaves a non-empty native device delta on the immediate re-dry-run. The
    dry-run uses the same *method* as the apply (PUT for a replace, so a still-present
    removed entry is also caught).

    Fail-safe: transport errors, non-2xx, unparseable bodies or unexpected shapes
    are logged and treated as inconclusive (no raise) so verification never blocks
    an otherwise-successful apply. Compares against NSO's CDB, so out-of-band
    device drift (CDB vs physical) is out of scope here — that needs sync-from.
    """
    if not VERIFY_AFTER_APPLY:
        return

    delta = await native_dry_run(client, url, payload, device_name, method=method)
    if delta is None:
        logger.warning("nso.apply.verify_inconclusive_or_unexpected", scope=scope, device=device_name)
        return
    if delta.strip():
        logger.error("nso.apply.verify_mismatch", scope=scope, device=device_name, delta=delta)
        raise NsoApplyError(
            "verify_mismatch",
            f"{scope}: applied intent did not land on {device_name!r} — "
            f"NSO would still push changes to the device",
            detail={"device_delta": delta},
        )
    logger.info("nso.apply.verify_ok", scope=scope, device=device_name)


async def _send_service_config(
    client: NsoClient,
    service_path: str,
    root_key: str,
    device_name: str,
    body: dict,
    *,
    scope: str,
    replace: bool = False,
    dry_run: bool = False,
) -> str | None:
    """Send a reconciler service instance body to NSO — the shared apply/removal tail.

    ``replace=False`` (apply/add/update): merge-PATCH the service list path, then run
    the native dry-run verify guard. ``replace=True`` (removal): PUT-replace the keyed
    instance (``<service_path>=<device>``) so omitted list entries are dropped and
    FASTMAP reverts them — merge-PATCH never drops, and a node-level DELETE 404s on
    empty-string list keys. On replace, *body* must be the FULL desired state.
    """
    payload = json.dumps({root_key: [body]})
    if replace:
        url = f"{client._base}{service_path}={device_name}"
        method = "put"
    else:
        url = f"{client._base}{service_path}"
        method = "patch"
    if dry_run:
        # Preview: compute the native device delta without committing anything.
        return await native_dry_run(client, url, payload, device_name, method=method)
    async with client._client(timeout=client._action_timeout) as c:
        resp = await getattr(c, method)(
            url, content=payload, headers={"Content-Type": "application/yang-data+json"}
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.service_send_failed",
                scope=scope,
                device=device_name,
                method=method,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_put_failed" if replace else "nso_patch_failed",
                f"NSO {method.upper()} for {scope} failed with status {resp.status_code}",
                detail={"nso_error": err},
            )
    logger.info("nso.apply.service_sent", scope=scope, device=device_name, method=method, replace=replace)
    await _verify_native_or_raise(client, url, payload, device_name, scope=scope, method=method)


async def apply_interface_attribute(
    client: NsoClient,
    device_name: str,
    interface_name: str,
    attribute: str,
    value: str | None,
) -> None:
    """Write a single (device, interface, attribute) intent slice to NSO.

    Creates or updates the service instance keyed by (device_name, interface_name)
    using NSO RESTCONF PATCH with the reconcile commit option.

    Raises NsoApplyError on failure.
    """
    # Build the service instance body — only include the attribute being applied
    service_body: dict = {
        "interface-reconciler:interface-config": [
            {
                "device": device_name,
                "interface-name": interface_name,
            }
        ]
    }

    entry = service_body["interface-reconciler:interface-config"][0]

    if attribute == "description":
        entry["description"] = value if value is not None else ""
    elif attribute == "enabled":
        # intent_value is stored as a string and its case varies by source ("true"
        # from a JSON boolean push, "True" from str(bool)); compare case-insensitively
        # so an enabled interface is never silently written as disabled.
        entry["enabled"] = value is True or str(value).strip().lower() == "true"
    else:
        raise NsoApplyError(
            "unsupported_attribute",
            f"Attribute '{attribute}' is not supported by interface-reconciler",
        )

    url = f"{client._base}{_SERVICE_PATH}"
    payload = json.dumps(
        {"interface-reconciler:interface-config": service_body["interface-reconciler:interface-config"]}
    )

    async with client._client(timeout=client._action_timeout) as c:
        # Use PATCH to create-or-update the service instance
        resp = await c.patch(
            url,
            content=payload,
            headers={"Content-Type": "application/yang-data+json"},
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.patch_failed",
                device=device_name,
                interface=interface_name,
                attribute=attribute,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.ok",
        device=device_name,
        interface=interface_name,
        attribute=attribute,
    )

    await _verify_native_or_raise(client, url, payload, device_name, scope="interface_attribute")


async def apply_interface_ips(
    client: NsoClient,
    device_name: str,
    interface_name: str,
    ip_intent_rows: list,
    *,
    kind: str | None = None,
    service: str | None = None,
    parent_binding: str | None = None,
    encap_tag: str | None = None,
    dry_run: bool = False,
) -> str | None:
    """Write IP addresses and VRF for a single interface to NSO.

    Builds a full interface-reconciler PATCH body from the supplied rows,
    one PATCH call per interface covering all IPv4, IPv6, and VRF intent.

    ``kind``/``service``/``parent_binding``/``encap_tag`` carry the Nokia M27
    routed-interface context so the reconciler writes the IP to the SR OS
    ``configure router Base`` / ``configure service {ies,vprn} <service>``
    interface (bound to its port) instead of to the port.  They are ignored by
    the IOS/Junos handlers.

    Raises NsoApplyError on failure.
    """
    entry: dict = {
        "device": device_name,
        "interface-name": interface_name,
    }

    # VRF is an interface-level concept; take the first non-empty VRF value.
    vrf = next((r.vrf for r in ip_intent_rows if r.vrf), None)
    if vrf:
        entry["vrf"] = vrf

    # Nokia routed-interface context (M27 apply): route the IP to the router/service
    # interface, not the port. Only emitted when kind is set (Nokia L3 interfaces).
    if kind:
        entry["kind"] = kind
        if service:
            entry["service"] = service
        if parent_binding:
            entry["parent-binding"] = parent_binding
        if encap_tag:
            entry["encap-tag"] = encap_tag

    ipv4_entries = []
    ipv6_entries = []
    for row in ip_intent_rows:
        addr, plen_str = row.address.rsplit("/", 1)
        prefix_len = int(plen_str)
        if row.family == "ipv4":
            ipv4_entries.append({"address": addr, "prefix-length": prefix_len, "secondary": row.secondary})
        elif row.family == "ipv6":
            ipv6_entries.append({"address": addr, "prefix-length": prefix_len})

    if ipv4_entries:
        entry["ipv4-address"] = ipv4_entries
    if ipv6_entries:
        entry["ipv6-address"] = ipv6_entries

    url = f"{client._base}{_SERVICE_PATH}"
    payload = json.dumps({"interface-reconciler:interface-config": [entry]})

    if dry_run:
        return await native_dry_run(client, url, payload, device_name, method="patch")

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(
            url,
            content=payload,
            headers={"Content-Type": "application/yang-data+json"},
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.ip_patch_failed",
                device=device_name,
                interface=interface_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for IP intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.ip_ok",
        device=device_name,
        interface=interface_name,
        ipv4_count=len(ipv4_entries),
        ipv6_count=len(ipv6_entries),
    )

    await _verify_native_or_raise(client, url, payload, device_name, scope="interface_ip")


_SNMP_SERVICE_PATH = "/restconf/data/snmp-reconciler:snmp-config"

# RESTCONF path to the static-route-reconciler service list
_STATIC_ROUTE_SERVICE_PATH = "/restconf/data/static-route-reconciler:static-route-config"

# RESTCONF path to the logging-reconciler service list (remote syslog write path)
_LOGGING_SERVICE_PATH = "/restconf/data/logging-reconciler:logging-config"

# RESTCONF path to the svi-reconciler service list (SVI/IRB write path, M35)
_SVI_SERVICE_PATH = "/restconf/data/svi-reconciler:svi-config"

# RESTCONF path to the subinterface-reconciler service list (dot1q write path, M36)
_SUBIF_SERVICE_PATH = "/restconf/data/subinterface-reconciler:subif-config"

# RESTCONF path to the vlan-reconciler service list (VLAN-database write path, M34)
_VLAN_SERVICE_PATH = "/restconf/data/vlan-reconciler:vlan-config"

# RESTCONF path to the bfd-reconciler service list (per-interface BFD write path)
_BFD_SERVICE_PATH = "/restconf/data/bfd-reconciler:bfd-config"

# RESTCONF path to the l2-sap-reconciler service list (M37 P2b)
_L2_SAP_SERVICE_PATH = "/restconf/data/l2-sap-reconciler:l2-sap-config"

_LAG_SERVICE_PATH = "/restconf/data/lag-reconciler:lag-config"

_SWITCHPORT_SERVICE_PATH = "/restconf/data/switchport-reconciler:switchport-config"

# RESTCONF path to the isis-reconciler service list
_ISIS_SERVICE_PATH = "/restconf/data/isis-reconciler:isis-config"

# RESTCONF path to the bgp-reconciler service list
_BGP_SERVICE_PATH = "/restconf/data/bgp-reconciler:bgp-config"

# RESTCONF path to the route-policy-reconciler service list
_ROUTE_POLICY_SERVICE_PATH = "/restconf/data/route-policy-reconciler:route-policy-config"


async def apply_snmp_config(
    client: NsoClient,
    device_name: str,
    community_intents: list,
    v3_user_intents: list,
    host_intents: list,
    system_info_intent,
    *,
    replace: bool = False,
) -> None:
    """Write the full SNMP intent snapshot for a device to the snmp-reconciler service.

    Builds a single body covering all communities (by label + vault_ref), v3 users,
    hosts, and system info.  The snmp-reconciler NSO service resolves Vault refs at
    commit time — vault_refs are passed verbatim.  ``replace=True`` PUT-replaces the
    keyed instance so removed elements are reverted.  Raises NsoApplyError on failure.
    """
    entry: dict = {"device": device_name}

    if community_intents:
        entry["community"] = [
            {
                "label": c.label,
                "vault-ref": c.vault_ref,
                "access": c.access,
                **({"acl": c.acl} if c.acl else {}),
            }
            for c in community_intents
        ]

    if v3_user_intents:
        entry["v3-user"] = [
            {
                "username": u.username,
                **({"auth-vault-ref": u.auth_vault_ref} if u.auth_vault_ref else {}),
                **({"priv-vault-ref": u.priv_vault_ref} if u.priv_vault_ref else {}),
            }
            for u in v3_user_intents
        ]

    if host_intents:
        entry["host"] = [
            {
                "address": h.address,
                "version": h.version,
                "notify-type": h.notify_type,
                "community-or-user": h.community_or_user,
            }
            for h in host_intents
        ]

    if system_info_intent:
        if system_info_intent.location is not None:
            entry["location"] = system_info_intent.location
        if system_info_intent.contact is not None:
            entry["contact"] = system_info_intent.contact

    return await _send_service_config(
        client,
        _SNMP_SERVICE_PATH,
        "snmp-reconciler:snmp-config",
        device_name,
        entry,
        scope="snmp",
        replace=replace,
    )


async def apply_static_routes(
    client: NsoClient,
    device_name: str,
    route_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write static route intent for a single device to NSO.

    Builds a full static-route-reconciler body from the supplied rows and commits in
    reconcile mode so pre-existing routes are adopted. ``replace=True`` PUT-replaces
    the keyed instance (full desired state) so removed routes are reverted on the
    device. Raises NsoApplyError on failure.
    """
    routes = []
    for row in route_intent_rows:
        entry: dict = {
            "vrf": row.vrf,
            "prefix": row.prefix,
            "next-hop": row.next_hop,
        }
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.permanent is not None and row.permanent:
            entry["permanent"] = row.permanent
        if row.tag is not None:
            entry["tag"] = row.tag
        routes.append(entry)

    return await _send_service_config(
        client,
        _STATIC_ROUTE_SERVICE_PATH,
        "static-route-reconciler:static-route-config",
        device_name,
        {"device": device_name, "route": routes},
        scope="static_route",
        replace=replace,
    )


async def apply_logging_config(
    client: NsoClient,
    device_name: str,
    host_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write the full remote-syslog intent snapshot for a device to NSO.

    Builds a logging-reconciler body from the supplied rows; the service adopts
    pre-existing brownfield logging config (reconcile). ``replace=True`` PUT-replaces
    the keyed instance so removed hosts are reverted. No secrets.
    """
    hosts = []
    for row in host_intent_rows:
        entry: dict = {"address": row.address}
        if row.port is not None:
            entry["port"] = row.port
        if row.severity:
            entry["severity"] = row.severity
        if row.facility:
            entry["facility"] = row.facility
        if row.transport:
            entry["transport"] = row.transport
        if row.vrf:
            entry["vrf"] = row.vrf
        if row.source:
            entry["source"] = row.source
        hosts.append(entry)

    return await _send_service_config(
        client,
        _LOGGING_SERVICE_PATH,
        "logging-reconciler:logging-config",
        device_name,
        {"device": device_name, "host": hosts},
        scope="logging",
        replace=replace,
    )


async def apply_svi_config(
    client: NsoClient,
    device_name: str,
    svi_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write the SVI/IRB intent snapshot for a device to NSO (M35).

    Materialises interface VlanN / interfaces irb unit N via the svi-reconciler;
    IPs ride the interface-reconciler. Reconcile mode (brownfield adoption).
    ``replace=True`` PUT-replaces the keyed instance so removed SVIs are reverted.
    """
    interfaces = []
    for row in svi_intent_rows:
        entry: dict = {"interface-name": row.interface_name, "vlan-id": row.vlan_id, "type": row.svi_type}
        if row.vrf:
            entry["vrf"] = row.vrf
        interfaces.append(entry)

    return await _send_service_config(
        client,
        _SVI_SERVICE_PATH,
        "svi-reconciler:svi-config",
        device_name,
        {"device": device_name, "interface": interfaces},
        scope="svi",
        replace=replace,
    )


async def apply_subinterface_config(
    client: NsoClient,
    device_name: str,
    subif_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write the dot1q subinterface intent snapshot for a device to NSO (M36).

    Materialises <parent>.<unit> (encapsulation dot1Q + vrf forwarding) / Junos
    unit vlan-id via the subinterface-reconciler; IPs ride the interface-reconciler.
    Reconcile mode (brownfield adoption). ``replace=True`` PUT-replaces the keyed
    instance so removed subinterfaces are reverted.
    """
    interfaces = []
    for row in subif_intent_rows:
        entry: dict = {
            "interface-name": row.interface_name,
            "parent-interface": row.parent_interface,
            "dot1q-vlan": row.dot1q_vlan,
            "type": row.sub_type,
        }
        if row.vrf:
            entry["vrf"] = row.vrf
        interfaces.append(entry)

    return await _send_service_config(
        client,
        _SUBIF_SERVICE_PATH,
        "subinterface-reconciler:subif-config",
        device_name,
        {"device": device_name, "interface": interfaces},
        scope="subinterface",
        replace=replace,
    )


async def apply_vlan_config(
    client: NsoClient,
    device_name: str,
    vlan_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write the VLAN-database intent snapshot for a device to NSO (M34 write path).

    Materialises 'vlan <id> / name <name>' (IOS) / 'vlans <name> vlan-id <id>'
    (Junos) via the vlan-reconciler. Reconcile mode (brownfield adoption).
    ``replace=True`` PUT-replaces the keyed instance (full desired list) so removed
    VLANs are reverted on the device. Raises NsoApplyError on failure.
    """
    vlans = []
    for row in vlan_intent_rows:
        entry: dict = {"vlan-id": row.vlan_id}
        if row.name:
            entry["name"] = row.name
        vlans.append(entry)

    return await _send_service_config(
        client,
        _VLAN_SERVICE_PATH,
        "vlan-reconciler:vlan-config",
        device_name,
        {"device": device_name, "vlan": vlans},
        scope="vlan",
        replace=replace,
    )


async def apply_bfd_config(
    client: NsoClient,
    device_name: str,
    bfd_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write the per-interface BFD intent snapshot for a device to NSO.

    Materialises BFD timers via the bfd-reconciler (IOS interface bfd interval;
    IOS-XR bfd address-family; Junos ae bfd-liveness-detection; Nokia router
    interface ipv4 bfd). Reconcile mode. ``replace=True`` PUT-replaces the keyed
    instance so removed BFD interfaces are reverted.
    """
    interfaces = []
    for row in bfd_intent_rows:
        entry: dict = {"interface-name": row.interface_name, "micro-bfd": bool(row.micro_bfd)}
        if row.min_tx is not None:
            entry["min-tx"] = row.min_tx
        if row.min_rx is not None:
            entry["min-rx"] = row.min_rx
        if row.multiplier is not None:
            entry["multiplier"] = row.multiplier
        interfaces.append(entry)

    return await _send_service_config(
        client,
        _BFD_SERVICE_PATH,
        "bfd-reconciler:bfd-config",
        device_name,
        {"device": device_name, "interface": interfaces},
        scope="bfd",
        replace=replace,
    )


async def apply_l2_saps(
    client: NsoClient,
    device_name: str,
    sap_intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write Nokia L2 SAP intent for a single device to NSO (M37 P2b).

    Builds a full l2-sap-reconciler body from the supplied rows and commits in
    reconcile mode so pre-existing SAPs are adopted. The NSO service adds each SAP
    under an EXISTING epipe/vpls service (SAP-only). ``replace=True`` PUT-replaces
    the keyed instance so removed SAPs are reverted.
    """
    saps = []
    for row in sap_intent_rows:
        entry: dict = {
            "service-name": row.service_name,
            "sap-id": row.sap_id,
            "service-type": row.service_type,
        }
        if row.port:
            entry["port"] = row.port
        if row.outer_tag is not None:
            entry["outer-tag"] = row.outer_tag
        if row.inner_tag is not None:
            entry["inner-tag"] = row.inner_tag
        saps.append(entry)

    return await _send_service_config(
        client,
        _L2_SAP_SERVICE_PATH,
        "l2-sap-reconciler:l2-sap-config",
        device_name,
        {"device": device_name, "sap": saps},
        scope="l2_sap",
        replace=replace,
    )


async def apply_lag_config(
    client: NsoClient,
    device_name: str,
    bundles: list[dict],
    *,
    replace: bool = False,
) -> None:
    """Write LACP/LAG bundle intent for a single device to NSO (M33).

    Builds a full lag-reconciler body from the supplied bundle dicts and commits in
    reconcile mode so pre-existing LAGs are adopted. ``replace=True`` PUT-replaces the
    keyed instance so removed bundles are reverted (the plugin force-pushes the full
    owned snapshot, so the input is already the full desired state).

    Each bundle dict uses YANG-style keys: ``name`` (key), ``lag-id``,
    optional ``min-links``/``system-priority``/``system-id``/``timer``/
    ``admin-key``, and ``member`` (list of ``interface-name`` + optional
    ``mode``/``port-priority``).
    """
    return await _send_service_config(
        client,
        _LAG_SERVICE_PATH,
        "lag-reconciler:lag-config",
        device_name,
        {"device": device_name, "bundle": bundles},
        scope="lag",
        replace=replace,
    )


async def apply_switchport_config(
    client: NsoClient,
    device_name: str,
    interfaces: list[dict],
    *,
    replace: bool = False,
) -> None:
    """Write L2 switchport intent for a single device to NSO (M34).

    Builds a full switchport-reconciler body and commits in reconcile mode. Each
    interface dict uses YANG-style keys: ``interface-name`` (key), optional ``mode``
    (access|trunk|trunk-all), ``untagged-vlan``, and ``tagged-vlan`` (list of ids).
    ``replace=True`` PUT-replaces the keyed instance so removed switchports are
    reverted (the plugin force-pushes the full owned snapshot).
    """
    return await _send_service_config(
        client,
        _SWITCHPORT_SERVICE_PATH,
        "switchport-reconciler:switchport-config",
        device_name,
        {"device": device_name, "interface": interfaces},
        scope="switchport",
        replace=replace,
    )


def build_isis_process_payload(
    isis_process_rows: list | None,
    redistribution_rows: list | None = None,
    flex_algo_rows: list | None = None,
) -> list[dict]:
    """Build the isis-reconciler ``process-config`` payload (with nested redistribute
    and flex-algo) from store rows.  Shared by the apply path and the flex-algo
    removal path (PUT-replace), so both produce identical process-config bodies."""
    redist_by_proc: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        entry: dict = {
            "source-protocol": row.source_protocol,
            "source-ref": row.source_ref,
        }
        if row.route_map:
            entry["route-map"] = row.route_map
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.metric_type:
            entry["metric-type"] = row.metric_type
        redist_by_proc.setdefault(row.dest_ref, []).append(entry)

    processes: list[dict] = []
    for row in isis_process_rows or []:
        entry = {"process-tag": row.process_tag or ""}
        if row.net is not None:
            entry["net"] = row.net
        # Enum leaves reject the empty string — omit when blank (not just None).
        if row.is_type:
            entry["is-type"] = row.is_type
        if row.metric_style:
            entry["metric-style"] = row.metric_style
        if row.overload_bit is not None:
            entry["overload-bit"] = bool(row.overload_bit)
        if row.area_auth_type:
            entry["area-auth-type"] = row.area_auth_type
            if row.area_auth_key is not None:
                entry["area-auth-key"] = row.area_auth_key
        if row.domain_auth_type:
            entry["domain-auth-type"] = row.domain_auth_type
            if row.domain_auth_key is not None:
                entry["domain-auth-key"] = row.domain_auth_key
        proc_redist = redist_by_proc.get(row.process_tag or "", [])
        if proc_redist:
            entry["redistribute"] = proc_redist
        processes.append(entry)

    # Attach Flex-Algo definitions to their process-config entry, creating a
    # minimal entry for any process-tag that has flex-algo but no process row.
    flex_by_proc: dict[str, list[dict]] = {}
    for row in flex_algo_rows or []:
        fa_entry: dict = {"algo-id": int(row.algo_id)}
        if row.metric_type:
            fa_entry["metric-type"] = row.metric_type
        if row.priority is not None:
            fa_entry["priority"] = int(row.priority)
        if row.admin_group_exclude:
            fa_entry["admin-group-exclude"] = row.admin_group_exclude
        if row.admin_group_include_any:
            fa_entry["admin-group-include-any"] = row.admin_group_include_any
        if row.admin_group_include_all:
            fa_entry["admin-group-include-all"] = row.admin_group_include_all
        flex_by_proc.setdefault(row.process_tag or "", []).append(fa_entry)

    if flex_by_proc:
        proc_by_tag = {p["process-tag"]: p for p in processes}
        for tag, fa_list in flex_by_proc.items():
            proc = proc_by_tag.get(tag)
            if proc is None:
                proc = {"process-tag": tag}
                processes.append(proc)
                proc_by_tag[tag] = proc
            proc["flex-algo"] = fa_list

    return processes


async def apply_isis_interfaces(
    client: NsoClient,
    device_name: str,
    isis_intent_rows: list,
    isis_process_rows: list | None = None,
    redistribution_rows: list | None = None,
    flex_algo_rows: list | None = None,
) -> None:
    """Write IS-IS interface-enablement and process intent for a single device to NSO.

    Builds a full isis-reconciler PATCH body from the supplied rows and
    commits with reconcile option so pre-existing IS-IS config is adopted.

    When *isis_process_rows* is provided, a ``process-config`` list is included
    so the reconciler writes process-level config (net, is-type, metric-style,
    overload-bit, area/domain auth) before enabling interfaces.

    *redistribution_rows* rows must have: dest_ref (process_tag), source_protocol,
    source_ref, route_map (optional), metric (optional), metric_type (optional).

    Raises NsoApplyError on failure.
    """
    processes = build_isis_process_payload(isis_process_rows, redistribution_rows, flex_algo_rows)

    interfaces = build_isis_interface_payload(isis_intent_rows)

    service_body: dict = {"device": device_name}
    if interfaces:
        service_body["interface-config"] = interfaces
    if processes:
        service_body["process-config"] = processes

    payload = json.dumps({"isis-reconciler:isis-config": [service_body]})

    url = f"{client._base}{_ISIS_SERVICE_PATH}"

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(
            url,
            content=payload,
            headers={"Content-Type": "application/yang-data+json"},
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.isis_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for IS-IS intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.isis_ok",
        device=device_name,
        interface_count=len(interfaces),
        process_count=len(processes),
    )

    await _verify_native_or_raise(client, url, payload, device_name, scope="isis")


def build_isis_interface_payload(isis_intent_rows: list | None) -> list[dict]:
    """Build the isis-reconciler ``interface-config`` payload from store rows."""
    interfaces: list[dict] = []
    for row in isis_intent_rows or []:
        entry = {
            "interface-name": row.interface_name,
            "af": row.af,
            "process-tag": row.process_tag or "",
            "passive": bool(row.passive) if row.passive is not None else False,
        }
        if row.circuit_type:
            # YANG enum is level-1 | level-2-only | level-1-2; "level-2" is a
            # common alias that the reconciler rejects → normalise it.
            entry["circuit-type"] = "level-2-only" if row.circuit_type == "level-2" else row.circuit_type
        if row.network_type:
            entry["network-type"] = row.network_type
        if row.metric is not None:
            entry["metric"] = row.metric
        interfaces.append(entry)
    return interfaces


async def replace_service_instance(
    client: NsoClient,
    service_path: str,
    root_key: str,
    device_name: str,
    body: dict,
) -> None:
    """RESTCONF PUT-replace a keyed reconciler service instance with the full desired body.

    Generalised removal primitive. Removal must be explicit: a merge-PATCH that omits
    an entry leaves it in the FASTMAP service intent (and on the device), and a
    node-level DELETE 404s on empty-string list keys. PUT on the keyed instance
    (``<service_path>=<device>``) replaces its entire content with *body*, so omitted
    list entries are dropped and FASTMAP reverts the device config.

    *body* is the full desired instance dict (must include the ``device`` key). A body
    with only the device key clears all of this service's managed config for the device.
    Reconcile commit is implied by the service's own commit behaviour.
    """
    url = f"{client._base}{service_path}={device_name}"
    payload = json.dumps({root_key: [body]})
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.put(
            url, content=payload, headers={"Content-Type": "application/yang-data+json"}
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.service_replace_failed",
                service=root_key,
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_put_failed",
                f"NSO PUT-replace for {root_key} failed with status {resp.status_code}",
                detail={"nso_error": err},
            )
    logger.info("nso.apply.service_replaced", service=root_key, device=device_name)


async def replace_isis_service(
    client: NsoClient,
    device_name: str,
    interfaces: list[dict],
    processes: list[dict],
) -> None:
    """PUT-replace the isis-reconciler service instance (full desired state).

    Thin wrapper over :func:`replace_service_instance` used to propagate IS-IS
    removals (e.g. a deleted flex-algo). *interfaces* and *processes* are the FULL
    desired state for the device.
    """
    body: dict = {"device": device_name}
    if interfaces:
        body["interface-config"] = interfaces
    if processes:
        body["process-config"] = processes
    await replace_service_instance(
        client, _ISIS_SERVICE_PATH, "isis-reconciler:isis-config", device_name, body
    )


async def apply_bgp_config(
    client: NsoClient,
    device_name: str,
    router_intent_rows: list,
    redistribution_rows: list | None = None,
    *,
    replace: bool = False,
) -> None:
    """Write BGP intent for a single device to NSO via the bgp-reconciler.

    Builds the full router/scope/AF/peer/peer-AF tree from the supplied
    BgpRouterIntent rows (with relationships eagerly loaded by the caller)
    and commits in reconcile mode so pre-existing BGP config is adopted.
    ``replace=True`` PUT-replaces the keyed instance so removed routers/peers are
    reverted.

    *redistribution_rows* rows must have: dest_ref (f"{asn}:{vrf}:{af}"),
    source_protocol, source_ref, route_map (optional), metric (optional).

    Raises NsoApplyError on failure.
    """
    # Index redistribution by dest_ref
    redist_by_af: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        entry: dict = {
            "source-protocol": row.source_protocol,
            "source-ref": row.source_ref,
        }
        if row.route_map:
            entry["route-map"] = row.route_map
        if row.metric is not None:
            entry["metric"] = row.metric
        redist_by_af.setdefault(row.dest_ref, []).append(entry)

    routers = []
    for r in router_intent_rows:
        scopes_out = []
        for scope in r.scopes:
            afs_out = []
            for af in scope.address_families:
                af_dest_ref = f"{r.asn}:{scope.vrf}:{af.af}"
                af_entry: dict = {"afi": af.af}
                af_redist = redist_by_af.get(af_dest_ref, [])
                if af_redist:
                    af_entry["redistribute"] = af_redist
                afs_out.append(af_entry)
            peers_out = []
            for peer in scope.peers:
                peer_entry: dict = {
                    "peer-address": peer.peer_address,
                    "enabled": peer.enabled,
                }
                if peer.peer_group is not None:
                    peer_entry["peer-group"] = peer.peer_group
                if peer.remote_as is not None:
                    peer_entry["remote-as"] = peer.remote_as
                if peer.local_as is not None:
                    peer_entry["local-as"] = peer.local_as
                if peer.ttl is not None:
                    peer_entry["ttl"] = peer.ttl
                if peer.password is not None:
                    peer_entry["password"] = peer.password
                peer_entry["peer-address-family"] = [
                    {
                        "afi": paf.af,
                        "enabled": paf.enabled,
                        **({"routemap-in": paf.routemap_in} if paf.routemap_in else {}),
                        **({"routemap-out": paf.routemap_out} if paf.routemap_out else {}),
                        **({"prefixlist-in": paf.prefixlist_in} if paf.prefixlist_in else {}),
                        **({"prefixlist-out": paf.prefixlist_out} if paf.prefixlist_out else {}),
                    }
                    for paf in peer.peer_address_families
                ]
                peers_out.append(peer_entry)
            scopes_out.append(
                {
                    "vrf": scope.vrf,
                    "address-family": afs_out,
                    "peer": peers_out,
                }
            )
        routers.append({"asn": int(r.asn), "scope": scopes_out})

    return await _send_service_config(
        client,
        _BGP_SERVICE_PATH,
        "bgp-reconciler:bgp-config",
        device_name,
        {"device": device_name, "router": routers},
        scope="bgp",
        replace=replace,
    )


async def apply_route_policy_config(
    client: NsoClient,
    device_name: str,
    intent_rows: list,
    *,
    replace: bool = False,
) -> None:
    """Write route-policy intent for a single device to NSO via the route-policy-reconciler.

    Groups RoutePolicyObjectIntent rows by family and builds the canonical NSO service
    payload per docs/m17-route-policy-contract.md §2 (reconcile semantics).
    ``replace=True`` PUT-replaces the keyed instance so removed policy objects are
    reverted. Raises NsoApplyError on failure.
    """
    from collections import defaultdict

    by_family: dict[str, list] = defaultdict(list)
    for row in intent_rows:
        by_family[row.family].append({"name": row.name, "entries": row.entries})

    body = {
        "device": device_name,
        "prefix-list": [
            {"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("prefix_list", [])
        ],
        "community-list": [
            {"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("community_list", [])
        ],
        "as-path": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("as_path", [])],
        "route-map": [
            {"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("route_map", [])
        ],
    }
    return await _send_service_config(
        client,
        _ROUTE_POLICY_SERVICE_PATH,
        "route-policy-reconciler:route-policy-config",
        device_name,
        body,
        scope="route_policy",
        replace=replace,
    )


# RESTCONF path to the ospf-reconciler service list
_OSPF_SERVICE_PATH = "/restconf/data/ospf-reconciler:ospf-config"


async def apply_ospf_config(
    client: NsoClient,
    device_name: str,
    process_intent_rows: list,
    interface_intent_rows: list,
    redistribution_rows: list | None = None,
    *,
    replace: bool = False,
    dry_run: bool = False,
) -> str | None:
    """Write OSPF process and interface intent for a single device to NSO.

    Builds a full ospf-reconciler body from the supplied rows and commits in
    reconcile mode so pre-existing OSPF config is adopted. ``replace=True``
    PUT-replaces the keyed instance so removed processes/interfaces are reverted.

    *process_intent_rows* rows must have: process_id, router_id (optional), vrf (optional).
    *interface_intent_rows* rows must have: interface_name, process_id, area_id,
    passive (bool), priority (optional), cost (optional), network_type (optional),
    auth_type (optional), auth_key (optional).
    *redistribution_rows* rows must have: dest_ref (str(process_id)), source_protocol,
    source_ref, route_map (optional), metric (optional), metric_type (optional).

    Raises NsoApplyError on failure.
    """
    # Index redistribution by dest_ref (= str(process_id))
    redist_by_proc: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        entry: dict = {
            "source-protocol": row.source_protocol,
            "source-ref": row.source_ref,
        }
        if row.route_map:
            entry["route-map"] = row.route_map
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.metric_type:
            entry["metric-type"] = row.metric_type
        redist_by_proc.setdefault(row.dest_ref, []).append(entry)

    processes = []
    for row in process_intent_rows:
        entry = {"process-id": int(row.process_id)}
        if row.router_id:
            entry["router-id"] = row.router_id
        if row.vrf:
            entry["vrf"] = row.vrf
        proc_redist = redist_by_proc.get(str(row.process_id), [])
        if proc_redist:
            entry["redistribute"] = proc_redist
        processes.append(entry)

    interfaces = []
    for row in interface_intent_rows:
        entry = {
            "interface-name": row.interface_name,
            "process-id": int(row.process_id),
            "area-id": row.area_id,
            "passive": bool(row.passive) if row.passive is not None else False,
        }
        if row.priority is not None:
            entry["priority"] = int(row.priority)
        if row.cost is not None:
            entry["cost"] = int(row.cost)
        if row.network_type:
            entry["network-type"] = row.network_type
        if row.auth_type:
            entry["auth-type"] = row.auth_type
            if row.auth_key:
                entry["auth-key"] = row.auth_key
        interfaces.append(entry)

    service_body: dict = {
        "device": device_name,
        "interface-config": interfaces,
    }
    if processes:
        service_body["process-config"] = processes

    return await _send_service_config(
        client,
        _OSPF_SERVICE_PATH,
        "ospf-reconciler:ospf-config",
        device_name,
        service_body,
        scope="ospf",
        replace=replace,
        dry_run=dry_run,
    )
