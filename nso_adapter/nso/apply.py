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

import structlog

from nso_adapter.nso.client import NsoClient

logger = structlog.get_logger(__name__)

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
) -> None:
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


_SNMP_SERVICE_PATH = "/restconf/data/snmp-reconciler:snmp-config"

# RESTCONF path to the static-route-reconciler service list
_STATIC_ROUTE_SERVICE_PATH = "/restconf/data/static-route-reconciler:static-route-config"

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
) -> None:
    """Write the full SNMP intent snapshot for a device to the snmp-reconciler service.

    Builds a single PATCH body covering all communities (by label + vault_ref),
    v3 users, hosts, and system info.  The snmp-reconciler NSO service resolves
    Vault refs at commit time — vault_refs are passed verbatim.

    Raises NsoApplyError on failure.
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

    url = f"{client._base}{_SNMP_SERVICE_PATH}"
    payload = json.dumps({"snmp-reconciler:snmp-config": [entry]})

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
                "nso.apply.snmp_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO SNMP PATCH failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.snmp_ok",
        device=device_name,
        community_count=len(community_intents),
        v3_user_count=len(v3_user_intents),
        host_count=len(host_intents),
    )


async def apply_static_routes(
    client: NsoClient,
    device_name: str,
    route_intent_rows: list,
) -> None:
    """Write static route intent for a single device to NSO.

    Builds a full static-route-reconciler PATCH body from the supplied rows
    and commits with reconcile option so pre-existing routes are adopted.

    Raises NsoApplyError on failure.
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

    payload = json.dumps({"static-route-reconciler:static-route-config": [{"device": device_name, "route": routes}]})

    url = f"{client._base}{_STATIC_ROUTE_SERVICE_PATH}"

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
                "nso.apply.static_route_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for static-route intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.static_route_ok",
        device=device_name,
        route_count=len(routes),
    )


async def apply_l2_saps(
    client: NsoClient,
    device_name: str,
    sap_intent_rows: list,
) -> None:
    """Write Nokia L2 SAP intent for a single device to NSO (M37 P2b).

    Builds a full l2-sap-reconciler PATCH body from the supplied rows and
    commits with reconcile option so pre-existing SAPs are adopted. The NSO
    service adds each SAP under an EXISTING epipe/vpls service (SAP-only).

    Raises NsoApplyError on failure.
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

    payload = json.dumps({"l2-sap-reconciler:l2-sap-config": [{"device": device_name, "sap": saps}]})

    url = f"{client._base}{_L2_SAP_SERVICE_PATH}"

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
                "nso.apply.l2_sap_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for L2 SAP intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.l2_sap_ok",
        device=device_name,
        sap_count=len(saps),
    )


async def apply_lag_config(
    client: NsoClient,
    device_name: str,
    bundles: list[dict],
) -> None:
    """Write LACP/LAG bundle intent for a single device to NSO (M33).

    Builds a full lag-reconciler PATCH body from the supplied bundle dicts and
    commits with reconcile option so pre-existing LAGs are adopted. The bundle
    list is full-replace: FASTMAP removes bundles absent from the payload.

    Each bundle dict uses YANG-style keys: ``name`` (key), ``lag-id``,
    optional ``min-links``/``system-priority``/``system-id``/``timer``/
    ``admin-key``, and ``member`` (list of ``interface-name`` + optional
    ``mode``/``port-priority``).

    Raises NsoApplyError on failure.
    """
    payload = json.dumps(
        {"lag-reconciler:lag-config": [{"device": device_name, "bundle": bundles}]}
    )

    url = f"{client._base}{_LAG_SERVICE_PATH}"

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
                "nso.apply.lag_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for LAG intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.lag_ok",
        device=device_name,
        bundle_count=len(bundles),
    )


async def apply_switchport_config(
    client: NsoClient,
    device_name: str,
    interfaces: list[dict],
) -> None:
    """Write L2 switchport intent for a single device to NSO (M34).

    Builds a full switchport-reconciler PATCH body and commits in reconcile mode.
    Each interface dict uses YANG-style keys: ``interface-name`` (key), optional
    ``mode`` (access|trunk|trunk-all), ``untagged-vlan``, and ``tagged-vlan``
    (list of ids). The interface list is full-replace (FASTMAP removes absent).

    Raises NsoApplyError on failure.
    """
    payload = json.dumps(
        {"switchport-reconciler:switchport-config": [{"device": device_name, "interface": interfaces}]}
    )

    url = f"{client._base}{_SWITCHPORT_SERVICE_PATH}"

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
                "nso.apply.switchport_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for switchport intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.switchport_ok",
        device=device_name,
        interface_count=len(interfaces),
    )


async def apply_isis_interfaces(
    client: NsoClient,
    device_name: str,
    isis_intent_rows: list,
    isis_process_rows: list | None = None,
    redistribution_rows: list | None = None,
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
    # Index redistribution by dest_ref (= process_tag)
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
    for row in isis_process_rows or []:
        entry = {"process-tag": row.process_tag or ""}
        if row.net is not None:
            entry["net"] = row.net
        if row.is_type is not None:
            entry["is-type"] = row.is_type
        if row.metric_style is not None:
            entry["metric-style"] = row.metric_style
        if row.overload_bit is not None:
            entry["overload-bit"] = bool(row.overload_bit)
        if row.area_auth_type is not None:
            entry["area-auth-type"] = row.area_auth_type
            if row.area_auth_key is not None:
                entry["area-auth-key"] = row.area_auth_key
        if row.domain_auth_type is not None:
            entry["domain-auth-type"] = row.domain_auth_type
            if row.domain_auth_key is not None:
                entry["domain-auth-key"] = row.domain_auth_key
        proc_redist = redist_by_proc.get(row.process_tag or "", [])
        if proc_redist:
            entry["redistribute"] = proc_redist
        processes.append(entry)

    interfaces = []
    for row in isis_intent_rows:
        entry = {
            "interface-name": row.interface_name,
            "af": row.af,
            "process-tag": row.process_tag or "",
            "passive": bool(row.passive) if row.passive is not None else False,
        }
        if row.circuit_type is not None:
            entry["circuit-type"] = row.circuit_type
        if row.network_type is not None:
            entry["network-type"] = row.network_type
        if row.metric is not None:
            entry["metric"] = row.metric
        interfaces.append(entry)

    service_body: dict = {"device": device_name, "interface-config": interfaces}
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


async def apply_bgp_config(
    client: NsoClient,
    device_name: str,
    router_intent_rows: list,
    redistribution_rows: list | None = None,
) -> None:
    """Write BGP intent for a single device to NSO via bgp-reconciler PATCH.

    Builds the full router/scope/AF/peer/peer-AF tree from the supplied
    BgpRouterIntent rows (with relationships eagerly loaded by the caller)
    and commits with reconcile option so pre-existing BGP config is adopted.

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
                af_entry: dict = {"af": af.af}
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
                peer_entry["address-family"] = [
                    {
                        "af": paf.af,
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

    payload = json.dumps({"bgp-reconciler:bgp-config": [{"device": device_name, "router": routers}]})
    url = f"{client._base}{_BGP_SERVICE_PATH}"

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
                "nso.apply.bgp_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for BGP intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    peer_count = sum(len(scope["peer"]) for r in routers for scope in r["scope"])
    logger.info(
        "nso.apply.bgp_ok",
        device=device_name,
        router_count=len(routers),
        peer_count=peer_count,
    )


async def apply_route_policy_config(
    client: NsoClient,
    device_name: str,
    intent_rows: list,
) -> None:
    """Write route-policy intent for a single device to NSO via route-policy-reconciler PATCH.

    Groups RoutePolicyObjectIntent rows by family and builds the canonical NSO service
    payload per docs/m17-route-policy-contract.md §2.  Commits with PATCH (reconcile
    semantics on the service list).

    Raises NsoApplyError on failure.
    """
    from collections import defaultdict

    by_family: dict[str, list] = defaultdict(list)
    for row in intent_rows:
        by_family[row.family].append({"name": row.name, "entries": row.entries})

    payload = json.dumps(
        {
            "route-policy-reconciler:route-policy-config": [
                {
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
            ]
        }
    )
    url = f"{client._base}{_ROUTE_POLICY_SERVICE_PATH}"

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
                "nso.apply.route_policy_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for route-policy intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    obj_count = sum(len(v) for v in by_family.values())
    logger.info(
        "nso.apply.route_policy_ok",
        device=device_name,
        object_count=obj_count,
        families=list(by_family.keys()),
    )


# RESTCONF path to the ospf-reconciler service list
_OSPF_SERVICE_PATH = "/restconf/data/ospf-reconciler:ospf-config"


async def apply_ospf_config(
    client: NsoClient,
    device_name: str,
    process_intent_rows: list,
    interface_intent_rows: list,
    redistribution_rows: list | None = None,
) -> None:
    """Write OSPF process and interface intent for a single device to NSO.

    Builds a full ospf-reconciler PATCH body from the supplied rows and
    commits with reconcile option so pre-existing OSPF config is adopted.

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

    payload = json.dumps({"ospf-reconciler:ospf-config": [service_body]})
    url = f"{client._base}{_OSPF_SERVICE_PATH}"

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
                "nso.apply.ospf_patch_failed",
                device=device_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO PATCH for OSPF intent failed with status {resp.status_code}",
                detail={"nso_error": err},
            )

    logger.info(
        "nso.apply.ospf_ok",
        device=device_name,
        process_count=len(processes),
        interface_count=len(interfaces),
    )
