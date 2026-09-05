# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The in-protocol intent-PUT registry: what receipt admission keys on (#1522 §G2/§G5).

Admission keys on the ENDPOINT the plugin's outbox delivered to, not on the removal scopes.
The plugin runs one claim sequence per intent family, so two families sharing one receipt
row would each read the other's sequences as ``stale``; and two families that HAVE a PUT but
no removal scope — the interface-IP intent and the IS-IS flex-algo intent — could never be
admitted at all while the vocabulary came from :data:`core.removal.VALID_REMOVAL_SCOPES`.

That per-endpoint lane is the ``stream``, and it is the AUTHORIZATION unit as well as the
replay one: each stream owns an explicit set of intent tables (:mod:`core.projection`), and
promoting it authorizes those tables and no others.

Each entry also names the PROJECTION section the endpoint's writes contribute to: the family
whose document the deployment is built from. The two vocabularies are deliberately different
sizes. Sixteen streams compose fourteen families — ``ip`` contributes to ``interface_config``
(one device document carries interface attributes and their addresses) and ``isis_flex_algo``
to ``isis`` — so collapsing them into one name would either lose a replay lane or let one
lane's push authorize the other's un-promoted state.

``interface_config`` keeps its spelling because the plugin already maps its ``interface``
family onto that string on the wire; ``ip`` and ``isis_flex_algo`` are the plugin's own
names, verbatim.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IntentEndpoint:
    """One in-protocol intent PUT.

    *stream* is the receipt key and the promotion unit — one per endpoint, because it
    identifies one outbox lane and one set of owned tables. *promotes* is the projection
    section this lane's fragment composes into.
    """

    stream: str
    promotes: str


#: Route path -> its protocol entry. THE source of the admission vocabulary: paths are what
#: the ASGI scope hands back, so an endpoint resolves its own stream rather than repeating
#: it, and ``tests/core/test_receipt_admission.py`` pins this map against the live route
#: table so a new PUT cannot silently fall out of the protocol.
INTENT_PUT_ENDPOINTS: dict[str, IntentEndpoint] = {
    "/api/v1/devices/{device_id}/bfd-intent": IntentEndpoint("bfd", "bfd"),
    "/api/v1/devices/{device_id}/bgp-intent": IntentEndpoint("bgp", "bgp"),
    "/api/v1/devices/{device_id}/intent": IntentEndpoint("interface_config", "interface_config"),
    "/api/v1/devices/{device_id}/interface-mtu-intent": IntentEndpoint("interface_mtu", "interface_mtu"),
    "/api/v1/devices/{device_id}/ip-intent": IntentEndpoint("ip", "interface_config"),
    "/api/v1/devices/{device_id}/isis-flex-algo-intent": IntentEndpoint("isis_flex_algo", "isis"),
    "/api/v1/devices/{device_id}/isis-interface-intent": IntentEndpoint("isis", "isis"),
    "/api/v1/devices/{device_id}/l2-sap-intent": IntentEndpoint("l2_sap", "l2_sap"),
    "/api/v1/devices/{device_id}/logging-intent": IntentEndpoint("logging", "logging"),
    "/api/v1/devices/{device_id}/ospf-intent": IntentEndpoint("ospf", "ospf"),
    "/api/v1/devices/{device_id}/route-policy-intent": IntentEndpoint("route_policy", "route_policy"),
    "/api/v1/devices/{device_id}/snmp-intent": IntentEndpoint("snmp", "snmp"),
    "/api/v1/devices/{device_id}/static-route-intent": IntentEndpoint("static_route", "static_route"),
    "/api/v1/devices/{device_id}/subinterface-intent": IntentEndpoint("subinterface", "subinterface"),
    "/api/v1/devices/{device_id}/svi-intent": IntentEndpoint("svi", "svi"),
    "/api/v1/devices/{device_id}/vlan-intent": IntentEndpoint("vlan", "vlan"),
}

#: The PUT endpoints that are NOT intent deliveries and therefore carry no claim: the
#: adapter's own failover configuration and the managed-scope declaration. The ratified
#: #1503 contract also keeps lacp and switchport out of the protocol — those are POSTs, so
#: they never reach this map.
OUT_OF_PROTOCOL_PUTS: frozenset[str] = frozenset(
    {
        "/api/v1/config/failover",
        "/api/v1/devices/{device_id}/scope",
    }
)

#: Apply-POST route path -> the projection stream it prepares (#1612). These two families
#: are stored by a POST that carries no claim, so they have no receipt lane and no replay:
#: the POST prepares a snapshot and the manual Apply authorizes it. The route path is THE
#: source, and the stream set below is derived from it, so the two cannot drift apart.
OUT_OF_PROTOCOL_APPLY_POSTS: dict[str, str] = {
    "/api/v1/devices/{device_id}/lag-config/apply": "lag",
    "/api/v1/devices/{device_id}/switchport/apply": "switchport",
}

#: The two projection streams no intent PUT delivers to.
OUT_OF_PROTOCOL_STREAMS: frozenset[str] = frozenset(OUT_OF_PROTOCOL_APPLY_POSTS.values())

#: Every stream receipt admission accepts. Sixteen: the two out-of-protocol streams are
#: deliberately absent, so ``admit_push`` can never admit them.
INTENT_STREAMS: frozenset[str] = frozenset(endpoint.stream for endpoint in INTENT_PUT_ENDPOINTS.values())


def intent_endpoint(route_path: str) -> IntentEndpoint:
    """Return the protocol entry for *route_path*, or raise.

    A PUT that reaches the delivery dependency without an entry is a wiring bug: admitting
    it under a guessed stream would dedupe one outbox lane against another's sequences.
    """
    endpoint = INTENT_PUT_ENDPOINTS.get(route_path)
    if endpoint is None:
        raise RuntimeError(f"{route_path!r} is not a registered in-protocol intent PUT")
    return endpoint


__all__ = [
    "INTENT_PUT_ENDPOINTS",
    "INTENT_STREAMS",
    "OUT_OF_PROTOCOL_APPLY_POSTS",
    "OUT_OF_PROTOCOL_PUTS",
    "OUT_OF_PROTOCOL_STREAMS",
    "IntentEndpoint",
    "intent_endpoint",
]
