# nso-adapter

Middleware service between Cisco NSO and consumers (NetBox first). Speaks NSO
RESTCONF; exposes a consumer-agnostic northbound REST API. Internally split
into a generic NSO core and pluggable consumer bindings; a NetBox binding ships
with it.

## Documentation

[`docs/api-contract.md`](docs/api-contract.md) is the canonical northbound REST
API contract. The NetBox plugin builds against it.

Companion repo **[`netbox-nso-plugin`](../netbox-nso-plugin)** provides the
NetBox plugin that consumes this adapter. **`nso-packages`** contains the
NSO-side YANG service packages (`network-state-export` read exports and the
`*-reconciler` write services) that the adapter drives over RESTCONF.

## Status

In active development (Phase 2, 2026-06). The northbound API serves ~57
endpoints across 16 config families (interfaces/IPs/MTU, VLAN/switchport,
SVI, subinterfaces, L2 services, LAG, IS-IS, OSPF, BGP, route-policy,
redistribution, static routes, BFD, SNMP, and logging). It provides read mirrors
plus a full-replace intent store with a durable apply worker, post-apply dry-run
verification, periodic + SSE-triggered sync, and an intent-summary endpoint
for split-brain detection. `docs/api-contract.md` is kept in lock-step with
the implemented surface.
