# nso-adapter

Middleware service between Cisco NSO and consumers (NetBox first). Speaks NSO
RESTCONF; exposes a consumer-agnostic northbound REST API. Internally split
into a generic NSO core and pluggable consumer bindings; a NetBox binding ships
with it.

## Documentation

- [`docs/00-plan.md`](docs/00-plan.md) — overall plan for the NSO ↔ NetBox
  integration (cross-cutting; lives here because the adapter implements the
  contract).
- [`docs/api-contract.md`](docs/api-contract.md) — canonical northbound REST
  API contract. The NetBox plugin builds against this.
- [`docs/nso-adapter.md`](docs/nso-adapter.md) — adapter design (this repo).

Companion repos: **[`netbox-nso-plugin`](../netbox-nso-plugin)** — the NetBox
plugin that consumes this adapter; **`nso-packages`** — the NSO-side YANG
service packages (`network-state-export` read exports + the `*-reconciler`
write services) the adapter drives over RESTCONF.

## Status

In active development (Phase 2, 2026-06). The northbound API serves ~57
endpoints across 16 config families (interfaces/IPs/MTU, VLAN/switchport,
SVI, subinterfaces, L2 services, LAG, IS-IS, OSPF, BGP, route-policy,
redistribution, static routes, BFD, SNMP, logging) — read mirrors plus a
full-replace intent store with a durable apply worker, post-apply dry-run
verification, periodic + SSE-triggered sync, and an intent-summary endpoint
for split-brain detection. `docs/api-contract.md` is kept in lock-step with
the implemented surface; `docs/00-plan.md` §6/§7 records the original
Phase 1 plan.
