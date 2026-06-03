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

Companion repo: **[`netbox-nso-plugin`](../netbox-nso-plugin)** — the NetBox
plugin that consumes this adapter.

## Status

Pre-implementation (2026-05-23). See `docs/00-plan.md` §6 for Phase 1
milestones and §7 for spikes.
