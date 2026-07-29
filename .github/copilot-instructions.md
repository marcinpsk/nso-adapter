# GitHub Copilot Instructions — `nso-adapter`

## Repo Overview

This is the **`nso-adapter`** repo — a standalone FastAPI middleware between
Cisco NSO 6.7 and consumers (NetBox 4.6 first). **Polyrepo (three repos)**:
the companion **`netbox-nso-plugin`** repo (`../netbox-nso-plugin/`) consumes
this adapter via its REST API, and **`nso-packages`** (`../nso-packages/`)
holds the NSO-side YANG/Python — `interface-reconciler` (the Phase 2 apply
service), `vault-cred-manager`, and the Phase 3 `network-state-export`
exporter (M9 LAG topology, consumed here over RESTCONF SSE).

Cross-cutting docs live in this repo under `docs/`:

- `docs/00-plan.md` — phasing, architectural decisions, spikes
- `docs/api-contract.md` — canonical northbound REST API (the plugin builds against this)
- `docs/nso-adapter.md` — adapter design (modules, DB schema, NACM, secrets)

## NSO RESTCONF (dev environment)

Concrete hostnames, IPs, and account names for the dev instance live in
`LOCAL-DEV.md` (gitignored). Use the placeholders below; resolve them from
`LOCAL-DEV.md`.

- Base URL (inside containers): `<NSO_RESTCONF_BASE>`
- Auth: HTTP Basic. Use the dedicated NSO service account `<NSO_SVC_USER>`,
  which already exists in NSO AAA and whose credentials are in Vault at
  `<VAULT_KV_MOUNT>/<VAULT_PATH_NSO_SVC>` (existing keys `username`,
  `password`) — **never use `admin`**. Least-privilege NACM rule-list per
  `docs/nso-adapter.md` §3.1.
- **REQUIRED**: `Host: <NSO_SERVER_NAME>` header on every request (NSO webui
  `server-name` is set; without the matching `Host:` you get HTTP 400).
- Protocol: HTTP (not HTTPS) for dev; `Accept` / `Content-Type`:
  `application/yang-data+json`.
- **Proxy note**: host has `http_proxy` env var — always set `NO_PROXY` to
  bypass the NSO and Vault hosts (see `LOCAL-DEV.md`) when running on host
  or in containers.
- Smoke test (no devices in NSO yet): `GET /restconf/data/tailf-ncs:devices`

## NSO NEDs and the interface slice (Phase 1)

| NED | YANG prefix | Interface path |
|-----|-------------|----------------|
| `cisco-ios-cli-6.114` | `tailf-ned-cisco-ios` | `tailf-ned-cisco-ios:interface` |
| `cisco-iosxr-cli-7.76` | `tailf-ned-cisco-ios-xr` | `tailf-ned-cisco-ios-xr:interface` |
| `cisco-nx-cli-5.32` | `tailf-ned-cisco-nx` | `tailf-ned-cisco-nx:interface` |

- `enabled` maps to **absence** of the `shutdown` leaf on all IOS-family NEDs.
- Interface-name normalization is NED-specific (`GigabitEthernet0/0/0/1` vs
  `Gi0/0/0/1`); see spike S1 in `docs/00-plan.md` §7 and `nso_adapter/nso/neds.py`.

## Phase status (`docs/00-plan.md` §6.6)

- **Phase 1** — complete. Device-config-layer reads; actions `sync-from`,
  `compare-config`, `check-sync`, `connect`.
- **Phase 2** — complete. Intent ownership + apply via the thin NSO
  reconcile-commit service (Spike **S2** / **M4**), per-attribute lifecycle.
- **Phase 3** — **in-flight** (`docs/00-plan.md` §6.6). **M7 ✅**
  device-matching API, **M8 ✅** derived intent (plugin-side), **S3 ✅** NSO
  notification spike (`docs/s3-nso-notifications-findings.md` — `SSESubscriber`
  built; live-stream validation gated on the S3-1 NACM grant). **M9 in
  design** — LAG topology via a new NSO `network-state-export` package
  consumed over RESTCONF SSE; contract + plan at `docs/m9-lag-topology.md` /
  `docs/m9-lag-topology-plan.md`. Event streams are now **in scope** (see the
  flipped guardrail below).

### Phase 2 endpoints (`docs/api-contract.md`)

New endpoints introduced in Phase 2:

- `PUT /api/v1/devices/{id}/intent` — plugin pushes the device's full intent
  snapshot; adapter writes the mirror (`interface_intent` table).
- `GET /api/v1/devices/{id}/intent` — read back the mirror.
- `POST /api/v1/devices/{id}/actions/apply` — push intent to NSO; `force`
  body flag (default `true`) controls whether `in_sync` attrs are re-pushed.

Extended:

- `PUT /api/v1/devices/{id}/scope` body now also carries `auto_apply`
  (Phase 2, optional, default `false`).
- `GET /api/v1/devices/{id}/interfaces` `attrs` now carries `intent_value`,
  `last_apply_at`, `last_apply_error` (null in Phase 1).
- `compliance_status` enum gains `accepted`, `deploying`, `apply_failed`.

### Per-attribute state machine (`docs/nso-adapter.md` §8)

```
unknown → imported ─┬→ changed → imported       (re-sync re-baselines)
                    └→ accepted → deploying ─┬→ in_sync  ─→ drifted ─→ deploying
                                             └→ apply_failed ─→ deploying (retry)
```

`changed` and `drifted` are distinct: `changed` is pre-accept drift,
`drifted` is post-deploy drift. Apply failures keep the attribute
`apply_failed` (not `accepted`); intent value untouched; **no automatic
rollback** (decision O). Apply jobs snapshot `interface_intent` into
`job.context.intent_snapshot` at job start — that snapshot is the audit
trail and is what subsequent retries / status updates work against, **not**
the live mirror.

## Vault

Concrete address / mount / paths are in `LOCAL-DEV.md` (gitignored).

- Address: `<VAULT_ADDR>`
- Auth: AppRole — `VAULT_ROLE_ID` + `VAULT_SECRET_ID` injected into the
  adapter's `.env` by Ansible (Compose) / ESO (k8s).
- KV v2 mount: `<VAULT_KV_MOUNT>`
- NSO service-account secret path: `<VAULT_PATH_NSO_SVC>`
  (existing keys: `username`, `password` — **do not rename**).

### How the adapter consumes secrets — `*_ref` model (`docs/nso-adapter.md` §10)

Config holds *references*, never secret values. References are `path#field`
for the `vault` provider, or an env-var name (with optional `<VAR>_FILE`
fallback for Docker / k8s mounted-secret files) for the `local` provider.
**Do not hard-code logical key names** like `nso_password`, `netbox_token`,
`adapter_bearer_token` — the adapter resolves whatever the `*_ref` points at,
including the existing `username`/`password` keys at the path above.

```yaml
nso_instances:
  - name: nso-dev
    base_url: <NSO_BASE_URL>
    username_ref: "<VAULT_PATH_NSO_SVC>#username"
    password_ref: "<VAULT_PATH_NSO_SVC>#password"
netbox:
  api_token_ref: "<VAULT_PATH_NETBOX_TOKEN>#token"
api:
  adapter_token_ref: "<VAULT_PATH_ADAPTER_TOKEN>#adapter_token"
```

## Canonical token name — `adapter_token`

The shared secret authenticating the NetBox plugin to this adapter is called
**`adapter_token`** everywhere. Same value, same name:

| Surface                          | Name                  |
|----------------------------------|-----------------------|
| HTTP header (api-contract.md)    | `Authorization: Bearer <adapter_token>` |
| Adapter config field             | `api.adapter_token_ref` |
| Adapter env var (`local` provider) | `ADAPTER_TOKEN` (or whatever `adapter_token_ref` points at) |
| Vault key (KV v2)                | `adapter_token`       |
| Plugin `PLUGINS_CONFIG` key      | `adapter_token`       |
| Plugin env var                   | `NSO_ADAPTER_TOKEN`   |

Self-generated (`openssl rand -hex 32`), stored once in Vault, read by both
the adapter (via `adapter_token_ref`) and the NetBox plugin (env-injected
into NetBox's process). One Vault entry, two consumers. Do not introduce
parallel names (`adapter_bearer_token`, `api_token`, `bearer_token`, etc.).

## API Contract — `docs/api-contract.md` is canonical

- All endpoints require `Authorization: Bearer <adapter_token>`.
- Versioned at `/api/v1`. Async actions return `202` with `{job_id}`.
- **Error shape** (per `api-contract.md`):
  ```json
  {"error": {"code": "snake_case", "message": "...", "detail": {}}}
  ```
  Codes: `unauthorized`, `not_found`, `validation_error`, `nso_unreachable`,
  `netbox_unreachable`, `conflict`, `internal`.
- Concurrency: one job per device at a time — a second request returns
  `409 conflict` with the running job id in `error.detail.job_id`.
- Device ID = integer PK in the adapter DB, **not** the NSO device name.
- `PATCH /devices/{id}` re-keys mapping (changing `nso_device_name` or
  `nso_instance`); interface state is cleared, job history retained.

## Coding Conventions

- Python 3.12, `uv` for package management.
- `# SPDX-License-Identifier: Apache-2.0` header on every Python file.
- Ruff `line-length = 120`; the canonical ignore set lives in this repo's
  `pyproject.toml` (do **not** assume parity with the plugin repo).
- Async throughout: `async def`, `httpx.AsyncClient`, SQLAlchemy
  `AsyncSession`.
- `structlog` for logging (not stdlib `logging` directly).
- `pydantic-settings` for config; non-secret config in `config.yaml`;
  secrets via `SecretsProvider`.
- `SecretsProvider` — `Protocol` with `get(name: str) -> str`; two
  implementations: `local` (env var + `<VAR>_FILE` fallback) and `vault`
  (`hvac`, AppRole, KV v2). `local` is the default (matches Ansible / ESO ops).
- SQLAlchemy 2 mapped columns; Alembic for migrations.

## Running tests

**Use `make test`. Do not invoke `pytest` directly and do not `pip install` anything to make tests work.** The Makefile target runs `uv run -- pytest tests/ -v` from the project root, which uses the lockfile-pinned environment from `uv.lock` — that environment already has `pytest`, `pytest-asyncio`, `pytest-cov`, `httpx`, and everything else the suite needs.

Common variants:

```bash
make test                                # full suite
uv run -- pytest tests/api/test_api.py   # one file
uv run -- pytest tests/api -k onboard    # filter by keyword
uv run -- pytest --cov-report=html       # HTML coverage report under htmlcov/
```

The suite uses `httpx.ASGITransport` against `create_app()` for API tests (in-process, no live server), a `tmp_path` config fixture, and `monkeypatch`-stubbed NSO/NetBox clients. PostgreSQL is the only engine, in tests as in production: every DB-backed test gets a private database cloned from a template built by `alembic upgrade head`, so it needs the throwaway test server (`adapter-test-db`, `NSO_ADAPTER_TEST_DB_URL`) running — there is no skip lane. NSO and Vault are still stubbed; live NSO/Vault checks belong to manual / acceptance tests per the milestone plan docs (e.g. `docs/m7-match-api.md` §6). `tests/store/test_schema_parity.py` asserts `Base.metadata` still equals the alembic head.

If `make test` fails on missing tools, run `uv sync --dev` to install the dev dependency group; if `uv` itself is missing, this is not the expected dev environment.

Testing strategy + the scenarios each milestone owns: `docs/testing-strategy.md`. Read it before signing off on any milestone.

## Hard guardrails (do not violate)

- **Phase boundaries are real.** Phase 1 (device-config reads) and Phase 2
  (intent + apply via the M4 reconcile-commit service — and **only** that
  service, not arbitrary NSO service work) are complete. Phase 3 is
  **in-flight**: NSO event streams are now in scope via the
  `network-state-export` SSE pattern (S3 spike done), gated behind
  `ENABLE_NSO_STREAMS`. Still out of scope until explicitly kicked off:
  multi-NSO at scale, adapter / NetBox HA.
- **Apply jobs work from `job.context.intent_snapshot`, never from the live
  `interface_intent` mirror.** A `PUT /intent` landing mid-job must not
  redirect what an in-flight apply is committing.
- **No adapter-side drift-remediation _push_ scheduler.** NSO owns drift
  long-term (`docs/00-plan.md` §11 decision M); the adapter must not grow
  scheduled push behaviour. (M9's APScheduler job is a _read-side_
  LAG-topology refresh fallback for when SSE is down — it pulls operational
  state, it does not push intent. Do not read "scheduler now allowed" as
  license to add drift push back.)
- **No adapter → plugin HTTP calls** (`docs/api-contract.md` §Call
  directions). "Read from plugin" always goes through NetBox's REST.
- **Secrets never logged.** Tokens, passwords, bearer values stay out of
  logs, exception messages, and debug dumps.
- **No hard-coded Vault paths or key names.** Always resolve through the
  configurable `*_ref` references.
- **TLS verification on by default** for both NSO and Vault clients
  (`verify=True`); CA paths come from config, never disabled in code.
- **Fail fast on missing secrets** at startup with a clear message naming
  the unresolved reference — never a vague 401 later.
- **No file-level `omit` entries in `[tool.coverage.run]`.** The only
  acceptable file-level exclusions are `*/migrations/*`, `*/tests/*`, and
  `*/alembic/*`. Do not add `scheduler.py`, `importer.py`, `secrets/vault.py`,
  or any other module on the basis that it "needs integration tests" or
  "requires live infrastructure" — extract the pure logic into testable
  helpers and let the lifecycle wrapper carry low coverage as a signal,
  not hide it behind an exclusion. For genuinely unreachable lines use
  `# pragma: no cover`. Full rationale in `docs/testing-strategy.md` §5.1.
  Existing exclusions for `scheduler.py`, `importer.py`, and `vault.py`
  are grandfathered pending a paydown pass — do not add new ones, and
  remove them as part of any change that touches those files.
