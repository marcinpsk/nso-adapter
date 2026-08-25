# nso-adapter Northbound API Contract

- **Status:** draft for implementation handover
- **Consumers:** `netbox-nso-plugin` (Phase 1); other consumers later.
- **This file is canonical.** Both teams build against it. The plugin team
  mocks these endpoints; the adapter team also serves an auto-generated
  OpenAPI document from FastAPI at `/openapi.json` that must match this file.
  Keeping this prose in step with the served schema is a manual discipline, but
  three test suites keep the *served* schema itself honest and change-controlled
  so it cannot drift silently between reviews:
  - **`tests/api/test_openapi_snapshot.py`** — a schema-change review gate:
    `create_app().openapi()` must equal the committed `tests/api/openapi_snapshot.json`
    (regenerate deliberately via `uv run --native-tls -- python -m tests.api.gen_openapi --write`).
    It also asserts no disambiguation-qualified component names and no dangling internal `$ref`s.
    Framing: the snapshot flags any *schema* change but cannot catch a handler that emits a
    key its `response_model` lacks (the rendered schema is unchanged) — that is the golden tests' job.
  - **`tests/api/test_golden_*.py`** — per-endpoint response-body byte-identity (maximal / empty /
    nullable variants), the drop guard the snapshot structurally cannot provide.
  - **`tests/api/test_error_codes.py`** — pins the closed error-code set: every `api_error(` call
    site ⊆ `ERROR_CODES` ⊆ the enum below.

---

## Conventions

- Base path: `/api/v1`. All bodies JSON (`application/json`).
- **Auth:** `Authorization: Bearer <adapter_token>`. Static token, called
  `adapter_token` on **both** sides (plugin env `NSO_ADAPTER_TOKEN` →
  `PLUGINS_CONFIG["netbox_nso_plugin"]["adapter_token"]`; adapter config
  `api.adapter_token_ref`). Missing/invalid → `401`.
- Timestamps: ISO-8601 UTC.
- Async operations return a **job**; the consumer polls `GET /jobs/{id}`.
- **`X-Store-Incarnation`** is set on every `200` from `GET /api/v1/jobs`. It carries the
  live incarnation of the adapter store — a new random value each time the store is
  rebuilt from an empty schema. It is a header and not a body field for two reasons: the
  default job page must stay byte-identical for the consumers that already read it, and
  the value has to be readable on an **empty** page, which is exactly what a cursor
  belonging to a dead store returns. See [The ordered settlement feed](#the-ordered-settlement-feed).
- Error body (all non-2xx):
  ```json
  { "error": { "code": "string", "message": "human readable", "detail": {} } }
  ```
  Codes (closed set — `/openapi.json` enum must match; mechanically enforced
  by `nso_adapter/api/errors.py::ERROR_CODES` + `tests/api/test_error_codes.py`,
  which pin call sites ⊆ ERROR_CODES ⊆ this document):
  - Phase 1: `unauthorized`, `not_found`, `method_not_allowed`, `validation_error`,
    `nso_unreachable`, `netbox_unreachable`, `conflict`, `internal`.
  - Phase 2: `not_implemented` (apply endpoint pre-M4),
    `nso_commit_failed` (M5+, reconcile-commit refused or partially
    failed; `error.detail.attributes` lists the failed ones), and
    `apply_unexecutable` (409, a selected stream cannot be delivered faithfully by its
    existing apply or removal runner; `error.detail.streams` maps each stream to its reason).
  - Per-endpoint: `ambiguous_device` (device lookup matches >1),
    `bad_request` (malformed action parameter), `community_not_found`
    (SNMP harvest), `harvest_unsupported_ned`, `invalid_vault_ref`
    (secrets/SNMP refs), `no_ned_id` (capability probe before NED learned),
    `no_nso_client` (device's NSO instance not configured),
    `nso_unavailable` (secrets probe), `secrets_write_unsupported`,
    `vault_error`, and route-policy intent validation:
    `invalid_payload`, `invalid_family`, `invalid_name`, `invalid_entries`.
  - Keyed intent pushes (`X-Push-Seq`, see below): `sequence_reuse` (409 — the
    same sequence was already admitted for this stream with a different body or
    a different request mode) and `stale` (409 — the sequence is older than the
    admitted one).
  - Request-body validation failures (Pydantic) return the SAME envelope with
    `code=validation_error` and the field errors under
    `error.detail.errors` — FastAPI's default `{"detail": [...]}` shape is
    never emitted.
  - An unexpected failure anywhere (no specific handler claimed it) returns the
    SAME envelope with `code=internal`, a generic message and an empty `detail`.
    Nothing from the exception is echoed: it can carry the credential, URL or row
    it failed on. The adapter log retains only redacted trace context: the exception
    type, request method and path, and safe frame locations. It excludes exception
    arguments, source text, and frame locals.

### `X-Push-Seq` — keyed intent pushes

An intent PUT delivered by the plugin's outbox carries `X-Push-Seq`: the claim's
identity. Delivery is at-least-once, so the adapter keeps one receipt per
(device, **stream**) and admits by it.

**Which endpoints.** Every in-protocol intent PUT, with no exceptions — the sixteen
listed below. The stream is a property of the ENDPOINT, not of the removal scopes:
the plugin runs one claim sequence per intent family, so two families that share a
projection document still get separate receipts.

| stream | endpoint | promotes |
|---|---|---|
| `bfd` | `PUT /api/v1/devices/{id}/bfd-intent` | `bfd` |
| `bgp` | `PUT /api/v1/devices/{id}/bgp-intent` | `bgp` |
| `interface_config` | `PUT /api/v1/devices/{id}/intent` | `interface_config` |
| `interface_mtu` | `PUT /api/v1/devices/{id}/interface-mtu-intent` | `interface_mtu` |
| `ip` | `PUT /api/v1/devices/{id}/ip-intent` | `interface_config` |
| `isis` | `PUT /api/v1/devices/{id}/isis-interface-intent` | `isis` |
| `isis_flex_algo` | `PUT /api/v1/devices/{id}/isis-flex-algo-intent` | `isis` |
| `l2_sap` | `PUT /api/v1/devices/{id}/l2-sap-intent` | `l2_sap` |
| `logging` | `PUT /api/v1/devices/{id}/logging-intent` | `logging` |
| `ospf` | `PUT /api/v1/devices/{id}/ospf-intent` | `ospf` |
| `route_policy` | `PUT /api/v1/devices/{id}/route-policy-intent` | `route_policy` |
| `snmp` | `PUT /api/v1/devices/{id}/snmp-intent` | `snmp` |
| `static_route` | `PUT /api/v1/devices/{id}/static-route-intent` | `static_route` |
| `subinterface` | `PUT /api/v1/devices/{id}/subinterface-intent` | `subinterface` |
| `svi` | `PUT /api/v1/devices/{id}/svi-intent` | `svi` |
| `vlan` | `PUT /api/v1/devices/{id}/vlan-intent` | `vlan` |

`interface_config` keeps that spelling because the plugin maps its `interface`
family onto it; `ip` and `isis_flex_algo` are the plugin's own names. The
`promotes` column is the projection family the write composes into — two endpoints
may share one, which is why the receipt key and the projection section are
different vocabularies.

**What a push authorizes.** The stream is also the AUTHORIZATION unit, and it owns an
explicit, disjoint subset of its family's intent tables: `interface_config` owns the
interface attributes and `ip` owns the addresses; `isis` owns the IS-IS processes,
interfaces and levels and `isis_flex_algo` owns the flex-algorithms. A push authorizes
its own stream's tables and no others, so a normal push on one lane never carries out
the state an un-promoted `?store_only=true` push left in the sibling lane. The document
the deployment sends is still the COMPLETE device document: each family is composed from
its streams' last-authorized fragments, with the just-promoted stream's fresh snapshot
overlaid.

An intent push is the only thing that authorizes. `POST …/actions/force-removal` authorizes
nothing: it re-issues a deployment of one scope, of state an earlier push already
authorized, with the collateral guard off — so it promotes no stream and marks none applied.

**What identifies a delivery.** The sequence, the body digest AND the request mode:

- the digest is `sha256` over the canonical JSON of the raw request body
  (`json.dumps(body, sort_keys=True, default=str)`);
- the mode is the triple (`?store_only=`, `?delete_origin=`, `?backfill_only=`) as
  parsed: `1`, `true`, `yes`, `on` are true; `0`, `false`, `no`, `off` and an absent
  flag are false; any other spelling is refused with `422 validation_error`. The
  body does not say what the request does with it: store-only authorizes no
  device write, delete-origin turns a shrink into a networked retraction, the unmarked
  form detaches instead, and backfill-only adopts ids and prunes uncorrelated rows while
  writing no content at all. One sequence carrying one body under two of those is two
  different operations.

  The `static_route` stream normalizes `delete_origin` to false before receipt admission.
  The query flag does not apply to this stream because each required `deleted_routes`
  record carries its own deletion authority. The flag is therefore not part of a
  static-route delivery's mode or replay identity.

**Admission.**

- same sequence, same body, same mode → **200 with the stored response**, nothing
  applied again;
- same sequence with a different body OR a different mode → **409 `sequence_reuse`**;
- a lower sequence than the admitted one → **409 `stale`**;
- a higher sequence → admitted; it becomes that stream's receipt.

The receipt is written in the same transaction as the mutation it admits, under the
device's projection lock, so a refused or replayed delivery leaves nothing behind and
two concurrent deliveries of one sequence cannot both proceed. A device offboarded
INSIDE that lock answers **404 `not_found`**, the same as a push for a device that was
already gone — the outbox retrying a push while an operator removes the device is a
race, not a server error.

The header is **required** on every one of the sixteen endpoints above, and the sequence
domain is `1 … 2^63-1`. An absent header is a **422**, exactly as a malformed or
out-of-domain one is, and never a silent downgrade to an unkeyed write: the refusal happens
at request validation, so the mutation does not run and no receipt is written. Without it
the delivery is unresolvable — the write commits with nothing to recognise a redelivery by,
so a lost response turns the retry into a second operation.

The header is a DECLARED, required parameter of each of the sixteen endpoints, with those
bounds, and appears in `/openapi.json`; the two out-of-protocol PUTs declare no such
parameter. The direct-apply families (lacp, switchport) are deliberately claim-less and are
POSTs, not intent PUTs, so they never reach admission. The two remaining PUTs
(`/api/v1/config/failover` and `PUT /api/v1/devices/{id}/scope`) are adapter
configuration rather than intent deliveries and carry no claim either.

### Call directions

Everything the adapter *reads* from the plugin goes through NetBox's own REST API for the
plugin's models. Beyond those reads it calls exactly two plugin endpoints, both
fire-and-forget notifications that carry no state and are never read back.

| From | To | What | Phase |
|------|----|------|-------|
| plugin → adapter | `POST /api/v1/devices/{id}/sync-notify` | scope/intent changed, sync now (push kicker) | 1+ |
| plugin → adapter | `POST/PATCH/DELETE /api/v1/devices/...` | onboard, edit mapping, offboard | 1+ |
| plugin → adapter | `PUT /api/v1/devices/{id}/scope` | push scope on save | 1+ |
| plugin → adapter | `PUT /api/v1/devices/{id}/intent` (+ the per-scope `PUT …/*-intent` family) | **push intent on accept** | 2 |
| plugin → adapter | `POST /api/v1/devices/{id}/actions/{sync,detect-drift,connect,apply}` | trigger jobs | 1+, `apply` is 2 |
| operator → adapter | `POST /api/v1/devices/{id}/actions/{retry,abandon}-generation` | clear a blocked deployment generation | 2 |
| plugin → adapter | `GET /api/v1/devices/{id}/{interfaces,state,intent,intent-summary,scope}` (+ the per-scope read mirrors), `GET /api/v1/jobs/...` | read state | 1+, `intent` is 2 |
| adapter → NetBox | `GET /api/plugins/nso/device-management/` | reconcile mirrored scope (pull) | 1+ |
| adapter → NetBox | `GET /api/plugins/nso/interface-state/` | **reconcile mirrored intent (pull)** | 2 |
| adapter → NetBox | `PATCH dcim.Interface` (and create if missing) | write synced attribute values | 1+ |
| adapter → NSO   | RESTCONF `sync-from`, `compare-config`, `check-sync`, `connect` (`/devices/device`) | Phase 1 NSO surface | 1+ |
| adapter → NSO   | RESTCONF write to a thin reconcile-commit service (Spike S2) | apply intent | 2 |
| adapter → plugin | `POST /api/plugins/nso/sync-complete/` — `{"netbox_device_id": <int>}` | a device sync finished; refresh its overlays | 1+ |
| adapter → plugin | `POST /api/plugins/nso/provision-complete/` — `{"provision_job_id": <int>}` | a provision job reached a terminal state | 1+ |

Both notifications are best effort: the adapter logs a failure and does not retry, and
neither answer is read. **No result may depend on them alone.** A plugin-side consumer
that reacts to a notification must also have a clock that runs plugin → adapter, because
the two directions fail independently — an adapter-to-NetBox token that has gone invalid
answers `401` on every callback while plugin-to-adapter reads stay healthy. The settlement
feed below is consumed on both.

Beyond those two, if you find yourself adding an HTTP call from the adapter to a path on
the plugin (anything under `/api/plugins/nso/` other than the rows above), stop — that
direction is not in the spec.

## Enums

- **`mapping_status`**: `mapped` · `unmatched_device` · `unmatched_interfaces`
- **`job.type`**: `sync` · `detect-drift` · `connect` · `apply` (Phase 2) ·
  `removal` (async PUT-replace that reverts dropped intent — see
  [Removal propagation](#removal-propagation))
- **`job.status`**: `queued` · `running` · `succeeded` · `failed`
- **`compliance_status`** (per attribute) — full lifecycle:
  - `unknown` — known to the plugin but never synced.
  - `imported` — NetBox value matches the last import from NSO; no user
    intent yet (terminal state in Phase 1).
  - `changed` — re-sync reports a device value differing from the last
    import, **and** the attribute is not yet `accepted`. Pre-accept drift.
  - `accepted` — user has blessed the current value as intent (Phase 2).
  - `deploying` — an `apply` job is currently pushing this attribute.
  - `in_sync` — apply succeeded; device matches deployed intent.
  - `apply_failed` — last apply job failed for this attribute; error in
    `last_apply_error`. Intent state stays `accepted` underneath; the
    attribute is eligible for retry.
  - `drifted` — was `in_sync`, then `detect-drift` found device value
    diverging from intent. Post-deploy drift.
  - `error` — could not be evaluated (transient NSO/NetBox read error).

  Phase 1 deployments will only ever see `unknown` / `imported` / `changed`
  / `error`. The Phase 2 statuses appear once the plugin's
  `NSOInterfaceState` model and apply worker land (M5–M6).

---

## Health

### `GET /healthz`
No auth. → `200`
```json
{ "status": "ok", "version": "0.1.0",
  "nso_instances": [ { "name": "nso-prod", "reachable": true } ] }
```

## NSO instances

NSO instances are registered via the adapter's **config file** in Phase 1
(no create endpoint).

### `GET /api/v1/nso-instances` → `200`
```json
[ { "id": "nso-prod", "name": "nso-prod",
    "base_url": "https://nso.example.net", "reachable": true } ]
```

### `GET /api/v1/nso-instances/{id}/devices` → `200 | 404 | 502`

Returns the enriched device inventory from the named NSO instance, with an
onboarded cross-reference against the adapter's Device table.  All nullable
fields are always present (never omitted).

```json
[
  {
    "name":                        "core-rtr-01",
    "address":                     "10.0.0.1",
    "ned_id":                      "cisco-ios-cli-6.95",
    "platform":                    "ios",
    "auth_group":                  "default",
    "admin_state":                 "unlocked",
    "onboarded":                   true,
    "onboarded_device_id":         17,
    "onboarded_netbox_device_id":  42
  },
  {
    "name":                        "edge-rtr-02",
    "address":                     null,
    "ned_id":                      null,
    "platform":                    null,
    "auth_group":                  null,
    "admin_state":                 null,
    "onboarded":                   false,
    "onboarded_device_id":         null,
    "onboarded_netbox_device_id":  null
  }
]
```

`platform` is derived from `ned_id` prefix: `"cisco-ios-cli"` → `"ios"`,
`"cisco-iosxr-cli"` → `"iosxr"`, `"cisco-nx-cli"` → `"nxos"`,
`"juniper-junos-nc"` → `"junos"`, unknown → `null`.

`404` if `{id}` is not a configured NSO instance.
`502` on NSO connectivity error.

### `GET /api/v1/nso-instances/{id}/neds` → `200 | 404 | 502`

Returns the **NED packages installed** on the named NSO instance (the *available*
NEDs an operator can onboard a device with). Read live from
`tailf-ncs:packages/package`; only packages exposing a `ned` component are
returned — service/application packages (the reconcilers, auth, observability)
are excluded.

```json
[
  {
    "ned_id":            "cisco-ios-cli-6.114:cisco-ios-cli-6.114",
    "package":           "cisco-ios-cli-6.114",
    "version":           "6.114",
    "oper_status":       "up",
    "vendor":            "Cisco",
    "operating_systems": ["IOS", "IOS-XE"],
    "product_families":  ["Cisco ASR 1000 Series Aggregation Services Routers"],
    "platform":          "ios"
  }
]
```

`platform` is the short family label (same derivation as `/devices`), `null` when
the NED prefix is unrecognised. `vendor`/`operating_systems`/`product_families`
come from the NED's `device` metadata and drive NetBox-platform → NED matching.

`404` if `{id}` is not a configured NSO instance.
`502` on NSO connectivity error.

## Devices

A *device* is a NetBox device onboarded into the adapter and linked to an NSO
device.

### `GET /api/v1/devices` → `200`
```json
[ { "id": 1, "nso_instance": "nso-prod", "nso_device_name": "core-rtr-01",
    "netbox_device_id": 42, "mapping_status": "mapped",
    "last_sync_at": "2026-05-20T10:00:00Z", "last_sync_status": "succeeded",
    "compliance_summary": { "managed_interfaces": 12, "imported": 11,
                            "changed": 1, "error": 0 } } ]
```

### `GET /api/v1/devices/by-nso` → `200 | 404`

Resolve an adapter Device by its NSO coordinates.  One-hop lookup — no
need to paginate `GET /devices` and filter client-side.

**Request:** query params `instance` (required) and `name` (required).
Missing either → `422`.

**Response:** same shape as `GET /api/v1/devices/{id}` (device object
with `scope` and `last_job_id`).

```json
{
  "id": 17,
  "nso_instance": "nso-dev",
  "nso_device_name": "core-rtr-01",
  "netbox_device_id": 42,
  "mapping_status": "mapped",
  "last_sync_at": "2026-05-24T20:00:00Z",
  "last_sync_status": "succeeded",
  "scope": { "attributes": ["description", "enabled"] },
  "last_job_id": 99
}
```

`404 not_found` if no device matches `(instance, name)`.

### `POST /api/v1/devices` — onboard
Request:
```json
{ "nso_instance": "nso-prod", "nso_device_name": "core-rtr-01",
  "netbox_device_id": 42 }
```
→ `201` device object (as above). `409 conflict` if the NetBox device or NSO
device is already onboarded.

This onboard only creates the adapter **mapping row**; it assumes the device node
already exists in NSO. To create the device *in NSO* and bring it up, use
`/provision` below.

### `POST /api/v1/devices/provision` — create the device in NSO + bring it up
Request:
```json
{ "nso_instance": "nso-prod", "device_name": "core-rtr-01",
  "address": "10.0.0.1", "ned_id": "cisco-ios-cli-6.114:cisco-ios-cli-6.114",
  "authgroup": "network", "netbox_device_id": 42,
  "ned_type": "cli", "port": null, "admin_state": "unlocked", "sync": true }
```
Runs the NSO onboarding sequence — **create** the device node (idempotent) →
**ssh fetch-host-keys** (TOFU; needs the device reachable) → set **admin-state**
(unlocked) → **sync-from** (non-fatal) → create the adapter **mapping** row (when
`netbox_device_id` is given). Always `200` (even when a blocking step fails — the
device is left in NSO for retry); inspect the body:
```json
{ "ok": true, "device_id": 17,
  "steps": [ {"step": "create", "status": "ok"},
             {"step": "fetch_host_keys", "status": "ok"},
             {"step": "admin_state", "status": "ok", "detail": "unlocked"},
             {"step": "sync_from", "status": "ok"},
             {"step": "adapter_mapping", "status": "ok"} ] }
```
Step `status` ∈ `ok | exists | failed`; a `failed` blocking step (create /
fetch_host_keys / admin_state) sets `ok=false` and stops. `422` if `nso_instance`
is unknown.

At most one provision is active per `(nso_instance, device_name)` — enforced by a unique
index over queued and running rows, not by a lookup — so a double-submit returns the
in-flight job. From the mapping step onwards the job holds the device's execution claim, so
a sync, a failover tick or an offboard on that device waits or is refused while it runs. If
the device is already held when the mapping is reached, the job ends `failed` with
`error.code = "device_busy"` and `error.detail.reason = "claim_unavailable"`; nothing was
written, and re-submitting is the retry.

### `GET /api/v1/devices/{id}` → `200`
Device object plus `scope` (see below) and `last_job_id`.

### `GET /api/v1/devices/{id}/generations` → `200 | 401 | 404 | 422`

Return the device's deployment generations in ascending `seq` order. The optional
`since_seq` query parameter selects only generations whose `seq` is strictly greater
than the supplied value. `limit` defaults to 100 and must be between 1 and 500. An
out-of-range limit returns `422 validation_error`; it is never clamped.

```json
[
  {
    "generation_id": 81,
    "seq": 4,
    "status": "pending",
    "job_id": null,
    "mode": "networked",
    "settlement_cohort": null,
    "digest": "7e4a...",
    "stream_revisions": {"vlan": 12},
    "source_push_seq": {"vlan": 501},
    "created_at": "2026-08-12T09:15:00Z",
    "updated_at": "2026-08-12T09:30:00Z"
  }
]
```

Every object always contains every shown field. An unset field is `null`, never
missing. `job_id` is `null` while a pending generation is not attached to a job.
`settlement_cohort` is `null` when the generation is not part of a request-atomic
settlement cohort. `status` is one of `pending`, `running`, `settled`, `failed`,
`outcome_unknown`, or `abandoned`. `mode` is `networked` or `detach`.

`404 not_found` if the device does not exist.

### `POST /api/v1/devices/{id}/deployment-evidence`

Return one unpaged snapshot of the device deployment barrier and the requested durable
Apply attempts. The request accepts at most 100 attempt UUIDs:

```json
{
  "apply_attempt_ids": ["8a2c9231-7ad8-4b17-a4b8-f5b4df745dd8"]
}
```

The response contains:

```json
{
  "device_id": 17,
  "head": {
    "generation_id": 301,
    "seq": 44,
    "status": "failed",
    "mode": "networked",
    "settlement_cohort": null,
    "sections": ["vlan"],
    "source_push_seq": {"vlan": 501},
    "apply_attempt_id": "8a2c9231-7ad8-4b17-a4b8-f5b4df745dd8",
    "carrier_job_id": 900,
    "carrier_job_status": "failed",
    "carrier_job_result": null,
    "carrier_job_error": {"code": "nso_commit_failed", "message": "commit failed", "detail": {}},
    "created_at": "2026-08-25T10:00:00Z",
    "updated_at": "2026-08-25T10:01:00Z"
  },
  "blocked": true,
  "write_work_pending": false,
  "held_jobs": [901],
  "pending_generations": 1,
  "attempts": [
    {
      "apply_attempt_id": "8a2c9231-7ad8-4b17-a4b8-f5b4df745dd8",
      "admission_state": "admitted",
      "http_status": 202,
      "response": {
        "outcome": "promoted",
        "skipped": {},
        "skipped_detail": null,
        "generations": [{"generation_id": 301}]
      },
      "generations": [
        {
          "generation_id": 301,
          "seq": 44,
          "status": "failed",
          "sections": ["vlan"],
          "source_push_seq": {"vlan": 501},
          "carrier_job_id": 900,
          "carrier_job_status": "failed",
          "carrier_job_result": null,
          "carrier_job_error": {"code": "nso_commit_failed", "message": "commit failed", "detail": {}},
          "updated_at": "2026-08-25T10:01:00Z"
        }
      ]
    }
  ],
  "unknown_apply_attempt_ids": []
}
```

`head` is the device-wide executable head, not a cohort-scoped generation. `blocked` is
true when that head is `failed` or `outcome_unknown`. `write_work_pending` counts only a
queued or running device-writing job that the generation barrier admits. A queued
device-writing job that the barrier refuses appears in `held_jobs` and does not make
`write_work_pending` true. `pending_generations` counts non-crossable generations after
the head. No part of this response is paginated.

`sections` contains unique document section names. `source_push_seq` is keyed by intent stream.
This preserves both stream values when `interface_config` and `ip` map to the
`interface_config` section, or when `isis` and `isis_flex_algo` map to the `isis` section.

The stored attempt `response` is the complete replay body. Terminal generation evidence
uses the carrier snapshot, so it remains available after the job row is pruned. An attempt
referenced by a generation is never deleted. A deterministic rejection that stamps no
generation is valid when its replay body omits `generations` or stores it as `null`.

An ID in `unknown_apply_attempt_ids` is non-actionable. The caller must re-submit the
identical Apply request. It must not settle or roll back local intent from an unknown ID
alone. A generation-bearing replay body is corrupt when it omits the generation list, uses
a non-list value, contains invalid or duplicate IDs, or does not name exactly the generations
stamped to its attempt. Corrupt evidence is also non-actionable. The endpoint returns the
`500 internal` error envelope for this invariant failure. More than 100 IDs returns
`422 validation_error`. A missing device returns `404 not_found`.

### `PATCH /api/v1/devices/{id}` — correct the mapping
Request (any subset):
```json
{ "nso_instance": "nso-prod", "nso_device_name": "core-rtr-01" }
```
Changing `nso_device_name` or `nso_instance` re-keys the device: stored
interface mappings and `interface_attr_state` are cleared and rebuilt on the
next sync; `job` history is retained. → `200` device object.

### `DELETE /api/v1/devices/{id}` — offboard
→ `204`. Removes adapter state for the device. Does **not** modify NetBox.

## Scope

Scope source of truth is the NetBox plugin model; the adapter mirrors it.

### `PUT /api/v1/devices/{id}/scope` — set the mirrored scope (+ device settings)
Request:
```json
{ "attributes": ["description", "enabled"],
  "auto_apply":  false }          // Phase 2; optional, default false
```
- Empty `attributes` list = device managed but no attributes (effectively
  paused).
- `auto_apply` (**Phase 2, M6**): when `true`, every `PUT /devices/{id}/intent`
  that actually changes intent enqueues an `apply` job inline. The flag is
  owned by the plugin's `NSODeviceManagement` model (sits next to scope on
  the same row) and propagates via this PUT + the existing scope
  reconciler. Phase 1 callers may omit the field; adapter treats missing as
  `false`.

→ `200`
```json
{ "device_id": 1, "attributes": ["description", "enabled"],
  "auto_apply": false,
  "updated_at": "2026-05-20T10:05:00Z" }
```

### `GET /api/v1/devices/{id}/scope` → `200`
Same shape as the `PUT` response.

## Intent (Phase 2)

Intent — the accepted "what we want the device to look like" — is owned by
the NetBox plugin (`NSOInterfaceState` model) and **mirrored** in the
adapter (decision L). The plugin pushes on accept; the adapter reconciles
periodically. Apply workers read **only** the adapter mirror.

### `PUT /api/v1/devices/{id}/intent` — push the device's full intent snapshot
The plugin sends every per-attribute intent the plugin currently knows for
this device (not a delta — keeps the contract idempotent and a missing entry
unambiguously means "no intent"). Adapter replaces its mirror for this
device atomically.

Request:
```json
{ "attributes": [
    { "interface": "GigabitEthernet0/0/0/1", "attribute": "description",
      "intent_value": "uplink to spine-1", "accepted_at": "2026-05-23T10:05:00Z" },
    { "interface": "GigabitEthernet0/0/0/1", "attribute": "enabled",
      "intent_value": true,                  "accepted_at": "2026-05-23T10:05:00Z" }
] }
```
- Empty `attributes` list = "no intent on this device" (clears the mirror;
  in-flight `apply` jobs are unaffected — they use their own snapshot).
- `interface` is the NSO/NED-normalized name (same as
  `/devices/{id}/interfaces` returns).
- `attribute` ∈ `description` · `enabled` (Phase 2 slice).
- `intent_value` types: `description` → string; `enabled` → bool.

→ `200`
```json
{ "device_id": 1, "attribute_count": 2,
  "updated_at": "2026-05-23T10:05:01Z" }
```

`409 conflict` is **not** returned for "an apply is running" — intent writes
are non-blocking; the running apply uses its own pre-job snapshot.

### `GET /api/v1/devices/{id}/intent` → `200`
Same shape as the `PUT` request body, plus `updated_at`; rows additionally
carry `last_apply_at` / `last_apply_error`.

### `GET /api/v1/devices/{id}/intent-summary` → `200 | 404`

Per-scope summary of the adapter's **entire intent mirror** for a device —
one entry per non-empty `*_intent` table (interface attributes *and* every
per-scope intent family: VLAN, SVI, subinterface, L2 SAP, BFD, MTU, IS-IS,
OSPF, BGP, route-policy, static routes, SNMP, logging). Discovered from
table metadata, so new `*_intent` tables appear automatically. Tables keyed
by `interface_id` are resolved to the device via the `interfaces` table;
child tables with neither key (`bgp_scope/af/peer*_intent`) are covered by
their parent and skipped.

```json
{ "device_id": 1,
  "scopes": {
    "static_route_intent":  { "count": 4, "applied": 4, "failed": 0 },
    "vlan_intent":          { "count": 12, "applied": 0, "failed": 0 }
  },
  "pending_clear": {
    "ospf": { "provenance": "authorized", "since": "2026-08-25T10:30:00Z" }
  } }
```

- `count` — intent rows the adapter holds for this device.
- `applied`: rows with a non-null `last_apply_at`. This counts timestamps. It is not proof
  that the current intent is on the device.
- `failed` — rows with a non-null `last_apply_error`.
- `pending_clear`: streams with a recorded clear that has no admitted networked carrier.
  The map itself is always present (`{}` when no row exists); only a stream's entry inside
  it is absent. Each value reports only its provenance and the
  time the obligation was first recorded. It never reports leaf names or paths. A listed
  stream can still hold a device leaf that the intent store says is unset.

Cheap by design (one count query per table): the plugin calls it on every
device-tab render to detect intent split-brain (next section).

### Intent split-brain & re-sync

The adapter's `*_intent` tables mirror what the plugin last pushed; the
plugin's `NSO*State` overlays are the source of truth for what NetBox
*owns*. They can diverge — e.g. a NetBox-side migration reset overlays to
`imported` while the adapter kept its accepted rows. Such **stale adapter
intent** is invisible in the UI yet would be pushed to the device by a
device-wide apply.

Detection (plugin-side, `netbox_nso_plugin/intent_drift.py`, per scope on
every device-tab render, fed by `GET intent-summary`):

- **orphaned** — adapter holds intent, NetBox owns *nothing* in the scope;
- **partial** — for scopes with count *parity* (one owned overlay row ↔ one
  adapter intent row; all scopes except BGP): the adapter holds *more* rows
  than NetBox owns, so the surplus is stale even though the scope looks
  healthy.

Recovery: the plugin re-pushes the scope's *current* owned snapshot through
the normal `PUT *-intent` endpoint; full-replace semantics drop the stale
rows (empty push → all removed). Re-sync **never writes to the device** —
it only aligns the adapter mirror with NetBox ownership.

Known limits (accepted): equal-count/different-rows drift is invisible;
multi-table scopes (IS-IS, OSPF, SNMP) compare summed counts, so a surplus
in one table can be masked by a deficit in another; BGP is detected at
scope level only (one `bgp_router_intent` row covers N owned peers —
counts aren't 1:1, but any BGP re-push rewrites the whole router tree and
heals stale children). Design + per-scope parity audit:
`docs/intent-split-brain-design.md`.

## Interfaces & sync state

### `GET /api/v1/devices/{id}/interfaces` → `200`
```json
[ { "name": "GigabitEthernet0/0/0/1", "netbox_interface_id": 555,
    "parent_binding": null, "kind": null, "encap_tag": null,   // M27R logical-iface modeling;
    "vrf": null, "service": null,                              //   null for physical / Cisco / Junos
    "attrs": {
      "description": {
        "nso_value":    "uplink to spine-1",
        "netbox_value": "uplink to spine-1",
        "intent_value": "uplink to spine-1",      // Phase 2; null pre-accept
        "status":       "in_sync",                 // see compliance_status enum
        "last_apply_at":    "2026-05-23T10:06:00Z",  // null until first apply
        "last_apply_error": null                     // populated when status=apply_failed
      },
      "enabled": {
        "nso_value": true, "netbox_value": true,
        "intent_value": true, "status": "in_sync",
        "last_apply_at": "2026-05-23T10:06:00Z",
        "last_apply_error": null
      } } } ]
```

Phase 1 deployments will have `intent_value: null`, `last_apply_at: null`,
`last_apply_error: null` on every attribute and `status` ∈ `{unknown,
imported, changed, error}`. The Phase 2 fields are reserved in the contract
so the plugin doesn't need a Phase 1 / Phase 2 shape switch.

### `GET /api/v1/devices/{id}/state` → `200 | 404`

Per-attribute sync-state rollup for the device's interface scope (the tab's
status strip). Counts `interface_attr_state` rows by status.

```json
{ "device_id": 1, "managed_interfaces": 12,
  "by_status": { "unknown": 1, "imported": 22, "changed": 1, "accepted": 0,
                 "deploying": 0, "in_sync": 0, "apply_failed": 0,
                 "drifted": 0, "error": 0 },
  "last_checked_at": "2026-05-20T10:06:00Z" }
```
Phase 1: only `unknown`/`imported`/`changed`/`error` are ever non-zero.
Phase 2: the rest activate as M5–M6 land.

## Actions

Unless an endpoint says otherwise, a job-producing action returns `202` with
`{ "job_id": <int> }`. The generation actions allow the nullable value documented
below. `apply-diff` is synchronous and creates no job.

### Execution and admission

Execution and admission are separate rules. At most one device-bound job executes
for a device at one time. The worker's device claim enforces that rule.

Admission is endpoint-specific. It is not a one-job-per-device table constraint.
Several queued jobs can coexist, and the generation success barrier can prevent a
queued job from starting.

| Endpoint | Admission rule |
|---|---|
| `actions/sync`, `actions/sync-from-nso`, `actions/detect-drift`, `actions/connect`, and `sync-notify` | Return `409 conflict` only when a queued job of the same requested job type already exists. `error.detail.job_id` identifies that queued job. A running job of that type permits a queued successor. Jobs of other types do not cause this conflict. |
| `actions/force-removal` | Every valid request creates a new removal generation and a new dedicated removal job. The endpoint does not inspect active jobs and does not deduplicate repeated requests, including requests for the same scope. |
| `actions/apply` | Evaluate the selection first. If no selected stream is promotable, return the documented `200` no-op without checking active jobs. If at least one stream is promotable, inspect queued and running jobs once. Return `409 conflict` with the first observed job by `(created_at, id)` in `error.detail.job_id` when any exists. Otherwise, promote and enqueue the generation chain. This is a point-in-time check in the device-locked transaction. A later action can enqueue after Apply commits. |
| `actions/retry-generation` | Return `409 conflict` only when there is no blocked executable head or another barrier action already acted on it. Otherwise, create a fresh dedicated job for the blocked head and return `202`. Existing jobs and successor bindings remain unchanged. Unrelated queued or running jobs do not cause a conflict. |
| `actions/abandon-generation` | Return `409 conflict` only when there is no blocked executable head or another barrier action already acted on it. Otherwise, abandon the head, ensure the next executable generation has a live carrier, and return `202`. Unrelated queued or running jobs do not cause a conflict. |
| `actions/apply-diff` | Create no job and apply no queue-admission rule. |

### `POST /api/v1/devices/{id}/actions/sync`

Run NSO `sync-from`, read managed attributes, write them to NetBox, and recompute
compliance.

### `POST /api/v1/devices/{id}/actions/sync-from-nso`

Read the complete managed state from NSO CDB without first running a device
`sync-from`.

### `POST /api/v1/devices/{id}/actions/detect-drift`

Re-read NSO and recompute per-attribute sync state without writing to NetBox.

### `POST /api/v1/devices/{id}/actions/connect`

Run the NSO connectivity test.

### `POST /api/v1/devices/{id}/actions/force-removal`

Reissue one removal scope with the collateral guard disabled. The request body is
`{ "scope": "<scope>", "interfaces": ["<name>", ...] | null }`.
`interface_config` requires a non-empty `interfaces` list.

Each valid request creates a distinct reissue generation and a distinct removal
job. A repeated request for the same scope does not reuse or replace an earlier
job.

### `POST /api/v1/devices/{id}/actions/{retry,abandon}-generation`

These actions are the two explicit exits from the deployment-generation success
barrier. A failed or outcome-unknown head blocks every successor.

- `retry-generation` creates a fresh dedicated job that executes the head's exact
  stored document under its stored mode, verifies the stored digest before
  execution, and reconstructs removal execution from the stored removal context.
  It never takes over an existing job.
- `abandon-generation` records the blocked head as not delivered and advances the
  chain. The operator asserts that the state the generation was to establish is
  already present or is no longer required.

Both return:

`202 { "job_id": <int|null> }`

For retry, `job_id` identifies the fresh job that will execute the blocked head.
For abandon, `job_id` identifies the carrier of the successor made executable.
It is `null` when abandoning the head makes no successor executable. This includes
when a successor exists but is itself failed (for example the adjacent generation
of the same failed carrier). Both actions return `409 conflict` when
the device has no blocked generation; `error.detail.head_status` reports the
current head status. A concurrent retry or abandon that loses the
compare-and-set race returns `409 conflict`, with an empty `error.detail` object.
Queued or running jobs of any type do not otherwise refuse these barrier actions.

### `POST /api/v1/devices/{id}/actions/apply` (Phase 2)

Atomically promote the exact intent pushes selected by the caller and enqueue their
immutable deployment-generation chain. The selector maps the adapter's receipt stream name
to the `X-Push-Seq` that the plugin drained:

```json
{ "selected": { "vlan": 4711 } }
```

The selector uses push sequences, not current revisions, because the plugin already owns
these values in its drain bookkeeping and the adapter receipts use the same identity. A
selected sequence is a strict integer in the receipt domain `1..2^63-1`. A value outside
that domain returns `422 validation_error`.

A stream is promotable only when its latest durable receipt and projection row both match the
selected sequence. A later push never rides an earlier selection. Every stale selection is
reported under `skipped`: `superseded` for an older sequence, `already_applied` when its
revision settled, `already_authorized` when its generation is still unsettled or was
abandoned, `no_receipt` when the adapter has no matching receipt, `backfill_only` when the
matching receipt was admitted in backfill-only mode, and `revision_mismatch` when the
receipt matches but the projection row does not. `backfill_only` is terminal for that
sequence: the receipt exists and holds it, but a backfill repairs correlation only, so no
retry of the same selection can promote it.
`skipped_detail` is keyed by stream; only its CONTENT is conditional — the key itself is
always present. It identifies the generation for
each `already_authorized` skip whose owning generation can be identified. Each member has
the shape `{ "generation_id": <int>, "seq": <int>, "status": "<status>" }`. The value is
`null` when no member qualifies. The retry and abandon actions own the
identified generation. Apply cannot replace it with weaker work.
Apply performs its active-job check only after it finds at least one promotable
selection. The check observes both queued and running jobs. It does not apply to
an empty or fully skipped selection. The device-row lock orders concurrent job
inserts around the check: work admitted before the check causes `409`; a request
ordered after Apply can enqueue after the Apply transaction commits.
An empty selection, or a request in which every selection is skipped, returns `200` with an
explicit no-op and no `job_id`:

```json
{ "device_id": 1, "outcome": "no_op",
  "selected": { "vlan": 4711 },
  "skipped": { "vlan": "already_authorized" },
  "skipped_detail": {
    "vlan": { "generation_id": 80, "seq": 3, "status": "abandoned" }
  },
  "generations": [] }
```

A promotion returns the complete ordered chain. Each link has its queued job. The success
barrier decides when each successor job may start. The response returns `202`, and its
top-level `job_id` is the first job in that chain.

```json
{
  "device_id": 1,
  "outcome": "promoted",
  "job_id": 501,
  "selected": { "vlan": 4711 },
  "skipped": {},
  "skipped_detail": null,
  "generations": [
    { "generation_id": 81, "seq": 4, "job_id": 501, "mode": "networked",
      "source_push_seq": { "vlan": 4711 },
      "stream_revisions": { "vlan": 7 }, "digest": "<sha256>" }
  ]
}
```

Promotion, document storage, cohort allocation, generation creation, and job enqueue occur
in one transaction under the device projection lock. Removal work uses the ordinary
`enqueue_removal` path and its existing scope runner. Networked removal generations precede
the apply generation; detach removal generations follow it, so the top-level `job_id` names
the first networked removal when one exists. The networked intermediate document
retains every detach-only row. The detach generation stores the selected desired document.
All links in the request share one non-null `settlement_cohort`; a singleton leaves it null.
Settlement is request-atomic within that cohort. One failed member withholds every member's
`applied_revision`. A successful retry of the failed member releases and stamps the full
cohort. A failed head also blocks every successor through the ordinary generation success
barrier.

Apply refuses the whole request with `409 apply_unexecutable` when any selected stream cannot
be routed faithfully through those runners. The refusal names each stream and reason. It does
not promote or enqueue a subset. Reasons are stable machine codes:

<!-- apply-unexecutable-reasons:start -->
- `interface_attribute_eligibility_unresolved`
- `mixed_detach_replacement`
- `no_executable_interface`
- `outstanding_deletion_provenance`
- `unresolved_interface_identity`
<!-- apply-unexecutable-reasons:end -->

The manual-Apply boundary is exactly `DOCUMENT_EXECUTED_SECTIONS`. It contains every section,
so all sixteen streams are executable through `ACTION_APPLY_EXECUTABLE_SECTIONS`. SNMP
documents store Vault references verbatim. The SNMP writer reads those references from the
hydrated rows when it builds the send body. BGP documents store the router, scope,
address-family, peer, and peer address-family tables. The hydrator rebuilds their relationship
graph from durable parent identities before the writer walks it. Static-route documents also
record the apply and removal classifications. A store-only push between Apply and worker start
cannot change the selected body, removal authority, proof carriers, or deployed-key decisions.

Deletion provenance from a store-only revision remains an execution obligation. If a later
ordinary push would promote that stream without executing the carried deletion, the entire
push refuses with `409 apply_unexecutable` and reason `outstanding_deletion_provenance`.
The transaction rollback preserves the earlier receipt and its provenance. The error message
directs the caller to Apply that receipt when its stream is document-executed, then retry the
later push.

**Reconcile commit (brownfield guardrail).** Every reconciler-service write (apply,
update, and removal PUT-replace) is committed with the NSO RESTCONF
`?reconcile=keep-non-service-config` query parameter. When a service's footprint
overlaps config the device already carries as *non-service* config (pulled into CDB
by `sync-from`), `keep-non-service-config` tells NSO to **keep** (adopt without
deleting) that config. Live testing (a Nokia route-target community on ra1) showed
this is **equivalent to NSO's implicit default here** — a plain commit already adopts
brownfield config rather than conflicting — so the param does **not** change current
behaviour; it makes the safe choice explicit and immune to a deployment whose NSO
global-settings (or a future default) is `discard`. What it locks out is
`discard-non-service-config`, which actively **deletes** unmodeled config under the
footprint: a partial/empty intent would, under discard, wipe the device's real
config (verified live — an empty community intent emitted a `member delete` under
discard, nothing under keep). NSO validates the value (unknown → `400 invalid-value`),
so it is sent verbatim. The pre-apply dry-run preview (`apply-diff`) and the
post-apply native verify carry the same param so the preview matches the commit.
Controlled by `NSO_ADAPTER_RECONCILE_COMMIT` (default `keep-non-service-config`;
`discard-non-service-config` to prune; `""`/`off` for a plain commit — same observed
result as keep on this NSO).

### `GET /api/v1/devices/{id}/actions/apply-diff` → `200 | 404`

Preview the per-scope **native device diff** the next Apply would push (NSO
`?dry-run=native&reconcile=keep-non-service-config`; nothing is committed — the
reconcile param makes the preview match the real reconcile commit). Synchronous —
no job. `diffs` maps
scope → native delta; scopes already in sync yield an empty delta and are
omitted. LAG/switchport have no preview (pushed out-of-band by the plugin,
not from the intent store).

```json
{ "device_id": 1,
  "diffs": { "interface": "interface GigabitEthernet0/1\n description uplink\n!" } }
```

### `POST /api/v1/devices/{id}/sync-notify`
**Served by the adapter; called by the plugin** (Django `post_save` on
`NSODeviceManagement`, and any future scope/intent-changing model) when scope
or intent changes for this device. Triggers an immediate sync job, so the
user doesn't have to wait for the next scheduled poll.

It uses the ordinary trigger rule for the `sync` job type. A queued `sync` job
returns `409 conflict` with that job's id. A running `sync` job permits a queued
successor. Jobs of other types do not refuse the notification.

This is the **only** plugin → adapter push beyond the standard `/api/v1/*`
client calls. For the two notifications that run the other way, see *Call
directions* under §Conventions.
```json
{ "job_id": 9 }
```

## Jobs

### `GET /api/v1/jobs/{id}` → `200`
```json
{ "id": 7, "type": "sync", "device_id": 1, "status": "succeeded",
  "result": { "interfaces_written": 12, "interfaces_created": 2,
              "changes_detected": 1 },
  "error": null,
  "context": null,
  "created_at": "2026-05-20T10:05:30Z",
  "updated_at": "2026-05-20T10:06:00Z",
  "started_at": "2026-05-20T10:05:31Z",
  "heartbeat_at": "2026-05-20T10:05:55Z",
  "settle_seq": 4 }
```

Every key is always present; nullables are emitted as `null`.
`failed` jobs carry `error` in the standard error shape; `result` is `null`.

`settle_seq` is this job's position in its **device's** settlement order, described under
[The ordered settlement feed](#the-ordered-settlement-feed). It is `null` while the job is
`queued` or `running`, and `null` forever for a job that reached a terminal state with no
device: a provision that failed before it acquired one, and the queued jobs a device
offboard terminalizes on its way out. Offboarding also detaches the device's **already
sequenced** history — those rows keep the sequence they were given and simply leave the
device's feed with it, because the device they belonged to is gone.

`result` is free-form per job type. An **apply** job additionally carries
`reader_compare` (the per-scope post-apply presence check) and, for devices with static
routes, `static_route_results` — the per-route record described under
[Per-route apply results](#per-route-apply-results).

### `GET /api/v1/jobs?device_id={id}&status={status}` → `200`

Array of job objects, newest first, 100 per page by default and 500 at most (see `limit`
under [The ordered settlement feed](#the-ordered-settlement-feed)). Both query params optional.

Ordering is `created_at` descending, tie-broken by `id` descending. The tiebreak matters:
`created_at` defaults to the transaction's start time, so several jobs can share one
timestamp and the page would otherwise be non-deterministic — the same request could serve
a different 100 rows each time. It makes the page stable; it is **not** a commit order and
**not** a cursor, so it cannot be walked as a feed.

### The ordered settlement feed

The same collection serves a second, opt-in shape: a per-device feed of terminal jobs in
**commit** order, which a consumer walks under a durable cursor to settle the intent it
pushed. Three query parameters select it, all optional and all defaulting to today's
behavior:

| parameter | default | meaning |
|---|---|---|
| `order` | `desc` | `asc` orders by `settle_seq` ascending — the feed. `desc` is the page above, unchanged. |
| `after_settle_seq` | none | the cursor. Serves rows with `settle_seq > <value>` only. |
| `limit` | `100` | page size, `1..500`. |

Validation is fail-fast, and nothing is coerced:

- `order=asc` **or** `after_settle_seq` **requires** `device_id` → otherwise `422`
  `validation_error`. Sequences are allocated per device, so an unscoped ascending page
  would interleave two devices' independent sequences into one order that is wrong for both.
- `after_settle_seq` **requires** `order=asc` → otherwise `422`. The cursor names a position
  in the settlement order; the descending page is in creation order, and serving the mix
  would let a consumer skip or repeat settlements.
- `status` **cannot combine with** `order=asc` → `422`. A filtered-out terminal row still
  owns its `settle_seq`, so a thinned ascending page advances the cursor past it and that
  settlement becomes permanently invisible to the cursor. Status filtering stays on the
  descending list.
- `limit` outside `1..500` → `422`. It is **not** clamped: a caller that asked for 5000 and
  silently received 500 believes it holds the whole page and advances its cursor as if it did.
- An `asc` request with no `after_settle_seq` starts at `0`, i.e. the beginning of the
  sequence.

**What `settle_seq` guarantees.** It is allocated per device from a counter row locked
until the terminal transaction **commits**, so for one device *sequence order equals commit
order*. Two devices' terminal writes interleave freely; that is allowed, because the cursor
is per device. Monotonicity is the contract; **contiguity is not** — walk rows, never
values, and never treat a missing number as a lost result. `created_at` cannot substitute
for any of this: it is transaction time, not commit time.

**A terminal job is written exactly once.** A terminal write made by a running execution
names that execution (an internal `run_attempt` token, not served) and is applied as a
compare-and-set on it, so a runner that was abandoned — and whose job has since been
requeued or restarted — cannot write over the run that succeeded it. (Writes that name no
execution because there is none to name, such as the bulk terminalization of a device's
queued jobs during offboard, compare on the `queued` status instead.) A rejected write
changes no column and allocates no sequence, so a job appears in the feed exactly once,
under one sequence. Queued and running jobs carry no sequence and are therefore invisible
to the ascending page — the `settle_seq > :cursor` predicate is the visibility rule, and it
needs no status filter.

**The cursor belongs to one store and one device.** A consumer must key its saved cursor on
the pair *(store incarnation, adapter device id)* and compare **both** on every read,
against the `X-Store-Incarnation` header of the page it is about to consume and against the
adapter device id it is polling:

- the store half must come from the **header**, not from any locally cached copy. A store
  rebuilt from an empty schema restarts every counter at 1, and if it recreates the device
  under the same numeric id, a cursor at 100 silently hides settlements 1..100.
- the device half matters because a device may be remapped to a different adapter device id
  within one incarnation. A fresh adapter device also starts at 1.

On either mismatch the consumer resets its cursor, records the new pair, and re-requests
the page from the start rather than applying the old cursor to it.

**One unresolvable result must not block the device.** The feed is strictly ordered, so a
consumer that cannot decide the row at the head would stop forever. Bound it: count
attempts against the specific `settle_seq` that is stuck, and after a fixed number of
passes advance past it with a loud log. The plugin's bound is five attempts, and it is
persisted per device so a process restart does not reset it.

---

## Intent push receipts

### `GET /api/v1/intent-receipts` → `200 | 401 | 422`

Every per-key push receipt, plus the two fleet-wide maxima a restored pusher needs before
it resolves a single outstanding claim. Filterable by `device_id` and by `section`; an
unrecognised `section` is a **422** (`reason = "unknown_section"`, with the valid set in
`detail.sections`) rather than an empty page, because an empty page reads as "this key has
no receipt" and sends the restore down the wrong branch.

```json
{
  "receipts": [
    {
      "device_id": 1,
      "section": "static_route",
      "push_seq": 4,
      "request_digest": "3b1f…",
      "store_only": false,
      "delete_origin": false,
      "backfill_only": false,
      "status_code": 200,
      "response": { "device_id": 1, "count": 1, "removed": 0, "replaced": false,
                    "routes": [{ "route_id": 41, "generation": 12, "fingerprint": "9f2c…" }],
                    "deleted_executed_ids": [], "deleted_degraded_ids": [],
                    "deleted_moot_ids": [], "removed_uncorrelated": [] },
      "generation_id": 12,
      "created_at": "2026-08-11T10:00:00Z",
      "updated_at": "2026-08-11T10:00:00Z"
    }
  ],
  "global_max_push_seq": 11,
  "global_max_route_id": 4242
}
```

`section` is the adapter's own stream vocabulary — the sixteen names in the `X-Push-Seq`
table above. The plugin's delivery key for the interface family is `interface` and maps onto
`interface_config` here; the other fifteen are identical on both sides.

`response` is the stored response that push returned. A restored pusher re-validates its
claim's exact deletion set against it rather than re-sending.

**Both maxima are fleet-wide, filter or no filter.** `global_max_push_seq` is the highest
admitted sequence anywhere, so a restored pusher allocates above it and never re-uses one.
`global_max_route_id` is the highest NetBox route pk the adapter holds anywhere, counting the
tombstones as well as the live intent rows: a plugin-only restore rewinds `StaticRoute`'s pk
sequence, so a snapshot taken before pk R existed can re-allocate R while the adapter still
holds an unrelated row carrying `route_id = R` — and the deletion partition's first pass
would bind that row as genuine and authorize removing it. Advancing the pk sequence past this
value is what closes that. `null` on both means the adapter holds nothing, which is not `0`.

## SNMP Configuration (M11)

### `GET /api/v1/devices/{id}/snmp-config` → `200 | 404`

Read-mirror of SNMP config as exported by the `network-state-export` NSO package.
Community strings are **never** present — only a SHA-256 hash prefix.

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T12:00:00Z",
  "refresh_source": "sse",
  "communities": [
    { "community_hash": "a1b2c3d4e5f6a1b2", "access": "RO", "acl": null }
  ],
  "v3_users": [
    { "username": "nms-user", "has_auth_secret": true, "has_priv_secret": true }
  ],
  "hosts": [
    { "address": "10.0.0.1", "version": "2c", "notify_type": "trap", "port": null }
  ],
  "system_info": { "location": "dc-row-1", "contact": "noc@example.com" }
}
```

Empty response (no SNMP data yet):
```json
{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never",
  "communities": [], "v3_users": [], "hosts": [], "system_info": null }
```

### `PUT /api/v1/devices/{id}/snmp-intent` → `200 | 404`

Push (full-replace) the SNMP intent mirror for this device.
**Community strings are NEVER sent** — only a `vault_ref` (`"mount/path#key"`) that
the `snmp-reconciler` NSO service resolves at commit time.  The `label` field
is the stable key for the service list entry (use `community_hash` from the
read-mirror if no human-readable label exists).

```json
{
  "communities": [
    { "label": "a1b2c3d4e5f6a1b2", "vault_ref": "network/credentials/sw01#snmp_ro", "access": "RO", "acl": null }
  ],
  "v3_users": [
    { "username": "nms-user", "auth_vault_ref": "network/credentials/sw01#snmp_auth", "priv_vault_ref": null }
  ],
  "hosts": [
    { "address": "10.0.0.1", "version": "2c", "notify_type": "trap", "community_or_user": "a1b2c3d4e5f6a1b2" }
  ],
  "system_info": { "location": "dc-row-1", "contact": "noc@example.com" }
}
```

Response:
```json
{
  "device_id": 1,
  "community_count": 1,
  "v3_user_count": 1,
  "host_count": 1,
  "has_system_info": true,
  "updated_at": "2026-06-01T12:00:00Z"
}
```

If `auto_apply` is `true` on the device settings, an apply job is enqueued automatically.

---

## Static Routing (M10)

### `GET /api/v1/devices/{id}/static-routes` → `200 | 404`

Returns the adapter's cached static route mirror for this device (populated by the
`static-route-reconciler` NSO package via SSE / scheduled poll).

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T12:00:00Z",
  "refresh_source": "poll",
  "routes": [
    {
      "vrf": "",
      "prefix": "0.0.0.0/0",
      "next_hop": "10.0.0.1",
      "metric": 1,
      "permanent": false,
      "tag": null,
      "name": null
    }
  ]
}
```

Empty response (no static route data yet):
```json
{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never", "routes": [] }
```

Field notes:
- `vrf`: empty string `""` for the global routing table; VRF name otherwise.
- `prefix`: CIDR notation (`"10.0.0.0/8"`), both IPv4 and IPv6.
- `next_hop`: IP address string; omitted when `interface_next_hop` is set instead.
- `interface_next_hop`: interface name string; present only when there is no IP next-hop.
- `metric`, `permanent`, `tag`, `name`: optional; omitted from response when `null`.

### `GET /api/v1/devices/{id}/static-route-intent` → `200 | 404`

Re-serves the settlement coordinates of every stored intent row for this device, as
`{route_id, generation, fingerprint}` triples rendered by the same renderer the `PUT` echo
uses.

```json
{
  "device_id": 1,
  "routes": [
    { "route_id": 41, "generation": 12, "fingerprint": "9f2c…" }
  ]
}
```

This is the recovery path for a **lost PUT response**. The PUT commits its store write
before it answers, so a response lost in flight leaves the pusher holding intent the adapter
has already stored but for which the pusher recorded no expectation — and an apply result
for that intent would then correlate with nothing. The triples are rendered from the stored
rows themselves, by the same renderer the PUT uses, so a coordinate read back here cannot
drift from the coordinate a PUT reports for that row.

It is **not** a report of what any one pass wrote. The read reflects the store's CURRENT
correlation state: it echoes every stored row whatever assigned its `route_id`, and it
cannot attribute a row to a push. The **receipt** is the sole authority for that — replay
the lost push at its own `X-Push-Seq` to get that pass's exact response back. The two
answers differ on purpose under `?backfill_only=true`, whose `count` and `routes` name only
the rows the pass wrote an id onto: an entry carrying neither `route_id` nor `generation`
wrote nothing, so it is absent from the receipt while its already-correlated row is still
listed here. A recovering pusher must therefore not read this list as an acknowledgement.

`route_id` and `generation` are `null` for a row whose pusher never supplied them; a `null`
on either never correlates with anything (see below).

### `PUT /api/v1/devices/{id}/static-route-intent` → `200 | 404 | 409 | 422`

Push (full-replace) the static route intent mirror for this device.
Only routes with an IP `next_hop` are supported in v1 — interface-only next-hop
routes must be omitted by the caller.

```json
{
  "routes": [
    {
      "route_id": 41,
      "generation": 12,
      "vrf": "",
      "prefix": "0.0.0.0/0",
      "next_hop": "10.0.0.1",
      "metric": 1,
      "permanent": false,
      "tag": null
    }
  ],
  "deleted_routes": [
    {
      "route_id": 40,
      "triples": [{ "vrf": "", "prefix": "10.0.0.0/24", "next_hop": "10.0.0.2" }],
      "unverified": false
    }
  ]
}
```

Response:
```json
{
  "device_id": 1,
  "count": 1,
  "removed": 1,
  "replaced": true,
  "routes": [
    { "route_id": 41, "generation": 12, "fingerprint": "9f2c…" }
  ],
  "deleted_executed_ids": [40],
  "deleted_degraded_ids": [],
  "deleted_moot_ids": [],
  "removed_uncorrelated": []
}
```

Full-replace semantics: any route the body does not carry is deleted from the intent
mirror.  `accepted_at` defaults to the server's current UTC time if not supplied.

If `auto_apply` is `true` on the device settings and `count > 0`, an apply job is
enqueued automatically.

#### Matching: `route_id` first, then the triple

`route_id` is optional and holds the pusher's own identifier for the route (the NetBox
`routing.StaticRoute` pk). Entries are paired with stored rows by `route_id` where it is
present, and by `(vrf, prefix, next_hop)` otherwise. Because of that, editing a route's
prefix or next-hop updates the stored row **in place** — it keeps its identity, its
`accepted_at` history and its record of what was last applied — instead of reading as an
unrelated delete plus insert.

An entry carrying a `route_id` the store has not seen adopts the row holding its triple
**only if that row has no `route_id` of its own**, backfilling the id. If the row already
belongs to a different `route_id`, the two are different routes that happen to share a
triple: the stored row is deleted (its deletion is recorded) and a new row is inserted.

A push that omits `route_id` on every entry behaves exactly as it always has: matching
is by triple, and any route the body drops is treated as no longer governed
(`detach` — the device is not touched). Deletions only produce a correlated deletion
record once **every** stored row for that device carries a `route_id`.

#### `generation` and the settlement echo

`generation` is the pusher's own token for the *content* an entry carries — the pusher bumps
it on every content change and never reuses a value. The adapter stores it on the row and
reports it back on every apply result for that row, which is what lets the pusher tell a
result about the intent it is still waiting on from a result about intent that has since been
superseded. The adapter neither allocates nor interprets it; it is opaque carriage.

Like `route_id`, `generation` is **adopted only when non-null**: an entry omitting it leaves
the stored value alone, so a pusher that never learned the field cannot erase a newer one's
correlation. A row whose `generation` is `null` correlates with nothing — that is the whole
point, and it is why nothing is defaulted in its place.

The response's `routes[]` carries, per stored row, the `route_id`, the `generation` now on
the row, and the `fingerprint` of the exact wire entry that row renders. Those three are what
a pusher records as its expectation for the next apply result. Because the fingerprint is
computed from the stored row rather than from the request body, it describes what the adapter
actually holds; a pusher cannot recompute it locally, which is why it is echoed rather than
assumed. `GET /api/v1/devices/{id}/static-route-intent` re-serves the same triples if the
response is lost.

#### `deleted_routes` — the deletion authority, and its acknowledgement

A full-replace body cannot say WHY a route left it: an operator deleting a NetBox route and
an operator un-owning it produce the same shrink, and the two have opposite device outcomes.
`deleted_routes` is that answer, one record per deleted NetBox route pk:

| field | meaning |
|---|---|
| `route_id` | the deleted `routing.StaticRoute` pk |
| `triples` | its LINEAGE, most-authoritative-first: the last acknowledged triple, then the current one. Deduplicated. Empty is a **422**, and so is a third triple — the ceiling is enforced at request validation, not assumed |
| `unverified` | declared by the pusher when the overlay held no acknowledged triple. Never inferred from the lineage's shape — a verified `[C, C]` deduplicates to exactly what an unverified `[C]` produces |

The field is required on every static-route intent PUT. It is always a list. An empty list
means the push carries no deleted NetBox routes. For non-backfill pushes, omitted intent rows
are marked as per-object detaches. `?backfill_only=true` is the exception: omitted rows with a
`route_id` remain, while omitted uncorrelated rows are pruned and reported in
`removed_uncorrelated`. `?delete_origin=` does not apply to this scope.

**Classification.** Two ordered passes over the rows this push removes:

1. **by `route_id`, exclusively.** A requested id equal to a removed row's `route_id` is
   GENUINE: its removal retracts from the device, and both the id and the row leave the pool.
2. **by triple, over the remainder.** A removed `route_id IS NULL` row classifies EVERY
   remaining id whose lineage carries its triple, identically — NetBox has no uniqueness
   constraint on the triple, so two deleted pks can legitimately share one against one
   adapter row. Those ids are DEGRADED: the row is detached, and the record exists so the
   detach is not silent.

A remaining id that matched nothing is MOOT, unless its lineage is `unverified` **and** this
push removed at least one uncorrelated row — then it is degraded, attributed to that row.

The response carries the three id lists plus `removed_uncorrelated`, the triples of the
removed `route_id IS NULL` rows no requested id claimed. The three lists PARTITION the
requested set: unique within each, pairwise disjoint, no unknown id, exact coverage. All four
fields are emitted on **every** mode — normal, store-only, backfill-only and delete_origin.

Response lists are sorted and do not preserve request order. The receipt's
`request_digest` covers the body as sent — array order included — so two orderings of one
request are different delivery identities, not one replayed receipt.

#### `409` before any effect: `fence_shut`

A genuine deletion needs a tombstone, the only carrier of the deletion once the intent row
is gone for an immediately promoted request. No tombstone is written while the device's
replacement fence is shut (some row still carries no `route_id`), so a normal request with a
genuine deletion is refused with `409 conflict` and `error.detail.reason = "fence_shut"`
before any effect. `error.detail.route_ids` names the ids.

A `?store_only=true` request writes no tombstone and no job. It stores each removed row's
deletion marking in the durable receipt instead. A later `actions/apply` can promote that
exact push sequence and construct its networked and detach links without losing provenance.

#### `?backfill_only=true` — opening the fence

A key holding a pending genuine deletion cannot open its own fence: any ordinary push omitting
the deleted route destroys the before-image the deletion depends on. This mode is the way out.
Under it the request:

- adopts `route_id` and `generation` from every payload entry onto its matched row, and writes
  **no content** — a row whose adapter state has drifted stays drifted. `count` and `routes`
  report the rows an id was written onto, so an entry carrying neither field acknowledges
  nothing. Recover a lost response by replaying the push, never from the `GET`, which lists
  every stored row regardless of which pass correlated it;
- leaves every omitted row that carries a `route_id` exactly as it is;
- prunes every omitted row whose `route_id` is NULL, reporting each in `removed_uncorrelated`;
- refuses with **422** (`reason = "backfill_missing_route_id"`) if a matched row has a NULL
  `route_id` and its payload entry does not supply one; `detail.routes` names the affected triples;
- spawns nothing: no removal job, no tombstone, no auto-apply;
- carries no authority: a non-empty `deleted_routes` is a **422**
  (`reason = "backfill_carries_deletions"`);
- takes an `X-Push-Seq` and writes a receipt, so it is replayable and cannot be re-applied at
  a stale sequence. The mode is part of the receipt identity, so the same sequence delivered
  once as a backfill and once as an ordinary push is `409 sequence_reuse`, never a replay.

`static_route` is the only stream that implements it. Any other in-protocol intent PUT
carrying the flag is a **422** (`reason = "backfill_only_unsupported"`).

#### `422` refusals

All use the standard envelope with `error.code = "validation_error"`; the specific rule
is in `error.detail.reason`:

| `reason` | meaning |
|---|---|
| `duplicate_triple` | two entries carry the same `(vrf, prefix, next_hop)`; `detail.triple` names it |
| `duplicate_route_id` | two entries claim the same non-null `route_id`; `detail.route_id` names it |
| `duplicate_deleted_route_id` | two `deleted_routes` records claim the same `route_id`; emission is id-oriented, exactly one outcome per id |
| `backfill_carries_deletions` | a `?backfill_only=true` body carried a non-empty `deleted_routes` |
| `backfill_missing_route_id` | a `?backfill_only=true` entry matched an uncorrelated row but did not assign a non-null `route_id`; `detail.routes` names the affected triples |

Entries with no `route_id` never collide with each other — a body of routes that all omit
it is the normal shape and is accepted.

#### `409` while the device is busy

This endpoint reads the state it plans against **under an exclusive per-device claim**, so
two concurrent pushes cannot both plan from the same snapshot and then apply plans whose
premise is gone. If another operation (a running job, a teardown, another intent push)
holds the device, the request waits **5 seconds by default** — `intent_claim_wait_seconds`
in the adapter config — and then answers `409 conflict` with
`error.detail.reason = "device_claimed"`. The wait sits well below the plugin's 30s request
timeout, so it can never turn a would-be success into a client-side timeout. Retry is safe:
the push is a full replace.

#### Replacing a route in place: the guarded PUT

Editing a route's identity in place (see *Matching* above) leaves the device carrying the
**old** `(vrf, prefix, next_hop)` while the store holds the new one. A merge-PATCH only adds,
so it would leave both live. The adapter records per row what it last proved deployed, and
when that differs from the row's current triple it delivers the whole scope as a
**PUT-replace** of the `static-route-config` service instance instead of a merge-PATCH. The
body is then every *accepted* row of the device, not just the eligible subset — an
eligible-only replace would retract every accepted-and-clean sibling.

The replace is guarded and gated:

- The live service is read once, and that read must be **conclusive**. A keyed `404` is a real
  absence and the PUT proceeds; a `200` whose body carries no recognizable non-empty root is
  *inconclusive*, and the scope fails with `static_route_snapshot_inconclusive` — a
  destructive body must not be built from a read that may be hiding the entries it was
  supposed to preserve.
- Service entries no accepted row asserts are **collateral**. The scope refuses with
  `removal_blocked_collateral`; `error.detail.items[].orphans` names the keys and
  `…items[].preview` carries the device delta the replace would have pushed, so the operator
  can accept those routes into intent or flush them deliberately via
  `POST /api/v1/devices/{id}/actions/force-removal`.
- Entries a queued removal still owns ride through **verbatim** — including leaves the intent
  store has no column for — so an apply never drops what a removal is about to remove.
- The replace runs only while post-apply verification is enabled (`NSO_ADAPTER_VERIFY_APPLY`).
  With it off the scope stays a merge-PATCH and records nothing as deployed: a destructive
  replace whose proof is structurally unavailable is refused rather than run blind.
  A queued generation whose immutable plan already records `PUT` is failed before sync-from
  or any RESTCONF request if verification is disabled when its worker starts.
- `actions/apply-diff` renders the identical payload as a PUT dry-run, so the preview the
  operator approves is byte-for-byte what the apply sends.

#### Per-route apply results

An apply job's `result.static_route_results` carries one entry per route the pass covered:

```json
{ "route_id": 41, "row_id": 12, "key": ["", "0.0.0.0/0", "10.0.0.1"],
  "fingerprint": "9f2c…", "generation": 12, "outcome": "in_sync", "error": null }
```

| `outcome` | meaning |
|---|---|
| `in_sync` | the write landed **and** the post-apply device view proves this route's key present, with no route it replaced left over |
| `apply_failed` | the send failed, this route's key is missing from the post-apply device view, or a route this apply was replacing survived on the device |
| `unproven` | the write was accepted and nothing proves it — verification disabled or inconclusive, the device view unreadable, a replacement a merge-PATCH could not deliver, or a cleared leaf still owed |

`fingerprint` is a SHA-256 over the exact wire entry that was sent, so every emitted leaf moves
it and a store-only field with no wire form (`name`) does not. `route_id` and `generation` are
this row's stored values — `null` where the pusher supplied none, which is a statement about
that one row and never a signal about the device.

`error` is **this route's own** failure, in the standard `{code, message, detail}` shape, so a
job that fails two routes for two different reasons reports both instead of one shared scope
message. It is populated only for `apply_failed`: the stored error column outlives a pass, and
an `unproven` route can still be carrying an earlier apply's error, which reported here would
date a superseded generation's failure to this job. Every `apply_failed` outcome had its error
written by this pass, so nothing is lost by the scoping.

`unproven` is **not** a failure: the job still succeeds — a transient read failure must not
fail an apply whose device write landed — but nothing is recorded as deployed and no green is
reported for that route. Note that `result.static_route_count_by_outcome` is the older
per-scope *send* counter: it counts a route the device accepted as `in_sync` even where the
per-route outcome is `unproven`. `static_route_results` is the authoritative per-route record.

#### Clearing a leaf

Setting `metric`, `tag`, `permanent`, `interface_next_hop` or `next_hop_vrf` back to null is a
**deletion on the device**, and only a networked PUT can deliver one: the writer omits an unset
leaf, and a merge-PATCH never drops what it does not carry. The push therefore records the
cleared field names on the row, and the adapter delivers them either through a PUT-mode apply
(whose store-rendered body omits the leaf) or through one networked static-route removal job
queued for the device, whose body deletes exactly the named wire leaves and leaves every other
leaf at its live value.

Until per-field evidence from the post-write device view shows that leaf **absent or neutral**,
the route's outcome stays `unproven`. Neutral is defined per field and never by falsiness:
`metric: 0` and `tag: 0` are real values that keep the route `unproven`; `permanent: false` is
indistinguishable from unset and counts as cleared.

`?store_only=true` is honored end to end. A clear observed under it is recorded separately and
**never** authorizes a device write: no removal job is queued for it, and an unrelated networked
removal will not deliver it. It still blocks `in_sync` — the device really does carry a value
the store says is unset — and is released only by a later ordinary push re-observing the cleared
state, or by a PUT-mode apply that omits the leaf as part of its own authorized body.

Clearing `name` is a documented no-op: it has no wire leaf, so there is nothing to deliver.
No job is queued and the route's outcome is unaffected.

#### Static-route removals are live-service-relative

Removal propagation for `static_route` diverges from the shared pattern in
[Removal propagation](#removal-propagation), which rebuilds the PUT body from the remaining
accepted store rows:

- the body is the **live service minus exactly the keys this job is authorized to drop** — the
  removed route's own triple and whatever it was last proved deployed as. Everything else on
  the service rides through verbatim, so a removal can neither forward-deploy an unrelated
  store edit nor flush config no store row describes.
- because such a body cannot flush collateral, a static-route removal **no longer blocks** on
  unrelated service-owned entries. It retains them and logs
  `static_route.removal_retained_orphans`, naming exactly the retained keys no route in the
  generation document claims. That log is the operator's signal. The apply-side guard above still refuses, which
  is where a store-assertive body really can flush something.
- if the generation-creation snapshot shows that every authorized key is claimed and there is
  no cleared leaf to deliver, the job issues no device write at all and succeeds. A later push
  cannot change that recorded decision.
- the proof is **enforcing**. A key still on the device after the PUT, an unreadable device
  view, or a failed `sync-from` on an un-own fails the job and keeps the deletion record, so it
  is retried. Removals get no "succeed while unproven" treatment: a succeeded removal is what
  retires the record, so one that consumed nothing must not report success.

#### Interaction with the atomic apply

With `NSO_ADAPTER_ATOMIC_APPLY` on, every scope normally stages into one combined transaction.
Staging is merge-PATCH only, so a pass that owes a **PUT-replace** cannot ride it: the
static-route scope is excluded from the combined body and delivered by its own PUT immediately
after that transaction commits. The replacement is therefore **not** atomic with the other
scopes — a rejected follow-on fails the job and stamps only the static-route rows while the rest
of the apply stays applied. A combined commit that fails issues no follow-on at all, leaving the
static rows pending and retried, exactly like any other non-offending scope.

### `GET /api/v1/devices/{id}/interface-ips` → `200 | 404`

Returns the adapter's cached per-interface IP address mirror for this device
(populated by the `network-state-export` NSO package via SSE / scheduled poll).

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T12:00:00Z",
  "refresh_source": "poll",
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/1",
      "vrf": "",
      "address": "192.0.2.1",
      "prefix_length": 30,
      "af": "ipv4"
    }
  ]
}
```

Empty response: `{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never", "interfaces": [] }`

Field notes:
- `vrf`: empty string `""` for the global routing table; VRF name otherwise.
- `af`: `"ipv4"` or `"ipv6"`.
- `address`: IP address without prefix length.
- `prefix_length`: integer subnet mask length.

### `PUT /api/v1/devices/{id}/ip-intent` → `200 | 404`

Push (full-replace) the interface IP intent mirror for this device.

```json
{
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/1",
      "vrf": "",
      "address": "192.0.2.1",
      "prefix_length": 30,
      "af": "ipv4"
    }
  ]
}
```

Response: `{ "device_id": 1, "count": 1 }`

Full-replace semantics: any `(interface_name, af, address)` triple not present in
the request body is deleted from the intent mirror.

### `GET /api/v1/devices/{id}/isis-interfaces` → `200 | 404`

Returns the adapter's cached IS-IS interface state for this device (populated by
the `network-state-export` NSO package).

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T12:00:00Z",
  "refresh_source": "poll",
  "processes": [
    { "process_tag": "", "net": "49.0001.0000.0000.0001.00", "is_type": "level-2-only" }
  ],
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/1",
      "af": "ipv4",
      "process_tag": "",
      "circuit_type": "level-2-only",
      "network_type": "point-to-point",
      "metric": 10,
      "passive": false
    }
  ]
}
```

Empty response:
```json
{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never", "processes": [], "interfaces": [] }
```

Field notes:
- `process_tag`: empty string `""` for the unnamed (default) IS-IS process; named process tag otherwise.
- `af`: `"ipv4"` or `"ipv6"` — one row per (interface, AF) that has IS-IS enabled.
- `circuit_type`, `network_type`, `metric`: omitted from response when `null` (device default applies).
- `passive`: always present; `true` when the interface is in the process passive-interface list.

### `PUT /api/v1/devices/{id}/isis-interface-intent` → `200 | 404`

Push (full-replace) the IS-IS intent (interfaces **and** processes) for this device.
The `isis-reconciler` NSO service reads both blocks:
- `interfaces` — writes `ip/ipv6 router isis <tag>` per-interface knobs.
- `processes` — writes process-level knobs (`net`, `is-type`, `metric-style`,
  authentication, `set-overload-bit`) via the `process-config` list in the
  `isis-reconciler:isis-config` YANG service (added M18).

The service **validates process presence** for interface entries — if
`router isis <tag>` does not exist on the device and the tag is not in the
`processes` list, the interface entry is silently skipped.

```json
{
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/1",
      "af": "ipv4",
      "process_tag": "",
      "circuit_type": "level-2-only",
      "network_type": "point-to-point",
      "metric": 10,
      "passive": false,
      "accepted_at": null
    }
  ],
  "processes": [
    {
      "process_tag": "",
      "net": "49.0001.0001.0001.0001.00",
      "is_type": "level-2",
      "metric_style": "wide",
      "overload_bit": null,
      "area_auth_type": null,
      "area_auth_key": null,
      "domain_auth_type": null,
      "domain_auth_key": null,
      "accepted_at": null,
      "redistribution": [
        {
          "source_protocol": "ospf",
          "source_ref": "1",
          "route_map": "IMPORT-OSPF",
          "metric": null,
          "metric_type": "internal"
        }
      ]
    }
  ]
}
```

Response: `{ "device_id": 1, "interface_count": 2, "process_count": 1 }`

Full-replace semantics apply independently to each list:
- Any `(interface_name, af)` pair not in `interfaces` is deleted from the intent mirror.
- Any `process_tag` not in `processes` is deleted from the process intent mirror.

`accepted_at` defaults to now if not supplied.  `processes` defaults to `[]`
(omitting it leaves process intent unchanged — no deletes).

If `auto_apply` is `true` on the device settings and either count > 0, an apply job
is enqueued automatically.

---

## BGP (M15/M16)

### `GET /api/v1/devices/{id}/bgp-config` → `200 | 404`

Return the BGP config read-mirror for this device (populated from NSO via the
`network-state-export` package callpoint `bgp-config-cp`).

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T10:00:00Z",
  "refresh_source": "poll",
  "routers": [
    {
      "asn": "65100",
      "scopes": [
        {
          "vrf": "",
          "address_families": ["ipv4-unicast", "ipv6-unicast"],
          "peers": [
            {
              "peer_address": "192.0.2.1",
              "enabled": false,
              "peer_group": "UPSTREAM",
              "remote_as": "65001",
              "local_as": "65100",
              "ttl": 2,
              "password": "s3cret",
              "source": "Loopback0",
              "bfd_enabled": true,
              "address_families": [
                {"af": "ipv4-unicast", "enabled": true,
                 "routemap_in": "RM-IN", "routemap_out": "RM-OUT",
                 "prefixlist_in": "PL-IN", "prefixlist_out": "PL-OUT"}
              ]
            }
          ],
          "peer_groups": [
            {
              "name": "UPSTREAM",
              "remote_as": "65001",
              "source": "Loopback0",
              "address_families": [
                {"af": "ipv4-unicast",
                 "routemap_in": "RM-IN", "routemap_out": "RM-OUT",
                 "prefixlist_in": "PL-IN", "prefixlist_out": "PL-OUT"}
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

When no BGP data exists: `{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never", "routers": [] }`.

**Key contract (pinned by `tests/api/test_contract_bgp.py` ↔ plugin
`tests/test_contract_bgp.py`).** Optional keys are **omitted when unset**, not emitted
as `null`:

| Level | Always present | Optional (omitted when unset) |
|---|---|---|
| top | `device_id`, `last_refreshed_at`, `refresh_source`, `routers` | — |
| router | `asn`, `scopes` | — |
| scope | `vrf`, `address_families` (list[str]), `peers`, `peer_groups` | — |
| peer | `peer_address`, `enabled`, `address_families` | `peer_group`, `remote_as`, `local_as`, `ttl`, `password`, `source`, `bfd_enabled` |
| peer AF | `af`, `enabled` | `routemap_in`, `routemap_out`, `prefixlist_in`, `prefixlist_out` |
| peer_group | `name`, `address_families` | `remote_as`, `source` |
| peer_group AF | `af` | `routemap_in`, `routemap_out`, `prefixlist_in`, `prefixlist_out` |

`remote_as`/`local_as` are **strings** (ASN). `password` is included when set
(plaintext by design — BGP session passwords authenticate adjacencies, not config access).

---

### `PUT /api/v1/devices/{id}/bgp-intent` → `200 | 404`

Push (full-replace) the BGP intent snapshot for this device.  The `bgp-reconciler`
NSO service reads this intent and writes IOS BGP router/scope/peer configuration.

```json
{
  "routers": [
    {
      "asn": "65100",
      "scopes": [
        {
          "vrf": "",
          "address_families": [
            {
              "af": "ipv4-unicast",
              "redistribution": [
                {
                  "source_protocol": "ospf",
                  "source_ref": "1",
                  "route_map": "IMPORT-OSPF",
                  "metric": null
                }
              ]
            }
          ],
          "peers": [
            {
              "peer_address": "192.0.2.1",
              "enabled": true,
              "remote_as": "65001",
              "local_as": null,
              "ttl": null,
              "password": null,
              "address_families": [{"af": "ipv4-unicast", "enabled": true}]
            }
          ]
        }
      ]
    }
  ]
}
```

Response: `{ "device_id": 1, "router_count": 1 }`

Full-replace semantics: all existing BGP intent rows for the device are deleted and
replaced with the new payload atomically.  An empty `routers` list clears all intent.

`accepted_at` per router defaults to `now()` if not supplied.

`password` is stored plaintext (same design decision as the read-mirror — do not add Vault).

If `auto_apply` is `true` on the device settings and `router_count > 0`, an apply job
is enqueued automatically.

The `address_families` list on each peer may include optional policy refs:

```json
{
  "af": "ipv4-unicast",
  "enabled": true,
  "routemap_in": "IMPORT-FROM-PEER",
  "routemap_out": "EXPORT-TO-PEER",
  "prefixlist_in": "ALLOWED-PREFIXES-IN",
  "prefixlist_out": "ALLOWED-PREFIXES-OUT"
}
```

All four policy fields are optional strings (route-map or prefix-list name). The
`bgp-reconciler` NSO service applies them as `neighbor X route-map Y in/out` and
`neighbor X prefix-list Z in/out` under the appropriate address-family block.

The `address_families` list on each **scope** (not peer) may include a
`redistribution` list (M20):

```json
{
  "af": "ipv4-unicast",
  "redistribution": [
    {
      "source_protocol": "ospf",
      "source_ref": "1",
      "route_map": "IMPORT-OSPF",
      "metric": null
    }
  ]
}
```

`source_ref` is blank for `connected`/`static`; required for `ospf`/`isis`/`bgp`/`eigrp`.
The redistribution list is full-replace per `(dest_ref, source_protocol, source_ref)` key.

---

## OSPF (M19)

### `GET /api/v1/devices/{id}/ospf` → `200 | 404`

Returns the OSPF read-mirror for a device — process instances and per-interface
configuration as last observed from NSO.

```json
{
  "device_id": 1,
  "last_refreshed_at": "2025-01-01T00:00:00",
  "refresh_source": "sse",
  "instances": [
    {"process_id": "1", "vrf": "", "areas": ["0.0.0.0"], "router_id": "10.0.0.1"}
  ],
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/0",
      "passive": false, "auth_present": false,
      "process_id": "1", "area_id": "0.0.0.0",
      "priority": 10, "cost": 100,
      "network_type": "point-to-point", "auth_type": "md5"
    }
  ]
}
```

**Key contract (pinned by `tests/api/test_contract_ospf.py` ↔ plugin
`tests/test_contract_ospf.py`).** Optional keys are **omitted when unset**, not `null`:

| Level | Always present | Optional (omitted when unset) |
|---|---|---|
| top | `device_id`, `last_refreshed_at`, `refresh_source`, `instances`, `interfaces` | — |
| instance | `process_id`, `vrf`, `areas` | `router_id` |
| interface | `interface_name`, `passive`, `auth_present` | `process_id`, `area_id`, `priority`, `cost`, `network_type`, `auth_type` |

`process_id` is a **string** (named processes on IOS-XR/Junos). `areas` is an opaque
list passthrough (the plugin stores it verbatim). `last_refreshed_at` on the OSPF
endpoint is the raw datetime (no `Z` suffix, unlike BGP/ISIS).

### `PUT /api/v1/devices/{id}/ospf-intent` → `200 | 404`

Push OSPF intent for this device.  Full-replace semantics: instances and
interfaces not present in the payload are removed from the intent store.

Request body shape mirrors the GET response (`instances` + `interfaces` lists).
Each instance entry may include an optional `redistribution` list:

```json
{
  "instances": [
    {
      "process_id": 1,
      "router_id": "10.0.0.1",
      "vrf": "",
      "redistribution": [
        {
          "source_protocol": "isis",
          "source_ref": "",
          "route_map": "IMPORT-ISIS",
          "metric": null,
          "metric_type": "1"
        }
      ]
    }
  ],
  "interfaces": []
}
```

The `redistribution` list on each instance is full-replace per `(dest_ref, source_protocol, source_ref)` key.
`source_ref` is blank for `connected`/`static`; required for `ospf`/`isis`/`bgp`/`eigrp`.

The `ospf-reconciler` NSO service applies the intent to device configuration.
If the target OSPF process is absent on the device, interface entries
referencing it are silently skipped (process-presence gate in the NSO package).

**Admin-state delete-guard.** An OSPF instance is ALWAYS applied with an explicit
`enabled` (Nokia SR OS `admin-state`): when the intent row leaves `enabled` unset
(`None`) the apply body and the reconciler both default it to **enabled**. This is
load-bearing on the removal path — a PUT-replace (see below) rebuilds the service
footprint from the remaining rows, and an omitted `enabled` would drop `admin-state`
from that footprint, which FASTMAP then deletes on the device, disabling OSPF
entirely. An operator who genuinely wants an instance down must send `enabled: false`
explicitly; `None` is treated as "unknown → keep enabled", never as "remove".

If `auto_apply` is `true` on device settings and at least one eligible intent
row is present, an apply job is enqueued automatically. Removed instances/interfaces/
redistribution are reverted on the device via an async removal job (see
[Removal propagation](#removal-propagation)), not inline in this request.

---

## Redistribution (M20)

### `GET /api/v1/devices/{id}/redistribution` → `200`

Return all redistribution statements cached from NSO for this device (the flat
read-mirror; the same data also appears nested under the BGP/OSPF/ISIS intent PUTs).
The plugin consumes this in `redistribution_reconciler.reconcile_redistribution`.

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T10:00:00",
  "refresh_source": "poll",
  "entries": [
    {
      "dest_protocol": "ospf", "dest_ref": "1",
      "source_protocol": "bgp", "source_ref": "65100",
      "route_map": "RM-REDIST", "metric": 100, "metric_type": "type-1"
    },
    {"dest_protocol": "isis", "dest_ref": "", "source_protocol": "connected", "source_ref": ""}
  ]
}
```

When no data exists: `{ "device_id": 1, "last_refreshed_at": null, "refresh_source": "never", "entries": [] }`.

**Key contract (pinned by `tests/api/test_contract_redistribution.py` ↔ plugin
`tests/test_contract_redistribution.py`).** Optional keys are **omitted when unset**,
not `null`:

| Level | Always present | Optional (omitted when unset) |
|---|---|---|
| top | `device_id`, `last_refreshed_at`, `refresh_source`, `entries` | — |
| entry | `dest_protocol`, `dest_ref`, `source_protocol`, `source_ref` | `route_map`, `metric`, `metric_type` |

`source_ref` is blank for `connected`/`static`; populated for `ospf`/`isis`/`bgp`.

---

## Route Policy (M17)

### `GET /api/v1/devices/{id}/route-policy` → `200 | 404`

Return the route-policy read-mirror: the four policy-object families (prefix-list /
community-list / as-path / route-map) each with their ordered entries. Consumed by the
plugin in `route_policy_reconciler.reconcile_route_policy`. Full YANG-level detail in
`m17-route-policy-contract.md`. **No top-level `refresh_source`** on this endpoint.

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-06-01T10:00:00Z",
  "prefix_lists": [
    {"name": "PL-1", "family": 4, "entries": [
      {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8", "ge": 16, "le": 24},
      {"sequence": 20, "action": "deny", "prefix": "0.0.0.0/0"}
    ]}
  ],
  "community_lists": [
    {"name": "CL-1", "entries": [{"sequence": 10, "action": "permit", "community": "65000:100"}]}
  ],
  "as_paths": [
    {"name": "AP-1", "entries": [{"sequence": 10, "action": "permit", "pattern": "^65000_"}]}
  ],
  "route_maps": [
    {"name": "RM-1", "entries": [
      {"sequence": 10, "action": "permit",
       "match_prefix_lists": ["PL-1"], "match_community_lists": ["CL-1"], "match_as_paths": ["AP-1"],
       "match": "{\"prefix\": \"PL-1\"}", "set": "{\"local_preference\": 200}"}
    ]}
  ]
}
```

**Key contract (pinned by `tests/api/test_contract_route_policy.py` ↔ plugin
`tests/test_contract_route_policy.py`).** Only the prefix-list entry has optional keys
(`ge`/`le`, omitted when unset); every other level emits a fixed key set:

| Level | Keys |
|---|---|
| top | `device_id`, `last_refreshed_at`, `prefix_lists`, `community_lists`, `as_paths`, `route_maps` |
| prefix_list | `name`, `family` (int 4/6), `entries` |
| prefix_list entry | `sequence`, `action`, `prefix` (+ optional `ge`, `le`) |
| community_list / as_path | `name`, `entries` |
| community_list entry | `sequence`, `action`, `community` |
| as_path entry | `sequence`, `action`, `pattern` |
| route_map | `name`, `entries` |
| route_map entry | `sequence`, `action`, `match_prefix_lists`, `match_community_lists`, `match_as_paths`, `match`, `set` |

`match_*` are `list[str]` (object names); `match`/`set` are **JSON strings** the plugin
`json.loads`es. `action` is normalised to permit/deny by the plugin.

### `PUT /api/v1/devices/{id}/route-policy-intent` → `200 | 404 | 422`

Store route-policy object intent for the device. **Full-replace**: the plugin
always pushes the full owned set; objects absent from the payload are deleted
from the mirror. Each object with `"accepted": true` gets `accepted_at`
stamped. If `auto_apply` is enabled on the device, an apply job is enqueued.

```json
{ "objects": [
    { "family": "prefix_list", "name": "PL-RFC1918",
      "entries": [ {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8"} ],
      "accepted": true }
] }
```

Valid `family` values: `prefix_list` · `community_list` · `as_path` ·
`route_map` (unknown family → `422 invalid_family`). The
`route-policy-reconciler` NSO service applies the objects on the next Apply.

`route_map` entries use the reconciler's YANG leaf names (passed verbatim
into the service payload): `sequence`, `action`, `match-prefix-lists`,
`match-community-lists`, `match-as-paths`, `match-json`, `set-json` (the
JSON blobs are serialised strings, keys per m17-route-policy-contract.md §2).
Legacy snake_case / `match`+`set` entries are normalised at apply time.

### Capability matrix (route-policy pre-flight)

A per-device compatibility cache that lets the plugin flag, **at attach time**, which parts
of a route-map / community-list won't apply on a device — instead of the operator finding
out only when it silently didn't land. Backed by the persisted `device_capability` table,
keyed by **`(ned_id, sw_version)`** so identical boxes share one verdict (probe one, reuse
for many). Design + rationale: `compatibility-matrix-plan.md`.

Each verdict row has a `status` — `native` (applies as-is) · `translated` (dialect-rewritten,
e.g. Nokia color→hex) · `skipped` (the NED can't model it) · `unsupported` (the device parser
**refused** it at commit) — and a `source`: `probe` (representable half, from the NSO
capability-probe action) or `apply` (accepted half, from a real `apply_failed` device
rejection). **Apply wins** — a real rejection is never downgraded by a later probe.

**Key resolution — `refresh`.** Resolving a device's `(ned_id, sw_version)` key normally needs
a live NSO probe. The `refresh` query param controls this: `refresh=true` probes NSO now and
persists the learned key on the device row (authoritative — "check now" / attach-time);
`refresh=false` serves the **last-learned** key from the device row with no probe (the cheap
panel read), reporting `"known": false` when the device has never been probed.

All three endpoints return `409 no_nso_client` when the device's NSO instance has no client.

#### `POST /api/v1/devices/{id}/capability/refresh` → `200 | 404 | 409`

Force a probe now and persist the representable-half verdict. Returns the learned key + count:

```json
{ "ned_id": "cisco-ios-cli-6.114", "sw_version": "17.15.4c", "count": 23 }
```

A probe that reports no NED returns `{"ned_id": "", "sw_version": "", "count": 0}`.

#### `GET /api/v1/devices/{id}/capability?refresh={bool}` → `200 | 404 | 409`

Return the cached verdict rows for this device's key. **`refresh` defaults to `false`** (cache-only).

```json
{
  "known": true,
  "ned_id": "cisco-ios-cli-6.114", "sw_version": "15.2(4)E10",
  "elements": [
    {"scope": "community", "name": "color:0:128", "status": "skipped", "detail": "no IOS home", "source": "probe"},
    {"scope": "rm-set", "name": "set extcommunity color", "status": "unsupported", "detail": "% Invalid input", "source": "apply"}
  ]
}
```

A never-probed device (cache-only) returns `{"known": false, "ned_id": "", "sw_version": "", "elements": []}`.
`scope` is `community` · `rm-set` · `rm-match`.

#### `POST /api/v1/devices/{id}/route-policy/preflight?refresh={bool}` → `200 | 404 | 409`

Check a would-be attach against the matrix. **`refresh` defaults to `true`** (an attach is an
explicit, authoritative check). Body — what the object would push (the plugin derives it from
the netbox-routing object, mirroring the route-policy intent it would send):

```json
{ "community_members": ["color:0:200", "65000:9"],
  "set_keys": ["extcommunity_color", "metric_type"],
  "match_keys": ["local_preference"] }
```

`community_members` are matched by **KIND** (`color:0:200` inherits the verdict probed for any
`color` member); `set_keys` / `match_keys` are the set-/match-json keys, mapped to construct
names (`extcommunity_color` → `set extcommunity color`, `metric_type` → `set metric-type`, …).

```json
{ "known": true, "ned_id": "cisco-ios-cli-6.114", "sw_version": "15.2(4)E10",
  "fully_supported": false,
  "unsupported": [
    {"scope": "community", "element": "color:0:200", "status": "skipped", "detail": "no IOS home"}
  ] }
```

An element flags when its matched row is `skipped` or `unsupported`. **Block-only-on-known-negative:**
an unknown device (cache-only, never probed) or unreachable adapter returns
`{"known": false, "fully_supported": true, "unsupported": []}` — the plugin must **not** block on
it, only on a `known` + `fully_supported: false` verdict.

---

## IS-IS (M18 / M33)

### `GET /api/v1/devices/{id}/isis-interfaces` → `200 | 404`

The deepest read-mirror: IS-IS `processes` and per-`interfaces` config. Processes carry a
large optional scalar set plus four nested JSON-bag containers; interfaces carry their own
scalars + `settings`/`levels`. Optional scalars and the containers are **omitted when
unset**. The nested dicts are hyphen→snake normalised (`_snake`) and the plugin reads fixed
key sets out of them. Consumed by the plugin in `template_content._reconcile_isis_process`
/ `_reconcile_isis_interfaces`.

```json
{
  "device_id": 1, "last_refreshed_at": "2026-06-01T10:00:00Z", "refresh_source": "poll",
  "processes": [
    {"process_tag": "1", "net": "49.0001...00", "is_type": "level-2", "sr_enabled": true,
     "levels": [{"level": "2", "default_metric": 10, "wide_metrics_only": true, "preference": 7,
                 "labeled_preference": 7, "disabled": false, "auth_type": "md5"}],
     "segment_routing": {"enabled": true, "prefix_sid_range": "global", "srgb_start": 100000,
                         "srgb_range": 200000, "node_sid_index": 100, "node_sid_label": 100100,
                         "node_sid_v6_index": 200, "node_sid_v6_label": 100200,
                         "maximum_sid_depth": 10, "tunnel_table_pref": 8},
     "flex_algos": [{"algo_id": 128, "metric_type": "igp", "priority": 100,
                     "admin_group_exclude": ["RED"], "admin_group_include_any": ["BLUE"],
                     "admin_group_include_all": []}]}
  ],
  "interfaces": [
    {"interface_name": "GE0/0", "af": "ipv4", "process_tag": "1", "passive": false,
     "metric": 10, "network_type": "point-to-point",
     "levels": [{"level": "2", "metric": 10, "hello_interval": 3, "hello_multiplier": 3,
                 "priority": 64, "passive": false}]}
  ]
}
```

**Key contract (pinned by `tests/api/test_contract_isis.py` ↔ plugin
`tests/test_contract_isis.py`).** Optional keys omitted when unset:

| Level | Always present | Optional (omitted when unset) |
|---|---|---|
| top | `device_id`, `last_refreshed_at`, `refresh_source`, `processes`, `interfaces` | — |
| process | `process_tag` | `net`, `is_type`, `metric_style`, `overload_bit`, `area_auth_type/present/key`, `domain_auth_type/present/key`, `spf_initial_wait`, `spf_max_wait`, `lsp_initial_wait`, `lsp_max_wait`, `lsp_lifetime`, `lsp_refresh_interval`, `lsp_mtu`, `overload_on_startup`, `overload_timeout`, `te_enabled`, `sr_enabled`, `sr_node_msd`, `distance`, `maximum_paths`, `reference_bandwidth`, `settings`, `levels`, `segment_routing`, `flex_algos` |
| interface | `interface_name`, `af`, `process_tag`, `passive` | `circuit_type`, `network_type`, `metric`, `bound_port`, `hello_auth_type`, `hello_auth_present`, `bfd_enabled`, `csnp_interval`, `retransmit_interval`, `lsp_interval`, `mesh_group`, `settings`, `levels` |

Nested-bag key sets (snake_case): **instance level** = `level`, `default_metric`,
`wide_metrics_only`, `preference`, `labeled_preference`, `disabled`, `auth_type`;
**interface level** = `level`, `metric`, `hello_interval`, `hello_multiplier`, `priority`,
`passive`; **segment_routing** = `enabled`, `prefix_sid_range`, `srgb_start`, `srgb_range`,
`node_sid_index`, `node_sid_label`, `node_sid_v6_index`, `node_sid_v6_label`,
`maximum_sid_depth`, `tunnel_table_pref`; **flex_algos** = `algo_id`, `metric_type`,
`priority`, `admin_group_exclude`, `admin_group_include_any`, `admin_group_include_all`.
`process_tag` / `af` are strings; `settings` is an opaque EAV `{key: value}` bag.

### `PUT /api/v1/devices/{id}/isis-flex-algo-intent` → `200 | 404`

IS-IS Flex-Algorithm intent, keyed `(process_tag, algo_id)`. Standard
*intent-mirror PUT* (see box below). `admin_group_*` are comma-joined
strings (not lists) at this layer.

```json
{ "flex_algos": [
    { "process_tag": "", "algo_id": 128, "metric_type": "igp", "priority": 100,
      "admin_group_exclude": "RED", "admin_group_include_any": "BLUE,GREEN",
      "admin_group_include_all": null, "accepted_at": "2026-06-01T10:00:00Z" }
] }
```

---

## The intent-mirror PUT pattern

Every `PUT /api/v1/devices/{id}/*-intent` endpoint below (and `vlan-intent`,
`svi-intent`, …) shares the same contract:

- **Full-replace**: the plugin pushes the device's full owned snapshot; rows
  absent from the body are deleted from the mirror. An empty list clears the
  scope.
- `accepted_at` per row; defaults to *now* when omitted.
- Storing intent **never touches the device synchronously**. If `auto_apply` is enabled in
  the device settings, an ordinary non-store-only PUT enqueues the scope's apply job.
  Otherwise the intent remains stored in the mirror.
- Explicit `actions/apply` promotes every section through `DOCUMENT_EXECUTED_SECTIONS`, so
  all sixteen streams are executable from their stored generation documents. Use a new
  `X-Push-Seq` when resending a stored payload because receipt replay returns the recorded
  response without new work.
- Where dropping a row from a keyed NSO service list requires it, the adapter
  queues an async removal job (see [Removal propagation](#removal-propagation)).
- → `200` `{ "device_id": 1, "count": <rows stored>, "removed": <rows dropped> }`.
  The PUT returns as soon as the rows are stored; if rows were dropped a `removal`
  job is queued and runs in the background (the response does not wait for it).
- `404` unknown device; `422` invalid payload.

### Removal propagation

A merge-PATCH apply never drops a list entry the payload omits, so deleting a row
from the intent store would otherwise leave the config orphaned on the device. To
revert it, the owning `*-reconciler` service instance is **PUT-replaced** with the
full remaining accepted state, which lets NSO FASTMAP delete the dropped entries.

The same rule applies to a device-effective scalar that changes from emitted state to
omitted state. A clear with no un-own gets a networked removal job, so that admission
discharges any pending-clear row for the promoted stream. If an un-own rides with the
clear, the existing detach job still admits and the clear is deferred. The adapter then
records one `stream_pending_clear` row for each promoted stream at its current desired
revision. Static routes use their own per-route pending-clear mechanism and never use this
table. L2 SAP `port`, `outer_tag`, and `inner_tag` are informational and key-derived, so
omitting them is not a device-effective clear.

The recording provenance comes from the request mode. An ordinary deferred clear records
`authorized`. A clear detected under `?store_only=true` records `store_only`, creates no
job, and creates no deployment generation. A later authorized recording for the same
stream replaces its store-only row and keeps the highest revision. A later store-only push
never demotes or advances an authorized row. A parked row can leave store-only provenance
only when a later authorized push of the same stream reaches the removal choke. The
Apply-action promotion-release hook is follow-on work above this branch, so this core does
not release a parked row from an unrelated Apply.
Recording is per stream, not per document section. An `isis` push cannot create or promote
an `isis_flex_algo` row.

`POST /api/v1/devices/{id}/actions/force-removal` is the operator discharge. When its
promotion-free removal job admits, it deletes the device's pending-clear rows for every
stream in the affected section in the same transaction. It records no new row.

That PUT-replace is a synchronous device commit that can exceed the plugin's HTTP
client timeout (~30s). So it does **not** run inline in the intent PUT: when an
intent PUT drops one or more rows it enqueues **`removal`** jobs (a `jobtype`
enum value), one per deletion-marking group. A request whose drops carry both
delete-origin and detach markings produces a networked job and a detach job
atomically with the row deletes, and returns immediately.
The `PUT .../static-route-intent?store_only=true` path is the exception: it updates
the mirror without authorizing the drop, so it creates neither a removal job nor a
tombstone (and no apply job, whatever the device's `auto_apply` setting) — clients
must not wait for jobs it never creates.
The `PUT .../static-route-intent?backfill_only=true` path is also an exception: it
prunes omitted uncorrelated rows but creates neither a removal job nor a tombstone.
A worker runs each job in the background. A promoted generation hydrates its exact stored
document and execution plan, so a retry repeats the same selected operation after a worker
restart. A reissue generation carries no promoted revisions or stored execution plan and
executes its one removal context's scope from then-current live state. The sweeper, reclaimer,
and force-removal paths produce reissues. A removal job with no generation is invalid and is
refused. Scope is carried in `Job.context.scope` (one of
`route_policy · bfd · svi · subinterface · static_route · interface_mtu · vlan ·
logging · l2_sap · ospf · bgp · isis · interface_config · snmp`). Job status is
observable via `GET …/jobs` like any other job; a failed removal records
`error.code = "removal_failed"`.

`static_route` is the one scope that does **not** rebuild its body from the remaining accepted
rows — it drops exactly what it is authorized to drop and keeps the rest of the live service
verbatim. See
[Static-route removals are live-service-relative](#static-route-removals-are-live-service-relative).

---

## LAG (M9 read / M33 write)

### `GET /api/v1/devices/{id}/lag-topology` → `200 | 404`

Read-mirror of LAG membership (M9 — topology only).

```json
{ "device_id": 1, "last_refreshed_at": "2026-06-01T10:00:00Z", "refresh_source": "poll",
  "lags": [
    { "name": "Port-channel1", "id": 1,
      "members": [ { "interface": "GigabitEthernet0/1", "mode": "active" } ] }
  ] }
```

### `GET /api/v1/devices/{id}/lag-config` → `200 | 404`

Read-mirror of full LACP bundle config (M33). Optional keys omitted when
unset: bundle `min_links`/`system_priority`/`system_id`/`timer`/`admin_key`;
member `mode`/`port_priority`.

```json
{ "device_id": 1, "last_refreshed_at": "2026-06-01T10:00:00Z", "refresh_source": "poll",
  "bundles": [
    { "name": "lag-2", "lag_id": 2, "min_links": 1, "system_priority": 32768,
      "members": [ { "interface_name": "1/1/c2/1", "mode": "active", "port_priority": 100 } ] }
  ] }
```

### `POST /api/v1/devices/{id}/lag-config/apply` → `200 | 404`

Synchronous direct apply (NOT the intent-mirror pattern — LAG is owned in
NetBox and applied via the `lag-reconciler` service immediately). Body =
the GET `bundles` shape.

```json
{ "status": "deployed", "device": "lab01c-ra1", "bundle_count": 1 }
```

`status: "error"` (with `error`/`message`/`detail`) on NSO failure — the
HTTP status stays 200; callers check `status`.

---

## VLAN database & switchport (M34)

### `GET /api/v1/devices/{id}/vlan-database` → `200 | 404`

```json
{ "device_id": 1,
  "vlans": [ { "vlan_id": 100, "name": "users", "source": "vlan-database" } ] }
```

### `GET /api/v1/devices/{id}/switchport` → `200 | 404`

L2 switchport read-mirror. `mode` ∈ `access` · `trunk` · `""` (unset);
`untagged_vlan` nullable int; `tagged_vlans` sorted list of ints.

```json
{ "device_id": 1,
  "interfaces": [
    { "interface_name": "GigabitEthernet1/0/1", "mode": "trunk",
      "untagged_vlan": 1, "tagged_vlans": [100, 200], "source": "switchport" }
  ] }
```

### `POST /api/v1/devices/{id}/switchport/apply` → `200 | 404`

Synchronous direct apply (like `lag-config/apply` — switchport is owned in
NetBox, not mirrored as adapter intent). Body = `{ "interfaces": [...] }`
with the GET row shape minus `source`.

```json
{ "status": "deployed", "device": "sw03", "interface_count": 2 }
```

### `PUT /api/v1/devices/{id}/vlan-intent` → `200 | 404`

VLAN-database intent, keyed `vlan_id`. Standard intent-mirror PUT.

```json
{ "vlans": [ { "vlan_id": 100, "name": "users", "accepted_at": "2026-06-01T10:00:00Z" } ] }
```

---

## SVI / IRB (M35)

### `GET /api/v1/devices/{id}/svi` → `200 | 404`

L3 VLAN interfaces (Cisco `interface VlanN`, Junos `irb.N`). No IPs — those
ride `interface-ips`.

```json
{ "device_id": 1,
  "interfaces": [
    { "interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "", "source": "svi" }
  ] }
```

### `PUT /api/v1/devices/{id}/svi-intent` → `200 | 404`

Keyed `interface_name`. Standard intent-mirror PUT; rows carry
`interface_name`, `vlan_id`, `type`, `vrf`, `accepted_at`.

---

## dot1q subinterfaces (M36)

### `GET /api/v1/devices/{id}/subinterface` → `200 | 404`

```json
{ "device_id": 1,
  "interfaces": [
    { "interface_name": "Port-channel10.200", "parent_interface": "Port-channel10",
      "dot1q_vlan": 200, "type": "subinterface", "vrf": "", "source": "subinterface" }
  ] }
```

### `PUT /api/v1/devices/{id}/subinterface-intent` → `200 | 404`

Keyed `interface_name`. Standard intent-mirror PUT; rows carry
`interface_name`, `parent_interface`, `dot1q_vlan`, `type`, `vrf`,
`accepted_at`.

---

## L2 services (M37 — Nokia epipe/vpls + SAP)

### `GET /api/v1/devices/{id}/l2-services` → `200 | 404`

SAP rows grouped by service. `service_type` ∈ `epipe` · `vpls`.

```json
{ "device_id": 1,
  "services": [
    { "service_name": "CUST-A", "service_type": "epipe", "service_id": 1001,
      "saps": [ { "sap_id": "1/1/c3/1:100", "port": "1/1/c3/1",
                  "outer_tag": 100, "inner_tag": null } ] }
  ] }
```

### `PUT /api/v1/devices/{id}/l2-sap-intent` → `200 | 404`

Keyed `(service_name, sap_id)`. Standard intent-mirror PUT; rows carry
`service_name`, `service_type`, `sap_id`, `port`, `outer_tag`, `inner_tag`,
`accepted_at`.

---

## Interface MTU (Phase 2b)

### `GET /api/v1/devices/{id}/interface-mtu` → `200 | 404`

Per-interface `mtu` / `ip_mtu` / `mpls_mtu` (all nullable — read with
NO_DEFAULTS, so only explicitly-configured values appear). `bound_port`
carries the Nokia port whose L2 MTU governs a router interface (`""`
elsewhere).

```json
{ "device_id": 1,
  "interfaces": [
    { "interface_name": "LAG99:99", "mtu": null, "ip_mtu": 9000,
      "mpls_mtu": null, "bound_port": "lag-99" }
  ] }
```

### `PUT /api/v1/devices/{id}/interface-mtu-intent` → `200 | 404`

Keyed `interface_name`. Standard intent-mirror PUT; rows carry
`interface_name`, `mtu`, `ip_mtu`, `mpls_mtu`, `accepted_at`.

---

## BFD (M32)

### `GET /api/v1/devices/{id}/bfd` → `200 | 404`

Per-interface BFD read-mirror. `micro_bfd` distinguishes LAG micro-BFD from
plain interface BFD. Optional keys (`bound_port`, `min_tx`, `min_rx`,
`multiplier`) omitted when unset.

```json
{ "device_id": 1, "last_refreshed_at": "2026-06-01T10:00:00Z", "refresh_source": "poll",
  "interfaces": [
    { "interface_name": "lag-2", "micro_bfd": true, "enabled": true,
      "min_tx": 300, "min_rx": 300, "multiplier": 3 }
  ] }
```

### `PUT /api/v1/devices/{id}/bfd-intent` → `200 | 404`

Keyed `interface_name`. Standard intent-mirror PUT; rows carry
`interface_name`, `min_tx`, `min_rx`, `multiplier`, `micro_bfd`,
`accepted_at`. Applied via the `bfd-reconciler` NSO service.

---

## Logging / syslog

### `GET /api/v1/devices/{id}/logging-config` → `200 | 404`

Remote syslog hosts. Optional keys (`port`, `severity`, `facility`,
`transport`, `vrf`, `source`) omitted when unset.

```json
{ "device_id": 1, "last_refreshed_at": "2026-06-01T10:00:00Z", "refresh_source": "poll",
  "hosts": [
    { "address": "10.0.0.50", "port": 514, "severity": "informational",
      "transport": "udp", "vrf": "MGMT" } ] }
```

### `PUT /api/v1/devices/{id}/logging-intent` → `200 | 404`

Keyed `address`. Standard intent-mirror PUT; rows carry `address`, `port`,
`severity`, `facility`, `transport`, `vrf`, `source`, `accepted_at`.
Applied via the `logging-reconciler` NSO service.
