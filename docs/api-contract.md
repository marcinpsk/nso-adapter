# nso-adapter Northbound API Contract

- **Status:** draft for implementation handover
- **Consumers:** `netbox-nso-plugin` (Phase 1); other consumers later.
- **This file is canonical.** Both teams build against it. The plugin team
  mocks these endpoints; the adapter team also serves an auto-generated
  OpenAPI document from FastAPI at `/openapi.json` that must match this file.

---

## Conventions

- Base path: `/api/v1`. All bodies JSON (`application/json`).
- **Auth:** `Authorization: Bearer <adapter_token>`. Static token, called
  `adapter_token` on **both** sides (plugin env `NSO_ADAPTER_TOKEN` →
  `PLUGINS_CONFIG["netbox_nso_plugin"]["adapter_token"]`; adapter config
  `api.adapter_token_ref`). Missing/invalid → `401`.
- Timestamps: ISO-8601 UTC.
- Async operations return a **job**; the consumer polls `GET /jobs/{id}`.
- Error body (all non-2xx):
  ```json
  { "error": { "code": "string", "message": "human readable", "detail": {} } }
  ```
  Codes (closed set — `/openapi.json` enum must match):
  - Phase 1: `unauthorized`, `not_found`, `validation_error`,
    `nso_unreachable`, `netbox_unreachable`, `conflict`, `internal`.
  - Phase 2: `not_implemented` (apply endpoint pre-M4),
    `nso_commit_failed` (M5+, reconcile-commit refused or partially
    failed; `error.detail.attributes` lists the failed ones).

### Call directions

The plugin does **not** expose any endpoint the adapter calls back into.
Everything the adapter "reads from the plugin" goes through NetBox's own
REST API for the plugin's models.

| From | To | What | Phase |
|------|----|------|-------|
| plugin → adapter | `POST /api/v1/devices/{id}/sync-notify` | scope/intent changed, sync now (push kicker) | 1+ |
| plugin → adapter | `POST/PATCH/DELETE /api/v1/devices/...` | onboard, edit mapping, offboard | 1+ |
| plugin → adapter | `PUT /api/v1/devices/{id}/scope` | push scope on save | 1+ |
| plugin → adapter | `PUT /api/v1/devices/{id}/intent` | **push intent on accept** | 2 |
| plugin → adapter | `POST /api/v1/devices/{id}/actions/{sync,check-compliance,connect,apply}` | trigger jobs | 1+, `apply` is 2 |
| plugin → adapter | `GET /api/v1/devices/{id}/{interfaces,compliance,intent,scope}`, `GET /api/v1/jobs/...` | read state | 1+, `intent` is 2 |
| adapter → NetBox | `GET /api/plugins/netbox-nso-plugin/device-management/` | reconcile mirrored scope (pull) | 1+ |
| adapter → NetBox | `GET /api/plugins/netbox-nso-plugin/interface-state/` | **reconcile mirrored intent (pull)** | 2 |
| adapter → NetBox | `PATCH dcim.Interface` (and create if missing) | write synced attribute values | 1+ |
| adapter → NSO   | RESTCONF `sync-from`, `compare-config`, `check-sync`, `connect` (`/devices/device`) | Phase 1 NSO surface | 1+ |
| adapter → NSO   | RESTCONF write to a thin reconcile-commit service (Spike S2) | apply intent | 2 |
| adapter → plugin | **nothing** — direct adapter→plugin calls are not part of the contract | — | — |

Implementers: if you find yourself adding an HTTP call from the adapter to
a path on the plugin (anything under `/plugins/netbox-nso-plugin/` other
than via NetBox's REST for the plugin model), stop — that direction is not
in the spec.

## Enums

- **`mapping_status`**: `mapped` · `unmatched_device` · `unmatched_interfaces`
- **`job.type`**: `sync` · `check-compliance` · `connect` · `apply` (Phase 2)
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
  - `drifted` — was `in_sync`, then `check-compliance` found device value
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

### `GET /api/v1/devices/{id}` → `200`
Device object plus `scope` (see below) and `last_job_id`.

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
Same shape as the `PUT` request body, plus `updated_at`.

## Interfaces & compliance

### `GET /api/v1/devices/{id}/interfaces` → `200`
```json
[ { "name": "GigabitEthernet0/0/0/1", "netbox_interface_id": 555,
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

### `GET /api/v1/devices/{id}/compliance` → `200`
```json
{ "device_id": 1, "managed_interfaces": 12,
  "by_status": { "unknown": 1, "imported": 22, "changed": 1, "accepted": 0,
                 "deploying": 0, "in_sync": 0, "apply_failed": 0,
                 "drifted": 0, "error": 0 },
  "last_checked_at": "2026-05-20T10:06:00Z" }
```
Phase 1: only `unknown`/`imported`/`changed`/`error` are ever non-zero.
Phase 2: the rest activate as M5–M6 land.

## Actions (async)

All return `202` with `{ "job_id": <int> }`. Only one job per device runs at a
time: if an action is requested while a `queued`/`running` job exists for that
device, the adapter returns `409 conflict` with the existing job's id in
`error.detail.job_id`.

### `POST /api/v1/devices/{id}/actions/sync`
Runs NSO `sync-from`, reads managed attributes, writes them to NetBox,
recomputes compliance.

### `POST /api/v1/devices/{id}/actions/check-compliance`
Runs `compare-config` / re-reads NSO and recomputes compliance **without**
writing to NetBox. Used to detect out-of-band changes.

### `POST /api/v1/devices/{id}/actions/connect`
NSO `connect` — connectivity test only.

### `POST /api/v1/devices/{id}/actions/apply` (Phase 2)
Push accepted NetBox intent to NSO via the reconcile-commit service (Spike
**S2**, milestone **M4**). Until M4 ships, this endpoint returns `501`:
```json
{ "error": { "code": "not_implemented",
             "message": "apply action requires Phase 2 NSO reconcile service (Spike S2 / M4)",
             "detail": { "spike": "S2", "milestone": "M4",
                         "docs": "docs/00-plan.md#11-phase-2-kickoff-2026-05-23" } } }
```

Once M4 lands, normal contract:

- Body: empty, or `{ "force": true }` (default `true` — see decision Q in
  `docs/00-plan.md` §11). `force=true` pushes every eligible attribute on
  the device (`accepted` / `apply_failed` / `drifted` / `in_sync`) so the
  operator can "shake the tree". `force=false` skips `in_sync` attributes
  (idempotent NSO no-op avoided).
- → `202 { "job_id": <int> }`. Same 409 concurrency rule as other actions.
- The apply worker (see `nso-adapter.md` §7a):
  1. Snapshots the device's `interface_intent` rows at job start into the
     `job.context` field — this snapshot is the audit trail; the plugin's
     live state may move on.
  2. For each in-scope attribute: writes via the reconcile-commit service,
     status transitions `accepted`/`drifted`/`apply_failed` → `deploying`.
  3. On NSO commit success: status → `in_sync`, `last_apply_at` updated.
  4. On NSO commit failure: status → `apply_failed`, NSO error captured in
     `last_apply_error`; intent value **unchanged**; no rollback attempted
     (decision O).
- New error codes for this endpoint:
  - `not_implemented` (501) — pre-M4.
  - `nso_commit_failed` (502) — the apply ran but at least one attribute's
    commit failed; `error.detail.attributes` lists the failed ones.

### `POST /api/v1/devices/{id}/sync-notify`
**Served by the adapter; called by the plugin** (Django `post_save` on
`NSODeviceManagement`, and any future scope/intent-changing model) when scope
or intent changes for this device. Triggers an immediate sync job, so the
user doesn't have to wait for the next scheduled poll. Same 202/409 semantics
as `actions/sync`.

This is the **only** plugin → adapter push beyond the standard `/api/v1/*`
client calls. The adapter never calls back into the plugin (see *Call
directions* under §Conventions).
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
  "created_at": "2026-05-20T10:05:30Z",
  "updated_at": "2026-05-20T10:06:00Z" }
```
`failed` jobs carry `error` in the standard error shape; `result` is `null`.

### `GET /api/v1/jobs?device_id={id}&status={status}` → `200`
Array of job objects. Both query params optional.

---

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

### `PUT /api/v1/devices/{id}/static-route-intent` → `200 | 404`

Push (full-replace) the static route intent mirror for this device.
Only routes with an IP `next_hop` are supported in v1 — interface-only next-hop
routes must be omitted by the caller.

```json
{
  "routes": [
    {
      "vrf": "",
      "prefix": "0.0.0.0/0",
      "next_hop": "10.0.0.1",
      "metric": 1,
      "permanent": false,
      "tag": null
    }
  ]
}
```

Response:
```json
{ "device_id": 1, "count": 1 }
```

Full-replace semantics: any route key `(vrf, prefix, next_hop)` not present in the
request body is deleted from the intent mirror.  `accepted_at` defaults to the
server's current UTC time if not supplied.

If `auto_apply` is `true` on the device settings and `count > 0`, an apply job is
enqueued automatically.

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
              "enabled": true,
              "peer_group": "UPSTREAM",
              "remote_as": "65001",
              "local_as": null,
              "ttl": null,
              "password": null,
              "address_families": [
                {"af": "ipv4-unicast", "enabled": true}
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

`password` is included when set (plaintext by design — BGP session passwords authenticate adjacencies, not config access).

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

### `GET /api/v1/devices/{id}/route-policy-state` → `200 | 404`

Returns the route-policy read-mirror for a device.  Covers all four families:
prefix-list, community-list, as-path, and route-map.

```json
{
  "device_id": 1,
  "last_refreshed_at": "2026-01-15T12:00:00Z",
  "refresh_source": "sse",
  "prefix_lists": [
    {
      "name": "ALLOWED-IN",
      "entries": [
        {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8", "ge": null, "le": null}
      ]
    }
  ],
  "community_lists": [
    {
      "name": "LOCAL-COMMUNITY",
      "entries": [{"value": "65100:100"}]
    }
  ],
  "as_paths": [
    {
      "name": "ALLOW-PEERS",
      "entries": [{"sequence": 10, "action": "permit", "regex": "^65001_"}]
    }
  ],
  "route_maps": [
    {
      "name": "IMPORT-FROM-PEER",
      "entries": [
        {
          "sequence": 10,
          "action": "permit",
          "match": {"ip-address": "ALLOWED-IN"},
          "set": {"local-preference": 100}
        }
      ]
    }
  ]
}
```

---

### `PUT /api/v1/devices/{id}/route-policy-intent` → `200 | 404`

Push route-policy objects for this device.  Partial-update semantics: only objects
present in the request body are modified; omitted names are untouched.

```json
[
  {
    "family": "prefix_list",
    "name": "ALLOWED-IN",
    "entries": [
      {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8", "ge": null, "le": null}
    ]
  },
  {
    "family": "route_map",
    "name": "IMPORT-FROM-PEER",
    "entries": [
      {
        "sequence": 10,
        "action": "permit",
        "match": {"ip-address": "ALLOWED-IN"},
        "set": {"local-preference": 100}
      }
    ]
  }
]
```

Valid `family` values: `prefix_list`, `community_list`, `as_path`, `route_map`.

Response: full route-policy intent state for the device (same shape as GET, but
only `accepted_at` timestamps for modified objects).

The `route-policy-reconciler` NSO service applies the intent objects to device
configuration.  If `auto_apply` is `true` on device settings and at least one
eligible object is present, an apply job is enqueued automatically.

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
    {
      "process_id": 1,
      "router_id": "10.0.0.1",
      "vrf": "",
      "areas": [{"area_id": "0.0.0.0"}]
    }
  ],
  "interfaces": [
    {
      "interface_name": "GigabitEthernet0/0",
      "process_id": 1,
      "area_id": "0.0.0.0",
      "passive": false,
      "priority": null,
      "cost": null,
      "network_type": null,
      "auth_type": null,
      "auth_present": false
    }
  ]
}
```

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

If `auto_apply` is `true` on device settings and at least one eligible intent
row is present, an apply job is enqueued automatically.
