# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""NSO reconcile-commit apply operations (Phase 2, /).

Uses NSO RESTCONF transactions with the ``reconcile`` commit option so that
the interface-reconciler service adopts pre-existing brownfield config instead
of creating conflicts.

Protocol summary:
1. PATCH a reconciler-service path (e.g. ``interface-reconciler:interface-config``)
   with the desired intent body, carrying the ``reconcile`` commit query param so NSO
   adopts pre-existing brownfield config (see ``_commit_url``). Each such PATCH is its
   own NSO transaction → its own device commit.
2. The atomic path (``NSO_ADAPTER_ATOMIC_APPLY`` → :func:`apply_combined`) instead PATCHes
   several module bodies to ``/restconf/data`` in one request → one transaction → one
   device commit. NSO RESTCONF is stateless (no cross-request transaction handle), so a
   single multi-module edit is the atomic unit.
"""

from __future__ import annotations

import json
import os

import structlog

from nso_adapter.core.isis_canon import isis_level
from nso_adapter.nso.client import NsoClient, _url_key

logger = structlog.get_logger(__name__)

# After a successful apply, re-issue the same intent as a native dry-run and
# assert NSO would push nothing further to the device. A non-empty device delta
# means the intent did not actually land (silently dropped/normalised by the
# NED, or rejected) — i.e. a false success. Toggle off with NSO_ADAPTER_VERIFY_APPLY=0.
VERIFY_AFTER_APPLY = os.environ.get("NSO_ADAPTER_VERIFY_APPLY", "1").strip().lower() not in ("0", "false", "no")

# NSO reconcile commit option — a brownfield GUARDRAIL. When a reconciler service's
# footprint overlaps config the device already carries as *non-service* config (pulled
# in by sync-from), `keep-non-service-config` tells NSO to KEEP (adopt without deleting)
# that config. Live testing on a Nokia route-target community showed `keep` is
# equivalent to NSO's implicit default here — a plain commit already adopts brownfield
# config rather than conflicting — so this does NOT change current behaviour; it makes
# the safe choice EXPLICIT and immune to a deployment whose NSO global-settings (or a
# future default) is discard. The real danger it locks out is `discard-non-service-config`,
# which actively DELETES unmodeled config under the footprint: a partial/empty intent
# (e.g. a community-list whose members didn't make it into the push) would, under discard,
# wipe the device's real members. Verified live: an empty community intent under discard
# emitted a `member delete`, under keep emitted nothing. NSO validates the value (an
# unknown one → HTTP 400 invalid-value), so it is sent verbatim. Override with
# NSO_ADAPTER_RECONCILE_COMMIT=discard-non-service-config, or ""/off/none for a plain
# commit (no reconcile param — same observed result as keep on this NSO).
_RAW_RECONCILE = os.environ.get("NSO_ADAPTER_RECONCILE_COMMIT", "keep-non-service-config").strip()
RECONCILE_COMMIT = "" if _RAW_RECONCILE.lower() in ("", "0", "off", "false", "no", "none") else _RAW_RECONCILE

# RESTCONF path to the interface-reconciler service list
_SERVICE_PATH = "/restconf/data/interface-reconciler:interface-config"

# RESTCONF datastore root. The atomic apply path (NSO_ADAPTER_ATOMIC_APPLY) PATCHes
# several reconciler-service module bodies here in ONE request → ONE NSO transaction →
# ONE device commit. NSO RESTCONF has no cross-request transaction handle
# (``tailf-netconf-transactions`` is NETCONF-session-only and is not exposed under
# ``/restconf/operations``), so a single multi-module edit *is* the atomic unit. Verified
# live on sw01: a combined subif+IP PATCH renders one ``<edit-config>`` to the device.
_DATA_PATH = "/restconf/data"


def atomic_apply_enabled() -> bool:
    """Whether to stage an apply's scopes into ONE transaction (NSO_ADAPTER_ATOMIC_APPLY).

    Off by default — the per-scope PATCH+commit path stays the fallback until the
    whole-apply atomic path (I3b) is proven. When on, ``run_apply`` stages the
    subinterface + interface-IP pair into a single :func:`apply_combined` commit so the
    subif unit and its IP land together (dissolving the greenfield ordering dependency).
    """
    return os.environ.get("NSO_ADAPTER_ATOMIC_APPLY", "0").strip().lower() in ("1", "true", "yes", "on")


def _commit_url(url: str, *, dry_run: bool = False) -> str:
    """Append NSO RESTCONF commit query params to a reconciler-service write *url*.

    Always adds ``reconcile=<RECONCILE_COMMIT>`` (when configured) so every service
    write adopts pre-existing brownfield device config instead of conflicting with it.
    ``dry_run=True`` also adds ``dry-run=native`` (compute the southbound delta, commit
    nothing). The two combine — NSO accepts ``?dry-run=native&reconcile=...`` and a
    dry-run then previews exactly what the reconcile commit would do.
    """
    params: list[str] = []
    if dry_run:
        params.append("dry-run=native")
    if RECONCILE_COMMIT:
        params.append(f"reconcile={RECONCILE_COMMIT}")
    if not params:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{'&'.join(params)}"


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
    # A device with no delta is omitted from NSO's native result, so our device being
    # absent from a non-empty list normally means "no delta" (""). Log it though: if the
    # names simply MISMATCH (NSO normalised the device name), our delta is hiding under a
    # different key and this "" is a false "verified" — the log makes that observable.
    logger.debug(
        "nso.apply.dry_run_device_absent",
        device=device_name,
        present=[e.get("name") for e in devices if isinstance(e, dict)],
    )
    return ""


async def native_dry_run(
    client: NsoClient, url: str, payload: str, device_name: str, *, method: str = "patch", strict: bool = False
) -> str | None:
    """Issue *payload* to *url* as a native dry-run (``?dry-run=native``) and return the delta.

    No commit happens — NSO computes the native device config the intent *would* push and
    returns it. The returned string is the device-native delta (``""`` = no change), or
    ``None`` when the dry-run was inconclusive (non-2xx / transport / unparseable / wrong
    shape). Same machinery as the post-apply verify guard, surfaced for the pre-apply preview.

    ``strict=True`` (the post-apply verify path): a CONCLUSIVE ``4xx`` rejection raises
    :class:`NsoApplyError` (carrying the RESTCONF error body) rather than being swallowed as
    an inconclusive ``None`` — a 4xx means NSO/the device would reject the intent, i.e. the
    apply did NOT land. Transport errors and ``5xx`` stay inconclusive (never block) either way.
    """
    dry_url = _commit_url(url, dry_run=True)
    try:
        async with client._client(timeout=client._action_timeout) as c:
            resp = await getattr(c, method)(
                dry_url,
                content=payload,
                headers={"Content-Type": "application/yang-data+json"},
            )
    except Exception:  # network/transport — inconclusive, never block
        return None
    if resp.status_code not in (200, 201, 204):
        # Surface the device error rather than silently discarding it as "inconclusive".
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text}
        logger.warning("nso.apply.dry_run_non_2xx", device=device_name, status=resp.status_code, body=err)
        if strict and 400 <= resp.status_code < 500:
            raise NsoApplyError(
                "dry_run_rejected",
                f"dry-run for {device_name!r} rejected with status {resp.status_code}",
                detail={"nso_error": err},
            )
        return None
    try:
        body = resp.json()
    except Exception:  # unparseable body — inconclusive
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

    # strict=True: a conclusive 4xx on the re-dry-run raises (the apply did not land),
    # rather than being swallowed as an inconclusive false success.
    delta = await native_dry_run(client, url, payload, device_name, method=method, strict=True)
    if delta is None:
        logger.warning("nso.apply.verify_inconclusive_or_unexpected", scope=scope, device=device_name)
        return
    if delta.strip():
        logger.error("nso.apply.verify_mismatch", scope=scope, device=device_name, delta=delta)
        raise NsoApplyError(
            "verify_mismatch",
            f"{scope}: applied intent did not land on {device_name!r} — NSO would still push changes to the device",
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
    stage: dict[str, list] | None = None,
    instance_key: str | None = None,
) -> str | None:
    """Send a reconciler service instance body to NSO — the shared apply/removal tail.

    ``replace=False`` (apply/add/update): merge-PATCH the service list path, then run
    the native dry-run verify guard. ``replace=True`` (removal): PUT-replace the keyed
    instance (``<service_path>=<device>``) so omitted list entries are dropped and
    FASTMAP reverts them — merge-PATCH never drops, and a node-level DELETE 404s on
    empty-string list keys. On replace, *body* must be the FULL desired state.

    ``stage`` (atomic apply, I3b): when given, capture this scope's instance body into
    ``stage[root_key]`` and return WITHOUT any HTTP — the caller commits every staged
    scope in one :func:`apply_combined` transaction. Mutually exclusive with the commit
    here; ignores ``replace``/``dry_run`` (atomic apply is merge-PATCH only).
    """
    if stage is not None:
        stage[root_key] = [body]
        return None
    payload = json.dumps({root_key: [body]})
    if replace:
        # instance_key overrides the single-key default for a compound-key list (e.g.
        # interface-config is keyed by "device interface-name" → "<device>,<iface>").
        url = f"{client._base}{service_path}={instance_key or _url_key(device_name)}"
        method = "put"
    else:
        url = f"{client._base}{service_path}"
        method = "patch"
    if dry_run:
        # Preview: compute the native device delta without committing anything.
        return await native_dry_run(client, url, payload, device_name, method=method)
    async with client._client(timeout=client._action_timeout) as c:
        resp = await getattr(c, method)(
            _commit_url(url), content=payload, headers={"Content-Type": "application/yang-data+json"}
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
    return None


async def apply_combined(
    client: NsoClient,
    device_name: str,
    modules: dict[str, list[dict]],
    *,
    dry_run: bool = False,
    strict: bool = False,
) -> str | None:
    """Stage several reconciler-service bodies into ONE ``/restconf/data`` PATCH.

    *modules* maps a module-qualified service root key (e.g.
    ``"subinterface-reconciler:subif-config"``) to its list of service-instance bodies.
    Empty lists are dropped. The single PATCH commits as ONE NSO transaction → ONE device
    commit, so FASTMAP resolves intra-transaction dependencies (a subif unit and its IP
    land together). Carries the ``reconcile`` commit param like every other write.

    ``dry_run=True`` returns the native device delta the combined commit would push (no
    commit). Otherwise commits, runs the post-apply verify guard, and returns None.
    Raises NsoApplyError on a non-2xx commit. ``strict`` (dry-run only) makes a conclusive
    4xx dry-run rejection raise instead of returning None — used by atomic-failure
    localisation to tell a real NED rejection apart from a transient/inconclusive blip.
    """
    body = {root: bodies for root, bodies in modules.items() if bodies}
    payload = json.dumps(body)
    url = f"{client._base}{_DATA_PATH}"

    if dry_run:
        return await native_dry_run(client, url, payload, device_name, method="patch", strict=strict)

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(_commit_url(url), content=payload, headers={"Content-Type": "application/yang-data+json"})
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.combined_failed",
                device=device_name,
                modules=list(body),
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_patch_failed",
                f"NSO combined PATCH failed with status {resp.status_code}",
                detail={"nso_error": err},
            )
    logger.info("nso.apply.combined_sent", device=device_name, modules=list(body))
    await _verify_native_or_raise(client, url, payload, device_name, scope="combined", method="patch")
    return None


# Recognised boolean spellings for the `enabled` interface attribute. The intent value
# is stored as a string whose case varies by source ("true" from a JSON boolean push,
# "True" from str(bool)) — those are the ONLY values the system produces. Anything else
# is a corrupt value that must NOT be coerced to a silent shutdown.
_ENABLED_TRUE = frozenset({"true"})
_ENABLED_FALSE = frozenset({"false"})


def _coerce_enabled_intent(value) -> bool:
    """Parse an `enabled` intent value to a bool, raising on an unrecognised token.

    Never silently coerces garbage to ``False`` (which would shut the interface).
    Shared by the per-scope apply and the atomic combined path so both agree.
    """
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in _ENABLED_TRUE:
        return True
    if token in _ENABLED_FALSE:
        return False
    raise NsoApplyError(
        "invalid_enabled_value",
        f"'enabled' intent value {value!r} is not a recognised boolean",
    )


async def apply_interface_attribute(
    client: NsoClient,
    device_name: str,
    interface_name: str,
    attribute: str,
    value: str | None,
    *,
    dry_run: bool = False,
) -> str | None:
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
        entry["enabled"] = _coerce_enabled_intent(value)
    else:
        raise NsoApplyError(
            "unsupported_attribute",
            f"Attribute '{attribute}' is not supported by interface-reconciler",
        )

    url = f"{client._base}{_SERVICE_PATH}"
    payload = json.dumps(
        {"interface-reconciler:interface-config": service_body["interface-reconciler:interface-config"]}
    )

    if dry_run:
        return await native_dry_run(client, url, payload, device_name, method="patch")

    async with client._client(timeout=client._action_timeout) as c:
        # Use PATCH to create-or-update the service instance (reconcile commit).
        resp = await c.patch(
            _commit_url(url),
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
    return None


def build_interface_ip_entry(
    device_name: str,
    interface_name: str,
    ip_intent_rows: list,
    *,
    kind: str | None = None,
    service: str | None = None,
    parent_binding: str | None = None,
    encap_tag: str | None = None,
) -> dict:
    """Shape one interface-reconciler service-instance body from a device's IP rows.

    Shared by :func:`apply_interface_ips` (per-scope path) and the atomic combined path,
    so both emit byte-identical IP bodies. ``kind``/``service``/``parent_binding``/
    ``encap_tag`` carry the Nokia routed-interface context (ignored by IOS/Junos).
    """
    entry: dict = {"device": device_name, "interface-name": interface_name}

    # VRF is an interface-level concept; take the first non-empty VRF value.
    vrf = next((r.vrf for r in ip_intent_rows if r.vrf), None)
    if vrf:
        entry["vrf"] = vrf

    # Nokia routed-interface context (apply): route the IP to the router/service
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
        # Validate ip/prefix up front: a malformed address would otherwise raise a bare
        # ValueError that, on the atomic path, aborts the WHOLE combined commit with an
        # opaque error. Surface a descriptive NsoApplyError naming the interface instead.
        if "/" not in (row.address or ""):
            raise NsoApplyError(
                "invalid_ip_address",
                f"{interface_name}: IP intent address {row.address!r} is not in ip/prefix-length form",
            )
        addr, plen_str = row.address.rsplit("/", 1)
        try:
            prefix_len = int(plen_str)
        except ValueError as exc:
            raise NsoApplyError(
                "invalid_ip_address",
                f"{interface_name}: IP intent address {row.address!r} has a non-numeric prefix length",
            ) from exc
        if row.family == "ipv4":
            # A None `secondary` must serialize as JSON false, never null (a boolean YANG
            # leaf rejects null and would 400 the whole interface's IP apply).
            ipv4_entries.append({"address": addr, "prefix-length": prefix_len, "secondary": bool(row.secondary)})
        elif row.family == "ipv6":
            ipv6_entries.append({"address": addr, "prefix-length": prefix_len})
        else:
            # Never silently drop an address whose family we don't recognise — it would be
            # stamped in_sync while never emitted. Fail loud so the bad row is fixed.
            raise NsoApplyError(
                "unsupported_ip_family",
                f"{interface_name}: unsupported IP family {row.family!r} for {row.address}",
            )

    if ipv4_entries:
        entry["ipv4-address"] = ipv4_entries
    if ipv6_entries:
        entry["ipv6-address"] = ipv6_entries
    return entry


def build_interface_config_entry(
    device_name: str,
    interface_name: str,
    attr_intent_rows,
    ip_intent_rows,
    *,
    kind: str | None = None,
    service: str | None = None,
    parent_binding: str | None = None,
    encap_tag: str | None = None,
) -> dict:
    """Build ONE interface-reconciler entry merging attribute + IP intent for an interface.

    Used by the removal path to PUT-replace a single ``(device, interface-name)`` instance
    with its full remaining desired state.
    """
    entry = build_interface_ip_entry(
        device_name,
        interface_name,
        list(ip_intent_rows),
        kind=kind,
        service=service,
        parent_binding=parent_binding,
        encap_tag=encap_tag,
    )
    for row in attr_intent_rows:
        if row.attribute == "description":
            entry["description"] = row.intent_value if row.intent_value is not None else ""
        elif row.attribute == "enabled":
            entry["enabled"] = _coerce_enabled_intent(row.intent_value)
    return entry


async def replace_interface_config(
    client: NsoClient, device_name: str, interface_name: str, entry: dict, *, dry_run: bool = False
) -> str | None:
    """PUT-replace the ``(device, interface-name)`` interface-reconciler instance with *entry*.

    FASTMAP reverts any address/attribute the previous deploy created that is no longer in
    the full desired state — how a removed IP is propagated to the device (merge-PATCH can
    never drop a list entry). Raises NsoApplyError on failure.
    """
    instance_key = f"{_url_key(device_name)},{_url_key(interface_name)}"
    return await _send_service_config(
        client,
        _SERVICE_PATH,
        "interface-reconciler:interface-config",
        device_name,
        entry,
        scope="interface_config",
        replace=True,
        dry_run=dry_run,
        instance_key=instance_key,
    )


async def delete_interface_config(client: NsoClient, device_name: str, interface_name: str) -> None:
    """DELETE the ``(device, interface-name)`` instance → FASTMAP reverts all config it created.

    Used when an interface has NO remaining accepted attr/IP intent (the operator wants
    nothing managed there). A 404 is treated as success (already gone — idempotent).
    """
    instance_key = f"{_url_key(device_name)},{_url_key(interface_name)}"
    url = f"{client._base}{_SERVICE_PATH}={instance_key}"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.delete(_commit_url(url))
        if resp.status_code not in (200, 204, 404):
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            logger.error(
                "nso.apply.interface_config_delete_failed",
                device=device_name,
                interface=interface_name,
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_delete_failed",
                f"NSO DELETE for interface_config {device_name}/{interface_name} failed with status {resp.status_code}",
                detail={"nso_error": err},
            )
    logger.info("nso.apply.interface_config_deleted", device=device_name, interface=interface_name)


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

    ``kind``/``service``/``parent_binding``/``encap_tag`` carry the Nokia
    routed-interface context so the reconciler writes the IP to the SR OS
    ``configure router Base`` / ``configure service {ies,vprn} <service>``
    interface (bound to its port) instead of to the port.  They are ignored by
    the IOS/Junos handlers.

    Raises NsoApplyError on failure.
    """
    entry = build_interface_ip_entry(
        device_name,
        interface_name,
        ip_intent_rows,
        kind=kind,
        service=service,
        parent_binding=parent_binding,
        encap_tag=encap_tag,
    )

    url = f"{client._base}{_SERVICE_PATH}"
    payload = json.dumps({"interface-reconciler:interface-config": [entry]})

    if dry_run:
        return await native_dry_run(client, url, payload, device_name, method="patch")

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(
            _commit_url(url),
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
        ipv4_count=len(entry.get("ipv4-address", [])),
        ipv6_count=len(entry.get("ipv6-address", [])),
    )

    await _verify_native_or_raise(client, url, payload, device_name, scope="interface_ip")
    return None


_SNMP_SERVICE_PATH = "/restconf/data/snmp-reconciler:snmp-config"

# RESTCONF path to the static-route-reconciler service list
_STATIC_ROUTE_SERVICE_PATH = "/restconf/data/static-route-reconciler:static-route-config"

# RESTCONF path to the logging-reconciler service list (remote syslog write path)
_LOGGING_SERVICE_PATH = "/restconf/data/logging-reconciler:logging-config"

# RESTCONF path to the svi-reconciler service list (SVI/IRB write path)
_SVI_SERVICE_PATH = "/restconf/data/svi-reconciler:svi-config"

# RESTCONF path to the subinterface-reconciler service list (dot1q write path)
_SUBIF_SERVICE_PATH = "/restconf/data/subinterface-reconciler:subif-config"

# RESTCONF path to the vlan-reconciler service list (VLAN-database write path)
_VLAN_SERVICE_PATH = "/restconf/data/vlan-reconciler:vlan-config"

# RESTCONF path to the bfd-reconciler service list (per-interface BFD write path)
_BFD_SERVICE_PATH = "/restconf/data/bfd-reconciler:bfd-config"

# RESTCONF path to the mtu-reconciler service list (per-interface MTU write path, Phase 2b)
_MTU_SERVICE_PATH = "/restconf/data/mtu-reconciler:mtu-config"

# RESTCONF path to the l2-sap-reconciler service list
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
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_static_routes(
    client: NsoClient,
    device_name: str,
    route_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_logging_config(
    client: NsoClient,
    device_name: str,
    host_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_svi_config(
    client: NsoClient,
    device_name: str,
    svi_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write the SVI/IRB intent snapshot for a device to NSO.

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
        dry_run=dry_run,
        stage=stage,
    )


def build_subif_interfaces(subif_intent_rows: list) -> list[dict]:
    """Shape the subinterface-reconciler ``interface`` list from a device's subif rows.

    Shared by :func:`apply_subinterface_config` (per-scope path) and the atomic combined
    path so both emit byte-identical subif bodies.
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
    return interfaces


async def apply_subinterface_config(
    client: NsoClient,
    device_name: str,
    subif_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write the dot1q subinterface intent snapshot for a device to NSO.

    Materialises <parent>.<unit> (encapsulation dot1Q + vrf forwarding) / Junos
    unit vlan-id via the subinterface-reconciler; IPs ride the interface-reconciler.
    Reconcile mode (brownfield adoption). ``replace=True`` PUT-replaces the keyed
    instance so removed subinterfaces are reverted.
    """
    return await _send_service_config(
        client,
        _SUBIF_SERVICE_PATH,
        "subinterface-reconciler:subif-config",
        device_name,
        {"device": device_name, "interface": build_subif_interfaces(subif_intent_rows)},
        scope="subinterface",
        replace=replace,
        dry_run=dry_run,
        stage=stage,
    )


async def apply_vlan_config(
    client: NsoClient,
    device_name: str,
    vlan_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write the VLAN-database intent snapshot for a device to NSO (write path).

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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_bfd_config(
    client: NsoClient,
    device_name: str,
    bfd_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_mtu_config(
    client: NsoClient,
    device_name: str,
    mtu_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write the per-interface MTU intent snapshot for a device to NSO (Phase 2b).

    Materialises native L2 mtu / ip-mtu / mpls-mtu via the mtu-reconciler (IOS
    interface mtu + subif ip mtu; IOS-XR interface mtu + ipv4 mtu; Junos physical
    mtu + unit family mtu; Nokia port ethernet mtu + router interface ip-mtu).
    Reconcile mode. ``replace=True`` PUT-replaces the keyed instance so removed
    MTU interfaces are reverted.
    """
    interfaces = []
    for row in mtu_intent_rows:
        entry: dict = {"interface-name": row.interface_name}
        if row.mtu is not None:
            entry["mtu"] = row.mtu
        if row.ip_mtu is not None:
            entry["ip-mtu"] = row.ip_mtu
        if row.mpls_mtu is not None:
            entry["mpls-mtu"] = row.mpls_mtu
        interfaces.append(entry)

    return await _send_service_config(
        client,
        _MTU_SERVICE_PATH,
        "mtu-reconciler:mtu-config",
        device_name,
        {"device": device_name, "interface": interfaces},
        scope="interface_mtu",
        replace=replace,
        dry_run=dry_run,
        stage=stage,
    )


async def apply_l2_saps(
    client: NsoClient,
    device_name: str,
    sap_intent_rows: list,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write Nokia L2 SAP intent for a single device to NSO.

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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_lag_config(
    client: NsoClient,
    device_name: str,
    bundles: list[dict],
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write LACP/LAG bundle intent for a single device to NSO.

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
        dry_run=dry_run,
        stage=stage,
    )


async def apply_switchport_config(
    client: NsoClient,
    device_name: str,
    interfaces: list[dict],
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write L2 switchport intent for a single device to NSO.

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
        dry_run=dry_run,
        stage=stage,
    )


def _redistribute_entry(row) -> dict:
    """One ``redistribute`` entry; route-map/metric/metric-type emitted only when set.

    Shared by the IS-IS and OSPF process payloads — both group ``RedistributionIntent``
    rows by their destination process and nest this identical entry shape.
    """
    entry: dict = {"source-protocol": row.source_protocol, "source-ref": row.source_ref}
    if row.route_map:
        entry["route-map"] = row.route_map
    if row.metric is not None:
        entry["metric"] = row.metric
    if row.metric_type:
        entry["metric-type"] = row.metric_type
    return entry


def _isis_process_entry(row, proc_redist: list[dict]) -> dict:
    """One ``process-config`` entry from a process row plus its nested redistribute list."""
    entry: dict = {"process-tag": row.process_tag or ""}
    if row.net is not None:
        entry["net"] = row.net
    # Enum leaves reject the empty string — omit when blank (not just None).
    if row.is_type:
        entry["is-type"] = isis_level(row.is_type)
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
    if proc_redist:
        entry["redistribute"] = proc_redist
    return entry


def _isis_flex_algo_entry(row) -> dict:
    """One ``flex-algo`` definition entry; optional metric/priority/admin-groups when set."""
    entry: dict = {"algo-id": int(row.algo_id)}
    if row.metric_type:
        entry["metric-type"] = row.metric_type
    if row.priority is not None:
        entry["priority"] = int(row.priority)
    if row.admin_group_exclude:
        entry["admin-group-exclude"] = row.admin_group_exclude
    if row.admin_group_include_any:
        entry["admin-group-include-any"] = row.admin_group_include_any
    if row.admin_group_include_all:
        entry["admin-group-include-all"] = row.admin_group_include_all
    return entry


def build_isis_process_payload(
    isis_process_rows: list | None,
    redistribution_rows: list | None = None,
    flex_algo_rows: list | None = None,
) -> list[dict]:
    """Build the isis-reconciler ``process-config`` payload from store rows.

    Includes nested redistribute and flex-algo. Shared by the apply path and the
    flex-algo removal path (PUT-replace), so both produce identical process-config bodies.
    """
    redist_by_proc: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        redist_by_proc.setdefault(row.dest_ref, []).append(_redistribute_entry(row))

    processes: list[dict] = [
        _isis_process_entry(row, redist_by_proc.get(row.process_tag or "", [])) for row in isis_process_rows or []
    ]
    proc_by_tag = {p["process-tag"]: p for p in processes}

    # Redistribute rows whose destination process has NO process row in this apply
    # (e.g. the process already applied cleanly and was filtered out by force=False
    # eligibility) must still land — synthesize a minimal process entry so the
    # redistribute is never silently dropped (parity with the flex-algo orphan below).
    for tag, redist_list in redist_by_proc.items():
        if tag not in proc_by_tag:
            proc = {"process-tag": tag, "redistribute": redist_list}
            processes.append(proc)
            proc_by_tag[tag] = proc

    # Attach Flex-Algo definitions to their process-config entry, creating a
    # minimal entry for any process-tag that has flex-algo but no process row.
    flex_by_proc: dict[str, list[dict]] = {}
    for row in flex_algo_rows or []:
        flex_by_proc.setdefault(row.process_tag or "", []).append(_isis_flex_algo_entry(row))

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
    *,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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

    if stage is not None:
        stage["isis-reconciler:isis-config"] = [service_body]
        return None

    payload = json.dumps({"isis-reconciler:isis-config": [service_body]})

    url = f"{client._base}{_ISIS_SERVICE_PATH}"

    if dry_run:
        return await native_dry_run(client, url, payload, device_name, method="patch")

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.patch(
            _commit_url(url),
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
    return None


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
            entry["circuit-type"] = isis_level(row.circuit_type)
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
    The PUT carries the ``reconcile`` commit option (see ``_commit_url``) so the replace
    still adopts brownfield config rather than conflicting with it.
    """
    url = f"{client._base}{service_path}={_url_key(device_name)}"
    payload = json.dumps({root_key: [body]})
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.put(_commit_url(url), content=payload, headers={"Content-Type": "application/yang-data+json"})
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
    await replace_service_instance(client, _ISIS_SERVICE_PATH, "isis-reconciler:isis-config", device_name, body)


def _parse_asn(asn) -> int:
    """Return the uint32 AS number for *asn*, accepting plain decimal or asdot ``X.Y``.

    ``X.Y`` (4-byte asdot) → ``X * 65536 + Y``. Raises a descriptive NsoApplyError on an
    unparseable value rather than a bare ``int()`` ValueError that would abort the whole
    (possibly atomic) BGP apply with an opaque internal error.
    """
    s = str(asn).strip()
    try:
        if "." in s:
            hi, lo = s.split(".", 1)
            return int(hi) * 65536 + int(lo)
        return int(s)
    except ValueError as exc:
        raise NsoApplyError("invalid_asn", f"BGP ASN {asn!r} is not a valid AS number") from exc


def _bgp_redistribute_entry(row) -> dict:
    """One BGP AF ``redistribute`` entry; route-map/metric emitted only when set."""
    entry: dict = {"source-protocol": row.source_protocol, "source-ref": row.source_ref}
    if row.route_map:
        entry["route-map"] = row.route_map
    if row.metric is not None:
        entry["metric"] = row.metric
    return entry


def _bgp_peer_entry(peer) -> dict:
    """One BGP ``peer`` entry incl. its peer-address-family list; optionals when set."""
    entry: dict = {"peer-address": peer.peer_address, "enabled": peer.enabled}
    for attr, key in (
        ("peer_group", "peer-group"),
        ("remote_as", "remote-as"),
        ("local_as", "local-as"),
        ("ttl", "ttl"),
        ("password", "password"),
        ("source", "source"),
    ):
        val = getattr(peer, attr)
        if val is not None:
            entry[key] = val
    entry["peer-address-family"] = [
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
    return entry


def _attach_orphan_bgp_redistribute(routers, redist_by_af, router_by_asn, scope_by_key, af_seen) -> None:
    """Synthesize router→scope→AF skeletons for orphan redistribute rows.

    A redistribute row whose parent router/scope/AF is absent from this apply (parent
    applied cleanly → filtered out by force=False eligibility) must still land, so build
    the minimal skeleton carrying just the redistribute rather than dropping it silently.
    Mutates *routers* and the index dicts in place.
    """
    for dest_ref, redist_list in redist_by_af.items():
        parts = dest_ref.split(":", 2)
        if len(parts) != 3:
            continue
        asn_str, vrf, af = parts
        if (asn_str, vrf, af) in af_seen:
            continue
        router_dict = router_by_asn.get(asn_str)
        if router_dict is None:
            router_dict = {"asn": _parse_asn(asn_str), "scope": []}
            routers.append(router_dict)
            router_by_asn[asn_str] = router_dict
        scope_dict = scope_by_key.get((asn_str, vrf))
        if scope_dict is None:
            scope_dict = {"vrf": vrf, "address-family": [], "peer": []}
            router_dict["scope"].append(scope_dict)
            scope_by_key[(asn_str, vrf)] = scope_dict
        scope_dict["address-family"].append({"afi": af, "redistribute": redist_list})
        af_seen.add((asn_str, vrf, af))


async def apply_bgp_config(
    client: NsoClient,
    device_name: str,
    router_intent_rows: list,
    redistribution_rows: list | None = None,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
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
        redist_by_af.setdefault(row.dest_ref, []).append(_bgp_redistribute_entry(row))

    routers: list[dict] = []
    # Indexes for orphan-redistribute merge (below): keyed by the string ASN / (asn,vrf)
    # forms that appear in a redistribution dest_ref.
    router_by_asn: dict[str, dict] = {}
    scope_by_key: dict[tuple[str, str], dict] = {}
    af_seen: set[tuple[str, str, str]] = set()
    for r in router_intent_rows:
        asn_str = str(r.asn)
        scopes_out = []
        for scope in r.scopes:
            afs_out = []
            for af in scope.address_families:
                af_entry: dict = {"afi": af.af}
                af_redist = redist_by_af.get(f"{asn_str}:{scope.vrf}:{af.af}", [])
                if af_redist:
                    af_entry["redistribute"] = af_redist
                afs_out.append(af_entry)
                af_seen.add((asn_str, scope.vrf, af.af))
            scope_dict = {
                "vrf": scope.vrf,
                "address-family": afs_out,
                "peer": [_bgp_peer_entry(peer) for peer in scope.peers],
            }
            scopes_out.append(scope_dict)
            scope_by_key[(asn_str, scope.vrf)] = scope_dict
        router_dict = {"asn": _parse_asn(r.asn), "scope": scopes_out}
        routers.append(router_dict)
        router_by_asn[asn_str] = router_dict

    _attach_orphan_bgp_redistribute(routers, redist_by_af, router_by_asn, scope_by_key, af_seen)

    return await _send_service_config(
        client,
        _BGP_SERVICE_PATH,
        "bgp-reconciler:bgp-config",
        device_name,
        {"device": device_name, "router": routers},
        scope="bgp",
        replace=replace,
        dry_run=dry_run,
        stage=stage,
    )


# Route-map intent entry keys → route-policy-reconciler YANG leaf names. The plugin
# pushes YANG-shaped keys; legacy intents carried snake_case / "match"+"set" dict
# blobs — normalise both so a stale row can't 400 the RESTCONF call.
_RM_ENTRY_KEY_MAP = {
    "sequence": "sequence",
    "action": "action",
    "match-prefix-lists": "match-prefix-lists",
    "match_prefix_lists": "match-prefix-lists",
    "match-community-lists": "match-community-lists",
    "match_community_lists": "match-community-lists",
    "match-as-paths": "match-as-paths",
    "match_as_paths": "match-as-paths",
    "match-json": "match-json",
    "match_json": "match-json",
    "match": "match-json",
    "set-json": "set-json",
    "set_json": "set-json",
    "set": "set-json",
}


def _normalize_route_map_entry(entry: dict) -> dict:
    """Map a stored route-map intent entry onto the reconciler's YANG leaf names.

    Unmapped keys are dropped (all config-bearing route-map content arrives via the
    mapped refs or the match-json/set-json blobs, so an unmapped top-level key is benign
    metadata that would otherwise 400 the RESTCONF call) — but logged, so a genuinely-new
    reconciler leaf missing from the map is visible rather than silently swallowed.

    Several source spellings can map to the same YANG leaf (``match`` / ``match_json`` /
    ``match-json`` → ``match-json``). If two carry DIFFERENT values, the canonical YANG
    spelling wins deterministically rather than letting dict iteration order decide.
    """
    out: dict = {}
    source_of: dict[str, str] = {}
    for key, value in entry.items():
        yang_key = _RM_ENTRY_KEY_MAP.get(key)
        if yang_key is None:
            logger.warning("nso.apply.route_map_entry.dropped_key", key=key)
            continue
        if yang_key in ("match-json", "set-json") and not isinstance(value, str):
            value = json.dumps(value or {}, sort_keys=True)
        if yang_key in out and out[yang_key] != value:
            existing_is_canonical = source_of[yang_key] == yang_key
            incoming_is_canonical = key == yang_key
            logger.warning(
                "nso.apply.route_map_entry.ambiguous_key",
                yang_key=yang_key,
                kept=source_of[yang_key] if existing_is_canonical or not incoming_is_canonical else key,
                dropped=key if existing_is_canonical or not incoming_is_canonical else source_of[yang_key],
            )
            if existing_is_canonical or not incoming_is_canonical:
                continue  # keep the canonical (or already-set) value
        out[yang_key] = value
        source_of[yang_key] = key
    return out


async def apply_route_policy_config(
    client: NsoClient,
    device_name: str,
    intent_rows: list,
    *,
    ned_id: str | None = None,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
) -> str | None:
    """Write route-policy intent for a single device to NSO via the route-policy-reconciler.

    Groups RoutePolicyObjectIntent rows by family and builds the canonical NSO service
    payload per docs/m17-route-policy-contract.md §2 (reconcile semantics).
    ``replace=True`` PUT-replaces the keyed instance so removed policy objects are
    reverted. Raises NsoApplyError on failure.

    Community members are stored canonically (Cisco/Junos form) in NetBox but each NED
    spells them differently; ``ned_id`` selects the dialect that translates them to the
    device's wire form. Members the NED cannot represent (e.g. ``color:`` on Nokia) are
    dropped from this device's push — so one bad member can't abort the whole community —
    and logged per-device on a real apply (``dry_run=False``) for the operator/auto-apply
    journal.
    """
    from collections import defaultdict

    from nso_adapter.core.community_dialect import UNREPRESENTABLE, community_dialect_for

    dialect = community_dialect_for(ned_id)

    by_family: dict[str, list] = defaultdict(list)
    for row in intent_rows:
        entries = row.entries
        if row.family == "route_map":
            entries = [_normalize_route_map_entry(e) for e in entries if isinstance(e, dict)]
        by_family[row.family].append(
            {"name": row.name, "entries": entries, "invert_match": getattr(row, "invert_match", False)}
        )

    def _community_list_entry(obj: dict) -> dict:
        """Translate this community's members to the device dialect, skipping any the NED can't hold."""
        kept: list = []
        for entry in obj["entries"]:
            wire = dialect.from_canonical(entry["community"])
            if wire is UNREPRESENTABLE:
                if not dry_run:
                    logger.warning(
                        "apply.route_policy.member_skipped",
                        device=device_name,
                        ned_id=ned_id,
                        community=obj["name"],
                        member=entry["community"],
                        reason="unrepresentable_on_ned",
                    )
                continue
            kept.append({**entry, "community": wire} if wire != entry["community"] else entry)
        return {"name": obj["name"], "invert-match": bool(obj.get("invert_match", False)), "entry": kept}

    body = {
        "device": device_name,
        "prefix-list": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("prefix_list", [])],
        "community-list": [_community_list_entry(obj) for obj in by_family.get("community_list", [])],
        "as-path": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("as_path", [])],
        "route-map": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("route_map", [])],
    }
    return await _send_service_config(
        client,
        _ROUTE_POLICY_SERVICE_PATH,
        "route-policy-reconciler:route-policy-config",
        device_name,
        body,
        scope="route_policy",
        replace=replace,
        dry_run=dry_run,
        stage=stage,
    )


# RESTCONF path to the ospf-reconciler service list
_OSPF_SERVICE_PATH = "/restconf/data/ospf-reconciler:ospf-config"


def _ospf_process_entry(row, proc_redist: list[dict]) -> dict:
    """One OSPF ``process-config`` entry from a process row plus its nested redistribute list."""
    # process-id is sent as a STRING: the ospf-reconciler YANG leaf is a string so named
    # IOS-XR processes (e.g. 'test') round-trip; int() would crash on a non-numeric id.
    entry: dict = {"process-id": str(row.process_id)}
    if row.router_id:
        entry["router-id"] = row.router_id
    if row.vrf:
        entry["vrf"] = row.vrf
    # Delete-guard: ALWAYS assert the admin-state. Omitting `enabled` lets a
    # PUT-replace (removal propagation, replace=True) rebuild the service footprint
    # without admin-state — which FASTMAP then deletes on the device, disabling OSPF
    # entirely (Nokia SR OS needs an explicit `admin-state enable`). A managed OSPF
    # instance defaults to enabled; an operator who wants it down sets enabled=False.
    entry["enabled"] = bool(row.enabled) if getattr(row, "enabled", None) is not None else True
    if proc_redist:
        entry["redistribute"] = proc_redist
    return entry


def _ospf_interface_entry(row) -> dict:
    """One OSPF ``interface-config`` entry; optional priority/cost/network-type/auth when set."""
    entry: dict = {
        "interface-name": row.interface_name,
        # string process-id — see _ospf_process_entry (named IOS-XR processes round-trip).
        "process-id": str(row.process_id),
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
    return entry


async def apply_ospf_config(
    client: NsoClient,
    device_name: str,
    process_intent_rows: list,
    interface_intent_rows: list,
    redistribution_rows: list | None = None,
    *,
    replace: bool = False,
    dry_run: bool = False,
    stage: dict[str, list] | None = None,
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
        redist_by_proc.setdefault(row.dest_ref, []).append(_redistribute_entry(row))

    processes = [_ospf_process_entry(row, redist_by_proc.get(str(row.process_id), [])) for row in process_intent_rows]
    emitted_pids = {p["process-id"] for p in processes}
    # Redistribute rows whose OSPF process has no process row in this apply (parent already
    # applied cleanly → filtered out) must still land via a minimal synthesized entry
    # (parity with IS-IS/BGP). Only process-id + redistribute so a merge-PATCH doesn't
    # touch the process's admin-state.
    for pid, redist_list in redist_by_proc.items():
        if pid not in emitted_pids:
            processes.append({"process-id": pid, "redistribute": redist_list})
            emitted_pids.add(pid)

    interfaces = [_ospf_interface_entry(row) for row in interface_intent_rows]

    service_body: dict = {"device": device_name}
    # Only send interface-config when non-empty: an explicit `interface-config: []` on a
    # keyed-list merge can be read as "replace with empty", over-deleting the device's
    # existing OSPF interfaces on a process-only apply (IS-IS omits it the same way).
    if interfaces:
        service_body["interface-config"] = interfaces
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
        stage=stage,
    )
