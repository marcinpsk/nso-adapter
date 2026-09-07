# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The aggregate ``device-intent`` write path: one document, one PUT, one commit (#1522).

Every family of a device is one container of ONE ``device-intent`` service instance, so a
deployment is a full-document PUT of that instance and removal is by omission. There is one
sender (:func:`apply_device_intent`) and one wire vocabulary (the ``encode_*`` functions at
the bottom of the module, bound to their containers by the section registry in
``core/projection.py``). The sixteen per-service senders, their ``*_SERVICE_PATH`` constants
and the multi-module ``/restconf/data`` staging path are gone with the reconcilers they wrote.

Every write carries the ``reconcile`` commit option (see :func:`_commit_url`) so the service
adopts pre-existing brownfield device config instead of conflicting with it.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any, NamedTuple, cast

import structlog

from nso_adapter.core.community_dialect import UNREPRESENTABLE, CommunityDialect, community_dialect_for
from nso_adapter.core.isis_canon import isis_level
from nso_adapter.nso.client import DEVICE_INTENT_PATH, DEVICE_INTENT_ROOT, NsoClient, _url_key
from nso_adapter.nso.nso_json import boundary_safe_dumps
from nso_adapter.secrets.refs import VaultRefError, parse_vault_ref

logger = structlog.get_logger(__name__)

# After a successful apply, re-issue the same intent as a native dry-run and
# assert NSO would push nothing further to the device. A non-empty device delta
# means the intent did not actually land (silently dropped/normalised by the
# NED, or rejected) — i.e. a false success. Toggle off with NSO_ADAPTER_VERIFY_APPLY=0.
VERIFY_AFTER_APPLY = os.environ.get("NSO_ADAPTER_VERIFY_APPLY", "1").strip().lower() not in ("0", "false", "no")

# #1396 R2 §4.4 — the explicit proof verdict a committing send returns, instead of letting a
# caller infer success from "nothing raised". None of the three post-commit signals is
# conclusive on its own: native verify is optional and fail-open, reader-compare's `unknown`/
# `error` never fail a scope, and residue is recorded rather than enforced. A consumer that
# CASes a `deployed_key` or empties a clear carrier must know WHICH of those it got.
VERIFY_CONCLUSIVE = "conclusive"  # the re-dry-run came back and NSO would push nothing further
VERIFY_INCONCLUSIVE = "inconclusive"  # transport/5xx/unparseable — native_dry_run returned None
VERIFY_DISABLED = "disabled"  # NSO_ADAPTER_VERIFY_APPLY is off; no proof was even attempted

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


def _commit_url(url: str, *, dry_run: bool | str = False, no_networking: bool = False) -> str:
    """Append NSO RESTCONF commit query params to a reconciler-service write *url*.

    Always adds ``reconcile=<RECONCILE_COMMIT>`` (when configured) so every service
    write adopts pre-existing brownfield device config instead of conflicting with it.
    ``dry_run=True`` also adds ``dry-run=native`` (compute the southbound delta, commit
    nothing); ``dry_run="cli"`` asks for the NED-uniform ``+``/``-`` tree diff instead
    (the apply-preview "diff -u" panel). ``no_networking=True`` commits to CDB only —
    the detach path (#106): drop service governance without pushing anything to the
    device, followed by a sync-from to re-align CDB with device truth. The params
    combine — NSO accepts ``?dry-run=...&reconcile=...`` and a dry-run then previews
    exactly what the reconcile commit would do.
    """
    params: list[str] = []
    if dry_run:
        params.append("dry-run=cli" if dry_run == "cli" else "dry-run=native")
    if no_networking:
        params.append("no-networking")
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


def _cli_delta_from_dry_run(body: object) -> str | None:
    """Return the ``outformat cli`` tree-diff text from a dry-run-result body.

    The cli form is NSO's NED-uniform ``+``/``-`` config-tree diff (one text for the
    whole transaction, not per-device). Empty/absent ``cli`` means "no change" ("");
    a body that is not the expected dry-run-result shape returns None (inconclusive).
    """
    if not isinstance(body, dict):
        return None
    result = body.get("dry-run-result")
    if not isinstance(result, dict):
        return None
    cli = result.get("cli")
    if cli in (None, {}):
        return ""
    if not isinstance(cli, dict):
        return None
    node = cli.get("local-node")
    if not isinstance(node, dict):
        return ""
    return node.get("data") or ""


async def native_dry_run(
    client: NsoClient,
    url: str,
    payload: str,
    device_name: str,
    *,
    method: str = "patch",
    strict: bool = False,
    outformat: str = "native",
    no_networking: bool = False,
) -> str | None:
    """Issue *payload* to *url* as a dry-run and return the delta (no commit).

    ``outformat="native"`` (default): the device-native southbound delta (CLI lines for
    cli NEDs, edit-config XML for netconf NEDs). ``outformat="cli"``: NSO's NED-uniform
    ``+``/``-`` config-tree diff — the apply-preview "diff -u" rendering.

    No commit happens — NSO computes the native device config the intent *would* push and
    returns it. The returned string is the device-native delta (``""`` = no change), or
    ``None`` when the dry-run was inconclusive (non-2xx / transport / unparseable / wrong
    shape). Same machinery as the post-apply verify guard, surfaced for the pre-apply preview.

    ``strict=True`` (the post-apply verify path): a CONCLUSIVE ``4xx`` rejection raises
    :class:`NsoApplyError` (carrying the RESTCONF error body) rather than being swallowed as
    an inconclusive ``None`` — a 4xx means NSO/the device would reject the intent, i.e. the
    apply did NOT land. Transport errors and ``5xx`` stay inconclusive (never block) either way.
    """
    dry_url = _commit_url(url, dry_run="cli" if outformat == "cli" else True, no_networking=no_networking)
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
        except ValueError:
            err = {"raw": "[redacted]"}
        err = _sanitized_nso_error(err)
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
    if outformat == "cli":
        return _cli_delta_from_dry_run(body)
    return _device_delta_from_dry_run(body, device_name)


async def _verify_native_or_raise(
    client: NsoClient, url: str, payload: str, device_name: str, *, scope: str, method: str = "patch"
) -> str:
    """Re-issue *payload* as a native dry-run; raise if NSO would still change the device.

    Catches false successes: a 2xx apply whose intent NSO silently did not fully
    apply leaves a non-empty native device delta on the immediate re-dry-run. The
    dry-run uses the same *method* as the apply (PUT for a replace, so a still-present
    removed entry is also caught).

    Fail-safe: transport errors, non-2xx, unparseable bodies or unexpected shapes
    are logged and treated as inconclusive (no raise) so verification never blocks
    an otherwise-successful apply. Compares against NSO's CDB, so out-of-band
    device drift (CDB vs physical) is out of scope here — that needs sync-from.

    Returns the verdict (:data:`VERIFY_CONCLUSIVE` / :data:`VERIFY_INCONCLUSIVE` /
    :data:`VERIFY_DISABLED`) so a caller that is about to record deletion authority can
    tell "proven" from "we did not look" — fail-open is right for the apply and wrong
    for the bookkeeping that follows it (R2 §4.4).
    """
    if not VERIFY_AFTER_APPLY:
        return VERIFY_DISABLED

    # strict=True: a conclusive 4xx on the re-dry-run raises (the apply did not land),
    # rather than being swallowed as an inconclusive false success.
    delta = await native_dry_run(client, url, payload, device_name, method=method, strict=True)
    if delta is None:
        logger.warning("nso.apply.verify_inconclusive_or_unexpected", scope=scope, device=device_name)
        return VERIFY_INCONCLUSIVE
    if delta.strip():
        delta = "[redacted]"
        logger.error("nso.apply.verify_mismatch", scope=scope, device=device_name, delta=delta)
        raise NsoApplyError(
            "verify_mismatch",
            f"{scope}: applied intent did not land on {device_name!r} — NSO would still push changes to the device",
            detail={"device_delta": delta},
        )
    logger.info("nso.apply.verify_ok", scope=scope, device=device_name)
    return VERIFY_CONCLUSIVE


def device_intent_instance(device_name: str, containers: Mapping[str, dict]) -> dict:
    """Return the ``list device-intent`` entry for *device_name* carrying *containers*.

    The entry is the whole desired state of the device: a family the mapping omits owns
    nothing, which is what makes removal an omission rather than a delete.
    """
    return {"device": device_name, **{container: body for container, body in containers.items()}}


def _diagnostic_message(value: object) -> str:
    """Keep only a recognized refusal family from opaque server text."""
    from nso_adapter.core.projection import section_registry

    if isinstance(value, str):
        match = re.search(r"device-intent:\s*refused\s*\[\s*family=([A-Za-z0-9._-]+)(?=\s|\])", value)
        if match and match.group(1) in {entry.container for entry in section_registry().values()}:
            return f"device-intent: refused [family={match.group(1)}]: [redacted]"
    return "[redacted]"


def _sanitized_nso_error(value: object) -> object:
    """Retain validated diagnostics without server text or Vault fields."""
    if isinstance(value, dict):
        return {
            key: _sanitized_nso_error(child)
            if key in {"ietf-restconf:errors", "errors", "error"}
            else _diagnostic_message(child)
            if key == "error-message"
            else "[redacted]"
            for key, child in value.items()
            if key in {"ietf-restconf:errors", "errors", "error", "error-message", "error-info", "raw"}
        }
    if isinstance(value, list):
        return [_sanitized_nso_error(child) for child in value]
    return _diagnostic_message(value)


async def apply_device_intent(
    client: NsoClient,
    device_name: str,
    containers: Mapping[str, dict],
    *,
    dry_run: bool | str = False,
    no_networking: bool = False,
    strict: bool = False,
) -> str | None:
    """PUT one device's whole ``device-intent`` instance — the ONE sender.

    *containers* maps a YANG container name under ``list device-intent`` to its encoded
    body. A PUT replaces the keyed instance, so the mapping is complete desired state and an
    absent family is a retraction; merge-PATCH could never express that (it never drops) and
    per-family PUTs would put N transactions and the ordering problem back.

    One request is one NSO transaction is one device commit, so FASTMAP resolves every
    cross-family dependency inside it (a subinterface unit and its address land together).

    ``dry_run=True`` returns the native device delta the commit would push and commits
    nothing; ``dry_run="cli"`` returns the NED-uniform tree diff instead. ``no_networking``
    commits to CDB only — the detach path (#106), which drops service governance without
    touching the device. ``strict`` (dry-run only) makes a conclusive 4xx rejection raise, so
    failure localisation can tell a real rejection from a transient blip.

    Returns the delta under ``dry_run``, else the R2 §4.4 proof verdict of the commit — ONE
    verdict, shared by every family in the document. Raises :class:`NsoApplyError` on a
    non-2xx commit.
    """
    payload = boundary_safe_dumps({DEVICE_INTENT_ROOT: [device_intent_instance(device_name, containers)]})
    url = f"{client._base}{DEVICE_INTENT_PATH}={_url_key(device_name)}"

    if dry_run:
        return await native_dry_run(
            client,
            url,
            payload,
            device_name,
            method="put",
            strict=strict,
            no_networking=no_networking,
            outformat="cli" if dry_run == "cli" else "native",
        )

    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.put(
            _commit_url(url, no_networking=no_networking),
            content=payload,
            headers={"Content-Type": "application/yang-data+json"},
        )
        if resp.status_code not in (200, 201, 204):
            try:
                err = resp.json()
            except ValueError:
                err = {"raw": "[redacted]"}
            err = _sanitized_nso_error(err)
            logger.error(
                "nso.apply.device_intent_failed",
                device=device_name,
                families=sorted(containers),
                status=resp.status_code,
                body=err,
            )
            raise NsoApplyError(
                "nso_put_failed",
                f"NSO device-intent PUT failed with status {resp.status_code}",
                detail={"nso_error": err},
            )
    logger.info("nso.apply.device_intent_sent", device=device_name, families=sorted(containers))
    return await _verify_native_or_raise(client, url, payload, device_name, scope="device-intent", method="put")


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


def _add_nokia_routed_context(
    entry: dict,
    *,
    kind: str | None,
    service: str | None,
    parent_binding: str | None,
    encap_tag: str | None,
) -> None:
    """Stamp the Nokia routed-interface context onto *entry* in place (no-op when kind is None).

    Routes a Nokia logical/loopback interface's config to the ``router``/``service`` interface
    instead of the physical port. Ignored by IOS/Junos.
    """
    if not kind:
        return
    entry["kind"] = kind
    if service:
        entry["service"] = service
    if parent_binding:
        entry["parent-binding"] = parent_binding
    if encap_tag:
        entry["encap-tag"] = encap_tag


def nokia_attr_kind(iface) -> str | None:
    """Return the Nokia writer context an interface's ATTRIBUTES need: the routed kinds plus ``lag``.

    A LAG's description and admin state belong on ``configure lag``, which no routed kind
    names: :func:`nokia_routed_kind` answers ``None`` for a lag because a lag carries no IP.
    Without this, an attribute-only entry reaches the wire with no context at all and the
    description targets a phantom ``configure port <logical-name>``.
    """
    return "lag" if getattr(iface, "kind", None) == "lag" else nokia_routed_kind(iface)


def nokia_routed_kind(iface) -> str | None:
    """Derive the SR OS router context (base|ies|vprn) for a Nokia routed interface.

    The adapter's ``DbInterface.kind`` is the interface *type* (physical/logical/loopback/
    lag); the router context comes from ``service``/``vrf``:
      * VPRN — ``service`` set and ``vrf`` == ``service`` (VPRN addrs carry vrf=service-name)
      * IES  — ``service`` set, global table (vrf empty)
      * Base — no service
    Returns None for non-routed interfaces (physical ports, LAGs) and for non-Nokia
    devices (where ``kind`` is unset) so the IP lands via the normal port/interface path.
    """
    if iface.kind not in ("logical", "loopback"):
        return None
    if iface.service:
        return "vprn" if (iface.vrf and iface.vrf == iface.service) else "ies"
    return "base"


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

    The aggregate encoder uses this body. ``kind``/``service``/``parent_binding``/
    ``encap_tag`` carry the Nokia routed-interface context (ignored by IOS/Junos).
    """
    return {
        "device": device_name,
        **build_interface_ip_body(
            interface_name,
            ip_intent_rows,
            kind=kind,
            service=service,
            parent_binding=parent_binding,
            encap_tag=encap_tag,
        ),
    }


def build_interface_ip_body(
    interface_name: str,
    ip_intent_rows: list,
    *,
    kind: str | None = None,
    service: str | None = None,
    parent_binding: str | None = None,
    encap_tag: str | None = None,
) -> dict:
    """Shape one interface's IP body — the same leaves, without the service instance key.

    The aggregate keys its interface list by name alone, so the device leaf that made the
    reconciler instance is added by the caller that still needs one.
    """
    entry: dict = {"interface-name": interface_name}

    # VRF is an interface-level concept; take the first non-empty VRF value.
    vrf = next((r.vrf for r in ip_intent_rows if r.vrf), None)
    if vrf:
        entry["vrf"] = vrf

    # Nokia routed-interface context (apply): route the IP to the router/service
    # interface, not the port. Only emitted when kind is set (Nokia L3 interfaces).
    _add_nokia_routed_context(entry, kind=kind, service=service, parent_binding=parent_binding, encap_tag=encap_tag)

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


# Plugin/adapter spellings → snmp-reconciler YANG enum values. The API constrains these
# fields to exactly these keys (api/snmp.py), so the store can never hold a spelling the
# writer cannot render — a raise here aborts the whole SNMP body, taking unrelated
# communities and v3 users down with it. A bare "2" is a legitimate v2c spelling and was
# missing, so a single such host row failed every SNMP apply on the device.
_SNMP_ACCESS = {"ro": "ro", "rw": "rw"}
_SNMP_VERSION = {"1": "v1", "v1": "v1", "2": "v2c", "2c": "v2c", "v2c": "v2c", "3": "v3", "v3": "v3"}
_SNMP_NOTIFY = {"trap": "traps", "traps": "traps", "inform": "informs", "informs": "informs"}


def _snmp_enum(value, mapping: dict[str, str], field: str, owner: str) -> str:
    """Normalize an SNMP intent enum to its YANG spelling; raise on unknown values."""
    normalized = mapping.get(str(value).strip().lower())
    if normalized is None:
        raise NsoApplyError(
            "invalid_snmp_intent",
            f"SNMP intent {owner!r}: unsupported {field} value {value!r}",
        )
    return normalized


def _snmp_vault_triple(vault_ref: str, prefix: str, owner: str) -> dict[str, str]:
    """Split a fully-qualified ``mount/path#key`` ref into the YANG triple leaves.

    The triples are mandatory for communities, so a ref that cannot be split must
    fail the apply (a silent drop would remove the element on a replace apply).
    """
    try:
        ref = parse_vault_ref(vault_ref, require_key=True)
    except VaultRefError:
        raise NsoApplyError(
            "invalid_vault_ref",
            f"SNMP intent {owner!r}: vault_ref must be a valid mount/path#key reference",
        ) from None
    return {
        f"{prefix}vault-mount": ref.mount,
        f"{prefix}vault-path": ref.path,
        f"{prefix}vault-key": cast(str, ref.key),
    }


def _static_route_required(row: object, field: str) -> Any:
    if isinstance(row, Mapping):
        return row[field]
    return getattr(row, field)


def _static_route_optional(row: object, field: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(field)
    return getattr(row, field, None)


def static_route_entry(row: object) -> dict:
    """Render ONE static-route intent row as the wire entry the reconciler expects.

    The single renderer accepts both live ORM rows and serialized projection rows. The
    body builder below, preview, promotion comparison and per-route fingerprint all
    call it, so none can drift from what is sent.
    """
    entry: dict = {
        "vrf": _static_route_required(row, "vrf"),
        "prefix": _static_route_required(row, "prefix"),
        "next-hop": _static_route_required(row, "next_hop"),
    }
    # Optional next-hop forms (IOS-XR): an egress/discard interface and an inter-VRF
    # (leaked) next-hop VRF. Absent on a plain IP next-hop.
    if interface_next_hop := _static_route_optional(row, "interface_next_hop"):
        entry["interface-next-hop"] = interface_next_hop
    if next_hop_vrf := _static_route_optional(row, "next_hop_vrf"):
        entry["next-hop-vrf"] = next_hop_vrf
    if (metric := _static_route_optional(row, "metric")) is not None:
        entry["metric"] = metric
    if permanent := _static_route_optional(row, "permanent"):
        entry["permanent"] = permanent
    if (tag := _static_route_optional(row, "tag")) is not None:
        entry["tag"] = tag
    return entry


def static_route_entry_key(entry: dict) -> tuple[str, str, str]:
    """Return the list key of a rendered or verbatim wire entry."""
    return (entry.get("vrf") or "", entry.get("prefix") or "", entry.get("next-hop") or "")


def local_levels_write_enabled() -> bool:
    """Whether to emit the logging ``local-levels`` container (NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE).

    The gate is off by default. Every outbound local-levels write passes through
    :func:`refuse_gated_local_levels` at the send boundary. Enable the gate after
    the reloaded packages accept the container.
    """
    return os.environ.get("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0").strip().lower() in ("1", "true", "yes", "on")


#: service leaf name → intent-row attribute, for the local-levels container.
_LOCAL_LEVEL_LEAVES = (
    ("console-severity", "console_severity"),
    ("monitor-severity", "monitor_severity"),
    ("module-severity", "module_severity"),
)

_LOCAL_LEVELS_GATED = (
    "accepted local-levels intent cannot be applied: the "
    "NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE gate is off (open it once the "
    "reloaded logging-reconciler is live, or un-manage the levels)"
)


def _local_levels(row) -> dict:
    """Return one local-levels row's SET severities; an absent leaf is unmanaged."""
    return {leaf: value for leaf, attr in _LOCAL_LEVEL_LEAVES if (value := getattr(row, attr))}


def build_subif_interfaces(subif_intent_rows: list) -> list[dict]:
    """Shape the subinterface-reconciler ``interface`` list from a device's subif rows.

    The aggregate encoder uses this list.
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
    # FRR (#83): fast-reroute is an enum leaf → omit when blank; microloop-avoidance
    # tri-state (None = leave brownfield alone).
    if getattr(row, "fast_reroute", None):
        entry["fast-reroute"] = row.fast_reroute
    microloop = getattr(row, "microloop_avoidance", None)
    if microloop is not None:
        entry["microloop-avoidance"] = bool(microloop)
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


def _isis_level_entry(row) -> dict:
    """One YANG ``level`` entry from an IsisLevelIntent row (None fields omitted)."""
    entry: dict = {"level": int(row.level)}
    if row.wide_metrics_only is not None:
        entry["wide-metrics-only"] = bool(row.wide_metrics_only)
    if row.labeled_preference is not None:
        entry["labeled-preference"] = int(row.labeled_preference)
    if row.disabled is not None:
        entry["disabled"] = bool(row.disabled)
    return entry


def build_isis_process_payload(
    isis_process_rows: list | None,
    redistribution_rows: list | None = None,
    flex_algo_rows: list | None = None,
    level_rows: list | None = None,
) -> list[dict]:
    """Build the isis-reconciler ``process-config`` payload from store rows.

    Includes nested redistribute, flex-algo and per-level tuning. Shared by the
    apply path and the flex-algo removal path (PUT-replace), so both produce
    identical process-config bodies.
    """
    redist_by_proc: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        redist_by_proc.setdefault(row.dest_ref, []).append(_redistribute_entry(row))

    processes: list[dict[str, Any]] = [
        _isis_process_entry(row, redist_by_proc.get(row.process_tag or "", [])) for row in isis_process_rows or []
    ]
    proc_by_tag: dict[str, dict[str, Any]] = {p["process-tag"]: p for p in processes}

    # Redistribute rows whose destination process has NO process row in this apply
    # (e.g. the process already applied cleanly and was filtered out by force=False
    # eligibility) must still land — synthesize a minimal process entry so the
    # redistribute is never silently dropped (parity with the flex-algo orphan below).
    for tag, redist_list in redist_by_proc.items():
        if tag not in proc_by_tag:
            orphan_process: dict[str, Any] = {"process-tag": tag, "redistribute": redist_list}
            processes.append(orphan_process)
            proc_by_tag[tag] = orphan_process

    # Attach Flex-Algo definitions to their process-config entry, creating a
    # minimal entry for any process-tag that has flex-algo but no process row.
    flex_by_proc: dict[str, list[dict]] = {}
    for row in flex_algo_rows or []:
        flex_by_proc.setdefault(row.process_tag or "", []).append(_isis_flex_algo_entry(row))

    for tag, fa_list in flex_by_proc.items():
        flex_process = proc_by_tag.get(tag)
        if flex_process is None:
            flex_process = {"process-tag": tag}
            processes.append(flex_process)
            proc_by_tag[tag] = flex_process
        flex_process["flex-algo"] = fa_list

    # Per-level tuning attaches identically (orphan tags get a minimal entry so a
    # level row is never silently dropped).
    levels_by_proc: dict[str, list[dict]] = {}
    for row in level_rows or []:
        levels_by_proc.setdefault(row.process_tag or "", []).append(_isis_level_entry(row))

    for tag, lvl_list in levels_by_proc.items():
        level_process = proc_by_tag.get(tag)
        if level_process is None:
            level_process = {"process-tag": tag}
            processes.append(level_process)
            proc_by_tag[tag] = level_process
        level_process["level"] = lvl_list

    return processes


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
        # bfd-enabled is tri-state: emit only when the intent asserts it (True/False);
        # None = no opinion → the reconciler leaves any brownfield BFD untouched.
        bfd_enabled = getattr(row, "bfd_enabled", None)
        if bfd_enabled is not None:
            entry["bfd-enabled"] = bool(bfd_enabled)
        # FRR (#83): same tri-state contract; frr-protection is an enum leaf →
        # emit only when non-blank.
        frr_enabled = getattr(row, "frr_enabled", None)
        if frr_enabled is not None:
            entry["frr-enabled"] = bool(frr_enabled)
        if getattr(row, "frr_protection", None):
            entry["frr-protection"] = row.frr_protection
        interfaces.append(entry)
    return interfaces


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


# ── The aggregate device-intent wire encoders (#1522 C9, memo A8) ─────────────────────
#
# One encoder per DOCUMENT SECTION: the section's stored rows plus its frozen execution
# facts in, the YANG container body out. Pure — no device row, no live intent row, no
# I/O — so the body a generation sends is a function of the document it froze and a
# retry sends the same bytes. Rows are keyed by TABLE NAME, exactly as the stored
# document keys them, and every declared table is present (empty when the section has no
# such rows), so a table rename raises here instead of silently emitting an empty list.
#
# The per-service builders above delegate to these, so one shape reaches the wire until
# the aggregate sender retires them.


class SectionExecution(NamedTuple):
    """The frozen facts an encoder may read besides its rows (#1522 memo A9).

    *ned_id* and *dialect* are the section's persistent encoding CONTEXT, frozen with the
    fragment that carries it; *proof* is the section's hydrated proof metadata, ``None``
    for a section that declares none. Nothing here is read from a live device row.
    """

    ned_id: str | None
    dialect: CommunityDialect
    proof: Any = None


#: One section's rows as an encoder reads them: table name -> the document's rows.
SectionRows = Mapping[str, list[Any]]

#: What to hand an encoder that reads no execution facts at all. Only ``route_policy``
#: (NED-conditioned) and ``interface_config`` (proof-fed) read the argument.
_CONTEXT_FREE_EXECUTION = SectionExecution(None, community_dialect_for(None))


def encode_snmp(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``snmp`` container: communities, v3 users, trap hosts, location, contact.

    Secret material never reaches the body: each Vault reference is split into the
    mount/path/key triple the service resolves at commit time.
    """
    entry: dict = {}
    community_intents = rows["snmp_community_intent"]
    v3_user_intents = rows["snmp_v3_user_intent"]
    host_intents = rows["snmp_host_intent"]
    system_info = rows["snmp_system_info_intent"]

    if community_intents:
        entry["community"] = [
            {
                "name": c.label,
                "access": _snmp_enum(c.access, _SNMP_ACCESS, "access", c.label),
                **({"acl": c.acl} if c.acl else {}),
                **_snmp_vault_triple(c.vault_ref, "", f"community {c.label}"),
            }
            for c in community_intents
        ]

    if v3_user_intents:
        entry["v3-user"] = [
            {
                "username": u.username,
                **({"group": u.group_name} if u.group_name else {}),
                **({"auth-protocol": u.auth_protocol} if u.auth_protocol else {}),
                **({"priv-protocol": u.priv_protocol} if u.priv_protocol else {}),
                **(_snmp_vault_triple(u.auth_vault_ref, "auth-", f"v3-user {u.username}") if u.auth_vault_ref else {}),
                **(_snmp_vault_triple(u.priv_vault_ref, "priv-", f"v3-user {u.username}") if u.priv_vault_ref else {}),
            }
            for u in v3_user_intents
        ]

    if host_intents:
        entry["host"] = [
            {
                "address": h.address,
                "version": _snmp_enum(h.version, _SNMP_VERSION, "version", h.address),
                "notify-type": _snmp_enum(h.notify_type, _SNMP_NOTIFY, "notify_type", h.address),
                # optional leaf: a binding-less host (ArcOS targets bind via
                # target-parameters, not the target) must omit it, not send null
                **({"community-or-user": h.community_or_user} if h.community_or_user else {}),
                **({"port": h.port} if h.port is not None else {}),
            }
            for h in host_intents
        ]

    for info in system_info:
        if info.location is not None:
            entry["location"] = info.location
        if info.contact is not None:
            entry["contact"] = info.contact
    return entry


def encode_static_route(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``static-route`` container from the document's own routes.

    Retention is NOT here: the ratified sender exception overlays the live certified
    service entries of the frozen retained key set onto this body, after encoding.
    """
    return {"route": [static_route_entry(row) for row in rows["static_route_intent"]]}


def refuse_gated_local_levels(rows: SectionRows) -> None:
    """Refuse a SEND whose logging rows carry local-levels while the deploy gate is closed.

    The refusal belongs to the send boundary, never to :func:`encode_logging`: an encoder is
    a pure function of its rows and its frozen context, so one document must encode the same
    bytes in every process. Sending a weaker host-only body instead would stamp the levels
    row in_sync with no severity landing, and a replace-mode body missing local-levels would
    FASTMAP-retract severities the device already holds (on NX that disables the destination).
    """
    if local_levels_write_enabled():
        return
    for levels_row in rows["logging_levels_intent"]:
        if levels := _local_levels(levels_row):
            raise NsoApplyError("local_levels_gated", _LOCAL_LEVELS_GATED, {"levels": levels})


def encode_logging(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``logging`` container: remote syslog hosts plus the local-levels singleton.

    The severities are emitted whenever the row sets them; whether they may be SENT is
    :func:`refuse_gated_local_levels`'s question, at the send boundary.
    """
    hosts = []
    for row in rows["logging_host_intent"]:
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

    body: dict = {"host": hosts}
    for levels_row in rows["logging_levels_intent"]:
        if levels := _local_levels(levels_row):
            body["local-levels"] = levels
    return body


def encode_svi(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``svi`` container: L3 VLAN interfaces (SVIs / IRBs)."""
    interfaces = []
    for row in rows["svi_intent"]:
        entry: dict = {"interface-name": row.interface_name, "vlan-id": row.vlan_id, "type": row.svi_type}
        if row.vrf:
            entry["vrf"] = row.vrf
        interfaces.append(entry)
    return {"interface": interfaces}


def encode_subinterface(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``subinterface`` container: dot1q L3 subinterfaces."""
    return {"interface": build_subif_interfaces(rows["subinterface_intent"])}


def encode_vlan(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``vlan`` container: the device's L2 VLAN database."""
    vlans = []
    for row in rows["vlan_intent"]:
        entry: dict = {"vlan-id": row.vlan_id}
        if row.name:
            entry["name"] = row.name
        vlans.append(entry)
    return {"vlan": vlans}


def encode_bfd(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``bfd`` container: per-interface BFD timers."""
    interfaces = []
    for row in rows["bfd_intent"]:
        entry: dict = {"interface-name": row.interface_name, "micro-bfd": bool(row.micro_bfd)}
        if row.min_tx is not None:
            entry["min-tx"] = row.min_tx
        if row.min_rx is not None:
            entry["min-rx"] = row.min_rx
        if row.multiplier is not None:
            entry["multiplier"] = row.multiplier
        interfaces.append(entry)
    return {"interface": interfaces}


def encode_interface_mtu(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``mtu`` container: per-interface L2 / IP / MPLS MTU."""
    interfaces = []
    for row in rows["interface_mtu_intent"]:
        entry: dict = {"interface-name": row.interface_name}
        if row.mtu is not None:
            entry["mtu"] = row.mtu
        if row.ip_mtu is not None:
            entry["ip-mtu"] = row.ip_mtu
        if row.mpls_mtu is not None:
            entry["mpls-mtu"] = row.mpls_mtu
        interfaces.append(entry)
    return {"interface": interfaces}


def encode_l2_sap(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``l2-sap`` container: SAPs under existing SR OS epipe/vpls services."""
    saps = []
    for row in rows["l2_sap_intent"]:
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
    return {"sap": saps}


def encode_isis(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``isis`` container: processes, per-level tuning, flex-algo and interfaces.

    An empty list is OMITTED, not sent as ``[]``: a keyed-list merge reads an explicit
    empty list as "replace with empty" and would over-delete the device's IS-IS state.
    """
    processes = build_isis_process_payload(
        rows["isis_process_intent"],
        rows["redistribution_intent"],
        rows["isis_flex_algo_intent"],
        rows["isis_level_intent"],
    )
    interfaces = build_isis_interface_payload(rows["isis_interface_intent"])
    body: dict = {}
    if interfaces:
        body["interface-config"] = interfaces
    if processes:
        body["process-config"] = processes
    return body


def encode_bgp(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``bgp`` container: the router / scope / address-family / peer tree."""
    redist_by_af: dict[str, list[dict]] = {}
    for row in rows["redistribution_intent"]:
        redist_by_af.setdefault(row.dest_ref, []).append(_bgp_redistribute_entry(row))

    routers: list[dict] = []
    router_by_asn: dict[str, dict] = {}
    scope_by_key: dict[tuple[str, str], dict] = {}
    af_seen: set[tuple[str, str, str]] = set()
    for r in rows["bgp_router_intent"]:
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
        router_dict: dict = {"asn": _parse_asn(r.asn), "scope": scopes_out}
        if r.router_id:
            router_dict["router-id"] = r.router_id  # bgp-reconciler leaf, sibling of asn
        routers.append(router_dict)
        router_by_asn[asn_str] = router_dict

    _attach_orphan_bgp_redistribute(routers, redist_by_af, router_by_asn, scope_by_key, af_seen)
    return {"router": routers}


def _community_member(entry: object, name: str) -> str:
    """Validate one stored community member for both encoding and reporting."""
    if not isinstance(entry, dict) or not isinstance(entry.get("community"), str):
        raise NsoApplyError(
            "invalid_route_policy_intent",
            f"Community list {name!r} has an entry without a string community key",
        )
    return entry["community"]


def unrenderable_route_policy_members(rows: SectionRows, dialect: CommunityDialect) -> list[tuple[str, str]]:
    """``(community-list name, member)`` for every member this dialect cannot hold.

    The encoder drops them so one unrepresentable member cannot abort a whole community;
    the caller reports them, which is why the verdict is exposed rather than logged here.
    """
    skipped: list[tuple[str, str]] = []
    for row in rows["route_policy_object_intent"]:
        if row.family != "community_list":
            continue
        for entry in row.entries:
            member = _community_member(entry, row.name)
            if dialect.from_canonical(member) is UNREPRESENTABLE:
                skipped.append((row.name, member))
    return skipped


def encode_route_policy(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``route-policy`` container: prefix-lists, community-lists, AS-paths, route-maps.

    Community members are stored canonically and spelled per NED; the FROZEN dialect
    translates them, never the device row, so an unrelated reissue after a NED change
    sends the members the authorization froze.
    """
    by_family: dict[str, list] = {}
    for row in rows["route_policy_object_intent"]:
        entries = row.entries
        if row.family == "route_map":
            entries = [_normalize_route_map_entry(e) for e in entries if isinstance(e, dict)]
        by_family.setdefault(row.family, []).append(
            {"name": row.name, "entries": entries, "invert_match": getattr(row, "invert_match", False)}
        )

    def _community_list_entry(obj: dict) -> dict:
        kept: list = []
        for entry in obj["entries"]:
            member = _community_member(entry, obj["name"])
            wire = execution.dialect.from_canonical(member)
            if wire is UNREPRESENTABLE:
                continue
            kept.append({**entry, "community": wire} if wire != member else entry)
        return {"name": obj["name"], "invert-match": bool(obj.get("invert_match", False)), "entry": kept}

    return {
        "prefix-list": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("prefix_list", [])],
        "community-list": [_community_list_entry(obj) for obj in by_family.get("community_list", [])],
        "as-path": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("as_path", [])],
        "route-map": [{"name": obj["name"], "entry": obj["entries"]} for obj in by_family.get("route_map", [])],
    }


def encode_ospf(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``ospf`` container: processes and interfaces.

    A redistribute row whose process has no row of its own still lands, through a minimal
    synthesized process entry — parity with IS-IS and BGP, so nothing is silently dropped.
    """
    redist_by_proc: dict[str, list[dict]] = {}
    for row in rows["redistribution_intent"]:
        redist_by_proc.setdefault(row.dest_ref, []).append(_redistribute_entry(row))

    processes = [
        _ospf_process_entry(row, redist_by_proc.get(str(row.process_id), [])) for row in rows["ospf_instance_intent"]
    ]
    emitted_pids = {p["process-id"] for p in processes}
    for pid, redist_list in redist_by_proc.items():
        if pid not in emitted_pids:
            processes.append({"process-id": pid, "redistribute": redist_list})
            emitted_pids.add(pid)

    interfaces = [_ospf_interface_entry(row) for row in rows["ospf_interface_intent"]]
    body: dict = {}
    if interfaces:
        body["interface-config"] = interfaces
    if processes:
        body["process-config"] = processes
    return body


def encode_switchport(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``switchport`` container: per-interface L2 mode and VLAN membership.

    ``tagged-vlan`` is sorted so two stores holding the same membership in a different
    insertion order encode the same bytes; a YANG leaf-list is order-insensitive.
    """
    interfaces = []
    for row in rows["switchport_intent"]:
        entry: dict = {"interface-name": row.interface_name}
        if row.mode:
            entry["mode"] = row.mode
        if row.untagged_vlan is not None:
            entry["untagged-vlan"] = row.untagged_vlan
        tagged = sorted(tag.vlan_id for tag in row.tagged_vlans)
        if tagged:
            entry["tagged-vlan"] = tagged
        interfaces.append(entry)
    return {"interface": interfaces}


#: LAG bundle scalar -> its YANG leaf. Every one is optional: an unset column is a leaf
#: the operator has no opinion on, and an absent leaf is what says so on the wire.
_LAG_BUNDLE_LEAVES = (
    ("lag_id", "lag-id"),
    ("min_links", "min-links"),
    ("system_priority", "system-priority"),
    ("system_id", "system-id"),
    ("timer", "timer"),
    ("admin_key", "admin-key"),
)


def encode_lag(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``lag`` container: LACP bundles and their member interfaces."""
    bundles = []
    for row in rows["lag_bundle_intent"]:
        entry: dict = {"name": row.name}
        for attribute, leaf in _LAG_BUNDLE_LEAVES:
            value = getattr(row, attribute)
            if value is not None:
                entry[leaf] = value
        members = []
        for member in row.members:
            member_entry: dict = {"interface-name": member.interface_name}
            if member.mode:
                member_entry["mode"] = member.mode
            if member.port_priority is not None:
                member_entry["port-priority"] = member.port_priority
            members.append(member_entry)
        if members:
            entry["member"] = members
        bundles.append(entry)
    return {"bundle": bundles}


#: The interface attributes this writer has a wire leaf for. Anything else is refused rather
#: than dropped: the managed scope is operator data, not a closed enum.
INTERFACE_ATTRIBUTE_LEAVES = frozenset({"description", "enabled"})


def encode_interface_config(rows: SectionRows, execution: SectionExecution) -> dict:
    """Encode the ``interface`` container: description, admin state and addresses, merged.

    Attributes and addresses ride ONE keyed interface entry, so an interface named by both
    halves must appear once. The section's proof supplies the writer context and the
    per-attribute eligibility decisions; an ineligible attribute never reaches the wire.
    """
    interfaces = execution.proof.interfaces
    eligible = execution.proof.eligible_attributes
    by_name: dict[str, dict] = {}

    def _entry(name: str) -> dict:
        return by_name.setdefault(name, {"interface-name": name})

    attributed: set[str] = set()
    for row in rows["interface_intent"]:
        if (row.interface_id, row.attribute) not in eligible:
            continue
        iface = interfaces[row.interface_id]
        attributed.add(iface.name)
        if row.attribute not in INTERFACE_ATTRIBUTE_LEAVES:
            # The managed scope is data, so the store CAN hold an attribute this writer has
            # no leaf for. Refusing is the only honest answer: emitting the entry without it
            # would stamp the row in_sync for a leaf that never reached the device (#26).
            raise NsoApplyError(
                "unsupported_attribute",
                f"interface_config: attribute {row.attribute!r} on {iface.name!r} has no wire leaf",
                detail={"interface": iface.name, "attribute": row.attribute},
            )
        entry = _entry(iface.name)
        if row.attribute == "description":
            entry["description"] = row.intent_value if row.intent_value is not None else ""
        else:
            # Strict coercion (raises on garbage), so a corrupt value never silently
            # shuts an interface down.
            entry["enabled"] = _coerce_enabled_intent(row.intent_value)

    ip_by_iface: dict[int, list] = {}
    for row in rows["interface_ip_intent"]:
        ip_by_iface.setdefault(row.interface_id, []).append(row)
    for interface_id, ip_rows in ip_by_iface.items():
        iface = interfaces[interface_id]
        routed_kind = nokia_routed_kind(iface)
        ip_entry = build_interface_ip_body(
            iface.name,
            ip_rows,
            kind=routed_kind,
            service=iface.service if routed_kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )
        entry = _entry(iface.name)
        for key, value in ip_entry.items():
            if key != "interface-name":
                entry[key] = value

    # The attribute half's context, last so a lag keeps the kind only IT can name. The two
    # halves rode separate service instances before and each stamped its own; one entry
    # carries one context, and an attribute-only entry with none writes to the wrong node.
    for interface_id, iface in interfaces.items():
        if iface.name not in attributed:
            continue
        attr_kind = nokia_attr_kind(iface)
        _add_nokia_routed_context(
            by_name[iface.name],
            kind=attr_kind,
            service=iface.service if attr_kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )

    return {"interface": list(by_name.values())}
