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
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, NamedTuple, cast

import structlog

from nso_adapter.core.community_dialect import UNREPRESENTABLE, CommunityDialect, community_dialect_for
from nso_adapter.core.isis_canon import isis_level
from nso_adapter.domain.asn import asn_row_identity, checked_asn, checked_intent_source_ref, validate_asn_rows
from nso_adapter.domain.diagnostics import device_fields
from nso_adapter.nso.client import DEVICE_INTENT_PATH, DEVICE_INTENT_ROOT, NsoClient, _url_key
from nso_adapter.nso.nso_json import boundary_safe_dumps
from nso_adapter.nso.origins import (
    EncodedSection,
    LogicalRow,
    OriginConstructionError,
    OriginContext,
    OriginRecorder,
    UnsupportedOrigin,
)
from nso_adapter.nso.refusal_protocol import SUBJECTS, row_ref
from nso_adapter.nso.refusals import (
    DefiniteRefusal,
    ErrorClassification,
    OrdinaryRejection,
    ProtocolFault,
    Unavailable,
    classify_error,
)
from nso_adapter.nso.shape import require_vlan_id
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


SenderPhase = Literal["pre_commit_dry_run", "commit", "post_commit_verification"]
ResponseCheckpoint = Callable[[SenderPhase, ErrorClassification | None, bool], Awaitable[None]]


class NsoApplyError(Exception):
    """Raised when a NSO commit fails for a specific attribute."""

    def __init__(
        self,
        code: str,
        message: str,
        detail: dict | None = None,
        *,
        classification: ErrorClassification | None = None,
        sender_phase: SenderPhase = "commit",
        commit_acknowledged: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}
        self.classification = classification
        self.sender_phase = sender_phase
        self.commit_acknowledged = commit_acknowledged


def _device_delta_from_dry_run(body: object, device_name: str, *, device_id: int) -> str | None:
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
        **device_fields(device_id=device_id),
        present_count=len(devices),
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


def _classified_apply_error(
    classification: ErrorClassification, phase: SenderPhase, *, detail: dict | None = None
) -> NsoApplyError:
    if isinstance(classification, DefiniteRefusal):
        code, message = "refused", "NSO refused the device document"
    elif isinstance(classification, ProtocolFault):
        code, message = "refusal_protocol_fault", ProtocolFault.message
    elif isinstance(classification, Unavailable):
        code, message = "nso_unavailable", "NSO delivery outcome is unavailable"
    else:
        code = "dry_run_rejected" if phase != "commit" else "nso_put_failed"
        message = "NSO rejected the device document"
    return NsoApplyError(
        code,
        message,
        detail,
        classification=classification,
        sender_phase=phase,
        commit_acknowledged=phase == "post_commit_verification",
    )


async def native_dry_run(
    client: NsoClient,
    url: str,
    payload: str,
    device_name: str,
    *,
    device_id: int,
    method: str = "patch",
    strict: bool = False,
    outformat: str = "native",
    no_networking: bool = False,
    sender_phase: SenderPhase = "pre_commit_dry_run",
    checkpoint: ResponseCheckpoint | None = None,
) -> str | None:
    """Decode a dry-run after claim-first classification, for both strict settings."""
    dry_url = _commit_url(url, dry_run="cli" if outformat == "cli" else True, no_networking=no_networking)
    response = None
    exception_type = None
    try:
        async with client._client(timeout=client._action_timeout) as c:
            response = await getattr(c, method)(
                dry_url, content=payload, headers={"Content-Type": "application/yang-data+json"}
            )
    except Exception as exc:
        response = None
        exception_type = type(exc).__name__
    classification = None
    if response is None:
        classification = classify_error(None, None)
    else:
        classification = classify_error(response.status_code, response.content)
    if classification is not None:
        logger.warning(
            "nso.apply.dry_run_non_2xx",
            **device_fields(device_id=device_id),
            status=response.status_code if response is not None else None,
            exception_type=exception_type,
        )
        if checkpoint is not None:
            await checkpoint(sender_phase, classification, False)
        if isinstance(classification, (DefiniteRefusal, ProtocolFault)) or (
            strict and isinstance(classification, OrdinaryRejection)
        ):
            detail = _ordinary_error_detail(response) if isinstance(classification, OrdinaryRejection) else None
            error = _classified_apply_error(classification, sender_phase, detail=detail)
            raise error
        return None
    body = None
    try:
        body = response.json() if response is not None else None
    except ValueError:
        pass
    delta = (
        _cli_delta_from_dry_run(body)
        if outformat == "cli"
        else _device_delta_from_dry_run(body, device_name, device_id=device_id)
    )
    if delta is None and checkpoint is not None:
        await checkpoint(sender_phase, Unavailable(), False)
    return delta


async def _verify_native_or_raise(
    client: NsoClient,
    url: str,
    payload: str,
    device_name: str,
    *,
    device_id: int,
    scope: str,
    checkpoint: ResponseCheckpoint | None = None,
    method: str = "patch",
    no_networking: bool = False,
) -> str:
    """Re-issue *payload* as a native dry-run; raise if NSO would still change the device.

    Catches false successes: a 2xx apply whose intent NSO silently did not fully
    apply leaves a non-empty native device delta on the immediate re-dry-run. The
    dry-run uses the same *method* as the apply (PUT for a replace, so a still-present
    removed entry is also caught).

    Unavailable responses leave verification inconclusive. Claimed refusals and
    protocol faults raise with an acknowledged commit and preserve that phase. Compares against NSO's CDB, so out-of-band
    device drift (CDB vs physical) is out of scope here — that needs sync-from.

    Returns the verdict (:data:`VERIFY_CONCLUSIVE` / :data:`VERIFY_INCONCLUSIVE` /
    :data:`VERIFY_DISABLED`) so a caller that is about to record deletion authority can
    tell "proven" from "we did not look" — fail-open is right for the apply and wrong
    for the bookkeeping that follows it (R2 §4.4).
    """
    if not VERIFY_AFTER_APPLY:
        if checkpoint is not None:
            await checkpoint("post_commit_verification", Unavailable(), False)
        return VERIFY_DISABLED

    # strict=True: a conclusive 4xx on the re-dry-run raises (the apply did not land),
    # rather than being swallowed as an inconclusive false success.
    # The verification re-issues the commit, so it must carry the commit's own no-networking.
    delta = await native_dry_run(
        client,
        url,
        payload,
        device_name,
        device_id=device_id,
        method=method,
        strict=True,
        no_networking=no_networking,
        sender_phase="post_commit_verification",
        checkpoint=checkpoint,
    )
    if delta is None:
        logger.warning(
            "nso.apply.verify_inconclusive_or_unexpected",
            scope=scope,
            **device_fields(device_id=device_id),
        )
        return VERIFY_INCONCLUSIVE
    if delta.strip():
        if checkpoint is not None:
            await checkpoint("post_commit_verification", Unavailable(), False)
        delta = "[redacted]"
        logger.error("nso.apply.verify_mismatch", scope=scope, **device_fields(device_id=device_id), delta=delta)
        raise NsoApplyError(
            "verify_mismatch",
            f"{scope}: applied intent did not land; NSO would still push changes to the device",
            detail={"device_delta": delta},
            classification=Unavailable(),
            sender_phase="post_commit_verification",
            commit_acknowledged=True,
        )
    if checkpoint is not None:
        await checkpoint("post_commit_verification", None, False)
    logger.info("nso.apply.verify_ok", scope=scope, **device_fields(device_id=device_id))
    return VERIFY_CONCLUSIVE


def device_intent_instance(device_name: str, containers: Mapping[str, dict]) -> dict:
    """Return the ``list device-intent`` entry for *device_name* carrying *containers*.

    The entry is the whole desired state of the device: a family the mapping omits owns
    nothing, which is what makes removal an omission rather than a delete.
    """
    return {"device": device_name, **{container: body for container, body in containers.items()}}


def _diagnostic_message(value: object) -> str:
    """Keep only allowlisted ordinary device-parser constructs."""
    from nso_adapter.core.capability import allowlisted_rejection_fragment

    if not isinstance(value, str):
        return "[redacted]"
    return allowlisted_rejection_fragment(value) or "[redacted]"


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


def _ordinary_error_detail(response) -> dict:
    body = None
    try:
        body = response.json() if response is not None else None
    except ValueError:
        pass
    return {"nso_error": _sanitized_nso_error(body)}


async def apply_device_intent(
    client: NsoClient,
    device_name: str,
    containers: Mapping[str, dict],
    *,
    device_id: int,
    dry_run: bool | str = False,
    no_networking: bool = False,
    strict: bool = False,
    checkpoint: ResponseCheckpoint | None = None,
) -> str | None:
    """Send one document, preserving classification, phase, and commit acknowledgement."""
    payload = boundary_safe_dumps({DEVICE_INTENT_ROOT: [device_intent_instance(device_name, containers)]})
    url = f"{client._base}{DEVICE_INTENT_PATH}={_url_key(device_name)}"
    if dry_run:
        return await native_dry_run(
            client,
            url,
            payload,
            device_name,
            device_id=device_id,
            method="put",
            strict=strict,
            no_networking=no_networking,
            outformat="cli" if dry_run == "cli" else "native",
            checkpoint=checkpoint,
        )
    response = None
    exception_type = None
    try:
        async with client._client(timeout=client._action_timeout) as c:
            response = await c.put(
                _commit_url(url, no_networking=no_networking),
                content=payload,
                headers={"Content-Type": "application/yang-data+json"},
            )
    except Exception as exc:
        response = None
        exception_type = type(exc).__name__
    classification = None
    if response is None:
        classification = classify_error(None, None)
    else:
        classification = classify_error(response.status_code, response.content)
    if checkpoint is not None:
        await checkpoint("commit", classification, classification is None)
    if classification is not None:
        detail = _ordinary_error_detail(response) if isinstance(classification, OrdinaryRejection) else None
        logger.error(
            "nso.apply.device_intent_failed",
            **device_fields(device_id=device_id),
            families=sorted(containers),
            status=response.status_code if response is not None else None,
            exception_type=exception_type,
        )
        error = _classified_apply_error(classification, "commit", detail=detail)
        raise error
    logger.info("nso.apply.device_intent_sent", **device_fields(device_id=device_id), families=sorted(containers))
    return await _verify_native_or_raise(
        client,
        url,
        payload,
        device_name,
        device_id=device_id,
        scope="device-intent",
        method="put",
        no_networking=no_networking,
        checkpoint=checkpoint,
    )


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
        "'enabled' intent value is not a recognised boolean",
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


def build_interface_ip_body(
    interface_name: str,
    ip_intent_rows: list,
    *,
    kind: str | None = None,
    service: str | None = None,
    parent_binding: str | None = None,
    encap_tag: str | None = None,
    origins: OriginRecorder | None = None,
) -> dict:
    """Shape one interface's IP body — the same leaves, without the service instance key.

    The aggregate keys its interface list by name alone, so the device leaf that made the
    reconciler instance is added by the caller that still needs one.
    """
    entry: dict = {"interface-name": interface_name}

    vrfs = {row.vrf for row in ip_intent_rows}
    if len(vrfs) > 1:
        raise OriginConstructionError("competing field producers")
    vrf = next(iter(vrfs), None)
    if vrf:
        entry["vrf"] = vrf

    # Nokia routed-interface context (apply): route the IP to the router/service
    # interface, not the port. Only emitted when kind is set (Nokia L3 interfaces).
    _add_nokia_routed_context(entry, kind=kind, service=service, parent_binding=parent_binding, encap_tag=encap_tag)

    ipv4_entries = []
    ipv6_entries = []
    logicals = []
    vrf_logicals = []
    for row in ip_intent_rows:
        # Validate ip/prefix before the atomic path can fail with an opaque ValueError.
        if "/" not in (row.address or ""):
            raise NsoApplyError(
                "invalid_ip_address",
                "IP intent address is not in ip/prefix-length form",
            )
        addr, plen_str = row.address.rsplit("/", 1)
        try:
            prefix_len = int(plen_str)
        except ValueError:
            prefix_len = None
        if prefix_len is None:
            raise NsoApplyError(
                "invalid_ip_address",
                "IP intent address has a non-numeric prefix length",
            )
        if origins is not None and row.family in {"ipv4", "ipv6"}:
            subject = "interface." + row.family + "-address"
            path = (("interface", (interface_name,)), (row.family + "-address", (addr,)))
            source = origins.source(
                row,
                "interface_ip_intent",
                "ip",
                writer_context=(interface_name, kind, service, parent_binding, encap_tag),
            )
            logical = origins.logical(subject, path, source, "ip")
            origins.emit(subject, path, (logical,))
            logicals.append(logical)
            if row.vrf:
                vrf_logicals.append(logical)
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
                "IP intent address has an unsupported family",
            )

    if origins is not None and logicals:
        origins.emit("interface.vrf", (("interface", (interface_name,)),), vrf_logicals or logicals)
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


def _snmp_enum(value, mapping: dict[str, str], field: str) -> str:
    """Normalize an SNMP intent enum to its YANG spelling; raise on unknown values."""
    normalized = mapping.get(str(value).strip().lower())
    if normalized is None:
        raise NsoApplyError(
            "invalid_snmp_intent",
            f"SNMP intent has an unsupported {field} value",
        )
    return normalized


def _snmp_vault_triple(vault_ref: str, prefix: str) -> dict[str, str]:
    """Split a fully-qualified ``mount/path#key`` ref into the YANG triple leaves.

    The triples are mandatory for communities, so a ref that cannot be split must
    fail the apply (a silent drop would remove the element on a replace apply).
    """
    try:
        ref = parse_vault_ref(vault_ref, require_key=True)
    except VaultRefError:
        ref = None
    # Raised OUTSIDE the except block. `from None` only sets __suppress_context__, and the
    # parser exception stays reachable as __context__ with the whole reference in its repr.
    if ref is None:
        raise NsoApplyError(
            "invalid_vault_ref",
            "SNMP intent vault_ref must be a valid mount/path#key reference",
        )
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
        if row.dot1q_vlan is None:
            raise ValueError(f"subinterface {row.interface_name} has no dot1q_vlan")
        require_vlan_id(row.dot1q_vlan, "subinterface", row.interface_name, "dot1q_vlan")
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


def _redistribute_entry(row, ned_id: str | None) -> dict:
    """One ``redistribute`` entry; route-map/metric/metric-type emitted only when set.

    Shared by the IS-IS and OSPF process payloads — both group ``RedistributionIntent``
    rows by their destination process and nest this identical entry shape.
    """
    source_ref = checked_intent_source_ref(row.source_ref, ned_id) if row.source_protocol == "bgp" else row.source_ref
    entry: dict = {"source-protocol": row.source_protocol, "source-ref": source_ref}
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
    *,
    ned_id: str | None = None,
) -> list[dict]:
    """Build the isis-reconciler ``process-config`` payload from store rows.

    Includes nested redistribute, flex-algo and per-level tuning. Shared by the
    apply path and the flex-algo removal path (PUT-replace), so both produce
    identical process-config bodies.
    """
    redist_by_proc: dict[str, list[dict]] = {}
    for row in redistribution_rows or []:
        redist_by_proc.setdefault(row.dest_ref, []).append(_redistribute_entry(row, ned_id))

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


def _bgp_redistribute_entry(row, ned_id: str | None) -> dict:
    """One BGP AF ``redistribute`` entry; route-map/metric emitted only when set."""
    source_ref = checked_intent_source_ref(row.source_ref, ned_id) if row.source_protocol == "bgp" else row.source_ref
    entry: dict = {"source-protocol": row.source_protocol, "source-ref": source_ref}
    if row.route_map:
        entry["route-map"] = row.route_map
    if row.metric is not None:
        entry["metric"] = row.metric
    return entry


class _BgpConstruct(NamedTuple):
    subject: str
    path: tuple
    contributors: list[LogicalRow]
    sources: list
    field_rows: dict | None = None
    field_sources: dict | None = None


def _bgp_nested_sources(origins: OriginRecorder, router) -> tuple:
    scopes = []
    for scope in router.scopes:
        children = [origins.source(af, "bgp_af_intent", "bgp", stampable=False) for af in scope.address_families]
        for peer in scope.peers:
            afs = tuple(
                origins.source(af, "bgp_peer_af_intent", "bgp", stampable=False) for af in peer.peer_address_families
            )
            children.append(origins.source(peer, "bgp_peer_intent", "bgp", stampable=False, children=afs))
        scopes.append(origins.source(scope, "bgp_scope_intent", "bgp", stampable=False, children=children))
    return tuple(scopes)


def _bgp_peer_entry(peer, origins: OriginRecorder, path: tuple, carrier) -> tuple[dict, LogicalRow]:
    af_sources = tuple(
        origins.source(af, "bgp_peer_af_intent", "bgp", stampable=False) for af in peer.peer_address_families
    )
    source = origins.source(peer, "bgp_peer_intent", "bgp", stampable=False, children=af_sources)
    logical = origins.logical("bgp.router.scope.peer", path, source, "bgp", carrier=carrier, children=af_sources)
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
            entry[key] = (
                str(checked_asn(val, "bgp_peer_intent", getattr(peer, "id", None), attr))
                if attr in {"remote_as", "local_as"}
                else val
            )
    afs = []
    for af, af_source in zip(peer.peer_address_families, af_sources, strict=True):
        af_entry = {
            "afi": af.af,
            "enabled": af.enabled,
            **({"routemap-in": af.routemap_in} if af.routemap_in else {}),
            **({"routemap-out": af.routemap_out} if af.routemap_out else {}),
            **({"prefixlist-in": af.prefixlist_in} if af.prefixlist_in else {}),
            **({"prefixlist-out": af.prefixlist_out} if af.prefixlist_out else {}),
        }
        origins.emit(
            "bgp.router.scope.peer.peer-address-family",
            (*path, ("peer-address-family", (af.af,))),
            (logical,),
            sources=(af_source,),
        )
        afs.append(af_entry)
    entry["peer-address-family"] = afs
    fields = {
        field: (af_sources if field == "bgp.paf.enabled" and af_sources else (source,))
        for field in SUBJECTS["bgp.router.scope.peer"].fields
    }
    origins.emit("bgp.router.scope.peer", path, (logical,), field_sources=fields)
    return entry, logical


def _bgp_scope_entry(
    scope, asn: int, origins: OriginRecorder, router: LogicalRow, constructs: dict, af_by_key: dict
) -> dict:
    path = (("router", (str(asn),)), ("scope", (scope.vrf,)))
    scope_source = origins.source(scope, "bgp_scope_intent", "bgp", stampable=False)
    scope_key = (asn, scope.vrf)
    if scope_key in constructs:
        raise OriginConstructionError("duplicate origin")
    construct = _BgpConstruct(
        "bgp.router.scope",
        path,
        [router],
        [scope_source],
        {"bgp.scope.vrf": (router,)},
        {"bgp.scope.vrf": (scope_source,)},
    )
    constructs[scope_key] = construct
    afs = []
    for af in scope.address_families:
        af_path = (*path, ("address-family", (af.af,)))
        af_source = origins.source(af, "bgp_af_intent", "bgp", stampable=False)
        af_key = (asn, scope.vrf, af.af)
        if af_key in constructs:
            raise OriginConstructionError("duplicate origin")
        constructs[af_key] = _BgpConstruct("bgp.router.scope.address-family", af_path, [router], [af_source])
        af_entry = {"afi": af.af}
        af_by_key[af_key] = af_entry
        afs.append(af_entry)
    peers = []
    for peer in scope.peers:
        entry, logical = _bgp_peer_entry(peer, origins, (*path, ("peer", (peer.peer_address,))), router.carrier)
        construct.contributors.append(logical)
        construct.sources.extend(logical.source_refs)
        constructs[(asn,)].contributors.append(logical)
        constructs[(asn,)].sources.extend(logical.source_refs)
        peers.append(entry)
    return {"vrf": scope.vrf, "address-family": afs, "peer": peers}


def _bgp_attach_redistribution(
    row,
    identity: tuple,
    origins: OriginRecorder,
    routers: list,
    router_by_asn: dict,
    scope_by_key: dict,
    af_by_key: dict,
    constructs: dict,
    ned_id: str | None,
) -> None:
    asn, vrf, af = identity
    path = (("router", (str(asn),)), ("scope", (vrf,)), ("address-family", (af,)))
    entry = _bgp_redistribute_entry(row, ned_id)
    logical = _origin_row(
        origins,
        "bgp.router.scope.address-family.redistribute",
        (*path, ("redistribute", (entry["source-protocol"], entry["source-ref"]))),
        row,
        "redistribution_intent",
        "bgp",
    )
    router = router_by_asn.get(asn)
    if router is None:
        router = {"asn": asn, "scope": []}
        router_by_asn[asn] = router
        routers.append(router)
        constructs[(asn,)] = _BgpConstruct("bgp.router", path[:1], [], [])
    scope = scope_by_key.get((asn, vrf))
    if scope is None:
        scope = {"vrf": vrf, "address-family": [], "peer": []}
        scope_by_key[(asn, vrf)] = scope
        router["scope"].append(scope)
        constructs[(asn, vrf)] = _BgpConstruct("bgp.router.scope", path[:2], [], [])
    af_entry = af_by_key.get((asn, vrf, af))
    if af_entry is None:
        af_entry = {"afi": af}
        af_by_key[(asn, vrf, af)] = af_entry
        scope["address-family"].append(af_entry)
        constructs[(asn, vrf, af)] = _BgpConstruct("bgp.router.scope.address-family", path, [], [])
    af_entry.setdefault("redistribute", []).append(entry)
    for key in ((asn,), (asn, vrf), (asn, vrf, af)):
        constructs[key].contributors.append(logical)
        constructs[key].sources.extend(logical.source_refs)


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
    origin_context: OriginContext | None = None


#: One section's rows as an encoder reads them: table name -> the document's rows.
SectionRows = Mapping[str, list[Any]]

#: What to hand an encoder that reads no execution facts at all.
_CONTEXT_FREE_EXECUTION = SectionExecution(None, community_dialect_for(None))


def encode_snmp(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode SNMP references and keep credential natural keys transient."""
    origins = OriginRecorder("snmp", execution.origin_context)
    body: dict = {}
    for table, subject, list_name, key_name in (
        ("snmp_community_intent", "snmp.community", "community", "label"),
        ("snmp_v3_user_intent", "snmp.v3-user", "v3-user", "username"),
        ("snmp_host_intent", "snmp.host", "host", "address"),
    ):
        entries = []
        for row in rows[table]:
            if subject == "snmp.community":
                entry = {
                    "name": row.label,
                    "access": _snmp_enum(row.access, _SNMP_ACCESS, "access"),
                    **({"acl": row.acl} if row.acl else {}),
                    **_snmp_vault_triple(row.vault_ref, ""),
                }
            elif subject == "snmp.v3-user":
                entry = {
                    "username": row.username,
                    **({"group": row.group_name} if row.group_name else {}),
                    **({"auth-protocol": row.auth_protocol} if row.auth_protocol else {}),
                    **({"priv-protocol": row.priv_protocol} if row.priv_protocol else {}),
                    **(_snmp_vault_triple(row.auth_vault_ref, "auth-") if row.auth_vault_ref else {}),
                    **(_snmp_vault_triple(row.priv_vault_ref, "priv-") if row.priv_vault_ref else {}),
                }
            else:
                entry = {
                    "address": row.address,
                    "version": _snmp_enum(row.version, _SNMP_VERSION, "version"),
                    "notify-type": _snmp_enum(row.notify_type, _SNMP_NOTIFY, "notify_type"),
                    **({"community-or-user": row.community_or_user} if row.community_or_user else {}),
                    **({"port": row.port} if row.port is not None else {}),
                }
            path = ((list_name, (getattr(row, key_name),)),)
            source = origins.source(row, table, "snmp")
            logical = origins.logical(subject, path, source, "snmp")
            origins.emit(subject, path, (logical,))
            entries.append(entry)
        if entries:
            body[list_name] = entries
    for info in rows["snmp_system_info_intent"]:
        if info.location is not None or info.contact is not None:
            source = origins.source(info, "snmp_system_info_intent", "snmp")
            logical = origins.logical("snmp.system-info", (), source, "snmp")
            origins.emit("snmp.system-info", (), (logical,))
        if info.location is not None:
            body["location"] = info.location
        if info.contact is not None:
            body["contact"] = info.contact
    return origins.finish(body)


def encode_static_route(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode current routes. The caller records certified retained contributions."""
    origins = OriginRecorder("static-route", execution.origin_context)
    routes = []
    for row in rows["static_route_intent"]:
        entry = static_route_entry(row)
        path = (("route", (entry["vrf"], entry["prefix"], entry["next-hop"])),)
        source = origins.source(row, "static_route_intent", "static_route")
        logical = origins.logical("static-route.route", path, source, "static_route")
        origins.emit("static-route.route", path, (logical,))
        routes.append(entry)
    return origins.finish({"route": routes})


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


def encode_logging(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``logging`` container: remote syslog hosts plus the local-levels singleton.

    The severities are emitted whenever the row sets them; whether they may be SENT is
    :func:`refuse_gated_local_levels`'s question, at the send boundary.
    """
    origins = OriginRecorder("logging", execution.origin_context)
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
        source = origins.source(row, "logging_host_intent", "logging")
        logical = origins.logical("logging.host", (("host", (row.address,)),), source, "logging")
        origins.emit("logging.host", (("host", (row.address,)),), (logical,))
        hosts.append(entry)

    body: dict = {"host": hosts}
    for levels_row in rows["logging_levels_intent"]:
        if levels := _local_levels(levels_row):
            source = origins.source(levels_row, "logging_levels_intent", "logging")
            logical = origins.logical("logging.local-levels", (), source, "logging")
            origins.emit("logging.local-levels", (), (logical,))
            body["local-levels"] = levels
    return origins.finish(body)


def encode_svi(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``svi`` container: L3 VLAN interfaces (SVIs / IRBs)."""
    origins = OriginRecorder("svi", execution.origin_context)
    interfaces = []
    for row in rows["svi_intent"]:
        require_vlan_id(row.vlan_id, "svi", row.interface_name, "vlan_id")
        entry: dict = {"interface-name": row.interface_name, "vlan-id": row.vlan_id, "type": row.svi_type}
        if row.vrf:
            entry["vrf"] = row.vrf
        source = origins.source(row, "svi_intent", "svi")
        logical = origins.logical("svi.interface", (("interface", (row.interface_name,)),), source, "svi")
        origins.emit("svi.interface", (("interface", (row.interface_name,)),), (logical,))
        interfaces.append(entry)
    return origins.finish({"interface": interfaces})


def encode_subinterface(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``subinterface`` container: dot1q L3 subinterfaces."""
    origins = OriginRecorder("subinterface", execution.origin_context)
    interfaces = []
    for row in rows["subinterface_intent"]:
        entry = build_subif_interfaces([row])[0]
        path = (("interface", (row.interface_name,)),)
        source = origins.source(row, "subinterface_intent", "subinterface")
        logical = origins.logical("subinterface.interface", path, source, "subinterface")
        origins.emit("subinterface.interface", path, (logical,))
        interfaces.append(entry)
    return origins.finish({"interface": interfaces})


def encode_vlan(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``vlan`` container: the device's L2 VLAN database."""
    origins = OriginRecorder("vlan", execution.origin_context)
    vlans = []
    for row in rows["vlan_intent"]:
        require_vlan_id(row.vlan_id, "vlan", str(row.vlan_id), "vlan_id")
        entry: dict = {"vlan-id": row.vlan_id}
        if row.name:
            entry["name"] = row.name
        source = origins.source(row, "vlan_intent", "vlan")
        logical = origins.logical("vlan.vlan", (("vlan", (row.vlan_id,)),), source, "vlan")
        origins.emit("vlan.vlan", (("vlan", (row.vlan_id,)),), (logical,))
        vlans.append(entry)
    return origins.finish({"vlan": vlans})


def encode_bfd(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``bfd`` container: per-interface BFD timers."""
    origins = OriginRecorder("bfd", execution.origin_context)
    interfaces = []
    for row in rows["bfd_intent"]:
        entry: dict = {"interface-name": row.interface_name, "micro-bfd": bool(row.micro_bfd)}
        if row.min_tx is not None:
            entry["min-tx"] = row.min_tx
        if row.min_rx is not None:
            entry["min-rx"] = row.min_rx
        if row.multiplier is not None:
            entry["multiplier"] = row.multiplier
        source = origins.source(row, "bfd_intent", "bfd")
        logical = origins.logical("bfd.interface", (("interface", (row.interface_name,)),), source, "bfd")
        origins.emit("bfd.interface", (("interface", (row.interface_name,)),), (logical,))
        interfaces.append(entry)
    return origins.finish({"interface": interfaces})


def encode_interface_mtu(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``mtu`` container: per-interface L2 / IP / MPLS MTU."""
    origins = OriginRecorder("mtu", execution.origin_context)
    interfaces = []
    for row in rows["interface_mtu_intent"]:
        entry: dict = {"interface-name": row.interface_name}
        if row.mtu is not None:
            entry["mtu"] = row.mtu
        if row.ip_mtu is not None:
            entry["ip-mtu"] = row.ip_mtu
        if row.mpls_mtu is not None:
            entry["mpls-mtu"] = row.mpls_mtu
        source = origins.source(row, "interface_mtu_intent", "interface_mtu")
        logical = origins.logical("mtu.interface", (("interface", (row.interface_name,)),), source, "interface_mtu")
        origins.emit("mtu.interface", (("interface", (row.interface_name,)),), (logical,))
        interfaces.append(entry)
    return origins.finish({"interface": interfaces})


def encode_l2_sap(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``l2-sap`` container: SAPs under existing SR OS epipe/vpls services."""
    origins = OriginRecorder("l2-sap", execution.origin_context)
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
        source = origins.source(row, "l2_sap_intent", "l2_sap")
        logical = origins.logical("l2-sap.sap", (("sap", (row.service_name, row.sap_id)),), source, "l2_sap")
        origins.emit("l2-sap.sap", (("sap", (row.service_name, row.sap_id)),), (logical,))
        saps.append(entry)
    return origins.finish({"sap": saps})


def _origin_row(origins: OriginRecorder, subject: str, path: tuple, row: Any, table: str, stream: str) -> LogicalRow:
    source = origins.source(row, table, stream)
    logical = origins.logical(subject, path, source, stream)
    origins.emit(subject, path, (logical,))
    return logical


def encode_isis(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode IS-IS and preserve each contributor's exact stream."""
    origins = OriginRecorder("isis", execution.origin_context)
    children: dict[str, list[LogicalRow]] = {}
    redist_by_proc: dict[str, list[dict]] = {}
    for row in rows["redistribution_intent"]:
        entry = _redistribute_entry(row, execution.ned_id)
        path: tuple = (
            ("process-config", (row.dest_ref,)),
            ("redistribute", (entry["source-protocol"], entry["source-ref"])),
        )
        logical = _origin_row(origins, "isis.process-config.redistribute", path, row, "redistribution_intent", "isis")
        children.setdefault(row.dest_ref, []).append(logical)
        redist_by_proc.setdefault(row.dest_ref, []).append(entry)
    flex_by_proc: dict[str, list[dict]] = {}
    for row in rows["isis_flex_algo_intent"]:
        tag = row.process_tag or ""
        entry = _isis_flex_algo_entry(row)
        path = (("process-config", (tag,)), ("flex-algo", (entry["algo-id"],)))
        logical = _origin_row(
            origins, "isis.process-config.flex-algo", path, row, "isis_flex_algo_intent", "isis_flex_algo"
        )
        children.setdefault(tag, []).append(logical)
        flex_by_proc.setdefault(tag, []).append(entry)
    levels_by_proc: dict[str, list[dict]] = {}
    for row in rows["isis_level_intent"]:
        tag = row.process_tag or ""
        entry = _isis_level_entry(row)
        path = (("process-config", (tag,)), ("level", (entry["level"],)))
        logical = _origin_row(origins, "isis.process-config.level", path, row, "isis_level_intent", "isis")
        children.setdefault(tag, []).append(logical)
        levels_by_proc.setdefault(tag, []).append(entry)
    parents: dict[str, dict] = {}
    own: dict[str, LogicalRow] = {}
    for row in rows["isis_process_intent"]:
        tag = row.process_tag or ""
        source = origins.source(
            row,
            "isis_process_intent",
            "isis",
            children=tuple(source for logical in children.get(tag, []) for source in logical.source_refs),
        )
        own[tag] = origins.logical("isis.process-config", (("process-config", (tag,)),), source, "isis")
        parents[tag] = _isis_process_entry(row, redist_by_proc.get(tag, []))
    for tag, entries in redist_by_proc.items():
        if tag not in parents:
            parents[tag] = {"process-tag": tag, "redistribute": entries}
    for tag, entries in flex_by_proc.items():
        parents.setdefault(tag, {"process-tag": tag})["flex-algo"] = entries
    for tag, entries in levels_by_proc.items():
        parents.setdefault(tag, {"process-tag": tag})["level"] = entries
    processes = []
    for tag, entry in parents.items():
        contributors = (*((own[tag],) if tag in own else ()), *children.get(tag, []))
        fields = {field: (own[tag],) for field in SUBJECTS["isis.process-config"].fields} if tag in own else None
        origins.emit("isis.process-config", (("process-config", (tag,)),), contributors, field_rows=fields)
        processes.append(entry)
    interfaces = []
    for row in rows["isis_interface_intent"]:
        entry = build_isis_interface_payload([row])[0]
        _origin_row(
            origins,
            "isis.interface-config",
            (("interface-config", (row.interface_name, row.af)),),
            row,
            "isis_interface_intent",
            "isis",
        )
        interfaces.append(entry)
    body: dict = {}
    if interfaces:
        body["interface-config"] = interfaces
    if processes:
        body["process-config"] = processes
    return origins.finish(body)


def encode_bgp(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode BGP with canonical string ASN identities and separate peer outcomes."""
    origins = OriginRecorder("bgp", execution.origin_context)
    validate_asn_rows(
        "bgp_router_intent", [{"id": getattr(row, "id", None), "asn": row.asn} for row in rows["bgp_router_intent"]]
    )
    redistribution = [
        {
            "id": getattr(row, "id", None),
            "dest_protocol": "bgp",
            "dest_ref": row.dest_ref,
            "source_protocol": row.source_protocol,
            "source_ref": row.source_ref,
        }
        for row in rows["redistribution_intent"]
    ]
    validate_asn_rows("redistribution_intent", redistribution, ned_id=execution.ned_id)
    routers: list[dict] = []
    router_by_asn: dict[int, dict] = {}
    scope_by_key: dict[tuple[int, str], dict] = {}
    af_by_key: dict[tuple[int, str, str], dict] = {}
    constructs: dict[tuple, _BgpConstruct] = {}
    for row in rows["bgp_router_intent"]:
        asn = checked_asn(row.asn, "bgp_router_intent", getattr(row, "id", None), "asn")
        path = (("router", (str(asn),)),)
        source = origins.source(row, "bgp_router_intent", "bgp", children=_bgp_nested_sources(origins, row))
        logical = origins.logical("bgp.router", path, source, "bgp")
        constructs[(asn,)] = _BgpConstruct(
            "bgp.router",
            path,
            [logical],
            [source],
            {field: (logical,) for field in SUBJECTS["bgp.router"].fields},
            {field: (source,) for field in SUBJECTS["bgp.router"].fields},
        )
        scopes = []
        for scope in row.scopes:
            entry = _bgp_scope_entry(scope, asn, origins, logical, constructs, af_by_key)
            scope_by_key[(asn, scope.vrf)] = entry
            scopes.append(entry)
        router: dict = {"asn": asn, "scope": scopes}
        if row.router_id:
            router["router-id"] = row.router_id
        routers.append(router)
        router_by_asn[asn] = router
    for row, record in zip(rows["redistribution_intent"], redistribution, strict=True):
        identity = asn_row_identity("redistribution_intent", record)
        assert identity is not None
        _bgp_attach_redistribution(
            row, identity[1], origins, routers, router_by_asn, scope_by_key, af_by_key, constructs, execution.ned_id
        )
    for construct in constructs.values():
        origins.emit(
            construct.subject,
            construct.path,
            construct.contributors,
            sources=tuple(dict.fromkeys(construct.sources)),
            field_rows=construct.field_rows,
            field_sources=construct.field_sources,
        )
    return origins.finish({"router": routers})


def _community_member(entry: object) -> str:
    """Validate one stored community member for both encoding and reporting."""
    if not isinstance(entry, dict) or not isinstance(entry.get("community"), str):
        raise NsoApplyError(
            "invalid_route_policy_intent",
            "Community list has an entry without a string community key",
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
            member = _community_member(entry)
            if dialect.from_canonical(member) is UNREPRESENTABLE:
                skipped.append((row.name, member))
    return skipped


def encode_route_policy(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode policy objects and entry causes at the plugin's object settlement grain."""
    origins = OriginRecorder("route-policy", execution.origin_context)
    body: dict = {"prefix-list": [], "community-list": [], "as-path": [], "route-map": []}
    for row in rows["route_policy_object_intent"]:
        list_name = row.family.replace("_", "-")
        if list_name not in body:
            continue
        subject = "route-policy." + list_name
        path = ((list_name, (row.name,)),)
        source = origins.source(row, "route_policy_object_intent", "route_policy")
        logical = origins.logical(subject, path, source, "route_policy")
        entries = row.entries
        if row.family == "route_map":
            entries = [_normalize_route_map_entry(entry) for entry in entries if isinstance(entry, dict)]
        kept = []
        for entry in entries:
            if row.family == "community_list":
                member = _community_member(entry)
                wire = execution.dialect.from_canonical(member)
                if wire is UNREPRESENTABLE:
                    continue
                entry = {**entry, "community": wire} if wire != member else entry
            entry_subject = subject + ".entry"
            if "sequence" in entry:
                origins.emit(entry_subject, (*path, ("entry", (entry["sequence"],))), (logical,))
            else:
                origins.index.unsupported_subjects[entry_subject] = UnsupportedOrigin("missing_natural_key")
            kept.append(entry)
        emitted: dict = {"name": row.name}
        if row.family == "community_list":
            emitted["invert-match"] = bool(getattr(row, "invert_match", False))
        emitted["entry"] = kept
        origins.emit(subject, path, (logical,))
        body[list_name].append(emitted)
    return origins.finish(body)


def encode_ospf(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode OSPF with exact same-stream synthetic parent contributors."""
    origins = OriginRecorder("ospf", execution.origin_context)
    redist_by_proc: dict[str, list[dict]] = {}
    children: dict[str, list[LogicalRow]] = {}
    for row in rows["redistribution_intent"]:
        entry = _redistribute_entry(row, execution.ned_id)
        path: tuple = (
            ("process-config", (row.dest_ref,)),
            ("redistribute", (entry["source-protocol"], entry["source-ref"])),
        )
        logical = _origin_row(origins, "ospf.process-config.redistribute", path, row, "redistribution_intent", "ospf")
        children.setdefault(row.dest_ref, []).append(logical)
        redist_by_proc.setdefault(row.dest_ref, []).append(entry)
    processes = []
    emitted_pids = set()
    for row in rows["ospf_instance_intent"]:
        pid = str(row.process_id)
        entry = _ospf_process_entry(row, redist_by_proc.get(pid, []))
        source = origins.source(
            row,
            "ospf_instance_intent",
            "ospf",
            children=tuple(source for logical in children.get(pid, []) for source in logical.source_refs),
        )
        path = (("process-config", (pid,)),)
        logical = origins.logical("ospf.process-config", path, source, "ospf")
        origins.emit(
            "ospf.process-config",
            path,
            (logical, *children.get(pid, [])),
            field_rows={field: (logical,) for field in SUBJECTS["ospf.process-config"].fields},
        )
        processes.append(entry)
        emitted_pids.add(pid)
    for pid, redist_list in redist_by_proc.items():
        if pid not in emitted_pids:
            origins.emit("ospf.process-config", (("process-config", (pid,)),), children[pid])
            processes.append({"process-id": pid, "enabled": True, "redistribute": redist_list})
            emitted_pids.add(pid)
    interfaces = []
    for row in rows["ospf_interface_intent"]:
        entry = _ospf_interface_entry(row)
        _origin_row(
            origins,
            "ospf.interface-config",
            (("interface-config", (row.interface_name,)),),
            row,
            "ospf_interface_intent",
            "ospf",
        )
        interfaces.append(entry)
    body: dict = {}
    if interfaces:
        body["interface-config"] = interfaces
    if processes:
        body["process-config"] = processes
    return origins.finish(body)


def encode_switchport(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode the ``switchport`` container: per-interface L2 mode and VLAN membership.

    ``tagged-vlan`` is sorted so two stores holding the same membership in a different
    insertion order encode the same bytes; a YANG leaf-list is order-insensitive.
    """
    origins = OriginRecorder("switchport", execution.origin_context)
    interfaces = []
    for row in rows["switchport_intent"]:
        entry: dict = {"interface-name": row.interface_name}
        if row.mode:
            entry["mode"] = row.mode
        if row.untagged_vlan is not None:
            require_vlan_id(row.untagged_vlan, "switchport", row.interface_name, "untagged_vlan")
            entry["untagged-vlan"] = row.untagged_vlan
        tagged = sorted(tag.vlan_id for tag in row.tagged_vlans)
        if len(tagged) != len(set(tagged)):
            raise OriginConstructionError("duplicate origin")
        for vlan_id in tagged:
            require_vlan_id(vlan_id, "switchport", row.interface_name, "tagged_vlans")
        if tagged:
            entry["tagged-vlan"] = tagged
        child_sources = tuple(
            origins.source(tag, "switchport_tagged_vlan_intent", "switchport", stampable=False)
            for tag in row.tagged_vlans
        )
        source = origins.source(row, "switchport_intent", "switchport", children=child_sources)
        logical = origins.logical(
            "switchport.interface",
            (("interface", (row.interface_name,)),),
            source,
            "switchport",
            children=child_sources,
        )
        origins.emit("switchport.interface", (("interface", (row.interface_name,)),), (logical,))
        interfaces.append(entry)
    return origins.finish({"interface": interfaces})


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


def encode_lag(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode LACP bundles with separate member outcomes and bundle carriers."""
    origins = OriginRecorder("lag", execution.origin_context)
    bundles = []
    for row in rows["lag_bundle_intent"]:
        path = (("bundle", (row.name,)),)
        member_sources = tuple(
            origins.source(member, "lag_member_intent", "lag", stampable=False) for member in row.members
        )
        source = origins.source(row, "lag_bundle_intent", "lag", children=member_sources)
        bundle = origins.logical("lag.bundle", path, source, "lag")
        entry: dict = {"name": row.name}
        for attribute, leaf in _LAG_BUNDLE_LEAVES:
            value = getattr(row, attribute)
            if value is not None:
                entry[leaf] = value
        members = []
        member_logicals = []
        for member, member_source in zip(row.members, member_sources, strict=True):
            member_entry: dict = {"interface-name": member.interface_name}
            if member.mode:
                member_entry["mode"] = member.mode
            if member.port_priority is not None:
                member_entry["port-priority"] = member.port_priority
            member_path = (*path, ("member", (member.interface_name,)))
            logical = origins.logical("lag.bundle.member", member_path, member_source, "lag", carrier=bundle.carrier)
            origins.emit("lag.bundle.member", member_path, (logical,))
            member_logicals.append(logical)
            members.append(member_entry)
        if members:
            entry["member"] = members
        origins.emit(
            "lag.bundle",
            path,
            (bundle, *member_logicals),
            field_rows={
                field: (member_logicals if field == "lag.member_mode" and member_logicals else (bundle,))
                for field in SUBJECTS["lag.bundle"].fields
            },
        )
        bundles.append(entry)
    return origins.finish({"bundle": bundles})


INTERFACE_ATTRIBUTE_LEAVES = frozenset({"description", "enabled"})


def _interface_attribute_rows(
    rows: SectionRows, execution: SectionExecution, origins: OriginRecorder, by_name: dict
) -> dict[str, dict[str, LogicalRow]]:
    attributes: dict[str, dict[str, LogicalRow]] = {}
    for row in rows["interface_intent"]:
        if (row.interface_id, row.attribute) not in execution.proof.eligible_attributes:
            continue
        iface = execution.proof.interfaces[row.interface_id]
        if row.attribute not in INTERFACE_ATTRIBUTE_LEAVES:
            raise NsoApplyError(
                "unsupported_attribute",
                "interface_config: an attribute in the managed scope has no wire leaf",
                detail={"interface": iface.name, "attribute": row.attribute},
            )
        entry = by_name.setdefault(iface.name, {"interface-name": iface.name})
        if row.attribute == "description":
            entry["description"] = row.intent_value if row.intent_value is not None else ""
        else:
            entry["enabled"] = _coerce_enabled_intent(row.intent_value)
        context = (iface.name, nokia_attr_kind(iface), iface.service, iface.parent_binding, iface.encap_tag)
        source = origins.source(row, "interface_intent", "interface_config", writer_context=context)
        logical = origins.logical(
            "interface." + row.attribute, (("interface", (iface.name,)),), source, "interface_config"
        )
        attributes.setdefault(iface.name, {})[row.attribute] = logical
    return attributes


def _interface_parent_origins(
    origins: OriginRecorder,
    name: str,
    attrs: dict[str, LogicalRow],
    ips: list[LogicalRow],
    attr_kind: str | None,
    ip_kind: str | None,
) -> None:
    path = (("interface", (name,)),)
    attr_rows = tuple(attrs.values())
    for attribute in ("description", "enabled"):
        subject = "interface." + attribute
        if attr_rows:
            origins.emit(subject, path, (attrs[attribute],) if attribute in attrs else attr_rows)
        else:
            origins.index.mark_unsupported("interface", row_ref("interface", subject, path), "missing_binding")
    structural = attr_rows if attr_kind else ips if ip_kind else (*attr_rows, *ips)
    fields = {field: structural for field in SUBJECTS["interface.interface"].fields}
    fields["interface.attrs.interface_name"] = attr_rows or tuple(ips)
    origins.emit("interface.interface", path, (*attr_rows, *ips), field_rows=fields)


def encode_interface_config(rows: SectionRows, execution: SectionExecution) -> EncodedSection:
    """Encode one interface row and preserve attribute and IP stream ownership."""
    origins = OriginRecorder("interface", execution.origin_context)
    interfaces = execution.proof.interfaces
    by_name: dict[str, dict] = {}
    attributes = _interface_attribute_rows(rows, execution, origins, by_name)
    ip_by_iface: dict[int, list] = {}
    for row in rows["interface_ip_intent"]:
        ip_by_iface.setdefault(row.interface_id, []).append(row)
    ip_logicals: dict[str, list[LogicalRow]] = {}
    ip_kinds: dict[str, str | None] = {}
    for interface_id, ip_rows in ip_by_iface.items():
        iface = interfaces[interface_id]
        kind = nokia_routed_kind(iface)
        before = len(origins.contributors)
        ip_entry = build_interface_ip_body(
            iface.name,
            ip_rows,
            kind=kind,
            service=iface.service if kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
            origins=origins,
        )
        ip_logicals[iface.name] = origins.contributors[before:]
        ip_kinds[iface.name] = kind
        entry = by_name.setdefault(iface.name, {"interface-name": iface.name})
        entry.update({key: value for key, value in ip_entry.items() if key != "interface-name"})
    attr_kinds: dict[str, str | None] = {}
    for iface in interfaces.values():
        if iface.name not in attributes:
            continue
        kind = nokia_attr_kind(iface)
        attr_kinds[iface.name] = kind
        _add_nokia_routed_context(
            by_name[iface.name],
            kind=kind,
            service=iface.service if kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )
    for name in by_name:
        _interface_parent_origins(
            origins, name, attributes.get(name, {}), ip_logicals.get(name, []), attr_kinds.get(name), ip_kinds.get(name)
        )
    return origins.finish({"interface": list(by_name.values())})
