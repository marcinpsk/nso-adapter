# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Async removal propagation for the per-service intent PUT endpoints.

A merge-PATCH apply never drops a list entry you omit, and a node-level RESTCONF
DELETE 404s on empty-string list keys. So when an intent PUT (full-replace store)
deletes rows, the device keeps the orphaned config until the FULL remaining
desired state is re-asserted via a PUT-replace of the keyed service instance
(``apply_callable(..., replace=True)``), which lets FASTMAP revert the removed
entries.

That PUT-replace is a synchronous device commit and can take well over the
plugin's HTTP client timeout (~30s). So it does NOT run inline in the intent
PUT anymore — :func:`replace_on_removal` enqueues a ``removal`` job and returns
immediately; the worker runs :func:`run_removal` in the background. The job is
idempotent (it re-reads the current accepted rows and PUT-replaces), so it is
safe to requeue after a restart.
"""

from __future__ import annotations

import ipaddress
import json
from functools import cache
from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


# ── device-state verify budget (READSEM 1328) ────────────────────────────────
# The post-commit verifiers (removal residue + apply reader-compare) read the far side of
# the FASTMAP writer through the device-state-read ACTION. That action is HEAVY — it runs a
# live whole-device CDB extraction inside a txid bracket (not the cache-backed legacy GET),
# so it needs an explicit wall-clock bound. _VERIFY_BATCH_TIMEOUT bounds one batched/residue
# action; the default-path per-scope budget lives in apply.py (_VERIFY_PER_CALL_TIMEOUT /
# _VERIFY_TOTAL_BUDGET) and wraps translate + semaphore + HTTP together.
_VERIFY_PER_CALL_TIMEOUT = 60.0  # single-family action ceiling (default reader-compare path)
_VERIFY_TOTAL_BUDGET = 120.0  # per-apply default-path reader-compare wall-clock budget
_VERIFY_BATCH_TIMEOUT = 360.0  # atomic reader-compare batch + single-scope residue action


async def _live_family_sections(client, device_name: str, wire_names: list[str], *, timeout: float) -> dict[str, dict]:
    """Fetch each family's section via the device-state-read ACTION, behind the action semaphore.

    The action output is CERTIFIED inside :meth:`NsoClient.run_device_state_read` (atomic,
    device-name echo, terminal per-section status) — so a version-skewed / wrong-device /
    non-terminal response raises :class:`NsoReadContractError` there and every caller's
    catch→keep-rows handling takes over. Here we only serialize against the shared 4-slot
    action semaphore and pick the requested sections out. A requested wire absent from the
    certified output is a contract bug (the action always answers a requested family) → the
    ``KeyError`` propagates to the caller's except → "error", never a fabricated clean/present.
    """
    from nso_adapter.core.refresh_engine import _action_semaphore

    async with _action_semaphore():
        output = await client.run_device_state_read(device_name, list(wire_names), timeout=timeout)
    return {wire: output[wire] for wire in wire_names}


def _verifier_section_status(section: dict) -> str:
    """Classify a certified device-state section for the post-commit verifiers → "ok"|"unknown"|"error".

    Certification guarantees the status is terminal (``ok|unsupported|error``); this maps it to
    the shared verify verdict the residue and reader-compare consumers each refine:
    ``ok`` → walk the section; ``unsupported`` → "unknown" (no export surface — absence proves
    nothing: residue reads it as unsupported, reader-compare as unknown); ``error`` (the family
    read errored) → "error", which each caller turns into its error verdict.
    """
    status = section.get("status")
    if status == "ok":
        return "ok"
    if status == "unsupported":
        return "unknown"
    return "error"


# scope → (intent store model name, apply function name) for the "simple" services
# whose apply takes a single ``(client, device_name, rows, replace=True)`` signature.
# logging is listed for the model→scope map/valid-scope set but dispatches bespoke
# (_replace_logging): its PUT-replace must also carry the local-levels singleton.
_SIMPLE_TARGETS: dict[str, tuple[str, str]] = {
    "route_policy": ("RoutePolicyObjectIntent", "apply_route_policy_config"),
    "bfd": ("BfdIntent", "apply_bfd_config"),
    "svi": ("SviIntent", "apply_svi_config"),
    "subinterface": ("SubinterfaceIntent", "apply_subinterface_config"),
    "static_route": ("StaticRouteIntent", "apply_static_routes"),
    "interface_mtu": ("InterfaceMtuIntent", "apply_mtu_config"),
    "vlan": ("VlanIntent", "apply_vlan_config"),
    "logging": ("LoggingHostIntent", "apply_logging_config"),
    "l2_sap": ("L2SapIntent", "apply_l2_saps"),
}

# Reverse map: intent store model name → removal scope, so the legacy
# replace_on_removal(store_model, apply_callable) callers need no change.
_SCOPE_BY_MODEL: dict[str, str] = {model: scope for scope, (model, _) in _SIMPLE_TARGETS.items()}

# Scopes whose apply function translates values to the device's NED dialect and so
# takes a ``ned_id`` kwarg. The PUT-replace MUST thread it — otherwise the identity
# dialect pushes canonical (wrong) wire form and fails to skip unrepresentable members
# (route-policy communities). Kept as an explicit set so it survives mocking/refactors.
_NED_DIALECT_SCOPES: frozenset[str] = frozenset({"route_policy"})

# OSPF/BGP/IS-IS have multi-row applies; interface_config is a compound-key (device,interface)
# list whose removal PUT-replaces/deletes per-interface instances — all bespoke below.
VALID_REMOVAL_SCOPES: set[str] = set(_SIMPLE_TARGETS) | {"ospf", "bgp", "isis", "interface_config", "snmp"}


class RemovalBlockedError(Exception):
    """A PUT-replace would retract service rows nobody just removed (collateral).

    Raised by the scope guard BEFORE anything is committed; carries the orphan keys
    per YANG list and a native dry-run preview so the failed job's detail gives the
    operator the full would-be device delta to review (the ra1 lo0 incident guard).
    """

    def __init__(self, orphans: dict[str, list], preview: str | None):
        self.orphans = orphans
        self.preview = preview
        super().__init__(f"PUT-replace would retract rows not in intent: {orphans}")


# ── collateral guard (#90) — every device-keyed PUT-replace scope ─────────────


class _GuardList(NamedTuple):
    """One keyed YANG list the guard compares.

    ``label`` names the list in ``context["removed"]`` and in the orphan report;
    ``path`` walks nested lists from the service entry root to the keyed list
    (bgp peers live at router→scope→peer); ``keys`` are the key leaf names.
    """

    label: str
    path: tuple[str, ...]
    keys: tuple[str, ...]


class _GuardSpec(NamedTuple):
    service_path: str
    lists: tuple[_GuardList, ...]


@cache
def _guard_specs() -> dict[str, _GuardSpec]:
    """Scope → service instance path + the keyed YANG lists a PUT-replace can retract.

    Service paths come from the apply module (single source of truth). Nested
    non-keyed content (route-map entries, redistribute rows, bgp address-families,
    snmp system-info scalars) is intentionally NOT guarded: the collateral unit is
    the keyed config object a stale service row would silently flush off the device.
    interface_config is excluded — its removal is per-instance PUT/DELETE by design.
    """
    from nso_adapter.nso import apply as A

    return {
        "isis": _GuardSpec(
            A._ISIS_SERVICE_PATH,
            (
                _GuardList("interface-config", ("interface-config",), ("interface-name", "af")),
                _GuardList("process-config", ("process-config",), ("process-tag",)),
            ),
        ),
        "ospf": _GuardSpec(
            A._OSPF_SERVICE_PATH,
            (
                _GuardList("interface-config", ("interface-config",), ("interface-name",)),
                _GuardList("process-config", ("process-config",), ("process-id",)),
            ),
        ),
        "bgp": _GuardSpec(
            A._BGP_SERVICE_PATH,
            (
                _GuardList("router", ("router",), ("asn",)),
                # device-wide flatten: the trigger can only produce peer addresses
                # across all routers/scopes, so the guard compares at the same grain
                _GuardList("peer", ("router", "scope", "peer"), ("peer-address",)),
            ),
        ),
        "snmp": _GuardSpec(
            A._SNMP_SERVICE_PATH,
            (
                _GuardList("community", ("community",), ("name",)),
                _GuardList("v3-user", ("v3-user",), ("username",)),
                _GuardList("host", ("host",), ("address",)),
            ),
        ),
        "route_policy": _GuardSpec(
            A._ROUTE_POLICY_SERVICE_PATH,
            (
                _GuardList("prefix-list", ("prefix-list",), ("name",)),
                _GuardList("community-list", ("community-list",), ("name",)),
                _GuardList("as-path", ("as-path",), ("name",)),
                _GuardList("route-map", ("route-map",), ("name",)),
            ),
        ),
        "bfd": _GuardSpec(A._BFD_SERVICE_PATH, (_GuardList("interface", ("interface",), ("interface-name",)),)),
        "svi": _GuardSpec(A._SVI_SERVICE_PATH, (_GuardList("interface", ("interface",), ("interface-name",)),)),
        "subinterface": _GuardSpec(
            A._SUBIF_SERVICE_PATH, (_GuardList("interface", ("interface",), ("interface-name",)),)
        ),
        "static_route": _GuardSpec(
            A._STATIC_ROUTE_SERVICE_PATH, (_GuardList("route", ("route",), ("vrf", "prefix", "next-hop")),)
        ),
        "interface_mtu": _GuardSpec(
            A._MTU_SERVICE_PATH, (_GuardList("interface", ("interface",), ("interface-name",)),)
        ),
        "vlan": _GuardSpec(A._VLAN_SERVICE_PATH, (_GuardList("vlan", ("vlan",), ("vlan-id",)),)),
        "logging": _GuardSpec(A._LOGGING_SERVICE_PATH, (_GuardList("host", ("host",), ("address",)),)),
        "l2_sap": _GuardSpec(A._L2_SAP_SERVICE_PATH, (_GuardList("sap", ("sap",), ("service-name", "sap-id")),)),
    }


def is_cleared(before, after) -> bool:
    """Whether an owned scalar went from SET to UNSET — the #83 retract trigger.

    A merge-PATCH apply never drops a leaf the writer omits, so a value that goes back to
    unset can only be reverted on the device by a PUT-replace of the whole service. This is
    the single predicate that decides when an intent PUT must enqueue that retract.

    Two spellings of "unset", because the store uses both:

    * ``None`` — a nullable column (isis metric, ospf cost).
    * ``""``   — a NOT NULL column with ``default=""`` (ospf vrf, logging severity). The
      writers emit these only when truthy (``if row.vrf:``), so an empty string is just as
      undroppable as a None.

    A boolean flipping ``True -> False`` is NOT a clear: the writers emit ``False``
    explicitly (isis ``microloop-avoidance: false``), so the merge-PATCH does carry it and
    no retract is needed. Treating it as one would fire a real device PUT-replace on every
    toggle-off.
    """
    if before is None or before is False or before == "":
        return False  # was already unset — nothing to retract
    if after is None:
        return True
    if isinstance(before, str) and isinstance(after, str):
        return after == ""
    return False


#: List-entry key leaves, in preference order. An entry carrying one of these is a KEYED list
#: entry (a route-map term): its key is its identity, and a merge-PATCH overwrites the rest of
#: its leaves in place. An entry carrying none of them is its own key (a community member, a
#: prefix-list line) — there, changing the value ADDS the new one and leaves the old behind.
_ENTRY_KEY_LEAVES = ("sequence", "seq", "index", "order")


def _entry_identity(item):
    """Return the identity of one list entry, for telling a DROPPED entry from an EDITED one."""
    if isinstance(item, dict):
        for leaf in _ENTRY_KEY_LEAVES:
            if item.get(leaf) is not None:
                return (leaf, str(item[leaf]))
    return json.dumps(item, sort_keys=True, default=str)


def lost_content(before, after) -> bool:
    """Whether *after* drops anything *before* carried — the structural form of :func:`is_cleared`.

    For scopes whose intent is a nested JSON document (route-policy entries) rather than a row
    of scalars. The question is the same one: is this change something a merge-PATCH can
    express? A merge only ever ADDS or overwrites, so these it cannot:

    * a cleared scalar leaf — the old value stays (:func:`is_cleared`).
    * a keyed list ENTRY that disappears — a route-map term the operator deleted is still in
      the service's CDB input, so FASTMAP keeps creating it on the device.
    * a leaf-LIST that loses (or swaps) a member — the old members merge straight back in, so
      ``match-prefix-lists: [A, B] -> [A]`` leaves B applied.
    * an unkeyed entry whose value CHANGED — a community member or prefix-list line IS its own
      key, so the edit adds the new one and leaves the old one behind.

    A value change to a KEYED entry's leaves is fine, and so is a rewritten ``set-json`` blob:
    the merge overwrites the leaf, the service input really changes, and FASTMAP reverts
    whatever the old value had created. It is the OMITTED leaf that is undroppable.
    """
    if is_cleared(before, after):
        return True
    if isinstance(before, dict):
        if not isinstance(after, dict):
            return bool(before)
        return any(lost_content(value, after.get(key)) for key, value in before.items())
    if isinstance(before, (list, tuple)):
        if not isinstance(after, (list, tuple)):
            return bool(before)
        after_by_id = {_entry_identity(item): item for item in after}
        for item in before:
            identity = _entry_identity(item)
            if identity not in after_by_id:
                return True  # the entry (or the leaf-list member) is gone
            if lost_content(item, after_by_id[identity]):
                return True  # the entry survived but something inside it was blanked
        return False
    return False


def _norm_key(key) -> tuple[str, ...]:
    """Normalize a key (scalar or sequence) to a tuple of strings.

    NSO JSON may carry ints (vlan-id) where the store/trigger has ints or strings,
    and compound keys round-trip through JSON job context as arrays — string tuples
    make all three sources comparable.
    """
    parts = key if isinstance(key, (list, tuple)) else (key,)
    return tuple("" if p is None else str(p) for p in parts)


def _leaf_keys(entry: dict, guard_list: _GuardList) -> set[tuple[str, ...]]:
    """Collect the key tuples of *guard_list*'s leaf entries under *entry*."""
    level = [entry]
    for name in guard_list.path:
        level = [child for node in level for child in (node.get(name) or [])]
    return {tuple(str(e.get(f, "")) for f in guard_list.keys) for e in level}


def _removed_context(scope: str, context: dict) -> dict[str, list]:
    """Return the trigger's just-removed keys per YANG list (with pre-#90 isis compat)."""
    removed = dict(context.get("removed") or {})
    if scope == "isis":  # legacy context shape from jobs queued before the generalization
        removed.setdefault("interface-config", context.get("removed_interfaces", []))
        removed.setdefault("process-config", context.get("removed_processes", []))
    return removed


# Scope → the device-state envelope SECTION name for that scope (READSEM 1328). The residue
# check reads the section through the ``device-state-read`` ACTION (a fresh post-commit CDB
# extraction inside a whole-build txid bracket, read as soon as possible after the replace
# commit) rather than the legacy per-family getter. The action is fresher than those getters
# (they are SUBSCRIBER-CACHE-backed — served from a cache re-extracted only on a miss or an
# async CDB-subscriber notification, so they can lag a just-made commit) and never serves the
# record-served facade's stale/not-ready. Its terminal per-family status closes the legacy
# None/empty blind spot: an ``unsupported`` section reports residue_check="unsupported"
# (transparency over a silent "clean" — intent-integrity), never a fabricated clean bill.
# Names are pinned to the real envelope section set by
# test_residue_wire_names_match_the_envelope_sections.
_RESIDUE_WIRE_NAMES: dict[str, str] = {
    "svi": "svi",
    "subinterface": "subinterface",
    "static_route": "static-route",
    "vlan": "vlan-database",
    "logging": "logging-config",
    "interface_mtu": "interface-mtu",
    "bfd": "bfd-config",
    "l2_sap": "l2-service",
    # #104 phase-2 — the guarded complex scopes; same key grain as the guard lists.
    "bgp": "bgp-config",
    "isis": "isis-interface",
    "ospf": "ospf-config",
    "route_policy": "route-policy",
    "snmp": "snmp-config",
    # #104 phase-3 — value grain, bespoke compare in _interface_config_residue: the
    # per-instance replace/delete retracts address VALUES, not keyed rows, so the
    # check intersects the trigger's removed (interface, address, vrf) triples with
    # the interface-ip section instead of walking a guard spec.
    "interface_config": "interface-ip",
}

# Guard-list label → the network-state-export list path, for the scopes where the
# export YANG names its lists differently from the reconciler-service YANG (the
# key LEAF names agree everywhere, so only the walk path needs translating).
_READER_LIST_PATHS: dict[tuple[str, str], tuple[str, ...]] = {
    ("isis", "interface-config"): ("interface",),
    ("isis", "process-config"): ("process",),
    ("ospf", "interface-config"): ("interface",),
    ("ospf", "process-config"): ("instance",),
}


def _reader_keys(scope: str, entry: dict, guard_list: _GuardList) -> set[tuple[str, ...]]:
    """Key tuples of *guard_list* present in a network-state-export reader *entry*.

    The reader lists mirror the reconciler-service shapes (bgp's router→scope→peer
    nesting included), so the guard's own ``_leaf_keys`` walk applies as-is — except
    the isis/ospf list renames in ``_READER_LIST_PATHS`` and l2, where the reader
    nests ``sap`` under ``service`` while the service list is flat (service-name,
    sap-id) — a parent-level key leaf the generic walk cannot express.
    """
    if scope == "l2_sap":
        return {
            (str(svc.get("service-name", "")), str(sap.get("sap-id", "")))
            for svc in entry.get("service") or []
            for sap in svc.get("sap") or []
        }
    path = _READER_LIST_PATHS.get((scope, guard_list.label))
    if path is not None:
        guard_list = guard_list._replace(path=path)
    return _leaf_keys(entry, guard_list)


# (scope, guard-list label) pairs whose intent key and export key live in DIFFERENT
# NAMESPACES, so they cannot be intersected as-is — a naive intersection is silently empty,
# which reads as "clean" (removal residue) and "missing" (post-apply reader-compare). Both
# verdicts would be fabricated, so these grains are TRANSLATED (below) or reported
# unverifiable — never guessed.
#
# snmp/community: the reconciler keys a community by its human-readable LABEL and pushes a
# Vault TRIPLE (mount/path/key) — NSO resolves the secret, so the adapter never sees the
# community string. network-state-export identifies the community on the device by
# sha256(community-string)[:16] (network-state-export.yang:597).
#
# CR-A17: the adapter holds the vault_ref, so it CAN resolve the secret and compute that same
# digest — see _export_key_map / core.snmp_verify. The grain is now checked whenever Vault can
# answer, and falls back to unverifiable (never to "clean") when it cannot.
UNCOMPARABLE_LISTS: frozenset[tuple[str, str]] = frozenset({("snmp", "community")})


async def _snmp_community_keys(keys: set[tuple[str, ...]], context: dict) -> dict[tuple, tuple]:
    """``(label,) → (sha256(secret)[:16],)`` for the removed communities Vault can answer for.

    The intent ROW is deleted before the removal worker ever runs, so the trigger captures each
    dropped community's vault_ref alongside its label (``context["vault_refs"]``). A job queued
    before that existed carries none, and stays unverifiable — exactly as it was.
    """
    from nso_adapter.core.snmp_verify import community_fingerprints

    refs = {label: ref for label, ref in (context.get("vault_refs") or {}).items() if (label,) in keys}
    return {(label,): (digest,) for label, digest in (await community_fingerprints(refs)).items()}


# The uncomparable grains the adapter can nonetheless RE-KEY into the export's namespace, and how.
# A grain in UNCOMPARABLE_LISTS with no translator here can only ever be reported unverifiable — so
# this registry is also what licenses a grain to appear in apply's _READER_COMPARE_SPECS at all
# (test_an_uncomparable_grain_is_only_reader_compared_if_it_can_be_TRANSLATED).
_KEY_TRANSLATORS = {("snmp", "community"): _snmp_community_keys}


async def _export_key_map(scope: str, label: str, keys: set[tuple[str, ...]], context: dict) -> dict[tuple, tuple]:
    """INTENT key → EXPORT key, for the keys whose export key can actually be determined.

    Identity for every grain that shares a namespace with the export. For the ones that do not, a
    translator re-keys what it can — and a key it CANNOT resolve is simply ABSENT from the map.
    Callers must treat an absent key as unverifiable, never as "not on the device": a Vault outage
    must not fabricate a clean bill on a credential that is still live on the router.
    """
    if (scope, label) not in UNCOMPARABLE_LISTS:
        return {k: k for k in keys}
    translator = _KEY_TRANSLATORS.get((scope, label))
    if translator is None:
        return {}
    return await translator(keys, context)


def _norm_addr(address) -> str:
    """Canonicalize an ``ip/prefix-length`` string for comparison.

    The intent form (NetBox) and the export form (device CDB) of one address can
    differ textually — IPv6 case and zero-compression — while naming the same
    interface address; ``ip_interface`` collapses both to one canonical string.
    Unparseable values fall back to the raw string.
    """
    try:
        return str(ipaddress.ip_interface(str(address)))
    except ValueError:
        return str(address)


def _norm_ip_triple(triple) -> tuple[str, str, str]:
    """Normalize one removed-value triple to comparable (interface, address, vrf)."""
    iface, address, vrf = (list(triple) + ["", "", ""])[:3]
    return (str(iface), _norm_addr(address), str(vrf or ""))


async def _interface_config_residue(client, device, context: dict) -> dict[str, list] | None:
    """Value-grain residue for interface_config removals (#104 phase-3).

    The per-instance PUT-replace/DELETE retracts address VALUES rather than keyed
    service rows, so the check intersects the trigger's just-removed
    (interface, address, vrf) triples with the interface-ip section read through the
    ACTION. A surviving address — whether a kept-adopted leaf or a husk entry — is
    reported: the operator deleted the IP in NetBox and would otherwise believe it
    left the device. Jobs without captured values (legacy queue rows,
    actions/force-removal) return ``None`` → residue_check="unsupported", never a
    silent "clean"; a NED with no interface-ip surface (status ``unsupported``) is
    likewise ``None``, and a ``status=error`` section raises → residue_check="error".
    """
    removed = (context.get("removed") or {}).get("address")
    if removed is None:
        return None
    wire = _RESIDUE_WIRE_NAMES["interface_config"]
    section = (await _live_family_sections(client, device.nso_device_name, [wire], timeout=_VERIFY_BATCH_TIMEOUT))[wire]
    status = _verifier_section_status(section)
    if status == "error":
        raise RuntimeError(f"device-state read of {wire!r} returned status=error: {section.get('error-reason')!r}")
    if status == "unknown":  # unsupported — no interface-ip surface on this NED
        return None
    present = {
        (str(iface.get("interface-name", "")), _norm_addr(addr.get("address", "")), str(addr.get("vrf") or ""))
        for iface in section.get("interface") or []
        for addr in iface.get("address") or []
    }
    survivors = sorted([str(p) for p in t] for t in removed if _norm_ip_triple(t) in present)
    return {"address": survivors} if survivors else {}


async def _residue_after_removal(client, device, scope: str, context: dict) -> tuple[dict[str, list] | None, list[str]]:
    """Report the just-removed keys the device STILL has after the replace commit (#104).

    FASTMAP's reverse diff keeps service-created entries that picked up foreign
    leaves (sw03 Vlan987: a sync between apply and removal imported the
    device-rendered ``no ip address`` into the CDB entry), so a removal can report
    SUCCESS while its keys survive in the device tree. Re-read the scope's
    device-state section through the ACTION — the NED-agnostic device-tree view,
    freshly extracted after the replace commit — and report survivors per YANG list
    (interface_config compares removed VALUES instead — see
    :func:`_interface_config_residue`).

    Returns ``(residue, unverifiable)``:

    ``residue``      survivors per YANG list; ``{}`` means every list that COULD be
                     checked came back clean; ``None`` means nothing could be checked at
                     all (no wire mapping / no captured keys / the NED does not export
                     the section — status ``unsupported``) → residue_check "unsupported".
    ``unverifiable`` labels whose grain cannot be key-matched against the export
                     (:data:`UNCOMPARABLE_LISTS`). Their keys are neither silently
                     intersected (an empty intersection would read as "clean" for a
                     credential still live on the router) nor guessed at.

    A ``status=error`` section (the family read errored) RAISES, so
    :func:`_record_residue` records residue_check="error" rather than a fabricated verdict.
    """
    if scope == "interface_config":
        return await _interface_config_residue(client, device, context), []
    wire = _RESIDUE_WIRE_NAMES.get(scope)
    spec = _guard_specs().get(scope)
    if wire is None or spec is None:
        return None, []
    removed = _removed_context(scope, context)
    if not any(removed.values()):
        # No captured keys to look for — a force-removal (nothing was trigger-deleted; the
        # operator is deliberately flushing orphans), a cleared-scalar retract (no KEY was
        # removed at all), or a legacy queue row. The check cannot run, so say so. Returning
        # {} here reported "clean" WITHOUT EVER READING THE DEVICE, on exactly the paths
        # where a survivor matters most — the same silent-clean _interface_config_residue's
        # docstring promises never to emit.
        return None, []

    # Translate each grain's removed keys into the namespace the EXPORT keys by (CR-A17: an snmp
    # community's export key is a digest of a Vault-held secret). A key the translation cannot
    # resolve makes its grain unverifiable — it is never folded into "clean".
    keymaps: dict[str, dict[tuple, tuple]] = {}
    unverifiable: list[str] = []
    for guard_list in spec.lists:
        keys = {_norm_key(k) for k in removed.get(guard_list.label, [])}
        if not keys:
            continue
        keymap = await _export_key_map(scope, guard_list.label, keys, context)
        if keys - set(keymap):
            unverifiable.append(guard_list.label)
        if keymap:
            keymaps[guard_list.label] = keymap
    unverifiable.sort()
    if not keymaps:
        return None, unverifiable  # nothing in any grain could be translated

    section = (await _live_family_sections(client, device.nso_device_name, [wire], timeout=_VERIFY_BATCH_TIMEOUT))[wire]
    status = _verifier_section_status(section)
    if status == "error":
        raise RuntimeError(f"device-state read of {wire!r} returned status=error: {section.get('error-reason')!r}")
    if status == "unknown":  # unsupported — the NED does not export this section
        return None, unverifiable
    entry = section
    residue: dict[str, list] = {}
    for guard_list in spec.lists:
        keymap = keymaps.get(guard_list.label)
        if not keymap:
            continue
        present = _reader_keys(scope, entry, guard_list)
        survivors = sorted(intent for intent, exported in keymap.items() if exported in present)
        if survivors:
            residue[guard_list.label] = [list(k) for k in survivors]
    return residue, unverifiable


async def _record_residue(job, client, device, scope: str, context: dict, *, job_id: int, device_id: int) -> None:
    """Run the post-replace residue check (#104) and record its verdict on *job*.

    A "succeeded" replace can still leave residue on the device (FASTMAP keeps entries
    holding foreign leaves), so re-read the scope's device-tree view and surface survivors.
    Verdicts:

    ``found``        a removed key is still on the device.
    ``clean``        every removed key was checked and none survived.
    ``partial``      what could be checked came back clean, but some grain could not be
                     checked at all (:data:`UNCOMPARABLE_LISTS`) — NOT a clean bill.
    ``unsupported``  nothing could be checked (no reader, no captured keys, or every
                     removed key was in an unverifiable grain).
    ``error``        the reader blew up — never fails the removal.
    """
    try:
        residue, unverifiable = await _residue_after_removal(client, device, scope, context)
    except Exception as exc:  # noqa: BLE001 — the check must never fail the removal
        logger.warning("removal.residue_check_error", job_id=job_id, device_id=device_id, scope=scope, error=repr(exc))
        job.result["residue_check"] = "error"
        return

    if unverifiable:
        # Grains whose intent key and export key live in different namespaces (snmp
        # community: a label vs a sha256 of a secret the adapter never sees). Never fold
        # these into a "clean" — a survivor there is a credential still live on the router.
        job.result["residue_unverifiable"] = unverifiable
        logger.warning(
            "removal.residue_unverifiable", job_id=job_id, device_id=device_id, scope=scope, lists=unverifiable
        )

    if residue is None:
        job.result["residue_check"] = "unsupported"
    elif residue:
        job.result["residue_check"] = "found"
        job.result["residue"] = residue
        logger.warning("removal.residue_found", job_id=job_id, device_id=device_id, scope=scope, residue=residue)
    elif unverifiable:
        job.result["residue_check"] = "partial"
    else:
        job.result["residue_check"] = "clean"


async def _guarded_apply(client, device, scope: str, context: dict | None, apply_thunk) -> None:
    """Run *scope*'s PUT-replace behind the collateral guard (the ra1 lo0 incident).

    ``apply_thunk(**kwargs)`` must call the scope's apply function with its full row
    collections, forwarding ``replace``/``dry_run``/``stage``. Guard flow: GET the
    current service instance; stage the would-be PUT body (no HTTP — the apply
    builder is the single source of key truth, so the diff is YANG-to-YANG); any
    current key that is neither re-asserted nor in the trigger's just-removed set
    is an ORPHAN — block with a native dry-run preview instead of committing.
    ``context["force"]`` (the actions/force-removal override) skips the guard.
    """
    context = context or {}
    if context.get("force"):
        logger.warning("removal.force", device_id=device.id, scope=scope)
        await apply_thunk(replace=True)
        return
    if context.get("detach"):
        # Detach (#106): the replace commits with no-networking, so nothing can be
        # flushed from the device — the orphan guard (which protects device config
        # from a real PUT-replace) must stand down, or every un-own on an instance
        # holding un-adopted siblings blocks forever.
        await apply_thunk(replace=True)
        return
    spec = _guard_specs().get(scope)
    current = None
    if spec is not None:
        current = await client.get_service_config(spec.service_path, device.nso_device_name)
    if current:
        stage: dict[str, list] = {}
        await apply_thunk(replace=True, stage=stage)
        staged_entries = next(iter(stage.values()), None) or [{}]
        entry = staged_entries[0]
        removed = _removed_context(scope, context)
        orphans: dict[str, list] = {}
        for gl in spec.lists:
            allowed = {_norm_key(k) for k in removed.get(gl.label, [])}
            orphan = sorted(_leaf_keys(current, gl) - _leaf_keys(entry, gl) - allowed)
            if orphan:
                orphans[gl.label] = [list(k) for k in orphan]
        if orphans:
            preview = await apply_thunk(replace=True, dry_run=True)
            raise RemovalBlockedError(orphans, preview)
    await apply_thunk(replace=True)


async def _replace_simple(db: AsyncSession, device, client, scope: str, context: dict | None = None) -> None:
    """PUT-replace a single-model service with its remaining accepted rows."""
    from nso_adapter.nso import apply as nso_apply
    from nso_adapter.store import models as store_models

    model_name, apply_name = _SIMPLE_TARGETS[scope]
    model = getattr(store_models, model_name)
    apply_fn = getattr(nso_apply, apply_name)
    rows = (
        (await db.execute(select(model).where(model.device_id == device.id, model.accepted_at.is_not(None))))
        .scalars()
        .all()
    )
    extra: dict = {}
    if scope in _NED_DIALECT_SCOPES:
        extra["ned_id"] = device.ned_id

    async def _apply(**kwargs):
        return await apply_fn(client, device.nso_device_name, rows, **extra, **kwargs)

    await _guarded_apply(client, device, scope, context, _apply)


async def _replace_logging(db: AsyncSession, device, client, context: dict | None = None) -> None:
    """PUT-replace the logging-reconciler with hosts AND the local-levels singleton.

    Bespoke (not routed through :func:`_replace_simple`) because the replace body must
    re-assert the ACCEPTED local-levels intent alongside the remaining hosts — a
    host-only body would FASTMAP-retract the owned severities, and on NX a retracted
    ``console`` leaf DISABLES the destination (default enabled@2), not a benign revert.
    Only accepted rows ride (never imported/staged intent), like every replace path.
    """
    from nso_adapter.nso.apply import apply_logging_config
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    rows = (
        (
            await db.execute(
                select(LoggingHostIntent).where(
                    LoggingHostIntent.device_id == device.id, LoggingHostIntent.accepted_at.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    levels = (
        await db.execute(
            select(LoggingLevelsIntent).where(
                LoggingLevelsIntent.device_id == device.id, LoggingLevelsIntent.accepted_at.is_not(None)
            )
        )
    ).scalar_one_or_none()

    async def _apply(**kwargs):
        return await apply_logging_config(client, device.nso_device_name, rows, levels_intent_row=levels, **kwargs)

    await _guarded_apply(client, device, "logging", context, _apply)


async def _replace_ospf(db: AsyncSession, device, client, context: dict | None = None) -> None:
    from nso_adapter.nso.apply import apply_ospf_config
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

    # A PUT-replace re-asserts the FULL desired state, so it must include only accepted
    # rows — never not-yet-accepted (imported/staged) intent, which would deploy
    # un-reviewed config to the device (matches _replace_simple / _replace_bgp).
    insts = (
        (
            await db.execute(
                select(OspfInstanceIntent).where(
                    OspfInstanceIntent.device_id == device.id, OspfInstanceIntent.accepted_at.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    ifaces = (
        (
            await db.execute(
                select(OspfInterfaceIntent).where(
                    OspfInterfaceIntent.device_id == device.id, OspfInterfaceIntent.accepted_at.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    redist = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device.id,
                    RedistributionIntent.dest_protocol == "ospf",
                    RedistributionIntent.accepted_at.is_not(None),
                )
            )
        )
        .scalars()
        .all()
    )

    async def _apply(**kwargs):
        return await apply_ospf_config(client, device.nso_device_name, insts, ifaces, redist, **kwargs)

    await _guarded_apply(client, device, "ospf", context, _apply)


async def _replace_bgp(db: AsyncSession, device, client, context: dict | None = None) -> None:
    from nso_adapter.core.bgp_load import attach_bgp_relationships
    from nso_adapter.nso.apply import apply_bgp_config
    from nso_adapter.store.models import BgpRouterIntent, RedistributionIntent

    routers = (
        (
            await db.execute(
                select(BgpRouterIntent).where(
                    BgpRouterIntent.device_id == device.id, BgpRouterIntent.accepted_at.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    await attach_bgp_relationships(db, routers)
    redist = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device.id,
                    RedistributionIntent.dest_protocol == "bgp",
                    RedistributionIntent.accepted_at.is_not(None),
                )
            )
        )
        .scalars()
        .all()
    )

    async def _apply(**kwargs):
        return await apply_bgp_config(client, device.nso_device_name, routers, redist, **kwargs)

    await _guarded_apply(client, device, "bgp", context, _apply)


async def _replace_interface_config(db: AsyncSession, device, client, interface_names: list[str]) -> None:
    """Propagate interface attribute/IP removal for each affected interface.

    interface-reconciler is keyed by ``(device, interface-name)``, so each interface is its
    own service instance. For an interface that still has accepted attr/IP intent, PUT-replace
    the instance with its full remaining desired state (FASTMAP reverts the dropped address).
    For an interface with NO remaining accepted intent, DELETE the instance (FASTMAP reverts
    everything it created there — the operator wants nothing managed).
    """
    from nso_adapter.core.apply import _nokia_routed_kind
    from nso_adapter.nso.apply import build_interface_config_entry, delete_interface_config, replace_interface_config
    from nso_adapter.store.models import DbInterface, InterfaceIntent, InterfaceIpIntent

    for name in interface_names:
        iface = (
            (await db.execute(select(DbInterface).where(DbInterface.device_id == device.id, DbInterface.name == name)))
            .scalars()
            .first()
        )
        if iface is None:
            await delete_interface_config(client, device.nso_device_name, name)
            continue
        ip_rows = (
            (
                await db.execute(
                    select(InterfaceIpIntent).where(
                        InterfaceIpIntent.interface_id == iface.id, InterfaceIpIntent.accepted_at.is_not(None)
                    )
                )
            )
            .scalars()
            .all()
        )
        attr_rows = (
            (
                await db.execute(
                    select(InterfaceIntent).where(
                        InterfaceIntent.interface_id == iface.id,
                        InterfaceIntent.accepted_at.is_not(None),
                        InterfaceIntent.attribute.in_(("description", "enabled")),
                    )
                )
            )
            .scalars()
            .all()
        )
        if not ip_rows and not attr_rows:
            await delete_interface_config(client, device.nso_device_name, name)
            continue
        routed_kind = _nokia_routed_kind(iface)
        entry = build_interface_config_entry(
            device.nso_device_name,
            name,
            attr_rows,
            ip_rows,
            kind=routed_kind,
            service=iface.service if routed_kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )
        await replace_interface_config(client, device.nso_device_name, name, entry)


async def _replace_isis(db: AsyncSession, device, client, context: dict | None = None) -> None:
    """PUT-replace the isis-reconciler with the device's full remaining accepted intent.

    Bespoke (not a _SIMPLE_TARGET) because apply_isis_interfaces takes several row
    collections — interfaces, processes, IS-IS redistribution, flex-algos, levels. A
    PUT-replace re-asserts only the ACCEPTED rows, so a deleted interface OR a cleared
    owned scalar (metric back to blank, whose leaf a merge-PATCH would never drop) is
    reverted on the device, while un-owned brownfield IS-IS config stays (reconcile).

    COLLATERAL GUARD (the ra1 lo0 incident): handled by :func:`_guarded_apply` like
    every PUT-replace scope — anything the service carries beyond the snapshot plus
    the trigger's just-removed keys blocks with a native dry-run preview.
    """
    from nso_adapter.nso.apply import apply_isis_interfaces
    from nso_adapter.store.models import (
        IsisFlexAlgoIntent,
        IsisInterfaceIntent,
        IsisLevelIntent,
        IsisProcessIntent,
        RedistributionIntent,
    )

    device_id = device.id

    async def _accepted(model, *extra):
        stmt = select(model).where(model.device_id == device_id, model.accepted_at.is_not(None), *extra)
        return (await db.execute(stmt)).scalars().all()

    ifaces = await _accepted(IsisInterfaceIntent)
    procs = await _accepted(IsisProcessIntent)
    flex = await _accepted(IsisFlexAlgoIntent)
    levels = await _accepted(IsisLevelIntent)
    redist = await _accepted(RedistributionIntent, RedistributionIntent.dest_protocol == "isis")

    async def _apply(**kwargs):
        return await apply_isis_interfaces(
            client, device.nso_device_name, ifaces, procs, redist, flex, levels, **kwargs
        )

    await _guarded_apply(client, device, "isis", context, _apply)


async def _replace_snmp(db: AsyncSession, device, client, context: dict | None = None) -> None:
    """PUT-replace the snmp-reconciler with the device's full remaining intent (all collections).

    Bespoke (not a _SIMPLE_TARGET) because apply_snmp_config takes four collections
    — communities, v3 users, hosts, system-info — not a single model's rows.
    """
    from nso_adapter.nso.apply import apply_snmp_config
    from nso_adapter.store.models import (
        SnmpCommunityIntent,
        SnmpHostIntent,
        SnmpSystemInfoIntent,
        SnmpV3UserIntent,
    )

    device_id = device.id
    comms = (
        (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
        .scalars()
        .all()
    )
    users = (await db.execute(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))).scalars().all()
    hosts = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().all()
    sysinfo = (
        await db.execute(select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id))
    ).scalar_one_or_none()

    async def _apply(**kwargs):
        return await apply_snmp_config(client, device.nso_device_name, comms, users, hosts, sysinfo, **kwargs)

    await _guarded_apply(client, device, "snmp", context, _apply)


async def _dispatch_scope(db: AsyncSession, device, client, scope: str, context: dict | None = None) -> None:
    if scope == "ospf":
        await _replace_ospf(db, device, client, context)
    elif scope == "bgp":
        await _replace_bgp(db, device, client, context)
    elif scope == "snmp":
        await _replace_snmp(db, device, client, context)
    elif scope == "isis":
        await _replace_isis(db, device, client, context)
    elif scope == "logging":
        await _replace_logging(db, device, client, context)
    elif scope == "interface_config":
        await _replace_interface_config(db, device, client, (context or {}).get("interfaces") or [])
    elif scope in _SIMPLE_TARGETS:
        await _replace_simple(db, device, client, scope, context)
    else:
        raise ValueError(f"Unknown removal scope {scope!r}")


async def enqueue_removal(
    db: AsyncSession,
    device_id: int,
    scope: str,
    *,
    interfaces: list[str] | None = None,
    removed: dict[str, list] | None = None,
    vault_refs: dict[str, str] | None = None,
    force: bool = False,
    retract: bool = False,
    shrank: bool = False,
):
    """Queue an async ``removal`` job that PUT-replaces *scope*'s service.

    Non-blocking: the intent PUT returns immediately and the worker runs the
    (potentially slow) device commit in the background via :func:`run_removal`.
    *interfaces* scopes an ``interface_config`` removal to the affected interface names.
    *removed* maps each YANG list to the keys the trigger JUST deleted so the
    collateral guard can tell an intended retraction from an orphaned service row;
    *force* skips the guard (the operator override after reviewing a blocked removal).

    *retract* marks a removal whose cause is a CLEARED OWNED SCALAR (a metric blanked
    back to none, #83) rather than a shrink. The row stays owned and accepted — only a
    leaf was blanked — so nothing is being un-owned and the replace must actually reach
    the device, even though no NetBox object was deleted and the push therefore cannot
    carry ?delete_origin. Without this, #106's detach-by-default committed it
    ``no-networking`` and the device kept the old value forever.

    *shrank* says whole rows were dropped by the same push. It is distinct from *removed*
    because a scope's dropped rows may be non-guarded nested content (IS-IS/OSPF
    redistribute) that never appears there — and an un-own must still detach.

    Returns ``None`` without queueing anything on a store-only request (the plugin's
    intent re-sync, tracker #103): a store shrink then reconciles the intent mirror
    only and must never retract FASTMAP-owned config from the device. *force* is
    exempt — the operator force-removal action is an explicit device flush.
    """
    from nso_adapter.core.request_flags import DELETE_ORIGIN, STORE_ONLY
    from nso_adapter.store.models import Job, JobStatus, JobType

    if scope not in VALID_REMOVAL_SCOPES:
        raise ValueError(f"Unknown removal scope {scope!r}")
    if STORE_ONLY.get() and not force:
        logger.info("removal.skipped_store_only", device_id=device_id, scope=scope)
        return None
    context: dict = {"scope": scope}
    if interfaces:
        context["interfaces"] = interfaces
    if removed:
        # compound keys arrive as tuples — make them JSON-safe arrays for the job row
        context["removed"] = {
            label: [list(k) if isinstance(k, (list, tuple)) else k for k in keys]
            for label, keys in removed.items()
            if keys
        }
    if vault_refs:
        # CR-A17. The removed intent ROW is gone by the time the worker runs, so the residue check
        # would have no way back to the Vault ref it needs to compute the removed community's
        # export key (a sha256 of the secret). Captured here, at the trigger, while the row still
        # exists. A vault_ref is a PATH, not a secret — the same value already sits in plaintext in
        # snmp_community_intent and in the push payload.
        context["vault_refs"] = dict(vault_refs)
    # An un-own rode along with the clear in the same push: ONE PUT-replace cannot honour
    # both. Networking it would strip the un-owned row's config off the device (the #106
    # damage); not networking it leaves the cleared leaf. Safety wins — but the deferred
    # retract is recorded, never silently dropped (intent-integrity). The next push that
    # carries no un-own retracts it.
    un_own = (shrank or bool(context.get("removed"))) and not DELETE_ORIGIN.get()
    if retract and un_own:
        context["retract_deferred"] = True
        logger.warning("removal.retract_deferred", device_id=device_id, scope=scope)
    if force:
        context["force"] = True
    elif not DELETE_ORIGIN.get() and not (retract and not un_own):
        # Unmarked shrink = un-own ("NetBox stops governing"), NOT an object deletion:
        # detach — drop service governance without touching the device (#106). Only a
        # push the plugin marked ?delete_origin=true (a NetBox object DELETE), a cleared
        # owned scalar (#83, above) or the operator's force-removal may retract config
        # from the live device.
        context["detach"] = True
    job = Job(
        job_type=JobType.removal,
        device_id=device_id,
        status=JobStatus.queued,
        context=context,
    )
    db.add(job)
    await db.flush()
    logger.info("removal.enqueued", device_id=device_id, scope=scope, job_id=job.id)
    return job


async def run_removal(job_id: int, device_id: int) -> None:
    """Execute a queued ``removal`` job: PUT-replace the scope's reconciler service.

    Idempotent — reads the CURRENT accepted rows at run time, so a requeue after a
    restart re-asserts whatever the present desired state is.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, Job, JobStatus

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            return
        job.status = JobStatus.running
        await db.commit()
        context = job.context or {}
        scope = context.get("scope")
        try:
            device = await db.get(Device, device_id)
            if not device:
                raise ValueError(f"Device {device_id} not found")
            from nso_adapter.nso import apply as nso_apply_mod

            client = get_nso_client(device.nso_instance)
            detach = bool(context.get("detach"))
            detach_token = nso_apply_mod.DETACH_REPLACE.set(detach)
            try:
                await _dispatch_scope(db, device, client, scope, context)
            finally:
                nso_apply_mod.DETACH_REPLACE.reset(detach_token)
            job.status = JobStatus.succeeded
            job.result = {"scope": scope}
            if detach:
                # Detach (#106): the device was deliberately untouched — the removed
                # keys are EXPECTED to remain (that is the point), so the residue
                # check is meaningless here. Re-align CDB with device truth: the
                # no-networking commit applied FASTMAP's reverse diff to CDB only.
                job.result["detach"] = True
                job.result["residue_check"] = "skipped_detach"
                from nso_adapter.nso import actions

                for attempt in (1, 2):  # one retry — slow-session flake (sw03 read eof)
                    try:
                        await actions.sync_from(client, device.nso_device_name)
                        break
                    except Exception as exc:  # noqa: BLE001 — surface, never fail the job
                        logger.warning(
                            "removal.detach_sync_from_failed",
                            job_id=job_id,
                            device_id=device_id,
                            attempt=attempt,
                            error=repr(exc),
                        )
                        if attempt == 2:
                            # CDB keeps the locally-applied reverse diff until some
                            # later sync-from — make that visible on the job.
                            job.result["sync_from"] = "failed"
            else:
                await _record_residue(job, client, device, scope, context, job_id=job_id, device_id=device_id)
            await db.commit()
            # Option A follow-up: sync now so any residue re-imports as an unowned
            # mirror immediately instead of at the next poll cycle. After the commit
            # above this job is no longer active, so the per-device dedup admits the
            # sync; best-effort — the scheduler covers it if this loses a race.
            try:
                from nso_adapter.core.jobs import enqueue_job
                from nso_adapter.store.models import JobType

                await enqueue_job(device_id, JobType.sync, db)
            except Exception as exc:  # noqa: BLE001 — never fail a committed removal on this
                logger.warning(
                    "removal.followup_sync_enqueue_failed", job_id=job_id, device_id=device_id, error=repr(exc)
                )
        except RemovalBlockedError as blocked:
            from nso_adapter.core.jobs import _mark_job_failed

            logger.error(
                "removal.blocked_collateral",
                job_id=job_id,
                device_id=device_id,
                scope=scope,
                orphans=blocked.orphans,
            )
            await _mark_job_failed(
                db,
                job_id,
                {
                    "code": "removal_blocked_collateral",
                    "message": str(blocked),
                    "detail": {
                        "scope": scope,
                        "orphans": blocked.orphans,
                        "preview": blocked.preview,
                        "hint": (
                            "These service rows are not in the remaining intent and were not part of "
                            "this retraction. Re-accept them into intent to keep them, or re-run via "
                            "POST /devices/{id}/actions/force-removal to flush them deliberately."
                        ),
                    },
                },
            )
        except Exception as exc:  # noqa: BLE001 — record on the job, never crash the worker
            from nso_adapter.core.jobs import _mark_job_failed

            logger.error("removal.failed", job_id=job_id, device_id=device_id, scope=scope, error=repr(exc))
            await _mark_job_failed(
                db, job_id, {"code": "removal_failed", "message": repr(exc), "detail": {"scope": scope}}
            )


# store family → the YANG list that carries the object in the route-policy service
_ROUTE_POLICY_FAMILY_LISTS: dict[str, str] = {
    "prefix_list": "prefix-list",
    "community_list": "community-list",
    "as_path": "as-path",
    "route_map": "route-map",
}


def _removed_map(scope: str, removed) -> dict[str, list]:
    """Map a simple scope's just-removed store keys onto its guarded YANG list(s).

    For every _SIMPLE_TARGET the intent PUT already computes the removed keys and
    they equal the YANG key values verbatim — except route_policy, whose
    ``(family, name)`` tuples bucket into the per-family lists.
    """
    if not isinstance(removed, (list, tuple, set)):
        return {}
    if scope == "route_policy":
        by_list: dict[str, list] = {}
        for family, name in removed:
            yang_list = _ROUTE_POLICY_FAMILY_LISTS.get(family)
            if yang_list:
                by_list.setdefault(yang_list, []).append(name)
        return by_list
    spec = _guard_specs().get(scope)
    if spec is None or len(spec.lists) != 1:
        return {}
    return {spec.lists[0].label: list(removed)}


async def replace_on_removal(
    db: AsyncSession, device, removed, store_model, apply_callable=None, *, retract: bool = False
) -> bool:
    """Enqueue an async removal job for *store_model*'s scope.

    Back-compat shim: the per-service intent PUTs still call this with their
    ``(store_model, apply_callable)``; the scope is derived from ``store_model`` and
    the device commit now runs in a background ``removal`` job rather than inline.
    *removed* — the just-removed store keys every caller already computes — is
    threaded into the job context so the collateral guard can tell the intended
    retraction from an orphaned service row. *apply_callable* is retained for
    signature compatibility but superseded by the scope registry. Returns True if
    a removal job was queued.

    *retract* says a CLEARED OWNED SCALAR caused (part of) this call — see
    :func:`is_cleared`. A clear is not a shrink: it removes no key, so a caller with
    nothing in *removed* must still get a job, and that job must actually reach the device
    rather than detaching (#106's default). Without this the nine simple scopes would
    detect a clear and still commit it ``no-networking`` — a no-op.

    These callers invoke this AFTER committing their row deletes, so the enqueued
    job is committed here. (OSPF/BGP call :func:`enqueue_removal` directly, before
    their own commit, to persist the deletes and the job atomically.)
    """
    if not removed and not retract:
        return False
    scope = _SCOPE_BY_MODEL.get(store_model.__name__)
    if scope is None:
        logger.error("removal.unknown_model", model=store_model.__name__)
        return False
    job = await enqueue_removal(
        db,
        device.id,
        scope,
        removed=_removed_map(scope, removed) if removed else None,
        retract=retract,
        shrank=bool(removed),
    )
    if job is None:  # store-only request — the shrink stays store-side
        return False
    await db.commit()
    return True
