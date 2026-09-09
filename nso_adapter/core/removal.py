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
immediately; the worker runs :func:`run_removal` in the background. Promoted jobs execute
their immutable projection document. A reissue promotes no projection and retains live-store
behavior; a job carrying no generation at all is refused, never executed from the store.
"""

from __future__ import annotations

import ipaddress
import json
from collections.abc import Sequence
from functools import cache
from typing import NamedTuple
from uuid import UUID

import structlog
from sqlalchemy import delete, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimLostError, JobError, error_envelope
from nso_adapter.core.projection import GuardList as _GuardList
from nso_adapter.core.request_flags import (
    AUTHORIZED_PROVENANCE,
    DELETE_ORIGIN_MARKING,
    DETACH_MARKING,
    REMOVAL_MARKINGS,
    STORE_ONLY_PROVENANCE,
    request_marking,
)

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

#: "the caller supplied no snapshot" for :func:`_guarded_apply`. A sentinel, because ``None``
#: is a legitimate snapshot value (no service instance) and must not re-trigger the GET.
_NO_SNAPSHOT = object()


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


#: Every removal scope: the write-path families, which ARE the document's sections. There is
#: no separate list — a scope is a family the document can carry, and the registry says which.
def valid_removal_scopes() -> frozenset[str]:
    """Return the sections a removal may empty. Derived, never restated."""
    from nso_adapter.core.projection import projection_sections

    return projection_sections()


class RemovalBlockedError(Exception):
    """A PUT-replace would retract service rows nobody just removed (collateral).

    Raised by the scope guard BEFORE anything is committed; carries the orphan keys per
    YANG list, which name the rows the operator must re-accept or flush (the ra1 lo0
    incident guard). It carries no device delta: native config is opaque text that holds
    resolved communities and auth keys, and this payload is persisted on the job and rows.
    """

    def __init__(self, orphans: dict[str, list]):
        self.orphans = orphans
        super().__init__(f"PUT-replace would retract rows not in intent: {orphans}")


# ── collateral guard (#90) — every device-keyed PUT-replace scope ─────────────


@cache
def _section_by_model() -> dict[str, str]:
    """Intent model name → the document section it belongs to, off the registry.

    A DISCRIMINATED table is excluded: one redistribution model serves three sections, so its
    section cannot be resolved from the model alone and the caller passes the stream instead.
    """
    from nso_adapter.core.projection import section_registry

    by_model: dict[str, str] = {}
    for section, entry in section_registry().items():
        for spec in entry.tables:
            if spec.discriminator is None:
                by_model.setdefault(spec.model.__name__, section)
    return by_model


def section_guard_lists(section: str) -> tuple[_GuardList, ...]:
    """Return the guarded YANG lists of one section, empty when it has none."""
    from nso_adapter.core.projection import section_registry

    return section_registry()[section].guard_lists if section in section_registry() else ()


@cache
def residue_wire_name(section: str) -> str | None:
    """Return the device-state envelope section a family's residue is read from.

    The registry names the READ family a write family verifies against, and the read
    engine's ``FamilySpec`` names its wire section, so the two sides cannot drift: there is
    no third table saying what ``l2_sap`` reads (memo A8).
    """
    from nso_adapter.core.importer import projectable_spec
    from nso_adapter.core.projection import section_registry

    spec = projectable_spec(section_registry()[section].read_family)
    return spec.wire_name if spec is not None else None


_EXPLICIT_FALSE_EMISSION_FIELDS = frozenset(
    {
        ("isis_interface_intent", "bfd_enabled"),
        ("isis_interface_intent", "frr_enabled"),
        ("isis_process_intent", "overload_bit"),
        ("isis_process_intent", "microloop_avoidance"),
        ("isis_level_intent", "wide_metrics_only"),
        ("isis_level_intent", "disabled"),
    }
)


def is_cleared(before, after, *, emission_field: tuple[str, str] | None = None) -> bool:
    """Whether an owned scalar went from SET to UNSET — the #83 retract trigger.

    A merge-PATCH apply never drops a leaf the writer omits, so a value that goes back to
    unset can only be reverted on the device by a PUT-replace of the whole service. This is
    the single predicate that decides when an intent PUT must enqueue that retract.

    Two spellings of "unset", because the store uses both:

    * ``None`` — a nullable column (isis metric, ospf cost).
    * ``""``   — a NOT NULL column with ``default=""`` (ospf vrf, logging severity). The
      writers emit these only when truthy (``if row.vrf:``), so an empty string is just as
      undroppable as a None.

    A boolean flipping ``True -> False`` is NOT a clear. The audited IS-IS tri-state
    fields in ``_EXPLICIT_FALSE_EMISSION_FIELDS`` emit both boolean values and omit only
    ``None``, so ``False -> None`` is a clear for those fields. Other callers keep the
    general rule that a prior ``False`` is unset.
    """
    if before is None or before == "":
        return False  # was already unset — nothing to retract
    if before is False:
        return after is None and emission_field in _EXPLICIT_FALSE_EMISSION_FIELDS
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
    if guard_list.presence:
        assert guard_list.parent_key is not None
        return {
            (str(parent[guard_list.parent_key]),)
            for parent in entry.get(guard_list.path[0]) or []
            if guard_list.path[1] in parent
        }
    if guard_list.parent_key is not None:
        return {
            (
                str(parent[guard_list.parent_key]),
                *((str(child),) if guard_list.scalar else tuple(str(child[k]) for k in guard_list.keys)),
            )
            for parent in entry.get(guard_list.path[0]) or []
            for child in parent.get(guard_list.path[1]) or []
        }
    level = [entry]
    for name in guard_list.path:
        level = [child for node in level for child in (node.get(name) or [])]
    return {tuple(str(e.get(f, "")) for f in guard_list.keys) for e in level}


def _removed_context(scope: str, context: dict) -> dict[str, list]:
    """Return the trigger's just-removed keys per YANG list."""
    return dict(context.get("removed") or {})


# Guard-list label → the network-state-export list path, for the scopes where the
# export YANG names its lists differently from the reconciler-service YANG (the
# key LEAF names agree everywhere, so only the walk path needs translating).
_READER_LIST_PATHS: dict[tuple[str, str], tuple[str, ...]] = {
    ("isis", "interface-config"): ("interface",),
    ("isis", "process-config"): ("process",),
    ("ospf", "interface-config"): ("interface",),
    ("ospf", "process-config"): ("instance",),
    ("lag", "lag_bundle_intent"): ("lag",),
    ("lag", "lag_member_intent"): ("lag", "member"),
}


def _reader_keys(scope: str, entry: dict, guard_list: _GuardList) -> set[tuple[str, ...]]:
    """Key tuples of *guard_list* present in a network-state-export reader *entry*.

    The reader lists mirror the reconciler-service shapes (bgp's router→scope→peer
    nesting included), so the guard's own ``_leaf_keys`` walk applies as-is — except
    the list renames in ``_READER_LIST_PATHS`` and the two grains the export renders
    in a different SHAPE: l2 nests ``sap`` under ``service`` while the service list is
    flat (service-name, sap-id), and switchport emits its tagged VLANs as one comma/range
    string where the service carries a leaf-list. Both would intersect empty, which reads
    as a clean bill for config the removal never took off the device.
    """
    from nso_adapter.core.vlan import parse_vlan_string

    if scope == "l2_sap":
        return {
            (str(svc.get("service-name", "")), str(sap.get("sap-id", "")))
            for svc in entry.get("service") or []
            for sap in svc.get("sap") or []
        }
    if scope == "switchport" and guard_list.scalar:
        return {
            (str(iface.get("interface-name", "")), str(vlan_id))
            for iface in entry.get(guard_list.path[0]) or []
            for vlan_id in parse_vlan_string(iface.get("tagged-vlans"))
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
    wire = residue_wire_name("interface_config")
    if wire is None:  # pragma: no cover - the registry pins interface_config to interface_ip
        return None
    section = (await _live_family_sections(client, device.nso_device_name, [wire], timeout=_VERIFY_BATCH_TIMEOUT))[wire]
    status = _verifier_section_status(section)
    if status == "error":
        # The error-reason is the server's own text and can name a community-keyed path.
        raise RuntimeError(f"device-state read of {wire!r} returned status=error")
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
    wire = residue_wire_name(scope)
    guard_lists = section_guard_lists(scope)
    if wire is None or not guard_lists:
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
    for guard_list in guard_lists:
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
        # The error-reason is the server's own text and can name a community-keyed path.
        raise RuntimeError(f"device-state read of {wire!r} returned status=error")
    if status == "unknown":  # unsupported — the NED does not export this section
        return None, unverifiable
    entry = section
    residue: dict[str, list] = {}
    for guard_list in guard_lists:
        guard_keymap = keymaps.get(guard_list.label)
        if not guard_keymap:
            continue
        present = _reader_keys(scope, entry, guard_list)
        survivors = sorted(intent for intent, exported in guard_keymap.items() if exported in present)
        if survivors:
            residue[guard_list.label] = [list(k) for k in survivors]
    return residue, unverifiable


async def _record_residue(
    result: dict, client, device, scope: str, context: dict, *, job_id: int, device_id: int
) -> None:
    """Run the post-replace residue check (#104) and record its verdict in *result*.

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
        # Metadata only: any exception from the reader can repeat what the server said.
        logger.warning(
            "removal.residue_check_error",
            job_id=job_id,
            device_id=device_id,
            scope=scope,
            error_type=type(exc).__name__,
        )
        result["residue_check"] = "error"
        return

    if unverifiable:
        # Grains whose intent key and export key live in different namespaces (snmp
        # community: a label vs a sha256 of a secret the adapter never sees). Never fold
        # these into a "clean" — a survivor there is a credential still live on the router.
        result["residue_unverifiable"] = unverifiable
        logger.warning(
            "removal.residue_unverifiable", job_id=job_id, device_id=device_id, scope=scope, lists=unverifiable
        )

    if residue is None:
        result["residue_check"] = "unsupported"
    elif residue:
        result["residue_check"] = "found"
        result["residue"] = residue
        logger.warning("removal.residue_found", job_id=job_id, device_id=device_id, scope=scope, residue=residue)
    elif unverifiable:
        result["residue_check"] = "partial"
    else:
        result["residue_check"] = "clean"


def _document_orphans(current: dict, containers: dict[str, dict], allowed: dict[str, dict]) -> dict[str, list]:
    """Return every live keyed row the document would retract without authority.

    Device-wide, because the write is: one PUT makes EVERY omission a retraction, so a
    removal of one family can flush another family's orphaned rows just as easily as its own.
    The report is scope-qualified (``<section>/<list>``) — two families both have a ``host``
    list, and an operator reading the blocked job must know which one is stuck.
    """
    from nso_adapter.core.projection import section_registry

    registry = section_registry()
    orphans: dict[str, list] = {}
    for section, entry in registry.items():
        guard_lists = entry.guard_lists
        container = registry[section].container
        live = current.get(container) or {}
        body = containers.get(container) or {}
        permitted_by_label = allowed.get(section) or {}
        for guard_list in guard_lists:
            permitted = {_norm_key(key) for key in permitted_by_label.get(guard_list.label, [])}
            orphan = sorted(_leaf_keys(live, guard_list) - _leaf_keys(body, guard_list) - permitted)
            if orphan:
                orphans[f"{section}/{guard_list.label}"] = [list(key) for key in orphan]
    return orphans


def guard_allowed(generation, *, scope: str | None = None, context: dict | None = None, route_keys=None) -> dict:
    """Return the keys a deployment may drop, per section and per guarded YANG list.

    Three sources, all authorized before the send: the immutable generation's own
    scope-qualified authority, the executing removal's just-removed keys, and the static-route
    keys the frozen plan authorizes — the predecessors an apply's replacement delivers, or the
    triples a removal's operation plane owns. Scope-qualified because the guard is device-wide
    and two families both have a ``host`` list.
    """
    from nso_adapter.core.static_route_plan import validate_removal_authority

    authority = getattr(generation, "allowed_removal_keys", None)
    if authority is None:
        authority = {}
    validate_removal_authority(authority)
    allowed: dict[str, dict[str, list]] = {
        section: {label: list(keys) for label, keys in labels.items()} for section, labels in authority.items()
    }
    if scope is not None:
        for label, keys in _removed_context(scope, context or {}).items():
            allowed.setdefault(scope, {}).setdefault(label, []).extend(keys)
    if route_keys:
        entries = allowed.setdefault("static_route", {}).setdefault("route", [])
        entries.extend(list(key) for key in sorted(route_keys))
    return allowed


async def guarded_device_write(
    client,
    device,
    containers: dict[str, dict],
    *,
    allowed: dict[str, dict],
    context: dict | None = None,
    current=_NO_SNAPSHOT,
    no_networking: bool = False,
):
    """PUT the device's document behind the device-wide collateral guard (the ra1 lo0 incident).

    Guard flow: GET the current aggregate instance; compare it against the body about to be
    sent; any live key that the body neither re-asserts nor is authorized to drop is an
    ORPHAN — block, naming the orphan keys, instead of committing.

    ``context["force"]`` (the actions/force-removal override) skips the guard, and so does a
    ``no-networking`` write: nothing can be flushed from a device the commit never reaches.

    *current* lets a caller that already read the live instance hand it in, so the
    one-snapshot contract holds (R2 §4.1: the retained entries and the guard must see the
    SAME read). The default is a sentinel, not ``None``: ``None`` is a valid snapshot meaning
    "no instance", so ``if current is None: GET`` would issue a second read on exactly the
    absent-service case.

    Returns the send's R2 §4.4 proof verdict. A guard that swallowed it would leave the
    caller unable to tell a proven commit from an unverified one.
    """
    from nso_adapter.nso.apply import apply_device_intent

    context = context or {}
    device_name = device.nso_device_name
    if context.get("force"):
        logger.warning("removal.force", device_id=device.id, scope=context.get("scope"))
    elif not no_networking:
        if current is _NO_SNAPSHOT:
            current = await client.get_service_config(device_name)
        if current:
            orphans = _document_orphans(current, containers, allowed)
            if orphans:
                raise RemovalBlockedError(orphans)
    return await apply_device_intent(client, device_name, containers, no_networking=no_networking)


# Static-route carriers retain their settlement classification across the aggregate send.

#: Consumption by supersession: the selected plan claims every key the job could drop.
SR_SUPERSEDED_EVENT = "static_route.removal_superseded"


class SrRemoval(NamedTuple):
    """The aggregate removal result, for the proof and bookkeeping that follow.

    Returned rather than acted on in place: §4.6 requires the consumption, the carrier
    updates and the terminal job status to land in ONE transaction, and that transaction
    cannot open until every post-write read has happened.
    """

    #: ``force`` | ``detach`` | ``networked`` | ``superseded``.
    branch: str
    #: The keys this job may drop from the device — tombstone triples ∪ ``deployed_key``s,
    #: minus everything the recorded plan or current reissue state claims.
    authorized: frozenset
    #: The tombstone ids snapshotted BEFORE the network call; only these may be deleted.
    tombstone_ids: tuple[int, ...]
    #: Every route key the PUT body carried.
    sent_keys: frozenset
    #: Whether a PUT was actually issued (a 2xx, since a non-2xx raises).
    put_issued: bool
    #: The commit's native-verify verdict, or ``None`` when no PUT was sent.
    verify: str | None
    #: ``{intent row id: [store field names]}`` whose wire leaves this body deleted.
    clears: dict[int, tuple[str, ...]]
    #: The creation-time route key for each delivered carrier.
    clear_keys: dict[int, tuple[str, str, str]]


def _sr_triple(key) -> tuple[str, str, str]:
    """Normalize a job-context / tombstone key to the wire triple the renderer emits."""
    parts = list(key) if isinstance(key, (list, tuple)) else [key]
    parts = (parts + ["", "", ""])[:3]
    return tuple("" if p is None else str(p) for p in parts)  # type: ignore[return-value]


async def _executing_document(db: AsyncSession, job_id: int | None, scope: str):
    """Return the generation this removal deploys, or refuse. Every producer attaches one."""
    from nso_adapter.core.generation import executing_generation

    if job_id is None:
        raise RuntimeError(f"a {scope} removal needs the generation of the job that carries it")
    generation = await executing_generation(db, job_id)
    if generation is None:
        raise RuntimeError(f"removal job {job_id} for scope {scope!r} carries no generation to deploy")
    return generation


def _classify_static_route_removal(generation, context: dict) -> SrRemoval:
    """Read the frozen operation's settlement classification without constructing a body."""
    from nso_adapter.core.static_route_plan import hydrate_static_route_removal_plan

    if context.get("force"):
        return SrRemoval("force", frozenset(), (), frozenset(), False, None, {}, {})
    plan = hydrate_static_route_removal_plan(generation.document)
    if plan.reclaimed:
        logger.warning(
            "static_route.removal_key_reclaimed",
            device_id=generation.device_id,
            job_id=generation.job_id,
            keys=[list(key) for key in plan.reclaimed],
        )
    branch = "detach" if context.get("detach") else "networked"
    if not plan.authorized and not plan.clears:
        branch = "superseded"
        logger.info(
            SR_SUPERSEDED_EVENT,
            device_id=generation.device_id,
            job_id=generation.job_id,
            tombstones=list(plan.tombstone_ids),
            reclaimed=[list(key) for key in plan.reclaimed],
        )
    return SrRemoval(
        branch,
        frozenset(plan.authorized),
        plan.tombstone_ids,
        frozenset(),
        False,
        None,
        {clear.row_id: clear.fields for clear in plan.clears},
        {clear.row_id: clear.key for clear in plan.clears},
    )


async def _put_removal_document(db: AsyncSession, device, client, scope: str, context: dict | None, *, job_id):
    """Build and send the aggregate once, with the frozen operation's settlement proof."""
    from nso_adapter.core.apply import _refuse_unverifiable_recorded_put, build_device_containers
    from nso_adapter.core.generation import execution_policy

    generation = await _executing_document(db, job_id, scope)
    policy = execution_policy(generation)
    context = policy.context
    out = _classify_static_route_removal(generation, context) if scope == "static_route" else None
    if out is not None and out.branch == "superseded":
        return out
    if not policy.no_networking:
        _refuse_unverifiable_recorded_put(generation)
    body = await build_device_containers(
        client, device, generation.document, retain_static_routes=policy.retain_static_routes
    )
    if body.errors:
        raise next(iter(body.errors.values()))
    if out is not None:
        if body.sent_route_keys is None:
            raise RuntimeError("static-route removal document has no static-route section")
        sent_keys = frozenset(body.sent_route_keys)
        delivered = {row_id: fields for row_id, fields in out.clears.items() if out.clear_keys[row_id] in sent_keys}
        out = out._replace(
            sent_keys=sent_keys,
            clears=delivered,
            clear_keys={row_id: out.clear_keys[row_id] for row_id in delivered},
        )
        if out.branch != "force" and not out.authorized and not delivered:
            return out._replace(branch="superseded", sent_keys=frozenset())
    verdict = await guarded_device_write(
        client,
        device,
        body.containers,
        allowed=guard_allowed(generation, scope=scope, context=context, route_keys=out.authorized if out else None),
        context=context,
        current=body.snapshot,
        no_networking=policy.no_networking,
    )
    return out._replace(put_issued=True, verify=verdict) if out is not None else None


# ── #1396 R2 §4.4/§4.6 — the removal's proof and its ONE terminal transaction ─


def _sr_verify_ok(out: SrRemoval) -> bool:
    """Whether the commit's own verdict permits consumption.

    A carrier-owning removal is NOT governed by OQ-R2-1's apply-side "succeed on an
    inconclusive proof": a succeeded job makes its tombstone permanently un-sweepable (G17),
    so an unproven verdict has to fail and retry. With no PUT issued there is no commit to
    have a verdict about, and the branch's other evidence carries the proof instead.
    """
    from nso_adapter.nso.apply import VERIFY_CONCLUSIVE

    return not out.put_issued or out.verify == VERIFY_CONCLUSIVE


async def _sr_service_clean(client, device, out: SrRemoval) -> bool:
    """Post-commit: whether every authorized key is gone from the SERVICE instance (§4.4).

    Certified, never inferred: an uncertifiable read answers ``None`` both for a keyed 404
    and for any 2xx it could not parse (G31), and consuming a carrier on that reading throws
    the deletion record away while the service may still own the key.

    Taken by the NETWORKED settlement as well as by detach (#1683): authority to omit a key
    must not outlive retention, so no path may consume a carrier while the live section still
    holds a key that carrier claims. Without it, ordinary settlement opens exactly the
    service-present gap the reclaimer is being fixed for.
    """
    from nso_adapter.core.static_route_reader import certified_static_route_section
    from nso_adapter.nso.apply import static_route_entry_key

    section = await certified_static_route_section(client, device)
    if section.inconclusive:
        logger.warning("static_route.detach_proof_inconclusive", device_id=device.id)
        return False
    live = {static_route_entry_key(entry) for entry in section.routes}
    return not (live & set(out.authorized))


async def _sr_sync_from(client, device, result: dict, *, job_id: int) -> bool:
    """Re-align CDB with device truth after a ``no-networking`` commit. Two attempts.

    Unlike G11's unconditional success, a failure now FAILS the job: CDB keeps the locally
    applied reverse diff, so the detach is not proven and its tombstone must survive to be
    retried.
    """
    from nso_adapter.nso import actions

    for attempt in (1, 2):  # one retry — slow-session flake (sw03 read eof)
        try:
            await actions.sync_from(client, device.nso_device_name)
            return True
        except ClaimLostError:
            raise
        except Exception as exc:  # noqa: BLE001 — surfaced on the job below
            logger.warning(
                "removal.detach_sync_from_failed",
                job_id=job_id,
                device_id=device.id,
                attempt=attempt,
                error=repr(exc),
            )
    result["sync_from"] = "failed"
    return False


async def _sr_networked_proof(client, device, out: SrRemoval, result: dict):
    """Gather §4.4's evidence for a networked removal → ``(proven, residue_found, per_field)``.

    ONE certified device-state read serves both consumers: the residue check over the keys
    this job authorized, and §4.11's per-FIELD evidence for the clears it delivered. Key-grain
    proof does not imply field-grain proof — a route whose old ``metric 10`` is still live
    satisfies the key check completely — so a cleared field is only consumable once its own
    wire leaf is absent or neutral in that read.
    """
    from nso_adapter.core.apply import _static_route_device_state
    from nso_adapter.core.static_route_plan import leaf_is_neutral

    status, entries = await _static_route_device_state(client, device)
    # §4.4's set literally: `authorized − keys in the sent body`. The subtraction is empty by
    # construction today (the body IS current minus authorized), and it is written out anyway
    # so a future body that re-asserts an authorized key cannot report it as residue.
    consumed = set(out.authorized) - set(out.sent_keys)
    survivors = sorted(key for key in consumed if key in entries) if status == "ok" else []
    residue = None
    if consumed:
        residue = status if status != "ok" else ("found" if survivors else "clean")
        result["residue_check"] = residue
        if survivors:
            result["residue"] = {"route": [list(key) for key in survivors]}
            logger.error("removal.residue_found", device_id=device.id, scope="static_route", residue=result["residue"])
    else:
        # A pure-clear removal authorizes no key at all: nothing to look for, so say so
        # rather than reporting a clean bill nothing was checked against.
        result["residue_check"] = "unsupported"

    per_field: dict[int, tuple[str, ...]] = {}
    clears_ok = True
    for row_id, fields in out.clears.items():
        entry = entries.get(out.clear_keys[row_id]) if status == "ok" else None
        proven = tuple(f for f in fields if entry is not None and leaf_is_neutral(f, entry))
        per_field[row_id] = proven
        if len(proven) != len(fields):
            clears_ok = False
    if out.clears:
        result["pending_clear_proven"] = {str(row_id): list(fields) for row_id, fields in sorted(per_field.items())}
    keys_ok = residue is None or residue == "clean"
    service_clean = not out.authorized or await _sr_service_clean(client, device, out)
    if not service_clean:
        result["service_clean"] = False
    return (
        (keys_ok and clears_ok and service_clean and _sr_verify_ok(out)),
        residue == "found",
        per_field,
    )


async def _sr_consume(db: AsyncSession, device, out: SrRemoval, per_field: dict, result: dict, *, reg) -> None:
    """Delete the snapshotted tombstones and empty the proven carrier fields. Nothing else."""
    from nso_adapter.core.static_route_plan import AUTHORIZED, STORE_ONLY, triple_of
    from nso_adapter.store.models import StaticRouteIntent
    from nso_adapter.store.tombstone_store import delete_tombstones

    if out.tombstone_ids:
        if reg is None or not reg.registered:
            # G19: the delete is claim-token-scoped. Making it unguarded to keep a caller
            # convenient is exactly the shortcut §4.7 forbids.
            raise JobError(
                "removal_failed", "static_route removal: consuming tombstones needs a REGISTERED claim registration"
            )
        # Snapshotted ids only — a tombstone written during the network call survives,
        # because nothing has proven anything about it.
        await delete_tombstones(db, out.tombstone_ids, device_id=device.id, claim_token=reg.token)
        result["consumed_tombstones"] = list(out.tombstone_ids)

    for row_id, fields in per_field.items():
        if not fields:
            continue
        row = await db.get(StaticRouteIntent, row_id)
        if row is None or triple_of(row) != out.clear_keys[row_id]:
            continue
        carrier = row.pending_clear or {}
        # Both halves: they are disjoint by construction (an authorized clear promotes out of
        # store_only), so this only ever removes what this PUT actually delivered.
        remaining_auth = sorted({*(carrier.get(AUTHORIZED) or ())} - set(fields))
        remaining_store = sorted({*(carrier.get(STORE_ONLY) or ())} - set(fields))
        row.pending_clear = (
            {AUTHORIZED: remaining_auth, STORE_ONLY: remaining_store} if (remaining_auth or remaining_store) else None
        )
        logger.info("static_route.pending_clear_consumed", device_id=device.id, row_id=row_id, fields=list(fields))


async def _commit_terminal_removal(db: AsyncSession, job_id: int) -> None:
    """Commit the removal's terminal transaction under the three-state contract (§4.6).

    A COMMIT that raises may still have been applied, and the ordinary fallback — roll back,
    write ``failed`` in a second transaction — would then leave a consumed carrier under a
    failed job. Raising instead hands the decision to claim recovery, which re-dispositions
    only a job still ``running`` (G38).
    """
    from nso_adapter.core.claim import ClaimOutcome, _commit_outcome

    outcome = await _commit_outcome(db)
    if outcome is not ClaimOutcome.COMMIT_ACKNOWLEDGED:
        logger.error("removal.terminal_commit_outcome_unknown", job_id=job_id, outcome=outcome.value)
        raise BookkeepingOutcomeUnknown(f"removal job {job_id}: terminal commit outcome is {outcome.value}")


async def _finalize_static_route_removal(db, job_id: int, device, client, out: SrRemoval, *, reg) -> bool:
    """Prove the write, then consume and finalize in one claim-guarded transaction.

    The status-coupling rule is the most breakable invariant in R2: R1's sweeper only re-issues
    a tombstone whose owner is NULL or ``failed`` (G17), so a job that leaves a tombstone
    unconsumed must NOT end ``succeeded`` — the carrier would be stranded with no retry path.
    An Apply-promoted context is also a carrier: its attached generation and job context are
    the only durable retry obligation after enqueue consumes receipt provenance. A removal
    that owns none of these carriers keeps the apply-side leniency and records that it proved nothing.

    Returns whether the job ended ``succeeded``.
    """
    from nso_adapter.core.claim import ClaimRegistration, lock_claim, terminalize
    from nso_adapter.store.models import DeploymentGeneration, JobStatus

    result: dict = {"scope": "static_route", "removal_branch": out.branch}
    if out.authorized:
        result["authorized"] = [list(key) for key in sorted(out.authorized)]

    per_field: dict[int, tuple[str, ...]] = {}
    residue_found = False
    if out.branch == "superseded":
        # The selected plan claimed every authorized key, so there is nothing to prove.
        result["residue_check"] = "skipped_superseded"
        result["superseded"] = True
        proven = True
    elif out.branch == "detach":
        result["detach"] = True
        result["residue_check"] = "skipped_detach"
        service_clean = await _sr_service_clean(client, device, out)
        sync_ok = await _sr_sync_from(client, device, result, job_id=job_id)
        # "PUT 2xx OR the instance is absent": demanding a literal 2xx makes a crash between a
        # committed detach PUT and its bookkeeping commit permanently unprovable — every retry
        # sees no instance and could never satisfy the predicate.
        proven = service_clean and sync_ok and out.put_issued and _sr_verify_ok(out)
    else:
        proven, residue_found, per_field = await _sr_networked_proof(client, device, out, result)

    with db.no_autoflush:
        promotes_static_route = await db.scalar(
            select(
                exists().where(
                    DeploymentGeneration.job_id == job_id,
                    DeploymentGeneration.stream_revisions["static_route"].as_string().is_not(None),
                )
            )
        )
    promoted_context = bool(out.authorized) and bool(promotes_static_route)
    owns_carrier = bool(out.tombstone_ids) or bool(out.clears) or promoted_context
    consume = proven and not residue_found

    # The lock FIRST and held to COMMIT. no_autoflush is the lock ORDER: an ORM SELECT that
    # autoflushed a dirtied intent row would take row locks before the claim lock, the reverse
    # of the order every claimed writer uses.
    with db.no_autoflush:
        await lock_claim(db, reg if reg is not None else ClaimRegistration())
    if consume:
        await _sr_consume(db, device, out, per_field, result, reg=reg)

    status = JobStatus.failed
    error: dict | None = None
    if residue_found:
        error = {
            "code": "static_route_removal_residue_found",
            "message": f"static_route: removed route(s) {result['residue']['route']} are still on the device",
            "detail": {"scope": "static_route", "residue": result["residue"]},
        }
    elif proven or not owns_carrier:
        status = JobStatus.succeeded
        if not proven:
            result["unproven"] = True
            logger.warning(
                "static_route.removal_unproven",
                job_id=job_id,
                device_id=device.id,
                branch=out.branch,
                verify=out.verify,
                residue=result.get("residue_check"),
            )
    else:
        # Inconclusive, and this job owns a carrier. Succeeding would strand it forever.
        error = {
            "code": "static_route_removal_unproven",
            "message": "static_route: the removal could not be proven, so its carrier is retained for retry",
            "detail": {
                "scope": "static_route",
                "branch": out.branch,
                "verify": out.verify,
                "residue_check": result.get("residue_check"),
            },
        }
        logger.error(
            "static_route.removal_unproven_carrier_retained",
            job_id=job_id,
            device_id=device.id,
            branch=out.branch,
            verify=out.verify,
            residue=result.get("residue_check"),
        )
    write = await terminalize(
        db,
        job_id,
        status=status,
        expect=JobStatus.running,
        run_attempt=reg.run_attempt if reg is not None else None,
        result=result,
        error=error,
    )
    if write is None:
        # Another execution owns this job. The consumption in this transaction belongs to
        # that owner's decision, not ours: discard it rather than commit a consumed carrier
        # under a status we were refused.
        await db.rollback()
        return False
    await _commit_terminal_removal(db, job_id)
    return write.status is JobStatus.succeeded


async def _dispatch_scope(
    db: AsyncSession,
    device,
    client,
    scope: str,
    context: dict | None = None,
    *,
    job_id: int | None = None,
    reg=None,
):
    """Deploy one removal: the device's document, with *scope*'s removed rows omitted.

    Every family goes the same way now — there is one service and one PUT — so the only
    branch left is the one scope that owns durable carriers. *job_id* and *reg* are the
    running job's identity and its live claim registration: the static-route branches need
    the job id to tell their OWN tombstones from a sibling's, and a real registered claim to
    guard the transaction that consumes them.

    Returns :class:`SrRemoval` for ``static_route`` — what the write did, so the caller can
    prove it before consuming anything — and ``None`` for every other scope.
    """
    if scope not in valid_removal_scopes():
        raise ValueError(f"Unknown removal scope {scope!r}")
    return await _put_removal_document(db, device, client, scope, context, job_id=job_id)


def _refuse_force_incompatible(
    scope,
    marking,
    document,
    allowed_removal_keys,
    settlement_cohort,
    static_route_tombstone_ids,
    promotes,
    apply_attempt_id,
    frozen_fragments,
) -> None:
    """Refuse the arguments a reissue cannot honor: it promotes nothing and skips the guard."""
    if promotes:
        raise ValueError(f"a force-removal of {scope!r} promotes nothing; got {promotes!r}")
    if marking is not None:
        raise ValueError(f"a force-removal of {scope!r} carries no deletion marking; got {marking!r}")
    if document is not None:
        raise ValueError(f"a force-removal of {scope!r} composes its own reissue document; got one to deploy")
    if allowed_removal_keys is not None:
        raise ValueError(f"a force-removal of {scope!r} skips the collateral guard; got allowed removal keys")
    if settlement_cohort is not None:
        raise ValueError(
            f"a force-removal of {scope!r} settles no promoted revisions; got settlement cohort {settlement_cohort}"
        )
    if apply_attempt_id is not None:
        raise ValueError(f"a force-removal of {scope!r} carries no Apply attempt; got {apply_attempt_id}")
    if frozen_fragments:
        raise ValueError(f"a force-removal of {scope!r} promotes nothing; got frozen fragments to promote")
    if static_route_tombstone_ids:
        raise ValueError(
            f"a force-removal of {scope!r} selects no lifecycle carrier; got tombstone ids "
            f"{list(static_route_tombstone_ids)}"
        )


async def _record_pending_clears(
    db: AsyncSession,
    device_id: int,
    streams: tuple[str, ...],
    *,
    provenance: str,
) -> None:
    """Record pending clears without weakening an existing authorization."""
    from nso_adapter.store.models import DeviceProjectionStream, StreamPendingClear

    for stream in sorted(set(streams)):
        revision = await db.scalar(
            select(DeviceProjectionStream.desired_revision).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )
        if revision is None:
            raise RuntimeError(f"device {device_id} stream {stream!r} has no accepted write to record")
        # uq_stream_pending_clear: at most ONE row per (device, stream); its provenance is
        # the row's current standing, upgraded in place (store_only -> authorized).
        existing = (
            await db.execute(
                select(StreamPendingClear).where(
                    StreamPendingClear.device_id == device_id,
                    StreamPendingClear.stream == stream,
                )
            )
        ).scalar_one_or_none()

        if provenance == STORE_ONLY_PROVENANCE:
            if existing is None:
                db.add(
                    StreamPendingClear(
                        device_id=device_id,
                        stream=stream,
                        provenance=STORE_ONLY_PROVENANCE,
                        revision=revision,
                    )
                )
            elif existing.provenance == STORE_ONLY_PROVENANCE:
                existing.revision = max(existing.revision, revision)
            continue

        if existing is None:
            db.add(
                StreamPendingClear(
                    device_id=device_id,
                    stream=stream,
                    provenance=AUTHORIZED_PROVENANCE,
                    revision=revision,
                )
            )
        else:
            existing.provenance = AUTHORIZED_PROVENANCE
            existing.revision = max(revision, existing.revision)
    await db.flush()


async def _promote_parked_clears(
    db: AsyncSession,
    device_id: int,
    streams: tuple[str, ...],
) -> None:
    """Authorize parked rows for streams this authorized push re-asserts; never create."""
    from nso_adapter.store.models import DeviceProjectionStream, StreamPendingClear

    for stream in sorted(set(streams)):
        # uq_stream_pending_clear: at most ONE row per (device, stream).
        row = (
            await db.execute(
                select(StreamPendingClear).where(
                    StreamPendingClear.device_id == device_id,
                    StreamPendingClear.stream == stream,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            continue
        revision = await db.scalar(
            select(DeviceProjectionStream.desired_revision).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )
        row.provenance = AUTHORIZED_PROVENANCE
        row.revision = max(row.revision, revision or 0)
    await db.flush()


async def _pending_clears_discharged_by(
    db: AsyncSession,
    device_id: int,
    scope: str,
    promotes: tuple[str, ...],
    *,
    mode,
    force: bool,
) -> tuple[int, ...]:
    """Return the ids :func:`_settle_pending_clears_at_admission` will delete, in id order.

    Read BEFORE the generation is written so its operation plane can name the carriers it
    discharges; the same transaction holds the projection lock, so nothing moves in between.
    """
    from nso_adapter.core.projection import section_streams
    from nso_adapter.store.models import GenerationMode, StreamPendingClear

    if force:
        streams: tuple[str, ...] = section_streams(scope)
    elif scope != "static_route" and mode is GenerationMode.networked:
        streams = promotes
    else:
        return ()
    ids = (
        await db.execute(
            select(StreamPendingClear.id).where(
                StreamPendingClear.device_id == device_id,
                StreamPendingClear.stream.in_(streams),
            )
        )
    ).scalars()
    return tuple(sorted(ids.all()))


async def _settle_pending_clears_at_admission(
    db: AsyncSession,
    device_id: int,
    scope: str,
    promotes: tuple[str, ...],
    *,
    mode,
    force: bool,
) -> None:
    """Settle stream obligations for the carrier this admission created.

    Keyed off the admitted carrier's MODE, not the request's retract: a networked
    replace of the stream delivers every recorded omission (discharge); a detach
    re-asserts the omitting store state and authorizes a parked row (promote).
    """
    from nso_adapter.store.models import GenerationMode

    if force:
        await _discharge_pending_clears(db, device_id, scope)
    elif scope != "static_route":
        if mode is GenerationMode.networked:
            await _discharge_pending_clear_streams(db, device_id, promotes)
        else:
            await _promote_parked_clears(db, device_id, promotes)


async def _discharge_pending_clear_streams(
    db: AsyncSession,
    device_id: int,
    streams: tuple[str, ...],
) -> None:
    """Delete obligations whose clears have gained a networked carrier."""
    from nso_adapter.store.models import StreamPendingClear

    await db.execute(
        delete(StreamPendingClear).where(
            StreamPendingClear.device_id == device_id,
            StreamPendingClear.stream.in_(streams),
        )
    )


async def _discharge_pending_clears(db: AsyncSession, device_id: int, scope: str) -> None:
    """Discharge every stream the operator's force-removal flush affects."""
    from nso_adapter.core.projection import section_streams

    await _discharge_pending_clear_streams(db, device_id, section_streams(scope))


def _refuse_unmarked_deletion(scope: str, marking: str | None, *, deletes: bool, force: bool) -> None:
    """Refuse a deleting removal that carries no marking (#106); the force reissue is exempt."""
    if marking is None and deletes and not force:
        raise ValueError(f"an unmarked deletion of {scope!r} would commit networked instead of detaching")


def _refuse_deferred_delete_origin(scope: str, marking: str | None, *, retract: bool, defer_retract: bool) -> None:
    """Refuse a deferred retract on a delete-origin job (static_route defers by design).

    The job's networked generation would discharge the clear it just recorded as deferred.
    """
    if retract and defer_retract and scope != "static_route" and marking == DELETE_ORIGIN_MARKING:
        raise ValueError(f"a deferred retract of {scope!r} cannot ride a delete-origin removal job")


async def enqueue_removal(
    db: AsyncSession,
    device_id: int,
    scope: str,
    *,
    marking: str | None,
    defer_retract: bool,
    promotes: tuple[str, ...],
    settlement_cohort: int | None = None,
    interfaces: list[str] | None = None,
    removed: dict[str, list] | None = None,
    allowed_removal_keys: dict[str, dict[str, list]] | None = None,
    vault_refs: dict[str, str] | None = None,
    force: bool = False,
    retract: bool = False,
    shrank: bool = False,
    document: dict | None = None,
    static_route_tombstone_ids: tuple[int, ...] = (),
    apply_attempt_id: UUID | None = None,
    frozen_fragments: dict[str, dict] | None = None,
):
    """Queue an async ``removal`` job that PUT-replaces *scope*'s service.

    Non-blocking: the intent PUT returns immediately and the worker runs the
    (potentially slow) device commit in the background via :func:`run_removal`.

    *marking* is the deletion provenance of THIS job's rows and decides how the job
    commits: ``delete_origin`` retracts from the device, ``detach`` un-owns without touching
    it (#106), and ``None`` says the job deletes nothing at all (a pure cleared-scalar
    retract, which is never a detach; see *retract*). It is an ARGUMENT, never a read of
    ``?delete_origin``: one request can delete at both markings and then needs one job per
    marking, because ``detach`` is a job-wide dispatch switch (§4.5,
    :func:`enqueue_static_route_removals`).

    *defer_retract* is the whole-REQUEST fact that an un-own rides along. It cannot be
    derived from this job's own rows once a request produces several jobs, and it is what
    holds a clear back: one PUT-replace cannot both network a clear and leave an un-own
    off the device.

    *promotes* names the projection STREAMS this removal authorizes, and it is stated by the
    caller rather than derived from *scope*: two endpoints share the ``interface_config``
    scope and two share ``isis``, so promoting the whole section would authorize the sibling
    lane's un-promoted store-only state (#103). An endpoint passes its own delivery stream.
    *settlement_cohort* makes all promoted generations created by one request a settlement
    barrier. It stays NULL when the request creates only this generation.
    *force* names none — see below.
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

    *force* is also the one caller that PROMOTES NOTHING and carries NO marking. It
    re-deploys state an earlier push already authorized, with the collateral guard off, so
    its generation is a reissue (:func:`core.generation.create_reissue_generation`) carrying
    ``stream_revisions={}``: no store write stands behind it to authorize, and there is
    nothing for its settlement to certify. Promoting the family's streams instead
    authorized — and then marked applied — whatever un-promoted store-only state the sibling
    lanes held, for instances this job does not even send (``interface_config`` flushes
    exactly *interfaces*).

    *document* is the composed document the promoted generation deploys, stated by the caller
    that already built it. A reissue composes its own, so *force* refuses it here rather than
    dropping it silently. The same refusal applies to *apply_attempt_id*, *marking*,
    *promotes* and *frozen_fragments*.
    """
    from nso_adapter.core.generation import (
        create_generation,
        create_reissue_generation,
        lock_projection,
        require_attach_to_job,
    )
    from nso_adapter.core.jobs import create_dedicated_job
    from nso_adapter.core.request_flags import STORE_ONLY
    from nso_adapter.store.models import GenerationMode, JobType

    if scope not in valid_removal_scopes():
        raise ValueError(f"Unknown removal scope {scope!r}")
    if marking is not None and marking not in REMOVAL_MARKINGS:
        raise ValueError(f"Unknown removal marking {marking!r}")
    if force:
        _refuse_force_incompatible(
            scope,
            marking,
            document,
            allowed_removal_keys,
            settlement_cohort,
            static_route_tombstone_ids,
            promotes,
            apply_attempt_id,
            frozen_fragments,
        )
    store_only = STORE_ONLY.get()
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
    # carries no un-own retracts it. The frozen operation records the deferred clear.
    deletes = shrank or bool(context.get("removed"))
    _refuse_unmarked_deletion(scope, marking, deletes=deletes, force=force)
    _refuse_deferred_delete_origin(scope, marking, retract=retract, defer_retract=defer_retract)
    if retract and defer_retract:
        context["retract_deferred"] = True
        logger.warning("removal.retract_deferred", device_id=device_id, scope=scope)
    # Record only when this admission gives the clear no networked carrier. A pure clear
    # gets a networked job below and needs no durable pending-clear row.
    if retract and scope != "static_route" and (defer_retract or store_only):
        await _record_pending_clears(
            db,
            device_id,
            promotes,
            provenance=STORE_ONLY_PROVENANCE if store_only else AUTHORIZED_PROVENANCE,
        )
    if store_only and not force:
        logger.info("removal.skipped_store_only", device_id=device_id, scope=scope)
        return None
    if force:
        context["force"] = True
    elif marking == DETACH_MARKING and not (retract and not deletes):
        # Unmarked shrink = un-own ("NetBox stops governing"), NOT an object deletion:
        # detach — drop service governance without touching the device (#106). Only a
        # push the plugin marked ?delete_origin=true (a NetBox object DELETE), a cleared
        # owned scalar (#83, above) or the operator's force-removal may retract config
        # from the live device.
        context["detach"] = True
    # The mode is part of the generation's identity, so it is decided HERE, with the push
    # that authorized it, and frozen. A later job may not reinterpret an un-own as a
    # networked retraction (#106) or a marked deletion as a detach (the lost deletion).
    mode = GenerationMode.detach if context.get("detach") else GenerationMode.networked
    # The context rides the GENERATION either way, not only the job: a retry of a blocked
    # head has to rebuild a job that commits the same operation, down to the detach flag.
    #
    # The projection lock FIRST, and held to commit. The carriers this admission discharges
    # are selected here and named in the immutable operation plane, while the discharge below
    # deletes them by STREAM: a store-only clear committing between an unlocked selection and
    # the creation would be deleted by an operation that never named it, and its obligation
    # would be gone with no record that anything discharged it.
    await lock_projection(db, device_id)
    from nso_adapter.core.static_route_plan import scope_qualified

    discharged_clear_ids = await _pending_clears_discharged_by(db, device_id, scope, promotes, mode=mode, force=force)
    if force:
        generation = await create_reissue_generation(
            db,
            device_id,
            mode=mode,
            removal_context=context,
            discharged_clear_ids=discharged_clear_ids,
        )
    else:
        generation = await create_generation(
            db,
            device_id,
            streams=promotes,
            mode=mode,
            allowed_removal_keys=allowed_removal_keys
            if allowed_removal_keys is not None
            else scope_qualified(scope, context.get("removed")),
            document=document,
            removal_context=context,
            settlement_cohort=settlement_cohort,
            static_route_tombstone_ids=static_route_tombstone_ids,
            discharged_clear_ids=discharged_clear_ids,
            apply_attempt_id=apply_attempt_id,
            frozen_fragments=frozen_fragments,
        )
    job = await create_dedicated_job(db, device_id, JobType.removal, context=context)
    await require_attach_to_job(db, generation, job)
    await _settle_pending_clears_at_admission(db, device_id, scope, promotes, mode=mode, force=force)
    logger.info("removal.enqueued", device_id=device_id, scope=scope, job_id=job.id, marking=marking)
    return job


def _removal_scope(context: dict) -> str:
    scope = context.get("scope")
    if not isinstance(scope, str):
        raise ValueError("Removal job context has no string scope")
    return scope


class RemovalMarking(NamedTuple):
    """What one intent PUT's deletions are marked with, for a ``query_flag`` scope.

    ``defer_retract`` is the whole-request fact :func:`enqueue_removal` needs and cannot
    derive: an un-own rides along, so a cleared leaf cannot go out on this push.
    """

    marking: str
    defer_retract: bool


def query_flag_marking(*, deletes: bool) -> RemovalMarking:
    """Return the marking facts for a scope still marked by ``?delete_origin=`` (§4.5).

    ``?delete_origin`` marks the WHOLE request, so every row such a push deletes carries the
    same provenance and one job covers them all. Static routes mark PER OBJECT and group
    their deletions themselves; see :func:`enqueue_static_route_removals`.

    *deletes* says this push drops rows (or nested content) rather than only clearing a
    leaf; it is what makes an unmarked push an un-own.
    """
    marking = request_marking()
    return RemovalMarking(marking, deletes and marking == DETACH_MARKING)


#: Marking order when one push deletes at both: the networked retraction FIRST. The detach
#: commits no-networking and then runs ``sync-from``, which is slow and fails the job when it
#: cannot complete; a failed head blocks its successors (#1522 §H2), so ordering the detach
#: first would let a re-sync flake hold back the device write the operator actually asked for.
_MARKING_ORDER: tuple[str, ...] = (DELETE_ORIGIN_MARKING, DETACH_MARKING)


async def enqueue_static_route_removals(
    db: AsyncSession,
    device_id: int,
    *,
    promotes: tuple[str, ...],
    removed: dict[str, list],
    tombstones=(),
    retract: bool = False,
    settlement_cohort: int | None = None,
) -> list:
    """Queue ONE marking-homogeneous removal job per marking this push deleted at (§4.5).

    *removed* maps a marking to the route keys deleted with it, and *tombstones* are the
    carriers written for those keys. Each is stamped with the job that owns ITS marking, so
    a job's authority is exactly its own rows (its generation selects its tombstones by id).

    One job cannot carry both markings. ``detach`` is a job-wide dispatch switch: it decides
    whether the whole PUT-replace commits ``no-networking``, so a mixed job would either
    leave a delete-origin retraction off the device or play an un-own's reverse diff against
    it (#106). The jobs are ordered by :data:`_MARKING_ORDER` and each takes its own
    deployment generation, so the device's ordered chain runs them one at a time.

    A cleared leaf is one whole-request fact and rides the FIRST job only (two jobs
    delivering it would push the same retraction twice), and it is deferred whenever any of
    the request's rows is an un-own, exactly as an unsplit push defers it today.

    Every job promotes the same streams, because one store state stands behind them all.
    The caller supplies *settlement_cohort* when the request also creates an apply
    generation. A marking split allocates its own cohort when no request cohort was supplied.

    Returns the jobs in creation order, or ``[]`` on a store-only request (no device write,
    and no carrier was written either).
    """
    from nso_adapter.core.generation import allocate_settlement_cohort

    present = [marking for marking in _MARKING_ORDER if removed.get(marking)]
    if not present and not retract:
        return []
    if settlement_cohort is None and len(present) > 1:
        settlement_cohort = await allocate_settlement_cohort(db)
    jobs: dict = {}
    # A pure clear deletes nothing, so it carries no marking at all and is never a detach.
    for index, marking in enumerate(present or [None]):
        owned_tombstone_ids = tuple(tombstone.id for tombstone in tombstones if tombstone.marking == marking)
        job = await enqueue_removal(
            db,
            device_id,
            "static_route",
            marking=marking,
            defer_retract=DETACH_MARKING in present,
            promotes=promotes,
            settlement_cohort=settlement_cohort,
            removed=removed_map("static_route", removed[marking]) if marking is not None else None,
            retract=retract and index == 0,
            shrank=marking is not None,
            static_route_tombstone_ids=owned_tombstone_ids,
        )
        if job is None:
            # Store-only, and the flag is request-scoped: no job was created for any marking.
            return []
        jobs[marking] = job
    for tombstone in tombstones:
        owner = jobs.get(tombstone.marking)
        if owner is None:
            raise RuntimeError(
                f"static_route removal: carrier marked {tombstone.marking!r} has no job: "
                f"the markings passed were {sorted(removed)}"
            )
        tombstone.job_id = owner.id
    return list(jobs.values())


async def run_removal(job_id: int, device_id: int, reg=None) -> None:
    """Execute a queued ``removal`` job: PUT-replace the scope's reconciler service.

    Promoted generations execute their recorded document and classification. Reissues retain
    live-store execution with job and tombstone bookkeeping; a job carrying no generation is
    refused. A retry repeats the same selected operation after a restart.

    *reg* is the worker's live ``ClaimRegistration`` for this device. Physical continuity
    already existed (the worker holds the claim for the runner's whole lifetime); the
    token is what was missing, and R2's carrier-consuming writes need it to guard their
    own transactions and to authorize ``delete_tombstones``.
    """
    from nso_adapter.core.claim import terminalize
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import session
    from nso_adapter.store.models import Device, Job, JobStatus

    async with session() as db:
        row = (await db.execute(select(Job.id, Job.context).where(Job.id == job_id))).one_or_none()
        if row is None:
            return
        context = row.context or {}
        scope: str | None = None
        try:
            from nso_adapter.core.generation import executing_generation, execution_policy

            scope = _removal_scope(context)
            generation = await executing_generation(db, job_id)
            if generation is None:
                raise RuntimeError(f"removal job {job_id} carries no generation to deploy")
            policy = execution_policy(generation)
            context = policy.context
            scope = _removal_scope(context)
            device = await db.get(Device, device_id)
            if not device:
                raise ValueError(f"Device {device_id} not found")
            client = get_nso_client(device.nso_instance)
            detach = policy.no_networking
            outcome = await _dispatch_scope(db, device, client, scope, context, job_id=job_id, reg=reg)
            if isinstance(outcome, SrRemoval) and outcome.branch != "force":
                # R2 §4.4/§4.6: this write owns durable carriers, so its proof, its
                # consumption and its terminal status are one transaction of their own.
                # A `force` removal owns nothing and keeps the generic tail below.
                succeeded = await _finalize_static_route_removal(db, job_id, device, client, outcome, reg=reg)
                if succeeded:
                    await _enqueue_followup_sync(db, job_id, device_id)
                return
            result: dict = {"scope": scope}
            if detach:
                # Detach (#106): the device was deliberately untouched — the removed
                # keys are EXPECTED to remain (that is the point), so the residue
                # check is meaningless here. Re-align CDB with device truth: the
                # no-networking commit applied FASTMAP's reverse diff to CDB only.
                result["detach"] = True
                result["residue_check"] = "skipped_detach"
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
                            result["sync_from"] = "failed"
            else:
                await _record_residue(result, client, device, scope, context, job_id=job_id, device_id=device_id)
            if (
                await terminalize(
                    db,
                    job_id,
                    status=JobStatus.succeeded,
                    expect=JobStatus.running,
                    run_attempt=reg.run_attempt if reg is not None else None,
                    result=result,
                )
                is None
            ):
                await db.rollback()
                return
            await db.commit()
            await _enqueue_followup_sync(db, job_id, device_id)
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
                        "hint": (
                            "These service rows are not in the remaining intent and were not part of "
                            "this retraction. Re-accept them into intent to keep them, or re-run via "
                            "POST /devices/{id}/actions/force-removal to flush them deliberately."
                        ),
                    },
                },
                reg,
            )
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except BookkeepingOutcomeUnknown:
            # R2 §4.6: the terminal COMMIT may have landed. Writing `failed` here would flip a
            # job whose tombstone consumption and carrier updates are already committed —
            # exactly the torn state the single transaction exists to prevent. Recovery
            # re-dispositions only a job still `running` (G38).
            raise
        except Exception as exc:  # noqa: BLE001 — record on the job, never crash the worker
            from nso_adapter.core.jobs import _mark_job_failed

            logger.error("removal.failed", job_id=job_id, device_id=device_id, scope=scope, error=repr(exc))
            await _mark_job_failed(db, job_id, error_envelope(exc, code="removal_failed", detail={"scope": scope}), reg)


async def _enqueue_followup_sync(db: AsyncSession, job_id: int, device_id: int) -> None:
    """Option A follow-up: re-import any residue as an unowned mirror right away.

    After the terminal commit this job is no longer active, so the per-device dedup admits
    the sync; best-effort — the scheduler covers it if this loses a race.
    """
    try:
        from nso_adapter.core.jobs import enqueue_job
        from nso_adapter.store.models import JobType

        await enqueue_job(device_id, JobType.sync, db)
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except Exception as exc:  # noqa: BLE001 — never fail a committed removal on this
        logger.warning("removal.followup_sync_enqueue_failed", job_id=job_id, device_id=device_id, error=repr(exc))


# store family → the YANG list that carries the object in the route-policy service
_ROUTE_POLICY_FAMILY_LISTS: dict[str, str] = {
    "prefix_list": "prefix-list",
    "community_list": "community-list",
    "as_path": "as-path",
    "route_map": "route-map",
}


class PromotionRemovalContext(NamedTuple):
    """Existing removal-runner inputs derived from an immutable projection delta."""

    interfaces: list[str] | None
    removed: dict[str, list] | None
    vault_refs: dict[str, str] | None


class PromotionInterfaceUnresolved(ValueError):
    """A projection delta refers to an interface row that no longer exists."""


def _row_keys(rows: dict[str, list[dict]], table: str, *fields: str) -> list:
    values = []
    for row in rows.get(table, []):
        key = tuple(row.get(field) for field in fields)
        values.append(key[0] if len(key) == 1 else key)
    return sorted(set(values), key=str)


_PROMOTION_GUARD_FIELDS: dict[str, tuple[tuple[str, str, tuple[str, ...]], ...]] = {
    "vlan": (("vlan_intent", "vlan", ("vlan_id",)),),
    "bfd": (("bfd_intent", "interface", ("interface_name",)),),
    "svi": (("svi_intent", "interface", ("interface_name",)),),
    "subinterface": (("subinterface_intent", "interface", ("interface_name",)),),
    "interface_mtu": (("interface_mtu_intent", "interface", ("interface_name",)),),
    "logging": (("logging_host_intent", "host", ("address",)),),
    "l2_sap": (("l2_sap_intent", "sap", ("service_name", "sap_id")),),
    "static_route": (("static_route_intent", "route", ("vrf", "prefix", "next_hop")),),
    "snmp": (
        ("snmp_community_intent", "community", ("label",)),
        ("snmp_v3_user_intent", "v3-user", ("username",)),
        ("snmp_host_intent", "host", ("address",)),
    ),
    "bgp": (
        ("bgp_router_intent", "router", ("asn",)),
        ("bgp_peer_intent", "peer", ("peer_address",)),
    ),
    "ospf": (
        ("ospf_instance_intent", "process-config", ("process_id",)),
        ("ospf_interface_intent", "interface-config", ("interface_name",)),
    ),
    "isis": (
        ("isis_process_intent", "process-config", ("process_tag",)),
        ("isis_interface_intent", "interface-config", ("interface_name", "af")),
    ),
}


def interface_removal_keys(
    interfaces: Sequence[str],
    addresses: Sequence[Sequence[str]],
    attributes: Sequence[Sequence[str]] = (),
) -> dict[str, list]:
    """Return the keys an interface_config removal authorizes, in every grain that reads them.

    ``address`` is the VALUE grain :func:`_interface_config_residue` intersects with the
    export's ``ip/prefix-length`` strings. The document's own lists key an interface by its
    name and an address by its host part alone, so the collateral guard's grains are DERIVED
    from the same triples here rather than captured a second time somewhere else.
    """
    from nso_adapter.nso.apply import INTERFACE_ATTRIBUTE_LEAVES

    keys: dict[str, list] = {}
    for interface, attribute in attributes:
        if attribute in INTERFACE_ATTRIBUTE_LEAVES:
            keys.setdefault(attribute, []).append([str(interface)])
    if addresses:
        keys["address"] = [list(triple) for triple in addresses]
    for interface, address, _vrf in addresses:
        try:
            version = ipaddress.ip_interface(str(address)).version
        except ValueError:
            from nso_adapter.core.generation import ApplyUnexecutable

            raise ApplyUnexecutable({"ip": "invalid_stored_address"}) from None
        keys.setdefault(f"ipv{version}-address", []).append([str(interface), str(address).rsplit("/", 1)[0]])
    if interfaces:
        keys["interface"] = [[str(name)] for name in interfaces]
    return keys


async def _promotion_interface_context(
    db: AsyncSession,
    device_id: int,
    removed_rows: dict[str, list[dict]],
    replacement_rows: dict[str, list[dict]],
) -> tuple[list[str], dict[str, list]]:
    from nso_adapter.store.models import DbInterface

    all_rows = [
        row
        for tables in (removed_rows, replacement_rows)
        for rows in tables.values()
        for row in rows
        if isinstance(row.get("interface_id"), int)
    ]
    interface_ids = sorted({row["interface_id"] for row in all_rows})
    names_by_id: dict[int, str] = {}
    if interface_ids:
        names_by_id = dict(
            (
                await db.execute(
                    select(DbInterface.id, DbInterface.name).where(
                        DbInterface.device_id == device_id,
                        DbInterface.id.in_(interface_ids),
                    )
                )
            )
            .tuples()
            .all()
        )
    unresolved = sorted(set(interface_ids) - names_by_id.keys())
    if unresolved:
        raise PromotionInterfaceUnresolved(f"unresolved interface ids: {unresolved}")
    # Every integer interface id in all_rows is present in names_by_id.
    interfaces = sorted({names_by_id[row["interface_id"]] for row in all_rows})
    # Keep this filter for id-less rows, which all_rows excludes.
    address_keys = [
        (names_by_id[row["interface_id"]], row.get("address") or "", row.get("vrf") or "")
        for row in removed_rows.get("interface_ip_intent", [])
        if row.get("interface_id") in names_by_id
    ]
    # A REPLACEMENT interface keeps rows and so keeps its entry; only a removal may empty one.
    replaced = {
        names_by_id[row["interface_id"]]
        for rows in replacement_rows.values()
        for row in rows
        if row.get("interface_id") in names_by_id
    }
    emptied = sorted(
        {
            names_by_id[row["interface_id"]]
            for rows in removed_rows.values()
            for row in rows
            if row.get("interface_id") in names_by_id
        }
        - replaced
    )
    attribute_keys = [
        (names_by_id[row["interface_id"]], row["attribute"])
        for row in removed_rows.get("interface_intent", [])
        if row.get("interface_id") in names_by_id
    ]
    return interfaces, interface_removal_keys(emptied, sorted(set(address_keys)), sorted(set(attribute_keys)))


async def promotion_removal_context(
    db: AsyncSession,
    device_id: int,
    scope: str,
    removed_rows: dict[str, list[dict]],
    *,
    replacement_rows: dict[str, list[dict]] | None = None,
) -> PromotionRemovalContext:
    """Build the normal enqueue inputs for one selected projection stream.

    This is the Apply-side inverse of the existing intent endpoints. It maps store keys to
    the same guarded YANG labels and interface instance list those endpoints pass to
    :func:`enqueue_removal`. The runner therefore keeps sole ownership of guard, detach,
    residue, carrier, and proof behavior.
    """
    if scope not in valid_removal_scopes():  # pragma: no cover - caller validates first
        raise ValueError(f"Unknown removal scope {scope!r}")

    removed: dict[str, list] = {}
    interfaces: list[str] | None = None
    vault_refs: dict[str, str] | None = None

    for table, label, fields in _PROMOTION_GUARD_FIELDS.get(scope, ()):
        if keys := _row_keys(removed_rows, table, *fields):
            removed[label] = keys
    if scope == "route_policy":
        for row in removed_rows.get("route_policy_object_intent", []):
            family = row.get("family")
            family_list = _ROUTE_POLICY_FAMILY_LISTS.get(family) if isinstance(family, str) else None
            if family_list:
                removed.setdefault(family_list, []).append(row.get("name"))
    if scope == "snmp":
        refs = {
            row["label"]: row["vault_ref"]
            for row in removed_rows.get("snmp_community_intent", [])
            if row.get("label") and row.get("vault_ref")
        }
        vault_refs = refs or None
    if scope == "interface_config":
        interfaces, removed = await _promotion_interface_context(db, device_id, removed_rows, replacement_rows or {})

    return PromotionRemovalContext(interfaces, removed or None, vault_refs)


def removed_map(scope: str, removed) -> dict[str, list]:
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
    guard_lists = section_guard_lists(scope)
    if len(guard_lists) != 1:
        return {}
    return {guard_lists[0].label: list(removed)}


async def replace_on_removal(
    db: AsyncSession,
    device,
    removed,
    store_model,
    *,
    stream: str,
    retract: bool = False,
    settlement_cohort: int | None = None,
) -> bool:
    """Enqueue an async removal job for *store_model*'s scope.

    The scope is derived from ``store_model``. The caller states the endpoint's
    projection *stream* explicitly, so promotion authorization does not depend on
    reversing the model ownership map. *removed* contains the just-removed store keys. It is
    threaded into the job context so the collateral guard can tell the intended
    retraction from an orphaned service row. Returns True if a removal job was queued.

    *retract* says a CLEARED OWNED SCALAR caused (part of) this call — see
    :func:`is_cleared`. A clear is not a shrink: it removes no key, so a caller with
    nothing in *removed* must still get a job, and that job must actually reach the device
    rather than detaching (#106's default). Without this the nine simple scopes would
    detect a clear and still commit it ``no-networking`` — a no-op.

    Does NOT commit, and must be called BEFORE the caller's own commit (#1522 §G1): the
    shrink, the deployment generation that authorizes it and the job that carries it are one
    transaction. The previous shape — commit the deletes, then enqueue — had a crash
    boundary in between that left a mutation with nothing authorized to deploy it.
    """
    if not removed and not retract:
        return False
    scope = _section_by_model().get(store_model.__name__)
    if scope is None:
        logger.error("removal.unknown_model", model=store_model.__name__)
        return False
    from nso_adapter.core.projection import stream_section

    if stream_section(stream) != scope:
        raise ValueError(f"stream {stream!r} does not belong to removal scope {scope!r}")
    marks = query_flag_marking(deletes=bool(removed))
    job = await enqueue_removal(
        db,
        device.id,
        scope,
        marking=marks.marking,
        defer_retract=marks.defer_retract,
        promotes=(stream,),
        settlement_cohort=settlement_cohort,
        removed=removed_map(scope, removed) if removed else None,
        retract=retract,
        shrank=bool(removed),
    )
    return job is not None  # None = store-only request; the shrink stays store-side
