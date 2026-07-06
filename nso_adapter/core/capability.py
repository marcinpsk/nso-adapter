# SPDX-License-Identifier: Apache-2.0
"""Route-policy capability matrix — populate + query the device_capability cache.

The cache (keyed by ``(ned_id, sw_version)``) lets the plugin flag, at attach time,
which parts of a route-map / community-list won't apply on a device — instead of the
operator finding out only when it silently didn't land. Three sources feed the matrix:

  - representable (``source='probe'``) — from the NSO ``capability-probe`` action (what
    the reconciler can model/send for this NED);
  - accepted (``source='apply'``) — from a real device-parser rejection at commit (what
    the box actually takes). Apply wins over probe.
  - readable (``source='read'``) — per-scope read-support rows fed by an external read
    probe (the vendor-test harness read matrix), keyed ``(scope, name='read')`` so they
    never collide with the probe/apply construct rows. This is the only signal a
    brand-new NED emits before any apply has ever run.
"""

from __future__ import annotations

import re

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso import actions
from nso_adapter.nso.client import NsoClient
from nso_adapter.store import models as m

logger = structlog.get_logger(__name__)

# The READ half rows are keyed (scope, name=READ_ROW_NAME) so they can never collide with
# the coarse apply rows (name == scope) or the route-policy construct rows.
READ_ROW_NAME = "read"
# native = surface returned real config · unsupported = read raised / expected config missing ·
# skipped = scope not applicable on this platform · unknown = read empty with no belief
# (no config or no reader — probed, but carries no verdict).
_READ_STATUSES = frozenset({"native", "unsupported", "skipped", "unknown"})
_READ_DEFINITE = frozenset({"native", "unsupported", "skipped"})


async def _upsert(
    db: AsyncSession, ned_id: str, sw_version: str, scope: str, name: str, status: str, detail: str, source: str
) -> None:
    row = (
        await db.execute(
            select(m.DeviceCapability).where(
                m.DeviceCapability.ned_id == ned_id,
                m.DeviceCapability.sw_version == sw_version,
                m.DeviceCapability.scope == scope,
                m.DeviceCapability.name == name,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        db.add(
            m.DeviceCapability(
                ned_id=ned_id,
                sw_version=sw_version,
                scope=scope,
                name=name,
                status=status,
                detail=detail,
                source=source,
            )
        )
        return
    # An apply-sourced 'unsupported' is authoritative (a real device rejection) — a later
    # representable probe must NOT downgrade it back to native.
    if row.source == "apply" and row.status == "unsupported" and source == "probe":
        return
    # A no-information read observation ('unknown': the surface read empty on a device with
    # no belief) must not clobber a definite read verdict learned from another device that
    # HAS the config on the same (ned, sw) key.
    if source == "read" and status == "unknown" and row.source == "read" and row.status in _READ_DEFINITE:
        return
    row.status, row.detail, row.source = status, detail, source


def _clean_capability_key(value) -> str:
    """Normalise a ``(ned_id | sw_version)`` key value.

    The literal string ``"None"`` (an unselected ``device_type.cli`` / absent platform
    stringified to a truthy value) must never become a real cache key.
    """
    s = str(value or "").strip()
    return "" if s == "None" else s


async def record_probe_capability(db: AsyncSession, ned_id: str, sw_version: str, elements) -> int:
    """Store the representable-half verdict (list of ``{scope,name,status,detail}``)."""
    # RESTCONF may render a singleton YANG list as a bare object — coerce so we never
    # iterate a dict's string keys (which would AttributeError on ``el.get``).
    if isinstance(elements, dict):
        elements = [elements]
    count = 0
    for el in elements:
        if not isinstance(el, dict):
            continue
        await _upsert(
            db,
            ned_id,
            sw_version,
            str(el.get("scope", "")),
            str(el.get("name", "")),
            str(el.get("status", "")),
            str(el.get("detail", ""))[:256],
            "probe",
        )
        count += 1
    await db.commit()
    return count


async def record_read_capability(db: AsyncSession, ned_id: str, sw_version: str, elements) -> int:
    """Store the READ-half verdict — per-scope read-support fed by an external read probe.

    *elements* is a list of ``{scope, status, detail}`` observed by the vendor-test harness
    read matrix (statuses per ``_READ_STATUSES``). Rows land as ``(scope, name='read')``
    with ``source='read'``, distinct from the probe/apply construct rows, so read-support
    and write-rejection facts coexist for the same scope. Invalid elements (empty scope,
    non-read status) are skipped; returns the number of rows recorded. ``_upsert`` keeps a
    definite read verdict from being downgraded by a later no-information ``unknown``.
    """
    if isinstance(elements, dict):
        elements = [elements]
    count = 0
    for el in elements:
        if not isinstance(el, dict):
            continue
        scope = str(el.get("scope", "")).strip()
        status = str(el.get("status", "")).strip()
        if not scope or status not in _READ_STATUSES:
            continue
        await _upsert(db, ned_id, sw_version, scope, READ_ROW_NAME, status, str(el.get("detail", ""))[:256], "read")
        count += 1
    await db.commit()
    return count


async def record_capability_rejection(
    db: AsyncSession, ned_id: str, sw_version: str, scope: str, name: str, detail: str
) -> None:
    """Record an accepted-half rejection (device parser refused it at commit). Apply wins."""
    if not ned_id:
        return
    await _upsert(db, ned_id, sw_version, scope, name, "unsupported", str(detail)[:256], "apply")
    await db.commit()


async def clear_capability_rejections(db: AsyncSession, ned_id: str, sw_version: str, scopes) -> int:
    """Clear reactive (apply-sourced) rejections for scopes that just committed cleanly.

    A successful device commit is the strongest positive signal — stronger than a probe (which
    :func:`_upsert` deliberately cannot use to downgrade an apply-rejection). Without this, a
    scope rejected once would stay ``unsupported`` forever, even after the device is fixed /
    upgraded and the intent lands. Only the coarse generic rows (``name == scope``, as written by
    ``core/apply.py`` ``_record_atomic_capability``) for the applied scopes are removed;
    route-policy fine-grained constructs (scope ``rm-set`` / ``rm-match`` / ``community``) are
    never touched — a clean ``route_policy`` apply does not prove any specific construct was
    present. Returns the number of rows deleted.
    """
    if not ned_id:
        return 0
    scope_set = {str(s) for s in scopes}
    if not scope_set:
        return 0
    rows = (
        (
            await db.execute(
                select(m.DeviceCapability).where(
                    m.DeviceCapability.ned_id == ned_id,
                    m.DeviceCapability.sw_version == sw_version,
                    m.DeviceCapability.source == "apply",
                )
            )
        )
        .scalars()
        .all()
    )
    cleared = 0
    for row in rows:
        if row.scope in scope_set and row.name == row.scope and row.status in ("unsupported", "skipped"):
            await db.delete(row)
            cleared += 1
    if cleared:
        await db.commit()
    return cleared


async def get_device_capability(db: AsyncSession, ned_id: str, sw_version: str) -> list[m.DeviceCapability]:
    """All cached capability rows for a ``(ned_id, sw_version)`` key."""
    return list(
        (
            await db.execute(
                select(m.DeviceCapability).where(
                    m.DeviceCapability.ned_id == ned_id,
                    m.DeviceCapability.sw_version == sw_version,
                )
            )
        )
        .scalars()
        .all()
    )


async def refresh_device_capability(
    db: AsyncSession, nso_client: NsoClient, device_name: str, device: m.Device | None = None
) -> dict:
    """Invoke the NSO capability-probe action for a device and store the representable half.

    Also persists the learned ``(ned_id, sw_version)`` onto the device row (when *device*
    is given) so a later read can resolve the key without re-probing.

    Returns ``{ned_id, sw_version, count}`` (or ``{}`` when the probe reports no NED).
    """
    out = await actions.capability_probe(nso_client, device_name)
    # Both keys defended against the literal 'None' (an unselected device_type.cli / absent
    # platform stringifies to a truthy "None"); neither may become a bogus cache key.
    ned_id = _clean_capability_key(out.get("ned-id"))
    sw_version = _clean_capability_key(out.get("sw-version"))
    if not ned_id:
        logger.debug("capability.refresh.no_ned", device=device_name)
        return {}
    elements = out.get("element", []) or []
    count = await record_probe_capability(db, ned_id, sw_version, elements)
    if device is not None and (device.ned_id != ned_id or device.sw_version != sw_version):
        device.ned_id, device.sw_version = ned_id, sw_version
        await db.commit()
    logger.info("capability.refresh.done", device=device_name, ned_id=ned_id, sw_version=sw_version, elements=count)
    return {"ned_id": ned_id, "sw_version": sw_version, "count": count}


async def resolve_capability_key(db: AsyncSession, nso_client: NsoClient, device: m.Device, *, refresh: bool) -> dict:
    """Resolve a device's ``(ned_id, sw_version)`` capability key.

    With ``refresh=True`` (the authoritative path — "check now" / attach) this probes NSO
    and persists the learned key. With ``refresh=False`` (the cheap panel-read path) it uses
    the key already stored on the device row, returning ``{}`` when none has been learned yet
    (so the caller reports an honest "unknown — check now" instead of blocking).
    """
    if refresh:
        return await refresh_device_capability(db, nso_client, device.nso_device_name, device)
    # ned_id alone identifies the key — sw_version may legitimately be "" (NEDs like Nokia timos
    # report no version, and their matrix rows are stored under sw_version=""). Requiring a
    # non-empty sw_version here left those devices reading 'unknown' forever even after a probe.
    if device.ned_id:
        return {"ned_id": device.ned_id, "sw_version": device.sw_version or "", "count": 0}
    return {}


# ── Preflight: check an attach against the cached matrix ──────────────────────

_RANK = {"native": 0, "translated": 1, "skipped": 2, "unsupported": 3}
_RT_KW = {"target", "rt", "route-target"}
_SOO_KW = {"origin", "soo", "route-origin"}
_REGEX_META = frozenset(".[]()?+^$|\\*")

# set-/match-json key → candidate matrix construct names (probe uses the container name;
# the apply hook may record a finer 'set extcommunity color', so list both).
_SET_KEY_CONSTRUCTS = {
    "community": ["set community"],
    "community_additive": ["set community"],
    "extcommunity_rt": ["set extcommunity"],
    "extcommunity_soo": ["set extcommunity"],
    "extcommunity_color": ["set extcommunity color", "set extcommunity"],
    "comm_list_delete": ["set comm-list delete"],
    "extcomm_list_delete": ["set extcomm-list delete"],
    "metric_type": ["set metric-type"],
    "tag": ["set tag"],
    "level": ["set level"],
    "large_community": ["set large-community"],
}
_MATCH_KEY_CONSTRUCTS = {
    "route_type": ["match route-type"],
    "local_preference": ["match local-preference"],
    "length": ["match length"],
    "large_community": ["match large-community"],
}


def _community_kind(member: str) -> str:
    """Coarse kind of a community member — the granularity the matrix groups by."""
    # NB: not `m` — that is the module-level `import models as m` alias; shadowing it here
    # would silently break any future `m.<Model>` reference in this function.
    member_str = str(member).strip()
    head, sep, _rest = member_str.partition(":")
    keyword = head.lower() if sep and head and not head[0].isdigit() else None
    if keyword in _RT_KW:
        return "rt"
    if keyword in _SOO_KW:
        return "soo"
    if keyword in ("large", "color", "bandwidth", "encapsulation"):
        return keyword
    return "regex" if any(c in _REGEX_META for c in member_str) else "standard"


def _index_rows(rows):
    """Build (community kind→(status,detail), construct (scope,name)→(status,detail)) maps.

    Community rows collapse to their KIND, keeping the worst (highest-ranked) status.
    """
    kind: dict[str, tuple[str, str]] = {}
    construct: dict[tuple[str, str], tuple[str, str]] = {}
    for r in rows:
        if r.scope == "community":
            k = _community_kind(r.name)
            cur = kind.get(k)
            if cur is None or _RANK.get(r.status, 0) > _RANK.get(cur[0], 0):
                kind[k] = (r.status, r.detail)
        else:
            construct[(r.scope, r.name)] = (r.status, r.detail)
    return kind, construct


def _check_constructs(keys, mapping, scope, construct):
    """Flag set/match keys whose best matching construct row is skipped/unsupported."""
    out = []
    for key in keys:
        best = None
        for name in mapping.get(key, []):
            got = construct.get((scope, name))
            if got and _RANK.get(got[0], 0) >= _RANK.get((best or (name, "native", ""))[1], 0):
                best = (name, got[0], got[1])
        if best and best[1] in ("skipped", "unsupported"):
            out.append({"scope": scope, "element": best[0], "status": best[1], "detail": best[2]})
    return out


def coverage_unknown(rows) -> bool:
    """Return True when the probe has not assessed this NED's route-policy at all.

    The probe emits a ``('coverage', <ned>, 'unknown')`` marker for NEDs it doesn't yet
    classify (Junos / Nokia / unknown). It means "not assessed", NOT "all supported" — so
    the consumer shows "not assessed" instead of green, and never blocks an attach on it.
    """
    return any(r.scope == "coverage" and r.status == "unknown" for r in rows)


def _check_aspath_names(aspath_names, construct):
    """Flag as-path lists whose name has no home on this NED.

    Driven by the probe's ``('as-path', 'named-list', 'unsupported')`` row: where it is
    present (IOS — as-path access-lists are numbered 1-500, the name IS the number), an
    as-path whose name is not a 1-500 number can't apply. NEDs that support named as-path
    lists (IOS-XR / Junos / Nokia) carry no such row, so nothing is flagged.
    """
    rule = construct.get(("as-path", "named-list"))
    if not (rule and rule[0] == "unsupported"):
        return []
    out = []
    for raw in aspath_names:
        name = str(raw).strip()
        if not (name.isdigit() and 1 <= int(name) <= 500):
            out.append({"scope": "as-path", "element": name, "status": "unsupported", "detail": rule[1]})
    return out


def preflight(rows, community_members=(), set_keys=(), match_keys=(), aspath_names=()) -> dict:
    """Check requested elements against cached capability *rows* for one ``(ned, sw)``.

    Returns ``{fully_supported, unsupported:[{scope,element,status,detail}], coverage_unknown}``.
    A community member is matched by KIND (so ``color:0:200`` inherits the verdict probed for
    any color); a set/match key maps to its construct name(s); an as-path name is checked for a
    NED-numeric requirement. ``status`` in (skipped, unsupported) flags. ``coverage_unknown`` is
    a SEPARATE signal: when the NED is unassessed it stays ``fully_supported`` (block only on a
    known-negative) but the consumer shows "not assessed" rather than claiming full support.
    """
    kind, construct = _index_rows(rows)
    unsupported = []
    for member in community_members:
        status, detail = kind.get(_community_kind(member), ("native", ""))
        if status in ("skipped", "unsupported"):
            unsupported.append({"scope": "community", "element": str(member), "status": status, "detail": detail})
    unsupported += _check_constructs(set_keys, _SET_KEY_CONSTRUCTS, "rm-set", construct)
    unsupported += _check_constructs(match_keys, _MATCH_KEY_CONSTRUCTS, "rm-match", construct)
    unsupported += _check_aspath_names(aspath_names, construct)
    return {
        "fully_supported": not unsupported,
        "unsupported": unsupported,
        "coverage_unknown": coverage_unknown(rows),
    }


def preflight_scopes(rows, scopes) -> dict:
    """Check a set of about-to-apply scope names against the capability matrix.

    The generic analog of :func:`preflight` (which is route-policy-construct granular).
    A scope the matrix marks ``unsupported``/``skipped`` — recorded reactively when a prior
    apply's per-scope dry-run was rejected by the NED (see ``core/apply.py``
    ``_record_atomic_capability``) — is flagged so the plugin can warn before a device write.
    Definite READ gaps (``(scope, 'read')`` unsupported/skipped) participate deliberately:
    no reader for a scope on this NED strongly implies no writer either (per-NED handler
    pairs), so the operator is warned before the write fails loudly. A read ``unknown``
    carries no verdict and stays fail-open. The plugin passes the scopes from its apply
    diff. Returns ``{fully_supported, unsupported:[{scope,name,status,detail}]}``.
    """
    requested = {str(s) for s in scopes}
    unsupported = [
        {"scope": r.scope, "name": r.name, "status": r.status, "detail": r.detail}
        for r in rows
        if r.scope in requested and r.status in ("skipped", "unsupported")
    ]
    return {"fully_supported": not unsupported, "unsupported": unsupported}


# Rejected-command → (scope, construct-name) for the accepted half. Names match the
# preflight candidate names so a later preflight surfaces the device's real rejection.
_REJECTION_CONSTRUCTS = (
    ("rm-set", "set extcommunity color", "set extcommunity color"),
    ("rm-set", "set extcommunity", "set extcommunity"),
    ("rm-set", "set comm-list delete", "set comm-list"),
    ("rm-set", "set extcomm-list delete", "set extcomm-list"),
    ("rm-set", "set metric-type", "set metric-type"),
    ("rm-set", "set large-community", "set large-community"),
    ("rm-set", "set tag", "set tag"),
    ("rm-set", "set level", "set level"),
    ("rm-match", "match route-type", "match route-type"),
    ("rm-match", "match local-preference", "match local-preference"),
    ("rm-match", "match length", "match length"),
    ("rm-match", "match as-path", "match as-path"),
    ("community", "ip large-community-list", "ip large-community-list"),
)


def parse_rejected_construct(message: str):
    """Map a device-parser rejection error → ``(scope, construct-name)`` (or ``(None, None)``).

    Pulls the offending ``command: <cmd>`` from the NED error and normalises it to a known
    construct so the accepted-half rejection matches what the preflight checks.
    """
    # Capture the command up to end-of-line OR end-of-string, so a rejection whose command
    # is the final line (no trailing newline) still parses (`.` excludes newline by default).
    match = re.search(r"command:\s*(.+)", message or "")
    cmd = (match.group(1) if match else "").strip()
    if not cmd:
        return None, None
    low = cmd.lower()
    for scope, name, prefix in _REJECTION_CONSTRUCTS:
        if low.startswith(prefix):
            return scope, name
    if low.startswith("set "):
        return "rm-set", " ".join(cmd.split()[:3])
    if low.startswith("match "):
        return "rm-match", " ".join(cmd.split()[:3])
    if "community-list" in low:
        return "community", " ".join(cmd.split()[:3])
    return None, None
