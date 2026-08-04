# SPDX-License-Identifier: Apache-2.0
"""Async RESTCONF client for Cisco NSO."""

from __future__ import annotations

from typing import NamedTuple
from urllib.parse import quote

import httpx
import structlog

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.nso.neds import _ned_oper_status
from nso_adapter.nso.neds import extract_ned_component as _extract_ned_component

logger = structlog.get_logger(__name__)


class NsoExportUnavailableError(RuntimeError):
    """The network-state-export oper-data is not reachable — a READ FAILURE, not an empty device.

    Raised instead of returning None where the caller would otherwise treat the absence of data as
    "the operator removed this config" and DELETE the mirrored rows. The two are different facts,
    and conflating them is how a transient NSO blip wipes accepted state (see the route-policy
    reader's RoutePolicyReadError for the other half of this contract).
    """


class NsoReadContractError(RuntimeError):
    """A ``device-state-read`` action response that the server did not certify (READSEM 1328).

    The action's contract is a single ATOMIC, device-scoped snapshot whose every section carries a
    TERMINAL status (``ok|unsupported|error`` — never ``stale``/``not-ready``). A response that is
    non-atomic, echoes the wrong device, or carries a non-terminal/malformed section is a
    version-skew / proxy-garbage failure, NOT authoritative data. Raised so every consumer — the
    not-ready escalation, the atomic importer, and the apply/removal verifiers — abstains and KEEPS
    rows rather than materializing a fabricated section (an ok-empty one would wipe a pop family).
    """


# The only statuses a device-state-read action section may carry: the build is a terminal
# extraction (never the record-served facade's stale/not-ready). See _certify_device_state_output.
_TERMINAL_SECTION_STATUSES = frozenset({"ok", "unsupported", "error"})


class ServiceInstanceState(NamedTuple):
    """A CERTIFIED read of one reconciler service instance (#1396 R2 §4.4).

    ``status`` is ``present`` (``entry`` is the instance body), ``absent`` (a conclusive
    keyed 404 — the service really has no instance for this device) or ``inconclusive``
    (a 2xx the reader could not certify either way). ``inconclusive`` is never a proof and
    never an "absent": no destructive PUT may be built on it and nothing may be consumed.
    """

    status: str
    entry: dict | None

    @property
    def present(self) -> bool:
        return self.status == "present"

    @property
    def inconclusive(self) -> bool:
        return self.status == "inconclusive"


def _inconclusive(service_path: str, device_name: str, reason: str) -> ServiceInstanceState:
    logger.warning("nso.service_instance_inconclusive", service=service_path, device=device_name, reason=reason)
    return ServiceInstanceState("inconclusive", None)


def _certify_device_state_output(output: object, device_name: str, wire_families: list[str]) -> None:
    """Validate a ``device-state-read`` action response before any consumer trusts it (READSEM 1328).

    Raises :class:`NsoReadContractError` unless the server certified an ATOMIC, device-scoped
    snapshot whose every requested-and-present section is a dict with a TERMINAL status. Presence is
    NOT required here (a missing requested section is the caller's to interpret — the escalation
    reads it as read_error, the verifiers as a contract bug); this function only refuses a response
    that would be actively misleading if walked.
    """
    if not isinstance(output, dict) or output.get("atomic") is not True:
        raise NsoReadContractError(f"device-state-read for {device_name!r} did not certify an atomic snapshot")
    echoed = output.get("device-name")
    if echoed != device_name:
        raise NsoReadContractError(
            f"device-state-read echoed device {echoed!r}, expected {device_name!r} — refusing a "
            "version-skewed / wrong-device snapshot"
        )
    for wire in wire_families:
        section = output.get(wire)
        if section is None:
            continue
        if not isinstance(section, dict):
            raise NsoReadContractError(f"device-state-read section {wire!r} is not a dict")
        status = section.get("status")
        if status not in _TERMINAL_SECTION_STATUSES:
            raise NsoReadContractError(
                f"device-state-read section {wire!r} has non-terminal status {status!r} "
                f"(expected one of {sorted(_TERMINAL_SECTION_STATUSES)})"
            )


def _url_key(value: str) -> str:
    """Percent-encode a value for use as a RESTCONF list-key URL path segment (RFC 8040).

    Device names are operator-set free strings; one containing ``/ # ? % space =`` would
    otherwise corrupt the path (404 / wrong resource / swallowed query params).
    """
    return quote(str(value), safe="")


RESTCONF_HEADERS = {
    "Accept": "application/yang-data+json",
    "Content-Type": "application/yang-data+json",
}


class NsoClient:
    """Thin async wrapper around NSO RESTCONF API."""

    def __init__(self, instance: NsoInstanceConfig, username: str, password: str) -> None:
        self._base = instance.base_url.rstrip("/")
        self._instance = instance
        self._auth = (username, password)
        # Use provided CA bundle; fall back to system certs (True). Never disable by default.
        self._verify: str | bool = instance.ca_cert if instance.ca_cert else True
        self._timeout = 30.0
        # Actions (sync-from, compare-config, connect) may take up to 2 min on real devices
        self._action_timeout = 120.0
        # device-state-read extracts EVERY requested family inside one txid-bracketed CDB
        # build; the fleet whale (rc1: all-18 in 75.6s) needs ~2x headroom over the live
        # worst case, which the 120s action timeout does not give.
        self._device_state_read_timeout = 180.0
        # The record-served whole-device doc GET (grain b): serialization-only, but the
        # fleet whale is real - rc1 measured 14.4s/1.7MB live (2026-07-21). The blanket
        # 30s leaves no growth margin; 120s does.
        self._device_state_doc_timeout = 120.0

    def _client(self, timeout: float | None = None) -> httpx.AsyncClient:
        headers = dict(RESTCONF_HEADERS)
        if self._instance.host_header:
            headers["Host"] = self._instance.host_header
        return httpx.AsyncClient(
            auth=self._auth,
            headers=headers,
            verify=self._verify,
            timeout=timeout if timeout is not None else self._timeout,
        )

    async def list_devices(self) -> list[dict]:
        """Return list of device objects from tailf-ncs:devices.

        Uses a RESTCONF ``fields`` filter to fetch only inventory metadata
        (name, address, authgroup, NED type, admin-state) — NOT each device's
        full ``config`` subtree. The unfiltered query pulls every device's
        complete config and can take 30s+ on a real NSO with large devices.
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices"
        params = {"fields": "device(name;address;authgroup;device-type;state(admin-state))"}
        async with self._client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            device_list = data.get("tailf-ncs:devices", {}).get("device", [])
            return device_list if isinstance(device_list, list) else []

    async def list_ned_packages(self) -> list[dict]:
        """Return the installed NED packages from tailf-ncs:packages/package.

        Only packages that expose a ``ned`` component (cli/netconf/generic) are
        NEDs — service/application packages (our reconcilers, auth, observability)
        have ``application``/``callback`` components and are excluded. Each entry:
        {ned_id, package, version, oper_status, vendor, operating_systems,
        product_families}. Parsing is done in Python (robust against the nested
        component/ned/device choice that a RESTCONF ``fields`` filter handles poorly).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:packages/package"
        params = {"fields": "name;package-version;oper-status;component(name;ned)"}
        async with self._client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        packages = data.get("tailf-ncs:package") or data.get("package") or []
        if not isinstance(packages, list):
            return []
        out: list[dict] = []
        for pkg in packages:
            if not isinstance(pkg, dict):
                continue
            ned = _extract_ned_component(pkg.get("component"))
            if ned is None:
                continue  # not a NED package
            ned_id, device_meta = ned
            out.append(
                {
                    "ned_id": ned_id,
                    "package": pkg.get("name"),
                    "version": pkg.get("package-version"),
                    "oper_status": _ned_oper_status(pkg.get("oper-status")),
                    "vendor": device_meta.get("vendor"),
                    "operating_systems": device_meta.get("operating-system") or [],
                    "product_families": device_meta.get("product-family") or [],
                }
            )
        out.sort(key=lambda x: x["ned_id"] or "")
        return out

    async def get_device_config(self, device_name: str) -> dict:
        """Return the full config subtree for *device_name*.

        Returns an empty dict on 204 No Content (CDB not yet populated — run sync-from first).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}/config"
        async with self._client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            if resp.status_code == 204 or not resp.content:
                return {}
            data = resp.json()
            return data.get("tailf-ncs:config", data)

    async def get_device_config_subtree(self, device_name: str, subpath: str) -> dict | None:
        """Return one targeted subtree of *device_name*'s config mirror, or None.

        ``subpath`` is a module-qualified RESTCONF path fragment under the
        device's ``config`` node (e.g. ``tailf-ned-cisco-ios:snmp-server/community``).
        Targeted reads only — the unfiltered device config can be ~900 KB (see
        :meth:`get_device_ned_id`). None on 404 (path absent) or 204 (empty CDB
        mirror — run sync-from first).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}/config/{subpath}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            if resp.status_code == 204 or not resp.content:
                return None
            return resp.json()

    async def get_device_ned_id(self, device_name: str) -> str | None:
        """Return the NED ID for *device_name* or None.

        Uses a RESTCONF ``fields=device-type`` filter so NSO returns only the
        small device-type subtree. The unfiltered query pulls the device's
        entire config AND oper-data (e.g. live NETCONF notification replay
        logs) — on a real device that can be ~900 KB and is streamed without a
        Content-Length, so it can truncate mid-body and raise a JSONDecodeError.
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url, params={"fields": "device-type"})
            resp.raise_for_status()
            raw = resp.json().get("tailf-ncs:device", {})
            # NSO returns a list for keyed list entries
            dev = raw[0] if isinstance(raw, list) else raw
            # NED ID lives under device-type → {cli,netconf,generic} → ned-id
            device_type = dev.get("device-type", {})
            for key in ("cli", "netconf", "generic"):
                if key in device_type:
                    return device_type[key].get("ned-id")
            return None

    async def get_service_config(self, service_path: str, device_name: str) -> dict | None:
        """Return the device's current reconciler service instance, or None when absent.

        *service_path* is the service's RESTCONF data path (the apply module's
        ``*_SERVICE_PATH`` constants, e.g. ``/restconf/data/isis-reconciler:isis-config``).
        The removal collateral guard compares these rows — what a PUT-replace will
        RETRACT from the device — against the would-be replacement body before
        committing (the ra1 lo0 incident: orphaned service rows silently flushed).
        """
        url = f"{self._base}{service_path}={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            root = service_path.rsplit("/", 1)[-1]
            entries = data.get(root) or data.get(root.split(":", 1)[-1], [])
            return entries[0] if entries else None

    async def service_instance_state(self, service_path: str, device_name: str) -> ServiceInstanceState:
        """Read the service instance and CERTIFY the verdict — ``present``/``absent``/``inconclusive``.

        :meth:`get_service_config` cannot certify an absence (#1396 R2 §4.4): it returns ``None``
        both for a keyed 404 and for any 2xx whose parsed body lacks a recognized non-empty root —
        a malformed answer, a renamed root, an empty list. That conflation is safe where the read
        can only OMIT work, but a live-service-relative PUT body built from "looks empty" silently
        drops both the entries it had to retain and the collateral the guard had to see, and then
        verifies cleanly. So every pre-PUT static-route read takes this reader instead, and
        ``inconclusive`` means: no PUT, no consumption, no CAS.

        ``absent`` is ONLY a conclusive keyed 404. A transport error or a non-404 error status
        raises, as it does for every other read here.
        """
        url = f"{self._base}{service_path}={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return ServiceInstanceState("absent", None)
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception:
                data = None
            if not isinstance(data, dict):
                return _inconclusive(service_path, device_name, "unparseable body")
            root = service_path.rsplit("/", 1)[-1]
            entries = data.get(root)
            if entries is None:
                entries = data.get(root.split(":", 1)[-1])
            if not isinstance(entries, list) or len(entries) != 1:
                # A keyed GET answers with exactly its one instance. Zero is the empty-root
                # case; more than one means we are not reading what we asked for, and
                # picking [0] would compute retention and collateral from another device's
                # instance — a PUT that omits this device's real rows.
                got = len(entries) if isinstance(entries, list) else "no recognized root"
                return _inconclusive(service_path, device_name, f"expected one instance, got {got}")
            entry = entries[0]
            if not isinstance(entry, dict) or not entry:
                return _inconclusive(service_path, device_name, "empty instance entry")
            if entry.get("device") != device_name:
                return _inconclusive(service_path, device_name, f"instance echoes device {entry.get('device')!r}")
            return ServiceInstanceState("present", entry)

    # ── device-state envelope (READSEM S3) — status-declared per-family reads ─────────

    async def get_device_state_section(self, device_name: str, wire_family: str) -> dict | None:
        """GET one family's section from the device-state envelope.

        *wire_family* is the YANG section name (e.g. ``"ospf-config"``, ``"interface-ip"``).
        Returns the section dict — ``status`` (``ok|stale|unsupported|not-ready|error``) plus
        optional ``error-reason``/``last-updated`` and the family's list keys. RESTCONF omits
        empty lists, so an authoritative empty is ``status=ok`` with the list keys ABSENT —
        consumers must ``.get(key, [])``, never infer from key presence.

        A section on an EXISTING device always serves (the status leaf is always set), so a
        404 here can only mean the device is unknown to NSO — or the whole export is down.
        The ambiguity is resolved by probing the ``device-state`` container: alive → None
        (device genuinely absent); container 404 → raise :class:`NsoExportUnavailableError`.
        """
        base = f"{self._base}/restconf/data/network-state-export:device-state"
        async with self._client() as c:
            resp = await c.get(f"{base}/device={_url_key(device_name)}/{wire_family}")
            if resp.status_code == 404:
                # Liveness-only probe: ?depth=1 keeps NSO from serializing EVERY device x 18
                # sections just to answer "is the export alive" (only the status code is read,
                # so the depth-truncation trap does not apply).
                probe = await c.get(f"{base}?depth=1")
                if probe.status_code == 404:
                    raise NsoExportUnavailableError(
                        f"network-state-export:device-state is not exported by NSO — refusing to "
                        f"read {device_name!r}'s 404 as 'device absent'."
                    )
                probe.raise_for_status()
                return None
            resp.raise_for_status()
            data = resp.json()
            section = data.get(f"network-state-export:{wire_family}")
            if section is None:
                section = data.get(wire_family)
            return section if section is not None else {}

    async def get_device_state_doc(self, device_name: str) -> dict | None:
        """GET one device's WHOLE device-state envelope entry (all 18 family sections).

        The record-served projection (READSEM fetch grain b): warm, cheap, non-atomic by
        declared design. Same 404 disambiguation as :meth:`get_device_state_section`.
        """
        base = f"{self._base}/restconf/data/network-state-export:device-state"
        async with self._client(self._device_state_doc_timeout) as c:
            resp = await c.get(f"{base}/device={_url_key(device_name)}")
            if resp.status_code == 404:
                probe = await c.get(f"{base}?depth=1")  # liveness only — see get_device_state_section
                if probe.status_code == 404:
                    raise NsoExportUnavailableError(
                        f"network-state-export:device-state is not exported by NSO — refusing to "
                        f"read {device_name!r}'s 404 as 'device absent'."
                    )
                probe.raise_for_status()
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            # A 200 whose body lacks exactly this device is a MALFORMED response (truncated
            # doc, wrong namespace, proxy garbage) - never device absence. None is reserved
            # for the confirmed-404 branch above; classifying a bad 200 as absence would
            # clear every pop-policy family downstream (codex S3-R2 F3).
            if (
                not isinstance(entries, list)
                or len(entries) != 1
                or not isinstance(entries[0], dict)
                or entries[0].get("device-name") != device_name
            ):
                raise NsoExportUnavailableError(
                    f"device-state GET for {device_name!r} returned 200 with a malformed body"
                )
            return entries[0]

    async def run_device_state_read(
        self, device_name: str, wire_families: list[str], *, timeout: float | None = None
    ) -> dict:
        """POST the ``device-state-read run`` action — the on-demand ATOMIC multi-family read.

        Extracts every requested family inside ONE txid-bracketed CDB build and CAS-updates
        the export's state records (re-warming the envelope — the designed record-refresh
        path; the envelope itself never extracts). Returns the action output dict:
        ``atomic`` (always true on success — bracket exhaustion raises an action ERROR
        instead, so torn data is never returned), ``last-updated``, and one section per
        requested family with terminal status ``ok|unsupported|error`` (never ``not-ready``
        or ``stale``).

        Raises ``httpx.HTTPStatusError`` on an action error (bracket exhaustion, unknown
        device) — callers keep rows for every requested family.
        """
        url = f"{self._base}/restconf/data/network-state-export:device-state-read/run"
        body = {"network-state-export:input": {"device": device_name, "family": list(wire_families)}}
        async with self._client(timeout if timeout is not None else self._device_state_read_timeout) as c:
            resp = await c.post(url, json=body)
            resp.raise_for_status()
            data = resp.json()
            output = data.get("network-state-export:output")
            if output is None:
                output = data.get("output", {})
            _certify_device_state_output(output, device_name, wire_families)
            return output

    async def check_sync(self, device_name: str) -> bool:
        """Return True if the device is in-sync with NSO's internal CDB.

        Raises on an HTTP error (auth/unreachable/5xx) rather than reporting a false
        'out-of-sync' — a real error must be distinguishable from genuine drift. Runs on
        the action timeout (a check-sync round-trips to the device).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}/check-sync"
        async with self._client(self._action_timeout) as c:
            resp = await c.post(url)
            resp.raise_for_status()
            result = resp.json()
            return result.get("tailf-ncs:output", {}).get("result", "") == "in-sync"

    # ── Onboarding (write/action) — create the device node + bring it up ──────

    async def device_exists(self, device_name: str) -> bool:
        """Return True if a device with this name already exists in NSO."""
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url, params={"fields": "name"})
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
            return True

    async def create_device(
        self,
        device_name: str,
        address: str,
        ned_id: str,
        authgroup: str,
        *,
        ned_type: str = "cli",
        port: int | None = None,
    ) -> None:
        """Create (PUT) a device node: address, authgroup, device-type/ned-id, port.

        ``ned_type`` is the transport (cli/netconf/generic). The device is created
        in its default admin-state; the caller unlocks it explicitly afterwards.
        """
        entry: dict = {
            "name": device_name,
            "address": address,
            "authgroup": authgroup,
            "device-type": {ned_type: {"ned-id": ned_id}},
        }
        if port is not None:
            entry["port"] = port
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        # Device create can commit to the device; use the action timeout, not the blanket 30s.
        async with self._client(self._action_timeout) as c:
            resp = await c.put(url, json={"tailf-ncs:device": [entry]})
            resp.raise_for_status()

    async def set_admin_state(self, device_name: str, admin_state: str = "unlocked") -> None:
        """PATCH the device's admin-state (e.g. ``unlocked``)."""
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        body = {"tailf-ncs:device": [{"name": device_name, "state": {"admin-state": admin_state}}]}
        async with self._client(self._action_timeout) as c:
            resp = await c.patch(url, json=body)
            resp.raise_for_status()

    async def _device_action(self, device_name: str, action: str) -> dict:
        """POST a device action (e.g. ``ssh/fetch-host-keys``, ``sync-from``)."""
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}/{action}"
        async with self._client(self._action_timeout) as c:
            resp = await c.post(url)
            resp.raise_for_status()
            return resp.json() if resp.content else {}

    async def fetch_host_keys(self, device_name: str) -> dict:
        """Fetch (TOFU-trust) the device's SSH host keys. Returns the action output.

        NSO returns HTTP 200 even when the fetch fails to negotiate an SSH
        connection (``result: failed`` with no ``fingerprint``).  That must be
        surfaced as an error — otherwise onboarding records a successful
        key-fetch step while no host key was actually stored, and the first real
        connect then fails with "could not verify host key".  Mirrors the
        result-checking ``sync_from`` already does.
        """
        out = await self._device_action(device_name, "ssh/fetch-host-keys")
        body = out.get("tailf-ncs:output", {}) if isinstance(out, dict) else {}
        result = body.get("result")
        if result not in ("updated", "unchanged") or not body.get("fingerprint"):
            info = body.get("info") or body.get("error") or ""
            raise RuntimeError(
                f"fetch-host-keys for {device_name!r} did not store a key "
                f"(result={result!r}){f': {info}' if info else ''}"
            )
        return out

    async def sync_from(self, device_name: str) -> bool:
        """Pull the device's running config into NSO's CDB. Returns the result bool."""
        out = await self._device_action(device_name, "sync-from")
        return bool(out.get("tailf-ncs:output", {}).get("result", False))

    # ── Management-address failover (read/write/action) ───────────────────────

    async def get_address(self, device_name: str) -> str | None:
        """Return the device's configured management address, or None if absent.

        Uses a ``fields=address`` filter so NSO returns only the address leaf (see
        get_device_ned_id for why the unfiltered device query is unsafe).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url, params={"fields": "address"})
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            raw = resp.json().get("tailf-ncs:device", {})
            dev = raw[0] if isinstance(raw, list) else raw
            return dev.get("address")

    async def set_address(self, device_name: str, address: str, port: int | None = None) -> None:
        """PATCH the device's management address (and optional port).

        Mirrors set_admin_state: a list-wrapped device node touching only the address
        (+ port) leaf, leaving the rest of the device untouched.
        """
        entry: dict = {"name": device_name, "address": address}
        if port is not None:
            entry["port"] = port
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={_url_key(device_name)}"
        # Mgmt-address change commits to the device (failover path); use the action timeout.
        async with self._client(self._action_timeout) as c:
            resp = await c.patch(url, json={"tailf-ncs:device": [entry]})
            resp.raise_for_status()

    async def disconnect(self, device_name: str) -> dict:
        """POST disconnect — drop NSO's cached management session for the device.

        After changing a device's address, NSO keeps its live session pinned to the OLD
        address; disconnecting forces NSO to dial the new address on the next connect.
        """
        return await self._device_action(device_name, "disconnect")
