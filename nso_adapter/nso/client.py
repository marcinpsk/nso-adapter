# SPDX-License-Identifier: Apache-2.0
"""Async RESTCONF client for Cisco NSO."""

from __future__ import annotations

from urllib.parse import quote

import httpx
import structlog

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.nso.neds import _ned_oper_status
from nso_adapter.nso.neds import extract_ned_component as _extract_ned_component

logger = structlog.get_logger(__name__)


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

    async def get_lag_topology(self, device_name: str) -> dict | None:
        """Return the lag-topology entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no LAG data (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:lag-topology/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_svi(self, device_name: str) -> dict | None:
        """Return the svi entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no SVI/IRB data (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:svi/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_subinterface(self, device_name: str) -> dict | None:
        """Return the subinterface entry for *device_name* from the NSO oper-data.

        Returns None if the device has no dot1q subinterface data (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:subinterface/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_interface_mtu(self, device_name: str) -> dict | None:
        """Return the interface-mtu entry for *device_name* from the NSO oper-data (Phase 2b).

        Returns None if the device has no interface with an explicit MTU (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:interface-mtu/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_lag_config(self, device_name: str) -> dict | None:
        """Return the lag-config entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no LAG config (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:lag-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_vlan_database(self, device_name: str) -> dict | None:
        """Return the vlan-database entry for *device_name* (None on 404).."""
        url = f"{self._base}/restconf/data/network-state-export:vlan-database/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_switchport(self, device_name: str) -> dict | None:
        """Return the switchport entry for *device_name* (None on 404).."""
        url = f"{self._base}/restconf/data/network-state-export:switchport/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_interface_ips(self, device_name: str) -> dict | None:
        """Return the interface-ip entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no IP data (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:interface-ip/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_interface_attributes(self, device_name: str) -> dict | None:
        """Return the interface-attributes entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no interface-attribute data (404 or empty list).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:interface-attributes/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_snmp_config(self, device_name: str) -> dict | None:
        """Return the snmp-config entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no SNMP config (404).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:snmp-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_logging_config(self, device_name: str) -> dict | None:
        """Return the logging-config entry for *device_name* from the package oper-data.

        Returns None if the device has no logging config (404).
        """
        url = f"{self._base}/restconf/data/network-state-export:logging-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_static_routes(self, device_name: str) -> dict | None:
        """Return the static-route entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no static routes (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:static-route/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_l2_services(self, device_name: str) -> dict | None:
        """Return the l2-service entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no L2 services (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:l2-service/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_isis_interfaces(self, device_name: str) -> dict | None:
        """Return the isis-interface entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no IS-IS config (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:isis-interface/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

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

    async def get_bgp_config(self, device_name: str) -> dict | None:
        """Return the bgp-config entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no BGP config (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:bgp-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_route_policy(self, device_name: str) -> dict | None:
        """Return the route-policy entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no route-policy config (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:route-policy/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_ospf(self, device_name: str) -> dict | None:
        """Return the ospf-config entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no OSPF config (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:ospf-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_bfd_config(self, device_name: str) -> dict | None:
        """Return the bfd-config entry for *device_name* (per-interface BFD).

        Returns None if the device has no BFD config (404 or empty).
        """
        url = f"{self._base}/restconf/data/network-state-export:bfd-config/device={_url_key(device_name)}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

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
