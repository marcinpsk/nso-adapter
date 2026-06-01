# SPDX-License-Identifier: Apache-2.0
"""Async RESTCONF client for Cisco NSO."""

from __future__ import annotations

import httpx
import structlog

from nso_adapter.config import NsoInstanceConfig

logger = structlog.get_logger(__name__)

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

    async def get_device_config(self, device_name: str) -> dict:
        """Return the full config subtree for *device_name*.

        Returns an empty dict on 204 No Content (CDB not yet populated — run sync-from first).
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={device_name}/config"
        async with self._client() as c:
            resp = await c.get(url)
            resp.raise_for_status()
            if resp.status_code == 204 or not resp.content:
                return {}
            data = resp.json()
            return data.get("tailf-ncs:config", data)

    async def get_device_ned_id(self, device_name: str) -> str | None:
        """Return the NED ID for *device_name* or None.

        Uses a RESTCONF ``fields=device-type`` filter so NSO returns only the
        small device-type subtree. The unfiltered query pulls the device's
        entire config AND oper-data (e.g. live NETCONF notification replay
        logs) — on a real device that can be ~900 KB and is streamed without a
        Content-Length, so it can truncate mid-body and raise a JSONDecodeError.
        """
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:lag-topology/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:interface-ip/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:interface-attributes/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:snmp-config/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:static-route/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:isis-interface/device={device_name}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def get_bgp_config(self, device_name: str) -> dict | None:
        """Return the bgp-config entry for *device_name* from the NSO package oper-data.

        Returns None if the device has no BGP config (404 or empty).
        Raises httpx.HTTPStatusError on other errors.
        """
        url = f"{self._base}/restconf/data/network-state-export:bgp-config/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:route-policy/device={device_name}"
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
        url = f"{self._base}/restconf/data/network-state-export:ospf-config/device={device_name}"
        async with self._client() as c:
            resp = await c.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("network-state-export:device") or data.get("device", [])
            return entries[0] if entries else None

    async def check_sync(self, device_name: str) -> bool:
        """Return True if device is in-sync with NSO's internal CDB."""
        url = f"{self._base}/restconf/data/tailf-ncs:devices/device={device_name}/check-sync"
        async with self._client() as c:
            try:
                resp = await c.post(url)
                resp.raise_for_status()
                result = resp.json()
                sync_result = result.get("tailf-ncs:output", {}).get("result", "")
                return sync_result == "in-sync"
            except httpx.HTTPStatusError:
                return False
