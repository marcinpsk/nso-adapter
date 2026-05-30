#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Probe NSO RESTCONF notification streams via SSE.

Usage (env-based):
    NSO_URL=http://nso-host:8080 NSO_USER=admin NSO_PASSWORD=secret \\
      NSO_HOST_HEADER=nso.example.com PROBE_DURATION=30 \\
      python scripts/probe_nso_streams.py

Falls back to config.yaml when NSO_URL is not set (reads first nso_instances entry).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse, urlunparse


async def main() -> int:  # noqa: C901
    base_url = os.environ.get("NSO_URL", "")
    user = os.environ.get("NSO_USER", "")
    password = os.environ.get("NSO_PASSWORD", "")
    host_header: str | None = os.environ.get("NSO_HOST_HEADER") or None
    duration = float(os.environ.get("PROBE_DURATION", "30"))

    if not base_url:
        config_file = os.environ.get(
            "CONFIG_FILE", str(Path(__file__).parent.parent / "config.yaml")
        )
        try:
            import yaml  # noqa: PLC0415

            with open(config_file) as f:
                cfg = yaml.safe_load(f)
            inst = cfg.get("nso_instances", [{}])[0]
            base_url = inst.get("base_url", "")
            host_header = inst.get("host_header") or host_header
        except (FileNotFoundError, IndexError, KeyError) as exc:
            print(f"ERROR: cannot load config from {config_file}: {exc}", file=sys.stderr)
            return 1

    if not base_url:
        print(
            "ERROR: set NSO_URL (or CONFIG_FILE pointing to a yaml with nso_instances)",
            file=sys.stderr,
        )
        return 1

    if not user:
        print("ERROR: set NSO_USER and NSO_PASSWORD", file=sys.stderr)
        return 1

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from nso_adapter.notifications.sse_subscriber import SSESubscriber  # noqa: PLC0415

    subscriber = SSESubscriber(base_url, (user, password), host_header=host_header)

    print(f"=== Discovering streams at {base_url} ===")
    try:
        streams = await subscriber.discover_streams()
    except Exception as exc:
        print(f"ERROR: discover_streams failed: {exc}", file=sys.stderr)
        return 1

    if not streams:
        print("No streams discovered — check auth and Host header.", file=sys.stderr)
        return 1

    print(f"Found {len(streams)} stream(s):")
    for s in streams:
        print(f"  {s.get('name', '?'):20s}  {s.get('description', '')}")

    results: dict[str, list[dict]] = defaultdict(list)

    for stream in streams:
        name = stream.get("name", "?")
        access = stream.get("access", [])
        json_access = next((a for a in access if a.get("encoding") == "json"), None)
        if not json_access:
            print(f"\n[{name}] No JSON access endpoint — skipping.")
            results[name] = []
            continue

        url = json_access["location"]
        # NSO echoes the Host header into subscription URLs; rewrite to the
        # actual base_url netloc so we can connect over the real IP/port.
        parsed_base = urlparse(base_url)
        parsed_url = urlparse(url)
        url = urlunparse(parsed_url._replace(scheme=parsed_base.scheme, netloc=parsed_base.netloc))
        print(f"\n[{name}] Subscribing for {duration}s")
        print(f"         URL: {url}")

        events: list[dict] = []

        def on_event(
            raw: str,
            parsed: dict | None,
            _name: str = name,
            _events: list = events,
        ) -> None:
            _events.append({"raw": raw, "parsed": parsed})
            print(f"  EVENT #{len(_events):3d}: {raw[:140]}")

        try:
            await subscriber.subscribe(url, on_event, duration=duration)
        except Exception as exc:
            print(f"  ERROR: {exc}")

        results[name] = events
        print(f"  → {len(events)} event(s) captured in {duration}s")

    print("\n=== Summary ===")
    for name, events in results.items():
        print(f"  {name:20s}: {len(events)} event(s)")
        if events and events[0]["parsed"]:
            print(f"    First event top-level keys: {list(events[0]['parsed'].keys())}")

    print("\n=== Raw results (JSON) ===")
    print(json.dumps({k: v for k, v in results.items()}, indent=2, default=str))

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
