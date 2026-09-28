# SPDX-License-Identifier: Apache-2.0
"""Temporary maintenance server and inspected Apply release for cutover."""

from __future__ import annotations

import argparse
import asyncio

import uvicorn
from fastapi import FastAPI

from nso_adapter.core.worker import FollowupSyncFailed, ReleaseRefused, run_inspected_generation
from nso_adapter.main import create_app, maintenance_lifespan
from nso_adapter.store.models import JobStatus


def create_maintenance_app() -> FastAPI:
    """Serve ordinary producers and preview without background execution."""
    return create_app(lifespan_context=maintenance_lifespan)


app = create_maintenance_app()


async def release(device_id: int, generation_id: int, document_digest: str) -> JobStatus:
    """Start the app dependencies and run only the inspected generation."""
    async with app.router.lifespan_context(app):
        return await run_inspected_generation(device_id, generation_id, document_digest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Temporary cutover maintenance runner")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="serve Apply and preview without workers")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    release_cmd = commands.add_parser("release", help="execute one inspected generation")
    release_cmd.add_argument("device_id", type=int)
    release_cmd.add_argument("--generation", type=int, required=True)
    release_cmd.add_argument("--document-digest", required=True)
    args = parser.parse_args(argv)
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    try:
        status = asyncio.run(release(args.device_id, args.generation, args.document_digest))
    except ReleaseRefused as exc:
        parser.exit(1, f"release refused: {exc}\n")
    except FollowupSyncFailed as exc:
        parser.exit(1, f"release follow-up failed: {exc}\n")
    if status is not JobStatus.succeeded:
        parser.exit(1, f"release job finished with {status.value}\n")
    print(f"generation {args.generation} released")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
