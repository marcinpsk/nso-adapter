# SPDX-License-Identifier: Apache-2.0
"""Temporary maintenance server, inspected Apply release, and window store commands for cutover."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.config import get_config
from nso_adapter.core.cutover import (
    CutoverBlocked,
    CutoverJobsBlocked,
    CutoverReset,
    CutoverSchemaBlocked,
    CutoverStateBlocked,
    DiscardedJob,
    deauthorize_for_cutover,
    discard_queued_read_jobs,
)
from nso_adapter.core.worker import FollowupSyncFailed, ReleaseRefused, run_inspected_generation
from nso_adapter.main import create_app, maintenance_lifespan
from nso_adapter.store.db import StoreEngineUrlError, create_store_engine
from nso_adapter.store.models import JobStatus


def create_maintenance_app() -> FastAPI:
    """Serve ordinary producers and preview without background execution."""
    return create_app(lifespan_context=maintenance_lifespan)


app = create_maintenance_app()


async def release(device_id: int, generation_id: int, document_digest: str) -> JobStatus:
    """Start the app dependencies and run only the inspected generation."""
    async with app.router.lifespan_context(app):
        return await run_inspected_generation(device_id, generation_id, document_digest)


@asynccontextmanager
async def _store_transaction() -> AsyncIterator[AsyncSession]:
    """Open one transaction on a private engine and commit it on success; no recovery or workers start."""
    engine = create_store_engine(get_config().database_url, application_name="nso-adapter.cutover")
    try:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            yield db
            await db.commit()
    finally:
        await engine.dispose()


async def discard_read_jobs() -> tuple[DiscardedJob, ...]:
    """Fail the queued read jobs the stopped scheduler left behind."""
    async with _store_transaction() as db:
        return await discard_queued_read_jobs(db)


async def reset() -> CutoverReset:
    """Retire fleet authority and return the post-cutover worklist."""
    async with _store_transaction() as db:
        return await deauthorize_for_cutover(db)


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
    commands.add_parser("discard-read-jobs", help="fail every queued read job; refuse on any other live job")
    commands.add_parser("reset", help="retire fleet authority and print the post-cutover worklist")
    args = parser.parse_args(argv)
    if args.command in ("discard-read-jobs", "reset"):
        # Stdout carries only the JSON result.
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    if args.command == "discard-read-jobs":
        try:
            discarded = asyncio.run(discard_read_jobs())
        except (CutoverJobsBlocked, StoreEngineUrlError) as exc:
            parser.exit(1, f"discard refused: {exc}\n")
        print(json.dumps({"discarded": [job._asdict() for job in discarded]}))
        return 0
    if args.command == "reset":
        try:
            worklist = asyncio.run(reset())
        except (CutoverStateBlocked, CutoverBlocked, CutoverSchemaBlocked, StoreEngineUrlError) as exc:
            parser.exit(1, f"reset refused: {exc}\n")
        print(json.dumps({"devices": [device._asdict() for device in worklist.devices]}))
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
