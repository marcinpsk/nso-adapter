# CI teardown connection leak diagnosis

Date: 2026-08-29

## Outcome

The holder was the process-global async store engine used by the `store_engine` fixture.
Internal callers consumed the FastAPI `get_session()` async generator with `async for`.
Several callers then returned or broke after the generator yielded its session. That exit did
not await `aclose()`. It left the `AsyncSession` and its checked-out connection pending until
async-generator finalization.

On an idle local loop, finalization usually ran before fixture teardown. On a loaded xdist
worker, fixture teardown could dispose the engine and close the test loop first. Engine
disposal closes checked-in connections, but it cannot close a connection that is still checked
out by the suspended generator. The teardown guard then saw the orphaned PostgreSQL backend as
`idle` for its full 15-second grace period.

This was not slow backend exit. It was deferred session ownership with no deterministic close.

## CI evidence

The three supplied runs showed the same symptom on different workers and victim tests:

| Run | Attempt | Victim test | Suite result |
| --- | ---: | --- | --- |
| 33195015997 | 1 | `test_intent_reconcile_aborts_on_fetch_error` | 2,900 passed, 82 warnings, 1 teardown error |
| 33211745663 | 1 | `test_span_exception_propagates_without_cancel` | 2,916 passed, 82 warnings, 1 teardown error |
| 33243471580 | 1 | `test_run_apply_job_not_found` | 2,968 passed, 82 warnings, 1 teardown error |

Each error reported one backend in `idle` state after 15 seconds. The successful reruns for the
second and third workflow runs confirmed the intermittent scheduling dependency.

The common path was short execution after `adapter_client` setup. Setup called
`ensure_store_meta()`, which returned from an `async for get_session()` body. The third victim
also called `run_apply()` with an absent job, which performed another early return from the same
pattern.

## Instrumentation

Commit `2df1176` adds persistent attribution before the fix:

- The guard now reports `pid`, `application_name`, `backend_start`, `state`, `xact_start`, and
  `query` for each surviving client backend.
- Every engine constructor in `nso_adapter`, Alembic, and the tests sets a distinct
  `application_name` for its creation site.
- `test_drop_database_reports_surviving_backend_diagnostics` holds a real PostgreSQL connection,
  invokes strict teardown, and verifies the complete assertion message.

The instrumentation test was red before the change. The old assertion contained only the PID
and state. It passed after the guard and engine attribution were added.

## Lifecycle sweep

The sweep covered all connection and background ownership sites.

- There are no raw `asyncpg.connect()` or `asyncpg.create_pool()` calls.
- The application has one async engine constructor. Alembic uses one sync `NullPool` engine.
- Every direct test engine has an owning `finally` block or fixture finalizer that calls
  `dispose()`.
- `pg_provisioner` is the only session-scoped database engine. It is synchronous and uses
  `NullPool`, so it is not bound to a pytest event loop.
- `store_engine` is function-scoped. It owns the store globals, awaits disposal, and clears the
  globals after every dependent client and session fixture has closed.
- `rival_engine` depends on `store_engine`, so pytest finalizes the rival first.
- The adapter client fixtures patch scheduler, worker, and SSE startup. They do not leave
  production background tasks running. The production lifespan retains and drains worker and
  SSE tasks before engine disposal.
- Test-created tasks in the affected worker, scheduler, cancellation, and generation suites are
  awaited or cancelled in cleanup paths. The focused lifecycle suite passed after moving their
  session fakes to the context-manager seam.
- The source sweep found 25 internal `async for get_session()` consumers. Sixteen contained an
  early `return` or a loop exit that could suspend the dependency generator. Natural loop exit
  was also replaced so all internal ownership follows one rule.

The engine constructors, fixture order, and background task ownership did not produce the
deterministic failure. The session consumer did.

## Deterministic proof

The regression uses the real `run_apply()` seam and the real PostgreSQL-backed store engine:

```text
uv run --native-tls -- pytest \
  tests/core/test_apply.py::test_run_apply_job_not_found -q -n 0 --no-cov
```

Before the fix, the test failed immediately after `run_apply()` returned:

```text
assert store_engine.sync_engine.pool.checkedout() == 0
assert 1 == 0
```

The command failed in three consecutive runs. This is the exact code path named by the third CI
failure. It proves that the function returned while its store connection was still checked out.
No sleep, database force, injected connection, mock, or garbage collection was needed.

After the first fix slice moved `run_apply()` to the context manager, the same command passed and
the checked-out count was zero.

A second red test scanned `nso_adapter` for internal calls to the FastAPI dependency. It listed 25
remaining call sites across startup, schedulers, workers, jobs, generation, removal, and store
metadata. The test passed after every internal consumer moved to the context manager.

## Fix

Commit `77f9999` makes session closure structural:

- `nso_adapter.store.db.session()` is the single internal session context manager.
- `get_session()` remains the FastAPI dependency and delegates to `session()`.
- Every internal caller uses `async with session() as db`.
- Existing claim and test wrappers reuse the same context manager instead of implementing their
  own generator close protocol.
- The `run_apply()` regression asserts that no connection remains checked out after its early
  return.
- The source invariant rejects future internal calls to `get_session()`.

An early return, exception, cancellation, or outer loop exit now runs and awaits the context
manager exit before control reaches fixture teardown.

The fix does not change the teardown guard. The committed grace remains 15 seconds. The guard
still inspects every client backend state and still uses `DROP ... WITH (FORCE)` only after it has
reported the defect.

## Stress and verification

The three supplied victim tests ran in their stated order under the following pressure:

- `-n 2`
- one CPU with `taskset -c 0`
- reduced priority with `nice -n 10`
- strict teardown enabled
- temporary teardown grace of 0.2 seconds

Ten iterations passed, for 30 victim-test executions. The grace was then restored to 15 seconds.
This result shows that deterministic close also lets the PostgreSQL backend exit well below the
normal grace on a CPU-starved runner.

Additional results:

- Focused lifecycle integration set: 419 passed, 81 warnings in 61.60 seconds.
- Ruff check: passed.
- Ruff format check: passed, 421 files already formatted.
- Mypy gate: passed with zero new or unresolved errors. The now-empty generated baseline was
  synchronized.
- Final CI-equivalent full suite: **2,917 passed, 82 warnings in 250.11 seconds**.
- Final coverage: **95.47%**, above the 90% gate.
- Final environment enabled strict teardown, the no-skip gate, eight capped xdist workers, and a
  non-UTC timezone.
- Final result contained no teardown connection warning or error.

The 82 warnings are existing async mock warnings. None is a database teardown warning.
