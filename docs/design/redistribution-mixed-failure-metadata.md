# Redistribution mixed failure metadata

## Status

Ratified revision r1. Implemented and verified.

## 0. Review ledger

- CLOSED: `Present.composite()` can preserve multiple ordered failures without a wrapper.
- CLOSED: `outcome_store` is the deep persistence seam. JSONB is sufficient because no relational failure query exists.
- CLOSED: commit recovery already reuses the same merged outcome object.
- CLOSED: an explicit five-field persistence whitelist excludes exception, response, server, URL, and device text.
- CLOSED: `JSONB(none_as_null=True)` plus a PostgreSQL `IS NULL` assertion distinguishes no failures from JSON null.
- REFUTED: leaving all-unavailable cardinality and `FamilyReadState` unchanged violates the acceptance conditions. Reopen if a consumer requires every unavailable component cause or wire-visible diagnostics.
- CLOSED: real mixed/store integration tests, migration round-trip, schema parity, and the full gate cover the stated acceptance.

## 1. Brief

### Decision

Choose the module interface and persistence seam that retain authored read-failure classification when a composite redistribution refresh both replaces an authoritative partition and keeps another partition after a read error.

### Current evidence

- `refresh_redistribution_from_outcomes()` records mixed success as `Present({}, stale)`, `result="replaced"`, and `succeeded=True`. It returns `False` so the device remains partial.
- `Unavailable.failure` carries an authored `ReadFailure`: operation, component family, exception type, numeric HTTP status, and contract failure code. It never carries exception text, response text, a URL, or a server reason phrase.
- `Present` has no failure field. The mixed aggregation therefore drops the failed component's classification before `outcome_store.record_read_outcome()`.
- `outcome_store._decompose()` stores only outcome, reason, and freshness. It drops failure classification from every outcome, including `Unavailable`.
- `RefreshOutcome` is the single phase-one persistence row and the current-pointer target. The read-state wire response currently exposes outcome, reason, freshness, and terminal materialization fields, but no failure classification.
- A composite can contain more than one failed component. The present merge policy already selects one worst `Unavailable` when no component is authoritative.

### Constraints

- Preserve `present` / `stale` / `replaced` / `succeeded=True` for mixed replacement and error retention.
- Preserve the function's `False` return for a partial device refresh.
- Persist only authored classification. Do not persist exception text, server text, response bodies, URLs, or reason phrases.
- Keep `outcome_store` as the central persistence seam. Do not make redistribution callers write persistence tables directly.
- Keep the migration additive and reversible. This development-only workspace stays at protocol version 1.0.
- Keep this PR below 100 changed files.

### Observable acceptance conditions

1. A real database integration test runs a mixed redistribution refresh and reads back the failed component's operation, family, exception type, numeric HTTP status, and failure code from persisted outcome state.
2. That row still reads `present`, `stale`, `replaced`, and `succeeded=True`.
3. A store test proves an ordinary `Unavailable(read_error, failure=...)` persists the same classification through the central seam.
4. A no-failure outcome stores null classification fields.
5. The full repository gate and migration checks pass.

### Mechanical guard

The `ReadOutcome` type carries only authored `ReadFailure` values. The central `outcome_store` adapter decomposes that typed value into dedicated nullable columns. Store integration tests cover failure-bearing `Present` and `Unavailable` outcomes, and the mixed redistribution integration test covers the only constructor that combines authoritative data with a component failure.

### Candidate shapes

1. Extend the `Present` interface with optional failure classification and add nullable failure columns to `RefreshOutcome`. The composite selects a classified component failure and the central store persists it.
2. Keep `Present` unchanged and add child component-failure records owned by each `RefreshOutcome`. Redistribution passes all component failures to a widened store interface.

## 2. Blind designs

### Primary design (kept hidden from the blind designer)

Model: primary Codex session model. Reasoning effort: high.

The `ReadOutcome` module owns the authored classification. Extend `Present` with a tuple of `ReadFailure` values. This tuple means that the returned data is authoritative for at least one part of a composite, while the named component reads failed and their partitions stayed stale. Ordinary successful `Present` values keep the empty-tuple default, so existing callers do not learn a new requirement.

The redistribution module collects every failure-bearing `Unavailable` in the component order when it creates the mixed `Present`. It does not attach declared non-failures such as `unsupported` or `not_authoritative`. The existing no-authoritative branch continues to choose its one worst `Unavailable`, because that outcome represents a single aggregate refusal rather than an authoritative composite with retained partitions.

The outcome-store module remains the only persistence seam. Its `record_read_outcome()` interface does not gain a parallel metadata parameter that could disagree with the outcome. Its implementation extracts zero or more classifications from the typed outcome and writes child rows after the parent attempt gets its immutable id.

Add a `refresh_outcome_failure` table with these fields:

- `id`: primary key.
- `attempt_id`: foreign key to `refresh_outcome.id` with cascade deletion.
- `operation`: required authored `ReadOperation` value.
- `family`: required component-family value.
- `error_type`: nullable raised type.
- `http_status`: nullable numeric response status.
- `failure_code`: nullable authored `ReadFailureCode` value.

Do not duplicate device text. The parent attempt already identifies the device, and the failure family identifies the failed redistribution component. Do not expose the rows through `FamilyReadState` in this increment. The finding concerns durable diagnostic classification, while the wire interface authorizes payload adoption. Mixing diagnostic lists into that authorization interface would widen every family response and every plugin consumer without changing an authorization decision.

Validation crosses the same persistence seam as production. A store integration test proves that `Unavailable.failure` creates one child row and no-failure outcomes create none. The existing real-database mixed redistribution test supplies two classified failed components, verifies both child rows, and preserves the current parent terminal tuple and return value. A migration test checks the table shape and cascade.

The deletion test favors this shape: delete the store implementation and every caller would have to coordinate parent ids, child inserts, safe field projection, and transaction ordering. Keeping it behind `record_read_outcome()` creates leverage and locality. A separate `failures=` argument would be a shallow interface because every caller and test would have to keep two descriptions of one read synchronized.

### Independent blind design

Model: `gpt-5.6-sol`. Reasoning effort: high. The designer received only the brief and evidence pointers and was told not to inspect this record.

The blind design proposed a `ReadObservation(outcome, failures)` interface and one nullable JSONB column on `RefreshOutcome`. Factory methods would copy one failure from an ordinary `Unavailable` or collect every failure from ordered composite outcomes. `record_read_outcome()` would accept only `ReadObservation`, which would require changing all persistence call sites. The designer rejected a normalized child-table alternative because its current diagnostic value does not justify relationship loading or relational query machinery. It also rejected a wire change because `FamilyReadState` controls reconciliation authorization, not diagnostic history.

## 3. Divergence table

| Decision | Primary choice | Blind choice | Evidence | Disposition | Consequence |
|---|---|---|---|---|---|
| Domain carrier | Add `failures` to `Present`; keep `Unavailable.failure` | Add `ReadObservation` around every outcome | An AST count finds 8 production and 27 test calls to `record_read_outcome()`. Only the redistribution mixed branch combines authoritative data and component failures. | Use the outcome variants, not a wrapper. `ReadObservation` would duplicate `Unavailable.failure` into a second value and widen 35 calls. | Ordinary callers stay unchanged. The mixed constructor must explicitly use the composite factory described below. |
| Composite construction | Construct `Present(..., failures=...)` in the merge branch | Central `ReadObservation.composite()` factory | A composite can carry multiple failures, and future composite branches could forget a raw tuple. | Add `Present.composite(data, freshness, component_outcomes)` as the sole failure-collecting constructor. | The factory preserves component order and extracts only authored `Unavailable.failure` values. The mixed branch has one visible, testable seam. |
| Persistence shape | Normalized child rows | Nullable JSONB array on the parent | The evidence is diagnostic, has no current relational join or predicate, and the parent row already owns attempt ordering and retention. | Use one nullable JSONB column. | Zero failures are SQL null. A non-empty ordered array holds an explicit whitelist shape. No relationship or extra query is required. |
| Persisted fields | Relational columns including component family | Explicit JSON objects, excluding device text | The parent identifies the device. `ReadFailure` already separates authored fields from exception/server text. | Add a dedicated `persistence_fields()` whitelist. Do not use `asdict`, `__dict__`, or `log_fields()`. | Stored objects contain only operation, component family, error type, HTTP status, and failure code. |
| All-unavailable aggregation | Keep the selected worst `Unavailable` | Preserve every component failure in the observation | The finding is the mixed present branch. Existing no-authoritative semantics select the worst failure object, and changing them would widen the reviewed behavior. | Keep the existing worst-unavailable contract in this increment. | Ordinary unavailable storage retains its selected classification. A separate requirement would be needed to preserve multiple all-unavailable causes. |
| Read-state wire | No change | No change | No current authorization decision consumes failure detail. Adding it changes every family response and downstream plugin contract. | Closed: no wire change. | The database retains diagnostics without widening the reconciliation interface. |
| Merge-policy refactor | Keep the existing branch structure | Add `_CompositeDecision` | The current terminal tuple and recovery flow are already tested and reviewed. The metadata fix needs no policy rewrite. | Keep the branch structure. Thread the same merged outcome into the existing recovery path. | Smaller implementation surface and lower regression risk. |

## 4. Merged design

Revision r1.

The `ReadOutcome` module remains the source of truth. Add an ordered, compare-excluded `failures: tuple[ReadFailure, ...]` field to `Present`, with an empty default. Add a `Present.composite()` class method that receives ordered component outcomes and retains each non-null `Unavailable.failure`. This interface describes the existing degraded-success meaning directly: some composite data is authoritative, while named failed partitions remain stale.

Add `ReadFailure.persistence_fields()`. It returns only `read_operation`, `component_family`, `error_type`, `http_status`, and `failure_code`. It does not accept or serialize an exception, response, URL, server phrase, device string, or future dataclass field implicitly.

Add `RefreshOutcome.read_failures`, a nullable JSONB array. `outcome_store` stays the only persistence seam. Its implementation extracts `Present.failures`, one `Unavailable.failure`, or no failures for `AbsentAuthoritative`, then stores a non-empty list of whitelisted dictionaries or SQL null. Its public interface remains `record_read_outcome(..., outcome)`, so callers cannot supply metadata that disagrees with the outcome.

The redistribution mixed branch calls `Present.composite()` with component outcomes in `_REDIST_COMPONENTS` order. The same merged outcome already travels through initial persistence, terminal materialization, and commit-recovery persistence, so the classification survives both writes. Preserve the existing terminal tuple and false return.

Do not change `FamilyReadState` or the API schema. The authorization interface stays small, while diagnostic persistence stays local to the outcome-store adapter.

Tests cross production seams: the real database mixed test supplies two failed components and verifies their ordered persisted classifications plus the unchanged terminal tuple; the store integration test verifies one ordinary unavailable classification and SQL null for a clean present; a migration test verifies additive upgrade, reversible downgrade, JSONB type, and nullability. The existing schema-parity gate verifies model-to-head agreement.

The deletion test favors this merged shape. Removing central outcome-store projection would force safe serialization into callers. Removing `Present.composite()` would force each composite policy to rediscover how to select authored failure evidence. Both modules earn their interfaces through locality and leverage.

## 5. Ratification

Reviewer: `gpt-6-astra`. Reasoning effort: high. The reviewer inspected the merged record and repository in a fresh context, then independently counted persistence calls and checked SQLAlchemy JSONB binding behavior.

Verdict: **RATIFY revision r1, scope: mixed redistribution failure metadata persistence.**

The verdict requires `JSONB(none_as_null=True)` and a real PostgreSQL `IS NULL` assertion. Database migration execution and the full configured gate remain implementation evidence.

## 6. Implementation increment

The implementation follows the ratified shape:

1. The mixed real-database test supplies two classified failures. It asserts the ordered persisted dictionaries, unchanged `present` / `stale` / `replaced` / `succeeded=True` state, and the `False` return.
2. Store integration tests prove that an ordinary `Unavailable.failure` persists and a clean `Present` stores SQL null.
3. `Present.composite()`, the five-field whitelist, and the central store projection are implemented.
4. The nullable JSONB model column has a reversible migration from the live Alembic head.
5. The migration round-trip, schema parity, OpenGrep, Ruff, mypy, and the full eight-worker gate pass. The full gate reports 3,742 tests passed and 95.11% coverage.
