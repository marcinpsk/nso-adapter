# Authored failure diagnostics

## Status

Ratified as revision r1, scope: authored failure diagnostics.

## 0. Review ledger

No findings have been dispositioned yet.

## 1. Brief

### Decision

Choose the module interface that lets `failure_detail()` retain useful adapter-authored diagnostics without allowing a caller-bearing exception message to reach a log, job, or response sink.

### Current evidence

- `failure_detail()` is the shared rendering seam for 35 direct production call sites.
- It preserves the full `repr()` of every type in `AUTHORED_FAILURES`.
- `NsoReadContractError` and `NsoExportUnavailableError` production messages include a requested device name.
- `NsoActionFailedError` currently has one production construction site. Its message uses only an action name and a closed failure kind, but the exception constructor accepts any string.
- Structured `ReadFailure` classification already stores the operation, family, exception type, numeric HTTP status, and closed failure code without exception text.

### Constraints

- A requested device name, request URL, server reason phrase, response body, or arbitrary exception message must not reach a diagnostic sink through `failure_detail()`.
- Preserve numeric HTTP status classification.
- Keep the shared formatter as the one rendering seam for current callers.
- Preserve existing refusal and read-outcome behavior.
- Keep the change within PR 51 and below 100 changed files.

### Observable acceptance conditions

1. A secret-bearing `NsoReadContractError` renders only its safe classification.
2. A secret-bearing `NsoExportUnavailableError` renders only its safe classification.
3. The host-key failure retains only an explicitly closed diagnostic, or renders by type if no safe closed interface remains.
4. Production export-unavailable construction does not include the requested device name.
5. Existing HTTP status, read-outcome, and onboarding behavior remains unchanged.

### Mechanical guard

The formatter must obtain detail only from data whose interface prevents arbitrary strings. A test must enumerate every exception type whose message the formatter preserves and prove that its construction interface accepts only closed values. If no preserved-message type remains, the absence of a message-rendering branch is the guard.

### Candidate shapes

1. Remove message preservation entirely. `failure_detail()` returns an HTTP status classification or the exception type.
2. Add a closed diagnostic enum interface. Only exceptions constructed from that enum can expose the enum value through `failure_detail()`.
3. Keep a type whitelist, but make each whitelisted exception constructor reject arbitrary strings and build its message from a closed value.

## 2. Primary design

Remove `AUTHORED_FAILURES`. Render every exception by type, except that an HTTP
status error also retains its numeric status code. The host-key onboarding step
already names the action, so its exception type is sufficient at that sink.

This shape deletes all message authority from `failure_detail()`. It is the
smallest interface and fails closed for every new exception type.

## 3. Independent design

The independent designer also removes `AUTHORED_FAILURES`, but preserves the two
host-key refusal kinds through a closed enum. `NsoActionFailedError` accepts only
an enum member. `failure_detail()` uses an exact type check and maps that member
to authored text. All other non-HTTP exceptions render by type.

The independent design also moves the formatter and three exception types to a
new failure module. This gives the cross-cutting sink policy its own owner.

## 4. Divergence table

| Decision | Primary design | Independent design | Evidence | Merged choice |
| --- | --- | --- | --- | --- |
| Host-key detail | Type only | Closed enum detail | The action has two materially different failures, and the current focused test treats the distinction as useful | Preserve the two kinds through a closed enum |
| Module ownership | Keep the seam in `nso.client` | Add a dedicated failure module | The exception contracts describe RESTCONF client behavior, and moving every import does not simplify a current caller | Keep the definitions together in `nso.client` |
| Unsafe message authority | Delete it | Delete it | A class tuple cannot prove what data an exception message contains | Delete `AUTHORED_FAILURES` |

## 5. Merged design

Add `NsoActionFailureKind`, a closed string enum with the two safe host-key
failure descriptions. `NsoActionFailedError` requires
`type(kind) is NsoActionFailureKind`, stores the member, and builds its internal
message from the enum value. This exact check rejects equal plain strings and
string subclasses.

`failure_detail()` keeps one interface:

- A real HTTP status error renders as its type and numeric status.
- An exact `NsoActionFailedError` instance renders its type only when
  `type(exc.kind) is NsoActionFailureKind`; otherwise it falls back to its type.
- Every other exception renders by type only.

The exact type check prevents subclasses from gaining diagnostic authority. A
missing or invalid `kind` falls back to the exception type. No formatter branch
uses `str(exc)`, `repr(exc)`, `exc.args`, request fields, response text, or an
exception chain.

Production `NsoExportUnavailableError` messages become fixed strings. They do
not include the requested device name. `NsoReadContractError` can retain useful
internal messages because the shared formatter never emits them.

The formatter and exception contracts remain in `nso.client`. They belong to
the RESTCONF client contract and changing their import owner would not reduce
the interface presented to current consumers.

## 6. Mechanical guard and validation

- OpenGrep rejects direct `str()`, `repr()`, or formatted rendering of the
  formatter parameter.
- An AST regression rejects raw exception text, exception chaining, request
  fields, and response fields other than `status_code` inside the formatter.
- Constructor tests prove that arbitrary text cannot create an action failure.
- Both host-key refusal branches prove their exact closed diagnostics.
- Secret-bearing read and export errors prove type-only output.
- All three export-unavailable paths prove that their internal messages omit the
  requested device name.

## 7. Ratification

`RATIFY revision r1, scope: authored failure diagnostics`

The adversarial ratifier executed a candidate with exact type checks against
both closed kinds, equal strings, string subclasses, tampered arguments,
invalid and missing kinds, exception subclasses, secret-bearing read and export
errors, and a real hostile HTTP status error. No blocking defect remained.
