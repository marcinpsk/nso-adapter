# Exception-context guard symbol resolution

Status: ratified

## Problem

The repository guard identifies helpers that always raise and flags calls to
them while an exception handler is active. It currently stores only function
names. An unrelated method can therefore taint a module function with the same
name. It also rejects every function that contains any `return`, so a terminal
`return refuse()` wrapper evades the guard even though the call raises before
the function can return.

Both defects are reproduced by runtime-backed tests. The analyzer must continue
to judge what executes while a handler is active. It must not infer behavior
from names that resolve in another lexical namespace.

## Evidence

- `tests/test_no_suppressed_exception_context.py::_always_raising_helpers`
  collapses each helper to `node.name`.
- `_terminal_call_name` accepts only expression statements.
- `test_flags_a_RETURN_wrapper_around_an_always_raising_helper` proves the
  missed runtime context.
- `test_an_unrelated_method_name_does_not_taint_a_module_builder` proves the
  false positive from a class/module name collision.

## First draft

Index helper definitions by their binding scope and name. Module functions,
nested functions, and each class namespace then have distinct symbols. Resolve
direct calls through the current lexical function scopes and the module scope.
Resolve `self.name()` and `cls.name()` only through the nearest class scope.
Keep alias closure within the scope that owns the assignment.

Treat a terminal expression call and a terminal `return <call>` as the same
wrapper edge. Ignore returns inside nested definitions when deciding whether an
outer function can return. Exclude generator functions because calling one does
not execute its body.

## Alternatives

1. Keep global names and subtract collision cases. This cannot distinguish two
   classes that use the same method name.
2. Add only terminal-return support. This leaves false positives that make the
   required guard unreliable.
3. Implement full Python name binding and control-flow analysis. The current
   rule needs scoped local helper identity, not a general static analyzer.

## Decision

The requester and blind designer converged on scoped helper identities. Keep
`scan_source` and the fixed-point effect classifier, but resolve calls to a
specific function definition through a small lexical binding index.

The index must preserve these Python boundaries:

- Bare names resolve through function scopes and the module scope. A method's
  bare name does not resolve through its class namespace. Class-body expressions
  and method defaults or decorators do use the active class namespace.
- `self.name()` resolves only from an ordinary instance method's unchanged
  first-parameter binding. `cls.name()` resolves only from a recognized class
  method's unchanged first-parameter binding. Closures can capture those
  bindings. Static-method parameters have no receiver identity, regardless of
  spelling. A shadowed or reassigned receiver is unknown.
- Simple aliases remain local to the binding scope. Unknown, conflicting, or
  shadowed bindings do not fall through to another scope.
- Defaults and decorators execute in the enclosing scope. Deferred bodies use
  their own function scope. Comprehension targets create their own bindings.
- Parameters, imports, arbitrary decorators, assignments, and conflicting
  definitions block inference for the affected binding. Unshadowed built-in
  `classmethod` and `staticmethod` are the only recognized descriptor
  decorators.

The classifier must inspect only code that executes during the call. It must
walk nested definitions' defaults and decorators but not their deferred bodies.
It must exclude generators, including a yield in a nested definition-time
expression. A terminal expression call or terminal `return <call>` can inherit
an already known immediate raise, but any other executed return disqualifies the
wrapper. Async helpers count only when awaited. Creating a coroutine or
generator does not count as executing it. Unseeded alias or wrapper cycles stay
unknown.

Generator-expression creation evaluates only its outermost iterable. Its body,
filters, and later iterables remain deferred. Pattern captures bind unknown
names in their enclosing lexical scope. An assignment expression inside a
comprehension binds its target in the nearest enclosing non-comprehension scope.
Every supported plain-name or receiver read and every binding write uses the
same declaration-aware scope rule. A `global` read or write targets the module.
A `nonlocal` read skips the declaring scope, and its writes conservatively
invalidate the possible enclosing function bindings. A lookup can read a class
namespace only when that class is its initial evaluation scope. It skips later
class scopes before reading their bindings or declarations, as Python does for
method bodies, comprehensions, and nested classes.

Cause aliases to `None` use the same scoped binding index, so a same-spelled
binding in another function cannot change `raise ... from name` classification.

This remains a narrow syntactic proof for the patterns the guard supports. It
does not add import resolution, a control-flow graph, or general data-flow
analysis.

OpenGrep remains appropriate for the direct source patterns introduced in the
next PR. The current OpenGrep framework explicitly does not follow helper calls,
so it cannot replace this rule's alias, wrapper, or method-call analysis.

The third adversarial round found no remaining counterexample after receiver
identity was limited to ordinary instance methods and recognized class methods.
Implementation must preserve the runtime-backed counterexamples from all three
rounds and keep unsupported binding behavior conservative.

## Exception-log guard extension

Status: ratified (revision r1)

### Problem

The diagnostic-sink guard rejects direct `str(exc)` and `repr(exc)` rendering,
but accepts `error=exc`, `error=f"{exc}"`, and `logger.exception`. Each form can
copy an HTTP reason phrase, URL, response body, secret reference, or other
provider text to an operator log.

The guard module owns one narrow interface: exception-bearing diagnostic calls
in the importer and read-outcome bookkeeping use
`error=failure_detail(exc)` and never use `logger.exception`. Its two adapters
sit at the pre-commit seam. The Python AST test provides exact structural
checks and production-file coverage. OpenGrep provides fast local feedback for
the same forbidden forms. OpenGrep remains a pre-commit check only.

### Evidence

- `tests/core/test_importer_failure_sinks.py::_raw_log_exception_renderers`
  reports only `str` and `repr` calls below an `error` keyword.
- `.opengrep/nso-rules.yaml::nso-outcome-raw-exception-renderer` matches the
  same three direct renderer shapes.
- Both checks accept the three unsafe forms above today.
- The guarded production files already render every exception-bearing
  `error` field through `failure_detail` and contain no `logger.exception`
  call. Tightening the interface needs no production logging rewrite.

### Selected plan

The operator selected custom OpenGrep in pre-commit. Keep the dual-adapter
shape because the implementations have different leverage:

1. The AST adapter rejects every `logger.exception` call and every `error`
   keyword whose value is not exactly one direct `failure_detail(...)` call in
   the guarded files.
2. The OpenGrep adapter rejects `logger.exception` and any `error` keyword that
   is not a direct `failure_detail(...)` call. Its paths include the importer,
   refresh engine, redistribution bookkeeping, application entry point, and its
   own fixture.
3. The behavior fixtures include direct exception values, f-strings, `str`,
   `repr`, `logger.exception`, and the accepted `failure_detail` form.

The interface is intentionally syntactic. It does not infer which arbitrary
expressions contain exception data. In these diagnostic modules, the `error`
field is reserved for classified exception detail. An authored non-exception
classification uses separate fields such as `reason`, `failure_code`, and
`http_status`.

### Alternatives

1. Use OpenGrep alone. This makes its partial parser the only authority. The
   existing Python syntax check provides an independent executable contract.
2. Infer exception bindings through `try` and `except` control flow. This would
   allow unrelated `error` values, but adds lexical and deferred-body rules the
   selected diagnostic modules do not need. Callers gain no required behavior
   from that larger interface.
3. Match only the newly reported forms. This leaves aliases and other
   expressions open, so the failure class remains possible.

### Acceptance conditions

- The AST fixture reports `error=exc`, `error=f"{exc}"`, direct `str` and
  `repr`, and `logger.exception`, and accepts `error=failure_detail(exc)`.
- The OpenGrep fixture proves the same positive and negative forms.
- `logger.exception(..., error=failure_detail(exc))` is still rejected because
  the exception method adds raw exception text outside the structured field.
- Both adapters report no finding in the guarded production files.
- The OpenGrep hooks remain in `.pre-commit-config.yaml` only. No GitHub Actions
  or pre-push hook runs them.
- The adapter PR remains below 100 changed files.

The first increment tightened both adapters and their fixtures without changing
production logging. A later live review found three raw exception sinks in the
application entry point's SSE background work. The second increment adds the
whole `nso_adapter/main.py` module to both guard adapters, classifies all three
sinks with `failure_detail`, and exercises them with real `httpx` errors. Other
modules remain outside this file-based interface until their failure contracts
receive separate behavioral analysis.

The adversarial reviewer ratified revision r1 after executing both proposed
predicates. The AST predicate accepted all 19 guarded production `error`
fields. OpenGrep 1.30.0 accepted the negative-pattern structure, reported no
production violation, and kept its documented partial-parser warning. The
reviewer also confirmed the hooks run only at the pre-commit stage.

The adversarial reviewer ratified revision r2 after checking the three SSE paths.
The extension preserves cancellation, dirty-refresh retries, notification
handling, and dispatch cleanup. Its tests require the exact classified type and
status while excluding the request URL and server reason phrase.
