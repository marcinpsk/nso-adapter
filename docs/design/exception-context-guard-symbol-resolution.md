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
