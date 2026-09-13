# Removal-authority label vocabulary

Status: ratified

## Problem

`allowed_removal_keys` is validated when a generation is stored. Each top-level
scope must name a projection section. Each nested label must name a key grain
that a current removal consumer understands.

Most labels are collateral-guard list labels from `Section.guard_lists`.
`interface_config.address` is different. It carries removed address triples for
the post-commit residue check, and it is not a device document list. Validation
against only `guard_lists` rejects every interface address shrink with HTTP 500.

The fix must keep one authoritative per-section vocabulary. It must preserve the
existing wire shape and all current producers and consumers. It must not weaken
validation to accept arbitrary labels.

## Evidence

- `nso_adapter/core/removal.py::interface_removal_keys` emits `address` plus the
  relevant collateral-guard labels.
- `nso_adapter/core/removal.py::_interface_config_residue` consumes `address`.
- `nso_adapter/core/static_route_plan.py::validate_removal_authority` validates
  stored generation context.
- `nso_adapter/core/projection.py::_Section.guard_lists` owns the current
  collateral-guard label vocabulary.
- `tests/core/test_execution_context.py::test_partial_address_removal_has_only_address_authority`
  reproduces the failed admission through the real API and database path.

## First draft

Add `residue_labels` to the projection section record. Derive the accepted
removal-authority labels from each section's guard-list labels plus its explicit
residue labels. Set `interface_config.residue_labels` to `("address",)`.

This shape keeps the two meanings distinct. A guard-list label identifies a
device document list and includes path and key metadata. A residue-only label
identifies evidence consumed after the write and needs no document path. The
section remains the single owner of both vocabularies, and the validator accepts
their union.

## Alternatives

1. Add an `interface_config.address` special case in the validator. This splits
   the vocabulary across modules and can drift from the producer or consumer.
2. Put `address` in `guard_lists`. This gives it false document path and key
   semantics, so generic collateral and residue walkers can treat it as a list.
3. Split `allowed_removal_keys` into collateral and residue envelopes. This is a
   larger wire-shape change with no current consumer requirement.

## Decision

The requester and blind designer converged on the same boundary and data shape.
Keep vocabulary ownership in `core/projection.py`. Add `residue_labels` to
`_Section`, and derive a `removal_authority_labels` property from those labels
and every `guard_lists` label. Declare only `interface_config.address` as a
residue label. Keep validation at the generation-admission boundary in
`core/static_route_plan.py`.

The validator must preserve these invariants:

- Every registered guard-list label is accepted for its own section.
- `address` is accepted only for `interface_config`.
- `address` remains outside `guard_lists`, because it has no document-list path.
- Unknown labels fail even when their key collection is empty.
- Residue evidence does not replace the IPv4 or IPv6 collateral-guard identity.

The adversarial ratifier found no counterexample. Generic collateral and
residue walkers continue to consume only `guard_lists`, while the
interface-specific residue check consumes `address` explicitly. All `_Section`
constructors use keyword arguments, and current consumers use attributes, so
the defaulted field and derived property do not change a public or positional
interface.
