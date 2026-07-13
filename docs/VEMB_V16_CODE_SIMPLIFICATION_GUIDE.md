# VEMB v16 Code Simplification Guide

## Goal

When a call path already enforces strong preconditions, storage/proxy code should follow those guarantees instead of repeating defensive checks everywhere.

## Rules

1. Remove redundant `NULL` checks on parameters when all real callers already guarantee non-`NULL` inputs.

2. Remove redundant range or identity validation when the request has already been validated at an upper layer and lower layers only serve as execution paths.

3. Do not repeat the same validation in both `proxy` and `storage`.
   Put request-shape validation and orchestration decisions in `proxy`.
   Keep `storage` focused on applying state changes and data-path work.

4. Do not zero an output struct with `memset(..., 0, sizeof(*out))` if:
   the caller already treats the struct as required and initialized by convention, and
   the function fully owns the fields it writes or the zeroing is otherwise unnecessary.

5. Keep `memset` for output structs only when it is still needed to prevent stale fields from leaking because the function does not overwrite every field.

6. Prefer one authoritative place for peer-view/topology prechecks.
   If `proxy` already decides whether a peer owner must be attached, `storage` should not re-run the same decision logic.

7. Strengthen assumptions deliberately.
   If a function relies on non-`NULL` required inputs by contract, write code directly against that contract instead of mixing contract-style code with fallback-style defensive branches.

## Review Checklist

- Is this check guaranteed by all callers already?
- Is the same validation happening in an upper layer?
- Is this `memset` only hiding partially written output?
- Does this function own validation, or should it only execute?
- Would removing the branch make the main path clearer without changing behavior?

## Applied Here

This guidance came from simplifying the `peer_view_map`, `topology_set`, and related `vemb_v16_storage.c` / `vemb_v16_proxy.c` paths:

- move request validation and peer-owner attach prechecks to `proxy`
- keep `storage` focused on apply/publish behavior
- remove duplicated required-parameter checks
- remove unnecessary output clearing only where call contracts already make it redundant
