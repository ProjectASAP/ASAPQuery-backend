# PR #749 foundation rebase and SDS review

The implementation is based on main `b7c0f08a`, including the merged #737 design.
Commit `79eb631a` preserves the prior implementation tree while removing the
already-squashed documentation history. Fixes are in `070fad6e` and `3c840ff9`.

## Corrections

- An unchanged definition previously authorized reading an older generation.
  The compatibility map and read path are removed; adoption metadata is rejected.
- After restart, the persisted resolver could send fresh input to an excluded,
  completed old series. The old physical address is reserved and fresh writes
  allocate a new series.
- A live version change now authorizes the same fresh allocation without first
  requiring the old series to be removed.

Metadata, store-visibility and live-reactivation regressions fail before their
fixes. The process test also exposed the completed-series rejection before the
fresh-allocation fix. Generated failure logs are kept outside version control.

## Validation

On the corrected #749 code:

- 112 type-library tests, 427 control-plane tests and 1,161 data-plane tests pass.
- The production-process test recovers the same version without re-ingestion,
  verifies a new version is cold, then ingests fresh data and verifies its result.
- Strict all-target Clippy passes for those three packages.

The data-plane suite and process test were rerun after the final allocation fix.
No production-cost or independent human approval claim is made.

## Historical scope

This report records the earlier foundation validation. The final SDS identity
contract and its new test results supersede the scope below; see
[final identity validation](../pr749-final-sds-2026-09-28/README.md).

This is the plan/schema foundation described in #737's staged migration, not the
completed SDS implementation. Its policy-fingerprint-based schema remains
transitional. Canonical semantic definitions, independent deployed-output
identity and explicit logical dataset identity land in #774 using Planner #462;
subsequent PRs complete shared execution and storage integration. The developer
installation guide and PR description make that boundary explicit.

