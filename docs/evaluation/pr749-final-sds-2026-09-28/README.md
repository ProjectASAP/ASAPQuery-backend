# #749 final SDS contract validation

Audience: implementation reviewers. This supersedes the earlier
[foundation report](../pr749-sds-foundation-2026-09-28/README.md).

## Contract established in this PR

#749 consumes Planner's canonical semantic export and binds it to an explicit
logical dataset identity. Catalog schema 6 separates semantic definitions from
deployed outputs; planning snapshot schema 3 requires dataset identity.
Hot and rebuild outputs can share meaning without sharing read authorization.
The typed Count/Rate support previously staged in #771 is included here because
collapsing those families produces conflicting semantic identities.

An installed query resolves only its selected output within its plan version,
then checks definition, revision, format and coverage. Recovery accepts the same
installed generation; a new version must populate fresh state before serving it.
Ad-hoc semantic discovery and cross-version state adoption are not implemented.
Restricted native configuration helpers remain for explicit imported-state
fixtures; they do not establish a multi-dataset Planner binding.

## Problems found before the fixes

- Policy fingerprints coupled semantic identity to deployment routing and could
  not express separate hot/rebuild outputs sharing one definition.
- Metric/table names alone could not distinguish equal expressions over
  different logical datasets.
- Importing semantic definitions without typed Count/Rate support failed the
  workload tests: one output claimed different definitions.
- A Planner API adapter could discard a join pruning contract. Unsupported
  pruned relational joins now take the explicit fallback path; the vector
  adapter validates the complete label-equality predicate.
- Old tests assumed Planner always selected a heap. Collector fixtures now
  advertise the intended capabilities; exact backend candidates remain legal.
- A persistence test waited for any new disk part, which could belong to the
  old series. It now waits for the newly allocated series' actual record.

## Validation

Passed locally on the final implementation:

- Type, control-plane and data-plane libraries: **117 + 431 + 1,164 tests**.
- Installed-plan serving/transport integration: **13 tests**.
- Offline evidence integration: **6 tests**.
- Production-process restart/new-version warm-up: **1 test**.
- All-target strict Clippy for all three packages and workspace formatting.

Identity collisions were exposed while extracting the contract ahead of typed
Count/Rate support. Generated logs are kept outside version control.
The production-process test covers same-version restart without re-ingestion,
new-version cold state, and fresh input becoming queryable in that version.
No production workload, production-cost, or independent human approval claim is
made. #728/#742/#775 retain their structural, synthetic-selection, and deployment
execution acceptance roles.

## Obsolete-format cleanup

See [cleanup and validation](cleanup.md) for removed wire adapters, strict metadata recovery, and downstream verification.
