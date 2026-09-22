# Planner main updates

The backend keeps reproducible Planner revisions in `Cargo.toml` and `Cargo.lock`.
`Sync ASAPPlanner main` checks upstream main daily (or on manual dispatch), updates
all four Planner dependencies together, and opens or refreshes
`automation/sync-asapplanner-main` against backend main. GitHub scheduled runs can be delayed.

The existing `ASAP_CI_REPO_TOKEN` secret must have read access to private dependency
repositories and **Contents and Pull requests write** access to this backend repo.
The updater uses this token to create its PR so GitHub triggers `MVP CI`.
The merge workflow uses the repository's `GITHUB_TOKEN` and does not require
GitHub's optional auto-merge feature to be enabled.

MVP CI uses `--locked` and runs the workspace checks, Clippy, unit tests, and
process E2E tests. Only a successful run for the current update PR head can
trigger the merge workflow; it also verifies the repository, branch, base, and
that main has not advanced since that run and only the two Cargo files changed. Branch protection remains in force.
Failed updates stay open and main retains its last verified version. Fix an
upstream incompatibility separately, or let the next upstream update refresh the
PR. An automation failure is visible in Actions; check token permissions there.

Locally, run `./scripts/sync-asapplanner-main.sh` from the repository root,
then run the same checks as MVP CI. Updater unit tests run with:

```sh
python3 -m unittest discover -s scripts/ci -p 'test_*.py'
```

The sync script and scheduled workflow are shared with PR #765, at the same paths
and with the same automation branch; there is only one updater.
