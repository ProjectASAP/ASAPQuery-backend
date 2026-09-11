# UnivMon dependency checkout

The UnivMon runtime requires asap_sketchlib commit
`c0de315754f9a6c77dd7a25aca0b2b62f0aec276` from PR #139. CI checks
out this exact revision. This dependency is not yet merged into sketchlib main.

For local builds, use that revision in the sibling `asap_sketchlib` checkout
referenced by the workspace Cargo patch and ASAPCollector's precompute crate.
Both consumers must resolve to the same checkout so their state types agree.
Use an isolated checkout when other work depends on a different revision.

The dependency exposes the standard-update compatibility predicate. Restoring
a terminal-mode UnivMon state into an ingest accumulator is rejected before
mutation. After #139 merges, update the pin explicitly and rerun restore and
real-process tests; do not silently substitute an unverified main revision.
