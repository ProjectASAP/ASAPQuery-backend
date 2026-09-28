# SDS obsolete-format cleanup

This follow-up removes obsolete installed-plan and persistence compatibility paths.

- Removed old identity and projection field aliases from installed plan contracts, along with superseded deployment API aliases.
- Removed `reused_from_generation`; unknown-field decoding rejects adoption metadata, and payload generation must still match the installed generation.
- Removed untyped projection decoders and the unused materialization JSON/byte adapter and serializer. Canonical typed Serde is the installed materialization format.
- Removed flat-map metadata migration and old-version readers. Only the branch's current sidecar schema is accepted (4 in #749/#774; 5 from #763 onward).
- Removed the recovery caller's log-and-return-zero fallback. Invalid persisted metadata propagates as an error; a missing sidecar is a fresh store.
- Made the producer partition roster explicit on the wire. An empty roster cannot authorize completion.
- Corrected stale recovery documentation and changed old-format acceptance tests into rejection tests.

Current semantic checks for dataset, definition, state format, revision and coverage remain required. Low-level raw storage formats are distinct from installed SDS authorization; this change does not delete current storage operators or codecs.

## Verification

Foundation checks passed locally:

| Check | Result |
| --- | --- |
| Type library | 117 passed |
| Control-plane library and HTTP API tests | 431 + 8 passed |
| Data-plane library | 1,165 passed |
| Serving integration | 13 passed |
| Restart and new-version warm-up process | 1 passed |
| Strict Clippy, all targets for the three crates | passed |

Generated logs are kept outside version control. The restart process test covers same-version recovery without re-ingestion and new-version cold start followed by fresh input. Downstream verification checks the restacked Level 1/2 plans and current storage schema.

No manual deployment verification or human approval is claimed.

Downstream checks on the restacked #775 branch also passed: Level 1 (3 tests),
Level 2 (1 exhaustive cost-selection test), storage (332 tests), and serving
integration (13 tests). Workspace formatting passed. Concurrent remote fixture
fixes were merged and retained before push.
