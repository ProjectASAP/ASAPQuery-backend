# Resource measurement validation

This PR validates the existing sketch-bench export path and ERP workload
composition. The Python tests distinguish process CPU from wall time, preserve
paired repetitions, reject missing/invalid inputs and normalize milliseconds to
nanoseconds per operation. The Rust test checks update/read/merge multiplicities
and retained byte-seconds while retaining the ERP record identity.

The regression first failed because `cpu_batch` silently truncated unmatched
user/system samples and accepted a negative raw component when their sum was
positive. `cpu_per_op` could also turn infinite work into a zero price. Both now
reject these inputs. Missing CPU remains unknown, not zero.

The tests use synthetic coefficients to validate arithmetic. They do not claim
those coefficients are real measurements. Real resource evidence must retain raw
paired samples, implementation/build/machine identity and measurement scope.
The existing `tools/empirical-bench/run.py` records real benchmark provenance;
its offline microbenchmarks do not independently establish deployment selection
quality. Shared-work costing tests remain in `physical/workload_cost.rs`.
