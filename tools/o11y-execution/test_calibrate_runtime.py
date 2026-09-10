"""Fail-closed validation for calibrated CandidateTopK artifacts."""
import unittest

from calibrate_runtime import validate_candidate_topk_artifact, validate_candidate_topk_execution


class CandidateTopKArtifactTests(unittest.TestCase):
    def artifact(self, membership):
        return {"install_request": {
            "query_plan": {"entries": {"q": {"nodes": {
                "0": {"op": "candidate_top_k", "inputs": [1, 3]},
                "1": {"op": "summary_estimate", "input": 2},
                "2": membership,
                "3": {"op": "exact_readout", "input": 4},
                "4": {"op": "read_materialization", "binding": {"materialization": 8}},
            }}}},
            "precompute_plan": {"schemas": [
                {"materialization": 7, "family": {"family": "sketch", "kind": {
                    "algorithm": "CmsWithHeap", "params": {"width": 32, "depth": 4, "heap_size": 2}}}},
                {"materialization": 8, "family": {"family": "exact", "kind": "increase"}},
            ]},
        }}

    def test_rejects_exact_membership_fallback(self):
        with self.assertRaisesRegex(ValueError, "contains ExactFallback"):
            validate_candidate_topk_artifact(self.artifact({"op": "exact_fallback", "reason": "unsupported"}))

    def test_rejects_uninstalled_heap_membership(self):
        artifact = self.artifact({"op": "read_materialization", "binding": {"materialization": 9}})
        with self.assertRaisesRegex(ValueError, "no installed heap"):
            validate_candidate_topk_artifact(artifact)

    def test_accepts_heap_membership_and_exact_values(self):
        validate_candidate_topk_artifact(
            self.artifact({"op": "read_materialization", "binding": {"materialization": 7}})
        )

    def test_ignores_plans_without_candidate_topk(self):
        validate_candidate_topk_artifact({"install_request": {
            "query_plan": {"entries": {"q": {"nodes": {"0": {"op": "exact_fallback"}}}}},
            "precompute_plan": {"schemas": []},
        }})

    def test_rejects_exact_value_fallback(self):
        artifact = self.artifact({"op": "read_materialization", "binding": {"materialization": 7}})
        artifact["install_request"]["query_plan"]["entries"]["q"]["nodes"]["3"] = {"op": "exact_fallback"}
        with self.assertRaisesRegex(ValueError, "contains ExactFallback"):
            validate_candidate_topk_artifact(artifact)

    def test_rejects_missing_or_cyclic_input_nodes(self):
        artifact = self.artifact({"op": "summary_estimate", "input": 99})
        with self.assertRaisesRegex(ValueError, "missing node 99"):
            validate_candidate_topk_artifact(artifact)
        artifact = self.artifact({"op": "summary_estimate", "input": 2})
        with self.assertRaisesRegex(ValueError, "contains a cycle"):
            validate_candidate_topk_artifact(artifact)

    def test_requires_two_local_summary_readouts_at_runtime(self):
        artifact = self.artifact({"op": "read_materialization", "binding": {"materialization": 7}})
        provenance = {"summary_readout_evaluations": 2, "exact_subquery_rpcs": 0,
                      "exact_subquery_evaluations": 0, "exact_branch_evaluations": 0}
        validate_candidate_topk_execution(artifact, [{"execution": "warm", "execution_provenance": provenance}])
        with self.assertRaisesRegex(ValueError, "both summary branches"):
            validate_candidate_topk_execution(artifact, [{"execution": "warm", "execution_provenance": {
                **provenance, "summary_readout_evaluations": 1}}])
        with self.assertRaisesRegex(ValueError, "used exact path"):
            validate_candidate_topk_execution(artifact, [{"execution": "warm", "execution_provenance": {
                **provenance, "exact_subquery_rpcs": 1}}])


if __name__ == "__main__":
    unittest.main()
