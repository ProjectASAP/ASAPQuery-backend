"""Fail-closed validation for calibrated CandidateTopK artifacts."""
import unittest

from calibrate_runtime import validate_candidate_topk_artifact


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


if __name__ == "__main__":
    unittest.main()
