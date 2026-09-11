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

    def candidate_filtered_artifact(self):
        artifact = self.artifact({"op": "read_materialization", "binding": {"materialization": 7}})
        request = artifact["install_request"]
        request["query_plan"]["entries"]["q"]["nodes"]["3"] = {
            "op": "logical",
            "operator": {"kind": "candidate_exact_subquery", "query": "sum by (job) (rate(m[5m]))",
                         "item_label": "job"},
            "inputs": [1],
        }
        request["precompute_plan"]["schemas"] = request["precompute_plan"]["schemas"][:1]
        return artifact

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

    def test_accepts_one_heap_and_candidate_filtered_external_exact(self):
        validate_candidate_topk_artifact(self.candidate_filtered_artifact())

    def test_candidate_filtered_contract_rejects_local_exact_state_or_unshared_input(self):
        artifact = self.candidate_filtered_artifact()
        artifact["install_request"]["precompute_plan"]["schemas"].append(
            {"materialization": 8, "family": {"family": "exact", "kind": "increase"}})
        with self.assertRaisesRegex(ValueError, "must not install"):
            validate_candidate_topk_artifact(artifact)
        artifact = self.candidate_filtered_artifact()
        artifact["install_request"]["query_plan"]["entries"]["q"]["nodes"]["3"]["inputs"] = [2]
        with self.assertRaisesRegex(ValueError, "shared membership"):
            validate_candidate_topk_artifact(artifact)

    def test_candidate_filtered_execution_requires_hybrid_one_rpc_and_one_summary_read(self):
        artifact = self.candidate_filtered_artifact()
        provenance = {"detail": "hybrid", "raw_scan_evaluations": 0,
                      "summary_readout_evaluations": 1, "exact_subquery_rpcs": 1,
                      "exact_subquery_evaluations": 1, "exact_branch_evaluations": 1}
        validate_candidate_topk_execution(
            artifact, [{"execution": "hybrid", "execution_provenance": provenance}])
        for key in ("summary_readout_evaluations", "exact_subquery_rpcs",
                    "exact_subquery_evaluations", "exact_branch_evaluations"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "invalid provenance"):
                validate_candidate_topk_execution(artifact, [{"execution": "hybrid",
                    "execution_provenance": {**provenance, key: 0}}])
        with self.assertRaisesRegex(ValueError, "hybrid execution"):
            validate_candidate_topk_execution(
                artifact, [{"execution": "hybrid", "execution_provenance": {
                    **provenance, "detail": "external_exact"}}])


if __name__ == "__main__":
    unittest.main()

class PerQueryAccuracyTests(unittest.TestCase):
    def test_entropy_absolute_error_does_not_accept_relative_tolerance(self):
        from types import SimpleNamespace
        from calibrate_runtime import comparison_tolerances
        from compare import compare_results
        args = SimpleNamespace(relative_tolerance=.9, absolute_tolerance=.9)
        relative, absolute = comparison_tolerances({"accuracy_validation": {"metric": "absolute_bits", "bound": .1}}, args)
        actual = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [0, "10.2"]}]}}
        exact = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [0, "10"]}]}}
        self.assertEqual((relative, absolute), (0, .1))
        self.assertFalse(compare_results(actual, exact, relative, absolute)["equal"])

    def test_unknown_metric_and_invalid_bound_are_rejected(self):
        from types import SimpleNamespace
        from calibrate_runtime import comparison_tolerances
        for metric, bound in [("rank", .1), ("relative", float("nan")), ("absolute_bits", -1), ("exact", .1)]:
            with self.assertRaises(ValueError):
                comparison_tolerances({"accuracy_validation": {"metric": metric, "bound": bound}}, SimpleNamespace())

    def test_result_cache_policy_is_explicit_on_both_query_endpoints(self):
        from urllib.parse import parse_qs
        from calibrate_runtime import query_parameters
        row = {"query": "count_over_time(m[1h])", "eval_timestamp_ms": 1234}
        self.assertNotIn("nocache", parse_qs(query_parameters(row, False)))
        self.assertEqual(parse_qs(query_parameters(row, True))["nocache"], ["1"])
