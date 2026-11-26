#!/usr/bin/env python3
"""Merge per-query InstallablePlans into one plan the data plane installs.

Until the Planner plans a whole workload jointly, each query is planned
alone; the data plane installs one plan, so the per-query DAGs are put side
by side with their node ids offset. Nothing is shared between them by the
merge (the data plane's SDS still stores identical stored outputs once).
The data plane validates the result (`InstallablePlan::validate`) when it
installs it.
"""
import copy
import json
import sys

# Payload fields that name another node (checked against the edges below).
NODE_REFERENCE_FIELDS = ("child", "children", "lhs", "rhs", "summary_input", "input", "inputs")


def _shift_payload(payload, offset, producers):
    """Offset every node reference in the operator body of `payload`."""
    (side, body), = payload.items()
    (op, fields), = body.items()
    if not isinstance(fields, dict):
        return
    for key in NODE_REFERENCE_FIELDS:
        if key not in fields:
            continue
        value = fields[key]
        if isinstance(value, int):
            assert value in producers, (op, key, value)
            fields[key] = value + offset
        elif isinstance(value, list):
            assert all(v in producers for v in value), (op, key, value)
            fields[key] = [v + offset for v in value]


def _shift_node_ids(value, offset):
    if isinstance(value, dict):
        for key, item in value.items():
            if (key == "node_id" or key.endswith("_node_id")) and isinstance(item, int):
                value[key] = item + offset
            elif (key.endswith("_node_ids") or key == "stored_outputs") and isinstance(item, list) \
                    and all(isinstance(i, int) for i in item):
                # `inputs.stored_outputs` (later Planner versions): node ids.
                value[key] = [i + offset for i in item]
            else:
                _shift_node_ids(item, offset)
    elif isinstance(value, list):
        for item in value:
            _shift_node_ids(item, offset)


def merge(plans):
    merged = {
        "dag": {"schema_version": None, "dag": {"nodes": [], "edges": [], "roots": [], "scalar_roots": []}},
        "queries": [],
        "stored_outputs": [],
        "query_time_raw_windows": [],
        "selection": None,
    }
    offset = 0
    for plan in plans:
        plan = copy.deepcopy(plan)
        doc = plan["dag"]
        merged["dag"]["schema_version"] = doc["schema_version"]
        dag = doc["dag"]
        assert not dag["scalar_roots"], "standalone scalar queries are not merged"
        producers = {}
        for edge in dag["edges"]:
            producers.setdefault(edge["consumer"], set()).add(edge["producer"])
        for node in dag["nodes"]:
            _shift_payload(node["payload"], offset, producers.get(node["id"], set()))
            node["id"] += offset
        for edge in dag["edges"]:
            edge["producer"] += offset
            edge["consumer"] += offset
        out = merged["dag"]["dag"]
        out["nodes"] += dag["nodes"]
        out["edges"] += dag["edges"]
        out["roots"] += [r + offset for r in dag["roots"]]
        for query in plan["queries"]:
            query["root_node_id"] += offset
        merged["queries"] += plan["queries"]
        # Node ids in the runtime inputs: `node_id` and every `*_node_id`,
        # wherever they sit (a stored output's pane, or its coverage and
        # inputs in later Planner versions).
        _shift_node_ids(plan["stored_outputs"], offset)
        merged["stored_outputs"] += plan["stored_outputs"]
        _shift_node_ids(plan["query_time_raw_windows"], offset)
        merged["query_time_raw_windows"] += plan["query_time_raw_windows"]
        offset = max(n["id"] for n in out["nodes"]) + 1
    return merged


if __name__ == "__main__":
    plans = [json.load(open(path)) for path in sys.argv[2:]]
    json.dump(merge(plans), open(sys.argv[1], "w"))
