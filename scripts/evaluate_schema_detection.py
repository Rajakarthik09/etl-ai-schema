#!/usr/bin/env python3
"""
Evaluate schema change detection accuracy for schema change JSONs.

This script compares JSON change summaries (added/removed/renamed columns)
with a configurable ground truth and reports precision/recall/F1 for each
change type (added, removed, renamed) and overall (added+removed). It writes
the per-scenario metrics as JSON.

By default it evaluates the NYC taxi scenarios using
docs/schema_detection_ground_truth.json, but it can be pointed at any set
of change JSONs and ground truth definitions.
"""

import json
import os
from typing import Dict, List, Tuple, Any

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")


def load_changes(name: str) -> Dict:
    path = os.path.join(SCHEMAS_DIR, f"{name}.json")
    with open(path, "r") as f:
        return json.load(f)


def prf(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
    return prec, rec, f1


def eval_scenario(
    name: str,
    gt_added: List[str],
    gt_removed: List[str],
    gt_renamed: List[Tuple[str, str]] = None,
) -> Dict:
    det = load_changes(name)
    det_renamed = {
        (r["old_column"], r["new_column"])
        for r in det.get("renamed_columns", [])
        if "old_column" in r and "new_column" in r
    }

    # Start from raw detected added/removed sets
    det_added = {c["column"] for c in det.get("added_columns", [])}
    det_removed = {c["column"] for c in det.get("removed_columns", [])}

    # Exclude columns that participate in a rename so that renamed endpoints
    # are only evaluated in the "renamed" category, not double-counted as
    # pure added/removed.
    renamed_old = {old for (old, _new) in det_renamed}
    renamed_new = {_new for (old, _new) in det_renamed}
    det_added -= renamed_new
    det_removed -= renamed_old

    gt_added_set = set(gt_added)
    gt_removed_set = set(gt_removed)
    gt_renamed_set = set(gt_renamed or [])

    tp_added = len(det_added & gt_added_set)
    fp_added = len(det_added - gt_added_set)
    fn_added = len(gt_added_set - det_added)

    tp_removed = len(det_removed & gt_removed_set)
    fp_removed = len(det_removed - gt_removed_set)
    fn_removed = len(gt_removed_set - det_removed)

    p_add, r_add, f_add = prf(tp_added, fp_added, fn_added)
    p_rem, r_rem, f_rem = prf(tp_removed, fp_removed, fn_removed)

    # Rename metrics (string-similarity-based rename detection)
    tp_ren = len(det_renamed & gt_renamed_set)
    fp_ren = len(det_renamed - gt_renamed_set)
    fn_ren = len(gt_renamed_set - det_renamed)
    p_ren, r_ren, f_ren = prf(tp_ren, fp_ren, fn_ren)

    tp_all = tp_added + tp_removed
    fp_all = fp_added + fp_removed
    fn_all = fn_added + fn_removed
    p_all, r_all, f_all = prf(tp_all, fp_all, fn_all)

    return {
        "scenario": name,
        "added": {
            "tp": tp_added,
            "fp": fp_added,
            "fn": fn_added,
            "precision": p_add,
            "recall": r_add,
            "f1": f_add,
        },
        "removed": {
            "tp": tp_removed,
            "fp": fp_removed,
            "fn": fn_removed,
            "precision": p_rem,
            "recall": r_rem,
            "f1": f_rem,
        },
        "renamed": {
            "tp": tp_ren,
            "fp": fp_ren,
            "fn": fn_ren,
            "precision": p_ren,
            "recall": r_ren,
            "f1": f_ren,
        },
        "overall": {
            "tp": tp_all,
            "fp": fp_all,
            "fn": fn_all,
            "precision": p_all,
            "recall": r_all,
            "f1": f_all,
        },
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate schema change detection (precision/recall/F1) "
            "for added/removed/renamed columns."
        )
    )
    parser.add_argument(
        "--ground-truth",
        default=os.path.join(DOCS_DIR, "schema_detection_ground_truth.json"),
        help=(
            "Path to JSON file with ground truth definitions. "
            "Format: { "
            "\"scenario_name\": {"
            "\"added\": [...], \"removed\": [...], "
            "\"renamed\": [[old, new], ...]"
            "} }"
        ),
    )
    parser.add_argument(
        "--scenario",
        default=None,
        help="If set, only evaluate this scenario (key in the ground-truth JSON).",
    )
    parser.add_argument(
        "--output", default=None, help="Write JSON results to this file (in addition to stdout)."
    )
    args = parser.parse_args()

    # Load ground truth configuration
    if not os.path.isfile(args.ground_truth):
        raise FileNotFoundError(f"Ground truth file not found: {args.ground_truth}")
    with open(args.ground_truth, "r") as f:
        gt_config: Dict[str, Any] = json.load(f)

    # Decide which scenarios to evaluate
    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = sorted(gt_config.keys())

    results: Dict[str, Any] = {}
    for scenario in scenarios:
        cfg = gt_config.get(scenario)
        if not cfg:
            continue
        changes_file = os.path.join(SCHEMAS_DIR, f"{scenario}.json")
        if not os.path.isfile(changes_file):
            # No detection output for this scenario yet
            results[scenario] = None
            continue
        gt_added = cfg.get("added", [])
        gt_removed = cfg.get("removed", [])
        gt_renamed = [tuple(p) for p in cfg.get("renamed", [])]
        metrics = eval_scenario(
            scenario,
            gt_added=gt_added,
            gt_removed=gt_removed,
            gt_renamed=gt_renamed,
        )
        results[scenario] = metrics

    out_str = json.dumps(results, indent=2)
    print(out_str)
    if args.output:
        with open(args.output, "w") as f:
            f.write(out_str)


if __name__ == "__main__":
    main()

