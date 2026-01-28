#!/usr/bin/env python3
"""
Regime Contract Validation Test
===============================
Validates that regime labels across CB, BRAIN, FOX, and OWL are properly mapped.

Run periodically (weekly cron or on deploy) to catch drift.

Usage:
    python3 regime_contract_test.py [--emit-elephant] [--verbose]

Output:
    - Confusion matrix of CB vs FOX classifications
    - Unmapped label warnings
    - Mapping confidence scores
    - Optional ELEPHANT event emission for drift alerts
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any

import yaml

# Paths
CONTRACT_PATH = Path("/opt/shadow-trader/config/regime_contract.yaml")
SPAWN_POLICY_PATH = Path("/opt/shadow-trader/fox/regime_spawn_policy.yaml")
ELEPHANT_PATH = Path("/opt/shadow-trader/logs/regime_contract_events.jsonl")

# Add paths for imports
sys.path.insert(0, "/opt/shadow-trader/fox")
sys.path.insert(0, "/opt/shadow-trader/lib")


def load_contract() -> Dict:
    """Load the regime contract YAML."""
    if not CONTRACT_PATH.exists():
        print(f"ERROR: Contract not found at {CONTRACT_PATH}")
        sys.exit(1)
    with open(CONTRACT_PATH) as f:
        return yaml.safe_load(f)


def load_spawn_policy() -> Dict:
    """Load BRAIN spawn policy to extract labels."""
    if not SPAWN_POLICY_PATH.exists():
        return {}
    with open(SPAWN_POLICY_PATH) as f:
        return yaml.safe_load(f)


def get_fox_classifications(days: int = 365) -> Dict[str, List[Tuple[str, str]]]:
    """
    Run FOX classifier on historical data for each asset class.
    Returns: {asset: [(date, fox_regime), ...]}
    """
    try:
        from regime_aware_backtest import load_historical_data, classify_regime_from_pc1
    except ImportError as e:
        print(f"WARNING: Could not import FOX classifier: {e}")
        return {}

    results = {}
    for asset in ["crypto", "equities", "forex", "commodities"]:
        try:
            daily = load_historical_data(asset)
            if daily is None or "PC1" not in daily.columns:
                continue

            # Filter to last N days
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            daily = daily[daily.index >= cutoff]

            if len(daily) == 0:
                continue

            regimes = classify_regime_from_pc1(daily["PC1"])
            results[asset] = [(str(d.date()), r) for d, r in zip(daily.index, regimes)]
        except Exception as e:
            print(f"WARNING: FOX classification failed for {asset}: {e}")

    return results


def get_cb_classifications(days: int = 365) -> List[Tuple[str, str]]:
    """
    Get Crystal Ball regime history.
    Returns: [(date, cb_regime), ...]
    """
    # Try to load from CB cache history if available
    cb_history_path = Path("/opt/shadow-trader/cache/macro/crystal_ball_history.jsonl")
    if not cb_history_path.exists():
        # Fall back to current regime only
        cb_current_path = Path("/opt/shadow-trader/cache/macro/crystal_ball_regime.json")
        if cb_current_path.exists():
            with open(cb_current_path) as f:
                data = json.load(f)
            regime = data.get("global", {}).get("market_regime", "UNKNOWN")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            return [(today, regime)]
        return []

    results = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with open(cb_history_path) as f:
            for line in f:
                if not line.strip():
                    continue
                entry = json.loads(line)
                ts = entry.get("as_of_utc") or entry.get("timestamp")
                if not ts:
                    continue
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt < cutoff:
                    continue
                regime = entry.get("global", {}).get("market_regime", "UNKNOWN")
                results.append((dt.strftime("%Y-%m-%d"), regime))
    except Exception as e:
        print(f"WARNING: Failed to load CB history: {e}")

    return results


def build_confusion_matrix(
    fox_results: Dict[str, List[Tuple[str, str]]],
    cb_results: List[Tuple[str, str]],
) -> Dict[str, Dict[str, int]]:
    """
    Build confusion matrix: count(CB_regime, FOX_regime).
    Since CB is global and FOX is per-asset, we compare against each asset.
    """
    # Index CB by date
    cb_by_date = {d: r for d, r in cb_results}

    matrix = defaultdict(lambda: defaultdict(int))

    for asset, fox_data in fox_results.items():
        for date, fox_regime in fox_data:
            cb_regime = cb_by_date.get(date)
            if cb_regime:
                matrix[cb_regime][fox_regime] += 1

    return dict(matrix)


def validate_mappings(
    contract: Dict,
    spawn_policy: Dict,
    fox_results: Dict[str, List[Tuple[str, str]]],
    verbose: bool = False,
) -> Tuple[bool, List[str], Dict]:
    """
    Validate that all observed labels are mapped.
    Returns: (passed, warnings, stats)
    """
    warnings = []
    stats = {"unmapped_brain": [], "unmapped_cb": [], "mapping_confidence": {}}

    brain_to_fox = contract.get("brain_to_fox", {})
    cb_to_fox = contract.get("cb_global_to_fox", {})
    fox_labels = set(contract.get("fox_labels", []))

    # Check BRAIN labels from spawn policy
    for asset, asset_config in spawn_policy.items():
        if not isinstance(asset_config, dict):
            continue
        for regime_key in asset_config.keys():
            if regime_key not in brain_to_fox:
                warnings.append(f"BRAIN label '{regime_key}' ({asset}) not in brain_to_fox mapping")
                stats["unmapped_brain"].append(regime_key)

    # Check FOX classifier outputs match expected labels
    observed_fox = set()
    for asset, fox_data in fox_results.items():
        for _, regime in fox_data:
            observed_fox.add(regime)

    unexpected_fox = observed_fox - fox_labels
    if unexpected_fox:
        for label in unexpected_fox:
            warnings.append(f"FOX classifier emitted unexpected label: {label}")

    # Check mapping targets are valid FOX labels
    for brain_label, fox_label in brain_to_fox.items():
        if fox_label not in fox_labels:
            warnings.append(f"brain_to_fox[{brain_label}] -> {fox_label} is not a valid FOX label")

    for cb_label, fox_label in cb_to_fox.items():
        if fox_label not in fox_labels:
            warnings.append(f"cb_global_to_fox[{cb_label}] -> {fox_label} is not a valid FOX label")

    passed = len(warnings) == 0
    return passed, warnings, stats


def compute_mapping_confidence(
    confusion_matrix: Dict[str, Dict[str, int]],
    contract: Dict,
) -> Dict[str, float]:
    """
    For each CB label, compute confidence of the mapping.
    Confidence = max_fox_count / total_count
    """
    cb_to_fox = contract.get("cb_global_to_fox", {})
    confidence = {}

    for cb_label, fox_counts in confusion_matrix.items():
        total = sum(fox_counts.values())
        if total == 0:
            confidence[cb_label] = 0.0
            continue

        mapped_fox = cb_to_fox.get(cb_label, "CONSOLIDATION")
        mapped_count = fox_counts.get(mapped_fox, 0)
        confidence[cb_label] = mapped_count / total

    return confidence


def emit_elephant_event(
    passed: bool,
    warnings: List[str],
    confusion_matrix: Dict,
    confidence: Dict[str, float],
):
    """Emit drift event to ELEPHANT."""
    event = {
        "event_type": "regime_contract_drift",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "warning_count": len(warnings),
        "warnings": warnings[:10],  # Limit
        "confusion_matrix_summary": {
            cb: dict(sorted(fox.items(), key=lambda x: -x[1])[:3])
            for cb, fox in list(confusion_matrix.items())[:5]
        },
        "mapping_confidence": confidence,
    }

    ELEPHANT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ELEPHANT_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")

    print(f"Emitted ELEPHANT event to {ELEPHANT_PATH}")


def print_confusion_matrix(matrix: Dict[str, Dict[str, int]]):
    """Pretty print the confusion matrix."""
    if not matrix:
        print("  (no data)")
        return

    # Get all FOX labels
    all_fox = set()
    for fox_counts in matrix.values():
        all_fox.update(fox_counts.keys())
    all_fox = sorted(all_fox)

    # Header
    header = "CB \\ FOX".ljust(20) + "".join(f.ljust(15) for f in all_fox)
    print(header)
    print("-" * len(header))

    # Rows
    for cb_label in sorted(matrix.keys()):
        row = cb_label.ljust(20)
        for fox_label in all_fox:
            count = matrix[cb_label].get(fox_label, 0)
            row += str(count).ljust(15)
        print(row)


def main():
    parser = argparse.ArgumentParser(description="Validate regime contract")
    parser.add_argument("--emit-elephant", action="store_true", help="Emit event to ELEPHANT")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--days", type=int, default=365, help="Days of history for confusion matrix")
    args = parser.parse_args()

    print("=" * 70)
    print("REGIME CONTRACT VALIDATION")
    print("=" * 70)
    print()

    # Load contract
    contract = load_contract()
    print(f"Contract version: {contract.get('version', 'unknown')}")

    # Load spawn policy
    spawn_policy = load_spawn_policy()
    print(f"Spawn policy assets: {list(spawn_policy.keys())[:4]}")

    # Get FOX classifications
    print(f"\nRunning FOX classifier on last {args.days} days...")
    fox_results = get_fox_classifications(args.days)
    for asset, data in fox_results.items():
        print(f"  {asset}: {len(data)} days classified")

    # Get CB classifications
    print("\nLoading Crystal Ball history...")
    cb_results = get_cb_classifications(args.days)
    print(f"  CB entries: {len(cb_results)}")

    # Build confusion matrix
    print("\nBuilding confusion matrix...")
    confusion_matrix = build_confusion_matrix(fox_results, cb_results)

    print("\n=== Confusion Matrix (CB vs FOX) ===")
    print_confusion_matrix(confusion_matrix)

    # Compute mapping confidence
    confidence = compute_mapping_confidence(confusion_matrix, contract)
    print("\n=== Mapping Confidence ===")
    for cb_label, conf in sorted(confidence.items()):
        mapped_to = contract.get("cb_global_to_fox", {}).get(cb_label, "?")
        status = "✓" if conf >= 0.4 else "⚠"
        print(f"  {status} {cb_label} -> {mapped_to}: {conf:.1%}")

    # Validate mappings
    print("\n=== Validation ===")
    passed, warnings, stats = validate_mappings(contract, spawn_policy, fox_results, args.verbose)

    if warnings:
        print("Warnings:")
        for w in warnings:
            print(f"  ⚠ {w}")
    else:
        print("  ✓ All labels properly mapped")

    # Emit to ELEPHANT if requested
    if args.emit_elephant:
        emit_elephant_event(passed, warnings, confusion_matrix, confidence)

    # Exit code
    print()
    if passed:
        print("✅ REGIME CONTRACT VALID")
        return 0
    else:
        print("❌ REGIME CONTRACT VALIDATION FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
