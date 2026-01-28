#!/usr/bin/env python3
"""
Regime Vocabulary Smoke Test
=============================
Validates that ALL regime labels across the system can be normalized
to a canonical regime. Fails loudly if drift is detected.

Sources scanned:
  1. Crystal Ball cache (global regime)
  2. BRAIN spawn policy (regime_spawn_policy.yaml) - top-level keys per asset
  3. FOX classifier outputs (regime_aware_backtest.py)
  4. Coverage matrix (coverage_matrix.yaml)
  5. Regime contract (regime_contract.yaml)
  6. Active strategy YAMLs (regime_guard.allowed_regimes)

Usage:
    python3 /opt/shadow-trader/bin/regime_vocab_smoke_test.py
    
Exit codes:
    0 = All labels mappable
    2 = Unmapped labels found (drift detected)
"""

import json
import re
import sys
import yaml
from pathlib import Path
from typing import Dict, Set, Optional

# Paths
COVERAGE_MATRIX = Path('/opt/shadow-trader/brain/config/coverage_matrix.yaml')
SPAWN_POLICY = Path('/opt/shadow-trader/fox/regime_spawn_policy.yaml')
REGIME_CONTRACT = Path('/opt/shadow-trader/config/regime_contract.yaml')
CRYSTAL_BALL_CACHE = Path('/opt/shadow-trader/cache/macro/crystal_ball_regime.json')
FOX_BACKTEST = Path('/opt/shadow-trader/fox/regime_aware_backtest.py')
TOURNAMENT_DIRS = {
    'crypto': Path('/opt/shadow-trader/tournament-crypto/active'),
    'equities': Path('/opt/shadow-trader/tournament-equities/active'),
    'forex': Path('/opt/shadow-trader/tournament-forex/active'),
    'commodities': Path('/opt/shadow-trader/tournament-commodities/active'),
}

# Config keys that look like regime labels but aren't
CONFIG_KEY_EXCLUSIONS = {
    'EXPLORATION_PCT', 'MIN_PER_ARCHETYPE', 'NEUTRAL_WEIGHT', 'REGIME_WEIGHT',
    'WEIGHT', 'PCT', 'MIN', 'MAX', 'DEFAULT', 'ENABLED', 'DISABLED',
}


def load_yaml(p: Path) -> dict:
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text()) or {}


def load_json(p: Path) -> dict:
    if not p.exists():
        return {}
    return json.loads(p.read_text()) or {}


def build_alias_to_canonical(regime_map: dict) -> Dict[str, str]:
    """Build uppercase alias -> canonical mapping."""
    alias = {}
    for canonical, labels in (regime_map or {}).items():
        # Canonical itself is valid
        alias[str(canonical).strip().upper()] = canonical
        for lab in labels or []:
            alias[str(lab).strip().upper()] = canonical
    return alias


def normalize(label: str, alias_to_canonical: Dict[str, str]) -> Optional[str]:
    """Normalize a label to canonical form."""
    if not label:
        return None
    key = str(label).strip().upper()
    return alias_to_canonical.get(key)


def collect_spawn_policy_labels(doc: dict) -> Set[str]:
    """
    Extract regime labels from BRAIN spawn policy.
    Only extracts top-level keys under each asset (these are regime names).
    """
    labels = set()
    for asset, regimes in doc.items():
        if not isinstance(regimes, dict):
            continue
        for regime_name, regime_config in regimes.items():
            # Only include if it looks like a regime config (has archetypes or cb_conditions)
            if isinstance(regime_config, dict):
                if 'archetypes' in regime_config or 'cb_conditions' in regime_config or 'weight' in regime_config:
                    labels.add(str(regime_name).upper())
    return labels - CONFIG_KEY_EXCLUSIONS


def collect_regime_contract_labels(doc: dict) -> Set[str]:
    """Extract all labels from regime contract."""
    labels = set()
    
    # FOX labels
    for lab in doc.get('fox_labels', []):
        labels.add(str(lab).upper())
    
    # Crystal Ball labels
    for lab in doc.get('crystal_ball_labels', []):
        labels.add(str(lab).upper())
    
    # BRAIN labels (per-asset)
    for asset, asset_labels in doc.get('brain_labels', {}).items():
        for lab in asset_labels or []:
            labels.add(str(lab).upper())
    
    # brain_to_fox mapping keys
    for lab in doc.get('brain_to_fox', {}).keys():
        labels.add(str(lab).upper())
    
    # cb_global_to_fox mapping keys
    for lab in doc.get('cb_global_to_fox', {}).keys():
        labels.add(str(lab).upper())
    
    return labels - CONFIG_KEY_EXCLUSIONS


def collect_crystal_ball_labels(doc: dict) -> Set[str]:
    """Extract current regime from Crystal Ball cache."""
    labels = set()
    
    global_regime = doc.get('global', {}).get('market_regime')
    if global_regime:
        labels.add(str(global_regime).upper())
    
    vix_regime = doc.get('global', {}).get('vix_regime')
    if vix_regime:
        labels.add(str(vix_regime).upper())
    
    return labels - CONFIG_KEY_EXCLUSIONS


def collect_fox_classifier_labels(fox_path: Path) -> Set[str]:
    """Extract regime labels from FOX classifier code."""
    labels = set()
    if not fox_path.exists():
        return labels
    
    code = fox_path.read_text()
    
    # Find BRAIN_TO_FOX_REGIME dict
    match = re.search(r'BRAIN_TO_FOX_REGIME\s*=\s*\{([^}]+)\}', code, re.DOTALL)
    if match:
        dict_content = match.group(1)
        # Extract quoted strings
        for lab in re.findall(r'["\']([A-Z][A-Z0-9_]+)["\']', dict_content):
            labels.add(lab)
    
    # Find regime classification outputs (return statements with regime strings)
    for lab in re.findall(r'return\s+["\']([A-Z][A-Z0-9_]+)["\']', code):
        labels.add(lab)
    
    return labels - CONFIG_KEY_EXCLUSIONS


def collect_strategy_allowed_regimes() -> Set[str]:
    """Extract allowed_regimes from active strategy YAMLs."""
    labels = set()
    
    for asset, dir_path in TOURNAMENT_DIRS.items():
        if not dir_path.exists():
            continue
        for yaml_file in dir_path.glob('*.yaml'):
            try:
                doc = yaml.safe_load(yaml_file.read_text())
                allowed = doc.get('regime_guard', {}).get('allowed_regimes', [])
                for lab in allowed or []:
                    labels.add(str(lab).upper())
            except Exception:
                continue
    
    return labels - CONFIG_KEY_EXCLUSIONS


def main():
    print('=' * 70)
    print('REGIME VOCABULARY SMOKE TEST')
    print('=' * 70)
    
    # Load coverage matrix
    cov = load_yaml(COVERAGE_MATRIX)
    regime_map = cov.get('regime_map', {})
    alias_to_canonical = build_alias_to_canonical(regime_map)
    canonical_regimes = set(regime_map.keys())
    
    print(f'\n[1] Coverage Matrix')
    print(f'    Canonical regimes: {sorted(canonical_regimes)}')
    print(f'    Total aliases: {len(alias_to_canonical)}')
    
    # Collect all labels from all sources
    all_labels: Dict[str, Set[str]] = {}
    
    # Source 1: BRAIN spawn policy
    spawn_policy = load_yaml(SPAWN_POLICY)
    all_labels['spawn_policy'] = collect_spawn_policy_labels(spawn_policy)
    print(f'\n[2] BRAIN Spawn Policy: {len(all_labels["spawn_policy"])} labels')
    for lab in sorted(all_labels['spawn_policy']):
        print(f'    - {lab}')
    
    # Source 2: Regime contract
    contract = load_yaml(REGIME_CONTRACT)
    all_labels['regime_contract'] = collect_regime_contract_labels(contract)
    print(f'\n[3] Regime Contract: {len(all_labels["regime_contract"])} labels')
    
    # Source 3: Crystal Ball cache
    cb_cache = load_json(CRYSTAL_BALL_CACHE)
    all_labels['crystal_ball'] = collect_crystal_ball_labels(cb_cache)
    print(f'[4] Crystal Ball Cache: {len(all_labels["crystal_ball"])} labels')
    
    # Source 4: FOX classifier
    all_labels['fox_classifier'] = collect_fox_classifier_labels(FOX_BACKTEST)
    print(f'[5] FOX Classifier: {len(all_labels["fox_classifier"])} labels')
    
    # Source 5: Active strategies
    all_labels['active_strategies'] = collect_strategy_allowed_regimes()
    print(f'[6] Active Strategies: {len(all_labels["active_strategies"])} labels')
    
    # Find unmapped labels
    print(f'\n' + '=' * 70)
    print('UNMAPPED LABELS BY SOURCE')
    print('=' * 70)
    
    total_unmapped = set()
    unmapped_by_source: Dict[str, Set[str]] = {}
    
    for source, labels in all_labels.items():
        unmapped = set()
        for lab in labels:
            if normalize(lab, alias_to_canonical) is None:
                unmapped.add(lab)
                total_unmapped.add(lab)
        unmapped_by_source[source] = unmapped
        
        if unmapped:
            print(f'\n{source}:')
            for u in sorted(unmapped):
                print(f'  - {u}')
        else:
            print(f'\n{source}: ✅ All mapped')
    
    # Summary
    print(f'\n' + '=' * 70)
    print('SUMMARY')
    print('=' * 70)
    
    if total_unmapped:
        print(f'\n❌ DRIFT DETECTED: {len(total_unmapped)} unmapped labels')
        print(f'\nAdd these to coverage_matrix.yaml regime_map:')
        for u in sorted(total_unmapped):
            # Suggest canonical based on name heuristics
            suggestion = 'unknown'
            u_lower = u.lower()
            if any(x in u_lower for x in ['trend', 'momentum', 'bull', 'risk_on', 'risk-on']):
                suggestion = 'trend_up'
            elif any(x in u_lower for x in ['bear', 'risk_off', 'risk-off']):
                suggestion = 'trend_down'
            elif any(x in u_lower for x in ['crash', 'crisis']):
                suggestion = 'crash'
            elif any(x in u_lower for x in ['consol', 'chop', 'range', 'meanrev', 'neutral']):
                suggestion = 'chop'
            elif any(x in u_lower for x in ['high_vol', 'elevated']):
                suggestion = 'high_vol'
            elif any(x in u_lower for x in ['low_vol']):
                suggestion = 'low_vol'
            print(f'  {u} -> {suggestion}')
        
        return 2
    else:
        print(f'\n✅ All {sum(len(v) for v in all_labels.values())} labels are mappable')
        print(f'   Canonical regimes: {sorted(canonical_regimes)}')
        return 0


if __name__ == '__main__':
    sys.exit(main())
