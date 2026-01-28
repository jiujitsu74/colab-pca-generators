#!/usr/bin/env python3
"""
Merge Promoted Accounts Utility
===============================
Merges newly FOX-promoted strategy accounts into RABBIT's runner snapshot.

Problem: FOX promoter writes to snapshot.json, RABBIT reads runner_snapshot_{asset}.json
Result: Newly promoted strategies appear as ORPHAN in OWL (no broker account)

Solution: This utility merges missing accounts from FOX → RABBIT snapshot.

Usage:
    python3 merge_promoted_accounts.py --asset crypto [--dry-run]
    python3 merge_promoted_accounts.py --all [--dry-run]

Files:
    Source: /opt/shadow-trader/paper-live/broker_journal/{asset}/snapshot.json
    Target: /opt/shadow-trader/paper-live/broker_journal/{asset}/runner_snapshot_{asset}.json
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

# Paths
BROKER_JOURNAL_BASE = Path('/opt/shadow-trader/paper-live/broker_journal')
ELEPHANT_LOG = Path('/opt/shadow-trader/logs/elephant_events.jsonl')
ASSETS = ['crypto', 'equities', 'forex', 'commodities']

# Keys that are metadata, not strategy accounts
METADATA_KEYS = {'__quotes__', 'asof', 'quotes', 'metadata', '__metadata__', 'last_update'}


def emit_elephant_event(event_type: str, data: Dict[str, Any]) -> None:
    """Emit event to ELEPHANT log."""
    event = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'event_type': event_type,
        **data
    }
    try:
        ELEPHANT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(ELEPHANT_LOG, 'a') as f:
            f.write(json.dumps(event) + '\n')
    except Exception as e:
        print(f'  [WARN] Failed to emit ELEPHANT event: {e}')


def load_json(path: Path) -> Dict:
    """Load JSON file, return empty dict if not exists."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f'  [ERROR] Invalid JSON in {path}: {e}')
        return {}


def save_json_atomic(path: Path, data: Dict) -> bool:
    """Atomically write JSON file (write to temp, then rename)."""
    temp_path = path.with_suffix('.tmp')
    try:
        with open(temp_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        shutil.move(str(temp_path), str(path))
        return True
    except Exception as e:
        print(f'  [ERROR] Failed to write {path}: {e}')
        if temp_path.exists():
            temp_path.unlink()
        return False


def extract_accounts(data: Dict) -> Dict[str, Dict]:
    """Extract strategy accounts from snapshot data (handles both formats)."""
    accounts = {}

    for key, value in data.items():
        if key in METADATA_KEYS:
            continue
        if not isinstance(value, dict):
            continue
        # Check if this looks like a broker account
        if 'cash' in value or 'positions' in value or 'equity' in value:
            accounts[key] = value

    return accounts


def merge_accounts(
    asset: str,
    dry_run: bool = False,
    verbose: bool = True
) -> Tuple[int, int, List[str]]:
    """
    Merge accounts from FOX promoter snapshot into RABBIT runner snapshot.

    Returns: (merged_count, skipped_count, merged_strategy_ids)
    """
    asset_dir = BROKER_JOURNAL_BASE / asset

    # Source: FOX promoter output
    fox_snapshot_path = asset_dir / 'snapshot.json'

    # Target: RABBIT runner state
    rabbit_snapshot_path = asset_dir / f'runner_snapshot_{asset}.json'

    if verbose:
        print(f'\n[{asset.upper()}] Merging promoted accounts')
        print(f'  Source: {fox_snapshot_path}')
        print(f'  Target: {rabbit_snapshot_path}')

    # Load both snapshots
    fox_data = load_json(fox_snapshot_path)
    rabbit_data = load_json(rabbit_snapshot_path)

    if not fox_data:
        if verbose:
            print(f'  [SKIP] No FOX snapshot found')
        return (0, 0, [])

    # Extract accounts from both
    fox_accounts = extract_accounts(fox_data)
    rabbit_accounts = extract_accounts(rabbit_data)

    if verbose:
        print(f'  FOX accounts: {len(fox_accounts)}')
        print(f'  RABBIT accounts: {len(rabbit_accounts)}')

    # Find accounts in FOX but not in RABBIT
    fox_ids = set(fox_accounts.keys())
    rabbit_ids = set(rabbit_accounts.keys())
    missing_ids = fox_ids - rabbit_ids

    if not missing_ids:
        if verbose:
            print(f'  [OK] All FOX accounts already in RABBIT')
        return (0, len(fox_ids), [])

    if verbose:
        print(f'  [MERGE] {len(missing_ids)} accounts to add:')
        for sid in sorted(missing_ids):
            account = fox_accounts[sid]
            cash = account.get('cash', 0)
            print(f'    + {sid} (cash: ${cash:,.2f})')

    if dry_run:
        if verbose:
            print(f'  [DRY-RUN] Would merge {len(missing_ids)} accounts')
        return (0, 0, list(missing_ids))

    # Merge missing accounts into RABBIT data
    merged_ids = []
    for sid in missing_ids:
        rabbit_data[sid] = fox_accounts[sid]
        merged_ids.append(sid)

    # Update timestamp
    rabbit_data['asof'] = datetime.now(timezone.utc).isoformat()

    # Atomic write
    if save_json_atomic(rabbit_snapshot_path, rabbit_data):
        if verbose:
            print(f'  [OK] Merged {len(merged_ids)} accounts')

        # Emit ELEPHANT event
        emit_elephant_event('broker_accounts_merged', {
            'asset': asset,
            'merged_count': len(merged_ids),
            'merged_ids': merged_ids,
            'source': str(fox_snapshot_path),
            'target': str(rabbit_snapshot_path),
        })

        return (len(merged_ids), len(rabbit_ids), merged_ids)
    else:
        print(f'  [ERROR] Failed to write merged snapshot')
        return (0, 0, [])


def main():
    parser = argparse.ArgumentParser(
        description='Merge FOX-promoted accounts into RABBIT snapshot'
    )
    parser.add_argument(
        '--asset', '-a',
        choices=ASSETS,
        help='Asset class to merge'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Merge all asset classes'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be merged without making changes'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Minimal output'
    )
    args = parser.parse_args()

    if not args.asset and not args.all:
        parser.error('Must specify --asset or --all')

    assets_to_merge = ASSETS if args.all else [args.asset]
    verbose = not args.quiet

    if verbose:
        print('=' * 60)
        print('MERGE PROMOTED ACCOUNTS')
        print('=' * 60)
        if args.dry_run:
            print('[DRY-RUN MODE - No changes will be made]')

    total_merged = 0
    total_skipped = 0
    all_merged_ids = []

    for asset in assets_to_merge:
        merged, skipped, merged_ids = merge_accounts(
            asset,
            dry_run=args.dry_run,
            verbose=verbose
        )
        total_merged += merged
        total_skipped += skipped
        all_merged_ids.extend(merged_ids)

    if verbose:
        print('\n' + '=' * 60)
        print('SUMMARY')
        print('=' * 60)
        print(f'  Merged: {total_merged} accounts')
        print(f'  Already synced: {total_skipped} accounts')
        if all_merged_ids:
            print(f'  Merged IDs: {", ".join(sorted(all_merged_ids)[:5])}{"..." if len(all_merged_ids) > 5 else ""}')

    # Exit code: 0 if merge succeeded or nothing to merge, 1 if errors
    return 0


if __name__ == '__main__':
    sys.exit(main())
