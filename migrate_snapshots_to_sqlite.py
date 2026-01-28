#!/usr/bin/env python3
"""
Migrate JSON Snapshots to SQLite
================================
One-time migration script to move broker state from JSON files to SQLite database.

Sources:
  - /opt/shadow-trader/paper-live/broker_journal/{asset}/snapshot.json
  - /opt/shadow-trader/paper-live/broker_journal/{asset}/runner_snapshot_{asset}.json

Target:
  - /opt/shadow-trader/data/broker.db

Usage:
    python3 migrate_snapshots_to_sqlite.py [--dry-run] [--no-backup]

The script:
  1. Reads all JSON snapshot files
  2. Merges accounts (runner_snapshot takes precedence for balance data)
  3. Imports into SQLite database
  4. Validates row counts match
  5. Renames JSON files to .bak (unless --no-backup)
"""

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Set, Tuple

# Import our new database module
# In production this will be: from broker_db import BrokerDB
# For migration, we include it inline or import from same directory
try:
    from broker_db import BrokerDB
except ImportError:
    # If running standalone, add current directory to path
    sys.path.insert(0, str(Path(__file__).parent))
    from broker_db import BrokerDB

# Paths
BROKER_JOURNAL_BASE = Path('/opt/shadow-trader/paper-live/broker_journal')
DB_PATH = Path('/opt/shadow-trader/data/broker.db')
ASSETS = ['crypto', 'equities', 'forex', 'commodities']

# Keys that are metadata, not strategy accounts
METADATA_KEYS = {'__quotes__', 'asof', 'quotes', 'metadata', '__metadata__', 'last_update', 'version'}


def load_json(path: Path) -> Dict:
    """Load JSON file, return empty dict if not exists."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f'  [WARN] Invalid JSON in {path}: {e}')
        return {}


def extract_accounts_from_snapshot(data: Dict) -> Dict[str, Dict]:
    """
    Extract strategy accounts from snapshot data.
    Handles both nested ({"accounts": {...}}) and flat formats.
    """
    accounts = {}

    # Check for nested accounts structure (FOX promoter format)
    if 'accounts' in data and isinstance(data['accounts'], dict):
        source = data['accounts']
    else:
        source = data

    for key, value in source.items():
        if key in METADATA_KEYS:
            continue
        if not isinstance(value, dict):
            continue
        # Check if this looks like a broker account
        if 'cash' in value or 'positions' in value or 'equity' in value:
            accounts[key] = value

    return accounts


def merge_account_data(
    fox_account: Dict[str, Any],
    runner_account: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge account data from FOX promoter and RABBIT runner snapshots.
    Runner data takes precedence for dynamic fields (cash, positions).
    FOX data used for creation metadata.
    """
    merged = {}

    # Static metadata from FOX (if available)
    merged['created_at'] = fox_account.get('created_at') or runner_account.get('created_at')
    merged['created_by'] = fox_account.get('created_by') or runner_account.get('created_by')
    merged['initial_capital'] = fox_account.get('initial_capital') or fox_account.get('cash', 10000.0)
    merged['tier'] = fox_account.get('tier') or runner_account.get('tier', 'canary')
    merged['family'] = fox_account.get('family') or runner_account.get('family')

    # Dynamic data from runner (takes precedence)
    merged['cash'] = runner_account.get('cash', fox_account.get('cash', 10000.0))
    merged['equity'] = runner_account.get('equity', fox_account.get('account_equity', merged['cash']))
    merged['pnl_realized'] = runner_account.get('pnl_realized', fox_account.get('pnl_realized', 0.0))
    merged['sod_equity'] = runner_account.get('sod_equity', fox_account.get('sod_equity', 0.0))
    merged['sod_date'] = runner_account.get('sod_date', fox_account.get('sod_date', ''))
    merged['fees_today'] = runner_account.get('fees_today', fox_account.get('fees_today', 0.0))
    merged['trading_disabled'] = runner_account.get('trading_disabled', fox_account.get('trading_disabled', False))
    merged['trading_disabled_reason'] = runner_account.get('trading_disabled_reason', fox_account.get('trading_disabled_reason', ''))

    # Positions from runner (takes precedence)
    merged['positions'] = runner_account.get('positions', fox_account.get('positions', {}))

    return merged


def collect_all_accounts() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """
    Collect all accounts from all JSON snapshot files.

    Returns:
        accounts: Dict mapping sid -> merged account data
        sources: Dict mapping sid -> source description
    """
    all_accounts: Dict[str, Dict[str, Any]] = {}
    sources: Dict[str, str] = {}

    for asset in ASSETS:
        asset_dir = BROKER_JOURNAL_BASE / asset

        # Load FOX promoter snapshot
        fox_path = asset_dir / 'snapshot.json'
        fox_data = load_json(fox_path)
        fox_accounts = extract_accounts_from_snapshot(fox_data)

        # Load RABBIT runner snapshot
        runner_path = asset_dir / f'runner_snapshot_{asset}.json'
        runner_data = load_json(runner_path)
        runner_accounts = extract_accounts_from_snapshot(runner_data)

        # Merge accounts from both sources
        all_sids = set(fox_accounts.keys()) | set(runner_accounts.keys())

        for sid in all_sids:
            fox_acc = fox_accounts.get(sid, {})
            runner_acc = runner_accounts.get(sid, {})

            merged = merge_account_data(fox_acc, runner_acc)
            merged['asset'] = asset  # Track which asset this belongs to

            # Determine source for logging
            if sid in fox_accounts and sid in runner_accounts:
                source_desc = f'{asset}:fox+runner'
            elif sid in fox_accounts:
                source_desc = f'{asset}:fox'
            else:
                source_desc = f'{asset}:runner'

            # Don't overwrite if already seen (shouldn't happen, but defensive)
            if sid not in all_accounts:
                all_accounts[sid] = merged
                sources[sid] = source_desc
            else:
                print(f'  [WARN] Duplicate sid {sid} found in {source_desc}, keeping first')

    return all_accounts, sources


def migrate(dry_run: bool = False, no_backup: bool = False) -> Tuple[int, int, int]:
    """
    Perform the migration.

    Returns: (accounts_migrated, positions_migrated, errors)
    """
    print('=' * 70)
    print('MIGRATE JSON SNAPSHOTS TO SQLITE')
    print('=' * 70)
    print(f'Target database: {DB_PATH}')
    if dry_run:
        print('[DRY-RUN MODE - No changes will be made]')
    print()

    # Collect all accounts
    print('[1] Collecting accounts from JSON snapshots...')
    all_accounts, sources = collect_all_accounts()
    print(f'    Found {len(all_accounts)} unique accounts')

    # Count positions
    total_positions = sum(
        len(acc.get('positions', {}))
        for acc in all_accounts.values()
    )
    print(f'    Found {total_positions} positions total')

    # Show accounts by asset
    by_asset: Dict[str, int] = {}
    for sid, source in sources.items():
        asset = source.split(':')[0]
        by_asset[asset] = by_asset.get(asset, 0) + 1
    for asset, count in sorted(by_asset.items()):
        print(f'      {asset}: {count} accounts')

    if dry_run:
        print('\n[DRY-RUN] Would migrate:')
        for sid in sorted(all_accounts.keys())[:20]:
            acc = all_accounts[sid]
            print(f'    {sid}: cash=${acc.get("cash", 0):.2f}, positions={len(acc.get("positions", {}))}')
        if len(all_accounts) > 20:
            print(f'    ... and {len(all_accounts) - 20} more')
        return (0, 0, 0)

    # Create database
    print('\n[2] Creating SQLite database...')
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    accounts_migrated = 0
    positions_migrated = 0
    errors = 0

    with BrokerDB(DB_PATH) as db:
        print(f'    Database initialized at {DB_PATH}')

        # Import each account
        print('\n[3] Importing accounts...')
        for sid, account_data in all_accounts.items():
            try:
                positions = account_data.pop('positions', {})

                success = db.bulk_import_account(sid, account_data, positions)
                if success:
                    accounts_migrated += 1
                    positions_migrated += len(positions)

                    if accounts_migrated % 10 == 0:
                        print(f'    Imported {accounts_migrated}/{len(all_accounts)} accounts...')
            except Exception as e:
                print(f'    [ERROR] Failed to import {sid}: {e}')
                errors += 1

        print(f'    Imported {accounts_migrated} accounts, {positions_migrated} positions')

        # Validate
        print('\n[4] Validating migration...')
        stats = db.get_stats()
        print(f'    Database stats:')
        print(f'      Active accounts: {stats["active_accounts"]}')
        print(f'      Total positions: {stats["total_positions"]}')

        if stats['active_accounts'] != len(all_accounts):
            print(f'    [WARN] Account count mismatch: expected {len(all_accounts)}, got {stats["active_accounts"]}')
        else:
            print(f'    [OK] Account count matches')

        if stats['total_positions'] != total_positions:
            print(f'    [WARN] Position count mismatch: expected {total_positions}, got {stats["total_positions"]}')
        else:
            print(f'    [OK] Position count matches')

    # Backup JSON files
    if not no_backup:
        print('\n[5] Backing up JSON files...')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        for asset in ASSETS:
            asset_dir = BROKER_JOURNAL_BASE / asset

            for filename in ['snapshot.json', f'runner_snapshot_{asset}.json']:
                src = asset_dir / filename
                if src.exists():
                    dst = asset_dir / f'{filename}.bak_{timestamp}'
                    shutil.copy2(src, dst)
                    print(f'    Backed up {src.name} -> {dst.name}')
    else:
        print('\n[5] Skipping backup (--no-backup)')

    # Summary
    print('\n' + '=' * 70)
    print('MIGRATION SUMMARY')
    print('=' * 70)
    print(f'  Accounts migrated: {accounts_migrated}')
    print(f'  Positions migrated: {positions_migrated}')
    print(f'  Errors: {errors}')
    print(f'  Database: {DB_PATH}')

    if errors == 0:
        print('\n[OK] Migration completed successfully!')
    else:
        print(f'\n[WARN] Migration completed with {errors} errors')

    return (accounts_migrated, positions_migrated, errors)


def main():
    parser = argparse.ArgumentParser(
        description='Migrate JSON snapshots to SQLite database'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be migrated without making changes'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip backing up JSON files'
    )
    parser.add_argument(
        '--db',
        type=Path,
        default=DB_PATH,
        help=f'Database path (default: {DB_PATH})'
    )
    args = parser.parse_args()

    # Update module-level DB_PATH if custom path provided
    if args.db != DB_PATH:
        global DB_PATH
        DB_PATH = args.db

    accounts, positions, errors = migrate(
        dry_run=args.dry_run,
        no_backup=args.no_backup
    )

    # Exit code: 0 if no errors, 1 if errors
    sys.exit(0 if errors == 0 else 1)


if __name__ == '__main__':
    main()
