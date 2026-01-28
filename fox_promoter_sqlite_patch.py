#!/usr/bin/env python3
"""
FOX Promoter SQLite Patch
=========================
This file contains the functions to patch into fox_promoter.py to use SQLite
instead of JSON for account creation.

Apply by:
1. Add import at top: from broker_db import BrokerDB, DEFAULT_DB_PATH
2. Replace create_broker_account() with version below
3. Replace account creation logic in promote() with version below

Or run: python3 fox_promoter_sqlite_patch.py --apply
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

# Import SQLite database module
try:
    from broker_db import BrokerDB, DEFAULT_DB_PATH
except ImportError:
    sys.path.insert(0, '/opt/shadow-trader/bin')
    from broker_db import BrokerDB, DEFAULT_DB_PATH

# Constants
INITIAL_CASH = 10_000.0


def create_broker_account_sqlite(
    strategy_id: str,
    family: str,
    cash: float = INITIAL_CASH,
    meta: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Create a new broker account in SQLite database.

    This replaces the JSON-based account creation in fox_promoter.py.
    Uses INSERT OR IGNORE for idempotency.

    Args:
        strategy_id: Strategy ID (e.g., "crypto01_pca_trend_paper")
        family: Asset family (e.g., "crypto", "equities")
        cash: Initial cash amount
        meta: Optional metadata dict

    Returns:
        True if account was created, False if already exists
    """
    account_meta = {
        'family': family,
        'tier': 'canary',
        'created_by': 'fox_promoter',
        **(meta or {})
    }

    try:
        with BrokerDB(DEFAULT_DB_PATH) as db:
            created = db.create_account(
                sid=strategy_id,
                initial_cash=cash,
                meta=account_meta
            )
            if created:
                print(f"[BROKER-SQLITE] Created account for {strategy_id} (${cash})")
            else:
                print(f"[BROKER-SQLITE] Account already exists for {strategy_id}")
            return created
    except Exception as e:
        print(f"[ERROR] Failed to create SQLite account for {strategy_id}: {e}")
        # Fallback to JSON-based creation
        return False


# =============================================================================
# PATCH APPLICATION
# =============================================================================

FOX_PROMOTER_PATH = Path('/opt/shadow-trader/bin/fox_promoter.py')

OLD_IMPORT_MARKER = "import json"
NEW_IMPORT = """import json
# SQLite broker database
try:
    from broker_db import BrokerDB, DEFAULT_DB_PATH
except ImportError:
    import sys
    sys.path.insert(0, '/opt/shadow-trader/bin')
    from broker_db import BrokerDB, DEFAULT_DB_PATH
"""

OLD_CREATE_ACCOUNT = '''def create_broker_account(strategy_id: str, family: str, cash: float = INITIAL_CASH) -> Dict:
    """Create a new broker account for strategy."""
    return {
        "initial_capital": cash,
        "account_total": cash,
        "account_equity": cash,
        "cash": cash,
        "positions": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "fox_promoter",
        "tier": "canary",
        "family": family,
    }'''

NEW_CREATE_ACCOUNT = '''def create_broker_account(strategy_id: str, family: str, cash: float = INITIAL_CASH) -> Dict:
    """Create a new broker account for strategy - now uses SQLite."""
    # Create in SQLite (primary)
    account_meta = {
        'family': family,
        'tier': 'canary',
        'created_by': 'fox_promoter',
    }
    try:
        with BrokerDB(DEFAULT_DB_PATH) as db:
            created = db.create_account(
                sid=strategy_id,
                initial_cash=cash,
                meta=account_meta
            )
            if created:
                logger.info(f"[BROKER-SQLITE] Created account for {strategy_id} (${cash})")
            else:
                logger.info(f"[BROKER-SQLITE] Account already exists for {strategy_id}")
    except Exception as e:
        logger.error(f"Failed to create SQLite account: {e}")

    # Return dict for JSON compat (backward compatibility)
    return {
        "initial_capital": cash,
        "account_total": cash,
        "account_equity": cash,
        "cash": cash,
        "positions": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "fox_promoter",
        "tier": "canary",
        "family": family,
    }'''


def apply_patch(dry_run: bool = False) -> bool:
    """Apply SQLite patch to fox_promoter.py."""
    if not FOX_PROMOTER_PATH.exists():
        print(f"ERROR: {FOX_PROMOTER_PATH} not found")
        return False

    content = FOX_PROMOTER_PATH.read_text()

    # Check if already patched
    if 'BrokerDB' in content:
        print("Already patched (BrokerDB import found)")
        return True

    # Apply import patch
    if OLD_IMPORT_MARKER in content:
        content = content.replace(OLD_IMPORT_MARKER, NEW_IMPORT, 1)
        print("Added BrokerDB import")
    else:
        print("WARNING: Could not find import marker")

    # Apply create_broker_account patch
    if 'def create_broker_account' in content:
        # Find and replace the function
        import re
        # Match the function definition and body
        pattern = r'def create_broker_account\(strategy_id: str, family: str, cash: float = INITIAL_CASH\) -> Dict:.*?return \{[^}]+\}'
        if re.search(pattern, content, re.DOTALL):
            content = re.sub(pattern, NEW_CREATE_ACCOUNT.strip(), content, flags=re.DOTALL)
            print("Replaced create_broker_account function")
        else:
            print("WARNING: Could not find create_broker_account function to replace")
    else:
        print("WARNING: create_broker_account function not found")

    if dry_run:
        print("\n[DRY-RUN] Would write patched content to:", FOX_PROMOTER_PATH)
        print("\n--- First 100 lines of patched content ---")
        for i, line in enumerate(content.split('\n')[:100]):
            print(f"{i+1:4d}: {line}")
        return True

    # Backup original
    backup_path = FOX_PROMOTER_PATH.with_suffix('.py.bak_sqlite')
    if not backup_path.exists():
        import shutil
        shutil.copy2(FOX_PROMOTER_PATH, backup_path)
        print(f"Backed up to {backup_path}")

    # Write patched content
    FOX_PROMOTER_PATH.write_text(content)
    print(f"Patched {FOX_PROMOTER_PATH}")

    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description='FOX Promoter SQLite Patch')
    parser.add_argument('--apply', action='store_true', help='Apply patch to fox_promoter.py')
    parser.add_argument('--dry-run', '-n', action='store_true', help='Show what would be changed')
    parser.add_argument('--test', action='store_true', help='Test account creation')
    args = parser.parse_args()

    if args.test:
        print("Testing SQLite account creation...")
        created = create_broker_account_sqlite(
            "test_sqlite_account",
            "crypto",
            cash=10000.0,
            meta={'test': True}
        )
        print(f"Created: {created}")

        # Verify
        with BrokerDB(DEFAULT_DB_PATH) as db:
            acc = db.get_account("test_sqlite_account")
            if acc:
                print(f"Verified: {acc['sid']}, cash=${acc['cash']}")
            else:
                print("ERROR: Account not found after creation")
        return

    if args.apply or args.dry_run:
        success = apply_patch(dry_run=args.dry_run)
        sys.exit(0 if success else 1)

    parser.print_help()


if __name__ == '__main__':
    main()
