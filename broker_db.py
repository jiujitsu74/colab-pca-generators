#!/usr/bin/env python3
"""
Broker Database Module
======================
SQLite-based broker state persistence for ALEXANDER trading system.

Replaces JSON snapshot files with proper database to solve race condition
where SimBroker filtered reads + full writes would lose FOX-promoted accounts.

Key Invariant: create_account() is the ONLY way to add accounts.
Update functions can only modify existing accounts, never delete others.

Usage:
    from broker_db import BrokerDB

    with BrokerDB() as db:
        db.create_account("strategy_01", initial_cash=10000.0)
        db.update_balance("strategy_01", cash=9500.0, equity=9800.0)
        db.update_position("strategy_01", "BTCUSD", qty=0.5, avg_price=50000.0)

        account = db.get_account("strategy_01")
        positions = db.get_positions("strategy_01")

Database: /opt/shadow-trader/data/broker.db
"""

import json
import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Default database path
DEFAULT_DB_PATH = Path('/opt/shadow-trader/data/broker.db')

# Schema version for migrations
SCHEMA_VERSION = 1


def _now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


class BrokerDB:
    """
    SQLite-based broker state database.

    Thread-safe via WAL mode and check_same_thread=False.
    Uses connection pooling via context manager.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    def __enter__(self) -> 'BrokerDB':
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=30.0
        )
        self._conn.row_factory = sqlite3.Row

        # Enable WAL mode for concurrent reads during writes
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

        # Initialize schema
        self._init_schema()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()
            self._conn = None
        return False

    def _init_schema(self):
        """Create tables if they don't exist."""
        cursor = self._conn.cursor()

        # Accounts table - core strategy registration
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                sid TEXT PRIMARY KEY,
                initial_cash REAL NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                meta JSON
            )
        """)

        # Balances table - current cash/equity state
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balances (
                sid TEXT PRIMARY KEY,
                cash REAL NOT NULL,
                equity REAL NOT NULL,
                pnl_realized REAL DEFAULT 0.0,
                sod_equity REAL DEFAULT 0.0,
                sod_date TEXT DEFAULT '',
                fees_today REAL DEFAULT 0.0,
                trading_disabled INTEGER DEFAULT 0,
                trading_disabled_reason TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                FOREIGN KEY (sid) REFERENCES accounts(sid) ON DELETE CASCADE
            )
        """)

        # Positions table - per-symbol holdings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                sid TEXT NOT NULL,
                symbol TEXT NOT NULL,
                qty REAL NOT NULL,
                avg_price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (sid, symbol),
                FOREIGN KEY (sid) REFERENCES accounts(sid) ON DELETE CASCADE
            )
        """)

        # Schema version tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)

        # Insert schema version if not exists
        cursor.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
            (SCHEMA_VERSION,)
        )

        # Create indexes for common queries
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_sid ON positions(sid)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_updated ON balances(updated_at)"
        )

        self._conn.commit()

    # =========================================================================
    # ACCOUNT OPERATIONS
    # =========================================================================

    def create_account(
        self,
        sid: str,
        initial_cash: float,
        meta: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a new broker account. INSERT OR IGNORE - idempotent.

        This is the ONLY way to add new accounts to the system.

        Returns: True if account was created, False if already exists.
        """
        cursor = self._conn.cursor()
        now = _now_iso()
        meta_json = json.dumps(meta) if meta else None

        # INSERT OR IGNORE for idempotency
        cursor.execute("""
            INSERT OR IGNORE INTO accounts (sid, initial_cash, created_at, status, meta)
            VALUES (?, ?, ?, 'active', ?)
        """, (sid, initial_cash, now, meta_json))

        created = cursor.rowcount > 0

        if created:
            # Also create initial balance record
            cursor.execute("""
                INSERT OR IGNORE INTO balances
                (sid, cash, equity, pnl_realized, sod_equity, sod_date, fees_today,
                 trading_disabled, trading_disabled_reason, updated_at)
                VALUES (?, ?, ?, 0.0, 0.0, '', 0.0, 0, '', ?)
            """, (sid, initial_cash, initial_cash, now))

        self._conn.commit()
        return created

    def get_account(self, sid: str) -> Optional[Dict[str, Any]]:
        """Get a single account by strategy ID."""
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT a.sid, a.initial_cash, a.created_at, a.status, a.meta,
                   b.cash, b.equity, b.pnl_realized, b.sod_equity, b.sod_date,
                   b.fees_today, b.trading_disabled, b.trading_disabled_reason,
                   b.updated_at
            FROM accounts a
            LEFT JOIN balances b ON a.sid = b.sid
            WHERE a.sid = ?
        """, (sid,))

        row = cursor.fetchone()
        if not row:
            return None

        return self._row_to_account(row)

    def get_accounts(self, sids: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get multiple accounts, optionally filtered by strategy IDs.

        Args:
            sids: List of strategy IDs to fetch. If None, returns all accounts.

        Returns: Dict mapping sid -> account data
        """
        cursor = self._conn.cursor()

        if sids is None:
            cursor.execute("""
                SELECT a.sid, a.initial_cash, a.created_at, a.status, a.meta,
                       b.cash, b.equity, b.pnl_realized, b.sod_equity, b.sod_date,
                       b.fees_today, b.trading_disabled, b.trading_disabled_reason,
                       b.updated_at
                FROM accounts a
                LEFT JOIN balances b ON a.sid = b.sid
                WHERE a.status = 'active'
            """)
        else:
            placeholders = ','.join('?' * len(sids))
            cursor.execute(f"""
                SELECT a.sid, a.initial_cash, a.created_at, a.status, a.meta,
                       b.cash, b.equity, b.pnl_realized, b.sod_equity, b.sod_date,
                       b.fees_today, b.trading_disabled, b.trading_disabled_reason,
                       b.updated_at
                FROM accounts a
                LEFT JOIN balances b ON a.sid = b.sid
                WHERE a.sid IN ({placeholders}) AND a.status = 'active'
            """, sids)

        result = {}
        for row in cursor.fetchall():
            account = self._row_to_account(row)
            result[account['sid']] = account

        return result

    def _row_to_account(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Convert database row to account dict."""
        meta = None
        if row['meta']:
            try:
                meta = json.loads(row['meta'])
            except json.JSONDecodeError:
                meta = None

        return {
            'sid': row['sid'],
            'initial_cash': row['initial_cash'],
            'created_at': row['created_at'],
            'status': row['status'],
            'meta': meta,
            'cash': row['cash'] if row['cash'] is not None else row['initial_cash'],
            'equity': row['equity'] if row['equity'] is not None else row['initial_cash'],
            'pnl_realized': row['pnl_realized'] or 0.0,
            'sod_equity': row['sod_equity'] or 0.0,
            'sod_date': row['sod_date'] or '',
            'fees_today': row['fees_today'] or 0.0,
            'trading_disabled': bool(row['trading_disabled']),
            'trading_disabled_reason': row['trading_disabled_reason'] or '',
            'updated_at': row['updated_at'],
        }

    def account_exists(self, sid: str) -> bool:
        """Check if an account exists."""
        cursor = self._conn.cursor()
        cursor.execute("SELECT 1 FROM accounts WHERE sid = ?", (sid,))
        return cursor.fetchone() is not None

    def set_account_status(self, sid: str, status: str) -> bool:
        """Update account status (active, disabled, tombstoned)."""
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE accounts SET status = ? WHERE sid = ?",
            (status, sid)
        )
        self._conn.commit()
        return cursor.rowcount > 0

    # =========================================================================
    # BALANCE OPERATIONS
    # =========================================================================

    def update_balance(
        self,
        sid: str,
        cash: float,
        equity: float,
        pnl_realized: Optional[float] = None,
        sod_equity: Optional[float] = None,
        sod_date: Optional[str] = None,
        fees_today: Optional[float] = None,
        trading_disabled: Optional[bool] = None,
        trading_disabled_reason: Optional[str] = None
    ) -> bool:
        """
        Update balance for an existing account.

        Only updates fields that are provided (not None).
        Cannot create accounts - use create_account() first.
        """
        if not self.account_exists(sid):
            return False

        cursor = self._conn.cursor()
        now = _now_iso()

        # Build dynamic UPDATE statement
        updates = ['cash = ?', 'equity = ?', 'updated_at = ?']
        values = [cash, equity, now]

        if pnl_realized is not None:
            updates.append('pnl_realized = ?')
            values.append(pnl_realized)
        if sod_equity is not None:
            updates.append('sod_equity = ?')
            values.append(sod_equity)
        if sod_date is not None:
            updates.append('sod_date = ?')
            values.append(sod_date)
        if fees_today is not None:
            updates.append('fees_today = ?')
            values.append(fees_today)
        if trading_disabled is not None:
            updates.append('trading_disabled = ?')
            values.append(1 if trading_disabled else 0)
        if trading_disabled_reason is not None:
            updates.append('trading_disabled_reason = ?')
            values.append(trading_disabled_reason)

        values.append(sid)

        cursor.execute(f"""
            UPDATE balances SET {', '.join(updates)} WHERE sid = ?
        """, values)

        # If no balance row exists, insert it
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO balances
                (sid, cash, equity, pnl_realized, sod_equity, sod_date, fees_today,
                 trading_disabled, trading_disabled_reason, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sid, cash, equity,
                pnl_realized or 0.0,
                sod_equity or 0.0,
                sod_date or '',
                fees_today or 0.0,
                1 if trading_disabled else 0,
                trading_disabled_reason or '',
                now
            ))

        self._conn.commit()
        return True

    # =========================================================================
    # POSITION OPERATIONS
    # =========================================================================

    def update_position(
        self,
        sid: str,
        symbol: str,
        qty: float,
        avg_price: float
    ) -> bool:
        """
        Update or insert a position for an account.

        If qty is 0, deletes the position (closed position).
        """
        if not self.account_exists(sid):
            return False

        cursor = self._conn.cursor()
        now = _now_iso()

        if qty == 0:
            # Delete closed position
            cursor.execute(
                "DELETE FROM positions WHERE sid = ? AND symbol = ?",
                (sid, symbol)
            )
        else:
            # Upsert position
            cursor.execute("""
                INSERT INTO positions (sid, symbol, qty, avg_price, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sid, symbol) DO UPDATE SET
                    qty = excluded.qty,
                    avg_price = excluded.avg_price,
                    updated_at = excluded.updated_at
            """, (sid, symbol, qty, avg_price, now))

        self._conn.commit()
        return True

    def delete_position(self, sid: str, symbol: str) -> bool:
        """Delete a position (for closed positions)."""
        cursor = self._conn.cursor()
        cursor.execute(
            "DELETE FROM positions WHERE sid = ? AND symbol = ?",
            (sid, symbol)
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def get_positions(self, sid: str) -> Dict[str, Dict[str, float]]:
        """Get all positions for a strategy."""
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT symbol, qty, avg_price, updated_at
            FROM positions
            WHERE sid = ?
        """, (sid,))

        return {
            row['symbol']: {
                'qty': row['qty'],
                'avg_price': row['avg_price'],
                'updated_at': row['updated_at']
            }
            for row in cursor.fetchall()
        }

    def get_all_positions(
        self,
        sids: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Get positions for multiple strategies.

        Returns: Dict mapping sid -> symbol -> position data
        """
        cursor = self._conn.cursor()

        if sids is None:
            cursor.execute("""
                SELECT sid, symbol, qty, avg_price, updated_at
                FROM positions
            """)
        else:
            placeholders = ','.join('?' * len(sids))
            cursor.execute(f"""
                SELECT sid, symbol, qty, avg_price, updated_at
                FROM positions
                WHERE sid IN ({placeholders})
            """, sids)

        result: Dict[str, Dict[str, Dict[str, float]]] = {}
        for row in cursor.fetchall():
            sid = row['sid']
            if sid not in result:
                result[sid] = {}
            result[sid][row['symbol']] = {
                'qty': row['qty'],
                'avg_price': row['avg_price'],
                'updated_at': row['updated_at']
            }

        return result

    # =========================================================================
    # BULK OPERATIONS (for migration)
    # =========================================================================

    def bulk_import_account(
        self,
        sid: str,
        account_data: Dict[str, Any],
        positions: Dict[str, Dict[str, float]]
    ) -> bool:
        """
        Import a complete account with balance and positions.
        Used for migration from JSON snapshots.
        """
        now = _now_iso()
        cursor = self._conn.cursor()

        # Create account
        initial_cash = account_data.get('initial_capital', account_data.get('cash', 10000.0))
        meta = {
            k: v for k, v in account_data.items()
            if k not in ('cash', 'equity', 'positions', 'pnl_realized', 'sod_equity',
                        'sod_date', 'fees_today', 'trading_disabled', 'trading_disabled_reason',
                        'initial_capital', 'account_total', 'account_equity')
        }

        # Use INSERT OR REPLACE to ensure account exists
        cursor.execute("""
            INSERT OR REPLACE INTO accounts (sid, initial_cash, created_at, status, meta)
            VALUES (?, ?, ?, 'active', ?)
        """, (sid, initial_cash, account_data.get('created_at', now), json.dumps(meta) if meta else None))

        # Create/update balance - use INSERT OR REPLACE to handle existing
        cursor.execute("""
            INSERT OR REPLACE INTO balances
            (sid, cash, equity, pnl_realized, sod_equity, sod_date, fees_today,
             trading_disabled, trading_disabled_reason, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sid,
            account_data.get('cash', initial_cash),
            account_data.get('equity', account_data.get('account_equity', initial_cash)),
            account_data.get('pnl_realized', 0.0),
            account_data.get('sod_equity', 0.0),
            account_data.get('sod_date', ''),
            account_data.get('fees_today', 0.0),
            1 if account_data.get('trading_disabled') else 0,
            account_data.get('trading_disabled_reason', ''),
            now
        ))

        # Import positions
        for symbol, pos_data in positions.items():
            qty = pos_data.get('qty', 0)
            if qty != 0:
                cursor.execute("""
                    INSERT OR REPLACE INTO positions (sid, symbol, qty, avg_price, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (sid, symbol, qty, pos_data.get('avg_price', pos_data.get('avg_cost', 0.0)), now))

        self._conn.commit()
        return True

    # =========================================================================
    # STATS
    # =========================================================================

    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        cursor = self._conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
        active_accounts = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM accounts")
        total_accounts = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM positions")
        total_positions = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT sid) FROM positions")
        accounts_with_positions = cursor.fetchone()[0]

        return {
            'active_accounts': active_accounts,
            'total_accounts': total_accounts,
            'total_positions': total_positions,
            'accounts_with_positions': accounts_with_positions,
        }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def init_db(db_path: Optional[Path] = None) -> BrokerDB:
    """Initialize database and return context manager."""
    db = BrokerDB(db_path)
    return db


@contextmanager
def get_db(db_path: Optional[Path] = None):
    """Context manager for database access."""
    db = BrokerDB(db_path)
    with db as conn:
        yield conn


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Broker Database CLI')
    parser.add_argument('--db', type=Path, default=DEFAULT_DB_PATH, help='Database path')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--list', action='store_true', help='List all accounts')
    parser.add_argument('--get', type=str, help='Get account by SID')
    parser.add_argument('--test', action='store_true', help='Run basic test')
    args = parser.parse_args()

    with BrokerDB(args.db) as db:
        if args.stats:
            stats = db.get_stats()
            print(f"Database: {args.db}")
            for k, v in stats.items():
                print(f"  {k}: {v}")

        elif args.list:
            accounts = db.get_accounts()
            print(f"Found {len(accounts)} accounts:")
            for sid, acc in sorted(accounts.items()):
                print(f"  {sid}: cash=${acc['cash']:.2f}, equity=${acc['equity']:.2f}")

        elif args.get:
            acc = db.get_account(args.get)
            if acc:
                print(json.dumps(acc, indent=2, default=str))
                positions = db.get_positions(args.get)
                if positions:
                    print(f"\nPositions:")
                    for sym, pos in positions.items():
                        print(f"  {sym}: qty={pos['qty']}, avg_price={pos['avg_price']}")
            else:
                print(f"Account not found: {args.get}")

        elif args.test:
            print("Running basic test...")

            # Create test account
            test_sid = "test_account_001"
            created = db.create_account(test_sid, initial_cash=10000.0, meta={'test': True})
            print(f"  Created account: {created}")

            # Update balance
            db.update_balance(test_sid, cash=9500.0, equity=9800.0)
            print("  Updated balance")

            # Add position
            db.update_position(test_sid, "BTCUSD", qty=0.5, avg_price=50000.0)
            print("  Added position")

            # Read back
            acc = db.get_account(test_sid)
            print(f"  Account: cash=${acc['cash']}, equity=${acc['equity']}")

            positions = db.get_positions(test_sid)
            print(f"  Positions: {positions}")

            # Cleanup
            db.set_account_status(test_sid, 'tombstoned')
            print("  Marked as tombstoned")

            print("\nTest completed successfully!")

        else:
            parser.print_help()
