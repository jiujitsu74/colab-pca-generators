#!/usr/bin/env python3
"""
SimBroker with SQLite State Persistence
=======================================
Drop-in replacement for sim_broker.py that uses SQLite instead of JSON snapshots.

Key changes from original:
1. _load_state() reads from SQLite via broker_db
2. _persist_state() writes to SQLite via broker_db
3. Writes are ADDITIVE - never delete accounts not in allowed_sids
4. JSON snapshot still written for backward compatibility (read by OWL/RABBIT snapshot)

Migration path:
1. Run migrate_snapshots_to_sqlite.py first
2. Replace sim_broker.py with this file
3. FOX promoter uses broker_db.create_account() instead of JSON
"""

from dataclasses import dataclass, field
from typing import Dict, Tuple, List, Optional
import json
import time
import re
import os
import tempfile
from pathlib import Path
from datetime import datetime
import zoneinfo

# Rate limiter for churn protection (added 2026-01-15)
from trade_rate_limiter import get_limiter, TradeRateLimiter

# NEW: Import SQLite database module
try:
    from broker_db import BrokerDB, DEFAULT_DB_PATH
except ImportError:
    # Fallback: try from bin directory
    import sys
    sys.path.insert(0, '/opt/shadow-trader/bin')
    from broker_db import BrokerDB, DEFAULT_DB_PATH


# Debug logging (env-gated, scoped to specific SIDs)
BROKER_DEBUG = os.getenv("BROKER_DEBUG", "0") == "1"
BROKER_DEBUG_SIDS = {s.strip() for s in os.getenv("BROKER_DEBUG_SIDS", "").split(",") if s.strip()}

def bdbg(sid, msg, **kw):
    if not BROKER_DEBUG:
        return
    if BROKER_DEBUG_SIDS and sid not in BROKER_DEBUG_SIDS:
        return
    print(f"[BRKDBG] {time.strftime('%H:%M:%S')} {sid} {msg} {json.dumps(kw, default=str)}", flush=True)

def breject(sid, reason, **kw):
    """Always log order rejections regardless of debug settings"""
    print(f"[BROKER][REJECT] {time.strftime('%H:%M:%S')} {sid} {reason} {json.dumps(kw, default=str)}", flush=True)

def _is_reduce_intent(acc, norm_intent):
    sym = norm_intent.get("symbol")
    delta = norm_intent.get("delta", 0)
    current_qty = norm_intent.get("current_qty", 0)
    if delta == 0:
        return False
    if current_qty > 0 and delta < 0:
        return True
    if current_qty < 0 and delta > 0:
        return True
    return False


MAX_LEVERAGE = 2.0
MAINT_MARGIN = 0.25
FEES_CAP_USD = float(os.getenv("FEES_CAP_USD", "100"))
FEES_CAP_PCT = float(os.getenv("FEES_CAP_PCT", "1.0"))
DDAY_KILL_PCT = 0.06
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "100"))

def compute_fee_cap(equity: float) -> float:
    cap_pct = abs(equity) * FEES_CAP_PCT / 100.0
    return max(FEES_CAP_USD, cap_pct)

MIN_SHORT_PRICE = 2.0
MIN_SHORT_ADV = 1e6

# Keep JSON path for backward compatibility
def get_state_path(asset_class: str = "equities") -> str:
    return f"/opt/shadow-trader/paper-live/broker_journal/{asset_class}/snapshot.json"

STATE_PATH = get_state_path("equities")
STATE_VERSION = 2

NY = zoneinfo.ZoneInfo("America/New_York")

CRYPTO_BASIS = ("BTC","ETH","SOL","ADA","XRP","LTC","DOGE","DOT","LINK","BNB","AVAX","MATIC","ATOM","NEAR","TON","OP","ARB","FIL","MKR","UNI")

def normalize_symbol(sym: str) -> str:
    if not sym:
        return ""
    s = str(sym).upper().strip()
    s_dash = s.replace("_", "-").replace("/", "-")
    parts = s_dash.split("-")
    if len(parts) == 2 and (parts[0] in CRYPTO_BASIS or parts[1] in ("USD","USDT","USDC","BTC","ETH")):
        return f"{parts[0]}{parts[1]}"
    for base in CRYPTO_BASIS:
        for quote in ("USD", "USDT", "USDC", "BTC", "ETH"):
            if s == f"{base}{quote}":
                return f"{base}{quote}"
    return re.sub(r"[^A-Z0-9]", "", s)

def _canonical_symbol(sym: str) -> str:
    if not isinstance(sym, str):
        return sym
    return sym.replace("-", "").replace("_", "").replace("/", "")

def _atomic_write_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".snap.", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def sod_sanity_check(equity: float, sod: float, tolerance: float = 0.10) -> float:
    if not sod:
        return equity
    deviation = abs((equity - sod) / sod) if sod != 0 else 0
    if deviation > tolerance:
        print(f"[WARN] SOD anomaly detected: equity=${equity:.2f}, sod=${sod:.2f}, deviation={deviation:.1%}")
        return equity
    return sod


@dataclass
class Position:
    qty: int = 0
    avg_cost: float = 0.0

@dataclass
class Account:
    cash: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    pnl_realized: float = 0.0
    sod_equity: float = 0.0
    sod_date: str = ""
    fees_today: float = 0.0
    trades_today: int = 0
    trading_disabled: bool = False
    trading_disabled_reason: str = ""


class SimBroker:
    """
    SimBroker with SQLite-backed state persistence.

    Key change: _persist_state() writes to SQLite, only updating accounts
    that are in self.accounts (loaded based on allowed_sids).
    This prevents race conditions where FOX-promoted accounts get deleted.
    """

    def __init__(self, exec_model, mark_method: str = "mid", asset_class: str = "equities", allowed_sids: set = None):
        self.exec = exec_model
        self.mark_method = mark_method
        self.asset_class = asset_class
        self.accounts: Dict[str, Account] = {}
        self.alpha_snapshot: Dict[Tuple[str, str], float] = {}
        self.last_quotes: Dict[str, dict] = {}
        self.allowed_sids = allowed_sids

        self.pending_flatten_intents: Dict[str, List[dict]] = {}

        # Paths
        self.state_dir = Path(f"/opt/shadow-trader/paper-live/broker_journal/{asset_class}")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = str(self.state_dir / "snapshot.json")  # Keep for JSON compat
        self.journal_path = self.state_dir / "fills.jsonl"

        # NEW: SQLite database path
        self.db_path = DEFAULT_DB_PATH

        # Load state from SQLite
        self._load_state_sqlite()

        # Rehydrate trades_today from fills.jsonl
        self._rehydrate_trades_today()

    # =========================================================================
    # NEW: SQLite-based state persistence
    # =========================================================================

    def _load_state_sqlite(self):
        """
        Load persistent state from SQLite database.
        Only loads accounts in allowed_sids (if specified).
        """
        try:
            with BrokerDB(self.db_path) as db:
                # Get accounts filtered by allowed_sids
                sids_to_load = list(self.allowed_sids) if self.allowed_sids else None
                accounts_data = db.get_accounts(sids_to_load)

                loaded_count = 0
                for sid, acc_data in accounts_data.items():
                    # Apply manifest filter
                    if self.allowed_sids is not None and sid not in self.allowed_sids:
                        continue

                    if sid not in self.accounts:
                        self.accounts[sid] = Account()
                    acc = self.accounts[sid]

                    # Load balance data
                    acc.cash = float(acc_data.get('cash', 10000.0))
                    acc.pnl_realized = float(acc_data.get('pnl_realized', 0.0))
                    acc.sod_equity = float(acc_data.get('sod_equity', 0.0))
                    acc.sod_date = acc_data.get('sod_date', '')
                    acc.fees_today = float(acc_data.get('fees_today', 0.0))
                    acc.trading_disabled = bool(acc_data.get('trading_disabled', False))
                    acc.trading_disabled_reason = acc_data.get('trading_disabled_reason', '')

                    # Load positions
                    acc.positions.clear()
                    positions = db.get_positions(sid)
                    for sym, pos_data in positions.items():
                        qty = float(pos_data.get('qty', 0))
                        avg = float(pos_data.get('avg_price', 0.0))
                        if qty != 0:
                            acc.positions[sym] = Position(qty=qty, avg_cost=avg)

                    loaded_count += 1

                if self.allowed_sids is not None:
                    skipped = len(accounts_data) - loaded_count if accounts_data else 0
                    print(f"[Broker] Loaded {loaded_count} accounts from SQLite (skipped {skipped} not in manifest)")
                else:
                    print(f"[Broker] Loaded {loaded_count} accounts from SQLite (no manifest filter)")

        except Exception as e:
            print(f"[WARN] Failed to load state from SQLite: {e}")
            # Fallback to JSON if SQLite fails
            print("[Broker] Falling back to JSON state...")
            self._load_state_json_fallback()

    def _load_state_json_fallback(self):
        """Legacy JSON loading as fallback."""
        try:
            with open(self.state_path, "r") as f:
                blob = json.load(f)

            accounts_data = blob.get("accounts", {})
            loaded_count = 0

            for sid, ent in accounts_data.items():
                if self.allowed_sids is not None and sid not in self.allowed_sids:
                    continue

                if sid not in self.accounts:
                    self.accounts[sid] = Account()
                acc = self.accounts[sid]
                acc.cash = float(ent.get("cash", 10000.0))
                acc.pnl_realized = float(ent.get("pnl_realized", 0.0))
                acc.sod_equity = float(ent.get("sod_equity", 0.0))
                acc.sod_date = ent.get("sod_date", "")
                acc.fees_today = float(ent.get("fees_today", 0.0))
                acc.trading_disabled = bool(ent.get("trading_disabled", False))
                acc.trading_disabled_reason = ent.get("trading_disabled_reason", "")
                acc.positions.clear()
                for sym, p in ent.get("positions", {}).items():
                    qty = float(p.get("qty", 0))
                    avg = float(p.get("avg_cost", 0.0))
                    if qty != 0:
                        acc.positions[sym] = Position(qty=qty, avg_cost=avg)
                loaded_count += 1

            print(f"[Broker] Loaded {loaded_count} accounts from JSON fallback")
        except FileNotFoundError:
            print("[Broker] No state file found, starting fresh")
        except Exception as e:
            print(f"[WARN] Failed to load JSON state: {e}, starting fresh")

    def _persist_state(self):
        """
        Save current state to SQLite database.

        KEY INVARIANT: Only updates accounts in self.accounts.
        Does NOT delete accounts that aren't loaded (e.g., FOX-promoted accounts
        that weren't in allowed_sids when we started).
        """
        try:
            with BrokerDB(self.db_path) as db:
                for sid, acc in self.accounts.items():
                    # Ensure account exists (may have been created by ensure_account)
                    if not db.account_exists(sid):
                        db.create_account(sid, initial_cash=acc.cash)

                    # Compute equity for balance update
                    # Note: We don't have quotes here, so use cash + pnl_realized as proxy
                    # Actual equity will be computed in snapshot() which has quotes
                    equity = acc.cash + acc.pnl_realized

                    # Update balance
                    db.update_balance(
                        sid,
                        cash=acc.cash,
                        equity=equity,
                        pnl_realized=acc.pnl_realized,
                        sod_equity=acc.sod_equity,
                        sod_date=acc.sod_date,
                        fees_today=acc.fees_today,
                        trading_disabled=acc.trading_disabled,
                        trading_disabled_reason=acc.trading_disabled_reason
                    )

                    # Update positions
                    # First, get current DB positions to track deletions
                    current_db_positions = set(db.get_positions(sid).keys())
                    current_mem_positions = set(acc.positions.keys())

                    # Update/insert active positions
                    for sym, pos in acc.positions.items():
                        db.update_position(sid, sym, qty=pos.qty, avg_price=pos.avg_cost)

                    # Delete closed positions (in DB but not in memory)
                    for sym in current_db_positions - current_mem_positions:
                        db.delete_position(sid, sym)

            # ALSO write JSON for backward compatibility (OWL/RABBIT snapshot reader)
            self._persist_state_json_compat()

        except Exception as e:
            print(f"[ERROR] Failed to persist state to SQLite: {e}")
            # Fallback to JSON-only
            self._persist_state_json_compat()

    def _persist_state_json_compat(self):
        """Write JSON snapshot for backward compatibility with OWL/RABBIT."""
        try:
            data = self._serialize_state()
            _atomic_write_json(self.state_path, data)
        except Exception as e:
            print(f"[ERROR] Failed to persist JSON state: {e}")

    def _serialize_state(self) -> dict:
        """Convert in-memory accounts to on-disk schema (JSON format)."""
        out = {
            "version": STATE_VERSION,
            "asof": _now_iso(),
            "accounts": {},
        }
        for sid, acc in self.accounts.items():
            out["accounts"][sid] = {
                "cash": acc.cash,
                "pnl_realized": acc.pnl_realized,
                "sod_equity": acc.sod_equity,
                "sod_date": acc.sod_date,
                "fees_today": acc.fees_today,
                "trading_disabled": acc.trading_disabled,
                "trading_disabled_reason": acc.trading_disabled_reason,
                "positions": {sym: {"qty": pos.qty, "avg_cost": pos.avg_cost}
                              for sym, pos in acc.positions.items()},
            }
        return out

    # Keep original _load_state for backward compat but rename
    def _load_state(self, path: str):
        """Deprecated: Use _load_state_sqlite() instead."""
        self._load_state_sqlite()

    # =========================================================================
    # Rest of SimBroker methods (unchanged from original)
    # =========================================================================

    def _rehydrate_trades_today(self):
        """Rehydrate trades_today counter from fills.jsonl on startup."""
        try:
            TZ = zoneinfo.ZoneInfo("America/New_York")
            today_str = datetime.now(TZ).date().isoformat()

            if not self.journal_path.exists():
                print("[Broker] No fills.jsonl found, trades_today stays 0")
                return

            fills_by_strategy = {}

            with open(self.journal_path, 'r') as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        if entry.get("type") != "fill":
                            continue
                        ts = entry.get("ts")
                        if not ts:
                            continue
                        fill_dt = datetime.fromtimestamp(ts, tz=TZ)
                        if fill_dt.date().isoformat() != today_str:
                            continue
                        sid = entry.get("strategy_id")
                        if sid:
                            fills_by_strategy[sid] = fills_by_strategy.get(sid, 0) + 1
                    except (json.JSONDecodeError, ValueError):
                        continue

            rehydrated = 0
            for sid, count in fills_by_strategy.items():
                if sid in self.accounts:
                    self.accounts[sid].trades_today = count
                    rehydrated += 1

            if rehydrated > 0:
                print(f"[Broker] Rehydrated trades_today for {rehydrated} strategies")
            else:
                print("[Broker] No fills today to rehydrate")

        except Exception as e:
            print(f"[WARN] Failed to rehydrate trades_today: {e}")

    def ensure_account(self, sid: str, initial_cash: float = 10_000.0):
        """Ensure account exists in memory. Also creates in SQLite if needed."""
        if sid not in self.accounts:
            self.accounts[sid] = Account(cash=initial_cash, sod_equity=initial_cash)
            # Also create in SQLite
            try:
                with BrokerDB(self.db_path) as db:
                    db.create_account(sid, initial_cash=initial_cash)
            except Exception as e:
                print(f"[WARN] Failed to create account in SQLite: {e}")

    def _maybe_roll_sod(self, sid: str, equity_now: float) -> None:
        acc = self.accounts[sid]
        now = datetime.now(tz=NY)
        today_str = now.date().isoformat()
        if acc.sod_date != today_str and (now.hour > 9 or (now.hour == 9 and now.minute >= 30)):
            acc.sod_equity = equity_now
            acc.sod_date = today_str
            acc.fees_today = 0.0
            acc.trades_today = 0
            acc.trading_disabled = False
            acc.trading_disabled_reason = ""

    def _mark_price(self, q: dict) -> float:
        if not q:
            return 0.0
        px = q.get("close") or q.get("price")
        if px is not None:
            try:
                return float(px)
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    def _account_nav(self, sid: str, quotes: Dict[str, dict]) -> Tuple[float, float, float, float]:
        acc = self.accounts[sid]
        unreal = 0.0
        notional = 0.0
        for sym, pos in acc.positions.items():
            q = quotes.get(sym)
            if not q:
                canon = _canonical_symbol(sym)
                if canon != sym:
                    q = quotes.get(canon, {})
            if not q:
                q = {}
            px = self._mark_price(q)
            if px <= 0.0:
                continue
            unreal += pos.qty * px
            notional += abs(pos.qty) * px
        equity = acc.cash + acc.pnl_realized + unreal
        pnl_day = equity - acc.sod_equity if acc.sod_equity else 0.0
        return equity, notional, unreal, pnl_day

    def _apply_fill(self, acc: Account, symbol: str, fill_qty: int, fill_px: float, fee: float):
        pos = acc.positions.get(symbol, Position(qty=0, avg_cost=0.0))
        acc.trades_today += 1
        q0, c0 = pos.qty, pos.avg_cost
        q1 = q0 + fill_qty

        if (q0 >= 0 and fill_qty >= 0) or (q0 <= 0 and fill_qty <= 0):
            if q1 != 0:
                pos.avg_cost = (abs(q0) * c0 + abs(fill_qty) * fill_px) / abs(q1)
            else:
                pos.avg_cost = 0.0
            pos.qty = q1
        else:
            closed = min(abs(fill_qty), abs(q0))
            if q0 > 0:
                realized = (fill_px - c0) * closed
            else:
                realized = (c0 - fill_px) * closed
            acc.pnl_realized += realized
            pos.qty = q1
            if q1 == 0:
                pos.avg_cost = 0.0
            elif (q0 > 0 and q1 < 0) or (q0 < 0 and q1 > 0):
                pos.avg_cost = fill_px

        acc.cash -= fill_qty * fill_px
        acc.cash -= fee
        acc.fees_today += fee

        if pos.qty == 0:
            acc.positions.pop(symbol, None)
        else:
            acc.positions[symbol] = pos

    def _normalize_intent(self, intent: dict) -> dict:
        sym = normalize_symbol(intent.get("symbol", ""))
        sid = intent.get("sid")
        acc = self.accounts.get(sid, Account())
        current_qty = acc.positions.get(sym, Position()).qty

        if "target_qty" in intent:
            target_qty = float(intent.get("target_qty", 0))
            delta = target_qty - current_qty
        else:
            side_in = intent.get("side", "").lower()
            qty_in = float(intent.get("qty", 0) or 0)
            if side_in == "buy":
                delta = qty_in
            elif side_in == "sell":
                delta = -qty_in
            else:
                return None

        if delta == 0:
            return None

        req_qty = abs(delta)

        if delta > 0:
            if current_qty < 0:
                if abs(delta) <= abs(current_qty):
                    side, position_effect = "buy", "CLOSE"
                else:
                    side, position_effect = "buy", "OPEN"
            else:
                side, position_effect = "buy", "OPEN"
        else:
            if current_qty > 0:
                if abs(delta) <= abs(current_qty):
                    side, position_effect = "sell", "CLOSE"
                else:
                    side, position_effect = "sell", "OPEN"
            else:
                side, position_effect = "sell", "OPEN"

        return {
            "symbol": sym,
            "sid": sid,
            "side": side,
            "position_effect": position_effect,
            "qty": req_qty,
            "delta": delta,
            "current_qty": current_qty,
            "alpha": float(intent.get("alpha", 0.0)),
            "alpha_long": float(intent.get("alpha_long", 0.0)),
            "alpha_short": float(intent.get("alpha_short", 0.0))
        }

    def _log_fill(self, sid: str, symbol: str, side: str, qty: float, price: float, commission: float, notional: float, equity: float, position_effect: str = "OPEN"):
        try:
            evt = {
                "type": "fill",
                "ts": time.time(),
                "strategy_id": sid,
                "symbol": symbol,
                "side": side,
                "position_effect": position_effect,
                "qty": qty,
                "price": price,
                "commission": commission,
                "notional": notional,
                "account_equity": equity
            }
            with open(self.journal_path, "a") as f:
                f.write(json.dumps(evt) + "\n")
        except Exception:
            pass

    def flatten(self, sid: str, reason: str = "regime_guard"):
        if sid not in self.accounts:
            return
        acc = self.accounts[sid]
        if not acc.positions:
            return
        close_intents = []
        for symbol, pos in list(acc.positions.items()):
            if pos.qty != 0:
                side = "sell" if pos.qty > 0 else "buy"
                qty = abs(pos.qty)
                close_intents.append({
                    "sid": sid,
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "order_type": "MARKET",
                    "reason": reason
                })
                print(f"[FLATTEN] {sid}: {side.upper()} {qty} {symbol} ({reason})")
        self.pending_flatten_intents[sid] = close_intents

    def snapshot(self, quotes: Dict[str, dict]) -> dict:
        out = {}
        out["__quotes__"] = quotes
        for sid in self.accounts:
            equity, notional, unreal, pnl_day = self._account_nav(sid, quotes)
            acc = self.accounts[sid]
            longs = sum(1 for pos in acc.positions.values() if pos.qty > 0)
            shorts = sum(1 for pos in acc.positions.values() if pos.qty < 0)
            gross = notional
            net = sum(pos.qty * self._mark_price(quotes.get(sym, {}))
                     for sym, pos in acc.positions.items())

            out[sid] = {
                "cash": round(acc.cash, 2),
                "equity": round(equity, 2),
                "notional": round(notional, 2),
                "gross": round(gross, 2),
                "net": round(net, 2),
                "longs": longs,
                "shorts": shorts,
                "maintenance_ratio": round((equity / max(notional, 1e-9)) if notional else 1.0, 4),
                "positions": {sym: {"qty": pos.qty, "avg_cost": pos.avg_cost} for sym, pos in acc.positions.items()},
                "pnl_realized": round(acc.pnl_realized, 2),
                "pnl_day": round(pnl_day, 2),
                "fees_today": round(acc.fees_today, 2),
                "trades": acc.trades_today,
                "trading_disabled": acc.trading_disabled,
                "trading_disabled_reason": acc.trading_disabled_reason,
            }

        self._persist_state()
        return out

    def on_tick(self, quotes: Dict[str, dict], adv: dict, order_intents: List[dict]):
        """Process order intents - full implementation from original."""
        # Process pending flatten intents
        if hasattr(self, "pending_flatten_intents") and self.pending_flatten_intents:
            for sid, intents in list(self.pending_flatten_intents.items()):
                order_intents[:0] = intents
            self.pending_flatten_intents.clear()

        self.last_quotes = quotes
        self.alpha_snapshot.clear()

        for oi in order_intents:
            if oi.get("reason", "").startswith("regime_mismatch"):
                continue
            sid = oi["sid"]
            self.ensure_account(sid)
            sym = normalize_symbol(oi.get("symbol"))
            if sym:
                self.alpha_snapshot[(sid, sym)] = float(oi.get("alpha", 0.0))

        fills_applied = 0
        allow_unwind = bool(int(os.getenv("BROKER_POLICY_ALLOW_UNWIND_WHEN_KILLED", "1")))

        # Pre-evaluate kill-switch
        accounts_to_check = set(oi.get("sid") for oi in order_intents if oi.get("sid"))
        for check_sid in accounts_to_check:
            self.ensure_account(check_sid)
            check_acc = self.accounts[check_sid]
            if not check_acc.trading_disabled:
                equity_now, _, _, pnl_day = self._account_nav(check_sid, quotes)
                fee_cap = compute_fee_cap(equity_now)
                if check_acc.fees_today >= fee_cap:
                    check_acc.trading_disabled = True
                    check_acc.trading_disabled_reason = "fees_cap"
                if check_acc.sod_equity > 0 and pnl_day <= -DDAY_KILL_PCT * check_acc.sod_equity:
                    check_acc.trading_disabled = True
                    check_acc.trading_disabled_reason = "dday_kill"
                if check_acc.trades_today >= MAX_TRADES_PER_DAY:
                    check_acc.trading_disabled = True
                    check_acc.trading_disabled_reason = "max_trades_per_day"

        for oi in order_intents:
            sid = oi["sid"]
            self.ensure_account(sid)
            acc = self.accounts[sid]

            norm = self._normalize_intent(oi)
            if norm is None:
                continue

            sym = norm["symbol"]
            delta = norm["delta"]
            req_qty = norm["qty"]
            current_qty = norm["current_qty"]
            position_effect = norm["position_effect"]

            if acc.trading_disabled:
                if acc.trading_disabled_reason == "dday_kill" and allow_unwind:
                    if not _is_reduce_intent(acc, norm):
                        continue
                else:
                    continue

            equity_now, _, _, pnl_day = self._account_nav(sid, quotes)
            fee_cap = compute_fee_cap(equity_now)

            if acc.fees_today >= fee_cap:
                acc.trading_disabled = True
                acc.trading_disabled_reason = "fees_cap"
                continue

            if acc.sod_equity > 0 and pnl_day <= -DDAY_KILL_PCT * acc.sod_equity:
                acc.trading_disabled = True
                acc.trading_disabled_reason = "dday_kill"
                continue

            if acc.trades_today >= MAX_TRADES_PER_DAY:
                acc.trading_disabled = True
                acc.trading_disabled_reason = "max_trades_per_day"
                continue

            limiter = get_limiter()
            allowed, limit_reason = limiter.allow(sid, sym, norm["side"])
            if not allowed:
                is_killed, kill_reason = limiter.is_killed(sid)
                if is_killed:
                    acc.trading_disabled = True
                    acc.trading_disabled_reason = f"rate_limiter_kill: {kill_reason}"
                continue

            # Quote lookup
            raw_sym = oi.get("symbol")
            alts = [normalize_symbol(raw_sym), raw_sym]
            sym_norm = normalize_symbol(raw_sym)
            if sym_norm:
                alts.extend([sym_norm.replace("-", ""), sym_norm.replace("-", "/")])
            if raw_sym and "/" in raw_sym:
                alts.append(raw_sym.replace("/", "-"))
            if raw_sym and "-" in raw_sym:
                alts.append(raw_sym.replace("-", ""))

            q = {}
            for a in alts:
                if a and a in quotes:
                    q = quotes[a]
                    break

            px_mark = self._mark_price(q)
            if px_mark <= 0:
                continue

            reducing = (current_qty != 0) and ((delta > 0 and current_qty < 0) or (delta < 0 and current_qty > 0))

            if not reducing:
                curr_equity, curr_notional, _, _ = self._account_nav(sid, quotes)
                projected_notional = curr_notional + abs(delta) * px_mark
                projected_equity = curr_equity

                if projected_equity <= 0 or (projected_notional / max(projected_equity, 1e-9)) > MAX_LEVERAGE:
                    continue

                if delta < 0 and current_qty >= 0:
                    if px_mark < MIN_SHORT_PRICE:
                        continue
                    if adv:
                        symbol_adv = adv.get(sym, 0.0)
                        if symbol_adv > 0 and symbol_adv < MIN_SHORT_ADV:
                            continue

            symbol_adv = adv.get(sym, 0.0) if adv else 0.0
            result = self.exec.try_fill_market(req_qty, q, symbol_adv)
            if result is None:
                continue

            f_qty, f_px, slip_bp, fee = result
            if f_qty <= 0 or f_px <= 0:
                continue

            signed_qty = f_qty if delta > 0 else -f_qty
            self._apply_fill(acc, sym, signed_qty, f_px, fee)
            fills_applied += 1

            equity, _, _, _ = self._account_nav(sid, quotes)
            self._log_fill(sid, sym, norm["side"], f_qty, f_px, fee,
                          abs(signed_qty * f_px), equity, position_effect)

        if fills_applied > 0:
            self._persist_state()

        for sid in self.accounts.keys():
            equity, _, _, _ = self._account_nav(sid, quotes)
            self._maybe_roll_sod(sid, equity)

        # Maintenance check & liquidation
        for sid in list(self.accounts.keys()):
            guard = 0
            liquidations = 0
            while True:
                equity, notional, _, _ = self._account_nav(sid, quotes)
                if notional == 0:
                    break
                maint = equity / max(notional, 1e-9)
                if maint >= MAINT_MARGIN:
                    break
                if guard > 1000:
                    break
                guard += 1

                acc = self.accounts[sid]
                cands = []
                for sym, pos in acc.positions.items():
                    q = quotes.get(sym, {})
                    px = self._mark_price(q)
                    if px <= 0:
                        continue
                    a = self.alpha_snapshot.get((sid, sym), 0.0)
                    score = a if pos.qty > 0 else -a
                    cands.append((score, sym, pos.qty, px))
                if not cands:
                    break

                cands.sort(key=lambda x: x[0])
                _, sym, qty, px = cands[0]
                liq_qty = max(1, abs(qty) // 2)
                side = "sell" if qty > 0 else "buy"
                q = quotes.get(sym, {})
                symbol_adv = adv.get(sym, 0.0) if adv else 0.0

                result = self.exec.try_fill_market(liq_qty, q, symbol_adv)
                if result is None:
                    break

                f_qty, f_px, slip_bp, fee = result
                if f_qty <= 0 or f_px <= 0:
                    break

                signed_qty = -f_qty if qty > 0 else f_qty
                self._apply_fill(acc, sym, signed_qty, f_px, fee)
                liquidations += 1

                equity, _, _, _ = self._account_nav(sid, quotes)
                self._log_fill(sid, sym, side, f_qty, f_px, fee, abs(signed_qty * f_px), equity, "CLOSE")

            if liquidations > 0:
                self._persist_state()
