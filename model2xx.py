#!/usr/bin/env python3
"""
Octopus: Database-Driven Execution Engine (Month-End FF & PF Mixed Edition).
"""

import os
import sys
import time
import logging
import psycopg2
import calendar
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found.")
    sys.exit(1)

# --- Configuration ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")

LEVERAGE = 0.5
TIMEFRAME_MINUTES = 30
TRIGGER_OFFSET_SEC = 30 
LOG_LOCK = threading.Lock()

# --- Asset Configuration ---

# Assets configured for Perpetual Futures (PF)
PERP_ASSETS = {
    "AAVE/USDT", "NEAR/USDT", "FIL/USDT", 
    "ALGO/USDT", "EOS/USDT", "SAND/USDT"
}

# Assets configured for Fixed Futures (FF)
FF_ASSETS = {
    "ETH/USDT", "SOL/USDT"
}

# Active Trading List
ASSETS = list(FF_ASSETS) + list(PERP_ASSETS)

# Inactive/Deactivated Assets (Kept in script for reference)
# INACTIVE_ASSETS = [
#     "BTC/USDT", "XRP/USDT", "ADA/USDT", "AVAX/USDT", 
#     "DOT/USDT", "LTC/USDT", "BCH/USDT", "LINK/USDT", 
#     "UNI/USDT", "XLM/USDT", "DOGE/USDT", "SHIB/USDT"
# ]

# Configure standard logging for the file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("octopus_exec.log")]
)
logger = logging.getLogger("Octopus")

def octopus_log(msg, level="info"):
    """Custom print function with 1s delay and thread safety."""
    with LOG_LOCK:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {msg}"
        print(formatted_msg)
        
        if level == "info": logger.info(msg)
        elif level == "error": logger.error(msg)
        elif level == "warning": logger.warning(msg)
        
        time.sleep(1)

# --- Expiry & Mapping Logic ---

def get_last_friday_expiry():
    octopus_log("Calculating target expiry for Kraken FF contracts...")
    now = datetime.now(timezone.utc)
    
    def last_friday_of_month(year, month):
        last_day = calendar.monthrange(year, month)[1]
        dt = datetime(year, month, last_day, 8, 0, tzinfo=timezone.utc)
        while dt.weekday() != 4: # 4 = Friday
            dt -= timedelta(days=1)
        return dt

    target_dt = last_friday_of_month(now.year, now.month)
    if now >= target_dt:
        octopus_log("Current date past this month's expiry. Rolling to next month.")
        if now.month == 12:
            target_dt = last_friday_of_month(now.year + 1, 1)
        else:
            target_dt = last_friday_of_month(now.year, now.month + 1)
            
    expiry_str = target_dt.strftime("%y%m%d")
    octopus_log(f"Target Expiry Identified: {expiry_str}")
    return expiry_str

TARGET_EXPIRY = get_last_friday_expiry()

def map_to_kraken_symbol(binance_asset: str) -> str:
    """Maps Binance tickers to Kraken PF or FF symbols based on configuration."""
    base = binance_asset.split('/')[0].lower()
    if base == "btc": base = "xbt"

    # Check against Perpetual set
    if binance_asset in PERP_ASSETS:
        return f"pf_{base}usd"
    
    # Default to Fixed Maturity
    return f"ff_{base}usd_{TARGET_EXPIRY}"

SYMBOL_MAP = {a: map_to_kraken_symbol(a) for a in ASSETS}

# --- Main Engine ---

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.instrument_specs = {}

    def initialize(self):
        octopus_log("--- Initializing Octopus Engine ---")
        self._fetch_instrument_specs()
        
        try:
            octopus_log("Verifying Kraken API connectivity...")
            acc = self.kf.get_accounts()
            if "error" in acc: 
                octopus_log(f"API Error detected: {acc}", "error")
            else:
                octopus_log("API Connection: SUCCESS")
            
            valid_contracts = [s for s in SYMBOL_MAP.values() if s in self.instrument_specs]
            octopus_log(f"Contract Audit: {len(valid_contracts)}/{len(ASSETS)} symbols valid")
            
            if len(valid_contracts) < len(ASSETS):
                missing = set(SYMBOL_MAP.values()) - set(self.instrument_specs.keys())
                octopus_log(f"Unavailable Contracts: {missing}", "warning")
        except Exception as e:
            octopus_log(f"Initialization Critical Failure: {e}", "error")

    def _fetch_instrument_specs(self):
        octopus_log("Fetching instrument specifications from Kraken...")
        try:
            import requests
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].lower()
                    precision = inst.get("contractValueTradePrecision", 3)
                    self.instrument_specs[sym] = {
                        "sizeStep": 10 ** (-int(precision)),
                        "tickSize": float(inst.get("tickSize", 0.1))
                    }
                octopus_log(f"Successfully cached specs for {len(self.instrument_specs)} instruments.")
        except Exception as e:
            octopus_log(f"Failed to fetch specs: {e}", "error")

    def _round_to_step(self, value, step):
        return round(round(value / step) * step, 8) if step != 0 else value

    def run(self):
        octopus_log(f"Bot Active. Strategy: {TIMEFRAME_MINUTES}m Rebalance | Trigger Offset: {TRIGGER_OFFSET_SEC}s")
        while True:
            now = datetime.now(timezone.utc)
            if now.minute % TIMEFRAME_MINUTES == 0 and now.second == TRIGGER_OFFSET_SEC:
                octopus_log(f">>> REBALANCE TRIGGERED AT {now.strftime('%H:%M:%S')} <<<")
                self._process_signals()
                octopus_log("Cycle complete. Returning to sleep.")
                time.sleep(2) 
            time.sleep(0.5)

    def _process_signals(self):
        try:
            octopus_log("Connecting to PostgreSQL to fetch latest signals...")
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("SELECT asset, prediction FROM signal;")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            octopus_log(f"Signals retrieved: {len(rows)} assets found in DB.")
            
            signal_map = {"LONG": 1, "SHORT": -1, "NEUTRAL": 0}
            asset_votes = {asset: signal_map.get(pred, 0) for asset, pred in rows if asset in SYMBOL_MAP}
            
            octopus_log("Retrieving Kraken Flex Equity...")
            acc = self.kf.get_accounts()
            equity = float(acc.get("accounts", {}).get("flex", {}).get("marginEquity", 0))
            
            if equity <= 0:
                octopus_log("Insufficient Flex Equity. Blocking execution.", "error")
                return

            # Dynamically calculate size based on Active ASSETS list length
            unit_size_usd = (equity * LEVERAGE) / len(ASSETS)
            octopus_log(f"Portfolio Stats | Equity: ${equity:,.2f} | Leverage: {LEVERAGE}x | Allocation/Asset: ${unit_size_usd:,.2f}")

            octopus_log("Dispatching execution threads for assets...")
            for asset, vote in asset_votes.items():
                target_usd = vote * unit_size_usd
                self.executor.submit(self._execute_logic, asset, target_usd)

        except Exception as e:
            octopus_log(f"Process Loop Error: {e}", "error")

    def _execute_logic(self, binance_asset: str, target_usd: float):
        kf_symbol = SYMBOL_MAP[binance_asset]
        if kf_symbol not in self.instrument_specs: 
            octopus_log(f"Skipping {kf_symbol}: No specs found.", "warning")
            return

        try:
            pos_resp = self.kf.get_open_positions()
            current_qty = 0.0
            # Parse open positions
            for p in pos_resp.get("openPositions", []):
                if p["symbol"].lower() == kf_symbol:
                    current_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                    break
            
            tick_resp = self.kf.get_tickers()
            mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == kf_symbol), 0)
            
            if mark_price == 0: 
                octopus_log(f"[{kf_symbol}] Could not fetch mark price.", "error")
                return

            target_qty = target_usd / mark_price
            delta = target_qty - current_qty
            
            specs = self.instrument_specs[kf_symbol]
            if abs(delta) < specs["sizeStep"]:
                octopus_log(f"[{kf_symbol}] Delta {delta:.4f} below min step. No trade needed.")
                return

            octopus_log(f"[{kf_symbol}] Target: {target_qty:.4f} | Current: {current_qty:.4f} | Delta: {delta:.4f}")
            self._run_maker_loop(kf_symbol, delta, mark_price)

        except Exception as e:
            octopus_log(f"Execution Error {kf_symbol}: {e}", "error")

    def _run_maker_loop(self, symbol, quantity, mark_price):
        """Aggressive Maker Loop with 1s logging per adjustment."""
        side = "buy" if quantity > 0 else "sell"
        abs_qty = abs(quantity)
        specs = self.instrument_specs[symbol]
        
        order_id = None
        duration = 60
        interval = 5
        steps = duration // interval 

        octopus_log(f"[{symbol}] Starting 60s Maker Loop. Side: {side.upper()}")

        for i in range(steps + 1):
            aggression_bps = i * 1.0 
            adjustment = 1 + (aggression_bps * 0.0001) if side == "buy" else 1 - (aggression_bps * 0.0001)
            
            limit_price = self._round_to_step(mark_price * adjustment, specs["tickSize"])
            size = self._round_to_step(abs_qty, specs["sizeStep"])

            try:
                if not order_id:
                    res = self.kf.send_order({"orderType": "lmt", "symbol": symbol, "side": side, "size": size, "limitPrice": limit_price})
                    if "error" in res:
                        raise Exception(f"API Error: {res}")
                    order_id = res.get("sendStatus", {}).get("order_id")
                    octopus_log(f"[{symbol}] Initial Order Placed: {order_id} at {limit_price}")
                else:
                    self.kf.edit_order({"orderId": order_id, "limitPrice": limit_price, "size": size, "symbol": symbol})
                    octopus_log(f"[{symbol}] Adjustment {i}: Price moved to {limit_price} (+{aggression_bps}bps)")
                
                time.sleep(interval)
            except Exception as e:
                octopus_log(f"[{symbol}] Loop Step {i} failed: {e}", "warning")
                # If order placement failed, order_id remains None, enabling retry in next loop

        if order_id:
            octopus_log(f"[{symbol}] Loop finished. Cancelling remaining order {order_id}.")
            try: self.kf.cancel_order({"order_id": order_id, "symbol": symbol})
            except: pass

if __name__ == "__main__":
    bot = Octopus()
    bot.initialize()
    bot.run()
