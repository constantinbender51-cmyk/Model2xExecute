#!/usr/bin/env python3
"""
Octopus: Database-Driven Execution Engine (Fixed Maturity Edition).
Targets Kraken 'FF' (Fixed Maturity Linear) contracts for 1-week ahead.
"""

import os
import sys
import time
import logging
import psycopg2
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError as e:
    print(f"CRITICAL: Import failed: {e}. Ensure 'kraken_futures.py' is in the directory.")
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

LEVERAGE = 10
TIMEFRAME_MINUTES = 30
TRIGGER_OFFSET_SEC = 30 

# Base mapping (Inference Asset -> Kraken Base)
# Note: FF contracts are typically available for majors (BTC, ETH). 
# If a symbol is not found as FF, the bot will log an error for that specific asset.
ASSETS = [
    "BTC", "ETH", "SOL", "XRP", "ADA", "AVAX", "DOT", "LTC", "BCH", "LINK",
    "UNI", "AAVE", "NEAR", "FIL", "ALGO", "XLM", "EOS", "DOGE", "SHIB", "SAND"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("octopus_exec.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Octopus")

# --- Helper Logic ---

def get_target_expiry():
    """
    Calculates the '1 week ahead' Friday expiry.
    Kraken Fixed Maturity contracts usually expire on Fridays at 08:00 UTC.
    """
    now = datetime.now(timezone.utc)
    # Find next Friday (weekday 4)
    days_until_friday = (4 - now.weekday()) % 7
    if days_until_friday == 0 and now.hour >= 8: # If today is Friday after expiry
        days_until_friday = 7
    
    # Target "1 week ahead" means the Friday AFTER the immediate next one
    target_date = now + timedelta(days=days_until_friday + 7)
    return target_date.strftime("%y%m%d")

TARGET_EXPIRY = get_target_expiry()
# Construct SYMBOL_MAP: e.g., "BTC/USDT" -> "ff_xbtusd_260130"
SYMBOL_MAP = {f"{a}/USDT": f"ff_{a.lower().replace('btc', 'xbt')}usd_{TARGET_EXPIRY}" for a in ASSETS}

# --- Database Signal Fetcher ---

class DatabaseFetcher:
    def __init__(self, db_url):
        self.db_url = db_url

    def fetch_signals(self) -> Tuple[Dict[str, int], int]:
        votes = {}
        total_strategies = 0
        if not self.db_url:
            return {}, 0
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT asset, prediction FROM signal;")
            rows = cur.fetchall()
            signal_map = {"LONG": 1, "SHORT": -1, "NEUTRAL": 0}
            for asset, pred in rows:
                if asset in SYMBOL_MAP:
                    votes[asset] = signal_map.get(pred, 0)
                    total_strategies += 1
            cur.close()
            conn.close()
            return votes, total_strategies
        except Exception as e:
            logger.error(f"DB Fetch Error: {e}")
            return {}, 0

# --- Main Octopus Engine ---

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = DatabaseFetcher(DATABASE_URL)
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.instrument_specs = {}

    def initialize(self):
        logger.info(f"Initializing Octopus (FF Mode | Expiry: {TARGET_EXPIRY})...")
        self._fetch_instrument_specs()
        
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                logger.error(f"API Error: {acc}")
            else:
                logger.info("Kraken API Connected.")
                # Verify if our FF symbols exist in the exchange specs
                for asset, kf_sym in SYMBOL_MAP.items():
                    if kf_sym.lower() not in self.instrument_specs:
                        logger.warning(f"Contract {kf_sym} not found on exchange. Majors only?")
        except Exception as e:
            logger.error(f"API Connection Failed: {e}")

    def _fetch_instrument_specs(self):
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
        except Exception as e:
            logger.error(f"Error fetching specs: {e}")

    def _round_to_step(self, value, step):
        if step == 0: return value
        return round(round(value / step) * step, 8)

    def run(self):
        logger.info(f"Bot Active. Targeting FF week: {TARGET_EXPIRY}")
        while True:
            now = datetime.now(timezone.utc)
            if now.minute % TIMEFRAME_MINUTES == 0 and now.second == TRIGGER_OFFSET_SEC:
                logger.info(f"--- Trigger Time: {now.strftime('%H:%M:%S')} UTC ---")
                self._process_signals()
                time.sleep(2) 
            time.sleep(0.5)

    def _process_signals(self):
        asset_votes, total_strategies = self.fetcher.fetch_signals()
        if total_strategies == 0:
            logger.warning("No signals found in database.")
            return

        try:
            acc = self.kf.get_accounts()
            flex_data = acc.get("accounts", {}).get("flex", {})
            equity = float(flex_data.get("marginEquity", 0))
            
            if equity <= 0:
                logger.error("Equity is 0. Check Multi-Collateral / Flex wallet.")
                return

            unit_size_usd = (equity * LEVERAGE) / total_strategies
            logger.info(f"Equity: ${equity:.2f} | Target per Asset: ${unit_size_usd:.2f}")

            for asset, vote in asset_votes.items():
                target_usd = vote * unit_size_usd
                self.executor.submit(self._execute_logic, asset, target_usd)

        except Exception as e:
            logger.error(f"Process Loop Error: {e}")

    def _execute_logic(self, binance_asset: str, target_usd: float):
        kf_symbol = SYMBOL_MAP.get(binance_asset)
        if not kf_symbol or kf_symbol.lower() not in self.instrument_specs:
            return

        try:
            # 1. Position Check
            pos_resp = self.kf.get_open_positions()
            current_qty = 0.0
            for p in pos_resp.get("openPositions", []):
                if p["symbol"].lower() == kf_symbol.lower():
                    current_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                    break
            
            # 2. Pricing
            tick_resp = self.kf.get_tickers()
            mark_price = 0.0
            for t in tick_resp.get("tickers", []):
                if t["symbol"].lower() == kf_symbol.lower():
                    mark_price = float(t["markPrice"])
                    break
            
            if mark_price == 0: return

            # 3. Execution
            target_qty = target_usd / mark_price
            delta = target_qty - current_qty
            
            specs = self.instrument_specs[kf_symbol.lower()]
            if abs(delta) < specs["sizeStep"]:
                return

            logger.info(f"[{kf_symbol}] Delta: {delta:.4f} @ ${mark_price}")
            self._run_maker_loop(kf_symbol, delta, mark_price, 60, 5, 0, 1.0)

        except Exception as e:
            logger.error(f"Execution Error {kf_symbol}: {e}")

    def _run_maker_loop(self, symbol, quantity, price, duration, interval, start_bp, step_bp):
        side = "buy" if quantity > 0 else "sell"
        abs_qty = abs(quantity)
        specs = self.instrument_specs[symbol.lower()]
        
        order_id = None
        steps = duration // interval

        for i in range(steps + 1):
            aggression = (start_bp + (i * step_bp)) * 0.0001
            adjustment = 1 + aggression if side == "buy" else 1 - aggression
            limit_price = self._round_to_step(price * adjustment, specs["tickSize"])
            size = self._round_to_step(abs_qty, specs["sizeStep"])

            try:
                if not order_id:
                    res = self.kf.send_order({"orderType": "lmt", "symbol": symbol, "side": side, "size": size, "limitPrice": limit_price})
                    order_id = res.get("sendStatus", {}).get("order_id")
                else:
                    self.kf.edit_order({"orderId": order_id, "limitPrice": limit_price, "size": size, "symbol": symbol})
                time.sleep(interval)
            except Exception as e:
                logger.debug(f"Maker step error: {e}")
        
        if order_id:
            try: self.kf.cancel_order({"order_id": order_id, "symbol": symbol})
            except: pass

if __name__ == "__main__":
    bot = Octopus()
    bot.initialize()
    bot.run()
