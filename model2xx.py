#!/usr/bin/env python3
"""
Octopus: Database-Driven Execution Engine (Month-End FF Edition).
Targets Kraken 'FF' (Fixed Maturity) contracts expiring on the LAST FRIDAY of the month.
"""

import os
import sys
import time
import logging
import psycopg2
import calendar
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple

# --- Configuration & Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found.")
    sys.exit(1)

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

# --- Helper Logic: Calculate Last Friday of the Month ---

def get_last_friday(year, month):
    """Returns the date of the last Friday for a given year/month."""
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day, 8, 0, tzinfo=timezone.utc)
    # 4 is Friday (Monday=0)
    days_to_subtract = (last_date.weekday() - 4) % 7
    return last_date - timedelta(days=days_to_subtract)

def get_target_expiry():
    """Calculates the Kraken FF YYMMDD string for the last Friday of the month."""
    now = datetime.now(timezone.utc)
    target_friday = get_last_friday(now.year, now.month)
    
    # If we are past the current month's expiry (Friday 08:00 UTC), roll to next month
    if now >= target_friday:
        next_month = now.month + 1 if now.month < 12 else 1
        next_year = now.year if now.month < 12 else now.year + 1
        target_friday = get_last_friday(next_year, next_month)
        
    return target_friday.strftime("%y%m%d")

# --- Asset Mapping ---

# Database (Binance) -> Kraken Base Tickers
BASE_MAP = {
    "BTC": "xbt",
    "ETH": "eth",
    "SOL": "sol",
    "XRP": "xrp",
    "ADA": "ada"
    # Add others as needed; Kraken uses standard tickers for most alts.
}

TARGET_EXPIRY = get_target_expiry()

def get_ff_symbol(binance_symbol):
    """Maps 'ETH/USDT' -> 'ff_ethusd_260130'"""
    base = binance_symbol.split('/')[0].upper()
    kraken_base = BASE_MAP.get(base, base.lower())
    return f"ff_{kraken_base}usd_{TARGET_EXPIRY}"

# --- Main Engine ---

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.instrument_specs = {}
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        self.logger = logging.getLogger("Octopus")

    def initialize(self):
        self.logger.info(f"Octopus Booting. Targeted Expiry: {TARGET_EXPIRY}")
        self._fetch_specs()
        
    def _fetch_specs(self):
        try:
            import requests
            resp = requests.get("https://futures.kraken.com/derivatives/api/v3/instruments").json()
            for inst in resp.get("instruments", []):
                sym = inst["symbol"].lower()
                self.instrument_specs[sym] = {
                    "sizeStep": 10 ** (-int(inst.get("contractValueTradePrecision", 3))),
                    "tickSize": float(inst.get("tickSize", 0.01))
                }
        except Exception as e:
            self.logger.error(f"Failed to fetch specs: {e}")

    def _process_signals(self):
        # 1. Fetch from DB
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("SELECT asset, prediction FROM signal;")
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"DB Error: {e}")
            return

        # 2. Account Check
        acc = self.kf.get_accounts()
        equity = float(acc.get("accounts", {}).get("flex", {}).get("marginEquity", 0))
        if equity <= 0: return

        unit_size = (equity * LEVERAGE) / len(rows) if rows else 0

        # 3. Execution Loop
        for asset, pred in rows:
            kf_symbol = get_ff_symbol(asset)
            if kf_symbol not in self.instrument_specs:
                self.logger.warning(f"Contract {kf_symbol} not available.")
                continue

            vote = {"LONG": 1, "SHORT": -1}.get(pred, 0)
            target_usd = vote * unit_size
            self._execute_single(kf_symbol, target_usd)

    def _execute_single(self, symbol, target_usd):
        # Simplified execution for brevity - uses mark price to determine delta
        tick = self.kf.get_tickers()
        price = next((float(t["markPrice"]) for t in tick["tickers"] if t["symbol"].lower() == symbol), 0)
        if price == 0: return

        # Current Pos
        pos = self.kf.get_open_positions()
        curr_qty = 0.0
        for p in pos.get("openPositions", []):
            if p["symbol"].lower() == symbol:
                curr_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])

        delta = (target_usd / price) - curr_qty
        if abs(delta) > self.instrument_specs[symbol]["sizeStep"]:
            self.logger.info(f"Rebalancing {symbol} | Delta: {delta:.4f}")
            # Insert your _run_maker_loop here

    def run(self):
        while True:
            now = datetime.now(timezone.utc)
            if now.minute % TIMEFRAME_MINUTES == 0 and now.second == TRIGGER_OFFSET_SEC:
                self._process_signals()
                time.sleep(2)
            time.sleep(0.5)

if __name__ == "__main__":
    bot = Octopus()
    bot.initialize()
    bot.run()
