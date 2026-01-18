#!/usr/bin/env python3
"""
Octopus: Database-Driven Execution Engine (Month-End FF Edition).
Targets Kraken 'FF' (Fixed Maturity) contracts expiring on the LAST FRIDAY of the month.
Enhanced with granular logging and delayed printing.
"""

import os
import sys
import time
import logging
import psycopg2
import calendar
import requests
from datetime import datetime, timezone, timedelta
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

# Environment Variables
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")

# Engine Constants
LEVERAGE = 10
TIMEFRAME_MINUTES = 30
TRIGGER_OFFSET_SEC = 30 

# --- Custom Delayed Print & Logging ---

def delayed_print(message: str):
    """Prints a message and pauses for 1 second."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    time.sleep(1)

# Configure Standard Logging to File/Console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Octopus")

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
    
    if now >= target_friday:
        logger.info("Current month expiry passed. Rolling to next month.")
        next_month = now.month + 1 if now.month < 12 else 1
        next_year = now.year if now.month < 12 else now.year + 1
        target_friday = get_last_friday(next_year, next_month)
        
    expiry_str = target_friday.strftime("%y%m%d")
    delayed_print(f"Target Expiry calculated: {expiry_str}")
    return expiry_str

# --- Asset Mapping ---

BASE_MAP = {
    "BTC": "xbt",
    "ETH": "eth",
    "SOL": "sol",
    "XRP": "xrp",
    "ADA": "ada"
}

TARGET_EXPIRY = get_target_expiry()

def get_ff_symbol(binance_symbol):
    """Maps 'ETH/USDT' -> 'ff_ethusd_260130'"""
    base = binance_symbol.split('/')[0].upper()
    kraken_base = BASE_MAP.get(base, base.lower())
    symbol = f"ff_{kraken_base}usd_{TARGET_EXPIRY}"
    return symbol

# --- Main Engine ---

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.instrument_specs = {}

    def initialize(self):
        delayed_print("--- Octopus Engine Initializing ---")
        logger.info(f"Targeted Expiry for session: {TARGET_EXPIRY}")
        self._fetch_specs()
        delayed_print("Initialization Complete.")
        
    def _fetch_specs(self):
        delayed_print("Fetching Kraken Futures instrument specifications...")
        try:
            resp = requests.get("https://futures.kraken.com/derivatives/api/v3/instruments").json()
            if resp.get("result") == "success":
                for inst in resp.get("instruments", []):
                    sym = inst["symbol"].lower()
                    self.instrument_specs[sym] = {
                        "sizeStep": 10 ** (-int(inst.get("contractValueTradePrecision", 3))),
                        "tickSize": float(inst.get("tickSize", 0.01))
                    }
                logger.info(f"Successfully cached specs for {len(self.instrument_specs)} instruments.")
            else:
                logger.error("API response unsuccessful while fetching specs.")
        except Exception as e:
            logger.error(f"Failed to fetch specs: {e}")

    def _process_signals(self):
        delayed_print(">>> Cycle Triggered: Fetching signals from Database.")
        
        # 1. Fetch from DB
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("SELECT asset, prediction FROM signal;")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            logger.info(f"Retrieved {len(rows)} signals from database.")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return

        if not rows:
            delayed_print("No signals found in the database. Standing by.")
            return

        # 2. Account Check
        delayed_print("Checking Kraken Futures account balance...")
        try:
            acc = self.kf.get_accounts()
            # Depending on Kraken API version, access logic may vary
            flex_acc = acc.get("accounts", {}).get("flex", {})
            equity = float(flex_acc.get("marginEquity", 0))
            logger.info(f"Current Flex Equity: ${equity:.2f} USD")
        except Exception as e:
            logger.error(f"Failed to fetch account equity: {e}")
            return

        if equity <= 0:
            logger.warning("Equity is zero or negative. Execution halted.")
            return

        unit_size_usd = (equity * LEVERAGE) / len(rows)
        delayed_print(f"Strategy: {LEVERAGE}x Leverage. Allocation per signal: ${unit_size_usd:.2f}")

        # 3. Execution Loop
        for asset, pred in rows:
            kf_symbol = get_ff_symbol(asset)
            delayed_print(f"Processing Signal: {asset} -> {pred} (Target: {kf_symbol})")
            
            if kf_symbol not in self.instrument_specs:
                logger.warning(f"Contract {kf_symbol} is not listed in Kraken specs. Skipping.")
                continue

            vote = {"LONG": 1, "SHORT": -1}.get(pred.upper(), 0)
            target_usd = vote * unit_size_usd
            self._execute_single(kf_symbol, target_usd)

    def _execute_single(self, symbol, target_usd):
        try:
            # Get current Mark Price
            tick_data = self.kf.get_tickers()
            price = next((float(t["markPrice"]) for t in tick_data.get("tickers", []) 
                         if t["symbol"].lower() == symbol), 0)
            
            if price == 0:
                logger.error(f"Could not find mark price for {symbol}")
                return

            # Get current Position
            pos_data = self.kf.get_open_positions()
            curr_qty = 0.0
            for p in pos_data.get("openPositions", []):
                if p["symbol"].lower() == symbol:
                    curr_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
            
            delayed_print(f"[{symbol}] Price: {price} | Current Qty: {curr_qty}")

            # Calculate Delta
            target_qty = target_usd / price
            delta = target_qty - curr_qty
            
            step = self.instrument_specs[symbol]["sizeStep"]
            if abs(delta) >= step:
                side = "buy" if delta > 0 else "sell"
                logger.info(f"EXECUTING: {side} {abs(delta):.4f} {symbol} to rebalance.")
                # Note: Replace this with your actual order placement method
                # self.kf.create_order(symbol, side, "market", abs(delta))
                delayed_print(f"Order sent for {symbol}. Moving to next asset.")
            else:
                delayed_print(f"Delta for {symbol} is below step size ({step}). No trade required.")

        except Exception as e:
            logger.error(f"Execution error for {symbol}: {e}")

    def run(self):
        delayed_print("Octopus is now monitoring the clock.")
        while True:
            now = datetime.now(timezone.utc)
            # Check for trigger: minute matches interval and second matches offset
            if now.minute % TIMEFRAME_MINUTES == 0 and now.second == TRIGGER_OFFSET_SEC:
                logger.info(f"Execution interval reached: {now.strftime('%H:%M:%S')}")
                self._process_signals()
                # Sleep to prevent double-triggering within the same second
                time.sleep(2)
            
            # High-frequency check for the trigger time
            time.sleep(0.5)

if __name__ == "__main__":
    bot = Octopus()
    try:
        bot.initialize()
        bot.run()
    except KeyboardInterrupt:
        delayed_print("Shutdown signal received. Octopus powering down.")
        sys.exit(0)
