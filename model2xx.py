#!/usr/bin/env python3
"""
Octopus: Database Signal Aggregator & Execution Engine.
Fetches signals from PostgreSQL database populated by inference script.
- UPDATED: Database integration, signal parsing from DB.
- RETAINED: Kraken Futures Execution, Maker Loop, Risk Management.
"""

import os
import sys
import time
import logging
import psycopg2
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

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

# API Keys
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Database
DATABASE_URL = os.getenv("DATABASE_URL")

# Global Settings
LEVERAGE = 70

# Asset Mapping (Inference Symbol -> Kraken Futures Perpetual)
SYMBOL_MAP = {
    # --- Majors ---
    "BTC/USDT": "ff_xbtusd_260327",
    "ETH/USDT": "pf_ethusd",
    "SOL/USDT": "pf_solusd",
    "XRP/USDT": "pf_xrpusd",
    "ADA/USDT": "pf_adausd",
    
    # --- Alts ---
    "DOGE/USDT": "pf_dogeusd",
    "AVAX/USDT": "pf_avaxusd",
    "DOT/USDT": "pf_dotusd",
    "LINK/USDT": "pf_linkusd",
    "LTC/USDT": "pf_ltcusd",
    "BCH/USDT": "pf_bchusd",
    "XLM/USDT": "pf_xlmusd",
    "SHIB/USDT": "pf_shibusd",
    "UNI/USDT": "pf_uniusd",
    
    # --- Additional (add more as needed) ---
    "NEAR/USDT": "pf_nearusd",
    "FIL/USDT": "pf_filusd",
    "ALGO/USDT": "pf_algousd",
    "EOS/USDT": "pf_eosusd",
    "SAND/USDT": "pf_sandusd",
    "AAVE/USDT": "pf_aaveusd",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Octopus")

# --- Database Signal Fetcher ---

class DatabaseSignalFetcher:
    def __init__(self):
        if not DATABASE_URL:
            logger.error("DATABASE_URL environment variable not set!")
            sys.exit(1)
        self.db_url = DATABASE_URL

    def get_connection(self):
        """Establishes database connection"""
        try:
            return psycopg2.connect(self.db_url)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return None

    def fetch_signals(self) -> Tuple[Dict[str, int], int]:
        """
        Fetches signals from the database.
        Returns: ({ "BTC/USDT": 1, "ETH/USDT": -1, ... }, total_strategies)
        
        Signal mapping:
        - "LONG" -> +1
        - "SHORT" -> -1
        - "NEUTRAL" -> 0
        """
        conn = self.get_connection()
        if not conn:
            logger.error("Cannot fetch signals - no database connection")
            return {}, 0

        try:
            cur = conn.cursor()
            
            # Fetch all current signals
            query = """
                SELECT asset, prediction, start_time, end_time
                FROM signal
                ORDER BY asset;
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            asset_votes = {}
            total_strategies = 0
            now = datetime.now(timezone.utc)
            
            for asset, prediction, start_time, end_time in rows:
                # Skip if signal is expired
                if end_time and end_time < now:
                    logger.debug(f"Skipping expired signal for {asset}")
                    continue
                
                # Map prediction to vote
                vote = 0
                if prediction == "LONG":
                    vote = 1
                elif prediction == "SHORT":
                    vote = -1
                # NEUTRAL stays 0
                
                asset_votes[asset] = vote
                total_strategies += 1
                
                logger.debug(f"DB Signal: {asset} -> {prediction} (vote: {vote})")
            
            cur.close()
            conn.close()
            
            logger.info(f"Fetched {len(asset_votes)} signals from database (total strategies: {total_strategies})")
            return asset_votes, total_strategies
            
        except Exception as e:
            logger.error(f"Failed to fetch signals from database: {e}")
            if conn:
                conn.close()
            return {}, 0

# --- Main Octopus Engine ---

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = DatabaseSignalFetcher()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.instrument_specs = {}

    def initialize(self):
        logger.info("Initializing Octopus (Database Mode)...")
        self._fetch_instrument_specs()
        
        # Test API Connection
        logger.info("Checking Kraken Futures API Connection...")
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                logger.error(f"API Error: {acc}")
            else:
                logger.info("✓ Kraken API Connection Successful.")
        except Exception as e:
            logger.error(f"✗ Kraken API Connection Failed: {e}")

        # Test Database Connection
        logger.info("Checking Database Connection...")
        conn = self.fetcher.get_connection()
        if conn:
            logger.info("✓ Database Connection Successful.")
            conn.close()
        else:
            logger.error("✗ Database Connection Failed.")

        logger.info("Initialization Complete. Bot ready.")

    def _fetch_instrument_specs(self):
        """Fetch trading specifications for all Kraken Futures instruments"""
        try:
            import requests
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].lower()
                    tick_size = float(inst.get("tickSize", 0.1))
                    precision = inst.get("contractValueTradePrecision")
                    size_step = 10 ** (-int(precision)) if precision is not None else 1.0
                    
                    self.instrument_specs[sym] = {
                        "sizeStep": size_step,
                        "tickSize": tick_size,
                        "contractSize": float(inst.get("contractSize", 1.0))
                    }
                logger.info(f"Loaded specs for {len(self.instrument_specs)} instruments")
        except Exception as e:
            logger.error(f"Error fetching instrument specs: {e}")

    def _round_to_step(self, value: float, step: float) -> float:
        """Round value to nearest step size"""
        if step == 0: return value
        rounded = round(value / step) * step
        if isinstance(step, float) and "." in str(step):
            decimals = len(str(step).split(".")[1])
            rounded = round(rounded, decimals)
        elif isinstance(step, int) or step.is_integer():
            rounded = int(rounded)
        return rounded

    def run(self):
        """Main execution loop - syncs with 30-minute intervals"""
        logger.info("Bot started. Syncing with 30-minute intervals...")
        
        while True:
            now = datetime.now(timezone.utc)
            
            # Trigger at 0 and 30 minutes past the hour, with 60s buffer after candle close
            # This gives the inference script time to update predictions
            if now.minute in [0, 30] and 60 <= now.second < 70:
                logger.info(f"\n{'='*60}")
                logger.info(f"EXECUTION TRIGGER: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info(f"{'='*60}")
                
                self._process_signals()
                
                time.sleep(60)  # Prevent double trigger
                
            time.sleep(1) 

    def _process_signals(self):
        """Main signal processing logic"""
        # 1. Fetch Signals from Database
        asset_votes, total_strategies = self.fetcher.fetch_signals()
        
        if total_strategies == 0:
            logger.warning("No active strategies found in database. Skipping execution.")
            return

        # 2. Get Account Equity
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            elif "accounts" in acc:
                first_acc = list(acc["accounts"].values())[0]
                equity = float(first_acc.get("marginEquity", 0))
            else:
                equity = 0
                
            if equity <= 0:
                logger.error("Account equity is 0 or negative. Aborting execution.")
                return
                
            logger.info(f"Account Equity: ${equity:,.2f}")
            
        except Exception as e:
            logger.error(f"Failed to fetch account info: {e}")
            return

        # 3. Calculate Unit Size per Strategy
        # Formula: (Equity * Leverage) / Total Active Strategies
        unit_size_usd = (equity * LEVERAGE) / total_strategies
        logger.info(f"Active Strategies: {total_strategies} | Unit Size: ${unit_size_usd:,.2f}")

        # 4. Execute trades for each asset
        # Use maker loop settings optimized for 30-minute execution window
        exec_duration = 120  # 2 minutes per asset
        exec_interval = 10   # Check/adjust every 10 seconds
        start_offset_bp = 0  # Start at market
        step_bp = 2.0        # Increase aggression by 2bp per step

        logger.info(f"\n{'='*60}")
        logger.info("ASSET ALLOCATION & EXECUTION")
        logger.info(f"{'='*60}")

        for asset, vote in asset_votes.items():
            target_usd = vote * unit_size_usd
            
            # Format output with emoji
            icon = "⚪"
            action = "NEUTRAL"
            if vote > 0:
                icon = "🟢"
                action = "LONG"
            elif vote < 0:
                icon = "🔴"
                action = "SHORT"
            
            logger.info(f"{asset:12} | {action:8} {icon} | Vote: {vote:+2d} | Target: ${target_usd:+,.2f}")
            
            # Submit execution task to thread pool
            self.executor.submit(
                self._execute_single_asset_logic, 
                asset, 
                target_usd,
                exec_duration, 
                exec_interval, 
                start_offset_bp, 
                step_bp
            )

        logger.info(f"{'='*60}\n")

    def _execute_single_asset_logic(self, asset: str, net_target_usd: float, 
                                    duration: int, interval: int, start_bp: float, step_bp: float):
        """Execute trade logic for a single asset"""
        
        kf_symbol = SYMBOL_MAP.get(asset)
        if not kf_symbol:
            logger.warning(f"No Kraken mapping for {asset}. Skipping.")
            return

        try:
            # Get Current Position
            open_pos = self.kf.get_open_positions()
            current_pos_size = 0.0
            if "openPositions" in open_pos:
                for p in open_pos["openPositions"]:
                    if p["symbol"].lower() == kf_symbol.lower():
                        size = float(p["size"])
                        if p["side"] == "short": 
                            size = -size
                        current_pos_size = size
                        break
            
            # Get Mark Price
            tickers = self.kf.get_tickers()
            mark_price = 0.0
            for t in tickers.get("tickers", []):
                if t["symbol"].lower() == kf_symbol.lower():
                    mark_price = float(t["markPrice"])
                    break
            
            if mark_price == 0:
                logger.error(f"[{kf_symbol}] Could not get mark price. Skipping.")
                return
            
            # Calculate Position Delta
            target_contracts = net_target_usd / mark_price
            delta = target_contracts - current_pos_size
            
            # Check minimum size requirements
            specs = self.instrument_specs.get(kf_symbol.lower())
            size_increment = specs['sizeStep'] if specs else 0.001
            check_qty = self._round_to_step(abs(delta), size_increment)

            if check_qty < size_increment:
                logger.debug(f"[{kf_symbol}] Delta too small ({delta:.4f}). No action needed.")
                return

            logger.info(f"[{kf_symbol}] Executing Delta: {delta:+.4f} contracts (Current: {current_pos_size:.4f} → Target: {target_contracts:.4f})")

            # Execute via maker loop
            self._run_maker_loop(kf_symbol, delta, mark_price, duration, interval, start_bp, step_bp)

        except Exception as e:
            logger.error(f"[{kf_symbol}] Execution error: {e}", exc_info=True)

    def _run_maker_loop(self, symbol: str, quantity: float, initial_mark: float, 
                        max_duration: int, interval: int, start_offset_bp: float, step_bp: float):
        """
        Maker loop: Places limit order and gradually walks it toward market price.
        """
        side = "buy" if quantity > 0 else "sell"
        abs_qty = abs(quantity)
        
        specs = self.instrument_specs.get(symbol.lower())
        size_inc = specs['sizeStep'] if specs else 0.001
        price_inc = specs['tickSize'] if specs else 0.01

        steps = max_duration // interval
        order_id = None
        
        for i in range(steps + 1):
            try:
                # Get current market price
                tickers = self.kf.get_tickers()
                curr_mark = 0.0
                for t in tickers.get("tickers", []):
                    if t["symbol"].lower() == symbol.lower():
                        curr_mark = float(t["markPrice"])
                        break
                if curr_mark == 0: 
                    curr_mark = initial_mark
                
                # Calculate limit price with increasing aggression
                current_aggression_bp = start_offset_bp + (i * step_bp)
                pct_change = current_aggression_bp * 0.0001
                
                if side == "buy":
                    final_limit = curr_mark * (1 + pct_change)
                else:
                    final_limit = curr_mark * (1 - pct_change)

                final_limit = self._round_to_step(final_limit, price_inc)
                final_size = self._round_to_step(abs_qty, size_inc)
                
                # Place or edit order
                if order_id is None:
                    resp = self.kf.send_order({
                        "orderType": "lmt", 
                        "symbol": symbol, 
                        "side": side,
                        "size": final_size, 
                        "limitPrice": final_limit
                    })
                    if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                        order_id = resp["sendStatus"]["order_id"]
                        logger.info(f"[{symbol}] Order placed @ ${final_limit} ({current_aggression_bp}bp)")
                    else:
                        logger.warning(f"[{symbol}] Order placement failed: {resp}")
                else:
                    self.kf.edit_order({
                        "orderId": order_id, 
                        "limitPrice": final_limit,
                        "size": final_size, 
                        "symbol": symbol 
                    })
                    logger.debug(f"[{symbol}] Order adjusted @ ${final_limit} ({current_aggression_bp}bp)")
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"[{symbol}] Maker loop error: {e}")
                time.sleep(1)
        
        # Cancel any remaining unfilled order
        if order_id:
            try:
                self.kf.cancel_order({"order_id": order_id, "symbol": symbol})
                logger.info(f"[{symbol}] Order cancelled after {max_duration}s")
            except Exception as e:
                logger.debug(f"[{symbol}] Cancel order error: {e}")

if __name__ == "__main__":
    bot = Octopus()
    bot.initialize()
    bot.run()
