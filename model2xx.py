#!/usr/bin/env python3
"""
Octopus Trend: Execution Engine for ETH Trend Strategy
- Logic: Long ETH if BTC Price > 365-period BTC SMA, else Short ETH.
- Source: Binance (BTCUSDT) for OHLC/SMA.
- Execution: Kraken Futures (ETHUSD).
- Interval: 5 Minutes
- Sizing: 1x Margin Equity
- Filter: Updates only if position delta > 10%
- Debug: Prints API responses and precise size calculations.
"""

import os
import sys
import time
import logging
import requests
import numpy as np

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

# API Keys
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Strategy Settings
TRADE_SYMBOL = "PF_ETHUSD"
SIGNAL_SYMBOL = "BTCUSDT"
SMA_PERIOD = 365
LEVERAGE = 1.0
UPDATE_INTERVAL = 300

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("trend_octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OctopusTrend")

class OctopusTrendBot:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.precision = 2  # Default to 2 decimals
        self.min_size = 0.01

    def initialize(self):
        logger.info("--- Initializing Octopus Trend Bot (Debug Mode) ---")
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                logger.error(f"API Error: {acc}")
                sys.exit(1)
            
            self._fetch_specs()
            
        except Exception as e:
            logger.error(f"Startup Failed: {e}")
            sys.exit(1)

    def _fetch_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    if inst["symbol"].upper() == TRADE_SYMBOL:
                        # Fetch Precision (decimals) and Tick Size
                        self.precision = inst.get("contractValueTradePrecision", 2)
                        self.min_size = 10 ** (-int(self.precision))
                        
                        tick_size = inst.get("tickSize", 0.1)
                        
                        logger.info(f"SPECS FOUND | Symbol: {TRADE_SYMBOL}")
                        logger.info(f"Precision: {self.precision} (Min Size: {self.min_size})")
                        logger.info(f"Tick Size: {tick_size}")
                        break
        except Exception as e:
            logger.warning(f"Spec fetch failed, using defaults: {e}")

    def get_btc_sma_state(self):
        try:
            url = "https://api.binance.com/api/v3/klines"
            params = {"symbol": SIGNAL_SYMBOL, "interval": "1h", "limit": 500}
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            
            if isinstance(data, dict) and "code" in data:
                logger.error(f"Binance API Error: {data}")
                return None, None, None

            closes = np.array([float(candle[4]) for candle in data])

            if len(closes) < SMA_PERIOD:
                logger.warning(f"Insufficient Data: {len(closes)}/{SMA_PERIOD}")
                return None, None, None

            sma = np.mean(closes[-SMA_PERIOD:])
            current_price = closes[-1]

            is_bullish = current_price > sma
            return current_price, sma, is_bullish

        except Exception as e:
            logger.error(f"Signal Logic Error: {e}")
            return None, None, None

    def get_equity(self):
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                return float(acc["accounts"]["flex"].get("marginEquity", 0))
            first = list(acc.get("accounts", {}).values())[0]
            return float(first.get("marginEquity", 0))
        except Exception:
            return 0.0

    def get_current_position(self):
        try:
            pos = self.kf.get_open_positions()
            for p in pos.get("openPositions", []):
                if p["symbol"].upper() == TRADE_SYMBOL:
                    size = float(p["size"])
                    return size if p["side"] == "long" else -size
            return 0.0
        except Exception:
            return 0.0

    def _clean_qty(self, size):
        """
        Formats size to exact string precision to avoid float artifacts (e.g. 0.30000004)
        """
        # Truncate/Round to specific decimal places
        fmt = f"{{:.{self.precision}f}}"
        clean_str = fmt.format(size)
        return float(clean_str)

    def run(self):
        logger.info(f"Bot Running. Cycle: {UPDATE_INTERVAL}s.")
        
        while True:
            try:
                # 1. Equity Check
                equity = self.get_equity()
                if equity <= 0:
                    logger.error("Equity 0 or Fetch Fail.")
                    time.sleep(60)
                    continue

                # 2. Strategy Logic
                btc_price, sma, bull = self.get_btc_sma_state()
                if btc_price is None:
                    time.sleep(60)
                    continue

                logger.info(f"State | BTC: {btc_price:.2f} | SMA({SMA_PERIOD}): {sma:.2f} | Bias: {'LONG' if bull else 'SHORT'}")

                # 3. Sizing
                eth_tickers = self.kf.get_tickers()
                eth_price = 0.0
                for t in eth_tickers.get("tickers", []):
                    if t["symbol"].upper() == TRADE_SYMBOL:
                        eth_price = float(t["markPrice"])
                        break
                
                if eth_price == 0:
                    logger.error("ETH Price unavailable.")
                    continue

                target_value = equity * LEVERAGE
                target_qty_raw = target_value / eth_price
                
                if not bull:
                    target_qty_raw = -target_qty_raw

                target_qty = self._clean_qty(target_qty_raw)
                current_qty = self.get_current_position()

                # 4. Filter Logic
                should_trade = False
                
                if abs(current_qty) == 0:
                    should_trade = abs(target_qty) >= self.min_size
                elif abs(target_qty) < self.min_size:
                    should_trade = abs(current_qty) >= self.min_size
                else:
                    delta = abs(target_qty - current_qty)
                    pct_change = delta / abs(current_qty)
                    
                    if pct_change > 0.10:
                        should_trade = True
                    else:
                        logger.info(f"Delta {pct_change*100:.2f}% < 10%. Holding.")

                # 5. Execution
                if should_trade:
                    diff = target_qty - current_qty
                    
                    # Ensure diff is also clean to avoid precision errors on the order size
                    trade_size = self._clean_qty(abs(diff))

                    if trade_size >= self.min_size:
                        side = "buy" if diff > 0 else "sell"
                        
                        logger.info(f"--- PREPARING ORDER ---")
                        logger.info(f"Raw Diff: {diff}")
                        logger.info(f"Clean Size: {trade_size} (Precision: {self.precision})")
                        
                        order_payload = {
                            "orderType": "mkt",
                            "symbol": TRADE_SYMBOL.lower(),
                            "side": side,
                            "size": trade_size
                        }
                        
                        try:
                            # Capture and Print Response
                            resp = self.kf.send_order(order_payload)
                            logger.info(f"API RESPONSE: {resp}")
                            
                            if "error" in resp:
                                logger.error(f"ORDER REJECTED: {resp['error']}")
                            elif "sendStatus" in resp:
                                logger.info(f"Order Status: {resp['sendStatus']}")
                                
                        except Exception as api_err:
                            logger.error(f"API EXCEPTION: {api_err}")
                    else:
                        logger.info(f"Diff {trade_size} below min_size {self.min_size}. Skipping.")

            except Exception as e:
                logger.exception("CRITICAL LOOP ERROR")

            logger.info(f"Sleeping {UPDATE_INTERVAL/60:.1f} min...")
            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = OctopusTrendBot()
    bot.initialize()
    bot.run()
