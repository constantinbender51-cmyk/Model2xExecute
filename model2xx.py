#!/usr/bin/env python3
"""
Octopus: NetSum Strategy (ETH Fixed Futures Edition)
Target: ff_ethusd260130 | Source: Railway JSON
"""

import os
import sys
import time
import logging
import requests
import threading
import math
from datetime import datetime, timezone

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
DATA_URL = "https://web-production-73e1d.up.railway.app/"

# Strategy Constants
TARGET_SYMBOL = "ff_ethusd260130"
LEVERAGE = 1.0
TIMEFRAME_MINUTES = 1
TRIGGER_OFFSET_SEC = 3
LOG_LOCK = threading.Lock()

# Execution Constants
MAX_EXECUTION_TIME = 45
UPDATE_INTERVAL = 5
MAX_EDITS = 8 # 8 edits * 5s = 40s (buffer for MKT order at end)
INITIAL_OFFSET_PCT = 0.0002 # 0.02%
OFFSET_DECAY = 0.90 # Reduce offset by 10% per edit

# Configure standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("octopus_exec.log")]
)
logger = logging.getLogger("Octopus")

def octopus_log(msg, level="info"):
    """Custom print function with thread safety."""
    with LOG_LOCK:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {msg}"
        print(formatted_msg)
        
        if level == "info": logger.info(msg)
        elif level == "error": logger.error(msg)
        elif level == "warning": logger.warning(msg)

class Octopus:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.instrument_specs = {}

    def initialize(self):
        octopus_log("--- Initializing Octopus Engine (NetSum ETH) ---")
        self._fetch_instrument_specs()
        
        try:
            octopus_log("Verifying Kraken API connectivity...")
            acc = self.kf.get_accounts()
            if "error" in acc: 
                octopus_log(f"API Error detected: {acc}", "error")
                sys.exit(1)
            else:
                octopus_log("API Connection: SUCCESS")
                
            if TARGET_SYMBOL not in self.instrument_specs:
                octopus_log(f"CRITICAL: Target symbol {TARGET_SYMBOL} not found on Kraken.", "error")
                sys.exit(1)
                
        except Exception as e:
            octopus_log(f"Initialization Critical Failure: {e}", "error")
            sys.exit(1)

    def _fetch_instrument_specs(self):
        try:
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
            octopus_log(f"Failed to fetch specs: {e}", "error")

    def _round_to_step(self, value, step):
        if step == 0: return value
        return round(round(value / step) * step, 8)

    def run(self):
        octopus_log(f"Bot Active. Strategy: {TIMEFRAME_MINUTES}m Sync | Target: {TARGET_SYMBOL}")
        while True:
            now = datetime.now(timezone.utc)
            # Sync to XX:XX:03
            if now.minute % TIMEFRAME_MINUTES == 0 and now.second == TRIGGER_OFFSET_SEC:
                octopus_log(f">>> TRIGGERED AT {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                octopus_log("Cycle complete. Waiting for next minute.")
                time.sleep(2) 
            time.sleep(0.5)

    def _process_cycle(self):
        try:
            # 1. Fetch Data
            octopus_log(f"Fetching NetSum from {DATA_URL}...")
            resp = requests.get(DATA_URL, timeout=5).json()
            net_sum = float(resp.get("netSum", 0))
            octopus_log(f"Data Recieved: NetSum={net_sum}")

            # 2. Get Equity
            acc = self.kf.get_accounts()
            equity = float(acc.get("accounts", {}).get("flex", {}).get("marginEquity", 0))
            
            if equity <= 0:
                octopus_log("Insufficient Equity. Skipping.", "error")
                return

            # 3. Calculate Target Position (USD)
            # Formula: (netSum/1440) * marginEquity * leverage
            target_usd = (net_sum / 1440.0) * equity * LEVERAGE
            
            octopus_log(f"Logic: ({net_sum}/1440) * ${equity:.2f} * {LEVERAGE}x = Target ${target_usd:.2f}")

            self._execute_logic(target_usd)

        except Exception as e:
            octopus_log(f"Cycle Error: {e}", "error")

    def _execute_logic(self, target_usd: float):
        specs = self.instrument_specs[TARGET_SYMBOL]
        
        try:
            # 1. Get Current Position
            pos_resp = self.kf.get_open_positions()
            current_qty = 0.0
            for p in pos_resp.get("openPositions", []):
                if p["symbol"].lower() == TARGET_SYMBOL:
                    current_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                    break
            
            # 2. Get Mark Price
            tick_resp = self.kf.get_tickers()
            mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), 0)
            
            if mark_price == 0:
                octopus_log("Error: Could not fetch mark price.", "error")
                return

            # 3. Calculate Delta
            target_qty = target_usd / mark_price
            delta = target_qty - current_qty
            
            # 4. Filter Small Moves ("Stop if close enough")
            if abs(delta) < specs["sizeStep"]:
                octopus_log(f"Delta {delta:.4f} < Step {specs['sizeStep']}. Position accurate. No trade.")
                return

            octopus_log(f"Execution Required | Target: {target_qty:.4f} | Current: {current_qty:.4f} | Delta: {delta:.4f}")
            self._run_smart_execution(delta, mark_price)

        except Exception as e:
            octopus_log(f"Execution Logic Failed: {e}", "error")

    def _run_smart_execution(self, quantity, initial_mark):
        """
        Executes order with Smart Maker Loop:
        1. Starts 0.02% passive.
        2. Updates every 5s, reducing offset by 10% (getting closer to mark).
        3. Max 8 edits.
        4. If not filled, sends Market Order.
        """
        side = "buy" if quantity > 0 else "sell"
        abs_qty = abs(quantity)
        specs = self.instrument_specs[TARGET_SYMBOL]
        
        order_id = None
        current_offset = INITIAL_OFFSET_PCT # Starts at 0.02%

        octopus_log(f"Starting Execution: {side.upper()} {abs_qty:.4f} {TARGET_SYMBOL}")

        for i in range(MAX_EDITS):
            # Refresh Mark Price for accuracy
            try:
                tick_resp = self.kf.get_tickers()
                mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), initial_mark)
                
                # Calculate Limit Price
                # If Buy: Price = Mark * (1 - Offset) -> Lower than mark (Passive)
                # If Sell: Price = Mark * (1 + Offset) -> Higher than mark (Passive)
                price_mult = (1 - current_offset) if side == "buy" else (1 + current_offset)
                limit_price = self._round_to_step(mark_price * price_mult, specs["tickSize"])
                size = self._round_to_step(abs_qty, specs["sizeStep"])

                if order_id is None:
                    # Place Initial Order
                    res = self.kf.send_order({
                        "orderType": "lmt", 
                        "symbol": TARGET_SYMBOL, 
                        "side": side, 
                        "size": size, 
                        "limitPrice": limit_price
                    })
                    if "error" in res:
                        octopus_log(f"Order Placement Failed: {res}", "error")
                        break # Break to fallback
                    
                    status = res.get("sendStatus", {})
                    order_id = status.get("order_id")
                    octopus_log(f"Iter {i+1}: Placed {order_id} @ {limit_price} (Offset: {current_offset*100:.4f}%)")
                else:
                    # Edit Existing Order
                    self.kf.edit_order({
                        "orderId": order_id, 
                        "limitPrice": limit_price, 
                        "size": size, 
                        "symbol": TARGET_SYMBOL
                    })
                    octopus_log(f"Iter {i+1}: Updated @ {limit_price} (Offset: {current_offset*100:.4f}%)")

                # Wait for fill or next interval
                time.sleep(UPDATE_INTERVAL)

                # Check if filled (optional optimization, API dependent, assuming loop continues if not filled)
                # Here we assume if no error, we continue to aggression
                
                # Decay the offset for next round (Make it 10% smaller/more aggressive)
                current_offset = current_offset * OFFSET_DECAY

            except Exception as e:
                octopus_log(f"Loop Exception: {e}", "warning")
                time.sleep(1)

        # --- Post-Loop Verification & Market Fallback ---
        octopus_log("Maker loop finished. Checking status...")
        
        remaining_qty = abs_qty # Default assumption
        
        # 1. Cancel any active limit order
        if order_id:
            try:
                self.kf.cancel_order({"order_id": order_id, "symbol": TARGET_SYMBOL})
                time.sleep(0.5) # Wait for cancel to process
            except:
                pass

        # 2. Check remaining position size needed
        try:
            pos_resp = self.kf.get_open_positions()
            # We need to recalculate delta because some might have filled
            # Re-fetch is safer than tracking local state blindly
            current_actual = 0.0
            for p in pos_resp.get("openPositions", []):
                if p["symbol"].lower() == TARGET_SYMBOL:
                    current_actual = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                    break
            
            # Recalculate original target vs actual
            # Note: We must remember the original target_usd/mark logic or just check delta again
            # Simplified: We know what we wanted to Buy/Sell (quantity). 
            # If we were buying 1.0, and now we have 0.2 more than start, we need 0.8
            # But simpler: Just do a raw delta check against the intended target state is safest
            
            # However, to be fast, we can just check if the order filled via API, 
            # but Kraken API is complex. 
            # Let's do a fresh Delta calculation to be 100% sure what's left.
            
            # Re-fetch Mark
            tick_resp = self.kf.get_tickers()
            mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), initial_mark)
            
            # Recalculate Target Quantity (Target USD hasn't changed)
            # We need to pass target_usd or Recalc it. 
            # Since we are inside execution, let's assume valid 'quantity' passed was the Total Delta needed.
            # We just need to see if that Delta was achieved.
            
            # Actually, standard practice for Market Fallback:
            # Just send MKT for whatever wasn't filled on the Limit order.
            # But since we cancelled, we don't know partial fill easily without querying fills.
            
            # FASTEST PATH: Query Open Positions one last time
            new_pos_resp = self.kf.get_open_positions()
            new_qty = 0.0
            for p in new_pos_resp.get("openPositions", []):
                if p["symbol"].lower() == TARGET_SYMBOL:
                    new_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
            
            # Original Goal: Move from Start_Qty to (Start_Qty + quantity)
            # We don't have Start_Qty stored here easily unless passed.
            # Instead, let's just use the fact that 'quantity' was the delta.
            # We can't easily know "Remaining" without knowing "Start".
            
            # Hack for reliability: If we are here, we likely didn't fill completely if the price moved away.
            # But we might have partials.
            # To ensure we hit target, we need to know the GLOBAL target again.
            # But for this scope, let's assume we just fire a MKT for the *remaining* order size if we can query the order status.
            
            # Query Order Status
            if order_id:
                ords = self.kf.get_open_orders() # Or query specific order if API supports
                # Kraken Futures 'get_open_orders' returns currently open.
                # If it's not there, it's either Filled or Cancelled.
                # Since we just Cancelled it, we need to check Fills.
                pass 
                
            # PROPOSED ROBUST FALLBACK:
            # Re-read the global target logic? No, too slow.
            # We will issue a Market Order for the *Remaining Unfilled Size* of the LIMIT order.
            # We can calculate this by checking the order status before cancelling, OR
            # simply re-calculating the delta against the portfolio right now.
            
            # Let's Re-Calculate Delta from Scratch (Safest)
            # We need the target_usd from the previous scope? 
            # To avoid scope complexity, we will just pass 'target_usd' into this function in future refactors.
            # For now, we will perform a Market Order for the *full size* if the limit didn't fill? No, dangerous.
            
            # Refined Approach:
            # The 'quantity' passed to this function is the DELTA.
            # We tried to fill 'quantity'.
            # Let's check the current position vs (Old_Pos + Quantity).
            # Start_Pos = Current_Pos (now) - (Filled_Amount). Unknown.
            
            # Let's rely on the previous delta calculation.
            # We will query the Current Position NOW.
            # We calculate the NEW Delta required to hit the Target USD (which we can recompute roughly or pass down).
            
            # To fix the scope issue, I will modify `_execute_logic` to handle the Fallback, 
            # NOT `_run_smart_execution`. `_run_smart_execution` should just try to fill.
            # But the prompt asks to "Update order... max 8 edits... after which we place a mkt order".
            # Implies it happens here.
            
            # Let's use the `fills` endpoint or just check position change?
            # Lets simply Re-Calculate Delta inside here.
            # We need 'target_usd' to do that. I will add it to arguments.
             pass

        except:
             pass

    # Redefining logic to allow MKT fallback with accurate sizing
    def _execute_logic(self, target_usd: float):
        specs = self.instrument_specs[TARGET_SYMBOL]
        
        # 1. Get Initial State
        pos_resp = self.kf.get_open_positions()
        current_qty = 0.0
        for p in pos_resp.get("openPositions", []):
            if p["symbol"].lower() == TARGET_SYMBOL:
                current_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                break
        
        tick_resp = self.kf.get_tickers()
        mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), 0)
        
        target_qty = target_usd / mark_price
        initial_delta = target_qty - current_qty
        
        if abs(initial_delta) < specs["sizeStep"]:
            return # Close enough

        # 2. Run Maker Loop
        remaining_delta = self._run_maker_loop_logic(initial_delta, specs, mark_price)
        
        # 3. Market Fallback (If remaining delta is still significant)
        if abs(remaining_delta) > specs["sizeStep"]:
            octopus_log(f"Maker Loop incomplete. Remaining Delta: {remaining_delta:.4f}. Sending MKT.", "warning")
            side = "buy" if remaining_delta > 0 else "sell"
            size = self._round_to_step(abs(remaining_delta), specs["sizeStep"])
            self.kf.send_order({
                "orderType": "mkt",
                "symbol": TARGET_SYMBOL,
                "side": side,
                "size": size
            })

    def _run_maker_loop_logic(self, total_delta, specs, initial_mark):
        """
        Returns the REMAINING delta after the loop.
        """
        side = "buy" if total_delta > 0 else "sell"
        abs_qty = abs(total_delta)
        order_id = None
        current_offset = INITIAL_OFFSET_PCT
        
        octopus_log(f"Maker Strategy: {side.upper()} {abs_qty:.4f}")

        for i in range(MAX_EDITS):
            try:
                # Update Mark
                tick_resp = self.kf.get_tickers()
                mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), initial_mark)
                
                # Calc Price
                price_mult = (1 - current_offset) if side == "buy" else (1 + current_offset)
                limit_price = self._round_to_step(mark_price * price_mult, specs["tickSize"])
                size = self._round_to_step(abs_qty, specs["sizeStep"])

                if not order_id:
                    res = self.kf.send_order({
                        "orderType": "lmt", "symbol": TARGET_SYMBOL, "side": side, 
                        "size": size, "limitPrice": limit_price
                    })
                    if "error" not in res and "sendStatus" in res:
                        order_id = res["sendStatus"]["order_id"]
                        octopus_log(f"Order Placed: {limit_price}")
                else:
                    self.kf.edit_order({
                        "orderId": order_id, "limitPrice": limit_price, 
                        "size": size, "symbol": TARGET_SYMBOL
                    })
                    octopus_log(f"Order Edited: {limit_price} (Offset {current_offset*100:.4f}%)")

                time.sleep(UPDATE_INTERVAL)
                current_offset *= OFFSET_DECAY

            except Exception as e:
                octopus_log(f"Loop error: {e}", "warning")
                time.sleep(1)

        # Cleanup and calc remaining
        if order_id:
            try:
                self.kf.cancel_order({"order_id": order_id, "symbol": TARGET_SYMBOL})
                time.sleep(1) # Allow fill updates
            except: pass

        # Check actual filled amount by looking at current position vs target
        # We need to query position again to see what's left
        pos_resp = self.kf.get_open_positions()
        new_qty = 0.0
        for p in pos_resp.get("openPositions", []):
            if p["symbol"].lower() == TARGET_SYMBOL:
                new_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
        
        # We wanted to move by 'total_delta'. 
        # But price moved, so 'target_usd' implies a specific Qty.
        # Simpler: Return the difference between where we are NOW and where we WANTED to be.
        # But we don't have 'target_usd' here. 
        # We will assume 'total_delta' was the precise volume we wanted to add/subtract.
        # So Remaining = (Old_Pos + total_delta) - New_Pos.
        # But we don't have Old_Pos.
        
        # Correction: We just calculate the drift from the loop.
        # Since we don't have the state easily, let's look at the result in _execute_logic
        # For this function, let's just return 0 to force _execute_logic to re-calc, 
        # or better yet, do the re-calc in _execute_logic.
        
        return 0 # Placeholder, logic moved to _execute_logic re-check

    # Re-writing _execute_logic to be the master controller
    def _execute_logic(self, target_usd: float):
        specs = self.instrument_specs[TARGET_SYMBOL]
        
        # --- Pre-Trade Check ---
        pos_resp = self.kf.get_open_positions()
        current_qty = 0.0
        for p in pos_resp.get("openPositions", []):
            if p["symbol"].lower() == TARGET_SYMBOL:
                current_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])
                break
        
        tick_resp = self.kf.get_tickers()
        mark_price = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), 0)
        
        target_qty = target_usd / mark_price
        delta = target_qty - current_qty
        
        if abs(delta) < specs["sizeStep"]:
            return 

        # --- Maker Loop ---
        side = "buy" if delta > 0 else "sell"
        abs_qty = abs(delta)
        order_id = None
        current_offset = INITIAL_OFFSET_PCT

        octopus_log(f"Executing: {side.upper()} {abs_qty:.4f} | Target USD: {target_usd:.2f}")

        for i in range(MAX_EDITS):
            try:
                # Update Mark Price
                tick_resp = self.kf.get_tickers()
                rt_mark = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), mark_price)
                
                # Calc passive price
                price_mult = (1 - current_offset) if side == "buy" else (1 + current_offset)
                limit_price = self._round_to_step(rt_mark * price_mult, specs["tickSize"])
                size = self._round_to_step(abs_qty, specs["sizeStep"]) # Note: Simplification. Ideally re-calc size based on partial fills.

                if not order_id:
                    res = self.kf.send_order({
                        "orderType": "lmt", "symbol": TARGET_SYMBOL, "side": side, 
                        "size": size, "limitPrice": limit_price
                    })
                    if "sendStatus" in res:
                        order_id = res["sendStatus"]["order_id"]
                        octopus_log(f"LMT {i+1}: {limit_price} (Off: {current_offset*100:.3f}%)")
                else:
                    self.kf.edit_order({
                        "orderId": order_id, "limitPrice": limit_price, 
                        "size": size, "symbol": TARGET_SYMBOL
                    })
                    octopus_log(f"MOD {i+1}: {limit_price} (Off: {current_offset*100:.3f}%)")
                
                time.sleep(UPDATE_INTERVAL)
                current_offset *= OFFSET_DECAY # Decay offset
            except Exception as e:
                octopus_log(f"Loop Error: {e}", "warning")

        # --- Cleanup & Market Fallback ---
        if order_id:
            try: self.kf.cancel_order({"order_id": order_id, "symbol": TARGET_SYMBOL})
            except: pass
            time.sleep(1)

        # --- Final Drift Check ---
        # Re-fetch everything to be precise about what is left
        pos_resp = self.kf.get_open_positions()
        final_qty = 0.0
        for p in pos_resp.get("openPositions", []):
            if p["symbol"].lower() == TARGET_SYMBOL:
                final_qty = float(p["size"]) if p["side"] == "long" else -float(p["size"])

        # Re-fetch price to re-calc target qty (since price moved, target qty slightly shifts to maintain USD value)
        tick_resp = self.kf.get_tickers()
        final_mark = next((float(t["markPrice"]) for t in tick_resp["tickers"] if t["symbol"].lower() == TARGET_SYMBOL), mark_price)
        
        final_target_qty = target_usd / final_mark
        remaining_delta = final_target_qty - final_qty

        if abs(remaining_delta) > specs["sizeStep"]:
            octopus_log(f"Maker timeout. Remaining: {remaining_delta:.4f}. Executing MKT.")
            mkt_side = "buy" if remaining_delta > 0 else "sell"
            mkt_size = self._round_to_step(abs(remaining_delta), specs["sizeStep"])
            self.kf.send_order({
                "orderType": "mkt", "symbol": TARGET_SYMBOL, 
                "side": mkt_side, "size": mkt_size
            })
        else:
            octopus_log("Execution Complete (Target Reached).")

if __name__ == "__main__":
    bot = Octopus()
    bot.initialize()
    bot.run()
