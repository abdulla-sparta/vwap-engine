# engine/trade_engine.py
#
# VWAP + HTF Confluence Trade Engine
#
# ─────────────────────────────────────────────────────────────────────────────
# ENTRY FLOW:
#   1. on_htf_candle() → HTFStructure updates bias (15m BOS)
#   2. on_ltf_candle() → VWAP.update() (intraday, anchored 09:15)
#                      → VWAPEntry.update(candle, bias, vwap_data)
#                      → if signal: PaperBroker.open()
#
# KEY DIFFERENCE from structure engine:
#   - VWAP resets every day at 09:15 (first 5m candle of the session)
#   - Entry condition: VWAP reclaim (long) or VWAP rejection (short)
#   - Stop is placed at VWAP ±1σ band, not prior candle extreme
#
# TRAILING KILL SWITCH (live only):
#   equity_peak × kill_switch_percent = threshold
#   If equity drops below threshold → kill switch fires, no new entries.
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
from config import CONFIG
from engine.session import is_entry_allowed, is_force_exit_time
from engine.scheduler import DailyScheduler
from engine.state_manager import StateManager


class TradeEngine:

    def __init__(self, broker,
                 cooldown_candles:    int   = None,
                 min_price_distance:  float = None,
                 symbol:              str   = "",
                 backtest_rr:         float = None):

        self.broker  = broker
        self.symbol  = symbol
        self.index   = 0

        # Strategy modules — set by attach()
        self.htf  = None   # HTFStructure
        self.vwap = None   # VWAP
        self.ltf  = None   # VWAPEntry

        self.current_bias = None
        self.last_vwap    = None

        # Entry cooldown
        self.cooldown_candles  = cooldown_candles  if cooldown_candles  is not None else CONFIG["cooldown"]
        self.min_price_distance = min_price_distance if min_price_distance is not None else CONFIG["min_price_distance"]
        self.last_entry_index   = -9999
        self.last_entry_price   = None

        # Kill switch (live only)
        ks = CONFIG.get("kill_switch_enabled", True)
        self.kill_switch_enabled   = ks
        self.kill_switch_triggered = False
        self.equity_peak           = None

        # Backtest vs live
        self._is_backtest = backtest_rr is not None
        if self._is_backtest:
            if self.ltf and hasattr(self.ltf, "set_rr"):
                self.ltf.set_rr(backtest_rr)

        self.replay_mode = False

        # State persistence (live only)
        self._state_mgr = StateManager(symbol) if not self._is_backtest else None
        if not self._is_backtest:
            self._load_state()

        # VWAP daily reset tracking
        self._last_vwap_date = None

    def _log_entry_block(self, reason: str, candle, vwap_data=None):
        if not CONFIG.get("entry_debug_logs", False):
            return
        try:
            ts = candle.name.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = str(getattr(candle, "name", "N/A"))
        close = None
        try:
            close = round(float(candle["close"]), 2)
        except Exception:
            close = None
        vwap_val = None
        if isinstance(vwap_data, dict):
            vwap_val = vwap_data.get("vwap")
        print(
            f"[{self.symbol}] [ENTRY-BLOCK] {reason} | "
            f"time={ts} bias={self.current_bias or 'NONE'} "
            f"close={close} vwap={vwap_val}"
        )

    # ── Module attachment ─────────────────────────────────────────────────────

    def attach(self, htf_structure, vwap_instance, vwap_entry):
        self.htf  = htf_structure
        self.vwap = vwap_instance
        self.ltf  = vwap_entry

    # ── State persistence ─────────────────────────────────────────────────────

    def _load_state(self):
        if self._state_mgr is None:
            return
        state = self._state_mgr.load()
        if state:
            self.kill_switch_triggered = state.get("kill_switch_triggered", False)
            self.equity_peak           = state.get("equity_peak")
            self.current_bias          = state.get("bias")
            print(f"[{self.symbol}] State loaded: bias={self.current_bias} "
                  f"ks={self.kill_switch_triggered}")

    def _save_state(self):
        if self._state_mgr is None:
            return
        self._state_mgr.save({
            "kill_switch_triggered": self.kill_switch_triggered,
            "equity_peak":           self.equity_peak,
            "bias":                  self.current_bias,
        })

    # ── Replay (live startup gap-fill) ────────────────────────────────────────

    def start_replay(self):
        self.replay_mode = True

    def stop_replay(self):
        self.replay_mode = False

    # ── Candle handlers ───────────────────────────────────────────────────────

    def on_htf_candle(self, candle):
        if self.htf is None:
            return
        bias = self.htf.update(candle)
        if bias != self.current_bias:
            self.current_bias = bias
            print(f"[{self.symbol}] HTF bias → {bias}")

    def on_ltf_candle(self, candle):
        self.index += 1

        # Update VWAP — reset daily at 09:15
        if self.vwap is not None:
            candle_date = None
            try:
                candle_date = candle.name.date() if hasattr(candle.name, "date") else None
            except Exception:
                pass
            if candle_date and candle_date != self._last_vwap_date:
                self.vwap.reset()
                if self.ltf:
                    self.ltf.reset()
                self._last_vwap_date = candle_date
            vwap_data = self.vwap.update(candle)
            self.last_vwap = vwap_data
        else:
            vwap_data = None

        price        = float(candle["close"])
        current_time = candle.name

        # 1. Kill switch check (live only)
        if not self._is_backtest and self.kill_switch_enabled:
            equity = self.broker.get_equity(price)
            if self.equity_peak is None:
                self.equity_peak = equity
            elif equity > self.equity_peak:
                self.equity_peak = equity

            threshold = self.equity_peak * CONFIG.get("kill_switch_percent", 0.90)
            if not self.kill_switch_triggered and equity < threshold:
                self.kill_switch_triggered = True
                print(f"[{self.symbol}] ⛔ Kill switch triggered — "
                      f"equity ₹{equity:,.0f} < threshold ₹{threshold:,.0f}")

        # 2. Update open position (SL / target / trailing)
        if self.broker.position:
            self.broker.update(price, current_time)
            if not self.broker.position:
                if not self._is_backtest and CONFIG.get("mode") == "live":
                    self._save_state()
                return

        # 3. Kill switch block
        if self.kill_switch_triggered:
            if not self.broker.position:
                self._log_entry_block("kill_switch_triggered", candle, vwap_data)
                return

        # 4. Force exit at session end
        if is_force_exit_time(current_time):
            if self.broker.position:
                self.broker.force_close(price=price, time=candle.name)
            if not self._is_backtest and CONFIG.get("mode") == "live":
                self._save_state()
            self._log_entry_block("force_exit_time_reached", candle, vwap_data)
            return

        # 5. Session filter — live only
        if not self._is_backtest and CONFIG.get("mode") == "live":
            if not is_entry_allowed(current_time):
                self._save_state()
                self._log_entry_block("outside_entry_window", candle, vwap_data)
                return

        # 6. Guards
        if not self.current_bias:
            self._log_entry_block("no_htf_bias", candle, vwap_data)
            return
        if self.broker.position:
            self._log_entry_block("position_already_open", candle, vwap_data)
            return
        if (self.index - self.last_entry_index) < self.cooldown_candles:
            self._log_entry_block("cooldown_active", candle, vwap_data)
            return
        if self.replay_mode:
            self._log_entry_block("replay_mode_active", candle, vwap_data)
            return

        # 7. VWAP Entry signal
        if self.ltf is None or not self.vwap or not self.vwap.is_ready:
            self._log_entry_block("vwap_not_ready", candle, vwap_data)
            return
        signal = self.ltf.update(candle, self.current_bias, vwap_data)
        if not signal:
            self._log_entry_block("no_ltf_vwap_signal", candle, vwap_data)
            return

        # 8. Price distance filter
        if (self.last_entry_price is not None and
                abs(signal["entry"] - self.last_entry_price) < self.min_price_distance):
            self._log_entry_block("min_price_distance_filter", candle, vwap_data)
            return

        # 9. Execute trade
        self.broker.open(
            side=signal["side"],
            price=signal["entry"],
            stop=signal["stop"],
            target=signal["target"],
            time=candle.name,
        )
        self.last_entry_index = self.index
        self.last_entry_price = signal["entry"]

        # 9b. Emit signal alert to dashboard + persist to DB
        try:
            from live_engine.instrument_runner import _socketio_emit
            import datetime as _dt
            _now = _dt.datetime.now()
            rr_val = (
                round(abs(signal["target"] - signal["entry"]) /
                      abs(signal["entry"] - signal["stop"]), 1)
                if signal["entry"] != signal["stop"] else 0
            )
            _sig_payload = {
                "symbol":  self.symbol,
                "side":    signal["side"],
                "entry":   signal["entry"],
                "stop":    signal["stop"],
                "target":  signal["target"],
                "qty":     self.broker.position.get("qty", 0) if self.broker.position else 0,
                "bias":    self.current_bias,
                "rr":      rr_val,
                "vwap":    signal.get("vwap"),
                "band":    signal.get("band"),
                "time":    _now.strftime("%H:%M:%S"),
                "ts":      _now.isoformat(),
                "status":  "fired",
            }
            # Persist to DB so /api/signals can query by date
            try:
                import db as _db
                _db.save_signal(self.symbol, _sig_payload, _now.strftime("%Y-%m-%d"))
            except Exception:
                pass
            if _socketio_emit:
                _socketio_emit("signal_fired", _sig_payload)
        except Exception:
            pass

        # 10. Scheduler + state (live only)
        if not self._is_backtest and CONFIG.get("mode") == "live":
            self.scheduler.check(candle_time=candle.name, current_price=price)
            self._save_state()

    def reset_kill_switch(self):
        self.kill_switch_triggered = False
        self.equity_peak           = self.broker.balance
        self._save_state()
        print(f"[{self.symbol}] ✅ Kill switch reset — peak=₹{self.equity_peak:,.0f}")

    def get_summary(self) -> dict:
        b     = self.broker
        start = b.starting_balance
        net   = b.total_net_pnl
        wins  = sum(1 for t in b.trade_log if t["net_pnl"] > 0)
        total = len(b.trade_log)
        return {
            "symbol":    self.symbol,
            "net_pnl":   round(net, 2),
            "return_pct": round(net / start * 100, 2) if start else 0,
            "trades":    total,
            "wins":      wins,
            "win_rate":  round(wins / total * 100, 1) if total else 0,
            "bias":      self.current_bias,
        }
