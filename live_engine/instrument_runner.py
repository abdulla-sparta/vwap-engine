# live_engine/instrument_runner.py
# One InstrumentRunner per symbol — LIVE mode only.
# Receives ticks from UpstoxV3Client (streaming).
# Uses VWAP + HTF Bias confluence strategy.

from broker.paper_broker import PaperBroker
from engine.trade_engine import TradeEngine
from engine.htf_structure import HTFStructure
from engine.vwap import VWAP
from engine.vwap_entry import VWAPEntry
from engine.scheduler import DailyScheduler
from live_engine.live_candle_builder import LiveCandleBuilder
from config import CONFIG

# SocketIO emitter — set by app.py
_socketio_emit = None

def set_socketio_emitter(fn):
    global _socketio_emit
    _socketio_emit = fn


class InstrumentRunner:

    def __init__(self, symbol: str, instrument_token: str, preset: dict):
        self.symbol           = symbol
        self.instrument_token = instrument_token

        risk     = float(preset.get("risk_per_trade",     CONFIG["risk_per_trade"]))
        cooldown = int(preset.get("cooldown",             CONFIG["cooldown"]))
        min_dist = float(preset.get("min_price_distance", CONFIG["min_price_distance"]))
        rr       = float(preset.get("rr_target",          CONFIG["rr_target"]))
        band_conf = bool(preset.get("band_confluence",    CONFIG.get("band_confluence", True)))

        self.broker = PaperBroker(
            balance=CONFIG["starting_balance"],
            risk_per_trade=risk,
            symbol=symbol,
            live_mode=True,
        )

        self.engine = TradeEngine(
            self.broker,
            cooldown_candles=cooldown,
            min_price_distance=min_dist,
            symbol=symbol,
        )
        swing_look  = int(CONFIG.get("swing_lookback", 20))
        pivot_side  = max(2, swing_look // 2)
        self.engine.attach(
            HTFStructure(pivot_left=pivot_side, pivot_right=pivot_side),
            VWAP(),
            VWAPEntry(rr_target=rr, band_confluence=band_conf),
        )

        self.builder_5m  = LiveCandleBuilder(5)
        self.builder_15m = LiveCandleBuilder(15)
        self.scheduler   = DailyScheduler(self.broker, symbol)

        # Tick state
        self.last_price  = None
        self.last_time   = None
        self.prev_close  = None
        self.last_volume = None
        self.last_atp    = None

        print(f"[{self.symbol}] VWAP+HTF runner ready")

    # ── Tick entry point ──────────────────────────────────────────────────────

    def on_tick(self, ltp: float, prev_close=None, timestamp=None,
                volume=None, atp=None):
        self.last_price  = ltp
        self.last_time   = timestamp
        self.last_volume = volume
        self.last_atp    = atp

        if prev_close and not self.prev_close:
            self.prev_close = prev_close

        # Emit tick to browser
        if _socketio_emit:
            try:
                from datetime import datetime as _dt
                pc  = self.prev_close or ltp
                chg = round(ltp - pc, 2)
                pct = round((chg / pc) * 100, 2) if pc else 0.0
                ts  = timestamp.strftime("%H:%M:%S") if timestamp else _dt.now().strftime("%H:%M:%S")
                _socketio_emit("tick_update", {
                    "symbol":     self.symbol,
                    "ltp":        round(ltp, 2),
                    "prev_close": round(pc, 2),
                    "change":     chg,
                    "change_pct": pct,
                    "ts":         ts,
                }, broadcast=True)
            except Exception:
                pass

        candle_5m  = self.builder_5m.update(ltp, timestamp, volume)
        candle_15m = self.builder_15m.update(ltp, timestamp, volume)

        if candle_15m is not None:
            self.engine.on_htf_candle(candle_15m)

        if candle_5m is not None:
            self.engine.on_ltf_candle(candle_5m)
            self.scheduler.check(
                candle_time=candle_5m.name,
                current_price=candle_5m["close"],
            )

    # ── Status for live dashboard ──────────────────────────────────────────────

    def get_status(self) -> dict:
        b   = self.broker
        ltp = self.last_price

        if not ltp:
            try:
                from datetime import datetime, timezone, timedelta, time as _dtime
                _ist = timezone(timedelta(hours=5, minutes=30))
                _now = datetime.now(_ist).time()
                if not (_dtime(9, 14) <= _now <= _dtime(15, 36)):
                    import heatmap_feed as _hf
                    cached = _hf.get_ltp(self.symbol)
                    if cached:
                        ltp = cached.get("ltp")
                        if not self.prev_close:
                            self.prev_close = cached.get("prev_close")
            except Exception:
                pass

        pc      = self.prev_close
        chg     = round(ltp - pc, 2)         if ltp and pc else None
        chg_pct = round((chg / pc) * 100, 2) if chg and pc else None

        from datetime import date as _date
        today_str = str(_date.today())
        daily_trades = [
            {
                "direction":   t.get("side", t.get("direction", "")),
                "entry_time":  str(t.get("entry_time", "")),
                "exit_time":   str(t.get("exit_time", "")),
                "entry_price": t.get("entry_price", t.get("entry", 0)),
                "exit_price":  t.get("exit_price",  t.get("exit",  0)),
                "qty":         t.get("qty", 0),
                "net_pnl":     round(t.get("net_pnl", 0), 2),
                "gross_pnl":   round(t.get("gross_pnl", 0), 2),
                "charges":     round(t.get("charges", 0), 2),
                "reason":      t.get("reason", ""),
            }
            for t in b.trade_log
            if str(t.get("entry_time", "")).startswith(today_str)
        ]
        realised   = sum(t["net_pnl"] for t in daily_trades)
        unrealised = round(b.get_equity(ltp or b.balance) - b.balance, 2) if b.position else 0.0

        # VWAP state for dashboard
        vwap_data = self.engine.last_vwap or {}

        return {
            "symbol":        self.symbol,
            "ltp":           ltp,
            "prev_close":    pc,
            "change":        chg,
            "change_pct":    chg_pct,
            "volume":        self.last_volume,
            "atp":           self.last_atp,
            "bias":          self.engine.current_bias,
            "vwap":          vwap_data.get("vwap"),
            "vwap_upper1":   vwap_data.get("upper1"),
            "vwap_lower1":   vwap_data.get("lower1"),
            "position":      b.position,
            "balance":       round(b.balance, 2),
            "equity":        round(b.get_equity(ltp or b.balance), 2),
            "realised_pnl":  round(realised, 2),
            "unrealised_pnl":round(unrealised, 2),
            "daily_net":     round(b.daily_net_pnl, 2),
            "total_net":     round(b.total_net_pnl, 2),
            "daily_trades":  daily_trades,
            "kill_switch": {
                "triggered": self.engine.kill_switch_triggered,
                "peak":      round(self.engine.equity_peak, 2) if self.engine.equity_peak is not None else None,
                "current":   round(b.get_equity(ltp or b.balance), 2),
                "percent":   CONFIG.get("kill_switch_percent", 0.90),
            },
        }

    def get_trades(self) -> list:
        return self.broker.trade_log

    def get_equity_log(self) -> list:
        return self.broker.equity_log
