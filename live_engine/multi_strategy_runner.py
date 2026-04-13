# live_engine/multi_strategy_runner.py
# Owns one InstrumentRunner per symbol.
# Manages the single shared WebSocket client.
# Routes ticks to correct runner AND heatmap feed.
#
# KEY ADDITION vs structure engine:
#   - Phase 1 replay seeds BOTH HTF bias AND today's VWAP from CSV history
#   - VWAP is reset at 09:15 during intraday gap-fill (phase 2)

from config import CONFIG, INSTRUMENTS
from live_engine.instrument_runner import InstrumentRunner
from live_engine.upstox_v3_client import UpstoxV3Client
import heatmap_feed
import os, pandas as pd
from datetime import date as _date, timedelta as _td


def _load_preset() -> dict:
    """Load saved live preset from DB (falls back to CONFIG defaults)."""
    try:
        import db as _db
        import json
        rows = _db.query("SELECT value FROM engine_state WHERE key='live_preset' LIMIT 1")
        if rows:
            return json.loads(rows[0][0])
    except Exception:
        pass
    return {
        "name":             "Default",
        "risk_per_trade":   CONFIG["risk_per_trade"],
        "rr_target":        CONFIG["rr_target"],
        "cooldown":         CONFIG["cooldown"],
        "min_price_distance": CONFIG["min_price_distance"],
        "band_confluence":  CONFIG.get("band_confluence", True),
    }


class MultiStrategyRunner:

    def __init__(self):
        CONFIG["mode"] = "live"

        preset = _load_preset()
        print(f"🔒 Live preset: {preset.get('name', 'Default')}")

        self.runners: dict[str, InstrumentRunner] = {}
        tokens: list[str] = []

        base_risk = float(preset.get("risk_per_trade", CONFIG["risk_per_trade"]))

        for inst in INSTRUMENTS:
            sym   = inst["symbol"]
            token = inst["token"]

            runner = InstrumentRunner(
                symbol=sym,
                instrument_token=token,
                preset=preset,
            )
            self.runners[token] = runner
            tokens.append(token)
            print(f"  ▸ {sym} ({token})")

        self._daily_loss_limit = CONFIG.get("portfolio_daily_loss_limit", 50000)
        self._portfolio_halted = False

        self.client = UpstoxV3Client(tokens=tokens)

        for token, runner in self.runners.items():
            self.client.on_tick(token, self._make_handler(runner))

        heatmap_feed.register_with_client(self.client)
        print(f"[MSR] {len(self.runners)} runners registered")

    # ── Tick routing ──────────────────────────────────────────────────────────

    def _make_handler(self, runner: InstrumentRunner):
        def handler(ltp: float, prev_close=None, timestamp=None, volume=None, atp=None):
            if self._portfolio_halted:
                if not runner.broker.position:
                    return
            runner.on_tick(ltp=ltp, prev_close=prev_close, timestamp=timestamp,
                           volume=volume, atp=atp)
            if not self._portfolio_halted:
                self._check_portfolio_loss()
        return handler

    def _check_portfolio_loss(self):
        total = sum(r.broker.daily_net_pnl for r in self.runners.values())
        if total <= -abs(self._daily_loss_limit) and not self._portfolio_halted:
            self._portfolio_halted = True
            msg = (
                f"🚨 PORTFOLIO DAILY LOSS LIMIT HIT\n"
                f"Total loss: ₹{abs(total):,.0f}\n"
                f"Limit: ₹{self._daily_loss_limit:,.0f}\nAll instruments halted."
            )
            print(msg)
            try:
                from telegram.notifier import send_telegram_message
                send_telegram_message(CONFIG["telegram_bot_token"],
                                      CONFIG["telegram_chat_id"], msg)
            except Exception:
                pass

    # ── Portfolio status ──────────────────────────────────────────────────────

    def get_all_status(self) -> list[dict]:
        return [r.get_status() for r in self.runners.values()]

    def get_portfolio_equity(self) -> float:
        return sum(
            r.broker.get_equity(r.last_price or r.broker.balance)
            for r in self.runners.values()
        )

    def is_connected(self) -> bool:
        return self.client.is_connected()

    def apply_config(self, preset: dict):
        for runner in self.runners.values():
            runner.broker.risk_per_trade = float(preset.get("risk_per_trade", runner.broker.risk_per_trade))
            runner.engine.cooldown_candles    = int(preset.get("cooldown", runner.engine.cooldown_candles))
            runner.engine.min_price_distance  = float(preset.get("min_price_distance", runner.engine.min_price_distance))
            if runner.engine.ltf and hasattr(runner.engine.ltf, "set_rr"):
                runner.engine.ltf.set_rr(float(preset.get("rr_target", 3)))
            if runner.engine.ltf and hasattr(runner.engine.ltf, "_band_conf"):
                runner.engine.ltf._band_conf = bool(preset.get("band_confluence", True))

    # ── Intraday data fetch ───────────────────────────────────────────────────

    def _fetch_today_intraday(self, token: str) -> pd.DataFrame | None:
        try:
            import requests as _req
            access_token = CONFIG.get("upstox_access_token", "")
            if not access_token:
                return None
            url = (
                f"https://api.upstox.com/v2/historical-candle/intraday/"
                f"{token.replace('|', '%7C')}/1minute"
            )
            resp = _req.get(
                url,
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                timeout=15,
            )
            if not resp.ok:
                return None
            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return None
            df = pd.DataFrame(candles, columns=["datetime","open","high","low","close","volume","oi"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.set_index("datetime").sort_index()
            return df[["open","high","low","close","volume"]]
        except Exception:
            return None

    # ── Startup replay ────────────────────────────────────────────────────────

    def _replay_htf_history(self):
        """
        Two-phase startup replay:

        PHASE 1 — Historical CSV (up to yesterday):
            Last 200 × 15m candles → HTF engine (establishes swing bias).
            Last 200 × 5m candles  → VWAP (seeds sigma, but reset on today's date).

        PHASE 2 — Today's intraday gap-fill (09:15 → now):
            Fetch today's 1min candles from Upstox.
            Feed HTF (15m) + VWAP/LTF (5m) in chronological order.
            replay_mode=True → engine processes but skips order placement.
            VWAP auto-resets on first 5m candle of today (date change).
        """
        today   = _date.today()
        data_dir = os.environ.get(
            "CSV_DATA_DIR",
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "csvdata")
        )

        for inst in INSTRUMENTS:
            sym   = inst["symbol"]
            token = inst["token"]
            runner = self.runners.get(token)
            if not runner:
                continue

            # ── PHASE 1: Historical CSV → HTF only ──────────────────────────
            csv_path = os.path.join(data_dir, f"{sym}_1YEAR_1MIN.csv")
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path, parse_dates=["datetime"])
                    df = df.sort_values("datetime").set_index("datetime")
                    # 15m for HTF
                    df15 = df["close"].resample("15min").ohlc().dropna()
                    df15.columns = ["open","high","low","close"]
                    df15["volume"] = df["volume"].resample("15min").sum()
                    cutoff = pd.Timestamp(today, tz=df15.index.tz)
                    hist15 = df15[df15.index < cutoff].tail(200)
                    for ts, row in hist15.iterrows():
                        runner.engine.on_htf_candle(pd.Series({
                            "open": row["open"], "high": row["high"],
                            "low":  row["low"],  "close": row["close"],
                            "volume": row["volume"],
                        }, name=ts))
                except Exception as e:
                    print(f"  ⚠ [{sym}] Phase 1 replay failed: {e}")

            # ── PHASE 2: Today's intraday gap-fill → HTF + VWAP + LTF ──────
            runner.engine.start_replay()
            _orig_broker_update = runner.broker.update
            runner.broker.update = lambda price, time: None   # suppress exits

            df1m = self._fetch_today_intraday(token)
            if df1m is not None and len(df1m) > 0:
                df15 = df1m["close"].resample("15min").ohlc().dropna()
                df15.columns = ["open","high","low","close"]
                df15["volume"] = df1m["volume"].resample("15min").sum()

                df5 = df1m["close"].resample("5min").ohlc().dropna()
                df5.columns = ["open","high","low","close"]
                df5["volume"] = df1m["volume"].resample("5min").sum()

                htf_list = list(df15.iterrows())
                htf_i    = 0
                for ltf_ts, ltf_row in df5.iterrows():
                    while htf_i < len(htf_list) and htf_list[htf_i][0] <= ltf_ts:
                        ts, row = htf_list[htf_i]
                        runner.engine.on_htf_candle(pd.Series({
                            "open": row["open"], "high": row["high"],
                            "low":  row["low"],  "close": row["close"],
                            "volume": row["volume"],
                        }, name=ts))
                        htf_i += 1
                    runner.engine.on_ltf_candle(pd.Series({
                        "open":   ltf_row["open"],  "high":   ltf_row["high"],
                        "low":    ltf_row["low"],   "close":  ltf_row["close"],
                        "volume": ltf_row["volume"],
                    }, name=ltf_ts))

                bias = runner.engine.current_bias
                vwap = runner.engine.last_vwap
                print(f"  📊 [{sym}] Gap-fill: {len(df1m)} ticks → bias={bias or 'NONE'} "
                      f"VWAP={vwap['vwap'] if vwap else 'N/A'}")
            else:
                print(f"  📊 [{sym}] No intraday data — market closed?")

            runner.broker.update = _orig_broker_update
            runner.engine.stop_replay()

        print("✅ Startup replay complete — engine fully caught up")

    # ── Force-exit watchdog ───────────────────────────────────────────────────

    def _force_exit_watchdog(self):
        import time as _time
        from datetime import datetime, timezone, timedelta, time as _dtime
        _IST = timezone(timedelta(hours=5, minutes=30))
        print("[MSR] Force-exit watchdog started")
        while True:
            _time.sleep(10)
            now = datetime.now(_IST).time()
            if _dtime(15, 25) <= now <= _dtime(15, 26):
                print("[MSR] ⏰ 15:25 IST — force-exit watchdog firing")
                _closed = 0
                for token, runner in self.runners.items():
                    try:
                        if runner.broker.position:
                            ltp = runner.last_price or runner.broker.position.get("entry_price", 0)
                            runner.broker.force_close(ltp, pd.Timestamp.now(tz="Asia/Kolkata"))
                            _closed += 1
                    except Exception as e:
                        print(f"[MSR] Force-close error {runner.symbol}: {e}")
                print(f"[MSR] Force-exit done — {_closed} positions closed")
                break

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def start(self):
        print("🚀 Starting VWAP+HTF MultiStrategyRunner...")
        self._replay_htf_history()
        try:
            if heatmap_feed._ws_client and heatmap_feed._ws_client.is_connected():
                heatmap_feed._ws_client.stop()
            heatmap_feed._started = False
        except Exception:
            pass
        import threading as _threading
        _threading.Thread(target=self._force_exit_watchdog, daemon=True,
                          name="ForceExitWatchdog").start()
        self.client.start()

    def stop(self):
        try:
            self.client.stop()
        except Exception as e:
            print(f"[MSR] stop error: {e}")
