# live_engine/live_candle_builder.py
#
# Builds OHLCV candles from a tick stream using time-bucketing.
# Volume from Upstox V3 WebSocket is cumulative-for-the-day (vtt field),
# so we track deltas between ticks to compute per-candle volume.

import pandas as pd
from datetime import datetime


class LiveCandleBuilder:
    """
    Call update(price, timestamp, volume) on every tick.
    Returns a closed candle Series when a new time bucket starts,
    otherwise returns None.

    Volume handling:
        Upstox sends vtt (volume traded today) — a cumulative counter.
        We compute per-candle volume as the delta between consecutive ticks.
        On the very first tick of the day (or after a reset), delta = 0;
        subsequent ticks accumulate correctly.

    Example:
        builder_5m  = LiveCandleBuilder(5)
        builder_15m = LiveCandleBuilder(15)

        def on_tick(ltp, prev_close, timestamp, volume, atp):
            candle_5m  = builder_5m.update(ltp, timestamp, volume)
            candle_15m = builder_15m.update(ltp, timestamp, volume)
            if candle_15m: engine.on_htf_candle(candle_15m)
            if candle_5m:  engine.on_ltf_candle(candle_5m)
    """

    def __init__(self, timeframe_minutes: int):
        self.tf              = timeframe_minutes
        self.current_candle  = None
        self.current_bucket  = None
        self._prev_cum_vol   = None   # last seen cumulative volume (vtt)

    def update(self, price: float, timestamp, volume=None) -> pd.Series | None:
        """
        Feed one tick. Returns closed candle as pd.Series or None.

        Parameters
        ----------
        price     : last traded price
        timestamp : tick timestamp (any pd.to_datetime-compatible)
        volume    : cumulative day volume from Upstox vtt field (or None)
        """
        dt     = pd.to_datetime(timestamp)
        bucket = dt.replace(
            minute=(dt.minute // self.tf) * self.tf,
            second=0,
            microsecond=0,
        )

        # Compute per-tick volume delta from cumulative counter
        tick_vol = 0
        if volume is not None:
            try:
                cum_vol = float(volume)
                if self._prev_cum_vol is not None and cum_vol >= self._prev_cum_vol:
                    tick_vol = cum_vol - self._prev_cum_vol
                self._prev_cum_vol = cum_vol
            except (TypeError, ValueError):
                pass

        if self.current_bucket != bucket:
            closed = self._to_series(self.current_candle, self.current_bucket)

            self.current_bucket = bucket
            self.current_candle = {
                "open":   price,
                "high":   price,
                "low":    price,
                "close":  price,
                "volume": tick_vol,
            }

            return closed  # None on very first tick

        # Update running candle
        c = self.current_candle
        c["high"]    = max(c["high"], price)
        c["low"]     = min(c["low"],  price)
        c["close"]   = price
        c["volume"] += tick_vol

        return None

    def _to_series(self, candle: dict | None, bucket) -> pd.Series | None:
        if candle is None or bucket is None:
            return None
        return pd.Series(candle, name=bucket)

    def reset(self):
        self.current_candle  = None
        self.current_bucket  = None
        self._prev_cum_vol   = None

