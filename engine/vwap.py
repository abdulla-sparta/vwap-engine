# engine/vwap.py
#
# Anchored VWAP (reset at 09:15 IST each day) with ±1σ / ±2σ bands.
#
# VWAP = cumulative(price × volume) / cumulative(volume)
#   where price = typical price = (high + low + close) / 3
#
# Bands use rolling standard deviation of (typical_price - VWAP):
#   band_1 = VWAP ± 1 × rolling_std
#   band_2 = VWAP ± 2 × rolling_std
#
# reset() is called at the start of each trading day (09:15 IST).


class VWAP:

    def __init__(self):
        self._cum_pv  = 0.0   # cumulative price×volume
        self._cum_v   = 0.0   # cumulative volume
        self._sq_sum  = 0.0   # cumulative (tp - vwap)² × volume  (for σ)
        self._n       = 0

        # Last computed values
        self.value    = None
        self.upper1   = None
        self.lower1   = None
        self.upper2   = None
        self.lower2   = None

    # ── Public API ────────────────────────────────────────────────────────────

    def update(self, candle) -> dict | None:
        """
        Feed one closed candle. Returns dict with vwap + band values, or None
        if volume is zero / data missing.
        """
        try:
            high   = float(candle["high"])
            low    = float(candle["low"])
            close  = float(candle["close"])
            volume = float(candle.get("volume", 0))
        except (KeyError, TypeError, ValueError):
            return None

        if volume <= 0:
            return None

        tp = (high + low + close) / 3.0

        self._cum_pv += tp * volume
        self._cum_v  += volume
        self._n      += 1

        vwap = self._cum_pv / self._cum_v

        # Welford-style variance accumulation
        self._sq_sum += volume * (tp - vwap) ** 2
        variance = self._sq_sum / self._cum_v
        sigma    = variance ** 0.5

        self.value  = round(vwap, 2)
        self.upper1 = round(vwap + sigma, 2)
        self.lower1 = round(vwap - sigma, 2)
        self.upper2 = round(vwap + 2 * sigma, 2)
        self.lower2 = round(vwap - 2 * sigma, 2)

        return {
            "vwap":   self.value,
            "upper1": self.upper1,
            "lower1": self.lower1,
            "upper2": self.upper2,
            "lower2": self.lower2,
            "sigma":  round(sigma, 4),
        }

    def reset(self):
        """Call at start of each trading day (09:15 IST anchor reset)."""
        self._cum_pv = 0.0
        self._cum_v  = 0.0
        self._sq_sum = 0.0
        self._n      = 0
        self.value   = None
        self.upper1  = None
        self.lower1  = None
        self.upper2  = None
        self.lower2  = None

    @property
    def is_ready(self) -> bool:
        return self._n >= 3 and self.value is not None
