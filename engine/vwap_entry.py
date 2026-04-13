# engine/vwap_entry.py
#
# VWAP + HTF Bias Confluence Entry
#
# ── LOGIC ──────────────────────────────────────────────────────────────────
#
# Entry only fires when BOTH conditions align:
#   1. HTF bias (15m BOS) is BULLISH or BEARISH
#   2. Price interacts with VWAP in the direction of that bias
#
# LONG entry (BULLISH bias):
#   - Previous candle closed BELOW VWAP (pullback to VWAP)
#   - Current candle closes ABOVE VWAP (reclaim)
#   → Entry = close, Stop = lower1 band (1σ below VWAP), Target = entry + rr × risk
#
# SHORT entry (BEARISH bias):
#   - Previous candle closed ABOVE VWAP (bounce to VWAP)
#   - Current candle closes BELOW VWAP (rejection)
#   → Entry = close, Stop = upper1 band (1σ above VWAP), Target = entry - rr × risk
#
# Additional confluence filter (optional, enabled by default):
#   LONG:  price must be below upper2 (not already extended above 2σ)
#   SHORT: price must be above lower2 (not already extended below 2σ)
#
# ── STOP DISTANCE ──────────────────────────────────────────────────────────
# Stop is placed at the 1σ band, not a fixed %. If 1σ < MIN_STOP_PCT,
# we widen to MIN_STOP_PCT of entry price.

MIN_STOP_PCT = 0.003   # 0.3 % — same as structure engine


class VWAPEntry:

    def __init__(self, rr_target: float = None, min_stop_pct: float = MIN_STOP_PCT,
                 band_confluence: bool = True):
        self._rr           = rr_target
        self._min_stop_pct = min_stop_pct
        self._band_conf    = band_confluence   # reject trades outside 2σ bands
        self.prev_candle   = None
        self.prev_vwap     = None   # VWAP value from previous candle

    def set_rr(self, rr: float):
        self._rr = rr

    def _rr_target(self) -> float:
        if self._rr is not None:
            return self._rr
        from config import CONFIG
        return CONFIG["rr_target"]

    def update(self, candle, bias, vwap_data: dict) -> dict | None:
        """
        Call on each closed LTF candle.

        candle    : pd.Series with open/high/low/close
        bias      : "BULLISH" | "BEARISH" | None  (from HTFStructure)
        vwap_data : dict returned by VWAP.update()  {vwap, upper1, lower1, upper2, lower2}

        Returns signal dict or None.
        """
        if bias is None or self.prev_candle is None or vwap_data is None:
            self.prev_candle = candle
            self.prev_vwap   = vwap_data
            return None

        if not self.prev_vwap:
            self.prev_candle = candle
            self.prev_vwap   = vwap_data
            return None

        close      = float(candle["close"])
        prev_close = float(self.prev_candle["close"])
        vwap       = vwap_data["vwap"]
        upper1     = vwap_data["upper1"]
        lower1     = vwap_data["lower1"]
        upper2     = vwap_data["upper2"]
        lower2     = vwap_data["lower2"]
        rr         = self._rr_target()
        signal     = None

        if bias == "BULLISH":
            # Pullback to VWAP then reclaim: prev close below VWAP, current close above
            if prev_close < vwap and close > vwap:
                # Confluence: not extended above 2σ
                if not self._band_conf or close < upper2:
                    entry    = close
                    stop_raw = lower1
                    risk_raw = entry - stop_raw

                    # Enforce minimum stop
                    min_risk = entry * self._min_stop_pct
                    if risk_raw < min_risk:
                        risk_raw = min_risk
                        stop_raw = round(entry - risk_raw, 2)

                    if risk_raw > 0:
                        signal = {
                            "side":   "BUY",
                            "entry":  round(entry, 2),
                            "stop":   round(stop_raw, 2),
                            "target": round(entry + risk_raw * rr, 2),
                            "vwap":   vwap,
                            "band":   lower1,
                        }

        elif bias == "BEARISH":
            # Bounce to VWAP then rejection: prev close above VWAP, current close below
            if prev_close > vwap and close < vwap:
                # Confluence: not extended below 2σ
                if not self._band_conf or close > lower2:
                    entry    = close
                    stop_raw = upper1
                    risk_raw = stop_raw - entry

                    # Enforce minimum stop
                    min_risk = entry * self._min_stop_pct
                    if risk_raw < min_risk:
                        risk_raw = min_risk
                        stop_raw = round(entry + risk_raw, 2)

                    if risk_raw > 0:
                        signal = {
                            "side":   "SELL",
                            "entry":  round(entry, 2),
                            "stop":   round(stop_raw, 2),
                            "target": round(entry - risk_raw * rr, 2),
                            "vwap":   vwap,
                            "band":   upper1,
                        }

        self.prev_candle = candle
        self.prev_vwap   = vwap_data
        return signal

    def reset(self):
        self.prev_candle = None
        self.prev_vwap   = None
