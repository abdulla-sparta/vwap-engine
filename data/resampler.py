# data/resampler.py
#
# Resamples 1-minute OHLCV to N-minute candles.
# Volume is summed per candle — required for VWAP calculation.
#
# NSE session: 09:15–15:30 IST
# 5m  → 75 candles/day  (09:15–15:25)
# 15m → 25 candles/day  (09:15–15:15)

import pandas as pd

NSE_OPEN  = "09:15"
NSE_CLOSE = "15:30"


def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Resample 1-minute OHLCV to N-minute candles, market hours only.

    Parameters
    ----------
    df      : DataFrame with DatetimeIndex, columns open/high/low/close/volume
    minutes : target candle size (5, 15, 30, etc.)

    Returns
    -------
    DataFrame with columns open/high/low/close/volume, N-minute candles.
    """
    # 1. Market hours only (strips pre/post market rows)
    df_mkt = df.between_time(NSE_OPEN, NSE_CLOSE).copy()

    # 2. Build agg dict — include volume if present
    agg = {
        "open":  "first",
        "high":  "max",
        "low":   "min",
        "close": "last",
    }
    if "volume" in df_mkt.columns:
        agg["volume"] = "sum"

    # 3. Resample anchored to start of day (gives 09:15, 09:20 … grid)
    resampled = (
        df_mkt
        .resample(f"{minutes}min", origin="start_day")
        .agg(agg)
        .dropna(subset=["open", "close"])
    )

    # 4. Cap at 15:25 — drop any partial candle at 15:30
    resampled = resampled[resampled.index.time <= pd.Timestamp("15:25").time()]

    # 5. Fill zero-volume candles (shouldn't happen but safety guard)
    if "volume" in resampled.columns:
        resampled["volume"] = resampled["volume"].fillna(0)

    return resampled