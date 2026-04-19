# data/loader.py
#
# Loads 1-minute OHLCV CSV for backtest.
# IMPORTANT: volume column is required — VWAP calculation uses it.

import pandas as pd
import os


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a 1-minute OHLCV CSV.
    Expects columns: datetime/time/date, open, high, low, close, volume.
    Sets DatetimeIndex, returns [open, high, low, close, volume].
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # Find datetime column
    time_col = next(
        (c for c in df.columns if c in ("datetime", "time", "date", "timestamp")),
        next((c for c in df.columns if "time" in c or "date" in c), None),
    )
    if time_col is None:
        raise ValueError(f"No datetime column found in CSV: {path}")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df.dropna(subset=[time_col], inplace=True)   # drop NaT rows
    df.set_index(time_col, inplace=True)
    df = df[~df.index.duplicated(keep="last")]    # drop duplicate timestamps
    df.sort_index(inplace=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in {path}")

    # Volume is required for VWAP. Warn if missing.
    if "volume" not in df.columns:
        print(f"  WARNING: no 'volume' column in {path} — VWAP bands will be inaccurate")
        df["volume"] = 1

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["open", "high", "low", "close"], inplace=True)

    return df[["open", "high", "low", "close", "volume"]]