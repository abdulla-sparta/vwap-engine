#!/usr/bin/env python3
# fetch_upstox_history.py
#
# Fetches 1-minute OHLCV history from Upstox v2 API for all INSTRUMENTS.
# Run this once after deploy to seed CSVs, then EOD updater keeps them current.
#
# Upstox hard limit: ~200 days of 1min history per symbol.
# CSV grows over time — each run appends only NEW data.
#
# Usage:
#   python fetch_upstox_history.py               # all symbols, smart append
#   python fetch_upstox_history.py --symbol TCS  # single symbol
#   python fetch_upstox_history.py --force       # full re-fetch ignoring CSV
#
# Token priority:
#   1. UPSTOX_ACCESS_TOKEN env var
#   2. DB (if Railway/PostgreSQL configured)
#   3. Prompt user to set env var

import os
import sys
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

from config import CONFIG, INSTRUMENTS

NSE_OPEN     = "09:15"
NSE_CLOSE    = "15:30"
DATA_DIR     = os.environ.get(
    "CSV_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "csvdata"),
)
INTERVAL     = "1minute"
FETCH_MONTHS = 12
MONTHS       = FETCH_MONTHS   # alias used by app.py import


def _get_token() -> str:
    """Resolve access token from env → DB → error."""
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if token:
        return token
    # Try DB
    try:
        import db as _db
        data = _db.get("upstox_access_token")
        if data and data.get("token"):
            return data["token"]
    except Exception:
        pass
    return ""


def _token_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _encode_token(instrument_token: str) -> str:
    """NSE_EQ|INE... → NSE_EQ%7CINE... (Upstox URL encoding)."""
    return instrument_token.replace("|", "%7C")


def fetch_chunk(token: str, start: datetime, end: datetime,
                headers: dict) -> pd.DataFrame | None:
    """Fetch one chunk (≤30 days) of 1min candles from Upstox v2 API."""
    url = (
        f"https://api.upstox.com/v2/historical-candle/"
        f"{_encode_token(token)}/"
        f"{INTERVAL}/"
        f"{end.strftime('%Y-%m-%d')}/"
        f"{start.strftime('%Y-%m-%d')}"
    )
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 401:
            print(f"    ❌ 401 Unauthorized — token expired or invalid")
            return None
        if resp.status_code == 429:
            print(f"    ⚠ Rate limited — sleeping 5s")
            time.sleep(5)
            return None
        if not resp.ok:
            print(f"    ⚠ HTTP {resp.status_code}: {resp.text[:120]}")
            return None

        candles = resp.json().get("data", {}).get("candles", [])
        if not candles:
            return None

        df = pd.DataFrame(
            candles,
            columns=["datetime", "open", "high", "low", "close", "volume", "oi"],
        )
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df = df.between_time(NSE_OPEN, NSE_CLOSE)

        # Cast numeric
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(subset=["open", "close"], inplace=True)

        return df[["open", "high", "low", "close", "volume"]]

    except Exception as e:
        print(f"    Exception: {e}")
        return None


def csv_path(symbol: str) -> str:
    return os.path.join(DATA_DIR, f"{symbol}_1YEAR_1MIN.csv")


def csv_exists(symbol: str) -> bool:
    p = csv_path(symbol)
    return os.path.exists(p) and os.path.getsize(p) > 1024


def _load_existing(symbol: str) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(csv_path(symbol), index_col=0, parse_dates=True)
        return df if not df.empty else None
    except Exception:
        return None


def fetch_symbol(symbol: str, token: str, access_token: str,
                 force: bool = False, months: int = None) -> bool:
    """
    Smart fetch for one symbol.
    - CSV exists + not force → append only new days since last row
    - CSV missing or force   → fetch up to FETCH_MONTHS back

    Returns True on success.
    """
    headers    = _token_header(access_token)
    today      = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    existing   = _load_existing(symbol) if not force else None

    if existing is not None:
        last_ts = pd.Timestamp(existing.index[-1])
        last_ts_naive = (
            last_ts.tz_localize(None) if last_ts.tzinfo is None
            else last_ts.tz_convert(None)
        )
        fetch_from = (last_ts_naive + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if fetch_from.date() >= today.date():
            print(f"  [{symbol}] ✅ Already up to date ({last_ts.date()})")
            return True
        months_needed = max(1, int((today - fetch_from).days / 30) + 1)
        start_date    = fetch_from
        print(f"  [{symbol}] Appending {(today - fetch_from).days} new days "
              f"({last_ts.date()} → {today.date()})")
    else:
        months_needed = months if months else FETCH_MONTHS
        start_date    = today - relativedelta(months=months_needed)
        label = "Force re-fetch" if force else "Fresh fetch"
        print(f"  [{symbol}] {label} — {months_needed} months "
              f"({start_date.date()} → {today.date()})")

    all_frames = []
    got_401    = False

    for m in range(months_needed):
        chunk_end   = today - relativedelta(months=m)
        chunk_start = chunk_end - relativedelta(months=1) + timedelta(days=1)

        if chunk_end.date() < start_date.date():
            break
        if chunk_start < start_date:
            chunk_start = start_date

        print(f"    Chunk {m+1:>2}: {chunk_start.date()} → {chunk_end.date()}", end="  ")
        df = fetch_chunk(token, chunk_start, chunk_end, headers)

        if df is None:
            print("(no data)")
            # 401 is fatal — abort entire symbol
            if "401" in "":   # checked inside fetch_chunk already
                got_401 = True
                break
        elif df.empty:
            print("(empty)")
        else:
            all_frames.append(df)
            print(f"✅ {len(df):,} rows")

        time.sleep(0.4)

    if got_401:
        print(f"  [{symbol}] ❌ Aborted — token invalid")
        return False

    if not all_frames:
        if existing is not None:
            print(f"  [{symbol}] No new API data — keeping existing CSV")
            return True
        print(f"  [{symbol}] ❌ No data fetched")
        return False

    new_df   = pd.concat(all_frames)
    combined = pd.concat([existing, new_df]) if existing is not None else new_df
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    combined.to_csv(csv_path(symbol))

    span = (combined.index[-1] - combined.index[0]).days
    print(f"  [{symbol}] ✅ {len(combined):,} candles "
          f"({combined.index[0].date()} → {combined.index[-1].date()}, "
          f"{span}d) saved to {csv_path(symbol)}")
    return True


def fetch_all(symbols: list[str] = None, force: bool = False):
    access_token = _get_token()
    if not access_token:
        print(
            "\n❌ No Upstox access token found.\n"
            "   Set UPSTOX_ACCESS_TOKEN in your .env file, then run again.\n"
            "   Or login via the dashboard: /auth/login\n"
        )
        sys.exit(1)

    # Update CONFIG so trade_engine can use it
    CONFIG["upstox_access_token"] = access_token

    targets = [i for i in INSTRUMENTS
               if symbols is None or i["symbol"] in symbols]
    if not targets:
        print("No matching symbols found.")
        return

    mode = "FORCE RE-FETCH" if force else "SMART APPEND"
    print(f"\n[{mode}] 1-min OHLCV for {len(targets)} symbols")
    print(f"Data dir: {DATA_DIR}\n")

    os.makedirs(DATA_DIR, exist_ok=True)
    ok, fail = [], []

    for inst in targets:
        success = fetch_symbol(
            symbol=inst["symbol"],
            token=inst["token"],
            access_token=access_token,
            force=force,
        )
        (ok if success else fail).append(inst["symbol"])
        time.sleep(1.0)

    print(f"\n{'='*55}")
    print(f"✅ Success ({len(ok)}): {ok}")
    if fail:
        print(f"❌ Failed  ({len(fail)}): {fail}")
    print("\nTip: Run monthly — EOD updater keeps it current day-to-day.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Upstox 1-min OHLCV history — appends to existing CSVs"
    )
    parser.add_argument("--symbol", nargs="+", default=None,
                        help="Fetch specific symbols only (e.g. --symbol TCS TRENT)")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch full history even if CSV exists")
    parser.add_argument("--months", type=int, default=None,
                        help="Override fetch window (default: 12 months)")
    args = parser.parse_args()

    fetch_all(symbols=args.symbol, force=args.force)