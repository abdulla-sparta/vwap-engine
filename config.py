# config.py — VWAP + HTF Confluence Engine
# All secrets from .env, never hardcoded

import os
from dotenv import load_dotenv

load_dotenv()

# ── INSTRUMENTS ───────────────────────────────────────────────────────────────
INSTRUMENTS = [
    {"symbol": "TRENT",      "token": "NSE_EQ|INE849A01020", "margin_pct": 0.20},
    {"symbol": "TITAN",      "token": "NSE_EQ|INE280A01028", "margin_pct": 0.20},
    {"symbol": "OFSS",       "token": "NSE_EQ|INE881D01027", "margin_pct": 0.20},
    {"symbol": "PERSISTENT", "token": "NSE_EQ|INE262H01021", "margin_pct": 0.20},
    {"symbol": "MPHASIS",    "token": "NSE_EQ|INE356A01018", "margin_pct": 0.20},
    {"symbol": "PIDILITIND", "token": "NSE_EQ|INE318A01026", "margin_pct": 0.20},
    {"symbol": "CUMMINSIND", "token": "NSE_EQ|INE298A01020", "margin_pct": 0.20},
    {"symbol": "BAJAJ-AUTO", "token": "NSE_EQ|INE917I01010", "margin_pct": 0.20},
    {"symbol": "APOLLOHOSP", "token": "NSE_EQ|INE437A01024", "margin_pct": 0.20},
    {"symbol": "CIPLA",      "token": "NSE_EQ|INE059A01026", "margin_pct": 0.20},
    {"symbol": "ASIANPAINT", "token": "NSE_EQ|INE021A01026", "margin_pct": 0.20},
    {"symbol": "LT",         "token": "NSE_EQ|INE018A01030", "margin_pct": 0.20},
    {"symbol": "VEDL",       "token": "NSE_EQ|INE205A01025", "margin_pct": 0.20},
    {"symbol": "TVSMOTOR",   "token": "NSE_EQ|INE494B01023", "margin_pct": 0.20},
    {"symbol": "DIVISLAB",   "token": "NSE_EQ|INE361B01024", "margin_pct": 0.20},
    {"symbol": "AUROPHARMA", "token": "NSE_EQ|INE406A01037", "margin_pct": 0.20},
    {"symbol": "SUNPHARMA",  "token": "NSE_EQ|INE044A01036", "margin_pct": 0.20},
    {"symbol": "MAXHEALTH",  "token": "NSE_EQ|INE027H01010", "margin_pct": 0.20},
    {"symbol": "ALKEM",      "token": "NSE_EQ|INE540L01014", "margin_pct": 0.20},
    {"symbol": "WIPRO",      "token": "NSE_EQ|INE075A01022", "margin_pct": 0.20},
    {"symbol": "INFY",       "token": "NSE_EQ|INE009A01021", "margin_pct": 0.20},
    {"symbol": "M&M",        "token": "NSE_EQ|INE101A01026", "margin_pct": 0.20},
    {"symbol": "MARUTI",     "token": "NSE_EQ|INE585B01010", "margin_pct": 0.20},
    {"symbol": "BOSCHLTD",   "token": "NSE_EQ|INE323A01026", "margin_pct": 0.20},
    {"symbol": "RELIANCE",   "token": "NSE_EQ|INE002A01018", "margin_pct": 0.20},
    {"symbol": "HCLTECH",    "token": "NSE_EQ|INE860A01027", "margin_pct": 0.20},
    {"symbol": "TCS",        "token": "NSE_EQ|INE467B01029", "margin_pct": 0.20},
    {"symbol": "BHARTIARTL", "token": "NSE_EQ|INE397D01024", "margin_pct": 0.20},
]

MAX_SYMBOLS = 50
MARGIN_TABLE = {i["symbol"]: i["margin_pct"] for i in INSTRUMENTS}

CONFIG = {

    # ── MODE ──────────────────────────────────────────────────────────────────
    "mode":          "backtest",
    "backtest_mode": True,

    # ── ACCOUNT ───────────────────────────────────────────────────────────────
    "starting_balance": 200000,

    # ── QTY SIZING ────────────────────────────────────────────────────────────
    "risk_per_trade": 0.01,       # 1 % of balance per trade

    # ── STRATEGY ──────────────────────────────────────────────────────────────
    "rr_target":          3,
    "cooldown":           20,     # 5m candles between entries
    "min_price_distance": 5,
    "band_confluence":    True,   # reject entries outside 2σ bands
    "entry_debug_logs":   False,  # print per-candle "why no entry" diagnostics

    # ── TIMEFRAMES ────────────────────────────────────────────────────────────
    "htf_minutes":    15,         # HTF for BOS bias
    "ltf_minutes":     5,         # LTF for VWAP entry
    "swing_lookback": 20,         # HTFStructure pivot_left / pivot_right

    # ── SESSION (IST) ─────────────────────────────────────────────────────────
    "entry_start_time": "09:30",  # give VWAP a few candles to anchor first
    "entry_end_time":   "14:30",
    "force_exit_time":  "15:25",

    # ── TRAILING KILL SWITCH (live only) ──────────────────────────────────────
    "kill_switch_enabled": True,
    "kill_switch_percent": 0.90,

    # ── PORTFOLIO DAILY LOSS LIMIT ────────────────────────────────────────────
    "portfolio_daily_loss_limit": 50000,

    # ── CREDENTIALS (from .env) ───────────────────────────────────────────────
    "upstox_access_token": os.getenv("UPSTOX_ACCESS_TOKEN", ""),
    "upstox_api_key":      os.getenv("UPSTOX_API_KEY", ""),
    "upstox_ws_url":       os.getenv("UPSTOX_WS_URL",
                               "wss://api.upstox.com/v3/feed/market-data-feed"),

    # ── TELEGRAM (from .env) ──────────────────────────────────────────────────
    "telegram_enabled":   True,
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":   os.getenv("TELEGRAM_CHAT_ID", ""),
}
