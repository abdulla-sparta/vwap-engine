# VWAP + HTF Confluence Engine

Paper trading engine that combines **anchored intraday VWAP** with **Higher Timeframe structural bias** (Break of Structure on 15m) to generate high-confluence trade signals.

## Strategy Logic

### HTF Bias (15m Break of Structure)
- Scans 15m candles for confirmed swing highs/lows (pivot_left=3, pivot_right=3)
- BULLISH bias: close breaks above a confirmed swing high
- BEARISH bias: close breaks below a confirmed swing low

### VWAP Entry (5m)
- VWAP resets daily at 09:15 IST (anchored session VWAP)
- Bands: ±1σ and ±2σ computed from intraday typical price × volume
- **LONG** (BULLISH bias): prev 5m close below VWAP → current close above VWAP (reclaim)
- **SHORT** (BEARISH bias): prev 5m close above VWAP → current close below VWAP (rejection)
- Stop: placed at VWAP ±1σ band
- Band confluence filter: entry rejected if price is already outside 2σ

## Key Differences from Structure Engine
| Feature | Structure Engine | VWAP+HTF Engine |
|---|---|---|
| Entry trigger | LTF candle breaks prev candle high/low | Price reclaims/rejects VWAP |
| Stop placement | Prior candle extreme | VWAP ±1σ band |
| VWAP | Not used | Anchored daily VWAP |
| Confluence filter | None | Must be inside 2σ bands |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in UPSTOX_API_KEY, UPSTOX_API_SECRET, UPSTOX_REDIRECT_URI
python app.py
```

Then visit `http://localhost:5001/auth/login` to authenticate with Upstox.

## Railway Deployment
- Uses the same Railway + PostgreSQL setup as the structure engine
- Cron jobs in railway.toml auto-start at 09:00 IST and stop at 15:31 IST
