# backtest_runner.py
#
# Runs backtest for ONE symbol + ONE preset config using VWAP + HTF strategy.
# run_backtest_all() runs symbols sequentially.
#
# Returns per-symbol summary for the results table:
#   symbol, trades, win_rate, gross_pnl, charges, net_pnl, return_pct, balance

import os
from config import CONFIG, INSTRUMENTS
from broker.paper_broker import PaperBroker
from engine.trade_engine import TradeEngine
from engine.htf_structure import HTFStructure
from engine.vwap import VWAP
from engine.vwap_entry import VWAPEntry
from data.loader import load_csv
from data.resampler import resample


def run_backtest(symbol: str, preset: dict) -> dict:
    """
    Run backtest for one symbol with one preset config.
    Thread-safe: uses only local variables, never touches global CONFIG.
    """
    data_dir = os.environ.get(
        "CSV_DATA_DIR",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "csvdata")
    )
    csv_path = None
    for suffix in ["1YEAR_1MIN", "6MONTH_1MIN"]:
        candidate = os.path.join(data_dir, f"{symbol}_{suffix}.csv")
        if os.path.exists(candidate):
            csv_path = candidate
            break
    if csv_path is None:
        return {
            "symbol":     symbol,
            "error":      "CSV not found",
            "return_pct": -9999,
        }

    def _resolve(key, default):
        if key in preset and preset[key] is not None:
            return preset[key]
        return CONFIG.get(key, default)

    rr        = float(_resolve("rr_target",          3))
    cooldown  = int(  _resolve("cooldown",            20))
    min_dist  = float(_resolve("min_price_distance",  5))
    risk      = float(_resolve("risk_per_trade",      0.01))
    band_conf = bool( _resolve("band_confluence",     True))
    balance     = float(_resolve("starting_balance",    200000))
    htf_min     = int(  _resolve("htf_minutes",         15))
    ltf_min     = int(  _resolve("ltf_minutes",         5))
    swing_look  = int(  _resolve("swing_lookback",       20))

    df1m = load_csv(csv_path)
    df_htf = resample(df1m, htf_min)
    df_ltf = resample(df1m, ltf_min)

    broker = PaperBroker(balance=balance, risk_per_trade=risk, symbol=symbol)
    engine = TradeEngine(
        broker,
        cooldown_candles=cooldown,
        min_price_distance=min_dist,
        symbol=symbol,
        backtest_rr=rr,
    )
    engine._backtest_mode = True   # suppress per-candle HTF bias logs
    # pivot_left/right derived from swing_lookback (half-window each side)
    pivot_side = max(2, swing_look // 2)
    engine.attach(
        HTFStructure(pivot_left=pivot_side, pivot_right=pivot_side),
        VWAP(),
        VWAPEntry(rr_target=rr, band_confluence=band_conf),
    )

    # Interleaved replay: feed HTF and LTF candles in time order
    htf_list = list(df_htf.iterrows())
    htf_i    = 0

    for ltf_ts, ltf_row in df_ltf.iterrows():
        # Feed any HTF candles that closed at or before this LTF candle
        while htf_i < len(htf_list) and htf_list[htf_i][0] <= ltf_ts:
            ts, row = htf_list[htf_i]
            engine.on_htf_candle(row)
            htf_i += 1
        engine.on_ltf_candle(ltf_row)

    # Build summary
    b      = broker
    trades = b.trade_log
    total  = len(trades)
    wins   = sum(1 for t in trades if t["net_pnl"] > 0)
    gross  = sum(t.get("gross_pnl", 0) for t in trades)
    chgs   = sum(t.get("charges",   0) for t in trades)
    net    = sum(t.get("net_pnl",   0) for t in trades)

    # Sanitise trade_log for JSON serialisation (convert Timestamps → ISO str)
    def _clean(t: dict) -> dict:
        out = {}
        for k, v in t.items():
            try:
                import pandas as pd
                if isinstance(v, pd.Timestamp):
                    out[k] = v.isoformat()
                    continue
            except Exception:
                pass
            out[k] = v
        return out

    clean_log = [_clean(t) for t in trades]

    return {
        "symbol":     symbol,
        "trades":     total,
        "wins":       wins,
        "win_rate":   round(wins / total * 100, 1) if total else 0,
        "gross_pnl":  round(gross, 2),
        "charges":    round(chgs, 2),
        "net_pnl":    round(net, 2),
        "return_pct": round(net / balance * 100, 2) if balance else 0,
        "balance":    round(b.balance, 2),
        "rr":         rr,
        "band_confluence": band_conf,
        "trade_log":  clean_log,
    }


def run_backtest_all(preset: dict) -> list[dict]:
    """
    Run backtest for all instruments in parallel (ThreadPoolExecutor).
    Returns results sorted by return_pct descending.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    max_workers = min(8, len(INSTRUMENTS))   # cap at 8 threads — Railway has limited cores
    results     = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(run_backtest, inst["symbol"], preset): inst["symbol"]
            for inst in INSTRUMENTS
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                r = fut.result(timeout=120)   # 2-min per-symbol hard timeout
            except Exception as e:
                r = {"symbol": sym, "error": str(e), "return_pct": -9999}
            results.append(r)
            print(f"[BT] {sym} done — {len(results)}/{len(INSTRUMENTS)}")

    results.sort(key=lambda x: x.get("return_pct", -9999), reverse=True)
    return results
