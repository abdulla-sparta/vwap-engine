# engine/performance.py
#
# Institutional-grade performance metrics for StructureEngine.
# Used by reports.py and the Strategy Report dashboard page.
#
# Functions:
#   calculate_sharpe(daily_pnl, risk_free_rate) → float
#   calculate_max_drawdown(equity_curve)         → dict
#   calculate_win_rate(trades)                   → dict
#   calculate_expectancy(trades)                 → float
#   calculate_risk_adjusted_return(trades)       → float
#   build_equity_curve(trades, starting_balance) → list[dict]
#   full_performance_report(trades, starting_balance, risk_free_rate) → dict

import math
from datetime import datetime, timedelta
from collections import defaultdict


def _net_pnl(t: dict) -> float:
    """Extract net PnL from a trade dict — handles multiple key names."""
    for key in ("net_pnl", "net", "pnl", "profit"):
        if key in t:
            try:
                return float(t[key])
            except (ValueError, TypeError):
                pass
    return 0.0


def _trade_date(t: dict) -> str:
    """Extract date string (YYYY-MM-DD) from a trade dict."""
    for key in ("date", "exit_date", "entry_date", "exit_time", "entry_time"):
        val = t.get(key)
        if val:
            s = str(val)[:10]
            if len(s) == 10 and s[4] == "-":
                return s
    return datetime.now().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# Core metric functions
# ─────────────────────────────────────────────────────────────────────────────

def calculate_win_rate(trades: list[dict]) -> dict:
    """
    Returns:
        wins, losses, total, win_rate (%), avg_win, avg_loss
    """
    if not trades:
        return {"wins": 0, "losses": 0, "total": 0, "win_rate": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0}

    wins, losses = [], []
    for t in trades:
        pnl = _net_pnl(t)
        (wins if pnl > 0 else losses).append(pnl)

    total = len(trades)
    return {
        "wins":      len(wins),
        "losses":    len(losses),
        "total":     total,
        "win_rate":  round(len(wins) / total * 100, 2) if total else 0.0,
        "avg_win":   round(sum(wins)   / len(wins),   2) if wins   else 0.0,
        "avg_loss":  round(sum(losses) / len(losses),  2) if losses else 0.0,
    }


def calculate_expectancy(trades: list[dict]) -> float:
    """
    Expectancy = (Win Rate × Avg Win) − (Loss Rate × |Avg Loss|)
    Positive expectancy = edge exists.
    Returns value in ₹ per trade.
    """
    wr = calculate_win_rate(trades)
    if wr["total"] == 0:
        return 0.0
    win_rate  = wr["win_rate"]  / 100
    loss_rate = 1 - win_rate
    expectancy = (win_rate * wr["avg_win"]) - (loss_rate * abs(wr["avg_loss"]))
    return round(expectancy, 2)


def build_equity_curve(trades: list[dict],
                        starting_balance: float = 200_000.0) -> list[dict]:
    """
    Aggregate trades by date and build a cumulative equity curve.
    Returns list of {date, daily_pnl, equity, drawdown_pct}.
    """
    daily: dict[str, float] = defaultdict(float)
    for t in trades:
        daily[_trade_date(t)] += _net_pnl(t)

    if not daily:
        return []

    equity   = starting_balance
    peak     = starting_balance
    curve    = []

    for date_str in sorted(daily):
        pnl    = daily[date_str]
        equity = round(equity + pnl, 2)
        peak   = max(peak, equity)
        dd_pct = round((equity - peak) / peak * 100, 4) if peak > 0 else 0.0
        curve.append({
            "date":        date_str,
            "daily_pnl":   round(pnl, 2),
            "equity":      equity,
            "drawdown_pct": dd_pct,
        })

    return curve


def calculate_max_drawdown(equity_curve: list[dict]) -> dict:
    """
    Input: output of build_equity_curve().
    Returns:
        max_drawdown_pct, max_drawdown_abs, peak_equity,
        trough_equity, peak_date, trough_date
    """
    if not equity_curve:
        return {"max_drawdown_pct": 0.0, "max_drawdown_abs": 0.0,
                "peak_equity": 0.0, "trough_equity": 0.0,
                "peak_date": None, "trough_date": None}

    peak_eq    = equity_curve[0]["equity"]
    peak_date  = equity_curve[0]["date"]
    max_dd_pct = 0.0
    max_dd_abs = 0.0
    trough_eq  = peak_eq
    trough_dt  = peak_date

    for pt in equity_curve:
        eq = pt["equity"]
        if eq > peak_eq:
            peak_eq   = eq
            peak_date = pt["date"]
        dd_abs = peak_eq - eq
        dd_pct = dd_abs / peak_eq * 100 if peak_eq > 0 else 0.0
        if dd_pct > max_dd_pct:
            max_dd_pct = round(dd_pct, 4)
            max_dd_abs = round(dd_abs, 2)
            trough_eq  = eq
            trough_dt  = pt["date"]

    return {
        "max_drawdown_pct": max_dd_pct,
        "max_drawdown_abs": max_dd_abs,
        "peak_equity":      peak_eq,
        "trough_equity":    trough_eq,
        "peak_date":        peak_date,
        "trough_date":      trough_dt,
    }


def calculate_sharpe(equity_curve: list[dict],
                      risk_free_rate: float = 0.065,
                      trading_days: int = 252) -> float:
    """
    Annualised Sharpe Ratio using daily PnL returns from equity curve.
    risk_free_rate: annual (default 6.5% — RBI repo rate proxy).
    Returns 0.0 if insufficient data (< 2 data points).
    """
    if len(equity_curve) < 2:
        return 0.0

    equities = [pt["equity"] for pt in equity_curve]
    # Percentage daily returns
    returns = [
        (equities[i] - equities[i - 1]) / equities[i - 1]
        for i in range(1, len(equities))
        if equities[i - 1] != 0
    ]

    if len(returns) < 2:
        return 0.0

    n        = len(returns)
    mean_ret = sum(returns) / n
    variance = sum((r - mean_ret) ** 2 for r in returns) / (n - 1)
    std_dev  = math.sqrt(variance) if variance > 0 else 0.0

    if std_dev == 0:
        return 0.0

    daily_rf = risk_free_rate / trading_days
    sharpe   = (mean_ret - daily_rf) / std_dev * math.sqrt(trading_days)
    return round(sharpe, 3)


def calculate_risk_adjusted_return(trades: list[dict],
                                    starting_balance: float = 200_000.0) -> float:
    """
    Net Return % ÷ Max Drawdown %.
    A ratio > 1.0 means you earn more than your worst drawdown — positive edge.
    Returns 0.0 if drawdown is zero.
    """
    curve = build_equity_curve(trades, starting_balance)
    if not curve:
        return 0.0

    net_return_pct = (curve[-1]["equity"] - starting_balance) / starting_balance * 100
    dd             = calculate_max_drawdown(curve)
    max_dd         = dd["max_drawdown_pct"]

    if max_dd == 0:
        return 0.0
    return round(net_return_pct / max_dd, 3)


def calculate_cagr(equity_curve: list[dict],
                    starting_balance: float = 200_000.0) -> float:
    """
    Compound Annual Growth Rate (%) based on first and last equity points.
    """
    if len(equity_curve) < 2:
        return 0.0
    try:
        start_dt = datetime.strptime(equity_curve[0]["date"],  "%Y-%m-%d")
        end_dt   = datetime.strptime(equity_curve[-1]["date"], "%Y-%m-%d")
        days     = (end_dt - start_dt).days
        if days <= 0:
            return 0.0
        end_eq   = equity_curve[-1]["equity"]
        years    = days / 365.25
        cagr     = ((end_eq / starting_balance) ** (1 / years) - 1) * 100
        return round(cagr, 2)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Master report builder
# ─────────────────────────────────────────────────────────────────────────────

def full_performance_report(trades: list[dict],
                              starting_balance: float = 200_000.0,
                              risk_free_rate:   float = 0.065) -> dict:
    """
    Single call that returns every institutional metric + equity curve data.

    Returns:
    {
        summary: { net_pnl, net_return_pct, cagr, starting_balance,
                   ending_balance, trading_days },
        risk: { sharpe_ratio, max_drawdown_pct, max_drawdown_abs,
                risk_adjusted_return, peak_date, trough_date },
        trades: { total, wins, losses, win_rate, avg_win, avg_loss,
                  expectancy },
        equity_curve: [ {date, daily_pnl, equity, drawdown_pct}, ... ],
    }
    """
    curve  = build_equity_curve(trades, starting_balance)
    wr     = calculate_win_rate(trades)
    dd     = calculate_max_drawdown(curve)
    sharpe = calculate_sharpe(curve, risk_free_rate)
    rar    = calculate_risk_adjusted_return(trades, starting_balance)
    cagr   = calculate_cagr(curve, starting_balance)
    exp    = calculate_expectancy(trades)

    ending_balance  = curve[-1]["equity"] if curve else starting_balance
    net_pnl         = round(ending_balance - starting_balance, 2)
    net_return_pct  = round(net_pnl / starting_balance * 100, 2) if starting_balance else 0.0
    trading_days    = len(curve)

    return {
        "summary": {
            "net_pnl":         net_pnl,
            "net_return_pct":  net_return_pct,
            "cagr":            cagr,
            "starting_balance": starting_balance,
            "ending_balance":  ending_balance,
            "trading_days":    trading_days,
        },
        "risk": {
            "sharpe_ratio":         sharpe,
            "max_drawdown_pct":     dd["max_drawdown_pct"],
            "max_drawdown_abs":     dd["max_drawdown_abs"],
            "risk_adjusted_return": rar,
            "peak_date":            dd["peak_date"],
            "trough_date":          dd["trough_date"],
        },
        "trades": {
            "total":       wr["total"],
            "wins":        wr["wins"],
            "losses":      wr["losses"],
            "win_rate":    wr["win_rate"],
            "avg_win":     wr["avg_win"],
            "avg_loss":    wr["avg_loss"],
            "expectancy":  exp,
        },
        "equity_curve": curve,
    }