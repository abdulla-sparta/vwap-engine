# app.py — VWAP + HTF Confluence Engine

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import threading
import json
import requests as _requests
from flask import Flask, render_template, request, jsonify, redirect, send_from_directory
from flask_socketio import SocketIO, emit

import db
from config import CONFIG, INSTRUMENTS
from backtest_runner import run_backtest_all, run_backtest

app = Flask(__name__)
app.config["SECRET_KEY"] = "vwap-htf-engine-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

live_runner    = None
_signal_log    = []   # in-memory signal cache (last 200)
_runner_lock   = threading.Lock()

# ── Backtest progress ──────────────────────────────────────────────────────────
_bt_progress = {
    "running": False, "config": "", "total": 0, "done": 0, "symbols": {},
}

_fetch_status = {
    "running": False, "done": True, "total": 0, "done_count": 0,
    "results": {}, "errors": {}
}


# ── JSON sanitiser (strips pandas Timestamps) ─────────────────────────────────
def _sanitize(obj):
    import pandas as pd
    from datetime import datetime, date
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(i) for i in obj]
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, (datetime, date)):
        return str(obj)
    return obj


# ── Live runner helpers ────────────────────────────────────────────────────────

def _start_live_runner():
    global live_runner
    with _runner_lock:
        if live_runner is not None:
            if getattr(live_runner, "_replaying", False):
                return
            if not live_runner.is_connected():
                try:
                    live_runner.stop()
                except Exception:
                    pass
                live_runner = None
            else:
                return

        # Wire SocketIO emitter into instrument_runner
        try:
            import live_engine.instrument_runner as _ir
            def _emitter(event, data, **kw):
                if event == "signal_fired":
                    _signal_log.append(data)
                    if len(_signal_log) > 200:
                        _signal_log.pop(0)
                socketio.emit(event, data, **kw)
            _ir.set_socketio_emitter(_emitter)
        except Exception as e:
            print(f"[App] emitter wire failed: {e}")

        from live_engine.multi_strategy_runner import MultiStrategyRunner
        live_runner = MultiStrategyRunner()
        live_runner._replaying = True

        def _start_and_signal():
            try:
                live_runner.start()
            finally:
                live_runner._replaying = False

        threading.Thread(target=_start_and_signal, daemon=True).start()

        def push_loop():
            import time
            _signalled = False
            while True:
                time.sleep(0.5)
                try:
                    runner = live_runner
                    if runner is None:
                        _signalled = False
                        socketio.emit("live_update", {"connected": False, "instruments": [], "portfolio_equity": {}})
                        continue
                    instruments = _sanitize(runner.get_all_status())
                    portfolio   = runner.get_portfolio_equity()
                    connected   = runner.is_connected()
                    if not _signalled and connected:
                        _signalled = True
                        socketio.emit("engine_started", {"status": "ok", "instruments": instruments})
                    socketio.emit("live_update", {
                        "connected":        connected,
                        "portfolio_equity": portfolio,
                        "instruments":      instruments,
                    })
                except Exception:
                    pass

        if not getattr(_start_live_runner, "_push_running", False):
            _start_live_runner._push_running = True
            threading.Thread(target=push_loop, daemon=True, name="push_loop").start()


# ── Startup ───────────────────────────────────────────────────────────────────

def _on_startup():
    os.makedirs("data",  exist_ok=True)
    os.makedirs("state", exist_ok=True)
    db.init_db()

    try:
        from upstox_auth import restore_token_on_startup, start_daily_token_scheduler
        restore_token_on_startup()
        start_daily_token_scheduler()
    except Exception as e:
        print(f"⚠ Auth module error: {e}")

    try:
        from eod_updater import start_eod_scheduler
        start_eod_scheduler()
    except Exception as e:
        print(f"⚠ EOD updater: {e}")

    try:
        from margin_fetcher import fetch_margins
        threading.Thread(target=fetch_margins, daemon=True).start()
    except Exception as e:
        print(f"⚠ Margin fetch: {e}")

    try:
        import heatmap_feed as _hf
        _hf.set_socketio_emitter(socketio.emit)
        from heatmap_feed import start_heatmap_feed
        start_heatmap_feed()
    except Exception as e:
        print(f"⚠ Heatmap feed: {e}")

    # Restore last backtest from DB
    try:
        bt_meta = db.get("bt_last_run_meta") or {}
        if bt_meta:
            summary = bt_meta.get("summary", {})
            _bt_progress.update({
                "running": False, "config": bt_meta.get("preset", ""),
                "total": len(summary), "done": len(summary),
                "symbols": {sym: {"status": "ok", **v} for sym, v in summary.items()},
            })
    except Exception:
        pass

    print("[App] VWAP+HTF Engine ready")


# ── Static / PWA ──────────────────────────────────────────────────────────────

@app.route("/sw.js")
def service_worker():
    return send_from_directory("templates", "sw.js", mimetype="application/javascript")


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route("/auth/login")
def auth_login():
    try:
        from upstox_auth import get_login_url
        url = get_login_url()
        return redirect(url)
    except Exception as e:
        return f"Auth error: {e}", 500

@app.route("/auth/callback")
def auth_callback():
    code = request.args.get("code", "")
    if not code:
        return "No auth code", 400
    try:
        from upstox_auth import exchange_code_for_token, save_token
        token = exchange_code_for_token(code)
        if not token:
            return "Token exchange failed", 500
        save_token(token)
        return redirect("/live")
    except Exception as e:
        return f"Token exchange failed: {e}", 500

@app.route("/auth/status")
def auth_status():
    token = CONFIG.get("upstox_access_token", "") or os.getenv("UPSTOX_ACCESS_TOKEN", "")
    return jsonify({"authenticated": bool(token), "token_preview": token[:8] + "…" if token else ""})


# ── Main pages ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", instruments=INSTRUMENTS, config=CONFIG)

@app.route("/live")
def live_dashboard():
    return render_template("live.html")

@app.route("/signals")
def signals_page():
    return render_template("signals.html")

@app.route("/trade_log")
def trade_log_page():
    return render_template("trade_log.html")


# ── History fetch API (Backtest step 2) ───────────────────────────────────────

def _resolve_upstox_token() -> str:
    from dotenv import load_dotenv
    load_dotenv(override=True)
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "") or CONFIG.get("upstox_access_token", "")
    if token:
        return token
    try:
        from upstox_auth import load_token_from_db
        token = load_token_from_db() or ""
        if token:
            CONFIG["upstox_access_token"] = token
        return token
    except Exception:
        return ""


@app.route("/refetch_all", methods=["POST"])
def refetch_all_route():
    global _fetch_status
    payload = request.get_json(silent=True) or {}
    symbols = payload.get("symbols") or [i["symbol"] for i in INSTRUMENTS]

    token = _resolve_upstox_token()
    if not token:
        _fetch_status = {
            "running": False, "done": True, "total": len(symbols), "done_count": 0,
            "results": {s: "failed" for s in symbols},
            "errors": {s: "No Upstox token" for s in symbols},
        }
        return jsonify({"status": "error", "reason": "no_token"}), 400

    _fetch_status = {
        "running": True, "done": False, "total": len(symbols), "done_count": 0,
        "results": {s: "fetching" for s in symbols},
        "errors": {},
    }

    def _worker(target_symbols: list[str], access_token: str):
        global _fetch_status
        from fetch_upstox_history import fetch_symbol

        inst_by_symbol = {i["symbol"]: i for i in INSTRUMENTS}
        for sym in target_symbols:
            inst = inst_by_symbol.get(sym)
            if not inst:
                _fetch_status["results"][sym] = "failed"
                _fetch_status["errors"][sym] = "Unknown symbol"
                _fetch_status["done_count"] += 1
                continue
            try:
                ok = fetch_symbol(sym, inst["token"], access_token, force=False)
                _fetch_status["results"][sym] = "ok" if ok else "failed"
            except Exception as e:
                _fetch_status["results"][sym] = "failed"
                _fetch_status["errors"][sym] = str(e)
            _fetch_status["done_count"] += 1

        _fetch_status["running"] = False
        _fetch_status["done"] = True

    threading.Thread(target=_worker, args=(symbols, token), daemon=True, name="history_fetch").start()
    return jsonify({"status": "started", "total": len(symbols)})


@app.route("/fetch_status")
def fetch_status_route():
    status = dict(_fetch_status)
    if "results" not in status:
        symbols = status.get("symbols", {})
        status["results"] = {k: (v.get("status") if isinstance(v, dict) else str(v)) for k, v in symbols.items()}
    status.setdefault("errors", {})
    return jsonify(status)


# ── Backtest API ──────────────────────────────────────────────────────────────

@app.route("/run_backtest", methods=["POST"])
def api_run_backtest():
    global _bt_progress
    data = request.json or {}

    preset = {
        "name":               data.get("name", "Custom"),
        "risk_per_trade":     float(data.get("risk_per_trade",     CONFIG["risk_per_trade"])),
        "rr_target":          float(data.get("rr_target",          CONFIG["rr_target"])),
        "cooldown":           int(  data.get("cooldown",           CONFIG["cooldown"])),
        "min_price_distance": float(data.get("min_price_distance", CONFIG["min_price_distance"])),
        "band_confluence":    bool( data.get("band_confluence",    CONFIG.get("band_confluence", True))),
    }

    symbols = data.get("symbols") or [i["symbol"] for i in INSTRUMENTS]
    _bt_progress.update({
        "running": True, "config": "Custom",
        "total": len(symbols), "done": 0,
        "symbols": {s: {"status": "pending"} for s in symbols},
    })
    socketio.emit("bt_progress", _bt_progress)

    def _run():
        global _bt_progress
        results = []
        for i, sym in enumerate(symbols):
            r = run_backtest(sym, preset)
            results.append(r)
            _bt_progress["done"] = i + 1
            _bt_progress["symbols"][sym] = {
                "status":     "ok" if not r.get("error") else "error",
                "return_pct": r.get("return_pct"),
                "net_pnl":    r.get("net_pnl"),
                "trades":     r.get("trades"),
                "win_rate":   r.get("win_rate"),
                "error":      r.get("error"),
            }
            socketio.emit("bt_progress", dict(_bt_progress))

        _bt_progress["running"] = False
        socketio.emit("bt_progress", _bt_progress)

        # Persist to DB
        from datetime import datetime as _dt
        try:
            db.set("bt_last_run_meta", {
                "preset":   preset["name"],
                "run_time": _dt.now().strftime("%Y-%m-%d %H:%M"),
                "config":   preset,
                "summary":  {r["symbol"]: {k: r.get(k) for k in
                    ["return_pct","net_pnl","gross_pnl","charges","trades","win_rate","balance","tier"]}
                    for r in results if r.get("symbol")},
            })
        except Exception as e:
            print(f"[BT] DB save error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/bt_progress")
def bt_progress_route():
    return jsonify(_bt_progress)

@app.route("/api/bt_last_run")
def api_bt_last_run():
    meta = db.get("bt_last_run_meta") or {}
    # Frontend expects `symbols`; keep `summary` for backward compatibility.
    if "symbols" not in meta:
        meta["symbols"] = meta.get("summary", {})
    return jsonify(meta)


# ── Live instrument management ────────────────────────────────────────────────

@app.route("/get_instruments")
def get_instruments():
    return jsonify(INSTRUMENTS)

@app.route("/live_status")
def live_status_route():
    if live_runner is None:
        return jsonify({"connected": False, "instruments": []})
    try:
        return jsonify({
            "connected":   live_runner.is_connected(),
            "instruments": _sanitize(live_runner.get_all_status()),
        })
    except Exception as e:
        return jsonify({"connected": False, "error": str(e)})


# ── Signals API ───────────────────────────────────────────────────────────────

@app.route("/api/signals")
def api_signals():
    date_str = request.args.get("date")
    if not date_str:
        from datetime import date as _date
        date_str = str(_date.today())
    try:
        sigs = db.get_signals(date_str=date_str)
    except Exception:
        sigs = []
    # Fallback to in-memory log for today
    if not sigs and not request.args.get("date"):
        sigs = list(_signal_log)
    return jsonify({"signals": sigs})


# ── Trades API ────────────────────────────────────────────────────────────────

@app.route("/api/trades/<symbol>")
def api_trades_symbol(symbol: str):
    date_str = request.args.get("date")
    try:
        trades = db.get_trades(symbol=symbol.upper(), date_str=date_str)
        return jsonify({"trades": trades})
    except Exception as e:
        return jsonify({"error": str(e), "trades": []}), 500


# ── Kill switch ───────────────────────────────────────────────────────────────

@app.route("/reset_kill_switch", methods=["POST"])
def api_reset_kill_switch():
    if not live_runner:
        return jsonify({"error": "Engine not running"}), 400
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").upper()
    for runner in live_runner.runners.values():
        if runner.symbol == symbol:
            runner.engine.reset_kill_switch()
            return jsonify({"status": "ok", "symbol": symbol})
    return jsonify({"error": f"{symbol} not found"}), 404


@app.route("/close_position", methods=["POST"])
def api_close_position():
    if not live_runner:
        return jsonify({"error": "Engine not running"}), 400
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").upper()
    import pandas as pd
    for token, runner in live_runner.runners.items():
        if runner.symbol == symbol and runner.broker.position:
            ltp = runner.last_price or runner.broker.position.get("entry_price", 0)
            runner.broker.force_close(ltp, pd.Timestamp.now(tz="Asia/Kolkata"))
            return jsonify({"status": "closed", "symbol": symbol, "price": ltp})
    return jsonify({"error": f"No open position for {symbol}"}), 404


# ── Admin ─────────────────────────────────────────────────────────────────────

@app.route("/admin/factory-reset", methods=["POST"])
def api_factory_reset():
    secret = request.get_json(silent=True, force=True) or {}
    if secret.get("confirm") != "RESET":
        return jsonify({"error": "confirm=RESET required"}), 400
    results = {}
    try:
        cx = db._get_conn()
        for tbl in ["trades", "signals", "engine_state"]:
            try:
                cx.execute(f"DELETE FROM {tbl}")
            except Exception:
                pass
        cx.commit()
        results["db"] = "trades, signals, engine_state wiped"
    except Exception as e:
        results["db_error"] = str(e)
    return jsonify(results)


# ── Upstox OAuth / token ──────────────────────────────────────────────────────

@app.route("/refresh_margins", methods=["POST", "GET"])
def refresh_margins():
    try:
        from margin_fetcher import fetch_margins
        fetch_margins()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/test_telegram", methods=["POST"])
def api_test_telegram():
    try:
        from telegram.notifier import send_telegram_message
        send_telegram_message(
            CONFIG["telegram_bot_token"],
            CONFIG["telegram_chat_id"],
            "✅ VWAP+HTF Engine — Telegram test OK",
        )
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Cron auto-start / stop ────────────────────────────────────────────────────

@app.route("/cron/start", methods=["POST", "GET"])
def cron_start():
    from datetime import datetime
    from dotenv import load_dotenv
    load_dotenv(override=True)

    if datetime.now().weekday() >= 5:
        return jsonify({"status": "skipped", "reason": "weekend"})

    token = os.getenv("UPSTOX_ACCESS_TOKEN", "") or CONFIG.get("upstox_access_token", "")
    if not token:
        return jsonify({"status": "error", "reason": "no_token"}), 400

    CONFIG["upstox_access_token"] = token
    if live_runner is not None:
        return jsonify({"status": "already_running"})

    _start_live_runner()

    try:
        bot  = CONFIG.get("telegram_bot_token", "")
        chat = CONFIG.get("telegram_chat_id", "")
        if bot and chat:
            _requests.post(
                f"https://api.telegram.org/bot{bot}/sendMessage",
                json={"chat_id": chat,
                      "text": f"🚀 <b>VWAP+HTF Engine Auto-Started</b>\n📅 {datetime.now().date()}  ⏰ 09:00 IST",
                      "parse_mode": "HTML"},
                timeout=6,
            )
    except Exception:
        pass

    return jsonify({"status": "started"})


@app.route("/cron/stop", methods=["POST", "GET"])
def cron_stop():
    from datetime import datetime
    global live_runner
    if live_runner is None:
        return jsonify({"status": "not_running"})

    try:
        live_runner.stop()
    except Exception as e:
        print(f"[Cron] stop error: {e}")
    live_runner = None

    socketio.emit("live_update", {"connected": False, "instruments": [], "portfolio_equity": {}})

    try:
        import heatmap_feed as _hf
        _hf.start_standalone_ws()
    except Exception:
        pass

    try:
        bot  = CONFIG.get("telegram_bot_token", "")
        chat = CONFIG.get("telegram_chat_id", "")
        if bot and chat:
            _requests.post(
                f"https://api.telegram.org/bot{bot}/sendMessage",
                json={"chat_id": chat,
                      "text": f"🔴 <b>VWAP+HTF Engine Auto-Stopped</b>\n📅 {datetime.now().date()}  ⏰ 15:31 IST",
                      "parse_mode": "HTML"},
                timeout=6,
            )
    except Exception:
        pass

    return jsonify({"status": "stopped"})


# ── SocketIO ──────────────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect(auth=None):
    if live_runner:
        try:
            instruments = _sanitize(live_runner.get_all_status())
            connected   = live_runner.is_connected()
            portfolio   = live_runner.get_portfolio_equity()
        except Exception:
            instruments, connected, portfolio = [], False, {}
        emit("engine_started", {"status": "ok", "instruments": instruments})
        emit("live_update", {
            "connected": connected,
            "portfolio_equity": portfolio,
            "instruments": instruments,
        })
        if _signal_log:
            emit("signal_history", _signal_log)


@socketio.on("stop_engine")
def on_stop_engine():
    global live_runner
    with _runner_lock:
        if live_runner is None:
            emit("engine_stopped", {"status": "not_running"})
            return
        try:
            live_runner.stop()
        except Exception as e:
            print(f"[App] stop error: {e}")
        live_runner = None
    try:
        import heatmap_feed as _hf
        _hf.start_standalone_ws()
    except Exception:
        pass
    emit("engine_stopped", {"status": "ok"})


@socketio.on("start_all")
def on_start_all():
    from dotenv import load_dotenv
    load_dotenv(override=True)

    token = os.getenv("UPSTOX_ACCESS_TOKEN", "") or CONFIG.get("upstox_access_token", "")
    if not token:
        try:
            from upstox_auth import load_token_from_db
            token = load_token_from_db() or ""
        except Exception:
            pass

    if not token:
        emit("engine_error", {"msg": "❌ No Upstox token — go to /auth/login first."})
        return

    CONFIG["upstox_access_token"] = token
    _start_live_runner()
    emit("engine_starting", {"status": "ok", "msg": "Replaying history + connecting WS..."})


# ── Entry point ───────────────────────────────────────────────────────────────

_on_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
