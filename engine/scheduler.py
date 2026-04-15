# engine/scheduler.py
#
# DailyScheduler — called on every live LTF candle.
# Handles: force-exit, EOD reporting, daily stat reset.
#
# Reclassification (tier_classifier) removed — not used in VWAP+HTF engine.

import threading
from datetime import date, datetime, timezone, timedelta, time as _time

from engine.session import is_force_exit_time
from engine.reporter import generate_daily_report
from engine.persistence import save_daily_trades
from engine.monthly_reporter import (
    generate_monthly_report,
    generate_monthly_equity_chart,
    is_last_trading_day,
)
from config import CONFIG


class DailyScheduler:
    """
    Called every LTF candle in live mode.
    Handles end-of-day force-exit, EOD reporting, and daily stat reset.
    """

    def __init__(self, broker, symbol: str = ""):
        self.broker           = broker
        self.symbol           = symbol
        self.last_report_date = None

    def check(self, candle_time, current_price: float):

        current_time = candle_time.time()
        today        = candle_time.date()

        # ── Force exit open position at session end ───────────────────────────
        if is_force_exit_time(current_time) and self.broker.position:
            self.broker.force_close(price=current_price, time=candle_time)

        # ── Daily EOD report ──────────────────────────────────────────────────
        # Guard: only run during real IST EOD window (15:20–16:00).
        # Prevents false fires when startup replay processes old 15:25 candles.
        _IST        = timezone(timedelta(hours=5, minutes=30))
        _real_ist   = datetime.now(_IST).time()
        _in_eod_win = _time(15, 20) <= _real_ist <= _time(16, 0)

        if (is_force_exit_time(current_time)
                and self.last_report_date != today
                and _in_eod_win):

            report = generate_daily_report(self.broker, self.symbol)

            save_daily_trades(
                trade_log=self.broker.trade_log,
                symbol=self.symbol,
                date_str=str(today),
            )

            if CONFIG.get("telegram_enabled"):
                try:
                    from telegram.notifier import send_telegram_summary
                    send_telegram_summary(
                        bot_token=CONFIG["telegram_bot_token"],
                        chat_id=CONFIG["telegram_chat_id"],
                        report=report,
                    )
                except Exception as e:
                    print(f"[Scheduler] Telegram error: {e}")

            # Monthly report on last trading day of month
            if is_last_trading_day(today):
                monthly = generate_monthly_report(self.symbol)
                img     = generate_monthly_equity_chart(self.symbol)

                if monthly and CONFIG.get("telegram_enabled"):
                    try:
                        from telegram.notifier import send_telegram_summary, send_telegram_image
                        send_telegram_summary(
                            bot_token=CONFIG["telegram_bot_token"],
                            chat_id=CONFIG["telegram_chat_id"],
                            report={
                                "date":     monthly["month"],
                                "symbol":   monthly["symbol"],
                                "gross":    monthly["gross"],
                                "charges":  monthly["charges"],
                                "net":      monthly["net"],
                                "trades":   monthly["trades"],
                                "win_rate": monthly["win_rate"],
                                "balance":  self.broker.balance,
                            },
                        )
                        if img:
                            send_telegram_image(
                                bot_token=CONFIG["telegram_bot_token"],
                                chat_id=CONFIG["telegram_chat_id"],
                                image_path=img,
                                caption=f"📊 Monthly Equity — {self.symbol}",
                            )
                    except Exception as e:
                        print(f"[Scheduler] Monthly Telegram error: {e}")

            self.broker.reset_daily_stats()
            self.last_report_date = today