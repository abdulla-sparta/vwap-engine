# upstox_auth.py
#
# Upstox OAuth 2.0 — auto daily token flow.
#
# How Upstox OAuth works:
#   1. Your app redirects user to Upstox login URL
#   2. User logs in with their Upstox credentials
#   3. Upstox redirects back to YOUR redirect_uri with ?code=AUTH_CODE
#   4. Your app exchanges that code for access_token via POST
#   5. access_token is valid until 3:30 AM next day
#
# This module:
#   - Provides /auth/login  → redirects to Upstox login page
#   - Provides /auth/callback → receives code, exchanges for token,
#                               stores in DB + CONFIG, restarts WS
#   - On app startup: loads last saved token from DB if still valid
#   - Token auto-refreshes: a background thread checks at 3:31 AM daily
#     and opens the login URL (sends Telegram notification with login link)
#
# Setup (one time):
#   1. In Upstox developer console, set redirect URI to:
#      http://YOUR_SERVER_IP:5000/auth/callback
#   2. Add to .env:
#      UPSTOX_API_KEY=your_api_key
#      UPSTOX_API_SECRET=your_api_secret
#      UPSTOX_REDIRECT_URI=http://YOUR_SERVER_IP:5000/auth/callback

import os
import json
import requests
import threading
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode
from config import CONFIG
import db

log = logging.getLogger(__name__)

UPSTOX_AUTH_URL  = "https://api.upstox.com/v2/login/authorization/dialog"

# ── NSE Holiday Calendar (2025 + 2026) ───────────────────────────────────────
# Update this list annually. Format: "YYYY-MM-DD"
NSE_HOLIDAYS = {
    # 2025
    "2025-01-26",  # Republic Day
    "2025-02-19",  # Chhatrapati Shivaji Maharaj Jayanti
    "2025-03-14",  # Holi
    "2025-04-10",  # Mahavir Jayanti (Good Friday falls on 18th)
    "2025-04-14",  # Dr. Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-08-27",  # Ganesh Chaturthi
    "2025-10-02",  # Gandhi Jayanti / Dussehra
    "2025-10-21",  # Diwali Laxmi Pujan (Muhurat trading — exchange call)
    "2025-10-22",  # Diwali Balipratipada
    "2025-11-05",  # Prakash Gurpurab
    "2025-12-25",  # Christmas
    # 2026
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Mahashivratri
    "2026-03-20",  # Holi
    "2026-04-02",  # Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day
    "2026-09-16",  # Ganesh Chaturthi
    "2026-10-02",  # Gandhi Jayanti
    "2026-10-09",  # Dussehra
    "2026-11-10",  # Diwali Laxmi Pujan (tentative)
    "2026-11-11",  # Diwali Balipratipada (tentative)
    "2026-11-25",  # Guru Nanak Jayanti (tentative)
    "2026-12-25",  # Christmas
}


def is_market_open_today() -> bool:
    """
    Returns True if NSE is open today (IST date):
      - Weekday (Mon–Fri)
      - Not in NSE_HOLIDAYS list
    Uses IST date explicitly — Railway servers run in UTC.
    """
    from datetime import datetime, timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(IST).date()
    if today.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    return str(today) not in NSE_HOLIDAYS
UPSTOX_TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _api_key() -> str:
    return os.getenv("UPSTOX_API_KEY", CONFIG.get("upstox_api_key", "")).strip()

def _api_secret() -> str:
    return os.getenv("UPSTOX_API_SECRET", "").strip()

def _redirect_uri() -> str:
    return os.getenv("UPSTOX_REDIRECT_URI", "http://localhost:5000/auth/callback").strip()


# ── Token storage in DB ───────────────────────────────────────────────────────

def save_token(access_token: str):
    """Save token to DB, .env file, and update live CONFIG so WS picks it up."""
    # 1. Save to DB
    db.set("upstox_access_token", {
        "token":    access_token,
        "saved_at": datetime.now().isoformat(),
    })
    # 2. Update live CONFIG immediately
    CONFIG["upstox_access_token"] = access_token

    # 3. Write back to .env so token survives app restarts
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(env_path):
            lines = open(env_path).readlines()
            updated = []
            token_written = False
            for line in lines:
                if line.startswith("UPSTOX_ACCESS_TOKEN"):
                    updated.append(f"UPSTOX_ACCESS_TOKEN={access_token}\n")
                    token_written = True
                else:
                    updated.append(line)
            if not token_written:
                updated.append(f"UPSTOX_ACCESS_TOKEN={access_token}\n")
            open(env_path, "w").writelines(updated)
            print(f"✅ Token written to .env")
    except Exception as e:
        print(f"⚠ Could not write token to .env: {e}")

    print(f"✅ Token saved to DB and CONFIG updated")


def load_token_from_db() -> str | None:
    """
    Load last saved token from DB.
    Upstox tokens expire at 3:30 AM IST = 22:00 UTC the *previous* calendar day.
    A token is valid only if it was saved AFTER the most recent 22:00 UTC cutoff.
    """
    try:
        data = db.get("upstox_access_token")
        if not data or not data.get("token"):
            return None
        saved_at = datetime.fromisoformat(data["saved_at"])
        now_utc  = datetime.utcnow()

        # Strip tz for naive comparison
        if saved_at.tzinfo is not None:
            saved_at = saved_at.replace(tzinfo=None)

        # Most recent Upstox expiry cutoff = last 22:00 UTC that has already passed
        # e.g. at 02:00 UTC on Thu, cutoff = Wed 22:00 UTC
        #      at 23:00 UTC on Thu, cutoff = Thu 22:00 UTC
        today_cutoff = now_utc.replace(hour=22, minute=0, second=0, microsecond=0)
        if now_utc >= today_cutoff:
            last_cutoff = today_cutoff          # today's 22:00 UTC already passed
        else:
            last_cutoff = today_cutoff - timedelta(days=1)  # yesterday's 22:00 UTC

        if saved_at < last_cutoff:
            age_h = (now_utc - saved_at).total_seconds() / 3600
            print(f"[Auth] Saved token expired (saved {age_h:.1f}h ago, cutoff was {last_cutoff.strftime('%Y-%m-%d %H:%M')} UTC)")
            return None

        return data["token"]
    except Exception as e:
        log.warning(f"[Auth] Could not load token from DB: {e}")
        return None


def is_token_valid() -> bool:
    """Check if current CONFIG token is non-empty."""
    return bool(CONFIG.get("upstox_access_token", ""))


# ── OAuth URL builder ─────────────────────────────────────────────────────────

def get_login_url() -> str:
    """Build Upstox OAuth login URL to redirect user to."""
    params = {
        "response_type": "code",
        "client_id":     _api_key(),
        "redirect_uri":  _redirect_uri(),
    }
    return f"{UPSTOX_AUTH_URL}?{urlencode(params)}"


# ── Token exchange ────────────────────────────────────────────────────────────

def exchange_code_for_token(auth_code: str) -> str | None:
    """
    Exchange OAuth authorization code for access_token.
    Called by /auth/callback route when Upstox redirects back.
    Returns access_token string or None on failure.
    """
    try:
        resp = requests.post(
            UPSTOX_TOKEN_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept":       "application/json",
            },
            data={
                "code":          auth_code,
                "client_id":     _api_key(),
                "client_secret": _api_secret(),
                "redirect_uri":  _redirect_uri(),
                "grant_type":    "authorization_code",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"❌ Token exchange failed: {resp.status_code} — {resp.text[:300]}")
            return None
        token = resp.json().get("access_token")
        if token:
            print(f"✅ Token received from Upstox")
            return token
        print(f"❌ No access_token in response: {resp.json()}")
        return None
    except Exception as e:
        print(f"❌ Token exchange error: {e}")
        return None


# ── Startup: restore token from DB ───────────────────────────────────────────

def restore_token_on_startup():
    """
    Called at app startup. If a valid token is saved in DB, load it
    into CONFIG so WS can start immediately without manual login.
    """
    token = load_token_from_db()
    if token:
        CONFIG["upstox_access_token"] = token
        print(f"✅ Token restored from DB — WS will start automatically")
        return True
    else:
        print(f"⚠ No valid token in DB — visit /auth/login to authenticate")
        return False


# ── Daily auto-refresh scheduler ──────────────────────────────────────────────

def start_daily_token_scheduler():
    """
    Background thread that fires at 7:30 AM IST every weekday market day.
    Sends a Telegram notification with the login URL so you can authenticate
    before market open at 9:15 AM. Engine auto-starts after OAuth callback.
    Runs forever as daemon.
    """
    def _scheduler():
        import time
        while True:
            now = datetime.now()
            # Next 7:30 AM IST (server runs in UTC, datetime.now() = server local)
            # Railway servers are UTC — so 7:30 AM IST = 2:00 AM UTC
            next_alert = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if next_alert <= now:
                next_alert += timedelta(days=1)

            wait_secs = (next_alert - datetime.now()).total_seconds()
            print(f"[Auth] Next login alert at {next_alert.strftime('%Y-%m-%d %H:%M UTC')} "
                  f"= 7:30 AM IST ({wait_secs/3600:.1f}h away)")
            time.sleep(max(wait_secs, 0))

            # Only send on weekdays + market open days (uses IST date)
            if not is_market_open_today():
                print(f"[Auth] Skipping login alert — market closed today (IST)")
                time.sleep(60)
                continue

            # Skip if a valid token already exists (user logged in early)
            existing = load_token_from_db()
            if existing:
                print(f"[Auth] Token already valid — skipping 7:30 AM alert")
                CONFIG["upstox_access_token"] = existing
                time.sleep(60)
                continue

            # Build login URL and send alert
            login_url = get_login_url()
            msg = (
                f"🔐 <b>StructureEngine — Token Expired</b>\n\n"
                f"Upstox token has expired (3:30 AM cutoff).\n\n"
                f"👉 <a href=\"{login_url}\">Click here to re-login</a>\n\n"
                f"Or open this URL in your browser:\n"
                f"<code>{login_url}</code>\n\n"
                f"After login the engine will <b>auto-start</b>. "
                f"Market opens at 9:15 AM — you have ~1h 45min."
            )
            _send_telegram(msg)
            print(f"[Auth] 7:30 AM IST — sent login alert via Telegram")
            time.sleep(60)   # prevent double-fire within same minute

    t = threading.Thread(target=_scheduler, daemon=True, name="TokenScheduler")
    t.start()
    return t


def _send_telegram(msg: str):
    """Send message via Telegram (non-blocking, best effort)."""
    bot_token = CONFIG.get("telegram_bot_token", "")
    chat_id   = CONFIG.get("telegram_chat_id", "")
    if not bot_token or not chat_id:
        print(f"[Auth] Telegram not configured — login URL: {get_login_url()}")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id":                chat_id,
                "text":                   msg,
                "parse_mode":             "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if not r.ok:
            log.warning(f"[Auth] Telegram HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.warning(f"[Auth] Telegram send failed: {e}")