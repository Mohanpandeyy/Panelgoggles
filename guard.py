# ╔══════════════════════════════════════════════════════════════╗
#   GUARD.PY — Device Fingerprint Guard for office_relay.py
#   Features: SQLite FP Store · Referral Interception
#             WebApp sendData flow · New / Already-Verified / Blocked
# ╚══════════════════════════════════════════════════════════════╝
#
# HOW TO INTEGRATE WITH office_relay.py
# ───────────────────────────────────────────────────────────────
# Add exactly ONE line anywhere in office_relay.py (top is fine):
#
#     import guard
#
# That's it. guard.py:
#   1. Initialises guard.db
#   2. Waits for office_relay's dp + db + bot to be ready, then:
#      • Patches db.reg_user to defer referral counting until
#        the new user completes device verification
#      • Registers a web_app_data handler into the existing dispatcher
# ───────────────────────────────────────────────────────────────

# ── Configuration — fill these in ─────────────────────────────
GUARD_BOT_TOKEN  = "8908948438:AAE1fRjlrsl8cGRJYjne8a5-c1AEJbZPNb4"   # same token as office_relay.py BOT_TOKEN
GUARD_WEBAPP_URL = "https://verify-1-1626.vercel.app/"   # full HTTPS URL to verify.html on Vercel
GUARD_DB_PATH    = "guard.db"
RELAY_DB_PATH    = "office_relay.db"
# ── End configuration ──────────────────────────────────────────

import sys, threading, sqlite3, json, logging
from datetime import datetime, timedelta

_IST = timedelta(hours=5, minutes=30)

def _now_ist_str() -> str:
    return (datetime.utcnow() + _IST).strftime("%d-%m-%Y %H:%M")

log = logging.getLogger("guard")


# ══════════════════════════════════════════════════════════════
# GUARD DATABASE
# ══════════════════════════════════════════════════════════════

class GuardDB:
    """Manages guard.db — fingerprint records and pending verifications."""

    def __init__(self, path: str = GUARD_DB_PATH):
        self.path = path
        self._init()

    def _cx(self):
        c = sqlite3.connect(self.path)
        c.row_factory = lambda cur, row: {
            col[0]: row[i] for i, col in enumerate(cur.description)
        }
        return c

    def _init(self):
        with self._cx() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS device_fingerprints (
                    fp_hash     TEXT PRIMARY KEY,
                    user_id     INTEGER NOT NULL,
                    verified_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pending_verifications (
                    user_id       INTEGER PRIMARY KEY,
                    referrer_id   INTEGER,
                    verify_msg_id INTEGER DEFAULT NULL,
                    created_at    TEXT NOT NULL
                );
            """)

    def add_pending(self, user_id: int, referrer_id: int):
        with self._cx() as c:
            c.execute(
                "INSERT OR IGNORE INTO pending_verifications"
                "(user_id, referrer_id, created_at) VALUES(?,?,?)",
                (user_id, referrer_id, _now_ist_str())
            )

    def save_msg_id(self, user_id: int, msg_id: int):
        with self._cx() as c:
            c.execute(
                "UPDATE pending_verifications SET verify_msg_id=? WHERE user_id=?",
                (msg_id, user_id)
            )

    def get_pending(self, user_id: int):
        return self._cx().execute(
            "SELECT * FROM pending_verifications WHERE user_id=?", (user_id,)
        ).fetchone()

    def remove_pending(self, user_id: int):
        with self._cx() as c:
            c.execute("DELETE FROM pending_verifications WHERE user_id=?", (user_id,))

    def get_fp(self, fp_hash: str):
        return self._cx().execute(
            "SELECT * FROM device_fingerprints WHERE fp_hash=?", (fp_hash,)
        ).fetchone()

    def save_fp(self, fp_hash: str, user_id: int):
        with self._cx() as c:
            c.execute(
                "INSERT OR IGNORE INTO device_fingerprints"
                "(fp_hash, user_id, verified_at) VALUES(?,?,?)",
                (fp_hash, user_id, _now_ist_str())
            )

    def is_already_verified_user(self, user_id: int) -> bool:
        return bool(
            self._cx().execute(
                "SELECT 1 FROM device_fingerprints WHERE user_id=?", (user_id,)
            ).fetchone()
        )


guard_db = GuardDB()


# ══════════════════════════════════════════════════════════════
# RELAY DATABASE HELPERS (writes to office_relay.db)
# ══════════════════════════════════════════════════════════════

def _relay_cx():
    c = sqlite3.connect(RELAY_DB_PATH)
    c.row_factory = lambda cur, row: {
        col[0]: row[i] for i, col in enumerate(cur.description)
    }
    return c

def _relay_get_setting(key: str):
    row = _relay_cx().execute(
        "SELECT value FROM settings WHERE key=?", (key,)
    ).fetchone()
    return row["value"] if row else None

def _relay_credit_referral(referrer_id: int, referred_id: int) -> dict:
    """
    Credits a referral in office_relay.db exactly as reg_user would have.
    Returns {"notify_ref": referrer_id, "new_key": key} if a key was generated.
    """
    from string import ascii_uppercase, digits
    import secrets as _secrets
    KEY_PREFIX = "RELAY-"
    KEY_LENGTH = 30

    def gen_key():
        ch = ascii_uppercase + digits
        return KEY_PREFIX + "".join(
            _secrets.choice(ch) for _ in range(KEY_LENGTH - len(KEY_PREFIX))
        )

    now = _now_ist_str()
    with _relay_cx() as c:
        if c.execute(
            "SELECT 1 FROM refer_log WHERE referred_id=?", (referred_id,)
        ).fetchone():
            return {}

        c.execute("INSERT INTO refer_log VALUES(NULL,?,?,?)",
                  (referrer_id, referred_id, now))
        c.execute(
            "INSERT OR IGNORE INTO referral_audit"
            "(referrer_id, referred_id, status, at) VALUES(?,?,?,?)",
            (referrer_id, referred_id, "Valid", now)
        )
        c.execute("UPDATE users SET refer_count=refer_count+1 WHERE user_id=?",
                  (referrer_id,))
        ref   = c.execute("SELECT * FROM users WHERE user_id=?", (referrer_id,)).fetchone()
        limit = int(_relay_get_setting("refer_limit") or 1)
        if ref and ref["refer_count"] >= limit and not ref["access_key"]:
            nk = gen_key()
            c.execute("UPDATE users SET access_key=? WHERE user_id=?", (nk, referrer_id))
            c.execute("INSERT INTO access_keys VALUES(?,?,?,NULL,0,0)",
                      (nk, referrer_id, now))
            return {"notify_ref": referrer_id, "new_key": nk}
    return {}


# ══════════════════════════════════════════════════════════════
# TELEGRAM BOT API — raw urllib (no second dispatcher conflict)
# ══════════════════════════════════════════════════════════════

import urllib.request

def _tg_api(method: str, payload: dict) -> dict:
    url  = f"https://api.telegram.org/bot{GUARD_BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.error("TG API %s error: %s", method, e)
        return {}

def _send_webapp_button(user_id: int) -> int:
    """Sends the 'Verify My Device' WebApp button as a ReplyKeyboardMarkup. Returns message_id."""
    resp = _tg_api("sendMessage", {
        "chat_id":    user_id,
        "parse_mode": "HTML",
        "text": (
            "╔══════════════════════╗\n"
            "  🛡 <b>DEVICE VERIFICATION</b>\n"
            "╚══════════════════════╝\n\n"
            "Before your referral is counted, verify your device.\n\n"
            "Tap the button below — it takes under 5 seconds "
            "and only needs to be done once."
        ),
        "reply_markup": json.dumps({
            "keyboard": [[{
                "text":    "🔐 Verify My Device",
                "web_app": {"url": GUARD_WEBAPP_URL}
            }]],
            "resize_keyboard":   True,
            "one_time_keyboard": True
        })
    })
    return resp.get("result", {}).get("message_id", 0)

def _edit_remove_button(user_id: int, msg_id: int, new_text: str):
    """Sends the result as a new message and removes the ReplyKeyboard."""
    _tg_api("sendMessage", {
        "chat_id":    user_id,
        "text":       new_text,
        "parse_mode": "HTML",
        "reply_markup": json.dumps({"remove_keyboard": True})
    })

def _send_message(user_id: int, text: str):
    _tg_api("sendMessage", {
        "chat_id":    user_id,
        "text":       text,
        "parse_mode": "HTML"
    })


# ══════════════════════════════════════════════════════════════
# CORE FINGERPRINT LOGIC
# ══════════════════════════════════════════════════════════════

def on_referral_join(user_id: int, referrer_id: int):
    """
    Called when a NEW user joins via referral.
    Defers referral counting and sends the WebApp verification button.
    """
    if guard_db.is_already_verified_user(user_id):
        result = _relay_credit_referral(referrer_id, user_id)
        if result.get("notify_ref"):
            _send_message(
                result["notify_ref"],
                f"╔══════════════════════╗\n"
                f"  🎉 <b>REFERRAL SUCCESS!</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Your Access Key:\n\n<code>{result['new_key']}</code>\n\n"
                f"<i>Use this key to unlock full access.</i>"
            )
        return

    # Only send the button once — skip if already pending or already verified
    if guard_db.get_pending(user_id):
        log.info("Guard: verification already pending for user %s, skipping duplicate button", user_id)
        return

    guard_db.add_pending(user_id, referrer_id)
    msg_id = _send_webapp_button(user_id)
    if msg_id:
        guard_db.save_msg_id(user_id, msg_id)
    log.info("Guard: pending verification queued for user %s (referrer %s)",
             user_id, referrer_id)


def handle_fingerprint(user_id: int, fp_hash: str) -> str:
    """
    Processes a fingerprint submission received via web_app_data.

    Returns:
      "verified"          — new fingerprint, referral credited
      "already_verified"  — same fingerprint, same user
      "blocked"           — same fingerprint, different user (fraud)
    """
    existing = guard_db.get_fp(fp_hash)

    # ── New fingerprint ─────────────────────────────────────
    if existing is None:
        guard_db.save_fp(fp_hash, user_id)
        pending = guard_db.get_pending(user_id)
        if pending:
            referrer_id = pending["referrer_id"]
            msg_id      = pending.get("verify_msg_id")
            guard_db.remove_pending(user_id)
            result = _relay_credit_referral(referrer_id, user_id)
            _edit_remove_button(
                user_id, msg_id,
                "╔══════════════════════╗\n"
                "  ✅ <b>DEVICE VERIFIED</b>\n"
                "╚══════════════════════╝\n\n"
                "Your device is verified and your referral has been credited. "
                "Tap /start to continue."
            )
            if result.get("notify_ref"):
                _send_message(
                    result["notify_ref"],
                    f"╔══════════════════════╗\n"
                    f"  🎉 <b>REFERRAL SUCCESS!</b>\n"
                    f"╚══════════════════════╝\n\n"
                    f"Your Access Key:\n\n<code>{result['new_key']}</code>\n\n"
                    f"<i>Use this key to unlock full access.</i>"
                )
        else:
            _send_message(
                user_id,
                "✅ <b>Device verified.</b>\n"
                "No pending referral found — your device is now on record."
            )
        return "verified"

    # ── Same fingerprint, same user ─────────────────────────
    if existing["user_id"] == user_id:
        pending = guard_db.get_pending(user_id)
        if pending:
            msg_id      = pending.get("verify_msg_id")
            referrer_id = pending["referrer_id"]
            guard_db.remove_pending(user_id)
            _relay_credit_referral(referrer_id, user_id)
            _edit_remove_button(
                user_id, msg_id,
                "╔══════════════════════╗\n"
                "  ✅ <b>ALREADY VERIFIED</b>\n"
                "╚══════════════════════╝\n\n"
                "Device was already on record. Referral credited. "
                "Tap /start to continue."
            )
        return "already_verified"

    # ── Same fingerprint, different user — BLOCK ────────────
    log.warning(
        "Guard: FP collision! fp=%s..., original=%s, attacker=%s",
        fp_hash[:12], existing["user_id"], user_id
    )
    pending = guard_db.get_pending(user_id)
    if pending:
        msg_id = pending.get("verify_msg_id")
        guard_db.remove_pending(user_id)
        _edit_remove_button(
            user_id, msg_id,
            "╔══════════════════════╗\n"
            "  ⛔ <b>BLOCKED</b>\n"
            "╚══════════════════════╝\n\n"
            "This device is already registered to a different account. "
            "Fake referral detected — no credit has been issued."
        )
    return "blocked"


# ══════════════════════════════════════════════════════════════
# WEB APP DATA HANDLER (aiogram) — registered into the host dp
# ══════════════════════════════════════════════════════════════

def _build_guard_router():
    """
    Returns a new aiogram Router with a web_app_data handler.
    This router is injected into the host dispatcher by _deferred_patch.
    """
    try:
        from aiogram import Router as _Router, F as _F
        from aiogram.types import Message as _Message
    except ImportError:
        log.error("Guard: aiogram not available — web_app_data handler not registered.")
        return None

    r = _Router()

    @r.message(_F.web_app_data)
    async def _web_app_data_handler(msg: _Message):
        raw = msg.web_app_data.data if msg.web_app_data else ""
        try:
            payload = json.loads(raw)
            fp_hash = payload.get("fingerprint_hash", "").lower()
        except (json.JSONDecodeError, AttributeError):
            fp_hash = raw.strip().lower()   # plain hash string fallback

        if not fp_hash or len(fp_hash) != 64:
            log.warning("Guard: invalid fingerprint payload from %s: %r",
                        msg.from_user.id, raw[:80])
            return

        user_id = msg.from_user.id
        # Run synchronously in a thread so we don't block the event loop
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, handle_fingerprint, user_id, fp_hash)

    return r


# ══════════════════════════════════════════════════════════════
# MONKEY-PATCH — intercepts db.reg_user in office_relay.py
# ══════════════════════════════════════════════════════════════

def _install_db_patch(db_instance):
    """
    Replaces db.reg_user with a guarded version that defers referral counting
    until device fingerprint is verified via the Telegram WebApp.
    """
    original_reg_user = db_instance.reg_user   # keep reference in closure

    def guarded_reg_user(uid, uname, ref_id=None):
        with db_instance.cx() as c:
            existing = c.execute(
                "SELECT * FROM users WHERE user_id=?", (uid,)
            ).fetchone()

        if existing:
            return dict(existing)

        # New user — register without crediting referral yet
        with db_instance.cx() as c:
            c.execute(
                "INSERT INTO users(user_id, username) VALUES(?,?)", (uid, uname)
            )

        if ref_id and ref_id != uid:
            threading.Thread(
                target=on_referral_join,
                args=(uid, ref_id),
                daemon=True,
                name=f"guard-join-{uid}"
            ).start()

        return {"is_banned": 0}

    db_instance.reg_user = guarded_reg_user
    log.info("Guard: db.reg_user patched — referrals now require device verification")


# ══════════════════════════════════════════════════════════════
# DEFERRED PATCH — waits for host module to fully initialise
# ══════════════════════════════════════════════════════════════

def _deferred_patch():
    """
    Polls sys.modules for office_relay's dp and db objects, then:
      1. Patches db.reg_user
      2. Registers the web_app_data handler router into dp
    Runs in a daemon thread so it never blocks the import.
    """
    import time

    guard_router = _build_guard_router()

    for _ in range(60):          # up to 30 seconds
        relay = sys.modules.get("__main__") or sys.modules.get("office_relay")
        if relay and hasattr(relay, "db") and hasattr(relay.db, "reg_user") \
                 and hasattr(relay, "dp"):
            _install_db_patch(relay.db)
            if guard_router is not None:
                try:
                    relay.dp.include_router(guard_router)
                    log.info("Guard: web_app_data handler registered into dp")
                except Exception as e:
                    log.warning("Guard: could not register router into dp: %s", e)
            return
        time.sleep(0.5)

    log.warning(
        "Guard: host module (db / dp) not found after 30 s. "
        "Call guard.install(db, dp) manually after they are initialised."
    )


def install(db_instance, dp_instance=None):
    """
    Manual fallback — call this if the auto-patch didn't fire.
    Example:
        import guard
        guard.install(db, dp)
    """
    _install_db_patch(db_instance)
    if dp_instance is not None:
        gr = _build_guard_router()
        if gr:
            dp_instance.include_router(gr)
            log.info("Guard: web_app_data handler registered via manual install()")


# ══════════════════════════════════════════════════════════════
# MODULE INIT — runs once when office_relay.py does `import guard`
# ══════════════════════════════════════════════════════════════

threading.Thread(
    target=_deferred_patch,
    daemon=True,
    name="guard-patcher"
).start()
log.info("Guard: module loaded — patcher thread started")
