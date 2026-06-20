"""
sms_patch.py — Drop-in fix + SMS Filter Manager for office_relay.py
====================================================================
Run THIS file instead of office_relay.py:
    python sms_patch.py

Your original office_relay.py is NEVER modified.

What this does:
  1. Loads office_relay.py as a module (its __main__ block is skipped).
  2. Strips the Android SMS Retriever API wrapper  (<#> prefix + 11-char hash)
     from every incoming message — fixes Blinkit / Grofers / PhonePe OTP drops.
  3. HTML-escapes all SMS body/sender text before Telegram sends — stops
     the silent TelegramBadRequest crash caused by raw <#> in HTML mode.
  4. Adds Admin → System → 🚫 SMS Filters:
       • Add pattern (Prefix / Contains / Regex)
       • Action: 🧹 Strip (remove prefix, deliver rest) or 🚫 Block (drop SMS)
       • Per-filter hit counter
       • Delete any filter instantly

Place this file in the SAME folder as office_relay.py then run it.
"""

from __future__ import annotations
import re, html, time, asyncio, logging, importlib.util, sys

log = logging.getLogger("sms_patch")

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Load office_relay WITHOUT triggering its  if __name__ == "__main__"  block
# ─────────────────────────────────────────────────────────────────────────────
_spec  = importlib.util.spec_from_file_location("office_relay", "office_relay.py")
relay  = importlib.util.module_from_spec(_spec)
sys.modules["office_relay"] = relay
_spec.loader.exec_module(relay)

# Handy shortcuts into the loaded module
db        = relay.db
router    = relay.router
bot       = relay.bot
ADMIN_IDS = relay.ADMIN_IDS

from aiogram import F
from aiogram.types import (Message, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state  import State, StatesGroup


# ─────────────────────────────────────────────────────────────────────────────
# 2.  HTML-escape helper
# ─────────────────────────────────────────────────────────────────────────────

def _h(text) -> str:
    """Escape user-supplied text before embedding in Telegram parse_mode=HTML."""
    return html.escape(str(text) if text is not None else "")


# ─────────────────────────────────────────────────────────────────────────────
# 3.  SMS Retriever API wrapper stripper
# ─────────────────────────────────────────────────────────────────────────────
# Messages that start with  <#>  are Android SMS Retriever API format.
# e.g.  "<#> Use OTP 9195 for Blinkit. Valid 5 min h2i2dXfT8qL"
#        ^^^                                               ^^^^^^^^^^^
#       prefix (breaks Telegram HTML)              app hash suffix

_PREFIX_RE = re.compile(r"^<#>\s*")
_SUFFIX_RE = re.compile(r"\s+[A-Za-z0-9]{11}$")

def _strip_wrapper(body: str) -> str:
    if not body:
        return body
    body = _PREFIX_RE.sub("", body)
    body = _SUFFIX_RE.sub("", body)
    return body.strip()


# ─────────────────────────────────────────────────────────────────────────────
# 4.  SMS Filter Engine  (in-memory cache, DB-backed)
# ─────────────────────────────────────────────────────────────────────────────

# Create table if it doesn't exist yet
with db.cx() as _cx:
    _cx.execute("""
        CREATE TABLE IF NOT EXISTS sms_filters (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern    TEXT UNIQUE,
            match_type TEXT DEFAULT 'prefix',
            action     TEXT DEFAULT 'strip',
            label      TEXT DEFAULT NULL,
            added_by   INTEGER,
            added_at   TEXT,
            hit_count  INTEGER DEFAULT 0
        )
    """)

_filter_cache:    list  = []
_filter_cache_ts: float = 0.0
_FILTER_TTL:      float = 60.0   # seconds between DB reloads


def _reload_filters() -> list:
    global _filter_cache, _filter_cache_ts
    try:
        rows = db.cx().execute(
            "SELECT id, pattern, match_type, action FROM sms_filters"
        ).fetchall()
        out = []
        for r in rows:
            entry = dict(r)
            if entry["match_type"] == "regex":
                try:    entry["_re"] = re.compile(entry["pattern"])
                except: entry["_re"] = None
            out.append(entry)
        _filter_cache    = out
        _filter_cache_ts = time.monotonic()
    except Exception as e:
        log.warning("[Filters] reload error: %s", e)
    return _filter_cache


def _get_filters() -> list:
    if time.monotonic() - _filter_cache_ts > _FILTER_TTL:
        return _reload_filters()
    return _filter_cache


def _invalidate_cache():
    global _filter_cache_ts
    _filter_cache_ts = 0.0


def _apply_filters(body: str) -> tuple[str, bool]:
    """
    Apply all admin filters.
    Returns (body, blocked).  blocked=True → drop SMS entirely.
    """
    if not body:
        return body, False
    for f in _get_filters():
        mt  = f.get("match_type", "prefix")
        act = f.get("action",     "strip")
        pat = f.get("pattern",    "")
        matched  = False
        match_end = 0
        if mt == "prefix":
            if body.startswith(pat):
                matched, match_end = True, len(pat)
        elif mt == "contains":
            matched = pat in body
        elif mt == "regex":
            rx = f.get("_re")
            if rx:
                m = rx.search(body)
                if m:
                    matched, match_end = True, m.end()
        if matched:
            try:
                with db.cx() as c:
                    c.execute(
                        "UPDATE sms_filters SET hit_count=hit_count+1 WHERE id=?",
                        (f["id"],))
            except Exception:
                pass
            if act == "block":
                return body, True
            if act == "strip" and mt == "prefix":
                body = body[match_end:].strip()
    return body, False


# Pre-warm cache at startup
_reload_filters()


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Monkey-patch  _norm_sms  — strip wrapper + apply filters on every SMS
# ─────────────────────────────────────────────────────────────────────────────

_orig_norm_sms = relay._norm_sms

def _patched_norm_sms(entry):
    msg, sender, ts = _orig_norm_sms(entry)
    if msg:
        msg = _strip_wrapper(msg)         # remove <#> prefix + hash
        msg, blocked = _apply_filters(msg)
        if blocked:
            return None, None, None       # callers all check  `if msg:`
    return msg, sender, ts

relay._norm_sms = _patched_norm_sms
log.info("[sms_patch] Patched _norm_sms ✓")


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Monkey-patch  fmt_otp  — HTML-escape body + sender
# ─────────────────────────────────────────────────────────────────────────────

def _patched_fmt_otp(d, number_detail=None):
    otp_val = d.get("otp")
    number  = relay._disp(d.get("number", ""))
    sender  = _h(d.get("sender",  "?"))
    message = _h(d.get("message", ""))

    if otp_val:
        header = (f"🔖 <b>NEW OTP DETECTED</b> 🔖\n\n"
                  f"⚡ Fresh SMS just arrived!\n\n"
                  f"🔑 <b>NEW OTP → <code>{_h(otp_val)}</code></b>")
    else:
        header = (f"╔══════════════════════╗\n"
                  f"  📨 <b>SMS RECEIVED</b>\n"
                  f"╚══════════════════════╝\n\n"
                  f"⚡ Fresh SMS just arrived!")

    return (f"{header}\n"
            f"📱 <b>Device</b> · <code>{number}</code>\n"
            f"👤 <b>Sender</b> · {sender}\n\n"
            f"💬 <i>{message}</i>\n\n"
            f"✨ <i>Panel auto-refreshed above</i> ⬆")

relay.fmt_otp = _patched_fmt_otp
log.info("[sms_patch] Patched fmt_otp ✓")


# ─────────────────────────────────────────────────────────────────────────────
# 7.  Monkey-patch  _check_watchlist  — HTML-escape body/sender/note
# ─────────────────────────────────────────────────────────────────────────────

async def _patched_check_watchlist(number: str, sender: str, body: str, otp: str):
    row = db.cx().execute(
        "SELECT * FROM watchlist WHERE number=?", (number,)
    ).fetchone()
    if not row:
        return
    note_str = f"\n📝 Note: {_h(row['note'])}" if row.get("note") else ""
    alert = (
        f"🚨 <b>WATCHLIST ALERT</b>\n\n"
        f"📞 <b>Number:</b> <code>{number}</code>{note_str}\n"
        f"📨 <b>From:</b> {_h(sender)}\n"
        f"{'🎯 OTP: <code>' + _h(otp) + '</code>' + chr(10) if otp else ''}"
        f"💬 <b>SMS:</b> <i>{_h(body[:200])}</i>"
    )
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, alert)
        except Exception:
            pass

relay._check_watchlist = _patched_check_watchlist
log.info("[sms_patch] Patched _check_watchlist ✓")


# ─────────────────────────────────────────────────────────────────────────────
# 8.  Monkey-patch  fmt_device_detail  — HTML-escape message history display
# ─────────────────────────────────────────────────────────────────────────────

_orig_fmt_device_detail = relay.fmt_device_detail

def _patched_fmt_device_detail(num, msgs, *args, **kwargs
    safe = []
    for m in (msgs or []):
        s = dict(m)
        if s.get("message"): s["message"] = _h(s["message"])
        if s.get("sender"):  s["sender"]  = _h(s["sender"])
        if s.get("otp"):     s["otp"]     = _h(s["otp"])
        safe.append(s)
    return _orig_fmt_device_detail(num, safe, *args, **kwargs)

relay.fmt_device_detail = _patched_fmt_device_detail
log.info("[sms_patch] Patched fmt_device_detail ✓")


# ─────────────────────────────────────────────────────────────────────────────
# 9.  Monkey-patch  menu_system  — inject 🚫 SMS Filters button
# ─────────────────────────────────────────────────────────────────────────────

_orig_menu_system = relay.menu_system

def _patched_menu_system():
    kb   = _orig_menu_system()
    rows = list(kb.inline_keyboard)
    back = rows[-1]                        # keep Back button last
    rows.insert(-1, [
        InlineKeyboardButton(text="🚫 SMS Filters", callback_data="adm_sms_filters")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

relay.menu_system = _patched_menu_system
log.info("[sms_patch] Patched menu_system ✓")


# ─────────────────────────────────────────────────────────────────────────────
# 10.  SMS Filter Manager — admin bot UI
# ─────────────────────────────────────────────────────────────────────────────

class SFS(StatesGroup):
    pattern = State()   # waiting for pattern text from admin


_pending: dict = {}    # {user_id: {"pattern": str, "match_type": str}}


def _fmt_filter_list() -> str:
    rows = db.cx().execute(
        "SELECT id, pattern, match_type, action, label, hit_count, added_at "
        "FROM sms_filters ORDER BY id"
    ).fetchall()
    if not rows:
        return "📭 <i>No SMS filters configured yet.</i>"
    icons = {"strip": "🧹", "block": "🚫"}
    types = {"prefix": "Prefix", "contains": "Contains", "regex": "Regex"}
    lines = []
    for r in rows:
        label = f" <i>{_h(r['label'])}</i>" if r.get("label") else ""
        lines.append(
            f"{icons.get(r['action'], '❓')} <b>#{r['id']}</b>{label}\n"
            f"   📐 {types.get(r['match_type'], r['match_type'])} · "
            f"<code>{_h(r['pattern'])}</code>\n"
            f"   🎯 Hits: {r['hit_count']}  · {r['added_at'] or '—'}"
        )
    return "\n\n".join(lines)


def _kb_filter_list() -> InlineKeyboardMarkup:
    rows = db.cx().execute(
        "SELECT id, pattern FROM sms_filters ORDER BY id"
    ).fetchall()
    kb = []
    for r in rows:
        short = r["pattern"][:26] + "…" if len(r["pattern"]) > 26 else r["pattern"]
        kb.append([InlineKeyboardButton(
            text=f"🗑 #{r['id']}  {short}",
            callback_data=f"smsf_del_{r['id']}"
        )])
    kb.append([InlineKeyboardButton(text="➕ Add Filter",  callback_data="smsf_add")])
    kb.append([InlineKeyboardButton(text="🔙 System",      callback_data="adm_system")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ── Show filter list ──────────────────────────────────────────────────────────
@router.callback_query(F.data == "adm_sms_filters")
async def cb_smsf_home(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer()
    text = (
        "╔══════════════════════╗\n"
        "  🚫 <b>SMS FILTER MANAGER</b>\n"
        "╚══════════════════════╝\n\n"
        "<b>Match types:</b>\n"
        "• <b>Prefix</b> — message starts with pattern\n"
        "• <b>Contains</b> — pattern found anywhere\n"
        "• <b>Regex</b> — full Python regex\n\n"
        "<b>Actions:</b>\n"
        "• 🧹 <b>Strip</b> — remove matched prefix, deliver the rest\n"
        "• 🚫 <b>Block</b> — drop SMS entirely, no delivery\n\n"
        + _fmt_filter_list()
    )
    await call.message.answer(text[:4000], reply_markup=_kb_filter_list())
    await call.answer()


# ── Start add-filter flow ─────────────────────────────────────────────────────
@router.callback_query(F.data == "smsf_add")
async def cb_smsf_add(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer()
    _pending[call.from_user.id] = {}
    await call.message.answer(
        "🚫 <b>Add SMS Filter — Step 1 of 3</b>\n\n"
        "Send the <b>pattern</b> you want to match.\n\n"
        "<i>Examples:\n"
        "• <code>&lt;#&gt;</code>  — any SMS Retriever message\n"
        "• <code>[Promo]</code>  — prefix block\n"
        "• <code>Dear Customer</code>  — promo blast prefix\n"
        "• <code>Win a prize</code>  — contains block\n"
        "• <code>^OFFER|^SALE</code>  — regex at start</i>"
    )
    await state.set_state(SFS.pattern)
    await call.answer()


# ── Receive pattern text ──────────────────────────────────────────────────────
@router.message(SFS.pattern)
async def proc_smsf_pattern(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    pattern = (msg.text or "").strip()
    if not pattern:
        return await msg.answer("❌ Pattern cannot be empty. Try again:")
    _pending[msg.from_user.id]["pattern"] = pattern
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔡 Prefix   (starts with)",  callback_data="smsf_mt_prefix")],
        [InlineKeyboardButton(text="🔍 Contains  (anywhere)",    callback_data="smsf_mt_contains")],
        [InlineKeyboardButton(text="🔢 Regex    (advanced)",     callback_data="smsf_mt_regex")],
        [InlineKeyboardButton(text="❌ Cancel",                  callback_data="adm_sms_filters")],
    ])
    await msg.answer(
        f"✅ Pattern: <code>{_h(pattern)}</code>\n\n"
        f"<b>Step 2 of 3 — Match type:</b>",
        reply_markup=kb
    )


# ── Choose match type ─────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("smsf_mt_"))
async def cb_smsf_match_type(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer()
    uid     = call.from_user.id
    mt      = call.data.split("smsf_mt_")[1]
    pending = _pending.get(uid)
    if not pending or "pattern" not in pending:
        return await call.answer("Session expired. Start again.", show_alert=True)
    if mt == "regex":
        try:
            re.compile(pending["pattern"])
        except re.error as e:
            return await call.answer(f"❌ Invalid regex: {e}", show_alert=True)
    _pending[uid]["match_type"] = mt
    labels = {"prefix": "Prefix", "contains": "Contains", "regex": "Regex"}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🧹 Strip  — remove prefix, deliver rest",
            callback_data="smsf_act_strip")],
        [InlineKeyboardButton(
            text="🚫 Block  — drop SMS entirely",
            callback_data="smsf_act_block")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="adm_sms_filters")],
    ])
    await call.message.answer(
        f"Match: <b>{labels.get(mt, mt)}</b>\n\n"
        f"<b>Step 3 of 3 — Action:</b>",
        reply_markup=kb
    )
    await call.answer()


# ── Choose action → save filter ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("smsf_act_"))
async def cb_smsf_action(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer()
    uid     = call.from_user.id
    action  = call.data.split("smsf_act_")[1]
    pending = _pending.pop(uid, None)
    if not pending or "pattern" not in pending or "match_type" not in pending:
        return await call.answer("Session expired. Start again.", show_alert=True)
    pattern    = pending["pattern"]
    match_type = pending["match_type"]
    now_str    = relay._now_ist().strftime("%d-%m-%Y %H:%M")
    try:
        with db.cx() as c:
            c.execute(
                "INSERT INTO sms_filters(pattern, match_type, action, added_by, added_at) "
                "VALUES (?,?,?,?,?)",
                (pattern, match_type, action, uid, now_str)
            )
        _invalidate_cache()
        relay.db.log_action(uid, "SMS Filter Added",
                            f"{match_type}/{action}: {pattern[:80]}")
        alabel = "🧹 Strip prefix" if action == "strip" else "🚫 Block SMS"
        mlabel = {"prefix": "Prefix", "contains": "Contains",
                  "regex": "Regex"}.get(match_type, match_type)
        await call.message.answer(
            f"✅ <b>Filter saved!</b>\n\n"
            f"📐 Match: <b>{mlabel}</b>\n"
            f"🔡 Pattern: <code>{_h(pattern)}</code>\n"
            f"⚡ Action: {alabel}\n\n"
            f"<i>Active immediately.</i>",
            reply_markup=_kb_filter_list()
        )
    except Exception as e:
        if "UNIQUE" in str(e):
            await call.message.answer(
                f"⚠️ Pattern <code>{_h(pattern)}</code> already exists.")
        else:
            await call.message.answer(f"❌ Error saving filter: {_h(str(e))}")
    await call.answer()


# ── Delete filter ─────────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith("smsf_del_"))
async def cb_smsf_delete(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer()
    fid = int(call.data.split("smsf_del_")[1])
    row = db.cx().execute(
        "SELECT pattern, match_type, action FROM sms_filters WHERE id=?", (fid,)
    ).fetchone()
    if not row:
        return await call.answer("Filter not found.", show_alert=True)
    with db.cx() as c:
        c.execute("DELETE FROM sms_filters WHERE id=?", (fid,))
    _invalidate_cache()
    relay.db.log_action(call.from_user.id, "SMS Filter Deleted",
                        f"#{fid} {row['match_type']}/{row['action']}: {row['pattern'][:80]}")
    await call.answer(f"🗑 Filter #{fid} deleted.", show_alert=True)
    text = (
        "╔══════════════════════╗\n"
        "  🚫 <b>SMS FILTER MANAGER</b>\n"
        "╚══════════════════════╝\n\n"
        + _fmt_filter_list()
    )
    try:
        await call.message.edit_text(text[:4000], reply_markup=_kb_filter_list())
    except Exception:
        await call.message.answer(text[:4000], reply_markup=_kb_filter_list())


# ─────────────────────────────────────────────────────────────────────────────
# 11.  Entry point — run the original bot with all patches applied
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    log.info("=== sms_patch loaded — all patches active ===")
    asyncio.run(relay.main())
