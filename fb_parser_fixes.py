"""
fb_parser_fixes.py — Drop-in patch for fb_parser.py  [v2 FIXED]
================================================================
Import and apply this BEFORE calling any fb_parser functions.

Usage (at the top of office_relay.py or sms_patch.py):
    import fb_parser
    import fb_parser_fixes          # ← add this line
    fb_parser_fixes.apply(fb_parser)

What is fixed
─────────────
FIX-1  _FB_SYSTEM_KEYS expanded — prevents wrong root-key selection when the
        Firebase database has keys like "account", "bot_state", "clients",
        "Favorites", "login", "guard", "Card", etc.  Previously those were
        picked as device root keys → ALL Pattern-A probes went to a dead path.

FIX-2  latest_sms() now also tries epoch-ms integer keys (tinmm88-style databases
        use the send timestamp as the push key).  Previously _latest() fell back
        to lexicographic sort on integer keys, which picked the wrong entry when
        keys had different digit lengths (e.g. "9" > "10" lexicographically).

FIX-3  _detect_and_parse now also probes "sms_forward" root key as a standalone
        source (many Indian SMS relay apps use only sms_forward/ with no user_data).

FIX-4  Broader SMS body/sender/time field lists — covers "Msg", "Message",
        "receivedAt", "sentAt", "id" (epoch-ms push-key alternate), "content".
"""

from __future__ import annotations
import re
from typing import Any


# ─────────────────────────────────────────────────────────────
# FIX-1  Expanded system keys
# ─────────────────────────────────────────────────────────────

_EXPANDED_SYSTEM_KEYS: set[str] = {
    # original fb_parser keys
    ".indexOn", ".read", ".write", ".validate",
    "rules", ".settings", "Favorites", "backups",
    "bot_state", "users", "clients", "guard", "login",
    "bot_users", "panelAnalytics", "_scary_links",
    "profex_incoming", "nextUserId", "page2", "page4",
    "page5", "page6", "page7", "all_pas", "account",
    "user_old", "Card",
    # additional keys from office_relay.py and real-world databases
    "firebase", "metadata",
    "user_data", "user_list", "user_sms",
    "sms_forward", "sms", "messages", "All_Users",
    "smsLogs", "registeredDevices", "Sms", "csc",
    "admin", "status", "register", "history",
    "callForwarding", "callForward",
    "fcm", "fcm_tokens", "fcmTokens", "fcmDelivery", "firebase-messaging",
    "notifications", "analytics", "remoteConfig", "remote_config",
    "crashlytics", "performance", "__fbfiles__", "__storage__",
    "appCheck", "hosting", "indexes", "functions",
    "firestore", "auth", "identitytoolkit", "securetoken",
}

# FIX-4  Expanded field lists
_EXPANDED_SMS_BODY_FIELDS = (
    "body", "message", "msg", "text", "content", "sms",
    "Body", "Message", "Msg", "Text", "SMS",
)
_EXPANDED_SMS_SENDER_FIELDS = (
    "sender", "from", "ph", "address", "senderNumber",
    "from_number", "number", "Sender", "Address", "Number",
    "phoneNumber", "phone", "source",
)
_EXPANDED_SMS_TIME_FIELDS = (
    "date", "dateTime", "timestamp", "receivedDate", "recivedDate",
    "formattedTimestamp", "backupTime", "time", "datetime",
    "Date", "DateTime", "Timestamp", "ReceivedDate",
    "receivedAt", "received_at", "sentAt", "sentTime",
    "created_at", "createdAt", "id",
)


# ─────────────────────────────────────────────────────────────
# FIX-2  Patched latest_sms with integer-key epoch support
# ─────────────────────────────────────────────────────────────

def _patched_latest_sms(node: dict, body_field=None, sender_field=None, time_field=None):
    """
    FIX-2: Also sorts by integer keys numerically.
    Previously: str sort of integer keys gave wrong newest-entry selection.
    Now: int_keys sorted as integers, AND epoch-ms dict keys are tried.
    """
    if not isinstance(node, dict) or not node:
        return None, None

    def _norm(entry, bf=body_field, sf=sender_field, tf=time_field):
        bf_list = ([bf] + list(_EXPANDED_SMS_BODY_FIELDS))   if bf else _EXPANDED_SMS_BODY_FIELDS
        sf_list = ([sf] + list(_EXPANDED_SMS_SENDER_FIELDS)) if sf else _EXPANDED_SMS_SENDER_FIELDS
        body = ""
        for f in bf_list:
            v = entry.get(f)
            if v and isinstance(v, (str, int, float)):
                body = str(v).strip()
                if body:
                    break
        return body

    # 1) Firebase push-keys (start with "-") — already time-ordered lexicographically
    push_keys = sorted([k for k in node if str(k).startswith("-")], reverse=True)
    for k in push_keys:
        v = node[k]
        if isinstance(v, dict) and _norm(v):
            return v, k

    # 2) Pure-integer keys — sort NUMERICALLY (FIX-2)
    int_pairs = []
    for k in node:
        s = str(k).lstrip("-")
        if s.isdigit():
            try:
                int_pairs.append((int(s), k))
            except ValueError:
                pass
    int_pairs.sort(reverse=True)
    for _, k in int_pairs:
        v = node[k]
        if isinstance(v, dict) and _norm(v):
            return v, k

    # 3) Fallback: sort all remaining keys lexicographically
    for k in sorted(node.keys(), reverse=True):
        v = node[k]
        if isinstance(v, dict) and _norm(v):
            return v, k

    return None, None


# ─────────────────────────────────────────────────────────────
# FIX-2  Patched norm_sms with expanded field coverage
# ─────────────────────────────────────────────────────────────

def _patched_norm_sms(entry: dict,
                      body_field=None, sender_field=None, time_field=None) -> tuple:
    """FIX-4: Uses expanded field lists to cover more Firebase app variants."""
    if not isinstance(entry, dict):
        return "", "", ""
    bf = ([body_field]   + list(_EXPANDED_SMS_BODY_FIELDS))   if body_field   else _EXPANDED_SMS_BODY_FIELDS
    sf = ([sender_field] + list(_EXPANDED_SMS_SENDER_FIELDS)) if sender_field else _EXPANDED_SMS_SENDER_FIELDS
    tf = ([time_field]   + list(_EXPANDED_SMS_TIME_FIELDS))   if time_field   else _EXPANDED_SMS_TIME_FIELDS
    body = sender = ts = ""
    for f in bf:
        v = entry.get(f)
        if v and isinstance(v, (str, int, float)):
            body = str(v).strip()
            if body:
                break
    for f in sf:
        v = entry.get(f)
        if v and isinstance(v, (str, int)):
            sender = str(v).strip()
            if sender:
                break
    for f in tf:
        v = entry.get(f)
        if v:
            ts = str(v).strip()
            if ts:
                break
    return body, sender, ts


# ─────────────────────────────────────────────────────────────
# FIX-3  Patched _detect_and_parse to handle sms_forward root
# ─────────────────────────────────────────────────────────────

def _patch_detect_and_parse(fb_module: Any) -> None:
    """
    Wraps _detect_and_parse to also handle databases where
    the root has 'sms_forward' as the only meaningful key.
    Many cheap Indian SMS relay APKs use only:
        {base}/sms_forward/{device_id}/{push_key}: {sms_entry}
    These have no user_data/messages/All_Users, so they fell
    through all patterns and hit the AI fallback unnecessarily.
    """
    _orig = fb_module._detect_and_parse

    async def _wrapped(sess, base, api_key=None,
                       groq_key=None, groq_model=None, anthropic_key=None,
                       bot_token=None, admin_ids=None):
        entries, stype, err = await _orig(
            sess, base, api_key,
            groq_key, groq_model, anthropic_key, bot_token, admin_ids
        )
        if entries:
            return entries, stype, err

        # FIX-3: Try sms_forward as standalone pattern
        try:
            import aiohttp as _aiohttp
            sf_sh = await fb_module.fb_get(
                sess, f"{base}/sms_forward.json?shallow=true", api_key=api_key
            )
            if isinstance(sf_sh, dict) and sf_sh:
                DeviceEntry = fb_module.DeviceEntry
                _sys = fb_module._FB_SYSTEM_KEYS
                rows = []
                for dev_id in sf_sh:
                    if dev_id in _sys:
                        continue
                    rows.append(DeviceEntry(
                        number=f"DEV-{dev_id}", device_id=dev_id,
                        device_name=dev_id[:40], sim_slot="sim1",
                        carrier="", status="Active", struct_type="SF",
                        sms_path=f"{base}/sms_forward/{dev_id}",
                        status_path="", is_ghost=True,
                    ))
                if rows:
                    return rows, "SF", None
        except Exception:
            pass

        return entries, stype, err

    fb_module._detect_and_parse = _wrapped


# ─────────────────────────────────────────────────────────────
# APPLY ALL FIXES
# ─────────────────────────────────────────────────────────────

def apply(fb_module: Any) -> None:
    """
    Apply all fixes to a loaded fb_parser module.

    Usage:
        import fb_parser
        import fb_parser_fixes
        fb_parser_fixes.apply(fb_parser)
    """
    # FIX-1: Replace system key set
    fb_module._FB_SYSTEM_KEYS = _EXPANDED_SYSTEM_KEYS
    print("[fb_parser_fixes] FIX-1: _FB_SYSTEM_KEYS expanded ✓")

    # FIX-2 + FIX-4: Replace latest_sms and norm_sms
    fb_module.latest_sms = _patched_latest_sms
    fb_module.norm_sms   = _patched_norm_sms
    print("[fb_parser_fixes] FIX-2 + FIX-4: latest_sms + norm_sms patched ✓")

    # FIX-3: Wrap _detect_and_parse
    _patch_detect_and_parse(fb_module)
    print("[fb_parser_fixes] FIX-3: _detect_and_parse wrapped for sms_forward ✓")

    print("[fb_parser_fixes] All fixes applied — fb_parser ready.")
