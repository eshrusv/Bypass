import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import requests
import re
import time
import threading
import json
import os
import html as _html_mod
from urllib.parse import quote as _quote
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

# ============================================================
# CONFIGURATION
# ============================================================

BOT_TOKEN    = "8568752690:AAE000QuliBxQeIoK2akC4qxfJDzAdD4su0"
BOT_USERNAME = "@eshxbypass_bot"

OWNER_ID       = 8189708860
OWNER_USERNAME = "@iam_eshh"

# Default endpoint can be overridden by the admin panel and persists in MongoDB.
# The URL may be entered as a full template containing `{}`, or as a base URL;
# base URLs are normalized to `/bypass?link={}` by set_bypass_api_url().
DEFAULT_BYPASS_API_URL = os.environ.get(
    "BYPASS_API_URL",
    "https://bypass-gc.onrender.com/bypass?link={}"
)
BYPASS_API_SETTING_KEY = "bypass_api_url"

# ── Log channel (all user activity goes here) ─────────────
LOG_CHANNEL_ID   = -5540760338
LOG_CHANNEL_LINK = ""

ALLOWED_GROUP_ID       = -1003987893888
GROUP_LINK             = "https://t.me/+MWGTpHPuSrI5OGY0"
GROUP_NAME             = "Our GC"

# ── MongoDB ───────────────────────────────────────────────
# Prefer environment variables so the password isn't hardcoded in source.
# MONGO_URI          -> normal mongodb+srv:// string (default, needs working DNS SRV lookup)
# MONGO_URI_STANDARD -> non-SRV mongodb:// string with explicit shard hosts.
#                       Set this instead if your host's DNS can't resolve SRV records
#                       (this completely skips the SRV lookup that was timing out).
_MONGO_URI_SRV      = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://redoxbhai96_db_user:redox56@cluster0.jmuuyom.mongodb.net/felix?retryWrites=true&w=majority&appName=Cluster0",
)
_MONGO_URI_STANDARD = os.environ.get("MONGO_URI_STANDARD", "")
MONGO_DB  = "felix_bypass"

# Termux/Android has no /etc/resolv.conf, so dnspython (used internally by
# pymongo for mongodb+srv:// lookups) fails with NoResolverConfiguration.
# Point it at public DNS resolvers in-process, with a longer timeout so
# slow/flaky networks don't hard-fail on the very first attempt.
try:
    import dns.resolver as _dns_resolver
    _r = _dns_resolver.Resolver(configure=False)
    _r.nameservers = ["8.8.8.8", "1.1.1.1", "8.8.4.4", "9.9.9.9"]
    _r.timeout  = 5     # per-nameserver timeout
    _r.lifetime = 20    # total time before giving up across all nameservers
    _dns_resolver.default_resolver = _r
except Exception as _dns_e:
    print(f"[DNS_FIX] could not preconfigure resolver: {_dns_e}")

_IST = timezone(timedelta(hours=5, minutes=30))
def _now_ist(): return datetime.now(_IST).strftime("%d %b %Y %I:%M %p")

# ============================================================
# STYLED BUTTON
# ============================================================

class SBtn(InlineKeyboardButton):
    def __init__(self, text, style=None, icon_custom_emoji_id=None, **kwargs):
        super().__init__(text, **kwargs)
        self._style = style
        self._icon  = icon_custom_emoji_id

    def to_dict(self):
        d = super().to_dict()
        try:
            if self._style: d["style"] = self._style
        except: pass
        try:
            if self._icon: d["icon_custom_emoji_id"] = self._icon
        except: pass
        return d

# ============================================================
# MONGODB ENGINE
# ============================================================

def _make_mongo_client():
    """
    Prefer MONGO_URI_STANDARD (non-SRV, explicit hosts) when set — this
    completely avoids the DNS SRV lookup that times out on hosts with
    restricted/broken outbound DNS. Falls back to the mongodb+srv:// URI.
    """
    if _MONGO_URI_STANDARD:
        print("[MONGO] Using MONGO_URI_STANDARD (non-SRV) connection string.")
        return MongoClient(_MONGO_URI_STANDARD, serverSelectionTimeoutMS=15000)
    try:
        return MongoClient(_MONGO_URI_SRV, serverSelectionTimeoutMS=15000)
    except Exception as e:
        print(f"[MONGO] SRV connection failed: {e}")
        print("[MONGO] Your host likely can't resolve mongodb+srv:// SRV/DNS records.")
        print("[MONGO] Fix: get the non-SRV connection string from Atlas "
              "(Connect > Drivers, or run `dig SRV _mongodb._tcp.<cluster-host>` "
              "to get the shard hostnames) and set it as the MONGO_URI_STANDARD "
              "environment variable, e.g.:")
        print('  export MONGO_URI_STANDARD="mongodb://user:pass@shard1:27017,'
              'shard2:27017,shard3:27017/felix?ssl=true&replicaSet=atlas-xxxxx-shard-0'
              '&authSource=admin&retryWrites=true&w=majority"')
        raise

_mongo_client = _make_mongo_client()
_mongo_db     = _mongo_client[MONGO_DB]

# Collections
_col_users     = _mongo_db["users"]
_col_links     = _mongo_db["bypassed_links"]
_col_channels  = _mongo_db["force_channels"]
_col_welcome   = _mongo_db["welcome_settings"]
_col_settings  = _mongo_db["bot_settings"]
_col_groups    = _mongo_db["allowed_groups"]
_col_join_requests = _mongo_db["pending_join_requests"]

def _init_indexes():
    """Create indexes once at startup."""
    try:
        _col_users.create_index([("user_id", ASCENDING)], unique=True)
        _col_users.create_index([("bypass_count", DESCENDING)])
        _col_links.create_index([("original_url", ASCENDING)], unique=True)
        _col_channels.create_index([("username", ASCENDING)])
        _col_settings.create_index([("key", ASCENDING)], unique=True)
        _col_welcome.create_index([("key", ASCENDING)], unique=True)
        _col_groups.create_index([("group_id", ASCENDING)], unique=True)
        _col_join_requests.create_index(
            [("user_id", ASCENDING), ("chat_id", ASCENDING)], unique=True
        )
        _col_join_requests.create_index([("invite_link", ASCENDING)])
        print("[MONGO] ✅ Indexes ready")
    except Exception as e:
        print(f"[MONGO] Index error: {e}")

def _seed_default_channels():
    """Seed default force channels if empty."""
    if _col_channels.count_documents({}) == 0:
        _col_channels.insert_many([
            {"name": "Must Join Channel", "type": "channel", "username": "", "link": "https://t.me/+Zix8IqMIxEEyMjU1", "chat_id": -1004334801058},
        ])
        print("[MONGO] Seeded default force channels")

# ============================================================
# IN-MEMORY CACHE LAYER
# ============================================================

_cache_lock     = threading.RLock()
_settings_cache = {}
_welcome_cache  = {}
_channels_cache = None
_channels_ts    = 0.0
_CHANNELS_TTL   = 30.0

_bypass_cache         = {}
_WELCOME_HTML_GENERIC = None
_LOADING_MSGS_RAM     = []

_user_join_cache      = {}
_user_join_cache_lock = threading.Lock()
_USER_JOIN_TTL        = 120.0

_admin_cache      = {}
_admin_cache_lock = threading.Lock()
_ADMIN_TTL        = 300.0

# ── Settings ──────────────────────────────────────────────

def db_get_setting(key, default=None):
    with _cache_lock:
        if key in _settings_cache:
            return _settings_cache[key]
    doc = _col_settings.find_one({"key": key})
    val = doc["value"] if doc else default
    with _cache_lock:
        _settings_cache[key] = val
    return val

def db_set_setting(key, value):
    _col_settings.update_one(
        {"key": key},
        {"$set": {"key": key, "value": str(value)}},
        upsert=True
    )
    with _cache_lock:
        _settings_cache[key] = str(value)


def get_bypass_api_url():
    """Return the currently configured API URL, falling back to the env default."""
    return str(db_get_setting(BYPASS_API_SETTING_KEY, DEFAULT_BYPASS_API_URL) or DEFAULT_BYPASS_API_URL).strip()


def normalize_bypass_api_url(value):
    """Validate and normalize an admin-supplied endpoint URL."""
    from urllib.parse import urlparse

    value = str(value or "").strip()
    if not value:
        raise ValueError("API URL empty hai")
    if "{}" not in value:
        value = value.rstrip("/")
        if value.endswith("/bypass"):
            value += "?link={}"
        elif "?" in value:
            value += "&link={}"
        else:
            value += "/bypass?link={}"
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Sirf valid http:// ya https:// URL bhejo")
    if value.count("{}") != 1:
        raise ValueError("URL mein exactly ek {} placeholder hona chahiye")
    return value


def set_bypass_api_url(value):
    normalized = normalize_bypass_api_url(value)
    db_set_setting(BYPASS_API_SETTING_KEY, normalized)
    return normalized

# ── Welcome media ─────────────────────────────────────────

def db_get_welcome(key, default=None):
    with _cache_lock:
        if key in _welcome_cache:
            return _welcome_cache[key]
    doc = _col_welcome.find_one({"key": key})
    val = doc["value"] if doc else default
    with _cache_lock:
        _welcome_cache[key] = val
    return val

def db_set_welcome(key, value):
    _col_welcome.update_one(
        {"key": key},
        {"$set": {"key": key, "value": str(value)}},
        upsert=True
    )
    with _cache_lock:
        _welcome_cache[key] = str(value)

def db_del_welcome(key):
    _col_welcome.delete_one({"key": key})
    with _cache_lock:
        _welcome_cache.pop(key, None)

# ── Force channels ────────────────────────────────────────

def get_force_channels():
    global _channels_cache, _channels_ts
    now = time.monotonic()
    with _cache_lock:
        if _channels_cache is not None and (now - _channels_ts) < _CHANNELS_TTL:
            return _channels_cache
    docs = list(_col_channels.find({}, {"_id": 1, "name": 1, "type": 1,
                                        "username": 1, "link": 1, "chat_id": 1}))
    # Convert ObjectId to str for use in callback_data
    data = [{"id": str(d["_id"]), "name": d.get("name",""), "type": d.get("type","channel"),
              "username": d.get("username",""), "link": d.get("link",""),
              "chat_id": d.get("chat_id")} for d in docs]
    with _cache_lock:
        _channels_cache = data
        _channels_ts    = now
    return data

def _invalidate_channels():
    global _channels_cache
    with _cache_lock:
        _channels_cache = None

def add_force_channel(name, ch_type, username, link, chat_id):
    _col_channels.insert_one({
        "name": name, "type": ch_type,
        "username": username, "link": link,
        "chat_id": chat_id,
        "added_at": _now_ist()
    })
    _invalidate_channels()

def delete_force_channel(ch_id_str):
    from bson import ObjectId
    try:
        _col_channels.delete_one({"_id": ObjectId(ch_id_str)})
    except Exception as e:
        print(f"[DEL_CHANNEL] {e}")
    _invalidate_channels()

# ── Bypass link cache ─────────────────────────────────────

def _cache_entry(bypassed_url, extra=None, time_taken=0.0, source=""):
    return {"url": bypassed_url, "extra": extra or {},
            "time": time_taken, "source": source}

def cache_get_bypassed(original_url):
    """Returns full payload dict so a cached reply looks EXACTLY like a fresh one."""
    with _cache_lock:
        if original_url in _bypass_cache:
            return _bypass_cache[original_url]
    doc = _col_links.find_one({"original_url": original_url})
    if doc:
        val = _cache_entry(doc.get("bypassed_url"), doc.get("extra"),
                           doc.get("time_taken", 0.0), doc.get("source", ""))
        with _cache_lock:
            _bypass_cache[original_url] = val
        return val
    return None

def cache_save_bypassed(original_url, bypassed_url, extra=None, time_taken=0.0, source=""):
    extra = extra or {}
    _col_links.update_one(
        {"original_url": original_url},
        {"$set": {"original_url": original_url,
                  "bypassed_url": bypassed_url,
                  "extra": extra,
                  "time_taken": time_taken,
                  "source": source,
                  "saved_at": _now_ist()}},
        upsert=True
    )
    with _cache_lock:
        _bypass_cache[original_url] = _cache_entry(bypassed_url, extra, time_taken, source)

def cache_remove_bypassed(original_url):
    """Purge a single URL from both RAM cache and MongoDB. Returns True if it existed."""
    existed = False
    with _cache_lock:
        if original_url in _bypass_cache:
            del _bypass_cache[original_url]
            existed = True
    res = _col_links.delete_one({"original_url": original_url})
    if res.deleted_count:
        existed = True
    return existed

# ── Users ─────────────────────────────────────────────────

def register_user(uid, fname, uname):
    _col_users.update_one(
        {"user_id": uid},
        {"$set":     {"first_name": fname, "username": uname},
         "$setOnInsert": {"user_id": uid, "bypass_count": 0,
                          "joined_at": _now_ist()}},
        upsert=True
    )

def increment_bypass(uid):
    _col_users.update_one({"user_id": uid}, {"$inc": {"bypass_count": 1}})

def get_all_users():
    return list(_col_users.find({}, {"_id": 0}).sort("bypass_count", DESCENDING))

# ============================================================
# PREMIUM EMOJI & TEXT HELPERS
# ============================================================

def _te(eid, ch): return f'<tg-emoji emoji-id="{eid}">{ch}</tg-emoji>'
def b(t):         return f"<b>{t}</b>"
def bq(t):        return f"<blockquote>{t}</blockquote>"

_E_LOOP  = "5465629669829128119"
_E_FLOW  = "6073117703965511893"
_E_STAR  = "6086747705369957045"
_E_BOLT  = "6111583289334570419"
_E_PARTY = "6111831096062646831"
_E_SMILE = "5976721148736445255"
_E_CHAIN = "6111396350883010682"
_E_GIFT  = "5465339480363795746"
_E_CLOCK = "6285240160120477644"
_E_SAND  = "6267229004311303657"
_E_CAN   = "6226665409022527349"
_E_LOCK  = "6195245116207143870"
_E_ROCKET= "5193179501739145102"
_E_GEAR  = "5472354553662285862"
_E_INBOX = "5467415297536088297"
_E_GLOBE = "5399934661818359384"
_E_INFO  = "5226521854255936545"
_E_GROUP = "5467531326890228611"
_E_CROSS = "5407025283456835113"
_E_CHECK = "6147565374289220368"
_E_GREEN = "6120953301656670791"
_E_RED   = "6122945749870187150"
_E_MINUS = "6120775833608001118"
_E_ADD   = "6257944590687410044"
_E_BACK  = "5252294880386202873"
_E_BCAST = "5467415297536088297"
_E_FILM  = "5467369612820042920"
_E_DASH  = "5472354553662285862"
_E_USR   = "5467254337188930430"
_E_CHAN  = "5467254337188930430"
_E_TRASH = "6129486856212979482"
_E_EYE   = "6159021800119341504"
# Welcome message specific emoji
_E_SKULL = "4956461073550017373"
_E_HI    = "4958469026595472714"
_E_TEASE = "5422827632074962213"
_E_DEV   = "4958900559139570572"

# Bypass settings screen — verified premium emoji
_E_PLUS   = "4956507094124594921"  # ➕
_E_TREE   = "4956557431141303451"  # 🌲
_E_PEN    = "5258500400918587241"  # ✍️
_E_GRN2   = "5981066684977384749"  # 🟢
_E_CHK2   = "5980930633298350051"  # ✅
_E_GLB2   = "4956560549287560231"  # 🌐
_E_RED2   = "4956395910306202687"  # 🔴
_E_CRS2   = "4956612582816351459"  # ❌
_E_EYE2   = "4958808208752772190"  # 👁
_E_BELL   = "4956290155326473271"  # 🔔
_E_LOCK2  = "5879895758202735862"  # 🔒
_E_GRP2   = "5942877472163892475"  # 👥

def pe_loop():  return _te(_E_LOOP,  "➿")
def pe_flow():  return _te(_E_FLOW,  "💐")
def pe_star():  return _te(_E_STAR,  "🌟")
def pe_bolt():  return _te(_E_BOLT,  "⚡")
def pe_party(): return _te(_E_PARTY, "🥳")
def pe_smile(): return _te(_E_SMILE, "😀")
def pe_chain(): return _te(_E_CHAIN, "⛓")
def pe_gift():  return _te(_E_GIFT,  "🎁")
def pe_clock(): return _te(_E_CLOCK, "⏰")
def pe_sand():  return _te(_E_SAND,  "⌛")
def pe_can():   return _te(_E_CAN,   "🥫")
def pe_lock():  return _te(_E_LOCK,  "🔒")
def pe_gear():  return _te(_E_GEAR,  "⚙️")
def pe_info():  return _te(_E_INFO,  "ℹ️")
def pe_group(): return _te(_E_GROUP, "👥")
def pe_check(): return _te(_E_CHECK, "✅")
def pe_cross(): return _te(_E_CROSS, "❌")
def pe_green(): return _te(_E_GREEN, "🟢")
def pe_red():   return _te(_E_RED,   "🔴")
def pe_trash(): return _te(_E_TRASH, "🗑")
def pe_eye():   return _te(_E_EYE,   "👁")
def pe_rocket():return _te(_E_ROCKET,"🚀")

_PE_HEART = _te("5422581569103626576", "❤️")
_PE_GIFTH = _te("5425035421358781637", "🎁")
_PE_CROWN = _te("6226410236425543060", "👑")
_PE_STAR2 = _te("6118457255642799940", "🌟")

_LOOP_SINGLE = _te("5465629669829128119", "➿")
# Decorative loop rows now use PREMIUM tg-emoji for consistent branding
_LOOP9       = _LOOP_SINGLE * 9
_HEADER_MID  = f"{_PE_HEART} {b('Tʜᴇ Fᴇʟɪx Bʏᴩᴀꜱꜱ Bᴏᴛ')} {_PE_GIFTH}"
_HEADER      = f"{_LOOP9}\n{_HEADER_MID}\n{_LOOP9}"
_FOOTER      = f"{_PE_CROWN} {b('Pᴏᴡᴇʀᴇᴅ Bʏ:')} {b('@felixbypass_bot')} {_PE_STAR2}\n{_LOOP9}"

_SC = {
    'A':'ᴀ','B':'ʙ','C':'ᴄ','D':'ᴅ','E':'ᴇ','F':'ꜰ','G':'ɢ','H':'ʜ','I':'ɪ','J':'ᴊ',
    'K':'ᴋ','L':'ʟ','M':'ᴍ','N':'ɴ','O':'ᴏ','P':'ᴩ','Q':'Q','R':'ʀ','S':'ꜱ','T':'ᴛ',
    'U':'ᴜ','V':'ᴠ','W':'ᴡ','X':'x','Y':'ʏ','Z':'ᴢ',
}

def _st(text):
    result = []; i = 0; n = len(text); word_start = True
    while i < n:
        if text[i] == '<':
            j = text.find('>', i)
            if j == -1: result.append(text[i]); i += 1
            else: result.append(text[i:j+1]); i = j+1; word_start = True
            continue
        if text[i] == '@' or text[i:i+4] == 'http':
            j = i
            while j < n and text[j] not in (' ', '\n', '\t'): j += 1
            result.append(text[i:j]); i = j; word_start = True
            continue
        ch = text[i]
        if ch.isalpha():
            up = ch.upper()
            if word_start:
                result.append(ch); word_start = False
            else:
                result.append(_SC.get(up, ch))
        else:
            result.append(ch)
            word_start = ch in (' ', '\n', '\t', ':', '-', '|')
        i += 1
    return ''.join(result)

# ============================================================
# LOADING ANIMATION — PRE-RENDERED
# ============================================================

PROGRESS_STEPS = [
    ("▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱",  "0%",  _te("6267229004311303657","⌛"), "ɪɴɪᴛɪᴀʟɪꜱɪɴɢ..."),
    ("▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱",  "6%",  _te("6111583289334570419","⚡"), "ꜰᴇᴛᴄʜɪɴɢ ʟɪɴᴋ..."),
    ("▰▰▱▱▱▱▱▱▱▱▱▱▱▱▱", "12%",  _te("6111396350883010682","⛓"), "ꜱᴄᴀɴɴɪɴɢ..."),
    ("▰▰▰▱▱▱▱▱▱▱▱▱▱▱▱", "18%",  _te("6195245116207143870","🔒"), "ʙʏᴩᴀꜱꜱɪɴɢ ꜱᴇᴄᴜʀɪᴛʏ..."),
    ("▰▰▰▰▱▱▱▱▱▱▱▱▱▱▱", "25%",  _te("6195245116207143870","🔒"), "ᴄʀᴀᴄᴋɪɴɢ ᴛᴏᴋᴇɴ..."),
    ("▰▰▰▰▰▱▱▱▱▱▱▱▱▱▱", "31%",  _te("6073117703965511893","💐"), "ᴅᴇᴄᴏᴅɪɴɢ..."),
    ("▰▰▰▰▰▰▱▱▱▱▱▱▱▱▱", "37%",  _te("6111583289334570419","⚡"), "ᴩʀᴏᴄᴇꜱꜱɪɴɢ..."),
    ("▰▰▰▰▰▰▰▱▱▱▱▱▱▱▱", "43%",  _te("6195245116207143870","🔒"), "ʙʏᴩᴀꜱꜱɪɴɢ ꜱᴇᴄᴜʀɪᴛʏ..."),
    ("▰▰▰▰▰▰▰▰▱▱▱▱▱▱▱", "50%",  _te("5465629669829128119","➿"), "ʜᴀʟꜰᴡᴀʏ ᴛʜᴇʀᴇ..."),
    ("▰▰▰▰▰▰▰▰▰▱▱▱▱▱▱", "56%",  _te("6086747705369957045","🌟"), "ᴠᴇʀɪꜰʏɪɴɢ..."),
    ("▰▰▰▰▰▰▰▰▰▰▱▱▱▱▱", "62%",  _te("6111396350883010682","⛓"), "ᴜɴʟᴏᴄᴋɪɴɢ..."),
    ("▰▰▰▰▰▰▰▰▰▰▰▱▱▱▱", "68%",  _te("6111583289334570419","⚡"), "ᴀʟᴍᴏꜱᴛ ᴅᴏɴᴇ..."),
    ("▰▰▰▰▰▰▰▰▰▰▰▰▱▱▱", "75%",  _te("6073117703965511893","💐"), "ꜰɪɴᴀʟɪꜱɪɴɢ..."),
    ("▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱", "87%",  _te("6086747705369957045","🌟"), "ᴄᴏᴍᴩʟᴇᴛɪɴɢ..."),
    ("▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱", "93%",  _te("5465339480363795746","🎁"), "ᴡʀᴀᴩᴩɪɴɢ ᴜᴩ..."),
]

def _render_loading_step(bar, pct, emoji, label):
    return bq(f"{emoji} {b(label)}\n<code>{bar}  {pct}</code>")

def _build_loading_cache():
    global _LOADING_MSGS_RAM
    msgs = [_render_loading_step(*step) for step in PROGRESS_STEPS]
    _LOADING_MSGS_RAM = msgs
    print(f"[LOADING_CACHE] ✅ Built {len(msgs)} loading steps")

def get_loading_msg(idx):
    if _LOADING_MSGS_RAM:
        return _LOADING_MSGS_RAM[min(idx, len(_LOADING_MSGS_RAM) - 1)]
    step = PROGRESS_STEPS[min(idx, len(PROGRESS_STEPS) - 1)]
    return _render_loading_step(*step)

# ============================================================
# WELCOME MESSAGE
# ============================================================

def _build_welcome_template():
    global _WELCOME_HTML_GENERIC
    _WELCOME_HTML_GENERIC = (
        f"<blockquote>"
        f"{_te(_E_FLOW,'💐')} <b>𝗧ʜᴇ 𝗘ꜱʜ 𝗕ʏᴩᴀꜱꜱ 𝗕ᴏᴛ</b> {_te(_E_STAR,'🌟')}\n"
        f"{_LOOP_SINGLE * 10}\n"
        f"{_te(_E_SKULL,'💀')}{_te(_E_HI,'👋')} <b>𝗛ᴇʟʟᴏ 殺┋ <a href='tg://user?id={{uid}}'>{{fname}}</a> !!</b>\n\n"
        f"{_te(_E_TEASE,'😝')} <b>𝚂ʜᴏʀᴛɴᴇʀ ʟɪɴᴋ ʙʏᴩᴀꜱꜱ ʙᴏᴛ!</b>\n\n"
        f"{_te(_E_GIFT,'🎁')} Sᴇɴᴅ Yᴏᴜʀ Sʜᴏʀᴛᴇɴᴇʀ Lɪɴᴋ\n"
        f"{_te(_E_BOLT,'⚡')} Fʀᴇᴇ Uɴʟɪᴍɪᴛᴇᴅ Uꜱᴇꜱ\n"
        f"{_te(_E_CHAIN,'⛓')} Pʀᴇᴍɪᴜᴍ Bʏᴩᴀꜱꜱ Sᴜᴩᴩᴏʀᴛ\n\n"
        f"{_LOOP_SINGLE * 10}\n"
        f"{_te(_E_GIFT,'🎁')} ᴇxᴀᴍᴩʟᴇ :- <code>https://just2earn.com/qXekdh</code>\n"
        f"{_LOOP_SINGLE * 10}\n"
        f"{_te(_E_DEV,'👨‍💻')} ᴅᴇᴠ : @iam_eshh {_te(_E_CHECK,'✅')}"
        f"</blockquote>"
    )

def get_welcome_message(uid, name):
    global _WELCOME_HTML_GENERIC
    if _WELCOME_HTML_GENERIC is None:
        _build_welcome_template()
    fname = _html_mod.escape(name)
    return _WELCOME_HTML_GENERIC.format(uid=uid, fname=fname)

def _safe_caption(text, limit=1024):
    stripped = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', '', text, flags=re.DOTALL)
    stripped = re.sub(r'<[^>]+>', '', stripped)
    if len(stripped) <= limit:
        return text
    return stripped[:limit]

def get_welcome_buttons():
    mk = InlineKeyboardMarkup(row_width=2)
    mk.add(SBtn("ᴜꜱᴇ ʜᴇʀᴇ", style="primary",  icon_custom_emoji_id="6257944590687410044", url="https://t.me/+MWGTpHPuSrI5OGY0"))
    mk.row(
        SBtn("ᴏᴡɴᴇʀ", style="primary",  icon_custom_emoji_id="5427328491513224187", url="https://t.me/iam_eshh"),
        SBtn("ᴜᴩᴅᴀᴛᴇ", style="primary",  icon_custom_emoji_id="5260691173541970181", url="https://t.me/+Zix8IqMIxEEyMjU1"),
    )
    return mk

# 🔥 Message effect animation (private chats only)
# Official Telegram message effect IDs — inhi 5 ke ilawa koi id valid nahi hai.
_FIRE_EFFECT_ID = "5046509860389126442"   # 🎉 party popper
_EFFECT_IDS = [
    "5046509860389126442",  # 🎉 party popper  ← main effect
    "5104841245755180586",  # 🔥 fire
    "5107584321108051014",  # 👍 thumbs up
    "5044134455711629726",  # ❤️ heart
]


_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

def _raw_send(method, payload):
    """Direct Bot API call — library version se independent, effect guaranteed."""
    try:
        r = requests.post(f"{_API_BASE}/{method}", json=payload, timeout=20)
        j = r.json()
        if j.get("ok"):
            return j["result"]
        print(f"[EFFECT raw {method}] {j.get('description')}")
    except Exception as e:
        print(f"[EFFECT raw {method}] {e}")
    return None

def _send_effect_raw(chat_id, *, text=None, caption=None, photo=None, video=None, markup=None):
    """Private chat me effect ke sath bhejo (raw API). Group me effect allowed nahi."""
    try:
        if int(chat_id) < 0:   # group/channel → effects unsupported
            return None
    except Exception:
        return None
    if video:
        method, base = "sendVideo", {"chat_id": chat_id, "video": video, "caption": caption, "parse_mode": "HTML"}
    elif photo:
        method, base = "sendPhoto", {"chat_id": chat_id, "photo": photo, "caption": caption, "parse_mode": "HTML"}
    else:
        method, base = "sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                                       "link_preview_options": {"is_disabled": True}}
    if markup is not None:
        try:
            base["reply_markup"] = markup.to_dict()
        except Exception:
            pass
    for _eid in _EFFECT_IDS:
        res = _raw_send(method, {**base, "message_effect_id": _eid})
        if res:
            return res
    return None

def _send_with_effect(fn, *args, **kwargs):
    """Try each valid effect id; agar koi bhi na chale to bina effect ke bhejo."""
    for _eid in _EFFECT_IDS:
        try:
            return fn(*args, message_effect_id=_eid, **kwargs)
        except TypeError as _te:
            # Installed pyTelegramBotAPI is too old for message_effect_id
            print(f"[EFFECT] not supported by library: {_te}")
            break
        except Exception as _e:
            print(f"[EFFECT {_eid}] rejected: {_e}")
            continue
    try:
        return fn(*args, **kwargs)
    except Exception as _e2:
        print(f"[EFFECT] plain send failed: {_e2}")
        return None


def send_welcome(chat_id, uid, name):
    """Send welcome — video → photo → text fallback."""
    msg      = get_welcome_message(uid, name)
    video_id = db_get_welcome("video_file_id")
    image_id = db_get_welcome("image_file_id")
    markup   = get_welcome_buttons()
    caption  = _safe_caption(msg)
    sent     = None
    # 1) Raw Bot API with animation effect (private chat) — sabse pehle try
    if video_id:
        sent = _send_effect_raw(chat_id, video=video_id, caption=caption, markup=markup)
    if not sent and image_id:
        sent = _send_effect_raw(chat_id, photo=image_id, caption=caption, markup=markup)
    if not sent:
        sent = _send_effect_raw(chat_id, text=msg, markup=markup)
    # 2) Library path (effect if supported, warna plain)
    if not sent and video_id:
        try:
            sent = _send_with_effect(bot.send_video, chat_id, video_id, caption=caption, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            print(f"[send_welcome video] {e}")
            if "file_id" in str(e).lower() or "wrong file" in str(e).lower() or "bad request" in str(e).lower():
                db_del_welcome("video_file_id")
    if not sent and image_id:
        try:
            sent = _send_with_effect(bot.send_photo, chat_id, image_id, caption=caption, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            print(f"[send_welcome photo] {e}")
            if "file_id" in str(e).lower() or "wrong file" in str(e).lower() or "bad request" in str(e).lower():
                db_del_welcome("image_file_id")
    if not sent:
        try:
            sent = _send_with_effect(bot.send_message, chat_id, msg, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            print(f"[send_welcome text] {e}")

    # Final guarantees: no effect, then no markup, then plain text.
    if not sent:
        try:
            sent = _orig_send_message(chat_id, msg, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            print(f"[send_welcome plain-effect] {e}")
    if not sent:
        try:
            sent = _orig_send_message(chat_id, msg, parse_mode="HTML")
        except Exception as e:
            print(f"[send_welcome no-markup] {e}")
    if not sent:
        try:
            _plain = re.sub(r"<[^>]+>", "", msg)
            sent = _orig_send_message(chat_id, _plain)
        except Exception as e:
            print(f"[send_welcome last] {e}")
    return sent


# ============================================================
# OUTPUT FORMAT
# ============================================================

_E_ROCKET = "5193179501739145102"

def _bypass_markup(links_dict):
    """
    links_dict: {"bypassed": url, "instant_dl": url, "telegram": url, "direct": url}
    Ek ya zyada buttons — jo bhi links available hain.
    """
    if not links_dict:
        return None
    if isinstance(links_dict, str):
        # backward compat: agar koi purana caller string pass kare
        links_dict = {"bypassed": links_dict}

    mk = InlineKeyboardMarkup(row_width=1)
    _defs = [
        ("bypassed",   "ᴏᴩᴇɴ ʙʏᴩᴀꜱꜱᴇᴅ ʟɪɴᴋ",    "success", _E_ROCKET),
        ("instant_dl", "⚡ ɪɴꜱᴛᴀɴᴛ ᴅᴏᴡɴʟᴏᴀᴅ",   "success", _E_BOLT),
        ("telegram",   "📨 ᴛᴇʟᴇɢʀᴀᴍ ʟɪɴᴋ",      "primary", _E_CHAIN),
        ("direct",     "⬇️ ᴅɪʀᴇᴄᴛ ᴅᴏᴡɴʟᴏᴀᴅ",   "primary", _E_ROCKET),
    ]
    seen = set()
    for key, label, style, icon in _defs:
        url = links_dict.get(key)
        if not url or url in seen:
            continue
        seen.add(url)
        mk.add(SBtn(label, style=style, icon_custom_emoji_id=icon, url=url))

    # extra numbered links (bypassed_2, direct_3, ...) — inke bhi buttons
    _known = {k for k, _, _, _ in _defs}
    _i = 1
    for key, url in links_dict.items():
        if key in _known or key == "original" or not url or url in seen:
            continue
        if not str(url).startswith("http"):
            continue
        seen.add(url)
        _i += 1
        mk.add(SBtn(f"⬇️ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋ {_i}", style="primary",
                    icon_custom_emoji_id=_E_ROCKET, url=url))
    return mk if seen else None
_HOST_NAMES = [
    ("pixeldrain",           "Pixeldrain"),
    ("googleusercontent",    "10Gbps Server"),
    ("storage.googleapis",   "FSL Server"),
    ("zipdisk",              "ZipDisk Server"),
    ("pixeldra",             "Pixeldrain"),
    ("gofile",               "GoFile Server"),
    ("t.me",                 "TG Link"),
    ("telegram",             "TG Link"),
    ("workers.dev",          "Cloudflare"),
    ("buzzheavier",          "BuzzHeavier"),
    ("gdflix",               "GDFlix"),
    ("hubcloud",             "HubCloud"),
    ("fastdl",               "Fast Server"),
    ("drive.google",         "Google Drive"),
    ("mediafire",            "MediaFire"),
    ("mega.nz",              "Mega"),
    ("1024tera",             "TeraBox"),
    ("terabox",              "TeraBox"),
    ("pixeld",               "Pixeldrain"),
]


def _link_short_name(url):
    """URL ke host se readable short name banata hai (Pixeldrain, GoFile Server...)."""
    u = str(url or "")
    low = u.lower()
    for frag, name in _HOST_NAMES:
        if frag in low:
            return name
    try:
        host = low.split("//", 1)[-1].split("/", 1)[0].split("?", 1)[0]
        host = host.replace("www.", "")
        parts = [p for p in host.split(".") if p not in ("com", "net", "org", "io", "co", "cc", "eu", "in", "to", "xyz")]
        if parts:
            return parts[-1].capitalize() + " Server"
    except Exception:
        pass
    return "Download Link"



def format_output(original_url, bypassed_url, time_taken, usage_count,
                  retry_count, user_name=None, from_cache=False,
                  source="", extra=None):
    """
    extra = {
        "file":  {"name": ..., "size": ...}  or None,
        "links": {"bypassed": ..., "instant_dl": ..., "telegram": ..., "direct": ...}
    }
    bypassed_url = best single URL (for cache / fail-check)
    """
    extra      = extra or {}
    file_info  = extra.get("file") or {}
    links_dict = extra.get("links") or {}

    fail_kw = ["failed", "error", "timeout", "cannot", "❌"]
    bypassed_url = str(bypassed_url or "")
    _err_reason = ""
    if bypassed_url.startswith("ERR::"):
        _err_reason  = bypassed_url[5:].strip() or "Link Not Supported"
        bypassed_url = ""
    is_fail = (
        not bypassed_url or
        original_url.rstrip("/") == bypassed_url.rstrip("/") or
        any(kw in bypassed_url.lower() for kw in fail_kw)
    )
    orig_safe = _html_mod.escape(original_url)

    # ── New branded layout premium emoji ────────────────────────────────
    _n_loop   = _te("5465629669829128119", "➿")
    _n_heart  = _te("5422581569103626576", "❤️")
    _n_gift_h = _te("5425035421358781637", "🎁")
    _n_chain  = _te("6111396350883010682", "⛓")
    _n_gift_r = _te("5384490809825450636", "🎁")
    _n_rocket = _te("6203911118964924015", "🚀")
    _n_crown  = _te("6226410236425543060", "👑")
    _n_star   = _te("6118457255642799940", "🌟")
    _n_red    = _te("6116324271804387654", "🔴")

    _n_line   = _n_loop * 9
    _n_header = (f"{_n_line}\n{_n_heart} {b(_st('The Felix Bypass Bot'))} {_n_gift_h}\n"
                 f"{_n_line}")
    _n_footer = (f"{_n_line}\n{_n_crown} {b(_st('Powered By:'))} "
                 f"{b('@felixbypass_bot')} {_n_star}\n{_n_line}")

    if is_fail:
        inner  = f"{_n_header}\n\n"
        inner += f"{_n_chain} {b(_st('Original :'))}\n{b(orig_safe)}\n\n"
        inner += f"{_n_chain} {b(_st('Bypassed :'))}\n"
        inner += f"{_n_red}{b(_st('Error  :- ' + _html_mod.escape(_err_reason or 'Link Not Supported')))}\n\n"
        inner += _n_footer
        return bq(inner), None
    else:
        _n_book = _te("5373098009640836781", "📚")
        _n_wow  = _te("5294544118653937093", "🤩")

        # ── Collect unique links in order (name = host se short name) ────────
        shown_urls = set()
        _items = []  # (label, url)
        for key in list(links_dict.keys()):
            lurl = str(links_dict.get(key) or "")
            if not lurl or lurl in shown_urls:
                continue
            shown_urls.add(lurl)
            _items.append((_link_short_name(lurl), lurl))
        if not _items and bypassed_url:
            _items.append((_link_short_name(bypassed_url), bypassed_url))


        inner  = f"{_n_header}\n\n"

        multi = len(_items) > 1
        if multi:
            if file_info.get("name"):
                inner += f"{_n_book} {b(_st('File Name  :- ' + _html_mod.escape(str(file_info['name']))))}\n\n"
            if file_info.get("size"):
                inner += f"{_n_wow} {b(_st('File Size :- ' + _html_mod.escape(str(file_info['size']))))}\n\n"

        inner += f"{_n_chain} {b(_st('Original :'))}\n{b(orig_safe)}\n\n"

        if multi:
            _parts = [
                f'<a href="{_html_mod.escape(u, quote=True)}">{b(_st(lb))}</a>'
                for lb, u in _items
            ]
            inner += f"{_n_chain} {b(_st('Bypassed  :'))}\n" + " | ".join(_parts) + "\n\n"
        else:
            lb, u = _items[0]
            inner += f"{_n_chain} {b(_st('Bypassed :'))}\n{b(_html_mod.escape(u))}\n\n"

        if user_name:
            inner += f"{_n_gift_r}{b(_st('Requested By: ' + _html_mod.escape(user_name)))}\n"
        inner += f"{_n_rocket}{b(_st('Time : ' + str(time_taken) + 's'))}\n"

        inner += _n_footer

        # Markup: only ONE button (first/primary link)
        mk = _bypass_markup({"bypassed": _items[0][1]}) if _items else None
        return bq(inner), mk

# ============================================================
# FORCE JOIN
# ============================================================

def _bot_is_admin(identifier):
    now = time.monotonic()
    with _admin_cache_lock:
        if identifier in _admin_cache:
            result, ts = _admin_cache[identifier]
            if (now - ts) < _ADMIN_TTL:
                return result
    try:
        bid    = _get_bot_id()
        member = bot.get_chat_member(identifier, bid)
        result = member.status in ["administrator", "creator"]
    except:
        result = False
    with _admin_cache_lock:
        _admin_cache[identifier] = (result, now)
    return result

def _request_invite_url(join_request):
    invite = getattr(join_request, "invite_link", None)
    return str(getattr(invite, "invite_link", "") or "").strip().rstrip("/")


def _has_pending_join_request(user_id, chat):
    """Return True when this user has a recently observed pending request.

    Telegram's Bot API does not expose a method to list all pending requests.
    Therefore the bot records chat_join_request updates and never approves them.
    """
    chat_id = chat.get("chat_id")
    invite_link = str(chat.get("link", "") or "").strip().rstrip("/")
    query = {"user_id": user_id, "$or": []}
    if chat_id:
        query["$or"].append({"chat_id": chat_id})
    if invite_link:
        query["$or"].append({"invite_link": invite_link})
    if not query["$or"]:
        return False
    # A request can be declined after it is recorded; avoid treating old
    # records as valid forever when the bot cannot query Telegram's list.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    query["requested_at"] = {"$gte": cutoff}
    return _col_join_requests.find_one(query, {"_id": 1}) is not None


def check_user_joined_all(user_id):
    not_joined = []
    channels = get_force_channels()
    if not channels:
        return not_joined

    def _check_one(chat):
        try:
            identifier = chat["username"] if chat["username"] else chat["chat_id"]
            if not identifier: return None
            # For private channels, a pending join request is sufficient for
            # verification. The request remains pending; it is never approved.
            if _has_pending_join_request(user_id, chat):
                return None
            if not _bot_is_admin(identifier): return None
            member = bot.get_chat_member(identifier, user_id)
            if member.status in ["left", "kicked"]:
                return chat
        except Exception as e:
            print(f"[check_join] {chat.get('username','?')}: {e}")
        return None

    from concurrent.futures import as_completed as _asc
    with ThreadPoolExecutor(max_workers=min(len(channels), 5)) as _ex:
        futures = {_ex.submit(_check_one, ch): ch for ch in channels}
        for fut in _asc(futures, timeout=8):
            try:
                r = fut.result()
                if r: not_joined.append(r)
            except: pass
    return not_joined

def check_user_joined_all_cached(user_id):
    now = time.monotonic()
    with _user_join_cache_lock:
        if user_id in _user_join_cache:
            result, ts = _user_join_cache[user_id]
            if (now - ts) < _USER_JOIN_TTL:
                return result
    result = check_user_joined_all(user_id)
    with _user_join_cache_lock:
        _user_join_cache[user_id] = (result, now)
    return result

def is_user_definitely_joined(user_id):
    with _user_join_cache_lock:
        if user_id in _user_join_cache:
            result, ts = _user_join_cache[user_id]
            if (time.monotonic() - ts) < _USER_JOIN_TTL:
                return result == []
    return False

def _invalidate_user_join_cache(user_id):
    with _user_join_cache_lock:
        _user_join_cache.pop(user_id, None)

_E_JOIN   = "5244681343643713903"
_E_VERIFY = "5465339480363795746"

def force_join_markup(not_joined_chats, user_id, chat_id, message_id=None):
    markup = InlineKeyboardMarkup(row_width=1)
    for chat in not_joined_chats:
        markup.add(SBtn(f"ᴊᴏɪɴ  {chat['name']}", style="primary",
                        icon_custom_emoji_id=_E_JOIN, url=chat["link"]))
    verify_data = f"verify_{user_id}_{chat_id}"
    if message_id:
        verify_data += f"_{message_id}"
    markup.add(SBtn("ᴠᴇʀɪꜰʏ  ᴊᴏɪɴᴇᴅ", style="success",
                    icon_custom_emoji_id=_E_VERIFY, callback_data=verify_data))
    return markup

def get_force_join_message(name, not_joined_chats):
    chats_text = "\n".join(
        f"{pe_gift()} {b(_st('Channel') if c['type'] == 'channel' else _st('Group'))} {b(c['name'])}"
        for c in not_joined_chats
    )
    inner  = f"{_HEADER}\n\n"
    inner += f"{pe_lock()} {b('Access Restricted!')}\n\n"
    inner += f"{b('Hello ' + name)}\n"
    inner += f"{b('Join our channels to use this bot.')}\n\n"
    inner += f"{_LOOP9}\n\n{chats_text}\n\n"
    inner += f"{_LOOP9}\n{_FOOTER}"
    return bq(inner)

# ============================================================
# DM / WRONG GROUP BLOCK MESSAGES
# ============================================================

_PE_LOOP2 = f'<tg-emoji emoji-id="5465629669829128119">➿</tg-emoji>'
_PE_LOCK2 = f'<tg-emoji emoji-id="6195245116207143870">🔒</tg-emoji>'
_PE_ARR2  = f'<tg-emoji emoji-id="5201892882281162850">↘️</tg-emoji>'
_LOOP9_2  = _LOOP_SINGLE * 9  # premium tg-emoji decoration (matching main loop style)

def _wrong_group_message():
    mid = f'<b>{_PE_LOCK2} Tʜɪꜱ Bᴏᴛ Oɴʟʏ Wᴏʀᴋꜱ Iɴ Tʜɪꜱ Gʀᴏᴜᴩ {_PE_ARR2}</b>'
    return f'<blockquote>{_LOOP9_2}\n{mid}\n{_LOOP9_2}</blockquote>'

def _wrong_group_markup():
    mk = InlineKeyboardMarkup(row_width=1)
    mk.add(SBtn("ᴜꜱᴇ ʜᴇʀᴇ", style="primary",
                icon_custom_emoji_id="6170163662544707668", url=GROUP_LINK))
    return mk

def _dm_blocked_message():
    mid = f'<b>{_PE_LOCK2} PRIVATE BYPASS BLOCKED {_PE_ARR2}</b>'
    return f'<blockquote>{_LOOP9_2}\n{mid}\n{_LOOP9_2}</blockquote>'

def _dm_blocked_markup():
    mk = InlineKeyboardMarkup(row_width=1)
    mk.add(SBtn("ᴜꜱᴇ ʜᴇʀᴇ", style="primary",
                icon_custom_emoji_id="6170163662544707668", url=GROUP_LINK))
    return mk

# ============================================================
# HTTP SESSION — POOLED
# ============================================================

_SESSION = requests.Session()
_SESSION.headers.update({"Connection": "keep-alive"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=100, pool_maxsize=200, max_retries=0
)
_SESSION.mount("http://",  _adapter)
_SESSION.mount("https://", _adapter)

# ============================================================
# BYPASS CORE
# ============================================================

def extract_url(text):
    urls = re.findall(r"https?://[^\s]+", text)
    return urls[0] if urls else None

def is_bypassable_url(url):
    """
    Ab koi bhi blocklist nahi hai — instagram / facebook / youtube / twitter
    sab kuch allowed. Sirf valid http(s) URL hona chahiye.
    """
    u = str(url or "").strip().lower()
    return u.startswith("http://") or u.startswith("https://")


# ── Group trigger commands ───────────────────────────────────
# Group me link tabhi bypass hoga jab message in me se kisi word se start ho:
#   /bypass <url>   |   /b <url>   |   b <url>   |   bypass <url>
# (bot username suffix bhi chalega: /b@felix_bypass_bot <url>)
_TRIGGER_RE = re.compile(
    r"^\s*[/!.]?\s*(bypass|b)\s*(?:@[\w_]+)?\s*(?::|-)?\s+(?=\S)",
    re.IGNORECASE,
)

def _trigger_text(text):
    """
    Agar message trigger word se start hota hai to (True, baaki_text) warna (False, text).
    """
    t = str(text or "")
    m = _TRIGGER_RE.match(t)
    if m:
        return True, t[m.end():].strip()
    # sirf "b" / "/b" / "/bypass" (reply ke saath use hota hai)
    if re.fullmatch(r"\s*[/!.]?\s*(bypass|b)\s*(?:@[\w_]+)?\s*", t, re.IGNORECASE):
        return True, ""
    return False, t

def bypass_url_with_retry(url):
    # ── Timeout note ──────────────────────────────────────────────────────────
    # b4pass.onrender.com/bypass:
    #   • DZHQ (group) + Alex run in parallel race — jo pehle de wo return
    #   • DZHQ: 15s no-response timeout, phir infinite wait jab tak bot active
    #   • Alex: 60s HTTP timeout
    #   • API max wait ≈ 75s (Alex fallback)
    #   • No API key required
    #
    # API Response formats:
    #   SUCCESS 200: { "status": true, "source": "dzhq"/"alex:module",
    #                  "response_ms": "1234ms",
    #                  "links": { "original", "bypassed", "instant_dl",
    #                             "telegram", "direct" },
    #                  "file": { "name": ..., "size": ... }  ← optional }
    #   FAIL    422: { "status": false, "message": "Both failed",
    #                  "dzhq": "...", "alex": "...", "response_ms": "..." }
    #   FAIL    400: { "status": false, "message": "Missing 'link' parameter" }
    #   FAIL    503: { "status": false, "message": "Telegram not connected" }
    # ─────────────────────────────────────────────────────────────────────────
    total_start = time.time()
    try:
        api_url = get_bypass_api_url().format(_quote(url, safe=""))
        # timeout=(connect, read)
        # connect = 15s  — agar server reachable nahi to jaldi fail ho
        # read   = None  — INFINITE wait. Bot khud se kabhi timeout nahi karega;
        #          timeout sirf tab jab API khud timeout response bhejta hai.
        resp    = _SESSION.get(api_url, timeout=(15, None))

        elapsed = round(time.time() - total_start, 2)

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            return "ERR::API Invalid Response", True, 1, elapsed, "", {}

        sc = resp.status_code

        # ── SUCCESS ───────────────────────────────────────────────────────────
        # New API shape:
        #   {"status": true, "source": "nick"/"dzhq"/"alex_bot", "module": ...,
        #    "account": ..., "response_ms": "123ms",
        #    "url": "<best single link>"                  ← top-level (naya)
        #    "file": {"name":..,"size":..}                ← optional
        #    "links": {"original": ..., "bypassed": str | [str, ...]}}
        _ok = (sc == 200) and (data.get("status") is not False)
        if _ok:
            raw_links = dict(data.get("links") or {})
            # Agar links dict me bypassed nahi hai to top-level fallback keys use karo
            if not any(k for k in raw_links if k != "original"):
                for _fk in ("url", "bypassed", "bypassed_url", "result",
                            "download", "direct", "instant_dl", "telegram"):
                    _fv = data.get(_fk)
                    if _fv:
                        raw_links["bypassed" if _fk == "url" else _fk] = _fv
                        break
            # top-level "url" ko bhi merge karo agar links me na ho
            if data.get("url") and not raw_links.get("bypassed"):
                raw_links["bypassed"] = data["url"]
            # API kabhi-kabhi ek key ke andar LIST bhejta hai
            # (e.g. "bypassed": [url1, url2]) — usko flatten karo
            links = {}
            for _k, _v in raw_links.items():
                if isinstance(_v, (list, tuple)):
                    _i = 0
                    for _item in _v:
                        if isinstance(_item, dict):
                            _item = (_item.get("url") or _item.get("link")
                                     or _item.get("download") or "")
                        if not _item:
                            continue
                        _i += 1
                        links[_k if _i == 1 else f"{_k}_{_i}"] = _item
                elif isinstance(_v, dict):
                    _u = _v.get("url") or _v.get("link") or _v.get("download") or ""
                    if _u:
                        links[_k] = _u
                else:
                    links[_k] = _v
            # Keep ALL links returned by API — show every one to user
            # Priority for "best single URL" used in cache:
            best = ""
            for _bk in ("bypassed", "instant_dl", "direct", "telegram"):
                for _k, _v in links.items():
                    if _k == _bk or _k.startswith(_bk + "_"):
                        if _v and str(_v).startswith("http"):
                            best = str(_v)
                            break
                if best:
                    break
            # Filter links to only valid http URLs (exclude original)
            clean_links = {}
            _seen_urls = set()
            _order = ["bypassed", "instant_dl", "direct", "telegram"]
            _keys = sorted(
                [k for k in links if k != "original"],
                key=lambda k: (
                    _order.index(k.rsplit("_", 1)[0] if k.rsplit("_", 1)[-1].isdigit() else k)
                    if (k.rsplit("_", 1)[0] if k.rsplit("_", 1)[-1].isdigit() else k) in _order
                    else 99,
                    int(k.rsplit("_", 1)[-1]) if k.rsplit("_", 1)[-1].isdigit() else 1,
                ),
            )
            for k in _keys:
                v = str(links.get(k) or "")
                if not v.startswith("http"):
                    continue
                if v.rstrip("/") == url.rstrip("/") or v in _seen_urls:
                    continue      # duplicate URLs (bypassed == instant_dl) skip
                _seen_urls.add(v)
                clean_links[k] = v

            if clean_links:
                source = str(data.get("source") or data.get("module") or "")
                extra  = {
                    "file":  data.get("file"),   # {"name":..,"size":..} or None
                    "links": clean_links,          # all valid links
                    "batch": data.get("batch"),   # DZHQ multi-block results (rare)
                }
                return best, False, 1, elapsed, source, extra

            # status=true but no valid link
            return "ERR::No Valid URL Found", True, 1, elapsed, "", {}

        # ── BOTH FAILED (422) ─────────────────────────────────────────────────
        if sc == 422 or data.get("status") is False:
            _m = str(data.get("message") or "").lower()
            # API ke andar ke bots ne reply hi nahi kiya → ye API-side timeout hai
            # (bot keys badalte rehte hain: dzhq / nick / alex / alex_bot ...)
            _no_resp = any(
                isinstance(v, str) and v.strip().lower().startswith("no response")
                for k, v in data.items()
                if k not in ("status", "message", "developer", "response_ms",
                             "source", "module", "account")
            )
            if _no_resp or "timeout" in _m or "all bots failed" in _m or "bots failed" in _m:
                return "ERR::Link Not Supported or Try Again !!", True, 1, elapsed, "", {}
            return "ERR::Link Not Supported or Try Again !!", True, 1, elapsed, "", {}


        # ── OTHER ERRORS ──────────────────────────────────────────────────────
        err_msg = data.get("message") or data.get("error") or "Unknown error"

        if sc == 400:
            _reason = f"Bad Request: {err_msg}"
        elif sc == 503:
            _reason = "Bypass Unavailable — Try Again"
        elif sc == 500:
            _reason = "Server Error — Try Again"
        else:
            _reason = str(err_msg)[:120]
        return f"ERR::{_reason}", True, 1, elapsed, "", {}

    except requests.exceptions.Timeout:
        return "ERR::Request Timeout — Try Again", True, 1, round(time.time() - total_start, 2), "", {}
    except requests.exceptions.ConnectionError:
        return "ERR::API Connect Failed", True, 1, round(time.time() - total_start, 2), "", {}
    except Exception as e:
        return f"ERR::{str(e)[:80]}", True, 1, round(time.time() - total_start, 2), "", {}

# ============================================================
# BOT + THREAD POOL
# ============================================================

# ── Latency tuning ────────────────────────────────────────
# Keep-alive session reuse (no new TLS handshake per API call) + more worker
# threads so replies go out immediately instead of queueing.
try:
    telebot.apihelper.SESSION_TIME_TO_LIVE = 600
    telebot.apihelper.CONNECT_TIMEOUT      = 5
    telebot.apihelper.READ_TIMEOUT         = 20
    telebot.apihelper.RETRY_ON_ERROR       = True
except Exception as _api_e:
    print(f"[API_TUNE] {_api_e}")

bot = telebot.TeleBot(BOT_TOKEN, threaded=True, num_threads=100)

_pending_state_lock = threading.Lock()
_pending_state      = {}

def pending_get(uid):
    with _pending_state_lock:
        return _pending_state.get(uid)

def pending_set(uid, val):
    with _pending_state_lock:
        _pending_state[uid] = val

def pending_del(uid):
    with _pending_state_lock:
        _pending_state.pop(uid, None)

_user_stats_lock = threading.Lock()
_user_stats      = defaultdict(int)

def increment_user_stat(uid):
    with _user_stats_lock:
        _user_stats[uid] += 1
        return _user_stats[uid]

_POOL = ThreadPoolExecutor(max_workers=500)

_BOT_SELF_ID = None
def _get_bot_id():
    global _BOT_SELF_ID
    if _BOT_SELF_ID: return _BOT_SELF_ID
    try:
        _BOT_SELF_ID = bot.get_me().id
    except: pass
    return _BOT_SELF_ID

# ============================================================
# SAFE HELPERS
# ============================================================

def _strip_tg_emoji(text):
    return re.sub(r"<tg-emoji[^>]*>(.*?)</tg-emoji>", r"\1", text, flags=re.DOTALL)

def _safe_react(chat_id, message_id, emoji="⚡", is_big=False):
    try:
        bot.set_message_reaction(
            chat_id, message_id,
            reaction=[telebot.types.ReactionTypeEmoji(emoji)],
            is_big=is_big,
        )
    except: pass

def _strip_blockquote(text):
    return re.sub(r"</?blockquote[^>]*>", "", text)

_ENTITY_ERRS = ("entity_text_invalid", "can't parse entities",
                "unsupported start tag", "document_invalid")

_orig_send_message = bot.send_message
_orig_edit_message = bot.edit_message_text

def _safe_send(chat_id, text, **kwargs):
    try:
        return _orig_send_message(chat_id, text, **kwargs)
    except Exception as _e:
        print(f"[SAFE_SEND] primary send failed: {_e}")
        # Fallback 1: strip blockquote, KEEP tg-emoji (premium emoji usually work)
        try:
            text_no_bq = _strip_blockquote(text)
            return _orig_send_message(chat_id, text_no_bq, **kwargs)
        except Exception as _e2:
            print(f"[SAFE_SEND] blockquote strip failed: {_e2}")
            # Fallback 2: strip tg-emoji too as last resort before plain text
            try:
                return _orig_send_message(chat_id, _strip_tg_emoji(text), **kwargs)
            except Exception as _e3:
                # Fallback 3: plain text, no HTML
                try:
                    plain = re.sub(r"<[^>]+>", "", text)
                    kw = {k: v for k, v in kwargs.items() if k != "parse_mode"}
                    return _orig_send_message(chat_id, plain, **kw)
                except Exception as _e4:
                    print(f"[SAFE_SEND] all fallbacks failed: {_e4}")

bot.send_message = _safe_send

def _safe_edit(chat_id, msg_id, text, **kwargs):
    try:
        return _orig_edit_message(text, chat_id, msg_id, **kwargs)
    except Exception as _e:
        _es = str(_e).lower()
        if "message is not modified" in _es or "message to edit not found" in _es:
            return None
        # Widened on purpose: don't gate the fallback on matching specific
        # error-string keywords (Telegram's wording varies by version/locale
        # and this was silently swallowing edits — e.g. admin panel / failed
        # bypass screens just not updating and premium emoji not rendering).
        # Any other edit failure → delete old message, send fresh one so
        # tg-emoji + blockquote render correctly.
        reply_markup = kwargs.get("reply_markup")
        parse_mode   = kwargs.get("parse_mode", "HTML")
        disable_preview = kwargs.get("disable_web_page_preview", False)
        try:
            bot.delete_message(chat_id, msg_id)
        except: pass
        try:
            return _orig_send_message(
                chat_id, text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_preview,
            )
        except Exception as _e2:
            # Fallback: strip tg-emoji, keep blockquote
            try:
                return _orig_send_message(
                    chat_id, _strip_tg_emoji(text),
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_web_page_preview=disable_preview,
                )
            except Exception as _e3:
                # Last resort: strip blockquote too, keep going
                try:
                    return _orig_send_message(
                        chat_id, _strip_blockquote(_strip_tg_emoji(text)),
                        parse_mode=parse_mode,
                        reply_markup=reply_markup,
                        disable_web_page_preview=disable_preview,
                    )
                except Exception as _e4:
                    print(f"[SAFE_EDIT] all fallbacks failed: orig={_e} send1={_e2} send2={_e3} send3={_e4}")

def _safe_answer(call_id, text="", show_alert=False, **kwargs):
    try:
        bot.answer_callback_query(call_id, text=text, show_alert=show_alert, **kwargs)
    except Exception as _ae:
        _ae_s = str(_ae).lower()
        if any(x in _ae_s for x in ("query is too old", "query_id_invalid", "invalid", "400")):
            pass
        else:
            print(f"[SAFE_ANSWER] {_ae}")

# ============================================================
# AUTO-DELETE HELPER
# ============================================================

_AUTO_DELETE_DELAY = 600

def _schedule_delete(chat_id, *message_ids, delay=_AUTO_DELETE_DELAY):
    def _do():
        time.sleep(delay)
        for mid in message_ids:
            try:
                bot.delete_message(chat_id, mid)
            except Exception as _de:
                _des = str(_de).lower()
                if "message to delete not found" not in _des and "message can't be deleted" not in _des:
                    print(f"[AUTO_DEL] chat={chat_id} mid={mid}: {_de}")
    threading.Thread(target=_do, daemon=True).start()

# ============================================================
# LOG CHANNEL — full user activity feed
# ============================================================

def _u_tag(user):
    """Clickable user tag for logs."""
    if not user:
        return "—"
    name = _html_mod.escape(user.first_name or "User")
    uname = f"@{user.username}" if getattr(user, "username", None) else "—"
    return (f'<a href="tg://user?id={user.id}">{name}</a>\n'
            f"┣ ɪᴅ : <code>{user.id}</code>\n"
            f"┗ ᴜɴ : {_html_mod.escape(uname)}")

def _c_tag(chat):
    """Chat info for logs."""
    if not chat:
        return "—"
    if chat.type == "private":
        return "ᴩʀɪᴠᴀᴛᴇ (ᴅᴍ)"
    title = _html_mod.escape(chat.title or "Group")
    uname = f" (@{chat.username})" if getattr(chat, "username", None) else ""
    return f"{title}{_html_mod.escape(uname)} · <code>{chat.id}</code>"

def _do_log(text):
    try:
        _orig_send_message(
            LOG_CHANNEL_ID, text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as _le:
        # Retry once without HTML so a bad entity never loses the log
        try:
            _orig_send_message(LOG_CHANNEL_ID, re.sub(r"<[^>]+>", "", text),
                               disable_web_page_preview=True)
        except Exception as _le2:
            print(f"[LOG_CH] failed: {_le} / {_le2}")

def log_event(title, user=None, chat=None, lines=None, icon="⚡"):
    """Fire-and-forget log to the log channel (never blocks the reply)."""
    def _build_and_send():
        body  = f"{icon} <b>{_html_mod.escape(title)}</b>\n"
        body += "━━━━━━━━━━━━━━━━━━\n"
        if user is not None:
            body += f"👤 <b>ᴜꜱᴇʀ :</b> {_u_tag(user)}\n"
        if chat is not None:
            body += f"💬 <b>ᴄʜᴀᴛ :</b> {_c_tag(chat)}\n"
        for ln in (lines or []):
            body += f"{ln}\n"
        body += f"🕒 <b>ᴛɪᴍᴇ :</b> {_now_ist()}"
        _do_log(body)
    try:
        _POOL.submit(_build_and_send)
    except Exception as _se:
        print(f"[LOG_CH] submit failed: {_se}")

def _log_link(label, url, limit=120):
    u = _html_mod.escape(str(url or "—"))
    if len(u) > limit:
        u = u[:limit] + "…"
    return f"{label} <code>{u}</code>"

# ============================================================
# BYPASS MODE TOGGLES — Pure RAM flags
# ============================================================

_DM_BYPASS_ON  = False
_ALL_GROUPS_ON = False
_toggle_lock   = threading.Lock()

def _load_toggle_flags():
    global _DM_BYPASS_ON, _ALL_GROUPS_ON
    with _toggle_lock:
        _DM_BYPASS_ON  = db_get_setting("dm_bypass_on",  "1") == "1"
        _ALL_GROUPS_ON = db_get_setting("all_groups_on", "0") == "1"

def get_dm_bypass():  return _DM_BYPASS_ON
def get_all_groups(): return _ALL_GROUPS_ON

def toggle_dm_bypass():
    global _DM_BYPASS_ON
    with _toggle_lock:
        _DM_BYPASS_ON = not _DM_BYPASS_ON
        val = "1" if _DM_BYPASS_ON else "0"
    db_set_setting("dm_bypass_on", val)
    return _DM_BYPASS_ON

def toggle_all_groups():
    global _ALL_GROUPS_ON
    with _toggle_lock:
        _ALL_GROUPS_ON = not _ALL_GROUPS_ON
        val = "1" if _ALL_GROUPS_ON else "0"
    db_set_setting("all_groups_on", val)
    return _ALL_GROUPS_ON

# ── Allowed groups ────────────────────────────────────────

_allowed_groups_cache    = None
_allowed_groups_cache_ts = 0.0
_ALLOWED_GROUPS_TTL      = 60.0
_allowed_groups_lock     = threading.Lock()

def get_allowed_groups():
    global _allowed_groups_cache, _allowed_groups_cache_ts
    now = time.monotonic()
    with _allowed_groups_lock:
        if _allowed_groups_cache is not None and (now - _allowed_groups_cache_ts) < _ALLOWED_GROUPS_TTL:
            return _allowed_groups_cache
    docs = list(_col_groups.find({}, {"_id": 0, "group_id": 1, "group_name": 1}).sort("group_id", ASCENDING))
    with _allowed_groups_lock:
        _allowed_groups_cache    = docs
        _allowed_groups_cache_ts = now
    return docs

def _invalidate_allowed_groups_cache():
    global _allowed_groups_cache
    with _allowed_groups_lock:
        _allowed_groups_cache = None

_allowed_group_ids_set: set = set()
_allowed_group_ids_ts       = 0.0
_ALLOWED_IDS_TTL            = 60.0
_allowed_ids_lock           = threading.Lock()

def _get_allowed_ids():
    global _allowed_group_ids_set, _allowed_group_ids_ts
    now = time.monotonic()
    with _allowed_ids_lock:
        if (now - _allowed_group_ids_ts) < _ALLOWED_IDS_TTL:
            return _allowed_group_ids_set
    ids = {d["group_id"] for d in _col_groups.find({}, {"_id": 0, "group_id": 1})}
    with _allowed_ids_lock:
        _allowed_group_ids_set = ids
        _allowed_group_ids_ts  = now
    return ids

def _invalidate_allowed_ids():
    global _allowed_group_ids_ts
    with _allowed_ids_lock:
        _allowed_group_ids_ts = 0.0

def add_allowed_group(group_id, group_name="Unknown"):
    _col_groups.update_one(
        {"group_id": group_id},
        {"$set": {"group_id": group_id, "group_name": group_name,
                  "added_at": _now_ist()}},
        upsert=True
    )
    _invalidate_allowed_groups_cache()
    _invalidate_allowed_ids()

def remove_allowed_group(group_id):
    _col_groups.delete_one({"group_id": group_id})
    _invalidate_allowed_groups_cache()
    _invalidate_allowed_ids()

def is_allowed_group(chat_id, chat_username=""):
    if int(chat_id) == ALLOWED_GROUP_ID:
        return True
    return int(chat_id) in _get_allowed_ids()

# ============================================================
# WARM ALL CACHES
# ============================================================

def _warm_all_caches():
    global _WELCOME_HTML_GENERIC
    with _cache_lock:
        _settings_cache.clear()
        _welcome_cache.clear()
        _bypass_cache.clear()
    global _channels_cache
    _channels_cache = None

    # Load bypass links into RAM
    docs = list(_col_links.find({}, {"_id": 0, "original_url": 1, "bypassed_url": 1,
                                     "extra": 1, "time_taken": 1, "source": 1}))
    with _cache_lock:
        for d in docs:
            _bypass_cache[d["original_url"]] = _cache_entry(
                d.get("bypassed_url"), d.get("extra"),
                d.get("time_taken", 0.0), d.get("source", ""))
    print(f"[CACHE_WARM] {len(docs)} bypass links loaded into RAM")

    _build_loading_cache()
    _WELCOME_HTML_GENERIC = None
    _build_welcome_template()
    print("[CACHE_WARM] Welcome template & loading msgs ready")
    _load_toggle_flags()
    _get_allowed_ids()

# ============================================================
# ADMIN KEYBOARDS
# ============================================================

def admin_main_keyboard():
    mk = InlineKeyboardMarkup(row_width=2)
    mk.add(
        SBtn("ʙʀᴏᴀᴅᴄᴀꜱᴛ",       style="primary", icon_custom_emoji_id="5467415297536088297", callback_data="adm_broadcast"),
        SBtn("ᴡᴇʟᴄᴏᴍᴇ ꜱᴇᴛᴛɪɴɢ", style="primary", icon_custom_emoji_id="5467369612820042920", callback_data="adm_welcome"),
    )
    mk.add(
        SBtn("ᴄʜᴀɴɴᴇʟ ᴍɢᴍᴛ",    style="primary", icon_custom_emoji_id="5467254337188930430", callback_data="adm_channels"),
        SBtn("ᴅᴀꜱʜʙᴏᴀʀᴅ",        style="primary", icon_custom_emoji_id="5472354553662285862", callback_data="adm_dashboard"),
    )
    mk.add(
        SBtn("ᴜꜱᴇʀ ʟɪꜱᴛ",        style="primary", icon_custom_emoji_id="5467531326890228611", callback_data="adm_userlist"),
        SBtn("ʙʏᴩᴀꜱꜱ ꜱᴇᴛᴛɪɴɢꜱ", style="primary", icon_custom_emoji_id="6195245116207143870", callback_data="adm_bypass_settings"),
    )
    return mk

def bypass_settings_keyboard():
    dm_on  = get_dm_bypass()
    all_on = get_all_groups()
    groups = get_allowed_groups()
    total  = len(groups)
    dm_icon  = _E_GREEN if dm_on  else _E_RED
    all_icon = _E_GREEN if all_on else _E_RED
    dm_lbl   = f"ᴅᴍ ʙʏᴩᴀꜱꜱ  :  {'ᴏɴ' if dm_on else 'ᴏꜰꜰ'}"
    all_lbl  = f"ᴀʟʟ ɢʀᴏᴜᴩꜱ  :  {'ᴏɴ' if all_on else 'ᴏꜰꜰ'}"
    mk = InlineKeyboardMarkup(row_width=1)
    mk.add(SBtn(dm_lbl,  style="success" if dm_on  else "danger",
                icon_custom_emoji_id=dm_icon,  callback_data="byp_toggle_dm"))
    mk.add(SBtn(all_lbl, style="success" if all_on else "danger",
                icon_custom_emoji_id=all_icon, callback_data="byp_toggle_all"))
    mk.add(SBtn(f"ᴍᴀɴᴀɢᴇ ɢʀᴏᴜᴩꜱ  [{total}]", style="primary",
                icon_custom_emoji_id=_E_GROUP, callback_data="byp_manage_groups"))
    mk.add(SBtn("ᴄʜᴀɴɢᴇ ᴀᴘɪ ᴜʀʟ", style="primary",
                icon_custom_emoji_id=_E_GLB2, callback_data="byp_api_url"))
    mk.add(SBtn("ʙᴀᴄᴋ", style="primary",
                icon_custom_emoji_id=_E_BACK, callback_data="adm_back"))
    return mk

def _bypass_settings_msg(dm_on, all_on, groups):
    dm_icon  = _te(_E_GRN2,'🟢') if dm_on  else _te(_E_RED2,'🔴')
    all_icon = _te(_E_GRN2,'🟢') if all_on else _te(_E_RED2,'🔴')
    dm_stat  = f"{_te(_E_CHK2,'✅')}" if dm_on  else f"{_te(_E_CRS2,'❌')}"
    all_stat = f"{_te(_E_CHK2,'✅')}" if all_on else f"{_te(_E_CRS2,'❌')}"
    inner  = f"{_HEADER}\n\n"
    inner += f"{_te(_E_PLUS,'➕')} {b('ʙʏᴩᴀꜱꜱ ꜱᴇᴛᴛɪɴɢꜱ — ᴀᴅᴠᴀɴᴄᴇᴅ')}\n\n"
    inner += f"{_LOOP9}\n\n"
    inner += f"{_te(_E_TREE,'🌲')}{b('ꜱᴛᴀᴛᴜꜱ ᴏᴠᴇʀᴠɪᴇᴡ')}\n\n"
    inner += f"  {_te(_E_PEN,'✍️')} {b('ᴅᴍ ʙʏᴩᴀꜱꜱ')}  ›  {dm_icon} {dm_stat}\n"
    api_url_display = _html_mod.escape(get_bypass_api_url())
    inner += f"  {_te(_E_GLB2,'🌐')} {b('ᴀᴘɪ ᴜʀʟ')}  ›  <code>{api_url_display}</code>\n"
    inner += f"  {_te(_E_GLB2,'🌐')} {b('ᴀʟʟ ɢʀᴏᴜᴩꜱ')}  ›  {all_icon} {all_stat}\n\n"
    inner += f"{_LOOP9}\n\n"
    inner += f"{_te(_E_EYE2,'👁')}{b('ʜᴏᴡ ɪᴛ ᴡᴏʀᴋꜱ')}\n\n"
    inner += f"  {_te(_E_BELL,'🔔')} {b('ᴅᴍ ᴏɴ → ᴜꜱᴇʀꜱ ᴄᴀɴ ʙʏᴩᴀꜱꜱ ɪɴ ʙᴏᴛ ᴅᴍ')}\n"
    inner += f"   {_te(_E_LOCK2,'🔒')} {b('ᴅᴍ ᴏꜰꜰ → ᴩʀɪᴠᴀᴛᴇ ʙʏᴩᴀꜱꜱ ʙʟᴏᴄᴋᴇᴅ')}\n\n"
    inner += f"  {_te(_E_GRP2,'👥')} {b('ᴀʟʟ ɢʀᴏᴜᴩꜱ ᴏɴ → ᴇᴠᴇʀʏ ɢʀᴏᴜᴩ ʙʏᴩᴀꜱꜱ')}\n"
    inner += f"   {pe_chain()} {b('ᴀʟʟ ɢʀᴏᴜᴩꜱ ᴏꜰꜰ → ᴏɴʟʏ ᴀʟʟᴏᴡᴇᴅ ɢʀᴏᴜᴩꜱ')}\n\n"
    inner += f"{_LOOP9}\n\n"
    inner += f"{_te(_E_GRP2,'👥')} {b('ᴛᴏᴛᴀʟ ᴀʟʟᴏᴡᴇᴅ ɢʀᴏᴜᴩꜱ : ' + str(len(groups)))}\n\n"
    if groups:
        for i, g in enumerate(groups, 1):
            gname = g.get("group_name") or "Unknown"
            gid   = g["group_id"]
            inner += f"  {b(str(i) + '.')} {_te(_E_GRP2,'👥')} {b(gname)}\n"
            inner += f"  {pe_bolt()} {b('ɪᴅ :')} <code>{gid}</code>\n\n"
    else:
        inner += f"  {_te(_E_CRS2,'❌')} {b('ᴋᴏɪ ɢʀᴏᴜᴩ ɴᴀʜɪ ʜᴀɪ ᴀʙʜɪ')}\n\n"
    inner += f"{_LOOP9}\n{_FOOTER}"
    return bq(inner)

def welcome_settings_keyboard():
    video_id = db_get_welcome("video_file_id")
    image_id = db_get_welcome("image_file_id")
    mk = InlineKeyboardMarkup(row_width=2)
    mk.add(
        SBtn(f"ꜱᴇᴛ ɪᴍᴀɢᴇ  {'✓' if image_id else '✗'}", style="primary", callback_data="wel_set_image"),
        SBtn(f"ꜱᴇᴛ ᴠɪᴅᴇᴏ  {'✓' if video_id else '✗'}", style="primary", callback_data="wel_set_video"),
    )
    mk.add(
        SBtn("ᴄʟᴇᴀʀ ɪᴍᴀɢᴇ", style="danger", callback_data="wel_clear_image"),
        SBtn("ᴄʟᴇᴀʀ ᴠɪᴅᴇᴏ", style="danger", callback_data="wel_clear_video"),
    )
    mk.add(SBtn("ʀᴇꜱᴇᴛ ᴅᴇꜰᴀᴜʟᴛ", style="danger",  callback_data="wel_reset"))
    mk.add(SBtn("ʙᴀᴄᴋ",          style="primary", icon_custom_emoji_id=_E_BACK, callback_data="adm_back"))
    return mk

def channel_mgmt_keyboard():
    channels = get_force_channels()
    mk = InlineKeyboardMarkup(row_width=1)
    for ch in channels:
        mk.add(SBtn(f"{ch['name']}  ʀᴇᴍᴏᴠᴇ", style="danger",
                    icon_custom_emoji_id="6129486856212979482",
                    callback_data=f"ch_del_{ch['id']}"))
    mk.add(SBtn("ᴀᴅᴅ ᴩᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟ",  style="success", callback_data="ch_add_public"))
    mk.add(SBtn("ᴀᴅᴅ ᴩʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟ", style="primary", callback_data="ch_add_private"))
    mk.add(SBtn("ʙᴀᴄᴋ", style="primary", icon_custom_emoji_id=_E_BACK, callback_data="adm_back"))
    return mk

# ============================================================
# PENDING PRIVATE-CHANNEL JOIN REQUESTS
# ============================================================
@bot.chat_join_request_handler()
def handle_chat_join_request(join_request):
    """Remember a user's pending request without approving or declining it."""
    try:
        uid = join_request.from_user.id
        channel_id = join_request.chat.id
        invite_link = _request_invite_url(join_request)
        now = datetime.now(timezone.utc)

        # Resolve the configured private channel from its invite link, then
        # persist the real chat_id because private channels have no username.
        matched = None
        for configured in get_force_channels():
            configured_link = str(configured.get("link", "") or "").strip().rstrip("/")
            if configured.get("chat_id") == channel_id or (
                invite_link and configured_link == invite_link
            ):
                matched = configured
                break

        if matched:
            try:
                from bson import ObjectId
                _col_channels.update_one(
                    {"_id": ObjectId(matched["id"])},
                    {"$set": {"chat_id": channel_id}},
                )
                _invalidate_channels()
            except Exception as e:
                print(f"[JOIN_REQUEST] channel mapping failed: {e}")

            _col_join_requests.update_one(
                {"user_id": uid, "chat_id": channel_id},
                {"$set": {
                    "user_id": uid,
                    "chat_id": channel_id,
                    "invite_link": invite_link,
                    "requested_at": now,
                }},
                upsert=True,
            )
            _invalidate_user_join_cache(uid)
            print(f"[JOIN_REQUEST] pending request saved: user={uid}, chat={channel_id}")
    except Exception as e:
        print(f"[JOIN_REQUEST] {e}")

# ============================================================
# /START
# ============================================================
@bot.message_handler(commands=["start"])

def start_command(message):
    if not message.from_user or message.from_user.is_bot: return
    if message.chat.type != "private": return
    uid   = message.from_user.id
    cid   = message.chat.id
    fname = message.from_user.first_name
    uname = message.from_user.username or ""
    # Nothing blocking before the reply: DB write + reaction go to the pool.
    _POOL.submit(register_user, uid, fname, uname)
    _POOL.submit(_safe_react, cid, message.message_id, "⚡", True)
    log_event("/START ᴄᴏᴍᴍᴀɴᴅ", message.from_user, message.chat, icon="🚀")
    # Fast path — cache already says user joined → welcome instantly (0 API calls)
    if is_user_definitely_joined(uid):
        send_welcome(cid, uid, fname)
        return
    nj = check_user_joined_all_cached(uid)
    if nj:
        log_event("ꜰᴏʀᴄᴇ ᴊᴏɪɴ ꜱʜᴏᴡɴ", message.from_user, message.chat,
                  lines=[f"🔒 <b>ᴩᴇɴᴅɪɴɢ :</b> {len(nj)} channel(s)"], icon="🔒")
        _safe_send(cid, get_force_join_message(fname, nj),
                         parse_mode="HTML", reply_markup=force_join_markup(nj, uid, cid))
        return
    send_welcome(cid, uid, fname)

# ============================================================
# VERIFY CALLBACK
# ============================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("verify_"))
def verify_callback(call):
    parts = call.data.split("_")
    if len(parts) < 3: return
    uid = int(parts[1]); cid = int(parts[2])
    if call.from_user.id != uid:
        alert_msg = f"{pe_cross()} Not For You!"
        _safe_answer(call.id, alert_msg, show_alert=True); return
    _invalidate_user_join_cache(uid)
    nj = check_user_joined_all(uid)
    with _user_join_cache_lock:
        _user_join_cache[uid] = (nj, time.monotonic())
    if not nj:
        log_event("ᴠᴇʀɪꜰɪᴇᴅ (ᴊᴏɪɴᴇᴅ ᴀʟʟ)", call.from_user, call.message.chat, icon="✅")
        try: bot.delete_message(cid, call.message.id)
        except: pass
        send_welcome(cid, uid, call.from_user.first_name)
        alert_msg = f"{pe_check()} VERIFIED! WELCOME!"
        _safe_answer(call.id, alert_msg)
    else:
        alert_msg = f"{pe_cross()} Still Not Joined All Channels!"
        _safe_answer(call.id, alert_msg, show_alert=True)

# ============================================================
# /REMOVE — purge a single URL from cache (RAM + MongoDB)
# ============================================================

@bot.message_handler(commands=["remove"])
def remove_cache_command(message):
    if message.from_user.id != OWNER_ID:
        _safe_send(message.chat.id, bq(f"{pe_cross()} {b('ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ!')}"), parse_mode="HTML"); return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        _safe_send(message.chat.id,
                    bq(f"{pe_cross()} {b('ᴜꜱᴀɢᴇ :')} <code>/remove &lt;link&gt;</code>"),
                    parse_mode="HTML")
        return
    target_url = parts[1].strip()
    removed = cache_remove_bypassed(target_url)
    if removed:
        log_event("ᴄᴀᴄʜᴇ ʟɪɴᴋ ʀᴇᴍᴏᴠᴇᴅ", message.from_user, message.chat,
                  lines=[f"🗑 <b>Link :</b> {target_url}"], icon="🗑")
        _safe_send(message.chat.id,
                    bq(f"{pe_trash()} {b('ʀᴇᴍᴏᴠᴇᴅ ꜰʀᴏᴍ ᴄᴀᴄʜᴇ!')}\n{pe_check()} {b(target_url)}"),
                    parse_mode="HTML")
    else:
        _safe_send(message.chat.id,
                    bq(f"{pe_cross()} {b('ʟɪɴᴋ ɴᴏᴛ ꜰᴏᴜɴᴅ ɪɴ ᴄᴀᴄʜᴇ.')}"),
                    parse_mode="HTML")

# ============================================================
# /ADMIN
# ============================================================

@bot.message_handler(commands=["admin"])
def admin_command(message):
    if message.from_user.id != OWNER_ID:
        _safe_send(message.chat.id, bq(f"{pe_cross()} {b('ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ!')}"), parse_mode="HTML"); return
    log_event("/ADMIN ᴩᴀɴᴇʟ ᴏᴩᴇɴᴇᴅ", message.from_user, message.chat, icon="👑")
    txt = bq(f"{pe_bolt()} {b('ꜰᴇʟɪx ʙʏᴩᴀꜱꜱ ʙᴏᴛ — ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ')} {pe_star()}")
    _safe_send(message.chat.id, txt, parse_mode="HTML", reply_markup=admin_main_keyboard())

# ============================================================
# ADMIN CALLBACKS
# ============================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("adm_") or
                                            c.data.startswith("wel_") or
                                            c.data.startswith("ch_")  or
                                            c.data.startswith("bc_")  or
                                            c.data.startswith("byp_") or
                                            c.data.startswith("prm_"))
def admin_callbacks(call):
    uid = call.from_user.id
    if uid != OWNER_ID:
        alert_msg = f"{pe_cross()} Not Authorized!"
        _safe_answer(call.id, alert_msg, show_alert=True); return
    data = call.data; cid = call.message.chat.id; mid = call.message.message_id

    if data == "adm_back":
        txt = f"{pe_bolt()} {b('FELIX BYPASS BOT — ADMIN PANEL')} {pe_star()}"
        _safe_edit(cid, mid, txt, parse_mode="HTML", reply_markup=admin_main_keyboard())
        _safe_answer(call.id)

    elif data == "adm_dashboard":
        users       = get_all_users()
        cache_count = _col_links.count_documents({})
        vid_set     = f"{pe_check()}" if db_get_welcome("video_file_id") else f"{pe_cross()}"
        img_set     = f"{pe_check()}" if db_get_welcome("image_file_id") else f"{pe_cross()}"
        ram_links   = len(_bypass_cache)
        inner  = f"{_HEADER}\n\n{pe_bolt()} {b('Dashboard')}\n\n"
        inner += f"{pe_party()} {b('Total Users    : ' + str(len(users)))}\n"
        inner += f"{pe_bolt()}  {b('Total Bypasses : ' + str(sum(u.get('bypass_count',0) for u in users)))}\n"
        inner += f"{pe_star()}  {b('Active Users   : ' + str(sum(1 for u in users if u.get('bypass_count',0) > 0)))}\n"
        inner += f"{pe_chain()} {b('Force Channels : ' + str(len(get_force_channels())))}\n"
        inner += f"{pe_gift()}  {b('Cached Links DB: ' + str(cache_count))}\n"
        inner += f"{pe_bolt()}  {b('Cached Links RAM: ' + str(ram_links))}\n\n"
        inner += f"{b('ᴠɪᴅᴇᴏ   : ' + vid_set + '  |  ɪᴍᴀɢᴇ : ' + img_set)}\n\n"
        inner += f"{_LOOP9}\n{_FOOTER}"
        mk = InlineKeyboardMarkup()
        mk.add(SBtn("ʙᴀᴄᴋ", style="primary", callback_data="adm_back"))
        _safe_edit(cid, mid, bq(inner), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data == "adm_userlist":
        users = get_all_users()
        lines = [f"{pe_group()} {b(_st('Total Users'))}: {len(users)}", ""]
        for u in users[:20]:
            uname_str = f"@{u['username']}" if u.get('username') else "—"
            lines.append(f"• {u.get('first_name','?')} ({uname_str}) — {u.get('bypass_count',0)} bypasses")
        if len(users) > 20:
            lines.append(f"\n... +{len(users)-20} more users")
        mk = InlineKeyboardMarkup()
        mk.add(SBtn("ʙᴀᴄᴋ", style="primary", callback_data="adm_back"))
        _safe_edit(cid, mid, "\n".join(lines), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data == "adm_welcome":
        _safe_edit(cid, mid, bq(f"{pe_gift()} {b('ᴡᴇʟᴄᴏᴍᴇ ꜱᴇᴛᴛɪɴɢꜱ')} {pe_star()}"),
                              parse_mode="HTML", reply_markup=welcome_settings_keyboard())
        _safe_answer(call.id)

    elif data == "wel_set_image":
        pending_set(uid, "set_image")
        _safe_send(cid, bq(f"{pe_gift()} {b('ᴡᴇʟᴄᴏᴍᴇ ɪᴍᴀɢᴇ ʙʜᴇᴊᴏ:')}"), parse_mode="HTML")
        _safe_answer(call.id)

    elif data == "wel_set_video":
        pending_set(uid, "set_video")
        _safe_send(cid, bq(f"{pe_party()} {b('ᴡᴇʟᴄᴏᴍᴇ ᴠɪᴅᴇᴏ ʙʜᴇᴊᴏ:')}"), parse_mode="HTML")
        _safe_answer(call.id)

    elif data == "wel_clear_image":
        db_del_welcome("image_file_id")
        bot.edit_message_reply_markup(cid, mid, reply_markup=welcome_settings_keyboard())
        _safe_answer(call.id, "✅ Image cleared!", show_alert=True)

    elif data == "wel_clear_video":
        db_del_welcome("video_file_id")
        bot.edit_message_reply_markup(cid, mid, reply_markup=welcome_settings_keyboard())
        _safe_answer(call.id, "✅ Video cleared!", show_alert=True)

    elif data == "wel_reset":
        db_del_welcome("image_file_id")
        db_del_welcome("video_file_id")
        bot.edit_message_reply_markup(cid, mid, reply_markup=welcome_settings_keyboard())
        _safe_answer(call.id, "✅ Reset!", show_alert=True)

    elif data == "adm_channels":
        _safe_edit(cid, mid, bq(f"{pe_chain()} {b('ᴄʜᴀɴɴᴇʟ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ')} {pe_bolt()}"),
                              parse_mode="HTML", reply_markup=channel_mgmt_keyboard())
        _safe_answer(call.id)

    elif data.startswith("ch_del_"):
        ch_id_str = data[len("ch_del_"):]
        delete_force_channel(ch_id_str)
        bot.edit_message_reply_markup(cid, mid, reply_markup=channel_mgmt_keyboard())
        _safe_answer(call.id, "✅ Channel removed!", show_alert=True)

    elif data == "ch_add_public":
        pending_set(uid, "add_public_channel")
        _safe_send(cid, bq(f"{pe_bolt()} {b('ᴩᴜʙʟɪᴄ ᴄʜᴀɴɴᴇʟ ᴜꜱᴇʀɴᴀᴍᴇ ʙʜᴇᴊᴏ (e.g. @mychannel):')}"), parse_mode="HTML")
        _safe_answer(call.id)

    elif data == "ch_add_private":
        pending_set(uid, "add_private_channel")
        _safe_send(cid, bq(f"{pe_lock()} {b('ᴩʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟ/ɢʀᴏᴜᴩ ᴋᴀ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ʙʜᴇᴊᴏ:')}"), parse_mode="HTML")
        _safe_answer(call.id)

    elif data == "adm_bypass_settings":
        dm_on  = get_dm_bypass()
        all_on = get_all_groups()
        groups = get_allowed_groups()
        _safe_edit(cid, mid, _bypass_settings_msg(dm_on, all_on, groups),
                   parse_mode="HTML", reply_markup=bypass_settings_keyboard())
        _safe_answer(call.id)

    elif data == "byp_api_url":
        pending_set(uid, f"set_bypass_api_url:{cid}:{mid}")
        current = _html_mod.escape(get_bypass_api_url())
        prompt = (f"{_HEADER}\n\n"
                  f"{pe_bolt()} {b('ɴᴇᴡ ʙʏᴩᴀꜱꜱ ᴀᴩɪ ᴜʀʟ ʙʜᴇᴊᴏ')}\n\n"
                  f"{pe_info()} {b('ᴄᴜʀʀᴇɴᴛ :')}\n<code>{current}</code>\n\n"
                  f"{pe_info()} {b('ᴇxᴀᴍᴩʟᴇ :')}\n<code>https://bypass-gc.onrender.com</code>\n\n"
                  f"{pe_info()} {b('Base URL bhejne par /bypass?link={{}} automatically add ho jayega.')}\n\n"
                  f"{_LOOP9}\n{_FOOTER}")
        mk = InlineKeyboardMarkup()
        mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger", icon_custom_emoji_id=_E_BACK,
                    callback_data="adm_bypass_settings"))
        _safe_edit(cid, mid, bq(prompt), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data == "byp_toggle_dm":
        new_state = toggle_dm_bypass()
        status_emoji = pe_green() if new_state else pe_red()
        status_text = b('ᴏɴ') if new_state else b('ᴏꜰꜰ')
        alert_text = f"{status_emoji} ᴅᴍ ʙʏᴩᴀꜱꜱ: {status_text}"
        _safe_answer(call.id, alert_text, show_alert=True)
        dm_on  = new_state
        all_on = get_all_groups()
        groups = get_allowed_groups()
        _safe_edit(cid, mid, _bypass_settings_msg(dm_on, all_on, groups),
                   parse_mode="HTML", reply_markup=bypass_settings_keyboard())

    elif data == "byp_toggle_all":
        new_state = toggle_all_groups()
        status_emoji = pe_green() if new_state else pe_red()
        status_text = b('ᴏɴ') if new_state else b('ᴏꜰꜰ')
        alert_text = f"{status_emoji} ᴀʟʟ ɢʀᴏᴜᴩꜱ: {status_text}"
        _safe_answer(call.id, alert_text, show_alert=True)
        dm_on  = get_dm_bypass()
        all_on = new_state
        groups = get_allowed_groups()
        _safe_edit(cid, mid, _bypass_settings_msg(dm_on, all_on, groups),
                   parse_mode="HTML", reply_markup=bypass_settings_keyboard())

    elif data == "byp_manage_groups":
        groups = get_allowed_groups()
        total  = len(groups)
        inner  = f"{_HEADER}\n\n"
        inner += f"{pe_group()} {b('ɢʀᴏᴜᴩ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ')}\n\n"
        inner += f"{_LOOP9}\n\n"
        inner += f"{pe_group()} {b('ᴛᴏᴛᴀʟ ᴀʟʟᴏᴡᴇᴅ ɢʀᴏᴜᴩꜱ : ' + str(total))}\n\n"
        if groups:
            for i, g in enumerate(groups, 1):
                gname = g.get("group_name") or "Unknown"
                gid   = g["group_id"]
                inner += f"  {b(str(i) + '.')} {pe_group()} {b(gname)}\n"
                inner += f"  {pe_bolt()} {b('ɪᴅ :')} <code>{gid}</code>\n\n"
        else:
            inner += f"  {pe_cross()} {b('ᴋᴏɪ ɢʀᴏᴜᴩ ɴᴀʜɪ ʜᴀɪ ᴀʙʜɪ')}\n\n"
        inner += f"{_LOOP9}\n{_FOOTER}"
        mk = InlineKeyboardMarkup(row_width=2)
        mk.add(
            SBtn("ᴀᴅᴅ ɢʀᴏᴜᴩ", style="success",
                 icon_custom_emoji_id=_E_ADD, callback_data="byp_add_group"),
            SBtn("ʀᴇᴍᴏᴠᴇ ɢʀᴏᴜᴩ", style="danger",
                 icon_custom_emoji_id=_E_MINUS, callback_data="byp_remove_group"),
        )
        for g in groups:
            gname = g.get("group_name") or "Unknown"
            gid   = g["group_id"]
            mk.add(SBtn(f"{gname}  ({gid})", style="danger",
                        icon_custom_emoji_id=_E_TRASH, callback_data=f"byp_del_{gid}"))
        mk.add(SBtn("ʙᴀᴄᴋ", style="primary",
                    icon_custom_emoji_id=_E_BACK, callback_data="adm_bypass_settings"))
        _safe_edit(cid, mid, bq(inner), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data == "byp_add_group":
        pending_set(uid, f"add_allowed_group:{cid}:{mid}")
        inner  = f"{_HEADER}\n\n"
        inner += f"{pe_bolt()} {b('ɢʀᴏᴜᴩ ɪᴅ ʙʜᴇᴊᴏ ᴊɪꜱᴇ ᴀʟʟᴏᴡ ᴋᴀʀɴᴀ ʜᴀɪ:')}\n\n"
        inner += f"{pe_group()} {b('ꜰᴏʀᴍᴀᴛ:')}\n<code>-100xxxxxxxxxx</code>\n\n"
        inner += f"{pe_info()} {b('ᴇxᴀᴍᴩʟᴇ:')}\n<code>-1001234567890</code>\n\n"
        inner += f"{_LOOP9}\n{_FOOTER}"
        mk = InlineKeyboardMarkup()
        mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger",
                    icon_custom_emoji_id=_E_BACK, callback_data="byp_manage_groups"))
        _safe_edit(cid, mid, bq(inner), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data == "byp_remove_group":
        pending_set(uid, f"remove_allowed_group:{cid}:{mid}")
        groups = get_allowed_groups()
        inner  = f"{_HEADER}\n\n"
        inner += f"{pe_trash()} {b('ɢʀᴏᴜᴩ ɪᴅ ʙʜᴇᴊᴏ ᴊɪꜱᴇ ʀᴇᴍᴏᴠᴇ ᴋᴀʀɴᴀ ʜᴀɪ:')}\n\n"
        inner += f"{pe_group()} {b('ꜰᴏʀᴍᴀᴛ:')}\n<code>-100xxxxxxxxxx</code>\n\n"
        if groups:
            inner += f"{_LOOP9}\n\n{pe_group()} {b('ᴄᴜʀʀᴇɴᴛ ᴀʟʟᴏᴡᴇᴅ ɢʀᴏᴜᴩꜱ:')}\n\n"
            for g in groups:
                gname = g.get("group_name") or "Unknown"
                gid   = g["group_id"]
                inner += f"{pe_bolt()} {b(gname)}\n<code>{gid}</code>\n\n"
        inner += f"{_LOOP9}\n{_FOOTER}"
        mk = InlineKeyboardMarkup()
        mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger",
                    icon_custom_emoji_id=_E_BACK, callback_data="byp_manage_groups"))
        _safe_edit(cid, mid, bq(inner), parse_mode="HTML", reply_markup=mk)
        _safe_answer(call.id)

    elif data.startswith("byp_del_"):
        gid = int(data.split("_")[-1])
        remove_allowed_group(gid)
        alert_text = f"{pe_trash()} ɢʀᴏᴜᴩ {gid} ʀᴇᴍᴏᴠᴇᴅ!"
        _safe_answer(call.id, alert_text, show_alert=True)
        groups = get_allowed_groups()
        inner  = f"{_HEADER}\n\n{pe_gear()} {b('ʙʏᴩᴀꜱꜱ ꜱᴇᴛᴛɪɴɢꜱ')}\n\n"
        inner += f"{pe_chain()} {b('ɢʀᴏᴜᴩ ʀᴇᴍᴏᴠᴇᴅ')} {pe_bolt()}\n\n"
        inner += f"{_LOOP9}\n{_FOOTER}"
        mk = InlineKeyboardMarkup(row_width=2)
        mk.add(
            SBtn("ᴀᴅᴅ ɢʀᴏᴜᴩ", style="success",
                 icon_custom_emoji_id=_E_ADD, callback_data="byp_add_group"),
            SBtn("ʀᴇᴍᴏᴠᴇ ɢʀᴏᴜᴩ", style="danger",
                 icon_custom_emoji_id=_E_MINUS, callback_data="byp_remove_group"),
        )
        for g in groups:
            gname2 = g.get("group_name") or "Unknown"
            gid2   = g["group_id"]
            mk.add(SBtn(f"{gname2}  ({gid2})", style="danger",
                        icon_custom_emoji_id=_E_TRASH, callback_data=f"byp_del_{gid2}"))
        mk.add(SBtn("ʙᴀᴄᴋ", style="primary",
                    icon_custom_emoji_id=_E_BACK, callback_data="adm_bypass_settings"))
        _safe_edit(cid, mid, bq(inner), parse_mode="HTML", reply_markup=mk)

    elif data == "adm_broadcast":
        pending_set(uid, "broadcast")
        _safe_send(cid, bq(f"{pe_bolt()} {b('ʙʀᴏᴀᴅᴄᴀꜱᴛ ᴍᴇꜱꜱᴀɢᴇ ʙʜᴇᴊᴏ (text/photo/video):')}"), parse_mode="HTML")
        _safe_answer(call.id)

# ============================================================
# ADMIN PENDING STATE HANDLER
# ============================================================

def handle_admin_pending(message):
    uid   = message.from_user.id
    cid   = message.chat.id
    state = pending_get(uid)
    if not state: return False

    if state == "set_image":
        if message.photo:
            fid = message.photo[-1].file_id
            db_set_welcome("image_file_id", fid)
            db_del_welcome("video_file_id")
            bot.reply_to(message, "✅ Welcome image set!", parse_mode="HTML")
        else:
            bot.reply_to(message, "❌ Photo bhejo!", parse_mode="HTML")
            return True
        pending_del(uid); return True

    elif state == "set_video":
        if message.video:
            fid = message.video.file_id
            db_set_welcome("video_file_id", fid)
            db_del_welcome("image_file_id")
            bot.reply_to(message, "✅ Welcome video set!", parse_mode="HTML")
        else:
            bot.reply_to(message, "❌ Video bhejo!", parse_mode="HTML")
            return True
        pending_del(uid); return True

    elif state == "add_public_channel":
        text  = (message.text or "").strip()
        uname = text.lstrip("@")
        if not uname:
            bot.reply_to(message, "❌ Valid username bhejo!", parse_mode="HTML")
            return True
        try:
            ch   = bot.get_chat(f"@{uname}")
            link = f"https://t.me/{uname}"
            add_force_channel(ch.title or uname, "channel", f"@{uname}", link, ch.id)
            bot.reply_to(message, f"✅ Added: {ch.title}", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}", parse_mode="HTML")
        pending_del(uid); return True

    elif state == "add_private_channel":
        text = (message.text or "").strip()
        if not text.startswith("https://t.me/"):
            bot.reply_to(message, "❌ Valid invite link bhejo!", parse_mode="HTML")
            return True
        add_force_channel("Private Channel", "channel", "", text, None)
        bot.reply_to(message, "✅ Private channel added!", parse_mode="HTML")
        pending_del(uid); return True

    elif state and state.startswith("set_bypass_api_url:"):
        try:
            _, pcid_s, pmid_s = state.split(":", 2)
            pcid = int(pcid_s); pmid = int(pmid_s)
        except Exception:
            pcid = cid; pmid = None
        text = (message.text or "").strip()
        try:
            new_url = set_bypass_api_url(text)
            try: bot.delete_message(cid, message.message_id)
            except: pass
            success = (f"{_HEADER}\n\n"
                       f"{pe_check()} {b('ʙʏᴩᴀꜱꜱ ᴀᴩɪ ᴜʀʟ ᴜᴩᴅᴀᴛᴇᴅ!')}\n\n"
                       f"{pe_info()} {b('ɴᴇᴡ ᴜʀʟ :')}\n<code>{_html_mod.escape(new_url)}</code>\n\n"
                       f"{_LOOP9}\n{_FOOTER}")
            if pmid:
                _safe_edit(pcid, pmid, bq(success), parse_mode="HTML",
                           reply_markup=bypass_settings_keyboard())
            else:
                _safe_send(cid, bq(success), parse_mode="HTML",
                           reply_markup=bypass_settings_keyboard())
            log_event("ʙʏᴩᴀꜱꜱ ᴀᴩɪ ᴜʀʟ ᴜᴩᴅᴀᴛᴇᴅ", message.from_user,
                      message.chat, lines=[_log_link("ᴜʀʟ :", new_url)], icon="🌐")
        except ValueError as exc:
            bot.reply_to(message, f"❌ {exc}\n\nExample: https://bypass-gc.onrender.com", parse_mode="HTML")
            return True
        pending_del(uid); return True

    elif state and state.startswith("add_allowed_group:"):
        try:
            _, pcid_s, pmid_s = state.split(":", 2)
            pcid = int(pcid_s); pmid = int(pmid_s)
        except Exception:
            pcid = cid; pmid = None
        text = (message.text or "").strip()
        try: bot.delete_message(cid, message.message_id)
        except: pass
        try:
            gid   = int(text)
            gname = "Unknown Group"
            try:
                ch    = bot.get_chat(gid)
                gname = ch.title or gname
            except: pass
            add_allowed_group(gid, gname)
            ok_inner = (f"{_HEADER}\n\n"
                        f"{pe_check()} {b('ɢʀᴏᴜᴩ ᴀᴅᴅᴇᴅ!')}\n\n"
                        f"{pe_group()} {b('ɴᴀᴍᴇ : ' + gname)}\n"
                        f"{pe_bolt()}  {b('ɪᴅ   : ' + str(gid))}\n\n"
                        f"{_LOOP9}\n{_FOOTER}")
            if pmid:
                _safe_edit(pcid, pmid, bq(ok_inner), parse_mode="HTML",
                           reply_markup=bypass_settings_keyboard())
        except ValueError:
            err_inner = (f"{_HEADER}\n\n"
                         f"{pe_cross()} {b('ᴠᴀʟɪᴅ ɢʀᴏᴜᴩ ɪᴅ ʙʜᴇᴊᴏ')}\n\n"
                         f"<code>ꜰᴏʀᴍᴀᴛ: -100xxxxxxxxxx</code>\n\n"
                         f"{_LOOP9}\n{_FOOTER}")
            if pmid:
                mk = InlineKeyboardMarkup()
                mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger",
                            icon_custom_emoji_id=_E_BACK, callback_data="adm_bypass_settings"))
                _safe_edit(pcid, pmid, bq(err_inner), parse_mode="HTML", reply_markup=mk)
        pending_del(uid); return True

    elif state and state.startswith("remove_allowed_group:"):
        try:
            _, pcid_s, pmid_s = state.split(":", 2)
            pcid = int(pcid_s); pmid = int(pmid_s)
        except Exception:
            pcid = cid; pmid = None
        text = (message.text or "").strip()
        try: bot.delete_message(cid, message.message_id)
        except: pass
        try:
            gid    = int(text)
            groups = get_allowed_groups()
            found  = next((g for g in groups if g["group_id"] == gid), None)
            if found:
                gname = found.get("group_name") or "Unknown"
                remove_allowed_group(gid)
                ok_inner = (f"{_HEADER}\n\n"
                            f"{pe_trash()} {b('ɢʀᴏᴜᴩ ʀᴇᴍᴏᴠᴇᴅ!')}\n\n"
                            f"{pe_group()} {b('ɴᴀᴍᴇ : ' + gname)}\n"
                            f"{pe_bolt()}  {b('ɪᴅ   : ' + str(gid))}\n\n"
                            f"{_LOOP9}\n{_FOOTER}")
                if pmid:
                    _safe_edit(pcid, pmid, bq(ok_inner), parse_mode="HTML",
                               reply_markup=bypass_settings_keyboard())
            else:
                warn_inner = (f"{_HEADER}\n\n"
                              f"{pe_info()} {b(f'ɢʀᴏᴜᴩ {gid} ᴀʟʟᴏᴡᴇᴅ ʟɪꜱᴛ ᴍᴇɪɴ ɴᴀʜɪ ʜᴀɪ')}\n\n"
                              f"{_LOOP9}\n{_FOOTER}")
                if pmid:
                    mk = InlineKeyboardMarkup()
                    mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger",
                                icon_custom_emoji_id=_E_BACK, callback_data="adm_bypass_settings"))
                    _safe_edit(pcid, pmid, bq(warn_inner), parse_mode="HTML", reply_markup=mk)
        except ValueError:
            err_inner = (f"{_HEADER}\n\n"
                         f"{pe_cross()} {b('ᴠᴀʟɪᴅ ɢʀᴏᴜᴩ ɪᴅ ʙʜᴇᴊᴏ')}\n\n"
                         f"<code>ꜰᴏʀᴍᴀᴛ: -100xxxxxxxxxx</code>\n\n"
                         f"{_LOOP9}\n{_FOOTER}")
            if pmid:
                mk = InlineKeyboardMarkup()
                mk.add(SBtn("ᴄᴀɴᴄᴇʟ", style="danger",
                            icon_custom_emoji_id=_E_BACK, callback_data="adm_bypass_settings"))
                _safe_edit(pcid, pmid, bq(err_inner), parse_mode="HTML", reply_markup=mk)
        pending_del(uid); return True

    elif state == "broadcast":
        users  = get_all_users()
        sent = failed = 0
        for u in users:
            try:
                # copy_message = premium/custom emoji + saari formatting as-is preserve
                try:
                    bot.copy_message(u["user_id"], message.chat.id, message.message_id,
                                     reply_markup=message.reply_markup)
                except Exception:
                    ents  = getattr(message, "entities", None)
                    cents = getattr(message, "caption_entities", None)
                    if message.text:
                        bot.send_message(u["user_id"], message.text, entities=ents)
                    elif message.photo:
                        bot.send_photo(u["user_id"], message.photo[-1].file_id,
                                       caption=message.caption or "", caption_entities=cents)
                    elif message.video:
                        bot.send_video(u["user_id"], message.video.file_id,
                                       caption=message.caption or "", caption_entities=cents)
                    else:
                        raise
                sent += 1
                time.sleep(0.05)
            except: failed += 1
        bot.reply_to(message, f"✅ Broadcast done!\n📤 Sent: {sent}\n❌ Failed: {failed}", parse_mode="HTML")
        pending_del(uid); return True

    return False

# ============================================================
# /STATS
# ============================================================

@bot.message_handler(commands=["stats"])
def stats_command(message):
    if message.from_user.id != OWNER_ID: return
    users       = get_all_users()
    cache_count = _col_links.count_documents({})
    _stats_inner  = f"{_HEADER}\n\n"
    _stats_inner += f"{pe_bolt()} {b('📊 BOT STATS')}\n\n"
    _stats_inner += f"{pe_party()} {b('Users        : ' + str(len(users)))}\n"
    _stats_inner += f"{pe_chain()} {b('Total Bypass : ' + str(sum(u.get('bypass_count',0) for u in users)))}\n"
    _stats_inner += f"{pe_bolt()}  {b('Active Users : ' + str(sum(1 for u in users if u.get('bypass_count',0) > 0)))}\n"
    _stats_inner += f"{pe_lock()}  {b('Cached DB    : ' + str(cache_count))}\n"
    _stats_inner += f"{pe_star()}  {b('Cached RAM   : ' + str(len(_bypass_cache)))}\n"
    _stats_inner += f"{pe_check()} {b('Status: Online 🟢')}\n\n"
    _stats_inner += f"{_LOOP9}\n{_FOOTER}"
    _safe_send(message.chat.id, bq(_stats_inner), parse_mode="HTML")

# ============================================================
# MAIN MESSAGE HANDLER
# ============================================================

def _handle_messages_worker(message):
    if not message.from_user or message.from_user.is_bot: return

    uid      = message.from_user.id
    cid      = message.chat.id
    fname    = message.from_user.first_name
    uname    = message.from_user.username or ""
    is_group = message.chat.type in ["group", "supergroup"]
    is_dm    = message.chat.type == "private"
    text     = (message.text or message.caption or "").strip()

    if uid == OWNER_ID and pending_get(uid):
        if handle_admin_pending(message): return

    # ── DM / Group bypass toggle enforcement (admin panel) ───────────────
    if uid != OWNER_ID:
        if is_dm and not get_dm_bypass():
            try:
                _safe_send(
                    cid,
                    _dm_blocked_message(),
                    parse_mode="HTML",
                    reply_markup=_dm_blocked_markup(),
                    reply_to_message_id=message.message_id,
                )
            except Exception as _dmoff_e:
                print(f"[DM_OFF] {_dmoff_e}")
            return
        if is_group and not get_all_groups() and not is_allowed_group(cid, getattr(message.chat, "username", "") or ""):
            try:
                _safe_send(
                    cid,
                    _wrong_group_message(),
                    parse_mode="HTML",
                    reply_markup=_wrong_group_markup(),
                    reply_to_message_id=message.message_id,
                )
            except Exception as _grpoff_e:
                print(f"[GROUP_OFF] {_grpoff_e}")
            return

    # ── Trigger detect (group ke liye zaroori, DM me optional) ───────────
    has_trigger, _rest_text = _trigger_text(text)
    _scan_text = _rest_text if has_trigger else text

    url = extract_url(_scan_text)
    # "b" / "/bypass" reply ke saath — link replied message se lo
    if not url and has_trigger and getattr(message, "reply_to_message", None):
        _r = message.reply_to_message
        url = extract_url((_r.text or _r.caption or "") if _r else "")

    if url:
        _POOL.submit(_safe_react, cid, message.message_id, "⚡", False)

    # Every user is treated the same — no premium system, DM + all groups open.
    if is_dm:
        if not url or not is_bypassable_url(url):
            # Non-link DM: always answer instead of staying silent.
            try:
                _safe_send(
                    cid,
                    bq(f"{pe_gift()} {b('ᴀᴩɴᴀ ꜱʜᴏʀᴛɴᴇʀ ʟɪɴᴋ ʙʜᴇᴊᴏ!')}\n"
                       f"{pe_bolt()} {b('ᴇxᴀᴍᴩʟᴇ :')} <code>https://just2earn.com/qXekdh</code>"),
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )
            except Exception as _hint_e:
                print(f"[DM_HINT] {_hint_e}")
            log_event("ɴᴏɴ-ʟɪɴᴋ ᴍᴇꜱꜱᴀɢᴇ (ᴅᴍ)", message.from_user, message.chat,
                      lines=[f"📝 <b>ᴛᴇxᴛ :</b> <code>{_html_mod.escape(text[:80]) or '—'}</code>"],
                      icon="💬")
            return

    if is_group:
        # Group me sirf command ke saath hi bypass: /bypass url | /b url | b url
        if not has_trigger: return
        if not url or not is_bypassable_url(url):
            try:
                _safe_send(
                    cid,
                    bq(f"{pe_bolt()} {b('ꜱᴀʜɪ ᴛᴀʀɪQᴀ :')}\n"
                       f"<code>/bypass https://example.com/abc</code>\n"
                       f"<code>/b https://example.com/abc</code>\n"
                       f"<code>b https://example.com/abc</code>"),
                    parse_mode="HTML",
                    reply_to_message_id=message.message_id,
                )
            except Exception as _ge:
                print(f"[GROUP_HINT] {_ge}")
            return

    if not is_group and not is_dm: return
    if not url: return
    if not is_bypassable_url(url): return

    _POOL.submit(register_user, uid, fname, uname)

    if not is_user_definitely_joined(uid):
        nj = check_user_joined_all_cached(uid)
        if nj:
            log_event("ꜰᴏʀᴄᴇ ᴊᴏɪɴ ꜱʜᴏᴡɴ", message.from_user, message.chat,
                      lines=[f"🔒 <b>ᴩᴇɴᴅɪɴɢ :</b> {len(nj)} channel(s)"], icon="🔒")
            _safe_send(cid, get_force_join_message(fname, nj),
                             parse_mode="HTML",
                             reply_markup=force_join_markup(nj, uid, cid, message.message_id),
                             reply_to_message_id=message.message_id)
            return

    usage_count = increment_user_stat(uid)
    _POOL.submit(increment_bypass, uid)

    log_event("ʟɪɴᴋ ʀᴇQᴜᴇꜱᴛ", message.from_user, message.chat,
              lines=[_log_link("🔗 <b>ʟɪɴᴋ :</b>", url),
                     f"📊 <b>ᴜꜱᴀɢᴇ :</b> #{usage_count}"], icon="⚡")

    # Cache check — INSTANT reply
    cached = cache_get_bypassed(url)
    if cached:
        _c_url   = cached.get("url")
        _c_extra = cached.get("extra") or {}
        log_event("ʙʏᴩᴀꜱꜱ ᴅᴏɴᴇ (ᴄᴀᴄʜᴇ)", message.from_user, message.chat,
                  lines=[_log_link("🔗 <b>ᴏʀɪɢɪɴᴀʟ :</b>", url),
                         _log_link("✅ <b>ʙʏᴩᴀꜱꜱᴇᴅ :</b>", _c_url),
                         "💾 <b>ꜱᴏᴜʀᴄᴇ :</b> ᴄᴀᴄʜᴇ (ɪɴꜱᴛᴀɴᴛ)"], icon="⚡")
        final_txt, final_mk = format_output(
            url, _c_url, cached.get("time", 0.0), usage_count, 0, fname,
            source=cached.get("source", ""), extra=_c_extra,
        )
        try:
            sent_cached = _safe_send(
                cid, final_txt,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=final_mk,
                reply_to_message_id=message.message_id,
            )
            if sent_cached:
                _POOL.submit(_safe_react, cid, sent_cached.message_id, "⚡", True)
                _schedule_delete(cid, sent_cached.message_id, message.message_id)
        except: pass
        return

    # Fresh bypass — API call + loading animation
    _result_holder = {}
    _bypass_done   = threading.Event()

    def _do_bypass():
        _result_holder['res'] = bypass_url_with_retry(url)
        _bypass_done.set()

    _bypass_t = threading.Thread(target=_do_bypass, daemon=True)
    _bypass_t.start()

    try:
        proc_msg = _safe_send(
            cid, get_loading_msg(0),
            parse_mode="HTML",
            reply_to_message_id=message.message_id,
        )
    except Exception as _sm:
        print(f"[LOADING] send failed: {_sm}")
        _bypass_t.join()
        bypassed, is_err, retry_cnt, total_time, src, extra = _result_holder['res']
        if not is_err and bypassed and not bypassed.startswith(("http://","https://")):
            bypassed = "https://" + bypassed.lstrip("//")
        if not is_err:
            cache_save_bypassed(url, bypassed, extra, total_time, src)
        final_txt, final_mk = format_output(url, bypassed, total_time, usage_count, retry_cnt, fname,
                                            source=src, extra=extra)
        try:
            sent_fb = _safe_send(cid, final_txt, parse_mode="HTML",
                                 disable_web_page_preview=True, reply_markup=final_mk)
            if sent_fb:
                _schedule_delete(cid, sent_fb.message_id, message.message_id)
        except Exception as _fb_e:
            print(f"[BYPASS_FB] send failed: {_fb_e}")
        return

    # Animate while waiting
    # API max wait ≈ 75s (parallel race: DZHQ + Alex)
    # Phase 0: steps 1-8   @ 1.5s each  =  12s  (fast early progress)
    # Phase 1: steps 9-13  @ 5.0s each  =  25s  (mid-wait)
    # Phase 2: steps 14-15 @ 8.0s each  =  16s  (last two steps)
    # After all steps exhausted, _bypass_t.join() waits for actual result (no cap)
    _PHASE_SPEED = [(8, 1.5), (5, 5.0), (99, 8.0)]
    bar_idx = 1; phase_step = 0; phase_idx = 0
    phase_limit, phase_interval = _PHASE_SPEED[0]

    while not _bypass_done.is_set() and bar_idx < len(PROGRESS_STEPS):
        try:
            _safe_edit(
                proc_msg.chat.id, proc_msg.message_id,
                get_loading_msg(bar_idx),
                parse_mode="HTML",
            )
        except: pass
        bar_idx += 1; phase_step += 1
        if phase_step >= phase_limit and phase_idx < len(_PHASE_SPEED) - 1:
            phase_idx += 1; phase_step = 0
            phase_limit, phase_interval = _PHASE_SPEED[phase_idx]
        _bypass_done.wait(timeout=phase_interval)

    _bypass_t.join()
    bypassed, is_err, retry_cnt, total_time, src, extra = _result_holder['res']

    if not is_err and bypassed and not bypassed.startswith(("http://", "https://")):
        bypassed = ("https:" if bypassed.startswith("//") else "https://") + bypassed.lstrip("//")

    if not is_err:
        _POOL.submit(cache_save_bypassed, url, bypassed, extra, total_time, src)

    final_txt, final_mk = format_output(url, bypassed, total_time, usage_count, retry_cnt, fname,
                                        source=src, extra=extra)
    log_event("ʙʏᴩᴀꜱꜱ ꜰᴀɪʟᴇᴅ" if is_err else "ʙʏᴩᴀꜱꜱ ᴅᴏɴᴇ (ꜰʀᴇꜱʜ)",
              message.from_user, message.chat,
              lines=[_log_link("🔗 <b>ᴏʀɪɢɪɴᴀʟ :</b>", url),
                     _log_link("✅ <b>ʀᴇꜱᴜʟᴛ :</b>", bypassed),
                     f"⏱ <b>ᴛɪᴍᴇ :</b> {total_time}s · <b>ʀᴇᴛʀʏ :</b> {retry_cnt}",
                     f"🛠 <b>ᴀᴩɪ :</b> {_html_mod.escape(str(src or '—'))}"],
              icon="❌" if is_err else "✅")
    try:
        _safe_edit(
            proc_msg.chat.id, proc_msg.message_id,
            final_txt,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=final_mk,
        )
        _POOL.submit(_safe_react, cid, proc_msg.message_id, "⚡", True)
        _schedule_delete(cid, proc_msg.message_id, message.message_id)
    except:
        try:
            sent_fb2 = _safe_send(proc_msg.chat.id, final_txt, parse_mode="HTML",
                            disable_web_page_preview=True, reply_markup=final_mk)
            if sent_fb2:
                _schedule_delete(cid, sent_fb2.message_id, message.message_id)
        except: pass


@bot.message_handler(func=lambda m: True, content_types=[
    "text", "photo", "video", "animation", "sticker", "document", "audio", "voice"
])
def handle_messages(message):
    _POOL.submit(_handle_messages_worker, message)

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════╗")
    print("║     FELIX BYPASS BOT — MONGODB EDITION      ║")
    print("║   SQLite removed · Channel backup removed   ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"Bot: {BOT_USERNAME}  |  Owner: {OWNER_USERNAME}")
    _uri_shown = _MONGO_URI_STANDARD or _MONGO_URI_SRV
    print(f"MongoDB mode: {'STANDARD (non-SRV)' if _MONGO_URI_STANDARD else 'SRV'}  |  host: {_uri_shown.split('@')[-1][:40]}...")

    # STEP 1: Connect MongoDB + create indexes
    print("\n[STARTUP] STEP 1: Connecting to MongoDB...")
    try:
        _mongo_client.admin.command("ping")
        print("[STARTUP] ✅ MongoDB connected!")
    except Exception as _me:
        print(f"[STARTUP] ❌ MongoDB connection failed: {_me}")
        print("[STARTUP] Set MONGO_URI (SRV) or MONGO_URI_STANDARD (non-SRV) env vars correctly.")
        raise SystemExit(1)

    _init_indexes()

    # STEP 2: Seed default channels if empty
    print("[STARTUP] STEP 2: Seeding defaults...")
    _seed_default_channels()

    # STEP 3: Warm all RAM caches
    print("[STARTUP] STEP 3: Warming RAM caches...")
    _warm_all_caches()
    _get_bot_id()

    print("\n[STARTUP] ✅ ALL SYSTEMS GO — Starting polling...\n")
    print(f"🤖 {BOT_USERNAME}  |  👑 {OWNER_USERNAME}")
    print(f"🍃 MongoDB: {MONGO_DB} database")

    try:
        _do_log(f"🟢 <b>ʙᴏᴛ ꜱᴛᴀʀᴛᴇᴅ</b>\n━━━━━━━━━━━━━━━━━━\n"
                f"🤖 <b>ʙᴏᴛ :</b> {BOT_USERNAME}\n🕒 <b>ᴛɪᴍᴇ :</b> {_now_ist()}")
    except Exception as _se:
        print(f"[LOG_CH] startup ping failed: {_se}")

    bot.remove_webhook()
    bot.infinity_polling(
        timeout=20,
        long_polling_timeout=15,
        skip_pending=True,
        allowed_updates=["message", "callback_query", "chat_join_request"],
        interval=0,
        restart_on_change=False,
    )
