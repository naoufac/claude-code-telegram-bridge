#!/usr/bin/env python3
"""Telegram <-> headless Claude Code bridge.

Long-polls Telegram getUpdates, feeds owner messages to `claude -p`
(full tool access, --resume session continuity), streams Claude's
text back to the chat. Owner-gated. One poller per token, enforced
by flock.

Bridge-level commands: /start /new /stop /status
Everything else (including other /slash commands) is passed to Claude
verbatim, so Claude Code skills work from Telegram.
"""
import base64
import collections
import fcntl
import glob
import hmac
import json
import os
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.expanduser("~")
TOKEN = os.environ["TG_TOKEN"]
OWNER = int(os.environ["OWNER_ID"])  # your numeric Telegram user id — required
API = "https://api.telegram.org/bot" + TOKEN
CLAUDE_BIN = os.environ.get("CLAUDE_BIN", os.path.join(HOME_DIR, ".local/bin/claude"))
WORKDIR = os.environ.get("CLAUDE_WORKDIR", HOME_DIR)
RUN_TIMEOUT = int(os.environ.get("RUN_TIMEOUT", "3600"))
BRIDGE_LABEL = os.environ.get("BRIDGE_LABEL", "this machine")
STATE_PATH = os.path.join(BASE, "state.json")
LOCK_PATH = os.path.join(BASE, "poller.lock")
LOG_PATH = os.path.join(BASE, "bridge.log")
HOOK = "%s %s" % (sys.executable, os.path.join(BASE, "inbox_hook.py"))
HOOK_SETTINGS = json.dumps({"hooks": {
    "PostToolUse": [{"matcher": "", "hooks": [{"type": "command", "command": HOOK + " PostToolUse"}]}],
    "Stop": [{"hooks": [{"type": "command", "command": HOOK + " Stop"}]}],
}})
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", os.path.join(HOME_DIR, "tg-uploads"))
PROJECTS_DIR = os.path.join(HOME_DIR, ".claude", "projects")
ELEVENLABS_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
DASH_PORT = int(os.environ.get("DASH_PORT", "0"))  # 0 = dashboard disabled
DASH_KEY = os.environ.get("DASH_KEY", "")
BOOT_TS = time.time()

SYSTEM_APPEND = (
    "This conversation is relayed through a Telegram bot bridge; the user is "
    "on their phone. Keep replies tight and phone-readable: short paragraphs, "
    "no tables, minimal Markdown. For long output, write it to a file and "
    "send the file instead. You can send files straight to the user's "
    "Telegram chat (env vars TG_TOKEN and TG_CHAT_ID are set): "
    "curl -s -F chat_id=$TG_CHAT_ID -F document=@/path/to/file "
    "\"https://api.telegram.org/bot$TG_TOKEN/sendDocument\" "
    "(use -F photo=@... with sendPhoto for images)."
)

CLAUDE_ENV = {
    "HOME": HOME_DIR,
    "USER": os.environ.get("USER", "root"),
    "PATH": os.path.join(HOME_DIR, ".local/bin") + ":/usr/local/bin:/usr/bin:/bin",
    "IS_SANDBOX": "1",
    "TERM": "dumb",
    "LANG": "C.UTF-8",
}

state_lock = threading.Lock()
proc_lock = threading.Lock()
current_proc = None  # claude subprocess currently running, if any
work_q = queue.Queue()
reject_throttle = {}  # chat_id -> last "Private bot." reply ts
group_lock = threading.Lock()
pending_groups = {}  # media_group_id -> {chat_id, entries: [(path, kind)], captions, ts}
reboot_requested = threading.Event()


def log(msg):
    line = "%s %s" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg)
    print(line, flush=True)


def load_state():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"offset": 0, "sessions": {}, "last_announce": 0}


def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f)
    os.replace(tmp, STATE_PATH)


STATE = load_state()

sys.path.insert(0, BASE)
from native_commands import HELP, MENU  # noqa: E402

EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")
MODEL = STATE.get("primary_model", "")
EFFORT = STATE.get("effort", "")
if EFFORT not in EFFORT_LEVELS:
    EFFORT = ""
if "turn_timeout" in STATE:
    RUN_TIMEOUT = int(STATE["turn_timeout"])
CLAUDE_MODELS = ("opus", "sonnet", "haiku", "fable")
GLM_BIN = os.environ.get("GLM_BIN", "glm")
GLM_TIMEOUT = int(os.environ.get("ZAI_TIMEOUT", "900"))
KNOWN_MODELS = ("claude-opus-5-5", "claude-fable-5-1", "claude-sonnet-5-5", "claude-haiku-4-5-20251001")


def _ago(ts):
    d = time.time() - ts
    return "%dm ago" % (d // 60) if d < 86400 else "%.1fd ago" % (d / 86400)


def tg(method, payload, timeout=70):
    req = urllib.request.Request(
        API + "/" + method,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        log("tg %s HTTP %s: %s" % (method, e.code, body))
        try:
            return json.loads(body)
        except ValueError:
            return {"ok": False, "error_code": e.code, "description": body}
    except Exception as e:
        log("tg %s error: %r" % (method, e))
        return {"ok": False, "description": repr(e)}


def multipart_post(url, fields, file_field=None, file_path=None,
                   content_type="application/octet-stream", headers=None, timeout=120):
    """Stdlib multipart/form-data POST. Returns parsed JSON (or {} on failure)."""
    boundary = "----nao" + uuid.uuid4().hex
    parts = []
    for k, v in (fields or {}).items():
        parts.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n"
                      % (boundary, k, v)).encode())
    if file_field and file_path:
        with open(file_path, "rb") as f:
            blob = f.read()
        parts.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\n"
                      "Content-Type: %s\r\n\r\n"
                      % (boundary, file_field, os.path.basename(file_path), content_type)).encode())
        parts.append(blob)
        parts.append(b"\r\n")
    parts.append(("--%s--\r\n" % boundary).encode())
    body = b"".join(parts)
    req = urllib.request.Request(url, data=body, headers=dict(headers or {}))
    req.add_header("Content-Type", "multipart/form-data; boundary=%s" % boundary)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        log("multipart %s HTTP %s: %s" % (url.split("/")[-1].split("?")[0], e.code, body))
        try:
            return json.loads(body)
        except ValueError:
            return {"ok": False, "error_code": e.code, "description": body}
    except Exception as e:
        log("multipart error: %r" % e)
        return {}


def download_tg_file(file_id, name_hint):
    """Fetch a Telegram file to UPLOAD_DIR; returns local path or None."""
    r = tg("getFile", {"file_id": file_id}, timeout=30)
    if not r.get("ok"):
        return None
    remote = r["result"]["file_path"]
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name_hint)[-80:]
    if "." not in safe and "." in remote:
        safe += "." + remote.rsplit(".", 1)[-1]
    local = os.path.join(UPLOAD_DIR, "%s-%s" % (time.strftime("%Y%m%d-%H%M%S"), safe))
    url = "https://api.telegram.org/file/bot%s/%s" % (TOKEN, remote)
    try:
        with urllib.request.urlopen(url, timeout=180) as src, open(local, "wb") as dst:
            while True:
                chunk = src.read(1 << 16)
                if not chunk:
                    break
                dst.write(chunk)
        return local
    except Exception as e:
        log("download failed: %r" % e)
        return None


def stt(path):
    """Transcribe audio: ElevenLabs scribe, then Gemini. Returns (text, source)."""
    if ELEVENLABS_KEY:
        r = multipart_post(
            "https://api.elevenlabs.io/v1/speech-to-text",
            {"model_id": "scribe_v1"},
            "file", path, "audio/ogg",
            headers={"xi-api-key": ELEVENLABS_KEY},
        )
        if r.get("text"):
            return r["text"].strip(), "elevenlabs"
        log("elevenlabs stt failed: %s" % str(r)[:200])
    if GEMINI_KEY and os.path.getsize(path) < 15 << 20:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        req = urllib.request.Request(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + GEMINI_KEY,
            data=json.dumps({"contents": [{"parts": [
                {"text": "Transcribe this audio verbatim. Output only the transcript."},
                {"inline_data": {"mime_type": "audio/ogg", "data": b64}},
            ]}]}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.load(r)
            text = d["candidates"][0]["content"]["parts"][0]["text"].strip()
            if text:
                return text, "gemini"
        except Exception as e:
            log("gemini stt failed: %r" % e)
    return None, None


def chunk_text(text, limit=3900):
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n", limit // 2, limit)
        if cut == -1:
            cut = text.rfind(" ", limit // 2, limit)
        if cut == -1:
            cut = limit
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


def send_text(chat_id, text):
    if not text or not text.strip():
        return
    text = text.replace(TOKEN, "‹bot-token›")
    for chunk in chunk_text(text):
        r = tg("sendMessage", {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=30)
        if r.get("ok"):
            continue
        if r.get("error_code") == 400:
            # Almost always a Markdown entity-parse failure: resend plain.
            tg("sendMessage", {
                "chat_id": chat_id,
                "text": chunk,
                "disable_web_page_preview": True,
            }, timeout=30)
        else:
            # Transient (timeout/5xx): one retry, then plain as last resort.
            time.sleep(2)
            r2 = tg("sendMessage", {
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=30)
            if not r2.get("ok"):
                tg("sendMessage", {
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                }, timeout=30)


def typing_loop(chat_id, stop_evt):
    while not stop_evt.wait(7):
        tg("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=15)


def get_session(chat_id):
    with state_lock:
        return STATE["sessions"].get(str(chat_id))


def set_session(chat_id, sid):
    with state_lock:
        if sid:
            STATE["sessions"][str(chat_id)] = sid
        else:
            STATE["sessions"].pop(str(chat_id), None)
        save_state(STATE)


def session_file_exists(sid):
    munged = WORKDIR.replace("/", "-")
    return os.path.exists(os.path.join(PROJECTS_DIR, munged, sid + ".jsonl"))


def session_snippet(path, max_lines=120):
    """First real user-text line of a session jsonl, for listings."""
    try:
        with open(path) as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                m = obj.get("message") or {}
                if obj.get("type") != "user" or m.get("role") != "user":
                    continue
                c = m.get("content")
                if isinstance(c, list):
                    c = " ".join(b.get("text", "") for b in c if isinstance(b, dict) and b.get("type") == "text")
                if not isinstance(c, str):
                    continue
                c = c.strip()
                if not c or c.startswith("<"):  # skip system-reminder/command wrappers
                    continue
                return " ".join(c.split())[:60]
    except OSError:
        pass
    return "(no user text)"


def list_sessions(limit=8):
    """Recent sessions for WORKDIR: [(sid, age_str, size_kb, snippet)], newest first."""
    munged = WORKDIR.replace("/", "-")
    files = [p for p in glob.glob(os.path.join(PROJECTS_DIR, munged, "*.jsonl"))
             if ".bak" not in os.path.basename(p)]
    files.sort(key=os.path.getmtime, reverse=True)
    out = []
    for p in files[:limit]:
        sid = os.path.basename(p)[:-6]
        age = time.time() - os.path.getmtime(p)
        if age < 3600:
            age_s = "%dm" % (age // 60)
        elif age < 86400:
            age_s = "%dh" % (age // 3600)
        else:
            age_s = "%dd" % (age // 86400)
        out.append((sid, age_s, os.path.getsize(p) // 1024, session_snippet(p)))
    return out


def resolve_session(prefix):
    """Unique session id matching prefix, or (None, matches)."""
    munged = WORKDIR.replace("/", "-")
    stems = [os.path.basename(p)[:-6] for p in glob.glob(os.path.join(PROJECTS_DIR, munged, "*.jsonl"))
             if ".bak" not in os.path.basename(p)]
    hits = [s for s in stems if s.startswith(prefix)]
    return (hits[0], hits) if len(hits) == 1 else (None, hits)


def inbox_path(chat_id):
    return os.path.join(BASE, "inbox-%s.txt" % chat_id)


def take_inbox(chat_id):
    path = inbox_path(chat_id)
    taken = path + ".bridge"
    try:
        os.rename(path, taken)
    except OSError:
        return ""
    with open(taken) as f:
        text = f.read().strip()
    os.unlink(taken)
    return text


def run_claude(chat_id, prompt, retried=False):
    """Run one claude turn, streaming text blocks back to the chat."""
    global current_proc
    sid = get_session(chat_id)
    if sid and not session_file_exists(sid):
        log("session %s gone from disk — starting fresh" % sid)
        set_session(chat_id, None)
        sid = None
    cmd = [
        CLAUDE_BIN, "-p", prompt,
        "--output-format", "stream-json", "--verbose",
        "--dangerously-skip-permissions",
        "--append-system-prompt", SYSTEM_APPEND,
        "--settings", HOOK_SETTINGS,
    ]
    if MODEL:
        cmd += ["--model", MODEL]
    if EFFORT:
        cmd += ["--effort", EFFORT]
    if sid:
        cmd += ["--resume", sid]
    run_env = dict(CLAUDE_ENV, TG_TOKEN=TOKEN, TG_CHAT_ID=str(chat_id),
                   BRIDGE_INBOX=inbox_path(chat_id))
    if not retried:
        with state_lock:
            STATE.setdefault("last_prompt", {})[str(chat_id)] = prompt
            save_state(STATE)

    stop_evt = threading.Event()
    threading.Thread(target=typing_loop, args=(chat_id, stop_evt), daemon=True).start()

    started = time.time()
    last_sent = None
    new_sid = sid
    result_seen = False
    need_heal = False
    killed = [False]
    stderr_tail = collections.deque(maxlen=40)
    proc = None

    def reaper(p):
        while p.poll() is None:
            if RUN_TIMEOUT > 0 and time.time() - started > RUN_TIMEOUT:
                killed[0] = True
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except OSError:
                    pass
                return
            time.sleep(5)

    def drain_stderr(p):
        # Keep the stderr pipe empty so a chatty child can't block on it.
        try:
            for line in p.stderr:
                stderr_tail.append(line.rstrip())
        except (OSError, ValueError):
            pass

    try:
        proc = subprocess.Popen(
            cmd, cwd=WORKDIR, env=run_env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
        with proc_lock:
            current_proc = proc
        threading.Thread(target=reaper, args=(proc,), daemon=True).start()
        err_t = threading.Thread(target=drain_stderr, args=(proc,), daemon=True)
        err_t.start()

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            etype = ev.get("type")
            if etype == "system" and ev.get("subtype") == "init":
                new_sid = ev.get("session_id") or new_sid
            elif etype == "assistant":
                for block in (ev.get("message") or {}).get("content") or []:
                    if block.get("type") == "text" and block.get("text", "").strip():
                        send_text(chat_id, block["text"])
                        last_sent = block["text"]
            elif etype == "result":
                result_seen = True
                new_sid = ev.get("session_id") or new_sid
                cost_usd = ev.get("total_cost_usd")
                if isinstance(cost_usd, (int, float)) and cost_usd >= 0:
                    with state_lock:
                        STATE["last_cost"] = {"usd": cost_usd, "ts": time.time(), "model": MODEL or "account default", "effort": EFFORT or "account default"}
                        STATE["cost_total"] = round(STATE.get("cost_total", 0.0) + cost_usd, 6)
                        save_state(STATE)
                text = ev.get("result") or ""
                if ev.get("is_error"):
                    if sid and last_sent is None and not retried:
                        # Resumed run died before producing anything —
                        # treat the session as broken and heal below.
                        need_heal = True
                    else:
                        send_text(chat_id, "⚠️ run ended with an error:\n%s" % (text or ev.get("subtype", "unknown"))[:1500])
                elif text and text != last_sent:
                    send_text(chat_id, text)

        proc.wait(timeout=30)
        err_t.join(timeout=5)
    finally:
        stop_evt.set()
        with proc_lock:
            current_proc = None

    if new_sid != sid:
        with state_lock:
            # Compare-and-set: a /attach issued mid-run wins over this
            # finished run's session id.
            if STATE["sessions"].get(str(chat_id)) == sid:
                if new_sid:
                    STATE["sessions"][str(chat_id)] = new_sid
                    save_state(STATE)

    err_text = "\n".join(stderr_tail)[-1200:]
    rc = proc.returncode if proc else None
    log("run done chat=%s %.1fmin rc=%s sid=%s" % (chat_id, (time.time() - started) / 60, rc, new_sid))

    if killed[0]:
        send_text(chat_id, "⏱ run hit the %d-minute limit and was stopped. Session kept — send a follow-up to continue." % (RUN_TIMEOUT // 60))
    elif need_heal or (not result_seen and rc not in (0, None) and sid and not retried):
        # Stale/broken --resume target: drop the session and replay this
        # prompt once on a fresh one.
        log("resume failed (rc=%s), retrying fresh: %s" % (rc, err_text[-200:]))
        set_session(chat_id, None)
        send_text(chat_id, "♻️ Stored session was unresumable — restarted fresh, re-running your message.")
        return run_claude(chat_id, prompt, retried=True)
    elif not result_seen:
        if rc not in (0, None):
            send_text(chat_id, "⚠️ claude exited (code %s) without a result.\n%s" % (rc, err_text))
        elif last_sent is None:
            send_text(chat_id, "⚠️ run ended with no output (stopped?).")


def run_glm(chat_id, prompt):
    """Run one task on a separate GLM worker command (GLM_BIN reads a prompt on stdin) and relay the answer."""
    global current_proc
    proc = None
    try:
        proc = subprocess.Popen([GLM_BIN, "-"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, start_new_session=True)
        with proc_lock:
            current_proc = proc
        try:
            out, err = proc.communicate(prompt, timeout=GLM_TIMEOUT)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
            send_text(chat_id, "GLM worker timed out after %d minutes." % (GLM_TIMEOUT // 60))
            return
    except OSError as e:
        send_text(chat_id, "GLM worker could not start: %r" % e)
        return
    finally:
        with proc_lock:
            current_proc = None
    if proc.returncode == 0 and out.strip():
        send_text(chat_id, "GLM worker:\n" + out.strip())
    else:
        send_text(chat_id, "GLM worker failed (code %s): %s" % (proc.returncode, (err or out).strip()[-600:]))


def queue_prompt(chat_id, prompt, provider="claude"):
    with proc_lock:
        busy = current_proc is not None and current_proc.poll() is None
    if busy or work_q.qsize():
        send_text(chat_id, "⏳ Busy — queued behind %d. Use /go <message> to send it into the running turn instead."
                  % (work_q.qsize() + (1 if busy else 0)))
    work_q.put((chat_id, prompt, provider))


def extract_media(msg):
    """Returns (file_id, name_hint, kind) for the message's attachment, or None."""
    if msg.get("document"):
        d = msg["document"]
        return d["file_id"], d.get("file_name", "document"), "document"
    if msg.get("photo"):
        p = max(msg["photo"], key=lambda x: x.get("file_size", 0))
        return p["file_id"], "photo.jpg", "image"
    if msg.get("voice"):
        return msg["voice"]["file_id"], "voice.oga", "voice"
    if msg.get("audio"):
        a = msg["audio"]
        return a["file_id"], a.get("file_name", "audio.mp3"), "audio"
    if msg.get("video"):
        return msg["video"]["file_id"], "video.mp4", "video"
    if msg.get("video_note"):
        return msg["video_note"]["file_id"], "videonote.mp4", "video"
    return None


def media_prompt(entries, caption):
    lines = ["[Telegram upload%s from the user:]" % ("s" if len(entries) > 1 else "")]
    for path, kind in entries:
        try:
            kb = os.path.getsize(path) / 1024
        except OSError:
            kb = 0
        lines.append("- %s: %s (%.0f KB)" % (kind, path, kb))
    lines.append("")
    lines.append(caption if caption else
                 "No caption was given. Inspect the upload%s (images can be Read directly) and respond appropriately."
                 % ("s" if len(entries) > 1 else ""))
    return "\n".join(lines)


def handle_media(msg, chat_id):
    media = extract_media(msg)
    if not media:
        send_text(chat_id, "Unsupported message type — text, files, photos, voice, audio and video work.")
        return
    file_id, name_hint, kind = media
    gid = msg.get("media_group_id")
    if gid:
        # Register with the album BEFORE the (slow) download so the flusher
        # waits for every item instead of splitting mixed-speed albums.
        with group_lock:
            g = pending_groups.setdefault(gid, {"chat_id": chat_id, "entries": [],
                                                "captions": [], "ts": time.time(), "pending": 0})
            g["pending"] += 1
    tg("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=15)
    path = download_tg_file(file_id, name_hint)
    caption = (msg.get("caption") or "").strip()
    if gid:
        with group_lock:
            g = pending_groups.get(gid)
            if g:
                g["pending"] -= 1
                g["ts"] = time.time()
                if path:
                    g["entries"].append((path, kind))
                if caption:
                    g["captions"].append(caption)
        if not path:
            send_text(chat_id, "⚠️ couldn't download one album item (20 MB bot-API limit?).")
        return  # flusher queues the batch once the album is complete
    if not path:
        send_text(chat_id, "⚠️ couldn't download that from Telegram (20 MB bot-API limit?).")
        return

    if kind in ("voice", "audio"):
        text, src = stt(path)
        if text:
            send_text(chat_id, "🎤 heard (%s): %s" % (src, text[:300]))
            queue_prompt(chat_id, "[Voice message from the user, transcribed via %s; audio file: %s]\n%s%s"
                         % (src, path, ("Caption: %s\n" % caption) if caption else "", text))
        else:
            send_text(chat_id, "⚠️ transcription failed (both providers) — passing the audio file to Claude.")
            queue_prompt(chat_id, media_prompt([(path, kind)], caption or
                         "Transcription was unavailable. The audio file is at the path above; ffmpeg is installed if you need it."))
        return

    queue_prompt(chat_id, media_prompt([(path, kind)], caption))


def handle_media_safe(msg, chat_id):
    try:
        handle_media(msg, chat_id)
    except Exception as e:
        log("handle_media error: %r" % e)
        send_text(chat_id, "⚠️ media processing failed: %r" % e)


def group_flusher():
    while True:
        time.sleep(1)
        try:
            ready = []
            with group_lock:
                for gid, g in list(pending_groups.items()):
                    if g["pending"] == 0 and time.time() - g["ts"] > 2.5:
                        ready.append(pending_groups.pop(gid))
            for g in ready:
                if g["entries"]:
                    queue_prompt(g["chat_id"], media_prompt(g["entries"], " ".join(g["captions"])))
        except Exception as e:
            log("group_flusher error: %r" % e)


def worker():
    while True:
        chat_id, prompt, provider = work_q.get()
        try:
            if provider == "zai":
                run_glm(chat_id, prompt)
                continue
            run_claude(chat_id, prompt)
            late = take_inbox(chat_id)
            if late:
                send_text(chat_id, "📨 Your /go message arrived as the turn ended. Running it now.")
                work_q.put((chat_id, late, "claude"))
        except Exception as e:
            log("worker error: %r" % e)
            send_text(chat_id, "⚠️ bridge error: %r" % e)
        finally:
            work_q.task_done()


def handle_message(msg):
    global MODEL, EFFORT, RUN_TIMEOUT
    chat_id = msg["chat"]["id"]
    from_id = (msg.get("from") or {}).get("id")
    if from_id != OWNER:
        log("ignored non-owner %s in chat %s" % (from_id, chat_id))
        # Reply only in private chats, at most once per 10 min per chat.
        if msg["chat"].get("type") == "private":
            now = time.time()
            if now - reject_throttle.get(chat_id, 0) > 600:
                reject_throttle[chat_id] = now
                tg("sendMessage", {"chat_id": chat_id, "text": "Private bot."}, timeout=15)
        return

    text = msg.get("text")
    if not text:
        # Off the poller thread: downloads/STT can take minutes and /stop
        # must stay responsive meanwhile.
        threading.Thread(target=handle_media_safe, args=(msg, chat_id), daemon=True).start()
        return
    text = text.strip()
    cmd = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""
    arg = text.split(None, 1)[1].strip() if len(text.split(None, 1)) > 1 else ""

    if cmd in ("/start", "/help"):
        send_text(chat_id,
                  "🟢 Claude Code bridge on %s. Talk normally: every message is a full Claude Code turn "
                  "in %s. Files, photos and voice notes work. Other /commands (skills, /goal) go to Claude.\n\n"
                  % (BRIDGE_LABEL, WORKDIR) + HELP)
    elif cmd == "/ping":
        send_text(chat_id, "Pong, bridge responsive.")
    elif cmd == "/workspace":
        send_text(chat_id, WORKDIR)
    elif cmd == "/models":
        send_text(chat_id, "Current: %s\nAliases: %s\nFull names: %s\nGLM worker: /zai <prompt>"
                  % (MODEL or "account default", ", ".join(CLAUDE_MODELS), ", ".join(KNOWN_MODELS)))
    elif cmd == "/model":
        if not arg:
            send_text(chat_id, "Model: %s. Set with /model <name>, back to account default with /model default." % (MODEL or "account default"))
        elif work_q.unfinished_tasks:
            send_text(chat_id, "Wait until the current and queued work finishes before switching.")
        else:
            new = "" if arg.lower() in ("default", "account", "reset") else arg.strip()
            if new and new.lower().startswith("glm"):
                send_text(chat_id, "%s is the GLM worker: use /zai <prompt>." % new)
            elif new and new not in CLAUDE_MODELS and not ("-" in new and any(c.isdigit() for c in new)):
                send_text(chat_id, "Unknown model %r. See /models." % new[:40])
            else:
                with state_lock:
                    STATE["primary_model"] = new
                    save_state(STATE)
                MODEL = new
                send_text(chat_id, "Model: %s (applies from the next turn)." % (MODEL or "account default"))
    elif cmd == "/effort":
        if not arg:
            send_text(chat_id, "Effort: %s (low, medium, high, xhigh, max, default)." % (EFFORT or "account default"))
        elif arg.lower() not in EFFORT_LEVELS + ("default",):
            send_text(chat_id, "Choose one of: " + ", ".join(EFFORT_LEVELS + ("default",)))
        elif work_q.unfinished_tasks:
            send_text(chat_id, "Wait until the current and queued work finishes before changing effort.")
        else:
            EFFORT = "" if arg.lower() == "default" else arg.lower()
            with state_lock:
                STATE["effort"] = EFFORT
                save_state(STATE)
            send_text(chat_id, "Effort: %s (applies from the next turn)." % (EFFORT or "account default"))
    elif cmd == "/timeout":
        if not arg:
            send_text(chat_id, "Turn limit: %s." % ("none" if RUN_TIMEOUT <= 0 else "%d minutes" % (RUN_TIMEOUT // 60)))
        elif not arg.isdigit() or int(arg) > 1440:
            send_text(chat_id, "Give minutes from 0 to 1440 (0 = no limit).")
        else:
            RUN_TIMEOUT = int(arg) * 60
            with state_lock:
                STATE["turn_timeout"] = RUN_TIMEOUT
                save_state(STATE)
            send_text(chat_id, "Turn limit: %s." % ("none" if RUN_TIMEOUT <= 0 else "%d minutes" % int(arg)))
    elif cmd == "/retry":
        last = STATE.get("last_prompt", {}).get(str(chat_id))
        if not last:
            send_text(chat_id, "No previous prompt to retry.")
        else:
            send_text(chat_id, "Retrying: %s" % last[:120])
            queue_prompt(chat_id, last)
    elif cmd == "/resume":
        if not get_session(chat_id):
            send_text(chat_id, "No current session. Send a task or /attach first.")
        else:
            queue_prompt(chat_id, arg or "Continue the current task from where you stopped.")
    elif cmd == "/queue":
        send_text(chat_id, "Waiting tasks: %d" % work_q.qsize())
    elif cmd == "/clear":
        count = 0
        while True:
            try:
                work_q.get_nowait()
                work_q.task_done()
                count += 1
            except queue.Empty:
                break
        send_text(chat_id, "Discarded %d waiting tasks; the current turn keeps running." % count)
    elif cmd == "/zai":
        if not arg:
            send_text(chat_id, "Usage: /zai <prompt>. The GLM worker has no tools and cannot see this chat.")
        else:
            queue_prompt(chat_id, arg, provider="zai")
            send_text(chat_id, "GLM worker queued.")
    elif cmd == "/cost":
        lc = STATE.get("last_cost")
        send_text(chat_id, ("Last turn: $%.4f (model %s, effort %s, %s)" % (lc["usd"], lc["model"], lc["effort"], _ago(lc["ts"])))
                  if lc else "No completed turns recorded yet.")
    elif cmd == "/usage":
        send_text(chat_id, "Total turn cost since install: $%.4f" % STATE.get("cost_total", 0.0))
    elif cmd == "/session":
        sid = get_session(chat_id)
        send_text(chat_id, "Attached: %s" % (sid[:12] + "…" if sid else "none, next message starts fresh"))
    elif cmd == "/detach":
        set_session(chat_id, None)
        send_text(chat_id, "Detached. Next message starts a fresh conversation.")
    elif cmd == "/health":
        v = system_vitals()
        send_text(chat_id, "Load %s on %s CPUs · RAM %s%% of %sG · disk %s%% of %sG"
                  % (v.get("load"), v.get("cpus"), v.get("mem_pct"), v.get("mem_total_gb"), v.get("disk_pct"), v.get("disk_total_gb")))
    elif cmd == "/uptime":
        send_text(chat_id, "Bridge up %.1f hours." % ((time.time() - BOOT_TS) / 3600))
    elif cmd == "/config":
        send_text(chat_id, "model=%s\neffort=%s\nturn_limit=%s\nworkdir=%s\nglm=%s\ndashboard=%s"
                  % (MODEL or "account default", EFFORT or "account default",
                     "none" if RUN_TIMEOUT <= 0 else "%dm" % (RUN_TIMEOUT // 60), WORKDIR, GLM_BIN,
                     "on :%d" % DASH_PORT if DASH_PORT else "off"))
    elif cmd == "/sessions":
        cur = get_session(chat_id)
        rows = []
        for sid, age, kb, snip in list_sessions(10):
            mark = "▶ " if sid == cur else ""
            rows.append("%s`%s` · %s · %dKB\n   %s" % (mark, sid[:8], age, kb, snip))
        send_text(chat_id, "Recent sessions (newest first):\n\n" + "\n".join(rows) +
                  "\n\n/attach <first-8-chars> to continue one here.")
    elif cmd == "/attach":
        if not arg:
            send_text(chat_id, "Usage: /attach <session-id-prefix> (see /sessions)")
        else:
            sid, hits = resolve_session(arg)
            if sid:
                set_session(chat_id, sid)
                munged = WORKDIR.replace("/", "-")
                snip = session_snippet(os.path.join(PROJECTS_DIR, munged, sid + ".jsonl"))
                send_text(chat_id, "🔗 Attached to `%s`\n\"%s\"\nNext message continues that conversation." % (sid[:8], snip))
            elif hits:
                send_text(chat_id, "Ambiguous — matches: %s" % ", ".join(h[:8] for h in hits[:6]))
            else:
                send_text(chat_id, "No session matches `%s`. See /sessions." % arg[:20])
    elif cmd == "/last":
        cur = get_session(chat_id)
        pick = next((s for s in list_sessions(10) if s[0] != cur), None)
        if pick:
            set_session(chat_id, pick[0])
            send_text(chat_id, "🔗 Attached to `%s` (%s ago)\n\"%s\"" % (pick[0][:8], pick[1], pick[3]))
        else:
            send_text(chat_id, "No other session found.")
    elif cmd == "/file":
        path = arg
        if not path.startswith("/") or not os.path.isfile(path):
            send_text(chat_id, "Usage: /file /absolute/path — file not found: %s" % path[:100])
        elif os.path.getsize(path) > 49 << 20:
            send_text(chat_id, "File is %d MB — over Telegram's 50 MB bot limit." % (os.path.getsize(path) >> 20))
        else:
            r = multipart_post(API + "/sendDocument", {"chat_id": str(chat_id)}, "document", path)
            if not r.get("ok"):
                send_text(chat_id, "⚠️ send failed: %s" % str(r.get("description", r))[:200])
    elif cmd == "/log":
        try:
            with open(LOG_PATH) as f:
                tail = "".join(f.readlines()[-15:])
        except OSError as e:
            tail = repr(e)
        send_text(chat_id, "```\n%s\n```" % tail[-3500:].replace("`", "'"))
    elif cmd == "/reboot":
        send_text(chat_id, "♻️ Bridge restarting (systemd brings it back in ~5s).")
        log("reboot requested via telegram")
        # Exit happens in the main loop AFTER this update's offset is saved,
        # otherwise Telegram redelivers /reboot forever.
        reboot_requested.set()
    elif cmd in ("/new", "/restart"):
        set_session(chat_id, None)
        send_text(chat_id, "🆕 Fresh session. Next message starts a new conversation.")
    elif cmd == "/go":
        with proc_lock:
            busy = current_proc is not None and current_proc.poll() is None
        if not arg:
            send_text(chat_id, "Usage: /go <message> — reaches the running turn at its next step.")
        elif busy:
            with open(inbox_path(chat_id), "a") as f:
                f.write(arg + "\n\n")
            send_text(chat_id, "📨 Delivered to the running turn. Claude sees it at its next step.")
        else:
            queue_prompt(chat_id, arg)
    elif cmd == "/stop":
        with proc_lock:
            p = current_proc
        drained = 0
        try:
            while True:
                work_q.get_nowait()
                work_q.task_done()
                drained += 1
        except queue.Empty:
            pass
        if p and p.poll() is None:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except OSError:
                pass
            send_text(chat_id, "🛑 Stopped the running turn (+%d queued dropped)." % drained)
        else:
            send_text(chat_id, "Nothing running (%d queued dropped)." % drained)
    elif cmd == "/status":
        with proc_lock:
            busy = current_proc is not None and current_proc.poll() is None
        sid = get_session(chat_id)
        send_text(chat_id,
                  "Bridge: 🟢 up\nRunning turn: %s\nQueued: %d\nSession: %s\nWorkdir: %s\nModel: %s · effort: %s"
                  % ("yes" if busy else "no", work_q.qsize(), sid or "(none — next msg starts one)", WORKDIR,
                     MODEL or "account default", EFFORT or "account default"))
    else:
        queue_prompt(chat_id, text)


_treasury_cache = {"ts": 0, "data": None}
# Optional Solana treasury panel — set TREASURY_WALLET to enable. Fully generic.
TREASURY_WALLET = os.environ.get("TREASURY_WALLET", "")
TREASURY_SYMBOL = os.environ.get("TREASURY_SYMBOL", "TOKEN")
TREASURY_MINT_FILE = os.environ.get("TREASURY_MINT_FILE", "")
SOLANA_RPC = os.environ.get("SOLANA_RPC", "https://api.mainnet-beta.solana.com")


def get_treasury():
    """Solana wallet/token status for the dashboard (cached 60s).

    Returns None unless TREASURY_WALLET is set, so the panel is opt-in and the
    bridge stays generic.
    """
    if not TREASURY_WALLET:
        return None
    if time.time() - _treasury_cache["ts"] < 60 and _treasury_cache["data"]:
        return _treasury_cache["data"]
    data = {"wallet": TREASURY_WALLET, "sol": None, "mint": None,
            "symbol": TREASURY_SYMBOL, "launched": False}
    if TREASURY_MINT_FILE:
        try:
            with open(TREASURY_MINT_FILE) as f:
                m = f.read().strip().split()[-1]
                if m:
                    data["mint"], data["launched"] = m, True
        except (OSError, IndexError):
            pass
    if data["wallet"]:
        try:
            req = urllib.request.Request(
                SOLANA_RPC,
                data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getBalance",
                                 "params": [data["wallet"]]}).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lam = (json.load(r).get("result") or {}).get("value", 0)
                data["sol"] = round(lam / 1e9, 4)
        except Exception:
            pass
    _treasury_cache["ts"], _treasury_cache["data"] = time.time(), data
    return data


def system_vitals():
    v = {"load": None, "mem_pct": None, "disk_pct": None, "cpus": os.cpu_count()}
    try:
        with open("/proc/loadavg") as f:
            v["load"] = float(f.read().split()[0])
    except (OSError, ValueError):
        pass
    try:
        mem = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    mem[parts[0].rstrip(":")] = int(parts[1])
        total, avail = mem.get("MemTotal", 1), mem.get("MemAvailable", 0)
        v["mem_pct"] = round(100 * (total - avail) / total, 1)
        v["mem_total_gb"] = round(total / 1048576, 1)
    except (OSError, ValueError, ZeroDivisionError):
        pass
    try:
        du = shutil.disk_usage("/")
        v["disk_pct"] = round(100 * du.used / du.total, 1)
        v["disk_total_gb"] = round(du.total / 1e9)
    except OSError:
        pass
    return v


def dash_state():
    """Read-only snapshot for the web dashboard."""
    with proc_lock:
        busy = current_proc is not None and current_proc.poll() is None
    with state_lock:
        cur = STATE["sessions"].get(str(OWNER))
    loglines = []
    try:
        size = os.path.getsize(LOG_PATH)
        with open(LOG_PATH, "rb") as f:
            f.seek(max(0, size - 65536))
            loglines = f.read().decode(errors="replace").splitlines()[-300:]
    except OSError:
        pass
    runs = []
    for ln in loglines:
        if " run done " not in ln:
            continue
        try:
            ts = ln.split()[0]
            mins = float(ln.split("min", 1)[0].rsplit(" ", 1)[-1])
            rc = ln.split("rc=", 1)[1].split()[0]
            runs.append({"ts": ts, "mins": mins, "ok": rc == "0"})
        except (IndexError, ValueError):
            continue
    today = time.strftime("%Y-%m-%d", time.gmtime())
    uploads = []
    try:
        paths = sorted(glob.glob(os.path.join(UPLOAD_DIR, "*")), key=os.path.getmtime, reverse=True)[:10]
        for p in paths:
            uploads.append({"name": os.path.basename(p), "kb": os.path.getsize(p) // 1024,
                            "ts": time.strftime("%H:%M", time.gmtime(os.path.getmtime(p)))})
    except OSError:
        pass
    return {
        "label": BRIDGE_LABEL,
        "busy": busy,
        "queue": work_q.qsize(),
        "uptime_s": int(time.time() - BOOT_TS),
        "runs_today": sum(1 for r in runs if r["ts"].startswith(today)),
        "runs": runs[-15:][::-1],
        "sessions": [{"id": s, "age": a, "kb": kb, "snippet": sn, "current": s == cur}
                     for s, a, kb, sn in list_sessions(10)],
        "uploads": uploads,
        "treasury": get_treasury(),
        "system": system_vitals(),
        "log": "\n".join(loglines[-30:]).replace(TOKEN, "‹bot-token›"),
    }


class DashHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _authed(self):
        q = parse_qs(urlparse(self.path).query)
        key = (q.get("key") or [""])[0] or self.headers.get("X-Dash-Key", "")
        return bool(DASH_KEY) and hmac.compare_digest(key, DASH_KEY)

    def do_GET(self):
        try:
            path = urlparse(self.path).path
            if not self._authed():
                self.send_response(401)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"unauthorized")
                return
            if path == "/api/state":
                body, ctype = json.dumps(dash_state()).encode(), "application/json"
            elif path == "/":
                try:
                    with open(os.path.join(BASE, "dashboard.html"), "rb") as f:
                        body = f.read()
                except OSError:
                    body = b"dashboard.html missing next to bridge.py"
                ctype = "text/html; charset=utf-8"
            else:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            log("dash error: %r" % e)


def start_dashboard():
    if not DASH_PORT:
        return
    if not DASH_KEY:
        log("DASH_PORT set but DASH_KEY empty — dashboard stays off")
        return
    srv = ThreadingHTTPServer(("0.0.0.0", DASH_PORT), DashHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log("dashboard listening on :%d" % DASH_PORT)


def register_commands():
    for scope in ({"type": "default"}, {"type": "chat", "chat_id": OWNER}):
        r = tg("setMyCommands", {"commands": MENU, "scope": scope}, timeout=20)
        if not r.get("ok"):
            log("setMyCommands failed: %s" % str(r)[:200])


def main():
    lock_f = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("another poller holds the lock — exiting")
        sys.exit(1)

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=group_flusher, daemon=True).start()
    start_dashboard()
    register_commands()
    log("bridge up, owner=%s offset=%s" % (OWNER, STATE.get("offset")))

    now = time.time()
    if now - STATE.get("last_announce", 0) > 600:
        r = tg("sendMessage", {"chat_id": OWNER, "text": "🟢 bridge online — send anything."}, timeout=15)
        if r.get("ok"):
            with state_lock:
                STATE["last_announce"] = now
                save_state(STATE)

    while True:
        r = tg("getUpdates", {
            "offset": STATE["offset"],
            "timeout": 50,
            "allowed_updates": ["message"],
        }, timeout=70)
        if not r.get("ok"):
            if r.get("error_code") == 409:
                log("409 conflict: another consumer on this token — backing off 30s")
                time.sleep(30)
            else:
                time.sleep(5)
            continue
        for upd in r.get("result", []):
            msg = upd.get("message")
            if msg:
                try:
                    handle_message(msg)
                except Exception as e:
                    log("handle error: %r" % e)
            # Advance offset only after the handle attempt so a crash
            # mid-handle replays the update instead of dropping it.
            with state_lock:
                STATE["offset"] = upd["update_id"] + 1
                save_state(STATE)
            if reboot_requested.is_set():
                log("rebooting now (offset saved)")
                os._exit(42)


if __name__ == "__main__":
    main()
