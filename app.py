#!/usr/bin/env python3
"""
Shadow Music API — the public song-resolver API behind @ShadowMusicAPIBot.

Deploy (Render, free web service — see render.yaml at the repo root):

    New Web Service → repo XLEWIN/RobinChan → root dir: shadow_api
    or: Dashboard → Blueprints → deploy the render.yaml blueprint.

Endpoints (Authorization: Bearer smp_... — free token from @ShadowMusicAPIBot)

    GET  /v1/resolve?q=<query>          search + metadata + stream URL
    GET  /v1/resolve?url=<youtube url>  metadata + stream URL for a known video
    POST /v1/resolve                    body {"query": "..."} or {"url": "..."}
    GET  /v1/stream/<vidid>             just the stream URL

Public (no token):

    GET  /v1/healthz                    {"ok": true, ...}
    GET  /ping                          keep-alive target (used by the self-ping)
    GET  /                              service info + endpoint list

Auth model: stateless HMAC tokens (smp_<b64 uid>.<b64 hmac>) signed with
SHADOW_API_SECRET. No database — any restart is fine, tokens never expire,
usage is unlimited. Invalid token → 401.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import os
import random
import re
import subprocess
import time
from typing import Any, Optional

import aiohttp
from aiohttp import web

try:
    from dotenv import load_dotenv
    # Local dev convenience (./.env); on Render, vars come from the dashboard.
    load_dotenv()
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [shadow-api] %(message)s")
log = logging.getLogger("shadow-api")

# ─── config ─────────────────────────────────────────────────────────────────

SECRET = os.environ.get("SHADOW_API_SECRET", "").strip()
YUKI_API_URL = os.environ.get("YUKI_API_URL", "https://music.yukiapi.site").rstrip("/")
YUKI_API_KEY = os.environ.get("YUKI_API_KEY", "").strip()
NEXGEN_URL = os.environ.get("API_URL", "https://api.nexgenbots.xyz").rstrip("/")
NEXGEN_KEY = os.environ.get("API_KEY", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
SELF_URL = RENDER_EXTERNAL_URL or f"http://127.0.0.1:{int(os.environ.get('PORT', '10000'))}"
BIND = os.environ.get("SHADOW_API_BIND", "0.0.0.0")
PORT = int(os.environ.get("PORT", "10000"))

# Public innertube keys (embedded in YouTube's own clients; used by yt-dlp too).
_INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"      # WEB (search)
_INNERTUBE_ANDROID_KEY = "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w"  # ANDROID (player)
_YT_VIDEO_RE = re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/|/embed/|/v/)([a-zA-Z0-9_-]{11})")
_BARE_VIDID_RE = re.compile(r"[A-Za-z0-9_-]{11}")

if not SECRET:
    log.warning("SHADOW_API_SECRET is empty — every token will 401. Set it before going live.")

# ─── tokens ──────────────────────────────────────────────────────────────────

def verify_token(token: str) -> bool:
    """Stateless check: smp_<b64url uid>.<b64url hmac16>, HMAC over 'shadow-music|<uid>'."""
    if not SECRET:
        return False
    if not token or not token.startswith("smp_"):
        return False
    body = token[4:]
    parts = body.split(".")
    if len(parts) != 2:
        return False
    uid_b64, sig_b64 = parts
    if not uid_b64 or not sig_b64:
        return False
    try:
        uid = base64.urlsafe_b64decode(uid_b64 + "=" * (-len(uid_b64) % 4)).decode("utf-8", errors="strict")
    except Exception:
        return False
    if not uid or len(uid) > 32:
        return False
    expected = hmac.new(SECRET.encode(), f"shadow-music|{uid}".encode(), hashlib.sha256).digest()[:16]
    got = base64.urlsafe_b64decode(sig_b64 + "=" * (-len(sig_b64) % 4))
    if len(got) != 16:
        return False
    return hmac.compare_digest(expected, got)


# ─── HTTP helpers ────────────────────────────────────────────────────────────

def _client(timeout: float = 8.0) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))


async def _http_json(session: aiohttp.ClientSession, url: str, **kwargs) -> Optional[Any]:
    try:
        async with session.post(url, **kwargs) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception:
        return None


def _sec_to_duration(seconds: Any) -> str:
    try:
        s = int(float(seconds))
    except Exception:
        return "0:00"
    m, s = divmod(max(0, s), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _thumb(vidid: str) -> str:
    return f"https://i.ytimg.com/vi/{vidid}/hqdefault.jpg"


def _clean_author(name: str) -> str:
    name = re.sub(r"\s*-?\s*Topic$", "", name or "").strip()
    name = re.sub(r"VEVO$", "", name).strip()
    return name


# ─── search metadata: innertube WEB ──────────────────────────────────────────

def _find_dicts(node: Any, key: str):
    if isinstance(node, dict):
        if key in node:
            yield node[key]
        for v in node.values():
            yield from _find_dicts(v, key)
    elif isinstance(node, list):
        for v in node:
            yield from _find_dicts(v, key)


def _text(node: Any) -> str:
    if isinstance(node, dict):
        if isinstance(node.get("simpleText"), str):
            return node["simpleText"]
        runs = node.get("runs")
        if isinstance(runs, list) and runs and isinstance(runs[0], dict):
            return str(runs[0].get("text") or "")
    return ""


async def innertube_search(query: str, limit: int = 1) -> Optional[dict]:
    payload = {
        "context": {"client": {"clientName": "WEB", "clientVersion": "2.20241001.00.00", "hl": "en"}},
        "query": query,
        "params": "EgIQAQ==",  # videos only
    }
    async with _client(6.0) as sess:
        data = await _http_json(
            sess,
            f"https://www.youtubei.googleapis.com/youtubei/v1/search?key={_INNERTUBE_KEY}",
            json=payload,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    if not isinstance(data, dict):
        return None
    for vr in _find_dicts(data, "videoRenderer"):
        if not isinstance(vr, dict):
            continue
        vidid = vr.get("videoId")
        if not vidid:
            continue
        title = _text(vr.get("title")).strip()
        length = _text(vr.get("lengthText")).strip() or "0:00"
        thumbs = (vr.get("thumbnail") or {}).get("thumbnails") or []
        thumb = thumbs[-1].get("url", "") if thumbs and isinstance(thumbs[-1], dict) else ""
        author = _clean_author(_text(vr.get("ownerText")) or _text(vr.get("shortBylineText")))
        return {
            "vidid": vidid,
            "title": title,
            "duration": length,
            "artist": author,
            "thumbnail_url": thumb or _thumb(vidid),
        }
    return None


async def ytdlp_search(query: str) -> Optional[dict]:
    """Fallback: yt-dlp flat search (works even when innertube is blocked)."""
    try:
        out = await asyncio.create_subprocess_exec(
            "yt-dlp", "--dump-json", "--no-download", "--no-playlist", "--no-warnings",
            f"ytsearch1:{query}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        raw = await asyncio.wait_for(out.stdout.read(), timeout=12.0)
        info = None
        for line in raw.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if line.startswith("{"):
                info = __import__("json").loads(line)
                break
        if not info:
            return None
        return {
            "vidid": info.get("id") or "",
            "title": str(info.get("title") or ""),
            "duration": _sec_to_duration(info.get("duration")),
            "artist": str((info.get("channel") or info.get("uploader") or "")).strip(),
            "thumbnail_url": _thumb(info.get("id") or ""),
        }
    except Exception:
        return None


# ─── stream URL resolvers ──────────────────────────────────────────────────────

async def yuki_stream(vidid: str) -> Optional[str]:
    if not YUKI_API_KEY:
        return None
    url = f"{YUKI_API_URL}/stream/{vidid}?key={YUKI_API_KEY}&type=audio&quality=128"
    try:
        async with _client(2.5) as sess:
            async with sess.head(url) as resp:
                ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                if resp.status == 200 and ctype and not ctype.startswith("text/"):
                    return url
                log.info("yuki rejected %s: HTTP %s content-type=%s", vidid, resp.status, ctype or "none")
                return None
    except Exception as e:
        log.info("yuki failed for %s: %s", vidid, e)
        return None


async def nexgen_stream(vidid: str) -> Optional[str]:
    if not NEXGEN_KEY:
        return None
    url = f"{NEXGEN_URL}/song/{vidid}?api={NEXGEN_KEY}"
    try:
        async with _client(6.0) as sess:
            async with sess.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
        if not isinstance(data, dict):
            return None
        if str(data.get("status", "")).lower() == "done":
            link = data.get("link")
            if link:
                return str(link)
        return None
    except Exception as e:
        log.info("nexgen failed for %s: %s", vidid, e)
        return None


async def innertube_android_stream(vidid: str) -> Optional[str]:
    """Direct progressive/audio URL from the ANDROID player endpoint — no key, no JS."""
    payload = {
        "context": {"client": {"clientName": "ANDROID", "clientVersion": "19.09.37", "androidSdkVersion": 30, "hl": "en"}},
        "videoId": vidid,
    }
    async with _client(5.0) as sess:
        data = await _http_json(
            sess,
            f"https://www.youtubei.googleapis.com/youtubei/v1/player?key={_INNERTUBE_ANDROID_KEY}",
            json=payload,
            headers={"User-Agent": "com.google.android.youtube/19.09.37 (Linux; Android 11) gzip"},
        )
    if not isinstance(data, dict):
        return None
    streaming = data.get("streamingData") or {}
    adaptive = streaming.get("adaptiveFormats") or []
    # Prefer audio-only mp4 128kbps (itag 140), else any audio, else muxed itag 18.
    for f in adaptive:
        if isinstance(f, dict) and f.get("itag") == 140 and f.get("url"):
            return str(f["url"])
    for f in adaptive:
        if isinstance(f, dict) and "audio" in str(f.get("mimeType", "")) and f.get("url"):
            return str(f["url"])
    for f in streaming.get("formats") or []:
        if isinstance(f, dict) and f.get("url"):
            return str(f["url"])
    return None


async def ytdlp_stream(vidid: str) -> Optional[str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "-g", "--no-playlist", "--no-warnings",
            "-f", "bestaudio[ext=m4a]/bestaudio/best",
            f"https://www.youtube.com/watch?v={vidid}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        raw = await asyncio.wait_for(proc.stdout.read(), timeout=25.0)
        urls = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip().startswith("http")]
        return urls[0] if urls else None
    except Exception as e:
        log.info("yt-dlp -g failed for %s: %s", vidid, e)
        return None


async def resolve_stream(vidid: str) -> tuple[Optional[str], str]:
    """Returns (stream_url, source) trying every source in order."""
    for label, fn in (("yuki", yuki_stream), ("nexgen", nexgen_stream), ("innertube", innertube_android_stream), ("yt-dlp", ytdlp_stream)):
        url = await fn(vidid)
        if url:
            return url, label
    return None, ""


# ─── metadata for a single video (URL path) ──────────────────────────────────

async def innertube_player_metadata(vidid: str) -> Optional[dict]:
    payload = {
        "context": {"client": {"clientName": "ANDROID", "clientVersion": "19.09.37", "androidSdkVersion": 30, "hl": "en"}},
        "videoId": vidid,
    }
    async with _client(5.0) as sess:
        data = await _http_json(
            sess,
            f"https://www.youtubei.googleapis.com/youtubei/v1/player?key={_INNERTUBE_ANDROID_KEY}",
            json=payload,
            headers={"User-Agent": "com.google.android.youtube/19.09.37 (Linux; Android 11) gzip"},
        )
    if not isinstance(data, dict):
        return None
    details = data.get("videoDetails") or {}
    thumbs = (details.get("thumbnail") or {}).get("thumbnails") or []
    thumb = thumbs[-1].get("url", "") if thumbs and isinstance(thumbs[-1], dict) else _thumb(vidid)
    return {
        "vidid": vidid,
        "title": str(details.get("title") or ""),
        "duration": _sec_to_duration(details.get("lengthSeconds")),
        "artist": _clean_author(str(details.get("author") or "")),
        "thumbnail_url": thumb,
    }


# ─── HTTP app ──────────────────────────────────────────────────────────────────

@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, Accept"
    return resp


_PUBLIC_PATHS = {"/", "/ping", "/v1/healthz"}


@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in _PUBLIC_PATHS or request.method == "OPTIONS":
        return await handler(request)
    auth = request.headers.get("Authorization", "")
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    if not verify_token(token):
        return _err(401, "UNAUTHORIZED", "Invalid or missing token. Get a free one from @ShadowMusicAPIBot on Telegram.")
    return await handler(request)


def _ok(data: dict) -> web.Response:
    return web.json_response({"ok": True, **data})


def _err(status: int, code: str, error: str) -> web.Response:
    return web.json_response({"ok": False, "error": error, "code": code}, status=status)


async def root(request: web.Request) -> web.Response:
    return _ok({
        "service": "Shadow Music API",
        "description": "Free, unlimited YouTube audio search + direct stream URLs. Token from @ShadowMusicAPIBot.",
        "endpoints": ["/v1/resolve", "/v1/stream/{vidid}", "/v1/healthz"],
        "docs": "https://github.com/XLEWIN/RobinChan/blob/phone-serverless/docs/SHADOW_API.md",
        "get_token": "https://t.me/ShadowMusicAPIBot",
    })


async def ping(request: web.Request) -> web.Response:
    return _ok({"ts": int(time.time())})


async def healthz(request: web.Request) -> web.Response:
    return _ok({"service": "shadow-music-api", "yuki_configured": bool(YUKI_API_KEY), "nexgen_configured": bool(NEXGEN_KEY)})


def _vidid_from_url(url: str) -> Optional[str]:
    m = _YT_VIDEO_RE.search(url or "")
    if m:
        return m.group(1)
    u = (url or "").strip()
    if _BARE_VIDID_RE.fullmatch(u):
        return u
    return None


async def resolve(request: web.Request) -> web.Response:
    if request.method == "GET":
        params = request.query
        query = params.get("q") or params.get("query")
        url = params.get("url")
    else:
        try:
            body = await request.json()
        except Exception:
            return _err(400, "BAD_JSON", "Invalid JSON body.")
        query = (body or {}).get("query")
        url = (body or {}).get("url")

    search_meta: Optional[dict] = None
    if query:
        url_match = _vidid_from_url(str(query))
        if url_match:
            vidid = url_match
        else:
            search_meta = await innertube_search(str(query))
            if not search_meta:
                search_meta = await ytdlp_search(str(query))
            vidid = (search_meta or {}).get("vidid")
            if not vidid:
                return _err(404, "NOT_FOUND", "No results found for that query.")
    elif url:
        vidid = _vidid_from_url(str(url))
        if not vidid:
            return _err(400, "BAD_URL", "Not a YouTube URL or video id.")
    else:
        return _err(400, "NO_QUERY", "Provide 'query'/'q' or 'url'.")

    link = f"https://www.youtube.com/watch?v={vidid}"
    stream_url, source = await resolve_stream(vidid)
    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL right now — try again in a moment.")

    if search_meta:
        # Came from a text search — innertube already gave us title/duration/etc.
        meta = search_meta
    else:
        # URL/vidid path — fetch title/duration/author from the ANDROID player endpoint.
        meta = await innertube_player_metadata(vidid) or {
            "title": "",
            "duration": "0:00",
            "artist": "",
        }
        meta["thumbnail_url"] = meta.get("thumbnail_url") or _thumb(vidid)

    return _ok({
        "vidid": vidid,
        "title": meta.get("title") or "",
        "duration": meta.get("duration") or "0:00",
        "artist": meta.get("artist") or "",
        "thumbnail_url": meta.get("thumbnail_url") or _thumb(vidid),
        "stream_url": stream_url,
        "source": source,
        "video_url": link,
    })


async def stream(request: web.Request) -> web.Response:
    vidid = request.match_info["vidid"]
    if not _BARE_VIDID_RE.fullmatch(vidid):
        return _err(400, "BAD_VIDID", "Invalid video id.")
    stream_url, source = await resolve_stream(vidid)
    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL right now — try again in a moment.")
    return _ok({"vidid": vidid, "stream_url": stream_url, "source": source})


async def options(request: web.Request) -> web.Response:
    return web.Response()


# ─── keep-alive: ping our own PUBLIC url every ~10s (user's request) ───────────

async def _keep_alive() -> None:
    await asyncio.sleep(5)
    target = f"{SELF_URL}/ping"
    logged = False
    while True:
        delay = random.uniform(8.0, 12.0)
        await asyncio.sleep(delay)
        try:
            async with _client(5.0) as sess:
                async with sess.get(target) as resp:
                    if not logged and resp.status == 200:
                        log.info("keep-alive self-ping OK → %s (every 8–12s)", target)
                        logged = True
        except Exception as e:
            log.debug("keep-alive ping failed: %s", e)


async def _on_startup(app: web.Application) -> None:
    asyncio.create_task(_keep_alive())


def make_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware, auth_middleware])
    app.router.add_get("/", root)
    app.router.add_get("/ping", ping)
    app.router.add_get("/v1/healthz", healthz)
    app.router.add_route("*", "/v1/resolve", resolve)
    app.router.add_get("/v1/stream/{vidid}", stream)
    app.router.add_route("OPTIONS", "/{tail:.*}", options)
    app.on_startup.append(_on_startup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=BIND, port=PORT, access_log=None)
