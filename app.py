#!/usr/bin/env python3
"""
Shadow Music API — fast public YouTube audio resolver.

Endpoints (Authorization: Bearer smp_...):
  GET/POST /v1/resolve?q=<query>|url=<youtube>
  GET      /v1/stream/<vidid>
Public:
  GET /, /ping, /v1/healthz
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
from typing import Any, Optional

import aiohttp
from aiohttp import web

try:
    from dotenv import load_dotenv
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

_INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
_INNERTUBE_ANDROID_KEY = "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w"
_YT_VIDEO_RE = re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/|/embed/|/v/)([a-zA-Z0-9_-]{11})")
_BARE_VIDID_RE = re.compile(r"[A-Za-z0-9_-]{11}")

_STREAM_TTL = 8 * 60
_META_TTL = 30 * 60
_SEARCH_TTL = 5 * 60
_CACHE_CAP = 600

if not SECRET:
    log.warning("SHADOW_API_SECRET is empty — every token will 401. Set it before going live.")

# ─── tiny in-memory cache ───────────────────────────────────────────────────

class TTLCache:
    def __init__(self, ttl: float, cap: int = _CACHE_CAP):
        self.ttl = ttl
        self.cap = cap
        self._data: dict[str, tuple[float, Any]] = {}

    def get(self, key: str):
        item = self._data.get(key)
        if not item:
            return None
        ts, val = item
        if time.monotonic() - ts > self.ttl:
            self._data.pop(key, None)
            return None
        return val

    def set(self, key: str, val: Any):
        if len(self._data) >= self.cap:
            # Drop oldest-ish entry (dict preserves insertion order in Py3.7+).
            try:
                self._data.pop(next(iter(self._data)))
            except Exception:
                self._data.clear()
        self._data[key] = (time.monotonic(), val)


_stream_cache = TTLCache(_STREAM_TTL)
_meta_cache = TTLCache(_META_TTL)
_search_cache = TTLCache(_SEARCH_TTL, cap=300)

# ─── tokens ─────────────────────────────────────────────────────────────────

def verify_token(token: str) -> bool:
    if not SECRET or not token or not token.startswith("smp_"):
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
    try:
        got = base64.urlsafe_b64decode(sig_b64 + "=" * (-len(sig_b64) % 4))
    except Exception:
        return False
    if len(got) != 16:
        return False
    return hmac.compare_digest(expected, got)


# ─── helpers ────────────────────────────────────────────────────────────────

def _session(app: web.Application) -> aiohttp.ClientSession:
    return app["http_session"]


async def _post_json(session: aiohttp.ClientSession, url: str, *, json_body: dict, headers: dict, timeout: float) -> Optional[Any]:
    try:
        async with session.post(url, json=json_body, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
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
    s = max(0, s)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _thumb(vidid: str) -> str:
    return f"https://i.ytimg.com/vi/{vidid}/hqdefault.jpg"


def _clean_author(name: str) -> str:
    name = re.sub(r"\s*-?\s*Topic$", "", name or "").strip()
    return re.sub(r"VEVO$", "", name).strip()


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


def _pick_audio(streaming: dict) -> Optional[str]:
    adaptive = streaming.get("adaptiveFormats") or []
    muxed = streaming.get("formats") or []
    for f in adaptive:
        if isinstance(f, dict) and f.get("itag") == 140 and f.get("url"):
            return str(f["url"])
    for f in adaptive:
        if isinstance(f, dict) and "audio" in str(f.get("mimeType", "")) and f.get("url"):
            return str(f["url"])
    for f in muxed:
        if isinstance(f, dict) and f.get("url"):
            return str(f["url"])
    return None


# ─── innertube ──────────────────────────────────────────────────────────────

async def innertube_search(session: aiohttp.ClientSession, query: str, limit: int = 1) -> list[dict]:
    cache_key = f"{limit}:{query.strip().lower()}"
    cached = _search_cache.get(cache_key)
    if cached is not None:
        return cached

    payload = {
        "context": {"client": {"clientName": "WEB", "clientVersion": "2.20241001.00.00", "hl": "en"}},
        "query": query,
        "params": "EgIQAQ==",
    }
    data = await _post_json(
        session,
        f"https://youtubei.googleapis.com/youtubei/v1/search?key={_INNERTUBE_KEY}",
        json_body=payload,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=6.0,
    )
    out: list[dict] = []
    if isinstance(data, dict):
        for vr in _find_dicts(data, "videoRenderer"):
            if not isinstance(vr, dict):
                continue
            vidid = vr.get("videoId")
            if not vidid:
                continue
            thumbs = (vr.get("thumbnail") or {}).get("thumbnails") or []
            thumb = thumbs[-1].get("url", "") if thumbs and isinstance(thumbs[-1], dict) else ""
            author = _clean_author(_text(vr.get("ownerText")) or _text(vr.get("shortBylineText")))
            out.append({
                "vidid": vidid,
                "title": _text(vr.get("title")).strip() or "YouTube Video",
                "duration": _text(vr.get("lengthText")).strip() or "0:00",
                "artist": author,
                "thumbnail_url": thumb or _thumb(vidid),
            })
            if len(out) >= limit:
                break
    if out:
        _search_cache.set(cache_key, out)
    return out


async def innertube_player(session: aiohttp.ClientSession, vidid: str) -> dict[str, Any]:
    cached = _meta_cache.get(vidid)
    # We cache meta; audio URL is short-lived/IP-bound but still worth a short cache.
    cached_stream = _stream_cache.get(vidid)
    if cached and cached_stream:
        return {"meta": cached, "audio_url": cached_stream["url"], "source": cached_stream["source"]}

    payload = {
        "context": {"client": {"clientName": "ANDROID", "clientVersion": "19.09.37", "androidSdkVersion": 30, "hl": "en"}},
        "videoId": vidid,
    }
    data = await _post_json(
        session,
        f"https://youtubei.googleapis.com/youtubei/v1/player?key={_INNERTUBE_ANDROID_KEY}",
        json_body=payload,
        headers={"User-Agent": "com.google.android.youtube/19.09.37 (Linux; Android 11) gzip"},
        timeout=5.0,
    )
    if not isinstance(data, dict):
        return {"meta": cached or _fallback_meta(vidid), "audio_url": None, "source": None}

    details = data.get("videoDetails") or {}
    thumbs = (details.get("thumbnail") or {}).get("thumbnails") or []
    thumb = thumbs[-1].get("url", "") if thumbs and isinstance(thumbs[-1], dict) else _thumb(vidid)
    meta = {
        "vidid": vidid,
        "title": str(details.get("title") or "YouTube Video"),
        "duration": _sec_to_duration(details.get("lengthSeconds")),
        "artist": _clean_author(str(details.get("author") or "")),
        "thumbnail_url": thumb or _thumb(vidid),
    }
    _meta_cache.set(vidid, meta)
    audio_url = _pick_audio(data.get("streamingData") or {})
    if audio_url:
        _stream_cache.set(vidid, {"url": audio_url, "source": "innertube"})
    return {"meta": meta, "audio_url": audio_url, "source": "innertube" if audio_url else None}


def _fallback_meta(vidid: str) -> dict:
    return {"vidid": vidid, "title": "YouTube Video", "duration": "0:00", "artist": "", "thumbnail_url": _thumb(vidid)}


# ─── stream sources ─────────────────────────────────────────────────────────

async def yuki_stream(session: aiohttp.ClientSession, vidid: str) -> Optional[str]:
    if not YUKI_API_KEY:
        return None
    url = f"{YUKI_API_URL}/stream/{vidid}?key={YUKI_API_KEY}&type=audio&quality=128"
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if resp.status == 200 and ctype and not ctype.startswith("text/"):
                return url
    except Exception:
        return None
    return None


async def nexgen_stream(session: aiohttp.ClientSession, vidid: str) -> Optional[str]:
    if not NEXGEN_KEY:
        return None
    url = f"{NEXGEN_URL}/song/{vidid}?api={NEXGEN_KEY}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6.0)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
        if isinstance(data, dict) and str(data.get("status", "")).lower() == "done" and data.get("link"):
            return str(data["link"])
    except Exception:
        return None
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
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=25.0)
        for line in raw.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if line.startswith("http"):
                return line
    except Exception as e:
        log.info("yt-dlp -g failed for %s: %s", vidid, e)
    return None


async def resolve_stream(session: aiohttp.ClientSession, vidid: str, *, player: Optional[dict] = None) -> tuple[Optional[str], str, dict]:
    cached = _stream_cache.get(vidid)
    meta = (player or {}).get("meta") or _meta_cache.get(vidid) or _fallback_meta(vidid)
    if cached:
        return cached["url"], cached["source"], meta

    # Fast path: if we already have a player result with audio, use it.
    if player and player.get("audio_url"):
        _stream_cache.set(vidid, {"url": player["audio_url"], "source": player.get("source") or "innertube"})
        return player["audio_url"], player.get("source") or "innertube", player.get("meta") or meta

    # Race cheap network sources in parallel; do expensive yt-dlp only if all fail.
    tasks = {
        asyncio.create_task(yuki_stream(session, vidid)): "yuki",
        asyncio.create_task(nexgen_stream(session, vidid)): "nexgen",
    }
    # If player wasn't preloaded, fetch it in the same race.
    player_task = None
    if player is None:
        player_task = asyncio.create_task(innertube_player(session, vidid))
        tasks[player_task] = "innertube"

    try:
        pending = set(tasks.keys())
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                label = tasks.get(t, "")
                try:
                    res = t.result()
                except Exception:
                    res = None
                if label == "innertube":
                    player = res if isinstance(res, dict) else player
                    meta = (player or {}).get("meta") or meta
                    url = (player or {}).get("audio_url")
                    if url:
                        for p in pending:
                            p.cancel()
                        _stream_cache.set(vidid, {"url": url, "source": "innertube"})
                        return url, "innertube", meta
                else:
                    url = res if isinstance(res, str) and res else None
                    if url:
                        for p in pending:
                            p.cancel()
                        _stream_cache.set(vidid, {"url": url, "source": label})
                        return url, label, meta
        # All cheap sources failed; try yt-dlp last.
        url = await ytdlp_stream(vidid)
        if url:
            _stream_cache.set(vidid, {"url": url, "source": "yt-dlp"})
            return url, "yt-dlp", meta
        return None, "", meta
    finally:
        for t in tasks.keys():
            if not t.done():
                t.cancel()


# ─── HTTP app ───────────────────────────────────────────────────────────────

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
    return _ok({
        "service": "shadow-music-api",
        "yuki_configured": bool(YUKI_API_KEY),
        "nexgen_configured": bool(NEXGEN_KEY),
        "cache": {"streams": len(_stream_cache._data), "meta": len(_meta_cache._data), "search": len(_search_cache._data)},
    })


def _vidid_from_url(url: str) -> Optional[str]:
    m = _YT_VIDEO_RE.search(url or "")
    if m:
        return m.group(1)
    u = (url or "").strip()
    return u if _BARE_VIDID_RE.fullmatch(u) else None


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
        body = body or {}
        query = body.get("query")
        url = body.get("url")

    session = _session(request.app)
    search_meta: Optional[dict] = None

    if query:
        url_match = _vidid_from_url(str(query))
        if url_match:
            vidid = url_match
        else:
            results = await innertube_search(session, str(query), limit=1)
            search_meta = results[0] if results else None
            vidid = (search_meta or {}).get("vidid")
            if not vidid:
                return _err(404, "NOT_FOUND", "No results found for that query.")
    elif url:
        vidid = _vidid_from_url(str(url))
        if not vidid:
            return _err(400, "BAD_URL", "Not a YouTube URL or video id.")
    else:
        return _err(400, "NO_QUERY", "Provide 'query'/'q' or 'url'.")

    # For URL/vidid path, prefetch player so we get meta+audio in one shot.
    player = None
    if not search_meta:
        cached_meta = _meta_cache.get(vidid)
        cached_stream = _stream_cache.get(vidid)
        if cached_meta and cached_stream:
            meta = cached_meta
            stream_url = cached_stream["url"]
            source = cached_stream["source"]
        else:
            player = await innertube_player(session, vidid)
            stream_url, source, meta = await resolve_stream(session, vidid, player=player)
    else:
        stream_url, source, meta = await resolve_stream(session, vidid)

    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL right now — try again in a moment.")

    if search_meta:
        # Prefer fresh search metadata, fill gaps from player/meta.
        meta = {**meta, **{k: v for k, v in search_meta.items() if v}}

    return _ok({
        "vidid": vidid,
        "title": meta.get("title") or "YouTube Video",
        "duration": meta.get("duration") or "0:00",
        "artist": meta.get("artist") or "",
        "thumbnail_url": meta.get("thumbnail_url") or _thumb(vidid),
        "stream_url": stream_url,
        "source": source,
        "video_url": f"https://www.youtube.com/watch?v={vidid}",
    })


async def stream(request: web.Request) -> web.Response:
    vidid = request.match_info["vidid"]
    if not _BARE_VIDID_RE.fullmatch(vidid):
        return _err(400, "BAD_VIDID", "Invalid video id.")
    session = _session(request.app)
    stream_url, source, _ = await resolve_stream(session, vidid)
    if not stream_url:
        return _err(502, "RESOLVE_FAILED", "Could not resolve a stream URL right now — try again in a moment.")
    return _ok({"vidid": vidid, "stream_url": stream_url, "source": source})


async def options(request: web.Request) -> web.Response:
    return web.Response()


# ─── keep-alive ─────────────────────────────────────────────────────────────

async def _keep_alive(app: web.Application) -> None:
    await asyncio.sleep(5)
    target = f"{SELF_URL}/ping"
    logged = False
    session = _session(app)
    while True:
        await asyncio.sleep(random.uniform(8.0, 12.0))
        try:
            async with session.get(target, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
                if not logged and resp.status == 200:
                    log.info("keep-alive self-ping OK → %s (every 8–12s)", target)
                    logged = True
        except Exception as e:
            log.debug("keep-alive ping failed: %s", e)


async def _on_startup(app: web.Application) -> None:
    app["http_session"] = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20.0),
        connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
    )
    app["ka_task"] = asyncio.create_task(_keep_alive(app))


async def _on_cleanup(app: web.Application) -> None:
    task = app.get("ka_task")
    if task:
        task.cancel()
    sess = app.get("http_session")
    if sess:
        await sess.close()


def make_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware, auth_middleware])
    app.router.add_get("/", root)
    app.router.add_get("/ping", ping)
    app.router.add_get("/v1/healthz", healthz)
    app.router.add_route("*", "/v1/resolve", resolve)
    app.router.add_get("/v1/stream/{vidid}", stream)
    app.router.add_route("OPTIONS", "/{tail:.*}", options)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=BIND, port=PORT, access_log=None)


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
