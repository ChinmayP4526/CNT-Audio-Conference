"""FastAPI room signaling for the CNT audio conference demo."""
from __future__ import annotations

import json
import base64
import hashlib
import hmac
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
ROOM_RE = re.compile(r"^[A-Z0-9-]{3,24}$")
NAME_RE = re.compile(r"^[\w .'-]{1,24}$", re.UNICODE)

@dataclass
class Peer:
    id: str
    name: str
    ws: WebSocket
    muted: bool = True
    speaking: bool = False
    joined_at: float = field(default_factory=time.time)

rooms: dict[str, dict[str, Peer]] = {}
room_messages: dict[str, list[dict[str, Any]]] = {}
app = FastAPI(title="Intelligent UDP Audio Conferencing System", version="1.0.0")
app.mount("/static", StaticFiles(directory=ROOT / "frontend"), name="static")
logger = logging.getLogger("cnt.signaling")

@app.get("/")
async def index():
    return FileResponse(ROOT / "frontend" / "index.html")

@app.get("/health")
async def health():
    return {"status": "ok", "rooms": len(rooms), "participants": sum(map(len, rooms.values()))}

@app.get("/api/ice-servers")
async def ice_servers(response: Response):
    """Return public STUN configuration and short-lived TURN REST credentials."""
    response.headers["Cache-Control"] = "no-store"
    stun_urls = [value.strip() for value in os.getenv(
        "ICE_STUN_URLS", "stun:stun.l.google.com:19302"
    ).split(",") if value.strip()]
    servers = [{"urls": url} for url in stun_urls]
    turn_urls = [value.strip() for value in os.getenv("TURN_URLS", "").split(",") if value.strip()]
    secret = os.getenv("TURN_SHARED_SECRET", "")
    result = {"iceServers": servers, "turnAvailable": False, "expiresAt": None}
    if turn_urls and secret:
        try:
            ttl = int(os.getenv("TURN_CREDENTIAL_TTL_SECONDS", "600"))
        except ValueError:
            ttl = 600
        ttl = max(60, min(ttl, 86400))
        username = f"{int(time.time()) + ttl}:{os.getenv('TURN_USERNAME_PREFIX', 'cnt')}"
        digest = hmac.new(secret.encode("utf-8"), username.encode("utf-8"), hashlib.sha1).digest()
        credential = base64.b64encode(digest).decode("ascii")
        servers.append({"urls": turn_urls, "username": username, "credential": credential})
        result.update(turnAvailable=True, expiresAt=int(username.split(":", 1)[0]))
    elif turn_urls:
        logger.warning("TURN_URLS is set but TURN_SHARED_SECRET is missing; TURN will not be advertised")
    return result

def snapshot(room: str) -> list[dict[str, Any]]:
    return [{"id": p.id, "name": p.name, "muted": p.muted, "speaking": p.speaking,
             "joinedAt": p.joined_at} for p in rooms.get(room, {}).values()]

async def send(peer: Peer, payload: dict[str, Any]):
    try:
        await peer.ws.send_text(json.dumps(payload))
    except Exception:
        pass

async def broadcast(room: str, payload: dict[str, Any], omit: str | None = None):
    for peer in list(rooms.get(room, {}).values()):
        if peer.id != omit:
            await send(peer, payload)

@app.websocket("/ws/{room}")
async def websocket_room(ws: WebSocket, room: str):
    await ws.accept()
    peer: Peer | None = None
    room = room.upper()
    try:
        hello = json.loads(await ws.receive_text())
        if not isinstance(hello, dict):
            raise ValueError("Initial WebSocket message must be a JSON object")
        name = str(hello.get("name", "")).strip()
        if not ROOM_RE.fullmatch(room) or not NAME_RE.fullmatch(name):
            await send(Peer("", "", ws), {"type": "error", "message": "Use a valid room ID and a name of 1–24 characters."})
            await ws.close(code=1008)
            return
        members = rooms.get(room)
        if hello.get("create"):
            if members:
                await send(Peer("", "", ws), {"type": "error", "message": "That room ID is already in use. Create another room."})
                await ws.close(code=1008)
                return
            members = rooms.setdefault(room, {})
        elif not members:
            await send(Peer("", "", ws), {"type": "error", "message": "Room not found. Check the room ID or ask the host to create it."})
            await ws.close(code=1008)
            return
        if any(p.name.casefold() == name.casefold() for p in members.values()):
            await send(Peer("", "", ws), {"type": "error", "message": "That username is already in this room."})
            await ws.close(code=1008)
            return
        peer = Peer(uuid.uuid4().hex[:10], name, ws)
        rooms[room][peer.id] = peer
        await send(peer, {"type": "welcome", "selfId": peer.id, "participants": snapshot(room), "messages": room_messages.get(room, [])})
        await broadcast(room, {"type": "participants", "participants": snapshot(room)}, omit=peer.id)
        await broadcast(room, {"type": "event", "message": f"{name} joined the room."}, omit=peer.id)
        while True:
            msg = json.loads(await ws.receive_text())
            if not isinstance(msg, dict):
                await send(peer, {"type": "error", "message": "WebSocket messages must be JSON objects."})
                continue
            kind = msg.get("type")
            if kind in {"offer", "answer", "ice"}:
                target = rooms.get(room, {}).get(str(msg.get("to", "")))
                if target:
                    await send(target, {"type": kind, "from": peer.id, "description": msg.get("description"), "candidate": msg.get("candidate")})
            elif kind in {"state", "telemetry", "event", "chat"}:
                if kind == "state":
                    peer.muted = bool(msg.get("muted", peer.muted))
                    peer.speaking = bool(msg.get("speaking", peer.speaking))
                    await broadcast(room, {"type": "participants", "participants": snapshot(room)})
                elif kind == "telemetry":
                    await broadcast(room, {"type": "telemetry", "from": peer.id, "telemetry": msg.get("telemetry", {})})
                elif kind == "chat":
                    text = str(msg.get("text", "")).strip()
                    if text:
                        entry = {"id": uuid.uuid4().hex[:12], "peerId": peer.id, "name": peer.name, "text": text[:1000], "sentAt": time.time()}
                        history = room_messages.setdefault(room, [])
                        history.append(entry)
                        del history[:-200]
                        await broadcast(room, {"type": "chat", "message": entry})
                else:
                    await broadcast(room, {"type": "event", "message": str(msg.get("message", ""))[:180]}, omit=peer.id)
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass
    except Exception:
        logger.exception("Unexpected signaling error in room %s", room)
        if peer:
            await send(peer, {"type": "error", "message": "The signaling server encountered an error. Please rejoin the room."})
    finally:
        if peer and room in rooms and peer.id in rooms[room]:
            del rooms[room][peer.id]
            if rooms[room]:
                await broadcast(room, {"type": "participants", "participants": snapshot(room)})
                await broadcast(room, {"type": "event", "message": f"{peer.name} left the room."})
            else:
                del rooms[room]
                room_messages.pop(room, None)
