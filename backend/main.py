"""FastAPI room signaling for the CNT audio conference demo."""
from __future__ import annotations

import io
import json
import re
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
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
app = FastAPI(title="cnt_cp", version="1.0.0")
app.mount("/static", StaticFiles(directory=ROOT / "frontend"), name="static")

@app.get("/")
async def index():
    return FileResponse(ROOT / "frontend" / "index.html")

@app.get("/health")
async def health():
    return {"status": "ok", "rooms": len(rooms), "participants": sum(map(len, rooms.values()))}

@app.get("/download-zip")
async def download_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in ROOT.rglob("*"):
            if file.is_file():
                rel = file.relative_to(ROOT)
                parts = rel.parts
                if any(p in {".git", "__pycache__", ".venv", ".pytest_cache"} or p.endswith(".pyc") or p.endswith(".zip") for p in parts):
                    continue
                zf.write(file, arcname=str(rel))
    buf.seek(0)
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="cnt_cp.zip"'}
    )

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
    finally:
        if peer and room in rooms and peer.id in rooms[room]:
            del rooms[room][peer.id]
            if rooms[room]:
                await broadcast(room, {"type": "participants", "participants": snapshot(room)})
                await broadcast(room, {"type": "event", "message": f"{peer.name} left the room."})
            else:
                del rooms[room]
                room_messages.pop(room, None)
