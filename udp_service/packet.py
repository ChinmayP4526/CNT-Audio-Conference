"""Wire format for the standalone UDP measurement demonstration."""
import json
import time
import uuid

def encode(client_id: str, room_id: str, sequence: int, kind: str = "probe") -> bytes:
    return json.dumps({"client_id": client_id, "room_id": room_id, "sequence": sequence,
                       "timestamp": time.time(), "kind": kind}, separators=(",", ":")).encode()

def decode(data: bytes) -> dict:
    packet = json.loads(data.decode())
    for key in ("client_id", "room_id", "sequence", "timestamp"):
        if key not in packet:
            raise ValueError(f"Missing packet field: {key}")
    return packet

def new_client_id() -> str:
    return uuid.uuid4().hex[:10]
