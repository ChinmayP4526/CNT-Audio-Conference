"""UDP echo endpoint. Run with: python -m udp_service.server"""
import argparse
import asyncio
import json
import time
from .packet import decode

class EchoProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print(f"UDP probe server listening on {transport.get_extra_info('sockname')}")

    def datagram_received(self, data, addr):
        try:
            packet = decode(data)
            packet["server_received_at"] = time.time()
            packet["server_addr"] = f"{addr[0]}:{addr[1]}"
            encoded = json.dumps(packet, separators=(",", ":")).encode()
            self.transport.sendto(encoded, addr)
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            print(f"Dropped malformed datagram from {addr}: {exc}")

async def main(host: str, port: int):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(lambda: EchoProtocol(), local_addr=(host, port))
    try:
        await asyncio.Future()
    finally:
        transport.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw UDP echo service for CNT measurement demos")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    args = parser.parse_args()
    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        print("UDP probe server stopped")
