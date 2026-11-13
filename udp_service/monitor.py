"""Send numbered UDP probes and report RTT, sequence gaps, jitter and throughput."""
import argparse
import asyncio
import json
import statistics
import time
from .packet import encode, new_client_id

class ProbeProtocol(asyncio.DatagramProtocol):
    def __init__(self, count, interval, room):
        self.count, self.interval, self.room = count, interval, room
        self.client_id = new_client_id()
        self.sent = {}
        self.received = []
        self.bytes_in = 0
        self.done = asyncio.get_running_loop().create_future()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        asyncio.create_task(self.run())

    async def run(self):
        for seq in range(self.count):
            now = time.time()
            self.sent[seq] = now
            self.transport.sendto(encode(self.client_id, self.room, seq))
            await asyncio.sleep(self.interval)
        await asyncio.sleep(max(1.0, self.interval * 3))
        if not self.done.done():
            self.done.set_result(None)

    def datagram_received(self, data, addr):
        now = time.time()
        try:
            p = json.loads(data.decode())
            if p.get("client_id") != self.client_id:
                return
            seq = int(p["sequence"])
            if seq in self.sent:
                self.received.append((seq, now, (now - self.sent[seq]) * 1000))
                self.bytes_in += len(data)
        except (ValueError, KeyError, json.JSONDecodeError):
            return

async def run(host, port, count, interval, room):
    loop = asyncio.get_running_loop()
    protocol = ProbeProtocol(count, interval, room)
    transport, _ = await loop.create_datagram_endpoint(lambda: protocol, remote_addr=(host, port))
    started = time.time()
    try:
        await protocol.done
    finally:
        transport.close()
    got = {seq for seq, _, _ in protocol.received}
    missing = sorted(set(range(count)) - got)
    rtts = [rtt for _, _, rtt in protocol.received]
    arrivals = [t for _, t, _ in sorted(protocol.received)]
    variation = [abs((arrivals[i] - arrivals[i - 1]) * 1000 - interval * 1000) for i in range(1, len(arrivals))]
    elapsed = max(time.time() - started, .001)
    result = {
        "sent": count, "received": len(got), "missing_sequences": missing,
        "packet_loss_percent": (count - len(got)) * 100 / max(1, count),
        "rtt_avg_ms": round(statistics.mean(rtts), 2) if rtts else None,
        "rtt_min_ms": round(min(rtts), 2) if rtts else None,
        "rtt_max_ms": round(max(rtts), 2) if rtts else None,
        "inter_arrival_jitter_ms": round(statistics.mean(variation), 2) if variation else None,
        "observed_receive_kbps": round(protocol.bytes_in * 8 / elapsed / 1000, 2),
        "note": "RTT uses echoed client timestamps; jitter is mean absolute deviation from configured probe interval."
    }
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UDP packet loss / latency / jitter probe")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--interval", type=float, default=.1)
    parser.add_argument("--room", default="CNT-DEMO")
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port, max(1, args.count), max(.01, args.interval), args.room))
