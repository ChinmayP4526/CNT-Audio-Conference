# Intelligent UDP Audio Conferencing System

A Computer Networks project with a real multi-user browser voice room, an adaptive network dashboard, and an independent Python UDP packet measurement service.

## Project overview

Participants join a room by name and room ID. FastAPI holds ephemeral room membership and relays WebSocket signaling messages. Each browser forms a peer-to-peer WebRTC connection with every other participant; microphone audio is carried by WebRTC media transport (ICE prefers UDP when the network allows it). The server does **not** relay or record the conference audio. To create a specific shareable ID such as `CNT101`, enter it in the Room ID field and choose **Create room**; leave it blank for an automatically generated ID.

The Network Control Room samples browser `RTCPeerConnection.getStats()` reports for inbound audio jitter, packet loss, throughput and candidate-pair RTT when exposed by the browser. RTT is a round-trip estimate, not one-way latency. A separate simulation page injects modeled values into the dashboard and adaptation rules; it does not impair the actual WebRTC media path. The independent UDP probe sends actual numbered UDP datagrams and calculates sequence gaps, RTT, inter-arrival jitter and receive throughput.

## Architecture

```text
Browser A microphone ── WebRTC media over ICE (UDP preferred) ── Browser B/C/…
         │                                                        │
         └── WebSocket offers / answers / ICE ── FastAPI ─────────┘

WebRTC getStats ── Monitor ── Analyze (quality thresholds) ── Adaptive profile recommendation

UDP probe client ── numbered UDP datagrams ── UDP echo service ── packet statistics
```

The browser mesh creates one connection per participant pair. It works for the requested small classroom rooms; a larger deployment would use an SFU (selective forwarding unit), authentication, persistence, TURN relays, rate limiting and TLS.

## Requirements

- Python 3.10 or newer
- A modern browser with WebRTC, Web Audio and microphone support
- Microphone access is restricted by browsers to secure origins. `http://localhost` works for local testing; other devices need HTTPS (or a trusted local development certificate).

## Install and run on Windows PowerShell

From the project directory:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Open [http://localhost:8000](http://localhost:8000). The frontend is served by FastAPI; no separate frontend build step is needed. For another device on your LAN, open `https://<host>` after configuring HTTPS at a reverse proxy. `--host 0.0.0.0` makes the service listen on LAN interfaces, but plain HTTP on a LAN IP will not usually receive microphone permission.

Optional signaling integration smoke check (with the backend running):

```powershell
node scripts/smoke.mjs
```

### Environment variables

No environment variables or database are required for the local demo. Rooms are held in memory and disappear when the server restarts. For deployment, terminate TLS at a reverse proxy and configure firewall rules for HTTPS, WebSocket signaling, and WebRTC ICE connectivity. A TURN service may be needed for restrictive NATs.

## Three-user demo

1. Start the server and open `http://localhost:8000` in three browser tabs. Use different names in each tab (duplicate names within a room are rejected).
2. In tab one, go to **Conference**, enter `Yukta` and a room ID such as `CNT101`, then create the room. Copy the displayed room ID.
3. In tabs two and three, enter `Shravani` and `Atharv`, enter that room ID, and join.
4. Enable the microphone in each tab and allow the browser permission. Participants should appear dynamically, and microphone/VAD status updates as the room changes.
5. Open **Messages** in each tab to chat live with the same room. The room keeps the most recent 200 messages while at least one participant remains connected.
6. Use **Network control** to view available browser media statistics. Before remote media has arrived, some values correctly display as unavailable.
7. Use **Network simulation** and apply 220 ms RTT, 12% loss, 70 ms jitter and 100 kbps capacity. The modeled dashboard should classify the path as POOR and recommend NETWORK SAVER, a reduced audio target, increased jitter buffering and redundancy.
8. Reset to healthy to show recovery.

If audio cannot connect between separate networks, configure a TURN server. The demo uses a public STUN server for ICE discovery and cannot bypass every firewall/NAT configuration. In local same-device tabs, feedback can occur if speakers and microphones are close; use headphones.

## Run the actual UDP packet demonstration

In one terminal:

```powershell
python -m udp_service.server --host 0.0.0.0 --port 9999
```

In another terminal:

```powershell
python -m udp_service.monitor --host 127.0.0.1 --port 9999 --count 100 --interval 0.1 --room CNT101
```

The client sends JSON UDP datagrams containing client ID, room ID, sequence number, timestamp and packet kind. The server echoes valid datagrams. The client reports missing sequence numbers, loss percentage, average/min/max RTT, inter-arrival jitter and observed receive throughput. Run the client against a different reachable host to measure a network path; firewall UDP port 9999 as appropriate. UDP does not guarantee delivery or order. This probe is for network concepts and is not the browser audio transport.

## How adaptation works

Quality thresholds classify measured or explicitly simulated telemetry: POOR above 180 ms RTT, 8% loss, 50 ms jitter or below 150 kbps simulated capacity; MODERATE above 100 ms RTT, 3% loss, 25 ms jitter or below 400 kbps simulated capacity; otherwise GOOD. The engine recommends 64/32/16 kbps targets, Normal/Increased/High jitter buffer profiles and 0/10/20% redundancy according to quality. When a microphone is active, the app also applies the target bitrate with `RTCRtpSender.setParameters()` where supported; browsers may clamp or reject that control. Jitter buffer and redundancy values are recommendations because browsers manage playout and packet recovery internally. Browser WebRTC's own congestion control and packet-loss concealment keep audio flowing.

## Network concepts shown

- **UDP:** connectionless datagrams in the separate Python socket service; delivery and ordering are not guaranteed.
- **IP and ports:** host/port addressing on UDP probes and ICE-selected WebRTC paths.
- **Packetization / sequence numbers:** the UDP probe numbers each application-level datagram; the receiver identifies gaps.
- **Latency:** probe echo RTT or browser candidate-pair RTT where available. One-way latency needs synchronized clocks and is not claimed here.
- **Packet loss:** missing UDP sequence numbers or the browser's inbound RTP counters where the browser exposes them.
- **Jitter:** variation in UDP inter-arrival timing or inbound RTP jitter statistics.
- **Bandwidth / throughput:** observed received bytes per time window; it is distinct from the recommended audio bitrate and does not prove all available capacity.
- **VAD:** Web Audio time-domain amplitude threshold on the local microphone estimates whether the user is speaking. It updates participant state; it is not a trained speech recognizer, and silence suppression savings are not presented as measured.
- **Jitter buffer / recovery:** the dashboard recommends a buffer profile; WebRTC manages its actual playout buffer and packet-loss concealment internally.

## Limitations and future scope

This is a local academic demonstration. Signaling and rooms are in-memory without authentication. Mesh bandwidth and CPU load grow with participant count. Public STUN availability and direct ICE connectivity depend on network policy. Browser APIs expose different statistics across vendors. Simulation is intentionally separate from real media impairments. Useful extensions include an SFU, TURN deployment, authenticated room tokens, durable analytics, real congestion emulation, codec/bitrate sender tuning, and a secured UDP packet monitor interface.
#   C N T - A u d i o - C o n f e r e n c e  
 #   C N T - A u d i o - C o n f e r e n c e  
 #   C N T - A u d i o - C o n f e r e n c e  
 