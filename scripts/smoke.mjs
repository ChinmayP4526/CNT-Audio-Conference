// Backend integration smoke test using Node's built-in WebSocket client.
const base = process.env.BASE_URL || 'http://127.0.0.1:8000';
const room = `SMOKE-${Date.now().toString(36).toUpperCase()}`;
const wsBase = base.replace(/^http/, 'ws');
const clients = [];
function assert(ok, message) { if (!ok) throw new Error(message); }
function connect(name) {
  const ws = new WebSocket(`${wsBase}/ws/${room}`);
  const queue = [];
  const waiters = [];
  ws.addEventListener('message', e => {
    const msg = JSON.parse(e.data);
    const i = waiters.findIndex(w => !w.type || w.type === msg.type);
    if (i >= 0) waiters.splice(i, 1)[0].resolve(msg); else queue.push(msg);
  });
  const next = (type, timeout = 3000) => {
    const i = queue.findIndex(m => !type || m.type === type);
    if (i >= 0) return Promise.resolve(queue.splice(i, 1)[0]);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${type || 'message'}`)), timeout);
      waiters.push({type, resolve: msg => { clearTimeout(timer); resolve(msg); }});
    });
  };
  const opened = new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, {once:true});
    ws.addEventListener('error', () => reject(new Error('WebSocket connection failed')), {once:true});
  });
  const client = {ws, next, opened, join: async (create=false) => { await opened; ws.send(JSON.stringify({type:'join', name, create})); return next('welcome'); }};
  clients.push(client);
  return client;
}
async function rejection(roomId, name, expected) {
  const ws = new WebSocket(`${wsBase}/ws/${roomId}`);
  try {
    await new Promise((resolve, reject) => {
      ws.addEventListener('open', resolve, {once:true});
      ws.addEventListener('error', () => reject(new Error('Rejection test WebSocket failed')), {once:true});
    });
    ws.send(JSON.stringify({type:'join', name}));
    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timed out waiting for rejection')), 3000);
      ws.addEventListener('message', e => { clearTimeout(timer); resolve(JSON.parse(e.data)); }, {once:true});
    });
    assert(result.type === 'error' && result.message.includes(expected), `expected rejection: ${expected}`);
  } finally { ws.close(); }
}
try {
  const health = await fetch(`${base}/health`).then(r => r.json());
  assert(health.status === 'ok', 'health endpoint did not report ok');
  const first = connect('Yukta');
  assert((await first.join(true)).participants.length === 1, 'first participant missing');
  await rejection(`MISSING-${Date.now().toString(36).toUpperCase()}`, 'Guest', 'Room not found');
  await rejection(room, 'Yukta', 'already in this room');
  const second = connect('Shravani');
  assert((await second.join()).participants.length === 2, 'join snapshot should include both participants');
  assert((await first.next('participants')).participants.length === 2, 'two-user roster did not update');
  const third = connect('Atharv');
  assert((await third.join()).participants.length === 3, 'third participant snapshot incomplete');
  assert((await first.next('participants')).participants.length === 3, 'three-user roster did not update');
  assert((await second.next('participants')).participants.length === 3, 'second client roster did not update');
  first.ws.send(JSON.stringify({type:'state', muted:false, speaking:true}));
  const state = await second.next('participants');
  assert(state.participants.some(p => p.name === 'Yukta' && !p.muted && p.speaking), 'participant microphone/speaking state did not broadcast');
  await first.next('participants');
  await third.next('participants');
  first.ws.send(JSON.stringify({type:'telemetry', telemetry:{simulation:true,values:{rtt:220,loss:12,jitter:70,bw:100},label:'Network degradation detected.'}}));
  assert((await second.next('telemetry')).telemetry.values.loss === 12, 'modeled telemetry did not reach other room participants');
  await third.next('telemetry');
  first.ws.send(JSON.stringify({type:'chat',text:'Hello from the room'}));
  assert((await second.next('chat')).message.text === 'Hello from the room', 'room chat did not reach the second participant');
  assert((await third.next('chat')).message.name === 'Yukta', 'room chat did not reach the third participant');
  third.ws.close();
  const afterLeave = await first.next('participants');
  assert(afterLeave.participants.length === 2, 'leave did not update participant count');
  console.log('PASS: room create/join, input validation, 3 participants, state/simulation/chat broadcasts, and leave update');
} finally {
  for (const c of clients) { try { c.ws.close(); } catch {} }
}
