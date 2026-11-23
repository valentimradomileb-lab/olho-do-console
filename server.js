// Olho do Console — site SOMENTE LEITURA do console do servidor de Minecraft (painel Pterodactyl / Enxada Host)
// O site se conecta no console do painel com a sua chave de API e só RECEBE as linhas.
// Não existe nenhuma rota nem código que envie comando pro servidor.

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const WebSocket = require('ws');

const PANEL = (process.env.PANEL_URL || 'https://painel.enxadahost.com').replace(/\/+$/, '');
const KEY = process.env.API_KEY;
const SID = process.env.SERVER_ID;
const SENHA = process.env.SENHA;
const PORT = Number(process.env.PORT || 8080);

if (!KEY || !SID || !SENHA) {
  console.error('Faltam configurações. Defina API_KEY, SERVER_ID e SENHA (veja o LEIA-ME).');
  process.exit(1);
}

const COOKIE = crypto.createHmac('sha256', SENHA).update('olho-do-console').digest('hex');
const MAX = 2000;
const buf = [];
const recent = new Map();
let seq = 0;
const clients = new Set();
let state = { status: 'conectando', players: [], online: null, max: null, mem: null };

// ------------------------------------------------------------------ painel

function api(path) {
  return fetch(PANEL + '/api/client' + path, {
    headers: { Authorization: 'Bearer ' + KEY, Accept: 'Application/vnd.pterodactyl.v1+json' },
  }).then(async (r) => {
    if (!r.ok) throw new Error('Painel respondeu ' + r.status + ' em ' + path);
    return r.json();
  });
}

const ANSI = /\x1b\[[0-9;?]*[ -\/]*[@-~]/g;
const IP_BRACKET = /\[\/[^\]]*\]/g;
const IPV4 = /\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b/g;
const clean = (s) => s.replace(ANSI, '').replace(/\r/g, '').replace(IP_BRACKET, '[ip oculto]').replace(IPV4, 'x.x.x.x');

let replayUntil = 0;
function addLine(raw) {
  for (const part of String(raw).split('\n')) {
    const t = clean(part);
    if (!t.trim()) continue;
    if (Date.now() < replayUntil && recent.has(t)) continue; // evita repetir histórico ao reconectar
    seq++;
    buf.push({ id: seq, t });
    recent.set(t, (recent.get(t) || 0) + 1);
    while (buf.length > MAX) {
      const old = buf.shift();
      const n = recent.get(old.t) - 1;
      n > 0 ? recent.set(old.t, n) : recent.delete(old.t);
    }
    broadcast(`id: ${seq}\ndata: ${t}\n\n`);
  }
}

function setState(patch) {
  const next = { ...state, ...patch };
  if (JSON.stringify(next) === JSON.stringify(state)) return;
  state = next;
  broadcast(`event: state\ndata: ${JSON.stringify(state)}\n\n`);
}

let ws = null;
let retry = null;
function scheduleReconnect() {
  clearTimeout(retry);
  retry = setTimeout(connect, 5000);
}

async function connect() {
  try {
    const { data } = await api(`/servers/${SID}/websocket`);
    ws = new WebSocket(data.socket, { headers: { Origin: PANEL } });
    const sock = ws;
    sock.on('open', () => sock.send(JSON.stringify({ event: 'auth', args: [data.token] })));
    sock.on('message', (msg) => {
      let e;
      try { e = JSON.parse(msg); } catch { return; }
      switch (e.event) {
        case 'auth success':
          replayUntil = Date.now() + 3000;
          sock.send(JSON.stringify({ event: 'send logs', args: [null] }));
          sock.send(JSON.stringify({ event: 'send stats', args: [null] }));
          console.log('Conectado ao console do painel.');
          break;
        case 'console output':
          addLine(e.args[0]);
          break;
        case 'status':
          setState({ status: e.args[0] });
          break;
        case 'stats':
          try {
            const s = JSON.parse(e.args[0]);
            setState({ status: s.state, mem: Math.round(s.memory_bytes / 1048576) });
          } catch {}
          break;
        case 'token expiring':
          api(`/servers/${SID}/websocket`)
            .then((r) => sock.send(JSON.stringify({ event: 'auth', args: [r.data.token] })))
            .catch(() => {});
          break;
        case 'token expired':
        case 'jwt error':
          sock.close();
          break;
      }
    });
    sock.on('close', () => { setState({ status: 'conectando' }); scheduleReconnect(); });
    sock.on('error', (err) => console.error('Erro no console:', err.message));
  } catch (err) {
    console.error(err.message);
    setState({ status: 'erro' });
    scheduleReconnect();
  }
}

// ------------------------------------------------------------------ quem está online (ping igual ao da lista de servidores)

const varint = (n) => { const b = []; do { let x = n & 0x7f; n >>>= 7; if (n) x |= 0x80; b.push(x); } while (n); return Buffer.from(b); };

function pingMC(host, port) {
  return new Promise((resolve, reject) => {
    const s = net.connect(port, host);
    let data = Buffer.alloc(0);
    const fail = (err) => { clearTimeout(to); s.destroy(); reject(err); };
    const to = setTimeout(() => fail(new Error('timeout')), 5000);
    s.on('connect', () => {
      const h = Buffer.from(host, 'utf8');
      const body = Buffer.concat([varint(0), varint(767), varint(h.length), h, Buffer.from([port >> 8, port & 255]), varint(1)]);
      s.write(Buffer.concat([varint(body.length), body, Buffer.from([1, 0])]));
    });
    s.on('data', (d) => {
      data = Buffer.concat([data, d]);
      let o = 0;
      const rv = () => { let r = 0, sh = 0, b; do { if (o >= data.length) throw 'incompleto'; b = data[o++]; r |= (b & 0x7f) << sh; sh += 7; } while (b & 0x80); return r; };
      try {
        const len = rv();
        if (data.length < o + len) return;
        rv();
        const sl = rv();
        const json = JSON.parse(data.slice(o, o + sl).toString('utf8'));
        clearTimeout(to); s.destroy(); resolve(json);
      } catch (err) { if (err !== 'incompleto') fail(err); }
    });
    s.on('error', fail);
  });
}

let mcHost = process.env.MC_HOST || null;
let mcPort = Number(process.env.MC_PORT || 0) || null;
async function updatePlayers() {
  try {
    if (!mcHost) {
      const srv = await api(`/servers/${SID}`);
      const allocs = srv.attributes.relationships.allocations.data.map((a) => a.attributes);
      const a = allocs.find((x) => x.is_default) || allocs[0];
      mcHost = a.ip_alias || a.ip;
      mcPort = a.port;
    }
    const j = await pingMC(mcHost, mcPort);
    setState({ online: j.players.online, max: j.players.max, players: (j.players.sample || []).map((p) => p.name).filter((n) => !/§/.test(n)) });
  } catch {
    setState({ online: null, players: [] });
  }
}

// ------------------------------------------------------------------ site

function authed(req) {
  const m = /(?:^|;\s*)olho=([a-f0-9]{64})/.exec(req.headers.cookie || '');
  return !!m && crypto.timingSafeEqual(Buffer.from(m[1]), Buffer.from(COOKIE));
}

function send(res, code, type, body) {
  res.writeHead(code, { 'Content-Type': type + '; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}

function broadcast(chunk) {
  for (const c of clients) c.write(chunk);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');

  if (url.pathname === '/' && req.method === 'GET') {
    return send(res, 200, 'text/html', authed(req) ? PAGE : LOGIN.replace('{{ERRO}}', ''));
  }

  if (url.pathname === '/login' && req.method === 'POST') {
    let body = '';
    req.on('data', (d) => { body += d; if (body.length > 4096) req.destroy(); });
    req.on('end', () => {
      const pw = new URLSearchParams(body).get('senha') || '';
      const a = crypto.createHash('sha256').update(pw).digest();
      const b = crypto.createHash('sha256').update(SENHA).digest();
      if (crypto.timingSafeEqual(a, b)) {
        res.writeHead(303, { 'Set-Cookie': `olho=${COOKIE}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000`, Location: '/' });
        return res.end();
      }
      setTimeout(() => send(res, 401, 'text/html', LOGIN.replace('{{ERRO}}', '<p class=err>Senha errada.</p>')), 1500);
    });
    return;
  }

  if (url.pathname === '/stream' && req.method === 'GET') {
    if (!authed(req)) return send(res, 401, 'text/plain', 'faça login');
    res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no' });
    const last = Number(req.headers['last-event-id'] || 0);
    let out = '';
    for (const l of buf) if (l.id > last) out += `id: ${l.id}\ndata: ${l.t}\n\n`;
    out += `event: state\ndata: ${JSON.stringify(state)}\n\n`;
    res.write(out);
    clients.add(res);
    const ping = setInterval(() => res.write(': ping\n\n'), 20000);
    req.on('close', () => { clearInterval(ping); clients.delete(res); });
    return;
  }

  if (req.method !== 'GET') return send(res, 405, 'text/plain', 'somente leitura');
  send(res, 404, 'text/plain', '404');
});

server.listen(PORT, () => {
  console.log(`Olho do Console no ar: http://localhost:${PORT}  (somente leitura)`);
  connect();
  updatePlayers();
  setInterval(updatePlayers, 15000);
});

// ------------------------------------------------------------------ páginas

const STYLE = `<style>
:root{--bg:#0d1014;--panel:#151a21;--line:#232a33;--txt:#c9d1d9;--dim:#6b7683;--green:#5fd068;--yellow:#e3b341;--red:#f2545b;--purple:#c792ea;--cyan:#56c8d8}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font:14px/1.45 ui-monospace,Consolas,'Cascadia Mono',monospace}.err{color:var(--red)}
</style>`;

const LOGIN = `<!DOCTYPE html><html lang=pt-BR><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Olho do Console</title>${STYLE}
<style>form{max-width:340px;margin:18vh auto;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:28px}
h1{font-size:18px;margin:0 0 6px}p{color:var(--dim);margin:0 0 18px}input,button{width:100%;padding:11px;border-radius:8px;font:inherit}
input{background:var(--bg);border:1px solid var(--line);color:var(--txt);margin-bottom:12px}button{background:var(--green);border:0;color:#08110a;font-weight:700;cursor:pointer}</style></head>
<body><form method=post action=/login><h1>👁 Olho do Console</h1><p>Console do servidor de Minecraft ao vivo. Só pra ver: ninguém manda comando por aqui.</p>
{{ERRO}}<input type=password name=senha placeholder=Senha autofocus><button>Entrar</button></form></body></html>`;

const PAGE = `<!DOCTYPE html><html lang=pt-BR><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Olho do Console</title>${STYLE}
<style>
body{display:flex;flex-direction:column;height:100vh}
header{display:flex;gap:12px;align-items:center;flex-wrap:wrap;padding:10px 16px;background:var(--panel);border-bottom:1px solid var(--line)}
header h1{font-size:15px;margin:0}.tag{font-size:11px;color:var(--dim);border:1px solid var(--line);border-radius:99px;padding:2px 8px}
#st{font-size:12px;border-radius:99px;padding:2px 10px;background:#f2545b22;color:var(--red)}#st.running{background:#5fd06822;color:var(--green)}#st.starting,#st.stopping,#st.conectando{background:#e3b34122;color:var(--yellow)}
#mem{font-size:12px;color:var(--dim)}.sp{flex:1}
input{background:var(--bg);border:1px solid var(--line);color:var(--txt);border-radius:7px;padding:6px 10px;font:inherit;width:200px}
button{background:var(--bg);border:1px solid var(--line);color:var(--txt);border-radius:7px;padding:6px 10px;font:inherit;cursor:pointer}button.on{border-color:var(--green);color:var(--green)}
main{flex:1;display:flex;min-height:0}#log{flex:1;overflow:auto;padding:10px 16px;white-space:pre-wrap;word-break:break-word}
aside{width:190px;border-left:1px solid var(--line);padding:12px;background:var(--panel)}aside h2{font-size:12px;color:var(--dim);margin:0 0 8px;text-transform:uppercase;letter-spacing:.06em}
#players div{padding:3px 0;color:var(--green)}#players div::before{content:'● '}
.warn{color:var(--yellow)}.error{color:var(--red)}.join{color:var(--green)}.left{color:var(--dim)}.cmd{color:var(--purple);background:#c792ea14}.chat{color:var(--cyan)}
@media(max-width:700px){aside{display:none}input{width:120px}}
</style></head><body>
<header><h1>👁 Olho do Console</h1><span class=tag>somente leitura</span><span id=st class=conectando>conectando</span><span id=mem></span><span class=sp></span>
<input id=q placeholder="Filtrar..."><button id=cmdOnly>Só comandos</button><button id=auto class=on>Rolagem automática</button></header>
<main><div id=log></div><aside><h2>Online (<span id=count>–</span>)</h2><div id=players></div></aside></main>
<script>
const log=document.getElementById('log'),q=document.getElementById('q'),autoB=document.getElementById('auto'),cmdB=document.getElementById('cmdOnly');
let auto=true,cmdOnly=false;
const NOMES={running:'ligado',starting:'iniciando',stopping:'desligando',offline:'desligado',conectando:'conectando',erro:'erro na chave/painel'};
function cls(t){
  if(/\\/(ERROR|FATAL)\\]|\\bERROR\\b/.test(t))return'error';if(/\\/WARN\\]/.test(t))return'warn';
  if(/ joined the game/.test(t))return'join';if(/ left the game/.test(t))return'left';
  if(/\\]: \\[[^\\]]+: /.test(t)||/issued server command/i.test(t))return'cmd';
  if(/\\]: (\\[Not Secure\\] )?<[^>]+>/.test(t))return'chat';return''}
function vis(d){const s=q.value.toLowerCase();return(!s||d.textContent.toLowerCase().includes(s))&&(!cmdOnly||d.classList.contains('cmd'))}
function refilter(){for(const d of log.children)d.style.display=vis(d)?'':'none';if(auto)log.scrollTop=log.scrollHeight}
q.oninput=refilter;
cmdB.onclick=()=>{cmdOnly=!cmdOnly;cmdB.classList.toggle('on',cmdOnly);refilter()};
autoB.onclick=()=>{auto=!auto;autoB.classList.toggle('on',auto);if(auto)log.scrollTop=log.scrollHeight};
log.onscroll=()=>{const fim=log.scrollHeight-log.scrollTop-log.clientHeight<40;if(auto!==fim){auto=fim;autoB.classList.toggle('on',auto)}};
const es=new EventSource('/stream');
es.onmessage=e=>{const d=document.createElement('div');d.className=cls(e.data);d.textContent=e.data;if(!vis(d))d.style.display='none';
  log.appendChild(d);while(log.children.length>3000)log.firstChild.remove();if(auto)log.scrollTop=log.scrollHeight};
es.addEventListener('state',e=>{const s=JSON.parse(e.data);const st=document.getElementById('st');st.className=s.status;st.textContent=NOMES[s.status]||s.status;
  document.getElementById('mem').textContent=s.mem!=null&&s.status==='running'?s.mem+' MB de RAM':'';
  document.getElementById('count').textContent=s.online==null?'–':s.online+(s.max?'/'+s.max:'');
  const box=document.getElementById('players');box.innerHTML='';s.players.forEach(n=>{const d=document.createElement('div');d.textContent=n;box.appendChild(d)})});
</script></body></html>`;
