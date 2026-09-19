// One origin for fast UI work: hot-reloaded JS from Metro, live data from the QA pilot.
//
// WHY THIS EXISTS. On web the app calls its API on its own origin (`apiBase` is '' in
// src/model.ts), and the session is a cookie. Metro on 8081 serves the JS but has no API;
// the pilot on 8083 has the API but serves a BUILT bundle. Pointing the browser at either
// one alone means a full `expo export` for every visual change - a production build to move
// a font. This proxy puts both behind a single origin so a style edit shows up on reload in
// about a second, with real data and a real session.
//
//   node scripts/dev-proxy.cjs            -> http://127.0.0.1:8090
//
// /api/* and the auth routes go to the QA pilot; everything else goes to Metro.
// Never point this at 8082: that is the owner's own pilot.
const http = require('http');

const PORT = Number(process.env.DEV_PROXY_PORT || 8090);
const API = { host: '127.0.0.1', port: Number(process.env.DEV_PROXY_API_PORT || 8083) };
const WEB = { host: '127.0.0.1', port: Number(process.env.DEV_PROXY_WEB_PORT || 8081) };
// ONLY the API crosses over. Every page route must reach Metro: /signin and the rest are
// client-side routes, and serving them from the pilot hands the browser the BUILT bundle,
// whose asset URLs then 404 against Metro and leave a blank page. The session cookie still
// works because the proxy makes both sides a single origin.
const TO_API = /^\/api(\/|$|\?)/;

// A browser that navigates away mid-request resets the socket. Node turns an unhandled
// socket error into a process exit, which would take the proxy down in the middle of a
// working session — so every socket gets a listener that simply lets the request die.
const ignore = (...streams) => streams.forEach(s => s && s.on('error', () => {}));

const proxy = (req, res, target) => {
  ignore(req, res, req.socket);
  const upstream = http.request({
    host: target.host, port: target.port, method: req.method, path: req.url,
    headers: { ...req.headers, host: `${target.host}:${target.port}` },
  }, r => { res.writeHead(r.statusCode || 502, r.headers); r.pipe(res); });
  upstream.on('error', e => {
    if (res.headersSent) return res.destroy();
    const who = target.port === API.port ? `the QA pilot on ${API.port}` : `Metro on ${WEB.port}`;
    res.writeHead(502, { 'content-type': 'text/plain' });
    res.end(`dev-proxy: ${who} is not answering (${e.code}).\n`);
  });
  req.pipe(upstream);
};

const server = http.createServer((req, res) =>
  proxy(req, res, TO_API.test(req.url || '') ? API : WEB));

// Metro's hot reload rides a websocket; without this, edits never reach the page.
server.on('upgrade', (req, socket, head) => {
  ignore(socket);
  const target = TO_API.test(req.url || '') ? API : WEB;
  const upstream = http.request({
    host: target.host, port: target.port, path: req.url, method: req.method,
    headers: { ...req.headers, host: `${target.host}:${target.port}` },
  });
  upstream.on('upgrade', (r, up, upHead) => {
    socket.write(`HTTP/1.1 101 Switching Protocols\r\n` +
      Object.entries(r.headers).map(([k, v]) => `${k}: ${v}`).join('\r\n') + '\r\n\r\n');
    if (upHead && upHead.length) up.unshift(upHead);
    up.pipe(socket); socket.pipe(up);
  });
  ignore(upstream);
  upstream.on('error', () => socket.destroy());
  if (head && head.length) upstream.write(head);
  upstream.end();
});

// Last resort: a reset that still escapes must not end the session.
process.on('uncaughtException', e => { if (e && /ECONNRESET|EPIPE|ECANCELED/.test(e.code||'')) return; throw e; });

server.listen(PORT, '127.0.0.1', () => {
  console.log(`dev-proxy on http://127.0.0.1:${PORT}`);
  console.log(`  bundle -> Metro ${WEB.port} (hot reload)   api -> QA pilot ${API.port}`);
});
