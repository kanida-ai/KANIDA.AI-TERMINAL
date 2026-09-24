/* KANIDA market story — prototype behaviour.
 *
 * One reading drives the whole page. Selecting a reading in the timeline re-renders the narrative,
 * the side summaries, the chart annotations, the strike participation and the evidence from that one
 * reading's object — so no section can show a time, a strike or a number another section disagrees
 * with, and nothing after the selected reading is drawn.
 */
(function () {
  'use strict';

  var S = window.KANIDA_SCENARIOS;
  var BREADTH = window.KANIDA_BREADTH;
  var $ = function (id) { return document.getElementById(id); };
  var el = function (tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  };

  var state = { s: 0, i: S[0].defaultIndex, ev: false, side: 'calls', tlAll: false };
  var anims = [], rafId = 0, timers = [];

  function scenario() { return S[state.s]; }
  function reading() { return scenario().readings[state.i]; }
  function quiet() {
    return document.documentElement.getAttribute('data-quiet') === 'on' ||
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /* ---------- scenario selector ------------------------------------------------------------- */
  function buildScenarios() {
    var box = document.querySelector('.scenarios');
    S.forEach(function (sc, idx) {
      var b = el('button', 'scn', sc.name);
      b.type = 'button';
      b.setAttribute('aria-pressed', String(idx === state.s));
      b.title = sc.blurb;
      b.addEventListener('click', function () {
        if (state.s === idx) return;
        stop();
        state.s = idx;
        state.i = S[idx].defaultIndex;
        state.tlAll = false;
        [].forEach.call(box.children, function (c, j) { c.setAttribute('aria-pressed', String(j === idx)); });
        render(true);
      });
      box.appendChild(b);
    });
  }

  /* ---------- context ------------------------------------------------------------------------ */
  function renderContext() {
    var sc = scenario(), r = reading();
    $('ctxInstrument').textContent = sc.instrument;
    $('ctxExpiry').textContent = sc.expiry + ' · ' + sc.dte + 'd';
    $('ctxSession').textContent = sc.sessionLabel;
    $('ctxReading').textContent = r.at + ' IST' + (r.taken ? '' : ' · not captured');
    $('ctxMode').textContent = sc.mode;
    var cap = $('ctxCapture');
    cap.className = 'chip ' + (sc.capture.tone === 'warn' ? 'warn' : 'ok');
    cap.lastChild.textContent = sc.capture.label;
    cap.title = sc.capture.detail;
    $('storyAside').textContent = sc.instrument + ' · ' + r.at + ' IST · ' + sc.sessionLabel;
  }

  /* ---------- word groups --------------------------------------------------------------------- */
  function groups(text, n) {
    var w = String(text || '').split(' '), out = [], i;
    for (i = 0; i < w.length; i += n) out.push(w.slice(i, i + n).join(' '));
    return out;
  }
  function fillLede(node, text) {
    node.textContent = '';
    if (!text) { node.hidden = true; return; }
    node.hidden = false;
    groups(text, 5).forEach(function (g, i) {
      var s = el('span', 'wg', (i ? ' ' : '') + g);
      node.appendChild(s);
    });
  }

  /* ---------- story ---------------------------------------------------------------------------- */
  function renderStory() {
    var r = reading(), c = r.calls;
    var b = $('behav');
    var tone = r.taken === false ? 'gap'
      : (c.behavior === 'unconfirmed' || c.behavior === 'none') ? 'unconf' : '';
    b.className = 'behav ' + tone;
    b.lastChild.textContent =
      r.taken === false ? 'Not observed'
        : c.behavior === 'unconfirmed' ? 'Unconfirmed'
          : c.behavior === 'none' ? 'Baseline'
            : c.behavior + ' · ' + c.state;

    var bc = $('breadthChip');
    bc.textContent = BREADTH[c.breadth] || BREADTH.none;
    bc.className = 'chip' + (c.strikes.length > 1 ? ' ok' : '');

    $('headline').textContent = r.headline;
    fillLede($('lede1'), r.sentences[0]);
    fillLede($('lede2'), r.sentences[1]);
    $('persist').textContent = r.persistence;
    $('infer').textContent = r.inference;
    $('conflict').hidden = !r.conflict;
    if (r.conflict) $('conflictText').textContent = r.conflict;
  }

  /* ---------- sides ------------------------------------------------------------------------------ */
  function sideCard(node, side, label) {
    node.textContent = '';
    node.appendChild(el('h3', null, label));
    var st = el('div', 'state' + (side.behavior === 'unconfirmed' ? ' unconf'
      : (side.behavior === 'none' || side.behavior === 'gap') ? ' muted' : ''), side.text);
    node.appendChild(st);

    var dl = el('dl', 'kv');
    function row(k, v) { dl.appendChild(el('dt', null, k)); dl.appendChild(el('dd', 'num', v)); }
    row('Location', side.location || '—');
    row('Leading strike', side.lead != null ? side.lead.toLocaleString('en-IN') : '—');
    row('Breadth', side.strikes.length ? (BREADTH[side.breadth] || '—') : '—');
    row('First detected', side.firstAt ? side.firstAt + ' IST' : '—');
    row('Duration', side.durationMin == null ? '—'
      : (side.durationMin === 0 ? 'just detected' : side.durationMin + ' min'
        + (side.frozen ? ' · frozen' : '')));
    row('Consecutive readings', side.scans ? String(side.scans) : '—');
    node.appendChild(dl);
    node.appendChild(el('p', 'note', side.note));
  }

  function renderSides() {
    var r = reading();
    sideCard($('sideCalls'), r.calls, 'Calls');
    sideCard($('sidePuts'), r.puts, 'Puts');
    var comb = $('sideCombined');
    comb.textContent = '';
    comb.appendChild(el('h3', null, 'Combined — read last'));
    comb.appendChild(el('p', null, r.combined));
    if (r.conflict) {
      var n = el('p', 'note', 'Conflict kept: ' + r.conflict);
      n.style.color = 'var(--amber)';
      comb.appendChild(n);
    }
    $('sideCalls').classList.toggle('shown', state.side === 'calls');
    $('sidePuts').classList.toggle('shown', state.side === 'puts');
    $('tabCalls').setAttribute('aria-selected', String(state.side === 'calls'));
    $('tabPuts').setAttribute('aria-selected', String(state.side === 'puts'));
  }

  /* ---------- chart -------------------------------------------------------------------------------- */
  var P = { x0: 62, x1: 690, y0: 20, y1: 196, w: 720, h: 250 };
  var geo = {};

  function svgNode(tag, attrs) {
    var n = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (var k in attrs) if (attrs[k] != null) n.setAttribute(k, attrs[k]);
    return n;
  }

  /* A deterministic hand-drawn stroke: the anchors are exact, the ink between them is not.
     Used ONLY for explanatory marks — never for the price line or the axes. */
  function handPath(pts, amp, seed) {
    var rnd = (function (s) { return function () { s = (s * 16807) % 2147483647; return s / 2147483647 - 0.5; }; })(seed || 7);
    var out = [], i, j;
    for (i = 0; i < pts.length - 1; i++) {
      var a = pts[i], b = pts[i + 1];
      var dx = b[0] - a[0], dy = b[1] - a[1], len = Math.hypot(dx, dy) || 1;
      var nx = -dy / len, ny = dx / len;
      var steps = Math.max(2, Math.round(len / 14));
      for (j = (i ? 1 : 0); j <= steps; j++) {
        var t = j / steps, w = amp * rnd() * Math.sin(Math.PI * t);
        out.push([a[0] + dx * t + nx * w, a[1] + dy * t + ny * w]);
      }
    }
    var d = 'M' + out[0][0].toFixed(1) + ' ' + out[0][1].toFixed(1);
    for (i = 1; i < out.length - 1; i++) {
      var mx = (out[i][0] + out[i + 1][0]) / 2, my = (out[i][1] + out[i + 1][1]) / 2;
      d += ' Q' + out[i][0].toFixed(1) + ' ' + out[i][1].toFixed(1) + ' ' + mx.toFixed(1) + ' ' + my.toFixed(1);
    }
    var last = out[out.length - 1];
    d += ' L' + last[0].toFixed(1) + ' ' + last[1].toFixed(1);
    return d;
  }

  function renderChart() {
    var sc = scenario(), rs = sc.readings, sel = state.i, r = rs[sel];
    var svg = $('plot');
    while (svg.childNodes.length > 1) svg.removeChild(svg.lastChild);

    var spots = rs.map(function (x) { return x.spot; });
    var vals = spots.filter(function (v) { return v != null; }).concat(sc.ladder);
    var lo = Math.min.apply(null, vals), hi = Math.max.apply(null, vals);
    var pad = (hi - lo) * 0.10 || 10;
    lo -= pad; hi += pad;

    var n = rs.length;
    var X = function (i) { return n < 2 ? (P.x0 + P.x1) / 2 : P.x0 + i * (P.x1 - P.x0) / (n - 1); };
    var Y = function (v) { return P.y1 - (v - lo) / (hi - lo) * (P.y1 - P.y0); };
    geo = { X: X, Y: Y, n: n };

    var g = svgNode('g');

    /* strike levels — they double as the rupee ticks on the y axis */
    var on = r.calls.strikes.concat(r.puts.strikes);
    sc.ladder.forEach(function (k) {
      var y = Y(k), isOn = on.indexOf(k) >= 0, isLead = k === r.calls.lead || k === r.puts.lead;
      g.appendChild(svgNode('line', {
        class: 'strike' + (isOn ? ' on' : '') + (isLead ? ' lead' : ''),
        x1: P.x0, x2: P.x1, y1: y, y2: y
      }));
      var t = svgNode('text', { class: 'slab' + (isOn ? ' on' : ''), x: P.x0 - 8, y: y + 3.5, 'text-anchor': 'end' });
      t.textContent = k.toLocaleString('en-IN');
      g.appendChild(t);
      if (isLead) {
        t.setAttribute('id', 'leadLabel');
        var u = svgNode('path', {
          class: 'hand', id: 'leadUnderline', 'stroke-width': 1.5,
          d: handPath([[P.x0 - 8 - 36, y + 7], [P.x0 - 7, y + 7]], 1.4, 31)
        });
        g.appendChild(u);
      }
    });

    /* time axis */
    g.appendChild(svgNode('line', { class: 'axis', x1: P.x0, x2: P.x1, y1: P.y1 + 8, y2: P.y1 + 8 }));
    rs.forEach(function (x, i) {
      var t = svgNode('text', { class: 'tick', x: X(i), y: P.y1 + 22, 'text-anchor': 'middle' });
      t.textContent = x.at;
      t.setAttribute('opacity', i > sel ? '0.4' : '1');
      g.appendChild(t);
    });
    var ist = svgNode('text', { class: 'tick', x: P.x1, y: P.y1 + 36, 'text-anchor': 'end' });
    ist.textContent = 'IST';
    g.appendChild(ist);

    /* spot line, drawn only up to the selected reading, never across a gap */
    var segs = [], cur = [];
    for (var i = 0; i <= sel; i++) {
      if (rs[i].spot == null) {
        if (cur.length) segs.push(cur);
        cur = [];
        g.appendChild(svgNode('line', { class: 'gapmark', x1: X(i), x2: X(i), y1: P.y0, y2: P.y1 }));
        var gl = svgNode('text', { class: 'gaplab', x: X(i), y: P.y0 - 6, 'text-anchor': 'middle' });
        gl.textContent = 'no reading';
        g.appendChild(gl);
      } else {
        cur.push([X(i), Y(rs[i].spot)]);
      }
    }
    if (cur.length) segs.push(cur);
    segs.forEach(function (pts) {
      if (pts.length === 1) {
        g.appendChild(svgNode('circle', { class: 'spotdot', cx: pts[0][0], cy: pts[0][1], r: 2.4 }));
        return;
      }
      g.appendChild(svgNode('path', {
        class: 'spot',
        d: 'M' + pts.map(function (p) { return p[0].toFixed(1) + ' ' + p[1].toFixed(1); }).join(' L')
      }));
    });
    if (r.spot != null) {
      g.appendChild(svgNode('circle', { class: 'spotdot', cx: X(sel), cy: Y(r.spot), r: 3.2 }));
      var sl = svgNode('text', { class: 'slab on', x: X(sel), y: Y(r.spot) - 9, 'text-anchor': 'middle' });
      sl.textContent = '₹' + r.spot.toLocaleString('en-IN');
      g.appendChild(sl);
    }

    /* the replay trace: the stretch from first detection to the selected reading */
    var fi = firstIndex();
    if (fi != null && fi < sel) {
      var tp = [];
      for (var k2 = fi; k2 <= sel; k2++) if (rs[k2].spot != null) tp.push([X(k2), Y(rs[k2].spot)]);
      if (tp.length > 1) {
        g.appendChild(svgNode('path', {
          class: 'trace', id: 'trace', opacity: '0',
          d: 'M' + tp.map(function (p) { return p[0].toFixed(1) + ' ' + p[1].toFixed(1); }).join(' L')
        }));
      }
    }

    /* the hand-drawn bracket around the participating cluster */
    if (on.length) {
      var ks = on.slice().sort(function (a, b) { return a - b; });
      var yTop = Y(ks[ks.length - 1]) - 7, yBot = Y(ks[0]) + 7, xb = P.x1 - 4;
      g.appendChild(svgNode('path', {
        class: 'hand', id: 'bracket', opacity: '0',
        d: handPath([[xb - 10, yTop], [xb, yTop], [xb, yBot], [xb - 10, yBot]], 1.7, 13)
      }));
      var note = svgNode('text', {
        class: 'handnote', id: 'bracketNote', x: xb - 14, y: (yTop + yBot) / 2 + 3.5,
        'text-anchor': 'end', opacity: '0'
      });
      note.textContent = ks.length + (ks.length === 1 ? ' strike' : ' strikes');
      g.appendChild(note);
    }

    g.appendChild(svgNode('circle', { class: 'travel', id: 'travel', r: 3.6, opacity: '0', cx: -20, cy: -20 }));
    svg.appendChild(g);

    $('chartSub').textContent = sc.instrument + ' spot · ' + rs[0].at + '–' + r.at + ' IST'
      + (sel < n - 1 ? ' · later readings not drawn' : '');
  }

  function firstIndex() {
    var rs = scenario().readings, f = rs[state.i].calls.firstAt;
    if (!f) return null;
    for (var i = 0; i < rs.length; i++) if (rs[i].at === f) return i;
    return null;
  }

  /* ---------- strike participation ---------------------------------------------------------------- */
  function renderPartic() {
    var sc = scenario(), rs = sc.readings, sel = state.i, r = rs[sel];
    var box = $('particRows');
    box.textContent = '';
    sc.ladder.forEach(function (k) {
      var row = el('div', 'prow');
      var isLead = k === r.calls.lead || k === r.puts.lead;
      row.appendChild(el('span', 'k num' + (isLead ? ' lead' : ''), k.toLocaleString('en-IN')));
      var cells = el('div', 'pcells');
      rs.forEach(function (x, i) {
        var c = el('span', 'pcell');
        // A READING AFTER THE ONE SELECTED IS NOT DRAWN AT ALL. Dimming its real state would still
        // show the reader something the session did not know yet.
        if (i > sel) {
          c.className += ' future';
          c.title = x.at + ' IST · not read yet';
        } else {
          if (x.taken === false) c.className += ' gap';
          else {
            var inC = x.calls.strikes.indexOf(k) >= 0, inP = x.puts.strikes.indexOf(k) >= 0;
            if (inC) c.className += ' on' + (k === x.calls.lead ? ' lead' : '');
            else if (inP) c.className += ' put on';
          }
          c.title = x.at + ' IST';
        }
        cells.appendChild(c);
      });
      row.appendChild(cells);
      var num = (r.evidence.numbers || []).filter(function (x) { return x.strike === k; })[0];
      var v = el('span', 'v num' + (num ? ' on' : ''), num ? num.doi : '—');
      v.title = num ? 'Open-interest change at ' + r.at + ' IST' : 'No material change at this reading';
      row.appendChild(v);
      box.appendChild(row);
    });
    $('particLegend').textContent =
      'Filled = participated (mint calls, blue puts) · faded = after the selected reading · '
      + 'hatched = none captured · right column = open-interest change at ' + r.at + ' IST.';
  }

  /* ---------- timeline ------------------------------------------------------------------------------ */
  function renderTimeline() {
    var rs = scenario().readings, box = $('tl');
    box.textContent = '';
    rs.forEach(function (x, i) {
      var b = el('button', 'tlitem' + (i > state.i ? ' future' : '') + (x.taken === false ? ' gapev' : ''));
      b.type = 'button';
      b.setAttribute('aria-current', String(i === state.i));
      b.appendChild(el('span', 't num', x.at + ' IST'));
      // A LATER READING KEEPS ITS TIME AND LOSES ITS DESCRIPTION. Printing "a third strike joins"
      // beside 10:00 while the reader is on 09:45 is showing them something the session had not
      // observed yet. It stays clickable — moving forward is the reader's choice, not a reveal.
      b.appendChild(el('span', 'd', i > state.i ? 'not read yet' : x.timelineTitle));
      b.addEventListener('click', function () { select(i); });
      box.appendChild(b);
    });
    applyTimelineWindow();
  }

  /* Mobile: the selected reading and the two before it, with the whole day one tap away. */
  function applyTimelineWindow() {
    var items = [].slice.call($('tl').children), more = $('tlMore');
    var from = Math.max(0, state.i - 2);
    var hiding = !state.tlAll && from > 0;
    items.forEach(function (b, i) {
      b.classList.toggle('hidden-m', !state.tlAll && (i < from || i > state.i));
    });
    more.hidden = !(hiding || state.tlAll || state.i < items.length - 1);
    more.textContent = state.tlAll
      ? 'Show only the latest readings'
      : 'Show the full day · ' + items.length + ' readings';
  }

  /* ---------- evidence ------------------------------------------------------------------------------- */
  function renderEvidence() {
    var r = reading(), e = r.evidence, body = $('evBody');
    $('evHint').textContent = r.at + ' IST · ' + (e.numbers.length
      ? e.numbers.length + ' measured contract' + (e.numbers.length === 1 ? '' : 's')
      : 'nothing measurable at this reading');
    body.textContent = '';

    var f = el('div', 'evblock');
    f.appendChild(el('h4', null, 'Facts observed'));
    var dl = el('dl', 'facts');
    e.facts.forEach(function (p) { dl.appendChild(el('dt', null, p[0])); dl.appendChild(el('dd', 'num', p[1])); });
    f.appendChild(dl);
    body.appendChild(f);

    var inf = el('div', 'evblock');
    inf.appendChild(el('h4', null, 'Inference'));
    inf.appendChild(el('p', null, e.inference));
    body.appendChild(inf);

    var ba = el('div', 'evblock');
    ba.appendChild(el('h4', null, 'Comparison baseline'));
    ba.appendChild(el('p', null, e.baseline));
    body.appendChild(ba);

    var un = el('div', 'evblock');
    un.appendChild(el('h4', null, 'Uncertainty'));
    var ul = el('ul');
    e.uncertainty.forEach(function (u) { ul.appendChild(el('li', null, u)); });
    un.appendChild(ul);
    body.appendChild(un);

    var nb = el('div', 'evblock evfull');
    nb.appendChild(el('h4', null, 'Supporting numbers · ' + r.at + ' IST'));
    if (!e.numbers.length) {
      nb.appendChild(el('p', null, 'No contract carried a measurable change at this reading.'));
    } else {
      nb.appendChild(numbersTable(e.numbers, r.calls.lead));
      if (e.quiet) nb.appendChild(el('p', 'quiet', e.quiet));
    }
    body.appendChild(nb);
    body.hidden = !state.ev;
    $('evToggle').setAttribute('aria-expanded', String(state.ev));
  }

  function numbersTable(rows, lead) {
    var t = el('table', 'nums');
    var head = el('tr');
    ['Contract', 'ΔOI', 'ΔOI %', 'Premium', 'Δ premium', 'IV', 'Δ IV'].forEach(function (h) {
      head.appendChild(el('th', null, h));
    });
    t.appendChild(el('thead')).appendChild(head);
    var tb = el('tbody');
    rows.forEach(function (x) {
      var tr = el('tr', x.strike === lead ? 'lead' : '');
      tr.appendChild(el('td', 'num', x.strike.toLocaleString('en-IN') + ' ' + x.side));
      [x.doi, x.doiPct, x.price, x.priceChg, x.iv, x.ivChg].forEach(function (v) {
        var cls = 'num ' + (v === '—' ? 'na' : /^\+/.test(v) ? 'pos' : /^[-−]/.test(v) ? 'neg' : '');
        tr.appendChild(el('td', cls, v));
      });
      tb.appendChild(tr);
    });
    t.appendChild(tb);
    return t;
  }

  /* ---------- drawer: the one implemented inspect tool ----------------------------------------------- */
  function openDrawer() {
    var sc = scenario(), r = reading();
    $('drawerSub').textContent = sc.instrument + ' · ' + sc.expiry + ' · ' + r.at + ' IST · illustrative';
    var body = $('drawerBody');
    body.textContent = '';
    body.appendChild(el('p', 'quiet',
      'Every strike this preview tracks, at the reading selected above. A strike with no row in the '
      + 'evidence is shown as no material change rather than as a zero.'));
    var byStrike = {};
    (r.evidence.numbers || []).forEach(function (x) { byStrike[x.strike] = x; });
    var t = el('table', 'nums');
    var head = el('tr');
    ['Strike', 'Side', 'ΔOI', 'ΔOI %', 'Premium', 'Δ premium', 'IV'].forEach(function (h) { head.appendChild(el('th', null, h)); });
    t.appendChild(el('thead')).appendChild(head);
    var tb = el('tbody');
    sc.ladder.slice().sort(function (a, b) { return b - a; }).forEach(function (k) {
      var x = byStrike[k], tr = el('tr', x && k === r.calls.lead ? 'lead' : '');
      tr.appendChild(el('td', 'num', k.toLocaleString('en-IN')));
      tr.appendChild(el('td', null, x ? x.side : '—'));
      if (x) {
        [x.doi, x.doiPct, x.price, x.priceChg, x.iv].forEach(function (v) {
          tr.appendChild(el('td', 'num ' + (v === '—' ? 'na' : /^\+/.test(v) ? 'pos' : /^[-−]/.test(v) ? 'neg' : ''), v));
        });
      } else {
        var td = el('td', 'na', 'no material change at this reading');
        td.colSpan = 5;
        tr.appendChild(td);
      }
      tb.appendChild(tr);
    });
    t.appendChild(tb);
    body.appendChild(t);
    $('scrim').hidden = false;
    $('drawer').hidden = false;
    $('drawerClose').focus();
  }
  function closeDrawer() {
    $('drawer').hidden = true;
    $('scrim').hidden = true;
    $('toolChain').focus();
  }

  /* ---------- replay ----------------------------------------------------------------------------------- */
  function stop() {
    anims.forEach(function (a) { try { a.cancel(); } catch (e) { } });
    anims = [];
    timers.forEach(clearTimeout); timers = [];
    if (rafId) cancelAnimationFrame(rafId); rafId = 0;
    document.documentElement.setAttribute('data-replay', 'off');
    [].forEach.call(document.querySelectorAll('.wg'), function (w) { w.classList.remove('lit', 'cur'); });
    settle();
    $('skip').hidden = true;
    $('replay').hidden = false;
  }

  /* the end state of every animated mark, applied directly when motion is off or replay is skipped */
  function settle() {
    ['trace', 'bracket', 'bracketNote'].forEach(function (id) {
      var n = $(id);
      if (!n) return;
      n.setAttribute('opacity', id === 'trace' ? '0' : '1');
      n.style.strokeDasharray = '';
      n.style.strokeDashoffset = '';
    });
    var u = $('leadUnderline');
    if (u) { u.style.strokeDasharray = ''; u.style.strokeDashoffset = ''; u.setAttribute('opacity', '1'); }
    var t = $('travel');
    if (t) t.setAttribute('opacity', '0');
  }

  function announce() {
    var r = reading();
    $('liveRegion').textContent = r.headline + '. ' + r.sentences.join(' ') + ' ' + r.persistence + '.';
  }

  function replay() {
    stop();
    announce();
    if (quiet()) return;

    document.documentElement.setAttribute('data-replay', 'on');
    $('replay').hidden = true;
    $('skip').hidden = false;

    var wgs = [].slice.call(document.querySelectorAll('#lede1 .wg, #lede2 .wg'));
    var per = Math.min(150, 1300 / Math.max(1, wgs.length));
    wgs.forEach(function (w, i) {
      timers.push(setTimeout(function () {
        w.classList.add('lit', 'cur');
        timers.push(setTimeout(function () { w.classList.remove('cur'); }, per * 2.2));
      }, i * per));
    });
    var wordsEnd = wgs.length * per + 120;

    timers.push(setTimeout(function () {
      document.documentElement.setAttribute('data-replay', 'off');
      traceSegment(function () { drawMarks(finish); });
    }, wordsEnd));

    function finish() { $('skip').hidden = true; $('replay').hidden = false; }
  }

  function traceSegment(next) {
    var p = $('trace');
    if (!p) { next(); return; }
    var len = p.getTotalLength();
    p.setAttribute('opacity', '1');
    p.style.strokeDasharray = len;
    p.style.strokeDashoffset = len;
    var dur = 850, t0 = performance.now(), dot = $('travel');
    if (dot) dot.setAttribute('opacity', '1');
    function step(now) {
      var k = Math.min(1, (now - t0) / dur);
      p.style.strokeDashoffset = String(len * (1 - k));
      if (dot) {
        var pt = p.getPointAtLength(len * k);
        dot.setAttribute('cx', pt.x); dot.setAttribute('cy', pt.y);
      }
      if (k < 1) { rafId = requestAnimationFrame(step); }
      else {
        rafId = 0;
        if (dot) anims.push(dot.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 280, fill: 'forwards' }));
        timers.push(setTimeout(function () {
          anims.push(p.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 420, fill: 'forwards' }));
        }, 500));
        next();
      }
    }
    rafId = requestAnimationFrame(step);
  }

  function drawMarks(next) {
    var br = $('bracket'), note = $('bracketNote'), und = $('leadUnderline');
    [br, und].forEach(function (p) {
      if (!p) return;
      var len = p.getTotalLength();
      p.setAttribute('opacity', '1');
      p.style.strokeDasharray = len;
      p.style.strokeDashoffset = len;
      anims.push(p.animate([{ strokeDashoffset: len }, { strokeDashoffset: 0 }],
        { duration: 560, easing: 'cubic-bezier(.25,.8,.4,1)', fill: 'forwards' }));
    });
    if (note) anims.push(note.animate([{ opacity: 0 }, { opacity: 1 }],
      { duration: 300, delay: 360, fill: 'forwards' }));
    timers.push(setTimeout(next, 620));
  }

  /* ---------- reading change --------------------------------------------------------------------------- */
  function select(i) {
    if (i === state.i) return;
    stop();
    state.i = i;
    render(false);
  }

  function fade(nodes) {
    nodes.forEach(function (n) {
      if (!n) return;
      n.animate([{ opacity: 0.25 }, { opacity: 1 }], { duration: quiet() ? 1 : 200, easing: 'ease-out' });
    });
  }

  function render(full) {
    renderContext();
    renderStory();
    renderSides();
    renderChart();
    renderPartic();
    renderTimeline();
    renderEvidence();
    settle();
    fade([document.querySelector('.story'), $('chart'), document.querySelector('.sides')]);
    if (!$('drawer').hidden) openDrawer();
    if (full) window.scrollTo({ top: 0, behavior: quiet() ? 'auto' : 'smooth' });
  }

  /* ---------- wiring -------------------------------------------------------------------------------------- */
  function wire() {
    $('replay').addEventListener('click', replay);
    $('skip').addEventListener('click', function () { stop(); announce(); });

    $('quiet').addEventListener('click', function () {
      var on = document.documentElement.getAttribute('data-quiet') === 'on';
      document.documentElement.setAttribute('data-quiet', on ? 'off' : 'on');
      this.setAttribute('aria-pressed', String(!on));
      stop();
    });

    $('evToggle').addEventListener('click', function () {
      state.ev = !state.ev;
      $('evBody').hidden = !state.ev;
      this.setAttribute('aria-expanded', String(state.ev));
    });
    $('jumpEvidence').addEventListener('click', function () {
      state.ev = true;
      $('evBody').hidden = false;
      $('evToggle').setAttribute('aria-expanded', 'true');
      $('evidence').scrollIntoView({ behavior: quiet() ? 'auto' : 'smooth', block: 'start' });
      $('evToggle').focus();
    });

    $('tlMore').addEventListener('click', function () {
      state.tlAll = !state.tlAll;
      applyTimelineWindow();
    });

    $('tabCalls').addEventListener('click', function () { state.side = 'calls'; renderSides(); });
    $('tabPuts').addEventListener('click', function () { state.side = 'puts'; renderSides(); });

    $('toolChain').addEventListener('click', openDrawer);
    $('drawerClose').addEventListener('click', closeDrawer);
    $('scrim').addEventListener('click', closeDrawer);
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && !$('drawer').hidden) closeDrawer();
      else if (e.key === 'Escape') stop();
    });

    [].forEach.call(document.querySelectorAll('[data-jump]'), function (b) {
      b.addEventListener('click', function () {
        var t = $(b.getAttribute('data-jump'));
        if (t) t.scrollIntoView({ behavior: quiet() ? 'auto' : 'smooth', block: 'start' });
      });
    });

    var secs = ['story', 'sides', 'chart', 'timeline', 'evidence'];
    if (window.IntersectionObserver) {
      var io = new IntersectionObserver(function (entries) {
        entries.forEach(function (en) {
          if (!en.isIntersecting) return;
          [].forEach.call(document.querySelectorAll('[data-jump]'), function (b) {
            b.setAttribute('aria-current', String(b.getAttribute('data-jump') === en.target.id));
          });
        });
      }, { rootMargin: '-45% 0px -50% 0px' });
      secs.forEach(function (id) { if ($(id)) io.observe($(id)); });
    }

    window.addEventListener('resize', applyTimelineWindow);
    window.matchMedia('(prefers-reduced-motion: reduce)').addEventListener('change', stop);
  }

  buildScenarios();
  wire();
  render(false);
})();
