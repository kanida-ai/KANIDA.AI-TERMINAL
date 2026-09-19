// Discover Strategies · main navigation · admin strategy registry — browser acceptance (docs/FALCON_DISCOVER_SPEC.md §1, §3, §4, §6).
// Runs ONLY against the local UI-QA server (scripts/qa_server.py, port 8083, isolated var/ui-qa/qa.sqlite3, fictional fixture accounts).
// Member session: created through the owner fixture's own API (invite → register → onboarding → access) inside the isolated QA DB; qa_server.py is not changed.
// Usage (from kanida-app):  node scripts/check-discover.e2e.cjs            all viewports + admin
//                           node scripts/check-discover.e2e.cjs --only=desktop,phone [--no-admin]
// Artifacts: qa/discover/*.png, qa/discover/results.json. Exit 1 if any check FAILs.
// PARK = a check waiting on an owner decision (see PARKED below): reported, never counted as a failure.
const fs=require('node:fs'),path=require('node:path');
const {chromium}=require('@playwright/test');
const BASE=(process.env.QA_BASE||'http://127.0.0.1:8083').replace(/\/+$/,'');
const QA_PORT=(BASE.match(/:(\d+)\/?$/)||[])[1];
// The guard exists to keep these destructive suites (they create accounts, invites and admin changes) away
// from the REAL pilot - not to pin one port. Loopback, and never the pilot's own 8082.
if(!/^http:\/\/127\.0\.0\.1:\d+\/?$/.test(BASE)||QA_PORT==='8082'){console.error('Refusing to run: QA_BASE must be a local UI-QA server (127.0.0.1, not the pilot on 8082). Default: http://127.0.0.1:8083');process.exit(2);}
const ROOT=path.join(__dirname,'..'),OUT=path.join(ROOT,'qa','discover');fs.mkdirSync(OUT,{recursive:true});
// WHAT WAS RUN, recorded with every result. Two of us changed the bundle and this script at the same time
// without recording either, and a script change read as a bundle regression. Never again: the summary and
// results.json both carry the script's hash and the exported bundle's timestamp.
const SCRIPT_SHA=require('node:crypto').createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0,12);
const WEB_DIR=process.env.QA_WEB_DIRECTORY||path.join(ROOT,'dist-pilot');
const BUNDLE=(()=>{try{return fs.statSync(path.join(WEB_DIR,'index.html')).mtime.toISOString();}catch{return 'unknown';}})();
const PROVENANCE=`script ${SCRIPT_SHA} · bundle ${BUNDLE} (${path.relative(ROOT,WEB_DIR)||WEB_DIR})`;
const fixture=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(fixture.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(fixture.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const MEMBER={email:'qa-member@example.invalid',password:'Local-QA-member-fixture-2026',name:'QA member'};
const POLICY='private-pilot-v1';
const QA_KEY='qa-falling_wedge-1D-long',QA_NAME='QA Falling Wedge breakout · 1D';
// ---------------- pattern-set mode ----------------
// The scanner runs one of two detector sets and the page is DIFFERENT in each, so the suite asserts the
// mode-appropriate behaviour rather than one mode's behaviour twice:
//   legacy   - the stored 10-pattern scan. The "Chart Strategies" block leads the page and its cards list
//              stocks with a 95% low, an average and a trade count. Exactly today's suite.
//   research - the researched 107-pattern set. The stored block has no current matches (its 10 detectors
//              are a subset of the 107), so it is collapsed behind one line and the "Chart patterns" block
//              leads, listing TODAY'S DETECTIONS with a lifecycle state and a detection time.
// Every structural check below runs in both modes against the block that leads the page; the mode-specific
// content assertions are in the profile, and section 15 asserts the research-only behaviour on top.
const MODES={
 legacy:{mode:'legacy',block:'Chart Strategies',blockKey:'chart',
  a:'Falling Wedge breakout · 1D',b:'Channel breakout · 1D',alt:'Channel breakout · 1H',
  keyA:'falling_wedge-1D-long',keyB:'channel-1D-long',
  loadingRe:/^Loading stored scan/,readyKind:'status',readySource:'Stored( scan)? · ',rowKind:'stored',
  listLabel:'stored scan',minBlockStrategies:52,sortLow:'95% low',sortN:'n',
  statusRe:/^Stored(?: scan)? · (.+?) · (?:data )?to (.+?) · (\d+) found$/},
 research:{mode:'research',block:'Chart patterns',blockKey:'chart_patterns',
  a:'Falling Wedge · 1D',b:'Rising Wedge · 1D',alt:'Falling Wedge · 1H',
  keyA:'ch05-legacy_1.0.1-long-1d',keyB:'ch06-legacy_1.0.1-short-1d',
  // "The card says it is loading", not one phrasing of it. An error, an empty list and a ready list all fail
  // this, so it cannot pass vacuously; a reworded loading line does not break it. (Both of a research card's
  // sources use it: "Loading setups…" and "Loading history…".)
  loadingRe:/^Loading\b/,readyKind:'liveTab',readySource:null,rowKind:'live',
  listLabel:'active setups',minBlockStrategies:45*4,
  // The research card's ONE count lives in its "Now" tab, and its status line is EMPTY once the list is
  // complete (cardLogic.liveStatusText) - so the status line cannot be the readiness signal. `cutRe` is
  // asserted on its own, as a statement about the cut, never as the ready gate.
  sortLow:'Edge low',sortN:'Trades',statusRe:null,cutRe:/^Showing ([\d,]+) of ([\d,]+)$/},
};
let P=MODES.legacy;  // replaced once the catalog has been read
for(const m of Object.values(MODES)){m.driveA=m.a;m.driveKeyA=m.keyA;m.driveB=m.b;m.driveKeyB=m.keyB;}
const HOLD_LABEL='Pattern hold-period history — not a tested exit rule',TESTED_LABEL='Tested exit rule · later-test (out-of-sample)';
const STORED_BASIS=[HOLD_LABEL,TESTED_LABEL];
// EVIDENCE_SERVING_CONTRACT.md §6, verbatim, plus the pending state. One of these must always be on the card.
const RESEARCH_BASIS=['Historical walk-forward result','Limited historical sample','No selected walk-forward trades',
 'No occurrences in this historical sample','Not enough historical data','Incompatible historical evidence',
 'Historical data requires review','Evidence loading'];
const ARGS=process.argv.slice(2),ONLY=(ARGS.find(a=>a.startsWith('--only='))||'').slice(7).split(',').filter(Boolean),NO_ADMIN=ARGS.includes('--no-admin');
const VIEWPORTS=[
 {key:'desktop',width:1440,height:900,mode:'four'},
 {key:'short',width:1366,height:600,mode:'four'},
 {key:'tablet',width:1024,height:768,mode:'two'},
 {key:'phone',width:390,height:844,mode:'stack',isMobile:true,hasTouch:true},
].filter(v=>!ONLY.length||ONLY.includes(v.key));
// Five items since the Derivative tab was added next to Discover Strategies (src/shell/routes.tsx NAV_ITEMS).
const NAV=['Falcon','Discover Strategies','Derivative','Watchlist','AutoTrade'],NAV_ROUTE={'Falcon':'/','Discover Strategies':'/discover','Derivative':'/derivative','Watchlist':'/watch','AutoTrade':'/autotrade'};

const results=[],consoleLog={},extra={};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const short=v=>{try{return JSON.stringify(v).slice(0,900)}catch{return String(v)}};
async function until(fn,{timeout=15000,interval=120,msg='condition'}={}){
 const end=Date.now()+timeout;let last;
 // Strict: when the result carries `ok`, only a truthy ok passes (null/undefined from `a&&b` short-circuits never counts as success).
 for(;;){try{last=await fn();if(last&&(!('ok' in last)||!!last.ok))return last;}catch(e){last={error:e.message.split('\n')[0]};}
  if(Date.now()>end)throw new Error(`${msg} not met within ${timeout}ms; last=${short(last)}`);await sleep(interval);}
}
function withTimeout(p,ms,label){let t;return Promise.race([p,new Promise((_,rej)=>{t=setTimeout(()=>rej(new Error(`${label} timed out after ${ms}ms`)),ms)})]).finally(()=>clearTimeout(t));}
// ---------------- parked checks ----------------
// These assert behaviour that BACKLOG item 2 ("Make the Discover page simple") removed, and item 2 is still
// 👀 WAITING FOR THE OWNER'S REVIEW. They are therefore neither passing nor broken: they are PARKED on a
// decision that has not been taken. They do not fail the run, and they are not deleted — if the owner rejects
// item 2, these are the coverage that catches the old behaviour not coming back properly.
// Signing item 2 off means retiring 15.1/15.2/15.5 and retargeting 13.5/13.6 at a research strategy.
const PARK_ITEM='BACKLOG item 2 (Make the Discover page simple) — 👀 awaiting the owner\'s review';
const PARKED={
 '15.1':'item 2: "The old 10-pattern block is hidden, not shown with an explanation" — the server drops the stored block, so there is no line to expand.',
 '15.2':'item 2: the stored block is hidden, so there are no expanded stored cards to state a reason.',
 '15.5':'item 2: "No \'Spotted today\' chip" — the chip was removed; both counts live on the Now / History tabs.',
 '13.5':'item 2: the stored block is hidden on the research set, so an enabled STORED strategy has no block to appear in. Retarget at a research strategy once item 2 is signed off.',
 '13.6':'item 2: same as 13.5 — nothing to disappear from while the stored block is hidden.',
};
// ---------------- overlay hygiene ----------------
// ANY check can throw mid-interaction — after opening the strategy picker, a card or block menu, the Filters
// sheet, the ⓘ popover, an admin dialog — and an overlay left up blocks every click after it. That is how one
// missing menu item in 8.1 turned into six unrelated reds in 8.2-10.2, and sent the next reader hunting six
// ghosts. So every check STARTS from a page with nothing modal open, and a check that throws dismisses on its
// way out (after its screenshot, so the overlay is still visible in the evidence).
// This helper never throws and never fails a check: it is cleanup, not an assertion. On a page with nothing
// open it does nothing. The checks that legitimately find the picker already open all reopen it if it is not.
async function dismissOverlays(page){
 const OPEN='[role=dialog],[role=menu],[aria-modal="true"]';
 try{
  for(let i=0;i<3;i++){
   if(!await page.locator(OPEN).filter({visible:true}).count())return;
   await page.keyboard.press('Escape').catch(()=>{});await sleep(180);
  }
  // Still up: something that closes on an outside press. One click in the top-left gutter, clear of the cards.
  if(await page.locator(OPEN).filter({visible:true}).count()){await page.mouse.click(3,3).catch(()=>{});await sleep(180);}
 }catch{}
}
async function check(id,vp,name,page,fn,timeout=90000){
 const t0=Date.now();
 await dismissOverlays(page);
 try{const detail=await withTimeout(Promise.resolve().then(fn),timeout,id);const info=detail&&detail.info;
  // A PARKED check that PASSES is itself news: the behaviour is back, so the park is stale. Never silent.
  if(PARKED[id]){results.push({id,vp,name,status:'PASS',parkedButPassing:true,detail:short(info||detail||''),ms:Date.now()-t0});
   console.log(`PASS ${vp} ${id} ${name}  <-- was parked on ${PARK_ITEM}; it passes now, so un-park it`);return detail;}
  results.push({id,vp,name,status:info?'INFO':'PASS',detail:short(info||detail||''),ms:Date.now()-t0});console.log(`${info?'INFO':'PASS'} ${vp} ${id} ${name}`);return detail;}
 catch(e){const first=String(e.message).split('\n')[0];
  if(PARKED[id]){await dismissOverlays(page);
   results.push({id,vp,name,status:'PARK',detail:PARKED[id],error:first.slice(0,400),ms:Date.now()-t0});
   console.log(`PARK ${vp} ${id} ${name}\n      -> parked, not broken: ${PARKED[id]}`);return null;}
  const file=path.join(OUT,`${vp}-FAIL-${id.replace(/[^\w.-]/g,'_')}.png`);try{await page.screenshot({path:file});}catch{}
  await dismissOverlays(page);   // after the screenshot, so the evidence still shows what was open
  results.push({id,vp,name,status:'FAIL',detail:String(e.message).split('\n').slice(0,3).join(' | ').slice(0,1200),shot:path.relative(ROOT,file),ms:Date.now()-t0});console.log(`FAIL ${vp} ${id} ${name}: ${first.slice(0,500)}`);return null;}
}
async function shot(page,vp,name,full=false){const file=path.join(OUT,`${vp}-${name}.png`);try{await page.screenshot({path:file,fullPage:full});return path.relative(ROOT,file);}catch(e){return `screenshot failed: ${e.message.split('\n')[0]}`;}}

// ---------------- in-page probes (installed before app scripts on every document) ----------------
function installProbes(cfg){
 const READY_RE=cfg.readySource?new RegExp('^(?:'+cfg.readySource+')'):null,LIVE=cfg.rowKind==='live';
 // READINESS. Two conditions, both required on the research set: the card's own count is positive AND a
 // setup row is actually rendered. The status line is deliberately NOT part of it - it is legitimately empty
 // once the list is complete, which is what made the old "N active" regex go stale. Requiring a rendered row
 // is what stops a ready-looking-but-empty card passing.
 const readyOf=s=>!!s&&s.count>=1&&(cfg.readyKind==='liveTab'?Number.isFinite(s.live)&&s.live>=1:READY_RE.test(s.status||''));
 const vis=e=>!!e&&e.getClientRects().length>0;
 const R=r=>({x:Math.round(r.left),y:Math.round(r.top),w:Math.round(r.width),h:Math.round(r.height)});
 const regions=()=>Array.from(document.querySelectorAll('[role=region][aria-label]')).filter(vis);
 const byLabel=test=>regions().filter(r=>test(r.getAttribute('aria-label')||''));
 const pctNum=t=>{if(!t)return null;const v=parseFloat(t.replace(/[−–]/g,'-').replace(/[+%\s]/g,''));return Number.isFinite(v)?v:null;};
 const lead=()=>byLabel(l=>l===cfg.block)[0]||document;
 const scanner=(slot,light)=>{const root=lead();
  const el=Array.from(root.querySelectorAll('[role=region][aria-label]')).filter(vis)
   .filter(e=>{const l=e.getAttribute('aria-label')||'';return l===`Scanner ${slot}`||l.startsWith(`Scanner ${slot}, `)})[0];if(!el)return null;
  const title=el.querySelector(`[aria-label^="Scanner ${slot}:"]`),st=el.querySelector('[aria-live="polite"]');
  const opts=Array.from(el.querySelectorAll('[role=option]'));
  // The card's own count now lives in its list tabs. The LIVE tab is identified by what its accessible name
  // COUNTS ("…, 120 setups"; the other tab counts stocks), read from the DOM, so the tab's visible wording
  // can change without this probe going stale.
  const tabs=Array.from(el.querySelectorAll('[role=tab]')).filter(vis)
   .map(t=>({label:t.getAttribute('aria-label')||'',text:t.innerText.trim(),selected:t.getAttribute('aria-selected')==='true'}));
  const liveTab=tabs.find(t=>/\d[\d,]*\s+setups?\b/i.test(t.label))||null;
  const live=liveTab?+((liveTab.label.match(/(\d[\d,]*)\s+setups?\b/)||[])[1]||'').replace(/,/g,''):null;
  if(light)return {count:opts.length,rows:opts.length,status:st?st.textContent.trim():null,live};
  // A stored row carries its evidence columns; a LIVE detection row carries a lifecycle state and the time
  // it was detected. Both are parsed here so every selection/keyboard check reads the same shape.
  // A LIVE row must name a lifecycle state AND say when the setup was detected. The state words are a closed
  // vocabulary (cardLogic.detectionStateLabel) - they ARE the meaning - but the "when" is matched by SHAPE,
  // a date or an age in candles, not by one phrasing, so the detail line can be reworded without this going
  // stale. A row carrying neither still fails. (Today: "Forming · since 16 Sep · new · 2 candles ago".)
  const STATE=/\.\s+(Forming|Confirmed|Invalidated|Expired)\b/;
  const WHEN=/since\s+\d{1,2}\s+\w{3,}|\d+\s+candles?\s+ago|latest candle/i;
  const rows=opts.map(o=>{const l=o.getAttribute('aria-label')||'';
   // "…low X, average Y after costs, N <qualifier?> trades" — the qualifier ("tested", "walk-forward",
   // "out-of-sample", none) names which trades they are and is allowed to change; the three NUMBERS are the
   // meaning and are still required.
   const m=l.match(/(?:95% low|Edge low) ([^,]+), (?:[a-z-]+ )?average ([^,]+) after costs, (\d+)(?: [a-z-]+)? trades/);
   const st=l.match(STATE),wh=l.match(WHEN);
   return {sym:l.split(',')[0],sel:o.getAttribute('aria-selected')==='true',low:m?pctNum(m[1]):null,lowText:m?m[1]:null,n:m?+m[3]:null,
    state:st?st[1]:null,detected:wh?wh[0].trim():null,tf:(l.match(/^[^ ,]+ (1H|4H|1D|1W) /)||[])[1]||null,focused:document.activeElement===o};});
  return {label:el.getAttribute('aria-label'),rect:R(el.getBoundingClientRect()),title:title?title.getAttribute('aria-label'):null,status:st?st.innerText.trim():null,rows,count:rows.length,live,tabs,liveTabText:liveTab?liveTab.text:null,text:el.innerText.slice(0,700)};};
 // On a phone the leading block renders only its two scanners; its chart + backtest pair is the open bottom
 // sheet, which is an overlay beside the page's ScrollView and therefore NOT inside the block's own region.
 // While that sheet is open it IS the pair being asserted on, so it is where the two cards are read from.
 const card=(exact,prefix)=>{const root=byLabel(l=>l.startsWith('Chart and backtest'))[0]||lead();
  const el=Array.from(root.querySelectorAll('[role=region][aria-label]')).filter(vis)
   .filter(e=>{const l=e.getAttribute('aria-label')||'';return l===exact||l.startsWith(prefix)})[0];if(!el)return null;const h=el.querySelector('[role=heading]');
  const svgs=Array.from(el.querySelectorAll('svg')).filter(vis).map(s=>R(s.getBoundingClientRect()));const canvas=el.querySelector('[aria-label*=" candles, NSE"]');
  return {label:el.getAttribute('aria-label'),rect:R(el.getBoundingClientRect()),heading:h?h.innerText.trim():null,text:el.innerText.slice(0,12000),svgMax:svgs.reduce((a,b)=>b.w*b.h>a.w*a.h?b:a,{w:0,h:0}),canvasLabel:canvas?canvas.getAttribute('aria-label').slice(0,160):null};};
 window.__qaDisc=()=>{const sheet=byLabel(l=>l.startsWith('Chart and backtest'))[0],block=byLabel(l=>l===cfg.block)[0];
  return {path:location.pathname,search:location.search,title:document.title,A:scanner('A'),B:scanner('B'),chart:card('Chart','Chart, '),bt:card('Backtest','Backtest · '),sheet:sheet?sheet.getAttribute('aria-label'):null,block:block?R(block.getBoundingClientRect()):null,active:document.activeElement?(document.activeElement.getAttribute('aria-label')||document.activeElement.tagName):null};};
 window.__qaNav=()=>Array.from(document.querySelectorAll('[role=navigation][aria-label="Main navigation"]')).filter(vis).map(n=>({rect:R(n.getBoundingClientRect()),
  items:Array.from(n.querySelectorAll('[role=link],a')).filter(vis).map(a=>({label:a.getAttribute('aria-label'),current:a.getAttribute('aria-current'),text:a.innerText.trim(),rect:R(a.getBoundingClientRect())}))}));
 // Reveal/loading timeline for scanner A and B: one entry per change, sampled every animation frame.
 window.__qaRevStart=(ms=60000)=>{const t0=performance.now(),out=window.__qaRev=[];let last='',doneAt=0;
  const loop=()=>{const a=scanner('A',true),b=scanner('B',true);const s={a:a?a.rows:-1,b:b?b.rows:-1,sa:a?a.status:null,sb:b?b.status:null,la:a?a.live:null,lb:b?b.live:null};const k=JSON.stringify(s);const now=performance.now();
   if(k!==last){last=k;out.push({t:Math.round(now-t0),...s});}
   const done=readyOf(a)&&readyOf(b);if(done&&!doneAt)doneAt=now;if(!done)doneAt=0;
   if(now-t0<ms&&!(doneAt&&now-doneAt>1000))requestAnimationFrame(loop);else window.__qaRevDone=true;};
  window.__qaRevDone=false;requestAnimationFrame(loop);};
 if(location.pathname==='/discover')window.__qaRevStart(60000);
}
const disc=page=>page.evaluate(()=>window.__qaDisc());
const navState=page=>page.evaluate(()=>window.__qaNav());
// Same two-condition readiness as the in-page probe, for the Node-side waits. The status line is never part
// of it on the research set: it is legitimately empty once the list is complete.
const READY=()=>new RegExp('^(?:'+P.readySource+')');
const rowCount=s=>s?(typeof s.count==='number'?s.count:(s.rows||[]).length):0;
const isReady=s=>!!s&&rowCount(s)>=1&&(P.readyKind==='liveTab'?Number.isFinite(s.live)&&s.live>=1:READY().test(s.status||''));
/** Readiness of one `__qaRev` timeline sample, for the slot's row count `a`/`b` and live count `la`/`lb`. */
const revReady=(x,slot)=>{const n=x[slot],live=x['l'+slot],status=x['s'+slot];
 return n>=1&&(P.readyKind==='liveTab'?Number.isFinite(live)&&live>=1:READY().test(status||''));};
// A card's two list tabs are told apart by WHAT THEY COUNT — the live tab counts setups, the researched-history
// tab counts stocks — not by their wording. The words in front of the number may change without this going
// stale, and a tab whose name counts neither still fails. Mirrored inside installProbes (page context).
const rxEsc=s=>String(s).replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
const TAB_SETUPS=/(\d[\d,]*)\s+setups?\b/i,TAB_STOCKS=/(\d[\d,]*)\s+stocks?\b/i;
const tabN=(label,re)=>{const m=String(label||'').match(re);return m?+m[1].replace(/,/g,''):null;};
/** What the card says about a cut list, asserted on its own — never as the ready gate. */
const cutState=s=>{const m=P.cutRe?String(s&&s.status||'').match(P.cutRe):null;
 return m?{shown:+m[1].replace(/,/g,''),total:+m[2].replace(/,/g,'')}:null;};
// Every block renders a Scanner A and B, so each locator is scoped to the LEADING block.
const BLK=()=>`[role=region][aria-label="${P.block}"]`;
const SC=slot=>`${BLK()} [role=region][aria-label^="Scanner ${slot}"]`;
const rowsLoc=(page,slot)=>page.locator(`${SC(slot)} [role=option] >> visible=true`);
const titleBtn=(page,slot)=>page.locator(`${SC(slot)} [aria-label^="Scanner ${slot}:"] >> visible=true`).first();
const pickerDlg=(page,slot)=>page.getByRole('dialog',{name:`Choose a strategy for Scanner ${slot}`}).last();
const pickerRows=dlg=>dlg.locator('[role=button][aria-label*=". NIFTY 500 · "]');
const chartSym=d=>d&&d.chart&&d.chart.heading&&d.chart.heading.includes(' · ')?d.chart.heading.split(' · ')[0]:null;
// The linked evidence card is the BACKTEST card for the stored block and the trader EVIDENCE card for the
// researched blocks; both head with "<kind> · SYMBOL · …" inside a region labelled "Backtest · …".
const btSym=d=>d&&d.bt&&d.bt.heading&&/^(Backtest|Evidence) · /.test(d.bt.heading)?d.bt.heading.split(' · ')[1]:null;
const apiResults=(page,key)=>page.evaluate(async k=>{const r=await fetch(`/api/strategies/${encodeURIComponent(k)}/results?universe=nifty500`,{credentials:'include'});const b=await r.json();return {status:r.status,total:b.total,unavailable:b.unavailable||null,data_end:b.data_end,universe:b.universe,rows:(b.rows||[]).map(x=>({symbol:x.symbol,match_id:x.match_id,low_pct:x.low_pct,n:x.n,timeframe:x.timeframe,status:x.status}))};},key);
// The rows a card LISTS in this mode: the stored scan's ranked stocks, or today's live detections. Both come
// back as {symbol, match_id, timeframe} so every selection, keyboard and deep-link check reads one shape.
const apiDetections=(page,key)=>page.evaluate(async k=>{const r=await fetch(`/api/strategies/${encodeURIComponent(k)}/detections?scope=live&limit=100`,{credentials:'include'});const b=await r.json();
 return {status:r.status,total:b.total,data_end:(b.live||{}).as_of||null,universe:{key:'live',label:"Today's detections",count:b.total},
  rows:(b.rows||[]).map(x=>({symbol:x.symbol,match_id:x.match_id,detection_id:x.detection_id,low_pct:null,n:null,timeframe:x.timeframe,status:x.state}))};},key);
const apiRows=(page,key)=>P.mode==='research'?apiDetections(page,key):apiResults(page,key);
// The workspace's own match set (GET /api/matches), which is what /chart?s=&tf= resolves against.
const apiWorkspaceMatches=page=>page.evaluate(async()=>{const r=await fetch('/api/matches?min_trades=0',{credentials:'include'});const b=await r.json();
 return Array.isArray(b)?b.slice(0,20).map(m=>({symbol:m.symbol,timeframe:m.timeframe,id:m.id})):[];});
const apiCatalog=page=>page.evaluate(async()=>{const r=await fetch('/api/strategies/catalog',{credentials:'include'});const c=await r.json();return {status:r.status,data_end:c.data_end,age_days:c.age_days,stale:c.stale,universe:c.universe,blocks:(c.blocks||[]).map(b=>({key:b.key,enabled:b.enabled,superseded:!!b.superseded,strategies:b.strategies.map(s=>({key:s.key,name:s.name,timeframe:s.timeframe,found:s.found,default_slot:s.default_slot,order:s.order,tags:s.tags,detections_today:s.detections_today,detections_live:s.detections_live,detections_last_detected:s.detections_last_detected}))}))};});
async function gotoDiscover(page,query=''){await page.goto(BASE+'/discover'+query,{waitUntil:'domcontentloaded',timeout:60000});await page.getByRole('heading',{name:'Discover Strategies',level:1}).waitFor({state:'visible',timeout:60000});}
async function discoverReady(page,{slots=['A','B'],timeout=45000}={}){
 return until(async()=>{const d=await disc(page);const ok=slots.every(s=>isReady(d[s]));
  return {ok,a:d.A&&d.A.rows.length,b:d.B&&d.B.rows.length,la:d.A&&d.A.live,lb:d.B&&d.B.live,sa:d.A&&d.A.status,sb:d.B&&d.B.status};},
  {timeout,msg:`scanner cards loaded (${P.listLabel}: a positive tab count AND at least one row rendered)`});
}
const clearDiscoverStore=page=>page.evaluate(()=>{for(const k of Object.keys(localStorage))if(k.startsWith('kanida.layout.discover.'))localStorage.removeItem(k);});
async function tapOrClick(loc,vp){if(vp==='phone')await loc.tap();else await loc.click();}
async function waitLinked(page,sym,{slot,timeout=20000}={}){
 return until(async()=>{const d=await disc(page);const src=d.chart&&d.chart.text.match(/(\S[^\n]*?) · from ([AB])/);
  const bases=P.mode==='research'?RESEARCH_BASIS:STORED_BASIS;
  const basis=d.bt?(bases.find(l=>d.bt.text.includes(l))||(P.mode!=='research'&&/No historical .* trades are recorded/.test(d.bt.text)?'no history':null)):null;
  const ok=chartSym(d)===sym&&btSym(d)===sym&&(!slot||(src&&src[2]===slot))&&!!d.chart.canvasLabel&&d.chart.canvasLabel.startsWith(sym+' ')&&d.chart.svgMax.w>100&&d.chart.svgMax.h>80&&!!basis;
  return {ok,chart:d.chart&&d.chart.heading,source:src?src[0]:null,svg:d.chart&&d.chart.svgMax,canvas:d.chart&&d.chart.canvasLabel,bt:d.bt&&d.bt.heading,btBasis:basis,costLine:!!(d.bt&&d.bt.text.includes('Costs 0.40% included'))};},{timeout,msg:`chart + backtest linked to ${sym}${slot?' ('+slot+')':''}`});
}
// Ensure the strategy picker for `slot` is open, whatever state the page is in — and never assume.
// The title button TOGGLES, so this lane had a hidden coupling in BOTH directions: 5.2 and 5.3a inherited
// the picker 5.1 leaves open (they broke the moment `dismissOverlays` started closing it on entry), while
// 5.3c, 5.4, 5.6 and the admin picker assumed it was CLOSED and would have closed an open one. Every check
// that needs the picker now calls this, so the order it runs in stops mattering.
async function openPicker(page,slot,vp){
 const dlg=pickerDlg(page,slot);
 if(!(await dlg.isVisible().catch(()=>false)))await tapOrClick(titleBtn(page,slot),vp||'desktop');
 await dlg.waitFor({state:'visible',timeout:8000});
 return dlg;
}
async function pickStrategy(page,slot,name,vp){
 const dlg=await openPicker(page,slot,vp);
 await dlg.getByRole('textbox',{name:'Search strategies'}).fill(name.split(' · ')[0]);
 const row=dlg.locator(`[role=button][aria-label^="${name}. NIFTY 500 · "]`).first();await row.waitFor({state:'visible',timeout:5000});await tapOrClick(row,vp);
 await dlg.waitFor({state:'hidden',timeout:5000});
 await until(async()=>{const d=await disc(page);return {ok:d[slot]&&d[slot].title===`Scanner ${slot}: ${name}. Change strategy`,title:d[slot]&&d[slot].title};},{timeout:8000,msg:`card ${slot} shows ${name}`});
}
async function ensureDrive(page,vp){
 if(P.driveKeyA===P.keyA&&P.driveKeyB===P.keyB)return;
 const d=await disc(page).catch(()=>null);
 if(!d||!d.A||!(d.A.title||'').includes(P.driveA))await pickStrategy(page,'A',P.driveA,vp.key).catch(()=>{});
 if(!d||!d.B||!(d.B.title||'').includes(P.driveB))await pickStrategy(page,'B',P.driveB,vp.key).catch(()=>{});
}
// A menu that is opened and then not used must be DISMISSED before the failure is raised. An overlay left
// open blocks every click after it, which turned one missing menu item into six unrelated red checks.
async function pickMenuItem(page,mi,what){
 try{await mi.first().waitFor({state:'visible',timeout:5000});}
 catch(e){await page.keyboard.press('Escape').catch(()=>{});await sleep(200);throw new Error(`${what}: ${String(e.message).split('\n')[0]}`);}
 await mi.first().click();await sleep(250);
}
async function cardMenu(page,slot,item){await page.locator(`${SC(slot)} [aria-label="Scanner ${slot} options"]`).first().click();
 await pickMenuItem(page,page.getByRole('menuitem',{name:item}),`card ${slot} menu item ${item}`);}
async function blockMenu(page,item){await page.locator(`${BLK()} [aria-label="${P.block} options"]`).first().click();
 await pickMenuItem(page,page.getByRole('menuitem',{name:item,exact:true}),`block menu item ${item}`);}
async function waitStableRows(page,slot){let prev='';return until(async()=>{const d=await disc(page);const k=d[slot]?d[slot].rows.map(r=>r.sym).join(','):'';const ok=!!k&&k===prev&&isReady(d[slot]);prev=k;return {ok,rows:d[slot]?d[slot].rows:[]};},{timeout:10000,interval:250,msg:`rows of ${slot} stable`});}

// ---------------- sessions ----------------
async function jfetch(p,{method='GET',body,cookie,csrf}={}){
 const r=await fetch(BASE+p,{method,redirect:'manual',headers:{...(body?{'content-type':'application/json'}:{}),...(cookie?{cookie}:{}),...(csrf?{'x-kanida-csrf':csrf}:{})},body:body?JSON.stringify(body):undefined});
 let d=null;try{d=await r.json();}catch{}const sc=r.headers.get('set-cookie');return {status:r.status,data:d,cookie:sc?sc.split(';')[0]:null};
}
async function ensureMember(){
 const steps=[];const o=await jfetch('/api/auth/login',{method:'POST',body:{email:EMAIL,password:PASSWORD}});if(o.status!==200)throw new Error('owner API login failed '+o.status);
 let m=await jfetch('/api/auth/login',{method:'POST',body:{email:MEMBER.email,password:MEMBER.password}});steps.push('member login '+m.status);
 if(m.status!==200){
  const inv=await jfetch('/api/admin/invites',{method:'POST',body:{email:MEMBER.email},cookie:o.cookie,csrf:o.data.csrf});steps.push('invite '+inv.status);if(inv.status!==200)throw new Error('invite failed '+short(inv.data));
  const token=new URL(inv.data.url).searchParams.get('invite');
  m=await jfetch('/api/auth/register',{method:'POST',body:{email:MEMBER.email,password:MEMBER.password,name:MEMBER.name,invite:token,policy_version:POLICY}});steps.push('register '+m.status);
  if(m.status!==200)throw new Error('member register failed '+short(m.data));
 }
 if(!m.data.user.onboarded){const ob=await jfetch('/api/account/onboarding',{method:'POST',body:{policy_version:POLICY,acknowledge_pilot:true,name:MEMBER.name},cookie:m.cookie,csrf:m.data.csrf});steps.push('onboarding '+ob.status);}
 const acc=await jfetch('/api/admin/access',{method:'POST',body:{email:MEMBER.email},cookie:o.cookie,csrf:o.data.csrf});steps.push('access '+acc.status);
 return {role:m.data.user.role,steps};
}
async function login(browser,email,password){
 const ctx=await browser.newContext({viewport:{width:1440,height:900}});const page=await ctx.newPage();
 await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
 await page.getByRole('textbox',{name:'Email'}).fill(email);await page.locator('input[aria-label="Password"]').fill(password);
 await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
 await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});await sleep(1500);
 const p=new URL(page.url()).pathname;if(p==='/billing'||p==='/onboarding')throw new Error(`${email} landed on ${p}`);
 const state=await ctx.storageState();await ctx.close();
 for(const o of state.origins||[])o.localStorage=[];// every viewport starts from default Discover/layout state (session cookie kept)
 return state;
}
async function newPage(browser,state,vp,tag){
 const ctx=await browser.newContext({viewport:{width:vp.width,height:vp.height},isMobile:!!vp.isMobile,hasTouch:!!vp.hasTouch,deviceScaleFactor:vp.isMobile?2:1,storageState:state});
 await ctx.addInitScript(installProbes,{readyKind:P.readyKind,readySource:P.readySource,rowKind:P.rowKind,block:P.block});const page=await ctx.newPage();page.setDefaultTimeout(15000);
 const log=consoleLog[tag]={errors:[],expected:[]};page.__expect=[];
 page.on('pageerror',e=>log.errors.push({type:'pageerror',text:e.message.slice(0,500),url:page.url()}));
 page.on('console',m=>{if(m.type()!=='error')return;const loc=(m.location()&&m.location().url)||'';const item={type:'console',text:m.text().slice(0,400),resource:loc,url:page.url()};
  if(/Failed to load resource/.test(item.text)&&page.__expect.some(x=>loc.includes(x)))log.expected.push(item);else log.errors.push(item);});
 return {ctx,page,log};
}

// ---------------- checks ----------------
async function navChecks(page,vp){
 const phone=vp.key==='phone';
 await check('1.1',vp.key,`Nav on "/": ${NAV.length} items in order${phone?' (bottom tab bar)':''}, Falcon active; placeholder has disabled scan button`,page,async()=>{
  await page.goto(BASE+'/',{waitUntil:'domcontentloaded',timeout:60000});await page.getByRole('heading',{name:'Falcon',level:1}).waitFor({state:'visible',timeout:60000});await sleep(500);
  const navs=await navState(page);if(navs.length!==1)throw new Error(`${navs.length} visible Main navigation landmarks: `+short(navs));
  const n=navs[0],labels=n.items.map(i=>(i.label||'').replace(/, current page$/,'')),cur=n.items.filter(i=>i.current==='page').map(i=>i.label);
  const inner=await page.evaluate(()=>({h:innerHeight,w:innerWidth}));
  const scan=page.getByRole('button',{name:/^Scan the market in 30 sec/});await scan.waitFor({state:'visible'});const scanDisabled=await scan.getAttribute('aria-disabled');
  const open=page.getByRole('link',{name:'Open Discover Strategies'});await open.waitFor({state:'visible'});const openText=(await open.innerText()).trim();
  const out={labels,texts:n.items.map(i=>i.text),current:cur,navRect:n.rect,viewport:inner,scanDisabled,openText,shot:await shot(page,vp.key,'01-falcon-home')};
  if(JSON.stringify(labels)!==JSON.stringify(NAV))throw new Error('nav labels '+short(out));
  if(cur.length!==1||!cur[0].startsWith('Falcon'))throw new Error('active item '+short(out));
  if(phone&&n.rect.y+n.rect.h<inner.h-4)throw new Error('phone nav is not a bottom tab bar: '+short(out));
  if(!phone&&n.rect.y>90)throw new Error('desktop nav not in top bar: '+short(out));
  if(scanDisabled!=='true')throw new Error('scan button not disabled: '+short(out));
  return out;});
 await check('1.2',vp.key,'Nav links navigate with correct active state (/discover → /watch → /autotrade → /), logo → /',page,async()=>{
  const seq=[];
  for(const label of ['Discover Strategies','Watchlist','AutoTrade','Falcon']){
   const link=page.locator(`[role=navigation][aria-label="Main navigation"] [role=link][aria-label^="${label}"] >> visible=true`).first();await tapOrClick(link,vp.key);
   const r=await until(async()=>{const u=new URL(page.url());const n=await navState(page);const cur=n.length?n[0].items.filter(i=>i.current==='page').map(i=>i.label):[];
    return {ok:u.pathname===NAV_ROUTE[label]&&n.length===1&&cur.length===1&&cur[0].startsWith(label),path:u.pathname,current:cur,navs:n.length};},{timeout:45000,msg:`nav → ${label}`});
   seq.push({label,path:r.path,current:r.current});await sleep(400);await shot(page,vp.key,`01-nav-${NAV_ROUTE[label].replace('/','')||'home'}`);
  }
  await tapOrClick(page.getByRole('link',{name:'Discover Strategies'}).first(),vp.key).catch(async()=>{await page.goto(BASE+'/discover');});
  await until(async()=>({ok:new URL(page.url()).pathname==='/discover'}),{timeout:20000,msg:'back on /discover'});
  const logo=page.getByRole('link',{name:/^KANIDA home/}).first();await logo.waitFor({state:'visible'});const logoLabel=await logo.getAttribute('aria-label');await tapOrClick(logo,vp.key);
  await until(async()=>({ok:new URL(page.url()).pathname==='/',path:new URL(page.url()).pathname}),{timeout:20000,msg:'logo → /'});
  return {sequence:seq,logo:{label:logoLabel,path:'/'}};});
 await check('1.3',vp.key,'"/" placeholder: "Open Discover Strategies" navigates to /discover',page,async()=>{
  await page.getByRole('heading',{name:'Falcon',level:1}).waitFor({state:'visible',timeout:30000});
  await tapOrClick(page.getByRole('link',{name:'Open Discover Strategies'}),vp.key);
  await until(async()=>({ok:new URL(page.url()).pathname==='/discover',path:new URL(page.url()).pathname}),{timeout:20000,msg:'placeholder → /discover'});
  await page.getByRole('heading',{name:'Discover Strategies',level:1}).waitFor({state:'visible',timeout:30000});return {path:'/discover'};});
}

async function redirectChecks(page,vp,sym,tf='1D'){
 await check('2.1',vp.key,`Legacy /?s=${sym}&tf=${tf} redirects to /chart?s=${sym}&tf=${tf} showing ${sym}`,page,async()=>{
  await page.goto(BASE+`/?s=${sym}&tf=${tf}`,{waitUntil:'domcontentloaded',timeout:60000});
  const r=await until(async()=>{const u=new URL(page.url());const t=await page.title();const head=await page.locator(`[role=heading][aria-label^="Chart header for ${sym}"] >> visible=true`).count();
   return {ok:u.pathname==='/chart'&&u.searchParams.get('s')===sym&&u.searchParams.get('tf')===tf&&(t.startsWith(sym+' · ')||head>0),url:u.pathname+u.search,title:t,heading:head};},{timeout:60000,msg:'redirect + chart symbol'});
  return {...r,shot:await shot(page,vp.key,'02-legacy-redirect')};});
 if(vp.key==='phone'){results.push({id:'2.2',vp:vp.key,name:'/chart?…&tab=evidence opens Evidence/Replay',status:'INFO',detail:'phone workspace panels have no Evidence/Replay tab (Phase 0 PHONE_TABS); not applicable'});return;}
 await check('2.2',vp.key,`/chart?s=${sym}&tf=${tf}&tab=evidence opens the Evidence/Replay tab`,page,async()=>{
  await page.goto(BASE+`/chart?s=${sym}&tf=${tf}&tab=evidence`,{waitUntil:'domcontentloaded',timeout:60000});
  const r=await until(async()=>{const tabs=await page.locator('[role=tab][aria-selected="true"]').evaluateAll(els=>els.filter(e=>e.getClientRects().length).map(e=>e.getAttribute('aria-label')));const u=new URL(page.url());
   return {ok:tabs.includes('Evidence/Replay'),tabs,url:u.pathname+u.search,title:await page.title()};},{timeout:60000,msg:'Evidence/Replay tab selected'});
  return {...r,shot:await shot(page,vp.key,'02-chart-evidence-tab')};});
}

async function discoverLoad(page,vp,catalog){
 // 3.0 + 4.3: throttled load so the loading state can be observed and captured; then reveal timeline.
 const cdp=await page.context().newCDPSession(page);await cdp.send('Network.enable');
 await check('4.3',vp.key,`Loading state (a line starting "${P.mode==='research'?'Loading':'Loading stored scan'}…") visible, then rows appear`,page,async()=>{
  await cdp.send('Network.emulateNetworkConditions',{offline:false,latency:700,downloadThroughput:-1,uploadThroughput:-1});
  let loadingShot=null,mid=null;
  try{
   await page.goto(BASE+'/discover',{waitUntil:'domcontentloaded',timeout:90000});
   const l=await until(async()=>{const d=await disc(page);return {ok:!!(d.A&&P.loadingRe.test(d.A.status||'')),status:d.A&&d.A.status};},{timeout:60000,interval:40,msg:`"${P.loadingRe.source}" in scanner A`});
   loadingShot=await shot(page,vp.key,'04-loading');loadingShot={status:l.status,shot:loadingShot};
  }finally{await cdp.send('Network.emulateNetworkConditions',{offline:false,latency:0,downloadThroughput:-1,uploadThroughput:-1}).catch(()=>{});}
  try{const m=await until(async()=>{const d=await disc(page);const n=d.A?d.A.rows.length:0;return {ok:n>0&&(P.mode==='research'||!isReady(d.A)),rows:n,status:d.A&&d.A.status};},{timeout:30000,interval:15,msg:'mid-reveal'});
   const file=await shot(page,vp.key,'04-mid-reveal');const after=await disc(page);mid={rowsBeforeShot:m.rows,statusBeforeShot:m.status,rowsAfterShot:after.A.rows.length,shot:file};}catch(e){mid={error:e.message.split('\n')[0]};}
  await discoverReady(page,{timeout:60000});await until(async()=>({ok:await page.evaluate(()=>window.__qaRevDone===true)}),{timeout:15000,msg:'reveal sampler finished'}).catch(()=>{});
  const rev=await page.evaluate(()=>window.__qaRev||[]);extra[vp.key+'-reveal']=rev;
  const d=await disc(page),total=d.A.rows.length;
  const aCounts=[...new Set(rev.map(x=>x.a))].filter(n=>n>0&&n<total),loadingSeen=rev.some(x=>P.loadingRe.test(x.sa||'')),countingSeen=rev.filter(x=>/^Loading stored scan… \d+ found$/.test(x.sa||'')).map(x=>x.sa);
  const firstRow=rev.find(x=>x.a>0),lastEntry=rev.find(x=>x.a===total&&revReady(x,'a'));
  const out={total,intermediateRowCounts:aCounts.length,sampleInstants:firstRow&&lastEntry?[{t:firstRow.t,rows:firstRow.a,status:firstRow.sa},{t:lastEntry.t,rows:lastEntry.a,status:lastEntry.sa}]:null,revealMs:firstRow&&lastEntry?lastEntry.t-firstRow.t:null,loadingSeen,countingStatusSamples:countingSeen.slice(0,4),loading:loadingShot,mid};
  // The stored list reveals progressively (one step per 25 ms); the live list is short and lands in one go,
  // so only the loading state is asserted there. Neither assertion is skipped - they are different lists.
  if(!loadingSeen)throw new Error('loading state not observed: '+short(out));
  if(P.mode!=='research'&&aCounts.length<3)throw new Error('progressive reveal not observed: '+short(out));return out;},150000);
 await cdp.detach().catch(()=>{});
}

async function layoutChecks(page,vp){
 await check('3.1',vp.key,'Discover header: title, universe chip, data-age pill, Columns, Refresh; stale research-only line',page,async()=>{
  await page.getByRole('heading',{name:'Discover Strategies',level:1}).waitFor({state:'visible'});
  const uni=page.getByRole('button',{name:/^Universe: .*Change universe$/});await uni.waitFor({state:'visible'});
  const cols=page.getByRole('button',{name:/^Card columns: .*Change density$/});if(vp.key!=='phone')await cols.waitFor({state:'visible'}); // phone hides Columns by design (stacked layout)
  const refresh=page.getByRole('button',{name:'Refresh all strategy cards'});await refresh.waitFor({state:'visible'});
  const m=await page.evaluate(()=>{const vis=e=>e.getClientRects().length>0;const pills=Array.from(document.querySelectorAll('[role=status][aria-label^="Market data"]')).filter(vis);const inTop=p=>!!p.closest('[role=group][aria-label$="op bar"]');
   const page=pills.filter(p=>!inTop(p));const txt=document.body.innerText;const stale=(txt.match(/Research only — prices are[^\n]*/)||[])[0]||null;return {pagePill:page.map(p=>({label:p.getAttribute('aria-label'),text:p.innerText.trim()})),topBarPill:pills.filter(inTop).map(p=>p.innerText.trim()),staleLine:stale};});
  const out={universe:(await uni.innerText()).trim(),columns:vp.key==='phone'?'(hidden on phone by design)':(await cols.innerText()).trim(),...m,shot:await shot(page,vp.key,'03-discover')};
  if(!m.pagePill.length)throw new Error('no data-age pill in the Discover header: '+short(out));
  if(!/nifty 500/i.test(out.universe))throw new Error('universe chip: '+out.universe);
  return out.universe!=='NIFTY 500'?{...out,note:`universe chip reads "${out.universe}" (spec: "NIFTY 500")`}:out;});
 await check('3.2',vp.key,`${P.block} block: card layout = ${vp.mode==='four'?'4-across':vp.mode==='two'?'2×2':'stacked scanners (chart/backtest via sheet)'}`,page,async()=>{
  const d=await disc(page);const A=d.A&&d.A.rect,B=d.B&&d.B.rect,Ch=d.chart&&d.chart.rect,Bt=d.bt&&d.bt.rect;const near=(a,b,t=3)=>Math.abs(a-b)<=t;
  const out={A,B,chart:Ch,backtest:Bt,block:d.block};
  if(!A||!B)throw new Error('scanner cards missing '+short(out));
  if(vp.mode==='four'){if(!Ch||!Bt)throw new Error('chart/backtest card missing '+short(out));
   out.gaps=[B.x-(A.x+A.w),Ch.x-(B.x+B.w),Bt.x-(Ch.x+Ch.w)];out.widthRatios={chartToA:+(Ch.w/A.w).toFixed(3),backtestToA:+(Bt.w/A.w).toFixed(3),spec:'~0.95:0.95:1.45:1.25, scanner min 268px'};out.heights=[A.h,B.h,Ch.h,Bt.h];
   if(!(near(A.y,B.y)&&near(A.y,Ch.y)&&near(A.y,Bt.y)&&A.x<B.x&&B.x<Ch.x&&Ch.x<Bt.x))throw new Error('not 4-across '+short(out));
   if(out.widthRatios.chartToA<1.3||out.widthRatios.backtestToA<1.15||out.gaps.some(g=>Math.abs(g-12)>2))throw new Error('4-across proportions/gaps off spec '+short(out));}
  else if(vp.mode==='two'){if(!Ch||!Bt)throw new Error('chart/backtest card missing '+short(out));
   if(!(near(A.y,B.y)&&near(Ch.y,Bt.y)&&Ch.y>=A.y+A.h&&near(A.x,Ch.x)&&near(B.x,Bt.x)))throw new Error('not 2×2 '+short(out));out.heights=[A.h,B.h,Ch.h,Bt.h];}
  else{if(!(B.y>=A.y+A.h&&near(A.x,B.x)))throw new Error('scanners not stacked '+short(out));if(Ch||Bt)throw new Error('chart/backtest rendered in the phone grid '+short(out));}
  return out;});
 await check('3.3',vp.key,'No horizontal page scroll; page scrolls vertically when taller than the window',page,async()=>{
  const m=await page.evaluate(BLOCK=>{const block=Array.from(document.querySelectorAll(`[role=region][aria-label="${BLOCK}"]`)).find(e=>e.getClientRects().length);let el=block&&block.parentElement,sc=null;
   while(el&&el!==document.body){const cs=getComputedStyle(el);if(/(auto|scroll)/.test(cs.overflowY)&&el.scrollHeight>el.clientHeight+1){sc=el;break;}el=el.parentElement;}
   const out={docScrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,innerWidth,innerHeight};
   const wide=Array.from(document.querySelectorAll('[role=region]')).filter(e=>e.getClientRects().length&&e.getBoundingClientRect().right>innerWidth+1).map(e=>e.getAttribute('aria-label'));out.regionsPastRightEdge=wide;
   if(sc){const before=sc.scrollTop;sc.scrollTop=before+240;out.scroll={scrollHeight:sc.scrollHeight,clientHeight:sc.clientHeight,movedTo:sc.scrollTop};sc.scrollTop=before;}else out.scroll=null;
   return out;},P.block);
  if(m.docScrollWidth>m.innerWidth+1||m.bodyScrollWidth>m.innerWidth+1)throw new Error('horizontal overflow '+short(m));
  if(m.scroll&&m.scroll.movedTo<=0)throw new Error('vertical scroll container did not scroll '+short(m));
  if(m.regionsPastRightEdge.length)throw new Error('regions past right edge '+short(m));
  if(!m.scroll)return {...m,note:'content fits the window; no vertical scroll needed'};
  const s=await page.evaluate(BLOCK=>{const b=Array.from(document.querySelectorAll(`[role=region][aria-label="${BLOCK}"]`)).find(e=>e.getClientRects().length);let el=b.parentElement;while(el&&!(/(auto|scroll)/.test(getComputedStyle(el).overflowY)&&el.scrollHeight>el.clientHeight+1))el=el.parentElement;el.scrollTop=el.scrollHeight;return true;},P.block);
  await sleep(300);const bottom=await shot(page,vp.key,'03-discover-scrolled-bottom');
  await page.evaluate(BLOCK=>{const b=Array.from(document.querySelectorAll(`[role=region][aria-label="${BLOCK}"]`)).find(e=>e.getClientRects().length);let el=b.parentElement;while(el&&!(/(auto|scroll)/.test(getComputedStyle(el).overflowY)&&el.scrollHeight>el.clientHeight+1))el=el.parentElement;if(el)el.scrollTop=0;},P.block);
  return {...m,shotBottom:bottom,s};});
}

async function scannerDefaultChecks(page,vp,catalog,api){
 // The leading block's registry defaults, and the status line + row order OF THE LIST THAT BLOCK SHOWS.
 // A stored card lists stocks ranked by 95% low; a research card lists today's detections, confirmed first
 // and newest first inside each state. Both are asserted - neither is asserted against the other's rules.
 const block=catalog.blocks.find(b=>b.key===P.blockKey);
 const defA=block.strategies.find(s=>s.default_slot==='A'),defB=block.strategies.find(s=>s.default_slot==='B');
 for(const [slot,def] of [['A',defA],['B',defB]]){
  if(!def)throw new Error(`${P.block} has no default slot ${slot}`);
  await check(`4.${slot==='A'?1:2}`,vp.key,`Scanner ${slot} default = ${def.name}; status line; N = rows = API total; ${P.mode==='research'?'confirmed detections first':'sorted by 95% low desc (nulls last)'}`,page,async()=>{
   const d=(await discoverReady(page),await disc(page))[slot];const res=api[def.key]||await apiRows(page,def.key);
   const m=P.statusRe?(d.status||'').match(P.statusRe):null;
   const domSyms=d.rows.map(r=>r.sym),apiSyms=res.rows.map(r=>r.symbol);
   let sorted=true,statusN=null,universeText=null,dataTo=null,cut=null;
   if(P.mode==='research'){
    // The card's ONE count is its "Now" tab label; the status line carries the cut, not the count.
    statusN=d.live;cut=cutState(d);
    // The active list is newest-detection-first (the API's own order, asserted by orderMatchesApi), and every
    // row must show that it is still live and when it was detected.
    if(d.rows.some(r=>!r.state||!r.detected||!['Forming','Confirmed'].includes(r.state)))sorted=false;
   }else{
    statusN=m?+m[3]:null;universeText=m?m[1]:null;dataTo=m?m[2]:null;
    const lows=d.rows.map(r=>r.low);
    for(let i=1;i<lows.length;i++){const a=lows[i-1],b=lows[i];if(a===null&&b!==null)sorted=false;if(a!==null&&b!==null&&b>a+1e-9)sorted=false;}
   }
   const out={mode:P.mode,title:d.title,status:d.status,tab:d.liveTabText,statusN,universeText,dataTo,cut,rows:d.rows.length,apiTotal:res.total,apiStatus:res.status,
    orderMatchesApi:JSON.stringify(domSyms)===JSON.stringify(apiSyms),sampleRows:d.rows.slice(0,3)};
   if(d.title!==`Scanner ${slot}: ${def.name}. Change strategy`)throw new Error('default strategy '+short(out));
   if(P.mode==='research'){
    // The count the card shows is the API's total, and the rows it renders are the API's rows.
    if(!Number.isFinite(statusN))throw new Error('no count on the Now tab '+short(out));
    if(statusN!==res.total)throw new Error('tab count vs API total '+short(out));
    if(out.rows<1||out.rows!==Math.min(res.total,res.rows.length||res.total))throw new Error('rows vs API '+short(out));
    // The cut line is its OWN statement: present and exact when the list is cut, absent when it is whole.
    if(out.rows<statusN){
     if(!cut||cut.shown!==out.rows||cut.total!==statusN)throw new Error('cut line '+short(out));
    }else if(d.status)throw new Error('a complete list must not carry a status line '+short(out));
   }else{
    if(!m)throw new Error('status text '+short(out));
    if(!/^nifty 500$/i.test(m[1]))throw new Error('status text '+short(out));
    if(out.statusN!==out.rows||out.rows!==res.total)throw new Error('count mismatch '+short(out));
   }
   if(!out.orderMatchesApi||!sorted)throw new Error('row order '+short(out));
   return (P.mode!=='research'&&m[1]!=='NIFTY 500')?{...out,note:`status reads "${m[1]}" (spec: "NIFTY 500")`}:out;});
 }
}

async function pickerChecks(page,vp,catalog,full){
 const chart=catalog.blocks.find(b=>b.key===P.blockKey),W=vp.width,H=vp.height;
 await check('5.1',vp.key,'Picker opens anchored under card A title (~480px, ≤70% height), "CHOOSE A STRATEGY", search focused',page,async()=>{
  await discoverReady(page);const tb=titleBtn(page,'A');const tbr=await tb.boundingBox();await tapOrClick(tb,vp.key);
  // The one check that must OPEN the picker itself rather than ensure it is open: it measures the popover
  // against the title button and asserts that opening focuses the search box. `check()` dismisses any overlay
  // on entry, so it always starts from closed.
  const dlg=pickerDlg(page,'A');await dlg.waitFor({state:'visible',timeout:8000});await sleep(350);
  const box=await dlg.boundingBox();const act=await page.evaluate(()=>document.activeElement&&document.activeElement.getAttribute('aria-label'));
  const head=(await dlg.innerText()).split('\n')[0].trim();const rows=await pickerRows(dlg).count();
  const out={titleButton:tbr&&{x:Math.round(tbr.x),y:Math.round(tbr.y),w:Math.round(tbr.width),h:Math.round(tbr.height)},popover:{x:Math.round(box.x),y:Math.round(box.y),w:Math.round(box.width),h:Math.round(box.height)},gapBelowTitle:Math.round(box.y-(tbr.y+tbr.height)),leftOffset:Math.round(box.x-tbr.x),heightPctOfWindow:+(box.height/H*100).toFixed(1),activeElement:act,header:head,rows,shot:await shot(page,vp.key,'05-picker-open')};
  if(out.gapBelowTitle<0||out.gapBelowTitle>16)throw new Error('not anchored under title '+short(out));
  if(vp.key!=='phone'&&Math.abs(out.leftOffset)>8)throw new Error('not left-aligned with title '+short(out));
  if(Math.abs(box.width-Math.min(480,W-16))>4)throw new Error('width '+short(out));
  if(box.height>H*.7+20)throw new Error('taller than 70% of window '+short(out));
  if(act!=='Search strategies')throw new Error('search not focused '+short(out));
  if(head!=='CHOOSE A STRATEGY')throw new Error('header text '+short(out));
  if(rows!==chart.strategies.length)throw new Error(`rows ${rows} != catalog strategies ${chart.strategies.length}`);
  return out;});
 if(!full){
  await check('5.5',vp.key,'Picker: Esc closes',page,async()=>{const dlg=await openPicker(page,'A',vp.key);await page.keyboard.press('Escape');await dlg.waitFor({state:'hidden',timeout:4000});return {closed:true};});
  return;
 }
 await check('5.2',vp.key,'Picker: typing filters; chip "1W" shows only 1W strategies',page,async()=>{
  // Self-sufficient: it used to inherit the picker 5.1 leaves open. The search box is focused on open, so
  // typing still lands in it whether this check opened the picker or found it already open.
  const dlg=await openPicker(page,'A',vp.key);const search=dlg.getByRole('textbox',{name:'Search strategies'});
  await search.focus();
  await page.keyboard.type('channel',{delay:20});await sleep(300);
  const typed=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label').split('. NIFTY 500 · ')[0]));
  const expectTyped=chart.strategies.filter(s=>/channel/i.test(s.name)).length;
  await search.fill('');await dlg.getByRole('button',{name:'Filter: 1W'}).click();await sleep(300);
  const w=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label').split('. NIFTY 500 · ')[0]));const expectW=chart.strategies.filter(s=>s.timeframe==='1W').length;
  const sh=await shot(page,vp.key,'05-picker-chip-1W');const chips=await dlg.locator('[role=group][aria-label="Filter strategies"] [role=button]').evaluateAll(els=>els.map(e=>e.innerText.trim()));
  await dlg.getByRole('button',{name:'Filter: All'}).click();
  const out={typed:{query:'channel',rows:typed.length,expected:expectTyped,allMatch:typed.every(n=>/channel/i.test(n))},chip1W:{rows:w.length,expected:expectW,allIW:w.every(n=>/· 1W\b/.test(n)),sample:w.slice(0,3)},chips,shot:sh};
  if(!out.typed.allMatch||out.typed.rows!==expectTyped||!out.chip1W.allIW||out.chip1W.rows!==expectW)throw new Error(short(out));return out;});
 let keyboardPicked=null;
 const activeLabel=()=>page.evaluate(()=>document.activeElement&&document.activeElement.getAttribute('aria-label'));
 await check('5.3a',vp.key,'Picker keyboard: ↓ from the focused search box moves focus into the list',page,async()=>{
  // Self-sufficient: it used to inherit the picker left open by 5.1 / 5.2.
  const dlg=await openPicker(page,'A',vp.key);await dlg.getByRole('textbox',{name:'Search strategies'}).focus();
  const names=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label').split('. NIFTY 500 · ')[0]));
  await page.keyboard.press('ArrowDown');await sleep(200);const a1=await activeLabel();
  const out={firstRow:names[0],focusAfterArrowDown:a1};if(!a1||!a1.startsWith(names[0]+'.'))throw new Error('focus stayed in search '+short(out));return out;});
 await check('5.3b',vp.key,'Picker keyboard inside list: ↓/↑ move focus, Enter picks → card A strategy + results change',page,async()=>{
  const dlg=await openPicker(page,'A',vp.key);
  const names=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label').split('. NIFTY 500 · ')[0]));
  await pickerRows(dlg).nth(0).focus();const a0=await activeLabel();
  await page.keyboard.press('ArrowDown');await sleep(150);const a1=await activeLabel();
  await page.keyboard.press('ArrowDown');await sleep(150);const a2=await activeLabel();
  await page.keyboard.press('ArrowUp');await sleep(150);const a3=await activeLabel();
  const target=(a3||'').split('. NIFTY 500 · ')[0];
  await page.keyboard.press('Enter');
  const closed=await dlg.waitFor({state:'hidden',timeout:4000}).then(()=>true).catch(()=>false);
  const out={names:names.slice(0,3),focus:[a0,a1,a2,a3].map(x=>x&&x.split('. NIFTY 500 · ')[0]),enterClosedPicker:closed,target};
  if(!a1||!a1.startsWith(names[1]+'.')||!a2||!a2.startsWith(names[2]+'.')||!a3||!a3.startsWith(names[1]+'.'))throw new Error('arrow focus '+short(out));
  if(!closed){await page.keyboard.press('Escape').catch(()=>{});throw new Error('Enter did not pick '+short(out));}
  const st=chart.strategies.find(s=>s.name===target);
  const r=await until(async()=>{const d=await disc(page);return {ok:!!d.A&&d.A.title===`Scanner A: ${target}. Change strategy`&&(isReady(d.A)||/^No /.test(d.A.status||'')),title:d.A&&d.A.title,status:d.A&&d.A.status,live:d.A&&d.A.live,rows:d.A?d.A.rows.length:null};},{timeout:15000,msg:'card A switched'});
  // The rows on screen must equal the total the API reports for the SAME list the card is showing.
  const api=await apiRows(page,st.key);if(r.rows!==Math.min(api.total,api.rows.length||api.total))throw new Error('rows vs API '+short({r,api:api.total}));
  return {...out,card:r,apiTotal:api.total,shot:await shot(page,vp.key,'05-picker-keyboard-picked')};});
 await check('5.3c',vp.key,'Picker: Enter in the search box (no arrow) — what it does (informational)',page,async()=>{
  const before=(await disc(page)).A.title;const dlg=await openPicker(page,'A',vp.key);await sleep(200);
  await dlg.getByRole('textbox',{name:'Search strategies'}).focus();const first=(await pickerRows(dlg).nth(0).getAttribute('aria-label')).split('. NIFTY 500 · ')[0];
  await page.keyboard.press('Enter');const closed=await dlg.waitFor({state:'hidden',timeout:3000}).then(()=>true).catch(()=>false);await sleep(300);const after=(await disc(page)).A.title;
  if(!closed)await page.keyboard.press('Escape');keyboardPicked=after.replace(/^Scanner A: /,'').replace(/\. Change strategy$/,'');
  return {info:{before,firstRowInList:first,enterClosedPicker:closed,after,note:'Enter in the empty search box picks the first listed strategy (onSubmitEditing)'}};});
 await check('5.4',vp.key,'Picker: "Recently used" section lists the last pick first',page,async()=>{
  const dlg=await openPicker(page,'A',vp.key);await sleep(250);
  const grp=dlg.locator('[role=group][aria-label="Recently used"]');const n=await grp.count();const items=n?await grp.locator('[role=button]').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label').split('. NIFTY 500 · ')[0])):[];
  const groups=await dlg.locator('[role=group][aria-label]').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));
  const out={groups,recent:items,expectedFirst:keyboardPicked,shot:await shot(page,vp.key,'05-picker-recent')};
  if(!n||items[0]!==keyboardPicked||groups.indexOf('Recently used')>groups.indexOf('All strategies'))throw new Error(short(out));return out;});
 await check('5.5',vp.key,'Picker: Esc closes; outside click closes (and selects nothing underneath)',page,async()=>{
  const dlg=await openPicker(page,'A',vp.key);
  await page.keyboard.press('Escape');const esc=await dlg.waitFor({state:'hidden',timeout:4000}).then(()=>true).catch(()=>false);
  await openPicker(page,'A',vp.key);await sleep(250);const box=await dlg.boundingBox();
  const x=Math.round(Math.min(W-30,box.x+box.width+60)),y=Math.round(Math.min(H-30,box.y+box.height+40));const before=await disc(page);
  await page.mouse.click(x,y);const outside=await dlg.waitFor({state:'hidden',timeout:4000}).then(()=>true).catch(()=>false);await sleep(300);const after=await disc(page);
  const out={escCloses:esc,outsideClickAt:[x,y],outsideCloses:outside,urlUnchanged:before.path===after.path,linkUnchanged:chartSym(before)===chartSym(after)};
  if(!esc||!outside||!out.urlUnchanged||!out.linkUnchanged)throw new Error(short(out));return out;});
 await check('5.6',vp.key,`Picker mouse pick back to ${P.driveA} → results change`,page,async()=>{
  await pickStrategy(page,'A',P.driveA,vp.key);const r=await discoverReady(page,{slots:['A']});return {rows:r.a,status:r.sa};});
}

async function linkingChecks(page,vp,api){
 let fw=api[P.driveKeyA],ch=api[P.driveKeyB];let lastSym=null;
 const fromDom=async()=>{const d=await disc(page);
  if(d.A&&d.A.rows.length)fw={...fw,rows:d.A.rows.map(r=>({symbol:r.sym,match_id:null,timeframe:fw.rows[0]&&fw.rows[0].timeframe}))};
  if(d.B&&d.B.rows.length)ch={...ch,rows:d.B.rows.map(r=>({symbol:r.sym,match_id:null,timeframe:ch.rows[0]&&ch.rows[0].timeframe}))};};
 await check('6.1',vp.key,`Click A row → Chart + Backtest show it with "from: … (A)", canvas drawn, evidence basis, "Costs 0.40% included"`,page,async()=>{
  await discoverReady(page);await fromDom();const sym=fw.rows[0].symbol;await rowsLoc(page,'A').nth(0).click();
  const r=await waitLinked(page,sym,{slot:'A'});const d=await disc(page);
  const out={...r,rowSelected:d.A.rows[0].sel,shot:await shot(page,vp.key,'06-linked-A')};if(!r.costLine||!out.rowSelected)throw new Error(short(out));return out;});
 await check('6.2',vp.key,'Click B row → both cards switch to B stock with "(B)"; A row deselected',page,async()=>{
  await fromDom();const sym=ch.rows[0].symbol;await rowsLoc(page,'B').nth(0).click();const r=await waitLinked(page,sym,{slot:'B'});const d=await disc(page);
  const out={...r,bRowSelected:d.B.rows[0].sel,aSelected:d.A.rows.filter(x=>x.sel).map(x=>x.sym),shot:await shot(page,vp.key,'06-linked-B')};
  if(!out.bRowSelected||out.aSelected.length)throw new Error(short(out));lastSym=sym;return out;});
 await check('6.3',vp.key,'Rapid-click 5 A rows within ~1 s → final chart + backtest = last click, no stale mix after settling',page,async()=>{
  await fromDom();const n=Math.min(5,fw.rows.length-1),clicked=[];const t0=Date.now();
  for(let i=1;i<=n;i++){await rowsLoc(page,'A').nth(i).click({timeout:3000});clicked.push(fw.rows[i].symbol);await sleep(120);}
  const clickMs=Date.now()-t0,last=clicked[clicked.length-1];const samples=[];
  for(let k=0;k<30;k++){const d=await disc(page);samples.push({t:Date.now()-t0,chart:chartSym(d),bt:btSym(d),canvas:d.chart&&d.chart.canvasLabel?d.chart.canvasLabel.split(' ')[0]:null});await sleep(100);}
  extra[vp.key+'-rapid']={clicked,samples};
  const firstGood=samples.findIndex(s=>s.chart===last&&s.bt===last);const after=firstGood<0?[]:samples.slice(firstGood);
  const regress=after.filter(s=>s.chart!==last||s.bt!==last||(s.canvas&&s.canvas!==last));const mixed=samples.filter(s=>s.chart&&s.bt&&s.chart!==s.bt);
  const r=await waitLinked(page,last,{slot:'A',timeout:10000});
  const out={clicked,clickMs,settledAtMs:firstGood<0?null:samples[firstGood].t,regressionsAfterSettle:regress.length,chartVsBacktestMismatchSamples:mixed.length,final:r,shot:await shot(page,vp.key,'06-rapid-final')};
  if(clickMs>1600||firstGood<0||regress.length)throw new Error(short(out));lastSym=last;
  return mixed.length?{...out,note:`${mixed.length} transient samples where chart and backtest headers named different stocks while loading`}:out;});
 if(vp.key!=='desktop')return;
 await check('6.4',vp.key,'Global active symbol follows the Discover click (Watchlist workspace shows it)',page,async()=>{
  const sym=lastSym;await page.locator('[role=navigation][aria-label="Main navigation"] [role=link][aria-label^="Watchlist"] >> visible=true').first().click();
  const r=await until(async()=>{const u=new URL(page.url());const t=await page.title();return {ok:u.pathname==='/watch'&&(t.startsWith(sym+' · ')||u.searchParams.get('s')===sym),path:u.pathname,s:u.searchParams.get('s'),title:t};},{timeout:30000,msg:`workspace follows ${sym}`});
  const out={expected:sym,...r,shot:await shot(page,vp.key,'06-global-symbol-watch')};
  await page.locator('[role=navigation][aria-label="Main navigation"] [role=link][aria-label^="Discover Strategies"] >> visible=true').first().click();
  await page.getByRole('heading',{name:'Discover Strategies',level:1}).waitFor({state:'visible',timeout:30000});return out;});
}

async function keyboardChecks(page,vp,api){
 const fw=api[P.driveKeyA];
 await check('7.1',vp.key,'Keyboard: focus row, ↓ moves selection + chart; Enter opens /chart?s=&tf=&m=',page,async()=>{
  await gotoDiscover(page);await ensureDrive(page,vp);await discoverReady(page);
  const d0=await disc(page);if(d0.A.rows.length<2)throw new Error('card A lists fewer than 2 rows');
  const r0=rowsLoc(page,'A').nth(0);await r0.click();await waitLinked(page,d0.A.rows[0].sym,{slot:'A'});await r0.focus();
  await page.keyboard.press('ArrowDown');
  const s1=d0.A.rows[1].sym;const moved=await until(async()=>{const d=await disc(page);return {ok:d.A.rows[1].sel&&d.A.rows[1].focused&&chartSym(d)===s1,selected:d.A.rows.filter(x=>x.sel).map(x=>x.sym),focused:d.A.rows.filter(x=>x.focused).map(x=>x.sym),chart:chartSym(d)};},{timeout:10000,msg:'↓ moved selection'});
  await waitLinked(page,s1,{slot:'A'});const sh=await shot(page,vp.key,'07-keyboard-down');
  await page.keyboard.press('Enter');
  const nav=await until(async()=>{const u=new URL(page.url());return {ok:u.pathname==='/chart'&&u.searchParams.get('s')===s1&&u.searchParams.get('tf')===(d0.A.rows[1].tf||u.searchParams.get('tf'))&&/^[^:]+:[^:]+:/.test(u.searchParams.get('m')||'')&&(u.searchParams.get('m')||'').startsWith(s1+':'),url:u.pathname+u.search};},{timeout:20000,msg:'Enter opened full chart'});
  return {moved,enter:nav.url,expectedM:fw.rows[1].match_id,shot:sh};});
}

async function menuChecks(page,vp){
 await check('8.1',vp.key,`Card ⋮ Sort by Symbol / n reorder rows; back to 95% low (${P.mode==='research'?'researched-history list':'stored list'})`,page,async()=>{
  await gotoDiscover(page);await discoverReady(page);
  // The sorts order the researched-history list; open it first on the researched set. The tab is addressed
  // by what it counts (stocks), not by its wording — same rule as 15.3.
  if(P.mode==='research'){await page.getByRole('tab',{name:TAB_STOCKS}).first().click();
   await until(async()=>{const d=await disc(page);return {ok:d.A.rows.length>0&&d.A.rows.some(r=>r.n!==null),rows:d.A.rows.length};},{timeout:20000,msg:'researched-history rows'});}
  const stable=async()=>P.mode==='research'
   ?(await until(async()=>{const d=await disc(page);return {ok:d.A.rows.length>0,rows:d.A.rows};},{timeout:15000,interval:250,msg:'history rows'}))
   :await waitStableRows(page,'A');
  // The menu names the COLUMN being sorted, and the two lists name their columns differently ("n"/"95% low"
  // on the stored list, "Trades"/"Edge low" on the researched one — cardLogic.STORED_COLUMNS /
  // RESEARCH_COLUMNS). The check is about the ordering, so it addresses the column this mode actually shows.
  await cardMenu(page,'A',/^Sort by Symbol/);await sleep(400);const sym=(await stable()).rows;const bySym=sym.map(r=>r.sym),sortedSym=[...bySym].sort((a,b)=>a.localeCompare(b));
  await cardMenu(page,'A',new RegExp('^Sort by '+rxEsc(P.sortN)));await sleep(400);const byN=(await stable()).rows.map(r=>r.n);const nDesc=byN.every((v,i)=>!i||byN[i-1]>=v);const sh=await shot(page,vp.key,'08-sort-by-n');
  await cardMenu(page,'A',new RegExp('^Sort by '+rxEsc(P.sortLow)));await sleep(400);const low=(await stable()).rows.map(r=>r.lowText);
  const out={bySymbol:bySym.slice(0,6),symbolSorted:JSON.stringify(bySym)===JSON.stringify(sortedSym),byN:byN.slice(0,8),nDesc,backToLow:low.slice(0,5),shot:sh};
  if(!out.symbolSorted||!nDesc)throw new Error(short(out));
  if(P.mode==='research')await page.getByRole('tab',{name:TAB_SETUPS}).first().click();
  return out;});
 await check('8.2',vp.key,'ⓘ popover explains costs 0.40% + next-open entry',page,async()=>{
  const name=(await disc(page)).A.title.replace(/^Scanner A: /,'').replace(/\. Change strategy$/,'');
  await page.getByRole('button',{name:`About ${name}`,exact:true}).click();const dlg=page.getByRole('dialog',{name:`About ${name}`});await dlg.waitFor({state:'visible',timeout:5000});await sleep(250);
  const txt=await dlg.innerText();const sh=await shot(page,vp.key,'08-info-popover');await page.keyboard.press('Escape');await dlg.waitFor({state:'hidden',timeout:4000}).catch(()=>{});
  const out={hasCost:/0\.40% round trip included/.test(txt),hasEntry:/Entry: next open/.test(txt),excerpt:txt.slice(0,300),shot:sh};if(!out.hasCost||!out.hasEntry)throw new Error(short(out));return out;});
 await check('8.3',vp.key,`↻ reloads card A (loading status observed, then ${P.listLabel})`,page,async()=>{
  const name=(await disc(page)).A.title.replace(/^Scanner A: /,'').replace(/\. Change strategy$/,'');
  await page.evaluate(()=>window.__qaRevStart(5000));await page.getByRole('button',{name:`Reload ${name}`,exact:true}).click();
  await sleep(600);await discoverReady(page,{slots:['A']});await sleep(1300);const rev=await page.evaluate(()=>window.__qaRev);
  const seq=rev.map(x=>x.sa),finalStatus=seq[seq.length-1];
  // The reload refreshes BOTH of a research card's sources; `loadingRe` covers either loading line.
  const loadingIdx=seq.findIndex(s=>P.loadingRe.test(s||''));
  const last=rev[rev.length-1],finalReady=revReady(last,'a');
  const out={statusSequence:[...new Set(seq)].slice(0,4),loadingSeenAtMs:loadingIdx<0?null:rev[loadingIdx].t,finalStatus,finalRows:last.a,finalLive:last.la,finalReady,finalAtMs:last.t};
  if(loadingIdx<0||!finalReady)throw new Error(short(out));return out;});
 await check('8.4',vp.key,'Card ⋮ Clear card → empty state "Choose a strategy ▾"',page,async()=>{
  await cardMenu(page,'A','Clear card');
  const r=await until(async()=>{const d=await disc(page);return {ok:d.A&&d.A.title==='Scanner A: choose a strategy'&&d.A.rows.length===0,title:d.A&&d.A.title,text:d.A&&d.A.text};},{timeout:6000,msg:'card A empty'});
  const cta=page.getByRole('button',{name:'Choose a strategy for Scanner A'});await cta.waitFor({state:'visible'});const ctaText=(await cta.innerText()).trim();
  const linked=await disc(page);return {title:r.title,ctaText,cardText:r.text.replace(/\n+/g,' | ').slice(0,200),chartAfterClear:linked.chart&&linked.chart.heading,shot:await shot(page,vp.key,'08-card-cleared')};});
 await check('8.5',vp.key,'Block ⋮ Collapse hides cards, Expand restores; Reset block restores default strategies',page,async()=>{
  await blockMenu(page,'Collapse');const col=await until(async()=>{const d=await disc(page);return {ok:!d.A&&!d.B&&!d.chart,A:!!d.A};},{timeout:5000,msg:'collapsed'});const sh=await shot(page,vp.key,'08-block-collapsed');
  await blockMenu(page,'Expand');await until(async()=>{const d=await disc(page);return {ok:!!d.A&&!!d.B};},{timeout:5000,msg:'expanded'});
  await page.locator(`${SC("B")} [aria-label="Clear Scanner B"]`).first().click();await until(async()=>{const d=await disc(page);return {ok:d.B.title==='Scanner B: choose a strategy'};},{timeout:5000,msg:'B cleared'});
  await blockMenu(page,'Reset block');
  const r=await until(async()=>{const d=await disc(page);return {ok:d.A.title.includes(P.a)&&d.B.title.includes(P.b),A:d.A.title,B:d.B.title};},{timeout:8000,msg:'reset to defaults'});
  return {collapsed:col,afterReset:r,shot:sh};});
}

async function persistenceChecks(page,vp){
 await check('9.1',vp.key,'Change card A strategy, reload → kept',page,async()=>{
  await discoverReady(page);await pickStrategy(page,'A',P.alt,vp.key);await page.reload({waitUntil:'domcontentloaded'});
  const r=await until(async()=>{const d=await disc(page);return {ok:d.A&&d.A.title===`Scanner A: ${P.alt}. Change strategy`,title:d.A&&d.A.title};},{timeout:30000,msg:'A kept after reload'});return r;});
 await check('9.2',vp.key,'Columns ▾ = 2 → grid reflows to 2×2 (measured); persists after reload',page,async()=>{
  const before=await disc(page);await page.getByRole('button',{name:/^Card columns: /}).click();await page.getByRole('menuitemradio',{name:'2 columns',exact:true}).click();
  const two=await until(async()=>{const d=await disc(page);const A=d.A.rect,B=d.B.rect,Ch=d.chart.rect,Bt=d.bt.rect;return {ok:Math.abs(A.y-B.y)<=3&&Ch.y>=A.y+A.h&&Math.abs(Ch.y-Bt.y)<=3&&Math.abs(A.x-Ch.x)<=3,A,B,chart:Ch,bt:Bt};},{timeout:6000,msg:'2×2 after Columns=2'});
  const sh=await shot(page,vp.key,'09-columns-2');await page.reload({waitUntil:'domcontentloaded'});await page.getByRole('heading',{name:'Discover Strategies',level:1}).waitFor({state:'visible',timeout:30000});
  const chip=await until(async()=>{const t=await page.getByRole('button',{name:/^Card columns: /}).getAttribute('aria-label');const d=await disc(page);return {ok:/^Card columns: 2\./.test(t)&&d.chart&&d.A&&d.chart.rect.y>=d.A.rect.y+d.A.rect.h,chip:t};},{timeout:20000,msg:'Columns=2 persisted'});
  await page.getByRole('button',{name:/^Card columns: /}).click();await page.getByRole('menuitemradio',{name:'Auto',exact:true}).click();
  await blockMenu(page,'Reset block');await until(async()=>{const d=await disc(page);return {ok:d.A.title.includes(P.a)&&Math.abs(d.A.rect.y-d.chart.rect.y)<=3};},{timeout:8000,msg:'restored Auto + defaults'});
  return {before:{A:before.A.rect,chart:before.chart&&before.chart.rect},two,persisted:chip.chip,shot:sh};});
}

async function openLinkChecks(page,vp,api){
 const fw=api[P.driveKeyA],sym=fw.rows[0].symbol,tf=fw.rows[0].timeframe;
 await check('10.1',vp.key,`"Open full chart →" → /chart?s=${sym}&tf=${tf}&m=<match>`,page,async()=>{
  await gotoDiscover(page);await ensureDrive(page,vp);await discoverReady(page);await rowsLoc(page,'A').nth(0).click();await waitLinked(page,sym,{slot:'A'});
  await page.getByRole('link',{name:`Open ${sym} ${tf} on the full chart`}).click();
  const r=await until(async()=>{const u=new URL(page.url());const t=await page.title();return {ok:u.pathname==='/chart'&&u.searchParams.get('s')===sym&&u.searchParams.get('tf')===tf&&u.searchParams.get('m')===fw.rows[0].match_id&&t.startsWith(sym+' · '),url:u.pathname+u.search,title:t};},{timeout:45000,msg:'full chart'});
  return {...r,shot:await shot(page,vp.key,'10-open-full-chart')};});
 await check('10.2',vp.key,`"Open evidence & replay →" → /chart?…&tab=evidence with Evidence/Replay selected`,page,async()=>{
  await gotoDiscover(page);await ensureDrive(page,vp);await discoverReady(page);await rowsLoc(page,'A').nth(0).click();await waitLinked(page,sym,{slot:'A'});
  const hist=[];const onNav=f=>{if(f===page.mainFrame())hist.push(new URL(f.url()).pathname+new URL(f.url()).search);};page.on('framenavigated',onNav);
  await page.getByRole('link',{name:`Open evidence and replay for ${sym} ${tf}`}).click();
  const r=await until(async()=>{const u=new URL(page.url());const tabs=await page.locator('[role=tab][aria-selected="true"]').evaluateAll(els=>els.filter(e=>e.getClientRects().length).map(e=>e.getAttribute('aria-label')));
   return {ok:u.pathname==='/chart'&&u.searchParams.get('s')===sym&&u.searchParams.get('tf')===tf&&tabs.includes('Evidence/Replay'),url:u.pathname+u.search,tabs};},{timeout:45000,msg:'evidence tab'});
  await sleep(1500);page.off('framenavigated',onNav);
  const finalUrl=new URL(page.url()),pushedTab=hist.some(h=>/[?&]tab=evidence\b/.test(h));
  const out={...r,finalUrl:finalUrl.pathname+finalUrl.search,urlHistory:hist,linkPushedTabEvidence:pushedTab,shot:await shot(page,vp.key,'10-open-evidence')};
  // Fresh page load before the click, so a selected Evidence/Replay tab can only come from the link. MainWorkspace.tsx:76 clears ?tab= after opening it (by design),
  // often before framenavigated reports the pushed URL, so the transient tab param is recorded but not required.
  return finalUrl.searchParams.get('tab')!=='evidence'?{...out,note:'Evidence/Replay tab opens; the workspace then drops tab=evidence from the URL (MainWorkspace.tsx:76, router.setParams)'}:out;});
}

async function phoneSheetChecks(page,vp,api){
 const fw=api[P.driveKeyA],sym=fw.rows[0].symbol;
 await check('11.1',vp.key,'Phone: tapping a stock opens a bottom sheet with chart + backtest; close works; reopen button works',page,async()=>{
  await ensureDrive(page,vp);
  await gotoDiscover(page);await discoverReady(page);await rowsLoc(page,'A').nth(0).tap();
  const s=await until(async()=>{const d=await disc(page);return {ok:!!d.sheet,sheet:d.sheet};},{timeout:8000,msg:'bottom sheet opens'});
  const r=await waitLinked(page,sym,{slot:'A'});await sleep(500);const sh=await shot(page,vp.key,'11-sheet-open');
  const sheetBox=await page.locator('[role=region][aria-label^="Chart and backtest"]').boundingBox();
  await page.getByRole('button',{name:'Close chart and backtest'}).tap();await until(async()=>{const d=await disc(page);return {ok:!d.sheet};},{timeout:5000,msg:'sheet closed'});
  const reopen=page.getByRole('button',{name:`Show chart and backtest for ${sym}`});await reopen.scrollIntoViewIfNeeded();await reopen.tap();
  await until(async()=>{const d=await disc(page);return {ok:!!d.sheet};},{timeout:5000,msg:'sheet reopened'});await sleep(400);const sh2=await shot(page,vp.key,'11-sheet-reopened');
  await page.getByRole('button',{name:'Close chart and backtest'}).tap();await until(async()=>{const d=await disc(page);return {ok:!d.sheet};},{timeout:5000,msg:'sheet closed again'});
  return {sheet:s.sheet,sheetBox,linked:r,shot:sh,shotReopen:sh2};});
}

async function deepLinkCheck(page,vp,api){
 const fw=api[P.driveKeyA],row=fw.rows[Math.min(3,fw.rows.length-1)],sym=row.symbol;
 await check('12.1',vp.key,`Deep link /discover?b=${P.blockKey}&a=${P.driveKeyA}&sel=A&s=${sym} → selected, chart + backtest`,page,async()=>{
  await clearDiscoverStore(page);await gotoDiscover(page,`?b=${P.blockKey}&a=${P.driveKeyA}&sel=A&s=${sym}`);
  const r=await until(async()=>{const d=await disc(page);const sel=d.A?d.A.rows.filter(x=>x.sel).map(x=>x.sym):[];return {ok:sel.length===1&&sel[0]===sym&&chartSym(d)===sym&&btSym(d)===sym&&(vp.key!=='phone'||!!d.sheet),selected:sel,chart:chartSym(d),bt:btSym(d),sheet:d.sheet};},{timeout:45000,msg:'deep link applied'});
  const l=await waitLinked(page,sym,{slot:'A'});await sleep(400);return {...r,linked:l,shot:await shot(page,vp.key,'12-deeplink')};});
}

async function adminChecks(browser,ownerState,memberState){
 const vp={key:'desktop',width:1440,height:900};const tag='admin-owner';const {ctx,page,log}=await newPage(browser,ownerState,vp,tag);
 try{
  await check('13.1','admin','Owner /admin/strategies lists blocks (chart enabled; quant/results/options disabled) and strategies',page,async()=>{
   await page.goto(BASE+'/admin/strategies',{waitUntil:'domcontentloaded',timeout:60000});await page.getByRole('heading',{name:'Manage strategies',level:1}).waitFor({state:'visible',timeout:60000});
   await page.getByRole('region',{name:'Block Chart Strategies'}).waitFor({state:'visible',timeout:30000});
   const blocks=await page.locator('[role=switch][aria-label^="Block "]').evaluateAll(els=>els.map(e=>({label:e.getAttribute('aria-label'),checked:e.getAttribute('aria-checked')})));
   const rows=await page.locator('[role=table][aria-label="Strategies in Chart Strategies"] [role=row]').count();
   const out={blocks,chartStrategyRows:rows-1,shot:await shot(page,'admin','13-admin-list')};
   const st=Object.fromEntries(blocks.map(b=>[b.label.replace(/^Block /,'').replace(/: (enabled|disabled)$/,''),b.checked]));
   if(st['Chart Strategies']!=='true'||st['Quant Strategies']!=='false'||st['Results & Events']!=='false'||st['Options Strategies']!=='false'||out.chartStrategyRows<52)throw new Error(short(out));return out;});
  // The admin preview drives the STORED strategy in both modes. On the researched set that scan has no
  // current matches, so the preview must still open and report a measured zero with its reason - a 503 there
  // would take the whole admin page down although nothing is actually broken.
  await check('13.2','admin',`Preview Falling Wedge breakout · 1D (${P.mode}) opens and agrees with the API`,page,async()=>{
   await page.getByRole('button',{name:'Preview Falling Wedge breakout · 1D',exact:true}).click();const dlg=page.getByRole('dialog',{name:'Preview · Falling Wedge breakout · 1D'});await dlg.waitFor({state:'visible',timeout:8000});
   const api=await apiResults(page,'falling_wedge-1D-long');
   let n=null;
   if(api.total>0){const tbl=dlg.getByRole('table',{name:'Top 5 by 95% low'});await tbl.waitFor({state:'visible',timeout:20000});n=(await tbl.locator('[role=row]').count())-1;}
   else await sleep(800);
   const txt=await dlg.innerText();const found=+((txt.match(/Found\s*\n?\s*(\d+)/)||[])[1]);
   const sh=await shot(page,'admin','13-admin-preview');await page.getByRole('button',{name:'Close Preview · Falling Wedge breakout · 1D'}).click();await dlg.waitFor({state:'hidden',timeout:5000});
   const out={mode:P.mode,found,apiStatus:api.status,apiTotal:api.total,apiUnavailable:api.unavailable||null,top5Rows:n,shot:sh};
   if(api.status!==200)throw new Error('the stored preview must not fail when its detector is not running: '+short(out));
   if(found!==api.total)throw new Error(short(out));
   if(api.total>0&&n!==Math.min(5,api.total))throw new Error(short(out));
   if(P.mode==='research'&&!api.unavailable)throw new Error('a superseded stored scan must carry its reason: '+short(out));
   return out;});
  await check('13.3','admin',`Add strategy through the admin UI with an explicit unique key (${QA_KEY}), disabled`,page,async()=>{
   await page.getByRole('button',{name:'Add strategy',exact:true}).first().click();const dlg=page.getByRole('dialog',{name:'Add strategy'});await dlg.waitFor({state:'visible',timeout:8000});
   const keyField=await dlg.getByRole('textbox',{name:/^Key\b/}).count();
   await dlg.getByRole('textbox',{name:'Name',exact:true}).fill(QA_NAME);
   const patterns=await dlg.locator('[role=radiogroup][aria-label^="Pattern"] [role=button]').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));
   const fwChip=dlg.locator('[role=radiogroup][aria-label^="Pattern"] [role=button][aria-label="Falling Wedge"]');if(await fwChip.count())await fwChip.click();
   await dlg.getByRole('textbox',{name:'Tags (comma separated)'}).fill('Chart patterns, Bullish, 1D, QA');
   await page.getByRole('button',{name:'Save as disabled',exact:true}).click();await sleep(700);
   const alerts=await dlg.locator('[role=alert]').allInnerTexts();const confirm=page.getByRole('dialog',{name:/^Save “/});const confirmOpen=await confirm.count();let saveResult=null;
   if(confirmOpen){await confirm.getByRole('button',{name:'Save as disabled',exact:true}).click();await sleep(1500);saveResult=await dlg.locator('[role=alert]').allInnerTexts().catch(()=>null);}
   const sh=await shot(page,'admin','13-admin-add-ui');
   if(await dlg.isVisible())await page.getByRole('button',{name:'Close Add strategy'}).click().catch(()=>{});
   const reg=await page.evaluate(async k=>{const r=await fetch('/api/admin/strategies',{credentials:'include'});const b=await r.json();return b.blocks.flatMap(x=>x.strategies).filter(s=>s.key===k||s.name.startsWith('QA ')).map(s=>({key:s.key,name:s.name,enabled:s.enabled}));},QA_KEY);
   const out={keyFieldInForm:keyField>0,patternChips:patterns,validationAlerts:alerts,confirmOpened:!!confirmOpen,saveResult,registryAfter:reg,shot:sh};
   if(!keyField||!reg.some(s=>s.key===QA_KEY))throw new Error('admin UI cannot add a strategy with an explicit unique key: '+short(out));return out;});
  await check('13.4','admin',`Fallback: create ${QA_KEY} via owner API (disabled) → appears in admin list after Refresh`,page,async()=>{
   page.__expect.push('/api/admin/strategies');
   const created=await page.evaluate(async({key,name})=>{const me=await (await fetch('/api/auth/me',{credentials:'include'})).json();const h={'content-type':'application/json','x-kanida-csrf':me.csrf};
    const r=await fetch('/api/admin/strategies',{method:'POST',credentials:'include',headers:h,body:JSON.stringify({key,block_key:'chart',name,description:'QA acceptance fixture strategy (kept disabled).',tags:['Chart patterns','Bullish','1D','QA'],source_type:'stored_pattern',pattern:'falling_wedge',timeframe:'1D',side:'long',audience:'trader',min_trades:10,default_slot:null,enabled:false})});
    const b=await r.json().catch(()=>null);const out={status:r.status,code:b&&b.code,enabled:b&&b.enabled};
    if(r.status===409){const p=await fetch('/api/admin/strategies/'+encodeURIComponent(key),{method:'PATCH',credentials:'include',headers:h,body:JSON.stringify({enabled:false})});out.patchDisable=p.status;}return out;},{key:QA_KEY,name:QA_NAME});
   await page.getByRole('button',{name:'Refresh',exact:true}).click();
   const sw=page.getByRole('switch',{name:`${QA_NAME}: disabled`});await sw.waitFor({state:'visible',timeout:15000});await sw.scrollIntoViewIfNeeded();await sleep(300);
   const out={created,switch:`${QA_NAME}: disabled`,shot:await shot(page,'admin','13-admin-qa-added')};if(!(created.status===200||created.status===201||created.status===409))throw new Error(short(out));return out;});
  // The QA strategy belongs to the STORED block, so its picker is that block's. On the researched set that
  // block starts collapsed behind its one-line note, so it is opened first - the assertion is unchanged.
  const storedPicker=async()=>{
   await gotoDiscover(page);
   const card=page.locator('[role=region][aria-label^="Scanner A, Falling Wedge breakout"]').first();
   const note=page.getByRole('button',{name:/These 10 stored patterns are part of it/}).first();
   await until(async()=>{const c=await card.count(),n=await note.count();
    if(!c&&n){await note.click();await sleep(700);}
    return {ok:(await card.count())>0,card:c,note:n};},{timeout:40000,interval:500,msg:'stored block card'});
   await card.waitFor({state:'visible',timeout:15000});
   // Ensure-open, not click-and-hope: the card title toggles, so a picker left open by an earlier step would
   // be CLOSED by a bare click here. Same rule as openPicker, against the stored block's own card.
   const dlg=page.getByRole('dialog',{name:'Choose a strategy for Scanner A'}).last();
   if(!(await dlg.isVisible().catch(()=>false)))await card.locator('[aria-label^="Scanner A:"]').first().click();
   await dlg.waitFor({state:'visible',timeout:8000});return dlg;};
  await check('13.5','admin','Enable it via toggle (confirm) → appears in the Discover picker (with its "QA" chip)',page,async()=>{
   await page.getByRole('switch',{name:`${QA_NAME}: disabled`}).click();const conf=page.getByRole('dialog',{name:`Enable “${QA_NAME}”?`});await conf.waitFor({state:'visible',timeout:5000});
   await conf.getByRole('button',{name:'Enable strategy',exact:true}).click();await page.getByRole('switch',{name:`${QA_NAME}: enabled`}).waitFor({state:'visible',timeout:15000});
   const dlg=await storedPicker();
   await dlg.getByRole('textbox',{name:'Search strategies'}).fill('QA');await sleep(300);const rows=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));
   await dlg.getByRole('textbox',{name:'Search strategies'}).fill('');const chip=await dlg.getByRole('button',{name:'Filter: QA'}).count();const sh=await shot(page,'admin','13-discover-picker-qa-enabled');await page.keyboard.press('Escape');
   const out={pickerRows:rows,qaChip:chip>0,shot:sh};if(!rows.some(l=>l.startsWith(QA_NAME+'. ')))throw new Error(short(out));return out;});
  await check('13.6','admin','Disable it again → gone from the Discover picker',page,async()=>{
   await page.goto(BASE+'/admin/strategies',{waitUntil:'domcontentloaded'});const sw=page.getByRole('switch',{name:`${QA_NAME}: enabled`});await sw.waitFor({state:'visible',timeout:30000});await sw.click();
   const conf=page.getByRole('dialog',{name:`Disable “${QA_NAME}”?`});await conf.waitFor({state:'visible',timeout:5000});await conf.getByRole('button',{name:'Disable strategy',exact:true}).click();
   await page.getByRole('switch',{name:`${QA_NAME}: disabled`}).waitFor({state:'visible',timeout:15000});
   const dlg=await storedPicker();
   await dlg.getByRole('textbox',{name:'Search strategies'}).fill('QA');await sleep(300);const rows=await pickerRows(dlg).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));const empty=await dlg.getByText('No strategy matches this search.').count();
   await page.keyboard.press('Escape');const out={pickerRows:rows,emptyMessage:empty>0};if(rows.some(l=>l.startsWith(QA_NAME+'. ')))throw new Error(short(out));return out;});
  await check('14.x','admin','Console (owner admin session): no pageerror / console.error',page,async()=>{if(log.errors.length)throw new Error(`${log.errors.length} error(s): `+short(log.errors.slice(0,5)));return {errors:0,expectedNetworkErrors:log.expected.length};});
 }finally{await ctx.close();}
 if(!memberState){results.push({id:'13.7',vp:'admin',name:'Member session blocked',status:'FAIL',detail:'member session could not be created'});return;}
 const m=await newPage(browser,memberState,vp,'admin-member');
 try{
  await check('13.7','admin','Member /admin/strategies shows "Only the owner can manage strategies"; API returns 403',m.page,async()=>{
   m.page.__expect.push('/api/admin/');
   await m.page.goto(BASE+'/admin/strategies',{waitUntil:'domcontentloaded',timeout:60000});await m.page.getByText('Only the owner can manage strategies').waitFor({state:'visible',timeout:45000});
   const api=await m.page.evaluate(async()=>{const me=await (await fetch('/api/auth/me',{credentials:'include'})).json();const g=await fetch('/api/admin/strategies',{credentials:'include'});
    const p=await fetch('/api/admin/strategies/falling_wedge-1D-long/preview',{method:'POST',credentials:'include',headers:{'content-type':'application/json','x-kanida-csrf':me.csrf},body:'{}'});
    const b=await fetch('/api/admin/blocks',{credentials:'include'});return {role:me.user&&me.user.role,getStrategies:g.status,postPreview:p.status,getBlocks:b.status};});
   const out={...api,shot:await shot(m.page,'admin','13-member-blocked')};if(api.role!=='member'||api.getStrategies!==403||api.postPreview!==403||api.getBlocks!==403)throw new Error(short(out));return out;});
  await check('14.x','admin-member','Console (member admin page): no pageerror / console.error (excluding the intentional 403 probes)',m.page,async()=>{if(m.log.errors.length)throw new Error(`${m.log.errors.length} error(s): `+short(m.log.errors.slice(0,5)));return {errors:0,expected403:m.log.expected.length};});
 }finally{await m.ctx.close();}
}

// ---------------- 15: research pattern set only ----------------
// These assert the behaviour that only EXISTS on the researched set, so they are additions, never substitutes:
// the stored block is superseded rather than broken, and the leading block lists today's live detections with
// their own lifecycle state and evidence identity.
async function researchModeChecks(page,vp){
 await check('15.1',vp.key,'Stored block is collapsed behind one line (superseded, not broken) and still expands',page,async()=>{
  await clearDiscoverStore(page);await gotoDiscover(page);await discoverReady(page);
  const note=page.getByRole('button',{name:/These 10 stored patterns are part of it/});
  await note.waitFor({state:'visible',timeout:20000});
  const text=(await note.innerText()).trim();
  const cardsBefore=await page.locator('[role=region][aria-label^="Scanner A, Falling Wedge breakout"]').count();
  await note.click();await sleep(600);
  const cardsAfter=await page.locator('[role=region][aria-label^="Scanner A, Falling Wedge breakout"]').count();
  const out={note:text,storedCardsWhenCollapsed:cardsBefore,storedCardsWhenExpanded:cardsAfter,shot:await shot(page,vp.key,'15-stored-collapsed')};
  if(cardsBefore!==0)throw new Error('the stored block was not collapsed: '+short(out));
  if(cardsAfter===0)throw new Error('"Show anyway" did not expand the stored block: '+short(out));
  if(!/researched 107-pattern set/.test(text))throw new Error(short(out));
  return out;});
 await check('15.2',vp.key,'Expanded stored cards state the reason, never a generic error',page,async()=>{
  const el=page.locator('[role=region][aria-label^="Scanner A, Falling Wedge breakout"]').first();
  await el.waitFor({state:'visible',timeout:20000});
  // The card loads its own results; wait until it has settled (no in-flight Stop) before reading its reason.
  const settled=await until(async()=>{const x=(await el.innerText()).replace(/\s+/g,' ').trim();return {ok:!/Stop/.test(x)&&/researched pattern set/.test(x),text:x.slice(0,300)};},{timeout:30000,msg:'stored card settled with its reason'}).catch(e=>({text:String(e.message).slice(0,300)}));
  const text=(await el.innerText()).replace(/\s+/g,' ').trim();
  const out={text:text.slice(0,400),shot:await shot(page,vp.key,'15-stored-expanded')};
  if(/Results unavailable|No list is shown while results are unavailable/.test(text))throw new Error('generic error shown for a superseded scan: '+short(out));
  if(!/researched pattern set/.test(text))throw new Error(short(out));
  await clearDiscoverStore(page);await gotoDiscover(page);await discoverReady(page);
  return out;});
 await check('15.3',vp.key,'Research card: the live list is the default tab, its count = the active list, rows are live with an age',page,async()=>{
  const d=await disc(page);
  const tabs=await page.locator('[role=tab]').evaluateAll(els=>els.map(e=>({label:e.getAttribute('aria-label'),selected:e.getAttribute('aria-selected')})));
  // Identified by what each tab COUNTS, not by its wording (TAB_SETUPS / TAB_STOCKS above).
  const live=tabs.filter(x=>TAB_SETUPS.test(x.label||'')),hist=tabs.filter(x=>TAB_STOCKS.test(x.label||''));
  const researched=hist.map(x=>tabN(x.label,TAB_STOCKS));
  const out={tabs:tabs.slice(0,4),rows:d.A.rows.slice(0,3),status:d.A.status,shot:await shot(page,vp.key,'15-research-card')};
  if(!live.length||!hist.length)throw new Error('the two lists are not both offered: '+short(out));
  if(live[0].selected!=='true')throw new Error('the active list is not the default: '+short(out));
  // The tab's count is the ACTIVE book and must equal what the active list reports - no mixing with today.
  const activeApi=await apiDetections(page,P.keyA);
  const liveN=tabN(live[0].label,TAB_SETUPS),cut=cutState(d.A);
  // The status line no longer repeats the count; when it reports a CUT, that cut's total is the same book.
  if(liveN!==activeApi.total||(cut&&cut.total!==activeApi.total))throw new Error('active counts disagree: '+short({liveN,cut,api:activeApi.total}));
  // The regression this guards: the researched-history tab read "(0)" although the index holds every stock.
  if(!researched.length||!researched.every(n=>n>0))throw new Error('researched-history count is empty: '+short({hist}));
  if(!d.A.rows.length||!d.A.rows[0].state||!d.A.rows[0].detected)throw new Error('live rows carry no state/time: '+short(out));
  return out;});
 await check('15.4',vp.key,'Evidence card opens from a live detection with its identity and the contract label',page,async()=>{
  const d0=await disc(page);const sym=d0.A.rows[0].sym;
  await rowsLoc(page,'A').nth(0).click();
  const r=await until(async()=>{const d=await disc(page);return {ok:chartSym(d)===sym&&!!d.bt&&/Evidence · /.test(d.bt.text||''),chart:chartSym(d),bt:d.bt&&d.bt.heading};},{timeout:25000,msg:`evidence card for ${sym}`});
  const LABELS=['Historical walk-forward result','Limited historical sample','No selected walk-forward trades',
   'No occurrences in this historical sample','Not enough historical data','Incompatible historical evidence',
   'Historical data requires review','Evidence loading'];
  await until(async()=>{const d=await disc(page);const x=(d.bt&&d.bt.text)||'';
   return {ok:LABELS.some(l=>x.includes(l)),text:x.slice(0,160)};},{timeout:25000,msg:'evidence card label'});
  const bt=(await disc(page)).bt;const text=(bt.text||'').replace(/\s+/g,' ');
  const labels=['Historical walk-forward result','Limited historical sample','No selected walk-forward trades','No occurrences in this historical sample','Not enough historical data','Incompatible historical evidence','Historical data requires review','Evidence loading'];
  const shown=labels.filter(l=>text.includes(l));
  const out={symbol:sym,...r,labelsShown:shown,detected:/Detected /.test(text),shot:await shot(page,vp.key,'15-live-evidence')};
  if(!shown.length)throw new Error('the evidence card shows no contract label: '+short({text:text.slice(0,300)}));
  if(!out.detected)throw new Error('the evidence card does not name the detection it was opened from: '+short(out));
  return out;});
}

async function researchScopeChecks(page,vp,catalog){
 const block=catalog.blocks.find(b=>b.key===P.blockKey);
 await check('15.5',vp.key,'"Spotted today" chip narrows the active tab to the today count, and back',page,async()=>{
  await clearDiscoverStore(page);await gotoDiscover(page);await discoverReady(page);
  const chip=page.locator(`${SC('A')} [role=checkbox][aria-label^="Spotted today"]`).first();
  await chip.waitFor({state:'visible',timeout:15000});
  const label=await chip.getAttribute('aria-label');const chipN=+((label.match(/\((\d+)\)/)||[])[1]||'NaN');
  const today=await page.evaluate(async k=>{const r=await fetch(`/api/strategies/${encodeURIComponent(k)}/detections?scope=today&limit=100`,{credentials:'include'});return (await r.json()).total;},P.keyA);
  await chip.click();
  const on=await until(async()=>{const d=await disc(page);const n=+(((d.A.status||'').match(/^(\d+) (?:spotted today|today)/)||[])[1]||'NaN');
   const empty=/No stock is spotted today|^0 today/.test(d.A.status||'');return {ok:n===today||(today===0&&empty),n,status:d.A.status};},{timeout:15000,msg:'today scope applied'});
  await chip.click();
  const back=await until(async()=>{const d=await disc(page);return {ok:/^\d+ active|^No active setup/.test(d.A.status||''),status:d.A.status};},{timeout:15000,msg:'back to active'});
  const out={chipLabel:label,chipN,apiToday:today,on,back};
  if(chipN!==today)throw new Error('chip count != API today total: '+short(out));
  return out;});
 await check('15.6',vp.key,'A strategy with no active setup says so, names when it was last seen, and offers the researched history',page,async()=>{
  const idle=block.strategies.find(s=>(s.detections_live||0)===0&&s.detections_last_detected)||block.strategies.find(s=>(s.detections_live||0)===0);
  if(!idle)return {info:'every strategy in this block has an active setup; nothing to assert'};
  await clearDiscoverStore(page);await gotoDiscover(page);
  await pickStrategy(page,'A',idle.name,vp.key);
  // "The card says there is nothing standing right now" — matched by meaning (a "no …" line that is not the
  // loading, error or cut line), not by one phrasing. Today: "No setup right now · last seen 16 Sep."
  const r=await until(async()=>{const d=await disc(page);const s=d.A.status||'';
   return {ok:/^No\b/i.test(s)&&!P.loadingRe.test(s)&&!cutState(d.A),status:s};},{timeout:20000,msg:'no-active empty state'});
  const full=(await disc(page)).A.text||'';
  if(idle.detections_last_detected&&!/last seen \d{1,2} \w{3}/.test(full))throw new Error('empty state does not name when it was last seen: '+short({r,text:full.slice(0,240)}));
  // The researched history is reached by the card's second tab. BACKLOG item 2 settled this: "Two tabs only:
  // Now (N) and History (N)" — the separate "Show researched history" button was folded into that tab.
  const btn=page.locator(`${SC('A')} [role=tab]`).filter({hasText:/\d/}).last();
  await btn.waitFor({state:'visible',timeout:8000});await btn.click();
  const hist=await until(async()=>{const tabs=await page.locator(`${SC('A')} [role=tab]`).evaluateAll(els=>els.map(e=>({l:e.getAttribute('aria-label'),s:e.getAttribute('aria-selected')})));
   return {ok:tabs.some(x=>TAB_STOCKS.test(x.l||'')&&x.s==='true'),tabs};},{timeout:15000,msg:'switched to researched history'});
  return {strategy:idle.name,lastDetected:idle.detections_last_detected||null,empty:r,switched:hist.ok,shot:await shot(page,vp.key,'15-no-active')};});
}

async function runViewport(browser,state,vp){
 const {ctx,page,log}=await newPage(browser,state,vp,vp.key);
 try{
  await navChecks(page,vp);
  let catalog=null,api={};
  await check('3.0',vp.key,'Catalog + results API reachable from the page',page,async()=>{
   catalog=await apiCatalog(page);
   // The STORED results endpoint must answer 200 in EITHER mode. On the researched set it legitimately has no
   // current matches, and it says so in `unavailable` instead of failing - a 503 there would take the admin
   // page, the preview and every stored read down with it although nothing is broken.
   const stored=await apiResults(page,'falling_wedge-1D-long');
   for(const k of [P.keyA,P.keyB])api[k]=await apiRows(page,k);
   if(catalog.status!==200||stored.status!==200||api[P.keyA].status!==200)throw new Error(short({catalog:catalog.status,stored:stored.status,a:api[P.keyA].status}));
   if(P.mode==='research'&&!stored.unavailable)throw new Error('the superseded stored scan must serve its reason: '+short(stored));
   const block=catalog.blocks.find(b=>b.key===P.blockKey);
   if(!block||block.strategies.length<P.minBlockStrategies)throw new Error(short({block:block&&block.strategies.length}));
   if(P.mode==='research'){
    // The row-driven checks need a list with rows in it. The registry defaults are asserted as defaults by
    // 4.1/4.2; the checks that CLICK rows are driven by the two strategies detecting the most today.
    const busiest=[...block.strategies].filter(s=>(s.detections_live||0)>=6).sort((a,b)=>(b.detections_live||0)-(a.detections_live||0));
    if(busiest.length>=2){P.driveA=busiest[0].name;P.driveKeyA=busiest[0].key;P.driveB=busiest[1].name;P.driveKeyB=busiest[1].key;}
    for(const k of [P.driveKeyA,P.driveKeyB])if(!api[k])api[k]=await apiRows(page,k);
   }
   return {pattern_set:catalog.pattern_set,data_end:catalog.data_end,age_days:catalog.age_days,stale:catalog.stale,universe:catalog.universe,
    blockStrategies:block.strategies.length,storedStatus:stored.status,storedUnavailable:stored.unavailable||null,
    aTotal:api[P.keyA].total,bTotal:api[P.keyB].total};});
  if(!catalog)return;
  if(!api[P.driveKeyA]||!api[P.driveKeyA].rows.length)throw new Error(`${P.driveKeyA} lists no rows in ${P.mode} mode; the suite has nothing to drive`);
  const ws=await apiWorkspaceMatches(page);
  if(!ws.length)throw new Error('the workspace holds no matches; /chart has nothing to resolve');
  await redirectChecks(page,vp,ws[0].symbol,ws[0].timeframe||'1D');
  await discoverLoad(page,vp,catalog);
  await layoutChecks(page,vp);
  await scannerDefaultChecks(page,vp,catalog,api);
  await pickerChecks(page,vp,catalog,vp.key==='desktop');
  // Both cards now hold the strategies the row-driven checks drive (identical to the registry defaults in
  // legacy mode, so nothing about that run changes).
  if(P.driveKeyA!==P.keyA)await pickStrategy(page,'A',P.driveA,vp.key).catch(()=>{});
  if(P.driveKeyB!==P.keyB)await pickStrategy(page,'B',P.driveB,vp.key).catch(()=>{});
  if(vp.key!=='phone')await linkingChecks(page,vp,api);
  if(vp.key==='desktop'){await keyboardChecks(page,vp,api);await menuChecks(page,vp);await persistenceChecks(page,vp);await openLinkChecks(page,vp,api);}
  if(vp.key==='phone')await phoneSheetChecks(page,vp,api);
  if(vp.key==='desktop'||vp.key==='phone')await deepLinkCheck(page,vp,api);
  if(P.mode==='research'&&vp.key==='desktop'){await researchModeChecks(page,vp);await researchScopeChecks(page,vp,catalog);}
  await check('14',vp.key,'Console: no pageerror / console.error on any page',page,async()=>{if(log.errors.length)throw new Error(`${log.errors.length} error(s): `+short(log.errors.slice(0,5)));return {errors:0};});
 }finally{await ctx.close();}
}

(async()=>{
 const started=new Date().toISOString();
 console.log(`Running against: ${PROVENANCE}`);
 try{const r=await fetch(BASE+'/health');if(!r.ok)throw new Error('status '+r.status);}catch(e){console.error('QA server on 8083 is not responding: '+e.message+' (not restarting it).');process.exit(3);}
 const browser=await chromium.launch({channel:'msedge',headless:true});let state=null,memberState=null;
 try{
  try{state=await login(browser,EMAIL,PASSWORD);results.push({id:'0.1',vp:'all',name:'Owner fixture sign-in via /signin',status:'PASS',detail:EMAIL});console.log('PASS owner login');}
  catch(e){results.push({id:'0.1',vp:'all',name:'Owner fixture sign-in via /signin',status:'FAIL',detail:e.message});console.log('FAIL owner login: '+e.message);}
  // Which detector set the scanner runs decides which block leads the page and what its rows mean, and the
  // in-page probes are parameterised by it, so it is resolved ONCE before any instrumented page exists.
  if(state){
   const ctx=await browser.newContext({storageState:state,viewport:{width:1280,height:900}});const probe=await ctx.newPage();
   try{await probe.goto(BASE+'/discover',{waitUntil:'domcontentloaded',timeout:60000});
    const blockKeys={};for(const [k,m] of Object.entries(MODES))blockKeys[k]=m.blockKey;
    const info=await probe.evaluate(async keys=>{const r=await fetch('/api/strategies/catalog',{credentials:'include'});const c=await r.json();
     const set=c&&c.pattern_set,blk=(c.blocks||[]).find(x=>x.key===keys[set])||null;
     const pick=slot=>{const s=((blk&&blk.strategies)||[]).find(x=>x.default_slot===slot);return s?{key:s.key,name:s.name}:null};
     return {set,A:pick('A'),B:pick('B')};},blockKeys);
    const set=info.set;
    P=MODES[set]||MODES.legacy;
    // The block's DEFAULT slots are registry DATA and they move (a new strategy, a changed default_slot).
    // Checks that mean "the block's defaults" must read them, not hard-code two names that were the defaults
    // the day the suite was written — that is what made 8.5, 9.2 and 15.3 fail on data rather than on
    // behaviour. 4.1/4.2 already read them; these keep the rest of the suite in step.
    if(info.A&&info.B){P.a=info.A.name;P.keyA=info.A.key;P.b=info.B.name;P.keyB=info.B.key;
     P.driveA=P.a;P.driveKeyA=P.keyA;P.driveB=P.b;P.driveKeyB=P.keyB;}
    results.push({id:'0.3',vp:'all',name:`Scanner pattern set: ${P.mode} (leading block "${P.block}", defaults A="${P.a}" B="${P.b}")`,status:'PASS',detail:String(set)});
    console.log(`PASS pattern set: ${P.mode} (defaults A="${P.a}" B="${P.b}")`);}
   catch(e){results.push({id:'0.3',vp:'all',name:'Scanner pattern set',status:'FAIL',detail:e.message});}
   finally{await ctx.close();}
  }
  if(state)for(const vp of VIEWPORTS){try{await runViewport(browser,state,vp);}catch(e){results.push({id:'x',vp:vp.key,name:'viewport run crashed',status:'FAIL',detail:e.message});console.log('CRASH '+vp.key+': '+e.stack);}}
  if(state&&!NO_ADMIN){
   try{const mem=await ensureMember();memberState=await login(browser,MEMBER.email,MEMBER.password);results.push({id:'0.2',vp:'admin',name:'Member session (invite → register → onboarding → access, isolated QA DB)',status:'PASS',detail:short(mem)});}
   catch(e){results.push({id:'0.2',vp:'admin',name:'Member session',status:'FAIL',detail:e.message});}
   try{await adminChecks(browser,state,memberState);}catch(e){results.push({id:'x',vp:'admin',name:'admin run crashed',status:'FAIL',detail:e.message});console.log('CRASH admin: '+e.stack);}
  }
 }finally{await browser.close();}
 const fails=results.filter(r=>r.status==='FAIL'),parked=results.filter(r=>r.status==='PARK');
 const stale=results.filter(r=>r.parkedButPassing);
 fs.writeFileSync(path.join(OUT,'results.json'),JSON.stringify({base:BASE,started,finished:new Date().toISOString(),
  script_sha256:SCRIPT_SHA,bundle_mtime:BUNDLE,web_directory:WEB_DIR,
  summary:{pass:results.filter(r=>r.status==='PASS').length,info:results.filter(r=>r.status==='INFO').length,fail:fails.length,
   parked:parked.length,parked_on:PARK_ITEM,parked_passing:stale.map(r=>r.id)},results,console:consoleLog,extra},null,1));
 console.log('\n==== SUMMARY ====');for(const r of results)console.log(`${r.status.padEnd(4)} ${String(r.vp).padEnd(12)} ${r.id.padEnd(5)} ${r.name}${r.status==='FAIL'||r.status==='PARK'?'\n      -> '+String(r.detail).slice(0,400):''}`);
 // "0 failed · 5 parked" so a reader never has to work out whether five reds are a problem.
 console.log(`\n${results.length} checks · ${results.filter(r=>r.status==='PASS').length} passed · ${fails.length} failed`
  +(parked.length?` · ${parked.length} parked on ${PARK_ITEM}`:'')+` · artifacts in qa/discover/`);
 console.log(`Ran against: ${PROVENANCE}`);
 if(parked.length)console.log(`Parked = waiting on a decision, not broken: ${[...new Set(parked.map(r=>r.id))].join(', ')}. `
  +`Signing item 2 off retires 15.1/15.2/15.5 and retargets 13.5/13.6; rejecting it un-parks them.`);
 if(stale.length)console.log(`ATTENTION: parked check(s) ${[...new Set(stale.map(r=>r.id))].join(', ')} now PASS — the behaviour is back, so the park is stale.`);
 process.exitCode=fails.length?1:0;
})().catch(e=>{console.error(e);process.exitCode=1;});
