// Phase 0 (wave 0C) pilot acceptance script — region shell, dock/deep links, symbol propagation, legend key guard, gates, responsiveness.
// Runs ONLY against the local UI-QA server (scripts/qa_server.py, port 8083, isolated var/ui-qa/qa.sqlite3, fictional fixture account).
// Usage (from kanida-app):  node scripts/check-phase0.cjs            all viewports
//                           node scripts/check-phase0.cjs --only=desktop,phone
// Artifacts: qa/phase0/*.png, qa/phase0/results.json, qa/phase0/rapid-switch-samples.json. Exit 1 if any check FAILs.
const fs=require('node:fs'),path=require('node:path');
const {chromium}=require('@playwright/test');
const BASE=process.env.QA_BASE||'http://127.0.0.1:8083';
const QA_PORT=(BASE.match(/:(\d+)\/?$/)||[])[1];
// The guard exists to keep these destructive suites (they create accounts, invites and admin changes) away
// from the REAL pilot - not to pin one port. Loopback, and never the pilot's own 8082.
if(!/^http:\/\/127\.0\.0\.1:\d+\/?$/.test(BASE)||QA_PORT==='8082'){console.error('Refusing to run: QA_BASE must be a local UI-QA server (127.0.0.1, not the pilot on 8082). Default: http://127.0.0.1:8083');process.exit(2);}
const ROOT=path.join(__dirname,'..'),OUT=path.join(ROOT,'qa','phase0');fs.mkdirSync(OUT,{recursive:true});
const fixture=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(fixture.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(fixture.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const ONLY=(process.argv.find(a=>a.startsWith('--only='))||'').slice(7).split(',').filter(Boolean);
const VIEWPORTS=[
 {key:'desktop',width:1440,height:1100,mode:'desktop'},
 {key:'short',width:1366,height:543,mode:'desktop'},
 {key:'tablet',width:768,height:1024,mode:'tablet'},
 {key:'phone',width:390,height:844,mode:'phone',isMobile:true,hasTouch:true},
].filter(v=>!ONLY.length||ONLY.includes(v.key));
const DESK_TABS=['Discover','Evidence/Replay','Simulate','Plans/AutoTrade','Activity','Watch'];
const PHONE_TABS=['Discover','Watch','Simulate','AutoTrade','Activity'];
const TAB_ROUTE={'Discover':'/chart','Evidence/Replay':'/chart','Simulate':'/simulate','Plans/AutoTrade':'/autotrade','Activity':'/activity','Watch':'/watch'};

const results=[],metrics={},consoleByVp={},extra={};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const short=v=>{try{return JSON.stringify(v).slice(0,700)}catch{return String(v)}};
const esc=s=>String(s).replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
async function until(fn,{timeout=15000,interval=150,msg='condition'}={}){
 const end=Date.now()+timeout;let last;
 for(;;){try{last=await fn();if(last&&last.ok!==false)return last;}catch(e){last={error:e.message.split('\n')[0]};}
  if(Date.now()>end)throw new Error(`${msg} not met within ${timeout}ms; last=${short(last)}`);await sleep(interval);}
}
function withTimeout(p,ms,label){let t;return Promise.race([p,new Promise((_,rej)=>{t=setTimeout(()=>rej(new Error(`${label} timed out after ${ms}ms`)),ms)})]).finally(()=>clearTimeout(t));}
// Header-source tracking: every snap() inside a check records which source resolved the active symbol
// (heading → legend → legendToggle → title → none), so results show when a fallback was needed.
let curSrc=null;
function srcNote(src){
 if(src.includes('title'))return 'header heading/label missing (symbol from document.title fallback)';
 const fb=src.filter(x=>x==='legend'||x==='legendToggle');return fb.length?`header heading/label missing (symbol from ${fb.join('+')} fallback)`:undefined;
}
async function check(id,vp,name,page,fn,timeout=60000){
 const t0=Date.now();curSrc=new Set();
 const meta=()=>{const src=[...(curSrc||[])];curSrc=null;const note=srcNote(src);return {...(src.length?{headerSrc:src}:{}),...(note?{note}:{})};};
 try{const detail=await withTimeout(Promise.resolve().then(fn),timeout,id);const info=detail&&detail.info;const m=meta();results.push({id,vp,name,status:info?'INFO':'PASS',detail:short(info?detail.info:detail||''),...m,ms:Date.now()-t0});console.log(`${info?'INFO':'PASS'} ${vp} ${id} ${name}${m.note?' [note: '+m.note+']':''}`);}
 catch(e){const m=meta();const shot=path.join(OUT,`${vp}-FAIL-${id.replace(/[^\w.-]/g,'_')}.png`);try{await page.screenshot({path:shot});}catch{}
  results.push({id,vp,name,status:'FAIL',detail:String(e.message).split('\n').slice(0,3).join(' | ').slice(0,900),...m,shot:path.relative(ROOT,shot),ms:Date.now()-t0});console.log(`FAIL ${vp} ${id} ${name}: ${String(e.message).split('\n')[0].slice(0,400)}${m.note?' [note: '+m.note+']':''}`);}
}
async function shot(page,vp,name,locator,full=false){const file=path.join(OUT,`${vp}-${name}.png`);try{if(locator)await locator.screenshot({path:file,timeout:8000});else await page.screenshot({path:file,fullPage:full});return path.relative(ROOT,file);}catch(e){return `screenshot failed: ${e.message.split('\n')[0]}`;}}

// ---- in-page probes (installed before app scripts on every navigation) ----
function installSnap(){
 const vis=e=>!!e&&e.getClientRects().length>0;
 const q=(sel,root)=>Array.from((root||document).querySelectorAll(sel)).filter(vis);
 // Visible height of an element after clipping by the viewport and every overflow-clipping ancestor.
 window.__qaVisH=function(el){if(!el)return null;const r=el.getBoundingClientRect();let top=Math.max(r.top,0),bottom=Math.min(r.bottom,window.innerHeight);let p=el.parentElement;
  while(p&&p!==document.documentElement){const cs=getComputedStyle(p);if(/(auto|scroll|hidden|clip)/.test(cs.overflowY+' '+cs.overflow)){const pr=p.getBoundingClientRect();top=Math.max(top,pr.top);bottom=Math.min(bottom,pr.bottom);}p=p.parentElement;}
  return {height:Math.round(r.height),top:Math.round(r.top),bottom:Math.round(r.bottom),visible:Math.max(0,Math.round(bottom-top))};};
 window.__qaSnap=function(){
  const sp=new URLSearchParams(location.search),out={path:location.pathname,s:sp.get('s'),tf:sp.get('tf'),m:sp.get('m'),title:document.title};
  const symOf=(txt)=>{const t=(txt||'').trim().split(/[\s,]+/)[0];return /^[A-Z0-9][A-Z0-9&.\-]{0,24}$/.test(t)?t:null;};
  // Chart header (fix2 contract): role=heading, aria-label "Chart header for SYMBOL, TF, …".
  const heading=q('[role="heading"][aria-label^="Chart header for "]')[0];out.headingSym=heading?symOf(heading.getAttribute('aria-label').slice(17)):null;
  const canvas=q('[aria-label*=" candles, NSE"]')[0];out.canvas=canvas?canvas.getAttribute('aria-label').split(' ')[0]:null;
  // Legend container "Chart legend for SYMBOL …" (collapsed or expanded). Use the innermost labelled container (the ChartLegend group) for head/rows.
  const legends=q('[aria-label^="Chart legend for "]');const legend=legends.find(l=>!l.querySelector('[aria-label^="Chart legend for "]'))||legends[0]||null;
  out.legendContainerSym=legend?symOf(legend.getAttribute('aria-label').slice(17)):null;
  // Collapsed legend toggle / one-liner: aria-label "SYMBOL TF, O …. Expand legend" (or "… Collapse legend" when it carries the one-liner while expanded).
  const toggles=q('[aria-label$="Expand legend"],[aria-label$="Collapse legend"]');const tog=toggles.map(e=>e.getAttribute('aria-label')).find(l=>symOf(l));
  out.legendToggleSym=tog?symOf(tog):null;out.legendToggle=tog||null;out.legendCollapsed=q('[aria-label$="Expand legend"]').length>0;
  const ct=tog&&tog.match(/avg ([^,]+), 95% low ([^,]+), n (\d+|n\/a)/);out.legendCompactEv=ct?{avg:ct[1],low95:ct[2],n:ct[3]}:null;
  out.legend=out.legendContainerSym||out.legendToggleSym;
  out.titleSym=document.title.includes(' · ')?symOf(document.title.split(' · ')[0]):null;
  // Active-symbol resolution chain: heading → legend container → legend toggle → document.title.
  out.header=out.headingSym||out.legendContainerSym||out.legendToggleSym||out.titleSym||null;
  out.headerSrc=out.headingSym?'heading':out.legendContainerSym?'legend':out.legendToggleSym?'legendToggle':out.titleSym?'title':'none';
  const headEl=legend&&legend.firstElementChild;out.legendHead=headEl&&!out.legendCollapsed?headEl.textContent:null;
  const lev=legend?q('[aria-label^="Evidence for the selected pattern"]',legend)[0]:null;out.legendEv=lev?lev.getAttribute('aria-label'):null;
  out.legendLoading=(!!legend&&legend.textContent.includes('Loading evidence'))||(!!tog&&tog.includes('Loading evidence'));
  const head=q('[role="heading"]').find(h=>h.textContent.trim()==='Evidence summary');const region=head?head.closest('[role="region"]'):null;
  out.widget=null;out.widgetEv=null;out.widgetLoading=false;
  if(region){const g=q('[aria-label^="Evidence summary for "]',region)[0];const t=region.innerText||'';out.widgetLoading=t.includes('Loading evidence');
   if(g){out.widget=g.getAttribute('aria-label').slice(21).split(' ')[0];out.widgetEv=g.textContent;}
   else{const m=t.match(/No stored setup selected for (\S+)/);if(m)out.widget=m[1];else{const l=t.split('\n').map(x=>x.trim()).filter(Boolean).slice(1).find(x=>/^[A-Z0-9&.\-]{2,20}$/.test(x));out.widget=l||null;}}}
  out.tabs=q('[role="tab"][aria-selected="true"]').map(e=>e.getAttribute('aria-label'));
  out.full=q('[aria-label="Exit full screen Workspace dock"]').length>0;
  return out;
 };
}
const snap=async page=>{const s=await page.evaluate(()=>window.__qaSnap());if(curSrc&&s)curSrc.add(s.headerSrc||'none');return s;};
const parseRow=l=>{const m=l&&l.match(/^Inspect (\S+) (\S+) (.+?)\. ([^.]+?)\. (\S+) hold-period av(?:erage|g)(?: net per trade)?(?:, 95% low ([^,]+))?, (\d+) trades/);return m?{label:l,sym:m[1],tf:m[2],pattern:m[3],verdict:m[4],avg:m[5],low95:m[6]||null,n:m[7]}:null;};
// Discover result rows: "Inspect SYMBOL TF PATTERN. VERDICT. X hold-period average net per trade, …" (tolerates denser one-line rows).
const ROW_SEL='[aria-label^="Inspect "][aria-label*="hold-period av"]';
// Count Discover rows visible without resizing: "visible" = ≥75% of the row's height unclipped; "partial" = ≥20px.
async function discoverRowCounts(page){return page.evaluate((sel)=>{const els=Array.from(document.querySelectorAll(sel)).filter(e=>e.getClientRects().length);const v=els.map(e=>window.__qaVisH(e));const dock=document.querySelector('[aria-label="Workspace dock"]');
 return {rendered:els.length,visibleRows:v.filter(x=>x.height>0&&x.visible>=Math.max(1,x.height*.75)).length,partialRows:v.filter(x=>x.visible>=20).length,rowHeight:v[0]?v[0].height:null,dockHeight:dock?Math.round(dock.getBoundingClientRect().height):null};},ROW_SEL);}
const LEGEND_EXPAND='[aria-label$="Expand legend"] >> visible=true',LEGEND_COLLAPSE='[aria-label$="Collapse legend"] >> visible=true';
// Run fn with the legend overlay expanded; if it was collapsed, expand first and restore the collapsed state afterwards.
async function withLegendExpanded(page,fn){
 const wasCollapsed=(await page.locator(LEGEND_EXPAND).count())>0;
 if(wasCollapsed){const exact=page.locator('[aria-label="Expand legend"] >> visible=true');await ((await exact.count())?exact.first():page.locator(LEGEND_EXPAND).last()).click();
  await until(async()=>({ok:(await page.locator(LEGEND_EXPAND).count())===0}),{timeout:6000,msg:'legend expands'});await sleep(300);}
 let out,restore=wasCollapsed?'pending':'not needed (legend already expanded)';
 try{out=await fn();}
 finally{if(wasCollapsed){try{await page.locator(LEGEND_COLLAPSE).last().click();await until(async()=>({ok:(await page.locator(LEGEND_EXPAND).count())>0}),{timeout:6000,msg:'legend collapses again'});restore='restored collapsed';}catch(e){restore='restore failed: '+e.message.split('\n')[0];}}}
 if(out&&typeof out==='object'&&!Array.isArray(out))return out.info?{info:{...out.info,legend:{wasCollapsed,restore}}}:{...out,legend:{wasCollapsed,restore}};
 return out;
}
// "Test these rules in Simulate for SYMBOL" (fix2 contract), with a prefix fallback. Never builds a label from a null symbol.
async function simLink(page,sym){
 if(!sym)throw new Error('active symbol unresolved (heading, legend, legend toggle and document.title all empty); refusing to build "Test these rules in Simulate for null"');
 const exact=page.locator(`[aria-label="Test these rules in Simulate for ${sym}"] >> visible=true`),loose=page.locator('[aria-label^="Test these rules in Simulate"] >> visible=true');
 const how=await until(async()=>{const e=await exact.count(),l=await loose.count();return {ok:e>0||l>0,how:e?'exact':'prefix fallback',exact:e,prefix:l};},{timeout:15000,msg:`Simulate link for ${sym}`});
 const loc=how.how==='exact'?exact.first():loose.first();return {loc,how:how.how,label:await loc.getAttribute('aria-label')};
}
const parseLegendEv=l=>{const m=l&&l.match(/Average (\S+) per trade, 95% range (\S+) to (\S+), (\S+) trades/);return m?{avg:m[1],low95:m[2],n:m[4]}:null;};
const parseWidgetEv=t=>{if(!t)return null;const a=t.match(/not this exit rule([+\-−]?[\d.,]+%|—)/),r=t.match(/this result alone(\S+) to /),n=t.match(/N (\d+|n\/a)/);return a?{avg:a[1],low95:r?r[1]:null,n:n?n[1]:null}:null;};
const sameStats=(a,b)=>!!a&&!!b&&a.avg===b.avg&&String(a.low95)===String(b.low95)&&String(a.n)===String(b.n);
async function discoverRows(page){const labels=await page.locator(ROW_SEL).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));const seen=new Set();return labels.map(parseRow).filter(r=>r&&!seen.has(r.sym+r.tf+r.pattern)&&seen.add(r.sym+r.tf+r.pattern));}
const rowLoc=(page,r)=>page.locator(`[aria-label^="Inspect ${r.sym} ${r.tf} ${r.pattern}."]`).first();
// Scroll the virtualised Discover list by `dy` px. Returns null when no scroller is found, else how far it moved.
const scrollDiscover=(page,dy)=>page.evaluate((dy)=>{
 const vis=e=>!!e&&e.getClientRects().length>0;
 const panel=Array.from(document.querySelectorAll('[aria-label^="Discover results."]')).filter(vis)[0];if(!panel)return null;
 const sc=[panel,...panel.querySelectorAll('*')].filter(n=>n.scrollHeight-n.clientHeight>4&&/(auto|scroll)/.test(getComputedStyle(n).overflowY))
  .sort((a,b)=>(b.scrollHeight-b.clientHeight)-(a.scrollHeight-a.clientHeight))[0];
 if(!sc)return null;const before=sc.scrollTop;sc.scrollTop=before+dy;return {before,after:sc.scrollTop,moved:sc.scrollTop!==before};
},dy);
// A Discover row whose symbol is NOT in `exclude`, guaranteed MOUNTED when it is returned (an unmounted row
// cannot be clicked). The researched 107-pattern set lists dozens of rows for a single stock, so the rows
// mounted at any one moment can all belong to one or two symbols; the earlier capture is tried first and the
// list is then scrolled until a row for a new symbol mounts. Throws naming the symbols it did see, so a list
// that genuinely holds only one stock reads as that, never as "cannot read properties of undefined".
async function otherRow(page,captured,exclude,{from=1,tries=30,step=560}={}){
 const seen=new Set();const add=list=>list.forEach(r=>seen.add(r.sym));
 add(captured);const first=captured.slice(from).find(r=>!exclude.includes(r.sym));
 if(first&&await rowLoc(page,first).count())return first;
 for(let i=0;i<tries;i++){
  const live=await discoverRows(page).catch(()=>[]);add(live);
  const hit=live.find(r=>!exclude.includes(r.sym));if(hit)return hit;
  const s=await scrollDiscover(page,step);if(!s||!s.moved)break;
  await sleep(220);
 }
 const live=await discoverRows(page).catch(()=>[]);add(live);
 const hit=live.find(r=>!exclude.includes(r.sym));if(hit)return hit;
 throw new Error(`no Discover row outside ${short(exclude)} after scrolling the whole list; symbols listed: ${short([...seen].slice(0,15))}`);
}
const dockTab=(page,name)=>page.getByRole('tablist',{name:'Workspace dock tabs'}).getByRole('tab',{name,exact:true});
const visPanel=page=>page.locator('[role=tabpanel] >> visible=true');
async function waitSymbol(page,sym,{timeout=20000,legend=true,widget=true,url=true}={}){
 return until(async()=>{const s=await snap(page);const ok=s.header===sym&&(!legend||s.legend===sym)&&(!widget||s.widget===sym)&&s.title.startsWith(sym+' · ')&&(!url||s.s===sym);
  return {ok,header:s.header,headerSrc:s.headerSrc,canvas:s.canvas,legend:s.legend,widget:s.widget,title:s.title,s:s.s,tf:s.tf,path:s.path};},{timeout,msg:`symbol ${sym} propagated to header/legend/widget/title/URL`});
}
// The chart workspace must render on EITHER detector set: the stored 10-pattern scan, or the researched 107
// served as today's live detections. The wait is the same; when it times out the mode and what the page is
// actually showing are reported, so "no setups in this mode" can never be mistaken for a broken chart.
let PATTERN_SET='unknown';
async function workspaceLoaded(page,timeout=60000){
 try{await page.getByRole('button',{name:'Inspect candle date and closing price'}).first().waitFor({state:'visible',timeout});}
 catch(e){
  const why=await page.evaluate(async()=>{let set=null,matches=null;
   try{const s=await (await fetch('/api/state',{credentials:'include'})).json();set=s&&s.pattern_set;}catch{}
   try{const m=await (await fetch('/api/matches?min_trades=0',{credentials:'include'})).json();matches=Array.isArray(m)?m.length:(m&&m.total);}catch{}
   return {set,matches,text:document.body.innerText.replace(/\s+/g,' ').slice(0,300)};}).catch(()=>null);
  throw new Error(`chart workspace did not render (pattern_set=${why&&why.set}, matches=${why&&why.matches}): ${why&&why.text}`);
 }
}
async function readPatternSet(page){
 PATTERN_SET=await page.evaluate(async()=>{try{const s=await (await fetch('/api/state',{credentials:'include'})).json();return (s&&s.pattern_set)||'legacy';}catch{return 'unknown';}});
 return PATTERN_SET;
}
async function waitTab(page,name,timeout=15000){return until(async()=>{const s=await snap(page);return {ok:s.tabs.includes(name),tabs:s.tabs,path:s.path,full:s.full};},{timeout,msg:`tab ${name} selected`});}

async function login(browser){
 if(!EMAIL||!PASSWORD)throw new Error('Could not read the fixture credentials from scripts/qa_server.py');
 const ctx=await browser.newContext({viewport:{width:1440,height:1100}});const page=await ctx.newPage();
 await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
 await page.getByRole('textbox',{name:'Email'}).fill(EMAIL);
 await page.locator('input[aria-label="Password"]').fill(PASSWORD);
 await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
 await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});
 await sleep(1500);
 if(new URL(page.url()).pathname==='/onboarding'){ // fixture is created onboarded; kept for completeness
  await page.getByRole('textbox').first().fill('Pilot tester');await page.getByRole('checkbox').first().click();await page.getByRole('button',{name:'Open my workspace'}).click();await sleep(2000);await page.goto(BASE+'/chart');
 }
 if(new URL(page.url()).pathname==='/billing')throw new Error('Fixture redirected to /billing (no pilot access)');
 if(new URL(page.url()).pathname!=='/chart')await page.goto(BASE+'/chart'); // sign-in lands on Falcon (/); the chart workspace now lives at /chart
 await readPatternSet(page);await workspaceLoaded(page);
 const state=await ctx.storageState();await ctx.close();
 // Each viewport starts from default layout: drop kanida.layout.* written by the 1440px login context (session cookie kept).
 for(const o of state.origins||[])o.localStorage=(o.localStorage||[]).filter(i=>!i.name.startsWith('kanida.layout.'));
 return state;
}

// ---------------- viewport runs ----------------
async function regionsDesktopLike(page,vp){
 const top=page.getByRole('group',{name:'Workspace top bar'});
 await check('2.1',vp,'TopBar: search + data-age pill states the data age (STALE + date when stale)',page,async()=>{
  await top.waitFor({state:'visible'});await top.getByRole('button',{name:/^Search stocks/}).waitFor({state:'visible'});
  const pill=top.getByRole('status',{name:/^Market data/});await pill.waitFor({state:'visible'});const txt=(await pill.innerText()).trim();
  // The pill must always state the market data's age. When the data IS stale it says so and names the day;
  // when it is current it names the session. A hard-coded fixture date would only assert the fixture.
  const stale=/STALE/.test(txt),dated=/\d{1,2} \w{3}( \d{4})?/.test(txt)||/Live/.test(txt);
  if(!txt.trim()||!dated)throw new Error('pill text: '+txt);
  if(stale&&!/\d{1,2} \w{3} \d{4}/.test(txt))throw new Error('stale pill without a date: '+txt);
  return {pill:txt,stale,shot:await shot(page,vp,'topbar',top)};});
 await check('2.2',vp,'ToolRail: Prev stock / Next stock / See evidence',page,async()=>{
  const rail=page.getByRole('toolbar',{name:'Chart tools'});await rail.waitFor({state:'visible'});
  for(const n of ['Prev stock','Next stock','See evidence'])await rail.getByRole('button',{name:n,exact:true}).waitFor({state:'visible'});return {shot:await shot(page,vp,'toolrail',rail)};});
 await check('2.3',vp,'Chart: PatternCanvas SVG present',page,async()=>{
  const c=page.getByRole('button',{name:'Inspect candle date and closing price'}).first();await c.waitFor({state:'visible'});const n=await c.locator('svg').count();if(!n)throw new Error('no svg in chart');
  return {svg:n,dashPaths:await c.locator('path[stroke-dashoffset]').count(),shot:await shot(page,vp,'chart',page.locator('[aria-label*=" candles, NSE"] >> visible=true').first())};});
 await check('2.4',vp,'Legend visible (container "Chart legend for" or collapsed toggle)',page,legendVisibleCheck(page,vp));
 await check('2.5',vp,'Sidebar: Watch + Evidence summary widgets',page,async()=>{
  const side=page.getByRole('complementary',{name:'Widgets'});await side.waitFor({state:'visible'});
  await side.getByRole('heading',{name:'Watch',exact:true}).waitFor({state:'visible'});await side.getByRole('heading',{name:'Evidence summary',exact:true}).waitFor({state:'visible'});
  return {shot:await shot(page,vp,'sidebar',side)};});
 await check('2.6',vp,'Dock tab bar: Discover · Evidence/Replay · Simulate · Plans/AutoTrade · Activity · Watch',page,async()=>{
  const list=page.getByRole('tablist',{name:'Workspace dock tabs'});await list.waitFor({state:'visible'});
  const names=await list.getByRole('tab').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));if(JSON.stringify(names)!==JSON.stringify(DESK_TABS))throw new Error('tabs: '+names.join(' · '));
  const box=await list.boundingBox();return {tabs:names,tabBarBottom:Math.round(box.y+box.height),inViewport:box.y+box.height<=page.viewportSize().height+1,shot:await shot(page,vp,'dock',page.getByRole('region',{name:'Workspace dock',exact:true}))};});
}
function legendVisibleCheck(page,vp){return async()=>{
 const l=page.locator('[aria-label^="Chart legend for "] >> visible=true'),t=page.locator(LEGEND_EXPAND);
 const r=await until(async()=>{const a=await l.count(),b=await t.count();return {ok:a>0||b>0,container:a,collapsedToggle:b};},{timeout:15000,msg:'legend container or collapsed legend toggle visible'});
 const s=await snap(page);const el=r.container?l.first():t.last();
 return {mode:s.legendCollapsed?'collapsed':'expanded',container:r.container?await l.first().getAttribute('aria-label'):null,toggle:s.legendToggle,legendSym:s.legend,shot:await shot(page,vp,'legend',r.container?l.first():el)};};}
async function layoutMetrics(page,vp,{phone=false}={}){
 await check('2.9',vp,'No horizontal overflow (scrollWidth <= innerWidth+1)',page,async()=>{
  const m=await page.evaluate(()=>({scrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,innerWidth:window.innerWidth}));
  (metrics[vp]=metrics[vp]||{}).overflow=m;if(m.scrollWidth>m.innerWidth+1)throw new Error(short(m));return m;});
 const minPlot=vp==='desktop'||vp==='short'?180:phone?150:1;
 await check('2.10',vp,`Chart plot visible height >= ${minPlot}px`,page,async()=>{
  const m=await page.evaluate((phone)=>{const c=Array.from(document.querySelectorAll('[aria-label="Inspect candle date and closing price"]')).find(e=>e.getClientRects().length);const g=c&&c.closest('[aria-label*=" candles, NSE"]');
   const out={svgPlot:window.__qaVisH(c),chartCard:window.__qaVisH(g)};
   if(phone){const sheet=Array.from(document.querySelectorAll('[role=region]')).find(e=>/ panel, /.test(e.getAttribute('aria-label')||''));if(sheet&&c){const r=c.getBoundingClientRect(),st=sheet.getBoundingClientRect().top;out.sheetTop=Math.round(st);out.svgUnobscuredAtPeek=Math.max(0,Math.round(Math.min(r.bottom,st,innerHeight)-Math.max(r.top,0)));}}
   return out;},phone);
  (metrics[vp]=metrics[vp]||{}).chart=m;
  // Phone: the plot height that counts is the part not covered by the peeking bottom sheet (if measured).
  const plotVisible=m.svgPlot?(m.svgUnobscuredAtPeek!=null?Math.min(m.svgPlot.visible,m.svgUnobscuredAtPeek):m.svgPlot.visible):0;m.plotVisible=plotVisible;m.minRequired=minPlot;
  if(plotVisible<minPlot)throw new Error(`chart plot visible ${plotVisible}px < ${minPlot}px: `+short(m));return {plotVisible,minRequired:minPlot,...m};});
 if(vp==='desktop'||vp==='short'){
  const minRows=vp==='desktop'?3:2;
  await check('2.11',vp,`Discover rows visible at default dock height >= ${minRows} (no resize)`,page,async()=>{
   await until(async()=>{const c=await discoverRowCounts(page);return {ok:c.rendered>0,...c};},{timeout:20000,msg:'Discover rows rendered'}).catch(()=>{});
   await sleep(300);const m=await discoverRowCounts(page);(metrics[vp]=metrics[vp]||{}).discoverDefault=m;
   if(m.visibleRows<minRows)throw new Error(`${m.visibleRows} Discover row(s) visible < ${minRows}: `+short(m));return {visibleRows:m.visibleRows,minRequired:minRows,...m};});
 }
}
// 2.12 duplicate controls (desktop): exactly one Prev stock + one Next stock (in the ToolRail) and one Watch toggle for the active setup (top bar; widget rows excluded).
async function duplicateControls(page,vp){
 await check('2.12',vp,'No duplicate controls: one Prev/Next stock (ToolRail) and one Watch toggle (top bar)',page,async()=>{
  const sym=(await snap(page)).header;if(!sym)throw new Error('active symbol unresolved; cannot identify the Watch toggle');
  const top=page.getByRole('group',{name:'Workspace top bar'});
  await top.getByRole('button',{name:new RegExp('^(Watch|Watching) '+esc(sym)+' ')}).first().waitFor({state:'visible',timeout:15000}).catch(()=>{});
  const m=await page.evaluate((sym)=>{
   const vis=e=>e.getClientRects().length>0&&getComputedStyle(e).visibility!=='hidden';
   const els=Array.from(document.querySelectorAll('[aria-label]')).filter(vis);
   const where=e=>({inToolRail:!!e.closest('[aria-label="Chart tools"]'),inTopBar:!!e.closest('[aria-label="Workspace top bar"]'),label:e.getAttribute('aria-label')});
   const inWidgets=e=>!!e.closest('[role="complementary"][aria-label="Widgets"],[role="dialog"][aria-label="Widgets"]');
   const prev=els.filter(e=>/^(Prev|Previous) stock$/.test(e.getAttribute('aria-label'))).map(where);
   const next=els.filter(e=>/^Next stock$/.test(e.getAttribute('aria-label'))).map(where);
   const symRe=new RegExp('^(Watch|Watching) '+sym.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'( |\\.|$)');
   const watch=els.filter(e=>{const l=e.getAttribute('aria-label');if(inWidgets(e)||e.closest('[role="tablist"],[role="tab"],[role="toolbar"]')||e.getAttribute('role')==='tab')return false;return symRe.test(l)||/^(Watch|Watching)$/.test(l);}).map(where);
   return {sym,prev,next,watch};},sym);
  (metrics[vp]=metrics[vp]||{}).duplicates={prev:m.prev.length,next:m.next.length,watch:m.watch.length};
  const problems=[];
  if(m.prev.length!==1||!m.prev[0].inToolRail)problems.push(`Prev stock controls: ${m.prev.length} (${m.prev.filter(x=>x.inToolRail).length} in ToolRail)`);
  if(m.next.length!==1||!m.next[0].inToolRail)problems.push(`Next stock controls: ${m.next.length} (${m.next.filter(x=>x.inToolRail).length} in ToolRail)`);
  if(m.watch.length!==1||!m.watch[0].inTopBar)problems.push(`Watch toggles for ${sym}: ${m.watch.length} (${m.watch.filter(x=>x.inTopBar).length} in top bar)`);
  if(problems.length)throw new Error(problems.join('; ')+' '+short(m));
  return {info:{prev:m.prev.length,next:m.next.length,watchToggles:m.watch.length,detail:m}};});
}

async function runDesktopSuite(page,vp,counters){
 // ---- 6.1 / 6.2 gates on first load ----
 await check('6.1',vp,'Prepare trade is disabled and states a gate reason (chart + Evidence widget)',page,async()=>{
  const disabled=async b=>(await b.isDisabled())||(await b.getAttribute('aria-disabled'))==='true';
  // Chart Prepare trade: accessible name "Prepare trade, unavailable: …" when blocked; pick the visible one outside the Widgets sidebar.
  const all=page.getByRole('button',{name:/^Prepare trade/});
  const idx=(await until(async()=>{const n=await all.count();for(let i=0;i<n;i++){const b=all.nth(i);if(await b.isVisible()&&!(await b.evaluate(e=>!!e.closest('[role="complementary"]'))))return {ok:true,i};}return {ok:false,count:n};},{timeout:15000,msg:'chart Prepare trade button visible'})).i;
  const chartBtn=all.nth(idx);const chartName=await chartBtn.getAttribute('aria-label');if(!(await disabled(chartBtn)))throw new Error('chart Prepare trade is enabled: '+chartName);
  const why=page.getByRole('button',{name:/^Why is Prepare trade unavailable\?/}).first();
  const whyLabel=await why.getAttribute('aria-label',{timeout:3000}).catch(()=>null),whyTitle=await why.getAttribute('title',{timeout:1000}).catch(()=>null);
  const chartReason=((chartName||'').match(/unavailable: (.+)$/)||[])[1]||(whyLabel||'').replace(/^Why is Prepare trade unavailable\?\s*/,'')||whyTitle||'';
  // Every reason the gate may give, all of which mean "not tradable". The gate must name ONE of them; a
  // blank or unknown reason is the failure. Which one is correct depends on the data and the detector set.
  const GATE=/Fresh prices are needed|No historical evidence for this setup|No tested evidence for this exit rule|Exit-rule evidence could not load|not on a current candle/;
  if(!GATE.test(chartReason))throw new Error('chart gate reason missing: '+short({chartReason,chartName,whyLabel,whyTitle}));
  const side=page.getByRole('complementary',{name:'Widgets'});const wBtn=side.getByRole('button',{name:/^Prepare trade,? unavailable: /});await wBtn.waitFor({state:'visible',timeout:20000});if(!(await disabled(wBtn)))throw new Error('widget Prepare trade enabled');
  const wName=await wBtn.getAttribute('aria-label');const wAlerts=await side.getByRole('alert').filter({hasText:/Prepare trade,? unavailable/}).allInnerTexts();
  const widgetReason=((wName||'').match(/unavailable: (.+)$/)||[])[1]||wAlerts[0]||'';if(!GATE.test(widgetReason))throw new Error('widget gate reason missing: '+short({widgetReason,wName,wAlerts}));
  const pill=await page.getByRole('status',{name:/^Market data/}).first().getAttribute('aria-label');if(!pill||!/\d{1,2} \w{3}/.test(pill))throw new Error('pill: '+pill);
  return {chartReason,widgetReason,chart:{name:chartName,reasonFrom:((chartName||'').includes('unavailable: ')?'accessible name':whyLabel?'why? label':'title'),why:whyLabel},widget:{name:wName,alerts:[...new Set(wAlerts)]},pill};});
 await check('6.2',vp,'Discover caveat "partly luck" + verdict chips',page,async()=>{
  const panel=visPanel(page);await panel.getByText(/partly luck/).first().waitFor({state:'visible'});
  const chips=await panel.getByRole('button',{name:/^Show (All|For review|Watch|Pass) \d+$/}).evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));if(chips.length!==4)throw new Error('chips: '+short(chips));return {chips};});
 // ---- 5.1 / 5.2 legend ----
 await check('5.1',vp,'Legend found / not-found rows (legend expanded first if collapsed)',page,()=>withLegendExpanded(page,async()=>{
  const m=await until(async()=>{const m=await page.evaluate(()=>{const ls=Array.from(document.querySelectorAll('[aria-label^="Chart legend for "]')).filter(e=>e.getClientRects().length);const l=ls.find(x=>!x.querySelector('[aria-label^="Chart legend for "]'))||ls[0];if(!l)return {found:0,notFound:0,selected:[],legend:false};
   return {legend:l.getAttribute('aria-label'),found:l.querySelectorAll('[aria-label*="Found on this chart"]').length,notFound:l.querySelectorAll('[aria-label*="Not found on this chart"]').length,selected:Array.from(l.querySelectorAll('[aria-pressed="true"][aria-label*="Selected."]')).map(e=>e.getAttribute('aria-label'))};});return {ok:!!m.found&&m.selected.length===1,...m};},{timeout:8000,msg:'legend found rows + one selected row'});
  delete m.ok;return m;}));
 await check('5.2',vp,`Selected row evidence badges (${PATTERN_SET==='research'?'researched set: no per-match numbers, and none are shown':'hold-period label, 95%, n+sample, costs 0.40% incl., next-open'})`,page,()=>withLegendExpanded(page,async()=>{
  if(PATTERN_SET==='research'){
   // A researched detection has no per-match history, so the legend must show the pattern WITHOUT inventing
   // an evidence number. Finding numbers here would be the real failure.
   await sleep(1500);const s0=await snap(page);
   const out={legend:s0.legend,legendEv:s0.legendEv,legendCompactEv:s0.legendCompactEv};
   if(!s0.legend)throw new Error('no legend for the selected detection: '+short(out));
   if(s0.legendEv||s0.legendCompactEv)throw new Error('evidence numbers shown for a match with no history: '+short(out));
   return out;}
  const s=await until(async()=>{const s=await snap(page);return {ok:!!s.legendEv,s};},{timeout:20000,msg:'legend evidence numbers'});
  const text=await page.locator('[aria-label^="Evidence for the selected pattern"] >> visible=true').first().innerText();const label=s.s.legendEv;
  const need={holdPeriodLabel:/hold-period/i.test(text),ci95:/95% low/.test(text)&&/95% range/.test(label),nSample:/n \d+ · \w+ sample/i.test(text),costs:/costs 0\.40% incl\./.test(text),nextOpen:/next-open/.test(text)};
  if(Object.values(need).some(v=>!v))throw new Error(short({need,text}));return {need,text:text.replace(/\s+/g,' ').slice(0,300)};}));
 await check('5.3',vp,'Chart hover changes legend OHLC; leaving restores last bar',page,()=>withLegendExpanded(page,async()=>{
  const chart=page.getByRole('button',{name:'Inspect candle date and closing price'}).first();await chart.scrollIntoViewIfNeeded();const box=await chart.boundingBox();
  const before=(await until(async()=>{const h=(await snap(page)).legendHead;return {ok:!!h&&/last bar/.test(h),h};},{timeout:5000,msg:'legend head shows "last bar"'})).h;
  // Hover the right part of the plot: the expanded legend overlay sits top-left and would swallow a hover there.
  await page.mouse.move(box.x+box.width*.78,box.y+box.height*.6,{steps:5});await page.mouse.move(box.x+box.width*.8,box.y+box.height*.6,{steps:2});
  const hovered=await until(async()=>{const h=(await snap(page)).legendHead;return {ok:!!h&&h!==before&&!/last bar/.test(h),h};},{timeout:5000,msg:'legend readout follows hover'});
  await page.mouse.move(box.x+box.width*.8,Math.max(2,box.y-160),{steps:5});
  await until(async()=>{const h=(await snap(page)).legendHead;return {ok:h===before,h};},{timeout:5000,msg:'legend restores last bar after leave'});
  return {before:before.slice(0,160),hovered:hovered.h.slice(0,160)};}));

 // ---- 3.1 dock tabs ----
 const expectations={'Discover':async p=>p.getByText(/partly luck/).first().waitFor({state:'visible'}),
  'Evidence/Replay':async p=>p.getByText(/historical replay/).first().waitFor({state:'visible'}),
  'Simulate':async p=>p.getByRole('button',{name:/^(Run historical simulation|Run walk-forward study)$/}).first().waitFor({state:'visible',timeout:20000}),
  'Plans/AutoTrade':async p=>p.getByText('LIVE ORDERS DISABLED',{exact:true}).first().waitFor({state:'visible',timeout:20000}),
  'Activity':async p=>p.getByText('Every decision has a trail.',{exact:true}).first().waitFor({state:'visible'}),
  'Watch':async p=>p.getByText('My watch',{exact:true}).first().waitFor({state:'visible'})};
 for(const name of [...DESK_TABS.slice(1),'Discover']){
  await check('3.1',vp,`Dock tab "${name}" shows its panel; URL path ${TAB_ROUTE[name]}`,page,async()=>{
   await dockTab(page,name).click();await waitTab(page,name);await expectations[name](visPanel(page));
   const pth=await until(async()=>{const s=await snap(page);return {ok:s.path===TAB_ROUTE[name],path:s.path,s:s.s};},{timeout:8000,msg:`URL path ${TAB_ROUTE[name]}`});
   return {path:pth.path,s:pth.s,shot:await shot(page,vp,'dock-'+name.replace(/\W+/g,'-').toLowerCase())};});
 }
 await check('6.3',vp,'Plans/AutoTrade shows "NOT CAPITAL"',page,async()=>{await dockTab(page,'Plans/AutoTrade').click();await waitTab(page,'Plans/AutoTrade');const t=visPanel(page).getByText(/NOT CAPITAL/).first();await t.waitFor({state:'visible',timeout:20000});return {text:await t.innerText()};});
 await check('3.5',vp,'AutoTrade /api/trading polling after leaving the tab (known gap, informational)',page,async()=>{
  await dockTab(page,'Discover').click();await waitTab(page,'Discover');await sleep(500);const c0=counters.trading;await sleep(11000);return {info:{requestsIn11sAfterLeaving:counters.trading-c0}};},30000);

 // ---- 3.3 persistence ----
 const dock=page.getByRole('region',{name:'Workspace dock',exact:true});
 await check('3.3a',vp,'Dock resize persists after reload',page,async()=>{
  const h0=Math.round((await dock.boundingBox()).height);await page.getByRole('separator',{name:'Resize Workspace dock'}).focus();await page.keyboard.press('ArrowUp');
  const h1=(await until(async()=>{const h=Math.round((await dock.boundingBox()).height);return {ok:h!==h0,h};},{timeout:5000,msg:'dock height changes'})).h;
  await page.reload();await workspaceLoaded(page);await sleep(800);const h2=Math.round((await dock.boundingBox()).height);if(Math.abs(h2-h1)>2)throw new Error(short({h0,h1,h2}));
  await page.getByRole('separator',{name:'Resize Workspace dock'}).focus();await page.keyboard.press('ArrowDown');return {h0,h1,afterReload:h2};});
 await check('3.3b',vp,'Dock collapse persists after reload',page,async()=>{
  await page.getByRole('button',{name:'Collapse Workspace dock to tab bar'}).click();await page.getByRole('button',{name:'Expand Workspace dock'}).waitFor({state:'visible'});
  await page.reload();await workspaceLoaded(page);await page.getByRole('button',{name:'Expand Workspace dock'}).waitFor({state:'visible',timeout:10000});const h=Math.round((await dock.boundingBox()).height);
  const s=await shot(page,vp,'dock-collapsed-after-reload');await page.getByRole('button',{name:'Expand Workspace dock'}).click();await page.getByRole('button',{name:'Collapse Workspace dock to tab bar'}).waitFor({state:'visible'});return {collapsedHeight:h,shot:s};});
 await check('3.3c',vp,'Sidebar collapse persists after reload',page,async()=>{
  await page.getByRole('button',{name:'Collapse Widgets'}).click();await page.getByRole('button',{name:'Expand Widgets'}).waitFor({state:'visible'});
  await page.reload();await workspaceLoaded(page);await page.getByRole('button',{name:'Expand Widgets'}).waitFor({state:'visible',timeout:10000});
  const hidden=await page.getByRole('heading',{name:'Evidence summary',exact:true}).isHidden();await page.getByRole('button',{name:'Expand Widgets'}).click();await page.getByRole('heading',{name:'Evidence summary',exact:true}).waitFor({state:'visible'});return {widgetsHiddenWhileCollapsed:hidden};});

 // ---- Discover rows reachable? enlarge dock for row-click checks ----
 let rows=[];
 await check('2.11e',vp,'Discover rows after enlarging the dock (row list for click checks, metric)',page,async()=>{
  const m=await discoverRowCounts(page);
  await page.getByRole('separator',{name:'Resize Workspace dock'}).focus();for(let i=0;i<3;i++){await page.keyboard.press('Shift+ArrowUp');await sleep(120);}await sleep(400);
  const after=await discoverRowCounts(page);
  metrics[vp].discoverEnlarged=after;rows=await discoverRows(page);if(rows.length<10)throw new Error('fewer than 10 Discover rows: '+rows.length);
  return {info:{beforeEnlarge:m,afterEnlarge:after,rows:rows.length,shot:await shot(page,vp,'dock-enlarged')}};});
 if(!rows.length)rows=await discoverRows(page).catch(()=>[]);
 const statsBySym=Object.fromEntries(rows.map(r=>[r.sym,{avg:r.avg,low95:r.low95,n:r.n}]));

 // ---- 4 symbol propagation ----
 let idx=-1;
 await check('4.a',vp,'Discover row click propagates symbol',page,async()=>{
  const cur=(await snap(page)).header;idx=rows.findIndex((r,i)=>i>=2&&r.sym!==cur&&rows[i+1]&&rows[i+2]&&rows[i+1].sym!==cur&&rows[i+2].sym!==cur);const r=rows[idx];
  await rowLoc(page,r).click();const w=await waitSymbol(page,r.sym);return {picked:r.sym,...w};});
 await check('4.b',vp,'↓ arrow in Discover panel propagates next symbol',page,async()=>{
  const r=rows[idx+1];await page.keyboard.press('ArrowDown');let how='focus on clicked row';
  try{await waitSymbol(page,r.sym,{timeout:5000});}catch{how='focused Discover panel container';await page.locator('[aria-label^="Discover results."]').first().focus();await page.keyboard.press('ArrowDown');}
  const w=await waitSymbol(page,r.sym);return {expected:r.sym,how,...w};});
 await check('4.c',vp,'ToolRail Next / Prev propagate symbol',page,async()=>{
  const rail=page.getByRole('toolbar',{name:'Chart tools'});await rail.getByRole('button',{name:'Next stock',exact:true}).click();const n=await waitSymbol(page,rows[idx+2].sym);
  await rail.getByRole('button',{name:'Prev stock',exact:true}).click();const p=await waitSymbol(page,rows[idx+1].sym);return {next:n.header,prev:p.header};});
 await check('4.d',vp,'Top-bar search → result propagates symbol',page,async()=>{
  const s0=await snap(page);const target=rows.slice(5).find(r=>r.tf===s0.tf&&r.sym!==s0.header)||await otherRow(page,rows,[s0.header],{from:5});
  await page.getByRole('group',{name:'Workspace top bar'}).getByRole('button',{name:/^Search stocks/}).click();
  const box=page.getByRole('textbox',{name:'Stock symbol or company'});await box.fill(target.sym);
  const hit=page.getByRole('button',{name:new RegExp('^Open '+esc(target.sym)+'(,| on the chart)')}).first();await hit.waitFor({state:'visible',timeout:15000});await hit.click();
  const w=await waitSymbol(page,target.sym);const sh=await shot(page,vp,'search-picked');return {target:target.sym,tf:s0.tf,...w,shot:sh};});
 await check('4.e',vp,'Watch widget row propagates symbol (after Watch toggle)',page,async()=>{
  const top=page.getByRole('group',{name:'Workspace top bar'}),side=page.getByRole('complementary',{name:'Widgets'});
  const cur=(await snap(page)).header;const x=await otherRow(page,rows,[cur],{from:1});await rowLoc(page,x).click();await waitSymbol(page,x.sym);
  const toggle=top.getByRole('button',{name:new RegExp('^(Watch|Watching) '+esc(x.sym)+' ')});await toggle.waitFor({state:'visible'});let added=false;
  if(!/^Watching /.test(await toggle.getAttribute('aria-label'))){await toggle.click();added=true;}
  await top.getByRole('button',{name:new RegExp('^Watching '+esc(x.sym)+' ')}).waitFor({state:'visible',timeout:15000});
  const wrow=side.getByRole('button',{name:new RegExp('^Open '+esc(x.sym)+' ')}).first();await wrow.waitFor({state:'visible',timeout:15000});
  const y=await otherRow(page,rows,[x.sym,cur],{from:2});await rowLoc(page,y).click();await waitSymbol(page,y.sym);
  await wrow.click();const w=await waitSymbol(page,x.sym);const sh=await shot(page,vp,'watch-widget-picked');
  const selected=await wrow.getAttribute('aria-label');
  if(added){await top.getByRole('button',{name:new RegExp('^Watching '+esc(x.sym)+' ')}).click();await top.getByRole('button',{name:new RegExp('^Watch '+esc(x.sym)+' ')}).waitFor({state:'visible',timeout:15000});}
  return {watched:x.sym,via:y.sym,addedThenRemoved:added,rowLabel:selected,...w,shot:sh};});
 await check('4.f',vp,'Deep link /?s=&tf= on load propagates symbol',page,async()=>{
  const t=rows[Math.min(rows.length-1,9)];await page.goto(`${BASE}/chart?s=${encodeURIComponent(t.sym)}&tf=${encodeURIComponent(t.tf)}`);await workspaceLoaded(page);
  const w=await waitSymbol(page,t.sym);if(w.tf!==t.tf)throw new Error('tf lost: '+short(w));return {url:`/chart?s=${t.sym}&tf=${t.tf}`,...w};});

 // ---- 5.4 rapid switching key guard ----
 async function rapidTrial(label,trigger){
  // The Discover list is virtualised, so a row captured earlier may no longer be MOUNTED by the time the
  // rapid trial clicks it - which is exactly what an in-page click cannot recover from. The three targets are
  // therefore taken from the rows mounted right now, falling back to the earlier capture if that read fails.
  const cur=(await snap(page)).header;
  const live=await discoverRows(page).catch(()=>[]);const pool=live.length>=4?live:rows;
  let i=pool.findIndex((r,k)=>k>=1&&pool[k+2]&&![r.sym,pool[k+1].sym,pool[k+2].sym].includes(cur));
  if(i<0)i=1;
  const abc=pool.slice(i,i+3);
  if(abc.length<3)throw new Error('fewer than 3 mounted Discover rows to drive the rapid trial: '+short({live:live.length,rows:rows.length}));
  await page.evaluate(()=>{window.__qaSamples=[];window.__qaT0=performance.now();const id=setInterval(()=>{const s=window.__qaSnap();window.__qaSamples.push({t:Math.round(performance.now()-window.__qaT0),header:s.header,headerSrc:s.headerSrc,legend:s.legend,legendEv:s.legendEv,legendCompactEv:s.legendCompactEv,legendLoading:s.legendLoading,widget:s.widget,widgetEv:s.widgetEv,widgetLoading:s.widgetLoading,title:s.title,s:s.s});},40);setTimeout(()=>clearInterval(id),2400);});
  const clickInfo=await trigger(abc,i);await sleep(2600);
  const raw=await page.evaluate(()=>window.__qaSamples);
  // "header" below is the resolved active symbol (heading → legend container → legend toggle → title); headerSrc records which one.
  const samples=raw.map(s=>{const le=parseLegendEv(s.legendEv),we=parseWidgetEv(s.widgetEv);if(curSrc)curSrc.add(s.headerSrc||'none');return {t:s.t,header:s.header,headerSrc:s.headerSrc,legend:s.legend,legendEv:le,legendCompactEv:s.legendCompactEv,legendLoading:s.legendLoading,widget:s.widget,widgetEv:we,widgetLoading:s.widgetLoading,title:s.title,s:s.s};});
  const violations=[];
  // Strict rule: any evidence numbers on screen must belong to the resolved active symbol and equal its Discover stats.
  const test=(s,where,owner,shown)=>{if(!shown)return;const exp=s.header?statsBySym[s.header]:undefined;if(!s.header||owner!==s.header||!sameStats(shown,exp)){const match=Object.keys(statsBySym).filter(k=>sameStats(statsBySym[k],shown));violations.push({t:s.t,where,header:s.header,headerSrc:s.headerSrc,[where==='widget'?'widget':'legend']:owner,shown,expected:exp||null,numbersMatch:match,...(s.header?{}:{reason:'active symbol unresolved while evidence numbers shown'})});}};
  for(const s of samples){test(s,'legend',s.legend,s.legendEv);test(s,'legendCompact',s.legend,s.legendCompactEv);test(s,'widget',s.widget,s.widgetEv);}
  const last=samples[samples.length-1]||{};const headers=[...new Set(samples.map(s=>s.header))];
  const summary={label,targets:abc.map(r=>r.sym),clicks:clickInfo,samples:samples.length,headersSeen:headers,headerSources:[...new Set(samples.map(s=>s.headerSrc))],loadingLegendSamples:samples.filter(s=>s.legendLoading).length,loadingWidgetSamples:samples.filter(s=>s.widgetLoading).length,
   withNumbers:{legend:samples.filter(s=>s.legendEv).length,legendCompact:samples.filter(s=>s.legendCompactEv).length,widget:samples.filter(s=>s.widgetEv).length},final:{header:last.header,headerSrc:last.headerSrc,legend:last.legend,widget:last.widget,legendEv:last.legendEv,legendCompactEv:last.legendCompactEv,widgetEv:last.widgetEv,title:last.title,s:last.s},violations};
  (extra.rapid=extra.rapid||[]).push({...summary,samplesDetail:samples});return summary;
 }
 await check('5.4a',vp,'Rapid A→B→C Discover row clicks (in-page clicks 120ms apart): no evidence numbers for a non-header symbol',page,async()=>{
  const r=await rapidTrial('discover-row-clicks',async(abc)=>page.evaluate((labels)=>new Promise(res=>{const log=[];labels.forEach((lab,i)=>setTimeout(()=>{const el=Array.from(document.querySelectorAll('[aria-label]')).find(e=>(e.getAttribute('aria-label')||'').startsWith(lab));log.push({i,t:Math.round(performance.now()-window.__qaT0),found:!!el});if(el)el.click();if(i===labels.length-1)res(log);},i*120));}),abc.map(x=>`Inspect ${x.sym} ${x.tf} ${x.pattern}.`)));
  if(r.final.header!==r.targets[2])throw new Error('final header '+r.final.header+' != '+r.targets[2]+' '+short(r.clicks));
  if(r.violations.length)throw new Error('violations: '+short(r.violations.slice(0,3)));
  const c=r.clicks;return {targets:r.targets,clickSpanMs:c[c.length-1].t-c[0].t,samples:r.samples,headersSeen:r.headersSeen,loadingLegend:r.loadingLegendSamples,loadingWidget:r.loadingWidgetSamples,withNumbers:r.withNumbers,final:r.final};},30000);
 await check('5.4b',vp,'Rapid ↓↓↓ in Discover (100ms apart): no evidence numbers for a non-header symbol',page,async()=>{
  const r=await rapidTrial('discover-arrow-keys',async(abc,i)=>{await rowLoc(page,rows[i-1]).click();await sleep(50);const t=Date.now();for(let k=0;k<3;k++){await page.keyboard.press('ArrowDown');await sleep(100);}return {spanMs:Date.now()-t,startedFrom:rows[i-1].sym};});
  if(r.violations.length)throw new Error('violations: '+short(r.violations.slice(0,3)));
  return {targets:r.targets,clicks:r.clicks,samples:r.samples,headersSeen:r.headersSeen,loadingLegend:r.loadingLegendSamples,loadingWidget:r.loadingWidgetSamples,withNumbers:r.withNumbers,final:r.final};},30000);

 // ---- 6.4 Simulate origin / back to chart ----
 await check('6.4',vp,'"Test these rules in Simulate →" opens Simulate with origin; "← Back to chart" returns to Discover with same setup',page,async()=>{
  const s0=await snap(page);const sym=s0.header;
  const link=await simLink(page,sym);await link.loc.click();
  const t=await waitTab(page,'Simulate');await until(async()=>{const s=await snap(page);return {ok:s.path==='/simulate'&&s.full,path:s.path,full:s.full};},{timeout:8000,msg:'/simulate full screen'});
  const back=page.locator(`[aria-label^="Back to ${sym} "] >> visible=true`).first();await back.waitFor({state:'visible',timeout:15000});const backLabel=await back.getAttribute('aria-label');
  await visPanel(page).getByRole('button',{name:/^(Run historical simulation|Run walk-forward study)$/}).first().waitFor({state:'visible'});const sh=await shot(page,vp,'simulate-origin');
  await back.click();await waitTab(page,'Discover');const w=await waitSymbol(page,sym);const s1=await snap(page);if(s1.full||s1.path!=='/chart')throw new Error('not back on chart: '+short(s1));
  return {simulateLink:{label:link.label,match:link.how},origin:backLabel,after:{path:s1.path,s:s1.s,m:s1.m,header:w.header,tabs:s1.tabs},mBefore:s0.m,shot:sh};});

 // ---- 3.4 back / forward ----
 await check('3.4',vp,'Browser back/forward between tabs (URL ↔ tab consistency)',page,async()=>{
  const states=[];const rec=async(step)=>{await sleep(1500);const s=await snap(page);const tab=s.tabs.find(t=>DESK_TABS.includes(t));const consistent=tab?(TAB_ROUTE[tab]===s.path):false;states.push({step,path:s.path,s:s.s,tab,full:s.full,header:s.header,consistent});};
  await rec('start');const sym=(await snap(page)).header;
  const link=await simLink(page,sym);await link.loc.click();await rec(`push: Test these rules in Simulate (${link.how})`);
  await page.goBack();await rec('back');await page.goForward();await rec('forward');
  await dockTab(page,'Activity').click();await rec('tab click Activity (replace)');await dockTab(page,'Watch').click();await rec('tab click Watch (replace)');
  await page.goBack();await rec('back');await page.goForward();await rec('forward');
  extra.backForward=states;const bad=states.filter(s=>!s.consistent);if(bad.length)throw new Error('inconsistent states: '+short(bad));return {states};},60000);

 // ---- 7 responsiveness ----
 await check('7',vp,'Dock tab response while pencil reveal animates + long tasks',page,async()=>{
  await page.goto(BASE+'/chart');await workspaceLoaded(page);await page.getByText('Structure drawn. Scenarios illustrated.').first().waitFor({state:'visible',timeout:20000}).catch(()=>{});
  const trials=[];
  async function trial(tabName,panelText,label){
   if(!(await snap(page)).tabs.includes('Discover')){await dockTab(page,'Discover').click();await waitTab(page,'Discover');}
   const cur=(await snap(page)).header;const r=await otherRow(page,rows,[cur,...trials.map(t=>t.row)],{from:1});
   await page.evaluate(()=>{window.__lt=[];if(window.__obs)window.__obs.disconnect();window.__obs=new PerformanceObserver(l=>{for(const e of l.getEntries())window.__lt.push({start:e.startTime,dur:e.duration});});window.__obs.observe({type:'longtask'});window.__tSel=performance.now();});
   await rowLoc(page,r).click();await waitSymbol(page,r.sym,{widget:false,timeout:10000});await sleep(150);
   const reveal=await page.evaluate(()=>{const c=Array.from(document.querySelectorAll('[aria-label="Inspect candle date and closing price"]')).find(e=>e.getClientRects().length);const d=c?Array.from(c.querySelectorAll('path[stroke-dashoffset]')).map(p=>parseFloat(p.getAttribute('stroke-dashoffset'))):[];return {dashoffsets:d.slice(0,4).map(v=>Math.round(v)),animating:d.some(v=>v>0.5),drawnText:document.body.innerText.includes('Structure drawn. Scenarios illustrated.'),msSinceSelect:Math.round(performance.now()-window.__tSel)};});
   await page.evaluate((txt)=>{window.__tab={};const down=()=>{window.__tab.down=performance.now();window.removeEventListener('pointerdown',down,true);};window.addEventListener('pointerdown',down,true);
    const loop=()=>{const el=Array.from(document.querySelectorAll('[role=tabpanel]')).find(p=>p.getClientRects().length&&p.textContent.includes(txt));if(el&&window.__tab.down){window.__tab.visible=performance.now();return;}requestAnimationFrame(loop);};requestAnimationFrame(loop);},panelText);
   const w0=Date.now();await dockTab(page,tabName).click();await visPanel(page).getByText(panelText).first().waitFor({state:'visible',timeout:15000});const wall=Date.now()-w0;
   const inPage=await until(async()=>{const t=await page.evaluate(()=>window.__tab);return {ok:!!t.visible,t};},{timeout:5000,msg:'rAF saw panel'});
   const done=await page.waitForFunction(()=>{const c=Array.from(document.querySelectorAll('[aria-label="Inspect candle date and closing price"]')).find(e=>e.getClientRects().length);const d=c?Array.from(c.querySelectorAll('path[stroke-dashoffset]')).map(p=>parseFloat(p.getAttribute('stroke-dashoffset'))):[];return d.length&&d.every(v=>v<=0.5)?Math.round(performance.now()-window.__tSel):false;},null,{timeout:12000,polling:'raf'}).then(h=>h.jsonValue()).catch(()=>null);
   await sleep(300);const lt=await page.evaluate(()=>window.__lt.filter(e=>e.start>=window.__tSel-5).map(e=>({startMsAfterSelect:Math.round(e.start-window.__tSel),dur:Math.round(e.dur)})));
   trials.push({label,row:r.sym,tab:tabName,revealAtClick:reveal,clickToPanelVisibleFrameMs:Math.round(inPage.t.visible-inPage.t.down),nodeWallClickToVisibleMs:wall,revealCompleteMsAfterSelect:done,longTasks:{count:lt.length,totalMs:lt.reduce((a,b)=>a+b.dur,0),maxMs:lt.reduce((a,b)=>Math.max(a,b.dur),0),list:lt}});
   await shot(page,vp,`responsiveness-${label}`);
  }
  await trial('Activity','Every decision has a trail.','activity-first-mount');
  await trial('Activity','Every decision has a trail.','activity-mounted');
  await trial('Simulate','Simulation','simulate-mounted-or-first');
  // baseline: no reveal running
  await page.evaluate(()=>{window.__tab={};const down=()=>{window.__tab.down=performance.now();window.removeEventListener('pointerdown',down,true);};window.addEventListener('pointerdown',down,true);const loop=()=>{const el=Array.from(document.querySelectorAll('[role=tabpanel]')).find(p=>p.getClientRects().length&&p.textContent.includes('partly luck'));if(el&&window.__tab.down){window.__tab.visible=performance.now();return;}requestAnimationFrame(loop);};requestAnimationFrame(loop);});
  const b0=Date.now();await dockTab(page,'Discover').click();await visPanel(page).getByText(/partly luck/).first().waitFor({state:'visible'});const bw=Date.now()-b0;const bt=await until(async()=>{const t=await page.evaluate(()=>window.__tab);return {ok:!!t.visible,t};},{timeout:5000,msg:'baseline'});
  extra.responsiveness={trials,baselineNoReveal:{tab:'Discover',clickToPanelVisibleFrameMs:Math.round(bt.t.visible-bt.t.down),nodeWallClickToVisibleMs:bw}};
  const notAnimating=trials.filter(t=>!t.revealAtClick.animating);
  return {info:{...extra.responsiveness,note:notAnimating.length?`${notAnimating.length} trial(s) clicked after the reveal had finished`:'all trials clicked while reveal animating'}};},120000);

 // ---- 3.2 deep links ----
 for(const [route,tab,probe] of [['/simulate','Simulate',p=>p.getByRole('button',{name:/^(Run historical simulation|Run walk-forward study)$/}).first()],['/watch','Watch',p=>p.getByText('My watch',{exact:true}).first()],['/autotrade','Plans/AutoTrade',p=>p.getByText('LIVE ORDERS DISABLED',{exact:true}).first()],['/activity','Activity',p=>p.getByText('Every decision has a trail.',{exact:true}).first()]]){
  await check('3.2',vp,`Deep link ${route} opens "${tab}" full screen`,page,async()=>{
   await page.goto(BASE+route);await page.getByRole('tablist',{name:'Workspace dock tabs'}).waitFor({state:'visible',timeout:45000});await waitTab(page,tab,20000);
   await page.getByRole('button',{name:'Exit full screen Workspace dock'}).waitFor({state:'visible'});await probe(visPanel(page)).waitFor({state:'visible',timeout:20000});
   const s=await snap(page);const chartVisible=await page.locator('[aria-label^="Chart legend for "],[aria-label$="Expand legend"],[role="heading"][aria-label^="Chart header for "] >> visible=true').count();if(chartVisible)throw new Error('chart still visible in full screen');
   return {path:s.path,tabs:s.tabs,full:s.full,shot:await shot(page,vp,'deeplink'+route.replace('/','-'))};});
 }
}

async function runTablet(page,vp){
 await check('2.1',vp,'TopBar: search + data-age pill',page,async()=>{const top=page.getByRole('group',{name:'Workspace top bar'});await top.getByRole('button',{name:/^Search stocks/}).waitFor({state:'visible'});const pill=top.getByRole('status',{name:/^Market data/});await pill.waitFor({state:'visible'});return {pill:(await pill.innerText()).trim(),shot:await shot(page,vp,'topbar',top)};});
 await check('2.3',vp,'Chart: PatternCanvas SVG present',page,async()=>{const c=page.getByRole('button',{name:'Inspect candle date and closing price'}).first();await c.waitFor({state:'visible'});if(!(await c.locator('svg').count()))throw new Error('no svg');return {shot:await shot(page,vp,'chart',page.locator('[aria-label*=" candles, NSE"] >> visible=true').first())};});
 await check('2.4',vp,'Legend visible (container "Chart legend for" or collapsed toggle)',page,legendVisibleCheck(page,vp));
 await check('2.6',vp,'Dock tab bar present (chart + dock, no sidebar/rail)',page,async()=>{const list=page.getByRole('tablist',{name:'Workspace dock tabs'});await list.waitFor({state:'visible'});const names=await list.getByRole('tab').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));
  const rail=await page.locator('[aria-label="Chart tools"] >> visible=true').count(),side=await page.locator('[role=complementary][aria-label="Widgets"] >> visible=true').count();if(JSON.stringify(names)!==JSON.stringify(DESK_TABS)||side)throw new Error(short({names,rail,side}));return {tabs:names,toolRailVisible:!!rail,sidebarVisible:!!side,shot:await shot(page,vp,'dock',page.getByRole('region',{name:'Workspace dock',exact:true}))};});
 await check('2.7',vp,'"Widgets" button opens slide-over; Escape closes it',page,async()=>{
  const btn=page.getByRole('button',{name:'Open widgets: Watch and Evidence summary'});await btn.click();const dlg=page.getByRole('dialog',{name:'Widgets'});await dlg.waitFor({state:'visible',timeout:8000});
  await dlg.getByRole('heading',{name:'Watch',exact:true}).waitFor({state:'visible'});await dlg.getByRole('heading',{name:'Evidence summary',exact:true}).waitFor({state:'visible'});await sleep(400);const sh=await shot(page,vp,'widgets-slideover');
  await page.keyboard.press('Escape');await dlg.waitFor({state:'hidden',timeout:5000});return {shot:sh};});
}

async function runPhone(page,vp){
 await check('2.1',vp,'Compact top bar (mark without .AI, compact STALE pill, search)',page,async()=>{
  const top=page.getByRole('group',{name:'Workspace top bar'});await top.waitFor({state:'visible'});const mark=(await top.getByRole('link',{name:'KANIDA home'}).innerText()).trim();
  const pill=(await top.getByRole('status',{name:/^Market data/}).innerText()).trim();await top.getByRole('button',{name:/^Search stocks/}).waitFor({state:'visible'});
  if(/\.AI/.test(mark)||/Data:/.test(pill)||!pill.trim())throw new Error(short({mark,pill}));return {mark,pill,stale:/STALE/.test(pill),shot:await shot(page,vp,'topbar',top)};});
 await check('2.3',vp,'Chart: PatternCanvas SVG present + compact one-line legend',page,async()=>{const c=page.getByRole('button',{name:'Inspect candle date and closing price'}).first();await c.waitFor({state:'visible'});if(!(await c.locator('svg').count()))throw new Error('no svg');
  const one=page.getByRole('button',{name:/Expand legend$/}).first();await one.waitFor({state:'visible'});return {legend:await one.getAttribute('aria-label'),shot:await shot(page,vp,'chart',page.locator('[aria-label*=" candles, NSE"] >> visible=true').first())};});
 await check('2.5',vp,'Widget cards: Watch + Evidence summary',page,async()=>{const side=page.getByRole('complementary',{name:'Widgets'});await side.waitFor({state:'attached'});await side.scrollIntoViewIfNeeded();
  await side.getByRole('heading',{name:'Watch',exact:true}).waitFor({state:'visible'});await side.getByRole('heading',{name:'Evidence summary',exact:true}).waitFor({state:'attached'});return {shot:await shot(page,vp,'widget-cards',side)};});
 const sheetLabel=async()=>page.evaluate(()=>{const r=Array.from(document.querySelectorAll('[role=region]')).find(e=>/ panel, /.test(e.getAttribute('aria-label')||''));return r?{label:r.getAttribute('aria-label'),top:Math.round(r.getBoundingClientRect().top)}:null;});
 await check('2.8a',vp,'BottomSheet header tabs + snap via buttons (peek→half→full→half→peek)',page,async()=>{
  const list=page.getByRole('tablist',{name:'Workspace panels'});await list.waitFor({state:'visible'});const names=await list.getByRole('tab').evaluateAll(els=>els.map(e=>e.getAttribute('aria-label')));if(JSON.stringify(names)!==JSON.stringify(PHONE_TABS))throw new Error('tabs '+short(names));
  const seq=[];const at=async(re)=>{const s=await until(async()=>{const s=await sheetLabel();return {ok:!!s&&re.test(s.label),...s};},{timeout:6000,msg:'sheet '+re});await sleep(500);seq.push({label:s.label,top:(await sheetLabel()).top});};
  await at(/collapsed$/);await shot(page,vp,'sheet-peek');
  const lab=(await sheetLabel()).label.replace(/, (collapsed|half open|full screen)$/,'');
  await page.getByRole('button',{name:`Expand ${lab}`}).click();await at(/half open$/);await shot(page,vp,'sheet-half');
  await page.getByRole('button',{name:`Expand ${lab}`}).click();await at(/full screen$/);await shot(page,vp,'sheet-full');
  await page.getByRole('button',{name:`Collapse ${lab}`}).click();await at(/half open$/);
  await page.getByRole('button',{name:`Collapse ${lab}`}).click();await at(/collapsed$/);return {tabs:names,sequence:seq};});
 // Real touch drag via CDP (what a phone sends): grab strip up 400px, then down to peek.
 const touchDrag=async(x,y0,dy,steps=20)=>{const cdp=await page.context().newCDPSession(page);await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y:y0}]});
  for(let k=1;k<=steps;k++){await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x,y:y0+dy*k/steps}]});await sleep(20);}await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await cdp.detach();};
 await check('2.8b',vp,'BottomSheet touch drag changes snap (up from peek, down back to peek)',page,async()=>{
  const s0=await sheetLabel();if(!/collapsed$/.test(s0.label))throw new Error('not at peek before drag: '+s0.label);
  await touchDrag(120,s0.top+7,-400);const up=await until(async()=>{const s=await sheetLabel();return {ok:!/collapsed$/.test(s.label),...s};},{timeout:5000,msg:'touch drag up leaves peek'});await sleep(500);const sh=await shot(page,vp,'sheet-after-touch-drag-up');
  const s1=await sheetLabel();await touchDrag(120,s1.top+7,(page.viewportSize().height-s1.top)+200,25);
  const down=await until(async()=>{const s=await sheetLabel();return {ok:/collapsed$/.test(s.label),...s};},{timeout:5000,msg:'touch drag down returns to peek'});
  return {start:s0.label,afterUp:up.label,afterDown:down.label,shot:sh};});
 await check('2.8d',vp,'BottomSheet mouse drag on header (narrow desktop-mouse case, informational)',page,async()=>{
  const s0=await sheetLabel();await page.mouse.move(120,s0.top+7);await page.mouse.down();for(let k=1;k<=20;k++){await page.mouse.move(120,s0.top+7-k*20);await sleep(20);}await page.mouse.up();await sleep(900);
  const s1=await sheetLabel();if(!/collapsed$/.test(s1.label)){await page.getByRole('button',{name:/^Collapse .* panel$/}).first().click().catch(()=>{});}
  return {info:{before:s0.label,after:s1.label,snapChanged:s0.label!==s1.label}};});
 await check('2.8c',vp,'Sheet tab switch (Watch) opens half with its content',page,async()=>{
  await page.getByRole('tablist',{name:'Workspace panels'}).getByRole('tab',{name:'Watch',exact:true}).click();
  const s=await until(async()=>{const s=await sheetLabel();return {ok:/^Watch panel, half open$/.test(s.label),...s};},{timeout:6000,msg:'Watch panel half'});
  await page.locator('[role=tabpanel][aria-label="Watch"]').getByText('My watch',{exact:true}).waitFor({state:'visible',timeout:10000});await sleep(400);const sh=await shot(page,vp,'sheet-watch-half');
  await page.getByRole('tablist',{name:'Workspace panels'}).getByRole('tab',{name:'Discover',exact:true}).click();await page.getByRole('button',{name:/^Collapse Discover panel$/}).click().catch(()=>{});return {label:s.label,shot:sh};});
}

async function runViewport(browser,state,vp){
 const ctx=await browser.newContext({viewport:{width:vp.width,height:vp.height},isMobile:!!vp.isMobile,hasTouch:!!vp.hasTouch,deviceScaleFactor:vp.isMobile?2:1,storageState:state});
 await ctx.addInitScript(installSnap);const page=await ctx.newPage();page.setDefaultTimeout(15000);
 const errors=[];consoleByVp[vp.key]=errors;page.on('pageerror',e=>errors.push({type:'pageerror',text:e.message.slice(0,500),url:page.url()}));page.on('console',m=>{if(m.type()==='error')errors.push({type:'console',text:m.text().slice(0,500),url:page.url()});});
 const counters={trading:0};page.on('request',r=>{try{if(new URL(r.url()).pathname==='/api/trading')counters.trading++;}catch{}});
 metrics[vp.key]={viewport:`${vp.width}x${vp.height}`};
 await check('2.0',vp.key,'Workspace loads with auto-selected top Discover result',page,async()=>{
  await page.goto(BASE+'/chart',{waitUntil:'domcontentloaded',timeout:45000});await workspaceLoaded(page);await sleep(1500);const s=await snap(page);
  metrics[vp.key].autoSelected={header:s.header,headerSrc:s.headerSrc,headingSym:s.headingSym,legendContainerSym:s.legendContainerSym,legendToggleSym:s.legendToggleSym,title:s.title,url:`${s.path}?s=${s.s}&tf=${s.tf}`};if(!s.header||!s.title.startsWith(s.header+' · '))throw new Error(short(s));
  return {header:s.header,headerSrc:s.headerSrc,title:s.title,s:s.s,full:await shot(page,vp.key,'full',null,true)};},90000);
 // Layout metrics first, on the untouched initial load (before any scrolling, sheet snaps or dock resizes).
 await layoutMetrics(page,vp.key,{phone:vp.mode==='phone'});
 if(vp.mode==='desktop')await regionsDesktopLike(page,vp.key);
 if(vp.key==='desktop')await duplicateControls(page,vp.key);
 if(vp.mode==='tablet')await runTablet(page,vp.key);
 if(vp.mode==='phone')await runPhone(page,vp.key);
 if(vp.key==='desktop')await runDesktopSuite(page,vp.key,counters);
 if(vp.key==='short'){
  // (2.11 Discover rows at default dock height now runs in layoutMetrics for desktop + short.)
  await check('2.14',vp.key,'Short laptop: dock tab bar and Prepare row reachable (metric)',page,async()=>{const m=await page.evaluate(()=>{const tl=document.querySelector('[aria-label="Workspace dock tabs"]'),pb=Array.from(document.querySelectorAll('[aria-label^="Prepare trade"]')).find(e=>e.getClientRects().length&&!e.closest('[role="complementary"]'));const r=e=>e?{top:Math.round(e.getBoundingClientRect().top),bottom:Math.round(e.getBoundingClientRect().bottom)}:null;return {tabBar:r(tl),prepareButton:r(pb),innerHeight};});metrics.short.reach=m;return {info:m};});
 }
 await shot(page,vp.key,'final',null,true);
 await check('2.13',vp.key,'Console: no pageerror / console.error',page,async()=>{if(errors.length)throw new Error(`${errors.length} error(s): `+short(errors.slice(0,4)));return {errors:0};});
 await ctx.close();
}

(async()=>{
 const started=new Date().toISOString();
 try{const r=await fetch(BASE+'/health');if(!r.ok)throw new Error('status '+r.status);}catch(e){console.error('QA server on 8083 is not responding: '+e.message+' (not restarting it).');process.exit(3);}
 const browser=await chromium.launch({channel:'msedge',headless:true});let state;
 try{
  const ph={id:'1.0',vp:'all',name:'Login via /signin with fixture account'};
  try{state=await login(browser);results.push({...ph,status:'PASS',detail:`signed in as fixture account (pattern set: ${PATTERN_SET})`});console.log(`PASS login (pattern set: ${PATTERN_SET})`);}
  catch(e){results.push({...ph,status:'FAIL',detail:e.message});console.log('FAIL login: '+e.message);}
  if(state)for(const vp of VIEWPORTS){try{await runViewport(browser,state,vp);}catch(e){results.push({id:'x',vp:vp.key,name:'viewport run crashed',status:'FAIL',detail:e.message});console.log('CRASH '+vp.key+': '+e.message);}}
 }finally{await browser.close();}
 const fails=results.filter(r=>r.status==='FAIL');
 const rapid=extra.rapid||[];fs.writeFileSync(path.join(OUT,'rapid-switch-samples.json'),JSON.stringify(rapid,null,1));
 fs.writeFileSync(path.join(OUT,'results.json'),JSON.stringify({base:BASE,started,finished:new Date().toISOString(),summary:{pass:results.filter(r=>r.status==='PASS').length,info:results.filter(r=>r.status==='INFO').length,fail:fails.length},results,metrics,console:consoleByVp,responsiveness:extra.responsiveness,backForward:extra.backForward,rapidSummary:rapid.map(({samplesDetail,...r})=>r)},null,1));
 console.log('\n==== SUMMARY ====');for(const r of results)console.log(`${r.status.padEnd(4)} ${String(r.vp).padEnd(7)} ${r.id.padEnd(5)} ${r.name}${r.note?' [note: '+r.note+']':''}${r.status==='FAIL'?'\n      -> '+r.detail.slice(0,300):''}`);
 console.log(`\n${results.length} checks · ${fails.length} failed · artifacts in qa/phase0/`);
 process.exitCode=fails.length?1:0;
})().catch(e=>{console.error(e);process.exitCode=1;});
