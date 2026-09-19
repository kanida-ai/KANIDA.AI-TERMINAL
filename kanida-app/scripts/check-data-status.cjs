// Pure checks for src/layout/dataStatus.ts — the tone, copy and rows behind the Data status
// popover and the top-bar data pill. Run: node scripts/check-data-status.cjs (no server needed).
//
// The three things worth a test: the IST maths must not depend on the machine's time zone, a
// vendor's published delay must never read as staleness, and "stalled" must only be claimed while
// the market is open. `now` is injected everywhere so the checks are deterministic.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const load=(rel,req)=>{const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..',rel),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;const ctx={exports:{},require:req,process:{env:{}}};vm.runInNewContext(code,ctx);return ctx.exports;};
const constants=load('src/constants.ts',n=>{throw Error(n)});
const D=load('src/layout/dataStatus.ts',n=>{if(n==='../constants')return constants;throw Error(n)});
let checks=0;const ok=(fn)=>{fn();checks++;};

// A naive-IST stamp as a real Date, so `now` in these checks is zone-independent.
const at=(stamp)=>new Date(D.istMs(stamp));
const LIVE_AT_1420={
 version:1,as_of:'2026-09-16 14:20:00',timezone:'Asia/Kolkata',
 provider:{id:'kite',label:'Zerodha Kite',delay_seconds:0,note:null},
 source:{kind:'market15',label:'Live 15-minute store',live:true,store:'market15.db',note:null},
 latest_bar:{start:'2026-09-16 14:00:00',end:'2026-09-16 14:15:00',by_timeframe:{'1H':'2026-09-16 14:15:00'},note:null},
 last_run:{run_id:'run_1',started_at:'2026-09-16 14:17:00',finished_at:'2026-09-16 14:20:00',status:'ok',
  provider:'kite',symbols:500,requests:500,bars_written:500,daily_bars_written:0,errors:0,seconds:172.4,
  running:false,age_seconds:0,started_age_seconds:180,last_success_at:'2026-09-16 14:20:00',last_success_age_seconds:0,note:null},
 last_run_note:null,
 next_refresh:{at:'2026-09-16 14:30:00',in_seconds:600,note:'The bar closes at 2026-09-16 14:30:00; the loop then needs a cycle (a few minutes for NIFTY 500) to write it.'},
 session:{state:'open',reason:null,date:'2026-09-16',open:'2026-09-16 09:15:00',close:'2026-09-16 15:30:00',next_open:'2026-09-17 09:15:00',note:null},
 stalled:{value:false,after_minutes:20,note:null},stale:{value:false,note:null},
};
const clone=(extra)=>JSON.parse(JSON.stringify({...LIVE_AT_1420,...extra}));
const LEGACY={
 version:1,as_of:'2026-09-16 14:20:00',
 provider:{id:'kite',label:'Zerodha Kite',delay_seconds:0,note:null},
 source:{kind:'legacy',label:'Stored research scan',live:false,store:'kanida.db',
  note:'SCANNER_CANDLE_SOURCE=legacy: candles come from the frozen research store, not from a live feed.'},
 latest_bar:{start:'2026-07-31',end:null,by_timeframe:{},note:'The stored research source keeps daily bars only.'},
 last_run:null,last_run_note:'The stored research source is refreshed by an operator run, not by the live ingest loop.',
 next_refresh:{at:null,in_seconds:null,note:'The stored research source advances only when an operator refreshes it.'},
 session:{state:'open',reason:null,date:'2026-09-16',open:'2026-09-16 09:15:00',close:'2026-09-16 15:30:00',next_open:'2026-09-17 09:15:00',note:null},
 stalled:{value:false,after_minutes:20,note:'Only the live 15-minute store is expected to refresh on its own.'},
 stale:{value:false,note:null},
};

// -- IST maths ---------------------------------------------------------------
ok(()=>assert.equal(D.istMs('2026-09-16 14:15:00'),Date.UTC(2026,8,16,8,45,0)));
ok(()=>assert.equal(D.istMs('2026-09-16'),Date.UTC(2026,8,15,18,30,0)));
ok(()=>assert.equal(D.istMs('rubbish'),undefined));
ok(()=>assert.equal(D.formatIst('2026-09-16 14:15:00',at('2026-09-16 14:20:00')),'16 Sep 14:15'));
ok(()=>assert.equal(D.formatIst('2025-12-31 15:30:00',at('2026-09-16 14:20:00')),'31 Dec 2025 15:30'));
ok(()=>assert.equal(D.formatIst('2026-07-31',at('2026-09-16 14:20:00')),'31 Jul'));
ok(()=>assert.equal(D.formatIst(null),'—'));
// the market date is IST's, not the browser's: 2026-07-31 is 46 days before 2026-09-15 IST
ok(()=>assert.equal(D.dataAgeDays('2026-07-31',at('2026-09-15 23:00:00')),46));
ok(()=>assert.equal(D.dataAgeDays('2026-07-31',at('2026-09-16 00:30:00')),47));
ok(()=>assert.equal(D.formatDataDate('2026-07-31'),'31 Jul 2026'));
ok(()=>{assert.equal(D.agoText(0),'just now');assert.equal(D.agoText(89),'just now');assert.equal(D.agoText(720),'12 min ago');
 assert.equal(D.agoText(11100),'3h 5m ago');assert.equal(D.agoText(7200),'2h ago');assert.equal(D.agoText(86400*4),'4 days ago');assert.equal(D.agoText(null),'—');});
ok(()=>{assert.equal(D.inText(30),'in under a minute');assert.equal(D.inText(600),'in 10 min');assert.equal(D.inText(7500),'in 2h 5m');});

// -- lag vs vendor delay -----------------------------------------------------
ok(()=>assert.equal(D.barLagSeconds(LIVE_AT_1420,at('2026-09-16 14:20:00')),300));
ok(()=>{ // 15-minute vendor, same bar, same clock: the delay is subtracted, so the excess is 0
 const vendor=clone({provider:{id:'vendor15',label:'15-minute delayed vendor',delay_seconds:900,note:null}});
 assert.equal(D.barLagSeconds(vendor,at('2026-09-16 14:20:00')),0);});

// -- tone --------------------------------------------------------------------
ok(()=>assert.equal(D.dataStatusTone(null,0),'fresh'));
ok(()=>assert.equal(D.dataStatusTone(null,46),'very-stale'));
ok(()=>assert.equal(D.dataStatusTone(null,4),'stale'));
ok(()=>assert.equal(D.dataStatusTone(null,undefined),'unknown'));
ok(()=>assert.equal(D.dataStatusTone(LIVE_AT_1420,0,3,at('2026-09-16 14:20:00')),'fresh'));
ok(()=>assert.equal(D.dataStatusTone(clone({stalled:{value:true,after_minutes:20,note:'x'}}),0,3,at('2026-09-16 14:20:00')),'very-stale'));
ok(()=>assert.equal(D.dataStatusTone(clone({stale:{value:true,note:'x'}}),0,3,at('2026-09-16 14:20:00')),'stale'));
// the normal wait for the next bar is not amber: 35 min behind = one 15-min bar + the 20-min stall limit
ok(()=>{const waiting=clone({latest_bar:{start:'2026-09-16 13:30:00',end:'2026-09-16 13:45:00',by_timeframe:{},note:null}});
 assert.equal(D.dataStatusTone(waiting,0,3,at('2026-09-16 14:20:00')),'fresh');});
// a healthy 15-minute-delayed vendor stays green; the same clock with delay 0 goes amber
ok(()=>{const late=clone({latest_bar:{start:'2026-09-16 13:15:00',end:'2026-09-16 13:30:00',by_timeframe:{},note:null}});
 assert.equal(D.dataStatusTone(late,0,3,at('2026-09-16 14:20:00')),'stale');
 const vendor={...late,provider:{id:'vendor15',label:'15-minute delayed vendor',delay_seconds:900,note:null}};
 assert.equal(D.dataStatusTone(vendor,0,3,at('2026-09-16 14:20:00')),'fresh');});
// outside market hours the same gap is not amber: the loop is idle by design
ok(()=>{const closed=clone({latest_bar:{start:'2026-09-16 15:15:00',end:'2026-09-16 15:30:00',by_timeframe:{},note:null},
 session:{state:'closed',reason:null,date:'2026-09-16',open:'2026-09-16 09:15:00',close:'2026-09-16 15:30:00',next_open:'2026-09-17 09:15:00',note:null}});
 assert.equal(D.dataStatusTone(closed,0,3,at('2026-09-16 22:00:00')),'fresh');});
// a live, current feed after a long weekend is not stale just because the calendar says 4 days
ok(()=>assert.equal(D.dataStatusTone(LIVE_AT_1420,4,3,at('2026-09-16 14:20:00')),'fresh'));
// a scanner that could not build the object falls back to the calendar-age tone
ok(()=>assert.equal(D.dataStatusTone({error:'boom'},46,3),'very-stale'));

// -- pill copy ---------------------------------------------------------------
ok(()=>{ // no data_status: byte-for-byte what the pill said before this popover existed
 const p=D.pillContent(null,{dataEnd:'2026-07-31',ageDays:46,now:at('2026-09-16 14:20:00')});
 assert.equal(p.text,'Data: 31 Jul 2026 · STALE 46d');
 assert.equal(p.label,'Market data ends 31 Jul 2026, 46 days old, stale, limit 3 days');
 assert.equal(D.pillContent(null,{dataEnd:'2026-07-31',ageDays:46,compact:true}).text,'STALE 46d');});
ok(()=>{const p=D.pillContent(null,{});assert.equal(p.text,'Data age unknown');assert.equal(p.label,'Market data age unknown');});
ok(()=>{const p=D.pillContent(LIVE_AT_1420,{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(p.text,'Live · Prices 14:15 · Patterns 14:15');assert.equal(p.tone,'fresh');
 assert.equal(p.label,'Market data live from Zerodha Kite: prices 16 Sep 14:15 IST, patterns updated 14:15');});
ok(()=>{const p=D.pillContent(LIVE_AT_1420,{ageDays:0,compact:true,now:at('2026-09-16 14:20:00')});assert.equal(p.text,'Live 14:15');});
ok(()=>{const p=D.pillContent(clone({stalled:{value:true,after_minutes:20,note:'x'}}),{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(p.text,'Live · STALLED · Prices 14:15 · Patterns 14:15');assert.equal(p.tone,'very-stale');assert.match(p.label,/ingestion stalled$/);});
// every pill label still starts with "Market data" — the e2e selectors and screen readers rely on it
ok(()=>{for(const s of [null,LIVE_AT_1420,LEGACY,{error:'boom'}])
 assert.match(D.pillContent(s,{dataEnd:'2026-07-31',ageDays:46,now:at('2026-09-16 14:20:00')}).label,/^Market data/);});

// -- the popover view --------------------------------------------------------
const rowOf=(view,key)=>view.rows.find(r=>r.key===key);
ok(()=>{const v=D.dataStatusView(LIVE_AT_1420,{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Live');assert.equal(v.tone,'fresh');assert.equal(v.live,true);
 assert.equal(v.headline,'Prices 14:15 · Patterns updated 14:15');
 // an older scanner without the patterns block: say what is missing, never guess a next update
 assert.equal(v.detail,'The scanner does not report when patterns next update · prices from Zerodha Kite, 5 min ago');
 assert.equal(rowOf(v,'patterns-next').value,'Unknown');assert.match(rowOf(v,'patterns-next').note,/does not report/);
 assert.equal(v.warning,null);
 assert.match(v.expectation,/^Prices update every 15 minutes. Patterns update only when a candle completes/);
 assert.equal(rowOf(v,'source').value,'Live 15-minute store · Zerodha Kite');
 assert.equal(rowOf(v,'bar').value,'16 Sep 14:15 IST · 5 min ago');
 assert.equal(rowOf(v,'refresh').value,'16 Sep 14:20 · just now — 500 bars written, 500 symbols');
 assert.equal(rowOf(v,'next').value,'16 Sep 14:30 IST · in 10 min');
 assert.equal(rowOf(v,'session').value,'Open · closes 16 Sep 15:30');
 assert.equal(rowOf(v,'delay'),undefined);
 assert.match(v.a11y,/^Data status: Prices 14:15 · Patterns updated 14:15./);});
ok(()=>{const v=D.dataStatusView(LEGACY,{dataEnd:'2026-07-31',ageDays:46,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Historical');assert.equal(v.live,false);assert.equal(v.tone,'very-stale');
 assert.equal(v.headline,'Historical · data to 31 Jul 2026 (46 days old)');
 assert.equal(rowOf(v,'refresh').value,'Never');
 assert.equal(rowOf(v,'next').value,'Not scheduled');
 assert.match(v.warning.text,/46 days old/);
 assert.equal(v.expectation,'A stored scan only changes when the research data is refreshed.');});
ok(()=>{const v=D.dataStatusView(clone({stalled:{value:true,after_minutes:20,
  note:'The market is open but the last ingest cycle finished 80 minutes ago (limit 20).'}}),{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Stalled');assert.equal(v.tone,'very-stale');
 assert.equal(v.warning.tone,'very-stale');assert.match(v.warning.text,/price feed looks stalled\. The market is open/);});
ok(()=>{const v=D.dataStatusView(clone({stale:{value:true,note:'The newest bar is behind the newest session the calendar expects.'}}),
  {ageDays:1,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Behind');assert.equal(v.tone,'stale');assert.match(v.warning.text,/behind the market/);});
ok(()=>{const vendor=clone({provider:{id:'vendor15',label:'15-minute delayed vendor',delay_seconds:900,note:null}});
 const v=D.dataStatusView(vendor,{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Live · 15 min delayed');assert.equal(v.tone,'fresh');assert.equal(v.warning,null);
 assert.equal(rowOf(v,'delay').value,'15 min, by design');
 assert.match(v.detail,/published 15 min late by design/);});
ok(()=>{const v=D.dataStatusView(clone({last_run:{...LIVE_AT_1420.last_run,errors:7}}),{ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.match(rowOf(v,'refresh').value,/7 errors/);assert.match(v.warning.text,/7 errors/);});
// a cycle in flight: the row says so and dates its counters by the last finish, never "Never"
ok(()=>{const v=D.dataStatusView(clone({last_run:{...LIVE_AT_1420.last_run,run_id:'run_2',started_at:'2026-09-16 14:22:00',
  finished_at:null,status:'running',running:true,age_seconds:null,started_age_seconds:120,
  last_success_at:'2026-09-16 14:20:00',last_success_age_seconds:240,
  note:'This cycle is still running; the counters are from the last finished one.'},
 stalled:{value:false,after_minutes:20,note:'A cycle is running now; it started 2 minutes ago.'}}),
 {ageDays:0,now:at('2026-09-16 14:24:00')});
 assert.equal(rowOf(v,'refresh').value,'running now, started 16 Sep 14:22 · last finished 16 Sep 14:20 — 500 bars written, 500 symbols');
 assert.equal(v.warning,null);assert.equal(v.tone,'fresh');});
ok(()=>{const v=D.dataStatusView(clone({last_run:null,last_run_note:'No ingest run is recorded in this store yet.'}),
  {ageDays:0,now:at('2026-09-16 14:20:00')});
 assert.equal(rowOf(v,'refresh').value,'Never');assert.equal(rowOf(v,'refresh').note,'No ingest run is recorded in this store yet.');});
ok(()=>{const v=D.dataStatusView(clone({session:{state:'holiday',reason:'weekend',date:'2026-09-19',open:null,close:null,next_open:'2026-09-21 09:15:00',note:'NSE is closed today (weekend).'}}),
  {ageDays:0,now:at('2026-09-19 11:00:00')});
 assert.equal(rowOf(v,'session').value,'Closed (weekend) · next open 21 Sep 09:15');
 assert.equal(v.expectation,'Prices and patterns update again once the market opens.');});
ok(()=>{const v=D.dataStatusView(null,{ageDays:46,now:at('2026-09-16 14:20:00')});
 assert.equal(v.kind,'Unknown');assert.equal(v.headline,'Data source unknown');assert.equal(v.tone,'very-stale');});
ok(()=>{const v=D.dataStatusView({error:'market15.db could not be opened read-only'},{ageDays:0});
 assert.equal(v.kind,'Unknown');assert.match(v.rows[0].note,/could not be opened/);});

// -- one story: prices vs patterns (backlog item 2, point 5) -----------------
// The real /api/state.data_status of 17 Sep 2026 10:01 IST, plus the `patterns` block the scanner now adds.
const FRAMES=(v,w)=>({'1H':{scanned:v,expected:v,next_close:'2026-09-17 10:15:00',due:false,behind:false},'4H':{scanned:v,expected:v,next_close:'2026-09-17 13:15:00',due:false,behind:false},
 '1D':{scanned:v,expected:v,next_close:'2026-09-17 15:30:00',due:false,behind:false},'1W':{scanned:w,expected:w,next_close:'2026-09-18 15:30:00',due:false,behind:false}});
const REAL_1001={...clone({}),as_of:'2026-09-17 10:01:16',
 latest_bar:{start:'2026-09-17 09:30:00',end:'2026-09-17 09:45:00',by_timeframe:{'1H':'2026-09-16 15:30:00','4H':'2026-09-16 15:30:00','1D':'2026-09-16 15:30:00','1W':'2026-09-11 15:30:00'},note:null},
 next_refresh:{at:'2026-09-17 10:15:00',in_seconds:824,note:null},
 session:{state:'open',reason:null,date:'2026-09-17',open:'2026-09-17 09:15:00',close:'2026-09-17 15:30:00',next_open:'2026-09-18 09:15:00',note:null},
 patterns:{latest:'2026-09-16 15:30:00',next_update:'2026-09-17 10:15:00',up_to_date:true,updating:[],behind:[],by_timeframe:FRAMES('2026-09-16 15:30:00','2026-09-11 15:30:00'),note:'Every timeframe has scanned its newest completed candle.'}};
const T1001=at('2026-09-17 10:01:16');
ok(()=>{ // live, waiting for the first hourly candle: no STALE, no "1 day old", next update named
 const st=D.dataStory(REAL_1001,T1001,1);
 assert.equal(st.line,'Prices 09:45 · patterns updated 16 Sep 15:30 · next update 10:15');
 assert.equal(st.pill,'Live · Prices 09:45 · Patterns 16 Sep 15:30');assert.equal(st.compact,'Live 09:45');
 assert.equal(st.tone,'fresh');assert.equal(st.pricesBehind,null);assert.equal(st.patternsBehind,null);assert.equal(st.missing.length,0);
 const p=D.pillContent(REAL_1001,{dataEnd:'2026-09-16',ageDays:1,now:T1001});
 assert.equal(p.line,st.line);assert.doesNotMatch(p.text+p.line+p.label,/STALE|day old|days old/);
 const v=D.dataStatusView(REAL_1001,{dataEnd:'2026-09-16',ageDays:1,now:T1001});
 assert.equal(v.headline,'Prices 09:45 · Patterns updated 16 Sep 15:30');assert.equal(v.warning,null);
 assert.equal(v.detail,'Patterns next update 10:15 · prices from Zerodha Kite, 16 min ago');
 assert.equal(rowOf(v,'patterns-next').value,'17 Sep 10:15 IST · in 14 min');
 assert.equal(rowOf(v,'timeframes').value,'1H 16 Sep 15:30 · 4H 16 Sep 15:30 · 1D 16 Sep 15:30 · 1W 11 Sep 15:30');});
ok(()=>{ // after the 10:15 candle is scanned: the example line from the backlog
 const s=JSON.parse(JSON.stringify(REAL_1001));Object.assign(s.latest_bar,{start:'2026-09-17 10:15:00',end:'2026-09-17 10:30:00'});
 Object.assign(s.patterns,{latest:'2026-09-17 10:15:00',next_update:'2026-09-17 11:15:00'});
 assert.equal(D.dataStory(s,at('2026-09-17 10:33:00')).line,'Prices 10:30 · patterns updated 10:15 · next update 11:15');
 assert.equal(D.dataStory(s,at('2026-09-17 10:33:00')).pill,'Live · Prices 10:30 · Patterns 10:15');});
ok(()=>{ // a candle just closed and its scan is running: "updating now", still green
 const s=JSON.parse(JSON.stringify(REAL_1001));Object.assign(s.patterns,{up_to_date:false,updating:['1H'],next_update:'2026-09-17 11:15:00'});
 const st=D.dataStory(s,at('2026-09-17 10:20:00'));assert.equal(st.tone,'fresh');assert.match(st.line,/ · updating now$/);assert.equal(st.patternsBehind,null);});
ok(()=>{ // genuinely behind: the 10:15 candle closed 30 min ago and was never scanned
 const s=JSON.parse(JSON.stringify(REAL_1001));Object.assign(s.latest_bar,{start:'2026-09-17 10:30:00',end:'2026-09-17 10:45:00'});
 Object.assign(s.patterns,{up_to_date:false,behind:['1H'],next_update:'2026-09-17 11:15:00'});
 Object.assign(s.patterns.by_timeframe['1H'],{expected:'2026-09-17 10:15:00',due:true,behind:true,overdue_seconds:1800});
 const now=at('2026-09-17 10:45:00'),st=D.dataStory(s,now);
 assert.equal(st.tone,'stale');assert.equal(st.pill,'Live · Prices 10:45 · Patterns BEHIND');assert.equal(st.compact,'BEHIND');
 assert.equal(st.line,'Prices 10:45 · patterns BEHIND · last updated 16 Sep 15:30');
 assert.equal(st.patternsBehind,'Patterns are behind: the 1H candle that closed 10:15 (30 min ago) has not been scanned yet.');
 const v=D.dataStatusView(s,{now});assert.equal(v.warning.text,st.patternsBehind);assert.equal(rowOf(v,'patterns').tone,'stale');});
ok(()=>{ // genuinely behind: the price fetcher missed bars (09:45 bar at 10:25 = 40 min, over one bar + 20 min)
 const st=D.dataStory(REAL_1001,at('2026-09-17 10:25:00'));
 assert.equal(st.tone,'stale');assert.equal(st.pill,'Live · BEHIND · Prices 09:45 · Patterns 16 Sep 15:30');
 assert.match(st.line,/^Prices 09:45 \(behind\) · /);assert.match(st.pricesBehind,/newest price bar is 40 min ago/);});
ok(()=>{ // market closed, everything scanned
 const s=JSON.parse(JSON.stringify(REAL_1001));Object.assign(s.latest_bar,{start:'2026-09-16 15:15:00',end:'2026-09-16 15:30:00'});
 s.session={state:'closed',reason:null,date:'2026-09-16',open:'2026-09-16 09:15:00',close:'2026-09-16 15:30:00',next_open:'2026-09-17 09:15:00',note:null};
 s.patterns.next_update='2026-09-17 10:15:00';
 const now=at('2026-09-16 20:00:00'),st=D.dataStory(s,now);
 assert.equal(st.line,'Market closed · prices to 16 Sep 15:30 · patterns up to date');
 assert.equal(st.pill,'Closed · Prices 16 Sep 15:30 · Patterns up to date');assert.equal(st.compact,'Closed');assert.equal(st.tone,'fresh');
 const v=D.dataStatusView(s,{now});assert.equal(v.kind,'Closed');assert.equal(v.headline,'Market closed · Prices to 16 Sep 15:30 · Patterns up to date');
 assert.equal(rowOf(v,'patterns').value,'Up to date · 16 Sep 15:30 candle');assert.equal(v.warning,null);});
ok(()=>{ // missing values are named, not guessed
 const s=JSON.parse(JSON.stringify(REAL_1001));delete s.patterns;s.latest_bar={start:null,end:null,by_timeframe:{},note:null};
 const st=D.dataStory(s,T1001);
 assert.equal(JSON.stringify(st.missing),JSON.stringify(['the newest price bar','the newest candle the pattern scan covered','when patterns next update']));
 assert.equal(st.line,'Prices: newest bar unknown · patterns: last scan unknown');});

// -- Discover cards use the same words ----------------------------------------
ok(()=>{
 assert.equal(D.cardDataLine(REAL_1001,'2026-09-16 15:30:00',T1001),'Pattern as of 16 Sep 15:30 · prices 09:45');
 assert.equal(D.cardWarning(REAL_1001,{stale:true,ageDays:4,now:T1001}),null); // current live feed: no "4 days old"
 assert.equal(D.cardWarning(clone({stalled:{value:true,after_minutes:20,note:'x'}}),{now:at('2026-09-16 14:20:00')}),'Price feed stalled · newest prices 14:15');
 assert.equal(D.cardWarning(null,{stale:true,ageDays:46}),'Research only — prices 46 days old');
 assert.equal(D.cardDataLine(LEGACY,'2026-07-31',at('2026-09-16 14:20:00')),'Data to 31 Jul');});

// -- the scanner connection: what the app says while the scanner is loading or down (BACKLOG item 2a) --------
// The whole point of item 2a is the WORDING: a temporary outage must never read as a broken app, and a lasting
// one must never be dressed up as "reconnecting". These pin both, plus the pill and panel that repeat them.
const CACHED={served_from_cache:true,cached_at:'2026-09-17 15:30:12',cached_as_of:'2026-09-17 15:30:00',
 upstream_error:'The scanner is still starting up.',upstream_down_seconds:40,reconnecting:true};
const NOW17=at('2026-09-17 15:32:00');
ok(()=>assert.equal(D.connectionView(null,{}),null));                       // healthy: nothing is said at all
ok(()=>assert.equal(D.connectionView({served_from_cache:false},{}),null));
ok(()=>{const c=D.connectionView(CACHED,{now:NOW17});
 assert.equal(c.mode,'reconnecting');assert.equal(c.tone,'stale');assert.equal(c.retry,true);
 assert.equal(c.text,'Showing the last scan · 17 Sep 15:30 · reconnecting…');
 assert.ok(!/unavailable|error|failed/i.test(c.text));                      // no error words for a blip
 assert.ok(c.detail.startsWith('The scanner is still starting up.'));});
ok(()=>{ // past the 5-minute grace it is stated plainly, and the data shown is still named
 const c=D.connectionView({...CACHED,upstream_down_seconds:8*60,reconnecting:false,upstream_error:'The scanner is not answering.'},{now:NOW17});
 assert.equal(c.mode,'lost');assert.equal(c.tone,'very-stale');
 assert.equal(c.text,'Showing the last scan · 17 Sep 15:30 · not reconnected for 8 min');
 assert.ok(c.detail.includes('Nothing here has changed since 17 Sep 15:30.'));});
ok(()=>{ // the grace period is the server's, to the second
 assert.equal(D.RECONNECT_GRACE_SECONDS,300);
 assert.equal(D.connectionView({...CACHED,upstream_down_seconds:300},{now:NOW17}).mode,'reconnecting');
 assert.equal(D.connectionView({...CACHED,upstream_down_seconds:301,reconnecting:false},{now:NOW17}).mode,'lost');});
ok(()=>{ // nothing cached at all - the only case that is a real error, and it says what it is
 const c=D.connectionView(null,{error:'Research is temporarily unavailable.',now:NOW17});
 assert.equal(c.mode,'offline');assert.equal(c.text,'The scanner is not answering and there is no earlier scan to show.');
 assert.equal(D.connectionView({unreachable:true,upstream_error:'The scanner is not answering.'},{now:NOW17}).mode,'offline');});
ok(()=>{ // the pill says the same thing, and NEVER "Data age unknown"
 const p=D.pillContent(null,{cache:CACHED,now:NOW17});
 assert.equal(p.text,'Showing the last scan · 17 Sep 15:30 · reconnecting…');assert.equal(p.tone,'stale');assert.equal(p.stale,true);
 assert.ok(!/age unknown/i.test(p.text+p.line+p.label));
 assert.equal(D.pillContent(null,{cache:CACHED,compact:true,now:NOW17}).text,'Last scan 15:30 · reconnecting');
 assert.ok(!/age unknown/i.test(D.pillContent(null,{error:'down',now:NOW17}).text));});
ok(()=>{ // ... and so does the panel, with the live rows kept beside it
 const v=D.dataStatusView(REAL_1001,{cache:CACHED,now:NOW17});
 assert.equal(v.kind,'Last scan');assert.equal(v.headline,'Showing the last scan · 17 Sep 15:30 · reconnecting…');
 assert.equal(rowOf(v,'connection').value,'Reconnecting…');
 assert.equal(rowOf(v,'taken').value,'17 Sep 15:30 IST');
 assert.ok(!!rowOf(v,'source'));                                            // the scan's own rows survive
 assert.ok(/nothing can be simulated or sent live/i.test(v.expectation));});
ok(()=>{const v=D.dataStatusView(null,{error:'down',now:NOW17});
 assert.equal(v.kind,'No scan');assert.ok(!/age unknown|source unknown/i.test(v.headline));});

console.log(`${checks} Data status logic checks passed.`);
