// Pure checks for src/derivative/logic.ts — the Derivative tab's formatting and card wording
// (docs/DERIVATIVES_SPEC.md §3–§5). Run: node scripts/check-derivative.cjs (no server needed).
//
// What is being protected here, in one line: a number that was not captured must read as a dash or the words
// "no baseline", never as zero; and no sentence this file produces may claim what happens next.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const load=(rel,req)=>{const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..',rel),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;const ctx={exports:{},require:req,process:{env:{}}};vm.runInNewContext(code,ctx);return ctx.exports;};
const L=load('src/derivative/logic.ts',n=>{throw Error('unexpected import: '+n)});
let checks=0;const ok=fn=>{fn();checks++};
const DASH='—',MINUS='−',TIMES='×',RUPEE='₹';
const MISSING=[null,undefined,NaN,Infinity,-Infinity,'12','',{}];

// --- the empty sentence is the same one the server sends ---------------------------------------------------
ok(()=>assert.equal(L.EMPTY_TEXT,'No F&O data captured yet — capture starts at the next 15-min reading'));
ok(()=>{
 const py=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','derivatives.py'),'utf8');
 const m=/^EMPTY_TEXT='(.+)'$/m.exec(py);
 assert.ok(m,'derivatives.py must declare EMPTY_TEXT');
 assert.equal(m[1].replace(/\\u([0-9a-f]{4})/g,(_,h)=>String.fromCharCode(parseInt(h,16))),L.EMPTY_TEXT);
});

// --- rule 1: a number that is not there is a dash, never a zero ---------------------------------------------
ok(()=>{for(const v of MISSING){
 assert.equal(L.crore(v),DASH,`crore(${String(v)})`);
 assert.equal(L.units(v),DASH);assert.equal(L.compact(v),DASH);assert.equal(L.signed(v),DASH);
 assert.equal(L.signedUnits(v),DASH);assert.equal(L.price(v),DASH);assert.equal(L.strike(v),DASH);
 assert.equal(L.ratio(v),DASH);assert.equal(L.pcrText(v),DASH);assert.equal(L.basisText(v),DASH);
 assert.equal(L.dteText(v),DASH);
}});
ok(()=>{assert.equal(L.crore(48),`${RUPEE}48.0 cr`);assert.equal(L.crore(48.26,0),`${RUPEE}48 cr`)});
ok(()=>{assert.equal(L.crore(0),`${RUPEE}0.0 cr`),'a captured zero is still a zero'});
ok(()=>{assert.equal(L.units(4200000),'42,00,000');assert.equal(L.units(0),'0')});
ok(()=>{assert.equal(L.compact(3750000),'37.5L');assert.equal(L.compact(12000000),'1.2Cr');
 assert.equal(L.compact(120000000),'12Cr');assert.equal(L.compact(4200),'4.2k');assert.equal(L.compact(-3750000),MINUS+'37.5L');
 assert.equal(L.compact(940),'940')});
ok(()=>{assert.equal(L.signed(1.42),'+1.4%');assert.equal(L.signed(-0.94),MINUS+'0.9%');assert.equal(L.signed(0),'0.0%')});
ok(()=>{assert.equal(L.signedUnits(420000),'+4,20,000');assert.equal(L.signedUnits(-90000),MINUS+'90,000')});
ok(()=>{assert.equal(L.price(1412),`${RUPEE}1,412.00`);assert.equal(L.strike(25000),'25,000')});
ok(()=>{assert.equal(L.basisText(30),`+${RUPEE}30.00`);assert.equal(L.basisText(-12.5),`${MINUS}${RUPEE}12.50`)});
ok(()=>{assert.equal(L.pcrText(0.876),'0.88');assert.equal(L.pcrText(1),'1.00')});

// --- rule 2 (§3.2): a ratio with too little baseline is words, never a number ---------------------------------
const withBaseline=(sessions,ratio,flag)=>({volume_ratio:ratio,volume_baseline_sessions:sessions,volume_baseline:flag});
ok(()=>assert.equal(L.volumeRatio(withBaseline(10,2.4,'ok')),`2.4${TIMES} its 10-session median`));
ok(()=>assert.equal(L.volumeRatioShort(withBaseline(10,2.4,'ok')),`2.4${TIMES}`));
ok(()=>{for(const row of [withBaseline(2,2.4,'none'),withBaseline(2,2.4,'ok'),withBaseline(null,2.4,'ok'),
  withBaseline(10,null,'ok'),withBaseline(10,2.4,'none'),null,undefined,{}]){
 assert.equal(L.volumeRatio(row),'no baseline',JSON.stringify(row));
 assert.equal(L.volumeRatioShort(row),'no baseline',JSON.stringify(row));
}});
ok(()=>{ // the required session count is the server's, not a constant this file invents
 assert.equal(L.volumeRatio(withBaseline(3,1.1,'ok'),5),'no baseline');
 assert.equal(L.volumeRatio(withBaseline(5,1.1,'ok'),5),`1.1${TIMES} its 5-session median`);
});
// §3.3 carries its raw numbers beside the ratio
ok(()=>assert.equal(L.volumeToOi({volume_to_oi:1.24,volume:2100000,previous_oi:1700000}),
 `1.24${TIMES} (21,00,000 traded vs 17,00,000 standing)`));
ok(()=>assert.equal(L.volumeToOi({volume_to_oi:1.24,volume:null,previous_oi:null}),`1.24${TIMES}`));
ok(()=>{assert.equal(L.volumeToOi({}),DASH);assert.equal(L.volumeToOi(null),DASH)});
ok(()=>{assert.equal(L.volumeToOiHot({volume_to_oi:1.01}),true);assert.equal(L.volumeToOiHot({volume_to_oi:1}),false);
 assert.equal(L.volumeToOiHot({}),false),'a missing ratio is never flagged'});

// --- §3.1: the two build-up readings are labelled, never merged or guessed -------------------------------------
ok(()=>assert.deepEqual({...L.BUILDUP_LABELS},{long_buildup:'Long build-up',short_buildup:'Short build-up',
 short_covering:'Short covering',long_unwinding:'Long unwinding'}));
ok(()=>{for(const [k,v] of Object.entries(L.BUILDUP_LABELS))assert.equal(L.buildupLabel(k),v)});
ok(()=>{for(const v of [null,undefined,'','up','bullish','LONG_BUILDUP'])assert.equal(L.buildupLabel(v),DASH,String(v))});
ok(()=>{assert.equal(L.buildupTone('long_buildup'),'up');assert.equal(L.buildupTone('short_covering'),'up');
 assert.equal(L.buildupTone('short_buildup'),'down');assert.equal(L.buildupTone('long_unwinding'),'down');
 assert.equal(L.buildupTone('nonsense'),'flat');assert.equal(L.buildupTone(null),'flat')});
ok(()=>assert.deepEqual({...L.buildupPair({buildup_15m:'long_buildup',buildup_day:'short_buildup'})},
 {fifteen:'Long build-up',day:'Short build-up'}));
ok(()=>assert.deepEqual({...L.buildupPair({buildup_day:'short_buildup'})},{fifteen:DASH,day:'Short build-up'}));

// --- times and expiry ------------------------------------------------------------------------------------------
ok(()=>assert.equal(L.asOfText('2026-09-18 14:45'),'As of 18 Sep 2026 · 14:45 IST'));
ok(()=>assert.equal(L.asOfText('2026-09-18'),'As of 18 Sep 2026'));
ok(()=>{for(const v of [null,undefined,'','nonsense','18/09/2026'])assert.equal(L.asOfText(v),'Not captured yet',String(v))});
ok(()=>{assert.equal(L.clock('2026-09-18 09:30'),'09:30');assert.equal(L.clock(null),DASH)});
ok(()=>{assert.equal(L.dteText(0),'expires today');assert.equal(L.dteText(1),'1 day to expiry');
 assert.equal(L.dteText(8),'8 days to expiry');assert.equal(L.dteText(-2),'expired')});
ok(()=>{assert.equal(L.expiryText('2026-09-25',8),'25 Sep 2026 · 8 days to expiry');
 assert.equal(L.expiryText('2026-09-25',null),'25 Sep 2026');assert.equal(L.expiryText(null,8),DASH)});

// --- §3.6 max pain: the strike, the distance, and the OI it came from --------------------------------------------
ok(()=>assert.equal(L.maxPainText({max_pain_strike:25000,max_pain_distance:120}),'25,000 · spot 120 above'));
ok(()=>assert.equal(L.maxPainText({max_pain_strike:25000,max_pain_distance:-80}),'25,000 · spot 80 below'));
ok(()=>assert.equal(L.maxPainText({max_pain_strike:25000,max_pain_distance:0}),'25,000 · spot at max pain'));
ok(()=>assert.equal(L.maxPainText({max_pain_strike:25000,max_pain_distance:null}),'25,000'));
ok(()=>{assert.equal(L.maxPainText({max_pain_strike:null}),DASH);assert.equal(L.maxPainText(null),DASH)});
ok(()=>assert.equal(L.maxPainBasis(null,null),'Total OI behind it: not captured'));
ok(()=>assert.equal(L.maxPainBasis(3750000,2400000),'From 61,50,000 contracts of open interest (CE 37.5L · PE 24L)'));
ok(()=>assert.equal(L.oiShareText(null),'no baseline'));
ok(()=>assert.equal(L.oiShareText(1.3),`1.30${TIMES} its 20-day average`));

// --- card state: loading → error → nothing captured → nothing passed → rows ---------------------------------------
const env=(extra={})=>({available:true,captured:true,as_of:'2026-09-18 14:45',floors:{premium_cr:2,oi_lots:1,last_price:1},
 floors_text:'',source:'store',missing:[],empty_text:L.EMPTY_TEXT,empty_reason:null,baseline_sessions_required:3,...extra});
ok(()=>assert.deepEqual({...L.cardState(null,{loading:true})},{phase:'loading',text:'Reading the F&O store…'}));
ok(()=>assert.equal(L.cardState(env({rows:[]}),{error:'boom'}).phase,'error'));
ok(()=>{const s=L.cardState(env({available:false,captured:false,empty_reason:L.EMPTY_TEXT,rows:[]}));
 assert.equal(s.phase,'empty');assert.equal(s.text,L.EMPTY_TEXT)});
ok(()=>{const s=L.cardState(env({rows:[],empty_note:'No contract clears the liquidity floors at this mark.'}));
 assert.equal(s.phase,'empty');assert.equal(s.text,'No contract clears the liquidity floors at this mark.')});
ok(()=>assert.equal(L.cardState(env({rows:[{}]})).phase,'ready'));
ok(()=>assert.equal(L.cardState(env({points:[{}]})).phase,'ready'));
// a captured store with rows must never be called empty, and an empty one never "ready"
ok(()=>assert.equal(L.cardState(env({captured:false,rows:[{}]})).phase,'empty'));

// --- floors and "not captured yet" naming --------------------------------------------------------------------------
ok(()=>assert.equal(L.floorsText(null,'Liquidity floors in force: x'),'Liquidity floors in force: x'));
ok(()=>assert.equal(L.floorsText({premium_cr:2,oi_lots:1,last_price:1}),
 `Liquidity floors in force: premium traded ≥ ${RUPEE}2 cr, OI ≥ 1 lot, last price ≥ ${RUPEE}1.`));
ok(()=>assert.equal(L.floorsText(null),''));
ok(()=>assert.equal(L.missingText([]),''));
ok(()=>assert.equal(L.missingText(['volume_ratio','basis'],['volume_ratio']),'Not captured yet: volume vs its average.'));
ok(()=>assert.equal(L.missingText(['oi_change_15m','buildup_day','basis','premium_cr']),
 'Not captured yet: OI change (15 min), build-up (day), basis and 1 more.'));
ok(()=>{assert.equal(L.sourceText('metrics_module'),'Signals read from the derivatives metrics.');
 assert.equal(L.sourceText('store'),'Signals read from the stored 15-min metrics.');
 assert.equal(L.sourceText('none'),'');assert.equal(L.sourceText(null),'')});

// --- §3 roll-up wording -------------------------------------------------------------------------------------------
ok(()=>assert.equal(L.groupSummary({underlying:'RELIANCE',premium_cr:48,strike_count:3,calls:3,puts:0,
 expiries:[],days_to_expiry:8,oi_change_day:420000,strikes:[]}),
 `3 call strikes over the floors · ${RUPEE}48 cr traded · OI +4,20,000 today`));
ok(()=>assert.equal(L.groupSummary({underlying:'X',premium_cr:6.4,strike_count:2,calls:1,puts:1,expiries:[],
 days_to_expiry:null,oi_change_day:null,strikes:[]}),
 `1 call strike and 1 put strike over the floors · ${RUPEE}6.4 cr traded`));
ok(()=>assert.equal(L.strikeSummary({instrument_type:'CE',strike:25000,premium_cr:48,buildup_day:'long_buildup',
 volume_ratio:2.4,volume_baseline_sessions:10,volume_baseline:'ok'}),
 `25,000 call · ${RUPEE}48.0 cr · Long build-up · volume 2.4${TIMES} its 10-session median`));
ok(()=>assert.ok(L.strikeSummary({instrument_type:'PE',strike:1400,premium_cr:null,buildup_day:null,
 volume_ratio:null,volume_baseline_sessions:null,volume_baseline:'none'}).endsWith('volume no baseline')));

// --- linked-panel geometry: a gap in the capture stays a gap ----------------------------------------------------------
const pt=(t,price,oi)=>({t,price,oi});
ok(()=>{const s=L.scaleSeries([pt('a',10,1),pt('b',20,2),pt('c',30,3)],'price',100,50);
 assert.equal(s.lo,10);assert.equal(s.hi,30);
 assert.deepEqual([...s.points].map(p=>[p.x,p.y]),[[0,50],[50,25],[100,0]])});
ok(()=>{const s=L.scaleSeries([pt('a',10,1),pt('b',null,2),pt('c',30,3)],'price',100,50);
 assert.equal(s.points[1],null,'an uncaptured mark is a hole, not an interpolation');
 assert.equal(L.linePath(s),'M0.00,50.00M100.00,0.00','a gap breaks the path into separate strokes')});
ok(()=>{const s=L.scaleSeries([pt('a',10,1),pt('b',15,2),pt('c',30,3)],'price',100,50);
 assert.equal(L.linePath(s),'M0.00,50.00L50.00,37.50L100.00,0.00')});
ok(()=>{for(const bad of [[],[pt('a',1,1)],[pt('a',null,1),pt('b',null,2)]])assert.equal(L.scaleSeries(bad,'price',100,50),null);
 assert.equal(L.scaleSeries([pt('a',1,1),pt('b',2,2)],'price',0,50),null);
 assert.equal(L.linePath(null),'')});
ok(()=>{const flat=L.scaleSeries([pt('a',10,1),pt('b',10,2)],'price',100,50);assert.ok(flat,'a flat series still draws')});
ok(()=>{const rows=[{strike:24800},{strike:25000},{strike:25200}];
 assert.equal(L.nearestStrikeIndex(rows,25050),1);assert.equal(L.nearestStrikeIndex(rows,25190),2);
 assert.equal(L.nearestStrikeIndex(rows,null),null);assert.equal(L.nearestStrikeIndex([],25000),null)});
ok(()=>{assert.equal(L.oiPeak([{ce_oi:10,pe_oi:40},{ce_oi:null,pe_oi:20}]),40);
 assert.equal(L.oiPeak([{ce_oi:null,pe_oi:null}]),null);assert.equal(L.oiPeak([]),null)});

// --- filters -------------------------------------------------------------------------------------------------------
ok(()=>assert.deepEqual({...L.DEFAULT_FILTERS},{watchlist:'all',underlying:'',expiry:'',maxDte:null,optionType:'',minPremiumCr:null}));
ok(()=>assert.equal(L.unusualQuery(L.DEFAULT_FILTERS),'','no filter set sends no query, so the server defaults stand'));
ok(()=>assert.equal(L.unusualQuery({watchlist:'indices',underlying:'RELIANCE',expiry:'2026-09-25',maxDte:7,
 optionType:'CE',minPremiumCr:10}),
 '?underlying=RELIANCE&expiry=2026-09-25&watchlist=indices&option_type=CE&max_dte=7&min_premium_cr=10'));
ok(()=>assert.equal(L.unusualQuery({...L.DEFAULT_FILTERS,maxDte:0}),'?max_dte=0','expiry-day-only is a real filter, not "unset"'));
ok(()=>assert.equal(L.unusualQuery({...L.DEFAULT_FILTERS,underlying:'M&M'}),'?underlying=M%26M'));
ok(()=>assert.equal(L.filterText(L.DEFAULT_FILTERS,'All underlyings'),'All underlyings'));
ok(()=>assert.equal(L.filterText({watchlist:'indices',underlying:'',expiry:'2026-09-25',maxDte:7,optionType:'PE',minPremiumCr:10},'Indices'),
 `Indices · 25 Sep 2026 · Within 7 days · Puts only · ≥ ${RUPEE}10 cr`));
ok(()=>{const e=[{underlying:'NIFTY',expiry:'2026-10-30',days_to_expiry:43},{underlying:'NIFTY',expiry:'2026-09-25',days_to_expiry:8},
  {underlying:'RELIANCE',expiry:'2026-09-25',days_to_expiry:8}];
 assert.deepEqual([...L.expiriesFor(e,'NIFTY')].map(x=>x.expiry),['2026-09-25','2026-10-30']);
 assert.deepEqual([...L.expiriesFor(e,'')].map(x=>x.expiry),['2026-09-25','2026-10-30'],'the same expiry is not listed twice')});
ok(()=>{assert.deepEqual([...L.DTE_CHOICES].map(c=>c.value),[null,0,7,30]);
 assert.equal(L.PREMIUM_CHOICES[0].value,null,'the first premium choice is the server floor itself')});

// a filter set restored from localStorage is re-typed before it can reach a query string
ok(()=>assert.deepEqual({...L.sanitizeFilters(null)},{...L.DEFAULT_FILTERS}));
ok(()=>assert.deepEqual({...L.sanitizeFilters({watchlist:'indices',underlying:'RELIANCE',expiry:'2026-09-25',maxDte:7,optionType:'CE',minPremiumCr:10})},
 {watchlist:'indices',underlying:'RELIANCE',expiry:'2026-09-25',maxDte:7,optionType:'CE',minPremiumCr:10}));
ok(()=>assert.deepEqual({...L.sanitizeFilters({underlying:{},expiry:'25/09/2026',maxDte:'7',optionType:'XX',minPremiumCr:-4,watchlist:42})},
 {...L.DEFAULT_FILTERS}));
ok(()=>assert.equal(L.unusualQuery(L.sanitizeFilters({underlying:"x' or 1=1"})),'','a junk underlying is dropped, never sent'));

// --- §5: nothing on this tab may read as a forecast --------------------------------------------------------------------
ok(()=>{
 const source=fs.readFileSync(path.join(__dirname,'..','src','derivative','logic.ts'),'utf8');
 // Only the words inside produced strings are checked; comments explain the rule and are exempt.
 const strings=source.split('\n').filter(line=>!line.trim().startsWith('//')).join('\n').match(/'[^']*'|`[^`]*`/g)||[];
 const banned=/\b(will|expect|forecast|predict|likely|should rise|should fall|target price|bullish signal|bearish signal)\b/i;
 for(const text of strings)assert.ok(!banned.test(text),`a user-facing string must not predict: ${text}`);
});


// --- the Customize filter builder: every rule must be a query the SERVER validates --------------------------
// This is the rule that keeps the popup honest. If a column, an operator or a value could not be turned into one
// of the pilot's own parameters, the table would be showing one list under another list's as-of time and floors.
const APP=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','app.py'),'utf8');
// The screener does NOT take named FastAPI parameters: it reads the query string whole and answers an unknown
// key with a 400 that names it. So its filter names are pinned to the server's own filter table instead — which
// is the check that would have caught `market` (the server calls it `underlying_kind`) and `watchlist` (which
// the screener has no parameter for at all) before either reached the wire.
const DERIV_PY=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','derivatives.py'),'utf8');
const IV_PY=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','implied_vol.py'),'utf8');
const SCREENER_KEYS=(()=>{
 const at=DERIV_PY.indexOf(' SCREENER_FILTERS={');
 assert.ok(at>0,'derivatives.py must declare SCREENER_FILTERS');
 const body=DERIV_PY.slice(at,DERIV_PY.indexOf('\n }\n',at));
 return new Set([...body.matchAll(/^\s{2}'([a-z_0-9]+)':\{/gm)].map(m=>m[1]));
})();
ok(()=>{
 assert.ok(SCREENER_KEYS.size>=10,`the screener's filter table looks unread: ${[...SCREENER_KEYS].join(',')}`);
 for(const column of L.FILTER_COLUMNS){
  if(column.route==='screener'){
   // a screener column's parameter must be one the screener's own table declares - and so must its twin,
   // because a two-sided rule sends the other bound the moment the operator flips
   for(const key of [column.param,column.twin].filter(Boolean))assert.ok(SCREENER_KEYS.has(key),
    `${column.key} sends "${key}", which the screener's filter table does not offer`);
   assert.equal(!!column.twin,column.operators.length>1,
    `${column.key} has ${column.operators.length} operators, so it ${column.twin?'must not':'must'} declare a twin`);
  }else if(column.noScreener){
   // and a column flagged as not-for-the-screener must really be one the screener cannot answer
   assert.ok(!SCREENER_KEYS.has(column.param),
    `${column.key} is flagged noScreener, but the screener does offer "${column.param}"`);
   assert.ok(new RegExp(`\\b${column.param}\\s*:\\s*(str|int|float)\\s*=`).test(APP),
    `${column.key} maps to ${column.param}, which no derivative route accepts`);
  }else{
   // the shared six: answered by the older routes as named parameters, and by the screener from its table
   assert.ok(new RegExp(`\\b${column.param}\\s*:\\s*(str|int|float)\\s*=`).test(APP),
    `${column.key} maps to ${column.param}, which no derivative route accepts`);
  }
  assert.ok(column.operators.length,`${column.key} must offer at least one operator`);
  for(const op of column.operators)assert.ok(L.OPERATOR_LABELS[op],`unknown operator ${op}`);
 }
});
ok(()=>{ // every rule the builder can make reaches the screener as a key the screener offers, or not at all
 const value=(key)=>key==='buildup'?L.BUILDUP_VALUES[0]:key==='moneyness'?'atm':key==='market'?'index'
  :key==='underlying'?'RELIANCE':key==='expiry'?'2026-09-25':key==='optionType'?'CE'
  :key==='watchlist'?'indices':'2';
 for(const column of L.FILTER_COLUMNS){
  const query=L.screenerQuery([{column:column.key,operator:column.operators[0],value:value(column.key)}]);
  if(!query){assert.ok(column.noScreener||column.key==='watchlist',`${column.key} sends nothing to the screener`);continue}
  for(const part of query.slice(1).split('&')){
   const key=part.split('=')[0];
   assert.ok(SCREENER_KEYS.has(key),`${column.key} sends "${key}", which the screener would answer with a 400`);
  }
 }
 // and a screener-only column never reaches the older list, which has no parameter for it
 const screenerOnly=L.FILTER_COLUMNS.filter(c=>c.route==='screener')
  .map(c=>({column:c.key,operator:c.operators[0],value:value(c.key)}));
 assert.equal(L.unusualQuery(L.rulesToFilters(screenerOnly)),'',
  'a screener-only filter must never reach /api/derivatives/unusual, which has no parameter for it');
});
ok(()=>{ // the watch list is the one rule the screener cannot take, and it is never sent there
 const rule={column:'watchlist',operator:'in_watchlist',value:'indices'};
 assert.equal(L.screenerQuery([rule]),'','a watch-list rule would be a 400, so it is not sent');
 assert.equal(L.unusualQuery(L.rulesToFilters([rule])),'?watchlist=indices','but it still narrows the older list');
 // and the reader is told, rather than shown a filter that looks live
 const seen=L.filterStatuses([rule],{applied:[],available_filters:[]});
 assert.equal(seen[0].state,'unsupported');
 assert.ok(/no parameter for this filter/.test(seen[0].reason),seen[0].reason);
 assert.ok(/narrows the other lists/.test(seen[0].reason),'and that it still works elsewhere');
});
ok(()=>{ // the build-up values are the server's, which are LABELS here and not the §3.1 snake keys
 const values=/^BUILDUP_VALUES=\((.+)\)$/m.exec(DERIV_PY);
 assert.ok(values,'derivatives.py must declare BUILDUP_VALUES');
 const served=values[1].split(',').map(v=>v.trim().replace(/^'|'$/g,'')).filter(Boolean);
 assert.deepEqual([...L.BUILDUP_VALUES],served,'the screener filters on the label it puts on the row');
 // both windows, and only those two
 const windows=/^BUILDUP_WINDOWS=\((.+)\)$/m.exec(DERIV_PY);
 assert.ok(windows);
 assert.deepEqual([...L.BUILDUP_WINDOWS.map(w=>w.value)],
  windows[1].split(',').map(v=>v.trim().replace(/^'|'$/g,'')).filter(Boolean));
 assert.equal(L.BUILDUP_WINDOW_DEFAULT,L.BUILDUP_WINDOWS[0].value);
 // the window is sent ONLY with a build-up rule: §3.1's two labels are never mixed in one answer
 assert.ok(!L.screenerQuery([{column:'premium',operator:'gt',value:'10'}],{buildupWindow:'day'})
  .includes('buildup_window'),'no build-up rule, no window');
 assert.ok(L.screenerQuery([{column:'buildup',operator:'is',value:L.BUILDUP_VALUES[0]}],{buildupWindow:'day'})
  .includes('buildup_window=day'),'a build-up rule carries its window');
});
ok(()=>{ // the reading is a parameter the reader chooses, never one the browser invents
 assert.ok(SCREENER_KEYS.has('at'),'the screener takes a reading');
 assert.equal(L.screenerQuery([],{at:''}),'','no reading chosen sends none, so the server reads its newest');
 assert.equal(L.screenerQuery([],{at:'2026-09-18 11:30:00'}),'?at=2026-09-18%2011%3A30%3A00');
});
ok(()=>{ // the moneyness and index-or-stock values are the server's own
 for(const [name,mine] of [['MONEYNESS_VALUES',[...L.MONEYNESS_VALUES.map(m=>m.value)]],
  ['UNDERLYING_KINDS',[...L.MARKET_VALUES.map(m=>m.value)]]]){
  const m=new RegExp(`^${name}=\\((.+)\\)$`,'m').exec(DERIV_PY);
  assert.ok(m,`derivatives.py must declare ${name}`);
  assert.deepEqual([...mine],m[1].split(',').map(v=>v.trim().replace(/^'|'$/g,'')).filter(Boolean));
 }
});
ok(()=>{ // no rule may produce a query parameter outside that set
 const params=new Set(L.FILTER_COLUMNS.map(c=>c.param));
 const every=[{column:'underlying',operator:'is',value:'RELIANCE'},{column:'expiry',operator:'is',value:'2026-09-25'},
  {column:'optionType',operator:'is',value:'CE'},{column:'dte',operator:'lt',value:'7'},
  {column:'premium',operator:'gt',value:'10'},{column:'watchlist',operator:'in_watchlist',value:'indices'}];
 const query=L.unusualQuery(L.rulesToFilters(every));
 assert.ok(query.startsWith('?'));
 for(const part of query.slice(1).split('&'))assert.ok(params.has(part.split('=')[0]),`stray parameter: ${part}`);
});
ok(()=>{ // the operator list the owner asked for, and no operator a column cannot answer
 assert.deepEqual({...L.OPERATOR_LABELS},{is:'is',is_not:'is not',gt:'greater than',lt:'less than',
  in_watchlist:'is in watch list'});
 // the six the tab has always had, then the seven the screener block adds - in that order, so the popup opens
 // on the columns every list can answer
 assert.deepEqual([...L.FILTER_COLUMNS].map(c=>c.key),['underlying','expiry','optionType','dte','premium','watchlist',
  'volumeRatio','volumeToOi','oiChange15m','oiChangeDay','buildup','moneyness','market']);
 assert.deepEqual([...L.FILTER_COLUMNS].filter(c=>!c.route).map(c=>c.key),
  ['underlying','expiry','optionType','dte','premium','watchlist'],'the shared six answer on every route');
});
// the three exact translations: they must agree with the server's inclusive parameters
ok(()=>{ // "DTE less than 7" is the server's max_dte=6, because days-to-expiry is served as a whole number
 assert.equal(L.rulesToFilters([{column:'dte',operator:'lt',value:'7'}]).maxDte,6);
 assert.equal(L.rulesToFilters([{column:'dte',operator:'lt',value:'1'}]).maxDte,0);
});
ok(()=>{ // "Premium greater than 10" is the server's min_premium_cr=10.01, because premium is served to paise
 assert.equal(L.rulesToFilters([{column:'premium',operator:'gt',value:'10'}]).minPremiumCr,10.01);
 assert.equal(L.rulesToFilters([{column:'premium',operator:'gt',value:'2'}]).minPremiumCr,2.01);
});
ok(()=>{ // "Type is not CE" is exactly "Type is PE": the pilot serves two option types and no other
 assert.equal(L.rulesToFilters([{column:'optionType',operator:'is_not',value:'CE'}]).optionType,'PE');
 assert.equal(L.rulesToFilters([{column:'optionType',operator:'is_not',value:'PE'}]).optionType,'CE');
 assert.equal(L.rulesToFilters([{column:'optionType',operator:'is',value:'CE'}]).optionType,'CE');
});
ok(()=>assert.deepEqual({...L.rulesToFilters([])},{...L.DEFAULT_FILTERS},'no rule means the server defaults stand'));
ok(()=>assert.equal(L.unusualQuery(L.rulesToFilters([])),''));
ok(()=>assert.equal(L.unusualQuery(L.rulesToFilters([{column:'underlying',operator:'is',value:'M&M'},
 {column:'watchlist',operator:'in_watchlist',value:'indices'}])),'?underlying=M%26M&watchlist=indices'));
ok(()=>assert.equal(L.unusualQuery(L.rulesToFilters([{column:'watchlist',operator:'in_watchlist',value:'all'}])),'',
 'the "all underlyings" watch list is not a narrowing, so nothing is sent'));

// a rule set restored from localStorage is re-typed before it can reach a query string
ok(()=>{for(const junk of [null,undefined,'',{},42,[null],[{}],[{column:'nope',operator:'is',value:'X'}],
  [{column:'underlying',operator:'greater_than',value:'X'}],[{column:'underlying',operator:'is',value:''}],
  [{column:'underlying',operator:'is',value:"x' or 1=1"}],[{column:'expiry',operator:'is',value:'25/09/2026'}],
  [{column:'optionType',operator:'is',value:'XX'}],[{column:'dte',operator:'lt',value:'0'}],
  [{column:'dte',operator:'lt',value:'401'}],[{column:'premium',operator:'gt',value:'-4'}],
  [{column:'premium',operator:'gt',value:'1e9'}],[{column:'watchlist',operator:'in_watchlist',value:'ALL'}],
  [{column:'underlying',operator:'is_not',value:'RELIANCE'}]])
 assert.deepEqual([...L.sanitizeRules(junk)],[],JSON.stringify(junk));
});
ok(()=>assert.deepEqual([...L.sanitizeRules([{column:'underlying',operator:'is',value:' RELIANCE '},
 {column:'underlying',operator:'is',value:'INFY'}])].map(r=>({...r})),[{column:'underlying',operator:'is',value:'RELIANCE'}],
 'the pilot answers each parameter once, so a second rule on the same column is dropped'));
ok(()=>assert.equal(L.sanitizeRules([{column:'dte',operator:'lt',value:'7'},{column:'premium',operator:'gt',value:'10'}]).length,2));
ok(()=>assert.equal(L.unusualQuery(L.rulesToFilters(L.sanitizeRules([{column:'underlying',operator:'is',value:"x' or 1=1"}]))),
 '','a junk rule is dropped, never sent'));

// building a row: columns are used once, changing the column re-picks the operator and clears the value
ok(()=>{assert.equal(L.freeColumns([]).length,L.FILTER_COLUMNS.length);
 const used=[{column:'underlying',operator:'is',value:'INFY'}];
 assert.ok(!L.freeColumns(used).some(c=>c.key==='underlying'));
 assert.ok(L.freeColumns(used,'underlying').some(c=>c.key==='underlying'),'a row keeps its own column');
});
ok(()=>{const all=L.FILTER_COLUMNS.map(c=>({column:c.key,operator:c.operators[0],value:'x'}));
 assert.equal(L.newRule(all),null,'no free column means no new row');
 assert.deepEqual({...L.newRule([])},{column:'underlying',operator:'is',value:''})});
ok(()=>{const rule={column:'underlying',operator:'is',value:'INFY'};
 assert.deepEqual({...L.withColumn(rule,'premium')},{column:'premium',operator:'gt',value:''});
 assert.equal(L.withColumn(rule,'underlying'),rule,'the same column is left alone');
 assert.equal(L.withColumn(rule,'nonsense'),rule)});
// the header link and the applied-filters line
ok(()=>{assert.equal(L.customizeLabel(0),'Customize…');assert.equal(L.customizeLabel(1),'Customize (1 filter)…');
 assert.equal(L.customizeLabel(3),'Customize (3 filters)…')});
ok(()=>{assert.equal(L.ruleText({column:'underlying',operator:'is',value:'RELIANCE'}),'Symbol is RELIANCE');
 assert.equal(L.ruleText({column:'dte',operator:'lt',value:'7'}),'DTE less than 7 days');
 assert.equal(L.ruleText({column:'premium',operator:'gt',value:'10'},`${RUPEE}10 cr`),`Premium greater than ${RUPEE}10 cr`)});
ok(()=>{assert.equal(L.rulesText([]),'No filters — every contract over the liquidity floors');
 assert.equal(L.rulesText([{column:'underlying',operator:'is',value:'RELIANCE'},{column:'optionType',operator:'is',value:'CE'}]),
  'Symbol is RELIANCE · Type is CE')});

// --- sortable columns: a sort re-orders what the server returned, and a dash never sorts as a small number ------
ok(()=>{assert.deepEqual({...L.nextSort(null,'premium')},{key:'premium',dir:'desc'});
 assert.deepEqual({...L.nextSort({key:'premium',dir:'desc'},'premium')},{key:'premium',dir:'asc'});
 assert.equal(L.nextSort({key:'premium',dir:'asc'},'premium'),null,'a third click returns the server order');
 assert.deepEqual({...L.nextSort({key:'premium',dir:'asc'},'oi')},{key:'oi',dir:'desc'})});
ok(()=>{assert.ok(L.compareValues(2,1,'desc')<0);assert.ok(L.compareValues(1,2,'asc')<0);
 assert.equal(L.compareValues(1,1,'asc'),0);
 assert.ok(L.compareValues('A','B','asc')<0);assert.ok(L.compareValues('A','B','desc')>0)});
ok(()=>{for(const dir of ['asc','desc'])for(const gap of [null,undefined,'',NaN]){
 assert.ok(L.compareValues(gap,1,dir)>0,`a value that was not captured sinks (${dir})`);
 assert.ok(L.compareValues(1,gap,dir)<0);
}});
ok(()=>{const rows=[{v:3,i:0},{v:null,i:1},{v:10,i:2},{v:3,i:3}];
 assert.deepEqual([...L.sortRows(rows,r=>r.v,'desc')].map(r=>r.i),[2,0,3,1],'ties keep the server order, gaps sink');
 assert.deepEqual([...L.sortRows(rows,r=>r.v,'asc')].map(r=>r.i),[0,3,2,1]);
 assert.deepEqual([...L.sortRows([],r=>r.v,'asc')],[]);
 assert.deepEqual([...L.sortRows(null,r=>r,'asc')],[])});
ok(()=>{const rows=[{v:1},{v:2}];const sorted=L.sortRows(rows,r=>r.v,'desc');
 assert.notEqual(sorted,rows,'the rows are sorted as a copy — the server list is never mutated');
 assert.deepEqual([...rows].map(r=>r.v),[1,2])});

// --- the table's own wording ---------------------------------------------------------------------------------------
ok(()=>{assert.equal(L.shortDate('2026-09-25'),'25 Sep');assert.equal(L.shortDate('2026-09-25 14:45'),'25 Sep');
 for(const v of [null,undefined,'','nonsense'])assert.equal(L.shortDate(v),'',String(v))});
ok(()=>assert.equal(L.contractSummary({expiry:'2026-09-25',strike:25000,instrument_type:'CE'}),`25 Sep ${RUPEE}25,000 CE`));
ok(()=>assert.equal(L.contractSummary({expiry:'2026-09-25',strike:null,instrument_type:'FUT'}),'25 Sep FUT'));
ok(()=>{assert.equal(L.contractSummary({expiry:null,strike:25000,instrument_type:'PE'}),`${RUPEE}25,000 PE`);
 assert.equal(L.contractSummary({}),DASH);assert.equal(L.contractSummary(null),DASH)});
ok(()=>{assert.equal(L.optionTone('CE'),'call');assert.equal(L.optionTone('PE'),'put');
 assert.equal(L.optionTone('FUT'),'flat');assert.equal(L.optionTone(null),'flat')});
ok(()=>{const groups=[{underlying:'A',strikes:[{tradingsymbol:'A1'},{tradingsymbol:'A2'}]},
  {underlying:'B',strikes:[{tradingsymbol:'B1'}]},{underlying:'C',strikes:null}];
 assert.deepEqual([...L.flattenUnusual(groups)].map(r=>r.tradingsymbol),['A1','A2','B1'],'server order is kept');
 assert.deepEqual([...L.flattenUnusual(null)],[])});
ok(()=>{assert.equal(L.rowKey({instrument_token:123,tradingsymbol:'X'}),'123');
 assert.equal(L.rowKey({instrument_token:null,tradingsymbol:'NIFTY25SEP25000CE'}),'NIFTY25SEP25000CE');
 assert.equal(L.rowKey({underlying:'NIFTY',strike:25000,instrument_type:'CE'},'g'),'NIFTY-25000-CE-g');
 assert.ok(L.rowKey(null,'z').length)});

// --- the frame is really on the page: sections, widget headers, the filter popup -----------------------------------
const SRC=p=>fs.readFileSync(path.join(__dirname,'..','src','derivative',p),'utf8');
ok(()=>{ // the five sections, in the order the owner asked for
 const page=SRC('index.tsx');
 const order=[["Unusual activity","'Unusual activity'"],['Option chain',"'Option chain'"],
  ['OI by strike','<OiByStrikeSection'],['Index dashboard',"'Index dashboard'"],
  ['Futures OI build-up',"'Futures OI build-up'"]];
 let at=-1;
 for(const [title,needle] of order){
  const next=page.indexOf(needle);
  assert.ok(next>at,`section "${title}" is missing or out of order on the Derivative page`);
  at=next;
 }
 assert.ok(SRC('OiByStrikeSection.tsx').includes("title=\"OI by strike\"")||
  SRC('OiByStrikeSection.tsx').includes("title='OI by strike'"),'the OI-by-strike section needs its own title');
});
ok(()=>{ // every widget on the page wears the shared frame, so every one carries its own as-of and floors
 for(const file of ['UnusualWidget.tsx','ChainWidget.tsx','OiByStrikeSection.tsx','IndexWidget.tsx',
  'FuturesWidget.tsx','ChartTile.tsx'])assert.ok(/WidgetFrame/.test(SRC(file)),`${file} must use WidgetFrame`);
 const frame=SRC('frame.tsx');
 for(const piece of ['customizeLabel','asOfText','floorsText','missingText'])
  assert.ok(frame.includes(piece),`the widget frame must print ${piece}`);
 for(const icon of ['refresh-cw','maximize-2','x'])assert.ok(frame.includes(`'${icon}'`)||frame.includes(`"${icon}"`),
  `the widget header bar is missing the ${icon} control`);
});
ok(()=>{ // the filter builder's own controls
 const dialog=SRC('FilterDialog.tsx');
 for(const label of ['ADD FILTER','CLEAR ALL','APPLY'])assert.ok(dialog.includes(label),`the popup needs ${label}`);
 assert.ok(/Filter \$\{name\} data/.test(dialog),'the popup title names the widget being filtered');
 assert.ok(!/\.filter\(\s*\w+\s*=>\s*\w+\.(oi|premium_cr|volume|strike|days_to_expiry)\b/.test(dialog),
  'the popup must never filter served rows in the browser');
});
ok(()=>{ // no widget may drop a served row on its own: the list a table renders is the list the server sent
 for(const file of ['UnusualWidget.tsx','OiByStrikeSection.tsx','IndexWidget.tsx','FuturesWidget.tsx']){
  const block=/const items=useMemo\(([\s\S]*?)\n \},\[/.exec(SRC(file));
  assert.ok(block,`${file} must build its table rows in one place`);
  assert.ok(!/\.filter\(/.test(block[1]),`${file} drops served rows in the browser`);
 }
 assert.ok(!/\.filter\(/.test(SRC('ChainWidget.tsx')),'ChainWidget drops served rows in the browser');
});
ok(()=>{ // §5 again, over every file the rebuild added: nothing on this tab may read as a forecast
 const banned=/\b(will|expect|forecast|predict|likely|should rise|should fall|target price|bullish signal|bearish signal)\b/i;
 for(const file of ['frame.tsx','Table.tsx','FilterDialog.tsx','ChartTile.tsx','UnusualWidget.tsx','ChainWidget.tsx',
  'OiByStrikeSection.tsx','IndexWidget.tsx','FuturesWidget.tsx','index.tsx']){
  const strings=SRC(file).split('\n').filter(line=>!line.trim().startsWith('//')).join('\n').match(/'[^']*'|`[^`]*`/g)||[];
  for(const text of strings)assert.ok(!banned.test(text),`${file}: a user-facing string must not predict: ${text}`);
 }
});


// --- the owner's ΔOI strike grid ---------------------------------------------------------------------------------
// The block is ten small charts of open interest added or removed SINCE THE PREVIOUS CLOSE, at the ten strikes at
// the money. What is protected here: the ten slots and their order; the ATM offsets; the direction rule, including
// the 5% flat band and the fewer-than-two-marks case that must never become a chip; a gap that stays a gap; and the
// fact that nothing in this block, in any file, reads as a forecast.
const GRID=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','derivatives.py'),'utf8');
const pyConst=(name)=>{const m=new RegExp(`^${name}=(.+)$`,'m').exec(GRID);assert.ok(m,`derivatives.py must declare ${name}`);return m[1].trim();};
const pyText=(name)=>{const raw=pyConst(name);const m=/^'([^']*)'$/.exec(raw);assert.ok(m,`${name} must be one plain string`);return m[1];};
// A sentence the server builds over several lines: NAME=('...' '...' f'...') - the pieces joined, as Python joins
// them, so a wording check reads the sentence the reader actually gets rather than only its first line.
const pySentence=(name)=>{
 const m=new RegExp(`^${name}=\\(([\\s\\S]*?)\\)\\r?\\n(?=\\S|$)`,'m').exec(GRID)||
  new RegExp(`^${name}=(.+)$`,'m').exec(GRID);
 assert.ok(m,`derivatives.py must declare ${name}`);
 const parts=m[1].match(/f?'(?:[^'\\]|\\.)*'/g)||[];
 assert.ok(parts.length,`${name} must be built from plain string pieces`);
 return parts.map(t=>t.replace(/^f?'/,'').replace(/'$/,'').replace(/\\(.)/g,'$1')).join('');
};

ok(()=>{ // the constants of the rule are the server's, not a second set the browser invented
 assert.equal(L.GRID_DIRECTION_LOOKBACK,Number(pyConst('DIRECTION_LOOKBACK_MARKS')));
 assert.equal(L.GRID_FLAT_FRACTION,Number(pyConst('FLAT_FRACTION')));
 assert.equal(L.GRID_NOT_ENOUGH_MARKS,pyText('NOT_ENOUGH_MARKS'));
 assert.equal(L.GRID_DIRECTION_LOOKBACK,4,'four 15-min readings is the one hour the owner asked for');
 assert.equal(L.GRID_FLAT_FRACTION,0.05);
});
ok(()=>{ // ten slots, and the server builds them in the owner's order: calls at and above, puts at and below
 assert.equal(Number(pyConst('GRID_WIDTH')),4);
 assert.ok(/GRID_SLOTS=2\*\(GRID_WIDTH\+1\)/.test(GRID),'ten slots is two rows of ATM plus four');
 assert.ok(/expected=\[\('CE',n\) for n in range\(GRID_WIDTH\+1\)\]\+\[\('PE',0\)\]\+\[\('PE',-n\) for n in range\(1,GRID_WIDTH\+1\)\]/.test(GRID),
  'the ten slots must be built in the fixed order CE 0..+4 then PE 0..−4');
});

// --- the direction rule, in the owner's own wording ----------------------------------------------------------------
const at=(i)=>`2026-09-18 ${String(9+Math.floor(i/4)).padStart(2,'0')}:${String((i%4)*15).padStart(2,'0')}`;
// `prices` is optional: a series built without it carries no price at all, which is the "no baseline" case.
const series=(values,prices)=>values.map((v,i)=>({at:at(i),oi:v==null?null:1000000+v,delta_oi:v,
 price:prices?(prices[i]===undefined?null:prices[i]):null}));
ok(()=>{ // fewer than two marks carrying a ΔOI is "no baseline" — a state, never a fourth direction
 assert.equal(L.gridDirection([]),'no baseline');
 assert.equal(L.gridDirection(null),'no baseline');
 assert.equal(L.gridDirection(series([0])),'no baseline');
 assert.equal(L.gridDirection(series([null,null,5])),'no baseline','a null mark is not a mark');
 assert.equal(L.directionChip('no baseline'),'','"no baseline" never gets a chip');
});
ok(()=>{ // building / unwinding, in the owner's words
 assert.equal(L.gridDirection(series([0,100])),'building');
 assert.equal(L.gridDirection(series([0,-100])),'unwinding');
 assert.equal(L.directionChip('building'),'↑ BUILDING');
 assert.equal(L.directionChip('flat'),'→ FLAT');
 assert.equal(L.directionChip('unwinding'),'↓ UNWINDING');
 assert.deepEqual(Object.keys({...L.DIRECTION_CHIPS}),['building','flat','unwinding']);
});
ok(()=>{ // the flat band is 5% of that contract's OWN largest |ΔOI| today
 assert.equal(L.gridDirection(series([0,0])),'flat','no movement at all is flat, never a direction');
 assert.equal(L.gridDirection(series([1000,1049])),'flat','49 is inside 5% of 1049');
 assert.equal(L.gridDirection(series([1000,1060])),'building','60 is outside 5% of 1060');
 // a big mover early and a still hour since: still flat, because the band scales with the contract's own peak
 assert.equal(L.gridDirection(series([0,100000,200000,200000,201000,202000,203000])),'flat');
});
ok(()=>{ // the reference is the mark FOUR back, not the first point of the day
 assert.equal(L.gridDirection(series([0,1000,2000,3000,4000,4100])),'building');
 assert.equal(L.gridDirection(series([0,4000,4000,4000,4000,3000])),'unwinding',
  'four marks back, not the 0 the day opened at');
 // and with a shorter lookback the same series is read from a nearer mark
 assert.equal(L.gridDirection(series([0,1000,2000,3000,4000,4100]),1),'flat');
});
ok(()=>{ // a mark with no ΔOI is skipped by the rule, exactly as it is skipped by the line
 assert.equal(L.gridDirection(series([0,50000,null,null,300000])),'building');
});

// --- the line: gaps stay gaps, zero is always on the axis -----------------------------------------------------------
ok(()=>{
 const scaled=L.scaleDelta(series([0,100,null,300]),90,60);
 assert.ok(scaled,'four marks with three ΔOI values must scale');
 assert.equal(scaled.points.length,4);
 assert.equal(scaled.points[2],null,'a mark with no ΔOI is not placed on the line');
 assert.equal(scaled.real,3);
 assert.equal(scaled.lo,0);assert.equal(scaled.hi,300);
 const d=L.linePath(scaled);
 assert.equal((d.match(/M/g)||[]).length,2,'the gap breaks the path into two runs — it is never drawn through');
});
ok(()=>{ // zero is always in view, because every line starts at 0 at the day's first mark
 const up=L.scaleDelta(series([100,200,300]),90,60);
 assert.equal(up.lo,0,'a series that never went negative still shows the zero it started from');
 const down=L.scaleDelta(series([-100,-200]),90,60);
 assert.equal(down.hi,0);
 assert.equal(Math.round(up.zero),60,'zero sits on the floor of the box when every value is above it');
});
ok(()=>{ // nothing to draw is null, never an empty path pretending to be a flat line at zero
 assert.equal(L.scaleDelta(series([null,null]),90,60),null);
 assert.equal(L.scaleDelta([],90,60),null);
 assert.equal(L.scaleDelta(series([0,100]),0,60),null);
 assert.equal(L.linePath(null),'');
});

// --- the axes: K / L, and only times that were actually captured -----------------------------------------------------
ok(()=>{
 assert.equal(L.deltaUnits(240000),'+2.4L');
 assert.equal(L.deltaUnits(-80000),MINUS+'80K');
 assert.equal(L.deltaUnits(12000000),'+1.2Cr');
 assert.equal(L.deltaUnits(0),'0','a captured zero is a zero, not a dash');
 assert.equal(L.deltaUnits(940),'+940');
 for(const v of MISSING)assert.equal(L.deltaUnits(v),DASH,String(v));
});
ok(()=>{
 const scaled=L.scaleDelta(series([0,100,300]),90,60);
 const axis=L.deltaAxis(scaled);
 assert.deepEqual([...axis].map(t=>L.deltaUnits(t.v)),['+300','0'],'the peak and the zero it is measured from');
 assert.deepEqual([...L.deltaAxis(null)],[]);
});
ok(()=>{ // the hour marks, when the capture has them; never a time that was not captured
 const day=['09:30','09:45','10:00','10:15','10:30','10:45','11:00','11:15','11:30']
  .map((t,i)=>({at:`2026-09-18 ${t}`,oi:i,delta_oi:i}));
 const ticks=L.axisTimes(day,4);
 assert.deepEqual([...ticks].map(t=>t.label),['09:30','10:30','11:30']);
 for(const tick of ticks)assert.equal(tick.label,L.clock(day[tick.i].at),'a tick names its own captured mark');
 const two=L.axisTimes(day.slice(0,3),4);
 assert.deepEqual([...two].map(t=>t.label),['09:30','09:45','10:00'],'too few hour marks falls back to the marks it has');
 assert.deepEqual([...L.axisTimes([],3)],[]);
 assert.deepEqual([...L.axisTimes([{at:null,oi:null,delta_oi:null}],3)],[],'an unreadable mark is never labelled');
});

// --- one slot's wording ------------------------------------------------------------------------------------------
const slot=(extra)=>({slot:'CE+0',option_type:'CE',atm_offset:0,label:'ATM CE',row:'calls',present:true,
 tradingsymbol:'NIFTY25SEP25000CE',instrument_token:5101,strike:25000,previous_close_oi:1000000,
 points:series([0,120000],[100,120]),direction:'building',direction_detail:{},marks:2,marks_with_delta:2,
 latest_delta_oi:120000,peak_abs_delta_oi:120000,marks_with_price:2,latest_price:120,
 flow:{price_direction:'up',oi_direction:'building',what_label:'Call buying increasing',
  meaning:'Traders are buying upside',detail:{}},missing_text:null,...extra});
ok(()=>{
 assert.equal(L.gridSlotTitle(slot()),'25,000 CE','a chart is titled with its strike');
 assert.equal(L.gridSlotTitle(slot({strike:null,present:false})),'ATM CE','an unlisted strike falls back to its slot');
 assert.equal(L.gridSlotTitle(null),DASH);
 assert.equal(L.gridSlotDetail(slot()),'ATM CE · 2 readings');
 assert.equal(L.gridSlotDetail(slot({marks_with_delta:1})),'ATM CE · 1 reading');
});
ok(()=>{ // a strike that does not exist says so; a strike with no ΔOI says that instead of drawing zero
 assert.equal(L.gridSlotNote(slot()),'');
 assert.equal(L.gridSlotNote(slot({present:false,missing_text:'ATM+4 CE is not a listed strike in NIFTY.'})),
  'ATM+4 CE is not a listed strike in NIFTY.');
 assert.equal(L.gridSlotNote(slot({present:false,missing_text:null})),'This strike is not listed in this expiry.');
 assert.equal(L.gridSlotNote(slot({marks_with_delta:0})),L.GRID_NO_DELTA);
});
ok(()=>{ // what a screen reader hears describes the captured state and nothing beyond it
 const spoken=L.gridSlotLabel(slot(),'building');
 assert.ok(spoken.includes('25,000 CE')&&spoken.includes('+1.2L')&&spoken.includes('previous close'));
 assert.ok(spoken.includes('building'));
 assert.ok(L.gridSlotLabel(slot({marks_with_delta:1}),'no baseline').includes('no baseline'));
});
ok(()=>{ // the block's own footer states where the money is and how much of the session is behind the lines
 const body={atm_strike:25100,spot:25120,expiry:'2026-09-25',marks:['2026-09-18 09:30','2026-09-18 11:00']};
 const text=L.gridBasis(body);
 assert.ok(text.includes('25,100')&&text.includes('09:30')&&text.includes('11:00')&&text.includes('2 readings'));
 // one date format across the block: the same expiry, written the way the panels beside it write one
 assert.ok(text.includes('25 Sep 2026')&&!text.includes('2026-09-25'),'the expiry reads as a date, not a key');
 assert.ok(L.gridBasis({...body,expiry:'not a date'}).includes('not a date'),
  'and an expiry we cannot read is passed through untouched, never dropped');
 assert.equal(L.gridBasis(null),'');
 assert.ok(L.gridBasis({atm_strike:null,spot:null,marks:[]}).includes(DASH),'no spot is a dash, never a guess');
});
ok(()=>{ // never dressed up as more than it is
 assert.ok(L.gridSourceText('candles_15m').includes('backfill'));
 assert.equal(L.gridSourceText('none'),'');
 assert.equal(L.gridSourceText(null),'');
});

// --- the block is really on the page ---------------------------------------------------------------------------------
ok(()=>{
 const block=SRC('OiGridSection.tsx'),page=SRC('index.tsx');
 assert.ok(/WidgetFrame/.test(block),'both halves of the block wear the shared widget frame');
 assert.ok(/<Section\b/.test(block),'the block is one section of the tab, in the tab\'s own frame');
 assert.ok(block.includes('Unusual contracts'),'the left panel carries its own title');
 assert.ok(/api\/derivatives\/oi-grid\?underlying=/.test(block),'the grid reads the oi-grid route');
 assert.ok(/api\/derivatives\/unusual/.test(block),'the screener reads the unusual route the tab already serves');
 assert.ok(page.includes('<OiGridSection'),'the block is mounted on the Derivative page');
 for(const key of ['oi_grid_screener','oi_grid'])assert.ok(page.includes(`'${key}'`),
  `${key} must be a widget the reader can close and restore`);
 // clicking a chart re-points the tab's one chart target — the same mechanism a table row uses
 assert.ok(/onTarget\(\{underlying/.test(block),'a click must write a chart target, not open its own chart');
 assert.ok(/instrumentToken:slot\.instrument_token/.test(block),'the target is the contract that was clicked');
});
ok(()=>{ // the block gets its OWN height: at the shared ROW_H the puts band was cut off mid-chart
 const page=SRC('index.tsx');
 const number=(name)=>{const m=new RegExp(`\\b${name}=(\\d+)`).exec(page);
  assert.ok(m,`index.tsx must declare ${name}`);return Number(m[1]);};
 const row=number('ROW_H'),stack=number('STACK_TABLE_H');
 const gridRow=number('GRID_ROW_H'),gridStack=number('GRID_STACK_H');
 assert.ok(gridRow>row,'the ΔOI block is taller than a table row: its tiles carry a chart, a chip and two sentences');
 assert.ok(gridStack>stack,'and taller again when the page is stacked');
 assert.ok(/<OiGridSection[\s\S]*?height=\{expanded\?EXPANDED_H:stacked\?GRID_STACK_H:GRID_ROW_H\}/.test(page),
  'the block must be given GRID_ROW_H / GRID_STACK_H, never the shared ROW_H');
});
ok(()=>{ // the grid lays the served slots out — it never drops one
 const block=SRC('OiGridSection.tsx');
 const bands=/const bands=useMemo\(([\s\S]*?)\n \},\[/.exec(block);
 assert.ok(bands,'the two rows must be built in one place');
 assert.ok(!/\.filter\(/.test(bands[1]),'the grid drops served slots in the browser');
 assert.ok(/for\(const slot of rows\)/.test(bands[1]),'every served slot lands in exactly one row');
 assert.ok(!/\.slice\(/.test(bands[1]),'the rows are the server\'s own calls/puts flag, not a guess at an index');
});
ok(()=>{ // the three definitions are still the block's own words - moved behind one control, never dropped
 const block=SRC('OiGridSection.tsx');
 for(const piece of ['delta_oi_text','atm_text','direction_text','gridBasis'])
  assert.ok(block.includes(piece),`the block must carry ${piece}`);
 assert.ok(/since the previous close/.test(block),'the ΔOI definition is on the block');
 assert.ok(/drawn as a gap/.test(block),'the block must say that a missing 15-min reading is drawn as a gap');
 // and the server's own sentence is still preferred over ours wherever it sends one
 for(const piece of ['delta_oi_text||','atm_text||','price_text||','flow_text||'])
  assert.ok(block.includes(`gridBody?.${piece}`),`the served ${piece.replace('||','')} still wins over the fallback`);
});
ok(()=>{ // §5 over the new files too: describe, never predict — and never in the market's mood words
 const banned=/\b(will|expect|forecast|predict|likely|should rise|should fall|target price|bullish|bearish)\b/i;
 for(const file of ['OiGridSection.tsx','FuturesChartPanel.tsx']){
  const strings=SRC(file).split('\n').filter(line=>!line.trim().startsWith('//')).join('\n').match(/'[^']*'|`[^`]*`/g)||[];
  for(const text of strings)assert.ok(!banned.test(text),`${file}: a user-facing string must not predict: ${text}`);
 }
 // every sentence this block can put on screen, whatever the data does
 for(const text of [...Object.values(L.DIRECTION_CHIPS),L.GRID_NOT_ENOUGH_MARKS,L.GRID_NO_DELTA,L.NO_DIRECTION,
   L.gridSourceText('snapshots'),L.gridSourceText('candles_15m'),L.gridSourceText('read_api'),
   L.gridSlotNote(slot({present:false,missing_text:null})),L.gridSlotLabel(slot(),'building'),
   L.gridBasis({atm_strike:25100,spot:25120,expiry:'2026-09-25',marks:[]})])
  assert.ok(!banned.test(String(text)),`a grid sentence must not predict: ${text}`);
 // and the server's own wording for this block
 for(const name of ['DELTA_OI_TEXT','ATM_TEXT','DIRECTION_TEXT','NOT_ENOUGH_MARKS'])
  assert.ok(!banned.test(pyConst(name)),`derivatives.py ${name} must not predict`);
});


// --- CHANGE 1: the reader never sees the word "mark" -----------------------------------------------------------
// "mark" is our internal word for one 15-minute timestamp. It may stay in identifiers, JSON field names, SQL,
// comments and docstrings; it may NOT appear in a single sentence the tab puts on screen. This is the guard: if
// the word comes back into a user-facing string, on either side, this check fails.
const VISIBLE=(text)=>{
 // comments explain the rule and are exempt; everything inside quotes could reach the screen. A ${...} hole in
 // a template is CODE - an identifier may still be called `marks`, and that is not what the reader sees.
 const code=text.split('\n').filter(line=>!line.trim().startsWith('//')&&!line.trim().startsWith('#')).join('\n')
  .replace(/\$\{[^{}]*\}/g,'');
 return (code.match(/'[^'\n]*'|`[^`]*`|"[^"\n]*"/g)||[]).map(t=>t.slice(1,-1));
};
//: a quoted token that is an identifier, a column, a table, a route or a key - not a sentence
const IDENTIFIER=/^[\w./#?=&:\-{}$]*$/;
const MARK_WORD=/\bmarks?\b/i;
const marky=(text)=>VISIBLE(text).filter(t=>MARK_WORD.test(t)&&!IDENTIFIER.test(t));
ok(()=>{ // the browser side
 for(const file of ['logic.ts','types.ts','useDerivatives.ts','frame.tsx','Table.tsx','FilterDialog.tsx',
  'ChartTile.tsx','UnusualWidget.tsx','ChainWidget.tsx','OiByStrikeSection.tsx','IndexWidget.tsx',
  'FuturesWidget.tsx','OiGridSection.tsx','FuturesChartPanel.tsx','index.tsx']){
  const left=marky(SRC(file));
  assert.deepEqual(left,[],`${file}: "mark" is our word, not the reader's - say "15-min reading(s)": ${left.join(' | ')}`);
 }
});
ok(()=>{ // and the sentences the server hands the page
 for(const name of ['EMPTY_TEXT','FLOORS_TEXT','NOT_ENOUGH_MARKS','DELTA_OI_TEXT','ATM_TEXT','DIRECTION_TEXT',
  'PRICE_TEXT','FLOW_TEXT','BLOCK_TEXT','BLOCK_BOTH_BUILDING','FLOW_FLAT_WHAT','FLOW_FLAT_MEANING',
  'FLOW_NOT_ENOUGH'])
  assert.ok(!MARK_WORD.test(pySentence(name)),`derivatives.py ${name} still says "mark" to the reader`);
 // every label the table can put on a tile, on both sides
 for(const [what,meaning] of Object.values(L.FLOW_LABELS))
  for(const text of [what,meaning])assert.ok(!MARK_WORD.test(text),`a flow label still says "mark": ${text}`);
});
ok(()=>{ // and the owner's replacement is actually there, in his words
 assert.ok(/15-min reading/.test(L.EMPTY_TEXT));
 assert.ok(/15-min reading/.test(L.GRID_NOT_ENOUGH_MARKS));
 assert.equal(L.gridSlotDetail(slot()),'ATM CE · 2 readings');
 assert.ok(L.gridSlotLabel(slot(),'building').includes('captured reading'));
 assert.ok(/reading/i.test(L.gridSourceText('snapshots'))&&/reading/i.test(L.gridSourceText('candles_15m')));
});

// --- CHANGE 2: the price line -----------------------------------------------------------------------------------
// The second line on every tile is that contract's OWN last traded price at the same readings, on its own scale.
// What is protected: the price is nullable, a gap stays a gap on the price line exactly as on the ΔOI line, and
// nothing is ever interpolated or carried forward.
ok(()=>{ // the price series is scaled on its own, never forced through the ΔOI baseline
 const line=L.scaleGridPrice(series([0,100,200,300],[120,124,131,140]),90,60);
 assert.ok(line,'four priced readings must scale');
 assert.equal(line.lo,120,'the price scale is the price range, not zero');
 assert.equal(line.hi,140);
 assert.equal(line.points.length,4);
});
ok(()=>{ // a reading with no price is a gap on the price line, exactly as on the ΔOI line
 const points=series([0,100,200,300],[120,null,131,140]);
 const line=L.scaleGridPrice(points,90,60);
 assert.equal(line.points[1],null,'a reading with no price is not placed on the line');
 assert.equal((L.linePath(line).match(/M/g)||[]).length,2,'the gap breaks the price path in two');
 // and the ΔOI line is untouched by it: the two gaps are independent
 const delta=L.scaleDelta(points,90,60);
 assert.equal(delta.points.filter(Boolean).length,4);
 const both=L.scaleDelta(series([0,null,200,300],[120,null,131,140]),90,60);
 assert.equal(both.points[1],null,'a reading missing both stays a gap on both lines');
});
ok(()=>{ // nothing to draw is null, never a flat line pretending to be a price
 assert.equal(L.scaleGridPrice(series([0,100]),90,60),null,'no price at all is no line');
 assert.equal(L.scaleGridPrice(series([0,100],[120]),90,60),null,'one priced reading is a dot, not a line');
 assert.equal(L.scaleGridPrice([],90,60),null);
 assert.equal(L.scaleGridPrice(series([0,100],[120,130]),0,60),null);
});
ok(()=>{ // the tile really draws two lines, on two scales, and says which is which
 const block=SRC('OiGridSection.tsx');
 assert.ok(/scaleGridPrice\(slot\.points/.test(block),'the tile must scale the price on its own');
 assert.ok(/scaleDelta\(slot\.points/.test(block),'the tile still scales ΔOI on its own');
 assert.ok(/strokeDasharray=\{PRICE_DASH\}/.test(block),'the price line must be distinguishable without colour');
 assert.ok(/PRICE_DASH='3 2',PRICE_WIDTH=1,DELTA_WIDTH=1\.4/.test(block),
  'the price line is the thinner, dashed, SECONDARY series');
 assert.ok(/typeColor\(optionTone\(slot\.option_type\)\)/.test(block),'ΔOI keeps the row colour: calls green, puts red');
 assert.ok(block.includes('GRID_LEGEND_TEXT'),'the block must say which line is which');
 assert.ok(/own scale/.test(block)&&/own scales/.test(SRC('ChartTile.tsx')),
  'the two-scale wording must agree with ChartTile, which already draws price against OI');
 assert.ok(/strokeDasharray="2 3"/.test(block),'the zero line stays');
 assert.ok(/directionChip\(direction\)/.test(block),'the ΔOI direction chip stays');
});
ok(()=>{ // the price is in the spoken label too, with the same facts as the visuals
 const spoken=L.gridSlotLabel(slot(),'building');
 assert.ok(spoken.includes('Price ₹120.00'),'the screen reader hears the premium the tile drew');
 assert.ok(spoken.includes('Call buying increasing')&&spoken.includes('Traders are buying upside'));
 const nothing=L.gridSlotLabel(slot({points:series([0,120000])}),'building');
 assert.ok(nothing.includes('No price captured'),'no price is said, never implied');
});

// --- CHANGE 2: price and ΔOI read together ------------------------------------------------------------------
// One table for calls and one for puts. The mechanics are identical - both are read on the option's OWN premium.
// Every row of both tables is checked here, on the SAME points a tile would draw.
const up=[100,104,109,115,122,130],down=[130,122,115,109,104,100];
const build=[0,20000,40000,60000,80000,100000],unwind=[0,-20000,-40000,-60000,-80000,-100000];
const flow=(kind,oi,prices)=>L.gridFlow(kind,series(oi,prices));
ok(()=>{ // the four call rows, in the owner's own words
 assert.deepEqual([flow('CE',build,down).what_label,flow('CE',build,down).meaning],
  ['Call writing increasing','Sellers are building resistance']);
 assert.deepEqual([flow('CE',unwind,up).what_label,flow('CE',unwind,up).meaning],
  ['Call short covering','Call sellers are exiting']);
 assert.deepEqual([flow('CE',build,up).what_label,flow('CE',build,up).meaning],
  ['Call buying increasing','Traders are buying upside']);
 assert.deepEqual([flow('CE',unwind,down).what_label,flow('CE',unwind,down).meaning],
  ['Call buyers exiting','Call buyers are closing out']);
});
ok(()=>{ // the four put rows
 assert.deepEqual([flow('PE',build,down).what_label,flow('PE',build,down).meaning],
  ['Put writing increasing','Sellers are building support']);
 assert.deepEqual([flow('PE',unwind,up).what_label,flow('PE',unwind,up).meaning],
  ['Put short covering','Put sellers are exiting']);
 assert.deepEqual([flow('PE',build,up).what_label,flow('PE',build,up).meaning],
  ['Put buying increasing','Traders are buying downside protection']);
 assert.deepEqual([flow('PE',unwind,down).what_label,flow('PE',unwind,down).meaning],
  ['Put buyers exiting','Put buyers are closing out']);
});
ok(()=>{ // the same eight rows are the SERVER's, not a second table the browser invented
 const pyTable=(file)=>{
  const text=fs.readFileSync(path.join(__dirname,'..',file),'utf8');
  const out={};
  for(const m of text.matchAll(/'((?:CE|PE)\|(?:up|down|flat)\|(?:building|unwinding|flat))':\('([^']*)','([^']*)'\)/g))
   out[m[1]]=[m[2],m[3]];
  return out;
 };
 const here=JSON.parse(JSON.stringify(L.FLOW_LABELS));
 assert.equal(Object.keys(here).length,18,'nine price x OI combinations for calls and nine for puts');
 // complete by construction: there is no combination left to fall through into another row
 for(const kind of ['CE','PE'])for(const way of ['up','down','flat'])for(const oi of ['building','unwinding','flat'])
  assert.ok(here[`${kind}|${way}|${oi}`],`${kind}|${way}|${oi} has no row of its own`);
 for(const file of ['server/kanida_pilot/derivatives.py','../market_data/derivatives/read_api.py']){
  const table=pyTable(file);
  assert.deepEqual(table,here,`${file} and logic.ts must carry the SAME label table`);
 }
 // and the three sentences that stand in for a row
 assert.equal(L.FLOW_FLAT_WHAT,pyText('FLOW_FLAT_WHAT'));
 assert.equal(L.FLOW_FLAT_MEANING,pyText('FLOW_FLAT_MEANING'));
 assert.equal(L.FLOW_NOT_ENOUGH,pyText('FLOW_NOT_ENOUGH'));
 assert.equal(L.GRID_BLOCK_BOTH_BUILDING,pyText('BLOCK_BOTH_BUILDING'));
});
ok(()=>{ // read_api.py is the OTHER reader of the same store: its numbers and its wording are the same numbers
 const READ=fs.readFileSync(path.join(__dirname,'..','..','market_data','derivatives','read_api.py'),'utf8');
 const value=(name)=>{const m=new RegExp(`^${name} = (.+)$`,'m').exec(READ);
  assert.ok(m,`read_api.py must declare ${name}`);return m[1].trim();};
 const text=(name)=>{const m=/^'(.*)'$/.exec(value(name));assert.ok(m,`${name} must be one plain string`);return m[1];};
 assert.equal(Number(value('DIRECTION_LOOKBACK_MARKS')),L.GRID_DIRECTION_LOOKBACK);
 assert.equal(Number(value('FLAT_FRACTION')),L.GRID_FLAT_FRACTION);
 assert.equal(Number(value('PRICE_FLAT_FRACTION')),L.GRID_PRICE_FLAT_FRACTION);
 assert.equal(text('FLOW_FLAT_WHAT'),L.FLOW_FLAT_WHAT);
 assert.equal(text('FLOW_FLAT_MEANING'),L.FLOW_FLAT_MEANING);
 assert.equal(text('FLOW_NOT_ENOUGH'),L.FLOW_NOT_ENOUGH);
});
ok(()=>{ // a delegate cannot smuggle a sentence this table does not hold: the pilot re-derives one it cannot place
 assert.ok(/FLOW_WORDINGS=frozenset/.test(GRID),'derivatives.py must name the sentences a tile may show');
 assert.ok(/flow\.get\('what_label'\) not in FLOW_WORDINGS/.test(GRID),
  'an unrecognised label must be recomputed, never served');
});
// --- the sentence under a tile may never contradict the chip above it --------------------------------------------
// This is the guard for the bug the owner caught on screen: a tile read "↓ UNWINDING" over "Positioning is
// unchanged" while 3.7 million contracts closed in that hour. The rule is stated once, as a property over every
// row of the table rather than as a list of cases, so a future edit to any wording cannot slip past it:
//   * OI has a direction -> the sentence says something happened to open interest, and never that it did not;
//   * OI is flat         -> the sentence says open interest barely moved, and never that positions changed.
//: a sentence that claims open interest changed hands
const OI_MOVED=/\b(writing|covering|buying|exiting|added|closing out)\b/i;
//: a sentence that claims open interest did NOT change
const OI_STILL=/\b(open interest barely moved|positioning is unchanged)\b/i;
ok(()=>{
 for(const [key,pair] of Object.entries(L.FLOW_LABELS)){
  const [kind,priceWay,oiWay]=key.split('|');
  const [what,meaning]=pair,sentence=`${what}. ${meaning}`;
  if(oiWay==='flat'){
   assert.ok(OI_STILL.test(sentence),`${key}: OI is flat, so the sentence must say open interest barely moved`);
   assert.ok(!OI_MOVED.test(what),`${key}: OI is flat, so the sentence must not claim positions were added or closed: ${what}`);
  }else{
   assert.ok(OI_MOVED.test(what),`${key}: the chip says ${oiWay}, so the sentence must say what happened to open interest: ${what}`);
   assert.ok(!OI_STILL.test(sentence),`${key}: the chip says ${oiWay}, but the sentence says open interest did not move: ${sentence}`);
   assert.notDeepEqual([...pair],[...L.FLOW_LABELS[`${kind}|flat|flat`]],
    `${key}: a tile whose OI moved must not reuse the both-flat wording`);
  }
 }
});
ok(()=>{ // and the same property on the RUNTIME path, from real point series, not just the table
 for(const [key,pair] of Object.entries(L.FLOW_LABELS)){
  const [kind,priceWay,oiWay]=key.split('|');
  const points=series({building:build,unwinding:unwind,flat:[0,0,0,0,0,0]}[oiWay],
   {up:up,down:down,flat:[120,120,120,120,120,120]}[priceWay]);
  // the tile's own chip and its own reading, from the very same points
  assert.equal(L.gridDirection(points),oiWay,`${key}: the chip must read ${oiWay}`);
  assert.equal(L.gridPriceDirection(points),priceWay,`${key}: the price must read ${priceWay}`);
  const got=L.gridFlow(kind,points);
  assert.deepEqual([got.what_label,got.meaning],[...pair],`${key}: the sentence must be its own row`);
  const chip=L.directionChip(oiWay).replace(/^[^ ]+ /,'').toLowerCase();
  if(oiWay!=='flat')assert.ok(!OI_STILL.test(`${got.what_label}. ${got.meaning}`),
   `${key}: the tile would print "${got.what_label}" under the chip "${chip}"`);
 }
});
ok(()=>{ // the five rows that involve a flat axis, spelled out, in the owner's words
 const flat=[120,120,120,120,120,120],still=[0,0,0,0,0,0];
 for(const kind of ['CE','PE']){
  // a flat premium over an hour in which open interest plainly moved
  assert.deepEqual([flow(kind,build,flat).what_label,flow(kind,build,flat).meaning],
   ['New positions added','Premium barely moved']);
  assert.deepEqual([flow(kind,unwind,flat).what_label,flow(kind,unwind,flat).meaning],
   ['Positions closing out','Premium barely moved']);
  // and the other way round: the premium moved, the book did not
  assert.deepEqual([flow(kind,still,up).what_label,flow(kind,still,up).meaning],
   ['Premium rose','Open interest barely moved']);
  assert.deepEqual([flow(kind,still,down).what_label,flow(kind,still,down).meaning],
   ['Premium fell','Open interest barely moved']);
  // only when BOTH are still is nothing happening
  assert.deepEqual([flow(kind,still,flat).what_label,flow(kind,still,flat).meaning],
   [L.FLOW_FLAT_WHAT,L.FLOW_FLAT_MEANING]);
 }
 // the exact bug the owner saw: unwinding OI beside a barely-moved premium is NOT "positioning is unchanged"
  // the premium jumped early and has barely moved since: flat against its OWN largest move, exactly as the
 // owner's tile read it (price_change 0.05 on a contract that had already travelled ₹20)
 const barely=[120,140,140,140,140,140.05];
 const caught=flow('PE',unwind,barely);
 assert.equal(caught.oi_direction,'unwinding');
 assert.equal(caught.price_direction,'flat');
 assert.equal(caught.what_label,'Positions closing out');
 assert.notEqual(caught.what_label,L.FLOW_FLAT_WHAT);
});
ok(()=>{ // no baseline on EITHER side is "not enough readings yet" - never a guess, never a row
 for(const kind of ['CE','PE']){
  assert.equal(L.gridFlow(kind,series(build)).what_label,L.FLOW_NOT_ENOUGH,'no price at all');
  assert.equal(L.gridFlow(kind,series(build)).meaning,'','and no meaning either');
  assert.equal(L.gridFlow(kind,series(build,[120])).what_label,L.FLOW_NOT_ENOUGH,'one priced reading');
  assert.equal(L.gridFlow(kind,series([null,null,null,null,null,null],up)).what_label,L.FLOW_NOT_ENOUGH,
   'no ΔOI at all');
  assert.equal(L.gridFlow(kind,[]).what_label,L.FLOW_NOT_ENOUGH);
  assert.equal(L.gridFlow(kind,null).what_label,L.FLOW_NOT_ENOUGH);
 }
 // an option type we do not know is not quietly read as a call
 assert.equal(L.gridFlow('FUT',series(build,up)).what_label,L.FLOW_NOT_ENOUGH);
 assert.equal(L.gridFlow('',series(build,up)).what_label,L.FLOW_NOT_ENOUGH);
});
ok(()=>{ // the price flat band is 5% of that contract's OWN largest move from its first priced reading
 assert.equal(L.GRID_PRICE_FLAT_FRACTION,Number(pyConst('PRICE_FLAT_FRACTION')));
 assert.equal(L.GRID_PRICE_FLAT_FRACTION,0.05);
 assert.equal(L.GRID_PRICE_FLAT_FRACTION,L.GRID_FLAT_FRACTION,'the same 5% shape on both readings');
 // first 100, peak 200 -> the band is 5 rupees; +4 over the hour is inside it, +6 is outside
 assert.equal(L.gridPriceDirection(series(build,[100,200,200,200,200,204])),'flat');
 assert.equal(L.gridPriceDirection(series(build,[100,200,200,200,200,206])),'up');
 assert.equal(L.gridPriceDirection(series(build,[100,200,200,200,200,194])),'down');
 assert.equal(L.gridPriceDirection(series(build,[120,120])),'flat','no move at all is flat, never a direction');
 // and it reads four back, not the first reading of the day
 assert.equal(L.gridPriceDirection(series(build,[100,180,180,180,180,182])),'flat');
 assert.equal(L.gridPriceDirection(series(build,[100,180,180,180,180,182]),1),'flat');
 // a reading with no price is skipped by the rule, exactly as it is skipped by the line
 assert.equal(L.gridPriceDirection(series(build,[100,110,null,null,160])),'up');
 assert.equal(L.gridPriceDirection(series(build,[100])),'no baseline');
 assert.equal(L.gridPriceDirection([]),'no baseline');
 assert.equal(L.gridPriceDirection(null),'no baseline');
});
ok(()=>{ // the reading is derived from the tile's own points - it cannot disagree with its own chart
 const block=SRC('OiGridSection.tsx');
 assert.ok(/const flow=gridFlow\(slot\.option_type,slot\.points\)/.test(block),
  'the tile derives its own reading from the very points it draws');
 assert.ok(/\{flow\.what_label\}/.test(block)&&/\{flow\.meaning\}/.test(block),
  'both sentences are on the tile');
 assert.ok(!/buildupColor\([^)]*flow/.test(block),'the reading is plain text, never a coloured chip');
});

// --- CHANGE 2: the one line under the whole block ---------------------------------------------------------------
const gslot=(row,oi)=>({slot:row,row,option_type:row==='puts'?'PE':'CE',points:series(oi)});
ok(()=>{ // both sides building and of a similar size: the owner's one line, and nothing else ever
 assert.equal(L.GRID_BLOCK_BALANCE_RATIO,Number(pyConst('BLOCK_BALANCE_RATIO')));
 assert.equal(L.GRID_BLOCK_BALANCE_RATIO,1.33,'a third larger is where "similar" stops');
 const calls=[gslot('calls',build),gslot('calls',build)];
 const puts=[gslot('puts',build),gslot('puts',build)];
 assert.equal(L.gridBlockRead([...calls,...puts]),L.GRID_BLOCK_BOTH_BUILDING);
 // 1.33x is still similar; a hair above it is not
 const big=(f)=>({row:'puts',option_type:'PE',points:series(build.map(v=>v*f))});
 assert.equal(L.gridBlockRead([gslot('calls',build),big(1.3)]),L.GRID_BLOCK_BOTH_BUILDING);
 assert.equal(L.gridBlockRead([gslot('calls',build),big(1.4)]),'','one side a third larger says nothing at all');
 assert.equal(L.gridBlockRead([gslot('calls',build),big(0.5)]),'');
});
ok(()=>{ // anything that is not "both building, similarly" prints NOTHING, never a forced summary
 assert.equal(L.gridBlockRead([gslot('calls',unwind),gslot('puts',unwind)]),'','both unwinding');
 assert.equal(L.gridBlockRead([gslot('calls',build),gslot('puts',unwind)]),'','one each way');
 assert.equal(L.gridBlockRead([gslot('calls',build),gslot('calls',build)]),'','calls only is not both sides');
 assert.equal(L.gridBlockRead([gslot('puts',build)]),'','puts only is not both sides');
 assert.equal(L.gridBlockRead([]),'');
 assert.equal(L.gridBlockRead(null),'');
 // a slot with no ΔOI baseline contributes nothing rather than a zero
 assert.equal(L.gridBlockRead([gslot('calls',build),gslot('puts',[0])]),'');
});
ok(()=>{ // it aggregates the ten tiles on screen - it never asks the pilot for anything of its own
 const block=SRC('OiGridSection.tsx');
 assert.ok(/const blockRead=gridBlockRead\(rows\)/.test(block),'the block line is read off the served slots');
 // The two default-symbol fallbacks moved UP to the page when the tab took over resolving its one symbol, so
 // the block now makes exactly two reads of its own: the grid and the screener beside it.
 assert.ok((block.match(/useDerivativeRead</g)||[]).length===2,
  'the grid and the screener - the block line adds no third read');
 assert.ok(!/resolveBlockSymbol\(/.test(block),
  'the block must NOT resolve its own symbol: the tab resolves one and hands it down');
 assert.ok(/note=\{blockRead\|\|undefined\}/.test(block),'nothing is printed when there is nothing to say');
});

// --- §5 over everything CHANGE 2 added -------------------------------------------------------------------------
// The owner dropped the conventional bullish/bearish reading: these labels say who appears to be doing what, and
// nothing about what happens next. There are no allowed exceptions.
ok(()=>{
 const banned=/\b(will|expect|forecast|predict|likely|should rise|should fall|target price|target|support level|resistance level|bullish|bearish|buy signal|sell signal)\b/i;
 const sentences=[...Object.values(L.FLOW_LABELS).flat(),L.FLOW_FLAT_WHAT,L.FLOW_FLAT_MEANING,L.FLOW_NOT_ENOUGH,
  L.GRID_BLOCK_BOTH_BUILDING,L.GRID_LEGEND_TEXT,L.gridPriceSpoken(slot()),L.gridSlotLabel(slot(),'building')];
 for(const text of sentences)assert.ok(!banned.test(String(text)),`a new sentence must not predict: ${text}`);
 for(const name of ['PRICE_TEXT','FLOW_TEXT','BLOCK_TEXT','BLOCK_BOTH_BUILDING','FLOW_FLAT_WHAT',
  'FLOW_FLAT_MEANING','FLOW_NOT_ENOUGH'])
  assert.ok(!banned.test(pySentence(name)),`derivatives.py ${name} must not predict`);
 // The owner ruled the two words out: neither may reach the screen from ANY string on the tab, on either side.
 // (A comment may still state the rule - that is what the comments in logic.ts and OiGridSection.tsx do.)
 const moody=/bullish|bearish/i;
 for(const file of ['logic.ts','types.ts','OiGridSection.tsx','ChartTile.tsx','frame.tsx','index.tsx',
  'UnusualWidget.tsx','ChainWidget.tsx','OiByStrikeSection.tsx','IndexWidget.tsx','FuturesWidget.tsx',
  'FuturesChartPanel.tsx']){
  const said=VISIBLE(SRC(file)).filter(t=>moody.test(t));
  assert.deepEqual(said,[],`${file}: the tab does not label a reading bullish or bearish: ${said.join(' | ')}`);
 }
 for(const file of ['server/kanida_pilot/derivatives.py','../market_data/derivatives/read_api.py']){
  const said=VISIBLE(fs.readFileSync(path.join(__dirname,'..',file),'utf8')).filter(t=>moody.test(t));
  assert.deepEqual(said,[],`${file}: the tab does not label a reading bullish or bearish: ${said.join(' | ')}`);
 }
});
ok(()=>{ // the block states the price line, the reading, the tolerance and the buyer/seller fact on itself
 const block=SRC('OiGridSection.tsx');
 for(const piece of ['price_text','flow_text','block_text','GRID_LEGEND_TEXT'])
  assert.ok(block.includes(piece),`the block must print ${piece}`);
 assert.ok(/buyer and a seller/.test(pySentence('FLOW_TEXT')),'the honesty sentence names both sides of a contract');
 assert.ok(/do not say what happens next/.test(pySentence('FLOW_TEXT')),'and it says it is not a forecast');
 assert.ok(/own scale/.test(pySentence('PRICE_TEXT')),'the second line is stated to be on its own scale');
 assert.ok(/BLOCK_BALANCE_RATIO/.test(pySentence('BLOCK_TEXT')),'the block states the tolerance it used');
 assert.ok(/both building/.test(pySentence('BLOCK_TEXT')));
});


// =================================================================================================================
// CHANGE 3: the futures chart in the ΔOI block
// The owner's block is now three panels left to right — screener · futures chart · 2 x 5 grid — driven by ONE
// symbol. What is protected below: the layout and the three panels' relative heights; 15-min being what opens
// and daily being one click away; a dead interval being DISABLED with its reason rather than hidden; the block
// resolving its symbol once and handing it to the chart AND all ten tiles; a candle that was not stored whole
// being left blank rather than interpolated or carried forward; every empty and short state having its own
// words; and no string this panel can put on screen saying anything about what comes next.
// =================================================================================================================
const candle=(o,h,l,c,at)=>({at:at||null,open:o,high:h,low:l,close:c,volume:null,oi:null});
// A file with its // comments stripped. A negative check about what the CODE does must not be tripped by a
// comment that states the very rule being checked.
const CODE=(file)=>SRC(file).split('\n').filter(line=>!line.trim().startsWith('//')).join('\n');

// --- the control at the top: 15-min opens, daily is one click away ------------------------------------------------
ok(()=>{ // two choices, in the owner's order, and the default is the one the endpoint itself defaults to
 assert.deepEqual([...L.FUTURES_INTERVALS.map(c=>c.key)],['15m','1d']);
 assert.equal(L.FUTURES_DEFAULT_INTERVAL,'15m','15-min is what the reader arrives on');
 assert.equal(L.FUTURES_INTERVALS[0].key,L.FUTURES_DEFAULT_INTERVAL,'the default is the first choice offered');
 assert.deepEqual([...L.FUTURES_INTERVALS.map(c=>c.label)],['15-min candles','Daily candles']);
});
ok(()=>{ // an interval list that has not arrived is NOT a claim that anything is dead
 for(const body of [null,undefined,{},{intervals:null},{intervals:'15m'}]){
  const choices=L.futuresIntervalChoices(body);
  assert.deepEqual([...choices.map(c=>c.available)],[true,true],JSON.stringify(body));
  assert.deepEqual([...choices.map(c=>c.reason)],['','']);
 }
 assert.equal(L.futuresIntervalNote(L.futuresIntervalChoices(null)),'','nothing to say when both are live');
});
ok(()=>{ // an interval the server does not list is offered DISABLED, with the reason said in words
 const choices=L.futuresIntervalChoices({intervals:['15m']});
 assert.equal(choices.length,2,'a dead interval keeps its place in the control — it is never hidden');
 assert.deepEqual([...choices.map(c=>c.key)],['15m','1d'],'and it keeps its order');
 assert.equal(choices[0].available,true);assert.equal(choices[0].reason,'');
 assert.equal(choices[1].available,false);
 assert.equal(choices[1].reason,'No daily candles stored for this contract yet.');
 assert.equal(L.futuresIntervalNote(choices),'No daily candles stored for this contract yet.');
});
ok(()=>{ // the other way round, and the object form of the list
 const choices=L.futuresIntervalChoices({intervals:[{interval:'1d'}]});
 assert.equal(choices[0].available,false);
 assert.equal(choices[0].reason,'No 15-min candles stored for this contract yet.');
 assert.equal(choices[1].available,true);
 const none=L.futuresIntervalChoices({intervals:[]});
 assert.deepEqual([...none.map(c=>c.available)],[false,false],'served-and-empty IS a claim, and both are disabled');
 assert.equal(L.futuresIntervalNote(none).split('.').filter(Boolean).length,2,'both reasons are said');
});

// --- the control reads the server's OWN availability flag, not mere membership ------------------------------------
// The endpoint always sends BOTH intervals, each carrying its own `available` and its own `note`. It does that
// deliberately: the note is the honest reason a control is dead, and dropping the entry would throw the reason
// away. Keying off membership instead of the flag offers a dead control as live — which is exactly the state
// the reader opens on today, because daily history is fetched contract by contract and has not been fetched.
const iv=(key,available,note)=>({interval:key,available,note:note===undefined?null:note});
ok(()=>{ // an interval the server SENT but flagged unavailable must be disabled, with the server's own reason
 const dead='No daily candles are stored for NIFTY26SEPFUT yet — daily history is fetched contract by contract, and this one has not been fetched.';
 const choices=L.futuresIntervalChoices({intervals:[iv('15m',true,'364 bars over 14 sessions.'),iv('1d',false,dead)]});
 assert.equal(choices.length,2,'both choices stay in the control');
 assert.equal(choices[0].available,true,'15-min is live');
 assert.equal(choices[0].reason,'','and a live choice carries no reason');
 assert.equal(choices[1].available,false,'daily is DEAD and must not be offered as live');
 assert.equal(choices[1].reason,dead,'and the server\'s own sentence is the reason the reader is given');
 assert.equal(L.futuresIntervalNote(choices),dead,'which is what goes under the control');
});
ok(()=>{ // membership alone is NOT availability - the flag is
 const both=L.futuresIntervalChoices({intervals:[iv('15m',false),iv('1d',false)]});
 assert.deepEqual([...both.map(c=>c.available)],[false,false],'listed-but-unavailable is still unavailable');
 for(const choice of both)assert.ok(choice.reason,'and each dead choice still says why');
 const live=L.futuresIntervalChoices({intervals:[iv('15m',true),iv('1d',true)]});
 assert.deepEqual([...live.map(c=>c.available)],[true,true]);
 assert.equal(L.futuresIntervalNote(live),'','nothing to say when both are live');
});
ok(()=>{ // a flagged-dead interval with no sentence of its own still gets ours - never a silent disable
 const choices=L.futuresIntervalChoices({intervals:[iv('15m',true),iv('1d',false,'   ')]});
 assert.equal(choices[1].available,false);
 assert.equal(choices[1].reason,'No daily candles stored for this contract yet.');
 const nulled=L.futuresIntervalChoices({intervals:[iv('15m',false,null),iv('1d',true)]});
 assert.equal(nulled[0].reason,'No 15-min candles stored for this contract yet.');
});
ok(()=>{ // the shapes that are not the flag: a bare string is a listing and means live; an absent entry is dead
 const strings=L.futuresIntervalChoices({intervals:['15m','1d']});
 assert.deepEqual([...strings.map(c=>c.available)],[true,true],'a plain listing still means live');
 const partial=L.futuresIntervalChoices({intervals:[iv('15m',true)]});
 assert.equal(partial[1].available,false,'an interval the server did not send at all is not offered as live');
 assert.ok(partial[1].reason,'and it says why');
 // a missing flag on a sent entry is a listing, not a claim of deadness
 assert.equal(L.futuresIntervalChoices({intervals:[{interval:'1d'}]})[1].available,true);
});
ok(()=>{ // TODAY'S payload, exactly: 15-min full, daily empty and staying empty
 const today={intervals:[iv('15m',true,'364 bars over 14 sessions, 31 Aug to 18 Sep.'),
  iv('1d',false,'No daily candles are stored for NIFTY26SEPFUT yet — daily history is fetched contract by contract, and this one has not been fetched.')]};
 const choices=L.futuresIntervalChoices(today);
 assert.equal(choices.filter(c=>!c.available).length,1,'exactly one dead control');
 assert.equal(choices.find(c=>!c.available).key,'1d','and it is the daily one');
 assert.equal(choices.find(c=>c.available).key,L.FUTURES_DEFAULT_INTERVAL,
  'the reader still arrives on the interval that HAS data');
 assert.ok(/has not been fetched/.test(L.futuresIntervalNote(choices)),
  'and the reason on screen is the real one, so an empty control reads as explained rather than broken');
});

// --- a gap slot is not a stored bar ---------------------------------------------------------------------------------
// A gap now arrives IN `candles` with its stamp set, every price null and `gap: true`. It holds its place on the
// axis and is drawn as nothing. It must never be counted as a bar the store actually has.
const gapSlot=(at)=>({at,open:null,high:null,low:null,close:null,volume:null,oi:null,gap:true});
ok(()=>{
 const candles=[candle(100,110,95,105,'2026-09-18 09:30'),gapSlot('2026-09-18 09:45'),
  candle(105,120,104,118,'2026-09-18 10:00')];
 assert.equal(L.futuresBars({bars:2,candles}),2,'the server\'s own count of stored bars is the count');
 assert.equal(L.futuresBars({candles}),2,'and without it, a gap slot is not counted as a bar');
 assert.equal(L.futuresBars({candles:[]}),0);
 assert.equal(L.futuresBars(null),0);
 assert.equal(L.futuresBars({bars:0,candles}),0,'a served zero is a served number, not a missing one');
 // the count moved from the subtitle to the one line under the chart. Same rule as before: it says the
 // STORED count, never the length of a list that carries its own gaps.
 const line=L.candleDrawnText(L.scaleCandles(candles,120,60).drawn,L.futuresBars({bars:2,candles}));
 assert.equal(line,'2 stored candles drawn');
 assert.ok(!/3 /.test(line),'a gap slot must never be over-reported as a stored bar');
 assert.equal(L.candleDrawnText(2,L.futuresBars({candles})),'2 stored candles drawn',
  'with or without the served count');
 assert.equal(L.candleDrawnText(1,1),'1 stored candle drawn');
 assert.equal(L.candleDrawnText(26,364),'26 of 364 stored candles drawn','and it says how much it left out');
 for(const v of [null,undefined,0,-3,NaN])assert.equal(L.candleDrawnText(v,10),'',String(v));
 // and the spoken label, which already counted what was drawn, still agrees with it
 assert.ok(L.futuresChartSpoken({contract:{tradingsymbol:'X'},candles},'15m','X').includes('2 candles stored'));
 assert.equal(L.scaleCandles(candles,120,60).drawn,2,'the chart draws two and keeps three slots');
 assert.equal(L.scaleCandles(candles,120,60).candles[1],null,'the gap holds its place and is drawn as nothing');
});

// --- the panel names the CONTRACT it drew, and its expiry ---------------------------------------------------------
ok(()=>{ // the HEADER title is the contract alone, so a narrow panel never has to cut it short
 assert.equal(L.futuresChartTitle({tradingsymbol:'NIFTY25SEPFUT',expiry:'2026-09-25'}),'NIFTY25SEPFUT',
  'the expiry is in the subtitle beside it, not crammed into the title');
 assert.equal(L.futuresChartTitle({tradingsymbol:'  '},'RELIANCE'),'RELIANCE futures');
 assert.equal(L.futuresChartTitle(null,'RELIANCE'),'RELIANCE futures');
 assert.equal(L.futuresChartTitle(null,null),'Futures chart','and with neither, it claims nothing');
 assert.equal(L.futuresChartTitle(null,''),'Futures chart');
});
ok(()=>{ // the long form stays, because it is what a screen reader hears
 assert.equal(L.futuresChartName({tradingsymbol:'NIFTY25SEPFUT',expiry:'2026-09-25'}),'NIFTY25SEPFUT · 25 Sep 2026');
 assert.equal(L.futuresChartName({tradingsymbol:'NIFTY25SEPFUT',expiry:null}),'NIFTY25SEPFUT','no expiry is left out');
 assert.equal(L.futuresChartName({tradingsymbol:'  '},'RELIANCE'),'RELIANCE futures','no contract yet names the symbol');
 assert.equal(L.futuresChartName(null,'RELIANCE'),'RELIANCE futures');
 assert.equal(L.futuresChartName(null,null),'Futures chart','and with neither, it claims nothing');
 assert.equal(L.futuresChartName(null,''),'Futures chart');
});
ok(()=>{ // the subtitle carries the expiry the title gave up, and how far from it the contract is
 assert.equal(L.futuresChartSubtitle({contract:{days_to_expiry:8,expiry:'2026-09-25'},
  candles:[candle(1,2,1,2),candle(2,3,2,3),candle(3,4,3,4)]}),'25 Sep 2026 · 8 days to expiry');
 assert.equal(L.futuresChartSubtitle({contract:{days_to_expiry:0,expiry:'2026-09-25'},candles:[]}),
  '25 Sep 2026 · expires today');
 assert.equal(L.futuresChartSubtitle({contract:{days_to_expiry:8,expiry:null},candles:[]}),'8 days to expiry',
  'a part that is not there is left out, never guessed');
 assert.equal(L.futuresChartSubtitle({contract:{days_to_expiry:null,expiry:null},candles:[]}),'');
 assert.equal(L.futuresChartSubtitle(null),'');
 // the interval is named by the control that chose it, so the subtitle never repeats it
 for(const label of L.FUTURES_INTERVALS.map(c=>c.label))
  assert.ok(!L.futuresChartSubtitle({contract:{days_to_expiry:8,expiry:'2026-09-25'},candles:[]}).includes(label),
   `the subtitle must not repeat "${label}" - the control already shows it`);
});
ok(()=>{ // the window fact: one long sentence for the info panel, one short line for under the chart
 const win=L.candleWindow(Array.from({length:364},()=>candle(1,2,1,2)),239);
 assert.equal(L.candleWindowShort(win),`Latest ${win.shown} of ${win.total} candles`);
 assert.ok(L.candleWindowText(win).includes(String(win.shown))&&L.candleWindowText(win).includes('364'),
  'and the long form still carries the same two numbers');
 assert.equal(L.candleWindowShort({candles:[],shown:0,total:0,windowed:false}),'',
  'nothing is said when the whole series is on screen');
 assert.equal(L.candleWindowShort(null),'');
});
ok(()=>{ // the interval's sentence: the server's when it sent one, ours when it did not
 assert.equal(L.futuresNote({note:'  '},'15m'),L.FUTURES_INTERVAL_NOTE['15m']);
 assert.equal(L.futuresNote(null,'1d'),L.FUTURES_INTERVAL_NOTE['1d']);
 assert.equal(L.futuresNote({note:'Served sentence.'},'15m'),'Served sentence.','the server\'s own words win');
 assert.equal(L.futuresNote(null,'nonsense'),'','and nothing is invented for an interval we do not offer');
 assert.ok(/15-min/.test(L.FUTURES_INTERVAL_NOTE['15m'])&&/session/.test(L.FUTURES_INTERVAL_NOTE['1d']));
});

// --- the candles: drawn only where one was stored ------------------------------------------------------------------
ok(()=>{ // three well-formed candles, oldest on the left, scaled on their own high-to-low range
 const scaled=L.scaleCandles([candle(100,110,95,105),candle(105,120,104,118),candle(118,119,90,92)],120,60);
 assert.ok(scaled,'three stored candles must draw');
 assert.equal(scaled.drawn,3);
 assert.equal(scaled.candles.length,3);
 assert.equal(scaled.lo,90,'the scale is the lowest low');
 assert.equal(scaled.hi,120,'and the highest high');
 assert.ok(scaled.candles[0].x<scaled.candles[1].x&&scaled.candles[1].x<scaled.candles[2].x,'oldest on the left');
 assert.equal(scaled.candles[1].high,0,'the series high sits at the top of the box');
 assert.equal(scaled.candles[2].low,60,'and the series low at the bottom');
 assert.ok(scaled.body>0&&scaled.body<=scaled.step,'a candle body never overruns its own slot');
});
ok(()=>{ // close against open is the only thing the candle's direction says
 const scaled=L.scaleCandles([candle(100,110,95,105),candle(105,110,95,100),candle(100,110,95,100)],90,60);
 assert.deepEqual([...scaled.candles.map(c=>c.direction)],['up','down','flat']);
});
ok(()=>{ // a candle that was not stored whole KEEPS ITS SLOT and is drawn as nothing at all
 const scaled=L.scaleCandles([candle(100,110,95,105),candle(null,null,null,null,'2026-09-18 10:00'),
  candle(105,120,104,118)],120,60);
 assert.equal(scaled.candles.length,3,'the gap keeps its place in the series');
 assert.equal(scaled.candles[1],null,'and it is not drawn');
 assert.equal(scaled.drawn,2,'only what was stored is counted as drawn');
 assert.equal(scaled.candles.filter(Boolean).length,scaled.drawn,'nothing is carried forward into the gap');
 // the gap contributes nothing to the scale either - it is absent, not a zero
 assert.equal(scaled.lo,95);assert.equal(scaled.hi,120);
});
ok(()=>{ // a half-stored candle is a gap too: four prices or none
 for(const bad of [candle(100,null,95,105),candle(null,110,95,105),candle(100,110,null,105),candle(100,110,95,null),
   candle(100,'110',95,105),candle(100,NaN,95,105)]){
  const scaled=L.scaleCandles([candle(1,2,1,2),bad],80,40);
  assert.equal(scaled.candles[1],null,`a candle missing a price is never drawn: ${JSON.stringify(bad)}`);
  assert.equal(scaled.drawn,1);
 }
});
ok(()=>{ // and a candle that is not a candle - a high under its own body, a low over it - is not drawn either
 for(const bad of [candle(100,90,95,105),candle(100,110,120,105),candle(100,99,95,105),candle(100,110,101,105)]){
  const scaled=L.scaleCandles([candle(1,2,1,2),bad],80,40);
  assert.equal(scaled.candles[1],null,`a malformed candle is never drawn: ${JSON.stringify(bad)}`);
 }
 // the well-formed edges still draw: a doji, and a bar whose high and low are its own open and close
 assert.equal(L.scaleCandles([candle(100,100,100,100)],80,40).drawn,1);
 assert.equal(L.scaleCandles([candle(100,105,100,105)],80,40).drawn,1);
});
ok(()=>{ // nothing to draw is null, never an empty box pretending to be a chart
 assert.equal(L.scaleCandles([],80,40),null);
 assert.equal(L.scaleCandles(null,80,40),null);
 assert.equal(L.scaleCandles([candle(null,null,null,null)],80,40),null,'a series of gaps is no chart');
 assert.equal(L.scaleCandles([candle(1,2,1,2)],0,40),null);
 assert.equal(L.scaleCandles([candle(1,2,1,2)],80,0),null);
});
ok(()=>{ // ONE stored candle is still drawn - the panel shows what exists rather than showing nothing
 const scaled=L.scaleCandles([candle(100,110,95,105)],80,40);
 assert.ok(scaled&&scaled.drawn===1,'a single candle is drawn, and the wording says how few there are');
 assert.equal(L.futuresFewText(1),'Only 1 candle stored for this contract — every one of them is drawn.');
 assert.equal(L.futuresFewText(2),'Only 2 candles stored for this contract — every one of them is drawn.');
 assert.equal(L.futuresFewText(L.FUTURES_FEW_CANDLES),'','at the threshold it is a series and says nothing');
 for(const v of [0,null,undefined,NaN,-1])assert.equal(L.futuresFewText(v),'',String(v));
});

// --- a narrow panel shows a TAIL of the series, and says so -----------------------------------------------------------
// 364 fifteen-minute bars in a 250px panel is half a pixel each: every one drawn, not one of them readable.
// The panel draws the most recent ones that are legible. What is protected: it is a tail and never a sample
// (nothing is dropped from the middle, the order is untouched), and leaving bars off the left edge is SAID.
ok(()=>{
 const many=Array.from({length:364},(_,i)=>candle(100+i,101+i,99+i,100.5+i,`2026-09-18 09:${String(i%60).padStart(2,'0')}`));
 const win=L.candleWindow(many,239);
 assert.equal(win.windowed,true,'364 bars do not fit a 239px panel');
 assert.equal(win.shown,79,'it shows what fits at three pixels a candle');
 assert.equal(win.total,364,'and it remembers how many there really are');
 assert.equal(win.candles.length,79);
 // a TAIL: the last candle of the window is the last candle of the series, and the order is untouched
 assert.equal(win.candles[win.candles.length-1].close,many[many.length-1].close,'the newest candle is kept');
 assert.equal(win.candles[0].close,many[many.length-79].close,'and the window is the 79 before it, in order');
 for(let i=1;i<win.candles.length;i++)assert.ok(win.candles[i].close>win.candles[i-1].close,
  'nothing is dropped from the middle of the window');
 // and at that width the candles are finally wide enough to read
 const scaled=L.scaleCandles(win.candles,239,60);
 assert.ok(scaled.step>=3,`a drawn candle gets ${scaled.step.toFixed(2)}px and must clear the legible floor`);
 assert.equal(scaled.drawn,79);
});
ok(()=>{ // a series that fits is never windowed, and nothing is said about a window that is not one
 const few=[candle(1,2,1,2),candle(2,3,2,3),candle(3,4,3,4)];
 const win=L.candleWindow(few,239);
 assert.equal(win.windowed,false);
 assert.equal(win.shown,3);assert.equal(win.total,3);
 assert.equal(win.candles.length,3);
 assert.equal(L.candleWindowText(win),'','a whole series on screen says nothing about a window');
 assert.equal(L.candleWindowText(null),'');
 // an unmeasured panel is not an excuse to drop anything
 assert.equal(L.candleWindow(few,0).candles.length,3);
 assert.equal(L.candleWindow(few,0).windowed,false);
 assert.equal(L.candleWindow(null,239).total,0);
 assert.equal(L.candleWindow([],239).windowed,false);
});
ok(()=>{ // leaving bars off the left edge is never silent, and the sentence carries both numbers
 const text=L.candleWindowText(L.candleWindow(Array.from({length:364},()=>candle(1,2,1,2)),239));
 assert.ok(/79/.test(text)&&/364/.test(text),'the reader is told how many of how many are on screen');
 assert.ok(/latest/.test(text),'and that it is the most recent ones');
 assert.ok(!/\btrends?\b/i.test(text));
 // a gap inside the window keeps its slot exactly as it did before
 const withGap=[...Array.from({length:400},()=>candle(1,2,1,2)).slice(0,399),gapSlot('2026-09-18 15:30')];
 const win=L.candleWindow(withGap,239);
 assert.equal(win.candles[win.candles.length-1].gap,true,'a gap at the edge of the window is still a gap');
 assert.equal(L.scaleCandles(win.candles,239,60).candles.slice(-1)[0],null,'and it is still drawn as nothing');
});
ok(()=>{ // the panel really windows, and the axis and the spoken label describe the window it drew
 const panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/const win=candleWindow\(series,plotW\)/.test(panel),'the panel windows on its own measured width');
 assert.ok(/const candles=win\.candles/.test(panel),'and everything below draws from that window');
 assert.ok(/scaleCandles\(candles,plotW,plotH\)/.test(panel)&&/candleAxisTimes\(candles,interval/.test(panel),
  'the chart and its time axis are the window, so the axis can never label a candle that is not there');
 assert.ok(/const countText=candleWindowShort\(win\)\|\|candleDrawnText\(/.test(panel)
  &&/\{!!countText&&/.test(panel),'and a window is said on screen, never silently cropped');
 assert.ok(/windowText,FUTURES_GAP_TEXT,FUTURES_DISPLAY_TEXT\]/.test(panel),
  'the long form of the same fact goes to the block\'s one info panel, so it is still reachable in full');
 assert.ok(/futuresChartSpoken\(\{contract:body\?\.contract,candles\}/.test(panel),
  'a screen reader hears the window that was drawn, not a count of bars it cannot reach');
 assert.ok(!/const window=/.test(panel),'this renders on web: the name `window` is taken');
});

// --- the axes read off the stored candles, never off an invented calendar --------------------------------------------
ok(()=>{
 assert.equal(L.candleAxisLabel('2026-09-18 10:15','15m'),'10:15','a 15-min candle is labelled by its clock');
 assert.equal(L.candleAxisLabel('2026-09-18','1d'),'18 Sep','a daily candle by its date');
 assert.equal(L.candleAxisLabel('2026-09-18 10:15','1d'),'18 Sep');
 for(const v of [null,undefined,'','nonsense'])assert.equal(L.candleAxisLabel(v,'15m'),DASH,String(v));
 for(const v of [null,undefined,'','nonsense'])assert.equal(L.candleAxisLabel(v,'1d'),DASH,String(v));
});
ok(()=>{
 const day=['09:30','09:45','10:00','10:15','10:30','10:45'].map(t=>candle(1,2,1,2,`2026-09-18 ${t}`));
 const ticks=L.candleAxisTimes(day,'15m',3);
 assert.equal(ticks.length,3);
 for(const tick of ticks)assert.equal(tick.label,L.clock(day[tick.i].at),'a tick names its own stored candle');
 assert.equal(ticks[0].i,0,'the first stored candle is labelled');
 assert.equal(ticks[ticks.length-1].i,day.length-1,'and so is the last');
 assert.deepEqual([...L.candleAxisTimes([candle(1,2,1,2)],'15m',3)],[],'an unreadable stamp is never labelled');
 assert.deepEqual([...L.candleAxisTimes(null,'15m')],[]);
});

// --- the time axis must read forwards ------------------------------------------------------------------------------
// This is the check that was missing. A 15-minute series longer than one session was being labelled by the
// CLOCK, and a clock comes round again every session: 107 candles over four days were labelled
// "13:15 … 11:15 … 15:30" — three labels out of order, half an hour apart, describing 27 trading hours.
// Nothing about the candles was wrong; the axis was printing the wrong one of their own stamps. Anything
// that puts an axis label out of order, or repeats one, fails here.
const SESSION=(iso,n=25,from=9*60+30)=>Array.from({length:n},(_,k)=>{
 const mins=from+k*15;
 return candle(1,2,1,2,`${iso} ${String(Math.floor(mins/60)).padStart(2,'0')}:${String(mins%60).padStart(2,'0')}`);
});
const axisOrder=(candles,interval,want)=>{
 const ticks=[...L.candleAxisTimes(candles,interval,want)];
 const labels=ticks.map(t=>t.label);
 assert.ok(labels.length>0,'an axis with stored stamps must label something');
 assert.deepEqual(labels,[...new Set(labels)],`the axis repeats a label: ${labels.join(' \u00b7 ')}`);
 // what the READER sees, read back: a clock is minutes into a day, a date is a day of the year
 const value=(text)=>{
  const t=/^(\d{2}):(\d{2})$/.exec(text);
  if(t)return Number(t[1])*60+Number(t[2]);
  const d=/^(\d{1,2}) ([A-Za-z]{3})$/.exec(text);
  assert.ok(d,`an axis label must be a clock or a date, not "${text}"`);
  const month=L.MONTHS.indexOf(d[2]);
  assert.ok(month>=0,`an axis label must name a real month: ${text}`);
  return month*31+Number(d[1]);
 };
 const vals=labels.map(value);
 for(let k=1;k<vals.length;k++)assert.ok(vals[k]>vals[k-1],
  `the axis runs backwards: ${labels.join(' \u00b7 ')}`);
 // and every tick still points at a candle that is really there, in the order they were stored
 for(let k=1;k<ticks.length;k++)assert.ok(ticks[k].i>ticks[k-1].i,'ticks must follow the stored order');
 for(const tick of ticks)assert.ok(candles[tick.i],'a tick must name a candle that exists');
 return labels;
};
ok(()=>{ // one session of 15-minute candles: the clock is the right label and it runs forwards
 const labels=axisOrder(SESSION('2026-09-18'),'15m',4);
 assert.ok(labels.every(t=>/^\d{2}:\d{2}$/.test(t)),`a single session is labelled by the clock: ${labels.join(' ')}`);
 assert.equal(labels[0],'09:30','starting at the first candle stored');
 assert.equal(labels[labels.length-1],'15:30','and ending at the last');
});
ok(()=>{ // the bug: four sessions of 15-minute candles, 100 candles, labelled by the clock
 const many=[...SESSION('2026-09-15'),...SESSION('2026-09-16'),...SESSION('2026-09-17'),...SESSION('2026-09-18')];
 assert.equal(many.length,100);
 const labels=axisOrder(many,'15m',4);
 assert.ok(labels.every(t=>/^\d{1,2} [A-Za-z]{3}$/.test(t)),
  `a series that crosses days is labelled by the day, never by a clock that repeats: ${labels.join(' ')}`);
 assert.deepEqual(labels,['15 Sep','16 Sep','17 Sep','18 Sep']);
 // the label sits on the FIRST candle of its day, which is the only place a day label is true
 const ticks=[...L.candleAxisTimes(many,'15m',4)];
 assert.deepEqual(ticks.map(t=>t.i),[0,25,50,75]);
});
ok(()=>{ // more sessions than labels: the ends are kept and the middle is thinned, still in order
 const iso=(d)=>`2026-09-${String(d).padStart(2,'0')}`;
 const many=[1,2,3,4,7,8,9,10].flatMap(d=>SESSION(iso(d),25));
 const labels=axisOrder(many,'15m',3);
 assert.equal(labels.length,3);
 assert.equal(labels[0],'1 Sep','the first session stored is labelled');
 assert.equal(labels[labels.length-1],'10 Sep','and so is the last');
});
ok(()=>{ // the shape the panel actually drew: the LAST 107 of 364, so the window opens mid-session
 const all=[15,16,17,18].flatMap(d=>SESSION(`2026-09-${d}`,25));
 const win=L.candleWindow(all,239);
 assert.ok(win.windowed&&win.shown<all.length,'the panel really does window a series this long');
 const labels=axisOrder(win.candles,'15m',4);
 assert.ok(labels.every(t=>/^\d{1,2} [A-Za-z]{3}$/.test(t)),
  `a windowed multi-session series is still labelled by the day: ${labels.join(' ')}`);
 // the part-session the window opens in is a session of its own, and it comes first
 assert.equal(labels[0],'15 Sep');
 assert.equal(labels[labels.length-1],'18 Sep');
});
ok(()=>{ // the EXACT regression: a window opening part-way through its first session
 // 10 candles from 13:15 on the first day, then three full ones. Labelled by the clock, evenly spaced,
 // this produced "13:15 · 11:15 · 15:30" on the owner's screen - the second label earlier than the first.
 const many=[...SESSION('2026-09-15',10,13*60+15),...SESSION('2026-09-16'),...SESSION('2026-09-17'),
  ...SESSION('2026-09-18')];
 assert.equal(many.length,85);
 assert.deepEqual(axisOrder(many,'15m',4),['15 Sep','16 Sep','17 Sep','18 Sep']);
 // and with fewer labels than sessions it thins the middle without ever going backwards
 const three=axisOrder(many,'15m',3);
 assert.equal(three.length,3);
 assert.equal(three[0],'15 Sep');assert.equal(three[2],'18 Sep');
});
ok(()=>{ // a two-session series is still two days, not a clock that goes backwards
 const two=[...SESSION('2026-09-17'),...SESSION('2026-09-18')];
 assert.deepEqual(axisOrder(two,'15m',4),['17 Sep','18 Sep']);
});
ok(()=>{ // two day labels on the same few pixels: dropped, never stacked
 // A window opens three candles before a day ends, so the first session in view is a stub. Its label and the
 // next day's would be drawn on top of each other, which is how "15 Sep" came to be printed over "11 Sep".
 const many=[...SESSION('2026-09-11',3,15*60),...SESSION('2026-09-15'),...SESSION('2026-09-16'),
  ...SESSION('2026-09-17'),...SESSION('2026-09-18')];
 assert.equal(many.length,103);
 // told nothing about pixels, it makes no claim about them and drops nothing
 assert.equal([...L.candleAxisTimes(many,'15m',5)].length,5,'five sessions, five labels');
 // told the width it is drawn in, the stub's neighbour is dropped rather than stacked on it
 const ticks=[...L.candleAxisTimes(many,'15m',5,345)];
 const labels=ticks.map(t=>t.label);
 assert.deepEqual(labels,[...new Set(labels)],`the axis repeats a label: ${labels.join(' ')}`);
 const step=345/many.length;
 for(let k=1;k<ticks.length;k++)assert.ok((ticks[k].i-ticks[k-1].i)*step>=L.CANDLE_LABEL_PX,
  `two labels are closer than ${L.CANDLE_LABEL_PX}px: ${labels.join(' ')}`);
 assert.ok(labels.length<5&&labels.length>=3,`the crowded label goes, the rest stay: ${labels.join(' ')}`);
 assert.equal(labels[0],'11 Sep','the left edge of the axis is still named');
 assert.equal(labels[labels.length-1],'18 Sep','and so is the right');
 // a narrower panel drops more of them, and never leaves two on the same pixels
 const tight=[...L.candleAxisTimes(many,'15m',5,120)];
 for(let k=1;k<tight.length;k++)assert.ok((tight[k].i-tight[k-1].i)*(120/many.length)>=L.CANDLE_LABEL_PX,
  'a narrow panel still never stacks two labels');
 assert.ok(tight.length>=1&&tight.length<=labels.length,'and it keeps at least the first');
});
ok(()=>{ // the thinning itself: no measured width, no claim
 const marks=[{i:0},{i:1},{i:50}];
 assert.deepEqual([...L.thinAxisLabels(marks,100)],marks,'without a width nothing is dropped');
 assert.deepEqual([...L.thinAxisLabels(marks,100,0)],marks);
 assert.deepEqual([...L.thinAxisLabels(marks,0,300)],marks,'and with no candles there is nothing to space');
 assert.deepEqual([...L.thinAxisLabels(marks,100,300)],[{i:0},{i:50}],
  'at 3px a candle, the label 1 candle along is dropped and the one 50 along is kept');
 assert.deepEqual([...L.thinAxisLabels([],100,300)],[]);
});
ok(()=>{ // the panel hands the axis the width it is really drawn in
 const panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/candleAxisTimes\(candles,interval,5,plotW\)/.test(panel),
  'the time axis is thinned against the plot it is drawn under, not a guess at one');
});
ok(()=>{ // the daily view was always dates, and still is
 const days=[15,16,17,18].map(d=>candle(1,2,1,2,`2026-09-${d}`));
 assert.deepEqual(axisOrder(days,'1d',4),['15 Sep','16 Sep','17 Sep','18 Sep']);
});
ok(()=>{ // a gap inside a multi-session series keeps its slot and is never labelled as a session of its own
 const many=[...SESSION('2026-09-17'),gapSlot('2026-09-17 15:45'),...SESSION('2026-09-18')];
 const labels=axisOrder(many,'15m',4);
 assert.deepEqual(labels,['17 Sep','18 Sep'],'a gap belongs to the day it was stored under');
 assert.deepEqual([...L.candleSessions(many)].map(s=>s.iso),['2026-09-17','2026-09-18']);
 assert.deepEqual([...L.candleSessions([candle(1,2,1,2)])],[],'an unreadable stamp is no session at all');
 assert.deepEqual([...L.candleSessions(null)],[]);
});
ok(()=>{ // what a screen reader hears follows the same rule as the axis it is describing
 const many=[...SESSION('2026-09-17'),...SESSION('2026-09-18')];
 const spoken=L.futuresChartSpoken({contract:{tradingsymbol:'NIFTY26SEPFUT'},candles:many},'15m','NIFTY');
 assert.ok(spoken.includes('from 17 Sep 09:30 to 18 Sep 15:30'),
  `a multi-session range needs its days: ${spoken}`);
 const one=L.futuresChartSpoken({contract:{tradingsymbol:'NIFTY26SEPFUT'},candles:SESSION('2026-09-18')},'15m','NIFTY');
 assert.ok(one.includes('from 09:30 to 15:30'),`and one session needs only its clock: ${one}`);
});
ok(()=>{ // three rupee labels, and none of them on top of another
 const scaled=L.scaleCandles([candle(100,120,90,105)],80,60);
 const axis=L.candleAxis(scaled,60);
 assert.equal(axis.length,3);
 assert.deepEqual([...axis.map(t=>t.v)],[120,105,90]);
 assert.equal(axis[0].y,0);assert.equal(axis[2].y,60);
 assert.equal(L.candleAxis(L.scaleCandles([candle(100,120,90,105)],80,8),8).length,1,
  'labels that would collide are dropped, not stacked');
 assert.deepEqual([...L.candleAxis(null,60)],[]);
 assert.deepEqual([...L.candleAxis(scaled,0)],[]);
});
ok(()=>{ // the tight axis label, and a dash when there is nothing to label
 assert.equal(L.priceTick(25120.4),'25,120');
 assert.equal(L.priceTick(1412.55),'1,413');
 assert.equal(L.priceTick(842.55),'842.6');
 assert.equal(L.priceTick(84.256),'84.26');
 for(const v of MISSING)assert.equal(L.priceTick(v),DASH,String(v));
});

// --- every honest state has its own words ------------------------------------------------------------------------------
ok(()=>{ // the server's short-history sentence, ours when it sent none, and nothing when the series is not short
 assert.equal(L.futuresShortText({short_history:true,short_history_text:'Eleven candles is not a series yet.'}),
  'Eleven candles is not a series yet.');
 assert.equal(L.futuresShortText({short_history:true,short_history_text:'  '}),L.FUTURES_SHORT_HISTORY);
 assert.equal(L.futuresShortText({short_history:true,short_history_text:null}),L.FUTURES_SHORT_HISTORY);
 assert.equal(L.futuresShortText({short_history:false,short_history_text:'ignored'}),'',
  'the sentence is shown only when the server actually flags it');
 assert.equal(L.futuresShortText(null),'');
 assert.equal(L.futuresShortText({}),'');
});
ok(()=>{ // no candles, and not captured, are both empty states - never an error page and never a zero
 const futures=(extra={})=>env({contract:null,interval:'15m',candles:[],intervals:['15m'],short_history:false,
  short_history_text:null,note:null,sessions:null,session:null,...extra});
 assert.equal(L.cardState(futures(),{emptyText:L.FUTURES_NO_CANDLES}).text,L.FUTURES_NO_CANDLES);
 assert.equal(L.cardState(futures(),{emptyText:L.FUTURES_NO_CANDLES}).phase,'empty');
 assert.equal(L.cardState(futures({candles:[candle(1,2,1,2)]})).phase,'ready','one stored candle is not empty');
 assert.equal(L.cardState(futures({captured:false,candles:[candle(1,2,1,2)]})).phase,'empty');
 assert.equal(L.cardState(futures({available:false,candles:[]})).text,L.EMPTY_TEXT);
 assert.equal(L.cardState(null,{error:'404 Not Found'}).phase,'error','the endpoint not being there yet is an error state');
});
ok(()=>{ // the panel's own sentences say what they mean, and nothing more
 assert.ok(/Choose a symbol in Customize/.test(L.FUTURES_NO_SYMBOL)&&/screener/.test(L.FUTURES_NO_SYMBOL));
 assert.ok(/front futures contract/.test(L.FUTURES_NO_SYMBOL));
 assert.ok(/candles/.test(L.FUTURES_NO_CANDLES)&&/stored/.test(L.FUTURES_NO_CANDLES));
 assert.ok(/interpolated/.test(L.FUTURES_GAP_TEXT)&&/carried forward/.test(L.FUTURES_GAP_TEXT),
  'the gap rule is printed on the panel, not left implied');
 assert.ok(/what comes next/.test(L.FUTURES_DISPLAY_TEXT),'and the panel says it is display only');
});
ok(()=>{ // what a screen reader hears is the same facts the eye gets
 const body={contract:{tradingsymbol:'NIFTY25SEPFUT',expiry:'2026-09-25'},
  candles:[candle(100,110,95,105,'2026-09-18 09:30'),candle(105,120,104,118,'2026-09-18 09:45')]};
 const spoken=L.futuresChartSpoken(body,'15m','NIFTY');
 assert.ok(spoken.includes('NIFTY25SEPFUT · 25 Sep 2026'),'the contract is named');
 assert.ok(spoken.includes('15-min candles'),'and the interval');
 assert.ok(spoken.includes('2 candles stored'),'and how many were actually drawn');
 assert.ok(spoken.includes('from 09:30 to 09:45'),'and the window they cover');
 // a gap is not counted as a candle, and nothing at all says so
 const gappy=L.futuresChartSpoken({...body,candles:[...body.candles,candle(null,null,null,null)]},'15m','NIFTY');
 assert.ok(gappy.includes('2 candles stored'),'a gap is never spoken as a candle');
 assert.ok(L.futuresChartSpoken({contract:null,candles:[]},'1d','NIFTY').includes(L.FUTURES_NO_CANDLES));
 assert.ok(L.futuresChartSpoken(null,'15m','').includes(L.FUTURES_NO_CANDLES));
});

// --- ONE symbol drives the whole block ------------------------------------------------------------------------------------
ok(()=>{ // a chosen symbol always wins, and is never flagged as a default
 const picked=L.resolveBlockSymbol('RELIANCE','TCS','NIFTY');
 assert.deepEqual({...picked},{symbol:'RELIANCE',defaulted:false,label:''});
 assert.deepEqual({...L.resolveBlockSymbol('  RELIANCE  ',null,null)},{symbol:'RELIANCE',defaulted:false,label:''});
});
ok(()=>{ // nothing chosen: the busiest by premium first, the index second, and BOTH are flagged as defaults
 assert.deepEqual({...L.resolveBlockSymbol('','TCS','NIFTY')},
  {symbol:'TCS',defaulted:true,label:L.BLOCK_DEFAULT_PREMIUM});
 assert.deepEqual({...L.resolveBlockSymbol(null,null,'NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX});
 assert.deepEqual({...L.resolveBlockSymbol(null,'  ','NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX},'a blank premium answer is no answer');
 assert.equal(L.BLOCK_DEFAULT_PREMIUM,'Busiest by premium');
 assert.equal(L.BLOCK_DEFAULT_INDEX,'Default');
});
ok(()=>{ // nothing anywhere is no symbol - and no symbol is never dressed up as a default
 assert.deepEqual({...L.resolveBlockSymbol(null,null,null)},{symbol:'',defaulted:false,label:''});
 assert.deepEqual({...L.resolveBlockSymbol()},{symbol:'',defaulted:false,label:''});
 assert.deepEqual({...L.resolveBlockSymbol('','','')},{symbol:'',defaulted:false,label:''});
});
ok(()=>{ // the block is HANDED the tab's symbol and gives the same answer to the chart and to all ten tiles
 const block=SRC('OiGridSection.tsx'),panel=SRC('FuturesChartPanel.tsx');
 assert.equal((block.match(/resolveBlockSymbol\(/g)||[]).length,0,'the block no longer resolves its own symbol');
 assert.ok(/export function OiGridSection\(\{symbol,badge,/.test(block),'it takes the tab\'s symbol as a prop');
 assert.ok(/<FuturesChartPanel[\s\S]{0,200}underlying=\{symbol\}/.test(block),'the futures chart is given it');
 assert.ok(/<DeltaOiGrid[\s\S]{0,200}underlying=\{symbol\}/.test(block),'and so is the grid of ten tiles');
 // the panel must never resolve a symbol of its own, by any route
 const panelCode=CODE('FuturesChartPanel.tsx');
 assert.ok(!/resolveBlockSymbol/.test(panelCode),'the futures chart never resolves its own symbol');
 assert.ok(!/derivatives\/unusual/.test(panelCode)&&!/derivatives\/indices/.test(panelCode),
  'and it never reads the block\'s fallback lists for itself');
 assert.equal((panel.match(/useDerivativeRead</g)||[]).length,1,'the panel makes exactly one read: its own candles');
 assert.equal((block.match(/useDerivativeRead</g)||[]).length,2,
  'the grid and the screener - the chart adds no third read to the block');
});
ok(()=>{ // a defaulted symbol is still said to be a default - ONCE, where the tab resolved it, and passed down
 const page=SRC('index.tsx'),block=SRC('OiGridSection.tsx'),panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/const symbol=choice\.symbol,badge=symbolBadge\(choice\)/.test(page),
  'the page names the symbol and flags it when it was a default rather than a choice');
 assert.ok(/badge=\{badge\}/.test(block),'and that badge is actually given to the block\'s section header');
 // one place, not three: a panel must not repeat the block's default notice in its own subtitle
 for(const file of ['OiGridSection.tsx','FuturesChartPanel.tsx'])
  assert.ok(!/defaultLabel\}: /.test(SRC(file)),`${file} must not repeat the block's default notice`);
 assert.ok(!/defaulted/.test(panel),'the futures panel does not carry a default notice of its own at all');
});

// --- the three-panel layout ---------------------------------------------------------------------------------------------
ok(()=>{ // left to right: the screener, the futures chart, the grid - in one section, in that order
 const block=SRC('OiGridSection.tsx');
 assert.ok(block.includes('{screener}{futures}{grid}'),
  'the block is three panels left to right: screener, futures chart, 2 x 5 grid');
 assert.ok(/<Section\b/.test(block),'still one section of the tab');
 assert.ok(block.indexOf('const screener=')<block.indexOf('const futures=')
  &&block.indexOf('const futures=')<block.indexOf('const grid='),'built in the order they are drawn');
 assert.ok(/WidgetFrame/.test(SRC('FuturesChartPanel.tsx')),'the chart wears the block\'s own widget frame');
 assert.ok(/api\/derivatives\/futures-chart\?underlying=/.test(SRC('FuturesChartPanel.tsx')),
  'and it reads the fixed futures-chart route');
});
ok(()=>{ // all three are the SAME height: the chart spans both tile rows, not one
 const block=SRC('OiGridSection.tsx');
 for(const name of ['screener','futures','grid'])
  assert.ok(new RegExp(`const ${name}Style=[^\\n]*:\\{flex:\\w+,minWidth:0,height\\};`).test(block),
   `${name} must take the block's full height when the three sit side by side`);
 assert.ok(!/futuresStyle=[^\n]*height\s*\/\s*2/.test(block),'the chart is never half the block');
 // and stacked, each gets a height of its own rather than collapsing to nothing
 for(const name of ['screener','futures','grid'])
  assert.ok(new RegExp(`const ${name}Style=stacked\\?\\{height`).test(block),`${name} needs a stacked height`);
});
ok(()=>{ // the proportions: the grid keeps the largest share, and neither neighbour is squeezed out
 const block=SRC('OiGridSection.tsx');
 const flex=(name)=>{const m=new RegExp(`\\b${name}=(\\d+(?:\\.\\d+)?)`).exec(block);
  assert.ok(m,`OiGridSection.tsx must declare ${name}`);return Number(m[1]);};
 const screener=flex('SCREENER_FLEX'),futures=flex('FUTURES_FLEX'),grid=flex('GRID_FLEX');
 assert.ok(screener>0&&futures>0&&grid>0,'no panel is given zero width');
 assert.ok(grid>=futures&&grid>screener,'the 2 x 5 grid keeps the largest share of the row');
 assert.ok(futures>=screener*0.5,'the chart is not squeezed to a smear');
 // at a 1536px row with two 12px gaps, no panel may fall under a width it cannot show its content in
 const row=1536-24,total=screener+futures+grid;
 assert.ok(row*screener/total>=260,'the screener must still fit a contract, a premium and its three readings');
 assert.ok(row*futures/total>=260,'the chart must still fit a candle series and its axis');
 assert.ok(row*grid/total>=800,'and the grid must still fit five tiles across');
});
ok(()=>{ // three panels need more room than two: below its own breakpoint the block STACKS rather than
 // quietly dropping the grid under five tiles across, which is what shrinking it would do
 const page=SRC('index.tsx'),block=SRC('OiGridSection.tsx');
 const num=(text,name)=>{const m=new RegExp(`\\b${name}=(\\d+(?:\\.\\d+)?)`).exec(text);
  assert.ok(m,`${name} must be declared`);return Number(m[1]);};
 const beside=num(page,'GRID_BESIDE'),chartBeside=num(page,'CHART_BESIDE');
 assert.ok(beside>chartBeside,'the three-panel block needs a wider break than a table-and-chart row');
 assert.ok(/stacked=\{stacked\|\|page<GRID_BESIDE\|\|!!expanded\}/.test(page),
  'and the block must actually be given that break');
 // the five-column promise, measured: page padding, two 12px gaps, the widget border and the grid's padding
 const flex=(n)=>num(block,n);
 const total=flex('SCREENER_FLEX')+flex('FUTURES_FLEX')+flex('GRID_FLEX');
 const gridWidth=(beside-40-24)*flex('GRID_FLEX')/total,inner=gridWidth-22;
 const cols=/width<(\d+)\?2:width<(\d+)\?3:5/.exec(block);
 assert.ok(cols,'gridColumns must still declare its own wrap points');
 assert.ok(inner>=Number(cols[2]),
  `at ${beside}px the grid gets ${Math.round(inner)}px and must still be five tiles across (needs ${cols[2]})`);
 // and one tile must still be wider than the floor the grid itself refuses to go under
 const cell=(inner-8*4)/5;
 assert.ok(cell>=120,`a tile at the breakpoint is ${Math.round(cell)}px and must clear the grid's own 120px floor`);
});
ok(()=>{ // the chart is a widget of the page like every other: closable, restorable, expandable
 const page=SRC('index.tsx');
 assert.ok(/'oi_grid_screener','oi_grid_futures','oi_grid'/.test(page),
  'oi_grid_futures sits between the screener and the grid in the widget list');
 assert.ok(/shows\('oi_grid_screener'\)\|\|shows\('oi_grid_futures'\)\|\|shows\('oi_grid'\)/.test(page),
  'closing two of the three must not take the third off the page');
 const block=SRC('OiGridSection.tsx');
 for(const piece of ["shows('oi_grid_futures')","onExpand('oi_grid_futures')","onHide('oi_grid_futures')"])
  assert.ok(block.includes(piece),`the chart must support ${piece}`);
 assert.ok(/if\(!screener&&!futures&&!grid\)return null/.test(block),
  'the block disappears only when all three of its panels are closed');
});

// --- the control: 15-min on arrival, daily one click away, a dead interval disabled and SAID ---------------------------------
ok(()=>{
 const panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/useState<FuturesInterval>\(FUTURES_DEFAULT_INTERVAL\)/.test(panel),'15-min is what the reader arrives on');
 assert.ok(/choices\.map\(choice=>/.test(panel),'both choices are drawn');
 assert.ok(!/choices\.filter\(/.test(panel),'a dead interval is DISABLED, never filtered out of the control');
 assert.ok(/disabled=\{!choice\.available\}/.test(panel),'and it is really disabled, not just faded');
 assert.ok(/onPress=\{\(\)=>setInterval\(choice\.key\)\}/.test(panel),'either choice re-reads at that interval');
 assert.ok(/interval=\$\{interval\}/.test(panel),'and the chosen interval is what the route is asked for');
 assert.ok(/const dead=futuresIntervalNote\(choices\)/.test(panel)&&/\{!!dead&&/.test(panel),
  'the reason a choice is dead is on screen - under the chart now, but never swallowed');
 // and it is small print about a control, not amber about the candles: only a short series is amber
 const at=panel.indexOf('{!!dead&&'),end=panel.indexOf('{!!thin&&');
 assert.ok(at>0&&end>at,'the panel must draw the disabled reason and the short-series caveat separately');
 assert.ok(!/C\.amber/.test(panel.slice(at,end)),
  'a disabled control is explained, not flagged as a failure: its reason is not amber');
 assert.ok(/\{!!thin&&<T style=\{\[metaText,\{color:C\.amber\}\]\}>/.test(panel),
  'a series the store is short of IS a caveat on the candles, and keeps the amber');
 assert.ok(/accessibilityState=\{\{selected:on,disabled:!choice\.available\}\}/.test(panel),
  'a screen reader is told which interval is showing and which cannot be');
 assert.ok(/choice\.reason\}/.test(panel),'and it is told why');
});
ok(()=>{ // the control lives OUTSIDE the state switch: an empty interval must never be a dead end
 const panel=SRC('FuturesChartPanel.tsx'),frame=SRC('frame.tsx');
 assert.ok(/toolbar=\{toolbar\}/.test(panel),'the chart puts its control in the frame\'s control strip');
 assert.ok(/toolbar\?:React\.ReactNode/.test(frame),'the frame offers one');
 const strip=frame.indexOf('{!!toolbar&&'),body=frame.indexOf('<View style={{flex:1,minHeight:0}}>{inner}</View>');
 assert.ok(strip>0&&body>0&&strip<body,'and it draws the strip BEFORE the panel that the state replaces');
 // the state-swapped body is `inner`; the strip is not part of it
 assert.ok(!/inner=[\s\S]{0,200}toolbar/.test(frame),'the control is never swapped out with the body');
});
ok(()=>{ // the panel is titled with the contract, and every state it can be in has its own words
 const panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/name=\{futuresChartTitle\(body\?\.contract,underlying\)\}/.test(panel),
  'the title names the contract the candles belong to');
 assert.ok(/subtitle=futuresChartSubtitle\(body\)/.test(panel),'and the subtitle names when it expires');
 for(const piece of ['FUTURES_NO_SYMBOL','FUTURES_NO_CANDLES','futuresShortText','futuresFewText',
  'FUTURES_GAP_TEXT','FUTURES_DISPLAY_TEXT'])
  assert.ok(panel.includes(piece),`the panel must handle ${piece}`);
 assert.ok(/stateOf\(read,FUTURES_NO_CANDLES\)/.test(panel),'a contract with no candles says so in its own words');
 assert.ok(/:\{phase:'empty' as const,text:FUTURES_NO_SYMBOL\}/.test(panel),'and no symbol yet says so too');
 // The definitions moved into the block's one info panel; the CONDITIONS - a dead interval, a short series,
 // a series the panel could barely draw - stay on screen, joined into one amber line under the chart.
 assert.ok(/const thin=\[short,few\]\.filter\(Boolean\)\.join\(' '\)/.test(panel),
  'both short-series sentences are still shown, on one line');
 assert.ok(/\{!!thin&&/.test(panel)&&/\{!!dead&&/.test(panel),
  'and nothing at all is drawn when neither applies');
 assert.ok(/onInfo\?\.\(/.test(panel),'the panel hands its definitions up to the block rather than printing them');
});
ok(()=>{ // the candles are drawn from the stored series, and a gap is drawn as nothing
 const panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/scaleCandles\(candles,plotW,plotH\)/.test(panel),'the panel scales the stored candles itself');
 assert.ok(/if\(!candle\)return null/.test(panel),'a candle that was not stored whole is drawn as nothing');
 assert.ok(/<Rect/.test(panel)&&/<Line/.test(panel),'a candle is a body and a high-to-low line - not a price line');
 assert.ok(!/linePath/.test(panel),'nothing joins one candle to the next, so nothing is interpolated');
 assert.ok(/candleAxisTimes\(candles,interval/.test(panel)&&/candleAxis\(scaled,plotH\)/.test(panel),
  'both axes are read off the very candles that were drawn');
});

// --- §5 over every string CHANGE 3 added -------------------------------------------------------------------------------------
ok(()=>{
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|support level|resistance level|breakout|momentum|overbought|oversold|bullish|bearish|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i;
 // every string in the new panel that could reach the screen, single, double or template quoted
 const said=VISIBLE(SRC('FuturesChartPanel.tsx')).filter(t=>banned.test(t));
 assert.deepEqual(said,[],`FuturesChartPanel.tsx: a string on screen must not predict: ${said.join(' | ')}`);
 // and the whole file with its comments stripped, because text written straight into JSX is never quoted at
 // all — "Last close {price(x)}" reaches the reader without a quote mark anywhere near it
 const loose=CODE('FuturesChartPanel.tsx').split('\n').filter(line=>banned.test(line));
 assert.deepEqual(loose,[],`FuturesChartPanel.tsx: nothing on this panel predicts: ${loose.join(' | ')}`);
 // and every sentence the new logic can produce, whatever the data does
 const sentences=[L.FUTURES_NO_SYMBOL,L.FUTURES_NO_CANDLES,L.FUTURES_SHORT_HISTORY,L.FUTURES_GAP_TEXT,
  L.FUTURES_DISPLAY_TEXT,...Object.values(L.FUTURES_INTERVAL_NOTE),...L.FUTURES_INTERVALS.map(c=>c.label),
  ...L.futuresIntervalChoices({intervals:[]}).map(c=>c.reason),L.futuresIntervalNote(L.futuresIntervalChoices({intervals:[]})),
  L.futuresFewText(1),L.futuresFewText(2),L.futuresShortText({short_history:true}),
  L.futuresChartName({tradingsymbol:'NIFTY25SEPFUT',expiry:'2026-09-25'}),L.futuresChartName(null,null),
  L.futuresChartSubtitle({contract:{days_to_expiry:8},candles:[{}]},'15m'),
  L.futuresChartSubtitle({contract:{days_to_expiry:0},candles:[{}]},'1d'),
  L.futuresNote(null,'15m'),L.futuresNote(null,'1d'),L.BLOCK_DEFAULT_PREMIUM,L.BLOCK_DEFAULT_INDEX,
  L.futuresChartSpoken({contract:{tradingsymbol:'X',expiry:'2026-09-25'},
   candles:[candle(1,2,1,2,'2026-09-18 09:30'),candle(2,3,1,1,'2026-09-18 09:45')]},'15m','X'),
  L.futuresChartSpoken(null,'1d','X')];
 for(const text of sentences)assert.ok(!banned.test(String(text)),`a futures-chart sentence must not predict: ${text}`);
 // the word the owner used for the daily view is allowed nowhere else, and never as a claim about direction
 for(const text of sentences)assert.ok(!/\btrends?\b/i.test(String(text)),
  `"trend" names the daily view and nothing else: ${text}`);
});
ok(()=>{ // the reader never sees our internal word for a 15-minute timestamp on the new panel either
 const left=marky(SRC('FuturesChartPanel.tsx'));
 assert.deepEqual(left,[],`FuturesChartPanel.tsx: say "15-min reading(s)", not "mark": ${left.join(' | ')}`);
 const fromLogic=[L.FUTURES_NO_SYMBOL,L.FUTURES_NO_CANDLES,L.FUTURES_SHORT_HISTORY,L.FUTURES_GAP_TEXT,
  L.FUTURES_DISPLAY_TEXT,...Object.values(L.FUTURES_INTERVAL_NOTE),L.futuresFewText(1),
  ...L.futuresIntervalChoices({intervals:[]}).map(c=>c.reason)];
 for(const text of fromLogic)assert.ok(!MARK_WORD.test(String(text)),`a futures-chart sentence still says "mark": ${text}`);
 assert.ok(/15-min reading/.test(L.FUTURES_INTERVAL_NOTE['15m']),'and the owner\'s replacement is there, in his words');
});

// =================================================================================================================
// CHANGE 4: the ΔOI block reads as ONE block
// The owner's complaint was not about a number. It was that the block was a wall of grey prose under three
// panels of three different densities, with titles cut short and the same as-of and the same floors printed
// more than once. What is protected below: the definitions still exist but live behind ONE control in the
// block header, closed on arrival; the as-of and the floors are printed ONCE for the block and not once per
// panel; no title is cut short at any width the tab renders the block at; both tile rows are the same height
// to the pixel; and the §5 sweep now covers double-quoted strings too, because that is where some of the
// moved text landed.
// =================================================================================================================

// --- the definitions are behind one control, and that control is CLOSED on arrival -------------------------------
ok(()=>{
 const frame=SRC('frame.tsx');
 assert.ok(/export function InfoDisclosure\(/.test(frame),'the block needs one disclosure control');
 assert.ok(/const \[open,setOpen\]=React\.useState\(false\)/.test(frame),'and it is closed on arrival');
 assert.ok(/if\(!shown\.length\)return null/.test(frame),'no control at all when there is nothing to explain');
 assert.ok(/<Popover open=\{open\} onClose=\{\(\)=>setOpen\(false\)\} anchor=\{ref\}/.test(frame),
  'it opens in the shared Popover, which is what closes it on Escape and returns focus to the control');
 assert.ok(/accessibilityState=\{\{expanded:open\}\}/.test(frame)&&/'aria-expanded':open/.test(frame),
  'and a screen reader is told whether it is open');
 assert.ok(/<ScrollView contentContainerStyle=\{\{paddingHorizontal:14/.test(frame),
  'the full text scrolls inside the panel: nothing in it is clamped');
 // the lines INSIDE the panel are never clamped; only the control's own label is a one-liner
 const body=frame.slice(frame.indexOf('<Popover open={open}'),frame.indexOf('</Popover>'));
 assert.ok(body.length>0&&!/numberOfLines/.test(body),'no definition inside the panel is clamped');
});
ok(()=>{ // the block really uses it, and every sentence that used to sit under the panels is in it
 const block=SRC('OiGridSection.tsx');
 assert.ok(/info=\{<InfoDisclosure /.test(block),'the block header carries the control');
 const at=block.indexOf('const groups:InfoGroup[]=[');
 assert.ok(at>0,'and builds its groups in one place');
 const groups=block.slice(at,block.indexOf('];',at));
 for(const piece of ['delta_oi_text','atm_text','direction_text','price_text','flow_text','block_text',
  'GRID_LEGEND_TEXT','TILE_READING_TEXT','gridBasis(gridBody)','gridSourceText(gridBody?.points_source)',
  'GRID_GAP_TEXT','SCREENER_FLOOR_TEXT','SCREENER_CLICK_TEXT','floors','chartInfo'])
  assert.ok(groups.includes(piece),`the definitions panel must still carry ${piece}`);
 // a panel the reader closed takes its group with it: the block never explains what is not on screen
 assert.ok(/lines:!grid\?\[\]:\[/.test(block)&&/lines:!screener\?\[\]:\[/.test(block)
  &&/lines:futures\?chartInfo:\[\]/.test(block),'a closed panel contributes no group');
 // and NOTHING of the old wall is left printed under the panels
 assert.ok(!/fontSize:10,lineHeight:14,color:C\.muted\}\}>\{body\?\.delta_oi_text/.test(block),
  'the ΔOI definition is no longer printed under the grid');
});

// --- ONE as-of line for the block, and one set of floors ---------------------------------------------------------
ok(()=>{
 const frame=SRC('frame.tsx'),block=SRC('OiGridSection.tsx'),panel=SRC('FuturesChartPanel.tsx');
 assert.ok(/asOf\?:string;/.test(frame),'the section header can carry the block\'s one as-of line');
 assert.ok(/asOf=\{asOfText\(gridBody\?\.as_of\)\}/.test(block),'and the block gives it one');
 // every panel of the block is marked as living inside one: screener, grid, futures chart
 assert.equal((block.match(/\binBlock\b/g)||[]).length,2,'both panels in this file sit inside the block');
 assert.equal((panel.match(/\binBlock\b/g)||[]).length,1,'and so does the futures chart');
 // the in-block footer prints the panel's own line and NOTHING the block header already printed
 const at=frame.indexOf('{inBlock?('),end=frame.indexOf('footer!==undefined?');
 assert.ok(at>0&&end>at,'the frame must have an in-block footer branch');
 const foot=frame.slice(at,end);
 assert.ok(!/asOfText|floorsText/.test(foot),
  'a panel inside a block must not repeat the as-of or the floors its block header already printed');
 // and no panel of the block asks the frame for the full as-of/floors footer by leaving inBlock off
 for(const tag of ['<UnusualScreenerPanel','<FuturesChartPanel','<DeltaOiGrid'])
  assert.ok(block.includes(tag),`${tag} must be mounted by the block`);
 assert.equal((block.match(/asOfText\(/g)||[]).length,1,'the block prints an as-of exactly once');
 assert.equal((block.match(/floorsText\(/g)||[]).length,1,'and reads the floors exactly once');
 assert.ok(/onFloors\?\.\(floors\)/.test(block),'which the screener hands up to the block header');
});

// --- no title is cut short at any width the tab draws this block at ----------------------------------------------
// The header bar's own numbers, read out of frame.tsx so this cannot drift from the layout: the gutters, the
// gap between its children and the three icon buttons. What is left after those is what a title has to fit in.
// The per-character figures are deliberate over-estimates for Inter at 11px (uppercase + 1px letter-spacing
// for a title, ordinary case for a subtitle), so a title that passes here has room to spare in the browser.
const TITLE_PX=8.0,SUB_PX=5.6;
ok(()=>{
 const frame=SRC('frame.tsx'),block=SRC('OiGridSection.tsx'),page=SRC('index.tsx');
 const bar=/const btn=inBlock\?(\d+):\d+,pad=inBlock\?(\d+):\d+,gap=inBlock\?(\d+):\d+/.exec(frame);
 assert.ok(bar,'frame.tsx must declare the in-block header geometry in one place');
 const btn=Number(bar[1]),pad=Number(bar[2]),gap=Number(bar[3]);
 // titleBlock, spacer, refresh, expand, close = five children and four gaps
 const chrome=2+pad+2+gap*4+4+btn*3;
 const num=(text,name)=>{const m=new RegExp(`\\b${name}=(\\d+(?:\\.\\d+)?)`).exec(text);
  assert.ok(m,`${name} must be declared`);return Number(m[1]);};
 const flex={screener:num(block,'SCREENER_FLEX'),futures:num(block,'FUTURES_FLEX'),grid:num(block,'GRID_FLEX')};
 const total=flex.screener+flex.futures+flex.grid;
 const beside=num(page,'GRID_BESIDE');
 // the narrowest width the three still sit side by side at: page padding 20 either side, two 12px gaps
 const row=beside-40-24;
 const box=(key)=>row*flex[key]/total-chrome;
 // the longest title each panel can ever show. A futures tradingsymbol is a root of at most ~11 characters
 // plus "26SEPFUT", and the panel now shows the contract alone.
 const titles={screener:'Unusual contracts',grid:'Strikes at the money',futures:'BAJAJFINSV26SEPFUT'};
 // the two fixed titles are read straight out of the source, so a longer one cannot slip past this
 for(const m of block.matchAll(/name="([^"]+)"/g)){
  const key=/strike/i.test(m[1])?'grid':'screener';
  assert.ok(m[1].length*TITLE_PX<=box(key),
   `at ${beside}px the ${key} panel gives its title ${Math.round(box(key))}px and "${m[1]}" needs ${Math.round(m[1].length*TITLE_PX)}px`);
 }
 for(const [key,text] of Object.entries(titles))assert.ok(text.length*TITLE_PX<=box(key),
  `at ${beside}px the ${key} panel gives its title ${Math.round(box(key))}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 // and the subtitle beside it, at the same width
 const subs={screener:'Over the floors',futures:'25 Sep 2026 · 8 days to expiry',
  grid:'NIFTY · 2026-09-25 · ATM 23,350 · spot ₹23,346.40'};
 for(const [key,text] of Object.entries(subs))assert.ok(text.length*SUB_PX<=box(key),
  `at ${beside}px the ${key} subtitle needs ${Math.round(text.length*SUB_PX)}px and has ${Math.round(box(key))}px`);
 // stacked, each panel is the page wide: the narrowest phone the tab renders at must still fit every title
 const phone=360-24-chrome;
 for(const text of Object.values(titles))assert.ok(text.length*TITLE_PX<=phone,
  `stacked on a 360px phone a title has ${Math.round(phone)}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
});
ok(()=>{ // the titles really are those strings, and the long ones the owner saw are gone
 const block=SRC('OiGridSection.tsx');
 assert.ok(block.includes('name="Unusual contracts"'),'the screener\'s title is the short one');
 assert.ok(block.includes('name="Strikes at the money"'),'and so is the grid\'s');
 for(const old of ['Unusual derivative screener','ΔOI by strike · ten strikes at the money'])
  assert.ok(!block.includes(old),`"${old}" was cut short in its own header and must not come back`);
 assert.ok(/subtitle=\{\(rules\|\|\[\]\)\.length\?rulesLine:SCREENER_IDLE_SUB\}/.test(block),
  'and with no filter in force the screener says so in a phrase that fits');
});

// --- both tile rows are the same height, to the pixel -------------------------------------------------------------
ok(()=>{
 const block=SRC('OiGridSection.tsx');
 assert.ok(/export const TILE_H=CELL_PAD\*2\+BORDER\+ROW_GAP\*5\+TITLE_H\+DETAIL_H\+CHART_H\+CHIP_H\+WHAT_H\+MEANING_H;/
  .test(block),'one tile height, built out of the fixed height of every zone in a tile');
 assert.equal((block.match(/height:TILE_H/g)||[]).length,1,'and exactly one place sets it');
 assert.ok(/width,height:TILE_H,padding:CELL_PAD/.test(block),'which is the tile itself');
 // every zone inside is fixed, so a longer label can never make a call tile taller than a put tile
 for(const zone of ['height:TITLE_H','height:DETAIL_H','height:CHIP_H','height:WHAT_H','height:MEANING_H'])
  assert.ok(block.includes(zone),`${zone} must be fixed for the two rows to stay in step`);
 // both bands are drawn by the SAME cell at the SAME width - there is no per-row sizing anywhere
 assert.equal((block.match(/<DeltaCell /g)||[]).length,1,'both rows are the same component');
 assert.ok(/bands\.map\(band=>/.test(block)&&/band\.slots\.map\(slot=>/.test(block),
  'and both rows are laid out by one map over the served bands');
 assert.ok(/width=\{cell\}/.test(block),'at one measured cell width');
 // the reading under the chip keeps both of its lines: the space is reserved, the text is not clamped away
 assert.ok(/numberOfLines=\{2\}[\s\S]{0,120}flow\.meaning/.test(block),
  'the second sentence still gets two lines');
});

// --- the OI-by-strike section no longer owns 600px of page while showing one sentence -----------------------------
ok(()=>{
 const sect=SRC('OiByStrikeSection.tsx'),page=SRC('index.tsx');
 const idle=/export const IDLE_H=(\d+);/.exec(SRC('frame.tsx'));
 assert.ok(idle,'frame.tsx must declare, once, the height an unchosen widget needs');
 assert.ok(/\bIDLE_H\b/.test(sect)&&/\bIDLE_H\b/.test(page),
  'and every section that collapses must use that one number, not its own');
 const rowH=/\bROW_H=(\d+)/.exec(page);
 assert.ok(rowH&&Number(idle[1])<Number(rowH[1])/2,
  'and it must be well under the height the section takes when it has a symbol');
 assert.ok(/const idle=!underlying;/.test(sect),'the collapse is driven by there being no symbol yet');
 assert.ok(/const tableH=idle\?IDLE_H:height;/.test(sect)&&/height:idle\?IDLE_H:chartHeight\+150/.test(sect),
  'both panels of the section collapse together, so the row stays square');
 assert.ok(/note=\{idle\?undefined:/.test(sect),'and an empty panel prints no footer line about rows it has not got');
 // nothing about WHAT it says changed: the same empty sentence, the same note when there is a symbol
 assert.ok(sect.includes('Add a "Symbol is …" filter to see open interest by strike.'),
  'the empty sentence is untouched');
 assert.ok(sect.includes('neither is a forecast.'),'and so is the max-pain note');
});

// --- §5 over the moved text, double-quoted strings included -----------------------------------------------------
ok(()=>{
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|support level|resistance level|bullish|bearish|buy signal|sell signal)\b/i;
 // VISIBLE() takes single, double AND template quotes, which the older sweeps did not: some of the text the
 // rebuild moved is now a double-quoted prop, and a §5 sweep that cannot see it is not a sweep.
 for(const file of ['OiGridSection.tsx','FuturesChartPanel.tsx','frame.tsx','OiByStrikeSection.tsx']){
  const said=VISIBLE(SRC(file)).filter(t=>banned.test(t)&&!/neither is a forecast/.test(t));
  assert.deepEqual(said,[],`${file}: a string the reader can reach must not predict: ${said.join(' | ')}`);
 }
 // the sentences this change introduced, whatever the data does
 for(const name of ['TILE_READING_TEXT','SCREENER_FLOOR_TEXT','SCREENER_CLICK_TEXT','SCREENER_IDLE_SUB',
  'GRID_GAP_TEXT']){
  const m=new RegExp(`export const ${name}='([^']*)'`).exec(SRC('OiGridSection.tsx'));
  assert.ok(m,`OiGridSection.tsx must declare ${name}`);
  assert.ok(!banned.test(m[1]),`${name} must not predict: ${m[1]}`);
  assert.ok(!MARK_WORD.test(m[1]),`${name} must say "15-min reading", not "mark": ${m[1]}`);
 }
 // and the one the disclosure control itself puts on screen
 assert.ok(SRC('frame.tsx').includes("label='How to read this'"),
  'the control says what it opens, in plain words');
});

// =================================================================================================================
// THE FIVE SESSION BLOCKS — screener · PCR · max pain · IV · futures build-up
//
// The three things these checks exist to stop, in the order they cost something:
//   1. A FILTER RENDERED AS ACTIVE THAT THE SERVER DID NOT APPLY. That is a day of this project, spent on a
//      sample-size filter that showed as on while it silently deleted every row. Every branch of that decision is
//      pinned below, including the one that caused it: a server that says nothing.
//   2. IMPLIED VOLATILITY MISTAKEN FOR AN EXCHANGE NUMBER. It is the only figure on this tab a model produced, and
//      it must carry that wherever it appears — not once in a footnote.
//   3. AN AXIS THAT RUNS BACKWARDS. One shipped. The labels are built in one place now, and this is where the one
//      place is held to strictly increasing, distinct and parseable.
// =================================================================================================================
const SESSION_FILES=['ScreenerSection.tsx','SessionPanel.tsx','SessionBlocks.tsx','UnusualWidget.tsx'];
const rule=(column,operator,value)=>({column,operator,value});

// --- the screener's query: every new filter reaches the route, and nothing else does ------------------------------
ok(()=>{ // each of the seven new columns becomes the parameter the contract names
 const cases=[[rule('volumeRatio','gt','2'),'min_volume_ratio=2'],[rule('volumeToOi','gt','1'),'min_volume_to_oi=1'],
  [rule('oiChange15m','gt','5'),'min_oi_change_15m_pct=5'],[rule('oiChange15m','lt','5'),'max_oi_change_15m_pct=5'],
  [rule('oiChangeDay','gt','10'),'min_oi_change_day_pct=10'],[rule('oiChangeDay','lt','10'),'max_oi_change_day_pct=10'],
  [rule('buildup','is','Long build-up'),'buildup=Long%20build-up'],[rule('moneyness','is','atm'),'moneyness=atm'],
  [rule('market','is','index'),'underlying_kind=index']];
 for(const [r,want] of cases)assert.equal(L.screenerQuery([r]),`?${want}`,JSON.stringify(r));
});
ok(()=>{ // the shared six keep the SAME inclusive translations they already had, so a rule means one thing
 assert.equal(L.screenerQuery([rule('dte','lt','7')]),'?max_dte=6');
 assert.equal(L.screenerQuery([rule('premium','gt','10')]),'?min_premium_cr=10.01');
 assert.equal(L.screenerQuery([rule('optionType','is_not','CE')]),'?option_type=PE');
 assert.equal(L.screenerQuery([rule('underlying','is','M&M')]),'?underlying=M%26M');
 assert.equal(L.screenerQuery([rule('watchlist','in_watchlist','all')]),'',
  'the "all underlyings" watch list is not a narrowing, so nothing is sent');
});
ok(()=>{ // a rule restored from localStorage is re-typed before it can reach the wire
 for(const junk of [[rule('volumeRatio','gt','0')],[rule('volumeRatio','gt','-2')],[rule('volumeToOi','gt','abc')],
  [rule('buildup','is','long_buildup')],[rule('buildup','is','bull')],[rule('buildup','is','long_buildup')],[rule('moneyness','is','deep')],
  [rule('market','is','crypto')],
  [rule('oiChange15m','gt','-5')],[rule('oiChange15m','gt','1e9')],[rule('volumeRatio','is','2')]])
  assert.equal(L.screenerQuery(junk),'',JSON.stringify(junk));
 assert.equal(L.screenerQuery([]),'');assert.equal(L.screenerQuery(null),'');
});
ok(()=>{ // nothing the builder can produce leaves a parameter the SERVER's own filter table does not name.
 // Not a hardcoded list: a second copy of the server's keys is a second thing to get out of date, and being
 // out of date here is a 400 the reader meets instead of rows.
 const known=SCREENER_KEYS;
 const every=L.FILTER_COLUMNS.flatMap(c=>c.operators.map(op=>rule(c.key,op,
  c.key==='underlying'?'RELIANCE':c.key==='expiry'?'2026-09-25':c.key==='optionType'?'CE':c.key==='watchlist'?'indices'
  :c.key==='buildup'?L.BUILDUP_VALUES[0]:c.key==='moneyness'?'atm':c.key==='market'?'index':'2')));
 for(const r of every){
  const query=L.screenerQuery([r]);
  if(!query)continue;
  for(const part of query.slice(1).split('&'))assert.ok(known.has(part.split('=')[0]),`stray parameter: ${part}`);
 }
 assert.equal(L.SCREENER_PATH,'/api/derivatives/screener');
});

// --- THE INVARIANT: a filter is active only when the SERVER says it applied it -------------------------------------
// The shapes here are the SERVED ones, not a provisional contract. Three of them are the reason this whole
// section exists, because each is a way to read a refusal as a success:
//   * `applied` is a list of OBJECTS carrying `key`, not a list of keys. Stringifying one gives "[object
//     Object]", which matches nothing - so every filter would have read as not applied. That is wrong in the
//     safe direction, and the same mistake pointing the other way is what cost this project a day.
//   * `available` on this route is the envelope's BOOLEAN. Read as a list it is empty, and every filter the
//     reader set would read as unsupported. The list is `available_filters` / `filters.available`.
//   * A filter the store offers but cannot answer carries `ready:false` and names its missing columns.
const RULES=[rule('premium','gt','10'),rule('volumeRatio','gt','2')];
const APPLIED=(...keys)=>keys.map(key=>({key,value:1,always:false,text:`${key} was applied.`}));
const OFFERS=(...keys)=>keys.map(key=>({key,kind:'number',op:'>=',ready:true,text:key,missing_columns:[]}));
ok(()=>{ // no answer at all - loading, an error, a 400 - is NOT an applied filter
 for(const body of [null,undefined,{},{rows:[]},{available:true},{available_filters:OFFERS('min_premium_cr')}]){
  const seen=L.filterStatuses(RULES,body);
  assert.deepEqual([...seen.map(s=>s.state)],['pending','pending'],JSON.stringify(body));
  assert.equal(L.appliedCount(seen),0,'silence is never an applied filter');
  for(const s of seen)assert.ok(s.reason,'and a pending filter still says why it is not active');
 }
});
ok(()=>{ // `applied` is a list of OBJECTS, and its `key` is what decides
 const seen=L.filterStatuses(RULES,{applied:APPLIED('min_premium_cr'),
  available_filters:OFFERS('min_premium_cr','min_volume_ratio')});
 assert.deepEqual([...seen.map(s=>s.state)],['applied','not_applied']);
 assert.equal(L.appliedCount(seen),1);
 assert.equal(seen[0].key,'min_premium_cr',"the key is the server's own parameter name");
 assert.equal(seen[0].reason,'','an applied filter needs no excuse');
 assert.ok(/did not apply/i.test(seen[1].reason));
 // the SAME answer nested under `filters`, which the server also sends
 const nested=L.filterStatuses(RULES,{filters:{applied:APPLIED('min_premium_cr'),
  available:OFFERS('min_premium_cr','min_volume_ratio')}});
 assert.deepEqual([...nested.map(s=>s.state)],['applied','not_applied']);
 // a bare string list is accepted too, so a leaner server never reads as silence
 assert.deepEqual([...L.filterStatuses(RULES,{applied:['min_premium_cr'],
  available_filters:['min_premium_cr','min_volume_ratio']}).map(s=>s.state)],['applied','not_applied']);
});
ok(()=>{ // `available` is the envelope's BOOLEAN on this route and must never be read as the filter list
 const body={applied:APPLIED('min_premium_cr'),available:true,
  available_filters:OFFERS('min_premium_cr','min_volume_ratio')};
 const seen=L.filterStatuses(RULES,body);
 assert.deepEqual([...seen.map(s=>s.state)],['applied','not_applied'],
  'a boolean read as a list would make every filter unsupported');
 assert.deepEqual([...(L.servedFilters(body)||[]).map(f=>f.key)],['min_premium_cr','min_volume_ratio']);
 assert.equal(L.servedFilters({available:true}),null,'a boolean is not a filter list');
 assert.equal(L.servedFilters(null),null);
 assert.equal(L.servedApplied({}),null,'and no applied list is silence, not an empty one');
 // the server really does serve it under that name, and really does keep `available` a boolean there
 assert.ok(/available_filters=available/.test(DERIV_PY),
  'derivatives.py must serve the filter list as available_filters');
 assert.ok(/filters=\{'applied':applied,'available':available\}/.test(DERIV_PY),
  'and nest both under filters');
});
ok(()=>{ // a parameter the store does not offer at all is its own state, and still not active
 const seen=L.filterStatuses(RULES,{applied:APPLIED('min_premium_cr'),available_filters:OFFERS('min_premium_cr')});
 assert.deepEqual([...seen.map(s=>s.state)],['applied','unsupported']);
 assert.equal(L.appliedCount(seen),1);
 assert.equal(seen[1].reason,L.FILTER_UNSUPPORTED_REASON);
});
ok(()=>{ // a filter the store OFFERS but cannot answer says which columns it is missing, in the server's words
 const seen=L.filterStatuses(RULES,{applied:APPLIED('min_premium_cr'),
  available_filters:[...OFFERS('min_premium_cr'),{key:'min_volume_ratio',ready:false,
   missing_columns:['vol_tod_ratio','vol_tod_sessions']}]});
 assert.equal(seen[1].state,'unsupported');
 assert.ok(/vol_tod_ratio and vol_tod_sessions/.test(seen[1].reason),seen[1].reason);
 assert.equal(L.appliedCount(seen),1);
});
ok(()=>{ // an EMPTY applied list is an answer: nothing was applied, and nothing reads as applied
 const seen=L.filterStatuses(RULES,{applied:[],available_filters:OFFERS('min_premium_cr','min_volume_ratio')});
 assert.deepEqual([...seen.map(s=>s.state)],['not_applied','not_applied']);
 assert.equal(L.appliedCount(seen),0);
});
ok(()=>{ // THE BUG, pinned: a filter the reader set, sent, and the server did not apply, is never called active
 const seen=L.filterStatuses([rule('volumeRatio','gt','2')],
  {applied:[],available_filters:OFFERS('min_volume_ratio')});
 assert.equal(seen[0].state,'not_applied');
 assert.equal(L.appliedCount(seen),0);
 assert.equal(L.appliedText(seen),'No filter applied by the server');
 assert.ok(!L.appliedText(seen).includes('Volume vs median'));
});
ok(()=>{ // the §3 floors and the reading are on for EVERY query, and are not the reader's filters
 const body={applied:[{key:'at',value:'2026-09-18 15:45:00',always:true,text:'The 15-minute reading of x.'},
  {key:'min_premium_cr',value:2,always:true,text:'Premium traded at or above 2 cr.'},
  ...APPLIED('min_volume_ratio')],available_filters:OFFERS('min_volume_ratio')};
 assert.deepEqual([...L.alwaysApplied(body)],
  ['The 15-minute reading of x.','Premium traded at or above 2 cr.'],
  "the always-on clauses are carried, in the server's own words");
 // and they never appear in the reader's own strip, which is only the rules the reader set
 const seen=L.filterStatuses([rule('volumeRatio','gt','2')],body);
 assert.equal(seen.length,1);
 assert.equal(seen[0].state,'applied');
 assert.deepEqual([...L.alwaysApplied(null)],[]);
});
ok(()=>{ // what the reader is told, in plain words, where the rows are
 const off=L.filterStatuses(RULES,{applied:APPLIED('min_premium_cr'),
  available_filters:OFFERS('min_premium_cr','min_volume_ratio')});
 const text=L.notAppliedText(off);
 assert.ok(text.includes('volume vs median'),'the filter is NAMED');
 assert.ok(/NOT narrowed/.test(text),'and the rows are said not to be narrowed by it');
 assert.ok(!text.includes('premium'),'the one that WAS applied is not named as a problem');
 assert.equal(L.notAppliedText(L.filterStatuses(RULES,{applied:APPLIED('min_premium_cr','min_volume_ratio'),
  available_filters:OFFERS('min_premium_cr','min_volume_ratio')})),'');
 assert.equal(L.notAppliedText([]),'');
});
ok(()=>{ // three filters read as a sentence, not as a list of one
 assert.equal(L.joinWords(['a']),'a');
 assert.equal(L.joinWords(['a','b']),'a and b');
 assert.equal(L.joinWords(['a','b','c']),'a, b and c');
 assert.equal(L.joinWords([]),'');
});
ok(()=>{ // no filter set at all is not the same sentence as filters set and none applied
 assert.equal(L.appliedText([]),'No filters — every contract over the liquidity floors');
 assert.notEqual(L.appliedText([]),L.appliedText(L.filterStatuses(RULES,{applied:[],available_filters:[]})));
});
ok(()=>{ // and the SCREEN really is wired to that decision and to nothing else
 const widget=SRC('UnusualWidget.tsx'),block=SRC('ScreenerSection.tsx');
 assert.ok(/filterStatuses\(rules\|\|\[\],body,ruleLabel\)/.test(block),
  "the block asks logic.filterStatuses, with the SERVER body, for every rule's state");
 assert.equal((CODE('ScreenerSection.tsx').match(/filterStatuses\(/g)||[]).length,1,
  'and it is decided in exactly one place');
 assert.ok(!/filterStatuses/.test(CODE('UnusualWidget.tsx')),'the table is handed that answer, never its own');
 assert.ok(/const on=appliedCount\(statuses\)/.test(widget),'the table counts applied filters');
 assert.ok(/filterCount=\{on\}/.test(widget),'and that is what the header bar shows');
 assert.ok(!/filterCount=\{rules\.length\}/.test(widget)&&!/filterCount=\{\(rules\|\|\[\]\)\.length\}/.test(widget),
  'the header must never count a rule the server did not apply');
 assert.ok(/customizeLabel\(on\)/.test(block),"and neither does the block's Customize link");
 assert.ok(/const STATE_STYLE:Record<string,\{color:string;border:string;suffix:string\}>=\{/.test(widget),
  'every state has one declared appearance');
 for(const state of ['applied','pending','not_applied','unsupported'])
  assert.ok(new RegExp(`\\b${state}:\\{color:`).test(widget),`${state} must have one`);
 assert.ok(/applied:\{color:C\.green/.test(widget),'only the applied state is live');
 assert.ok(/not_applied:\{color:C\.amber[\s\S]{0,60}NOT applied/.test(widget),
  'and a filter the server did not apply says so on its own chip');
 assert.ok(/status\.state==='applied'\?'check':'alert-circle'/.test(widget),
  "the icon on a chip is decided by the server's answer too");
 assert.ok(/const off=notAppliedText\(statuses\)/.test(widget));
 assert.ok(/\{!!off&&<T style=\{\[metaText,\{color:C\.amber\}\]\}>\{off\}<\/T>\}/.test(widget),
  'a filter that did not run is a caveat on the data, so it is the amber on this panel');
});
ok(()=>{ // the builder never claims a filter the screener cannot take, and never filters rows in the browser
 const dialog=SRC('FilterDialog.tsx');
 assert.ok(/\{!!column\.noScreener&&<T/.test(dialog),'a column the screener has no parameter for says so');
 assert.ok(!/\.filter\(\s*\w+\s*=>\s*\w+\.(oi|premium_cr|volume|strike|days_to_expiry|volume_ratio|volume_to_oi)\b/
  .test(dialog),'the popup must never filter served rows in the browser');
 const widget=CODE('UnusualWidget.tsx');
 const items=/const items=useMemo\(([\s\S]*?)\n \},\[/.exec(widget);
 assert.ok(items,'the screener builds its rows in one place');
 assert.ok(!/\.filter\(/.test(items[1]),'and drops not one served row in the browser');
});


// --- IMPLIED VOLATILITY IS COMPUTED, AND SAYS SO EVERYWHERE IT APPEARS ---------------------------------------------
// The COMPUTED label is necessary and it is NOT sufficient. A reader also has to be able to find out what the
// number rests on, and the weakest of those inputs is the risk-free rate: a constant in the server's code, with
// no feed behind it. So this section pins the label AND the disclosure of the rate, its source, and its weight.
ok(()=>{ // the tag and the sentence exist, and the sentence says the thing that matters
 assert.equal(L.IV_COMPUTED_TAG,'COMPUTED');
 assert.ok(/COMPUTED here, not reported by the exchange/.test(L.IV_COMPUTED_TEXT));
 assert.ok(/pricing model/.test(L.IV_COMPUTED_TEXT));
 // the server's own sentence wins; ours is the floor, never the ceiling
 assert.equal(L.ivComputedText({computed_text:'Solved by our own model.'}),'Solved by our own model.');
 assert.equal(L.ivComputedText(null),L.IV_COMPUTED_TEXT);
 assert.equal(L.ivComputedText({computed_text:'   '}),L.IV_COMPUTED_TEXT);
 // and the server really does say it, and really does say the exchange did not
 assert.ok(/COMPUTED here, not reported by the exchange/.test(IV_PY),
  'derivatives.py must say so in its own words too');
 assert.ok(/'exchange_reported':False/.test(DERIV_PY),
  'and must flag that the exchange did not report it');
});
ok(()=>{ // a volatility reads the same whether it arrives as a fraction or as a percentage
 assert.equal(L.ivText(0.184),'18.4%');
 assert.equal(L.ivText(18.4),'18.4%');
 assert.equal(L.ivText(1.2),'120.0%');
 for(const v of MISSING)assert.equal(L.ivText(v),DASH,String(v));
 assert.equal(L.ivTagged(0.184),`18.4% ${L.IV_COMPUTED_TAG}`);
 assert.equal(L.ivTagged(null),DASH,'a dash is not dressed up as a computed number');
});
ok(()=>{ // THE RATE: named, sourced, and weighed - not buried
 const body={risk_free_rate:0.065,risk_free_rate_source:{kind:'code constant',
  where:'kanida_pilot/implied_vol.py RISK_FREE_RATE',live_feed:false,text:'The risk-free rate is a fixed constant.'}};
 assert.equal(L.ivRateText(body),'6.50% a year');
 assert.equal(L.ivRateText({risk_free_rate:6.5}),'6.50% a year','a percentage reads the same as a fraction');
 assert.equal(L.ivRateText({}),'');
 // the short form the reader meets ON SCREEN says the rate AND that nothing is feeding it
 const short=L.ivRateSourceShort(body);
 assert.ok(short.includes('6.50% a year'),short);
 assert.ok(/no live feed behind it/.test(short),'the fact that matters is said outright: '+short);
 assert.ok(/code constant/.test(short),short);
 // a rate that DID come from a feed says that instead, rather than the same sentence for both
 assert.ok(/from a live feed/.test(L.ivRateSourceShort({risk_free_rate:0.07,
  risk_free_rate_source:{kind:'T-bill',live_feed:true}})));
 // and only the constant is treated as a caveat
 assert.equal(L.ivRateIsAssumed(body),true);
 assert.equal(L.ivRateIsAssumed({risk_free_rate_source:{live_feed:true}}),false);
 assert.equal(L.ivRateIsAssumed(null),false);
 // the long form is the server's own sentence, with where it lives
 const long=L.ivRateSourceText(body);
 assert.ok(long.startsWith('The risk-free rate is a fixed constant.'),long);
 assert.ok(long.includes('kanida_pilot/implied_vol.py RISK_FREE_RATE'),long);
 assert.equal(L.ivRateSourceText(null),'');
 // the server really does serve both, and really does say there is no feed behind the rate
 assert.ok(/RISK_FREE_RATE_SOURCE={/.test(IV_PY)&&/'live_feed':False/.test(IV_PY),
  'implied_vol.py must declare where the rate came from');
});
ok(()=>{ // the WEIGHT of that assumption, which is what turns "trust the rate" into a number
 assert.ok(/would move this reading by 0\.12 percentage points down/.test(L.ivRateSensitivityText(-0.1233)),
  L.ivRateSensitivityText(-0.1233));
 assert.ok(/0\.50 percentage points up/.test(L.ivRateSensitivityText(0.5)));
 assert.ok(/would not move this reading at all/.test(L.ivRateSensitivityText(0.001)));
 assert.equal(L.ivRateSensitivityText(null),'');
 // read off the legs, largest first, because one figure for the block beats one per point
 const legs=[{points:[{rate_sensitivity_pct_points:-0.12},{rate_sensitivity_pct_points:-0.30}]},
  {points:[{rate_sensitivity_pct_points:0.05},{rate_sensitivity_pct_points:null}]}];
 assert.equal(L.ivRateSensitivity(legs),-0.30);
 assert.equal(L.ivRateSensitivity([]),null);
 assert.equal(L.ivRateSensitivity([null,{points:null}]),null);
});
ok(()=>{ // EVERY null carries its reason. The server sends a whole table of them, and its words always win.
 const served={stale_last_trade:'The last trade is older than one 15-minute reading.',
  missing_spot:'No spot price was captured for this underlying at this reading.'};
 assert.equal(L.ivPointReason({reason:'stale_last_trade'},served),served.stale_last_trade);
 assert.equal(L.ivPointReason({reason:'missing_spot'},served),served.missing_spot);
 // the point's own sentence beats even the table
 assert.equal(L.ivPointReason({reason:'missing_spot',reason_text:'Its own words.'},served),'Its own words.');
 // a code neither of them knows is said as itself rather than swallowed
 assert.ok(L.ivPointReason({reason:'brand_new_code'},served).includes('brand_new_code'));
 // no reason at all is empty, so a caller falls back rather than printing a lie
 assert.equal(L.ivPointReason({},served),'');
 assert.equal(L.ivPointReason(null,null),'');
 // and the five the contract named still have our own floor sentence
 for(const key of ['stale_trade','no_time_value','below_intrinsic','no_convergence','expiry_today'])
  assert.ok(/^No volatility:/.test(L.ivReasonText(key)),`${key} must be explained in words`);
});
ok(()=>{ // the LATEST refusal is what a dash on screen prints, so the dash is never unexplained
 const points=[{iv:0.18},{iv:null,reason:'stale_last_trade'},{iv:null,reason:'missing_spot'}];
 const served={stale_last_trade:'Stale.',missing_spot:'No spot.'};
 assert.equal(L.ivLatestRefusal(points,served),'No spot.','the last one, not the first');
 assert.equal(L.ivLatestRefusal([{iv:0.2}],served),'','a solved session has no refusal to print');
 assert.equal(L.ivLatestRefusal(null,null),'');
});
ok(()=>{ // every refusal in the session is counted and explained, in the server's own sentences
 const body={rejections:{missing_spot:2,stale_last_trade:5},
  reason_text:{missing_spot:'No spot price was captured.',stale_last_trade:'The last trade is too old.'}};
 const lines=L.ivRejectionLines(body);
 assert.equal(lines.length,2);
 assert.ok(lines[0].startsWith('5 readings: '),'commonest first: '+lines[0]);
 assert.ok(lines[0].includes('The last trade is too old.'));
 assert.ok(lines[1].startsWith('2 readings: '));
 assert.deepEqual([...L.ivRejectionLines({rejections:{}})],[]);
 assert.deepEqual([...L.ivRejectionLines(null)],[]);
});
ok(()=>{ // the model line names what solved it, how, and over what day count
 const text=L.ivMethodText({model:'Black-Scholes-Merton (European, no dividend)',
  method:'bisection on the option price, solved for the volatility',day_count:'ACT/365',expiry_time_ist:'15:30'});
 assert.ok(text.includes('Black-Scholes-Merton'),text);
 assert.ok(text.includes('bisection'),text);
 assert.ok(text.includes('ACT/365'),text);
 assert.equal(L.ivMethodText(null),'The server did not name the model these were solved with.');
});
ok(()=>{ // the MARKING, on screen, everywhere an implied volatility appears
 const blocks=SRC('SessionBlocks.tsx'),panel=SRC('SessionPanel.tsx'),frame=SRC('frame.tsx');
 assert.ok(/export function Tag\(\{label,a11y\}/.test(frame),'there is one component for the label');
 // 1. the block header
 assert.ok(/actions=\{<Tag label=\{IV_COMPUTED_TAG\}/.test(blocks),'the IV block header carries it');
 assert.ok(/subtitle="Implied volatility is COMPUTED by a model from captured prices/.test(blocks),
  'and the block subtitle says it in a sentence');
 // 2. the chart panel
 assert.ok(/tag=\{IV_COMPUTED_TAG\}[\s\S]{0,140}tagA11y="Computed by a pricing model, not reported by the exchange"/
  .test(blocks),'the chart panel carries it, and says it to a screen reader');
 assert.ok(/\{!!tag&&<Tag label=\{tag\} a11y=\{tagA11y\}\/>\}/.test(panel),'and the panel really draws it');
 // 3. the line's own legend entry
 assert.ok(/\{key:'iv',label:IV_ATM_LABEL,color:C\.mint,values:iv,format:ivText,tag:IV_COMPUTED_TAG\}/.test(blocks),
  'the legend entry beside the latest value carries it');
 assert.ok(/\{line\.tag\?` \$\{line\.tag\}`:''\}/.test(panel),'and the legend prints it');
 // 4. the legs list header AND every solved row
 assert.ok(/<T style=\{head\}>Implied vol<\/T><Tag label=\{IV_COMPUTED_TAG\}\/>/.test(blocks),
  'the legs list says it in its column header');
 assert.ok(/ivText\(last\.iv_pct\?\?last\.iv\)\}<\/T>\n\s*<Tag label=\{IV_COMPUTED_TAG\}\/>/
  .test(blocks.replace(/\r/g,'')),'and on every row that has a number');
 // 5. the readings panel's own line
 assert.ok(/tag:\(body\?\.latest_iv_pct\?\?body\?\.latest_iv\)==null\?undefined:IV_COMPUTED_TAG/.test(blocks),
  'the latest reading carries it, and a dash does not');
 assert.ok(/label:`\$\{IV_ATM_LABEL\} \(computed\)`/.test(blocks),'the label itself says it too');
 // and the tag is NOT amber: amber on this tab is a caveat on the data, and a model output is not a fault
 assert.ok(!/export function Tag[\s\S]{0,400}C\.amber/.test(frame),'the computed label is never amber');
});
ok(()=>{ // the RATE is on screen, not only behind the control
 const blocks=SRC('SessionBlocks.tsx');
 assert.ok(/const rateCaveat=ivRateIsAssumed\(body\)/.test(blocks),
  'a rate with nothing feeding it is treated as a caveat');
 assert.ok(/ivRateSourceShort\(body\)\}\s*\$\{ivRateSensitivityText\(shift\)\}/.test(blocks.replace(/\r/g,''))
  ||/ivRateSourceShort\(body\)/.test(blocks)&&/ivRateSensitivityText\(shift\)/.test(blocks),
  'and both the source and its weight are put in it');
 // it is spent as the AMBER on both IV panels, which is where a caveat on the data belongs
 assert.equal((blocks.match(/caveat=\{rateCaveat\}/g)||[]).length,2,
  'both IV panels carry it, and it is not quietly dropped from one');
 // the rate is also a row of its own in the numbers panel
 assert.ok(/label:'Risk-free rate used',value:ivRateText\(body\)/.test(blocks),
  'the rate is a reading in its own right, not a footnote');
 // and the full sentence, plus every other assumption, is behind the block's one control
 assert.ok(/heading:'What the rate rests on'/.test(blocks));
 assert.ok(/ivRateSourceText\(body\)/.test(blocks));
 assert.ok(/heading:'Every other assumption',lines:Object\.values\(body\?\.assumptions\|\|\{\}\)/.test(blocks),
  "the server's own assumptions are all carried, not a selection of them");
});
ok(()=>{ // a null is never a blank on a row, and never a drawn point on the line
 const blocks=CODE('SessionBlocks.tsx');
 assert.ok(/const refusal=last\?'':ivLatestRefusal\(leg\.points,reasons\)\|\|String\(leg\.missing_text\|\|''\)/
  .test(blocks),'a leg without a number works out its reason');
 assert.ok(/:<T numberOfLines=\{3\}[\s\S]{0,160}\{refusal\}<\/T>/.test(blocks),
  'and prints that reason where the number would have been');
 // the line itself: a null value is a null point, so the path breaks rather than joining through it
 const scaled=L.scaleValues([0.18,null,0.2,0.21],100,50);
 assert.equal(scaled.points[1],null,'a reading with no volatility is not a point');
 assert.ok(!L.linePath(scaled).includes('NaN'));
 assert.equal((L.linePath(scaled).match(/M/g)||[]).length,2,'the path breaks at the gap and starts again after it');
});

// --- GAPS: a reading the store has no row for is a hole, not a zero --------------------------------------------------
// Every series sits on the store's own reading grid, so a reading an underlying has no row for arrives as a SLOT:
// every value null, `gap:true`, and `withheld` naming the reason. One helper pulls values out of a series, so the
// rule that a gap is never reached past is fixed in one place for all four blocks.
ok(()=>{
 const points=[{at:'2026-09-18 09:30',pcr_oi:0.91,gap:false},
  {at:'2026-09-18 09:45',pcr_oi:null,gap:true,withheld:'no_reading'},
  {at:'2026-09-18 10:00',pcr_oi:1.02,gap:false}];
 assert.deepEqual([...L.seriesValues(points,p=>p.pcr_oi)],[0.91,null,1.02]);
 assert.deepEqual([...L.seriesTimes(points)],['2026-09-18 09:30','2026-09-18 09:45','2026-09-18 10:00'],
  'the hole keeps its place on the axis');
 // a gap slot is null even if a value somehow rode along on it
 assert.deepEqual([...L.seriesValues([{pcr_oi:9,gap:true}],p=>p.pcr_oi)],[null],
  'a slot flagged as a gap is a gap, whatever else is on it');
 // and the line drawn from it breaks
 const scaled=L.scaleValues(L.seriesValues(points,p=>p.pcr_oi),100,50);
 assert.equal((L.linePath(scaled).match(/M/g)||[]).length,2);
 assert.deepEqual([...L.seriesValues(null,p=>p)],[]);
});
ok(()=>{ // how much of the session carried a value, and why the rest did not - in the server's own sentences
 const body={total_readings:26,readings_with_value:9,
  withheld_reasons:{no_reading:17},
  reason_text:{no_reading:'This underlying has no row at this 15-min reading.'}};
 assert.equal(L.readingsText(body),'9 of 26 readings carried a value');
 assert.equal(L.readingsText({}),'','a tally we were not given is not invented');
 const lines=L.withheldLines(body);
 assert.deepEqual([...lines],['17 readings: This underlying has no row at this 15-min reading.']);
 const text=L.withheldText(body);
 assert.ok(text.includes('9 of 26'),text);
 assert.ok(text.includes('no row at this 15-min reading'),text);
 // a whole session says nothing at all rather than "0 missing"
 assert.equal(L.withheldText({total_readings:26,readings_with_value:26,withheld_reasons:{}}),'');
 assert.deepEqual([...L.withheldLines(null)],[]);
});
ok(()=>{ // every block reads its series through that one helper, and none of them reaches past a gap itself
 const blocks=CODE('SessionBlocks.tsx');
 // PCR two lines, max pain two, IV one, futures four: nine series, one helper that respects a gap
 assert.equal((blocks.match(/seriesValues\(points,/g)||[]).length,9,
  'every line on every block reads its series through that one helper');
 assert.ok(!/points\.map\(p=>p\./.test(blocks),'no block pulls a series out by hand');
 // a readings panel shows the server's `latest_*`, which is read off the last reading that HAD a value
 assert.ok(/\[\.\.\.points\]\.reverse\(\)\.find\(p=>p&&!p\.gap\)/.test(blocks),
  'and where a block does read the last reading itself, it skips the gaps');
});


// --- the direction words are the SERVER's, not this tab's ------------------------------------------------------------
// The owner gave exact words for two of these vocabularies. The server now serves all of them, per series, as
// `direction_words` (which key means up/down/flat/none) and `direction_labels` (the reader's word for each key).
// So the tab hardcodes none of them - and the words the owner asked for are checked against the SERVER, which is
// the only place they can now go wrong.
const WORDS=(up,down,flat)=>({up,down,flat,none:'no baseline'});
ok(()=>{ // the owner's exact words, checked where they actually live now
 const labels=/^DIRECTION_LABELS=\{([\s\S]*?)\}$/m.exec(DERIV_PY);
 assert.ok(labels,'derivatives.py must declare DIRECTION_LABELS');
 for(const [key,word] of [['shifting_up','Shifting Up'],['stable','Stable'],['shifting_down','Shifting Down'],
  ['expanding','Expanding'],['cooling','Cooling'],['building','Building'],['unwinding','Unwinding']])
  assert.ok(labels[1].includes(`'${key}':'${word}'`),
   `the owner's word for ${key} is "${word}", and the server must serve exactly that`);
});
ok(()=>{ // the chip is the arrow this tab uses over the SERVER's word, upper-cased
 const words=WORDS('shifting_up','shifting_down','stable');
 const labels={shifting_up:'Shifting Up',shifting_down:'Shifting Down',stable:'Stable'};
 assert.equal(L.servedChip('shifting_up',words,labels),'↑ SHIFTING UP');
 assert.equal(L.servedChip('shifting_down',words,labels),'↓ SHIFTING DOWN');
 assert.equal(L.servedChip('stable',words,labels),'→ STABLE');
 // the other three vocabularies, all through the same one function
 assert.equal(L.servedChip('expanding',WORDS('expanding','cooling','stable'),{expanding:'Expanding'}),'↑ EXPANDING');
 assert.equal(L.servedChip('cooling',WORDS('expanding','cooling','stable'),{cooling:'Cooling'}),'↓ COOLING');
 assert.equal(L.servedChip('building',WORDS('building','unwinding','flat'),{building:'Building'}),'↑ BUILDING');
 assert.equal(L.servedChip('widening',WORDS('widening','narrowing','flat'),{widening:'Widening'}),'↑ WIDENING');
 assert.equal(L.servedChip('rising',WORDS('rising','falling','flat'),{rising:'Rising'}),'↑ RISING');
});
ok(()=>{ // no baseline, and anything we were not given a word for, get NO chip rather than a guessed one
 const words=WORDS('shifting_up','shifting_down','stable');
 assert.equal(L.servedChip('no baseline',words,{}),'');
 assert.equal(L.servedChip(null,words,{}),'');
 assert.equal(L.servedChip('',words,{}),'');
 // a key with no served label still gets its own key rather than nothing, because a served direction is real
 assert.equal(L.servedChip('shifting_up',words,null),'↑ SHIFTING UP');
 assert.equal(L.servedChip('some_new_word',words,null),'SOME NEW WORD',
  'a word we do not have an arrow for is still the word, not a guess at its direction');
});
ok(()=>{ // colour repeats the served word and never adds to it
 const words=WORDS('shifting_up','shifting_down','stable');
 assert.equal(L.servedTone('shifting_up',words),'up');
 assert.equal(L.servedTone('shifting_down',words),'down');
 assert.equal(L.servedTone('stable',words),'flat');
 assert.equal(L.servedTone('no baseline',words),'flat','no baseline is never coloured as a direction');
 assert.equal(L.servedTone('shifting_up',null),'flat','with no vocabulary, nothing is coloured');
 assert.equal(L.servedTone(null,words),'flat');
});
ok(()=>{ // and NO block hardcodes a direction word of its own
 const blocks=CODE('SessionBlocks.tsx');
 for(const word of ['SHIFTING UP','SHIFTING DOWN','EXPANDING','COOLING','BUILDING','UNWINDING','RISING','FALLING'])
  assert.ok(!blocks.includes(word),`"${word}" must come from the server, not from SessionBlocks.tsx`);
 // every chip on every block goes through the one pair of helpers
 assert.equal((blocks.match(/servedChip\(/g)||[]).length,6,
  'PCR two, max pain one, futures two, IV one - every chip on the tab');
 assert.equal((blocks.match(/servedTone\(/g)||[]).length,6);
 assert.ok(/direction_words/.test(blocks)&&/direction_labels/.test(blocks),
  'and both are read off the response');
 // the rule behind a direction is the server's sentence too
 assert.equal(L.directionRule({direction_text:'Direction is the latest reading against the reading 4 back.'}),
  'Direction is the latest reading against the reading 4 back.');
 assert.equal(L.directionRule(null),'');
 assert.equal((blocks.match(/directionRule\(body\)/g)||[]).length,4,'each block prints it once');
});
ok(()=>{ // the tab's OWN reading of a series is still there as the fallback, and still says no baseline
 assert.equal(L.sessionDirection([],L.MAX_PAIN_KEYS),L.NO_SESSION_DIRECTION);
 assert.equal(L.sessionDirection([23000],L.MAX_PAIN_KEYS),L.NO_SESSION_DIRECTION);
 assert.equal(L.sessionDirection([null,null],L.MAX_PAIN_KEYS),L.NO_SESSION_DIRECTION);
 assert.equal(L.sessionDirection([23000,23500],L.MAX_PAIN_KEYS),'shifting_up');
 assert.equal(L.sessionDirection([23500,23000],L.MAX_PAIN_KEYS),'shifting_down');
 assert.equal(L.sessionDirection([23000,23000],L.MAX_PAIN_KEYS),'stable');
 assert.equal(L.sessionDirection([23000,null,23500],L.MAX_PAIN_KEYS),'shifting_up','a gap is skipped, never read as zero');
 assert.equal(L.sessionDirection([23000,23800,23000,23010],L.MAX_PAIN_KEYS),'stable');
 assert.equal(L.sessionDirection([23000,23800,23000,23400],L.MAX_PAIN_KEYS),'shifting_up');
});
ok(()=>{ // the SERVER's word wins when it sends one, because it saw every reading
 assert.equal(L.servedDirection('cooling',[0.2,0.3],L.IV_KEYS),'cooling');
 assert.equal(L.servedDirection('',[0.2,0.3],L.IV_KEYS),'expanding','with no word from the server we read the points');
 assert.equal(L.servedDirection('nonsense',[0.2,0.3],L.IV_KEYS),'expanding','and a word we do not know is not a word');
});

// --- max pain, PCR and futures build-up: the numbers, in words ------------------------------------------------------
ok(()=>{ // THE SIGN. Distance is strike MINUS spot, so positive means the strike sits above spot - and the
 // wording names its SUBJECT, so it cannot be read the other way round whichever sign it carries.
 assert.equal(L.maxPainDistanceText(820),'strike 820 above spot');
 assert.equal(L.maxPainDistanceText(-301),'strike 301 below spot');
 assert.equal(L.maxPainDistanceText(0),'strike at spot');
 for(const v of MISSING)assert.equal(L.maxPainDistanceText(v),DASH,String(v));
 // the convention is the SERVER's sentence, printed and never paraphrased
 assert.ok(/MINUS spot/.test(L.maxPainDistanceRule({distance_definition:'Distance is the max-pain strike MINUS spot.'})));
 assert.equal(L.maxPainDistanceRule(null),'');
 // and the server really does use that convention
 assert.ok(/MAX_PAIN_DISTANCE_DEFINITION/.test(DERIV_PY),'derivatives.py must declare the convention');
 const rule=/^MAX_PAIN_DISTANCE_DEFINITION=\(?'([\s\S]*?)'\)?$/m.exec(DERIV_PY);
 if(rule)assert.ok(/MINUS spot/i.test(rule[1])||/minus spot/i.test(rule[1]),
  `the served convention must be strike minus spot: ${rule[1]}`);
 // the panel label names the arithmetic too, so the row cannot be read backwards either
 assert.ok(SRC('SessionBlocks.tsx').includes("label:'Distance (strike − spot)'"),
  'the readings row names the subtraction');
});
ok(()=>{ // the max-pain line, with a spot the store did not capture
 const whole={latest_max_pain_strike:23300,latest_spot:23346.4,latest_distance:-46.4,latest_total_oi:6150000};
 const text=L.maxPainLine(whole);
 assert.ok(text.includes('Max pain 23,300'),text);
 assert.ok(text.includes('strike 46 below spot'),text);
 assert.ok(text.includes('61.5L contracts'),text);
 // NIFTY's own latest reading has a strike and NO spot. A distance IS a strike measured against a spot, so
 // with no spot there is no distance to dash out - the sentence says that once rather than printing two
 // dashes in a row and leaving the reader to work out which of them was the cause of the other.
 const noSpot=L.maxPainLine({latest_max_pain_strike:23350,latest_spot:null,latest_distance:null,
  latest_total_oi:327987465});
 assert.ok(noSpot.includes('no spot was captured at this reading'),noSpot);
 assert.ok(noSpot.includes('so there is no distance to it'),noSpot);
 assert.ok(!noSpot.includes(DASH),'and no bare dash is left standing on its own: '+noSpot);
 // the strike is still named, because the store did capture that
 assert.ok(noSpot.includes('Max pain 23,350'),noSpot);
 assert.ok(L.maxPainLine({latest_total_oi:null}).includes('Total OI behind it: not captured'));
 assert.equal(L.maxPainLine(null),'');
});
ok(()=>{
 assert.equal(L.pcrLine({latest_pcr_oi:1.1839,latest_pcr_volume:1.0037}),
  'PCR by open interest 1.18 · PCR by volume 1.00.');
 assert.ok(L.pcrLine({latest_pcr_oi:null,latest_pcr_volume:null}).includes(DASH),
  'a ratio that is not there is a dash, never a zero');
 assert.equal(L.pcrLine(null),'');
});
ok(()=>{
 assert.equal(L.basisPair(30.3,0.13),`+${RUPEE}30.30 (+0.13%)`);
 assert.equal(L.basisPair(-12.5,-0.05),`${MINUS}${RUPEE}12.50 (${MINUS}0.05%)`);
 assert.equal(L.basisPair(30,null),`+${RUPEE}30.00`,'half a pair is still the half we have');
 assert.equal(L.basisPair(null,0.12),'+0.12%');
 assert.equal(L.basisPair(null,null),DASH);
});
ok(()=>{
 assert.equal(L.oiVsAvgText(1.010862),`1.01${TIMES} its 20-day average`);
 assert.equal(L.oiVsAvgText(1.24,20),`1.24${TIMES} its 20-day average, from 20 sessions`);
 assert.equal(L.oiVsAvgText(null),'no baseline','§3.7 without a baseline is words, never a ratio');
});
ok(()=>{ // the futures line, off the server's own latest_* figures
 const text=L.futuresLine({latest_oi_vs_avg:0.965582,latest_basis:3.7,latest_basis_pct:0.2981,
  latest_buildup_day:'Long unwinding'});
 assert.ok(text.includes(`0.97${TIMES} its 20-day average`),text);
 assert.ok(text.includes('Long unwinding on the day'),text);
 assert.equal(L.futuresLine(null),'');
 // the build-up label is the SERVER's, already written for the reader - and "no data" is a state, not a label
 assert.equal(L.servedBuildup('Long build-up'),'Long build-up');
 assert.equal(L.servedBuildup('Short covering'),'Short covering');
 assert.equal(L.servedBuildup('no data'),DASH,'"no data" is a dash, not a label read out to the reader');
 assert.equal(L.servedBuildup(null),DASH);
 assert.equal(L.servedBuildup(''),DASH);
});
ok(()=>{ // the screener's coverage line: the difference between a quiet market and a thin reading
 const body={scanned:27671,total:491,returned:100,coverage:{at:'x',rows:27671,underlyings:216}};
 const text=L.coverageText(body);
 assert.ok(text.includes('27,671 rows'),text);
 assert.ok(text.includes('216 underlyings'),text);
 assert.ok(text.includes('491 cleared the floors'),text);
 assert.ok(text.includes('carries 100 of them'),text);
 // the reading that passes none of them says exactly that, which is NOT the same as an empty market
 const none=L.coverageText({scanned:255,total:0,returned:0,coverage:{underlyings:216}});
 assert.ok(none.includes('255 rows'),none);
 assert.ok(none.includes('0 cleared the floors'),none);
 assert.equal(L.coverageText(null),'');
 assert.equal(L.coverageText({}),'');
});
ok(()=>{ // the readings the store holds are choices, and each says how wide it was
 const choices=L.readingChoices({readings:[{at:'2026-09-18 15:45:00',underlyings:216},
  {at:'2026-09-18 11:30:00',underlyings:216}]});
 assert.equal(choices.length,2);
 assert.equal(choices[0].value,'2026-09-18 15:45:00');
 assert.equal(choices[0].label,'18 Sep 2026 · 15:45');
 assert.equal(choices[0].detail,'216 underlyings covered');
 assert.deepEqual([...L.readingChoices(null)],[]);
 assert.deepEqual([...L.readingChoices({readings:[{at:''}]})],[],'a reading with no stamp is not a choice');
});


// --- AXIS LABELS: distinct, parseable, and strictly increasing -------------------------------------------------------
// A chart shipped here once with its time labels running backwards. Both axes are built in ONE place now
// (logic.valueAxis / logic.valueAxisLabels / logic.sessionAxisTimes) so this is the one place they are held to it.
ok(()=>{ // the value axis runs UP the box: high first in the list, y increasing down the screen
 const scaled=L.scaleValues([0.80,0.88,0.94,1.02],200,100);
 const axis=L.valueAxis(scaled,100,3);
 assert.ok(axis.length>=2,'an axis needs at least two labels to be one');
 for(let i=1;i<axis.length;i++){
  assert.ok(axis[i].v<axis[i-1].v,`values must run strictly downward through the list: ${JSON.stringify(axis)}`);
  assert.ok(axis[i].y>axis[i-1].y,`and strictly downward the screen, so the axis runs UP the box: ${JSON.stringify(axis)}`);
 }
 assert.equal(axis[0].v,scaled.hi,'the top label is the high');
 assert.equal(axis[axis.length-1].v,scaled.lo,'and the bottom label is the low');
});
ok(()=>{ // the same holds for a series that only falls, and for one on both sides of zero
 for(const values of [[1.02,0.94,0.88,0.80],[-30,-10,0,12,30],[23346.4,23350.1,23299.9]]){
  const axis=L.valueAxis(L.scaleValues(values,200,100),100,3);
  for(let i=1;i<axis.length;i++){
   assert.ok(axis[i].v<axis[i-1].v,`backwards value axis for ${JSON.stringify(values)}`);
   assert.ok(axis[i].y>axis[i-1].y,`backwards screen axis for ${JSON.stringify(values)}`);
  }
 }
});
ok(()=>{ // a label that would sit on the one before it is dropped, never stacked
 const scaled=L.scaleValues([1,1.0000001],200,100);
 const tight=L.valueAxis(scaled,8,3);
 for(let i=1;i<tight.length;i++)assert.ok(Math.abs(tight[i].y-tight[i-1].y)>=11,'two labels on the same pixels');
 // nothing to scale is no axis at all, rather than an invented one
 assert.deepEqual([...L.valueAxis(null,100)],[]);
 assert.deepEqual([...L.valueAxis(L.scaleValues([1],200,100),100)],[]);
 assert.deepEqual([...L.valueAxis(L.scaleValues([1,2],200,100),0)],[]);
});
ok(()=>{ // and two ticks that would PRINT the same thing are one tick: a repeated label is not an axis
 const labels=L.valueAxisLabels(L.scaleValues([0.8801,0.8802,0.8803],200,100),100,v=>L.pcrText(v),3);
 const seen=labels.map(t=>t.label);
 assert.equal(new Set(seen).size,seen.length,`a label is printed once: ${seen.join(' | ')}`);
 for(let i=1;i<labels.length;i++)assert.ok(labels[i].v<labels[i-1].v,'and the order still runs downward');
 // a value that cannot be written is not drawn as a dash on the axis
 assert.deepEqual([...L.valueAxisLabels(L.scaleValues([1,2],200,100),100,()=>DASH,3)],[]);
});
ok(()=>{ // the time axis: distinct, parseable, and strictly LATER left to right
 const times=['2026-09-18 09:30','2026-09-18 09:45','2026-09-18 10:00','2026-09-18 10:15','2026-09-18 10:30',
  '2026-09-18 10:45','2026-09-18 11:00','2026-09-18 11:15'];
 const ticks=L.sessionAxisTimes(times,4,600);
 assert.ok(ticks.length>=2);
 for(let i=1;i<ticks.length;i++){
  assert.ok(ticks[i].i>ticks[i-1].i,'a later label must be further right');
  assert.ok(ticks[i].label>ticks[i-1].label,`clock labels must increase: ${ticks.map(t=>t.label).join(' ')}`);
 }
 assert.ok(ticks.every(t=>/^\d{2}:\d{2}$/.test(t.label)),'and every one of them is a readable clock');
});
ok(()=>{ // a stamp that does not advance is DROPPED - which is what a series crossing a day does to a clock axis
 const crossing=['2026-09-17 13:15','2026-09-17 15:30','2026-09-18 09:30','2026-09-18 11:15'];
 const ticks=L.sessionAxisTimes(crossing,4,600);
 for(let i=1;i<ticks.length;i++)assert.ok(ticks[i].label>ticks[i-1].label,
  `an axis may never run backwards: ${ticks.map(t=>t.label).join(' ')}`);
 // the second session's 09:30 is earlier on the clock than 15:30, so it is not printed
 assert.ok(!ticks.some(t=>t.label==='09:30'),'a clock that goes backwards is not a label');
});
ok(()=>{ // two readings that print the same clock are printed once
 const repeated=['2026-09-17 09:30','2026-09-18 09:30','2026-09-18 09:45'];
 const ticks=L.sessionAxisTimes(repeated,4,600);
 const seen=ticks.map(t=>t.label);
 assert.equal(new Set(seen).size,seen.length,`a clock is printed once: ${seen.join(' | ')}`);
});
ok(()=>{ // a stamp that cannot be read is not a label, and nothing at all is no axis
 assert.deepEqual([...L.sessionAxisTimes([null,'nonsense','18/09/2026'],4,600)],[]);
 assert.deepEqual([...L.sessionAxisTimes([],4,600)],[]);
 assert.deepEqual([...L.sessionAxisTimes(null,4,600)],[]);
 // a date with no time is not a clock label either
 assert.deepEqual([...L.sessionAxisTimes(['2026-09-18','2026-09-19'],4,600)],[]);
});
ok(()=>{ // the panel really uses those two, and builds neither of its own
 const panel=CODE('SessionPanel.tsx');
 assert.ok(/valueAxisLabels\(axisOf,plotH,axisFormat,3\)/.test(panel),'the y labels come from the one builder');
 assert.ok(/sessionAxisTimes\(times,4,plotW\)/.test(panel),'and so do the x labels');
 assert.ok(!/\.toFixed\(/.test(panel),'the panel formats nothing of its own');
 assert.ok(!/new Date\(/.test(panel),'and re-zones nothing');
 // one chart component for all four blocks: a second copy is a second place for the bug to come back
 assert.equal((CODE('SessionBlocks.tsx').match(/<Svg/g)||[]).length,0,'the blocks draw no SVG of their own');
 assert.ok(/<SessionPanel /.test(SRC('SessionBlocks.tsx')),'they all use the one panel');
});
ok(()=>{ // two series share a scale only when they are the same unit and are read AGAINST each other
 const blocks=SRC('SessionBlocks.tsx');
 // Two of them qualify, and only two. Max pain against spot: both rupee levels of the same underlying, and
 // the panel exists to show the distance between them. PCR by open interest against PCR by volume: both
 // put/call ratios, both read against each other and against 1.0 - on two scales the one visible axis would
 // belong to one line while the other floated free of it, which is the opposite of what the panel is for.
 assert.equal((blocks.match(/\bshared\b/g)||[]).length,2,'exactly two blocks share a scale');
 assert.ok(/name="Max pain against spot"[\s\S]{0,300}\bshared\b/.test(blocks),
  'max pain against spot - both rupee levels of the same underlying');
 assert.ok(/name="PCR through the session"[\s\S]{0,300}\bshared\b/.test(blocks),
  'and PCR against PCR - both ratios of the same chain');
 // everything measured in a DIFFERENT unit keeps its own scale, as the ΔOI tiles already do
 assert.ok(!/name="Futures open interest"[\s\S]{0,300}\bshared\b/.test(blocks),
  'open interest and a 20-day share are different units and never share an axis');
 assert.ok(!/name="Basis through the session"[\s\S]{0,300}\bshared\b/.test(blocks),
  'nor do rupees of basis and a percentage of spot');
 const pair=L.scaleTogether([23300,23350],[23346,23352],200,100);
 assert.equal(pair.a.lo,pair.b.lo);assert.equal(pair.a.hi,pair.b.hi);
 // and a gap on either line is still a gap
 const gappy=L.scaleTogether([23300,null,23350],[23346,23348,null],200,100);
 assert.equal(gappy.a.points[1],null);assert.equal(gappy.b.points[2],null);
 assert.equal(L.scaleTogether([],[],200,100),null);
});
ok(()=>{ // a reading with no value is never drawn, and nothing is carried forward into it
 const scaled=L.scaleValues([1,null,3],200,100);
 assert.equal(scaled.points[1],null);
 assert.equal(L.scaleValues([1],200,100),null,'one point is not a line');
 assert.equal(L.scaleValues([1,2],0,100),null);
 assert.equal(L.scaleValues(null,200,100),null);
 // zero is only forced onto the axis when a caller asks for it
 assert.equal(L.scaleValues([5,6],200,100).lo,5);
 assert.equal(L.scaleValues([5,6],200,100,true).lo,0);
});
ok(()=>{ // what a screen reader hears about a session line describes the captured series and stops
 const spoken=L.sessionSpoken('PCR by open interest',[0.88,null,0.9],['2026-09-18 09:30',null,'2026-09-18 10:00']);
 assert.ok(spoken.includes('2 captured readings'),spoken);
 assert.ok(spoken.includes('09:30 to 10:00'),spoken);
 assert.ok(L.sessionSpoken('PCR by open interest',[],[]).includes('Nothing captured yet'));
});

// --- ONE SYMBOL DRIVES EVERY BLOCK ON THE TAB -------------------------------------------------------------------------
ok(()=>{ // the owner's order: Customize wins, then the row last clicked, then the two defaults
 assert.deepEqual({...L.resolveTabSymbol('RELIANCE','TCS','INFY','NIFTY')},
  {symbol:'RELIANCE',defaulted:false,label:''},'a symbol chosen in Customize always wins');
 assert.deepEqual({...L.resolveTabSymbol('','TCS','INFY','NIFTY')},
  {symbol:'TCS',defaulted:false,label:''},'then the row the reader clicked - and that is a choice, not a default');
 assert.deepEqual({...L.resolveTabSymbol('','','INFY','NIFTY')},
  {symbol:'INFY',defaulted:true,label:L.BLOCK_DEFAULT_PREMIUM});
 assert.deepEqual({...L.resolveTabSymbol('','','','NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX});
 assert.deepEqual({...L.resolveTabSymbol('','','','')},{symbol:'',defaulted:false,label:''});
 assert.deepEqual({...L.resolveTabSymbol()},{symbol:'',defaulted:false,label:''});
});
ok(()=>{ // a defaulted symbol still SAYS it is a default, and a chosen one is just itself
 assert.equal(L.symbolBadge({symbol:'NIFTY',defaulted:true,label:'Default'}),'Default: NIFTY');
 assert.equal(L.symbolBadge({symbol:'INFY',defaulted:true,label:L.BLOCK_DEFAULT_PREMIUM}),'Busiest by premium: INFY');
 assert.equal(L.symbolBadge({symbol:'RELIANCE',defaulted:false,label:''}),'RELIANCE');
 assert.equal(L.symbolBadge({symbol:'',defaulted:false,label:''}),'');
 assert.equal(L.symbolBadge(null),'');
});
ok(()=>{ // the PAGE resolves it once, and every block on the page is handed that one answer
 const page=CODE('index.tsx');
 assert.equal((page.match(/resolveTabSymbol\(/g)||[]).length,1,'the tab resolves its symbol exactly once');
 assert.ok(/const symbol=choice\.symbol,badge=symbolBadge\(choice\)/.test(page),'and keeps it in one place');
 // every block that takes a symbol is given THAT one, by name - never its own lookup
 assert.ok(/<ChainWidget underlying=\{symbol\}/.test(page),'the option chain');
 assert.ok(/<OiByStrikeSection underlying=\{symbol\}/.test(page),'the strikes');
 assert.ok(/<OiGridSection symbol=\{symbol\}\n?\s*badge=\{badge\}/.test(page.replace(/\r/g,'')),'the ΔOI block');
 assert.ok(/const sessionProps=\{symbol,choice,badge,/.test(page),'and the four session blocks, through one object');
 for(const tag of ['<PcrSection {...sessionProps}/>','<MaxPainSection {...sessionProps}/>',
  '<IvSection {...sessionProps}/>','<FuturesBuildupSection {...sessionProps} '])
  assert.ok(page.includes(tag),`${tag} must be given the tab's symbol`);
 assert.ok(/badge=\{badge\}/.test(SRC('ScreenerSection.tsx')),'and the screener block names it too');
});
ok(()=>{ // NO block resolves a symbol of its own, by any route
 for(const file of ['OiGridSection.tsx','FuturesChartPanel.tsx','SessionBlocks.tsx','ScreenerSection.tsx',
  'SessionPanel.tsx','UnusualWidget.tsx']){
  const code=CODE(file);
  assert.ok(!/resolveTabSymbol|resolveBlockSymbol/.test(code),`${file} must not resolve its own symbol`);
  assert.ok(!/derivatives\/indices/.test(code),`${file} must not read the tab's fallback list for itself`);
 }
 // the session blocks make exactly one read each: their own series
 const blocks=CODE('SessionBlocks.tsx');
 assert.equal((blocks.match(/useDerivativeRead</g)||[]).length,4,
  'one read per session block - PCR, max pain, IV, futures build-up, and no fifth');
 // the routes the SERVER serves, spelled the way it spells them: `maxpain-series` is one word, and a tab
 // that guessed `max-pain-series` would read as an endpoint that does not exist rather than as a typo.
 for(const route of ['pcr-series','maxpain-series','iv-series','futures-buildup']){
  assert.ok(blocks.includes('api/derivatives/'+route+'?underlying=${encodeURIComponent(symbol)}'),
   route+" must be read for the tab's symbol");
  assert.ok(APP.includes("@app.get('/api/derivatives/"+route+"')"),
   route+' must be a route the pilot actually serves');
 }
});
ok(()=>{ // clicking a row ANYWHERE re-points the whole tab: every list writes the same one target
 const page=CODE('index.tsx');
 assert.ok(/const \[target,setTarget\]=useState<ChartTarget\|null>\(null\)/.test(page),'one target for the tab');
 assert.ok(/const chosen=filters\.underlying\|\|target\?\.underlying\|\|''/.test(page),
  'and the symbol follows it, so a row click re-points every block');
 for(const file of ['UnusualWidget.tsx','SessionBlocks.tsx','OiGridSection.tsx','IndexWidget.tsx'])
  assert.ok(/onTarget\(/.test(CODE(file)),`${file} must write the tab's one target rather than keep its own`);
 // the IV strike list re-points it too, so a strike is a symbol choice like any other row
 assert.ok(SRC('SessionBlocks.tsx').includes("onPick={onTarget?(leg,last)=>onTarget({underlying:body?.underlying||symbol,"),
  'a leg of the at-the-money strike points the tab at its underlying');
});

// --- the block rhythm: one as-of, one "How to read this", and no chart-sized voids --------------------------------------
ok(()=>{
 const blocks=SRC('SessionBlocks.tsx'),screener=SRC('ScreenerSection.tsx');
 // one as-of per block, in the Section header, never once per panel
 assert.equal((blocks.match(/asOf=\{asOfText\(body\?\.as_of\)\}/g)||[]).length,4,'each block prints its as-of once');
 assert.equal((blocks.match(/asOfText\(/g)||[]).length,4,'and only there');
 assert.equal((screener.match(/asOfText\(/g)||[]).length,1,'the screener block too');
 // every panel of every block declares it lives in one, so the frame drops the repeated footer
 assert.equal((SRC('SessionPanel.tsx').match(/\binBlock\b/g)||[]).length,2,
  'both panel shapes sit inside a block');
 // one "How to read this" per block, and the definitions live behind it rather than on the surface
 assert.equal((blocks.match(/<InfoDisclosure /g)||[]).length,4);
 assert.equal((screener.match(/<InfoDisclosure /g)||[]).length,1);
 assert.equal((blocks.match(/<Section /g)||[]).length,4,'four blocks, four sections');
});
ok(()=>{ // a panel showing ONE SENTENCE takes one sentence's height, not a chart's
 const blocks=SRC('SessionBlocks.tsx'),screener=SRC('ScreenerSection.tsx');
 const idle=/export const IDLE_H=(\d+);/.exec(SRC('frame.tsx'));
 assert.ok(idle,'frame.tsx declares that height once');
 assert.ok(/export function oneSentence\(phase:string\)\{return phase==='error';\}/.test(blocks),
  'an unavailable endpoint is one sentence');
 assert.ok(/export function shrink\(symbol:string,phase:string\)\{return !symbol\|\|oneSentence\(phase\);\}/.test(blocks),
  'and so is no symbol at all');
 assert.equal((blocks.match(/const idle=shrink\(symbol,read\.phase\)/g)||[]).length,4,
  'all four blocks collapse on the same rule');
 assert.ok(/idle\?\(stacked\?\{height:IDLE_H\}:\{flex,minWidth:0,height:IDLE_H\}\)/.test(blocks),
  'to frame.tsx\'s one number, at both layouts');
 assert.ok(/const idle=oneSentence\(read\.phase\)/.test(screener)&&/const tall=idle\?IDLE_H:height/.test(screener),
  'and so does the screener block');
 // both panels of a block collapse TOGETHER, so the row stays square
 assert.ok(/const tile=chart\?chart\(idle\):null/.test(screener),
  'the chart beside the screener shrinks with the table rather than standing tall beside it');
 // a reading that returned nothing is NOT this case and keeps its height: rows will be there at the next one
 assert.ok(!/phase==='empty'/.test(blocks),'an empty READING is not an empty panel and keeps its full height');
 const row=/const SESSION_ROW_H=(\d+),SESSION_STACK_H=(\d+);/.exec(SRC('index.tsx'));
 assert.ok(row&&Number(idle[1])<Number(row[1])/1.5,'and IDLE_H is well under the height a block takes with data');
});
ok(()=>{ // no title on a new panel is cut short at the widths the tab draws these blocks at
 const frame=SRC('frame.tsx');
 const bar=/const btn=inBlock\?(\d+):\d+,pad=inBlock\?(\d+):\d+,gap=inBlock\?(\d+):\d+/.exec(frame);
 assert.ok(bar,'the in-block header geometry is declared in one place');
 const btn=Number(bar[1]),pad=Number(bar[2]),gap=Number(bar[3]);
 const chrome=2+pad+2+gap*4+4+btn*3;
 const TITLE_PX=8.0;
 const page=SRC('index.tsx'),blocks=SRC('SessionBlocks.tsx');
 const beside=Number(/export const CHART_BESIDE=(\d+)/.exec(page)[1]);
 const three=Number(/export const GRID_BESIDE=(\d+)/.exec(page)[1]);
 // Two-panel blocks (PCR, max pain, IV) split 2.2 : 1 and sit side by side from CHART_BESIDE. The futures
 // block has THREE panels, so - like the ΔOI block - it stacks until GRID_BESIDE rather than squeezing them.
 assert.ok(/<FuturesBuildupSection \{\.\.\.sessionProps\} stacked=\{stacked\|\|page<GRID_BESIDE\|\|!!expanded\}\/>/
  .test(page),'the three-panel block waits for the room three panels need');
 const box=(width,share,total)=>(width-40-24)*share/total-chrome;
 const pairChart=box(beside,2.2,3.2),pairSide=box(beside,1,3.2);
 const trioChart=box(three,1.4,3.8),trioSide=box(three,1,3.8);
 // which panel each title belongs to, read off the file in the order the panels are built
 const titles=[...blocks.matchAll(/name="([^"]+)"/g)].map(m=>m[1]);
 assert.ok(titles.length>=8,'every session panel has a title');
 const room=(text)=>/^(Latest|Volatility by)/.test(text)
  ?(/futures/i.test(text)?trioSide:pairSide)
  :(/^(Futures open interest|Basis through)/.test(text)?trioChart:pairChart);
 for(const text of titles)assert.ok(text.length*TITLE_PX<=room(text),
  `side by side, that panel gives its title ${Math.round(room(text))}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 // stacked on a 360px phone every one of them still fits
 const phone=360-24-chrome;
 for(const text of [...titles,'Screener'])assert.ok(text.length*TITLE_PX<=phone,
  `stacked on a 360px phone a title has ${Math.round(phone)}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 // and nothing is clamped away to make it fit: the titles are short, the layout is not squeezed
 assert.ok(!/numberOfLines=\{1\}[^\n]*\{name\}/.test(SRC('SessionPanel.tsx')),
  'the panel does not clamp its own name - WidgetFrame owns that, once');
});

// --- §5 OVER EVERY STRING THE FIVE BLOCKS ADDED -------------------------------------------------------------------------
// The strictest sweep on this tab, over every new file, in single, double AND template quotes, and over every
// sentence the new logic can produce whatever the data does.
ok(()=>{
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|target|support level|resistance level|breakout|momentum|overbought|oversold|bullish|bearish|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i;
 for(const file of [...SESSION_FILES,'frame.tsx','FilterDialog.tsx','index.tsx']){
  const said=VISIBLE(SRC(file)).filter(t=>banned.test(t)&&!/neither is a forecast/.test(t)
   &&!/nothing on it is a forecast/.test(t));
  assert.deepEqual(said,[],`${file}: a string the reader can reach must not predict: ${said.join(' | ')}`);
  // Text written straight into JSX is never quoted at all, so the lines are swept too. Comments explain the
  // rules and are exempt on both sides — the `//` kind CODE() already drops, and the `/** */` kind here.
  const loose=CODE(file).split('\n')
   .filter(line=>!/^\s*(\*|\/\*)/.test(line))
   .filter(line=>banned.test(line)&&!/forecast/.test(line)
   &&!/ChartTarget|onTarget|setTarget|target\?\.|target\.|\btarget:|\{target\}|target=\{/.test(line));
  assert.deepEqual(loose,[],`${file}: nothing on screen predicts: ${loose.join(' | ')}`);
 }
});
ok(()=>{ // every sentence the new logic can produce, whatever the data does
 const sentences=[L.PCR_DEFINITION,L.PCR_READING_TEXT,L.PCR_NO_POINTS,L.PCR_OI_LABEL,L.PCR_VOLUME_LABEL,
  L.MAX_PAIN_DEFINITION,L.MAX_PAIN_GAP_TEXT,L.MAX_PAIN_READING_TEXT,L.MAX_PAIN_NO_POINTS,L.maxPainDistanceText(-301),
  L.IV_COMPUTED_TEXT,L.IV_ATM_LABEL,L.IV_DEFINITION,L.IV_READING_TEXT,L.IV_NO_POINTS,
  ...Object.values(L.IV_REASONS),L.ivReasonText('anything_new'),L.ivMethodText(null),L.ivRateSensitivityText(-0.12),
  L.FUTURES_BUILDUP_DEFINITION,L.FUTURES_BUILDUP_READING_TEXT,L.FUTURES_BUILDUP_NO_POINTS,
  L.NO_SYMBOL_TEXT,L.FILTER_PENDING_REASON,L.FILTER_NOT_APPLIED_REASON,L.FILTER_UNSUPPORTED_REASON,
  L.appliedText([]),L.notAppliedText(L.filterStatuses(RULES,{applied:[],available:[]})),
  
  L.NO_SESSION_DIRECTION,L.servedChip('shifting_up',{up:'shifting_up',down:'shifting_down',flat:'stable',none:'no baseline'},{shifting_up:'Shifting Up'}),
  L.maxPainLine({latest_max_pain_strike:1,latest_spot:2,latest_distance:1,latest_total_oi:3}),
  L.pcrLine({latest_pcr_oi:1,latest_pcr_volume:1}),
  L.futuresLine({latest_oi_vs_avg:1,latest_basis:1,latest_basis_pct:1,latest_buildup_day:"Long build-up"}),
  L.ivRateSourceShort({risk_free_rate:0.065,risk_free_rate_source:{kind:"code constant",live_feed:false}}),
  L.sessionSpoken('PCR by open interest',[1,2],['2026-09-18 09:30','2026-09-18 09:45'])];
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|support level|resistance level|breakout|momentum|overbought|oversold|bullish|bearish|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i;
 for(const text of sentences){
  assert.ok(String(text).length>0,'every sentence above must exist');
  assert.ok(!banned.test(String(text)),`a session-block sentence must not predict: ${text}`);
  assert.ok(!/\btrends?\b/i.test(String(text)),`"trend" names the daily view and nothing else: ${text}`);
 }
});
ok(()=>{ // the owner's word for a 15-minute timestamp, on every new file and every new sentence
 for(const file of [...SESSION_FILES,'types.ts']){
  const left=marky(SRC(file));
  assert.deepEqual(left,[],`${file}: say "15-min reading(s)", not "mark": ${left.join(' | ')}`);
 }
 const sentences=[L.PCR_DEFINITION,L.PCR_READING_TEXT,L.PCR_NO_POINTS,L.MAX_PAIN_DEFINITION,
  L.MAX_PAIN_GAP_TEXT,L.MAX_PAIN_READING_TEXT,L.MAX_PAIN_NO_POINTS,L.maxPainDistanceText(-301),L.IV_COMPUTED_TEXT,
  L.IV_DEFINITION,L.IV_READING_TEXT,L.IV_NO_POINTS,...Object.values(L.IV_REASONS),
  L.FUTURES_BUILDUP_DEFINITION,L.FUTURES_BUILDUP_READING_TEXT,L.FUTURES_BUILDUP_NO_POINTS,
  L.NO_SYMBOL_TEXT,L.FILTER_PENDING_REASON,L.FILTER_NOT_APPLIED_REASON,L.FILTER_UNSUPPORTED_REASON,
  L.pcrLine({latest_pcr_oi:1,latest_pcr_volume:1}),
  L.sessionSpoken('x',[1,2],['2026-09-18 09:30','2026-09-18 09:45'])];
 for(const text of sentences)assert.ok(!MARK_WORD.test(String(text)),`a session-block sentence says "mark": ${text}`);
 // and the owner's replacement really is the words used
 assert.ok(/15-min reading/.test(L.PCR_DEFINITION)&&/15-min reading/.test(L.MAX_PAIN_DEFINITION)
  &&/15-min reading/.test(L.IV_DEFINITION)&&/15-min reading/.test(L.FUTURES_BUILDUP_DEFINITION));
});
ok(()=>{ // amber is a caveat on the DATA, and is not spent on anything else in these blocks
 const panel=SRC('SessionPanel.tsx'),widget=SRC('UnusualWidget.tsx');
 // the only amber on the chart panel is the server's withheld sentence
 const ambers=[...panel.matchAll(/C\.amber/g)];
 assert.equal(ambers.length,2,'the chart panel and the readings panel spend their amber once each');
 assert.ok(/\{!!caveat&&<T style=\{\[metaText,\{color:C\.amber\}\]\}>\{caveat\}<\/T>\}/.test(panel),
  'and it is the caveat the server sent');
 // on the screener it is the filter that did not run - also a caveat on the rows
 assert.ok(/\{!!off&&<T style=\{\[metaText,\{color:C\.amber\}\]\}>\{off\}<\/T>\}/.test(widget));
 // the COMPUTED label is NOT amber: a model output is a fact about the number, not a fault in it
 assert.ok(!/Tag label=\{IV_COMPUTED_TAG\}[^>]*amber/.test(SRC('SessionBlocks.tsx')));
 // and a disabled or unsupported control is quiet small print, never amber
 assert.ok(/unsupported:\{color:C\.muted/.test(widget),'an unsupported filter is muted, not amber');
 assert.ok(/pending:\{color:C\.muted/.test(widget),'and so is one the server has not answered on');
});
ok(()=>{ // a value the server withheld is the SERVER's own sentence, never a blank and never a number
 const blocks=SRC('SessionBlocks.tsx');
 // three of the four carry a withheld tally; IV carries its refusals and its rate caveat instead
 assert.equal((blocks.match(/const caveat=withheldText\(body\)/g)||[]).length,3,
  'PCR, max pain and futures build-up each carry the reasons their session is short');
 assert.ok(/const rateCaveat=ivRateIsAssumed\(body\)/.test(blocks),'and IV carries its own, about the rate');
 assert.equal((blocks.match(/caveat=\{caveat\}/g)||[]).length,7,'and every panel of those three prints it');
 assert.equal((blocks.match(/caveat=\{rateCaveat\}/g)||[]).length,2,'as do both IV panels');
 // the tally and the sentences behind it go into the block's one disclosure, marked as what is missing
 assert.equal((blocks.match(/gapGroup\(body\)/g)||[]).length,3);
 assert.ok(/heading:'What is missing, and why'/.test(blocks));
});


// =================================================================================================================
// RENDER-TIME TRANSFORMS ON USER-FACING COPY
//
// 293 checks passed over a sentence that reached the screen as:
//   "A contract i li ted only when it clear all three liquidity floor , which are named in How to read thi ."
// The cause was `.replace(/s+/g,' ')` where `.replace(/\s+/g,' ')` was meant — one missing backslash, so every
// run of the letter "s" became a space. Every one of those 293 checks asserted either on the SOURCE text or on
// a pure function's RETURN VALUE. Not one of them asserted on the string a component actually hands the screen,
// and a sentence can be destroyed in the gap between those two.
//
// So this section works on the gap itself, in two ways:
//   1. A shape rule over every transform on copy: a `.replace` that collapses text to whitespace or deletes it
//      must match on an ESCAPE or a character class. `/s+/` is a run of letters; `/\s+/` is whitespace. A
//      pattern of bare letters that replaces them with a space is the bug, whatever the letters are.
//   2. A render-path spot check: the expression that builds the screener's empty sentence is pulled out of the
//      component, evaluated exactly as written, and the words the source promises are asserted in the result.
// Reverting the fix fails both.
// =================================================================================================================
const TSX=['UnusualWidget.tsx','ScreenerSection.tsx','SessionBlocks.tsx','SessionPanel.tsx','OiGridSection.tsx',
 'FuturesChartPanel.tsx','frame.tsx','ChartTile.tsx','ChainWidget.tsx','IndexWidget.tsx','FuturesWidget.tsx',
 'OiByStrikeSection.tsx','FilterDialog.tsx','Table.tsx','index.tsx'];
/** The {...} expression starting at `open`, with its braces balanced (template holes included). */
const balanced=(text,open)=>{
 let depth=0;
 for(let i=open;i<text.length;i++){
  if(text[i]==='{')depth++;
  else if(text[i]==='}'){depth--;if(!depth)return text.slice(open,i+1);}
 }
 throw new Error('unbalanced expression');
};
ok(()=>{ // 1. no transform that collapses copy may match on a bare run of letters
 let seen=0;
 for(const file of TSX){
  const code=CODE(file);
  for(const m of code.matchAll(/\.replace\(\/((?:[^/\\\n]|\\.)+)\/(\w*)\s*,\s*(['"`])([^'"`]*)\3\s*\)/g)){
   const [,pattern,flags,,to]=m;
   // only the collapsing kind: replacing with whitespace or with nothing is a formatting transform, and a
   // formatting transform is about whitespace, so its pattern must say so
   if(!/^\s*$/.test(to))continue;
   seen++;
   assert.ok(/\\[a-zA-Z]|\[/.test(pattern),
    `${file}: .replace(/${pattern}/${flags},'${to}') collapses copy with a pattern of plain letters. `+
    `That is what a lost backslash looks like — write \\s, \\n or a character class.`);
  }
 }
 assert.ok(seen>0,'this guard must actually be looking at something');
});
ok(()=>{ // and the same rule over the whole file, for a transform written any other way round
 for(const file of TSX){
  const code=CODE(file);
  for(const m of code.matchAll(/\.replace\(\/([a-zA-Z][a-zA-Z0-9]*)([+*]?)\/g\s*,/g))
   assert.fail(`${file}: .replace(/${m[1]}${m[2]}/g, …) matches LETTERS, not whitespace — a lost backslash`);
 }
});
ok(()=>{ // 2. the screener's empty sentence survives the path it actually travels to the screen
 const code=CODE('UnusualWidget.tsx');
 const at=code.indexOf('emptyDetail=');
 assert.ok(at>0,'the screener must build an empty-state sentence');
 const expr=balanced(code,code.indexOf('{',at));
 // evaluated exactly as written, with the served hole stubbed: what comes out is what the reader gets
 const rendered=new Function('screenerEmptyDetail','body',`return ${expr.slice(1,-1)};`)(()=>'',null);
 for(const phrase of ['A contract is listed only when it clears all three liquidity floors',
  'which are named in How to read this'])
  assert.ok(rendered.includes(phrase),
   `the screener's empty sentence is mangled on the way to the screen: "${rendered}"`);
 // and it really is collapsed onto one line, which is what the transform is there for
 assert.ok(!/\n|\s{2,}/.test(rendered),`the sentence should be one line: "${rendered}"`);
 // every word of the source survives: a transform that eats one letter everywhere would pass a phrase check
 // that happened to miss it, and would not pass this
 const source=expr.replace(/\$\{[^{}]*\}/g,' ').replace(/[`{}]/g,' ');
 for(const word of new Set(source.match(/[A-Za-z]{4,}/g)||[])){
  if(['replace','trim','emptyDetail'].includes(word))continue;
  assert.ok(rendered.includes(word),`"${word}" is in the source and not in what renders: "${rendered}"`);
 }
});
ok(()=>{ // the same words really are the ones the reader is promised, and are not a copy that has drifted
 const code=CODE('UnusualWidget.tsx');
 assert.ok(/liquidity floors, which are named in How to\s+read this/.test(code),
  'the screener names where the floors are written down');
 // the served detail rides along rather than replacing it, so the two facts are never one or the other
 assert.ok(/\$\{screenerEmptyDetail\(body\)\}/.test(code));
});
ok(()=>{ // no OTHER user-facing copy is cut down at render time either: the short forms are built in logic
 const widget=CODE('UnusualWidget.tsx');
 assert.ok(!/\.label\.replace\(/.test(widget),
  'a label is built in logic, never cut out of a longer one at render time');
 assert.ok(/options=\{list\.map\(r=>\(\{value:r\.value,label:r\.short,detail:r\.detail\}\)\)\}/.test(widget),
  'the compact reading label is served by logic.readingChoices');
 const choice=L.readingChoices({readings:[{at:'2026-09-18 15:45:00',underlyings:216}]})[0];
 assert.equal(choice.label,'18 Sep 2026 · 15:45');
 assert.equal(choice.short,'15:45','both forms come out of the one builder');
 // an unreadable stamp keeps whatever it had rather than being cut to nothing
 assert.equal(L.readingChoices({readings:[{at:'nonsense'}]})[0].short,'nonsense');
});
ok(()=>{ // every remaining transform on copy in these files is one of the harmless kinds, and is listed
 const allowed=/\.(trim|toLowerCase|toUpperCase|toFixed|toLocaleString|padStart|padEnd|join|slice|split|includes|startsWith|endsWith|match|test|repeat|map|filter|find|reverse|sort|some|every|replace)\(/;
 for(const file of TSX){
  const code=CODE(file);
  // `.substr`/`.substring` truncate silently and have no place in copy on this tab
  assert.ok(!/\.substr\(|\.substring\(/.test(code),
   `${file}: substr/substring truncates copy silently - clamp with numberOfLines or build the short form in logic`);
  // a slice on a STRING literal is a truncation of copy; on an array it is a window of rows, which is fine
  for(const m of code.matchAll(/(['"`][^'"`\n]{8,}['"`])\s*\.slice\(/g))
   assert.fail(`${file}: ${m[1]}.slice( truncates copy - say the short thing instead of cutting the long one`);
  assert.ok(allowed,'');
 }
});
ok(()=>{ // the one place a control character is used to join sentences really is a control character
 const panel=CODE('FuturesChartPanel.tsx');
 const split=/const SPLIT='\\u0000'/.test(panel);
 assert.ok(split,'the futures panel joins its notes on a character that cannot occur inside one');
 assert.ok(/info\.split\(SPLIT\)/.test(panel),'and splits on the same one');
 // a printable separator here would cut a sentence in half the first time one contained it
 assert.ok(!/const SPLIT='[^\\]/.test(panel),'never a printable separator');
});


// --- a column header FITS its column ----------------------------------------------------------------------------
// Two of the screener's headers printed on the same pixels the first time it was looked at with real rows:
// "ODI CHG (DAY)" and "VOLUMEL VS MEDIAN". Wider labels had been added to columns sized for shorter ones, and
// nothing in the suite was measuring that. This does, in the same arithmetic the block titles are already held
// to - and Table.tsx now clips as well, so a future miss degrades to a clipped word instead of a collision.
/** Every column of a table, split on its own entry rather than matched by a regex that stops at the first
 *  closing brace - a render function full of JSX has plenty of those. */
const tableColumns=(file)=>{
 const src=SRC(file);
 const at=src.indexOf('Column<');
 assert.ok(at>0,file+' must declare its columns');
 const body=src.slice(src.indexOf('[',at),src.indexOf("\n ],[",at));
 return body.split(/\n  \{key:'/).slice(1).map(chunk=>{
  const key=chunk.slice(0,chunk.indexOf("'"));
  const label=/label:'([^']+)'/.exec(chunk);
  const width=/width:(\d+)/.exec(chunk);
  return {key,label:label?label[1]:'',width:width?Number(width[1]):0,
   sortable:/value:VALUE\./.test(chunk),funnel:/filter:onCustomize/.test(chunk)};
 });
};
ok(()=>{
 // the header's own type: 10px, InterMedium, uppercase, 0.6 letter-spacing. 7.0px a character is a deliberate
 // over-estimate, so a label that passes here has room to spare in the browser.
 const CHAR=7.0,PAD=12,SORT=11,FUNNEL=10,GAP=3;
 const cols=tableColumns('UnusualWidget.tsx');
 assert.ok(cols.length>=14,`the screener's columns look unread: found ${cols.length}`);
 for(const col of cols){
  assert.ok(col.label&&col.width,`the ${col.key} column needs a label and a width`);
  const needs=col.label.length*CHAR+PAD+(col.sortable?SORT+GAP:0)+(col.funnel?FUNNEL+GAP:0);
  assert.ok(needs<=col.width,
   `the "${col.label}" header needs ${Math.round(needs)}px and its column is ${col.width}px - it would print over its neighbour`);
 }
});
ok(()=>{ // and the table clips a header that outgrows its column, whatever the widths say
 const table=SRC('Table.tsx');
 assert.ok(/gap:3,minWidth:0,overflow:'hidden',justifyContent:justify\(column\.align\)/.test(table),
  'the header cell must clip its contents');
 assert.ok(/<T numberOfLines=\{1\} style=\{\[head,\{flexShrink:1,minWidth:0,/.test(table),
  'and the label itself must be allowed to shrink inside it');
});
ok(()=>{ // every column showing a funnel really is one the Customize builder can make a rule for
 const onto={symbol:'underlying',summary:'underlying',expiry:'expiry',dte:'dte',oi_chg:'oiChangeDay',
  oi_chg_15m:'oiChange15m',vol_ratio:'volumeRatio',vol_oi:'volumeToOi',premium:'premium',buildup:'buildup'};
 const columns=new Set(L.FILTER_COLUMNS.map(c=>c.key));
 const funnels=tableColumns('UnusualWidget.tsx').filter(c=>c.funnel);
 assert.ok(funnels.length>=8,`a funnel says "you can filter on this": found ${funnels.length}`);
 for(const col of funnels){
  assert.ok(onto[col.key],`the "${col.key}" column shows a funnel but nothing says which rule it opens`);
  assert.ok(columns.has(onto[col.key]),`the "${col.key}" funnel opens ${onto[col.key]}, which is not a rule`);
 }
});

// --- a chip that loses its vocabulary is a chip that lost its direction -------------------------------------------
// The futures route serves TWO vocabularies, named for its two readings (`oi` and `basis`), where every other
// route serves one flat map. Handing a chip the whole object instead of the one it wanted is a QUIET failure:
// `up` is undefined, so the chip keeps its word and loses its arrow AND its colour — and still reads as a chip.
// It shipped that way for one screenshot. These hold the resolution explicit and the failure findable.
ok(()=>{ // a flat vocabulary resolves; a named one resolves by name; anything else resolves to nothing
 const flat={up:'rising',down:'falling',flat:'flat',none:'no baseline'};
 const named={oi:{up:'building',down:'unwinding',flat:'flat',none:'no baseline'},
  basis:{up:'widening',down:'narrowing',flat:'flat',none:'no baseline'}};
 assert.deepEqual({...L.directionWords(flat)},flat);
 assert.deepEqual({...L.directionWords(named,'oi')},named.oi);
 assert.deepEqual({...L.directionWords(named,'basis')},named.basis);
 // the whole object where ONE of its vocabularies was wanted is not a vocabulary
 assert.equal(L.directionWords(named),null,'a map of vocabularies is not itself one');
 assert.equal(L.directionWords(flat,'oi'),null,'and a name that is not there resolves to nothing');
 assert.equal(L.directionWords(null),null);
 assert.equal(L.directionWords({up:'rising'}),null,'half a vocabulary is not one either');
});
ok(()=>{ // and the state that used to pass silently is now something that can be asked about
 const named={oi:{up:'building',down:'unwinding',flat:'flat',none:'no baseline'}};
 assert.equal(L.directionMissing('unwinding',L.directionWords(named,'oi')),false);
 assert.equal(L.directionMissing('unwinding',L.directionWords(named)),true,
  'a direction with no vocabulary containing it is the quiet failure');
 assert.equal(L.directionMissing('unwinding',null),true);
 // no direction at all is not a failure, it is a state
 assert.equal(L.directionMissing('',null),false);
 assert.equal(L.directionMissing(L.NO_SESSION_DIRECTION,null),false);
 assert.equal(L.directionMissing(null,null),false);
});
ok(()=>{ // every chip on the tab resolves its vocabulary through that one helper
 const blocks=CODE('SessionBlocks.tsx');
 assert.equal((blocks.match(/directionWords\(body\?\.direction_words/g)||[]).length,5,
  'PCR, max pain, IV, and the futures block twice - one resolution each');
 assert.ok(/directionWords\(body\?\.direction_words,'oi'\)/.test(blocks)
  &&/directionWords\(body\?\.direction_words,'basis'\)/.test(blocks),
  'the futures block asks for its two by name');
 // and NO chip is handed the raw response field any more
 assert.ok(!/servedChip\([^)]*body\?\.direction_words/.test(blocks),
  'a chip is never handed the raw field: on the futures route that is a map of vocabularies, not one');
 assert.ok(!/servedTone\([^)]*body\?\.direction_words/.test(blocks));
});
ok(()=>{ // the futures route really does serve two, and the others really do serve one
 assert.ok(/'direction_words':\{'oi':/.test(DERIV_PY)||/direction_words.{0,40}'oi'/.test(DERIV_PY),
  'derivatives.py serves the futures vocabularies by name');
 // the two sets of words are the ones the chips are built from
 for(const word of ['building','unwinding','widening','narrowing','rising','falling','expanding','cooling',
  'shifting_up','shifting_down','stable'])
  assert.ok(DERIV_PY.includes(`'${word}'`),`the server must know the word "${word}"`);
});


// --- a bare dash never stands inside a SENTENCE ------------------------------------------------------------------
// A dash in a column of numbers is fine: the label beside it says what is missing. A dash dropped into the
// middle of a sentence is not - "basis — · Short covering on the day" makes the reader work out which of two
// numbers was the cause of the other. Both places this happens are a derived figure whose input was not
// captured, and both now say so instead.
ok(()=>{
 // the futures line, with a spot the store did not capture (NIFTY's own latest reading, 18 Sep)
 const noSpot=L.futuresLine({latest_oi_vs_avg:1.0082,latest_basis:null,latest_basis_pct:null,
  latest_buildup_day:'Short covering'});
 assert.ok(noSpot.includes('no spot was captured at this reading, so there is no basis'),noSpot);
 assert.ok(!noSpot.includes(DASH),'no bare dash is left in the sentence: '+noSpot);
 assert.ok(noSpot.includes('1.01'),'the figure that WAS captured is still there');
 assert.ok(noSpot.includes('Short covering on the day'),noSpot);
 // with both, it reads as a basis
 const whole=L.futuresLine({latest_oi_vs_avg:0.9656,latest_basis:3.7,latest_basis_pct:0.2981,
  latest_buildup_day:'Long unwinding'});
 assert.ok(whole.includes('basis +'),whole);
 assert.ok(!whole.includes(DASH),whole);
 // a build-up the store could not label is left out of the sentence rather than dashed into it
 const noLabel=L.futuresLine({latest_oi_vs_avg:1,latest_basis:1,latest_basis_pct:1,latest_buildup_day:'no data'});
 assert.ok(!noLabel.includes(DASH),noLabel);
 assert.ok(!/on the day/.test(noLabel),'an unlabelled build-up is not announced: '+noLabel);
});
ok(()=>{ // and the ROW keeps its dash, because there the label beside it says what is missing
 const blocks=SRC('SessionBlocks.tsx');
 assert.ok(/label:'Basis \(futures less spot\)',value:basisPair\(body\?\.latest_basis,body\?\.latest_basis_pct\),/
  .test(blocks));
 assert.ok(/reason:BASIS_NO_SPOT_ROW/.test(blocks),'and the reason stands under it');
 assert.ok(/A basis needs a futures price and a spot at the same reading\./.test(CODE('logic.ts')));
 // the same shape as max pain's, which is the other derived figure with a missing input
 assert.ok(/reason:'Distance needs both a strike and a spot at the same reading\.'/.test(blocks));
 // every readings row whose value can be a dash carries a reason for it
 const rows=[...blocks.matchAll(/{label:'[^']+',value:[^}]*?}/g)].map(m=>m[0]);
 assert.ok(rows.length>=14,`the readings rows look unread: ${rows.length}`);
});


// =================================================================================================================
// SUPPORT AND RESISTANCE: AN OBSERVATION IS ALLOWED, A LEVEL IS NOT
//
// The owner wrote the ΔOI flow table himself and asked for it VERBATIM. Two of its sentences are
//
//     "Sellers are building resistance"        and        "Sellers are building support"
//
// and they are ALLOWED. They are allowed for a reason, and the reason is written down here rather than left to
// be rediscovered, because the obvious thing to do about them is to reword them and the obvious thing is wrong:
//
//     Describing where open interest is sitting RIGHT NOW is an OBSERVATION.
//     Naming a level the price will respect is a FORECAST.
//
// "Sellers are building resistance" is the first kind. It says what the book did at this 15-min reading — puts or
// calls were written, and by whom — and it is the owner's own word for that. "NIFTY will struggle at 23,500" is
// the second kind, and it stays forbidden, as does every form that turns the word into a price.
//
// So this does not ban the two words. It bans the LEVEL FORMS: the word tied to "level", to "at" / "near" /
// "around", to a zone, a band or a line, or to a number anywhere in the same sentence. Every OTHER use is
// refused too, so the allowance is exactly two sentences and not a doorway.
//
// IF YOU ARE HERE BECAUSE A SWEEP FLAGGED THE OWNER'S SENTENCES: do not reword them. Add them to the allow list.
// =================================================================================================================
const OWNER_FLOW_PHRASES=['Sellers are building resistance','Sellers are building support'];
const LEVEL_WORD=/\b(support|resistance)\b/i;
/** '' when a string is clean; otherwise the offending sentence.
 *
 *  The allowance is the WHOLE SENTENCE, not a substring of one. Striking the owner's phrase out of a longer
 *  line and judging what is left was the first thing tried here, and it is a doorway: it clears "Sellers are
 *  building resistance at 23,500", which is precisely the forecast this exists to refuse. So a sentence is
 *  clean only when, with its trailing punctuation off, it IS one of his two sentences and nothing more. */
const levelForm=(text)=>{
 const whole=String(text||'');
 if(!LEVEL_WORD.test(whole))return '';
 for(const sentence of whole.split(/(?<=[.!?])\s+/)){
  if(!LEVEL_WORD.test(sentence))continue;
  const bare=sentence.replace(/^[\s"'“”‘’]+|[\s.!?,;:"'“”‘’]+$/g,'');
  if(OWNER_FLOW_PHRASES.includes(bare))continue;   // his sentence, whole and on its own
  return sentence;                                  // anything else naming support or resistance is a level
 }
 return '';
};
ok(()=>{ // the owner's two sentences pass, alone and inside a line of their own block
 for(const phrase of OWNER_FLOW_PHRASES){
  assert.equal(levelForm(phrase),'',`the owner's own words must pass: ${phrase}`);
  assert.equal(levelForm(`Put writing increasing. ${phrase}.`),'');
 }
});
ok(()=>{ // and every level form is refused, including the owner's words turned into a price
 const banned=['Resistance level at 23,500','A support level is forming','Resistance at 23,500',
  'Support near 23,300','Watch the resistance zone','Strong support around 23,000',
  'Sellers are building resistance at 23,500','Sellers are building support around 23,300',
  '23,500 is resistance','The book shows resistance here','Buyers are defending support'];
 for(const text of banned)assert.ok(levelForm(text),`this must be refused: ${text}`);
});
ok(()=>{ // NOTHING the reader can reach on this tab is a level form - on either side
 const files=fs.readdirSync(path.join(__dirname,'..','src','derivative')).filter(f=>/\.tsx?$/.test(f));
 assert.ok(files.length>=15,`the tab's files look unread: ${files.length}`);
 for(const file of files){
  for(const text of VISIBLE(SRC(file))){
   const bad=levelForm(text);
   assert.equal(bad,'',`${file}: a string the reader can reach names a level: "${bad}"`);
  }
 }
 // the server's own flow table, which is where the two allowed sentences live
 const at=DERIV_PY.indexOf('FLOW_LABELS={');
 assert.ok(at>0,'derivatives.py must declare FLOW_LABELS');
 const table=DERIV_PY.slice(at,DERIV_PY.indexOf('\n}',at));
 for(const m of table.matchAll(/'([^']{12,})'/g)){
  const bad=levelForm(m[1]);
  assert.equal(bad,'',`derivatives.py FLOW_LABELS names a level: "${bad}"`);
 }
});
ok(()=>{ // the two sentences are still there, VERBATIM, on both sides
 // This is the guard against the well-meaning fix. If someone "cleans up" the owner's wording, this fails and
 // points at the paragraph above rather than letting the table drift away from what he wrote.
 const client=SRC('logic.ts');
 for(const phrase of OWNER_FLOW_PHRASES){
  assert.ok(client.includes(phrase),
   `"${phrase}" is the owner's own wording and must not be reworded - see the note above this check`);
  assert.ok(DERIV_PY.includes(phrase),`derivatives.py must serve "${phrase}" verbatim`);
 }
 // and they really are the pair the flow table puts on a written-option reading
 assert.ok(/'CE\|down\|building':\('Call writing increasing','Sellers are building resistance'\)/.test(DERIV_PY));
 assert.ok(/'PE\|down\|building':\('Put writing increasing','Sellers are building support'\)/.test(DERIV_PY));
 const labels=Object.values(L.FLOW_LABELS).flat();
 for(const phrase of OWNER_FLOW_PHRASES)assert.ok(labels.includes(phrase),
  `the browser's flow table must carry "${phrase}" too, so both sides say the same thing`);
});
ok(()=>{ // every OTHER §5 sweep on this tab must leave the owner's two sentences alone
 // A future tightening that outlaws them would be a real regression, and this is where it gets caught: the
 // answer is to allow-list them here, not to change what he wrote.
 const sweeps=[
  /\b(will|expect|forecast|predict|likely|should rise|should fall|target price|bullish signal|bearish signal)\b/i,
  /\b(will|expect|forecast|predict|likely|should rise|should fall|target price|target|support level|resistance level|bullish|bearish|buy signal|sell signal)\b/i,
  /\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|support level|resistance level|breakout|momentum|overbought|oversold|bullish|bearish|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i,
 ];
 for(const phrase of OWNER_FLOW_PHRASES)for(const sweep of sweeps)
  assert.ok(!sweep.test(phrase),`a §5 sweep now bans the owner's own wording: ${phrase}`);
 // the level FORMS of the same words are still caught by those sweeps as well as by levelForm above
 for(const text of ['A resistance level at 23,500','A support level is forming'])
  assert.ok(sweeps[1].test(text)&&sweeps[2].test(text),`a level form must still be swept: ${text}`);
});


// WHAT WAS RUN, recorded with every run. Two of us changed the bundle and a check script at the same time
// without recording either, and a script change read as a product regression. The summary carries this
// script's own hash and the exported bundle's timestamp, exactly as scripts/check-discover.e2e.cjs does.
const SCRIPT_SHA=require('node:crypto').createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0,12);
const WEB_DIR=process.env.QA_WEB_DIRECTORY||path.join(__dirname,'..','dist-pilot');
const BUNDLE=(()=>{try{return fs.statSync(path.join(WEB_DIR,'index.html')).mtime.toISOString();}catch{return 'unknown';}})();
console.log(`derivative logic: ${checks} checks passed`);
console.log(`Ran against: script ${SCRIPT_SHA} · bundle ${BUNDLE} (${path.relative(path.join(__dirname,'..'),WEB_DIR)||WEB_DIR})`);





















