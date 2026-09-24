// Pure checks for src/derivative/logic.ts — the Derivative tab's formatting and card wording
// (docs/DERIVATIVES_SPEC.md §3–§5). Run: node scripts/check-derivative.cjs (no server needed).
//
// What is being protected here, in one line: a number that was not captured must read as a dash or the words
// "no baseline", never as zero; and no sentence this file produces may claim what happens next.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const load=(rel,req)=>{const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..',rel),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;const ctx={exports:{},require:req,process:{env:{}}};vm.runInNewContext(code,ctx);return ctx.exports;};
const L=load('src/derivative/logic.ts',n=>{throw Error('unexpected import: '+n)});
let checks=0;const ok=fn=>{fn();checks++};
/** Source with its COMMENTS removed - line and block both. A comment explains the rule and never reaches the
 *  reader, and prose in one contains apostrophes: leaving block comments in meant a doc comment could open a
 *  quote that ran on into the code, so a check looking for user-facing strings saw neither the comment nor
 *  what it had swallowed. Strings are extracted from what this returns and from nothing else. */
const NO_COMMENTS=(text)=>text.replace(/\/\*[\s\S]*?\*\//g,'')
 .split('\n').filter(line=>!line.trim().startsWith('//')).join('\n');
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
ok(()=>{for(const v of [null,undefined,'',' '])assert.equal(L.buildupLabel(v),DASH,String(v))});
ok(()=>{assert.equal(L.buildupTone('long_buildup'),'up');assert.equal(L.buildupTone('short_covering'),'up');
 assert.equal(L.buildupTone('short_buildup'),'down');assert.equal(L.buildupTone('long_unwinding'),'down');
 assert.equal(L.buildupTone('nonsense'),'flat');assert.equal(L.buildupTone(null),'flat')});

// =================================================================================================================
// P05 — THE BUILD-UP VOCABULARY, END TO END.
//
// THE DEFECT, REPRODUCED: the store writes the reader-facing strings ('Long build-up', 'Flat', 'no data') and
// this file's lookup was keyed on §3.1's snake ids. Every served build-up missed the lookup, so every build-up
// cell fed from a screener row, and all six choices in the build-up filter menu, rendered as a dash. The 359
// checks that stood here did not catch it because they only ever fed the lookup its OWN keys.
//
// THE FIX: one canonical id, one display label, one wire value, and a single normalising boundary. These checks
// feed the boundary the STORE's vocabulary, which is what the screen actually receives.
// =================================================================================================================
ok(()=>{ // THE REPRODUCTION: every value the database actually holds renders as words, not as a dash
 const stored=['Long build-up','Short build-up','Short covering','Long unwinding','Flat','no data'];
 assert.deepEqual([...L.BUILDUP_VALUES],stored,'these are the six the store holds');
 for(const value of stored.slice(0,5)) // the five that are labels the reader sees
  assert.notEqual(L.buildupLabel(value),DASH,
   `the store's own "${value}" must not render as a dash - this is the P05 defect`);
 assert.equal(L.buildupLabel('Long build-up'),'Long build-up');
 assert.equal(L.buildupLabel('Flat'),'Flat','Flat is a label the store wrote, not an absence');
 // "no data" is the store saying it could not classify the reading: a dash, and never the word Flat
 assert.equal(L.buildupLabel('no data'),DASH);
 assert.notEqual(L.buildupLabel('no data'),L.buildupLabel('Flat'));
});
ok(()=>{ // ONE ENUM: the ids, the labels and the wire values are one list read three ways
 assert.deepEqual([...L.BUILDUP_IDS],['long_buildup','short_buildup','short_covering','long_unwinding',
  'flat','no_data']);
 assert.deepEqual([...L.BUILDUP_IDS].map(id=>L.BUILDUP_WIRE[id]),[...L.BUILDUP_VALUES],
  'every id has exactly one wire value, in the server\'s own order');
 for(const id of L.BUILDUP_IDS)assert.equal(L.buildupId(L.BUILDUP_WIRE[id]),id,
  `the wire value of ${id} must fold back to ${id}`);
 for(const id of L.BUILDUP_IDS)assert.equal(L.buildupId(id),id,'and an id is already canonical');
});
ok(()=>{ // FLAT, MISSING, NO DATA and UNKNOWN are four different answers and none becomes another
 assert.equal(L.buildupId('Flat'),'flat');
 assert.equal(L.buildupId('no data'),'no_data');
 for(const v of [null,undefined,'','   '])assert.equal(L.buildupId(v),'missing',String(v));
 for(const v of ['up','bullish','Long buildup!','LONG-BUILD-UPP','3','{}'])
  assert.equal(L.buildupId(v),'unknown',String(v));
 // the one that matters: a value nothing recognises must NEVER read as Flat, and must be observable
 for(const v of ['up','bullish','Long buildup!']){
  assert.notEqual(L.buildupLabel(v),L.buildupLabel('Flat'),`"${v}" must not silently become Flat`);
  assert.notEqual(L.buildupLabel(v),DASH,`"${v}" must not silently vanish either`);
  assert.equal(L.buildupLabel(v),L.BUILDUP_UNKNOWN,'it says the tab does not recognise it');
  assert.equal(L.buildupTone(v),'flat','and it carries no colour it has not earned');
 }
});
ok(()=>{ // case and spelling variants of OUR OWN id fold in; they are the same value written differently
 for(const v of ['LONG_BUILDUP','long buildup','Long-Buildup'])assert.equal(L.buildupId(v),'long_buildup',v);
 for(const v of ['SHORT COVERING','short_covering','Short covering'])assert.equal(L.buildupId(v),'short_covering',v);
});
ok(()=>{ // the MENU says "no data" out loud, because it is one of the six the reader can choose
 assert.equal(L.buildupChoiceLabel('no_data'),'no data');
 assert.equal(L.buildupChoiceLabel('no data'),'no data');
 assert.equal(L.buildupChoiceLabel('flat'),'Flat');
 assert.equal(L.buildupChoiceLabel('long_buildup'),'Long build-up');
 // and a TABLE CELL keeps the tab's rule: nothing captured is a dash
 assert.equal(L.buildupLabel('no_data'),DASH);
});
ok(()=>{ // §3.1 AS A TRUTH TABLE: all nine price x open-interest cells, for the instrument's own price
 assert.equal(L.buildupFromMoves('up','building'),'long_buildup');
 assert.equal(L.buildupFromMoves('down','building'),'short_buildup');
 assert.equal(L.buildupFromMoves('up','unwinding'),'short_covering');
 assert.equal(L.buildupFromMoves('down','unwinding'),'long_unwinding');
 // a flat axis is NOT a quiet version of a direction: the store's own Flat is a zero change on either axis
 for(const [price,oi] of [['up','flat'],['down','flat'],['flat','building'],['flat','unwinding'],['flat','flat']])
  assert.equal(L.buildupFromMoves(price,oi),'flat',`${price} + ${oi}`);
 // nine cells, no tenth, and nothing falls through to a neighbour
 assert.equal(Object.keys(L.BUILDUP_FROM_MOVES).length,9);
 for(const price of ['up','down','flat'])for(const oi of ['building','flat','unwinding'])
  assert.ok(L.BUILDUP_FROM_MOVES[`${price}|${oi}`],`no §3.1 answer for ${price} + ${oi}`);
 // an axis with no direction at all yields no build-up to report
 for(const [price,oi] of [['no baseline','building'],['up','no baseline'],['',''],[null,null]])
  assert.equal(L.buildupFromMoves(price,oi),'unknown',`${price} + ${oi}`);
 // and the colour intent is that same table read once more, never a second rule
 for(const id of L.BUILDUP_IDS)assert.equal(L.buildupTone(id),L.buildupTone(L.BUILDUP_WIRE[id]),
  `${id}: the id and the wire value must colour the same`);
});
ok(()=>{ // the FUTURES summary reads build-ups through the same boundary as a table cell
 assert.equal(L.servedBuildup('Long build-up'),L.buildupLabel('Long build-up'));
 assert.equal(L.servedBuildup('Flat'),'Flat');
 assert.equal(L.servedBuildup('not a build-up'),L.BUILDUP_UNKNOWN,
  'the futures summary cannot show a value the table would refuse');
});
ok(()=>{ // the §3.1 counts are keyed by the string the STORE wrote, and are folded by id
 assert.equal(L.groupBuildupText({buildup_counts:{'Long build-up':4,'Short covering':2}}),
  'Long build-up 4 · Short covering 2');
 assert.equal(L.groupBuildupText({buildup_counts:{long_buildup:1,'Long build-up':2}}),'Long build-up 3',
  'the same build-up written two ways is one count');
 assert.equal(L.groupBuildupText({buildup_counts:{'Flat':3,'no data':1}}),'Flat 3 · no data 1');
 assert.ok(L.groupBuildupText({buildup_counts:{'Long buildup!':2}}).includes(L.BUILDUP_UNKNOWN),
  'a key nothing recognises is counted and said, not dropped');
 assert.equal(L.groupBuildupText({buildup_counts:{}}),'');
 assert.equal(L.groupBuildupText(null),'');
});
ok(()=>{ // A FILTER RULE ROUND-TRIPS: menu id -> sanitised rule -> the exact string the server validates
 for(const id of L.BUILDUP_IDS){
  const clean=L.sanitizeRules([{column:'buildup',operator:'is',value:id}]);
  assert.equal(clean.length,1,`the builder must accept ${id}`);
  assert.equal(clean[0].value,id,'and keep it canonical');
  const query=L.screenerQuery([{column:'buildup',operator:'is',value:id}]);
  assert.equal(query,`?buildup=${encodeURIComponent(L.BUILDUP_WIRE[id])}`,
   `${id} must reach the server as the string it stores`);
 }
 // a rule restored from a browser that stored the WIRE string is the same rule, normalised on the way in
 for(const wire of L.BUILDUP_VALUES){
  const clean=L.sanitizeRules([{column:'buildup',operator:'is',value:wire}]);
  assert.equal(clean.length,1,`a stored "${wire}" must survive`);
  assert.equal(clean[0].value,L.buildupId(wire),'as its canonical id');
  assert.equal(L.screenerQuery([{column:'buildup',operator:'is',value:wire}]),
   `?buildup=${encodeURIComponent(wire)}`,'and reach the wire unchanged');
 }
 // and a value neither vocabulary holds is still refused outright
 for(const junk of ['bull','Flatt','long_build_up_x',''])
  assert.deepEqual([...L.sanitizeRules([{column:'buildup',operator:'is',value:junk}])],[],junk);
});
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
ok(()=>{ // NULL IS NOT NOUGHT. A reading that captured no traded average price has no premium for this name -
 // and "₹0.0 cr traded" is a claim about the market where the truth is a gap in our own capture.
 const line=L.groupSummary({underlying:'X',premium_cr:null,strike_count:2,calls:1,puts:1,expiries:[],
  days_to_expiry:null,oi_change_day:null,strikes:[]});
 assert.ok(/premium traded not captured at this 15-min reading/.test(line),line);
 assert.ok(!/0\.0 cr/.test(line)&&!/₹0/.test(line),'never a fabricated zero');
});
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
 const strings=NO_COMMENTS(source).match(/'[^']*'|`[^`]*`/g)||[];
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
ok(()=>{ // every block on the page, in the order the owner asked for, UNDER the one pinned screener
 const page=SRC('index.tsx');
 // The rail is rendered in the page's own return, AFTER `body` is built, so it is not in this sequence:
 // being pinned and being the ONLY screener on the tab are checked on their own below.
 const order=[['Option chain','title="Option chain"'],
  ['OI by strike','<OiByStrikeSection'],['delta-OI by strike','<OiGridSection'],['PCR','<PcrSection'],
  ['Max pain','<MaxPainSection'],['IV','<IvSection'],['Futures build-up','<FuturesBuildupSection'],
  ['Index dashboard','title="Index dashboard"'],['Futures OI build-up','title="Futures OI build-up"']];
 let at=-1;
 for(const [title,needle] of order){
  const next=page.indexOf(needle);
  assert.ok(next>at,`block "${title}" is missing or out of order on the Derivative page`);
  at=next;
 }
 assert.ok(SRC('OiByStrikeSection.tsx').includes('title="OI by strike"')||
  SRC('OiByStrikeSection.tsx').includes("title='OI by strike'"),'the OI-by-strike block needs its own title');
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
  const strings=NO_COMMENTS(SRC(file)).match(/'[^']*'|`[^`]*`/g)||[];
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
 assert.ok(/WidgetFrame/.test(block),'both panels of the block wear the shared widget frame');
 assert.ok(/<Block\b/.test(block),'the block is drawn through the tab\'s ONE template');
 assert.ok(!/<Section\b/.test(block),'and not through a shape of its own');
 // THE OI-GRID ROUTE IS READ EXACTLY ONCE FOR THE WHOLE TAB, in index.tsx. Three panels are built on those
 // ten at-the-money contracts - the ΔOI grid, the signal table beside the screener, and the IV grid - and each
 // used to fetch them itself. That is three fetches of one payload, three answers that can drift apart at a
 // refresh, and precisely the redundancy the owner objected to. It must not be able to come back quietly.
 assert.ok(/api\/derivatives\/oi-grid\?underlying=/.test(page),'the PAGE reads the oi-grid route');
 assert.ok(/read=\{gridRead\}/.test(page),'and hands that one read to the ΔOI block');
 assert.equal((page.match(/api\/derivatives\/oi-grid\?underlying=/g)||[]).length,1,
  'exactly one oi-grid read exists on this tab');
 for(const name of ['OiGridSection.tsx','SignalTable.tsx','IvGridSection.tsx']){
  let src;try{src=SRC(name)}catch{continue}
  assert.ok(!/api\/derivatives\/oi-grid\?underlying=/.test(src),
   `${name} must RECEIVE the ten contracts, not fetch them: one payload, one answer`);
 }
 // THE SCREENER IS GONE FROM THIS BLOCK. It was the second copy of the tab's one list, down the left of the
 // block, and it is what the owner objected to: one screener, once, pinned at the top.
 assert.ok(!/api\/derivatives\/unusual/.test(block),
  'the block must not carry its own screener: there is ONE screener on this tab and it is pinned at the top');
 assert.ok(!/Unusual contracts/.test(block),'and no panel of it is titled like one');
 assert.ok(page.includes('<OiGridSection'),'the block is mounted on the Derivative page');
 assert.ok(!page.includes("'oi_grid_screener'"),'the screener panel key is gone with the panel');
 for(const key of ['oi_grid_futures','oi_grid'])assert.ok(page.includes(`'${key}'`),
  `${key} must be a widget the reader can close and restore`);
 // clicking a chart re-points the tab's one chart target - the same mechanism a table row uses
 assert.ok(/onTarget\(\{underlying/.test(block),'a click must write a chart target, not open its own chart');
 assert.ok(/instrumentToken:slot\.instrument_token/.test(block),'the target is the contract that was clicked');
});
ok(()=>{ // TWO panels, on the template: the futures chart, and the grid AS THE CONTENT PANEL
 const block=SRC('OiGridSection.tsx');
 assert.ok(/chart=\{futures\}/.test(block),'the front futures chart is the block\'s CHART panel');
 assert.ok(/content=\{grid\}/.test(block),'and the 2 x 5 grid IS its CONTENT panel');
 assert.ok(/const futures=\(style:PaneStyle\)=>/.test(block)&&/const grid=\(style:PaneStyle\)=>/.test(block),
  'both panels take the style the template hands them, rather than choosing their own');
 assert.ok(!/SCREENER_FLEX|FUTURES_FLEX|GRID_FLEX/.test(block),
  'the block declares no widths of its own: the template owns them');
 assert.ok(!/height=\{Math\.min/.test(block),'and no height of its own either');
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
  const strings=NO_COMMENTS(SRC(file)).match(/'[^']*'|`[^`]*`/g)||[];
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
 // The two default-symbol fallbacks moved UP to the page when the tab took over resolving its one symbol, and
 // the screener went with the panel that carried it. The GRID READ moved up too, once the signal table and the
 // IV grid needed the very same ten contracts. The block now makes NO read of its own at all: it RECEIVES the
 // one the page made. (The futures chart panel still makes its own, in its own file.)
 assert.equal((block.match(/useDerivativeRead</g)||[]).length,0,
  'the block receives the ten contracts and fetches nothing: one payload, one answer');
 assert.ok(/read:Read<OiGrid>/.test(block),'and it says so in its own props');
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
 // ================================================================================================================
 // THE OWNER'S MARKET SIGNAL COLUMN, AND WHY THESE WORDS ARE ALLOWED IN EXACTLY ONE PLACE.
 //
 // This guard was added when the owner told us to REMOVE the bullish/bearish column, and it was right then. He
 // has since asked for it back, in writing, with a full specification: the MARKET SIGNAL column of the signal
 // table, with his own rule -
 //   🟢 Bullish: put writing up + call unwinding down
 //   🔴 Bearish: call writing up + put unwinding down
 //   🟡 Flat:    both sides roughly balanced, or little change
 //
 // THIS COLUMN STATES HOW OPTION POSITIONING READS RIGHT NOW, IN THE OWNER'S OWN VOCABULARY, AT HIS EXPLICIT
 // INSTRUCTION. IT IS NOT A FORECAST, AND THE WORDS ARE ALLOWED HERE AND NOWHERE ELSE.
 //
 // IF YOU ARE HERE BECAUSE A SWEEP FLAGGED THE OWNER'S SIGNAL COLUMN, DO NOT REWORD IT - this allowance is
 // deliberate. Add to the list below only if he extends the vocabulary himself.
 //
 // The allowance is the WHOLE VALUE, never a substring of a sentence. That shortcut is exactly what let
 // "Sellers are building resistance at 23,500" through the support/resistance sweep the first time, and the
 // mutation tests under this check exist to prove it cannot happen again: his words in the signal column pass,
 // the same words in a block subtitle fail, and a forecast inside the signal table fails.
 // ================================================================================================================
 const moody=/bullish|bearish/i;
 const OWNER_SIGNAL_VALUES=['Bullish','Strong Bullish','Bearish','Strong Bearish','Neutral','Flat'];
 /** True only when the whole string IS one of his signal values (or its key form), punctuation aside. */
 const ownerSignalValue=(text)=>{
  const bare=String(text||'').replace(/^[\s"'“”‘’🟢🔴🟡]+|[\s.!?,;:"'“”‘’]+$/g,'');
  return OWNER_SIGNAL_VALUES.includes(bare)
   ||OWNER_SIGNAL_VALUES.map(v=>v.toLowerCase().replace(/ /g,'_')).includes(bare);
 };
 // Every file on the tab EXCEPT the signal table: the words may not reach the screen at all.
 // (A comment may still state the rule - that is what the comments in logic.ts and OiGridSection.tsx do.)
 for(const file of ['types.ts','OiGridSection.tsx','ChartTile.tsx','frame.tsx','index.tsx',
  'UnusualWidget.tsx','ChainWidget.tsx','OiByStrikeSection.tsx','IndexWidget.tsx','FuturesWidget.tsx',
  'FuturesChartPanel.tsx']){
  const said=VISIBLE(SRC(file)).filter(t=>moody.test(t));
  assert.deepEqual(said,[],`${file}: the tab does not label a reading bullish or bearish: ${said.join(' | ')}`);
 }
 // logic.ts and SignalTable.tsx hold the signal column. His VALUES pass; any other use of the words fails.
 for(const file of ['logic.ts','SignalTable.tsx']){
  const said=VISIBLE(SRC(file)).filter(t=>moody.test(t)&&!ownerSignalValue(t));
  assert.deepEqual(said,[],
   `${file}: only the owner's own signal VALUES may use these words - see the note above: ${said.join(' | ')}`);
 }
 // The allowance is not a doorway. His values pass...
 for(const text of [...OWNER_SIGNAL_VALUES,'🟢 Bullish','🔴 Strong Bearish','strong_bullish','bearish'])
  assert.ok(ownerSignalValue(text),`the owner's own signal value must pass: ${text}`);
 // ...and every sentence that merely CONTAINS one of them does not.
 for(const text of ['NIFTY looks bullish here','A bullish reading of the chain','Bullish above 23,500',
  'The book is bearish','bearish momentum','Strong Bullish above the max pain strike',
  'This is bullish for the index'])
  assert.ok(!ownerSignalValue(text),`this must still be refused: ${text}`);
 // And a forecast inside the signal table is still a forecast: the §5 sweep below covers that file too.
 for(const text of ['NIFTY will fall to 23,200','Expect a move to 23,500','A target of 23,500'])
  assert.ok(banned.test(text),`a forecast must fail wherever it is written: ${text}`);
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
const CODE=(file)=>NO_COMMENTS(SRC(file));

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
ok(()=>{ // nothing chosen: row one of the list the server served first, the index second, BOTH flagged
 assert.deepEqual({...L.resolveBlockSymbol('','TCS','NIFTY')},
  {symbol:'TCS',defaulted:true,label:L.BLOCK_DEFAULT_RANKED});
 assert.deepEqual({...L.resolveBlockSymbol(null,null,'NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX});
 assert.deepEqual({...L.resolveBlockSymbol(null,'  ','NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX},'a blank ranked answer is no answer');
 // THE LABEL NAMES THE ORDER THAT IS ACTUALLY IN FORCE. "Busiest by premium" described a sort the screener
 // has not used for a long time - it opens with the unusual first and premium is its THIRD key - over a row
 // the reader never chose. The server's own name for the order it served wins; the constant is the fallback.
 assert.equal(L.BLOCK_DEFAULT_RANKED,'First in the screener order');
 assert.ok(!/premium/i.test(L.BLOCK_DEFAULT_RANKED),'and it must not claim a premium sort');
 assert.deepEqual({...L.resolveBlockSymbol('','TCS','NIFTY','Unusual first')},
  {symbol:'TCS',defaulted:true,label:'Unusual first'},"the server's own ranking label wins");
 assert.deepEqual({...L.resolveBlockSymbol('','TCS','NIFTY','   ')},
  {symbol:'TCS',defaulted:true,label:L.BLOCK_DEFAULT_RANKED},'a blank label falls back, never to a blank badge');
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
 assert.equal((block.match(/useDerivativeRead</g)||[]).length,0,
  'the block fetches nothing itself - the page hands it the grid, the panel makes its own candles');
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

// =================================================================================================================
// ONE TEMPLATE FOR THE WHOLE TAB, AND ONE SCREENER
//
// The owner, on the version where every section had its own shape: "everything is wrong - I asked for same block
// template across for all blocks right?" These are the checks that make the answer something the code holds to.
//
// What is protected here:
//   * the geometry lives in ONE place (frame.tsx) - a block is HANDED its panel styles and never invents them;
//   * every block on the tab is drawn through frame.Block, with exactly two panels;
//   * there is exactly ONE screener on the tab, it is PINNED, and it cannot be closed;
//   * one height, one pair of widths, one breakpoint - no block has a wider one;
//   * the grid is still five tiles across, measured at the ONE breakpoint rather than at its own.
// =================================================================================================================
const FRAME=SRC('frame.tsx');
const frameNum=(name)=>{const m=new RegExp(`\\b${name}=(\\d+(?:\\.\\d+)?)`).exec(FRAME);
 assert.ok(m,`frame.tsx must declare ${name}`);return Number(m[1]);};
/** Every block of the tab: its file, and the two panel keys the template gives it. */
const BLOCKS=[
 ['Option chain','index.tsx','chain_chart','chain'],
 ['OI by strike','OiByStrikeSection.tsx','oi_chart','oi_table'],
 ['delta-OI by strike','OiGridSection.tsx','oi_grid_futures','oi_grid'],
 ['PCR','SessionBlocks.tsx','pcr_chart','pcr_readings'],
 ['Max pain','SessionBlocks.tsx','max_pain_chart','max_pain_readings'],
 ['IV','SessionBlocks.tsx','iv_chart','iv_strikes'],
 ['Futures build-up','SessionBlocks.tsx','fut_oi','fut_readings'],
 ['Index dashboard','index.tsx','indices_chart','indices'],
 ['Futures OI build-up','index.tsx','futures_chart','futures'],
];
ok(()=>{ // the geometry is declared ONCE, in the frame, and nowhere else
 for(const name of ['BLOCK_H','BLOCK_CHART_FLEX','BLOCK_CONTENT_FLEX','BLOCK_BESIDE','BLOCK_GAP'])
  assert.ok(new RegExp(`export const [A-Z_,=\\d.]*${name}=`).test(FRAME),`frame.tsx must export ${name}`);
 assert.ok(/export function paneStyles\(stacked:boolean,height:number\)/.test(FRAME),
  'and it must build BOTH panel styles from that one height, in one function');
 // no block file may declare a width, a height or a breakpoint of its own
 for(const file of ['OiGridSection.tsx','OiByStrikeSection.tsx','SessionBlocks.tsx','ScreenerSection.tsx']){
  const code=CODE(file);
  assert.ok(!/\b(ROW_H|STACK_TABLE_H|GRID_ROW_H|GRID_STACK_H|SESSION_ROW_H|SESSION_STACK_H|CHART_FLEX|READINGS_FLEX|SCREENER_FLEX|FUTURES_FLEX|GRID_FLEX|GRID_BESIDE|CHART_BESIDE)\b/
   .test(code),`${file} must not declare a geometry of its own: the template owns it`);
 }
 const page=CODE('index.tsx');
 assert.ok(!/\bGRID_BESIDE\b|\bCHART_BESIDE\b/.test(page),'and the page has ONE breakpoint, not two');
 assert.ok(/BLOCK_BESIDE/.test(page)&&/const stacked=page<BLOCK_BESIDE/.test(page),
  'which every block is given, together');
});
ok(()=>{ // every block is drawn through the ONE template, with exactly two panels
 for(const [title,file,chartKey,contentKey] of BLOCKS){
  const code=SRC(file);
  assert.ok(/<Block\b/.test(code),`${title}: ${file} must draw its blocks through frame.Block`);
  assert.ok(code.includes(`'${chartKey}'`)||code.includes(`"${chartKey}"`),
   `${title}: its CHART panel is ${chartKey}`);
  assert.ok(code.includes(`'${contentKey}'`)||code.includes(`"${contentKey}"`),
   `${title}: its CONTENT panel is ${contentKey}`);
 }
 // and the widget list is exactly those pairs, in order, under the pinned screener
 const page=SRC('index.tsx');
 const list=/export const WIDGET_KEYS=\[([\s\S]*?)\] as const;/.exec(page);
 assert.ok(list,'index.tsx must list every widget in one place');
 const keys=(list[1].match(/'[^']+'/g)||[]).map(t=>t.slice(1,-1));
 const want=['unusual',...BLOCKS.flatMap(([,,c,n])=>[c,n])];
 assert.deepEqual(keys,want,'the widget list is the pinned screener, then each block\'s chart and content in turn');
});
ok(()=>{ // no block invents a third shape: two panels, both taking the style the template hands them
 for(const file of ['OiGridSection.tsx','OiByStrikeSection.tsx','SessionBlocks.tsx']){
  const code=SRC(file);
  const blocks=(code.match(/<Block\b/g)||[]).length;
  assert.equal((code.match(/\bchart=\{/g)||[]).length,blocks,`${file}: one chart panel per block`);
  assert.equal((code.match(/\bcontent=\{/g)||[]).length,blocks,`${file}: one content panel per block`);
  assert.ok(!/<Section\b/.test(code),`${file} must not use the old section shape`);
 }
 // the futures build-up block had THREE panels; its two charts share the template's one chart panel now
 const session=SRC('SessionBlocks.tsx');
 assert.ok(!session.includes("'fut_basis'"),'the basis is no longer a panel of its own');
 assert.ok(/<SessionPairPanel/.test(session),'it shares the chart panel with open interest');
 const panel=SRC('SessionPanel.tsx');
 assert.ok(/export function SessionPairPanel/.test(panel)&&/<SessionPlot \{\.\.\.top\}/.test(panel)
  &&/<SessionPlot \{\.\.\.bottom\}/.test(panel),'two plots, one panel, each keeping its own legend and scale');
 assert.ok(/times=\{times\}/.test(panel),'both drawn on the same reading grid');
});
ok(()=>{ // ONE screener on the tab, pinned, and it cannot be closed
 const page=SRC('index.tsx');
 assert.equal((page.match(/<ScreenerRail\b/g)||[]).length,1,'the screener is mounted exactly once');
 // and nowhere else on the tab does a panel read the screener or the list it replaced
 for(const file of ['OiGridSection.tsx','OiByStrikeSection.tsx','SessionBlocks.tsx','ChainWidget.tsx',
  'IndexWidget.tsx','FuturesChartPanel.tsx']){
  const code=CODE(file);
  assert.ok(!/derivatives\/screener/.test(code)&&!/derivatives\/unusual/.test(code),
   `${file} must not carry a second copy of the tab's one list`);
 }
 assert.equal((page.match(/SCREENER_PATH/g)||[]).length>0,true,'the page makes the one screener read itself');
 // NOT PINNED, and this is the guard that keeps it that way. The screener was sticky; the owner's verdict on
 // seeing it was "I can't see anything other than the screener". It is a table beside a signal table, not a
 // thin strip, so stuck to the top it held most of the window and left a sliver for nine blocks. A reader who
 // has already chosen an instrument wants to read the blocks, not keep the list they chose from in view.
 assert.ok(!/stickyHeaderIndices/.test(page),
  'the page scroller pins nothing: the screener scrolls away with everything else');
 const rail=SRC('ScreenerSection.tsx');
 assert.ok(!/Pinned/.test(rail),'and no badge claims it is pinned, because it is not');
 assert.ok(/backgroundColor:C\.bg/.test(rail),'a pinned bar needs an opaque background to scroll under');
 // it is the tab's control surface, not one of its widgets: no close control anywhere on it
 assert.ok(!/onClose/.test(rail),'the screener cannot be closed: every block answers to it');
 assert.ok(/export const PINNED_KEY/.test(page)&&/if\(key!==PINNED_KEY&&raw\[key\]===true\)/.test(page),
  'and it can never be restored from storage into a hidden state');
});
ok(()=>{ // the five-column promise, measured at the ONE breakpoint the whole tab shares
 const block=SRC('OiGridSection.tsx');
 const beside=frameNum('BLOCK_BESIDE'),chartFlex=frameNum('BLOCK_CHART_FLEX'),
  contentFlex=frameNum('BLOCK_CONTENT_FLEX'),gap=frameNum('BLOCK_GAP');
 assert.ok(contentFlex>chartFlex,'the content panel carries the block\'s substance, so it takes the larger share');
 // page padding either side, the gutter, the widget border and the grid's own padding
 const contentWidth=(beside-40-gap)*contentFlex/(chartFlex+contentFlex),inner=contentWidth-22;
 const cols=/width<(\d+)\?2:width<(\d+)\?3:5/.exec(block);
 assert.ok(cols,'gridColumns must still declare its own wrap points');
 assert.ok(inner>=Number(cols[2]),
  `at ${beside}px the grid gets ${Math.round(inner)}px and must still be five tiles across (needs ${cols[2]})`);
 const cell=(inner-8*4)/5;
 assert.ok(cell>=120,`a tile at the breakpoint is ${Math.round(cell)}px and must clear the grid's own 120px floor`);
 // and the chart panel beside it must still be a chart rather than a smear
 assert.ok((beside-40-gap)*chartFlex/(chartFlex+contentFlex)>=260,
  'the chart panel must still fit a candle series and its axis');
});
ok(()=>{ // the one height really is one height: the tallest content panel fits inside it
 const block=SRC('OiGridSection.tsx');
 const n=(name)=>{const m=new RegExp(`\\b${name}=(\\d+)`).exec(block);
  assert.ok(m,`OiGridSection.tsx must declare ${name}`);return Number(m[1]);};
 const tile=n('CELL_PAD')*2+n('BORDER')+n('ROW_GAP')*5+n('TITLE_H')+n('DETAIL_H')
  +(n('PLOT_H')+n('TIME_H'))+n('CHIP_H')+n('WHAT_H')+n('MEANING_H');
 // two bands, each with its heading and gap, the block's own padding, and the widget's header and footer
 const needed=tile*2+2*(14+6)+10+20+38+26;
 assert.ok(frameNum('BLOCK_H')>=needed,
  `BLOCK_H is ${frameNum('BLOCK_H')} and the two tile bands need ${needed}: the puts band must not be cut off`);
});
ok(()=>{ // the futures chart is a panel of the page like every other: closable, restorable, expandable
 const page=SRC('index.tsx'),block=SRC('OiGridSection.tsx');
 for(const piece of ["shows('oi_grid_futures')","onExpand('oi_grid_futures')","onHide('oi_grid_futures')"])
  assert.ok(block.includes(piece),`the chart must support ${piece}`);
 assert.ok(/WidgetFrame/.test(SRC('FuturesChartPanel.tsx')),'the chart wears the tab\'s own widget frame');
 assert.ok(/api\/derivatives\/futures-chart\?underlying=/.test(SRC('FuturesChartPanel.tsx')),
  'and it reads the fixed futures-chart route');
 assert.ok(/if\(!left&&!right\)return null/.test(FRAME),
  'a block disappears only when BOTH of its panels are closed - the template decides that, once');
 assert.ok(page.includes("'oi_grid_futures'")&&page.includes("'oi_grid'"),
  'both panels are widgets the reader can close and restore');
});
ok(()=>{ // ONE headline figure per block, and it is never a bare number
 assert.ok(/export function Headline\(/.test(FRAME),'the headline is built in one place');
 assert.ok(/fontSize:26/.test(FRAME),'at a size nothing else in the block comes close to');
 assert.ok(/missing\?\(reason\|\|''\):\(against\|\|''\)/.test(FRAME),
  'and it carries either the comparison that gives it meaning, or the reason there is no figure');
 assert.ok(/export function headlineAgainst/.test(SRC('logic.ts')),
  'a comparison is built from the parts that HAVE a value');
 const dash=L.DASH;
 assert.equal(L.headlineAgainst('1.01x its 20-day average',`basis ${dash}`),'1.01x its 20-day average',
  'a part that is a dash is dropped, never printed as half a comparison');
 assert.equal(L.headlineAgainst(dash,'',null),'','and nothing is printed when nothing has a value');
 assert.ok(L.maxPainAgainstSpot(23350,null,null).includes('no distance'),
  'no spot, no distance - and the headline says why rather than trailing off');
 assert.ok(L.maxPainAgainstSpot(23350,23302,48).includes('above'),
  'and with both, the strike is placed against the captured spot');
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
  L.futuresNote(null,'15m'),L.futuresNote(null,'1d'),L.BLOCK_DEFAULT_RANKED,L.BLOCK_DEFAULT_INDEX,
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
  'GRID_GAP_TEXT','GRID_HIGHLIGHT_TEXT','chartInfo'])
  assert.ok(groups.includes(piece),`the definitions panel must still carry ${piece}`);
 // a panel the reader closed takes its group with it: the block never explains what is not on screen
 assert.ok(/lines:!shows\('oi_grid'\)\?\[\]:\[/.test(block)
  &&/lines:shows\('oi_grid_futures'\)\?chartInfo:\[\]/.test(block),'a closed panel contributes no group');
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
 assert.equal((block.match(/\binBlock\b/g)||[]).length,1,'the grid panel sits inside the block');
 assert.equal((panel.match(/\binBlock\b/g)||[]).length,1,'and so does the futures chart');
 // the in-block footer prints the panel's own line and NOTHING the block header already printed
 const at=frame.indexOf('{inBlock?('),end=frame.indexOf('footer!==undefined?');
 assert.ok(at>0&&end>at,'the frame must have an in-block footer branch');
 const foot=frame.slice(at,end);
 assert.ok(!/asOfText|floorsText/.test(foot),
  'a panel inside a block must not repeat the as-of or the floors its block header already printed');
 // and no panel of the block asks the frame for the full as-of/floors footer by leaving inBlock off
 for(const tag of ['<FuturesChartPanel','<DeltaOiGrid'])
  assert.ok(block.includes(tag),`${tag} must be mounted by the block`);
 assert.ok(!block.includes('<UnusualScreenerPanel'),
  'and the screener that used to sit beside them is gone: there is ONE screener, pinned at the top');
 assert.equal((block.match(/asOfText\(/g)||[]).length,1,'the block prints an as-of exactly once');
 // the floors left with the screener that applied them; they are printed on the pinned bar now
 assert.equal((block.match(/floorsText\(/g)||[]).length,0,'the block no longer reads the floors');
 assert.ok(SRC('ScreenerSection.tsx').includes('floorsText(body?.floors,body?.floors_text)'),
  'the pinned screener does, once, beside the rows the floors decided');
});

// --- no title is cut short at any width the tab draws this block at ----------------------------------------------
// The header bar's own numbers, read out of frame.tsx so this cannot drift from the layout: the gutters, the
// gap between its children and the three icon buttons. What is left after those is what a title has to fit in.
// The per-character figures are deliberate over-estimates for Inter at 11px (uppercase + 1px letter-spacing
// for a title, ordinary case for a subtitle), so a title that passes here has room to spare in the browser.
const TITLE_PX=8.0,SUB_PX=5.6;
ok(()=>{
 const frame=SRC('frame.tsx');
 const bar=/const btn=inBlock\?(\d+):\d+,pad=inBlock\?(\d+):\d+,gap=inBlock\?(\d+):\d+/.exec(frame);
 assert.ok(bar,'frame.tsx must declare the in-block header geometry in one place');
 const btn=Number(bar[1]),pad=Number(bar[2]),gap=Number(bar[3]);
 // titleBlock, spacer, refresh, expand, close = five children and four gaps
 const chrome=2+pad+2+gap*4+4+btn*3;
 const beside=frameNum('BLOCK_BESIDE'),chartFlex=frameNum('BLOCK_CHART_FLEX'),
  contentFlex=frameNum('BLOCK_CONTENT_FLEX'),gutter=frameNum('BLOCK_GAP');
 // the narrowest width the two panels still sit side by side at: page padding 20 either side, one gutter
 const row=beside-40-gutter;
 const box=(key)=>row*(key==='chart'?chartFlex:contentFlex)/(chartFlex+contentFlex)-chrome;
 // The longest title each panel of the template can ever show. Every block is measured, not just one: that is
 // what "the same header treatment" has to mean if it is to survive the next edit.
 const titles={
  chart:'BAJAJFINSV26SEPFUT',            // the futures chart panel, named for its contract
  content:'Strikes at the money',
 };
 // the fixed titles are read straight out of the source, so a longer one cannot slip past this
 for(const file of ['OiGridSection.tsx','OiByStrikeSection.tsx','ChainWidget.tsx','SessionPanel.tsx'])
  for(const m of SRC(file).matchAll(/name="([^"]+)"/g))
   assert.ok(m[1].length*TITLE_PX<=box('chart'),
    `at ${beside}px the narrower panel gives its title ${Math.round(box('chart'))}px and "${m[1]}" needs ${Math.round(m[1].length*TITLE_PX)}px`);
 // the session panels are titled by their block, so those titles are checked where they are written
 for(const text of ['PCR through the session','Max pain against spot','ATM implied volatility',
  'Futures open interest and basis','Latest futures reading','Volatility by strike','Latest max pain',
  'Latest PCR reading'])
  assert.ok(text.length*TITLE_PX<=box('chart'),
   `at ${beside}px a panel title has ${Math.round(box('chart'))}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 for(const [key,text] of Object.entries(titles))assert.ok(text.length*TITLE_PX<=box(key),
  `at ${beside}px the ${key} panel gives its title ${Math.round(box(key))}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 // and the subtitle beside it, at the same width
 const subs={chart:'25 Sep 2026 · 8 days to expiry',
  content:'NIFTY · 2026-09-25 · ATM 23,350 · spot ₹23,346.40'};
 for(const [key,text] of Object.entries(subs))assert.ok(text.length*SUB_PX<=box(key),
  `at ${beside}px the ${key} subtitle needs ${Math.round(text.length*SUB_PX)}px and has ${Math.round(box(key))}px`);
 // stacked, each panel is the page wide: the narrowest phone the tab renders at must still fit every title
 const phone=360-24-chrome;
 for(const text of Object.values(titles))assert.ok(text.length*TITLE_PX<=phone,
  `stacked on a 360px phone a title has ${Math.round(phone)}px and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
});
ok(()=>{ // the titles really are those strings, and the long ones the owner saw are gone
 const block=SRC('OiGridSection.tsx');
 assert.ok(block.includes('name="Strikes at the money"'),'the grid\'s title is the short one');
 for(const old of ['Unusual derivative screener','ΔOI by strike · ten strikes at the money'])
  assert.ok(!block.includes(old),`"${old}" was cut short in its own header and must not come back`);
 // the one screener on the tab is titled once, on the pinned bar
 assert.ok(SRC('UnusualWidget.tsx').includes('name="Screener"'),'the pinned screener keeps its short title');
 assert.ok(!block.includes('Unusual contracts'),'and the copy that lived in this block is gone with it');
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

// --- a block with no symbol yet collapses rather than owning 600px of page for one sentence -----------------------
ok(()=>{
 const sect=SRC('OiByStrikeSection.tsx'),frame=SRC('frame.tsx');
 const idle=/export const IDLE_H=(\d+);/.exec(frame);
 assert.ok(idle,'frame.tsx must declare, once, the height an unchosen widget needs');
 assert.ok(Number(idle[1])<frameNum('BLOCK_H')/2,
  'and it must be well under the one height a block takes when it has a symbol');
 // the collapse is the TEMPLATE's decision now, taken once for both panels together, so a block where one
 // panel shrank and the other did not is not a shape the code can make any more
 assert.ok(/const h=height\|\|\(idle\?IDLE_H:BLOCK_H\);/.test(frame),
  'the template collapses BOTH panels of a block together, from one height');
 assert.ok(/const idle=!underlying;/.test(sect),'and a block says when it has no symbol yet');
 assert.ok(/idle=\{idle\}/.test(sect),'which is what it hands the template');
 assert.ok(/note=\{idle\?undefined:/.test(sect),'an empty panel prints no footer line about rows it has not got');
 // nothing about WHAT it says changed beyond naming the pinned screener as where a symbol comes from
 assert.ok(sect.includes('to see open interest by strike.'),'the empty sentence still names what is missing');
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
 for(const name of ['TILE_READING_TEXT','GRID_HIGHLIGHT_TEXT','GRID_GAP_TEXT']){
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
  // P05: 'long_buildup' USED to be junk here, because the rule carried the wire string and the id was a
  // stranger to it. The id is now the canonical rule value and the wire string is made at the edge, so the
  // junk that belongs on this line is a value NEITHER vocabulary holds.
  [rule('buildup','is','bull')],[rule('buildup','is','Long buildup!')],[rule('moneyness','is','deep')],
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
 assert.ok(/\{line\.tag\?' '\+line\.tag:''\}/.test(panel),'and the legend prints it');
 // 4. and the crosshair readout, which is what a reader sees while pointing at the line
 assert.ok(/\{row\.value\}\{row\.tag\?` \$\{row\.tag\}`:''\}/.test(SRC('frame.tsx')),
  'a value read off the chart under the pointer carries the tag too');
 // 4. EVERY TILE of the 2 x 5 grid — the strike list became a grid, and the marking came with it
 const grid=SRC('IvGridSection.tsx');
 assert.equal((grid.match(/<Tag label=\{IV_COMPUTED_TAG\}/g)||[]).length,2,
  'a tile says it twice: once on its title, once beside its number');
 assert.ok(/height:TITLE_H\}\]\}>[\s\S]{0,400}<Tag label=\{IV_COMPUTED_TAG\} a11y="Computed by a pricing model, not reported by the exchange"\/>/
  .test(grid.replace(/\r/g,'')),'the title row carries it, and says it to a screen reader');
 // and it is OUTSIDE every conditional in the tile: a tile that solved nothing is still a computed tile
 assert.ok(!/\{[^\n]*\?[^\n]*<Tag label=\{IV_COMPUTED_TAG\}/.test(grid),
  'the label is never made conditional on there being a number');
 assert.ok(/\{ivText\(value\)\}<\/T>\n\s*<Tag label=\{IV_COMPUTED_TAG\}\/>/.test(grid.replace(/\r/g,'')),
  'and it rides beside the figure itself');
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
ok(()=>{ // a null is never a blank on a tile, and never a drawn point on the line
 const grid=CODE('IvGridSection.tsx');
 assert.ok(/const refusal=value!=null\?''\s*\n?\s*:\(ivLatestRefusal\(points,body\?\.reason_text\)/
  .test(grid.replace(/\r/g,'')),'a tile without a number works out its reason');
 assert.ok(/<T numberOfLines=\{2\}[\s\S]{0,120}\{refusal\}<\/T>/.test(grid),
  'and prints that reason where the number would have been');
 // the reason box is ALWAYS reserved, so a refused tile is the same height as a solved one
 assert.ok(/<View style=\{\{height:REASON_H\}\}>/.test(grid),'and the room for it is always there');
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
  {symbol:'INFY',defaulted:true,label:L.BLOCK_DEFAULT_RANKED});
 assert.deepEqual({...L.resolveTabSymbol('','','INFY','NIFTY','Unusual first')},
  {symbol:'INFY',defaulted:true,label:'Unusual first'},"and the server's own ranking label reaches the badge");
 assert.deepEqual({...L.resolveTabSymbol('','','','NIFTY')},
  {symbol:'NIFTY',defaulted:true,label:L.BLOCK_DEFAULT_INDEX});
 assert.deepEqual({...L.resolveTabSymbol('','','','')},{symbol:'',defaulted:false,label:''});
 assert.deepEqual({...L.resolveTabSymbol()},{symbol:'',defaulted:false,label:''});
});
ok(()=>{ // a defaulted symbol still SAYS it is a default, and a chosen one is just itself
 assert.equal(L.symbolBadge({symbol:'NIFTY',defaulted:true,label:'Default'}),'Default: NIFTY');
 assert.equal(L.symbolBadge({symbol:'INFY',defaulted:true,label:L.BLOCK_DEFAULT_RANKED}),
  'First in the screener order: INFY');
 assert.equal(L.symbolBadge({symbol:'INFY',defaulted:true,label:'Unusual first'}),'Unusual first: INFY');
 assert.equal(L.symbolBadge({symbol:'RELIANCE',defaulted:false,label:''}),'RELIANCE');
 assert.equal(L.symbolBadge({symbol:'',defaulted:false,label:''}),'');
 assert.equal(L.symbolBadge(null),'');
});
ok(()=>{ // the PAGE resolves it once, and every block on the page is handed that one answer
 const page=CODE('index.tsx');
 assert.equal((page.match(/resolveTabSymbol\(/g)||[]).length,1,'the tab resolves its symbol exactly once');
 assert.ok(/const symbol=choice\.symbol,badge=symbolBadge\(choice\)/.test(page),'and keeps it in one place');
 // every block that takes a symbol is given THAT one, by name - never its own lookup
 assert.ok(/<ChainWidget key="chain" underlying=\{symbol\}/.test(page),'the option chain');
 assert.ok(/<OiByStrikeSection underlying=\{symbol\}/.test(page),'the strikes');
 assert.ok(/<OiGridSection symbol=\{symbol\}\n?\s*badge=\{badge\}/.test(page.replace(/\r/g,'')),'the ΔOI block');
 assert.ok(/const sessionProps=\{symbol,choice,badge,/.test(page),'and the four session blocks, through one object');
 for(const tag of ['<PcrSection {...sessionProps}/>','<MaxPainSection {...sessionProps}/>',
  '<IvSection {...sessionProps} ','<FuturesBuildupSection {...sessionProps}/>'])
  assert.ok(page.includes(tag),`${tag} must be given the tab's symbol`);
 // The pinned screener names it in the SAME pill every block header uses, so the link between the bar
 // and everything under it is something the reader SEES rather than something they confirm by reading.
 assert.ok(/<SymbolPill label=\{badge\} linked=\{linked\} large\/>/.test(SRC('ScreenerSection.tsx')),
  'and the pinned screener names it in the same pill');
 assert.ok(/export function SymbolPill/.test(SRC('frame.tsx'))
  &&/<SymbolPill label=\{badge\} linked=\{linked\}\/>/.test(SRC('frame.tsx')),
  'which is built once and worn by every block header');
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
 // one as-of per block, in the block header, never once per panel
 assert.equal((blocks.match(/asOf=\{asOfText\(body\?\.as_of\)\}/g)||[]).length,4,'each block prints its as-of once');
 assert.equal((blocks.match(/asOfText\(/g)||[]).length,4,'and only there');
 assert.equal((screener.match(/asOfText\(/g)||[]).length,1,'the pinned screener too');
 // every panel shape of every block declares it lives in one, so the frame drops the repeated footer
 assert.equal((SRC('SessionPanel.tsx').match(/\binBlock\b/g)||[]).length,3,
  'all three panel shapes - the plot, the pair of plots and the readings - sit inside a block');
 // one "How to read this" per block, and the definitions live behind it rather than on the surface
 assert.equal((blocks.match(/<InfoDisclosure /g)||[]).length,4);
 assert.equal((screener.match(/<InfoDisclosure /g)||[]).length,1);
 assert.equal((blocks.match(/<Block /g)||[]).length,4,'four blocks, four copies of the ONE template');
 assert.equal((blocks.match(/<Section /g)||[]).length,0,'and no shape of their own');
});
ok(()=>{ // a panel showing ONE SENTENCE takes one sentence's height, not a chart's
 const blocks=SRC('SessionBlocks.tsx'),frame=SRC('frame.tsx');
 const idle=/export const IDLE_H=(\d+);/.exec(frame);
 assert.ok(idle,'frame.tsx declares that height once');
 assert.ok(/export function oneSentence\(phase:string\)\{return phase==='error';\}/.test(blocks),
  'an unavailable endpoint is one sentence');
 assert.ok(/export function shrink\(symbol:string,phase:string\)\{return !symbol\|\|oneSentence\(phase\);\}/.test(blocks),
  'and so is no symbol at all');
 assert.equal((blocks.match(/const idle=shrink\(symbol,read\.phase\)/g)||[]).length,4,
  'all four blocks collapse on the same rule');
 assert.equal((blocks.match(/idle=\{idle\}/g)||[]).length,4,'and hand that one answer to the template');
 // the TEMPLATE decides what a collapsed block is worth, once, for BOTH panels together - so a block where
 // one panel shrank and the other did not is not a shape the code can produce any more
 assert.ok(/const h=height\|\|\(idle\?IDLE_H:BLOCK_H\);/.test(frame),'to frame.tsx\'s one number');
 assert.ok(/const panes=paneStyles\(!!stacked,h\);/.test(frame),'at both layouts');
 // a reading that returned nothing is NOT this case and keeps its height: rows will be there at the next one
 assert.ok(!/phase==='empty'/.test(blocks),'an empty READING is not an empty panel and keeps its full height');
 assert.ok(Number(idle[1])<frameNum('BLOCK_H')/1.5,'and IDLE_H is well under the height a block takes with data');
});
ok(()=>{ // no title on a session panel is cut short at the ONE width the tab draws every block at
 const frame=SRC('frame.tsx'),blocks=SRC('SessionBlocks.tsx');
 const bar=/const btn=inBlock\?(\d+):\d+,pad=inBlock\?(\d+):\d+,gap=inBlock\?(\d+):\d+/.exec(frame);
 assert.ok(bar,'the in-block header geometry is declared in one place');
 const btn=Number(bar[1]),pad=Number(bar[2]),gap=Number(bar[3]);
 const chrome=2+pad+2+gap*4+4+btn*3;
 const TITLE_PX=8.0;
 const beside=frameNum('BLOCK_BESIDE'),chartFlex=frameNum('BLOCK_CHART_FLEX'),
  contentFlex=frameNum('BLOCK_CONTENT_FLEX'),gutter=frameNum('BLOCK_GAP');
 const row=beside-40-gutter;
 // ONE pair of widths for every block. The chart panel is the narrower of the two, so measuring every title
 // against IT is the strict reading - a title that fits there fits anywhere on the tab.
 const narrow=row*chartFlex/(chartFlex+contentFlex)-chrome;
 const titles=[...blocks.matchAll(/name="([^"]+)"/g)].map(m=>m[1]);
 assert.ok(titles.length>=8,'every session panel has a title');
 for(const text of titles)assert.ok(text.length*TITLE_PX<=narrow,
  `at ${beside}px a panel has ${Math.round(narrow)}px for its title and "${text}" needs ${Math.round(text.length*TITLE_PX)}px`);
 // the subtitle under it is the same shape for all four blocks, so it is measured once
 assert.ok(/export function blockSubtitle/.test(blocks),'one subtitle shape for the family');
 const sub='BAJAJFINSV · 25 Sep 2026 · 8 days to expiry';
 assert.ok(sub.length*5.6<=narrow,
  `the block subtitle needs ${Math.round(sub.length*5.6)}px and has ${Math.round(narrow)}px`);
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
 // The fourth is the reading a figure came from when it is NOT the block's own. That is a caveat on
 // the data - the number beside it was captured at a different reading - so it is amber too.
 assert.equal(ambers.length,4,
  'the plot, the pair of plots, the readings panel and the earlier-reading note, once each');
 assert.ok(/\{!!row\.at&&!missing&&<T numberOfLines=\{1\} style=\{\[metaText,\{color:C\.amber\}\]\}>/
  .test(panel),'and a figure from an earlier reading says so where the number is');
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
 // the futures block's TWO charts share one panel now, so its three caveats became two
 assert.equal((blocks.match(/caveat=\{caveat\}/g)||[]).length,6,'and every panel of those three prints it');
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
 // THE SAME SHAPE AS MAX PAIN'S, which is the other derived figure with a missing input - except that max
 // pain's reason is the SERVER'S, not a sentence written in the page.
 //
 // The page used to hard-code "no reading of this session carried both", and that is false in the very case
 // the dash appears in most often: on 18 Sep 2026 the 11:30 reading carried a strike AND a spot, while the
 // newest strike (15:45) had no spot beside it. The server already tells the three cases apart - no strike at
 // all, no spot at all, and no spot at THIS strike's reading - and `latest_distance_reason` says which.
 assert.ok(/reason:body\?\.latest_distance_reason\|\|undefined/.test(blocks),
  "the distance row must print the server's own reason");
 assert.ok(!/no reading of this session carried both/.test(blocks),
  'and must not claim the session carried no pair when an earlier reading did');
 assert.ok(/'no_spot_at_reading':\('The reading that carries this max-pain strike carries no spot/.test(DERIV_PY),
  'the server distinguishes a strike with no spot at ITS reading from a session with no spot at all');
 assert.ok(/'latest_distance_reason':\(DISTANCE_REASONS\.get\(gap_reason\) if gap_reason else None\)/.test(DERIV_PY),
  'and serves it under the name the page reads');
 // BOTH TIMES OR NEITHER. A 15:45 strike beside an 11:30 spot with only the spot time-stamped reads as one
 // reading. Whichever of the two the caller can name, the comparison names.
 assert.equal(L.maxPainAgainstSpot(23350,23302,48,'at 11:30','at 15:45'),
  'strike at 15:45 · spot ₹23,302.00 at 11:30 · the strike is 48 above it');
 assert.equal(L.maxPainAgainstSpot(23350,23302,48,'at 11:30'),
  'spot ₹23,302.00 at 11:30 · the strike is 48 above it','and a caller with only one time still says it');
 assert.ok(/maxPainAgainstSpot\(body\?\.latest_max_pain_strike,body\?\.latest_spot,body\?\.latest_distance,pairAt\('spot'\),\s*pairAt\('max_pain_strike'\)\)/
  .test(blocks),"and the max-pain headline passes the strike's own reading through");
 // AND THE STAMP IS SUPPRESSED ONLY WHEN THE PAIR AGREES. The block-wide `at()` hides a stamp that equals
 // the block's as-of - which on 18 Sep 2026 was the 15:45 strike itself, so the line stamped the 11:30 spot
 // and left the strike bare, reading as one reading.
 assert.ok(/const samePair=body\?\.latest_max_pain_strike_at===body\?\.latest_spot_at;/.test(blocks));
 assert.ok(/const pairAt=\(key:string\)=>figureAtText\(\(body as any\)\?\.\[`latest_\$\{key\}_at`\],samePair\?body\?\.as_of:null\);/
  .test(blocks),'the pair stamps itself against the OTHER figure, not against the block');
 assert.equal(L.figureAtText('2026-09-18 15:45',null),'at 15:45','with no as-of every figure names its reading');
 assert.equal(L.figureAtText('2026-09-18 15:45','2026-09-18 15:45'),'','and a figure at its own as-of stays bare');
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
// THE SAME ALLOWANCE COVERS HIS OI INTERPRETATION COLUMN, and for the same reason. He wrote that table too -
// "More resistance than support", "Resistance reducing + support increasing", "Resistance increasing + support
// weakening" - and in every one of them:
//
//     "RESISTANCE" AND "SUPPORT" ARE NAMES FOR WHERE OPTION WRITERS HAVE BUILT POSITIONS, AT THIS READING.
//     THEY ARE NOT LEVELS.
//
// A LEVEL is a price the market is claimed to respect: "resistance at 23,500", "support near 23,300", "23,500 is
// resistance". Those stay banned, with a number and without one, and the mutation tests below prove it.
//
// TWO OWNER-SPECIFIED VOCABULARIES NOW LIVE ON THIS TAB, AND BOTH ARE DELIBERATE: his MARKET SIGNAL column
// (Bullish / Bearish / Flat, guarded further up this file) and his OI INTERPRETATION column (here). He had
// previously asked for BOTH to be removed, and he has since asked for BOTH back in writing, with a full
// specification of each. They are his words, at his explicit written instruction.
//
// THIS IS THE THIRD TIME A SWEEP HAS FLAGGED THE OWNER'S OWN COPY. IF YOU ARE HERE BECAUSE OF THAT: DO NOT
// REWORD WHAT HE WROTE. EXTEND THE ALLOW LIST BELOW.
// =================================================================================================================
const OWNER_FLOW_PHRASES=['Sellers are building resistance','Sellers are building support',
 // his OI INTERPRETATION column, verbatim from the table he wrote
 'More resistance than support','Support strengthening','Resistance reducing + support increasing',
 'Resistance increasing + support weakening','No meaningful new positioning',
 // the rest of the closed interpretation table, which names no level either
 'More support than resistance','Resistance strengthening','Resistance reducing','Support weakening',
 'Both sides adding positions','Both sides reducing positions'];
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
ok(()=>{ // HIS INTERPRETATION COLUMN: the whole value passes, and the same value with a price does not
 for(const phrase of ['More resistance than support','Support strengthening',
  'Resistance reducing + support increasing','Resistance increasing + support weakening',
  'No meaningful new positioning'])
  assert.equal(levelForm(phrase),'',`the owner's own interpretation must pass: ${phrase}`);
 // the allowance is the WHOLE VALUE and never a substring - a price turns any of them into a level
 for(const text of ['Resistance increasing + support weakening at 23,500',
  'More resistance than support near 23,300','Support strengthening around 23,000',
  'Resistance reducing + support increasing above 23,500','More resistance than support at the 23,300 level'])
  assert.ok(levelForm(text),`his words turned into a price must still be refused: ${text}`);
 // and every value the interpretation table can actually produce is on the allow list
 for(const text of Object.values(L.SIGNAL_INTERPRETATIONS))
  assert.equal(levelForm(text),'',`the interpretation table must not name a level: ${text}`);
 for(const text of [L.SIGNAL_MORE_CALLS,L.SIGNAL_MORE_PUTS,L.SIGNAL_BALANCED])
  assert.equal(levelForm(text),'',`the both-building readings must not name a level: ${text}`);
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
 // The two PER-SIDE sentences are served by the server's own FLOW_LABELS, so both sides must carry them.
 // His INTERPRETATION values are read together in the browser from the very slots the ΔOI grid drew - the same
 // place gridBlockRead is computed - so they live in logic.ts and the server serves no such sentence. Both are
 // his wording and neither may be reworded; only one of them has a server side to check.
 const OWNER_SERVED_PHRASES=['Sellers are building resistance','Sellers are building support'];
 for(const phrase of OWNER_FLOW_PHRASES){
  assert.ok(client.includes(phrase),
   `"${phrase}" is the owner's own wording and must not be reworded - see the note above this check`);
  if(OWNER_SERVED_PHRASES.includes(phrase))
   assert.ok(DERIV_PY.includes(phrase),`derivatives.py must serve "${phrase}" verbatim`);
 }
 // and they really are the pair the flow table puts on a written-option reading
 assert.ok(/'CE\|down\|building':\('Call writing increasing','Sellers are building resistance'\)/.test(DERIV_PY));
 assert.ok(/'PE\|down\|building':\('Put writing increasing','Sellers are building support'\)/.test(DERIV_PY));
 const labels=Object.values(L.FLOW_LABELS).flat();
 for(const phrase of OWNER_SERVED_PHRASES)assert.ok(labels.includes(phrase),
  `the browser's flow table must carry "${phrase}" too, so both sides say the same thing`);
 // and his INTERPRETATION values really are the ones the interpretation table produces - not just strings
 // sitting in an allow list that nothing on screen uses
 const interpretations=[...Object.values(L.SIGNAL_INTERPRETATIONS),L.SIGNAL_MORE_CALLS,L.SIGNAL_MORE_PUTS];
 for(const phrase of ['More resistance than support','Support strengthening',
  'Resistance reducing + support increasing','Resistance increasing + support weakening',
  'No meaningful new positioning'])
  assert.ok(interpretations.includes(phrase),
   `the interpretation table must produce the owner's own "${phrase}"`);
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

// =================================================================================================================
// THE TAB MUST NOT LAND ON A DEAD READING - AND A FLOOR MUST NOT MAKE ONE DEAD
//
// Measured on the real store, 18 Sep 2026: nine readings from 09:30 to 11:30 with 157-491 contracts over the
// floors, then SEVENTEEN readings from 11:45 to 15:45 with none at all. The afternoon was rebuilt from 15-minute
// candles after the capture died, and a candle carries no traded-price average - so `premium_cr` is NULL for
// every contract in those readings.
//
// The tab defaulted to the newest reading. So the screener was empty, there was no row to click, and every block
// on the page stayed on its fallback index: "why other stocks are not populating".
//
// THE FIRST FIX was a DEFAULT, not a filter: open on the newest reading that HAS contracts over the floors, serve
// the facts about that choice, and say in plain words which reading is on screen. It left the tab four hours
// behind the close of a session whose last reading held 10,510 contracts with a real price, volume and open
// interest - because the premium floor was still being applied to a premium nobody had measured.
//
// THE SECOND FIX is that the floors DEGRADE. A floor is applied at a reading that measured the number it rests
// on and is NOT applied at one that did not; the rows are kept, and the response names the floors in force and
// why the others are not. "Premium was below 2 crore" is a reading of the market; "no premium was captured" is a
// gap in ours, and judging the second as the first is what emptied the screen.
//
// WHAT IS NOT DONE, and must not be: the store also carries an ESTIMATED traded price. Using it would let a
// model's number decide what the reader can see. The floors decide visibility, so the floors rest on what the
// exchange reported and on nothing else - and a floor that cannot rest on that is dropped ALOUD, never filled in.
// =================================================================================================================
ok(()=>{ // the server resolves the usable reading, next to the coverage it already computes
 assert.ok(/def _clears_floors\(self,at,names=None\):/.test(DERIV_PY),
  'derivatives.py must be able to ask whether ONE reading has anything over the floors');
 assert.ok(/def _usable_reading\(self,readings\):/.test(DERIV_PY),
  'and to walk back to the newest reading that has');
 // ONE BUILDER, so the probe, the floors-only count and the screener's own query cannot become three
 // definitions. The SQL lives in `_floor_clauses` and every one of the three calls it.
 assert.ok(/def _floor_clauses\(self,names,applied,min_premium_cr=None\):/.test(DERIV_PY),
  'the floor SQL must be built in one place');
 const builder=DERIV_PY.slice(DERIV_PY.indexOf('def _floor_clauses'),DERIV_PY.indexOf('def _clears_floors'));
 assert.ok(/premium_cr>=\?/.test(builder)&&/FLOOR_PREMIUM_CR/.test(builder),'the premium floor');
 assert.ok(/last_price>=\?/.test(builder)&&/FLOOR_LAST_PRICE/.test(builder),'the last-price floor');
 assert.ok(/oi>=lot_size\*\?/.test(builder)&&/FLOOR_OI_LOTS/.test(builder),
  'and the open-interest floor, worked out against the contract\'s own lot size exactly as _passes does');
 const probe=DERIV_PY.slice(DERIV_PY.indexOf('def _clears_floors'),DERIV_PY.indexOf('def _usable_reading'));
 assert.ok(/self\._floor_clauses\(names,force\['applied'\]\)/.test(probe),
  'the probe asks the builder, it does not write its own floors');
 assert.ok(/self\._floors_in_force\(at,names\)/.test(probe),
  'and asks which floors that reading can be measured against at all');
 assert.ok(/limit 1/.test(probe),'it stops at the first row: this is a question, not a count');
 // a floor is applied where its number was captured, and dropped ALOUD where it was not
 assert.ok(/def _floors_in_force\(self,at,names=None\):/.test(DERIV_PY),
  'derivatives.py must resolve which floors a reading can be measured against');
 for(const key of ['floors_applied','floors_unmeasured','floors_degraded','floors_unmeasured_text'])
  assert.ok(new RegExp(`'${key}'`).test(DERIV_PY),`the response must carry ${key}`);
 assert.ok(/FLOOR_UNMEASURED_TEXT=\{/.test(DERIV_PY),'with one sentence per floor, never one shared sentence');
 assert.ok(/FALLBACK_RANK_FIELD='volume'/.test(DERIV_PY),
  'and a list whose premium is null is ranked by something that was measured');
 // and it rests on nothing a model produced
 assert.ok(!/average_price_est/.test(DERIV_PY),
  'a number that decides what the reader SEES must be one the exchange reported, never an estimate');
});
ok(()=>{ // the screener opens on it, and an explicitly chosen reading is never overridden
 const fn=DERIV_PY.slice(DERIV_PY.indexOf('def screener(self,filters=None'),
  DERIV_PY.indexOf('def _screener_row'));
 assert.ok(/asked_for_reading=bool\(at\)/.test(fn),'the server knows whether the reader chose the reading');
 assert.ok(/at,skipped=self\._usable_reading\(readings\)/.test(fn),
  'and resolves the usable one ONLY when they did not');
 const chosen=fn.indexOf("if at:"),resolved=fn.indexOf('self._usable_reading(readings)');
 assert.ok(chosen>0&&chosen<resolved,'a reading the reader named is honoured before anything else is tried');
 assert.ok(/if not at:at=newest/.test(fn),
  'and a store with no reading list still falls back to its newest metric row, as it always did');
 // the facts the browser needs, served rather than guessed at
 for(const key of ['newest_at','reading_at','reading_is_newest','reading_chosen','reading_skipped','reading_rule'])
  assert.ok(new RegExp(`'${key}'`).test(DERIV_PY),`the response must carry ${key}`);
 assert.ok(/READING_RULE_TEXT=\(/.test(DERIV_PY),'and the RULE itself is one served sentence');
 assert.ok(!MARK_WORD.test(pySentence('READING_RULE_TEXT')),
  'which says "15-min reading", never "mark"');
 const banned=/\b(will|expect|forecast|predict|likely|bullish|bearish)\b/i;
 assert.ok(!banned.test(pySentence('READING_RULE_TEXT')),'and predicts nothing');
});
ok(()=>{ // the BROWSER says which floors the list was gated on, whenever that is not all three
 // A reader who cannot see this takes a two-floor list for a three-floor one, which is the same mistake in
 // the other direction from the one that emptied the screen.
 assert.equal(L.floorsDegradedNote(null),'');
 assert.equal(L.floorsDegradedNote({floors_degraded:false,floors_unmeasured:['premium_cr']}),'',
  'a reading that measured every floor has nothing to caveat');
 assert.equal(L.floorsDegradedNote({floors_degraded:true,floors_unmeasured:[],floors_absent:[]}),'',
  'and nothing is said when nothing is actually missing');
 const note=L.floorsDegradedNote({floors_degraded:true,floors_applied:['oi','last_price'],
  floors_unmeasured:['premium_cr'],floors_absent:[],
  floors_labels:{premium_cr:'premium traded \u2265 \u20b92 cr',oi:'OI \u2265 1 lot',last_price:'last price \u2265 \u20b91'}});
 assert.ok(/2 of the 3 liquidity floors/.test(note),'it says how many of the three were in force');
 assert.ok(note.includes('OI \u2265 1 lot')&&note.includes('last price \u2265 \u20b91'),'and names them');
 assert.ok(/Not applied here: premium traded/.test(note),'and names the one that was not');
 assert.ok(/was not captured/.test(note),'with the reason, which is a gap in our capture');
 assert.ok(/no contract in the list failed it/.test(note),
  'and the distinction that matters: nothing here failed a floor it was never measured against');
 assert.ok(!/mark/i.test(note),'in the owner\u2019s words, not ours');
 assert.ok(!/\b(will|expect|forecast|predict|likely|bullish|bearish)\b/i.test(note),'and it predicts nothing');
 // the bar actually prints it, beside the reading note and in the same amber
 const rail=SRC('ScreenerSection.tsx');
 assert.ok(/const floorNote=floorsDegradedNote\(body\)/.test(rail),'the bar works it out from the response');
 assert.ok(/\{!!floorNote&&<View role="note"/.test(rail),'and prints it, on the bar, above the rows');
});
ok(()=>{ // the BROWSER says which reading is on screen, and only when that is worth saying
 const dash=L.DASH;
 // on the newest reading there is nothing to say
 assert.equal(L.readingNote({reading_is_newest:true,reading_at:'2026-09-18 15:45',newest_at:'2026-09-18 15:45'}),'');
 assert.equal(L.readingNote(null),'');
 assert.equal(L.readingNote({}),'');
 // the fallback: the reading, the fact that it is NOT the newest, the newest, and why
 const note=L.readingNote({reading_is_newest:false,reading_chosen:false,reading_skipped:17,
  reading_at:'2026-09-18 11:30:00',newest_at:'2026-09-18 15:45:00'});
 assert.ok(note.includes('18 Sep 2026 · 11:30'),'it names the reading on screen');
 assert.ok(/NOT the newest/.test(note),'it says plainly that this is not the newest');
 assert.ok(note.includes('18 Sep 2026 · 15:45'),'and names the one it is not');
 assert.ok(/17 readings after it/.test(note),'and says how many newer readings hold nothing over the floors');
 assert.ok(/Any reading can be chosen/.test(note),'and that the reader can still go to any of them');
 assert.ok(!/mark/i.test(note),'in the owner\'s words, not ours');
 // a reading the READER chose is described as their own choice, never as a fallback
 const picked=L.readingNote({reading_is_newest:false,reading_chosen:true,reading_skipped:0,
  reading_at:'2026-09-18 09:45:00',newest_at:'2026-09-18 15:45:00'});
 assert.ok(/the one chosen above/.test(picked)&&!/NOT the newest/.test(picked),
  'a chosen reading is not a fallback and must not be reported as one');
 assert.ok(picked.includes('The newest this store holds is 18 Sep 2026 · 15:45'));
 // and only the fallback is treated as a caveat on the data
 assert.equal(L.readingIsFallback({reading_is_newest:false,reading_chosen:false}),true);
 assert.equal(L.readingIsFallback({reading_is_newest:false,reading_chosen:true}),false);
 assert.equal(L.readingIsFallback({reading_is_newest:true}),false);
 assert.equal(L.readingIsFallback(null),false);
 assert.equal(L.readingLabel(null),'');
 assert.equal(L.readingLabel('2026-09-18 11:30:00'),'18 Sep 2026 · 11:30');
 assert.notEqual(L.readingLabel('2026-09-18 11:30:00'),dash);
});
ok(()=>{ // and the pinned bar really puts it where the reader is looking
 const rail=SRC('ScreenerSection.tsx');
 assert.ok(/const note=readingNote\(body\)/.test(rail)&&/const fallback=readingIsFallback\(body\)/.test(rail),
  'the bar works the note out from the facts the server served');
 assert.ok(/\{!!note&&<View role="note"/.test(rail),'and prints it, on the bar, above the rows');
 assert.ok(/backgroundColor:fallback\?C\.amberBg:C\.dark/.test(rail),
  'a tab that moved itself off the newest reading is AMBER: the newest state of the book is not on screen');
 assert.ok(/String\(body\?\.reading_rule\|\|''\)/.test(rail),
  'and the rule behind the choice is in the one "How to read this"');
 // the reading control is still there, and it still reaches every reading of the session
 assert.ok(/readings=\{readings\}/.test(rail)&&/onAt=\{onAt\}/.test(rail),
  'the reader can still move to any reading, the empty ones included');
 assert.ok(/readingChoices\(body\)/.test(rail),'from the list the server served');
});

// =================================================================================================================
// INTERACTION: a terminal answers the reader
//
// "Hovering a chart should tell you the values at that moment." Four blocks draw a time series against the same
// session grid and a reader could see the shape and not one number on it. And the grid, the chain and the
// volatility list are all about the same strikes and shared no visual link at all.
//
// What must hold while they do it: a crosshair READS the series, it never fills one in. A slot the store has no
// value for still reads as a dash under the pointer, and no ring is drawn where nothing was captured.
// =================================================================================================================
ok(()=>{ // one hover mechanism, built once
 const frame=SRC('frame.tsx');
 assert.ok(/export function useHoverIndex\(/.test(frame),'pointer-to-index arithmetic lives in one place');
 assert.ok(/native\?\.pointerType==='touch'/.test(frame),
  'a finger has no hover, and a tap is a different gesture');
 assert.ok(/requestAnimationFrame/.test(frame),'at most one update per frame');
 assert.ok(/Platform\.OS!=='web'/.test(frame),'and nothing at all off the web');
 assert.ok(/export function CrosshairReadout\(/.test(frame),'and one readout shape for every chart that has one');
 // every chart on the tab that draws a series can be read off
 for(const file of ['SessionPanel.tsx','ChartTile.tsx','OiByStrikeSection.tsx']){
  const code=SRC(file);
  assert.ok(/useHoverIndex\(/.test(code),`${file} must let the reader read a value off it`);
  assert.ok(/<CrosshairReadout/.test(code),`${file} must show what is under the pointer`);
 }
});
ok(()=>{ // the crosshair reads the series - it never bridges a gap
 const panel=SRC('SessionPanel.tsx');
 assert.ok(/value:line\.format\(\(line\.values\|\|\[\]\)\[cursor as number\]\)/.test(panel),
  'the readout takes the series\' own value at that reading, through the series\' own formatter');
 // logic.ts already guarantees that formatter turns a null into a dash - the same rule as everywhere else
 assert.equal(L.pcrText(null),L.DASH);
 assert.equal(L.compact(undefined),L.DASH);
 assert.ok(/const point=scaled\?\.points\?\.\[cursor as number\];\s*\n?\s*return point\?/.test(panel.replace(/\r/g,'')),
  'and a ring is drawn only where the line HAS a point: nothing is drawn where nothing was captured');
 assert.ok(/time=\{clock\(times\[cursor as number\]\)\}/.test(panel),
  'the readout names the reading it is reading, so a value can never be read against the wrong one');
 // the index can never point outside the readings actually drawn
 assert.ok(/Math\.max\(0,Math\.min\(count-1,/.test(panel),'the cursor is clamped to the series');
});
ok(()=>{ // the same strike is lit wherever it is on the tab
 const page=SRC('index.tsx');
 assert.ok(/const \[strikeHover,setStrikeHover\]=useState<number\|null>\(null\)/.test(page),
  'the tab holds ONE hovered strike, as it holds one target');
 for(const tag of ['<ChainWidget','<OiByStrikeSection','<OiGridSection','<IvSection'])
  assert.ok(new RegExp(`${tag}[\\s\\S]{0,900}?highlight=\\{strikeHover\\}`).test(page),
   `${tag} must be given the hovered strike`);
 // every one of them lights it the same way, and NONE of them acts on it
 for(const file of ['ChainWidget.tsx','OiByStrikeSection.tsx','OiGridSection.tsx','SessionBlocks.tsx']){
  const code=SRC(file);
  assert.ok(/highlight/.test(code),`${file} must take the hovered strike`);
  assert.ok(/C\.mint/.test(code),`${file} must light it in the one highlight colour`);
  assert.ok(!/onTarget\([\s\S]{0,80}highlight/.test(code),
   `${file} must not let a HOVER change what the tab is pointed at`);
 }
 // and it is said, once, in a block's own words
 assert.ok(/It highlights; it changes nothing\./.test(SRC('OiGridSection.tsx')));
 assert.ok(/It highlights; it changes nothing\./.test(SRC('OiByStrikeSection.tsx')));
});
ok(()=>{ // anything clickable looks clickable, and the keyboard lands exactly where the mouse does
 const table=SRC('Table.tsx');
 assert.ok(/onHoverIn=\{\(\)=>onRowHover\?\.\(row\)\}/.test(table)&&/onFocus=\{\(\)=>onRowHover\?\.\(row\)\}/.test(table),
  'a row reports itself on hover AND on focus: a keyboard reader gets the same link a mouse does');
 assert.ok(/borderLeftColor:on\|\|st\.focused\?C\.green:glow\?C\.mint:'transparent'/.test(table),
  'and the rail says which of the three states the row is in');
 assert.ok(/tabIndex:0/.test(table),'rows are reachable by keyboard');
 // the same three states on the grid tiles and on the chain
 assert.ok(/borderWidth:selected\|\|st\.focused\?2:1/.test(SRC('OiGridSection.tsx')),
  'a focused tile is as loud as a selected one');
 assert.ok(/borderColor:selected\|\|st\.focused\?C\.green:lit\?C\.mint:C\.line/.test(SRC('OiGridSection.tsx')));
 assert.ok(/borderWidth:selected\|\|st\.focused\?2:1/.test(SRC('ChainWidget.tsx')),
  'and so is a focused leg of the chain');
});


// =================================================================================================================
// ONE ROW PER INSTRUMENT
//
// The owner: "Just show one row per instrument - like nifty show one row, stock show one row."
//
// The screener listed CONTRACTS, largest premium traded first. At the 11:30 reading of 18 Sep 2026, 85
// instruments had a contract over the liquidity floors and NIFTY alone held 104 of the 491 contracts - so a
// hundred-row list was a hundred rows of NIFTY, and the reader's conclusion was the obvious one: no stock is
// active. That is "why other stocks are not populating".
//
// What these checks hold is the part that can quietly go wrong once a row IS an instrument: AN AGGREGATE THAT
// IS NOT HONEST. A sum of rupees is a sum. A mean of ratios is not a ratio, and none is served.
// =================================================================================================================
ok(()=>{ // the aggregate is built on the server, over every row that cleared - never in the browser
 assert.ok(/def _screener_groups\(self,rows\):/.test(DERIV_PY),
  'derivatives.py must build the per-instrument rows itself');
 // EVERY INSTRUMENT TYPE. Options-only hid 129 of the 214 names that clear the floors, which is the same bug
 // this view exists to fix, one step further down.
 assert.ok(/INSTRUMENT_TYPES=\('CE','PE','FUT'\)/.test(DERIV_PY),'calls, puts and futures');
 const screener=DERIV_PY.slice(DERIV_PY.indexOf('def screener(self,filters'),
  DERIV_PY.indexOf('def _screener_row'));
 assert.ok(!/clauses\.append\("instrument_type in \('CE','PE'\)"\)/.test(screener),
  'the screener must not narrow itself to options: a futures-only name is a name where something is happening');
 // ...and a future has no moneyness, which left alone would be computed from strike 0 and look like a bucket
 assert.ok(/if str\(option_type or ''\)\.upper\(\) not in OPTION_TYPES:return None/.test(DERIV_PY),
  'a future has no moneyness and must not be given one');
 const fn=DERIV_PY.slice(DERIV_PY.indexOf('def _screener_groups'),DERIV_PY.indexOf('def screener(self,filters'));
 // the SUMS: things that add
 for(const key of ['premium_cr','volume','oi','oi_change_day','oi_change_15m'])
  assert.ok(new RegExp(`group\\['${key}'\\]\\+=`).test(fn),`${key} is summed`);
 // the COUNTS
 for(const key of ['contracts','calls','puts','volume_baseline_contracts','volume_to_oi_over_1'])
  assert.ok(new RegExp(`group\\['${key}'\\]\\+=1`).test(fn),`${key} is a count of contracts`);
 // AND THE REFUSALS. Not one of these may be averaged anywhere in the method.
 assert.ok(!/sum\(.*volume_ratio/.test(fn)&&!/\/len\(/.test(fn),
  'nothing in the aggregate is divided by a count: a mean of ratios is not a ratio');
 assert.ok(/group\['volume_ratio_max'\] is None or ratio>group\['volume_ratio_max'\]/.test(fn),
  '§3.2 is the LARGEST reading among the instrument\'s contracts');
 assert.ok(/group\['volume_ratio_max_symbol'\]=row\.get\('tradingsymbol'\)/.test(fn),
  'and the contract it belongs to is named, so it can never be read as the instrument\'s own');
 assert.ok(/if row\['volume_to_oi'\]>1:group\['volume_to_oi_over_1'\]\+=1/.test(fn),
  '§3.3 is a COUNT over the tab\'s own existing threshold');
 // AND IT TRAVELS WITH ITS DENOMINATOR. The store computes neither §3.2 nor §3.3 for a futures contract, so
 // an instrument whose whole book is futures has nothing to count - and a zero would read as "nothing
 // unusual" when the truth is "not measured".
 assert.ok(/group\['volume_to_oi_contracts'\]\+=1/.test(fn),'and it says what it was counted over');
 assert.ok(/if not group\['volume_to_oi_contracts'\]:group\['volume_to_oi_over_1'\]=None/.test(fn),
  'nothing to count is None, never 0');
 assert.ok(/group\['buildup_counts'\]\[label\]=/.test(fn),'§3.1 is counts per label');
 // the calls/puts split is the OPTIONS half; a future is neither side of anything
 assert.ok(/if kind=='CE':group\['calls'\]\+=1;group\['options'\]\+=1/.test(fn));
 assert.ok(/elif kind=='PE':group\['puts'\]\+=1;group\['options'\]\+=1/.test(fn));
 assert.ok(/else:group\['futures'\]\+=1/.test(fn),'and a futures contract is counted as one');
 assert.equal(L.groupSplit({calls:45,puts:59,options:104,futures:2}),'45C / 59P · 2F');
 assert.equal(L.groupSplit({calls:0,puts:0,options:0,futures:2}),'futures only · 2F',
  'a name with no listed options must NOT read as 0C / 0P: that would say its options were quiet');
 assert.equal(L.groupSplit({calls:8,puts:9,options:17,futures:0}),'8C / 9P');
 assert.equal(L.groupHot({volume_to_oi_over_1:null,volume_to_oi_contracts:0}),DASH,
  'nothing to count is a dash, never a zero');
 assert.equal(L.groupHot({volume_to_oi_over_1:67,volume_to_oi_contracts:104}),'67');
 assert.equal(L.groupOptionsNote({options:0,contracts:2}),'no options listed');
 assert.equal(L.groupOptionsNote({options:17,contracts:19}),'of 17 options');
 assert.equal(L.groupOptionsNote({options:19,contracts:19}),'of 19');
 assert.ok(L.groupHotText({volume_to_oi_over_1:null,volume_to_oi_contracts:0}).includes('not computed for a futures contract'),
  'and it says why rather than leaving a dash unexplained');
 // a build-up LABEL for a whole instrument does not exist and must not be invented
 assert.ok(!/group\['buildup_day'\]|group\['buildup'\]=/.test(fn),
  'there is no such thing as an instrument\'s build-up label');
 // spot is passed through, and only when the rows AGREE on it
 assert.ok(/group\['spot'\]=None;group\['spot_disagrees'\]=True/.test(fn),
  'a spot the rows disagree on is served as nothing, not as one of the two');
});
ok(()=>{ // the tab asks for that list, and shows the reader no other
 const page=CODE('index.tsx'),widget=SRC('UnusualWidget.tsx'),rail=SRC('ScreenerSection.tsx');
 assert.ok(/group:SCREENER_VIEW_DEFAULT/.test(page),'the page asks for one row per instrument, by name');
 assert.equal(L.SCREENER_VIEW_DEFAULT,'underlying');
 // NO view control anywhere the reader can reach: one list, full stop
 for(const piece of ['onView','onDrill','All underlyings','Show:'])
  assert.ok(!widget.includes(piece),`the screener must not offer a mode switch (${piece})`);
 assert.ok(!/onView|onDrill|view=\{/.test(rail),'and neither must the bar it sits on');
 // which table is drawn is the SERVER's statement about what it served, not a setting
 assert.ok(/const grouped=\(body\?\.view\|\|'underlying'\)==='underlying'/.test(widget),
  'the table draws what the server says it served');
 // clicking a row points the tab, and does nothing else
 const pick=widget.slice(widget.indexOf('const pickGroup='),widget.indexOf('const contractColumns'));
 assert.ok(/onTarget\(\{underlying:row\.underlying/.test(pick),'a row click points the whole tab at it');
 assert.ok(!/setView|onDrill|scrollTo/.test(pick),'and does not open a second list inside the screener');
});
ok(()=>{ // the columns answer "is something happening in this name", and the two that cannot be summed say so
 const widget=SRC('UnusualWidget.tsx');
 const block=widget.slice(widget.indexOf('const groupColumns'),widget.indexOf('],[onCustomize]);',
  widget.indexOf('const groupColumns')));
 const labels=[...block.matchAll(/label:'([^']+)'/g)].map(m=>m[1]);
 for(const want of ['Instrument','Contracts','Premium ₹cr','OI chg (day)'])
  assert.ok(labels.includes(want),`the list must carry ${want}`);
 // the honest roll-ups are LABELLED as what they are, in the header the reader reads
 assert.ok(labels.some(l=>/highest/i.test(l)),
  'the §3.2 column must say in its own header that it is the highest reading, not an average');
 assert.ok(labels.some(l=>/over 1/i.test(l)),'and the §3.3 column that it is a count');
 // no column may present a per-contract signal as the instrument's own
 for(const banned of ['Build-up','Strike','Vol/OI','Moneyness'])
  assert.ok(!labels.includes(banned),`${banned} is a statement about one contract and must not head a column`);
 assert.ok(/groupSplit\(r\)/.test(block),
  'what the book is made of is on the row, through the one helper that knows a future is not a side');
});
ok(()=>{ // the sentences that explain the list, and what it refuses to compute
 const rail=SRC('ScreenerSection.tsx');
 for(const name of ['SCREENER_GROUP_WHAT','SCREENER_GROUP_SUMS','SCREENER_GROUP_REFUSED','SCREENER_GROUP_DRILL'])
  assert.ok(new RegExp(`export const ${name}=`).test(rail),`the bar must declare ${name}`);
 const refused=/export const SCREENER_GROUP_REFUSED='([^']*)'/.exec(rail);
 assert.ok(refused,'the refusal must be one plain sentence');
 assert.ok(/mean of ratios is not a ratio/.test(refused[1]),'and it must say why');
 // The REQUIREMENT is that the sentence says neither signal exists for a futures contract. "Neither is
 // computed by the store for a FUTURES contract" and "not computed by the store for a FUTURES contract" both
 // say it; pinning one of the two phrasings pinned the grammar rather than the claim.
 assert.ok(/computed by the store for a FUTURES contract/.test(refused[1]),
  'and that neither signal exists for a futures contract at all');
 assert.ok(/dash rather than a nought/.test(refused[1]),'and what that means on a futures-only row');
 assert.ok(/\{text:SCREENER_GROUP_REFUSED,tone:'amber' as const\}/.test(rail),
  'a signal the tab will NOT compute is a caveat on the data, so it is amber');
 assert.ok(/heading:'One row per instrument'/.test(rail),'and it is all behind the one control');
 // the formatters never average either
 assert.equal(L.groupVolumeRatioMax({volume_ratio_max:null}),L.NO_BASELINE);
 assert.equal(L.groupVolumeRatioMax({volume_ratio_max:2.44}),`2.4${TIMES}`);
 assert.ok(L.groupVolumeRatioText({volume_ratio_max:2.44,volume_ratio_max_symbol:'NIFTY26SEP23300CE',
  volume_baseline_contracts:98,contracts:104}).includes('not an average'),
  'the spoken form says in words that it is one contract\'s reading');
 assert.ok(L.groupVolumeRatioText({volume_ratio_max:null,volume_baseline_contracts:0,contracts:9})
  .includes('no ratio to show'),'and with no baseline at all it says that instead of a number');
 assert.equal(L.groupContracts({contracts:104,calls:45,puts:59}),'104 contracts · 45C / 59P');
 assert.equal(L.groupContracts(null),DASH);
 // §3.3 counts over the contracts that CARRY the ratio, not over the whole book: the store computes it for no
 // futures contract, so the denominator is the options that had one. A count over 104 when only 98 could
 // carry it would be a count over six contracts that were never measured.
 assert.ok(L.groupHotText({volume_to_oi_over_1:67,volume_to_oi_contracts:98,contracts:104}).includes('67 of the 98'),
  'the §3.3 count names the contracts it was counted over');
 assert.ok(L.groupHotText({volume_to_oi_over_1:null,volume_to_oi_contracts:0,contracts:104})
  .includes('nothing to count'),
  'and a book no contract of which carries the ratio says so rather than counting zero');
 assert.equal(L.groupBuildupText({buildup_counts:{long_buildup:41,short_buildup:32}}),
  'Long build-up 41 · Short build-up 32');
 assert.equal(L.groupBuildupText({buildup_counts:{}}),'');
});

// --- the option chain opens AT THE MONEY --------------------------------------------------------------------
ok(()=>{ // the reading the tab is on reaches the blocks that need a spot
 const page=SRC('index.tsx');
 assert.ok(/const reading=screener\.data\?\.reading_at\|\|screener\.data\?\.coverage\?\.at\|\|''/.test(page),
  'the page knows which 15-min reading its screener resolved');
 assert.ok(/<ChainWidget[\s\S]{0,200}at=\{reading\}/.test(page),'and the chain is read at it');
 assert.ok(/<OiByStrikeSection[\s\S]{0,200}at=\{reading\}/.test(page),'and so are the strikes');
 // the server: a NAMED reading must never go through the delegate, which has no parameter for one
 const fn=DERIV_PY.slice(DERIV_PY.indexOf('def chain(self,underlying'),DERIV_PY.indexOf('def _chain_metrics'));
 assert.ok(/rows,delegated=\(\(\[\],False\) if at/.test(fn),
  'a named reading reads the store directly: the delegate answers for the newest and would silently return it');
 assert.ok(/spot=self\._spot\(underlying,as_of\)/.test(fn),'and the spot comes from THAT reading');
});
ok(()=>{ // where the chain lands, and what it says when it cannot land anywhere
 const chain=SRC('ChainWidget.tsx');
 assert.ok(/const ROW_H=\d+;/.test(chain),'one row height, so the offset can be worked out at all');
 assert.ok(/height:ROW_H/.test(chain),'and every row really takes it');
 assert.ok(/const atmIndex=useMemo\(\(\)=>nearestStrikeIndex\(rows,body\?\.spot\)/.test(chain),
  'the anchor is the strike nearest the captured spot');
 assert.ok(/useEffect\(\(\)=>\{jump\(false\)\}/.test(chain),'the chain lands there on arrival');
 assert.ok(/scrollTo\?\.\(\{y:chainStartRow\(rows,body\?\.spot\)\*ROW_H/.test(chain),'at the computed offset');
 assert.ok(/\{atmIndex==null\?CHAIN_NO_SPOT_TEXT:CHAIN_AT_MONEY_TEXT\}/.test(chain),
  'and it says which of the two it did');
 // NOTHING is dropped or reordered to achieve it
 assert.ok(!/\.slice\(/.test(chain)&&!/\.filter\(/.test(chain),
  'the whole ladder is still listed: the chain is not windowed, only scrolled');
 // the arithmetic itself
 const rows=[{strike:21350},{strike:21400},{strike:23300},{strike:23350},{strike:23400}];
 assert.equal(L.chainStartRow(rows,23302),0,'three rows of lead, clamped at the top of the list');
 assert.equal(L.chainStartRow(rows,23302,0),2,'with no lead it is the at-the-money row itself');
 assert.equal(L.chainStartRow(rows,null),0,'no spot, no anchor - it opens where it always did');
 assert.equal(L.chainStartRow([],23302),0);
 assert.ok(L.CHAIN_NO_SPOT_TEXT.includes('No spot was captured'),
  'and a reading with no spot says so rather than marking the wrong strike');
 assert.ok(!MARK_WORD.test(L.CHAIN_AT_MONEY_TEXT)&&!MARK_WORD.test(L.CHAIN_NO_SPOT_TEXT));
});

// --- the IV block is a 2 x 5 grid of the SAME ten contracts the ΔOI grid draws ---------------------------------------
// The owner asked to read volatility and open interest on the same ten contracts, laid out identically, so the two
// blocks can be compared tile for tile. What is pinned here is the ten slots and their ORDER, that the tile height
// is the ΔOI tile's own rather than a copy of it, that every tile carries the COMPUTED marking, that a tile the
// model could not solve prints the reason instead of a blank, and that an unsolved reading is a HOLE in the line.
ok(()=>{ // ten slots, one order, and a slot the server did not send is still a slot
 const grid=SRC('IvGridSection.tsx');
 const order=/export const IV_GRID_ORDER=\[([^\]]+)\] as const;/.exec(grid);
 assert.ok(order,'the ten slots are declared in ONE place');
 const keys=(order[1].match(/'[^']+'/g)||[]).map(t=>t.slice(1,-1));
 assert.deepEqual(keys,['CE+0','CE+1','CE+2','CE+3','CE+4','PE+0','PE-1','PE-2','PE-3','PE-4'],
  'calls at the money and above, then puts at the money and below — the ΔOI grid\'s own order');
 assert.equal(keys.length,10,'exactly ten');
 // the same ten the ΔOI block builds: its expected list, read out of the server, is that same sequence
 const py=fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','derivatives.py'),'utf8');
 assert.ok(/expected=\[\('CE',n\) for n in range\(GRID_WIDTH\+1\)\]\+\[\('PE',0\)\]\+\[\('PE',-n\) for n in range\(1,GRID_WIDTH\+1\)\]/
  .test(py),'and the server builds the ΔOI ladder in that same order');
 // the grid is built FROM that list, not from whatever arrived, so nothing shifts under the reader
 assert.ok(/return IV_GRID_ORDER\.map\(key=>\{/.test(grid),
  'the ten tiles are laid out from the fixed order, not from the response');
 assert.ok(/missing_text:underlying\?IV_GRID_NO_LADDER:''/.test(grid),
  'and a slot with nothing behind it still says why it is empty');
 assert.ok(/present:false/.test(grid),'an unlisted strike keeps its slot');
 // The ladder is the ΔOI endpoint's: ONE resolution of "at the money" on this tab, not two - and it is now
 // RECEIVED rather than fetched. The page reads it once and hands it to all three panels built on those ten
 // contracts, so this grid's eleventh request is gone and the two grids still cannot be about different
 // strikes. The ten per-strike IV reads below it are inherent; a second ladder was not.
 assert.ok(/ladder:Read<OiGrid>/.test(CODE('IvGridSection.tsx')),
  'the at-the-money ladder is handed down, so the two grids cannot be about different strikes');
 assert.ok(/ladder=\{gridRead\}/.test(CODE('SessionBlocks.tsx')),
  'and the IV block passes the tab\'s one grid read into it');
 // two bands, five each
 assert.ok(/slots:slots\.slice\(0,IV_GRID_SLOTS\/2\)/.test(grid)&&/slots:slots\.slice\(IV_GRID_SLOTS\/2\)/.test(grid),
  'calls on the first row, puts on the second');
 assert.ok(/IV_GRID_CALLS_LABEL='Calls — at the money and above'/.test(grid)
  &&/IV_GRID_PUTS_LABEL='Puts — at the money and below'/.test(grid),'each row named as the ΔOI grid names it');
});
ok(()=>{ // the tile is the ΔOI tile's size, by import rather than by copy
 const grid=SRC('IvGridSection.tsx');
 assert.ok(/import \{TILE_H,gridColumns\} from '\.\/OiGridSection';/.test(grid),
  'the IV tile takes its height and its wrap points FROM the ΔOI grid, so the two cannot drift apart');
 assert.equal((grid.match(/height:TILE_H/g)||[]).length,1,'and exactly one place sets it');
 assert.ok(/width,height:TILE_H,padding:CELL_PAD/.test(grid),'which is the tile itself');
 // the chart is the REMAINDER of that height, so "both rows are the same height" is arithmetic, not a promise
 assert.ok(/export const IV_CHART_H=TILE_H-\(CELL_PAD\*2\+BORDER\+ROW_GAP\*5\+TITLE_H\+DETAIL_H\+CHIP_H\+STATE_H\+REASON_H\);/
  .test(grid),'every zone is fixed and the chart takes what is left');
 // and no zone of the tile is left to the text inside it
 for(const zone of ['TITLE_H','DETAIL_H','CHIP_H','STATE_H','REASON_H'])
  assert.ok(new RegExp(`height:${zone}`).test(grid),`${zone} is a reserved box, whatever its label says`);
 // the direction words are the owner's, and they are the block's own vocabulary rather than a new one
 assert.deepEqual(Object.values(L.IV_CHIPS),['↑ EXPANDING','→ STABLE','↓ COOLING'],
  'Expanding · Stable · Cooling');
 assert.ok(/sessionChip\(IV_CHIPS,direction\)/.test(grid)&&/sessionTone\(IV_CHIPS,direction\)/.test(grid),
  'and the tile spends them, rather than hardcoding a fourth set');
 assert.ok(/servedDirection\(leg\?\.direction,values,IV_KEYS\)/.test(grid),
  "the server's word wins, and the points the tile drew answer when it sent none");
});
ok(()=>{ // a gap is a gap: an unsolved reading is never interpolated, carried forward or drawn at zero
 const grid=CODE('IvGridSection.tsx');
 assert.ok(/if\(typeof v!=='number'\|\|!Number\.isFinite\(v\)\)return null;/.test(grid),
  'a reading with no volatility maps to null, not to zero');
 assert.ok(/points:values\.map\(\(v,i\)=>v==null\?null:\{x:i\*step/.test(grid),
  'and a null value is a null point, so nothing is drawn where nothing was solved');
 assert.ok(!/fill\(0\)|\?\?0|\|\|0\b/.test(grid.split('scaleIv')[1].split('export function ivAxis')[0]),
  'nothing in the scaler substitutes a zero for a missing reading');
 assert.ok(/d=\{linePath\(scaled\)\}/.test(grid),'the line is drawn by the one path builder that breaks at gaps');
 // the builder itself, on a series with a hole in it
 const scaled={lo:18,hi:20,points:[{x:0,y:10,i:0},null,{x:20,y:4,i:2},{x:30,y:2,i:3}]};
 const d=L.linePath(scaled);
 assert.ok(!d.includes('NaN'));
 assert.equal((d.match(/M/g)||[]).length,2,'the path breaks at the gap and starts again after it');
 // the x labels come from readings that were captured, so a hole is never relabelled as a time
 assert.ok(/axisTimes\(points as any,3\)/.test(grid));
});
ok(()=>{ // a tile that could not be solved prints the REASON, and nothing on a tile is a forecast
 const grid=SRC('IvGridSection.tsx');
 // the five reasons a solver returns, in the server's own sentences, reached from the tile's refusal chain
 for(const key of ['stale_trade','no_time_value','below_intrinsic','no_convergence','expiry_today'])
  assert.ok(L.IV_REASONS[key]&&L.IV_REASONS[key].startsWith('No volatility:'),
   `${key} has a sentence a tile can print`);
 assert.ok(/ivLatestRefusal\(points,body\?\.reason_text\)/.test(grid),
  "a tile asks the server for its reason first");
 assert.ok(/\|\|String\(slot\.missing_text\|\|''\)\|\|\(read\.phase==='loading'\?'':IV_GRID_NO_VALUE\)/.test(grid),
  'and falls through to the slot\'s own reason, then to a sentence — never to a blank');
 assert.ok(/value==null\?'No volatility at the latest reading':'Solved at the latest reading'/.test(grid),
  'the tile says outright whether its latest reading carries a number');
 // every sentence this file owns is past tense and about what was captured
 const code=CODE('IvGridSection.tsx');
 for(const word of ['forecast','predict','bullish','bearish','support level','resistance','will be','expected to'])
  assert.ok(!new RegExp(word,'i').test(code),`"${word}" has no place on an IV tile`);
 // "mark" is OUR word for a 15-min reading and the reader never meets it. The stored FIELD names keep it
 // (`marks_with_delta` is the server's), so this is held against what reaches the screen, not the code.
 for(const text of VISIBLE(grid))
  assert.ok(!MARK_WORD.test(text),`IvGridSection.tsx: "mark" is our word, not the reader's: "${text}"`);
 // the highlight is a highlight and nothing else, said in the block's own words
 assert.ok(/It highlights; it changes nothing\./.test(grid));
 assert.ok(/onHighlight\?\.\(slot\.strike==null\?null:Number\(slot\.strike\)\)/.test(grid),
  'pointing at a tile lights that strike wherever else it is on the tab');
 assert.ok(!/onPick\([\s\S]{0,80}highlight/.test(code),'and a hover never re-points the tab');
});

// =================================================================================================================
// THE SIGNAL TABLE (the owner's TIME | CALL ACTIVITY | PUT ACTIVITY | OI INTERPRETATION | MARKET SIGNAL)
//
// This is the one panel on the tab a reader could mistake for a trading signal, so it is the one that must be
// pinned hardest. What is guarded here: his signal rule exactly as he wrote it; that the strength words are
// MEASURED against the instrument's own session rather than against a constant somebody picked; that no row is
// invented for a reading with no data; and that every row of his source vocabulary really is reachable.
// =================================================================================================================
/** "2026-09-18 09:15" and onwards. REAL stamps, because the window is now found BY the stamps: a fixture whose
 *  times do not parse would quietly test nothing at all. */
const sigStamp=(i,start=9*60+15,step=15)=>{const m=start+i*step;
 return `2026-09-18 ${String(Math.floor(m/60)).padStart(2,'0')}:${String(m%60).padStart(2,'0')}`};
/** A slot of `n` readings whose ΔOI walks by `step` each reading and whose premium walks by `priceStep`. */
const sigSlot=(row,type,n,step,priceStep=0,from=0,times=null)=>({slot:`${type}+0`,option_type:type,row,present:true,
 strike:23300,instrument_token:1,points:Array.from({length:n},(_,i)=>({at:times?times[i]:sigStamp(i),
  oi:null,delta_oi:from+step*i,price:100+priceStep*i}))});
/** One side of one reading, built by hand, for the rule tests. `lean` comes from the table under test. */
const leg=(kind,price,oi,change=100)=>({contracts:5,change,price_change:0,oi_direction:oi,
 price_direction:price,strength:1,lean:L.signalLean(kind,price,oi),ranked:9,what_label:'',meaning:'',text:''});
const PRICE_DIRS=['up','down','flat'],OI_DIRS=['building','unwinding','flat'];

// =================================================================================================================
// P03 — THE REPAINT, REPRODUCED, AND THE FIX PROVED.
//
// signal/1 measured every row against the largest window change over the WHOLE available array, later readings
// included, and found "an hour back" by counting four rows. The auditor appended one extreme observation to the
// end of a stored session and watched the same time=4 row rewrite itself: Very strong became Stable, and its
// market signal turned over. Below is that rule in eleven lines, the flip it produces, and the proof that the
// rule on this tab today does not produce it.
//
// This matters more than the count of checks in this file: the 359 that stood here all fed signalRows ONE
// payload and never asked what a second, longer one did to the first one's rows.
// =================================================================================================================
/** signal/1's side, reproduced: whole-array peak, four ROWS back, open interest only. */
const v1Side=(points,i,lookback=4)=>{
 const ch=points.map((_,k)=>points[k].delta_oi-points[Math.max(0,k-lookback)].delta_oi);
 const peak=Math.max(0,...ch.map(Math.abs));
 const v=ch[i],flat=peak>0&&Math.abs(v)<0.05*peak;
 const dir=peak<=0||flat?'flat':v>0?'building':'unwinding';
 const share=peak>0?Math.abs(v)/peak:0;
 const strength=flat||!peak?0:share<1/3?1:share<2/3?2:3;
 return {dir,change:v,word:strength?['Mild','Strong','Very strong'][strength-1]:'Stable'};
};
/** signal/1's market signal, reproduced: it never reads a price. */
const v1Signal=(c,p)=>{
 if(p.dir==='building'&&c.dir==='unwinding')return 'strong_bullish';
 if(c.dir==='building'&&p.dir==='unwinding')return 'strong_bearish';
 if((p.dir==='building'&&c.dir==='flat')||(c.dir==='unwinding'&&p.dir==='flat'))return 'bullish';
 if((c.dir==='building'&&p.dir==='flat')||(p.dir==='unwinding'&&c.dir==='flat'))return 'bearish';
 if(c.dir==='flat'&&p.dir==='flat')return 'flat';
 const cm=Math.abs(c.change),pm=Math.abs(p.change);
 if(!cm||!pm)return 'flat';
 if(cm<=1.33*pm&&pm<=1.33*cm)return 'flat';
 const callsBigger=cm>pm;
 if(c.dir==='building')return callsBigger?'bearish':'bullish';
 return callsBigger?'bullish':'bearish';
};
ok(()=>{ // THE OLD FAILURE, KEPT: appending one later observation rewrote the time=4 row under signal/1
 const callPts=Array.from({length:8},(_,i)=>({at:sigStamp(i),oi:null,delta_oi:1000*i,price:100}));
 const putPts=Array.from({length:8},(_,i)=>({at:sigStamp(i),oi:null,delta_oi:100*i,price:100}));
 // BEFORE: at time=4 the calls are the session's largest builder and the puts are the smaller one
 const before=v1Signal(v1Side(callPts,4),v1Side(putPts,4));
 assert.equal(v1Side(callPts,4).word,'Very strong','signal/1 read the time=4 calls as Very strong');
 assert.equal(before,'bearish','and the time=4 row as one direction');
 // AFTER: one more observation, taken an hour and a quarter LATER, and nothing else changes
 const callsPlus=[...callPts,{at:sigStamp(8),oi:null,delta_oi:1000*7+1e6,price:100}];
 const putsPlus=[...putPts,{at:sigStamp(8),oi:null,delta_oi:100*7,price:100}];
 const after=v1Signal(v1Side(callsPlus,4),v1Side(putsPlus,4));
 assert.equal(v1Side(callsPlus,4).word,'Stable','signal/1 then read the SAME time=4 calls as Stable');
 assert.equal(after,'bullish','and turned the SAME time=4 row over');
 assert.notEqual(before,after,'this is the defect: a later reading rewrote an earlier row');
});
ok(()=>{ // AND THE FIX: the same two payloads through the rule on this tab today, row for row
 const mk=(n,extra)=>[
  {slot:'CE+0',option_type:'CE',row:'calls',present:true,strike:23300,instrument_token:1,
   points:Array.from({length:n},(_,i)=>({at:sigStamp(i),oi:null,
    delta_oi:1000*i+(extra&&i===n-1?1e6:0),price:100+5*i}))},
  {slot:'PE+0',option_type:'PE',row:'puts',present:true,strike:23300,instrument_token:2,
   points:Array.from({length:n},(_,i)=>({at:sigStamp(i),oi:null,delta_oi:100*i,price:100-2*i}))},
 ];
 const before=L.signalRows(mk(8,false));
 const after=L.signalRows(mk(9,true));
 assert.equal(before.length,8);assert.equal(after.length,9);
 // the rows are newest-first, so the eight earlier readings are the TAIL of the longer table
 assert.equal(JSON.stringify(after.slice(1)),JSON.stringify(before),
  'appending an extreme later observation must leave every earlier row byte for byte identical');
 // named explicitly, because this is the auditor's own row
 const at4=(rows)=>rows.find(r=>r.at===sigStamp(4));
 assert.ok(at4(before)&&at4(after),'the time=4 row is in both tables');
 assert.deepEqual(JSON.parse(JSON.stringify(at4(after))),JSON.parse(JSON.stringify(at4(before))),
  'the time=4 row reads the same before and after the append - strength, signal and all');
});
ok(()=>{ // APPEND-INVARIANCE, generally: over gaps, holes, flat sides and long sessions alike
 const shapes=[
  [sigSlot('calls','CE',9,1000,5),sigSlot('puts','PE',9,100,-2)],
  [sigSlot('calls','CE',9,-500,-4),sigSlot('puts','PE',9,700,3)],
  [sigSlot('calls','CE',9,0,0),sigSlot('puts','PE',9,0,0)],
  [sigSlot('calls','CE',9,100,1,-9000)],
 ];
 for(const shape of shapes){
  const short=shape.map(s=>({...s,points:s.points.slice(0,7)}));
  const base=L.signalRows(short);
  for(const spike of [1e9,-1e9,0]){
   const grown=shape.map(s=>({...s,points:[...s.points.slice(0,7),
    {at:sigStamp(7),oi:null,delta_oi:spike,price:spike}]}));
   const rows=L.signalRows(grown);
   assert.equal(rows.length,base.length+1);
   assert.equal(JSON.stringify(rows.slice(1)),JSON.stringify(base),
    `an appended observation of ${spike} rewrote an earlier row`);
  }
 }
});
ok(()=>{ // AN HOUR IS AN HOUR, BY THE CLOCK - not four rows. A hole makes those two different things.
 assert.equal(L.SIGNAL_WINDOW_MINUTES,60);
 // 09:15, 09:30, 09:45, 10:00, 10:15: five readings, and the fifth has exactly an hour behind it
 const dense=[0,15,30,45,60].map(m=>sigStamp(0,9*60+15+m,0));
 assert.equal(L.signalBaselineIndex(dense,4),0,'exactly sixty minutes back is the baseline');
 assert.equal(L.signalBaselineIndex(dense,3),null,'forty-five minutes is not an hour');
 assert.equal(L.signalBaselineIndex(dense,0),null,'and the first reading has nothing behind it');
 // FOUR ROWS BACK IS NOT AN HOUR when readings are missing. 09:15, 09:30, 10:45, 11:30, 11:45: four rows
 // back from 11:45 is 09:15, which is two and a half hours. The window picks 10:45, which really is an hour.
 const gappy=[0,15,90,135,150].map(m=>sigStamp(0,9*60+15+m,0));
 assert.equal(L.signalBaselineIndex(gappy,4),2,'the window is found by the clock, not by counting rows');
 assert.notEqual(L.signalBaselineIndex(gappy,4),0,'which is what four rows back would have given');
 // and a hole WIDER than the window leaves no baseline at all rather than a stale one
 const holed=[0,15,150,165,180].map(m=>sigStamp(0,9*60+15+m,0));
 assert.equal(L.signalBaselineIndex(holed,4),null,
  'the nearest earlier reading is 165 minutes back - wider than the window, so there is no baseline');
 assert.equal(L.signalBaselineIndex(holed,3),null);
 // a hole of exactly the limit is still usable; a minute more is not
 const edge=[sigStamp(0,0,0),sigStamp(0,L.SIGNAL_WINDOW_MAX_MINUTES,0)];
 assert.equal(L.signalBaselineIndex(edge,1),0);
 const over=[sigStamp(0,0,0),sigStamp(0,L.SIGNAL_WINDOW_MAX_MINUTES+1,0)];
 assert.equal(L.signalBaselineIndex(over,1),null);
 // the LATEST qualifying reading wins, never the oldest one lying around
 const many=[0,15,30,45,60,75,90].map(m=>sigStamp(0,9*60+m,0));
 assert.equal(L.signalBaselineIndex(many,6),2,'90 minutes in, the reading 60 minutes back is the baseline');
});
ok(()=>{ // DUPLICATE STAMPS, UNREADABLE STAMPS, AND A SESSION BOUNDARY
 // two readings carrying one stamp are not an hour apart, and neither is a baseline for the other
 const twins=['2026-09-18 10:15','2026-09-18 10:15','2026-09-18 11:15'];
 assert.equal(L.signalBaselineIndex(twins,1),null);
 assert.equal(L.signalBaselineIndex(twins,2),1,'and the later of the twins is the one an hour back');
 // a stamp that cannot be read is skipped over, never guessed at
 assert.equal(L.readingMinutes(null),null);
 assert.equal(L.readingMinutes('2026-09-18'),null,'a date with no time is not a reading time');
 assert.equal(L.readingMinutes('not a time'),null);
 assert.equal(L.readingMinutes('2026-09-18 10:15')-L.readingMinutes('2026-09-18 09:15'),60);
 assert.equal(L.readingMinutes('2026-09-19 09:15')-L.readingMinutes('2026-09-18 09:15'),1440);
 assert.equal(L.signalBaselineIndex(['2026-09-18 09:15',null,'2026-09-18 10:15'],2),0);
 assert.equal(L.signalBaselineIndex([null,'2026-09-18 10:15'],1),null);
 // yesterday's close is a day away, not an hour: it is outside the window and is refused
 assert.equal(L.signalBaselineIndex(['2026-09-17 15:30','2026-09-18 09:15'],1),null,
  'a reading from the previous session is not this session\'s hour-back baseline');
});
ok(()=>{ // AN EARLY-SESSION ROW SAYS WHAT IT HAS, AND IS NEVER GIVEN A DIRECTION IT CANNOT HAVE
 const rows=L.signalRows([sigSlot('calls','CE',6,1000,5),sigSlot('puts','PE',6,100,-2)]);
 assert.equal(rows.length,6,'every captured reading keeps its row');
 const early=rows.filter(r=>r.at<sigStamp(4)); // 09:15 .. 10:00, none with an hour behind it
 assert.equal(early.length,4);
 for(const row of early){
  assert.equal(row.calls,null,'no side is computed without a baseline');
  assert.equal(row.puts,null);
  assert.equal(row.signal,'','and no direction is invented for it');
  assert.equal(row.label,DASH);
  assert.equal(row.reason,L.SIGNAL_NO_WINDOW,'the row says which absence this is');
  assert.equal(row.from,null,'and names no reading it was compared against');
  assert.equal(row.window_minutes,null);
 }
 // and the rows that DO have an hour behind them carry that hour, in minutes, on the row
 for(const row of rows.filter(r=>r.at>=sigStamp(4))){
  assert.equal(row.window_minutes,60,'the window a word came from is on the row');
  assert.ok(row.from&&row.from<row.at,'together with the reading it was compared against');
  assert.equal(L.readingMinutes(row.at)-L.readingMinutes(row.from),60);
 }
 // the three absences are three different sentences and never borrow each other's
 assert.notEqual(L.SIGNAL_NO_WINDOW,L.SIGNAL_NO_DATA);
 assert.notEqual(L.SIGNAL_NO_WINDOW,L.SIGNAL_NO_BASELINE);
});
ok(()=>{ // EVERY ROW CARRIES THE RULE THAT PRODUCED IT
 assert.equal(L.SIGNAL_RULE_VERSION,'signal/2');
 const rows=L.signalRows([sigSlot('calls','CE',8,1000,5),sigSlot('puts','PE',8,100,-2)]);
 for(const row of rows)assert.equal(row.rule_version,L.SIGNAL_RULE_VERSION,'on every row, signal or not');
 assert.ok(L.SIGNAL_RULE_TEXT.includes(L.SIGNAL_RULE_VERSION),
  'and the reader can find the version in How to read this');
});
ok(()=>{ // THE TEN CONTRACTS ARE THE LATEST TEN, AND THE TABLE SAYS SO RATHER THAN PRETENDING OTHERWISE
 // The basket is chosen from the LATEST spot and carried back over the session. This browser cannot choose a
 // historical basket - the server picks the ten - so the honest answer is to label the view, and it is.
 assert.ok(/LATEST reading/.test(L.SIGNAL_AGGREGATE_TEXT),
  'the panel must say the ten are the ten at the latest reading');
 assert.ok(/carried back/.test(L.SIGNAL_AGGREGATE_TEXT)&&/not a record of which strikes/.test(L.SIGNAL_AGGREGATE_TEXT),
  'and that an earlier row is not a record of the strikes that were at the money then');
});
ok(()=>{ // NOTHING IS REMEMBERED BETWEEN CALLS: an expiry change is a different payload and nothing else
 const septem=[sigSlot('calls','CE',8,1000,5),sigSlot('puts','PE',8,100,-2)];
 const octob=[sigSlot('calls','CE',8,-4000,-9),sigSlot('puts','PE',8,25,1)];
 const first=JSON.stringify(L.signalRows(septem));
 L.signalRows(octob);L.signalRows(octob);L.signalRows([]);
 assert.equal(JSON.stringify(L.signalRows(septem)),first,
  'reading another expiry in between must not change what the first one says');
 assert.notEqual(JSON.stringify(L.signalRows(octob)),first,'and the two really are different payloads');
});
ok(()=>{ // THE STRENGTH RULE IS MEASURED, NOT A CONSTANT, AND NEVER AGAINST A READING TAKEN LATER
 // One side climbing 100 a reading against one climbing 10,000 a reading. The LAST row of each is the same
 // fraction of its own session's largest change, so both read the same word - which is the point: the ladder
 // is that instrument's own session and never a fixed number of contracts.
 const small=L.signalRows([sigSlot('calls','CE',10,100),sigSlot('puts','PE',10,100)]);
 const big=L.signalRows([sigSlot('calls','CE',10,10000),sigSlot('puts','PE',10,10000)]);
 assert.equal(small[0].calls.text,big[0].calls.text,
  'a hundred contracts and ten thousand read the same when each is the same share of its own session');
 assert.ok(small[0].calls.text!=='','and both really got a word');
 // the flat cut IS the tab's own band, and the bands are equal thirds - no third constant anywhere
 assert.equal(L.GRID_FLAT_FRACTION,0.05);
 assert.deepEqual([...L.SIGNAL_STRENGTH_BANDS],[1/3,2/3],'equal thirds of the side\'s own largest change');
 assert.equal(L.SIGNAL_STRENGTH_WORDS.length,L.SIGNAL_STRENGTH_BANDS.length+1,
  'one word per band, and the bands are derived from how many words there are');
 assert.equal(L.SIGNAL_STRENGTH_MIN,L.SIGNAL_STRENGTH_WORDS.length,
  'and the ladder needs one observation per word before it ranks anything');
 // a side that barely moved is STABLE and gets no arrow at all
 const still=L.signalRows([sigSlot('calls','CE',10,0),sigSlot('puts','PE',10,0)]);
 assert.equal(still[0].calls.text,L.SIGNAL_STABLE,'a side that did not move is stable, not mildly anything');
 assert.equal(still[0].calls.oi_direction,'flat');
 // A CHANGE IS NOT RANKED AGAINST ITSELF. The first measurable reading has nothing behind it, and the row
 // keeps its direction and says the strength is not ranked rather than calling it the session's largest.
 const young=L.signalRows([sigSlot('calls','CE',6,1000,5),sigSlot('puts','PE',6,100,-2)]);
 const firstMeasured=young[young.length-5]; // the 10:15 reading, the first with an hour behind it
 assert.equal(firstMeasured.at,sigStamp(4));
 assert.equal(firstMeasured.calls.ranked,1,'one measured change stands behind it');
 assert.equal(firstMeasured.calls.text,L.SIGNAL_NO_STRENGTH);
 assert.equal(firstMeasured.calls.strength,0);
 assert.notEqual(firstMeasured.calls.oi_direction,'no baseline','and it still has its direction');
 // the third measured change - 11:00, with 10:15 and 10:30 behind it - is the first the ladder can rank
 const longer=L.signalRows([sigSlot('calls','CE',8,1000,5),sigSlot('puts','PE',8,100,-2)]);
 const third=longer.find(r=>r.at===sigStamp(6));
 assert.equal(third.calls.ranked,3);
 assert.ok([...L.SIGNAL_STRENGTH_WORDS].some(w=>third.calls.text.startsWith(w)),
  `the third measured change is the first that can be ranked: ${third.calls.text}`);
 assert.equal(longer.find(r=>r.at===sigStamp(5)).calls.text,L.SIGNAL_NO_STRENGTH,
  'and the second one still is not');
 // and the rule is STATED where the reader can find it, in the words it actually uses
 assert.ok(/own session/i.test(L.SIGNAL_STRENGTH_TEXT)&&/thirds/i.test(L.SIGNAL_STRENGTH_TEXT),
  'How to read this must say the strength is measured against the instrument\'s own session');
 assert.ok(/never a reading taken later/i.test(L.SIGNAL_STRENGTH_TEXT),
  'and that a later reading is never in the yardstick');
 assert.ok(/not a confidence/i.test(L.SIGNAL_STRENGTH_TEXT),
  'strength is a size, and must say it is not a confidence');
 for(const word of L.SIGNAL_STRENGTH_WORDS)
  assert.ok(L.SIGNAL_STRENGTH_TEXT.includes(word),`and must name the word "${word}"`);
});
ok(()=>{ // NO ROW IS INVENTED FOR A READING WITHOUT DATA
 // a reading the store never took is not a row at all.
 // (Spread first: logic.ts runs in its own vm realm, so its arrays have a different Array prototype and
 // deepStrictEqual would compare the realms rather than the contents.)
 assert.deepEqual([...L.signalRows([])],[],'no slots, no rows');
 assert.deepEqual([...L.signalRows(null)],[],'and a null body is not a table of zeroes');
 assert.deepEqual([...L.signalRows([{slot:'CE+0',option_type:'CE',row:'calls',present:false,points:[]}])],[],
  'an unlisted strike contributes no reading');
 // a reading that WAS taken but carried nothing keeps its row, says so, and gets NO signal
 const holed=[{slot:'CE+0',option_type:'CE',row:'calls',present:true,strike:1,instrument_token:1,
  points:[{at:'2026-09-18 09:30',oi:null,delta_oi:null,price:null},
   {at:'2026-09-18 09:45',oi:null,delta_oi:null,price:null}]}];
 const rows=L.signalRows(holed);
 assert.equal(rows.length,2,'the readings are still there');
 for(const row of rows){
  assert.equal(row.signal,'','a reading with no data is never given a signal');
  assert.equal(row.label,DASH,'and never a word');
  assert.equal(row.reason,L.SIGNAL_NO_DATA,'it says what it has instead');
  assert.equal(row.interpretation,'','and reads nothing into nothing');
 }
 // a reading that carried nothing is not the same thing as one with no hour behind it, even side by side
 const mixed=[{slot:'CE+0',option_type:'CE',row:'calls',present:true,strike:1,instrument_token:1,
  points:[{at:sigStamp(0),oi:null,delta_oi:null,price:null},
   {at:sigStamp(1),oi:null,delta_oi:5,price:10},
   {at:sigStamp(4),oi:null,delta_oi:7,price:12}]}];
 const said=L.signalRows(mixed).map(r=>r.reason);
 assert.equal(said[said.length-1],L.SIGNAL_NO_DATA,'the reading that carried nothing says so');
 assert.equal(said[said.length-2],L.SIGNAL_NO_WINDOW,'the one with no hour behind it says THAT');
 // an empty BASKET is a third thing again: both readings carried values, but no contract carried both
 const apart=[{slot:'CE+0',option_type:'CE',row:'calls',present:true,strike:1,instrument_token:1,
   points:[{at:sigStamp(0),oi:null,delta_oi:5,price:10},{at:sigStamp(4),oi:null,delta_oi:null,price:null}]},
  {slot:'CE+1',option_type:'CE',row:'calls',present:true,strike:2,instrument_token:2,
   points:[{at:sigStamp(0),oi:null,delta_oi:null,price:null},{at:sigStamp(4),oi:null,delta_oi:9,price:11}]}];
 assert.equal(L.signalRows(apart)[0].reason,L.SIGNAL_NO_BASELINE,
  'two readings with no contract in common are not a comparison');
 // newest first, which is what he asked for
 const ordered=L.signalRows([sigSlot('calls','CE',8,100),sigSlot('puts','PE',8,100)]);
 assert.equal(ordered.length,8);
 assert.ok(String(ordered[0].at)>String(ordered[ordered.length-1].at),'newest reading is the first row');
});

// =================================================================================================================
// P04 — THE TRUTH TABLE. PRICE AND OPEN INTEREST TOGETHER, OR NO DIRECTION AT ALL.
//
// signal/1 chose the market signal from the two sides' OPEN INTEREST alone while the price-direction fields sat
// unread beside it. Puts building while calls unwound read Strong Bullish whether the put premium was rising or
// falling - so put BUYING, which is the opposite reading, produced the same word as put WRITING. The owner's own
// source table is keyed on price AND open interest; reading half of it was the defect.
//
// Written out below, cell by cell, because a truth table asserted by re-deriving it is not a test of anything.
// =================================================================================================================
ok(()=>{ // THE CALL SIDE, all nine cells, from the owner's own flow table
 // both axes moved: the four §3.1 pairs, and a call's premium rises with the underlying
 assert.equal(L.signalLean('CE','up','building'),'up');    // call buying increasing
 assert.equal(L.signalLean('CE','up','unwinding'),'up');   // call short covering
 assert.equal(L.signalLean('CE','down','building'),'down');// call writing increasing
 assert.equal(L.signalLean('CE','down','unwinding'),'down');// call buyers exiting
 // an axis that barely moved carries NO lean - it is not a quiet version of a direction
 assert.equal(L.signalLean('CE','flat','building'),'');
 assert.equal(L.signalLean('CE','flat','unwinding'),'');
 assert.equal(L.signalLean('CE','up','flat'),'');
 assert.equal(L.signalLean('CE','down','flat'),'');
 assert.equal(L.signalLean('CE','flat','flat'),'');
});
ok(()=>{ // THE PUT SIDE, all nine cells. A put's premium falls as the underlying rises, so it reads the other way
 assert.equal(L.signalLean('PE','up','building'),'down');  // put buying increasing
 assert.equal(L.signalLean('PE','up','unwinding'),'down'); // put short covering
 assert.equal(L.signalLean('PE','down','building'),'up');  // put writing increasing
 assert.equal(L.signalLean('PE','down','unwinding'),'up'); // put buyers exiting
 assert.equal(L.signalLean('PE','flat','building'),'');
 assert.equal(L.signalLean('PE','flat','unwinding'),'');
 assert.equal(L.signalLean('PE','up','flat'),'');
 assert.equal(L.signalLean('PE','down','flat'),'');
 assert.equal(L.signalLean('PE','flat','flat'),'');
 // eighteen cells, no nineteenth, and each one has a row of the owner's flow table behind it
 assert.equal(Object.keys(L.SIGNAL_LEANS).length,18);
 for(const kind of ['CE','PE'])for(const price of PRICE_DIRS)for(const oi of OI_DIRS)
  assert.ok(L.FLOW_LABELS[`${kind}|${price}|${oi}`],`no flow row behind ${kind}|${price}|${oi}`);
 // a side with no baseline on either axis leans nowhere at all
 for(const kind of ['CE','PE']){
  assert.equal(L.signalLean(kind,'no baseline','building'),'');
  assert.equal(L.signalLean(kind,'up','no baseline'),'');
  assert.equal(L.signalLean(kind,'',''),'');
 }
});
ok(()=>{ // THE DEFECT, NAMED: put buying with rising open interest may not read Strong Bullish
 // calls unwinding on a falling premium, puts BUILDING on a RISING premium - traders buying downside.
 const calls=leg('CE','down','unwinding'),puts=leg('PE','up','building');
 // signal/1 saw only "puts building + calls unwinding" and said so:
 assert.equal(v1Signal({dir:'unwinding',change:-100},{dir:'building',change:100}),'strong_bullish',
  'this is what the old rule produced for a put-BUYING state');
 // signal/2 reads both axes and cannot:
 assert.notEqual(L.marketSignal(calls,puts),'strong_bullish');
 assert.equal(L.marketSignal(calls,puts),'strong_bearish','both sides read the same way, and it is not that one');
 // and the put-WRITING state - the same open-interest move on a FALLING premium - reads the other way
 assert.equal(L.marketSignal(leg('CE','up','unwinding'),leg('PE','down','building')),'strong_bullish');
 // the two states differ ONLY in the price fields the old rule never read
 assert.equal(leg('PE','up','building').oi_direction,leg('PE','down','building').oi_direction);
 assert.notEqual(L.signalLean('PE','up','building'),L.signalLean('PE','down','building'));
});
ok(()=>{ // THE COMBINATION, over every one of the eighty-one pairs of cells
 const seen=new Set();
 for(const cp of PRICE_DIRS)for(const co of OI_DIRS)for(const pp of PRICE_DIRS)for(const po of OI_DIRS){
  const calls=leg('CE',cp,co),puts=leg('PE',pp,po);
  const signal=L.marketSignal(calls,puts);
  seen.add(signal);
  assert.ok(L.SIGNAL_LABELS[signal],`${cp}/${co} vs ${pp}/${po} produced no word`);
  const c=calls.lean,p=puts.lean;
  if(c&&p&&c!==p)assert.equal(signal,'neutral','mixed evidence must stay mixed');
  else if(!c&&!p)assert.equal(signal,'flat','neither side reading is Flat');
  else if(c&&p)assert.ok(signal==='strong_bullish'||signal==='strong_bearish','both legs present reads Strong');
  else assert.ok(signal==='bullish'||signal==='bearish','one leg alone is the plain word');
  // and the word never contradicts the two sides it came from
  if(signal==='strong_bullish'||signal==='bullish')assert.ok(c!=='down'&&p!=='down');
  if(signal==='strong_bearish'||signal==='bearish')assert.ok(c!=='up'&&p!=='up');
 }
 // every one of his words is reachable from the table, and no word outside it is
 assert.deepEqual([...seen].sort(),['bullish','bearish','flat','neutral','strong_bearish','strong_bullish'].sort());
 for(const key of [...seen])assert.ok(L.SIGNAL_LABELS[key]&&L.SIGNAL_DOTS[key],
  `the owner's own word and dot for ${key}`);
});
ok(()=>{ // CONTRADICTORY EVIDENCE STAYS ON THE ROW, AND IS NOT RESOLVED BY SIZE
 const calls=leg('CE','up','building',5_000_000),puts=leg('PE','up','building',1);
 assert.equal(L.marketSignal(calls,puts),'neutral',
  'calls read one way and puts the other: the bigger side is still only the bigger side');
 assert.equal(L.SIGNAL_LABELS.neutral,'Neutral','and it is said in the owner\'s own word');
 // swapping which side is enormous cannot change the answer
 assert.equal(L.marketSignal(leg('CE','up','building',1),leg('PE','up','building',5_000_000)),'neutral');
 // the two readings that disagree are BOTH on the row, in his own vocabulary, for the reader to see
 const rows=L.signalRows([sigSlot('calls','CE',10,1000,9),sigSlot('puts','PE',10,1000,9)]);
 const row=rows[0];
 assert.equal(row.signal,'neutral');
 assert.equal(row.calls.what_label,'Call buying increasing');
 assert.equal(row.puts.what_label,'Put buying increasing');
 assert.ok(row.interpretation,'and the open-interest reading is still stated beside them');
 // a side with no comparable baseline is never given a word at all
 assert.equal(L.marketSignal(null,leg('PE','down','building')),'');
 assert.equal(L.marketSignal(leg('CE','up','building'),null),'');
});
ok(()=>{ // THE RULE IS INSPECTABLE: the window, the version and the vocabulary are all on the panel
 assert.ok(/60 minutes/.test(L.SIGNAL_BASKET_TEXT),'the window is stated in minutes');
 assert.ok(/never by counting four rows back/.test(L.SIGNAL_BASKET_TEXT),'and why it is not four rows');
 assert.ok(/price AND its open interest together/.test(L.SIGNAL_RULE_TEXT),
  'the rule must say it reads both axes');
 assert.ok(/Open interest alone never decides/.test(L.SIGNAL_RULE_TEXT),'and that open interest alone does not');
 assert.ok(/Neutral/.test(L.SIGNAL_RULE_TEXT)&&/OPPOSITE/.test(L.SIGNAL_RULE_TEXT),
  'and what happens when the two sides disagree');
 assert.ok(/the tape does not say which side was the aggressor/.test(L.SIGNAL_CAVEAT_TEXT),
  'and the caveat must refuse to name who initiated a trade');
 // STRENGTH IS A SIZE. Nothing on this table is a hit rate, and no percentage of anything is offered as one.
 // A sentence that DENIES being one of these is the opposite of offering one - the same exemption the older
 // §5 sweeps make for "nothing on it is a forecast".
 const rateWord=/\b(accuracy|accurate|win rate|hit rate|success rate|probability|odds|chance of)\b/i;
 const deniesRate=(t)=>/\bnot a (confidence|probability|chance|forecast|prediction)\b/i.test(String(t));
 for(const text of [L.SIGNAL_STRENGTH_TEXT,L.SIGNAL_RULE_TEXT,L.SIGNAL_CAVEAT_TEXT,L.SIGNAL_BASKET_TEXT,
  L.SIGNAL_AGGREGATE_TEXT])
  assert.ok(deniesRate(text)||!rateWord.test(text),
   `the signal table must not offer a measured-looking rate: ${text}`);
 assert.ok(rateWord.test('a 68% hit rate on these rows'),'and the sweep really does catch one');
 assert.ok(!deniesRate('a 68% hit rate on these rows'),'without the denial clearing it');
 // FIVE STRIKES ADDED TOGETHER ARE NOT AN INSTRUMENT. The side's premium change is a DIRECTION over a fixed
 // basket and nothing else: it is never rendered, never priced in rupees and never drawn as a series.
 const table=CODE('SignalTable.tsx');
 assert.ok(!/price_change/.test(table),'the side\'s basket premium change must not reach the screen');
 assert.ok(!/\bprice\(/.test(table),'and nothing on this table is formatted as a tradable price');
 const rows=L.signalRows([sigSlot('calls','CE',10,1000,5),sigSlot('puts','PE',10,100,-2)]);
 assert.ok(typeof rows[0].calls.price_change==='number','it exists as a direction input');
 assert.ok(['up','down','flat',L.NO_DIRECTION].includes(rows[0].calls.price_direction),
  'and all the reader ever sees of it is that direction');
});
ok(()=>{ // EVERY ROW OF HIS SOURCE VOCABULARY is reachable, and comes from the one table both sides share
 // his table, verbatim: price direction + OI direction -> what is happening, and what it means
 const want=[['CE','down','building','Call writing increasing','Sellers are building resistance'],
  ['PE','down','building','Put writing increasing','Sellers are building support'],
  ['CE','up','unwinding','Call short covering','Call sellers are exiting'],
  ['PE','up','unwinding','Put short covering','Put sellers are exiting'],
  ['CE','up','building','Call buying increasing','Traders are buying upside'],
  ['PE','up','building','Put buying increasing','Traders are buying downside protection']];
 for(const [kind,price,oi,what,meaning] of want){
  const found=L.FLOW_LABELS[`${kind}|${price}|${oi}`];
  assert.ok(found,`his table has no row for ${kind} ${price} + OI ${oi}`);
  assert.equal(found[0],what,`the owner's own words for ${kind}|${price}|${oi}`);
  assert.equal(found[1],meaning,`and his own meaning for ${kind}|${price}|${oi}`);
 }
 // "both sides building similarly" -> no clear directional edge, on the BLOCK's own tolerance
 assert.equal(L.SIGNAL_BALANCED,L.GRID_BLOCK_BOTH_BUILDING,'one balance sentence on this tab, not two');
 assert.ok(/no clear directional edge/i.test(L.SIGNAL_BALANCED));
 // "little OI change" -> positioning unchanged / flat
 assert.equal(L.FLOW_FLAT_WHAT,'Very little change');
 assert.equal(L.FLOW_FLAT_MEANING,'Positioning is unchanged');
 assert.equal(L.SIGNAL_INTERPRETATIONS['flat|flat'],'No meaningful new positioning');
 // the interpretation table is COMPLETE: all nine combinations, so no reading falls through to a blank
 for(const c of ['building','flat','unwinding'])for(const p of ['building','flat','unwinding'])
  assert.ok(L.SIGNAL_INTERPRETATIONS[`${c}|${p}`],`no interpretation for calls ${c} + puts ${p}`);
 // and the both-building cell really does split three ways on the tolerance
 const side=(dir,change)=>({contracts:5,change,price_change:0,oi_direction:dir,price_direction:'flat',
  strength:1,what_label:'',meaning:'',text:''});
 assert.equal(L.signalInterpretation(side('building',1000),side('building',100)),L.SIGNAL_MORE_CALLS);
 assert.equal(L.signalInterpretation(side('building',100),side('building',1000)),L.SIGNAL_MORE_PUTS);
 assert.equal(L.signalInterpretation(side('building',100),side('building',100)),L.SIGNAL_BALANCED);
});
ok(()=>{ // §5 OVER EVERY STRING THE SIGNAL TABLE ADDED - his vocabulary aside, nothing here may predict
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|should rise|should fall|target price|support level|resistance level|breakout|momentum|overbought|oversold|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i;
 const sentences=[L.SIGNAL_AGGREGATE_TEXT,L.SIGNAL_BASKET_TEXT,L.SIGNAL_CAVEAT_TEXT,L.SIGNAL_RULE_TEXT,
  L.SIGNAL_STRENGTH_TEXT,L.SIGNAL_NO_DATA,L.SIGNAL_NO_BASELINE,L.SIGNAL_NO_WINDOW,L.SIGNAL_NO_STRENGTH,
  L.SIGNAL_STABLE,L.SIGNAL_BALANCED,
  L.SIGNAL_MORE_CALLS,L.SIGNAL_MORE_PUTS,...Object.values(L.SIGNAL_INTERPRETATIONS),
  ...Object.values(L.SIGNAL_LABELS),...L.SIGNAL_STRENGTH_WORDS];
 // A sentence that DENIES being a prediction is the opposite of a forecast, and the one caveat is exactly
 // that sentence. The same exemption the older §5 sweeps make for "nothing on it is a forecast".
 const denies=(t)=>/\bnot a (prediction|forecast)\b/i.test(String(t));
 for(const text of sentences){
  assert.ok(String(text).length>0,'every sentence above must exist');
  assert.ok(denies(text)||!banned.test(String(text)),
   `a signal-table sentence must not predict: ${text}`);
  assert.ok(!MARK_WORD.test(String(text)),`a signal-table sentence says "mark": ${text}`);
  assert.ok(!/\d/.test(String(text))||!LEVEL_WORD.test(String(text)),
   `a sentence naming support or resistance must carry no number: ${text}`);
 }
 // the file itself, in single, double AND template quotes
 const said=VISIBLE(SRC('SignalTable.tsx')).filter(t=>banned.test(t));
 assert.deepEqual(said,[],`SignalTable.tsx must not predict: ${said.join(' | ')}`);
 assert.deepEqual(marky(SRC('SignalTable.tsx')),[],'and must say "15-min reading", not "mark"');
 // THE ONE CAVEAT, ONCE: it is in How to read this, and it is not repeated on a row
 assert.ok(/not a prediction/i.test(L.SIGNAL_CAVEAT_TEXT)&&/buyer and a seller/i.test(L.SIGNAL_CAVEAT_TEXT),
  'the caveat must say both halves: not a prediction, and every contract has both sides');
 // ONCE in the body of the file - the import that brings it in is not a second statement of it.
 const table=CODE('SignalTable.tsx').split('\n').filter(l=>!/^import\b|^\s+[A-Z_,{}\s]+\}?\s*from '/.test(l)).join('\n');
 assert.equal((table.match(/SIGNAL_CAVEAT_TEXT/g)||[]).length,1,
  'the caveat is stated ONCE, behind How to read this - never on every row');
 assert.ok(/tone:'amber' as const\}/.test(table)||/tone:'amber'\}/.test(table),
  'and it is amber, because it is a caveat on what the table is');
});
ok(()=>{ // ONE READ FOR THE TEN CONTRACTS: the grid and the signal table cannot describe different ones
 const page=CODE('index.tsx'),table=CODE('SignalTable.tsx'),grid=CODE('OiGridSection.tsx');
 assert.ok(/const gridRead=useDerivativeRead<OiGrid>\(gridPath,seq\)/.test(page),
  'the page makes the ΔOI-grid read once');
 assert.ok(/read=\{gridRead\}/.test(page),'and hands it to the block');
 assert.ok(/gridRead=\{gridRead\}/.test(page),'and to the bar the signal table sits on');
 for(const [name,src] of [['SignalTable.tsx',table],['OiGridSection.tsx',grid]])
  assert.ok(!/useDerivativeRead<OiGrid>/.test(src),
   `${name} must not make its own oi-grid read: two fetches of one payload can drift apart`);
});

ok(()=>{ // THE MENU IS BUILT FROM THE ENUM, so it cannot drift back into offering the wire strings
 const dialog=NO_COMMENTS(SRC('FilterDialog.tsx'));
 assert.ok(/column==='buildup'\)return BUILDUP_IDS\.map/.test(dialog),
  'the build-up menu must offer the canonical ids');
 assert.ok(/label:buildupChoiceLabel\(id\)/.test(dialog),'labelled by the same id');
 assert.ok(/detail:BUILDUP_DETAIL\[id\]/.test(dialog),'and explained by the same id');
 assert.ok(!/BUILDUP_VALUES/.test(dialog),
  'the builder must not reach for the wire strings - that is what made all six choices a dash');
 // every id offered has a §3.1 line under it, including the store's own two non-labels
 const detail=dialog.slice(dialog.indexOf('const BUILDUP_DETAIL'));
 for(const id of L.BUILDUP_IDS)assert.ok(new RegExp(`\\b${id}:'`).test(detail),
  `the menu offers ${id} with nothing under it`);
});
ok(()=>{ // ONE BOUNDARY: no file on the tab reads a build-up any other way
 const files=fs.readdirSync(path.join(__dirname,'..','src','derivative')).filter(f=>/\.tsx?$/.test(f));
 for(const file of files){
  if(file==='logic.ts')continue;
  const code=NO_COMMENTS(SRC(file));
  assert.ok(!/BUILDUP_LABELS\[/.test(code),`${file} must not index the label table itself`);
  assert.ok(!/buildup_(day|15m)\s*===/.test(code),
   `${file} must not compare a served build-up against a literal - use buildupId`);
 }
});

// =================================================================================================================
// P06 — "3 CONDITIONS" WAS NOT A COUNT OF THREE CONDITION TYPES
//
// At the 11:30 reading of 18 Sep 2026 the screener drew "Unusual · 3 conditions · 80 contracts" on NIFTY. The
// server had tallied the store's complete reason SENTENCE, and a sentence carries its own multiple — "volume
// 206.0x its own time-of-day median" and "volume 781.9x its own time-of-day median" are ONE RULE at two
// contracts. NIFTY's 80 flagged contracts wrote 124 different sentences. The badge clamped 124 to three.
//
// Three separate things were wrong, and all three are checked here:
//
//   1. the COUNT counted renderings, not rules. Two rules exist, §3.2 and §3.3, so the number can be 1 or 2.
//   2. the ORDER rested on that count, which made it "how many different NUMBERS appeared under this name",
//      and ties fell out in whatever order the rows arrived in.
//   3. the CELL carried every sentence of every flagged contract — 5,015 characters on NIFTY, clamped to two
//      lines on screen and read out in full by a screen reader.
// =================================================================================================================
const METRICS_PY=fs.readFileSync(path.join(__dirname,'..','..','market_data','derivatives','metrics.py'),'utf8');
ok(()=>{ // ONE REGISTRY, THREE FILES, NO DRIFT. The thresholds decide what is flagged; a copy that slips is a
 // page describing a comparison the store never made.
 assert.ok(/^UNUSUAL_VOL_TOD_RATIO = 2\.0$/m.test(METRICS_PY),'metrics.py pins the §3.2 multiple');
 assert.ok(/^VOL_OI_SPIKE_RATIO = 1\.0$/m.test(METRICS_PY),'metrics.py pins the §3.3 multiple');
 assert.ok(/^UNUSUAL_RULES_VERSION = 1$/m.test(METRICS_PY),'and a version on the definitions');
 assert.ok(/^RULE_VOL_TOD = "vol_tod_median"$/m.test(METRICS_PY));
 assert.ok(/^RULE_DAY_VOL_VS_PREV_OI = "day_vol_vs_prev_oi"$/m.test(METRICS_PY));
 assert.ok(/^UNUSUAL_RULES_VERSION=1$/m.test(DERIV_PY),'derivatives.py mirrors the version');
 assert.ok(/^UNUSUAL_VOL_TOD_RATIO=2\.0$/m.test(DERIV_PY),'and both multiples');
 assert.ok(/^VOL_OI_SPIKE_RATIO=1\.0$/m.test(DERIV_PY));
 assert.ok(/^RULE_VOL_TOD='vol_tod_median'$/m.test(DERIV_PY));
 assert.ok(/^RULE_DAY_VOL_VS_PREV_OI='day_vol_vs_prev_oi'$/m.test(DERIV_PY));
 // and the browser's own copy, which exists ONLY to read an older server's sentences back into a rule
 assert.deepEqual([...L.UNUSUAL_RULE_DEFS.map(r=>r.rule_id)],['vol_tod_median','day_vol_vs_prev_oi']);
 assert.deepEqual([...L.UNUSUAL_RULE_DEFS.map(r=>r.comparator)],['>=','>'],
  'the comparators are the ones the code performs: §3.2 is at-least, §3.3 is more-than');
 assert.deepEqual([...L.UNUSUAL_RULE_DEFS.map(r=>r.threshold)],[2,1]);
 assert.deepEqual([...L.UNUSUAL_RULE_DEFS.map(r=>r.short_label)],['Vol vs median','Day vol vs prev OI']);
 assert.ok(/short_label="Vol vs median"/.test(METRICS_PY)&&/'short_label':'Vol vs median'/.test(DERIV_PY),
  'and the short form is one string in all three files too');
 assert.equal(L.UNUSUAL_RULE_MAX,2,'two rules exist and there is no third to trip');
 // the comparator each Python side performs, at the one place it performs it
 assert.ok(/volume_vs_tod\.ratio >= floors\.unusual_vol_tod_ratio/.test(METRICS_PY),'§3.2 fires at >=');
 assert.ok(/is_spike=ratio > threshold/.test(METRICS_PY),'§3.3 fires at >');
 // the reason WORDING is written and read back by the same pair of patterns, in the same file
 assert.ok(/RULE_VOL_TOD: "volume \{value:\.1f\}x its own time-of-day median"/.test(METRICS_PY));
 assert.ok(/RULE_DAY_VOL_VS_PREV_OI: "day volume \{value:\.1f\}x yesterday's OI"/.test(METRICS_PY));
 assert.ok(/reasons: list\[str\] = \[t\.reason for t in triggers\]/.test(METRICS_PY),
  'the stored sentence is DERIVED from the structured trigger, so the two cannot drift');
 // every side reads the same sentence back to the same rule
 for(const [text,id] of [['volume 206.0x its own time-of-day median','vol_tod_median'],
  ['volume 781.9x its own time-of-day median','vol_tod_median'],
  ["day volume 28.3x yesterday's OI",'day_vol_vs_prev_oi'],
  ["day volume 1.2x yesterday's OI",'day_vol_vs_prev_oi']])
  assert.equal(L.unusualRuleOf(text),id,text);
 assert.equal(L.unusualRuleOf('something the store has never written'),L.UNUSUAL_RULE_UNCLASSIFIED,
  'a sentence no rule claims stays itself - it is never folded into a rule that did not fire');
 assert.equal(L.unusualRuleOf(''),'','and nothing at all is nothing, not an unclassified condition');
});
ok(()=>{ // NUMERIC VARIANTS OF ONE RULE COUNT ONCE. This is the defect, reproduced from the captured case.
 const nifty={underlying:'NIFTY',unusual:80,options:104,volume_to_oi_contracts:104,
  unusual_reasons:{'volume 206.0x its own time-of-day median':1,'volume 781.9x its own time-of-day median':1,
   'volume 73.1x its own time-of-day median':1,'volume 34.3x its own time-of-day median':1,
   "day volume 28.3x yesterday's OI":1,"day volume 23.0x yesterday's OI":1,
   "day volume 35.9x yesterday's OI":1,"day volume 1.2x yesterday's OI":3}};
 assert.equal(Object.keys(nifty.unusual_reasons).length,8,'eight distinct sentences...');
 assert.equal(L.unusualConditions(nifty),2,'...two rules');
 assert.ok(L.unusualConditions(nifty)<3,'the inspected two-family case CANNOT display three conditions');
 assert.equal(L.unusualDegree(nifty),2,'and the colour step moves with the rule count, not the multiple');
 // THREE NUMBERS THAT ARE NOT THE SAME NUMBER, and the badge keeps them apart
 assert.equal(L.unusualContractCount(nifty),80,'contracts flagged');
 assert.equal(L.unusualObservations(nifty),10,'times a rule fired');
 assert.equal(L.unusualText(nifty),'Unusual · 2 conditions · 80 contracts');
 assert.ok(!/3 conditions/.test(L.unusualText(nifty)));
 // ...and the server's own structured tally says the same, with the contract count it alone can know
 const served={underlying:'NIFTY',unusual:80,options:104,volume_to_oi_contracts:104,
  unusual_rule_count:2,unusual_observations:124,
  unusual_rules:[{rule_id:'vol_tod_median',label:'Volume vs its own median',short_label:'Vol vs median',
   contracts:74,observations:74,
   comparator:'>=',threshold:2,measure:'cumulative volume so far today',baseline_label:'its own median',
   sample_label:'session',value_max:781.9,value_max_symbol:'NIFTY2692223300CE',baseline_at_max:1200,
   sample_count_at_max:10},
   {rule_id:'day_vol_vs_prev_oi',label:'Day volume vs previous-close OI',short_label:'Day vol vs prev OI',
    contracts:50,observations:50,
    comparator:'>',threshold:1,value_max:57.8,value_max_symbol:'NIFTY2692223300PE'}]};
 assert.equal(L.unusualConditions(served),2);
 assert.equal(L.unusualContractCount(served),80);
 assert.equal(L.unusualObservations(served),124,'observations are the server\'s own total, never the rule count');
 assert.equal(L.unusualEvidenceSummary(served),'2 conditions · 80 contracts flagged · 124 observations');
 assert.equal(L.unusualRuleBadge(served.unusual_rules[0]),'Volume vs its own median · 74');
 // THE CELL'S WIDTH IS THE CELL'S WIDTH. The badge in the table uses the tab's existing column words; the
 // drawer, which has room, uses the rule's full name.
 assert.equal(L.unusualRuleBadge(served.unusual_rules[0],true),'Vol vs median · 74');
 assert.equal(L.unusualRuleBadge({rule_id:'x',label:'Only a long name',contracts:1,observations:1},true),
  'Only a long name · 1','a rule with no short form keeps its full name rather than losing it');
 assert.ok(/^Vol vs median · /.test(L.unusualBadgesText(served)),
  'the cell line is built from the short labels');
 assert.ok(L.unusualRuleDetail(served.unusual_rules[0]).includes('at least 2.0'),
  'the drawer states the comparator and the threshold, not just the reading');
 assert.ok(L.unusualRuleDetail(served.unusual_rules[0]).includes('NIFTY2692223300CE'),
  'and names the contract the largest reading belongs to');
 assert.ok(L.unusualRuleDetail(served.unusual_rules[0]).includes('over 10 sessions'),
  'and how many observations the baseline it was measured against stands on');
 // a sentence tally cannot say how many CONTRACTS tripped a rule, and does not pretend to
 assert.equal(L.unusualRules(nifty)[0].contracts,null);
 assert.ok(L.unusualRuleBadge(L.unusualRules(nifty)[0]).endsWith(' seen'),
  'it says what it CAN count rather than inventing a contract count');
});
ok(()=>{ // a flag with no condition named is still a flag; and nothing flagged is not "not measured"
 assert.equal(L.unusualDegree({unusual:3,unusual_rules:[],options:5}),1,'a named flag with no rule still marks');
 assert.equal(L.unusualBadgesText({unusual:3,unusual_rules:[],options:5}),'the store named no condition');
 assert.equal(L.unusualDegree({unusual:0,options:5}),0);
 assert.equal(L.unusualShort({unusual:0,options:5}),L.UNUSUAL_NONE);
 assert.equal(L.unusualShort({unusual:0,options:0,volume_to_oi_contracts:0,volume_baseline_contracts:0}),
  L.UNUSUAL_NOT_MEASURED,'a futures-only book was not measured, and that is not a quiet book');
 assert.equal(L.unusualConditions(null),0);
 assert.equal(L.unusualObservations(null),0);
});
ok(()=>{ // THE CELL IS CONCISE, AND THE FULL EVIDENCE IS STILL THERE
 const evidence=JSON.parse(fs.readFileSync(path.join(__dirname,'..','docs','derivative-audit-2026-09-19',
  'backend-evidence.json'),'utf8'));
 const nifty=evidence.screener.rows.find(r=>r.underlying==='NIFTY');
 assert.equal(Object.keys(nifty.unusual_reasons).length,124,'the captured case: 124 distinct sentences');
 // what the cell used to carry: every sentence of every flagged contract, concatenated
 const before=Object.entries(nifty.unusual_reasons).map(([k,v])=>`${k} ${v}`).join(' · ');
 assert.ok(before.length>4000,`the old reason line was ${before.length} characters`);
 // what it carries now: the counts and ONE BADGE PER RULE
 const after=`${L.unusualShort(nifty)} ${L.unusualBadgesText(nifty)}`;
 assert.equal(L.unusualConditions(nifty),2,'two rules, from the captured 124 sentences');
 assert.ok(after.length<200,`the cell text must stay short: ${after.length} characters — ${after}`);
 assert.ok(after.length*20<before.length,'and it is a different order of magnitude, not a trim');
 // the spoken row says WHY it is flagged, in the same concise form - it used to say nothing at all
 const spoken=L.groupRowLabel(nifty,3);
 assert.ok(spoken.includes('Unusual · 2 conditions'),`the row must speak the counts: ${spoken}`);
 assert.ok(spoken.length<900,`and the spoken row must stay readable: ${spoken.length} characters`);
 for(const text of [after,spoken])
  assert.ok(!/206\.0|781\.9|28\.3/.test(text),'no multiple is concatenated into the cell or the row');
});
ok(()=>{ // the widget draws badges and a drawer, and no longer concatenates reasons anywhere
 const widget=SRC('UnusualWidget.tsx'),code=CODE('UnusualWidget.tsx');
 assert.ok(!/unusualReasonsText/.test(code),'the concatenated reason line is gone from the cell');
 assert.ok(/unusualRuleBadge\(rule\)/.test(code),'one badge per rule is drawn');
 assert.ok(/UNUSUAL_RULE_MAX/.test(code),'and the line cannot grow past the number of rules that exist');
 assert.ok(/function UnusualEvidence\(\{group,onClose\}/.test(widget),'the evidence drawer exists');
 assert.ok(/<UnusualEvidence group=\{evidence\} onClose=/.test(code),'and is mounted once for the whole list');
 assert.ok(/unusualEvidence\(group\)/.test(code),'it lists the contracts the SERVER sent');
 assert.ok(/unusualTriggerText\(trigger,rules\)/.test(code),
  'each with its value, its threshold, its baseline and its sample count');
 assert.ok(/UNUSUAL_EVIDENCE_NONE/.test(code),
  'and a response that carried no contracts says so rather than showing an empty list');
 // the 5,015-character hover title is gone with it
 assert.ok(!/title:degree\?/.test(code),'no cell carries every reason as a hover title either');
});
ok(()=>{ // ONE TRIGGER, READ OUT: value, comparator, threshold, baseline, sample count. Never a nought for a
 // number the store does not carry.
 const rules=[{rule_id:'vol_tod_median',label:'Volume vs its own median',contracts:1,observations:1}];
 assert.equal(L.unusualTriggerText({rule_id:'vol_tod_median',value:781.9,comparator:'>=',threshold:2,
  baseline:1200,sample_count:10},rules),
  `Volume vs its own median · 781.9${TIMES} · >= 2.0${TIMES} · baseline 1.2k · 10 observations`);
 assert.ok(L.unusualTriggerText({rule_id:'vol_tod_median',value:null,text:'x'},rules).includes('no value stored'),
  'a condition the store recorded without a number says so rather than printing a nought');
 assert.equal(L.unusualTriggerText(null),'');
});
ok(()=>{ // THE ORDER IS DETERMINISTIC, AND THE TIE-BREAKER IS THE NAME
 const fn=DERIV_PY.slice(DERIV_PY.indexOf('def _screener_groups'),DERIV_PY.indexOf('def screener(self,filters'));
 // FOUR KEYS, and the last is the NAME. The third is premium traded where the reading captured one and
 // volume where it did not: sorting on a premium that is None for every row is the arrival order wearing a
 // caption. `_ranking` names whichever it was, and the page prints that name.
 assert.ok(/out\.sort\(key=lambda g:\(-\(g\['unusual_rule_count'\] or 0\),-\(g\['unusual'\] or 0\),\s*-\(g\['premium_cr'\] if g\['premium_cr'\] is not None else \(g\['volume'\] or 0\)\),\s*g\['underlying'\] or ''\)\)/
  .test(fn),'four keys, and the last is the instrument name so a tie is stable');
 // AND A SUM OF NOTHING IS NOT NOUGHT: a name with no premium captured carries no premium, never 0.0
 assert.ok(/group\['premium_cr'\]=_round\(group\['premium_cr'\],2\) if group\['has_premium'\] else None/.test(fn),
  'a name whose contracts carried no premium must show a dash, not a fabricated zero');
 assert.ok(/if row\.get\('premium_cr'\) is not None:/.test(fn),'and only a real number is added to the sum');
 assert.ok(!/-len\(g\['unusual_reasons'\]\)/.test(fn),'nothing is ordered by how many SENTENCES were written');
 assert.ok(!/group\['unusual_reasons'\]\[reason\]=/.test(fn),'and nothing tallies a sentence at all');
 assert.ok(/group\['unusual_rule_count'\]=len\(group\['unusual_rules'\]\)/.test(fn),
  'the condition count is a count of RULES');
 assert.ok(/group\['unusual_observations'\]\+=1/.test(fn),'observations are counted apart from contracts');
 assert.ok(/group\['unusual_rules'\]\[rule_id\]\['contracts'\]\+=1/.test(fn),
  'and a contract is counted ONCE per rule however many times it appears');
 // the browser restates the same order, so a client-side narrowing cannot quietly change it
 const a={underlying:'BBB',unusual:5,unusual_rule_count:2,premium_cr:10};
 const b={underlying:'AAA',unusual:5,unusual_rule_count:2,premium_cr:10};
 assert.deepEqual([...L.unusualRank(a)],[2,5,10,'BBB']);
 assert.deepEqual([...L.unusualRank(b)],[2,5,10,'AAA']);
 assert.ok(L.unusualRank(b)[3]<L.unusualRank(a)[3],'two rows level on all three counts still have an order');
});
ok(()=>{ // THE SORT THAT IS ACTUALLY IN FORCE IS THE ONE THE PAGE PRINTS
 assert.ok(/^SCREENER_RANK_LABEL='Unusual first'$/m.test(DERIV_PY),'the server names its own order');
 assert.ok(/SCREENER_RANK_TEXT=\('One row per instrument, ordered by: most distinct condition types/.test(DERIV_PY));
 assert.ok(/^CONTRACT_RANK_LABEL='Largest premium traded'$/m.test(DERIV_PY),
  'and the contract list is a different order with a different name');
 assert.ok(/def _ranking\(view,applied=None\):/.test(DERIV_PY),'one helper answers "what is this sorted by"');
 assert.ok(/ranking=self\._ranking\(view,force\['applied'\]\)/.test(DERIV_PY),'and it travels with the rows');
 // A SORT KEY HAS TO BE A COLUMN THE READING FILLED. Premium is null on every row of a rebuilt reading, so
 // ordering by it there is insertion order wearing a label - and this response would be printing a sort the
 // list does not obey, which is the exact drift this whole block exists to prevent.
 assert.ok(/'ranked_by':\('premium_cr' if not degraded else FALLBACK_RANK_FIELD\)/.test(DERIV_PY),
  'and it names the field actually ranked by, which is not always premium');
 assert.ok(/FALLBACK_RANK_TEXT=\(/.test(DERIV_PY)&&!MARK_WORD.test(pySentence('FALLBACK_RANK_TEXT')),
  'the swap is stated in the owner\u2019s words, never assumed');
 const ranking={view:'underlying',label:'Unusual first',text:'ordered by ...',
  keys:[{field:'unusual_rule_count',direction:'desc',text:'most distinct condition types'},
   {field:'underlying',direction:'asc',text:'then the name A to Z'}]};
 assert.equal(L.rankingLabel(ranking),'Unusual first');
 assert.equal(L.rankingLabel(null),'');
 assert.deepEqual([...L.rankingText(ranking)],['ordered by ...','most distinct condition types','then the name A to Z']);
 assert.deepEqual([...L.rankingText(null)],[]);
 // THE PAGE TAKES ROW ONE OF THAT LIST AND SAYS WHICH ORDER IT CAME FROM. It used to say "Busiest by
 // premium" over a list the screener had not ordered by premium for a long time.
 const page=CODE('index.tsx');
 assert.ok(/const rankLabel=rankingLabel\(screener\.data\?\.ranking\)/.test(page),
  'the page reads the server\u2019s own ranking label');
 assert.ok(/resolveTabSymbol\(filters\.underlying,target\?\.underlying,ranked,/.test(page),
  'and hands it, with row one, to the one place the tab resolves its symbol');
 // the phrase is gone from the CODE; the comments that explain why it was wrong are meant to stay
 for(const file of ['logic.ts','index.tsx','UnusualWidget.tsx','ScreenerSection.tsx'])
  assert.ok(!/Busiest by premium/.test(CODE(file)),`${file} must not claim a premium sort it does not perform`);
 // the reader can read the keys themselves, in the server's words, on the screener's own control
 const rail=SRC('ScreenerSection.tsx');
 assert.ok(/heading:'The order these rows are in',lines:\[UNUSUAL_SORT_TEXT,\.\.\.rankingText\(body\?\.ranking\)\]/
  .test(rail),'the screener prints the server\u2019s ranking keys');
 assert.ok(/most distinct conditions/.test(L.UNUSUAL_SORT_TEXT)&&/A to Z/.test(L.UNUSUAL_SORT_TEXT),
  'and the sentence over them names the tie-breaker too');
});
ok(()=>{ // the server carries a trigger's numbers, and gets them from the store rather than recomputing them
 assert.ok(/def _unusual_triggers\(cls,row\):/.test(DERIV_PY));
 for(const key of ['rule_id','rule_version','value','comparator','threshold','baseline','sample_count'])
  assert.ok(new RegExp(`'${key}':`).test(DERIV_PY),`a trigger must carry ${key}`);
 assert.ok(/'vol_tod_median','vol_tod_sessions'/.test(DERIV_PY),
  'the baseline column is selected, so a multiple never travels without its denominator');
 assert.ok(/'vol_oi_ratio','vol_oi_prev_oi'/.test(DERIV_PY));
 assert.ok(/'unusual_triggers':self\._unusual_triggers\(row\)/.test(DERIV_PY),'and every screener row carries them');
 // a stored row is READ, never rewritten: the metrics table keeps the columns it always had
 assert.ok(!/alter table metrics/i.test(DERIV_PY)&&!/ALTER TABLE metrics/i.test(METRICS_PY),
  'no stored row is rewritten and no column is added for this');
 assert.ok(/"unusual_reasons": ",".join\(self\.unusual_reasons\)/.test(METRICS_PY),
  'the stored row is written exactly as it always was');
});

// =================================================================================================================
// F&O CAPTURE HEALTH, BESIDE THE APP-WIDE DATA PILL
//
// THE DEFECT THESE CLOSE. `/api/derivatives/capture` was built, tested, and called by nothing at all. It is the
// one route that answers "was anything actually MEASURED in the F&O store", and it exists because the app's data
// pill describes the CASH feed — prices and patterns. On 18 Sep 2026 those two disagreed for an entire afternoon:
// F&O capture stopped at 11:30 IST, every contract row after it came back with no premium and no spot, and the
// pill stayed green the whole time because the cash feed really was healthy. Nothing anywhere on screen said the
// F&O book had gone dark.
//
// So the pill now carries a SECOND chip, from that route, and the rules below are what keep it honest:
// it appears only when the capture is a caveat, it never touches the cash feed's own tone or wording, and every
// sentence in it is the server's.
// =================================================================================================================
const LAYOUT=p=>fs.readFileSync(path.join(__dirname,'..','src','layout',p),'utf8');
/** `fnoCapture.ts` imports React and the app's `api`; neither is needed to judge its PURE half, so both are
 *  stubbed. Nothing below calls the hook — a hook is behaviour, and what is checked here is the wording. */
const F=(()=>{
 const code=ts.transpileModule(LAYOUT('fnoCapture.ts'),{compilerOptions:{module:ts.ModuleKind.CommonJS,
  target:ts.ScriptTarget.ES2022}}).outputText;
 const stub={react:{useState:()=>[null,()=>{}],useEffect:()=>{},default:{}},
  '../model':{api:()=>Promise.resolve(null)}};
 const ctx={exports:{},require:(n)=>{const key=n==='react'?'react':n;
  if(!(key in stub))throw Error('unexpected import: '+n);return stub[key];},process:{env:{}},
  setInterval:()=>0,clearInterval:()=>{}};
 vm.runInNewContext(code,ctx);return ctx.exports;
})();
const CAP_TEXT='Part of this 15-min reading was not captured.';
ok(()=>{ // NOTHING TO SAY IS NOTHING SHOWN. A badge that is always on is a badge nobody reads.
 for(const v of [null,undefined,{},{state:''},{state:'   '}])
  assert.equal(F.fnoCaptureView(v),null,`a capture with no state draws no chip: ${JSON.stringify(v)}`);
});
ok(()=>{ // a HEALTHY capture draws no chip at all, in every one of the three states that are healthy
 for(const state of ['complete','no_eligible_rows','filtered_out'])
  assert.equal(F.fnoCaptureView({state,healthy:true,state_text:'x'}),null,
   `${state} is not a caveat and must not put an alert on the data pill`);
});
ok(()=>{ // THE THREE CAVEAT STATES, each with its OWN words - "nothing captured" and "partly captured" are
 // different facts about the market and must never share a wording
 const seen=new Set();
 for(const [state,label] of [['missing_capture','F&O not captured'],
  ['partial_capture','F&O partly captured'],['failed','F&O store unreadable']]){
  const view=F.fnoCaptureView({state,healthy:false,state_text:CAP_TEXT,at:'2026-09-18 15:45:00'});
  assert.ok(view,`${state} must reach the reader`);
  assert.equal(view.label,label);
  assert.ok(!seen.has(view.label),`${state} reuses another state's wording: ${view.label}`);
  seen.add(view.label);
  // the DETAIL is the server's sentence verbatim - never a paraphrase written on this side
  assert.equal(view.detail,CAP_TEXT,'the reason is the server\'s own sentence');
  // and the headline names WHICH reading, because a caveat with no time on it is a claim with no subject
  assert.ok(view.headline.includes('15:45'),`${state} must name its reading: ${view.headline}`);
 }
});
ok(()=>{ // the reading is OPTIONAL: a capture with no `at` says the label alone rather than an empty time
 const view=F.fnoCaptureView({state:'failed',healthy:false,state_text:CAP_TEXT});
 assert.equal(view.headline,'F&O store unreadable');
 assert.ok(!/undefined|null|NaN|:/.test(view.headline),`no empty clock leaks into it: ${view.headline}`);
});
ok(()=>{ // AN UNKNOWN STATE IS NOT A GOOD STATE. The server's own `healthy:false` outranks this table.
 const flagged=F.fnoCaptureView({state:'something_new',healthy:false,state_text:''});
 assert.ok(flagged,'a state this app does not know, that the server calls unhealthy, still reaches the reader');
 assert.ok(/unrecognised/i.test(flagged.label),`it says so plainly: ${flagged.label}`);
 assert.ok(flagged.detail.length>0,'and it never shows an empty reason');
 // but an unknown state the server has NOT called unhealthy is not invented into an alarm either
 for(const v of [{state:'something_new'},{state:'something_new',healthy:true}])
  assert.equal(F.fnoCaptureView(v),null,'an unknown state is never turned into an alarm on its own');
});
ok(()=>{ // THE TWO FEEDS ARE KEPT APART, in words the reader can read - this is the whole point of the route
 const view=F.fnoCaptureView({state:'partial_capture',healthy:false,state_text:CAP_TEXT});
 assert.ok(/F&O capture only/.test(F.FNO_SEPARATE_TEXT),'it says which feed it describes');
 assert.ok(/[Pp]rices and patterns/.test(F.FNO_SEPARATE_TEXT),'and names the one it does NOT');
 assert.ok(view.a11y.includes(F.FNO_SEPARATE_TEXT),'a screen reader is told the same thing');
 assert.ok(view.a11y.includes(CAP_TEXT),'along with the server\'s reason');
});
ok(()=>{ // §5 over every string this file can put on screen: nothing here predicts, and nothing says "mark"
 const banned=/\b(will|expect|expected|forecast|predict|prediction|likely|target price|breakout|momentum|bullish|bearish|buy signal|sell signal|uptrend|downtrend|rally|reversal)\b/i;
 const said=VISIBLE(LAYOUT('fnoCapture.ts')).filter(t=>banned.test(t));
 assert.deepEqual(said,[],`fnoCapture.ts: a string the reader can reach must not predict: ${said.join(' | ')}`);
 const left=VISIBLE(LAYOUT('fnoCapture.ts')).filter(t=>MARK_WORD.test(t)&&!IDENTIFIER.test(t));
 assert.deepEqual(left,[],`fnoCapture.ts: say "15-min reading(s)", not "mark": ${left.join(' | ')}`);
 // every sentence the view can produce, whatever the route answers
 for(const state of ['missing_capture','partial_capture','failed','something_new']){
  const view=F.fnoCaptureView({state,healthy:false,state_text:CAP_TEXT,at:'2026-09-18 15:45:00'});
  for(const text of [view.label,view.headline,view.detail,view.a11y]){
   assert.ok(String(text).length>0,`${state}: every sentence must exist`);
   assert.ok(!banned.test(String(text)),`${state}: a capture sentence must not predict: ${text}`);
   assert.ok(!MARK_WORD.test(String(text))||IDENTIFIER.test(String(text)),
    `${state}: a capture sentence says "mark": ${text}`);
  }
 }
 assert.ok(!banned.test(F.FNO_SEPARATE_TEXT)&&!MARK_WORD.test(F.FNO_SEPARATE_TEXT));
});
ok(()=>{ // THE ROUTE IS ACTUALLY CALLED. This is the defect: it was built, tested and wired to nothing.
 const src=LAYOUT('fnoCapture.ts');
 assert.ok(/api\('\/api\/derivatives\/capture'\)/.test(src),
  'the capture health route must be the one this reads');
 // ONE poll for every consumer: the pill and the panel it opens are siblings and must not each run their own
 assert.ok(/const listeners=new Set/.test(src)&&/export function useFnoCapture/.test(src),
  'one shared subscription, not one request per component');
 // a request that did not arrive claims NOTHING - it must never manufacture a red state out of a failed fetch
 assert.ok(/\.catch\(\(\)=>\{/.test(src),'a failed fetch is swallowed, never turned into a verdict');
 // and the pilot really serves it
 assert.ok(/@app\.get\('\/api\/derivatives\/capture'\)/
  .test(fs.readFileSync(path.join(__dirname,'..','server','kanida_pilot','app.py'),'utf8')),
  'the pilot must serve /api/derivatives/capture');
});
ok(()=>{ // THE CHIP IS ON THE SURFACE, on the pill and in the panel behind it, and it is AMBER
 const bar=fs.readFileSync(path.join(__dirname,'..','src','layout','TopBar.tsx'),'utf8');
 assert.ok(/const fno=fnoCaptureView\(useFnoCapture\(\)\)/.test(bar),'the pill reads the capture health');
 assert.ok(/const badge=!fno\?null:/.test(bar),'and draws nothing at all when there is no caveat');
 assert.ok(/backgroundColor:C\.amberBg,borderWidth:1,borderColor:C\.amber/.test(bar),
  'a caveat on the data is amber, the convention this tab already works to');
 // THE CASH FEED'S OWN VERDICT IS UNTOUCHED. Not one of tone / col / a11y may be computed from `fno`:
 // two feeds, two chips, and a healthy cash feed still reads as healthy.
 for(const line of NO_COMMENTS(bar).split('\n')){
  if(/^\s*(const|let)\s+(tone|col|bg|a11y|stale|pill)\s*=/.test(line))
   assert.ok(!/\bfno\b/.test(line),`the cash feed's own status must not be derived from F&O capture: ${line}`);
 }
 const panel=fs.readFileSync(path.join(__dirname,'..','src','layout','DataStatusPanel.tsx'),'utf8');
 assert.ok(/fno=\{fno\}/.test(panel)&&/\{!!fno&&</.test(panel),
  'the panel behind the pill carries the full sentence, and only when there is one');
 assert.ok(/\{FNO_SEPARATE_TEXT\}/.test(panel),'with the line that says which feed it describes');
});


// ================================================================================================================
// BLOCK 1 — src/derivative/summary.ts
//
// The panel beside the screener writes SENTENCES, and a sentence is harder to audit than a number. These checks
// hold the four things that would make one false: a later reading changing an earlier one, elapsed time counted
// across an interval nobody measured, a strike named that was not moving, and a behaviour claimed with no
// baseline behind it.
// ================================================================================================================
const b1SUM=load('src/derivative/summary.ts',n=>{
 if(/\/logic$/.test(n))return L;
 throw Error('unexpected import: '+n);
});
/** The summary module runs in its own vm realm, so an array it returns does not share this
 *  file's Array.prototype. Every array that crosses the boundary is copied before it is compared. */
const b1arr=x=>Array.prototype.slice.call(x||[]);


/** A grid the way the server serves one: ten slots, one point per 15-minute mark. */
const b1MARKS=['09:15','09:30','09:45','10:00','10:15','10:30','10:45','11:00'].map(t=>`2026-09-18 ${t}:00`);
const b1CE_LADDER=[23350,23400,23450,23500,23550],b1PE_LADDER=[23300,23250,23200,23150,23100];
const b1gslot=(k,type,row,deltas,prices)=>({
 slot:`${type}${k}`,option_type:type,row,label:`${k} ${type}`,present:true,
 tradingsymbol:`NIFTY${k}${type}`,instrument_token:k,strike:k,previous_close_oi:100000,
 points:b1MARKS.map((at,i)=>({at,oi:100000+(deltas[i]||0),delta_oi:deltas[i],price:prices[i]})),
 direction:'building',direction_detail:{},marks:b1MARKS.length,marks_with_delta:b1MARKS.length,
 latest_delta_oi:deltas[deltas.length-1],peak_abs_delta_oi:0,marks_with_price:b1MARKS.length,
 latest_price:prices[prices.length-1],flow:{},missing_text:null});
const b1FLAT_D=[0,0,0,0,0,0,0,0],b1FLAT_P=[20,20,20,20,20,20,20,20];
const b1GRID={rows:[
 // 23,350 CE buys from 10:15 and never stops
 b1gslot(23350,'CE','calls',[0,0,0,0,20000,40000,60000,80000],[80,80,80,80,95,110,125,140]),
 // 23,400 CE joins at 10:30; by 11:00 its premium is back where it was an hour earlier
 b1gslot(23400,'CE','calls',[0,0,0,0,0,25000,50000,50000],[55,55,55,55,55,66,78,56]),
 // 23,450 CE joins at 10:45 only
 b1gslot(23450,'CE','calls',[0,0,0,0,0,0,30000,30500],[40,40,40,40,40,40,52,40]),
 b1gslot(23500,'CE','calls',b1FLAT_D,b1FLAT_P),
 b1gslot(23550,'CE','calls',b1FLAT_D,b1FLAT_P),
 ...b1PE_LADDER.map(k=>b1gslot(k,'PE','puts',b1FLAT_D,b1FLAT_P)),
],underlying:'NIFTY',expiry:'2026-09-25',marks:b1MARKS};

const b1seen=b1SUM.observe(b1GRID);
const b1atTime=(list,t)=>list.find(o=>String(o.at).endsWith(`${t}:00`));

// --- the window is the tab's own: nothing is claimed before a reading an hour back exists ------------------
ok(()=>{
 assert.equal(b1seen.length,b1MARKS.length,'one observation per mark');
 for(const t of ['09:15','09:30','09:45','10:00'])
  assert.equal(b1atTime(b1seen,t).calls.state,'no_baseline',
   `${t} has no reading ${L.SIGNAL_WINDOW_MINUTES} minutes behind it`);
});
// --- the story: appeared, broadened, broadened, narrowed ----------------------------------------------------
ok(()=>{
 assert.equal(b1atTime(b1seen,'10:15').calls.state,'appeared');
 assert.deepEqual(b1arr(b1atTime(b1seen,'10:15').calls.strikes),[23350]);
 assert.equal(b1atTime(b1seen,'10:30').calls.state,'broadened');
 assert.deepEqual(b1arr(b1atTime(b1seen,'10:30').calls.strikes),[23350,23400]);
 assert.deepEqual(b1arr(b1atTime(b1seen,'10:30').calls.joined),[23400]);
 assert.equal(b1atTime(b1seen,'10:45').calls.state,'broadened');
 assert.deepEqual(b1arr(b1atTime(b1seen,'10:45').calls.strikes),[23350,23400,23450]);
 assert.equal(b1atTime(b1seen,'11:00').calls.state,'narrowed');
 assert.deepEqual(b1arr(b1atTime(b1seen,'11:00').calls.strikes),[23350]);
 assert.deepEqual(b1arr(b1atTime(b1seen,'11:00').calls.left),[23400,23450]);
});
// --- THREE SCANS ARE 30 MINUTES, NOT 45. Elapsed time is measured between two stamps, never counted one per
//     scan. 10:15 starts the run, so 10:45 is its third reading and its thirtieth minute. -------------------
ok(()=>{
 assert.equal(b1atTime(b1seen,'10:15').calls.elapsed_minutes,0,'the first reading of a run has run for nothing');
 assert.equal(b1atTime(b1seen,'10:15').calls.scans,1);
 assert.equal(b1atTime(b1seen,'10:45').calls.scans,3,'three readings');
 assert.equal(b1atTime(b1seen,'10:45').calls.elapsed_minutes,30,'and thirty minutes, not forty-five');
 assert.equal(b1SUM.minutesText(30),'30 minutes');
 assert.equal(b1SUM.minutesText(75),'1 hour 15 minutes');
 assert.equal(b1SUM.minutesText(0),'');
 assert.equal(b1SUM.minutesText(null),'');
});
// --- the busiest strike is the one that moved most ----------------------------------------------------------
ok(()=>{
 const o=b1atTime(b1seen,'10:45');
 assert.equal(o.calls.lead,23350);
 assert.equal(o.calls.first_at,b1MARKS[4]);
 assert.equal(o.puts.behaviour,'none','the put side carries nothing in this fixture');
});
// --- NOTHING AFTER THE SELECTED READING IS READ --------------------------------------------------------------
ok(()=>{
 const cut=b1SUM.observe(b1GRID,b1MARKS[5]);
 assert.equal(cut.length,6);
 assert.deepEqual(b1arr(cut.map(o=>o.at)),b1MARKS.slice(0,6));
 for(let i=0;i<cut.length;i++)
  assert.deepEqual(cut[i],b1seen[i],`reading ${i} repainted when the session grew past it`);
});
// --- AN UNOBSERVED INTERVAL IS NOT MEASURED TIME ---------------------------------------------------------------
ok(()=>{
 const holed=JSON.parse(JSON.stringify(b1GRID));
 for(const row of holed.rows){row.points[7].delta_oi=null;row.points[7].price=null;}
 const walk=b1SUM.observe(holed);
 const last=walk[walk.length-1];
 assert.equal(last.calls.state,'not_observed');
 assert.equal(last.covered,false);
 assert.equal(last.calls.measured,0);
 assert.equal(last.calls.first_at,b1MARKS[4]);
 assert.equal(last.calls.elapsed_minutes,30,'elapsed must not run across an interval nobody observed');
 assert.equal(last.calls.scans,3,'an unobserved reading is not a scan of the run');
 const said=b1SUM.narrate(last,{underlying:'NIFTY'});
 assert.match(said.headline,/no comparable value/i);
 assert.ok(!/continuing for/i.test(said.context),'an unobserved reading never claims a duration');
});
// --- the behaviour table is the tab's own, read whole ------------------------------------------------------------
ok(()=>{
 assert.equal(b1SUM.behaviourOf('CE','up','building'),'buying');
 assert.equal(b1SUM.behaviourOf('CE','down','building'),'writing');
 assert.equal(b1SUM.behaviourOf('CE','up','unwinding'),'short_covering');
 assert.equal(b1SUM.behaviourOf('CE','down','unwinding'),'buyers_exiting');
 assert.equal(b1SUM.behaviourOf('PE','up','building'),'buying');
 assert.equal(b1SUM.behaviourOf('PE','down','building'),'writing');
 assert.equal(b1SUM.behaviourOf('CE','flat','building'),'positions_added');
 assert.equal(b1SUM.behaviourOf('CE','flat','flat'),'quiet');
 for(const p of [L.NO_DIRECTION,'',null,undefined])assert.equal(b1SUM.behaviourOf('CE',p,'building'),'none');
 for(const o of [L.NO_DIRECTION,'',null,undefined])assert.equal(b1SUM.behaviourOf('CE','up',o),'none');
 assert.equal(b1SUM.DIRECTIONAL.indexOf('positions_added'),-1,'a position change alone starts no run');
});
// --- where the activity is, said without claiming a strike that is not in it --------------------------------------
ok(()=>{
 assert.equal(b1SUM.whereText([23350],'CE',b1CE_LADDER),'at 23,350 CE');
 assert.equal(b1SUM.whereText([23350,23400,23450],'CE',b1CE_LADDER),'across 23,350–23,450 CE');
 assert.equal(b1SUM.whereText([23350,23450],'CE',b1CE_LADDER),'at 23,350 and 23,450 CE');
 assert.equal(b1SUM.whereText([],'CE',b1CE_LADDER),'');
});
// --- THE CLAIM POLICY, enforced ---------------------------------------------------------------------------------
//
// Three tiers with data preconditions. These checks hold the two rules that matter: a behaviour word may never
// reach a headline, and wherever one IS written it must carry its competing explanation in the same sentence.
// Without the second rule a caveat can sit under an overconfident line and repair nothing.
const BEHAVIOUR_WORDS=/\b(buying|writing|short covering|buyers closing out|covering|unwinding)\b/i;
const COMPETING=/\b(spot|underlying)\b/i;
/** A behaviour word is also permitted when it is QUOTED and credited to the surface that wrote it —
 *  reporting what the ΔOI tile says is not the same as saying it in our own voice. */
const ATTRIBUTED=/labels this “[^”]+”/;
/** Every sentence of a narrative, so each can be tested on its own. */
const b1sentences=(said)=>[said.headline,said.observed,said.qualified,said.otherSide,said.session,
 said.context,...said.evidence].filter(Boolean)
 .flatMap(t=>String(t).split(/(?<=\.)\s+/)).map(t=>t.trim()).filter(Boolean);

ok(()=>{
 const spot={at:'2026-09-18 10:45:00',from:'2026-09-18 10:30:00',spot:23284,change:-18};
 const said=b1SUM.narrate(b1atTime(b1seen,'10:45'),
  {underlying:'NIFTY',expiry:'2026-09-25',ladder:b1CE_LADDER,spot});

 // TIER 1: the headline states what the numbers did, and names no participant
 assert.equal(said.headline,'Call positions build at another strike');
 assert.ok(!BEHAVIOUR_WORDS.test(said.headline),`a headline may not name a behaviour: ${said.headline}`);
 assert.ok(said.headline.split(' ').length<=10,'the headline stays inside ten words');

 // the interval is named at BOTH ends — a fifteen-minute product must say which fifteen minutes
 assert.equal(said.window,'10:30 → 10:45 IST');

 // TIER 1 body: measured change only
 assert.ok(!BEHAVIOUR_WORDS.test(said.observed),`the observation may not name a behaviour: ${said.observed}`);
 assert.match(said.observed,/Open interest rose and premium rose/);
 assert.match(said.observed,/23,450 CE is new this interval/);

 // TIER 2: the behaviour word exists, and never without its competing explanation
 assert.match(said.qualified,/Consistent with call buying/);
 assert.match(said.qualified,/spot also fell 18 points/);
 assert.match(said.qualified,/may explain some of the premium change/);

 // THE RULE, applied to every sentence in the narrative
 for(const line of b1sentences(said))
  if(BEHAVIOUR_WORDS.test(line))
   assert.ok(COMPETING.test(line)||ATTRIBUTED.test(line),
    `a behaviour word must carry its competing explanation in the same sentence: "${line}"`);

 // nothing forecasts, and nothing reduces the market to a direction
 const all=b1sentences(said).join(' ');
 for(const banned of [/\bwill\b/i,/\bexpect/i,/\btarget\b/i,/\bshould\b/i,/\bbullish\b/i,/\bbearish\b/i,
  /\bresistance\b/i,/\bsupport\b/i])
  assert.ok(!banned.test(all),`the summary must not say ${banned}`);
});

// --- TIER 2 is refused outright when the competing explanation cannot be stated -----------------------------------
ok(()=>{
 const said=b1SUM.narrate(b1atTime(b1seen,'10:45'),{underlying:'NIFTY',ladder:b1CE_LADDER});
 assert.match(said.qualified,/was not captured/,
  'with no spot move captured, the interpretation must say so rather than stand bare');
 for(const line of b1sentences(said))
  if(BEHAVIOUR_WORDS.test(line))assert.ok(COMPETING.test(line)||ATTRIBUTED.test(line),`unqualified behaviour claim: "${line}"`);
});

// --- "led throughout" is only said when every reading of the run was checked --------------------------------------
ok(()=>{
 const o=b1atTime(b1seen,'10:45');
 assert.equal(o.calls.lead_stable,true,'23,350 led at every reading of this run');
 const said=b1SUM.narrate(o,{underlying:'NIFTY',ladder:b1CE_LADDER});
 assert.match(said.session,/has carried the largest change at every reading since/);

 // now move the lead at the last reading and the claim must weaken
 const moved=JSON.parse(JSON.stringify(b1GRID));
 moved.rows[1].points[6].delta_oi=900000;      // 23,400 overtakes 23,350 at 10:45
 const walk=b1SUM.observe(moved);
 const late=walk[walk.length-2];
 assert.equal(late.calls.lead_stable,false,'the leading strike changed, so the claim is not available');
 const other=b1SUM.narrate(late,{underlying:'NIFTY',ladder:b1CE_LADDER});
 assert.match(other.session,/the leading strike has changed during the episode/);
 assert.ok(!/at every reading since/.test(other.session));
});

// --- a quiet reading does not claim the same positions are still held ---------------------------------------------
ok(()=>{
 const still=JSON.parse(JSON.stringify(b1GRID));
 for(const row of still.rows)row.points=row.points.map(p=>({...p,delta_oi:0,price:20}));
 const walk=b1SUM.observe(still);
 const last=walk[walk.length-1];
 assert.equal(last.calls.behaviour,'none');
 assert.equal(last.covered,true,'a reading that carried values is covered even when nothing moved');
 const said=b1SUM.narrate(last,{underlying:'NIFTY'});
 assert.match(said.headline,/No material change at this reading/);
 // outstanding open interest is a TOTAL. It is not evidence that the same positions are still held.
 assert.match(said.observed,/which is a total rather than a statement about whose positions remain/);
 assert.ok(!/positions from .* remain in place/i.test(said.observed));
 assert.ok(!/not observed/i.test(said.context),'a quiet reading is not an unobserved one');
});

// --- an unobserved reading says LAST CONFIRMED, and never looks like a current measurement ------------------------
ok(()=>{
 const holed=JSON.parse(JSON.stringify(b1GRID));
 for(const row of holed.rows){row.points[7].delta_oi=null;row.points[7].price=null;}
 const walk=b1SUM.observe(holed);
 const last=walk[walk.length-1];
 assert.equal(last.calls.state,'not_observed');
 assert.equal(last.calls.elapsed_minutes,30,'elapsed must not run across an interval nobody observed');
 const said=b1SUM.narrate(last,{underlying:'NIFTY'});
 assert.match(said.headline,/carried no comparable value/i);
 assert.match(said.session,/Last confirmed/);
 assert.ok(!/continuing for/i.test(said.context));
});

// --- the ladder: one mark per kind of nothing, and a stated window ------------------------------------------------
ok(()=>{
 const o=b1atTime(b1seen,'10:45');
 // the ladder covers BOTH sides: the middle pane reads a call rung and a put rung on one row, and the
 // right-hand explanation filters to the side its own story is about.
 const all=b1arr(o.ladder);
 assert.equal(all.length,10,'one rung per strike the grid holds, both sides');
 assert.equal(all.filter(r=>r.row==='calls').length,5);
 assert.equal(all.filter(r=>r.row==='puts').length,5);
 const rungs=all.filter(r=>r.row==='calls');
 // and every rung carries the LEVEL behind its change, so the panel can offer a value instead of a delta
 assert.ok(rungs.every(r=>r.oi_level!==undefined&&r.price_level!==undefined));
 const by=Object.fromEntries(rungs.map(r=>[r.strike,r]));
 assert.equal(by[23350].status,'added');
 assert.equal(by[23450].status,'added');
 assert.equal(by[23450].joined,true,'23,450 joined at this reading');
 assert.equal(by[23500].status,'no_change','measured and did not move');
 // a strike the capture never reached is a different mark entirely
 const holed=JSON.parse(JSON.stringify(b1GRID));
 holed.rows[3].points[6].delta_oi=null;
 const late=b1SUM.observe(holed)[6];
 const gapped=b1arr(late.ladder).find(r=>r.strike===23500&&r.row==='calls');
 assert.equal(gapped.status,'not_captured','an absent measurement is never "no change"');
});

// --- the interval baseline exists and is narrower than the episode -------------------------------------------------
ok(()=>{
 const o=b1atTime(b1seen,'10:45');
 assert.ok(o.calls_interval,'every reading after the first carries an interval reading');
 assert.equal(o.previous_at,b1MARKS[5],'measured against the reading immediately before');
 assert.notEqual(o.previous_at,o.from,'the interval is not the hour-wide episode window');
 const first=b1seen[0];
 assert.equal(first.calls_interval,null,'the first reading has nothing before it');
});

// --- cross-market rows: scoped, unscored, and never crediting the wrong leg -----------------------------------------
ok(()=>{
 // PCR falls because CALL open interest rose, not because puts thinned. The row must say which.
 const pcr=[{at:'2026-09-18 10:30:00',pcr_oi:1.16,total_ce_oi:1000,total_pe_oi:1160},
            {at:'2026-09-18 10:45:00',pcr_oi:1.05,total_ce_oi:1105,total_pe_oi:1160}];
 const rows=b1arr(b1SUM.crossMarket({puts:b1atTime(b1seen,'10:45').puts,pcr,upto:'2026-09-18 10:45:00'}));
 const ratio=rows.find(r=>r.label==='PCR');
 assert.match(ratio.text,/1\.05, fell from 1\.16/);
 assert.match(ratio.text,/driven by call open interest rising, not the put side/);
 // no row is a verdict
 for(const r of rows){
  assert.ok(!/✓|✗|agree|disagree|confirm/i.test(r.text),`a context row may not be a verdict: ${r.text}`);
 }
 // the put row appears once and only once
 assert.equal(rows.filter(r=>r.label==='Puts').length,1);
});

// --- futures report what they did, and never declare a conflict they cannot name -------------------------------------
ok(()=>{
 const futures=[{at:'2026-09-18 10:30:00',oi:100,basis:4,spot:23302},
                {at:'2026-09-18 10:45:00',oi:120,basis:9,spot:23284}];
 const rows=b1arr(b1SUM.crossMarket({futures,upto:'2026-09-18 10:45:00'}));
 const fut=rows.find(r=>r.label==='Futures');
 assert.match(fut.text,/open interest rose and basis widened/);
 assert.ok(!/does not agree|conflict|contradict/i.test(fut.text),
  'a futures row may not declare a disagreement it cannot name');
 // and the same series carries the competing explanation
 const move=b1SUM.spotMove(futures,'2026-09-18 10:45:00');
 assert.equal(move.change,-18);
 assert.equal(move.spot,23284);
 assert.equal(b1SUM.spotMove([],null),null);
});

// --- the phrases the panel reveals are sentences, never words ----------------------------------------------------
ok(()=>{
 const spot={at:null,from:null,spot:23284,change:-18};
 const said=b1SUM.narrate(b1atTime(b1seen,'10:45'),{underlying:'NIFTY',ladder:b1CE_LADDER,spot});
 const parts=b1arr(b1SUM.phrases(said));
 assert.ok(parts.length>=3&&parts.length<=8,`expected a handful of phrases, got ${parts.length}`);
 for(const p of parts)assert.ok(p.split(' ').length>2,`"${p}" is not a readable phrase`);
 const whole=[said.observed,said.qualified,said.otherSide,said.session].join(' ');
 for(const p of parts)assert.ok(whole.includes(p),`"${p}" is not in the summary text`);
});


// --- THE HEADLINE VERB FOLLOWS THE OPEN INTEREST --------------------------------------------------------------
//
// A headline reading "positions begin building" over a body reading "open interest fell" is the screen
// contradicting itself. Seen live on TATASTEEL before this check existed.
ok(()=>{
 const shrink=JSON.parse(JSON.stringify(b1GRID));
 // 23,350 CE: open interest falls and premium falls together — a position REDUCTION, not a build
 shrink.rows[0].points=shrink.rows[0].points.map((pt,i)=>({...pt,
  delta_oi:[0,0,0,0,-20000,-40000,-60000,-80000][i],
  price:[80,80,80,80,70,60,50,40][i]}));
 const walk=b1SUM.observe(shrink);
 const last=walk[walk.length-1];
 assert.equal(last.calls.behaviour,'buyers_exiting','open interest and premium both falling is an exit');
 const said=b1SUM.narrate(last,{underlying:'NIFTY',ladder:b1CE_LADDER});
 assert.ok(!/building|build at/i.test(said.headline),
  `a reduction may not be headlined as a build: "${said.headline}"`);
 assert.match(said.headline,/reduc/i);
 // and the body must agree with it
 assert.match(said.observed,/Open interest fell/);
 // the two never disagree: if the body says fell, the headline may not say build
 if(/Open interest fell/.test(said.observed))
  assert.ok(!/\bbuild/i.test(said.headline),'headline and body disagree on the direction');
});

const SIG=load('src/derivative/signal.ts',n=>{if(/\/logic$/.test(n))return L;if(/\/summary$/.test(n))return b1SUM;throw Error(n);});
const real=require('/c/Users/SPS/AppData/Local/Temp/claude/C--Users-SPS-Documents-Kanida-Falcon/b7a955a0-1e5b-4cca-9bb4-84d1b4722070/scratchpad/real.json');
for(const t of Object.keys(real)){
 const top=SIG.states({grid:real[t]})[0];
 if(!top){console.log(t,'-> no state');continue;}
 console.log(`${t} (${(real[t].marks||[]).length} readings) -> ${String(top.state).padEnd(11)} persistence_known=${top.persistence_known}`);
 console.log(`   "${top.plain_language_read}"   [${SIG.persistenceText(top)||'-'}]`);
 console.log(`   ${top.plain_language_detail.slice(0,190)}`);
}
