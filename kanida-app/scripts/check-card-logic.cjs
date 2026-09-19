// Pure checks for src/discover/cardLogic.ts (the trader evidence card's formatting + the research-row adapter).
// Run: node scripts/check-card-logic.cjs (no server needed).
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const load=(rel,req)=>{const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..',rel),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;const ctx={exports:{},require:req,process:{env:{}}};vm.runInNewContext(code,ctx);return ctx.exports;};
const types=load('src/strategies/types.ts',n=>{throw Error(n)});
const L=load('src/discover/cardLogic.ts',n=>{if(n==='../strategies/types')return types;throw Error(n)});
let checks=0;const ok=fn=>{fn();checks++};
const DASH='—',MINUS='−';

// --- the contract's label table (EVIDENCE_SERVING_CONTRACT.md §6), verbatim and complete -------------------
ok(()=>assert.deepEqual({...types.EVIDENCE_LABELS},{
 insufficient_history:'Not enough historical data',
 no_occurrences:'No occurrences in this historical sample',
 no_walkforward_trades:'No selected walk-forward trades',
 limited_sample:'Limited historical sample',
 walkforward_result:'Historical walk-forward result',
 incompatible:'Incompatible historical evidence',
 requires_review:'Historical data requires review',
 loading:'Evidence loading'}));
ok(()=>{for(const [state,label] of Object.entries(types.EVIDENCE_LABELS))assert.equal(L.evidenceLabel(state),label)});
ok(()=>assert.equal(L.evidenceLabel('nonsense'),'Incompatible historical evidence'));
ok(()=>{assert.equal(L.evidenceTone('walkforward_result'),'good');assert.equal(L.evidenceTone('limited_sample'),'weak');
 assert.equal(L.evidenceTone('loading'),'pending');assert.equal(L.evidenceTone('no_occurrences'),'neutral')});
// Section order is the owner's, fixed.
ok(()=>assert.deepEqual(Array.from(L.CARD_SECTIONS),['summary','barriers','forward_curve','conditions','last_occurrences','honesty']));

// --- number formatting: a missing number is a dash, never zero ---------------------------------------------
ok(()=>{assert.equal(L.signed(1.42),'+1.4%');assert.equal(L.signed(-0.9),MINUS+'0.9%');assert.equal(L.signed(0),'0.0%')});
ok(()=>{for(const v of [null,undefined,NaN,Infinity,'1.4'])assert.equal(L.signed(v),DASH,String(v))});
ok(()=>{for(const v of [null,undefined,NaN])assert.equal(L.plain(v),DASH)});
ok(()=>{assert.equal(L.plain(5.26),'5.3%');assert.equal(L.rate(61.4),'61%');assert.equal(L.rate(null),DASH)});
ok(()=>{assert.equal(L.count(2019),'2,019');assert.equal(L.count(0),'0');assert.equal(L.count(null),DASH)});
ok(()=>{assert.equal(L.bars(8,'4H candles'),'8 4H candles');assert.equal(L.bars(3.5,'daily candles'),'3.5 daily candles');assert.equal(L.bars(null,'x'),DASH)});

// --- summary: the win rate always carries its baseline ----------------------------------------------------
const summary={occurrences:2019,sample_label:'adequate',horizon:8,horizon_bars_word:'4H candles',
 win_rate_pct:61,baseline_win_rate_pct:58,win_rate_text:'61% vs 58% for the stock alone',win_rate_scope:'out_of_sample',
 median_net_return_pct:0.9,mean_net_return_pct:2.02,baseline_mean_net_return_pct:0.48,
 median_mfe_pct:5.26,median_mae_pct:3.62,walkforward_n:46,flatten_low:5,flatten_high:8,
 window_text:'most of the move happens within 5–8 4H candles'};
ok(()=>assert.equal(L.winRateText(summary),'61% vs 58% for the stock alone'));
ok(()=>assert.equal(L.winRateScopeNote(summary),'out-of-sample'));
ok(()=>assert.equal(L.winRateScopeNote({...summary,win_rate_scope:'all_history_descriptive'}),'whole history, descriptive'));
ok(()=>{assert.equal(L.winRateText(null),DASH);assert.equal(L.winRateText({...summary,win_rate_text:null}),DASH)});
ok(()=>assert.equal(L.occurrenceText(summary),'2,019 occurrences'));
ok(()=>{assert.equal(L.occurrenceText({...summary,occurrences:1}),'1 occurrence');
 assert.equal(L.occurrenceText({...summary,occurrences:null}),'Occurrences not recorded');
 assert.equal(L.occurrenceText(null),'Occurrences not recorded')});
ok(()=>{assert.equal(L.windowText(summary),'most of the move happens within 5–8 4H candles');
 assert.equal(L.windowText({...summary,window_text:null}),null)});

// --- barriers: headline as served, tie rule always stated -------------------------------------------------
const barriers={barrier_id:'pct:2.0:1.0',target_pct:2,stop_pct:1,n:2019,p_target_first:64,p_stop_first:29,p_neither:7,
 headline:`hit +2.0% before ${MINUS}1.0%: 64% · hit ${MINUS}1.0% first: 29% · neither: 7%`,
 tie_rule:'stop_first_when_both_touched_in_the_same_bar',
 tie_rule_text:'If one candle touches both, the stop is counted first: OHLC cannot reveal which came first inside a bar.',
 median_bars_to_target:4,median_bars_to_stop:3,both_touched_same_bar_n:51,undetermined_n:6,sample_label:'adequate'};
ok(()=>assert.ok(L.barrierText(barriers).includes('hit +2.0% before')));
ok(()=>{assert.equal(L.barrierText(null),null);assert.equal(L.barrierText({...barriers,headline:null}),null)});
ok(()=>assert.equal(L.barrierTimingText(barriers,'4H candles'),'Typical time to target in 4 4H candles · failure in 3 4H candles'));
ok(()=>assert.equal(L.barrierTimingText({...barriers,median_bars_to_target:null},'4H candles'),'Typical time to failure in 3 4H candles'));
ok(()=>assert.equal(L.barrierTimingText({...barriers,median_bars_to_target:null,median_bars_to_stop:null},'x'),null));
ok(()=>assert.equal(L.barrierTimingText(null,'x'),null));

// --- forward-return curve: the declared grid, the peak marked, gaps kept in place -------------------------
const point=(h,median,base)=>({h,n:120,median_net_return_pct:median,mean_net_return_pct:median,win_rate_pct:50,
 baseline_mean_net_return_pct:base,median_mfe_pct:1,median_mae_pct:1});
const curve={grid:[1,2,3,5,10],declared:[1,2,3,5,10],dropped_beyond_horizon:[],max_horizon:30,bars_word:'daily candles',
 basis:'median net return after costs',selected_h:8,peak_h:10,
 points:[point(1,-0.4,-0.5),point(2,-0.2,-0.3),point(3,null,-0.2),point(5,0.4,0.1),point(10,0.9,0.3)]};
ok(()=>{const b=L.curveBars(curve);assert.equal(b.length,5);assert.deepEqual(b.map(x=>x.h),[1,2,3,5,10])});
ok(()=>{const b=L.curveBars(curve);assert.equal(b.filter(x=>x.peak).length,1);assert.equal(b.find(x=>x.peak).h,10)});
ok(()=>{const b=L.curveBars(curve)[2];assert.equal(b.value,null);assert.equal(b.height,0);assert.ok(b.a11y.includes(DASH))},'a missing horizon draws nothing');
ok(()=>{const b=L.curveBars(curve);assert.ok(b.every(x=>x.height>=0&&x.height<=1));assert.equal(b.find(x=>x.h===10).height,1)});
ok(()=>assert.deepEqual(Array.from(L.curveBars(null)),[]));
ok(()=>assert.deepEqual(Array.from(L.curveBars({points:[]})),[]));
ok(()=>assert.ok(L.curveBars(curve)[0].a11y.includes('stock alone')),'every bar names its baseline');
ok(()=>assert.equal(L.curveCaption(curve),'Median net return after costs · peaks at 10 daily candles'));
ok(()=>assert.ok(L.curveCaption({...curve,dropped_beyond_horizon:[24]}).includes('24 beyond the measured horizon')));
ok(()=>assert.ok(L.curveCaption({...curve,peak_h:null}).includes('no peak measured')));
// The display grid matches the contract, per timeframe.
ok(()=>assert.deepEqual(Object.fromEntries(Object.entries(types.DISPLAY_GRID).map(([k,v])=>[k,Array.from(v)])),{'1H':[1,2,4,8,12,24],'4H':[1,2,3,5,8,10],'1D':[1,2,3,5,10],'1W':[1,2,4,8,12]}));

// --- conditions: a thin bucket shows no rate at all -------------------------------------------------------
const bucket=(n,enough)=>({bucket:'high',name:'High',n,enough,note:enough?null:'not enough cases',
 win_rate_pct:enough?58:null,mean_net_return_pct:enough?1.9:null,median_net_return_pct:enough?1.2:null,
 baseline_mean_net_return_pct:enough?0.4:null,baseline_n:5000,baseline_scope:'unconditional_entries_in_the_same_bucket',
 diff_mean_net_return_pct:enough?1.5:null,q_value:enough?0.06:null});
ok(()=>assert.equal(L.bucketText(bucket(64,true)),'58% win · avg +1.9% vs +0.4% for the stock alone'));
ok(()=>assert.equal(L.bucketText(bucket(9,false)),'not enough cases'));
ok(()=>assert.ok(!/\d+%\s*win/.test(L.bucketText(bucket(9,false)))),'a thin bucket never prints a win rate');
ok(()=>assert.ok(L.bucketA11y(bucket(9,false),'Volume').includes('not enough cases to report a rate')));
ok(()=>assert.ok(L.bucketA11y(bucket(64,true),'Volume').includes('64 cases')));
ok(()=>assert.equal(L.bucketText({...bucket(64,true),baseline_mean_net_return_pct:null}),'58% win · avg +1.9%'));

// --- last 5 occurrences -----------------------------------------------------------------------------------
ok(()=>assert.equal(L.lastRead({read:'4 of the last 5 were positive after costs.'}),'4 of the last 5 were positive after costs.'));
ok(()=>{assert.equal(L.lastRead(null),null);assert.equal(L.lastRead({read:null}),null)});
ok(()=>{assert.equal(L.occurrenceOutcome({outcome:'target_hit'}),'Target first');
 assert.equal(L.occurrenceOutcome({outcome:'stop_hit'}),'Stop first');
 assert.equal(L.occurrenceOutcome({outcome:'weird'}),'weird');
 assert.equal(L.occurrenceOutcome({outcome:null}),DASH)});

// --- honesty: the significance sentence is always last ----------------------------------------------------
const card={honesty:['Costs 0.40% included, charged once on the entry notional.','Entry is the next open after the signal bar.'],
 significance:{q_value:0.42,p_value:0.03,fdr_trials:4820,discovery_q10:0,
  line:'Not statistically distinguishable from chance after correcting for the number of patterns tested (q = 0.42).'},
 summary};
ok(()=>{const l=L.honestyLines(card);assert.equal(l.length,3);assert.ok(l[2].startsWith('Not statistically distinguishable'))});
ok(()=>assert.deepEqual(Array.from(L.honestyLines(null)),[]));
ok(()=>assert.deepEqual(Array.from(L.honestyLines({honesty:['a'],significance:null})),['a']));
ok(()=>assert.ok(L.CARD_COST_LINE.includes('0.40% included')&&L.CARD_COST_LINE.includes('next-open entry')));
ok(()=>{assert.equal(L.hasNumbers(card),true);assert.equal(L.hasNumbers({summary:null}),false);assert.equal(L.hasNumbers(null),false)});

// --- research row adapter: columns are relabelled, never silently reused -----------------------------------
ok(()=>{assert.deepEqual({...L.STORED_COLUMNS},{low:'95% low',avg:'Avg',n:'n'});
 assert.deepEqual({...L.RESEARCH_COLUMNS},{low:'Edge low',avg:'Avg',n:'Trades'});
 assert.notDeepEqual({...L.STORED_COLUMNS},{...L.RESEARCH_COLUMNS})});
const st={key:'ch05-legacy_1.0.1-long-1d',pattern_id:'CH05',variant:'legacy_1.0.1',side:'long',timeframe:'1D',state:'confirmed',
 live_detection:false,detections_today:3,detections_week:35,researched_stocks:442,evidence_ready:181,evidence_total:495,
 best_edge_low_pct:0.31,best_edge_symbol:'AAAA',beats_baseline:1,cells_tested:4,evidence_pending:false};
const research=r=>({symbol:'AAAA',company:'AAAA Ltd.',sector:'Power',market_cap_tier:'large',has_evidence:true,occurrences:120,walkforward_n:46,
 walkforward_mean_pct:1.4,win_rate:61,edge_mean_pct:0.98,edge_low_pct:0.31,edge_high_pct:1.65,beats_baseline:true,
 baseline_mean_pct:0.42,p_target_first:64,p_stop_first:29,selected_horizon:8,q_value:0.004,p_value:0.03,
 last_seen:'2026-09-15',found:true,evidence_state:'walkforward_result',label:'Historical walk-forward result',
 research_status:'tested_positive',selection_status:'tested',...r});
ok(()=>{const r=L.researchRowToStrategyRow(research(),st);
 assert.equal(r.low_pct,0.31);assert.equal(r.high_pct,1.65);assert.equal(r.avg_pct,1.4);assert.equal(r.n,46);
 assert.equal(r.sample_label,'Historical walk-forward result');assert.equal(r.status,'tested');
 assert.equal(r.evidence_basis,'tested_rule');assert.equal(r.timeframe,'1D');assert.equal(r.side,'long');
 assert.equal(r.candle_end,'2026-09-15')});
ok(()=>{const r=L.researchRowToStrategyRow(research({evidence_state:'loading',label:'Evidence loading',walkforward_n:null,
  edge_low_pct:null,walkforward_mean_pct:null,selection_status:null}),st);
 assert.equal(r.low_pct,null);assert.equal(r.avg_pct,null);assert.equal(r.n,0);
 assert.equal(r.status,'no_validated_rule');assert.equal(r.evidence_basis,'none')},'loading never becomes a tested row');
ok(()=>{const r=L.researchRowToStrategyRow(research({evidence_state:'limited_sample',label:'Limited historical sample',walkforward_n:6}),st);
 assert.equal(r.status,'no_validated_rule');assert.equal(r.evidence_basis,'hold_period_history');assert.equal(r.n,6)});
ok(()=>assert.ok(L.researchRowA11y(research()).includes('tested on later data')));
ok(()=>assert.ok(L.researchRowA11y(research()).includes('46 tested trades')));
ok(()=>assert.ok(!/walk-?forward|out-of-sample|researched/i.test(L.researchRowA11y(research()))),'the spoken row label is plain words');
// Every evidence state has a plain list wording; an unknown code never reaches the screen raw.
ok(()=>{for(const state of Object.keys(types.EVIDENCE_LABELS)){const t=L.plainEvidence(state);assert.ok(t&&!/_/.test(t)&&!/walk-?forward|requires review/i.test(t),state)}
 assert.equal(L.plainEvidence('very_small'),'no matching past data');assert.equal(L.plainEvidence(null),'no matching past data')});

// --- picker tag + coverage: "pending" is never rendered as zero -------------------------------------------
ok(()=>assert.equal(L.researchPickerTag(st),'442 stocks · best edge low +0.3%'));
ok(()=>assert.ok(!/found|researched/i.test(L.researchPickerTag(st))),'the picker tag never says "found" or "researched"');
ok(()=>assert.equal(L.researchPickerTag({...st,live_detection:true}),'count unknown · best edge low +0.3%'),'no now count served: said so, never a "today" count');
ok(()=>assert.equal(L.researchPickerTag({...st,live_detection:true,detections_live:17}),'17 now · best edge low +0.3%'));
ok(()=>assert.equal(L.researchPickerTag({...st,live_detection:true,detections_live:0}),'0 now · best edge low +0.3%'),'a measured zero is shown, not hidden');
ok(()=>assert.equal(L.researchPickerTag({...st,evidence_pending:true}),'past results loading'));
ok(()=>assert.ok(!/\b0 found\b/.test(L.researchPickerTag({...st,evidence_pending:true}))));
ok(()=>assert.equal(L.researchPickerTag({...st,best_edge_low_pct:null}),'442 stocks · no ranked edge yet'));
ok(()=>assert.equal(L.researchPickerTag({...st,researched_stocks:null,best_edge_low_pct:null}),'history unknown · no ranked edge yet'));
ok(()=>assert.equal(L.coverageText({symbols_with_evidence:96,symbols_research:495,current_outcome_status:'running'}),
 'Evidence for 96 of 495 stocks — the rest are still being computed.'));
ok(()=>assert.equal(L.coverageText({symbols_with_evidence:495,symbols_research:495,current_outcome_status:'complete'}),
 'Evidence for all 495 stocks.'));
ok(()=>assert.equal(L.coverageText(null),null));

// --- status line: a research card NEVER says "found" -------------------------------------------------------
const rs=o=>({phase:'done',date:'16 Sep 2026',researched:442,shown:442,revealing:false,live_detection:false,
 detections_today:0,detections_week:0,...o});
ok(()=>assert.equal(L.researchStatusText(rs()),'Not scanning live yet — showing past cases.'));
ok(()=>assert.equal(L.researchStatusText(rs(),true),'Not scanning live yet'));
// The exact regression from the live page: 442 must never be presented as a detection count.
ok(()=>{for(const short of [false,true])for(const phase of ['idle','loading','done','error','stopped'])
 assert.ok(!/found/i.test(L.researchStatusText(rs({phase}),short)),`${phase}/${short}`)});
ok(()=>{for(const short of [false,true])assert.ok(!/\b442\s+found\b/.test(L.researchStatusText(rs(),short)))});
ok(()=>assert.equal(L.researchStatusText(rs({phase:'loading'})),'Loading history…'));
ok(()=>assert.equal(L.researchStatusText(rs({revealing:true,shown:40})),'Loading history… 40 of 442 stocks'));
ok(()=>assert.equal(L.researchStatusText(rs({pending:true})),L.EVIDENCE_INDEXING));
ok(()=>assert.ok(!/found/i.test(L.EVIDENCE_INDEXING)&&/still loading/.test(L.EVIDENCE_INDEXING)));
ok(()=>assert.equal(L.researchStatusText(rs({researched:0})),'This pattern has no history on this timeframe.'));
ok(()=>assert.equal(L.researchStatusText(rs({phase:'error'})),'History is unavailable right now.'));
// With live scanning on, the History tab carries the ONE count; a finished status line repeats no number at all.
ok(()=>assert.equal(L.researchStatusText(rs({live_detection:true,detections_today:3,detections_week:35,detections_live:17})),
 'Stocks where this pattern happened before, and what followed.'));
ok(()=>assert.equal(L.researchStatusText(rs({live_detection:true,detections_today:3,detections_week:35,detections_live:17}),true),
 'Past cases by stock'));
ok(()=>{for(const short of [false,true])for(const live_detection of [false,true])
 assert.ok(!/\d/.test(L.researchStatusText(rs({live_detection,detections_live:17,detections_today:3}),short)),`${live_detection}/${short}`)},
 'one count per card: a finished history line never repeats a number');
ok(()=>assert.equal(L.researchStatusText(rs({researched:1})),'Not scanning live yet — showing past cases.'));

// --- coverage + per-row pending chip ----------------------------------------------------------------------
ok(()=>assert.equal(L.evidenceCoverageText({ready:181,total:495}),'Evidence ready for 181 of 495 stocks.'));
ok(()=>assert.equal(L.evidenceCoverageText({ready:495,total:495}),null),'a finished run adds no second count to the card');
ok(()=>{assert.equal(L.evidenceCoverageText(null),null);assert.equal(L.evidenceCoverageText({ready:0,total:0}),null)});
ok(()=>assert.equal(L.ROW_PENDING,'evidence loading'));
ok(()=>{assert.equal(L.rowPending({evidence_state:'loading',has_evidence:false}),true);
 assert.equal(L.rowPending({evidence_state:'walkforward_result',has_evidence:true}),false);
 assert.equal(L.rowPending({evidence_state:'no_walkforward_trades',has_evidence:true}),false);
 assert.equal(L.rowPending(null),false)},'a computed cell with no walk-forward trades is NOT pending');
ok(()=>{const r=L.researchRowToStrategyRow(research({has_evidence:false,evidence_state:'loading'}),st);
 assert.equal(L.rowPending(r),true)},'the pending flag survives the row adapter');
ok(()=>{const r=L.researchRowToStrategyRow(research(),st);assert.equal(L.rowPending(r),false)});


// --- live detections: the card's new states (docs/LIVE_DETECTION.md) ---------------------------------------
// "Detected today", "researched history" and "evidence" are three different things and must never be merged.
const liveOn={available:true,day:'2026-09-16',as_of:'2026-09-16 15:15',detections:120,live:44};
const liveOff={available:false};
ok(()=>{assert.equal(L.liveAvailable(liveOn),true);assert.equal(L.liveAvailable(liveOff),false);
 assert.equal(L.liveAvailable(null),false);assert.equal(L.liveAvailable(undefined),false)});
// The live list is only offered when the ledger is readable; otherwise the card behaves exactly as before.
ok(()=>{assert.equal(L.defaultListMode(liveOn),'live');assert.equal(L.defaultListMode(liveOff),'history')});
ok(()=>{assert.equal(L.resolveListMode('live',liveOn),'live');assert.equal(L.resolveListMode('history',liveOn),'history');
 assert.equal(L.resolveListMode('live',liveOff),'history');
 assert.equal(L.resolveListMode(undefined,liveOn),'live');assert.equal(L.resolveListMode('nonsense',liveOff),'history')},
 'a stored preference can never turn a missing ledger on');
// Live detection being off is an OPERATOR fact, never "nothing was detected".
ok(()=>{assert.equal(L.liveOffText(liveOn),null);assert.equal(L.liveOffText(liveOff),L.LIVE_OFF);
 assert.ok(!/\b0\b|\bnone\b|no detections\b/i.test(L.LIVE_OFF));
 assert.equal(L.liveOffText({available:false,error:'database is locked'}),'Current setups are unavailable: database is locked')});
// Lifecycle: the tone is about the STATE, never about whether the detection looks good.
ok(()=>{assert.equal(L.detectionTone('confirmed'),'confirmed');assert.equal(L.detectionTone('forming'),'forming');
 assert.equal(L.detectionTone('invalidated'),'done');assert.equal(L.detectionTone('expired'),'done');
 assert.equal(L.detectionTone(null),'done')});
ok(()=>assert.deepEqual({...L.DETECTION_STATE_LABELS},
 {forming:'Forming',confirmed:'Confirmed',invalidated:'Invalidated',expired:'Expired'}));
ok(()=>{assert.equal(L.detectionStateLabel({state:'forming'}),'Forming');
 assert.equal(L.detectionStateLabel({state:'forming',state_label:'Forming'}),'Forming');
 assert.equal(L.detectionStateLabel(null),DASH)});
// Today shows the clock; an older bar keeps its day, so "detected today" can never be read off a stale row.
ok(()=>{assert.equal(L.detectionTimeText('2026-09-16 15:15','2026-09-16'),'15:15');
 assert.equal(L.detectionTimeText('2026-09-12 11:15','2026-09-16'),'12 Sep 11:15');
 assert.equal(L.detectionTimeText('2026-09-12','2026-09-16'),'12 Sep');
 assert.equal(L.detectionTimeText(null),DASH);assert.equal(L.detectionTimeText(''),DASH)});
const detection=(o)=>Object.assign({detection_id:'abc123',symbol:'TITAN',company:'Titan Company',timeframe:'1D',
 pattern_id:'CH05',variant:'legacy_1.0.1',side:'long',state:'forming',state_label:'Forming',
 detected_at:'2026-09-16 15:15',bars_since_state:2,bars_since_signal:2,current:true,drawable:true,geometry_note:'',
 evidence_compatible:true,evidence:{status:'identity_match',note:'ok'}},o||{});
// A row reads "since <day>"; the "new" marker is the latest SESSION for the timeframe, never the calendar day.
ok(()=>assert.equal(L.detectionDetailText(detection(),null),'Forming · since 16 Sep · 2 candles ago'));
ok(()=>assert.equal(L.detectionDetailText(detection(),'2026-09-16'),'Forming · since 16 Sep · new · 2 candles ago'));
ok(()=>assert.equal(L.detectionDetailText(detection({bars_since_signal:1}),'2026-09-15'),'Forming · since 16 Sep · 1 candle ago'));
ok(()=>assert.equal(L.detectionDetailText(detection({detected_at:'2026-09-12 11:15',state:'confirmed',state_label:'Confirmed',bars_since_signal:18}),'2026-09-16'),
 'Confirmed · since 12 Sep · 18 candles ago'),'an older-but-active setup reads as older and is not new');
ok(()=>assert.equal(L.detectionDetailText(detection({bars_since_signal:0}),null),'Forming · since 16 Sep · latest candle'));
ok(()=>assert.equal(L.detectionDetailText(detection({bars_since_signal:null}),null),'Forming · since 16 Sep'),'no age is invented');
ok(()=>assert.ok(L.detectionDetailText(detection({current:false}),null).endsWith('not in the latest scan')));
ok(()=>assert.ok(L.detectionRowA11y(detection(),'2026-09-16').startsWith('TITAN, Titan Company. Forming · since 16 Sep · new')));
ok(()=>{assert.ok(L.detectionRowA11y(detection({drawable:false,geometry_note:'x'}),null).includes('no chart outline'));
 assert.ok(L.detectionRowA11y(detection({evidence_compatible:false}),null).includes('no matching past data'))},
 'what the row no longer shows is still spoken');
ok(()=>{assert.equal(L.sinceText({detected_day:'2026-09-16'}),'since 16 Sep');assert.equal(L.sinceText({detected_at:'2026-09-01 09:15'}),'since 1 Sep');
 assert.equal(L.sinceText({detected_at:''}),'');assert.equal(L.sinceText(null),'')});
ok(()=>{assert.equal(L.isNewDetection({detected_day:'2026-09-16'},'2026-09-16'),true);
 assert.equal(L.isNewDetection({detected_at:'2026-09-15 15:30'},'2026-09-16'),false);
 assert.equal(L.isNewDetection({detected_day:'2026-09-16'},null),false,'no session known: nothing is marked new');
 assert.equal(L.isNewDetection({detected_day:'2026-09-15',new:true},'2026-09-16'),true,'a served flag is used as served');
 assert.equal(L.isNewDetection({detected_day:'2026-09-16',new:false},'2026-09-16'),false);
 assert.equal(L.isNewDetection(null,'2026-09-16'),false)});
ok(()=>{const state={available:true,timeframes:{'1D':{session:'2026-09-16'},'1W':{session:'2026-09-11'},'1H':{}}};
 assert.equal(L.latestSession(state,'1D'),'2026-09-16');assert.equal(L.latestSession(state,'1W'),'2026-09-11');
 assert.equal(L.latestSession(state,'1H'),null);assert.equal(L.latestSession(null,'1D'),null);assert.equal(L.latestSession(state,null),null)});
// A family with no drawable geometry says so; it never renders an empty chart in silence.
ok(()=>{assert.equal(L.geometryNoteText(detection()),null);
 assert.equal(L.geometryNoteText(detection({drawable:false,geometry_note:'CDLDOJI publishes no geometry'})),'CDLDOJI publishes no geometry');
 assert.equal(L.geometryNoteText(detection({drawable:false,geometry_note:''})),L.MARKER_ONLY);
 assert.equal(L.geometryNoteText(null),null)});
ok(()=>assert.ok(/marker only/i.test(L.MARKER_ONLY)));
// Evidence attaches ONLY on identity match; anything else carries the contract's own label and no numbers.
ok(()=>assert.equal(L.NO_COMPATIBLE_EVIDENCE,types.EVIDENCE_LABELS.incompatible));
ok(()=>{assert.equal(L.evidenceCompatible({status:'identity_match'}),true);
 assert.equal(L.evidenceCompatible({status:'detector_mismatch'}),false);
 assert.equal(L.evidenceCompatible({status:'unavailable'}),false);
 assert.equal(L.evidenceCompatible(null),false)});
ok(()=>{assert.equal(L.detectionEvidenceLabel({status:'identity_match'}),null);
 assert.equal(L.detectionEvidenceLabel({status:'detector_mismatch'}),'Incompatible historical evidence');
 assert.equal(L.detectionEvidenceLabel(null),'Incompatible historical evidence')});
ok(()=>{assert.equal(L.detectionEvidenceNote({status:'identity_match',note:'byte-identical'}),null);
 assert.equal(L.detectionEvidenceNote({status:'unavailable',note:'the run is not readable here'}),'the run is not readable here');
 assert.equal(L.detectionEvidenceNote({status:'detector_mismatch',note:''}),'Incompatible historical evidence')});
// The Now tab carries the card's ONE count: a complete list has no status text, a cut list says how much is shown.
const ls=o=>Object.assign({phase:'done',total:12,shown:12,available:true},o||{});
ok(()=>{assert.equal(L.liveStatusText(ls()),'');assert.equal(L.liveStatusText(ls(),true),'')},'a complete list repeats no count');
ok(()=>assert.equal(L.liveStatusText(ls({shown:100,total:412})),'Showing 100 of 412'));
// An empty Now list is never a bare blank: it says nothing is standing, and the day one last stood.
ok(()=>assert.equal(L.liveStatusText(ls({total:0,shown:0,lastDetected:'2026-09-12 11:15'})),'No setup right now · last seen 12 Sep.'));
ok(()=>assert.equal(L.liveStatusText(ls({total:0,shown:0,lastDetected:null})),'No setup right now'));
ok(()=>assert.equal(L.liveStatusText(ls({total:0,shown:0,lastDetected:'2026-09-12 11:15'}),true),'No setup right now'));
ok(()=>assert.equal(L.NO_ACTIVE,'No setup right now'));
// Live scanning being OFF is a different sentence from "nothing is standing".
ok(()=>assert.equal(L.liveStatusText(ls({available:false})),L.LIVE_OFF));
ok(()=>assert.equal(L.liveStatusText(ls({available:false,reason:'the scanner is elsewhere'})),'the scanner is elsewhere'));
ok(()=>{for(const phase of ['loading','stopped','error'])
 assert.ok(!/found/i.test(L.liveStatusText(ls({phase}))),phase)});
ok(()=>assert.equal(L.liveStatusText(ls({phase:'loading'})),'Loading setups…'));
ok(()=>assert.equal(L.liveStatusText(ls({phase:'error'})),'Current setups are unavailable right now.'));
// The "Spotted today" chip and its separate "N today" line are gone for good.
ok(()=>{assert.equal(L.scopeChipLabel,undefined);assert.equal(L.SCOPE_WORDS,undefined);
 for(const short of [false,true])for(const o of [{},{total:0,shown:0},{shown:5,total:9},{phase:'loading'}])
  assert.ok(!/today/i.test(L.liveStatusText(ls(o),short)))});
// Each tab carries its own count, so the two lists can never be read as one another.
ok(()=>{assert.equal(L.listModeLabel('live',12,442),'Now (12)');
 assert.equal(L.listModeLabel('history',12,442),'History (442)');
 assert.equal(L.listModeLabel('live',null,442),'Now');
 assert.equal(L.listModeLabel('live',0,442),'Now (0)')},'no count is shown rather than a zero');
ok(()=>assert.deepEqual({...L.LIST_MODE_LABELS},{live:'Now',history:'History'}));
// The researched-history line stays honest once live detection is on and the timeframe has no history.
ok(()=>assert.equal(L.researchStatusText({phase:'done',date:'16 Sep 2026',researched:0,shown:0,revealing:false,
 live_detection:true,detections_today:3,detections_week:9,detections_live:4}),
 'This pattern has no history on this timeframe.'));


// A detection on a stock the research run never studied is "no compatible evidence" too, with its own reason.
ok(()=>{assert.equal(L.evidenceCompatible({status:'not_studied'}),false);
 assert.equal(L.detectionEvidenceLabel({status:'not_studied'}),'Incompatible historical evidence');
 assert.equal(L.detectionEvidenceNote({status:'not_studied',note:'The research run never studied ZZZZ on 1D.'}),
  'The research run never studied ZZZZ on 1D.')});
// The history tab shows the RESEARCHED count and the live tab the DETECTION count; they never share a number.
ok(()=>{assert.equal(L.listModeLabel('history',12,495),'History (495)');
 assert.equal(L.listModeLabel('live',12,495),'Now (12)');
 assert.equal(L.listModeLabel('history',12,null),'History','absent, never a zero')});

// --- no research words in the list / card chrome (backlog item 2, point 4) ---------------------------------
const JARGON=/walk-?forward|very_small|requires review|\btiers?\b|ledger|researched|detections?\b|\bOOS\b|\bWF\b|out-of-sample/i;
ok(()=>{
 const texts=[L.LIVE_OFF,L.NO_LIVE_DETECTION,L.NO_LIVE_DETECTION_SHORT,L.EVIDENCE_INDEXING,L.NO_ACTIVE,L.ROW_PENDING,
  ...Object.values(L.RESEARCH_COLUMNS),...Object.values(L.LIST_MODE_LABELS),...Object.values(L.PLAIN_EVIDENCE),
  L.researchPickerTag(st),L.researchPickerTag({...st,live_detection:true,detections_live:4}),L.researchPickerTag({...st,evidence_pending:true}),
  L.coverageText({symbols_with_evidence:96,symbols_research:495,current_outcome_status:'running'}),
  L.evidenceCoverageText({ready:1,total:2}),L.liveOffText({available:false,error:'x'}),
  L.detectionDetailText(detection(),'2026-09-16'),L.detectionRowA11y(detection({drawable:false,evidence_compatible:false}),null),
  L.researchRowA11y(research())];
 for(const short of [false,true])for(const phase of ['idle','loading','done','error','stopped']){
  for(const live_detection of [false,true])texts.push(L.researchStatusText(rs({phase,live_detection}),short),L.researchStatusText(rs({phase,pending:true}),short));
  texts.push(L.liveStatusText(ls({phase}),short),L.liveStatusText(ls({phase,total:0,shown:0,lastDetected:'2026-09-12 11:15'}),short),L.liveStatusText(ls({phase,available:false}),short));
 }
 for(const t of texts)assert.ok(!JARGON.test(String(t||'')),String(t));
},'every list/card string is plain words');

console.log(`${checks} evidence-card logic checks passed.`);
