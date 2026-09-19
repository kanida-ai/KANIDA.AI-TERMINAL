'use strict';
let filterCatalogPromise;
const catalogData=()=>filterCatalogPromise||(filterCatalogPromise=api('/api/filter-options').catch(e=>{filterCatalogPromise=null;throw e;}));
const historyMode=prefix=>prefix==='bt'?(document.querySelector('[data-bt-mode].selected')?.dataset.btMode||'reference'):$(prefix+'-basis')?.value||'reference';
function researchParams(prefix){return {sector:$(prefix+'-sector')?.value||'',universe:$(prefix+'-universe')?.value||'',mode:historyMode(prefix),return_band:$(prefix+'-return-band')?.value||'',min_trades:$(prefix+'-minimum')?.value??'5'};}
function setHistoryMode(prefix,mode){if(prefix==='bt')document.querySelectorAll('[data-bt-mode]').forEach(b=>b.classList.toggle('selected',b.dataset.btMode===mode));else if($(prefix+'-basis'))$(prefix+'-basis').value=mode;}
const minimumTrades=p=>p.min_trades===''||p.min_trades==null?5:Number(p.min_trades);
function filterNote(prefix){
 if(!$(prefix+'-screen-note'))return;
 const p=researchParams(prefix),minimum=minimumTrades(p);
 $(prefix+'-screen-note').textContent=`${minimum?`Showing results based on at least ${minimum} historical trades.`:'Showing any history size, including setups without past trades.'} ${p.mode==='test'?'Returns come from later tests of rules chosen on earlier data.':'Returns are past averages per trade after assumed costs.'} Each stock, pattern, timeframe and direction stays separate.`;
}
function mountResearchFilters(prefix,target,onChange,{performance=true,scanner=false}={}){
 if($(prefix+'-sector'))return;
 $(target).innerHTML=`<div class="research-filters"><div class="market-filter-row"><label>Sector<select id="${prefix}-sector" aria-label="${prefix} sector"><option value="">All sectors</option></select></label><label>Stock universe<select id="${prefix}-universe" aria-label="${prefix} stock universe"><option value="">All database equities</option></select></label><label>Market cap<select disabled aria-label="Market-cap classification unavailable"><option>Classification unavailable</option><option>Large cap · unavailable</option><option>Mid cap · unavailable</option><option>Small cap · unavailable</option></select></label><button id="${prefix}-reset" class="quiet">Reset filters</button></div>${performance?`<div class="performance-filter-row"><label>Past average return per trade<select id="${prefix}-return-band" aria-label="${prefix} past average return per trade"><option value="">All returns</option><option value="0_0.5">0–0.5%</option><option value="0.5_1">0.5–1%</option><option value="1_2">1–2%</option><option value="2_5">2–5%</option><option value="5_10">5–10%</option><option value="over_10">Above 10%</option></select></label><label>Minimum historical trades<input type="number" id="${prefix}-minimum" min="0" max="100000" step="1" value="5" aria-label="${prefix} minimum historical trades"><small>0 = any history size</small></label></div><p class="filter-note" id="${prefix}-screen-note"></p><details class="metadata-note"><summary>How returns and holding time work</summary><p>The percentage is the average of winning and losing trades after the saved cost assumptions. Each result shows its actual sample size. Five trades is a browsing choice; it does not establish a reliable forecast. Bands use the displayed average rounded to two decimals: 0.50% belongs to 0.5–1%, 1.00% to 1–2%, 2.00% to 2–5%, and 5.00% through 10.00% to 5–10%. Values above 10.00% belong to Above 10%.</p><p>Holding time counts market candles. A trade can stay open overnight when its holding period has not finished, and weekends or holidays can extend the calendar time. These are the exits tested historically, not recommended holding periods.</p>${scanner?`<label class="history-basis">Historical data shown<select id="${prefix}-basis" aria-label="Scanner historical data shown"><option value="reference">Past averages · preset exits</option><option value="test">Later tests · separately chosen rules</option></select></label>`:''}</details>`:''}<details class="metadata-note"><summary id="${prefix}-metadata-summary">Loading classification coverage…</summary><p id="${prefix}-metadata-note"></p></details></div>`;
 const changed=()=>{filterNote(prefix);onChange();};
 for(const name of ['sector','universe','return-band','basis','minimum'])$(prefix+'-'+name)?.addEventListener('change',()=>{
   const n=$(prefix+'-minimum');if(n){if(n.value==='')n.value=5;if(!n.checkValidity()){n.reportValidity();return;}}changed();
 });
 $(prefix+'-reset').onclick=()=>{for(const name of ['sector','universe','return-band'])if($(prefix+'-'+name))$(prefix+'-'+name).value='';if($(prefix+'-minimum'))$(prefix+'-minimum').value=5;setHistoryMode(prefix,'reference');changed();};
 catalogData().then(data=>{
   for(const s of data.sectors){const o=document.createElement('option');o.value=s.value;o.textContent=`${s.value} (${number(s.count)})`;$(prefix+'-sector').append(o);}
   for(const u of data.universes){const o=document.createElement('option');o.value=u.value;o.textContent=`${u.label} (${u.count} in DB)`;$(prefix+'-universe').append(o);}
   const unknown=document.createElement('option');unknown.value='unknown';unknown.textContent=`Membership unknown (${data.membership_unknown})`;$(prefix+'-universe').append(unknown);
   $(prefix+'-metadata-summary').textContent=`Database labels · ${data.labels_as_of?dateText(data.labels_as_of):'undated'} · ${number(data.unclassified)} stocks without a sector`;
   $(prefix+'-metadata-note').textContent=data.note;
 }).catch(e=>{$(prefix+'-metadata-summary').textContent='Classification labels unavailable';$(prefix+'-metadata-note').textContent=e.message;});
 filterNote(prefix);
}
function matchingHistory(match,p=researchParams('scan')){
 const minimum=minimumTrades(p);
 return (match.history||[]).filter(h=>(h[p.mode]?.n||0)>=minimum&&(!p.return_band||h[p.mode]?.return_band===p.return_band));
}
function passesResearchFilters(match){
 const p=researchParams('scan'),needsHistory=minimumTrades(p)>0||p.return_band;
 return (!p.sector||match.sector===p.sector)&&(!p.universe||(p.universe==='unknown'?match.membership_unknown:match.universes?.includes(p.universe)))&&(!needsHistory||matchingHistory(match,p).length>0);
}
function holdingCopy(stats){return stats?.holding?`<span class="holding-copy" title="${esc(stats.holding.detail)}">${esc(stats.holding.label)}<small>Tested ${stats.holding.maximum?'maximum: ': 'exit: '}${stats.holding.bars} chart candles</small></span>`:'<span class="holding-copy">Holding period unavailable</span>';}
function matchHistoryCaption(match){
 const p=researchParams('scan'),history=matchingHistory(match,p),label=p.mode==='test'?'Later-test average':'Past average';
 if(!history.length)return '<div class="match-history">Not enough history for a return estimate</div>';
 return `<div class="match-history">${history.map(h=>{const s=h[p.mode];return s?.n?`<span class="history-return">${label}: <strong>${s.display_return_pct>0?'+':''}${Number(s.display_return_pct).toFixed(2)}%</strong> per trade · ${h.side==='short'?'hypothetical short':'long'}</span><span>Based on <strong>${s.n} historical trades</strong></span>${holdingCopy(s)}`:`<span>No historical trades for this ${h.side} study</span>`;}).join('')}</div>`;
}
