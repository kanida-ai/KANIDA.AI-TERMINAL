// Compare the browser's local filtering to the same screens served by the API.
// No browser or third-party DOM runtime is required.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const root=path.resolve(__dirname,'..','static');
const nodes=new Map();
const node=id=>{if(!nodes.has(id))nodes.set(id,{value:'',innerHTML:''});return nodes.get(id);};
const context=vm.createContext({URLSearchParams,Number,Math,Set,
  $:node,number:n=>new Intl.NumberFormat('en-IN',{maximumFractionDigits:2}).format(n),
  esc:v=>String(v??''),pct:n=>n==null?'—':Number(n).toFixed(2)+'%',
  dateText:v=>v?.slice(0,10)||'—',timestamp:v=>v||'—'});
for(const file of ['filters.js','capital.js'])vm.runInContext(fs.readFileSync(path.join(root,file),'utf8'),context,{filename:file});
const get=async(route,params={})=>{const response=await fetch('http://127.0.0.1:8765'+route+'?'+new URLSearchParams(params));assert.equal(response.status,200);return response.json();};
(async()=>{
  const matches=await get('/api/matches',{min_trades:0});
  let screens=0;
  for(const params of [{},{sector:'Financial Services',universe:'nifty50'},{universe:'fno'},
      {universe:'unknown'},{min_trades:0},{min_trades:5},{min_trades:20},{min_trades:1},
      ...['0_0.5','0.5_1','1_2','2_5','5_10','over_10'].map(return_band=>({return_band})),
      {return_band:'5_10',min_trades:10},{return_band:'0.5_1',mode:'test'}]){
    for(const name of ['sector','universe'])node('scan-'+name).value=params[name]||'';
    node('scan-return-band').value=params.return_band||'';
    node('scan-basis').value=params.mode||'reference';node('scan-minimum').value=String(params.min_trades??5);
    const expected=await get('/api/matches',params);
    const actual=matches.filter(m=>context.passesResearchFilters(m));
    assert.deepEqual(actual.map(m=>m.id).sort(),expected.map(m=>m.id).sort());
    for(const m of actual)assert.equal(typeof context.matchHistoryCaption(m),'string');
    screens++;
  }
  node('scan-basis').value='reference';node('scan-minimum').value='5';node('scan-return-band').value='0.5_1';
  const fixture={history:[{side:'long',reference:{n:5,expectancy_pct:.74,display_return_pct:.74,return_band:'0.5_1',holding:{label:'1–2 trading days · may carry overnight',detail:'Six chart candles',bars:6}}}]};
  assert.equal(context.matchingHistory(fixture).length,1);
  assert.match(context.matchHistoryCaption(fixture),/5 historical trades/);
  assert.match(context.matchHistoryCaption(fixture),/may carry overnight/);
  assert.doesNotMatch(context.matchHistoryCaption(fixture),/WR|expectancy|Max FAV/);
  const data=await get('/api/backtests/capital',{symbol:'CEMPRO',pattern:'channel',timeframe:'1H',side:'long',segment:'test',capital:'10000'});
  context.renderCapital(data);
  assert.match(node('capital-result').innerHTML,/₹8,704\.67/);
  assert.match(node('capital-result').innerHTML,/Held-out test/);
  assert.match(node('capital-result').innerHTML,/Cash ledger/);
  assert.match(context.capitalCurve({...data,eligible_trades:0}),/No eligible trades/);
  console.log(`Frontend contract passed: ${screens} filter combinations across ${matches.length} matches; capital rendering and empty account.`);
})().catch(e=>{console.error(e);process.exitCode=1;});
