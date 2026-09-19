// Navigation regression: pattern state must never collide with Expo's reserved `state` parameter.
const fs=require('node:fs'),assert=require('node:assert/strict'),ts=require('typescript');
const source=fs.readFileSync('src/patternHistory/logic.ts','utf8');
const output=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2020}}).outputText;
const logic={};new Function('exports',output)(logic);
const selection={symbol:'M&M',pattern_id:'CH16',variant:'canonical',state:'setup',strategy_key:'ch16-canonical-long-1d',detection_id:'abc123'};
const url=new URL(logic.historyHref(selection,10),'http://local');
assert.equal(url.searchParams.has('state'),false);
assert.equal(url.searchParams.get('phase'),'setup');
assert.equal(url.searchParams.get('symbol'),'M&M');
assert.equal(url.searchParams.get('detection_id'),'abc123');
assert.equal(url.searchParams.get('horizon'),'10');
assert.equal(new URLSearchParams(logic.query(selection)).get('state'),'setup');
assert.equal(logic.pilotSelection({symbol:'TEST',strategyKey:'ch16-canonical-long-1d',timeframe:'1D',side:'short'}),null);
assert.equal(logic.pilotSelection({symbol:'TEST',strategyKey:'cdlengulfing-canonical_context-long-1d',timeframe:'1D',side:'long',detectionState:'forming'}).state,'setup');
console.log('Pattern history navigation and identity checks passed.');
