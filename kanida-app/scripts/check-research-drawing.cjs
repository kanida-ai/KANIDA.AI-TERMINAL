const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),ts=require('typescript');
const loaded={};function load(name){if(loaded[name])return loaded[name];const exports={};loaded[name]=exports;const js=ts.transpileModule(fs.readFileSync(path.join(__dirname,'../src',name+'.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2020}}).outputText;new Function('exports','require',js)(exports,p=>load(p.replace('./','')));return exports;}
const d=load('researchDrawing'),g=load('patternGeometry');
const bars=Array.from({length:30},(_,i)=>({high:100+i%3,low:90-i%2,close:95}));
const lines=[{label:'Resistance',role:'boundary',points:[{index:4,value:110},{index:12,value:106}]},{label:'Support',role:'boundary',points:[{index:4,value:80},{index:12,value:84}]}];
const research={pattern:'CH14',start_index:4,end_index:12,lines};
const rendered=g.patternGeometry(research,bars);
assert.equal(Math.max(...rendered.envelope.map(p=>p.index)),12,'A historical formation must not stretch to the newest candle');
assert.equal(rendered.projection.length,0,'An old formation must not project past later known prices');
assert.equal(g.patternGeometry({...research,pattern:'symmetrical_triangle'},bars).envelope[1].index,12,'Legacy aliases must also stop at the selected event');
assert.equal(d.visualPattern('CH01'),'cup_handle');
assert.equal(d.visualPattern('CH07'),'channel');
assert.deepEqual(d.visibleDrawingValues(research,4,29),[110,106,80,84],'Published levels outside candles belong in the price scale');
const candle={pattern:'CDLDOJI',lines:[{role:'candle_range',label:'Pattern candles',points:[{index:8,value:91},{index:8,value:99}]}]};
assert.equal(d.candleRanges(candle)[0].first,8);assert.equal(d.candleRanges(candle)[0].last,8,'One candle remains one candle');
assert.equal(d.hasDrawing({lines:[]}),false);
assert.equal(d.drawingColor({role:'boundary',label:'Failure level'}),'#F17D87');
console.log('Research drawing causality, scale, candle bounds and legacy checks passed.');

assert.equal(g.patternGeometry({pattern:'CH02',start_index:0,end_index:29,lines:[lines[0]]},bars).swings.length,0,'A missing swing must not be invented from candles');
const fittedCup={role:'curve',label:'Cup',points:[{index:4,value:104.3},{index:8,value:94.1},{index:12,value:103.9}]};
assert.deepEqual(d.drawingLines({lines:[fittedCup,...candle.lines]}),[fittedCup],'Stored detector fit must pass through unchanged rather than be replaced by an OHLC-derived curve');
