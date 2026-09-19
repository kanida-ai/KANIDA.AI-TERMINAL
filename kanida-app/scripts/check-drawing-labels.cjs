const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),ts=require('typescript');
const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'../src/researchDrawing.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2020}}).outputText;
const helpers={};new Function('exports',code)(helpers);const {placedLabels}=helpers;
const label=(name,i=1,value=100)=>({label:name,point:{index:i,value}});
function validate(labels,left,right,top,bottom){for(const p of labels.filter(l=>!l.hidden)){assert.ok(p.x-p.width/2>=left-.001);assert.ok(p.x+p.width/2<=right+.001);assert.ok(p.y>=top+11&&p.y<=bottom-3);}for(let i=0;i<labels.length;i++)for(let j=0;j<i;j++){const a=labels[i],b=labels[j];if(a.hidden||b.hidden)continue;assert.ok(Math.abs(a.x-b.x)>=(a.width+b.width)/2+4-.001||Math.abs(a.y-b.y)>=14-.001,`${a.label} overlaps ${b.label}`);}}
const input=[label('Pattern recognized',19,110),label('Confirmation',20,111)];
const narrow=placedLabels(input,i=>180+i*3,v=>40+(111-v)*2,7,240,27,163);
validate(narrow,7,240,27,163);assert.equal(narrow.filter(l=>!l.hidden).length,2);assert.equal(narrow[0].label,'Recognized');assert.equal(narrow[1].label,'Confirmed');assert.equal(narrow[0].fullLabel,'Pattern recognized');assert.equal(narrow[0].anchorX,237);assert.equal(narrow[1].anchorX,240);
assert.deepEqual(placedLabels(input,i=>180+i*3,v=>40+(111-v)*2,7,240,27,163),narrow,'Placement is deterministic');
assert.deepEqual(input,[label('Pattern recognized',19,110),label('Confirmation',20,111)],'Frozen geometry is unchanged');
const dense=placedLabels(Array.from({length:12},(_,i)=>label('Point '+i)),()=>150,()=>40,7,340,27,163);validate(dense,7,340,27,163);assert.equal(dense.filter(l=>!l.hidden).length,12);
const impossible=placedLabels([label('Pattern recognized')],()=>10,()=>20,0,20,0,8);assert.equal(impossible[0].hidden,true);assert.equal(impossible[0].fullLabel,'Pattern recognized','Overflow remains available for accessible fallback');
const dataset=path.join(__dirname,'../../docs/pattern_research/drawing_audit_examples.json');
if(fs.existsSync(dataset)){const data=JSON.parse(fs.readFileSync(dataset,'utf8'));let overflow=0;for(const example of data.examples){const labels=example.lines.filter(l=>l.role==='label').map(l=>({label:l.label,point:l.points[0]}));const lo=Math.min(...example.bars.map(b=>b.low)),hi=Math.max(...example.bars.map(b=>b.high));const placed=placedLabels(labels,i=>7+(i+.5)/example.bars.length*233,v=>27+(hi-v)/(hi-lo||1)*136,7,240,27,163);validate(placed,7,240,27,163);overflow+=placed.filter(l=>l.hidden).length;}console.log(`Checked ${data.examples.length} real examples at a narrow 233px plot; ${overflow} labels require text fallback.`);}
console.log('PASS: deterministic bounded labels, collision avoidance, short lifecycle names and frozen anchors.');
