// THE INTELLIGENCE ENGINE, SERVER SIDE — the SAME TypeScript the browser runs, not a port of it.
//
// One long-lived process. It reads one JSON request per line on stdin and writes one JSON answer per line on
// stdout, in order. The snapshot worker (server/kanida_pilot/snapshots.py) is its only caller.
//
//   {"id":1,"op":"version"}                                      -> {"id":1,"engine_version":..,"rules_version":..}
//   {"id":2,"op":"reading","at":..,"grid":..,"pcr":..,"maxPain":..,"iv":..}
//        -> the reading AT `at`, described from its own fifteen minutes on its own contracts (snapshot mode)
//   {"id":3,"op":"opening","at":..,"body":<oi-by-strike>}         -> the session's first reading
//   {"id":4,"op":"chain","prev":<stored>,"cur":<reading>}         -> session context from the STORED previous one
//
// Why not Python: the pane's words and rules live in src/derivative/signal.ts. A second implementation would
// drift, and a drifted engine writing immutable history is worse than none.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),readline=require('node:readline');
const ROOT=path.join(__dirname,'..','..');
const ts=require(path.join(ROOT,'node_modules','typescript'));
const load=(rel,req)=>{
 const code=ts.transpileModule(fs.readFileSync(path.join(ROOT,rel),'utf8'),
  {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
 const ctx={exports:{},require:req,process:{env:{}},console};
 vm.runInNewContext(code,ctx);return ctx.exports;
};
const L=load('src/derivative/logic.ts',n=>{throw Error('unexpected import: '+n);});
const SUM=load('src/derivative/summary.ts',n=>{if(/\/logic$/.test(n))return L;if(/\/types$/.test(n))return {};throw Error('unexpected import: '+n);});
const SIG=load('src/derivative/signal.ts',n=>{if(/\/logic$/.test(n))return L;if(/\/summary$/.test(n))return SUM;if(/\/types$/.test(n))return {};throw Error('unexpected import: '+n);});

// objects crossing the vm boundary are re-serialised so the caller always receives plain JSON
const plain=v=>v==null?null:JSON.parse(JSON.stringify(v));

function handle(req){
 switch(req.op){
  case 'version':return {engine_version:SIG.ENGINE_VERSION,rules_version:L.SIGNAL_RULE_VERSION};
  case 'reading':{
   const list=SIG.states({grid:req.grid,pcr:req.pcr,maxPain:req.maxPain,iv:req.iv},{intervalOnly:true});
   const got=Array.prototype.find.call(list,x=>String(x.timestamp)===String(req.at));
   return {reading:plain(got)};
  }
  case 'opening':return {reading:plain(SIG.openingSnapshot(req.body,req.at))};
  case 'chain':return {reading:plain(SIG.chainStep(req.prev||null,req.cur))};
  default:throw new Error('unknown op '+req.op);
 }
}

const rl=readline.createInterface({input:process.stdin,crlfDelay:Infinity});
rl.on('line',line=>{
 if(!line.trim())return;
 let req;
 try{req=JSON.parse(line);}catch(e){process.stdout.write(JSON.stringify({id:null,error:'bad json'})+'\n');return;}
 try{process.stdout.write(JSON.stringify({id:req.id,...handle(req)})+'\n');}
 catch(e){process.stdout.write(JSON.stringify({id:req.id,error:String(e&&e.message||e)})+'\n');}
});
