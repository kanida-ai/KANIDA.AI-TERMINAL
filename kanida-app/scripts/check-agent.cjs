const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const ts=require('typescript');
const assert=require('node:assert/strict');
const {chromium,expect}=require('@playwright/test');
function moduleFrom(name,require){
 const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'../src/'+name+'.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
 const context={exports:{},require,process:{env:{}},URLSearchParams,AbortController,setTimeout,clearTimeout};
 vm.runInNewContext(code,context);return context.exports;
}
async function main(){
 const model=moduleFrom('model',()=>({Platform:{OS:'web'}}));
 const {decision}=moduleFrom('decision',()=>model);
 const all=await (await fetch('http://127.0.0.1:8082/api/matches?min_trades=0')).json();
 const sample=all.find(m=>m.symbol==='LTTS'&&m.timeframe==='1H');
 const own=(n,mean,side='long')=>({side,reference:{n,expectancy_pct:mean,display_return_pct:mean}});
 assert.equal(decision({...sample,history:[own(4,1)]},model.initialFilters).verdict,'watch');
 assert.equal(decision({...sample,history:[own(5,1)]},model.initialFilters).verdict,'review');
 assert.equal(decision({...sample,history:[own(5,-1)]},model.initialFilters).verdict,'pass');
 assert.equal(decision({...sample,direction:'neutral',history:[own(5,1)]},model.initialFilters).verdict,'watch');
 assert.equal(decision({...sample,history:[own(5,1),own(5,-1,'short')]},model.initialFilters,'short').verdict,'pass');
 const browser=await chromium.launch({channel:'msedge',headless:true});
 try{
  const page=await browser.newPage({viewport:{width:1280,height:1000}});
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  for(const tf of ['4H','1W']){
   const m=all.find(m=>m.timeframe===tf&&decision(m,model.initialFilters).verdict==='review');
   assert.ok(m,tf+' evidence available');
   await page.goto('http://127.0.0.1:8082/',{waitUntil:'networkidle'});
   await page.getByRole('textbox',{name:'Tell your agent what to find'}).fill(m.symbol);
   await page.getByRole('button',{name:'Find',exact:true}).click();
   const row=page.getByRole('button',{name:`Inspect ${m.symbol} ${m.timeframe} ${m.pattern_name}`,exact:true});
   await row.click();
   await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).waitFor();
   const chart=page.getByRole('button',{name:'Inspect candle date and closing price',exact:true});
   await expect(chart.locator('path[stroke-dashoffset]').first()).toHaveAttribute('stroke-dashoffset','0');
   await chart.screenshot({path:path.join(__dirname,'../qa/'+tf+'-chart.png')});
   await row.click();
   await expect(page.getByText('Agent tracing the pattern…',{exact:true})).toBeVisible();
   console.log(tf+' chart and repeated-stock reveal verified: '+m.symbol+' / '+m.pattern_name);
  }
  const reduced=await browser.newPage({viewport:{width:390,height:844},reducedMotion:'reduce'});
  reduced.on('pageerror',e=>errors.push(e.message));
  await reduced.goto('http://127.0.0.1:8082/',{waitUntil:'networkidle'});
  await reduced.getByRole('button',{name:'Inspect LTTS 1H Cup & Handle',exact:true}).click();
  await expect(reduced.getByText('Structure drawn. Scenarios illustrated.',{exact:true})).toBeVisible({timeout:2000});
  await expect(reduced.locator('path[stroke-dashoffset]').first()).toHaveAttribute('stroke-dashoffset','0');
  assert.deepEqual(errors,[]);
  console.log('5 decision cases, reduced-motion rendering and remaining timeframe charts passed.');
 }finally{await browser.close()}
}
main().catch(e=>{console.error(e);process.exitCode=1});
