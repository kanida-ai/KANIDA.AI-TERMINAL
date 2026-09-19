const {chromium,expect}=require('@playwright/test');
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const directory=path.join(__dirname,'..','qa');fs.mkdirSync(directory,{recursive:true});
async function top(page){await page.evaluate(()=>{for(const e of document.querySelectorAll('*'))if(e.scrollTop)e.scrollTop=0});await page.evaluate(()=>Promise.all(document.getAnimations().map(a=>a.finished.catch(()=>{}))))}
async function shot(page,name){await top(page);await page.screenshot({path:path.join(directory,name+'.png')})}
async function main(){
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const base=process.env.KANIDA_TEST_URL||'http://127.0.0.1:8082';const results=[];
 try {for(const [name,width,height] of [['desktop',1440,1100],['iphone',390,844],['tablet',768,1024]]){
  const page=await browser.newPage({viewport:{width,height},deviceScaleFactor:1,isMobile:name==='iphone',hasTouch:name!=='desktop'});
  const errors=[];page.on('pageerror',e=>{errors.push(e.message);console.error('BROWSER ERROR',e.message)});
  try {
   await page.goto(base+'/',{waitUntil:'networkidle'});
   const row=page.getByRole('button',{name:'Inspect LTTS 1H Cup & Handle',exact:true});await row.waitFor();
   await shot(page,name+'-agent');
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,name+' horizontal overflow');
   await row.click();
   const chart=page.getByRole('button',{name:'Inspect candle date and closing price',exact:true}).last();await chart.waitFor();
   await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
   await shot(page,name+'-case');
   await chart.screenshot({path:path.join(directory,name+'-chart.png')});
   await page.getByRole('button',{name:'Replay drawing',exact:true}).last().click();
   await expect(page.getByText('Agent tracing the pattern…',{exact:true}).last()).toBeVisible();
   const traced=chart.locator('path[stroke-dashoffset]').first();
   await expect.poll(async()=>Number(await traced.getAttribute('stroke-dashoffset'))).toBeGreaterThan(0);
   const halfway=Number(await traced.getAttribute('stroke-dashoffset'));
   await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
   await expect.poll(async()=>Number(await traced.getAttribute('stroke-dashoffset'))).toBe(0);
   assert.ok(halfway>0);
   assert.equal(await chart.locator('text').filter({hasText:/\d+ Jul/}).count(),3);
   await chart.click({position:{x:40,y:150}});
   await expect(page.getByText(/Jul 2026.*IST.*Close/).last()).not.toHaveText(/29 Jul 2026/);
   await page.getByRole('button',{name:'Hide scenarios',exact:true}).last().click();
   await expect(chart.getByText('BENCHMARK',{exact:true})).toHaveCount(0);
   await page.getByRole('button',{name:'Show scenarios',exact:true}).last().click();
   await expect(chart.getByText('BENCHMARK',{exact:true})).toHaveCount(1);
   if(process.env.KANIDA_TEST_SAVE==='1'){
    await page.getByRole('button',{name:'Add to watch',exact:true}).last().click();
    await page.getByRole('button',{name:'Watching',exact:true}).last().waitFor();
   }
   if(width>=1050)await page.getByRole('button',{name:'See evidence',exact:true}).last().click();
   await page.getByRole('button',{name:'History',exact:true}).click();
   await page.getByText('Best completed trade',{exact:true}).waitFor();
   await shot(page,name+'-history');
   await page.getByRole('button',{name:'Capital',exact:true}).click();
   await page.getByRole('button',{name:'Replay historical account',exact:true}).click();
   await page.getByText('Ending capital',{exact:true}).waitFor();
   await shot(page,name+'-capital');
   await page.getByRole('button',{name:'Build AutoTrade plan',exact:true}).click();
   await page.getByRole('button',{name:'Review paper plan',exact:true}).waitFor();
   await shot(page,name+'-prepare');
   await page.getByRole('button',{name:'Review paper plan',exact:true}).click();
   await shot(page,name+'-plan');
   if(process.env.KANIDA_TEST_SAVE==='1'){
    await page.getByRole('button',{name:'Save paper plan',exact:true}).click();
    await page.getByText('Draft · not executing',{exact:true}).first().waitFor();
    await page.getByRole('button',{name:'Pause plan',exact:true}).first().click();
    await page.getByRole('button',{name:'Return to draft',exact:true}).first().click();
    await page.getByRole('button',{name:'Pause plan',exact:true}).first().waitFor();
    await page.reload({waitUntil:'networkidle'});
    await page.getByText('Draft · not executing',{exact:true}).first().waitFor();
    await page.getByRole('button',{name:'Archive',exact:true}).first().click();
    await page.getByRole('button',{name:'View archive',exact:true}).click();
    await page.getByText('archived',{exact:true}).first().waitFor();
   }else await page.getByRole('button',{name:'Close',exact:true}).click();
   await page.getByRole('link',{name:'My watch',exact:true}).click();
   if(process.env.KANIDA_TEST_SAVE==='1'){
    await page.getByRole('button',{name:'Review LTTS',exact:true}).waitFor();
    await page.reload({waitUntil:'networkidle'});
    await page.getByRole('button',{name:'Review LTTS',exact:true}).waitFor();
    await shot(page,name+'-watch');
    await page.getByRole('button',{name:'Remove from watch',exact:true}).click();
    await page.getByText('Give your agent something to watch',{exact:true}).waitFor();
   }
   await page.getByRole('link',{name:'Trader Agent',exact:true}).click();
   await page.getByRole('button',{name:'Filters',exact:true}).click();
   await page.getByRole('button',{name:'2–5%',exact:true}).click();
   await page.getByRole('textbox',{name:'Minimum historical trades'}).fill('5');
   await page.getByRole('button',{name:/Show \d+ setups/}).click();
   await shot(page,name+'-filtered');
   if(width<700)await page.getByRole('button',{name:'Ask / search',exact:true}).click();
   await page.getByRole('textbox',{name:'Tell your agent what to find'}).fill('TITAN');
   await page.getByRole('button',{name:'Find',exact:true}).click();
   await page.getByRole('button',{name:'Open TITAN stock overview',exact:true}).click();
   await page.getByText('No qualified pattern in this stored chart.',{exact:true}).first().waitFor();
   await page.getByRole('button',{name:'See Rising Wedge evidence',exact:true}).click();
   await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
   await page.getByRole('button',{name:'Inspect candle date and closing price',exact:true}).last().screenshot({path:path.join(directory,name+'-bearish-chart.png')});
   await page.getByRole('button',{name:'Build AutoTrade plan',exact:true}).click();
   await page.getByText('Rising Wedge · 1D · Sell / short',{exact:true}).waitFor();
   await page.getByRole('button',{name:'Close',exact:true}).click();
   await page.getByRole('link',{name:'AutoTrade',exact:true}).click();
   await page.getByText('Let’s make every decision explicit.',{exact:true}).waitFor();
   await shot(page,name+'-autotrade');
   await page.goto(base+'/connect',{waitUntil:'networkidle'});
   await page.getByRole('button',{name:'Open in Expo Go',exact:true}).waitFor();
   await shot(page,name+'-connect');
   assert.deepEqual(errors,[],name+' browser errors');
   results.push({device:name,width,height,flow:'Animated chart → candle inspection → scenarios → watch → history → cash replay → plan lifecycle → filters → stock overview → short plan → Expo QR',animationVerified:true,errors});
   console.log(name+' passed');
  }catch(e){await page.screenshot({path:path.join(directory,name+'-failure.png')});console.log((await page.locator('body').innerText()).slice(-10000));throw e}finally{await page.close()}
 }}finally{await browser.close()}
 fs.writeFileSync(path.join(directory,'checks.json'),JSON.stringify(results,null,2));console.log(JSON.stringify(results,null,2));
}
main().catch(e=>{console.error(e);process.exitCode=1});
