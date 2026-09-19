// Screenshot of the Derivative tab for owner review. QA server only (8083).
// Usage: node scripts/shot-derivative.cjs [underlying]
const fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const BASE='http://127.0.0.1:8083',OUT=path.join(__dirname,'..','qa','derivative');
const SYM=(process.argv[2]||'NIFTY').toUpperCase();
const fixture=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(fixture.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(fixture.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const ctx=await browser.newContext({viewport:{width:1600,height:1000}});
 const page=await ctx.newPage();
 await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
 await page.getByRole('textbox',{name:'Email'}).fill(EMAIL);
 await page.locator('input[aria-label="Password"]').fill(PASSWORD);
 await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
 await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});
 // what the API actually holds, printed so the screenshot can be trusted
 for(const p of ['/api/derivatives/status',`/api/derivatives/unusual?limit=5`,`/api/derivatives/indices`]){
  const body=await page.evaluate(async u=>{const r=await fetch(u,{credentials:'include'});return {status:r.status,text:(await r.text()).slice(0,600)};},p);
  console.log('---',p,body.status,'\n',body.text);
 }
 await page.goto(BASE+`/derivative?underlying=${SYM}`,{waitUntil:'domcontentloaded',timeout:60000});
 await sleep(12000);
 await page.screenshot({path:path.join(OUT,`derivative-${SYM.toLowerCase()}-top.png`)});
 await page.mouse.wheel(0,900);await sleep(3000);
 await page.screenshot({path:path.join(OUT,`derivative-${SYM.toLowerCase()}-mid.png`)});
 await page.mouse.wheel(0,900);await sleep(3000);
 await page.screenshot({path:path.join(OUT,`derivative-${SYM.toLowerCase()}-bottom.png`)});
 const txt=await page.evaluate(()=>document.body.innerText.slice(0,2500));
 console.log('--- page text ---\n'+txt);
 await browser.close();
 console.log('shots in',OUT);
})().catch(e=>{console.error(e);process.exit(1);});
