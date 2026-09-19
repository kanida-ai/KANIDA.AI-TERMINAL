// Screenshots of the app running on live data: chart page, Discover, and the Data status popover.
// QA server only (8083). Usage: node scripts/shot-live.cjs
const fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const BASE='http://127.0.0.1:8083';
const OUT=path.join(__dirname,'..','qa','live');
const fixture=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(fixture.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(fixture.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const ctx=await browser.newContext({viewport:{width:1440,height:900}});
 const page=await ctx.newPage();
 await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
 await page.getByRole('textbox',{name:'Email'}).fill(EMAIL);
 await page.locator('input[aria-label="Password"]').fill(PASSWORD);
 await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
 await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});
 await sleep(2000);
 const state=await page.evaluate(async()=>{const r=await fetch('/api/state',{credentials:'include'});const s=await r.json();return {source_latest:s.source_latest,source_stale:s.source_stale,candle_source:s.candle_source,matches:s.matches,current:s.current_matches,data_status:s.data_status};});
 console.log(JSON.stringify(state,null,1).slice(0,1200));
 await page.goto(BASE+'/chart',{waitUntil:'domcontentloaded',timeout:45000});
 await sleep(6000);
 await page.screenshot({path:path.join(OUT,'chart-live.png')});
 // Data status popover from the top bar pill
 const pill=page.getByRole('button',{name:/Open data status/}).first();
 if(await pill.count()){await pill.click();await sleep(800);await page.screenshot({path:path.join(OUT,'data-status.png')});await page.keyboard.press('Escape');}
 else console.log('no data-status pill found');
 await page.goto(BASE+'/discover',{waitUntil:'domcontentloaded',timeout:45000});
 await sleep(7000);
 await page.screenshot({path:path.join(OUT,'discover-live.png')});
 await browser.close();
 console.log('shots in',OUT);
})().catch(e=>{console.error(e);process.exit(1);});
