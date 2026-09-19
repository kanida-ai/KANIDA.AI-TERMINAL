// Click a live research detection and capture the chart + evidence card. QA server only (8083).
const fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const BASE='http://127.0.0.1:8083',OUT=path.join(__dirname,'..','qa','live');
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
 await page.goto(BASE+'/discover',{waitUntil:'domcontentloaded',timeout:60000});
 await sleep(9000);
 // first live detection row inside a research scanner card
 const rows=page.locator('[role=option] >> visible=true');
 const n=await rows.count();
 console.log('rows visible:',n);
 for(let i=0;i<Math.min(n,12);i++){
  const label=await rows.nth(i).getAttribute('aria-label');
  if(label&&/forming|confirmed/i.test(label)){console.log('clicking:',label.slice(0,90));await rows.nth(i).click();break;}
 }
 await sleep(9000);
 await page.screenshot({path:path.join(OUT,'detection-chart.png')});
 // scroll the block into view for the evidence card
 await page.mouse.wheel(0,600);await sleep(2500);
 await page.screenshot({path:path.join(OUT,'detection-evidence.png')});
 const txt=await page.evaluate(()=>{const r=Array.from(document.querySelectorAll('[role=region][aria-label]')).find(e=>/^Backtest/i.test(e.getAttribute('aria-label')||''));return r?r.innerText.slice(0,900):'no backtest region';});
 console.log('--- evidence card text ---\n'+txt);
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1);});
