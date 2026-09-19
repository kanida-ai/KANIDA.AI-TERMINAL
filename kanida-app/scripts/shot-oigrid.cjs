// Screenshot of the ΔOI 2x5 strike block for owner review. QA server only (8083).
const fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const BASE='http://127.0.0.1:8083',OUT=path.join(__dirname,'..','qa','derivative');
const SYM=(process.argv[2]||'NIFTY').toUpperCase();
const f=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(f.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(f.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const page=await (await browser.newContext({viewport:{width:1720,height:1500}})).newPage();
 await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
 await page.getByRole('textbox',{name:'Email'}).fill(EMAIL);
 await page.locator('input[aria-label="Password"]').fill(PASSWORD);
 await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
 await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});
 await page.goto(BASE+`/derivative?underlying=${SYM}`,{waitUntil:'domcontentloaded',timeout:60000});
 await sleep(14000);
 const head=page.getByText('ΔOI by strike, through the session').first();
 await head.scrollIntoViewIfNeeded({timeout:20000});
 await sleep(4000);
 await page.screenshot({path:path.join(OUT,`oi-grid-${SYM.toLowerCase()}.png`)});
 // what the block itself is showing, in words
 const txt=await page.evaluate(()=>{
  const all=[...document.querySelectorAll('*')].filter(e=>e.innerText&&e.innerText.includes('ΔOI by strike, through the session'));
  const node=all[all.length-1];return node?node.innerText.slice(0,2000):'(block not found)';});
 console.log('--- block text ---\n'+txt);
 await browser.close();console.log('shot:',path.join(OUT,`oi-grid-${SYM.toLowerCase()}.png`));
})().catch(e=>{console.error(e);process.exit(1);});
