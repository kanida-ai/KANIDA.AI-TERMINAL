// Backlog item 2 screenshots of /discover at 1440x900 and 390x844.
// Usage: node scripts/shot-item2.cjs <before|after> [baseUrl]   (default base http://127.0.0.1:8083)
const fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const tag=process.argv[2]||'before',BASE=process.argv[3]||'http://127.0.0.1:8083';
const OUT=path.join(__dirname,'..','qa','item2');
const fixture=fs.readFileSync(path.join(__dirname,'qa_server.py'),'utf8');
const EMAIL=(fixture.match(/users\.c\.email=='([^']+)'/)||[])[1],PASSWORD=(fixture.match(/ph\.hash\('([^']+)'\)/)||[])[1];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
 fs.mkdirSync(OUT,{recursive:true});
 const browser=await chromium.launch({channel:'msedge',headless:true});
 for(const [name,vp] of [['desktop',{width:1440,height:900}],['phone',{width:390,height:844}]]){
  const ctx=await browser.newContext({viewport:vp,isMobile:name==='phone',hasTouch:name==='phone'});
  const page=await ctx.newPage();
  await page.goto(BASE+'/signin',{waitUntil:'networkidle',timeout:45000});
  await page.getByRole('textbox',{name:'Email'}).fill(EMAIL);
  await page.locator('input[aria-label="Password"]').fill(PASSWORD);
  await page.getByRole('button',{name:'Sign in',exact:true}).last().click();
  await page.waitForURL(u=>!new URL(u).pathname.startsWith('/signin'),{timeout:30000});
  await sleep(1500);
  await page.goto(BASE+'/discover',{waitUntil:'domcontentloaded',timeout:45000});
  await sleep(9000);
  const file=path.join(OUT,`${tag}-discover-${name}.png`);
  await page.screenshot({path:file});
  if(process.env.DUMP){const txt=await page.evaluate(()=>document.body.innerText);fs.writeFileSync(path.join(OUT,`${tag}-${name}.txt`),txt);}
  console.log(file);
  await ctx.close();
 }
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1);});
