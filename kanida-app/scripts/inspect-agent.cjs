const {chromium}=require('@playwright/test');
const path=require('node:path');
async function main(){
 const browser=await chromium.launch({channel:'msedge',headless:true});
 try {for(const [name,width,height] of [['desktop',1440,1100],['iphone',390,844],['tablet',768,1024]]){
  const page=await browser.newPage({viewport:{width,height}});
  page.on('pageerror',e=>console.log('PAGEERROR',e.message));
  await page.goto('http://127.0.0.1:8082/',{waitUntil:'networkidle'});
  await page.getByRole('button',{name:/Inspect LTTS 1H/}).waitFor();
  await page.screenshot({path:path.join(__dirname,'../qa/'+name+'-agent.png')});
  if(width<1050)await page.getByRole('button',{name:/Inspect LTTS 1H/}).click();
  await page.getByRole('button',{name:'Replay drawing',exact:true}).first().waitFor();
  await page.getByRole('button',{name:'Replay drawing',exact:true}).first().click();
  await page.waitForTimeout(450);
  await page.screenshot({path:path.join(__dirname,'../qa/'+name+'-drawing.png')});
  await page.waitForTimeout(3200);
  await page.screenshot({path:path.join(__dirname,'../qa/'+name+'-case.png')});
  console.log(name, await page.locator('body').innerText());
  await page.close();
 }}finally{await browser.close()}
}
main().catch(e=>{console.error(e);process.exitCode=1});
