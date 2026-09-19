const {chromium,expect}=require('@playwright/test');
async function main(){
 const browser=await chromium.launch({channel:'msedge',headless:true});
 try {
  const page=await browser.newPage({viewport:{width:390,height:844}});
  let release;const held=new Promise(resolve=>{release=resolve});let delayed;
  await page.route('**/api/backtests/capital?**',async route=>{
   const response=await route.fetch();
   if(new URL(route.request().url()).searchParams.get('capital')==='10000'){
    delayed=true;await held;
   }
   await route.fulfill({response});
  });
  await page.goto('http://127.0.0.1:8082/',{waitUntil:'networkidle'});
  await page.getByRole('button',{name:'See evidence',exact:true}).first().click();
  await page.getByRole('button',{name:'Capital',exact:true}).click();
  await page.getByRole('button',{name:'Replay historical account',exact:true}).click();
  await expect.poll(()=>delayed).toBe(true);
  await page.getByRole('button',{name:'₹30,000',exact:true}).click();
  await page.getByRole('button',{name:'Replay historical account',exact:true}).click();
  const starting=page.getByText('Starting capital',{exact:true}).locator('..');
  await expect(starting).toContainText('₹30,000');
  const oldResponse=page.waitForResponse(r=>r.url().includes('/api/backtests/capital')&&new URL(r.url()).searchParams.get('capital')==='10000');
  release();await oldResponse;
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  await expect(starting).toContainText('₹30,000');
  console.log('Delayed ₹10,000 replay cannot overwrite the newer ₹30,000 result.');
 } finally {await browser.close();}
}
main().catch(e=>{console.error(e);process.exitCode=1});
