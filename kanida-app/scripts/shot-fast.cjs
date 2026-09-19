// Fast look at a page while working on it. Seconds, not minutes.
//
//   node scripts/shot-fast.cjs [route] [width] [height]
//   node scripts/shot-fast.cjs derivative 1720 1500
//
// It goes through the dev proxy (8090), so the JS is Metro's hot-reloaded bundle and the
// data is the QA pilot's: a style edit is on screen on the next run, with no `expo export`
// and no server restart. The signed-in session is saved to qa/.session.json and reused, so
// repeated runs do not spend the pilot's sign-in allowance (12 per 900s) or wait on a login.
//
// Deliberately does NOT assert anything - it is a pair of eyes for the inner loop. Ship
// nothing on its say-so: check-derivative.cjs and the browser suites are still the gate,
// and they run against a real exported bundle.
const fs = require('fs'), path = require('path');
const { chromium } = require('playwright-core');

const BASE = process.env.SHOT_BASE || 'http://127.0.0.1:8090';
const ROUTE = (process.argv[2] || 'derivative').replace(/^\//, '');
const W = Number(process.argv[3] || 1720), H = Number(process.argv[4] || 1500);
const OUT = path.join(__dirname, '..', 'qa', 'fast');
const SESSION = path.join(__dirname, '..', 'qa', '.session.json');

const fixture = fs.readFileSync(path.join(__dirname, 'qa_server.py'), 'utf8');
const EMAIL = (fixture.match(/users\.c\.email=='([^']+)'/) || [])[1];
const PASSWORD = (fixture.match(/ph\.hash\('([^']+)'\)/) || [])[1];
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  fs.mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch({ channel: 'msedge', headless: true });
  const fresh = fs.existsSync(SESSION);
  const ctx = await browser.newContext({
    viewport: { width: W, height: H },
    ...(fresh ? { storageState: SESSION } : {}),
  });
  const page = await ctx.newPage();

  await page.goto(`${BASE}/${ROUTE}`, { waitUntil: 'domcontentloaded', timeout: 120000 });
  // A stale or absent session lands on /signin. Sign in once, then keep it for next time.
  if (new URL(page.url()).pathname.startsWith('/signin') || !fresh) {
    await page.goto(`${BASE}/signin`, { waitUntil: 'domcontentloaded', timeout: 180000 });
    await page.getByRole('textbox', { name: 'Email' }).waitFor({ state: 'visible', timeout: 180000 });
    await page.getByRole('textbox', { name: 'Email' }).fill(EMAIL);
    await page.locator('input[aria-label="Password"]').fill(PASSWORD);
    await page.getByRole('button', { name: 'Sign in', exact: true }).last().click();
    // Expo Router navigates client-side, so there is no load event to wait on: poll the URL.
    for (let i = 0; i < 120; i++) {
      if (!new URL(page.url()).pathname.startsWith('/signin')) break;
      await sleep(1000);
    }
    if (new URL(page.url()).pathname.startsWith('/signin')) throw new Error('sign-in did not complete');
    await ctx.storageState({ path: SESSION });
    await page.goto(`${BASE}/${ROUTE}`, { waitUntil: 'domcontentloaded', timeout: 120000 });
  }

  await sleep(Number(process.env.SHOT_SETTLE || 9000));
  // SHOT_FIND="ΔOI by strike" scrolls that text into view first, so a block far down the
  // page can be looked at without capturing everything above it.
  if (process.env.SHOT_FIND) {
    const target = page.getByText(process.env.SHOT_FIND, { exact: false }).first();
    await target.scrollIntoViewIfNeeded({ timeout: 30000 }).catch(() => {});
    await sleep(2500);
  }
  const file = path.join(OUT, `${ROUTE.replace(/[^a-z0-9]+/gi, '-')}-${W}.png`);
  await page.screenshot({ path: file, fullPage: process.env.SHOT_FULL === '1' });

  const errors = [];
  page.on('pageerror', e => errors.push(String(e)));
  console.log(file);
  if (errors.length) console.log('page errors:\n' + errors.join('\n'));
  await browser.close();
})().catch(e => { console.error(e.message || e); process.exit(1); });
