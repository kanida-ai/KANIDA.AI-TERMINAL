"""Slice 14 browser checks (P08 paper modes/times, consent, P15 origin panel, P10 phone badge, P09 costs). Uses slice13_harness.py."""
import json,os,re,sys
from pathlib import Path
from playwright.sync_api import sync_playwright
OUT=Path(os.environ['E2E_OUT']);B='http://127.0.0.1:8093'
ids=json.load(open(OUT/'ids.json'));cookies=json.load(open(OUT/'cookies.json'));checks={}
def ok(name,cond):checks[name]=bool(cond)
with sync_playwright() as p:
 br=p.chromium.launch();ctx=br.new_context(viewport={'width':1400,'height':1000});ctx.add_cookies(cookies);pg=ctx.new_page()
 pg.goto(f"{B}/strategies?view=paper");pg.wait_for_timeout(3000);t=pg.inner_text('body')
 ok('P08 practice mode named','PRACTICE · OPEN' in t and 'Stored-price practice' in t)
 ok('P08 action time and price time both shown',re.search(r'Opened .+ using prices from ',t))
 ok('P08 capital scoped','Live-quote paper capital (deployments only)' in t or 'Stored-price practice runs have no capital account' in t or 'deployments only' in t)
 pg.screenshot(path=str(OUT/'s14_paper.png'),full_page=True)
 pg.goto(f"{B}/strategies");pg.wait_for_timeout(2500);t=pg.inner_text('body')
 ok('consent card off by default','Help measure which adjustments work' in t and 'Sharing: off' in t)
 if ids.get('from_discover'):
  pg.goto(f"{B}/strategies?id={ids['from_discover']}");pg.wait_for_timeout(3500);t=pg.inner_text('body')
  ok('P15 origin panel','FROM DISCOVER' in t and 'Your view: Rise' in t and 'Shown then' in t)
  ok('P09 round-trip costs','round trip' in t or 'More numbers' in t)
  pg.screenshot(path=str(OUT/'s14_origin.png'),full_page=True)
 ctx.close()
 ctx=br.new_context(viewport={'width':390,'height':844},is_mobile=True,has_touch=True);ctx.add_cookies(cookies);pg=ctx.new_page()
 pg.goto(f"{B}/strategies?view=discover");pg.wait_for_timeout(3000)
 ok('P10 one risk-limit field',pg.get_by_text('Most I am willing to lose',exact=False).count()==1 and pg.get_by_text('Max-loss budget',exact=False).count()==0)
 pg.screenshot(path=str(OUT/'s14_discover_phone.png'),full_page=True)
 br.close()
print(json.dumps(checks,indent=1));sys.exit(0 if all(checks.values()) else 1)
