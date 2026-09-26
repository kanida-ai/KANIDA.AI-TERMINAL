"""Slice 13 browser regressions against scripts/e2e/slice13_harness.py (see its docstring). Exits 1 on any failure."""
import json,os,re,sys
from pathlib import Path
from playwright.sync_api import sync_playwright
OUT=Path(os.environ['E2E_OUT']);B='http://127.0.0.1:8093'
ids=json.load(open(OUT/'ids.json'));cookies=json.load(open(OUT/'cookies.json'));checks={}
def ok(name,cond):checks[name]=bool(cond)
with sync_playwright() as p:
 br=p.chromium.launch();ctx=br.new_context(viewport={'width':1400,'height':1000});ctx.add_cookies(cookies);pg=ctx.new_page()
 # P03 horizon label and P02 Adjust parity on a calendar
 pg.goto(f"{B}/strategies?id={ids['calendar']}");pg.wait_for_timeout(3500)
 t=pg.inner_text('body');ok('P03 model horizon shown','Model at near expiry' in t)
 bm=re.search(r'Max profit\n\+?(₹[\d,]+)',t);be=re.search(r'Breakeven\n([\d,]+ · [\d,]+)',t)
 pg.get_by_role('button',name='Adjust').first.click();pg.wait_for_timeout(3000);t=pg.inner_text('body')
 am=re.search(r'Best case now\n(₹[\d,]+)',t);ab=re.search(r'Breakevens now\n([\d,]+ / [\d,]+)',t)
 ok('P02 Adjust best case = builder max profit',bm and am and bm.group(1)==am.group(1))
 ok('P02 Adjust breakevens = builder',be and ab and be.group(1).replace(' · ',' / ')==ab.group(1))
 pg.keyboard.press('Escape');pg.goto(f"{B}/strategies?id={ids['calendar']}");pg.wait_for_timeout(3000)
 # P01 chain identity: the Sep chain never selects or removes the Oct leg
 pg.get_by_role('button',name='Add from chain').first.click();pg.wait_for_timeout(1500)
 ok('P01 Oct buy not shown as added in Sep chain',pg.get_by_role('button',name='Buy 23000 call 29 Sep (added)').count()==0)
 pg.get_by_role('button',name='Sell 23000 call 29 Sep (added)').first.click();pg.wait_for_timeout(800)
 ok('P01 still not added after removing the Sep short',pg.get_by_role('button',name='Buy 23000 call 29 Sep (added)').count()==0)
 pg.get_by_role('button',name='Buy 23000 call 29 Sep').first.click();pg.wait_for_timeout(800)
 pg.get_by_role('button',name='Done').click();pg.wait_for_timeout(2500)
 ok('P01 Oct leg survives a Sep buy',pg.get_by_role('button',name='Expiry 6 Oct. Change').count()==1 and pg.get_by_role('button',name='Expiry 29 Sep. Change').count()==1)
 # P04 fail closed
 pg.goto(f"{B}/strategies?id={ids['missing']}");pg.wait_for_timeout(3500);t=pg.inner_text('body')
 ok('P04 risk unavailable with the missing contract named','Risk unavailable' in t and '30000 CE' in t and not re.search(r'Max loss\n[−-]₹',t))
 # P07 tick before calculate
 pg.goto(f"{B}/strategies?id={ids['vertical']}");pg.wait_for_timeout(3500)
 f=pg.get_by_label('Entry price for 23000 CE');f.click();f.fill('120.12');pg.keyboard.press('Tab');pg.wait_for_timeout(2500)
 ok('P07 off-tick entry snaps on blur',f.input_value()=='120.1' and 'Rounded 120.12 to 120.1' in pg.inner_text('body'))
 ctx.close()
 # P01 phone leg editor follows the selected expiry
 ctx=br.new_context(viewport={'width':390,'height':844},is_mobile=True,has_touch=True);ctx.add_cookies(cookies);pg=ctx.new_page()
 pg.goto(f"{B}/strategies?id={ids['calendar_phone']}");pg.wait_for_timeout(3500)
 pg.get_by_role('button',name=re.compile('^Edit leg: Sell 1 × 23000 CE 29 Sep')).first.click();pg.wait_for_timeout(1200)
 pg.get_by_text(re.compile(r'^6 Oct · ')).first.click();pg.wait_for_timeout(1500);t=pg.inner_text('body')
 ok('P01 leg editor shows the Oct contract, not Sep','NIFTY26OCT23000CE' in t and 'NIFTY26SEP23000CE' not in t)
 br.close()
print(json.dumps(checks,indent=1));sys.exit(0 if all(checks.values()) else 1)
