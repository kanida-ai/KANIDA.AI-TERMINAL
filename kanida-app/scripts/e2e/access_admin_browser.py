"""Cloud-preview step 1 browser checks: compass logo, admin panel (codes, requests, users, jobs), request-access form."""
import json,os,re,sys
from pathlib import Path
from playwright.sync_api import sync_playwright
OUT=Path(os.environ['E2E_OUT']);B='http://127.0.0.1:8093'
cookies=json.load(open(OUT/'cookies.json'));checks={}
def ok(n,c):checks[n]=bool(c)
with sync_playwright() as p:
 br=p.chromium.launch()
 anon=br.new_context(viewport={'width':1280,'height':900}).new_page()
 anon.goto(f"{B}/signin");anon.wait_for_timeout(2500)
 anon.get_by_role('button',name='No invitation? Request access').click();anon.wait_for_timeout(300)
 anon.get_by_label('Your name').last.fill('Wait Lister');anon.get_by_label('Email').last.fill('wait@example.invalid');anon.get_by_label('How do you trade options? (optional)').fill('Weekly spreads')
 anon.get_by_role('button',name='Send request').click();anon.wait_for_timeout(1500)
 ok('request access sent','your request is in' in anon.inner_text('body'))
 anon.screenshot(path=str(OUT/'acc_signin.png'),full_page=True)
 ctx=br.new_context(viewport={'width':1280,'height':900});ctx.add_cookies(cookies);pg=ctx.new_page()
 pg.goto(f"{B}/admin");pg.wait_for_timeout(3000);t=pg.inner_text('body')
 ok('admin opens','Invite codes' in t and 'Access requests' in t)
 ok('request visible','wait@example.invalid' in t)
 pg.get_by_role('button',name='Create codes').click();pg.wait_for_timeout(1500);t=pg.inner_text('body')
 ok('code shown once',re.search(r'KANIDA-[A-Z2-7]{4}-[A-Z2-7]{4}-[A-Z2-7]{4}',t) and 'will not be shown again' in t)
 pg.get_by_role('button',name='Approve',exact=True).first.click();pg.wait_for_timeout(1500);t=pg.inner_text('body')
 ok('approval gives invite link','/signup?invite=' in t)
 pg.screenshot(path=str(OUT/'acc_admin.png'),full_page=True)
 pg.get_by_text('Users',exact=True).first.click();pg.wait_for_timeout(1500)
 ok('users tab','owner@example.invalid' in pg.inner_text('body'))
 pg.get_by_text('Jobs & health',exact=True).first.click();pg.wait_for_timeout(2000);t=pg.inner_text('body')
 ok('jobs tab','Option market capture' in t and ('STORED PRICES' in t or 'LIVE PRICES' in t))
 pg.screenshot(path=str(OUT/'acc_jobs.png'),full_page=True)
 br.close()
print(json.dumps(checks,indent=1));sys.exit(0 if all(checks.values()) else 1)
