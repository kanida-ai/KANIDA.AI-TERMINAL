"""Prepare an owner invitation without setting or printing an account password."""
from pathlib import Path
from sqlalchemy import select
import httpx
from dotenv import dotenv_values
from .config import Settings,ROOT
from .db import Database,users,row
from .auth import Auth

def main():
 settings=Settings.load()
 if not settings.owner_email:raise SystemExit('Set PILOT_OWNER_EMAIL before creating the owner invitation.')
 db=Database(settings.database_url);db.migrate()
 with db.tx() as c:existing=row(c,select(users).where(users.c.email==settings.owner_email))
 output=ROOT/'var'/'OWNER_INVITATION.txt'
 if existing:print('Owner account already exists.');return
 if output.exists():print('Owner invitation already exists in var/OWNER_INVITATION.txt.');return
 with httpx.Client(timeout=15) as client:
  raw=Auth(db,settings,client).invite(settings.owner_email,'owner')
 output.parent.mkdir(exist_ok=True)
 output.write_text('Private pilot owner invitation (expires in 72 hours).\nDo not share this link. Choose your own password in the app.\nInvited email: '+settings.owner_email+'\n\n'+settings.origin+'/signup?invite='+raw+'\n',encoding='utf-8')
 print('Owner invitation written to var/OWNER_INVITATION.txt. No account password was created.')
 db.close()
if __name__=='__main__':main()
