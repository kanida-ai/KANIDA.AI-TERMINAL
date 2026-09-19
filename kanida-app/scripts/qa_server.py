"""Local UI acceptance-test instance. Never use this fixture in a deployment.

Every setting below is overridable from the environment so a SECOND, isolated QA instance can be brought up
beside the default one - which is what running the browser suites against a scanner in the researched pattern
set needs, without restarting either the pilot on 8082 or the scanner on 8765:

  QA_PORT          8083                     the port (and therefore the origin) this instance serves on
  QA_RESEARCH_URL  http://127.0.0.1:8765    the scanner it reads; point it at your own research-mode instance
  QA_WEB_DIRECTORY <root>/dist-pilot        the exported web build to serve
  QA_STATE_DIR     <root>/var/ui-qa         its own database, key and research index (never shared)
  QA_DETECTION_DB  the scanner's own cache  the detection ledger, which MUST be the scanner's SCANNER_CACHE_DB

The defaults reproduce the original instance exactly, so an unset environment changes nothing.
"""
import os
from pathlib import Path
from cryptography.fernet import Fernet
from sqlalchemy import select
from kanida_pilot.config import Settings
from kanida_pilot.app import create_app
from kanida_pilot.db import users,row
from kanida_pilot.auth import ph

def application():
 root=Path(__file__).resolve().parents[1]
 port=int(os.environ.get('QA_PORT') or 8083)
 state=Path(os.environ.get('QA_STATE_DIR') or (root/'var'/'ui-qa'));state.mkdir(parents=True,exist_ok=True)
 key=state/'key'
 if not key.exists():key.write_bytes(Fernet.generate_key())
 origin=f'http://127.0.0.1:{port}'
 defaults=Settings()
 # The QA instance keeps its OWN research index. Sharing var/research_index.sqlite3 with the pilot let two
 # processes rebuild into one file and delete each other's rows, which left the page stuck on "indexing".
 settings=Settings(origin=origin,origins=[origin],database_url='sqlite:///'+str(state/'qa.sqlite3'),
  encryption_key=key.read_text(),owner_email='qa@example.invalid',
  web_directory=os.environ.get('QA_WEB_DIRECTORY') or str(root/'dist-pilot'),
  research_url=os.environ.get('QA_RESEARCH_URL') or defaults.research_url,
  # The ledger must be the SAME file the scanner writes (its SCANNER_CACHE_DB), or live detection reads a
  # different scan than the one the page is showing.
  pattern_detection_db=os.environ.get('QA_DETECTION_DB') or defaults.pattern_detection_db,
  pattern_index_path=str(state/'research_index.sqlite3'))
 app=create_app(settings)
 with app.state.db.tx() as c:
  if not row(c,select(users).where(users.c.email=='qa@example.invalid')):
   user=app.state.auth.create_user(c,'qa@example.invalid','Pilot tester','owner',ph.hash('local-qa-fixture-password'))
   c.execute(users.update().where(users.c.id==user['id']).values(onboarded=True,policy_version='private-pilot-v1'))
 return app
