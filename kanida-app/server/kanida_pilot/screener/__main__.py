"""The screener job: evaluate every saved scanner on the newest reading and raise alerts.

    python -m kanida_pilot.screener eval          # once (what the scheduled task runs every 5 minutes)
    python -m kanida_pilot.screener eval --loop   # every 60 s
    python -m kanida_pilot.screener eval --force  # even if this reading was already evaluated

A run with no NEW reading since the last one exits after one indexed query, without loading the session — so the
task can repeat around the clock (this machine runs on Pacific time) and only does work when a reading lands.
Read-only on db/derivatives.db (PILOT_DERIVATIVES_DATABASE overrides the path); writes only var/screener.db
(PILOT_SCREENER_DATABASE). It never touches the pilot process, the capture workers, or any vendor feed.
"""
from __future__ import annotations
import argparse,logging,os,sys,time
from pathlib import Path


def main(argv=None):
 p=argparse.ArgumentParser(prog='kanida_pilot.screener')
 p.add_argument('command',choices=['eval'])
 p.add_argument('--loop',action='store_true')
 p.add_argument('--force',action='store_true')
 args=p.parse_args(argv)
 logging.basicConfig(level=logging.INFO,format='%(asctime)s screener %(message)s')
 from .data import Store
 from .service import Screener,screener_database
 root=Path(__file__).resolve().parents[3]
 derivatives=os.getenv('PILOT_DERIVATIVES_DATABASE') or str(root.parent/'db'/'derivatives.db')
 stamp=Path(screener_database()).with_suffix('.last-eval')
 source=Store(derivatives)
 screener=None
 while True:
  latest=None
  if source.available():
   row=source.rows("select max(captured_at) from metrics where scope='underlying'")
   latest=row[0][0] if row else None
  done=stamp.read_text().strip() if stamp.exists() else ''
  if latest and (args.force or latest!=done):
   screener=screener or Screener(derivatives)
   started=time.time()
   count=screener.evaluate_all()
   stamp.write_text(latest)
   logging.info('evaluated %d scanner definitions through %s in %.1fs',count,latest,time.time()-started)
  if not args.loop:return 0
  time.sleep(60)


if __name__=='__main__':
 sys.exit(main())
