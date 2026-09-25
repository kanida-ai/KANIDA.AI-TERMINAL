"""Backup / restore drill for the strategy-builder database (GTM audit P24).

    python scripts/sb_backup.py backup  [--db var/strategy_builder.db] [--out var/backups]
    python scripts/sb_backup.py verify  <backup.db>
    python scripts/sb_backup.py restore <backup.db> --to <new.db>        (never overwrites an existing file)

Uses SQLite's online backup API, so it is safe while the pilot is running (WAL). `verify` runs integrity_check and
prints per-table row counts. A drill = backup -> restore to a NEW path -> compare counts table by table. Nothing here
touches a broker, a token or the live database in place.
"""
from __future__ import annotations
import argparse,json,os,sqlite3,sys,time
from pathlib import Path

HERE=Path(__file__).resolve().parents[1]


def counts(path):
 c=sqlite3.connect(f'file:{path}?mode=ro',uri=True)
 try:
  tabs=[r[0] for r in c.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name")]
  return {t:c.execute(f'select count(*) from "{t}"').fetchone()[0] for t in tabs}
 finally:c.close()


def integrity(path):
 c=sqlite3.connect(f'file:{path}?mode=ro',uri=True)
 try:return c.execute('pragma integrity_check').fetchone()[0]
 finally:c.close()


def backup(db,out_dir):
 db=Path(db);out_dir=Path(out_dir);out_dir.mkdir(parents=True,exist_ok=True)
 if not db.exists():raise SystemExit(f'No database at {db}')
 from datetime import datetime
 from zoneinfo import ZoneInfo
 dest=out_dir/f"{db.stem}-{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y%m%d-%H%M%S')}IST.db"
 src=sqlite3.connect(str(db));dst=sqlite3.connect(str(dest))
 try:src.backup(dst)
 finally:dst.close();src.close()
 ok=integrity(dest)
 if ok!='ok':raise SystemExit(f'Backup {dest} failed integrity_check: {ok}')
 return dest


def restore(backup_db,to):
 to=Path(to)
 if to.exists():raise SystemExit(f'{to} exists - restore never overwrites; choose a new path')
 src=sqlite3.connect(f'file:{backup_db}?mode=ro',uri=True);dst=sqlite3.connect(str(to))
 try:src.backup(dst)
 finally:dst.close();src.close()
 return to


def drill(db,out_dir):
 """backup -> restore to a new path -> every table's row count must match the backup."""
 b=backup(db,out_dir);r=restore(b,Path(out_dir)/(b.stem+'-restored.db'))
 cb,cr=counts(b),counts(r)
 return {'backup':str(b),'restored':str(r),'integrity':integrity(r),'tables':len(cb),'match':cb==cr,'counts':cb}


def main(argv=None):
 ap=argparse.ArgumentParser();sp=ap.add_subparsers(dest='cmd',required=True)
 b=sp.add_parser('backup');b.add_argument('--db',default=str(HERE/'var'/'strategy_builder.db'));b.add_argument('--out',default=str(HERE/'var'/'backups'))
 v=sp.add_parser('verify');v.add_argument('path')
 r=sp.add_parser('restore');r.add_argument('path');r.add_argument('--to',required=True)
 d=sp.add_parser('drill');d.add_argument('--db',default=str(HERE/'var'/'strategy_builder.db'));d.add_argument('--out',default=str(HERE/'var'/'backups'))
 a=ap.parse_args(argv)
 if a.cmd=='backup':print(backup(a.db,a.out))
 elif a.cmd=='verify':print(json.dumps({'integrity':integrity(a.path),'counts':counts(a.path)},indent=1))
 elif a.cmd=='restore':print(restore(a.path,a.to))
 else:print(json.dumps(drill(a.db,a.out),indent=1))


if __name__=='__main__':
 main()
