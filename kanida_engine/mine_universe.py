"""
Scale the unified engine across the deep-history F&O universe to confirm the micro+macro forward edge
at scale. Sequential (one heavy 1-min reader at a time) so it coexists with the network-bound IPO fetch.
Stores discovered patterns to KANIDA_SNR.db.unified_patterns (append, idempotent per symbol).

Run: PYTHONIOENCODING=utf-8 python mine_universe.py   (log: db/mine_universe.log)
"""
import os, sys, json, sqlite3
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
import miner

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
TABLE = os.environ.get("UNIFIED_TABLE", "unified_patterns")   # M12b re-mine writes to unified_patterns_event
FORCE = os.environ.get("FORCE_REMINE", "0") == "1"            # re-mine ALL symbols (ignore already-done)
LOG = Path(rf"C:\Users\SPS\Documents\Kanida_Falcon\db\mine_{TABLE}.log")


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def main():
    LOG.write_text("", encoding="utf-8")
    kc = sqlite3.connect(KDB)
    fno = sorted(set(r[0] for r in kc.execute(
        "SELECT symbol FROM instrument_labels WHERE in_nifty500=1 OR is_fno=1").fetchall()))
    kc.close()
    con = sqlite3.connect(SNR, timeout=180)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE}(
        symbol TEXT, target TEXT, direction TEXT, rule_text TEXT, rule_json TEXT, depth INTEGER,
        has_micro INTEGER, n_tr INTEGER, prec_tr REAL, base_tr REAL, lift_tr REAL,
        n_va INTEGER, prec_va REAL, promoted INTEGER, n_te INTEGER, prec_te REAL, base_te REAL,
        lift_te REAL, fp_te INTEGER, status TEXT)""")
    done = set() if FORCE else set(r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {TABLE}").fetchall())
    todo = [s for s in fno if s not in done]                 # resume-friendly (skip already-mined)
    log(f"=== unified engine mine -> {TABLE} | {len(fno)} syms, {len(todo)} to do (skip {len(done)}) FORCE={FORCE} ===")
    for i, s in enumerate(todo, 1):
        try:
            frame, pats = miner.mine_stock(s)
            if not pats:
                log(f"  [{i}/{len(todo)}] {s}: insufficient/shallow — skip"); continue
            con.execute(f"DELETE FROM {TABLE} WHERE symbol=?", (s,))
            rows = []
            for p in pats:
                hm = 1 if any(c[0].startswith(("id_", "eod_")) for c in p["conds"]) else 0
                rows.append((s, p["target"], p["direction"], miner.rule_text(p["conds"]),
                             json.dumps([[a, b, c] for a, b, c in p["conds"]]), p["depth"], hm,
                             p["n_tr"], p["prec_tr"], p["base_tr"], p["lift_tr"], p["n_va"], p["prec_va"],
                             p["promoted"], p["n_te"], p["prec_te"], p["base_te"], p["lift_te"],
                             p["fp_te"], p["status"]))
            con.executemany(f"INSERT INTO {TABLE} VALUES (" + ",".join("?" * 20) + ")", rows)
            con.commit()
            keep = sum(1 for p in pats if p["status"] == "Keep")
            if i % 5 == 0 or i <= 3:
                log(f"  [{i}/{len(todo)}] {s}: mined {len(pats)} Keep {keep}")
        except Exception as e:
            log(f"  [{i}/{len(todo)}] {s}: ERROR {str(e)[:120]}")
    tot = con.execute(f"SELECT count(DISTINCT symbol), count(*), sum(status='Keep') FROM {TABLE}").fetchone()
    con.close()
    log(f"UNIVERSE DONE: {tot[0]} stocks, {tot[1]} patterns, {tot[2]} Keep. UNIFIED ENGINE UNIVERSE COMPLETE")


if __name__ == "__main__":
    main()
