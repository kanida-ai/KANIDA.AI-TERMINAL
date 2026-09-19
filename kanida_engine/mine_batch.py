"""
Engine showcase: mine a batch of deep-history stocks with the unified micro+macro engine, store the
discovered patterns to KANIDA_SNR.db.unified_patterns for the deep-dive. Sequential (one heavy 1-min
reader at a time) so it coexists safely with the network-bound IPO fetch.

Run: PYTHONIOENCODING=utf-8 python mine_batch.py   (log: db/mine_batch.log)
"""
import sys, json, sqlite3
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
import miner

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\mine_batch.log")
BATCH = ["ADANIENT", "RELIANCE", "ICICIBANK", "HDFCBANK", "INFY", "TCS", "BEL", "HAL", "MARUTI",
         "TATASTEEL", "SIEMENS", "LT", "SBIN", "TRENT", "AXISBANK"]


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def main():
    LOG.write_text("", encoding="utf-8")
    con = sqlite3.connect(SNR, timeout=120)
    con.execute("""CREATE TABLE IF NOT EXISTS unified_patterns(
        symbol TEXT, target TEXT, direction TEXT, rule_text TEXT, rule_json TEXT, depth INTEGER,
        has_micro INTEGER, n_tr INTEGER, prec_tr REAL, base_tr REAL, lift_tr REAL,
        n_va INTEGER, prec_va REAL, promoted INTEGER, n_te INTEGER, prec_te REAL, base_te REAL,
        lift_te REAL, fp_te INTEGER, status TEXT)""")
    con.execute("DELETE FROM unified_patterns WHERE symbol IN (%s)" % ",".join("?" * len(BATCH)), BATCH)
    con.commit()
    log(f"=== unified engine batch mine | {len(BATCH)} deep-history stocks ===")
    for i, s in enumerate(BATCH, 1):
        try:
            frame, pats = miner.mine_stock(s)
            if not pats:
                log(f"  [{i}/{len(BATCH)}] {s}: insufficient data"); continue
            rows = []
            for p in pats:
                hm = 1 if any(c[0].startswith(("id_", "eod_")) for c in p["conds"]) else 0
                rows.append((s, p["target"], p["direction"], miner.rule_text(p["conds"]),
                             json.dumps([[a, b, c] for a, b, c in p["conds"]]), p["depth"], hm,
                             p["n_tr"], p["prec_tr"], p["base_tr"], p["lift_tr"], p["n_va"], p["prec_va"],
                             p["promoted"], p["n_te"], p["prec_te"], p["base_te"], p["lift_te"],
                             p["fp_te"], p["status"]))
            con.executemany("INSERT INTO unified_patterns VALUES (" + ",".join("?" * 20) + ")", rows)
            con.commit()
            keep = [p for p in pats if p["status"] == "Keep"]
            km = sum(1 for p in keep if any(c[0].startswith(("id_", "eod_")) for c in p["conds"]))
            log(f"  [{i}/{len(BATCH)}] {s}: {frame.shape[0]}d x{frame.shape[1]} | mined {len(pats)} "
                f"Keep {len(keep)} (micro+macro {km}) | best2026 +{max([p['lift_te'] for p in keep], default=0)}pp")
        except Exception as e:
            log(f"  [{i}/{len(BATCH)}] {s}: ERROR {str(e)[:140]}")
    tot = con.execute("SELECT count(*), sum(status='Keep') FROM unified_patterns").fetchone()
    con.close()
    log(f"BATCH DONE: {tot[0]} patterns stored, {tot[1]} Keep. UNIFIED ENGINE BATCH COMPLETE")


if __name__ == "__main__":
    main()
