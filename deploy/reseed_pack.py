"""Build the cloud re-seed pack from the laptop's CURRENT live DB state.

Read-only (VACUUM INTO with ?mode=ro) so it is SAFE to run while the backend is
using the DB. Produces kanida-seed.tar.gz (the two serving DBs + a SHA256SUMS.txt
the cloud seed task verifies with `sha256sum -c`). The R&D artifacts
(falcon_serve_evidence.db / falcon_sim_patterns.db) and rupeezy_instruments.json
are seeded separately and are NOT in this pack, so untarring it preserves them.

Run (laptop):  C:\\Users\\SPS\\anaconda3\\python.exe deploy\\reseed_pack.py
Then upload the tarball to  s3://kanida-prod-artifacts-389642461326/seed/kanida-seed.tar.gz
and run the cloud seed task (deploy/PHASE2_STEP8_SEED_RUNBOOK.md / ~/reseed_efs.sh).
"""
import sqlite3, os, hashlib, tarfile, tempfile, shutil

# repo root = parent of this file's dir (deploy/); override with KANIDA_ROOT.
ROOT = os.environ.get("KANIDA_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.environ.get("SEED_OUT") or os.path.join(os.path.expanduser("~"), "Desktop", "kanida-seed.tar.gz")
dbs = ["data/db/kanida_universe.db", "data/db/kanida_quant.db"]

stage = tempfile.mkdtemp()
sums = []
try:
    for db in dbs:
        src = os.path.join(ROOT, db)
        name = os.path.basename(db)
        dst = os.path.join(stage, name)
        con = sqlite3.connect(f"file:{src}?mode=ro", uri=True)  # read-only: safe while app runs
        con.execute("VACUUM INTO ?", (dst,))
        con.close()
        h = hashlib.sha256()
        with open(dst, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        sums.append(f"{h.hexdigest()}  {name}")
        print("vacuumed", name, "->", os.path.getsize(dst), "bytes")
    with open(os.path.join(stage, "SHA256SUMS.txt"), "w", newline="\n") as fh:
        fh.write("\n".join(sums) + "\n")
    if os.path.exists(OUT):
        os.remove(OUT)
    with tarfile.open(OUT, "w:gz") as t:
        for f in sorted(os.listdir(stage)):
            t.add(os.path.join(stage, f), arcname=f)
    print("BUILT", OUT, os.path.getsize(OUT), "bytes")
    print("--- SHA256SUMS ---")
    with open(os.path.join(stage, "SHA256SUMS.txt")) as fh:
        print(fh.read())
finally:
    shutil.rmtree(stage, ignore_errors=True)
