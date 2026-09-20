"""Compute the signals for every captured 15-minute mark that has none yet.

D1's capture writes `snapshots`; D2's metrics turn those into the numbers the Derivative tab reads. Nothing
ran them between the two, which is why the tab sat on yesterday's mark. This walks the gap oldest-first and
then follows the capture loop, one mark at a time. Read of `snapshots`, write of `metrics` only.
"""
import logging, sqlite3, sys, time
from datetime import datetime
from pathlib import Path

# This lives in scripts/, so Python puts scripts/ on the path and NOT the repository root — `import
# market_data` then fails. It did, the moment this became a scheduled task. Resolve the root from this
# file rather than relying on PYTHONPATH or on the working directory, because a service is started by
# the scheduler with neither of them set the way a shell would set them.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.derivatives import metrics as M

DB = str(ROOT / "db" / "derivatives.db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(sys.argv[1] if len(sys.argv)>1 else "metrics_loop.log", encoding="utf-8"), logging.StreamHandler()])
LOG = logging.getLogger("metrics_loop")


def pending(limit=40):
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        rows = c.execute(
            "SELECT s.captured_at FROM snapshots s"
            " LEFT JOIN metrics m ON m.captured_at = s.captured_at"
            " WHERE s.captured_at >= date('now','localtime','-1 day') AND m.captured_at IS NULL"
            " GROUP BY s.captured_at ORDER BY s.captured_at LIMIT ?", (limit,)).fetchall()
    finally:
        c.close()
    return [r[0] for r in rows]


while True:
    try:
        todo = pending()
        for stamp in todo:
            at = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
            conn = M.connect(DB)
            try:
                res = M.compute_for_mark(conn, at)
                LOG.info("%s -> %s rows", stamp, getattr(res, "written", res))
            finally:
                conn.close()
        if not todo:
            LOG.info("nothing pending")
    except Exception as error:  # a bad mark must never stop the loop
        LOG.warning("mark failed: %s", error)
    time.sleep(60)
