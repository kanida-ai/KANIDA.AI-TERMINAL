"""Compute the signals for every captured 15-minute mark that has none yet.

D1's capture writes `snapshots`; D2's metrics turn those into the numbers the Derivative tab reads. Nothing
ran them between the two, which is why the tab sat on yesterday's mark. This walks the gap oldest-first and
then follows the capture loop, one mark at a time. Read of `snapshots`, write of `metrics` only.
"""
import logging, sqlite3, sys, time
from datetime import datetime, timedelta, timezone
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


IST = timezone(timedelta(hours=5, minutes=30))
#: HOW FAR BACK A READING IS STILL RECOVERABLE, and why it is this number.
#:
#: The window used to be 24 hours, measured on the machine's LOCAL date. Two
#: things were wrong with that and both cost a day.
#:
#:  1. `captured_at` is stamped in IST and the host runs on US Pacific time, so
#:     "local yesterday" and "IST yesterday" are different days for half of
#:     every 24 hours. The comparison was a string comparison against a date
#:     with no time on it, which papered over it by accident rather than by
#:     design.
#:  2. Anything that fell outside the window was never picked up AT ALL. A
#:     reading captured while this loop was down stayed uncomputed for good:
#:     17 Sep 2026 had to be recomputed by hand for exactly this reason.
#:
#: So the window is now the RAW RETENTION window, in IST. Everything the store
#: still holds raw rows for is recoverable and gets picked up on its own;
#: nothing older is, because `snapshots` no longer has the rows to compute it
#: from, and scanning for it would be a search for something that cannot be
#: found. Newest first, so a live session is never queued behind a backlog.
LOOKBACK_DAYS = 30
BATCH = 40


def pending(limit=BATCH):
    """Every 15-min reading the store has raw rows for and no metrics, newest first."""
    cutoff = (datetime.now(IST) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        rows = c.execute(
            "SELECT DISTINCT s.captured_at FROM snapshots s"
            " WHERE s.captured_at >= ?"
            "   AND NOT EXISTS (SELECT 1 FROM metrics m WHERE m.captured_at = s.captured_at)"
            " ORDER BY s.captured_at DESC LIMIT ?", (cutoff, limit)).fetchall()
    finally:
        c.close()
    return [r[0] for r in rows]


LOG.info("metrics loop starting; lookback=%d days, batch=%d, db=%s", LOOKBACK_DAYS, BATCH, DB)
#: How many consecutive idle passes have been folded into one log line. A loop
#: that says "nothing pending" sixty times an hour buries the lines that matter;
#: a loop that says nothing at all cannot be told from a dead one. So it speaks
#: on the first idle pass and then once every QUIET passes.
QUIET = 15
idle = 0
while True:
    try:
        todo = pending()
        for stamp in todo:
            at = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
            # A reading from an earlier day is a CATCH-UP, not the live edge, and
            # says so: a backlog being worked off silently looks the same in a log
            # as a loop that is merely keeping up.
            behind = (datetime.now(IST).date() - at.date()).days
            conn = M.connect(DB)
            try:
                res = M.compute_for_mark(conn, at)
                LOG.info("%s -> %s rows%s", stamp, getattr(res, "written", res),
                         f" (catch-up, {behind} day(s) back)" if behind else "")
            finally:
                conn.close()
        if todo:
            idle = 0
        else:
            if idle % QUIET == 0:
                LOG.info("nothing pending; the metrics loop is alive")
            idle += 1
    except Exception as error:  # a bad mark must never stop the loop
        idle = 0
        LOG.warning("mark failed: %s", error)
    time.sleep(60)
