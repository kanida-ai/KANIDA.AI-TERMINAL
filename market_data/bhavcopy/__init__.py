"""NSE F&O daily bhavcopy archive: resumable downloader + loader.

Public NSE end-of-day files, one per trading day, mapped from two source
formats into one table in ``db/fo_bhavcopy.db``:

* ``old``   - ``fo{DD}{MON}{YYYY}bhav.csv.zip`` (used up to 2024-07-05)
* ``udiff`` - ``BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip`` (from 2024-07-08)

Units were verified against real files on 2024-07-05, a day published in both
formats: ``contracts`` is the number of contracts (old CONTRACTS == UDiFF
TtlTradgVol) and ``oi`` / ``oi_change`` are in shares (old OPEN_INT ==
UDiFF OpnIntrst). Futures rows are normalised to ``strike = 0`` and
``option_type = 'XX'`` (the old-format convention) so the primary key never
holds NULLs.

Nothing is ever fabricated: a day with no file is a ``fetch_log`` row with
its reason and contributes zero rows to ``fo_daily``.

Run: ``python -m market_data.bhavcopy.cli --help``
"""

from market_data.bhavcopy.load import (  # noqa: F401
    DEFAULT_DB,
    connect,
    expiries,
    load_day,
    members,
    parse_zip,
    record_log,
)
