"""Probe what NSE will serve us for NEW data classes: delivery%, results calendar,
bulk/block deals. Establishes a browser-like session first (NSE anti-bot)."""
import io, sys, time
import requests
import pandas as pd

H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def session():
    s = requests.Session()
    s.headers.update(H)
    try:
        s.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)
        s.get("https://www.nseindia.com/all-reports", timeout=15)
    except Exception as e:
        print("  warmup err:", e)
    return s


def probe_delivery(s):
    # sec_bhavdata_full — daily, has DELIV_QTY / DELIV_PER
    for url in [
        "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_05May2026.csv",
        "https://archives.nseindia.com/products/content/sec_bhavdata_full_05May2026.csv",
    ]:
        try:
            r = s.get(url, timeout=20)
            print(f"  delivery {url[-40:]}: {r.status_code} len={len(r.content)}")
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.StringIO(r.text))
                df.columns = [c.strip() for c in df.columns]
                print("    cols:", list(df.columns)[:20])
                print("    sample:", df.head(2)[[c for c in df.columns if 'SYMBOL' in c or 'DELIV' in c or 'CLOSE' in c]].to_dict('records'))
                return True
        except Exception as e:
            print("  delivery err:", e)
    return False


def probe_results(s):
    for url in [
        "https://www.nseindia.com/api/event-calendar",
        "https://www.nseindia.com/api/corporate-announcements?index=equities",
    ]:
        try:
            r = s.get(url, timeout=20, headers={"Referer": "https://www.nseindia.com/"})
            print(f"  results {url[-45:]}: {r.status_code} len={len(r.content)}")
            if r.status_code == 200 and len(r.content) > 100:
                j = r.json()
                print("    type:", type(j), "n:", len(j) if hasattr(j, '__len__') else '?')
                print("    sample:", (j[:1] if isinstance(j, list) else str(j)[:300]))
                return True
        except Exception as e:
            print("  results err:", e)
    return False


def probe_bulk(s):
    for url in [
        "https://nsearchives.nseindia.com/content/equities/bulk.csv",
        "https://www.nseindia.com/api/historical/bulk-deals?from=01-05-2026&to=07-05-2026",
    ]:
        try:
            r = s.get(url, timeout=20, headers={"Referer": "https://www.nseindia.com/"})
            print(f"  bulk {url[-40:]}: {r.status_code} len={len(r.content)}")
            if r.status_code == 200 and len(r.content) > 200:
                print("    head:", r.text[:200].replace("\n", " | "))
                return True
        except Exception as e:
            print("  bulk err:", e)
    return False


if __name__ == "__main__":
    s = session()
    print("DELIVERY:"); d = probe_delivery(s)
    print("RESULTS CALENDAR:"); probe_results(s)
    print("BULK DEALS:"); probe_bulk(s)
    print("\nverdict: delivery", "OK" if d else "FAIL")
