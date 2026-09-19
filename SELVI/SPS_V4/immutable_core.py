"""
IMMUTABLE INTEGRITY CORE  —  the agent MUST NOT modify this file.
=================================================================
Enforces point-in-time features, the sealed 2026 vault, the leak-free admission
gauntlet, and the cost model. Self-modification (capability_layer.py) feeds INTO
this; it can never bypass it. The engine verifies this file's SEAL hash at start.

Reuses SPS_V3's data primitives (load_min honours the vault seal) read-only.
"""
import sys, hashlib
from pathlib import Path
import numpy as np

_SPS3 = Path(__file__).resolve().parents[1] / "SPS_V3"
sys.path.insert(0, str(_SPS3))
from lab import load_min, load_atr, day_pack, TRAIN, VAL, VAULT_START  # noqa: data primitives

VAULT = (VAULT_START, "2026-12-31")
COST = 0.0010          # round-trip charges + exit slippage
SLIP = 0.0003          # entry slippage
FEATURES = ["gap", "atr", "orw", "vwap_dev", "ret_open", "range_pos", "vol_ratio", "tod"]


def features(day, atr_pct, prevclose, e):
    """Point-in-time features at entry offset e — computed ONLY from bars <= e."""
    o, h, l, cf, v, vwap = day["o"], day["h"], day["l"], day["cf"], day["v"], day["vwap"]
    hi = np.nanmax(h[:e + 1]); lo = np.nanmin(l[:e + 1]); rng = (hi - lo) if hi > lo else 1e-9
    recent = v[max(0, e - 5):e]; sofar = v[:e]
    return {
        "gap": (o[0] / prevclose - 1) if prevclose else 0.0,
        "atr": atr_pct if atr_pct == atr_pct else 0.0,
        "orw": (np.nanmax(h[:15]) - np.nanmin(l[:15])) / o[0],
        "vwap_dev": (cf[e] / vwap[e] - 1) if vwap[e] else 0.0,
        "ret_open": cf[e] / o[0] - 1,
        "range_pos": (cf[e] - lo) / rng,                          # 0=at low .. 1=at high
        "vol_ratio": (recent.mean() / sofar.mean()) if (e > 6 and sofar.mean() > 0) else 1.0,
        "tod": float(e),
    }


def orb_late(day, k=30, wstart=60, cutoff=210):
    """Base trigger: late-morning opening-range breakout (the proven V3 family)."""
    o, h, l = day["o"], day["h"], day["l"]; orh = np.nanmax(h[:k]); orl = np.nanmin(l[:k])
    if np.isnan(orh):
        return None
    for t in range(max(k, wstart), min(cutoff, day["last"] + 1)):
        if np.isnan(h[t]):
            continue
        up = h[t] >= orh; dn = l[t] <= orl
        if up and dn:
            continue
        if up:
            return (t, "long", orh)
        if dn:
            return (t, "short", orl)
    return None


def simulate(days, atrmap, filt, target, stop):
    """Run base trigger + an agent-authored filter over a day-set. Leak-free."""
    rets = []
    for d, day in days.items():
        atr_pct, prevclose = atrmap.get(d, (np.nan, np.nan))
        if prevclose is None or (isinstance(prevclose, float) and np.isnan(prevclose)):
            continue
        r = orb_late(day)
        if r is None:
            continue
        e, side, level = r
        f = features(day, atr_pct, prevclose, e)
        if not filt(f, side):
            continue
        entry = level * (1 + SLIP) if side == "long" else level * (1 - SLIP)
        tgt = entry * (1 + target) if side == "long" else entry * (1 - target)
        stp = (entry * (1 - stop) if side == "long" else entry * (1 + stop)) if stop else None
        h_, l_, cf, last = day["h"], day["l"], day["cf"], day["last"]
        exitp = None
        for t in range(e + 1, last + 1):
            if np.isnan(h_[t]):
                continue
            if side == "long":
                if stp is not None and l_[t] <= stp:
                    exitp = stp; break
                if h_[t] >= tgt:
                    exitp = tgt; break
            else:
                if stp is not None and h_[t] >= stp:
                    exitp = stp; break
                if l_[t] <= tgt:
                    exitp = tgt; break
        if exitp is None:
            exitp = cf[last]
        g = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
        rets.append(g - COST)
    a = np.array(rets)
    if len(a) == 0:
        return None
    return {"trades": int(len(a)), "net": float(a.mean()), "win": float((a > 0).mean()),
            "tstat": float(a.mean() / (a.std() + 1e-12) * np.sqrt(len(a)))}


def gauntlet(sym, filt, target, stop):
    """The admission gauntlet (LOCKED): train t>=2.5 AND val t>=2.5, net>0, min trades."""
    atrmap = load_atr(sym)
    tr = day_pack(load_min(sym, *TRAIN)); va = day_pack(load_min(sym, *VAL))
    rtr = simulate(tr, atrmap, filt, target, stop); rva = simulate(va, atrmap, filt, target, stop)
    if not rtr or not rva:
        return False, rtr, rva
    admit = (rtr["trades"] >= 100 and rva["trades"] >= 40 and rtr["net"] > 0 and rva["net"] > 0
             and rtr["tstat"] >= 2.5 and rva["tstat"] >= 2.5)
    return admit, rtr, rva


def vault_confirm(sym, filt, target, stop):
    """The one authorised break of the 2026 seal — honest final test of a champion."""
    atrmap = load_atr(sym)
    v = day_pack(load_min(sym, *VAULT, unlock=True))
    return simulate(v, atrmap, filt, target, stop)


SEAL = hashlib.md5(Path(__file__).read_bytes()).hexdigest()
