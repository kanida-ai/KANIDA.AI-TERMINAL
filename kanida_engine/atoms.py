"""
KANIDA unified engine — ATOMS (the speed core).
Each atom updates in O(1) per 1-min bar (no history rescans) and exposes a point-in-time value.
The SAME atom code runs in the mining plane (over historical bars) and the serving plane (over the
live stream) => backtest == live (parity). Intraday atoms reset at the day boundary; persistent
baselines (volume/range EWMA) do not. Pure-Python + __slots__ for low per-bar overhead; the live
batch path (state.py) will vectorise these across all stocks via struct-of-arrays.

No look-ahead by construction: an atom only ever sees bars up to 'now'.
"""


class RunningATP:
    """Day VWAP so far = running average traded price (resets daily)."""
    __slots__ = ("cum_tv", "cum_v", "_v")

    def __init__(self): self.reset_day()
    def update(self, o, h, l, c, v):
        typ = (h + l + c) / 3.0
        self.cum_tv += typ * v; self.cum_v += v
        self._v = self.cum_tv / self.cum_v if self.cum_v > 0 else typ
    def value(self): return self._v
    def reset_day(self): self.cum_tv = 0.0; self.cum_v = 0.0; self._v = 0.0


class DayState:
    """Open, running high/low, cum volume, return-from-open, range-to-time (resets daily)."""
    __slots__ = ("open", "hi", "lo", "cum_v", "last_c")

    def __init__(self): self.reset_day()
    def update(self, o, h, l, c, v):
        if self.open is None: self.open = o; self.hi = h; self.lo = l
        if h > self.hi: self.hi = h
        if l < self.lo: self.lo = l
        self.cum_v += v; self.last_c = c
    def ret_from_open(self): return (self.last_c / self.open - 1) * 100 if self.open else 0.0
    def range_pct(self): return (self.hi - self.lo) / self.open * 100 if self.open else 0.0
    def reset_day(self): self.open = None; self.hi = -1e18; self.lo = 1e18; self.cum_v = 0.0; self.last_c = 0.0


class Footprints:
    """ONE EXAMPLE feature family (not the target). Accumulation-footprint counters from 1-min OHLCV
    (illustrative use-cases 1 & 2). These are just a few convenience features inside a much larger
    NEUTRAL basis (see NeutralAgg); the miner discovers arbitrary per-stock behaviours over the whole
    basis and picks its OWN thresholds — nothing here is privileged or hand-tuned as 'the answer'.
      iceberg  = high volume + near-zero range         (volume WITHOUT volatility -> silent absorption)
      absorb   = high DOWN volume that HOLDS (closes upper half of a down bar) -> forced down but absorbed
      hivol    = any unusually high-volume bar
    'High volume' / 'tiny range' are judged vs PERSISTENT EWMA baselines (not reset daily). Counters
    reset daily; their end-of-day totals become next-day macro features (the micro<->macro bridge).
    In production the baselines are seeded from the per-minute-of-day historical profile for accuracy.
    """
    __slots__ = ("va", "ra", "hi_mult", "lo_mult", "vol_bl", "rng_bl", "init",
                 "n_hivol", "n_iceberg", "n_absorb")

    def __init__(self, vol_alpha=0.02, rng_alpha=0.02, hi_mult=2.0, lo_mult=0.5):
        self.va = vol_alpha; self.ra = rng_alpha; self.hi_mult = hi_mult; self.lo_mult = lo_mult
        self.vol_bl = 0.0; self.rng_bl = 0.0; self.init = False
        self.n_hivol = 0; self.n_iceberg = 0; self.n_absorb = 0

    def update(self, o, h, l, c, v):
        rng = h - l
        rng_pct = rng / c * 100 if c > 0 else 0.0
        if not self.init:
            self.vol_bl = v if v > 0 else 1.0; self.rng_bl = rng_pct if rng_pct > 0 else 0.1; self.init = True
        hivol = v >= self.hi_mult * self.vol_bl
        if hivol:
            self.n_hivol += 1
            if rng_pct <= self.lo_mult * self.rng_bl:
                self.n_iceberg += 1                                   # volume without volatility
            close_loc = (c - l) / rng if rng > 0 else 0.5
            if c < o and close_loc >= 0.5:
                self.n_absorb += 1                                    # down-volume that held
        # update baselines AFTER classifying (current bar judged vs PRIOR baseline)
        self.vol_bl = self.va * v + (1 - self.va) * self.vol_bl
        self.rng_bl = self.ra * rng_pct + (1 - self.ra) * self.rng_bl

    def reset_day(self):                                              # baselines persist across days
        self.n_hivol = 0; self.n_iceberg = 0; self.n_absorb = 0


class NeutralAgg:
    """NEUTRAL (unopinionated) intraday aggregates — raw vocabulary for the miner to combine. NOT tied
    to any hypothesis; the miner learns which of these (and what thresholds) matter, per stock. This is
    where generality lives — expand this freely; nothing here presumes an outcome."""
    __slots__ = ("up_vol", "dn_vol", "sum_absret", "n_bars", "max_vol_z", "atp_cross", "_side")

    def __init__(self): self.reset_day()
    def update(self, o, h, l, c, v, atp, vol_bl):
        self.n_bars += 1
        if c >= o: self.up_vol += v
        else: self.dn_vol += v
        self.sum_absret += abs(c / o - 1) * 100 if o > 0 else 0.0
        vz = v / vol_bl if vol_bl > 0 else 1.0
        if vz > self.max_vol_z: self.max_vol_z = vz
        side = 1 if c >= atp else -1
        if self._side != 0 and side != self._side: self.atp_cross += 1
        self._side = side
    def reset_day(self):
        self.up_vol = 0.0; self.dn_vol = 0.0; self.sum_absret = 0.0; self.n_bars = 0
        self.max_vol_z = 0.0; self.atp_cross = 0; self._side = 0


class VolumeProfile:
    """Intraday volume-by-price. POC = price with most traded volume; VA(H/L) = 70% value area.
    Institution-standard. Buckets are relative (~0.05% wide); POC/VA computed on demand (O(buckets))."""
    __slots__ = ("buckets", "tick", "ref")

    def __init__(self): self.reset_day()
    def reset_day(self): self.buckets = {}; self.tick = 0.0; self.ref = 0.0
    def update(self, o, h, l, c, v):
        typ = (h + l + c) / 3.0
        if self.tick == 0.0: self.ref = typ; self.tick = max(typ * 0.0005, 0.01)
        b = int((typ - self.ref) / self.tick)
        self.buckets[b] = self.buckets.get(b, 0.0) + v
    def poc(self):
        if not self.buckets: return self.ref
        return self.ref + max(self.buckets.items(), key=lambda kv: kv[1])[0] * self.tick
    def value_area(self):
        if not self.buckets: return (self.ref, self.ref)
        total = sum(self.buckets.values()); target = 0.7 * total
        pb = max(self.buckets.items(), key=lambda kv: kv[1])[0]; acc = self.buckets[pb]; lo = hi = pb
        while acc < target and hi - lo < 400:
            up = self.buckets.get(hi + 1, 0.0); dn = self.buckets.get(lo - 1, 0.0)
            if up >= dn: hi += 1; acc += up
            else: lo -= 1; acc += dn
        return (self.ref + lo * self.tick, self.ref + hi * self.tick)      # (VAL, VAH)


class CVD:
    """Cumulative volume delta proxy = running signed volume from close-location in the bar (buy vs
    sell pressure). Order-flow PROXY (no true tick), resets daily."""
    __slots__ = ("cvd",)
    def __init__(self): self.cvd = 0.0
    def reset_day(self): self.cvd = 0.0
    def update(self, o, h, l, c, v):
        rng = h - l; cl = (c - l) / rng if rng > 0 else 0.5
        self.cvd += v * (2 * cl - 1)
    def value(self): return self.cvd


class VolCompression:
    """Realized-vol compression: fast-EWMA variance / slow-EWMA variance of 1-min returns.
    <1 = volatility contracting (squeeze), >1 = expanding. O(1). (Your 'vol contracts 50%' case.)"""
    __slots__ = ("af", "asl", "vf", "vs", "prev", "init")
    def __init__(self, fast=1 / 30.0, slow=1 / 180.0): self.af = fast; self.asl = slow; self.reset_day()
    def reset_day(self): self.vf = 0.0; self.vs = 0.0; self.prev = 0.0; self.init = False
    def update(self, o, h, l, c, v):
        if self.prev > 0:
            r = c / self.prev - 1; r2 = r * r
            if not self.init: self.vf = r2; self.vs = r2; self.init = True
            else:
                self.vf = self.af * r2 + (1 - self.af) * self.vf
                self.vs = self.asl * r2 + (1 - self.asl) * self.vs
        self.prev = c
    def value(self): return (self.vf / self.vs) if self.vs > 0 else 1.0


class VWAPBand:
    """Price position in std-dev bands around VWAP/ATP (running EWMA variance of deviation). O(1)."""
    __slots__ = ("a", "ewvar", "init")
    def __init__(self, alpha=1 / 60.0): self.a = alpha; self.reset_day()
    def reset_day(self): self.ewvar = 0.0; self.init = False
    def update_dev(self, dev):
        d2 = dev * dev
        self.ewvar = d2 if not self.init else self.a * d2 + (1 - self.a) * self.ewvar
        self.init = True
    def zpos(self, dev):
        sd = self.ewvar ** 0.5
        return dev / sd if sd > 0 else 0.0


class Pullback:
    """Intraday pullback/liquidity structure: deepest pullback from the running high, and a
    'recovered' flag (reclaimed near the high after a dip). Lightweight sweep/absorption proxy."""
    __slots__ = ("rh", "deepest", "recovered")
    def __init__(self): self.reset_day()
    def reset_day(self): self.rh = -1e18; self.deepest = 0.0; self.recovered = 0
    def update(self, o, h, l, c, v):
        if h > self.rh: self.rh = h
        if self.rh > 0:
            dd = (c - self.rh) / self.rh * 100
            if dd < self.deepest: self.deepest = dd
            if self.deepest < -0.3 and (self.rh - c) / self.rh < 0.002: self.recovered = 1
    def value(self): return self.deepest


class StockState:
    """Bundles the atoms for one stock. Live per-minute cost = O(#atoms). Backtest feeds historical
    bars through the identical path. feature_vector() returns the point-in-time (to-time) NEUTRAL basis
    the miner mines over — the accumulation examples are just a few entries, given no special weight."""
    __slots__ = ("atp", "day", "foot", "neu", "vp", "cvd", "volc", "band", "pb", "prev_atp")

    def __init__(self):
        self.atp = RunningATP(); self.day = DayState(); self.foot = Footprints(); self.neu = NeutralAgg()
        self.vp = VolumeProfile(); self.cvd = CVD(); self.volc = VolCompression()
        self.band = VWAPBand(); self.pb = Pullback(); self.prev_atp = 0.0

    def update(self, o, h, l, c, v):
        self.prev_atp = self.atp.value()
        vol_bl = self.foot.vol_bl if self.foot.init else (v if v > 0 else 1.0)
        self.atp.update(o, h, l, c, v); self.day.update(o, h, l, c, v); self.foot.update(o, h, l, c, v)
        atp = self.atp.value()
        self.neu.update(o, h, l, c, v, atp, vol_bl)
        self.vp.update(o, h, l, c, v); self.cvd.update(o, h, l, c, v); self.volc.update(o, h, l, c, v)
        self.band.update_dev(c - atp); self.pb.update(o, h, l, c, v)

    def reset_day(self):
        for a in (self.atp, self.day, self.foot, self.neu, self.vp, self.cvd, self.volc, self.band, self.pb):
            a.reset_day()
        self.prev_atp = 0.0

    def feature_vector(self):
        atp = self.atp.value(); c = self.day.last_c; n = self.neu; tv = n.up_vol + n.dn_vol
        poc = self.vp.poc(); val, vah = self.vp.value_area()
        return {
            "atp": atp,
            "id_ret_from_open": self.day.ret_from_open(),
            "id_range_to_time_pct": self.day.range_pct(),
            "id_atp_dev_pct": (c / atp - 1) * 100 if atp > 0 else 0.0,
            "id_atp_push": (atp - self.prev_atp) / self.prev_atp * 100 if self.prev_atp > 0 else 0.0,
            "id_cum_vol": self.day.cum_v,
            # neutral flow basis
            "id_buy_sell_imbalance": (n.up_vol - n.dn_vol) / tv if tv > 0 else 0.0,
            "id_price_impact": n.sum_absret / (self.day.cum_v / 1e6) if self.day.cum_v > 0 else 0.0,
            "id_max_vol_z": n.max_vol_z, "id_atp_crossings": float(n.atp_cross),
            # institutional adds
            "id_dist_poc_pct": (c / poc - 1) * 100 if poc > 0 else 0.0,
            "id_in_value_area": 1.0 if (val <= c <= vah) else 0.0,
            "id_cvd": self.cvd.value(),
            "id_cvd_norm": self.cvd.value() / self.day.cum_v if self.day.cum_v > 0 else 0.0,
            "id_vol_compression": self.volc.value(),
            "id_vwap_band_z": self.band.zpos(c - atp),
            "id_deepest_pullback_pct": self.pb.deepest,
            "id_pullback_recovered": float(self.pb.recovered),
            # example footprints (not privileged)
            "id_n_hivol_bars": float(self.foot.n_hivol),
            "id_n_iceberg_bars": float(self.foot.n_iceberg),
            "id_n_absorb_bars": float(self.foot.n_absorb),
        }

    def eod_snapshot(self):
        """Completed-day totals -> daily features (T-1..T-N macro context)."""
        n = self.neu; tv = n.up_vol + n.dn_vol; poc = self.vp.poc(); c = self.day.last_c
        return {"eod_n_hivol": self.foot.n_hivol, "eod_n_iceberg": self.foot.n_iceberg,
                "eod_n_absorb": self.foot.n_absorb, "eod_ret": self.day.ret_from_open(),
                "eod_range_pct": self.day.range_pct(),
                "eod_buy_sell_imbalance": (n.up_vol - n.dn_vol) / tv if tv > 0 else 0.0,
                "eod_max_vol_z": n.max_vol_z, "eod_atp_crossings": n.atp_cross,
                "eod_cvd_norm": self.cvd.value() / self.day.cum_v if self.day.cum_v > 0 else 0.0,
                "eod_vol_compression": self.volc.value(), "eod_deepest_pullback": self.pb.deepest,
                "eod_dist_poc": (c / poc - 1) * 100 if poc > 0 else 0.0}


FEATURE_NAMES = ["atp", "id_ret_from_open", "id_range_to_time_pct", "id_atp_dev_pct", "id_atp_push",
                 "id_cum_vol", "id_buy_sell_imbalance", "id_price_impact", "id_max_vol_z",
                 "id_atp_crossings", "id_dist_poc_pct", "id_in_value_area", "id_cvd", "id_cvd_norm",
                 "id_vol_compression", "id_vwap_band_z", "id_deepest_pullback_pct",
                 "id_pullback_recovered", "id_n_hivol_bars", "id_n_iceberg_bars", "id_n_absorb_bars"]
