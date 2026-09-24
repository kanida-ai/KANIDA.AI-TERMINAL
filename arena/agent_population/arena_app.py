"""Kanida.AI — Trading Arena (local). Character-named AI agents, curated leaderboards, live picks, evolution.
IP-safe: pattern rules are NEVER shown. Run: streamlit run arena/agent_population/arena_app.py"""
import os, sqlite3, sys, json
import numpy as np, pandas as pd
import streamlit as st
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from replay_engine import run_replay
    _REPLAY_OK = True
except Exception as _re:
    _REPLAY_OK = False; _REPLAY_ERR = str(_re)
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "arena_metrics.db")
CAP = 5e5
TGT = {"hit_10pc_20d": "+10% in 20d", "hit_15pc_20d": "+15% in 20d", "hit_25pc_30d": "+25% in 30d", "hit_40pc_40d": "+40% in 40d"}
VIBE = {"TANK": ("🛡️", "Tank", "steady & durable"), "SNIPER": ("🎯", "Sniper", "precise, high-accuracy"),
        "HEAVY": ("🐋", "Heavyweight", "rare, big wins"), "SPRINT": ("⚡", "Sprinter", "fast & aggressive"),
        "WORK": ("🐜", "Workhorse", "high-frequency grinder"), "BAL": ("◆", "All-rounder", "balanced")}
REGE = {"breakout": "breakout", "capitulation": "capitulation rebound", "mean_reversion": "mean-reversion",
        "momentum": "momentum", "compression": "coiled breakout", "other": "multi-factor"}
# trader personas (hold horizon) — the leaderboard changes per persona
PERSONA = {"INTRADAY": ("Intraday", "1D", "enters and exits within the session"),
           "BTST": ("BTST", "2D", "buys today, sells tomorrow"),
           "WEEKLY": ("Weekly", "1W", "holds about a week"),
           "SWING": ("Swing", "2–3W", "holds a few weeks"),
           "MONTHLY": ("Monthly", "1M+", "holds a month or more")}
HOLDLBL = {"INTRADAY": "1 session", "BTST": "2 days", "WEEKLY": "~1 week", "SWING": "~3 weeks", "MONTHLY": "~5 weeks"}
# --- KANIDA.AI brand mark (compass, matches frontend/components/power/CompassLogo.tsx) ---
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FAV = os.path.join(_ROOT, "frontend", "app", "favicon.ico")
def COMPASS(sz=34, mr=12):
    return (f"<svg viewBox='0 0 32 32' width='{sz}' height='{sz}' style='vertical-align:middle;margin-right:{mr}px;flex:none'>"
            "<circle cx='16' cy='16' r='14' fill='none' stroke='rgba(63,227,164,.85)' stroke-width='1.5'/>"
            "<g stroke='rgba(63,227,164,.55)' stroke-width='1'>"
            "<line x1='16' y1='2' x2='16' y2='4.5'/><line x1='16' y1='27.5' x2='16' y2='30'/>"
            "<line x1='2' y1='16' x2='4.5' y2='16'/><line x1='27.5' y1='16' x2='30' y2='16'/></g>"
            "<g class='kcn'><polygon points='16,4 13.5,16 18.5,16' fill='#3FE3A4'/>"
            "<polygon points='16,28 13.5,16 18.5,16' fill='rgba(63,227,164,.35)'/></g>"
            "<circle cx='16' cy='16' r='1.5' fill='#0a0a0a' stroke='#3FE3A4' stroke-width='.8'/></svg>")
# KANIDA.AI wordmark: KANIDA (near-white) + .AI (brand green) — matches the nav
WORDMARK = "<span style='font-weight:900;letter-spacing:.5px'><span style='color:#f4f8ff'>KANIDA</span><span style='color:#3FE3A4'>.AI</span></span>"
st.set_page_config(page_title="KANIDA.AI — Trading Arena", layout="wide",
                   page_icon=(FAV if os.path.exists(FAV) else "🧭"))

st.markdown("""<style>
.stApp{background:radial-gradient(1200px 600px at 20% -5%,#12213b 0%,#0a1220 55%);}
h1,h2,h3,h4,h5,h6{color:#f4f8ff !important;font-family:'Segoe UI',system-ui,sans-serif;letter-spacing:.2px;font-weight:800;}
p,label,li,span,div,td,th{color:#dfe8f5;}
[data-testid="stCaptionContainer"],[data-testid="stCaptionContainer"] *{color:#aebcd0 !important;}
.stTabs [data-baseweb="tab"]{color:#cdd8e8 !important;font-weight:600;font-size:15px;}
.stTabs [aria-selected="true"]{color:#2dd4bf !important;}
[data-testid="stSidebar"]{background:#0a1424;border-right:1px solid #1c2c47;}
.hero{background:linear-gradient(120deg,#12304a,#0d1c30 75%);border:1px solid #2dd4bf3d;border-radius:18px;padding:22px 28px;margin-bottom:16px;box-shadow:0 0 44px #2dd4bf14;}
.hero h1{font-size:36px;font-weight:900;color:#ffffff !important;text-shadow:0 0 24px #2dd4bf40;}
.tag{color:#c3d2e6 !important;font-size:13.5px;}
.kpi{background:#0e1a2b;border:1px solid #1a2942;border-radius:14px;padding:14px 16px;transition:.2s;}
.kpi:hover{border-color:#2dd4bf55;box-shadow:0 6px 22px #2dd4bf12;}
.kpi .l{font-size:10.5px;color:#8aa0bc;text-transform:uppercase;letter-spacing:.06em;}
.kpi .v{font-size:24px;font-weight:900;color:#2dd4bf;font-family:'Consolas',monospace;margin-top:3px;}
.card{background:linear-gradient(180deg,#0f1929,#0c1422);border:1px solid #182842;border-radius:16px;padding:16px 18px;transition:.2s;}
.card:hover{border-color:#2dd4bf55;box-shadow:0 6px 30px #2dd4bf12;transform:translateY(-2px);}
.cn{font-size:21px;font-weight:800;color:#eaf2fb;}
.vibe{font-size:12px;color:#aebcd0;margin:2px 0 10px;}
.gr{float:right;font-size:12px;font-weight:800;padding:2px 11px;border-radius:20px;}
.gS{background:#2dd4bf22;color:#2dd4bf;border:1px solid #2dd4bf55;} .gA{background:#38bdf822;color:#7dd3fc;border:1px solid #38bdf855;}
.gB{background:#fbbf2422;color:#fcd34d;border:1px solid #fbbf2455;} .gC{background:#64748b22;color:#94a3b8;border:1px solid #64748b55;}
.row{display:flex;justify-content:space-between;font-size:13px;padding:4px 0;border-bottom:1px solid #14203400;}
.row .k{color:#aebcd0;} .p{color:#34d399;font-family:Consolas,monospace;font-weight:700;} .n{color:#fb7185;font-family:Consolas,monospace;font-weight:700;}
.tile{background:#0c1524;border:1px solid #182842;border-radius:12px;padding:12px 14px;}
.tile .tv{font-size:21px;font-weight:800;font-family:Consolas,monospace;} .tile .tl{font-size:9.5px;color:#aebcd0;text-transform:uppercase;letter-spacing:.06em;}
.pos{color:#34d399;} .neg{color:#fb7185;} .neu{color:#eaf2fb;} .acc{color:#2dd4bf;}
.pick{display:inline-block;background:#12233a;border:1px solid #223a55;border-radius:8px;padding:3px 10px;margin:3px 4px 0 0;font-size:12.5px;color:#bfe9ff;font-family:Consolas,monospace;}
/* fund-style podium — clean, subtle, mint */
.fcard{background:linear-gradient(180deg,#0f1c30,#0b1524);border:1px solid #1e2f4b;border-radius:20px;padding:20px 20px 18px;min-height:412px;display:flex;flex-direction:column;box-shadow:0 4px 22px #00000033;transition:.22s;}
.fcard:hover{border-color:#2dd4bf55;box-shadow:0 12px 44px #2dd4bf1c;transform:translateY(-3px);}
.fcard.champ{background:linear-gradient(180deg,#102c3a,#0c1f30);border:1.5px solid #2dd4bf;min-height:452px;box-shadow:0 0 0 1px #2dd4bf22,0 16px 54px #2dd4bf24;}
.ftop{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}
.rkbadge{font-size:11px;font-weight:800;letter-spacing:.05em;color:#8aa0bc;text-transform:uppercase;}
.fcard.champ .rkbadge{color:#2dd4bf;}
.slotpill{font-size:11px;font-weight:700;color:#bfeee6;background:#2dd4bf1a;border:1px solid #2dd4bf3a;border-radius:20px;padding:3px 11px;white-space:nowrap;}
.slotpill.full{color:#fda4af;background:#fb71851a;border-color:#fb71853a;}
.ftag{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.04em;color:#9fb8d4;background:#16233a;border:1px solid #26385480;border-radius:6px;padding:2px 9px;margin-bottom:9px;text-transform:uppercase;}
.fname{font-size:25px;font-weight:900;color:#f2f7fd;line-height:1.08;}
.fsub{font-size:11.5px;color:#8aa0bc;margin:4px 0 0;}
.investbox{background:#0a1826;border:1px solid #1c3348;border-radius:13px;padding:11px 14px;margin:14px 0 2px;}
.investbox .il{font-size:10px;color:#8aa0bc;letter-spacing:.02em;} .investbox .iv{font-size:23px;font-weight:900;color:#2dd4bf;font-family:Consolas,'SF Mono',monospace;letter-spacing:-.5px;}
.investbox .iv small{font-size:11px;color:#7fb9ad;font-weight:700;margin-left:6px;}
.fmet{display:grid;grid-template-columns:1fr 1fr;gap:13px 12px;margin:14px 0 6px;}
.fmet .mk{font-size:9.5px;color:#8aa0bc;text-transform:uppercase;letter-spacing:.05em;}
.fmet .mv{font-size:17px;font-weight:800;font-family:Consolas,'SF Mono',monospace;color:#e6eefb;margin-top:2px;}
.fmet .mv.pos{color:#34d399;} .fmet .mv.neg{color:#fb7185;}
.frent{margin-top:auto;padding-top:15px;border-top:1px solid #16273f;display:flex;align-items:center;justify-content:space-between;gap:10px;}
.frent .fp{font-size:17px;font-weight:900;color:#eaf2fb;font-family:Consolas,monospace;} .frent .fp small{font-size:10px;color:#8aa0bc;font-weight:600;}
.fbtn{font-size:12.5px;font-weight:800;border-radius:10px;padding:8px 16px;white-space:nowrap;}
.fbtn.go{background:#2dd4bf;color:#052a24;} .fbtn.wl{background:transparent;color:#fcd34d;border:1px solid #fbbf2455;}
/* next-ranked rows */
.nrow{display:flex;align-items:center;justify-content:space-between;background:#0e1a2b;border:1px solid #1a2942;border-radius:13px;padding:13px 17px;transition:.15s;}
.nrow:hover{border-color:#2dd4bf44;box-shadow:0 4px 18px #2dd4bf10;}
.nrow .nl{font-size:15px;font-weight:800;color:#eaf2fb;} .nrow .nl .rn{color:#8aa0bc;margin-right:7px;font-weight:800;}
.nrow .nmeta{font-size:11.5px;color:#8ea3bd;margin-top:3px;}
.nrow .nret{font-size:13px;font-weight:800;font-family:Consolas,monospace;padding:4px 10px;border-radius:9px;white-space:nowrap;}
.nret.pos{color:#34d399;background:#34d39914;} .nret.neg{color:#fb7185;background:#fb718514;}
.nrow .np{font-size:12px;color:#8aa0bc;margin-top:3px;} .nrow .np b{color:#2dd4bf;}
.brand{display:flex;align-items:center;gap:2px;}
.kcn{transform-box:fill-box;transform-origin:center;animation:kspin 8s ease-in-out infinite;}
.brand:hover .kcn{animation-play-state:paused;}
@keyframes kspin{0%{transform:rotate(0)}50%{transform:rotate(24deg)}100%{transform:rotate(0)}}
@media (prefers-reduced-motion:reduce){.kcn{animation:none;}}
</style>""", unsafe_allow_html=True)

@st.cache_data
def load():
    c = sqlite3.connect(DB)
    s = pd.read_sql_query("SELECT * FROM agent_summary", c); m = pd.read_sql_query("SELECT * FROM agent_monthly", c)
    def g(sr):
        if sr.cagr_comp > 80 and sr.maxdd < 20 and sr.profit_factor > 3: return "S"
        if sr.cagr_comp > 40 and sr.profit_factor > 2: return "A"
        if sr.cagr_comp > 15: return "B"
        return "C"
    s["grade"] = s.apply(g, axis=1)
    if "verified" not in s.columns: s["verified"]=((s.profit_factor>1.5)&(s.avg_ret>0)).astype(int)
    if "promo_status" not in s.columns: s["promo_status"]="measuring"
    try:
        now = pd.read_sql_query("SELECT agent_id,status,good_streak,trail_lift FROM agent_status_now", c)
        rt = pd.read_sql_query("SELECT * FROM roster_timeline", c); hist = pd.read_sql_query("SELECT * FROM agent_status_history", c)
        sig = pd.read_sql_query("SELECT * FROM agent_signals", c)
    except Exception:
        now = pd.DataFrame(columns=["agent_id", "status", "good_streak", "trail_lift"]); rt = hist = sig = pd.DataFrame()
    try:
        au = pd.read_sql_query("SELECT * FROM agent_universe", c); usym = pd.read_sql_query("SELECT * FROM universe_symbols", c)
    except Exception:
        au = pd.DataFrame(); usym = pd.DataFrame()
    try:
        ap = pd.read_sql_query("SELECT * FROM agent_persona", c)
    except Exception:
        ap = pd.DataFrame()
    try:
        val = pd.read_sql_query("SELECT * FROM validation_summary", c).iloc[0].to_dict()
    except Exception:
        val = {}
    try:
        man = pd.read_sql_query("SELECT * FROM run_manifest", c)
    except Exception:
        man = pd.DataFrame()
    c.close()
    s = s.merge(now, on="agent_id", how="left"); s["status"] = s.status.fillna("incubating")
    return s, m, rt, hist, sig, au, usym, ap, val, man
S, MO, RT, HIST, SIG, AU, USYM, AP_ALL, VAL, MAN = load()
def behavior(a):  # IP-safe: describe WHAT it does, never the rule
    em, nm, desc = VIBE.get(a.vibe, ("◆", "Agent", "")); return f"{em} {nm} · aims for {TGT.get(a.target,'')} on {REGE.get(a.regime,a.regime)} setups"

st.markdown(f"""<div class='hero'><h1 style='margin:0'><span class='brand'>{COMPASS(38,14)}<span>{WORDMARK}<span style='color:#8aa0bc;font-weight:700'> · Agents Trading Arena</span></span></span></h1>
<span class='tag'>Live rankings of {len(S)} AI trading agents — pick your style, rent the ones that fit. ₹5L modelled book · net of costs · Jan 2025 → Jul 2026</span></div>""", unsafe_allow_html=True)

S_FULL = S  # full population (roster/lifecycle is universe-independent)
sb = st.sidebar; sb.markdown(f"<div class='brand' style='font-size:17px;font-weight:800;color:#f4f8ff;margin-bottom:6px'>{COMPASS(22,8)}<span>Live roster</span></div>", unsafe_allow_html=True)
if not S_FULL.status.empty:
    for k, lab, c in [("active", "🟢 Active", "#34d399"), ("incubating", "🌱 Incubating", "#fbbf24"), ("probation", "⚠ Probation", "#fb7185"), ("retired", "✖ Retired", "#64748b")]:
        sb.markdown(f"<div style='color:{c};font-size:14px'>{lab}: <b>{int((S_FULL.status==k).sum())}</b></div>", unsafe_allow_html=True)

# --- Universe filter: re-score each agent on Nifty 50 / F&O (Nifty 200) / Nifty 500 only ---
sb.markdown("---"); sb.markdown("### 🌐 Universe")
uni_label = sb.radio("Universe", ["All stocks", "Nifty 50", "F&O (Nifty 200)", "Nifty 500"], index=0,
                     help="Ranks each agent using ONLY its trades on that universe — surfaces the agents that work best there (e.g. on F&O stocks). All figures net of 0.15%/trade.")
UKEY = {"All stocks": None, "Nifty 50": "N50", "F&O (Nifty 200)": "FO", "Nifty 500": "N500"}[uni_label]
USET = None
if UKEY and not AU.empty:
    au = AU[AU.universe == UKEY].set_index("agent_id")
    cols = [c for c in ["trades", "win_pct", "avg_win", "avg_loss", "profit_factor", "avg_ret", "cagr_comp", "maxdd", "final_5L_comp", "verified", "grade"] if c in au.columns]
    Su = S[S.agent_id.isin(au.index)].copy()
    for c in cols: Su[c] = Su.agent_id.map(au[c])
    S = Su[Su.trades >= 8].copy()
    if not USYM.empty:
        _uc = {"N50": "n50", "FO": "fo", "N500": "n500"}[UKEY]; USET = set(USYM[USYM[_uc] == 1].symbol)

vibes = sb.multiselect("Character", [VIBE[v][1] for v in VIBE], [VIBE[v][1] for v in VIBE])
vmap = {VIBE[v][1]: v for v in VIBE}; vsel = [vmap[x] for x in vibes]
sb.markdown(f"<div style='color:#2dd4bf;font-size:14px;font-weight:700'>✅ Verified & Sellable: <b>{int((S.verified==1).sum())}</b>{'' if not UKEY else ' · '+uni_label}</div>", unsafe_allow_html=True)
sb.markdown("---"); sb.markdown("### Filters")
verified_only = sb.checkbox("✅ Verified & Sellable only", False)
show_active = sb.checkbox("Active agents only", True)
F0 = S[S.vibe.isin(vsel)]
if show_active: F0 = F0[F0.status == "active"]
if verified_only: F0 = F0[F0.verified == 1]

t1, t2, t3, t4 = st.tabs(["🏆  Rankings & Rent", "🔬  Agent Deep-Dive", "🧬  Evolution", "🔁  Live Replay (Proof)"])

def _tag(a):
    if a.maxdd < 12: return "Low Drawdown"
    if a.win_pct >= 80: return "High Win-Rate"
    if a.ret_12m >= 150: return "Big Compounder"
    if a.trades_per_month >= 60: return "High Frequency"
    if a.profit_factor >= 4: return "Elite Edge"
    return VIBE.get(a.vibe, ("", "All-Rounder", ""))[1]

def _seed_rental(P):
    dem = (1 - (P.board_rank - 1) / 40.0).clip(0, 1)
    P["rented"] = (dem * 10).round().clip(0, 10).astype(int)
    P["free"] = (10 - P.rented).clip(lower=0).astype(int)
    P["waitlist"] = np.where(P.rented >= 10, ((dem - 0.75) * 40).round().clip(lower=0), 0).astype(int)
    P["rent_per_day"] = (100 * (1 + 1.4 * dem) * (1 + 0.05 * P.waitlist) / 10).round().astype(int) * 10
    return P

# --- Ranking period: the board, the champion and the rent all re-rank on the chosen metric ---
RANKBY = {
 "30 Days":       ("ret_30d",    "30D Return",   lambda v: f"{v:+.1f}%",  "~1-month model return"),
 "12 Months":     ("ret_12m",    "12M Return",   lambda v: f"{v:+.0f}%",  "annualised model return"),
 "Risk Adjusted": ("r_dd",       "Return ÷ DD",  lambda v: f"{v:.1f}",    "return per unit of drawdown"),
 "Per-signal Edge": ("expectancy","Avg / Trade", lambda v: f"{v:+.2f}%",  "net expectancy per signal (most robust)"),
}

def fund_card(a, hh, persona, rank_label, champion=False):
    free = int(a.free); wait = int(a.waitlist); rented = int(a.rented)
    slotcls = "full" if free == 0 else ""
    slottxt = f"{rented} / 10 slots" if free > 0 else f"Full · {wait} waiting"
    rk = "🏆 #1 Champion" if champion else f"#{int(a.board_rank)}"
    sub = f"{REGE.get(a.regime, a.regime).capitalize()} · exit at close after {int(a.hold_days)}d"
    r30 = "pos" if a.ret_30d >= 0 else "neg"; r12 = "pos" if a.ret_12m >= 0 else "neg"
    _, mlab, mfmt, _d = RANKBY[rank_label]
    box = (f"<div class='investbox'><div class='il'>Ranked by {mlab} · net of costs</div>"
           f"<div class='iv'>{mfmt(a[RANKBY[rank_label][0]])}<small>{a.expectancy:+.2f}%/trade · {a.win_pct:.0f}% win</small></div></div>") if champion else ""
    btn = (f"<span class='fbtn go'>Rent{' Champion' if champion else ''}</span>" if free > 0
           else "<span class='fbtn wl'>Join waitlist</span>")
    def tile(lab, val, cls=""):
        hot = " style='color:#2dd4bf'" if lab == mlab else ""
        return f"<div><div class='mk'>{lab}{' ●' if lab==mlab else ''}</div><div class='mv {cls}'{hot}>{val}</div></div>"
    return f"""<div class='fcard {'champ' if champion else ''}'>
<div class='ftop'><span class='rkbadge'>{rk}</span><span class='slotpill {slotcls}'>{slottxt}</span></div>
<span class='ftag'>{_tag(a)}</span>
<div class='fname'>{a.codename}{' ✅' if a.verified else ''}</div>
<div class='fsub'>{sub}</div>
{box}
<div class='fmet'>
{tile("30D Return", f"{a.ret_30d:+.1f}%", r30)}
{tile("12M Return", f"{a.ret_12m:+.0f}%", r12)}
{tile("Win Rate", f"{a.win_pct:.0f}%")}
{tile("Max DD", f"-{a.maxdd:.0f}%", "neg")}
{tile("Avg / Trade", f"{a.expectancy:+.2f}%", "pos")}
{tile("Return ÷ DD", f"{a.r_dd:.1f}")}
{tile("Trades / Month", f"{a.trades_per_month:.0f}")}
{tile("Avg Hold", hh)}
</div>
<div class='frent'><span class='fp'>₹{int(a.rent_per_day)}<small>/day</small></span>{btn}</div></div>"""

with t1:
    if AP_ALL.empty:
        st.info("Run build_persona_metrics.py to populate persona rankings.")
    else:
        st.markdown("<div style='font-size:12px;font-weight:700;letter-spacing:.08em;color:#2dd4bf;text-transform:uppercase'>Live Agent Rankings</div>"
                    "<div style='font-size:26px;font-weight:900;color:#f2f7fd;margin:0 0 10px'>Winner Podium</div>", unsafe_allow_html=True)
        _pc, _rc = st.columns([3, 1])
        with _pc:
            persona = st.radio("Persona", list(PERSONA), index=list(PERSONA).index("SWING"), horizontal=True,
                               label_visibility="collapsed", format_func=lambda p: f"{PERSONA[p][0]} · {PERSONA[p][1]}")
        with _rc:
            rank_label = st.selectbox("Ranking period", list(RANKBY), index=0,
                                      help="Re-ranks the board, the champion AND the rent on this metric.")
        rank_col, rank_metric_lab, rank_fmt, rank_desc = RANKBY[rank_label]
        ukey = UKEY or "ALL"
        P = AP_ALL[(AP_ALL.persona == persona) & (AP_ALL.universe == ukey)].merge(
            S_FULL[["agent_id", "codename", "vibe", "regime", "target", "status", "verified"]], on="agent_id", how="left")
        P = P[P.codename.notna()]; P = P[P.vibe.isin(vsel)]
        if verified_only: P = P[P.verified == 1]
        if show_active: P = P[P.status == "active"]
        scored = len(P)
        P = P.copy(); P["r_dd"] = P.ret_12m / P.maxdd.clip(lower=3)      # risk-adjusted: return per unit of drawdown
        # PROFITABLE gate, then rank on the SELECTED ranking period
        Pt = P[P.tradeable == 1].sort_values(rank_col, ascending=False).reset_index(drop=True)
        if Pt.empty:
            st.info(f"🚫 **No agent clears costs at the {PERSONA[persona][0]} hold for {uni_label}.** "
                    f"Of {scored} agents scored, none has positive net expectancy at this holding period — so there is *no tradeable champion* for this style. "
                    "Try a longer hold (Swing / Monthly) or a different universe. *(This is the honest answer, not an empty board.)*")
        else:
            Pt["board_rank"] = Pt.index + 1; Pt = _seed_rental(Pt)
            champ = Pt.iloc[0]; hh = HOLDLBL[persona]
            sc = st.columns(4)
            for col, lab, val in zip(sc, ["Agents Scored", "Profitable (tradeable)", f"Best {rank_metric_lab}", "Champion Rent"],
                                     [f"{scored}", f"{len(Pt)}", rank_fmt(champ[rank_col]), f"₹{int(champ.rent_per_day)}/day"]):
                col.markdown(f"<div class='kpi'><div class='l'>{lab}</div><div class='v'>{val}</div></div>", unsafe_allow_html=True)
            st.caption(f"**{PERSONA[persona][0]}** style — {PERSONA[persona][2]} · **{uni_label}** · ranked by **{rank_metric_lab}** ({rank_desc}) · Top 10 of the profitable set. "
                       "**True out-of-sample** (each agent scored only *after* its mining year) · net 0.15%/trade · profitable-gated. "
                       "Headline = **per-signal expectancy** (robust); *12M · model* is a noisier single-account figure. "
                       "🔒 Slots & waitlist are an **illustrative demand seed** until live bookings.")
            st.write("")
            top3 = Pt.head(3); order = [1, 0, 2] if len(top3) >= 3 else list(range(len(top3)))
            cols = st.columns([1, 1.2, 1] if len(top3) >= 3 else [1]*max(len(top3), 1))
            for col, oi in zip(cols, order):
                a = top3.iloc[oi]
                col.markdown(fund_card(a, hh, persona, rank_label, champion=(a.board_rank == 1)), unsafe_allow_html=True)
            st.write(""); st.markdown("<div style='font-size:15px;font-weight:800;color:#cdd8e8;margin:6px 0 4px'>Next Ranked · 4–10</div>", unsafe_allow_html=True)
            nxt = Pt.iloc[3:10]
            for rr in range(0, len(nxt), 2):
                ncols = st.columns(2)
                for ncol, (_, a) in zip(ncols, nxt.iloc[rr:rr+2].iterrows()):
                    free = int(a.free); avail = f"{free} slots free" if free > 0 else f"full · {int(a.waitlist)} waiting"
                    ncol.markdown(f"""<div class='nrow'><div>
<div class='nl'><span class='rn'>#{int(a.board_rank)}</span>{a.codename}{' ✅' if a.verified else ''}</div>
<div class='nmeta'>{a.win_pct:.0f}% win · {int(a.trades_per_month)} tr/mo · {hh} hold</div>
<div class='np'>₹<b>{int(a.rent_per_day)}</b>/day · {avail}</div></div>
<span class='nret {"pos" if a[rank_col]>=0 else "neg"}'>{rank_fmt(a[rank_col])}</span></div>""", unsafe_allow_html=True)
                st.write("")
            with st.expander(f"📋 Ranking table (Top 10 profitable · sorted by {rank_metric_lab})"):
                tt = Pt.head(10)[["board_rank", "codename", "ret_30d", "ret_12m", "r_dd", "expectancy", "win_pct", "maxdd", "profit_factor", "trades_per_month", "rented", "waitlist", "rent_per_day"]].rename(
                    columns={"board_rank": "#", "codename": "Agent", "ret_30d": "30D%", "ret_12m": "12M%", "r_dd": "Ret÷DD", "expectancy": "Avg/Tr%",
                             "win_pct": "Win%", "maxdd": "MaxDD%", "profit_factor": "PF", "trades_per_month": "Tr/mo",
                             "rented": "Rented", "waitlist": "Wait", "rent_per_day": "₹/day"})
                st.dataframe(tt, use_container_width=True, hide_index=True, column_config={
                    "Avg/Tr%": st.column_config.NumberColumn(format="%+.2f"), "Win%": st.column_config.NumberColumn(format="%.0f"),
                    "30D%": st.column_config.NumberColumn(format="%+.1f"), "Ret÷DD": st.column_config.NumberColumn(format="%.1f"),
                    "12M%": st.column_config.NumberColumn(format="%+.0f"), "MaxDD%": st.column_config.NumberColumn(format="%.0f"),
                    "PF": st.column_config.NumberColumn(format="%.2f"), "Rented": st.column_config.NumberColumn(format="%d/10"),
                    "₹/day": st.column_config.NumberColumn(format="₹%d")})
        if VAL:
            with st.expander("🔬 How much can you trust this leaderboard? — statistical validation"):
                v = VAL
                g1, g2, g3 = st.columns(3)
                g1.markdown(f"<div class='kpi'><div class='l'>Probability of Backtest Overfitting</div><div class='v' style='color:{'#34d399' if v.get('pbo',1)<0.5 else '#fb7185'}'>{v.get('pbo','—')}</div></div>", unsafe_allow_html=True)
                g2.markdown(f"<div class='kpi'><div class='l'>Deflated Sharpe (best agent)</div><div class='v' style='color:{'#34d399' if v.get('deflated_sharpe_ratio',0)>0.95 else '#fcd34d'}'>{v.get('deflated_sharpe_ratio','—')}</div></div>", unsafe_allow_html=True)
                g3.markdown(f"<div class='kpi'><div class='l'>Trials tested (multiple-testing)</div><div class='v'>{v.get('trials_N','—')}</div></div>", unsafe_allow_html=True)
                st.markdown(
                    f"- **PBO = {v.get('pbo')}** over {v.get('pbo_combos')} combinatorial splits → **{v.get('pbo_verdict')}**. An in-sample winner stays out-of-sample above median ~{100-int(float(v.get('pbo',0))*100)}% of the time — the *ensemble selection edge is real*.\n"
                    f"- **Deflated Sharpe = {v.get('deflated_sharpe_ratio')}** → **{v.get('dsr_verdict')}**. Across {v.get('trials_N')} agents over only {v.get('months_T')} OOS months, the single best agent (Sharpe {v.get('best_sharpe_ann')}) is *not* provably better than the luckiest of {v.get('trials_N')} random trials (~{v.get('expected_max_sharpe_ann_under_null')}).\n"
                    f"- **Honest read:** trust the *book/ensemble*, not any one 'champion' as statistically bulletproof on this short a track. Rent a basket, and let the **Live Replay** + a longer forward record earn single-agent confidence.")
                st.caption("Method: Deflated Sharpe Ratio & PBO/CSCV (Bailey & López de Prado). Recompute with validation_rigor.py.")

with t2:
    pick = st.selectbox("Choose an agent", F0.sort_values("cagr_comp", ascending=False).codename.tolist() if len(F0) else S.codename.tolist())
    a = S[S.codename == pick].iloc[0]
    stt = {"active": "🟢 Active", "incubating": "🌱 Incubating", "probation": "⚠ Probation", "retired": "✖ Retired"}.get(a.status, a.status)
    # --- EXIT MODEL: every number below declares the rule that produced it ---
    _tgt = TGT.get(a.target, a.target)
    MODELS = {f"Native target — {_tgt} (as mined)": None,
              **{f"{PERSONA[p][0]} — plain exit after {PERSONA[p][1]}": p for p in PERSONA}}
    mc1, mc2 = st.columns([2, 3])
    with mc1:
        model_lbl = st.selectbox("Exit model", list(MODELS), index=0,
                                 help="The SAME agent scored under different exit rules. The podium uses the persona holds; "
                                      "this defaults to the agent's native mined target. Numbers differ because the exit differs.")
    mkey = MODELS[model_lbl]
    _pr = None
    if mkey is not None and not AP_ALL.empty:
        _q = AP_ALL[(AP_ALL.agent_id == a.agent_id) & (AP_ALL.persona == mkey) & (AP_ALL.universe == (UKEY or "ALL"))]
        _pr = _q.iloc[0] if len(_q) else None
    if mkey is not None and _pr is None:
        st.warning(f"No {model_lbl.split(' —')[0]} data for this agent in **{uni_label}** — showing its native target model instead.")
        mkey = None
    if mkey is None:
        MV = dict(win=a.win_pct, aw=a.avg_win, al=a.avg_loss, pf=a.profit_factor, tr=a.trades,
                  ann=a.cagr_comp, avg=a.avg_ret, dd=a.maxdd, fin=a.final_5L_comp,
                  rule=f"buy next open · WIN if {_tgt.replace('in','within')} · else exit at horizon close")
    else:
        MV = dict(win=_pr.win_pct, aw=_pr.avg_win, al=_pr.avg_loss, pf=_pr.profit_factor, tr=_pr.trades,
                  ann=_pr.ret_12m, avg=_pr.expectancy, dd=_pr.maxdd, fin=_pr.final_5L,
                  rule=f"buy next open · plain exit at close after {int(_pr.hold_days)} trading days · no target")
    st.markdown(f"<span class='gr g{a.grade}'>Grade {a.grade}</span><h2 style='margin:0'>{a.codename}{' ✅' if a.verified else ''}</h2><div class='vibe'>{behavior(a)} · {stt}</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='background:#0c1c2c;border:1px solid #1e3550;border-left:3px solid #2dd4bf;border-radius:9px;padding:8px 13px;margin:8px 0'>"
                f"<span style='font-size:10.5px;color:#8aa0bc;text-transform:uppercase;letter-spacing:.06em'>Exit model in force</span><br>"
                f"<span style='color:#dbe6f5;font-size:13px'><b>{model_lbl}</b> — {MV['rule']}</span></div>", unsafe_allow_html=True)
    def tile(col, lab, val, cls="neu"): col.markdown(f"<div class='tile'><div class='tv {cls}'>{val}</div><div class='tl'>{lab}</div></div>", unsafe_allow_html=True)
    r1 = st.columns(5)
    tile(r1[0], "Win Rate", f"{MV['win']:.0f}%", "pos"); tile(r1[1], "Avg Win", f"{MV['aw']:+.2f}%", "pos"); tile(r1[2], "Avg Loss", f"{MV['al']:+.2f}%", "neg")
    tile(r1[3], "Profit Factor", f"{MV['pf']:.1f}", "acc"); tile(r1[4], "Trades", f"{int(MV['tr'])}", "neu"); st.write("")
    r2 = st.columns(5)
    tile(r2[0], "Ann. Return (₹5L acct)", f"{MV['ann']:+.0f}%", "pos" if MV['ann'] >= 0 else "neg")
    tile(r2[1], "Avg / Trade", f"{MV['avg']:+.2f}%", "pos" if MV['avg'] >= 0 else "neg")
    tile(r2[2], "Clean OOS Lift", f"{a.clean26_lift:+.0f}pp", "pos" if a.clean26_lift >= 0 else "neg")
    tile(r2[3], "Max Drawdown", f"{MV['dd']:.0f}%", "neg"); tile(r2[4], "₹5L → (comp)", f"₹{MV['fin']:,.0f}", "pos" if MV['fin'] >= CAP else "neg")
    st.caption("Metrics are LEAK-FREE (weekly-feature agents fire only week-end), **true out-of-sample** & net of 0.15%/trade. "
               "Robust reads: **Win%, Profit Factor, Avg/Trade**. The ₹5L CAGR is a single-account figure — noisier. "
               "⚠️ **Switch the exit model above to compare like-for-like with the podium** — a 15-day exit and a 30-day target are different trades.")
    if not AP_ALL.empty:
        _all = AP_ALL[(AP_ALL.agent_id == a.agent_id) & (AP_ALL.universe == (UKEY or "ALL"))]
        if len(_all):
            with st.expander("🔀 Same agent, every exit model — side by side (this is why numbers differ between screens)"):
                _t = _all[["persona", "hold_days", "trades", "win_pct", "expectancy", "profit_factor", "maxdd", "ret_12m"]].copy()
                _t["persona"] = _t.persona.map(lambda p: PERSONA[p][0]); _t = _t.sort_values("hold_days")
                _t.loc[len(_t)] = ["Native target", None, a.trades, a.win_pct, a.avg_ret, a.profit_factor, a.maxdd, a.cagr_comp]
                st.dataframe(_t.rename(columns={"persona": "Exit model", "hold_days": "Hold (d)", "trades": "Trades", "win_pct": "Win%",
                                                "expectancy": "Avg/Tr%", "profit_factor": "PF", "maxdd": "MaxDD%", "ret_12m": "Ann.Ret%"}),
                             use_container_width=True, hide_index=True, column_config={
                                 "Win%": st.column_config.NumberColumn(format="%.0f"), "Avg/Tr%": st.column_config.NumberColumn(format="%+.2f"),
                                 "PF": st.column_config.NumberColumn(format="%.2f"), "MaxDD%": st.column_config.NumberColumn(format="%.0f"),
                                 "Ann.Ret%": st.column_config.NumberColumn(format="%+.0f")})
                st.caption("The longer the hold, the closer it gets to the native target model — that convergence is the consistency check.")
    cL, cR = st.columns([1, 1])
    with cL:
        st.markdown("##### 📍 Stocks it's signalling")
        if not SIG.empty:
            dts = sorted(SIG[SIG.agent_id == a.agent_id].date.unique(), reverse=True)
            if dts:
                dsel = st.selectbox("Session", dts, format_func=lambda x: f"{x}  (latest)" if x == dts[0] else x)
                picks = sorted(SIG[(SIG.agent_id == a.agent_id) & (SIG.date == dsel)].symbol.unique())
                if USET is not None: picks = [p for p in picks if p in USET]
                st.markdown(" ".join(f"<span class='pick'>{p}</span>" for p in picks) or "_no signal that day_", unsafe_allow_html=True)
                st.caption(f"{len(picks)} stock(s) on {dsel}{' · '+uni_label if UKEY else ''} — buy next open per this agent's setup.")
            else: st.info("No signals in the last 20 sessions.")
    with cR:
        st.markdown("##### 📅 Month-by-month")
        mm = MO[MO.agent_id == a.agent_id].copy().rename(columns={"ym": "Month", "trades": "Trades", "wins": "Win", "losses": "Loss", "avg_win": "Avg Win", "avg_loss": "Avg Loss", "avg_ret": "Avg/Trade%"})
        mm["Win%"] = (mm.Win / mm.Trades * 100).round(0)
        st.dataframe(mm[["Month", "Trades", "Win", "Loss", "Win%", "Avg Win", "Avg Loss", "Avg/Trade%"]], use_container_width=True, hide_index=True, height=340,
            column_config={"Avg Win": st.column_config.NumberColumn(format="%+.2f"), "Avg Loss": st.column_config.NumberColumn(format="%+.2f"),
                           "Avg/Trade%": st.column_config.NumberColumn(format="%+.2f"), "Win%": st.column_config.NumberColumn(format="%.0f")})

with t3:
    if RT.empty: st.info("Run incubation_engine.py to populate evolution.")
    else:
        st.markdown("#### The arena is alive — agents are bred, tested, and *earn* their place")
        st.caption("Every month the population re-scores each agent on a trailing out-of-sample window and moves it through a lifecycle. "
                   "🟢 Active = live book · 🌱 Incubating & ⚠ Probation = proving on paper · ✖ Retired = edge decayed. Self-learning runs monthly from **Jan 2026**.")
        RTl = RT[RT.ym >= "2026-01"] if (RT.ym >= "2026-01").any() else RT
        last = RT.iloc[-1]; rc = st.columns(4)
        for col, key, lab, cls in zip(rc, ["active", "incubating", "probation", "retired"], ["🟢 Active (live)", "🌱 Incubating", "⚠ Probation", "✖ Retired"], ["pos", "neu", "neg", "neg"]):
            tile(col, lab, int(last[key]), cls)
        st.write(""); st.markdown("##### Evolution of the live book (Jan 2026 →)")
        st.area_chart(RTl.set_index("ym")[["retired", "probation", "incubating", "active"]], color=["#64748b", "#fb7185", "#fbbf24", "#34d399"], height=280)
        cA, cB = st.columns(2)
        with cA:
            st.markdown("##### 🌱 Incubation — earning their way up")
            inc = S_FULL[S_FULL.status.isin(["incubating", "probation"])].copy(); inc["→graduate"] = inc.good_streak.fillna(0).astype(int).astype(str) + "/3"
            st.dataframe(inc.sort_values(["good_streak", "trail_lift"], ascending=False)[["codename", "status", "→graduate", "trail_lift"]].head(14).rename(columns={"codename": "Agent", "status": "Stage", "trail_lift": "Form"}),
                         use_container_width=True, hide_index=True, height=320, column_config={"Form": st.column_config.NumberColumn(format="%+.1f")})
        with cB:
            st.markdown("##### This month")
            mths = sorted(HIST.ym.unique())
            if len(mths) >= 2:
                cur = HIST[HIST.ym == mths[-1]][["agent_id", "codename", "status"]]; prev = HIST[HIST.ym == mths[-2]][["agent_id", "status"]].rename(columns={"status": "prev"})
                ch = cur.merge(prev, on="agent_id"); ch = ch[ch.status != ch.prev]
                gr = ch[ch.status == "active"].codename.head(8).tolist(); rt2 = ch[ch.status == "retired"].codename.head(8).tolist()
                st.markdown(f"🟢 **{len(ch[ch.status=='active'])} graduated** to the live book" + (f"<br><span class='acc'>{', '.join(gr)}</span>" if gr else ""), unsafe_allow_html=True)
                st.markdown(f"✖ **{len(ch[ch.status=='retired'])} retired**" + (f"<br><span class='n'>{', '.join(rt2)}</span>" if rt2 else ""), unsafe_allow_html=True)
            st.markdown("**Next:** the mutation gate breeds *new* agents from the strongest — offspring incubate here before going live.")

@st.cache_data(show_spinner=False)
def cached_replay(agent_id, start, end, universe, hold_days):
    return run_replay(agent_id, start, end, universe=universe, hold_days=hold_days, use_1min=True)

with t4:
    st.markdown("<div style='font-size:12px;font-weight:700;letter-spacing:.08em;color:#2dd4bf;text-transform:uppercase'>Trust Layer</div>"
                "<div style='font-size:24px;font-weight:900;color:#f2f7fd;margin:0 0 2px'>Live Point-in-Time Replay</div>", unsafe_allow_html=True)
    st.caption("Watch one agent ‘trade live’ on history and **prove it never sees the future**: signal at Day-T close → execute Day-T+1 open → "
               "1-minute paper trade with rolling capital sleeves → data-derived journal → leakage PASS/FAIL. Every journal line is computed from that day’s numbers — no narration.")
    if not _REPLAY_OK:
        st.error(f"Replay engine unavailable: {_REPLAY_ERR}")
    else:
        cc = st.columns([2, 1, 1, 1])
        names = S_FULL.sort_values("codename").codename.tolist()
        dflt = "Accura" if "Accura" in names else names[0]
        pick = cc[0].selectbox("Agent", names, index=names.index(dflt))
        uni_r = cc[1].selectbox("Universe", ["ALL", "N50", "FO", "N500"], index=2,
                                format_func=lambda x: {"ALL": "All", "N50": "Nifty 50", "FO": "F&O", "N500": "Nifty 500"}[x])
        hold_r = cc[2].selectbox("Hold (days)", [1, 2, 5, 15, 25], index=3)
        c2 = st.columns([1, 1, 2])
        start_r = c2[0].text_input("Start", "2026-03-01"); end_r = c2[1].text_input("End", "2026-05-31")
        go = c2[2].button("▶ Run replay", type="primary", use_container_width=True)
        aid = S_FULL[S_FULL.codename == pick].agent_id.iloc[0]
        if go: st.session_state["replay_key"] = (aid, start_r, end_r, uni_r, int(hold_r))
        key = st.session_state.get("replay_key")
        if not key:
            st.info("Pick an agent, date range and hold, then **Run replay**. Tip: start with a Swing agent (e.g. Accura) over ~2–3 months.")
        else:
            try:
                with st.spinner("Replaying day-by-day (point-in-time)…"):
                    R = cached_replay(*key)
            except Exception as e:
                st.error(f"Replay failed: {e}"); R = None
            if R:
                s = R["summary"]
                badge = "✅ LEAKAGE AUDIT — ALL DAYS PASS" if s["leakage_all_pass"] else "❌ LEAKAGE DETECTED"
                bg = "#0f2c24" if s["leakage_all_pass"] else "#3a1620"; bc = "#2dd4bf66" if s["leakage_all_pass"] else "#fb718566"
                st.markdown(f"<div style='background:{bg};border:1px solid {bc};border-radius:12px;padding:11px 16px;font-weight:800;color:#8fe9d4;margin:8px 0'>"
                            f"{badge}<span style='font-weight:500;color:#9fb0c6'> &nbsp;·&nbsp; every signal used data ≤ Day-T close · every trade executed Day-T+1 open</span></div>", unsafe_allow_html=True)
                m = st.columns(5)
                for col, lab, val in zip(m, ["Final ₹", "Total Return", "Win Rate", "Max DD", "Closed Trades"],
                                         [f"₹{s['final_equity']:,.0f}", f"{s['total_return_pct']:+.1f}%", f"{s['win_rate']:.0f}%", f"{s['max_dd_pct']:.1f}%", f"{s['closed_trades']}"]):
                    col.markdown(f"<div class='kpi'><div class='l'>{lab}</div><div class='v'>{val}</div></div>", unsafe_allow_html=True)
                st.write("")
                eq = R["equity"]
                if len(eq): st.markdown("##### Equity curve"); st.line_chart(eq.set_index("date")["equity"], height=200, color="#2dd4bf")
                days = R["days"]; dates = [d["date"] for d in days]
                st.markdown("##### Step through any session")
                di = st.slider("Session", 0, len(dates)-1, len(dates)-1) if len(dates) > 1 else 0
                d = days[di]; st.markdown(f"### {d['date']}  <span style='font-size:13px;color:#8aa0bc'>· session {di+1} of {len(dates)}</span>", unsafe_allow_html=True)
                pc = st.columns(2)
                with pc[0]:
                    lk = d["leakage"]
                    st.markdown(f"<div class='tile'><b style='color:#7fe8cf'>🔒 Leakage audit · {lk['status']}</b>"
                                f"<div style='font-size:12px;color:#9fb0c6;margin-top:6px;line-height:1.7'>Data cutoff: <b style='color:#dbe6f5'>{lk['cutoff']} close</b><br>"
                                f"Signal formed: {lk['signal_ts']}<br>Trade executes: <b style='color:#dbe6f5'>{lk['exec_ts']}</b><br>"
                                f"Fields used (≤T): {', '.join(lk['available'][:6])}<br>Blocked until T+1: {', '.join(lk['blocked'])}</div></div>", unsafe_allow_html=True)
                    st.markdown(f"**Signal → tomorrow’s candidates ({d['n_signals']}):** " + ("".join(f"<span class='pick'>{x}</span>" for x in d['signals'][:12]) or "_none_"), unsafe_allow_html=True)
                    st.markdown(f"**Executed today ({len(d['executed'])}):** " + ("".join(f"<span class='pick'>{e['sym']}</span>" for e in d['executed']) or "_none_"), unsafe_allow_html=True)
                    st.markdown(f"**Closed today ({len(d['closed'])}):** " + (", ".join(f"{c['sym']} {c['ret_pct']:+.1f}%" for c in d['closed']) or "_none_"))
                    if d['basket_intraday_dd']: st.caption(f"1-min basket intraday drawdown: **{d['basket_intraday_dd']}%**")
                with pc[1]:
                    jr = d["journal"]
                    for sec, ic in [("summary", "📋 Today’s Summary"), ("reflection", "🔍 Reflection"), ("learnings", "🎓 Learnings")]:
                        rows = "".join(f"<div class='row'><span class='k'>{k}</span><span style='color:#dbe6f5;text-align:right'>{v}</span></div>" for k, v in jr[sec].items())
                        st.markdown(f"<div class='tile' style='margin-bottom:8px'><b>{ic}</b>{rows}</div>", unsafe_allow_html=True)
                exp = [{"date": x["date"], "equity": round(x["equity"], 2), "cash": round(x["cash"], 2), "realized": round(x["realized"], 2),
                        "drawdown": round(x["drawdown"], 2), "n_signals": x["n_signals"], "executed": len(x["executed"]),
                        "closed": len(x["closed"]), "basket_intraday_dd": x["basket_intraday_dd"], "leakage": x["leakage"]["status"]} for x in days]
                st.download_button("⬇ Export replay log (JSON) — proof of point-in-time behaviour",
                                   data=json.dumps({"summary": s, "days": exp}, indent=2, default=str),
                                   file_name=f"replay_{s['agent']}_{s['start']}_{s['end']}.json", mime="application/json")

# ---- data provenance footer: every number self-identifies its method ----
st.write("")
try:
    from run_manifest import METHOD_VERSION, LEAK_FIX, OOS_RULE, COST_PCT, WINDOW
except Exception:
    METHOD_VERSION = LEAK_FIX = OOS_RULE = WINDOW = "?"; COST_PCT = 0.15
if not MAN.empty:
    _stale = int((MAN.method_version != METHOD_VERSION).sum())
    _ok = "✅ all tables on the current standard" if _stale == 0 else f"⚠️ {_stale} table(s) built on an OLDER method — rebuild them"
    with st.expander(f"🧾 Data provenance — method v{METHOD_VERSION} · {_ok}"):
        st.markdown(f"**Every number in this console was produced under one recorded standard.** "
                    f"If a figure ever disagrees with an older one, this tells you exactly why.")
        st.markdown(f"- **Leakage rule:** {LEAK_FIX}")
        st.markdown(f"- **Out-of-sample rule:** {OOS_RULE}")
        st.markdown(f"- **Costs / window:** {COST_PCT}%/trade · {WINDOW}")
        st.markdown(f"- **Price data through:** {MAN.data_snapshot.max()}")
        _m = MAN.copy(); _m["status"] = np.where(_m.method_version != METHOD_VERSION, "STALE", "current")
        st.dataframe(_m[["table_name", "script", "method_version", "rows", "data_snapshot", "built_at_ist", "status"]]
                     .sort_values("table_name").rename(columns={
                         "table_name": "Table", "script": "Built by", "method_version": "Method",
                         "rows": "Rows", "data_snapshot": "Data through", "built_at_ist": "Built (IST)", "status": "Status"}),
                     use_container_width=True, hide_index=True, height=330)
        st.caption("Bump METHOD_VERSION in run_manifest.py whenever the methodology changes — every table built "
                   "under an older method then flags itself as STALE here.")
