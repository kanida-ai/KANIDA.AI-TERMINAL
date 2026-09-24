"""MONTHLY GRID for the FIXED book: liquidity-scaled sizing (each name <=2% of its ADV, best-edge first, spill down),
80/20 long/short, VWAP/POV worked execution (impact x0.35). Requested columns, Jan-Jul 2026.
Capital levels: 1cr / 3cr / 10cr, plus 10cr single-shot for contrast. Also appends blocks to MONTHLY_FIXED_BOOK_GRID.xlsx.
Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily WHERE trade_date BETWEEN '2026-01-01' AND '2026-07-17'", uc); uc.close()
adv = od.assign(tv=od.close * od.volume).groupby("symbol").tv.median().to_dict()
op = od.pivot_table(index="trade_date", columns="symbol", values="open"); cp = od.pivot_table(index="trade_date", columns="symbol", values="close")
cal = sorted(od.trade_date.unique()); nextd = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date,rank,symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2025-12-31' AND '2026-07-16'", rc); rc.close()
perday = {}
for sd, g in rk.groupby("signal_date"):
    ed = nextd.get(sd)
    if not ed or ed not in op.index: continue
    rows = []
    for _, r in g.iterrows():
        s = r.symbol
        if s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s] and adv.get(s, 0) > 0:
            rows.append((int(r["rank"]), op.at[ed, s], cp.at[ed, s], adv[s]))
    rows.sort(); perday[ed] = rows

def cost_rs(tb, ts):
    return min(20, 0.0003*tb)+min(20, 0.0003*ts)+0.00025*ts+0.0000297*(tb+ts)+0.00003*tb+0.000001*(tb+ts)+0.18*(min(20,0.0003*tb)+min(20,0.0003*ts)+0.0000297*(tb+ts)+0.000001*(tb+ts))

def fill(entries, notional, maxpart, sgn, eff):
    rem = notional; g = c = s = 0.0
    for (rk_, o, cl, a) in entries:
        if rem <= 0: break
        ci = min(maxpart * a, rem); qty = ci / o; tb = o*qty; ts = cl*qty
        g += sgn*(cl-o)*qty; c += cost_rs(tb, ts); s += 2*(2 + 800*(ci/a)*eff)/1e4*ci; rem -= ci
    return g, c + s

def daily(cap, maxpart, lw, longcut, eff):
    rows = []
    for ed, entries in perday.items():
        longs = [e for e in entries if e[0] <= longcut]; shorts = [e for e in entries if e[0] >= 201]
        gL, cL = fill(longs, cap*lw*5, maxpart, 1, eff); gS, cS = fill(shorts, cap*(1-lw)*5, maxpart, -1, eff)
        G = gL + gS; C = cL + cS; rows.append((ed, G, C, G - C))
    D = pd.DataFrame(rows, columns=["d", "gross", "cost", "net"]); D["ym"] = D.d.str[:7]; return D

def monthly(D, cap):
    out = []
    for ym, g in D.groupby("ym"):
        eq = g.net.cumsum(); mdd = (np.maximum.accumulate(eq.values) - eq.values).max() / cap * 100
        out.append(dict(month=ym, groc=g.gross.sum()/cap*100, gross=g.gross.sum(), cost=g.cost.sum(),
                        net=g.net.sum(), nroc=g.net.sum()/cap*100, mdd=mdd, days=len(g), dw=(g.net>0).mean()*100))
    return pd.DataFrame(out).set_index("month")
MLBL = {"2026-01":"Jan","2026-02":"Feb","2026-03":"Mar","2026-04":"Apr","2026-05":"May","2026-06":"Jun","2026-07":"Jul"}
MON = list(MLBL)

def show(cap, ctag, eff, elbl):
    D = daily(cap, 0.02, 0.8, 100, eff); m = monthly(D, cap)
    print(f"\nFIXED book — {ctag} — 80/20, ADV-cap 2%, {elbl}")
    print(f"{'month':<6}{'Capital':>8}{'exec':>14}{'GrossROC%':>11}{'₹P&L':>16}{'Cost stack':>15}{'Net P&L':>16}{'NetROC%':>9}{'MaxDD':>7}{'days':>6}{'Daywin':>8}")
    tg=tc=tn=td=w=0
    for ym in MON:
        if ym not in m.index: continue
        x = m.loc[ym]
        print(f"{MLBL[ym]:<6}{ctag:>8}{elbl:>14}{x.groc:>+11.1f}{x.gross:>+16,.0f}{x.cost:>15,.0f}{x.net:>+16,.0f}{x.nroc:>+9.1f}{x.mdd:>6.0f}%{int(x.days):>6}{x.dw:>7.0f}%")
        tg+=x.gross; tc+=x.cost; tn+=x.net; td+=x.days; w+=(x.dw/100)*x.days
    eqA=D.net.cumsum(); tmdd=(np.maximum.accumulate(eqA.values)-eqA.values).max()/cap*100
    print(f"{'TOTAL':<6}{ctag:>8}{elbl:>14}{tg/cap*100:>+11.1f}{tg:>+16,.0f}{tc:>15,.0f}{tn:>+16,.0f}{tn/cap*100:>+9.1f}{tmdd:>6.0f}%{int(td):>6}{w/td*100:>7.0f}%")
    return m, D

print("="*130)
CFG = [(1e7,"₹1cr",0.35,"VWAP/POV"), (3e7,"₹3cr",0.35,"VWAP/POV"), (1e8,"₹10cr",0.35,"VWAP/POV"), (1e8,"₹10cr",1.0,"single-shot")]
results = [(cap, ctag, eff, elbl, *show(cap, ctag, eff, elbl)) for cap, ctag, eff, elbl in CFG]

# append to Excel
xlsx = os.path.join(ROOT, "docs", "reports", "MONTHLY_FIXED_BOOK_GRID.xlsx"); from openpyxl import Workbook as _WB; wb = _WB(); wb.remove(wb.active)
ws = wb.create_sheet("FIXED book (ADV-cap+VWAP)")
HDR = ["month","Capital deployed","execution","Gross ROC (%)","₹ P&L","Cost stack","Net P&L","Net ROC (%)","Max DD","Trading days","Day-win"]
thin=Side(style="thin",color="BBBBBB"); bd=Border(thin,thin,thin,thin); hf=PatternFill("solid",fgColor="1F4E79"); tf=PatternFill("solid",fgColor="D9E1F2")
r=1
def put(row,vals,bold=False,fill=None,white=False):
    for j,v in enumerate(vals,1):
        c=ws.cell(row=row,column=j,value=v); c.border=bd; c.alignment=Alignment(horizontal="center")
        if bold: c.font=Font(bold=True,color="FFFFFF" if white else "000000")
        if fill: c.fill=fill
for cap,ctag,eff,elbl,m,D in results:
    ws.cell(row=r,column=1,value=f"FIXED book — {ctag} — 80/20 · ADV-cap 2% · {elbl}").font=Font(bold=True,size=12); r+=1
    put(r,HDR,bold=True,fill=hf,white=True); r+=1
    tg=tc=tn=td=w=0
    for ym in MON:
        if ym not in m.index: continue
        x=m.loc[ym]; put(r,[MLBL[ym],ctag,elbl,round(x.groc,1),round(x.gross),round(x.cost),round(x.net),round(x.nroc,1),f"{x.mdd:.0f}%",int(x.days),f"{x.dw:.0f}%"])
        tg+=x.gross; tc+=x.cost; tn+=x.net; td+=x.days; w+=(x.dw/100)*x.days; r+=1
    eqA=D.net.cumsum(); tmdd=(np.maximum.accumulate(eqA.values)-eqA.values).max()/cap*100
    put(r,["TOTAL",ctag,elbl,round(tg/cap*100,1),round(tg),round(tc),round(tn),round(tn/cap*100,1),f"{tmdd:.0f}%",int(td),f"{w/td*100:.0f}%"],bold=True,fill=tf); r+=2
for col,wd in zip("ABCDEFGHIJK",[7,16,13,14,16,15,16,10,9,13,9]): ws.column_dimensions[col].width=wd
wb.save(xlsx); print(f"\n-> appended sheet 'FIXED book (ADV-cap+VWAP)' to {xlsx}")
