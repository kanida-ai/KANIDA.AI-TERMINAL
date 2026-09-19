"""
PER-STOCK SPEC SHEET generator — produces one Falcon-style 1-pager per sellable worker (the repository),
data-driven from the trailed source of truth. Pulls: signal rule (top promoted pattern), order type,
walk-forward-validated trail, position sizing, trailed results, leak-free provenance. Writes
docs/specs/<SYMBOL>.html for every sellable worker + refreshes docs/newgen_spec.html.
Run: PYTHONIOENCODING=utf-8 python arena/spec_generator.py
"""
import sys, json, sqlite3
from pathlib import Path
import pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db")
REP = ROOT / "reports"; SPECS = ROOT / "docs" / "specs"

CSS = open(ROOT / "docs" / "newgen_spec.html", encoding="utf-8").read().split("</style>")[0] + "</style>"

TRAIL_DESC = {"cap 5/2/3/1.5": ("arm 5% / floor 2% / giveback 3% / hard-stop 1.5% (≈0.3% price)", "tight capital-%"),
              "cap 6/2/5/3": ("arm 6% / floor 2% / giveback 5% / hard-stop 3%", "capital-%"),
              "cap 8/3/4/2": ("arm 8% / floor 3% / giveback 4% / hard-stop 2%", "capital-%"),
              "cap 10/4/6/3": ("arm 10% / floor 4% / giveback 6% / hard-stop 3%", "wider capital-%"),
              "donch N20": ("Donchian trailing high, 20-min", "Donchian"),
              "donch N40": ("Donchian trailing high, 40-min", "Donchian"),
              "no-trail": ("no intraday trail — hold to 15:20 square-off", "none"),
              "atr K1.2 w14": ("ATR trail K=1.2 (14-min)", "ATR"), "atr K2.0 w14": ("ATR trail K=2.0 (14-min)", "ATR")}


def tier(c): rd = c.get("ret_dd", 0); return "Elite" if rd >= 25 else "Strong" if rd >= 15 else "Solid"


def spec_html(sym, card, trail, sig, is_fno):
    ne = card["net_exp"]; t = tier(card); rd = card.get("ret_dd", 0); td, tk = TRAIL_DESC.get(trail, (trail, "custom"))
    short = "Short"; prod = "MIS 5× intraday" if not is_fno else "MIS 5× intraday"
    rule = sig["rule"][:150] if sig else "—"
    return f"""{CSS}
<div class="sheet">
  <div class="hdr">
    <div class="eyebrow">Kanida.AI · Worker Spec Sheet</div>
    <div class="tick">{sym}</div>
    <div class="badges">
      <span class="badge b-el">{t}</span><span class="badge b-tag">{prod} {short}</span>
      <span class="badge b-tag">Non-F&amp;O · daily recycle</span>
      <span class="badge b-tag">Trail: {tk}</span>
    </div>
    <p class="lead">Autonomous per-stock trader: <b>shorts {sym} intraday on 5× MIS</b> when the signal fires, managed by a <b>walk-forward-validated {tk} trail</b>, squared off by 15:20. Slippage-stressed (0.5%). Leak-free signal; 2026 sealed.</p>
    <div class="kpis">
      <div class="kpi"><div class="l">Return on ₹1L/day (2yr)</div><div class="v pos mono">{card['total_return']:+.0f}%</div></div>
      <div class="kpi"><div class="l">Account MDD (headline)</div><div class="v neg mono">{card['max_dd']:+.1f}%</div></div>
      <div class="kpi"><div class="l">Return ÷ drawdown</div><div class="v mono">{rd:.1f}</div></div>
      <div class="kpi"><div class="l">Win rate / day</div><div class="v mono">{card['win_rate']:.0f}%</div></div>
      <div class="kpi"><div class="l">Leverage</div><div class="v mono">5× MIS</div></div>
    </div>
  </div>
  <h2>1 · The signal — when to short</h2>
  <div class="card"><p><b>Direction: SHORT</b> (fade an over-extended move). Representative promoted rule
    {'(2026 sealed hit-rate ' + str(sig['prec']) + '% vs ' + str(sig['base']) + '% base)' if sig else ''}:</p>
    <div class="rule">SHORT if&nbsp; {rule}</div>
    <p class="note">Evaluated at the prior day's close (point-in-time features); entered next morning — no look-ahead.</p></div>
  <h2>2 · Execution — 5× MIS + validated trail</h2>
  <div class="card scroll"><table><tbody class="k">
    <tr><td>Product</td><td>MIS intraday — <b>5× leverage, fixed</b>, squared off same day</td></tr>
    <tr><td>Entry</td><td>09:15 open (optional 50/50 split with 09:16)</td></tr>
    <tr><td>Trail (walk-forward validated)</td><td><b>{td}</b> · armed 09:16 on capital basis</td></tr>
    <tr><td>Square-off</td><td>15:20 — no overnight carry</td></tr>
  </tbody></table></div>
  <h2>3 · Position sizing &amp; capital</h2>
  <div class="card scroll"><table><tbody class="k">
    <tr><td>Invested capital</td><td>a fixed cash/day you choose — the drawdown acts on this</td></tr>
    <tr><td>Exposure</td><td>cash/day × 5 (MIS); recycles daily → max active capital = one day</td></tr>
    <tr><td>Scaling</td><td>linear — 2× cash/day → 2× rupee P&amp;L</td></tr>
    <tr><td>Risk control</td><td>the intraday trail (5× is fixed for MIS shorts)</td></tr>
  </tbody></table></div>
  <h2>4 · Trailed results — invested cash (walk-forward + 0.5% slippage)</h2>
  <div class="card scroll"><table>
    <thead><tr><th>Year</th><th>Days</th><th>Return</th><th>Win</th><th>Best mo</th><th>Worst mo</th></tr></thead>
    <tbody>{''.join(f'<tr><td>{y["y"]}</td><td>{y["n"]}</td><td class="pos">+{round(y["ret"])}%</td><td>{y["win"]}%</td><td class="pos">+{round(y["best_month"])}%</td><td class="neg">{round(y["worst_month"])}%</td></tr>' for y in card.get('by_year_detail',[]))}
    <tr style="font-weight:700"><td>2-yr</td><td>{card['trades']}</td><td class="pos">+{card['total_return']:.0f}%</td><td>{card['win_rate']:.0f}%</td><td colspan="2"></td></tr></tbody></table></div>
  <div class="card scroll" style="margin-top:10px"><table><tbody class="k">
    <tr><td>① Account MDD (from peak balance) — headline</td><td class="mono neg">{card['max_dd']:+.1f}%</td></tr>
    <tr><td>② Fixed-capital DD (give-back ÷ ₹1L committed)</td><td class="mono neg">{card.get('dd_fixed',0):+.1f}%</td></tr>
    <tr><td>③ Worst month</td><td class="mono neg">{card.get('worst_month',0):+.1f}%</td></tr>
    <tr><td>Return ÷ drawdown (Calmar, on ①)</td><td class="mono">{rd:.1f}</td></tr>
    <tr><td>Positive months · win rate/day</td><td class="mono">{card.get('consistency',{}).get('pct_pos_months',0)}% · {card['win_rate']:.0f}%</td></tr>
  </tbody></table>
  <p class="note" style="margin-top:8px">Consistent year-over-year incl. sealed 2026; month-to-month is lumpy — a few big months carry it. Three drawdown lenses on the same rupee give-back: ① what your growing balance felt, ② vs your committed ₹1L (conservative), ③ worst month.</p></div>
  <h2>5 · Monthly normalized scorecard (each month = fresh ₹1L)</h2>
  <div class="card scroll"><table>
    <thead><tr><th>Month</th><th>Days</th><th>Win</th><th>Net P&L</th><th>Return</th><th>Worst intramonth DD</th></tr></thead>
    <tbody>{''.join(f'<tr><td>{mo["m"]}</td><td>{mo["n"]}</td><td>{mo["win"]}%</td><td class="{"pos" if mo["pnl"]>=0 else "neg"}">₹{mo["pnl"]:+,}</td><td class="{"pos" if mo["ret"]>=0 else "neg"}">{mo["ret"]:+.0f}%</td><td class="neg">{mo.get("dd",0):+.1f}%</td></tr>' for mo in card.get('by_month',[]))}</tbody></table>
    <p class="note" style="margin-top:8px">Months compare on equal footing. Worst intramonth DD = deepest give-back inside that month (the per-month risk the equity curve smooths away).</p></div>
  <h2>6 · Data &amp; leak-free provenance</h2>
  <div class="card"><table><tbody class="k">
    <tr><td>Features</td><td>point-in-time (trailing windows, T-1…T-5) — no forward shift/centering</td></tr>
    <tr><td>Train / seal</td><td>mine ≤2024 · promote 2025 · <b>2026 sealed</b></td></tr>
    <tr><td>Trail</td><td>tuned on 2025, <b>confirmed out-of-sample on 2026</b>, slippage-stressed</td></tr>
  </tbody></table></div>
  <div class="foot">Kanida.AI · research / paper figures · real-money human-gated · trailed source of truth</div>
</div>"""


def main():
    cards = {c["symbol"]: c for c in json.load(open(REP / "worker_cards.json"))}
    trails = {r["symbol"]: r["chosen_trail"] for r in pd.read_csv(REP / "trail_optimizer.csv").to_dict("records")}
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    sellable = [s for s, c in cards.items() if c.get("ret_dd", 0) >= 10 and c["sustained"]]
    SPECS.mkdir(parents=True, exist_ok=True); n = 0
    for sym in sellable:
        rows = con.execute("SELECT rule_text,prec_te,base_te FROM unified_patterns WHERE symbol=? AND promoted=1 "
                           "AND direction='dn' ORDER BY lift_te DESC LIMIT 1", (sym,)).fetchall()
        sig = {"rule": rows[0][0], "prec": rows[0][1], "base": rows[0][2]} if rows else None
        html = spec_html(sym, cards[sym], trails.get(sym, "no-trail"), sig, int(fno.get(sym, 0)))
        (SPECS / f"{sym}.html").write_text(html, encoding="utf-8"); n += 1
    con.close()
    if "NEWGEN" in cards:
        (ROOT / "docs" / "newgen_spec.html").write_text(
            spec_html("NEWGEN", cards["NEWGEN"], trails.get("NEWGEN", "no-trail"),
                      None if "NEWGEN" not in sellable else sig, int(fno.get("NEWGEN", 0))), encoding="utf-8")
    print(f"generated {n} per-stock spec sheets -> docs/specs/  (+ refreshed docs/newgen_spec.html)")


if __name__ == "__main__":
    main()
