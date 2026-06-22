// falcon-f2.jsx — Version F2: labeled left nav · middle = persona → its Top 10
// → search · right = rich explanation (Why / Track record / Sector).
// Builds on F1; F and F1 untouched.
const { Tier } = window;

const f2Ico = {
  ask:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M12 3l1.8 4.9L18.7 9.7l-4.9 1.8L12 16.4l-1.8-4.9L5.3 9.7l4.9-1.8L12 3z" strokeLinejoin="round"/></svg>),
  signals:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M3 12h3l2.5-6 3 13 2.5-9 2 4h5" strokeLinecap="round" strokeLinejoin="round"/></svg>),
  coTrade:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><circle cx="9" cy="8" r="3"/><path d="M3.5 19a5.5 5.5 0 0 1 11 0" strokeLinecap="round"/><path d="M16 6.2a3 3 0 0 1 0 5.6M17.5 19a5.4 5.4 0 0 0-2.3-4.4" strokeLinecap="round"/></svg>),
  autoTrade:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M13 3L5 13h6l-1 8 8-10h-6l1-8z" strokeLinejoin="round"/></svg>),
  perf:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M4 19V5M4 19h16" strokeLinecap="round"/><path d="M7.5 15l3.5-4 3 2.5L20 7" strokeLinecap="round" strokeLinejoin="round"/></svg>),
  plans:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><rect x="3" y="6" width="18" height="13" rx="2.5"/><path d="M3 10h18" strokeLinecap="round"/></svg>),
  search:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5" strokeLinecap="round"/></svg>),
  chevron:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>),
  flame:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M12 3c1 3 4 4.5 4 8a4 4 0 0 1-8 0c0-1.5.6-2.5 1.2-3.3C9.8 9 11.5 8 12 3z" strokeLinejoin="round"/></svg>),
  clock:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2" strokeLinecap="round"/></svg>),
  bolt:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M13 3L5 13h6l-1 8 8-10h-6l1-8z" strokeLinejoin="round"/></svg>),
  trend:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M4 15l5-5 3 3 7-7" strokeLinecap="round" strokeLinejoin="round"/><path d="M16 6h4v4" strokeLinecap="round" strokeLinejoin="round"/></svg>),
  shield:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M12 3l7 3v5c0 4.5-3 8-7 10-4-2-7-5.5-7-10V6l7-3z" strokeLinejoin="round"/></svg>),
  arrow:(p)=>(<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" {...p}><path d="M5 12h14M13 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>),
};

const SEC = {
  Auto:"Auto is leading today (rank 2 of 20). 1 other Auto name is in today's picks. Money is rotating into the sector.",
  Consumer:"Consumer is firm today (rank 5 of 20). Flows have been steady here.",
  Energy:"Energy turned constructive today (rank 4 of 20). It's the only Energy name in the picks. Money just turned positive.",
  Infra:"Infra is strong today (rank 3 of 20). 2 Infra names are in today's picks. Money is moving in.",
  IT:"IT is leading today (rank 1 of 20). 2 IT names are in the picks. Strong inflows.",
  Financials:"Financials are firming today (rank 6 of 20). Money is starting to return.",
  Telecom:"Telecom is mid-pack today (rank 8 of 20). It's the only Telecom name in the picks. Money has been leaving this sector.",
  Pharma:"Pharma is rotating in today (rank 7 of 20). Breadth is improving.",
};

const F2DATA = [
  { rank:1, sym:"TATAMOTORS", name:"Tata Motors", sector:"Auto", tier:"goldplus", action:"STRONG BUY", conf:86, pct:"+4.1", d30:"+11.4", signals:112, why:"Trading at its one-year high, closing near the top of its weekly range and right at a fresh two-week high. A clean breakout — not a chase.", tr:{wins:6,total:10,rate:"63%",avgWin:"+13.0",avgLoss:"−6.7",worst:"−7"} },
  { rank:2, sym:"TITAN", name:"Titan Company", sector:"Consumer", tier:"gold", action:"BUY", conf:82, pct:"+3.3", d30:"+6.9", signals:74, why:"Reclaimed its 50-day line with an expanding range. Consumption names are rotating into favour and Titan is leading them.", tr:{wins:7,total:11,rate:"64%",avgWin:"+9.2",avgLoss:"−5.1",worst:"−6"} },
  { rank:3, sym:"RELIANCE", name:"Reliance Inds.", sector:"Energy", tier:"enterprise", action:"STRONG BUY", conf:78, pct:"+3.2", d30:"+5.1", signals:98, why:"Back above its 50-day average on heavy volume. Energy just flipped constructive and Reliance is the group's anchor.", tr:{wins:8,total:12,rate:"67%",avgWin:"+7.8",avgLoss:"−4.4",worst:"−5"} },
  { rank:4, sym:"LT", name:"Larsen & Toubro", sector:"Infra", tier:"gold", action:"BUY", conf:76, pct:"+2.9", d30:"+4.2", signals:69, why:"Breaking out of a tight multi-week base as the capex cycle turns. Buyers are stepping in on every dip.", tr:{wins:5,total:9,rate:"56%",avgWin:"+11.4",avgLoss:"−6.0",worst:"−8"} },
  { rank:5, sym:"INFY", name:"Infosys", sector:"IT", tier:"premium", action:"ACCUMULATE", conf:73, pct:"+2.6", d30:"+2.0", signals:61, why:"Re-accelerating with IT. Held its 20-day line all week — momentum turning back up after a pause.", tr:{wins:5,total:10,rate:"50%",avgWin:"+6.5",avgLoss:"−5.8",worst:"−7"} },
  { rank:6, sym:"ADANIPORTS", name:"Adani Ports", sector:"Infra", tier:"entcand", action:"BUY", conf:71, pct:"+2.4", d30:"+5.5", signals:83, why:"Higher-low structure intact with volume building under resistance. An Enterprise-quality setup is forming.", tr:{wins:6,total:9,rate:"67%",avgWin:"+10.2",avgLoss:"−5.5",worst:"−7"} },
  { rank:7, sym:"TCS", name:"Tata Consultancy", sector:"IT", tier:"gold", action:"BUY", conf:69, pct:"+2.1", d30:"+3.1", signals:58, why:"IT leadership is broadening and TCS is confirming it. Orderbook commentary supports the trend.", tr:{wins:7,total:12,rate:"58%",avgWin:"+6.0",avgLoss:"−4.0",worst:"−5"} },
  { rank:8, sym:"HDFCBANK", name:"HDFC Bank", sector:"Financials", tier:"enterprise", action:"ACCUMULATE", conf:67, pct:"+1.8", d30:"+3.8", signals:90, why:"Financials are firming and HDFC reclaimed its anchored VWAP. A steady, lower-risk continuation.", tr:{wins:9,total:13,rate:"69%",avgWin:"+5.2",avgLoss:"−3.6",worst:"−5"} },
  { rank:9, sym:"BHARTIARTL", name:"Bharti Airtel", sector:"Telecom", tier:"premium", action:"ACCUMULATE", conf:64, pct:"+1.4", d30:"+1.9", signals:47, why:"Defensive strength — a tight flag near highs with rising relative strength.", tr:{wins:6,total:10,rate:"60%",avgWin:"+4.8",avgLoss:"−3.9",worst:"−5"} },
  { rank:10, sym:"SUNPHARMA", name:"Sun Pharma", sector:"Pharma", tier:"premium", action:"ACCUMULATE", conf:62, pct:"+1.2", d30:"+2.6", signals:53, why:"Pharma is rotating in. Coiling above support with improving breadth.", tr:{wins:5,total:9,rate:"56%",avgWin:"+7.0",avgLoss:"−5.2",worst:"−6"} },
];

const F2NAV = [
  { id:"ask", label:"Ask Falcon", icon:"ask" },
  { id:"signals", label:"Signals", icon:"signals" },
  { id:"cotrade", label:"Co-Trading", icon:"coTrade" },
  { id:"auto", label:"AutoTrade", icon:"autoTrade", soon:true },
  { id:"perf", label:"Performance", icon:"perf" },
  { id:"plans", label:"Plans", icon:"plans" },
];
const F2PERS = [
  { id:"swing", name:"Falcon Top 10 Swing", hold:"3–10 days", icon:"flame" },
  { id:"btst", name:"BTST Trader", hold:"1–2 days", icon:"clock" },
  { id:"intraday", name:"Intraday Trader", hold:"Same day", icon:"bolt" },
  { id:"weekly", name:"Weekly Swing", hold:"1–4 weeks", icon:"trend" },
  { id:"longterm", name:"Long-Term Investor", hold:"3+ months", icon:"shield" },
];
const TIER_RANK = { enterprise:5, entcand:4, goldplus:4, gold:3, premium:2, standard:1 };
const num = s => parseFloat(String(s).replace("−","-").replace("+",""));
function orderFor(persona) {
  const a = [...F2DATA];
  if (persona==="btst") return a.sort((x,y)=>num(y.pct)-num(x.pct));
  if (persona==="intraday") return a.sort((x,y)=>y.conf-x.conf);
  if (persona==="weekly") return a.sort((x,y)=>num(y.d30)-num(x.d30));
  if (persona==="longterm") return a.sort((x,y)=>(TIER_RANK[y.tier]-TIER_RANK[x.tier])||(y.conf-x.conf));
  return a.sort((x,y)=>x.rank-y.rank);
}

if (typeof document !== "undefined" && !document.getElementById("f2-css")) {
  const s = document.createElement("style"); s.id = "f2-css";
  s.textContent = `
  .f2 { flex-direction: row; }
  .f2-rail { width: 210px; flex: 0 0 210px; border-right: 1px solid var(--line); background: var(--panel); display: flex; flex-direction: column; padding: 20px 12px 14px; }
  .f2-brand { display: flex; align-items: center; gap: 10px; padding: 2px 8px 16px; }
  .f2-bmark { width: 30px; height: 30px; border-radius: 9px; display: grid; place-items: center; background: linear-gradient(150deg, rgba(63,227,164,0.22), rgba(63,227,164,0.06)); border: 1px solid rgba(63,227,164,0.30); color: var(--mint); font-size: 15px; }
  .f2-bname { font-size: 16px; font-weight: 600; letter-spacing: -0.02em; }
  .f2-sec { font-size: 10px; text-transform: uppercase; letter-spacing: 0.12em; color: var(--faint); padding: 6px 10px 8px; font-weight: 500; }
  .f2-nav { display: flex; flex-direction: column; gap: 2px; }
  .f2-navitem { display: flex; align-items: center; gap: 11px; padding: 9px 10px; border-radius: 9px; color: var(--muted); font-size: 13.5px; font-weight: 450; cursor: pointer; letter-spacing: -0.01em; transition: background .14s, color .14s; }
  .f2-navitem:hover { background: rgba(255,255,255,0.035); color: var(--ink); }
  .f2-navitem.active { background: var(--mint-dim); color: var(--mint); font-weight: 500; }
  .f2-navitem svg { width: 17px; height: 17px; stroke-width: 1.6; flex: 0 0 auto; }
  .f2-navitem .lb { flex: 1; }
  .f2-soon { font-size: 8.5px; font-weight: 600; letter-spacing: 0.08em; color: var(--faint); border: 1px solid var(--line-2); padding: 1px 5px; border-radius: 4px; }
  .f2-foot { margin-top: auto; padding-top: 14px; border-top: 1px solid var(--line); display: flex; align-items: center; gap: 10px; padding-left: 6px; }
  .f2-avatar { width: 30px; height: 30px; border-radius: 8px; display: grid; place-items: center; font-size: 12px; font-weight: 600; color: #06120c; background: linear-gradient(150deg, var(--mint-hi), var(--mint)); }
  .f2-an { font-size: 12.5px; font-weight: 500; } .f2-as { font-size: 11px; color: var(--faint); }

  .f2-mid { width: 336px; flex: 0 0 336px; border-right: 1px solid var(--line); background: var(--panel); display: flex; flex-direction: column; }
  .f2-pwrap { padding: 15px 14px 13px; border-bottom: 1px solid var(--line); position: relative; }
  .f2-plabel { font-size: 10px; text-transform: uppercase; letter-spacing: 0.07em; color: var(--faint); margin-bottom: 7px; }
  .f2-pselect { display: flex; align-items: center; gap: 10px; background: rgba(255,255,255,0.04); border: 1px solid var(--line-2); border-radius: 11px; padding: 9px 11px; cursor: pointer; transition: border-color .14s; }
  .f2-pselect:hover { border-color: rgba(63,227,164,0.35); }
  .f2-pi { width: 28px; height: 28px; border-radius: 8px; display: grid; place-items: center; background: var(--mint-dim); color: var(--mint); flex: 0 0 auto; }
  .f2-pi svg { width: 15px; height: 15px; }
  .f2-pn { font-size: 13px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.2; }
  .f2-ph { font-size: 11px; color: var(--faint); }
  .f2-cv { margin-left: auto; color: var(--faint); } .f2-cv svg { width: 14px; height: 14px; }
  .f2-pmenu { position: absolute; top: calc(100% - 4px); left: 14px; right: 14px; background: #0e1713; border: 1px solid var(--line-2); border-radius: 12px; padding: 6px; z-index: 30; box-shadow: 0 22px 50px -16px rgba(0,0,0,0.7); }
  .f2-popt { display: flex; align-items: center; gap: 10px; padding: 9px 10px; border-radius: 8px; cursor: pointer; }
  .f2-popt:hover, .f2-popt.on { background: rgba(63,227,164,0.08); }
  .f2-popt .f2-pi { width: 24px; height: 24px; } .f2-popt .f2-pi svg { width: 13px; height: 13px; }

  .f2-lh { display: flex; align-items: center; justify-content: space-between; padding: 12px 18px 9px; }
  .f2-lh .t { font-size: 12px; font-weight: 600; }
  .f2-lh .d { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--muted); }
  .f2-lh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--mint); box-shadow: 0 0 8px var(--mint); }
  .f2-scroll { flex: 1; overflow-y: auto; padding: 0 9px; }
  .f2-scroll::-webkit-scrollbar { width: 0; }
  .f2-item { display: grid; grid-template-columns: 18px 1fr auto; align-items: center; gap: 11px; padding: 9px 11px; border-radius: 10px; cursor: pointer; transition: background .12s; }
  .f2-item:hover { background: rgba(255,255,255,0.035); }
  .f2-item.sel { background: var(--mint-dim); }
  .f2-item .r { font-size: 11px; color: var(--faint); text-align: center; font-family: "Geist Mono", monospace; }
  .f2-item.sel .r { color: var(--mint); }
  .f2-nm .s { font-size: 13.5px; font-weight: 550; }
  .f2-nm small { display: block; font-size: 11px; color: var(--faint); }
  .f2-rt { display: flex; align-items: center; gap: 9px; }
  .f2-rt .pc { font-size: 12.5px; font-weight: 600; color: var(--mint); font-family: "Geist Mono", monospace; }
  .f2-srch { padding: 12px 14px 14px; border-top: 1px solid var(--line); }
  .f2-box { display: flex; align-items: center; gap: 9px; background: rgba(255,255,255,0.04); border: 1px solid var(--line-2); border-radius: 11px; padding: 10px 12px; transition: border-color .14s; }
  .f2-box:focus-within { border-color: rgba(63,227,164,0.4); }
  .f2-box svg { width: 15px; height: 15px; color: var(--faint); }
  .f2-box input { flex: 1; background: none; border: none; outline: none; color: var(--ink); font-family: inherit; font-size: 13px; }
  .f2-box input::placeholder { color: var(--faint); }

  .f2-right { flex: 1; min-width: 0; overflow-y: auto; }
  .f2-rpad { padding: 26px 32px 40px; max-width: 760px; }
  .f2-greet { font-size: 24px; font-weight: 600; letter-spacing: -0.03em; }
  .f2-greet .sp { color: var(--mint); margin-right: 10px; }
  .f2-sub { font-size: 14px; color: var(--muted); margin-top: 7px; max-width: 540px; line-height: 1.5; }
  .f2-card { background: linear-gradient(180deg, var(--card), var(--card-2)); border: 1px solid var(--line-2); border-radius: 18px; padding: 22px 24px; margin-top: 22px; box-shadow: 0 24px 70px -34px rgba(0,0,0,0.55); }
  .f2-chead { display: flex; align-items: center; gap: 12px; }
  .f2-rankbig { font-size: 15px; font-weight: 700; color: var(--mint); font-family: "Geist Mono", monospace; }
  .f2-csym { font-size: 16px; font-weight: 600; } .f2-csym small { font-size: 13px; color: var(--faint); font-weight: 400; margin-left: 8px; }
  .f2-badges { display: flex; align-items: center; gap: 8px; margin: 13px 0 4px; flex-wrap: wrap; }
  .f2-badge { font-size: 10px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; padding: 3px 9px; border-radius: 6px; color: var(--mint); background: var(--mint-dim); box-shadow: inset 0 0 0 1px rgba(63,227,164,0.3); }
  .f2-sig { font-size: 12px; color: var(--muted); font-family: "Geist Mono", monospace; }
  .f2-h { font-size: 14px; font-weight: 600; letter-spacing: -0.01em; margin: 20px 0 8px; }
  .f2-p { font-size: 13.5px; line-height: 1.6; color: var(--ink-2); margin: 0; }
  .f2-p b { color: var(--ink); font-weight: 550; }
  .f2-up { color: var(--mint); font-weight: 600; } .f2-down { color: var(--red, #E8736B); font-weight: 600; }
  .f2-bul { list-style: none; padding: 0; margin: 11px 0 0; display: flex; flex-direction: column; gap: 8px; }
  .f2-bul li { font-size: 13px; color: var(--ink-2); display: flex; gap: 9px; line-height: 1.5; }
  .f2-bul li::before { content: ""; width: 5px; height: 5px; border-radius: 50%; background: var(--faint); margin-top: 7px; flex: 0 0 auto; }
  .f2-rule { height: 1px; background: var(--line); margin: 18px 0; }
  .f2-cmd { padding: 12px 14px 14px; border-top: 1px solid var(--line); }
  .f2-gp { background: rgba(255,255,255,0.022); border: 1px solid var(--line-2); border-radius: 12px; padding: 6px; display: flex; flex-direction: row; align-items: stretch; gap: 6px; }
  .f2-gsel { background: rgba(255,255,255,0.03); border: 1px solid var(--line); border-radius: 9px; padding: 6px 8px; cursor: pointer; display: flex; flex-direction: column; justify-content: center; gap: 1px; flex: 1 1 0; min-width: 0; transition: border-color .14s, background .14s; }
  .f2-gsel:hover { border-color: var(--line-2); background: rgba(255,255,255,0.05); }
  .f2-gk { font-size: 9px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--faint); font-weight: 500; }
  .f2-gv { display: flex; align-items: center; gap: 4px; font-size: 11.5px; color: var(--ink); font-weight: 500; }
  .f2-gv .vt { flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .f2-gv .cv { flex: 0 0 auto; color: var(--faint); display: inline-flex; } .f2-gv .cv svg { width: 12px; height: 12px; }
  .f2-ask { display: flex; align-items: center; justify-content: center; gap: 6px; background: var(--mint); color: #06130c; font-weight: 600; font-size: 13px; padding: 0 14px; border-radius: 9px; cursor: pointer; border: none; font-family: inherit; transition: background .14s; flex: 0 0 auto; box-shadow: 0 8px 22px -10px rgba(63,227,164,0.7); }
  .f2-ask:hover { background: var(--mint-hi); } .f2-ask svg { width: 13px; height: 13px; stroke-width: 2.2; }
  `;
  document.head.appendChild(s);
}

function PersonaSelect({ persona, setPersona }) {
  const [open, setOpen] = React.useState(false);
  const cur = F2PERS.find(p=>p.id===persona);
  const Ic = f2Ico[cur.icon];
  return (
    <div className="f2-pwrap">
      <div className="f2-plabel">Trader persona</div>
      <div className="f2-pselect" onClick={()=>setOpen(o=>!o)}>
        <div className="f2-pi">{Ic({})}</div>
        <div><div className="f2-pn">{cur.name}</div><div className="f2-ph">Top 10 · {cur.hold}</div></div>
        <div className="f2-cv">{f2Ico.chevron({})}</div>
      </div>
      {open && (
        <div className="f2-pmenu" onMouseLeave={()=>setOpen(false)}>
          {F2PERS.map(p => { const PI=f2Ico[p.icon]; return (
            <div key={p.id} className={"f2-popt"+(p.id===persona?" on":"")} onClick={()=>{ setPersona(p.id); setOpen(false); }}>
              <div className="f2-pi">{PI({})}</div>
              <div><div className="f2-pn" style={{fontSize:12.5}}>{p.name}</div><div className="f2-ph">{p.hold}</div></div>
            </div>
          ); })}
        </div>
      )}
    </div>
  );
}

function F2Detail({ s, pos }) {
  return (
    <div className="f2-card" key={s.sym}>
      <div className="f2-chead">
        <span className="f2-rankbig">#{pos}</span>
        <span className="f2-csym">{s.sym}<small>{s.name} · {s.sector}</small></span>
      </div>
      <div className="f2-badges">
        <span className="f2-badge">{s.action}</span>
        <Tier t={s.tier} />
        <span className="f2-sig">sig {s.pct}% · 30d {s.d30}%</span>
      </div>

      <div className="f2-h">Why we're picking this</div>
      <p className="f2-p">{s.why} <b>{s.signals} signals</b> are firing on this stock right now, and most are saying the same thing.</p>

      <div className="f2-rule"></div>
      <div className="f2-h">Historical track record</div>
      <p className="f2-p">When <b>{s.sym}</b> has appeared in our Falcon Top 10 before (2023–2026), it worked <b>{s.tr.wins} times out of {s.tr.total}</b> ({s.tr.rate} win rate).</p>
      <ul className="f2-bul">
        <li>When it worked → <span className="f2-up">{s.tr.avgWin}%</span> per trade. When it didn't → <span className="f2-down">{s.tr.avgLoss}%</span> per trade.</li>
        <li>Worst single trade: <span className="f2-down">{s.tr.worst}%</span> — it hit the stop, exactly what the stop is there to do.</li>
      </ul>

      <div className="f2-rule"></div>
      <div className="f2-h">What the sector is doing</div>
      <p className="f2-p">{SEC[s.sector]}</p>
    </div>
  );
}

function ScreenF2() {
  const [persona, setPersona] = React.useState("swing");
  const [q, setQ] = React.useState("");
  const [sel, setSel] = React.useState("TATAMOTORS");
  const ordered = orderFor(persona);
  const list = ordered.filter(s => (s.sym+" "+s.name+" "+s.sector).toLowerCase().includes(q.toLowerCase()));
  function pick(sym){ setSel(sym); }
  function changePersona(p){ setPersona(p); setSel(orderFor(p)[0].sym); }
  const current = ordered.find(s=>s.sym===sel) || ordered[0];
  const pos = ordered.findIndex(s=>s.sym===current.sym) + 1;

  return (
    <div className="fal f2">
      {/* left nav with names */}
      <div className="f2-rail">
        <div className="f2-brand"><div className="f2-bmark">✴</div><div className="f2-bname">Falcon</div></div>
        <div className="f2-sec">Workspace</div>
        <div className="f2-nav">
          {F2NAV.map((n,i) => { const Ic=f2Ico[n.icon]; return (
            <div key={n.id} className={"f2-navitem"+(i===0?" active":"")}>{Ic({})}<span className="lb">{n.label}</span>{n.soon && <span className="f2-soon">SOON</span>}</div>
          ); })}
        </div>
        <div className="f2-foot"><div className="f2-avatar">S</div><div><div className="f2-an">Shyam Verma</div><div className="f2-as">Pro · NSE</div></div></div>
      </div>

      {/* middle: persona → top 10 → search */}
      <div className="f2-mid">
        <PersonaSelect persona={persona} setPersona={changePersona} />
        <div className="f2-lh"><span className="t">Falcon Top 10</span><span className="d"><span className="dot"></span>today</span></div>
        <div className="f2-scroll">
          {list.length===0 && <div style={{padding:"24px 16px",textAlign:"center",color:"var(--faint)",fontSize:12.5}}>No name matches “{q}”.</div>}
          {list.map((s) => { const p = ordered.findIndex(o=>o.sym===s.sym)+1; return (
            <div key={s.sym} className={"f2-item"+(current.sym===s.sym?" sel":"")} onClick={()=>pick(s.sym)}>
              <span className="r">{p}</span>
              <div className="f2-nm"><span className="s">{s.sym}</span><small>{s.name}</small></div>
              <div className="f2-rt"><Tier t={s.tier} /><span className="pc">{s.pct}%</span></div>
            </div>
          ); })}
        </div>
        <div className="f2-cmd">
          <div className="f2-gp">
            <div className="f2-gsel" style={{flex:"1.3 1 0"}}><span className="f2-gk">Intent</span><span className="f2-gv"><span className="vt">Analyze a stock</span><span className="cv">{f2Ico.chevron({})}</span></span></div>
            <div className="f2-gsel"><span className="f2-gk">Stock</span><span className="f2-gv"><span className="vt">{current.sym}</span></span></div>
            <button className="f2-ask">Ask{f2Ico.arrow({})}</button>
          </div>
        </div>
      </div>

      {/* right: greeting + explanation */}
      <div className="f2-right">
        <div className="f2-rpad">
          <div className="f2-greet"><span className="sp">✴</span>Good morning, Shyam</div>
          <div className="f2-sub">Markets opened firm — IT &amp; financials are leading today.</div>
          <F2Detail s={current} pos={pos} />
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { ScreenF2, F2DATA, SEC, F2PERS, F2NAV, orderFor, f2Ico, PersonaSelect });
