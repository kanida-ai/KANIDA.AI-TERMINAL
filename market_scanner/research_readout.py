"""Generate a plain-language readout from the frozen studies, without rerunning or selecting rules."""
import html
import json
import math
import statistics
import zlib
from datetime import datetime
from . import backtest_store as store
from .data import ROOT, TIMEFRAMES

# The narrative discusses this frozen run; a new research run needs a fresh interpretation.
READOUT_RUN = '0e15f954dc432754'


def trade_readout(cell, segment='reference'):
    trades = cell['reference_trades'] if segment == 'reference' else [t for t in cell['trades'] if t['split'] == segment]
    stats = cell['reference'] if segment == 'reference' else cell['splits'].get(segment, {'n': 0})
    if not trades:
        return None
    returns = [t['net_return_pct'] for t in trades]
    # Exact exit timing is unknown for stop/target candles. Retain both elapsed-time bounds.
    elapsed_low, elapsed_high = [], []
    for t in trades:
        entry = datetime.fromisoformat(t['entry_time'])
        low = t['exit_candle_end'] if t['exit_timing'] == 'close' else t['exit_candle_start']
        high = t['exit_candle_start'] if t['exit_timing'] == 'open' else t['exit_candle_end']
        elapsed_low.append((datetime.fromisoformat(low)-entry).total_seconds()/86400)
        elapsed_high.append((datetime.fromisoformat(high)-entry).total_seconds()/86400)
    best = max(trades, key=lambda t:t['net_return_pct'])
    mfe = max(trades, key=lambda t:t['mfe_pct'])
    result = {key: cell[key] for key in ('symbol','pattern','pattern_name','timeframe','side','status')}
    result.update(segment=segment, n=len(trades), mean_net_pct=statistics.mean(returns), median_net_pct=statistics.median(returns),
        best_net_pct=max(returns), worst_net_pct=min(returns), win_rate=stats['win_rate'],
        best_favorable_move_pct=stats['max_favorable_pct'], worst_adverse_move_pct=stats['max_adverse_pct'],
        avg_win_pct=stats['avg_win_pct'], avg_loss_pct=stats['avg_loss_pct'], expectancy_ci95=stats['expectancy_ci95'],
        mean_without_best_pct=statistics.mean(sorted(returns)[:-1]) if len(returns)>1 else None,
        median_held_bars=statistics.median(t['holding_bars'] for t in trades),
        median_calendar_days_low=statistics.median(elapsed_low), median_calendar_days_high=statistics.median(elapsed_high),
        earliest_entry=min(t['entry_time'] for t in trades), latest_exit=max(t['exit_candle_end'] for t in trades),
        maximum_calendar_days_high=max(elapsed_high), minimum_calendar_days_low=min(elapsed_low),
        rule=cell['reference']['rule'] if segment=='reference' else cell['rule'],
        rule_description=cell['reference']['description'] if segment=='reference' else cell['rule_description'],
        best_trade={key:best[key] for key in ('signal_index','entry_time','exit_candle_end','exit_timing','entry','exit','net_return_pct','holding_bars')},
        maximum_favorable_trade_signal=mfe['signal_index'])
    assert len(trades) == stats['n']
    assert math.isclose(result['mean_net_pct'], stats['expectancy_pct'], abs_tol=1e-9)
    return result


def build_readout():
    groups, maximum_by_pattern, timeframe_counts = {}, {}, {}
    examples = {('ICICIBANK','flag_pole','1H','long'), ('TITAN','flag_pole','1H','long'),
                ('RELIANCE','flag_pole','1H','long'), ('TITAN','symmetrical_triangle','1H','long'),
                ('JSWSTEEL','channel','4H','long'), ('TITAN','channel','4H','long'),
                ('COLPAL','channel','1D','long'), ('APOLLOHOSP','channel','1W','long')}
    selected, comparisons = [], []
    evaluated = records = 0
    with store.connection() as con:
        run = READOUT_RUN
        metadata = json.loads(con.execute('SELECT payload FROM runs WHERE id=?',(run,)).fetchone()[0])
        assumed_cost = (metadata['rules']['round_trip_fee_bps'] + metadata['rules']['round_trip_slippage_bps']) / 100
        coverage = dict(con.execute('SELECT status,count(*) FROM cells WHERE run=? GROUP BY status',(run,)))
        for tf in TIMEFRAMES:
            timeframe_counts[tf] = {'studies_with_20_trades':0,'positive_net':0,'positive_gross':0,'largest_sample':0,
                                    'selected_rules':0,'test_with_20_trades':0,'positive_test_with_20_trades':0}
        rows = con.execute("SELECT payload FROM cells WHERE run=? AND json_extract(summary,'$.reference.n')>0 ORDER BY symbol,timeframe,pattern,side", (run,))
        for (raw,) in rows:
            cell = json.loads(zlib.decompress(raw))
            row = trade_readout(cell)
            evaluated += 1;records += row['n']
            tf, pattern, side = row['timeframe'], row['pattern'], row['side']
            key = (pattern, tf, side)
            group = groups.setdefault(key, {'pattern':pattern,'pattern_name':row['pattern_name'],'timeframe':tf,'side':side,
                'stocks_with_trades':0,'stocks_with_20_trades':0,'positive_net_with_20':0,'representative':row,'maximum_winner':row})
            group['stocks_with_trades'] += 1
            if row['n'] > group['representative']['n']:
                group['representative'] = row
            if row['best_net_pct'] > group['maximum_winner']['best_net_pct']:
                group['maximum_winner'] = row
            if pattern not in maximum_by_pattern or row['best_net_pct'] > maximum_by_pattern[pattern]['best_net_pct']:
                maximum_by_pattern[pattern] = row
            totals = timeframe_counts[tf]
            totals['largest_sample'] = max(totals['largest_sample'], row['n'])
            if row['n'] >= 20:
                group['stocks_with_20_trades'] += 1
                group['positive_net_with_20'] += row['mean_net_pct'] > 0
                totals['studies_with_20_trades'] += 1
                totals['positive_net'] += row['mean_net_pct'] > 0
                totals['positive_gross'] += row['mean_net_pct'] + assumed_cost > 0
            if (row['symbol'], pattern, tf, side) in examples:
                comparisons.append(row)
            if cell['rule']:
                totals['selected_rules'] += 1
                test = trade_readout(cell, 'test')
                if test:
                    selected.append(test)
                    totals['test_with_20_trades'] += test['n'] >= 20
                    totals['positive_test_with_20_trades'] += test['n'] >= 20 and test['mean_net_pct'] > 0
            if evaluated % 10000 == 0:print(f'Read {evaluated:,} nonempty studies', flush=True)
    return {'run_id':run,'source_latest':metadata['source_latest'],'rules':metadata['rules'],'coverage':coverage,
            'stocks':metadata['total'],'total_cells':sum(coverage.values()), 'nonempty_baselines':evaluated,
            'baseline_trade_records':records,'timeframes':timeframe_counts,
            'comparisons':comparisons,'groups':list(groups.values()),
            'maximum_by_pattern':list(maximum_by_pattern.values()),'selected_rule_tests':sorted(selected,key=lambda r:(-r['n'],r['symbol'])),
            'note':'Descriptive readout of the frozen run. No rules, sample thresholds, costs or original returns were changed.'}


def percent(value):
    return '—' if value is None else f'{value:+.2f}%'


def esc(value):return html.escape(str(value))


def metric(value):return f'<span class="{"positive" if value>0 else "negative" if value<0 else ""}">{percent(value)}</span>'


def calendar_days(row):
    a,b=row['median_calendar_days_low'],row['median_calendar_days_high']
    return f'{a:.2f}' if math.isclose(a,b) else f'{a:.2f}–{b:.2f}'


def study_table(rows):
    return '<div class="table-wrap"><table><thead><tr><th>Stock · pattern · chart</th><th>Trades</th><th>Average net / trade</th><th>Middle trade</th><th>Best closed trade</th><th>Win rate</th><th>Holding window</th></tr></thead><tbody>'+''.join(
        f'<tr><td><strong>{esc(r["symbol"])}</strong><small>{esc(r["pattern_name"])} · {r["timeframe"]} · {"long" if r["side"]=="long" else "hypothetical short"}</small></td>'
        f'<td>{r["n"]}{"<small>Small sample</small>" if r["n"]<20 else ""}</td><td>{metric(r["mean_net_pct"])}</td>'
        f'<td>{metric(r["median_net_pct"])}</td><td>{metric(r["best_net_pct"])}</td><td>{r["win_rate"]:.1f}%</td>'
        f'<td>{r["rule"]["hold"]} candles maximum<small>Median actually held: {r["median_held_bars"]:g} candles<br>{calendar_days(r)} elapsed calendar days</small></td></tr>' for r in rows
    )+'</tbody></table></div>'


def render_html(data):
    if data['run_id'] != READOUT_RUN:
        raise ValueError('The narrative must be reviewed before presenting a different research snapshot')
    n20=sum(t['studies_with_20_trades'] for t in data['timeframes'].values())
    net=sum(t['positive_net'] for t in data['timeframes'].values())
    gross=sum(t['positive_gross'] for t in data['timeframes'].values())
    comparison_order=['ICICIBANK','TITAN','RELIANCE','JSWSTEEL','COLPAL','APOLLOHOSP']
    comparisons=sorted(data['comparisons'], key=lambda r:(list(TIMEFRAMES).index(r['timeframe']),r['pattern'],comparison_order.index(r['symbol'])))
    rows=[]
    for r in sorted(data['maximum_by_pattern'],key=lambda r:r['pattern_name']):
        t=r['best_trade']
        rows.append(f'<tr><td>{esc(r["pattern_name"])}</td><td>{metric(r["best_net_pct"])}</td><td>{esc(r["symbol"])} · {r["timeframe"]}<small>{r["side"]}{" · hypothetical" if r["side"]=="short" else ""}</small></td><td>{t["entry_time"][:10]} → {t["exit_candle_end"][:10]}</td><td>{r["n"]}</td><td>{metric(r["mean_net_pct"])}</td></tr>')
    timeframe_sections=[]
    for tf in TIMEFRAMES:
        groups=sorted([g for g in data['groups'] if g['timeframe']==tf],key=lambda g:(g['pattern_name'],g['side']))
        timeline=[]
        for g in groups:
            r=g['representative'];m=g['maximum_winner']
            timeline.append(f'<tr><td>{esc(g["pattern_name"])}<small>{g["side"]}{" · hypothetical" if g["side"]=="short" else ""}</small></td>'
                f'<td>{g["positive_net_with_20"]} / {g["stocks_with_20_trades"]}<small>positive / with ≥20 trades</small></td>'
                f'<td>{esc(r["symbol"])}<small>{r["n"]} trades · largest stock sample</small></td>'
                f'<td>{metric(r["mean_net_pct"])}</td><td>{metric(r["best_net_pct"])}</td><td>{r["win_rate"]:.1f}%</td>'
                f'<td>{calendar_days(r)}<small>median calendar days<br>{r["rule"]["hold"]} chart candles</small></td>'
                f'<td>{metric(m["best_net_pct"])}<small>{esc(m["symbol"])} · {m["n"]} trades<br>{m["best_trade"]["entry_time"][:10]}</small></td></tr>')
        timeframe_sections.append(f'<section id="tf-{tf}"><h2>{tf} · every pattern</h2><p>Each example is the stock with the most baseline trades for that pattern and direction, chosen without looking at its return. The last column is a separate single-trade record; it is not the return to expect.</p><div class="table-wrap"><table><thead><tr><th>Pattern / direction</th><th>Stock evidence</th><th>Example stock</th><th>Its average net / trade</th><th>Its best closed trade</th><th>Its win rate</th><th>Its actual holding time</th><th>Record winner across stocks</th></tr></thead><tbody>{"".join(timeline)}</tbody></table></div></section>')
    evidence_rows=''.join(f'<tr><td>{tf}</td><td>{t["studies_with_20_trades"]:,}</td><td>{t["positive_net"]:,}</td><td>{t["selected_rules"]}</td><td>{t["test_with_20_trades"]}</td><td>{t["positive_test_with_20_trades"]}</td></tr>' for tf,t in data['timeframes'].items())
    tests=sorted(data['selected_rule_tests'],key=lambda r:0 if r['symbol']=='CEMPRO' else 1 if r['symbol']=='RADICO' else 2 if r['symbol']=='CAPLIPOINT' else 3)[:3]
    content=f'''<header><a href="/?view=backtest">KANIDA · back to studies</a><span>Research snapshot {esc(data['run_id'])}</span></header>
<main><div class="eyebrow">RESULTS FIRST</div><h1>What did we actually learn?</h1><p class="lead">Some stock–pattern combinations had positive historical returns. This run has not established a profitable stock-specific rule with at least 20 later-test trades. A best-ever winner and a preset exit are not a forecast and an optimal holding period.</p><p class="meta">Source candles through {data['source_latest'][:10]} · {data['stocks']:,} stocks · 10 patterns · four chart timeframes · 0.40% assumed round-trip cost. Results are per stock, pattern, timeframe and direction.</p>
<nav><a href="#examples">Actual stock results</a><a href="#holding">How long was each trade held?</a><a href="#maximum">Maximum returns</a><a href="#evidence">What survived later testing?</a><a href="#patterns">Every pattern × timeframe</a></nav>
<section id="examples"><h2>Start with actual stock results</h2><p>These are fixed-exit, whole-history baselines. Average net return includes winners and losers after costs. “Middle trade” is the median: half the outcomes were below it and half above it. Each row is independent.</p>{study_table(comparisons)}
<div class="callout"><strong>Same pattern, different stock:</strong> 1H Flag & Pole returned +0.50% per trade for ICICIBANK, −0.19% for TITAN and −0.48% for RELIANCE at the same six-candle exit. ICICIBANK still lost on more trades than it won; its average winner (+1.88%) was larger than its average loser (−0.76%). Its mean-return interval includes zero, and no rule passed this stock study’s validation gates.</div>
<p><strong>Why “Max FAV” confused the picture:</strong> ICICIBANK’s best closed Flag & Pole trade earned +7.54% net. Its largest favorable excursion was +9.17% before costs, seen somewhere during a held trade. That high-water mark was not necessarily captured by the exit rule and can come from a different trade. It is not average return or a target for the next setup.</p></section>
<section id="holding"><h2>Holding time: tested windows, not proven best exits</h2><p>The baseline used the same preset horizon for every pattern on a given chart timeframe. The rule search separately tried shorter/longer exits and stops/targets for each stock study. It did not establish a positive rule with ≥20 later-test trades.</p>
<div class="table-wrap"><table><thead><tr><th>Chart</th><th>Baseline exit</th><th>What that means</th><th>Maximum windows tried in rule mining</th></tr></thead><tbody>
<tr><td>1H</td><td>6 candles</td><td>5¼–6 trading hours; usually within 1–2 trading sessions</td><td>3 / 6 / 12 / 24 candles</td></tr>
<tr><td>4H</td><td>6 market-aligned candles</td><td>3 full-session equivalents; touches 3–4 trading dates depending on entry</td><td>3 / 6 / 12 / 24 candles</td></tr>
<tr><td>1D</td><td>10 daily candles</td><td>10 trading sessions, roughly two trading weeks</td><td>5 / 10 / 20 / 40 trading sessions</td></tr>
<tr><td>1W</td><td>4 weekly candles</td><td>About four trading weeks</td><td>2 / 4 / 8 / 13 weekly candles</td></tr></tbody></table></div>
<p>The 4H chart has two candles per normal NSE session: 09:15–13:15 and 13:15–15:30 IST. Six 4H bars therefore do not mean one 24-hour day. Calendar days include nights, weekends and holidays; the stock tables calculate elapsed time from actual trade timestamps. Stop/target exits show a time range where intrabar timing is unknown.</p>
<div class="callout"><strong>What we can say now:</strong> “This stock’s 4H pattern had these outcomes with a six-candle exit.” <strong>What we cannot yet say:</strong> “Every 4H Cup & Handle should be held for three days.” The stored research does not establish that.</div></section>
<section id="maximum"><h2>Largest recorded closed trade for each pattern</h2><p>Each value below is a single historical winner from the preset baseline, across all available stocks and all four timeframes, with its stock and date identified. These are deliberately extreme examples, can have tiny samples, and are sensitive to unadjusted source prices. Use them to inspect past trades, not to set a profit target. “Stock’s average” uses that winner’s own pattern/timeframe/direction study only.</p>
<div class="table-wrap"><table><thead><tr><th>Pattern</th><th>Largest net trade</th><th>Where it occurred</th><th>Entry → exit</th><th>Trades in that stock study</th><th>Stock’s average net</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div></section>
<section id="evidence"><h2>What survived testing on later candles?</h2><p>Across {n20:,} baseline studies with ≥20 trades, {net:,} had positive net averages. Before the assumed 0.40% cost, {gross:,} had positive averages. Thus {gross-net:,} crossed from a positive average before costs to a non-positive average after costs. These are counts of studies, not a pooled return or win probability.</p>
<div class="table-wrap"><table><thead><tr><th>Chart</th><th>Baseline studies with ≥20 trades</th><th>Positive baseline average</th><th>Rules passing early-data selection</th><th>Rules with ≥20 later-test trades</th><th>Positive average with ≥20 later-test trades</th></tr></thead><tbody>{evidence_rows}</tbody></table></div>
<p>The 34 rules that passed training and validation were all on 1H charts. Only CEMPRO’s long-channel rule reached 20 later-test trades; it lost on average. No selected 4H, daily or weekly rule passed the configured early-data gates. This may reflect limited occurrences, poor outcomes, or these detector/rule choices; it does not prove those patterns can never work.</p>
{study_table(tests)}<p>These three rows are later-test results, using the separately selected rule: CEMPRO uses a 1 ATR stop / 1R target and six-candle cap; CAPLIPOINT uses 2 ATR / 3R and a 12-candle cap; RADICO uses a 24-candle time exit. CEMPRO’s median actual hold was two candles because barriers often exited before the cap.</p>
<div class="callout"><strong>A large winner can hide a weak typical result.</strong> RADICO’s later-test channel average was +0.39%, but its median trade was −0.30%. Removing its single +15.71% winner changes the remaining average to −0.46%. There were only 19 test trades. This is a concentration diagnostic, not permission to remove winners from an expectancy calculation.</div>
<p>No weekly stock/pattern/direction study has 20 baseline trades; the largest weekly sample is 14. Cup & Handle and both Head & Shoulders families also have no single stock study with 20 baseline trades on any timeframe. Their statistics can be displayed, but reliable stock-specific probabilities and holding-time claims remain unsupported by these samples.</p></section>
<section id="patterns"><h2>Every pattern, on every chart timeframe</h2><p>No market-wide return is assigned to a pattern. Below, each row names the stock whose result it shows. Long and hypothetical short studies remain separate.</p><nav>{''.join(f'<a href="#tf-{tf}">{tf}</a>' for tf in TIMEFRAMES)}</nav></section>{''.join(timeframe_sections)}
<section><h2>What this tells us before choosing filters</h2><p>The useful questions are: Is the average return positive after costs? Is the typical trade also reasonable, or is one outlier carrying the average? How large were losses? How many trades support the number? Does the selected holding rule still work on later data? A minimum win-rate filter alone would hide ICICIBANK’s positive-average example, while a “maximum return” filter would elevate one-off winners.</p><p>We should decide filters after reviewing those distinctions. This readout adds no new screening thresholds and changes no existing backtest.</p></section>
<footer><p>All returns are hypothetical and tied to saved fills. Active-universe survivorship, unadjusted corporate actions, gap exclusions and assumed execution/costs limit the results. Historical shorts do not establish real borrowing or derivative execution. Confidence intervals do not correct for comparing many stocks or searching many rules.</p><p>Definition reference: <a href="https://www.cmegroup.com/education/courses/trading-psychology/the-mathematics-of-trading-success">CME on expectancy and why win rate alone is insufficient</a>. Numerical findings above come from KANIDA’s frozen run {esc(data['run_id'])}.</p></footer></main>'''
    css='''*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#f5f7f3;color:#26372a;font:15px/1.65 "Segoe UI",sans-serif}header{display:flex;justify-content:space-between;gap:20px;padding:18px 4vw;border-bottom:1px solid #dce4d6;background:white;font-size:12px}a{color:#276542}main{max-width:1440px;padding:44px 40px;margin:auto}.eyebrow{font-size:11px;letter-spacing:2px;color:#729068}h1{font-size:40px;line-height:1.2;letter-spacing:-1px;margin:12px 0 20px}h2{font-size:24px;line-height:1.3;margin:0 0 16px}.lead{font-size:19px;max-width:1050px}.meta{font-size:12px;color:#7b8873}nav{display:flex;gap:10px;flex-wrap:wrap;margin:24px 0}nav a{display:block;padding:8px 13px;border:1px solid #d5dfcf;border-radius:6px;text-decoration:none;background:white;font-size:13px}section{margin:38px 0;padding-top:28px;border-top:1px solid #dce4d6;scroll-margin-top:15px}p{max-width:1160px}.table-wrap{overflow:auto;border:1px solid #dce4d6;border-radius:8px;background:white;margin:20px 0}table{border-collapse:collapse;width:100%;font-size:13px;text-align:left}th{font-size:11px;background:#edf2e8;color:#647b58;font-weight:600;line-height:1.5;white-space:normal}td,th{padding:13px 15px;border-bottom:1px solid #e9eee4;vertical-align:top}td{min-width:85px}td:first-child{min-width:180px}small{display:block;font-size:11px;line-height:1.6;color:#85927a;margin-top:4px}.positive{color:#287949}.negative{color:#a64e43}.callout{padding:19px 22px;border-left:4px solid #96ad77;border-radius:4px;background:#edf3e6;margin:24px 0;max-width:1160px}footer{font-size:12px;color:#829077;border-top:1px solid #dce4d6;margin-top:40px;padding-top:20px}@media(max-width:750px){main{padding:28px 18px}h1{font-size:31px}.lead{font-size:17px}header span{display:none}table{min-width:850px}td,th{padding:11px}nav a{font-size:12px}}'''
    return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>KANIDA · Research findings</title><style>'+css+'</style></head><body>'+content+'</body></html>'


def main():
    data=build_readout()
    (ROOT/'output'/'research_readout.json').write_text(json.dumps(data,indent=2,allow_nan=False),encoding='utf-8')
    (ROOT/'output'/'research_readout.html').write_text(render_html(data),encoding='utf-8')
    print(json.dumps({k:data[k] for k in ('run_id','nonempty_baselines','baseline_trade_records','timeframes')},indent=2))
    print('Largest recorded trades:',json.dumps([{k:r[k] for k in ('pattern_name','symbol','timeframe','side','n','mean_net_pct','best_net_pct')} for r in data['maximum_by_pattern']],indent=2))


if __name__=='__main__':main()
