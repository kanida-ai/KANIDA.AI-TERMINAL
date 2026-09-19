"""Read-only bridge probe: two existing feature engines, one frozen OHLCV input.

This probes prefix invariance, not indicator correctness or investment efficacy.
Targets in Stock Miner's output are explicitly excluded from usable features.
"""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import time
import types
import warnings
from .inventory import TERMINAL,WORKSPACE


def load_pure_module(path,name):
    raw=path.read_bytes();module=types.ModuleType(name);module.__file__=str(path)
    # These inspected indicator modules contain NumPy/Pandas helpers. No miner,
    # scheduler, broker module, database writer or main entry point is executed.
    exec(compile(raw,str(path),'exec'),module.__dict__)
    return module,hashlib.sha256(raw).hexdigest()


def run():
    import numpy as np
    import pandas as pd
    from market_scanner import backtest_store as store
    with store.connection() as con:run_id=store.active_run(con)
    bars=store.load_history(run_id,'TITAN')['1D'][0]
    frame=pd.DataFrame([dict(symbol='TITAN',trade_date=b['end'][:10],**{k:b[k] for k in ('open','high','low','close','volume')}) for b in bars])
    miner,mhash=load_pure_module(TERMINAL/'stock_miner/indicators.py','kanida_probe_stock_features')
    ndp,nhash=load_pure_module(TERMINAL/'ndp/indicator_library.py','kanida_probe_ndp_features')
    def ndp_input(df):return {key:df[col].to_numpy(dtype=float) for key,col in [('o','open'),('h','high'),('l','low'),('c','close'),('v','volume')]}
    metadata={'trade_date','wk','ret_oc','y','trade_date_next'}
    with warnings.catch_warnings(record=True) as warnings_seen:
        warnings.simplefilter('always');started=time.perf_counter();mfull=miner.compute_features(frame);mt=time.perf_counter()-started
        started=time.perf_counter();nfull=ndp.build_all(ndp_input(frame));nt=time.perf_counter()-started
        features=[c for c in mfull.columns if c not in metadata]
        cuts=sorted({len(frame)//2,len(frame)-100});failures=[];checked=0
        for cut in cuts:
            prefix=frame.iloc[:cut].copy();mp=miner.compute_features(prefix);nprefix=ndp.build_all(ndp_input(prefix))
            for col in features:
                checked+=1
                if not np.allclose(mfull[col].to_numpy()[252:cut],mp[col].to_numpy()[252:],rtol=1e-8,atol=1e-9,equal_nan=True):failures.append(dict(engine='stock_miner',feature=col,prefix=cut))
            if [(n,p) for n,p,_ in nfull]!=[(n,p) for n,p,_ in nprefix]:raise AssertionError('Signal catalogue changed across history length')
            for (name,params,a),(_,_,b) in zip(nfull,nprefix):
                checked+=1
                if not np.allclose(a[252:cut],b[252:],rtol=1e-8,atol=1e-9,equal_nan=True):failures.append(dict(engine='ndp',feature=name,parameters=params,prefix=cut))
    result=dict(created=datetime.now(timezone.utc).isoformat(),symbol='TITAN',data_run=run_id,bars=len(bars),first=bars[0]['end'],last=bars[-1]['end'],
        engines=[dict(id='stock_miner',sha256=mhash,feature_count=len(features),seconds=round(mt,4),excluded_target_and_metadata_fields=sorted(metadata)),
                 dict(id='ndp',sha256=nhash,signal_variant_count=len(nfull),seconds=round(nt,4))],
        prefix_checks=checked,prefix_failures=failures,warmup_excluded=252,warnings=sorted({str(w.message) for w in warnings_seen}),
        limitations=['One-stock bridge probe, not a market-wide latency benchmark.','Prefix invariance was checked at two cutoffs after 252 warm-up observations.',
            'Indicator formula fidelity, adjustment policy, execution rules and statistical usefulness remain unvalidated.',
            'Feature timing excludes initial imports and historical-data loading. No backtest or walk-forward performance is claimed.',
            'Source engines were executed through adapters, not edited. Labels and future returns were excluded from features.'])
    folder=WORKSPACE/'reports/research-orchestrator';folder.mkdir(parents=True,exist_ok=True);path=folder/('feature-bridge-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.json')
    path.write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(dict(engines=result['engines'],prefix_checks=checked,prefix_failures=failures,report=str(path)),indent=2));return result


if __name__=='__main__':run()
