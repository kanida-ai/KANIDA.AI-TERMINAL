from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from .drawing_refresh import refresh_cell
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

from .data import (CAS_START, LEGACY, MARKET15, ROOT, TIMEFRAMES, Calendar, ClosingConnection, SymbolCalendarView,
                   aggregate, aggregate_market15, candle_source, connect_source, load_market15, load_rows,
                   market15_coverage, market15_has_daily, market15_latest, market15_path, market15_regimes,
                   market15_snapshot, market15_symbols, now_ist, observed_sessions, open_market15, universe)
from .data_status import data_status
from .detectors import PATTERNS, detect
from . import pattern_lines, pattern_live

LOG=logging.getLogger('kanida')


def dumps(value):
    return json.dumps(value, separators=(',', ':'), allow_nan=False)


class Scanner:
    def __init__(self, config):
        self.config=config
        self.source=(ROOT/config['database']).resolve()
        # Snapshot cache. Defaults to exactly the file it has always used; the override
        # exists so a research-set scan can be measured or trialled without overwriting
        # the running legacy snapshot.
        self.cache=Path(os.environ.get('SCANNER_CACHE_DB') or config.get('cache_database')
                        or (ROOT/'output'/'scanner.sqlite3'))
        self.cache.parent.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock()
        self.progress={'running':False,'done':0,'total':0,'error':None}
        self.stop=threading.Event()
        self.metadata={}
        self.cells={}
        # Pattern set: 'legacy' (default; the ten chart detectors in detectors.py, exactly
        # what this scanner has always run) or 'research' (the 107 research pattern ids /
        # 262 direction-variant cells, through market_scanner/pattern_live.py). Every
        # research branch below is gated on this one value, so the legacy path is
        # byte-identical while the switch is off.
        self.pattern_set=pattern_live.pattern_set(config)
        self.research={}
        if self.pattern_set==pattern_live.RESEARCH:
            # Refuses to start without a passing warm-up measurement: a research
            # detection on too little history is not the same event the research saw.
            pattern_live.warmup()
            self.research={'spec_hash':pattern_live.detector_spec_hash(),
                           'evidence':pattern_live.evidence_identity(config),
                           'catalogue':pattern_live.catalogue(),
                           'required_bars':{tf:pattern_live.required_bars(tf) for tf in config['timeframes']}}
            LOG.info('pattern set: research (%s cells, spec %s, warm-up %s, evidence %s)',
                     len(pattern_live.research_registry()[1]),self.research['spec_hash'][:12],
                     self.research['required_bars'],self.research['evidence']['status'])
        with self.connection() as con:
            con.execute('PRAGMA journal_mode=WAL')
            if self.pattern_set==pattern_live.RESEARCH:
                pattern_live.ensure_schema(con)
            con.executescript('''CREATE TABLE IF NOT EXISTS cells (
                symbol TEXT, timeframe TEXT, signature TEXT, payload TEXT NOT NULL,
                PRIMARY KEY(symbol,timeframe));
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);''')
            for symbol,tf,_,payload in con.execute('SELECT * FROM cells'):
                self.cells[(symbol,tf)]=json.loads(payload)
            row=con.execute("SELECT value FROM meta WHERE key='snapshot'").fetchone()
            if row: self.metadata=json.loads(row[0])
        # Candle source: 'legacy' (db/kanida.db, the default and the frozen-research
        # source) or 'market15' (db/market15.db, live). Nothing below changes for
        # 'legacy' -- every market15 branch is gated on this one value.
        self.candle_source=candle_source(config)
        self.market15=market15_path(config) if self.candle_source==MARKET15 else None
        self.market15_symbols=set()
        self.cas_symbols={}
        self.market15_coverage={}
        self.market15_latest=None
        self.market15_snapshot=None
        self.md_calendar=None
        with connect_source(self.source) as source:
            self.calendar=Calendar(config,observed_sessions(source))
            self.stocks=universe(source)
        self.stock_map={s['symbol']:s for s in self.stocks}
        self.enabled=[p for p in PATTERNS if p[0] in config['enabled_patterns']]
        if self.candle_source==MARKET15:
            self.refresh_market15()

    # -- market15 ------------------------------------------------------------
    def refresh_market15(self):
        """Reload what the 15-minute store holds. Once per scan, never per poll."""
        from market_data.live.calendar_ext import live_calendar
        with open_market15(self.market15) as store:
            self.market15_symbols=market15_symbols(store)
            self.cas_symbols=market15_regimes(store)
            self.market15_coverage=market15_coverage(store)
            self.market15_latest=market15_latest(store)
            self.market15_snapshot=market15_snapshot(store)
            has_daily=market15_has_daily(store)
        self.md_calendar=live_calendar()
        LOG.info('market15: %s symbols, %s CAS regimes, newest bar %s',
                 len(self.market15_symbols),len(self.cas_symbols),self.market15_latest)
        if not has_daily:
            LOG.warning('market15: no daily_bars table -- 1D/1W will be empty until the '
                        'live ingest daily pass has run (market_data.live.cli)')
        elif not self.cas_symbols:
            LOG.warning('market15: no CAS session regimes recorded -- post-2026-08-03 4H/1D '
                        'candles for F&O stocks will be dropped as incomplete (contract 2A)')

    def probe_market15(self):
        """Cheap freshness probe for the watcher: the newest completed bar."""
        try:
            with open_market15(self.market15) as store:
                return market15_latest(store)
        except Exception:
            LOG.exception('market15 freshness probe failed')
            return self.market15_latest

    def symbol_view(self,symbol):
        """(market_data SymbolCalendar, scanner SymbolCalendarView) for `symbol`."""
        from market_data.calendar import RegimeBook, SessionRegime, REGIME_CAS
        first=self.cas_symbols.get(symbol)
        book=RegimeBook([SessionRegime(symbol,first,REGIME_CAS,'stored')]) if first else RegimeBook()
        return self.md_calendar.for_symbol(symbol,book),SymbolCalendarView(self.calendar,first)

    def connection(self):
        return sqlite3.connect(self.cache, timeout=30, factory=ClosingConnection)

    def schedule(self,regime='regular'):
        now=now_ist()
        cutoff=now-timedelta(seconds=self.config['settlement_delay_seconds'])
        # A CAS stock's continuous session ends 15:15 from 2026-08-03 (contract 2A),
        # so its latest expected 1H/4H close differs from the market-wide one.
        calendar=self.calendar if regime=='regular' else SymbolCalendarView(self.calendar,CAS_START)
        return {tf:{'latest_expected':calendar.latest_close(tf,cutoff),
                    'next_close':calendar.next_close(tf,now),
                    'last_scan':self.metadata.get('timeframes',{}).get(tf,{}).get('finished_at'),
                    **self.metadata.get('timeframes',{}).get(tf,{})} for tf in self.config['timeframes']}

    def expected_map(self):
        """(regular, cas) schedules. `cas` is the same object unless market15 is
        active and the store knows of CAS symbols, so legacy is untouched."""
        regular=self.schedule()
        return regular,(self.schedule('cas') if self.cas_symbols else regular)

    def expected_close(self,tf,symbol,regular=None,cas=None):
        if regular is None: regular,cas=self.expected_map()
        table=cas if symbol in self.cas_symbols else regular
        return table[tf]['latest_expected']

    def source_freshness(self,schedule):
        """(source_latest, source_stale) -- the newest completed bar the active
        candle source holds, versus the newest the calendar expects."""
        if self.candle_source==MARKET15:
            latest=self.market15_latest.isoformat(sep=' ') if self.market15_latest else ''
        else:
            latest=max((s['daily_latest'] or '' for s in self.stocks),default='')
        expected=(schedule['1D']['latest_expected'] or '')[:10]
        return latest,bool(latest and latest[:10]<expected)

    def data_status(self,latest='',stale=False):
        """Provenance + freshness for `/api/state` (market_scanner/data_status.py).

        A failure here must never take the rest of the state down, so it
        degrades to a note instead of a 500."""
        try:
            return data_status(self,latest,stale)
        except Exception as exc:  # noqa: BLE001
            LOG.exception('data_status failed')
            return {'version':1,'error':str(exc),
                    'note':'Data provenance could not be read; see the scanner log.'}

    def state(self):
        with self.lock:
            schedule,cas=self.expected_map()
            matches=[m for cell in self.cells.values() for m in cell['matches']]
            latest,stale=self.source_freshness(schedule)
            current=sum(m['candle_end']==self.expected_close(m['timeframe'],m['symbol'],schedule,cas) for m in matches)
            research=self.pattern_set==pattern_live.RESEARCH
            return {'universe':len(self.stocks),
                    'patterns':(self.research['catalogue'] if research else
                                [{'id':p[0],'name':p[1],'description':p[2]} for p in self.enabled]),
                    'pattern_set':self.pattern_set,
                    # Additive and only populated on the research set; the legacy state
                    # payload keeps exactly the keys it always had plus 'pattern_set'.
                    **({'research':{
                        'cells':len(pattern_live.research_registry()[1]),
                        'pattern_ids':len(self.research['catalogue']),
                        'detector_spec_hash':self.research['spec_hash'],
                        'evidence':self.research['evidence'],
                        'warmup':pattern_live.warmup_summary(),
                        'history_bars':self.research['required_bars'],
                        'live_tail_bars':pattern_live.LIVE_TAIL,
                        'states':['forming','confirmed','invalidated','expired'],
                        'live':{s:sum(m['state']==s for m in matches)
                                for s in ('forming','confirmed','invalidated','expired')}}} if research else {}),
                    'timeframes':self.config['timeframes'], 'matches':len(matches),'current_matches':current,
                    'source_latest':latest,'source_stale':stale,
                    'schedule':schedule,'progress':dict(self.progress), 'metadata':self.metadata,
                    'calendar_supported':now_ist().year in self.config['calendar_years'],
                    'candle_source':self.candle_source,
                    # Additive: where the candles come from and how current they are, for the
                    # app's Data status popover. Read-only, memoised, and it never changes the
                    # shape of anything above it.
                    'data_status':self.data_status(latest,stale),
                    'server_time':now_ist().isoformat(sep=' '),
                    'database':str(self.market15 if self.candle_source==MARKET15 else self.source)}

    def summaries(self):
        with self.lock:
            expected,cas=self.expected_map()
            results=[]
            for (symbol,tf),cell in self.cells.items():
                for match in cell['matches']:
                    summary={k:v for k,v in match.items() if k not in ('lines','evidence','geometry','input')}
                    summary['current']=match['candle_end']==self.expected_close(tf,symbol,expected,cas)
                    results.append(summary)
            return sorted(results,key=lambda m:(-m['score'],m['symbol'],m['timeframe']))

    def stock(self, symbol):
        with self.lock:
            if symbol not in self.stock_map: return None
            expected,cas=self.expected_map()
            return {**self.stock_map[symbol], 'timeframes':{tf:{
                **{k:v for k,v in self.cells.get((symbol,tf),{'status':'pending','matches':[]}).items() if k not in ('bars','signature')},
                'current':self.cells.get((symbol,tf),{}).get('last_candle')==self.expected_close(tf,symbol,expected,cas)
            } for tf in self.config['timeframes']}}

    def chart(self,symbol,tf):
        with self.lock:
            cell=self.cells.get((symbol,tf))
            if cell is None: return None
            # Drawing adapter changes do not require rerunning detectors or rewriting the scan ledger.
            rendered=refresh_cell(cell) if self.pattern_set==pattern_live.RESEARCH else cell
            return {**rendered, 'stock':self.stock_map.get(symbol), 'current':cell.get('last_candle')==self.expected_close(tf,symbol)}

    def coverage(self):
        with self.lock:
            return [{'symbol':s['symbol'],'company':s['company'],
                     'daily_latest':self.market15_coverage.get(s['symbol'],(None,None))[1] if self.candle_source==MARKET15 else s['daily_latest'],
                     'intraday_latest':self.market15_coverage.get(s['symbol'],(None,None))[0] if self.candle_source==MARKET15 else s['intraday_latest'],
                     'timeframes':{tf:{k:v for k,v in self.cells.get((s['symbol'],tf),{'status':'pending'}).items() if k in ('status','bar_count','last_candle','quality','error')} for tf in self.config['timeframes']}} for s in self.stocks]

    def request_scan(self, timeframes=None, force=False):
        with self.lock:
            if self.progress['running']: return False
            self.progress={'running':True,'done':0,'total':len(self.stocks),'error':None,
                           'started_at':now_ist().isoformat(sep=' '),'timeframes':list(timeframes or self.config['timeframes'])}
        threading.Thread(target=self.run_scan,args=(timeframes or self.config['timeframes'],force),daemon=True,name='market-scan').start()
        return True

    def history_window(self,timeframes):
        """How deep the source read must go for the active pattern set.

        Legacy returns ``None`` everywhere, so the existing reads are unchanged.
        Research derives the depth from the **measured** warm-up table: sessions
        for the intraday store (1H is 6 buckets a CAS session, 4H is 2) and
        daily rows for 1D/1W (a 1W candle needs a full Mon-Fri of daily bars)."""
        if self.pattern_set!=pattern_live.RESEARCH:
            return None
        need=self.research['required_bars']
        sessions=max([1]+[-(-need[tf]//per) for tf,per in (('1H',6),('4H',2)) if tf in timeframes and tf in need])
        daily=max([1]+[need[tf]*(5 if tf=='1W' else 1) for tf in ('1D','1W') if tf in timeframes and tf in need])
        # +10 sessions / +10 rows of slack for holidays inside the window, never for the detector.
        return {'sessions':sessions+10,'daily_bars':daily+10,'five_minute_rows':(sessions+10)*75}

    def load_candles(self,symbol,timeframes,cutoff):
        """Rows for one symbol from the active source, plus the aggregator to use."""
        window=self.history_window(timeframes)
        if self.candle_source==MARKET15:
            if symbol not in self.market15_symbols:
                return None,None,None
            extra={} if window is None else {'sessions':window['sessions'],'daily_bars':window['daily_bars']}
            with open_market15(self.market15) as store:
                intraday=load_market15(store,symbol,True,cutoff,**{k:v for k,v in extra.items() if k=='sessions'}) if set(timeframes)&{'1H','4H'} else None
                daily=load_market15(store,symbol,False,cutoff,**{k:v for k,v in extra.items() if k=='daily_bars'}) if set(timeframes)&{'1D','1W'} else None
            return intraday,daily,self.symbol_view(symbol)
        with connect_source(self.source) as con:
            intraday=load_rows(con,symbol,True,cutoff,None if window is None else window['five_minute_rows']) if set(timeframes)&{'1H','4H'} else None
            daily=load_rows(con,symbol,False,cutoff,None if window is None else window['daily_bars']) if set(timeframes)&{'1D','1W'} else None
        return intraday,daily,None

    def trim_window(self,bars,matches):
        """Keep the candles the chart needs, and rebase match indices onto them.

        The research set detects on the measured warm-up window, which can be
        several times the display window; storing all of it for every cell would
        multiply the snapshot cache for no benefit. The kept window always covers
        the oldest surfaced formation, so no match can point outside it."""
        if not bars:
            return bars,matches
        # The kept window must cover every overlay point too, or a drawn boundary
        # would start outside the candles the app receives.
        oldest=min([pattern_lines.oldest_index(m.get('lines'),m['start_index']) for m in matches],
                   default=len(bars)-1)
        keep=min(len(bars),max(self.config['history_bars'],len(bars)-oldest+5))
        offset=len(bars)-keep
        if offset<=0:
            return bars,matches
        for match in matches:
            match['start_index']-=offset
            match['end_index']-=offset
            pattern_lines.rebase(match.get('lines'),offset)
        return bars[offset:],matches

    def history_bars(self,tf):
        """Candles handed to the detectors for `tf`: the config window on the
        legacy pattern set, the measured warm-up on the research set."""
        if self.pattern_set!=pattern_live.RESEARCH:
            return self.config['history_bars']
        return self.research['required_bars'][tf]

    def scan_stock(self,stock,timeframes,cutoff,force):
        result=[]
        research=self.pattern_set==pattern_live.RESEARCH
        intraday,daily,views=self.load_candles(stock['symbol'],timeframes,cutoff)
        for tf in timeframes:
            try:
                rows=intraday if tf in ('1H','4H') else daily
                limit=self.history_bars(tf)
                if self.candle_source==MARKET15:
                    bars,quality=(([],{'invalid_rows':0,'incomplete_buckets':0,'gaps':0}) if views is None
                                  else aggregate_market15(rows,tf,views[0],views[1],cutoff,limit))
                else:
                    bars,quality=aggregate(rows,tf,self.calendar,cutoff,limit)
                minimum=pattern_live.minimum_bars(tf) if research else self.config['minimum_bars']
                if research:
                    signature=hashlib.sha256(dumps([bars,pattern_live.RESEARCH,self.research['spec_hash'],
                                                    pattern_live.LIVE_RULES_VERSION,pattern_live.LIVE_TAIL]).encode()).hexdigest()
                else:
                    signature=hashlib.sha256(dumps([bars,self.config['pattern_version'],self.config['enabled_patterns']]).encode()).hexdigest()
                cached=self.cells.get((stock['symbol'],tf))
                if not force and cached and cached.get('signature')==signature:
                    result.append((stock['symbol'],tf,cached))
                    continue
                status='scanned' if len(bars)>=minimum else 'insufficient_history' if bars else 'no_complete_data'
                detection_bars=len(bars)
                families=short=()
                if research:
                    # Per family: a family whose measured warm-up this window does not
                    # meet is not run and is named, never run short and reported anyway.
                    families,short=pattern_live.families_for(tf,len(bars))
                    matches=pattern_live.detect_live(
                        bars,tf,symbol=stock['symbol'],company=stock['company'],sector=stock['sector'],
                        evidence=self.research['evidence'],spec_hash=self.research['spec_hash'],families=families,
                        input_snapshot={'source':self.candle_source,'snapshot_id':self.market15_snapshot,
                                        'first_bar':bars[0]['time'] if bars else None,
                                        'last_bar':bars[-1]['end'] if bars else None,
                                        'bars':len(bars),'quality':quality}) if status=='scanned' else []
                    # The cell keeps only the candles the app needs to draw; detection itself
                    # always ran on the full measured warm-up window above.
                    bars,matches=self.trim_window(bars,matches)
                else:
                    matches=detect(bars,self.config['enabled_patterns']) if len(bars)>=minimum else []
                    for match in matches:
                        match.update({'id':f"{stock['symbol']}:{tf}:{match['pattern']}",'symbol':stock['symbol'],'company':stock['company'],'sector':stock['sector'],'timeframe':tf})
                cell={'symbol':stock['symbol'],'timeframe':tf,'status':status,
                      'bar_count':len(bars), 'last_candle':bars[-1]['end'] if bars else None,'quality':quality,
                      'bars':bars,'matches':matches,'signature':signature}
                if research:
                    cell.update(detection_bars=detection_bars,families_scanned=list(families),
                                families_insufficient_history=list(short))
            except Exception as exc:
                LOG.exception('Could not scan %s %s',stock['symbol'],tf)
                cell={'symbol':stock['symbol'],'timeframe':tf,'status':'error','error':str(exc), 'bar_count':0, 'last_candle':None,'bars':[],'matches':[],'signature':''}
            result.append((stock['symbol'],tf,cell))
        return result

    def run_scan(self,timeframes,force=False):
        began=time.monotonic()
        try:
            cutoff=now_ist()-timedelta(seconds=self.config['settlement_delay_seconds'])
            with connect_source(self.source) as source:
                stocks=universe(source)
                self.calendar=Calendar(self.config,observed_sessions(source))
            if self.candle_source==MARKET15: self.refresh_market15()
            collected=[]
            with self.lock: self.progress['total']=len(stocks)
            with ThreadPoolExecutor(max_workers=self.config['workers'],thread_name_prefix='scan') as pool:
                futures={pool.submit(self.scan_stock,s,timeframes,cutoff,force):s for s in stocks}
                for future in as_completed(futures):
                    stock=futures[future]
                    try:
                        collected.extend(future.result())
                    except Exception as exc:
                        LOG.exception('Source read failed for %s',stock['symbol'])
                        for tf in timeframes:
                            collected.append((stock['symbol'],tf,{'symbol':stock['symbol'],'timeframe':tf,'status':'error','error':str(exc),'bar_count':0,'last_candle':None,'matches':[],'bars':[],'signature':''}))
                    with self.lock:
                        self.progress['done']+=1
                        self.progress['symbol']=stock['symbol']
                        done=self.progress['done']
                    if done%100==0: LOG.info('Scanned %s/%s equities in %.1fs',done,len(stocks),time.monotonic()-began)
            finished=now_ist().isoformat(sep=' ')
            meta={**self.metadata,'finished_at':finished,'duration_seconds':round(time.monotonic()-began,2),
                  'pattern_version':self.config['pattern_version'], 'universe':len(stocks),'timeframes':dict(self.metadata.get('timeframes',{}))}
            for tf in timeframes:
                cells=[c for _,t,c in collected if t==tf]
                meta['timeframes'][tf]={'finished_at':finished,'as_of':max((c.get('last_candle') or '' for c in cells),default=''),
                    'scanned':sum(c['status']=='scanned' for c in cells),'insufficient':sum(c['status']=='insufficient_history' for c in cells),
                    'missing':sum(c['status']=='no_complete_data' for c in cells),'errors':sum(c['status']=='error' for c in cells),
                    'matches':sum(len(c['matches']) for c in cells)}
            # One transaction replaces the full selected-timeframe snapshot, including zero matches.
            # Readers continue seeing the previous complete snapshot until this transaction commits.
            with self.connection() as con:
                con.executemany('DELETE FROM cells WHERE timeframe=?',[(tf,) for tf in timeframes])
                con.executemany('INSERT INTO cells VALUES(?,?,?,?)',[(s,tf,c['signature'],dumps(c)) for s,tf,c in collected])
                con.execute("INSERT OR REPLACE INTO meta VALUES('snapshot',?)",(dumps(meta),))
                if self.pattern_set==pattern_live.RESEARCH:
                    # Instance ledger: upsert, never replace. A detection keeps the moment it
                    # first appeared, which is what makes per-instance outcome tracking
                    # possible later; only its state and as-of move.
                    written=pattern_live.persist(con,[m for _,_,c in collected for m in c['matches']],finished)
                    meta['detections_persisted']=written
                    # Terminated instances age out; the live book never does.
                    meta['detection_retention']=pattern_live.prune(con,now_ist(),self.config)
                    meta['detection_states']=pattern_live.live_counts(con)
                    con.execute("INSERT OR REPLACE INTO meta VALUES('snapshot',?)",(dumps(meta),))
            with self.lock:
                active={s['symbol'] for s in stocks}
                self.cells={k:v for k,v in self.cells.items() if k[0] in active and k[1] not in timeframes}
                self.cells.update({(s,tf):c for s,tf,c in collected})
                self.stocks=stocks
                self.stock_map={s['symbol']:s for s in stocks}
                self.metadata=meta
            LOG.info('Snapshot committed: %s matches, %.1fs',sum(len(c['matches']) for _,_,c in collected),time.monotonic()-began)
        except Exception as exc:
            LOG.exception('Market scan failed; previous complete snapshot retained')
            with self.lock: self.progress['error']=str(exc)
        finally:
            with self.lock: self.progress['running']=False

    def behind(self,closes):
        """Timeframes whose committed snapshot is older than their latest expected close."""
        stored=self.metadata.get('timeframes',{})
        return [tf for tf,expected in closes.items()
                if expected and (stored.get(tf,{}).get('as_of') or '')<expected]

    def watch(self,initial_scan=True):
        def loop():
            # legacy: data_version changes on commits from other connections, including corrections.
            # market15: the newest completed bar_start, which the live ingest loop advances every
            # cycle. data_version is useless there -- the seeder/repair workers commit constantly.
            with connect_source(self.source) as con:
                version=con.execute('PRAGMA data_version').fetchone()[0] if self.candle_source==LEGACY else self.probe_market15()
                closes={tf:self.calendar.latest_close(tf,now_ist()-timedelta(seconds=self.config['settlement_delay_seconds'])) for tf in self.config['timeframes']}
                if initial_scan:self.request_scan()
                while not self.stop.wait(self.config['poll_seconds']):
                    if self.progress['running']: continue
                    new_version=con.execute('PRAGMA data_version').fetchone()[0] if self.candle_source==LEGACY else self.probe_market15()
                    new_closes={tf:self.calendar.latest_close(tf,now_ist()-timedelta(seconds=self.config['settlement_delay_seconds'])) for tf in self.config['timeframes']}
                    due=[tf for tf in closes if closes[tf]!=new_closes[tf]]
                    if new_version!=version:
                        # Legacy: a source commit can change anything, so everything is rescanned.
                        # Research: a full four-timeframe pass costs minutes (docs/LIVE_DETECTION.md
                        # section 7) and market15 advances every 15 minutes, so the source advance
                        # only rescans the timeframes that are actually behind their latest expected
                        # close. 1H/4H therefore run intraday and 1D/1W after the close; POST
                        # /api/scan still forces the full pass.
                        due=(list(self.config['timeframes']) if self.pattern_set!=pattern_live.RESEARCH
                             else sorted({*due,*self.behind(new_closes)}))
                        LOG.info('%s source advanced to %s; rescanning %s',self.candle_source,new_version,due or 'nothing')
                    if due and self.request_scan(due):
                        version=new_version
                        closes=new_closes
        threading.Thread(target=loop,daemon=True,name='candle-scheduler').start()
