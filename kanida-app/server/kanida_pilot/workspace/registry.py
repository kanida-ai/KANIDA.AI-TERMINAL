"""The widget registry: every widget type the workspace offers, what it shows, and the settings it accepts.

ONE LIST. The Add-widget library is drawn from this file (served by /api/workspace/registry); every saved widget
is validated against it, so a workspace is a structured definition rather than a bag of coordinates, and a
setting a widget does not have can never be stored on it. Every entry is backed by a route that already serves
real data — nothing here exists to make the library look bigger (docs/WORKSPACE_LAYOUT.md §3).

  instrument  True  = reads ONE instrument: it follows the workspace selection, or is pinned to its own.
              False = a list or a scanner; it has no instrument, and it is what SETS the workspace selection.
  settings    {name: allowed values}. `expiry` is special: 'follow' or a YYYY-MM-DD date.
"""
from __future__ import annotations

# size CLASSES, not pixel widths: a widget's share of its row (S 1 · M 2 · L 3) and its minimum width;
# F takes a row to itself. The layout engine is src/workbench/layout.ts.
SIZES={'S':1,'M':2,'L':3,'F':0}
HEIGHTS=('short','tall')
CATEGORIES=['Screeners','Options','Charts','Intelligence','Market']

#: 'auto' = fit to the tile: both panels side by side when there is room, the chart alone when there is not
VIEW3=['auto','both','chart','readings']

WIDGETS={
 # --- screeners: these drive the workspace ---------------------------------------------------------------
 'screener_results':dict(label='Screener results',category='Screeners',instrument=False,size='M',height='tall',
  blurb='Instruments matching a scanner, with their lifecycle. Click one to point the workspace at it.',
  settings={'scanner_id':'scanner','view':['active','ended','all']},source='/api/screener/scanners/{id}/results'),
 'screener_builder':dict(label='Screener builder',category='Screeners',instrument=False,size='L',height='tall',
  blurb='Describe a behaviour in words or with chips, run it, and pick from what it finds.',
  settings={},source='/api/screener/parse · describe · run'),
 'scanners':dict(label='Scanners',category='Screeners',instrument=False,size='S',height='tall',
  blurb='KANIDA scanners and your own. Opens one in a Screener results widget.',
  settings={'list':['all','kanida','mine']},source='/api/screener/scanners'),
 'market_scan':dict(label='Market scan',category='Screeners',instrument=False,size='M',height='tall',
  blurb='What every instrument is doing at this 15-min reading — the Derivative tab\'s own list.',
  settings={},source='/api/derivatives/screener'),
 # --- options -----------------------------------------------------------------------------------------------
 'option_chain':dict(label='Option chain',category='Options',instrument=True,size='M',height='tall',
  blurb='Calls and puts by strike at the reading on screen, opened at the money.',
  settings={'expiry':'expiry'},source='/api/derivatives/chain'),
 'oi_by_strike':dict(label='OI by strike',category='Options',instrument=True,size='L',height='tall',
  blurb='Where call and put open interest stands, strike by strike.',
  settings={'expiry':'expiry'},source='/api/derivatives/oi-by-strike'),
 'oi_session':dict(label='OI through the session',category='Charts',instrument=True,size='L',height='tall',
  blurb='ΔOI of the ten at-the-money contracts at every reading, beside the futures chart.',
  settings={'expiry':'expiry'},source='/api/derivatives/oi-grid'),
 'iv':dict(label='IV',category='Options',instrument=True,size='L',height='tall',
  blurb='At-the-money implied volatility through the session, and by strike. COMPUTED.',
  settings={'expiry':'expiry','view':VIEW3},source='/api/derivatives/iv-series'),
 'pcr':dict(label='PCR',category='Options',instrument=True,size='S',height='tall',
  blurb='Put-call ratio by open interest and by volume, at every reading.',
  settings={'expiry':'expiry','view':VIEW3},source='/api/derivatives/pcr-series'),
 'max_pain':dict(label='Max pain',category='Options',instrument=True,size='S',height='tall',
  blurb='The max-pain strike against spot, through the session.',
  settings={'expiry':'expiry','view':VIEW3},source='/api/derivatives/maxpain-series'),
 'greeks':dict(label='Greeks',category='Options',instrument=True,size='M',height='short',
  blurb='IV, delta and gamma by strike near the money at the latest reading. COMPUTED (Black-Scholes-Merton).',
  settings={'expiry':'expiry','atm':['2','3','5']},source='/api/workspace/greeks'),
 'key_strikes':dict(label='Key strikes',category='Options',instrument=True,size='S',height='short',
  blurb='The strikes carrying the activity at the latest reading, and what each is doing.',
  settings={},source='/api/workspace/key-strikes'),
 # --- charts -----------------------------------------------------------------------------------------------
 'price_chart':dict(label='Price chart',category='Charts',instrument=True,size='L',height='tall',
  blurb='Front-month futures candles, 15-min or daily.',
  settings={},source='/api/derivatives/futures-chart'),
 'volume':dict(label='Volume',category='Charts',instrument=True,size='M',height='short',
  blurb='Call and put volume traded in each 15-min interval of the session.',
  settings={'expiry':'expiry'},source='/api/workspace/volume'),
 # --- intelligence -----------------------------------------------------------------------------------------
 'signal':dict(label='15-min signal',category='Intelligence',instrument=True,size='M',height='tall',
  blurb='The market state at the latest reading and how it got there.',
  settings={},source='/api/derivatives/oi-grid + pcr / max pain / iv'),
 'ai_summary':dict(label='AI summary',category='Intelligence',instrument=True,size='L',height='tall',
  blurb='What matters across the widgets on this instrument: changed, where, persistent, conflicting, strikes, unusual.',
  settings={'scope':['connected','all']},source='/api/workspace/summary'),
 'session_history':dict(label='Session history',category='Intelligence',instrument=True,size='M',height='tall',
  blurb='Every 15-min reading of the session, one row each.',
  settings={},source='/api/derivatives/oi-grid'),
 'signal_noise':dict(label='Signal-to-noise',category='Intelligence',instrument=False,size='S',height='short',
  blurb='How the session\'s readings held up afterwards: signal, noise and pending, by type.',
  settings={},source='/api/derivatives/signal-noise'),
 # --- market -----------------------------------------------------------------------------------------------
 'futures_buildup':dict(label='Futures build-up',category='Market',instrument=True,size='L',height='tall',
  blurb='Futures open interest and basis through the session.',
  settings={'view':VIEW3},source='/api/derivatives/futures-buildup'),
 'index_dashboard':dict(label='Index dashboard',category='Market',instrument=False,size='M',height='tall',
  blurb='Every index underlying at a glance. Click one to point the workspace at it.',
  settings={},source='/api/derivatives/indices'),
 'futures_list':dict(label='Futures',category='Market',instrument=False,size='M',height='tall',
  blurb='Front futures of every underlying: price, OI and build-up.',
  settings={},source='/api/derivatives/futures'),
 'watchlist':dict(label='Watchlist',category='Market',instrument=False,size='S',height='tall',
  blurb='The symbols on your watchlist. Click one with F&O data to point the workspace at it.',
  settings={},source='/api/product'),
 'alerts':dict(label='Alerts',category='Market',instrument=False,size='S',height='tall',
  blurb='Your screener alerts — only changes, never repeats.',
  settings={},source='/api/screener/alerts'),
}


#: A widget whose CONTENT needs more width than its size class gives it (a chain's columns, a strike grid). The
#: layout engine never lays it out narrower than this; everything else uses its class minimum (S 250 · M 300 · L 400).
MIN_WIDTH={'option_chain':420,'oi_by_strike':440,'oi_session':520,'market_scan':360,'greeks':340,'screener_builder':440,
 'index_dashboard':340,'futures_list':360,'price_chart':420}


def catalogue():
 return {'categories':CATEGORIES,'sizes':SIZES,'heights':list(HEIGHTS),
  'widgets':[{'type':k,**{x:v for x,v in w.items()},'min_width':MIN_WIDTH.get(k,0)} for k,w in WIDGETS.items()]}
