"""Workspace templates — starting points, not fixed layouts. Each is an ordinary workspace definition, validated like
any other; picking one copies it into a new workspace the user then changes freely.

A template lists widgets IN READING ORDER with a size class (S compact · M standard · L wide · F a row to itself).
It does not lay out rows: the layout engine (src/workbench/layout.ts) composes them for the screen it opens on, so
the same template is a clean 2 x 2 on a laptop and one row on a wide monitor. Scanners are referenced by the KANIDA
default scanner ids (kanida_pilot/screener/defaults.py), which are stable slugs."""
from __future__ import annotations
from .model import widget as W

TEMPLATES=[
 ('options-trader','Options Trader','Screener results, option chain, 15-min signal and the AI summary.',
  {'selected':{'underlying':'NIFTY'},'widgets':[
   W('screener_results','M',scanner_id='call-oi-building'),W('option_chain','M'),W('signal','M'),W('ai_summary','L')]}),
 ('index-derivatives','Index Derivatives','An index scanner, OI, IV, PCR, max pain and the option chain.',
  {'selected':{'underlying':'NIFTY'},'widgets':[
   W('index_dashboard','M'),W('option_chain','M'),W('pcr','S'),W('max_pain','S'),
   W('oi_session','L'),W('iv','L')]}),
 ('intraday-scanner','Intraday Scanner','A scanner builder and its results, the price chart, the 15-min signal and alerts.',
  {'selected':{'underlying':'NIFTY'},'widgets':[
   W('screener_builder','L'),W('screener_results','M',scanner_id='call-premium-surge'),W('alerts','S'),
   W('price_chart','L'),W('signal','M')]}),
 ('volatility-watch','Volatility Watch','An IV scanner, the IV chart, the option chain and the AI summary.',
  {'selected':{'underlying':'NIFTY'},'widgets':[
   W('screener_results','M',scanner_id='call-iv-expanding'),W('iv','L',settings={'view':'chart'}),
   W('greeks','M'),W('option_chain','M'),W('ai_summary','L')]}),
 ('blank','Blank workspace','Nothing yet — add the widgets you want.',{'selected':{'underlying':'NIFTY'},'widgets':[]}),
]
