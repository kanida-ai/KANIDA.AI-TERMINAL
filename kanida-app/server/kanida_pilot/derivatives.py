"""The Derivative tab's reader: `db/derivatives.db`, READ-ONLY (docs/DERIVATIVES_SPEC.md §2-§4).

Boundary
--------
This module **never writes, never captures and never computes a signal**. The capture worker (D1) writes
`contracts`/`snapshots`/`candles_15m`/`underlying_snapshots`; the metrics worker (D2) writes `metrics` - the
§3 signals. Everything here is a SELECT plus presentation (rounding, ₹ crore, a sort, a group-by for the §3
roll-up). The connection is opened `mode=ro` with `PRAGMA query_only`, exactly like `detections.py`.

Where the numbers come from
---------------------------
`market_data/derivatives/metrics.py` (D2) owns the definitions. When that module is importable and exposes a
reader for a card (`METRIC_READERS`), it is called and its rows are served as-is. Until it lands, the same
rows are read from the `metrics` table D2 writes, which is the shape §2/§3 fixes. Which of the two answered
is stated in every response as `source`, so nothing here can quietly look computed when it was read, or the
other way round.

Because the exact column NAMES in `metrics` are D2's to choose, the reader is column-driven: `FIELDS` maps
each §3 signal to the names it may carry, and a signal whose column is absent is served as `null` -> the card
prints "no baseline" or a dash. A missing column can therefore never turn into a zero, and `missing` on every
response names what could not be found.

Honesty rules enforced here (§3.2, §5)
--------------------------------------
* A volume-versus-average ratio is served ONLY with at least `MIN_BASELINE_SESSIONS` sessions behind it.
  Fewer (or an unknown count) -> `volume_ratio: null`, `volume_baseline: "none"`. Never a ratio.
* The liquidity floors of §3 are applied to every "unusual" list and are returned with it, so the UI can
  state the floors in force on the card itself.
* Nothing is predicted. Every field describes a state that has already been captured, with its `as_of`.

Degradation: a missing file, a missing table or a locked database all report `available: False` with
`EMPTY_TEXT` - "No F&O data captured yet - capture starts at the next 15-min reading" - never an error page.
"""
from __future__ import annotations
import logging,re,sqlite3,threading,time
from . import session_events
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from . import implied_vol as IV

LOG=logging.getLogger('pilot.derivatives')
IST=timezone(timedelta(hours=5,minutes=30))
QUERY_TIMEOUT=8.0

#: §3 liquidity floors. Constants in code, shown in the UI (the response carries them).
FLOOR_PREMIUM_CR=2.0
FLOOR_OI_LOTS=1
FLOOR_LAST_PRICE=1.0
FLOORS={'premium_cr':FLOOR_PREMIUM_CR,'oi_lots':FLOOR_OI_LOTS,'last_price':FLOOR_LAST_PRICE}
# ==================================================================================================================
# A FLOOR CAN ONLY BE APPLIED WHERE THE NUMBER IT RESTS ON WAS MEASURED.
#
# The three §3 floors rest on three different captured numbers. Premium traded is volume × the exchange's own
# traded average price, and a 15-minute candle carries no traded average at all - so a session rebuilt from
# candles has `premium_cr` null on EVERY row. Applying the premium floor there does not filter the reading, it
# deletes it: at the 15:30 reading of 18 Sep 2026 the store held 10,510 contract rows with a real last price, a
# real volume and a real open interest, and the screen said "no contract cleared the liquidity floors" - which a
# trader reads as a quiet market. One unmeasurable derived field was hiding four hours of measured ones.
#
# So the floors DEGRADE rather than hide. At a reading where premium was never captured the screener applies the
# floors it CAN apply, keeps the rows, and says on the card which floors are in force and why the third is not.
# What it never does is substitute a number. The store carries an ESTIMATED traded average beside the real one,
# and it is not read here on purpose - the floors decide what a reader is shown, and a number that decides
# visibility has to be one the exchange reported rather than one a model produced. That column's own NAME is
# kept out of this file so a test can assert its absence; `test_derivatives_series.py` does exactly that. A
# degraded floor set is announced on screen, never quietly relaxed, and at a reading where premium IS measured
# nothing about any of this changes.
# ==================================================================================================================
#: Each §3 floor and the `FIELDS` signal it is measured on. The name here is the SIGNAL, which the store may
#: spell differently (a store with `premium_inr` and no `premium_cr` still measures premium); `_floors_in_force`
#: resolves it through the same map every card reads by. The OI floor is compared against the contract's own lot
#: size, which is contract metadata rather than a measurement, so it is not part of what makes the floor
#: measurable - a row missing it fails the floor in `_passes`, exactly as it always did.
FLOOR_COLUMNS={'premium_cr':('premium_cr',),'last_price':('last_price',),'oi':('oi','lot_size')}
#: The order the floors are named in, in text and in `floors_applied`.
FLOOR_ORDER=('premium_cr','oi','last_price')
FLOOR_PHRASES={'premium_cr':f'premium traded ≥ ₹{FLOOR_PREMIUM_CR:g} cr',
 'oi':f'OI ≥ {FLOOR_OI_LOTS} lot','last_price':f'last price ≥ ₹{FLOOR_LAST_PRICE:g}'}
#: Why each one could not be applied. One sentence per floor, because "premium was never captured" and "open
#: interest was never captured" are different facts about the capture and must not share a wording.
FLOOR_UNMEASURED_TEXT={
 'premium_cr':("The premium floor could not be applied at this 15-min reading. Premium traded is volume × the "
  "exchange's own traded average price, and no traded average was captured here - this reading was rebuilt "
  "from 15-minute candles, and a candle carries none. It cannot be recovered after the fact. The rows below "
  "were gated on the floors that COULD be measured; they were never measured against a premium floor and did "
  "not fail one."),
 'oi':('The open-interest floor could not be applied at this 15-min reading: no open interest was captured '
  'here, so no contract in it was measured against one.'),
 'last_price':('The last-price floor could not be applied at this 15-min reading: no last price was captured '
  'here, so no contract in it was measured against one.'),
}
#: The sentence when all three are in force, which is every reading the capture reached. It stays a literal,
#: not a call: `scripts/check-derivative.cjs` reads this declaration out of the source to assert the wording the
#: reader gets, and `floors_sentence` returns this very object for the all-three case so the two cannot drift.
FLOORS_TEXT=(f'Liquidity floors in force: premium traded ≥ ₹{FLOOR_PREMIUM_CR:g} cr, '
 f'OI ≥ {FLOOR_OI_LOTS} lot, last price ≥ ₹{FLOOR_LAST_PRICE:g}.')
#: What is said when not one of them could be applied - a reading with no premium, no price and no open
#: interest. It is not "no floors": it is that nothing here was measured against anything.
FLOORS_NONE_TEXT=('No liquidity floor could be applied at this 15-min reading: none of the numbers they rest '
 'on was captured.')
def floors_sentence(applied=None):
 """The floors line for ONE reading: the floors actually in force, named.

 Served as `floors_text` so the card states the floors it was gated on rather than the three constants. A
 reader who is shown a list gated on two floors must never read a sentence that names three.
 """
 keys=[k for k in FLOOR_ORDER if applied is None or k in set(applied)]
 if len(keys)==len(FLOOR_ORDER):return FLOORS_TEXT
 if not keys:return FLOORS_NONE_TEXT
 return 'Liquidity floors in force: '+', '.join(FLOOR_PHRASES[k] for k in keys)+'.'
#: What the screener ranks by when premium cannot be measured. Ordering by a column that is null on every row
#: is not an ordering at all - it is insertion order wearing a label. Volume IS captured at a rebuilt reading
#: (a candle carries traded quantity), so that is what the list is ranked by, and it says so.
FALLBACK_RANK_FIELD='volume'
FALLBACK_RANK_LABEL='Most contracts traded'
FALLBACK_RANK_TEXT=('Premium traded was not captured at this 15-min reading, so it cannot order anything here. '
 'This list is ranked by volume - contracts traded, which WAS captured.')
#: Which 15-min reading the screener opens on. It is the newest reading that has contracts over the floors it
#: can apply THERE. Before the floors degraded, a rebuilt afternoon cleared nothing at all and the whole tab
#: fell back to the last live reading - 11:30 on 18 Sep 2026, four hours behind the session's own close. Now a
#: rebuilt reading is usable on the floors that were measured and the tab opens on it, with the floor that was
#: not applied named on the card.
READING_RULE_TEXT=('This screener opens on the newest 15-min reading that has contracts over the liquidity '
 'floors it can apply at that reading. Not every floor can be applied at every reading: a reading rebuilt '
 'from 15-minute candles carries no traded-price average, so no contract in it has a premium traded and the '
 'premium floor is not applied there. The floors actually in force are named on the card. Every reading the '
 'store holds is still listed, and choosing one moves the whole tab to it.')
#: §3.2: fewer than 3 sessions of history is "no baseline", never a ratio.
MIN_BASELINE_SESSIONS=3
# ==================================================================================================================
# WHAT "UNUSUAL" IS: TWO RULES, NOT A HUNDRED SENTENCES.
#
# The store flags a contract under §3 and writes WHY as prose — "volume 206.0x its own time-of-day median". That
# sentence is different at every number, so NIFTY's 80 flagged contracts at the 11:30 reading of 18 Sep 2026 wrote
# 124 DIFFERENT sentences. Counting those sentences said "124 conditions"; the tab clamped that to three and drew
# "3 conditions". The truth is TWO: §3.2 and §3.3, fired at many multiples.
#
# So a trigger is carried as WHAT FIRED and WHAT THE NUMBERS WERE, never as the sentence:
#
#     rule id · rule version · the measured value · the comparator · the threshold · the baseline it was
#     measured against · how many observations that baseline stands on.
#
# These definitions belong to `market_data.derivatives.metrics` (D2), which computes them; the pilot runs with
# PYTHONPATH=server and usually cannot import it, so the registry is MIRRORED here and check-derivative.cjs reads
# both files and fails if the ids, comparators or thresholds ever differ.
#
# NOTHING IS RECOMPUTED HERE. Which rules fired is the store's own `unusual_reasons`, classified back to the rule
# that WROTE each sentence; the numbers beside them are the store's own columns (`vol_tod_ratio`, `vol_tod_median`,
# `vol_tod_sessions`, `vol_oi_ratio`, `vol_oi_prev_oi`). A sentence no rule claims is kept as itself under
# `unclassified` — observable, never folded into a rule that did not fire and never silently dropped.
# ==================================================================================================================
#: Bumped when a rule's measurement, comparator or threshold changes.
UNUSUAL_RULES_VERSION=1
#: The multiples the two rules compare against. Mirrors metrics.UNUSUAL_VOL_TOD_RATIO / VOL_OI_SPIKE_RATIO.
UNUSUAL_VOL_TOD_RATIO=2.0
VOL_OI_SPIKE_RATIO=1.0
RULE_VOL_TOD='vol_tod_median'
RULE_DAY_VOL_VS_PREV_OI='day_vol_vs_prev_oi'
RULE_UNCLASSIFIED='unclassified'
UNUSUAL_RULES=(
 {'rule_id':RULE_VOL_TOD,'rule_version':UNUSUAL_RULES_VERSION,
  'label':'Volume vs its own median','short_label':'Vol vs median',
  'measure':'cumulative volume so far today',
  'baseline_label':"the median of this contract's own cumulative volume at the same clock time",
  'sample_label':'session','comparator':'>=','threshold':UNUSUAL_VOL_TOD_RATIO,'unit':'x',
  'text':('§3.2. A contract is flagged when the volume it has traded so far today is at least '
   f'{UNUSUAL_VOL_TOD_RATIO:g}x the median of its OWN cumulative volume at the same clock time over the '
   f'baseline sessions. Fewer than {MIN_BASELINE_SESSIONS} sessions of history is no baseline, so no flag.')},
 {'rule_id':RULE_DAY_VOL_VS_PREV_OI,'rule_version':UNUSUAL_RULES_VERSION,
  'label':'Day volume vs previous-close OI','short_label':'Day vol vs prev OI',
  'measure':"the day's volume",
  'baseline_label':'the open interest standing at the previous close',
  'sample_label':'prior session','comparator':'>','threshold':VOL_OI_SPIKE_RATIO,'unit':'x',
  'text':('§3.3. A contract is flagged when the volume it has traded today is more than '
   f'{VOL_OI_SPIKE_RATIO:g}x the open interest that was standing at the previous close. One prior session is '
   'the whole baseline.')},
)
UNUSUAL_RULES_BY_ID={rule['rule_id']:rule for rule in UNUSUAL_RULES}
#: The one unrecognised case, named rather than hidden. It is not a rule and it is never given a threshold.
UNUSUAL_UNCLASSIFIED_RULE={'rule_id':RULE_UNCLASSIFIED,'rule_version':UNUSUAL_RULES_VERSION,
 'label':'Condition this build does not name','short_label':'Unnamed condition',
 'measure':'','baseline_label':'','sample_label':'',
 'comparator':'','threshold':None,'unit':'',
 'text':('The store recorded a condition in words that no rule in this build writes. It is shown in the '
  'store\'s own words and counted on its own, never added to one of the named rules.')}
#: How each rule's sentence is read back. The pattern lives beside the rule it belongs to, exactly as it does in
#: `market_data.derivatives.metrics`, so the two directions cannot drift apart.
UNUSUAL_REASON_PATTERNS=(
 (RULE_VOL_TOD,re.compile(r'^volume\s+([0-9.]+)x its own time-of-day median$',re.I)),
 (RULE_DAY_VOL_VS_PREV_OI,re.compile(r"^day volume\s+([0-9.]+)x yesterday's OI$",re.I)),
)
#: THE ORDER THE SCREENER SERVES, written down once and served with the rows so the page can print the sort that
#: is actually in force rather than a sort somebody once assumed. Every key is a number the store produced, and
#: the last key is the instrument NAME — so two instruments that match on all three counts still come out in the
#: same order on every request, on every machine.
SCREENER_RANK_KEYS=(
 {'field':'unusual_rule_count','direction':'desc',
  'text':'How many DISTINCT condition types the instrument\'s contracts tripped between them — not how many '
   'different numbers appeared.'},
 {'field':'unusual','direction':'desc','text':'Then how many of its contracts tripped any of them.'},
 {'field':'premium_cr','direction':'desc','text':'Then the premium traded across its whole book, largest first.'},
 {'field':'underlying','direction':'asc',
  'text':'Then the instrument name, A to Z, so two instruments level on all three still come out in the same '
   'order every time.'},
)
SCREENER_RANK_LABEL='Unusual first'
SCREENER_RANK_TEXT=('One row per instrument, ordered by: most distinct condition types, then most contracts '
 'flagged, then largest premium traded, then instrument name A to Z.')
#: The contract list is a different list with a different order, and it says so rather than borrowing the above.
CONTRACT_RANK_KEYS=({'field':'premium_cr','direction':'desc','text':'Largest premium traded first.'},)
CONTRACT_RANK_LABEL='Largest premium traded'
CONTRACT_RANK_TEXT='One row per contract, largest premium traded first.'
#: The one empty-state sentence for the whole tab. Not an error - capture simply has not run yet.
EMPTY_TEXT='No F&O data captured yet — capture starts at the next 15-min reading'
#: §4 card 4 names these three exactly.
INDEX_UNDERLYINGS=('NIFTY','BANKNIFTY','FINNIFTY')
#: Watch-list groups offered by the filter panel. "all" is every captured underlying.
WATCHLISTS=(('all','All underlyings'),('indices','Indices'))
OPTION_TYPES=('CE','PE')
BUILDUP_LABELS={'long_buildup':'Long build-up','short_buildup':'Short build-up',
 'short_covering':'Short covering','long_unwinding':'Long unwinding'}
#: Rupees per crore. The only unit conversion this module performs (§3.4 asks for ₹ crore).
CRORE=1e7
ROW_LIMIT_DEFAULT=60
ROW_LIMIT_MAX=500
SERIES_LIMIT_MAX=400

# --- the futures price chart (between the unusual screener and the ΔOI grid) -----------------------------------
#: The two cadences the chart offers. 15 minutes is the default; daily is the alternative offered at the top.
INTERVAL_15M='15m'
INTERVAL_1D='1d'
CHART_INTERVALS=(INTERVAL_15M,INTERVAL_1D)
DEFAULT_CHART_INTERVAL=INTERVAL_15M
#: (table, time column) per interval. `candles_day` holds the VENDOR'S OWN daily bar for the same contract,
#: written by `market_data.derivatives.backfill.run_daily_backfill`. It is deliberately not `daily_rollups`,
#: which is the capture worker's roll-up of its own 15-minute bars and is rebuilt from them.
CHART_TABLES={INTERVAL_15M:('candles_15m','bar_start'),INTERVAL_1D:('candles_day','session_date')}
INTERVAL_LABELS={INTERVAL_15M:'15 minutes',INTERVAL_1D:'Daily'}
#: One response is capped here. A futures contract lives about three months - roughly 1,560 fifteen-minute bars
#: and about 60 sessions - so this ceiling never cuts into a contract's own history.
CHART_MAX_CANDLES=2000
#: Fewer distinct trading sessions than this and the series is too short to read as a trend. It says so in plain
#: words rather than drawing a stub that looks broken: when the front contract rolls, the new one starts with
#: almost no history of its own, and that is a fact about the contract, not a fault in the data.
SHORT_HISTORY_SESSIONS=10
#: What the chart is, stated on itself. It is one contract's own candles - never a stitched continuous series
#: and never the underlying index.
CHART_TEXT=("This is the front futures contract's own price history. It is not a stitched continuous series "
 'and it is not the underlying index, so it begins on the day the exchange listed the contract.')
#: The gap rule, stated where the reader can see it. A reading the exchange HAD, that this contract has no bar
#: for, keeps its slot with empty prices - so the chart cannot close the hole up and draw two readings side by
#: side that were never side by side.
GAPS_TEXT=('A 15-min reading or a session this contract has no bar for keeps its place on the chart with no '
 'candle drawn. The hole is never closed up and no bar is carried forward into it.')
#: What the store can and cannot testify to, said plainly rather than implied. The session grid is the union of
#: the readings every contract in the same table holds: a reading exists there only because something traded at
#: it. A session missing from the WHOLE store therefore cannot be told apart from a day the exchange was shut,
#: and is not reported as a gap.
GRID_SOURCE_TEXT=('Which readings the exchange had is taken from the store itself \u2014 the readings every other '
 'contract holds. A session missing from every contract reads as no session at all, not as a gap.')

# --- the ΔOI strike grid (the owner's 2 × 5 block) ------------------------------------------------------------
#: Four strikes either side of the money: ATM CE and ATM+1..+4 CE on the calls row, ATM PE and ATM−1..−4 PE on the
#: puts row. Exactly ten slots, always in this order, so a strike that is not listed reads as a named gap rather
#: than shifting the grid.
GRID_WIDTH=4
GRID_SLOTS=2*(GRID_WIDTH+1)
#: The market-wide events pass is held for this long. A captured reading never changes; a request with no
#: `at` resolves to the newest the store holds, and new marks land through the session — so it is seconds,
#: not the session. Well inside the 15-minute cadence either way.
EVENTS_CACHE_TTL=60.0
EVENTS_CACHE_MAX=24
#: "the latest mark versus four marks ago" — one hour of 15-minute marks.
DIRECTION_LOOKBACK_MARKS=4
#: |change| under this fraction of the contract's OWN largest |ΔOI| today is flat, not a direction.
FLAT_FRACTION=0.05
#: The same 5% shape, applied to the contract's OWN premium: |price change over the window| under this fraction
#: of its largest |price move from the day's first reading| today is flat, not a direction. Kept as its own
#: constant (rather than reusing FLAT_FRACTION) so the check script can read BOTH sides and compare them.
PRICE_FLAT_FRACTION=0.05
#: The block-level read: both rows building, and neither side's total |ΔOI change| over the window more than
#: roughly a third larger than the other's. Above this ratio the two sides are not "similar" and nothing is said.
BLOCK_BALANCE_RATIO=1.33
#: The capture worker's own name for a 15-minute bar-close mark (market_data/derivatives/config.MARK_BAR_CLOSE).
#: Applied only when the store actually carries the column, so a store written before it existed still reads.
MARK_BAR_CLOSE='bar_close'
#: The grid's own empty sentence. Not an error: two marks simply have not been captured yet.
NOT_ENOUGH_MARKS='Not enough readings captured yet — the first line appears after two 15-min readings'
#: The three definitions the block states on itself. Stated, never re-invented per widget.
DELTA_OI_TEXT=('ΔOI is open interest added or removed since the previous close. Every line starts at 0 at the '
 'first 15-min reading of the day.')
ATM_TEXT=('ATM is the listed strike nearest spot in the front expiry. ATM+n is n strikes above spot, ATM−n is n '
 'strikes below.')
DIRECTION_TEXT=(f'Direction is the latest ΔOI against the reading {DIRECTION_LOOKBACK_MARKS} back (one hour). Flat '
 f'when the change is under {FLAT_FRACTION:.0%} of that contract\'s own largest ΔOI today. Fewer than two readings '
 'is "no baseline" and carries no direction at all.')
#: What a slot's direction may say. "no baseline" is a state, not a fourth direction.
GRID_DIRECTIONS=('building','flat','unwinding','no baseline')
#: What the contract's OWN premium may say over the same window. Same shape, same "no baseline" state.
GRID_PRICE_DIRECTIONS=('up','down','flat','no baseline')

# --- price and OI read together (the owner's two lines per tile) ----------------------------------------------
#: (option type, price direction, OI direction) -> (what is happening, what it means). Keyed as one string so
#: the check script can read this table out of the file and compare it with read_api.py and with logic.ts.
#: Nothing here says what happens next: each row names who appears to be doing what, right now.
#:
#: COMPLETE by construction: every one of the nine (price x OI) combinations is listed for each option type,
#: so there is no fall-through case for a reading to be collapsed into. The rule the check script enforces
#: over the whole table: when OI has a direction the sentence says something happened to open interest, and
#: when OI is flat the sentence says open interest barely moved. The chip and the sentence can then never
#: disagree. The four directional rows per type are the owner's own wording; the five rows that involve a
#: flat axis are shared, because a call and a put read the same way when one of the two numbers is still.
FLOW_LABELS={
 'CE|down|building':('Call writing increasing','Sellers are building resistance'),
 'CE|up|unwinding':('Call short covering','Call sellers are exiting'),
 'CE|up|building':('Call buying increasing','Traders are buying upside'),
 'CE|down|unwinding':('Call buyers exiting','Call buyers are closing out'),
 'CE|flat|building':('New positions added','Premium barely moved'),
 'CE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'CE|up|flat':('Premium rose','Open interest barely moved'),
 'CE|down|flat':('Premium fell','Open interest barely moved'),
 'CE|flat|flat':('Very little change','Positioning is unchanged'),
 'PE|down|building':('Put writing increasing','Sellers are building support'),
 'PE|up|unwinding':('Put short covering','Put sellers are exiting'),
 'PE|up|building':('Put buying increasing','Traders are buying downside protection'),
 'PE|down|unwinding':('Put buyers exiting','Put buyers are closing out'),
 'PE|flat|building':('New positions added','Premium barely moved'),
 'PE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'PE|up|flat':('Premium rose','Open interest barely moved'),
 'PE|down|flat':('Premium fell','Open interest barely moved'),
 'PE|flat|flat':('Very little change','Positioning is unchanged'),
}
#: ONLY when BOTH are flat. A tile whose OI moved never says this: the sentence under a tile must never
#: contradict the chip above it, and "positioning is unchanged" beside "↓ UNWINDING" is a false statement.
FLOW_FLAT_WHAT='Very little change'
FLOW_FLAT_MEANING='Positioning is unchanged'
#: Fewer than two readings carrying a price, or carrying a ΔOI. Never guessed at.
FLOW_NOT_ENOUGH='Not enough readings yet'
#: Every sentence a tile is allowed to show as "what is happening". Nothing else reaches the reader, whichever
#: module computed it - a delegate that drifts is recomputed here rather than served.
FLOW_WORDINGS=frozenset([w for w,_ in FLOW_LABELS.values()]+[FLOW_NOT_ENOUGH])
#: The one line under the whole 2 x 5 block, printed only when the test above passes. Nothing otherwise.
BLOCK_BOTH_BUILDING='Both sides building similarly — no clear directional edge'
#: The second line every tile now draws, stated on the block rather than implied.
PRICE_TEXT=('The dashed line on each tile is that contract\'s own last traded price at the same 15-min readings. '
 'It has its own scale — a rupee premium and a count of contracts share no units.')
#: The one sentence under the block that keeps the two-line reading honest. It is a reading of two numbers.
FLOW_TEXT=('Every opened contract has a buyer and a seller. These two lines read which side was paying up at each '
 '15-min reading — they do not say what happens next.')
#: What the block-level line is, and the tolerance it uses, stated where the reader can see it.
BLOCK_TEXT=(f'The line under the block appears only when the calls and the puts are both building over the same '
 f'window and neither side\'s total ΔOI change is more than {BLOCK_BALANCE_RATIO:g}x the other\'s. Otherwise '
 'nothing is said, because there is nothing to say.')
#: Underlyings and expiries arrive from the URL; they are matched against these before touching SQL.
SYMBOL_MAX=40
EXPIRY_LEN=10

# --- the session series: PCR, max pain, implied volatility, futures build-up ----------------------------------
# Every one of these is the SAME shape: one number per 15-minute reading of the newest captured session, oldest
# first, on the store's own reading grid, with one direction read over the last hour of it. The shape is
# written once here and reused, so four cards can never drift into four different definitions of "direction".
#
# The direction rule is the ΔOI grid's rule, applied to a series that does not start at zero: the latest
# reading against the reading DIRECTION_LOOKBACK_MARKS back (one hour); flat when |change| is under
# SERIES_FLAT_FRACTION of that series' OWN largest move away from its first reading today; fewer than two
# readings carrying a value is "no baseline", which is a state and never a fourth direction.
SERIES_FLAT_FRACTION=0.05
#: The words each series is allowed to use. The UI renders its own copy; these are the machine values it keys
#: on, and `direction_label` beside them is the owner's own wording for the same state.
DIRECTION_WORDS={
 'pcr':{'up':'rising','down':'falling','flat':'flat','none':'no baseline'},
 #: The owner's words, verbatim: "Shifting Up · Stable · Shifting Down".
 'max_pain':{'up':'shifting_up','down':'shifting_down','flat':'stable','none':'no baseline'},
 #: The owner's words, verbatim: "Expanding · Stable · Cooling".
 'iv':{'up':'expanding','down':'cooling','flat':'stable','none':'no baseline'},
 'oi':{'up':'building','down':'unwinding','flat':'flat','none':'no baseline'},
 'basis':{'up':'widening','down':'narrowing','flat':'flat','none':'no baseline'},
}
#: What each machine value is printed as. The UI owns its own copy; this travels with the response so the two
#: can be compared instead of guessed at.
DIRECTION_LABELS={'rising':'Rising','falling':'Falling','flat':'Flat','no baseline':'No baseline',
 'shifting_up':'Shifting Up','shifting_down':'Shifting Down','stable':'Stable',
 'expanding':'Expanding','cooling':'Cooling','building':'Building','unwinding':'Unwinding',
 'widening':'Widening','narrowing':'Narrowing'}
SERIES_DIRECTION_TEXT=(f'Direction is the latest reading against the reading {DIRECTION_LOOKBACK_MARKS} back '
 f'(one hour). Flat when the change is under {SERIES_FLAT_FRACTION:.0%} of that series\' own largest move away '
 'from its first reading today. Fewer than two readings carrying a value is "no baseline" and carries no '
 'direction at all. It describes what the number did, never what it does next.')
#: A reading the store holds for some OTHER underlying, and not for this one, keeps its slot with null values
#: and `gap: true`. The hole is never closed up and nothing is carried forward into it.
SERIES_GAPS_TEXT=('A 15-min reading this underlying has no row for keeps its place in the series with no '
 'value. The hole is never closed up and no reading is carried forward into it.')

# --- chain-level liquidity: when a chain figure is withheld rather than printed -------------------------------
#: §3 fixes the floors for a CONTRACT. A chain-level figure (PCR, max pain) rests on the whole chain, and the
#: spec does not name a floor for that, so this one is defined HERE and is stated on every response that uses
#: it. A chain of fewer than this many listed CE+PE contracts is too thin for a ratio or a payout minimum to
#: mean anything, and the figure is withheld with a reason instead of printed.
CHAIN_MIN_CONTRACTS=6
CHAIN_FLOORS={'min_contracts':CHAIN_MIN_CONTRACTS}
CHAIN_FLOORS_TEXT=(f'A chain-level figure is withheld when the chain carries fewer than {CHAIN_MIN_CONTRACTS} '
 'listed call and put contracts, or when one side of it has no open interest at all. This floor is defined by '
 'the server, not by the build spec, and the contract count the figure rests on travels with every reading.')
#: Why a reading's chain figure is blank. A withheld value ALWAYS carries one of these.
WITHHELD_REASONS={
 'thin_chain':f'Withheld: fewer than {CHAIN_MIN_CONTRACTS} listed contracts in this chain at this reading.',
 'one_side_empty':'Withheld: one side of this chain carried no open interest at this reading.',
 'not_computed':'The metrics worker did not write this figure for this reading.',
 'no_reading':'This underlying has no row at this 15-min reading.',
 'store_status':'The metrics worker marked this figure unusable for this reading.',
}
#: The two cadences the screener's build-up filter may read. §3.1 forbids mixing them in one label.
BUILDUP_WINDOWS=('15m','day')
#: The build-up labels the metrics worker writes, plus its own two non-labels. Served as `available` so the UI
#: can never offer a value the store does not hold.
BUILDUP_VALUES=('Long build-up','Short build-up','Short covering','Long unwinding','Flat','no data')
#: How far from spot a strike is still "at the money" for the screener's moneyness filter, as a fraction of
#: spot. Stated on the response; it is a serving convention, not an exchange definition.
MONEYNESS_BAND=0.005
MONEYNESS_VALUES=('itm','atm','otm')
MONEYNESS_TEXT=(f'A strike within {MONEYNESS_BAND:.1%} of spot at that reading counts as at the money. Above '
 'that band a call is out of the money and a put is in it, and below it the other way round.')
#: Which underlyings the screener calls an index. Kite does not flag this, so it is a named list here rather
#: than a guess, and it is served with the response so a reader can see exactly what it covers.
INDEX_KINDS=('NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50','NIFTYFPI')
UNDERLYING_KINDS=('index','stock')
#: The screener's own ceiling on returned rows. It always reports how many rows it SCANNED beside how many it
#: kept, so a cut list can never read as an empty market.
#: The screener's two views. ONE ROW PER UNDERLYING is the default: the contract list is sorted by
#: premium, and one busy index can own every visible row of it - 106 of the 491 contracts at 11:30 on
#: 18 Sep 2026 were NIFTY's, so a hundred-row list was a hundred rows of NIFTY.
#: Every instrument type the screener lists. A future is not an option and is not a side of one.
INSTRUMENT_TYPES=('CE','PE','FUT')
SCREENER_VIEWS=('underlying','contract')
SCREENER_VIEW_DEFAULT='underlying'
SCREENER_LIMIT_DEFAULT=100
SCREENER_LIMIT_MAX=500

#: What each series IS, in one sentence, carried on the response so the card never has to invent it.
PCR_DEFINITION=('PCR (OI) is the sum of put open interest divided by the sum of call open interest across '
 'every strike of this expiry, at that 15-min reading. PCR (volume) is the same ratio on the day\'s volume. '
 'Both are read from the metrics worker, not recomputed here.')
MAX_PAIN_DEFINITION=('Max pain is the strike at which the total payout to option buyers at expiry would be '
 'smallest, from the open interest standing across every strike of this expiry at that reading. It is read '
 'from the metrics worker, not recomputed here, and it moves as open interest moves.')
MAX_PAIN_DISTANCE_DEFINITION=('Distance is the max-pain strike MINUS spot at the same 15-min reading, which '
 'is the convention the store itself uses. A positive distance means the strike sits above spot. Two figures '
 'from two different readings are never subtracted from one another: with no reading carrying both, the '
 'distance is unavailable and says why.')
#: Why a distance is unavailable. A distance is the ONE figure on this tab that is a relationship between two
#: others, so it is the one figure that can be fabricated by pairing numbers that were never true together.
DISTANCE_REASONS={
 'no_strike':'No 15-min reading of this session carried a max-pain strike, so there is nothing to measure.',
 'no_spot_at_reading':('The reading that carries this max-pain strike carries no spot, so there is no '
  'distance at it. An earlier reading\'s spot is a different reading and is never subtracted from this '
  'strike — it would be a distance that was true of neither.'),
 'no_spot':'No 15-min reading of this session carried a spot, so there is no distance to measure to.',
}
#: The clearly-labelled ALTERNATIVE offered when the newest strike has no spot beside it: the newest reading
#: that carried BOTH. It is never substituted for the headline figure — it is served alongside it, with its
#: own reading stamp, for the reader to choose.
COMPLETE_PAIR_TEXT=('The newest 15-min reading of this session that carried a max-pain strike AND a spot '
 'together. It is offered as its own reading, with its own time — it is never mixed into the newest strike.')
FUTURES_DEFINITION=('OI vs average is this futures contract\'s open interest as a share of its own 20-day '
 'average — 1.00 is exactly its average. Basis is the futures price minus spot at the same reading, and '
 'basis % is that difference as a share of spot. All three are read from the metrics worker.')
IV_ATM_RULE=('The at-the-money strike is the listed strike nearest spot at the session\'s LAST reading, and '
 'is then held fixed across the whole session, so the line follows two contracts instead of hopping between '
 'strikes as spot drifts.')
#: ---------------------------------------------------------------------------------------------------------
#: THE ONE DEFINITION OF A DELTA ON THIS TAB. ΔOI already means "since the previous session's close"; PCR, max
#: pain and implied volatility now mean exactly the same thing by the same arithmetic, so a reader never has to
#: ask which baseline a Δ on this tab was measured from.
#:
#: No previous close means NO DELTA. Null with a stated reason — never a zero, never "unchanged". An absent
#: baseline is not a claim, and a 0 in a Δ column is a claim that the figure did not move.
SERIES_DELTA_TEXT=('Δ is the change since the previous session\'s close: this reading\'s figure minus the same '
 'figure at the last 15-min reading of the session before. The same baseline ΔOI uses. A figure with no '
 'previous close carries no Δ at all — it is null with a reason, never a zero and never "unchanged".')
SERIES_DELTA_REASONS={
 'no_baseline':'No previous-session close is stored for this figure, so there is nothing to measure against.',
 'no_value':'This 15-min reading carries no value for this figure, so there is nothing to measure.',
}
#: What each Δ is measured IN. A unit is part of the number, so it is served with it rather than assumed.
PCR_DELTA_TEXT=('PCR is a ratio, so its Δ is a change in that ratio: 0.93 to 0.96 is +0.03. It is never shown '
 'as a percentage of a ratio.')
MAX_PAIN_DELTA_TEXT=('Max pain is a STRIKE, so its Δ is in strike points and it moves in whole strike steps — '
 '23,300 to 23,350 is +50 points. A strike change is never shown as a percentage.')
IV_DELTA_TEXT=('Implied volatility is already a percentage, so its Δ is in VOLATILITY POINTS: 11.1% to 9.0% is '
 '2.1 points down. It is never shown as a percentage change of a percentage.')
PCR_DELTA_UNIT='ratio'
MAX_PAIN_DELTA_UNIT='strike points'
IV_DELTA_UNIT='volatility points'
IV_ATM_BASIS_RULE=('The at-the-money reading is the average of the at-the-money call\'s and the at-the-money '
 'put\'s solved volatility at the same 15-min reading. When only one of the two can be solved, that one IS '
 'the reading and `basis` says so — an average of one number and a blank is not an average.')


def today_ist():return datetime.now(IST).date()

def clean_symbol(value):
 """An underlying as the catalogue stores it, or '' - never a fragment of SQL."""
 text=str(value or '').strip().upper()
 if not text or len(text)>SYMBOL_MAX:return ''
 return text if all(c.isalnum() or c in '&-_.' for c in text) else ''

def clean_expiry(value):
 """'YYYY-MM-DD' or ''."""
 text=str(value or '').strip()[:EXPIRY_LEN]
 try:datetime.strptime(text,'%Y-%m-%d')
 except (TypeError,ValueError):return ''
 return text

def days_to_expiry(expiry,today=None):
 """Whole days from today (IST) to the expiry date. None when the date is unreadable.

 NOT for anything a reader sees beside a captured figure — use `session_days_to_expiry`. This one counts from
 the wall clock, which is right only for a card describing right now.
 """
 text=clean_expiry(expiry)
 if not text:return None
 try:return (date.fromisoformat(text)-(today or today_ist())).days
 except ValueError:return None

def session_date(value):
 """The session date out of a session string or a 15-min reading stamp, or ''."""
 return clean_expiry(str(value or '').strip()[:10])

def session_days_to_expiry(expiry,session=None):
 """Whole CALENDAR days from the session on screen to the expiry date. None when either date is unreadable.

 THE ONE DAY CONVENTION ON THIS TAB (`DTE_CONVENTION`). The same 22 Sep option used to read as 4 days on one
 panel and 2 on another: one panel counted from the date the reading was captured, the other from today. A
 session read back from the store is not today, and a figure captured on 18 Sep does not change because the
 clock moved — so every days-to-expiry beside a captured figure is counted from THAT figure's session.

 Calendar days, not trading days: the store holds no exchange holiday calendar, and a trading-day count
 built without one would be a guess wearing a number. Expiry day is 0 and the day before it is 1.
 """
 text=clean_expiry(expiry)
 if not text:return None
 base=session_date(session)
 try:start=date.fromisoformat(base) if base else today_ist()
 except ValueError:start=today_ist()
 try:return (date.fromisoformat(text)-start).days
 except ValueError:return None

def _num(value):
 try:
  out=float(value)
 except (TypeError,ValueError):return None
 return out if out==out and out not in (float('inf'),float('-inf')) else None

def _int(value):
 out=_num(value)
 return None if out is None else int(out)

def _round(value,places=2):
 out=_num(value)
 return None if out is None else round(out,places)


# --- ONE ANALYSIS CONTEXT ------------------------------------------------------------------------------------
# WHAT WENT WRONG WITHOUT ONE. Every card on this tab resolved its own instrument, its own expiry, its own
# session and its own "latest", and then the page put the answers side by side under one heading. On 18 Sep
# 2026 the max-pain headline printed a strike of 23,350 (the 15:45 reading), a spot of 23,302 (the 11:30
# reading) and "2 below" (a distance true only of the 11:30 PAIR). Every one of those three numbers was
# captured and correct. The sentence they made was false.
#
# So the tab has ONE context: which instrument, which expiry, which session, which 15-min reading is the
# boundary, in which timezone, and whether that boundary is the live edge of the store or a session read back.
# It travels on every response as `context`, and every metric under it is resolved AT OR BEFORE that boundary
# carrying the reading it was ACTUALLY observed at. Nothing is forward-filled into a reading that has no
# value, and no two figures from two different readings are combined into a third.
ANALYSIS_TZ='Asia/Kolkata'
ANALYSIS_MODES=('live','historical')
CONTEXT_TEXT=('Every figure under this context is resolved at or before the 15-min reading named by `at`, and '
 'carries the reading it was actually observed at. A figure with no value at a reading is left empty — it is '
 'never carried forward from an earlier one — and two figures observed at two different readings are never '
 'combined into a third.')
DTE_CONVENTION=('Days to expiry is counted from the SESSION on screen to the expiry date, in whole calendar '
 'days: expiry day is 0 and the day before it is 1. It is never counted from today\'s date, because a '
 'session read back from the store is not today. Calendar days, not trading days — this store holds no '
 'exchange holiday calendar, and a trading-day count built without one would be a guess wearing a number.')

class AnalysisContext:
 """Instrument, expiry, session, as-of reading, timezone and live-versus-historical. Carried on every response.

 `mode` is a fact about the boundary, not a setting: a session whose date is today (IST) is the live edge of
 the store and will grow at the next capture; any earlier session is closed and will not.
 """
 __slots__=('underlying','expiry','session','at','newest_at','source','tz')
 def __init__(self,underlying='',expiry='',session='',at='',newest_at='',source='store'):
  self.underlying=clean_symbol(underlying)
  self.expiry=clean_expiry(expiry)
  self.session=session_date(session) or session_date(at)
  self.at=str(at or '').strip()
  self.newest_at=str(newest_at or '').strip()
  self.source=source or 'store'
  self.tz=ANALYSIS_TZ

 @property
 def mode(self):
  """'live' while the session on screen is today in IST, 'historical' once it is a closed session."""
  if not self.session:return 'live'
  return 'live' if self.session==today_ist().isoformat() else 'historical'

 @property
 def days_to_expiry(self):
  return session_days_to_expiry(self.expiry,self.session)

 def as_dict(self):
  return {'underlying':self.underlying or None,'expiry':self.expiry or None,'session':self.session or None,
   'at':self.at or None,'newest_at':self.newest_at or None,'timezone':self.tz,'mode':self.mode,
   'is_newest':bool(not self.at or not self.newest_at or self.at==self.newest_at),
   'source':self.source,'days_to_expiry':self.days_to_expiry,'days_to_expiry_basis':DTE_CONVENTION,
   'context_text':CONTEXT_TEXT}


# --- DERIVATIVE CAPTURE HEALTH -------------------------------------------------------------------------------
# WHAT WENT WRONG WITHOUT IT. At the 15:45 reading of 18 Sep 2026 the store held 10,552 contract rows and NOT
# ONE of them carried a spot or a premium: the capture had died at 11:30 and everything after it was rebuilt
# from 15-minute candles, and a candle has no traded-price average. Nothing could ever clear a premium floor,
# because nothing was measured. The screen said "No contract cleared the liquidity floors", which a reader
# reads as "the market is quiet" — the opposite of the truth — while the page's own status chip said prices
# and patterns were healthy, which describes the cash feed and says nothing at all about F&O coverage.
#
# So the five states below are kept APART, each with its own sentence. "Nothing was measured" and "nothing
# qualified" are different facts about the market and must never share a wording.
CAPTURE_STATES=('missing_capture','partial_capture','complete','no_eligible_rows','filtered_out','failed')
#: The fields a contract row needs before it can be measured against the §3 floors at all. A row missing one
#: of these is not a row that failed the floor — it is a row that was never measured.
CAPTURE_REQUIRED_FIELDS=('premium_cr','last_price','oi')
CAPTURE_FIELD_LABELS={'premium_cr':'premium traded','last_price':'last price','oi':'open interest',
 'spot':'the underlying price','lot_size':'lot size'}
#: Fields that are not required to clear a floor but whose absence changes what the tab can say. A reading
#: with no spot cannot carry a distance, an at-the-money strike or a moneyness.
CAPTURE_REPORTED_FIELDS=CAPTURE_REQUIRED_FIELDS+('spot',)
CAPTURE_STATE_TEXT={
 'missing_capture':'No contract row was captured at this 15-min reading.',
 # A PARTIAL CAPTURE IS A DEGRADED READING, NOT AN EMPTY ONE. It used to end the sentence at "nothing could
 # be measured", which was true of the floor that was missing and false of the two that were not: at the 15:30
 # reading of 18 Sep 2026 that wording sat over 10,510 rows with a real last price, a real volume and a real
 # open interest. So it now says which floors were applied and which one was not, and the response carries
 # both lists beside it.
 'partial_capture':('Part of this 15-min reading was not captured. The fields that are here were measured and '
  'the floors that rest on them were applied; the floors that rest on the missing fields were not applied at '
  'all, so no contract here failed one. This is a gap in F&O capture, not a quiet market — and it says '
  'nothing about how much was traded.'),
 'complete':'Every field the liquidity floors need was captured at this 15-min reading.',
 'no_eligible_rows':('Every field the floors need was captured at this 15-min reading, and no contract '
  'cleared them. This is a reading of the market, not a gap in it.'),
 'filtered_out':'Contracts cleared the liquidity floors at this 15-min reading; the filters in force excluded every one of them.',
 'failed':'The F&O store could not be read for this request. No rows are shown, which is not the same as no rows existing.',
}
#: Which of the five a reader may treat as a healthy, complete reading. A partial capture is NEVER one.
CAPTURE_HEALTHY=('complete','no_eligible_rows','filtered_out')
CAPTURE_TEXT=('Capture health describes what was MEASURED in the F&O store at a 15-min reading, separately '
 'from what the measurements say. A reading with no premium captured cannot clear a premium floor for '
 'reasons that have nothing to do with trading activity, so the two are never reported under one sentence. '
 'This covers F&O capture only: the page-wide status chip describes the cash price and pattern feeds and '
 'says nothing at all about F&O coverage.')


# --- the §3 signal -> column-name map -----------------------------------------------------------------------
# D2 owns the names; these are the ones §2/§3 imply plus the obvious spellings. First hit wins. A signal with
# no column present is served as None, which the cards render as "no baseline" or a dash.
FIELDS={
 'last_price':('last_price','ltp','close'),
 'oi':('oi','open_interest'),
 'oi_lots':('oi_lots','oi_in_lots'),
 'volume':('volume','day_volume','cumulative_volume'),
 'average_price':('average_price','vwap','avg_price'),
 'premium_inr':('premium_inr','premium','premium_traded','premium_rupees','premium_value','premium_rs'),
 'premium_cr':('premium_cr','premium_crore','premium_traded_cr'),
 # never price_change_15m: that column is the rupee move, not a percent. `price_change_pct_15m` is what the
 # capture worker actually writes - without it here the option chain served this as null and named it in
 # `missing` while the number was sitting in the store under the other spelling.
 'price_change_15m_pct':('price_change_15m_pct','price_chg_15m_pct','price_change_pct_15m'),
 'oi_change_15m':('oi_change_15m','oi_chg_15m'),
 'oi_change_15m_pct':('oi_change_15m_pct','oi_chg_15m_pct','oi_change_pct_15m'),
 'buildup_15m':('buildup_15m','build_up_15m','oi_buildup_15m'),
 # never price_change_day: that column is the rupee move. Same story as the 15-minute one above.
 'price_change_day_pct':('price_change_day_pct','price_chg_day_pct','price_change_pct_day'),
 'oi_change_day':('oi_change_day','oi_chg_day','oi_change_dod'),
 'oi_change_day_pct':('oi_change_day_pct','oi_chg_day_pct','oi_change_dod_pct','oi_change_pct_day'),
 'buildup_day':('buildup_day','build_up_day','oi_buildup_day','buildup_dod'),
 'volume_ratio':('volume_ratio','volume_vs_median','volume_vs_average','volume_tod_ratio','vol_tod_ratio'),
 'volume_baseline_sessions':('volume_baseline_sessions','baseline_sessions','volume_baseline_n','baseline_n','vol_tod_sessions'),
 'volume_to_oi':('volume_to_oi','volume_oi_ratio','vol_oi_ratio'),
 'previous_oi':('previous_oi','prev_day_oi','prev_close_oi','vol_oi_prev_oi'),
 'days_to_expiry':('days_to_expiry','dte'),
 'basis':('basis','futures_basis'),
 'oi_vs_20d_avg':('oi_vs_20d_avg','oi_vs_20day_avg','oi_share_20d_avg','fut_oi_vs_avg'),
 'spot':('spot','underlying_price'),
}
#: Columns the cards read off `contracts`; these are D1's and are fixed by §2.
CONTRACT_FIELDS=('tradingsymbol','underlying','instrument_type','strike','expiry','lot_size')


class _Columns:
 """Which §3 signals this `metrics` table actually carries."""
 def __init__(self,present):
  self.present=set(present or ())
  self.map={}
  for field,names in FIELDS.items():
   for name in names:
    if name in self.present:self.map[field]=name;break
  self.missing=sorted(f for f in FIELDS if f not in self.map)
 def select(self,alias='m'):
  return [f'{alias}."{col}" as "{field}"' for field,col in self.map.items()]
 def has(self,field):return field in self.map


class Derivatives:
 """Read-only view of `db/derivatives.db` for the Derivative tab."""

 def __init__(self,path,metrics_module=None,read_module=None):
  self.path=Path(path) if path else None
  self._local=threading.local()
  self._metrics=metrics_module  # tests inject; production resolves lazily
  self._metrics_tried=metrics_module is not None
  self._read=read_module        # market_data.derivatives.read_api, same deal
  # the market-wide events pass, computed once per reading and handed to every reader after it
  self._events_cache={}
  self._read_tried=read_module is not None
  self._lock=threading.Lock()
  #: (newest reading, answer) for `_latest_complete_reading`. Keyed on the newest reading the store holds,
  #: so a capture landing invalidates it by itself - a cache that cannot go stale because its key IS the
  #: store's clock. It exists because the walk is cheap when the newest reading is complete (it stops at the
  #: first seek) and expensive exactly when it is not: during an outage every reading walked past has to be
  #: read through before it can be ruled out, which measured 124 ms over the 18 rebuilt readings of
  #: 18 Sep 2026. That is the one moment a reader most needs the answer, so it is computed once.
  self._complete_cache=(None,None)
  #: reading -> the floors that reading can be measured against, from `_floors_in_force`. One screener
  #: request asks the same question from the SQL builder, the row filter, the ranking and the usable-reading
  #: walk; the answer is three index seeks and it must be the same answer in all four places, or the list, the
  #: sentence above it and the sort below it would describe different screens.
  self._floor_cache={}

 # --- connection ------------------------------------------------------
 def _connect(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:return connection
  if not self.path or not self.path.is_file():return None
  try:
   connection=sqlite3.connect(f'file:{self.path.as_posix()}?mode=ro',uri=True,timeout=QUERY_TIMEOUT,check_same_thread=False)
   connection.row_factory=sqlite3.Row
   connection.execute('PRAGMA query_only=1')
   connection.execute('select 1 from contracts limit 1')
  except sqlite3.Error as error:
   LOG.info('derivatives: no readable F&O store yet (%s)',error)
   try:
    if connection is not None:connection.close()
   except sqlite3.Error:pass
   return None
  self._local.connection=connection
  return connection
 def close(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:
   try:connection.close()
   except sqlite3.Error:pass
   self._local.connection=None
 def available(self):return self._connect() is not None

 def _tables(self):
  connection=self._connect()
  if connection is None:return set()
  try:return {r[0] for r in connection.execute("select name from sqlite_master where type='table'")}
  except sqlite3.Error:return set()
 def _columns(self,table):
  connection=self._connect()
  if connection is None:return _Columns(())
  try:return _Columns([r[1] for r in connection.execute(f'PRAGMA table_info("{table}")')])
  except sqlite3.Error:return _Columns(())
 def _column_names(self,table):
  """The raw column names of a table, for the optional columns the store may or may not carry."""
  connection=self._connect()
  if connection is None:return set()
  try:return {r[1] for r in connection.execute(f'PRAGMA table_info("{table}")')}
  except sqlite3.Error:return set()
 def _rows(self,sql,params=()):
  connection=self._connect()
  if connection is None:return []
  try:return [dict(r) for r in connection.execute(sql,params)]
  except sqlite3.Error as error:
   LOG.info('derivatives: query failed (%s)',error);return []
 def _one(self,sql,params=()):
  rows=self._rows(sql,params)
  return rows[0] if rows else None
 def _max(self,table,column='captured_at',where='',params=()):
  if table not in self._tables():return None
  row=self._one(f'select max("{column}") as v from "{table}"'+(f' where {where}' if where else ''),params)
  return (row or {}).get('v')

 # --- D2's reader module ----------------------------------------------
 def metrics_module(self):
  """`market_data.derivatives.metrics` when it is importable, else None. Resolved once."""
  if not self._metrics_tried:
   with self._lock:
    if not self._metrics_tried:
     self._metrics_tried=True
     try:
      from market_data.derivatives import metrics  # noqa: PLC0415 - lazy: the pilot must boot without it
      self._metrics=metrics
     except Exception:  # not built yet, or not on this machine's path
      LOG.info('derivatives: metrics module not importable; serving the stored `metrics` rows.')
      self._metrics=None
  return self._metrics
 def read_module(self):
  """`market_data.derivatives.read_api` when it is importable, else None. Resolved once.

  The pilot runs with PYTHONPATH=server, so on most machines this is None and the ΔOI grid is read from the
  store below instead. Which of the two answered is stated in the response as `source`.
  """
  if not self._read_tried:
   with self._lock:
    if not self._read_tried:
     self._read_tried=True
     try:
      from market_data.derivatives import read_api  # noqa: PLC0415 - lazy: the pilot must boot without it
      self._read=read_api
     except Exception:
      LOG.info('derivatives: read_api not importable; reading the ΔOI grid from the store.')
      self._read=None
  return self._read

 def _delegate(self,name,**kwargs):
  """Call D2's reader for a card. Returns (rows, True) when it answered, ([], False) otherwise.

  A reader that is absent, or whose signature does not accept these keywords, is not an error: the stored
  rows are served instead and `source` says so.
  """
  module=self.metrics_module()
  reader=getattr(module,name,None) if module else None
  if reader is None:return [],False
  try:
   # The reader opens the store itself, so it must be told WHICH store: without this it resolves its own
   # default path and would answer from the production file while this reader is pointed at another one.
   try:rows=reader(db_path=self.path,**kwargs)
   except TypeError:rows=reader(**kwargs)
  except TypeError as error:
   LOG.info('derivatives: %s(%s) not called - %s',name,','.join(kwargs),error);return [],False
  except Exception as error:
   LOG.warning('derivatives: %s failed (%s); serving the stored rows.',name,error);return [],False
  return [dict(r) for r in (rows or [])],True

 # --- shared envelope --------------------------------------------------
 def envelope(self,as_of=None,source='store',missing=(),**extra):
  """Every card carries its own as-of, the floors in force and, when empty, the reason in plain words."""
  ready=self.available()
  body={'available':ready,'captured':bool(as_of),'as_of':as_of,'floors':dict(FLOORS),'floors_text':FLOORS_TEXT,
   'source':source if ready else 'none','missing':list(missing),'empty_text':EMPTY_TEXT,
   'empty_reason':None if as_of else EMPTY_TEXT,'baseline_sessions_required':MIN_BASELINE_SESSIONS}
  body.update(extra)
  return body

 # --- the one analysis context ----------------------------------------
 def context(self,underlying='',expiry='',session='',at='',source='store',newest_at=None):
  """The tab's ONE context for a request, as a typed object. Served as `context` on every card.

  `newest_at` is resolved for the same underlying the caller asked about rather than store-wide, because the
  capture worker does not reach every name at every reading: taking the store-wide newest reading for a card
  about RELIANCE says RELIANCE is behind when RELIANCE simply has no row at that reading.
  """
  newest=newest_at if newest_at is not None else (self._latest_mark(clean_symbol(underlying)) or '')
  return AnalysisContext(underlying=underlying,expiry=expiry,session=session,at=at,newest_at=newest,
   source=source)

 # --- derivative capture health ---------------------------------------
 def _field_coverage(self,at,fields=CAPTURE_REPORTED_FIELDS):
  """How many contract rows at ONE reading carry each field, counted rather than assumed.

  One aggregate over the reading's own slice of the primary key. `count(column)` is the count of NON-NULL
  values, which is exactly the question: a column the store has but never filled is not coverage.
  """
  names=self._column_names('metrics')
  present=[f for f in fields if f in names]
  clauses=['captured_at=?']
  if self._has_scope():clauses.insert(0,"scope='contract'")
  if 'instrument_type' in names:clauses.append("instrument_type in ('CE','PE','FUT')")
  select=['count(*) as rows']+[f'count("{f}") as "n_{f}"' for f in present]
  row=self._one('select '+','.join(select)+' from metrics where '+' and '.join(clauses),(at,)) or {}
  rows=_int(row.get('rows')) or 0
  out={}
  for field in fields:
   if field not in present:
    out[field]={'present':None,'rows':rows,'share':None,'column':False,
     'label':CAPTURE_FIELD_LABELS.get(field,field)}
    continue
   have=_int(row.get(f'n_{field}')) or 0
   out[field]={'present':have,'rows':rows,'share':(round(have/rows,4) if rows else None),'column':True,
    'label':CAPTURE_FIELD_LABELS.get(field,field)}
  return rows,out

 #: How far back the "latest complete reading" walk may go. A session is 26 readings, so this covers one
 #: whole session plus a little: an outage that ran longer than a session has no recent complete reading to
 #: offer, and saying so is the honest answer. Bounded because this runs on the screener's own request.
 COMPLETE_LOOKBACK=32

 def _reading_has(self,at,field):
  """Does ONE reading hold a single row with this field filled in? An index seek that stops at the first row.

  Deliberately an EXISTS and not a COUNT. `count(premium_cr) ... where captured_at=?` visits every row of
  the reading - 27,239 of them at 11:30 on 18 Sep 2026 - and this walk asks the question up to 32 times.
  Whether the field was captured AT ALL is a question the first matching row answers.
  """
  if 'metrics' not in self._tables() or not at:return False
  if field not in self._column_names('metrics'):return True  # a column the store lacks is not a gap it has
  clauses=[]
  if self._has_scope():clauses.append("scope='contract'")
  clauses.append('captured_at=?')
  clauses.append(f'"{field}" is not null')
  return bool(self._rows('select 1 from metrics where '+' and '.join(clauses)+' limit 1',(at,)))

 def _floors_in_force(self,at,names=None):
  """Which of the §3 floors THIS reading can actually be measured against, and which it cannot.

  A floor is in force when the store carries every column it rests on AND that reading filled at least one
  of them. A floor whose column is present but empty at this reading is `unmeasured`: it is not applied, and
  the rows it would have judged are kept and said to be ungated on it. A floor whose column the store does
  not carry at all is `absent` - the query never had that clause to begin with, which is a different fact
  and is reported under its own name rather than folded in.

  Three `_reading_has` seeks, each of which stops at the first matching row, cached per reading.

  What is NOT here, deliberately: any fallback number. The store's ESTIMATED traded average would make
  `premium_cr` look measurable at a rebuilt reading, and it is an estimate rather than the exchange's figure.
  A floor decides what a reader sees; it is applied on a captured number or it is not applied and says so.
  """
  at=str(at or '').strip()
  if not at:return {'applied':(),'unmeasured':(),'absent':tuple(FLOOR_ORDER)}
  hit=self._floor_cache.get(at)
  if hit is not None:return hit
  # THE COLUMN THE READER ACTUALLY USES, not the name §3 gives the floor. `FIELDS` carries the spellings a
  # store may use - a store that writes `premium_inr` and no `premium_cr` is measuring premium perfectly well,
  # and asking after the literal name would have called its premium floor unmeasurable and degraded a screen
  # that needs no degrading.
  resolved=self._columns('metrics').map
  applied,unmeasured,absent=[],[],[]
  for floor in FLOOR_ORDER:
   column=resolved.get(FLOOR_COLUMNS[floor][0])
   if column is None and floor=='premium_cr':column=resolved.get('premium_inr')
   if column is None:absent.append(floor);continue
   if self._reading_has(at,column):applied.append(floor)
   else:unmeasured.append(floor)
  out={'applied':tuple(applied),'unmeasured':tuple(unmeasured),'absent':tuple(absent)}
  if len(self._floor_cache)>64:self._floor_cache.clear()
  self._floor_cache[at]=out
  return out

 @staticmethod
 def _floor_note(force):
  """Everything a card needs to SAY about the floors it was gated on, as facts rather than a paragraph.

  `floors_degraded` is the one a reader must not be able to miss: it means the list in front of them was
  gated on fewer floors than §3 names, and `floors_unmeasured_text` says which and why.
  """
  applied=list((force or {}).get('applied') or ())
  unmeasured=list((force or {}).get('unmeasured') or ())
  absent=list((force or {}).get('absent') or ())
  return {'floors':dict(FLOORS),'floors_text':floors_sentence(applied),
   'floors_applied':applied,'floors_unmeasured':unmeasured,'floors_absent':absent,
   'floors_degraded':bool(unmeasured or absent),
   'floors_unmeasured_text':[FLOOR_UNMEASURED_TEXT[k] for k in unmeasured if k in FLOOR_UNMEASURED_TEXT],
   'floors_labels':dict(FLOOR_PHRASES)}

 def _latest_complete_reading(self,readings,limit=None):
  """The newest reading whose required fields were all captured, walking back at most `limit` readings.

  This is the reading the tab OFFERS when the one on screen measured nothing. It is never substituted for
  the reader's own choice - naming it is the whole point, because a reader who is silently moved to another
  reading has no way to know the one they asked for was empty.

  When nothing inside the window is complete it returns '' and the tab says so, rather than reaching further
  back and calling a reading from two sessions ago "the latest complete one".
  """
  newest=(readings or [{}])[0].get('at') if readings else None
  key,cached=self._complete_cache
  if newest and key==newest and limit is None:return cached
  found=''
  for row in (readings or ())[:max(1,int(limit or self.COMPLETE_LOOKBACK))]:
   at=row.get('at')
   if not at:continue
   if all(self._reading_has(at,field) for field in CAPTURE_REQUIRED_FIELDS):found=at;break
  if newest and limit is None:self._complete_cache=(newest,found)
  return found

 def capture_health(self,at='',readings=None,cleared=None,matched=None):
  """What was MEASURED at one 15-min reading, as one of `CAPTURE_STATES`, with the counts behind it.

  `cleared` is how many rows cleared the §3 floors and `matched` how many survived the reader's own filters;
  either may be None when the caller has not run that step. The state is decided in this order, because each
  one makes the next question meaningful:

    no rows at all              -> missing_capture   (nothing was captured)
    a required field is empty   -> partial_capture   (nothing could be measured)
    nothing cleared the floors  -> no_eligible_rows  (a real reading of a real market)
    filters removed the rest    -> filtered_out      (the reader's own choice, not the market's)
    otherwise                   -> complete

  A partial capture NEVER reaches `no_eligible_rows`: a contract with no premium did not fail a premium
  floor, it was never measured against one.
  """
  at=str(at or '').strip()
  if not self.available():
   return {'state':'failed','state_text':CAPTURE_STATE_TEXT['failed'],'at':at or None,'rows':0,
    'coverage':{},'missing_fields':[],'source':'none','healthy':False,'capture_text':CAPTURE_TEXT,
    'states':list(CAPTURE_STATES),'latest_attempted_at':None,'latest_available_at':None,
    'latest_complete_at':None,'cleared':None,'matched':None,'required_fields':list(CAPTURE_REQUIRED_FIELDS),
    **self._floor_note(None)}
  if readings is None:readings=self.screener_readings()
  attempted=(readings[0]['at'] if readings else None) or self._max('metrics') or None
  available=next((r['at'] for r in (readings or ()) if self._reading_rows(r.get('at'))),None)
  complete=self._latest_complete_reading(readings) or None
  # NO READING NAMED MEANS THE NEWEST ONE, NOT NONE. Found live on 21 Sep 2026: the app-wide F&O chip calls this
  # route with no `at`, and with no `at` the state below fell straight to `missing_capture` — so the chip read
  # "F&O not captured" through a complete 27,317-row capture, and had said so on every healthy day before it.
  # A caller that names no reading is asking about the live one, and that is the newest attempted reading.
  if not at:at=str(attempted or '')
  rows,coverage=self._field_coverage(at) if at else (0,{})
  missing=[f for f in CAPTURE_REQUIRED_FIELDS
   if (coverage.get(f) or {}).get('column') and not ((coverage.get(f) or {}).get('present') or 0)]
  empty=[f for f in CAPTURE_REPORTED_FIELDS
   if (coverage.get(f) or {}).get('column') and not ((coverage.get(f) or {}).get('present') or 0)]
  if not at or not rows:state='missing_capture'
  elif missing:state='partial_capture'
  elif cleared is not None and not cleared:state='no_eligible_rows'
  elif cleared and matched is not None and not matched:state='filtered_out'
  else:state='complete'
  # THE FLOORS THIS READING WAS GATED ON, beside the coverage that decided them. The chip and the screener
  # take the same answer from the same place, so the amber caveat and the list under it can never describe
  # two different screens.
  return {'state':state,'state_text':CAPTURE_STATE_TEXT[state],'at':at or None,'rows':rows,
   'coverage':coverage,'missing_fields':missing,'empty_fields':empty,
   **(self._floor_note(self._floors_in_force(at)) if at and rows else self._floor_note(None)),
   # WHERE THE READING CAME FROM. A reading whose premium column is empty for every row is one the metrics
   # worker rebuilt from 15-minute candles; a candle carries no traded-price average, and that is not a
   # guess about the store, it is what an empty premium column at a full reading means.
   'source':('candles_15m' if state=='partial_capture' and 'premium_cr' in missing
    else ('metrics' if rows else 'none')),
   'healthy':state in CAPTURE_HEALTHY,'capture_text':CAPTURE_TEXT,'states':list(CAPTURE_STATES),
   'latest_attempted_at':attempted,'latest_available_at':available,'latest_complete_at':complete,
   'latest_complete_is_here':bool(complete and at and complete==at),
   'cleared':cleared,'matched':matched,'required_fields':list(CAPTURE_REQUIRED_FIELDS),
   'field_labels':dict(CAPTURE_FIELD_LABELS)}

 def status(self):
  """Is there anything captured at all, and when. The tab's own header line reads this."""
  tables=self._tables()
  as_of=self._max('metrics') or self._max('snapshots') or self._max('underlying_snapshots')
  columns=self._columns('metrics')
  sessions=None
  if 'candles_15m' in tables:
   # How many trading days the backfill covers. NOT `count(distinct substr(bar_start,1,10))`: that builds a
   # temporary B-tree over every row in the table - 2,675,883 of them today - and measured 416 ms on the real
   # store, on the one call the tab's own header waits for. This walks the bar_start index instead, one seek
   # per day it finds (14 seeks today), and measured 0.03 ms for the same answer.
   row=self._one("with recursive day(d) as ("
    ' select substr(min(bar_start),1,10) from candles_15m'
    ' union all'
    " select (select substr(min(bar_start),1,10) from candles_15m where bar_start>day.d||'~')"
    ' from day where d is not null)'
    ' select count(d) as n from day')
   sessions=_int((row or {}).get('n'))
  counts={}
  if 'contracts' in tables:
   for row in self._rows('select instrument_type as t, count(*) as n from contracts group by instrument_type'):
    counts[str(row.get('t') or '?')]=_int(row.get('n')) or 0
  # THE TAB'S OWN HEALTH, NOT THE APP'S. The global status chip describes the cash feed; it said prices and
  # patterns were healthy through a derivative capture outage that had left every contract row of the newest
  # reading with no premium and no spot. F&O coverage is a separate measurement and is reported separately.
  capture=self.capture_health(as_of or '') if 'metrics' in tables else None
  return self.envelope(as_of=as_of,source='metrics_module' if self.metrics_module() else 'store',
   missing=columns.missing,tables=sorted(tables),contracts=counts,backfill_sessions=sessions,
   metrics_ready=bool(columns.map),index_underlyings=list(INDEX_UNDERLYINGS),capture=capture)

 # --- row assembly -----------------------------------------------------
 @staticmethod
 def _latest_at(points,key):
  """The newest reading that CARRIES this figure, and which reading that was.

  "Latest" used to mean the newest reading carrying the block's PRIMARY figure, and every other `latest_*`
  was taken from that same reading. On 18 Sep 2026 that put max pain at 23,350 beside a spot of "-": the
  newest reading with a max-pain strike was 15:45, a reading rebuilt from candles, which carries no spot at
  all. Two figures that were captured an hour apart are two different readings and must say so.

  So each figure is resolved on its own, and each says which reading it came from. Nothing is carried
  forward and nothing is combined: this walks back to a reading that HAS the number, or returns None.
  """
  row=next((p for p in reversed(points or ()) if p and p.get(key) is not None),None)
  return (row.get(key) if row else None),(row.get('at') if row else None)

 @staticmethod
 def _figure_at(points,at,key):
  """One figure AT a named reading. No walk-back: a reading with no value for it returns None, full stop.

  This is the counterpart to `_latest_at` and the one a RELATIONSHIP between two figures must use. A
  distance is only a distance when both of its terms were observed together; resolved with a walk-back it
  becomes an older pair's distance wearing a newer strike's heading, which is the sentence this tab printed
  on 18 Sep 2026: 23,350, 23,302 and "2 below", none of which were ever true at the same moment.
  """
  if not at:return None
  row=next((p for p in (points or ()) if p and p.get('at')==at),None)
  return (row or {}).get(key)

 @staticmethod
 def _latest_pair(points,*keys):
  """The newest reading that carries EVERY one of `keys`, as {'at':…, key:…}, or None when there is none."""
  for point in reversed(points or ()):
   if point and all(point.get(k) is not None for k in keys):
    return {'at':point.get('at'),**{k:point.get(k) for k in keys}}
  return None

 def _latest_mark(self,underlying=''):
  """The newest reading the store holds - for ONE underlying when named, otherwise across the whole store.

  This is here because the capture worker does not always reach every underlying at every reading. On the
  real store the newest reading covers one name; the reading before that covered 216. Taking the store-wide
  newest reading for a card about RELIANCE therefore asks for RELIANCE rows at a reading RELIANCE has none
  at, and the card comes back empty on a store that plainly holds the data - which is what was happening to
  the option chain, the unusual list and the futures table for every single stock.

  A named underlying gets its own newest reading instead, through `idx_metrics_und_at`, and the card's
  `as_of` then says which reading it is actually showing. Nothing is mixed: one card, one reading.
  """
  if 'metrics' not in self._tables():return None
  if not underlying:return self._max('metrics')
  row=self._one('select max(captured_at) as v from metrics where underlying=?',(underlying,))
  return (row or {}).get('v') or self._max('metrics')

 def _metric_rows(self,where='',params=(),limit=None,types=None,order='',underlying='',at=''):
  """`metrics` rows joined to their contract, at ONE reading. Column-driven; absent signals come back None.

  `at` names the reading. Omitted, it is the newest one this underlying has, exactly as before - the caller
  that knows which reading the TAB is on passes it, and a caller that does not is left alone.
  """
  tables=self._tables()
  if 'metrics' not in tables or 'contracts' not in tables:return [],None,_Columns(())
  columns=self._columns('metrics')
  if not columns.map:return [],None,columns
  as_of=str(at or '').strip() or self._latest_mark(underlying)
  if not as_of:return [],None,columns
  clauses=['m.captured_at=?']
  args=[as_of]
  if types:
   clauses.append('c.instrument_type in (%s)'%','.join('?' for _ in types));args.extend(types)
  if where:clauses.append(where);args.extend(params)
  select=','.join([f'c."{f}" as "{f}"' for f in CONTRACT_FIELDS]+['m.captured_at as captured_at',
   'm.instrument_token as instrument_token']+columns.select())
  sql=(f'select {select} from metrics m join contracts c on c.instrument_token=m.instrument_token '
   f'where {" and ".join(clauses)} {order}')
  if limit:sql+=f' limit {int(limit)}'
  return [self._shape(r) for r in self._rows(sql,tuple(args))],as_of,columns

 def _shape(self,row):
  """One contract row in the tab's own shape. Presentation only - no signal is computed here."""
  premium_cr=_num(row.get('premium_cr'))
  if premium_cr is None:
   premium=_num(row.get('premium_inr'))
   premium_cr=premium/CRORE if premium is not None else None
  sessions=_int(row.get('volume_baseline_sessions'))
  ratio=_num(row.get('volume_ratio'))
  # §3.2, enforced on the serving side too: a ratio without a stated baseline of >= 3 sessions is not served.
  enough=sessions is not None and sessions>=MIN_BASELINE_SESSIONS
  dte=_int(row.get('days_to_expiry'))
  # The stored figure is the metrics worker's own, counted at capture. The fallback is counted from THIS
  # ROW'S OWN reading for the same reason (DTE_CONVENTION): counted from today, a row captured on 18 Sep
  # would read as fewer days every morning while the number beside it never moved.
  if dte is None:dte=session_days_to_expiry(row.get('expiry'),session_date(row.get('captured_at')))
  return {
   'instrument_token':_int(row.get('instrument_token')),'tradingsymbol':row.get('tradingsymbol') or '',
   'underlying':row.get('underlying') or '','instrument_type':row.get('instrument_type') or '',
   'strike':_num(row.get('strike')),'expiry':row.get('expiry') or '','lot_size':_int(row.get('lot_size')),
   'days_to_expiry':dte,'captured_at':row.get('captured_at'),
   'last_price':_round(row.get('last_price')),'oi':_int(row.get('oi')),'oi_lots':_int(row.get('oi_lots')),
   'volume':_int(row.get('volume')),'average_price':_round(row.get('average_price')),
   'premium_cr':_round(premium_cr),
   'price_change_15m_pct':_round(row.get('price_change_15m_pct')),
   'oi_change_15m':_int(row.get('oi_change_15m')),'oi_change_15m_pct':_round(row.get('oi_change_15m_pct')),
   'buildup_15m':row.get('buildup_15m') or None,
   'price_change_day_pct':_round(row.get('price_change_day_pct')),
   'oi_change_day':_int(row.get('oi_change_day')),'oi_change_day_pct':_round(row.get('oi_change_day_pct')),
   'buildup_day':row.get('buildup_day') or None,
   'volume_ratio':_round(ratio) if enough else None,
   'volume_baseline_sessions':sessions,'volume_baseline':'ok' if enough else 'none',
   'volume_to_oi':_round(row.get('volume_to_oi')),'previous_oi':_int(row.get('previous_oi')),
   'basis':_round(row.get('basis')),'oi_vs_20d_avg':_round(row.get('oi_vs_20d_avg')),
   # The underlying's price at this mark, for the table's Spot column. `spot` is already in FIELDS (so a store
   # without the column already names it in `missing`); this only passes the captured value through. Absent ⇒
   # None ⇒ a dash, never the chain's spot borrowed from another mark.
   'spot':_round(row.get('spot')),
  }

 def _passes(self,row,applied=None):
  """The §3 liquidity floors that are IN FORCE at this reading, applied to one row.

  Within a floor that IS in force nothing changed and nothing is softened: a row missing the number is a row
  that does not pass, because absence is not evidence. What `applied` changes is which floors are asked at
  all. It defaults to all three, so every existing caller keeps the behaviour it had.

  The distinction is the whole point. "This contract's premium was below ₹2 cr" is a reading of the market;
  "no premium was captured at this reading" is a gap in ours. Judging the second as if it were the first is
  what emptied the screen for four hours of a real session.
  """
  keys=FLOOR_ORDER if applied is None else set(applied)
  if 'premium_cr' in keys:
   premium=row.get('premium_cr')
   if premium is None or premium<FLOOR_PREMIUM_CR:return False
  if 'last_price' in keys:
   price=row.get('last_price')
   if price is None or price<FLOOR_LAST_PRICE:return False
  if 'oi' in keys:
   lots=row.get('oi_lots')
   if lots is None:
    oi,lot=row.get('oi'),row.get('lot_size')
    lots=(oi/lot) if oi is not None and lot else None
   if lots is None or lots<FLOOR_OI_LOTS:return False
  return True

 # --- filters ----------------------------------------------------------
 def filters(self):
  """Everything the Customize panel offers: underlyings, expiries, watch-lists, the floors, the as-of."""
  tables=self._tables()
  if 'contracts' not in tables:return self.envelope(underlyings=[],expiries=[],watchlists=[dict(key=k,label=l) for k,l in WATCHLISTS],option_types=list(OPTION_TYPES))
  as_of=self._max('metrics') or self._max('snapshots')
  today=today_ist()
  underlyings=[]
  for row in self._rows("select underlying,"
    " sum(case when instrument_type in ('CE','PE') then 1 else 0 end) as options,"
    " sum(case when instrument_type='FUT' then 1 else 0 end) as futures,"
    ' max(lot_size) as lot_size from contracts group by underlying order by underlying'):
   name=row.get('underlying') or ''
   if not name:continue
   underlyings.append({'underlying':name,'options':_int(row.get('options')) or 0,'futures':_int(row.get('futures')) or 0,
    'lot_size':_int(row.get('lot_size')),'is_index':name.upper() in INDEX_UNDERLYINGS})
  expiries=[]
  for row in self._rows('select underlying,expiry,count(*) as contracts from contracts'
    ' where expiry>=? group by underlying,expiry order by underlying,expiry',(today.isoformat(),)):
   expiry=clean_expiry(row.get('expiry'))
   if not expiry:continue
   expiries.append({'underlying':row.get('underlying') or '','expiry':expiry,
    'days_to_expiry':days_to_expiry(expiry,today),'contracts':_int(row.get('contracts')) or 0})
  # THE ONE PLACE `days_to_expiry` IS COUNTED FROM TODAY, and it says so. This list is a PICKER of expiries
  # still to come, not a description of a captured reading: "which expiries can I choose right now" is a
  # question about today. Every days-to-expiry beside a captured figure uses DTE_CONVENTION instead.
  return self.envelope(as_of=as_of,missing=self._columns('metrics').missing,underlyings=underlyings,expiries=expiries,
   watchlists=[{'key':k,'label':l} for k,l in WATCHLISTS],option_types=list(OPTION_TYPES),
   expiry_days_basis=('Days to expiry in this picker is counted from today, because this list is the '
    'expiries still to come rather than a reading of a session. '+DTE_CONVENTION),
   index_underlyings=list(INDEX_UNDERLYINGS))

 # --- card 1: unusual activity ----------------------------------------
 def unusual(self,underlying='',expiry='',watchlist='',max_dte=None,option_type='',min_premium_cr=None,limit=None):
  """§3.2-§3.4 screen, rolled up per underlying and expandable to its strikes (§4 card 1).

  The floors of §3 are ALWAYS applied; `min_premium_cr` can only raise the premium floor, never lower it.
  """
  rows,delegated=self._delegate('unusual_activity',underlying=underlying or None,expiry=expiry or None,
   max_days_to_expiry=max_dte,option_type=option_type or None,min_premium_cr=min_premium_cr,limit=limit)
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=[],[]
   if underlying:clauses.append('c.underlying=?');params.append(underlying)
   if expiry:clauses.append('c.expiry=?');params.append(expiry)
   if option_type:clauses.append('c.instrument_type=?');params.append(option_type)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=OPTION_TYPES,
    underlying=underlying)
  # THE SAME DEGRADATION AS THE SCREENER, resolved from the same place. Card 1 rests on the same three floors,
  # so a reading with no traded average empties it for the same reason and must keep its rows for the same one.
  # No reading at all is not a degraded reading: there is nothing to serve either way, and the card must not
  # announce a relaxation that never happened. With no `as_of` the floors stand as §3 writes them.
  force=(self._floors_in_force(as_of) if as_of
   else {'applied':tuple(FLOOR_ORDER),'unmeasured':(),'absent':()})
  in_force=set(force['applied'])
  floor=max(FLOOR_PREMIUM_CR,_num(min_premium_cr) or 0.0)
  if min_premium_cr is not None and as_of and 'premium_cr' not in in_force:
   raise ValueError('min_premium_cr cannot be applied at the 15-min reading of '+str(as_of)+
    ': no traded average price was captured there, so no contract in it has a premium traded.')
  names=self._watchlist(watchlist)
  kept=[]
  for row in shaped:
   if not self._passes(row,force['applied']):continue
   if 'premium_cr' in in_force and (row.get('premium_cr') or 0)<floor:continue
   if names is not None and row['underlying'].upper() not in names:continue
   if max_dte is not None and (row.get('days_to_expiry') is None or row['days_to_expiry']>max_dte):continue
   kept.append(row)
  groups={}
  for row in kept:
   group=groups.setdefault(row['underlying'],{'underlying':row['underlying'],'premium_cr':0.0,
    'has_premium':False,'volume':0,'strikes':[],
    'expiries':set(),'calls':0,'puts':0,'oi_change_day':0,'has_oi_change':False})
   # A sum of absences is not ₹0 cr. See `_screener_groups`: the same rule, for the same reason.
   if row.get('premium_cr') is not None:
    group['premium_cr']+=row['premium_cr'];group['has_premium']=True
   group['volume']+=row.get('volume') or 0
   group['strikes'].append(row)
   if row.get('expiry'):group['expiries'].add(row['expiry'])
   if row['instrument_type']=='CE':group['calls']+=1
   elif row['instrument_type']=='PE':group['puts']+=1
   if row.get('oi_change_day') is not None:group['oi_change_day']+=row['oi_change_day'];group['has_oi_change']=True
  out=[]
  for group in groups.values():
   strikes=sorted(group['strikes'],key=(lambda r:-(r.get('premium_cr') or 0)) if group['has_premium']
    else (lambda r:(-(r.get('volume') or 0),r.get('tradingsymbol') or '')))
   out.append({'underlying':group['underlying'],
    'premium_cr':(round(group['premium_cr'],2) if group['has_premium'] else None),
    'volume':group['volume'],
    'strike_count':len(strikes),'calls':group['calls'],'puts':group['puts'],
    'expiries':sorted(group['expiries']),
    'days_to_expiry':min([s['days_to_expiry'] for s in strikes if s['days_to_expiry'] is not None],default=None),
    'oi_change_day':group['oi_change_day'] if group['has_oi_change'] else None,
    'strikes':strikes[:20]})
  # Ranked by what this reading measured: premium traded where it is there, volume where it is not.
  out.sort(key=lambda g:(-(g['premium_cr'] if g['premium_cr'] is not None else (g['volume'] or 0)),
   g['underlying'] or ''))
  cut=max(1,min(int(limit or ROW_LIMIT_DEFAULT),ROW_LIMIT_MAX))
  # The same rule the screener works to: an empty list says what actually happened. A reading whose premium
  # column was never filled did not fail a premium floor - nothing in it was measured against one.
  # `cleared` is the FLOORS-ONLY count, which is the one that separates "nothing qualified" from "the
  # filters in force excluded everything". `matched` is what survived this call's own narrowing.
  capture=self.capture_health(as_of or '',cleared=self._cleared_count(as_of or ''),
   matched=len(out)) if as_of else None
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out[:cut],total=len(out),
   # `floor_premium_cr` is the floor that was IN FORCE. None says it was not applied at all, which is a
   # different answer from "₹2 cr" and must not be printed as one.
   floor_premium_cr=(floor if 'premium_cr' in in_force else None),
   ranking=self._ranking('underlying',force['applied']),
   **self._floor_note(force),capture=capture,
   context=self.context(underlying,expiry,session_date(as_of),as_of or '',
    'metrics_module' if delegated else 'store').as_dict(),
   empty_note=(None if out or not capture else capture['state_text']),
   empty_state=(None if out or not capture else capture['state']))

 def _watchlist(self,key):
  key=str(key or '').strip().lower()
  if key=='indices':return {n.upper() for n in INDEX_UNDERLYINGS}
  return None

 # --- card 2: option chain ---------------------------------------------
 def chain(self,underlying,expiry='',option_type='',at=''):
  """§4 card 2: one row per strike with CE and PE beside each other, at ONE 15-min reading.

  `at` is the reading the tab is on - the one its screener resolved. Omitted, the newest this underlying has,
  which is what every caller got before. It is passed because the newest reading of a rebuilt session carries
  no spot, and a chain with no spot has nothing to sit around.
  """
  if not underlying:return self.envelope(rows=[],underlying='',expiry='')
  expiry=expiry or self._front_expiry(underlying)
  # A NAMED READING NEVER GOES THROUGH THE DELEGATE. D2's reader answers for the newest reading it finds and
  # has no parameter for a different one, so handing it `at` would silently return the newest anyway - the
  # chain would look fixed here and still open two thousand points from spot in front of the reader. When a
  # reading is named, the store is read directly for that reading.
  at=str(at or '').strip()
  rows,delegated=(([],False) if at
   else self._delegate('option_chain',underlying=underlying,expiry=expiry or None))
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=['c.underlying=?'],[underlying]
   if expiry:clauses.append('c.expiry=?');params.append(expiry)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=OPTION_TYPES,
    order='order by c.strike',underlying=underlying,at=at)
  strikes={}
  for row in shaped:
   if option_type and row['instrument_type']!=option_type:continue
   strike=row.get('strike')
   if strike is None:continue
   slot=strikes.setdefault(strike,{'strike':strike,'ce':None,'pe':None})
   slot['ce' if row['instrument_type']=='CE' else 'pe']=row
  # The spot is read AT this chain's own reading, so `spot_at` is this reading or the spot is not there. A
  # chain never borrows an earlier reading's spot to have one.
  spot=self._spot(underlying,as_of)
  out=sorted(strikes.values(),key=lambda r:r['strike'])
  session=session_date(as_of)
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out,underlying=underlying,expiry=expiry,spot=spot.get('spot'),
   spot_at=(spot.get('captured_at') if spot.get('spot') is not None else None),
   session=session or None,
   context=self.context(underlying,expiry,session,as_of or '',
    'metrics_module' if delegated else 'store').as_dict(),
   days_to_expiry=session_days_to_expiry(expiry,session),days_to_expiry_basis=DTE_CONVENTION,
   total=len(out))

 def _chain_metrics(self,underlying,expiry,at):
  """The per-underlying metric row for one chain at ONE reading, or None. A primary-key seek."""
  if not (underlying and expiry and at) or not self._has_scope():return None
  return self._one("select * from metrics where scope='underlying' and metric_key=? and captured_at=?",
   (f'{underlying}|{clean_expiry(expiry)}',at))

 def _front_expiry(self,underlying):
  row=self._one('select min(expiry) as e from contracts where underlying=? and expiry>=?',
   (underlying,today_ist().isoformat()))
  return clean_expiry((row or {}).get('e'))

 def _spot(self,underlying,at=''):
  """One `underlying_snapshots` row: spot, futures price, PCR, max pain - all D1/D2's numbers.

  `at` names the reading. This matters more than it looks: a session rebuilt from 15-minute candles carries
  NO SPOT at all, so the newest snapshot of 18 Sep 2026 has `spot` null from 11:45 onward. A chain that took
  the newest row therefore had no spot to sit around, and opened at its lowest strike - 21,350 against a spot
  of 23,302, nearly two thousand points away, with every visible contract far out of the money.
  """
  if 'underlying_snapshots' not in self._tables():return {}
  at=str(at or '').strip()
  row=(self._one('select * from underlying_snapshots where underlying=? and captured_at=?',(underlying,at))
   if at else
   self._one('select * from underlying_snapshots where underlying=? order by captured_at desc limit 1',(underlying,)))
  if not row:return {}
  return {'captured_at':row.get('captured_at'),'spot':_round(row.get('spot')),'fut_price':_round(row.get('fut_price')),
   'total_ce_oi':_int(row.get('total_ce_oi')),'total_pe_oi':_int(row.get('total_pe_oi')),
   'total_ce_volume':_int(row.get('total_ce_volume')),'total_pe_volume':_int(row.get('total_pe_volume')),
   'pcr_oi':_round(row.get('pcr_oi')),'pcr_volume':_round(row.get('pcr_volume')),
   'max_pain_strike':_num(row.get('max_pain_strike'))}

 # --- card 3: OI by strike ---------------------------------------------
 def oi_by_strike(self,underlying,expiry='',at=''):
  """§4 card 3: CE vs PE OI per strike, with max pain and spot marked. §3.6's total OI travels with it."""
  if not underlying:return self.envelope(rows=[],underlying='',expiry='')
  chain=self.chain(underlying,expiry,at=at)
  series=[]
  total_ce=total_pe=0
  for row in chain.get('rows') or []:
   ce,pe=row.get('ce') or {},row.get('pe') or {}
   total_ce+=ce.get('oi') or 0;total_pe+=pe.get('oi') or 0
   series.append({'strike':row['strike'],'ce_oi':ce.get('oi'),'pe_oi':pe.get('oi'),
    'ce_oi_change_day':ce.get('oi_change_day'),'pe_oi_change_day':pe.get('oi_change_day'),
    'ce_buildup_day':ce.get('buildup_day'),'pe_buildup_day':pe.get('buildup_day')})
  # ONE READING, AND ONLY ONE. Max pain per expiry lives in the metrics worker's per-underlying row, NOT in
  # `underlying_snapshots`, whose `max_pain_strike` column the worker leaves null - so the chain row is read
  # at the SAME reading this card is showing, and the snapshot is the fallback for a store that has no
  # per-underlying metric rows at all.
  #
  # The fallback used to be taken PER FIGURE: a missing strike fell back to the newest snapshot's strike and
  # a missing spot to the newest snapshot's spot, and the distance was then computed across whichever two
  # readings happened to answer. Now a fallback is taken for the PAIR or not at all, and whichever reading
  # the two figures came from travels with them.
  at=chain.get('as_of')
  chain_row=self._chain_metrics(underlying,chain.get('expiry'),at)
  max_pain=_num((chain_row or {}).get('max_pain_strike'))
  here=_num((chain_row or {}).get('spot'))
  pain_at=spot_at=at if (chain_row is not None) else None
  if max_pain is None:
   snapshot=self._spot(underlying,at)
   max_pain=snapshot.get('max_pain_strike')
   pain_at=snapshot.get('captured_at') if max_pain is not None else None
  if here is None:
   snapshot=self._spot(underlying,at)
   here=snapshot.get('spot')
   spot_at=snapshot.get('captured_at') if here is not None else None
  # The store's own convention, which `maxpain-series` and the metrics table both use: the max-pain STRIKE
  # MINUS spot, so a positive distance means the strike sits above spot. This card used to return the
  # opposite sign, which would have put two contradictory readings of one number on one tab.
  distance=_num((chain_row or {}).get('max_pain_distance'))
  reason=None
  if distance is None:
   # A distance is a relationship, so it needs BOTH of its terms at ONE reading. Two figures a reading
   # apart are never subtracted from one another to produce one.
   if max_pain is None:reason='no_strike'
   elif here is None:reason='no_spot_at_reading'
   elif pain_at!=spot_at:reason='no_spot_at_reading'
   else:distance=round(max_pain-here,2)
  session=session_date(at)
  return self.envelope(as_of=at,source=chain.get('source','store'),missing=chain.get('missing',[]),
   rows=series,underlying=underlying,expiry=chain.get('expiry'),spot=here,spot_at=spot_at,
   max_pain_strike=max_pain,max_pain_strike_at=pain_at,max_pain_distance=_round(distance),
   max_pain_distance_at=(pain_at if distance is not None else None),
   max_pain_distance_reason=(DISTANCE_REASONS.get(reason) if reason else None),
   max_pain_distance_withheld=reason,
   max_pain_distance_definition=MAX_PAIN_DISTANCE_DEFINITION,
   max_pain_total_oi=_int((chain_row or {}).get('max_pain_total_oi')),
   max_pain_status=(chain_row or {}).get('max_pain_status'),
   total_ce_oi=total_ce or None,total_pe_oi=total_pe or None,session=session or None,
   context=self.context(underlying,chain.get('expiry') or '',session,at or '',
    chain.get('source','store')).as_dict(),
   days_to_expiry=chain.get('days_to_expiry'),days_to_expiry_basis=DTE_CONVENTION)

 # --- card 4: index dashboard ------------------------------------------
 def indices(self,names=None,points=None):
  """§4 card 4: NIFTY / BANKNIFTY / FINNIFTY - PCR, max pain, and OI through the day.

  The through-the-day series is `underlying_snapshots` read back in capture order; nothing is interpolated.
  """
  wanted=[clean_symbol(n) for n in (names or INDEX_UNDERLYINGS)]
  wanted=[n for n in wanted if n]
  if 'underlying_snapshots' not in self._tables():
   return self.envelope(rows=[{'underlying':n,'captured':False} for n in wanted])
  cut=max(1,min(int(points or 60),SERIES_LIMIT_MAX))
  as_of=self._max('underlying_snapshots')
  day=str(as_of or '')[:10]
  out=[]
  for name in wanted:
   latest=self._spot(name)
   series=[]
   for row in self._rows('select captured_at,spot,pcr_oi,pcr_volume,total_ce_oi,total_pe_oi,max_pain_strike'
     ' from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?'
     ' order by captured_at desc limit ?',(name,day,cut)):
    series.append({'captured_at':row.get('captured_at'),'spot':_round(row.get('spot')),
     'pcr_oi':_round(row.get('pcr_oi')),'pcr_volume':_round(row.get('pcr_volume')),
     'total_ce_oi':_int(row.get('total_ce_oi')),'total_pe_oi':_int(row.get('total_pe_oi')),
     'max_pain_strike':_num(row.get('max_pain_strike'))})
   series.reverse()
   # ONE CONVENTION FOR THE WHOLE TAB: the max-pain STRIKE MINUS SPOT, so a positive distance means the
   # strike sits above spot. This list used to compute spot minus strike - the opposite sign of what
   # `maxpain-series`, `oi-by-strike` and the metrics table itself all serve - which put two contradictory
   # readings of one number on one page under one heading.
   #
   # Both terms come from the SAME `underlying_snapshots` row, so they are one reading by construction;
   # when either is absent the distance is unavailable with the reason, never a subtraction across readings.
   distance,reason=None,None
   if latest.get('max_pain_strike') is None:reason='no_strike'
   elif latest.get('spot') is None:reason='no_spot_at_reading'
   else:distance=round(latest['max_pain_strike']-latest['spot'],2)
   expiry=self._front_expiry(name)
   session=session_date(latest.get('captured_at')) or session_date(as_of)
   out.append({'underlying':name,'captured':bool(latest),'captured_at':latest.get('captured_at'),
    'spot':latest.get('spot'),'pcr_oi':latest.get('pcr_oi'),'pcr_volume':latest.get('pcr_volume'),
    'max_pain_strike':latest.get('max_pain_strike'),'max_pain_distance':distance,
    'max_pain_distance_reason':(DISTANCE_REASONS.get(reason) if reason else None),
    'max_pain_distance_withheld':reason,
    'total_ce_oi':latest.get('total_ce_oi'),'total_pe_oi':latest.get('total_pe_oi'),
    'expiry':expiry,'days_to_expiry':session_days_to_expiry(expiry,session),
    'session':session or None,'series':series})
  return self.envelope(as_of=as_of,rows=out,missing=self._columns('metrics').missing,
   context=self.context(session=session_date(as_of),at=as_of or '',source='underlying_snapshots').as_dict(),
   max_pain_distance_definition=MAX_PAIN_DISTANCE_DEFINITION,
   distance_reason_text=dict(DISTANCE_REASONS),days_to_expiry_basis=DTE_CONVENTION)

 # --- card 5: futures build-up -----------------------------------------
 def futures(self,underlying='',watchlist='',limit=None):
  """§3.7 / §4 card 5: the front futures contract per underlying with its build-up label, OI share and basis."""
  rows,delegated=self._delegate('futures_buildup',underlying=underlying or None,limit=limit)
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=[],[]
   if underlying:clauses.append('c.underlying=?');params.append(underlying)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=('FUT',),
    order='order by c.expiry',underlying=underlying)
  names=self._watchlist(watchlist)
  front,out={},[]
  for row in shaped:
   if names is not None and row['underlying'].upper() not in names:continue
   current=front.get(row['underlying'])
   if current is None or (row.get('expiry') or '9999')<(current.get('expiry') or '9999'):front[row['underlying']]=row
  for row in front.values():
   basis=row.get('basis')
   if basis is None:
    spot=self._spot(row['underlying']).get('spot')
    # §3.7 defines basis as futures − spot. Served only when BOTH numbers were captured; never half of one.
    if spot is not None and row.get('last_price') is not None:basis=round(row['last_price']-spot,2)
   out.append({**row,'basis':basis})
  out.sort(key=lambda r:-(r.get('premium_cr') or 0) if r.get('premium_cr') is not None else 0)
  cut=max(1,min(int(limit or ROW_LIMIT_DEFAULT),ROW_LIMIT_MAX))
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out[:cut],total=len(out))

 # --- the linked chart --------------------------------------------------
 def series(self,underlying='',instrument_token=None,points=None):
  """The linked panel: one contract's 15-minute price + OI, or the underlying's spot + total OI.

  `candles_15m` (D1's backfill and its top-up) for a contract; `underlying_snapshots` for an underlying.
  Nothing is resampled, gap-filled or extended - the points are the captured marks, newest last.
  """
  cut=max(1,min(int(points or 120),SERIES_LIMIT_MAX))
  tables=self._tables()
  if instrument_token is not None and 'candles_15m' in tables:
   contract=self._one('select tradingsymbol,underlying,instrument_type,strike,expiry,lot_size from contracts'
    ' where instrument_token=?',(instrument_token,)) or {}
   rows=self._rows('select bar_start,open,high,low,close,volume,oi from candles_15m where instrument_token=?'
    ' order by bar_start desc limit ?',(instrument_token,cut))
   rows.reverse()
   points_out=[{'t':r.get('bar_start'),'price':_round(r.get('close')),'oi':_int(r.get('oi')),
    'volume':_int(r.get('volume'))} for r in rows]
   as_of=points_out[-1]['t'] if points_out else self._max('candles_15m','bar_start')
   return self.envelope(as_of=as_of,points=points_out,kind='contract',
    tradingsymbol=contract.get('tradingsymbol') or '',underlying=contract.get('underlying') or underlying,
    instrument_token=instrument_token,expiry=contract.get('expiry') or '',
    strike=_num(contract.get('strike')),instrument_type=contract.get('instrument_type') or '',
    price_label='Last price',oi_label='Open interest')
  if not underlying or 'underlying_snapshots' not in tables:
   return self.envelope(points=[],kind='none',underlying=underlying)
  rows=self._rows('select captured_at,spot,total_ce_oi,total_pe_oi,pcr_oi from underlying_snapshots'
   ' where underlying=? order by captured_at desc limit ?',(underlying,cut))
  rows.reverse()
  points_out=[]
  for row in rows:
   ce,pe=_int(row.get('total_ce_oi')),_int(row.get('total_pe_oi'))
   total=None if ce is None and pe is None else (ce or 0)+(pe or 0)
   points_out.append({'t':row.get('captured_at'),'price':_round(row.get('spot')),'oi':total,
    'ce_oi':ce,'pe_oi':pe,'pcr_oi':_round(row.get('pcr_oi'))})
  as_of=points_out[-1]['t'] if points_out else self._max('underlying_snapshots')
  return self.envelope(as_of=as_of,points=points_out,kind='underlying',underlying=underlying,
   price_label='Spot',oi_label='Total option OI (CE + PE)')

 # --- the futures price chart -------------------------------------------
 @staticmethod
 def clean_interval(value):
  """'15m' / '1d' as the chart names them, or '' - never a fragment of anything."""
  text=str(value or '').strip().lower()
  return text if text in CHART_INTERVALS else ''

 @staticmethod
 def interval_note(interval,symbol,sessions,candles,available):
  """The one plain sentence that describes an interval for THIS contract. Describes, never predicts."""
  sym=symbol or 'this contract'
  plural='' if sessions==1 else 's'
  if interval==INTERVAL_1D:
   if not available:
    return (f'No daily candles are stored for {sym} yet — daily history is fetched contract by contract, and '
     'this one has not been fetched.')
   return (f'One bar per trading session of {sym} itself — the contract, not the index, and not a stitched '
    f'continuous series. {sessions} trading session{plural} stored. A futures contract is listed for roughly three '
    'months, so its own daily history is short by nature, and it starts again from almost nothing each time '
    'the front contract rolls.')
  if not available:
   return f'No 15-minute candles are stored for {sym} yet — they arrive with the next 15-min reading.'
  return (f'15-minute candles of {sym} itself — the contract, not the index. {candles} stored bars across '
   f'{sessions} trading session{plural}. A reading with no bar keeps its place and draws nothing.')

 @staticmethod
 def short_history_text(symbol,interval,sessions,expiry,dte):
  """Why this series is short, in the owner's register. A short series is a fact, not an error."""
  sym=symbol or 'this contract'
  span=INTERVAL_LABELS.get(interval,interval).lower()
  plural='' if sessions==1 else 's'
  tail=''
  if expiry:
   away='' if dte is None else f' ({dte} day{"" if dte==1 else "s"} away)'
   tail=(f' {sym} expires on {expiry}{away}, and the front contract after it starts again with almost no '
    'history of its own.')
  return (f'Short series: {sessions} trading session{plural} of {span} candles for {sym}. That is too little '
   'history to read as a trend, so it is drawn as it is rather than padded out.'+tail)

 def _front_future(self,underlying,today=None):
  """The front futures contract of `underlying`: the nearest expiry not yet past.

  Falls back to the LATEST expiry stored when every futures contract we hold has expired - an expired contract
  disappears from the vendor but stays in this store, and serving the newest one we know is honest as long as
  its expiry travels with it, which it always does. None means this underlying has no future here at all.
  """
  if 'contracts' not in self._tables():return None
  day=(today or today_ist()).isoformat()
  columns='instrument_token,tradingsymbol,underlying,expiry'
  row=self._one(f"select {columns} from contracts where underlying=? and instrument_type='FUT'"
   " and expiry>=? order by expiry limit 1",(underlying,day))
  if row is None:
   row=self._one(f"select {columns} from contracts where underlying=? and instrument_type='FUT'"
    " order by expiry desc limit 1",(underlying,))
  return row

 def _chart_counts(self,token):
  """Rows and distinct trading days this contract actually has, per interval.

  This is what lets the page disable an interval control instead of offering a dead one.
  """
  tables=self._tables()
  out={}
  for key in CHART_INTERVALS:
   table,column=CHART_TABLES[key]
   if table not in tables:
    out[key]={'candles':0,'sessions':0,'available':False};continue
   row=self._one(f'select count(*) as n,count(distinct substr("{column}",1,10)) as d from "{table}"'
    ' where instrument_token=?',(token,)) or {}
   count=_int(row.get('n')) or 0
   out[key]={'candles':count,'sessions':_int(row.get('d')) or 0,'available':count>0}
  return out

 def _chart_from_store(self,underlying,interval):
  """One contract's own candles at one cadence, oldest first, read straight from the §2 store.

  Same shape and same definitions as `market_data.derivatives.read_api.futures_chart_series`; that module
  answers instead whenever it is importable. Nothing here resamples, interpolates or extends - the rows are
  the bars the store holds, and a missing session stays missing.
  """
  base={'underlying':underlying,'interval':interval,'contract':None,'candles':[],'sessions':0,
   'session':None,'as_of':None,'max_candles':CHART_MAX_CANDLES,
   'intervals':{k:{'candles':0,'sessions':0,'available':False} for k in CHART_INTERVALS}}
  contract=self._front_future(underlying)
  if contract is None:return base
  token=_int(contract.get('instrument_token'))
  base['contract']={'tradingsymbol':contract.get('tradingsymbol') or '','instrument_token':token,
   'expiry':clean_expiry(contract.get('expiry')) or None}
  if token is None:return base
  base['intervals']=self._chart_counts(token)
  if not (base['intervals'].get(interval) or {}).get('available'):return base
  table,column=CHART_TABLES[interval]
  rows=self._rows(f'select "{column}" as at,open,high,low,close,volume,oi from "{table}"'
   f' where instrument_token=? order by "{column}" desc limit ?',(token,CHART_MAX_CANDLES))
  rows.reverse()
  stored=[{'at':r.get('at'),'open':_round(r.get('open')),'high':_round(r.get('high')),
   'low':_round(r.get('low')),'close':_round(r.get('close')),'volume':_int(r.get('volume')),
   # unknown stays unknown: a session the vendor sent no open interest for is not a session with none
   'oi':_int(r.get('oi'))} for r in rows if r.get('at')]
  if not stored:return base
  grid=self._session_grid(table,column,stored[0]['at'],stored[-1]['at'])
  base['candles']=self.with_gaps(stored,grid)
  # `sessions` counts the distinct trading days the series SPANS, and a day counts only when a real bar landed
  # on it. A day made entirely of empty slots is a day this contract has no data for.
  days=sorted({str(c['at'])[:10] for c in stored})
  base['sessions'],base['session']=len(days),(days[-1] if days else None)
  base['as_of']=stored[-1]['at']
  return base

 def _session_grid(self,table,column,lo,hi):
  """Every reading the STORE ITSELF knows the exchange had between `lo` and `hi`.

  The union of the readings every contract in that table holds: a 15-min bar start exists there only because
  something traded at it, and a session date only because something traded that day. Same definition, same
  SQL, as `market_data.derivatives.read_api.session_grid`.

  The boundary, stated because it matters: a session missing from the WHOLE store cannot be told apart from a
  day the exchange was shut, so it is not reported as a gap. Only a reading the store holds for some other
  contract, and not for this one, is a gap.
  """
  rows=self._rows(f'select distinct "{column}" as at from "{table}" where "{column}">=? and "{column}"<=?'
   f' order by "{column}"',(lo,hi))
  return [r.get('at') for r in rows if r.get('at')]

 @staticmethod
 def with_gaps(stored,grid):
  """The series on the exchange's own grid: a reading with no bar keeps its slot, drawing nothing.

  Leaving a missing bar OUT would make its neighbours adjacent, and the chart would then claim continuous
  trading across a period that had none - an untrue picture that nothing downstream could detect, because the
  absence would be invisible by construction. The slot stays and its prices are null.

  A volume of 0 and an `oi` of null are different statements from \"no bar at all\": a bar that genuinely traded
  nothing is a real bar with real prices and is never turned into a gap.
  """
  have={c['at']:c for c in stored}
  out=[]
  for at in grid:
   row=have.get(at)
   if row is None:
    out.append({'at':at,'open':None,'high':None,'low':None,'close':None,'volume':None,'oi':None,'gap':True})
   else:out.append({**row,'gap':False})
  return out

 def _chart_delegate(self,underlying,interval):
  """D2's own reader, when `market_data.derivatives.read_api` is on this machine's path."""
  module=self.read_module()
  reader=getattr(module,'futures_chart_series',None) if module else None
  connection=self._connect()
  if reader is None or connection is None:return None
  try:
   raw=reader(connection,underlying,interval=interval,max_candles=CHART_MAX_CANDLES)
  except TypeError as error:
   LOG.info('derivatives: futures_chart_series not called - %s',error);return None
  except Exception as error:
   LOG.warning('derivatives: futures_chart_series failed (%s); reading the chart from the store.',error)
   return None
  # A reader whose shape this module does not recognise does not get to put a chart on screen: the store
  # answers instead, and `source` says which of the two did. The two paths can then never be out of step.
  if not isinstance(raw,dict) or not isinstance(raw.get('candles'),list):return None
  intervals=raw.get('intervals')
  if not isinstance(intervals,dict) or any(k not in intervals for k in CHART_INTERVALS):return None
  contract=raw.get('contract')
  if contract is not None and not isinstance(contract,dict):return None
  return raw

 def futures_chart(self,underlying,interval=DEFAULT_CHART_INTERVAL):
  """The futures price chart: the FRONT contract's own candles, 15-minute by default, daily as the alternative.

  Display only. It states how many sessions it is actually returning and which contract they belong to, so a
  short series reads as a short series - which is exactly what happens the day the front contract rolls, since
  the new front contract has almost no history of its own. Nothing here is a forecast (§5).
  """
  interval=self.clean_interval(interval) or DEFAULT_CHART_INTERVAL
  notes={'chart_text':CHART_TEXT,'gaps_text':GAPS_TEXT,'grid_source_text':GRID_SOURCE_TEXT,
   'short_history_sessions':SHORT_HISTORY_SESSIONS,'max_candles':CHART_MAX_CANDLES,
   'default_interval':DEFAULT_CHART_INTERVAL}
  if not underlying:
   return self.envelope(interval=interval,contract=None,candles=[],sessions=0,session=None,bars=0,gaps=0,
    short_history=False,short_history_text=None,intervals=[],note=None,unknown_underlying=False,
    underlying='',**notes)
  raw=self._chart_delegate(underlying,interval)
  source='metrics_module' if raw is not None else 'store'
  if raw is None:raw=self._chart_from_store(underlying,interval)
  contract=raw.get('contract') or None
  symbol=(contract or {}).get('tradingsymbol') or ''
  expiry=clean_expiry((contract or {}).get('expiry')) or ''
  counts=raw.get('intervals') or {}
  offered=[]
  for key in CHART_INTERVALS:
   row=counts.get(key) or {}
   sessions=_int(row.get('sessions')) or 0
   count=_int(row.get('candles')) or 0
   available=bool(row.get('available')) and count>0
   offered.append({'interval':key,'label':INTERVAL_LABELS[key],'available':available,
    'candles':count,'sessions':sessions,'selected':key==interval,
    'note':self.interval_note(key,symbol,sessions,count,available)})
  candles=[]
  for row in (raw.get('candles') or []):
   if not row.get('at'):continue
   close=_round(row.get('close'))
   # A slot with no close is not a candle: whichever path produced it, it is a gap, and it is flagged as one
   # here rather than trusted - so a delegate and the store fallback can never disagree about what a hole is.
   gap=close is None
   candles.append({'at':row.get('at'),'open':_round(row.get('open')),'high':_round(row.get('high')),
    'low':_round(row.get('low')),'close':close,'volume':(None if gap else _int(row.get('volume'))),
    'oi':(None if gap else _int(row.get('oi'))),'gap':gap})
  drawn=[c for c in candles if not c['gap']]
  # `sessions` is the number of distinct TRADING DAYS the candles span - days, not readings, at either
  # interval - and only a day a real bar landed on is counted. `bars` is the stored-bar count; the two are
  # different numbers at the 15-minute interval and are never conflated.
  days=sorted({str(c['at'])[:10] for c in drawn})
  sessions=len(days)
  # Counted from the LAST SESSION ON THE CHART, not from today (DTE_CONVENTION). A chart of last week's
  # sessions is not a chart of today, and its contract's days-to-expiry must not move with the clock.
  chart_session=days[-1] if days else ''
  dte=session_days_to_expiry(expiry,chart_session) if expiry else None
  short=bool(drawn) and sessions<SHORT_HISTORY_SESSIONS
  served=next((o for o in offered if o['interval']==interval),None)
  # An empty card must say WHY it is empty. The tab's one empty sentence is about the 15-min capture, and on a
  # store that HAS been captured it would be a false reason for a chart whose daily history simply has not been
  # fetched - so the interval's own note is the reason instead. A store that is not readable at all keeps the
  # tab's sentence, because then the sentence is true.
  reason=None
  if not drawn:reason=(served or {}).get('note') if self.available() else EMPTY_TEXT
  return self.envelope(as_of=(drawn[-1]['at'] if drawn else None),source=source,missing=[],
   empty_reason=reason,
   underlying=underlying,interval=interval,bars=len(drawn),gaps=len(candles)-len(drawn),
   contract=({'tradingsymbol':symbol,'instrument_token':_int((contract or {}).get('instrument_token')),
    'expiry':expiry or None,'days_to_expiry':dte,'days_to_expiry_basis':DTE_CONVENTION}
    if contract else None),
   context=self.context(underlying,expiry,chart_session,(drawn[-1]['at'] if drawn else ''),source).as_dict(),
   candles=candles,sessions=sessions,session=(chart_session or None),
   short_history=short,
   short_history_text=(self.short_history_text(symbol,interval,sessions,expiry,dte) if short else None),
   intervals=offered,note=(served or {}).get('note'),
   unknown_underlying=contract is None and self.available(),**notes)

 # --- the owner's ΔOI strike grid (2 × 5) --------------------------------
 @staticmethod
 def grid_label(option_type,offset):
  """"ATM CE", "ATM+3 CE", "ATM−2 PE" — the owner's own wording for a slot."""
  if not offset:return f'ATM {option_type}'
  return f'ATM{"+" if offset>0 else "−"}{abs(int(offset))} {option_type}'

 @staticmethod
 def grid_direction(points):
  """building / unwinding / flat / "no baseline", from the very points the line is drawn from.

  The rule, stated once: the latest ΔOI against the mark DIRECTION_LOOKBACK_MARKS back (one hour). Flat when
  |change| is under FLAT_FRACTION of that contract's OWN largest |ΔOI| today. Fewer than two marks carrying a
  ΔOI is "no baseline" — a state, never a fourth direction and never a chip.
  """
  usable=[p for p in points or () if p.get('delta_oi') is not None]
  if len(usable)<2:return 'no baseline',{'points_with_delta':len(usable)}
  index=max(0,len(usable)-1-DIRECTION_LOOKBACK_MARKS)
  latest,reference=usable[-1],usable[index]
  change=latest['delta_oi']-reference['delta_oi']
  scale=max(abs(p['delta_oi']) for p in usable)
  threshold=FLAT_FRACTION*scale
  label='flat' if (scale==0 or abs(change)<threshold) else ('building' if change>0 else 'unwinding')
  return label,{'from':reference['at'],'to':latest['at'],'change':_round(change),
   'flat_threshold':_round(threshold),'marks_back':len(usable)-1-index}

 @staticmethod
 def grid_price_direction(points):
  """up / down / flat / "no baseline" for the contract's OWN premium, from the very points the tile draws.

  The same shape as grid_direction, read on price: the latest price against the reading
  DIRECTION_LOOKBACK_MARKS back (one hour); flat when |change| is under PRICE_FLAT_FRACTION of that contract's
  own largest |price move from the day's first reading| today. Fewer than two readings carrying a price is
  "no baseline" — never a direction, never a guess.
  """
  usable=[p for p in points or () if _num(p.get('price')) is not None]
  if len(usable)<2:return 'no baseline',{'points_with_price':len(usable)}
  index=max(0,len(usable)-1-DIRECTION_LOOKBACK_MARKS)
  latest,reference=_num(usable[-1]['price']),_num(usable[index]['price'])
  first=_num(usable[0]['price'])
  change=latest-reference
  scale=max(abs(_num(p['price'])-first) for p in usable)
  threshold=PRICE_FLAT_FRACTION*scale
  label='flat' if (scale==0 or abs(change)<threshold) else ('up' if change>0 else 'down')
  return label,{'from':usable[index].get('at'),'to':usable[-1].get('at'),'price_change':_round(change),
   'price_change_pct':None if not reference else _round(change/reference*100),
   'price_flat_threshold':_round(threshold,4),'readings_back':len(usable)-1-index}

 @classmethod
 def grid_flow(cls,option_type,points):
  """Price and OI over the SAME window, read together: what is happening, and what it means.

  Identical mechanics for a call and a put, read on the option's own premium; the wording differs because
  writing a call and writing a put sit on opposite sides of the strike. Nothing here is a forecast and nothing
  here is a recommendation: each label names who appears to be doing what, at the readings on the tile.
  """
  oi_direction,oi_detail=cls.grid_direction(points)
  price_direction,price_detail=cls.grid_price_direction(points)
  kind=str(option_type or '').upper()
  if price_direction=='no baseline' or oi_direction=='no baseline':
   what,meaning=FLOW_NOT_ENOUGH,None
  else:
   # A straight lookup: FLOW_LABELS holds all nine combinations, so a flat axis has its OWN row and is never
   # collapsed into another one. An option type the table does not hold falls to "not enough", never to a call.
   what,meaning=FLOW_LABELS.get(f'{kind}|{price_direction}|{oi_direction}',(FLOW_NOT_ENOUGH,None))
  detail={'from':price_detail.get('from') or oi_detail.get('from'),
   'to':price_detail.get('to') or oi_detail.get('to'),
   'price_change':price_detail.get('price_change'),'price_change_pct':price_detail.get('price_change_pct'),
   'oi_change':oi_detail.get('change'),'price_flat_threshold':price_detail.get('price_flat_threshold'),
   'oi_flat_threshold':oi_detail.get('flat_threshold'),
   'readings_back':oi_detail.get('marks_back',price_detail.get('readings_back',DIRECTION_LOOKBACK_MARKS))}
  return {'price_direction':price_direction,'oi_direction':oi_direction,'what_label':what,'meaning':meaning,
   'detail':detail}

 @staticmethod
 def grid_block_read(rows):
  """The one line under the whole block, or '' — aggregated from the ten slots already on screen.

  No second query and no strike the grid is not showing: this sums the SAME window change each tile already
  carries. Both sides building, and neither side's total more than BLOCK_BALANCE_RATIO x the other's, is the
  only thing it will say. Anything else prints nothing rather than forcing a summary.
  """
  totals={'calls':0.0,'puts':0.0};counted={'calls':0,'puts':0}
  for slot in rows or ():
   change=_num(((slot or {}).get('flow') or {}).get('detail',{}).get('oi_change'))
   if change is None:continue
   side='puts' if (slot.get('row')=='puts') else 'calls'
   totals[side]+=change;counted[side]+=1
  if not counted['calls'] or not counted['puts']:return ''
  calls,puts=totals['calls'],totals['puts']
  if calls<=0 or puts<=0:return ''
  high,low=max(calls,puts),min(calls,puts)
  if low<=0 or high>BLOCK_BALANCE_RATIO*low:return ''
  return BLOCK_BOTH_BUILDING

 def _grid_session(self,at=''):
  """The day the grid describes: the session of the reading the TAB is on, else the newest captured day.

  `at` is the tab's own 15-min boundary. Without it this block resolved its own newest session while the
  chain and the screener beside it were on another, which is one page describing two different moments.
  """
  here=session_date(at)
  if here:return here
  newest=self._max('snapshots') or self._max('candles_15m','bar_start') or self._max('underlying_snapshots')
  return str(newest or '')[:10] or None


 # --- WHAT IS HAPPENING ACROSS THE BOOK ---------------------------------------------------------------
 #
 # One pass per reading, over every instrument the capture covered, computed HERE and served to every
 # reader. The browser used to do this one instrument at a time: two hundred requests to answer "is
 # anything happening", repeated per user, with no way to alert on the answer.
 #
 # FIVE QUERIES, NOT SIX HUNDRED. The naive shape is `oi_grid` in a loop, which is four queries per name.
 # This resolves the spot of every underlying, the front expiry of every underlying, the ten at-the-money
 # contracts of each, and then reads ALL their points and ALL their previous closes in one statement each.
 #
 # It adds no analytic: `session_events` is the tab's own walk (rule signal/2, FLOW_LABELS, the hour-wide
 # window, the 5% flat bands), and `server/tests/test_session_events.py` fails if it drifts from the
 # TypeScript the panel uses for the selected instrument.
 def events(self,at='',limit=None):
  """The market list: one row per instrument, at the reading the tab is on.

  COMPUTED ONCE PER READING. A 15-minute reading that has been captured does not change, so this walk over
  two hundred instruments is done once and handed to every reader after it. Measured cold at 2.8 seconds of
  SQLite and arithmetic — running that on every page load, per user, for an answer identical to all of
  them, is the shape this endpoint exists to replace.

  The entry is held for a short time rather than forever because a request with NO `at` means "the newest
  reading the store holds", and new marks land through the session.
  """
  session=self._grid_session(at)
  key=(session,str(at or ''))
  hit=self._events_cache.get(key)
  if hit and (time.time()-hit[0])<EVENTS_CACHE_TTL:
   body=dict(hit[1])
   if limit:body['rows']=body['rows'][:int(limit)]
   return body
  built=self._events(session,at)
  # a small, bounded store: the oldest half is dropped when it grows past the cap
  if len(self._events_cache)>EVENTS_CACHE_MAX:
   for stale in sorted(self._events_cache,key=lambda k:self._events_cache[k][0])[:EVENTS_CACHE_MAX//2]:
    self._events_cache.pop(stale,None)
  self._events_cache[key]=(time.time(),built)
  body=dict(built)
  if limit:body['rows']=body['rows'][:int(limit)]
  return body

 def _events(self,session,at=''):
  notes={'rule_version':session_events.RULE_VERSION,'window_minutes':session_events.WINDOW_MINUTES,
   'grid_width':GRID_WIDTH,'grid_slots':GRID_SLOTS}
  if not session:
   return self.envelope(rows=[],session=None,reading_at=None,instruments=0,measured=0,**notes)
  bound=str(at or '').strip() or f'{session}~'
  tables=self._tables()
  if 'contracts' not in tables or 'underlying_snapshots' not in tables:
   return self.envelope(rows=[],session=session,reading_at=None,instruments=0,measured=0,**notes)

  # 1. THE SPOT OF EVERY UNDERLYING at or before the boundary. Everything below hangs off the at-the-money
  #    strike, and the at-the-money strike hangs off the spot: a name with no spot at or before this
  #    reading cannot be placed on a ladder, and is reported as not measurable rather than as quiet.
  spots={}
  for row in self._rows('select underlying,captured_at,spot from underlying_snapshots'
   ' where substr(captured_at,1,10)=? and captured_at<=? and spot is not null order by captured_at',
   (session,bound)):
   name=clean_symbol(row.get('underlying'))
   value=_num(row.get('spot'))
   if name and value is not None:spots[name]=(value,row.get('captured_at'))
  if not spots:
   return self.envelope(rows=[],session=session,reading_at=None,instruments=0,measured=0,**notes)

  names=sorted(spots)
  marks=','.join('?' for _ in names)
  # 2. THE FRONT EXPIRY of each, from the contracts the store actually lists.
  today=today_ist().isoformat()
  fronts={}
  for row in self._rows(f'select underlying,min(expiry) as e from contracts'
   f" where underlying in ({marks}) and expiry>=? and instrument_type in ('CE','PE')"
   ' group by underlying',(*names,today)):
   name=clean_symbol(row.get('underlying'))
   expiry=clean_expiry(row.get('e'))
   if name and expiry:fronts[name]=expiry

  # 3. THE LADDER of each name at that expiry, and the ten slots around its spot.
  ladders={}
  for row in self._rows(f'select underlying,expiry,instrument_token,tradingsymbol,strike,instrument_type'
   f" from contracts where underlying in ({marks}) and instrument_type in ('CE','PE')",(*names,)):
   name=clean_symbol(row.get('underlying'))
   if not name or fronts.get(name)!=clean_expiry(row.get('expiry')):continue
   strike=_num(row.get('strike'))
   if strike is None:continue
   ladders.setdefault(name,[]).append(row)

  chosen={}   # underlying -> [(contract row, offset)]
  tokens=[]
  for name,rows in ladders.items():
   spot=spots[name][0]
   rungs=sorted({_num(r.get('strike')) for r in rows if _num(r.get('strike')) is not None})
   if not rungs:continue
   atm=min(rungs,key=lambda s:(abs(s-spot),-s))
   index=rungs.index(atm)
   wanted=[(atm,'CE',0),(atm,'PE',0)]
   for step in range(1,GRID_WIDTH+1):
    if index+step<len(rungs):wanted.append((rungs[index+step],'CE',step))
    if index-step>=0:wanted.append((rungs[index-step],'PE',-step))
   by_key={(_num(r.get('strike')),r.get('instrument_type')):r for r in rows}
   picked=[(by_key[(s,t)],off) for s,t,off in wanted if (s,t) in by_key]
   if not picked:continue
   chosen[name]=picked
   tokens.extend([_int(r.get('instrument_token')) for r,_ in picked if _int(r.get('instrument_token')) is not None])
  if not tokens:
   return self.envelope(rows=[],session=session,reading_at=None,instruments=len(names),measured=0,**notes)

  # 4 and 5. EVERY POINT and EVERY PREVIOUS CLOSE, in one statement each.
  points=self._events_points(tokens,session,bound)
  closes=self._grid_previous_close_oi(tokens,session)

  reading=None
  out=[]
  for name in sorted(chosen):
   # THE SAME ANCHOR THE ΔOI BLOCK USES, per instrument. That block stops at the newest reading which
   # carried a SPOT, because everything it draws hangs off the strike at the money. Reading further here
   # would put this list and the explanation beside it on two different moments for the same name — on
   # 18 Sep 2026, where NIFTY's capture died at 11:30 and the rest was rebuilt from candles without a
   # spot, that is a four-hour disagreement on one screen.
   anchor=spots[name][1]
   slots=[]
   for contract,offset in chosen[name]:
    token=_int(contract.get('instrument_token'))
    kind=contract.get('instrument_type') or ''
    close=closes.get(token)
    series=[]
    for point in points.get(token,()):
     mark=point.get('captured_at')
     if anchor and mark and str(mark)>str(anchor):continue
     oi=_num(point.get('oi'))
     series.append({'at':mark,'oi':None if oi is None else _int(oi),
      'delta_oi':None if (oi is None or close is None) else _int(oi-close),
      'price':_round(point.get('price'))})
    slots.append({'present':True,'row':'puts' if kind=='PE' else 'calls','option_type':kind,
     'strike':_num(contract.get('strike')),'instrument_token':token,
     'tradingsymbol':contract.get('tradingsymbol') or '','points':series})
   walk=session_events.observe(slots)
   if not walk:continue
   row=session_events.summarise(name,walk,slots)
   if not row:continue
   row['expiry']=fronts.get(name)
   row['spot']=_round(spots[name][0])
   row['spot_at']=spots[name][1]
   out.append(row)
   if row.get('at') and (reading is None or str(row['at'])>str(reading)):reading=row['at']
  # THE ORDER: the ones with something to say first, then by the size behind them. `weight` orders the
  # list and is never shown as a score - it is the sum of the position changes the row is built on.
  # `limit` is NOT applied here: the pass is always the whole book, and the caller trims after the cache.
  out.sort(key=lambda r:(0 if r.get('live') else 1,-(r.get('weight') or 0),r['underlying']))
  measured=sum(1 for r in out if r.get('live'))
  return self.envelope(as_of=reading,rows=out,session=session,reading_at=reading,
   instruments=len(names),measured=measured,**notes)

 def _events_points(self,tokens,session,mark):
  """Every 15-minute point of every token handed in, grouped by token, in capture order. One statement.

  The same preference `_grid_points` uses: the capture worker's own table first, the 15-minute backfill
  second. A mark with no row is simply absent - nothing here interpolates or carries forward.
  """
  tables=self._tables()
  out={}
  marks=','.join('?' for _ in tokens)
  rows=[]
  if 'snapshots' in tables:
   kind=' and mark_kind=?' if 'mark_kind' in self._column_names('snapshots') else ''
   args=[*tokens,session]+([MARK_BAR_CLOSE] if kind else [])+[mark]
   rows=self._rows(f'select instrument_token,captured_at,oi,last_price as price from snapshots'
    f' where instrument_token in ({marks})'
    f' and substr(captured_at,1,10)=?{kind} and captured_at<=? order by captured_at',tuple(args))
  if not rows and 'candles_15m' in tables:
   rows=self._rows(f'select instrument_token,bar_start as captured_at,oi,close as price from candles_15m'
    f' where instrument_token in ({marks}) and substr(bar_start,1,10)=? and bar_start<=? order by bar_start',
    (*tokens,session,mark))
  for row in rows:
   token=_int(row.get('instrument_token'))
   if token is None:continue
   out.setdefault(token,[]).append(row)
  return out

 def _grid_previous_close_oi(self,tokens,session):
  """Each contract's OI at the last bar of the last session BEFORE `session`, from `candles_15m`.

  ΔOI is measured against the previous CLOSE, never against the day's first mark. A contract the backfill has
  never covered simply has no entry here and its ΔOI stays None — "no baseline", never a zero.
  """
  if not tokens or 'candles_15m' not in self._tables():return {}
  marks=','.join('?' for _ in tokens)
  sql=(f'with prior as (select instrument_token,bar_start,oi,substr(bar_start,1,10) as d from candles_15m'
   f' where substr(bar_start,1,10)<? and instrument_token in ({marks})),'
   ' last_day as (select instrument_token,max(d) as d from prior group by instrument_token),'
   ' last_bar as (select p.instrument_token as instrument_token,max(p.bar_start) as bar_start from prior p'
   '  join last_day l on l.instrument_token=p.instrument_token and l.d=p.d group by p.instrument_token)'
   ' select c.instrument_token as instrument_token,c.oi as oi from candles_15m c join last_bar b'
   '  on b.instrument_token=c.instrument_token and b.bar_start=c.bar_start')
  out={}
  for row in self._rows(sql,(session,*tokens)):
   value=_num(row.get('oi'))
   if value is not None:out[_int(row.get('instrument_token'))]=value
  return out

 def _grid_points(self,tokens,session,mark):
  """{'source':…,'rows':[…]} — every 15-minute mark of `session` up to `mark`, in capture order.

  `snapshots` is the capture worker's own table and is preferred; a store that only holds the backfill is read
  from `candles_15m` instead, and the response says which. A mark with no row is simply absent — nothing here
  interpolates, carries forward or back-fills, so a gap in the capture stays a gap in the line.
  """
  tables=self._tables()
  marks=','.join('?' for _ in tokens)
  if 'snapshots' in tables:
   kind=' and mark_kind=?' if 'mark_kind' in self._column_names('snapshots') else ''
   args=[*tokens,session]+([MARK_BAR_CLOSE] if kind else [])+[mark]
   rows=self._rows(f'select instrument_token,captured_at,oi,last_price as price from snapshots'
    f' where instrument_token in ({marks})'
    f' and substr(captured_at,1,10)=?{kind} and captured_at<=? order by captured_at',tuple(args))
   if rows:return {'source':'snapshots','rows':rows}
  if 'candles_15m' in tables:
   rows=self._rows(f'select instrument_token,bar_start as captured_at,oi,close as price from candles_15m'
    f' where instrument_token in ({marks}) and substr(bar_start,1,10)=? and bar_start<=? order by bar_start',
    (*tokens,session,mark))
   if rows:return {'source':'candles_15m','rows':rows}
  return {'source':'none','rows':[]}

 def _grid_from_store(self,underlying,expiry='',at=''):
  """The ten at-the-money contracts and their ΔOI series, read straight from the §2 store.

  Same shape and same definitions as `market_data.derivatives.read_api.strike_oi_series`; that module answers
  instead whenever it is importable. Nothing is computed here beyond the one subtraction the definition IS
  (captured OI − captured previous-close OI) and the direction rule stated above.
  """
  base={'underlying':underlying,'expiry':expiry or None,'session':None,'as_of':None,'spot':None,
   'spot_symbol':None,'atm_strike':None,'atm_basis':None,'marks':[],'contracts':[],'points_source':'none',
   'note':None}
  at=str(at or '').strip()
  session=self._grid_session(at)
  if session is None:
   base['note']='the store has no captured readings yet';return base
  base['session']=session
  # THE BOUNDARY IS RESPECTED. The anchor reading is the newest one AT OR BEFORE the tab's own reading that
  # carried a spot - never one after it. A block reading past the boundary the rest of the page is on is how
  # two panels come to describe two different moments under one heading.
  bound=at or f'{session}~'
  # `spot_symbol` is an optional column, so the row is read whole and the field simply read off it.
  row=self._one('select * from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?'
   ' and captured_at<=? and spot is not null order by captured_at desc limit 1',(underlying,session,bound)) \
   if 'underlying_snapshots' in self._tables() else None
  mark,spot=(row or {}).get('captured_at'),_num((row or {}).get('spot'))
  if row is None or spot is None:
   # Everything in this block hangs off the ATM, and the ATM hangs off the spot.
   base['note']=(f'no spot was captured for {underlying} at any 15-min reading of {session} — the strike at the money '
    'cannot be identified, so no line is drawn')
   return base
  base['as_of'],base['spot'],base['spot_symbol']=mark,_round(spot),row.get('spot_symbol')
  expiry=clean_expiry(expiry) or self._front_expiry(underlying)
  base['expiry']=expiry or None
  if not expiry:
   base['note']=f'no option expiry is listed for {underlying}';return base
  listed=[r for r in self._rows('select instrument_token,tradingsymbol,strike,instrument_type from contracts'
   " where underlying=? and expiry=? and instrument_type in ('CE','PE')",(underlying,expiry))
   if _num(r.get('strike')) is not None]
  if not listed:
   base['note']=f'no option contracts are listed for {underlying} {expiry}';return base
  ladder=sorted({_num(r.get('strike')) for r in listed})
  atm=min(ladder,key=lambda s:(abs(s-spot),-s))
  base['atm_strike'],base['atm_basis']=atm,{'mark':mark,'spot':_round(spot),'rule':'listed strike nearest spot'}
  at=ladder.index(atm)
  wanted=[(atm,'CE',0),(atm,'PE',0)]
  for step in range(1,GRID_WIDTH+1):
   if at+step<len(ladder):wanted.append((ladder[at+step],'CE',step))
   if at-step>=0:wanted.append((ladder[at-step],'PE',-step))
  by_key={(_num(r.get('strike')),r.get('instrument_type')):r for r in listed}
  chosen=[(by_key[(s,t)],off) for s,t,off in wanted if (s,t) in by_key]
  tokens=[t for t in (_int(r.get('instrument_token')) for r,_ in chosen) if t is not None]
  if not tokens:
   base['note']=f'no instrument token is stored for the strikes at the money in {underlying} {expiry}';return base
  previous=self._grid_previous_close_oi(tokens,session)
  captured=self._grid_points(tokens,session,mark)
  base['points_source']=captured['source']
  series={t:[] for t in tokens}
  for point in captured['rows']:
   token=_int(point.get('instrument_token'))
   if token not in series:continue
   oi=_num(point.get('oi'))
   close=previous.get(token)
   series[token].append({'at':point.get('captured_at'),'oi':_int(oi),
    # a gap stays a gap, and a missing previous close stays missing — never measured off the day's first mark
    'delta_oi':None if (oi is None or close is None) else _int(oi-close),
    # the contract's OWN last traded price at this reading. Absent stays absent: never carried forward.
    'price':_round(point.get('price'))})
  contracts=[]
  for contract,offset in chosen:
   token=_int(contract.get('instrument_token'))
   points=series.get(token) or []
   direction,detail=self.grid_direction(points)
   kind=contract.get('instrument_type') or ''
   contracts.append({'tradingsymbol':contract.get('tradingsymbol') or '','instrument_token':token,
    'strike':_num(contract.get('strike')),'option_type':kind,
    'atm_offset':offset,'previous_close_oi':_int(previous.get(token)),'points':points,
    'direction':direction,'direction_detail':detail,'flow':self.grid_flow(kind,points)})
  base['contracts']=contracts
  base['marks']=sorted({p['at'] for c in contracts for p in c['points'] if p.get('at')})
  return base

 def _grid_delegate(self,underlying,expiry=''):
  """D2's own reader, when `market_data.derivatives.read_api` is on this machine's path."""
  module=self.read_module()
  reader=getattr(module,'strike_oi_series',None) if module else None
  connection=self._connect()
  if reader is None or connection is None:return None
  try:
   raw=reader(connection,underlying,expiry=clean_expiry(expiry) or None,width=GRID_WIDTH)
  except TypeError as error:
   LOG.info('derivatives: strike_oi_series not called - %s',error);return None
  except Exception as error:
   LOG.warning('derivatives: strike_oi_series failed (%s); reading the ΔOI grid from the store.',error);return None
  if not isinstance(raw,dict):return None
  out=dict(raw)
  out.setdefault('points_source','read_api')
  return out

 def oi_grid(self,underlying,expiry='',at=''):
  """The owner's ΔOI block: ten small series, five calls at and above the money, five puts at and below it.

  ΔOI is open interest added or removed since the previous close, at each 15-minute reading of the session.
  Ten slots are ALWAYS returned in the same order, so a strike the exchange does not list reads as a named
  gap instead of shifting the grid under the reader.

  `at` is the tab's own 15-min boundary. Omitted, this block resolves the newest captured session exactly as
  it did before; passed, it stops there - a block that ran on to a later reading than the chain and the
  screener beside it is one page describing two different moments.
  """
  notes={'delta_oi_text':DELTA_OI_TEXT,'atm_text':ATM_TEXT,'direction_text':DIRECTION_TEXT,
   'not_enough_marks':NOT_ENOUGH_MARKS,'direction_lookback_marks':DIRECTION_LOOKBACK_MARKS,
   'flat_fraction':FLAT_FRACTION,'grid_width':GRID_WIDTH,'grid_slots':GRID_SLOTS,
   'price_text':PRICE_TEXT,'flow_text':FLOW_TEXT,'block_text':BLOCK_TEXT,
   'price_flat_fraction':PRICE_FLAT_FRACTION,'block_balance_ratio':BLOCK_BALANCE_RATIO}
  if not underlying:
   return self.envelope(rows=[],underlying='',expiry='',session=None,spot=None,spot_symbol=None,
    atm_strike=None,atm_basis=None,marks=[],total=0,days_to_expiry=None,points_source='none',
    empty_note=None,empty_detail=None,block_read='',**notes)
  # A NAMED READING NEVER GOES THROUGH THE DELEGATE, for the same reason the chain's does not: D2's reader
  # answers for the newest reading it finds and has no parameter for a different one, so handing it the tab's
  # boundary would silently return the newest anyway and the block would look fixed while it still ran past.
  at=str(at or '').strip()
  raw=None if at else self._grid_delegate(underlying,expiry)
  source='metrics_module' if raw is not None else 'store'
  if raw is None:raw=self._grid_from_store(underlying,clean_expiry(expiry),at=at)
  found={}
  for contract in raw.get('contracts') or []:
   kind=str(contract.get('option_type') or '').upper()
   offset=_int(contract.get('atm_offset'))
   if kind in OPTION_TYPES and offset is not None:found[(kind,offset)]=contract
  expected=[('CE',n) for n in range(GRID_WIDTH+1)]+[('PE',0)]+[('PE',-n) for n in range(1,GRID_WIDTH+1)]
  expiry_out=clean_expiry(raw.get('expiry')) or ''
  rows,present=[],0
  for kind,offset in expected:
   contract=found.get((kind,offset))
   label=self.grid_label(kind,offset)
   slot={'slot':f'{kind}{offset:+d}','option_type':kind,'atm_offset':offset,'label':label,
    'row':'calls' if kind=='CE' else 'puts','present':contract is not None,'tradingsymbol':None,
    'instrument_token':None,'strike':None,'previous_close_oi':None,'points':[],'direction':'no baseline',
    'direction_detail':{'points_with_delta':0},'marks':0,'marks_with_delta':0,'latest_delta_oi':None,
    'peak_abs_delta_oi':None,'marks_with_price':0,'latest_price':None,
    'flow':{'price_direction':'no baseline','oi_direction':'no baseline','what_label':FLOW_NOT_ENOUGH,
     'meaning':None,'detail':{'points_with_delta':0,'points_with_price':0}},
    'missing_text':None if contract is not None else
     f'{label} is not a listed strike in {underlying}{" "+expiry_out if expiry_out else ""}.'}
   if contract is not None:
    present+=1
    points=[{'at':p.get('at'),'oi':_int(p.get('oi')),'delta_oi':_int(p.get('delta_oi')),
     'price':_round(p.get('price'))} for p in contract.get('points') or []]
    deltas=[p['delta_oi'] for p in points if p['delta_oi'] is not None]
    prices=[p['price'] for p in points if p['price'] is not None]
    direction=contract.get('direction')
    detail=contract.get('direction_detail') or {}
    if direction not in GRID_DIRECTIONS:direction,detail=self.grid_direction(points)
    flow=contract.get('flow')
    # A reader that predates the two-line tile (or serves a flow this module does not recognise) does not get to
    # put an unknown sentence on screen: the reading is recomputed from the very points the tile draws. The
    # WORDING is checked too, not only the shape - a delegate cannot smuggle a label this table does not hold.
    if not isinstance(flow,dict) or flow.get('price_direction') not in GRID_PRICE_DIRECTIONS \
       or flow.get('oi_direction') not in GRID_DIRECTIONS or flow.get('what_label') not in FLOW_WORDINGS:
     flow=self.grid_flow(kind,points)
    slot.update({'tradingsymbol':contract.get('tradingsymbol') or '',
     'instrument_token':_int(contract.get('instrument_token')),'strike':_num(contract.get('strike')),
     'previous_close_oi':_int(contract.get('previous_close_oi')),'points':points,'direction':direction,
     'direction_detail':detail,'marks':len(points),'marks_with_delta':len(deltas),
     'latest_delta_oi':deltas[-1] if deltas else None,
     'peak_abs_delta_oi':max((abs(d) for d in deltas),default=None),
     'marks_with_price':len(prices),'latest_price':prices[-1] if prices else None,'flow':flow})
   rows.append(slot)
  drawable=max((s['marks_with_delta'] for s in rows),default=0)
  # Two marks is the whole rule: one point is a dot, not a line, so the grid says the ONE sentence the owner
  # asked for rather than drawing one - and the reader's own explanation travels beside it as `empty_detail`,
  # so "why" is never lost and "not enough marks yet" is never dressed up as an error.
  thin=drawable<2
  note=NOT_ENOUGH_MARKS if thin else None
  detail=raw.get('note') or None
  if thin:rows=[]
  block=self.grid_block_read(rows)
  return self.envelope(as_of=raw.get('as_of'),source=source,missing=[],rows=rows,underlying=underlying,
   empty_detail=detail,block_read=block,
   expiry=expiry_out,session=raw.get('session'),spot=_round(raw.get('spot')),spot_symbol=raw.get('spot_symbol'),
   atm_strike=_num(raw.get('atm_strike')),atm_basis=raw.get('atm_basis'),marks=list(raw.get('marks') or []),
   total=present,
   days_to_expiry=(session_days_to_expiry(expiry_out,raw.get('session')) if expiry_out else None),
   days_to_expiry_basis=DTE_CONVENTION,
   context=self.context(underlying,expiry_out or '',raw.get('session') or '',raw.get('as_of') or '',
    source).as_dict(),
   points_source=raw.get('points_source') or 'none',empty_note=note,**notes)

 # ==============================================================================================================
 # The session series: PCR, max pain, implied volatility, futures build-up.
 #
 # All four read PRECOMPUTED rows. The `metrics` table the D2 worker writes carries a per-underlying row per
 # expiry per 15-minute reading (`scope='underlying'`, `metric_key='UNDERLYING|expiry'`) and a per-contract row
 # (`scope='contract'`, `metric_key=tradingsymbol`). Those two columns are the table's own PRIMARY KEY, so a
 # whole session of one series is a single index seek of a few dozen rows - which is why these routes answer in
 # tens of microseconds on a 269,578-row table and do not get slower as the store grows.
 #
 # A store written before `scope` existed (the shape the unit tests build) has no per-underlying rows at all;
 # for those the same series is read from `underlying_snapshots`, and `source` says which of the two answered.
 # ==============================================================================================================

 def _has_scope(self):
  """Does `metrics` carry the scope/metric_key key. False on a store written before the D2 worker did."""
  names=self._column_names('metrics')
  return 'scope' in names and 'metric_key' in names

 @staticmethod
 def series_direction(points,key,words='pcr'):
  """One direction over the last hour of a series, plus the numbers it was read from.

  The ΔOI grid's rule, applied to a series that does not start at zero: the latest reading against the
  reading DIRECTION_LOOKBACK_MARKS back; flat when |change| is under SERIES_FLAT_FRACTION of the series' own
  largest move away from its FIRST reading today. Fewer than two readings carrying a value is "no baseline".
  Describes what the number did. Never what it does next.
  """
  vocabulary=DIRECTION_WORDS.get(words) or DIRECTION_WORDS['pcr']
  usable=[p for p in points or () if _num(p.get(key)) is not None]
  if len(usable)<2:
   return vocabulary['none'],{'readings_with_value':len(usable),'rule':SERIES_DIRECTION_TEXT}
  index=max(0,len(usable)-1-DIRECTION_LOOKBACK_MARKS)
  latest,reference=_num(usable[-1][key]),_num(usable[index][key])
  first=_num(usable[0][key])
  change=latest-reference
  scale=max(abs(_num(p[key])-first) for p in usable)
  threshold=SERIES_FLAT_FRACTION*scale
  if scale==0 or abs(change)<threshold:label=vocabulary['flat']
  else:label=vocabulary['up'] if change>0 else vocabulary['down']
  return label,{'from':usable[index].get('at'),'to':usable[-1].get('at'),
   'from_value':_round(reference,4),'to_value':_round(latest,4),'change':_round(change,4),
   'change_pct':None if not reference else _round(change/abs(reference)*100,3),
   'flat_threshold':_round(threshold,6),'readings_back':len(usable)-1-index,
   'readings_with_value':len(usable),'rule':SERIES_DIRECTION_TEXT}

 def _session_readings(self,session):
  """Every 15-minute reading the STORE ITSELF knows that session had, oldest first.

  The union of the readings every underlying in `underlying_snapshots` holds, falling back to `metrics`. A
  reading exists there only because something was captured at it, so a reading missing from the WHOLE store
  cannot be told apart from a reading the exchange never had, and is not reported as a gap.
  """
  if not session:return []
  lo,hi=f'{session} ',f'{session}~'
  tables=self._tables()
  for table,column in (('underlying_snapshots','captured_at'),('metrics','captured_at')):
   if table not in tables:continue
   rows=self._rows(f'select distinct "{column}" as at from "{table}" where "{column}">=? and "{column}"<=?'
    f' order by "{column}"',(lo,hi))
   marks=[r['at'] for r in rows if r.get('at')]
   if marks:return marks
  return []

 #: How many listed expiries are tested for metric rows. An underlying carries a handful; the cap is here so a
 #: catalogue that ever grows a long tail cannot turn this into a scan.
 EXPIRY_CANDIDATES=24

 def _underlying_expiries(self,underlying):
  """The expiries this underlying has per-underlying metric rows for, nearest first, then the listed ones.

  Deliberately NOT `select distinct expiry from metrics where scope='underlying' and underlying=?`: that has
  no usable index (the table's key starts at `scope`), so it reads every per-underlying row in the store -
  4,026 of them today, more every session - and measured 23 ms per call. Instead the candidates come from
  `contracts` through `ix_contracts_underlying`, and each is confirmed with a PRIMARY-KEY seek that touches
  exactly one row. Same answer, ~0.1 ms, and it does not get slower as the store fills up.
  """
  listed=[e for e in (clean_expiry(r.get('expiry')) for r in self._rows(
   "select distinct expiry from contracts where underlying=? and instrument_type in ('CE','PE')"
   ' order by expiry limit ?',(underlying,self.EXPIRY_CANDIDATES))) if e]
  if self._has_scope() and 'metrics' in self._tables():
   captured=[e for e in listed
    if self._one("select 1 as ok from metrics where scope='underlying' and metric_key=? limit 1",
     (f'{underlying}|{e}',))]
   if captured:return captured
  today=today_ist().isoformat()
  return [e for e in listed if e>=today] or listed

 def _chain_rows(self,underlying,expiry):
  """Every per-underlying metric row this store holds for one chain, oldest first.

  ONE index seek: `metrics`'s primary key is (scope, metric_key, captured_at) and `metric_key` is exactly
  'UNDERLYING|expiry', so this reads the rows it returns and nothing else.
  """
  if not self._has_scope() or 'metrics' not in self._tables():return [],'store'
  names=self._column_names('metrics')
  wanted=('captured_at','pcr_oi','pcr_volume','pcr_trend','pcr_trend_change','total_ce_oi','total_pe_oi',
   'total_ce_volume','total_pe_volume','max_pain_strike','max_pain_distance','max_pain_total_oi',
   'max_pain_status','spot','contracts','days_to_expiry','premium_cr','oi_change_pct_day','expiry')
  have=[c for c in wanted if c in names]
  if 'captured_at' not in have:return [],'store'
  sql=('select '+','.join(f'"{c}"' for c in have)+" from metrics where scope='underlying' and metric_key=?"
   ' order by captured_at')
  return self._rows(sql,(f'{underlying}|{expiry}',)),'metrics'

 def _chain_rows_fallback(self,underlying,expiry):
  """The same series from `underlying_snapshots`, for a store with no per-underlying metric rows.

  `underlying_snapshots` is keyed (underlying, captured_at), so this is an index seek too. It carries fewer
  columns than `metrics` - no contract count, no max-pain total OI - and the ones it cannot supply stay null
  rather than being filled in from somewhere else.
  """
  if 'underlying_snapshots' not in self._tables():return []
  names=self._column_names('underlying_snapshots')
  wanted=('captured_at','spot','pcr_oi','pcr_volume','total_ce_oi','total_pe_oi','total_ce_volume',
   'total_pe_volume','max_pain_strike')
  have=[c for c in wanted if c in names]
  rows=self._rows('select '+','.join(f'"{c}"' for c in have)+' from underlying_snapshots where underlying=?'
   ' order by captured_at',(underlying,))
  return [{**r,'expiry':expiry} for r in rows]

 def _chain_session(self,underlying,expiry):
  """(rows of the newest captured session, that session, where they came from). Nothing older is mixed in."""
  rows,source=self._chain_rows(underlying,expiry)
  # The `underlying_snapshots` fallback holds ONE row per underlying per reading, with no expiry dimension at
  # all, so it cannot answer a question about one expiry. It is therefore used only on a store that has no
  # per-underlying metric rows to begin with - the older shape - and never as a stand-in for an expiry this
  # store simply does not hold a chain for. An expiry with no rows is an empty series, not another expiry's
  # numbers wearing the requested date.
  if not rows and not self._has_scope():
   rows=self._chain_rows_fallback(underlying,expiry)
   source='underlying_snapshots' if rows else 'store'
  if not rows:return [],None,source
  session=str(rows[-1].get('captured_at') or '')[:10]
  if not session:return [],None,source
  return [r for r in rows if str(r.get('captured_at') or '').startswith(session)],session,source

 @staticmethod
 def _chain_withheld(row):
  """Why a chain-level figure is withheld at this reading, or None when it may be printed.

  Order matters and is stated: a reading with no row at all is "no reading"; a chain under the contract floor
  is "thin chain"; a chain with an empty leg is "one side empty". A figure is never printed and then
  qualified - it is withheld or it is served.
  """
  if row is None:return 'no_reading'
  contracts=_int(row.get('contracts'))
  if contracts is not None and contracts<CHAIN_MIN_CONTRACTS:return 'thin_chain'
  ce,pe=_num(row.get('total_ce_oi')),_num(row.get('total_pe_oi'))
  if (ce is not None and ce<=0) or (pe is not None and pe<=0):return 'one_side_empty'
  return None

 @staticmethod
 def _delta_against(value,baseline,places=4):
  """(delta, reason) for ONE figure against the previous session's close. ΔOI's rule, nothing added.

  Three answers and only three: a number, `no_value` when this reading carries nothing to measure, and
  `no_baseline` when no previous close is stored. A missing baseline never becomes a zero — a 0 in a Δ is a
  claim that the figure did not move, and an absent baseline makes no claim at all.
  """
  if value is None:return None,'no_value'
  if baseline is None:return None,'no_baseline'
  return _round(value-baseline,places),None

 def _chain_previous_close(self,underlying,expiry,session,source):
  """The per-underlying row of the LAST 15-min reading BEFORE `session`, or None. The chain's ΔOI baseline.

  `metrics`'s primary key is (scope, metric_key, captured_at), so the newest row strictly before the session's
  first second IS the previous session's closing reading — one index seek, whatever the store's size. A store
  that holds nothing before this session has no baseline at all, which is a null Δ with a reason, not a 0.
  """
  if not session or not underlying:return None
  if source=='underlying_snapshots':
   if 'underlying_snapshots' not in self._tables():return None
   return self._one('select * from underlying_snapshots where underlying=? and captured_at<?'
    ' order by captured_at desc limit 1',(underlying,f'{session} '))
  if not expiry or not self._has_scope() or 'metrics' not in self._tables():return None
  return self._one("select * from metrics where scope='underlying' and metric_key=? and captured_at<?"
   ' order by captured_at desc limit 1',(f'{underlying}|{expiry}',f'{session} '))

 def _chain_baseline(self,underlying,expiry,session,source):
  """{'row','at','withheld','reason'} for the chain's previous close, with the SAME gate the readings get.

  A previous close that would have been withheld at its own reading — a chain too thin to carry a ratio, a
  side with no open interest — is not quietly promoted into a baseline. It is refused for the same reason,
  and every Δ that would have rested on it is null with that reason beside it.
  """
  row=self._chain_previous_close(underlying,expiry,session,source)
  if row is None:
   return {'row':None,'at':None,'withheld':None,'reason':SERIES_DELTA_REASONS['no_baseline']}
  withheld=self._chain_withheld(row)
  at=str(row.get('captured_at') or '') or None
  if withheld:
   return {'row':None,'at':at,'withheld':withheld,
    'reason':WITHHELD_REASONS.get(withheld) or SERIES_DELTA_REASONS['no_baseline']}
  return {'row':row,'at':at,'withheld':None,'reason':None,'present':True}

 @staticmethod
 def _delta_notes(unit,unit_text,baseline,extra=None):
  """The shared Δ header every series carries: what a Δ IS here, what it is measured in, and its baseline."""
  out={'delta_text':SERIES_DELTA_TEXT,'delta_unit':unit,'delta_unit_text':unit_text,
   'delta_reason_text':dict(SERIES_DELTA_REASONS),
   'previous_close_at':(baseline or {}).get('at'),
   # A baseline that is not there ALWAYS says so. A blank reason beside a null Δ is the one thing this must
   # never serve: it reads as "no change" to anyone who does not know the difference.
   'previous_close_reason':(baseline or {}).get('reason') or (None if (baseline or {}).get('present')
    else SERIES_DELTA_REASONS['no_baseline']),
   'previous_close_withheld':(baseline or {}).get('withheld')}
  out.update(extra or {})
  return out

 def _series_envelope(self,underlying,expiry,session,source,extra):
  """The shared header every one of the four series carries: what it is, and what it is not.

  One correction happens here rather than in four places: `underlying_snapshots` has no expiry dimension, so
  a series read from it is the whole underlying across every listed expiry. It is served with `expiry: null`
  and `expiry_basis` saying so, rather than carrying a date it cannot testify to.
  """
  basis='one expiry'
  if source=='underlying_snapshots':
   expiry='';basis=('all expiries — this store has no per-expiry chain, so these numbers are the underlying '
    'across every listed expiry at that reading')
  # DAYS TO EXPIRY IS COUNTED FROM THE SESSION ON SCREEN, not from today. Counted from today, the same
  # 22 Sep option read as 4 days on a panel that used the captured date and 2 on a panel that used the clock.
  return {'underlying':underlying,'expiry':expiry or None,'expiry_basis':basis,'session':session,
   'days_to_expiry':session_days_to_expiry(expiry,session) if expiry else None,
   'days_to_expiry_basis':DTE_CONVENTION,
   'direction_text':SERIES_DIRECTION_TEXT,'direction_labels':dict(DIRECTION_LABELS),
   'gaps_text':SERIES_GAPS_TEXT,'direction_lookback_marks':DIRECTION_LOOKBACK_MARKS,
   'flat_fraction':SERIES_FLAT_FRACTION,'series_source':source,**extra}

 # --- 1. PCR through the session --------------------------------------------------------------------------
 def pcr_series(self,underlying,expiry=''):
  """§3.5: OI PCR and volume PCR at every 15-minute reading of the newest captured session, oldest first.

  PCR is the sum of put open interest divided by the sum of call open interest (and the same on volume) for
  ONE underlying and ONE expiry. It is read, never recomputed here. A thin chain has its ratio WITHHELD with
  a named reason rather than printed: a put-call ratio resting on three contracts is a number about three
  contracts, not about a market. Display only - it describes what the ratio did, never what it does next.
  """
  underlying=clean_symbol(underlying)
  blank={'direction':'no baseline','direction_detail':{},'direction_label':'No baseline','total_readings':0,
   'readings_with_value':0,'expiries':[],'withheld_reasons':{},'reason_text':dict(WITHHELD_REASONS),
   'chain_floors':dict(CHAIN_FLOORS),'chain_floors_text':CHAIN_FLOORS_TEXT,
   'direction_words':dict(DIRECTION_WORDS['pcr']),'volume_direction':'no baseline',
   'volume_direction_detail':{},'volume_direction_label':'No baseline',
   'latest_pcr_oi':None,'latest_pcr_volume':None,'definition':PCR_DEFINITION,
   'readings_with_delta':0,'latest_delta_pcr_oi':None,'latest_delta_pcr_volume':None,
   'previous_close_pcr_oi':None,'previous_close_pcr_volume':None,
   **self._delta_notes(PCR_DELTA_UNIT,PCR_DELTA_TEXT,None)}
  if not underlying:
   return self.envelope(points=[],context=self.context().as_dict(),**self._series_envelope('','',None,'none',blank))
  expiries=self._underlying_expiries(underlying)
  chosen=clean_expiry(expiry) or (expiries[0] if expiries else '')
  rows,session,source=self._chain_session(underlying,chosen) if chosen else ([],None,'store')
  by_mark={str(r.get('captured_at')):r for r in rows}
  grid=self._session_readings(session) or sorted(by_mark)
  # ΔOI's baseline, read for the chain: the last reading of the session BEFORE this one. Withheld there is
  # withheld here — a ratio that was refused at its own reading does not become a baseline for every other.
  baseline=self._chain_baseline(underlying,chosen,session,source)
  base_oi=_round((baseline['row'] or {}).get('pcr_oi'),4) if baseline['row'] else None
  base_volume=_round((baseline['row'] or {}).get('pcr_volume'),4) if baseline['row'] else None
  points,withheld=[],{}
  for at in grid:
   row=by_mark.get(at)
   reason=self._chain_withheld(row)
   point={'at':at,'pcr_oi':None,'pcr_volume':None,'total_ce_oi':None,'total_pe_oi':None,
    'contracts':_int((row or {}).get('contracts')),'gap':row is None,'withheld':reason}
   if reason is None:
    point.update({'pcr_oi':_round(row.get('pcr_oi'),4),'pcr_volume':_round(row.get('pcr_volume'),4),
     'total_ce_oi':_int(row.get('total_ce_oi')),'total_pe_oi':_int(row.get('total_pe_oi'))})
    if point['pcr_oi'] is None and point['pcr_volume'] is None:
     point['withheld']=reason='not_computed'
   # A gap stays a gap: a reading with no ratio gets no Δ, and no previous close gets no Δ either.
   delta_oi,why_oi=self._delta_against(point['pcr_oi'],base_oi)
   delta_volume,why_volume=self._delta_against(point['pcr_volume'],base_volume)
   point.update({'delta_pcr_oi':delta_oi,'delta_pcr_oi_reason':why_oi,
    'delta_pcr_volume':delta_volume,'delta_pcr_volume_reason':why_volume})
   if reason:withheld[reason]=withheld.get(reason,0)+1
   points.append(point)
  direction,detail=self.series_direction(points,'pcr_oi','pcr')
  volume_direction,volume_detail=self.series_direction(points,'pcr_volume','pcr')
  latest=next((p for p in reversed(points) if p['pcr_oi'] is not None),None)
  # each figure from the newest reading that HAS it, and each saying which reading that was
  oi_value,oi_at=self._latest_at(points,'pcr_oi')
  vol_value,vol_at=self._latest_at(points,'pcr_volume')
  ce_value,ce_at=self._latest_at(points,'total_ce_oi')
  pe_value,pe_at=self._latest_at(points,'total_pe_oi')
  delta_oi_value,delta_oi_at=self._latest_at(points,'delta_pcr_oi')
  delta_vol_value,delta_vol_at=self._latest_at(points,'delta_pcr_volume')
  return self.envelope(as_of=(points[-1]['at'] if points else None),source=source,missing=[],points=points,
   context=self.context(underlying,chosen,session,(points[-1]['at'] if points else ''),source).as_dict(),
   **self._series_envelope(underlying,chosen,session,source,{
    'direction':direction,'direction_detail':detail,'direction_label':DIRECTION_LABELS.get(direction,direction),
    'volume_direction':volume_direction,'volume_direction_detail':volume_detail,
    'volume_direction_label':DIRECTION_LABELS.get(volume_direction,volume_direction),
    'direction_words':dict(DIRECTION_WORDS['pcr']),
    'latest_pcr_oi':oi_value,'latest_pcr_oi_at':oi_at,
    'latest_pcr_volume':vol_value,'latest_pcr_volume_at':vol_at,
    'latest_total_ce_oi':ce_value,'latest_total_ce_oi_at':ce_at,
    'latest_total_pe_oi':pe_value,'latest_total_pe_oi_at':pe_at,
    # the Δ is resolved on its own reading too, exactly as every other figure on this tab is
    'latest_delta_pcr_oi':delta_oi_value,'latest_delta_pcr_oi_at':delta_oi_at,
    'latest_delta_pcr_volume':delta_vol_value,'latest_delta_pcr_volume_at':delta_vol_at,
    'previous_close_pcr_oi':base_oi,'previous_close_pcr_volume':base_volume,
    'readings_with_delta':sum(1 for p in points if p['delta_pcr_oi'] is not None),
    **self._delta_notes(PCR_DELTA_UNIT,PCR_DELTA_TEXT,baseline),
    'total_readings':len(points),
    'readings_with_value':sum(1 for p in points if p['pcr_oi'] is not None),
    'expiries':expiries,'withheld_reasons':withheld,'reason_text':dict(WITHHELD_REASONS),
    'chain_floors':dict(CHAIN_FLOORS),'chain_floors_text':CHAIN_FLOORS_TEXT,
    'definition':PCR_DEFINITION}))

 # --- 2. Max pain through the session ---------------------------------------------------------------------
 def maxpain_series(self,underlying,expiry=''):
  """§3.6: the max-pain strike, the spot beside it, their distance and the OI it rests on, per reading.

  Max pain is the strike at which the total payout to option buyers at expiry is smallest, from the open
  interest standing across every strike of that expiry at that reading. It is read from the metrics worker,
  never recomputed here. It moves as open interest moves; the direction says which way it moved over the last
  hour and nothing else.
  """
  underlying=clean_symbol(underlying)
  blank={'direction':'no baseline','direction_detail':{},'direction_label':'No baseline','total_readings':0,
   'readings_with_value':0,'expiries':[],'withheld_reasons':{},'reason_text':dict(WITHHELD_REASONS),
   'chain_floors':dict(CHAIN_FLOORS),'chain_floors_text':CHAIN_FLOORS_TEXT,
   'direction_words':dict(DIRECTION_WORDS['max_pain']),'latest_max_pain_strike':None,'latest_spot':None,
   'latest_distance':None,'latest_distance_reason':DISTANCE_REASONS['no_strike'],
   'latest_distance_withheld':'no_strike','latest_complete_pair':None,
   'latest_complete_pair_text':COMPLETE_PAIR_TEXT,'distance_reason_text':dict(DISTANCE_REASONS),
   'latest_total_oi':None,'distance_definition':MAX_PAIN_DISTANCE_DEFINITION,
   'definition':MAX_PAIN_DEFINITION,'readings_with_delta':0,'latest_delta_max_pain_strike':None,
   'previous_close_max_pain_strike':None,
   **self._delta_notes(MAX_PAIN_DELTA_UNIT,MAX_PAIN_DELTA_TEXT,None)}
  if not underlying:
   return self.envelope(points=[],context=self.context().as_dict(),**self._series_envelope('','',None,'none',blank))
  expiries=self._underlying_expiries(underlying)
  chosen=clean_expiry(expiry) or (expiries[0] if expiries else '')
  rows,session,source=self._chain_session(underlying,chosen) if chosen else ([],None,'store')
  by_mark={str(r.get('captured_at')):r for r in rows}
  grid=self._session_readings(session) or sorted(by_mark)
  # The previous session's CLOSING max-pain strike. A baseline the worker itself marked unusable, or that the
  # chain floors would have refused, is no baseline at all — a strike is not carried over just to have one.
  baseline=self._chain_baseline(underlying,chosen,session,source)
  base_row=baseline['row']
  if base_row is not None and str(base_row.get('max_pain_status') or '').strip().lower() not in ('','ok'):
   baseline={'row':None,'at':baseline['at'],'withheld':'store_status',
    'reason':WITHHELD_REASONS['store_status']}
   base_row=None
  base_strike=_num((base_row or {}).get('max_pain_strike')) if base_row is not None else None
  if base_row is not None and base_strike is None:
   baseline={'row':None,'at':baseline['at'],'withheld':'not_computed',
    'reason':WITHHELD_REASONS['not_computed']}
  points,withheld=[],{}
  for at in grid:
   row=by_mark.get(at)
   reason=self._chain_withheld(row)
   spot=_round((row or {}).get('spot'))
   point={'at':at,'max_pain_strike':None,'spot':spot,'distance':None,'total_oi':None,
    'distance_computed':False,'contracts':_int((row or {}).get('contracts')),
    'gap':row is None,'withheld':reason,'store_status':None}
   if reason is None:
    status=str(row.get('max_pain_status') or '').strip().lower()
    point['store_status']=status or None
    if status and status!='ok':
     point['withheld']=reason='store_status'
   if reason is None:
    strike=_num(row.get('max_pain_strike'))
    if strike is None:
     point['withheld']=reason='not_computed'
    else:
     distance=_num(row.get('max_pain_distance'))
     if distance is None and spot is not None:
      distance=strike-spot;point['distance_computed']=True
     point.update({'max_pain_strike':strike,'distance':_round(distance,4),
      'total_oi':_int(row.get('max_pain_total_oi'))})
   # In STRIKE POINTS, never a percentage: max pain is a strike and it moves in whole strike steps.
   delta,why=self._delta_against(point['max_pain_strike'],base_strike,2)
   point.update({'delta_max_pain_strike':delta,'delta_max_pain_strike_reason':why})
   if reason:withheld[reason]=withheld.get(reason,0)+1
   points.append(point)
  direction,detail=self.series_direction(points,'max_pain_strike','max_pain')
  pain_value,pain_at=self._latest_at(points,'max_pain_strike')
  spot_value,spot_at=self._latest_at(points,'spot')
  # ======================================================================================================
  # THE DISTANCE BELONGS TO THE STRIKE'S OWN READING, OR IT DOES NOT EXIST.
  #
  # `latest_distance` used to be resolved the way every other figure here is - walk back to the newest
  # reading that HAS one. For a strike and a spot that is right: each is a measurement of its own and says
  # which reading it came from. For a distance it is wrong, because a distance is not a measurement, it is
  # a RELATIONSHIP between two of them. The newest reading with a max-pain strike on 18 Sep 2026 was 15:45
  # (23,350); the newest reading with a distance was 11:30 (-2, from 23,300 against a spot of 23,302). Put
  # under one heading they read as "23,350, 2 below spot", a sentence that was true of no reading at all.
  #
  # So the distance is resolved AT the strike's reading and nowhere else. No spot there means no distance,
  # with the reason printed where the number would have been.
  # ======================================================================================================
  gap_at,gap_value,gap_reason=pain_at,None,None
  if pain_at is None:gap_reason,gap_at='no_strike',None
  else:
   gap_value=self._figure_at(points,pain_at,'distance')
   if gap_value is None:
    gap_reason='no_spot' if spot_at is None else 'no_spot_at_reading'
    gap_at=None
  # The clearly-labelled ALTERNATIVE, never a substitution: the newest reading that carried BOTH, with its
  # own time on it, so a reader who wants a coherent pair can have one and can see which reading it is.
  pair=self._latest_pair(points,'max_pain_strike','spot','distance')
  oi_value,oi_at=self._latest_at(points,'total_oi')
  delta_value,delta_at=self._latest_at(points,'delta_max_pain_strike')
  return self.envelope(as_of=(points[-1]['at'] if points else None),source=source,missing=[],points=points,
   context=self.context(underlying,chosen,session,(points[-1]['at'] if points else ''),source).as_dict(),
   **self._series_envelope(underlying,chosen,session,source,{
    'direction':direction,'direction_detail':detail,'direction_label':DIRECTION_LABELS.get(direction,direction),
    'direction_words':dict(DIRECTION_WORDS['max_pain']),
    'latest_max_pain_strike':pain_value,'latest_max_pain_strike_at':pain_at,
    'latest_spot':spot_value,'latest_spot_at':spot_at,
    'latest_distance':gap_value,'latest_distance_at':gap_at,
    'latest_distance_reason':(DISTANCE_REASONS.get(gap_reason) if gap_reason else None),
    'latest_distance_withheld':gap_reason,
    'latest_complete_pair':pair,'latest_complete_pair_text':COMPLETE_PAIR_TEXT,
    'distance_reason_text':dict(DISTANCE_REASONS),
    'latest_total_oi':oi_value,'latest_total_oi_at':oi_at,
    'latest_delta_max_pain_strike':delta_value,'latest_delta_max_pain_strike_at':delta_at,
    'previous_close_max_pain_strike':base_strike,
    'readings_with_delta':sum(1 for p in points if p['delta_max_pain_strike'] is not None),
    **self._delta_notes(MAX_PAIN_DELTA_UNIT,MAX_PAIN_DELTA_TEXT,baseline),
    'total_readings':len(points),
    'readings_with_value':sum(1 for p in points if p['max_pain_strike'] is not None),
    'expiries':expiries,'withheld_reasons':withheld,'reason_text':dict(WITHHELD_REASONS),
    'chain_floors':dict(CHAIN_FLOORS),'chain_floors_text':CHAIN_FLOORS_TEXT,
    'distance_definition':MAX_PAIN_DISTANCE_DEFINITION,'definition':MAX_PAIN_DEFINITION}))

 # --- 3. Implied volatility through the session -----------------------------------------------------------
 # The ONE number on this tab that is a model output. Kite does not supply implied volatility, so it cannot be
 # read; it is solved from the option's own last traded price by `implied_vol.py`. Everything about that is
 # said out loud on every response: `computed: true`, the model, the rate and where the rate came from, and a
 # plain sentence the card prints. Nothing computed here is written back to the store, so a reader can never
 # meet a computed figure sitting in a column beside the exchange's own.

 def _contract_session_rows(self,tradingsymbol,session=None):
  """One contract's own metric rows, oldest first. A primary-key seek, whatever the store's size."""
  if not self._has_scope() or 'metrics' not in self._tables():return []
  names=self._column_names('metrics')
  wanted=('captured_at','last_price','average_price','oi','volume','spot','strike','expiry','days_to_expiry',
   'instrument_type','instrument_token','lot_size','tradingsymbol','underlying','premium_cr','basis',
   'basis_pct','basis_status','fut_oi_avg','fut_oi_vs_avg','fut_oi_vs_avg_status','oi_change_pct_15m',
   'oi_change_pct_day','oi_change_15m','oi_change_day','buildup_15m','buildup_day','price_change_pct_15m',
   'price_change_pct_day')
  have=[c for c in wanted if c in names]
  rows=self._rows('select '+','.join(f'"{c}"' for c in have)+" from metrics where scope='contract'"
   ' and metric_key=? order by captured_at',(tradingsymbol,))
  if not rows:return []
  day=session or str(rows[-1].get('captured_at') or '')[:10]
  return [r for r in rows if str(r.get('captured_at') or '').startswith(day)] if day else rows

 def _last_trade_times(self,token,session):
  """{reading: last trade time} from `snapshots`, for the staleness gate. Absent stays absent.

  `snapshots` is keyed (instrument_token, captured_at), so this is one index seek. The capture worker does
  not always get a last-trade time from the vendor; when it did not, the reading's staleness is UNKNOWN and
  is reported as unknown rather than assumed to be fine or assumed to be stale.
  """
  if 'snapshots' not in self._tables() or token is None:return {}
  names=self._column_names('snapshots')
  if 'last_trade_time' not in names:return {}
  rows=self._rows('select captured_at,last_trade_time from snapshots where instrument_token=?'
   ' and captured_at>=? and captured_at<=?',(token,f'{session} ',f'{session}~'))
  return {r['captured_at']:r.get('last_trade_time') for r in rows if r.get('captured_at')}

 @staticmethod
 def _parse_stamp(value):
  """A store timestamp as a datetime, or None. Naive IST, exactly as the capture worker writes it."""
  text=str(value or '').strip().replace('T',' ')[:19]
  for shape in ('%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M'):
   try:return datetime.strptime(text,shape)
   except (TypeError,ValueError):continue
  return None

 @classmethod
 def _expiry_moment(cls,expiry):
  """The moment an expiry settles: EXPIRY_TIME_IST on the expiry date, naive IST like every stored stamp."""
  text=clean_expiry(expiry)
  if not text:return None
  return cls._parse_stamp(f'{text} {IV.EXPIRY_TIME_IST}:00')

 def _iv_points(self,rows,token,session,option_type,rate):
  """One contract's implied volatility at each of its readings, with the reason for every blank.

  A point ALWAYS carries either an `iv` or a `reason`. Nothing is interpolated and nothing is carried
  forward: a reading whose price cannot be solved is a reading with no implied volatility, not the previous
  reading's number wearing a new timestamp.
  """
  stamps=self._last_trade_times(token,session)
  out,reasons,moments=[],{},{}
  for row in rows:
   at=row.get('captured_at')
   mark=self._parse_stamp(at)
   # One expiry per contract, so the settlement moment is parsed once rather than at every reading.
   expiry=row.get('expiry')
   if expiry not in moments:moments[expiry]=self._expiry_moment(expiry)
   moment=moments[expiry]
   years=IV.years_to_expiry(mark,moment) if (mark and moment) else None
   last_trade=self._parse_stamp(stamps.get(at))
   # How long BEFORE the reading's label the last trade happened. The capture runs a minute or two after each
   # bar closes, so a trade that landed inside the bar can carry a stamp LATER than the label and the number
   # comes out negative. That is fresher than the reading, not staler, so the gate sees zero while the signed
   # figure is still reported - a reader can see the capture lag rather than have it rounded away.
   before_mark=None if (last_trade is None or mark is None) else (mark-last_trade).total_seconds()
   age=None if before_mark is None else max(0.0,before_mark)
   solved=IV.solve(_num(row.get('last_price')),_num(row.get('spot')),_num(row.get('strike')),years,
    option_type,rate=rate,days_to_expiry=_int(row.get('days_to_expiry')),seconds_since_last_trade=age)
   reason=solved.get('reason')
   if reason:reasons[reason]=reasons.get(reason,0)+1
   sensitivity=solved.get('rate_sensitivity') or {}
   out.append({'at':at,'iv':_round(solved.get('iv'),6),'iv_pct':_round(solved.get('iv_pct'),4),
    'computed':solved.get('iv') is not None,'reason':reason,
    'reason_text':IV.REASONS.get(reason) if reason else None,
    'last_price':_round(row.get('last_price')),'spot':_round(row.get('spot')),
    'strike':_num(row.get('strike')),'years_to_expiry':_round(years,6) if years is not None else None,
    'intrinsic':_round(solved.get('intrinsic'),4),'time_value':_round(solved.get('time_value'),4),
    'last_trade_at':stamps.get(at),
    'staleness':('unknown' if age is None else ('stale' if age>IV.STALE_SECONDS else 'current')),
    'seconds_since_last_trade':None if age is None else int(age),
    'seconds_before_mark':None if before_mark is None else int(before_mark),
    'rate_sensitivity_pct_points':_round(sensitivity.get('iv_change_pct_points'),4) or None,
    'gap':False})
  return out,reasons

 def _atm_pair(self,underlying,expiry,spot):
  """The listed strike nearest `spot`, and its call and put. The ΔOI grid's own ATM rule, reused verbatim."""
  if spot is None:return None,None,None
  listed=[r for r in self._rows('select instrument_token,tradingsymbol,strike,instrument_type from contracts'
   " where underlying=? and expiry=? and instrument_type in ('CE','PE')",(underlying,expiry))
   if _num(r.get('strike')) is not None]
  if not listed:return None,None,None
  ladder=sorted({_num(r.get('strike')) for r in listed})
  atm=min(ladder,key=lambda s:(abs(s-spot),-s))
  by_key={(_num(r.get('strike')),r.get('instrument_type')):r for r in listed}
  return atm,by_key.get((atm,'CE')),by_key.get((atm,'PE'))

 def _iv_header(self,rate):
  """Everything that makes a computed number unmistakable, on every implied-volatility response."""
  return {'computed':True,'model':IV.MODEL,'method':IV.METHOD,'computed_text':IV.COMPUTED_TEXT,
   'risk_free_rate':rate,'risk_free_rate_source':dict(IV.RISK_FREE_RATE_SOURCE),
   'assumptions':dict(IV.ASSUMPTIONS),'day_count':IV.DAY_COUNT,'expiry_time_ist':IV.EXPIRY_TIME_IST,
   'iv_bracket':[IV.IV_MIN,IV.IV_MAX],'stale_seconds':IV.STALE_SECONDS,
   'reason_text':dict(IV.REASONS),'direction_words':dict(DIRECTION_WORDS['iv']),
   'exchange_reported':False}

 def iv_series(self,underlying,expiry='',strike=None,option_type='',rate=None):
  """Implied volatility through the session: an ATM reading for the underlying, and one per strike on request.

  The ATM reading is what a reader means by "the IV" day to day: the average of the at-the-money call's and
  the at-the-money put's solved volatility at the same 15-minute reading. The at-the-money strike is the
  listed strike nearest spot at the session's LAST reading and is then held fixed across the session, so the
  line follows two contracts rather than hopping between strikes as spot drifts - which would be a different
  number wearing the same name.

  Every response says `computed: true` and names the model, the rate and where the rate came from. A reading
  the maths cannot be trusted on is null WITH its reason. Nothing computed here is stored.
  """
  underlying=clean_symbol(underlying)
  used_rate=IV.RISK_FREE_RATE if rate is None else float(rate)
  header=self._iv_header(used_rate)
  blank={'atm':None,'strike':None,'spot':None,'expiries':[],'rejections':{},'total_readings':0,
   'readings_with_value':0,'direction':'no baseline','direction_detail':{},'direction_label':'No baseline',
   'atm_rule':IV_ATM_RULE,'readings_with_delta':0,'latest_delta_iv':None,'latest_delta_iv_pct':None,
   'previous_close_iv':None,'previous_close_iv_pct':None,'previous_close_basis':None,
   **self._delta_notes(IV_DELTA_UNIT,IV_DELTA_TEXT,None),**header}
  if not underlying:
   return self.envelope(points=[],context=self.context().as_dict(),**self._series_envelope('','',None,'none',blank))
  expiries=self._underlying_expiries(underlying)
  chosen=clean_expiry(expiry) or (expiries[0] if expiries else '')
  chain,session,source=self._chain_session(underlying,chosen) if chosen else ([],None,'store')
  grid=self._session_readings(session)
  spot=next((_num(r.get('spot')) for r in reversed(chain) if _num(r.get('spot')) is not None),None)
  atm_strike,call,put=self._atm_pair(underlying,chosen,spot) if chosen else (None,None,None)
  atm=self._iv_leg_pair(call,put,session,used_rate,grid) if (call or put) else None
  rejections=dict((atm or {}).get('rejections') or {})
  requested=None
  wanted=_num(strike)
  kind=str(option_type or '').strip().upper()
  if wanted is not None and kind in OPTION_TYPES and chosen:
   row=self._one('select instrument_token,tradingsymbol,strike,instrument_type from contracts'
    ' where underlying=? and expiry=? and instrument_type=? and strike=?',(underlying,chosen,kind,wanted))
   requested=self._iv_leg(row,session,used_rate,grid) if row else {
    'present':False,'strike':wanted,'option_type':kind,'tradingsymbol':None,'instrument_token':None,
    'points':[],'direction':'no baseline','direction_detail':{},'direction_label':'No baseline',
    'rejections':{},'readings_with_value':0,'readings_with_delta':0,
    'previous_close_iv':None,'previous_close_iv_pct':None,'previous_close_at':None,
    'previous_close_reason':SERIES_DELTA_REASONS['no_baseline'],'latest_delta_iv_pct':None,
    'delta_unit':IV_DELTA_UNIT,'delta_unit_text':IV_DELTA_TEXT,'delta_text':SERIES_DELTA_TEXT,
    'missing_text':f'{wanted:g} {kind} is not a listed strike in {underlying} {chosen}.'}
   for key,count in (requested.get('rejections') or {}).items():rejections[key]=rejections.get(key,0)+count
  points=(atm or {}).get('points') or []
  direction,detail=self.series_direction(points,'iv','iv')
  latest=next((p for p in reversed(points) if p.get('iv') is not None),None)
  iv_value,iv_at=self._latest_at(points,'iv')
  iv_pct_value,iv_pct_at=self._latest_at(points,'iv_pct')
  delta_iv_value,delta_iv_at=self._latest_at(points,'delta_iv')
  delta_pct_value,delta_pct_at=self._latest_at(points,'delta_iv_pct')
  return self.envelope(as_of=(points[-1]['at'] if points else None),source=source,missing=[],points=points,
   context=self.context(underlying,chosen,session,(points[-1]['at'] if points else ''),source).as_dict(),
   **self._series_envelope(underlying,chosen,session,source,{
    'atm':atm,'strike':requested,'spot':_round(spot),'atm_strike':atm_strike,'atm_rule':IV_ATM_RULE,
    'direction':direction,'direction_detail':detail,'direction_label':DIRECTION_LABELS.get(direction,direction),
    'latest_iv':iv_value,'latest_iv_at':iv_at,
    'latest_iv_pct':iv_pct_value,'latest_iv_pct_at':iv_pct_at,
    # in VOLATILITY POINTS, against the at-the-money reading of the previous session's close
    'latest_delta_iv':delta_iv_value,'latest_delta_iv_at':delta_iv_at,
    'latest_delta_iv_pct':delta_pct_value,'latest_delta_iv_pct_at':delta_pct_at,
    'previous_close_iv':(atm or {}).get('previous_close_iv'),
    'previous_close_iv_pct':(atm or {}).get('previous_close_iv_pct'),
    'previous_close_basis':(atm or {}).get('previous_close_basis'),
    'readings_with_delta':sum(1 for p in points if p.get('delta_iv_pct') is not None),
    **self._delta_notes(IV_DELTA_UNIT,IV_DELTA_TEXT,{'at':(atm or {}).get('previous_close_at'),
     'reason':(atm or {}).get('previous_close_reason'),'withheld':None,
     'present':(atm or {}).get('previous_close_iv') is not None}),
    'total_readings':len(points),
    'readings_with_value':sum(1 for p in points if p.get('iv') is not None),
    'expiries':expiries,'rejections':rejections,**header}))

 def _iv_previous_close(self,contract,session,rate):
  """This option's implied volatility at the LAST reading BEFORE `session` — the ΔOI baseline, solved.

  Implied volatility is not in the store, so the previous session's closing reading is re-solved here with
  the same model, the same rate and the same rejections the session's own readings get. A closing reading the
  maths refuses is NOT a baseline: the reason travels out and every Δ that would have rested on it is null.
  """
  blank={'iv':None,'iv_pct':None,'at':None,'reason':'no_baseline',
   'reason_text':SERIES_DELTA_REASONS['no_baseline']}
  symbol=(contract or {}).get('tradingsymbol') or ''
  if not contract or not session or not symbol:return blank
  if not self._has_scope() or 'metrics' not in self._tables():return blank
  row=self._one("select * from metrics where scope='contract' and metric_key=? and captured_at<?"
   ' order by captured_at desc limit 1',(symbol,f'{session} '))
  if row is None:return blank
  day=str(row.get('captured_at') or '')[:10]
  solved,_ignored=self._iv_points([row],_int(contract.get('instrument_token')),day,
   str(contract.get('instrument_type') or '').upper(),rate)
  point=solved[0] if solved else {}
  if point.get('iv') is None:
   reason=point.get('reason') or 'no_baseline'
   return {'iv':None,'iv_pct':None,'at':point.get('at') or row.get('captured_at'),'reason':reason,
    'reason_text':IV.REASONS.get(reason) or SERIES_DELTA_REASONS['no_baseline']}
  return {'iv':point.get('iv'),'iv_pct':point.get('iv_pct'),'at':point.get('at'),'reason':None,
   'reason_text':None}

 @staticmethod
 def _iv_deltas(points,baseline_iv,baseline_pct):
  """Write a Δ in VOLATILITY POINTS onto every reading of an implied-volatility series, in place.

  Never a percentage change of a percentage: 11.1% to 9.0% is 2.1 points down, not 19% down. A reading that
  could not be solved has no Δ, and a session with no solved previous close has no Δ anywhere.
  """
  for point in points or ():
   delta,why=Derivatives._delta_against(point.get('iv'),baseline_iv,6)
   delta_pct,why_pct=Derivatives._delta_against(point.get('iv_pct'),baseline_pct,4)
   point['delta_iv'],point['delta_iv_reason']=delta,why
   point['delta_iv_pct'],point['delta_iv_pct_reason']=delta_pct,why_pct
  return points

 def _iv_leg(self,contract,session,rate,grid=None):
  """One listed option's implied-volatility series, on the store's own reading grid."""
  token=_int(contract.get('instrument_token'))
  symbol=contract.get('tradingsymbol') or ''
  kind=str(contract.get('instrument_type') or '').upper()
  rows=self._contract_session_rows(symbol,session)
  points,rejections=self._iv_points(rows,token,session or '',kind,rate)
  if grid:
   have={p['at']:p for p in points}
   points=[have.get(at) or {'at':at,'iv':None,'iv_pct':None,'computed':False,'reason':'no_reading',
    'reason_text':IV.REASONS['no_reading'],'last_price':None,'spot':None,
    'strike':_num(contract.get('strike')),'years_to_expiry':None,'intrinsic':None,'time_value':None,
    'last_trade_at':None,'staleness':'unknown','seconds_since_last_trade':None,'seconds_before_mark':None,
    'rate_sensitivity_pct_points':None,'gap':True} for at in grid]
  direction,detail=self.series_direction(points,'iv','iv')
  # Δ against this leg's OWN previous close, in volatility points. A leg with no solved previous close gets
  # no Δ at all rather than borrowing the other leg's or the at-the-money reading's.
  prior=self._iv_previous_close(contract,session,rate)
  self._iv_deltas(points,prior['iv'],prior['iv_pct'])
  delta_value,delta_at=self._latest_at(points,'delta_iv_pct')
  return {'present':True,'strike':_num(contract.get('strike')),'option_type':kind,'tradingsymbol':symbol,
   'instrument_token':token,'points':points,'direction':direction,'direction_detail':detail,
   'direction_label':DIRECTION_LABELS.get(direction,direction),'rejections':rejections,
   'readings_with_value':sum(1 for p in points if p.get('iv') is not None),'missing_text':None,
   'previous_close_iv':prior['iv'],'previous_close_iv_pct':prior['iv_pct'],
   'previous_close_at':prior['at'],'previous_close_reason':prior['reason_text'],
   'latest_delta_iv_pct':delta_value,'latest_delta_iv_pct_at':delta_at,
   'readings_with_delta':sum(1 for p in points if p.get('delta_iv_pct') is not None),
   'delta_unit':IV_DELTA_UNIT,'delta_unit_text':IV_DELTA_TEXT,'delta_text':SERIES_DELTA_TEXT}

 def _iv_leg_pair(self,call,put,session,rate,grid=None):
  """The at-the-money reading: the call's and the put's solved volatility averaged at the same reading.

  When only one side solves at a reading, that side IS the reading and `basis` says so - an average of one
  number and a blank is not an average. When neither solves, the reading is null with both reasons beside it.
  """
  legs={}
  for contract,name in ((call,'ce'),(put,'pe')):
   legs[name]=self._iv_leg(contract,session,rate,grid) if contract is not None else None
  # The store's own reading grid when there is one, so a reading neither leg has a row for keeps its slot
  # rather than being closed up. Falling back to the readings the legs themselves hold.
  marks=list(grid or ()) or sorted({p['at'] for leg in legs.values() if leg for p in leg['points'] if p.get('at')})
  ce_by={p['at']:p for p in (legs['ce'] or {}).get('points',[])}
  pe_by={p['at']:p for p in (legs['pe'] or {}).get('points',[])}
  points,rejections={},{}
  out=[]
  for at in marks:
   ce,pe=ce_by.get(at),pe_by.get(at)
   values=[v for v in ((ce or {}).get('iv'),(pe or {}).get('iv')) if v is not None]
   basis='call and put' if len(values)==2 else ('call only' if (ce or {}).get('iv') is not None
    else ('put only' if (pe or {}).get('iv') is not None else 'neither'))
   reasons=[r for r in ((ce or {}).get('reason'),(pe or {}).get('reason')) if r]
   for r in reasons:rejections[r]=rejections.get(r,0)+1
   value=(sum(values)/len(values)) if values else None
   out.append({'at':at,'iv':_round(value,6),'iv_pct':_round(value*100.0,4) if value is not None else None,
    'computed':value is not None,'basis':basis,
    'ce_iv_pct':(ce or {}).get('iv_pct'),'pe_iv_pct':(pe or {}).get('iv_pct'),
    'ce_reason':(ce or {}).get('reason'),'pe_reason':(pe or {}).get('reason'),
    'reason':None if value is not None else (reasons[0] if reasons else 'missing_price'),
    'reason_text':None if value is not None else IV.REASONS.get(reasons[0] if reasons else 'missing_price'),
    # A reading only counts as a gap when NEITHER leg has a row for it. One leg that traded and one that did
    # not is a reading with data in it, not a hole.
    'spot':(ce or pe or {}).get('spot'),
    'gap':bool((ce is None or ce.get('gap')) and (pe is None or pe.get('gap')))})
  direction,detail=self.series_direction(out,'iv','iv')
  # The at-the-money BASELINE is built the same way the at-the-money reading is: whichever of the two legs
  # solved at the previous session's close, averaged. `previous_close_basis` says which, because an average
  # of one leg is not an average of two and the reader is told which one the Δ is measured from.
  ce_prior=(legs['ce'] or {}).get('previous_close_iv')
  pe_prior=(legs['pe'] or {}).get('previous_close_iv')
  prior_values=[v for v in (ce_prior,pe_prior) if v is not None]
  prior_basis='call and put' if len(prior_values)==2 else ('call only' if ce_prior is not None
   else ('put only' if pe_prior is not None else 'neither'))
  prior_iv=(sum(prior_values)/len(prior_values)) if prior_values else None
  prior_pct=_round(prior_iv*100.0,4) if prior_iv is not None else None
  prior_at=next((a for a in ((legs['ce'] or {}).get('previous_close_at'),
   (legs['pe'] or {}).get('previous_close_at')) if a),None)
  self._iv_deltas(out,_round(prior_iv,6) if prior_iv is not None else None,prior_pct)
  delta_value,delta_at=self._latest_at(out,'delta_iv_pct')
  return {'strike':_num((call or put or {}).get('strike')),
   'ce':legs['ce'],'pe':legs['pe'],'points':out,'direction':direction,'direction_detail':detail,
   'direction_label':DIRECTION_LABELS.get(direction,direction),'rejections':rejections,
   'basis_rule':IV_ATM_BASIS_RULE,
   'previous_close_iv':_round(prior_iv,6) if prior_iv is not None else None,
   'previous_close_iv_pct':prior_pct,'previous_close_at':prior_at,'previous_close_basis':prior_basis,
   'previous_close_reason':None if prior_iv is not None else SERIES_DELTA_REASONS['no_baseline'],
   'latest_delta_iv_pct':delta_value,'latest_delta_iv_pct_at':delta_at,
   'readings_with_delta':sum(1 for p in out if p.get('delta_iv_pct') is not None),
   'delta_unit':IV_DELTA_UNIT,'delta_unit_text':IV_DELTA_TEXT,'delta_text':SERIES_DELTA_TEXT,
   'readings_with_value':sum(1 for p in out if p.get('iv') is not None)}

 # --- 5. Futures build-up through the session -------------------------------------------------------------
 def futures_buildup(self,underlying):
  """§3.7 through the session: the front futures contract's OI against its own average, and its basis.

  `fut_oi_vs_avg` is open interest as a share of that contract's own 20-day average, and `basis` is the
  futures price minus spot at the SAME reading (with `basis_pct` as a share of spot). Both are read from the
  metrics worker; nothing here recomputes them. The directions say what each number did over the last hour.
  """
  underlying=clean_symbol(underlying)
  blank={'contract':None,'oi_direction':'no baseline','oi_direction_detail':{},
   'oi_direction_label':'No baseline','basis_direction':'no baseline','basis_direction_detail':{},
   'basis_direction_label':'No baseline','direction_words':{'oi':dict(DIRECTION_WORDS['oi']),
   'basis':dict(DIRECTION_WORDS['basis'])},'total_readings':0,'readings_with_value':0,
   'latest_oi_vs_avg':None,'latest_basis':None,'latest_basis_pct':None,'definition':FUTURES_DEFINITION,
   'buildup_labels':dict(BUILDUP_LABELS)}
  if not underlying:
   return self.envelope(points=[],context=self.context().as_dict(),**self._series_envelope('','',None,'none',blank))
  contract=self._front_future(underlying)
  if contract is None:
   return self.envelope(points=[],context=self.context(underlying).as_dict(),
    **self._series_envelope(underlying,'',None,'store',
    {**blank,'empty_note':f'{underlying} has no futures contract in the F&O store.'}))
  symbol=contract.get('tradingsymbol') or ''
  expiry=clean_expiry(contract.get('expiry'))
  rows=self._contract_session_rows(symbol)
  session=str(rows[-1].get('captured_at') or '')[:10] if rows else None
  grid=self._session_readings(session) or [r.get('captured_at') for r in rows]
  by_mark={str(r.get('captured_at')):r for r in rows}
  points=[]
  for at in grid:
   row=by_mark.get(at)
   if row is None:
    points.append({'at':at,'oi':None,'oi_avg':None,'oi_vs_avg':None,'basis':None,'basis_pct':None,
     'last_price':None,'spot':None,'buildup_15m':None,'buildup_day':None,'oi_change_pct_15m':None,
     'oi_change_pct_day':None,'oi_status':None,'basis_status':None,'gap':True})
    continue
   oi_status=str(row.get('fut_oi_vs_avg_status') or '').strip().lower() or None
   basis_status=str(row.get('basis_status') or '').strip().lower() or None
   points.append({'at':at,'oi':_int(row.get('oi')),'oi_avg':_round(row.get('fut_oi_avg'),2),
    'oi_vs_avg':_round(row.get('fut_oi_vs_avg'),6) if oi_status in (None,'ok') else None,
    'basis':_round(row.get('basis'),4) if basis_status in (None,'ok') else None,
    'basis_pct':_round(row.get('basis_pct'),4) if basis_status in (None,'ok') else None,
    'last_price':_round(row.get('last_price')),'spot':_round(row.get('spot')),
    'buildup_15m':row.get('buildup_15m') or None,'buildup_day':row.get('buildup_day') or None,
    'oi_change_pct_15m':_round(row.get('oi_change_pct_15m'),4),
    'oi_change_pct_day':_round(row.get('oi_change_pct_day'),4),
    'oi_status':oi_status,'basis_status':basis_status,'gap':False})
  oi_direction,oi_detail=self.series_direction(points,'oi_vs_avg','oi')
  basis_direction,basis_detail=self.series_direction(points,'basis','basis')
  latest=next((p for p in reversed(points) if not p['gap']),None) or {}
  share_value,share_at=self._latest_at(points,'oi_vs_avg')
  basis_value,basis_at=self._latest_at(points,'basis')
  basis_pct_value,basis_pct_at=self._latest_at(points,'basis_pct')
  fut_oi_value,fut_oi_at=self._latest_at(points,'oi')
  day_value,day_at=self._latest_at(points,'buildup_day')
  fifteen_value,fifteen_at=self._latest_at(points,'buildup_15m')
  change_value,change_at=self._latest_at(points,'oi_change_pct_day')
  return self.envelope(as_of=(points[-1]['at'] if points else None),source='metrics' if rows else 'store',
   missing=[],points=points,
   context=self.context(underlying,expiry,session,(points[-1]['at'] if points else ''),
    'metrics' if rows else 'store').as_dict(),
   **self._series_envelope(underlying,expiry,session,'metrics' if rows else 'store',{
    'contract':{'tradingsymbol':symbol,'instrument_token':_int(contract.get('instrument_token')),
     'expiry':expiry or None,'days_to_expiry':session_days_to_expiry(expiry,session) if expiry else None,
     'days_to_expiry_basis':DTE_CONVENTION},
    'oi_direction':oi_direction,'oi_direction_detail':oi_detail,
    'oi_direction_label':DIRECTION_LABELS.get(oi_direction,oi_direction),
    'basis_direction':basis_direction,'basis_direction_detail':basis_detail,
    'basis_direction_label':DIRECTION_LABELS.get(basis_direction,basis_direction),
    'direction_words':{'oi':dict(DIRECTION_WORDS['oi']),'basis':dict(DIRECTION_WORDS['basis'])},
    'latest_oi_vs_avg':share_value,'latest_oi_vs_avg_at':share_at,
    'latest_basis':basis_value,'latest_basis_at':basis_at,
    'latest_basis_pct':basis_pct_value,'latest_basis_pct_at':basis_pct_at,
    'latest_oi':fut_oi_value,'latest_oi_at':fut_oi_at,
    'latest_oi_change_pct_day':change_value,'latest_oi_change_pct_day_at':change_at,
    'latest_buildup_15m':fifteen_value,'latest_buildup_15m_at':fifteen_at,
    'latest_buildup_day':day_value,'latest_buildup_day_at':day_at,
    'total_readings':len(points),
    'readings_with_value':sum(1 for p in points if p.get('oi_vs_avg') is not None),
    'buildup_labels':dict(BUILDUP_LABELS),'definition':FUTURES_DEFINITION,'empty_note':None}))

 # --- 4. The screener -------------------------------------------------------------------------------------
 # The rule that shapes this whole section: a filter is APPLIED or it is REFUSED. It is never accepted,
 # displayed as active, and then quietly dropped. That exact bug - a sample-size filter shown as on while it
 # silently deleted every row - cost a day, so `applied` here is built from what the query actually did, and
 # `available` is built from the columns the store actually carries. A filter the store cannot honour raises
 # rather than returning an empty list that looks like an empty market.

 #: Every filter this screener understands: the query column it needs, how it compares, and its bounds.
 #: `column` None means the filter is evaluated on rows already read (moneyness, underlying kind), so it
 #: needs no column of its own - but the columns it DOES read are listed in `needs`.
 SCREENER_FILTERS={
  'min_premium_cr':{'column':'premium_cr','op':'>=','kind':'number','min':0.0,'max':100000.0,'unit':'₹ crore',
   'text':'Premium traded at or above this, in ₹ crore.'},
  'min_volume_ratio':{'column':'vol_tod_ratio','op':'>=','kind':'number','min':0.0,'max':1000.0,
   'needs':('vol_tod_sessions',),
   'text':'Today\'s volume by this time of day, against this contract\'s own median at the same time of day '
    f'over the last sessions. Only a contract with at least {MIN_BASELINE_SESSIONS} sessions behind it is '
    'eligible; the rest are "no baseline" and are never given a ratio.'},
  'min_volume_to_oi':{'column':'vol_oi_ratio','op':'>=','kind':'number','min':0.0,'max':1000.0,
   'text':'Day volume divided by the previous day\'s closing open interest, at or above this.'},
  'min_oi_change_15m_pct':{'column':'oi_change_pct_15m','op':'>=','kind':'number','min':-100.0,'max':10000.0,
   'unit':'%','text':'Open-interest change over the last 15 minutes, at or above this percentage.'},
  'max_oi_change_15m_pct':{'column':'oi_change_pct_15m','op':'<=','kind':'number','min':-100.0,'max':10000.0,
   'unit':'%','text':'Open-interest change over the last 15 minutes, at or below this percentage.'},
  'min_oi_change_day_pct':{'column':'oi_change_pct_day','op':'>=','kind':'number','min':-100.0,'max':10000.0,
   'unit':'%','text':'Open-interest change since the previous close, at or above this percentage.'},
  'max_oi_change_day_pct':{'column':'oi_change_pct_day','op':'<=','kind':'number','min':-100.0,'max':10000.0,
   'unit':'%','text':'Open-interest change since the previous close, at or below this percentage.'},
  'buildup':{'column':None,'op':'in','kind':'enum','values':BUILDUP_VALUES,'needs':('buildup_15m','buildup_day'),
   'text':'The build-up label, read over the window `buildup_window` names. The 15-minute and the day-on-day '
    'label are never mixed in one answer.'},
  'buildup_window':{'column':None,'op':'choice','kind':'enum','values':BUILDUP_WINDOWS,
   'text':'Which window `buildup` reads: the last 15 minutes, or since the previous close.'},
  'min_dte':{'column':'days_to_expiry','op':'>=','kind':'number','min':0,'max':400,'unit':'days',
   'text':'Days to expiry, at or above this.'},
  'max_dte':{'column':'days_to_expiry','op':'<=','kind':'number','min':0,'max':400,'unit':'days',
   'text':'Days to expiry, at or below this.'},
  'option_type':{'column':'instrument_type','op':'=','kind':'enum','values':OPTION_TYPES,
   'text':'Calls only, or puts only.'},
  'moneyness':{'column':None,'op':'in','kind':'enum','values':MONEYNESS_VALUES,'needs':('strike','spot'),
   'text':MONEYNESS_TEXT},
  'underlying_kind':{'column':None,'op':'=','kind':'enum','values':UNDERLYING_KINDS,'needs':('underlying',),
   'text':'An index underlying, or a single stock. Which names count as an index is listed in `available`.'},
  'underlying':{'column':'underlying','op':'=','kind':'symbol','text':'One underlying by name.'},
  'expiry':{'column':'expiry','op':'=','kind':'date','text':'One expiry, as YYYY-MM-DD.'},
  'at':{'column':None,'op':'choice','kind':'reading',
   'text':'One 15-minute reading the store holds. Omitted, the screener opens on the newest one that has '
    'contracts over the liquidity floors.'},
  'group':{'column':None,'op':'choice','kind':'enum','values':SCREENER_VIEWS,
   'text':'One row per UNDERLYING (the default), or one row per CONTRACT. The per-underlying rows are sums '
    'and counts over the contracts that name has at this reading; a per-contract signal that cannot be '
    'summed honestly is served as a maximum or a count, never as an average.'},
  'limit':{'column':None,'op':'cut','kind':'number','min':1,'max':SCREENER_LIMIT_MAX,
   'text':'How many rows to return. The count scanned and the count kept are always reported beside them.'},
 }

 def screener_available(self):
  """What this store can actually be filtered on, right now. Built from its columns, never from a wish list."""
  names=self._column_names('metrics')
  out=[]
  for key,rule in self.SCREENER_FILTERS.items():
   needed=[c for c in ((rule.get('column'),)+tuple(rule.get('needs') or ())) if c]
   ready=all(c in names for c in needed) if needed else True
   row={'key':key,'kind':rule['kind'],'op':rule['op'],'ready':ready,'text':rule['text'],
    'requires_columns':needed,'missing_columns':[c for c in needed if c not in names]}
   for extra in ('min','max','unit','values'):
    if extra in rule:row[extra]=list(rule[extra]) if extra=='values' else rule[extra]
   out.append(row)
  return out

 @staticmethod
 def _moneyness(strike,spot,option_type):
  """itm / atm / otm for one row, or None when the store did not carry both numbers."""
  strike,spot=_num(strike),_num(spot)
  # A FUTURE HAS NO MONEYNESS. It has no strike to be in or out of the money against, and its stored strike is
  # 0 - which, left alone, is "far below spot" and would come back as a real-looking bucket.
  if str(option_type or '').upper() not in OPTION_TYPES:return None
  if strike is None or spot is None or spot<=0:return None
  if abs(strike-spot)<=MONEYNESS_BAND*spot:return 'atm'
  above=strike>spot
  if str(option_type or '').upper()=='CE':return 'otm' if above else 'itm'
  return 'itm' if above else 'otm'

 def screener_readings(self,limit=40):
  """The newest readings the store holds, and how many underlyings each covered, newest first.

  The coverage count is here because the capture worker does not always reach every underlying at every
  reading - the real store has readings that covered 216 and readings that covered one - and a screener that
  silently read a one-name reading would look like a market with one name in it.

  It comes from `underlying_snapshots`, which holds one row per underlying per reading and is keyed on the
  reading, so this is a covering-index scan of ~2,000 small rows. The obvious version,
  `count(distinct underlying) ... from metrics group by captured_at`, has no usable index for it: measured at
  612 ms over 265,552 rows with two temporary B-trees, and growing every session. The per-reading ROW count
  is not taken here at all - only the one reading actually served gets counted, by `_reading_rows`.
  """
  tables=self._tables()
  if 'underlying_snapshots' not in tables:
   if 'metrics' not in tables:return []
   rows=self._rows('select distinct captured_at from metrics order by captured_at desc limit ?',
    (max(1,int(limit)),))
   return [{'at':r.get('captured_at'),'rows':None,'underlyings':None} for r in rows if r.get('captured_at')]
  rows=self._rows('select captured_at,count(*) as underlyings from underlying_snapshots'
   ' group by captured_at order by captured_at desc limit ?',(max(1,int(limit)),))
  return [{'at':r.get('captured_at'),'rows':None,'underlyings':_int(r.get('underlyings'))}
   for r in rows if r.get('captured_at')]

 def _reading_rows(self,at):
  """How many contract rows the ONE reading being served holds. A covering-index seek, no row visits."""
  if 'metrics' not in self._tables() or not at:return None
  row=self._one('select count(*) as n from metrics where captured_at=?',(at,))
  return _int((row or {}).get('n'))

 def _floor_clauses(self,names,applied,min_premium_cr=None):
  """The §3 floors that are in force at a reading, as SQL. ONE builder, so the screener's own query, its
  existence test and its floors-only count can never drift into asking three different questions.

  A floor is written only when the store carries its column AND that reading measured it. Nothing is
  substituted for a floor that was not measured: the row is kept and the card says the floor was not applied.
  """
  keys=FLOOR_ORDER if applied is None else set(applied)
  clauses,params=[],[]
  if 'premium_cr' in keys and 'premium_cr' in names:
   # a caller may RAISE the premium floor, never lower it; the §3 constant is the floor of the floor
   clauses.append('premium_cr>=?');params.append(max(FLOOR_PREMIUM_CR,_num(min_premium_cr) or 0.0))
  if 'last_price' in keys and 'last_price' in names:
   clauses.append('last_price>=?');params.append(FLOOR_LAST_PRICE)
  # `oi_lots` is not a column: the floor is open interest against the contract's own lot size, exactly as
  # `_passes` works it out row by row. A row missing either number does not pass - absence is not evidence.
  if 'oi' in keys and 'oi' in names and 'lot_size' in names:
   clauses.append('oi is not null and lot_size is not null and lot_size>0 and oi>=lot_size*?')
   params.append(FLOOR_OI_LOTS)
  return clauses,params

 def _clears_floors(self,at,names=None):
  """Does ONE reading hold a single contract over the floors IT can be measured against? One index seek.

  The same floors the screener itself applies at that reading, in the same order, over the same columns,
  through the same builder. A floor whose number was never captured at this reading is not asked here either
  - otherwise this walk would rule out a reading the screener can serve perfectly well, which is exactly
  what kept the whole tab four hours behind the close of 18 Sep 2026.

  The store also carries an ESTIMATED traded price, and it is not read here on purpose: the floors decide
  what the reader is shown, and a number that decides visibility has to be one the exchange reported rather
  than one a model produced.
  """
  if 'metrics' not in self._tables() or not at:return False
  if names is None:names=self._column_names('metrics')
  force=self._floors_in_force(at,names)
  clauses,params=[],[]
  if self._has_scope():clauses.append("scope='contract'")
  clauses.append('captured_at=?');params.append(at)
  floors,floor_params=self._floor_clauses(names,force['applied'])
  clauses+=floors;params+=floor_params
  if 'instrument_type' in names:clauses.append("instrument_type in ('CE','PE')")
  return bool(self._rows('select 1 from metrics where '+' and '.join(clauses)+' limit 1',tuple(params)))

 def _cleared_count(self,at,names=None):
  """How many contracts at ONE reading clear the §3 floors, with NO reader filter in the way.

  The screener's own row count is taken after the reader's filters, so on its own it cannot tell
  "the filters in force excluded everything" from "nothing cleared the floors" - two facts a trader
  reads completely differently. This is the floors-only count that separates them: same three floors,
  same columns, same order as `_clears_floors`, counted instead of existence-tested.
  """
  if 'metrics' not in self._tables() or not at:return 0
  if names is None:names=self._column_names('metrics')
  force=self._floors_in_force(at,names)
  clauses,params=[],[]
  if self._has_scope():clauses.append("scope='contract'")
  clauses.append('captured_at=?');params.append(at)
  floors,floor_params=self._floor_clauses(names,force['applied'])
  clauses+=floors;params+=floor_params
  if 'instrument_type' in names:clauses.append("instrument_type in ('CE','PE','FUT')")
  row=self._one('select count(*) as n from metrics where '+' and '.join(clauses),tuple(params))
  return _int((row or {}).get('n')) or 0

 def _usable_reading(self,readings):
  """The newest reading a reader can actually do anything with, and how many newer ones hold nothing.

  The newest reading the store holds is not always one the tab can show. A session with no contract rows at
  all, or one whose every measurable floor rejects everything, hands the reader an empty screener, no row to
  click, and therefore no symbol for any block on the tab.

  A rebuilt reading is NO LONGER one of those. It carries no traded-price average, so the premium floor is
  not applied there (`_floors_in_force`), and its rows are measured against the two floors that were captured
  - which is why the 15:45 reading of 18 Sep 2026 is now usable, and why the tab no longer falls back four
  hours to 11:30 while ten thousand contracts with real prices sit unread.

  It is resolved here, once, next to the coverage the screener already computes, rather than by a browser that
  would have to fetch readings and throw them away. Nothing is hidden by it: every reading the store holds is
  still in `readings` and still one click away, and the response says which reading this is and that it is not
  the newest.

  Returns (at, skipped) - the reading to serve, and how many newer readings were walked past to reach it.
  When no reading clears the floors at all, the newest is served and `skipped` is 0: an empty screener under
  the newest reading is the honest answer when every reading is empty.
  """
  if not readings:return '',0
  names=self._column_names('metrics')
  for index,row in enumerate(readings):
   at=row.get('at')
   if at and self._clears_floors(at,names):return at,index
  return readings[0].get('at') or '',0

 def _reading_note(self,at,newest,asked,skipped):
  """The FACTS about which reading is on screen. The sentence itself is built in the browser.

  Not a formatted sentence, on purpose: the tab already owns one date formatter and one set of checks over it
  (logic.asOfText), and a second one here would be a second place for "18 Sep 2026 · 11:30" to drift. What
  the server owns is what it knows - which reading it served, which is the newest, whether the reader picked
  it, and how many newer readings hold nothing over the floors. `reading_rule` is a definition rather than a
  date, so it is served like every other definition on this tab.
  """
  return {'newest_at':newest or None,'reading_at':at or None,
   'reading_is_newest':bool(not at or not newest or at==newest),
   'reading_chosen':bool(asked),'reading_skipped':int(skipped or 0),
   'reading_rule':READING_RULE_TEXT}

 @staticmethod
 def _ranking(view,applied=None):
  """The order this view is ACTUALLY served in, with every key and its direction.

  It is served with the rows because the page used to print one sort while the list obeyed another: the block
  below the screener took its default instrument from row one of the unusual-ranked list and called it
  "Busiest by premium", which the ordering never guaranteed. A page that prints the server's own answer to
  "what is this sorted by" cannot drift from it again.

  WHEN PREMIUM WAS NOT CAPTURED it cannot be a sort key either. Ordering rows by a column that is null on
  every one of them is insertion order wearing a label, and this response would then be printing a sort the
  list does not obey - the very drift the block above exists to prevent. So the key is swapped for volume,
  which IS captured at a rebuilt reading, and the swap is stated rather than assumed.
  """
  degraded=applied is not None and 'premium_cr' not in set(applied)
  fallback={'field':FALLBACK_RANK_FIELD,'direction':'desc','text':FALLBACK_RANK_TEXT}
  if view=='underlying':
   keys=[dict(k) for k in SCREENER_RANK_KEYS]
   if degraded:keys=[k for k in keys if k['field']!='premium_cr']
   if degraded:keys.insert(-1,dict(fallback))
   return {'view':view,'label':SCREENER_RANK_LABEL,
    'text':(SCREENER_RANK_TEXT if not degraded else
     'One row per instrument, ordered by: most distinct condition types, then most contracts flagged, then '
     'largest volume traded, then instrument name A to Z. '+FALLBACK_RANK_TEXT),
    'keys':keys,'degraded':degraded,'ranked_by':('premium_cr' if not degraded else FALLBACK_RANK_FIELD)}
  return {'view':view,'label':(CONTRACT_RANK_LABEL if not degraded else FALLBACK_RANK_LABEL),
   'text':(CONTRACT_RANK_TEXT if not degraded else 'One row per contract, largest volume traded first. '+FALLBACK_RANK_TEXT),
   'keys':([dict(k) for k in CONTRACT_RANK_KEYS] if not degraded else [dict(fallback)]),
   'degraded':degraded,'ranked_by':('premium_cr' if not degraded else FALLBACK_RANK_FIELD)}

 def _screener_groups(self,rows):
  """One row per UNDERLYING, out of the contract rows that cleared the floors and the filters.

  WHY THIS IS THE DEFAULT VIEW. The screener lists contracts, sorted by premium traded. At the 11:30 reading
  of 18 Sep 2026, 214 underlyings cleared the floors - and NIFTY alone had 106 of the 491 contracts. A list
  of 100 contracts was therefore 100 rows of NIFTY, and the reader's conclusion was the obvious one: no stock
  is active. It is what the owner meant by "why other stocks are not populating". Scanning the market for
  which NAMES are busy is what a screener is for; the contract list is the drill-down.

  WHAT MAY BE AGGREGATED, AND WHAT MAY NOT. Every figure here is either a SUM of things that add up, or a
  COUNT of contracts. Nothing else is honest:

    * premium traded, volume, open interest and both open-interest changes ADD - they are rupees and
      contracts, and a sum of them is the same kind of number as its parts;
    * calls, puts and the contract count are COUNTS;
    * spot is not an aggregate at all: every contract of one underlying at one reading carries the same one,
      so it is passed through rather than combined.

  And what is REFUSED, because a mean of these is not the thing it looks like:

    * volume against its own median (§3.2) is a ratio per contract, and a ratio of ratios is not a ratio.
      The LARGEST one is served instead, with the contract it belongs to named, plus how many of the
      underlying's contracts had enough baseline to carry one at all. A maximum is a real reading of a real
      contract; an average would be a number nothing reported.
    * volume to open interest (§3.3), for the same reason. What is served is a COUNT - how many contracts
      traded more today than was standing at yesterday's close, which is the tab's own existing threshold
      (`volumeToOiHot`), not a new one invented here.
    * the build-up label (§3.1) is a statement about ONE contract. There is no such thing as an underlying's
      build-up, so none is served as a label; the four counts are, and the labels themselves stay in the
      contract view where they belong.
    * moneyness, strike and last price are not aggregated at all: a set of contracts has no strike.

  A word on FUTURES, which are in this list because the reader's question is about a NAME and not about its
  option chain. They carry a premium, an open interest, a build-up label and a spot, so they are in every sum
  and every count that uses those. They carry NEITHER §3.2 nor §3.3 - the store computes no volume-against-
  median and no volume-to-open-interest for a futures contract, 0 of 411 at the 11:30 reading of 18 Sep 2026 -
  so both of those arrive with the count of contracts that could carry them, and a name whose whole book is
  futures gets nothing rather than a zero.
  """
  groups={}
  for row in rows:
   name=row.get('underlying') or ''
   group=groups.get(name)
   if group is None:
    group=groups[name]={'underlying':name,'underlying_kind':row.get('underlying_kind'),
     # `has_premium` is why `premium_cr` is not simply a running total. At a reading that captured no traded
     # average price EVERY contract's premium is absent, and a sum of absences is not ₹0 cr - it is no number
     # at all. Printing 0.0 there would be this method inventing the one figure the whole card is about.
     'contracts':0,'options':0,'futures':0,'premium_cr':0.0,'has_premium':False,
     'calls':0,'puts':0,'volume':0,'oi':0,
     'oi_change_day':0,'has_oi_change_day':False,'oi_change_15m':0,'has_oi_change_15m':False,
     'spot':None,'spot_disagrees':False,'expiries':set(),'days_to_expiry':None,
     'volume_ratio_max':None,'volume_ratio_max_symbol':None,'volume_baseline_contracts':0,
     # How many of this name's contracts could carry each ratio at all. THE DENOMINATOR IS THE POINT: the
     # store computes neither §3.2 nor §3.3 for a futures contract, so a futures-only name has nothing to
     # count - and "0 over 1" would read as "nothing unusual" when the truth is "not measured for futures".
     'volume_to_oi_contracts':0,'volume_to_oi_over_1':0,'buildup_counts':{},'top':None,'top_by_volume':None,
     # WHAT IS UNUSUAL IN THIS NAME, AND IT IS THREE DIFFERENT NUMBERS.
     #
     #   `unusual`              how many CONTRACTS the store flagged;
     #   `unusual_rule_count`   how many DISTINCT RULES they tripped between them — two exist, so this is 0, 1
     #                          or 2, and it can never be three;
     #   `unusual_observations` how many TIMES a rule fired across those contracts.
     #
     # These used to be one number. The old code tallied the store's complete reason SENTENCE, which differs at
     # every multiple, so NIFTY's 80 flagged contracts produced 124 "conditions" — and the tab clamped that to
     # three and called it "3 conditions". Nothing here counts a sentence.
     'unusual':0,'unusual_rules':{},'unusual_observations':0,'unusual_contracts':[]}
   group['contracts']+=1
   # The calls/puts split describes the OPTIONS half and nothing else. A future is not a call, not a put, and
   # not a side of anything - a name with futures and no listed options must never read as "0C / 0P", which
   # says its options were quiet when it has none at all.
   kind=row.get('instrument_type')
   if kind=='CE':group['calls']+=1;group['options']+=1
   elif kind=='PE':group['puts']+=1;group['options']+=1
   else:group['futures']+=1
   # --- the sums: rupees and contracts, which add ---
   if row.get('premium_cr') is not None:
    group['premium_cr']+=row['premium_cr'];group['has_premium']=True
   group['volume']+=row.get('volume') or 0
   group['oi']+=row.get('oi') or 0
   if row.get('oi_change_day') is not None:
    group['oi_change_day']+=row['oi_change_day'];group['has_oi_change_day']=True
   if row.get('oi_change_15m') is not None:
    group['oi_change_15m']+=row['oi_change_15m'];group['has_oi_change_15m']=True
   # --- passed through, not combined: one underlying at one reading has one spot ---
   # ...but only when its rows AGREE on it. Picking the first of two different numbers and printing it as
   # the underlying's spot is exactly the kind of quiet invention the rest of this method refuses.
   if row.get('spot') is not None:
    if group['spot'] is None and not group['spot_disagrees']:group['spot']=row['spot']
    elif group['spot'] is not None and row['spot']!=group['spot']:
     group['spot']=None;group['spot_disagrees']=True
   if row.get('expiry'):group['expiries'].add(row['expiry'])
   dte=row.get('days_to_expiry')
   if dte is not None and (group['days_to_expiry'] is None or dte<group['days_to_expiry']):
    group['days_to_expiry']=dte
   # --- the ratios: a maximum and a count, never a mean ---
   ratio=row.get('volume_ratio')
   if ratio is not None:
    group['volume_baseline_contracts']+=1
    if group['volume_ratio_max'] is None or ratio>group['volume_ratio_max']:
     group['volume_ratio_max']=ratio
     group['volume_ratio_max_symbol']=row.get('tradingsymbol') or ''
   if row.get('volume_to_oi') is not None:
    group['volume_to_oi_contracts']+=1
    if row['volume_to_oi']>1:group['volume_to_oi_over_1']+=1
   label=row.get('buildup_day')
   if label:group['buildup_counts'][label]=group['buildup_counts'].get(label,0)+1
   # --- the store's own §3 flag, counted; and the RULES its reasons name, tallied ---
   if row.get('unusual'):
    group['unusual']+=1
    triggers=row.get('unusual_triggers') or []
    evidence=[]
    for trigger in triggers:
     rule_id=trigger.get('rule_id') or RULE_UNCLASSIFIED
     tally=group['unusual_rules'].get(rule_id)
     if tally is None:
      # THE WORDS LIVE IN THE REGISTRY, ONCE. A tally carries the rule's ID and its NUMBERS; its name, its
      # comparator, its threshold and its prose are served once per response under `unusual_rules`, not
      # copied onto all 214 instrument rows. The same registry is mirrored in logic.ts, and
      # check-derivative.cjs fails if any of the three copies drift.
      tally=group['unusual_rules'][rule_id]={'rule_id':rule_id,
       'rule_version':trigger.get('rule_version') or UNUSUAL_RULES_VERSION,
       # a COUNT of contracts and a COUNT of firings: the same contract can trip a rule once, and two
       # contracts tripping the same rule is one rule and two contracts
       'contracts':0,'observations':0,
       # the LARGEST reading of this rule in this name, with the contract it belongs to named and the
       # baseline it was measured against beside it — never an average of multiples
       'value_max':None,'value_max_symbol':None,'baseline_at_max':None,'sample_count_at_max':None}
     tally['observations']+=1
     group['unusual_observations']+=1
     value=trigger.get('value')
     if value is not None and (tally['value_max'] is None or value>tally['value_max']):
      tally['value_max']=value
      tally['value_max_symbol']=row.get('tradingsymbol') or ''
      tally['baseline_at_max']=trigger.get('baseline')
      tally['sample_count_at_max']=trigger.get('sample_count')
     evidence.append(trigger)
    # ONE ENTRY PER FLAGGED CONTRACT, so the same contract is counted once per rule and once in total
    for rule_id in {t.get('rule_id') or RULE_UNCLASSIFIED for t in triggers}:
     group['unusual_rules'][rule_id]['contracts']+=1
    # THE EVIDENCE ITSELF. 396 contracts carried 688 triggers across the whole market at the 11:30 reading of
    # 18 Sep 2026 — small enough to travel with the rows, so the drawer that lists them needs no second
    # request and cannot show a different reading from the one on screen.
    group['unusual_contracts'].append({'tradingsymbol':row.get('tradingsymbol') or '',
     'instrument_type':row.get('instrument_type') or '','strike':row.get('strike'),
     'expiry':row.get('expiry') or '','premium_cr':row.get('premium_cr'),'triggers':evidence})
   # THE BUSIEST CONTRACT OF THIS NAME, by the same measure the list is sorted on - and at a reading with no
   # premium that measure is volume, not a comparison of Nones that would hand back whichever row arrived
   # first. Both candidates are carried and the one that fits this reading is chosen below.
   candidate={'tradingsymbol':row.get('tradingsymbol') or '','strike':row.get('strike'),
    'instrument_type':row.get('instrument_type') or '','premium_cr':row.get('premium_cr'),
    'volume':row.get('volume'),
    'instrument_token':row.get('instrument_token'),'expiry':row.get('expiry') or '',
    'days_to_expiry':row.get('days_to_expiry')}
   top=group['top']
   if top is None or (row.get('premium_cr') or 0.0)>(top.get('premium_cr') or 0.0):group['top']=candidate
   busiest=group['top_by_volume']
   if busiest is None or (row.get('volume') or 0)>(busiest.get('volume') or 0):
    group['top_by_volume']=candidate
  out=[]
  for group in groups.values():
   # A SUM OF NOTHING IS NOT NOUGHT. No contract of this name carried a premium at this reading, so the name
   # has no premium traded - a dash, which the tab already renders, and never "₹0.0 cr traded".
   group['premium_cr']=_round(group['premium_cr'],2) if group['has_premium'] else None
   if not group['has_premium']:group['top']=group['top_by_volume']
   group.pop('has_premium');group.pop('top_by_volume')
   group['expiries']=sorted(group['expiries'])
   group['oi_change_day']=group['oi_change_day'] if group['has_oi_change_day'] else None
   group['oi_change_15m']=group['oi_change_15m'] if group['has_oi_change_15m'] else None
   group.pop('has_oi_change_day');group.pop('has_oi_change_15m')
   # kept, and named, so a dash where a spot should be is explained rather than just blank
   group['spot_disagrees']=bool(group['spot_disagrees'])
   # Nothing carried the ratio, so there is no count - not a count of zero.
   if not group['volume_to_oi_contracts']:group['volume_to_oi_over_1']=None
   # THE RULES, IN THE REGISTRY'S OWN ORDER, and the three counts kept apart. `unusual_rule_count` is how many
   # DISTINCT rules were tripped — the registry holds two, so it is 0, 1 or 2 and can never be three.
   order={rule['rule_id']:i for i,rule in enumerate(UNUSUAL_RULES)}
   group['unusual_rules']=sorted(group['unusual_rules'].values(),
    key=lambda r:(order.get(r['rule_id'],len(order)),r['rule_id']))
   group['unusual_rule_count']=len(group['unusual_rules'])
   # the evidence in a stable order: the contract with the largest reading of any rule first, then by name,
   # so the drawer lists the same contracts in the same order on every request
   group['unusual_contracts'].sort(key=lambda c:(
    -max([t['value'] for t in c['triggers'] if t.get('value') is not None],default=float('-inf')),
    c['tradingsymbol']))
   out.append(group)
  # ==================================================================================================================
  # THE DEFAULT ORDER, WRITTEN DOWN AND SERVED WITH THE ROWS.
  #
  # Sorted by premium alone this list is a directory - the same large names on top every session, with nothing
  # telling the reader where to look. The keys are SCREENER_RANK_KEYS and they are served in the response, so the
  # page prints the sort that is actually in force instead of a sort somebody once assumed.
  #
  # The first key is how many DISTINCT RULES the name's contracts tripped. It used to be how many distinct reason
  # SENTENCES they wrote, which is close to meaningless: the same rule at two multiples wrote two sentences, so
  # the list was ordered by how many different NUMBERS appeared under a name.
  #
  # The last key is the NAME, so two instruments level on all three counts come out in the same order on every
  # request and on every machine. Without it the order of a tie was whatever order the rows happened to arrive in.
  # ==================================================================================================================
  # The third key is the largest number this READING can support: premium traded where it was captured, and
  # volume where it was not. Sorting on a premium that is None for every name is not a sort at all - it is the
  # order the rows arrived in, printed under a caption that claims otherwise. `_ranking` says which it was.
  out.sort(key=lambda g:(-(g['unusual_rule_count'] or 0),-(g['unusual'] or 0),
   -(g['premium_cr'] if g['premium_cr'] is not None else (g['volume'] or 0)),
   g['underlying'] or ''))
  return out

 def screener(self,filters=None,limit=None):
  """§3.2-§3.4 as a real screen: the §3 floors always, plus whatever else the caller asked for.

  Raises ValueError for a filter this store cannot honour, so a refused filter is a refusal the caller sees
  rather than a silent deletion of every row. `applied` lists exactly what the query did, including the
  floors. What could have been asked for is `available_filters`, and BOTH lists are also nested under
  `filters` as `filters.applied` / `filters.available`.

  Why not a bare `available`: the standard derivative envelope already uses that key for the boolean "is the
  F&O store readable at all", which every card on the tab reads. Serving the filter list under the same name
  would have made `available` a list on one route and a boolean on the other eight, so the two are kept
  apart and the contract's own word survives as `filters.available`. Display only.
  """
  asked=dict(filters or {})
  names=self._column_names('metrics')
  available=self.screener_available()
  ready={row['key']:row for row in available}
  for key in asked:
   if key not in self.SCREENER_FILTERS:raise ValueError(f'{key} is not a filter this screener offers.')
   if not ready[key]['ready']:
    raise ValueError(f'{key} cannot be applied: this store does not carry '
     f'{", ".join(ready[key]["missing_columns"])}.')
  applied=[]
  clauses,params=[],[]
  if self._has_scope():clauses.append("scope='contract'")
  # The reading. Omitted, the newest one; named, it must be one the store actually holds - a reading that is
  # not there would otherwise answer with an empty list that reads exactly like an empty market.
  readings=self.screener_readings()
  newest=readings[0]['at'] if readings else (self._max('metrics') or '')
  at=str(asked.get('at') or '').strip()
  asked_for_reading=bool(at)
  skipped=0
  if at:
   if at not in {r['at'] for r in readings}:raise ValueError(f'{at} is not a 15-min reading this store holds.')
  else:
   # NOT simply the newest. The newest reading of a rebuilt session carries no premium at all, so nothing in
   # it can clear the §3 floors and the screener lands empty - no row to click, and therefore no symbol for
   # any block on the tab. `_usable_reading` walks back to the newest one that HAS rows over the floors, and
   # `reading_text` below says so in plain words.
   at,skipped=self._usable_reading(readings)
   # A store with no `underlying_snapshots` has no reading list to walk, so the newest metric row is still
   # the fallback it always was - the usable-reading rule narrows the choice, it never removes one.
   if not at:at=newest
  coverage=next((dict(r) for r in readings if r['at']==at),None)
  reading=self._reading_note(at,newest,asked_for_reading,skipped)
  if not at:
   capture=self.capture_health('',readings=readings)
   return self.envelope(rows=[],applied=[],available_filters=available,filters={'applied':[],'available':available},
    readings=readings,total=0,scanned=0,capture=capture,
    context=self.context(source='metrics' if self._has_scope() else 'store').as_dict(),
    floors=dict(FLOORS),floors_text=FLOORS_TEXT,floors_applied=list(FLOOR_ORDER),floors_unmeasured=[],
    floors_absent=[],floors_degraded=False,floors_unmeasured_text=[],floors_labels=dict(FLOOR_PHRASES),
    moneyness_text=MONEYNESS_TEXT,
    index_underlyings=list(INDEX_KINDS),coverage=None,limit=0,buildup_window=BUILDUP_WINDOWS[0],
    view=str(asked.get('group') or SCREENER_VIEW_DEFAULT),views=list(SCREENER_VIEWS),
    ranking=self._ranking(str(asked.get('group') or SCREENER_VIEW_DEFAULT)),
    unusual_rules=[dict(r) for r in UNUSUAL_RULES],unusual_rules_version=UNUSUAL_RULES_VERSION,
    groups_total=0,contracts_total=0,empty_note=capture['state_text'],**reading)
  clauses.append('captured_at=?');params.append(at)
  applied.append({'key':'at','value':at,'always':True,'text':f'The 15-minute reading of {at}.'})
  # ========================================================================================================
  # §3's FLOORS, DEGRADED RATHER THAN HIDDEN.
  #
  # Every floor whose number this reading captured is applied exactly as before. A floor whose number was
  # never captured is NOT applied - and is named, in `floors_unmeasured` and in `floors_text`, so the reader
  # knows the list in front of them was gated on two floors and not three. The rows are kept: a contract with
  # no premium did not fail a premium floor, it was never measured against one, and deleting it from the
  # screen turns a gap in our capture into a statement about the market.
  # ========================================================================================================
  force=self._floors_in_force(at,names)
  in_force=set(force['applied'])
  floor=max(FLOOR_PREMIUM_CR,_num(asked.get('min_premium_cr')) or 0.0)
  if 'min_premium_cr' in asked and 'premium_cr' not in in_force:
   # A filter the caller ASKED for and this reading cannot honour is a refusal the caller sees, exactly as
   # every other unhonourable filter is. Dropping it silently would serve a wider list than was requested.
   raise ValueError('min_premium_cr cannot be applied at the 15-min reading of '+at+
    ': no traded average price was captured there, so no contract in it has a premium traded.')
  floor_clauses,floor_params=self._floor_clauses(names,force['applied'],min_premium_cr=floor)
  clauses+=floor_clauses;params+=floor_params
  for key,value,text in (
   ('min_premium_cr',floor,f'Premium traded at or above ₹{floor:g} cr (the §3 floor is ₹{FLOOR_PREMIUM_CR:g} cr).'),
   ('min_oi_lots',FLOOR_OI_LOTS,f'Open interest at or above {FLOOR_OI_LOTS} lot (§3 floor).'),
   ('min_last_price',FLOOR_LAST_PRICE,f'Last price at or above ₹{FLOOR_LAST_PRICE:g} (§3 floor).')):
   name={'min_premium_cr':'premium_cr','min_oi_lots':'oi','min_last_price':'last_price'}[key]
   if name in in_force:
    applied.append({'key':key,'value':value,'always':True,'text':text})
   else:
    # RECORDED AS NOT APPLIED, never omitted. `applied` is the page's own answer to "what did this query do",
    # and a floor that silently vanished from it would be a relaxation the reader could not see.
    applied.append({'key':key,'value':None,'always':True,'applied':False,
     'text':FLOOR_UNMEASURED_TEXT.get(name,f'The {name} floor could not be applied at this 15-min reading.')})
  if 'min_volume_ratio' in asked and 'vol_tod_sessions' in names:
   clauses.append('vol_tod_sessions>=?');params.append(MIN_BASELINE_SESSIONS)
   applied.append({'key':'min_volume_baseline_sessions','value':MIN_BASELINE_SESSIONS,'always':True,
    'text':f'Only contracts with at least {MIN_BASELINE_SESSIONS} sessions of baseline behind the ratio '
     '(§3.2); the rest are "no baseline" and are never given one.'})
  for key,value in asked.items():
   if key in ('at','limit','buildup_window','min_premium_cr','group'):continue
   rule=self.SCREENER_FILTERS[key]
   column=rule.get('column')
   if column is None:continue  # applied on the rows below, and recorded there
   if rule['kind']=='number':
    number=_num(value)
    if number is None or not rule['min']<=number<=rule['max']:
     raise ValueError(f'{key} must be a number between {rule["min"]} and {rule["max"]}.')
    clauses.append(f'"{column}"{rule["op"]}?');params.append(number)
    applied.append({'key':key,'value':number,'always':False,'text':rule['text']})
   elif rule['kind']=='enum':
    text=str(value).strip().upper() if key=='option_type' else str(value).strip()
    if text not in rule['values']:
     raise ValueError(f'{key} must be one of {", ".join(rule["values"])}.')
    clauses.append(f'"{column}"=?');params.append(text)
    applied.append({'key':key,'value':text,'always':False,'text':rule['text']})
   elif rule['kind']=='symbol':
    symbol=clean_symbol(value)
    if not symbol:raise ValueError(f'{key} must be a traded symbol.')
    clauses.append(f'"{column}"=?');params.append(symbol)
    applied.append({'key':key,'value':symbol,'always':False,'text':rule['text']})
   elif rule['kind']=='date':
    when=clean_expiry(value)
    if not when:raise ValueError(f'{key} must be a date as YYYY-MM-DD.')
    clauses.append(f'"{column}"=?');params.append(when)
    applied.append({'key':key,'value':when,'always':False,'text':rule['text']})
  # EVERY INSTRUMENT TYPE. This list used to be options only, which hid 129 of the 214 instruments that clear
  # the floors: at the 11:30 reading of 18 Sep 2026, 85 names had an option over them and 214 had a contract of
  # any kind. A name with futures premium and no liquid options is a name where something is happening, and
  # this is the only list on the tab that reaches it. `option_type` still narrows to one side when it is asked,
  # and a future is neither side, so asking for it excludes the futures - which is what asking for it means.
  if 'instrument_type' in names and 'option_type' not in asked:
   applied.append({'key':'instrument_type','value':list(INSTRUMENT_TYPES),'always':True,
    'text':'Every instrument type: calls, puts and futures. This is the F&O book of each name, not its '
     'option chain alone.'})
  wanted=('captured_at','tradingsymbol','underlying','instrument_type','strike','expiry','lot_size',
   'days_to_expiry','last_price','average_price','oi','volume','spot','premium_cr','vol_tod_ratio',
   'vol_tod_median','vol_tod_sessions','vol_tod_status','vol_oi_ratio','vol_oi_prev_oi','vol_oi_status',
   'oi_change_pct_15m','oi_change_pct_day',
   'oi_change_15m','oi_change_day','buildup_15m','buildup_day','price_change_pct_15m','price_change_pct_day',
   'instrument_token','unusual','unusual_reasons')
  have=[c for c in wanted if c in names]
  sql='select '+','.join(f'"{c}"' for c in have)+' from metrics where '+' and '.join(clauses)
  raw=self._rows(sql,tuple(params))
  # How wide this reading was, counted for THIS reading only rather than for every reading in the list.
  scanned=self._reading_rows(at)
  if coverage is not None:coverage['rows']=scanned
  # The row-level filters: the ones that need two columns read together, so they cannot be a WHERE clause on
  # one of them. They are recorded in `applied` exactly like the SQL ones, and they drop rows the same way.
  window=str(asked.get('buildup_window') or BUILDUP_WINDOWS[0])
  if window not in BUILDUP_WINDOWS:raise ValueError(f'buildup_window must be one of {", ".join(BUILDUP_WINDOWS)}.')
  buildup=asked.get('buildup')
  if buildup is not None:
   if str(buildup) not in BUILDUP_VALUES:raise ValueError(f'buildup must be one of {", ".join(BUILDUP_VALUES)}.')
   applied.append({'key':'buildup','value':str(buildup),'always':False,
    'text':f'{self.SCREENER_FILTERS["buildup"]["text"]} Read over the {window} window.'})
   applied.append({'key':'buildup_window','value':window,'always':False,
    'text':self.SCREENER_FILTERS['buildup_window']['text']})
  moneyness=asked.get('moneyness')
  if moneyness is not None:
   if str(moneyness) not in MONEYNESS_VALUES:
    raise ValueError(f'moneyness must be one of {", ".join(MONEYNESS_VALUES)}.')
   applied.append({'key':'moneyness','value':str(moneyness),'always':False,'text':MONEYNESS_TEXT})
  kind=asked.get('underlying_kind')
  if kind is not None:
   if str(kind) not in UNDERLYING_KINDS:
    raise ValueError(f'underlying_kind must be one of {", ".join(UNDERLYING_KINDS)}.')
   applied.append({'key':'underlying_kind','value':str(kind),'always':False,
    'text':self.SCREENER_FILTERS['underlying_kind']['text']})
  rows=[]
  for row in raw:
   shaped=self._screener_row(row,window)
   if not self._passes(shaped,force['applied']):continue
   if buildup is not None and shaped['buildup']!=str(buildup):continue
   if moneyness is not None and shaped['moneyness']!=str(moneyness):continue
   if kind is not None and shaped['underlying_kind']!=str(kind):continue
   rows.append(shaped)
  # RANKED BY SOMETHING THAT WAS MEASURED AT THIS READING. Premium traded when it is there; volume when it is
  # not, because a sort over a column that is null on every row is not a sort. `ranking` below says which.
  if 'premium_cr' in in_force:rows.sort(key=lambda r:-(r.get('premium_cr') or 0.0))
  else:rows.sort(key=lambda r:(-(r.get(FALLBACK_RANK_FIELD) or 0),r.get('tradingsymbol') or ''))
  # The default cut was written for a list of CONTRACTS, where a hundred rows is a page of a long tail. One
  # row per instrument is a different list: there are 214 of them at the 11:30 reading of 18 Sep 2026, and
  # cutting it at a hundred would hide 114 names from the only list that reaches them - which is the bug this
  # view exists to fix, one step further down. A limit the caller ASKED for is still obeyed exactly.
  asked_limit=None if limit in (None,0,'') else max(1,min(int(limit),SCREENER_LIMIT_MAX))
  cut=asked_limit or max(1,min(SCREENER_LIMIT_DEFAULT,SCREENER_LIMIT_MAX))
  # THE VIEW. One row per underlying by default; the contract list is the drill-down. The aggregate is built
  # over EVERY row that cleared the floors and the filters, not over the page - a sum of the first hundred
  # contracts would be a number about this list rather than about the market.
  view=str(asked.get('group') or SCREENER_VIEW_DEFAULT)
  if view not in SCREENER_VIEWS:raise ValueError(f'group must be one of {", ".join(SCREENER_VIEWS)}.')
  groups=self._screener_groups(rows)
  applied.append({'key':'group','value':view,'always':True,
   'text':(('One row per underlying: sums and counts over the contracts that name has at this reading. '
    +SCREENER_RANK_TEXT) if view=='underlying' else CONTRACT_RANK_TEXT)})
  served=groups if view=='underlying' else rows
  if view=='underlying' and asked_limit is None:cut=max(1,min(len(served) or 1,SCREENER_LIMIT_MAX))
  # ========================================================================================================
  # WHAT THE EMPTY SCREEN IS ALLOWED TO SAY.
  #
  # An empty list used to say one thing - "No contract clears the floors" - whatever had actually happened.
  # At the 15:45 reading of 18 Sep 2026 that reading held 10,552 contract rows with ZERO spots and ZERO
  # premiums: the capture had died at 11:30 and the rest of the session was rebuilt from 15-minute candles,
  # which carry no traded-price average. Nothing could clear a premium floor because nothing had a premium -
  # nothing was MEASURED. A trader reads "nothing cleared the floors" as "the market is quiet", which is the
  # opposite of what happened.
  #
  # So the state is resolved from what was measured, and the five states are kept apart: no capture, partial
  # capture, a complete reading with nothing eligible, a complete reading the reader's own filters emptied,
  # and a failed request. The floors-only count is what separates the last two.
  # ========================================================================================================
  cleared=self._cleared_count(at,names)
  capture=self.capture_health(at,readings=readings,cleared=cleared,matched=len(rows))
  # A LATEST COMPLETE READING IS OFFERED, NEVER SUBSTITUTED. The screener stays on the reading it resolved;
  # this only names the newest reading whose required fields were all captured, so the reader can choose it.
  return self.envelope(as_of=at,source='metrics' if self._has_scope() else 'store',missing=[],
   capture=capture,
   context=self.context(str(asked.get('underlying') or ''),str(asked.get('expiry') or ''),
    session_date(at),at,'metrics' if self._has_scope() else 'store',newest_at=newest).as_dict(),
   rows=served[:cut],view=view,views=list(SCREENER_VIEWS),
   # THE SORT THAT IS ACTUALLY IN FORCE, and the closed list of rules a row can be flagged under, so the page
   # never has to guess at either.
   ranking=self._ranking(view,force['applied']),
   unusual_rules=[dict(r) for r in UNUSUAL_RULES],unusual_rules_version=UNUSUAL_RULES_VERSION,
   groups_total=len(groups),contracts_total=len(rows),
   total=len(rows),scanned=scanned,returned=min(len(served),cut),limit=cut,
   scanned_text=('`scanned` is every metric row the store holds at this reading — the whole set the query '
    'seeks over before the floors and the filters. `total` is what came through them, and `returned` is how '
    'many of those this response carries.'),
   applied=applied,available_filters=available,filters={'applied':applied,'available':available},
   readings=readings,coverage=coverage,**reading,
   # THE FLOORS THIS LIST WAS GATED ON, on the card itself. `floors` stays the three §3 constants because
   # they are the tab's definition and they did not change; `floors_applied` / `floors_unmeasured` /
   # `floors_text` are about THIS reading, and they are what a reader has to be able to see.
   buildup_window=window,**self._floor_note(force),moneyness_text=MONEYNESS_TEXT,
   index_underlyings=list(INDEX_KINDS),buildup_values=list(BUILDUP_VALUES),
   buildup_labels=dict(BUILDUP_LABELS),
   # The sentence an empty list prints. It is the CAPTURE STATE's own sentence, so an outage can never be
   # printed as a quiet market and a genuinely complete reading with nothing eligible still says "no matches".
   empty_note=(None if rows else capture['state_text']),
   empty_state=(None if rows else capture['state']))

 # --- WHAT FIRED ON ONE CONTRACT, AS RULES RATHER THAN AS PROSE -------
 @staticmethod
 def _classify_reason(text):
  """One stored reason sentence, read back into (rule id, the value it states).

  The store's own words are the statement of WHICH rules fired at that reading; this turns each sentence back
  into the rule that wrote it. A sentence no rule claims comes back as `unclassified` with no value — it is
  never guessed at, and never folded into a rule that did not fire.
  """
  clean=str(text or '').strip()
  for rule_id,pattern in UNUSUAL_REASON_PATTERNS:
   found=pattern.match(clean)
   if found:
    try:return rule_id,float(found.group(1))
    except ValueError:return rule_id,None
  return RULE_UNCLASSIFIED,None

 @classmethod
 def _unusual_triggers(cls,row):
  """Every rule that fired on one STORED metrics row, structured.

  Rule id, rule version, the measured value, the comparator, the threshold it was compared against, the
  baseline it was measured against and how many observations that baseline stands on. Nothing is recomputed:
  the rules come from the row's own `unusual_reasons`, and every number beside them is one of the row's own
  columns. A value the store no longer carries falls back to the number its own sentence states, and a
  sentence that states none is served as null rather than as a nought.
  """
  out=[]
  for piece in str(row.get('unusual_reasons') or '').split(','):
   piece=piece.strip()
   if not piece:continue
   rule_id,stated=cls._classify_reason(piece)
   rule=UNUSUAL_RULES_BY_ID.get(rule_id) or UNUSUAL_UNCLASSIFIED_RULE
   if rule_id==RULE_VOL_TOD:
    value,baseline=_num(row.get('vol_tod_ratio')),_num(row.get('vol_tod_median'))
    sample=_int(row.get('vol_tod_sessions'))
   elif rule_id==RULE_DAY_VOL_VS_PREV_OI:
    value,baseline=_num(row.get('vol_oi_ratio')),_num(row.get('vol_oi_prev_oi'))
    sample=1
   else:
    value,baseline,sample=None,None,None
   out.append({'rule_id':rule_id,'rule_version':UNUSUAL_RULES_VERSION,
    'value':_round(value if value is not None else stated,4),
    'comparator':rule['comparator'],'threshold':rule['threshold'],
    'baseline':_round(baseline,4),'sample_count':sample,'unit':rule['unit'],
    # THE STORE'S OWN WORDS, kept for a condition no rule claims - that sentence is the only statement of
    # what it was. A sentence a rule DID write is not repeated here: it is that rule at that value, and both
    # are already on this trigger.
    **({'text':piece} if rule_id==RULE_UNCLASSIFIED else {})})
  return out

 def _screener_row(self,row,window=BUILDUP_WINDOWS[0]):
  """One screener row in the tab's own shape. Presentation and two lookups - no signal is computed here."""
  sessions=_int(row.get('vol_tod_sessions'))
  ratio=_num(row.get('vol_tod_ratio'))
  enough=sessions is not None and sessions>=MIN_BASELINE_SESSIONS
  underlying=row.get('underlying') or ''
  strike,spot=_num(row.get('strike')),_num(row.get('spot'))
  window_15m=row.get('buildup_15m') or None
  window_day=row.get('buildup_day') or None
  oi,lot=_num(row.get('oi')),_int(row.get('lot_size'))
  return {'captured_at':row.get('captured_at'),'tradingsymbol':row.get('tradingsymbol') or '',
   'instrument_token':_int(row.get('instrument_token')),'underlying':underlying,
   'underlying_kind':'index' if underlying.upper() in INDEX_KINDS else 'stock',
   'instrument_type':row.get('instrument_type') or '','strike':strike,'expiry':row.get('expiry') or '',
   'lot_size':lot,'days_to_expiry':_int(row.get('days_to_expiry')),
   'moneyness':self._moneyness(strike,spot,row.get('instrument_type')),
   'last_price':_round(row.get('last_price')),'average_price':_round(row.get('average_price')),
   'spot':_round(spot),'oi':_int(oi),'oi_lots':(None if (oi is None or not lot) else int(oi/lot)),
   'volume':_int(row.get('volume')),'premium_cr':_round(row.get('premium_cr')),
   # §3.2 on the serving side too: a ratio without >= MIN_BASELINE_SESSIONS behind it is not served at all.
   'volume_ratio':_round(ratio,4) if enough else None,'volume_baseline_sessions':sessions,
   'volume_baseline':'ok' if enough else 'none','volume_baseline_status':row.get('vol_tod_status') or None,
   # WHAT THE RATIO WAS MEASURED AGAINST. A multiple with no denominator beside it cannot be judged: 782x a
   # median of four lots and 782x a median of forty thousand are not the same event.
   'volume_baseline_median':_round(row.get('vol_tod_median'),4),
   'volume_to_oi':_round(row.get('vol_oi_ratio'),4),'volume_to_oi_status':row.get('vol_oi_status') or None,
   'volume_to_oi_prev_oi':_round(row.get('vol_oi_prev_oi'),4),
   'oi_change_15m':_int(row.get('oi_change_15m')),
   'oi_change_15m_pct':_round(row.get('oi_change_pct_15m'),4),
   'oi_change_day':_int(row.get('oi_change_day')),
   'oi_change_day_pct':_round(row.get('oi_change_pct_day'),4),
   'price_change_15m_pct':_round(row.get('price_change_pct_15m'),4),
   'price_change_day_pct':_round(row.get('price_change_pct_day'),4),
   'buildup_15m':window_15m,'buildup_day':window_day,
   # The label the `buildup` filter read, and which window it read it over. §3.1 forbids mixing the two, so
   # the row says which one this is rather than leaving the caller to guess from a bare label.
   'buildup':(window_day if window=='day' else window_15m),'buildup_window':window,
   'unusual':bool(_int(row.get('unusual'))),'unusual_reasons':row.get('unusual_reasons') or None,
   # The SAME flags, as rules rather than as sentences: rule id, version, value, comparator, threshold,
   # baseline and sample count. The prose above is kept because it is what the store actually wrote.
   'unusual_triggers':self._unusual_triggers(row)}
