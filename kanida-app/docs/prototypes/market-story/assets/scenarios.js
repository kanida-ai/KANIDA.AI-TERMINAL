/* KANIDA market story — illustrative fixtures.
 *
 * EVERY NUMBER IN THIS FILE IS SYNTHETIC. Nothing here came from an exchange, a broker or the pilot
 * store. It exists so the owner can judge layout, narrative voice and motion without signing in.
 *
 * The rules the fixture keeps to, because the prototype is meant to demonstrate them:
 *   - A reading is a snapshot. Selecting it shows what was knowable AT it, never later.
 *   - Duration is measured from first detection, not from the session's first reading.
 *   - A strike is only "participating" when it is named in `strikes` — a broadening cluster has
 *     actual extra strikes behind it.
 *   - A reading that was never captured is a gap: no values, no behaviour, no carried-forward story.
 *   - Behaviour is an INFERENCE. Where premium and volatility disagree with open interest, no
 *     behaviour is claimed at all.
 */
(function (global) {
  'use strict';

  /* Shared vocabulary, so the narrative and the chips can never drift apart. */
  var BREADTH = {
    isolated: 'isolated · one strike',
    clustered: 'clustered · adjacent strikes',
    dispersed: 'dispersed · gaps between strikes',
    none: 'nothing participating'
  };

  var INFER =
    'Consistent with the observations above. Open interest and premium cannot identify who ' +
    'initiated a trade, so this is an inference, not known participant intent — and not a trade instruction.';

  var NO_INFER =
    'No behaviour is claimed at this reading. Buying and writing both add open interest; premium ' +
    'and implied volatility are what separate them, and here they do not agree.';

  /* ---------------------------------------------------------------------------------------------
   * SESSION A — Monday 22 September 2026, NIFTY, 25 Sep expiry.
   * A call buildup appears at one strike, broadens to three, cools, then narrows back to one.
   * Scenario 1 reads this session as far as 10:00. Scenario 2 reads the same session to 10:30.
   * ------------------------------------------------------------------------------------------- */

  var A_LADDER = [23450, 23400, 23350, 23300, 23250];

  var A = [
    {
      at: '09:15', taken: true, quality: 'complete', spot: 23318,
      timelineTitle: 'Baseline recorded',
      headline: 'Baseline recorded for this session',
      sentences: [
        'This is the first reading captured today, so there is nothing earlier to measure it against.',
        'Calls and puts are both quoted across the ladder, and no strike stands apart from its neighbours yet.'
      ],
      inference: 'No behaviour is claimed at a first reading. The comparison this session will use starts here.',
      persistence: 'First reading of the session · no comparison baseline',
      calls: { behavior: 'none', state: 'baseline', text: 'Nothing to compare against yet',
        location: '—', lead: null, breadth: 'none', strikes: [], firstAt: null, durationMin: null, scans: 0,
        note: 'Open interest and premium were captured for all five strikes; a change needs a second reading.' },
      puts: { behavior: 'none', state: 'baseline', text: 'Nothing to compare against yet',
        location: '—', lead: null, breadth: 'none', strikes: [], firstAt: null, durationMin: null, scans: 0,
        note: 'Open interest and premium were captured for all five strikes; a change needs a second reading.' },
      combined: 'Calls and puts are both quiet against a baseline that does not exist yet. The session starts here.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:15 IST · complete'],
          ['Strikes quoted', '5 of 5 · calls and puts'],
          ['Spot', '₹23,318'],
          ['Comparison available', 'none — first reading']
        ],
        inference: 'None offered. A single reading describes a state, not a change.',
        baseline: 'No baseline. The previous scan does not exist, and the previous session close is not read in this preview.',
        uncertainty: ['Everything below this reading is a level, not a movement.'],
        numbers: []
      }
    },
    {
      at: '09:30', taken: true, quality: 'complete', spot: 23331,
      timelineTitle: 'Fresh call interest appears',
      headline: 'Fresh call interest appears at 23,350',
      sentences: [
        'Open interest at 23,350 CE rose 41% against the 09:15 reading, and its premium and implied volatility rose with it.',
        'No strike on either side of it moved comparably, so this is a single-strike move for now.'
      ],
      inference: 'Consistent with call buying. ' + INFER.slice(INFER.indexOf('Open interest')),
      persistence: 'First detected 09:30 IST · 1 of 5 strikes · 23,350 CE',
      calls: { behavior: 'buying', state: 'appearing', text: 'Buying · appearing',
        location: '23,350 CE', lead: 23350, breadth: 'isolated', strikes: [23350],
        firstAt: '09:30', durationMin: 0, scans: 1,
        note: 'Open interest, premium and implied volatility all moved the same way at one strike.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: '23,300 PE shed a little open interest; the change is smaller than this reading can separate from noise.' },
      combined: 'Activity is one-sided so far. The put strikes are not confirming or contradicting it — they are quiet.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:30 IST · complete'],
          ['Strikes participating', '1 of 5 · 23,350 CE'],
          ['Spot', '₹23,331 · +13 from 09:15'],
          ['Compared against', 'the 09:15 reading, 15 minutes back']
        ],
        inference: 'Open interest, premium and implied volatility moved together at the same strike, which is the pattern buying leaves behind.',
        baseline: 'Previous scan (09:15). The session open and the previous close are not used at this reading.',
        uncertainty: [
          'One strike is a thin base. A second reading is what turns this into a pattern.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23350, side: 'CE', doi: '+18,450', doiPct: '+41%', price: '82.40', priceChg: '+12.35', iv: '14.8%', ivChg: '+0.9' },
          { strike: 23400, side: 'CE', doi: '+1,150', doiPct: '+3%', price: '54.10', priceChg: '+1.05', iv: '14.2%', ivChg: '+0.1' }
        ],
        quiet: '23,450 CE, 23,300 PE and 23,250 PE showed no material change at this reading.'
      }
    },
    {
      at: '09:45', taken: true, quality: 'complete', spot: 23346,
      timelineTitle: 'Buying spreads to a second strike',
      headline: 'Call buying is spreading above 23,350',
      sentences: [
        '23,400 CE has joined 23,350 CE — both added open interest this interval while their premiums and implied volatility rose together.',
        '23,350 CE still carries the larger position change, so it remains the centre of the activity.'
      ],
      inference: INFER,
      persistence: 'Continuing 15 min · 2 consecutive readings · 2 of 5 strikes · 23,350 CE leads',
      calls: { behavior: 'buying', state: 'broadening', text: 'Buying · broadening',
        location: '23,350–23,400 CE', lead: 23350, breadth: 'clustered', strikes: [23350, 23400],
        firstAt: '09:30', durationMin: 15, scans: 2,
        note: 'One more strike than the previous reading, and the two are adjacent on the ladder.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike has added or shed open interest beyond what this reading can separate from noise.' },
      combined: 'Still one-sided. Nothing on the put side is confirming the call activity, and nothing is contradicting it.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:45 IST · complete'],
          ['Strikes participating', '2 of 5 · 23,350 and 23,400 CE'],
          ['Spot', '₹23,346 · +15 from 09:30'],
          ['Compared against', 'the 09:30 reading, 15 minutes back']
        ],
        inference: 'The same three measurements agree at two adjacent strikes now instead of one. Adjacency matters: a cluster is harder to explain as a single large order than an isolated strike is.',
        baseline: 'Previous scan (09:30). Persistence is counted from first detection at 09:30, not from the session’s first reading at 09:15.',
        uncertainty: [
          'Two strikes is still a narrow base.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23350, side: 'CE', doi: '+14,900', doiPct: '+23%', price: '94.75', priceChg: '+12.35', iv: '15.4%', ivChg: '+0.6' },
          { strike: 23400, side: 'CE', doi: '+11,300', doiPct: '+27%', price: '62.80', priceChg: '+8.70', iv: '15.1%', ivChg: '+0.9' }
        ],
        quiet: '23,450 CE, 23,300 PE and 23,250 PE showed no material change at this reading.'
      }
    },
    {
      at: '10:00', taken: true, quality: 'complete', spot: 23362,
      timelineTitle: 'A third strike joins',
      headline: 'Call buying now spans three strikes',
      sentences: [
        '23,450 CE has joined, and 23,350 and 23,400 CE both added open interest again with their premiums.',
        'The activity is no longer concentrated at one strike, and 23,350 CE still leads it.'
      ],
      inference: INFER,
      persistence: 'Continuing 30 min · 3 consecutive readings · 3 of 5 strikes · 23,350 CE leads',
      calls: { behavior: 'buying', state: 'broadening', text: 'Buying · broadening',
        location: '23,350–23,450 CE', lead: 23350, breadth: 'clustered', strikes: [23350, 23400, 23450],
        firstAt: '09:30', durationMin: 30, scans: 3,
        note: 'Three adjacent strikes, each adding open interest with premium. The cluster has widened upward twice.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'The put strikes have not participated at any reading of this session so far.' },
      combined: 'One-sided and widening. Spot has risen ₹44 since first detection, which is consistent with the call side leading — but spot movement is not evidence of who is trading.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '10:00 IST · complete'],
          ['Strikes participating', '3 of 5 · 23,350, 23,400 and 23,450 CE'],
          ['Spot', '₹23,362 · +16 from 09:45 · +44 since first detection'],
          ['Compared against', 'the 09:45 reading, 15 minutes back']
        ],
        inference: 'The cluster has widened at two consecutive readings while the same three measurements kept agreeing. That is what "broadening" means here, and it is a stronger base than a single strike.',
        baseline: 'Previous scan (09:45) for the change; first detection (09:30) for the 30-minute duration.',
        uncertainty: [
          'Breadth is measured over the five strikes shown, not the whole chain.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23350, side: 'CE', doi: '+12,050', doiPct: '+15%', price: '108.20', priceChg: '+13.45', iv: '15.9%', ivChg: '+0.5' },
          { strike: 23400, side: 'CE', doi: '+9,880', doiPct: '+18%', price: '74.55', priceChg: '+11.75', iv: '15.7%', ivChg: '+0.6' },
          { strike: 23450, side: 'CE', doi: '+7,240', doiPct: '+21%', price: '48.30', priceChg: '+8.05', iv: '15.5%', ivChg: '+0.7' }
        ],
        quiet: '23,300 PE and 23,250 PE showed no material change at this reading.'
      }
    },
    {
      at: '10:15', taken: true, quality: 'complete', spot: 23368,
      timelineTitle: 'The pace cools',
      headline: 'The call buildup is slowing',
      sentences: [
        'Positions are still being added at all three strikes, but premiums have stopped rising with them.',
        'The earlier pattern is intact; what has changed is the pace behind it.'
      ],
      inference: INFER,
      persistence: 'Continuing 45 min · 4 consecutive readings · 3 of 5 strikes · 23,350 CE leads',
      calls: { behavior: 'buying', state: 'slowing', text: 'Buying · slowing',
        location: '23,350–23,450 CE', lead: 23350, breadth: 'clustered', strikes: [23350, 23400, 23450],
        firstAt: '09:30', durationMin: 45, scans: 4,
        note: 'Open interest still rising; premium changes are a fifth of what they were at 10:00.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'The put strikes have not participated at any reading of this session.' },
      combined: 'The call side is still the only side moving, but the agreement between its measurements is weaker here than it was at 10:00.',
      conflict: 'Open interest is still building; premiums have stopped confirming it at the same strength.',
      evidence: {
        facts: [
          ['Reading captured', '10:15 IST · complete'],
          ['Strikes participating', '3 of 5 · unchanged from 10:00'],
          ['Spot', '₹23,368 · +6 from 10:00'],
          ['Compared against', 'the 10:00 reading, 15 minutes back']
        ],
        inference: 'The behaviour is unchanged and the cluster is unchanged, so the pattern continues. The word is "slowing" because the premium change fell from ₹13.45 to ₹2.10 at the leading strike while open interest kept rising.',
        baseline: 'Previous scan (10:00) for the change; first detection (09:30) for the 45-minute duration.',
        uncertainty: [
          'Position changes and premium changes no longer agree in size. The interpretation is weaker here than at 10:00.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23350, side: 'CE', doi: '+6,410', doiPct: '+7%', price: '110.30', priceChg: '+2.10', iv: '15.8%', ivChg: '-0.1' },
          { strike: 23400, side: 'CE', doi: '+5,220', doiPct: '+8%', price: '76.05', priceChg: '+1.50', iv: '15.6%', ivChg: '-0.1' },
          { strike: 23450, side: 'CE', doi: '+3,900', doiPct: '+9%', price: '49.25', priceChg: '+0.95', iv: '15.4%', ivChg: '-0.1' }
        ],
        quiet: '23,300 PE and 23,250 PE showed no material change at this reading.'
      }
    },
    {
      at: '10:30', taken: true, quality: 'complete', spot: 23355,
      timelineTitle: 'The cluster narrows back',
      headline: 'The buildup has narrowed back to 23,350',
      sentences: [
        '23,400 and 23,450 CE stopped adding positions this interval; only 23,350 CE is still doing so.',
        'The wider participation that ran from 09:45 is no longer there — what remains is the strike it started at.'
      ],
      inference: INFER,
      persistence: 'Continuing 60 min · 5 consecutive readings · 1 of 5 strikes · 23,350 CE leads',
      calls: { behavior: 'buying', state: 'concentrating', text: 'Buying · concentrating',
        location: '23,350 CE', lead: 23350, breadth: 'isolated', strikes: [23350],
        firstAt: '09:30', durationMin: 60, scans: 5,
        note: 'Two of the three strikes that were participating at 10:15 are not participating now.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'The put strikes have not participated at any reading of this session.' },
      combined: 'The behaviour has not reversed — positions are not being closed. It has narrowed, which is a different thing, and the wording tracks that difference.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '10:30 IST · complete'],
          ['Strikes participating', '1 of 5 · down from 3 at 10:15'],
          ['Spot', '₹23,355 · −13 from 10:15'],
          ['Compared against', 'the 10:15 reading, 15 minutes back']
        ],
        inference: 'Narrowing is not reversal. No strike shed open interest this interval; two simply stopped adding. The duration keeps running because the leading strike never stopped.',
        baseline: 'Previous scan (10:15) for the change; first detection (09:30) for the 60-minute duration.',
        uncertainty: [
          'A single strike is a thin base again, as it was at 09:30.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23350, side: 'CE', doi: '+4,780', doiPct: '+5%', price: '104.60', priceChg: '-5.70', iv: '15.5%', ivChg: '-0.3' },
          { strike: 23400, side: 'CE', doi: '+310', doiPct: '+0.4%', price: '71.40', priceChg: '-4.65', iv: '15.3%', ivChg: '-0.3' },
          { strike: 23450, side: 'CE', doi: '+95', doiPct: '+0.2%', price: '45.10', priceChg: '-4.15', iv: '15.2%', ivChg: '-0.2' }
        ],
        quiet: '23,300 PE and 23,250 PE showed no material change at this reading.'
      }
    }
  ];

  /* ---------------------------------------------------------------------------------------------
   * SESSION B — Tuesday 23 September 2026. Open interest rises; premium and volatility disagree.
   * No behaviour is claimed at any reading, and no direction is forced.
   * ------------------------------------------------------------------------------------------- */

  var B_LADDER = [23400, 23350, 23300, 23250, 23200];

  var B = [
    {
      at: '09:15', taken: true, quality: 'complete', spot: 23290,
      timelineTitle: 'Baseline recorded',
      headline: 'Baseline recorded for this session',
      sentences: [
        'This is the first reading captured today, so there is nothing earlier to measure it against.',
        'Calls and puts are both quoted across the ladder, and no strike stands apart from its neighbours yet.'
      ],
      inference: 'No behaviour is claimed at a first reading. The comparison this session will use starts here.',
      persistence: 'First reading of the session · no comparison baseline',
      calls: { behavior: 'none', state: 'baseline', text: 'Nothing to compare against yet',
        location: '—', lead: null, breadth: 'none', strikes: [], firstAt: null, durationMin: null, scans: 0,
        note: 'Open interest and premium were captured for all five strikes; a change needs a second reading.' },
      puts: { behavior: 'none', state: 'baseline', text: 'Nothing to compare against yet',
        location: '—', lead: null, breadth: 'none', strikes: [], firstAt: null, durationMin: null, scans: 0,
        note: 'Open interest and premium were captured for all five strikes; a change needs a second reading.' },
      combined: 'Calls and puts are both quiet against a baseline that does not exist yet. The session starts here.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:15 IST · complete'],
          ['Strikes quoted', '5 of 5 · calls and puts'],
          ['Spot', '₹23,290'],
          ['Comparison available', 'none — first reading']
        ],
        inference: 'None offered. A single reading describes a state, not a change.',
        baseline: 'No baseline. The previous scan does not exist, and the previous session close is not read in this preview.',
        uncertainty: ['Everything below this reading is a level, not a movement.'],
        numbers: []
      }
    },
    {
      at: '09:30', taken: true, quality: 'complete', spot: 23281,
      timelineTitle: 'Positions build, premiums disagree',
      headline: 'Positions are building at 23,300, premiums are not confirming',
      sentences: [
        '23,300 CE added 12% to its open interest, but its premium fell ₹3.85 and implied volatility barely moved.',
        'Position growth on its own does not say whether the new contracts were bought or written.'
      ],
      inference: NO_INFER,
      persistence: 'First seen 09:30 IST · 1 of 5 strikes · no behaviour claimed',
      calls: { behavior: 'unconfirmed', state: 'unconfirmed', text: 'Unconfirmed',
        location: '23,300 CE', lead: 23300, breadth: 'isolated', strikes: [23300],
        firstAt: '09:30', durationMin: 0, scans: 1,
        note: 'Open interest up, premium down, implied volatility flat. These three do not describe one behaviour.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike moved beyond what this reading can separate from noise.' },
      combined: 'Nothing is combined, because there is no call reading to combine with the put side. The observation stands; the interpretation does not.',
      conflict: 'Open interest is building, but premiums are not confirming it.',
      evidence: {
        facts: [
          ['Reading captured', '09:30 IST · complete'],
          ['Strikes with a position change', '1 of 5 · 23,300 CE'],
          ['Spot', '₹23,281 · −9 from 09:15'],
          ['Compared against', 'the 09:15 reading, 15 minutes back']
        ],
        inference: 'Withheld. Buying would normally lift premium with open interest; writing would normally push it down with falling implied volatility. Neither picture is complete here, so neither is claimed.',
        baseline: 'Previous scan (09:15).',
        uncertainty: [
          'This is an unresolved observation, not a weak signal. It is not a quiet market either — something is being traded.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23300, side: 'CE', doi: '+9,640', doiPct: '+12%', price: '71.15', priceChg: '-3.85', iv: '13.9%', ivChg: '+0.0' }
        ],
        quiet: '23,400 CE, 23,350 CE, 23,250 PE and 23,200 PE showed no material change at this reading.'
      }
    },
    {
      at: '09:45', taken: true, quality: 'complete', spot: 23274,
      timelineTitle: 'The disagreement widens',
      headline: 'A second strike shows the same disagreement',
      sentences: [
        '23,350 CE has joined 23,300 CE — both added open interest while both premiums fell.',
        'The pattern is now at two strikes, which makes it harder to dismiss as noise and no easier to interpret.'
      ],
      inference: NO_INFER,
      persistence: 'Unresolved for 15 min · 2 consecutive readings · 2 of 5 strikes',
      calls: { behavior: 'unconfirmed', state: 'unconfirmed', text: 'Unconfirmed',
        location: '23,300–23,350 CE', lead: 23300, breadth: 'clustered', strikes: [23300, 23350],
        firstAt: '09:30', durationMin: 15, scans: 2,
        note: 'Two adjacent strikes, same disagreement: open interest up, premium down, implied volatility flat.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike moved beyond what this reading can separate from noise.' },
      combined: 'Breadth is a fact about the observation, not a resolution of it. Two strikes disagreeing is still a disagreement.',
      conflict: 'Open interest is building at two strikes; premiums are falling at both.',
      evidence: {
        facts: [
          ['Reading captured', '09:45 IST · complete'],
          ['Strikes with a position change', '2 of 5 · 23,300 and 23,350 CE'],
          ['Spot', '₹23,274 · −7 from 09:30'],
          ['Compared against', 'the 09:30 reading, 15 minutes back']
        ],
        inference: 'Still withheld. Spot has drifted down ₹16 since 09:15, which would be consistent with call writing — but implied volatility has not fallen, and it normally would. The prototype does not resolve this by picking the more convenient half.',
        baseline: 'Previous scan (09:30).',
        uncertainty: [
          'Persistence is counted for the disagreement, not for a behaviour. Nothing is being confirmed here.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23300, side: 'CE', doi: '+7,120', doiPct: '+8%', price: '66.40', priceChg: '-4.75', iv: '13.9%', ivChg: '+0.0' },
          { strike: 23350, side: 'CE', doi: '+6,050', doiPct: '+11%', price: '44.85', priceChg: '-3.20', iv: '13.8%', ivChg: '-0.1' }
        ],
        quiet: '23,400 CE, 23,250 PE and 23,200 PE showed no material change at this reading.'
      }
    },
    {
      at: '10:00', taken: true, quality: 'complete', spot: 23286,
      timelineTitle: 'A third reading does not resolve it',
      headline: 'Three readings in, the disagreement is unresolved',
      sentences: [
        'Both strikes added open interest again, and spot has turned back up ₹12 without lifting either premium.',
        'Nothing in this session has yet separated buying from writing at these strikes.'
      ],
      inference: NO_INFER,
      persistence: 'Unresolved for 30 min · 3 consecutive readings · 2 of 5 strikes',
      calls: { behavior: 'unconfirmed', state: 'unconfirmed', text: 'Unconfirmed',
        location: '23,300–23,350 CE', lead: 23300, breadth: 'clustered', strikes: [23300, 23350],
        firstAt: '09:30', durationMin: 30, scans: 3,
        note: 'A third consecutive reading with the same three measurements pointing in different directions.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike has moved beyond noise at any reading of this session.' },
      combined: 'No directional reading is offered, and none will be offered by repetition. Persistence makes an observation more reliable; it does not make an ambiguous one clear.',
      conflict: 'Open interest is building and spot has recovered, and premiums have confirmed neither.',
      evidence: {
        facts: [
          ['Reading captured', '10:00 IST · complete'],
          ['Strikes with a position change', '2 of 5 · unchanged from 09:45'],
          ['Spot', '₹23,286 · +12 from 09:45'],
          ['Compared against', 'the 09:45 reading, 15 minutes back']
        ],
        inference: 'Withheld for a third reading. This is the honest outcome of the rule, not a failure of it — a label here would be a guess wearing the same typeface as a measurement.',
        baseline: 'Previous scan (09:45) for the change; first observation (09:30) for the 30-minute span.',
        uncertainty: [
          'This state can persist for a whole session. It is reported as what it is.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23300, side: 'CE', doi: '+5,880', doiPct: '+6%', price: '65.90', priceChg: '-0.50', iv: '13.7%', ivChg: '-0.2' },
          { strike: 23350, side: 'CE', doi: '+4,410', doiPct: '+7%', price: '44.10', priceChg: '-0.75', iv: '13.6%', ivChg: '-0.2' }
        ],
        quiet: '23,400 CE, 23,250 PE and 23,200 PE showed no material change at this reading.'
      }
    }
  ];

  /* ---------------------------------------------------------------------------------------------
   * SESSION C — Wednesday 24 September 2026. Capture starts late, one reading is never taken, and
   * the reading after it arrives without premium or volatility. The earlier story is held as
   * history and its duration is NOT extended across the gap.
   * ------------------------------------------------------------------------------------------- */

  var C_LADDER = [23500, 23450, 23400, 23350, 23300];

  var C = [
    {
      at: '09:30', taken: true, quality: 'complete', spot: 23402,
      timelineTitle: 'First reading of the day',
      headline: 'Fresh call interest at 23,400',
      sentences: [
        'Today’s capture begins at 09:30 IST, and 23,400 CE added 29% to its open interest against it with premium and implied volatility rising.',
        'There is no 09:15 observation, so nothing here is measured against the market open.'
      ],
      inference: 'Consistent with call buying, against a 09:30 baseline rather than an opening one. ' +
        'Open interest and premium cannot identify who initiated a trade, so this is an inference, not known participant intent.',
      persistence: 'First detected 09:30 IST · 1 of 5 strikes · 23,400 CE',
      calls: { behavior: 'buying', state: 'appearing', text: 'Buying · appearing',
        location: '23,400 CE', lead: 23400, breadth: 'isolated', strikes: [23400],
        firstAt: '09:30', durationMin: 0, scans: 1,
        note: 'Measured against the first captured reading of the day, which is 09:30 — not the open.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike moved beyond what this reading can separate from noise.' },
      combined: 'One-sided, on a session whose first 15 minutes were never observed. What happened before 09:30 is unknown, not quiet.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:30 IST · complete'],
          ['Strikes participating', '1 of 5 · 23,400 CE'],
          ['Spot', '₹23,402'],
          ['Compared against', 'the 09:30 reading — the first the store holds today']
        ],
        inference: 'The three measurements agree at one strike. The baseline is weaker than session A’s because there is no earlier reading behind it.',
        baseline: 'First captured reading (09:30). The market open is not used and is not assumed.',
        uncertainty: [
          'The 09:15 interval was never captured. Any activity in it is unobserved, which is not the same as absent.',
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23400, side: 'CE', doi: '+13,720', doiPct: '+29%', price: '88.15', priceChg: '+9.40', iv: '16.1%', ivChg: '+1.1' }
        ],
        quiet: '23,500 CE, 23,450 CE, 23,350 PE and 23,300 PE showed no material change at this reading.'
      }
    },
    {
      at: '09:45', taken: true, quality: 'complete', spot: 23419,
      timelineTitle: 'It spreads to a second strike',
      headline: 'Call buying spreads to 23,450',
      sentences: [
        '23,450 CE has joined 23,400 CE, and both added open interest with their premiums and implied volatility.',
        '23,400 CE still carries the larger position change and remains the centre of the activity.'
      ],
      inference: INFER,
      persistence: 'Continuing 15 min · 2 consecutive readings · 2 of 5 strikes · 23,400 CE leads',
      calls: { behavior: 'buying', state: 'broadening', text: 'Buying · broadening',
        location: '23,400–23,450 CE', lead: 23400, breadth: 'clustered', strikes: [23400, 23450],
        firstAt: '09:30', durationMin: 15, scans: 2,
        note: 'One more strike than the previous reading, and the two are adjacent on the ladder.' },
      puts: { behavior: 'none', state: 'quiet', text: 'Nothing material',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0,
        note: 'No put strike moved beyond what this reading can separate from noise.' },
      combined: 'One-sided and widening, on a partial session. This is the last reading at which any behaviour is confirmed today.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '09:45 IST · complete'],
          ['Strikes participating', '2 of 5 · 23,400 and 23,450 CE'],
          ['Spot', '₹23,419 · +17 from 09:30'],
          ['Compared against', 'the 09:30 reading, 15 minutes back']
        ],
        inference: 'The same three measurements agree at two adjacent strikes. Duration runs from first detection at 09:30.',
        baseline: 'Previous scan (09:30).',
        uncertainty: [
          'Implied volatility here is model-derived from premium and spot, not an exchange measurement.'
        ],
        numbers: [
          { strike: 23400, side: 'CE', doi: '+10,480', doiPct: '+17%', price: '99.60', priceChg: '+11.45', iv: '16.6%', ivChg: '+0.5' },
          { strike: 23450, side: 'CE', doi: '+8,150', doiPct: '+22%', price: '68.25', priceChg: '+8.90', iv: '16.4%', ivChg: '+0.6' }
        ],
        quiet: '23,500 CE, 23,350 PE and 23,300 PE showed no material change at this reading.'
      }
    },
    {
      at: '10:00', taken: false, quality: 'missing', spot: null,
      timelineTitle: 'No reading captured',
      headline: 'No reading was captured at 10:00',
      sentences: [
        'The 10:00 capture did not run, so there is no observation of this interval at all.',
        'Nothing is carried forward across it, and no behaviour is claimed for it.'
      ],
      inference: 'A gap is an absence of measurement. It is not evidence that the market was quiet, and it is not evidence that it was busy.',
      persistence: 'Earlier buildup last confirmed 09:45 · 15 min · not extended across this gap',
      calls: { behavior: 'gap', state: 'gap', text: 'Not observed',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: '09:30', durationMin: 15, scans: 0, frozen: true,
        note: 'The 09:30–09:45 buildup is held in session memory as history. Its confirmed duration stops at 09:45.' },
      puts: { behavior: 'gap', state: 'gap', text: 'Not observed',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0, frozen: true,
        note: 'No observation exists for this interval on either side.' },
      combined: 'Nothing is combined across a gap. The next captured reading starts a new comparison rather than continuing the old one.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '10:00 IST · none'],
          ['Strikes participating', 'not observed'],
          ['Spot', 'not captured'],
          ['Compared against', 'nothing — there is no reading to compare']
        ],
        inference: 'None. The prototype draws no line across this interval and prints no value in it.',
        baseline: 'Unavailable. The next captured reading will establish a new comparison baseline rather than reach back to 09:45.',
        uncertainty: [
          'Unavailable observation is not quiet market activity. The distinction is the whole point of this reading.',
          'Any activity between 09:45 and the next capture is unobserved.'
        ],
        numbers: []
      }
    },
    {
      at: '10:15', taken: true, quality: 'partial', spot: 23431,
      timelineTitle: 'The reading returns incomplete',
      headline: 'The 10:15 reading is incomplete',
      sentences: [
        'Open interest and spot were captured at 10:15, but no traded-price average and no implied volatility were.',
        'Position changes on their own cannot separate buying from writing, so no behaviour is claimed for this reading.'
      ],
      inference: 'Withheld for want of measurement rather than for want of agreement. Two of the three inputs the rule needs are absent here.',
      persistence: 'Earlier buildup last confirmed 09:45 · 15 min · not extended across the gap',
      calls: { behavior: 'unconfirmed', state: 'partial', text: 'Not measurable',
        location: '23,400–23,450 CE', lead: null, breadth: 'none', strikes: [],
        firstAt: '09:30', durationMin: 15, scans: 0, frozen: true,
        note: 'Open interest rose at both earlier strikes, but premium and implied volatility are missing, so this is not a confirmed continuation.' },
      puts: { behavior: 'unconfirmed', state: 'partial', text: 'Not measurable',
        location: '—', lead: null, breadth: 'none', strikes: [],
        firstAt: null, durationMin: null, scans: 0, frozen: true,
        note: 'The same two fields are missing on the put side.' },
      combined: 'The morning’s story is preserved as history and is not extended. A reading that is missing the fields the rule depends on cannot confirm anything.',
      conflict: null,
      evidence: {
        facts: [
          ['Reading captured', '10:15 IST · partial'],
          ['Fields present', 'open interest 5 of 5 · spot 1 of 1'],
          ['Fields absent', 'traded-price average 0 of 5 · implied volatility 0 of 5'],
          ['Compared against', 'the 09:45 reading — the last complete one, 30 minutes back']
        ],
        inference: 'Withheld. The prototype shows the open-interest changes it does have and refuses the label it cannot support.',
        baseline: 'Last complete reading (09:45), named rather than substituted. The 10:00 gap is not bridged.',
        uncertainty: [
          'A reading rebuilt without a traded-price average carries no premium to measure against.',
          'Open interest rose at both strikes, and that alone is consistent with buying and with writing.'
        ],
        numbers: [
          { strike: 23400, side: 'CE', doi: '+6,900', doiPct: '+10%', price: '—', priceChg: '—', iv: '—', ivChg: '—' },
          { strike: 23450, side: 'CE', doi: '+5,240', doiPct: '+12%', price: '—', priceChg: '—', iv: '—', ivChg: '—' }
        ],
        quiet: '23,500 CE, 23,350 PE and 23,300 PE showed no material open-interest change at this reading.'
      }
    }
  ];

  /* --------------------------------------------------------------------------------------------- */

  var SCENARIOS = [
    {
      id: 'broadening',
      name: 'Buildup broadens',
      blurb: 'One strike becomes three across three readings',
      instrument: 'NIFTY',
      expiry: '25 Sep 2026',
      dte: 3,
      sessionLabel: 'Mon 22 Sep 2026',
      mode: 'Session read back',
      ladder: A_LADDER,
      readings: A.slice(0, 4),
      defaultIndex: 3,
      capture: { tone: 'ok', label: 'Capture complete', detail: 'Every field this preview uses was present at all four readings of this illustrative session.' }
    },
    {
      id: 'weakening',
      name: 'Buildup weakens',
      blurb: 'The same session, two readings later, narrowing',
      instrument: 'NIFTY',
      expiry: '25 Sep 2026',
      dte: 3,
      sessionLabel: 'Mon 22 Sep 2026',
      mode: 'Session read back',
      ladder: A_LADDER,
      readings: A,
      defaultIndex: 5,
      capture: { tone: 'ok', label: 'Capture complete', detail: 'Every field this preview uses was present at all six readings of this illustrative session.' }
    },
    {
      id: 'conflict',
      name: 'Signals disagree',
      blurb: 'Open interest rises, premium and volatility do not agree',
      instrument: 'NIFTY',
      expiry: '25 Sep 2026',
      dte: 2,
      sessionLabel: 'Tue 23 Sep 2026',
      mode: 'Session read back',
      ladder: B_LADDER,
      readings: B,
      defaultIndex: 3,
      capture: { tone: 'ok', label: 'Capture complete', detail: 'The data is complete. It is the interpretation that is unresolved, and the two are shown as different things.' }
    },
    {
      id: 'partial',
      name: 'Capture gap',
      blurb: 'A reading never taken, then one that returns incomplete',
      instrument: 'NIFTY',
      expiry: '25 Sep 2026',
      dte: 1,
      sessionLabel: 'Wed 24 Sep 2026',
      mode: 'Session read back',
      ladder: C_LADDER,
      readings: C,
      defaultIndex: 3,
      capture: { tone: 'warn', label: 'Partial capture', detail: 'Capture began at 09:30, the 10:00 reading never ran, and the 10:15 reading carries open interest without a traded-price average or implied volatility.' }
    }
  ];

  global.KANIDA_SCENARIOS = SCENARIOS;
  global.KANIDA_BREADTH = BREADTH;
})(window);
