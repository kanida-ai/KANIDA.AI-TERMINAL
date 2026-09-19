# Harmonic and price-action drawing reaudit

## Result

Inspected **52/52 directional variants**: 26 harmonic and 26 price-action drawings across 18 pattern IDs. Every entry is bound to its exact frozen event identity and screenshot in the JSON companion. All 53 focused tests pass; 89 pass when combined with the existing drawing accuracy suite.

The published harmonic failure level was missing. It is now drawn verbatim; all 26 affected drawings were visually rechecked. Projected PRZ meaning and setup/confirmed status are explicit. HA10 auxiliary signed AD/XA is omitted from its display only; both final 5-0 drawings were visually rechecked after deployment.

## Scope and limits

One actual frozen example per variant and side, at its observed timeframe. This validates the inspected drawing semantics and rendering; it does not certify detector validity, trading value, every historical occurrence, or every timeframe. No detector or frozen event was changed.

## Evidence

| Pattern | Variant | Side | Symbol / timeframe | Screenshot |
|---|---|---|---|---|
| HA01 | canonical | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r1.png) |
| HA01 | canonical | short | AADHARHFC / 1W | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r1.png) |
| HA02 | ext_1_27 | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r1.png) |
| HA02 | ext_1_27 | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r2.png) |
| HA02 | ext_1_618 | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r2.png) |
| HA02 | ext_1_618 | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r2.png) |
| HA03 | canonical | long | ABSLAMC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r3.png) |
| HA03 | canonical | short | ABSLAMC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r3.png) |
| HA04 | b_0_382 | long | TATAPOWER / 4H | [View](reaudit_harmonic_pa_images/harmonic-final-p1-r3.png) |
| HA04 | b_0_382 | short | ABSLAMC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r1.png) |
| HA04 | b_0_50 | long | ITCHOTELS / 1W | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r1.png) |
| HA04 | b_0_50 | short | ABDL / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r1.png) |
| HA05 | canonical | long | DCMSHRIRAM / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r2.png) |
| HA05 | canonical | short | BEL / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r2.png) |
| HA06 | ext_1_27 | long | ABSLAMC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r2.png) |
| HA06 | ext_1_27 | short | MMTC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r3.png) |
| HA06 | ext_1_618 | long | ABSLAMC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r3.png) |
| HA06 | ext_1_618 | short | DCMSHRIRAM / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p2-r3.png) |
| HA07 | canonical | long | DCMSHRIRAM / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r1.png) |
| HA07 | canonical | short | LTTS / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r1.png) |
| HA08 | canonical | long | ICICIBANK / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r1.png) |
| HA08 | canonical | short | ICICIBANK / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r2.png) |
| HA09 | canonical | long | ABSLAMC / 4H | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r2.png) |
| HA09 | canonical | short | ITCHOTELS / 4H | [View](reaudit_harmonic_pa_images/harmonic-final-p3-r2.png) |
| HA10 | canonical | long | ABSLAMC / 1D | [View](reaudit_harmonic_pa_images/harmonic-final-ha10-ratios.png) |
| HA10 | canonical | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/harmonic-final-ha10-ratios.png) |
| PA01 | bottom | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r1.png) |
| PA01 | top | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r1.png) |
| PA02 | cluster_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r1.png) |
| PA02 | cluster_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r2.png) |
| PA02 | first_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r2.png) |
| PA02 | first_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r2.png) |
| PA02 | nested_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r3.png) |
| PA02 | nested_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r3.png) |
| PA03 | bearish_close | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p1-r3.png) |
| PA03 | bullish_close | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r1.png) |
| PA04 | lower_shadow | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r1.png) |
| PA04 | lower_shadow_context | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r1.png) |
| PA04 | upper_shadow | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r2.png) |
| PA04 | upper_shadow_context | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r2.png) |
| PA05 | inside_nr4_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r2.png) |
| PA05 | inside_nr4_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r3.png) |
| PA05 | nr4_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r3.png) |
| PA05 | nr4_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p2-r3.png) |
| PA05 | nr7_break_down | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r1.png) |
| PA05 | nr7_break_up | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r1.png) |
| PA06 | bear | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r1.png) |
| PA06 | bull | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r2.png) |
| PA07 | falling | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r2.png) |
| PA07 | rising | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r2.png) |
| PA08 | bearish | short | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r3.png) |
| PA08 | bullish | long | AADHARHFC / 1H | [View](reaudit_harmonic_pa_images/price-action-p3-r3.png) |

## Checks

Harmonics: exact OHLC extrema, causal pivot availability, alternating side orientation, named labels, independently recomputed leg/time ratios, exact projected PRZ, trigger, and failure levels. Price action: exact formation box and recognition/confirmation separation; tweezer tolerance, inside/outside bars, pin shadow proportions, NR comparison windows, false-break return, full-range gaps versus opening gaps, and trigger close crossings.

## Observations

- **HA01:** A-B-C-D orientation and actual wick anchors are coherent; measured equality tolerance is visible in the ratio readout.
- **HA02:** Alternate AB=CD extension and side orientation match the frozen pivots; no synthetic terminal point.
- **HA03:** Gartley X-A-B-C-D labels match alternating wick pivots; setup is distinguished from confirmation.
- **HA04:** Both B-depth subtypes and directions retain their exact pivots, ratio-derived PRZ and terminal failure level.
- **HA05:** Alternate Bat terminal D extends beyond X; failure is now drawn beyond D, including BEL bearish sample.
- **HA06:** Both 1.27 and 1.618 Butterfly variants show the published terminal extension; projected zone is named explicitly.
- **HA07:** Crab legs and extrema are correct. Bullish sample has one-bar CD versus eleven-bar AB; time symmetry is descriptive, not gated. This visual peculiarity is not a mapping error or detector-validity certification.
- **HA08:** Deep Crab deep B and terminal extension map to actual alternating wick extrema.
- **HA09:** Shark uses O-X-A-B-C notation, with terminal C correctly drawn. Bullish example is a setup, not a triggered signal.
- **HA10:** 5-0 X-A-B-C-D geometry is coherent. AD/XA is a signed auxiliary retracement, not a gated 5-0 ratio; its omission from the readout was visually verified on both directions after the final deployment.
- **PA01:** Tweezer box covers only the equal-extreme pair, while later confirmation is separately marked.
- **PA02:** Mother-bar reference and inside-range recognition are separated from subsequent breakout confirmation; all six directional variants inspected.
- **PA03:** Outside bar spans both prior extremes and closes in the specified outer quarter; later confirmation is excluded from the shape box.
- **PA04:** Single pin-bar box, dominant wick orientation, and following confirmation are correct in plain and contextual variants. Context requirements verified against published metadata, not independently refitted trend.
- **PA05:** Range-comparison box covers the NR4/NR7 lookback; narrowest bar and later confirmation are separately identified.
- **PA06:** Mother range, false-break bar, return-inside failure close and later directional confirmation are all visible; labels refer to bars rather than arbitrary inferred pivot highs/lows.
- **PA07:** Full-range candle gap uses the actual prior/current wick edges in both directions; it is distinct from an opening gap.
- **PA08:** Opening-gap region is explicitly labelled filled intrabar; gapped open is positioned at the actual open, reversal close and later confirmation remain distinct.
