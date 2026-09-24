# Global Datafeeds trial — integration and verdict (21 Sep 2026)

**Integrated as provider `gdf`** (`market_data/gdf_provider.py`, dependency-free WebSocket client
`market_data/wsclient.py`), selectable by configuration exactly like Kite: `MARKET_DATA_PROVIDER=gdf` or
`get_provider("gdf")`. Key and endpoint live in `market_data/.env` (git-ignored; read into a private dict, never
logged or echoed). Passes the provider conformance suite live (RELIANCE, 18 + 21 Sep: 25 session bars per day,
delay-aware latest bar) and 8 offline tests. Shadow tool: `python -m market_data.gdf_shadow --session <date>`.

## Measured on the trial key (not assumed)
| | |
|---|---|
| Functions enabled | `GetHistory`, `GetSnapshot`, `GetExchangeSnapshot`, `SubscribeSnapshot` (+ metadata). **No live quote** — `GetLastQuote*`, `SubscribeRealtime`: "Function not enabled" |
| Bars | 1-min and 15-min only; **DAY and HOUR disabled** (daily = aggregate 15-min) |
| Limits | 3,600 calls/h · 100 NFO + 95 NSE + 5 index instruments · 900 s delay |
| Timestamps | epoch seconds, **bar START** (pinned: vendor "15:30" bar = Kite's 15:45 reading, OI identical) |
| Quirks | after-hours flat filler bars on NSE_IDX (20:45–21:15); 15:30-start post-close bar — both excluded from `candles()` |
| Expiry | **23 Sep 2026 23:59:59 IST** |
| **Licence** | **`AllowVMRunning: false`, `AllowServerOSRunning: false`** |

## Verdict
- **Data quality: good.** Spot and futures agree with Kite to hundredths of a percent; OI agrees closely. Option
  premium gaps (0.4–2.3% median) are the two sources sampling different instants, largest on NIFTY a day before expiry.
- **It cannot replace Kite on this plan**: 100 NFO symbols against ~27,300 contracts per reading, and no live quote.
- **The licence flags block the cloud move** as the key stands: a cloud server is a VM running a server OS.
  **Ask the vendor, in writing, before paying:** a production plan's symbol count, whether quotes/OI snapshots cover the
  whole NFO book, and whether VM / server-OS running is allowed.
- One more session of evidence is possible before expiry: run the shadow check after tomorrow's (22 Sep, expiry day)
  close.

---

# GDF shadow check vs Kite — 2026-09-21

36 instruments compared, 0 refused · 36 vendor calls.
Price: median of per-instrument median |Δ| **0.826%**. Open interest: **289/858** paired readings identical.

Pairing: the vendor bar STARTING at T−15m against Kite's snapshot at T. Kite's snapshot is a last trade taken ~20–60 s after the mark; the vendor's is the bar close — small price gaps are expected, OI should match closely.

### NIFTY — ATM 23400 at 09:30 (spot 23402.45), options 2026-09-22
- `OPTIDX_NIFTY_22SEP2026_CE_23400`: paired 26/26 readings · price |Δ| median 1.774% (max 5.48%, 7/26 within 0.5%) · OI exact 6/26, median |Δ| 0.189%
- `OPTIDX_NIFTY_22SEP2026_CE_23450`: paired 26/26 readings · price |Δ| median 1.844% (max 7.33%, 5/26 within 0.5%) · OI exact 5/26, median |Δ| 0.525%
- `OPTIDX_NIFTY_22SEP2026_CE_23500`: paired 26/26 readings · price |Δ| median 2.049% (max 8.5%, 5/26 within 0.5%) · OI exact 4/26, median |Δ| 0.333%
- `OPTIDX_NIFTY_22SEP2026_CE_23550`: paired 26/26 readings · price |Δ| median 2.107% (max 10.09%, 3/26 within 0.5%) · OI exact 5/26, median |Δ| 0.345%
- `OPTIDX_NIFTY_22SEP2026_CE_23600`: paired 26/26 readings · price |Δ| median 2.297% (max 11.42%, 4/26 within 0.5%) · OI exact 5/26, median |Δ| 0.21%
- `OPTIDX_NIFTY_22SEP2026_PE_23200`: paired 26/26 readings · price |Δ| median 1.666% (max 6.37%, 3/26 within 0.5%) · OI exact 4/26, median |Δ| 0.172%
- `OPTIDX_NIFTY_22SEP2026_PE_23250`: paired 26/26 readings · price |Δ| median 1.546% (max 7.32%, 6/26 within 0.5%) · OI exact 5/26, median |Δ| 0.121%
- `OPTIDX_NIFTY_22SEP2026_PE_23300`: paired 26/26 readings · price |Δ| median 2.135% (max 7.54%, 5/26 within 0.5%) · OI exact 5/26, median |Δ| 0.224%
- `OPTIDX_NIFTY_22SEP2026_PE_23350`: paired 26/26 readings · price |Δ| median 1.868% (max 7.05%, 5/26 within 0.5%) · OI exact 7/26, median |Δ| 0.264%
- `OPTIDX_NIFTY_22SEP2026_PE_23400`: paired 26/26 readings · price |Δ| median 1.534% (max 6.12%, 9/26 within 0.5%) · OI exact 6/26, median |Δ| 0.181%
- `FUTIDX_NIFTY_29SEP2026_XX_0`: paired 26/26 readings · price |Δ| median 0.012% (max 0.04%, 26/26 within 0.5%) · OI exact 4/26, median |Δ| 0.002%
- `NIFTY 50`: paired 26/26 readings · price |Δ| median 0.006% (max 0.03%, 26/26 within 0.5%)
### BANKNIFTY — ATM 56500 at 09:30 (spot 56477.25), options 2026-09-29
- `OPTIDX_BANKNIFTY_29SEP2026_CE_56500`: paired 26/26 readings · price |Δ| median 0.43% (max 4.27%, 14/26 within 0.5%) · OI exact 15/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_CE_56600`: paired 26/26 readings · price |Δ| median 0.586% (max 4.08%, 12/26 within 0.5%) · OI exact 17/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_CE_56700`: paired 26/26 readings · price |Δ| median 0.783% (max 5.11%, 7/26 within 0.5%) · OI exact 16/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_CE_56800`: paired 26/26 readings · price |Δ| median 0.551% (max 5.65%, 10/26 within 0.5%) · OI exact 17/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_CE_56900`: paired 26/26 readings · price |Δ| median 0.868% (max 5.2%, 11/26 within 0.5%) · OI exact 17/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_PE_56100`: paired 26/26 readings · price |Δ| median 0.678% (max 4.82%, 10/26 within 0.5%) · OI exact 13/26, median |Δ| 0.007%
- `OPTIDX_BANKNIFTY_29SEP2026_PE_56200`: paired 26/26 readings · price |Δ| median 0.751% (max 3.85%, 9/26 within 0.5%) · OI exact 14/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_PE_56300`: paired 26/26 readings · price |Δ| median 0.684% (max 4.78%, 9/26 within 0.5%) · OI exact 15/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_PE_56400`: paired 26/26 readings · price |Δ| median 0.505% (max 3.73%, 13/26 within 0.5%) · OI exact 14/26, median |Δ| 0.0%
- `OPTIDX_BANKNIFTY_29SEP2026_PE_56500`: paired 26/26 readings · price |Δ| median 0.528% (max 3.09%, 12/26 within 0.5%) · OI exact 16/26, median |Δ| 0.0%
- `FUTIDX_BANKNIFTY_29SEP2026_XX_0`: paired 26/26 readings · price |Δ| median 0.005% (max 0.05%, 26/26 within 0.5%) · OI exact 18/26, median |Δ| 0.0%
- `NIFTY BANK`: paired 26/26 readings · price |Δ| median 0.012% (max 0.06%, 26/26 within 0.5%)
### RELIANCE — ATM 1240 at 09:30 (spot 1242.2), options 2026-09-29
- `OPTSTK_RELIANCE_29SEP2026_CE_1250`: paired 26/26 readings · price |Δ| median 0.575% (max 8.47%, 12/26 within 0.5%) · OI exact 6/26, median |Δ| 0.066%
- `OPTSTK_RELIANCE_29SEP2026_CE_1270`: paired 26/26 readings · price |Δ| median 1.13% (max 11.5%, 11/26 within 0.5%) · OI exact 7/26, median |Δ| 0.053%
- `OPTSTK_RELIANCE_29SEP2026_CE_1240`: paired 26/26 readings · price |Δ| median 0.656% (max 7.87%, 8/26 within 0.5%) · OI exact 6/26, median |Δ| 0.076%
- `OPTSTK_RELIANCE_29SEP2026_CE_1260`: paired 26/26 readings · price |Δ| median 0.755% (max 9.49%, 6/26 within 0.5%) · OI exact 4/26, median |Δ| 0.088%
- `OPTSTK_RELIANCE_29SEP2026_CE_1280`: paired 26/26 readings · price |Δ| median 1.629% (max 14.1%, 11/26 within 0.5%) · OI exact 3/26, median |Δ| 0.05%
- `OPTSTK_RELIANCE_29SEP2026_PE_1210`: paired 26/26 readings · price |Δ| median 1.213% (max 12.28%, 8/26 within 0.5%) · OI exact 8/26, median |Δ| 0.147%
- `OPTSTK_RELIANCE_29SEP2026_PE_1230`: paired 26/26 readings · price |Δ| median 1.144% (max 10.85%, 8/26 within 0.5%) · OI exact 2/26, median |Δ| 0.094%
- `OPTSTK_RELIANCE_29SEP2026_PE_1200`: paired 26/26 readings · price |Δ| median 1.703% (max 10.34%, 11/26 within 0.5%) · OI exact 5/26, median |Δ| 0.057%
- `OPTSTK_RELIANCE_29SEP2026_PE_1220`: paired 26/26 readings · price |Δ| median 1.142% (max 9.41%, 8/26 within 0.5%) · OI exact 7/26, median |Δ| 0.185%
- `OPTSTK_RELIANCE_29SEP2026_PE_1240`: paired 26/26 readings · price |Δ| median 1.022% (max 9.6%, 11/26 within 0.5%) · OI exact 7/26, median |Δ| 0.088%
- `FUTSTK_RELIANCE_29SEP2026_XX_0`: paired 26/26 readings · price |Δ| median 0.016% (max 0.18%, 26/26 within 0.5%) · OI exact 6/26, median |Δ| 0.001%
- `RELIANCE`: paired 26/26 readings · price |Δ| median 0.024% (max 0.23%, 26/26 within 0.5%)
