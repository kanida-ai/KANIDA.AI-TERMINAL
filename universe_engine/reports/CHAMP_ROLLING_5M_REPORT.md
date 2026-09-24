# Champ Rolling 5-Mo Walk-Forward — momentum_rs

- Universe: in_nifty200, top-N concentration = 5
- Capital: ₹1,000,000  ·  Cost: 30.0 bps RT  ·  Slip: 5.0 bps
- Window envelope: 2021-01-01 → 2026-04-30
- N windows: 60

## How to read this
Each row is one rolling 5-month window. **No leakage**: signal scores are computed strictly from bars before window_start; positions are managed with same-day data only. Forced exits at window_end on the last close.

## Headline — championship-threshold hit rate

| Threshold | Hit / total |
|---|---|
| ≥ +50%   | 6/60 |
| ≥ +100%  | 0/60 |
| ≥ +200%  | 0/60 |
| ≥ +300%  | 0/60 |
| Median window return | +10.6% |

## Per-window results

| Window | Ret% | MDD% | Trades | Closed | Top trades | Worst trade |
|---|---|---|---|---|---|---|
| 2021-01-01→2021-05-31 | +58.2% | -10.3% | 15 | 15 | ADANIENT+134%, SAIL+66%, DEEPAKNTR+63% | NAUKRI-12%/initial_stop |
| 2021-02-01→2021-06-30 | +71.2% | -7.7% | 10 | 10 | ADANIENT+154%, PERSISTENT+81%, DIXON+36% | SAIL-8%/initial_stop |
| 2021-03-01→2021-07-31 | +52.7% | -8.4% | 9 | 9 | SAIL+85%, ADANIENT+57%, JSWSTEEL+50% | JINDALSTEL-10%/initial_stop |
| 2021-04-01→2021-08-31 | +34.9% | -8.9% | 8 | 8 | PERSISTENT+61%, LAURUSLABS+53%, JSWSTEEL+29% | VEDL-8%/initial_stop |
| 2021-05-01→2021-09-30 | +21.5% | -8.4% | 13 | 13 | PERSISTENT+50%, COFORGE+23%, LAURUSLABS+19% | JINDALSTEL-9%/initial_stop |
| 2021-06-01→2021-10-31 | +11.0% | -13.9% | 10 | 10 | COFORGE+29%, ASTRAL+12%, IRCTC+5% | IEX-8%/initial_stop |
| 2021-07-01→2021-11-30 | -6.9% | -19.3% | 24 | 24 | COFORGE+24%, PERSISTENT+3%, APOLLOHOSP+3% | VEDL-14%/initial_stop |
| 2021-08-01→2021-12-31 | -2.0% | -18.1% | 27 | 27 | PERSISTENT+41%, TECHM+12%, SRF+11% | IRCTC-12%/initial_stop |
| 2021-09-01→2022-01-31 | +1.7% | -19.5% | 19 | 19 | PERSISTENT+27%, IEX+21%, COFORGE+3% | CHOLAFIN-10%/initial_stop |
| 2021-10-01→2022-02-28 | -10.1% | -24.4% | 23 | 23 | HINDALCO+11%, TITAN+4%, MPHASIS+3% | DEEPAKNTR-12%/initial_stop |
| 2021-11-01→2022-03-31 | -17.2% | -22.1% | 22 | 22 | HINDALCO+3%, LTTS+2%, PERSISTENT+2% | APOLLOHOSP-14%/initial_stop |
| 2021-12-01→2022-04-30 | -5.6% | -17.9% | 28 | 28 | INDHOTEL+23%, TITAN+3%, LAURUSLABS+3% | CANBK-11%/initial_stop |
| 2022-01-01→2022-05-31 | -15.2% | -15.2% | 17 | 17 | HINDALCO+3%, ONGC+0%, SRF+-1% | MPHASIS-10%/initial_stop |
| 2022-02-01→2022-06-30 | -16.9% | -21.8% | 25 | 25 | ITC+3%, VEDL+2%, TITAN+2% | ADANIENT-12%/initial_stop |
| 2022-03-01→2022-07-31 | -15.2% | -16.9% | 24 | 24 | VEDL+3%, COALINDIA+2%, INDHOTEL+2% | PAGEIND-9%/initial_stop |
| 2022-04-01→2022-08-31 | -15.5% | -17.1% | 19 | 19 | INDHOTEL+2%, ADANIENT+2%, ITC+-0% | LTTS-10%/initial_stop |
| 2022-05-01→2022-09-30 | +11.3% | -12.7% | 17 | 17 | HAL+42%, CANBK+26%, TRENT+22% | ADANIENT-10%/initial_stop |
| 2022-06-01→2022-10-31 | +39.5% | -8.1% | 9 | 9 | BANKBARODA+59%, ADANIENT+58%, INDHOTEL+53% | HAL-9%/initial_stop |
| 2022-07-01→2022-11-30 | +35.1% | -7.5% | 5 | 5 | ADANIENT+67%, INDHOTEL+39%, TVSMOTOR+22% | M&M+17%/window_end |
| 2022-08-01→2022-12-31 | +19.1% | -11.1% | 13 | 13 | BANKBARODA+52%, HAL+21%, ABCAPITAL+7% | ADANIENT-12%/initial_stop |
| 2022-09-01→2023-01-31 | -6.1% | -13.6% | 28 | 28 | M&M+4%, FEDERALBNK+3%, CANBK+3% | PFC-9%/initial_stop |
| 2022-10-01→2023-02-28 | -5.5% | -14.3% | 25 | 25 | SIEMENS+8%, ADANIENT+2%, PFC+2% | INDHOTEL-8%/initial_stop |
| 2022-11-01→2023-03-31 | -5.4% | -14.4% | 25 | 25 | SIEMENS+8%, ITC+7%, M&M+3% | IRFC-14%/initial_stop |
| 2022-12-01→2023-04-30 | -7.6% | -12.8% | 29 | 29 | SIEMENS+12%, IDFCFIRSTB+4%, ITC+3% | CANBK-11%/initial_stop |
| 2023-01-01→2023-05-31 | -1.5% | -9.6% | 24 | 24 | ITC+22%, IDFCFIRSTB+14%, CANFINHOME+4% | CANBK-9%/initial_stop |
| 2023-02-01→2023-06-30 | +16.5% | -8.6% | 13 | 13 | ABCAPITAL+27%, ITC+23%, IDFCFIRSTB+19% | BHEL-8%/initial_stop |
| 2023-03-01→2023-07-31 | +14.3% | -7.7% | 17 | 17 | IDFCFIRSTB+30%, BANKBARODA+23%, JINDALSTEL+14% | HAL-8%/initial_stop |
| 2023-04-01→2023-08-31 | +14.8% | -6.3% | 7 | 7 | PFC+26%, APOLLOTYRE+16%, ITC+14% | SIEMENS-5%/breakeven_stop |
| 2023-05-01→2023-09-30 | +35.2% | -6.4% | 14 | 14 | PFC+78%, IDFCFIRSTB+43%, BHEL+19% | CHOLAFIN-7%/initial_stop |
| 2023-06-01→2023-10-31 | +32.1% | -9.4% | 11 | 11 | IRFC+77%, RECLTD+38%, PFC+33% | GLENMARK-7%/initial_stop |
| 2023-07-01→2023-11-30 | +57.1% | -8.8% | 9 | 9 | RECLTD+109%, PFC+83%, IRFC+31% | GLENMARK-7%/initial_stop |
| 2023-08-01→2023-12-31 | +55.3% | -9.1% | 11 | 11 | RECLTD+102%, IRFC+80%, NMDC+34% | BHEL-8%/initial_stop |
| 2023-09-01→2024-01-31 | +67.4% | -12.3% | 15 | 15 | IRFC+189%, NMDC+39%, MCX+38% | BHEL-10%/initial_stop |
| 2023-10-01→2024-02-29 | +37.7% | -6.8% | 11 | 11 | DLF+51%, CANBK+48%, NMDC+45% | RECLTD-9%/initial_stop |
| 2023-11-01→2024-03-31 | +41.3% | -10.0% | 5 | 5 | BAJAJ-AUTO+59%, ONGC+47%, MCX+33% | PERSISTENT+3%/time_stop |
| 2023-12-01→2024-04-30 | +40.9% | -13.5% | 8 | 8 | BHEL+56%, IRFC+54%, RECLTD+30% | MCX-1%/time_stop |
| 2024-01-01→2024-05-31 | +36.0% | -15.5% | 11 | 11 | IRFC+69%, DIXON+36%, GLENMARK+20% | MCX-6%/breakeven_stop |
| 2024-02-01→2024-06-30 | +40.7% | -8.6% | 11 | 11 | DIXON+73%, HAL+64%, GLENMARK+34% | RECLTD-8%/initial_stop |
| 2024-03-01→2024-07-31 | +34.4% | -15.8% | 13 | 13 | DIXON+63%, HAL+51%, BHEL+34% | PFC-9%/initial_stop |
| 2024-04-01→2024-08-31 | +23.4% | -15.1% | 11 | 11 | DIXON+60%, PFC+22%, HAL+18% | MCX-8%/initial_stop |
| 2024-05-01→2024-09-30 | +9.5% | -14.5% | 12 | 12 | DIXON+57%, BEL+9%, HINDPETRO+4% | BHEL-18%/initial_stop |
| 2024-06-01→2024-10-31 | -6.1% | -10.6% | 17 | 17 | NTPC+15%, MCX+11%, TRENT+3% | BEL-20%/initial_stop |
| 2024-07-01→2024-11-30 | +10.6% | -10.5% | 17 | 17 | MCX+33%, ZOMATO+31%, BEL+6% | DIXON-10%/initial_stop |
| 2024-08-01→2024-12-31 | +6.1% | -13.5% | 21 | 21 | MCX+42%, COFORGE+3%, DIXON+3% | SIEMENS-9%/initial_stop |
| 2024-09-01→2025-01-31 | -6.4% | -21.4% | 21 | 21 | DIXON+12%, MUTHOOTFIN+8%, TORNTPOWER+3% | INDHOTEL-12%/initial_stop |
| 2024-10-01→2025-02-28 | -16.2% | -18.0% | 22 | 22 | BEL+2%, MUTHOOTFIN+2%, OFSS+1% | BAJAJ-AUTO-17%/initial_stop |
| 2024-11-01→2025-03-31 | -16.1% | -25.4% | 26 | 26 | CHOLAFIN+3%, BEL+3%, DIXON+3% | LAURUSLABS-10%/initial_stop |
| 2024-12-01→2025-04-30 | -16.4% | -19.4% | 18 | 18 | DIXON+3%, MUTHOOTFIN+2%, OFSS+1% | TRENT-12%/initial_stop |
| 2025-01-01→2025-05-31 | -15.2% | -15.8% | 18 | 18 | BHARTIARTL+4%, MUTHOOTFIN+2%, WIPRO+-1% | TRENT-12%/initial_stop |
| 2025-02-01→2025-06-30 | -17.6% | -19.3% | 20 | 20 | CHOLAFIN+2%, UPL+1%, GLENMARK+1% | TRENT-17%/initial_stop |
| 2025-03-01→2025-07-31 | +10.7% | -13.4% | 16 | 16 | MCX+44%, BHARTIARTL+15%, COFORGE+2% | VEDL-12%/initial_stop |
| 2025-04-01→2025-08-31 | +6.7% | -12.8% | 18 | 18 | MCX+39%, TVSMOTOR+7%, UPL+5% | VEDL-12%/initial_stop |
| 2025-05-01→2025-09-30 | +3.2% | -10.8% | 22 | 22 | EICHERMOT+18%, MUTHOOTFIN+10%, BAJFINANCE+4% | MCX-10%/initial_stop |
| 2025-06-01→2025-10-31 | +12.3% | -9.7% | 19 | 19 | MCX+31%, EICHERMOT+9%, BEL+3% | SRF-5%/window_end |
| 2025-07-01→2025-11-30 | +5.6% | -6.7% | 23 | 23 | LAURUSLABS+31%, ASHOKLEY+9%, AUBANK+7% | COFORGE-13%/initial_stop |
| 2025-08-01→2025-12-31 | +17.1% | -2.8% | 15 | 15 | MUTHOOTFIN+29%, MCX+16%, EICHERMOT+10% | GLENMARK-7%/initial_stop |
| 2025-09-01→2026-01-31 | +20.4% | -4.2% | 13 | 13 | MCX+37%, MUTHOOTFIN+35%, EICHERMOT+7% | SRF-8%/initial_stop |
| 2025-10-01→2026-02-28 | +11.2% | -7.8% | 17 | 17 | MCX+25%, TATASTEEL+10%, FEDERALBNK+6% | MANAPPURAM-8%/initial_stop |
| 2025-11-01→2026-03-31 | -4.4% | -16.7% | 23 | 23 | ABCAPITAL+3%, TVSMOTOR+3%, TITAN+2% | UPL-10%/initial_stop |
| 2025-12-01→2026-04-30 | -7.3% | -18.5% | 30 | 25 | CANFINHOME+3%, MCX+3%, AUBANK+3% | UPL-10%/initial_stop |

## Best window — full trade list

**2021-02-01 → 2021-06-30**  ·  Return: +71.2%  ·  MDD: -7.7%

| Symbol | Open | Close | Avg Entry | Exit | Return | Adds | Reason |
|---|---|---|---|---|---|---|---|
| ADANIENT | 2021-02-01 | 2021-06-30 | ₹592.46 | ₹1503.15 | +153.7% | 2 | window_end |
| PERSISTENT | 2021-02-01 | 2021-06-30 | ₹781.35 | ₹1416.07 | +81.2% | 2 | window_end |
| DIXON | 2021-02-01 | 2021-06-30 | ₹3247.98 | ₹4409.23 | +35.8% | 2 | window_end |
| LAURUSLABS | 2021-06-23 | 2021-06-30 | ₹606.5 | ₹678.62 | +11.9% | 0 | window_end |
| HAVELLS | 2021-02-01 | 2021-03-05 | ₹1051.59 | ₹1082.35 | +2.9% | 1 | time_stop |
| TATASTEEL | 2021-05-24 | 2021-06-23 | ₹94.05 | ₹96.78 | +2.9% | 0 | time_stop |
| JINDALSTEL | 2021-04-12 | 2021-05-24 | ₹383.77 | ₹392.11 | +2.2% | 0 | time_stop |
| DEEPAKNTR | 2021-03-05 | 2021-04-05 | ₹1555.72 | ₹1588.9 | +2.1% | 0 | time_stop |
| LTIM | 2021-02-01 | 2021-02-23 | ₹3821.35 | ₹3536.35 | -7.5% | 1 | initial_stop |
| SAIL | 2021-04-05 | 2021-04-12 | ₹79.0 | ₹73.01 | -7.6% | 1 | initial_stop |

## Failure modes (windows where return ≤ 0%)

23 of 60 windows ended flat or down.

| Window | Ret% | MDD% | Worst trade |
|---|---|---|---|
| 2021-07-01→2021-11-30 | -6.9% | -19.3% | VEDL-14%/initial_stop |
| 2021-08-01→2021-12-31 | -2.0% | -18.1% | IRCTC-12%/initial_stop |
| 2021-10-01→2022-02-28 | -10.1% | -24.4% | DEEPAKNTR-12%/initial_stop |
| 2021-11-01→2022-03-31 | -17.2% | -22.1% | APOLLOHOSP-14%/initial_stop |
| 2021-12-01→2022-04-30 | -5.6% | -17.9% | CANBK-11%/initial_stop |
| 2022-01-01→2022-05-31 | -15.2% | -15.2% | MPHASIS-10%/initial_stop |
| 2022-02-01→2022-06-30 | -16.9% | -21.8% | ADANIENT-12%/initial_stop |
| 2022-03-01→2022-07-31 | -15.2% | -16.9% | PAGEIND-9%/initial_stop |
| 2022-04-01→2022-08-31 | -15.5% | -17.1% | LTTS-10%/initial_stop |
| 2022-09-01→2023-01-31 | -6.1% | -13.6% | PFC-9%/initial_stop |
| 2022-10-01→2023-02-28 | -5.5% | -14.3% | INDHOTEL-8%/initial_stop |
| 2022-11-01→2023-03-31 | -5.4% | -14.4% | IRFC-14%/initial_stop |
| 2022-12-01→2023-04-30 | -7.6% | -12.8% | CANBK-11%/initial_stop |
| 2023-01-01→2023-05-31 | -1.5% | -9.6% | CANBK-9%/initial_stop |
| 2024-06-01→2024-10-31 | -6.1% | -10.6% | BEL-20%/initial_stop |