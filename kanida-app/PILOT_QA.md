# Private pilot validation — 12 September 2026

## Automated checks

- 29 backend tests pass with isolated temporary databases and mocked provider responses.
- Coverage includes invitation-only registration, session revocation, CSRF/origin checks, account isolation, subscription access, native bearer sessions, device PKCE, recovery, explicit included access, verified Google claims, encrypted Kite tokens/account binding, subscription recovery, forged/duplicate webhooks, order request idempotency, concurrent reservations, partial fills/rejections/exits, cash release and live execution gates.
- TypeScript passes. Expo web, iOS and Android JavaScript/Hermes exports pass.
- Local and LAN health/config respond; anonymous research returns HTTP 401. Google and billing are unconfigured, Kite credentials are present, and live execution is false.

## Browser acceptance

Tested in Chrome using a separate local QA database and a fictional account:

- Landing → sign in → actual stored Discover charts.
- LTTS 1H Cup & Handle → Add to watch → Prepare AutoTrade → review → save plan.
- AutoTrade → select partial-fill synthetic scenario → consent → queue → partial → open → close.
- Virtual capital reserves ₹7,189 for the sample two-share plan; it is released on closure and the outcome is recorded separately from stock history.
- Activity shows the full order sequence and timestamps.
- Account and billing show honest unconfigured provider states and owner controls.
- Mobile content width measured at 390 CSS pixels with no document horizontal overflow. Bottom navigation remains accessible. Desktop and phone layouts were inspected; native touch/keyboard/safe-area behavior still needs testing on the actual iPhone/Android device.
- No browser console warnings or errors were observed in the inspected build.

No real payment, broker login or order was executed. Owner registration is deliberately left for the owner through the private invitation file.
