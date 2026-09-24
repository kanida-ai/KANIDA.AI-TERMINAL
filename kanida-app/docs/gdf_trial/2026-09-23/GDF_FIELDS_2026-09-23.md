# GDF — every field the trial returns (2026-09-23 09:11:05 IST)

## GetLimitation — 1 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `GeneralParams` | dict | {"AllowedBandwidthPerHour": -1.0, "AllowedCallsPerHour": 360 | 0 | 0 |
| `AllowedExchanges` | list | [{"AllowedInstruments": 100, "DataDelay": 900, "ExchangeName | 0 | 0 |
| `AllowedFunctions` | list | [{"FunctionName": "GetExchangeSnapshot", "IsEnabled": true}, | 0 | 0 |
| `HistoryLimitation` | dict | {"TickEnabled": false, "DayEnabled": false, "WeekEnabled": f | 0 | 0 |
| `GetSnapshotLimitation` | dict | {"DayEnabled": false, "Hour_1Enabled": false, "Minute_1Enabl | 0 | 0 |
| `ExchangeSnapshotLimitation` | dict | {"DayEnabled": true, "Hour_1Enabled": false, "Minute_1Enable | 0 | 0 |
| `ExchangeSnapshotInstrumentTypeLimitation` | NoneType | None | 1 | 0 |
| `SubscribeSnapshotLimitation` | dict | {"Hour_1Enabled": false, "Minute_1Enabled": true, "Minute_2E | 0 | 0 |
| `MessageType` | str | LimitationResult | 0 | 0 |

## GetExchanges — 3 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `Value` | str | NFO | 0 | 0 |

## GetInstruments NSE_IDX — 142 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `TokenNumber` | str | None | 142 | 0 |
| `LowPriceRange` | float | None | 0 | 142 |
| `HighPriceRange` | float | None | 0 | 142 |
| `ISIN` | str | None | 142 | 0 |
| `Series` | str | None | 142 | 0 |
| `High52Week` | float | None | 0 | 142 |
| `Low52Week` | float | None | 0 | 142 |
| `IsCommonExchange` | bool | None | 0 | 142 |
| `Category` | str | None | 142 | 0 |
| `Identifier` | str | BHARATBOND-APR25 | 0 | 0 |
| `Name` | str | None | 142 | 0 |
| `Expiry` | str | None | 142 | 0 |
| `StrikePrice` | float | None | 0 | 142 |
| `Product` | str | None | 142 | 0 |
| `PriceQuotationUnit` | str | None | 142 | 0 |
| `OptionType` | str | None | 142 | 0 |
| `ProductMonth` | str | None | 142 | 0 |
| `UnderlyingAsset` | str | None | 142 | 0 |
| `UnderlyingAssetExpiry` | str | None | 142 | 0 |
| `IndexName` | str | BHARATBOND-APR25 | 0 | 0 |
| `TradeSymbol` | str | BHARATBOND-APR25 | 0 | 0 |
| `QuotationLot` | float | None | 0 | 142 |
| `Description` | str | NIFTY BHARAT BOND INDEX - APRIL 2030 | 3 | 0 |

## GetInstruments NFO option — 2026 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `TokenNumber` | str | 74571 | 211 | 0 |
| `LowPriceRange` | float | 0.05 | 0 | 211 |
| `HighPriceRange` | float | 20.05 | 0 | 211 |
| `ISIN` | str | None | 2026 | 0 |
| `Series` | str | None | 2026 | 0 |
| `High52Week` | float | None | 0 | 2026 |
| `Low52Week` | float | None | 0 | 2026 |
| `IsCommonExchange` | bool | None | 0 | 2026 |
| `Category` | str | None | 2026 | 0 |
| `Identifier` | str | OPTIDX_NIFTY_29SEP2026_CE_28850 | 0 | 0 |
| `Name` | str | OPTIDX | 0 | 0 |
| `Expiry` | str | 29Sep2026 | 0 | 0 |
| `StrikePrice` | float | 28850.0 | 0 | 0 |
| `Product` | str | NIFTY | 0 | 0 |
| `PriceQuotationUnit` | str | None | 2026 | 0 |
| `OptionType` | str | CE | 0 | 0 |
| `ProductMonth` | str | 29Sep2026 | 0 | 0 |
| `UnderlyingAsset` | str | None | 2026 | 0 |
| `UnderlyingAssetExpiry` | str | None | 2026 | 0 |
| `IndexName` | str | None | 2026 | 0 |
| `TradeSymbol` | str | NIFTY29SEP2628850CE | 0 | 0 |
| `QuotationLot` | float | 65.0 | 0 | 0 |
| `Description` | str | None | 2026 | 0 |

## GetInstruments NSE equity — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 1m index — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 1m equity — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 1m future — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 1m option — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 15m option — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetSnapshot 1m NFO — 3 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | FUTIDX_NIFTY_29SEP2026_XX_0 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | None | 0 | 3 |
| `TradedQty` | int | None | 0 | 3 |
| `OpenInterest` | int | None | 0 | 3 |
| `Open` | float | None | 0 | 3 |
| `High` | float | None | 0 | 3 |
| `Low` | float | None | 0 | 3 |
| `Close` | float | None | 0 | 3 |
| `TokenNumber` | NoneType | None | 3 | 0 |

## GetSnapshot 15m NFO — 2 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | FUTIDX_NIFTY_29SEP2026_XX_0 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | None | 0 | 2 |
| `TradedQty` | int | None | 0 | 2 |
| `OpenInterest` | int | None | 0 | 2 |
| `Open` | float | None | 0 | 2 |
| `High` | float | None | 0 | 2 |
| `Low` | float | None | 0 | 2 |
| `Close` | float | None | 0 | 2 |
| `TokenNumber` | NoneType | None | 2 | 0 |

## GetSnapshot 1m NSE — 1 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | RELIANCE | 0 | 0 |
| `Exchange` | str | NSE | 0 | 0 |
| `LastTradeTime` | int | None | 0 | 1 |
| `TradedQty` | int | None | 0 | 1 |
| `OpenInterest` | int | None | 0 | 1 |
| `Open` | float | None | 0 | 1 |
| `High` | float | None | 0 | 1 |
| `Low` | float | None | 0 | 1 |
| `Close` | float | None | 0 | 1 |
| `TokenNumber` | NoneType | None | 1 | 0 |

## GetSnapshot 1m NSE_IDX — 1 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | NIFTY 50 | 0 | 0 |
| `Exchange` | str | NSE_IDX | 0 | 0 |
| `LastTradeTime` | int | None | 0 | 1 |
| `TradedQty` | int | None | 0 | 1 |
| `OpenInterest` | int | None | 0 | 1 |
| `Open` | float | None | 0 | 1 |
| `High` | float | None | 0 | 1 |
| `Low` | float | None | 0 | 1 |
| `Close` | float | None | 0 | 1 |
| `TokenNumber` | NoneType | None | 1 | 0 |

## GetExchangeSnapshot NFO 1m — 2 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | FUTSTK_181NSETEST_27NOV2036_XX_0 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | 1790120880 | 0 | 0 |
| `TradedQty` | int | 950 | 0 | 0 |
| `OpenInterest` | int | 697350 | 0 | 0 |
| `Open` | float | 200.0 | 0 | 0 |
| `High` | float | 200.0 | 0 | 0 |
| `Low` | float | 200.0 | 0 | 0 |
| `Close` | float | 200.0 | 0 | 0 |
| `TokenNumber` | str | 48577 | 0 | 0 |
| `_group` | int | 1790120880 | 0 | 0 |

## GetLastQuote NFO — refused

`gdf: Function not enabled.`

## GetExpiryDates NFO — 19 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `Value` | str | 29SEP2026 | 0 | 0 |

## GetHistory DAY — refused

`gdf: Selected periodicity or period disabled.`

## GetHistory TICK — refused

`gdf: Selected periodicity or period disabled.`

