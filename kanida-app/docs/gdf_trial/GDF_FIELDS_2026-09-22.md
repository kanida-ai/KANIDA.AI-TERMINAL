# GDF — every field the trial returns (2026-09-22 09:48:32 IST)

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

## GetInstruments NFO option — 2051 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `TokenNumber` | str | 57228 | 0 | 0 |
| `LowPriceRange` | float | 0.05 | 0 | 0 |
| `HighPriceRange` | float | 20.9 | 0 | 0 |
| `ISIN` | str | None | 2051 | 0 |
| `Series` | str | None | 2051 | 0 |
| `High52Week` | float | None | 0 | 2051 |
| `Low52Week` | float | None | 0 | 2051 |
| `IsCommonExchange` | bool | None | 0 | 2051 |
| `Category` | str | None | 2051 | 0 |
| `Identifier` | str | OPTIDX_NIFTY_22SEP2026_CE_24050 | 0 | 0 |
| `Name` | str | OPTIDX | 0 | 0 |
| `Expiry` | str | 22Sep2026 | 0 | 0 |
| `StrikePrice` | float | 24050.0 | 0 | 0 |
| `Product` | str | NIFTY | 0 | 0 |
| `PriceQuotationUnit` | str | None | 2051 | 0 |
| `OptionType` | str | CE | 0 | 0 |
| `ProductMonth` | str | 22Sep2026 | 0 | 0 |
| `UnderlyingAsset` | str | None | 2051 | 0 |
| `UnderlyingAssetExpiry` | str | None | 2051 | 0 |
| `IndexName` | str | None | 2051 | 0 |
| `TradeSymbol` | str | NIFTY22SEP2624050CE | 0 | 0 |
| `QuotationLot` | float | 65.0 | 0 | 0 |
| `Description` | str | None | 2051 | 0 |

## GetInstruments NSE equity — 0 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|

## GetHistory 1m index — 26 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `QuotationLot` | int | None | 0 | 26 |
| `TradedQty` | int | None | 0 | 26 |
| `OpenInterest` | int | None | 0 | 26 |
| `Open` | float | 23435.35 | 0 | 0 |
| `High` | float | 23439.7 | 0 | 0 |
| `Low` | float | 23434.5 | 0 | 0 |
| `Close` | float | 23436.7 | 0 | 0 |

## GetHistory 1m equity — 20 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `QuotationLot` | int | 1 | 0 | 0 |
| `TradedQty` | int | 17917 | 0 | 0 |
| `OpenInterest` | int | None | 0 | 20 |
| `Open` | float | 1247.8 | 0 | 0 |
| `High` | float | 1247.8 | 0 | 0 |
| `Low` | float | 1247.5 | 0 | 0 |
| `Close` | float | 1247.5 | 0 | 0 |

## GetHistory 1m future — 20 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `QuotationLot` | int | 65 | 0 | 0 |
| `TradedQty` | int | 3380 | 0 | 0 |
| `OpenInterest` | int | 16903380 | 0 | 0 |
| `Open` | float | 23444.0 | 0 | 0 |
| `High` | float | 23453.8 | 0 | 0 |
| `Low` | float | 23440.0 | 0 | 0 |
| `Close` | float | 23450.0 | 0 | 0 |

## GetHistory 1m option — 20 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `QuotationLot` | int | 65 | 0 | 0 |
| `TradedQty` | int | 1174615 | 0 | 0 |
| `OpenInterest` | int | 16568305 | 0 | 0 |
| `Open` | float | 69.1 | 0 | 0 |
| `High` | float | 71.55 | 0 | 0 |
| `Low` | float | 69.1 | 0 | 0 |
| `Close` | float | 71.15 | 0 | 0 |

## GetHistory 15m option — 2 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `LastTradeTime` | int | 1790049600 | 0 | 0 |
| `QuotationLot` | int | 65 | 0 | 0 |
| `TradedQty` | int | 33779135 | 0 | 0 |
| `OpenInterest` | int | 16523390 | 0 | 0 |
| `Open` | float | 78.1 | 0 | 0 |
| `High` | float | 84.5 | 0 | 0 |
| `Low` | float | 68.25 | 0 | 0 |
| `Close` | float | 82.5 | 0 | 0 |

## GetSnapshot 1m NFO — 3 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | OPTIDX_NIFTY_22SEP2026_CE_23400 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `TradedQty` | int | 1174615 | 0 | 0 |
| `OpenInterest` | int | 16568305 | 0 | 0 |
| `Open` | float | 69.1 | 0 | 0 |
| `High` | float | 71.55 | 0 | 0 |
| `Low` | float | 69.1 | 0 | 0 |
| `Close` | float | 71.15 | 0 | 0 |
| `TokenNumber` | NoneType | None | 3 | 0 |

## GetSnapshot 15m NFO — 2 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | FUTIDX_NIFTY_29SEP2026_XX_0 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | 1790049600 | 0 | 0 |
| `TradedQty` | int | 121420 | 0 | 0 |
| `OpenInterest` | int | 16905135 | 0 | 0 |
| `Open` | float | 23458.5 | 0 | 0 |
| `High` | float | 23470.0 | 0 | 0 |
| `Low` | float | 23440.0 | 0 | 0 |
| `Close` | float | 23469.9 | 0 | 0 |
| `TokenNumber` | NoneType | None | 2 | 0 |

## GetSnapshot 1m NSE — 1 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | RELIANCE | 0 | 0 |
| `Exchange` | str | NSE | 0 | 0 |
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `TradedQty` | int | 17917 | 0 | 0 |
| `OpenInterest` | int | None | 0 | 1 |
| `Open` | float | 1247.8 | 0 | 0 |
| `High` | float | 1247.8 | 0 | 0 |
| `Low` | float | 1247.5 | 0 | 0 |
| `Close` | float | 1247.5 | 0 | 0 |
| `TokenNumber` | NoneType | None | 1 | 0 |

## GetSnapshot 1m NSE_IDX — 1 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | NIFTY 50 | 0 | 0 |
| `Exchange` | str | NSE_IDX | 0 | 0 |
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `TradedQty` | int | None | 0 | 1 |
| `OpenInterest` | int | None | 0 | 1 |
| `Open` | float | 23435.35 | 0 | 0 |
| `High` | float | 23439.7 | 0 | 0 |
| `Low` | float | 23434.5 | 0 | 0 |
| `Close` | float | 23436.7 | 0 | 0 |
| `TokenNumber` | NoneType | None | 1 | 0 |

## GetExchangeSnapshot NFO 1m — 4015 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `InstrumentIdentifier` | str | OPTSTK_M&M_29SEP2026_CE_3000 | 0 | 0 |
| `Exchange` | str | NFO | 0 | 0 |
| `LastTradeTime` | int | 1790049840 | 0 | 0 |
| `TradedQty` | int | 600 | 0 | 0 |
| `OpenInterest` | int | 84000 | 0 | 3 |
| `Open` | float | 83.1 | 0 | 0 |
| `High` | float | 83.1 | 0 | 0 |
| `Low` | float | 81.6 | 0 | 0 |
| `Close` | float | 81.6 | 0 | 0 |
| `TokenNumber` | str | 124584 | 0 | 0 |
| `_group` | int | 1790049840 | 0 | 0 |

## GetLastQuote NFO — refused

`gdf: Function not enabled.`

## GetExpiryDates NFO — 19 rows

| Field | Type | Sample | Empty | Zero |
|---|---|---|---|---|
| `Value` | str | 22SEP2026 | 0 | 0 |

## GetHistory DAY — refused

`gdf: Selected periodicity or period disabled.`

## GetHistory TICK — refused

`gdf: Selected periodicity or period disabled.`

