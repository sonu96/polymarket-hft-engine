# Phase 3 backtest — historical data inventory

## Summary
**Can we run a 90-day backtest?** Yes, with a real intraday fill simulator. All five sources work and are free. The only friction is that Polymarket city-bucket weather markets only started running daily in **Jan 2025**, and the CLOB `prices-history` endpoint requires explicit `startTs`/`endTs` (not `interval=1d`) to return useful data.

## Source 1: Polymarket closed events (Gamma)
- **URL tested:** `https://gamma-api.polymarket.com/events?slug=highest-temperature-in-nyc-on-march-15-2026`
- **Response shape:** array of events; each has `markets[]` with `clobTokenIds` (JSON-encoded `[YES_id, NO_id]`), `outcomes`, `outcomePrices` (`["1","0"]` = YES won, `["0","1"]` = NO won), `conditionId`, `negRiskMarketID`, `endDate`, `umaResolutionStatus`. Resolved outcome IS in the event response — no separate endpoint needed.
- **Date range:** Daily city-bucket weather markets (`highest-temperature-in-*`) started **Jan 2025** and have been running ~daily across multiple cities since. Earliest weather event of any kind: `how-hot-will-april-2024-be` (May 2024) but those are old monthly-anomaly markets, not city-buckets.
- **Pagination:** `limit` caps at 500, use `offset=`. Total weather events >1000.
- **Gotchas:** `outcomePrices` is a JSON string, not a list. `clobTokenIds` is a JSON string too. Tag-filtered `order=end_date&ascending=false` returns interleaved order — sort client-side by `endDate`.

## Source 2: Polymarket CLOB price history
- **URL tested:** `https://clob.polymarket.com/prices-history?market=1472199860682289398941707323219756648626024204458895625831792653471681784244&startTs=1773100000&endTs=1773700000&fidelity=10`
- **Intraday data available?** **YES.** That request returned **391 ticks** for one bucket over ~3 days at 10-minute fidelity. `interval=max&fidelity=60` only returned 21 ticks for the same token because `interval=max` clamps to a window that doesn't capture pre-resolution trading. **Use `startTs`/`endTs` (unix seconds) + `fidelity` (minutes), never `interval=1d`/`1h` (returns empty for short-lived buckets).**
- **Granularities supported:** `fidelity` is in minutes; minimum 1 (`1m` interval) is 10 min. Tested values: 10, 60, 600. `fidelity=10` is the practical floor.
- **Critical caveat:** This is **midpoint/last-trade time series, NOT order book best-bid/best-ask**. To simulate whether our resting NO ask at `μ−5%` would have crossed, we need to assume that any tick observed at `p ≥ our_ask` represents a taker that would have lifted our ask. This is a reasonable approximation for thinly-traded buckets where every print is meaningful, but it's not a real book replay. **No historical L2 book endpoint exists** — the only way to get historical book state is the WS user channel or Predexon `/orderbook` snapshots that we'd have had to record live.
- **Date range:** Token-lifetime; for daily city-bucket markets that means roughly creation-time (typically T-2 days) through resolution.
- **First/last tick example (NYC Mar 15 winning bucket):** first `t=1773398433` (2026-03-13 21:20 UTC), last `t=1773645630` (2026-03-16 17:00 UTC), `p` ranging 0.31 → 0.9995.

## Source 3: Open-Meteo historical forecast (forecast-as-of-date, not reanalysis)
- **URL tested:** `https://historical-forecast-api.open-meteo.com/v1/forecast?latitude=40.77&longitude=-73.87&daily=temperature_2m_max&temperature_unit=fahrenheit&start_date=2026-03-13&end_date=2026-03-15&models=gfs_seamless,ecmwf_ifs025`
- **Response (first 200 chars):** `{"latitude":40.76809,"longitude":-73.862785,"generationtime_ms":3.05,"utc_offset_seconds":0,"timezone":"GMT","elevation":11.0,"daily_units":{"time":"iso8601","temperature_2m_max_gfs_seamless":"°F","temperature_2m_max_ecmwf_ifs025":"°F"},"daily":{"time":["2026-03-13","2026-03-14","2026-03-15"],"temperature_2m_max_gfs_seamless":[42.2,51.7,45.2],"temperature_2m_max_ecmwf_ifs025":[38.8,50.1,42.4]}}`
- **Does the endpoint return the forecast issued on a past date?** **Yes — confirmed.** `historical-forecast-api.open-meteo.com` archives the actual model run from each past date (NOT reanalysis). Per Open-Meteo docs the GFS/HRRR/ECMWF models go back to **2021** (some 2022). Free, no auth, generous rate limit.
- **Critical:** to backtest the **1-day-ahead** forecast for target date `T`, query with `start_date=T&end_date=T` and use the model run that would have been available on `T-1` — Open-Meteo's `historical-forecast-api` returns model output where each row is the forecast made closest to its valid time, which is what we want. To stress-test horizon, query `start_date=T-2` and look at how the prediction for `T` evolved across days. **For the backtest mock the same `models=` cascade as live (`gfs_hrrr,gfs_seamless,ecmwf_ifs025`).**

## Source 4: Observed TMAX settlement truth
- **Preferred source:** **Open-Meteo `/v1/archive`** (ERA5 reanalysis). Free, no auth, covers all 11 stations including `lucknow` (VILK), goes back decades, identical lat/lon API as the forecast endpoint. Iowa Mesonet is the fallback for hour-level audit when ERA5 disagrees with what Polymarket actually resolved against.
- **URL tested:** `https://archive-api.open-meteo.com/v1/archive?latitude=40.77&longitude=-73.87&daily=temperature_2m_max&temperature_unit=fahrenheit&start_date=2026-03-14&end_date=2026-03-16`
- **Response:** `temperature_2m_max":[49.8,44.5,58.4]` °F for 2026-03-14/15/16. ERA5 reanalysis daily TMAX. Free. Date range: 1940–present (a few-day lag).
- **Caveat:** Polymarket resolves against Wunderground/KLGA observed, not ERA5 grid-point. ERA5 will be within 1-2°F of the airport reading on most days but **bucket misses are possible near bin edges** — when ERA5 says 45.5°F and the resolution bucket is 44–45 vs 45–46, fall back to Iowa Mesonet METAR truth (Source 5) for the canonical observed TMAX.

## Source 5: METAR historical (Iowa State ASOS)
- **URL tested:** `https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=KLGA&data=tmpf&year1=2026&month1=3&day1=14&year2=2026&month2=3&day2=16&format=onlycomma`
- **Response (first 200 chars):** `station,valid,tmpf\nLGA,2026-03-14 00:00,M\nLGA,2026-03-14 00:05,M\n...LGA,2026-03-14 00:51,41.00\nLGA,2026-03-14 01:51,40.00\n...` — 5-minute METAR cadence, `M` = missing, hourly METARs at `:51` carry the temp readings.
- **Format:** CSV. Free, no auth.
- **Date range:** ASOS archive goes back to ~1995 for major US airports. Covers all 10 US stations. **Does NOT cover VILK (Lucknow)** — for India use NOAA ISD or Open-Meteo archive only.
- **Backtest use:** compute daily TMAX as `max(tmpf) over local-day window` and reconcile with ERA5 (Source 4) before trusting bin assignment.

## Recommended backtest window
**2025-04-01 through 2026-03-31 (12 months).** City-bucket markets are dense from Jan 2025; sticking to Apr-onward gives 365 days × ~10 cities × ~9 buckets/day ≈ 30k market-buckets. Use `startTs`/`endTs` price-history queries and treat each tick `p ≥ μ−5%` as a paper fill at our ask. Use ERA5 archive as primary settlement truth, METAR as audit.

## Open questions
- **Oldest city-bucket weather event:** Jan 2025 (`highest-temperature-in-nyc-on-jan-22`, `-23`, etc.). Earlier 2024 weather events exist but are monthly-anomaly format, not the same product.
- **Resolved outcomes always in event response:** **Yes** — `outcomePrices` field is sufficient when `closed=true` and `umaResolutionStatus=resolved`. No separate endpoint.
- **Sub-10-min price data for backfill:** No. CLOB `fidelity` floor is 10 minutes. For finer resolution we'd have to record live `book` channel WS frames going forward, which doesn't help historical backtests.
- **L2 book history:** Not available from any public endpoint. Backtest fill model must use trade prints as a proxy for taker activity.
