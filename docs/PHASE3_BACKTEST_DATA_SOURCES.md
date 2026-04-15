# Phase 3 backtest — historical data inventory

## Summary (revised — Supabase canonical)
**Can we run a 90-day backtest?** Yes, with a **real L2 book replay** — NOT a trade-print proxy as the original draft said. The discovery that changes everything: the `polymarket-weather-bot` repo has a sibling Supabase project with a pre-designed `weather_*` schema + a working ingest pipeline for Predexon orderbook snapshots (51,762 rows already cached for non-weather markets, proving the pipeline works). Migration `002_phase3_no_edge_farmer.sql` (in the sibling `daily-liquidity-bot` repo under `src/weather_bot/migrations/`) extends the schema with t2m_c for Celsius, order_id/mode/adverse_fill on weather_positions, and backtest isolation tables. **The backtest becomes: query Supabase, not fetch live.**

### Data flow at a glance
```
Gamma /events?closed=true   ┐
Predexon /v2/polymarket/    ┤
  orderbooks (L2 snapshots) ├─▶  Python backfill script  ─▶  Supabase tables
Open-Meteo                  │     (scripts/backfill_phase3_weather.py)      │
  historical-forecast-api   │                                                │
Open-Meteo /v1/archive      ┘                                                ▼
  (ERA5 settlement truth)                                          Backtest runner
                                                                   (Python, queries SQL)
```

## Source 1: Polymarket closed events (Gamma)
- **URL tested:** `https://gamma-api.polymarket.com/events?slug=highest-temperature-in-nyc-on-march-15-2026`
- **Response shape:** array of events; each has `markets[]` with `clobTokenIds` (JSON-encoded `[YES_id, NO_id]`), `outcomes`, `outcomePrices` (`["1","0"]` = YES won, `["0","1"]` = NO won), `conditionId`, `negRiskMarketID`, `endDate`, `umaResolutionStatus`. Resolved outcome IS in the event response — no separate endpoint needed.
- **Date range:** Daily city-bucket weather markets (`highest-temperature-in-*`) started **Jan 2025** and have been running ~daily across multiple cities since. Earliest weather event of any kind: `how-hot-will-april-2024-be` (May 2024) but those are old monthly-anomaly markets, not city-buckets.
- **Pagination:** `limit` caps at 500, use `offset=`. Total weather events >1000.
- **Gotchas:** `outcomePrices` is a JSON string, not a list. `clobTokenIds` is a JSON string too. Tag-filtered `order=end_date&ascending=false` returns interleaved order — sort client-side by `endDate`.

## Source 2: Predexon orderbook snapshots (primary — **real L2 history**)
- **Endpoint:** `GET https://api.predexon.com/v2/polymarket/orderbooks`
- **Auth:** `x-api-key` header (env var `PREDEXON_API_KEY`).
- **Parameters:** `token_id` (decimal U256 string), `start_time` + `end_time` (**Unix milliseconds — not seconds**, per the Predexon timestamp gotcha), `limit` 1-200 (default 100), `pagination_key` for base64 cursor.
- **Response:** `{snapshots: [{asks: [{price, size}, ...], bids: [{price, size}, ...], timestamp, assetId, tickSize, indexedAt, market, hash}], pagination: {...}}`. **Full L2 ladder per snapshot** — multiple levels on each side with per-level `(price, size)`. Live verification (sample NYC Mar 15 winning bucket): 3-snapshot page returned 13 ask levels and 4 bid levels on one snapshot. Exactly what the backtest fill simulator needs.
- **Rate limit:** "Free & Unlimited. This endpoint does not count toward your monthly usage limits." — per Predexon docs.
- **Date range:** Historical data from **January 1st, 2026 onward** per Predexon docs. This tightens the backtest window from the original 12-month plan to **2026-01-01 → 2026-04-14** (~3.5 months).
- **Storage:** snapshots get upserted to Supabase `polymarket_orderbook_snapshots` which already has a schema (`bids` + `asks` jsonb columns, best-bid/ask, spread, mid, n_levels, depth). The table already has 51,762 non-weather rows proving the ingest pipeline works — we just need to point it at weather tokens.
- **Python wrapper exists:** `daily-liquidity-bot/src/weather_bot/wx_predexon.py::PredexonClient.get_orderbook_snapshots` auto-paginates via `pagination_key` until `has_more=false`.

## Source 2b: Polymarket CLOB `/prices-history` (fallback only)
- Trade-print timeseries, not L2. Kept as a cross-check for Predexon snapshots in case of ingest gaps. URL: `https://clob.polymarket.com/prices-history?market=<token>&startTs=<seconds>&endTs=<seconds>&fidelity=10`. **Note the unit difference:** CLOB uses **seconds**, Predexon uses **milliseconds**. Don't mix them.
- First/last tick example (NYC Mar 15 winning bucket): first `t=1773398433` (2026-03-13 21:20 UTC), last `t=1773645630` (2026-03-16 17:00 UTC), `p` ranging 0.31 → 0.9995. Confirms the bucket was active and resolved YES.

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
**2026-01-01 → 2026-04-14 (~3.5 months).** Constrained by Predexon's stated historical coverage (Jan 1 2026 onwards). Expect ~11 cities × 100 days × ~11 buckets/day × YES+NO = ~24k token-windows, each with hundreds of L2 snapshots. Supabase `polymarket_orderbook_snapshots` can hold this (Postgres handles the volume comfortably).

**Fill simulator model (revised given L2 data):** at each snapshot, walk the full ask ladder for the NO token. If our paper target price `μ_no − 0.05` sits at or below the ladder's top levels, we'd be the best ask. A taker arrives when the snapshot's best_bid crosses up through our price — compute `fill_size = min(our_size, total ask-side size at or below best_bid)`. At fill time, recompute `fair_p_no` with the contemporaneous forecast to flag `adverse_fill=true` if the edge collapsed between post and fill. All much cleaner than the original trade-print proxy.

## Data backfill workstream
- **Migration applied:** `daily-liquidity-bot/src/weather_bot/migrations/002_phase3_no_edge_farmer.sql` adds `weather_forecasts.t2m_c`, `weather_positions.order_id/mode/adverse_fill`, and the `weather_backtest_runs` + `weather_backtest_fills` tables.
- **Python backfill script (ticket #35):** `scripts/backfill_phase3_weather.py` pulls from Gamma (event discovery), Predexon (orderbook snapshots + trades), Open-Meteo historical-forecast-api (contemporaneous forecasts), and Open-Meteo archive (ERA5 settlement truth). Upserts everything to Supabase via the existing `wx_supabase` wrapper.
- **Backtest runner (ticket #29 — revised):** Python script that SELECTs from the now-populated Supabase tables and runs the fill simulator. No live fetching.

## Open questions
- **Oldest city-bucket weather event:** Jan 2025 per Gamma, but **Predexon only covers Jan 2026+**, so the backtest starts there. For 2025 data we'd need a different historical source — out of scope for first backtest.
- **Lucknow settlement truth:** Open-Meteo ERA5 archive covers VILK lat/lon. Iowa Mesonet does NOT have VILK. If ERA5 disagrees with Polymarket's resolution for Lucknow, we'd need NOAA ISD India as a fallback — check only if spot-verification fails.
- **Sub-10-min data:** Predexon orderbook snapshots have no stated fidelity floor — they're event-driven, not bar-sampled. Cadence depends on how often the book actually moved.
- **L2 book history:** **SOLVED — Predexon provides it.** See Source 2 above.
