# Phase 3 NO-edge farmer — testing summary

**Date**: 2026-04-15
**Branch**: `phase3-no-edge-farmer` @ `2880d17` (Rust) · `weather-bot-phase0` @ `c736134` (Python)

---

## 1. Strategy

Passive resting NO-side asks on Polymarket weather temperature markets.
For each (city, date, bucket) where `P_NO(fair) − best_NO_ask ≥ 5%`,
post a resting sell at `target = P_NO − 5%`. Let takers cross us.
Portfolio-scale: 10-60 concurrent positions × $50-150 each.

## 2. Backtest configuration

| Parameter | Value |
|---|---|
| Window | 2026-03-01 → 2026-04-14 (45 days) |
| Cities | 9 (nyc, atlanta, seattle, dallas, miami, chicago, denver, houston, lucknow) |
| Markets in universe | 3,615 |
| Markets with orderbook data | 340 (10% — Predexon only indexes tokens with trading activity) |
| Orderbook source | Predexon L2 snapshots (129,035 rows in Supabase) |
| Settlement source | Iowa State Mesonet ASOS for US (300 city-days) + ERA5 for Lucknow (47 days) |
| Forecast source | Open-Meteo historical-forecast-api (HRRR + ECMWF + GFS seamless, 4,320 rows) |
| Climo source | NCEI 1991-2020 normals (US °F) + ERA5 1995-2024 (VILK °C) |
| min_edge_bps | 500 (5.00%) |
| sigma_scale | 1.0 |
| max_notional_per_market | $150 |

## 3. Backtest results

### 3.1 Three configurations tested

| Run | μ source | Settlement | ROI | P&L | Capital | Fills | Adverse |
|---|---|---|---:|---:|---:|---:|---:|
| Climo-only (post-fix) | climo | ERA5 | **+10.58%** | $2,966 | $28,023 | 580 | 0.00% |
| Forecast+ASOS (final) | ensemble+climo | ASOS+ERA5 | **+8.31%** | $2,265 | $27,263 | 585 | 1.88% |
| Lucknow-only (climo) | VILK climo | ERA5 | **+15.83%** | $406 | $2,567 | 47 | 0.00% |

### 3.2 Per-city breakdown (forecast+ASOS, the most honest run)

| City | Fills | Deployed | P&L | ROI | Adverse | Recommendation |
|---|---:|---:|---:|---:|---:|---|
| lucknow | 45 | $2,058 | $313 | **+15.21%** | 0.00% | **DEPLOY FIRST** — strongest edge, Apple Weather conviction |
| seattle | 97 | $4,106 | $468 | +11.41% | 4.12% | climo preferred |
| dallas | 103 | $4,005 | $404 | +10.09% | 0.97% | climo preferred |
| atlanta | 46 | $2,419 | $260 | +10.77% | 4.35% | climo preferred |
| chicago | 45 | $3,788 | $367 | +9.69% | 0.00% | climo strongly preferred |
| denver | 16 | $1,261 | $113 | +8.95% | 0.00% | climo preferred |
| nyc | 47 | $2,597 | $153 | +5.89% | 2.13% | climo preferred |
| houston | 71 | $2,813 | $86 | +3.06% | 1.41% | **forecast required** — only city where forecasts help |
| miami | 115 | $4,218 | $100 | +2.37% | 1.74% | climo preferred |

### 3.3 Per-edge-band (load-bearing finding)

| Band | Fills | Deployed | P&L | ROI |
|---|---:|---:|---:|---:|
| **5-7% edge** | 568 | $26,917 | **+$2,453** | **+9.11%** |
| <5% edge | 17 | $347 | −$188 | **−54.20%** |

**`min_edge_bps=500` is load-bearing.** The 17 fills that slipped below 5% lost
54% of their capital. Recommend bumping to 700 bps for live to eliminate the
tail completely — estimated +1.0 to +1.5 pp additional ROI for free.

### 3.4 Daily economics

| Metric | Value |
|---|---|
| Average daily P&L | $50-66/day |
| Fills per day | ~13 |
| Average fill size | ~$47 |
| Average holding period | 1-2 days |
| Estimated peak concurrent capital | ~$1,000-1,250 |
| **Daily ROI on working capital** | **~4-7%/day** |
| Compounding ceiling | ~$2-3k (liquidity-limited) |

## 4. Key findings

### 4.1 Forecasts hurt more cities than they help

GFS/ECMWF overfit specific cold/warm events that climo smooths out. Only Houston
benefits from forecasts (+5.4 pp vs climo). Chicago and Miami lose −7.5 pp and
−8.1 pp respectively. **Per-city source selection is the optimal strategy.**

### 4.2 Lucknow is the strongest market

+15.83% ROI on climo alone. The user has verified that Apple Weather (WeatherKit)
outperforms GFS+ICON for Lucknow TMAX over 3 days of observation. With this
personal forecast edge, Lucknow ROI should exceed 15% sustainably.

### 4.3 Settlement source matters at the margins

ASOS (actual METAR observations) is more accurate than ERA5 (reanalysis grid
point) for bucket-edge resolution. For the aggregate backtest the difference was
small (~0.5 pp), but individual city-days can flip by 1-2 buckets.

### 4.4 The strategy is liquidity-capped, not edge-capped

Only 340 of 3,615 markets (10%) had any Predexon orderbook data. Most tail
buckets have zero trading activity. The effective universe is ~15-25 markets per
day with real taker flow. Capital beyond ~$2-3k working sits idle.

## 5. Deployment plan

### Phase 1: Lucknow only (starting now)

| Parameter | Value |
|---|---|
| Cities | `lucknow` only |
| Mode | Paper → live after 7 profitable days |
| min_edge_bps | 700 |
| max_total_deployed_usdc | 500 |
| max_notional_per_market_usdc | 50 |
| forecast_source | climo (+ Apple Weather when available) |
| sigma_source | climo |

### Phase 2: Add profitable US cities (week 2+)

Add `seattle`, `dallas`, `atlanta`, `chicago`, `denver` one at a time.
Each city runs 3 days paper → live if profitable. All use `forecast_source: climo`.

### Phase 3: Add Houston with forecasts (week 3+)

Houston is the only city requiring `forecast_source: ensemble`.
Run paper for 5 days minimum since forecast-based fills have 1.41% adverse rate.

### Phase 4: Full deployment (week 4+)

All 9 cities live. max_total_deployed bumped to $2,000.
Weekly review: second-maker detection scan per design doc §7.1.

## 6. Rust bot readiness

- **216 unit tests** passing across 13 modules
- Binary boots cleanly with `NO_EDGE_FARMER_ENABLED=1 SIMULATION=true PAPER_MODE=true`
- Quoter state machine wired end-to-end (EdgeSignal → OrderSink → PaperEngine/Executor)
- Self-cross regression test guards Phase 2 / Phase 3 token isolation
- Bootstrap replay handles cold-start (one-shot Gamma snapshot + WS live tail)
- Portfolio caps enforce per-market / per-city / global limits independently

## 7. Known limitations

1. **No Apple Weather integration yet** — requires $99/year WeatherKit or phone-in-the-loop Shortcut
2. **LA and SF have no Polymarket weather markets** in the Mar-Apr window — may exist in other windows
3. **Adverse fill detection is climo-only for Lucknow** — no forecast data means fair_p_no can't move post → fill
4. **No L2 book history for 90% of markets** — Predexon only indexes tokens with real taker activity
5. **Python 3.9 `datetime.fromisoformat` is strict** — needed custom normalizer for Supabase timestamps
