# Phase 3 — NO-Edge Farmer

**Status:** Design · not yet implemented
**Owner:** _tbd_
**Prerequisite phases:** Phase 0 (climo bootstrap) · Phase 2 (mint-and-dump HFT)
**Target bring-up:** paper mode first, live behind `[no_edge_farmer].enabled=false` default

---

## 1. Why this exists

A live scan on 2026-04-14 of the 11 seeded weather cities × Apr 15/16 2026 temperature events turned up systematic overpricing of tail YES buckets:

- **Climo-only prior:** 65 of 205 buckets had `P_NO(climo) − best_NO_ask ≥ 5%` per share. Total fillable depth at the 5% floor = **$18,475** for **$3,400 expected profit** (+18.4% ROI per cycle) — but see §2.4, this is uncalibrated.
- **Forecast-conditioned prior** (Open-Meteo μ, σ=2°F 1-day / 3°F 2-day): **30 buckets / $5,067 fillable / $909 EP (+17.9% ROI)**. The narrower σ kills the bulk markets that the climo prior treated as tails but kept the real mispricings (Miami 80-81F Apr 15 at +23% with the forecast saying 78.7F, SF 62-63F at +17% with forecast 59.9F, Seattle 48-49F at +30% with forecast 44.2F, etc.).

The thesis is that Polymarket's weather orderbook has no informed market-makers, so buckets that are 1-2σ off the forecast mode stay mispriced in small size ($30-$200 per bucket) for hours at a time. The NO-edge farmer harvests those by resting NO-side asks at `target = P_NO − min_edge` across the full {city × date × bucket} watchlist, re-pricing on (a) every book tick and (b) every forecast refresh.

**This is a different strategy from Phase 2's mint-and-dump.** Mint-and-dump is a hot-path sprinter (mempool race, sub-5ms signer loop). The NO-edge farmer is a slow portfolio quoter — seconds of latency are fine, but it needs honest resting-order state, forecast staleness handling, and per-market cap enforcement that the existing code does not provide.

## 2. Scope & strategy parameters

### 2.1 Watchlist

| Layer | Entries |
|---|---|
| Cities (US, °F) | nyc · atlanta · seattle · dallas · miami · chicago · denver · san-francisco · los-angeles · houston |
| Cities (INTL, °C) | lucknow (VILK) |
| Dates | rolling `today..today+2` (settles `today+1` / `today+2`) |
| Bucket shape | all 11 children per event (parses `NforBelow`, `LO-HIf`, `NforHigher`, Celsius equivalents) |

11 cities × 2 dates × ~11 buckets = ~240 candidate markets per polling cycle. Typical selection after filters: 20-60 active quotes.

### 2.2 Pricing model

`P_NO(bucket) = 1 − Φ((bucket_hi+0.5 − μ)/σ) + Φ((bucket_lo−0.5 − μ)/σ)`

- **μ** = Open-Meteo forecast TMAX for `(lat, lon, date)`. Refreshed every 30 minutes. Falls back to the 30-year climo normal when `forecast_age > forecast_staleness_kill_secs` (default 90 min).
- **σ** = forecast RMSE.
  - **Primary:** Open-Meteo `/v1/ensemble` cross-model spread (11 members). See §3.5.
  - **Fallback:** hard-coded σ table indexed by `days_ahead`:
    - US (°F): 1-day = 2.0, 2-day = 3.0
    - INTL (°C): 1-day = 1.1, 2-day = 1.7
  - **Calibration multiplier:** configurable `sigma_scale` (default 1.0). Log `(hardcoded_sigma, ensemble_sigma, empirical_residual_sigma)` to a parquet file for 14 days before trusting ensemble values — they are known to be under-dispersive at tails.
- **Integer rounding:** buckets snap to whole degrees (Polymarket's stated resolution precision). The `+0.5` / `−0.5` inflates each bucket's probability to match the rounding.
- **Station map:** reuse `../../daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/wx_resolution_map.yaml` — do not re-derive. Port to a Rust static table in `climo.rs`.

### 2.3 Quote policy

| Parameter | Default | Purpose |
|---|---|---|
| `min_edge_bps` | 500 (5.00%) | minimum per-share edge before posting |
| `repost_threshold_cents` | 2 | don't amend unless target moved ≥2¢ from resting price |
| `repost_cooldown_secs` | 5 | don't cancel-and-replace a resting order faster than 5s |
| `max_notional_per_market_usdc` | 150 | hard cap per bucket |
| `max_notional_per_event_usdc` | 500 | hard cap per (city, date) event |
| `max_total_deployed_usdc` | 2000 | global portfolio cap |
| `min_notional_usdc` | 5 | don't place dust orders |
| `max_open_orders` | 60 | rate-limit / visibility cap |
| `forecast_staleness_kill_secs` | 5400 | cancel all resting if forecast stale |
| `forecast_poll_secs` | 1800 | 30-min Open-Meteo refresh cadence |
| ~~`gamma_poll_secs`~~ | — | **deleted** — discovery is WS-only after one-shot bootstrap (§3.4) |

Sizing is flat-per-market (`min(max_notional_per_market, book_depth_at_target)`) in v1. Kelly sizing is a v2 optimization — the flat policy is within ~20% of optimal for the 5-15% edge band and is much easier to reason about for position caps.

### 2.4 What the strategy is NOT

- Not a taker. The farmer **never** market-crosses the book. If a resting ask already sits at/below our target with our target size available, we accept (let the existing taker path handle it, or skip). Rationale: taker fee is 0 on Polymarket today but the **adverse-selection** on a 5-15% edge maker strategy is already tight; adding cross-the-spread gives up the maker-side ladder cushion.
- Not betting climo against forecast. The edge comes from the **forecast vs. Polymarket's price**, not forecast vs. climo. Climo is only the fallback prior when the forecast feed goes stale.
- Not a per-trade alpha bet. A single bucket at +8% expected edge and $50 notional is $4 of EV — below noise. The strategy is only economically meaningful at **portfolio scale** (20-60 concurrent positions). Risk caps must allow that.

---

## 3. Architecture

### 3.1 Diagram

```
┌────────────────────────┐
│ Open-Meteo /v1/forecast│──┐        ┌────────────────────┐
│  models=gfs_hrrr,      │  │        │                    │
│         ecmwf_ifs025   │  ├──────▶│ ForecastTick       │──┐
│  30-min poll           │  │        │ (μ per source)     │  │
│                        │  │        └────────────────────┘  │
│ AviationWeather METAR  │──┤                                │
│  5-min nowcast poll    │  │                                │
┌────────────────────────┐  │                                │
│ Polygon WSS onchain    │──┤        ┌────────────────────┐  │
│  (Phase 2 watcher;     │  ├──────▶│ WeatherEvent       │──┤
│   event.clone fork)    │  │        │                    │  │
└────────────────────────┘  │        └────────────────────┘  │
┌────────────────────────┐  │                                │
│ CLOB WS market channel │──┤        ┌────────────────────┐  │
│  dynamic subscription  │  ├──────▶│ BookUpdate         │──┤
└────────────────────────┘  │        └────────────────────┘  ▼
┌────────────────────────┐  │                                ┌──────────────────┐
│ CLOB WS user channel   │──┤        ┌────────────────────┐  │ EdgeBook (actor) │
│  own fills / cancels   │  ├──────▶│ FillEvent          │─▶│ single-owner task│
└────────────────────────┘  │        └────────────────────┘  │ HashMap<Tok,E>   │
                            │                                │                  │
                            │        ┌────────────────────┐  │  emits:          │
                            │        │ tokio::interval    │  │  EdgeSignal →    │
                            │        │  500ms dirty-tick  │─▶│                  │
                            │        └────────────────────┘  └────────┬─────────┘
                            │                                         │
┌────────────────────────┐  │                                         ▼
│ Station API (NWS/METAR)│──┘        ┌────────────────────┐  ┌──────────────────┐
│  post-settlement poll  │──────────▶│ SettlementTick     │─▶│ Quoter (actor)   │
│  (or Python cron, §5)  │           └────────────────────┘  │  state machine   │
└────────────────────────┘                                   │  per token       │
                                                             │                  │
┌─ Phase 2 pipeline (untouched) ───────────────┐             │  calls:          │
│ Polygon WSS onchain → WeatherEvent ──────────┼────────────▶│  Executor::      │
│ Polygon WSS mempool → MintReceipt ───────────┼────────────▶│    post/cancel   │
│ CLOB WS anchor      → ClobMarketReady ───────┘             └──────────────────┘
└──────────────────────────────────────────────┘
```

### 3.2 Rust modules (new files under `weather-bot/src/`)

| File | Role |
|---|---|
| `watchers/forecast.rs` | 30-min Open-Meteo poll over the city list. Single `/v1/forecast?models=gfs_hrrr,ecmwf_ifs025` call plus `/v1/ensemble` for σ. Source cascade encoded in `pricer.rs` (see §3.5). Emits `ForecastTick { city, date, mu, sigma, source, fetched_at_ns }`. |
| `watchers/metar.rs` | 5-min AviationWeather METAR poll per seeded ICAO. Computes `observed_tmax_so_far` + `remaining_variance_fraction` for nowcast σ collapse (see §3.5). Emits `NowcastTick { icao, date, observed_max, hour, remaining_var_frac }`. Per-ICAO staleness kill: reverts that station to forecast-only σ after 15 min. |
| `watchers/clob_book.rs` | CLOB WS `market` channel client. Maintains an authoritative `HashSet<TokenId>` of subscribed tokens (replays it after reconnect — see §3.4). Emits `BookUpdate { token_id, best_bid, best_ask, asks_ladder, ts }`. Dedicated from Phase 2's `clob_ws.rs` to avoid coupling. |
| `watchers/clob_user.rs` | CLOB WS `user` channel subscriber for own orders. Emits `FillEvent`, `CancelAck`, `OrderPlaced` events. |
| `no_edge/bootstrap.rs` | One-shot startup replay: subscribes WS first (buffers `WeatherEvent`s), then fetches `/events?active=true&closed=false` filtered to weather slugs, tags each as `source: BootstrapReplay`, drains the WS buffer with dedup-by-condition_id. Also runs `Executor::list_open_orders(funder)` to cross-ref `NoEdgeState` for orphan cancellation. **Runs once, not periodically.** |
| `climo.rs` | Static loader for `data/climo/*.json` files (copied from the Python engine). Exposes `daily_normal(icao, month, day) → (mu, sigma)` with Feb-29 fallback. Port of `wx_climo.py`. ~40 LoC. |
| `pricer.rs` | Pure function: `fair_p_no(bucket, forecast) → f64` using erf-based Φ. Exposes `gaussian_bucket_prob(lo, hi, tail, mu, sigma)`. Port of `wx_scoring::gaussian_bucket_prob`. ~60 LoC. |
| `edge_book.rs` | **Actor task** — single owner of `HashMap<TokenId, EdgeEntry>`. See §3.3. |
| `quoter.rs` | **Actor task** — single owner of per-token quote state machine `QState::{Idle, Resting{order_id, price, size, posted_at}, Cancelling{old_order_id, cancel_sent_at}}`. See §3.6. |
| `portfolio.rs` | Deployed-notional accounting across the farmer's positions. Exposes mpsc-request/oneshot-reply `Query::{CanDeploy(usdc)→bool, Deployed()→f64, RecordFill(...), RecordCancel(...)}`. **Does not touch `BotState::daily_minted_usdc`** — see §4.2. |
| `settlement.rs` | **Optional Rust path.** Phase-1 preference is a Python cron reading `bot_state.json` — see §5. Only write this module if the Python cron proves insufficient. |
| `no_edge/mod.rs` | Glue — builds all the actors, returns handles to `main.rs`. Gated behind `config.no_edge_farmer.enabled`. |

### 3.3 EdgeBook actor

EdgeBook is a task, not a `DashMap`. Rationale: multiple writers (scanner on book tick, forecast handler, user-ws on fill) break single-writer-by-convention on a concurrent map; the first bug is silent and non-local. Actor pattern gives one owner, one `HashMap`, and all mutations land in order:

```rust
enum EdgeCmd {
    BookTick(BookUpdate),
    ForecastTick(ForecastTick),
    Fill(FillEvent),
    CancelAck { token_id, order_id },
    RegisterBucket(BucketInfo, BucketContext),   // from EventSnapshot
    Query { token_id, reply: oneshot::Sender<Option<EdgeEntry>> },
}

struct EdgeEntry {
    bucket: BucketInfo,
    context: BucketContext,           // city, date, lat/lon, ICAO, unit
    fair_p_no: f64,                    // last-computed
    fair_p_no_ts: i64,                 // staleness guard
    top_ask: f64,                      // best NO ask from CLOB
    top_ask_size: f64,
    ladder: Vec<(f64, f64)>,           // sorted asks for depth calc
    my_state: QState,                  // mirrored from quoter for read-only scan
    shares_held: f64,                  // from fills
    avg_entry: f64,
    dirty: bool,                       // scanner sets, quoter clears
    last_signal_ts: i64,
}
```

EdgeBook does not call `Executor`. It maintains state and emits `EdgeSignal` events through an mpsc to the Quoter actor. A `tokio::time::interval(500ms)` tick drains the dirty set: for each dirty entry, recompute `target_ask = fair_p_no − min_edge`, compare to resting, emit a signal only if it passes `repost_threshold_cents` AND `repost_cooldown_secs`.

This is the **coalesce-via-dirty-bit** pattern: a 10Hz-per-token book stream across 60 tokens collapses to ≤2Hz/token of actual Quoter traffic, which collapses to ≤2Hz/token of CLOB POSTs regardless of upstream noise.

### 3.4 Subscription management

**Discovery is WS-only after boot.** There is no Gamma polling task. Phase 2 already has `watchers::onchain::run_onchain_watcher` subscribed to Polygon WSS logs on the NegRiskAdapter contract, emitting `WeatherEvent { buckets: Vec<BucketInfo> }` the instant `MarketPrepared` fires (12-36h before settlement). `BucketInfo` already carries `token_id_yes`, `token_id_no`, `condition_id`, `bucket_label`, and `outcome_index` — every field the farmer needs. Phase 3 consumes the **same stream**.

**Fan-out pattern: Option B (surgical, one line in main.rs).** The existing `event_rx: mpsc::UnboundedReceiver<WeatherEvent>` stays mpsc — no watcher signature change, no broadcast semantics drift. The existing `Some(event) = event_rx.recv()` arm in `main.rs` adds one `no_edge.event_tx.send(event.clone())` call **before** spawning the Phase 2 mint work. `WeatherEvent` is already `Clone` (Phase 2 already clones it for the paper path at `main.rs:166`). Zero-cost fork, zero blast radius, zero lagged-receiver risk.

```
                              watchers::onchain (Polygon WSS)
                                          │
                                  WeatherEvent (mpsc)
                                          │
                                          ▼
                                  main.rs event_rx arm
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   │ event_tx.send(event.clone())                │
                   ▼                                             ▼
       Phase 2 mint-dump (existing)                   Phase 3 no_edge.event_tx
       presigner + mint_exec spawns                    EdgeBook::RegisterBucket
```

**Subscription-add fan-out.** EdgeBook, on receiving a `RegisterBucket` command from the fan-out, emits `SubCmd::Add(token_id_no)` through an mpsc to `clob_book.rs`, which owns the authoritative `HashSet<TokenId>` and the WS write side:

```
EdgeBook ── SubCmd::Add(no_token) ──▶ clob_book (owns WS write)
                                      │
                                      ├─ inserts into local HashSet<TokenId>
                                      ├─ sends subscribe frame over existing WS
                                      └─ on reconnect: replays ENTIRE HashSet
```

**Critical reconnect behavior:** Polymarket's CLOB WS does not persist subscriptions across disconnects. `clob_book.rs` is the only task that knows the full subscribed set. The upstream discovery path (onchain stream) will not retransmit old events; the reconnect path must replay from the local authoritative `HashSet`. Unit test this by killing the WS connection mid-run and asserting every prior token re-subscribes before the first post-reconnect frame is accepted.

**Startup bootstrap (WS-first, then Gamma snapshot).** On binary startup the farmer needs to know every currently-open weather event, not just ones that appear after boot. The on-chain watcher only streams new events — it never replays historical `MarketPrepared` logs. Standard snapshot-and-tail pattern (`no_edge/bootstrap.rs`):

1. **Subscribe to WS first** (spawn `watchers::onchain` as normal) and buffer any `WeatherEvent`s that arrive into a `Vec<WeatherEvent>` bootstrap queue.
2. **Then fetch** `GET gamma-api.polymarket.com/events?active=true&closed=false&limit=500` (paginated until fewer than 500 rows), filter to slugs matching the weather template, construct `WeatherEvent`s tagged `source: DiscoverySource::BootstrapReplay`, feed them to EdgeBook.
3. **Then drain** the bootstrap queue into EdgeBook with dedup-by-`condition_id` so events that appeared mid-bootstrap are not double-counted.
4. **Then Phase 3 is WS-only for the rest of the process lifetime.**

Phase 2 mint-and-dump **must skip** events tagged `BootstrapReplay` — the mint window is 12-36h pre-settlement and is long gone for any event old enough to appear in the active-events snapshot. Plumb the tag through `WeatherEvent.source: DiscoverySource` (new field, defaults to `OnChain` for the live stream). Also at bootstrap: run `Executor::list_open_orders(funder)` and cross-ref against `NoEdgeState::known_orders` — adopt recognized orders into EdgeBook, cancel orphans before the first new post (see §4.4).

No `Arc<RwLock<HashSet>>` shared between tasks — the channel pattern matches every other watcher in this codebase.

### 3.5 Forecast source cascade + σ model

**μ source cascade** (per `(city, days_ahead)`):

| Region | Horizon | Primary μ | Rationale |
|---|---|---|---|
| US | ≤ 48h | HRRR (via Open-Meteo `models=gfs_hrrr`) | NOAA 3km CONUS high-res, hourly refresh, ~1.5°F RMSE at 24h |
| US | > 48h | ECMWF (via Open-Meteo `models=ecmwf_ifs025`) | Global, ~2-3°F RMSE at 48-72h |
| INTL | any | ECMWF (via Open-Meteo `models=ecmwf_ifs025`) | Consistent global model |
| any | fallback | Open-Meteo `seamless` default | When source-specific endpoint returns no data |

All three routes go through the **same** Open-Meteo `/v1/forecast?models=…` HTTP call, not raw NOMADS or ECMWF APIs. This bypasses GRIB2 parsing and gives us one HTTP client, one parser, one rate-limit surface. The source cascade is encoded as a lookup table in `pricer.rs`, not as branching logic scattered through the codebase.

**σ source rule (decided):** σ is **always** from Open-Meteo `/v1/ensemble` cross-model spread, regardless of which source supplies μ. Rationale: per-source point forecasts don't carry σ, and per-(source, days_ahead) hard-coded tables add a maintenance surface we don't need. One σ provider, one calibration target, one knob (`sigma_scale`). Fallback to hard-coded σ table only when the ensemble endpoint fails 3 consecutive polls or returns <3 members.

**METAR nowcast layer (new):** during the trading day, `watchers/metar.rs` polls AviationWeather METAR every 5 minutes for each seeded ICAO. Observed-TMAX-so-far is a physical lower bound on the final TMAX. The correct σ formulation at hour `h` is:

```
σ_remaining(h, icao, month) = σ_full_day × √remaining_variance_fraction(h, icao, month)

P(TMAX_final ≤ x) = Φ((x − μ_remaining)/σ_remaining),   truncated below at observed_so_far
```

Where `remaining_variance_fraction(h, icao, month)` is precomputed from a 5-year METAR archive as the fraction of daily TMAX variance that lives in hours `[h..24)`. Early-morning: ≈1.0 (full σ). Late-afternoon after TMAX has likely passed: ≈0.1-0.3. Not "σ → 0" — the math needs the diurnal variance curve, not a hand-wave.

Worked example: NYC at 18:00Z observed 78°F, forecast TMAX 85°F. If `remaining_variance_fraction(18, KLGA, apr) = 0.10`, then `σ_remaining = σ_full × √0.10 ≈ 0.32 × σ_full`. A late-afternoon 78°F observation collapses the 84-85°F YES bucket to ~5% (versus ~50% without nowcast), with a hard floor that `TMAX_final ≥ 78°F`.

**Calibration log schema** (written to `logs/forecast_residuals.parquet`):
```
(city, days_ahead, source, forecast_age_hours, hardcoded_sigma, ensemble_sigma,
 nowcast_sigma, actual_observed_tmax, residual, adverse_fill)
```
The grouping key is **`(city, days_ahead, source)`** — not just `days_ahead`. Miami 1-day RMSE differs from Seattle 1-day RMSE because of marine-layer variance, and HRRR vs ECMWF residuals differ at the source-switch boundary. Averaging across source switches produces a meaningless empirical σ.

After 14 days of paper-mode data, compute `σ_empirical = stdev(residual)` grouped by `(city, days_ahead, source)` and compare to `ensemble_sigma`. If ensemble is systematically tight (it usually is at tails), bump `sigma_scale` per group. Typical correction for 1-2 day forecasts: 1.15-1.35.

**Kill-switches (two independent):**
- `forecast_staleness_kill_secs` (default 5400 = 90 min): if the forecast feed has been unresponsive longer than this, quoter cancels all resting orders and refuses new posts. **Global.**
- `nowcast_staleness_kill_secs` (default 900 = 15 min = 3 missed METAR cycles): if METAR for a specific ICAO has been unresponsive, **revert that ICAO to forecast-only σ** (skip the nowcast layer). Not a global kill — only degrades one station's precision. Prevents overconfident σ on stale "observed-so-far" data.

**Rationale:** a 30% error in σ roughly doubles the tail-bucket probability error. A 5% edge floor under a wrong σ picks up fake signals and we lose real money. Run the calibration log for 14 days before turning live on.

### 3.6 Quoter state machine

```
        RegisterBucket           EdgeSignal(price, size)
Idle ─────────────────▶ Idle ──────────────────────────▶ post_order
                                                              │
                                                              ▼
                              EdgeSignal (same price)    Resting{id, price, size, ts}
                              no-op                           │
                                                              │
                             EdgeSignal (|Δ| ≥ 2¢)            ▼
                             ──────────────────────────▶ cancel_order(id)
                                                              │
                                                              ▼
                                                       Cancelling{old_id, ts}
                                                              │
                              CancelAck                       │
                              ────────────────────────────────┤
                                                              │
                                                              ▼
                                                            Idle
                                                              │
                                                              │ FillEvent during Cancelling:
                                                              │ ────────────────▶ update portfolio,
                                                              │                   treat as terminal,
                                                              │                   recompute fair and reprice
```

**Cancelling → Fill** is a **legal transition**, not an error. CLOB cancels are async and racy — a forecast refresh can trigger a cancel, and between `cancel_sent` and `CancelAck` a taker can still cross the resting ask. The state machine mirrors `FillEvent`s against the `old_order_id` during the cancel window and updates portfolio accounting atomically. Do not assume a sent cancel means the order is gone.

## 4. Integration with existing code

### 4.1 Main loop changes

`weather-bot/src/main.rs` gains a gated NO-edge section. Wiring sketch:

```rust
// after existing mint/dump watcher spawns, before the select loop:
let no_edge = if config.no_edge_farmer.enabled {
    Some(no_edge::spawn_all(&config, executor.clone(), alerts.clone()).await?)
} else {
    None
};

loop {
    tokio::select! {
        // --- existing Phase 2 arms, unchanged ---
        Some(event)   = event_rx.recv()   => { /* mint-and-dump */ }
        Some(receipt) = mint_rx.recv()    => { /* mint receipt */ }
        Some(ready)   = ready_rx.recv()   => { /* clob ready */ }

        // --- Phase 3 arms, gated on no_edge being Some ---
        Some(fill) = async { no_edge.as_ref()?.fill_rx.recv().await },
              if no_edge.is_some() => {
            no_edge.as_ref().unwrap().handle_fill(fill).await;
        }
        Some(alert) = async { no_edge.as_ref()?.alert_rx.recv().await },
              if no_edge.is_some() => {
            alerts.send(alert).await;
        }

        _ = signal::ctrl_c() => {
            if let Some(ne) = &no_edge { ne.shutdown().await; }  // cancel all resting, flush state
            state.lock().await.save();
            break;
        }
    }
}
```

Everything heavy (`EdgeBook`, `Quoter`, all watchers) runs in detached tasks with their own mpsc plumbing. Only **thin notification arms** land in the `tokio::select!` — forecast ticks, book updates, and edge signals never touch the main select. This keeps the mint-path arm latency unchanged.

### 4.2 State separation (do NOT reuse `BotState`)

`BotState.daily_minted_usdc` is a **mint-size throttle** (recycled on each mint). The NO-edge farmer tracks **deployed notional** (locked until settlement). These are different units and conflating them masks runaway loops — one strategy's low usage would silently mask the other's blowout.

Design:
- Keep `BotState` exactly as-is for mint-and-dump.
- Add a new `NoEdgeState` struct in `portfolio.rs`, owned by the portfolio actor, serialized to `no_edge_state.json` (separate file).
- A lightweight `Portfolio` facade exposes `query_deployed() -> f64` via `mpsc::Sender<Query>` / `oneshot::Sender<f64>`. The quoter calls the facade, never touches either state directly.
- Config surfaces **two separate caps**:
  - `[mint_dump].daily_cap_usdc` — existing, unchanged.
  - `[no_edge_farmer].max_total_deployed_usdc` — new, independent.

### 4.3 Executor extensions (**prerequisite — blocks Phase 3**)

`executor.rs` currently only has `submit_limit_order(token_id, price, size, side)` which returns `Result<()>` and discards the response. A resting-maker cannot use this. Phase 3 requires three new methods on `Executor`:

1. `post_limit_order(...) -> Result<OrderId>` — returns the CLOB-assigned order ID so the quoter can track it.
2. `cancel_order(order_id: &str) -> Result<()>` — cancels a specific order.
3. `list_open_orders(funder: Address) -> Result<Vec<OpenOrder>>` — for the startup recovery path (§4.4).

These wrap `polymarket-client-sdk::clob` methods that already exist in the SDK (see `client.post_order(...)` on executor.rs:124 — just thread through the return value).

### 4.4 Order recovery on restart

A resting-maker bot **cannot start fresh**. If the binary restarts while orders are live, the new process has no idea which CLOB orders are its own and no way to cancel orphans later. Startup sequence for the farmer:

1. `Executor::list_open_orders(funder)` → all open orders for our address.
2. Cross-reference each against `no_edge_state.json → known_orders` (populated on every post, pruned on every cancel/fill).
3. **Recognized:** adopt into `EdgeBook` so the quoter resumes management. The quoter state becomes `Resting { id, price, size, posted_at=now }` (we lose the original `posted_at` but the cooldown check is just a throttle).
4. **Orphan (open on CLOB, not in state):** cancel it immediately. This is the only case where the farmer issues a cancel without a prior post.
5. **Missing (in state, not on CLOB):** fetch the order's fill history via `GET /order/{id}`. If filled, update portfolio accounting. If cancelled or unknown, drop from state.

This recovery must run before the first new post, or a restart during a quoting burst will duplicate orders and strand old ones.

### 4.5 Paper-mode extension

Current `PaperEngine::run_event` (paper.rs:305) walks `walk_bids_for_sell` for a taker dump — useless for a resting maker.

Minimum changes to make paper mode honest:

1. Add `PaperRestingOrder { token_id, price, size, posted_at, fair_p_no_at_post }` to `PaperAccount`.
2. On every `BookUpdate` for a token with a paper resting order: check if the new `best_bid >= paper_order.price`. If so, compute fill: `fill_size = min(paper_order.size, sum of bids at or above paper_order.price)`. Mark the order filled. Update bankroll.
3. **Adverse-fill detection:** at the moment of the paper fill, recompute the current `fair_p_no` with the latest ForecastTick. If `fair_p_no < price + min_edge` (i.e. edge has collapsed), tag the fill as `adverse_fill=true` in the CSV log. Do **not** block the fill — record it. Adverse-fill rate is your real selection-bias metric.
4. Paper `PaperEngine::post_resting_order(...)` is called by the same quoter actor in paper mode — no branch in the quoter itself. Use a trait:

```rust
trait OrderSink {
    async fn post(&self, ...) -> Result<OrderId>;
    async fn cancel(&self, order_id: &str) -> Result<()>;
}
impl OrderSink for Executor { ... }
impl OrderSink for PaperEngine { ... }
```

Trait-based dispatch keeps the quoter identical in paper vs. live and lets you A/B the same state machine against a real and simulated fill stream.

5. Add a `paper_no_edge.csv` log alongside the existing mint-path paper log: `(ts, city, date, bucket, side, price, size, fair_p_no_at_post, fair_p_no_at_fill, edge_at_post, edge_at_fill, adverse_fill, bankroll)`.

Estimated size: ~200 LoC added to `paper.rs`.

## 5. Settlement — Python cron, not Rust port

**Decision:** run settlement out-of-band as a Python cron, not as a Rust module in this binary.

Rationale:
- Settlement is T+1 and has zero latency budget. Rust gives nothing here.
- `daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/engine/nws_settlement.py` already works. Porting is wasted effort.
- Python cron runs on the box nightly after the UTC 12:00 event cutoff.

Constraints:
- **Versioned schema on `no_edge_state.json`.** Add `{"schema_version": 1, ...}` at the top. Rust side uses `#[serde(deny_unknown_fields)]` and refuses to load any version it doesn't recognize — prevents a field rename from silently corrupting state. Bump the version on every field change.
- **File locking.** Python cron must take `no_edge_state.json.lock` (flock) before reading, and the Rust process must take the same lock before writing. Atomic rename (`tmp → real`) after writes. Alternative: SQLite. For a daily-cadence write pattern, flock + atomic rename is fine.
- **Python reads, Python writes a settlement ledger.** The cron reads `no_edge_state.json`, pulls TMAX via NWS / METAR for each settled event, computes realized P&L, and writes `no_edge_settlement_ledger.jsonl` (append-only). The Rust side reads this ledger on startup for display, but never mutates it.

If the Python cron proves operationally painful (multi-host deployments, stateful reconciliation), revisit and port `nws_settlement` to Rust. Not before.

## 6. Config additions

`config.rs` gains a `no_edge_farmer` section. New `.env` keys (all optional, defaults in code):

```
NO_EDGE_ENABLED=false                       # kill-switch
NO_EDGE_MIN_EDGE_BPS=500
NO_EDGE_REPOST_THRESHOLD_CENTS=2
NO_EDGE_REPOST_COOLDOWN_SECS=5
NO_EDGE_MAX_NOTIONAL_PER_MARKET_USDC=150
NO_EDGE_MAX_NOTIONAL_PER_EVENT_USDC=500
NO_EDGE_MAX_TOTAL_DEPLOYED_USDC=2000
NO_EDGE_MAX_OPEN_ORDERS=60
NO_EDGE_FORECAST_POLL_SECS=1800
NO_EDGE_METAR_POLL_SECS=300
NO_EDGE_FORECAST_STALENESS_KILL_SECS=5400   # 90min — global
NO_EDGE_NOWCAST_STALENESS_KILL_SECS=900     # 15min — per-ICAO, reverts to forecast σ
NO_EDGE_SIGMA_SCALE=1.0                     # calibration multiplier
NO_EDGE_SIGMA_SOURCE=ensemble               # always — "hardcoded" only via fallback path
NO_EDGE_CITIES=nyc,atlanta,seattle,dallas,miami,chicago,denver,san-francisco,los-angeles,houston,lucknow
NO_EDGE_LOOKAHEAD_DAYS=2
NO_EDGE_PAPER_LOG_PATH=paper_no_edge.csv
```

Note: `NO_EDGE_GAMMA_POLL_SECS` from the v1 design is **deleted**. There is no periodic Gamma poll — only a one-shot startup snapshot (§3.4).

## 7. Risks & open questions

### 7.1 Strategy decay
The strategy relies on the **absence of a second market-maker**. A taker bot (like the public @alterego_eth tutorial) does NOT compete with us — a taker crossing the book *fills* us and is a feature, not a threat. The actual decay trigger is another resting-maker quoting NO one tick tighter than ours.

**Reframed weekly review signals** (leading, not lagging):
- **Primary leading indicator:** scan CLOB depth each morning and count NO-side resting asks that (a) sit one tick above our target price and (b) post sizes ≥ $30 notional and (c) persist for ≥5 minutes across multiple buckets. One observation = benign (could be a retail mistake). Three in a day across different cities = a second maker is live. Kill-switch at that point and re-price to undercut or step aside.
- **Secondary leading indicator:** median top-of-book NO ask for the 10 highest-edge buckets over the past 24h. Rising median = the book is absorbing a new supply of NO liquidity = a maker is operating upstream.
- **Lagging indicator (use only for confirmation):** ROI per cycle vs 7-day rolling median. If ROI drops ≥25% week-over-week *and* the leading indicators fire, that's a confirmed regime shift.

**Decay budget: 1-2 weeks** (down from the original 2-4 — the public tutorial shortens the window for copycats to spin up). Weekly review, not monthly.

**Responses (in order of escalation):**
1. Already mid-week: tighten `min_edge_bps` from 500 → 700 to stop fighting over marginal buckets.
2. If ROI stays > 10%/cycle through week 4: **add YES-side asks** (symmetric edge where forecast says YES > market) to double effective capacity. Same codepath with `side: Buy` swapped.
3. If ROI drops below 3% and leading indicators say a maker is live: switch to less-liquid secondary cities (London, Paris, Tokyo — need climo + station map bootstrap per §3.2 `climo.rs`). These have thinner books and fewer competitors.
4. If ROI stays below 1% for 5 consecutive days across all cities: shut the farmer off, revert to Phase 2 mint-dump only, and revisit the thesis.

### 7.2 Forecast σ error
A 30% σ error roughly doubles the tail-bucket probability error, which at the 5% edge floor means most signals become noise. Mitigations listed in §3.5:
- **Always-ensemble-σ** decision means one provider, one calibration target, one `sigma_scale` knob
- Hard-coded σ table only as fallback when ensemble returns <3 members
- METAR nowcast layer shrinks σ live during the trading day with variance-fraction math (not hand-waved σ→0)
- 14-day calibration log grouped by `(city, days_ahead, source)` before trusting any multiplier
- Two independent staleness kill-switches: `forecast_staleness_kill_secs` (global) and `nowcast_staleness_kill_secs` (per-ICAO, demotes to forecast-only σ)

### 7.3 Thin depth vs. competing fills
$5k fillable at the 5% floor is **half** what it looks like — someone else's market buy can sweep the same ladder we're targeting. Per-cycle realized fill rate is probably 30-50% of top-of-ladder. Paper mode must measure this before turning live on.

### 7.4 Cancel-vs-fill race
Already addressed in §3.6 quoter state machine. Explicit: every Cancelling transition records `cancel_sent_at`. The quoter does not transition back to Idle until either `CancelAck` or `FillEvent` arrives for `old_order_id`. A 30-second hard timeout alerts and forces manual intervention.

### 7.5 Mint-path interaction — **no self-cross (verified)**
Verified against `presigner.rs:94`: the Phase 2 dump path only posts sell orders on `bucket.token_id_yes`. It never touches NO. The NO-edge farmer is posting sells on `token_id_no`. The two pipelines write to disjoint token IDs on the same underlying event and do not compete.

If Phase 2 is ever extended to dump NO (e.g. for a basket merge-and-redeem path), **this assumption breaks** and the quoter must read the presigner cache to avoid self-crossing. Guard: add a regression test on `no_edge::quoter` that fails if the presigner cache contains a `Side::Sell` order on a token ID the farmer manages.

### 7.6 WebSocket saturation
11 cities × 2 dates × ~11 buckets = ~240 potential NO token subscriptions. Polymarket's CLOB WS handles this fine; the real worry is our bandwidth to `book_rx`. If the dirty-bit ticker at 500ms can't keep up with incoming updates, back-pressure is silent (mpsc::unbounded). Add a per-tick metric: `edge_book.dirty_tokens_processed_per_tick`. Alert if it exceeds 80% of registered tokens for 3 consecutive ticks.

---

## 8. Work plan

Ticket-sized breakdown. Each item is independently PR-able.

| # | Ticket | Depends on | Est. LoC | Priority |
|---|---|---|---|---|
| 1 | Extend `Executor` with `post_limit_order → OrderId`, `cancel_order`, `list_open_orders` | — | ~120 | **P0 blocker** |
| 2 | Port `climo.rs` + embed `wx_resolution_map.yaml` as a static table | — | ~150 | P0 |
| 3 | Port `pricer.rs` (`gaussian_bucket_prob`, `fair_p_no` free functions + unit tests mirroring the Python tests) | #2 | ~120 | P0 |
| 4 | `watchers/forecast.rs` — Open-Meteo `/v1/forecast?models=gfs_hrrr,ecmwf_ifs025` cascade, ensemble σ primary, hard-coded σ fallback. **Acceptance: unit test that `gfs_hrrr` returns daily TMAX for NYC (not just hourlies we have to max)** | #3 | ~250 | P0 |
| 4b | `watchers/metar.rs` — 5-min AviationWeather METAR poll, `remaining_variance_fraction(icao,month,hour)` lookup table, `NowcastTick` emit, per-ICAO staleness kill | #2 | ~220 | P0 |
| 4c | Precompute `remaining_variance_fraction` lookup from 5-year METAR archive — one-shot data pipeline → ship table as `data/metar_variance.json` | #4b | ~150 Py | P0 |
| ~~5~~ | ~~`watchers/gamma_events.rs`~~ | **DELETED** — WeatherEvent fan-out from existing `watchers::onchain` makes this unnecessary (§3.4) | — | — |
| 6 | `watchers/clob_book.rs` — WS subscribe/unsubscribe + reconnect replay, `BookUpdate` emit | — | ~350 | P0 |
| 7 | `edge_book.rs` actor + `EdgeCmd` enum + dirty-bit ticker + `EdgeSignal` emit. Handles `RegisterBucket` cmd from fan-out (§3.4), emits `SubCmd::Add` to `clob_book.rs` | #3 #4 #4b #6 | ~450 | P0 |
| 8 | `quoter.rs` actor + state machine + `OrderSink` trait + Cancelling→Filled reconciliation | #1 #7 | ~350 | P0 |
| 9 | `portfolio.rs` + `NoEdgeState` + facade mpsc + `no_edge_state.json` serde w/ `schema_version=1` + `deny_unknown_fields` | — | ~250 | P0 |
| 10 | Paper-mode extension: `PaperRestingOrder`, book-cross fill sim, adverse-fill tag, `paper_no_edge.csv`, `OrderSink` impl for PaperEngine | #8 | ~250 | P0 |
| 11 | `no_edge/bootstrap.rs` — WS-first + Gamma snapshot + dedup + `DiscoverySource::BootstrapReplay` tag + orphan-order cancel via `list_open_orders` | #1 #6 #9 | ~250 | P0 |
| 12 | `main.rs` wiring: gated `no_edge::spawn_all`, event fan-out one-liner in existing `event_rx` arm, new `tokio::select!` arms, ctrl-c drain. **Phase 2 skip path for `BootstrapReplay`-tagged events** | #8 #9 #10 #11 | ~120 | P0 |
| 13 | `watchers/clob_user.rs` — user-channel fill stream, wire into EdgeBook + portfolio | #6 #9 | ~250 | P0 |
| 14 | **Self-cross regression test** — fails if presigner cache contains `Side::Sell` on any `token_id_no` managed by the farmer. **Promoted P1 → P0** per advisor: with no Gamma poll, the on-chain stream is the only coordination point between Phase 2 and Phase 3 | #8 | ~100 | P0 |
| 15 | Calibration log writer: `logs/forecast_residuals.parquet` with `(city, days_ahead, source)` grouping, reconciled against nightly settlement | #9 | ~180 | P0 |
| 16 | 14-day paper-mode bake + calibration analysis (`sigma_scale` fitting per source/city) | #12 #15 | N/A | P0 |
| 17 | Historical backtest: replay 30-90 days of Gamma closed weather events + price history through the farmer in offline mode. Deliverable: `docs/PHASE3_BACKTEST_REPORT.md` with per-city / per-bucket edge-capture numbers | #8 | ~300 Py | P0 |
| 18 | Python settlement cron + flock + atomic rename + versioned ledger | #9 | ~150 Py | P1 |
| 19 | Telegram alert hookups: new fill, forecast-staleness kill, orphan cancel, second-maker detection | #12 #13 | ~120 | P1 |
| 20 | Metrics: per-tick dirty-token count, WS reconnect counter, cancel-vs-fill race counter, second-maker leading indicator (§7.1) | #12 | ~150 | P1 |
| 21 | Operational runbook: how to read the paper CSV, kill-switch procedure, `sigma_scale` tuning, leading-indicator review | #16 | doc | P2 |

**P0 subtotal:** ~3,000 LoC Rust + ~450 LoC Python (variance precompute + backtest) + paper-mode. 1-2 engineer-weeks to working paper mode.
**Live bring-up:** gated on all of #14 (self-cross test), #16 (7 consecutive days of paper-mode P&L within 20% of model), and #17 (historical backtest confirming edge capture ≥ 50% of raw model EV).

---

## 9. Open questions to resolve before coding

1. **Funder vs. signer for `list_open_orders`.** The Polymarket CLOB SDK wants a `funder` address for open-order queries. Phase 2's `Executor` already derives this — verify and reuse.
2. **Order ID format.** SDK returns UUID string? 32-byte hex? Impact on `no_edge_state.json` serde. Check against live post response.
3. **Empirical σ source for calibration.** NWS daily TMAX for US. For Lucknow, ERA5 or Open-Meteo archive? Same-day-as-settlement availability matters.
4. **Tick size / min size per market.** Polymarket has per-market tick sizes (0.01, 0.001, 0.0001). `submit_limit_order` must round to the correct tick — otherwise posts are rejected. Add `clob::tick_size` fetch to the register-bucket path.
5. **Price bounds.** Polymarket rejects orders at price ≤ 0 or ≥ 1. Quoter must clamp and skip when `target_ask ≥ 0.999` (no profitable sell at that level anyway).

---

## 10. References

- Live scan that produced the thesis: `/tmp/wxscan/_portfolio.json` + `/tmp/wxscan/_portfolio_forecast.json` (on the dev box; not committed).
- Pricing kernel source of truth: `daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/wx_scoring.py::gaussian_bucket_prob`.
- Station map: `daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/wx_resolution_map.yaml`.
- Climo normals: `daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/data/climo/*.json` (NCEI 1991-2020 for US, ERA5 1995-2024 for VILK).
- Phase 2 anchors: `weather-bot/src/main.rs`, `weather-bot/src/presigner.rs`, `weather-bot/src/executor.rs`, `weather-bot/src/paper.rs`.
