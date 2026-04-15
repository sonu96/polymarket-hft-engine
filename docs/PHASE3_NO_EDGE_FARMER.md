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
| `gamma_poll_secs` | 300 | 5-min Gamma event-snapshot cadence |

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
│ Gamma /events?slug=    │──┤        ┌────────────────────┐  │
│  watchlist poll (5m)   │  ├──────▶│ EventSnapshot      │──┤
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
| `watchers/forecast.rs` | 30-min Open-Meteo poll over the city list. Uses `/v1/forecast` for μ; optionally `/v1/ensemble` for σ. Emits `ForecastTick { city, date, mu, sigma, source, fetched_at_ns }`. |
| `watchers/gamma_events.rs` | 5-min poll of `/events?slug=highest-temperature-in-{city}-on-{month}-{d}-{year}` for every watchlist entry. Parses each child market via `scanner::parse_temperature_slug` (already exists). Emits `EventSnapshot { event_slug, buckets: Vec<BucketInfo>, fetched_at_ns }`. Also owns the subscription-add side of the WS subscriber (§3.4). |
| `watchers/clob_book.rs` | CLOB WS `market` channel client. Maintains an authoritative `HashSet<TokenId>` of subscribed tokens (replays it after reconnect — see §3.4). Emits `BookUpdate { token_id, best_bid, best_ask, asks_ladder, ts }`. Dedicated from Phase 2's `clob_ws.rs` to avoid coupling. |
| `watchers/clob_user.rs` | CLOB WS `user` channel subscriber for own orders. Emits `FillEvent`, `CancelAck`, `OrderPlaced` events. |
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

`gamma_events.rs` discovers new NO token IDs. `clob_book.rs` owns the WebSocket. They are coupled via an mpsc `SubCmd::{Add(token), Remove(token)}`:

```
gamma_events ── SubCmd ──▶ clob_book (owns the WS write side)
                           │
                           ├─ updates local HashSet<TokenId>
                           ├─ sends subscribe frame over existing WS
                           └─ on reconnect: replays ENTIRE HashSet (authoritative)
```

**Critical reconnect behavior:** Polymarket's CLOB WS does not persist subscriptions across disconnects. `clob_book.rs` is the only task that knows the full subscribed set. The gamma poller will not retransmit; the reconnect path must replay from the local authoritative `HashSet`. Unit test this by killing the WS connection mid-run and asserting all prior tokens re-subscribe before the first post-reconnect frame is accepted.

No `Arc<RwLock<HashSet>>` shared between tasks — the channel pattern matches every other watcher in this codebase.

### 3.5 Forecast σ: primary vs. fallback

- **Primary path:** Open-Meteo `/v1/ensemble?models=ecmwf_ifs04,gfs_seamless,icon_seamless&latitude=...&longitude=...&daily=temperature_2m_max&start_date=...&end_date=...`. Parse all members, compute cross-model sample standard deviation, multiply by `sigma_scale` (default 1.0). This is the live σ.
- **Fallback path:** hard-coded table by `days_ahead` and unit. Used when ensemble fails 3 consecutive polls, or `/v1/ensemble` returns < 3 members.
- **Calibration log:** every ForecastTick writes a row to `logs/forecast_sigma.parquet`:
  `(city, date, forecast_age_hours, hardcoded_sigma, ensemble_sigma, actual_observed_tmax, residual)`.
  After 14 days of live data, compute `σ_empirical = stdev(residual)` grouped by `days_ahead` and compare. If ensemble is systematically tight, bump `sigma_scale` (typical correction: 1.15-1.35 for 1-2 day forecasts).
- **Rationale:** a 30% error in σ roughly doubles the tail-bucket probability error. A 5% edge floor under a wrong σ picks up fake signals and we lose real money. Run the calibration log before turning live on.

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
NO_EDGE_ENABLED=false                     # kill-switch
NO_EDGE_MIN_EDGE_BPS=500
NO_EDGE_REPOST_THRESHOLD_CENTS=2
NO_EDGE_REPOST_COOLDOWN_SECS=5
NO_EDGE_MAX_NOTIONAL_PER_MARKET_USDC=150
NO_EDGE_MAX_NOTIONAL_PER_EVENT_USDC=500
NO_EDGE_MAX_TOTAL_DEPLOYED_USDC=2000
NO_EDGE_MAX_OPEN_ORDERS=60
NO_EDGE_FORECAST_POLL_SECS=1800
NO_EDGE_GAMMA_POLL_SECS=300
NO_EDGE_FORECAST_STALENESS_KILL_SECS=5400
NO_EDGE_SIGMA_SCALE=1.0                   # calibration multiplier
NO_EDGE_SIGMA_SOURCE=ensemble             # "ensemble" | "hardcoded"
NO_EDGE_CITIES=nyc,atlanta,seattle,dallas,miami,chicago,denver,san-francisco,los-angeles,houston,lucknow
NO_EDGE_LOOKAHEAD_DAYS=2
NO_EDGE_PAPER_LOG_PATH=paper_no_edge.csv
```

## 7. Risks & open questions

### 7.1 Strategy decay
The strategy relies on the **absence** of other market-makers. If another bot starts posting NO at `p_no − 4.99%`, our edge compresses to ~0 in days. Plan:
- Budget a 2-4 week honeymoon period. Revisit ROI weekly.
- If ROI stays > 10%/cycle through week 4, consider adding YES-side asks (symmetric edge where forecast says YES > market) to double effective capacity.
- If ROI drops below 3%, switch to less-liquid secondary cities (London, Paris, Tokyo — need climo + station map bootstrap per §3.2 `climo.rs`).

### 7.2 Forecast σ error
A 30% σ error roughly doubles the tail-bucket probability error, which at the 5% edge floor means most signals become noise. Mitigations listed in §3.5:
- Ensemble σ as primary, hard-coded as fallback
- 14-day calibration log before trusting the multiplier
- Wide `sigma_scale` config knob for manual correction

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
| 4 | `watchers/forecast.rs` — Open-Meteo `/v1/forecast` poll, hard-coded σ fallback path, log-only output | #3 | ~200 | P0 |
| 5 | `watchers/gamma_events.rs` — `/events?slug=…` poll, slug parse reuse, subscription emit channel | — | ~250 | P0 |
| 6 | `watchers/clob_book.rs` — WS subscribe/unsubscribe + reconnect replay, `BookUpdate` emit | — | ~350 | P0 |
| 7 | `edge_book.rs` actor + `EdgeCmd` enum + dirty-bit ticker + `EdgeSignal` emit | #3 #4 #5 #6 | ~400 | P0 |
| 8 | `quoter.rs` actor + state machine + `OrderSink` trait (stub impl) | #1 #7 | ~350 | P0 |
| 9 | `portfolio.rs` + `NoEdgeState` + facade mpsc + `no_edge_state.json` serde w/ schema_version | — | ~250 | P0 |
| 10 | Paper-mode extension: `PaperRestingOrder`, book-cross fill sim, adverse-fill tag, `paper_no_edge.csv` | #8 | ~250 | P0 |
| 11 | `main.rs` wiring: gated `no_edge::spawn_all`, new `tokio::select!` arms, ctrl-c drain | #8 #9 #10 | ~100 | P0 |
| 12 | `watchers/clob_user.rs` — user-channel fill stream, wire into EdgeBook + portfolio | #6 #9 | ~250 | P0 |
| 13 | Startup recovery path: `list_open_orders` → cross-ref `no_edge_state.json` → adopt/cancel/reconcile | #1 #9 | ~200 | P0 |
| 14 | 14-day paper-mode bake + calibration-log analysis (`sigma_scale` fitting) | #11 | N/A | P0 |
| 15 | Open-Meteo ensemble σ primary path + calibration log schema | #4 | ~200 | P1 |
| 16 | Python settlement cron + flock + atomic rename | #9 | ~150 Py | P1 |
| 17 | Telegram alert hookups: new fill, forecast-staleness kill, orphan cancel on restart | #11 #12 | ~80 | P1 |
| 18 | Metrics: per-tick dirty-token count, WS reconnect counter, cancel-vs-fill race counter | #11 | ~120 | P1 |
| 19 | Regression test: self-cross detection against presigner cache | #8 | ~80 | P1 |
| 20 | Operational runbook: how to read the paper CSV, kill-switch procedure, `sigma_scale` tuning | #14 | doc | P2 |

**P0 subtotal:** ~3,000 LoC Rust + paper-mode. 1-2 engineer-weeks to working paper mode.
**Live bring-up:** only after #14 and at least 7 consecutive days of paper-mode P&L within 20% of model prediction.

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
