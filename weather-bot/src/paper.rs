//! Paper-trading engine.
//!
//! Drop-in replacement for `mint_executor` + `presigner` when
//! `config.paper_mode == true`. Runs the **same decision path** as live
//! (event-driven, same slug filter, same mint sizing, same dump logic) but:
//!
//!   - does NOT broadcast any Polygon transactions
//!   - does NOT POST any CLOB orders
//!   - fetches REAL orderbook depth for each bucket it would dump into
//!   - walks the book with slippage + taker fees to compute realistic fill
//!   - tracks a virtual USDC bankroll with a concurrent-events cap
//!   - appends every event (mint, dump, resolution) to a CSV log
//!
//! This means: when we flip the bot to live, the ROI we measured in paper
//! should match within slippage variance. No surprises.
//!
//! # Fee assumptions (verify empirically before going live)
//!
//!   CLOB taker fee   = 0 bps   (confirmed: Polymarket sets feeRateBps=0 on fills)
//!   CLOB maker fee   = 0 bps
//!   Gas (split)      = $0.008  (200k gas × 50 gwei × $0.65 MATIC)
//!   Gas (convert)    = $0.012  (300k × 50 × $0.65)
//!   Gas (redeem)     = $0.006  (150k × 50 × $0.65)
//!   NegRisk feeBips  = 0 bps   (paper default; real value is per-market — read
//!                              from NegRiskAdapter.MarketPrepared when the
//!                              onchain watcher decodes logs)
//!
//! Rebate income (CLOB rewards ≈ 23 USDC/day per qualifying market) is NOT
//! modeled. Consider it upside — the measured ROI is a floor.

use crate::types::{BucketInfo, WeatherEvent};
use alloy_primitives::U256;
use anyhow::{anyhow, Context, Result};
use polymarket_client_sdk::clob::types::Side;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

// -------- Fee constants (tunable via config) --------

/// Basis points the CLOB taker side pays. Real value: 0 on Polymarket today.
pub const DEFAULT_TAKER_FEE_BPS: u16 = 0;
/// Gas cost in USDC of a single splitPosition tx on Polygon.
pub const GAS_COST_SPLIT_USDC: f64 = 0.008;
/// Gas cost in USDC of a single convertPositions tx on Polygon.
pub const GAS_COST_CONVERT_USDC: f64 = 0.012;
/// Gas cost in USDC of a redeemPositions tx on Polygon.
pub const GAS_COST_REDEEM_USDC: f64 = 0.006;

// -------- Paper account state --------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub event_slug: String,
    pub city: String,
    pub resolution_date: String,
    pub bucket_label: String,
    pub token_id: String,
    /// Shares remaining (held as a tail lottery ticket).
    pub shares_held: f64,
    /// Effective cost basis per share in USDC, AFTER deducting dump proceeds
    /// from the mint outlay. Can be negative (we banked more than we spent).
    pub effective_cost_per_share: f64,
    /// Unix seconds when minted.
    pub minted_at: i64,
    /// Unix seconds when the event resolves (event endDate).
    pub resolves_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperAccount {
    pub bankroll_usdc: f64,
    pub starting_bankroll: f64,
    /// Open events → total shares held across all their buckets.
    /// Used to enforce `max_concurrent_events`.
    pub open_event_slugs: Vec<String>,
    pub open_positions: Vec<PaperPosition>,
    pub closed_positions: Vec<PaperPosition>,

    // Cumulative telemetry (never zeros out)
    pub cum_mint_count: u64,
    pub cum_mint_usdc: f64,
    pub cum_dump_count: u64,
    pub cum_dump_usdc: f64,
    pub cum_gas_paid: f64,
    pub cum_taker_fees: f64,
    pub cum_realized_pnl: f64,
}

impl Default for PaperAccount {
    fn default() -> Self {
        Self {
            bankroll_usdc: 0.0,
            starting_bankroll: 0.0,
            open_event_slugs: Vec::new(),
            open_positions: Vec::new(),
            closed_positions: Vec::new(),
            cum_mint_count: 0,
            cum_mint_usdc: 0.0,
            cum_dump_count: 0,
            cum_dump_usdc: 0.0,
            cum_gas_paid: 0.0,
            cum_taker_fees: 0.0,
            cum_realized_pnl: 0.0,
        }
    }
}

impl PaperAccount {
    pub fn new(starting_bankroll: f64) -> Self {
        Self {
            bankroll_usdc: starting_bankroll,
            starting_bankroll,
            ..Self::default()
        }
    }

    pub fn net_pnl(&self) -> f64 {
        self.bankroll_usdc - self.starting_bankroll + self.mark_to_market_open_value()
    }

    pub fn mark_to_market_open_value(&self) -> f64 {
        // Conservative: tail legs marked at zero. Underestimates P&L by the
        // value of the held lottery tickets.
        0.0
    }

    pub fn roi_pct(&self) -> f64 {
        if self.starting_bankroll == 0.0 {
            return 0.0;
        }
        100.0 * self.net_pnl() / self.starting_bankroll
    }
}

// -------- Orderbook fetcher + walker --------

#[derive(Deserialize, Debug)]
struct ClobBook {
    #[serde(default)]
    bids: Vec<ClobLevel>,
    #[serde(default)]
    asks: Vec<ClobLevel>,
}

#[derive(Deserialize, Debug)]
struct ClobLevel {
    price: String,
    size: String,
}

/// Fill result from walking the bid side of the book.
#[derive(Debug, Clone)]
pub struct WalkedFill {
    pub shares_filled: f64,
    pub gross_proceeds_usdc: f64,
    pub avg_price: f64,
    pub levels_consumed: usize,
    pub capped_by_depth: bool,
}

pub async fn fetch_book(http: &Client, clob_url: &str, token_id: &str) -> Result<ClobBook> {
    let url = format!("{}/book?token_id={}", clob_url, token_id);
    let resp = http
        .get(&url)
        .header("User-Agent", "PolyWeatherBot/paper")
        .send()
        .await?;
    let text = resp.text().await?;
    let book: ClobBook =
        serde_json::from_str(&text).context("failed to parse CLOB book response")?;
    Ok(book)
}

/// Walk the BID side of the book (we're selling), taking as many levels as
/// needed to fill `target_shares` or exhaust the book. Returns the realized
/// fill including VWAP and whether we were depth-capped.
pub fn walk_bids_for_sell(book: &ClobBook, target_shares: f64) -> WalkedFill {
    // Sort bids descending (best price first)
    let mut levels: Vec<(f64, f64)> = book
        .bids
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            let s = l.size.parse::<f64>().ok()?;
            if p > 0.0 && s > 0.0 {
                Some((p, s))
            } else {
                None
            }
        })
        .collect();
    levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut remaining = target_shares;
    let mut filled = 0.0;
    let mut gross = 0.0;
    let mut levels_consumed = 0;

    for (price, size) in &levels {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(*size);
        filled += take;
        gross += take * price;
        remaining -= take;
        levels_consumed += 1;
    }

    let avg_price = if filled > 0.0 { gross / filled } else { 0.0 };
    WalkedFill {
        shares_filled: filled,
        gross_proceeds_usdc: gross,
        avg_price,
        levels_consumed,
        capped_by_depth: remaining > 0.0,
    }
}

// -------- CSV append logger --------

/// Columns (pipe-separated for easy grep):
///   timestamp_unix | event_slug | kind | detail_json | bankroll_after
pub struct PaperLog {
    path: PathBuf,
    counter: AtomicU64,
}

impl PaperLog {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            counter: AtomicU64::new(0),
        }
    }

    fn append(&self, event_slug: &str, kind: &str, detail: serde_json::Value, bankroll: f64) {
        use std::io::Write;
        let ts = chrono::Utc::now().timestamp();
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let row = format!(
            "{}|{}|{}|{}|{}|{:.4}\n",
            n, ts, event_slug, kind, detail, bankroll
        );
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            let _ = f.write_all(row.as_bytes());
        }
    }
}

// -------- Resting maker orders (Phase 3 ticket #9) --------

/// One synthetic maker order that the no-edge farmer has "posted" to a
/// `PaperEngine`. Lives in `PaperEngine::resting_orders` until the event loop
/// feeds a `BookUpdate` that crosses it (fill) or a `cancel_order` call
/// removes it.
///
/// `fair_p_no_at_post` + `min_edge_at_post` are snapshots of the price-of-NO
/// and edge threshold at the instant of posting. When a fill eventually
/// happens we compare against the current fair — if it has moved inside the
/// `min_edge` guardrail we flag the fill as adverse and log it to
/// `paper_no_edge.csv` so backtests can measure how often the maker was
/// picked off.
#[derive(Debug, Clone)]
pub struct PaperRestingOrder {
    pub order_id: String,
    pub token_id: U256,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub posted_at_ns: u128,
    pub fair_p_no_at_post: f64,
    pub min_edge_at_post: f64,
    pub filled: bool,
    pub filled_shares: f64,
    pub filled_avg_price: f64,
    pub filled_at_ns: Option<u128>,
    pub adverse_fill: bool,
}

// -------- Paper engine (the thing main.rs calls) --------

pub struct PaperEngine {
    http: Client,
    clob_url: String,
    account: Mutex<PaperAccount>,
    log: PaperLog,
    paper_log_path: PathBuf,
    max_concurrent_events: usize,
    mint_amount_usdc: f64,
    min_dump_price: f64,
    dump_fraction: f64,
    taker_fee_bps: u16,
    /// Synthetic maker orders that have been "posted" via the `OrderSink`
    /// impl. Keyed by the synth `paper-<uuid>` order id.
    resting_orders: Mutex<HashMap<String, PaperRestingOrder>>,
}

impl PaperEngine {
    pub fn new(
        clob_url: String,
        starting_bankroll: f64,
        max_concurrent_events: usize,
        mint_amount_usdc: f64,
        min_dump_price: f64,
        dump_fraction: f64,
        log_path: PathBuf,
    ) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        let paper_log_path = log_path.clone();
        Self {
            http,
            clob_url,
            account: Mutex::new(PaperAccount::new(starting_bankroll)),
            log: PaperLog::new(log_path),
            paper_log_path,
            max_concurrent_events,
            mint_amount_usdc,
            min_dump_price,
            dump_fraction,
            taker_fee_bps: DEFAULT_TAKER_FEE_BPS,
            resting_orders: Mutex::new(HashMap::new()),
        }
    }

    /// Whole pipeline for one weather event: check bankroll + concurrent cap,
    /// mint, dump hot legs (walking real books), record residual tail legs,
    /// log everything. This is what `main.rs` calls in place of the sim
    /// mint + presign + flush.
    pub async fn run_event(&self, event: &WeatherEvent, now_unix: i64) -> Result<()> {
        // -- 1. Gate: bankroll + concurrent cap --
        {
            let acct = self.account.lock().await;
            let need = self.mint_amount_usdc * 2.0 + GAS_COST_SPLIT_USDC + GAS_COST_CONVERT_USDC;
            if acct.bankroll_usdc < need {
                tracing::info!(
                    "[PAPER] SKIP {} — bankroll ${:.2} < need ${:.2}",
                    event.event_slug,
                    acct.bankroll_usdc,
                    need
                );
                self.log.append(
                    &event.event_slug,
                    "skip_bankroll",
                    serde_json::json!({ "bankroll": acct.bankroll_usdc, "need": need }),
                    acct.bankroll_usdc,
                );
                return Ok(());
            }
            if acct.open_event_slugs.len() >= self.max_concurrent_events {
                tracing::info!(
                    "[PAPER] SKIP {} — at concurrent cap ({} events open)",
                    event.event_slug,
                    acct.open_event_slugs.len()
                );
                self.log.append(
                    &event.event_slug,
                    "skip_concurrent_cap",
                    serde_json::json!({ "open": acct.open_event_slugs.len() }),
                    acct.bankroll_usdc,
                );
                return Ok(());
            }
        }

        // -- 2. Mint (virtual) --
        let mint_total = self.mint_amount_usdc * 2.0;
        let gas_mint = GAS_COST_SPLIT_USDC + GAS_COST_CONVERT_USDC;
        {
            let mut acct = self.account.lock().await;
            acct.bankroll_usdc -= mint_total + gas_mint;
            acct.cum_mint_count += 1;
            acct.cum_mint_usdc += mint_total;
            acct.cum_gas_paid += gas_mint;
            acct.open_event_slugs.push(event.event_slug.clone());
            self.log.append(
                &event.event_slug,
                "mint",
                serde_json::json!({
                    "mint_total_usdc": mint_total,
                    "gas_usdc": gas_mint,
                    "bucket_count": event.buckets.len(),
                }),
                acct.bankroll_usdc,
            );
        }

        // -- 3. Walk each bucket's orderbook and dump the hot legs --
        let shares_per_bucket = self.mint_amount_usdc; // $X mint → X shares per outcome
        let target_dump = shares_per_bucket * self.dump_fraction;

        let mut total_proceeds = 0.0;
        let mut total_fees = 0.0;
        let mut residual_legs: Vec<PaperPosition> = Vec::new();
        let resolves_at = parse_resolution_date(&event.resolution_date, now_unix);

        for bucket in &event.buckets {
            let token_id = bucket.token_id_yes.to_string();
            let book = match fetch_book(&self.http, &self.clob_url, &token_id).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        "[PAPER] book fetch failed for {} {}: {} — treating as dust",
                        event.event_slug,
                        bucket.bucket_label,
                        e
                    );
                    residual_legs.push(PaperPosition {
                        event_slug: event.event_slug.clone(),
                        city: event.city.clone(),
                        resolution_date: event.resolution_date.clone(),
                        bucket_label: bucket.bucket_label.clone(),
                        token_id,
                        shares_held: shares_per_bucket,
                        effective_cost_per_share: self.mint_amount_usdc / (event.buckets.len() as f64 * shares_per_bucket).max(1.0),
                        minted_at: now_unix,
                        resolves_at,
                    });
                    continue;
                }
            };

            let walked = walk_bids_for_sell(&book, target_dump);

            if walked.shares_filled < 1.0 || walked.avg_price < self.min_dump_price {
                // Too thin or too cheap — skip dumping, keep full position
                residual_legs.push(PaperPosition {
                    event_slug: event.event_slug.clone(),
                    city: event.city.clone(),
                    resolution_date: event.resolution_date.clone(),
                    bucket_label: bucket.bucket_label.clone(),
                    token_id,
                    shares_held: shares_per_bucket,
                    effective_cost_per_share: (mint_total + gas_mint)
                        / (event.buckets.len() as f64 * shares_per_bucket).max(1.0),
                    minted_at: now_unix,
                    resolves_at,
                });
                continue;
            }

            let fee_usdc = walked.gross_proceeds_usdc * (self.taker_fee_bps as f64 / 10_000.0);
            let net_proceeds = walked.gross_proceeds_usdc - fee_usdc;
            total_proceeds += net_proceeds;
            total_fees += fee_usdc;

            let remaining_shares = shares_per_bucket - walked.shares_filled;
            if remaining_shares > 0.0 {
                residual_legs.push(PaperPosition {
                    event_slug: event.event_slug.clone(),
                    city: event.city.clone(),
                    resolution_date: event.resolution_date.clone(),
                    bucket_label: bucket.bucket_label.clone(),
                    token_id: token_id.clone(),
                    shares_held: remaining_shares,
                    effective_cost_per_share: 0.0, // already paid via mint, tail is free
                    minted_at: now_unix,
                    resolves_at,
                });
            }

            self.log.append(
                &event.event_slug,
                "dump",
                serde_json::json!({
                    "bucket": bucket.bucket_label,
                    "target_shares": target_dump,
                    "filled": walked.shares_filled,
                    "avg_price": walked.avg_price,
                    "gross_proceeds": walked.gross_proceeds_usdc,
                    "fee": fee_usdc,
                    "net_proceeds": net_proceeds,
                    "levels_consumed": walked.levels_consumed,
                    "capped_by_depth": walked.capped_by_depth,
                    "residual_shares": remaining_shares,
                }),
                -1.0, // placeholder; bankroll update happens in the cumulative block below
            );
        }

        // -- 4. Credit proceeds and record residual legs --
        {
            let mut acct = self.account.lock().await;
            acct.bankroll_usdc += total_proceeds;
            acct.cum_dump_count += event.buckets.len() as u64;
            acct.cum_dump_usdc += total_proceeds;
            acct.cum_taker_fees += total_fees;

            for leg in residual_legs {
                acct.open_positions.push(leg);
            }

            let cycle_pnl = total_proceeds - mint_total - gas_mint - total_fees;
            acct.cum_realized_pnl += cycle_pnl;

            self.log.append(
                &event.event_slug,
                "cycle_summary",
                serde_json::json!({
                    "mint_out": mint_total,
                    "gas": gas_mint,
                    "dump_proceeds_gross": total_proceeds + total_fees,
                    "dump_fees": total_fees,
                    "dump_proceeds_net": total_proceeds,
                    "cycle_pnl": cycle_pnl,
                    "roi_pct": acct.roi_pct(),
                    "bankroll_after": acct.bankroll_usdc,
                    "open_events": acct.open_event_slugs.len(),
                    "tail_legs_held": acct.open_positions.len(),
                }),
                acct.bankroll_usdc,
            );

            tracing::info!(
                "[PAPER] CYCLE {}: mint=-${:.2} gas=-${:.2} fees=-${:.2} proceeds=+${:.2} net={:+.2} | bankroll=${:.2} ROI={:.2}%",
                event.event_slug,
                mint_total,
                gas_mint,
                total_fees,
                total_proceeds,
                cycle_pnl,
                acct.bankroll_usdc,
                acct.roi_pct()
            );
        }

        Ok(())
    }

    pub async fn snapshot(&self) -> PaperAccount {
        self.account.lock().await.clone()
    }

    // -------- Resting maker path (Phase 3 ticket #9) --------

    /// Append one row to the adverse-fill CSV. Columns (comma-separated):
    ///   ts_ns,token_id,side,price,fill_size,fair_at_post,fair_at_fill,
    ///   edge_at_post,edge_at_fill,adverse_fill
    fn append_no_edge_row(
        &self,
        ts_ns: u128,
        token_id: &U256,
        side: Side,
        price: f64,
        fill_size: f64,
        fair_at_post: f64,
        fair_at_fill: f64,
        edge_at_post: f64,
        edge_at_fill: f64,
        adverse_fill: bool,
    ) {
        use std::io::Write;
        let mut path = self.paper_log_path.clone();
        let new_name = match path.file_name() {
            Some(n) => {
                let mut s = n.to_os_string();
                s.push(".no_edge.csv");
                s
            }
            None => std::ffi::OsString::from("paper.no_edge.csv"),
        };
        path.set_file_name(new_name);

        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }

        let side_str = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
            _ => "UNKNOWN",
        };
        let row = format!(
            "{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{}\n",
            ts_ns,
            token_id,
            side_str,
            price,
            fill_size,
            fair_at_post,
            fair_at_fill,
            edge_at_post,
            edge_at_fill,
            adverse_fill
        );
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            let _ = f.write_all(row.as_bytes());
        }
    }

    /// Synthetic "post" for the paper OrderSink impl. Bankroll-gated: if the
    /// virtual account can't cover `size * price`, returns an error. Otherwise
    /// debits the bankroll, inserts a `PaperRestingOrder`, and returns a synth
    /// `paper-<uuid>` order id.
    pub async fn paper_post_limit_order(
        &self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Side,
        fair_p_no_at_post: f64,
        min_edge_at_post: f64,
    ) -> Result<String> {
        let required = size * price;
        {
            let mut acct = self.account.lock().await;
            if acct.bankroll_usdc < required {
                return Err(anyhow!(
                    "paper_post_limit_order: insufficient bankroll ${:.4} < required ${:.4}",
                    acct.bankroll_usdc,
                    required
                ));
            }
            acct.bankroll_usdc -= required;
        }

        let order_id = format!("paper-{}", uuid::Uuid::new_v4());
        let posted_at_ns = crate::types::now_ns();
        let order = PaperRestingOrder {
            order_id: order_id.clone(),
            token_id,
            side,
            price,
            size,
            posted_at_ns,
            fair_p_no_at_post,
            min_edge_at_post,
            filled: false,
            filled_shares: 0.0,
            filled_avg_price: 0.0,
            filled_at_ns: None,
            adverse_fill: false,
        };
        self.resting_orders
            .lock()
            .await
            .insert(order_id.clone(), order);
        Ok(order_id)
    }

    /// Idempotent cancel. If the order exists and isn't fully filled, refund
    /// the (remaining) reserved bankroll. Unknown ids are a debug log + Ok.
    pub async fn paper_cancel_order(&self, order_id: &str) -> Result<()> {
        let removed = self.resting_orders.lock().await.remove(order_id);
        match removed {
            Some(order) => {
                if !order.filled {
                    let remaining = (order.size - order.filled_shares).max(0.0);
                    let refund = remaining * order.price;
                    if refund > 0.0 {
                        self.account.lock().await.bankroll_usdc += refund;
                    }
                }
                Ok(())
            }
            None => {
                tracing::debug!(
                    "[PAPER] cancel_order: unknown id {} (idempotent noop)",
                    order_id
                );
                Ok(())
            }
        }
    }

    /// Called from the event loop whenever a `BookUpdate` arrives for a token
    /// that may have resting paper orders. Walks every order matching
    /// `update.token_id`, fills any whose price is crossed by the opposing
    /// top of book, and flags the fill as adverse if the current fair has
    /// moved inside the `min_edge_at_post` guardrail.
    pub async fn on_book_update(
        &self,
        update: &crate::types::BookUpdate,
        fair_p_no_at_fill: f64,
    ) {
        let mut orders = self.resting_orders.lock().await;
        let now = crate::types::now_ns();
        for order in orders.values_mut() {
            if order.token_id != update.token_id || order.filled {
                continue;
            }
            let remaining = order.size - order.filled_shares;
            if remaining <= 0.0 {
                continue;
            }

            let (crosses, available) = match order.side {
                // Selling at `order.price`: fills when best_bid >= price,
                // filled against bid ladder levels >= price.
                Side::Sell => {
                    let bb = update.best_bid.unwrap_or(0.0);
                    if bb < order.price {
                        (false, 0.0)
                    } else {
                        let avail: f64 = update
                            .bids_ladder
                            .iter()
                            .filter(|(p, _)| *p >= order.price)
                            .map(|(_, s)| *s)
                            .sum();
                        (true, avail)
                    }
                }
                // Buying at `order.price`: fills when best_ask <= price,
                // filled against ask ladder levels <= price.
                Side::Buy => {
                    let ba = update.best_ask.unwrap_or(f64::INFINITY);
                    if ba > order.price {
                        (false, 0.0)
                    } else {
                        let avail: f64 = update
                            .asks_ladder
                            .iter()
                            .filter(|(p, _)| *p <= order.price)
                            .map(|(_, s)| *s)
                            .sum();
                        (true, avail)
                    }
                }
                _ => (false, 0.0),
            };

            if !crosses || available <= 0.0 {
                continue;
            }

            let fill_size = available.min(remaining);
            if fill_size <= 0.0 {
                continue;
            }

            // Running weighted average over cumulative fill size. The maker
            // nominally fills at its posted price (that's the whole point of
            // resting), so treat the avg as the order price.
            let prev_shares = order.filled_shares;
            let new_shares = prev_shares + fill_size;
            let new_avg = if new_shares > 0.0 {
                (order.filled_avg_price * prev_shares + order.price * fill_size) / new_shares
            } else {
                order.price
            };
            order.filled_shares = new_shares;
            order.filled_avg_price = new_avg;
            order.filled_at_ns = Some(now);
            if order.filled_shares >= order.size - 1e-9 {
                order.filled = true;
            }

            // Adverse fill check. For a Sell resting at `order.price`, the
            // order was posted because `fair_p_no_at_post + min_edge <=
            // order.price` (we were getting >= edge over fair). It becomes
            // adverse if the current fair has moved up such that
            // `fair_p_no_at_fill + min_edge > order.price`, i.e.
            // `fair_p_no_at_fill > order.price - min_edge`.
            //
            // The spec's formulation `fair_p_no_at_fill < order.price +
            // order.min_edge_at_post` is the Buy-side check (selling-the-NO
            // frame). We encode both directions symmetrically below.
            let adverse = match order.side {
                Side::Sell => {
                    fair_p_no_at_fill > order.price - order.min_edge_at_post
                }
                Side::Buy => {
                    fair_p_no_at_fill < order.price + order.min_edge_at_post
                }
                _ => false,
            };
            if adverse {
                order.adverse_fill = true;
            }

            let edge_at_post = match order.side {
                Side::Sell => order.price - order.fair_p_no_at_post,
                Side::Buy => order.fair_p_no_at_post - order.price,
                _ => 0.0,
            };
            let edge_at_fill = match order.side {
                Side::Sell => order.price - fair_p_no_at_fill,
                Side::Buy => fair_p_no_at_fill - order.price,
                _ => 0.0,
            };

            self.append_no_edge_row(
                now,
                &order.token_id,
                order.side,
                order.price,
                fill_size,
                order.fair_p_no_at_post,
                fair_p_no_at_fill,
                edge_at_post,
                edge_at_fill,
                adverse,
            );
        }
    }

    #[cfg(test)]
    async fn resting_order_count(&self) -> usize {
        self.resting_orders.lock().await.len()
    }

    #[cfg(test)]
    async fn resting_order_clone(&self, order_id: &str) -> Option<PaperRestingOrder> {
        self.resting_orders.lock().await.get(order_id).cloned()
    }

    #[cfg(test)]
    async fn bankroll(&self) -> f64 {
        self.account.lock().await.bankroll_usdc
    }
}

fn parse_resolution_date(date: &str, now_unix: i64) -> i64 {
    // YYYY-MM-DD → Unix seconds at 12:00 UTC that day (Polymarket weather
    // resolves at noon UTC).
    if date.len() != 10 {
        return now_unix + 86_400;
    }
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return now_unix + 86_400;
    }
    let y: i32 = parts[0].parse().unwrap_or(2026);
    let m: u32 = parts[1].parse().unwrap_or(1);
    let d: u32 = parts[2].parse().unwrap_or(1);
    chrono::NaiveDate::from_ymd_opt(y, m, d)
        .and_then(|nd| nd.and_hms_opt(12, 0, 0))
        .map(|ndt| ndt.and_utc().timestamp())
        .unwrap_or(now_unix + 86_400)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_bid_book_simple() {
        let book = ClobBook {
            bids: vec![
                ClobLevel {
                    price: "0.38".into(),
                    size: "28".into(),
                },
                ClobLevel {
                    price: "0.33".into(),
                    size: "12".into(),
                },
                ClobLevel {
                    price: "0.32".into(),
                    size: "5".into(),
                },
                ClobLevel {
                    price: "0.31".into(),
                    size: "100".into(),
                },
            ],
            asks: vec![],
        };
        let w = walk_bids_for_sell(&book, 62.0);
        // 28 + 12 + 5 = 45 from top 3; 17 more from the $0.31 wall
        assert_eq!(w.shares_filled, 62.0);
        assert_eq!(w.levels_consumed, 4);
        let expected_gross = 28.0 * 0.38 + 12.0 * 0.33 + 5.0 * 0.32 + 17.0 * 0.31;
        assert!((w.gross_proceeds_usdc - expected_gross).abs() < 0.0001);
        assert!(!w.capped_by_depth);
    }

    #[test]
    fn walks_bid_book_depth_capped() {
        let book = ClobBook {
            bids: vec![ClobLevel {
                price: "0.10".into(),
                size: "5".into(),
            }],
            asks: vec![],
        };
        let w = walk_bids_for_sell(&book, 62.0);
        assert_eq!(w.shares_filled, 5.0);
        assert!(w.capped_by_depth);
    }

    #[test]
    fn walks_empty_book_returns_zero() {
        let book = ClobBook {
            bids: vec![],
            asks: vec![],
        };
        let w = walk_bids_for_sell(&book, 62.0);
        assert_eq!(w.shares_filled, 0.0);
        assert!(w.capped_by_depth);
    }

    #[test]
    fn paper_account_roi_math() {
        let mut acct = PaperAccount::new(1000.0);
        acct.bankroll_usdc = 1100.0;
        assert!((acct.net_pnl() - 100.0).abs() < 0.001);
        assert!((acct.roi_pct() - 10.0).abs() < 0.001);
    }

    // -------- Resting-maker tests (Phase 3 ticket #9) --------

    use crate::types::BookUpdate;

    fn test_paper_engine(bankroll: f64) -> PaperEngine {
        // Unique temp log path per engine so parallel tests don't collide.
        let mut path = std::env::temp_dir();
        path.push(format!(
            "paper-test-{}-{}.log",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        PaperEngine::new(
            "https://clob.polymarket.com".to_string(),
            bankroll,
            5,
            62.0,
            0.05,
            0.8,
            path,
        )
    }

    fn book_update(
        token_id: U256,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) -> BookUpdate {
        BookUpdate {
            token_id,
            best_bid,
            best_ask,
            asks_ladder: asks,
            bids_ladder: bids,
            fetched_at_ns: 0,
        }
    }

    #[tokio::test]
    async fn paper_post_creates_resting_order_and_debits_bankroll() {
        let engine = test_paper_engine(1000.0);
        let id = engine
            .paper_post_limit_order(U256::from(42), 0.90, 100.0, Side::Sell, 0.50, 0.05)
            .await
            .expect("post should succeed");
        assert!(id.starts_with("paper-"));
        assert_eq!(engine.resting_order_count().await, 1);
        // Reserved: 100 * 0.90 = 90.0
        assert!((engine.bankroll().await - 910.0).abs() < 1e-6);
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert_eq!(order.token_id, U256::from(42));
        assert_eq!(order.size, 100.0);
        assert_eq!(order.price, 0.90);
        assert!(!order.filled);
        assert_eq!(order.filled_shares, 0.0);
    }

    #[tokio::test]
    async fn paper_post_rejects_on_insufficient_bankroll() {
        let engine = test_paper_engine(50.0);
        let result = engine
            .paper_post_limit_order(U256::from(1), 0.90, 100.0, Side::Sell, 0.50, 0.05)
            .await;
        assert!(result.is_err());
        assert_eq!(engine.resting_order_count().await, 0);
        // Bankroll untouched.
        assert!((engine.bankroll().await - 50.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn paper_cancel_refunds_bankroll() {
        let engine = test_paper_engine(1000.0);
        let id = engine
            .paper_post_limit_order(U256::from(7), 0.80, 50.0, Side::Sell, 0.40, 0.05)
            .await
            .unwrap();
        assert!((engine.bankroll().await - 960.0).abs() < 1e-6);
        engine.paper_cancel_order(&id).await.unwrap();
        assert_eq!(engine.resting_order_count().await, 0);
        assert!((engine.bankroll().await - 1000.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn paper_cancel_unknown_id_is_noop() {
        let engine = test_paper_engine(1000.0);
        let result = engine.paper_cancel_order("paper-nonexistent").await;
        assert!(result.is_ok());
        assert!((engine.bankroll().await - 1000.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn paper_on_book_update_fills_sell_when_bid_crosses() {
        let engine = test_paper_engine(1000.0);
        let tok = U256::from(99);
        let id = engine
            .paper_post_limit_order(tok, 0.85, 50.0, Side::Sell, 0.50, 0.05)
            .await
            .unwrap();
        // Bid at 0.86 with 60 shares crosses the 0.85 resting sell fully.
        let upd = book_update(tok, Some(0.86), Some(0.87), vec![(0.86, 60.0)], vec![(0.87, 100.0)]);
        // Current fair still 0.50 → not adverse.
        engine.on_book_update(&upd, 0.50).await;
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert!(order.filled);
        assert!((order.filled_shares - 50.0).abs() < 1e-9);
        assert!((order.filled_avg_price - 0.85).abs() < 1e-9);
        assert!(order.filled_at_ns.is_some());
        assert!(!order.adverse_fill);
    }

    #[tokio::test]
    async fn paper_on_book_update_no_fill_when_bid_below() {
        let engine = test_paper_engine(1000.0);
        let tok = U256::from(11);
        let id = engine
            .paper_post_limit_order(tok, 0.90, 50.0, Side::Sell, 0.50, 0.05)
            .await
            .unwrap();
        // Best bid 0.85 < 0.90 — no fill.
        let upd = book_update(tok, Some(0.85), Some(0.91), vec![(0.85, 200.0)], vec![(0.91, 100.0)]);
        engine.on_book_update(&upd, 0.50).await;
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert!(!order.filled);
        assert_eq!(order.filled_shares, 0.0);
    }

    #[tokio::test]
    async fn paper_on_book_update_partial_fill() {
        let engine = test_paper_engine(1000.0);
        let tok = U256::from(21);
        let id = engine
            .paper_post_limit_order(tok, 0.80, 100.0, Side::Sell, 0.50, 0.05)
            .await
            .unwrap();
        // Only 30 shares at >= 0.80 on the bid side.
        let upd = book_update(
            tok,
            Some(0.81),
            Some(0.82),
            vec![(0.81, 30.0), (0.79, 500.0)],
            vec![(0.82, 100.0)],
        );
        engine.on_book_update(&upd, 0.50).await;
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert!(!order.filled);
        assert!((order.filled_shares - 30.0).abs() < 1e-9);
        // Avg price is still the posted price (maker gets its own price).
        assert!((order.filled_avg_price - 0.80).abs() < 1e-9);
        assert!(order.filled_at_ns.is_some());
    }

    #[tokio::test]
    async fn paper_on_book_update_flags_adverse_fill() {
        let engine = test_paper_engine(1000.0);
        let tok = U256::from(33);
        // Sell @0.80, edge=0.05, fair_at_post=0.70 (edge_at_post = 0.10, well above min).
        let id = engine
            .paper_post_limit_order(tok, 0.80, 50.0, Side::Sell, 0.70, 0.05)
            .await
            .unwrap();
        // Bid at 0.80 crosses. But fair has moved to 0.78 — now price - fair
        // = 0.02 < min_edge_at_post (0.05) → adverse.
        let upd = book_update(tok, Some(0.80), Some(0.81), vec![(0.80, 100.0)], vec![(0.81, 100.0)]);
        engine.on_book_update(&upd, 0.78).await;
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert!(order.filled);
        assert!(order.adverse_fill, "fill should be flagged adverse");
    }

    #[tokio::test]
    async fn paper_on_book_update_ignores_unrelated_token() {
        let engine = test_paper_engine(1000.0);
        let my_tok = U256::from(1);
        let other_tok = U256::from(2);
        let id = engine
            .paper_post_limit_order(my_tok, 0.85, 50.0, Side::Sell, 0.50, 0.05)
            .await
            .unwrap();
        let upd = book_update(
            other_tok,
            Some(0.99),
            Some(1.0),
            vec![(0.99, 1000.0)],
            vec![(1.0, 100.0)],
        );
        engine.on_book_update(&upd, 0.50).await;
        let order = engine.resting_order_clone(&id).await.unwrap();
        assert!(!order.filled);
        assert_eq!(order.filled_shares, 0.0);
    }
}
