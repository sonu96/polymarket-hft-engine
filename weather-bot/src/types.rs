//! Shared types for the hot-path pipeline.
//!
//! This module defines the contract between the on-chain detector (Agent α),
//! the mint/mempool executor (Agent β), and the CLOB order executor (Agent γ).
//! Everything here is deliberately `Clone + Send + Sync` so it can ride the
//! `tokio::sync::mpsc::UnboundedSender` channels without fuss.

use alloy_primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// How a [`WeatherEvent`] entered the pipeline. The on-chain live stream is
/// the canonical path; the `BootstrapReplay` variant is set by
/// [`crate::no_edge::bootstrap`] for events we learned about via the one-shot
/// Gamma snapshot at startup.
///
/// Phase 2 mint-and-dump **must skip** `BootstrapReplay` events — the mint
/// window is 12–36 h pre-settlement and is already gone for anything old
/// enough to appear in the `active=true` snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoverySource {
    /// Default — from `watchers::onchain` live stream.
    OnChain,
    /// From `no_edge::bootstrap` — Phase 2 must skip these.
    BootstrapReplay,
}

impl Default for DiscoverySource {
    fn default() -> Self {
        DiscoverySource::OnChain
    }
}

/// Classification of the CTF market we just detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// NegRisk multi-bucket temperature market (the common case for weather).
    /// All buckets share one `marketId` and are linked via the NegRiskAdapter.
    NegRisk,
    /// Plain binary CTF market — e.g. a standalone "or below"/"or above"
    /// question that was prepared directly on the ConditionalTokens contract.
    BinaryCtf,
}

/// One temperature bucket inside a weather event. For a NegRisk event there
/// will be N of these, one per question index. For a plain binary event there
/// will be exactly one.
#[derive(Debug, Clone)]
pub struct BucketInfo {
    /// CTF `conditionId` = keccak256(oracle, questionId, outcomeSlotCount).
    pub condition_id: B256,
    /// Per-bucket `questionId`. For NegRisk, this is
    /// `bytes32(uint256(marketId) + questionIndex)`.
    pub question_id: B256,
    /// Index of this bucket inside the parent NegRisk market (0-based).
    /// Always 0 for a plain binary market.
    pub outcome_index: u32,
    /// Human-readable label from the market slug — e.g. "40°C",
    /// "37°C or below", "47°C or higher".
    pub bucket_label: String,
    /// ERC-1155 positionId for the YES outcome. This is what the executor
    /// mints, holds and sells.
    pub token_id_yes: U256,
    /// ERC-1155 positionId for the NO outcome.
    pub token_id_no: U256,
}

/// Emitted the instant we see a new weather market on chain and have derived
/// enough info to hand off to the executors. Ownership moves across the
/// unbounded mpsc channel — no back-pressure on the hot path.
#[derive(Debug, Clone)]
pub struct WeatherEvent {
    /// Polymarket event slug — e.g. "highest-temperature-in-lucknow-on-april-15-2026".
    pub event_slug: String,
    /// Lowercased city name parsed from the slug ("lucknow").
    pub city: String,
    /// ISO 8601 date parsed from the slug ("2026-04-15").
    pub resolution_date: String,
    pub kind: EventKind,
    /// For NegRisk: the `marketId` carried by `NegRiskAdapter.MarketPrepared`.
    /// `None` for pure binary events.
    pub neg_risk_market_id: Option<B256>,
    /// The NegRiskAdapter address (NegRisk) or the CTF oracle address (binary).
    /// Used to verify the event came from an allowlisted source.
    pub oracle: Address,
    /// All buckets discovered so far. For NegRisk we only emit the event once
    /// we believe we have the full set.
    pub buckets: Vec<BucketInfo>,
    /// Wall-clock nanos since epoch when the log was received from the WSS.
    /// Set exactly once, as close to the socket as possible, by
    /// `run_onchain_watcher`.
    pub detected_at_ns: u128,
    /// How this event was discovered. [`DiscoverySource::OnChain`] is the
    /// live-stream default; [`crate::no_edge::bootstrap`] tags snapshot
    /// events [`DiscoverySource::BootstrapReplay`] so Phase 2 can skip them.
    pub source: DiscoverySource,
}

/// Sender handed to `run_onchain_watcher` — upstream owns the receiver.
pub type WeatherEventSender = mpsc::UnboundedSender<WeatherEvent>;
/// Receiver consumed by the executor (Agent β entry point).
pub type WeatherEventReceiver = mpsc::UnboundedReceiver<WeatherEvent>;

/// Emitted by the mempool watcher (Agent β) when it has seen the first
/// `splitPosition`/`prepareMarket` mint transaction and can hand off to the
/// CLOB executor.
#[derive(Debug, Clone)]
pub struct MintReceipt {
    pub event_slug: String,
    pub tx_hash: B256,
    pub kind: EventKind,
    /// Nanos since epoch when the transaction entered the mempool
    /// (subscribed via `eth_subscribe("newPendingTransactions")`).
    pub seen_pending_at_ns: u128,
}

pub type MintReceiptSender = mpsc::UnboundedSender<MintReceipt>;
pub type MintReceiptReceiver = mpsc::UnboundedReceiver<MintReceipt>;

/// CLOB WebSocket told us a market is indexer-ready and we can POST orders.
#[derive(Debug, Clone)]
pub struct ClobMarketReady {
    pub condition_id: B256,
    pub asset_ids: Vec<U256>,
}

pub type ClobReadySender = mpsc::UnboundedSender<ClobMarketReady>;
pub type ClobReadyReceiver = mpsc::UnboundedReceiver<ClobMarketReady>;

/// Which Open-Meteo model family supplied μ for a [`ForecastTick`].
/// `Seamless` means we fell back to the source-agnostic default after the
/// preferred model returned no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ForecastSource {
    GfsHrrr,
    EcmwfIfs025,
    Seamless,
}

/// Where σ came from. Ensemble spread is the design default; the hard-coded
/// table is the fallback after 3 consecutive ensemble failures or when the
/// ensemble returns fewer than 3 members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum SigmaSource {
    EnsembleSpread,
    HardcodedFallback,
}

/// Emitted on a 30-minute cadence by `watchers::forecast` for every seeded
/// `(city, days_ahead)` pair. μ and σ are in the city's *native* unit
/// (°F for the 10 US stations, °C for VILK) — callers know the unit from
/// `climo::station_for_city(city).unit`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ForecastTick {
    pub city: &'static str,
    pub icao: &'static str,
    pub date: chrono::NaiveDate,
    pub days_ahead: i64,
    pub mu: f64,
    pub sigma: f64,
    pub source_mu: ForecastSource,
    pub source_sigma: SigmaSource,
    pub fetched_at_ns: u128,
}

#[allow(dead_code)]
pub type ForecastTickSender = mpsc::UnboundedSender<ForecastTick>;
#[allow(dead_code)]
pub type ForecastTickReceiver = mpsc::UnboundedReceiver<ForecastTick>;

/// METAR nowcast observation for a single ICAO. Emitted every 5 minutes by
/// `watchers/metar.rs`. Downstream (`edge_book.rs`) applies
/// `σ_remaining = σ_full × √remaining_var_frac` and truncates the bucket
/// distribution below `observed_max` (a physical lower bound on final TMAX).
#[derive(Debug, Clone)]
pub struct NowcastTick {
    pub icao: &'static str,
    pub date: chrono::NaiveDate,
    /// Max temperature observed so far for `date` in the station's native
    /// unit (°F for US ICAOs, °C for VILK / Lucknow).
    pub observed_max: f64,
    /// 0..=23 station-local hour of the most recent observation. For the
    /// staleness-revert tick this carries the most recent known hour, or
    /// 0 if no observation has ever been recorded.
    pub hour_local: u32,
    /// Fraction of the day's TMAX variance still ahead. `1.0` means full
    /// variance (early morning OR staleness-revert); `~0.05` means the
    /// observed-so-far is essentially the final TMAX.
    pub remaining_var_frac: f64,
    pub fetched_at_ns: u128,
}

pub type NowcastTickSender = mpsc::UnboundedSender<NowcastTick>;
pub type NowcastTickReceiver = mpsc::UnboundedReceiver<NowcastTick>;

/// Tiny helper — grabs nanos-since-epoch for the `detected_at_ns` field.
/// Inlined to keep the hot-path branchless.
#[inline(always)]
pub fn now_ns() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// One snapshot of a CLOB book for a single token. Emitted by
/// `watchers::clob_book` whenever Polymarket pushes a `book` snapshot or a
/// `price_change` delta. Ladders are sorted: asks ascending by price, bids
/// descending by price. `best_bid` and `best_ask` are the top of each ladder
/// or `None` when that side is empty.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BookUpdate {
    pub token_id: U256,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    /// (price, size) ascending by price.
    pub asks_ladder: Vec<(f64, f64)>,
    /// (price, size) descending by price.
    pub bids_ladder: Vec<(f64, f64)>,
    pub fetched_at_ns: u128,
}

#[allow(dead_code)]
pub type BookUpdateSender = mpsc::UnboundedSender<BookUpdate>;
#[allow(dead_code)]
pub type BookUpdateReceiver = mpsc::UnboundedReceiver<BookUpdate>;

/// Subscription command sent into `watchers::clob_book` from EdgeBook (or any
/// upstream owner of the wanted-token set). The watcher is the single
/// authoritative owner of the subscribed-token `HashSet`; this channel is the
/// only way to mutate it.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SubCmd {
    Add(U256),
    Remove(U256),
}

#[allow(dead_code)]
pub type SubCmdSender = mpsc::UnboundedSender<SubCmd>;
#[allow(dead_code)]
pub type SubCmdReceiver = mpsc::UnboundedReceiver<SubCmd>;
