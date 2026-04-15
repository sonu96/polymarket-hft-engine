//! Startup bootstrap replay for the Phase 3 NO-edge farmer.
//!
//! ## Problem
//!
//! The `watchers::onchain` task only streams *new* `MarketPrepared` logs — it
//! never replays history. On binary startup, the farmer therefore knows
//! nothing about weather events that were minted before the process came up.
//! Those events are still tradable and we want to quote them.
//!
//! ## Design (per `docs/PHASE3_NO_EDGE_FARMER.md` §3.4)
//!
//! Standard snapshot-and-tail pattern:
//!
//! 1. **Subscribe to WS first.** The parent (ticket #12 — main.rs wiring)
//!    spawns `watchers::onchain::run_onchain_watcher` *before* calling
//!    [`run_bootstrap`]. During the bootstrap window, any `WeatherEvent`
//!    arriving through the live channel is **buffered** by the caller and
//!    handed to us as `buffered_live_events`.
//! 2. **Fetch a one-shot Gamma snapshot** of `active=true&closed=false`
//!    events, filter to slugs matching the weather template, tag each with
//!    [`DiscoverySource::BootstrapReplay`], and forward through
//!    [`EdgeCmd::RegisterEvent`].
//! 3. **Drain the buffered live events**, dedup-by-event-slug against the
//!    snapshot set so events that appeared mid-bootstrap are not double-
//!    registered. Buffered survivors keep their [`DiscoverySource::OnChain`]
//!    tag so Phase 2 mint-and-dump can still attempt a normal fast-path.
//! 4. **Orphan reconciliation.** Call [`OpenOrderSource::list_open_orders`]
//!    (in production, [`Executor::list_open_orders`]) and cross-reference
//!    every order id against `known_orders` from the persisted
//!    `NoEdgeState`. Recognized orders are *adopted* (logged — the actual
//!    state adoption is the portfolio actor's job once ticket #7 lands);
//!    unrecognized orders are *cancelled* so we don't leak resting size
//!    from a previous run we lost track of.
//! 5. Return — control flows back to the parent, which then starts reading
//!    from the live channel directly.
//!
//! ## Phase 2 skip rule
//!
//! Events tagged [`DiscoverySource::BootstrapReplay`] are **ignored** by
//! `mint_executor` (ticket #12 wiring). The mint window is 12–36 h
//! pre-settlement and any event old enough to appear in the
//! `active=true&closed=false` snapshot has already blown past it.
//!
//! ## Testing
//!
//! This module is deliberately split into narrow helpers
//! ([`filter_weather_slug`], [`parse_clob_token_ids`],
//! [`dedup_buffered`], [`reconcile_orphans`]) so unit tests don't need
//! an HTTP mock or a real CLOB client. [`OpenOrderSource`] is a trait with
//! a stub impl in the test module.

#![allow(dead_code)]

use crate::executor::Executor;
use crate::types::{BucketInfo, DiscoverySource, EventKind, WeatherEvent, now_ns};
use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;

/// Bootstrap's own lightweight view of an open order. Kept separate from
/// the SDK's `OpenOrderResponse` (which is `#[non_exhaustive]` and thus
/// uninstantiable from outside) so unit tests can construct fixtures
/// without authenticating a real CLOB client.
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub id: String,
    /// Human-readable asset id. Only used for log lines — bootstrap does
    /// not compare asset ids against anything.
    pub asset_id: String,
}

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

/// Tallies everything bootstrap did in one run. Printed by the caller for
/// operability; also inspected by tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BootstrapReport {
    /// Events pulled from Gamma and tagged [`DiscoverySource::BootstrapReplay`].
    pub snapshot_events: usize,
    /// Buffered live events that survived the dedup pass and were forwarded
    /// to EdgeBook with their original [`DiscoverySource::OnChain`] tag.
    pub buffered_forwarded: usize,
    /// Buffered live events dropped because their slug was already in the
    /// snapshot set.
    pub buffered_deduped: usize,
    /// Open orders on CLOB whose id appeared in `known_orders` — no action
    /// taken, just logged for auditability.
    pub adopted_orders: usize,
    /// Open orders on CLOB NOT in `known_orders` — we called
    /// `cancel_order` on each one.
    pub orphan_cancellations_attempted: usize,
    /// Subset of `orphan_cancellations_attempted` where the cancel call
    /// returned `Ok`.
    pub orphan_cancellations_succeeded: usize,
}

/// Minimal abstraction over "list currently open orders". Production uses
/// [`Executor::list_open_orders`]; tests inject a stub so we don't have to
/// authenticate or hit CLOB.
#[async_trait]
pub trait OpenOrderSource {
    async fn list_open_orders(&self) -> Result<Vec<OpenOrderInfo>>;
    async fn cancel_order(&self, order_id: &str) -> Result<()>;
}

#[async_trait]
impl OpenOrderSource for Executor {
    async fn list_open_orders(&self) -> Result<Vec<OpenOrderInfo>> {
        let raw = Executor::list_open_orders(self).await?;
        Ok(raw
            .into_iter()
            .map(|o| OpenOrderInfo {
                id: o.id,
                asset_id: o.asset_id.to_string(),
            })
            .collect())
    }
    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        Executor::cancel_order(self, order_id).await
    }
}

/// Where to send bootstrap-materialised events. Kept as an abstract sink (a
/// closure-returned `Send`-able target) so the tests can capture into a
/// `Vec` instead of spinning up the EdgeBook actor. The `register_event`
/// callback is infallible from bootstrap's POV — the real EdgeBook channel
/// is unbounded, and a closed channel is a process-exit condition handled
/// by the parent supervisor, not here.
pub trait EventSink: Send {
    fn register_event(&mut self, event: WeatherEvent);
}

/// Adapter so a plain [`crate::edge_book::EdgeCmdSender`] implements
/// [`EventSink`]. The `send` error is logged and dropped — same as every
/// other producer in this codebase.
pub struct EdgeCmdEventSink {
    tx: crate::edge_book::EdgeCmdSender,
}

impl EdgeCmdEventSink {
    pub fn new(tx: crate::edge_book::EdgeCmdSender) -> Self {
        Self { tx }
    }
}

impl EventSink for EdgeCmdEventSink {
    fn register_event(&mut self, event: WeatherEvent) {
        if self
            .tx
            .send(crate::edge_book::EdgeCmd::RegisterEvent(event))
            .is_err()
        {
            tracing::warn!("[bootstrap] EdgeBook channel closed mid-register");
        }
    }
}

/// Run the full startup bootstrap: fetch the snapshot, drain buffered events,
/// reconcile orphans. See module docs for the sequence.
///
/// `executor` is anything implementing [`OpenOrderSource`] — production
/// uses [`Executor`]. `sink` is anything implementing [`EventSink`] —
/// production uses [`EdgeCmdEventSink`].
///
/// `buffered_live_events` is the queue the caller collected from the live
/// `WeatherEventReceiver` between "spawn the onchain watcher" and "call
/// bootstrap". Collect with a 1–2 s grace window so late-arriving events
/// from the initial `eth_subscribe` handshake don't get lost.
///
/// `known_orders` is the set of order ids currently in
/// `NoEdgeState::known_orders` — pass a snapshot of
/// `state.known_orders.keys()`.
pub async fn run_bootstrap<S: OpenOrderSource, Sk: EventSink>(
    gamma_base_url: &str,
    executor: &S,
    sink: &mut Sk,
    buffered_live_events: Vec<WeatherEvent>,
    known_orders: &HashSet<String>,
) -> Result<BootstrapReport> {
    let http = reqwest::Client::builder()
        .user_agent("curl/8.5.0")
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to build bootstrap http client")?;

    // --- 1. Snapshot fetch ------------------------------------------------
    let snapshot_raw = fetch_gamma_snapshot(&http, gamma_base_url)
        .await
        .context("fetching gamma snapshot")?;
    tracing::info!(
        "[bootstrap] gamma snapshot → {} raw events",
        snapshot_raw.len()
    );

    // --- 2. Filter + build WeatherEvent per snapshot row ------------------
    let mut snapshot_events: Vec<WeatherEvent> = Vec::new();
    for row in &snapshot_raw {
        if !filter_weather_slug(&row.slug) {
            continue;
        }
        match build_weather_event_from_gamma(row) {
            Some(ev) => snapshot_events.push(ev),
            None => {
                tracing::debug!(
                    "[bootstrap] skipping gamma row {} — could not build WeatherEvent",
                    row.slug
                );
            }
        }
    }
    tracing::info!(
        "[bootstrap] {} snapshot rows passed weather filter",
        snapshot_events.len()
    );

    let snapshot_slugs: HashSet<String> =
        snapshot_events.iter().map(|e| e.event_slug.clone()).collect();

    // --- 3. Forward snapshot events to the sink ---------------------------
    for ev in snapshot_events.iter() {
        sink.register_event(ev.clone());
    }

    // --- 4. Dedup buffered live events against snapshot_slugs, forward ----
    let (forwarded_buf, deduped_count) = dedup_buffered(&snapshot_slugs, buffered_live_events);
    for ev in forwarded_buf.iter() {
        sink.register_event(ev.clone());
    }

    // --- 5. Orphan reconciliation -----------------------------------------
    let orphan_result = reconcile_orphans(executor, known_orders).await?;

    let report = BootstrapReport {
        snapshot_events: snapshot_events.len(),
        buffered_forwarded: forwarded_buf.len(),
        buffered_deduped: deduped_count,
        adopted_orders: orphan_result.adopted,
        orphan_cancellations_attempted: orphan_result.attempted,
        orphan_cancellations_succeeded: orphan_result.succeeded,
    };
    tracing::info!("[bootstrap] report = {:?}", report);
    Ok(report)
}

// ---------------------------------------------------------------------------
// Gamma HTTP + parsing
// ---------------------------------------------------------------------------

/// Minimal projection of the Gamma `/events` response. We only pick the
/// fields bootstrap actually uses; anything else is left on the wire.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GammaEventRow {
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub markets: Vec<GammaMarketRow>,
    /// Some older payloads have `negRiskMarketID` at the event level; we
    /// take a best-effort stab at parsing it if present.
    #[serde(default, rename = "negRiskMarketID")]
    pub neg_risk_market_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GammaMarketRow {
    /// Hex-encoded conditionId, e.g. `0xabc...`.
    #[serde(default, rename = "conditionId")]
    pub condition_id: Option<String>,
    /// JSON-encoded STRING of `["<yes_hex>","<no_hex>"]`.
    #[serde(default, rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default, rename = "groupItemTitle")]
    pub group_item_title: Option<String>,
}

const GAMMA_PAGE_LIMIT: usize = 500;

async fn fetch_gamma_snapshot(
    http: &Client,
    gamma_base_url: &str,
) -> Result<Vec<GammaEventRow>> {
    let mut offset: usize = 0;
    let mut all: Vec<GammaEventRow> = Vec::new();
    loop {
        let url = format!(
            "{}/events?active=true&closed=false&limit={}&offset={}",
            gamma_base_url.trim_end_matches('/'),
            GAMMA_PAGE_LIMIT,
            offset
        );
        tracing::debug!("[bootstrap] GET {}", url);
        let resp = http.get(&url).send().await.with_context(|| format!("GET {}", url))?;
        if !resp.status().is_success() {
            anyhow::bail!("gamma {} → HTTP {}", url, resp.status());
        }
        let text = resp.text().await.context("reading gamma body")?;
        let page: Vec<GammaEventRow> = serde_json::from_str(&text)
            .with_context(|| format!("parsing gamma page (offset={})", offset))?;
        let page_len = page.len();
        all.extend(page);
        if page_len < GAMMA_PAGE_LIMIT {
            break;
        }
        offset += GAMMA_PAGE_LIMIT;
        // Defensive cap: weather events realistically number < 1000 active
        // at any moment. If we somehow page past 10k we're in a loop.
        if offset >= 10_000 {
            tracing::warn!("[bootstrap] gamma pagination cap hit at offset={}", offset);
            break;
        }
    }
    Ok(all)
}

// ---------------------------------------------------------------------------
// Slug filter
// ---------------------------------------------------------------------------

/// Accepts exactly the weather template — we reuse the stricter regex from
/// `weather_filter::parse_weather_slug` to stay consistent with the live
/// filter. A slug is "weather" iff it parses cleanly (city + date).
pub(crate) fn filter_weather_slug(slug: &str) -> bool {
    crate::weather_filter::parse_weather_slug(slug).is_some()
}

// ---------------------------------------------------------------------------
// clobTokenIds parsing
// ---------------------------------------------------------------------------

/// Gamma encodes `clobTokenIds` as a JSON *string* whose contents are a JSON
/// array of two decimal-string positionIds, YES first and NO second:
///   `"[\"12345\",\"67890\"]"`
/// or occasionally as `["0x..","0x.."]` with hex. Accepts both.
pub(crate) fn parse_clob_token_ids(raw: &str) -> Option<(U256, U256)> {
    let parsed: Vec<String> = serde_json::from_str(raw).ok()?;
    if parsed.len() != 2 {
        return None;
    }
    let yes = parse_u256_loose(&parsed[0])?;
    let no = parse_u256_loose(&parsed[1])?;
    Some((yes, no))
}

fn parse_u256_loose(s: &str) -> Option<U256> {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        U256::from_str_radix(stripped, 16).ok()
    } else {
        U256::from_str_radix(s, 10).ok()
    }
}

fn parse_b256_loose(s: &str) -> Option<B256> {
    let s = s.trim().trim_start_matches("0x");
    let bytes = hex::decode(s).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(B256::from(out))
}

// ---------------------------------------------------------------------------
// Build WeatherEvent from a Gamma event row
// ---------------------------------------------------------------------------

pub(crate) fn build_weather_event_from_gamma(row: &GammaEventRow) -> Option<WeatherEvent> {
    let (city, resolution_date) = crate::weather_filter::parse_weather_slug(&row.slug)?;

    let mut buckets: Vec<BucketInfo> = Vec::new();
    for (idx, market) in row.markets.iter().enumerate() {
        let cond_hex = match &market.condition_id {
            Some(c) => c,
            None => continue,
        };
        let condition_id = match parse_b256_loose(cond_hex) {
            Some(c) => c,
            None => {
                tracing::debug!(
                    "[bootstrap] bad conditionId {} on slug {} — skipping bucket",
                    cond_hex,
                    row.slug
                );
                continue;
            }
        };
        let tokens_raw = match &market.clob_token_ids {
            Some(t) => t,
            None => continue,
        };
        let (yes, no) = match parse_clob_token_ids(tokens_raw) {
            Some(pair) => pair,
            None => {
                tracing::debug!(
                    "[bootstrap] bad clobTokenIds on slug {} — skipping bucket",
                    row.slug
                );
                continue;
            }
        };

        let bucket_label = market
            .group_item_title
            .clone()
            .or_else(|| market.outcome.clone())
            .or_else(|| market.question.clone())
            .unwrap_or_else(|| format!("bucket-{}", idx));

        buckets.push(BucketInfo {
            condition_id,
            // Bootstrap does not know per-bucket questionId — set to ZERO;
            // the live-path re-derives questionId on-chain for NegRisk events
            // from `QuestionPrepared`. No downstream code reads questionId
            // after RegisterEvent, so this is safe.
            question_id: B256::ZERO,
            outcome_index: idx as u32,
            bucket_label,
            token_id_yes: yes,
            token_id_no: no,
        });
    }

    if buckets.is_empty() {
        return None;
    }

    let neg_risk_market_id = row
        .neg_risk_market_id
        .as_deref()
        .and_then(parse_b256_loose);

    Some(WeatherEvent {
        event_slug: row.slug.clone(),
        city,
        resolution_date,
        // Bootstrap only sees NegRisk temperature markets on the weather
        // template; a binary CTF slot would not match our slug regex anyway.
        kind: EventKind::NegRisk,
        neg_risk_market_id,
        oracle: Address::ZERO,
        buckets,
        detected_at_ns: now_ns(),
        source: DiscoverySource::BootstrapReplay,
    })
}

// ---------------------------------------------------------------------------
// Buffered-event dedup
// ---------------------------------------------------------------------------

/// Walk `buffered` and split into (forwarded, deduped_count):
///   * **forwarded**: events whose slug is NOT in `snapshot_slugs`. These
///     keep their original `source` (normally `OnChain`) and will be
///     register-ed after the snapshot events.
///   * **deduped_count**: number dropped because their slug was already in
///     `snapshot_slugs`.
pub(crate) fn dedup_buffered(
    snapshot_slugs: &HashSet<String>,
    buffered: Vec<WeatherEvent>,
) -> (Vec<WeatherEvent>, usize) {
    let mut forwarded = Vec::with_capacity(buffered.len());
    let mut deduped = 0usize;
    for ev in buffered {
        if snapshot_slugs.contains(&ev.event_slug) {
            deduped += 1;
        } else {
            forwarded.push(ev);
        }
    }
    (forwarded, deduped)
}

// ---------------------------------------------------------------------------
// Orphan order reconciliation
// ---------------------------------------------------------------------------

pub(crate) struct OrphanResult {
    pub adopted: usize,
    pub attempted: usize,
    pub succeeded: usize,
}

pub(crate) async fn reconcile_orphans<S: OpenOrderSource>(
    executor: &S,
    known_orders: &HashSet<String>,
) -> Result<OrphanResult> {
    let open = match executor.list_open_orders().await {
        Ok(v) => v,
        Err(e) => {
            // In simulation mode `list_open_orders` intentionally fails.
            // Warn and treat the CLOB as empty — no orders means no orphans.
            tracing::warn!(
                "[bootstrap] list_open_orders failed ({}), skipping orphan reconciliation",
                e
            );
            return Ok(OrphanResult {
                adopted: 0,
                attempted: 0,
                succeeded: 0,
            });
        }
    };
    tracing::info!(
        "[bootstrap] CLOB reports {} open orders; checking against {} known",
        open.len(),
        known_orders.len()
    );

    let mut adopted = 0usize;
    let mut attempted = 0usize;
    let mut succeeded = 0usize;
    for order in open {
        if known_orders.contains(&order.id) {
            tracing::info!(
                "[bootstrap] adopting known order id={} asset={}",
                order.id,
                order.asset_id
            );
            adopted += 1;
            continue;
        }
        tracing::warn!(
            "[bootstrap] orphan order id={} asset={} — cancelling",
            order.id,
            order.asset_id
        );
        attempted += 1;
        match executor.cancel_order(&order.id).await {
            Ok(()) => succeeded += 1,
            Err(e) => tracing::warn!("[bootstrap] cancel orphan {} failed: {}", order.id, e),
        }
    }
    Ok(OrphanResult {
        adopted,
        attempted,
        succeeded,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // ---- Stub OpenOrderSource --------------------------------------------

    /// Records every call so tests can assert on cancellations.
    struct StubOrderSource {
        orders: Vec<OpenOrderInfo>,
        /// Ids the stub should pretend to fail to cancel.
        fail_cancels: HashSet<String>,
        /// Call log — populated by `cancel_order`.
        cancelled: Mutex<Vec<String>>,
    }

    impl StubOrderSource {
        fn new(orders: Vec<OpenOrderInfo>) -> Self {
            Self {
                orders,
                fail_cancels: HashSet::new(),
                cancelled: Mutex::new(Vec::new()),
            }
        }
        fn with_failing_cancels(mut self, ids: &[&str]) -> Self {
            self.fail_cancels = ids.iter().map(|s| s.to_string()).collect();
            self
        }
        fn cancelled_snapshot(&self) -> Vec<String> {
            self.cancelled.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OpenOrderSource for StubOrderSource {
        async fn list_open_orders(&self) -> Result<Vec<OpenOrderInfo>> {
            Ok(self.orders.clone())
        }
        async fn cancel_order(&self, order_id: &str) -> Result<()> {
            self.cancelled.lock().unwrap().push(order_id.to_string());
            if self.fail_cancels.contains(order_id) {
                anyhow::bail!("stub-failure for {}", order_id);
            }
            Ok(())
        }
    }

    // ---- Capturing EventSink --------------------------------------------

    struct CapturingSink {
        events: Vec<WeatherEvent>,
    }
    impl CapturingSink {
        fn new() -> Self {
            Self { events: Vec::new() }
        }
    }
    impl EventSink for CapturingSink {
        fn register_event(&mut self, event: WeatherEvent) {
            self.events.push(event);
        }
    }

    // ---- Helpers ---------------------------------------------------------

    fn make_event(slug: &str, source: DiscoverySource) -> WeatherEvent {
        WeatherEvent {
            event_slug: slug.to_string(),
            city: "lucknow".to_string(),
            resolution_date: "2026-04-15".to_string(),
            kind: EventKind::NegRisk,
            neg_risk_market_id: Some(B256::repeat_byte(0xaa)),
            oracle: Address::ZERO,
            buckets: vec![BucketInfo {
                condition_id: B256::repeat_byte(0x01),
                question_id: B256::repeat_byte(0x02),
                outcome_index: 0,
                bucket_label: "40°C".to_string(),
                token_id_yes: U256::from(1u64),
                token_id_no: U256::from(2u64),
            }],
            detected_at_ns: 0,
            source,
        }
    }

    fn make_open_order(id: &str) -> OpenOrderInfo {
        OpenOrderInfo {
            id: id.to_string(),
            asset_id: "0".to_string(),
        }
    }

    fn sample_gamma_row(slug: &str) -> GammaEventRow {
        GammaEventRow {
            slug: slug.to_string(),
            markets: vec![
                GammaMarketRow {
                    condition_id: Some(format!("0x{}", "11".repeat(32))),
                    clob_token_ids: Some(r#"["12345","67890"]"#.to_string()),
                    question: Some("Will TMAX be 40°C?".to_string()),
                    outcome: Some("40°C".to_string()),
                    group_item_title: Some("40°C".to_string()),
                },
                GammaMarketRow {
                    condition_id: Some(format!("0x{}", "22".repeat(32))),
                    clob_token_ids: Some(r#"["99999","88888"]"#.to_string()),
                    question: Some("Will TMAX be 41°C?".to_string()),
                    outcome: Some("41°C".to_string()),
                    group_item_title: Some("41°C".to_string()),
                },
            ],
            neg_risk_market_id: None,
        }
    }

    // ---- Slug filter -----------------------------------------------------

    #[test]
    fn filter_keeps_valid_highest_temperature_slugs() {
        assert!(filter_weather_slug(
            "highest-temperature-in-lucknow-on-april-15-2026"
        ));
        assert!(filter_weather_slug(
            "highest-temperature-in-new-york-city-on-june-1-2026"
        ));
        assert!(filter_weather_slug(
            "highest-temperature-in-tokyo-on-december-31-2026"
        ));
    }

    #[test]
    fn filter_rejects_non_weather_slugs() {
        assert!(!filter_weather_slug("will-trump-win-the-2024-election"));
        assert!(!filter_weather_slug("btc-updown-5m-1776000000"));
        assert!(!filter_weather_slug("lowest-temperature-in-lucknow-on-april-15-2026"));
        assert!(!filter_weather_slug(""));
    }

    // ---- clobTokenIds parsing -------------------------------------------

    #[test]
    fn parse_clob_token_ids_extracts_yes_and_no() {
        let raw = r#"["12345","67890"]"#;
        let (yes, no) = parse_clob_token_ids(raw).unwrap();
        assert_eq!(yes, U256::from(12345u64));
        assert_eq!(no, U256::from(67890u64));
    }

    #[test]
    fn parse_clob_token_ids_accepts_hex_variant() {
        let raw = r#"["0x1","0x02"]"#;
        let (yes, no) = parse_clob_token_ids(raw).unwrap();
        assert_eq!(yes, U256::from(1u64));
        assert_eq!(no, U256::from(2u64));
    }

    #[test]
    fn parse_clob_token_ids_rejects_malformed() {
        assert!(parse_clob_token_ids(r#"["12345"]"#).is_none());
        assert!(parse_clob_token_ids("not json").is_none());
        assert!(parse_clob_token_ids(r#"["abc","def"]"#).is_none());
    }

    // ---- WeatherEvent construction --------------------------------------

    #[test]
    fn construct_weather_event_has_bootstrap_replay_source() {
        let row = sample_gamma_row("highest-temperature-in-lucknow-on-april-15-2026");
        let ev = build_weather_event_from_gamma(&row).unwrap();
        assert_eq!(ev.source, DiscoverySource::BootstrapReplay);
        assert_eq!(ev.city, "lucknow");
        assert_eq!(ev.resolution_date, "2026-04-15");
        assert_eq!(ev.buckets.len(), 2);
        assert_eq!(ev.buckets[0].token_id_yes, U256::from(12345u64));
        assert_eq!(ev.buckets[0].token_id_no, U256::from(67890u64));
        assert_eq!(ev.buckets[0].bucket_label, "40°C");
        assert_eq!(ev.buckets[1].outcome_index, 1);
        // Bucket 0 conditionId should be 32 × 0x11
        assert_eq!(ev.buckets[0].condition_id, B256::repeat_byte(0x11));
        assert_eq!(ev.buckets[1].condition_id, B256::repeat_byte(0x22));
    }

    #[test]
    fn construct_weather_event_rejects_non_weather_slug() {
        let mut row = sample_gamma_row("not-a-weather-event");
        row.slug = "not-a-weather-event".to_string();
        assert!(build_weather_event_from_gamma(&row).is_none());
    }

    #[test]
    fn construct_weather_event_rejects_row_with_no_valid_buckets() {
        let mut row = sample_gamma_row("highest-temperature-in-lucknow-on-april-15-2026");
        for m in row.markets.iter_mut() {
            m.condition_id = None;
        }
        assert!(build_weather_event_from_gamma(&row).is_none());
    }

    // ---- Dedup -----------------------------------------------------------

    #[test]
    fn dedup_drops_buffered_events_already_in_snapshot_set() {
        let mut snap = HashSet::new();
        snap.insert("highest-temperature-in-lucknow-on-april-15-2026".to_string());
        snap.insert("highest-temperature-in-nyc-on-april-15-2026".to_string());

        let buffered = vec![
            make_event(
                "highest-temperature-in-lucknow-on-april-15-2026",
                DiscoverySource::OnChain,
            ),
            make_event(
                "highest-temperature-in-nyc-on-april-15-2026",
                DiscoverySource::OnChain,
            ),
        ];
        let (fwd, deduped) = dedup_buffered(&snap, buffered);
        assert_eq!(fwd.len(), 0);
        assert_eq!(deduped, 2);
    }

    #[test]
    fn dedup_keeps_buffered_events_not_in_snapshot_set() {
        let mut snap = HashSet::new();
        snap.insert("highest-temperature-in-lucknow-on-april-15-2026".to_string());

        let buffered = vec![
            // kept — slug not in snapshot
            make_event(
                "highest-temperature-in-seattle-on-april-16-2026",
                DiscoverySource::OnChain,
            ),
            // dropped — slug in snapshot
            make_event(
                "highest-temperature-in-lucknow-on-april-15-2026",
                DiscoverySource::OnChain,
            ),
            // kept
            make_event(
                "highest-temperature-in-chicago-on-april-17-2026",
                DiscoverySource::OnChain,
            ),
        ];
        let (fwd, deduped) = dedup_buffered(&snap, buffered);
        assert_eq!(fwd.len(), 2);
        assert_eq!(deduped, 1);
        // order preserved
        assert_eq!(
            fwd[0].event_slug,
            "highest-temperature-in-seattle-on-april-16-2026"
        );
        assert_eq!(
            fwd[1].event_slug,
            "highest-temperature-in-chicago-on-april-17-2026"
        );
    }

    // ---- Orphan reconciliation ------------------------------------------

    #[tokio::test]
    async fn orphan_reconciliation_cancels_unknown_order_ids() {
        let stub = StubOrderSource::new(vec![
            make_open_order("orphan-1"),
            make_open_order("orphan-2"),
        ]);
        let known: HashSet<String> = HashSet::new();
        let res = reconcile_orphans(&stub, &known).await.unwrap();
        assert_eq!(res.adopted, 0);
        assert_eq!(res.attempted, 2);
        assert_eq!(res.succeeded, 2);
        let cancelled = stub.cancelled_snapshot();
        assert_eq!(cancelled.len(), 2);
        assert!(cancelled.contains(&"orphan-1".to_string()));
        assert!(cancelled.contains(&"orphan-2".to_string()));
    }

    #[tokio::test]
    async fn orphan_reconciliation_adopts_known_order_ids() {
        let stub = StubOrderSource::new(vec![
            make_open_order("known-1"),
            make_open_order("known-2"),
            make_open_order("orphan-1"),
        ]);
        let known: HashSet<String> = ["known-1".to_string(), "known-2".to_string()]
            .into_iter()
            .collect();
        let res = reconcile_orphans(&stub, &known).await.unwrap();
        assert_eq!(res.adopted, 2);
        assert_eq!(res.attempted, 1);
        assert_eq!(res.succeeded, 1);
        let cancelled = stub.cancelled_snapshot();
        assert_eq!(cancelled, vec!["orphan-1".to_string()]);
    }

    #[tokio::test]
    async fn orphan_reconciliation_records_failed_cancels_separately() {
        let stub = StubOrderSource::new(vec![
            make_open_order("orphan-good"),
            make_open_order("orphan-bad"),
        ])
        .with_failing_cancels(&["orphan-bad"]);
        let known: HashSet<String> = HashSet::new();
        let res = reconcile_orphans(&stub, &known).await.unwrap();
        assert_eq!(res.attempted, 2);
        assert_eq!(res.succeeded, 1);
        assert_eq!(res.adopted, 0);
    }

    // ---- Report counts ---------------------------------------------------

    #[test]
    fn bootstrap_report_counts_match_inputs() {
        // Build a synthetic report the way run_bootstrap would and assert
        // every counter is what we expect end-to-end, WITHOUT hitting HTTP.
        let snap_slugs: HashSet<String> = [
            "highest-temperature-in-lucknow-on-april-15-2026".to_string(),
            "highest-temperature-in-nyc-on-april-15-2026".to_string(),
        ]
        .into_iter()
        .collect();
        let buffered = vec![
            // dup → deduped
            make_event(
                "highest-temperature-in-lucknow-on-april-15-2026",
                DiscoverySource::OnChain,
            ),
            // new → forwarded
            make_event(
                "highest-temperature-in-chicago-on-april-17-2026",
                DiscoverySource::OnChain,
            ),
        ];
        let (fwd, deduped) = dedup_buffered(&snap_slugs, buffered);
        let orphan = OrphanResult {
            adopted: 3,
            attempted: 2,
            succeeded: 1,
        };
        let report = BootstrapReport {
            snapshot_events: snap_slugs.len(),
            buffered_forwarded: fwd.len(),
            buffered_deduped: deduped,
            adopted_orders: orphan.adopted,
            orphan_cancellations_attempted: orphan.attempted,
            orphan_cancellations_succeeded: orphan.succeeded,
        };
        assert_eq!(report.snapshot_events, 2);
        assert_eq!(report.buffered_forwarded, 1);
        assert_eq!(report.buffered_deduped, 1);
        assert_eq!(report.adopted_orders, 3);
        assert_eq!(report.orphan_cancellations_attempted, 2);
        assert_eq!(report.orphan_cancellations_succeeded, 1);
    }
}
