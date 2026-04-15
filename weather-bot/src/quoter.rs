//! Quoter actor — per-token state machine that turns `EdgeSignal`s into
//! live POST/CANCEL traffic on the Polymarket CLOB (or the paper-trading
//! `PaperEngine`, behind the `OrderSink` trait).
//!
//! Part of the Phase 3 NO-edge farmer pipeline. See
//! `docs/PHASE3_NO_EDGE_FARMER.md` §3.6 for the state-machine diagram; in
//! short:
//!
//! ```text
//!         ┌───────┐  signal   ┌─────────┐
//!         │ Idle  │──────────▶│ Resting │
//!         └───────┘           └─────────┘
//!            ▲ ▲                 │    │
//!            │ │ fill            │    │ signal (price-diff)
//!            │ └─────────────────┘    ▼
//!            │                   ┌────────────┐
//!            │     cancel_ack    │ Cancelling │
//!            └───────────────────│  (may also │
//!                                │  Fill!)    │
//!                                └────────────┘
//! ```
//!
//! The unintuitive edge is `Cancelling → Fill → Idle`: a CLOB cancel races
//! an incoming taker and can lose. When that happens we must still record
//! the fill and clear any `pending_next` replacement — the signal is stale
//! the moment we flip states.
//!
//! Not wired into `main.rs` yet (that's a follow-up ticket); kept compilable
//! under `#![allow(dead_code)]` like every other Phase 3 wave-2 module.

#![allow(dead_code)]

use crate::config::Config;
use crate::edge_book::EdgeSignal;
use crate::order_sink::OrderSink;
use crate::portfolio::{CanDeployResponse, KnownOrder, PortfolioHandle};
use crate::types::now_ns;
use crate::watchers::clob_user::FillEvent;
use alloy_primitives::U256;
use polymarket_client_sdk::clob::types::Side;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// If we've been sitting in `Cancelling` for this long without a CancelAck
/// or Fill, escalate via a WARN log. We deliberately do NOT auto-reset the
/// state — live ops has to look at the order before we decide what to do.
pub const CANCEL_TIMEOUT_SECS: u64 = 30;

/// Per-token state machine. Mirrors the summary enum in `edge_book::QState`
/// but carries the details the quoter needs to reason about replaces.
#[derive(Debug, Clone)]
pub enum QState {
    /// No resting order. Next EdgeSignal at/below target_ask triggers a post.
    Idle,
    /// Order placed, waiting for fill or replace. `posted_at_ns` measures
    /// cooldown; `price`/`size` drive the repost-threshold check.
    Resting {
        order_id: String,
        price: f64,
        size: f64,
        posted_at_ns: u128,
    },
    /// Cancel request sent, waiting for CancelAck. Fills can still arrive
    /// during this window — handle them as a terminal transition to Idle
    /// (the cancel was racing the taker and lost).
    Cancelling {
        old_order_id: String,
        orig_price: f64,
        orig_size: f64,
        cancel_sent_at_ns: u128,
    },
}

/// Commands consumed by the quoter actor. EdgeBook emits `Signal`,
/// `watchers::clob_user` emits `Fill` and `CancelAck`, and tests + metrics
/// use `QuerySnapshot` for read-only state inspection.
#[derive(Debug)]
pub enum QuoterCmd {
    /// From EdgeBook: new edge opportunity for this token.
    Signal(EdgeSignal),
    /// From `watchers::clob_user`: one of our resting orders got hit.
    Fill(FillEvent),
    /// From `watchers::clob_user`: CLOB acknowledged a cancel request.
    CancelAck {
        token_id: U256,
        order_id: String,
    },
    /// For metrics / tests: snapshot one token's current state.
    QuerySnapshot {
        token_id: U256,
        reply: oneshot::Sender<Option<QState>>,
    },
}

pub type QuoterCmdSender = mpsc::UnboundedSender<QuoterCmd>;
pub type QuoterCmdReceiver = mpsc::UnboundedReceiver<QuoterCmd>;

// ---------------------------------------------------------------------------
// Actor entry point
// ---------------------------------------------------------------------------

/// Run the quoter actor forever. Returns `Ok(())` when the command channel
/// closes (shutdown).
pub async fn run_quoter<S: OrderSink + 'static>(
    cfg: &Config,
    sink: Arc<S>,
    portfolio: PortfolioHandle,
    mut cmd_rx: QuoterCmdReceiver,
) -> anyhow::Result<()> {
    let mut states: HashMap<U256, QState> = HashMap::new();
    let mut pending_next: HashMap<U256, EdgeSignal> = HashMap::new();
    let mut cancel_timeouts: HashMap<U256, Instant> = HashMap::new();

    let mut timeout_ticker = tokio::time::interval(Duration::from_secs(5));
    // Skip the immediate tick — nothing to check yet.
    timeout_ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            maybe_cmd = cmd_rx.recv() => match maybe_cmd {
                Some(cmd) => {
                    handle_cmd(
                        cmd,
                        cfg,
                        sink.as_ref(),
                        &portfolio,
                        &mut states,
                        &mut pending_next,
                        &mut cancel_timeouts,
                    )
                    .await;
                }
                None => break,
            },
            _ = timeout_ticker.tick() => {
                check_cancel_timeouts(&states, &mut cancel_timeouts);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

async fn handle_cmd<S: OrderSink + ?Sized>(
    cmd: QuoterCmd,
    cfg: &Config,
    sink: &S,
    portfolio: &PortfolioHandle,
    states: &mut HashMap<U256, QState>,
    pending_next: &mut HashMap<U256, EdgeSignal>,
    cancel_timeouts: &mut HashMap<U256, Instant>,
) {
    match cmd {
        QuoterCmd::Signal(signal) => {
            handle_signal(signal, cfg, sink, portfolio, states, pending_next, cancel_timeouts).await
        }
        QuoterCmd::Fill(fill) => {
            handle_fill(fill, portfolio, states, pending_next, cancel_timeouts)
        }
        QuoterCmd::CancelAck { token_id, order_id } => {
            handle_cancel_ack(
                token_id,
                order_id,
                cfg,
                sink,
                portfolio,
                states,
                pending_next,
                cancel_timeouts,
            )
            .await
        }
        QuoterCmd::QuerySnapshot { token_id, reply } => {
            let snap = states.get(&token_id).cloned();
            let _ = reply.send(snap);
        }
    }
}

async fn handle_signal<S: OrderSink + ?Sized>(
    signal: EdgeSignal,
    cfg: &Config,
    sink: &S,
    portfolio: &PortfolioHandle,
    states: &mut HashMap<U256, QState>,
    pending_next: &mut HashMap<U256, EdgeSignal>,
    cancel_timeouts: &mut HashMap<U256, Instant>,
) {
    let token_id = signal.token_id;
    let current = states.get(&token_id).cloned().unwrap_or(QState::Idle);
    let repost_thresh = cfg.no_edge_repost_threshold_cents as f64 / 100.0;

    match current {
        QState::Idle => {
            try_post(signal, cfg, sink, portfolio, states).await;
        }
        QState::Resting {
            order_id,
            price,
            size,
            posted_at_ns,
        } => {
            let diff = (signal.target_ask - price).abs();
            if diff < repost_thresh {
                // EdgeBook is already cooldown/threshold-filtered, but be
                // defensive: a signal that doesn't move the price is a
                // no-op, don't churn the CLOB.
                debug!(
                    token = %token_id,
                    diff_cents = diff * 100.0,
                    "[quoter] resting signal within repost threshold — no-op"
                );
                // Keep state as-is.
                states.insert(
                    token_id,
                    QState::Resting {
                        order_id,
                        price,
                        size,
                        posted_at_ns,
                    },
                );
                return;
            }
            // Replace: cancel now, post on cancel_ack.
            match sink.cancel_order(&order_id).await {
                Ok(()) => {
                    pending_next.insert(token_id, signal);
                    cancel_timeouts.insert(token_id, Instant::now());
                    states.insert(
                        token_id,
                        QState::Cancelling {
                            old_order_id: order_id,
                            orig_price: price,
                            orig_size: size,
                            cancel_sent_at_ns: now_ns(),
                        },
                    );
                }
                Err(e) => {
                    warn!(
                        token = %token_id,
                        error = %e,
                        "[quoter] cancel_order failed; leaving state Resting"
                    );
                    // Restore Resting — the order is still live from our
                    // perspective until we get evidence otherwise.
                    states.insert(
                        token_id,
                        QState::Resting {
                            order_id,
                            price,
                            size,
                            posted_at_ns,
                        },
                    );
                }
            }
        }
        QState::Cancelling {
            old_order_id,
            orig_price,
            orig_size,
            cancel_sent_at_ns,
        } => {
            // Stash the new target; we'll process it once the cancel acks.
            // Overwrite any prior pending_next — the newer signal wins.
            debug!(
                token = %token_id,
                target = signal.target_ask,
                "[quoter] signal while cancelling — queued as pending_next"
            );
            pending_next.insert(token_id, signal);
            states.insert(
                token_id,
                QState::Cancelling {
                    old_order_id,
                    orig_price,
                    orig_size,
                    cancel_sent_at_ns,
                },
            );
        }
    }
}

fn handle_fill(
    fill: FillEvent,
    portfolio: &PortfolioHandle,
    states: &mut HashMap<U256, QState>,
    pending_next: &mut HashMap<U256, EdgeSignal>,
    cancel_timeouts: &mut HashMap<U256, Instant>,
) {
    let token_id = fill.token_id;
    let Some(current) = states.get(&token_id).cloned() else {
        debug!(
            token = %token_id,
            order = %fill.order_id,
            "[quoter] fill for unknown token — ignoring"
        );
        return;
    };

    let (matches, was_cancelling) = match &current {
        QState::Resting { order_id, .. } => (order_id == &fill.order_id, false),
        QState::Cancelling { old_order_id, .. } => (old_order_id == &fill.order_id, true),
        QState::Idle => (false, false),
    };

    if !matches {
        debug!(
            token = %token_id,
            order = %fill.order_id,
            "[quoter] fill order_id does not match current state — ignoring"
        );
        return;
    }

    // Record the fill. Adverse detection is EdgeBook's responsibility (it
    // recomputes fair and decides on the next signal); the quoter treats
    // every matching fill as accepted.
    portfolio.record_fill(&fill.order_id, fill.size, fill.price, false);

    if was_cancelling {
        // Race-lost-by-cancel: the taker crossed while our cancel was in
        // flight. Noteworthy but not an error — log at info level.
        info!(
            token = %token_id,
            order = %fill.order_id,
            size = fill.size,
            price = fill.price,
            "[quoter] fill arrived during Cancelling — taker won the race"
        );
    }

    states.insert(token_id, QState::Idle);
    pending_next.remove(&token_id);
    cancel_timeouts.remove(&token_id);
}

#[allow(clippy::too_many_arguments)]
async fn handle_cancel_ack<S: OrderSink + ?Sized>(
    token_id: U256,
    order_id: String,
    cfg: &Config,
    sink: &S,
    portfolio: &PortfolioHandle,
    states: &mut HashMap<U256, QState>,
    pending_next: &mut HashMap<U256, EdgeSignal>,
    cancel_timeouts: &mut HashMap<U256, Instant>,
) {
    // Classify against a snapshot so we don't hold a borrow on `states`
    // across the mutable transition below.
    let classification: CancelAckClass = match states.get(&token_id) {
        None => CancelAckClass::Unknown,
        Some(QState::Cancelling { old_order_id, .. }) if old_order_id == &order_id => {
            CancelAckClass::MatchedCancelling {
                old_order_id: old_order_id.clone(),
            }
        }
        Some(QState::Cancelling { old_order_id, .. }) => CancelAckClass::CancellingMismatch {
            current_order: old_order_id.clone(),
        },
        Some(QState::Idle) => CancelAckClass::Idle,
        Some(QState::Resting { order_id, .. }) => CancelAckClass::Resting {
            current_order: order_id.clone(),
        },
    };

    match classification {
        CancelAckClass::Unknown => {
            debug!(
                token = %token_id,
                order = %order_id,
                "[quoter] cancel_ack for unknown token — ignoring"
            );
        }
        CancelAckClass::MatchedCancelling { old_order_id } => {
            portfolio.record_cancel(&old_order_id);
            cancel_timeouts.remove(&token_id);
            // Transition through Idle so `try_post` sees a clean slate if
            // there's a pending_next to re-launch.
            states.insert(token_id, QState::Idle);
            if let Some(next) = pending_next.remove(&token_id) {
                try_post(next, cfg, sink, portfolio, states).await;
            }
        }
        CancelAckClass::CancellingMismatch { current_order } => {
            debug!(
                token = %token_id,
                ack_order = %order_id,
                current_order = %current_order,
                "[quoter] cancel_ack order_id mismatch — ignoring"
            );
        }
        CancelAckClass::Idle => {
            debug!(
                token = %token_id,
                order = %order_id,
                "[quoter] cancel_ack while Idle — ignoring"
            );
        }
        CancelAckClass::Resting { current_order } => {
            debug!(
                token = %token_id,
                order = %order_id,
                resting_order = %current_order,
                "[quoter] cancel_ack while Resting — ignoring"
            );
        }
    }
}

/// Small classification enum to avoid tripping the borrow checker inside
/// `handle_cancel_ack` (we want to inspect state, decide what to do, then
/// mutate the map — doing both in a single `match` would nest a mutable
/// borrow inside an immutable one).
enum CancelAckClass {
    Unknown,
    MatchedCancelling { old_order_id: String },
    CancellingMismatch { current_order: String },
    Idle,
    Resting { current_order: String },
}

// ---------------------------------------------------------------------------
// Post helper
// ---------------------------------------------------------------------------

async fn try_post<S: OrderSink + ?Sized>(
    signal: EdgeSignal,
    cfg: &Config,
    sink: &S,
    portfolio: &PortfolioHandle,
    states: &mut HashMap<U256, QState>,
) {
    let token_id = signal.token_id;
    let bucket = signal.bucket.clone();
    let event_slug = format!("{}:{}", bucket.city, bucket.event_date);
    let notional = signal.desired_size_shares * signal.target_ask;

    let can = portfolio
        .can_deploy(bucket.city, &event_slug, notional)
        .await;
    if can != CanDeployResponse::Ok {
        debug!(
            token = %token_id,
            city = %bucket.city,
            notional,
            response = ?can,
            "[quoter] portfolio rejected deploy — staying Idle"
        );
        states.insert(token_id, QState::Idle);
        return;
    }

    // EdgeBook computes `target_ask = fair_p_no - min_edge`; we reconstruct
    // both for the PaperEngine adverse-fill check. The live Executor impl
    // drops both args so this is purely bookkeeping.
    let min_edge_at_post = cfg.no_edge_min_edge_bps as f64 / 10_000.0;
    let fair_p_no_at_post = signal.target_ask + min_edge_at_post;

    match sink
        .post_limit_order(
            token_id,
            signal.target_ask,
            signal.desired_size_shares,
            Side::Sell,
            fair_p_no_at_post,
            min_edge_at_post,
        )
        .await
    {
        Ok(order_id) => {
            let posted_at_ns = now_ns();
            let known = KnownOrder {
                order_id: order_id.clone(),
                token_id: token_id.to_string(),
                condition_id: bucket.condition_id.to_string(),
                city: bucket.city.to_string(),
                event_date: bucket.event_date.to_string(),
                bucket_label: bucket.bucket_label.clone(),
                side: "sell".to_string(),
                price: signal.target_ask,
                size_shares: signal.desired_size_shares,
                notional_usdc: notional,
                posted_at_ns,
            };
            portfolio.record_post(known);
            states.insert(
                token_id,
                QState::Resting {
                    order_id,
                    price: signal.target_ask,
                    size: signal.desired_size_shares,
                    posted_at_ns,
                },
            );
        }
        Err(e) => {
            warn!(
                token = %token_id,
                error = %e,
                "[quoter] post_limit_order failed — staying Idle"
            );
            states.insert(token_id, QState::Idle);
        }
    }
}

// ---------------------------------------------------------------------------
// Cancel-timeout watchdog
// ---------------------------------------------------------------------------

fn check_cancel_timeouts(
    states: &HashMap<U256, QState>,
    cancel_timeouts: &mut HashMap<U256, Instant>,
) {
    let now = Instant::now();
    let threshold = Duration::from_secs(CANCEL_TIMEOUT_SECS);
    // Drain the expired set; we re-add survivors.
    let expired: Vec<U256> = cancel_timeouts
        .iter()
        .filter_map(|(k, t)| {
            if now.duration_since(*t) >= threshold {
                Some(*k)
            } else {
                None
            }
        })
        .collect();
    for token_id in expired {
        let age = cancel_timeouts
            .remove(&token_id)
            .map(|t| now.duration_since(t).as_secs())
            .unwrap_or(0);
        // Leave state as Cancelling — live ops must decide.
        let order = match states.get(&token_id) {
            Some(QState::Cancelling { old_order_id, .. }) => old_order_id.clone(),
            _ => String::from("<unknown>"),
        };
        warn!(
            token = %token_id,
            order_id = %order,
            age_s = age,
            "[quoter] cancel timeout: manual intervention may be needed"
        );
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge_book::BucketContext;
    use crate::climo::Unit;
    use crate::portfolio::spawn_portfolio;
    use alloy_primitives::B256;
    use anyhow::anyhow;
    use async_trait::async_trait;
    use chrono::NaiveDate;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    // -------- Stub OrderSink ------------------------------------------------

    #[derive(Debug, Clone)]
    enum StubCall {
        Post {
            token_id: U256,
            price: f64,
            size: f64,
            side: Side,
            fair_p_no: f64,
            min_edge: f64,
        },
        Cancel {
            order_id: String,
        },
    }

    struct StubSink {
        calls: Mutex<Vec<StubCall>>,
        post_order_id: Mutex<String>,
        post_fail: Mutex<bool>,
    }

    impl StubSink {
        fn new(order_id: &str) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                post_order_id: Mutex::new(order_id.to_string()),
                post_fail: Mutex::new(false),
            })
        }

        fn set_post_order_id(&self, id: &str) {
            *self.post_order_id.lock().unwrap() = id.to_string();
        }

        fn set_post_fail(&self, fail: bool) {
            *self.post_fail.lock().unwrap() = fail;
        }

        fn calls(&self) -> Vec<StubCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OrderSink for StubSink {
        async fn post_limit_order(
            &self,
            token_id: U256,
            price: f64,
            size: f64,
            side: Side,
            fair_p_no_at_post: f64,
            min_edge_at_post: f64,
        ) -> anyhow::Result<String> {
            self.calls.lock().unwrap().push(StubCall::Post {
                token_id,
                price,
                size,
                side,
                fair_p_no: fair_p_no_at_post,
                min_edge: min_edge_at_post,
            });
            if *self.post_fail.lock().unwrap() {
                Err(anyhow!("stub post failure"))
            } else {
                Ok(self.post_order_id.lock().unwrap().clone())
            }
        }

        async fn cancel_order(&self, order_id: &str) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(StubCall::Cancel {
                order_id: order_id.to_string(),
            });
            Ok(())
        }
    }

    // -------- Test fixtures -------------------------------------------------

    fn tmp_state_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("quoter_test_{tag}_{nanos}.json"))
    }

    fn test_cfg(state_path: PathBuf) -> Config {
        let mut cfg = Config::default();
        cfg.no_edge_state_path = state_path.to_string_lossy().to_string();
        cfg.no_edge_min_edge_bps = 500;
        cfg.no_edge_repost_threshold_cents = 5;
        cfg.no_edge_repost_cooldown_secs = 0;
        cfg.no_edge_max_notional_per_market_usdc = 150.0;
        cfg.no_edge_max_notional_per_event_usdc = 500.0;
        cfg.no_edge_max_notional_per_city_usdc = 800.0;
        cfg.no_edge_max_total_deployed_usdc = 2000.0;
        cfg.no_edge_max_open_orders = 60;
        cfg
    }

    fn mk_bucket() -> BucketContext {
        BucketContext {
            city: "nyc",
            icao: "KNYC",
            unit: Unit::Fahrenheit,
            event_date: NaiveDate::from_ymd_opt(2026, 4, 16).unwrap(),
            bucket_lo: Some(60.0),
            bucket_hi: Some(61.0),
            tail: None,
            condition_id: B256::from([0x11u8; 32]),
            token_id_no: U256::from(1234u64),
            bucket_label: "60-61F".to_string(),
        }
    }

    fn mk_signal(token_id: U256, target_ask: f64, size: f64) -> EdgeSignal {
        EdgeSignal {
            token_id,
            bucket: BucketContext {
                token_id_no: token_id,
                ..mk_bucket()
            },
            target_ask,
            desired_size_shares: size,
            reason: "test",
            computed_at_ns: 0,
        }
    }

    fn mk_fill(token_id: U256, order_id: &str, price: f64, size: f64) -> FillEvent {
        FillEvent {
            token_id,
            order_id: order_id.to_string(),
            side: Side::Sell,
            price,
            size,
            matched_at_ms: 0,
            status: "MATCHED".to_string(),
        }
    }

    struct Harness {
        cfg: Config,
        sink: Arc<StubSink>,
        portfolio: PortfolioHandle,
        states: HashMap<U256, QState>,
        pending_next: HashMap<U256, EdgeSignal>,
        cancel_timeouts: HashMap<U256, Instant>,
        state_path: PathBuf,
    }

    impl Harness {
        fn new(tag: &str, first_order_id: &str) -> Self {
            Self::with_cfg(tag, first_order_id, test_cfg(tmp_state_path(tag)))
        }

        fn with_cfg(tag: &str, first_order_id: &str, cfg_override: Config) -> Self {
            // If the caller passed a cfg with a default state path, replace
            // it with a tagged temp path so parallel tests don't collide.
            let mut cfg = cfg_override;
            if cfg.no_edge_state_path == Config::default().no_edge_state_path {
                cfg.no_edge_state_path = tmp_state_path(tag).to_string_lossy().to_string();
            }
            let state_path = PathBuf::from(&cfg.no_edge_state_path);
            let sink = StubSink::new(first_order_id);
            // Portfolio caps are snapshotted at spawn time — callers that
            // need tight caps must set them on `cfg_override` before the
            // Harness is constructed.
            let (portfolio, _join) = spawn_portfolio(&cfg).expect("spawn portfolio");
            Self {
                cfg,
                sink,
                portfolio,
                states: HashMap::new(),
                pending_next: HashMap::new(),
                cancel_timeouts: HashMap::new(),
                state_path,
            }
        }

        async fn dispatch(&mut self, cmd: QuoterCmd) {
            handle_cmd(
                cmd,
                &self.cfg,
                self.sink.as_ref(),
                &self.portfolio,
                &mut self.states,
                &mut self.pending_next,
                &mut self.cancel_timeouts,
            )
            .await;
        }

        async fn snapshot_via_cmd(&mut self, token_id: U256) -> Option<QState> {
            let (reply, rx) = oneshot::channel();
            self.dispatch(QuoterCmd::QuerySnapshot { token_id, reply })
                .await;
            rx.await.unwrap()
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.state_path);
        }
    }

    // -------- Tests ---------------------------------------------------------

    #[tokio::test]
    async fn idle_signal_posts_new_order() {
        let mut h = Harness::new("idle_post", "ord-1");
        let tok = U256::from(1u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        let calls = h.sink.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            StubCall::Post { price, size, .. } => {
                assert!((*price - 0.50).abs() < 1e-9);
                assert!((*size - 100.0).abs() < 1e-9);
            }
            _ => panic!("expected Post call"),
        }
    }

    #[tokio::test]
    async fn idle_signal_transitions_to_resting() {
        let mut h = Harness::new("idle_to_resting", "ord-A");
        let tok = U256::from(2u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.60, 50.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Resting {
                order_id,
                price,
                size,
                ..
            }) => {
                assert_eq!(order_id, "ord-A");
                assert!((*price - 0.60).abs() < 1e-9);
                assert!((*size - 50.0).abs() < 1e-9);
            }
            other => panic!("expected Resting, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resting_same_price_signal_is_noop() {
        let mut h = Harness::new("resting_same", "ord-1");
        let tok = U256::from(3u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;
        // Second signal at an identical price — well under the 5¢ threshold.
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        let calls = h.sink.calls();
        assert_eq!(calls.len(), 1, "only the first post should have fired");
        match h.states.get(&tok) {
            Some(QState::Resting { price, .. }) => assert!((*price - 0.50).abs() < 1e-9),
            other => panic!("expected Resting, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resting_signal_within_threshold_is_noop() {
        let mut h = Harness::new("resting_within", "ord-1");
        let tok = U256::from(4u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;
        // 2¢ move — under the 5¢ repost threshold.
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.48, 100.0)))
            .await;

        let calls = h.sink.calls();
        assert_eq!(calls.len(), 1);
    }

    #[tokio::test]
    async fn resting_signal_beyond_threshold_cancels_and_pending_next() {
        let mut h = Harness::new("resting_beyond", "ord-1");
        let tok = U256::from(5u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.70, 100.0)))
            .await;
        // 10¢ move — above the 5¢ threshold.
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.60, 100.0)))
            .await;

        // Expect: Post + Cancel in calls; state is Cancelling.
        let calls = h.sink.calls();
        assert_eq!(calls.len(), 2);
        assert!(matches!(calls[0], StubCall::Post { .. }));
        match &calls[1] {
            StubCall::Cancel { order_id } => assert_eq!(order_id, "ord-1"),
            _ => panic!("expected Cancel"),
        }
        match h.states.get(&tok) {
            Some(QState::Cancelling { old_order_id, .. }) => {
                assert_eq!(old_order_id, "ord-1");
            }
            other => panic!("expected Cancelling, got {other:?}"),
        }
        assert!(h.pending_next.contains_key(&tok));
        assert!(h.cancel_timeouts.contains_key(&tok));
    }

    #[tokio::test]
    async fn cancelling_signal_overwrites_pending_next() {
        let mut h = Harness::new("cancelling_overwrite", "ord-1");
        let tok = U256::from(6u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.70, 100.0)))
            .await;
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.60, 100.0)))
            .await;
        // Now in Cancelling with pending_next at 0.60.
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.55, 100.0)))
            .await;

        // Should not have issued a second cancel.
        let cancel_count = h
            .sink
            .calls()
            .into_iter()
            .filter(|c| matches!(c, StubCall::Cancel { .. }))
            .count();
        assert_eq!(cancel_count, 1);
        // pending_next should hold the newest.
        let next = h.pending_next.get(&tok).unwrap();
        assert!((next.target_ask - 0.55).abs() < 1e-9);
    }

    #[tokio::test]
    async fn cancelling_cancel_ack_transitions_to_idle_when_no_pending() {
        let mut h = Harness::new("cancel_ack_idle", "ord-1");
        let tok = U256::from(7u64);
        // Manually seed Cancelling state with no pending_next so we don't
        // have to race the Signal path.
        h.states.insert(
            tok,
            QState::Cancelling {
                old_order_id: "ord-1".to_string(),
                orig_price: 0.70,
                orig_size: 100.0,
                cancel_sent_at_ns: now_ns(),
            },
        );
        h.cancel_timeouts.insert(tok, Instant::now());

        h.dispatch(QuoterCmd::CancelAck {
            token_id: tok,
            order_id: "ord-1".to_string(),
        })
        .await;

        match h.states.get(&tok) {
            Some(QState::Idle) => {}
            other => panic!("expected Idle, got {other:?}"),
        }
        assert!(!h.cancel_timeouts.contains_key(&tok));
        assert!(!h.pending_next.contains_key(&tok));
    }

    #[tokio::test]
    async fn cancelling_cancel_ack_posts_pending_next_when_set() {
        let mut h = Harness::new("cancel_ack_repost", "ord-1");
        let tok = U256::from(8u64);
        // Seed the state then inject a pending_next so we can observe the repost.
        h.states.insert(
            tok,
            QState::Cancelling {
                old_order_id: "ord-1".to_string(),
                orig_price: 0.70,
                orig_size: 100.0,
                cancel_sent_at_ns: now_ns(),
            },
        );
        h.cancel_timeouts.insert(tok, Instant::now());
        h.pending_next.insert(tok, mk_signal(tok, 0.55, 80.0));
        h.sink.set_post_order_id("ord-2");

        h.dispatch(QuoterCmd::CancelAck {
            token_id: tok,
            order_id: "ord-1".to_string(),
        })
        .await;

        // Should end up Resting under the new order_id.
        match h.states.get(&tok) {
            Some(QState::Resting {
                order_id, price, ..
            }) => {
                assert_eq!(order_id, "ord-2");
                assert!((*price - 0.55).abs() < 1e-9);
            }
            other => panic!("expected Resting, got {other:?}"),
        }
        assert!(h.pending_next.is_empty());
    }

    #[tokio::test]
    async fn cancelling_fill_recorded_and_transitions_to_idle() {
        let mut h = Harness::new("cancel_lost_race", "ord-1");
        let tok = U256::from(9u64);
        h.states.insert(
            tok,
            QState::Cancelling {
                old_order_id: "ord-1".to_string(),
                orig_price: 0.60,
                orig_size: 100.0,
                cancel_sent_at_ns: now_ns(),
            },
        );
        h.cancel_timeouts.insert(tok, Instant::now());
        h.pending_next.insert(tok, mk_signal(tok, 0.55, 100.0));

        h.dispatch(QuoterCmd::Fill(mk_fill(tok, "ord-1", 0.60, 100.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Idle) => {}
            other => panic!("expected Idle after fill, got {other:?}"),
        }
        assert!(!h.cancel_timeouts.contains_key(&tok));
        assert!(
            !h.pending_next.contains_key(&tok),
            "pending_next should be cleared on fill"
        );
    }

    #[tokio::test]
    async fn resting_fill_recorded_and_transitions_to_idle() {
        let mut h = Harness::new("resting_fill", "ord-1");
        let tok = U256::from(10u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        h.dispatch(QuoterCmd::Fill(mk_fill(tok, "ord-1", 0.50, 100.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Idle) => {}
            other => panic!("expected Idle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resting_fill_mismatched_order_id_is_ignored() {
        let mut h = Harness::new("resting_mismatch", "ord-real");
        let tok = U256::from(11u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        // Stale fill for a different order_id on the same token.
        h.dispatch(QuoterCmd::Fill(mk_fill(tok, "ord-ghost", 0.50, 100.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Resting { order_id, .. }) => assert_eq!(order_id, "ord-real"),
            other => panic!("expected Resting unchanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_ack_for_unknown_token_logs_debug_and_noop() {
        let mut h = Harness::new("ack_unknown", "ord-1");
        let tok = U256::from(12u64);

        h.dispatch(QuoterCmd::CancelAck {
            token_id: tok,
            order_id: "ord-nope".to_string(),
        })
        .await;

        assert!(h.states.get(&tok).is_none());
        assert!(h.sink.calls().is_empty());
    }

    #[tokio::test]
    async fn portfolio_cap_rejection_keeps_state_idle() {
        // Portfolio snapshots caps at spawn time, so set the tight cap
        // *before* constructing the Harness.
        let mut cfg = test_cfg(tmp_state_path("cap_reject"));
        cfg.no_edge_max_notional_per_event_usdc = 0.01;
        let mut h = Harness::with_cfg("cap_reject", "ord-1", cfg);
        let tok = U256::from(13u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Idle) => {}
            other => panic!("expected Idle, got {other:?}"),
        }
        // No CLOB activity — the rejection happened before sink.post.
        let posts = h
            .sink
            .calls()
            .into_iter()
            .filter(|c| matches!(c, StubCall::Post { .. }))
            .count();
        assert_eq!(posts, 0);
    }

    #[tokio::test]
    async fn stub_sink_post_failure_keeps_state_idle() {
        let mut h = Harness::new("post_fail", "ord-1");
        h.sink.set_post_fail(true);
        let tok = U256::from(14u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        match h.states.get(&tok) {
            Some(QState::Idle) => {}
            other => panic!("expected Idle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_timeout_logs_alert_but_keeps_state_cancelling() {
        // Drive the timeout manually — insert a cancel_timeout entry with an
        // Instant in the past, then call `check_cancel_timeouts`.
        let mut h = Harness::new("cancel_timeout", "ord-1");
        let tok = U256::from(15u64);
        h.states.insert(
            tok,
            QState::Cancelling {
                old_order_id: "ord-1".to_string(),
                orig_price: 0.60,
                orig_size: 100.0,
                cancel_sent_at_ns: now_ns(),
            },
        );
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(CANCEL_TIMEOUT_SECS + 5))
            .expect("instant math");
        h.cancel_timeouts.insert(tok, long_ago);

        check_cancel_timeouts(&h.states, &mut h.cancel_timeouts);

        // Alert fired (cancel_timeout drained) but Cancelling is preserved.
        assert!(!h.cancel_timeouts.contains_key(&tok));
        match h.states.get(&tok) {
            Some(QState::Cancelling { old_order_id, .. }) => assert_eq!(old_order_id, "ord-1"),
            other => panic!("expected Cancelling preserved, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn query_snapshot_returns_cloned_state() {
        let mut h = Harness::new("snapshot", "ord-1");
        let tok = U256::from(16u64);
        h.dispatch(QuoterCmd::Signal(mk_signal(tok, 0.50, 100.0)))
            .await;

        let snap = h.snapshot_via_cmd(tok).await.expect("state present");
        match snap {
            QState::Resting { order_id, .. } => assert_eq!(order_id, "ord-1"),
            other => panic!("expected Resting snapshot, got {other:?}"),
        }

        let ghost = U256::from(99999u64);
        let snap_none = h.snapshot_via_cmd(ghost).await;
        assert!(snap_none.is_none());
    }
}
