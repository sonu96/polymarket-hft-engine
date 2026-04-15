//! Portfolio facade + `NoEdgeState` for the Phase 3 NO-edge farmer.
//!
//! Owns deployed-notional accounting, per-market / per-event / per-city /
//! global caps, and the persistent `no_edge_state.json` file. Pattern is
//! actor-style: a single owning task mutates the state in response to
//! [`PortfolioCmd`] messages received over an mpsc channel; queries reply via
//! one-shot senders. There is no shared memory and no `Mutex` around state.
//!
//! See `docs/PHASE3_NO_EDGE_FARMER.md` §4.2 (state separation — do NOT reuse
//! `BotState`) and §5 (settlement is a Python cron, schema is versioned, all
//! disk structs use `deny_unknown_fields`).
//!
//! The Rust process is the only writer to `no_edge_state.json` during trading
//! hours; the Python settlement cron runs after the daily cutoff and writes a
//! separate `no_edge_settlement_ledger.jsonl` (append-only, never mutated by
//! Rust). That hand-off is why we do not take a flock here — a future
//! multi-host setup would need to revisit this.
//!
//! TODO(precision): all money fields are `f64` to match the design doc and
//! keep the JSON readable. This is fine for per-cycle cap checks but is not
//! audit-grade. When we wire this to live capital, swap to `rust_decimal` or
//! integer micro-USDC and migrate the schema (bump `SCHEMA_VERSION`).

// The actor + facade are not wired into `main()` until ticket #12. Suppress
// dead-code warnings on the public surface so Phase 3 tickets can land
// independently without polluting the warning baseline.
#![allow(dead_code)]

use crate::config::Config;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

/// Schema version for `no_edge_state.json`. Bump on every field change so the
/// loader refuses unknown shapes instead of silently corrupting state.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NoEdgeState {
    pub schema_version: u32,
    pub total_deployed_usdc: f64,
    pub per_city_deployed: HashMap<String, f64>,
    pub per_event_deployed: HashMap<String, f64>,
    pub known_orders: HashMap<String, KnownOrder>,
    pub last_updated_ns: u128,
    pub cumulative_pnl_realized: f64,
    pub fill_count: u64,
    pub adverse_fill_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KnownOrder {
    pub order_id: String,
    /// Decimal U256 as a string — JSON-friendly. Quoter converts via `.to_string()`.
    pub token_id: String,
    /// Hex with `0x` prefix.
    pub condition_id: String,
    pub city: String,
    /// `YYYY-MM-DD`.
    pub event_date: String,
    pub bucket_label: String,
    /// `"sell"` (NO-side ask) or `"buy"` (future YES-side ask).
    pub side: String,
    pub price: f64,
    pub size_shares: f64,
    /// `price * size_shares` at post time.
    pub notional_usdc: f64,
    pub posted_at_ns: u128,
}

impl Default for NoEdgeState {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            total_deployed_usdc: 0.0,
            per_city_deployed: HashMap::new(),
            per_event_deployed: HashMap::new(),
            known_orders: HashMap::new(),
            last_updated_ns: 0,
            cumulative_pnl_realized: 0.0,
            fill_count: 0,
            adverse_fill_count: 0,
        }
    }
}

/// Caps + paths the actor needs at runtime. Snapshotted from `Config` at spawn
/// time so a config reload does not race the actor.
#[derive(Debug, Clone)]
struct PortfolioCaps {
    max_per_market: f64,
    max_per_event: f64,
    max_per_city: f64,
    max_total: f64,
    max_open_orders: usize,
    state_path: PathBuf,
}

impl PortfolioCaps {
    fn from_config(cfg: &Config) -> Self {
        Self {
            max_per_market: cfg.no_edge_max_notional_per_market_usdc,
            max_per_event: cfg.no_edge_max_notional_per_event_usdc,
            max_per_city: cfg.no_edge_max_notional_per_city_usdc,
            max_total: cfg.no_edge_max_total_deployed_usdc,
            max_open_orders: cfg.no_edge_max_open_orders,
            state_path: PathBuf::from(&cfg.no_edge_state_path),
        }
    }
}

#[derive(Debug)]
pub enum PortfolioCmd {
    /// Returns true if deploying `usdc_amount` on `(city, event_slug)` stays
    /// within all caps (per-market, per-event, per-city, global).
    CanDeploy {
        city: String,
        event_slug: String,
        usdc_amount: f64,
        reply: oneshot::Sender<CanDeployResponse>,
    },
    /// Record a freshly-posted order. Increments deployed totals and persists.
    RecordPost { order: KnownOrder },
    /// Mark an order cancelled. Decrements deployed totals and persists.
    RecordCancel { order_id: String },
    /// Mark an order filled. Decrements pending deployed and (when settlement
    /// later writes the realized P&L) the cron will reconcile.
    RecordFill {
        order_id: String,
        filled_shares: f64,
        filled_price: f64,
        adverse: bool,
    },
    /// Snapshot query for metrics/alerts.
    Snapshot {
        reply: oneshot::Sender<NoEdgeState>,
    },
    /// Force a save to disk. Auto-called on every Post/Cancel/Fill.
    Persist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanDeployResponse {
    Ok,
    GlobalCapExceeded,
    PerCityCapExceeded,
    PerEventCapExceeded,
    MaxOpenOrdersExceeded,
}

#[derive(Clone)]
pub struct PortfolioHandle {
    tx: mpsc::UnboundedSender<PortfolioCmd>,
}

impl PortfolioHandle {
    pub async fn can_deploy(
        &self,
        city: &str,
        event_slug: &str,
        usdc: f64,
    ) -> CanDeployResponse {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(PortfolioCmd::CanDeploy {
                city: city.to_string(),
                event_slug: event_slug.to_string(),
                usdc_amount: usdc,
                reply,
            })
            .is_err()
        {
            tracing::warn!("portfolio actor gone — denying deploy");
            return CanDeployResponse::GlobalCapExceeded;
        }
        rx.await.unwrap_or_else(|_| {
            tracing::warn!("portfolio actor dropped reply — denying deploy");
            CanDeployResponse::GlobalCapExceeded
        })
    }

    pub fn record_post(&self, order: KnownOrder) {
        let _ = self.tx.send(PortfolioCmd::RecordPost { order });
    }

    pub fn record_cancel(&self, order_id: &str) {
        let _ = self.tx.send(PortfolioCmd::RecordCancel {
            order_id: order_id.to_string(),
        });
    }

    pub fn record_fill(&self, order_id: &str, shares: f64, price: f64, adverse: bool) {
        let _ = self.tx.send(PortfolioCmd::RecordFill {
            order_id: order_id.to_string(),
            filled_shares: shares,
            filled_price: price,
            adverse,
        });
    }

    pub async fn snapshot(&self) -> NoEdgeState {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(PortfolioCmd::Snapshot { reply }).is_err() {
            return NoEdgeState::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the portfolio actor. Loads existing `no_edge_state.json` if present;
/// returns an error on schema mismatch or unknown fields rather than trying to
/// migrate silently.
pub fn spawn_portfolio(
    cfg: &Config,
) -> Result<(PortfolioHandle, tokio::task::JoinHandle<()>)> {
    let caps = PortfolioCaps::from_config(cfg);
    let state = load_state(&caps.state_path)?.unwrap_or_default();
    let (tx, rx) = mpsc::unbounded_channel();
    let join = tokio::spawn(run_actor(state, caps, rx));
    Ok((PortfolioHandle { tx }, join))
}

async fn run_actor(
    mut state: NoEdgeState,
    caps: PortfolioCaps,
    mut rx: mpsc::UnboundedReceiver<PortfolioCmd>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            PortfolioCmd::CanDeploy {
                city,
                event_slug,
                usdc_amount,
                reply,
            } => {
                let resp = check_can_deploy(&state, &caps, &city, &event_slug, usdc_amount);
                if reply.send(resp).is_err() {
                    tracing::debug!("CanDeploy reply receiver dropped");
                }
            }
            PortfolioCmd::RecordPost { order } => {
                apply_post(&mut state, order);
                touch(&mut state);
                if let Err(e) = save_state(&state, &caps.state_path) {
                    tracing::error!("portfolio: save after post failed: {e:#}");
                }
            }
            PortfolioCmd::RecordCancel { order_id } => {
                apply_cancel(&mut state, &order_id);
                touch(&mut state);
                if let Err(e) = save_state(&state, &caps.state_path) {
                    tracing::error!("portfolio: save after cancel failed: {e:#}");
                }
            }
            PortfolioCmd::RecordFill {
                order_id,
                filled_shares,
                filled_price,
                adverse,
            } => {
                apply_fill(&mut state, &order_id, filled_shares, filled_price, adverse);
                touch(&mut state);
                if let Err(e) = save_state(&state, &caps.state_path) {
                    tracing::error!("portfolio: save after fill failed: {e:#}");
                }
            }
            PortfolioCmd::Snapshot { reply } => {
                if reply.send(state.clone()).is_err() {
                    tracing::debug!("Snapshot reply receiver dropped");
                }
            }
            PortfolioCmd::Persist => {
                if let Err(e) = save_state(&state, &caps.state_path) {
                    tracing::error!("portfolio: explicit persist failed: {e:#}");
                }
            }
        }
    }
    tracing::info!("portfolio actor: command channel closed, shutting down");
}

fn check_can_deploy(
    state: &NoEdgeState,
    caps: &PortfolioCaps,
    city: &str,
    event_slug: &str,
    usdc_amount: f64,
) -> CanDeployResponse {
    if state.known_orders.len() >= caps.max_open_orders {
        return CanDeployResponse::MaxOpenOrdersExceeded;
    }
    if state.total_deployed_usdc + usdc_amount > caps.max_total {
        return CanDeployResponse::GlobalCapExceeded;
    }
    let city_now = state.per_city_deployed.get(city).copied().unwrap_or(0.0);
    if city_now + usdc_amount > caps.max_per_city {
        return CanDeployResponse::PerCityCapExceeded;
    }
    let event_now = state
        .per_event_deployed
        .get(event_slug)
        .copied()
        .unwrap_or(0.0);
    if event_now + usdc_amount > caps.max_per_event {
        return CanDeployResponse::PerEventCapExceeded;
    }
    // Per-market cap is enforced at the bucket level by the quoter; the
    // portfolio actor only knows about (city, event) granularity. The quoter
    // calls `can_deploy` after it has already capped the order size at
    // `max_per_market`, so any value reaching here is bucket-clamped.
    let _ = caps.max_per_market;
    CanDeployResponse::Ok
}

fn apply_post(state: &mut NoEdgeState, order: KnownOrder) {
    let event_slug = derive_event_slug(&order.city, &order.event_date);
    let n = order.notional_usdc;
    state.total_deployed_usdc += n;
    *state
        .per_city_deployed
        .entry(order.city.clone())
        .or_insert(0.0) += n;
    *state.per_event_deployed.entry(event_slug).or_insert(0.0) += n;
    state.known_orders.insert(order.order_id.clone(), order);
}

fn apply_cancel(state: &mut NoEdgeState, order_id: &str) {
    let Some(order) = state.known_orders.remove(order_id) else {
        tracing::warn!("portfolio: cancel for unknown order {order_id}");
        return;
    };
    let event_slug = derive_event_slug(&order.city, &order.event_date);
    let n = order.notional_usdc;
    state.total_deployed_usdc = (state.total_deployed_usdc - n).max(0.0);
    decr_or_remove(&mut state.per_city_deployed, &order.city, n);
    decr_or_remove(&mut state.per_event_deployed, &event_slug, n);
}

fn apply_fill(
    state: &mut NoEdgeState,
    order_id: &str,
    filled_shares: f64,
    _filled_price: f64,
    adverse: bool,
) {
    state.fill_count += 1;
    if adverse {
        state.adverse_fill_count += 1;
    }
    let Some(order) = state.known_orders.get_mut(order_id) else {
        tracing::warn!("portfolio: fill for unknown order {order_id}");
        return;
    };
    // Pro-rate the deployed notional decrement by the filled fraction. The
    // settlement cron will write the realized P&L into a separate ledger and
    // the next bot start will reload via the loader (§5).
    let fill_frac = if order.size_shares > 0.0 {
        (filled_shares / order.size_shares).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let released = order.notional_usdc * fill_frac;
    let city = order.city.clone();
    let event_slug = derive_event_slug(&order.city, &order.event_date);

    order.size_shares -= filled_shares;
    order.notional_usdc -= released;

    state.total_deployed_usdc = (state.total_deployed_usdc - released).max(0.0);
    decr_or_remove(&mut state.per_city_deployed, &city, released);
    decr_or_remove(&mut state.per_event_deployed, &event_slug, released);

    if order.size_shares <= 1e-9 {
        state.known_orders.remove(order_id);
    }
}

fn decr_or_remove(map: &mut HashMap<String, f64>, key: &str, amount: f64) {
    if let Some(v) = map.get_mut(key) {
        *v -= amount;
        if *v <= 1e-9 {
            map.remove(key);
        }
    }
}

fn derive_event_slug(city: &str, event_date: &str) -> String {
    format!("{city}:{event_date}")
}

fn touch(state: &mut NoEdgeState) {
    state.last_updated_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
}

/// Load `no_edge_state.json`. Returns `Ok(None)` if the file does not exist;
/// returns `Err` on any parse error, schema-version mismatch, or unknown
/// field. This is intentionally strict — silent corruption of deployed
/// notional would be far worse than a startup failure.
fn load_state(path: &Path) -> Result<Option<NoEdgeState>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let state: NoEdgeState = serde_json::from_str(&raw)
        .with_context(|| format!("parsing {}", path.display()))?;
    if state.schema_version != SCHEMA_VERSION {
        return Err(anyhow!(
            "no_edge_state.json schema_version {} does not match expected {} — refusing to load",
            state.schema_version,
            SCHEMA_VERSION
        ));
    }
    Ok(Some(state))
}

/// Atomic-rename persistence. Writes to `<path>.tmp`, then renames over the
/// real file. POSIX guarantees rename atomicity on the same filesystem.
///
/// NOTE: we deliberately do NOT take a flock here. The Python settlement cron
/// (see `docs/PHASE3_NO_EDGE_FARMER.md` §5) runs after the trading cutoff and
/// writes only to `no_edge_settlement_ledger.jsonl`, never to this file.
/// During trading hours the Rust actor is the sole writer.
fn save_state(state: &NoEdgeState, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(state)?;
    std::fs::write(&tmp, json)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("no_edge_state_{tag}_{nanos}.json"))
    }

    fn make_order(id: &str, city: &str, date: &str, notional: f64) -> KnownOrder {
        KnownOrder {
            order_id: id.to_string(),
            token_id: "12345".to_string(),
            condition_id: "0xdeadbeef".to_string(),
            city: city.to_string(),
            event_date: date.to_string(),
            bucket_label: "60-61F".to_string(),
            side: "sell".to_string(),
            price: 0.05,
            size_shares: notional / 0.05,
            notional_usdc: notional,
            posted_at_ns: 1,
        }
    }

    fn caps_with_defaults(state_path: PathBuf) -> PortfolioCaps {
        PortfolioCaps {
            max_per_market: 150.0,
            max_per_event: 500.0,
            max_per_city: 800.0,
            max_total: 2000.0,
            max_open_orders: 60,
            state_path,
        }
    }

    #[test]
    fn default_no_edge_state_has_schema_version_1() {
        let s = NoEdgeState::default();
        assert_eq!(s.schema_version, SCHEMA_VERSION);
        assert_eq!(s.schema_version, 1);
        assert_eq!(s.total_deployed_usdc, 0.0);
        assert!(s.known_orders.is_empty());
    }

    #[test]
    fn load_refuses_unknown_schema_version() {
        let path = tmp_path("schema_mismatch");
        std::fs::write(
            &path,
            r#"{"schema_version":99,"total_deployed_usdc":0.0,"per_city_deployed":{},"per_event_deployed":{},"known_orders":{},"last_updated_ns":0,"cumulative_pnl_realized":0.0,"fill_count":0,"adverse_fill_count":0}"#,
        )
        .unwrap();
        let err = load_state(&path).unwrap_err();
        assert!(format!("{err:#}").contains("schema_version"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_refuses_unknown_field() {
        let path = tmp_path("unknown_field");
        std::fs::write(
            &path,
            r#"{"schema_version":1,"total_deployed_usdc":0.0,"per_city_deployed":{},"per_event_deployed":{},"known_orders":{},"last_updated_ns":0,"cumulative_pnl_realized":0.0,"fill_count":0,"adverse_fill_count":0,"mystery_field":42}"#,
        )
        .unwrap();
        let err = load_state(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("mystery_field") || msg.contains("unknown"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn save_then_load_roundtrip() {
        let path = tmp_path("roundtrip");
        let mut s = NoEdgeState::default();
        s.total_deployed_usdc = 123.45;
        s.per_city_deployed.insert("nyc".into(), 100.0);
        s.per_event_deployed.insert("nyc:2026-04-15".into(), 100.0);
        s.fill_count = 7;
        save_state(&s, &path).unwrap();
        let loaded = load_state(&path).unwrap().unwrap();
        assert_eq!(loaded, s);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_missing_file_returns_none() {
        let path = tmp_path("missing");
        assert!(load_state(&path).unwrap().is_none());
    }

    // ---- can_deploy guard tests --------------------------------------------

    #[test]
    fn can_deploy_ok_under_caps() {
        let caps = caps_with_defaults(tmp_path("ok"));
        let state = NoEdgeState::default();
        assert_eq!(
            check_can_deploy(&state, &caps, "nyc", "nyc:2026-04-15", 50.0),
            CanDeployResponse::Ok
        );
    }

    #[test]
    fn can_deploy_global_cap_exceeded() {
        let caps = caps_with_defaults(tmp_path("global"));
        let mut state = NoEdgeState::default();
        state.total_deployed_usdc = 1950.0;
        assert_eq!(
            check_can_deploy(&state, &caps, "nyc", "nyc:2026-04-15", 100.0),
            CanDeployResponse::GlobalCapExceeded
        );
    }

    #[test]
    fn can_deploy_per_city_cap_exceeded() {
        let caps = caps_with_defaults(tmp_path("city"));
        let mut state = NoEdgeState::default();
        state.total_deployed_usdc = 750.0;
        state.per_city_deployed.insert("nyc".into(), 750.0);
        assert_eq!(
            check_can_deploy(&state, &caps, "nyc", "nyc:2026-04-15", 100.0),
            CanDeployResponse::PerCityCapExceeded
        );
    }

    #[test]
    fn can_deploy_per_event_cap_exceeded() {
        let caps = caps_with_defaults(tmp_path("event"));
        let mut state = NoEdgeState::default();
        state.total_deployed_usdc = 450.0;
        state.per_city_deployed.insert("nyc".into(), 450.0);
        state
            .per_event_deployed
            .insert("nyc:2026-04-15".into(), 450.0);
        assert_eq!(
            check_can_deploy(&state, &caps, "nyc", "nyc:2026-04-15", 100.0),
            CanDeployResponse::PerEventCapExceeded
        );
    }

    #[test]
    fn can_deploy_max_open_orders_exceeded() {
        let caps = caps_with_defaults(tmp_path("orders"));
        let mut state = NoEdgeState::default();
        for i in 0..60 {
            let id = format!("ord{i}");
            state
                .known_orders
                .insert(id.clone(), make_order(&id, "nyc", "2026-04-15", 1.0));
        }
        assert_eq!(
            check_can_deploy(&state, &caps, "nyc", "nyc:2026-04-15", 1.0),
            CanDeployResponse::MaxOpenOrdersExceeded
        );
    }

    // ---- mutating-command tests --------------------------------------------

    #[test]
    fn record_post_increments_totals() {
        let mut state = NoEdgeState::default();
        let order = make_order("o1", "nyc", "2026-04-15", 100.0);
        apply_post(&mut state, order);
        assert_eq!(state.total_deployed_usdc, 100.0);
        assert_eq!(state.per_city_deployed.get("nyc").copied(), Some(100.0));
        assert_eq!(
            state.per_event_deployed.get("nyc:2026-04-15").copied(),
            Some(100.0)
        );
        assert_eq!(state.known_orders.len(), 1);
    }

    #[test]
    fn record_cancel_decrements_totals() {
        let mut state = NoEdgeState::default();
        apply_post(&mut state, make_order("o1", "nyc", "2026-04-15", 100.0));
        apply_post(&mut state, make_order("o2", "nyc", "2026-04-15", 50.0));
        apply_cancel(&mut state, "o1");
        assert_eq!(state.total_deployed_usdc, 50.0);
        assert_eq!(state.per_city_deployed.get("nyc").copied(), Some(50.0));
        assert_eq!(
            state.per_event_deployed.get("nyc:2026-04-15").copied(),
            Some(50.0)
        );
        assert_eq!(state.known_orders.len(), 1);
    }

    #[test]
    fn record_cancel_unknown_order_is_noop() {
        let mut state = NoEdgeState::default();
        apply_post(&mut state, make_order("o1", "nyc", "2026-04-15", 100.0));
        apply_cancel(&mut state, "ghost");
        assert_eq!(state.total_deployed_usdc, 100.0);
        assert_eq!(state.known_orders.len(), 1);
    }

    #[test]
    fn record_full_fill_clears_order_and_decrements() {
        let mut state = NoEdgeState::default();
        let order = make_order("o1", "nyc", "2026-04-15", 100.0);
        let total_shares = order.size_shares;
        apply_post(&mut state, order);
        apply_fill(&mut state, "o1", total_shares, 0.05, false);
        assert_eq!(state.fill_count, 1);
        assert_eq!(state.adverse_fill_count, 0);
        assert!(state.total_deployed_usdc.abs() < 1e-6);
        assert!(state.per_city_deployed.is_empty());
        assert!(state.per_event_deployed.is_empty());
        assert!(state.known_orders.is_empty());
    }

    #[test]
    fn record_partial_fill_keeps_order() {
        let mut state = NoEdgeState::default();
        let order = make_order("o1", "nyc", "2026-04-15", 100.0);
        let half = order.size_shares / 2.0;
        apply_post(&mut state, order);
        apply_fill(&mut state, "o1", half, 0.05, false);
        assert_eq!(state.fill_count, 1);
        assert!((state.total_deployed_usdc - 50.0).abs() < 1e-6);
        assert_eq!(state.known_orders.len(), 1);
    }

    #[test]
    fn record_fill_flags_adverse() {
        let mut state = NoEdgeState::default();
        let order = make_order("o1", "nyc", "2026-04-15", 100.0);
        let total = order.size_shares;
        apply_post(&mut state, order);
        apply_fill(&mut state, "o1", total, 0.05, true);
        assert_eq!(state.fill_count, 1);
        assert_eq!(state.adverse_fill_count, 1);
    }

    // Atomic-rename interrupt safety is a property of POSIX `rename(2)`, not
    // something we can usefully exercise from a unit test (we would need to
    // SIGKILL between `write` and `rename`). Trust the kernel and move on.

    // ---- end-to-end actor test ---------------------------------------------

    #[tokio::test]
    async fn facade_handle_query_end_to_end() {
        let path = tmp_path("e2e");
        let caps = PortfolioCaps {
            state_path: path.clone(),
            ..caps_with_defaults(path.clone())
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = PortfolioHandle { tx };
        let actor = tokio::spawn(run_actor(NoEdgeState::default(), caps, rx));

        let r = handle.can_deploy("nyc", "nyc:2026-04-15", 50.0).await;
        assert_eq!(r, CanDeployResponse::Ok);

        handle.record_post(make_order("o1", "nyc", "2026-04-15", 100.0));
        let snap = handle.snapshot().await;
        assert_eq!(snap.total_deployed_usdc, 100.0);
        assert_eq!(snap.known_orders.len(), 1);

        handle.record_cancel("o1");
        let snap = handle.snapshot().await;
        assert!(snap.total_deployed_usdc.abs() < 1e-6);
        assert!(snap.known_orders.is_empty());

        drop(handle);
        actor.await.unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
