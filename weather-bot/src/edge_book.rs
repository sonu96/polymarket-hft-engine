//! EdgeBook actor — single-owner task holding `HashMap<TokenId, EdgeEntry>`.
//!
//! Part of the Phase 3 NO-edge farmer pipeline. This is the one place in the
//! system that owns the authoritative per-token fair-price / book / position
//! state. Every mutation arrives on an `mpsc::UnboundedReceiver<EdgeCmd>` and
//! every downstream signal leaves on an `mpsc::UnboundedSender<EdgeSignal>`.
//! The design doc calls this the "coalesce-via-dirty-bit" pattern: a ~10Hz
//! per-token book stream collapses to a 500ms dirty-set drain that emits at
//! most ~2Hz of quoter traffic per token, which then becomes at most ~2Hz of
//! CLOB POSTs regardless of upstream noise.
//!
//! See `docs/PHASE3_NO_EDGE_FARMER.md` §3.3 for the actor model, §3.5 for the
//! forecast σ + METAR nowcast math, and §3.6 for the quoter state machine
//! we're emitting signals for.
//!
//! The actor does **not** call `Executor` — its only output is `EdgeSignal`
//! events handed to the quoter actor (ticket #7). It does emit
//! `SubCmd::Add(token_id_no)` frames upstream to `watchers::clob_book` so the
//! book WS starts streaming for each newly-registered bucket.
//!
//! Wiring happens in ticket #12; the `#![allow(dead_code)]` baseline matches
//! every other wave-2 module until `main.rs` plumbs it in.

#![allow(dead_code)]

use crate::climo::{self, Unit};
use crate::pricer::{self, BucketTail};
use crate::scanner::parse_temperature_slug;
use crate::types::{now_ns, BookUpdate, ForecastTick, NowcastTick, SubCmd, WeatherEvent};
use alloy_primitives::{B256, U256};
use chrono::NaiveDate;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Static context captured once at `RegisterEvent` time for a single NO-side
/// token. Everything here is immutable for the life of the entry.
#[derive(Debug, Clone)]
pub struct BucketContext {
    pub city: &'static str,
    pub icao: &'static str,
    pub unit: Unit,
    pub event_date: NaiveDate,
    pub bucket_lo: Option<f64>,
    pub bucket_hi: Option<f64>,
    pub tail: Option<BucketTail>,
    pub condition_id: B256,
    pub token_id_no: U256,
    pub bucket_label: String,
}

/// Mirror of the quoter's per-token state. EdgeBook does not own this — it
/// tracks it so the dirty-drain can reason about "is a signal even worth
/// emitting right now". The quoter is still the authoritative state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QState {
    Idle,
    Resting,
    Cancelling,
}

/// One row of the EdgeBook map — authoritative per-token state.
#[derive(Debug, Clone)]
pub struct EdgeEntry {
    pub bucket: BucketContext,
    pub fair_p_no: Option<f64>,
    pub fair_p_no_ts_ns: u128,
    pub last_forecast_mu: Option<f64>,
    pub last_forecast_sigma: Option<f64>,
    /// `None` means "no nowcast currently applied" (either never received, or
    /// station was reverted via staleness-kill). `Some(1.0)` is the
    /// explicit-revert sentinel the METAR watcher emits on staleness.
    pub last_nowcast_var_frac: Option<f64>,
    pub last_nowcast_observed_max: Option<f64>,
    pub top_ask: Option<f64>,
    pub top_ask_size: Option<f64>,
    pub asks_ladder: Vec<(f64, f64)>,
    pub bids_ladder: Vec<(f64, f64)>,
    pub my_state: QState,
    pub shares_held: f64,
    pub avg_entry: f64,
    pub dirty: bool,
    pub last_signal_price: Option<f64>,
    pub last_signal_ts_ns: u128,
}

/// Commands consumed by the actor. All mutations to the map arrive through
/// this enum — there is no shared state otherwise.
#[derive(Debug)]
pub enum EdgeCmd {
    RegisterEvent(WeatherEvent),
    ForecastTick(ForecastTick),
    NowcastTick(NowcastTick),
    BookTick(BookUpdate),
    Fill {
        token_id: U256,
        filled_shares: f64,
        filled_price: f64,
    },
    CancelAck {
        token_id: U256,
        order_id: String,
    },
    Query {
        token_id: U256,
        reply: oneshot::Sender<Option<EdgeEntry>>,
    },
}

/// Signal emitted by the 500ms dirty-drain when an entry passes all the quote
/// policy filters. Consumed by the quoter actor (ticket #7).
#[derive(Debug, Clone)]
pub struct EdgeSignal {
    pub token_id: U256,
    pub bucket: BucketContext,
    pub target_ask: f64,
    pub desired_size_shares: f64,
    pub reason: &'static str,
    pub computed_at_ns: u128,
}

pub type EdgeCmdSender = mpsc::UnboundedSender<EdgeCmd>;
pub type EdgeCmdReceiver = mpsc::UnboundedReceiver<EdgeCmd>;
pub type EdgeSignalSender = mpsc::UnboundedSender<EdgeSignal>;
pub type EdgeSignalReceiver = mpsc::UnboundedReceiver<EdgeSignal>;

// ---------------------------------------------------------------------------
// Actor entry point
// ---------------------------------------------------------------------------

/// Run the EdgeBook actor forever (or until `cmd_rx` is closed, which is the
/// shutdown signal).
pub async fn run_edge_book(
    cfg: &crate::config::Config,
    mut cmd_rx: EdgeCmdReceiver,
    sub_cmd_tx: crate::types::SubCmdSender,
    signal_tx: EdgeSignalSender,
) -> anyhow::Result<()> {
    let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    // Skip the initial immediate tick — no state yet.
    tick.tick().await;

    loop {
        tokio::select! {
            biased;
            maybe_cmd = cmd_rx.recv() => match maybe_cmd {
                Some(cmd) => handle_cmd(cmd, &mut state, &sub_cmd_tx),
                None => break,
            },
            _ = tick.tick() => drain_dirty(&mut state, &signal_tx, cfg),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

fn handle_cmd(
    cmd: EdgeCmd,
    state: &mut HashMap<U256, EdgeEntry>,
    sub_cmd_tx: &crate::types::SubCmdSender,
) {
    match cmd {
        EdgeCmd::RegisterEvent(event) => handle_register_event(event, state, sub_cmd_tx),
        EdgeCmd::ForecastTick(tick) => handle_forecast_tick(tick, state),
        EdgeCmd::NowcastTick(tick) => handle_nowcast_tick(tick, state),
        EdgeCmd::BookTick(update) => handle_book_tick(update, state),
        EdgeCmd::Fill {
            token_id,
            filled_shares,
            filled_price,
        } => handle_fill(token_id, filled_shares, filled_price, state),
        EdgeCmd::CancelAck { token_id, .. } => handle_cancel_ack(token_id, state),
        EdgeCmd::Query { token_id, reply } => {
            let _ = reply.send(state.get(&token_id).cloned());
        }
    }
}

fn handle_register_event(
    event: WeatherEvent,
    state: &mut HashMap<U256, EdgeEntry>,
    sub_cmd_tx: &crate::types::SubCmdSender,
) {
    // Parse the event slug for (city, date). Skip the event entirely if the
    // slug doesn't match the weather template — there's nothing to price.
    let Some((city_string, date_string)) = parse_temperature_slug(&event.event_slug) else {
        debug!(
            slug = %event.event_slug,
            "edge_book: ignoring event with non-weather slug"
        );
        return;
    };
    let Some(station) = climo::station_for_city(&city_string) else {
        debug!(city = %city_string, "edge_book: ignoring event for unseeded city");
        return;
    };
    let Ok(event_date) = NaiveDate::parse_from_str(&date_string, "%Y-%m-%d") else {
        warn!(date = %date_string, "edge_book: failed to parse event_date");
        return;
    };

    for bucket in &event.buckets {
        let Some((lo, hi, tail)) = parse_bucket_label(&bucket.bucket_label, station.unit) else {
            debug!(
                label = %bucket.bucket_label,
                "edge_book: failed to parse bucket_label; dropping bucket"
            );
            continue;
        };

        let ctx = BucketContext {
            city: station.city,
            icao: station.icao,
            unit: station.unit,
            event_date,
            bucket_lo: lo,
            bucket_hi: hi,
            tail,
            condition_id: bucket.condition_id,
            token_id_no: bucket.token_id_no,
            bucket_label: bucket.bucket_label.clone(),
        };

        let entry = EdgeEntry {
            bucket: ctx,
            fair_p_no: None,
            fair_p_no_ts_ns: 0,
            last_forecast_mu: None,
            last_forecast_sigma: None,
            last_nowcast_var_frac: None,
            last_nowcast_observed_max: None,
            top_ask: None,
            top_ask_size: None,
            asks_ladder: Vec::new(),
            bids_ladder: Vec::new(),
            my_state: QState::Idle,
            shares_held: 0.0,
            avg_entry: 0.0,
            dirty: false,
            last_signal_price: None,
            last_signal_ts_ns: 0,
        };

        // Overwrite an existing row if we somehow re-register the same token —
        // the on-chain watcher is dedup'd upstream but be defensive.
        state.insert(bucket.token_id_no, entry);

        if let Err(e) = sub_cmd_tx.send(SubCmd::Add(bucket.token_id_no)) {
            warn!(error = %e, "edge_book: SubCmd::Add send failed (book watcher down?)");
        }
    }
}

fn handle_forecast_tick(tick: ForecastTick, state: &mut HashMap<U256, EdgeEntry>) {
    for entry in state.values_mut() {
        if entry.bucket.city != tick.city || entry.bucket.event_date != tick.date {
            continue;
        }
        entry.last_forecast_mu = Some(tick.mu);
        entry.last_forecast_sigma = Some(tick.sigma);
        recompute_fair(entry);
        entry.dirty = true;
    }
}

fn handle_nowcast_tick(tick: NowcastTick, state: &mut HashMap<U256, EdgeEntry>) {
    const STALE: f64 = 1.0 - 1e-9;
    let is_staleness_revert = tick.remaining_var_frac >= STALE;

    for entry in state.values_mut() {
        if entry.bucket.icao != tick.icao {
            continue;
        }
        if is_staleness_revert {
            entry.last_nowcast_var_frac = None;
            entry.last_nowcast_observed_max = None;
        } else {
            entry.last_nowcast_var_frac = Some(tick.remaining_var_frac);
            entry.last_nowcast_observed_max = Some(tick.observed_max);
        }
        recompute_fair(entry);
        entry.dirty = true;
    }
}

fn handle_book_tick(update: BookUpdate, state: &mut HashMap<U256, EdgeEntry>) {
    let Some(entry) = state.get_mut(&update.token_id) else {
        debug!(token = %update.token_id, "edge_book: book tick for unknown token; dropping");
        return;
    };
    entry.asks_ladder = update.asks_ladder;
    entry.bids_ladder = update.bids_ladder;
    entry.top_ask = update.best_ask;
    entry.top_ask_size = entry.asks_ladder.first().map(|(_, sz)| *sz);
    entry.dirty = true;
}

fn handle_fill(
    token_id: U256,
    filled_shares: f64,
    filled_price: f64,
    state: &mut HashMap<U256, EdgeEntry>,
) {
    let Some(entry) = state.get_mut(&token_id) else {
        debug!(token = %token_id, "edge_book: fill for unknown token; dropping");
        return;
    };
    let old_shares = entry.shares_held;
    let new_shares = old_shares + filled_shares;
    if new_shares > 0.0 {
        entry.avg_entry =
            (entry.avg_entry * old_shares + filled_price * filled_shares) / new_shares;
    } else {
        entry.avg_entry = 0.0;
    }
    entry.shares_held = new_shares;
    entry.dirty = true;
}

fn handle_cancel_ack(token_id: U256, state: &mut HashMap<U256, EdgeEntry>) {
    let Some(entry) = state.get_mut(&token_id) else {
        debug!(token = %token_id, "edge_book: cancel_ack for unknown token; dropping");
        return;
    };
    entry.my_state = QState::Idle;
    entry.last_signal_price = None;
    entry.dirty = true;
}

// ---------------------------------------------------------------------------
// Pricing helpers
// ---------------------------------------------------------------------------

/// Recompute `fair_p_no` from `last_forecast_mu/sigma` and the active nowcast
/// (if any). Does nothing when either forecast component is missing.
fn recompute_fair(entry: &mut EdgeEntry) {
    let Some(mu) = entry.last_forecast_mu else {
        return;
    };
    let Some(sigma) = entry.last_forecast_sigma else {
        return;
    };
    if !(sigma > 0.0) {
        return;
    }

    // Shrink σ by √var_frac per §3.5 when a nowcast is active.
    let var_frac = entry.last_nowcast_var_frac.unwrap_or(1.0);
    let sigma_eff = sigma * var_frac.max(0.0).sqrt();
    if !(sigma_eff > 0.0) {
        // Fully collapsed σ → skip; the physical-truncation branch below
        // still handles the edge case where the observed max rules out YES.
        if let Some(p_no) = physical_truncation(entry) {
            entry.fair_p_no = Some(p_no);
            entry.fair_p_no_ts_ns = now_ns();
        }
        return;
    }

    let p_yes = pricer::gaussian_bucket_prob(
        entry.bucket.bucket_lo,
        entry.bucket.bucket_hi,
        entry.bucket.tail,
        mu,
        sigma_eff,
    );
    let mut fair_no = p_yes.map(|p| (1.0 - p).clamp(0.0, 1.0));

    // Physical nowcast truncation overrides the Gaussian: if the observed
    // TMAX-so-far already rules out the entire YES bucket, P(NO) = 1.
    if let Some(p_no) = physical_truncation(entry) {
        fair_no = Some(p_no);
    }

    entry.fair_p_no = fair_no;
    entry.fair_p_no_ts_ns = now_ns();
}

/// If the observed max-so-far rules the bucket completely out (i.e. the
/// bucket's upper edge is strictly below what's already been observed), YES is
/// physically impossible and `P(NO) = 1.0`. Returns `None` when no such
/// override applies — the caller keeps the Gaussian estimate.
fn physical_truncation(entry: &EdgeEntry) -> Option<f64> {
    let observed = entry.last_nowcast_observed_max?;
    // Bucket upper edge (inclusive). For above-tail buckets there is no upper
    // edge — they can never be ruled out by a late-day observation, only
    // confirmed, so we don't touch them here.
    let upper = match entry.bucket.tail {
        Some(BucketTail::Above) => return None,
        Some(BucketTail::Below) => entry.bucket.bucket_lo?,
        None => entry.bucket.bucket_hi?,
    };
    // Integer-degree buckets round half-up, so the true boundary is upper+0.5.
    if observed > upper + 0.5 {
        Some(1.0)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Dirty-bit drain
// ---------------------------------------------------------------------------

fn drain_dirty(
    state: &mut HashMap<U256, EdgeEntry>,
    signal_tx: &EdgeSignalSender,
    cfg: &crate::config::Config,
) {
    let now = now_ns();
    let cooldown_ns = (cfg.no_edge_repost_cooldown_secs as u128) * 1_000_000_000u128;
    let repost_thresh = cfg.no_edge_repost_threshold_cents as f64 / 100.0;
    let min_edge = cfg.no_edge_min_edge_bps as f64 / 10_000.0;
    let max_notional = cfg.no_edge_max_notional_per_market_usdc;

    for (token_id, entry) in state.iter_mut() {
        if !entry.dirty {
            continue;
        }
        entry.dirty = false;

        let Some(fair) = entry.fair_p_no else { continue };
        let Some(top_ask) = entry.top_ask else { continue };

        let target_raw = fair - min_edge;
        // Clamp to the CLOB-valid range. Anything outside (0.001, 0.999) is
        // basically "this bucket is a degenerate certainty" — skip quoting.
        if !(0.001..=0.999).contains(&target_raw) {
            trace!(target = target_raw, "edge_book: target clamped out");
            continue;
        }
        let target_ask = target_raw;

        // No edge over the current best ask — the farmer is a maker-only
        // strategy, we never cross the spread.
        if target_ask >= top_ask {
            continue;
        }

        // Cooldown.
        if entry.last_signal_ts_ns > 0 && now.saturating_sub(entry.last_signal_ts_ns) < cooldown_ns
        {
            continue;
        }

        // Repost-threshold: don't chatter on sub-cent changes.
        if let Some(last) = entry.last_signal_price {
            if (target_ask - last).abs() < repost_thresh {
                continue;
            }
        }

        // Sizing: flat-per-market notional cap, optionally clamped by the
        // competing resting supply stacked at-or-above our target (the
        // farmer is maker-only, so the depth *above* our level is what we'd
        // share the queue with if the market held). If the ladder sum is
        // zero we still honour the notional cap — an empty book is precisely
        // the regime the farmer exists to exploit.
        let cap_shares = if target_ask > 0.0 {
            max_notional / target_ask
        } else {
            0.0
        };
        let ladder_shares_above: f64 = entry
            .asks_ladder
            .iter()
            .filter(|(p, _)| *p >= target_ask)
            .map(|(_, sz)| *sz)
            .sum();
        let desired_size_shares = if ladder_shares_above > 0.0 {
            cap_shares.min(ladder_shares_above)
        } else {
            cap_shares
        };
        if desired_size_shares <= 0.0 {
            continue;
        }

        let reason = if entry.last_signal_price.is_some() {
            "reprice"
        } else {
            "new_opportunity"
        };

        let signal = EdgeSignal {
            token_id: *token_id,
            bucket: entry.bucket.clone(),
            target_ask,
            desired_size_shares,
            reason,
            computed_at_ns: now,
        };
        if let Err(e) = signal_tx.send(signal) {
            warn!(error = %e, "edge_book: signal_tx closed; stopping drain emission");
            return;
        }
        entry.last_signal_price = Some(target_ask);
        entry.last_signal_ts_ns = now;
    }
}

// ---------------------------------------------------------------------------
// Bucket label parser
// ---------------------------------------------------------------------------

/// Parse a Polymarket temperature bucket label into `(lo, hi, tail)`.
///
/// Handles the shapes we actually see in the wild (see design doc §2.1):
///
/// * Fahrenheit range: `"80-81°F"` → `(Some(80), Some(81), None)`
/// * Fahrenheit below: `"≤69°F"` / `"69°F or below"` → `(Some(69), None, Below)`
/// * Fahrenheit above: `"≥90°F"` / `"90°F or higher"` / `"90°F or above"`
///   → `(Some(90), None, Above)`
/// * Celsius single:   `"40°C"` → `(Some(40), Some(40), None)`
/// * Celsius below:    `"≤37°C"` / `"37°C or below"` → `(Some(37), None, Below)`
/// * Celsius above:    `"≥47°C"` / `"47°C or higher"` → `(Some(47), None, Above)`
/// * "40C" / "40c"     bare slug form → `(Some(40), Some(40), None)`
///
/// Returns `None` on shapes we don't recognize — the caller drops the bucket.
pub(crate) fn parse_bucket_label(
    label: &str,
    unit: Unit,
) -> Option<(Option<f64>, Option<f64>, Option<BucketTail>)> {
    let label = label.trim();

    // Determine tail qualifier from either symbolic or word form.
    let lowered = label.to_lowercase();
    let has_below = label.starts_with('≤')
        || lowered.contains("or below")
        || lowered.contains("orbelow")
        || lowered.contains("forbelow");
    let has_above = label.starts_with('≥')
        || lowered.contains("or higher")
        || lowered.contains("orhigher")
        || lowered.contains("or above")
        || lowered.contains("orabove")
        || lowered.contains("forhigher")
        || lowered.contains("forabove");

    // Strip everything that isn't a digit, dot, or dash so we can focus on
    // the numeric payload. `°C`/`°F`/`c`/`f` qualifiers, ≤/≥ prefixes, and
    // "or below"/"or higher" suffixes all wash out here. Unit validation is
    // purely advisory — the caller's `station.unit` is the source of truth.
    let mut cleaned = String::with_capacity(label.len());
    let mut last_was_dash = false;
    for ch in label.chars() {
        let keep = ch.is_ascii_digit() || ch == '.' || ch == '-';
        if keep {
            // Collapse a leading dash (e.g. negatives aren't a thing here but
            // we're defensive) and avoid double-dashes from "69-°F"-type oddities.
            if ch == '-' && last_was_dash {
                continue;
            }
            cleaned.push(ch);
            last_was_dash = ch == '-';
        } else {
            last_was_dash = false;
        }
    }
    // Drop trailing/leading dashes left over from stripping.
    let cleaned = cleaned.trim_matches('-').to_string();
    if cleaned.is_empty() {
        return None;
    }

    // Optional sanity: if the label explicitly labels units and they disagree
    // with the station unit, warn but still accept (caller's unit wins).
    let _ = unit;

    // Range: "80-81"
    if let Some((lo_s, hi_s)) = cleaned.split_once('-') {
        let lo: f64 = lo_s.parse().ok()?;
        let hi: f64 = hi_s.parse().ok()?;
        if has_below || has_above {
            return None; // contradictory: range AND tail qualifier
        }
        if hi < lo {
            return None;
        }
        return Some((Some(lo), Some(hi), None));
    }

    // Single number — either a tail or a single-degree range.
    let n: f64 = cleaned.parse().ok()?;
    if has_below {
        Some((Some(n), None, Some(BucketTail::Below)))
    } else if has_above {
        Some((Some(n), None, Some(BucketTail::Above)))
    } else {
        Some((Some(n), Some(n), None))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{now_ns, BucketInfo, EventKind, ForecastSource, SigmaSource};
    use alloy_primitives::{Address, B256};

    // ---- helpers -----------------------------------------------------------

    fn test_cfg() -> crate::config::Config {
        let mut c = crate::config::Config::default();
        c.no_edge_min_edge_bps = 500;
        c.no_edge_repost_threshold_cents = 2;
        c.no_edge_repost_cooldown_secs = 5;
        c.no_edge_max_notional_per_market_usdc = 150.0;
        c
    }

    fn tok(n: u64) -> U256 {
        U256::from(n)
    }

    fn bucket_info(idx: u32, label: &str, no_token: U256) -> BucketInfo {
        BucketInfo {
            condition_id: B256::repeat_byte(idx as u8),
            question_id: B256::repeat_byte(idx as u8 + 1),
            outcome_index: idx,
            bucket_label: label.to_string(),
            token_id_yes: tok((idx as u64 + 1) * 1_000),
            token_id_no: no_token,
        }
    }

    fn lucknow_event(labels_and_tokens: &[(&str, U256)]) -> WeatherEvent {
        let buckets = labels_and_tokens
            .iter()
            .enumerate()
            .map(|(i, (label, tokn))| bucket_info(i as u32, label, *tokn))
            .collect();
        WeatherEvent {
            event_slug: "highest-temperature-in-lucknow-on-april-15-2026".to_string(),
            city: "lucknow".to_string(),
            resolution_date: "2026-04-15".to_string(),
            kind: EventKind::NegRisk,
            neg_risk_market_id: Some(B256::ZERO),
            oracle: Address::ZERO,
            buckets,
            detected_at_ns: now_ns(),
        }
    }

    fn nyc_event(labels_and_tokens: &[(&str, U256)]) -> WeatherEvent {
        let buckets = labels_and_tokens
            .iter()
            .enumerate()
            .map(|(i, (label, tokn))| bucket_info(i as u32, label, *tokn))
            .collect();
        WeatherEvent {
            event_slug: "highest-temperature-in-nyc-on-april-15-2026".to_string(),
            city: "nyc".to_string(),
            resolution_date: "2026-04-15".to_string(),
            kind: EventKind::NegRisk,
            neg_risk_market_id: Some(B256::ZERO),
            oracle: Address::ZERO,
            buckets,
            detected_at_ns: now_ns(),
        }
    }

    fn apr15() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 4, 15).unwrap()
    }

    fn forecast_for(city: &'static str, icao: &'static str, mu: f64, sigma: f64) -> ForecastTick {
        ForecastTick {
            city,
            icao,
            date: apr15(),
            days_ahead: 0,
            mu,
            sigma,
            source_mu: ForecastSource::GfsHrrr,
            source_sigma: SigmaSource::EnsembleSpread,
            fetched_at_ns: now_ns(),
        }
    }

    fn nowcast_for(
        icao: &'static str,
        observed_max: f64,
        remaining_var_frac: f64,
    ) -> NowcastTick {
        NowcastTick {
            icao,
            date: apr15(),
            observed_max,
            hour_local: 17,
            remaining_var_frac,
            fetched_at_ns: now_ns(),
        }
    }

    fn book_update(token_id: U256, asks: Vec<(f64, f64)>) -> BookUpdate {
        let best_ask = asks.first().map(|(p, _)| *p);
        BookUpdate {
            token_id,
            best_bid: None,
            best_ask,
            asks_ladder: asks,
            bids_ladder: vec![],
            fetched_at_ns: now_ns(),
        }
    }

    // ---- label parser sanity ----------------------------------------------

    #[test]
    fn parse_fahrenheit_range_label() {
        let (lo, hi, tail) = parse_bucket_label("80-81°F", Unit::Fahrenheit).unwrap();
        assert_eq!(lo, Some(80.0));
        assert_eq!(hi, Some(81.0));
        assert!(tail.is_none());
    }

    #[test]
    fn parse_fahrenheit_below_label() {
        let (lo, hi, tail) = parse_bucket_label("≤69°F", Unit::Fahrenheit).unwrap();
        assert_eq!(lo, Some(69.0));
        assert!(hi.is_none());
        assert_eq!(tail, Some(BucketTail::Below));
    }

    #[test]
    fn parse_fahrenheit_above_label_with_word_form() {
        let (lo, hi, tail) = parse_bucket_label("90°F or higher", Unit::Fahrenheit).unwrap();
        assert_eq!(lo, Some(90.0));
        assert!(hi.is_none());
        assert_eq!(tail, Some(BucketTail::Above));
    }

    #[test]
    fn parse_celsius_single_label() {
        let (lo, hi, tail) = parse_bucket_label("40°C", Unit::Celsius).unwrap();
        assert_eq!(lo, Some(40.0));
        assert_eq!(hi, Some(40.0));
        assert!(tail.is_none());
    }

    #[test]
    fn parse_celsius_below_label() {
        let (lo, hi, tail) = parse_bucket_label("≤37°C", Unit::Celsius).unwrap();
        assert_eq!(lo, Some(37.0));
        assert!(hi.is_none());
        assert_eq!(tail, Some(BucketTail::Below));
    }

    #[test]
    fn parse_bare_slug_form_40c() {
        let (lo, hi, tail) = parse_bucket_label("40c", Unit::Celsius).unwrap();
        assert_eq!(lo, Some(40.0));
        assert_eq!(hi, Some(40.0));
        assert!(tail.is_none());
    }

    // ---- RegisterEvent -----------------------------------------------------

    #[test]
    fn register_event_creates_entries_per_bucket() {
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        let event = lucknow_event(&[
            ("≤37°C", tok(1)),
            ("40°C", tok(2)),
            ("≥47°C", tok(3)),
        ]);
        handle_register_event(event, &mut state, &sub_tx);

        assert_eq!(state.len(), 3);
        assert!(state.contains_key(&tok(1)));
        assert!(state.contains_key(&tok(2)));
        assert!(state.contains_key(&tok(3)));

        // Bucket shapes landed correctly.
        let below = &state[&tok(1)];
        assert_eq!(below.bucket.tail, Some(BucketTail::Below));
        assert_eq!(below.bucket.bucket_lo, Some(37.0));
        assert_eq!(below.bucket.city, "lucknow");
        assert_eq!(below.bucket.icao, "VILK");
        assert_eq!(below.bucket.unit, Unit::Celsius);
        assert_eq!(below.bucket.event_date, apr15());

        let mid = &state[&tok(2)];
        assert_eq!(mid.bucket.bucket_lo, Some(40.0));
        assert_eq!(mid.bucket.bucket_hi, Some(40.0));

        let above = &state[&tok(3)];
        assert_eq!(above.bucket.tail, Some(BucketTail::Above));
        assert_eq!(above.bucket.bucket_lo, Some(47.0));

        // sub_tx should have received one Add per bucket.
        let mut adds = 0;
        while let Ok(cmd) = sub_rx.try_recv() {
            match cmd {
                SubCmd::Add(_) => adds += 1,
                SubCmd::Remove(_) => panic!("unexpected Remove"),
            }
        }
        assert_eq!(adds, 3);
    }

    #[test]
    fn register_event_emits_sub_cmd_add_per_bucket() {
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        let event = nyc_event(&[("80-81°F", tok(10)), ("≥90°F", tok(11))]);
        handle_register_event(event, &mut state, &sub_tx);

        let first = sub_rx.try_recv().unwrap();
        let second = sub_rx.try_recv().unwrap();
        let third = sub_rx.try_recv();
        match first {
            SubCmd::Add(t) => assert_eq!(t, tok(10)),
            _ => panic!("expected Add(10)"),
        }
        match second {
            SubCmd::Add(t) => assert_eq!(t, tok(11)),
            _ => panic!("expected Add(11)"),
        }
        assert!(third.is_err(), "no more subs expected");
    }

    // ---- ForecastTick ------------------------------------------------------

    #[test]
    fn forecast_tick_updates_fair_p_no_for_matching_city_and_date() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(100))]),
            &mut state,
            &sub_tx,
        );

        handle_forecast_tick(forecast_for("nyc", "KLGA", 80.5, 2.0), &mut state);
        let entry = &state[&tok(100)];
        assert_eq!(entry.last_forecast_mu, Some(80.5));
        assert_eq!(entry.last_forecast_sigma, Some(2.0));
        let fair = entry.fair_p_no.expect("fair_p_no computed");
        // 80-81 inflated to 79.5-81.5 under N(80.5, 2): P(YES) ≈ 0.383,
        // P(NO) ≈ 0.617.
        assert!((0.55..0.68).contains(&fair), "got {fair}");
        assert!(entry.dirty);
    }

    #[test]
    fn forecast_tick_ignores_mismatched_city() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(200))]),
            &mut state,
            &sub_tx,
        );

        // Atlanta tick — wrong city, should not update NYC entry.
        handle_forecast_tick(forecast_for("atlanta", "KATL", 75.0, 2.0), &mut state);
        let entry = &state[&tok(200)];
        assert!(entry.last_forecast_mu.is_none());
        assert!(entry.fair_p_no.is_none());
        assert!(!entry.dirty);
    }

    // ---- NowcastTick -------------------------------------------------------

    #[test]
    fn nowcast_tick_shrinks_sigma_via_sqrt_var_frac() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(300))]),
            &mut state,
            &sub_tx,
        );

        // Seed forecast: μ=78, σ=2. Then apply nowcast shrinking to 25%
        // remaining variance → σ_effective = 2 * sqrt(0.25) = 1.0.
        handle_forecast_tick(forecast_for("nyc", "KLGA", 78.0, 2.0), &mut state);
        let fair_before = state[&tok(300)].fair_p_no.unwrap();

        handle_nowcast_tick(nowcast_for("KLGA", 77.0, 0.25), &mut state);
        let entry = &state[&tok(300)];
        assert_eq!(entry.last_nowcast_var_frac, Some(0.25));
        assert_eq!(entry.last_nowcast_observed_max, Some(77.0));
        let fair_after = entry.fair_p_no.unwrap();
        // Shrinking σ pushes the 80-81 bucket further into the tail →
        // P(YES) drops → P(NO) rises.
        assert!(
            fair_after > fair_before,
            "expected fair_no to rise with tighter σ: before={fair_before} after={fair_after}"
        );
    }

    #[test]
    fn nowcast_tick_truncates_below_observed_max() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        // 70-71 bucket: an observed 85°F rules it out physically.
        handle_register_event(
            nyc_event(&[("70-71°F", tok(400))]),
            &mut state,
            &sub_tx,
        );
        handle_forecast_tick(forecast_for("nyc", "KLGA", 75.0, 5.0), &mut state);
        handle_nowcast_tick(nowcast_for("KLGA", 85.0, 0.1), &mut state);
        let entry = &state[&tok(400)];
        assert_eq!(
            entry.fair_p_no,
            Some(1.0),
            "bucket entirely below observed max → P(NO)=1"
        );
    }

    #[test]
    fn nowcast_staleness_revert_clears_shrink() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(500))]),
            &mut state,
            &sub_tx,
        );
        handle_forecast_tick(forecast_for("nyc", "KLGA", 80.0, 2.0), &mut state);
        handle_nowcast_tick(nowcast_for("KLGA", 77.0, 0.2), &mut state);
        assert!(state[&tok(500)].last_nowcast_var_frac.is_some());

        // Staleness revert: var_frac = 1.0 → clear nowcast fields.
        handle_nowcast_tick(nowcast_for("KLGA", 0.0, 1.0), &mut state);
        let entry = &state[&tok(500)];
        assert!(entry.last_nowcast_var_frac.is_none());
        assert!(entry.last_nowcast_observed_max.is_none());
    }

    // ---- BookTick ----------------------------------------------------------

    #[test]
    fn book_tick_updates_ladder_sets_dirty() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(600))]),
            &mut state,
            &sub_tx,
        );

        let asks = vec![(0.80, 50.0), (0.82, 100.0)];
        handle_book_tick(book_update(tok(600), asks), &mut state);
        let entry = &state[&tok(600)];
        assert_eq!(entry.top_ask, Some(0.80));
        assert_eq!(entry.top_ask_size, Some(50.0));
        assert_eq!(entry.asks_ladder.len(), 2);
        assert!(entry.dirty);
    }

    #[test]
    fn book_tick_for_unknown_token_is_ignored() {
        let (_sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        // No register_event, so tok(999) is unknown.
        handle_book_tick(book_update(tok(999), vec![(0.5, 10.0)]), &mut state);
        assert!(state.is_empty());
    }

    // ---- Fill / Cancel -----------------------------------------------------

    #[test]
    fn fill_updates_shares_held_and_avg_entry() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(700))]),
            &mut state,
            &sub_tx,
        );

        handle_fill(tok(700), 100.0, 0.60, &mut state);
        let entry = &state[&tok(700)];
        assert_eq!(entry.shares_held, 100.0);
        assert!((entry.avg_entry - 0.60).abs() < 1e-9);
        assert!(entry.dirty);

        handle_fill(tok(700), 100.0, 0.70, &mut state);
        let entry = &state[&tok(700)];
        assert_eq!(entry.shares_held, 200.0);
        assert!((entry.avg_entry - 0.65).abs() < 1e-9);
    }

    #[test]
    fn cancel_ack_returns_state_to_idle() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(800))]),
            &mut state,
            &sub_tx,
        );
        {
            let entry = state.get_mut(&tok(800)).unwrap();
            entry.my_state = QState::Cancelling;
            entry.last_signal_price = Some(0.50);
        }

        handle_cancel_ack(tok(800), &mut state);
        let entry = &state[&tok(800)];
        assert_eq!(entry.my_state, QState::Idle);
        assert!(entry.last_signal_price.is_none());
        assert!(entry.dirty);
    }

    // ---- Dirty drain -------------------------------------------------------

    /// Seed an entry with a healthy fair/top-ask combo so drain_dirty emits.
    ///
    /// Setup: forecast centers near the bucket so fair_no ≈ 0.80, but the
    /// book has a fat ask at 0.95 — plenty of room for a target at
    /// `fair − 0.05 ≈ 0.75` to undercut the top ask and clear the edge gate.
    fn seed_edge_ready(state: &mut HashMap<U256, EdgeEntry>, sub_tx: &crate::types::SubCmdSender) {
        handle_register_event(
            nyc_event(&[("80-81°F", tok(900))]),
            state,
            sub_tx,
        );
        handle_forecast_tick(forecast_for("nyc", "KLGA", 82.0, 3.0), state);
        handle_book_tick(
            book_update(tok(900), vec![(0.95, 200.0), (0.97, 300.0)]),
            state,
        );
    }

    #[test]
    fn dirty_drain_emits_signal_when_edge_meets_threshold() {
        let cfg = test_cfg();
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<EdgeSignal>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        seed_edge_ready(&mut state, &sub_tx);
        assert!(state[&tok(900)].dirty);

        drain_dirty(&mut state, &sig_tx, &cfg);
        let signal = sig_rx.try_recv().expect("expected signal");
        assert_eq!(signal.token_id, tok(900));
        // fair_no ≈ 0.77 (80-81 bucket under N(82,3)), min_edge 0.05 →
        // target_ask ≈ 0.72; undercuts the 0.95 top ask by a fat margin.
        assert!(
            (0.65..=0.80).contains(&signal.target_ask),
            "got {}",
            signal.target_ask
        );
        assert!(signal.desired_size_shares > 0.0);
        // Entry bookkeeping.
        let entry = &state[&tok(900)];
        assert_eq!(entry.last_signal_price, Some(signal.target_ask));
        assert!(!entry.dirty);
    }

    #[test]
    fn dirty_drain_skips_when_fair_below_ask() {
        let cfg = test_cfg();
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<EdgeSignal>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(901))]),
            &mut state,
            &sub_tx,
        );
        // Forecast says the bucket is ~the mode: P(NO) ≈ 0.6. Book is tight
        // at 0.80 → target_ask ≈ 0.55 < top_ask 0.80 (still edge) — flip it:
        // set the book cheap at 0.30, so target 0.55 >= 0.30 (no edge).
        handle_forecast_tick(forecast_for("nyc", "KLGA", 80.5, 2.0), &mut state);
        handle_book_tick(book_update(tok(901), vec![(0.30, 100.0)]), &mut state);

        drain_dirty(&mut state, &sig_tx, &cfg);
        assert!(sig_rx.try_recv().is_err(), "no edge → no signal");
    }

    #[test]
    fn dirty_drain_respects_cooldown() {
        let cfg = test_cfg();
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<EdgeSignal>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        seed_edge_ready(&mut state, &sub_tx);

        // First drain emits and stamps last_signal_ts_ns = now.
        drain_dirty(&mut state, &sig_tx, &cfg);
        let _ = sig_rx.try_recv().expect("first emit");

        // Force dirty again with a fresh book tick; immediately re-drain. The
        // cooldown (5s) should suppress the second emission (top ask still
        // >> target so the edge gate itself is still happy).
        handle_book_tick(
            book_update(tok(900), vec![(0.93, 200.0)]),
            &mut state,
        );
        drain_dirty(&mut state, &sig_tx, &cfg);
        assert!(sig_rx.try_recv().is_err(), "cooldown should suppress emit");

        // Rewind the stamp to simulate cooldown elapsed AND shift the
        // forecast enough for fair (and thus target) to move past the 2¢
        // repost gate, then drain again.
        handle_forecast_tick(forecast_for("nyc", "KLGA", 85.0, 3.0), &mut state);
        {
            let entry = state.get_mut(&tok(900)).unwrap();
            entry.last_signal_ts_ns = 1; // ancient
            entry.dirty = true;
        }
        drain_dirty(&mut state, &sig_tx, &cfg);
        let _ = sig_rx
            .try_recv()
            .expect("after cooldown expiry, emit resumes");
    }

    #[test]
    fn dirty_drain_respects_repost_threshold_cents() {
        let mut cfg = test_cfg();
        cfg.no_edge_repost_cooldown_secs = 0; // disable cooldown gate
        cfg.no_edge_repost_threshold_cents = 5; // 5¢
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<EdgeSignal>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        seed_edge_ready(&mut state, &sub_tx);

        drain_dirty(&mut state, &sig_tx, &cfg);
        let first = sig_rx.try_recv().expect("first emit");

        // Tiny book move — same top ask, different ladder size. fair_p_no
        // unchanged → target_ask unchanged → |Δ| = 0 < 5¢ → suppressed.
        handle_book_tick(
            book_update(tok(900), vec![(0.95, 999.0)]),
            &mut state,
        );
        drain_dirty(&mut state, &sig_tx, &cfg);
        assert!(
            sig_rx.try_recv().is_err(),
            "|Δprice|=0 < 5¢ → repost suppressed"
        );
        // Sanity: last_signal_price still the first target.
        assert_eq!(state[&tok(900)].last_signal_price, Some(first.target_ask));
    }

    #[test]
    fn dirty_drain_clamps_target_price() {
        let mut cfg = test_cfg();
        cfg.no_edge_min_edge_bps = 10_000; // 100% — forces target < 0
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<EdgeSignal>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        seed_edge_ready(&mut state, &sub_tx);

        drain_dirty(&mut state, &sig_tx, &cfg);
        assert!(
            sig_rx.try_recv().is_err(),
            "target outside (0.001, 0.999) should be clamped out"
        );
    }

    // ---- Query -------------------------------------------------------------

    #[tokio::test]
    async fn query_returns_cloned_entry() {
        let (sub_tx, _sub_rx) = mpsc::unbounded_channel::<SubCmd>();
        let mut state: HashMap<U256, EdgeEntry> = HashMap::new();
        handle_register_event(
            nyc_event(&[("80-81°F", tok(1000))]),
            &mut state,
            &sub_tx,
        );

        let (reply_tx, reply_rx) = oneshot::channel::<Option<EdgeEntry>>();
        handle_cmd(
            EdgeCmd::Query {
                token_id: tok(1000),
                reply: reply_tx,
            },
            &mut state,
            &sub_tx,
        );
        let got = reply_rx.await.unwrap();
        let got = got.expect("entry present");
        assert_eq!(got.bucket.token_id_no, tok(1000));
    }
}
