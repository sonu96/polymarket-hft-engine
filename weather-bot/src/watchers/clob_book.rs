//! Polymarket CLOB **book** WebSocket watcher.
//!
//! Owns a single long-lived connection to `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//! and is the single authoritative owner of the set of currently-subscribed
//! NO-token IDs. Two channels cross the boundary:
//!
//! * `SubCmdReceiver` — the only way for upstream tasks (EdgeBook) to ask for
//!   a token to start or stop being tracked. There is no shared `Arc<HashSet>`
//!   anywhere; the channel is the entire interface.
//! * `BookUpdateSender` — every parsed `book` snapshot or `price_change` delta
//!   is rebuilt into a full `BookUpdate` (sorted, top-of-book extracted) and
//!   shipped downstream.
//!
//! ## Reconnect replay
//!
//! Polymarket's CLOB WS does **not** persist subscriptions across disconnects
//! and does not support unsubscribe frames either. On any disconnect the task
//! must re-send a single subscribe frame containing the entire local
//! `HashSet<U256>` before any other input is processed, otherwise events for
//! pre-existing tokens are silently dropped. This file is the only place that
//! knows the full subscribed set, and the upstream discovery stream
//! (`watchers::onchain`) does not retransmit historical events — so getting
//! reconnect replay right is load-bearing.
//!
//! NB: this module is intentionally separate from `clob_ws.rs`, which
//! subscribes to a single anchor token to pick up the global `new_market`
//! broadcast. Different concern, different lifetime, different wire frames.

// Wired in by ticket #12 (main.rs fan-out); silence dead_code until then so
// the build stays warning-clean for unrelated reviewers.
#![allow(dead_code)]

use crate::config::Config;
use crate::types::{
    now_ns, BookUpdate, BookUpdateSender, SubCmd, SubCmdReceiver,
};
use alloy_primitives::U256;
use anyhow::{anyhow, Context};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the CLOB book watcher forever. Owns the subscribed-token set, owns the
/// WS connection, owns the reconnect loop.
pub async fn run_clob_book_watcher(
    cfg: &Config,
    sub_cmd_rx: SubCmdReceiver,
    book_tx: BookUpdateSender,
) -> anyhow::Result<()> {
    let url = cfg.clob_ws_market_url.clone();
    let mut state = WatcherState::new();
    let mut backoff = ExpoBackoff::new();
    let mut sub_cmd_rx = sub_cmd_rx;

    let mut reconnect_count: u64 = 0;
    loop {
        tracing::info!(
            url = %url,
            tokens = state.subscribed.len(),
            reconnect = reconnect_count,
            "[clob_book] connecting"
        );

        let conn_res = connect_async(&url).await;
        let conn = match conn_res {
            Ok((ws, _)) => ws,
            Err(e) => {
                let wait = backoff.next();
                tracing::warn!(
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    reconnect = reconnect_count,
                    "[clob_book] connect failed"
                );
                tokio::time::sleep(wait).await;
                reconnect_count += 1;
                continue;
            }
        };
        backoff.reset();

        let mut io = TungsteniteConn { ws: conn };
        match run_loop(&mut io, &mut state, &mut sub_cmd_rx, &book_tx).await {
            Ok(LoopExit::DownstreamDropped) => {
                return Err(anyhow!(
                    "[clob_book] BookUpdate receiver dropped — downstream gone, fatal"
                ));
            }
            Ok(LoopExit::SubCmdClosed) => {
                tracing::warn!("[clob_book] sub-cmd channel closed, exiting");
                return Ok(());
            }
            Err(e) => {
                let wait = backoff.next();
                reconnect_count += 1;
                tracing::warn!(
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    reconnect = reconnect_count,
                    tokens = state.subscribed.len(),
                    "[clob_book] WS error, will reconnect after backoff"
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IO abstraction (so the loop is testable without a real socket)
// ---------------------------------------------------------------------------

trait BookWsConn {
    fn send_text(
        &mut self,
        text: String,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    fn recv_text(
        &mut self,
    ) -> impl std::future::Future<Output = anyhow::Result<Option<String>>> + Send;
}

struct TungsteniteConn {
    ws: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl BookWsConn for TungsteniteConn {
    async fn send_text(&mut self, text: String) -> anyhow::Result<()> {
        self.ws
            .send(Message::Text(text.into()))
            .await
            .context("ws send")
    }

    async fn recv_text(&mut self) -> anyhow::Result<Option<String>> {
        loop {
            match self.ws.next().await {
                None => return Ok(None),
                Some(Err(e)) => return Err(anyhow!("ws recv: {e}")),
                Some(Ok(Message::Text(t))) => return Ok(Some(t.to_string())),
                Some(Ok(Message::Binary(_))) => continue,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Frame(_))) => continue,
                Some(Ok(Message::Close(_))) => return Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct WatcherState {
    /// Authoritative subscribed-token set. Owned solely by this task.
    subscribed: HashSet<U256>,
    /// Last full book per token, keyed by token id. Used to apply
    /// `price_change` deltas in place.
    books: HashMap<U256, FullBook>,
}

impl WatcherState {
    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Default)]
struct FullBook {
    /// price -> size, asks
    asks: BTreeMap<PriceKey, f64>,
    /// price -> size, bids
    bids: BTreeMap<PriceKey, f64>,
}

/// f64 prices need a deterministic total order to live in a BTreeMap. Bucket
/// to integer ticks (1e-6) so we never collide on FP equality. Polymarket's
/// minimum tick is 0.01; six decimals is conservative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PriceKey(i64);

impl PriceKey {
    fn from_f64(p: f64) -> Self {
        Self((p * 1_000_000.0).round() as i64)
    }
    fn to_f64(self) -> f64 {
        (self.0 as f64) / 1_000_000.0
    }
}

enum LoopExit {
    DownstreamDropped,
    SubCmdClosed,
}

// ---------------------------------------------------------------------------
// Main loop (one WS lifetime). On any IO error returns Err and the outer
// `run_clob_book_watcher` reconnects.
// ---------------------------------------------------------------------------

async fn run_loop<C: BookWsConn>(
    conn: &mut C,
    state: &mut WatcherState,
    sub_cmd_rx: &mut SubCmdReceiver,
    book_tx: &BookUpdateSender,
) -> anyhow::Result<LoopExit> {
    // Step 1: replay the entire authoritative set in a single subscribe frame
    // BEFORE accepting any other input. This is the reconnect-replay invariant.
    let frame = build_subscribe_frame(state.subscribed.iter().copied());
    conn.send_text(frame).await.context("initial subscribe")?;
    tracing::info!(
        tokens = state.subscribed.len(),
        "[clob_book] subscribe frame sent"
    );

    loop {
        tokio::select! {
            biased;

            cmd = sub_cmd_rx.recv() => {
                let cmd = match cmd {
                    Some(c) => c,
                    None => return Ok(LoopExit::SubCmdClosed),
                };
                if let Err(e) = handle_sub_cmd(cmd, state, conn).await {
                    return Err(e);
                }
                // Drain any other commands that arrived in the same tick so
                // we send one frame per add rather than batching — keeps the
                // implementation simple and matches the spec's "Add inserts +
                // sends a subscribe frame for that one token".
                loop {
                    match sub_cmd_rx.try_recv() {
                        Ok(c) => {
                            if let Err(e) = handle_sub_cmd(c, state, conn).await {
                                return Err(e);
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return Ok(LoopExit::SubCmdClosed),
                    }
                }
            }

            recv = conn.recv_text() => {
                let txt = match recv? {
                    Some(t) => t,
                    None => return Err(anyhow!("ws stream closed")),
                };
                match handle_incoming_text(&txt, state) {
                    Ok(updates) => {
                        for upd in updates {
                            if book_tx.send(upd).is_err() {
                                return Ok(LoopExit::DownstreamDropped);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "[clob_book] ignored unparsable frame");
                    }
                }
            }
        }
    }
}

async fn handle_sub_cmd<C: BookWsConn>(
    cmd: SubCmd,
    state: &mut WatcherState,
    conn: &mut C,
) -> anyhow::Result<()> {
    match cmd {
        SubCmd::Add(t) => {
            if state.subscribed.insert(t) {
                let frame = build_subscribe_frame(std::iter::once(t));
                conn.send_text(frame).await.context("add subscribe")?;
                tracing::info!(token = %t, "[clob_book] subscribed");
            }
        }
        SubCmd::Remove(t) => {
            // Polymarket has no unsubscribe — we just drop it locally and
            // the recv-side filter ignores any further frames for it.
            if state.subscribed.remove(&t) {
                state.books.remove(&t);
                tracing::info!(token = %t, "[clob_book] unsubscribed (local-only)");
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire format helpers
// ---------------------------------------------------------------------------

/// Build the Polymarket Market subscribe frame for the given token set. A
/// single frame can carry the whole set; reconnect replay relies on that.
fn build_subscribe_frame<I: IntoIterator<Item = U256>>(tokens: I) -> String {
    let assets_ids: Vec<String> = tokens.into_iter().map(|t| t.to_string()).collect();
    json!({
        "type": "Market",
        "assets_ids": assets_ids,
    })
    .to_string()
}

/// Parse one inbound frame into zero-or-more `BookUpdate`s. The raw frame may
/// be a single object or an array of events (Polymarket batches deltas).
fn handle_incoming_text(
    raw: &str,
    state: &mut WatcherState,
) -> anyhow::Result<Vec<BookUpdate>> {
    let v: serde_json::Value =
        serde_json::from_str(raw).context("frame is not valid JSON")?;
    let items: Vec<serde_json::Value> = match v {
        serde_json::Value::Array(a) => a,
        other => vec![other],
    };

    let mut out = Vec::new();
    for item in items {
        if let Some(upd) = handle_event(item, state)? {
            out.push(upd);
        }
    }
    Ok(out)
}

fn handle_event(
    v: serde_json::Value,
    state: &mut WatcherState,
) -> anyhow::Result<Option<BookUpdate>> {
    // Pull out asset_id + event_type early so we can filter dropped tokens
    // before doing any further parsing work.
    let asset_id_str = v
        .get("asset_id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("missing asset_id"))?;
    let token_id = U256::from_str_radix(asset_id_str, 10)
        .map_err(|e| anyhow!("asset_id not a decimal U256: {e}"))?;

    if !state.subscribed.contains(&token_id) {
        // Local filter: drop frames for tokens that were removed via
        // SubCmd::Remove but Polymarket hasn't flushed yet.
        return Ok(None);
    }

    let event_type = v
        .get("event_type")
        .and_then(|x| x.as_str())
        .unwrap_or("");

    match event_type {
        "book" => {
            let snap: BookEvent = serde_json::from_value(v).context("parse book")?;
            let book = state.books.entry(token_id).or_default();
            *book = FullBook::default();
            for lvl in &snap.bids {
                let (p, s) = parse_level(lvl)?;
                if s > 0.0 {
                    book.bids.insert(PriceKey::from_f64(p), s);
                }
            }
            for lvl in &snap.asks {
                let (p, s) = parse_level(lvl)?;
                if s > 0.0 {
                    book.asks.insert(PriceKey::from_f64(p), s);
                }
            }
            Ok(Some(emit(token_id, book)))
        }
        "price_change" => {
            let delta: PriceChangeEvent =
                serde_json::from_value(v).context("parse price_change")?;
            let book = state.books.entry(token_id).or_default();
            for ch in delta.changes {
                let (p, s) = parse_level_change(&ch)?;
                let key = PriceKey::from_f64(p);
                let side = ch.side.as_str();
                let map = match side {
                    "BUY" | "buy" | "bid" => &mut book.bids,
                    "SELL" | "sell" | "ask" => &mut book.asks,
                    other => return Err(anyhow!("unknown side {other}")),
                };
                if s > 0.0 {
                    map.insert(key, s);
                } else {
                    map.remove(&key);
                }
            }
            Ok(Some(emit(token_id, book)))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
struct BookEvent {
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    #[serde(default)]
    changes: Vec<RawChange>,
}

#[derive(Debug, Deserialize)]
struct RawChange {
    price: String,
    size: String,
    side: String,
}

fn parse_level(l: &RawLevel) -> anyhow::Result<(f64, f64)> {
    let p: f64 = l.price.parse().context("price")?;
    let s: f64 = l.size.parse().context("size")?;
    Ok((p, s))
}

fn parse_level_change(c: &RawChange) -> anyhow::Result<(f64, f64)> {
    let p: f64 = c.price.parse().context("price")?;
    let s: f64 = c.size.parse().context("size")?;
    Ok((p, s))
}

/// Snapshot the in-memory FullBook for this token into a sorted BookUpdate.
fn emit(token_id: U256, book: &FullBook) -> BookUpdate {
    // asks: ascending by price (BTreeMap is already ascending)
    let asks_ladder: Vec<(f64, f64)> = book
        .asks
        .iter()
        .map(|(k, v)| (k.to_f64(), *v))
        .collect();
    // bids: descending by price — reverse the BTreeMap iter
    let bids_ladder: Vec<(f64, f64)> = book
        .bids
        .iter()
        .rev()
        .map(|(k, v)| (k.to_f64(), *v))
        .collect();
    let best_ask = asks_ladder.first().map(|(p, _)| *p);
    let best_bid = bids_ladder.first().map(|(p, _)| *p);
    BookUpdate {
        token_id,
        best_bid,
        best_ask,
        asks_ladder,
        bids_ladder,
        fetched_at_ns: now_ns(),
    }
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

struct ExpoBackoff {
    cur_ms: u64,
}

impl ExpoBackoff {
    fn new() -> Self {
        Self { cur_ms: 0 }
    }
    fn reset(&mut self) {
        self.cur_ms = 0;
    }
    fn next(&mut self) -> Duration {
        // Start at 1s, double each call, cap at 30s.
        self.cur_ms = if self.cur_ms == 0 {
            1_000
        } else {
            (self.cur_ms.saturating_mul(2)).min(30_000)
        };
        Duration::from_millis(self.cur_ms)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;
    use std::sync::Arc;

    fn tok(s: &str) -> U256 {
        U256::from_str_radix(s, 10).unwrap()
    }

    // ---------- pure parsing / emit tests ----------

    #[test]
    fn book_update_from_event_type_book_full_snapshot() {
        let token = tok("12345");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);

        let raw = serde_json::json!({
            "event_type": "book",
            "asset_id": "12345",
            "bids": [
                {"price": "0.40", "size": "100"},
                {"price": "0.42", "size": "50"}
            ],
            "asks": [
                {"price": "0.45", "size": "75"},
                {"price": "0.43", "size": "200"}
            ],
        })
        .to_string();

        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        assert_eq!(updates.len(), 1);
        let u = &updates[0];
        assert_eq!(u.token_id, token);
        // asks ascending
        assert_eq!(u.asks_ladder, vec![(0.43, 200.0), (0.45, 75.0)]);
        // bids descending
        assert_eq!(u.bids_ladder, vec![(0.42, 50.0), (0.40, 100.0)]);
        assert_eq!(u.best_bid, Some(0.42));
        assert_eq!(u.best_ask, Some(0.43));
    }

    #[test]
    fn book_update_sorts_asks_ascending_bids_descending() {
        let token = tok("99");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);
        let raw = serde_json::json!({
            "event_type": "book",
            "asset_id": "99",
            "bids": [
                {"price": "0.10", "size": "1"},
                {"price": "0.30", "size": "3"},
                {"price": "0.20", "size": "2"}
            ],
            "asks": [
                {"price": "0.70", "size": "7"},
                {"price": "0.50", "size": "5"},
                {"price": "0.60", "size": "6"}
            ]
        })
        .to_string();
        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        let u = &updates[0];
        let ask_prices: Vec<f64> = u.asks_ladder.iter().map(|(p, _)| *p).collect();
        let bid_prices: Vec<f64> = u.bids_ladder.iter().map(|(p, _)| *p).collect();
        assert_eq!(ask_prices, vec![0.50, 0.60, 0.70]);
        assert_eq!(bid_prices, vec![0.30, 0.20, 0.10]);
    }

    #[test]
    fn best_bid_ask_from_ladder() {
        let token = tok("7");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);
        let raw = serde_json::json!({
            "event_type": "book",
            "asset_id": "7",
            "bids": [{"price":"0.33","size":"10"}, {"price":"0.31","size":"5"}],
            "asks": [{"price":"0.40","size":"8"}, {"price":"0.41","size":"9"}],
        })
        .to_string();
        let u = &handle_incoming_text(&raw, &mut state).unwrap()[0];
        assert_eq!(u.best_bid, Some(u.bids_ladder[0].0));
        assert_eq!(u.best_ask, Some(u.asks_ladder[0].0));
        assert_eq!(u.best_bid, Some(0.33));
        assert_eq!(u.best_ask, Some(0.40));
    }

    #[test]
    fn remove_filter_drops_updates_for_unsubscribed() {
        let token = tok("555");
        let mut state = WatcherState::new();
        // Token never subscribed → frame must produce zero updates.
        let raw = serde_json::json!({
            "event_type": "book",
            "asset_id": "555",
            "bids": [{"price":"0.10","size":"1"}],
            "asks": [{"price":"0.20","size":"1"}],
        })
        .to_string();
        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        assert!(updates.is_empty());

        // Now add then remove, simulating SubCmd::Remove flow.
        state.subscribed.insert(token);
        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        assert_eq!(updates.len(), 1);

        state.subscribed.remove(&token);
        state.books.remove(&token);
        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        assert!(updates.is_empty(), "post-remove frames must be filtered");
    }

    #[test]
    fn subscribe_frame_serializes_correctly() {
        let mut set = HashSet::new();
        set.insert(tok("1"));
        set.insert(tok("22"));
        set.insert(tok("333"));
        let frame = build_subscribe_frame(set.iter().copied());
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("Market"));
        let arr = v.get("assets_ids").and_then(|x| x.as_array()).unwrap();
        let strs: Vec<&str> = arr.iter().filter_map(|x| x.as_str()).collect();
        assert_eq!(strs.len(), 3);
        // HashSet ordering is unspecified — assert membership only.
        for want in &["1", "22", "333"] {
            assert!(strs.contains(want), "missing {want}");
        }
    }

    #[test]
    fn price_change_updates_only_changed_levels() {
        let token = tok("42");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);

        // Seed with a full book.
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "42",
            "bids": [
                {"price":"0.30","size":"100"},
                {"price":"0.29","size":"50"}
            ],
            "asks": [
                {"price":"0.32","size":"80"},
                {"price":"0.33","size":"60"}
            ]
        })
        .to_string();
        let _ = handle_incoming_text(&snap, &mut state).unwrap();

        // Apply a delta: ask at 0.32 grows to 200, ask at 0.33 disappears.
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "42",
            "changes": [
                {"price":"0.32","size":"200","side":"SELL"},
                {"price":"0.33","size":"0","side":"SELL"}
            ]
        })
        .to_string();
        let updates = handle_incoming_text(&delta, &mut state).unwrap();
        assert_eq!(updates.len(), 1);
        let u = &updates[0];

        // bids unchanged
        assert_eq!(u.bids_ladder, vec![(0.30, 100.0), (0.29, 50.0)]);
        // asks now have 0.32@200 only
        assert_eq!(u.asks_ladder, vec![(0.32, 200.0)]);
    }

    #[test]
    fn price_change_buy_side_updates_bids() {
        let token = tok("11");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);

        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "11",
            "bids": [{"price":"0.20","size":"10"}],
            "asks": [{"price":"0.25","size":"10"}],
        })
        .to_string();
        let _ = handle_incoming_text(&snap, &mut state).unwrap();

        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "11",
            "changes": [
                {"price":"0.21","size":"5","side":"BUY"}
            ]
        })
        .to_string();
        let u = &handle_incoming_text(&delta, &mut state).unwrap()[0];
        assert_eq!(u.bids_ladder, vec![(0.21, 5.0), (0.20, 10.0)]);
        assert_eq!(u.best_bid, Some(0.21));
        assert_eq!(u.best_ask, Some(0.25));
    }

    #[test]
    fn last_trade_price_event_is_ignored() {
        let token = tok("8");
        let mut state = WatcherState::new();
        state.subscribed.insert(token);
        let raw = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "8",
            "price": "0.42",
            "size": "10"
        })
        .to_string();
        let updates = handle_incoming_text(&raw, &mut state).unwrap();
        assert!(updates.is_empty());
    }

    // ---------- backoff ----------

    #[test]
    fn exponential_backoff_caps_at_30s() {
        let mut b = ExpoBackoff::new();
        assert_eq!(b.next(), Duration::from_millis(1_000));
        assert_eq!(b.next(), Duration::from_millis(2_000));
        assert_eq!(b.next(), Duration::from_millis(4_000));
        assert_eq!(b.next(), Duration::from_millis(8_000));
        assert_eq!(b.next(), Duration::from_millis(16_000));
        assert_eq!(b.next(), Duration::from_millis(30_000));
        // Saturates.
        for _ in 0..10 {
            assert_eq!(b.next(), Duration::from_millis(30_000));
        }
        b.reset();
        assert_eq!(b.next(), Duration::from_millis(1_000));
    }

    // ---------- mock WS for reconnect-replay test ----------

    /// A hand-rolled mock connection. Records every outgoing frame and
    /// replays a script of inbound frames. Setting `fail_after` to Some(N)
    /// causes recv to error on the (N+1)th call, simulating a disconnect.
    #[derive(Default)]
    struct MockShared {
        outgoing: Vec<String>,
        incoming: std::collections::VecDeque<String>,
        recv_count: usize,
        fail_after: Option<usize>,
        closed: bool,
    }

    #[derive(Clone)]
    struct MockConn {
        shared: Arc<Mutex<MockShared>>,
    }

    impl MockConn {
        fn new(shared: Arc<Mutex<MockShared>>) -> Self {
            Self { shared }
        }
    }

    impl BookWsConn for MockConn {
        async fn send_text(&mut self, text: String) -> anyhow::Result<()> {
            let mut g = self.shared.lock().await;
            if g.closed {
                return Err(anyhow!("mock closed"));
            }
            g.outgoing.push(text);
            Ok(())
        }
        async fn recv_text(&mut self) -> anyhow::Result<Option<String>> {
            loop {
                {
                    let mut g = self.shared.lock().await;
                    if let Some(limit) = g.fail_after {
                        if g.recv_count >= limit {
                            g.closed = true;
                            return Err(anyhow!("mock disconnect"));
                        }
                    }
                    if let Some(msg) = g.incoming.pop_front() {
                        g.recv_count += 1;
                        return Ok(Some(msg));
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    #[tokio::test]
    async fn reconnect_resends_full_set() {
        // We test the inner run_loop directly across two fake "connections"
        // by simulating exactly what run_clob_book_watcher's outer loop does:
        // build a shared state, call run_loop twice with two MockConns.
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        let (book_tx, _book_rx) = mpsc::unbounded_channel();
        let mut state = WatcherState::new();

        // Pre-populate the subscribed set as if three Adds happened on the
        // first connection. (The Adds themselves are tested below as part
        // of the full first-connection flow.)
        state.subscribed.insert(tok("100"));
        state.subscribed.insert(tok("200"));
        state.subscribed.insert(tok("300"));

        // -------- first "connection" --------
        let shared1 = Arc::new(Mutex::new(MockShared {
            fail_after: Some(0), // disconnect immediately after subscribe
            ..Default::default()
        }));
        let mut c1 = MockConn::new(shared1.clone());
        let res = run_loop(&mut c1, &mut state, &mut sub_rx, &book_tx).await;
        assert!(res.is_err(), "expected err from forced disconnect");
        let g = shared1.lock().await;
        // First outgoing frame should be the initial replay containing all 3.
        assert!(!g.outgoing.is_empty(), "expected at least one outgoing");
        let first = &g.outgoing[0];
        let v: Value = serde_json::from_str(first).unwrap();
        assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("Market"));
        let ids: Vec<&str> = v
            .get("assets_ids")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert_eq!(ids.len(), 3);
        for want in &["100", "200", "300"] {
            assert!(ids.contains(want), "missing {want} in first-conn replay");
        }
        drop(g);

        // -------- second "connection" (the reconnect) --------
        let shared2 = Arc::new(Mutex::new(MockShared {
            fail_after: Some(0), // disconnect immediately after subscribe
            ..Default::default()
        }));
        let mut c2 = MockConn::new(shared2.clone());
        let res = run_loop(&mut c2, &mut state, &mut sub_rx, &book_tx).await;
        assert!(res.is_err());
        let g = shared2.lock().await;
        // CRITICAL: the very first frame on the new connection is the full
        // replay, NOT an empty subscribe.
        assert!(!g.outgoing.is_empty(), "reconnect must send subscribe frame");
        let first = &g.outgoing[0];
        let v: Value = serde_json::from_str(first).unwrap();
        let ids: Vec<&str> = v
            .get("assets_ids")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert_eq!(
            ids.len(),
            3,
            "reconnect frame must replay all 3 tokens, got {ids:?}"
        );
        for want in &["100", "200", "300"] {
            assert!(ids.contains(want), "post-reconnect missing {want}");
        }

        // sub_tx not used in this test but kept alive so sub_rx isn't closed.
        drop(sub_tx);
    }

    #[tokio::test]
    async fn add_command_triggers_subscribe_frame_and_filter_lets_event_through() {
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        let (book_tx, mut book_rx) = mpsc::unbounded_channel();
        let mut state = WatcherState::new();

        let shared = Arc::new(Mutex::new(MockShared::default()));
        // Send an Add THEN a book event for that token, then force disconnect.
        sub_tx.send(SubCmd::Add(tok("777"))).unwrap();
        {
            let mut g = shared.lock().await;
            g.incoming.push_back(
                serde_json::json!({
                    "event_type": "book",
                    "asset_id": "777",
                    "bids": [{"price":"0.40","size":"10"}],
                    "asks": [{"price":"0.50","size":"10"}],
                })
                .to_string(),
            );
            g.fail_after = Some(1);
        }

        let mut conn = MockConn::new(shared.clone());
        let _ = run_loop(&mut conn, &mut state, &mut sub_rx, &book_tx).await;

        let g = shared.lock().await;
        // Frames sent: initial empty subscribe, then per-Add subscribe.
        assert!(g.outgoing.len() >= 2, "want initial + add frame");
        let add_frame: Value = serde_json::from_str(&g.outgoing[1]).unwrap();
        let ids: Vec<&str> = add_frame
            .get("assets_ids")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert_eq!(ids, vec!["777"]);

        // Downstream got the book update.
        let update = book_rx.try_recv().expect("book update should be emitted");
        assert_eq!(update.token_id, tok("777"));
        assert_eq!(update.best_bid, Some(0.40));
        assert_eq!(update.best_ask, Some(0.50));

        // State invariant: token is in the subscribed set.
        assert!(state.subscribed.contains(&tok("777")));
    }
}
