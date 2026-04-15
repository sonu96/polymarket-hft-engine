//! Polymarket CLOB **user** WebSocket watcher.
//!
//! Owns a single long-lived connection to
//! `wss://ws-subscriptions-clob.polymarket.com/ws/user` and fans out two
//! typed event streams for downstream consumers:
//!
//! * `FillEvent` — a `trade` event on one of our own orders. Consumed by
//!   Portfolio (ticket #8) to book realized P&L and by EdgeBook (ticket #6)
//!   to decrement resting-maker exposure.
//! * `OrderStateEvent` — a lifecycle transition on one of our orders
//!   (placement / cancellation / update). Consumed by EdgeBook to keep its
//!   resting-maker map in sync with the exchange's view.
//!
//! ## Auth + subscription
//!
//! Unlike the market (`ws/market`) channel, the user channel requires API-key
//! auth. Polymarket accepts a **single** frame carrying both auth and the
//! initial market subscription:
//!
//! ```json
//! {"auth": {"apiKey": "...", "secret": "...", "passphrase": "..."},
//!  "type": "user",
//!  "markets": ["<condition_id_hex>", ...]}
//! ```
//!
//! The credentials are passed explicitly to `run_clob_user_watcher` — this
//! module does NOT reach into `Executor` to pull them, because that would
//! couple the watcher to the order-issuing path.
//!
//! ## Reconnect replay
//!
//! Same invariant as `clob_book.rs`: subscriptions are not persisted across
//! disconnects. On every (re)connect the first and only auth+subscribe frame
//! must carry the **full** authoritative `HashSet<String>` of subscribed
//! condition ids. We never unsubscribe — removal is local-only and the wire
//! subscription for a removed market simply isn't re-sent next reconnect.
//!
//! ## Wire shapes
//!
//! Polymarket sends two different envelopes:
//!
//! * Single event: `{"event_type":"trade", ...}` / `{"event_type":"order", ...}`
//! * Batched array: `[{"event_type":"trade", ...}, ...]`
//!
//! Both are accepted; we iterate arrays and dispatch per-item.

// Wired in by ticket #12 (main.rs fan-out); silence dead_code until then so
// the build stays warning-clean for unrelated reviewers.
#![allow(dead_code)]

use crate::config::Config;
use alloy_primitives::U256;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk::clob::types::Side;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single fill on one of our orders (or a subsequent confirmation for an
/// already-reported fill as it transitions MATCHED → MINED / CONFIRMED).
/// Downstream dedup responsibility sits with the consumer.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub token_id: U256,
    pub order_id: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub matched_at_ms: i64,
    /// "MATCHED" | "MINED" | "CONFIRMED" | …
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStateKind {
    Placement,
    Cancellation,
    Update,
}

/// Lifecycle transition on one of our orders — emitted by the user channel's
/// `order` event_type. Consumers use it to keep resting-maker state aligned
/// with the exchange's view.
#[derive(Debug, Clone)]
pub struct OrderStateEvent {
    pub token_id: U256,
    pub order_id: String,
    pub kind: OrderStateKind,
    pub status: String,
    pub size_matched: f64,
    pub original_size: f64,
    pub price: f64,
    pub ts_ms: i64,
}

pub type FillEventSender = mpsc::UnboundedSender<FillEvent>;
pub type FillEventReceiver = mpsc::UnboundedReceiver<FillEvent>;
pub type OrderStateSender = mpsc::UnboundedSender<OrderStateEvent>;
pub type OrderStateReceiver = mpsc::UnboundedReceiver<OrderStateEvent>;

#[derive(Debug, Clone)]
pub struct ClobUserCreds {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

/// Upstream command to add or remove a condition-id from the subscribed set.
/// Mirrors `clob_book::SubCmd` but for the user channel (which is keyed by
/// condition_id, not token_id).
#[derive(Debug, Clone)]
pub enum UserSubCmd {
    Add(String),
    Remove(String),
}
pub type UserSubCmdSender = mpsc::UnboundedSender<UserSubCmd>;
pub type UserSubCmdReceiver = mpsc::UnboundedReceiver<UserSubCmd>;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the CLOB user watcher forever. Owns the subscribed-market set, owns
/// the WS connection, owns the reconnect loop.
pub async fn run_clob_user_watcher(
    cfg: &Config,
    creds: ClobUserCreds,
    initial_markets: Vec<String>,
    market_sub_rx: UserSubCmdReceiver,
    fill_tx: FillEventSender,
    order_state_tx: OrderStateSender,
) -> Result<()> {
    let url = cfg.clob_ws_user_url.clone();
    let mut subscribed: HashSet<String> = initial_markets.into_iter().collect();
    let mut backoff = ExpoBackoff::new();
    let mut market_sub_rx = market_sub_rx;

    let mut reconnect_count: u64 = 0;
    loop {
        // Drain any pending SubCmds into `subscribed` BEFORE (re)connecting so
        // the very first auth+subscribe frame is authoritative. Without this
        // drain, an Add that raced the reconnect window would not appear in
        // the replay and we'd silently miss events for that market.
        drain_pending_cmds(&mut market_sub_rx, &mut subscribed);

        tracing::info!(
            url = %url,
            markets = subscribed.len(),
            reconnect = reconnect_count,
            "[clob_user] connecting"
        );

        let conn_res = connect_async(&url).await;
        let ws = match conn_res {
            Ok((ws, _)) => ws,
            Err(e) => {
                let wait = backoff.next();
                tracing::warn!(
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    reconnect = reconnect_count,
                    "[clob_user] connect failed"
                );
                tokio::time::sleep(wait).await;
                reconnect_count += 1;
                continue;
            }
        };
        backoff.reset();

        let mut io = TungsteniteConn { ws };
        match run_session(
            &mut io,
            &creds,
            &mut subscribed,
            &mut market_sub_rx,
            &fill_tx,
            &order_state_tx,
        )
        .await
        {
            Ok(LoopExit::DownstreamDropped) => {
                return Err(anyhow!(
                    "[clob_user] downstream receiver dropped — fatal"
                ));
            }
            Ok(LoopExit::SubCmdClosed) => {
                tracing::warn!("[clob_user] sub-cmd channel closed, exiting");
                return Ok(());
            }
            Err(e) => {
                let wait = backoff.next();
                reconnect_count += 1;
                tracing::warn!(
                    error = %e,
                    backoff_ms = wait.as_millis() as u64,
                    reconnect = reconnect_count,
                    markets = subscribed.len(),
                    "[clob_user] session error, will reconnect after backoff"
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IO abstraction (so the loop is testable without a real socket)
// ---------------------------------------------------------------------------

trait UserWsConn: Send {
    fn send_text(
        &mut self,
        text: String,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn recv_text(
        &mut self,
    ) -> impl std::future::Future<Output = Option<Result<String>>> + Send;
}

struct TungsteniteConn {
    ws: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl UserWsConn for TungsteniteConn {
    async fn send_text(&mut self, text: String) -> Result<()> {
        self.ws
            .send(Message::Text(text.into()))
            .await
            .context("ws send")
    }

    async fn recv_text(&mut self) -> Option<Result<String>> {
        loop {
            match self.ws.next().await {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow!("ws recv: {e}"))),
                Some(Ok(Message::Text(t))) => return Some(Ok(t.to_string())),
                Some(Ok(Message::Binary(_))) => continue,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Frame(_))) => continue,
                Some(Ok(Message::Close(_))) => return None,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Session loop (one WS lifetime)
// ---------------------------------------------------------------------------

enum LoopExit {
    DownstreamDropped,
    SubCmdClosed,
}

async fn run_session<C: UserWsConn>(
    conn: &mut C,
    creds: &ClobUserCreds,
    subscribed: &mut HashSet<String>,
    market_sub_rx: &mut UserSubCmdReceiver,
    fill_tx: &FillEventSender,
    order_state_tx: &OrderStateSender,
) -> Result<LoopExit> {
    // Non-negotiable first action: send the combined auth + subscribe frame
    // before any recv / select — reconnect replay invariant.
    send_auth_and_subscribe(conn, creds, subscribed).await?;

    loop {
        tokio::select! {
            biased;

            cmd = market_sub_rx.recv() => {
                let cmd = match cmd {
                    Some(c) => c,
                    None => return Ok(LoopExit::SubCmdClosed),
                };
                apply_sub_cmd(cmd, subscribed);
                // Drain any co-arriving commands so we ship exactly one
                // updated subscription frame per select wakeup.
                loop {
                    match market_sub_rx.try_recv() {
                        Ok(c) => apply_sub_cmd(c, subscribed),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return Ok(LoopExit::SubCmdClosed),
                    }
                }
                let frame = build_subscribe_frame(subscribed);
                conn.send_text(frame).await.context("sub update")?;
            }

            recv = conn.recv_text() => {
                let txt_res = match recv {
                    Some(r) => r,
                    None => return Err(anyhow!("ws stream closed")),
                };
                let txt = txt_res?;
                match dispatch_frame(&txt, fill_tx, order_state_tx) {
                    Ok(DispatchOutcome::Ok) => {}
                    Ok(DispatchOutcome::DownstreamDropped) => {
                        return Ok(LoopExit::DownstreamDropped);
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "[clob_user] ignored unparsable frame");
                    }
                }
            }
        }
    }
}

fn apply_sub_cmd(cmd: UserSubCmd, subscribed: &mut HashSet<String>) {
    match cmd {
        UserSubCmd::Add(m) => {
            if subscribed.insert(m.clone()) {
                tracing::info!(market = %m, "[clob_user] market added");
            }
        }
        UserSubCmd::Remove(m) => {
            if subscribed.remove(&m) {
                tracing::info!(market = %m, "[clob_user] market removed (local)");
            }
        }
    }
}

fn drain_pending_cmds(rx: &mut UserSubCmdReceiver, subscribed: &mut HashSet<String>) {
    loop {
        match rx.try_recv() {
            Ok(c) => apply_sub_cmd(c, subscribed),
            Err(_) => return,
        }
    }
}

// ---------------------------------------------------------------------------
// Wire format helpers
// ---------------------------------------------------------------------------

/// Build the combined auth + markets frame. The user channel accepts a single
/// frame carrying both — one send, no round-trip.
fn build_auth_subscribe_frame(creds: &ClobUserCreds, subscribed: &HashSet<String>) -> String {
    let mut markets: Vec<&str> = subscribed.iter().map(|s| s.as_str()).collect();
    // Deterministic order for testability. HashSet iteration is unspecified.
    markets.sort_unstable();
    json!({
        "auth": {
            "apiKey": creds.api_key,
            "secret": creds.secret,
            "passphrase": creds.passphrase,
        },
        "type": "user",
        "markets": markets,
    })
    .to_string()
}

/// Build an auth-less subscription update frame. Sent in-session when an
/// Add/Remove command updates the local set — Polymarket's user channel
/// accepts additional `{"type":"user","markets":[…]}` frames to refresh the
/// subscription on an already-authenticated connection.
fn build_subscribe_frame(subscribed: &HashSet<String>) -> String {
    let mut markets: Vec<&str> = subscribed.iter().map(|s| s.as_str()).collect();
    markets.sort_unstable();
    json!({
        "type": "user",
        "markets": markets,
    })
    .to_string()
}

async fn send_auth_and_subscribe<C: UserWsConn>(
    conn: &mut C,
    creds: &ClobUserCreds,
    subscribed: &HashSet<String>,
) -> Result<()> {
    let frame = build_auth_subscribe_frame(creds, subscribed);
    conn.send_text(frame)
        .await
        .context("auth+subscribe frame")?;
    tracing::info!(
        markets = subscribed.len(),
        "[clob_user] auth+subscribe frame sent"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Parsing + dispatch
// ---------------------------------------------------------------------------

enum DispatchOutcome {
    Ok,
    DownstreamDropped,
}

fn dispatch_frame(
    raw: &str,
    fill_tx: &FillEventSender,
    order_state_tx: &OrderStateSender,
) -> Result<DispatchOutcome> {
    let v: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(e) => {
            return Err(anyhow!("frame is not valid JSON: {e}"));
        }
    };

    let items: Vec<serde_json::Value> = match v {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(_) => vec![v],
        // Non-JSON-object / non-array (e.g. a bare string like "PONG") is not
        // an error — Polymarket sends keepalives in that shape. Drop silently.
        _ => {
            tracing::debug!(frame = %raw, "[clob_user] dropping non-object/array frame");
            return Ok(DispatchOutcome::Ok);
        }
    };

    for item in items {
        let Some(obj) = item.as_object() else {
            tracing::debug!("[clob_user] dropping non-object batched item");
            continue;
        };
        let event_type = obj.get("event_type").and_then(|x| x.as_str()).unwrap_or("");
        match event_type {
            "trade" => {
                let fill = match parse_trade(&item) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::debug!(error = %e, "[clob_user] bad trade frame");
                        continue;
                    }
                };
                if fill_tx.send(fill).is_err() {
                    return Ok(DispatchOutcome::DownstreamDropped);
                }
            }
            "order" => {
                let evt = match parse_order_state(&item) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::debug!(error = %e, "[clob_user] bad order frame");
                        continue;
                    }
                };
                if order_state_tx.send(evt).is_err() {
                    return Ok(DispatchOutcome::DownstreamDropped);
                }
            }
            other => {
                tracing::debug!(event_type = %other, "[clob_user] dropping unknown event_type");
            }
        }
    }
    Ok(DispatchOutcome::Ok)
}

#[derive(Debug, Deserialize)]
struct RawTrade {
    asset_id: String,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    taker_order_id: Option<String>,
    side: String,
    price: String,
    size: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default, rename = "match_time")]
    match_time: Option<String>,
}

fn parse_trade(v: &serde_json::Value) -> Result<FillEvent> {
    let raw: RawTrade = serde_json::from_value(v.clone()).context("trade shape")?;
    let token_id = parse_token_id(&raw.asset_id)?;
    let side = parse_side(&raw.side)?;
    let price: f64 = raw.price.parse().context("trade.price")?;
    let size: f64 = raw.size.parse().context("trade.size")?;
    let order_id = raw
        .order_id
        .or(raw.taker_order_id)
        .unwrap_or_default();
    let matched_at_ms = raw
        .timestamp
        .as_deref()
        .or(raw.match_time.as_deref())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let status = raw.status.unwrap_or_else(|| "MATCHED".to_string());
    Ok(FillEvent {
        token_id,
        order_id,
        side,
        price,
        size,
        matched_at_ms,
        status,
    })
}

#[derive(Debug, Deserialize)]
struct RawOrderState {
    asset_id: String,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    size_matched: Option<String>,
    #[serde(default)]
    original_size: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
}

fn parse_order_state(v: &serde_json::Value) -> Result<OrderStateEvent> {
    let raw: RawOrderState = serde_json::from_value(v.clone()).context("order shape")?;
    let token_id = parse_token_id(&raw.asset_id)?;
    let order_id = raw.order_id.or(raw.id).unwrap_or_default();
    let kind = match raw.kind.as_deref().unwrap_or("") {
        "PLACEMENT" | "placement" => OrderStateKind::Placement,
        "CANCELLATION" | "cancellation" | "CANCELED" | "canceled" => OrderStateKind::Cancellation,
        "UPDATE" | "update" => OrderStateKind::Update,
        other => {
            return Err(anyhow!("unknown order type: {other}"));
        }
    };
    let status = raw.status.unwrap_or_default();
    let size_matched = raw
        .size_matched
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let original_size = raw
        .original_size
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let price = raw
        .price
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let ts_ms = raw
        .timestamp
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    Ok(OrderStateEvent {
        token_id,
        order_id,
        kind,
        status,
        size_matched,
        original_size,
        price,
        ts_ms,
    })
}

fn parse_token_id(s: &str) -> Result<U256> {
    U256::from_str_radix(s, 10)
        .map_err(|e| anyhow!("asset_id not a decimal U256: {e}"))
}

fn parse_side(s: &str) -> Result<Side> {
    match s {
        "BUY" | "buy" | "Buy" => Ok(Side::Buy),
        "SELL" | "sell" | "Sell" => Ok(Side::Sell),
        other => Err(anyhow!("unknown side: {other}")),
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
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;

    fn creds() -> ClobUserCreds {
        ClobUserCreds {
            api_key: "ak-123".into(),
            secret: "sk-456".into(),
            passphrase: "pp-789".into(),
        }
    }

    // ---------- backoff ----------

    #[test]
    fn expo_backoff_doubles_then_caps_at_30s() {
        let mut b = ExpoBackoff::new();
        assert_eq!(b.next(), Duration::from_millis(1_000));
        assert_eq!(b.next(), Duration::from_millis(2_000));
        assert_eq!(b.next(), Duration::from_millis(4_000));
        assert_eq!(b.next(), Duration::from_millis(8_000));
        assert_eq!(b.next(), Duration::from_millis(16_000));
        assert_eq!(b.next(), Duration::from_millis(30_000));
        for _ in 0..5 {
            assert_eq!(b.next(), Duration::from_millis(30_000));
        }
        b.reset();
        assert_eq!(b.next(), Duration::from_millis(1_000));
    }

    // ---------- auth frame ----------

    #[test]
    fn auth_frame_serializes_with_all_three_keys_and_initial_markets() {
        let mut set = HashSet::new();
        set.insert("0xabc".to_string());
        set.insert("0xdef".to_string());
        let frame = build_auth_subscribe_frame(&creds(), &set);
        let v: Value = serde_json::from_str(&frame).unwrap();

        let auth = v.get("auth").and_then(|x| x.as_object()).unwrap();
        assert_eq!(auth.get("apiKey").and_then(|x| x.as_str()), Some("ak-123"));
        assert_eq!(auth.get("secret").and_then(|x| x.as_str()), Some("sk-456"));
        assert_eq!(
            auth.get("passphrase").and_then(|x| x.as_str()),
            Some("pp-789")
        );

        assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("user"));

        let arr = v.get("markets").and_then(|x| x.as_array()).unwrap();
        let strs: Vec<&str> = arr.iter().filter_map(|x| x.as_str()).collect();
        assert_eq!(strs.len(), 2);
        assert!(strs.contains(&"0xabc"));
        assert!(strs.contains(&"0xdef"));
    }

    // ---------- parsing ----------

    fn make_channels() -> (
        FillEventSender,
        FillEventReceiver,
        OrderStateSender,
        OrderStateReceiver,
    ) {
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (os_tx, os_rx) = mpsc::unbounded_channel();
        (fill_tx, fill_rx, os_tx, os_rx)
    }

    #[test]
    fn parse_single_trade_event_produces_fill_event() {
        let (ftx, mut frx, otx, _orx) = make_channels();
        let raw = serde_json::json!({
            "event_type": "trade",
            "asset_id": "1234567890",
            "order_id": "order-abc",
            "side": "BUY",
            "price": "0.42",
            "size": "100",
            "status": "MATCHED",
            "timestamp": "1700000000000",
        })
        .to_string();
        let out = dispatch_frame(&raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        let fill = frx.try_recv().expect("fill event should be emitted");
        assert_eq!(fill.token_id, U256::from_str_radix("1234567890", 10).unwrap());
        assert_eq!(fill.order_id, "order-abc");
        assert!(matches!(fill.side, Side::Buy));
        assert_eq!(fill.price, 0.42);
        assert_eq!(fill.size, 100.0);
        assert_eq!(fill.status, "MATCHED");
        assert_eq!(fill.matched_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn parse_batched_trade_events_produces_multiple_fills() {
        let (ftx, mut frx, otx, _orx) = make_channels();
        let raw = serde_json::json!([
            {
                "event_type": "trade",
                "asset_id": "1",
                "order_id": "o1",
                "side": "BUY",
                "price": "0.10",
                "size": "5",
                "status": "MATCHED",
                "timestamp": "1700000000001",
            },
            {
                "event_type": "trade",
                "asset_id": "2",
                "order_id": "o2",
                "side": "SELL",
                "price": "0.90",
                "size": "7",
                "status": "MINED",
                "timestamp": "1700000000002",
            },
            {
                "event_type": "trade",
                "asset_id": "3",
                "order_id": "o3",
                "side": "SELL",
                "price": "0.50",
                "size": "3",
                "status": "MATCHED",
                "timestamp": "1700000000003",
            }
        ])
        .to_string();
        let out = dispatch_frame(&raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        let f1 = frx.try_recv().unwrap();
        let f2 = frx.try_recv().unwrap();
        let f3 = frx.try_recv().unwrap();
        assert!(frx.try_recv().is_err());
        assert_eq!(f1.order_id, "o1");
        assert_eq!(f2.order_id, "o2");
        assert_eq!(f3.order_id, "o3");
        assert!(matches!(f1.side, Side::Buy));
        assert!(matches!(f2.side, Side::Sell));
        assert_eq!(f2.status, "MINED");
    }

    #[test]
    fn parse_order_placement_event() {
        let (ftx, _frx, otx, mut orx) = make_channels();
        let raw = serde_json::json!({
            "event_type": "order",
            "asset_id": "999",
            "order_id": "ord-1",
            "type": "PLACEMENT",
            "status": "LIVE",
            "size_matched": "0",
            "original_size": "100",
            "price": "0.37",
            "timestamp": "1700000000500",
        })
        .to_string();
        let out = dispatch_frame(&raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        let evt = orx.try_recv().expect("order state event");
        assert_eq!(evt.order_id, "ord-1");
        assert_eq!(evt.kind, OrderStateKind::Placement);
        assert_eq!(evt.status, "LIVE");
        assert_eq!(evt.size_matched, 0.0);
        assert_eq!(evt.original_size, 100.0);
        assert_eq!(evt.price, 0.37);
        assert_eq!(evt.ts_ms, 1_700_000_000_500);
        assert_eq!(evt.token_id, U256::from_str_radix("999", 10).unwrap());
    }

    #[test]
    fn parse_order_cancellation_event() {
        let (ftx, _frx, otx, mut orx) = make_channels();
        let raw = serde_json::json!({
            "event_type": "order",
            "asset_id": "888",
            "order_id": "ord-2",
            "type": "CANCELLATION",
            "status": "CANCELED",
            "size_matched": "25",
            "original_size": "100",
            "price": "0.40",
            "timestamp": "1700000000600",
        })
        .to_string();
        let out = dispatch_frame(&raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        let evt = orx.try_recv().unwrap();
        assert_eq!(evt.kind, OrderStateKind::Cancellation);
        assert_eq!(evt.size_matched, 25.0);
        assert_eq!(evt.original_size, 100.0);
        assert_eq!(evt.status, "CANCELED");
    }

    #[test]
    fn parse_unknown_event_type_is_ignored() {
        let (ftx, mut frx, otx, mut orx) = make_channels();
        let raw = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "7",
            "price": "0.50",
        })
        .to_string();
        let out = dispatch_frame(&raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        assert!(frx.try_recv().is_err());
        assert!(orx.try_recv().is_err());
    }

    #[test]
    fn parse_malformed_json_does_not_crash() {
        let (ftx, _frx, otx, _orx) = make_channels();
        let raw = "{this is not valid json";
        let res = dispatch_frame(raw, &ftx, &otx);
        // Error is expected; the outer run_session logs+drops and continues.
        assert!(res.is_err());
    }

    #[test]
    fn parse_non_object_frame_is_ignored() {
        let (ftx, mut frx, otx, mut orx) = make_channels();
        // Bare string keepalives like "PONG" are valid JSON but not objects.
        let raw = "\"PONG\"";
        let out = dispatch_frame(raw, &ftx, &otx).unwrap();
        assert!(matches!(out, DispatchOutcome::Ok));
        assert!(frx.try_recv().is_err());
        assert!(orx.try_recv().is_err());
    }

    #[test]
    fn sub_cmd_add_inserts_into_subscribed_set() {
        let mut set = HashSet::new();
        apply_sub_cmd(UserSubCmd::Add("0xabc".to_string()), &mut set);
        assert!(set.contains("0xabc"));
        assert_eq!(set.len(), 1);
        // Re-adding is a no-op.
        apply_sub_cmd(UserSubCmd::Add("0xabc".to_string()), &mut set);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn sub_cmd_remove_removes_from_subscribed_set() {
        let mut set = HashSet::new();
        set.insert("0xabc".to_string());
        set.insert("0xdef".to_string());
        apply_sub_cmd(UserSubCmd::Remove("0xabc".to_string()), &mut set);
        assert_eq!(set.len(), 1);
        assert!(!set.contains("0xabc"));
        assert!(set.contains("0xdef"));
        // Removing a non-member is a no-op.
        apply_sub_cmd(UserSubCmd::Remove("0xghi".to_string()), &mut set);
        assert_eq!(set.len(), 1);
    }

    // ---------- mock WS for reconnect-replay test ----------

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

    impl UserWsConn for MockConn {
        async fn send_text(&mut self, text: String) -> Result<()> {
            let mut g = self.shared.lock().await;
            if g.closed {
                return Err(anyhow!("mock closed"));
            }
            g.outgoing.push(text);
            Ok(())
        }
        async fn recv_text(&mut self) -> Option<Result<String>> {
            loop {
                {
                    let mut g = self.shared.lock().await;
                    if let Some(limit) = g.fail_after {
                        if g.recv_count >= limit {
                            g.closed = true;
                            return Some(Err(anyhow!("mock disconnect")));
                        }
                    }
                    if let Some(msg) = g.incoming.pop_front() {
                        g.recv_count += 1;
                        return Some(Ok(msg));
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    #[tokio::test]
    async fn reconnect_resends_auth_and_full_markets_set() {
        // Simulate exactly what run_clob_user_watcher's outer loop does: call
        // run_session twice against two fresh MockConns, sharing the same
        // `subscribed` set. The invariant we're testing is that the first
        // outgoing frame on BOTH sessions is an identical auth+subscribe
        // carrying the full set.
        let (_sub_tx, mut sub_rx) = mpsc::unbounded_channel::<UserSubCmd>();
        let (fill_tx, _fill_rx) = mpsc::unbounded_channel();
        let (os_tx, _os_rx) = mpsc::unbounded_channel();

        let mut subscribed: HashSet<String> = HashSet::new();
        subscribed.insert("0xmarketA".to_string());
        subscribed.insert("0xmarketB".to_string());
        subscribed.insert("0xmarketC".to_string());

        let creds = creds();

        // First session — force immediate disconnect after the subscribe send.
        let shared1 = Arc::new(Mutex::new(MockShared {
            fail_after: Some(0),
            ..Default::default()
        }));
        let mut c1 = MockConn::new(shared1.clone());
        let res1 = run_session(
            &mut c1,
            &creds,
            &mut subscribed,
            &mut sub_rx,
            &fill_tx,
            &os_tx,
        )
        .await;
        assert!(res1.is_err(), "expected err from forced disconnect");

        let g1 = shared1.lock().await;
        assert_eq!(
            g1.outgoing.len(),
            1,
            "expected exactly one outgoing frame (the auth+subscribe)"
        );
        let frame1: Value = serde_json::from_str(&g1.outgoing[0]).unwrap();
        assert!(frame1.get("auth").is_some(), "frame1 must carry auth");
        assert_eq!(frame1.get("type").and_then(|x| x.as_str()), Some("user"));
        let markets1: Vec<&str> = frame1
            .get("markets")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert_eq!(markets1.len(), 3);
        for want in &["0xmarketA", "0xmarketB", "0xmarketC"] {
            assert!(markets1.contains(want), "missing {want} on session 1");
        }
        drop(g1);

        // Second session — the "reconnect". Same set, fresh conn.
        let shared2 = Arc::new(Mutex::new(MockShared {
            fail_after: Some(0),
            ..Default::default()
        }));
        let mut c2 = MockConn::new(shared2.clone());
        let res2 = run_session(
            &mut c2,
            &creds,
            &mut subscribed,
            &mut sub_rx,
            &fill_tx,
            &os_tx,
        )
        .await;
        assert!(res2.is_err());

        let g2 = shared2.lock().await;
        assert_eq!(
            g2.outgoing.len(),
            1,
            "reconnect must send exactly one frame — full auth+subscribe"
        );
        // CRITICAL: the very first (and only) frame on the new connection is
        // an auth+subscribe identical in shape to session 1.
        assert_eq!(
            g2.outgoing[0], g1_outgoing_first(&shared1).await,
            "reconnect frame must be identical to initial frame (sorted markets + same creds)"
        );
        let frame2: Value = serde_json::from_str(&g2.outgoing[0]).unwrap();
        assert!(frame2.get("auth").is_some(), "frame2 must carry auth");
        let markets2: Vec<&str> = frame2
            .get("markets")
            .and_then(|x| x.as_array())
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert_eq!(
            markets2.len(),
            3,
            "reconnect frame must replay all 3 markets, got {markets2:?}"
        );
        for want in &["0xmarketA", "0xmarketB", "0xmarketC"] {
            assert!(markets2.contains(want), "post-reconnect missing {want}");
        }
    }

    async fn g1_outgoing_first(shared: &Arc<Mutex<MockShared>>) -> String {
        shared.lock().await.outgoing[0].clone()
    }
}
