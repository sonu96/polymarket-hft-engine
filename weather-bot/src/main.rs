mod alerts;
mod climo;
mod config;
mod ctf_math;
mod edge_book;
mod executor;
mod mint_executor;
mod no_edge;
mod order_sink;
mod paper;
mod portfolio;
mod presigner;
mod quoter;
mod pricer;
mod scanner;
mod state;
mod types;
mod watchers;
mod weather_filter;

use crate::alerts::send_alert;
use crate::config::Config;
use crate::edge_book::{EdgeCmd, EdgeCmdSender};
use crate::executor::Executor;
use crate::mint_executor::MintExecutor;
use crate::no_edge::bootstrap::EdgeCmdEventSink;
use crate::paper::PaperEngine;
use crate::portfolio::PortfolioHandle;
use crate::presigner::{OrderTemplate, Presigner};
use crate::quoter::QuoterCmdSender;
use crate::state::BotState;
use crate::types::{
    ClobMarketReady, DiscoverySource, MintReceipt, WeatherEvent,
};
use crate::watchers::clob_user::ClobUserCreds;
use alloy_primitives::Address;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::{mpsc, Mutex};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("weather_bot=info")
        .init();

    tracing::info!("===============================================");
    tracing::info!("  Polymarket Weather HFT — Mint + Dump Pipeline ");
    tracing::info!("===============================================");

    let config = Arc::new(Config::load());
    let state = Arc::new(Mutex::new(BotState::load()));

    if config.private_key.is_empty() && !config.simulation {
        tracing::error!("PRIVATE_KEY not set and SIMULATION=false — refusing to start");
        std::process::exit(1);
    }

    let mode_label = if config.paper_mode {
        "PAPER"
    } else if config.simulation {
        "SIMULATION"
    } else {
        "LIVE"
    };
    tracing::info!(
        "[CONFIG] mode={} mint=${:.2} daily_cap=${:.0} min_dump=${:.3} dump_frac={:.2}",
        mode_label,
        config.mint_amount_usdc,
        config.daily_cap_usdc,
        config.min_dump_price,
        config.dump_fraction,
    );
    tracing::info!("[CONFIG] polygon_wss={}", config.polygon_wss_url);
    tracing::info!("[CONFIG] clob_ws={}", config.clob_ws_market_url);
    if config.paper_mode {
        tracing::info!(
            "[CONFIG] paper: bankroll=${:.0} max_concurrent={} log={}",
            config.paper_starting_bankroll,
            config.paper_max_concurrent_events,
            config.paper_log_path,
        );
    }

    // --- Executors ---
    let executor = Arc::new(
        Executor::new((*config).clone())
            .await
            .map_err(|e| {
                tracing::error!("[EXEC] init failed: {}", e);
                e
            })?,
    );
    let mint_exec = Arc::new(MintExecutor::new(config.clone()));
    let presigner = Arc::new(Presigner::new(
        executor.clone(),
        OrderTemplate {
            min_dump_price: config.min_dump_price,
            dump_fraction: config.dump_fraction,
            mint_amount_usdc: config.mint_amount_usdc,
        },
    ));
    // Paper engine is only instantiated when paper_mode is on. When enabled
    // it shadows mint_exec + presigner entirely — we take the PaperEngine
    // branch in the main select arm below and skip the live executors.
    let paper_engine: Option<Arc<PaperEngine>> = if config.paper_mode {
        Some(Arc::new(PaperEngine::new(
            config.clob_api_url.clone(),
            config.paper_starting_bankroll,
            config.paper_max_concurrent_events,
            config.mint_amount_usdc,
            config.min_dump_price,
            config.dump_fraction,
            PathBuf::from(&config.paper_log_path),
        )))
    } else {
        None
    };

    send_alert(&config, "🌡️ Weather HFT bot started").await;

    // --- Event channels ---
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<WeatherEvent>();
    let (mint_tx, mut mint_rx) = mpsc::unbounded_channel::<MintReceipt>();
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<ClobMarketReady>();

    // --- Spawn watchers ---
    let wss = config.polygon_wss_url.clone();
    let event_tx_cl = event_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = watchers::onchain::run_onchain_watcher(wss, event_tx_cl).await {
            tracing::error!("[onchain] fatal: {}", e);
        }
    });

    let clob_wss = config.clob_ws_market_url.clone();
    let anchor = config.clob_anchor_asset_id.clone();
    let ready_tx_cl = ready_tx.clone();
    tokio::spawn(async move {
        if let Err(e) =
            watchers::clob_ws::run_clob_market_watcher(clob_wss, anchor, ready_tx_cl).await
        {
            tracing::error!("[clob_ws] fatal: {}", e);
        }
    });

    // Mempool watcher (live only — skip in sim to avoid noisy public-node traffic)
    if !config.simulation {
        let wss = config.polygon_wss_url.clone();
        let addr = derive_signer_address(&config.private_key).unwrap_or(Address::ZERO);
        let mint_tx_cl = mint_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = watchers::mempool::run_mempool_watcher(wss, addr, mint_tx_cl).await {
                tracing::error!("[mempool] fatal: {}", e);
            }
        });
    }

    // =========================================================================
    // Phase 3 NO-edge farmer wiring (ticket #12)
    //
    // Everything below this block is inert unless `NO_EDGE_FARMER_ENABLED=1`.
    // When enabled, we spawn the EdgeBook actor, the forecast / METAR /
    // clob_book / clob_user watchers, the portfolio facade, and the one-shot
    // bootstrap replay. The main `select!` arms for fills / signals / book
    // updates / forecast / nowcast are added below; they short-circuit when
    // the feature is off by holding `None` channel halves that never fire.
    // =========================================================================
    let phase3 = if config.no_edge_farmer_enabled {
        match spawn_phase3(&config, executor.clone(), paper_engine.clone()) {
            Ok(h) => Some(h),
            Err(e) => {
                tracing::error!("[PHASE3] spawn failed: {:#} — falling back to Phase 2 only", e);
                None
            }
        }
    } else {
        None
    };

    // Decompose the Phase 3 handles into Options so the `select!` below can
    // pattern-match each receiver independently. `tokio::select!` does not
    // fire an arm whose future is `Pending` forever, so guarded `if` arms
    // are the standard pattern for optional receivers. We hold onto the
    // Phase 3 "keep-alive" senders (`_user_sub_tx`, `_sub_cmd_tx`) via
    // `_keepalive` so their downstream watchers don't see their command
    // channels close for the whole lifetime of the event loop.
    let mut edge_cmd_tx: Option<EdgeCmdSender> = None;
    let mut book_update_rx = None;
    let mut forecast_rx = None;
    let mut nowcast_rx = None;
    let mut fill_rx = None;
    let mut order_state_rx = None;
    let mut edge_signal_rx = None;
    let mut portfolio_handle: Option<PortfolioHandle> = None;
    let mut quoter_cmd_tx: Option<QuoterCmdSender> = None;
    let _keepalive: Option<(
        mpsc::UnboundedSender<crate::watchers::clob_user::UserSubCmd>,
        crate::types::SubCmdSender,
    )> = phase3.map(|h| {
        edge_cmd_tx = Some(h.edge_cmd_tx);
        book_update_rx = Some(h.book_update_rx);
        forecast_rx = Some(h.forecast_rx);
        nowcast_rx = Some(h.nowcast_rx);
        fill_rx = Some(h.fill_rx);
        order_state_rx = Some(h.order_state_rx);
        edge_signal_rx = Some(h.edge_signal_rx);
        portfolio_handle = Some(h.portfolio_handle);
        quoter_cmd_tx = h.quoter_cmd_tx;
        (h._user_sub_tx, h._sub_cmd_tx)
    });

    tracing::info!("[MAIN] event loop ready — waiting for on-chain weather events");

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                tracing::info!(
                    "[EVENT] {} buckets={} detected_at_ns={} source={:?}",
                    event.event_slug,
                    event.buckets.len(),
                    event.detected_at_ns,
                    event.source,
                );

                // ==== PHASE 3 FAN-OUT ====
                // Forward every event (including BootstrapReplay) to EdgeBook
                // so the NO-edge farmer can price every bucket. Phase 2 mint
                // below is gated to skip BootstrapReplay-tagged events — the
                // mint window is already gone for anything old enough to
                // appear in the Gamma snapshot.
                if let Some(tx) = edge_cmd_tx.as_ref() {
                    if tx.send(EdgeCmd::RegisterEvent(event.clone())).is_err() {
                        tracing::warn!(
                            "[MAIN] edge_book channel closed — dropping RegisterEvent for {}",
                            event.event_slug
                        );
                    }
                }

                // BootstrapReplay events skip the Phase 2 mint-and-dump path
                // entirely. Their mint window is already gone by the time
                // they surface in the active-events snapshot.
                if matches!(event.source, DiscoverySource::BootstrapReplay) {
                    tracing::debug!(
                        "[MAIN] skipping Phase 2 mint for BootstrapReplay event {}",
                        event.event_slug
                    );
                    continue;
                }

                // ==== PAPER MODE SHORT-CIRCUIT ====
                // When paper_mode is on, the PaperEngine owns the whole
                // event lifecycle (bankroll check, virtual mint, real
                // orderbook walk, fee-adjusted P&L, CSV logging) in ONE
                // async call. No mint_exec / presigner involvement.
                if let Some(paper) = paper_engine.clone() {
                    let event_for_paper = event.clone();
                    tokio::spawn(async move {
                        let now_unix = chrono::Utc::now().timestamp();
                        if let Err(e) = paper.run_event(&event_for_paper, now_unix).await {
                            tracing::error!("[PAPER] {}: {}", event_for_paper.event_slug, e);
                        }
                    });
                    continue;
                }

                // ==== LIVE / SIMULATION (non-paper) PATH ====
                // 1) Daily cap check
                {
                    let mut s = state.lock().await;
                    s.check_and_reset_daily_cap();
                    if s.daily_minted_usdc + config.mint_amount_usdc > config.daily_cap_usdc {
                        tracing::warn!(
                            "[DAILY-CAP] {:.2} + {:.2} > {:.2} — skipping {}",
                            s.daily_minted_usdc,
                            config.mint_amount_usdc,
                            config.daily_cap_usdc,
                            event.event_slug
                        );
                        continue;
                    }
                }

                // 2) Pre-sign dump orders (hot path, fire-and-forget)
                let pre = presigner.clone();
                let event_for_pre = event.clone();
                tokio::spawn(async move {
                    pre.presign_for_event(&event_for_pre).await;
                });

                // 3) Mint (also fire-and-forget — the mint channel is watched separately)
                let mint = mint_exec.clone();
                let event_for_mint = event.clone();
                let state_cl = state.clone();
                let mint_tx_cl = mint_tx.clone();
                let amount = config.mint_amount_usdc;
                tokio::spawn(async move {
                    match mint.mint_event(&event_for_mint, amount).await {
                        Ok(receipts) => {
                            let mut s = state_cl.lock().await;
                            s.daily_minted_usdc += amount;
                            // Forward every receipt to the mint channel so the dump
                            // path flushes the corresponding presigned orders.
                            for r in receipts {
                                let _ = mint_tx_cl.send(r);
                            }
                        }
                        Err(e) => {
                            tracing::error!("[MINT] {}: {}", event_for_mint.event_slug, e);
                        }
                    }
                });
            }

            Some(receipt) = mint_rx.recv() => {
                tracing::info!(
                    "[MINT-RECEIPT] {} tx=0x{} (lag from detect: n/a)",
                    receipt.event_slug,
                    hex::encode(&receipt.tx_hash.as_slice()[..8]),
                );
                // TODO: pair receipt.event_slug back to the condition_ids for that
                // event so we can flush each bucket's presigned orders. For now,
                // flush by slug is a no-op until the presigner cache is keyed
                // on slug as well as condition_id.
            }

            Some(ready) = ready_rx.recv() => {
                let cid_hex = hex::encode(ready.condition_id.as_slice());
                let pre = presigner.clone();
                tokio::spawn(async move {
                    let n = pre.flush_condition(&cid_hex).await;
                    if n > 0 {
                        tracing::info!("[CLOB-READY] flushed {} orders for cid=0x{}", n, cid_hex);
                    }
                });
            }

            // ======== Phase 3 arms (active only when no_edge_farmer_enabled) ========
            Some(update) = async {
                match book_update_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if book_update_rx.is_some() => {
                if let Some(tx) = edge_cmd_tx.as_ref() {
                    let _ = tx.send(EdgeCmd::BookTick(update));
                }
            }

            Some(tick) = async {
                match forecast_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if forecast_rx.is_some() => {
                if let Some(tx) = edge_cmd_tx.as_ref() {
                    let _ = tx.send(EdgeCmd::ForecastTick(tick));
                }
            }

            Some(tick) = async {
                match nowcast_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if nowcast_rx.is_some() => {
                if let Some(tx) = edge_cmd_tx.as_ref() {
                    let _ = tx.send(EdgeCmd::NowcastTick(tick));
                }
            }

            Some(fill) = async {
                match fill_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if fill_rx.is_some() => {
                tracing::info!(
                    "[fill] token={} order={} side={:?} size={} price={} status={}",
                    fill.token_id, fill.order_id, fill.side, fill.size, fill.price, fill.status
                );
                if let Some(tx) = edge_cmd_tx.as_ref() {
                    let _ = tx.send(EdgeCmd::Fill {
                        token_id: fill.token_id,
                        filled_shares: fill.size,
                        filled_price: fill.price,
                    });
                }
                if let Some(pf) = portfolio_handle.as_ref() {
                    pf.record_fill(&fill.order_id, fill.size, fill.price, false);
                }
                if let Some(ref qtx) = quoter_cmd_tx {
                    let _ = qtx.send(quoter::QuoterCmd::Fill(fill));
                }
            }

            Some(os) = async {
                match order_state_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if order_state_rx.is_some() => {
                tracing::debug!(
                    "[order_state] token={} order={} kind={:?} status={} matched={}/{} @ {}",
                    os.token_id, os.order_id, os.kind, os.status,
                    os.size_matched, os.original_size, os.price
                );
                // Fan out cancel acknowledgements into the quoter so it can
                // transition `Cancelling` → `Idle` (or consume a `pending_next`
                // replacement). Placements / updates are just observational
                // for now — the quoter learns about placements via the
                // post_limit_order return value at signal-dispatch time.
                if matches!(
                    os.kind,
                    crate::watchers::clob_user::OrderStateKind::Cancellation
                ) && os.status == "CANCELED"
                {
                    if let Some(ref qtx) = quoter_cmd_tx {
                        let _ = qtx.send(quoter::QuoterCmd::CancelAck {
                            token_id: os.token_id,
                            order_id: os.order_id,
                        });
                    }
                }
            }

            Some(signal) = async {
                match edge_signal_rx.as_mut() { Some(rx) => rx.recv().await, None => None }
            }, if edge_signal_rx.is_some() => {
                if let Some(ref qtx) = quoter_cmd_tx {
                    let _ = qtx.send(quoter::QuoterCmd::Signal(signal));
                } else {
                    // No quoter wired (pure simulation mode) — log and drop.
                    tracing::debug!(
                        "[edge_signal] (no quoter) token={} target=${:.3}",
                        signal.token_id,
                        signal.target_ask,
                    );
                }
            }

            _ = signal::ctrl_c() => {
                tracing::info!("[SHUTDOWN] ctrl-c — saving state");
                state.lock().await.save();
                break;
            }
        }
    }

    Ok(())
}

/// All the channel/actor handles created by [`spawn_phase3`] and consumed by
/// the main `select!` loop. Bundled into one struct so the spawn function
/// stays readable and the destructuring in `main` is explicit about which
/// halves we own.
struct Phase3Handles {
    edge_cmd_tx: EdgeCmdSender,
    book_update_rx: mpsc::UnboundedReceiver<crate::types::BookUpdate>,
    forecast_rx: mpsc::UnboundedReceiver<crate::types::ForecastTick>,
    nowcast_rx: mpsc::UnboundedReceiver<crate::types::NowcastTick>,
    fill_rx: mpsc::UnboundedReceiver<crate::watchers::clob_user::FillEvent>,
    order_state_rx: mpsc::UnboundedReceiver<crate::watchers::clob_user::OrderStateEvent>,
    edge_signal_rx: mpsc::UnboundedReceiver<crate::edge_book::EdgeSignal>,
    portfolio_handle: PortfolioHandle,
    /// Sender half of the quoter command channel. `None` when we're running
    /// in pure simulation mode (no paper engine, no live executor wired as a
    /// sink) — the main select loop falls back to a debug-log drop for
    /// EdgeSignals in that case.
    quoter_cmd_tx: Option<QuoterCmdSender>,
    // Senders we must keep alive so downstream watchers don't see their
    // command channels close.
    _user_sub_tx: mpsc::UnboundedSender<crate::watchers::clob_user::UserSubCmd>,
    _sub_cmd_tx: crate::types::SubCmdSender,
}

/// Spin up every Phase 3 NO-edge farmer task and return the channel halves
/// the main loop needs to hold onto. Idempotent from `main`'s POV — called
/// exactly once behind the `no_edge_farmer_enabled` flag.
fn spawn_phase3(
    cfg: &Arc<Config>,
    executor: Arc<Executor>,
    paper_engine: Option<Arc<PaperEngine>>,
) -> anyhow::Result<Phase3Handles> {
    // --- Channels ---
    let (edge_cmd_tx, edge_cmd_rx) = mpsc::unbounded_channel::<EdgeCmd>();
    let (sub_cmd_tx, sub_cmd_rx) = mpsc::unbounded_channel::<crate::types::SubCmd>();
    let (book_update_tx, book_update_rx) = mpsc::unbounded_channel();
    let (forecast_tx, forecast_rx) = mpsc::unbounded_channel();
    let (nowcast_tx, nowcast_rx) = mpsc::unbounded_channel();
    let (fill_tx, fill_rx) = mpsc::unbounded_channel();
    let (order_state_tx, order_state_rx) = mpsc::unbounded_channel();
    let (edge_signal_tx, edge_signal_rx) = mpsc::unbounded_channel();
    let (user_sub_tx, user_sub_rx) =
        mpsc::unbounded_channel::<crate::watchers::clob_user::UserSubCmd>();
    let (quoter_cmd_tx, quoter_cmd_rx) = mpsc::unbounded_channel::<quoter::QuoterCmd>();

    // --- Portfolio actor ---
    let (portfolio_handle, _portfolio_task) = crate::portfolio::spawn_portfolio(cfg.as_ref())
        .map_err(|e| anyhow::anyhow!("portfolio spawn failed: {e:#}"))?;

    // --- EdgeBook actor ---
    let edge_cfg = Arc::clone(cfg);
    let edge_sub_tx = sub_cmd_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::edge_book::run_edge_book(
            edge_cfg.as_ref(),
            edge_cmd_rx,
            edge_sub_tx,
            edge_signal_tx,
        )
        .await
        {
            tracing::error!("[edge_book] fatal: {}", e);
        }
    });

    // --- Forecast watcher ---
    let fcfg = Arc::clone(cfg);
    tokio::spawn(async move {
        if let Err(e) =
            crate::watchers::forecast::run_forecast_watcher(fcfg.as_ref(), forecast_tx).await
        {
            tracing::error!("[forecast] fatal: {}", e);
        }
    });

    // --- METAR watcher ---
    let mcfg = Arc::clone(cfg);
    tokio::spawn(async move {
        if let Err(e) = crate::watchers::metar::run_metar_watcher(mcfg.as_ref(), nowcast_tx).await {
            tracing::error!("[metar] fatal: {}", e);
        }
    });

    // --- CLOB book watcher ---
    let ccfg = Arc::clone(cfg);
    tokio::spawn(async move {
        if let Err(e) = crate::watchers::clob_book::run_clob_book_watcher(
            ccfg.as_ref(),
            sub_cmd_rx,
            book_update_tx,
        )
        .await
        {
            tracing::error!("[clob_book] fatal: {}", e);
        }
    });

    // --- CLOB user watcher (live only — paper / sim has no real orders) ---
    if !cfg.paper_mode && !cfg.simulation {
        let creds = ClobUserCreds {
            api_key: std::env::var("POLY_API_KEY").unwrap_or_default(),
            secret: std::env::var("POLY_API_SECRET").unwrap_or_default(),
            passphrase: std::env::var("POLY_API_PASSPHRASE").unwrap_or_default(),
        };
        if !creds.api_key.is_empty() {
            let ucfg = Arc::clone(cfg);
            tokio::spawn(async move {
                if let Err(e) = crate::watchers::clob_user::run_clob_user_watcher(
                    ucfg.as_ref(),
                    creds,
                    Vec::new(),
                    user_sub_rx,
                    fill_tx,
                    order_state_tx,
                )
                .await
                {
                    tracing::error!("[clob_user] fatal: {}", e);
                }
            });
        } else {
            tracing::warn!(
                "[clob_user] no POLY_API_KEY in env — skipping user-channel subscription"
            );
        }
    } else {
        tracing::info!(
            "[clob_user] skipping user-channel subscription (paper_mode or simulation)"
        );
    }

    // --- Bootstrap replay (one-shot) ---
    // Runs in its own task so it doesn't block `main()`. In this v1 wiring
    // we pass an empty buffered-events vec — late-arriving live events will
    // still be forwarded through the normal `event_rx` path. A follow-up
    // ticket can add a side-buffer if we observe dropped events in practice.
    let bootstrap_cfg = Arc::clone(cfg);
    let bootstrap_executor = Arc::clone(&executor);
    let bootstrap_edge_tx = edge_cmd_tx.clone();
    tokio::spawn(async move {
        let mut sink = EdgeCmdEventSink::new(bootstrap_edge_tx);
        let known_orders: HashSet<String> = HashSet::new();
        match crate::no_edge::bootstrap::run_bootstrap(
            &bootstrap_cfg.gamma_api_url,
            bootstrap_executor.as_ref(),
            &mut sink,
            Vec::new(),
            &known_orders,
        )
        .await
        {
            Ok(report) => tracing::info!("[bootstrap] {:?}", report),
            Err(e) => tracing::error!("[bootstrap] fatal: {:#}", e),
        }
    });

    // --- Quoter actor ---
    //
    // Dispatch the sink concretely — `run_quoter` is generic over
    // `S: OrderSink + 'static`, so we monomorphise per mode and skip the
    // `dyn OrderSink` indirection entirely.
    //
    //   paper_mode=true                 → Arc<PaperEngine> sink
    //   simulation=true && !paper_mode  → no quoter spawned (log-only mode).
    //                                     We still return the sender so the
    //                                     main loop can hold it, but since
    //                                     no consumer exists EdgeSignal sends
    //                                     would pile up. We deliberately
    //                                     return `None` for that case so the
    //                                     main loop falls back to its debug
    //                                     log drop-path.
    //   live (both false)               → Arc<Executor> sink
    let quoter_cmd_tx_opt: Option<QuoterCmdSender> = if cfg.paper_mode {
        match paper_engine.clone() {
            Some(paper) => {
                let qcfg = Arc::clone(cfg);
                let qportfolio = portfolio_handle.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        crate::quoter::run_quoter(qcfg.as_ref(), paper, qportfolio, quoter_cmd_rx)
                            .await
                    {
                        tracing::error!("[quoter] fatal: {}", e);
                    }
                });
                Some(quoter_cmd_tx)
            }
            None => {
                tracing::warn!(
                    "[quoter] paper_mode=true but PaperEngine missing — quoter disabled"
                );
                None
            }
        }
    } else if cfg.simulation {
        tracing::warn!(
            "[quoter] simulation=true — skipping quoter spawn (EdgeSignals will be logged only)"
        );
        drop(quoter_cmd_rx);
        None
    } else {
        let qcfg = Arc::clone(cfg);
        let qportfolio = portfolio_handle.clone();
        let qexec = Arc::clone(&executor);
        tokio::spawn(async move {
            if let Err(e) =
                crate::quoter::run_quoter(qcfg.as_ref(), qexec, qportfolio, quoter_cmd_rx).await
            {
                tracing::error!("[quoter] fatal: {}", e);
            }
        });
        Some(quoter_cmd_tx)
    };

    Ok(Phase3Handles {
        edge_cmd_tx,
        book_update_rx,
        forecast_rx,
        nowcast_rx,
        fill_rx,
        order_state_rx,
        edge_signal_rx,
        portfolio_handle,
        quoter_cmd_tx: quoter_cmd_tx_opt,
        _user_sub_tx: user_sub_tx,
        _sub_cmd_tx: sub_cmd_tx,
    })
}

/// Derive the EOA address from a hex private key. Returns `None` if the key
/// is invalid or empty. Used only to seed the mempool watcher's filter.
fn derive_signer_address(private_key: &str) -> Option<Address> {
    use std::str::FromStr;
    if private_key.is_empty() {
        return None;
    }
    let signer = alloy_signer_local::PrivateKeySigner::from_str(private_key).ok()?;
    Some(alloy_signer::Signer::address(&signer))
}
