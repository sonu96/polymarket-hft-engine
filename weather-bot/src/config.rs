use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // -------- Polymarket endpoints --------
    pub clob_api_url: String,
    pub gamma_api_url: String,
    /// `wss://ws-subscriptions-clob.polymarket.com/ws/market`
    pub clob_ws_market_url: String,
    /// `wss://ws-subscriptions-clob.polymarket.com/ws/user`
    pub clob_ws_user_url: String,
    /// A stable token ID to piggyback `custom_feature_enabled: true` on, so we
    /// receive the globally-broadcast `new_market` messages. Any long-running
    /// active token works.
    pub clob_anchor_asset_id: String,

    // -------- Polygon on-chain --------
    /// Polygon WSS URL for `eth_subscribe("logs", ...)` and `newPendingTransactions`.
    /// Default is the public `wss://polygon-rpc.com` — fine for sim/dev, switch
    /// to Alchemy/QuickNode for live HFT.
    pub polygon_wss_url: String,
    /// HTTP RPCs for parallel tx broadcast. Hitting >1 endpoint improves
    /// propagation to block producers.
    pub polygon_rpc_urls: Vec<String>,
    /// Hex private key, NO `0x` prefix. Loaded from `PRIVATE_KEY` env var.
    pub private_key: String,
    /// Proxy wallet address for Gnosis Safe signature type (optional).
    pub funder_address: Option<String>,

    // -------- HFT strategy params --------
    /// USDC per mint. Default 10.0 (user's calibrated size).
    pub mint_amount_usdc: f64,
    /// Skip dumping any leg whose estimated bid is below this.
    pub min_dump_price: f64,
    /// Fraction of minted shares to dump (0.95 = keep 5% as tail lottery).
    pub dump_fraction: f64,
    /// Daily safety rail — total USDC minted per 24h.
    pub daily_cap_usdc: f64,
    /// Priority-fee multiplier over base fee. 3.0 is aggressive.
    pub priority_fee_multiplier: f64,
    /// Mint transaction gas limit.
    pub tx_gas_limit: u64,

    // -------- Mode + telemetry --------
    pub simulation: bool,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,

    // -------- Paper-trading mode --------
    /// When true, the bot runs through the PaperEngine instead of the
    /// live mint executor: real orderbooks, real fees, virtual bankroll.
    /// Can only be true if `simulation == true` (paper implies sim).
    pub paper_mode: bool,
    /// Virtual USDC bankroll to start the paper run with.
    pub paper_starting_bankroll: f64,
    /// Max simultaneously-open events — paper run skips new mints once full.
    pub paper_max_concurrent_events: usize,
    /// Where to append the structured CSV log.
    pub paper_log_path: String,

    // -------- Phase 3 NO-edge farmer (watchers) --------
    /// METAR poll cadence per seeded ICAO, in seconds. Default 300 (5 min).
    /// At 5 min and 10 US stations that's 120 req/hr — under the 132 req/hr
    /// AviationWeather budget called out in the design doc.
    pub no_edge_metar_poll_secs: u64,
    /// Per-ICAO METAR staleness threshold in seconds. After this much time
    /// without a successful fetch for a given ICAO, the watcher emits a
    /// "revert to forecast-only σ" tick (`remaining_var_frac = 1.0`) for
    /// that station and keeps trying. Default 900 (15 min = 3 missed cycles).
    pub no_edge_nowcast_staleness_kill_secs: u64,
    /// Open-Meteo forecast poll cadence (seconds). Default 1800 = 30 min.
    pub no_edge_forecast_poll_secs: u64,
    /// How many days into the future to fetch forecasts for, inclusive of
    /// today (`days_ahead = 0..=lookahead`). Default 2.
    pub no_edge_lookahead_days: u32,
    /// Multiplier applied to the ensemble σ before emitting a `ForecastTick`.
    /// Default 1.0 — bumped after 14 days of paper-mode calibration if
    /// ensemble proves under-dispersive at tails (typical correction
    /// 1.15–1.35 per design doc §3.5).
    pub no_edge_sigma_scale: f64,
    /// City slugs to poll. Empty (the default) means "every seeded city".
    pub no_edge_cities: Vec<String>,

    // -------- Phase 3 NO-edge farmer (portfolio caps) --------
    // These are deliberately separate from `daily_cap_usdc` (Phase 2's mint
    // throttle, recycled per mint). The farmer tracks deployed notional that
    // is locked until settlement — a different unit. See
    // `docs/PHASE3_NO_EDGE_FARMER.md` §4.2.
    /// Per-bucket (single market) deployed-notional cap.
    pub no_edge_max_notional_per_market_usdc: f64,
    /// Per (city × date) event deployed-notional cap.
    pub no_edge_max_notional_per_event_usdc: f64,
    /// Per-city daily deployed-notional cap.
    pub no_edge_max_notional_per_city_usdc: f64,
    /// Global deployed-notional cap across all NO-edge orders.
    pub no_edge_max_total_deployed_usdc: f64,
    /// Hard ceiling on simultaneously-open NO-edge orders.
    pub no_edge_max_open_orders: usize,
    /// Path to the persisted `NoEdgeState` JSON file.
    pub no_edge_state_path: String,

    // -------- Phase 3 NO-edge farmer (EdgeBook quote policy) --------
    /// Minimum per-share edge (basis points) the EdgeBook requires before it
    /// will emit a signal. 500 bps = 5.00% under the fair NO probability.
    pub no_edge_min_edge_bps: u32,
    /// Minimum |Δprice| in cents between the last emitted signal and the new
    /// target before EdgeBook bothers to re-emit. Prevents chattering reposts
    /// on sub-cent book jitter. Default 2¢.
    pub no_edge_repost_threshold_cents: u32,
    /// Minimum seconds between successive EdgeBook signals for the same token.
    /// Rate-limits the quoter / executor downstream. Default 5s.
    pub no_edge_repost_cooldown_secs: u64,

    /// Master switch for the Phase 3 NO-edge farmer pipeline. When `false`
    /// (the default), `main.rs` runs the Phase 2 mint/dump loop exactly as
    /// before and none of the Phase 3 actors (portfolio, edge_book,
    /// forecast / metar / clob_book / clob_user watchers, bootstrap replay)
    /// are spawned. Set `NO_EDGE_FARMER_ENABLED=1` in the environment to
    /// opt in.
    pub no_edge_farmer_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clob_api_url: "https://clob.polymarket.com".to_string(),
            gamma_api_url: "https://gamma-api.polymarket.com".to_string(),
            clob_ws_market_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
                .to_string(),
            clob_ws_user_url: "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string(),
            clob_anchor_asset_id: String::new(),

            polygon_wss_url: "wss://polygon-rpc.com".to_string(),
            polygon_rpc_urls: vec!["https://polygon-rpc.com".to_string()],
            private_key: String::new(),
            funder_address: None,

            mint_amount_usdc: 10.0,
            min_dump_price: 0.08,
            dump_fraction: 0.95,
            daily_cap_usdc: 200.0,
            priority_fee_multiplier: 3.0,
            tx_gas_limit: 500_000,

            simulation: true,
            telegram_bot_token: None,
            telegram_chat_id: None,

            paper_mode: true,
            paper_starting_bankroll: 1000.0,
            paper_max_concurrent_events: 8,
            paper_log_path: "paper_run.log".to_string(),

            no_edge_metar_poll_secs: 300,
            no_edge_nowcast_staleness_kill_secs: 900,
            no_edge_forecast_poll_secs: 1800,
            no_edge_lookahead_days: 2,
            no_edge_sigma_scale: 1.0,
            no_edge_cities: Vec::new(),
            no_edge_max_notional_per_market_usdc: 150.0,
            no_edge_max_notional_per_event_usdc: 500.0,
            no_edge_max_notional_per_city_usdc: 800.0,
            no_edge_max_total_deployed_usdc: 2000.0,
            no_edge_max_open_orders: 60,
            no_edge_state_path: "weather-bot/no_edge_state.json".to_string(),

            no_edge_min_edge_bps: 500,
            no_edge_repost_threshold_cents: 2,
            no_edge_repost_cooldown_secs: 5,

            no_edge_farmer_enabled: false,
        }
    }
}

impl Config {
    pub fn load() -> Self {
        dotenv::dotenv().ok();

        let mut cfg = if Path::new("config.json").exists() {
            fs::read_to_string("config.json")
                .ok()
                .and_then(|d| serde_json::from_str(&d).ok())
                .unwrap_or_default()
        } else {
            Self::default()
        };

        // --- required creds ---
        if let Ok(key) = std::env::var("PRIVATE_KEY") {
            cfg.private_key = key.trim_start_matches("0x").to_string();
        }
        if let Ok(funder) = std::env::var("FUNDER_ADDRESS") {
            cfg.funder_address = Some(funder);
        }

        // --- on-chain endpoints ---
        if let Ok(wss) = std::env::var("POLYGON_WSS_URL") {
            cfg.polygon_wss_url = wss;
        }
        if let Ok(rpcs) = std::env::var("POLYGON_RPC_URLS") {
            cfg.polygon_rpc_urls = rpcs.split(',').map(|s| s.trim().to_string()).collect();
        } else if let Ok(rpc) = std::env::var("POLYGON_RPC_URL") {
            cfg.polygon_rpc_urls = vec![rpc];
        }

        // --- CLOB WS anchor ---
        if let Ok(anchor) = std::env::var("CLOB_ANCHOR_ASSET_ID") {
            cfg.clob_anchor_asset_id = anchor;
        }

        // --- strategy knobs ---
        if let Ok(v) = std::env::var("MINT_AMOUNT_USDC") {
            if let Ok(f) = v.parse() {
                cfg.mint_amount_usdc = f;
            }
        }
        if let Ok(v) = std::env::var("MIN_DUMP_PRICE") {
            if let Ok(f) = v.parse() {
                cfg.min_dump_price = f;
            }
        }
        if let Ok(v) = std::env::var("DUMP_FRACTION") {
            if let Ok(f) = v.parse() {
                cfg.dump_fraction = f;
            }
        }
        if let Ok(v) = std::env::var("DAILY_CAP_USDC") {
            if let Ok(f) = v.parse() {
                cfg.daily_cap_usdc = f;
            }
        }

        // --- telemetry ---
        if let Ok(token) = std::env::var("TELEGRAM_BOT_TOKEN") {
            cfg.telegram_bot_token = Some(token);
        }
        if let Ok(chat) = std::env::var("TELEGRAM_CHAT_ID") {
            cfg.telegram_chat_id = Some(chat);
        }
        if let Ok(sim) = std::env::var("SIMULATION") {
            cfg.simulation = sim == "true" || sim == "1";
        }

        // --- paper mode ---
        if let Ok(v) = std::env::var("PAPER_MODE") {
            cfg.paper_mode = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("PAPER_STARTING_BANKROLL") {
            if let Ok(f) = v.parse() {
                cfg.paper_starting_bankroll = f;
            }
        }
        if let Ok(v) = std::env::var("PAPER_MAX_CONCURRENT_EVENTS") {
            if let Ok(n) = v.parse() {
                cfg.paper_max_concurrent_events = n;
            }
        }
        if let Ok(v) = std::env::var("PAPER_LOG_PATH") {
            cfg.paper_log_path = v;
        }

        // --- Phase 3 NO-edge farmer (watchers) ---
        if let Ok(v) = std::env::var("NO_EDGE_METAR_POLL_SECS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_metar_poll_secs = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_NOWCAST_STALENESS_KILL_SECS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_nowcast_staleness_kill_secs = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_FORECAST_POLL_SECS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_forecast_poll_secs = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_LOOKAHEAD_DAYS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_lookahead_days = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_SIGMA_SCALE") {
            if let Ok(f) = v.parse() {
                cfg.no_edge_sigma_scale = f;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_CITIES") {
            cfg.no_edge_cities = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // --- Phase 3 NO-edge farmer (portfolio caps) ---
        if let Ok(v) = std::env::var("NO_EDGE_MAX_NOTIONAL_PER_MARKET_USDC") {
            if let Ok(f) = v.parse() {
                cfg.no_edge_max_notional_per_market_usdc = f;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_MAX_NOTIONAL_PER_EVENT_USDC") {
            if let Ok(f) = v.parse() {
                cfg.no_edge_max_notional_per_event_usdc = f;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_MAX_NOTIONAL_PER_CITY_USDC") {
            if let Ok(f) = v.parse() {
                cfg.no_edge_max_notional_per_city_usdc = f;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_MAX_TOTAL_DEPLOYED_USDC") {
            if let Ok(f) = v.parse() {
                cfg.no_edge_max_total_deployed_usdc = f;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_MAX_OPEN_ORDERS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_max_open_orders = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_STATE_PATH") {
            cfg.no_edge_state_path = v;
        }

        // --- Phase 3 NO-edge farmer (EdgeBook quote policy) ---
        if let Ok(v) = std::env::var("NO_EDGE_MIN_EDGE_BPS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_min_edge_bps = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_REPOST_THRESHOLD_CENTS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_repost_threshold_cents = n;
            }
        }
        if let Ok(v) = std::env::var("NO_EDGE_REPOST_COOLDOWN_SECS") {
            if let Ok(n) = v.parse() {
                cfg.no_edge_repost_cooldown_secs = n;
            }
        }

        // --- Phase 3 NO-edge farmer (master switch) ---
        if let Ok(v) = std::env::var("NO_EDGE_FARMER_ENABLED") {
            cfg.no_edge_farmer_enabled = v == "true" || v == "1";
        }

        // Invariant: paper_mode forces simulation
        if cfg.paper_mode {
            cfg.simulation = true;
        }

        cfg
    }

    pub fn save(&self) {
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = fs::write("config.json", json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sim_mode_on() {
        let c = Config::default();
        assert!(c.simulation);
    }

    #[test]
    fn default_mint_size_is_ten() {
        assert_eq!(Config::default().mint_amount_usdc, 10.0);
    }

    #[test]
    fn default_daily_cap_is_two_hundred() {
        assert_eq!(Config::default().daily_cap_usdc, 200.0);
    }

    #[test]
    fn default_polygon_wss_is_public() {
        assert_eq!(Config::default().polygon_wss_url, "wss://polygon-rpc.com");
    }

    #[test]
    fn default_mint_to_dump_sanity() {
        let c = Config::default();
        assert!(c.dump_fraction > 0.0 && c.dump_fraction <= 1.0);
        assert!(c.min_dump_price > 0.0 && c.min_dump_price < 1.0);
    }

    #[test]
    fn default_no_edge_farmer_disabled() {
        // Phase 3 wiring is gated off by default so existing Phase 2 runs
        // are unaffected by ticket #12 landing on the base branch.
        assert!(!Config::default().no_edge_farmer_enabled);
    }

    #[test]
    fn no_edge_farmer_can_be_constructed_enabled() {
        let mut c = Config::default();
        c.no_edge_farmer_enabled = true;
        assert!(c.no_edge_farmer_enabled);
        // And the rest of the defaults still hold — flipping the flag is
        // not supposed to tamper with anything else.
        assert!(c.simulation);
        assert_eq!(c.mint_amount_usdc, 10.0);
    }
}
