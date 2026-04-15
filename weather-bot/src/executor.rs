use crate::config::Config;
use crate::state::BotState;
use alloy_primitives::U256;
use anyhow::{Context, Result};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

use alloy_signer_local::LocalSigner;
use alloy_signer::Signer;
use k256::ecdsa::SigningKey;
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::clob::Config as ClobConfig;
use polymarket_client_sdk::POLYGON;

/// Authenticated Polymarket CLOB executor. Shared between the pre-signer
/// (which builds orders) and the dump path (which POSTs them).
///
/// The buy-cheap-outcomes path that used to live here was deleted after the
/// IWantYourMoney deep-dive proved the $0.05 CLOB liquidity it targeted does
/// not exist. See memory/project_iwantyourmoney_wallet.md.
pub struct Executor {
    pub http_client: reqwest::Client,
    pub config: Config,
    pub clob_client: Option<Arc<ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>>,
    pub signer: Option<LocalSigner<SigningKey>>,
}

impl Executor {
    pub async fn new(config: Config) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_default();

        if config.simulation || config.private_key.is_empty() {
            tracing::info!("[EXEC] SIMULATION mode — no real orders");
            return Ok(Self {
                http_client,
                config,
                clob_client: None,
                signer: None,
            });
        }

        tracing::info!("[EXEC] Authenticating with Polymarket CLOB...");
        let signer = LocalSigner::from_str(&config.private_key)
            .context("Invalid private key")?
            .with_chain_id(Some(POLYGON));

        let client_builder = ClobClient::new(&config.clob_api_url, ClobConfig::default())?;
        let mut auth_builder = client_builder.authentication_builder(&signer);

        if let Some(ref funder) = config.funder_address {
            // Hex address → SDK Address type
            let bytes = hex::decode(funder.trim_start_matches("0x"))
                .context("Funder address must be 20-byte hex")?;
            anyhow::ensure!(bytes.len() == 20, "Funder address must be 20 bytes");
            let sdk_addr = polymarket_client_sdk::types::Address::from_slice(&bytes);
            auth_builder = auth_builder
                .funder(sdk_addr)
                .signature_type(polymarket_client_sdk::clob::types::SignatureType::GnosisSafe);
        }

        let clob_client = auth_builder
            .authenticate()
            .await
            .context("Failed to authenticate with Polymarket CLOB")?;

        tracing::info!("[EXEC] Authenticated. LIVE trading enabled.");

        Ok(Self {
            http_client,
            config,
            clob_client: Some(Arc::new(clob_client)),
            signer: Some(signer),
        })
    }

    /// Post a signed limit order and return the CLOB-assigned order ID.
    ///
    /// Used by both pipelines: Phase 2 mint-and-dump (fire-and-forget — discards
    /// the returned id) and Phase 3 no-edge farmer (tracks the id in the quoter
    /// state machine for later cancel/amend).
    pub async fn post_limit_order(
        &self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Side,
    ) -> Result<String> {
        let client = self
            .clob_client
            .as_ref()
            .context("CLOB client not initialized (simulation mode?)")?;
        let signer = self.signer.as_ref().context("Signer not initialized")?;

        let price_dec = Decimal::from_f64(price).context("Invalid price")?;
        let size_dec = Decimal::from_f64(size).context("Invalid size")?;

        tracing::info!(
            "[CLOB] POST {:?} {} @ ${} x {}",
            side,
            token_id,
            price_dec,
            size_dec
        );

        let order = client
            .limit_order()
            .token_id(token_id)
            .size(size_dec)
            .price(price_dec)
            .side(side)
            .build()
            .await
            .context("Failed to build order")?;

        let signed_order = client
            .sign(signer, order)
            .await
            .context("Failed to sign order")?;

        let response = client
            .post_order(signed_order)
            .await
            .context("Failed to submit order")?;

        if !response.success {
            anyhow::bail!(
                "CLOB rejected order: {}",
                response.error_msg.unwrap_or_else(|| "no error message".into())
            );
        }

        tracing::info!(
            "[CLOB] accepted id={} status={:?}",
            response.order_id,
            response.status
        );
        Ok(response.order_id)
    }

    /// Cancel a single resting order by its CLOB order ID. Used by the Phase 3
    /// quoter when it needs to amend a resting quote or drop out on a
    /// forecast-staleness kill-switch.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let client = self
            .clob_client
            .as_ref()
            .context("CLOB client not initialized (simulation mode?)")?;
        let response = client
            .cancel_order(order_id)
            .await
            .context("Failed to cancel order")?;
        tracing::info!("[CLOB] cancel {} → {:?}", order_id, response);
        Ok(())
    }

    /// List every open order owned by the authenticated funder. Paginated
    /// via the SDK's `next_cursor`. Called at startup to reconcile resting
    /// orders against the persisted `NoEdgeState` — adopt known ones, cancel
    /// orphans — before the first new post of the session.
    pub async fn list_open_orders(&self) -> Result<Vec<OpenOrderResponse>> {
        let client = self
            .clob_client
            .as_ref()
            .context("CLOB client not initialized (simulation mode?)")?;
        let req = OrdersRequest::builder().build();
        let mut collected: Vec<OpenOrderResponse> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = client
                .orders(&req, cursor.clone())
                .await
                .context("Failed to list open orders")?;
            collected.extend(page.data);
            if page.next_cursor.is_empty() || page.next_cursor == "LTE=" {
                break;
            }
            cursor = Some(page.next_cursor);
        }
        tracing::info!("[CLOB] list_open_orders → {} orders", collected.len());
        Ok(collected)
    }

    /// Stub for periodic position price refresh. Called by the status/alerts
    /// loop, not by the hot path. Walks state and updates each active position's
    /// `current_price` from the CLOB price endpoint.
    pub async fn refresh_prices(&self, state: &mut BotState) {
        for pos in state.positions.iter_mut() {
            match self.fetch_token_price(&pos.token_id).await {
                Ok(price) => pos.current_price = price,
                Err(e) => tracing::debug!("[PRICE] {} refresh failed: {}", pos.token_id, e),
            }
        }
    }

    async fn fetch_token_price(&self, token_id: &str) -> Result<f64> {
        let url = format!(
            "{}/price?token_id={}&side=sell",
            self.config.clob_api_url, token_id
        );
        let resp: serde_json::Value = self
            .http_client
            .get(&url)
            .header("User-Agent", "PolyWeatherBot/1.0")
            .send()
            .await?
            .json()
            .await?;
        resp.get("price")
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .context("Failed to parse price")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            simulation: true,
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn executor_simulation_mode_ok() {
        let executor = Executor::new(test_config()).await.unwrap();
        assert!(executor.clob_client.is_none());
        assert!(executor.signer.is_none());
    }
}
