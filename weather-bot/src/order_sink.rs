//! OrderSink trait — unified interface over the live Polymarket CLOB
//! `Executor` and the paper-trading `PaperEngine`. Lets the Phase 3 no-edge
//! farmer (ticket #12) swap between live and paper modes without duplicating
//! the post/cancel plumbing.
//!
//! The live impl drops `fair_p_no_at_post` / `min_edge_at_post` — they are
//! only relevant to `PaperEngine` which uses them for adverse-fill bookkeeping
//! when a resting order eventually crosses.

#![allow(dead_code)]

use alloy_primitives::U256;
use anyhow::Result;
use async_trait::async_trait;
use polymarket_client_sdk::clob::types::Side;

#[async_trait]
pub trait OrderSink: Send + Sync {
    /// Post a limit order. Returns the CLOB-assigned order_id.
    /// `fair_p_no_at_post` and `min_edge_at_post` are used by `PaperEngine`
    /// for adverse-fill detection; the live `Executor` impl ignores them.
    async fn post_limit_order(
        &self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Side,
        fair_p_no_at_post: f64,
        min_edge_at_post: f64,
    ) -> Result<String>;

    /// Cancel a resting order by order_id. Idempotent: unknown ids are a noop.
    async fn cancel_order(&self, order_id: &str) -> Result<()>;
}

// -------- Live impl: delegate to the real Executor --------

#[async_trait]
impl OrderSink for crate::executor::Executor {
    async fn post_limit_order(
        &self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Side,
        _fair_p_no_at_post: f64,
        _min_edge_at_post: f64,
    ) -> Result<String> {
        crate::executor::Executor::post_limit_order(self, token_id, price, size, side).await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        crate::executor::Executor::cancel_order(self, order_id).await
    }
}

// -------- Paper impl: record synthetic orders in PaperEngine --------

#[async_trait]
impl OrderSink for crate::paper::PaperEngine {
    async fn post_limit_order(
        &self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Side,
        fair_p_no_at_post: f64,
        min_edge_at_post: f64,
    ) -> Result<String> {
        self.paper_post_limit_order(
            token_id,
            price,
            size,
            side,
            fair_p_no_at_post,
            min_edge_at_post,
        )
        .await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        self.paper_cancel_order(order_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::paper::PaperEngine;

    // Compile-time check that both types implement OrderSink.
    #[allow(dead_code)]
    fn _check_impls() {
        fn _needs_sink<T: OrderSink>() {}
        _needs_sink::<Executor>();
        _needs_sink::<PaperEngine>();
    }

    #[test]
    fn order_sink_trait_compiles_for_executor() {
        // Presence of `_check_impls` above ensures the trait bounds are
        // satisfied at build time. This test exists so `cargo test` will
        // surface any regression as a named failure rather than a build error.
        let _ = _check_impls;
    }
}
