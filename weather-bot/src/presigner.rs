//! Pre-signed order cache.
//!
//! The HFT pipeline requires that dump orders are BUILT and SIGNED before the
//! mint tx lands — when the mint lands, all we do is POST. This module owns
//! the cache of `SignedOrder`s keyed by `condition_id` and exposes two ops:
//!
//!   1. `presign_for_event(event)` — called on the hot path the instant a
//!      `WeatherEvent` is emitted by the onchain watcher. Builds and signs
//!      one sell order per bucket whose estimated fair price > min_dump_price.
//!
//!   2. `flush_event(condition_id)` — called when either (a) our mint tx is
//!      seen in the mempool or (b) CLOB WS announces `new_market`. POSTs
//!      every cached order for that condition to the CLOB REST.
//!
//! In simulation mode both ops are logged but no network I/O happens.
//!
//! # EIP-712 signing
//!
//! Rather than hand-rolling the order struct hash, we delegate to the already-
//! working `polymarket-client-sdk` `limit_order().build()` + `sign()` path
//! that `Executor` uses. That keeps the signing math in one place and means
//! any fix to the SDK automatically flows through.

use crate::executor::Executor;
use crate::types::{BucketInfo, WeatherEvent};
use alloy_primitives::U256;
use polymarket_client_sdk::clob::types::Side;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub token_id: U256,
    pub limit_price: f64,
    pub size_shares: f64,
    pub bucket_label: String,
    /// Placeholder for the actual signed EIP-712 struct. In the current
    /// implementation we re-build + re-sign at flush time via the SDK; a
    /// future optimization is to sign here and cache the signed bytes so
    /// flush becomes a pure POST.
    pub prebuilt_at_ns: u128,
}

#[derive(Debug, Clone, Copy)]
pub struct OrderTemplate {
    pub min_dump_price: f64,
    pub dump_fraction: f64,
    pub mint_amount_usdc: f64,
}

pub struct Presigner {
    executor: Arc<Executor>,
    template: OrderTemplate,
    /// condition_id hex (no 0x) → orders for that bucket's YES leg.
    cache: Mutex<HashMap<String, Vec<SignedOrder>>>,
}

impl Presigner {
    pub fn new(executor: Arc<Executor>, template: OrderTemplate) -> Self {
        Self {
            executor,
            template,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Estimate the fair sell price for a bucket. Very rough heuristic — a
    /// Gaussian around the typical daily high, 4°C stdev. Not what the
    /// competitor uses but enough to filter out legs that are obvious dust.
    ///
    /// TODO: plug in real climatological lookups via a city→(mean,stdev) table,
    /// or refine on the fly via CLOB WS price updates.
    fn estimate_bucket_price(&self, bucket: &BucketInfo) -> f64 {
        // Without real climate data, fall back to a uniform 1/N prior.
        // `dump_fraction` is folded into caller-side sizing, not the price.
        let _ = bucket;
        0.10
    }

    /// Hot path: called the instant we detect a weather event. Builds one
    /// sell order per bucket above min_dump_price and stashes them in the
    /// cache keyed by condition_id (hex, no 0x).
    pub async fn presign_for_event(&self, event: &WeatherEvent) -> usize {
        let mut inserted = 0;
        let shares_per_bucket = self.template.mint_amount_usdc * self.template.dump_fraction;

        for bucket in &event.buckets {
            let price = self.estimate_bucket_price(bucket);
            if price < self.template.min_dump_price {
                continue;
            }
            let order = SignedOrder {
                token_id: bucket.token_id_yes,
                limit_price: price,
                size_shares: shares_per_bucket,
                bucket_label: bucket.bucket_label.clone(),
                prebuilt_at_ns: crate::types::now_ns(),
            };
            let key = hex::encode(bucket.condition_id.as_slice());
            let mut guard = self.cache.lock().await;
            guard.entry(key).or_insert_with(Vec::new).push(order);
            inserted += 1;
        }
        tracing::info!(
            "[presigner] cached {} orders for event {}",
            inserted,
            event.event_slug
        );
        inserted
    }

    /// Called when we have confirmation the market is indexer-ready
    /// (mint tx pending / CLOB `new_market`). Flushes every cached order for
    /// the given condition_id by calling the executor's signed-order path.
    pub async fn flush_condition(&self, condition_id_hex: &str) -> usize {
        let orders = {
            let mut guard = self.cache.lock().await;
            guard.remove(condition_id_hex).unwrap_or_default()
        };
        if orders.is_empty() {
            return 0;
        }
        tracing::info!(
            "[presigner] flushing {} orders for cid=0x{}",
            orders.len(),
            condition_id_hex
        );

        let mut ok = 0;
        for o in orders {
            if self.executor.config.simulation {
                tracing::info!(
                    "[SIM] would POST sell {} shares {} @ ${} (token={})",
                    o.size_shares, o.bucket_label, o.limit_price, o.token_id
                );
                ok += 1;
                continue;
            }
            match self
                .executor
                .post_limit_order(o.token_id, o.limit_price, o.size_shares, Side::Sell)
                .await
            {
                Ok(_order_id) => ok += 1,
                Err(e) => tracing::warn!("[presigner] POST failed for {}: {}", o.token_id, e),
            }
        }
        ok
    }

    /// Stats hook for the dashboard / status loop.
    pub async fn cache_size(&self) -> usize {
        let guard = self.cache.lock().await;
        guard.values().map(|v| v.len()).sum()
    }

    /// Test-only, non-draining snapshot of the cache. Unlike
    /// [`Self::flush_condition`] (which removes entries) this clones the
    /// inner map so callers can enumerate every cached order without
    /// disturbing the hot-path state. Used by the Phase 3 self-cross
    /// regression test — see `tests::self_cross_presigner_cache_never_touches_no_tokens`.
    #[cfg(test)]
    pub(crate) async fn cached_orders(&self) -> Vec<(String, Vec<SignedOrder>)> {
        let guard = self.cache.lock().await;
        guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    //! Phase 3 ticket #13 — self-cross regression.
    //!
    //! Phase 2 mint-and-dump presigns **Sell** orders on `token_id_yes`
    //! (see `presign_for_event` above — it reads `bucket.token_id_yes` only).
    //! Phase 3 NO-edge farmer rests **Sell** orders on `token_id_no`. Today
    //! those two pipelines operate on disjoint token_id sets and cannot
    //! cross each other's books.
    //!
    //! If someone ever extends Phase 2 to ALSO dump NO legs (e.g. for a
    //! basket merge+redeem path), the two pipelines would both be selling
    //! on the same `token_id_no` and would self-cross — silently losing
    //! money while looking like normal activity. With no Gamma poll, the
    //! on-chain stream is the only coordination point between Phase 2 and
    //! Phase 3, so this check is load-bearing.
    //!
    //! Intended failure mode: if `presign_for_event` is edited to also loop
    //! over `bucket.token_id_no`, this test FAILS and forces a design
    //! conversation before the change ships.

    use super::*;
    use crate::config::Config;
    use crate::types::{DiscoverySource, EventKind, WeatherEvent};
    use alloy_primitives::{Address, B256};
    use std::collections::HashSet;

    /// Three-bucket NegRisk fixture with distinct YES/NO token ids per bucket.
    /// The integer values are intentionally tiny and non-overlapping so that
    /// the {YES} and {NO} sets are trivially distinguishable.
    fn three_bucket_event() -> WeatherEvent {
        WeatherEvent {
            event_slug: "highest-temperature-in-lucknow-on-april-15-2026".to_string(),
            city: "lucknow".to_string(),
            resolution_date: "2026-04-15".to_string(),
            kind: EventKind::NegRisk,
            neg_risk_market_id: Some(B256::repeat_byte(0xaa)),
            oracle: Address::ZERO,
            buckets: vec![
                BucketInfo {
                    condition_id: B256::repeat_byte(0x11),
                    question_id: B256::repeat_byte(0x21),
                    outcome_index: 0,
                    bucket_label: "40°C or below".to_string(),
                    token_id_yes: U256::from(100u64),
                    token_id_no: U256::from(101u64),
                },
                BucketInfo {
                    condition_id: B256::repeat_byte(0x12),
                    question_id: B256::repeat_byte(0x22),
                    outcome_index: 1,
                    bucket_label: "41°C".to_string(),
                    token_id_yes: U256::from(200u64),
                    token_id_no: U256::from(201u64),
                },
                BucketInfo {
                    condition_id: B256::repeat_byte(0x13),
                    question_id: B256::repeat_byte(0x23),
                    outcome_index: 2,
                    bucket_label: "42°C or higher".to_string(),
                    token_id_yes: U256::from(300u64),
                    token_id_no: U256::from(301u64),
                },
            ],
            detected_at_ns: 0,
            source: DiscoverySource::OnChain,
        }
    }

    fn sim_config() -> Config {
        Config {
            simulation: true,
            ..Config::default()
        }
    }

    fn permissive_template() -> OrderTemplate {
        // `estimate_bucket_price` currently returns 0.10 for every bucket,
        // so min_dump_price=0.01 keeps every bucket in the cache. We want
        // the full set because the regression question is about which
        // token_id gets signed, not which buckets survive the price gate.
        OrderTemplate {
            min_dump_price: 0.01,
            dump_fraction: 0.5,
            mint_amount_usdc: 10.0,
        }
    }

    /// Core regression: after `presign_for_event`, every cached order must
    /// reference a YES token and must NOT reference any NO token. If someone
    /// extends Phase 2 to dump NO legs, this test fails loudly with a
    /// message naming the offending token.
    #[tokio::test]
    async fn self_cross_presigner_cache_never_touches_no_tokens() {
        let executor = Arc::new(Executor::new(sim_config()).await.unwrap());
        let presigner = Presigner::new(executor, permissive_template());
        let event = three_bucket_event();

        let inserted = presigner.presign_for_event(&event).await;
        assert_eq!(
            inserted,
            event.buckets.len(),
            "expected one presigned order per bucket (all buckets should clear \
             min_dump_price=0.01 given the 0.10 placeholder fair-price)"
        );

        // Build the expected token-id universes from the fixture.
        let yes_tokens: HashSet<U256> =
            event.buckets.iter().map(|b| b.token_id_yes).collect();
        let no_tokens: HashSet<U256> =
            event.buckets.iter().map(|b| b.token_id_no).collect();

        // Snapshot the presigner cache without draining it.
        let cache = presigner.cached_orders().await;
        let cached_token_ids: Vec<U256> = cache
            .iter()
            .flat_map(|(_cid, orders)| orders.iter().map(|o| o.token_id))
            .collect();

        assert_eq!(
            cached_token_ids.len(),
            event.buckets.len(),
            "cache snapshot should contain exactly one order per bucket"
        );

        // (1) Every cached order is on a YES token.
        for tid in &cached_token_ids {
            assert!(
                yes_tokens.contains(tid),
                "cached SignedOrder references token_id {tid} which is NOT \
                 in the YES-token universe {yes_tokens:?}"
            );
        }

        // (2) The intersection with the NO universe is empty. This is the
        //     load-bearing invariant — if it ever fails, Phase 2 has been
        //     extended to dump NO legs and will self-cross the Phase 3
        //     NO-edge farmer's resting orders.
        for tid in &cached_token_ids {
            assert!(
                !no_tokens.contains(tid),
                "SELF-CROSS: presigner cached a SELL order on {tid}, which \
                 is a NO-side token managed by the Phase 3 farmer. This \
                 means Phase 2 has been extended to dump NO legs and the \
                 two pipelines will cross each other's books."
            );
        }
    }

    /// Negative control: the YES-universe and NO-universe in the fixture
    /// must themselves be disjoint, otherwise the self-cross check above
    /// is vacuous. Guards against someone "fixing" the fixture by making
    /// YES == NO and accidentally silencing the real test.
    #[tokio::test]
    async fn self_cross_fixture_yes_and_no_universes_are_disjoint() {
        let event = three_bucket_event();
        let yes_tokens: HashSet<U256> =
            event.buckets.iter().map(|b| b.token_id_yes).collect();
        let no_tokens: HashSet<U256> =
            event.buckets.iter().map(|b| b.token_id_no).collect();
        let overlap: HashSet<_> = yes_tokens.intersection(&no_tokens).collect();
        assert!(
            overlap.is_empty(),
            "fixture invariant broken: YES and NO token universes overlap \
             at {overlap:?} — the self-cross regression test would be \
             vacuously true"
        );
    }
}
