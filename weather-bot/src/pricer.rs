//! Pure-function pricing kernel for the NO-edge farmer.
//!
//! Port of `weather_bot.wx_scoring.gaussian_bucket_prob` (Python source of
//! truth lives in the `daily-liquidity-bot` repo). The kernel is unit-agnostic:
//! the caller passes (mu, sigma) already in the bucket's units (°F for US
//! stations, °C for international stations).
//!
//! Buckets are integer-degree (Polymarket rounds to whole °F / °C for
//! resolution), so the bounds are inflated by ±0.5 to match how the round
//! maps a continuous TMAX onto an integer bucket. See
//! `docs/PHASE3_NO_EDGE_FARMER.md` §2.2 / §3.5.

use std::f64::consts::SQRT_2;

/// Direction of a one-sided tail bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketTail {
    Below,
    Above,
}

/// Neutral bucket representation used by the pricer.
///
/// The farmer's slug parser extracts integer lo/hi from the market slug
/// (e.g. `-84-85f` → `range(84, 85)`, `-79forbelow` → `below(79)`,
/// `-40c` → `range(40, 40)`), then hands the resulting `BucketSpec` to
/// `fair_p_no` together with the forecast (mu, sigma).
#[derive(Debug, Clone, Copy)]
pub struct BucketSpec {
    pub lo: Option<f64>,
    pub hi: Option<f64>,
    pub tail: Option<BucketTail>,
}

impl BucketSpec {
    pub fn range(lo: f64, hi: f64) -> Self {
        Self {
            lo: Some(lo),
            hi: Some(hi),
            tail: None,
        }
    }

    pub fn below(lo: f64) -> Self {
        Self {
            lo: Some(lo),
            hi: None,
            tail: Some(BucketTail::Below),
        }
    }

    pub fn above(lo: f64) -> Self {
        Self {
            lo: Some(lo),
            hi: None,
            tail: Some(BucketTail::Above),
        }
    }
}

/// Standard-normal CDF via `libm::erf`. Φ(z) = 0.5 * (1 + erf(z / √2)).
pub fn phi(z: f64) -> f64 {
    0.5 * (1.0 + libm::erf(z / SQRT_2))
}

/// P(daily TMAX falls in `bucket`) under N(mu, sigma²).
///
/// - Range bucket: `lo` and `hi` both `Some`, `tail` `None`
///     → P(lo ≤ round(TMAX) ≤ hi) = Φ((hi+0.5 − μ)/σ) − Φ((lo−0.5 − μ)/σ)
/// - Below tail: `lo` `Some`, `tail = Some(Below)`
///     → P(round(TMAX) ≤ lo) = Φ((lo+0.5 − μ)/σ)
/// - Above tail: `lo` `Some`, `tail = Some(Above)`
///     → P(round(TMAX) ≥ lo) = 1 − Φ((lo−0.5 − μ)/σ)
///
/// Returns `None` on degenerate inputs (sigma ≤ 0, non-finite mu/sigma,
/// hi < lo, missing bounds for the requested shape).
pub fn gaussian_bucket_prob(
    lo: Option<f64>,
    hi: Option<f64>,
    tail: Option<BucketTail>,
    mu: f64,
    sigma: f64,
) -> Option<f64> {
    if !sigma.is_finite() || sigma <= 0.0 {
        return None;
    }
    if !mu.is_finite() {
        return None;
    }

    let p = match tail {
        None => {
            let lo = lo?;
            let hi = hi?;
            if !lo.is_finite() || !hi.is_finite() || hi < lo {
                return None;
            }
            phi((hi + 0.5 - mu) / sigma) - phi((lo - 0.5 - mu) / sigma)
        }
        Some(BucketTail::Below) => {
            let lo = lo?;
            if !lo.is_finite() {
                return None;
            }
            phi((lo + 0.5 - mu) / sigma)
        }
        Some(BucketTail::Above) => {
            let lo = lo?;
            if !lo.is_finite() {
                return None;
            }
            1.0 - phi((lo - 0.5 - mu) / sigma)
        }
    };

    Some(p.clamp(0.0, 1.0))
}

/// Given a bucket and a (mu, sigma) forecast, return P(NO wins) = 1 − P(YES wins).
///
/// Returns `None` if the underlying `gaussian_bucket_prob` rejects the inputs.
pub fn fair_p_no(bucket: &BucketSpec, mu: f64, sigma: f64) -> Option<f64> {
    let p_yes = gaussian_bucket_prob(bucket.lo, bucket.hi, bucket.tail, mu, sigma)?;
    Some((1.0 - p_yes).clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOL: f64 = 5e-3;

    #[test]
    fn phi_zero_is_half() {
        assert!((phi(0.0) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn phi_z_score_checks() {
        assert!((phi(1.96) - 0.975).abs() < 1e-3);
        assert!((phi(-1.96) - 0.025).abs() < 1e-3);
        assert!((phi(1.0) - 0.8413447461).abs() < 1e-6);
        assert!((phi(-1.0) - 0.1586552539).abs() < 1e-6);
    }

    /// Bucket [85, 85] under N(85, 2): inflated bounds [84.5, 85.5],
    /// z = ±0.25 → Φ(0.25) − Φ(−0.25) ≈ 0.1974.
    #[test]
    fn range_single_degree_at_mean() {
        let p = gaussian_bucket_prob(Some(85.0), Some(85.0), None, 85.0, 2.0).unwrap();
        assert!((p - 0.197).abs() < TOL, "got {p}");
    }

    /// Bucket [80, 84] under N(82, 3): inflated bounds [79.5, 84.5],
    /// z = ±0.8333 → Φ(0.8333) − Φ(−0.8333) ≈ 0.5953.
    #[test]
    fn range_wide_bucket() {
        let p = gaussian_bucket_prob(Some(80.0), Some(84.0), None, 82.0, 3.0).unwrap();
        assert!((p - 0.595).abs() < TOL, "got {p}");
    }

    /// Below tail ≤ 40 under N(45, 3): inflated lo = 40.5,
    /// z = (40.5 − 45)/3 = −1.5 → Φ(−1.5) ≈ 0.0668.
    #[test]
    fn below_tail() {
        let p =
            gaussian_bucket_prob(Some(40.0), None, Some(BucketTail::Below), 45.0, 3.0).unwrap();
        assert!((p - 0.0668).abs() < TOL, "got {p}");
    }

    /// Above tail ≥ 47 under N(42, 2): inflated lo = 46.5,
    /// z = (46.5 − 42)/2 = 2.25 → 1 − Φ(2.25) ≈ 0.0122.
    #[test]
    fn above_tail() {
        let p =
            gaussian_bucket_prob(Some(47.0), None, Some(BucketTail::Above), 42.0, 2.0).unwrap();
        assert!((p - 0.01222).abs() < TOL, "got {p}");
    }

    /// Boundary check: bucket [40, 40] centered at μ = 40 σ = 2 → ~0.197.
    /// The whole point of the ±0.5 inflation is that this is NOT 0.
    #[test]
    fn boundary_single_degree_centered() {
        let p = gaussian_bucket_prob(Some(40.0), Some(40.0), None, 40.0, 2.0).unwrap();
        assert!((p - 0.197).abs() < TOL, "got {p}");
    }

    #[test]
    fn sigma_zero_returns_none() {
        assert!(
            gaussian_bucket_prob(Some(60.0), Some(70.0), None, 65.0, 0.0).is_none()
        );
    }

    #[test]
    fn sigma_negative_returns_none() {
        assert!(
            gaussian_bucket_prob(Some(60.0), Some(70.0), None, 65.0, -1.0).is_none()
        );
    }

    #[test]
    fn sigma_nan_returns_none() {
        assert!(
            gaussian_bucket_prob(Some(60.0), Some(70.0), None, 65.0, f64::NAN).is_none()
        );
    }

    #[test]
    fn mu_nan_returns_none() {
        assert!(
            gaussian_bucket_prob(Some(60.0), Some(70.0), None, f64::NAN, 5.0).is_none()
        );
    }

    #[test]
    fn inverted_range_returns_none() {
        assert!(
            gaussian_bucket_prob(Some(75.0), Some(65.0), None, 70.0, 5.0).is_none()
        );
    }

    #[test]
    fn missing_bound_returns_none() {
        assert!(gaussian_bucket_prob(Some(65.0), None, None, 70.0, 5.0).is_none());
        assert!(gaussian_bucket_prob(None, Some(75.0), None, 70.0, 5.0).is_none());
        assert!(
            gaussian_bucket_prob(None, None, Some(BucketTail::Below), 70.0, 5.0).is_none()
        );
        assert!(
            gaussian_bucket_prob(None, None, Some(BucketTail::Above), 70.0, 5.0).is_none()
        );
    }

    /// Far-out tail must clamp to 0.0 cleanly even if float drift would push
    /// it slightly negative.
    #[test]
    fn extreme_tail_clamps_to_zero() {
        let p = gaussian_bucket_prob(Some(1000.0), None, Some(BucketTail::Above), 70.0, 5.0)
            .unwrap();
        assert_eq!(p, 0.0);
    }

    /// Range bucket enclosing ±many sigma should clamp to 1.0.
    #[test]
    fn enclosing_range_clamps_to_one() {
        let p = gaussian_bucket_prob(Some(-1000.0), Some(1000.0), None, 70.0, 5.0).unwrap();
        assert_eq!(p, 1.0);
    }

    /// fair_p_no on an above-bucket far above the forecast: NO wins ~certainly.
    /// `above(60)` with N(42, 2) is ~9 sigma → P(YES) ≈ 0 → P(NO) ≈ 1.
    #[test]
    fn fair_p_no_above_far_out() {
        let p = fair_p_no(&BucketSpec::above(60.0), 42.0, 2.0).unwrap();
        assert!(p > 0.999, "got {p}");
    }

    /// fair_p_no on a range bucket that covers the mean: P(YES) ≈ 0.197,
    /// P(NO) ≈ 0.803.
    #[test]
    fn fair_p_no_range_at_mean() {
        let p = fair_p_no(&BucketSpec::range(85.0, 85.0), 85.0, 2.0).unwrap();
        assert!((p - 0.803).abs() < TOL, "got {p}");
    }

    /// fair_p_no propagates `None` from gaussian_bucket_prob.
    #[test]
    fn fair_p_no_propagates_none() {
        assert!(fair_p_no(&BucketSpec::range(85.0, 85.0), 85.0, 0.0).is_none());
    }

    /// Symmetry check: a below-tail and an above-tail at the same z-distance
    /// from the mean must give the same probability (modulo inflation
    /// sign).
    #[test]
    fn tail_symmetry_around_mean() {
        let lo = gaussian_bucket_prob(Some(60.0), None, Some(BucketTail::Below), 70.0, 5.0)
            .unwrap();
        let hi = gaussian_bucket_prob(Some(80.0), None, Some(BucketTail::Above), 70.0, 5.0)
            .unwrap();
        assert!((lo - hi).abs() < 1e-12, "below={lo} above={hi}");
    }
}
