//! Phase 3 NO-edge farmer — Open-Meteo forecast watcher.
//!
//! Polls Open-Meteo on a 30-minute cadence for every seeded city and every
//! `days_ahead` in `0..=cfg.no_edge_lookahead_days`. For each `(city, date)`
//! pair we issue:
//!
//!   * one `/v1/forecast?models=…` call for μ (`temperature_2m_max`), with
//!     the model selected by [`pick_source`] — HRRR for ≤48h US horizons,
//!     ECMWF IFS025 otherwise (and always for international stations).
//!   * one `/v1/ensemble?models=ecmwf_ifs04,gfs_seamless,icon_seamless` call
//!     for σ via cross-model sample standard deviation. Per design doc §3.5
//!     σ is *always* from ensemble spread regardless of which source supplies
//!     μ — one provider, one calibration target, one knob.
//!
//! Failures are tolerated: any single (city, date) HTTP error logs a warning
//! and continues. Three consecutive ensemble failures for the same
//! (city, date, days_ahead) flips that triple to the hardcoded σ table until
//! the next success. The loop never exits on transient HTTP error.
//!
//! Native units only — °F for the 10 US stations, °C for VILK Lucknow.
//! Callers (pricer.rs) read the unit from `climo::station_for_city(...)`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use reqwest::Client;
use serde_json::Value;
use tracing::{debug, warn};

use crate::climo::{self, StationInfo, Unit};
use crate::config::Config;
use crate::types::{now_ns, ForecastSource, ForecastTick, ForecastTickSender, SigmaSource};

const OPEN_METEO_FORECAST_URL: &str = "https://api.open-meteo.com/v1/forecast";
const OPEN_METEO_ENSEMBLE_URL: &str = "https://ensemble-api.open-meteo.com/v1/ensemble";
const ENSEMBLE_MODELS: &str = "ecmwf_ifs04,gfs_seamless,icon_seamless";
const ENSEMBLE_FAILURE_THRESHOLD: u32 = 3;

/// Source cascade for μ. US stations (Fahrenheit) inside the HRRR horizon
/// take HRRR; everything else takes ECMWF IFS025. The boundary `days_ahead
/// <= 2` is tied to the design doc §3.5 phrase "≤ 48h" — `days_ahead = 2`
/// is the same calendar day as today + 48h in the worst case (UTC slop), and
/// the live default `no_edge_lookahead_days = 2` never reaches the > 2 arm.
/// The arm exists so a future operator who bumps the lookahead gets ECMWF
/// for the longer horizons without code changes.
pub fn pick_source(unit: Unit, days_ahead: i64) -> ForecastSource {
    match unit {
        Unit::Celsius => ForecastSource::EcmwfIfs025,
        Unit::Fahrenheit if days_ahead <= 2 => ForecastSource::GfsHrrr,
        Unit::Fahrenheit => ForecastSource::EcmwfIfs025,
    }
}

/// Hardcoded σ fallback table. Values come from design doc §3.5 / abstract
/// "σ=2°F 1-day, 3°F 2-day". Celsius values are the °F figures multiplied by
/// 5/9 and rounded to one decimal (2.0 → 1.1, 3.0 → 1.7).
pub fn hardcoded_sigma(unit: Unit, days_ahead: i64) -> f64 {
    match (unit, days_ahead) {
        (Unit::Fahrenheit, 0) => 2.0,
        (Unit::Fahrenheit, 1) => 2.0,
        (Unit::Fahrenheit, 2) => 3.0,
        (Unit::Fahrenheit, _) => 3.0,
        (Unit::Celsius, 0) => 1.1,
        (Unit::Celsius, 1) => 1.1,
        (Unit::Celsius, 2) => 1.7,
        (Unit::Celsius, _) => 1.7,
    }
}

#[inline]
pub fn apply_sigma_scale(sigma: f64, scale: f64) -> f64 {
    sigma * scale
}

/// Sample standard deviation across ensemble member values for a single
/// target date. Returns `None` when fewer than 3 members are usable —
/// per design doc §3.5 the fallback table fires under that floor.
pub fn ensemble_sigma_from_members(members: &[f64]) -> Option<f64> {
    if members.len() < 3 {
        return None;
    }
    let n = members.len() as f64;
    let mean = members.iter().sum::<f64>() / n;
    let var = members.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    Some(var.sqrt())
}

fn temperature_unit_param(unit: Unit) -> &'static str {
    match unit {
        Unit::Fahrenheit => "fahrenheit",
        Unit::Celsius => "celsius",
    }
}

fn source_models_param(source: ForecastSource) -> Option<&'static str> {
    match source {
        ForecastSource::GfsHrrr => Some("gfs_hrrr"),
        ForecastSource::EcmwfIfs025 => Some("ecmwf_ifs025"),
        ForecastSource::Seamless => None,
    }
}

/// Parse a `/v1/forecast` response JSON body and pull the first daily TMAX.
/// Tries the `models=` suffix variant first (e.g. `temperature_2m_max_gfs_hrrr`),
/// then plain `temperature_2m_max`, then any field starting with
/// `temperature_2m_max`. Returns `None` when nothing usable is present.
pub fn parse_forecast_tmax(body: &str, model_key: Option<&str>) -> Option<f64> {
    let v: Value = serde_json::from_str(body).ok()?;
    let daily = v.get("daily")?.as_object()?;

    if let Some(model) = model_key {
        let key = format!("temperature_2m_max_{model}");
        if let Some(arr) = daily.get(&key).and_then(|x| x.as_array()) {
            if let Some(first) = arr.first().and_then(|x| x.as_f64()) {
                return Some(first);
            }
        }
    }

    if let Some(arr) = daily.get("temperature_2m_max").and_then(|x| x.as_array()) {
        if let Some(first) = arr.first().and_then(|x| x.as_f64()) {
            return Some(first);
        }
    }

    for (k, val) in daily {
        if k.starts_with("temperature_2m_max") {
            if let Some(arr) = val.as_array() {
                if let Some(first) = arr.first().and_then(|x| x.as_f64()) {
                    return Some(first);
                }
            }
        }
    }
    None
}

/// Parse a `/v1/ensemble` response and collect the first hourly value of
/// `temperature_2m` across every member field. The ensemble endpoint exposes
/// per-member fields named `temperature_2m_member01`, `…_member02`, etc.
/// We treat the first hour as the representative draw for the sample stdev.
/// Returns the collected member values (possibly empty).
pub fn parse_ensemble_first_hour_members(body: &str) -> Vec<f64> {
    let mut out = Vec::new();
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return out;
    };
    let Some(hourly) = v.get("hourly").and_then(|x| x.as_object()) else {
        return out;
    };
    for (k, val) in hourly {
        if k.starts_with("temperature_2m") {
            if let Some(arr) = val.as_array() {
                if let Some(first) = arr.first().and_then(|x| x.as_f64()) {
                    out.push(first);
                }
            }
        }
    }
    out
}

#[derive(Debug, Default, Clone, Copy)]
struct EnsembleFailureState {
    consecutive: u32,
}

impl EnsembleFailureState {
    fn record_failure(&mut self) {
        self.consecutive = self.consecutive.saturating_add(1);
    }
    fn record_success(&mut self) {
        self.consecutive = 0;
    }
    fn fallback_active(&self) -> bool {
        self.consecutive >= ENSEMBLE_FAILURE_THRESHOLD
    }
}

async fn fetch_mu(
    client: &Client,
    station: &StationInfo,
    target: NaiveDate,
    days_ahead: i64,
) -> Option<(f64, ForecastSource)> {
    let preferred = pick_source(station.unit, days_ahead);
    if let Some(model) = source_models_param(preferred) {
        if let Some(v) = fetch_mu_with_model(client, station, target, Some(model)).await {
            return Some((v, preferred));
        }
    }
    // Fallback: omit `models=` so Open-Meteo serves its seamless default.
    fetch_mu_with_model(client, station, target, None)
        .await
        .map(|v| (v, ForecastSource::Seamless))
}

async fn fetch_mu_with_model(
    client: &Client,
    station: &StationInfo,
    target: NaiveDate,
    model: Option<&str>,
) -> Option<f64> {
    let date_str = target.format("%Y-%m-%d").to_string();
    let mut params: Vec<(&str, String)> = vec![
        ("latitude", format!("{:.4}", station.lat)),
        ("longitude", format!("{:.4}", station.lon)),
        ("daily", "temperature_2m_max".to_string()),
        ("temperature_unit", temperature_unit_param(station.unit).to_string()),
        ("timezone", "GMT".to_string()),
        ("start_date", date_str.clone()),
        ("end_date", date_str),
    ];
    if let Some(m) = model {
        params.push(("models", m.to_string()));
    }

    let resp = match client.get(OPEN_METEO_FORECAST_URL).query(&params).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(city = station.city, error = %e, "open-meteo /forecast request failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        warn!(
            city = station.city,
            status = %resp.status(),
            "open-meteo /forecast non-2xx"
        );
        return None;
    }
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            warn!(city = station.city, error = %e, "open-meteo /forecast body read failed");
            return None;
        }
    };
    parse_forecast_tmax(&body, model)
}

async fn fetch_sigma(
    client: &Client,
    station: &StationInfo,
    target: NaiveDate,
) -> Option<f64> {
    let date_str = target.format("%Y-%m-%d").to_string();
    let params = [
        ("latitude", format!("{:.4}", station.lat)),
        ("longitude", format!("{:.4}", station.lon)),
        ("hourly", "temperature_2m".to_string()),
        (
            "temperature_unit",
            temperature_unit_param(station.unit).to_string(),
        ),
        ("timezone", "GMT".to_string()),
        ("start_date", date_str.clone()),
        ("end_date", date_str),
        ("models", ENSEMBLE_MODELS.to_string()),
    ];

    let resp = match client.get(OPEN_METEO_ENSEMBLE_URL).query(&params).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(city = station.city, error = %e, "open-meteo /ensemble request failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        warn!(
            city = station.city,
            status = %resp.status(),
            "open-meteo /ensemble non-2xx"
        );
        return None;
    }
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            warn!(city = station.city, error = %e, "open-meteo /ensemble body read failed");
            return None;
        }
    };
    let members = parse_ensemble_first_hour_members(&body);
    ensemble_sigma_from_members(&members)
}

fn select_cities(cfg: &Config) -> Vec<&'static StationInfo> {
    let all = climo::all_seeded_cities();
    if cfg.no_edge_cities.is_empty() {
        return all.iter().collect();
    }
    let mut out: Vec<&'static StationInfo> = Vec::with_capacity(cfg.no_edge_cities.len());
    for slug in &cfg.no_edge_cities {
        match climo::station_for_city(slug) {
            Some(s) => out.push(s),
            None => warn!(slug = slug.as_str(), "no_edge_cities entry not in climo seeded set"),
        }
    }
    out
}

/// Long-running forecast watcher. Spawn from `no_edge::run` (later ticket)
/// via `tokio::spawn(run_forecast_watcher(cfg, tx))`. The function returns
/// `Result` only because tokio task handles want one — in practice the
/// `Ok(())` path is never reached, transient errors are swallowed by the
/// inner loop.
pub async fn run_forecast_watcher(cfg: &Config, tx: ForecastTickSender) -> Result<()> {
    let client = Client::builder()
        .user_agent("polymarket-weather-bot/0.1 (forecast-watcher)")
        .timeout(Duration::from_secs(15))
        .build()?;

    let stations = select_cities(cfg);
    if stations.is_empty() {
        warn!("forecast watcher: no cities selected — exiting");
        return Ok(());
    }
    let lookahead = cfg.no_edge_lookahead_days as i64;
    let sigma_scale = cfg.no_edge_sigma_scale;
    let poll_dur = Duration::from_secs(cfg.no_edge_forecast_poll_secs.max(1));

    let mut failures: HashMap<(&'static str, NaiveDate, i64), EnsembleFailureState> =
        HashMap::new();

    loop {
        let today = Utc::now().date_naive();
        for station in &stations {
            for days_ahead in 0..=lookahead {
                let target = today + ChronoDuration::days(days_ahead);

                let Some((mu, source_mu)) = fetch_mu(&client, station, target, days_ahead).await
                else {
                    warn!(
                        city = station.city,
                        date = %target,
                        days_ahead,
                        "skipping tick: no mu available from open-meteo"
                    );
                    continue;
                };

                let key = (station.city, target, days_ahead);
                let state = failures.entry(key).or_default();

                let (sigma_raw, source_sigma) = if state.fallback_active() {
                    (hardcoded_sigma(station.unit, days_ahead), SigmaSource::HardcodedFallback)
                } else {
                    match fetch_sigma(&client, station, target).await {
                        Some(s) => {
                            state.record_success();
                            (s, SigmaSource::EnsembleSpread)
                        }
                        None => {
                            state.record_failure();
                            (
                                hardcoded_sigma(station.unit, days_ahead),
                                SigmaSource::HardcodedFallback,
                            )
                        }
                    }
                };

                let sigma = apply_sigma_scale(sigma_raw, sigma_scale);

                let tick = ForecastTick {
                    city: station.city,
                    icao: station.icao,
                    date: target,
                    days_ahead,
                    mu,
                    sigma,
                    source_mu,
                    source_sigma,
                    fetched_at_ns: now_ns(),
                };

                debug!(
                    city = tick.city,
                    date = %tick.date,
                    days_ahead = tick.days_ahead,
                    mu = tick.mu,
                    sigma = tick.sigma,
                    source_mu = ?tick.source_mu,
                    source_sigma = ?tick.source_sigma,
                    "forecast tick"
                );

                if tx.send(tick).is_err() {
                    warn!("forecast watcher receiver dropped — exiting");
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(poll_dur).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_source_us_zero_days_is_hrrr() {
        assert_eq!(pick_source(Unit::Fahrenheit, 0), ForecastSource::GfsHrrr);
    }

    #[test]
    fn pick_source_us_one_day_is_hrrr() {
        assert_eq!(pick_source(Unit::Fahrenheit, 1), ForecastSource::GfsHrrr);
    }

    #[test]
    fn pick_source_us_two_days_is_hrrr() {
        // Boundary: ≤48h ⇒ days_ahead ≤ 2 inclusive. Default lookahead is
        // 2 so the > 2 arm exists only for operators who bump it.
        assert_eq!(pick_source(Unit::Fahrenheit, 2), ForecastSource::GfsHrrr);
    }

    #[test]
    fn pick_source_us_three_days_is_ecmwf() {
        assert_eq!(pick_source(Unit::Fahrenheit, 3), ForecastSource::EcmwfIfs025);
    }

    #[test]
    fn pick_source_intl_zero_days_is_ecmwf() {
        // Lucknow is always ECMWF regardless of horizon — ECMWF is the
        // consistent global model for non-CONUS stations.
        assert_eq!(pick_source(Unit::Celsius, 0), ForecastSource::EcmwfIfs025);
    }

    #[test]
    fn pick_source_intl_three_days_is_ecmwf() {
        assert_eq!(pick_source(Unit::Celsius, 3), ForecastSource::EcmwfIfs025);
    }

    #[test]
    fn hardcoded_sigma_fahrenheit_one_day_is_two() {
        assert_eq!(hardcoded_sigma(Unit::Fahrenheit, 1), 2.0);
    }

    #[test]
    fn hardcoded_sigma_fahrenheit_two_days_is_three() {
        assert_eq!(hardcoded_sigma(Unit::Fahrenheit, 2), 3.0);
    }

    #[test]
    fn hardcoded_sigma_celsius_one_day_is_one_point_one() {
        assert_eq!(hardcoded_sigma(Unit::Celsius, 1), 1.1);
    }

    #[test]
    fn hardcoded_sigma_celsius_two_days_is_one_point_seven() {
        assert_eq!(hardcoded_sigma(Unit::Celsius, 2), 1.7);
    }

    #[test]
    fn ensemble_sigma_five_members_matches_sample_stdev() {
        // Hand calc for [85.0, 87.0, 86.0, 84.0, 86.5]:
        //   mean = 85.7
        //   sumsq = 0.49 + 1.69 + 0.09 + 2.89 + 0.64 = 5.80
        //   var  = 5.80 / 4 = 1.45
        //   stdev= sqrt(1.45) ≈ 1.2042
        // Spec says "≈ 1.14 to within 0.01" — that figure looks like a
        // population stdev or a typo in the ticket. Both are within 0.07
        // of each other; we assert against the *sample* stdev that the
        // function actually computes (1.20) and let the spec deviation
        // ride in the report.
        let s = ensemble_sigma_from_members(&[85.0, 87.0, 86.0, 84.0, 86.5]).unwrap();
        assert!((s - 1.2042).abs() < 0.01, "got {s}");
    }

    #[test]
    fn ensemble_sigma_single_member_returns_none() {
        assert!(ensemble_sigma_from_members(&[85.0]).is_none());
    }

    #[test]
    fn ensemble_sigma_two_members_returns_none() {
        assert!(ensemble_sigma_from_members(&[85.0, 86.0]).is_none());
    }

    #[test]
    fn apply_sigma_scale_simple() {
        assert!((apply_sigma_scale(2.0, 1.25) - 2.5).abs() < 1e-9);
    }

    #[test]
    fn parse_forecast_tmax_with_model_suffix() {
        const BODY: &str = r#"{
            "latitude": 40.77,
            "longitude": -73.87,
            "daily_units": {"temperature_2m_max_gfs_hrrr": "°F"},
            "daily": {
                "time": ["2026-04-15"],
                "temperature_2m_max_gfs_hrrr": [78.4]
            }
        }"#;
        let v = parse_forecast_tmax(BODY, Some("gfs_hrrr")).unwrap();
        assert!((v - 78.4).abs() < 1e-9);
    }

    #[test]
    fn parse_forecast_tmax_plain_field_fallback() {
        const BODY: &str = r#"{
            "daily": {
                "time": ["2026-04-15"],
                "temperature_2m_max": [62.1]
            }
        }"#;
        let v = parse_forecast_tmax(BODY, None).unwrap();
        assert!((v - 62.1).abs() < 1e-9);
    }

    #[test]
    fn parse_forecast_tmax_missing_returns_none() {
        const BODY: &str = r#"{"daily": {"time": ["2026-04-15"]}}"#;
        assert!(parse_forecast_tmax(BODY, Some("gfs_hrrr")).is_none());
    }

    #[test]
    fn parse_ensemble_first_hour_members_collects_three() {
        const BODY: &str = r#"{
            "hourly": {
                "time": ["2026-04-15T00:00"],
                "temperature_2m_member01": [85.0],
                "temperature_2m_member02": [87.0],
                "temperature_2m_member03": [86.0]
            }
        }"#;
        let mut members = parse_ensemble_first_hour_members(BODY);
        members.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(members.len(), 3);
        assert!((members[0] - 85.0).abs() < 1e-9);
        assert!((members[2] - 87.0).abs() < 1e-9);
    }
}
