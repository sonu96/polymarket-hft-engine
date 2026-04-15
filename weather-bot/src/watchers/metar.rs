//! AviationWeather METAR nowcast watcher.
//!
//! Polls `https://aviationweather.gov/api/data/metar?ids=<icao>&format=json&hours=12`
//! every `no_edge_metar_poll_secs` seconds for each seeded ICAO, computes the
//! observed-TMAX-so-far in the station's local day, and emits a [`NowcastTick`]
//! that downstream `EdgeBook` uses to shrink σ via
//! `σ_remaining = σ_full × √remaining_var_frac`.
//!
//! See `docs/PHASE3_NO_EDGE_FARMER.md` §3.5 for the math.
//!
//! Per-ICAO staleness handling: if a station hasn't returned a successful
//! METAR in `no_edge_nowcast_staleness_kill_secs` seconds, the watcher emits
//! a "revert" tick with `remaining_var_frac = 1.0` so the EdgeBook drops back
//! to forecast-only σ for that one ICAO. Other ICAOs are unaffected.
//!
//! VILK (Lucknow) is NOT in the AviationWeather US-centric dataset and
//! `ids=VILK` returns an empty array. We log at DEBUG and skip — Lucknow
//! stays forecast-only until a separate INTL nowcast source is added.

use crate::climo;
use crate::config::Config;
use crate::types::{now_ns, NowcastTick, NowcastTickSender};
use chrono::{DateTime, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const AVIATIONWEATHER_METAR_URL: &str = "https://aviationweather.gov/api/data/metar";
const DEFAULT_USER_AGENT: &str = concat!(
    "polymarket-weather-bot/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/abhisonu/polymarket-weather-bot)",
);
/// Hours of METAR history to ask AviationWeather for. 12 covers the full
/// daylight window for any station after it crosses local noon.
const FETCH_HOURS: u32 = 12;
/// Inter-station sleep inside one poll cycle. Smooths the request rate so
/// we don't fire 10 requests in a tight burst at the top of every 5-min
/// tick — keeps us well under AviationWeather's per-IP throttle.
const INTER_STATION_SLEEP_MS: u64 = 750;

/// ICAO → IANA timezone name. Hardcoded mirror of the Python reference's
/// `STATION_TIMEZONES` map. Unknown ICAOs return `None` and are silently
/// skipped by the watcher.
static STATION_TIMEZONES: &[(&str, &str)] = &[
    ("KLGA", "America/New_York"),
    ("KATL", "America/New_York"),
    ("KSEA", "America/Los_Angeles"),
    ("KDAL", "America/Chicago"),
    ("KMIA", "America/New_York"),
    ("KORD", "America/Chicago"),
    ("KBKF", "America/Denver"),
    ("KSFO", "America/Los_Angeles"),
    ("KLAX", "America/Los_Angeles"),
    ("KHOU", "America/Chicago"),
    ("VILK", "Asia/Kolkata"),
];

pub fn timezone_for_icao(icao: &str) -> Option<&'static str> {
    let up = icao.to_ascii_uppercase();
    STATION_TIMEZONES
        .iter()
        .find(|(k, _)| *k == up.as_str())
        .map(|(_, v)| *v)
}

fn tz_for_icao(icao: &str) -> Option<Tz> {
    timezone_for_icao(icao).and_then(|name| name.parse::<Tz>().ok())
}

#[inline]
pub fn celsius_to_fahrenheit(c: f64) -> f64 {
    c * 9.0 / 5.0 + 32.0
}

// ---------- variance fraction lookup ----------

#[derive(Debug, Deserialize)]
struct VarianceFile {
    #[serde(default)]
    default: HashMap<String, f64>,
    // Future per-ICAO / per-month overrides land here. v1 ignores them.
}

fn variance_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("metar_variance.json")
}

static VARIANCE: OnceCell<VarianceFile> = OnceCell::new();

fn variance() -> &'static VarianceFile {
    VARIANCE.get_or_init(|| {
        let path = variance_path();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<VarianceFile>(&bytes) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "[metar] malformed metar_variance.json — falling back to all-1.0"
                    );
                    VarianceFile {
                        default: HashMap::new(),
                    }
                }
            },
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "[metar] missing metar_variance.json — falling back to all-1.0"
                );
                VarianceFile {
                    default: HashMap::new(),
                }
            }
        }
    })
}

/// Fraction of the day's TMAX variance still ahead at station-local hour `hour`.
///
/// v1 ignores `_icao` and `_month` and reads the `"default"` table — per-ICAO
/// and per-month refinement is ticket #4c. Always clamped to `[0.0, 1.0]`. Any
/// missing hour key returns `1.0` (full variance) so we never claim more
/// information than we have.
pub fn remaining_variance_fraction(_icao: &str, _month: u32, hour: u32) -> f64 {
    let v = variance();
    let key = hour.to_string();
    let raw = v.default.get(&key).copied().unwrap_or(1.0);
    raw.clamp(0.0, 1.0)
}

// ---------- AviationWeather METAR fetch + parse ----------

#[derive(Debug, Deserialize)]
struct MetarObservation {
    #[serde(default)]
    #[serde(rename = "icaoId")]
    _icao_id: Option<String>,
    /// Temperature in degrees Celsius. AviationWeather returns °C for every
    /// station regardless of region.
    #[serde(default)]
    temp: Option<f64>,
    /// Observation time as a UNIX epoch in **seconds**.
    #[serde(default, rename = "obsTime")]
    obs_time: Option<i64>,
}

/// Result of one METAR fetch: the observed max TMAX in the station's
/// **native** unit, the local hour 0..=23 of the latest in-window
/// observation, and the latest observation's UTC timestamp.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MetarSummary {
    observed_max: f64,
    hour_local: u32,
}

/// Pure parser: given an AviationWeather METAR JSON body, the target local
/// date, the station timezone and unit, return the max TMAX in native unit
/// and the local hour of the latest observation that landed on `target_date`.
///
/// Returns `None` when the body has no observations whose station-local date
/// matches `target_date` — common case for VILK and for any station before
/// its local day has started.
fn parse_metar_body(
    body: &str,
    target_date: NaiveDate,
    tz: Tz,
    unit: climo::Unit,
) -> anyhow::Result<Option<MetarSummary>> {
    let observations: Vec<MetarObservation> = serde_json::from_str(body)?;
    Ok(summarize_observations(&observations, target_date, tz, unit))
}

fn summarize_observations(
    observations: &[MetarObservation],
    target_date: NaiveDate,
    tz: Tz,
    unit: climo::Unit,
) -> Option<MetarSummary> {
    let mut max_native: Option<f64> = None;
    let mut latest_obs_ts: Option<i64> = None;
    let mut latest_local_hour: u32 = 0;

    for obs in observations {
        let Some(ts_secs) = obs.obs_time else { continue };
        let Some(temp_c) = obs.temp else { continue };

        let utc: DateTime<Utc> = match Utc.timestamp_opt(ts_secs, 0).single() {
            Some(dt) => dt,
            None => continue,
        };
        let local = utc.with_timezone(&tz);
        if local.date_naive() != target_date {
            continue;
        }

        let temp_native = match unit {
            climo::Unit::Fahrenheit => celsius_to_fahrenheit(temp_c),
            climo::Unit::Celsius => temp_c,
        };

        match max_native {
            Some(m) if temp_native <= m => {}
            _ => max_native = Some(temp_native),
        }
        match latest_obs_ts {
            Some(prev) if ts_secs <= prev => {}
            _ => {
                latest_obs_ts = Some(ts_secs);
                latest_local_hour = local.hour();
            }
        }
    }

    max_native.map(|observed_max| MetarSummary {
        observed_max,
        hour_local: latest_local_hour,
    })
}

async fn fetch_metar_summary(
    client: &reqwest::Client,
    icao: &str,
    target_date: NaiveDate,
    tz: Tz,
    unit: climo::Unit,
) -> anyhow::Result<Option<MetarSummary>> {
    let resp = client
        .get(AVIATIONWEATHER_METAR_URL)
        .query(&[
            ("ids", icao),
            ("format", "json"),
            ("hours", &FETCH_HOURS.to_string()),
        ])
        .header("User-Agent", DEFAULT_USER_AGENT)
        .header("Accept", "application/json")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("aviationweather returned HTTP {}", resp.status());
    }

    let body = resp.text().await?;
    parse_metar_body(&body, target_date, tz, unit)
}

// ---------- public entry point ----------

/// Run the METAR nowcast watcher forever. Polls every
/// `cfg.no_edge_metar_poll_secs` seconds, one ICAO at a time with a small
/// inter-station sleep, and emits a [`NowcastTick`] per successful fetch.
/// Per-ICAO staleness reverts emit a `remaining_var_frac = 1.0` tick.
pub async fn run_metar_watcher(
    cfg: &Config,
    tx: NowcastTickSender,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent(DEFAULT_USER_AGENT)
        .timeout(Duration::from_secs(10))
        .build()?;

    // Build the work list once: every seeded city whose ICAO has a known
    // timezone in the AviationWeather coverage map. VILK is included so we
    // log the empty-result path at DEBUG once per cycle (and emit a stale
    // revert tick if we never hear back).
    let stations: Vec<(&'static str, Tz, climo::Unit)> = climo::all_seeded_cities()
        .iter()
        .filter_map(|s| tz_for_icao(s.icao).map(|tz| (s.icao, tz, s.unit)))
        .collect();

    if stations.is_empty() {
        tracing::warn!("[metar] no seeded stations have a mapped IANA timezone — watcher idle");
        std::future::pending::<()>().await;
        return Ok(());
    }

    let poll_interval = Duration::from_secs(cfg.no_edge_metar_poll_secs.max(30));
    let staleness_kill = Duration::from_secs(cfg.no_edge_nowcast_staleness_kill_secs.max(60));
    let inter_station_sleep = Duration::from_millis(INTER_STATION_SLEEP_MS);

    tracing::info!(
        stations = stations.len(),
        poll_secs = poll_interval.as_secs(),
        staleness_secs = staleness_kill.as_secs(),
        "[metar] watcher started"
    );

    let mut last_ok: HashMap<&'static str, Instant> = HashMap::new();
    let mut last_emitted: HashMap<&'static str, MetarSummary> = HashMap::new();
    let mut staleness_warned: HashMap<&'static str, bool> = HashMap::new();

    loop {
        let cycle_start = Instant::now();
        let now_utc = Utc::now();

        for (icao, tz, unit) in &stations {
            // Resolve target_date as the station's *current* local date.
            let target_date = now_utc.with_timezone(tz).date_naive();
            let month = chrono::Datelike::month(&target_date);

            match fetch_metar_summary(&client, icao, target_date, *tz, *unit).await {
                Ok(Some(summary)) => {
                    last_ok.insert(*icao, Instant::now());
                    staleness_warned.insert(*icao, false);
                    last_emitted.insert(*icao, summary);

                    let frac = remaining_variance_fraction(icao, month, summary.hour_local);
                    let tick = NowcastTick {
                        icao,
                        date: target_date,
                        observed_max: summary.observed_max,
                        hour_local: summary.hour_local,
                        remaining_var_frac: frac,
                        fetched_at_ns: now_ns(),
                    };
                    tracing::debug!(
                        icao,
                        date = %target_date,
                        observed_max = summary.observed_max,
                        hour_local = summary.hour_local,
                        remaining_var_frac = frac,
                        "[metar] tick"
                    );
                    if tx.send(tick).is_err() {
                        tracing::warn!("[metar] receiver dropped — exiting watcher");
                        return Ok(());
                    }
                }
                Ok(None) => {
                    // Empty / no in-window observations — common for VILK
                    // and for any station whose local day hasn't kicked off
                    // yet. Do NOT update last_ok; let the staleness check
                    // below decide whether to emit a revert tick.
                    tracing::debug!(
                        icao,
                        date = %target_date,
                        "[metar] no in-window observations"
                    );
                }
                Err(e) => {
                    tracing::debug!(icao, error = %e, "[metar] fetch error");
                }
            }

            // Per-ICAO staleness check. Emit a single revert tick the first
            // time we cross the threshold, then keep retrying silently until
            // the next successful fetch resets the warned flag.
            let elapsed = last_ok
                .get(icao)
                .map(|t| t.elapsed())
                .unwrap_or(Duration::MAX);
            if elapsed >= staleness_kill && !*staleness_warned.get(icao).unwrap_or(&false) {
                let prev = last_emitted.get(icao).copied();
                let revert = NowcastTick {
                    icao,
                    date: now_utc.with_timezone(tz).date_naive(),
                    observed_max: prev.map(|s| s.observed_max).unwrap_or(f64::NEG_INFINITY),
                    hour_local: prev.map(|s| s.hour_local).unwrap_or(0),
                    remaining_var_frac: 1.0,
                    fetched_at_ns: now_ns(),
                };
                tracing::warn!(
                    icao,
                    elapsed_secs = elapsed.as_secs(),
                    "[metar] stale — reverting station to forecast-only σ"
                );
                staleness_warned.insert(*icao, true);
                if tx.send(revert).is_err() {
                    tracing::warn!("[metar] receiver dropped — exiting watcher");
                    return Ok(());
                }
            }

            tokio::time::sleep(inter_station_sleep).await;
        }

        let elapsed = cycle_start.elapsed();
        if elapsed < poll_interval {
            tokio::time::sleep(poll_interval - elapsed).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn celsius_to_fahrenheit_freezing() {
        assert!((celsius_to_fahrenheit(0.0) - 32.0).abs() < 1e-9);
    }

    #[test]
    fn celsius_to_fahrenheit_boiling() {
        assert!((celsius_to_fahrenheit(100.0) - 212.0).abs() < 1e-9);
    }

    #[test]
    fn timezone_for_icao_klga_is_eastern() {
        assert_eq!(timezone_for_icao("KLGA"), Some("America/New_York"));
    }

    #[test]
    fn timezone_for_icao_vilk_is_kolkata() {
        assert_eq!(timezone_for_icao("VILK"), Some("Asia/Kolkata"));
    }

    #[test]
    fn timezone_for_icao_lowercase_resolves() {
        assert_eq!(timezone_for_icao("klga"), Some("America/New_York"));
    }

    #[test]
    fn timezone_for_icao_unknown_returns_none() {
        assert!(timezone_for_icao("KFAKE").is_none());
    }

    #[test]
    fn variance_default_table_loads_known_hours() {
        assert!((remaining_variance_fraction("KLGA", 4, 5) - 0.98).abs() < 1e-9);
        assert!((remaining_variance_fraction("KLGA", 4, 15) - 0.06).abs() < 1e-9);
        assert!((remaining_variance_fraction("KLGA", 4, 22) - 0.02).abs() < 1e-9);
    }

    #[test]
    fn variance_clamped_and_safe_default() {
        // Hour 30 doesn't exist in the table → returns 1.0 (full variance).
        assert!((remaining_variance_fraction("KLGA", 4, 30) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parse_metar_body_klga_takes_max_in_local_day_and_converts_to_f() {
        // 2024-01-15 KLGA, local timezone America/New_York. Pick three obsTime
        // values that all land on 2024-01-15 NY-local:
        //   13:51 UTC = 08:51 EST   →  -2 °C  →  28.4 °F
        //   18:51 UTC = 13:51 EST   →   3 °C  →  37.4 °F  (max)
        //   22:51 UTC = 17:51 EST   →   1 °C  →  33.8 °F
        // Plus one observation OUTSIDE the local day to prove filtering works:
        //   2024-01-16 06:51 UTC = 2024-01-16 01:51 EST → 50 °C (must be ignored)
        let body = r#"[
            {"icaoId":"KLGA","temp":-2.0,"obsTime":1705326660},
            {"icaoId":"KLGA","temp":3.0, "obsTime":1705344660},
            {"icaoId":"KLGA","temp":1.0, "obsTime":1705359060},
            {"icaoId":"KLGA","temp":50.0,"obsTime":1705387860}
        ]"#;
        let target = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let tz: Tz = "America/New_York".parse().unwrap();
        let summary = parse_metar_body(body, target, tz, climo::Unit::Fahrenheit)
            .expect("parse ok")
            .expect("at least one in-window obs");
        // 3 °C → 37.4 °F
        assert!((summary.observed_max - 37.4).abs() < 1e-6, "got {}", summary.observed_max);
        // Latest in-window observation is 22:51 UTC = 17:51 EST → hour 17.
        assert_eq!(summary.hour_local, 17);
    }

    #[test]
    fn parse_metar_body_celsius_station_keeps_celsius() {
        // 2024-04-15 VILK, Asia/Kolkata. 09:00 UTC = 14:30 IST → 35 °C.
        // Lucknow stays in °C.
        let body = r#"[
            {"icaoId":"VILK","temp":35.0,"obsTime":1713171600}
        ]"#;
        let target = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
        let tz: Tz = "Asia/Kolkata".parse().unwrap();
        let summary = parse_metar_body(body, target, tz, climo::Unit::Celsius)
            .expect("parse ok")
            .expect("one in-window obs");
        assert!((summary.observed_max - 35.0).abs() < 1e-9);
        assert_eq!(summary.hour_local, 14);
    }

    #[test]
    fn parse_metar_body_empty_array_is_none() {
        let target = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
        let tz: Tz = "Asia/Kolkata".parse().unwrap();
        let summary = parse_metar_body("[]", target, tz, climo::Unit::Celsius)
            .expect("parse ok");
        assert!(summary.is_none(), "VILK empty payload must not panic");
    }

    #[test]
    fn parse_metar_body_filters_observations_outside_local_day() {
        // Only one observation, and it lands on 2024-01-14 NY-local
        // (2024-01-15 02:51 UTC = 2024-01-14 21:51 EST). Target is 01-15.
        let body = r#"[
            {"icaoId":"KLGA","temp":5.0,"obsTime":1705287060}
        ]"#;
        let target = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let tz: Tz = "America/New_York".parse().unwrap();
        let summary = parse_metar_body(body, target, tz, climo::Unit::Fahrenheit)
            .expect("parse ok");
        assert!(summary.is_none());
    }
}
