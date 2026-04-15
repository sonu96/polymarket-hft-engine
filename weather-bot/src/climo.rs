//! Per-day-of-year TMAX climate normals + station map for the seeded
//! Polymarket weather cities.
//!
//! Ported from the Python reference implementation in
//! `daily-liquidity-bot/bots/daily-liquidity-bot/src/weather_bot/wx_climo.py`
//! plus its INTL counterpart `wx_climo_intl.py`. The station map mirrors
//! `wx_resolution_map.yaml` (10 US + 1 INTL "seeded" entries; the three
//! "shelved" cities are intentionally omitted — we do not trade them).
//!
//! Two public APIs:
//!   * [`station_for_city`] — city slug → station info (icao, lat, lon, unit)
//!   * [`daily_normal`]      — (icao, month, day) → (mean, std) in the station's native unit
//!
//! `daily_normal` lazily loads `data/climo/*.json` once on first call and
//! caches the parsed map for the lifetime of the process. Feb 29 falls
//! back to Feb 28 (NCEI normals omit leap day).

use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Fahrenheit,
    Celsius,
}

#[derive(Debug, Clone)]
pub struct StationInfo {
    pub city: &'static str,
    pub icao: &'static str,
    pub airport: &'static str,
    pub lat: f64,
    pub lon: f64,
    pub unit: Unit,
}

static SEEDED_STATIONS: &[StationInfo] = &[
    StationInfo {
        city: "nyc",
        icao: "KLGA",
        airport: "LaGuardia",
        lat: 40.7772,
        lon: -73.8726,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "atlanta",
        icao: "KATL",
        airport: "Hartsfield-Jackson",
        lat: 33.6407,
        lon: -84.4277,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "seattle",
        icao: "KSEA",
        airport: "Sea-Tac",
        lat: 47.4502,
        lon: -122.3088,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "dallas",
        icao: "KDAL",
        airport: "Love Field",
        lat: 32.8471,
        lon: -96.8518,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "miami",
        icao: "KMIA",
        airport: "Miami Intl",
        lat: 25.7932,
        lon: -80.2906,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "chicago",
        icao: "KORD",
        airport: "O'Hare",
        lat: 41.9786,
        lon: -87.9048,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "denver",
        icao: "KBKF",
        airport: "Buckley SFB",
        lat: 39.7017,
        lon: -104.7517,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "san-francisco",
        icao: "KSFO",
        airport: "SF Intl",
        lat: 37.6213,
        lon: -122.3790,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "los-angeles",
        icao: "KLAX",
        airport: "LAX",
        lat: 33.9416,
        lon: -118.4085,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "houston",
        icao: "KHOU",
        airport: "Hobby",
        lat: 29.6454,
        lon: -95.2789,
        unit: Unit::Fahrenheit,
    },
    StationInfo {
        city: "lucknow",
        icao: "VILK",
        airport: "Lucknow Amausi (Chaudhary Charan Singh Intl)",
        lat: 26.7606,
        lon: 80.8893,
        unit: Unit::Celsius,
    },
];

static CITY_INDEX: Lazy<HashMap<&'static str, &'static StationInfo>> = Lazy::new(|| {
    SEEDED_STATIONS
        .iter()
        .map(|s| (s.city, s))
        .collect()
});

pub fn station_for_city(city: &str) -> Option<&'static StationInfo> {
    CITY_INDEX.get(city).copied()
}

pub fn all_seeded_cities() -> &'static [StationInfo] {
    SEEDED_STATIONS
}

#[derive(Debug, Deserialize)]
struct DayEntry {
    #[serde(default)]
    tmax_mean_f: Option<f64>,
    #[serde(default)]
    tmax_std_f: Option<f64>,
    #[serde(default)]
    tmax_mean_c: Option<f64>,
    #[serde(default)]
    tmax_std_c: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StationFile {
    #[serde(default)]
    station_icao: Option<String>,
    days: HashMap<String, DayEntry>,
}

fn climo_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("climo")
}

static CLIMO: Lazy<HashMap<String, HashMap<String, (f64, f64)>>> = Lazy::new(load_climo);

fn load_climo() -> HashMap<String, HashMap<String, (f64, f64)>> {
    let mut out: HashMap<String, HashMap<String, (f64, f64)>> = HashMap::new();
    let dir = climo_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(it) => it,
        Err(e) => {
            warn!(path = %dir.display(), error = %e, "climo dir missing — daily_normal will return None");
            return out;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "skipping unreadable climo file");
                continue;
            }
        };
        let parsed: StationFile = match serde_json::from_slice(&bytes) {
            Ok(p) => p,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "skipping malformed climo json");
                continue;
            }
        };

        let icao = parsed
            .station_icao
            .clone()
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default()
            .to_uppercase();
        if icao.is_empty() {
            continue;
        }

        let mut days: HashMap<String, (f64, f64)> = HashMap::with_capacity(parsed.days.len());
        for (key, entry) in parsed.days {
            let mean_std = match (entry.tmax_mean_f, entry.tmax_std_f) {
                (Some(m), Some(s)) => Some((m, s)),
                _ => match (entry.tmax_mean_c, entry.tmax_std_c) {
                    (Some(m), Some(s)) => Some((m, s)),
                    _ => None,
                },
            };
            if let Some(ms) = mean_std {
                days.insert(key, ms);
            }
        }
        out.insert(icao, days);
    }
    out
}

/// Returns `(tmax_mean, tmax_std)` for the given station/date in the
/// station's native unit (no °F↔°C conversion). Feb 29 falls back to
/// Feb 28. Returns `None` when the station is unknown or the day is
/// missing after the leap-day fallback.
pub fn daily_normal(icao: &str, month: u32, day: u32) -> Option<(f64, f64)> {
    let icao_u = icao.to_uppercase();
    let station = CLIMO.get(&icao_u)?;
    let key = format!("{:02}-{:02}", month, day);
    if let Some(v) = station.get(&key) {
        return Some(*v);
    }
    if month == 2 && day == 29 {
        return station.get("02-28").copied();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn station_for_city_nyc_returns_klga() {
        let s = station_for_city("nyc").expect("nyc seeded");
        assert_eq!(s.icao, "KLGA");
        assert_eq!(s.unit, Unit::Fahrenheit);
    }

    #[test]
    fn station_for_city_san_francisco_returns_ksfo() {
        let s = station_for_city("san-francisco").expect("sf seeded");
        assert_eq!(s.icao, "KSFO");
    }

    #[test]
    fn station_for_city_lucknow_returns_vilk_celsius() {
        let s = station_for_city("lucknow").expect("lucknow seeded");
        assert_eq!(s.icao, "VILK");
        assert_eq!(s.unit, Unit::Celsius);
    }

    #[test]
    fn station_for_city_unknown_returns_none() {
        assert!(station_for_city("bogus").is_none());
    }

    #[test]
    fn all_seeded_cities_has_eleven_entries() {
        assert_eq!(all_seeded_cities().len(), 11);
    }

    #[test]
    fn daily_normal_klga_july_4_in_summer_range() {
        let (mu, sigma) = daily_normal("KLGA", 7, 4).expect("KLGA Jul 4 present");
        assert!(
            (78.0..=92.0).contains(&mu),
            "NYC Jul 4 mean expected 78-92F, got {mu}"
        );
        assert!(
            (2.0..=10.0).contains(&sigma),
            "NYC Jul 4 std expected 2-10F, got {sigma}"
        );
    }

    #[test]
    fn daily_normal_vilk_april_15_in_pre_monsoon_range() {
        let (mu, sigma) = daily_normal("VILK", 4, 15).expect("VILK Apr 15 present");
        assert!(
            (30.0..=45.0).contains(&mu),
            "Lucknow Apr 15 mean expected 30-45C, got {mu}"
        );
        assert!(
            (0.5..=8.0).contains(&sigma),
            "Lucknow Apr 15 std expected 0.5-8C, got {sigma}"
        );
    }

    #[test]
    fn daily_normal_klga_feb_29_falls_back_to_feb_28() {
        let feb28 = daily_normal("KLGA", 2, 28).expect("Feb 28 present");
        let feb29 = daily_normal("KLGA", 2, 29).expect("Feb 29 fallback");
        assert_eq!(feb28, feb29);
    }

    #[test]
    fn daily_normal_lowercase_icao_resolves() {
        let upper = daily_normal("KLGA", 1, 1).expect("KLGA Jan 1");
        let lower = daily_normal("klga", 1, 1).expect("klga Jan 1");
        assert_eq!(upper, lower);
    }

    #[test]
    fn daily_normal_unknown_station_returns_none() {
        assert!(daily_normal("KFAKE", 1, 1).is_none());
    }
}
