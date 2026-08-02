//! Local donor contribution policy (product law 5).
//!
//! Caps, pause, schedule, and thermal/battery limits are **local** — remote
//! operators cannot raise them. Pure evaluation; I/O (sensors, files) is separate.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Optional wall-clock schedule window in **local** minutes from midnight.
/// (Field names keep `utc` for on-disk compatibility; values are local time.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleWindow {
    /// Inclusive start minute of local day [0, 1440).
    pub start_min_utc: u16,
    /// Exclusive end minute of local day (0 = 1440). May wrap past midnight.
    pub end_min_utc: u16,
}

/// Local minute-of-day from unix seconds + fixed offset (seconds east of UTC).
pub fn local_minute_of_day(unix_secs: u64, utc_offset_secs: i32) -> u16 {
    let local = (unix_secs as i64 + i64::from(utc_offset_secs)).rem_euclid(86_400) as u64;
    ((local / 60) % 1440) as u16
}

/// Best-effort system UTC offset (seconds east of UTC).
/// Prefer `TZ`/`chrono`-free: compare local civil time via `strftime` is OS-heavy;
/// use `i32::from(chrono)` alternative — env `JOULE_TZ_OFFSET_SECS` or 0.
pub fn system_utc_offset_secs() -> i32 {
    if let Ok(v) = std::env::var("JOULE_TZ_OFFSET_SECS") {
        if let Ok(n) = v.parse::<i32>() {
            return n;
        }
    }
    // Linux: /etc/localtime offset is hard without libc; default 0 (UTC).
    // Agents in non-UTC regions set JOULE_TZ_OFFSET_SECS or use inject in tests.
    0
}

impl ScheduleWindow {
    pub fn contains_minute(&self, minute_of_day: u16) -> bool {
        let m = minute_of_day % 1440;
        let s = self.start_min_utc % 1440;
        let mut e = self.end_min_utc % 1440;
        if e == 0 && self.end_min_utc != 0 {
            e = 1440;
        }
        if s == e {
            return true; // empty / full-day window = always on
        }
        if s < e {
            m >= s && m < e
        } else {
            // wraps midnight
            m >= s || m < e
        }
    }
}

/// Optional sensor sample (injected at boundary; absence = sensor unavailable).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SensorSample {
    pub temp_c: Option<f32>,
    pub battery_pct: Option<f32>,
    pub on_ac: Option<bool>,
}

/// Local donor policy. Defaults allow full contribution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DonorPolicy {
    /// When true, agent must not offer healthy contribution.
    #[serde(default)]
    pub paused: bool,
    /// Hard cap on advertised/claim MiB (local). None = no extra cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_cap_mib: Option<u32>,
    /// Optional UTC schedule for when donation is allowed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleWindow>,
    /// Pause contribution if temp ≥ this (°C). None = ignore thermal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_temp_c: Option<f32>,
    /// Pause when battery below this percent (and not on AC). None = ignore battery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_battery_pct: Option<f32>,
}

impl DonorPolicy {
    pub fn default_path() -> PathBuf {
        if let Ok(p) = std::env::var("JOULE_DONOR_POLICY") {
            return PathBuf::from(p);
        }
        std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".config/joule/donor-policy.json"))
            .unwrap_or_else(|| PathBuf::from("donor-policy.json"))
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.is_file() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_string_pretty(self)?;
        std::fs::write(path, raw)?;
        Ok(())
    }

    /// Clamp advertised claim by local mem cap (remote cannot raise this).
    pub fn effective_mem_mib(&self, claim_mib: u32) -> u32 {
        match self.mem_cap_mib {
            Some(cap) if cap > 0 => claim_mib.min(cap),
            _ => claim_mib,
        }
    }

    /// Whether the agent may donate (heartbeat healthy / join as contributor).
    /// Schedule uses **local** wall time (`utc_offset_secs` east of UTC; inject for tests).
    pub fn allows_donate_with_offset(
        &self,
        now_unix_secs: u64,
        utc_offset_secs: i32,
        sensors: SensorSample,
    ) -> bool {
        if self.paused {
            return false;
        }
        if let Some(ref win) = self.schedule {
            let minute = local_minute_of_day(now_unix_secs, utc_offset_secs);
            if !win.contains_minute(minute) {
                return false;
            }
        }
        if let (Some(max_t), Some(t)) = (self.max_temp_c, sensors.temp_c) {
            if t >= max_t {
                return false;
            }
        }
        if let Some(min_b) = self.min_battery_pct {
            let on_ac = sensors.on_ac.unwrap_or(false);
            if !on_ac {
                if let Some(b) = sensors.battery_pct {
                    if b < min_b {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Production helper: local TZ via [`system_utc_offset_secs`].
    pub fn allows_donate(&self, now_unix_secs: u64, sensors: SensorSample) -> bool {
        self.allows_donate_with_offset(now_unix_secs, system_utc_offset_secs(), sensors)
    }

    pub fn now_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Human summary for tray/status (includes sensors).
    pub fn status_lines(&self, sensors: SensorSample) -> Vec<String> {
        let now = Self::now_unix_secs();
        let off = system_utc_offset_secs();
        let donating = self.allows_donate_with_offset(now, off, sensors);
        vec![
            format!("paused={}", self.paused),
            format!(
                "mem_cap={}",
                self.mem_cap_mib
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "none".into())
            ),
            format!(
                "schedule_local={}",
                self.schedule
                    .as_ref()
                    .map(|w| format!(
                        "{:02}:{:02}-{:02}:{:02}",
                        w.start_min_utc / 60,
                        w.start_min_utc % 60,
                        w.end_min_utc / 60,
                        w.end_min_utc % 60
                    ))
                    .unwrap_or_else(|| "always".into())
            ),
            format!(
                "sensors temp_c={:?} battery_pct={:?} on_ac={:?}",
                sensors.temp_c, sensors.battery_pct, sensors.on_ac
            ),
            format!("allows_donate={donating} (local; remote cannot override)"),
        ]
    }
}

/// Best-effort platform sensor probe (never invents fake telemetry).
pub fn probe_sensors() -> SensorSample {
    // Linux thermal zone 0 if present; battery via sysfs when available.
    let mut s = SensorSample::default();
    #[cfg(target_os = "linux")]
    {
        if let Ok(raw) = std::fs::read_to_string("/sys/class/thermal/thermal_zone0/temp") {
            if let Ok(milli) = raw.trim().parse::<f32>() {
                s.temp_c = Some(milli / 1000.0);
            }
        }
        // Common battery path
        let cap = Path::new("/sys/class/power_supply/BAT0/capacity");
        let status = Path::new("/sys/class/power_supply/BAT0/status");
        if cap.is_file() {
            if let Ok(raw) = std::fs::read_to_string(cap) {
                s.battery_pct = raw.trim().parse().ok();
            }
        }
        if status.is_file() {
            if let Ok(raw) = std::fs::read_to_string(status) {
                let t = raw.trim().to_ascii_lowercase();
                s.on_ac = Some(t.contains("charging") || t.contains("full") || t == "not charging");
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_blocks_donate() {
        let mut p = DonorPolicy::default();
        assert!(p.allows_donate(0, SensorSample::default()));
        p.paused = true;
        assert!(!p.allows_donate(0, SensorSample::default()));
    }

    #[test]
    fn mem_cap_clamps_claim() {
        let p = DonorPolicy {
            mem_cap_mib: Some(4096),
            ..Default::default()
        };
        assert_eq!(p.effective_mem_mib(8192), 4096);
        assert_eq!(p.effective_mem_mib(2048), 2048);
    }

    #[test]
    fn schedule_window_local_tz() {
        let win = ScheduleWindow {
            start_min_utc: 9 * 60,
            end_min_utc: 17 * 60,
        };
        assert!(win.contains_minute(10 * 60));
        assert!(!win.contains_minute(8 * 60));
        let p = DonorPolicy {
            schedule: Some(win),
            ..Default::default()
        };
        // unix 10:00 UTC + offset -5h → local 05:00 → outside 9–17
        let t = 10 * 3600u64;
        assert!(!p.allows_donate_with_offset(t, -5 * 3600, SensorSample::default()));
        // same UTC 10:00 with offset 0 → local 10:00 → inside
        assert!(p.allows_donate_with_offset(t, 0, SensorSample::default()));
        // UTC 14:00 + offset +8h → local 22:00 → outside
        assert!(!p.allows_donate_with_offset(14 * 3600, 8 * 3600, SensorSample::default()));
        // UTC 02:00 + offset +8h → local 10:00 → inside
        assert!(p.allows_donate_with_offset(2 * 3600, 8 * 3600, SensorSample::default()));
    }

    #[test]
    fn thermal_and_battery_policy() {
        let p = DonorPolicy {
            max_temp_c: Some(85.0),
            min_battery_pct: Some(20.0),
            ..Default::default()
        };
        assert!(!p.allows_donate(
            0,
            SensorSample {
                temp_c: Some(90.0),
                ..Default::default()
            }
        ));
        assert!(p.allows_donate(
            0,
            SensorSample {
                temp_c: Some(40.0),
                battery_pct: Some(50.0),
                on_ac: Some(false),
            }
        ));
        assert!(!p.allows_donate(
            0,
            SensorSample {
                temp_c: Some(40.0),
                battery_pct: Some(10.0),
                on_ac: Some(false),
            }
        ));
        // On AC: low battery ignored
        assert!(p.allows_donate(
            0,
            SensorSample {
                temp_c: Some(40.0),
                battery_pct: Some(5.0),
                on_ac: Some(true),
            }
        ));
    }

    #[test]
    fn policy_roundtrip_file() {
        let dir = std::env::temp_dir().join(format!("joule-policy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("donor-policy.json");
        let p = DonorPolicy {
            paused: true,
            mem_cap_mib: Some(2048),
            schedule: Some(ScheduleWindow {
                start_min_utc: 0,
                end_min_utc: 60,
            }),
            max_temp_c: Some(80.0),
            min_battery_pct: Some(15.0),
        };
        p.save(&path).unwrap();
        let loaded = DonorPolicy::load(&path).unwrap();
        assert_eq!(loaded, p);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Agent must reload **this** policy path (not a different default file).
    #[test]
    fn explicit_policy_path_pause_and_cap_are_independent_of_default() {
        let dir = std::env::temp_dir().join(format!(
            "joule-policy-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let agent_path = dir.join("agent-session.json");
        let other_path = dir.join("other-default.json");
        // Session file: paused + cap 4096
        DonorPolicy {
            paused: true,
            mem_cap_mib: Some(4096),
            ..Default::default()
        }
        .save(&agent_path)
        .unwrap();
        // Unrelated default-looking file: fully open
        DonorPolicy::default().save(&other_path).unwrap();

        let session = DonorPolicy::load(&agent_path).unwrap();
        let other = DonorPolicy::load(&other_path).unwrap();
        assert!(session.paused);
        assert!(!other.paused);
        assert_eq!(session.effective_mem_mib(8192), 4096);
        assert_eq!(other.effective_mem_mib(8192), 8192);
        assert!(!session.allows_donate(0, SensorSample::default()));
        assert!(other.allows_donate(0, SensorSample::default()));
        // Mutating session path is what `joule donor pause --policy agent-session` does
        let mut s2 = DonorPolicy::load(&agent_path).unwrap();
        s2.paused = false;
        s2.save(&agent_path).unwrap();
        assert!(DonorPolicy::load(&agent_path)
            .unwrap()
            .allows_donate(0, SensorSample::default()));
        // other file untouched
        assert!(!DonorPolicy::load(&other_path).unwrap().paused);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
