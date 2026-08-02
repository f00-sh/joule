//! Startup GPU / VRAM probe — clamp advertised **claim** honesty.
//!
//! Mint and placement still use protocol **verified** capacity only.
//! This module only bounds what the agent is allowed to *advertise* as claim.
//!
//! Override for tests/ops: `JOULE_PROBE_VRAM_MIB` (forces probe total MiB;
//! empty/missing = real detect). `JOULE_PROBE_FORCE_CPU=1` forces no-GPU.

use std::process::Command;

/// Result of a local hardware probe (not a challenge, not mint authority).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuProbe {
    pub available: bool,
    /// Total detectable device memory MiB (0 if unknown / CPU-only).
    pub total_mem_mib: u32,
    /// cuda | metal | cpu | unknown
    pub backend: &'static str,
    pub detail: String,
}

impl GpuProbe {
    pub fn cpu_only(detail: impl Into<String>) -> Self {
        Self {
            available: false,
            total_mem_mib: 0,
            backend: "cpu",
            detail: detail.into(),
        }
    }
}

/// Probe local VRAM. Prefer env override for tests, then platform tools.
pub fn probe_vram() -> GpuProbe {
    if std::env::var("JOULE_PROBE_FORCE_CPU").ok().as_deref() == Some("1") {
        return GpuProbe::cpu_only("JOULE_PROBE_FORCE_CPU=1");
    }
    if let Ok(s) = std::env::var("JOULE_PROBE_VRAM_MIB") {
        if let Ok(n) = s.trim().parse::<u32>() {
            if n == 0 {
                return GpuProbe::cpu_only("JOULE_PROBE_VRAM_MIB=0");
            }
            return GpuProbe {
                available: true,
                total_mem_mib: n,
                backend: "env",
                detail: format!("JOULE_PROBE_VRAM_MIB={n}"),
            };
        }
    }

    if let Some(p) = probe_nvidia_smi() {
        return p;
    }
    if let Some(p) = probe_linux_drm() {
        return p;
    }
    if let Some(p) = probe_metal_sysctl() {
        return p;
    }

    GpuProbe::cpu_only("no GPU backend detected (nvidia-smi/drm/metal)")
}

/// Clamp user-requested claim to probed capacity.
///
/// - If probe has capacity: `min(requested, total)` (at least 0).
/// - If no GPU / zero VRAM: claim becomes 0 (cannot advertise a farm).
pub fn clamp_claim(requested_mem_mib: u32, probe: &GpuProbe) -> u32 {
    if !probe.available || probe.total_mem_mib == 0 {
        return 0;
    }
    requested_mem_mib.min(probe.total_mem_mib)
}

/// Adjust device class when claim is forced to 0 on a "gpu" request.
pub fn effective_device(requested: &str, claim_mib: u32) -> &'static str {
    let r = requested.to_ascii_lowercase();
    if claim_mib == 0 && (r == "gpu" || r == "metal") {
        return "cpu";
    }
    match r.as_str() {
        "metal" => "metal",
        "cpu" => "cpu",
        _ => "gpu",
    }
}

fn probe_nvidia_smi() -> Option<GpuProbe> {
    let out = Command::new("nvidia-smi")
        .args(["--query-gpu=memory.total", "--format=csv,noheader,nounits"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut total = 0u32;
    for line in text.lines() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        // nvidia-smi reports MiB
        if let Ok(mib) = t.parse::<u32>() {
            total = total.saturating_add(mib);
        }
    }
    if total == 0 {
        return None;
    }
    Some(GpuProbe {
        available: true,
        total_mem_mib: total,
        backend: "cuda",
        detail: format!("nvidia-smi sum={total} MiB"),
    })
}

fn probe_linux_drm() -> Option<GpuProbe> {
    // Best-effort: sum mem_info_vram_total from amdgpu/sysfs if present (bytes).
    let path = std::path::Path::new("/sys/class/drm");
    if !path.is_dir() {
        return None;
    }
    let mut total_bytes: u64 = 0;
    let entries = std::fs::read_dir(path).ok()?;
    for e in entries.flatten() {
        let p = e.path().join("device/mem_info_vram_total");
        if let Ok(s) = std::fs::read_to_string(&p) {
            if let Ok(b) = s.trim().parse::<u64>() {
                total_bytes = total_bytes.saturating_add(b);
            }
        }
    }
    if total_bytes == 0 {
        return None;
    }
    let mib = (total_bytes / (1024 * 1024)) as u32;
    if mib == 0 {
        return None;
    }
    Some(GpuProbe {
        available: true,
        total_mem_mib: mib,
        backend: "drm",
        detail: format!("sysfs drm vram≈{mib} MiB"),
    })
}

fn probe_metal_sysctl() -> Option<GpuProbe> {
    // macOS: hw.memsize is system RAM, not GPU VRAM — use only as weak upper bound
    // when no better signal; still better than advertising unlimited claim.
    #[cfg(target_os = "macos")]
    {
        let out = Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let bytes: u64 = text.trim().parse().ok()?;
        // Unified memory: allow up to 50% of system RAM as advertise clamp.
        let mib = ((bytes / 2) / (1024 * 1024)) as u32;
        if mib < 256 {
            return None;
        }
        return Some(GpuProbe {
            available: true,
            total_mem_mib: mib,
            backend: "metal",
            detail: format!("macos unified-memory clamp≈{mib} MiB (½ hw.memsize)"),
        });
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env overrides are process-global; serialize tests that touch them.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn clamp_never_exceeds_probe() {
        let p = GpuProbe {
            available: true,
            total_mem_mib: 8192,
            backend: "env",
            detail: "t".into(),
        };
        assert_eq!(clamp_claim(65_536, &p), 8192);
        assert_eq!(clamp_claim(4096, &p), 4096);
    }

    #[test]
    fn no_gpu_claim_is_zero() {
        let p = GpuProbe::cpu_only("none");
        assert_eq!(clamp_claim(24_576, &p), 0);
        assert_eq!(effective_device("gpu", 0), "cpu");
    }

    #[test]
    fn env_override_and_force_cpu_are_real_entries() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Drive real probe_vram with env (not a reimplemented clamp).
        std::env::remove_var("JOULE_PROBE_FORCE_CPU");
        std::env::set_var("JOULE_PROBE_VRAM_MIB", "3072");
        let p = probe_vram();
        assert!(p.available);
        assert_eq!(p.total_mem_mib, 3072);
        assert_eq!(clamp_claim(99_999, &p), 3072);

        // FORCE_CPU wins even if VRAM override remains set.
        std::env::set_var("JOULE_PROBE_FORCE_CPU", "1");
        let p = probe_vram();
        assert!(!p.available);
        assert_eq!(p.total_mem_mib, 0);

        std::env::remove_var("JOULE_PROBE_FORCE_CPU");
        std::env::remove_var("JOULE_PROBE_VRAM_MIB");
    }
}
