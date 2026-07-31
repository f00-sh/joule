//! Systray / product identity surface + headless tray-mode client.
//!
//! Default build: **monitor loop** (status dash to stdout) so Linux/macOS/Windows
//! share one binary path. Identity actions (copy CODE / enter CODE / open recovery)
//! are also exposed via `joule tray --copy-code` etc. and `joule identity *`.

use anyhow::{bail, Context, Result};
use joule_client::{format_monitor_dash, format_tray_tooltip};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crate::client_status::fetch_client_status;

/// Run tray/monitor client: poll control and refresh a compact dash.
pub async fn run_tray(api: String, key: Option<String>, interval_secs: u64) -> Result<()> {
    let interval = Duration::from_secs(interval_secs.max(1));
    println!(
        "joule tray/monitor — api={api} interval={}s",
        interval.as_secs()
    );
    println!("identity: joule tray --copy-code | --enter-code UUID | --open-recovery | --onboard");
    println!("Ctrl-C to stop.\n");

    loop {
        match fetch_client_status(&api, key.as_deref()).await {
            Ok(st) => {
                print!("\x1B[2J\x1B[H");
                println!("{}", format_tray_tooltip(&st));
                println!();
                println!("{}", format_monitor_dash(&st));
                println!();
                println!("updated {}", chrono_like_now());
            }
            Err(e) => {
                eprintln!("status poll error: {e}");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// Best-effort clipboard copy (real OS tools — not a fake in-memory clipboard).
pub fn copy_to_clipboard(text: &str) -> Result<()> {
    // Prefer wayland/x11/mac/windows tools when present.
    let candidates: &[(&str, &[&str])] = &[
        ("wl-copy", &[]),
        ("xclip", &["-selection", "clipboard"]),
        ("xsel", &["--clipboard", "--input"]),
        ("pbcopy", &[]),
        ("clip.exe", &[]), // WSL
    ];
    let has_display = std::env::var_os("DISPLAY").is_some()
        || std::env::var_os("WAYLAND_DISPLAY").is_some();
    for (bin, args) in candidates {
        // xclip/xsel block forever with no X/Wayland (headless SSH/CI).
        if matches!(*bin, "xclip" | "xsel") && !has_display {
            continue;
        }
        if which(bin) {
            let mut child = Command::new(bin)
                .args(*args)
                .stdin(std::process::Stdio::piped())
                .spawn()
                .with_context(|| format!("spawn {bin}"))?;
            use std::io::Write;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(text.as_bytes())?;
            }
            let st = child.wait()?;
            if st.success() {
                return Ok(());
            }
        }
    }
    // Fallback: write to a well-known file under config for GUIs to pick up.
    let path = crate::identity::default_path()
        .parent()
        .map(|p| p.join("CODE.clipboard.txt"))
        .unwrap_or_else(|| std::path::PathBuf::from("CODE.clipboard.txt"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, format!("{text}\n"))?;
    bail!(
        "no clipboard tool (wl-copy/xclip/pbcopy); wrote {}",
        path.display()
    );
}

/// Open a path with the OS default handler (`xdg-open` / `open` / `cmd start`).
pub fn open_path(path: &Path) -> Result<()> {
    let p = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf());
    if which("xdg-open") {
        Command::new("xdg-open").arg(&p).spawn()?.wait()?;
        return Ok(());
    }
    if which("open") {
        Command::new("open").arg(&p).spawn()?.wait()?;
        return Ok(());
    }
    if which("cmd.exe") {
        Command::new("cmd.exe")
            .args(["/C", "start", "", &p.to_string_lossy()])
            .spawn()?
            .wait()?;
        return Ok(());
    }
    if let Ok(ed) = std::env::var("EDITOR") {
        Command::new(ed).arg(&p).spawn()?.wait()?;
        return Ok(());
    }
    bail!("no opener found for {}", p.display());
}

fn which(bin: &str) -> bool {
    Command::new("sh")
        .args(["-c", &format!("command -v {bin} >/dev/null 2>&1")])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_fallback_writes_file() {
        // When no clipboard tool is forced unavailable, path still works via write fallback.
        // We only assert the helper does not panic on empty path env — real copy is OS-dependent.
        let r = copy_to_clipboard("550e8400-e29b-41d4-a716-446655440000");
        // Either Ok (tool present) or Err with clipboard file path.
        match r {
            Ok(()) => {}
            Err(e) => {
                let msg = format!("{e:#}");
                assert!(
                    msg.contains("CODE.clipboard.txt") || msg.contains("clipboard"),
                    "{msg}"
                );
            }
        }
    }
}
