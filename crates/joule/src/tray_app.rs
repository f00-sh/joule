//! Systray / headless tray-mode client.
//!
//! With feature `tray` and a display, this would own a system-tray icon.
//! Default build: **monitor loop** (status dash to stdout) so Linux/macOS/Windows
//! share one binary path that works headless in CI and as a desktop companion.

use anyhow::Result;
use joule_client::{format_monitor_dash, format_tray_tooltip};
use std::time::Duration;

use crate::client_status::fetch_client_status;

/// Run tray/monitor client: poll control and refresh a compact dash.
pub async fn run_tray(api: String, key: Option<String>, interval_secs: u64) -> Result<()> {
    let interval = Duration::from_secs(interval_secs.max(1));
    println!(
        "joule tray/monitor — api={api} interval={}s",
        interval.as_secs()
    );
    println!("(system tray GUI is feature-gated; this process is the status monitor surface)");
    println!("Ctrl-C to stop.\n");

    loop {
        match fetch_client_status(&api, key.as_deref()).await {
            Ok(st) => {
                // Clear-ish refresh for a living dash
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

fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}
