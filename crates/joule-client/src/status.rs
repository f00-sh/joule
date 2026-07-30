//! Pure status snapshot assembly (testable without display server or network).

use serde::{Deserialize, Serialize};

/// High-level link to the control plane / agent session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connected,
    Degraded,
    Disconnected,
    Unknown,
}

impl ConnectionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Degraded => "degraded",
            Self::Disconnected => "disconnected",
            Self::Unknown => "unknown",
        }
    }
}

/// One compact monitor card for CLI dash / tray menu.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorCard {
    pub label: String,
    pub value: String,
}

/// Live client snapshot used by CLI `status`/`monitor` and the systray.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientStatus {
    pub connection: ConnectionState,
    pub connection_detail: String,
    pub api_base: String,
    pub account: Option<String>,
    pub api_key_hint: Option<String>,
    pub donating: bool,
    pub balance_millijoules: i64,
    pub contributed_mj_window: i64,
    pub consumed_mj_window: i64,
    /// Lifetime prompt tokens attributed to this account (control-tracked).
    pub prompt_tokens_used: u64,
    /// Lifetime completion tokens attributed to this account.
    pub completion_tokens_used: u64,
    pub total_tokens_used: u64,
    pub pool_backends: u32,
    pub pool_vram_gib: u64,
    pub agents_connected: u32,
    pub service_live: bool,
    pub operator_paused: bool,
    pub inference_mode: String,
    pub cards: Vec<MonitorCard>,
}

/// Raw inputs gathered from control HTTP (or tests). Pure assemble — no I/O.
#[derive(Debug, Clone, Default)]
pub struct StatusInputs {
    pub api_base: String,
    /// True if healthz/HTTP base responded.
    pub control_reachable: bool,
    pub agents_connected: u32,
    pub pool_backends: u32,
    pub pool_vram_gib: u64,
    pub service_live: bool,
    pub operator_paused: bool,
    pub inference_mode: String,
    pub stream_slots_free: u32,
    pub stream_slots_used: u32,
    pub account: Option<String>,
    pub api_key_hint: Option<String>,
    pub donating: bool,
    pub balance_millijoules: i64,
    pub contributed_mj_window: i64,
    pub consumed_mj_window: i64,
    pub prompt_tokens_used: u64,
    pub completion_tokens_used: u64,
    /// True if the local agent process is known to be running (optional).
    pub local_agent_running: Option<bool>,
}

impl ClientStatus {
    /// Build snapshot from polled control fields (shared by CLI + tray).
    pub fn from_inputs(i: StatusInputs) -> Self {
        let (connection, connection_detail) = if !i.control_reachable {
            (
                ConnectionState::Disconnected,
                "control unreachable".to_string(),
            )
        } else if i.operator_paused {
            (
                ConnectionState::Degraded,
                "operator paused service".to_string(),
            )
        } else if i.agents_connected == 0 {
            (ConnectionState::Degraded, "no agents connected".to_string())
        } else if i.local_agent_running == Some(false) {
            (
                ConnectionState::Degraded,
                "local agent not running".to_string(),
            )
        } else {
            (
                ConnectionState::Connected,
                format!(
                    "ok · {} agent(s) · {} backends",
                    i.agents_connected, i.pool_backends
                ),
            )
        };

        let total = i
            .prompt_tokens_used
            .saturating_add(i.completion_tokens_used);
        let mut cards = vec![
            MonitorCard {
                label: "connection".into(),
                value: connection.as_str().into(),
            },
            MonitorCard {
                label: "balance_mJ".into(),
                value: i.balance_millijoules.to_string(),
            },
            MonitorCard {
                label: "tokens_used".into(),
                value: total.to_string(),
            },
            MonitorCard {
                label: "prompt_tok".into(),
                value: i.prompt_tokens_used.to_string(),
            },
            MonitorCard {
                label: "completion_tok".into(),
                value: i.completion_tokens_used.to_string(),
            },
            MonitorCard {
                label: "donating".into(),
                value: if i.donating { "yes" } else { "no" }.into(),
            },
            MonitorCard {
                label: "pool_backends".into(),
                value: i.pool_backends.to_string(),
            },
            MonitorCard {
                label: "pool_vram_GiB".into(),
                value: i.pool_vram_gib.to_string(),
            },
            MonitorCard {
                label: "agents".into(),
                value: i.agents_connected.to_string(),
            },
            MonitorCard {
                label: "slots".into(),
                value: {
                    let total = i.stream_slots_used.saturating_add(i.stream_slots_free);
                    format!("{}/{} used", i.stream_slots_used, total)
                },
            },
            MonitorCard {
                label: "service_live".into(),
                value: if i.service_live { "yes" } else { "no" }.into(),
            },
            MonitorCard {
                label: "mode".into(),
                value: if i.inference_mode.is_empty() {
                    "—".into()
                } else {
                    i.inference_mode.clone()
                },
            },
        ];
        if let Some(ref a) = i.account {
            cards.insert(
                1,
                MonitorCard {
                    label: "account".into(),
                    value: a.clone(),
                },
            );
        }

        Self {
            connection,
            connection_detail,
            api_base: i.api_base,
            account: i.account,
            api_key_hint: i.api_key_hint,
            donating: i.donating,
            balance_millijoules: i.balance_millijoules,
            contributed_mj_window: i.contributed_mj_window,
            consumed_mj_window: i.consumed_mj_window,
            prompt_tokens_used: i.prompt_tokens_used,
            completion_tokens_used: i.completion_tokens_used,
            total_tokens_used: total,
            pool_backends: i.pool_backends,
            pool_vram_gib: i.pool_vram_gib,
            agents_connected: i.agents_connected,
            service_live: i.service_live,
            operator_paused: i.operator_paused,
            inference_mode: i.inference_mode,
            cards,
        }
    }
}

/// Multi-line human status (CLI).
pub fn format_status_human(s: &ClientStatus) -> String {
    let mut out = String::new();
    out.push_str("joule client status\n");
    out.push_str(&format!(
        "  connection:  {} ({})\n",
        s.connection.as_str(),
        s.connection_detail
    ));
    out.push_str(&format!("  api:         {}\n", s.api_base));
    out.push_str(&format!(
        "  account:     {}\n",
        s.account.as_deref().unwrap_or("—")
    ));
    out.push_str(&format!(
        "  api_key:     {}\n",
        s.api_key_hint.as_deref().unwrap_or("—")
    ));
    out.push_str(&format!(
        "  donating:    {}\n",
        if s.donating { "yes" } else { "no" }
    ));
    out.push_str(&format!(
        "  balance:     {} millijoules\n",
        s.balance_millijoules
    ));
    out.push_str(&format!(
        "  window:      +{} mJ contributed / −{} mJ consumed\n",
        s.contributed_mj_window, s.consumed_mj_window
    ));
    out.push_str(&format!(
        "  tokens used: {} total (prompt {} + completion {})\n",
        s.total_tokens_used, s.prompt_tokens_used, s.completion_tokens_used
    ));
    out.push_str(&format!(
        "  pool:        {} backends · {} GiB · agents {}\n",
        s.pool_backends, s.pool_vram_gib, s.agents_connected
    ));
    out.push_str(&format!(
        "  service:     live={} paused={} mode={}\n",
        s.service_live, s.operator_paused, s.inference_mode
    ));
    out
}

/// Compact one-screen monitor dash (CLI `monitor` / people love small dashes).
pub fn format_monitor_dash(s: &ClientStatus) -> String {
    let mut lines = vec![
        "┌─ joule monitor ─────────────────────────────".into(),
        format!("│ {:12} {}", "link", s.connection.as_str()),
        format!("│ {:12} {}", "detail", s.connection_detail),
    ];
    for c in &s.cards {
        lines.push(format!("│ {:12} {}", c.label, c.value));
    }
    lines.push("└─────────────────────────────────────────────".into());
    lines.join("\n")
}

/// Short tooltip / tray title string.
pub fn format_tray_tooltip(s: &ClientStatus) -> String {
    format!(
        "joule · {} · {} mJ · {} tok · {} backends",
        s.connection.as_str(),
        s.balance_millijoules,
        s.total_tokens_used,
        s.pool_backends
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_maps_connection_balance_tokens_and_api() {
        let s = ClientStatus::from_inputs(StatusInputs {
            api_base: "http://127.0.0.1:7700".into(),
            control_reachable: true,
            agents_connected: 2,
            pool_backends: 2,
            pool_vram_gib: 24,
            service_live: false,
            operator_paused: false,
            inference_mode: "stub_awaiting_pool".into(),
            stream_slots_free: 3,
            stream_slots_used: 1,
            account: Some("alice".into()),
            api_key_hint: Some("joule_ab…".into()),
            donating: true,
            balance_millijoules: 120,
            contributed_mj_window: 80,
            consumed_mj_window: 10,
            prompt_tokens_used: 40,
            completion_tokens_used: 60,
            local_agent_running: Some(true),
        });
        assert_eq!(s.connection, ConnectionState::Connected);
        assert_eq!(s.balance_millijoules, 120);
        assert_eq!(s.total_tokens_used, 100);
        assert_eq!(s.prompt_tokens_used, 40);
        assert_eq!(s.completion_tokens_used, 60);
        assert_eq!(s.account.as_deref(), Some("alice"));
        assert!(s.donating);
        assert_eq!(s.api_base, "http://127.0.0.1:7700");
        let human = format_status_human(&s);
        assert!(human.contains("connected"));
        assert!(human.contains("120 millijoules"));
        assert!(human.contains("tokens used: 100"));
        assert!(human.contains("alice"));
        let dash = format_monitor_dash(&s);
        assert!(dash.contains("balance_mJ"));
        assert!(dash.contains("tokens_used"));
        let tip = format_tray_tooltip(&s);
        assert!(tip.contains("120 mJ"));
        assert!(tip.contains("100 tok"));
    }

    #[test]
    fn disconnected_when_control_down() {
        let s = ClientStatus::from_inputs(StatusInputs {
            api_base: "http://127.0.0.1:9".into(),
            control_reachable: false,
            ..Default::default()
        });
        assert_eq!(s.connection, ConnectionState::Disconnected);
        assert!(s.connection_detail.contains("unreachable"));
    }

    #[test]
    fn degraded_when_paused() {
        let s = ClientStatus::from_inputs(StatusInputs {
            control_reachable: true,
            operator_paused: true,
            agents_connected: 1,
            ..Default::default()
        });
        assert_eq!(s.connection, ConnectionState::Degraded);
    }
}
