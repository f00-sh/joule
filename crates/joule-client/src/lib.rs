//! Shared **client status** + **service install** helpers for joule donors.
//!
//! CLI, systray, and unit-file generators all consume [`ClientStatus`] so the
//! monitor fields stay one path (connection, API/account, millijoules, tokens).

mod service;
mod status;

pub use service::{
    default_unit_path, dumb_user_get_started, enable_commands, encode_utf16_le_bom,
    generate_launchd_plist, generate_systemd_unit, generate_unit_body, generate_windows_task_xml,
    generate_windows_task_xml_file_bytes, unit_name, InstallSpec, ServiceKind, ServicePlatform,
};
pub use status::{
    format_monitor_dash, format_status_human, format_tray_tooltip, ClientStatus, ConnectionState,
    MonitorCard, StatusInputs,
};
