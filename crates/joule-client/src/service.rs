//! OS-managed auto-start unit generation (systemd / launchd / Windows task).
//!
//! Prefer **user-session** services so a systray can coexist with the agent.
//! Pure string generation — install CLI may write files; tests assert content.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServicePlatform {
    LinuxSystemd,
    MacosLaunchd,
    WindowsTask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    /// Donor agent (background compute).
    Agent,
    /// Status tray UI (user session).
    Tray,
}

/// Inputs for generating install artifacts.
#[derive(Debug, Clone)]
pub struct InstallSpec {
    pub platform: ServicePlatform,
    pub kind: ServiceKind,
    /// Absolute or PATH-resolved binary (e.g. /usr/local/bin/joule).
    pub binary_path: String,
    /// For agent: control host:port.
    pub control: String,
    /// For agent: account name.
    pub account: String,
    /// For tray/status: HTTP API base.
    pub api: String,
    /// Optional API key for tray polling (prefer env file in real deploy).
    pub api_key: Option<String>,
    pub mem_mib: u32,
    /// Linux: user unit (default true) vs system unit.
    pub user_unit: bool,
    pub description: String,
}

impl Default for InstallSpec {
    fn default() -> Self {
        Self {
            platform: ServicePlatform::LinuxSystemd,
            kind: ServiceKind::Agent,
            binary_path: "joule".into(),
            control: "127.0.0.1:7701".into(),
            account: "donor".into(),
            api: "http://127.0.0.1:7700".into(),
            api_key: None,
            mem_mib: 8192,
            user_unit: true,
            description: "joule donor agent".into(),
        }
    }
}

/// systemd unit body (user or system).
pub fn generate_systemd_unit(spec: &InstallSpec) -> String {
    let exec = match spec.kind {
        ServiceKind::Agent => format!(
            "{} agent --account {} --control {} --mem-mib {}",
            shell_escape(&spec.binary_path),
            shell_escape(&spec.account),
            shell_escape(&spec.control),
            spec.mem_mib
        ),
        ServiceKind::Tray => {
            let mut e = format!(
                "{} tray --api {}",
                shell_escape(&spec.binary_path),
                shell_escape(&spec.api)
            );
            if let Some(ref k) = spec.api_key {
                e.push_str(&format!(" --key {}", shell_escape(k)));
            }
            e
        }
    };
    let wanted = if spec.user_unit {
        "default.target"
    } else {
        "multi-user.target"
    };
    format!(
        r#"[Unit]
Description={desc}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exec}
Restart=on-failure
RestartSec=5
# User-session agent/tray: do not require root.

[Install]
WantedBy={wanted}
"#,
        desc = spec.description,
        exec = exec,
        wanted = wanted,
    )
}

/// macOS launchd plist (LaunchAgents / user domain).
pub fn generate_launchd_plist(spec: &InstallSpec) -> String {
    let label = match spec.kind {
        ServiceKind::Agent => "sh.f00.joule.agent",
        ServiceKind::Tray => "sh.f00.joule.tray",
    };
    let mut args: Vec<String> = vec![spec.binary_path.clone()];
    match spec.kind {
        ServiceKind::Agent => {
            args.extend([
                "agent".into(),
                "--account".into(),
                spec.account.clone(),
                "--control".into(),
                spec.control.clone(),
                "--mem-mib".into(),
                spec.mem_mib.to_string(),
            ]);
        }
        ServiceKind::Tray => {
            args.extend(["tray".into(), "--api".into(), spec.api.clone()]);
            if let Some(ref k) = spec.api_key {
                args.extend(["--key".into(), k.clone()]);
            }
        }
    }
    let args_xml: String = args
        .iter()
        .map(|a| format!("    <string>{}</string>", xml_escape(a)))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{args}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/tmp/{label}.out.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/{label}.err.log</string>
</dict>
</plist>
"#,
        label = label,
        args = args_xml,
    )
}

/// Windows scheduled task XML (Task Scheduler user logon trigger).
pub fn generate_windows_task_xml(spec: &InstallSpec) -> String {
    let (name, args) = match spec.kind {
        ServiceKind::Agent => (
            "joule-agent",
            format!(
                "agent --account {} --control {} --mem-mib {}",
                spec.account, spec.control, spec.mem_mib
            ),
        ),
        ServiceKind::Tray => {
            let mut a = format!("tray --api {}", spec.api);
            if let Some(ref k) = spec.api_key {
                a.push_str(&format!(" --key {k}"));
            }
            ("joule-tray", a)
        }
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>{desc}</Description>
    <URI>\{name}</URI>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <Enabled>true</Enabled>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{bin}</Command>
      <Arguments>{args}</Arguments>
    </Exec>
  </Actions>
</Task>
"#,
        desc = xml_escape(&spec.description),
        name = name,
        bin = xml_escape(&spec.binary_path),
        args = xml_escape(&args),
    )
}

fn shell_escape(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "/._-:@".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_agent_unit_points_at_joule_agent() {
        let spec = InstallSpec {
            binary_path: "/usr/local/bin/joule".into(),
            account: "alice".into(),
            control: "127.0.0.1:7701".into(),
            ..Default::default()
        };
        let u = generate_systemd_unit(&spec);
        assert!(u.contains("[Service]"));
        assert!(u.contains("ExecStart="));
        assert!(u.contains("/usr/local/bin/joule"));
        assert!(u.contains("agent"));
        assert!(u.contains("--account alice"));
        assert!(u.contains("Restart=on-failure"));
        assert!(u.contains("WantedBy=default.target"));
    }

    #[test]
    fn launchd_plist_has_run_at_load() {
        let spec = InstallSpec {
            platform: ServicePlatform::MacosLaunchd,
            kind: ServiceKind::Tray,
            binary_path: "/opt/homebrew/bin/joule".into(),
            ..Default::default()
        };
        let p = generate_launchd_plist(&spec);
        assert!(p.contains("sh.f00.joule.tray"));
        assert!(p.contains("RunAtLoad"));
        assert!(p.contains("tray"));
        assert!(p.contains("/opt/homebrew/bin/joule"));
    }

    #[test]
    fn windows_task_has_logon_trigger() {
        let spec = InstallSpec {
            platform: ServicePlatform::WindowsTask,
            binary_path: r"C:\Program Files\joule\joule.exe".into(),
            ..Default::default()
        };
        let x = generate_windows_task_xml(&spec);
        assert!(x.contains("LogonTrigger"));
        assert!(x.contains("joule.exe"));
        assert!(x.contains("agent"));
    }
}
