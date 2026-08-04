//! Graphical shell for normie users — interactive dashboard with plots.
//!
//! Primary product surface: live graphs of pool capacity / balance / tokens,
//! zoom/pan legends, one-click control + agent, full local donor policy, chat.

use anyhow::Result;
use eframe::egui::{self, Color32, RichText, Sense, Vec2};
use egui_plot::{
    Bar, BarChart, CoordinatesFormatter, Corner, HLine, Legend, Line, Plot, PlotPoints,
};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::client_status::fetch_client_status;
use crate::donor_policy::{self, DonorPolicy, ScheduleWindow};
use crate::identity;
use joule_client::{ClientStatus, ConnectionState};
use joule_proto::CLUSTER_MODEL;

/// History sample for interactive plots.
#[derive(Debug, Clone)]
pub struct HistorySample {
    pub t: f64,
    pub backends: f64,
    pub agents: f64,
    pub mesh_peers: f64,
    pub dht_records: f64,
    pub vram_gib: f64,
    pub balance_mj: f64,
    pub tokens: f64,
    pub prompt_tokens: f64,
    pub completion_tokens: f64,
    pub contributed_mj: f64,
    pub consumed_mj: f64,
    pub donating: f64,
}

/// Pure assembly of plot series from history (unit-testable).
pub fn backends_series(hist: &[HistorySample]) -> Vec<[f64; 2]> {
    hist.iter().map(|h| [h.t, h.backends]).collect()
}

pub fn balance_series(hist: &[HistorySample]) -> Vec<[f64; 2]> {
    hist.iter().map(|h| [h.t, h.balance_mj]).collect()
}

pub fn series_xy(hist: &[HistorySample], f: impl Fn(&HistorySample) -> f64) -> Vec<[f64; 2]> {
    hist.iter().map(|h| [h.t, f(h)]).collect()
}

/// Per-second rate between consecutive samples (0 for first point).
pub fn rate_series(hist: &[HistorySample], f: impl Fn(&HistorySample) -> f64) -> Vec<[f64; 2]> {
    let mut out = Vec::with_capacity(hist.len());
    for i in 0..hist.len() {
        if i == 0 {
            out.push([hist[i].t, 0.0]);
            continue;
        }
        let dt = (hist[i].t - hist[i - 1].t).max(1e-6);
        let dv = f(&hist[i]) - f(&hist[i - 1]);
        out.push([hist[i].t, dv / dt]);
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuiTab {
    Overview,
    Graphs,
    Donor,
    Chat,
}

/// Which series show on the multi-metric pool plot.
#[derive(Debug, Clone)]
struct SeriesToggles {
    backends: bool,
    agents: bool,
    mesh: bool,
    dht: bool,
    vram: bool,
    balance: bool,
    tokens: bool,
    token_rate: bool,
    economy: bool,
}

impl Default for SeriesToggles {
    fn default() -> Self {
        Self {
            backends: true,
            agents: true,
            mesh: true,
            dht: false,
            vram: true,
            balance: true,
            tokens: true,
            token_rate: true,
            economy: true,
        }
    }
}

/// Launch the graphical shell (blocks until window closes).
pub fn run_gui(api: String) -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_min_inner_size([900.0, 600.0])
            .with_title("joule — pool dashboard"),
        ..Default::default()
    };
    let api_clone = api.clone();
    eframe::run_native(
        "joule",
        options,
        Box::new(move |cc| {
            // Dark-ish readable defaults for long monitoring sessions.
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            Ok(Box::new(JouleGuiApp::new(api_clone)))
        }),
    )
    .map_err(|e| anyhow::anyhow!("gui: {e}"))?;
    Ok(())
}

struct JouleGuiApp {
    api: String,
    status: Option<ClientStatus>,
    err: Option<String>,
    last_poll: Instant,
    poll_every: Duration,
    poll_secs: f32,
    history: VecDeque<HistorySample>,
    t0: Instant,
    control_child: Option<Child>,
    agent_child: Option<Child>,
    policy_path: PathBuf,
    mem_cap_draft: u32,
    max_temp_draft: f32,
    min_batt_draft: f32,
    use_max_temp: bool,
    use_min_batt: bool,
    use_schedule: bool,
    sched_start: u16,
    sched_end: u16,
    log: Vec<String>,
    rt: Option<tokio::runtime::Runtime>,
    tab: GuiTab,
    series: SeriesToggles,
    reset_plots: bool,
    auto_scroll_log: bool,
    chat_prompt: String,
    chat_reply: String,
    chat_busy: bool,
    chat_err: Option<String>,
    link_time_axes: bool,
    /// One-shot first-frame local pool boot for dumb users.
    auto_started: bool,
}

impl JouleGuiApp {
    fn new(api: String) -> Self {
        let policy_path = DonorPolicy::default_path();
        let pol = DonorPolicy::load(&policy_path).unwrap_or_default();
        let mem_cap = pol.mem_cap_mib.unwrap_or(0);
        let (use_schedule, sched_start, sched_end) = match pol.schedule.as_ref() {
            Some(w) => (
                true,
                w.start_min_utc,
                if w.end_min_utc == 0 {
                    1440
                } else {
                    w.end_min_utc
                },
            ),
            None => (false, 0, 1440),
        };
        Self {
            api,
            status: None,
            err: None,
            last_poll: Instant::now() - Duration::from_secs(10),
            poll_every: Duration::from_secs(2),
            poll_secs: 2.0,
            history: VecDeque::with_capacity(512),
            t0: Instant::now(),
            control_child: None,
            agent_child: None,
            policy_path,
            mem_cap_draft: mem_cap,
            max_temp_draft: pol.max_temp_c.unwrap_or(85.0),
            min_batt_draft: pol.min_battery_pct.unwrap_or(20.0),
            use_max_temp: pol.max_temp_c.is_some(),
            use_min_batt: pol.min_battery_pct.is_some(),
            use_schedule,
            sched_start,
            sched_end,
            log: vec![
                "joule GUI ready.".into(),
                "STUPID-EASY: click ★ DO EVERYTHING (local pool) once.".into(),
                "Or: Start control → Start agent (donate) → Chat tab.".into(),
                "Survive reboot: run  joule service install  in a terminal.".into(),
            ],
            auto_started: false,
            rt: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .ok(),
            tab: GuiTab::Overview,
            series: SeriesToggles::default(),
            reset_plots: false,
            auto_scroll_log: true,
            chat_prompt: String::new(),
            chat_reply: String::new(),
            chat_busy: false,
            chat_err: None,
            link_time_axes: true,
        }
    }

    fn push_log(&mut self, s: impl Into<String>) {
        self.log.push(s.into());
        if self.log.len() > 80 {
            self.log.drain(0..self.log.len() - 80);
        }
    }

    fn child_running(child: &mut Option<Child>) -> bool {
        match child.as_mut() {
            Some(c) => c.try_wait().ok().flatten().is_none(),
            None => false,
        }
    }

    fn poll(&mut self) {
        let Some(rt) = self.rt.as_ref() else {
            self.err = Some("tokio runtime missing".into());
            return;
        };
        let api = self.api.clone();
        let key = identity::cached_api_key(&identity::default_path());
        match rt.block_on(fetch_client_status(&api, key.as_deref())) {
            Ok(st) => {
                let t = self.t0.elapsed().as_secs_f64();
                self.history.push_back(HistorySample {
                    t,
                    backends: f64::from(st.pool_backends),
                    agents: f64::from(st.agents_connected),
                    mesh_peers: f64::from(st.mesh_peers),
                    dht_records: f64::from(st.dht_records),
                    vram_gib: st.pool_vram_gib as f64,
                    balance_mj: st.balance_millijoules as f64,
                    tokens: st.total_tokens_used as f64,
                    prompt_tokens: st.prompt_tokens_used as f64,
                    completion_tokens: st.completion_tokens_used as f64,
                    contributed_mj: st.contributed_mj_window as f64,
                    consumed_mj: st.consumed_mj_window as f64,
                    donating: if st.donating { 1.0 } else { 0.0 },
                });
                while self.history.len() > 400 {
                    self.history.pop_front();
                }
                self.status = Some(st);
                self.err = None;
            }
            Err(e) => {
                self.err = Some(format!("{e:#}"));
            }
        }
        self.last_poll = Instant::now();
    }

    fn spawn_control(&mut self) {
        if Self::child_running(&mut self.control_child) {
            self.push_log("control already running");
            return;
        }
        let bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("joule"));
        match Command::new(&bin)
            .args([
                "control",
                "--agent-listen",
                "127.0.0.1:7701",
                "--http-listen",
                "127.0.0.1:7700",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => {
                self.control_child = Some(child);
                self.push_log("started joule control on :7700 / :7701");
            }
            Err(e) => self.push_log(format!("failed to start control: {e}")),
        }
    }

    fn spawn_agent(&mut self) {
        if Self::child_running(&mut self.agent_child) {
            self.push_log("agent already running");
            return;
        }
        let bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("joule"));
        match Command::new(&bin)
            .args([
                "agent",
                "--control",
                "127.0.0.1:7701",
                "--policy",
                &self.policy_path.display().to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => {
                self.agent_child = Some(child);
                self.push_log("started joule agent → 127.0.0.1:7701");
            }
            Err(e) => self.push_log(format!("failed to start agent: {e}")),
        }
    }

    /// One click: control → wait → agent. For people who will not read docs.
    fn do_everything_local(&mut self) {
        self.push_log("★ DO EVERYTHING: starting local control + agent…");
        self.spawn_control();
        // Brief settle so agent can connect.
        std::thread::sleep(Duration::from_millis(600));
        self.spawn_agent();
        self.push_log("Done. Wait ~10s for challenges, then open Chat tab.");
        self.push_log("Survive reboot: open a terminal and run:  joule service install");
        self.push_log("CLI checklist anytime:  joule get-started");
    }

    fn stop_agent(&mut self) {
        if let Some(mut c) = self.agent_child.take() {
            let _ = c.kill();
            let _ = c.wait();
            self.push_log("stopped agent");
        } else {
            self.push_log("no agent child to stop");
        }
    }

    fn stop_control(&mut self) {
        if let Some(mut c) = self.control_child.take() {
            let _ = c.kill();
            let _ = c.wait();
            self.push_log("stopped control (GUI-spawned only)");
        } else {
            self.push_log("no control child to stop");
        }
    }

    fn apply_pause(&mut self, paused: bool) {
        let mut p = DonorPolicy::load(&self.policy_path).unwrap_or_default();
        p.paused = paused;
        if let Err(e) = p.save(&self.policy_path) {
            self.push_log(format!("policy save failed: {e}"));
            return;
        }
        self.push_log(if paused {
            "donor PAUSED (local policy)"
        } else {
            "donor RESUMED (local policy)"
        });
    }

    fn apply_cap(&mut self) {
        let mut p = DonorPolicy::load(&self.policy_path).unwrap_or_default();
        p.mem_cap_mib = if self.mem_cap_draft == 0 {
            None
        } else {
            Some(self.mem_cap_draft)
        };
        if let Err(e) = p.save(&self.policy_path) {
            self.push_log(format!("cap save failed: {e}"));
            return;
        }
        self.push_log(format!("mem_cap_mib set to {:?}", p.mem_cap_mib));
    }

    fn apply_full_policy(&mut self) {
        let mut p = DonorPolicy::load(&self.policy_path).unwrap_or_default();
        p.mem_cap_mib = if self.mem_cap_draft == 0 {
            None
        } else {
            Some(self.mem_cap_draft)
        };
        p.max_temp_c = if self.use_max_temp {
            Some(self.max_temp_draft)
        } else {
            None
        };
        p.min_battery_pct = if self.use_min_batt {
            Some(self.min_batt_draft)
        } else {
            None
        };
        p.schedule = if self.use_schedule {
            Some(ScheduleWindow {
                start_min_utc: self.sched_start.min(1439),
                end_min_utc: if self.sched_end >= 1440 {
                    0
                } else {
                    self.sched_end
                },
            })
        } else {
            None
        };
        if let Err(e) = p.save(&self.policy_path) {
            self.push_log(format!("policy save failed: {e}"));
            return;
        }
        self.push_log("full donor policy saved (agent reloads from --policy)");
    }

    fn send_chat(&mut self) {
        if self.chat_busy {
            return;
        }
        let prompt = self.chat_prompt.trim().to_string();
        if prompt.is_empty() {
            self.chat_err = Some("type a prompt first".into());
            return;
        }
        let Some(rt) = self.rt.as_ref() else {
            self.chat_err = Some("tokio runtime missing".into());
            return;
        };
        let key = match identity::cached_api_key(&identity::default_path()) {
            Some(k) => k,
            None => {
                self.chat_err = Some("no API key — claim one on Welcome / joule connect".into());
                return;
            }
        };
        self.chat_busy = true;
        self.chat_err = None;
        let api = self.api.clone();
        let model = CLUSTER_MODEL.to_string();
        let result = rt.block_on(async {
            let url = format!("{}/v1/chat/completions", api.trim_end_matches('/'));
            let body = serde_json::json!({
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": false,
            });
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?;
            let resp = client
                .post(&url)
                .bearer_auth(key)
                .json(&body)
                .send()
                .await?;
            let status = resp.status();
            let text = resp.text().await?;
            if !status.is_success() {
                anyhow::bail!("{status}: {text}");
            }
            let v: serde_json::Value = serde_json::from_str(&text)?;
            Ok::<_, anyhow::Error>(
                v.pointer("/choices/0/message/content")
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or(text),
            )
        });
        self.chat_busy = false;
        match result {
            Ok(reply) => {
                self.chat_reply = reply;
                self.push_log("chat ok");
            }
            Err(e) => {
                self.chat_err = Some(format!("{e:#}"));
                self.push_log(format!("chat failed: {e:#}"));
            }
        }
    }

    fn colored_line(
        name: &str,
        points: Vec<[f64; 2]>,
        color: Color32,
        fill: bool,
    ) -> Line<'static> {
        let mut line = Line::new(PlotPoints::from(points))
            .name(name.to_string())
            .color(color)
            .width(2.0_f32);
        if fill {
            line = line.fill(0.0_f32).fill_alpha(0.12_f32);
        }
        line
    }

    fn interactive_plot(name: &str, height: f32) -> Plot<'static> {
        Plot::new(name)
            .height(height)
            .allow_zoom(true)
            .allow_drag(true)
            .allow_scroll(true)
            .allow_boxed_zoom(true)
            .allow_double_click_reset(true)
            .show_axes(true)
            .show_grid(true)
            .legend(Legend::default().position(Corner::RightTop))
            .coordinates_formatter(Corner::LeftBottom, CoordinatesFormatter::with_decimals(2))
            .x_axis_label("seconds")
            .include_y(0.0)
    }

    fn draw_metric_card(
        ui: &mut egui::Ui,
        label: &str,
        value: &str,
        detail: &str,
        accent: Color32,
        spark: &[[f64; 2]],
    ) {
        ui.group(|ui| {
            ui.set_min_width(150.0);
            ui.vertical(|ui| {
                ui.label(RichText::new(label).small().color(Color32::LIGHT_GRAY));
                ui.label(RichText::new(value).heading().color(accent).strong());
                ui.small(detail);
                if spark.len() >= 2 {
                    Plot::new(format!("spark_{label}"))
                        .height(36.0)
                        .width(140.0)
                        .allow_zoom(false)
                        .allow_drag(false)
                        .allow_scroll(false)
                        .show_axes(false)
                        .show_grid(false)
                        .show_background(false)
                        .show(ui, |plot_ui| {
                            plot_ui.line(
                                Line::new(PlotPoints::from(spark.to_vec()))
                                    .color(accent)
                                    .width(1.5_f32)
                                    .fill(0.0_f32)
                                    .fill_alpha(0.2_f32),
                            );
                        });
                }
            });
        });
    }

    fn hist_vec(&self) -> Vec<HistorySample> {
        self.history.iter().cloned().collect()
    }

    fn ui_side_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Quick start");
        let ctrl_on = Self::child_running(&mut self.control_child);
        let agent_on = Self::child_running(&mut self.agent_child);
        ui.horizontal(|ui| {
            let label = if ctrl_on {
                "● control (GUI)"
            } else {
                "○ control"
            };
            ui.colored_label(
                if ctrl_on {
                    Color32::from_rgb(80, 200, 120)
                } else {
                    Color32::GRAY
                },
                label,
            );
        });
        ui.horizontal(|ui| {
            let label = if agent_on {
                "● agent (GUI)"
            } else {
                "○ agent"
            };
            ui.colored_label(
                if agent_on {
                    Color32::from_rgb(80, 180, 220)
                } else {
                    Color32::GRAY
                },
                label,
            );
        });

        ui.label(
            RichText::new("1) Click green button  2) Wait 10s  3) Chat tab  4) Terminal: joule service install")
                .small()
                .color(Color32::LIGHT_GRAY),
        );
        if ui
            .add_sized(
                [240.0, 40.0],
                egui::Button::new(
                    RichText::new("★ DO EVERYTHING (local pool)")
                        .strong()
                        .size(15.0),
                )
                .fill(Color32::from_rgb(40, 120, 60)),
            )
            .on_hover_text("Starts control + agent. One click. No thinking.")
            .clicked()
        {
            self.do_everything_local();
        }
        if ui
            .add_sized(
                [240.0, 34.0],
                egui::Button::new(RichText::new("▶ Start control only").strong()),
            )
            .on_hover_text("Local control plane on :7700 HTTP / :7701 agents")
            .clicked()
        {
            self.spawn_control();
        }
        if ui
            .add_sized(
                [240.0, 34.0],
                egui::Button::new(RichText::new("▶ Start agent (donate)").strong()),
            )
            .on_hover_text("Join pool with local donor policy")
            .clicked()
        {
            self.spawn_agent();
        }
        ui.horizontal(|ui| {
            if ui.button("Stop agent").clicked() {
                self.stop_agent();
            }
            if ui.button("Stop control").clicked() {
                self.stop_control();
            }
        });
        if ui
            .button("Enable autostart (reboot-safe)")
            .on_hover_text("Runs: joule service install (control+agent+tray user session)")
            .clicked()
        {
            self.push_log("running: joule service install …");
            let bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("joule"));
            match Command::new(&bin).args(["service", "install"]).status() {
                Ok(s) if s.success() => self.push_log("autostart installed (service install OK)"),
                Ok(s) => self.push_log(format!("service install exit {:?}", s.code())),
                Err(e) => self.push_log(format!("service install failed: {e}")),
            }
        }
        if ui.button("Open dashboard in browser").clicked() {
            let _ = open::that(format!("{}/", self.api.trim_end_matches('/')));
        }
        if ui.button("Print connect card (terminal)").clicked() {
            let id_path = identity::default_path();
            identity::print_connect_card(
                &id_path,
                &self.api,
                identity::cached_api_key(&id_path).as_deref(),
                CLUSTER_MODEL,
            );
            self.push_log("connect card printed to the launching terminal");
        }
        if ui.button("Force poll now").clicked() {
            self.poll();
            self.push_log("manual poll");
        }

        ui.separator();
        ui.heading("Refresh");
        ui.add(
            egui::Slider::new(&mut self.poll_secs, 0.5..=10.0)
                .text("sec")
                .logarithmic(false),
        );
        self.poll_every = Duration::from_secs_f32(self.poll_secs.max(0.5));
        ui.checkbox(&mut self.link_time_axes, "link graph time axes");
        if ui.button("Reset all plot views").clicked() {
            self.reset_plots = true;
        }
        if ui.button("Clear history").clicked() {
            self.history.clear();
            self.push_log("history cleared");
        }

        ui.separator();
        ui.heading("Donor (local)");
        let pol = DonorPolicy::load(&self.policy_path).unwrap_or_default();
        let mut paused = pol.paused;
        if ui
            .checkbox(&mut paused, "paused")
            .on_hover_text("Local pause — remote cannot override")
            .changed()
        {
            self.apply_pause(paused);
        }
        ui.horizontal(|ui| {
            if ui.button("Pause").clicked() {
                self.apply_pause(true);
            }
            if ui.button("Resume").clicked() {
                self.apply_pause(false);
            }
        });
        ui.horizontal(|ui| {
            ui.label("VRAM cap MiB");
            ui.add(egui::DragValue::new(&mut self.mem_cap_draft).range(0..=131_072));
        });
        if ui.button("Apply cap (0=none)").clicked() {
            self.apply_cap();
        }

        let sensors = donor_policy::probe_sensors();
        ui.separator();
        ui.heading("Sensors");
        ui.monospace(format!("temp_c={:?}", sensors.temp_c));
        ui.monospace(format!("battery={:?}", sensors.battery_pct));
        ui.monospace(format!("on_ac={:?}", sensors.on_ac));
        ui.monospace(format!(
            "tz_offset_s={}",
            donor_policy::system_utc_offset_secs()
        ));
        ui.small(format!("policy: {}", self.policy_path.display()));

        ui.separator();
        ui.heading("Activity");
        ui.checkbox(&mut self.auto_scroll_log, "newest first");
        egui::ScrollArea::vertical()
            .max_height(160.0)
            .stick_to_bottom(!self.auto_scroll_log)
            .show(ui, |ui| {
                let iter: Box<dyn Iterator<Item = &String>> = if self.auto_scroll_log {
                    Box::new(self.log.iter().rev())
                } else {
                    Box::new(self.log.iter())
                };
                for line in iter {
                    ui.small(line);
                }
            });
    }

    fn ui_overview(&mut self, ui: &mut egui::Ui) {
        if let Some(ref e) = self.err {
            ui.colored_label(Color32::from_rgb(220, 80, 60), format!("link: {e}"));
            ui.label("Start control from the left panel if you see connection refused.");
        }

        let hist = self.hist_vec();
        let be_spark = backends_series(&hist);
        let bal_spark = balance_series(&hist);
        let tok_spark = series_xy(&hist, |h| h.tokens);
        let vram_spark = series_xy(&hist, |h| h.vram_gib);

        if let Some(st) = self.status.clone() {
            let accent = match st.connection {
                ConnectionState::Connected => Color32::from_rgb(80, 200, 120),
                ConnectionState::Degraded => Color32::from_rgb(230, 180, 60),
                ConnectionState::Disconnected => Color32::from_rgb(220, 80, 60),
                ConnectionState::Unknown => Color32::GRAY,
            };
            ui.horizontal_wrapped(|ui| {
                Self::draw_metric_card(
                    ui,
                    "connection",
                    st.connection.as_str(),
                    &st.connection_detail,
                    accent,
                    &[],
                );
                Self::draw_metric_card(
                    ui,
                    "pool backends",
                    &format!("{}", st.pool_backends),
                    &format!("{} GiB · {} agents", st.pool_vram_gib, st.agents_connected),
                    Color32::from_rgb(100, 180, 255),
                    &be_spark,
                );
                Self::draw_metric_card(
                    ui,
                    "balance mJ",
                    &format!("{}", st.balance_millijoules),
                    if st.donating {
                        "donating"
                    } else {
                        "not donating"
                    },
                    Color32::from_rgb(200, 160, 255),
                    &bal_spark,
                );
                Self::draw_metric_card(
                    ui,
                    "tokens used",
                    &format!("{}", st.total_tokens_used),
                    &st.inference_mode,
                    Color32::from_rgb(255, 160, 100),
                    &tok_spark,
                );
                Self::draw_metric_card(
                    ui,
                    "mesh / dht",
                    &format!("{}/{}", st.mesh_peers, st.dht_records),
                    &format!("service_live={}", st.service_live),
                    Color32::from_rgb(120, 220, 200),
                    &vram_spark,
                );
            });

            ui.add_space(6.0);
            ui.horizontal_wrapped(|ui| {
                for c in &st.cards {
                    ui.group(|ui| {
                        ui.vertical(|ui| {
                            ui.small(&c.label);
                            ui.monospace(&c.value);
                        });
                    });
                }
            });
        } else {
            ui.label("Waiting for control… click Start control");
        }

        ui.add_space(10.0);
        ui.heading("Live capacity (interactive)");
        ui.small(
            "scroll=zoom · drag=pan · right-drag/box-zoom · double-click=reset · legend toggles",
        );
        self.ui_capacity_plot(ui, &hist, 280.0);

        ui.add_space(8.0);
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label("Balance mJ");
                self.ui_balance_plot(ui, &hist, 200.0);
            });
            ui.vertical(|ui| {
                ui.label("Tokens + rate");
                self.ui_tokens_plot(ui, &hist, 200.0);
            });
        });
    }

    fn ui_capacity_plot(&mut self, ui: &mut egui::Ui, hist: &[HistorySample], height: f32) {
        ui.horizontal(|ui| {
            ui.label("series:");
            ui.checkbox(&mut self.series.backends, "backends");
            ui.checkbox(&mut self.series.agents, "agents");
            ui.checkbox(&mut self.series.mesh, "mesh");
            ui.checkbox(&mut self.series.dht, "dht");
            ui.checkbox(&mut self.series.vram, "vram GiB");
        });

        let mut plot = Self::interactive_plot("capacity_plot", height).y_axis_label("count / GiB");
        if self.link_time_axes {
            plot = plot.link_axis("joule_time", true);
            plot = plot.link_cursor("joule_time", true);
        }
        if self.reset_plots {
            plot = plot.reset();
        }
        plot.show(ui, |plot_ui| {
            if self.series.backends {
                plot_ui.line(Self::colored_line(
                    "backends",
                    series_xy(hist, |h| h.backends),
                    Color32::from_rgb(80, 180, 255),
                    true,
                ));
            }
            if self.series.agents {
                plot_ui.line(Self::colored_line(
                    "agents",
                    series_xy(hist, |h| h.agents),
                    Color32::from_rgb(80, 220, 140),
                    false,
                ));
            }
            if self.series.mesh {
                plot_ui.line(Self::colored_line(
                    "mesh_peers",
                    series_xy(hist, |h| h.mesh_peers),
                    Color32::from_rgb(230, 180, 80),
                    false,
                ));
            }
            if self.series.dht {
                plot_ui.line(Self::colored_line(
                    "dht_records",
                    series_xy(hist, |h| h.dht_records),
                    Color32::from_rgb(200, 120, 255),
                    false,
                ));
            }
            if self.series.vram {
                plot_ui.line(Self::colored_line(
                    "vram_GiB",
                    series_xy(hist, |h| h.vram_gib),
                    Color32::from_rgb(255, 120, 120),
                    true,
                ));
            }
            // donating flag as thin binary line
            plot_ui.line(
                Line::new(PlotPoints::from(series_xy(hist, |h| h.donating)))
                    .name("donating (0/1)")
                    .color(Color32::from_rgb(180, 180, 180))
                    .width(1.0_f32),
            );
        });
    }

    fn ui_balance_plot(&mut self, ui: &mut egui::Ui, hist: &[HistorySample], height: f32) {
        let mut plot = Self::interactive_plot("balance_plot", height).y_axis_label("mJ");
        if self.link_time_axes {
            plot = plot.link_axis("joule_time", true);
            plot = plot.link_cursor("joule_time", true);
        }
        if self.reset_plots {
            plot = plot.reset();
        }
        plot.show(ui, |plot_ui| {
            if self.series.balance {
                plot_ui.line(Self::colored_line(
                    "balance",
                    series_xy(hist, |h| h.balance_mj),
                    Color32::from_rgb(180, 140, 255),
                    true,
                ));
            }
            if self.series.economy {
                plot_ui.line(Self::colored_line(
                    "contributed_window",
                    series_xy(hist, |h| h.contributed_mj),
                    Color32::from_rgb(80, 220, 140),
                    false,
                ));
                plot_ui.line(Self::colored_line(
                    "consumed_window",
                    series_xy(hist, |h| h.consumed_mj),
                    Color32::from_rgb(255, 120, 100),
                    false,
                ));
            }
            plot_ui.hline(
                HLine::new(0.0)
                    .color(Color32::from_gray(90))
                    .width(1.0_f32)
                    .name("zero"),
            );
        });
    }

    fn ui_tokens_plot(&mut self, ui: &mut egui::Ui, hist: &[HistorySample], height: f32) {
        let mut plot =
            Self::interactive_plot("tokens_plot", height).y_axis_label("tokens / tok·s⁻¹");
        if self.link_time_axes {
            plot = plot.link_axis("joule_time", true);
            plot = plot.link_cursor("joule_time", true);
        }
        if self.reset_plots {
            plot = plot.reset();
        }
        plot.show(ui, |plot_ui| {
            if self.series.tokens {
                plot_ui.line(Self::colored_line(
                    "total_tokens",
                    series_xy(hist, |h| h.tokens),
                    Color32::from_rgb(255, 160, 80),
                    true,
                ));
                plot_ui.line(Self::colored_line(
                    "prompt",
                    series_xy(hist, |h| h.prompt_tokens),
                    Color32::from_rgb(100, 180, 255),
                    false,
                ));
                plot_ui.line(Self::colored_line(
                    "completion",
                    series_xy(hist, |h| h.completion_tokens),
                    Color32::from_rgb(100, 220, 180),
                    false,
                ));
            }
            if self.series.token_rate {
                plot_ui.line(Self::colored_line(
                    "tok/s",
                    rate_series(hist, |h| h.tokens),
                    Color32::from_rgb(255, 80, 160),
                    false,
                ));
            }
        });
    }

    fn ui_graphs_tab(&mut self, ui: &mut egui::Ui) {
        let hist = self.hist_vec();
        ui.heading("Interactive graphs");
        ui.horizontal(|ui| {
            ui.label("toggles:");
            ui.checkbox(&mut self.series.backends, "backends");
            ui.checkbox(&mut self.series.agents, "agents");
            ui.checkbox(&mut self.series.mesh, "mesh");
            ui.checkbox(&mut self.series.dht, "dht");
            ui.checkbox(&mut self.series.vram, "vram");
            ui.checkbox(&mut self.series.balance, "balance");
            ui.checkbox(&mut self.series.economy, "economy window");
            ui.checkbox(&mut self.series.tokens, "tokens");
            ui.checkbox(&mut self.series.token_rate, "tok/s");
        });
        ui.separator();

        self.ui_capacity_plot(ui, &hist, 260.0);
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label("Millijoules");
                self.ui_balance_plot(ui, &hist, 240.0);
            });
            ui.vertical(|ui| {
                ui.label("Tokens");
                self.ui_tokens_plot(ui, &hist, 240.0);
            });
        });

        ui.add_space(8.0);
        ui.label("Token mix (latest snapshot)");
        if let Some(last) = hist.last() {
            let bars = vec![
                Bar::new(0.0, last.prompt_tokens)
                    .name("prompt")
                    .fill(Color32::from_rgb(100, 180, 255)),
                Bar::new(1.0, last.completion_tokens)
                    .name("completion")
                    .fill(Color32::from_rgb(100, 220, 180)),
                Bar::new(2.0, last.tokens)
                    .name("total")
                    .fill(Color32::from_rgb(255, 160, 80)),
            ];
            let mut plot = Plot::new("token_bars")
                .height(180.0)
                .allow_zoom(true)
                .allow_drag(true)
                .allow_boxed_zoom(true)
                .legend(Legend::default())
                .include_y(0.0)
                .y_axis_label("tokens")
                .x_axis_label("kind");
            if self.reset_plots {
                plot = plot.reset();
            }
            plot.show(ui, |plot_ui| {
                plot_ui.bar_chart(BarChart::new(bars).width(0.6).name("tokens"));
            });
        } else {
            ui.label("No samples yet — start control and wait for a poll.");
        }

        ui.add_space(6.0);
        ui.label("Balance rate (mJ/s)");
        let rate = rate_series(&hist, |h| h.balance_mj);
        let mut plot = Self::interactive_plot("balance_rate", 160.0).y_axis_label("mJ/s");
        if self.reset_plots {
            plot = plot.reset();
        }
        plot.show(ui, |plot_ui| {
            plot_ui.line(Self::colored_line(
                "Δbalance/s",
                rate,
                Color32::from_rgb(200, 140, 255),
                true,
            ));
            plot_ui.hline(HLine::new(0.0).color(Color32::from_gray(90)).width(1.0_f32));
        });
    }

    fn ui_donor_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Donor policy (local law 5)");
        ui.label("Remote operators cannot raise these limits. Agent reloads from --policy path.");
        ui.monospace(self.policy_path.display().to_string());
        ui.separator();

        let pol = DonorPolicy::load(&self.policy_path).unwrap_or_default();
        let sensors = donor_policy::probe_sensors();
        let now = DonorPolicy::now_unix_secs();
        let allowed = pol.allows_donate(now, sensors);
        ui.horizontal(|ui| {
            ui.label("currently allows donate:");
            ui.colored_label(
                if allowed {
                    Color32::from_rgb(80, 200, 120)
                } else {
                    Color32::from_rgb(220, 80, 60)
                },
                if allowed { "YES" } else { "NO" },
            );
        });
        for line in pol.status_lines(sensors) {
            ui.monospace(line);
        }

        ui.separator();
        ui.heading("Edit");
        let mut paused = pol.paused;
        if ui.checkbox(&mut paused, "paused").changed() {
            self.apply_pause(paused);
        }

        ui.horizontal(|ui| {
            ui.label("VRAM cap MiB (0 = none)");
            ui.add(
                egui::Slider::new(&mut self.mem_cap_draft, 0..=65_536)
                    .logarithmic(true)
                    .text("MiB"),
            );
        });

        ui.checkbox(&mut self.use_max_temp, "enforce max temperature");
        if self.use_max_temp {
            ui.add(
                egui::Slider::new(&mut self.max_temp_draft, 40.0..=110.0)
                    .suffix(" °C")
                    .text("max temp"),
            );
        }
        ui.checkbox(
            &mut self.use_min_batt,
            "enforce min battery (when not on AC)",
        );
        if self.use_min_batt {
            ui.add(
                egui::Slider::new(&mut self.min_batt_draft, 0.0..=100.0)
                    .suffix(" %")
                    .text("min battery"),
            );
        }

        ui.checkbox(&mut self.use_schedule, "limit to local schedule window");
        if self.use_schedule {
            ui.add(
                egui::Slider::new(&mut self.sched_start, 0..=1439)
                    .text("start (local minute of day)"),
            );
            ui.add(
                egui::Slider::new(&mut self.sched_end, 0..=1440)
                    .text("end (local minute; 1440=midnight)"),
            );
            ui.small(format!(
                "window ≈ {:02}:{:02} → {:02}:{:02} local (wraps if start>end)",
                self.sched_start / 60,
                self.sched_start % 60,
                (self.sched_end.min(1439)) / 60,
                (self.sched_end.min(1439)) % 60,
            ));
        }

        if ui
            .add_sized(
                [280.0, 36.0],
                egui::Button::new(RichText::new("Save full donor policy").strong()),
            )
            .clicked()
        {
            self.apply_full_policy();
        }

        ui.separator();
        ui.heading("Live sensors");
        // Mini gauge-ish bars for temp/battery when available
        if let Some(t) = sensors.temp_c {
            ui.horizontal(|ui| {
                ui.label(format!("temp {t:.1} °C"));
                let frac = (t / 100.0).clamp(0.0, 1.0);
                let color = if t >= self.max_temp_draft && self.use_max_temp {
                    Color32::from_rgb(220, 80, 60)
                } else {
                    Color32::from_rgb(80, 180, 255)
                };
                let (rect, _) = ui.allocate_exact_size(Vec2::new(200.0, 14.0), Sense::hover());
                ui.painter().rect_filled(rect, 3.0, Color32::from_gray(40));
                let mut filled = rect;
                filled.set_width(rect.width() * frac);
                ui.painter().rect_filled(filled, 3.0, color);
            });
        }
        if let Some(b) = sensors.battery_pct {
            ui.horizontal(|ui| {
                ui.label(format!("battery {b:.0}%"));
                let frac = (b / 100.0).clamp(0.0, 1.0);
                let color = if b < self.min_batt_draft && self.use_min_batt {
                    Color32::from_rgb(220, 80, 60)
                } else {
                    Color32::from_rgb(80, 200, 120)
                };
                let (rect, _) = ui.allocate_exact_size(Vec2::new(200.0, 14.0), Sense::hover());
                ui.painter().rect_filled(rect, 3.0, Color32::from_gray(40));
                let mut filled = rect;
                filled.set_width(rect.width() * frac);
                ui.painter().rect_filled(filled, 3.0, color);
            });
        }
        ui.monospace(format!("on_ac={:?}", sensors.on_ac));
    }

    fn ui_chat_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Cluster chat");
        ui.label(format!(
            "Model: {CLUSTER_MODEL} · requires API key + donating agent for that account"
        ));
        if let Some(st) = self.status.as_ref() {
            ui.small(format!(
                "account={} · key={} · balance={} mJ · donating={}",
                st.account.as_deref().unwrap_or("—"),
                st.api_key_hint.as_deref().unwrap_or("—"),
                st.balance_millijoules,
                st.donating
            ));
        }
        ui.separator();
        ui.label("Prompt");
        ui.add(
            egui::TextEdit::multiline(&mut self.chat_prompt)
                .desired_width(f32::INFINITY)
                .desired_rows(4)
                .hint_text("Say hi to the pool…"),
        );
        ui.horizontal(|ui| {
            let send = ui
                .add_enabled(
                    !self.chat_busy,
                    egui::Button::new(if self.chat_busy { "Sending…" } else { "Send" }),
                )
                .clicked();
            if ui.button("Clear").clicked() {
                self.chat_prompt.clear();
                self.chat_reply.clear();
                self.chat_err = None;
            }
            if send {
                self.send_chat();
            }
        });
        if let Some(ref e) = self.chat_err {
            ui.colored_label(Color32::from_rgb(220, 80, 60), e);
        }
        ui.separator();
        ui.label("Reply");
        egui::ScrollArea::vertical()
            .max_height(360.0)
            .show(ui, |ui| {
                ui.add(
                    egui::TextEdit::multiline(&mut self.chat_reply)
                        .desired_width(f32::INFINITY)
                        .desired_rows(12)
                        .interactive(false),
                );
            });
    }
}

impl eframe::App for JouleGuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // First frame: if nothing is listening, boot local pool (disable with JOULE_GUI_NO_AUTO=1).
        if !self.auto_started {
            self.auto_started = true;
            if std::env::var_os("JOULE_GUI_NO_AUTO").is_none() {
                let busy = std::net::TcpStream::connect_timeout(
                    &"127.0.0.1:7700"
                        .parse()
                        .unwrap_or_else(|_| ([127, 0, 0, 1], 7700).into()),
                    Duration::from_millis(150),
                )
                .is_ok();
                if busy {
                    self.push_log("control already on :7700 — not auto-starting another");
                } else {
                    self.push_log(
                        "first open: auto-starting local pool (set JOULE_GUI_NO_AUTO=1 to skip)",
                    );
                    self.do_everything_local();
                }
            }
        }
        if self.last_poll.elapsed() >= self.poll_every {
            self.poll();
        }
        // Keep UI lively for plots even between polls.
        ctx.request_repaint_after(Duration::from_millis(200));

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(RichText::new("joule").strong());
                ui.label("idle GPUs → open cluster · pay in compute");
                ui.separator();
                ui.selectable_value(&mut self.tab, GuiTab::Overview, "Overview");
                ui.selectable_value(&mut self.tab, GuiTab::Graphs, "Graphs");
                ui.selectable_value(&mut self.tab, GuiTab::Donor, "Donor");
                ui.selectable_value(&mut self.tab, GuiTab::Chat, "Chat");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.monospace(&self.api);
                    if let Some(st) = self.status.as_ref() {
                        let c = match st.connection {
                            ConnectionState::Connected => Color32::from_rgb(80, 200, 120),
                            ConnectionState::Degraded => Color32::from_rgb(230, 180, 60),
                            ConnectionState::Disconnected => Color32::from_rgb(220, 80, 60),
                            ConnectionState::Unknown => Color32::GRAY,
                        };
                        ui.colored_label(c, st.connection.as_str());
                    }
                });
            });
        });

        egui::SidePanel::left("actions")
            .default_width(270.0)
            .resizable(true)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    self.ui_side_panel(ui);
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| match self.tab {
                GuiTab::Overview => self.ui_overview(ui),
                GuiTab::Graphs => self.ui_graphs_tab(ui),
                GuiTab::Donor => self.ui_donor_tab(ui),
                GuiTab::Chat => self.ui_chat_tab(ui),
            });
        });

        // One-frame plot reset flag.
        self.reset_plots = false;
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if let Some(mut c) = self.agent_child.take() {
            let _ = c.kill();
        }
        // Leave control running if user started it — durable local pool.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(t: f64, backends: f64, balance: f64, tokens: f64) -> HistorySample {
        HistorySample {
            t,
            backends,
            agents: backends,
            mesh_peers: 0.0,
            dht_records: 0.0,
            vram_gib: backends * 8.0,
            balance_mj: balance,
            tokens,
            prompt_tokens: tokens * 0.4,
            completion_tokens: tokens * 0.6,
            contributed_mj: balance * 0.5,
            consumed_mj: 1.0,
            donating: 1.0,
        }
    }

    #[test]
    fn plot_series_from_history() {
        let h = vec![sample(0.0, 1.0, 10.0, 0.0), sample(1.0, 2.0, 20.0, 5.0)];
        let be = backends_series(&h);
        assert_eq!(be.len(), 2);
        assert_eq!(be[1][1], 2.0);
        let bal = balance_series(&h);
        assert_eq!(bal[1][1], 20.0);
        let agents = series_xy(&h, |s| s.agents);
        assert_eq!(agents[1][1], 2.0);
    }

    #[test]
    fn rate_series_computes_delta_per_sec() {
        let h = vec![sample(0.0, 1.0, 10.0, 0.0), sample(2.0, 1.0, 10.0, 10.0)];
        let rates = rate_series(&h, |s| s.tokens);
        assert_eq!(rates[0][1], 0.0);
        assert!((rates[1][1] - 5.0).abs() < 1e-9); // 10 tokens / 2s
    }
}
