//! Graphical shell for normie users — interactive dashboard with plots.
//!
//! Primary product surface: graphs of pool capacity / balance / tokens, plus
//! one-click control + agent launch, donor pause/cap, and connect card.

use anyhow::Result;
use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::client_status::fetch_client_status;
use crate::donor_policy::{self, DonorPolicy};
use crate::identity;
use joule_client::ClientStatus;

/// History sample for interactive plots.
#[derive(Debug, Clone)]
pub struct HistorySample {
    pub t: f64,
    pub backends: f64,
    pub vram_gib: f64,
    pub balance_mj: f64,
    pub tokens: f64,
}

/// Pure assembly of plot series from history (unit-testable).
pub fn backends_series(hist: &[HistorySample]) -> Vec<[f64; 2]> {
    hist.iter().map(|h| [h.t, h.backends]).collect()
}

pub fn balance_series(hist: &[HistorySample]) -> Vec<[f64; 2]> {
    hist.iter().map(|h| [h.t, h.balance_mj]).collect()
}

/// Launch the graphical shell (blocks until window closes).
pub fn run_gui(api: String) -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 720.0])
            .with_title("joule — pool dashboard"),
        ..Default::default()
    };
    let api_clone = api.clone();
    eframe::run_native(
        "joule",
        options,
        Box::new(move |_cc| Ok(Box::new(JouleGuiApp::new(api_clone)))),
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
    history: VecDeque<HistorySample>,
    t0: Instant,
    control_child: Option<Child>,
    agent_child: Option<Child>,
    policy_path: PathBuf,
    mem_cap_draft: u32,
    log: Vec<String>,
    rt: Option<tokio::runtime::Runtime>,
}

impl JouleGuiApp {
    fn new(api: String) -> Self {
        let policy_path = DonorPolicy::default_path();
        let mem_cap = DonorPolicy::load(&policy_path)
            .ok()
            .and_then(|p| p.mem_cap_mib)
            .unwrap_or(0);
        Self {
            api,
            status: None,
            err: None,
            last_poll: Instant::now() - Duration::from_secs(10),
            poll_every: Duration::from_secs(2),
            history: VecDeque::with_capacity(256),
            t0: Instant::now(),
            control_child: None,
            agent_child: None,
            policy_path,
            mem_cap_draft: mem_cap,
            log: vec!["joule GUI ready — start control, then agent.".into()],
            rt: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .ok(),
        }
    }

    fn push_log(&mut self, s: impl Into<String>) {
        self.log.push(s.into());
        if self.log.len() > 40 {
            self.log.drain(0..self.log.len() - 40);
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
                    vram_gib: st.pool_vram_gib as f64,
                    balance_mj: st.balance_millijoules as f64,
                    tokens: st.total_tokens_used as f64,
                });
                while self.history.len() > 200 {
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
        if let Some(c) = self.control_child.as_mut() {
            if c.try_wait().ok().flatten().is_none() {
                self.push_log("control already running");
                return;
            }
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
        if let Some(c) = self.agent_child.as_mut() {
            if c.try_wait().ok().flatten().is_none() {
                self.push_log("agent already running");
                return;
            }
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
}

impl eframe::App for JouleGuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.last_poll.elapsed() >= self.poll_every {
            self.poll();
        }
        ctx.request_repaint_after(self.poll_every);

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("joule");
                ui.label("idle GPUs → open cluster · pay in compute");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.monospace(&self.api);
                });
            });
        });

        egui::SidePanel::left("actions")
            .default_width(260.0)
            .show(ctx, |ui| {
                ui.heading("Quick start");
                if ui
                    .add_sized([240.0, 36.0], egui::Button::new("▶ Start control"))
                    .clicked()
                {
                    self.spawn_control();
                }
                if ui
                    .add_sized([240.0, 36.0], egui::Button::new("▶ Start agent (donate)"))
                    .clicked()
                {
                    self.spawn_agent();
                }
                if ui.button("Open dashboard in browser").clicked() {
                    let _ = open::that(format!("{}/", self.api.trim_end_matches('/')));
                }
                if ui.button("Show connect card (Cursor)").clicked() {
                    let id_path = identity::default_path();
                    identity::print_connect_card(
                        &id_path,
                        &self.api,
                        identity::cached_api_key(&id_path).as_deref(),
                        joule_proto::CLUSTER_MODEL,
                    );
                    self.push_log("connect card printed to the terminal that launched GUI");
                }
                ui.separator();
                ui.heading("Donor (local)");
                let pol = DonorPolicy::load(&self.policy_path).unwrap_or_default();
                ui.label(format!("paused: {}", pol.paused));
                ui.horizontal(|ui| {
                    if ui.button("Pause").clicked() {
                        self.apply_pause(true);
                    }
                    if ui.button("Resume").clicked() {
                        self.apply_pause(false);
                    }
                });
                ui.horizontal(|ui| {
                    ui.label("VRAM cap MiB (0=none)");
                    ui.add(egui::DragValue::new(&mut self.mem_cap_draft).range(0..=131_072));
                    if ui.button("Apply cap").clicked() {
                        self.apply_cap();
                    }
                });
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
                ui.separator();
                ui.heading("Activity");
                egui::ScrollArea::vertical()
                    .max_height(180.0)
                    .show(ui, |ui| {
                        for line in self.log.iter().rev() {
                            ui.small(line);
                        }
                    });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(ref e) = self.err {
                ui.colored_label(egui::Color32::from_rgb(220, 80, 60), format!("link: {e}"));
                ui.label("Start control from the left panel if you see connection refused.");
            }
            if let Some(ref st) = self.status {
                ui.horizontal(|ui| {
                    ui.group(|ui| {
                        ui.vertical(|ui| {
                            ui.label("connection");
                            ui.strong(st.connection.as_str());
                            ui.small(&st.connection_detail);
                        });
                    });
                    ui.group(|ui| {
                        ui.vertical(|ui| {
                            ui.label("pool backends");
                            ui.strong(format!("{}", st.pool_backends));
                            ui.small(format!("{} GiB verified class", st.pool_vram_gib));
                        });
                    });
                    ui.group(|ui| {
                        ui.vertical(|ui| {
                            ui.label("balance mJ");
                            ui.strong(format!("{}", st.balance_millijoules));
                            ui.small(if st.donating {
                                "donating"
                            } else {
                                "not donating"
                            });
                        });
                    });
                    ui.group(|ui| {
                        ui.vertical(|ui| {
                            ui.label("tokens used");
                            ui.strong(format!("{}", st.total_tokens_used));
                            ui.small(&st.inference_mode);
                        });
                    });
                });
            } else {
                ui.label("Waiting for control… click Start control");
            }

            ui.add_space(8.0);
            ui.heading("Live graphs");
            let hist: Vec<_> = self.history.iter().cloned().collect();
            let be = backends_series(&hist);
            let bal = balance_series(&hist);
            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    ui.label("Pool backends over time");
                    Plot::new("backends_plot")
                        .height(220.0)
                        .allow_zoom(true)
                        .allow_drag(true)
                        .show(ui, |plot_ui| {
                            if !be.is_empty() {
                                plot_ui.line(Line::new(PlotPoints::from(be)).name("backends"));
                            }
                        });
                });
                ui.vertical(|ui| {
                    ui.label("Millijoule balance over time");
                    Plot::new("balance_plot")
                        .height(220.0)
                        .allow_zoom(true)
                        .allow_drag(true)
                        .show(ui, |plot_ui| {
                            if !bal.is_empty() {
                                plot_ui.line(Line::new(PlotPoints::from(bal)).name("mJ"));
                            }
                        });
                });
            });
            let vram: Vec<[f64; 2]> = hist.iter().map(|h| [h.t, h.vram_gib]).collect();
            let toks: Vec<[f64; 2]> = hist.iter().map(|h| [h.t, h.tokens]).collect();
            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    ui.label("Pool VRAM (GiB class)");
                    Plot::new("vram_plot").height(200.0).show(ui, |plot_ui| {
                        if !vram.is_empty() {
                            plot_ui.line(Line::new(PlotPoints::from(vram)).name("vram"));
                        }
                    });
                });
                ui.vertical(|ui| {
                    ui.label("Tokens used");
                    Plot::new("tok_plot").height(200.0).show(ui, |plot_ui| {
                        if !toks.is_empty() {
                            plot_ui.line(Line::new(PlotPoints::from(toks)).name("tokens"));
                        }
                    });
                });
            });
        });
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

    #[test]
    fn plot_series_from_history() {
        let h = vec![
            HistorySample {
                t: 0.0,
                backends: 1.0,
                vram_gib: 8.0,
                balance_mj: 10.0,
                tokens: 0.0,
            },
            HistorySample {
                t: 1.0,
                backends: 2.0,
                vram_gib: 16.0,
                balance_mj: 20.0,
                tokens: 5.0,
            },
        ];
        let be = backends_series(&h);
        assert_eq!(be.len(), 2);
        assert_eq!(be[1][1], 2.0);
        let bal = balance_series(&h);
        assert_eq!(bal[1][1], 20.0);
    }
}
