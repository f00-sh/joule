//! joule — distributed compute cluster CLI.

mod client_status;
mod donor_policy;
mod gpu_probe;
mod gui;
mod identity;
mod peer_net;
mod tray_app;

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use clap::{Parser, Subcommand};
use joule_client::{
    format_monitor_dash, format_status_human, generate_launchd_plist, generate_systemd_unit,
    generate_windows_task_xml, generate_windows_task_xml_file_bytes, InstallSpec, ServiceKind,
    ServicePlatform,
};
use joule_cluster::{plan_redundant_chunks, Cluster, ModelChunk};
use joule_control::{
    body_sha256_hex, now_ms, operator_preimage, operator_pubkey_hex, verify_operator_sig,
};
use joule_ledger::{estimate_contribution_millijoules, estimate_usage_millijoules, Ledger};
use joule_proto::{
    decode_line, encode_line, BlobMeta, ClusterCapacity, DeviceClass, Envelope, Message, NodeCaps,
    NodeId, OperatorKind, SignedEnvelope, CLUSTER_MODEL,
};
use joule_runtime::{
    apply_staged, load_model, match_target, parse_software_update, prepare_and_install,
    prepare_and_install_for_band, read_stage, readiness_for_pool_ex, stage_blob, Engine,
    InferRequest, ManifestFile, ProductionEngine, RuntimeFlags, SoftwareTarget, StubEngine,
    WeightsStore,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Wire chunk size for peer BlobChunk (lab-sized files; large models later).
const BLOB_CHUNK_BYTES: usize = 64 * 1024;
/// Refuse assembling larger than this over control-relayed chunks (DoS guard).
const MAX_RELAY_BLOB_BYTES: u64 = 256 * 1024 * 1024;

struct PendingBlobRecv {
    sha256: String,
    buf: Vec<u8>,
    next_offset: u64,
}

#[derive(Parser, Debug)]
#[command(
    name = "joule",
    version,
    about = "Distributed compute cluster: donate idle GPUs, earn millijoules, run open-weight AI"
)]
struct Cli {
    /// Optional subcommand. **No subcommand = graphical dashboard** (normie default).
    #[command(subcommand)]
    cmd: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Graphical dashboard — graphs, start control/agent, donor controls (default).
    Gui {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
    },
    /// Print protocol and build identity.
    Version,
    /// Run the control plane (agent TCP + HTTP API + dashboard).
    Control {
        /// Agent protocol listen address.
        #[arg(long, default_value = "127.0.0.1:7701")]
        agent_listen: SocketAddr,
        /// HTTP API listen address (capacity + chat + dashboard).
        #[arg(long, default_value = "127.0.0.1:7700")]
        http_listen: SocketAddr,
        /// Persist accounts/keys/balances here (default: $JOULE_DATA_DIR or ~/.local/share/joule).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Do not load/save state on disk.
        #[arg(long, default_value_t = false)]
        ephemeral: bool,
    },
    /// Your joule code (anonymous multi-machine account — no PII).
    Identity {
        #[command(subcommand)]
        cmd: IdentityCmd,
    },
    /// Join the cluster as a donor agent (earn millijoules).
    Agent {
        /// Control plane agent address (host:port).
        #[arg(long, default_value = "127.0.0.1:7701")]
        control: String,
        /// Paste your joule code to link this machine (same code = same millijoules).
        /// If omitted, a code is created automatically on first run.
        #[arg(long, default_value = "")]
        code: String,
        /// Lab-only account nickname. Prefer --code / auto identity for real use.
        #[arg(long, default_value = "")]
        account: String,
        /// Path to identity JSON (default: ~/.config/joule/identity.json or $JOULE_IDENTITY).
        #[arg(long, default_value = "")]
        identity: String,
        /// Ignored (single-model cluster). Kept for CLI compat; always CLUSTER_MODEL.
        #[arg(long, default_value = CLUSTER_MODEL)]
        model: String,
        /// Advertised memory MiB.
        #[arg(long, default_value_t = 8192)]
        mem_mib: u32,
        /// Device class: gpu | metal | cpu.
        #[arg(long, default_value = "gpu")]
        device: String,
        /// Heartbeat interval seconds.
        #[arg(long, default_value_t = 5)]
        heartbeat_secs: u64,
        /// Peer listen for direct mesh dial (blob seed). Default: ephemeral port on 127.0.0.1.
        /// Set e.g. 0.0.0.0:7702 for internet donors. Empty disables peer listen.
        #[arg(long, default_value = "127.0.0.1:0")]
        peer_listen: String,
        /// Local pause: do not contribute (healthy=false). Product law 5.
        #[arg(long, default_value_t = false)]
        pause: bool,
        /// Local hard cap on advertised VRAM claim (MiB). Remote cannot raise this.
        #[arg(long, default_value_t = 0)]
        mem_cap_mib: u32,
        /// Max GPU/CPU temp °C before auto-pause (0 = ignore). Uses platform sensors when present.
        #[arg(long, default_value_t = 0.0)]
        max_temp_c: f32,
        /// Min battery % before auto-pause when on battery (0 = ignore).
        #[arg(long, default_value_t = 0.0)]
        min_battery_pct: f32,
        /// UTC schedule window HH:MM-HH:MM (e.g. 09:00-17:00). Empty = always.
        #[arg(long, default_value = "")]
        schedule: String,
        /// Load/save donor policy JSON (default: ~/.config/joule/donor-policy.json).
        #[arg(long, default_value = "")]
        policy: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Local donor contribution controls (pause, cap, schedule, thermal/battery).
    Donor {
        #[command(subcommand)]
        cmd: DonorCmd,
    },
    /// Live cluster capacity from a running control plane (or synthetic lab peers).
    Capacity {
        /// HTTP base of control plane (e.g. http://127.0.0.1:7700). Empty = synthetic.
        #[arg(long, default_value = "")]
        api: String,
        /// Synthetic peers when --api is empty.
        #[arg(long, default_value_t = 5)]
        peers: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Call OpenAI-compatible chat (requires donating agent for the key's account).
    Chat {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        /// API key (default: auto from identity after agent joins).
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long, default_value = CLUSTER_MODEL)]
        model: String,
        #[arg(long)]
        prompt: String,
        /// Request SSE stream (prints token chunks).
        #[arg(long, default_value_t = false)]
        stream: bool,
    },
    /// Show account balance / donating status.
    Whoami {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        /// API key (default: auto from identity after agent joins).
        #[arg(long, default_value = "")]
        key: String,
    },
    /// Show Base URL + API key + model for Cursor / Continue / any OpenAI app.
    /// This is how normal people get the generated key (copy / open file).
    Connect {
        /// Control HTTP base (not /v1 — we append it).
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        #[arg(long, default_value = CLUSTER_MODEL)]
        model: String,
        /// Copy the full API key to the clipboard.
        #[arg(long, default_value_t = false)]
        copy: bool,
        /// Copy the OpenAI Base URL (…/v1) to the clipboard.
        #[arg(long, default_value_t = false)]
        copy_url: bool,
        /// Open JOULE-CONNECT.txt in the OS default app.
        #[arg(long, default_value_t = false)]
        open: bool,
    },
    /// Live client status (connection, API, millijoules, tokens, pool) — all platforms.
    Status {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        /// Optional API key for account balance / tokens (default: identity cache).
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long, default_value_t = false)]
        json: bool,
        /// Compact monitor dash instead of multi-line status.
        #[arg(long, default_value_t = false)]
        dash: bool,
    },
    /// Continuous monitor dash (refreshes). Same fields as status; all platforms.
    Monitor {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long, default_value_t = 3)]
        interval_secs: u64,
    },
    /// Systray / tray-mode status surface (polls control; headless-safe monitor).
    /// Also the product surface for identity: CODE, enter CODE, open recovery.
    Tray {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long, default_value_t = 5)]
        interval_secs: u64,
        /// Show identity CODE / recovery once then exit (onboard).
        #[arg(long, default_value_t = false)]
        onboard: bool,
        /// Copy CODE to clipboard (platform tools) then exit.
        #[arg(long, default_value_t = false)]
        copy_code: bool,
        /// Enter CODE to link this machine (same as `identity use`).
        #[arg(long, default_value = "")]
        enter_code: String,
        /// Open JOULE-RECOVERY.txt with the OS default opener.
        #[arg(long, default_value_t = false)]
        open_recovery: bool,
        /// Show CONNECT card (Base URL + API key + model) then exit.
        #[arg(long, default_value_t = false)]
        connect: bool,
        /// Copy pool API key (for Cursor etc.) then exit.
        #[arg(long, default_value_t = false)]
        copy_api_key: bool,
        /// Pause local contribution (same as `joule donor pause`).
        #[arg(long, default_value_t = false)]
        donor_pause: bool,
        /// Resume local contribution.
        #[arg(long, default_value_t = false)]
        donor_resume: bool,
        /// Set local mem cap MiB then exit (`--donor-set-cap 0` clears the cap).
        #[arg(long)]
        donor_set_cap: Option<u32>,
        /// Print local donor policy + sensors then exit.
        #[arg(long, default_value_t = false)]
        donor_status: bool,
        /// Policy file for donor tray actions (default: ~/.config/joule/donor-policy.json).
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// First-run onboard: create CODE, write recovery file, show instructions once.
    Onboard {
        #[arg(long, default_value = "")]
        identity: String,
    },
    /// Generate OS service install artifacts (systemd / launchd / Windows task).
    Service {
        #[command(subcommand)]
        cmd: ServiceCmd,
    },
    /// Local offline lab (no network).
    Lab {
        #[arg(long, default_value = CLUSTER_MODEL)]
        model: String,
        #[arg(long, default_value = "status report from the cluster")]
        prompt: String,
        #[arg(long, default_value_t = true)]
        pipeline: bool,
        #[arg(long, default_value_t = 2)]
        stages: usize,
        #[arg(long, default_value_t = 3)]
        peers: usize,
    },
    /// Offline ledger demo.
    Credits {
        #[arg(long, default_value = "donor")]
        account: String,
    },
    /// Show model pool-readiness, milestones, and countdown.
    Ready {
        /// Optional live control HTTP base (e.g. http://127.0.0.1:7700).
        #[arg(long, default_value = "")]
        api: String,
        /// Override pool VRAM GiB when offline.
        #[arg(long, default_value_t = 0)]
        pool_vram_gib: u64,
        #[arg(long, default_value_t = 0)]
        backends: u32,
    },
    /// Load model weights into this process (from local weight cache).
    Load {
        #[arg(long, default_value = CLUSTER_MODEL)]
        model: String,
        /// Quant id (default: best fit for --mem-mib).
        #[arg(long, default_value = "")]
        quant: String,
        #[arg(long, default_value_t = 8192)]
        mem_mib: u32,
    },
    /// Operator broadcast bus (sign / inject / plan chunk placement).
    Broadcast {
        #[command(subcommand)]
        cmd: BroadcastCmd,
    },
    /// Seed a local file into the content-addressed blob store (for peer swarm).
    SeedBlob {
        /// File to hash and store under blobs/sha256/<hex>.
        #[arg(long)]
        path: PathBuf,
        /// Optional kind label for BlobsHave (weight|software|fixture).
        #[arg(long, default_value = "blob")]
        kind: String,
        /// Optional human name.
        #[arg(long, default_value = "")]
        name: String,
    },
    /// List local content-addressed blobs (what this machine can seed).
    Blobs {
        #[arg(long, default_value_t = false)]
        json: bool,
        /// Optional live control to show swarm catalog instead of local only.
        #[arg(long, default_value = "")]
        api: String,
    },
    /// Software update stage / apply (peer-seeded binaries only).
    Software {
        #[command(subcommand)]
        cmd: SoftwareCmd,
    },
}

#[derive(Subcommand, Debug)]
enum IdentityCmd {
    /// Show your joule code (auto-creates one if missing).
    Show {
        #[arg(long, default_value = "")]
        path: String,
        /// Also copy CODE to the system clipboard when possible.
        #[arg(long, default_value_t = false)]
        copy: bool,
    },
    /// Link this machine to an existing code (other PC already has millijoules).
    Use {
        /// UUID joule code, e.g. 550e8400-e29b-41d4-a716-446655440000
        code: String,
        #[arg(long, default_value = "")]
        path: String,
    },
    /// Alias for `use` (product wording: enter your code).
    Enter {
        code: String,
        #[arg(long, default_value = "")]
        path: String,
    },
    /// Open JOULE-RECOVERY.txt (OS default app / $EDITOR).
    OpenRecovery {
        #[arg(long, default_value = "")]
        path: String,
    },
    /// Create a new random code (only if you want to rotate — loses old link).
    New {
        #[arg(long, default_value = "")]
        path: String,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Advanced: write identity JSON to a file.
    Export {
        #[arg(long, default_value = "")]
        path: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Advanced: load identity JSON from a file.
    Import {
        #[arg(long)]
        from: PathBuf,
        #[arg(long, default_value = "")]
        path: String,
    },
}

#[derive(Subcommand, Debug)]
enum SoftwareCmd {
    /// Show staged software binary (if any).
    Status,
    /// Apply staged binary over a destination path (default: current executable).
    Apply {
        #[arg(long)]
        dest: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum DonorCmd {
    /// Show local donor policy (pause/cap/schedule/thermal/battery).
    Status {
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// Pause contribution (local only).
    Pause {
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// Resume contribution.
    Resume {
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// Set local VRAM claim hard cap (MiB). 0 clears cap.
    SetCap {
        mib: u32,
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// Set UTC schedule window HH:MM-HH:MM (empty clears).
    SetSchedule {
        #[arg(default_value = "")]
        window: String,
        #[arg(long, default_value = "")]
        policy: String,
    },
    /// Set max temp °C (0 clears) and/or min battery % (0 clears).
    SetSensors {
        #[arg(long, default_value_t = 0.0)]
        max_temp_c: f32,
        #[arg(long, default_value_t = 0.0)]
        min_battery_pct: f32,
        #[arg(long, default_value = "")]
        policy: String,
    },
}

#[derive(Subcommand, Debug)]
enum ServiceCmd {
    /// Write unit/plist/task XML to stdout or --out.
    Generate {
        /// linux | macos | windows
        #[arg(long, default_value = "linux")]
        platform: String,
        /// agent | tray
        #[arg(long, default_value = "agent")]
        kind: String,
        #[arg(long, default_value = "joule")]
        binary: String,
        #[arg(long, default_value = "127.0.0.1:7701")]
        control: String,
        #[arg(long, default_value = "donor")]
        account: String,
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long, default_value_t = 8192)]
        mem_mib: u32,
        /// Linux: user unit (true) vs system unit.
        #[arg(long, default_value_t = true)]
        user: bool,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Print minimal install steps for the current OS helpers.
    InstallHelp {
        #[arg(long, default_value = "linux")]
        platform: String,
    },
}

#[derive(Subcommand, Debug)]
enum BroadcastCmd {
    /// Generate operator ed25519 keypair (sec not for git).
    Keygen {
        #[arg(long, default_value = "operator.ed25519.sec")]
        secret: PathBuf,
        #[arg(long, default_value = "operator.ed25519.pub")]
        public: PathBuf,
    },
    /// Sign a body JSON file into a SignedEnvelope.
    Sign {
        /// notice | software_update | model_update | policy | …
        #[arg(long, default_value = "notice")]
        kind: String,
        /// Path to JSON body.
        #[arg(long)]
        body: PathBuf,
        /// Secret key file (32-byte hex).
        #[arg(long)]
        secret: PathBuf,
        /// Write envelope JSON here (default stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Inject a signed envelope into a running control (floods agents).
    Inject {
        #[arg(long, default_value = "http://127.0.0.1:7700")]
        api: String,
        /// Path to SignedEnvelope JSON from `broadcast sign`.
        #[arg(long)]
        envelope: PathBuf,
    },
    /// Demo: print redundant chunk plan (who stores which digests — not full model).
    PlanChunks {
        /// Number of synthetic chunks.
        #[arg(long, default_value_t = 12)]
        chunks: u32,
        /// Number of donor nodes.
        #[arg(long, default_value_t = 5)]
        nodes: u32,
        /// Replicas per chunk (overlap).
        #[arg(long, default_value_t = 2)]
        replicas: u32,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("joule=info".parse()?))
        .init();

    let cli = Cli::parse();
    let cmd = cli.cmd.unwrap_or(Commands::Gui {
        api: "http://127.0.0.1:7700".into(),
    });
    match cmd {
        Commands::Gui { api } => {
            // GUI is sync/eframe; leave async runtime after exit.
            gui::run_gui(api)?;
        }
        Commands::Version => {
            println!("joule {}", env!("CARGO_PKG_VERSION"));
            println!("protocol {}", joule_proto::PROTOCOL_VERSION);
            println!("distributed compute cluster");
            println!("cluster model {}", CLUSTER_MODEL);
        }
        Commands::Control {
            agent_listen,
            http_listen,
            data_dir,
            ephemeral,
        } => {
            let dir = if ephemeral {
                None
            } else {
                Some(data_dir.unwrap_or_else(joule_control::default_data_dir))
            };
            let app = joule_control::load_or_init_app(dir.clone())?;
            info!(%agent_listen, %http_listen, ?dir, "starting control plane");
            println!("joule control");
            println!("  agents    → {agent_listen}");
            println!("  http      → http://{http_listen}");
            println!("  dashboard → http://{http_listen}/");
            println!("  healthz   → http://{http_listen}/healthz");
            println!("  capacity  GET /v1/cluster/capacity");
            println!("  chat      POST /v1/chat/completions");
            if let Some(d) = &dir {
                println!("  data      → {}", d.display());
            } else {
                println!("  data      → (ephemeral)");
            }
            println!();
            println!("  open the dashboard, then run:");
            println!("    joule agent --control {agent_listen}");
            println!("  (auto joule code · other PCs: joule identity use <code>)");
            joule_control::serve(app, agent_listen, http_listen).await?;
        }
        Commands::Identity { cmd } => {
            match cmd {
                IdentityCmd::Show { path, copy } => {
                    let p = identity_path_arg(&path);
                    let (id, fresh) = identity::load_or_init(&p)?;
                    identity::print_code_banner(&id, &p, fresh);
                    if copy {
                        match tray_app::copy_to_clipboard(id.code()) {
                            Ok(()) => println!("CODE copied to clipboard."),
                            Err(e) => eprintln!("clipboard: {e} (code still printed above)"),
                        }
                    }
                }
                IdentityCmd::Use { code, path } | IdentityCmd::Enter { code, path } => {
                    let p = identity_path_arg(&path);
                    let id = identity::use_code(&p, &code)?;
                    println!("this machine is now linked to your joule code.");
                    identity::print_code_banner(&id, &p, false);
                }
                IdentityCmd::OpenRecovery { path } => {
                    let p = identity_path_arg(&path);
                    let (id, _) = identity::load_or_init(&p)?;
                    let note = identity::write_recovery_note(&p, &id)?;
                    tray_app::open_path(&note)?;
                    println!("opened {}", note.display());
                }
                IdentityCmd::New { path, force } => {
                    let p = identity_path_arg(&path);
                    if p.is_file() && !force {
                        let id = identity::load(&p)?;
                        println!("already have a code — showing it (use --force for a NEW empty account):");
                        identity::print_code_banner(&id, &p, false);
                    } else {
                        let id = identity::Identity::generate();
                        identity::save(&p, &id)?;
                        identity::print_code_banner(&id, &p, true);
                    }
                }
                IdentityCmd::Export { path, out } => {
                    let p = identity_path_arg(&path);
                    let id = identity::load(&p)?;
                    let raw = serde_json::to_string_pretty(&id)?;
                    if let Some(dest) = out {
                        std::fs::write(&dest, format!("{raw}\n"))?;
                        println!("exported {}", dest.display());
                    } else {
                        println!("{raw}");
                    }
                }
                IdentityCmd::Import { from, path } => {
                    let dest = identity_path_arg(&path);
                    let id = identity::load(&from)?;
                    identity::save(&dest, &id)?;
                    identity::print_code_banner(&id, &dest, false);
                }
            }
        }
        Commands::Onboard { identity: id_flag } => {
            let p = identity_path_arg(&id_flag);
            let (id, fresh) = identity::load_or_init(&p)?;
            // Product onboard: always show recovery once via banner + recovery file.
            identity::print_code_banner(&id, &p, fresh || true);
            let marker = p
                .parent()
                .map(|d| d.join("onboarded"))
                .unwrap_or_else(|| PathBuf::from("onboarded"));
            let _ = std::fs::write(&marker, format!("{}\n", id.code()));
            println!("onboard complete — keep JOULE-RECOVERY.txt safe.");
        }
        Commands::Agent {
            control,
            code,
            account,
            identity: identity_flag,
            model,
            mem_mib,
            device,
            heartbeat_secs,
            peer_listen,
            pause,
            mem_cap_mib,
            max_temp_c,
            min_battery_pct,
            schedule,
            policy,
            config,
        } => {
            let _ = config;
            let id_path = identity_path_arg(&identity_flag);
            let code_opt = if code.trim().is_empty() {
                None
            } else {
                Some(code.as_str())
            };
            let explicit = if account.trim().is_empty() {
                None
            } else {
                Some(account.as_str())
            };
            let (ident, fresh) = identity::resolve_account(code_opt, explicit, &id_path)?;
            if !ident.recovery_code.is_empty() {
                identity::print_code_banner(&ident, &id_path, fresh);
            } else {
                println!("lab account nickname: {}", ident.account_id);
            }
            let policy_path = if policy.trim().is_empty() {
                donor_policy::DonorPolicy::default_path()
            } else {
                PathBuf::from(&policy)
            };
            let mut pol = donor_policy::DonorPolicy::load(&policy_path).unwrap_or_default();
            if pause {
                pol.paused = true;
            }
            if mem_cap_mib > 0 {
                pol.mem_cap_mib = Some(mem_cap_mib);
            }
            if max_temp_c > 0.0 {
                pol.max_temp_c = Some(max_temp_c);
            }
            if min_battery_pct > 0.0 {
                pol.min_battery_pct = Some(min_battery_pct);
            }
            if !schedule.trim().is_empty() {
                pol.schedule = Some(parse_schedule_window(&schedule)?);
            }
            let _ = pol.save(&policy_path);
            run_agent(
                control,
                ident,
                model,
                mem_mib,
                device,
                heartbeat_secs,
                peer_listen,
                Some(id_path),
                pol,
                policy_path,
            )
            .await?;
        }
        Commands::Donor { cmd } => {
            run_donor_cmd(cmd)?;
        }
        Commands::Capacity { api, peers, json } => {
            run_capacity(api, peers, json).await?;
        }
        Commands::Chat {
            api,
            key,
            model,
            prompt,
            stream,
        } => {
            let key = resolve_api_key(&key)?;
            run_chat(api, key, model, prompt, stream).await?;
        }
        Commands::Whoami { api, key } => {
            let key = resolve_api_key(&key)?;
            run_whoami(api, key).await?;
        }
        Commands::Connect {
            api,
            model,
            copy,
            copy_url,
            open,
        } => {
            run_connect(&api, &model, copy, copy_url, open)?;
        }
        Commands::Status {
            api,
            key,
            json,
            dash,
        } => {
            let key = resolve_api_key_optional(&key);
            run_status(api, key.unwrap_or_default(), json, dash).await?;
        }
        Commands::Monitor {
            api,
            key,
            interval_secs,
        } => {
            let k = resolve_api_key_optional(&key);
            tray_app::run_tray(api, k, interval_secs).await?;
        }
        Commands::Tray {
            api,
            key,
            interval_secs,
            onboard,
            copy_code,
            enter_code,
            open_recovery,
            connect,
            copy_api_key,
            donor_pause,
            donor_resume,
            donor_set_cap,
            donor_status,
            policy,
        } => {
            let id_path = identity::default_path();
            let policy_path = policy_path_arg(&policy);
            if donor_status {
                run_donor_cmd(DonorCmd::Status {
                    policy: policy_path.display().to_string(),
                })?;
                return Ok(());
            }
            if donor_pause {
                run_donor_cmd(DonorCmd::Pause {
                    policy: policy_path.display().to_string(),
                })?;
                return Ok(());
            }
            if donor_resume {
                run_donor_cmd(DonorCmd::Resume {
                    policy: policy_path.display().to_string(),
                })?;
                return Ok(());
            }
            if let Some(mib) = donor_set_cap {
                // Explicit flag: 0 clears cap (product: tray --donor-set-cap 0).
                run_donor_cmd(DonorCmd::SetCap {
                    mib,
                    policy: policy_path.display().to_string(),
                })?;
                return Ok(());
            }
            if onboard {
                let (id, fresh) = identity::load_or_init(&id_path)?;
                identity::print_code_banner(&id, &id_path, fresh || true);
                return Ok(());
            }
            if copy_code {
                let (id, _) = identity::load_or_init(&id_path)?;
                identity::print_code_banner(&id, &id_path, false);
                match tray_app::copy_to_clipboard(id.code()) {
                    Ok(()) => println!("CODE copied to clipboard."),
                    Err(e) => eprintln!("clipboard: {e} (code still printed above)"),
                }
                return Ok(());
            }
            if !enter_code.trim().is_empty() {
                let id = identity::use_code(&id_path, &enter_code)?;
                identity::print_code_banner(&id, &id_path, false);
                return Ok(());
            }
            if open_recovery {
                let (id, _) = identity::load_or_init(&id_path)?;
                let note = identity::write_recovery_note(&id_path, &id)?;
                tray_app::open_path(&note)?;
                println!("opened {}", note.display());
                return Ok(());
            }
            if connect || copy_api_key {
                // --connect shows card; --copy-api-key copies key (both allowed).
                run_connect(&api, CLUSTER_MODEL, copy_api_key, false, false)?;
                return Ok(());
            }
            // Product tray: show identity once at start, then status monitor.
            if let Ok((id, fresh)) = identity::load_or_init(&id_path) {
                if fresh {
                    identity::print_code_banner(&id, &id_path, true);
                } else {
                    println!(
                        "joule CODE {}  ·  connect: joule connect | tray --copy-api-key",
                        id.code()
                    );
                }
            }
            let k = resolve_api_key_optional(&key);
            tray_app::run_tray(api, k, interval_secs).await?;
        }
        Commands::Service { cmd } => match cmd {
            ServiceCmd::Generate {
                platform,
                kind,
                binary,
                control,
                account,
                api,
                key,
                mem_mib,
                user,
                out,
            } => {
                run_service_generate(
                    platform, kind, binary, control, account, api, key, mem_mib, user, out,
                )?;
            }
            ServiceCmd::InstallHelp { platform } => {
                run_service_install_help(&platform)?;
            }
        },
        Commands::Lab {
            model,
            prompt,
            pipeline,
            stages,
            peers,
        } => {
            run_lab(model, prompt, pipeline, stages, peers).await?;
        }
        Commands::Credits { account } => {
            run_credits(&account)?;
        }
        Commands::Ready {
            api,
            pool_vram_gib,
            backends,
        } => {
            run_ready(api, pool_vram_gib, backends).await?;
        }
        Commands::Load {
            model,
            quant,
            mem_mib,
        } => {
            run_load(model, quant, mem_mib)?;
        }
        Commands::Broadcast { cmd } => match cmd {
            BroadcastCmd::Keygen { secret, public } => broadcast_keygen(secret, public)?,
            BroadcastCmd::Sign {
                kind,
                body,
                secret,
                out,
            } => broadcast_sign(kind, body, secret, out)?,
            BroadcastCmd::Inject { api, envelope } => {
                broadcast_inject(api, envelope).await?;
            }
            BroadcastCmd::PlanChunks {
                chunks,
                nodes,
                replicas,
                json,
            } => broadcast_plan_chunks(chunks, nodes, replicas, json)?,
        },
        Commands::SeedBlob { path, kind, name } => seed_blob(path, kind, name)?,
        Commands::Blobs { json, api } => run_blobs(json, api).await?,
        Commands::Software { cmd } => match cmd {
            SoftwareCmd::Status => software_status()?,
            SoftwareCmd::Apply { dest } => software_apply(dest)?,
        },
    }
    Ok(())
}

async fn run_blobs(json: bool, api: String) -> Result<()> {
    if !api.trim().is_empty() {
        let url = format!("{}/v1/blobs", api.trim_end_matches('/'));
        let text = reqwest::get(&url)
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .text()
            .await?;
        println!("{text}");
        return Ok(());
    }
    let list = WeightsStore::list_blob_store();
    if json {
        println!("{}", serde_json::to_string_pretty(&list)?);
    } else if list.is_empty() {
        println!(
            "no local blobs under {}",
            WeightsStore::blob_root().display()
        );
        println!("seed with: joule seed-blob --path FILE");
    } else {
        println!(
            "local blobs ({}): {}",
            list.len(),
            WeightsStore::blob_root().display()
        );
        for b in list {
            println!("  {}  {:>10}  {}  {}", b.sha256, b.size, b.kind, b.name);
        }
    }
    Ok(())
}

fn seed_blob(path: PathBuf, kind: String, name: String) -> Result<()> {
    use sha2::{Digest, Sha256};
    let data = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let hash = hex::encode(Sha256::digest(&data));
    WeightsStore::store_blob(&hash, &data).map_err(|e| anyhow::anyhow!(e))?;
    let display_name = if name.is_empty() {
        path.file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "blob".into())
    } else {
        name
    };
    println!("seeded sha256={hash}");
    println!("size={} kind={kind} name={display_name}", data.len());
    println!("blob path={}", WeightsStore::blob_path(&hash).display());
    println!("start `joule agent` so this node announces BlobsHave to the swarm");
    Ok(())
}

fn software_status() -> Result<()> {
    match read_stage() {
        Some(st) => {
            println!("{}", serde_json::to_string_pretty(&st)?);
        }
        None => {
            println!(r#"{{"staged":false,"message":"nothing staged"}}"#);
        }
    }
    Ok(())
}

fn software_apply(dest: Option<PathBuf>) -> Result<()> {
    let dest = match dest {
        Some(p) => p,
        None => std::env::current_exe().context("current_exe")?,
    };
    let st = apply_staged(&dest).map_err(|e| anyhow::anyhow!(e))?;
    println!("{}", serde_json::to_string_pretty(&st)?);
    Ok(())
}

fn broadcast_keygen(secret: PathBuf, public: PathBuf) -> Result<()> {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    let sk = SigningKey::generate(&mut OsRng);
    let sec_hex = hex::encode(sk.to_bytes());
    let pub_hex = hex::encode(sk.verifying_key().to_bytes());
    std::fs::write(&secret, format!("{sec_hex}\n"))?;
    std::fs::write(
        &public,
        format!(
            "# joule operator ed25519 public key (LAB / unofficial only)\n\
             # Stock builds verify the embedded official pin — this key is ignored unless:\n\
             #   JOULE_ALLOW_UNOFFICIAL_OPERATOR=1\n\
             #   JOULE_OPERATOR_PUBKEY={pub_hex}\n\
             {pub_hex}\n"
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600));
    }
    println!("wrote secret {}", secret.display());
    println!("wrote public {}", public.display());
    println!("LAB ONLY (forks / local experiments — not production):");
    println!("  export JOULE_ALLOW_UNOFFICIAL_OPERATOR=1");
    println!("  export JOULE_OPERATOR_PUBKEY={pub_hex}");
    println!(
        "Official network: use embedded pin + ~/.config/f00/joule/protocol.ed25519.sec to sign"
    );
    println!("(never commit the secret file)");
    Ok(())
}

fn parse_operator_kind(s: &str) -> Result<OperatorKind> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "notice" => OperatorKind::Notice,
        "software_update" | "update" => OperatorKind::SoftwareUpdate,
        "model_update" | "model" => OperatorKind::ModelUpdate,
        "policy" => OperatorKind::Policy,
        "pause_service" | "pause" => OperatorKind::PauseService,
        "resume_service" | "resume" => OperatorKind::ResumeService,
        "revoke" => OperatorKind::Revoke,
        other => bail!("unknown kind {other}"),
    })
}

fn broadcast_sign(
    kind: String,
    body: PathBuf,
    secret: PathBuf,
    out: Option<PathBuf>,
) -> Result<()> {
    use ed25519_dalek::{Signer, SigningKey};
    let kind = parse_operator_kind(&kind)?;
    let body_json = std::fs::read_to_string(&body)?;
    let sec_hex = std::fs::read_to_string(&secret)?;
    let sec_bytes = hex::decode(sec_hex.trim()).context("secret key hex")?;
    anyhow::ensure!(sec_bytes.len() == 32, "secret must be 32 bytes");
    let mut sb = [0u8; 32];
    sb.copy_from_slice(&sec_bytes);
    let sk = SigningKey::from_bytes(&sb);
    let mut env = SignedEnvelope {
        id: uuid::Uuid::new_v4(),
        issued_at_unix_ms: now_ms(),
        expires_at_unix_ms: None,
        kind,
        body_sha256: body_sha256_hex(body_json.trim()),
        body_json: body_json.trim().to_string(),
        sig_ed25519_hex: String::new(),
        openpgp_sig: None,
    };
    let pre = operator_preimage(&env);
    env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
    let text = serde_json::to_string_pretty(&env)?;
    if let Some(p) = out {
        std::fs::write(p, format!("{text}\n"))?;
    } else {
        println!("{text}");
    }
    Ok(())
}

async fn broadcast_inject(api: String, envelope: PathBuf) -> Result<()> {
    let raw = std::fs::read_to_string(&envelope)?;
    let env: SignedEnvelope = serde_json::from_str(&raw)?;
    let base = api.trim_end_matches('/');
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/broadcasts/inject"))
        .json(&env)
        .send()
        .await?
        .error_for_status()?;
    let v: serde_json::Value = resp.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}

fn broadcast_plan_chunks(chunks: u32, nodes: u32, replicas: u32, json: bool) -> Result<()> {
    anyhow::ensure!(nodes >= 1 && chunks >= 1, "need nodes>=1 chunks>=1");
    let node_list: Vec<(NodeId, u32)> = (0..nodes)
        .map(|i| (NodeId::new(), 8192 + i * 1024))
        .collect();
    let ch: Vec<ModelChunk> = (0..chunks)
        .map(|i| ModelChunk {
            index: i,
            path: format!("chunk-{i:03}.safetensors"),
            sha256: format!("{:064x}", i as u64 + 1),
            size: 512 * 1024 * 1024, // 512 MiB illustrative
            layer_start: i * 10,
            layer_end: i * 10 + 9,
        })
        .collect();
    let plan = plan_redundant_chunks(&node_list, &ch, replicas).map_err(|e| anyhow::anyhow!(e))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&plan)?);
        return Ok(());
    }
    println!(
        "redundant chunk plan: {} chunks × {} nodes, replica_factor={}",
        plan.chunk_count, plan.node_count, plan.replica_factor
    );
    println!("(no node stores the full model; each chunk has overlapping holders)\n");
    for np in &plan.by_node {
        let prim = np
            .holds
            .iter()
            .filter(|h| matches!(h.role, joule_cluster::ChunkRole::Primary))
            .count();
        let rep = np.holds.len() - prim;
        println!(
            "node {}  mem≈{} MiB  holds {} chunks ({} primary + {} replica)  ~{:.1} GiB",
            np.node,
            np.verified_mem_mib,
            np.holds.len(),
            prim,
            rep,
            np.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }
    println!("\nIf any single node drops, every chunk still has a live holder (r≥2).");
    Ok(())
}

fn parse_device(s: &str) -> Result<DeviceClass> {
    match s.to_ascii_lowercase().as_str() {
        "gpu" => Ok(DeviceClass::Gpu),
        "metal" => Ok(DeviceClass::Metal),
        "cpu" => Ok(DeviceClass::Cpu),
        other => bail!("unknown device {other}; use gpu|metal|cpu"),
    }
}

fn identity_path_arg(flag: &str) -> PathBuf {
    if flag.trim().is_empty() {
        identity::default_path()
    } else {
        PathBuf::from(flag)
    }
}

fn parse_schedule_window(s: &str) -> Result<donor_policy::ScheduleWindow> {
    let s = s.trim();
    let (a, b) = s
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("schedule must be HH:MM-HH:MM"))?;
    fn parse_hm(t: &str) -> Result<u16> {
        let (h, m) = t
            .trim()
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("bad time {t}"))?;
        let h: u16 = h.parse().context("hour")?;
        let m: u16 = m.parse().context("minute")?;
        if h > 23 || m > 59 {
            bail!("time out of range");
        }
        Ok(h * 60 + m)
    }
    Ok(donor_policy::ScheduleWindow {
        start_min_utc: parse_hm(a)?,
        end_min_utc: parse_hm(b)?,
    })
}

fn policy_path_arg(flag: &str) -> PathBuf {
    if flag.trim().is_empty() {
        donor_policy::DonorPolicy::default_path()
    } else {
        PathBuf::from(flag)
    }
}

fn run_donor_cmd(cmd: DonorCmd) -> Result<()> {
    match cmd {
        DonorCmd::Status { policy } => {
            let path = policy_path_arg(&policy);
            let p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            let sensors = donor_policy::probe_sensors();
            let now = donor_policy::DonorPolicy::now_unix_secs();
            let donating = p.allows_donate(now, sensors);
            println!("donor policy: {}", path.display());
            println!("  paused:          {}", p.paused);
            println!(
                "  mem_cap_mib:     {}",
                p.mem_cap_mib
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "none".into())
            );
            println!(
                "  schedule:        {}",
                p.schedule
                    .as_ref()
                    .map(|w| {
                        format!(
                            "{:02}:{:02}-{:02}:{:02} local (offset {}s)",
                            w.start_min_utc / 60,
                            w.start_min_utc % 60,
                            w.end_min_utc / 60,
                            w.end_min_utc % 60,
                            donor_policy::system_utc_offset_secs()
                        )
                    })
                    .unwrap_or_else(|| "always".into())
            );
            println!(
                "  max_temp_c:      {}",
                p.max_temp_c
                    .map(|t| format!("{t}"))
                    .unwrap_or_else(|| "none".into())
            );
            println!(
                "  min_battery_pct: {}",
                p.min_battery_pct
                    .map(|b| format!("{b}"))
                    .unwrap_or_else(|| "none".into())
            );
            println!(
                "  sensors:         temp={:?} battery={:?} on_ac={:?}",
                sensors.temp_c, sensors.battery_pct, sensors.on_ac
            );
            println!(
                "  allows_donate:   {} (local policy; remote cannot override)",
                donating
            );
        }
        DonorCmd::Pause { policy } => {
            let path = policy_path_arg(&policy);
            let mut p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            p.paused = true;
            p.save(&path)?;
            println!("paused contribution → {}", path.display());
        }
        DonorCmd::Resume { policy } => {
            let path = policy_path_arg(&policy);
            let mut p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            p.paused = false;
            p.save(&path)?;
            println!("resumed contribution → {}", path.display());
        }
        DonorCmd::SetCap { mib, policy } => {
            let path = policy_path_arg(&policy);
            let mut p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            p.mem_cap_mib = if mib == 0 { None } else { Some(mib) };
            p.save(&path)?;
            println!("mem_cap_mib={:?} → {}", p.mem_cap_mib, path.display());
        }
        DonorCmd::SetSchedule { window, policy } => {
            let path = policy_path_arg(&policy);
            let mut p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            p.schedule = if window.trim().is_empty() {
                None
            } else {
                Some(parse_schedule_window(&window)?)
            };
            p.save(&path)?;
            println!("schedule updated → {}", path.display());
        }
        DonorCmd::SetSensors {
            max_temp_c,
            min_battery_pct,
            policy,
        } => {
            let path = policy_path_arg(&policy);
            let mut p = donor_policy::DonorPolicy::load(&path).unwrap_or_default();
            p.max_temp_c = if max_temp_c > 0.0 {
                Some(max_temp_c)
            } else {
                None
            };
            p.min_battery_pct = if min_battery_pct > 0.0 {
                Some(min_battery_pct)
            } else {
                None
            };
            p.save(&path)?;
            println!("sensor limits updated → {}", path.display());
        }
    }
    Ok(())
}

/// Reload donor policy from the path this agent was started with (`--policy` /
/// `JOULE_DONOR_POLICY` via `DonorPolicy::default_path`). Never silently switch
/// to a different default path mid-session.
pub(crate) fn reload_live_policy(
    policy_path: &Path,
    fallback: &donor_policy::DonorPolicy,
) -> donor_policy::DonorPolicy {
    donor_policy::DonorPolicy::load(policy_path).unwrap_or_else(|_| fallback.clone())
}

#[allow(clippy::too_many_arguments)]
async fn run_agent(
    control: String,
    ident: identity::Identity,
    model: String,
    mem_mib: u32,
    device: String,
    heartbeat_secs: u64,
    peer_listen: String,
    identity_path: Option<PathBuf>,
    policy: donor_policy::DonorPolicy,
    policy_path: PathBuf,
) -> Result<()> {
    let node_id = NodeId::new();
    let account = ident.account_id.clone();
    let _ = model; // single-model cluster; agents always donate to CLUSTER_MODEL
                   // Startup GPU probe: clamp advertised claim (mint/placement still use verified only).
    let probe = gpu_probe::probe_vram();
    let claim_mib = gpu_probe::clamp_claim(mem_mib, &probe);
    // Local donor cap (product law 5) — remote cannot raise this.
    let claim_mib = policy.effective_mem_mib(claim_mib);
    let device_s = gpu_probe::effective_device(&device, claim_mib);
    if claim_mib != mem_mib {
        println!(
            "gpu probe / local cap: requested {mem_mib} MiB → effective claim {claim_mib} MiB ({})",
            probe.detail
        );
    } else {
        println!(
            "gpu probe: {} · claim {claim_mib} MiB ({})",
            probe.backend, probe.detail
        );
    }
    println!("donor policy file: {}", policy_path.display());
    if policy.paused {
        println!("donor policy: PAUSED (local) — heartbeats will report unhealthy");
    }
    if let Some(cap) = policy.mem_cap_mib {
        println!("donor policy: mem_cap_mib={cap}");
    }
    let device = parse_device(device_s)?;
    let throughput_class = match device {
        DeviceClass::Gpu => 40,
        DeviceClass::Metal => 30,
        DeviceClass::Cpu => 5,
    };
    let caps = NodeCaps::for_cluster(device, claim_mib, throughput_class);
    let mem_mib = claim_mib;

    // ADR 0003: production agents use ProductionEngine (CUDA driver FFI + content-proof path).
    // Weight residency is shared via ProductionEngine::cluster_arc() for prepare/peer paths.
    let production_engine = std::sync::Arc::new(ProductionEngine::new());
    let cuda = production_engine.cuda();
    info!(
        available = cuda.available,
        devices = cuda.device_count,
        detail = cuda.detail,
        "production CUDA probe (ADR 0003)"
    );
    let shared_engine = production_engine.cluster_arc();
    // Peer listen for direct mesh dial (decentral Phase A/C) + peer-direct Infer*.
    let local_mesh: peer_net::SharedMesh =
        std::sync::Arc::new(tokio::sync::Mutex::new(peer_net::LocalMesh::new()));
    let mut multiaddrs: Vec<String> = Vec::new();
    let mut bootstrap_targets: Vec<String> = Vec::new();
    if !peer_listen.trim().is_empty() {
        let bind: SocketAddr = peer_listen
            .parse()
            .with_context(|| format!("parse --peer-listen {peer_listen}"))?;
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("bind peer listen {bind}"))?;
        let local = listener.local_addr()?;
        multiaddrs.push(peer_net::format_tcp_multiaddr(local));
        // Production internet donors: JOULE_PUBLIC_ADDR or quic dual-stack multiaddrs.
        for a in joule_net::advertise_public_multiaddrs(
            local,
            std::env::var("JOULE_PUBLIC_HOST").ok().as_deref(),
            true,
        ) {
            if !multiaddrs.contains(&a) {
                multiaddrs.push(a);
            }
        }
        println!("peer listen: {}", multiaddrs[0]);
        if multiaddrs.len() > 1 {
            println!("public multiaddrs: {}", multiaddrs[1..].join(", "));
        }
        let nid = node_id.clone();
        let mesh = local_mesh.clone();
        let eng = production_engine.clone();
        tokio::spawn(async move {
            if let Err(e) = peer_net::run_peer_listener(listener, nid, mesh, eng).await {
                warn!(error = %e, "peer listener exited");
            }
        });
    }
    // Phase C: optional bootstrap list (replaceable; not f00-only).
    if let Some(boot) = joule_dht::BootstrapList::load_default() {
        println!(
            "bootstrap: {} multiaddr(s) · {}",
            boot.multiaddrs.len(),
            if boot.comment.is_empty() {
                "no comment"
            } else {
                boot.comment.as_str()
            }
        );
        for a in boot.multiaddrs.iter().take(8) {
            println!("  bootstrap dial hint: {a}");
            bootstrap_targets.push(a.clone());
        }
        if !multiaddrs.is_empty() && !bootstrap_targets.is_empty() {
            let nid = node_id.clone();
            let ours = multiaddrs.clone();
            let targets = bootstrap_targets.clone();
            tokio::spawn(async move {
                peer_net::announce_to_peers(&targets, &nid, &ours, 0, None).await;
            });
        }
    }

    info!(%control, %account, %node_id, model = CLUSTER_MODEL, "connecting agent");
    let sock = match TcpStream::connect(&control).await {
        Ok(s) => s,
        Err(e) => {
            bail!(
                "cannot connect to control at {control}: {e}\n\n\
                 The control plane is not running (or wrong host/port).\n\
                 Normie path:  joule gui     # start panel, click Start control + Start agent\n\
                 CLI path:     joule control # terminal 1\n\
                               joule agent  # terminal 2 (this command)\n\
                 Default agent port is 127.0.0.1:7701 · HTTP dashboard http://127.0.0.1:7700/"
            );
        }
    };
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();

    // Signed Hello for j1… accounts so the whole pool accepts only key holders.
    // Device key: recovery identity when present; else random process key (not NodeId-derived).
    let device_sk = if !ident.recovery_code.is_empty() {
        ident.signing_key()?
    } else {
        ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng)
    };
    let hello_msg = if ident.recovery_code.is_empty() {
        Message::Hello {
            account: account.clone(),
            caps,
            pubkey_hex: hex::encode(device_sk.verifying_key().as_bytes()),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
        }
    } else {
        ident.signed_hello(&node_id, caps)?
    };
    let hello = Envelope::new(node_id.clone(), hello_msg);
    writer.write_all(&encode_line(&hello)?).await?;

    // Tensor-backed when weights load; production Engine for control infer/stage/challenge.
    let engine = shared_engine;
    let production = production_engine;
    let store = WeightsStore::new(WeightsStore::default_root());
    store.ensure_root().ok();
    let mut last_armed = false;
    let mut pending_recv: HashMap<Uuid, PendingBlobRecv> = HashMap::new();
    // When software_update matches this host, stage once the digest lands.
    let mut pending_software: Option<(String, SoftwareTarget)> = None;
    // Unique mesh neighbors seen via gossip (Phase A).
    let mut mesh_seen: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
    // Pool PlanOffer verifying key from Welcome (fail-closed admit).
    let mut pool_offer_pubkey: Option<String> = None;
    // Agents never self-attest verified capacity on PeerAlive (always 0).
    // Control cluster verified is the only mint/placement authority for weighted geometry.
    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs.max(1)));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                // Reload **this agent's** policy path so `joule donor pause --policy <same>` works.
                let live_policy = reload_live_policy(&policy_path, &policy);
                let sensors = donor_policy::probe_sensors();
                let donating = live_policy.allows_donate(
                    donor_policy::DonorPolicy::now_unix_secs(),
                    sensors,
                );
                let offer_mem = if donating {
                    live_policy.effective_mem_mib(mem_mib)
                } else {
                    0
                };
                let hb = Envelope::new(
                    node_id.clone(),
                    Message::Heartbeat { load: 0.1, healthy: donating },
                );
                if writer.write_all(&encode_line(&hb)?).await.is_err() {
                    bail!("control connection closed during heartbeat");
                }
                // Refresh mesh presence (multiaddrs for direct blob seed).
                if !multiaddrs.is_empty() {
                    let blob_count = WeightsStore::list_blob_store().len() as u32;
                    let alive = Envelope::new(
                        node_id.clone(),
                        Message::PeerAlive {
                            multiaddrs: multiaddrs.clone(),
                            load: 0.1,
                            healthy: donating,
                            blob_count,
                            mem_mib: offer_mem, // claim for UI; 0 when local pause/cap schedule
                            // Never self-attest: peers must not treat this as control unlock.
                            verified_mem_mib: 0,
                            throughput_class,
                        },
                    );
                    let _ = writer.write_all(&encode_line(&alive)?).await;
                }
            }
            line = lines.next_line() => {
                let Some(line) = line? else {
                    bail!("control closed connection");
                };
                if line.trim().is_empty() {
                    continue;
                }
                let env = decode_line(line.as_bytes()).context("decode control line")?;
                match env.msg {
                    Message::Welcome { account: acc, api_key: key, pool_pubkey_hex } => {
                        println!("joined pool · account {acc}");
                        if !pool_pubkey_hex.is_empty() {
                            pool_offer_pubkey = Some(pool_pubkey_hex.to_ascii_lowercase());
                            info!(
                                pool_pk = %pool_pubkey_hex.chars().take(16).collect::<String>(),
                                "pool PlanOffer pubkey pinned from Welcome"
                            );
                        }
                        if let Some(ref ip) = identity_path {
                            if let Err(e) = identity::remember_api_key(ip, &key) {
                                warn!(error = %e, "could not cache api_key on identity");
                            }
                            // Idiot path: full CONNECT card + JOULE-CONNECT.txt for Cursor etc.
                            // HTTP default matches control default; agents join TCP, apps use HTTP.
                            let http = std::env::var("JOULE_HTTP_API")
                                .unwrap_or_else(|_| "http://127.0.0.1:7700".into());
                            identity::print_connect_card(ip, &http, Some(&key), CLUSTER_MODEL);
                        } else {
                            println!("API key (for Cursor / chat): {key}");
                            println!("chat: joule chat --prompt \"hello\"");
                        }
                        println!("other PC:  joule identity use <CODE from recovery file>");
                        println!("connect:   joule connect          # show/copy Base URL + key");
                        println!("weights:   {}", store.root().display());
                        if !multiaddrs.is_empty() {
                            println!("mesh dial:  {}", multiaddrs.join(", "));
                            let blob_count = WeightsStore::list_blob_store().len() as u32;
                            let live = reload_live_policy(&policy_path, &policy);
                            let sensors = donor_policy::probe_sensors();
                            let donating = live.allows_donate(
                                donor_policy::DonorPolicy::now_unix_secs(),
                                sensors,
                            );
                            let offer = if donating {
                                live.effective_mem_mib(mem_mib)
                            } else {
                                0
                            };
                            let alive = Envelope::new(
                                node_id.clone(),
                                Message::PeerAlive {
                                    multiaddrs: multiaddrs.clone(),
                                    load: 0.1,
                                    healthy: donating,
                                    blob_count,
                                    mem_mib: offer,
                                    verified_mem_mib: 0, // never self-attest
                                    throughput_class,
                                },
                            );
                            writer.write_all(&encode_line(&alive)?).await?;
                        }
                        // Seed swarm directory with any local content-addressed blobs.
                        if let Err(e) =
                            announce_local_blobs(&mut writer, &node_id, &store, &multiaddrs).await
                        {
                            warn!(error = %e, "initial BlobsHave failed");
                        }
                    }
                    Message::PeerAlive {
                        multiaddrs: peer_addrs,
                        load,
                        healthy,
                        blob_count,
                        mem_mib: peer_mem,
                        verified_mem_mib: peer_verified,
                        throughput_class: peer_tp,
                    } => {
                        if healthy {
                            mesh_seen.insert(env.from.clone());
                        } else {
                            mesh_seen.remove(&env.from);
                        }
                        {
                            let mut g = local_mesh.lock().await;
                            g.apply_peer_alive(
                                &env.from,
                                peer_addrs.clone(),
                                load,
                                healthy,
                                blob_count,
                                peer_mem,
                                peer_verified,
                                peer_tp,
                            );
                        }
                        // Re-announce ourselves to new peers (P2P mesh fill).
                        if !multiaddrs.is_empty() && !peer_addrs.is_empty() {
                            let nid = node_id.clone();
                            let ours = multiaddrs.clone();
                            let targets = peer_addrs.clone();
                            let bc = WeightsStore::list_blob_store().len() as u32;
                            tokio::spawn(async move {
                                peer_net::announce_to_peers(&targets, &nid, &ours, bc, None).await;
                            });
                        }
                        tracing::debug!(
                            blob_count,
                            mesh_peers = mesh_seen.len(),
                            "mesh PeerAlive (gossip)"
                        );
                    }
                    Message::RequestInfer {
                        request_id,
                        account: req_account,
                        model: req_model,
                        prompt: _,
                        max_tokens: _,
                    } => {
                        // Coordination signal only. **Do not** PlanOffer/PlanAccept here.
                        // Control (or peer-bus coordinator) owns geometry + plan_hash; emitting a
                        // self-built equal-unit plan hash poisons settle_plan_accept against the
                        // coordinator's want_hash and aborts multi-party mesh agreement.
                        // Agreement is only via PlanOffer → hashed PlanAccept.
                        let gossip_peers = {
                            let g = local_mesh.lock().await;
                            g.plan_donors().len()
                        };
                        info!(
                            %request_id,
                            %req_account,
                            %req_model,
                            gossip_peers,
                            equal_unit_mib = peer_net::LocalMesh::PEER_GOSSIP_UNIT_MIB,
                            "RequestInfer noted; waiting for PlanOffer"
                        );
                    }
                    Message::PlanOffer {
                        plan,
                        request_id: plan_req_id,
                        plan_hash_hex,
                        auth,
                    } => {
                        info!(
                            plan_id = %plan.plan_id,
                            %plan_req_id,
                            shards = plan.shards.len(),
                            pool_mem = plan.pool_mem_mib,
                            "received PlanOffer"
                        );
                        // Fail closed: PlanOffer must be signed by pool (Welcome pin) or mesh coordinator.
                        let offer_hash = if plan_hash_hex.is_empty() {
                            joule_cluster::plan_hash_hex(&plan)
                        } else {
                            plan_hash_hex.clone()
                        };
                        let offer_ok = if let Some(ref pool_pk) = pool_offer_pubkey {
                            let signer = auth.signer_pubkey_hex.trim().to_ascii_lowercase();
                            if signer != *pool_pk {
                                warn!(
                                    "PlanOffer rejected: signer not pool pubkey from Welcome"
                                );
                                false
                            } else if let Err(e) = joule_cluster::verify_plan_offer_sig(
                                &env.from,
                                plan.plan_id,
                                plan_req_id,
                                &offer_hash,
                                &auth.signer_pubkey_hex,
                                &auth.sig_hex,
                                auth.signed_at_unix_ms,
                            ) {
                                warn!(error = %e, "PlanOffer signature rejected");
                                false
                            } else {
                                true
                            }
                        } else if auth.signer_pubkey_hex.is_empty() || auth.sig_hex.is_empty() {
                            warn!("PlanOffer rejected: missing auth (no pool pin yet)");
                            false
                        } else if let Err(e) = joule_cluster::verify_plan_offer_sig(
                            &env.from,
                            plan.plan_id,
                            plan_req_id,
                            &offer_hash,
                            &auth.signer_pubkey_hex,
                            &auth.sig_hex,
                            auth.signed_at_unix_ms,
                        ) {
                            warn!(error = %e, "PlanOffer signature rejected");
                            false
                        } else {
                            true
                        };
                        if !offer_ok {
                            continue;
                        }
                        let accepted = plan.shards.iter().any(|s| s.node == node_id);
                        let (ph, confirm) = joule_cluster::plan_accept_fields(
                            &plan,
                            plan_req_id,
                            &node_id,
                            accepted,
                            Some(plan_hash_hex.as_str()).filter(|s| !s.is_empty()),
                        );
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let pre = joule_cluster::plan_accept_sign_preimage(
                            &node_id,
                            plan.plan_id,
                            plan_req_id,
                            accepted,
                            &ph,
                            &confirm,
                            ts,
                        );
                        let (pk, sig) = joule_cluster::sign_preimage(&device_sk, &pre);
                        let acc = Envelope::new(
                            node_id.clone(),
                            Message::PlanAccept {
                                plan_id: plan.plan_id,
                                request_id: plan_req_id,
                                accepted,
                                reason: if accepted {
                                    "shard assigned".into()
                                } else {
                                    "not in plan".into()
                                },
                                plan_hash_hex: ph,
                                confirm_hex: confirm,
                                auth: joule_proto::PlanAuth {
                                    signer_pubkey_hex: pk,
                                    sig_hex: sig,
                                    signed_at_unix_ms: ts,
                                },
                            },
                        );
                        writer.write_all(&encode_line(&acc)?).await?;
                        // Per-shard band-only load: when we own a layer band, prefer
                        // prepare_and_install_for_band (preferred files only) over full quant.
                        if accepted {
                            if let Some(shard) = plan.shards.iter().find(|s| s.node == node_id) {
                                if let (Some(ls), Some(le)) = (shard.layer_start, shard.layer_end) {
                                    if let Ok(m) = ManifestFile::load_default() {
                                        if let Some(spec) = m.model(CLUSTER_MODEL) {
                                            if let Some(q) = spec.pick_quant(mem_mib.max(256)) {
                                                production.begin_quant_intent(q);
                                                match prepare_and_install_for_band(
                                                    &store,
                                                    engine.as_ref(),
                                                    spec,
                                                    q,
                                                    ls,
                                                    le,
                                                ) {
                                                    Ok(report) => {
                                                        if let Err(e) = production
                                                            .mark_content_from_band(
                                                                &store,
                                                                &spec.id,
                                                                q,
                                                                ls,
                                                                le,
                                                            )
                                                        {
                                                            warn!(
                                                                error = %e,
                                                                quant = %q.id,
                                                                "band content gate refused — stage will fail closed"
                                                            );
                                                        } else {
                                                            info!(
                                                                layers = %format!("{ls}-{le}"),
                                                                tensors = report.tensors,
                                                                bytes = report.bytes_resident,
                                                                "band-only weights installed + production content marked"
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        info!(
                                                            error = %e,
                                                            "band-only prepare deferred; trying full quant"
                                                        );
                                                        match prepare_and_install(
                                                            &store,
                                                            engine.as_ref(),
                                                            spec,
                                                            q,
                                                        ) {
                                                            Ok(report) => {
                                                                if let Err(me) = production
                                                                    .mark_content_from_store(
                                                                        &store, q,
                                                                    )
                                                                {
                                                                    warn!(error = %me, "full quant content gate refused");
                                                                } else {
                                                                    info!(tensors = report.tensors, "full quant installed + marked");
                                                                }
                                                            }
                                                            Err(e2) => info!(error = %e2, "full prepare also deferred"),
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Message::PlanAccept {
                        plan_id,
                        request_id,
                        accepted,
                        reason,
                        ..
                    } => {
                        tracing::debug!(
                            %plan_id,
                            %request_id,
                            accepted,
                            %reason,
                            "PlanAccept"
                        );
                    }
                    Message::BlobLocate {
                        sha256,
                        peers,
                        sizes,
                        multiaddrs: locate_addrs,
                    } => {
                        info!(
                            %sha256,
                            seeders = peers.len(),
                            ?sizes,
                            "BlobLocate"
                        );
                        // Mirror locate into local DHT for future control-free fetches.
                        {
                            let mut g = local_mesh.lock().await;
                            for (i, peer) in peers.iter().enumerate() {
                                let addrs = locate_addrs.get(i).cloned().unwrap_or_default();
                                let size = sizes.get(i).copied().unwrap_or(0);
                                if !addrs.is_empty() {
                                    g.apply_peer_alive(
                                        peer,
                                        addrs.clone(),
                                        0.0,
                                        true,
                                        0,
                                        0, // claim
                                        0, // verified unknown from locate
                                        0,
                                    );
                                }
                                g.dht.put_blob_seeder(
                                    &sha256,
                                    &peer.to_string(),
                                    size,
                                    addrs,
                                );
                            }
                        }
                        // Phase B: try direct peer fetch before waiting on control relay chunks.
                        let mut tried_direct = false;
                        for addrs in &locate_addrs {
                            if addrs.is_empty() {
                                continue;
                            }
                            tried_direct = true;
                            match peer_net::fetch_blob_from_addrs(addrs, &sha256).await {
                                Ok(data) => {
                                    match WeightsStore::store_blob(&sha256, &data) {
                                        Ok(n) => {
                                            info!(%sha256, bytes = n, "direct peer blob OK");
                                            let _ = announce_local_blobs(
                                                &mut writer,
                                                &node_id,
                                                &store,
                                                &multiaddrs,
                                            )
                                            .await;
                                        }
                                        Err(e) => warn!(error = %e, "store direct blob failed"),
                                    }
                                    break;
                                }
                                Err(e) => {
                                    warn!(error = %e, "direct fetch failed; may use control relay")
                                }
                            }
                        }
                        if !tried_direct {
                            // Try local mesh DHT (may have learned seeders earlier).
                            if peer_net::try_fetch_from_local_mesh(&local_mesh, &sha256)
                                .await
                                .unwrap_or(false)
                            {
                                let _ = announce_local_blobs(
                                    &mut writer,
                                    &node_id,
                                    &store,
                                    &multiaddrs,
                                )
                                .await;
                            } else {
                                tracing::debug!(%sha256, "no multiaddrs yet; control BlobProvide path");
                            }
                        }
                    }
                    Message::PoolStatus {
                        pool_vram_mib,
                        backends,
                        pool_ready,
                        weights_published,
                        pool_progress_pct,
                        inference_mode,
                        message,
                        recommend_quant,
                    } => {
                        info!(
                            pool_ready,
                            weights_published,
                            pool_progress_pct,
                            %inference_mode,
                            pool_vram_gib = pool_vram_mib / 1024,
                            backends,
                            "{message}"
                        );
                        // Production: load the best available quant for this node's mem
                        // even when the pool is not yet "Kimi-ready" (lab-tiny/mid/large).
                        // Full Kimi remains gated by digests/service_live on control — not by
                        // leaving agents unloaded forever below the 64 GiB / 3-backend milestone.
                        if !last_armed {
                            if let Ok(manifest) = ManifestFile::load_default() {
                                if let Some(spec) = manifest.primary() {
                                    let quant = recommend_quant
                                        .as_deref()
                                        .and_then(|id| {
                                            spec.weights.quants.iter().find(|q| q.id == id)
                                        })
                                        .or_else(|| spec.pick_quant(mem_mib));
                                    if let Some(q) = quant {
                                        if !pool_ready {
                                            info!(
                                                quant = %q.id,
                                                "pool not milestone-ready; preparing lab/available quant for production path"
                                            );
                                        }
                                        // Prepare always (may arm while waiting for peer seeds).
                                        match store.prepare(spec, q) {
                                            Ok(st) => {
                                                last_armed = st.armed;
                                                println!(
                                                    "weights: {} ({})",
                                                    st.message, st.cache_dir
                                                );
                                                let ok = Envelope::new(
                                                    node_id.clone(),
                                                    Message::PrepareOk {
                                                        model: st.model.clone(),
                                                        quant: st.quant.clone(),
                                                        armed: st.armed,
                                                        files_complete: st.files_complete,
                                                        message: st.message.clone(),
                                                    },
                                                );
                                                writer.write_all(&encode_line(&ok)?).await?;
                                                // Seed directory: announce content we can share (no f00 CDN).
                                                let metas = store.local_blob_metas(&spec.id, q);
                                                if !metas.is_empty() {
                                                    let blobs: Vec<BlobMeta> = metas
                                                        .into_iter()
                                                        .map(|m| BlobMeta {
                                                            sha256: m.sha256,
                                                            size: m.size,
                                                            kind: m.kind,
                                                            name: m.name,
                                                            multiaddrs: multiaddrs.clone(),
                                                        })
                                                        .collect();
                                                    let have = Envelope::new(
                                                        node_id.clone(),
                                                        Message::BlobsHave { blobs },
                                                    );
                                                    writer
                                                        .write_all(&encode_line(&have)?)
                                                        .await?;
                                                }
                                            }
                                            Err(e) => warn!(error = %e, "prepare failed"),
                                        }
                                        // Shared load path (same as peer-seed completion).
                                        production.begin_quant_intent(q);
                                        match prepare_and_install(&store, engine.as_ref(), spec, q) {
                                            Ok(report) => {
                                                // ADR 0003: content-proof required before ModelLoaded honesty.
                                                match production.mark_content_from_store(&store, q) {
                                                    Ok(()) => {
                                                        last_armed = true;
                                                        println!("loaded: {}", report.message);
                                                        let loaded = Envelope::new(
                                                            node_id.clone(),
                                                            Message::ModelLoaded {
                                                                model: report.model,
                                                                quant: report.quant,
                                                                bytes_resident: report.bytes_resident,
                                                                tensors: report.tensors as u32,
                                                                message: report.message,
                                                            },
                                                        );
                                                        writer
                                                            .write_all(&encode_line(&loaded)?)
                                                            .await?;
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            error = %e,
                                                            quant = %q.id,
                                                            "content gate refused — not emitting ModelLoaded"
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                info!(error = %e, "model load deferred")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Message::InferRequest { .. } => {
                        // ADR 0003: ProductionEngine for infer (CUDA-tagged, content-proof path).
                        // After prepare_and_install, resident weights → require_band_weights.
                        let opts = joule_control::InferAgentOpts {
                            require_band_weights: production.has_resident_weights()
                                || production.is_model_loaded(),
                        };
                        let reply = joule_control::agent_handle_infer_with(
                            &env,
                            production.as_ref(),
                            opts,
                        )
                        .await
                        .context("handle infer")?;
                        let reply = Envelope::new(node_id.clone(), reply.msg);
                        writer.write_all(&encode_line(&reply)?).await?;
                    }
                    Message::Challenge { .. } => {
                        // Solve capacity proof only. Do **not** self-increment verified or
                        // advertise unlock on PeerAlive — only control cluster attestation
                        // after settle_challenge_result raises verified for mint/placement.
                        let reply =
                            joule_control::agent_handle_challenge(&env, production.as_ref())
                            .await
                            .context("handle challenge")?;
                        let reply = Envelope::new(node_id.clone(), reply.msg);
                        writer.write_all(&encode_line(&reply)?).await?;
                    }
                    Message::Error { error } => {
                        warn!(%error, "control error");
                    }
                    Message::CreditEvent { delta_millijoules, reason, .. } => {
                        info!(delta_millijoules, %reason, "credit event");
                    }
                    Message::OperatorBroadcast { envelope } => {
                        // Defense in depth: if operator key is pinned, verify even when
                        // the message arrived via control flood (control may be untrusted).
                        {
                            let expect = body_sha256_hex(&envelope.body_json);
                            if expect != envelope.body_sha256.to_lowercase()
                                && expect != envelope.body_sha256
                            {
                                warn!(id = %envelope.id, "reject operator broadcast (body_sha256)");
                                continue;
                            }
                        }
                        // Always verify against official embed (or lab override).
                        let pk = operator_pubkey_hex();
                        if let Err(e) = verify_operator_sig(&envelope, &pk) {
                            warn!(error = %e, id = %envelope.id, "reject operator broadcast (bad sig)");
                            continue;
                        }
                        info!(
                            id = %envelope.id,
                            kind = ?envelope.kind,
                            "operator broadcast"
                        );
                        println!(
                            "operator message: {:?} id={} body_sha256={}",
                            envelope.kind, envelope.id, envelope.body_sha256
                        );
                        // Append-only local journal (audit trail; not a second CDN).
                        if let Err(e) = append_broadcast_journal(&envelope) {
                            warn!(error = %e, "broadcast journal write failed");
                        }
                        // Allow-listed actions: notices print; model/software updates
                        // trigger peer-seed of digests only (never full-model force-download).
                        match envelope.kind {
                            OperatorKind::Notice => {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(
                                    &envelope.body_json,
                                ) {
                                    if let Some(t) = v.get("title").and_then(|x| x.as_str()) {
                                        println!("NOTICE: {t}");
                                    }
                                    if let Some(b) = v.get("body").and_then(|x| x.as_str()) {
                                        println!("{b}");
                                    }
                                }
                            }
                            OperatorKind::ModelUpdate => {
                                println!(
                                    "model_update: fetch only YOUR assigned chunk digests from peers"
                                );
                                println!(
                                    "(see joule broadcast plan-chunks — replica overlap, not full model)"
                                );
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(
                                    &envelope.body_json,
                                ) {
                                    if let Some(files) = v
                                        .pointer("/quants/0/files")
                                        .and_then(|x| x.as_array())
                                    {
                                        println!(
                                            "  announced files: {} (sha256 each; seed/swarm)",
                                            files.len()
                                        );
                                    }
                                }
                            }
                            OperatorKind::SoftwareUpdate => {
                                match parse_software_update(&envelope.body_json) {
                                    Ok(body) => {
                                        println!(
                                            "software_update v{} notes={}",
                                            body.version,
                                            if body.notes.is_empty() {
                                                "(none)"
                                            } else {
                                                body.notes.as_str()
                                            }
                                        );
                                        if let Some(t) = match_target(&body) {
                                            println!(
                                                "  match {}/{} sha256={} — peer seed only",
                                                t.os, t.arch, t.sha256
                                            );
                                            if WeightsStore::has_blob(&t.sha256) {
                                                match stage_blob(&body.version, t) {
                                                    Ok(st) => println!("  {}", st.message),
                                                    Err(e) => warn!(error = %e, "stage failed"),
                                                }
                                            } else {
                                                pending_software =
                                                    Some((body.version.clone(), t.clone()));
                                                let want = Envelope::new(
                                                    node_id.clone(),
                                                    Message::BlobWant {
                                                        sha256: t.sha256.to_lowercase(),
                                                    },
                                                );
                                                writer.write_all(&encode_line(&want)?).await?;
                                            }
                                        } else {
                                            println!(
                                                "  no target for this host ({}/{})",
                                                joule_runtime::current_os(),
                                                joule_runtime::current_arch()
                                            );
                                        }
                                    }
                                    Err(e) => warn!(error = %e, "software_update parse"),
                                }
                            }
                            OperatorKind::PauseService => {
                                println!("PAUSE: operator paused public service");
                            }
                            OperatorKind::ResumeService => {
                                println!("RESUME: operator resumed public service");
                            }
                            OperatorKind::Policy => {
                                println!("policy: {}", envelope.body_json);
                            }
                            _ => {
                                println!("  (stored/relayed; no local action for this kind)");
                            }
                        }
                    }
                    Message::FetchDigests {
                        digests,
                        reason,
                        replica_factor,
                    } => {
                        info!(
                            n = digests.len(),
                            %reason,
                            replica_factor,
                            "FetchDigests: obtain assigned chunk digests only"
                        );
                        for d in digests {
                            let hash = d.to_lowercase();
                            if WeightsStore::has_blob(&hash) {
                                continue;
                            }
                            // Phase C: try local mesh DHT before asking control.
                            if peer_net::try_fetch_from_local_mesh(&local_mesh, &hash)
                                .await
                                .unwrap_or(false)
                            {
                                let _ = announce_local_blobs(
                                    &mut writer,
                                    &node_id,
                                    &store,
                                    &multiaddrs,
                                )
                                .await;
                                continue;
                            }
                            info!(%hash, "BlobWant (missing digest)");
                            let want = Envelope::new(
                                node_id.clone(),
                                Message::BlobWant { sha256: hash },
                            );
                            writer.write_all(&encode_line(&want)?).await?;
                        }
                    }
                    Message::BlobProvide {
                        sha256,
                        request_id,
                        to: _,
                    } => {
                        // Control asked us to push this blob (we are a seeder).
                        let hash = sha256.to_lowercase();
                        match WeightsStore::read_blob(&hash) {
                            Ok(data) => {
                                info!(
                                    %hash,
                                    %request_id,
                                    bytes = data.len(),
                                    "BlobProvide: streaming chunks"
                                );
                                let mut offset = 0u64;
                                while (offset as usize) < data.len() {
                                    let end = ((offset as usize) + BLOB_CHUNK_BYTES).min(data.len());
                                    let slice = &data[offset as usize..end];
                                    let done = end == data.len();
                                    let chunk = Envelope::new(
                                        node_id.clone(),
                                        Message::BlobChunk {
                                            sha256: hash.clone(),
                                            request_id,
                                            offset,
                                            data_b64: B64.encode(slice),
                                            done,
                                        },
                                    );
                                    writer.write_all(&encode_line(&chunk)?).await?;
                                    offset = end as u64;
                                }
                                if data.is_empty() {
                                    let chunk = Envelope::new(
                                        node_id.clone(),
                                        Message::BlobChunk {
                                            sha256: hash.clone(),
                                            request_id,
                                            offset: 0,
                                            data_b64: String::new(),
                                            done: true,
                                        },
                                    );
                                    writer.write_all(&encode_line(&chunk)?).await?;
                                }
                            }
                            Err(e) => {
                                warn!(%hash, error = %e, "BlobProvide: cannot read blob");
                            }
                        }
                    }
                    Message::BlobChunk {
                        sha256,
                        request_id,
                        offset,
                        data_b64,
                        done,
                    } => {
                        let hash = sha256.to_lowercase();
                        let bytes = match B64.decode(data_b64.as_bytes()) {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(%request_id, error = %e, "BlobChunk bad base64");
                                pending_recv.remove(&request_id);
                                continue;
                            }
                        };
                        let entry = pending_recv.entry(request_id).or_insert_with(|| {
                            PendingBlobRecv {
                                sha256: hash.clone(),
                                buf: Vec::new(),
                                next_offset: 0,
                            }
                        });
                        if entry.sha256 != hash {
                            warn!(%request_id, "BlobChunk sha256 changed mid-transfer");
                            pending_recv.remove(&request_id);
                            continue;
                        }
                        if offset != entry.next_offset {
                            warn!(
                                %request_id,
                                expect = entry.next_offset,
                                got = offset,
                                "BlobChunk out of order"
                            );
                            pending_recv.remove(&request_id);
                            continue;
                        }
                        if entry.next_offset + bytes.len() as u64 > MAX_RELAY_BLOB_BYTES {
                            warn!(
                                %request_id,
                                "BlobChunk exceeds MAX_RELAY_BLOB_BYTES; abort"
                            );
                            pending_recv.remove(&request_id);
                            continue;
                        }
                        entry.buf.extend_from_slice(&bytes);
                        entry.next_offset = offset + bytes.len() as u64;
                        if done {
                            let finished = pending_recv.remove(&request_id).unwrap();
                            match WeightsStore::store_blob(&finished.sha256, &finished.buf) {
                                Ok(n) => {
                                    info!(
                                        hash = %finished.sha256,
                                        bytes = n,
                                        "blob received + verified"
                                    );
                                    if let Err(e) = announce_local_blobs(
                                        &mut writer,
                                        &node_id,
                                        &store,
                                        &multiaddrs,
                                    )
                                    .await
                                    {
                                        warn!(error = %e, "BlobsHave after receive failed");
                                    }
                                    // Stage software if this digest was the pending update.
                                    if let Some((ver, tgt)) = pending_software.clone() {
                                        if tgt.sha256.to_lowercase() == finished.sha256 {
                                            match stage_blob(&ver, &tgt) {
                                                Ok(st) => {
                                                    println!("{}", st.message);
                                                    pending_software = None;
                                                }
                                                Err(e) => {
                                                    warn!(error = %e, "stage after receive failed")
                                                }
                                            }
                                        }
                                    }
                                    // If a weight digest landed, try prepare/load for primary quant.
                                    try_load_after_blob(
                                        &store,
                                        production.as_ref(),
                                        &mut writer,
                                        &node_id,
                                        &finished.sha256,
                                        mem_mib,
                                        &mut last_armed,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    warn!(
                                        hash = %finished.sha256,
                                        error = %e,
                                        "blob verify/store failed"
                                    );
                                }
                            }
                        }
                    }
                    other => {
                        warn!(msg = ?other, "ignored control message");
                    }
                }
            }
        }
    }
}

/// After a peer seed lands, try to complete weight prepare + load if this hash is in the manifest.
fn append_broadcast_journal(envelope: &SignedEnvelope) -> Result<()> {
    use std::io::Write;
    let dir = std::env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(".local/share/joule/broadcasts"))
        .unwrap_or_else(|| PathBuf::from("./.joule-broadcasts"));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("journal.ndjson");
    let line = serde_json::json!({
        "id": envelope.id,
        "kind": format!("{:?}", envelope.kind),
        "issued_at_unix_ms": envelope.issued_at_unix_ms,
        "body_sha256": envelope.body_sha256,
        "body_json": envelope.body_json,
    });
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{line}")?;
    Ok(())
}

async fn try_load_after_blob(
    store: &WeightsStore,
    production: &ProductionEngine,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    node_id: &NodeId,
    sha256: &str,
    mem_mib: u32,
    last_armed: &mut bool,
) -> Result<()> {
    let engine = production.cluster();

    let Ok(manifest) = ManifestFile::load_default() else {
        return Ok(());
    };
    let Some(spec) = manifest.primary() else {
        return Ok(());
    };
    let hash = sha256.to_lowercase();
    let mut matched_quant = None;
    for q in &spec.weights.quants {
        if q.files.iter().any(|f| f.sha256.to_lowercase() == hash) {
            matched_quant = Some(q);
            break;
        }
    }
    let Some(q) = matched_quant.or_else(|| spec.pick_quant(mem_mib)) else {
        return Ok(());
    };
    // Only auto-load if this blob is part of the quant we're considering.
    if !q.files.iter().any(|f| f.sha256.to_lowercase() == hash) {
        return Ok(());
    }
    match store.prepare(spec, q) {
        Ok(st) => {
            *last_armed = st.armed;
            info!(%st.message, "prepare after peer seed");
            let ok = Envelope::new(
                node_id.clone(),
                Message::PrepareOk {
                    model: st.model.clone(),
                    quant: st.quant.clone(),
                    armed: st.armed,
                    files_complete: st.files_complete,
                    message: st.message.clone(),
                },
            );
            writer.write_all(&encode_line(&ok)?).await?;
            if st.files_complete {
                production.begin_quant_intent(q);
                match prepare_and_install(store, engine, spec, q) {
                    Ok(report) => match production.mark_content_from_store(store, q) {
                        Ok(()) => {
                            println!("loaded after peer seed: {}", report.message);
                            let loaded = Envelope::new(
                                node_id.clone(),
                                Message::ModelLoaded {
                                    model: report.model,
                                    quant: report.quant,
                                    bytes_resident: report.bytes_resident,
                                    tensors: report.tensors as u32,
                                    message: report.message,
                                },
                            );
                            writer.write_all(&encode_line(&loaded)?).await?;
                        }
                        Err(e) => warn!(error = %e, "peer-seed content gate refused"),
                    },
                    Err(e) => info!(error = %e, "load after seed deferred"),
                }
            }
        }
        Err(e) => warn!(error = %e, "prepare after seed failed"),
    }
    Ok(())
}

async fn announce_local_blobs(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    node_id: &NodeId,
    store: &WeightsStore,
    multiaddrs: &[String],
) -> Result<()> {
    // Ensure prepared weight files are also content-addressed so BlobProvide can read them.
    if let Ok(manifest) = ManifestFile::load_default() {
        if let Some(spec) = manifest.primary() {
            for q in &spec.weights.quants {
                for m in store.local_blob_metas(&spec.id, q) {
                    let path = store.model_dir(&spec.id, &q.id).join(
                        m.name
                            .rsplit_once('/')
                            .map(|(_, p)| p)
                            .unwrap_or(m.name.as_str()),
                    );
                    // Prefer reading from model_dir file when blob store empty.
                    if !WeightsStore::has_blob(&m.sha256) && path.is_file() {
                        if let Ok(bytes) = std::fs::read(&path) {
                            let _ = WeightsStore::store_blob(&m.sha256, &bytes);
                        }
                    }
                }
            }
        }
    }
    let blobs: Vec<BlobMeta> = WeightsStore::list_blob_store()
        .into_iter()
        .map(|m| BlobMeta {
            sha256: m.sha256,
            size: m.size,
            kind: m.kind,
            name: m.name,
            multiaddrs: multiaddrs.to_vec(),
        })
        .collect();
    if blobs.is_empty() {
        return Ok(());
    }
    let have = Envelope::new(node_id.clone(), Message::BlobsHave { blobs });
    writer.write_all(&encode_line(&have)?).await?;
    Ok(())
}

async fn run_ready(api: String, pool_vram_gib: u64, backends: u32) -> Result<()> {
    if !api.trim().is_empty() {
        let url = format!("{}/v1/models/readiness", api.trim_end_matches('/'));
        let text = reqwest::get(&url)
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .text()
            .await?;
        println!("{text}");
        return Ok(());
    }
    let r = readiness_for_pool_ex(
        pool_vram_gib.saturating_mul(1024),
        backends,
        RuntimeFlags::default(),
        None,
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    println!("{}", serde_json::to_string_pretty(&r)?);
    Ok(())
}

fn run_load(model: String, quant: String, mem_mib: u32) -> Result<()> {
    let manifest = ManifestFile::load_default().map_err(|e| anyhow::anyhow!(e))?;
    let spec = manifest
        .model(&model)
        .or_else(|| manifest.primary())
        .ok_or_else(|| anyhow::anyhow!("no model in manifest"))?;
    let q = if quant.is_empty() {
        spec.pick_quant(mem_mib)
            .ok_or_else(|| anyhow::anyhow!("no quant"))?
    } else {
        spec.weights
            .quants
            .iter()
            .find(|x| x.id == quant)
            .ok_or_else(|| anyhow::anyhow!("unknown quant {quant}"))?
    };
    let store = WeightsStore::new(WeightsStore::default_root());
    store.ensure_root().map_err(|e| anyhow::anyhow!(e))?;
    let prep = store.prepare(spec, q).map_err(|e| anyhow::anyhow!(e))?;
    println!("prepare: {}", prep.message);
    let loaded = load_model(&store, spec, q).map_err(|e| anyhow::anyhow!(e))?;
    println!("{}", serde_json::to_string_pretty(&loaded.report())?);
    Ok(())
}

async fn run_capacity(api: String, peers: usize, json: bool) -> Result<()> {
    let cap = if api.trim().is_empty() {
        demo_cluster(peers, CLUSTER_MODEL).capacity()
    } else {
        let url = format!("{}/v1/cluster/capacity", api.trim_end_matches('/'));
        let resp = reqwest::get(&url)
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .context("capacity status")?;
        resp.json::<ClusterCapacity>()
            .await
            .context("capacity json")?
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&cap)?);
    } else {
        print_capacity(&cap);
    }
    Ok(())
}

/// Prefer CLI --key; else cached key from identity (issued on Welcome).
fn resolve_api_key_optional(cli_key: &str) -> Option<String> {
    let t = cli_key.trim();
    if !t.is_empty() {
        return Some(t.to_string());
    }
    identity::cached_api_key(&identity::default_path())
}

fn resolve_api_key(cli_key: &str) -> Result<String> {
    resolve_api_key_optional(cli_key).ok_or_else(|| {
        anyhow::anyhow!(
            "no API key yet. Start the agent once (pool issues a key), then run: joule connect"
        )
    })
}

/// Product path: show/copy Base URL + generated API key for Cursor-class apps.
fn run_connect(api: &str, model: &str, copy: bool, copy_url: bool, open: bool) -> Result<()> {
    let id_path = identity::default_path();
    let key = identity::cached_api_key(&id_path);
    identity::print_connect_card(&id_path, api, key.as_deref(), model);
    if copy {
        let Some(ref k) = key else {
            bail!("no API key cached — run joule agent, join the pool, then joule connect --copy");
        };
        match tray_app::copy_to_clipboard(k) {
            Ok(()) => println!("API key copied to clipboard. Paste it into Cursor / your app."),
            Err(e) => eprintln!("clipboard: {e}"),
        }
    }
    if copy_url {
        let base = identity::openai_base_url(api);
        match tray_app::copy_to_clipboard(&base) {
            Ok(()) => println!("Base URL copied to clipboard: {base}"),
            Err(e) => eprintln!("clipboard: {e}"),
        }
    }
    if open {
        if let Some(ref k) = key {
            let note = identity::write_connect_note(&id_path, api, k, model)?;
            tray_app::open_path(&note)?;
            println!("opened {}", note.display());
        } else {
            bail!("no JOULE-CONNECT.txt yet — run joule agent first so a key exists");
        }
    }
    Ok(())
}

async fn run_chat(
    api: String,
    key: String,
    model: String,
    prompt: String,
    stream: bool,
) -> Result<()> {
    let url = format!("{}/v1/chat/completions", api.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
        .context("chat request")?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await?;
        bail!("chat failed {status}: {text}");
    }
    if stream {
        let text = resp.text().await?;
        // SSE: data: {...}\n\n — print content deltas
        for line in text.lines() {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            if data.trim() == "[DONE]" {
                break;
            }
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                if let Some(c) = v
                    .pointer("/choices/0/delta/content")
                    .and_then(|x| x.as_str())
                {
                    print!("{c}");
                }
            }
        }
        println!();
        return Ok(());
    }
    let text = resp.text().await?;
    let v: serde_json::Value = serde_json::from_str(&text)?;
    if let Some(content) = v
        .pointer("/choices/0/message/content")
        .and_then(|c| c.as_str())
    {
        println!("{content}");
    } else {
        println!("{text}");
    }
    Ok(())
}

async fn run_whoami(api: String, key: String) -> Result<()> {
    let url = format!("{}/v1/account", api.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .bearer_auth(key)
        .send()
        .await
        .context("account request")?
        .error_for_status()
        .context("account status")?;
    let v: serde_json::Value = resp.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}

async fn run_status(api: String, key: String, json: bool, dash: bool) -> Result<()> {
    let key_opt = if key.is_empty() {
        None
    } else {
        Some(key.as_str())
    };
    let st = client_status::fetch_client_status(&api, key_opt).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&st)?);
    } else if dash {
        println!("{}", format_monitor_dash(&st));
    } else {
        print!("{}", format_status_human(&st));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_service_generate(
    platform: String,
    kind: String,
    binary: String,
    control: String,
    account: String,
    api: String,
    key: String,
    mem_mib: u32,
    user: bool,
    out: Option<PathBuf>,
) -> Result<()> {
    let platform = match platform.to_ascii_lowercase().as_str() {
        "linux" | "systemd" => ServicePlatform::LinuxSystemd,
        "macos" | "darwin" | "launchd" => ServicePlatform::MacosLaunchd,
        "windows" | "win" | "task" => ServicePlatform::WindowsTask,
        other => bail!("unknown platform {other}; use linux|macos|windows"),
    };
    let kind = match kind.to_ascii_lowercase().as_str() {
        "agent" => ServiceKind::Agent,
        "tray" | "monitor" => ServiceKind::Tray,
        other => bail!("unknown kind {other}; use agent|tray"),
    };
    let spec = InstallSpec {
        platform,
        kind,
        binary_path: binary,
        control,
        account,
        api,
        api_key: if key.is_empty() { None } else { Some(key) },
        mem_mib,
        user_unit: user,
        description: match kind {
            ServiceKind::Agent => "joule donor agent".into(),
            ServiceKind::Tray => "joule status tray/monitor".into(),
        },
    };
    match platform {
        ServicePlatform::LinuxSystemd => {
            let body = generate_systemd_unit(&spec);
            write_service_text(out, &body)?;
        }
        ServicePlatform::MacosLaunchd => {
            let body = generate_launchd_plist(&spec);
            write_service_text(out, &body)?;
        }
        ServicePlatform::WindowsTask => {
            // schtasks /Create /XML requires UTF-16 LE (Unicode) when encoding="UTF-16".
            let bytes = generate_windows_task_xml_file_bytes(&spec);
            if let Some(p) = out {
                std::fs::write(&p, &bytes).with_context(|| format!("write {}", p.display()))?;
                println!(
                    "wrote {} (UTF-16 LE + BOM for schtasks /Create /XML)",
                    p.display()
                );
            } else {
                // stdout is text-mode; print UTF-8 preview + note.
                print!("{}", generate_windows_task_xml(&spec));
                eprintln!(
                    "note: for schtasks use --out FILE (writes UTF-16 LE + BOM, not plain UTF-8)"
                );
            }
        }
    }
    Ok(())
}

fn write_service_text(out: Option<PathBuf>, body: &str) -> Result<()> {
    if let Some(p) = out {
        std::fs::write(&p, body).with_context(|| format!("write {}", p.display()))?;
        println!("wrote {}", p.display());
    } else {
        print!("{body}");
    }
    Ok(())
}

fn run_service_install_help(platform: &str) -> Result<()> {
    match platform.to_ascii_lowercase().as_str() {
        "linux" | "systemd" => {
            println!(
                r#"Linux (user systemd — recommended with tray/GPU):

  joule service generate --platform linux --kind agent \
    --binary "$(command -v joule)" --account YOU --control 127.0.0.1:7701 \
    --out ~/.config/systemd/user/joule-agent.service
  systemctl --user daemon-reload
  systemctl --user enable --now joule-agent.service

Tray/monitor:
  joule service generate --platform linux --kind tray --out ~/.config/systemd/user/joule-tray.service
  systemctl --user enable --now joule-tray.service

Headless-only agent may use --user=false and install under /etc/systemd/system/ (root).
"#
            );
        }
        "macos" | "darwin" | "launchd" => {
            println!(
                r#"macOS (LaunchAgents — user domain, tray-friendly):

  joule service generate --platform macos --kind agent \
    --binary /opt/homebrew/bin/joule --account YOU \
    --out ~/Library/LaunchAgents/sh.f00.joule.agent.plist
  launchctl load ~/Library/LaunchAgents/sh.f00.joule.agent.plist

Tray:
  joule service generate --platform macos --kind tray --out ~/Library/LaunchAgents/sh.f00.joule.tray.plist
  launchctl load ~/Library/LaunchAgents/sh.f00.joule.tray.plist

CLI status on macOS: joule status --api http://127.0.0.1:7700 --key joule_…
"#
            );
        }
        "windows" | "win" => {
            println!(
                r#"Windows (Task Scheduler — logon trigger, least privilege):

  joule service generate --platform windows --kind agent \
    --binary "C:\Program Files\joule\joule.exe" --account YOU \
    --out %TEMP%\joule-agent.xml
  # --out writes UTF-16 LE + BOM (required by schtasks /Create /XML)
  schtasks /Create /TN joule-agent /XML %TEMP%\joule-agent.xml

Tray:
  joule service generate --platform windows --kind tray --out %TEMP%\joule-tray.xml
  schtasks /Create /TN joule-tray /XML %TEMP%\joule-tray.xml

CLI status on Windows: joule status --api http://127.0.0.1:7700 --key joule_…
"#
            );
        }
        other => bail!("unknown platform {other}"),
    }
    Ok(())
}

fn demo_cluster(peers: usize, _model: &str) -> Cluster {
    let mut cluster = Cluster::default();
    for i in 0..peers {
        let id = NodeId::new();
        let mem = 8192 + (i as u32) * 4096;
        cluster.upsert_node(
            id,
            format!("lab-{i}"),
            NodeCaps::for_cluster(DeviceClass::Gpu, mem, 10 + i as u16),
        );
    }
    cluster
}

fn print_capacity(cap: &ClusterCapacity) {
    println!("cluster capacity (live aggregate)");
    println!(
        "  nodes:     {} healthy / {} total",
        cap.nodes_healthy, cap.nodes_total
    );
    println!(
        "  devices:   gpu={} metal={} cpu={}",
        cap.nodes_gpu, cap.nodes_metal, cap.nodes_cpu
    );
    println!(
        "  memory:    {} MiB healthy / {} MiB total",
        cap.mem_mib_healthy, cap.mem_mib_total
    );
    println!(
        "  throughput_class_sum (healthy): {}",
        cap.throughput_class_sum
    );
    println!(
        "  tokens_per_sec (measured): {} (samples={})",
        cap.tokens_per_sec, cap.tokens_per_sec_samples
    );
    if cap.models_available.is_empty() {
        println!("  models:    (none)");
    } else {
        println!("  models:    {}", cap.models_available.join(", "));
    }
}

async fn run_lab(
    model: String,
    prompt: String,
    pipeline: bool,
    stages: usize,
    peers: usize,
) -> Result<()> {
    let cluster = demo_cluster(peers, &model);
    let cap = cluster.capacity();
    print_capacity(&cap);

    let plan = if pipeline {
        cluster
            .plan_for(CLUSTER_MODEL, true, stages.max(peers))
            .context("planning cluster")?
    } else {
        cluster
            .plan_for(CLUSTER_MODEL, false, 1)
            .context("planning cluster")?
    };
    println!(
        "plan {} shards={} model={} (pool uses all healthy donors for single model)",
        plan.plan_id,
        plan.shards.len(),
        plan.model
    );
    let _ = model;
    for (i, s) in plan.shards.iter().enumerate() {
        println!(
            "  shard[{i}] role={:?} node={} layers={:?}-{:?}",
            s.role, s.node.0, s.layer_start, s.layer_end
        );
    }

    let engine = StubEngine::new();
    engine.load_plan(&plan).await.context("load plan")?;
    let out = engine
        .infer(InferRequest {
            model: model.clone(),
            prompt: prompt.clone(),
            max_tokens: 64,
        })
        .await
        .context("infer")?;

    println!("completion: {}", out.text);
    println!(
        "tokens: prompt={} completion={}",
        out.prompt_tokens, out.completion_tokens
    );

    let mut ledger = Ledger::new();
    let mint = estimate_contribution_millijoules(out.completion_tokens, 8);
    let burn = estimate_usage_millijoules(out.prompt_tokens, out.completion_tokens);
    ledger
        .mint_contribution("lab-donor", mint, "lab-job")
        .context("mint")?;
    ledger
        .burn_usage("lab-donor", burn, "lab-chat")
        .context("burn")?;
    println!(
        "ledger lab-donor balance={} mJ (minted {mint}, burned {burn})",
        ledger.balance("lab-donor")
    );
    Ok(())
}

fn run_credits(account: &str) -> Result<()> {
    let mut ledger = Ledger::new();
    ledger.mint_contribution(account, 5000, "bootstrap-demo")?;
    ledger.burn_usage(account, 120, "demo-chat")?;
    println!("{account} balance={} mJ", ledger.balance(account));
    for e in ledger.events() {
        println!("  {} {:+} mJ  {}", e.id, e.delta_millijoules, e.reason);
    }
    Ok(())
}
