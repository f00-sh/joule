//! joule — distributed compute cluster CLI.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use clap::{Parser, Subcommand};
use joule_cluster::{plan_redundant_chunks, Cluster, ModelChunk};
use joule_control::{body_sha256_hex, now_ms, operator_preimage};
use joule_ledger::{estimate_contribution_millijoules, estimate_usage_millijoules, Ledger};
use joule_proto::{
    decode_line, encode_line, BlobMeta, ClusterCapacity, DeviceClass, Envelope, Message, NodeCaps,
    NodeId, OperatorKind, SignedEnvelope, CLUSTER_MODEL,
};
use joule_runtime::{
    apply_staged, load_model, match_target, parse_software_update, read_stage, readiness_for_pool_ex,
    stage_blob, Engine, InferRequest, ManifestFile, RuntimeFlags, SoftwareTarget, StubEngine,
    WeightsStore,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Wire chunk size for peer BlobChunk (lab-sized files; large models later).
const BLOB_CHUNK_BYTES: usize = 64 * 1024;

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
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
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
    /// Join the cluster as a donor agent (earn millijoules).
    Agent {
        /// Control plane agent address (host:port).
        #[arg(long, default_value = "127.0.0.1:7701")]
        control: String,
        /// Account that earns credits from this node.
        #[arg(long)]
        account: String,
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
        #[arg(long)]
        config: Option<PathBuf>,
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
        /// API key from agent welcome (joule_...).
        #[arg(long)]
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
        #[arg(long)]
        key: String,
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
    /// Software update stage / apply (peer-seeded binaries only).
    Software {
        #[command(subcommand)]
        cmd: SoftwareCmd,
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
    match cli.cmd {
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
            println!("    joule agent --account alice --control {agent_listen}");
            joule_control::serve(app, agent_listen, http_listen).await?;
        }
        Commands::Agent {
            control,
            account,
            model,
            mem_mib,
            device,
            heartbeat_secs,
            config,
        } => {
            let _ = config;
            run_agent(control, account, model, mem_mib, device, heartbeat_secs).await?;
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
            run_chat(api, key, model, prompt, stream).await?;
        }
        Commands::Whoami { api, key } => {
            run_whoami(api, key).await?;
        }
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
        Commands::Software { cmd } => match cmd {
            SoftwareCmd::Status => software_status()?,
            SoftwareCmd::Apply { dest } => software_apply(dest)?,
        },
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
        format!("# joule operator ed25519 public key — pin in JOULE_OPERATOR_PUBKEY\n{pub_hex}\n"),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600));
    }
    println!("wrote secret {}", secret.display());
    println!("wrote public {}", public.display());
    println!("export JOULE_OPERATOR_PUBKEY={pub_hex}");
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

async fn run_agent(
    control: String,
    account: String,
    model: String,
    mem_mib: u32,
    device: String,
    heartbeat_secs: u64,
) -> Result<()> {
    let device = parse_device(&device)?;
    let node_id = NodeId::new();
    let _ = model; // single-model cluster; agents always donate to CLUSTER_MODEL
    let caps = NodeCaps::for_cluster(
        device,
        mem_mib,
        match device {
            DeviceClass::Gpu => 40,
            DeviceClass::Metal => 30,
            DeviceClass::Cpu => 5,
        },
    );

    info!(%control, %account, %node_id, model = CLUSTER_MODEL, "connecting agent");
    let sock = TcpStream::connect(&control)
        .await
        .with_context(|| format!("connect agent port {control}"))?;
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();

    let hello = Envelope::new(
        node_id.clone(),
        Message::Hello {
            account: account.clone(),
            caps,
        },
    );
    writer.write_all(&encode_line(&hello)?).await?;

    let stub = StubEngine::new();
    let store = WeightsStore::new(WeightsStore::default_root());
    store.ensure_root().ok();
    let mut last_armed = false;
    let mut pending_recv: HashMap<Uuid, PendingBlobRecv> = HashMap::new();
    // When software_update matches this host, stage once the digest lands.
    let mut pending_software: Option<(String, SoftwareTarget)> = None;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs.max(1)));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let hb = Envelope::new(
                    node_id.clone(),
                    Message::Heartbeat { load: 0.1, healthy: true },
                );
                if writer.write_all(&encode_line(&hb)?).await.is_err() {
                    bail!("control connection closed during heartbeat");
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
                    Message::Welcome { account: acc, api_key: key } => {
                        println!("joined cluster as account={acc}");
                        println!("API key (save this): {key}");
                        println!("dashboard: http://127.0.0.1:7700/");
                        println!("readiness: curl -s http://127.0.0.1:7700/v1/models/readiness");
                        println!("chat:      joule chat --key {key} --prompt \"hello\"");
                        println!("weights:   {}", store.root().display());
                        // Seed swarm directory with any local content-addressed blobs.
                        if let Err(e) = announce_local_blobs(&mut writer, &node_id, &store).await {
                            warn!(error = %e, "initial BlobsHave failed");
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
                        if pool_ready && !last_armed {
                            if let Ok(manifest) = ManifestFile::load_default() {
                                if let Some(spec) = manifest.primary() {
                                    let quant = recommend_quant
                                        .as_deref()
                                        .and_then(|id| {
                                            spec.weights.quants.iter().find(|q| q.id == id)
                                        })
                                        .or_else(|| spec.pick_quant(mem_mib));
                                    if let Some(q) = quant {
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
                                                // Actual load into RAM when possible.
                                                match load_model(&store, spec, q) {
                                                    Ok(lm) => {
                                                        let report = lm.report();
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
                                                        info!(error = %e, "model load deferred")
                                                    }
                                                }
                                            }
                                            Err(e) => warn!(error = %e, "prepare failed"),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Message::InferRequest { .. } => {
                        let reply = joule_control::agent_handle_infer(&env, &stub)
                            .await
                            .context("handle infer")?;
                        let reply = Envelope::new(node_id.clone(), reply.msg);
                        writer.write_all(&encode_line(&reply)?).await?;
                    }
                    Message::Challenge { .. } => {
                        let reply = joule_control::agent_handle_challenge(&env, &stub)
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
                        info!(
                            id = %envelope.id,
                            kind = ?envelope.kind,
                            "operator broadcast"
                        );
                        println!(
                            "operator message: {:?} id={} body_sha256={}",
                            envelope.kind, envelope.id, envelope.body_sha256
                        );
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
                            info!(%hash, "BlobWant (missing digest)");
                            let want = Envelope::new(
                                node_id.clone(),
                                Message::BlobWant { sha256: hash },
                            );
                            writer.write_all(&encode_line(&want)?).await?;
                        }
                    }
                    Message::BlobLocate {
                        sha256,
                        peers,
                        sizes,
                    } => {
                        info!(
                            %sha256,
                            seeders = peers.len(),
                            ?sizes,
                            "BlobLocate"
                        );
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
                                    if let Err(e) =
                                        announce_local_blobs(&mut writer, &node_id, &store).await
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
async fn try_load_after_blob(
    store: &WeightsStore,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    node_id: &NodeId,
    sha256: &str,
    mem_mib: u32,
    last_armed: &mut bool,
) -> Result<()> {
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
                match load_model(store, spec, q) {
                    Ok(lm) => {
                        let report = lm.report();
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
