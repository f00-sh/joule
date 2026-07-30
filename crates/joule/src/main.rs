//! joule — distributed compute cluster CLI.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use joule_cluster::Cluster;
use joule_ledger::{estimate_contribution_millijoules, estimate_usage_millijoules, Ledger};
use joule_proto::{
    decode_line, encode_line, ClusterCapacity, DeviceClass, Envelope, Message, NodeCaps, NodeId,
};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

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
        /// Model tag this node can host.
        #[arg(long, default_value = "kimi-open-q4")]
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
        #[arg(long, default_value = "kimi-open-q4")]
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
        #[arg(long, default_value = "kimi-open-q4")]
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
    }
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
    let caps = NodeCaps {
        device,
        mem_mib,
        throughput_class: match device {
            DeviceClass::Gpu => 40,
            DeviceClass::Metal => 30,
            DeviceClass::Cpu => 5,
        },
        models: vec![model.clone()],
    };

    info!(%control, %account, %node_id, "connecting agent");
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
                        println!("capacity:  curl -s http://127.0.0.1:7700/v1/cluster/capacity");
                        println!("chat:      joule chat --key {key} --prompt \"hello\"");
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
                    other => {
                        warn!(msg = ?other, "ignored control message");
                    }
                }
            }
        }
    }
}

async fn run_capacity(api: String, peers: usize, json: bool) -> Result<()> {
    let cap = if api.trim().is_empty() {
        demo_cluster(peers, "kimi-open-q4").capacity()
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

fn demo_cluster(peers: usize, model: &str) -> Cluster {
    let mut cluster = Cluster::default();
    for i in 0..peers {
        let id = NodeId::new();
        let mem = 8192 + (i as u32) * 4096;
        cluster.upsert_node(
            id,
            format!("lab-{i}"),
            NodeCaps {
                device: DeviceClass::Gpu,
                mem_mib: mem,
                throughput_class: 10 + i as u16,
                models: vec![model.to_string()],
            },
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

    let plan = cluster
        .plan_for(&model, pipeline, stages)
        .context("planning cluster")?;
    println!(
        "plan {} shards={} model={}",
        plan.plan_id,
        plan.shards.len(),
        plan.model
    );
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
