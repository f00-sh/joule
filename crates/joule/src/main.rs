//! joule — distributed cluster supercomputer CLI.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use joule_cluster::Cluster;
use joule_ledger::{estimate_contribution_millijoules, estimate_usage_millijoules, Ledger};
use joule_proto::{ClusterCapacity, DeviceClass, NodeCaps, NodeId};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "joule",
    version,
    about = "Distributed internet-wide cluster: pool idle GPUs into open-weight AI inference"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print protocol and build identity.
    Version,
    /// Local lab: synthetic nodes, capacity snapshot, placement, stub inference.
    Lab {
        /// Model tag to plan for.
        #[arg(long, default_value = "kimi-open-q4")]
        model: String,
        /// Prompt text for stub inference.
        #[arg(long, default_value = "status report from the cluster")]
        prompt: String,
        /// Prefer pipeline parallelism when enough nodes exist.
        #[arg(long, default_value_t = true)]
        pipeline: bool,
        /// Pipeline stage count when pipeline is preferred.
        #[arg(long, default_value_t = 2)]
        stages: usize,
        /// Number of synthetic GPU nodes.
        #[arg(long, default_value_t = 3)]
        peers: usize,
    },
    /// Print cluster capacity (dashboard feed shape).
    Capacity {
        /// Synthetic healthy GPU nodes for local demo (0 = empty cluster).
        #[arg(long, default_value_t = 5)]
        peers: usize,
        /// Emit JSON (same schema as future GET /v1/cluster/capacity).
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Show ledger demo (mint contribution, burn usage).
    Credits {
        #[arg(long, default_value = "donor")]
        account: String,
    },
    /// Placeholder: run a donor agent (network join not wired yet).
    Agent {
        #[arg(long, default_value = "kimi-open-q4")]
        model: String,
        #[arg(long)]
        config: Option<PathBuf>,
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
            println!("distributed cluster (internet-wide)");
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
        Commands::Capacity { peers, json } => {
            run_capacity(peers, json)?;
        }
        Commands::Credits { account } => {
            run_credits(&account)?;
        }
        Commands::Agent { model, config } => {
            let _ = config;
            bail!(
                "agent network transport not implemented yet (model={model}). See docs/design/cluster-v0.md"
            );
        }
    }
    Ok(())
}

fn demo_cluster(peers: usize, model: &str) -> Cluster {
    let mut cluster = Cluster::new();
    for i in 0..peers {
        let id = NodeId::new();
        let mem = 8192 + (i as u32) * 4096;
        cluster.upsert_node(
            id,
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

fn run_capacity(peers: usize, json: bool) -> Result<()> {
    let cluster = demo_cluster(peers, "kimi-open-q4");
    let cap = cluster.capacity();
    if json {
        println!("{}", serde_json::to_string_pretty(&cap)?);
    } else {
        print_capacity(&cap);
    }
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
