//! joule — mesh supercomputer CLI (research mesh-first).

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use joule_ledger::{estimate_contribution_millijoules, estimate_usage_millijoules, Ledger};
use joule_mesh::Mesh;
use joule_proto::{DeviceClass, NodeCaps, NodeId};
use joule_runtime::{Engine, InferRequest, StubEngine};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "joule",
    version,
    about = "Decentralized mesh supercomputer for open-weight AI inference"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print protocol and build identity.
    Version,
    /// Local lab: register fake peers, plan a mesh, run stub inference.
    Lab {
        /// Model tag to plan for.
        #[arg(long, default_value = "kimi-open-q4")]
        model: String,
        /// Prompt text for stub inference.
        #[arg(long, default_value = "status report from the mesh")]
        prompt: String,
        /// Prefer pipeline parallelism when enough peers exist.
        #[arg(long, default_value_t = true)]
        pipeline: bool,
        /// Pipeline stage count when pipeline is preferred.
        #[arg(long, default_value_t = 2)]
        stages: usize,
        /// Number of synthetic GPU peers.
        #[arg(long, default_value_t = 3)]
        peers: usize,
    },
    /// Show ledger demo (mint contribution, burn usage).
    Credits {
        #[arg(long, default_value = "donor")]
        account: String,
    },
    /// Placeholder: run a donor agent (network mesh not wired yet).
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
            println!("mesh-first research build");
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
        Commands::Agent { model, config } => {
            let _ = config;
            bail!(
                "agent network transport not implemented yet (model={model}). See docs/design/mesh-v0.md"
            );
        }
    }
    Ok(())
}

async fn run_lab(
    model: String,
    prompt: String,
    pipeline: bool,
    stages: usize,
    peers: usize,
) -> Result<()> {
    let mut mesh = Mesh::new();
    for i in 0..peers {
        let id = NodeId::new();
        let mem = 8192 + (i as u32) * 4096;
        mesh.upsert_peer(
            id,
            NodeCaps {
                device: DeviceClass::Gpu,
                mem_mib: mem,
                throughput_class: 10 + i as u16,
                models: vec![model.clone()],
            },
        );
    }

    let plan = mesh
        .plan_for(&model, pipeline, stages)
        .context("planning mesh")?;
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
