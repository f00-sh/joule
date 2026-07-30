//! End-to-end: sharded pool across multi-donor VRAM + stream slots.

use joule_control::{load_or_init_app, serve_ephemeral};
use joule_proto::{
    decode_line, encode_line, DeviceClass, Envelope, Message, NodeCaps, NodeId, CLUSTER_MODEL,
    PROTOCOL_VERSION,
};
use joule_runtime::StubEngine;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

async fn spawn_agent(
    agent_addr: std::net::SocketAddr,
    account: &str,
    mem: u32,
) -> (String, tokio::task::JoinHandle<()>) {
    let node_id = NodeId::new();
    let sock = TcpStream::connect(agent_addr).await.expect("agent connect");
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();

    let hello = Envelope::new(
        node_id.clone(),
        Message::Hello {
            account: account.into(),
            caps: NodeCaps::for_cluster(DeviceClass::Gpu, mem, 40),
        },
    );
    writer
        .write_all(&encode_line(&hello).unwrap())
        .await
        .unwrap();

    let welcome_line = lines.next_line().await.unwrap().expect("welcome");
    let welcome = decode_line(welcome_line.as_bytes()).unwrap();
    let api_key = match welcome.msg {
        Message::Welcome { api_key, .. } => api_key,
        other => panic!("expected welcome, got {other:?}"),
    };

    let hb = Envelope::new(
        node_id.clone(),
        Message::Heartbeat {
            load: 0.0,
            healthy: true,
        },
    );
    writer.write_all(&encode_line(&hb).unwrap()).await.unwrap();

    let handle = tokio::spawn(async move {
        let stub = StubEngine::new();
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let hb = Envelope::new(
                        node_id.clone(),
                        Message::Heartbeat { load: 0.05, healthy: true },
                    );
                    if writer.write_all(&encode_line(&hb).unwrap()).await.is_err() {
                        break;
                    }
                }
                line = lines.next_line() => {
                    let Ok(Some(line)) = line else { break; };
                    if line.trim().is_empty() { continue; }
                    let env = decode_line(line.as_bytes()).unwrap();
                    match &env.msg {
                        Message::InferRequest { .. } => {
                            let reply = joule_control::agent_handle_infer(&env, &stub)
                                .await
                                .unwrap();
                            let reply = Envelope::new(node_id.clone(), reply.msg);
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::Challenge { .. } => {
                            let reply = joule_control::agent_handle_challenge(&env, &stub)
                                .await
                                .unwrap();
                            let reply = Envelope::new(node_id.clone(), reply.msg);
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    (api_key, handle)
}

#[tokio::test]
async fn pool_capacity_and_chat() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let (api_key, agent) = spawn_agent(agent_addr, "alice", 16384).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    let health: serde_json::Value = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["ok"], true);

    let sched: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sched["view"], "one_logical_device");
    assert_eq!(sched["mode"], "vram_sharded_pool");
    assert_eq!(sched["shards"], 1);
    assert!(sched["pool_mem_mib"].as_u64().unwrap() >= 16384);
    let cap: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cap["logical_device"]["id"], "joule-pool");
    assert_eq!(cap["logical_device"]["backends"], 1);
    // small pool → not model_ready for kimi (needs 64 GiB / 3 backends)
    assert_eq!(cap["logical_device"]["model_ready"], false);

    let ready: serde_json::Value = client
        .get(format!("{base}/v1/models/readiness"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready["pool_ready"], false);
    assert_eq!(ready["inference_mode"], "stub_awaiting_pool");

    let chat: serde_json::Value = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let content = chat["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("");
    assert!(content.contains("ping"), "content={content}");

    agent.abort();
    assert_eq!(PROTOCOL_VERSION, "0.1.0");
}

#[tokio::test]
async fn multi_donor_sharded_plan() {
    let app = load_or_init_app(None).unwrap();
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _) = serve_ephemeral(app.clone()).await.unwrap();
    // 8 + 16*4 = 72 GiB style pool
    let (key_a, a) = spawn_agent(agent_addr, "alice", 8192).await;
    let (_k, b) = spawn_agent(agent_addr, "bob", 16384).await;
    let (_k, c) = spawn_agent(agent_addr, "carol", 16384).await;
    let (_k, d) = spawn_agent(agent_addr, "dave", 16384).await;
    let (_k, e) = spawn_agent(agent_addr, "erin", 16384).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let sched: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sched["view"], "one_logical_device");
    assert_eq!(sched["shards"], 5);
    assert_eq!(sched["pool_mem_mib"], 8192 + 16384 * 4);
    assert_eq!(sched["pool_mem_gib"], (8192 + 16384 * 4) / 1024);
    let cap: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // joule sees ONE device whose VRAM is the sum of all backends
    assert_eq!(cap["logical_device"]["id"], "joule-pool");
    assert_eq!(cap["logical_device"]["backends"], 5);
    assert_eq!(cap["logical_device"]["vram_mib"], 8192 + 16384 * 4);

    // One request fans across whole pool; still succeeds
    let r = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&key_a)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "spread me"}]
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "status {}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(body["choices"][0]["message"]["content"]
        .as_str()
        .unwrap()
        .contains("spread"));

    a.abort();
    b.abort();
    c.abort();
    d.abort();
    e.abort();
}

#[tokio::test]
async fn persist_roundtrip() {
    let dir = tempfile_dir();
    let app = joule_control::App::load_or_init(Some(dir.clone())).unwrap();
    {
        let mut g = app.state.write().await;
        let _ = g.ensure_account("bob");
        g.ledger.mint_contribution("bob", 100, "seed").unwrap();
        let _ = g.ensure_account("carol");
        g.prune();
    }

    let app2 = joule_control::App::load_or_init(Some(dir)).unwrap();
    let g = app2.state.read().await;
    assert!(g.account_keys.contains_key("carol"));
    assert!(g.ledger.balance("bob") >= 100);
}

fn tempfile_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("joule-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&p).unwrap();
    p
}
