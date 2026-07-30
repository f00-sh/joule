//! End-to-end: control + multi-agent load balance + chat + challenges.

use joule_control::{load_or_init_app, serve_ephemeral};
use joule_proto::{
    decode_line, encode_line, DeviceClass, Envelope, Message, NodeCaps, NodeId, CLUSTER_MODEL,
    PROTOCOL_VERSION,
};
use joule_runtime::{Engine, InferRequest, StubEngine};
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

    // Heartbeat
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
        // also heartbeat loop
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
                            let reply = handle_infer(&env, &stub, &node_id).await;
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::Challenge { .. } => {
                            let reply = handle_challenge(&env, &stub, &node_id).await;
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

async fn handle_infer(env: &Envelope, stub: &StubEngine, node_id: &NodeId) -> Envelope {
    use joule_proto::{ClusterPlan, ShardAssignment, ShardRole};
    if let Message::InferRequest {
        request_id,
        model,
        prompt,
        max_tokens,
    } = &env.msg
    {
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: model.clone(),
            shards: vec![ShardAssignment {
                node: node_id.clone(),
                role: ShardRole::Replica,
                layer_start: None,
                layer_end: None,
                tp_rank: None,
                tp_world: None,
            }],
        };
        stub.load_plan(&plan).await.unwrap();
        let out = stub
            .infer(InferRequest {
                model: model.clone(),
                prompt: prompt.clone(),
                max_tokens: *max_tokens,
            })
            .await
            .unwrap();
        return Envelope::new(
            node_id.clone(),
            Message::InferDone {
                request_id: *request_id,
                text: out.text,
                prompt_tokens: out.prompt_tokens,
                completion_tokens: out.completion_tokens,
            },
        );
    }
    panic!("not infer");
}

async fn handle_challenge(env: &Envelope, stub: &StubEngine, node_id: &NodeId) -> Envelope {
    use joule_proto::{ClusterPlan, ShardAssignment, ShardRole};
    if let Message::Challenge {
        challenge_id,
        model,
        prompt,
    } = &env.msg
    {
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: model.clone(),
            shards: vec![ShardAssignment {
                node: node_id.clone(),
                role: ShardRole::Replica,
                layer_start: None,
                layer_end: None,
                tp_rank: None,
                tp_world: None,
            }],
        };
        stub.load_plan(&plan).await.unwrap();
        let out = stub
            .infer(InferRequest {
                model: model.clone(),
                prompt: prompt.clone(),
                max_tokens: 64,
            })
            .await
            .unwrap();
        return Envelope::new(
            node_id.clone(),
            Message::ChallengeResult {
                challenge_id: *challenge_id,
                completion: out.text,
                latency_ms: 1,
            },
        );
    }
    panic!("not challenge");
}

#[tokio::test]
async fn pool_capacity_and_chat() {
    let app = load_or_init_app(None).expect("app");
    // Disable dual-verify for single-agent chat test
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
    assert!(health["agents_connected"].as_u64().unwrap() >= 1);

    let cap: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cap["nodes_healthy"], 1);
    assert_eq!(cap["mem_mib_healthy"], 16384);

    let dash = client
        .get(format!("{base}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(dash.contains("joule"));

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

    let stream_resp = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "stream": true,
            "messages": [{"role": "user", "content": "stream me"}]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(stream_resp.contains("data:"));
    assert!(stream_resp.contains("[DONE]"));

    let bad = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth("joule_nope")
        .json(&serde_json::json!({
            "messages": [{"role": "user", "content": "x"}]
        }))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(bad.as_u16(), 401);

    agent.abort();
    assert_eq!(PROTOCOL_VERSION, "0.1.0");
}

#[tokio::test]
async fn multi_donor_load_balance() {
    let app = load_or_init_app(None).unwrap();
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _) = serve_ephemeral(app.clone()).await.unwrap();
    let (key_a, a) = spawn_agent(agent_addr, "alice", 8192).await;
    let (_key_b, b) = spawn_agent(agent_addr, "bob", 16384).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let cap: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cap["nodes_healthy"], 2);
    assert_eq!(cap["mem_mib_healthy"], 8192 + 16384);

    // Fire several chats; should succeed (routing across donors).
    for i in 0..6 {
        let r = client
            .post(format!("{base}/v1/chat/completions"))
            .bearer_auth(&key_a)
            .json(&serde_json::json!({
                "model": CLUSTER_MODEL,
                "messages": [{"role": "user", "content": format!("job {i}")}]
            }))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "chat {i} {}", r.status());
    }

    let nodes: serde_json::Value = client
        .get(format!("{base}/v1/cluster/nodes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 2);

    a.abort();
    b.abort();
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
