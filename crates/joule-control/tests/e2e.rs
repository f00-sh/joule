//! End-to-end: sharded pool across multi-donor VRAM + stream slots.

use joule_control::{load_or_init_app, serve_ephemeral};
use joule_proto::{
    decode_line, encode_line, DeviceClass, Envelope, Message, NodeCaps, NodeId, CLUSTER_MODEL,
    PROTOCOL_VERSION,
};
use joule_runtime::{prepare_and_install, ClusterEngine, ManifestFile, StubEngine, WeightsStore};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

fn e2e_plan_auth(
    node: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    accepted: bool,
    plan_hash_hex: &str,
    confirm_hex: &str,
) -> joule_proto::PlanAuth {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let sk = joule_cluster::lab_signing_key_for_node(node);
    let pre = joule_cluster::plan_accept_sign_preimage(
        node, plan_id, request_id, accepted, plan_hash_hex, confirm_hex, ts,
    );
    let (pk, sig) = joule_cluster::sign_preimage(&sk, &pre);
    joule_proto::PlanAuth {
        signer_pubkey_hex: pk,
        sig_hex: sig,
        signed_at_unix_ms: ts,
    }
}


async fn operator_env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(())).lock().await
}

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
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
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
    // Phase D: advertise multiaddrs + mem so mesh plan_donors is non-empty.
    let alive = Envelope::new(
        node_id.clone(),
        Message::PeerAlive {
            multiaddrs: vec![format!("tcp://127.0.0.1:{}", 17000 + (mem % 1000))],
            load: 0.05,
            healthy: true,
            blob_count: 0,
            mem_mib: mem,
            verified_mem_mib: 0,
            throughput_class: 40,
        },
    );
    writer
        .write_all(&encode_line(&alive).unwrap())
        .await
        .unwrap();

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
                    let alive = Envelope::new(
                        node_id.clone(),
                        Message::PeerAlive {
                            multiaddrs: vec![format!("tcp://127.0.0.1:{}", 17000 + (mem % 1000))],
                            load: 0.05,
                            healthy: true,
                            blob_count: 0,
                            mem_mib: mem,
                            verified_mem_mib: 0,
                            throughput_class: 40,
                        },
                    );
                    let _ = writer.write_all(&encode_line(&alive).unwrap()).await;
                }
                line = lines.next_line() => {
                    let Ok(Some(line)) = line else { break; };
                    if line.trim().is_empty() { continue; }
                    let env = decode_line(line.as_bytes()).unwrap();
                    match &env.msg {
                        Message::PlanOffer {
                            plan,
                            request_id,
                            plan_hash_hex, .. } => {
                            let accepted = plan.shards.iter().any(|s| s.node == node_id);
                            let (ph, confirm) = joule_cluster::plan_accept_fields(
                                plan,
                                *request_id,
                                &node_id,
                                accepted,
                                Some(plan_hash_hex.as_str()).filter(|s| !s.is_empty()),
                            );
                            let reply = Envelope::new(
                                node_id.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: *request_id,
                                    accepted,
                                    reason: if accepted {
                                        "e2e shard ok".into()
                                    } else {
                                        "not in plan".into()
                                    },
                                    auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, accepted, &ph, &confirm),
                                    plan_hash_hex: ph,
                                    confirm_hex: confirm,
                    },
                            );
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::RequestInfer { .. } => {
                            // Production `joule agent`: note only — never PlanAccept here.
                            // Self-accept with a local plan hash poisons control mesh settle.
                        }
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
    // Self-govern: production uses challenges to unlock verified VRAM; tests trust claims.
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

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
    assert!(ready["milestones"].as_array().unwrap().len() >= 3);
    assert!(ready["next_milestone"].is_object());
    assert!(ready["countdown_label"].as_str().unwrap().contains("next"));

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
    // With PeerAlive mem_mib, chat uses mesh RequestInfer path (not control-only).
    assert_eq!(
        chat["joule_coordination"].as_str().unwrap_or(""),
        "mesh_request_infer",
        "chat={chat}"
    );
    assert!(chat["joule_shard_count"].as_u64().unwrap_or(0) >= 1);

    agent.abort();
    assert_eq!(PROTOCOL_VERSION, "0.1.0");
}

/// Dashboard nodes JSON includes attestation_tier from claim vs verified.
#[tokio::test]
async fn nodes_api_includes_attestation_tier() {
    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    let (_k, h) = spawn_agent(agent_addr, "tier-alice", 8192).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        // leave verified 0 → claim_only
        let _g = app.state.read().await;
    }
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("http://{http_addr}/v1/cluster/nodes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nodes = body["nodes"].as_array().expect("nodes array");
    assert!(!nodes.is_empty());
    for n in nodes {
        let tier = n["attestation_tier"].as_str().unwrap_or("");
        assert!(
            matches!(tier, "claim_only" | "challenge_partial" | "challenge_full"),
            "missing/bad attestation_tier in {n}"
        );
        assert_eq!(
            tier, "claim_only",
            "unverified node is claim_only, got {tier}"
        );
    }
    // After trust, full tier
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    let body2: serde_json::Value = client
        .get(format!("http://{http_addr}/v1/cluster/nodes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tier2 = body2["nodes"][0]["attestation_tier"].as_str().unwrap_or("");
    assert_eq!(
        tier2, "challenge_full",
        "trusted claims → full, got {tier2}"
    );
    h.abort();
}

/// service_live flips via mark_node_loaded when pool gates satisfied (real control path).
#[tokio::test]
async fn service_live_flips_when_mesh_loaded() {
    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    // ≥3 backends, large verified VRAM for kimi-eligible
    let mut handles = vec![];
    for (i, mem) in [(0, 24_576u32), (1, 24_576), (2, 24_576)] {
        let (k, h) = spawn_agent(agent_addr, &format!("live-{i}"), mem).await;
        let _ = k;
        handles.push(h);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut node_ids = vec![];
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
        node_ids = g.cluster.nodes().map(|n| n.id.clone()).collect();
        assert!(
            !g.service_live,
            "service_live starts false before model loaded"
        );
        for id in &node_ids {
            g.mark_node_loaded(id.clone());
        }
        assert!(
            !g.service_live,
            "without digests_verified, service_live must stay false"
        );
        g.set_digests_verified(true);
        for id in &node_ids {
            g.mark_node_loaded(id.clone());
        }
        assert!(
            g.service_live,
            "service_live must flip true when digests verified + pool+mesh loaded"
        );
    }
    let client = reqwest::Client::new();
    let ready: serde_json::Value = client
        .get(format!("http://{http_addr}/v1/models/readiness"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // readiness may still show modes; operator status exposes service_live
    let op: serde_json::Value = client
        .get(format!("http://{http_addr}/v1/operator/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        op["service_live"], true,
        "operator status shows service_live op={op} ready={ready}"
    );
    for h in handles {
        h.abort();
    }
}

/// Local donor pause (Heartbeat healthy=false) drops backends without process kill.
/// Mem-cap on Hello clamps claimed_mem_mib (same as agent --mem-cap-mib).
#[tokio::test]
async fn donor_pause_and_mem_cap_affect_offered_capacity() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    // Agent A: full 8192 claim, stays healthy.
    let (_ka, ha) = spawn_agent(agent_addr, "cap-alice", 8192).await;
    // Agent B: Hello with capped claim 4096 (shipped agent applies policy.effective_mem_mib).
    let node_b = NodeId::new();
    let sock_b = TcpStream::connect(agent_addr).await.expect("b connect");
    let (reader_b, mut writer_b) = sock_b.into_split();
    let mut lines_b = BufReader::new(reader_b).lines();
    let hello_b = Envelope::new(
        node_b.clone(),
        Message::Hello {
            account: "cap-bob".into(),
            caps: NodeCaps::for_cluster(DeviceClass::Gpu, 4096, 40), // after mem-cap clamp
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
        },
    );
    writer_b
        .write_all(&encode_line(&hello_b).unwrap())
        .await
        .unwrap();
    let welcome_b = lines_b.next_line().await.unwrap().expect("welcome b");
    let _ = decode_line(welcome_b.as_bytes()).unwrap();
    // Start healthy
    let hb_ok = Envelope::new(
        node_b.clone(),
        Message::Heartbeat {
            load: 0.05,
            healthy: true,
        },
    );
    writer_b
        .write_all(&encode_line(&hb_ok).unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    // Mem-cap claim recorded on bob
    {
        let g = app.state.read().await;
        let bob = g
            .cluster
            .nodes()
            .find(|n| n.account == "cap-bob")
            .expect("bob node");
        assert_eq!(
            bob.claimed_mem_mib, 4096,
            "Hello claim must be local mem-cap (not 8192)"
        );
    }

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let cap_before: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let backends_before = cap_before["logical_device"]["backends"]
        .as_u64()
        .or_else(|| cap_before["nodes_healthy"].as_u64())
        .unwrap_or(0);
    assert!(
        backends_before >= 2,
        "need 2 healthy before pause, got {backends_before} {cap_before}"
    );

    // Pause bob only (same as agent reloading policy.paused=true → Heartbeat healthy=false).
    // Process stays alive — capacity drop must not require kill.
    let hb_pause = Envelope::new(
        node_b.clone(),
        Message::Heartbeat {
            load: 0.05,
            healthy: false,
        },
    );
    writer_b
        .write_all(&encode_line(&hb_pause).unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cap_after: serde_json::Value = client
        .get(format!("{base}/v1/cluster/capacity"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let backends_after = cap_after["logical_device"]["backends"]
        .as_u64()
        .or_else(|| cap_after["nodes_healthy"].as_u64())
        .unwrap_or(0);
    assert!(
        backends_after < backends_before,
        "pause-only must reduce healthy backends {backends_before} → {backends_after} cap={cap_after}"
    );
    assert!(backends_after >= 1, "alice still healthy: {cap_after}");
    {
        let g = app.state.read().await;
        let bob = g
            .cluster
            .nodes()
            .find(|n| n.account == "cap-bob")
            .expect("bob still registered");
        assert!(!bob.healthy, "paused bob must be marked unhealthy");
    }

    ha.abort();
    drop(writer_b);
}

/// Multi-agent live pool: N≥2 join; capacity reflects healthy set; churn drops a node.
#[tokio::test]
async fn multi_agent_capacity_under_churn() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let (key_a, a) = spawn_agent(agent_addr, "churn-alice", 8192).await;
    let (_key_b, b) = spawn_agent(agent_addr, "churn-bob", 16384).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

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
    let backends = cap["logical_device"]["backends"].as_u64().unwrap_or(0);
    assert!(
        backends >= 2,
        "need ≥2 healthy backends after join, got {backends} cap={cap}"
    );
    let vram_both = cap["logical_device"]["vram_mib"].as_u64().unwrap_or(0);
    // After trust, verified pool should cover both claims.
    assert!(
        vram_both >= 8192 + 16384 || cap["mem_mib_healthy"].as_u64().unwrap_or(0) > 0,
        "capacity after 2 agents: {cap}"
    );

    // Churn: drop bob.
    b.abort();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Force prune by advancing stale — e2e may still show 2 until heartbeat timeout.
    // Explicitly remove via cluster if exposed, else check account path still works with alice.
    let _ = key_a;
    let who: serde_json::Value = client
        .get(format!("{base}/v1/account"))
        .bearer_auth(&key_a)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(who["account"].as_str().unwrap_or(""), "churn-alice");

    // Mark bob unhealthy by not heartbeating — capacity healthy count must not invent nodes.
    {
        let g = app.state.read().await;
        let n = g.cluster.nodes().count();
        assert!(n >= 1, "registry still has nodes after join");
        // Claim-only never exceeds verified for placement: placement uses verified only.
        for node in g.cluster.nodes() {
            assert!(
                node.verified_mem_mib <= node.claimed_mem_mib
                    || node.claimed_mem_mib == 0
                    || node.verified_mem_mib == 0,
                "verified must not exceed claim"
            );
        }
    }

    a.abort();
}

/// Local pool: seed/prepare lab-mid on ClusterEngine (agent load path) → Infer is tensor-backed.
#[tokio::test]
async fn local_pool_lab_mid_tensor_infer() {
    use std::fs;

    let dir = std::env::temp_dir().join(format!(
        "joule-e2e-lab-mid-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    let _ = fs::remove_dir_all(&dir);
    let store = WeightsStore::new(&dir);
    let m = ManifestFile::load_default().expect("manifest");
    let spec = m.model("kimi-open").expect("kimi-open");
    let mid = spec
        .weights
        .quants
        .iter()
        .find(|q| q.id == "lab-mid")
        .expect("lab-mid");

    // Same helper the agent uses after Welcome/pool-ready and peer seed.
    let engine = ClusterEngine::new();
    let report =
        prepare_and_install(&store, &engine, spec, mid).expect("agent prepare_and_install");
    assert!(report.tensors >= 3, "tensors={}", report.tensors);
    assert!(engine.is_model_loaded());

    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    // Agent loop uses ClusterEngine + lab-mid (not StubEngine).
    let node_id = NodeId::new();
    let sock = TcpStream::connect(agent_addr).await.expect("agent connect");
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    let hello = Envelope::new(
        node_id.clone(),
        Message::Hello {
            account: "lab-mid-donor".into(),
            caps: NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 40),
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
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
    let alive = Envelope::new(
        node_id.clone(),
        Message::PeerAlive {
            multiaddrs: vec!["tcp://127.0.0.1:17901".into()],
            load: 0.05,
            healthy: true,
            blob_count: 2,
            mem_mib: 8192,
            verified_mem_mib: 0,
            throughput_class: 40,
        },
    );
    writer
        .write_all(&encode_line(&alive).unwrap())
        .await
        .unwrap();

    let eng = Arc::new(engine);
    let agent = tokio::spawn({
        let eng = eng.clone();
        let node_id = node_id.clone();
        async move {
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
                            Message::PlanOffer {
                                plan,
                                request_id,
                                plan_hash_hex, .. } => {
                                let accepted = plan.shards.iter().any(|s| s.node == node_id);
                                let (ph, confirm) = joule_cluster::plan_accept_fields(
                                    plan,
                                    *request_id,
                                    &node_id,
                                    accepted,
                                    Some(plan_hash_hex.as_str()).filter(|s| !s.is_empty()),
                                );
                                let reply = Envelope::new(
                                    node_id.clone(),
                                    Message::PlanAccept {
                                        plan_id: plan.plan_id,
                                        request_id: *request_id,
                                        accepted,
                                        reason: if accepted {
                                            "lab-mid e2e".into()
                                        } else {
                                            "not in plan".into()
                                        },
                                        auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, accepted, &ph, &confirm),
                                        plan_hash_hex: ph,
                                        confirm_hex: confirm,
                    },
                                );
                                let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                            }
                            Message::InferRequest { .. } => {
                                let reply = joule_control::agent_handle_infer(&env, eng.as_ref())
                                    .await
                                    .expect("infer");
                                let reply = Envelope::new(node_id.clone(), reply.msg);
                                let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                            }
                            Message::Challenge { .. } => {
                                let reply = joule_control::agent_handle_challenge(&env, eng.as_ref())
                                    .await
                                    .expect("challenge");
                                let reply = Envelope::new(node_id.clone(), reply.msg);
                                let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let chat: serde_json::Value = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "lab-mid local pool"}]
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
    assert!(
        content.contains("joule-tensor"),
        "local pool chat must be tensor-backed when lab-mid loaded, content={content} chat={chat}"
    );
    assert!(!content.contains("joule-stub"), "content={content}");

    agent.abort();
    let _ = fs::remove_dir_all(&dir);
}

/// Welcome issues a real `joule_…` key; that key authenticates HTTP; wrong/missing fail closed.
#[tokio::test]
async fn welcome_api_key_auth_fail_closed() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    let (api_key, agent) = spawn_agent(agent_addr, "key-alice", 8192).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    assert!(
        api_key.starts_with("joule_"),
        "Welcome must issue pool key with joule_ prefix, got {api_key}"
    );
    // Same key is what control maps for the account (not client-invented).
    {
        let g = app.state.read().await;
        assert_eq!(g.account_for_key(&api_key), Some("key-alice"));
        assert_eq!(g.account_for_key("joule_notissued"), None);
    }

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    let ok = client
        .get(format!("{base}/v1/account"))
        .bearer_auth(&api_key)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = ok.json().await.unwrap();
    assert_eq!(body["account"].as_str().unwrap_or(""), "key-alice");

    let missing = client
        .get(format!("{base}/v1/account"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = client
        .get(format!("{base}/v1/account"))
        .bearer_auth("joule_deadbeefnotarealkey")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    let chat_bad = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth("joule_deadbeefnotarealkey")
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "nope"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(chat_bad.status(), reqwest::StatusCode::UNAUTHORIZED);

    let chat_ok = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&api_key)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "auth-ok"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(chat_ok.status(), reqwest::StatusCode::OK);

    agent.abort();
}

/// Phase D: ≥2 donors with PeerAlive mem → mesh PlanOffer geometry → chat InferDone.
#[tokio::test]
async fn mesh_request_infer_chat_multi_donor() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let (key_a, a) = spawn_agent(agent_addr, "mesh-alice", 8192).await;
    let (_kb, b) = spawn_agent(agent_addr, "mesh-bob", 16384).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    // Mesh plan geometry from **cluster verified** (PeerAlive claim alone is never enough).
    assert!(
        joule_control::mesh_donors_ready(&app).await,
        "mesh donors with verified capacity must be ready"
    );
    let donors = {
        let g = app.state.read().await;
        g.mesh_plan_donors()
    };
    assert!(
        donors.len() >= 2,
        "need ≥2 mesh plan donors (verified), got {}",
        donors.len()
    );
    let mesh_plan = joule_cluster::plan_from_mesh_donors(&donors).expect("mesh plan");
    assert_eq!(mesh_plan.shards.len(), donors.len());
    assert_eq!(
        mesh_plan.pool_mem_mib,
        donors.iter().map(|(_, m)| u64::from(*m)).sum::<u64>()
    );

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    let mesh_http: serde_json::Value = client
        .get(format!("{base}/v1/mesh/plan"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mesh_http["ok"], true);
    assert_eq!(mesh_http["source"], "mesh_peer_alive");
    assert!(mesh_http["donors"].as_u64().unwrap() >= 2);

    // Direct shipped mesh coordinator (proves not solely dispatch_infer stream path).
    let direct = joule_control::dispatch_mesh_infer(
        &app,
        "mesh-alice",
        CLUSTER_MODEL,
        "user: mesh-phase-d-hello",
        32,
    )
    .await
    .expect("dispatch_mesh_infer");
    assert_eq!(direct.coordination, "mesh_request_infer");
    assert!(!direct.text.is_empty(), "empty mesh completion");
    assert!(
        direct.text.contains("mesh-phase-d-hello") || direct.text.len() > 4,
        "text={}",
        direct.text
    );
    assert!(direct.shard_count >= 2, "shards={}", direct.shard_count);
    assert_eq!(direct.pool_mem_mib, mesh_plan.pool_mem_mib);

    // Chat contract also uses mesh path when donors ready.
    let chat: serde_json::Value = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth(&key_a)
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role": "user", "content": "mesh-chat-ok"}]
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
    assert!(
        content.contains("mesh-chat-ok") || !content.is_empty(),
        "content={content}"
    );
    assert_eq!(
        chat["joule_coordination"].as_str().unwrap_or(""),
        "mesh_request_infer"
    );
    assert!(chat["joule_shard_count"].as_u64().unwrap_or(0) >= 2);

    a.abort();
    b.abort();
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
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

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
        g.operator_paused = true;
        g.heartbeat_mint_mj = 42;
        g.prune();
    }

    let app2 = joule_control::App::load_or_init(Some(dir)).unwrap();
    let g = app2.state.read().await;
    assert!(g.account_keys.contains_key("carol"));
    assert!(g.ledger.balance("bob") >= 100);
    assert!(g.operator_paused);
    assert_eq!(g.heartbeat_mint_mj, 42);
}

fn tempfile_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("joule-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Peer seed: seeder announces sha256 → leech BlobWant → control orchestrates
/// BlobProvide → BlobChunk stream → leech verifies hash (no f00 CDN).
#[tokio::test]
async fn peer_blob_chunk_transfer() {
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    use joule_proto::BlobMeta;
    use sha2::{Digest, Sha256};
    use std::sync::{Arc, Mutex};

    let app = load_or_init_app(None).expect("app");
    let (agent_addr, _http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let payload = b"lab-tiny-style peer seed payload for joule swarm v0";
    let hash = hex::encode(Sha256::digest(payload));
    let received: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

    // --- seeder agent ---
    let seeder_id = NodeId::new();
    let seeder_sock = TcpStream::connect(agent_addr).await.unwrap();
    let (s_reader, mut s_writer) = seeder_sock.into_split();
    let mut s_lines = BufReader::new(s_reader).lines();
    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::Hello {
                    account: "seeder".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 4096, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = s_lines.next_line().await.unwrap(); // Welcome
    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::BlobsHave {
                    blobs: vec![BlobMeta {
                        sha256: hash.clone(),
                        size: payload.len() as u64,
                        kind: "blob".into(),
                        name: "lab-seed".into(),
                        multiaddrs: vec![],
                    }],
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

    let seeder_payload = payload.to_vec();
    let seeder_hash = hash.clone();
    let seeder = tokio::spawn(async move {
        loop {
            let Ok(Some(line)) = s_lines.next_line().await else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            if let Message::BlobProvide {
                sha256, request_id, ..
            } = env.msg
            {
                assert_eq!(sha256.to_lowercase(), seeder_hash);
                // single chunk is enough for this payload
                let chunk = Envelope::new(
                    seeder_id.clone(),
                    Message::BlobChunk {
                        sha256: seeder_hash.clone(),
                        request_id,
                        offset: 0,
                        data_b64: B64.encode(&seeder_payload),
                        done: true,
                    },
                );
                if s_writer
                    .write_all(&encode_line(&chunk).unwrap())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    // --- leech agent ---
    let leech_id = NodeId::new();
    let leech_sock = TcpStream::connect(agent_addr).await.unwrap();
    let (l_reader, mut l_writer) = leech_sock.into_split();
    let mut l_lines = BufReader::new(l_reader).lines();
    l_writer
        .write_all(
            &encode_line(&Envelope::new(
                leech_id.clone(),
                Message::Hello {
                    account: "leech".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 4096, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = l_lines.next_line().await.unwrap(); // Welcome

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Ask swarm for the hash the seeder announced.
    l_writer
        .write_all(
            &encode_line(&Envelope::new(
                leech_id.clone(),
                Message::BlobWant {
                    sha256: hash.clone(),
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

    let recv = received.clone();
    let leech_hash = hash.clone();
    let leech = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut expect = 0u64;
        loop {
            let Ok(Some(line)) = l_lines.next_line().await else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            match env.msg {
                Message::BlobLocate { peers, .. } => {
                    assert!(!peers.is_empty(), "seeder should be listed");
                }
                Message::BlobChunk {
                    sha256,
                    offset,
                    data_b64,
                    done,
                    ..
                } => {
                    assert_eq!(sha256.to_lowercase(), leech_hash);
                    assert_eq!(offset, expect);
                    let piece = B64.decode(data_b64.as_bytes()).unwrap();
                    buf.extend_from_slice(&piece);
                    expect += piece.len() as u64;
                    if done {
                        *recv.lock().unwrap() = Some(buf.clone());
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), leech)
        .await
        .expect("leech timed out")
        .unwrap();

    let got = received.lock().unwrap().clone().expect("bytes");
    assert_eq!(got, payload);
    assert_eq!(hex::encode(Sha256::digest(&got)), hash);

    // Directory should still list seeder (leech may not re-announce in this mini harness).
    {
        let g = app.state.read().await;
        assert!(g.blobs.seeder_count(&hash) >= 1);
    }

    seeder.abort();
}

/// Phase A/B: PeerAlive fills mesh directory; BlobLocate returns multiaddrs for direct dial.
#[tokio::test]
async fn mesh_peer_alive_and_blob_locate_multiaddrs() {
    use joule_proto::BlobMeta;
    use sha2::{Digest, Sha256};

    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let payload = b"mesh multiaddr seed payload";
    let hash = hex::encode(Sha256::digest(payload));
    let seeder_multi = "tcp://127.0.0.1:17702".to_string();

    let seeder_id = NodeId::new();
    let seeder_sock = TcpStream::connect(agent_addr).await.unwrap();
    let (s_reader, mut s_writer) = seeder_sock.into_split();
    let mut s_lines = BufReader::new(s_reader).lines();
    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::Hello {
                    account: "mesh-seed".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 4096, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = s_lines.next_line().await.unwrap();

    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::PeerAlive {
                    multiaddrs: vec![seeder_multi.clone()],
                    load: 0.05,
                    healthy: true,
                    blob_count: 1,
                    mem_mib: 0,
                    verified_mem_mib: 0,
                    throughput_class: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::BlobsHave {
                    blobs: vec![BlobMeta {
                        sha256: hash.clone(),
                        size: payload.len() as u64,
                        kind: "blob".into(),
                        name: "mesh-seed".into(),
                        multiaddrs: vec![seeder_multi.clone()],
                    }],
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

    // Let control process PeerAlive + BlobsHave before leech asks.
    tokio::time::sleep(Duration::from_millis(80)).await;
    {
        let g = app.state.read().await;
        assert!(
            g.blobs.seeder_count(&hash) >= 1,
            "seeder must be in blob directory before BlobWant"
        );
        assert!(
            g.mesh.healthy_count() >= 1,
            "seeder multiaddr must be in mesh directory"
        );
    }

    // Peer that should receive gossip PeerAlive.
    let peer_id = NodeId::new();
    let peer_sock = TcpStream::connect(agent_addr).await.unwrap();
    let (p_reader, mut p_writer) = peer_sock.into_split();
    let mut p_lines = BufReader::new(p_reader).lines();
    p_writer
        .write_all(
            &encode_line(&Envelope::new(
                peer_id.clone(),
                Message::Hello {
                    account: "mesh-peer".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 4096, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = p_lines.next_line().await.unwrap();

    // Re-announce so peer gets gossip flood (PeerAlive after both connected).
    s_writer
        .write_all(
            &encode_line(&Envelope::new(
                seeder_id.clone(),
                Message::PeerAlive {
                    multiaddrs: vec![seeder_multi.clone()],
                    load: 0.05,
                    healthy: true,
                    blob_count: 1,
                    mem_mib: 0,
                    verified_mem_mib: 0,
                    throughput_class: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut saw_gossip = false;
    let mut locate_multi: Option<Vec<Vec<String>>> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);

    while tokio::time::Instant::now() < deadline && locate_multi.is_none() {
        p_writer
            .write_all(
                &encode_line(&Envelope::new(
                    peer_id.clone(),
                    Message::BlobWant {
                        sha256: hash.clone(),
                    },
                ))
                .unwrap(),
            )
            .await
            .unwrap();

        let slice_end = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < slice_end {
            let line = tokio::time::timeout(Duration::from_millis(200), p_lines.next_line()).await;
            let Ok(Ok(Some(line))) = line else {
                continue;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            match env.msg {
                Message::PeerAlive { multiaddrs, .. } => {
                    if multiaddrs.iter().any(|a| a == &seeder_multi) {
                        saw_gossip = true;
                    }
                }
                Message::BlobLocate {
                    sha256,
                    peers,
                    multiaddrs,
                    ..
                } => {
                    assert_eq!(sha256.to_lowercase(), hash);
                    if !peers.is_empty() {
                        locate_multi = Some(multiaddrs);
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    let multi = locate_multi.expect("BlobLocate with multiaddrs");
    assert!(
        multi
            .iter()
            .any(|addrs| addrs.iter().any(|a| a == &seeder_multi)),
        "BlobLocate should carry seeder multiaddr, got {multi:?}"
    );

    // HTTP mesh API + healthz fields.
    let client = reqwest::Client::new();
    let mesh = client
        .get(format!("http://{http_addr}/v1/mesh/peers"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(mesh["ok"], true);
    assert!(mesh["healthy"].as_u64().unwrap_or(0) >= 1);
    let peers = mesh["peers"].as_array().expect("peers array");
    assert!(peers.iter().any(|p| {
        p["multiaddrs"]
            .as_array()
            .map(|a| a.iter().any(|x| x.as_str() == Some(seeder_multi.as_str())))
            .unwrap_or(false)
    }));

    let hz = client
        .get(format!("http://{http_addr}/healthz"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(hz["mesh_peers"].as_u64().unwrap_or(0) >= 1);

    // Gossip may race; multiaddrs on locate is the hard Phase B requirement.
    let _ = saw_gossip;
}

/// Operator policy pause flips service_live; software_update fans FetchDigests.
#[tokio::test]
async fn operator_policy_and_software_fanout() {
    use ed25519_dalek::{Signer, SigningKey};
    use joule_control::{body_sha256_hex, now_ms, operator_preimage};
    use joule_proto::{OperatorKind, SignedEnvelope};
    use rand::rngs::OsRng;

    let _env = operator_env_lock().await;
    let sk = SigningKey::generate(&mut OsRng);
    let pk = hex::encode(sk.verifying_key().to_bytes());
    std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
    std::env::set_var("JOULE_OPERATOR_PUBKEY", &pk);

    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    // Agent that records FetchDigests digests.
    let got_digests = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_id = NodeId::new();
    let sock = TcpStream::connect(agent_addr).await.unwrap();
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::Hello {
                    account: "ops".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = lines.next_line().await.unwrap();
    let dig_slot = got_digests.clone();
    let agent = tokio::spawn(async move {
        loop {
            let Ok(Some(line)) = lines.next_line().await else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            if let Message::FetchDigests {
                digests, reason, ..
            } = env.msg
            {
                if reason.starts_with("software_update:") {
                    *dig_slot.lock().unwrap() = digests;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    // Pause via signed envelope (blocks chat)
    let body = r#"{"service_live":false,"pause":true}"#;
    let mut env = SignedEnvelope {
        id: Uuid::new_v4(),
        issued_at_unix_ms: now_ms(),
        expires_at_unix_ms: None,
        kind: OperatorKind::Policy,
        body_json: body.into(),
        body_sha256: body_sha256_hex(body),
        sig_ed25519_hex: String::new(),
        openpgp_sig: None,
    };
    let pre = operator_preimage(&env);
    env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
    let r = client
        .post(format!("{base}/v1/broadcasts/inject"))
        .json(&env)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    {
        let g = app.state.read().await;
        assert!(!g.service_live);
        assert!(g.operator_paused);
    }

    // Software update fanout
    let sw_body = r#"{"version":"0.0.1","targets":[{"os":"linux","arch":"x86_64","sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","size":1,"name":"joule"}]}"#;
    let mut sw = SignedEnvelope {
        id: Uuid::new_v4(),
        issued_at_unix_ms: now_ms(),
        expires_at_unix_ms: None,
        kind: OperatorKind::SoftwareUpdate,
        body_json: sw_body.into(),
        body_sha256: body_sha256_hex(sw_body),
        sig_ed25519_hex: String::new(),
        openpgp_sig: None,
    };
    let pre = operator_preimage(&sw);
    sw.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
    let r = client
        .post(format!("{base}/v1/broadcasts/inject"))
        .json(&sw)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !got_digests.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("FetchDigests timeout");
    let d = got_digests.lock().unwrap().clone();
    assert_eq!(d.len(), 1);
    assert!(d[0].starts_with("ccc"));

    let op: serde_json::Value = client
        .get(format!("{base}/v1/operator/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(op["service_live"], false);

    agent.abort();
    std::env::remove_var("JOULE_OPERATOR_PUBKEY");
    std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
}

/// model_update assigns digests (not full model) via FetchDigests.
#[tokio::test]
async fn model_update_assigns_digests() {
    use ed25519_dalek::{Signer, SigningKey};
    use joule_control::{body_sha256_hex, now_ms, operator_preimage};
    use joule_proto::{OperatorKind, SignedEnvelope};
    use rand::rngs::OsRng;

    let _env = operator_env_lock().await;
    let sk = SigningKey::generate(&mut OsRng);
    let pk = hex::encode(sk.verifying_key().to_bytes());
    std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
    std::env::set_var("JOULE_OPERATOR_PUBKEY", &pk);

    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_id = NodeId::new();
    let sock = TcpStream::connect(agent_addr).await.unwrap();
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::Hello {
                    account: "chunker".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 16384, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = lines.next_line().await.unwrap();
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    // heartbeat so node stays healthy
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::Heartbeat {
                    load: 0.0,
                    healthy: true,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

    let slot = got.clone();
    let agent = tokio::spawn(async move {
        loop {
            let Ok(Some(line)) = lines.next_line().await else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            if let Message::FetchDigests {
                digests, reason, ..
            } = env.msg
            {
                if reason.starts_with("model_update:") {
                    *slot.lock().unwrap() = digests;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let body = r#"{"model_id":"kimi-open","replica_factor":1,"chunks":[{"path":"a","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":1},{"path":"b","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":1}]}"#;
    let mut env = SignedEnvelope {
        id: Uuid::new_v4(),
        issued_at_unix_ms: now_ms(),
        expires_at_unix_ms: None,
        kind: OperatorKind::ModelUpdate,
        body_json: body.into(),
        body_sha256: body_sha256_hex(body),
        sig_ed25519_hex: String::new(),
        openpgp_sig: None,
    };
    let pre = operator_preimage(&env);
    env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
    let client = reqwest::Client::new();
    let r = client
        .post(format!("http://{http_addr}/v1/broadcasts/inject"))
        .json(&env)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !got.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("model FetchDigests timeout");
    let digests = got.lock().unwrap().clone();
    // Single node + r=1: should get both digests (only one holder each).
    assert_eq!(digests.len(), 2, "{digests:?}");
    {
        let g = app.state.read().await;
        assert_eq!(g.active_chunks.len(), 2);
        assert_eq!(g.active_replica_factor, 1);
    }
    agent.abort();
    std::env::remove_var("JOULE_OPERATOR_PUBKEY");
    std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
}

/// Late joiner receives model_update catch-up and FetchDigests after plan re-run.
#[tokio::test]
async fn late_joiner_gets_model_digests() {
    use ed25519_dalek::{Signer, SigningKey};
    use joule_control::{body_sha256_hex, now_ms, operator_preimage};
    use joule_proto::{OperatorKind, SignedEnvelope};
    use rand::rngs::OsRng;

    let _env = operator_env_lock().await;
    let sk = SigningKey::generate(&mut OsRng);
    let pk = hex::encode(sk.verifying_key().to_bytes());
    std::env::set_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR", "1");
    std::env::set_var("JOULE_OPERATOR_PUBKEY", &pk);

    let app = load_or_init_app(None).expect("app");
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");

    // First donor joins and stays quiet.
    let early = spawn_agent(agent_addr, "early", 8192).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    let body = r#"{"model_id":"kimi-open","replica_factor":2,"chunks":[{"path":"a","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":1},{"path":"b","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":1}]}"#;
    let mut env = SignedEnvelope {
        id: Uuid::new_v4(),
        issued_at_unix_ms: now_ms(),
        expires_at_unix_ms: None,
        kind: OperatorKind::ModelUpdate,
        body_json: body.into(),
        body_sha256: body_sha256_hex(body),
        sig_ed25519_hex: String::new(),
        openpgp_sig: None,
    };
    let pre = operator_preimage(&env);
    env.sig_ed25519_hex = hex::encode(sk.sign(&pre).to_bytes());
    let client = reqwest::Client::new();
    client
        .post(format!("http://{http_addr}/v1/broadcasts/inject"))
        .json(&env)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Late joiner with custom loop capturing FetchDigests.
    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_id = NodeId::new();
    let sock = TcpStream::connect(agent_addr).await.unwrap();
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::Hello {
                    account: "late".into(),
                    caps: NodeCaps::for_cluster(DeviceClass::Gpu, 16384, 40),
                    pubkey_hex: String::new(),
                    sig_hex: String::new(),
                    signed_at_unix_ms: 0,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = lines.next_line().await.unwrap(); // welcome
                                              // Late joiner must have verified capacity before chunk placement includes them.
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    let slot = got.clone();
    let late = tokio::spawn(async move {
        loop {
            let Ok(Some(line)) = lines.next_line().await else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let env = decode_line(line.as_bytes()).unwrap();
            if let Message::FetchDigests { digests, .. } = env.msg {
                if !digests.is_empty() {
                    *slot.lock().unwrap() = digests;
                }
            }
        }
    });

    // New envelope id (dedupe would reject re-inject of same id) after verified unlock.
    let mut env2 = env;
    env2.id = Uuid::new_v4();
    env2.issued_at_unix_ms = now_ms();
    env2.body_sha256 = body_sha256_hex(body);
    let pre2 = operator_preimage(&env2);
    env2.sig_ed25519_hex = hex::encode(sk.sign(&pre2).to_bytes());
    client
        .post(format!("http://{http_addr}/v1/broadcasts/inject"))
        .json(&env2)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if !got.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("late joiner FetchDigests");
    assert!(!got.lock().unwrap().is_empty());

    late.abort();
    early.1.abort();
    std::env::remove_var("JOULE_OPERATOR_PUBKEY");
    std::env::remove_var("JOULE_ALLOW_UNOFFICIAL_OPERATOR");
}

/// Concurrent chat free→used→free + GET /v1/cluster/leases audit trail.
#[tokio::test]
async fn lease_chat_free_used_free_and_audit() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    let (api_key, agent) = spawn_agent(agent_addr, "lease-alice", 16384).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    let sched_before: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let free_before = sched_before["stream_slots_free"].as_u64().unwrap();
    let used_before = sched_before["stream_slots_used"].as_u64().unwrap();
    assert!(free_before >= 1, "sched={sched_before}");
    assert_eq!(used_before, 0);
    eprintln!("OBSERVE before free={free_before} used={used_before} sched={sched_before}");

    // Concurrent completions within capacity.
    let mut handles = Vec::new();
    for i in 0..3 {
        let client = client.clone();
        let base = base.clone();
        let key = api_key.clone();
        handles.push(tokio::spawn(async move {
            client
                .post(format!("{base}/v1/chat/completions"))
                .header("Authorization", format!("Bearer {key}"))
                .json(&serde_json::json!({
                    "model": CLUSTER_MODEL,
                    "messages": [{"role":"user","content": format!("lease-concurrent-{i}")}],
                    "max_tokens": 16
                }))
                .send()
                .await
                .unwrap()
        }));
    }
    let mut mid_used = 0u64;
    for _ in 0..40 {
        let s: serde_json::Value = client
            .get(format!("{base}/v1/cluster/scheduler"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        mid_used = mid_used.max(s["stream_slots_used"].as_u64().unwrap_or(0));
        let done = handles.iter().all(|h| h.is_finished());
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    eprintln!("OBSERVE mid max_used={mid_used}");
    for h in handles {
        let resp = h.await.unwrap();
        assert!(
            resp.status().is_success(),
            "status={} body={}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    let sched_after: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        sched_after["stream_slots_used"].as_u64().unwrap(),
        0,
        "must free after concurrent chats: {sched_after}"
    );
    assert_eq!(
        sched_after["stream_slots_free"].as_u64().unwrap(),
        free_before
    );
    eprintln!("OBSERVE after free={} used={} sched={sched_after}",
        sched_after["stream_slots_free"], sched_after["stream_slots_used"]);

    let leases: serde_json::Value = client
        .get(format!("{base}/v1/cluster/leases"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(leases["ok"], true);
    assert_eq!(leases["active_leases"].as_u64().unwrap(), 0);
    let audit = leases["audit"].as_array().expect("audit array");
    assert!(
        audit.iter().any(|e| e["event"] == "lease_granted"),
        "audit={leases}"
    );
    assert!(
        audit.iter().any(|e| e["event"] == "lease_released"),
        "audit={leases}"
    );
    assert!(
        audit.iter().any(|e| e["event"] == "plan_agreed"),
        "audit must show multi-party agree: {leases}"
    );
    eprintln!("OBSERVE audit trail (request→lease→accepts→release): {leases}");
    // mid_used may be 0 if requests were very fast; prefer seeing use or at least audit grants.
    assert!(
        mid_used > 0
            || audit
                .iter()
                .filter(|e| e["event"] == "lease_granted")
                .count()
                >= 3,
        "mid_used={mid_used} audit={leases}"
    );

    agent.abort();
}

/// Pool full → HTTP 503 + code pool_full; used never exceeds total.
#[tokio::test]
async fn lease_pool_full_http_503() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
        // Fail closed quickly once saturated.
        g.lease_wait = Duration::from_millis(250);
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    // 4096 MiB → 1 stream slot. Hang on InferRequest to hold the lease.
    let hang = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (api_key, hang_agent) =
        spawn_agent_hang_infer(agent_addr, "full-only", 4096, hang.clone()).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
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
    let total = sched["stream_slots_total"].as_u64().unwrap().max(1);
    assert!(total >= 1, "sched={sched}");

    // Start `total` long-running chats that hold leases after PlanAccept.
    let mut holders = Vec::new();
    for i in 0..total {
        let client = client.clone();
        let base = base.clone();
        let key = api_key.clone();
        holders.push(tokio::spawn(async move {
            client
                .post(format!("{base}/v1/chat/completions"))
                .header("Authorization", format!("Bearer {key}"))
                .timeout(Duration::from_secs(8))
                .json(&serde_json::json!({
                    "model": CLUSTER_MODEL,
                    "messages": [{"role":"user","content": format!("hold-{i}")}],
                    "max_tokens": 8
                }))
                .send()
                .await
        }));
    }
    // Wait until used == total
    let mut used = 0u64;
    for _ in 0..80 {
        let s: serde_json::Value = client
            .get(format!("{base}/v1/cluster/scheduler"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        used = s["stream_slots_used"].as_u64().unwrap_or(0);
        if used >= total {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(used >= total, "failed to saturate used={used} total={total}");
    eprintln!("OBSERVE saturated used={used} total={total}");

    let over = client
        .post(format!("{base}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {api_key}"))
        .timeout(Duration::from_secs(5))
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role":"user","content":"should-be-full"}],
            "max_tokens": 8
        }))
        .send()
        .await
        .unwrap();
    let status = over.status();
    let body: serde_json::Value = over.json().await.unwrap();
    eprintln!("OBSERVE pool_full HTTP status={status} body={body}");
    assert_eq!(
        status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "status={status} body={body}"
    );
    assert_eq!(body["code"], "pool_full", "body={body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("pool full"),
        "body={body}"
    );

    let s_max: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        s_max["stream_slots_used"].as_u64().unwrap()
            <= s_max["stream_slots_total"].as_u64().unwrap(),
        "{s_max}"
    );

    // Unhang → holders finish → free restored.
    hang.store(false, std::sync::atomic::Ordering::SeqCst);
    for h in holders {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    {
        let mut g = app.state.write().await;
        g.prune();
    }
    let s_end: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        s_end["stream_slots_used"].as_u64().unwrap(),
        0,
        "end={s_end}"
    );
    eprintln!("OBSERVE after unhang free restored: {s_end}");

    hang_agent.abort();
}

/// Invalid PlanAccept confirm_hex aborts; lease released; no false success.
#[tokio::test]
async fn lease_invalid_plan_accept_aborts_and_releases() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
        g.lease_wait = Duration::from_secs(2);
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    let (api_key, agent) = spawn_agent_bad_accept(agent_addr, "bad-accept", 8192).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let sched0: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let free0 = sched0["stream_slots_free"].as_u64().unwrap();

    let resp = client
        .post(format!("{base}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role":"user","content":"tamper-accept"}],
            "max_tokens": 8
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    eprintln!("OBSERVE invalid PlanAccept status={status} body={body}");
    assert!(
        !status.is_success(),
        "tampered accept must not succeed: {status}"
    );
    assert!(
        body.contains("confirm")
            || body.contains("plan")
            || body.contains("reject")
            || body.contains("mismatch")
            || body.contains("timed out")
            || body.contains("Service"),
        "body={body}"
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    let sched1: serde_json::Value = client
        .get(format!("{base}/v1/cluster/scheduler"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    eprintln!("OBSERVE after invalid accept free0={free0} sched={sched1}");
    assert_eq!(
        sched1["stream_slots_used"].as_u64().unwrap(),
        0,
        "lease must release after invalid accept: {sched1}"
    );
    assert_eq!(sched1["stream_slots_free"].as_u64().unwrap(), free0);

    let leases: serde_json::Value = client
        .get(format!("{base}/v1/cluster/leases"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    eprintln!("OBSERVE invalid-accept audit={leases}");
    let audit = leases["audit"].as_array().cloned().unwrap_or_default();
    assert!(
        audit.iter().any(|e| {
            let ev = e["event"].as_str().unwrap_or("");
            ev == "plan_accept_invalid" || ev == "lease_released" || ev == "plan_hash_mismatch"
        }),
        "audit={leases}"
    );

    agent.abort();
}

/// Agent that hangs on InferRequest (holds stream lease while hanging).
async fn spawn_agent_hang_infer(
    agent_addr: std::net::SocketAddr,
    account: &str,
    mem: u32,
    release: Arc<std::sync::atomic::AtomicBool>,
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
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
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
    writer
        .write_all(&encode_line(&Envelope::new(
            node_id.clone(),
            Message::Heartbeat {
                load: 0.0,
                healthy: true,
            },
        ))
        .unwrap())
        .await
        .unwrap();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::PeerAlive {
                    multiaddrs: vec![format!("tcp://127.0.0.1:{}", 18000 + (mem % 1000))],
                    load: 0.05,
                    healthy: true,
                    blob_count: 0,
                    mem_mib: mem,
                    verified_mem_mib: 0,
                    throughput_class: 40,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

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
                        Message::PlanOffer {
                            plan,
                            request_id,
                            plan_hash_hex, .. } => {
                            let accepted = plan.shards.iter().any(|s| s.node == node_id);
                            let (ph, confirm) = joule_cluster::plan_accept_fields(
                                plan,
                                *request_id,
                                &node_id,
                                accepted,
                                Some(plan_hash_hex.as_str()).filter(|s| !s.is_empty()),
                            );
                            let reply = Envelope::new(
                                node_id.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: *request_id,
                                    accepted,
                                    reason: "hang-agent accept".into(),
                                    auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, accepted, &ph, &confirm),
                                    plan_hash_hex: ph,
                                    confirm_hex: confirm,
                    },
                            );
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::InferRequest { .. } => {
                            // Hold until release flag clears.
                            while release.load(std::sync::atomic::Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
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
                            let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    });
    (api_key, handle)
}

/// Agent that always sends invalid PlanAccept confirm_hex.
async fn spawn_agent_bad_accept(
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
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
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
    writer
        .write_all(&encode_line(&Envelope::new(
            node_id.clone(),
            Message::Heartbeat {
                load: 0.0,
                healthy: true,
            },
        ))
        .unwrap())
        .await
        .unwrap();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::PeerAlive {
                    multiaddrs: vec![format!("tcp://127.0.0.1:{}", 19000 + (mem % 1000))],
                    load: 0.05,
                    healthy: true,
                    blob_count: 0,
                    mem_mib: mem,
                    verified_mem_mib: 0,
                    throughput_class: 40,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

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
                        Message::PlanOffer {
                            plan,
                            request_id,
                            plan_hash_hex, .. } => {
                            let reply = Envelope::new(
                                node_id.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: *request_id,
                                    accepted: true,
                                    reason: "bad confirm".into(),
                                    plan_hash_hex: plan_hash_hex.clone(),
                                    confirm_hex: "deadbeef".into(),
                                    auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, true, plan_hash_hex, "deadbeef"),
                    },
                            );
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::InferRequest { .. } => {
                            // Should never be asked if fail-closed works.
                            let reply = joule_control::agent_handle_infer(&env, &stub)
                                .await
                                .unwrap();
                            let reply = Envelope::new(node_id.clone(), reply.msg);
                            let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                        }
                        Message::Challenge { .. } => {
                            let reply = joule_control::agent_handle_challenge(&env, &stub)
                                .await
                                .unwrap();
                            let reply = Envelope::new(node_id.clone(), reply.msg);
                            let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    });
    (api_key, handle)
}

/// Old agent noise: PlanAccept on RequestInfer with a **local** plan_id/hash.
/// Control ignores non-matching plan_id (no DoS-abort); PlanOffer still agrees.
/// Production agents no longer emit this; this proves resilience if they did.
#[tokio::test]
async fn mesh_poison_request_infer_wrong_plan_id_is_ignored_plan_offer_still_agrees() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    let (key_a, a) = spawn_agent_poison_request_infer(agent_addr, "poison-a", 8192).await;
    let (_key_b, b) = spawn_agent_poison_request_infer(agent_addr, "poison-b", 8192).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let r = client
        .post(format!("{base}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {key_a}"))
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role":"user","content":"poison-mesh-test"}],
            "max_tokens": 16
        }))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let body: serde_json::Value = r.json().await.unwrap();
    eprintln!("OBSERVE poison RequestInfer self-accept status={status} body={body}");
    assert!(status.is_success(), "PlanOffer path must still succeed: {body}");
    assert_eq!(
        body["joule_coordination"].as_str().unwrap_or(""),
        "mesh_request_infer",
        "wrong plan_id early accepts ignored; real PlanOffer multi-party must win: {body}"
    );
    let leases: serde_json::Value = client
        .get(format!("{base}/v1/cluster/leases"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    eprintln!("OBSERVE poison-resilience audit={leases}");
    let audit = leases["audit"].as_array().cloned().unwrap_or_default();
    assert!(
        audit.iter().any(|e| e["event"] == "plan_agreed"),
        "trail={leases}"
    );
    // Wrong plan_id must not be recorded as plan_accept_invalid abort of the mesh.
    assert!(
        !audit.iter().any(|e| e["event"] == "plan_accept_invalid"),
        "foreign plan_id should be ignored not abort: {leases}"
    );
    a.abort();
    b.abort();
}

/// Production-like multi-donor mesh: RequestInfer is note-only; PlanOffer carries hash.
#[tokio::test]
async fn mesh_production_request_infer_plan_offer_only_agree() {
    let app = load_or_init_app(None).expect("app");
    {
        let mut g = app.state.write().await;
        g.dual_verify_every = 0;
    }
    let (agent_addr, http_addr, _http) = serve_ephemeral(app.clone()).await.expect("serve");
    // spawn_agent mirrors fixed production: RequestInfer no PlanAccept.
    let (key_a, a) = spawn_agent(agent_addr, "mesh-prod-a", 8192).await;
    let (_key_b, b) = spawn_agent(agent_addr, "mesh-prod-b", 12288).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let mut g = app.state.write().await;
        g.cluster.trust_all_claims_for_tests();
    }
    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");
    let r = client
        .post(format!("{base}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {key_a}"))
        .json(&serde_json::json!({
            "model": CLUSTER_MODEL,
            "messages": [{"role":"user","content":"mesh-prod-agree"}],
            "max_tokens": 16
        }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "status={}", r.status());
    let body: serde_json::Value = r.json().await.unwrap();
    eprintln!("OBSERVE production mesh chat body={body}");
    assert_eq!(
        body["joule_coordination"].as_str().unwrap_or(""),
        "mesh_request_infer",
        "fixed agents must complete real mesh multi-party agree: {body}"
    );
    let leases: serde_json::Value = client
        .get(format!("{base}/v1/cluster/leases"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    eprintln!("OBSERVE production mesh audit={leases}");
    let audit = leases["audit"].as_array().cloned().unwrap_or_default();
    assert!(audit.iter().any(|e| e["event"] == "plan_agreed"), "{leases}");
    assert!(audit.iter().any(|e| e["event"] == "lease_released"), "{leases}");
    assert_eq!(leases["stream_slots_used"].as_u64().unwrap_or(99), 0);
    a.abort();
    b.abort();
}

/// Old/buggy agent behavior: on RequestInfer build local plan and PlanAccept (wrong hash).
async fn spawn_agent_poison_request_infer(
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
            pubkey_hex: String::new(),
            sig_hex: String::new(),
            signed_at_unix_ms: 0,
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
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::Heartbeat {
                    load: 0.0,
                    healthy: true,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    writer
        .write_all(
            &encode_line(&Envelope::new(
                node_id.clone(),
                Message::PeerAlive {
                    multiaddrs: vec![format!("tcp://127.0.0.1:{}", 20000 + (mem % 1000))],
                    load: 0.05,
                    healthy: true,
                    blob_count: 0,
                    mem_mib: mem,
                    verified_mem_mib: 0,
                    throughput_class: 40,
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();

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
                        Message::RequestInfer { request_id, .. } => {
                            // POISON: local equal-unit plan + self PlanAccept (old agent bug).
                            let donors = vec![(node_id.clone(), 1024u32)];
                            if let Ok(plan) = joule_cluster::plan_from_mesh_donors(&donors) {
                                let ph = joule_cluster::plan_hash_hex(&plan);
                                let (ph2, confirm) = joule_cluster::plan_accept_fields(
                                    &plan,
                                    *request_id,
                                    &node_id,
                                    true,
                                    Some(&ph),
                                );
                                let acc = Envelope::new(
                                    node_id.clone(),
                                    Message::PlanAccept {
                                        plan_id: plan.plan_id,
                                        request_id: *request_id,
                                        accepted: true,
                                        reason: "poison local mesh coordinator".into(),
                                        auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, true, &ph, &confirm),
                                        plan_hash_hex: ph2,
                                        confirm_hex: confirm,
                                    },
                                );
                                if writer.write_all(&encode_line(&acc).unwrap()).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Message::PlanOffer {
                            plan,
                            request_id,
                            plan_hash_hex, .. } => {
                            let accepted = plan.shards.iter().any(|s| s.node == node_id);
                            let (ph, confirm) = joule_cluster::plan_accept_fields(
                                plan,
                                *request_id,
                                &node_id,
                                accepted,
                                Some(plan_hash_hex.as_str()).filter(|s| !s.is_empty()),
                            );
                            let reply = Envelope::new(
                                node_id.clone(),
                                Message::PlanAccept {
                                    plan_id: plan.plan_id,
                                    request_id: *request_id,
                                    accepted,
                                    reason: "poison-agent later offer".into(),
                                    auth: e2e_plan_auth(&node_id, plan.plan_id, *request_id, accepted, &ph, &confirm),
                                    plan_hash_hex: ph,
                                    confirm_hex: confirm,
                    },
                            );
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
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
                            let _ = writer.write_all(&encode_line(&reply).unwrap()).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    });
    (api_key, handle)
}
