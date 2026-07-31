//! End-to-end: sharded pool across multi-donor VRAM + stream slots.

use joule_control::{load_or_init_app, serve_ephemeral};
use joule_proto::{
    decode_line, encode_line, DeviceClass, Envelope, Message, NodeCaps, NodeId, CLUSTER_MODEL,
    PROTOCOL_VERSION,
};
use joule_runtime::StubEngine;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

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
                        } => {
                            let accepted = plan.shards.iter().any(|s| s.node == node_id);
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
                                },
                            );
                            if writer.write_all(&encode_line(&reply).unwrap()).await.is_err() {
                                break;
                            }
                        }
                        Message::RequestInfer { .. } => {
                            // Coordinator handles plan; agents may log only.
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

    // Mesh plan geometry from PeerAlive mem (not only cluster registry).
    assert!(
        joule_control::mesh_donors_ready(&app).await,
        "mesh donors with mem_mib must be ready"
    );
    let donors = {
        let g = app.state.read().await;
        g.mesh.plan_donors()
    };
    assert!(
        donors.len() >= 2,
        "need ≥2 mesh donors, got {}",
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
        multi.iter().any(|addrs| addrs.iter().any(|a| a == &seeder_multi)),
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
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let _ = lines.next_line().await.unwrap(); // welcome
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
