//! Agent peer listen + direct blob transfer + peer-direct Infer* (decentral Phase A/B/C).
//!
//! Dial strings use `tcp://host:port`. Same NDJSON envelopes as the control agent port.
//! Peer sessions accept BlobWant (seed), PeerAlive + BlobsHave (local DHT), and
//! InferRequest → InferDone (no control relay required for the infer hop).

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use joule_dht::{DhtStore, PeerRecord};
use joule_proto::{decode_line, encode_line, BlobMeta, Envelope, Message, NodeId};
use joule_runtime::{ClusterEngine, WeightsStore};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{info, warn};

const CHUNK: usize = 64 * 1024;

/// Capacity + dial info for one mesh neighbor (Phase C/D).
#[derive(Debug, Clone, Default)]
pub struct MeshNeighbor {
    pub multiaddrs: Vec<String>,
    /// Self-reported claim (UI only).
    pub mem_mib: u32,
    /// Protocol-verified capacity for placement (claim ignored).
    pub verified_mem_mib: u32,
    pub throughput_class: u16,
    pub healthy: bool,
    pub load: f32,
    pub blob_count: u32,
}

impl MeshNeighbor {
    /// Compact status line for logs / debug.
    pub fn summary(&self) -> String {
        format!(
            "claim={} verified={} load={:.2} thr={} blobs={} healthy={}",
            self.mem_mib,
            self.verified_mem_mib,
            self.load,
            self.throughput_class,
            self.blob_count,
            self.healthy
        )
    }
}

/// Local mesh + DHT view (Phase C) — filled by control gossip and direct peer messages.
#[derive(Debug, Default)]
pub struct LocalMesh {
    pub dht: DhtStore,
    /// node_id → neighbor presence
    pub neighbors: HashMap<NodeId, MeshNeighbor>,
}

impl LocalMesh {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn apply_peer_alive(
        &mut self,
        from: &NodeId,
        multiaddrs: Vec<String>,
        load: f32,
        healthy: bool,
        blob_count: u32,
        mem_mib: u32,
        verified_mem_mib: u32,
        throughput_class: u16,
    ) {
        self.neighbors.insert(
            from.clone(),
            MeshNeighbor {
                multiaddrs: multiaddrs.clone(),
                mem_mib,
                verified_mem_mib,
                throughput_class,
                healthy,
                load,
                blob_count,
            },
        );
        let seq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.dht.put_peer(PeerRecord {
            node_id: from.to_string(),
            multiaddrs,
            load_milli: (load * 1000.0) as u32,
            healthy,
            blob_count,
            seq,
            updated_unix_ms: seq,
        });
    }

    pub fn apply_blobs_have(&mut self, from: &NodeId, blobs: &[BlobMeta]) {
        let id = from.to_string();
        for b in blobs {
            let mut addrs = b.multiaddrs.clone();
            if addrs.is_empty() {
                if let Some(n) = self.neighbors.get(from) {
                    addrs = n.multiaddrs.clone();
                }
            }
            self.dht.put_blob_seeder(&b.sha256, &id, b.size, addrs);
        }
    }

    /// All known multiaddrs that seed this hash (deduped).
    pub fn multiaddrs_for_blob(&self, sha256: &str) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(rec) = self.dht.get_blob(sha256) {
            for hint in rec.seeders.values() {
                for a in &hint.multiaddrs {
                    if !out.contains(a) {
                        out.push(a.clone());
                    }
                }
            }
        }
        out
    }

    pub fn peer_count(&self) -> u32 {
        self.neighbors.len() as u32
    }

    /// Equal unit weight for peer-gossip PlanOffer (Phase D).
    ///
    /// **CRITICAL:** PeerAlive `mem_mib` and `verified_mem_mib` are **self-reports**.
    /// A cheater can set verified=65536 on gossip; that must **never** buy placement.
    /// Control-coordinated placement uses `ControlState::mesh_plan_donors()` (cluster
    /// challenge-backed verified only). Peer-path geometry is equal-unit among healthy
    /// dialable peers so gossip cannot mint a GPU farm.
    pub const PEER_GOSSIP_UNIT_MIB: u32 = 1024;

    /// Healthy dialable peers for mesh PlanOffer — **equal unit only**.
    /// Ignores claim and self-reported verified from PeerAlive entirely.
    pub fn plan_donors(&self) -> Vec<(NodeId, u32)> {
        let mut v: Vec<_> = self
            .neighbors
            .iter()
            .filter(|(_, n)| n.healthy && !n.multiaddrs.is_empty())
            .map(|(id, _)| (id.clone(), Self::PEER_GOSSIP_UNIT_MIB))
            .collect();
        // Stable order by node id (weights are equal — no mem aristocracy from gossip).
        v.sort_by_key(|a| a.0.to_string());
        v
    }
}

pub type SharedMesh = Arc<Mutex<LocalMesh>>;

/// Parse `tcp://host:port` (or bare `host:port`) into a socket address.
pub fn parse_tcp_multiaddr(s: &str) -> Result<SocketAddr> {
    let t = s
        .trim()
        .strip_prefix("tcp://")
        .or_else(|| s.trim().strip_prefix("TCP://"))
        .unwrap_or(s.trim());
    t.parse::<SocketAddr>()
        .with_context(|| format!("parse multiaddr {s}"))
}

/// Advertise form for a bound peer listen address.
pub fn format_tcp_multiaddr(addr: SocketAddr) -> String {
    format!("tcp://{addr}")
}

/// Serve peer NDJSON: BlobWant → BlobChunk; PeerAlive/BlobsHave; InferRequest → InferDone.
pub async fn run_peer_listener(
    listener: TcpListener,
    node_id: NodeId,
    mesh: SharedMesh,
    engine: Arc<ClusterEngine>,
) -> Result<()> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let node_id = node_id.clone();
        let mesh = mesh.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer_session(sock, node_id, mesh, engine).await {
                warn!(%peer, error = %e, "peer session ended");
            }
        });
    }
}

async fn handle_peer_session(
    sock: TcpStream,
    node_id: NodeId,
    mesh: SharedMesh,
    engine: Arc<ClusterEngine>,
) -> Result<()> {
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let env: Envelope = decode_line(line.as_bytes())?;
        match env.msg {
            Message::InferRequest { .. } => {
                // Peer-direct infer hop: run shard Engine and reply InferDone on this socket.
                let reply = joule_control::agent_handle_infer(&env, engine.as_ref())
                    .await
                    .context("peer InferRequest")?;
                let reply = Envelope::new(node_id.clone(), reply.msg);
                writer.write_all(&encode_line(&reply)?).await?;
                info!(from = %env.from, "peer InferRequest → InferDone");
            }
            Message::BlobWant { sha256 } => {
                let hash = sha256.to_lowercase();
                match WeightsStore::read_blob(&hash) {
                    Ok(data) => {
                        let request_id = uuid::Uuid::new_v4();
                        let mut offset = 0u64;
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
                        } else {
                            while (offset as usize) < data.len() {
                                let end = ((offset as usize) + CHUNK).min(data.len());
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
                        }
                        info!(%hash, bytes = data.len(), "peer BlobWant served");
                    }
                    Err(e) => {
                        let err = Envelope::new(
                            node_id.clone(),
                            Message::Error {
                                error: format!("blob missing: {e}"),
                            },
                        );
                        writer.write_all(&encode_line(&err)?).await?;
                    }
                }
            }
            Message::PeerAlive {
                multiaddrs,
                load,
                healthy,
                blob_count,
                mem_mib,
                verified_mem_mib,
                throughput_class,
            } => {
                let mut g = mesh.lock().await;
                g.apply_peer_alive(
                    &env.from,
                    multiaddrs,
                    load,
                    healthy,
                    blob_count,
                    mem_mib,
                    verified_mem_mib,
                    throughput_class,
                );
                let detail = g
                    .neighbors
                    .get(&env.from)
                    .map(|n| n.summary())
                    .unwrap_or_default();
                info!(from = %env.from, peers = g.peer_count(), %detail, "peer PeerAlive → local DHT");
            }
            Message::BlobsHave { blobs } => {
                let mut g = mesh.lock().await;
                g.apply_blobs_have(&env.from, &blobs);
                info!(from = %env.from, n = blobs.len(), "peer BlobsHave → local DHT");
            }
            other => {
                warn!(msg = ?other, "ignored peer message");
            }
        }
    }
    Ok(())
}

/// Fetch blob bytes from a peer multiaddr via BlobWant / BlobChunk.
pub async fn fetch_blob_direct(multiaddr: &str, sha256: &str) -> Result<Vec<u8>> {
    let addr = parse_tcp_multiaddr(multiaddr)?;
    let hash = sha256.to_lowercase();
    let sock = tokio::time::timeout(Duration::from_secs(8), TcpStream::connect(addr))
        .await
        .context("connect timeout")?
        .with_context(|| format!("connect {addr}"))?;
    let (reader, mut writer) = sock.into_split();
    let me = NodeId::new();
    let want = Envelope::new(
        me.clone(),
        Message::BlobWant {
            sha256: hash.clone(),
        },
    );
    writer.write_all(&encode_line(&want)?).await?;

    let mut lines = BufReader::new(reader).lines();
    let mut buf = Vec::new();
    let mut next = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if tokio::time::Instant::now() > deadline {
            bail!("direct blob fetch timed out");
        }
        let line = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
            .await
            .context("read timeout")?
            .context("peer closed")?
            .ok_or_else(|| anyhow::anyhow!("peer closed"))?;
        if line.trim().is_empty() {
            continue;
        }
        let env: Envelope = decode_line(line.as_bytes())?;
        match env.msg {
            Message::BlobChunk {
                sha256: h,
                offset,
                data_b64,
                done,
                ..
            } => {
                if h.to_lowercase() != hash {
                    bail!("chunk hash mismatch");
                }
                if offset != next {
                    bail!("out of order chunk");
                }
                let piece = B64.decode(data_b64.as_bytes()).context("chunk base64")?;
                buf.extend_from_slice(&piece);
                next += piece.len() as u64;
                if done {
                    return Ok(buf);
                }
            }
            Message::Error { error } => bail!("peer error: {error}"),
            _ => {}
        }
    }
}

/// Try multiaddrs in order; first success wins.
pub async fn fetch_blob_from_addrs(addrs: &[String], sha256: &str) -> Result<Vec<u8>> {
    let mut last = anyhow::anyhow!("no multiaddrs");
    for a in addrs {
        match fetch_blob_direct(a, sha256).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                warn!(%a, error = %e, "direct blob fetch failed");
                last = e;
            }
        }
    }
    Err(last)
}

/// Dial bootstrap / known peer multiaddrs and announce PeerAlive (+ optional BlobsHave).
/// Best-effort; failures are logged, not fatal.
pub async fn announce_to_peers(
    targets: &[String],
    node_id: &NodeId,
    our_multiaddrs: &[String],
    blob_count: u32,
    blobs: Option<&[BlobMeta]>,
) {
    for a in targets {
        // Skip self.
        if our_multiaddrs.iter().any(|m| m == a) {
            continue;
        }
        if let Err(e) = announce_one(a, node_id, our_multiaddrs, blob_count, blobs).await {
            warn!(%a, error = %e, "bootstrap/mesh announce failed");
        }
    }
}

async fn announce_one(
    multiaddr: &str,
    node_id: &NodeId,
    our_multiaddrs: &[String],
    blob_count: u32,
    blobs: Option<&[BlobMeta]>,
) -> Result<()> {
    let addr = parse_tcp_multiaddr(multiaddr)?;
    let sock = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
        .await
        .context("connect timeout")?
        .with_context(|| format!("connect {addr}"))?;
    let (_reader, mut writer) = sock.into_split();
    let alive = Envelope::new(
        node_id.clone(),
        Message::PeerAlive {
            multiaddrs: our_multiaddrs.to_vec(),
            load: 0.1,
            healthy: true,
            blob_count,
            mem_mib: 0,
            verified_mem_mib: 0,
            throughput_class: 0,
        },
    );
    writer.write_all(&encode_line(&alive)?).await?;
    if let Some(blobs) = blobs {
        if !blobs.is_empty() {
            let have = Envelope::new(
                node_id.clone(),
                Message::BlobsHave {
                    blobs: blobs.to_vec(),
                },
            );
            writer.write_all(&encode_line(&have)?).await?;
        }
    }
    let _ = writer.shutdown().await;
    Ok(())
}

/// Try local DHT multiaddrs for a digest; returns true if stored successfully.
pub async fn try_fetch_from_local_mesh(mesh: &SharedMesh, sha256: &str) -> Result<bool> {
    let addrs = {
        let g = mesh.lock().await;
        g.multiaddrs_for_blob(sha256)
    };
    if addrs.is_empty() {
        return Ok(false);
    }
    match fetch_blob_from_addrs(&addrs, sha256).await {
        Ok(data) => {
            WeightsStore::store_blob(sha256, &data).map_err(|e| anyhow::anyhow!(e))?;
            info!(%sha256, "local mesh DHT blob OK");
            Ok(true)
        }
        Err(e) => {
            warn!(%sha256, error = %e, "local mesh fetch failed");
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::sync::{Mutex as StdMutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn parse_tcp_forms() {
        let a = parse_tcp_multiaddr("tcp://127.0.0.1:7702").unwrap();
        assert_eq!(a.port(), 7702);
        let b = parse_tcp_multiaddr("127.0.0.1:7703").unwrap();
        assert_eq!(b.port(), 7703);
        assert!(format_tcp_multiaddr(a).starts_with("tcp://"));
    }

    #[test]
    fn local_mesh_blob_multiaddrs() {
        let mut m = LocalMesh::new();
        let id = NodeId::new();
        m.apply_peer_alive(
            &id,
            vec!["tcp://127.0.0.1:9".into()],
            0.1,
            true,
            1,
            8192, // claim — ignored for placement
            0,
            40,
        );
        // Healthy + multiaddr → equal unit (not claim, not verified gossip).
        assert_eq!(m.plan_donors().len(), 1);
        assert_eq!(m.plan_donors()[0].1, LocalMesh::PEER_GOSSIP_UNIT_MIB);
        m.apply_peer_alive(
            &id,
            vec!["tcp://127.0.0.1:9".into()],
            0.1,
            true,
            1,
            8192,
            65_536, // self-reported "verified farm" — still equal unit only
            40,
        );
        assert_eq!(m.plan_donors().len(), 1);
        assert_eq!(
            m.plan_donors()[0].1,
            LocalMesh::PEER_GOSSIP_UNIT_MIB,
            "gossip verified must not inflate placement"
        );
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        m.apply_blobs_have(
            &id,
            &[BlobMeta {
                sha256: hash.into(),
                size: 1,
                kind: "blob".into(),
                name: "t".into(),
                multiaddrs: vec![],
            }],
        );
        let addrs = m.multiaddrs_for_blob(hash);
        assert_eq!(addrs, vec!["tcp://127.0.0.1:9".to_string()]);
        assert_eq!(m.peer_count(), 1);
        assert!(m.dht.len() >= 2);
        assert_eq!(m.plan_donors().len(), 1);
    }

    /// CRITICAL: PeerAlive self-reported verified without control unlock cannot
    /// create farm-weighted geometry on the peer-only path.
    #[test]
    fn gossip_self_reported_verified_cannot_mint_farm_placement() {
        use joule_cluster::plan_from_mesh_donors;
        let mut m = LocalMesh::new();
        let faker = NodeId::new();
        let honest = NodeId::new();
        m.apply_peer_alive(
            &faker,
            vec!["tcp://10.0.0.1:1".into()],
            0.0,
            true,
            0,
            65_536, // claim farm
            65_536, // self-attested verified farm (forged)
            99,
        );
        m.apply_peer_alive(
            &honest,
            vec!["tcp://10.0.0.2:2".into()],
            0.0,
            true,
            0,
            1024,
            0, // honest does not self-attest
            10,
        );
        let donors = m.plan_donors();
        assert_eq!(
            donors.len(),
            2,
            "both healthy dialable peers participate equally"
        );
        for (_, w) in &donors {
            assert_eq!(
                *w,
                LocalMesh::PEER_GOSSIP_UNIT_MIB,
                "no peer may carry claim/verified weight from gossip"
            );
        }
        let plan = plan_from_mesh_donors(&donors).expect("equal plan");
        assert_eq!(
            plan.pool_mem_mib,
            2 * u64::from(LocalMesh::PEER_GOSSIP_UNIT_MIB),
            "pool must not be 65536+… from gossip"
        );
        assert!(plan.pool_mem_mib < 65_536);
        // No multiaddr → not in plan_donors (not dialable).
        let ghost = NodeId::new();
        m.apply_peer_alive(&ghost, vec![], 0.0, true, 0, 99_999, 99_999, 1);
        assert_eq!(m.plan_donors().len(), 2);
    }

    /// Phase B: seeder peer listen → leech direct BlobWant (no control relay).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // test serializes JOULE_BLOBS_DIR for the whole transfer
    async fn direct_blob_want_chunk_roundtrip() {
        let _guard = env_lock();
        let dir = std::env::temp_dir().join(format!(
            "joule-peer-net-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("JOULE_BLOBS_DIR", &dir);

        let payload = b"direct peer blob phase-b lab payload";
        let hash = hex::encode(Sha256::digest(payload));
        WeightsStore::store_blob(&hash, payload).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let multi = format_tcp_multiaddr(addr);
        let seeder = NodeId::new();
        let mesh = Arc::new(Mutex::new(LocalMesh::new()));
        let mesh2 = mesh.clone();
        let eng = Arc::new(ClusterEngine::new());
        tokio::spawn(async move {
            let _ = run_peer_listener(listener, seeder, mesh2, eng).await;
        });
        tokio::time::sleep(Duration::from_millis(30)).await;

        let got = fetch_blob_direct(&multi, &hash)
            .await
            .expect("direct fetch");
        assert_eq!(got, payload);

        let got2 = fetch_blob_from_addrs(&["tcp://127.0.0.1:1".into(), multi.clone()], &hash)
            .await
            .expect("fallback multiaddr");
        assert_eq!(got2, payload);

        // Phase C: PeerAlive + BlobsHave over peer port fills local DHT.
        let leech = NodeId::new();
        announce_to_peers(
            std::slice::from_ref(&multi),
            &leech,
            &["tcp://127.0.0.1:19999".into()],
            0,
            Some(&[BlobMeta {
                sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
                size: 1,
                kind: "blob".into(),
                name: "c".into(),
                multiaddrs: vec!["tcp://127.0.0.1:19999".into()],
            }]),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let g = mesh.lock().await;
        assert!(g.peer_count() >= 1);
        assert!(!g
            .multiaddrs_for_blob("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
            .is_empty());

        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P3: dial peer multiaddr, send InferRequest, receive InferDone (no control relay).
    #[tokio::test]
    async fn peer_direct_infer_request_infer_done_roundtrip() {
        use joule_proto::{ClusterPlan, ShardAssignment, ShardRole, CLUSTER_MODEL};
        use uuid::Uuid;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let multi = format_tcp_multiaddr(addr);
        let seeder = NodeId::new();
        let mesh = Arc::new(Mutex::new(LocalMesh::new()));
        let eng = Arc::new(ClusterEngine::new());
        let seeder2 = seeder.clone();
        tokio::spawn(async move {
            let _ = run_peer_listener(listener, seeder2, mesh, eng).await;
        });
        tokio::time::sleep(Duration::from_millis(40)).await;

        let request_id = Uuid::new_v4();
        let plan = ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.into(),
            model_layers: 93,
            pool_mem_mib: 8192,
            shards: vec![ShardAssignment {
                node: seeder.clone(),
                layer_start: Some(0),
                layer_end: Some(79),
                mem_share_mib: 8192,
                mem_fraction_ppm: 1_000_000,
                role: ShardRole::Replica,
                tp_rank: None,
                tp_world: None,
            }],
        };
        let req = Envelope::new(
            seeder.clone(),
            Message::InferRequest {
                request_id,
                model: CLUSTER_MODEL.into(),
                prompt: "user: peer-direct-p3".into(),
                max_tokens: 16,
                plan,
                is_tail: true,
            },
        );
        let sock = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
            .await
            .expect("connect timeout")
            .expect("connect");
        let (reader, mut writer) = sock.into_split();
        writer
            .write_all(&encode_line(&req).unwrap())
            .await
            .expect("write InferRequest");
        let mut lines = BufReader::new(reader).lines();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("read timeout")
            .expect("read")
            .expect("line");
        let reply: Envelope = decode_line(line.as_bytes()).expect("decode InferDone");
        match reply.msg {
            Message::InferDone {
                request_id: rid,
                text,
                shard_ok,
                ..
            } => {
                assert_eq!(rid, request_id);
                assert!(shard_ok);
                assert!(
                    !text.is_empty() && text.contains("peer-direct-p3"),
                    "peer InferDone text={text}"
                );
                eprintln!("OBSERVE peer-direct InferRequest→InferDone multi={multi} text={text}");
            }
            other => panic!("expected InferDone, got {other:?}"),
        }
    }
}
