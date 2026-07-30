//! Agent peer listen + direct blob transfer (decentral Phase A/B).
//!
//! Dial strings use `tcp://host:port`. Same NDJSON envelopes as the control agent port.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use joule_proto::{decode_line, encode_line, Envelope, Message, NodeId};
use joule_runtime::WeightsStore;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

const CHUNK: usize = 64 * 1024;

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

/// Serve peer NDJSON: BlobWant → BlobChunk from local blob store.
pub async fn run_peer_listener(listener: TcpListener, node_id: NodeId) -> Result<()> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let node_id = node_id.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer_session(sock, node_id).await {
                warn!(%peer, error = %e, "peer session ended");
            }
        });
    }
}

async fn handle_peer_session(sock: TcpStream, node_id: NodeId) -> Result<()> {
    let (reader, mut writer) = sock.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let env: Envelope = decode_line(line.as_bytes())?;
        match env.msg {
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
            Message::PeerAlive { .. } => {
                // Accept as liveness ping; no reply required.
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
    let want = Envelope::new(me.clone(), Message::BlobWant { sha256: hash.clone() });
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
                let piece = B64
                    .decode(data_b64.as_bytes())
                    .context("chunk base64")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
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
        tokio::spawn(async move {
            let _ = run_peer_listener(listener, seeder).await;
        });
        // accept ready
        tokio::time::sleep(Duration::from_millis(30)).await;

        let got = fetch_blob_direct(&multi, &hash).await.expect("direct fetch");
        assert_eq!(got, payload);

        // multiaddr list helper
        let got2 = fetch_blob_from_addrs(
            &[
                "tcp://127.0.0.1:1".into(), // dead first
                multi,
            ],
            &hash,
        )
        .await
        .expect("fallback multiaddr");
        assert_eq!(got2, payload);

        std::env::remove_var("JOULE_BLOBS_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
