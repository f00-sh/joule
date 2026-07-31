//! Content-addressed DHT lite for joule decentral discovery (Phase C).
//!
//! Keys:
//! - `peer/<node_id>` → multiaddrs + caps summary + seq
//! - `blob/<sha256>` → seeder node ids + sizes + multiaddrs
//!
//! Local store + **k-bucket routing** + multi-hop iterative find/store (`routing` module).
//! Peers do **not** share one HashMap: each node has its own table and store;
//! `InProcessNetwork` proves multi-hop put/get for tests and lab sim. Bootstrap lists are **replaceable** — never a
//! single f00 hostname as the only root.
//!
//! See docs/design/decentral-discovery-v0.md.

mod routing;
pub use routing::{
    Contact, DhtNode, DhtRpc, FindValueResult, InProcessNetwork, KBucket, RoutingTable,
    ALPHA, K_BUCKET, MAX_FIND_ITERS,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Key namespace prefixes.
pub const KEY_PEER: &str = "peer/";
pub const KEY_BLOB: &str = "blob/";

/// Build a peer record key from a node id string.
pub fn peer_key(node_id: &str) -> String {
    format!("{KEY_PEER}{}", node_id.trim())
}

/// Build a blob record key from sha256 hex.
pub fn blob_key(sha256: &str) -> String {
    format!("{KEY_BLOB}{}", sha256.trim().to_lowercase())
}

/// XOR distance between two 256-bit digests (as 32-byte arrays). Lower is closer.
pub fn xor_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = a[i] ^ b[i];
    }
    out
}

/// Hash an arbitrary key string into a 32-byte DHT id.
pub fn key_id(key: &str) -> [u8; 32] {
    let d = Sha256::digest(key.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&d);
    out
}

/// Compare XOR distances: returns Ordering::Less if `a` is closer to `target` than `b`.
pub fn closer(target: &[u8; 32], a: &[u8; 32], b: &[u8; 32]) -> std::cmp::Ordering {
    xor_distance(target, a).cmp(&xor_distance(target, b))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Peer presence record stored under `peer/<id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    pub node_id: String,
    pub multiaddrs: Vec<String>,
    #[serde(default)]
    pub load_milli: u32,
    #[serde(default)]
    pub healthy: bool,
    #[serde(default)]
    pub blob_count: u32,
    /// Monotonic sequence for last-writer-wins.
    pub seq: u64,
    pub updated_unix_ms: u64,
}

/// Blob seeder record under `blob/<sha256>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRecord {
    pub sha256: String,
    /// node_id → (size, multiaddrs)
    pub seeders: HashMap<String, SeederHint>,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeederHint {
    pub size: u64,
    #[serde(default)]
    pub multiaddrs: Vec<String>,
}

/// Opaque DHT value (JSON string for wire simplicity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtValue {
    pub key: String,
    pub value_json: String,
    pub seq: u64,
    pub updated_unix_ms: u64,
}

/// In-process DHT store (Phase C). Thread-safe via external lock.
#[derive(Debug, Default, Clone)]
pub struct DhtStore {
    records: HashMap<String, DhtValue>,
}

impl DhtStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Put raw JSON under key; only if `seq` is >= existing.
    pub fn put_raw(&mut self, key: String, value_json: String, seq: u64) -> bool {
        if let Some(old) = self.records.get(&key) {
            if seq < old.seq {
                return false;
            }
        }
        self.records.insert(
            key.clone(),
            DhtValue {
                key,
                value_json,
                seq,
                updated_unix_ms: now_unix_ms(),
            },
        );
        true
    }

    pub fn get_raw(&self, key: &str) -> Option<&DhtValue> {
        self.records.get(key)
    }

    pub fn put_peer(&mut self, rec: PeerRecord) -> bool {
        let key = peer_key(&rec.node_id);
        let seq = rec.seq;
        let value_json = serde_json::to_string(&rec).unwrap_or_else(|_| "{}".into());
        self.put_raw(key, value_json, seq)
    }

    pub fn get_peer(&self, node_id: &str) -> Option<PeerRecord> {
        let key = peer_key(node_id);
        let v = self.get_raw(&key)?;
        serde_json::from_str(&v.value_json).ok()
    }

    pub fn put_blob_seeder(
        &mut self,
        sha256: &str,
        node_id: &str,
        size: u64,
        multiaddrs: Vec<String>,
    ) {
        let key = blob_key(sha256);
        let mut rec = self
            .get_raw(&key)
            .and_then(|v| serde_json::from_str::<BlobRecord>(&v.value_json).ok())
            .unwrap_or_else(|| BlobRecord {
                sha256: sha256.to_lowercase(),
                seeders: HashMap::new(),
                updated_unix_ms: now_unix_ms(),
            });
        rec.seeders.insert(
            node_id.to_string(),
            SeederHint {
                size,
                multiaddrs,
            },
        );
        rec.updated_unix_ms = now_unix_ms();
        let seq = rec.updated_unix_ms;
        let value_json = serde_json::to_string(&rec).unwrap_or_else(|_| "{}".into());
        self.put_raw(key, value_json, seq);
    }

    pub fn get_blob(&self, sha256: &str) -> Option<BlobRecord> {
        let key = blob_key(sha256);
        let v = self.get_raw(&key)?;
        serde_json::from_str(&v.value_json).ok()
    }

    /// Up to `k` keys whose ids are closest to `target` (for future routing).
    pub fn closest_keys(&self, target_key: &str, k: usize) -> Vec<String> {
        let tid = key_id(target_key);
        let mut keys: Vec<_> = self.records.keys().cloned().collect();
        keys.sort_by(|a, b| closer(&tid, &key_id(a), &key_id(b)));
        keys.truncate(k.max(1));
        keys
    }

    pub fn snapshot_keys(&self) -> Vec<String> {
        let mut v: Vec<_> = self.records.keys().cloned().collect();
        v.sort();
        v
    }
}

/// Bootstrap peer list — replaceable, community-seeded, **not** f00 payload origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BootstrapList {
    #[serde(default)]
    pub version: u32,
    /// Human note (optional).
    #[serde(default)]
    pub comment: String,
    /// Dial strings `tcp://host:port` (control agent, peer listen, or future QUIC).
    pub multiaddrs: Vec<String>,
    /// Optional DNS TXT / HTTPS mirror URLs for **signed** bootstrap refresh (not weights).
    #[serde(default)]
    pub list_urls: Vec<String>,
}

impl BootstrapList {
    pub fn empty() -> Self {
        Self {
            version: 1,
            comment: String::new(),
            multiaddrs: Vec::new(),
            list_urls: Vec::new(),
        }
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }

    pub fn load_path(path: &Path) -> Result<Self, String> {
        let s = fs::read_to_string(path).map_err(|e| e.to_string())?;
        Self::from_json(&s)
    }

    pub fn to_json_pretty(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self).map_err(|e| e.to_string())
    }

    /// Default search paths for bootstrap file (first that exists wins).
    pub fn default_search_paths() -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        if let Ok(p) = std::env::var("JOULE_BOOTSTRAP") {
            out.push(std::path::PathBuf::from(p));
        }
        if let Ok(home) = std::env::var("HOME") {
            out.push(
                std::path::PathBuf::from(home)
                    .join(".local/share/joule/bootstrap.json"),
            );
        }
        out.push(std::path::PathBuf::from("bootstrap.json"));
        out
    }

    pub fn load_default() -> Option<Self> {
        for p in Self::default_search_paths() {
            if p.is_file() {
                if let Ok(b) = Self::load_path(&p) {
                    return Some(b);
                }
            }
        }
        None
    }

    /// Merge multiaddrs from another list (dedupe, preserve order). Used for
    /// product multi-region bootstrap: local file + optional remote list_urls JSON.
    pub fn merge_multiaddrs(&mut self, other: &BootstrapList) {
        for a in &other.multiaddrs {
            let t = a.trim();
            if t.is_empty() {
                continue;
            }
            if !self.multiaddrs.iter().any(|x| x == t) {
                self.multiaddrs.push(t.to_string());
            }
        }
        for u in &other.list_urls {
            let t = u.trim();
            if t.is_empty() {
                continue;
            }
            if !self.list_urls.iter().any(|x| x == t) {
                self.list_urls.push(t.to_string());
            }
        }
    }

    /// Parse a remote bootstrap JSON body (same schema) — pure, no network.
    pub fn from_remote_json_body(body: &str) -> Result<Self, String> {
        Self::from_json(body)
    }

    /// True if this list is usable outside pure localhost lab (has a non-loopback
    /// multiaddr or at least one list_url for refresh).
    pub fn is_product_style(&self) -> bool {
        if !self.list_urls.is_empty() {
            return true;
        }
        self.multiaddrs.iter().any(|a| {
            let s = a.to_ascii_lowercase();
            !s.contains("127.0.0.1") && !s.contains("[::1]") && !s.contains("localhost")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_and_blob_roundtrip() {
        let mut d = DhtStore::new();
        assert!(d.put_peer(PeerRecord {
            node_id: "n1".into(),
            multiaddrs: vec!["tcp://1.2.3.4:7702".into()],
            load_milli: 100,
            healthy: true,
            blob_count: 2,
            seq: 1,
            updated_unix_ms: 1,
        }));
        // older seq ignored
        assert!(!d.put_peer(PeerRecord {
            node_id: "n1".into(),
            multiaddrs: vec!["tcp://9.9.9.9:1".into()],
            load_milli: 0,
            healthy: false,
            blob_count: 0,
            seq: 0,
            updated_unix_ms: 0,
        }));
        let p = d.get_peer("n1").unwrap();
        assert_eq!(p.multiaddrs[0], "tcp://1.2.3.4:7702");

        d.put_blob_seeder(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "n1",
            42,
            vec!["tcp://1.2.3.4:7702".into()],
        );
        let b = d
            .get_blob("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        assert_eq!(b.seeders["n1"].size, 42);
        assert!(!d.closest_keys("blob/aa", 3).is_empty());
    }

    #[test]
    fn xor_closer_ordering() {
        let t = key_id("target");
        let a = key_id("a");
        let b = key_id("b");
        let _ = closer(&t, &a, &b);
        assert_ne!(xor_distance(&a, &b), [0u8; 32]);
    }

    #[test]
    fn merge_and_product_style_bootstrap() {
        let mut local = BootstrapList::from_json(
            r#"{"version":1,"multiaddrs":["tcp://127.0.0.1:7701"],"list_urls":[]}"#,
        )
        .unwrap();
        assert!(!local.is_product_style());
        let remote = BootstrapList::from_remote_json_body(
            r#"{"version":1,"comment":"region-eu","multiaddrs":["tcp://203.0.113.10:7702","quic://198.51.100.8:7703"],"list_urls":["https://example.com/bootstrap.json"]}"#,
        )
        .unwrap();
        assert!(remote.is_product_style());
        local.merge_multiaddrs(&remote);
        assert!(local.is_product_style());
        assert!(local.multiaddrs.iter().any(|a| a.contains("203.0.113.10")));
        assert!(local.list_urls.iter().any(|u| u.contains("example.com")));
        // dedupe
        let n = local.multiaddrs.len();
        local.merge_multiaddrs(&remote);
        assert_eq!(local.multiaddrs.len(), n);
    }

    #[test]
    fn bootstrap_json() {
        let raw = r#"{
            "version": 1,
            "comment": "lab",
            "multiaddrs": ["tcp://127.0.0.1:7701", "tcp://127.0.0.1:7702"],
            "list_urls": []
        }"#;
        let b = BootstrapList::from_json(raw).unwrap();
        assert_eq!(b.multiaddrs.len(), 2);
        let pretty = b.to_json_pretty().unwrap();
        assert!(pretty.contains("7701"));
    }

    #[test]
    fn bootstrap_load_path() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bootstrap.json");
        fs::write(
            &p,
            r#"{"version":1,"multiaddrs":["tcp://10.0.0.1:7702"],"list_urls":[]}"#,
        )
        .unwrap();
        let b = BootstrapList::load_path(&p).unwrap();
        assert_eq!(b.multiaddrs[0], "tcp://10.0.0.1:7702");
    }
}
