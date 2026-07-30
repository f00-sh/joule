//! Swarm blob directory: who has which sha256.
//!
//! Control stores **locations only**, never multi-GB payloads.
//! See docs/design/distribution-v0.md.

use joule_proto::{BlobMeta, NodeId};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct BlobDirectory {
    /// sha256 → (node → meta)
    by_hash: HashMap<String, HashMap<NodeId, BlobMeta>>,
    /// node → set of hashes (for disconnect cleanup)
    by_node: HashMap<NodeId, HashSet<String>>,
}

impl BlobDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn announce(&mut self, node: NodeId, blobs: Vec<BlobMeta>) {
        // Drop previous inventory for this node, then re-add.
        if let Some(old) = self.by_node.remove(&node) {
            for h in old {
                if let Some(m) = self.by_hash.get_mut(&h) {
                    m.remove(&node);
                    if m.is_empty() {
                        self.by_hash.remove(&h);
                    }
                }
            }
        }
        let mut set = HashSet::new();
        for b in blobs {
            let h = b.sha256.to_lowercase();
            if h.len() != 64 {
                continue;
            }
            set.insert(h.clone());
            self.by_hash.entry(h).or_default().insert(node.clone(), b);
        }
        self.by_node.insert(node, set);
    }

    pub fn remove_node(&mut self, node: &NodeId) {
        if let Some(hashes) = self.by_node.remove(node) {
            for h in hashes {
                if let Some(m) = self.by_hash.get_mut(&h) {
                    m.remove(node);
                    if m.is_empty() {
                        self.by_hash.remove(&h);
                    }
                }
            }
        }
    }

    pub fn peers_for(&self, sha256: &str) -> Vec<(NodeId, BlobMeta)> {
        let h = sha256.to_lowercase();
        self.by_hash
            .get(&h)
            .map(|m| m.iter().map(|(n, b)| (n.clone(), b.clone())).collect())
            .unwrap_or_default()
    }

    pub fn catalog(&self) -> Vec<CatalogEntry> {
        let mut out = Vec::new();
        for (hash, peers) in &self.by_hash {
            let first = peers.values().next();
            out.push(CatalogEntry {
                sha256: hash.clone(),
                size: first.map(|b| b.size).unwrap_or(0),
                kind: first.map(|b| b.kind.clone()).unwrap_or_default(),
                name: first.map(|b| b.name.clone()).unwrap_or_default(),
                seeders: peers.len() as u32,
            });
        }
        out.sort_by(|a, b| a.sha256.cmp(&b.sha256));
        out
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogEntry {
    pub sha256: String,
    pub size: u64,
    pub kind: String,
    pub name: String,
    pub seeders: u32,
}
