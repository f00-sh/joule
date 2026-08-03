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

    /// Merge one (or more) hashes without wiping the rest of the node's inventory.
    pub fn announce_add(&mut self, node: NodeId, blobs: Vec<BlobMeta>) {
        let set = self.by_node.entry(node.clone()).or_default();
        for b in blobs {
            let h = b.sha256.to_lowercase();
            if h.len() != 64 {
                continue;
            }
            set.insert(h.clone());
            self.by_hash.entry(h).or_default().insert(node.clone(), b);
        }
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

    pub fn seeder_count(&self, sha256: &str) -> u32 {
        self.by_hash
            .get(&sha256.to_lowercase())
            .map(|m| m.len() as u32)
            .unwrap_or(0)
    }

    /// Digests with fewer than `target` seeders (for rebalance).
    pub fn under_replicated(&self, target: u32) -> Vec<(String, u32)> {
        let mut out = Vec::new();
        for (h, peers) in &self.by_hash {
            let n = peers.len() as u32;
            if n < target {
                out.push((h.clone(), n));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Pick a seeder for `sha256` that is not `exclude`.
    pub fn pick_seeder(&self, sha256: &str, exclude: &NodeId) -> Option<(NodeId, BlobMeta)> {
        let h = sha256.to_lowercase();
        self.by_hash.get(&h).and_then(|m| {
            m.iter()
                .find(|(n, _)| *n != exclude)
                .map(|(n, b)| (n.clone(), b.clone()))
        })
    }

    /// Rank-aware pick from fully built candidates (must come from
    /// [`crate::seeder_rank::seeder_candidates_from`] + [`BlobXferBook`] — never invent zeros).
    pub fn pick_seeder_ranked(
        &self,
        sha256: &str,
        exclude: &NodeId,
        candidates: &[crate::seeder_rank::SeederCandidate],
    ) -> Option<(NodeId, BlobMeta)> {
        let peers = self.peers_for(sha256);
        if peers.is_empty() {
            return None;
        }
        // Only candidates that actually announce this hash and are not the requester.
        let eligible: Vec<_> = candidates
            .iter()
            .filter(|c| c.node != *exclude && peers.iter().any(|(n, _)| n == &c.node))
            .cloned()
            .collect();
        let best = crate::seeder_rank::pick_ranked_seeder(&eligible)?;
        peers.into_iter().find(|(n, _)| n == &best.node)
    }

    /// Nodes that do not currently announce this hash (candidates to pull a replica).
    pub fn non_seeders(&self, sha256: &str, all_nodes: &[NodeId]) -> Vec<NodeId> {
        let h = sha256.to_lowercase();
        let have: HashSet<_> = self
            .by_hash
            .get(&h)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        all_nodes
            .iter()
            .filter(|n| !have.contains(n))
            .cloned()
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use joule_proto::BlobMeta;

    fn meta(h: &str) -> BlobMeta {
        BlobMeta {
            sha256: h.into(),
            size: 32,
            kind: "blob".into(),
            name: "t".into(),
            multiaddrs: vec![],
        }
    }

    #[test]
    fn announce_replace_and_add() {
        let a = NodeId::new();
        let b = NodeId::new();
        let h1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let h2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut d = BlobDirectory::new();
        d.announce(a.clone(), vec![meta(h1)]);
        assert_eq!(d.seeder_count(h1), 1);
        d.announce_add(a.clone(), vec![meta(h2)]);
        assert_eq!(d.seeder_count(h1), 1);
        assert_eq!(d.seeder_count(h2), 1);
        d.announce(b.clone(), vec![meta(h1)]);
        assert_eq!(d.seeder_count(h1), 2);
        let seeder = d.pick_seeder(h1, &a).unwrap();
        assert_eq!(seeder.0, b);
        let non = d.non_seeders(h2, &[a.clone(), b.clone()]);
        assert_eq!(non, vec![b]);
        d.remove_node(&a);
        assert_eq!(d.seeder_count(h1), 1);
        assert_eq!(d.seeder_count(h2), 0);
    }

    /// BlobWant path projection: book.begin(seeder, …) → candidates_from → pick_seeder_ranked.
    /// Overload seeder A (≥8 seeder-side xfers); pick must refuse A and choose B.
    #[test]
    fn blobwant_path_refuses_busy_seeder_via_xfer_book() {
        use crate::seeder_rank::{
            seeder_candidates_from, seeder_score, BlobXferBook, SeederPresence,
        };

        let seeder_a = NodeId::new();
        let seeder_b = NodeId::new();
        let leech = NodeId::new();
        let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

        let mut dir = BlobDirectory::new();
        let mut meta_a = meta(hash);
        meta_a.multiaddrs = vec!["tcp://10.0.0.1:9".into()];
        let mut meta_b = meta(hash);
        meta_b.multiaddrs = vec!["tcp://127.0.0.1:9".into()];
        dir.announce(seeder_a.clone(), vec![meta_a]);
        dir.announce_add(seeder_b.clone(), vec![meta_b]);

        let mut book = BlobXferBook::new();
        // Simulate prior BlobProvide begin() calls on A (seeder-attributed).
        for _ in 0..8 {
            book.begin(seeder_a.clone(), leech.clone(), hash);
        }
        assert_eq!(book.active_for_seeder(&seeder_a), 8);
        assert_eq!(book.active_for_seeder(&seeder_b), 0);

        // Same construction as tcp BlobWant (presence + seeder_candidates_from).
        let presence = vec![
            SeederPresence {
                node: seeder_a.clone(),
                multiaddrs: vec!["tcp://10.0.0.1:9".into()],
                healthy: true,
                load: 0.1, // agent-constant load must NOT save overloaded seeder
            },
            SeederPresence {
                node: seeder_b.clone(),
                multiaddrs: vec!["tcp://127.0.0.1:9".into()],
                healthy: true,
                load: 0.1,
            },
        ];
        let cands = seeder_candidates_from(&presence, &book, 4);
        let score_a = seeder_score(cands.iter().find(|c| c.node == seeder_a).unwrap());
        assert!(score_a < 0, "busy seeder score={score_a}");
        let picked = dir
            .pick_seeder_ranked(hash, &leech, &cands)
            .expect("idle seeder B must be pickable");
        assert_eq!(
            picked.0, seeder_b,
            "BlobWant path must deprioritize/refuse busy seeder A"
        );
        // begin on B like the real handler
        let rid = book.begin(picked.0.clone(), leech.clone(), hash);
        assert_eq!(book.active_for_seeder(&seeder_b), 1);
        book.end(&rid);
        assert_eq!(book.active_for_seeder(&seeder_b), 0);
        eprintln!(
            "OBSERVE BlobWant-path: seeder_a xfers=8 refused score_a={score_a}; picked seeder_b"
        );
    }
}
