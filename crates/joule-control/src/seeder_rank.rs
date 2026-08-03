//! Seeder selection under load: locality, health, **seeder-side** transfer backpressure.
//!
//! Transfer accounting is a typed book ([`BlobXferBook`]): only seeder load from
//! `active_for_seeder` feeds candidates. BlobWant must not invent `active_transfers: 0`.

use joule_proto::NodeId;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// One control-relayed (or tracked) blob transfer in flight.
#[derive(Debug, Clone)]
pub struct BlobXferRecord {
    pub seeder: NodeId,
    pub requester: NodeId,
    pub hash: String,
    pub started: Instant,
}

/// Source of truth for concurrent blob transfers (seeder load attribution).
#[derive(Debug, Default, Clone)]
pub struct BlobXferBook {
    active: HashMap<Uuid, BlobXferRecord>,
}

impl BlobXferBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.active.len()
    }

    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Begin a transfer attributed to **seeder** (not requester).
    pub fn begin(&mut self, seeder: NodeId, requester: NodeId, hash: impl Into<String>) -> Uuid {
        let id = Uuid::new_v4();
        self.active.insert(
            id,
            BlobXferRecord {
                seeder,
                requester,
                hash: hash.into().to_lowercase(),
                started: Instant::now(),
            },
        );
        id
    }

    pub fn end(&mut self, id: &Uuid) -> Option<BlobXferRecord> {
        self.active.remove(id)
    }

    /// Concurrent transfers **serving** this seeder (criterion-4 backpressure input).
    pub fn active_for_seeder(&self, seeder: &NodeId) -> u32 {
        self.active.values().filter(|r| &r.seeder == seeder).count() as u32
    }

    /// Concurrent transfers requested by this node (not used for seeder refuse).
    pub fn active_for_requester(&self, requester: &NodeId) -> u32 {
        self.active
            .values()
            .filter(|r| &r.requester == requester)
            .count() as u32
    }

    pub fn requester_and_hash(&self, id: &Uuid) -> Option<(NodeId, String)> {
        self.active
            .get(id)
            .map(|r| (r.requester.clone(), r.hash.clone()))
    }

    pub fn retain_fresh(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.active
            .retain(|_, r| now.duration_since(r.started) < max_age);
    }
}

/// Presence inputs for ranking (from mesh / blob meta). Load may be gossip; transfer
/// count always comes from the book via [`seeder_candidates_from`].
#[derive(Debug, Clone)]
pub struct SeederPresence {
    pub node: NodeId,
    pub multiaddrs: Vec<String>,
    pub healthy: bool,
    /// Gossip/heartbeat load (0..1). Secondary to transfer-count backpressure.
    pub load: f32,
}

/// Inputs for ranking a candidate seeder (pure — no I/O).
#[derive(Debug, Clone)]
pub struct SeederCandidate {
    pub node: NodeId,
    pub multiaddrs: Vec<String>,
    pub healthy: bool,
    /// 0.0 idle … 1.0 saturated.
    pub load: f32,
    /// Concurrent blob transfers already **seeded by** this node ([`BlobXferBook`]).
    pub active_transfers: u32,
    /// Free stream slots on the pool (global backpressure signal).
    pub pool_stream_slots_free: u32,
}

/// **Only** builder for candidates: projects book seeder load into `active_transfers`.
pub fn seeder_candidates_from(
    presence: &[SeederPresence],
    book: &BlobXferBook,
    pool_stream_slots_free: u32,
) -> Vec<SeederCandidate> {
    presence
        .iter()
        .map(|p| SeederCandidate {
            node: p.node.clone(),
            multiaddrs: p.multiaddrs.clone(),
            healthy: p.healthy,
            load: p.load,
            active_transfers: book.active_for_seeder(&p.node),
            pool_stream_slots_free,
        })
        .collect()
}

/// Rank score: higher is better. Negative ⇒ refuse.
pub fn seeder_score(c: &SeederCandidate) -> i64 {
    if !c.healthy {
        return -1_000_000;
    }
    // Global compute empty does not mean free network — apply soft backpressure.
    let mut s: i64 = 1000;
    if c.pool_stream_slots_free == 0 {
        s -= 200;
    }
    // Prefer local / loopback multiaddrs.
    let local = c
        .multiaddrs
        .iter()
        .any(|a| a.contains("127.0.0.1") || a.contains("localhost") || a.contains("::1"));
    if local {
        s += 500;
    } else if !c.multiaddrs.is_empty() {
        s += 100;
    } else {
        s -= 50; // no dial path
    }
    // Load penalty
    let load_pen = (c.load.clamp(0.0, 1.0) * 400.0) as i64;
    s -= load_pen;
    // Active transfer penalty (backpressure) — seeder-side count from book.
    s -= i64::from(c.active_transfers) * 150;
    // Hard refuse if overloaded (transfer count alone enforces criterion 4).
    if c.load >= 0.95 || c.active_transfers >= 8 {
        return -1;
    }
    s
}

/// Pick best seeder; None if all refused.
pub fn pick_ranked_seeder(cands: &[SeederCandidate]) -> Option<&SeederCandidate> {
    cands
        .iter()
        .filter(|c| seeder_score(c) >= 0)
        .max_by_key(|c| seeder_score(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn presence(node: NodeId, addrs: Vec<&str>, healthy: bool, load: f32) -> SeederPresence {
        SeederPresence {
            node,
            multiaddrs: addrs.into_iter().map(String::from).collect(),
            healthy,
            load,
        }
    }

    #[test]
    fn book_attributes_load_to_seeder_not_requester() {
        let seeder_a = NodeId::new();
        let seeder_b = NodeId::new();
        let leech = NodeId::new();
        let mut book = BlobXferBook::new();
        for _ in 0..5 {
            book.begin(seeder_a.clone(), leech.clone(), "aa".repeat(32));
        }
        book.begin(seeder_b.clone(), leech.clone(), "bb".repeat(32));
        assert_eq!(book.active_for_seeder(&seeder_a), 5);
        assert_eq!(book.active_for_seeder(&seeder_b), 1);
        assert_eq!(book.active_for_requester(&leech), 6);
        assert_eq!(
            book.active_for_seeder(&leech),
            0,
            "requester is not a seeder load"
        );
    }

    #[test]
    fn candidates_from_book_refuse_busy_seeder_prefer_idle() {
        let busy = NodeId::new();
        let idle = NodeId::new();
        let leech = NodeId::new();
        let mut book = BlobXferBook::new();
        // Overload busy seeder (≥8 → hard refuse).
        for _ in 0..8 {
            book.begin(busy.clone(), leech.clone(), "cc".repeat(32));
        }
        let presence = vec![
            presence(busy.clone(), vec!["tcp://10.0.0.1:9"], true, 0.1),
            presence(idle.clone(), vec!["tcp://127.0.0.1:9"], true, 0.1),
        ];
        let cands = seeder_candidates_from(&presence, &book, 4);
        assert_eq!(
            cands
                .iter()
                .find(|c| c.node == busy)
                .unwrap()
                .active_transfers,
            8
        );
        assert_eq!(
            cands
                .iter()
                .find(|c| c.node == idle)
                .unwrap()
                .active_transfers,
            0
        );
        assert!(seeder_score(cands.iter().find(|c| c.node == busy).unwrap()) < 0);
        let best = pick_ranked_seeder(&cands).expect("idle must win");
        assert_eq!(best.node, idle);
        eprintln!(
            "OBSERVE p4 book-projection: busy_xfers=8 refused idle_picked score_idle={}",
            seeder_score(best)
        );
    }

    #[test]
    fn prefers_local_healthy_via_candidates_from() {
        let local = NodeId::new();
        let remote = NodeId::new();
        let sick = NodeId::new();
        let book = BlobXferBook::new();
        let presence = vec![
            presence(remote.clone(), vec!["tcp://8.8.8.8:9"], true, 0.8),
            presence(sick.clone(), vec!["tcp://127.0.0.1:9"], false, 0.0),
            presence(local.clone(), vec!["tcp://127.0.0.1:9"], true, 0.1),
        ];
        let cands = seeder_candidates_from(&presence, &book, 4);
        let best = pick_ranked_seeder(&cands).expect("pick");
        assert_eq!(best.node, local);
    }
}
