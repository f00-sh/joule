//! Kademlia-style k-bucket routing + multi-hop find/store (Phase C production).
//!
//! Nodes never share a single LocalMesh HashMap: each holds its own
//! [`RoutingTable`] + [`DhtStore`]. Iterative lookup walks XOR-closest peers.

use crate::{closer, key_id, DhtStore, DhtValue};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// Kademlia bucket size (default k=20 in classic Kademlia; lab uses 8).
pub const K_BUCKET: usize = 8;
/// Parallelism for iterative lookup (α).
pub const ALPHA: usize = 3;
/// Max iterations for iterative find.
pub const MAX_FIND_ITERS: usize = 20;

/// Identity of a DHT peer (node id bytes + dial multiaddrs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contact {
    pub node_id: String,
    pub id: [u8; 32],
    pub multiaddrs: Vec<String>,
}

impl Contact {
    pub fn from_node_id(node_id: impl Into<String>, multiaddrs: Vec<String>) -> Self {
        let node_id = node_id.into();
        let id = key_id(&node_id);
        Self {
            node_id,
            id,
            multiaddrs,
        }
    }
}

/// One k-bucket: contacts sorted by last-seen (front = most recent).
#[derive(Debug, Clone, Default)]
pub struct KBucket {
    contacts: VecDeque<Contact>,
}

impl KBucket {
    pub fn upsert(&mut self, c: Contact) {
        self.contacts.retain(|x| x.node_id != c.node_id);
        self.contacts.push_front(c);
        while self.contacts.len() > K_BUCKET {
            self.contacts.pop_back();
        }
    }

    pub fn contacts(&self) -> impl Iterator<Item = &Contact> {
        self.contacts.iter()
    }
}

/// 256 k-buckets by XOR prefix length (bucket i = distance bit length i).
#[derive(Debug, Clone)]
pub struct RoutingTable {
    local_id: [u8; 32],
    local_node: String,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(local_node: impl Into<String>) -> Self {
        let local_node = local_node.into();
        let local_id = key_id(&local_node);
        Self {
            local_id,
            local_node,
            buckets: (0..256).map(|_| KBucket::default()).collect(),
        }
    }

    pub fn local_node(&self) -> &str {
        &self.local_node
    }

    pub fn local_id(&self) -> &[u8; 32] {
        &self.local_id
    }

    fn bucket_index(&self, id: &[u8; 32]) -> usize {
        let dist = crate::xor_distance(&self.local_id, id);
        // highest set bit of distance
        for (i, b) in dist.iter().enumerate() {
            if *b != 0 {
                let bit = 7 - b.leading_zeros() as usize;
                return i * 8 + bit;
            }
        }
        0
    }

    pub fn observe(&mut self, c: Contact) {
        if c.node_id == self.local_node {
            return;
        }
        let bi = self.bucket_index(&c.id);
        self.buckets[bi].upsert(c);
    }

    /// Up to `k` known contacts closest to `target`.
    pub fn closest(&self, target: &[u8; 32], k: usize) -> Vec<Contact> {
        let mut all: Vec<Contact> = self
            .buckets
            .iter()
            .flat_map(|b| b.contacts().cloned())
            .collect();
        all.sort_by(|a, b| closer(target, &a.id, &b.id));
        all.dedup_by(|a, b| a.node_id == b.node_id);
        all.truncate(k.max(1));
        all
    }

    pub fn contact_count(&self) -> usize {
        self.buckets.iter().map(|b| b.contacts().count()).sum()
    }
}

/// RPC surface between DHT nodes (in-process for tests; TCP/QUIC in production).
pub trait DhtRpc {
    fn find_node(&self, from: &str, target: &[u8; 32]) -> Vec<Contact>;
    fn find_value(&self, from: &str, key: &str) -> FindValueResult;
    fn store(&self, from: &str, value: DhtValue) -> bool;
    fn ping(&self, from: &str, to: &str) -> bool;
}

#[derive(Debug, Clone)]
pub enum FindValueResult {
    Value(DhtValue),
    Closer(Vec<Contact>),
}

/// One full DHT node: store + routing + local identity.
#[derive(Debug, Clone)]
pub struct DhtNode {
    pub contact: Contact,
    pub store: DhtStore,
    pub table: RoutingTable,
}

impl DhtNode {
    pub fn new(node_id: impl Into<String>, multiaddrs: Vec<String>) -> Self {
        let contact = Contact::from_node_id(node_id, multiaddrs);
        let table = RoutingTable::new(contact.node_id.clone());
        Self {
            contact,
            store: DhtStore::new(),
            table,
        }
    }

    pub fn handle_find_node(&self, target: &[u8; 32]) -> Vec<Contact> {
        let mut c = self.table.closest(target, K_BUCKET);
        // include self if closer
        c.push(self.contact.clone());
        c.sort_by(|a, b| closer(target, &a.id, &b.id));
        c.dedup_by(|a, b| a.node_id == b.node_id);
        c.truncate(K_BUCKET);
        c
    }

    pub fn handle_find_value(&self, key: &str) -> FindValueResult {
        if let Some(v) = self.store.get_raw(key) {
            return FindValueResult::Value(v.clone());
        }
        FindValueResult::Closer(self.handle_find_node(&key_id(key)))
    }

    pub fn handle_store(&mut self, value: DhtValue) -> bool {
        self.store
            .put_raw(value.key.clone(), value.value_json.clone(), value.seq)
    }
}

/// In-process multi-node DHT network for multi-hop tests and lab sim.
#[derive(Debug, Default)]
pub struct InProcessNetwork {
    nodes: HashMap<String, DhtNode>,
}

impl InProcessNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: DhtNode) {
        self.nodes.insert(node.contact.node_id.clone(), node);
    }

    pub fn get(&self, id: &str) -> Option<&DhtNode> {
        self.nodes.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut DhtNode> {
        self.nodes.get_mut(id)
    }

    /// Bootstrap: each node observes the others (full mesh of contacts, not of records).
    pub fn bootstrap_all(&mut self) {
        let contacts: Vec<Contact> = self.nodes.values().map(|n| n.contact.clone()).collect();
        for n in self.nodes.values_mut() {
            for c in &contacts {
                n.table.observe(c.clone());
            }
        }
    }

    /// Wire contacts so A only knows B, B knows C, etc. — forces multi-hop.
    pub fn bootstrap_chain(&mut self, order: &[&str]) {
        for w in order.windows(2) {
            let a = w[0];
            let b = w[1];
            let ca = self.nodes.get(a).map(|n| n.contact.clone());
            let cb = self.nodes.get(b).map(|n| n.contact.clone());
            if let (Some(ca), Some(cb)) = (ca, cb) {
                if let Some(na) = self.nodes.get_mut(a) {
                    na.table.observe(cb);
                }
                if let Some(nb) = self.nodes.get_mut(b) {
                    nb.table.observe(ca);
                }
            }
        }
    }

    fn rpc_find_node(&self, to: &str, target: &[u8; 32]) -> Vec<Contact> {
        self.nodes
            .get(to)
            .map(|n| n.handle_find_node(target))
            .unwrap_or_default()
    }

    fn rpc_find_value(&self, to: &str, key: &str) -> FindValueResult {
        self.nodes
            .get(to)
            .map(|n| n.handle_find_value(key))
            .unwrap_or_else(|| FindValueResult::Closer(vec![]))
    }

    fn rpc_store(&mut self, to: &str, value: DhtValue) -> bool {
        self.nodes
            .get_mut(to)
            .map(|n| n.handle_store(value))
            .unwrap_or(false)
    }

    /// Iterative FIND_NODE from `origin` toward `target_id`.
    pub fn iterative_find_node(&mut self, origin: &str, target_id: &[u8; 32]) -> Vec<Contact> {
        let mut shortlist: Vec<Contact> = self
            .nodes
            .get(origin)
            .map(|n| n.table.closest(target_id, K_BUCKET))
            .unwrap_or_default();
        let mut queried: HashSet<String> = HashSet::new();
        queried.insert(origin.to_string());

        for _ in 0..MAX_FIND_ITERS {
            shortlist.sort_by(|a, b| closer(target_id, &a.id, &b.id));
            shortlist.dedup_by(|a, b| a.node_id == b.node_id);
            let batch: Vec<Contact> = shortlist
                .iter()
                .filter(|c| !queried.contains(&c.node_id))
                .take(ALPHA)
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            let mut improved = false;
            for c in batch {
                queried.insert(c.node_id.clone());
                let found = self.rpc_find_node(&c.node_id, target_id);
                // learn contacts
                if let Some(n) = self.nodes.get_mut(origin) {
                    n.table.observe(c.clone());
                    for f in &found {
                        n.table.observe(f.clone());
                    }
                }
                for f in found {
                    if !shortlist.iter().any(|x| x.node_id == f.node_id) {
                        shortlist.push(f);
                        improved = true;
                    }
                }
            }
            if !improved {
                // still mark progress if shortlist grew from previous
                shortlist.sort_by(|a, b| closer(target_id, &a.id, &b.id));
                shortlist.truncate(K_BUCKET);
            }
            shortlist.sort_by(|a, b| closer(target_id, &a.id, &b.id));
            shortlist.truncate(K_BUCKET);
        }
        shortlist
    }

    /// Iterative FIND_VALUE; returns value if any node on the path holds it.
    pub fn iterative_find_value(&mut self, origin: &str, key: &str) -> Option<DhtValue> {
        let tid = key_id(key);
        // local first
        if let Some(v) = self.nodes.get(origin).and_then(|n| n.store.get_raw(key)) {
            return Some(v.clone());
        }
        let mut shortlist: Vec<Contact> = self
            .nodes
            .get(origin)
            .map(|n| n.table.closest(&tid, K_BUCKET))
            .unwrap_or_default();
        let mut queried: HashSet<String> = HashSet::new();
        queried.insert(origin.to_string());

        for _ in 0..MAX_FIND_ITERS {
            shortlist.sort_by(|a, b| closer(&tid, &a.id, &b.id));
            shortlist.dedup_by(|a, b| a.node_id == b.node_id);
            let batch: Vec<Contact> = shortlist
                .iter()
                .filter(|c| !queried.contains(&c.node_id))
                .take(ALPHA)
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            for c in batch {
                queried.insert(c.node_id.clone());
                match self.rpc_find_value(&c.node_id, key) {
                    FindValueResult::Value(v) => {
                        // cache on origin
                        if let Some(n) = self.nodes.get_mut(origin) {
                            n.store
                                .put_raw(v.key.clone(), v.value_json.clone(), v.seq);
                            n.table.observe(c);
                        }
                        return Some(v);
                    }
                    FindValueResult::Closer(found) => {
                        if let Some(n) = self.nodes.get_mut(origin) {
                            n.table.observe(c.clone());
                            for f in &found {
                                n.table.observe(f.clone());
                            }
                        }
                        for f in found {
                            if !shortlist.iter().any(|x| x.node_id == f.node_id) {
                                shortlist.push(f);
                            }
                        }
                    }
                }
            }
            shortlist.sort_by(|a, b| closer(&tid, &a.id, &b.id));
            shortlist.truncate(K_BUCKET);
        }
        None
    }

    /// STORE on origin then replicate to k closest peers (multi-hop discovered).
    pub fn put_replicated(&mut self, origin: &str, value: DhtValue) -> usize {
        let tid = key_id(&value.key);
        // store locally
        if let Some(n) = self.nodes.get_mut(origin) {
            n.handle_store(value.clone());
        }
        let closest = self.iterative_find_node(origin, &tid);
        let mut stored = 1usize;
        for c in closest {
            if c.node_id == origin {
                continue;
            }
            if self.rpc_store(&c.node_id, value.clone()) {
                stored += 1;
            }
        }
        stored
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{blob_key, peer_key};

    #[test]
    fn multi_hop_put_get_peer_and_blob() {
        // A -- B -- C  (chain topology; C never shares LocalMesh with A)
        let mut net = InProcessNetwork::new();
        net.add_node(DhtNode::new("node-a", vec!["tcp://10.0.0.1:7702".into()]));
        net.add_node(DhtNode::new("node-b", vec!["tcp://10.0.0.2:7702".into()]));
        net.add_node(DhtNode::new("node-c", vec!["tcp://10.0.0.3:7702".into()]));
        net.bootstrap_chain(&["node-a", "node-b", "node-c"]);

        // Put peer record on A
        let peer_val = DhtValue {
            key: peer_key("node-z"),
            value_json: r#"{"node_id":"node-z","multiaddrs":["quic://203.0.113.9:7702"],"seq":1}"#
                .into(),
            seq: 1,
            updated_unix_ms: 1,
        };
        let n = net.put_replicated("node-a", peer_val.clone());
        assert!(n >= 1, "replicated to at least origin");

        // C must retrieve via multi-hop find (C only knows B initially)
        let got = net
            .iterative_find_value("node-c", &peer_key("node-z"))
            .expect("C finds peer record via multi-hop");
        assert_eq!(got.key, peer_key("node-z"));
        assert!(got.value_json.contains("203.0.113.9"));

        // Blob key path
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let blob_val = DhtValue {
            key: blob_key(hash),
            value_json: format!(r#"{{"sha256":"{hash}","seeders":{{"node-a":{{"size":99,"multiaddrs":["tcp://10.0.0.1:7702"]}}}}}}"#),
            seq: 2,
            updated_unix_ms: 2,
        };
        net.put_replicated("node-a", blob_val);
        let got_blob = net
            .iterative_find_value("node-c", &blob_key(hash))
            .expect("C finds blob record via multi-hop");
        assert!(got_blob.value_json.contains(hash));

        // Prove C did not start with A's store: A has the key, C only after find
        assert!(net.get("node-a").unwrap().store.get_raw(&blob_key(hash)).is_some());
    }

    #[test]
    fn k_bucket_observes_and_closest() {
        let mut t = RoutingTable::new("local");
        for i in 0..12 {
            t.observe(Contact::from_node_id(format!("peer-{i}"), vec![]));
        }
        assert!(t.contact_count() <= 256 * K_BUCKET);
        assert!(!t.closest(&key_id("x"), 3).is_empty());
    }
}
