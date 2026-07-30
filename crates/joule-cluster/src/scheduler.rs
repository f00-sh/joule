//! Cluster compute scheduler: free / loaded / full slots for the single model.
//!
//! Every healthy donor has a finite number of concurrent job **slots**.
//! The scheduler only places work on nodes with free slots, prefers free
//! nodes over loaded ones, and never schedules onto full or offline nodes.

use crate::{Cluster, Node};
use joule_proto::DeviceClass;
use serde::Serialize;

/// How busy a donor is relative to its concurrent capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeState {
    /// Healthy, zero inflight — preferred.
    Free,
    /// Healthy, some work, still has free slots.
    Loaded,
    /// Healthy but all slots taken — not schedulable until a job finishes.
    Full,
    /// Unhealthy, banned, or self-reported saturated.
    Unavailable,
}

/// One node as the scheduler sees it.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSchedule {
    pub id: String,
    pub account: String,
    pub state: ComputeState,
    pub inflight: u32,
    pub max_slots: u32,
    pub free_slots: u32,
    pub load: f32,
    pub mem_mib: u32,
}

/// Live free/loaded summary for dashboard + API.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerSnapshot {
    pub nodes_free: u32,
    pub nodes_loaded: u32,
    pub nodes_full: u32,
    pub nodes_unavailable: u32,
    pub slots_free: u32,
    pub slots_used: u32,
    pub slots_total: u32,
    pub can_accept_work: bool,
    pub nodes: Vec<NodeSchedule>,
}

/// Concurrent job slots for a donor.
///
/// Conservative defaults: consumer GPUs usually run one generation at a time;
/// larger cards may take two. Heartbeat `load` near 1.0 closes slots.
pub fn max_slots(node: &Node) -> u32 {
    if !node.healthy || node.reputation.is_banned(std::time::Instant::now()) {
        return 0;
    }
    // Donor says it's saturated.
    if node.load >= 0.95 {
        return 0;
    }
    let base = match node.caps.device {
        DeviceClass::Gpu if node.caps.mem_mib >= 20_480 => 2,
        DeviceClass::Gpu if node.caps.mem_mib >= 8_192 => 1,
        DeviceClass::Gpu => 1,
        DeviceClass::Metal if node.caps.mem_mib >= 16_384 => 2,
        DeviceClass::Metal => 1,
        DeviceClass::Cpu => 1,
    };
    if node.load >= 0.75 {
        base.min(1)
    } else {
        base
    }
}

pub fn free_slots(node: &Node) -> u32 {
    max_slots(node).saturating_sub(node.inflight)
}

pub fn compute_state(node: &Node) -> ComputeState {
    let max = max_slots(node);
    if max == 0 {
        return ComputeState::Unavailable;
    }
    if node.inflight == 0 {
        ComputeState::Free
    } else if node.inflight < max {
        ComputeState::Loaded
    } else {
        ComputeState::Full
    }
}

impl Cluster {
    /// Snapshot free/loaded/full compute for control + dashboard.
    pub fn scheduler_snapshot(&self) -> SchedulerSnapshot {
        let mut nodes_free = 0u32;
        let mut nodes_loaded = 0u32;
        let mut nodes_full = 0u32;
        let mut nodes_unavailable = 0u32;
        let mut slots_free = 0u32;
        let mut slots_used = 0u32;
        let mut slots_total = 0u32;
        let mut nodes = Vec::new();

        for n in self.nodes() {
            let max = max_slots(n);
            let free = free_slots(n);
            let state = compute_state(n);
            match state {
                ComputeState::Free => nodes_free += 1,
                ComputeState::Loaded => nodes_loaded += 1,
                ComputeState::Full => nodes_full += 1,
                ComputeState::Unavailable => nodes_unavailable += 1,
            }
            slots_total = slots_total.saturating_add(max);
            slots_free = slots_free.saturating_add(free);
            slots_used = slots_used.saturating_add(n.inflight.min(max));

            nodes.push(NodeSchedule {
                id: n.id.to_string(),
                account: n.account.clone(),
                state,
                inflight: n.inflight,
                max_slots: max,
                free_slots: free,
                load: n.load,
                mem_mib: n.caps.mem_mib,
            });
        }

        nodes.sort_by(|a, b| {
            state_rank(a.state)
                .cmp(&state_rank(b.state))
                .then(b.free_slots.cmp(&a.free_slots))
                .then(a.account.cmp(&b.account))
        });

        SchedulerSnapshot {
            nodes_free,
            nodes_loaded,
            nodes_full,
            nodes_unavailable,
            slots_free,
            slots_used,
            slots_total,
            can_accept_work: slots_free > 0,
            nodes,
        }
    }

    /// Rank only nodes that still have free slots (free first, then loaded).
    pub fn rank_schedulable(&self) -> Vec<joule_proto::NodeId> {
        let mut eligible: Vec<&Node> = self.nodes().filter(|n| free_slots(n) > 0).collect();
        eligible.sort_by(|a, b| {
            // Free before loaded; more free slots; less inflight; better rep; more mem.
            let sa = compute_state(a);
            let sb = compute_state(b);
            state_rank(sa)
                .cmp(&state_rank(sb))
                .then(free_slots(b).cmp(&free_slots(a)))
                .then(a.inflight.cmp(&b.inflight))
                .then(
                    b.reputation
                        .score()
                        .partial_cmp(&a.reputation.score())
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
                .then(b.caps.mem_mib.cmp(&a.caps.mem_mib))
                .then(a.rr_seq.cmp(&b.rr_seq))
        });
        eligible.into_iter().map(|n| n.id.clone()).collect()
    }

    /// Try to reserve one job slot on the freest schedulable node.
    /// Returns `None` when the pool is fully loaded (caller may wait).
    pub fn try_acquire_slot(&mut self) -> Option<joule_proto::NodeId> {
        let ranked = self.rank_schedulable();
        let id = ranked.into_iter().next()?;
        // Re-check under mutation.
        {
            let node = self.get_mut(&id)?;
            if free_slots(node) == 0 {
                return None;
            }
        }
        self.rr_counter = self.rr_counter.wrapping_add(1);
        let seq = self.rr_counter;
        if let Some(n) = self.get_mut(&id) {
            n.inflight = n.inflight.saturating_add(1);
            n.rr_seq = seq;
        }
        Some(id)
    }

    /// Reserve up to `n` slots on distinct schedulable nodes (free first).
    pub fn try_acquire_slots(&mut self, n: usize) -> Vec<joule_proto::NodeId> {
        let mut out = Vec::new();
        for _ in 0..n {
            match self.try_acquire_slot() {
                Some(id) => out.push(id),
                None => break,
            }
        }
        out
    }

    pub fn total_free_slots(&self) -> u32 {
        self.nodes().map(free_slots).sum()
    }
}

fn state_rank(s: ComputeState) -> u8 {
    match s {
        ComputeState::Free => 0,
        ComputeState::Loaded => 1,
        ComputeState::Full => 2,
        ComputeState::Unavailable => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cluster;
    use joule_proto::{DeviceClass, NodeCaps, NodeId};

    fn add(c: &mut Cluster, mem: u32) -> NodeId {
        let id = NodeId::new();
        c.upsert_node(
            id.clone(),
            "d",
            NodeCaps::for_cluster(DeviceClass::Gpu, mem, 10),
        );
        id
    }

    #[test]
    fn free_then_loaded_then_full() {
        let mut c = Cluster::default();
        let a = add(&mut c, 8192); // max_slots = 1
        assert_eq!(compute_state(c.get(&a).unwrap()), ComputeState::Free);
        let got = c.try_acquire_slot().unwrap();
        assert_eq!(got, a);
        assert_eq!(compute_state(c.get(&a).unwrap()), ComputeState::Full);
        assert!(c.try_acquire_slot().is_none());
        c.release_worker(&a);
        assert_eq!(compute_state(c.get(&a).unwrap()), ComputeState::Free);
    }

    #[test]
    fn prefers_free_over_loaded() {
        let mut c = Cluster::default();
        let a = add(&mut c, 24_576); // max 2
        let b = add(&mut c, 24_576);
        let first = c.try_acquire_slot().unwrap();
        // one loaded, one free — next should be the free one
        let second = c.try_acquire_slot().unwrap();
        assert_ne!(first, second);
        c.release_worker(&a);
        c.release_worker(&b);
    }

    #[test]
    fn snapshot_counts() {
        let mut c = Cluster::default();
        let a = add(&mut c, 8192);
        let _b = add(&mut c, 8192);
        c.try_acquire_slot().unwrap(); // one full (1 slot)
        let snap = c.scheduler_snapshot();
        assert_eq!(snap.nodes_free, 1);
        assert_eq!(snap.nodes_full, 1);
        assert!(snap.can_accept_work);
        assert_eq!(snap.slots_free, 1);
        let _ = a;
    }
}
