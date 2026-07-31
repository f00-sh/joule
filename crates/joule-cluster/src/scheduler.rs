//! Sharded-pool scheduler.
//!
//! One model is spread across **all** healthy donors proportional to VRAM.
//! A single request does **not** monopolize one GPU: it rides the shared
//! multi-node plan and consumes one **stream slot** of aggregate capacity,
//! leaving room for other concurrent users.

use crate::{Cluster, Node};
use joule_proto::{ClusterPlan, NodeId, ShardAssignment, ShardRole, CLUSTER_MODEL};
use serde::Serialize;
use uuid::Uuid;

/// Default transformer layer count for placement math (placeholder until model config).
pub const DEFAULT_MODEL_LAYERS: u32 = 80;

/// Rough KV/activation budget (MiB) reserved per concurrent generation stream
/// against **aggregate** pool VRAM. Lower → more concurrent users.
pub const STREAM_BUDGET_MIB: u64 = 4096;

/// How a donor participates in the sharded pool (not exclusive whole-GPU ownership).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeState {
    /// Healthy, in the active shard map, not saturated.
    Free,
    /// Carrying some concurrent streams (shared mesh).
    Loaded,
    /// At stream capacity for this node.
    Full,
    /// Not in the mesh (down / banned).
    Unavailable,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeSchedule {
    pub id: String,
    pub account: String,
    pub state: ComputeState,
    pub mem_mib: u32,
    /// VRAM this node contributes to the sharded model.
    pub mem_share_mib: u32,
    pub mem_fraction_ppm: u32,
    pub layer_start: Option<u32>,
    pub layer_end: Option<u32>,
    pub inflight_streams: u32,
    pub max_streams: u32,
    pub free_stream_slots: u32,
    pub load: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerSnapshot {
    /// External story: one logical device, aggregate VRAM.
    pub view: &'static str,
    pub mode: &'static str,
    pub pool_mem_mib: u64,
    pub pool_mem_gib: u64,
    pub model: String,
    pub shards: u32,
    pub stream_slots_total: u32,
    pub stream_slots_used: u32,
    pub stream_slots_free: u32,
    pub can_accept_work: bool,
    pub nodes_free: u32,
    pub nodes_loaded: u32,
    pub nodes_full: u32,
    pub nodes_unavailable: u32,
    /// Internal: how the logical device is assembled (not separate public GPUs).
    pub backends: Vec<NodeSchedule>,
    pub plan: Option<ClusterPlan>,
}

/// Concurrent streams this node can help serve (shared sharded model).
pub fn max_streams(node: &Node) -> u32 {
    if !node.healthy || node.reputation.is_banned(std::time::Instant::now()) {
        return 0;
    }
    if node.load >= 0.95 {
        return 0;
    }
    // Unverified claim = zero slots (cannot fake a farm into the schedule).
    let place = crate::placement_mem_mib(node.verified_mem_mib);
    if place == 0 {
        return 0;
    }
    // Share of global streams scales with **verified** VRAM only.
    let eff = u64::from(crate::economic_mem_mib(place));
    let by_mem = (eff / (STREAM_BUDGET_MIB / 4).max(1)).max(1) as u32;
    let cap = match node.caps.device {
        joule_proto::DeviceClass::Gpu => by_mem.min(8),
        joule_proto::DeviceClass::Metal => by_mem.min(6),
        joule_proto::DeviceClass::Cpu => 1,
    };
    if node.load >= 0.75 {
        cap.clamp(1, 2)
    } else {
        cap.max(1)
    }
}

pub fn free_stream_slots(node: &Node) -> u32 {
    max_streams(node).saturating_sub(node.inflight)
}

/// Alias used by control/dashboard (stream slots, not exclusive whole-GPU locks).
pub fn max_slots(node: &Node) -> u32 {
    max_streams(node)
}

pub fn free_slots(node: &Node) -> u32 {
    free_stream_slots(node)
}

pub fn compute_state(node: &Node) -> ComputeState {
    let max = max_streams(node);
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

/// Global stream capacity from aggregate healthy VRAM.
pub fn pool_max_streams(total_mem_mib: u64) -> u32 {
    if total_mem_mib == 0 {
        return 0;
    }
    (total_mem_mib / STREAM_BUDGET_MIB).max(1) as u32
}

impl Cluster {
    /// VRAM-weighted pipeline: **every** healthy donor holds a slice of the one model.
    ///
    /// Example: 8+16+16+16+16 GiB → five shards sized ~8/72, 16/72, … of layers.
    pub fn plan_sharded_pool(&self) -> Result<ClusterPlan, crate::ClusterError> {
        // Placement requires verified > 0 — claim-only peers never enter the logical GPU.
        let mut donors: Vec<&Node> = self
            .eligible()
            .into_iter()
            .filter(|n| crate::placement_mem_mib(n.verified_mem_mib) > 0)
            .collect();
        if donors.is_empty() {
            return Err(crate::ClusterError::NoEligibleNodes(
                CLUSTER_MODEL.to_string(),
            ));
        }
        // Stable order: largest **verified** VRAM first (claims cannot buy priority).
        donors.sort_by(|a, b| {
            crate::placement_mem_mib(b.verified_mem_mib)
                .cmp(&crate::placement_mem_mib(a.verified_mem_mib))
                .then_with(|| a.id.0.cmp(&b.id.0))
        });

        // Self-govern: weight by verified placement only (claims cannot inflate geometry).
        let pool_mem: u64 = donors
            .iter()
            .map(|n| u64::from(crate::placement_mem_mib(n.verified_mem_mib)))
            .sum();
        if pool_mem == 0 {
            return Err(crate::ClusterError::NoEligibleNodes(
                CLUSTER_MODEL.to_string(),
            ));
        }

        let layers = DEFAULT_MODEL_LAYERS;
        let mut shards = Vec::with_capacity(donors.len());
        let mut layer_cursor = 0u32;
        let mut ppm_acc = 0u32;

        for (i, n) in donors.iter().enumerate() {
            let eff = crate::placement_mem_mib(n.verified_mem_mib);
            let mem = u64::from(eff);
            let mut ppm = ((mem * 1_000_000) / pool_mem) as u32;
            let is_last = i + 1 == donors.len();
            if is_last {
                ppm = 1_000_000u32.saturating_sub(ppm_acc);
            } else {
                ppm_acc = ppm_acc.saturating_add(ppm);
            }

            let layer_start = layer_cursor.min(layers.saturating_sub(1));
            let layer_end = if is_last {
                layers.saturating_sub(1)
            } else {
                let span = ((u64::from(layers) * mem) / pool_mem).max(1) as u32;
                let end = layer_start.saturating_add(span).saturating_sub(1);
                end.min(layers.saturating_sub(1))
            };
            let layer_end = layer_end.max(layer_start);
            layer_cursor = layer_end.saturating_add(1).min(layers);

            shards.push(ShardAssignment {
                node: n.id.clone(),
                role: if donors.len() == 1 {
                    ShardRole::Replica
                } else {
                    ShardRole::Pipeline
                },
                layer_start: Some(layer_start),
                layer_end: Some(layer_end),
                tp_rank: None,
                tp_world: None,
                mem_share_mib: eff,
                mem_fraction_ppm: ppm,
            });
        }

        Ok(ClusterPlan {
            plan_id: Uuid::new_v4(),
            model: CLUSTER_MODEL.to_string(),
            shards,
            pool_mem_mib: pool_mem,
            model_layers: layers,
        })
    }

    pub fn scheduler_snapshot(&self) -> SchedulerSnapshot {
        let plan = self.plan_sharded_pool().ok();
        let pool_mem = plan.as_ref().map(|p| p.pool_mem_mib).unwrap_or_else(|| {
            self.eligible()
                .iter()
                .map(|n| u64::from(crate::placement_mem_mib(n.verified_mem_mib)))
                .sum()
        });
        let stream_total = pool_max_streams(pool_mem);
        // Global used = max inflight across mesh (streams touch all shards).
        let stream_used = self
            .eligible()
            .iter()
            .map(|n| n.inflight)
            .max()
            .unwrap_or(0)
            .min(stream_total);

        let mut nodes_free = 0u32;
        let mut nodes_loaded = 0u32;
        let mut nodes_full = 0u32;
        let mut nodes_unavailable = 0u32;
        let mut nodes = Vec::new();

        let plan_ref = plan.as_ref();
        for n in self.nodes() {
            let st = compute_state(n);
            match st {
                ComputeState::Free => nodes_free += 1,
                ComputeState::Loaded => nodes_loaded += 1,
                ComputeState::Full => nodes_full += 1,
                ComputeState::Unavailable => nodes_unavailable += 1,
            }
            let (mem_share, ppm, ls, le) = plan_ref
                .and_then(|p| {
                    p.shards.iter().find(|s| s.node == n.id).map(|s| {
                        (
                            s.mem_share_mib,
                            s.mem_fraction_ppm,
                            s.layer_start,
                            s.layer_end,
                        )
                    })
                })
                .unwrap_or((n.caps.mem_mib, 0, None, None));

            nodes.push(NodeSchedule {
                id: n.id.to_string(),
                account: n.account.clone(),
                state: st,
                mem_mib: n.caps.mem_mib,
                mem_share_mib: mem_share,
                mem_fraction_ppm: ppm,
                layer_start: ls,
                layer_end: le,
                inflight_streams: n.inflight,
                max_streams: max_streams(n),
                free_stream_slots: free_stream_slots(n),
                load: n.load,
            });
        }

        nodes.sort_by_key(|a| std::cmp::Reverse(a.mem_share_mib));

        SchedulerSnapshot {
            view: "one_logical_device",
            mode: "vram_sharded_pool",
            pool_mem_mib: pool_mem,
            pool_mem_gib: pool_mem / 1024,
            model: CLUSTER_MODEL.to_string(),
            shards: plan.as_ref().map(|p| p.shards.len() as u32).unwrap_or(0),
            stream_slots_total: stream_total,
            stream_slots_used: stream_used,
            stream_slots_free: stream_total.saturating_sub(stream_used),
            can_accept_work: stream_used < stream_total && plan.is_some(),
            nodes_free,
            nodes_loaded,
            nodes_full,
            nodes_unavailable,
            backends: nodes,
            plan,
        }
    }

    /// Reserve one concurrent stream on the **whole** sharded mesh.
    /// Increments inflight on every healthy shard (shared model).
    pub fn try_acquire_stream(&mut self) -> Option<ClusterPlan> {
        let plan = self.plan_sharded_pool().ok()?;
        let pool_mem = plan.pool_mem_mib;
        let max = pool_max_streams(pool_mem);
        let used = self
            .eligible()
            .iter()
            .map(|n| n.inflight)
            .max()
            .unwrap_or(0);
        if used >= max {
            return None;
        }
        // Every shard participates lightly.
        for s in &plan.shards {
            if let Some(n) = self.nodes.get_mut(&s.node) {
                if free_stream_slots(n) == 0 {
                    // roll back partial
                    for s2 in &plan.shards {
                        if s2.node == s.node {
                            break;
                        }
                        if let Some(nn) = self.nodes.get_mut(&s2.node) {
                            nn.inflight = nn.inflight.saturating_sub(1);
                        }
                    }
                    return None;
                }
                n.inflight = n.inflight.saturating_add(1);
            }
        }
        Some(plan)
    }

    /// Release one stream reservation from all listed shards.
    pub fn release_stream(&mut self, plan: &ClusterPlan) {
        for s in &plan.shards {
            if let Some(n) = self.nodes.get_mut(&s.node) {
                n.inflight = n.inflight.saturating_sub(1);
            }
        }
    }

    // --- compatibility wrappers used by older call sites ---

    pub fn max_slots(node: &Node) -> u32 {
        max_streams(node)
    }

    pub fn free_slots(node: &Node) -> u32 {
        free_stream_slots(node)
    }

    pub fn try_acquire_slot(&mut self) -> Option<NodeId> {
        // Single-node acquire is wrong for product; keep for tests of slot math only.
        let ranked = self.rank_schedulable();
        let id = ranked.into_iter().next()?;
        {
            let node = self.get_mut(&id)?;
            if free_stream_slots(node) == 0 {
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

    pub fn try_acquire_slots(&mut self, n: usize) -> Vec<NodeId> {
        let mut out = Vec::new();
        for _ in 0..n {
            match self.try_acquire_slot() {
                Some(id) => out.push(id),
                None => break,
            }
        }
        out
    }

    pub fn rank_schedulable(&self) -> Vec<NodeId> {
        let mut eligible: Vec<&Node> = self.nodes().filter(|n| free_stream_slots(n) > 0).collect();
        eligible.sort_by(|a, b| {
            free_stream_slots(b)
                .cmp(&free_stream_slots(a))
                .then(a.inflight.cmp(&b.inflight))
                .then(
                    crate::placement_mem_mib(b.verified_mem_mib)
                        .cmp(&crate::placement_mem_mib(a.verified_mem_mib)),
                )
        });
        eligible.into_iter().map(|n| n.id.clone()).collect()
    }

    pub fn total_free_slots(&self) -> u32 {
        let snap = self.scheduler_snapshot();
        snap.stream_slots_free
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
    fn sharded_plan_uses_all_vram() {
        let mut c = Cluster::default();
        add(&mut c, 8192);
        add(&mut c, 16384);
        add(&mut c, 16384);
        add(&mut c, 16384);
        add(&mut c, 16384);
        c.trust_all_claims_for_tests();
        let plan = c.plan_sharded_pool().unwrap();
        assert_eq!(plan.shards.len(), 5);
        assert_eq!(plan.pool_mem_mib, 8192 + 16384 * 4);
        let ppm: u32 = plan.shards.iter().map(|s| s.mem_fraction_ppm).sum();
        assert!((999_000..=1_000_000).contains(&ppm), "ppm={ppm}");
        // 8GB node should get smaller layer span than 16GB
        let small = plan
            .shards
            .iter()
            .find(|s| s.mem_share_mib == 8192)
            .unwrap();
        let big = plan
            .shards
            .iter()
            .find(|s| s.mem_share_mib == 16384)
            .unwrap();
        let small_span = small.layer_end.unwrap() - small.layer_start.unwrap() + 1;
        let big_span = big.layer_end.unwrap() - big.layer_start.unwrap() + 1;
        assert!(big_span >= small_span);
    }

    #[test]
    fn stream_slots_scale_with_pool() {
        let mut c = Cluster::default();
        add(&mut c, 8192);
        add(&mut c, 16384);
        add(&mut c, 16384);
        add(&mut c, 16384);
        add(&mut c, 16384);
        c.trust_all_claims_for_tests();
        let total = 8192 + 16384 * 4;
        let max = pool_max_streams(total);
        assert!(max >= 1);
        let plan = c.try_acquire_stream().unwrap();
        assert_eq!(plan.shards.len(), 5);
        // second stream ok until cap
        let _ = c.try_acquire_stream();
        c.release_stream(&plan);
    }
}
