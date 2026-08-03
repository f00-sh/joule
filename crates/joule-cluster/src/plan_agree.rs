//! Pure multi-party PlanAccept settlement policy.
//!
//! **Single SoT** for control and peer-bus: same order, same effects.
//! Adapters only load state and apply [`PlanAcceptEffect`].
//!
//! Canonical order:
//! 1. no pending context → Ignore
//! 2. `from ∉ expected` → Ignore (outsider cannot pad or DoS-abort)
//! 3. wrong `plan_id` → Ignore
//! 4. bad/missing confirm or plan_hash mismatch → Abort
//! 5. `!accepted` → Abort
//! 6. else Record; `ready` iff every expected id has accepted (set equality)

use crate::verify_plan_accept_confirm;
use joule_proto::NodeId;
use std::collections::HashSet;
use uuid::Uuid;

/// Outcome of one PlanAccept against pending agreement state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanAcceptEffect {
    /// Drop message (unknown, outsider, wrong plan_id).
    Ignore,
    /// Fail closed: tear down pending agreement.
    Abort { event: &'static str, detail: String },
    /// Record accept from an expected shard; `ready` when all expected have accepted.
    Record { ready: bool },
}

/// Snapshot of pending multi-party agreement (caller owns the map).
#[derive(Debug, Clone)]
pub struct PlanAgreeView<'a> {
    pub plan_id: Uuid,
    pub want_hash: &'a str,
    pub expected: &'a HashSet<NodeId>,
    /// Shards that already sent a valid accept.
    pub already_accepted: &'a HashSet<NodeId>,
}

/// Pure settle decision (no I/O, no mutation).
///
/// `pending = None` means no active collector for this request_id.
pub fn on_accept(
    pending: Option<&PlanAgreeView<'_>>,
    from: &NodeId,
    plan_id: Uuid,
    request_id: Uuid,
    accepted: bool,
    plan_hash_hex: &str,
    confirm_hex: &str,
) -> PlanAcceptEffect {
    let Some(p) = pending else {
        return PlanAcceptEffect::Ignore;
    };
    // Outsiders: never verify (invalid confirm must not Abort).
    if !p.expected.contains(from) {
        return PlanAcceptEffect::Ignore;
    }
    if p.plan_id != plan_id {
        return PlanAcceptEffect::Ignore;
    }
    if let Err(e) = verify_plan_accept_confirm(
        plan_id,
        request_id,
        from,
        accepted,
        p.want_hash,
        confirm_hex,
    ) {
        return PlanAcceptEffect::Abort {
            event: "plan_accept_invalid",
            detail: format!("{from}: {e}"),
        };
    }
    if !plan_hash_hex.is_empty() && plan_hash_hex != p.want_hash {
        return PlanAcceptEffect::Abort {
            event: "plan_hash_mismatch",
            detail: format!("{from}: got {plan_hash_hex}"),
        };
    }
    if !accepted {
        return PlanAcceptEffect::Abort {
            event: "plan_rejected",
            detail: format!("plan rejected by {from}"),
        };
    }
    // Would be ready after inserting `from`.
    let ready = p
        .expected
        .iter()
        .all(|n| n == from || p.already_accepted.contains(n));
    PlanAcceptEffect::Record { ready }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{plan_accept_confirm_hex, plan_hash_hex};
    use joule_proto::{ClusterPlan, DeviceClass, NodeCaps, ShardAssignment, ShardRole};

    fn plan_two() -> (ClusterPlan, NodeId, NodeId) {
        let a = NodeId::new();
        let b = NodeId::new();
        let plan = ClusterPlan {
            plan_id: Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
            model: "kimi-open".into(),
            pool_mem_mib: 16384,
            model_layers: 80,
            shards: vec![
                ShardAssignment {
                    node: a.clone(),
                    role: ShardRole::Pipeline,
                    mem_share_mib: 8192,
                    mem_fraction_ppm: 500_000,
                    layer_start: Some(0),
                    layer_end: Some(39),
                    tp_rank: None,
                    tp_world: None,
                },
                ShardAssignment {
                    node: b.clone(),
                    role: ShardRole::Pipeline,
                    mem_share_mib: 8192,
                    mem_fraction_ppm: 500_000,
                    layer_start: Some(40),
                    layer_end: Some(79),
                    tp_rank: None,
                    tp_world: None,
                },
            ],
        };
        (plan, a, b)
    }

    fn expected_of(plan: &ClusterPlan) -> HashSet<NodeId> {
        plan.shards.iter().map(|s| s.node.clone()).collect()
    }

    #[test]
    fn table_plan_accept_policy() {
        let (plan, a, b) = plan_two();
        let want_hash = plan_hash_hex(&plan);
        let rid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let expected = expected_of(&plan);
        let empty_accepted = HashSet::new();
        let view = PlanAgreeView {
            plan_id: plan.plan_id,
            want_hash: &want_hash,
            expected: &expected,
            already_accepted: &empty_accepted,
        };

        // no pending
        assert_eq!(
            on_accept(None, &a, plan.plan_id, rid, true, &want_hash, "x"),
            PlanAcceptEffect::Ignore
        );

        // outsider valid confirm → Ignore
        let out = NodeId::new();
        let ok_out = plan_accept_confirm_hex(plan.plan_id, rid, &out, true, &want_hash);
        assert_eq!(
            on_accept(
                Some(&view),
                &out,
                plan.plan_id,
                rid,
                true,
                &want_hash,
                &ok_out
            ),
            PlanAcceptEffect::Ignore
        );

        // outsider invalid / empty confirm → still Ignore (not Abort)
        assert_eq!(
            on_accept(Some(&view), &out, plan.plan_id, rid, true, &want_hash, ""),
            PlanAcceptEffect::Ignore
        );
        assert_eq!(
            on_accept(
                Some(&view),
                &out,
                plan.plan_id,
                rid,
                true,
                &want_hash,
                &"00".repeat(32)
            ),
            PlanAcceptEffect::Ignore
        );

        // wrong plan_id → Ignore
        let ok_a = plan_accept_confirm_hex(plan.plan_id, rid, &a, true, &want_hash);
        assert_eq!(
            on_accept(Some(&view), &a, Uuid::nil(), rid, true, &want_hash, &ok_a),
            PlanAcceptEffect::Ignore
        );

        // expected invalid confirm → Abort
        match on_accept(
            Some(&view),
            &a,
            plan.plan_id,
            rid,
            true,
            &want_hash,
            "deadbeef",
        ) {
            PlanAcceptEffect::Abort {
                event: "plan_accept_invalid",
                ..
            } => {}
            other => panic!("expected Abort invalid, got {other:?}"),
        }

        // expected hash mismatch → Abort
        match on_accept(
            Some(&view),
            &a,
            plan.plan_id,
            rid,
            true,
            "ff".repeat(32).as_str(),
            &ok_a,
        ) {
            PlanAcceptEffect::Abort {
                event: "plan_hash_mismatch",
                ..
            } => {}
            other => panic!("expected hash mismatch Abort, got {other:?}"),
        }

        // expected reject → Abort
        let rej = plan_accept_confirm_hex(plan.plan_id, rid, &a, false, &want_hash);
        match on_accept(Some(&view), &a, plan.plan_id, rid, false, &want_hash, &rej) {
            PlanAcceptEffect::Abort {
                event: "plan_rejected",
                ..
            } => {}
            other => panic!("expected reject Abort, got {other:?}"),
        }

        // first expected accept → Record ready=false
        assert_eq!(
            on_accept(Some(&view), &a, plan.plan_id, rid, true, &want_hash, &ok_a),
            PlanAcceptEffect::Record { ready: false }
        );

        // pad with outsiders never ready: already has a, outsider ignore, only b missing
        let mut accepted = HashSet::new();
        accepted.insert(a.clone());
        let view2 = PlanAgreeView {
            plan_id: plan.plan_id,
            want_hash: &want_hash,
            expected: &expected,
            already_accepted: &accepted,
        };
        assert_eq!(
            on_accept(
                Some(&view2),
                &out,
                plan.plan_id,
                rid,
                true,
                &want_hash,
                &ok_out
            ),
            PlanAcceptEffect::Ignore
        );
        // still not ready with only a
        assert_eq!(
            on_accept(Some(&view2), &a, plan.plan_id, rid, true, &want_hash, &ok_a),
            PlanAcceptEffect::Record { ready: false }
        );

        // second expected → ready
        let ok_b = plan_accept_confirm_hex(plan.plan_id, rid, &b, true, &want_hash);
        assert_eq!(
            on_accept(Some(&view2), &b, plan.plan_id, rid, true, &want_hash, &ok_b),
            PlanAcceptEffect::Record { ready: true }
        );

        // len padding trap: already_accepted has outsider + a (len=2) but missing b
        let mut padded = HashSet::new();
        padded.insert(a.clone());
        padded.insert(out.clone());
        let view_pad = PlanAgreeView {
            plan_id: plan.plan_id,
            want_hash: &want_hash,
            expected: &expected,
            already_accepted: &padded,
        };
        // only a is expected among already; b still required
        assert_eq!(
            on_accept(
                Some(&view_pad),
                &a,
                plan.plan_id,
                rid,
                true,
                &want_hash,
                &ok_a
            ),
            PlanAcceptEffect::Record { ready: false }
        );
    }

    #[test]
    fn unused_caps_keep_cluster_plan_build_sane() {
        let _ = NodeCaps::for_cluster(DeviceClass::Gpu, 8192, 10);
    }
}
