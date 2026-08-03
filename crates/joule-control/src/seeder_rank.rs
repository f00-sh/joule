//! Seeder selection under load: locality, health, backpressure.

use joule_proto::NodeId;

/// Inputs for ranking a candidate seeder (pure — no I/O).
#[derive(Debug, Clone)]
pub struct SeederCandidate {
    pub node: NodeId,
    pub multiaddrs: Vec<String>,
    pub healthy: bool,
    /// 0.0 idle … 1.0 saturated.
    pub load: f32,
    /// Concurrent blob transfers already involving this seeder.
    pub active_transfers: u32,
    /// Free stream slots on the pool (global backpressure signal).
    pub pool_stream_slots_free: u32,
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
    let local = c.multiaddrs.iter().any(|a| {
        a.contains("127.0.0.1") || a.contains("localhost") || a.contains("::1")
    });
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
    // Active transfer penalty (backpressure)
    s -= i64::from(c.active_transfers) * 150;
    // Hard refuse if overloaded
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

    fn cand(
        _tag: &str,
        addrs: Vec<&str>,
        healthy: bool,
        load: f32,
        xfers: u32,
        free: u32,
    ) -> SeederCandidate {
        SeederCandidate {
            node: NodeId::new(),
            multiaddrs: addrs.into_iter().map(String::from).collect(),
            healthy,
            load,
            active_transfers: xfers,
            pool_stream_slots_free: free,
        }
    }

    #[test]
    fn prefers_local_healthy_over_remote_loaded() {
        let local = cand("l", vec!["tcp://127.0.0.1:9"], true, 0.1, 0, 4);
        let remote = cand("r", vec!["tcp://8.8.8.8:9"], true, 0.8, 2, 4);
        let sick = cand("s", vec!["tcp://127.0.0.1:9"], false, 0.0, 0, 4);
        let cands = vec![remote.clone(), sick.clone(), local.clone()];
        let best = pick_ranked_seeder(&cands).expect("pick");
        assert_eq!(best.node, local.node);
        assert!(seeder_score(&sick) < 0);
    }

    #[test]
    fn overloaded_refused_under_backpressure() {
        let hot = cand("h", vec!["tcp://10.0.0.1:9"], true, 0.99, 9, 0);
        assert!(seeder_score(&hot) < 0);
        assert!(pick_ranked_seeder(&[hot]).is_none());
    }
}
