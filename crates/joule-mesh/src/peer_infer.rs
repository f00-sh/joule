//! Peer-direct InferRequest path policy (no control relay required).

/// True when a shard has a dialable multiaddr for peer-direct Infer*.
pub fn can_peer_infer(multiaddrs: &[String]) -> bool {
    multiaddrs.iter().any(|a| {
        let a = a.trim();
        a.starts_with("tcp://") || a.starts_with("quic://")
    })
}

/// Prefer peer-direct when all required shards have multiaddrs; else control relay.
pub fn prefer_peer_direct_infer(shard_multiaddrs: &[Vec<String>]) -> bool {
    !shard_multiaddrs.is_empty() && shard_multiaddrs.iter().all(|m| can_peer_infer(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_direct_when_all_have_tcp() {
        assert!(can_peer_infer(&["tcp://127.0.0.1:9".into()]));
        assert!(!can_peer_infer(&[]));
        assert!(prefer_peer_direct_infer(&[
            vec!["tcp://10.0.0.1:1".into()],
            vec!["tcp://10.0.0.2:1".into()],
        ]));
        assert!(!prefer_peer_direct_infer(&[
            vec!["tcp://10.0.0.1:1".into()],
            vec![],
        ]));
    }
}
