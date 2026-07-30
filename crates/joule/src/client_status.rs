//! Fetch live control fields and assemble [`joule_client::ClientStatus`].

use anyhow::{Context, Result};
use joule_client::{ClientStatus, StatusInputs};

/// Poll control plane HTTP endpoints and build a status snapshot.
pub async fn fetch_client_status(api: &str, key: Option<&str>) -> Result<ClientStatus> {
    let base = api.trim_end_matches('/').to_string();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .context("http client")?;

    let mut inputs = StatusInputs {
        api_base: base.clone(),
        ..Default::default()
    };

    // healthz
    match client.get(format!("{base}/healthz")).send().await {
        Ok(r) if r.status().is_success() => {
            inputs.control_reachable = true;
            if let Ok(v) = r.json::<serde_json::Value>().await {
                inputs.agents_connected = v
                    .get("agents_connected")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                inputs.service_live = v
                    .get("service_live")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(false);
                inputs.operator_paused = v
                    .get("operator_paused")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(false);
                inputs.stream_slots_free = v
                    .get("stream_slots_free")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                inputs.stream_slots_used = v
                    .get("stream_slots_used")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                if let Some(ld) = v.get("logical_device") {
                    inputs.pool_backends =
                        ld.get("backends").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
                    inputs.pool_vram_gib = ld.get("vram_gib").and_then(|x| x.as_u64()).unwrap_or(0);
                }
            }
        }
        _ => {
            inputs.control_reachable = false;
        }
    }

    // capacity (richer pool fields)
    if inputs.control_reachable {
        if let Ok(r) = client
            .get(format!("{base}/v1/cluster/capacity"))
            .send()
            .await
        {
            if r.status().is_success() {
                if let Ok(v) = r.json::<serde_json::Value>().await {
                    if let Some(ld) = v.get("logical_device") {
                        if let Some(b) = ld.get("backends").and_then(|x| x.as_u64()) {
                            inputs.pool_backends = b as u32;
                        }
                        if let Some(g) = ld.get("vram_gib").and_then(|x| x.as_u64()) {
                            inputs.pool_vram_gib = g;
                        }
                        if let Some(m) = ld.get("inference_mode").and_then(|x| x.as_str()) {
                            inputs.inference_mode = m.to_string();
                        }
                    }
                    if let Some(n) = v.get("nodes_healthy").and_then(|x| x.as_u64()) {
                        if inputs.pool_backends == 0 {
                            inputs.pool_backends = n as u32;
                        }
                    }
                }
            }
        }
    }

    // account (optional key)
    if let Some(k) = key {
        if !k.is_empty() && inputs.control_reachable {
            inputs.api_key_hint = Some(mask_key(k));
            match client
                .get(format!("{base}/v1/account"))
                .bearer_auth(k)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        inputs.account = v
                            .get("account")
                            .and_then(|x| x.as_str())
                            .map(|s| s.to_string());
                        inputs.donating =
                            v.get("donating").and_then(|x| x.as_bool()).unwrap_or(false);
                        inputs.balance_millijoules = v
                            .get("balance_millijoules")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0);
                        inputs.contributed_mj_window = v
                            .get("contributed_mj_window")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0);
                        inputs.consumed_mj_window = v
                            .get("consumed_mj_window")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0);
                        inputs.prompt_tokens_used = v
                            .get("prompt_tokens_used")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(0);
                        inputs.completion_tokens_used = v
                            .get("completion_tokens_used")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(0);
                    }
                }
                Ok(r) => {
                    inputs.account = Some(format!("(account HTTP {})", r.status()));
                }
                Err(e) => {
                    inputs.account = Some(format!("(account error: {e})"));
                }
            }
        }
    }

    Ok(ClientStatus::from_inputs(inputs))
}

fn mask_key(k: &str) -> String {
    if k.len() <= 12 {
        return "joule_…".into();
    }
    format!("{}…{}", &k[..10], &k[k.len().saturating_sub(4)..])
}
