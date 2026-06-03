//! Phase 4 (M4.2): when a profile changes, push the freshly re-merged profile
//! to every live session that uses it, so po-k can hot-reload the plugin dir.

use serde_json::Value;
use uuid::Uuid;

use crate::state::XState;
use crate::{merge, store};

/// Called after `PUT /profiles/{name}` succeeds.
pub async fn on_profile_updated(st: &XState, name: &str) {
    let sessions = match store::live_sessions(&st.db).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "live_sessions query failed");
            return;
        }
    };
    // Which categories the changed profile touches → po-k uses this to decide
    // whether a `/reload-plugins` nudge is needed (agents/mcp/hooks are
    // structural; claude_md/skills hot-reload on their own).
    let changed_fields = changed_fields_for(st, name).await;

    for (sid, pok_id, names) in sessions {
        if !names.iter().any(|n| n == name) {
            continue;
        }
        let Some(merged) = remerge(st, &names).await else {
            continue;
        };
        let sent = st.registry.send(
            &pok_id,
            pok_proto::WsMsg::ProfileUpdate {
                session_id: sid.clone(),
                profile: merged,
                changed_fields: changed_fields.clone(),
            },
        );
        if sent {
            tracing::info!(sid, profile = name, "pushed profile_update");
        }
    }
}

async fn remerge(st: &XState, names: &[String]) -> Option<Value> {
    let mut profiles = Vec::with_capacity(names.len());
    for n in names {
        let row = store::get_profile(&st.db, n).await.ok()??;
        let v: Value = serde_json::from_str(&row.data).ok()?;
        profiles.push(pok_proto::Profile::from_json(&v).ok()?);
    }
    serde_json::to_value(merge::merge(&profiles)).ok()
}

async fn changed_fields_for(st: &XState, name: &str) -> Vec<String> {
    let Ok(Some(row)) = store::get_profile(&st.db, name).await else {
        return vec![];
    };
    let Ok(v) = serde_json::from_str::<Value>(&row.data) else {
        return vec![];
    };
    let mut out = Vec::new();
    for key in ["claude_md", "agents", "skills", "mcp_servers", "hooks"] {
        if let Some(val) = v.get(key) {
            let non_empty = match val {
                Value::Object(m) => !m.is_empty(),
                Value::String(s) => !s.is_empty(),
                _ => false,
            };
            if non_empty {
                out.push(key.to_string());
            }
        }
    }
    out
}

/// Reserved for direct (pre-session) profile delivery via `push_profile`.
#[allow(dead_code)]
pub fn fresh_request_id() -> Uuid {
    Uuid::new_v4()
}
