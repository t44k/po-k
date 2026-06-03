//! Permission round-trip: the blocking `approve` call (from the `po-k mcp`
//! subprocess) and the orchestrator's `resolve`.

use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::events_store;
use crate::permissions::Decision;
use crate::state::AppState;

pub async fn approve(
    state: &AppState,
    sid: &str,
    tool_name: &str,
    input: Value,
) -> CoreResult<CoreResponse> {
    if state.sessions.get(sid).await.is_none()
        && events_store::get_session(&state.db, sid)
            .await
            .map_err(internal)?
            .is_none()
    {
        return Err(CoreError::not_found(sid));
    }

    let timeout_ms: u64 = state
        .config
        .read()
        .await
        .cc
        .permission_timeout
        .0
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let request_id = format!("req-{}", Uuid::new_v4().simple());
    let rx = state.perms.register(request_id.clone()).await;

    let payload = json!({
        "request_id": request_id,
        "tool": tool_name,
        "input": input,
        "timeout_ms": timeout_ms,
    });
    let ts = events_store::now_iso();
    if let Err(e) =
        events_store::append_event(&state.db, sid, &ts, "permission_request", &payload).await
    {
        state.perms.forget(&request_id).await;
        return Err(internal(e));
    }
    state.bus.notify(sid).await;

    let decision = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
        Ok(Ok(d)) => d,
        Ok(Err(_)) => Decision::deny("po-k permission tracker dropped"),
        Err(_) => {
            state.perms.forget(&request_id).await;
            Decision::deny("po-k permission timeout")
        }
    };

    let outcome = json!({
        "request_id": request_id,
        "behavior": decision.behavior,
        "message": decision.message,
    });
    let _ = events_store::append_event(
        &state.db,
        sid,
        &events_store::now_iso(),
        "permission_decision",
        &outcome,
    )
    .await;
    state.bus.notify(sid).await;

    Ok(CoreResponse::ok(
        serde_json::to_value(decision).unwrap_or(json!({})),
    ))
}

pub async fn resolve(
    state: &AppState,
    req_id: &str,
    behavior: &str,
    message: Option<String>,
) -> CoreResult<CoreResponse> {
    if behavior != "allow" && behavior != "deny" {
        return Err(CoreError::BadRequest(
            "behavior must be \"allow\" or \"deny\"".into(),
        ));
    }
    state
        .perms
        .resolve(
            req_id,
            Decision {
                behavior: behavior.to_string(),
                message,
            },
        )
        .await
        .map_err(|e| CoreError::NotFound(e.to_string()))?;
    Ok(CoreResponse::ok(json!({ "ok": true, "request_id": req_id })))
}
