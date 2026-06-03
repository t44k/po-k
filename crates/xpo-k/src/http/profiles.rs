//! Profile CRUD + merge/preview endpoints (spec §4.3, §6.3).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::state::XState;
use crate::{merge, store};

type Resp = (StatusCode, Json<Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> Resp {
    (code, Json(json!({ "error": msg.into() })))
}

fn internal<E: std::fmt::Display>(e: E) -> Resp {
    err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

pub async fn list(State(st): State<XState>) -> Resp {
    match store::list_profiles(&st.db).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(json!(rows.iter().map(|r| r.summary()).collect::<Vec<_>>())),
        ),
        Err(e) => internal(e),
    }
}

pub async fn get(State(st): State<XState>, Path(name): Path<String>) -> Resp {
    match store::get_profile(&st.db, &name).await {
        Ok(Some(row)) => match serde_json::from_str::<Value>(&row.data) {
            Ok(v) => (StatusCode::OK, Json(v)),
            Err(e) => internal(e),
        },
        Ok(None) => err(StatusCode::NOT_FOUND, format!("profile {name:?} not found")),
        Err(e) => internal(e),
    }
}

pub async fn create(State(st): State<XState>, Json(body): Json<Value>) -> Resp {
    if body.get("name").and_then(|v| v.as_str()).is_none() {
        return err(StatusCode::BAD_REQUEST, "profile must have a string `name`");
    }
    match store::upsert_profile(&st.db, &body).await {
        Ok(row) => (StatusCode::CREATED, Json(row.summary())),
        Err(e) => internal(e),
    }
}

pub async fn update(
    State(st): State<XState>,
    Path(name): Path<String>,
    Json(mut body): Json<Value>,
) -> Resp {
    // Path name is authoritative.
    if let Value::Object(ref mut m) = body {
        m.insert("name".into(), json!(name));
    }
    match store::upsert_profile(&st.db, &body).await {
        Ok(row) => {
            // Phase 4: push the change to any live sessions using this profile.
            crate::live::on_profile_updated(&st, &name).await;
            (StatusCode::OK, Json(row.summary()))
        }
        Err(e) => internal(e),
    }
}

pub async fn delete(State(st): State<XState>, Path(name): Path<String>) -> Resp {
    match store::delete_profile(&st.db, &name).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true, "name": name }))),
        Ok(false) => err(StatusCode::NOT_FOUND, format!("profile {name:?} not found")),
        Err(e) => internal(e),
    }
}

pub async fn history(State(st): State<XState>, Path(name): Path<String>) -> Resp {
    match store::profile_history(&st.db, &name).await {
        Ok(h) => (StatusCode::OK, Json(json!({ "name": name, "history": h }))),
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct MergeBody {
    pub profiles: Vec<String>,
    #[serde(default)]
    pub project: Option<String>,
}

/// Resolve profile names → fetched profiles. Returns an error response if any
/// named profile is missing.
async fn resolve(st: &XState, names: &[String]) -> Result<Vec<pok_proto::Profile>, Resp> {
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        match store::get_profile(&st.db, n).await.map_err(internal)? {
            Some(row) => {
                let p = pok_proto::Profile::from_json(
                    &serde_json::from_str(&row.data).map_err(internal)?,
                )
                .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e))?;
                out.push(p);
            }
            None => return Err(err(StatusCode::NOT_FOUND, format!("profile {n:?} not found"))),
        }
    }
    Ok(out)
}

/// Combine request profiles with project + global defaults (§4.3 step 2),
/// dedup preserving order.
pub fn resolve_names(st: &XState, requested: &[String], project: Option<&str>) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let push = |n: &str, names: &mut Vec<String>| {
        if !names.iter().any(|x| x == n) {
            names.push(n.to_string());
        }
    };
    for n in &st.config.default_profiles {
        push(n, &mut names);
    }
    if let Some(proj) = project {
        if let Some(pd) = st.config.project_defaults.get(proj) {
            for n in &pd.default_profiles {
                push(n, &mut names);
            }
        }
    }
    for n in requested {
        push(n, &mut names);
    }
    names
}

pub async fn merge_endpoint(State(st): State<XState>, Json(body): Json<MergeBody>) -> Resp {
    let names = resolve_names(&st, &body.profiles, body.project.as_deref());
    let profiles = match resolve(&st, &names).await {
        Ok(p) => p,
        Err(e) => return e,
    };
    let merged = merge::merge(&profiles);
    match serde_json::to_value(&merged) {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => internal(e),
    }
}

pub async fn preview(State(st): State<XState>, Json(body): Json<MergeBody>) -> Resp {
    let names = resolve_names(&st, &body.profiles, body.project.as_deref());
    let profiles = match resolve(&st, &names).await {
        Ok(p) => p,
        Err(e) => return e,
    };
    let merged = merge::merge(&profiles);
    let capabilities = json!({
        "agents": merged.agents.keys().collect::<Vec<_>>(),
        "skills": merged.skills.keys().collect::<Vec<_>>(),
        "mcp_servers": merged.mcp_servers.keys().collect::<Vec<_>>(),
    });
    (
        StatusCode::OK,
        Json(json!({
            "profiles_resolved": names,
            "merged": merged,
            "capabilities": capabilities,
        })),
    )
}
