//! Xpo-k HTTP router. Public `/health`; everything else behind bearer auth.
//! Profile management is served locally; session/project endpoints are routed
//! to the owning po-k over WebSocket (see [`crate::routed`]).

use axum::http::{Method, StatusCode, Uri};
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::auth::require_bearer;
use crate::state::XState;

pub mod health;
pub mod profiles;

pub fn router(state: XState) -> Router {
    let protected = Router::new()
        .route("/registry", get(health::registry))
        .route("/clients", get(health::clients))
        .route("/profiles", get(profiles::list).post(profiles::create))
        .route(
            "/profiles/{name}",
            get(profiles::get)
                .put(profiles::update)
                .delete(profiles::delete),
        )
        .route("/profiles/{name}/history", get(profiles::history))
        .route("/profiles/merge", post(profiles::merge_endpoint))
        .route("/profiles/preview", post(profiles::preview))
        .merge(crate::routed::router())
        .route_layer(middleware::from_fn_with_state(
            state.token.clone(),
            require_bearer,
        ));

    let public = Router::new()
        .route("/health", get(health::health))
        .route("/help", get(health::help));

    public
        .merge(protected)
        .fallback(not_found)
        .with_state(state)
}

async fn not_found(method: Method, uri: Uri) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("no route for {method} {}", uri.path()) })),
    )
}
