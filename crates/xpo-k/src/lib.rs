//! Xpo-k library surface — exposed so integration tests can start the server
//! and drive the HTTP + WebSocket stack in-process.

pub mod auth;
pub mod cmd;
pub mod config;
pub mod http;
pub mod live;
pub mod merge;
pub mod registry;
pub mod routed;
pub mod state;
pub mod store;
pub mod ws;

use anyhow::Result;
use std::net::SocketAddr;

/// Build the combined HTTP + WebSocket app for a given state.
pub fn app(state: state::XState) -> axum::Router {
    http::router(state.clone()).merge(ws::router(state))
}

/// Start a server bound to `addr`, returning the bound local address. Used by
/// tests (and `cmd::serve`).
pub async fn serve_on(state: state::XState, addr: SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
