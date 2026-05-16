//! API-key auth: hash-based lookup of `X-Api-Key`.
//!
//! Keys are minted via `po-k_server admin keygen` and printed once. We store
//! `blake3(plaintext)` (256-bit hex) in `api_keys.key_hash`, never the plaintext.

use sqlx::{Row, SqlitePool};

/// Hash a plaintext API key to the form we store / look up by.
pub fn hash_api_key(plaintext: &str) -> String {
    blake3::hash(plaintext.as_bytes()).to_hex().to_string()
}

/// Resolved auth context for an authenticated request.
#[derive(Debug, Clone)]
pub struct AuthCtx {
    pub team_id: String,
}

pub async fn lookup(pool: &SqlitePool, presented_key: &str) -> sqlx::Result<Option<AuthCtx>> {
    if presented_key.is_empty() {
        return Ok(None);
    }
    let hash = hash_api_key(presented_key);
    let row = sqlx::query("SELECT team_id FROM api_keys WHERE key_hash = ?")
        .bind(&hash)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| AuthCtx {
        team_id: r.try_get("team_id").unwrap_or_default(),
    }))
}
