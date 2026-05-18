//! API-key auth: hash-based lookup of `X-Api-Key` against the users + teams graph.
//!
//! Keys are minted via the admin CLI (or the admin UI) and printed once. We store
//! `blake3(plaintext)` (256-bit hex) in `api_keys.key_hash`; never the plaintext.
//! Each key is bound to a single user, who belongs to a team and carries a role
//! (`admin` | `member`).

use sqlx::{Row, SqlitePool};

/// Hash a plaintext API key to the form we store / look up by.
pub fn hash_api_key(plaintext: &str) -> String {
    blake3::hash(plaintext.as_bytes()).to_hex().to_string()
}

/// Resolved auth context for an authenticated request.
#[derive(Debug, Clone)]
pub struct AuthCtx {
    pub team_id: String,
    pub user_id: String,
    pub role: Role,
    pub user_slug: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
    Member,
}

impl Role {
    pub fn from_db(s: &str) -> Self {
        match s {
            "admin" => Role::Admin,
            _ => Role::Member,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Member => "member",
        }
    }
    pub fn is_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

pub async fn lookup(pool: &SqlitePool, presented_key: &str) -> sqlx::Result<Option<AuthCtx>> {
    if presented_key.is_empty() {
        return Ok(None);
    }
    let hash = hash_api_key(presented_key);
    let row = sqlx::query(
        "SELECT u.team_id, u.id AS user_id, u.role, u.slug
         FROM api_keys k
         JOIN users u ON u.id = k.user_id
         WHERE k.key_hash = ?",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;
    // Best-effort last_used_at tick. Ignore errors (it's a UX nicety).
    if row.is_some() {
        let _ = sqlx::query(
            "UPDATE api_keys SET last_used_at = datetime('now') WHERE key_hash = ?",
        )
        .bind(&hash)
        .execute(pool)
        .await;
    }
    Ok(row.map(|r| AuthCtx {
        team_id: r.try_get("team_id").unwrap_or_default(),
        user_id: r.try_get("user_id").unwrap_or_default(),
        role: Role::from_db(r.try_get::<&str, _>("role").unwrap_or("member")),
        user_slug: r.try_get("slug").unwrap_or_default(),
    }))
}
