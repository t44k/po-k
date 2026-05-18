//! First-run bootstrap + admin CLI commands for users and API keys.
//!
//! When the server starts against a DB with zero users we auto-create:
//!   team = "default" (label "default")
//!   user = "admin"   (role "admin", label "Auto-created admin")
//!   one API key for that user (printed to STDERR, one chance to copy)
//!
//! Operators can then log in via /ui/login and mint additional users/keys.

use anyhow::{Context, Result};
use sqlx::{Row, SqlitePool};
use std::path::PathBuf;

use crate::auth;
use crate::state::AppState;

const BOOTSTRAP_TEAM: &str = "default";
const BOOTSTRAP_USER: &str = "admin";

pub async fn ensure_bootstrap(pool: &SqlitePool) -> Result<()> {
    let user_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(pool)
        .await?;
    if user_count > 0 {
        return Ok(());
    }
    tracing::info!("bootstrapping initial admin user (DB has zero users)");

    sqlx::query("INSERT OR IGNORE INTO teams (id, label) VALUES (?, ?)")
        .bind(BOOTSTRAP_TEAM)
        .bind(BOOTSTRAP_TEAM)
        .execute(pool)
        .await?;

    let user_id = new_id("u");
    sqlx::query(
        "INSERT INTO users (id, team_id, slug, label, role) VALUES (?, ?, ?, ?, 'admin')",
    )
    .bind(&user_id)
    .bind(BOOTSTRAP_TEAM)
    .bind(BOOTSTRAP_USER)
    .bind("Auto-created admin")
    .execute(pool)
    .await?;

    let (plaintext, hash) = mint_key_for(&user_id, pool, "bootstrap").await?;
    eprintln!();
    eprintln!("════════════════════════════════════════════════════════════════");
    eprintln!(" po-k bootstrap — first run on this database");
    eprintln!(" team   : {BOOTSTRAP_TEAM}");
    eprintln!(" user   : {BOOTSTRAP_USER}  (role admin)");
    eprintln!(" key    : {plaintext}");
    eprintln!(" label  : bootstrap");
    eprintln!(" Copy that key now — it's shown ONCE.");
    eprintln!(" Hash prefix on file: {}", &hash[..12]);
    eprintln!("════════════════════════════════════════════════════════════════");
    eprintln!();
    Ok(())
}

// ─── admin CLI: users ────────────────────────────────────────────────────────

pub async fn admin_user_add(
    db: PathBuf,
    team: String,
    slug: String,
    role: String,
    label: String,
) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let pool = state.pool();

    sqlx::query("INSERT OR IGNORE INTO teams (id, label) VALUES (?, ?)")
        .bind(&team)
        .bind(&team)
        .execute(pool)
        .await?;

    let user_id = new_id("u");
    sqlx::query(
        "INSERT INTO users (id, team_id, slug, label, role) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&user_id)
    .bind(&team)
    .bind(&slug)
    .bind(&label)
    .bind(&role)
    .execute(pool)
    .await
    .with_context(|| format!("creating user {slug} in team {team}"))?;

    let (plaintext, _hash) = mint_key_for(&user_id, pool, "first-key").await?;
    println!("{plaintext}");
    eprintln!("# user '{slug}' (role {role}) in team '{team}'. Key shown ONCE.");
    Ok(())
}

pub async fn admin_user_list(db: PathBuf, team: Option<String>) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let rows = match team.as_deref() {
        Some(t) => sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT team_id, slug, role, label, created_at
             FROM users WHERE team_id = ? ORDER BY created_at",
        )
        .bind(t)
        .fetch_all(state.pool())
        .await?,
        None => sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT team_id, slug, role, label, created_at
             FROM users ORDER BY team_id, created_at",
        )
        .fetch_all(state.pool())
        .await?,
    };
    if rows.is_empty() {
        println!("(no users)");
    } else {
        println!("{:<14}{:<14}{:<10}{:<24}{}", "team", "slug", "role", "label", "created_at");
        for (team, slug, role, label, created) in rows {
            println!("{:<14}{:<14}{:<10}{:<24}{}", team, slug, role, label, created);
        }
    }
    Ok(())
}

// ─── admin CLI: keys ─────────────────────────────────────────────────────────

pub async fn admin_keygen(
    db: PathBuf,
    team: String,
    user_slug: String,
    label: String,
) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let pool = state.pool();

    let user_id: String = sqlx::query_scalar(
        "SELECT id FROM users WHERE team_id = ? AND slug = ?",
    )
    .bind(&team)
    .bind(&user_slug)
    .fetch_optional(pool)
    .await?
    .with_context(|| format!("no user '{user_slug}' in team '{team}'"))?;

    let label = if label.is_empty() { "device".to_string() } else { label };
    let (plaintext, _hash) = mint_key_for(&user_id, pool, &label).await?;
    println!("{plaintext}");
    eprintln!("# user={user_slug} team={team} label={label}. Key shown ONCE.");
    Ok(())
}

pub async fn admin_list_keys(db: PathBuf, team: Option<String>) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let rows = match team.as_deref() {
        Some(t) => sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT substr(k.key_hash, 1, 12), u.team_id, u.slug, k.label, k.created_at
             FROM api_keys k JOIN users u ON u.id = k.user_id
             WHERE u.team_id = ?
             ORDER BY k.created_at",
        )
        .bind(t)
        .fetch_all(state.pool())
        .await?,
        None => sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT substr(k.key_hash, 1, 12), u.team_id, u.slug, k.label, k.created_at
             FROM api_keys k JOIN users u ON u.id = k.user_id
             ORDER BY u.team_id, k.created_at",
        )
        .fetch_all(state.pool())
        .await?,
    };
    if rows.is_empty() {
        println!("(no keys)");
    } else {
        println!("{:<14}{:<10}{:<14}{:<24}{}", "hash_prefix", "team", "user", "label", "created_at");
        for (h, team, user, label, created) in rows {
            println!("{:<14}{:<10}{:<14}{:<24}{}", h, team, user, label, created);
        }
    }
    Ok(())
}

pub async fn admin_revoke(db: PathBuf, label: String) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let r = sqlx::query("DELETE FROM api_keys WHERE label = ?")
        .bind(&label)
        .execute(state.pool())
        .await?;
    println!("revoked {} key(s) with label '{label}'", r.rows_affected());
    Ok(())
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
}

/// Mint a new API key for `user_id`, store its hash, return (plaintext, hash).
pub async fn mint_key_for(
    user_id: &str,
    pool: &SqlitePool,
    label: &str,
) -> Result<(String, String)> {
    let plaintext = format!("pk_{}", uuid::Uuid::now_v7().simple());
    let hash = auth::hash_api_key(&plaintext);
    sqlx::query(
        "INSERT INTO api_keys (key_hash, user_id, label) VALUES (?, ?, ?)",
    )
    .bind(&hash)
    .bind(user_id)
    .bind(label)
    .execute(pool)
    .await
    .with_context(|| format!("inserting api_key for user {user_id}"))?;
    Ok((plaintext, hash))
}

/// Look up `(team_id, user_id)` by user slug. Used by ingest.rs to optionally
/// override the calling key's user when a backfill collector authenticates as
/// admin but ships another user's logs. Currently unused; kept for symmetry.
#[allow(dead_code)]
pub async fn resolve_user(
    pool: &SqlitePool,
    team: &str,
    slug: &str,
) -> Result<Option<(String, String)>> {
    let row = sqlx::query("SELECT team_id, id FROM users WHERE team_id = ? AND slug = ?")
        .bind(team)
        .bind(slug)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| {
        (
            r.try_get("team_id").unwrap_or_default(),
            r.try_get("id").unwrap_or_default(),
        )
    }))
}
