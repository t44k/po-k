//! Topics CRUD and read helpers shared between admin CLI, distillation, and MCP.

use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::path::PathBuf;

use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct Topic {
    pub id: String,
    pub team_id: String,
    pub scope_kind: String, // 'global' | 'global-project' | 'user' | 'user-project'
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt_extras: Option<String>,
}

impl Topic {
    pub fn validate(&self) -> Result<()> {
        let has_user = self.user_id.is_some();
        let has_project = self.project_id.is_some();
        let ok = match self.scope_kind.as_str() {
            "global" => !has_user && !has_project,
            "global-project" => !has_user && has_project,
            "user" => has_user && !has_project,
            "user-project" => has_user && has_project,
            _ => false,
        };
        if !ok {
            anyhow::bail!(
                "scope_kind '{}' doesn't match (user_id is {:?}, project_id is {:?})",
                self.scope_kind,
                self.user_id,
                self.project_id
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicWithDigest {
    #[serde(flatten)]
    pub topic: Topic,
    pub version: i64,
    pub digest_markdown: String,
    pub evidence_event_ids: String,
    pub written_at: Option<String>,
    pub llm_backend: String,
    pub llm_model: String,
}

const TOPIC_COLUMNS: &str =
    "id, team_id, scope_kind, user_id, project_id, question, system_prompt_extras";

fn row_to_topic(r: &sqlx::sqlite::SqliteRow) -> Topic {
    Topic {
        id: r.try_get("id").unwrap_or_default(),
        team_id: r.try_get("team_id").unwrap_or_default(),
        scope_kind: r.try_get("scope_kind").unwrap_or_default(),
        user_id: r.try_get("user_id").ok(),
        project_id: r.try_get("project_id").ok(),
        question: r.try_get("question").unwrap_or_default(),
        system_prompt_extras: r.try_get("system_prompt_extras").ok(),
    }
}

pub async fn add(pool: &SqlitePool, t: &Topic) -> Result<()> {
    t.validate()?;
    sqlx::query(
        "INSERT INTO topics (id, team_id, scope_kind, user_id, project_id, question, system_prompt_extras)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
            team_id = excluded.team_id,
            scope_kind = excluded.scope_kind,
            user_id = excluded.user_id,
            project_id = excluded.project_id,
            question = excluded.question,
            system_prompt_extras = excluded.system_prompt_extras,
            updated_at = datetime('now')",
    )
    .bind(&t.id)
    .bind(&t.team_id)
    .bind(&t.scope_kind)
    .bind(t.user_id.as_deref())
    .bind(t.project_id.as_deref())
    .bind(&t.question)
    .bind(t.system_prompt_extras.as_deref())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list(pool: &SqlitePool, team: Option<&str>) -> Result<Vec<Topic>> {
    let sql = format!(
        "SELECT {TOPIC_COLUMNS} FROM topics {} ORDER BY team_id, id",
        if team.is_some() { "WHERE team_id = ?" } else { "" }
    );
    let mut q = sqlx::query(&sql);
    if let Some(t) = team {
        q = q.bind(t);
    }
    let rows = q.fetch_all(pool).await?;
    Ok(rows.iter().map(row_to_topic).collect())
}

pub async fn get(pool: &SqlitePool, id: &str) -> Result<Option<Topic>> {
    let row = sqlx::query(&format!("SELECT {TOPIC_COLUMNS} FROM topics WHERE id = ?"))
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row.as_ref().map(row_to_topic))
}

pub async fn list_with_digests(
    pool: &SqlitePool,
    team: &str,
) -> Result<Vec<TopicWithDigest>> {
    let rows = sqlx::query(
        "SELECT t.id, t.team_id, t.scope_kind, t.user_id, t.project_id,
                t.question, t.system_prompt_extras,
                COALESCE(d.version, 0) AS version,
                COALESCE(d.digest_markdown, '') AS digest_markdown,
                COALESCE(d.evidence_event_ids, '[]') AS evidence_event_ids,
                d.written_at AS written_at,
                COALESCE(d.llm_backend, '') AS llm_backend,
                COALESCE(d.llm_model, '') AS llm_model
         FROM topics t
         LEFT JOIN digests d ON d.topic_id = t.id
         WHERE t.team_id = ?
         ORDER BY t.id",
    )
    .bind(team)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| TopicWithDigest {
            topic: row_to_topic(&r),
            version: r.try_get("version").unwrap_or(0),
            digest_markdown: r.try_get("digest_markdown").unwrap_or_default(),
            evidence_event_ids: r.try_get("evidence_event_ids").unwrap_or_default(),
            written_at: r.try_get("written_at").ok(),
            llm_backend: r.try_get("llm_backend").unwrap_or_default(),
            llm_model: r.try_get("llm_model").unwrap_or_default(),
        })
        .collect())
}

pub async fn get_with_digest(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<TopicWithDigest>> {
    let row = sqlx::query(
        "SELECT t.id, t.team_id, t.scope_kind, t.user_id, t.project_id,
                t.question, t.system_prompt_extras,
                COALESCE(d.version, 0) AS version,
                COALESCE(d.digest_markdown, '') AS digest_markdown,
                COALESCE(d.evidence_event_ids, '[]') AS evidence_event_ids,
                d.written_at AS written_at,
                COALESCE(d.llm_backend, '') AS llm_backend,
                COALESCE(d.llm_model, '') AS llm_model
         FROM topics t
         LEFT JOIN digests d ON d.topic_id = t.id
         WHERE t.id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.as_ref().map(|r| TopicWithDigest {
        topic: row_to_topic(r),
        version: r.try_get("version").unwrap_or(0),
        digest_markdown: r.try_get("digest_markdown").unwrap_or_default(),
        evidence_event_ids: r.try_get("evidence_event_ids").unwrap_or_default(),
        written_at: r.try_get("written_at").ok(),
        llm_backend: r.try_get("llm_backend").unwrap_or_default(),
        llm_model: r.try_get("llm_model").unwrap_or_default(),
    }))
}

/// Resolve `(user slug, project slug)` to their internal ids, looked up within the team.
async fn resolve_scope_targets(
    pool: &SqlitePool,
    team: &str,
    user_slug: Option<&str>,
    project_slug: Option<&str>,
) -> Result<(Option<String>, Option<String>)> {
    let user_id = match user_slug {
        None => None,
        Some(slug) => Some(
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM users WHERE team_id = ? AND slug = ?",
            )
            .bind(team)
            .bind(slug)
            .fetch_optional(pool)
            .await?
            .with_context(|| format!("no user '{slug}' in team '{team}'"))?,
        ),
    };
    let project_id = match project_slug {
        None => None,
        Some(slug) => Some(
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM projects WHERE team_id = ? AND slug = ?",
            )
            .bind(team)
            .bind(slug)
            .fetch_optional(pool)
            .await?
            .with_context(|| format!("no project '{slug}' in team '{team}'"))?,
        ),
    };
    Ok((user_id, project_id))
}

// ─── Admin CLI handlers ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn admin_add(
    db: PathBuf,
    id: String,
    question: String,
    scope_kind: String,
    team: String,
    user_slug: Option<String>,
    project_slug: Option<String>,
    extras: Option<String>,
) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let (user_id, project_id) = resolve_scope_targets(
        state.pool(),
        &team,
        user_slug.as_deref(),
        project_slug.as_deref(),
    )
    .await?;
    add(
        state.pool(),
        &Topic {
            id: id.clone(),
            team_id: team,
            scope_kind,
            user_id,
            project_id,
            question,
            system_prompt_extras: extras,
        },
    )
    .await?;
    println!("added topic '{id}'");
    Ok(())
}

pub async fn admin_list(db: PathBuf, team: String) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let topics = list_with_digests(state.pool(), &team).await?;
    if topics.is_empty() {
        println!("(no topics in team '{team}')");
        return Ok(());
    }
    println!(
        "{:<24}{:<6}{:<16}{:<12}{:<12}{}",
        "id", "ver", "scope_kind", "user", "project", "question"
    );
    for t in topics {
        let v = if t.version == 0 {
            "—".to_string()
        } else {
            t.version.to_string()
        };
        let user = t.topic.user_id.as_deref().unwrap_or("—");
        let proj = t.topic.project_id.as_deref().unwrap_or("—");
        println!(
            "{:<24}{:<6}{:<16}{:<12}{:<12}{}",
            t.topic.id, v, t.topic.scope_kind, user, proj, t.topic.question
        );
    }
    Ok(())
}

pub async fn admin_remove(db: PathBuf, id: String) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let r = sqlx::query("DELETE FROM topics WHERE id = ?")
        .bind(&id)
        .execute(state.pool())
        .await?;
    println!("removed {} topic(s) with id '{id}'", r.rows_affected());
    Ok(())
}
