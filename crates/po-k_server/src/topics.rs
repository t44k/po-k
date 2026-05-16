//! Topics CRUD and read helpers shared between admin CLI, distillation, and MCP.

use anyhow::Result;
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::path::PathBuf;

use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct Topic {
    pub id: String,
    pub team_id: String,
    pub scope: String,
    pub question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt_extras: Option<String>,
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

pub async fn add(pool: &SqlitePool, t: &Topic) -> Result<()> {
    sqlx::query("INSERT OR IGNORE INTO teams (id, label) VALUES (?, ?)")
        .bind(&t.team_id)
        .bind(&t.team_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO topics (id, team_id, scope, question, system_prompt_extras)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
            team_id = excluded.team_id,
            scope = excluded.scope,
            question = excluded.question,
            system_prompt_extras = excluded.system_prompt_extras,
            updated_at = datetime('now')",
    )
    .bind(&t.id)
    .bind(&t.team_id)
    .bind(&t.scope)
    .bind(&t.question)
    .bind(t.system_prompt_extras.as_deref())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list(pool: &SqlitePool, team: Option<&str>) -> Result<Vec<Topic>> {
    let rows = match team {
        Some(t) => sqlx::query(
            "SELECT id, team_id, scope, question, system_prompt_extras
             FROM topics WHERE team_id = ? ORDER BY id",
        )
        .bind(t)
        .fetch_all(pool)
        .await?,
        None => sqlx::query(
            "SELECT id, team_id, scope, question, system_prompt_extras
             FROM topics ORDER BY team_id, id",
        )
        .fetch_all(pool)
        .await?,
    };
    Ok(rows
        .into_iter()
        .map(|r| Topic {
            id: r.try_get("id").unwrap_or_default(),
            team_id: r.try_get("team_id").unwrap_or_default(),
            scope: r.try_get("scope").unwrap_or_default(),
            question: r.try_get("question").unwrap_or_default(),
            system_prompt_extras: r.try_get("system_prompt_extras").ok(),
        })
        .collect())
}

pub async fn get(pool: &SqlitePool, id: &str) -> Result<Option<Topic>> {
    let row = sqlx::query(
        "SELECT id, team_id, scope, question, system_prompt_extras
         FROM topics WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| Topic {
        id: r.try_get("id").unwrap_or_default(),
        team_id: r.try_get("team_id").unwrap_or_default(),
        scope: r.try_get("scope").unwrap_or_default(),
        question: r.try_get("question").unwrap_or_default(),
        system_prompt_extras: r.try_get("system_prompt_extras").ok(),
    }))
}

pub async fn list_with_digests(
    pool: &SqlitePool,
    team: &str,
) -> Result<Vec<TopicWithDigest>> {
    let rows = sqlx::query(
        "SELECT t.id, t.team_id, t.scope, t.question, t.system_prompt_extras,
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
            topic: Topic {
                id: r.try_get("id").unwrap_or_default(),
                team_id: r.try_get("team_id").unwrap_or_default(),
                scope: r.try_get("scope").unwrap_or_default(),
                question: r.try_get("question").unwrap_or_default(),
                system_prompt_extras: r.try_get("system_prompt_extras").ok(),
            },
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
        "SELECT t.id, t.team_id, t.scope, t.question, t.system_prompt_extras,
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
    Ok(row.map(|r| TopicWithDigest {
        topic: Topic {
            id: r.try_get("id").unwrap_or_default(),
            team_id: r.try_get("team_id").unwrap_or_default(),
            scope: r.try_get("scope").unwrap_or_default(),
            question: r.try_get("question").unwrap_or_default(),
            system_prompt_extras: r.try_get("system_prompt_extras").ok(),
        },
        version: r.try_get("version").unwrap_or(0),
        digest_markdown: r.try_get("digest_markdown").unwrap_or_default(),
        evidence_event_ids: r.try_get("evidence_event_ids").unwrap_or_default(),
        written_at: r.try_get("written_at").ok(),
        llm_backend: r.try_get("llm_backend").unwrap_or_default(),
        llm_model: r.try_get("llm_model").unwrap_or_default(),
    }))
}

// ─── Admin CLI handlers ───────────────────────────────────────────────────────

pub async fn admin_add(
    db: PathBuf,
    id: String,
    question: String,
    scope: String,
    team: String,
    extras: Option<String>,
) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    add(
        state.pool(),
        &Topic {
            id: id.clone(),
            team_id: team,
            scope,
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
        "{:<24}{:<10}{:<28}{:<8}{}",
        "id", "ver", "scope", "team", "question"
    );
    for t in topics {
        let v = if t.version == 0 {
            "—".to_string()
        } else {
            t.version.to_string()
        };
        println!(
            "{:<24}{:<10}{:<28}{:<8}{}",
            t.topic.id, v, t.topic.scope, t.topic.team_id, t.topic.question
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
