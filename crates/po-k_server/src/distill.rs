//! Distillation loop: for a topic, gather evidence from session events, ask an LLM
//! to refresh the markdown digest, store the new version.
//!
//! v1 is straightforward — BM25 retrieval over events using the topic's question as
//! the query, top-N events, trimmed to a fixed character budget, fed to the LLM
//! alongside the prior digest. Hybrid retrieval (M4.3+) and contradiction detection
//! (deferred per the plan's "stay on the deterministic, demoable path") come later.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::SqlitePool;
use std::path::PathBuf;
use tracing::info;

use crate::llm::{self, Llm};
use crate::search;
use crate::state::AppState;
use crate::topics;

/// Maximum characters of evidence text per LLM call. Conservative: most CC models
/// handle far more, but tighter is faster and keeps the prompt-cache reusable.
const EVIDENCE_BUDGET_CHARS: usize = 80_000;
/// Cap on how many top BM25 hits we even consider.
const EVIDENCE_HIT_LIMIT: i64 = 80;

#[derive(Debug, Serialize)]
struct EvidenceEvent {
    file_relpath: String,
    line_no: i64,
}

pub async fn run_admin(
    db: PathBuf,
    only_id: Option<String>,
    backend: String,
    model: Option<String>,
) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let llm = llm::from_config(&backend, model)?;
    match only_id {
        Some(id) => distill_one(state.pool(), id.as_str(), llm.as_ref()).await?,
        None => {
            let all = topics::list(state.pool(), None).await?;
            for t in all {
                if let Err(e) = distill_one(state.pool(), &t.id, llm.as_ref()).await {
                    eprintln!("topic '{}': {e}", t.id);
                }
            }
        }
    }
    Ok(())
}

pub async fn distill_one(pool: &SqlitePool, topic_id: &str, llm: &dyn Llm) -> Result<()> {
    let topic = topics::get(pool, topic_id)
        .await?
        .with_context(|| format!("no topic with id '{topic_id}'"))?;
    info!(topic = topic_id, "starting distill");

    // BM25 hits scoped to the topic's team. Project scoping is enforced after the
    // BM25 query by filtering on sanitized_cwd — fts5 has no GROUP BY-friendly way
    // to do this in a single query without joining sessions.
    let mut hits = search::bm25_or(pool, &topic.question, Some(&topic.team_id), EVIDENCE_HIT_LIMIT)
        .await
        .context("bm25 retrieval")?;
    if topic.scope.starts_with("project:") {
        let cwd = topic.scope.trim_start_matches("project:");
        hits.retain(|h| h.sanitized_cwd == cwd);
    }
    if hits.is_empty() {
        info!(topic = topic_id, "no evidence found, skipping");
        return Ok(());
    }

    let prior = topics::get_with_digest(pool, topic_id).await?;
    let prior_digest = prior
        .as_ref()
        .map(|t| t.digest_markdown.as_str())
        .unwrap_or("(no prior digest)");
    let prior_version = prior.as_ref().map(|t| t.version).unwrap_or(0);

    // Build evidence section: pull each hit's raw line, label it with its origin,
    // and stop adding once we cross the character budget.
    let mut evidence = String::new();
    let mut used_events: Vec<EvidenceEvent> = Vec::new();
    for hit in &hits {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT CAST(raw AS TEXT) FROM events
             WHERE session_key = ? AND file_relpath = ? AND line_no = ?",
        )
        .bind(&hit.session_key)
        .bind(&hit.file_relpath)
        .bind(hit.line_no)
        .fetch_optional(pool)
        .await?;
        let Some(raw_text) = raw else { continue };
        let extracted = extract_searchable(&raw_text);
        let block = format!(
            "### {} · {} · line {}\n{}\n\n",
            hit.session_uuid, hit.file_relpath, hit.line_no, extracted
        );
        if evidence.len() + block.len() > EVIDENCE_BUDGET_CHARS {
            break;
        }
        evidence.push_str(&block);
        used_events.push(EvidenceEvent {
            file_relpath: hit.file_relpath.clone(),
            line_no: hit.line_no,
        });
    }

    let extras = topic
        .system_prompt_extras
        .as_deref()
        .unwrap_or("")
        .trim();
    let system = format!(
        "You maintain a living markdown digest answering one curated question for a team.\n\
         Read the prior digest and the new evidence below, then output an updated digest \
         (markdown only — no preface, no explanation). Keep it concise (~400-800 words). \
         Cite evidence with short inline references like (session <uuid>, line <n>). If the \
         evidence contradicts the prior digest, prefer the more recent / clearer evidence \
         and call out the change in a 'Recent changes' bullet at the bottom.\n\
         {extras}",
        extras = if extras.is_empty() {
            "".to_string()
        } else {
            format!("\nAdditional guidance: {extras}")
        }
    );

    let user = format!(
        "# Topic\n{question}\n\n# Prior digest (version {prior_version})\n{prior_digest}\n\n# New evidence\n{evidence}",
        question = topic.question,
    );

    let new_digest = llm
        .complete(&system, &user)
        .await
        .context("llm.complete failed")?;

    let version = prior_version + 1;
    sqlx::query(
        "INSERT INTO digests (topic_id, version, digest_markdown, evidence_event_ids, llm_backend, llm_model)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(topic_id) DO UPDATE SET
            version = excluded.version,
            digest_markdown = excluded.digest_markdown,
            evidence_event_ids = excluded.evidence_event_ids,
            llm_backend = excluded.llm_backend,
            llm_model = excluded.llm_model,
            written_at = datetime('now')",
    )
    .bind(topic_id)
    .bind(version)
    .bind(&new_digest)
    .bind(serde_json::to_string(&used_events).unwrap_or_else(|_| "[]".to_string()))
    .bind(llm.backend_label())
    .bind(llm.model_label())
    .execute(pool)
    .await?;

    info!(
        topic = topic_id,
        version,
        evidence_count = used_events.len(),
        chars = new_digest.len(),
        "digest updated"
    );
    Ok(())
}

/// Pull the *interesting* text out of an event's raw JSONL line so the evidence we
/// hand the LLM (or the embedder) is human-readable instead of escaped JSON. For
/// unknown shapes we just hand back a truncated version of the raw line.
pub fn extract_searchable(raw: &str) -> String {
    let Ok(v) = serde_json::from_str::<Value>(raw) else {
        return truncate(raw, 1500);
    };
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    let extracted = match kind {
        "user" => extract_user_text(&v),
        "assistant" => extract_assistant_text(&v),
        "system" => v
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        _ => raw.to_string(),
    };
    if extracted.trim().is_empty() {
        truncate(raw, 1500)
    } else {
        truncate(&extracted, 1500)
    }
}

fn extract_user_text(v: &Value) -> String {
    let Some(content) = v.pointer("/message/content") else {
        return String::new();
    };
    match content {
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut buf = String::new();
            for item in items {
                let t = item.get("type").and_then(Value::as_str).unwrap_or("");
                let text = if t == "text" {
                    item.get("text").and_then(Value::as_str).map(str::to_string)
                } else if t == "tool_result" {
                    let inner = item.get("content");
                    Some(match inner {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Array(arr)) => arr
                            .iter()
                            .filter_map(|x| x.get("text").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join("\n"),
                        _ => String::new(),
                    })
                } else {
                    None
                };
                if let Some(text) = text {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&text);
                }
            }
            buf
        }
        _ => String::new(),
    }
}

fn extract_assistant_text(v: &Value) -> String {
    let Some(items) = v.pointer("/message/content").and_then(Value::as_array) else {
        return String::new();
    };
    let mut buf = String::new();
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                if !buf.is_empty() {
                    buf.push('\n');
                }
                buf.push_str(text);
            }
        }
    }
    buf
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}
