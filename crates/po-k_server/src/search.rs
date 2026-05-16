//! BM25 search over events via fts5. The dense (sqlite-vec) half lands in M4.3/M4.4 and
//! will fuse with this via RRF; the public Hit type is shared so the upgrade is in place.

use crate::embed::{self, Embedder};
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub session_key: String,
    pub sanitized_cwd: String,
    pub session_uuid: String,
    pub file_relpath: String,
    pub line_no: i64,
    pub snippet: String,
    pub bm25: Option<f64>,
    pub dense: Option<f64>,
    pub team_id: String,
    /// Set when hybrid retrieval is used; tells the renderer how this hit was found.
    /// `bm25` / `dense` / `both`.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub source: String,
}

/// Build the fts5 MATCH expression from a user query. We escape any double quotes so
/// arbitrary text doesn't accidentally invoke fts5 operators, then wrap each token as a
/// phrase. Empty query returns no hits at the SQL layer.
fn build_match(q: &str) -> String {
    q.split_whitespace()
        .map(|tok| {
            let esc = tok.replace('"', "\"\"");
            format!("\"{esc}\"")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Like `build_match` but joins content words with OR — used when we want recall
/// across long natural-language inputs (e.g. distillation topic questions) where
/// requiring every word to appear in one event is too strict.
fn build_or_match(q: &str) -> String {
    q.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .filter(|w| w.len() >= 4 && !is_stopword(w))
        .map(|w| {
            let lower = w.to_lowercase();
            format!("\"{}\"", lower.replace('"', "\"\""))
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// English stopwords we don't bother indexing as topic terms.
fn is_stopword(w: &str) -> bool {
    matches!(
        w.to_lowercase().as_str(),
        "what" | "which" | "where" | "when" | "have" | "been" | "from"
            | "with" | "this" | "that" | "these" | "those" | "into" | "about"
            | "your" | "yours" | "ours" | "their" | "them" | "they" | "were"
            | "will" | "would" | "could" | "should" | "must" | "must've"
            | "does" | "doing" | "done" | "very" | "much" | "more" | "most"
            | "some" | "any" | "such" | "also" | "than" | "then" | "just"
            | "only" | "even" | "still" | "back" | "down" | "over" | "under"
            | "above" | "below" | "after" | "before" | "between"
    )
}

pub async fn bm25(
    pool: &SqlitePool,
    query: &str,
    team_filter: Option<&str>,
    limit: i64,
) -> sqlx::Result<Vec<Hit>> {
    bm25_with_mode(pool, query, team_filter, limit, MatchMode::And).await
}

/// Variant tuned for long natural-language queries (e.g. topic questions): joins
/// content words with OR so events that mention only a subset still surface.
pub async fn bm25_or(
    pool: &SqlitePool,
    query: &str,
    team_filter: Option<&str>,
    limit: i64,
) -> sqlx::Result<Vec<Hit>> {
    bm25_with_mode(pool, query, team_filter, limit, MatchMode::Or).await
}

#[derive(Copy, Clone)]
enum MatchMode {
    And,
    Or,
}

async fn bm25_with_mode(
    pool: &SqlitePool,
    query: &str,
    team_filter: Option<&str>,
    limit: i64,
    mode: MatchMode,
) -> sqlx::Result<Vec<Hit>> {
    let match_expr = match mode {
        MatchMode::And => build_match(query),
        MatchMode::Or => build_or_match(query),
    };
    if match_expr.is_empty() {
        return Ok(Vec::new());
    }
    let sql = "
        SELECT
            f.session_key,
            f.file_relpath,
            f.line_no,
            f.team_id,
            s.sanitized_cwd,
            s.session_uuid,
            snippet(events_fts, 4, '<mark>', '</mark>', '…', 12) AS snippet,
            bm25(events_fts) AS score
        FROM events_fts f
        JOIN sessions s USING (session_key)
        WHERE events_fts MATCH ?
          AND (? IS NULL OR f.team_id = ?)
        ORDER BY score
        LIMIT ?";
    let rows = sqlx::query(sql)
        .bind(&match_expr)
        .bind(team_filter)
        .bind(team_filter)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| Hit {
            session_key: r.try_get("session_key").unwrap_or_default(),
            sanitized_cwd: r.try_get("sanitized_cwd").unwrap_or_default(),
            session_uuid: r.try_get("session_uuid").unwrap_or_default(),
            file_relpath: r.try_get("file_relpath").unwrap_or_default(),
            line_no: r.try_get("line_no").unwrap_or(0),
            snippet: r.try_get("snippet").unwrap_or_default(),
            bm25: r.try_get("score").ok(),
            dense: None,
            team_id: r.try_get("team_id").unwrap_or_default(),
            source: String::new(),
        })
        .collect())
}

/// Brute-force dense top-K via cosine. Loads every embedding for the team and scores
/// each. Cheap below 100k events; introduce a vector index when corpora grow past that.
pub async fn dense_topk(
    pool: &SqlitePool,
    embedder: &dyn Embedder,
    query: &str,
    team_filter: Option<&str>,
    k: i64,
) -> anyhow::Result<Vec<Hit>> {
    if query.trim().is_empty() {
        return Ok(Vec::new());
    }
    let qvecs = embedder.embed(vec![query.to_string()]).await?;
    let Some(qvec) = qvecs.into_iter().next() else {
        return Ok(Vec::new());
    };

    let rows = match team_filter {
        Some(team) => sqlx::query(
            "SELECT e.session_key, e.file_relpath, e.line_no, e.team_id,
                    e.vec, s.sanitized_cwd, s.session_uuid
             FROM events_embedding e
             JOIN sessions s USING (session_key)
             WHERE e.team_id = ?",
        )
        .bind(team)
        .fetch_all(pool)
        .await?,
        None => sqlx::query(
            "SELECT e.session_key, e.file_relpath, e.line_no, e.team_id,
                    e.vec, s.sanitized_cwd, s.session_uuid
             FROM events_embedding e
             JOIN sessions s USING (session_key)",
        )
        .fetch_all(pool)
        .await?,
    };

    let mut scored: Vec<(f32, Hit)> = Vec::with_capacity(rows.len());
    for r in rows {
        let blob: Vec<u8> = r.try_get("vec").unwrap_or_default();
        let v = embed::decode_vec(&blob);
        let score = embed::cosine(&qvec, &v);
        scored.push((
            score,
            Hit {
                session_key: r.try_get("session_key").unwrap_or_default(),
                sanitized_cwd: r.try_get("sanitized_cwd").unwrap_or_default(),
                session_uuid: r.try_get("session_uuid").unwrap_or_default(),
                file_relpath: r.try_get("file_relpath").unwrap_or_default(),
                line_no: r.try_get("line_no").unwrap_or(0),
                snippet: String::new(),
                bm25: None,
                dense: Some(score as f64),
                team_id: r.try_get("team_id").unwrap_or_default(),
                source: String::new(),
            },
        ));
    }
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut out: Vec<Hit> = scored.into_iter().take(k as usize).map(|x| x.1).collect();

    // Populate snippets for the dense hits by pulling a small slice of their raw text.
    // Cheap because k is small (default ~50).
    for hit in &mut out {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT CAST(raw AS TEXT) FROM events
             WHERE session_key = ? AND file_relpath = ? AND line_no = ?",
        )
        .bind(&hit.session_key)
        .bind(&hit.file_relpath)
        .bind(hit.line_no)
        .fetch_optional(pool)
        .await?;
        if let Some(text) = raw {
            // Take a short readable excerpt — extract_searchable trims to ~1500.
            let snippet = crate::distill::extract_searchable(&text);
            hit.snippet = truncate(&snippet, 240);
        }
    }
    Ok(out)
}

/// Hybrid retrieval: BM25 (OR mode) and dense, fused with Reciprocal Rank Fusion (k=60).
/// Each list contributes the same `rrf_k`; the final ordering is by sum of (1/(rrf_k + rank)).
/// If `embedder` is None or returns an error, falls back to BM25 alone with `source="bm25"`.
pub async fn hybrid(
    pool: &SqlitePool,
    embedder: Option<&Arc<dyn Embedder>>,
    query: &str,
    team_filter: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<Hit>> {
    const RRF_K: f64 = 60.0;
    const PER_LIST: i64 = 80;

    let bm25_hits = bm25_or(pool, query, team_filter, PER_LIST).await?;

    let dense_hits = match embedder {
        Some(emb) => match dense_topk(pool, emb.as_ref(), query, team_filter, PER_LIST).await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "dense_topk failed; using bm25 only");
                Vec::new()
            }
        },
        None => Vec::new(),
    };

    // Key: (session_key, file_relpath, line_no)
    type Key = (String, String, i64);
    let mut by_key: HashMap<Key, Hit> = HashMap::new();
    let mut score_acc: HashMap<Key, f64> = HashMap::new();

    for (rank, h) in bm25_hits.into_iter().enumerate() {
        let k = (h.session_key.clone(), h.file_relpath.clone(), h.line_no);
        let s = 1.0 / (RRF_K + rank as f64 + 1.0);
        *score_acc.entry(k.clone()).or_insert(0.0) += s;
        by_key
            .entry(k)
            .and_modify(|existing| {
                existing.bm25 = h.bm25;
                if existing.snippet.is_empty() {
                    existing.snippet = h.snippet.clone();
                }
            })
            .or_insert_with(|| {
                let mut hit = h.clone();
                hit.source = "bm25".to_string();
                hit
            });
    }

    for (rank, h) in dense_hits.into_iter().enumerate() {
        let k = (h.session_key.clone(), h.file_relpath.clone(), h.line_no);
        let s = 1.0 / (RRF_K + rank as f64 + 1.0);
        *score_acc.entry(k.clone()).or_insert(0.0) += s;
        by_key
            .entry(k)
            .and_modify(|existing| {
                existing.dense = h.dense;
                if existing.snippet.is_empty() {
                    existing.snippet = h.snippet.clone();
                }
                existing.source = "both".to_string();
            })
            .or_insert_with(|| {
                let mut hit = h.clone();
                hit.source = "dense".to_string();
                hit
            });
    }

    let mut scored: Vec<(f64, Hit)> = by_key
        .into_iter()
        .map(|(k, hit)| (*score_acc.get(&k).unwrap_or(&0.0), hit))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(scored.into_iter().take(limit as usize).map(|x| x.1).collect())
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
