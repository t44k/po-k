//! Dense text embeddings and the background indexer that fills `events_embedding`.
//!
//! Embedder is intentionally tiny so a remote (Voyage / OpenAI / Jina) impl is a
//! drop-in replacement. The default `FastembedEmbedder` uses the
//! `BAAI/bge-small-en-v1.5` 384-dim model — a good speed/quality trade-off for
//! self-hosters. The model is downloaded on first instantiation via hf-hub and
//! cached under ~/.cache/fastembed (or wherever hf-hub points by default).

use anyhow::{Context, Result};
use async_trait::async_trait;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::distill;

#[async_trait]
pub trait Embedder: Send + Sync {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
    fn dim(&self) -> usize;
    fn model_label(&self) -> &str;
}

pub struct FastembedEmbedder {
    inner: Arc<Mutex<TextEmbedding>>,
    model_label: String,
    dim: usize,
}

impl FastembedEmbedder {
    /// Block-on-current-runtime model load. Returns an error if the network fetch fails;
    /// the caller should log and continue (BM25-only mode).
    pub async fn try_load() -> Result<Self> {
        // try_new is sync and blocking (downloads the model). Run on the blocking pool
        // so we don't stall the tokio runtime.
        let handle = tokio::task::spawn_blocking(|| {
            TextEmbedding::try_new(
                InitOptions::new(EmbeddingModel::BGESmallENV15).with_show_download_progress(true),
            )
        })
        .await
        .context("blocking task panicked while loading fastembed model")?;
        let model = handle.context("fastembed model load failed")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(model)),
            model_label: "fastembed/bge-small-en-v1.5".to_string(),
            dim: 384,
        })
    }
}

#[async_trait]
impl Embedder for FastembedEmbedder {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let inner = self.inner.clone();
        let out = tokio::task::spawn_blocking(move || {
            let model = inner.blocking_lock();
            model.embed(texts, None)
        })
        .await
        .context("embed task panicked")?
        .context("fastembed embed failed")?;
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_label(&self) -> &str {
        &self.model_label
    }
}

pub fn encode_vec(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

pub fn decode_vec(bytes: &[u8]) -> Vec<f32> {
    if bytes.len() % 4 != 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(bytes.len() / 4);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    out
}

/// Cosine similarity. Assumes neither side is zero. Returns NaN if one is.
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

// ─── background indexer ───────────────────────────────────────────────────────

/// Long-running task: in a loop, finds events with no embedding row and embeds them
/// in batches. Sleeps when the queue is empty. Drops gracefully on cancel.
pub async fn run_indexer(pool: SqlitePool, embedder: Arc<dyn Embedder>) {
    const BATCH: i64 = 64;
    let model_label = embedder.model_label().to_string();
    info!(model = %model_label, dim = embedder.dim(), "embedding indexer started");
    loop {
        match index_one_batch(&pool, embedder.as_ref(), BATCH).await {
            Ok(0) => {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Ok(n) => {
                tracing::debug!(batch = n, "indexed batch");
            }
            Err(e) => {
                warn!(error = %e, "indexer batch failed; backing off");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

async fn index_one_batch(
    pool: &SqlitePool,
    embedder: &dyn Embedder,
    batch: i64,
) -> Result<usize> {
    // Pull a batch of un-embedded events. Skip noisy event kinds.
    let rows = sqlx::query(
        "SELECT e.session_key, e.file_relpath, e.line_no, e.team_id, e.user_id, e.project_id, e.kind,
                CAST(e.raw AS TEXT) AS raw
         FROM events e
         LEFT JOIN events_embedding ev USING (session_key, file_relpath, line_no)
         WHERE ev.session_key IS NULL
           AND e.kind NOT IN ('file-history-snapshot', 'attachment', 'queue-operation', 'agent-name', 'permission-mode', 'last-prompt')
         ORDER BY e.session_key, e.line_no
         LIMIT ?",
    )
    .bind(batch)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    // Build inputs by extracting searchable text from each event's raw line.
    let mut keys: Vec<(String, String, i64, String, String, Option<String>)> =
        Vec::with_capacity(rows.len());
    let mut inputs: Vec<String> = Vec::with_capacity(rows.len());
    for r in &rows {
        let session_key: String = r.try_get("session_key").unwrap_or_default();
        let file_relpath: String = r.try_get("file_relpath").unwrap_or_default();
        let line_no: i64 = r.try_get("line_no").unwrap_or(0);
        let team_id: String = r.try_get("team_id").unwrap_or_default();
        let user_id: String = r.try_get("user_id").unwrap_or_default();
        let project_id: Option<String> = r.try_get("project_id").ok();
        let raw: String = r.try_get("raw").unwrap_or_default();
        let extracted = distill::extract_searchable(&raw);
        // Embedders choke on empty inputs; replace with a sentinel.
        let text = if extracted.trim().is_empty() {
            "(empty)".to_string()
        } else {
            extracted
        };
        keys.push((session_key, file_relpath, line_no, team_id, user_id, project_id));
        inputs.push(text);
    }

    let vectors = embedder.embed(inputs).await?;
    if vectors.len() != keys.len() {
        anyhow::bail!(
            "embedder returned {} vectors for {} inputs",
            vectors.len(),
            keys.len()
        );
    }

    let mut tx = pool.begin().await?;
    for ((session_key, file_relpath, line_no, team_id, user_id, project_id), vec) in
        keys.iter().zip(vectors.iter())
    {
        let blob = encode_vec(vec);
        sqlx::query(
            "INSERT OR REPLACE INTO events_embedding
             (session_key, file_relpath, line_no, team_id, user_id, project_id, vec, model)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(session_key)
        .bind(file_relpath)
        .bind(line_no)
        .bind(team_id)
        .bind(user_id)
        .bind(project_id.as_deref())
        .bind(&blob)
        .bind(embedder.model_label())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(keys.len())
}
