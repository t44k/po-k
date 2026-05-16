//! BM25 search over events via fts5. The dense (sqlite-vec) half lands in M4.3/M4.4 and
//! will fuse with this via RRF; the public Hit type is shared so the upgrade is in place.

use serde::Serialize;
use sqlx::{Row, SqlitePool};

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

pub async fn bm25(
    pool: &SqlitePool,
    query: &str,
    team_filter: Option<&str>,
    limit: i64,
) -> sqlx::Result<Vec<Hit>> {
    let match_expr = build_match(query);
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
        })
        .collect())
}
