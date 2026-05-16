use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use po_k_core::{Event, MachineId, SessionKey};
use po_k_proto::{BatchHeader, BatchKind, HEADER_API_KEY, HEADER_IDEMPOTENCY_KEY, IngestResponse};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{info, warn};
use walkdir::WalkDir;

/// po-k_collector — walks ~/.claude/projects/** and ships events to po-k_server.
///
/// M1 is a one-shot backfill: no watermarking, no notify watcher yet. Re-running is safe
/// because the server dedupes on (session_key, file_relpath, line_no).
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Root to walk. Defaults to ~/.claude/projects.
    #[arg(long, env = "PO_K_PROJECTS_ROOT")]
    projects_root: Option<PathBuf>,

    /// Server base URL (no trailing slash).
    #[arg(long, env = "PO_K_SERVER_URL", default_value = "http://127.0.0.1:8787")]
    server_url: String,

    /// API key sent as `X-Api-Key`.
    #[arg(long, env = "PO_K_API_KEY", default_value = "dev")]
    api_key: String,

    /// Stable machine identifier used in session_key derivation.
    #[arg(long, env = "PO_K_MACHINE_ID", default_value = "dev-machine")]
    machine_id: String,

    /// Maximum events per batch.
    #[arg(long, default_value_t = 256)]
    batch_size: usize,

    /// Perform a single backfill pass and exit. M1 only supports --once.
    #[arg(long, default_value_t = true)]
    once: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let root = cli
        .projects_root
        .clone()
        .or_else(default_projects_root)
        .context("could not resolve ~/.claude/projects; pass --projects-root")?;

    if !root.exists() {
        anyhow::bail!("projects root {} does not exist", root.display());
    }

    let machine_id = MachineId::from(cli.machine_id.clone());
    let client = reqwest::Client::builder()
        .user_agent(concat!("po-k_collector/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let files = discover_jsonl_files(&root);
    info!(file_count = files.len(), root = %root.display(), "starting backfill");

    let mut total_sent = 0u64;
    let mut total_accepted = 0u64;
    let mut total_dupes = 0u64;

    for path in files {
        let Some((sanitized_cwd, session_uuid)) = identify_session(&root, &path) else {
            warn!(file = %path.display(), "could not derive session identity, skipping");
            continue;
        };
        let session_key = SessionKey::derive(&machine_id, &sanitized_cwd, &session_uuid);
        let relpath = match path.strip_prefix(&root) {
            Ok(r) => r.to_string_lossy().to_string(),
            Err(_) => path.to_string_lossy().to_string(),
        };

        let Ok(file) = tokio::fs::File::open(&path).await else {
            warn!(file = %path.display(), "open failed, skipping");
            continue;
        };
        let mut reader = BufReader::new(file);

        let mut byte_offset: u64 = 0;
        let mut line_no: u64 = 0;
        let mut buf = Vec::with_capacity(8 * 1024);
        let mut batch: Vec<Event> = Vec::with_capacity(cli.batch_size);

        loop {
            buf.clear();
            let read = reader.read_until(b'\n', &mut buf).await?;
            if read == 0 {
                break;
            }
            let line_start = byte_offset;
            byte_offset += read as u64;

            // Strip trailing newline (we only emit complete lines).
            let line_bytes: &[u8] = if buf.ends_with(b"\n") {
                &buf[..buf.len() - 1]
            } else {
                // No trailing newline: incomplete line (file growing while we read).
                // M1 just drops it; the next backfill will catch the now-complete line.
                continue;
            };
            if line_bytes.is_empty() {
                line_no += 1;
                continue;
            }

            match Event::from_jsonl_line(
                line_bytes,
                session_key.clone(),
                relpath.clone(),
                line_start,
                line_no,
            ) {
                Ok(ev) => batch.push(ev),
                Err(e) => warn!(
                    file = %path.display(),
                    line = line_no,
                    error = %e,
                    "failed to parse jsonl line, skipping"
                ),
            }
            line_no += 1;

            if batch.len() >= cli.batch_size {
                let r = ship_batch(&client, &cli, &machine_id, &mut batch).await?;
                total_sent += r.requested;
                total_accepted += r.accepted;
                total_dupes += r.duplicates;
            }
        }

        if !batch.is_empty() {
            let r = ship_batch(&client, &cli, &machine_id, &mut batch).await?;
            total_sent += r.requested;
            total_accepted += r.accepted;
            total_dupes += r.duplicates;
        }
    }

    info!(
        sent = total_sent,
        accepted = total_accepted,
        duplicates = total_dupes,
        "backfill complete"
    );
    Ok(())
}

fn default_projects_root() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    Some(home.join(".claude").join("projects"))
}

/// Returns every `*.jsonl` under `root`, deterministically sorted so backfill ordering
/// is stable across runs (helps debugging).
fn discover_jsonl_files(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|p| p.extension().map(|x| x == "jsonl").unwrap_or(false))
        .collect();
    out.sort();
    out
}

/// Derive (sanitized_cwd, session_uuid) from a path like
/// `~/.claude/projects/-workspace/<uuid>.jsonl`
/// or  `~/.claude/projects/-workspace/<uuid>/subagents/agent-<id>.jsonl`.
fn identify_session(root: &Path, file: &Path) -> Option<(String, String)> {
    let rel = file.strip_prefix(root).ok()?;
    let mut comps = rel.components();
    let sanitized_cwd = comps.next()?.as_os_str().to_string_lossy().to_string();
    // Next component is either `<uuid>.jsonl` (main) or `<uuid>` (dir containing subagents/tool-results).
    let second = comps.next()?.as_os_str().to_string_lossy().to_string();
    let session_uuid = second.strip_suffix(".jsonl").unwrap_or(&second).to_string();
    Some((sanitized_cwd, session_uuid))
}

struct ShipResult {
    requested: u64,
    accepted: u64,
    duplicates: u64,
}

async fn ship_batch(
    client: &reqwest::Client,
    cli: &Cli,
    machine_id: &MachineId,
    batch: &mut Vec<Event>,
) -> Result<ShipResult> {
    let count = batch.len() as u64;
    let batch_id = uuid::Uuid::now_v7().to_string();
    let header = BatchHeader {
        kind: BatchKind::BatchHeader,
        batch_id: batch_id.clone(),
        machine_id: machine_id.clone(),
        sent_at: chrono::Utc::now().to_rfc3339(),
        count,
        team_id: None,
    };

    let mut body = Vec::with_capacity(16 * 1024);
    serde_json::to_writer(&mut body, &header)?;
    body.push(b'\n');
    for ev in batch.drain(..) {
        serde_json::to_writer(&mut body, &ev)?;
        body.push(b'\n');
    }

    let url = format!("{}/ingest", cli.server_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .header(HEADER_API_KEY, &cli.api_key)
        .header(HEADER_IDEMPOTENCY_KEY, &batch_id)
        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("ingest failed: {status}: {body}");
    }

    let parsed: IngestResponse = resp.json().await?;
    match parsed {
        IngestResponse::Ok { accepted, duplicates } => {
            tracing::debug!(batch_id, count, accepted, duplicates, "batch accepted");
            Ok(ShipResult { requested: count, accepted, duplicates })
        }
        IngestResponse::Error { message, rejected_line } => {
            anyhow::bail!("server error: {message} (rejected_line={rejected_line:?})")
        }
    }
}
