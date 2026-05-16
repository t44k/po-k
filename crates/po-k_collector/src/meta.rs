//! Discover and parse subagent `agent-*.meta.json` sidecars.
//!
//! Meta files don't grow, so we don't watermark them — just re-ship on every scan and
//! let the server upsert. Cost is one tiny row per subagent per scan, dwarfed by the
//! event stream.

use anyhow::Result;
use po_k_core::{MachineId, SessionKey};
use po_k_proto::SubagentMetaRow;
use serde::Deserialize;
use std::path::Path;
use tracing::warn;
use walkdir::WalkDir;

use crate::scan::identify_session;

#[derive(Debug, Deserialize)]
struct MetaFile {
    #[serde(rename = "agentType")]
    agent_type: Option<String>,
    description: Option<String>,
}

pub fn discover_meta_files(root: &Path) -> Vec<std::path::PathBuf> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("agent-") && n.ends_with(".meta.json"))
                .unwrap_or(false)
        })
        .collect()
}

pub async fn read_meta_rows(
    root: &Path,
    machine_id: &MachineId,
) -> Result<Vec<SubagentMetaRow>> {
    let mut out = Vec::new();
    for path in discover_meta_files(root) {
        // The transcript file lives next to the meta with .jsonl extension:
        // agent-<id>.meta.json → agent-<id>.jsonl
        let Some(transcript_path) = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|n| n.trim_end_matches(".meta.json").to_string() + ".jsonl")
        else {
            continue;
        };
        let Some(parent) = path.parent() else { continue };
        let transcript_abs = parent.join(&transcript_path);
        let Ok(transcript_rel) = transcript_abs.strip_prefix(root) else { continue };
        let transcript_rel = transcript_rel.to_string_lossy().to_string();

        let Some((sanitized_cwd, session_uuid)) = identify_session(root, &transcript_abs) else {
            warn!(file = %path.display(), "cannot identify session for meta sidecar");
            continue;
        };
        let session_key = SessionKey::derive(machine_id, &sanitized_cwd, &session_uuid);

        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) => {
                warn!(file = %path.display(), error = %e, "meta read failed");
                continue;
            }
        };
        let meta: MetaFile = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!(file = %path.display(), error = %e, "meta parse failed");
                continue;
            }
        };

        out.push(SubagentMetaRow {
            session_key: session_key.as_str().to_string(),
            agent_file: transcript_rel,
            agent_type: meta.agent_type,
            description: meta.description,
        });
    }
    Ok(out)
}
