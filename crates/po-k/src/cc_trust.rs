//! Pre-trust a session directory for Claude Code.
//!
//! Interactive CC asks "Do you trust the files in this folder?" the first time
//! it starts in a directory and exits when the answer is No. Nobody can answer
//! that dialog in a po-k pane (worse: its `❯` selection arrow looks like the
//! input prompt, so a typed prompt + Enter would pick "No, exit"). CC records
//! the decision in `~/.claude.json` under `projects.<cwd>.hasTrustDialogAccepted`,
//! so po-k records it before launching — consistent with driving CC in
//! `bypassPermissions` mode in a directory the orchestrator chose.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub fn claude_json_path() -> PathBuf {
    crate::config::expand_path("~/.claude.json")
}

/// Mark `cwd` trusted in `file` (creating a minimal file if missing).
/// Returns `true` when the file was changed.
pub fn ensure_trusted_in(file: &Path, cwd: &str) -> Result<bool> {
    let mut root: Value = match std::fs::read_to_string(file) {
        Ok(text) if !text.trim().is_empty() => {
            serde_json::from_str(&text).with_context(|| format!("parsing {}", file.display()))?
        }
        Ok(_) => json!({}),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => json!({}),
        Err(e) => return Err(e).with_context(|| format!("reading {}", file.display())),
    };
    if !root.is_object() {
        anyhow::bail!("{} is not a JSON object", file.display());
    }
    let projects = root
        .as_object_mut()
        .unwrap()
        .entry("projects")
        .or_insert_with(|| json!({}));
    if !projects.is_object() {
        *projects = json!({});
    }
    let entry = projects
        .as_object_mut()
        .unwrap()
        .entry(cwd.to_string())
        .or_insert_with(|| json!({}));
    if !entry.is_object() {
        *entry = json!({});
    }
    let obj = entry.as_object_mut().unwrap();
    if obj.get("hasTrustDialogAccepted").and_then(Value::as_bool) == Some(true) {
        return Ok(false);
    }
    // The shape CC writes itself for a fresh project entry.
    for (k, v) in [
        ("allowedTools", json!([])),
        ("mcpContextUris", json!([])),
        ("mcpServers", json!({})),
        ("enabledMcpjsonServers", json!([])),
        ("disabledMcpjsonServers", json!([])),
        ("hasClaudeMdExternalIncludesApproved", json!(false)),
        ("hasClaudeMdExternalIncludesWarningShown", json!(false)),
    ] {
        obj.entry(k.to_string()).or_insert(v);
    }
    obj.insert("hasTrustDialogAccepted".into(), json!(true));

    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = file.with_extension("json.po-k.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&root).context("serialising ~/.claude.json")?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, file).with_context(|| format!("replacing {}", file.display()))?;
    Ok(true)
}

pub fn ensure_trusted(cwd: &str) -> Result<bool> {
    ensure_trusted_in(&claude_json_path(), cwd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_trust_and_preserves_everything_else() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join(".claude.json");
        std::fs::write(
            &f,
            r#"{"hasCompletedOnboarding": true, "projects": {"/old": {"hasTrustDialogAccepted": false, "lastCost": 1.5}}}"#,
        )
        .unwrap();
        assert!(ensure_trusted_in(&f, "/old").unwrap());
        assert!(ensure_trusted_in(&f, "/new dir").unwrap());
        // Already trusted → untouched.
        assert!(!ensure_trusted_in(&f, "/new dir").unwrap());
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&f).unwrap()).unwrap();
        assert_eq!(v["hasCompletedOnboarding"], true);
        assert_eq!(v["projects"]["/old"]["hasTrustDialogAccepted"], true);
        assert_eq!(v["projects"]["/old"]["lastCost"], 1.5);
        assert_eq!(v["projects"]["/new dir"]["hasTrustDialogAccepted"], true);
        assert_eq!(v["projects"]["/new dir"]["allowedTools"], json!([]));
        assert!(!dir.path().join(".claude.json.po-k.tmp").exists());
    }

    #[test]
    fn creates_a_minimal_file_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("sub").join(".claude.json");
        assert!(ensure_trusted_in(&f, "/w").unwrap());
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&f).unwrap()).unwrap();
        assert_eq!(v["projects"]["/w"]["hasTrustDialogAccepted"], true);
    }
}
