//! Discover currently-running Claude Code instances by scanning /proc, and join
//! them against the gateway.projects allowlist from the config.
//!
//! v1 mechanism: walk /proc/<pid>/comm; whenever it matches "claude" (or
//! "claude-code"), read /proc/<pid>/cwd and /proc/<pid>/stat for the parent pid.
//! Optional zellij pane info is best-effort — populated when we can match the
//! process to a pane via the (richer) t44k/zellij MCP, otherwise left empty.
//! M10.7 / a future upgrade plugs in the fork's MCP client for exact tab+pane ids.

use anyhow::Result;
use serde::Serialize;
use std::path::{Path, PathBuf};

use crate::config::{self, Effective, ProjectEntry};

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredProject {
    /// Allowlist slug (matches `gateway.projects[].slug` in po-k.yaml).
    pub slug: String,
    pub cwd: PathBuf,
    pub cc_pid: u32,
    /// Parent shell pid, if we can read /proc/<cc_pid>/stat. Useful when the
    /// zellij side wants to find the hosting pane by PPID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_pid: Option<u32>,
    /// Best-effort zellij metadata. None when we couldn't determine the pane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zellij: Option<ZellijPane>,
    /// True if /proc/<cc_pid> is still alive when we read it.
    pub live: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ZellijPane {
    pub session: String,
    pub tab_index: Option<u32>,
    pub pane_id: Option<u32>,
}

pub fn discover(cfg: &Effective) -> Result<Vec<DiscoveredProject>> {
    let allowlist = &cfg.gateway.projects;
    if allowlist.is_empty() {
        return Ok(Vec::new());
    }
    let cc_processes = scan_claude_processes();
    let mut out = Vec::new();
    for proc_info in cc_processes {
        let Some(entry) = match_allowlist(&proc_info.cwd, allowlist) else {
            continue;
        };
        let live = std::path::Path::new(&format!("/proc/{}", proc_info.pid)).exists();
        out.push(DiscoveredProject {
            slug: entry.slug.clone(),
            cwd: proc_info.cwd,
            cc_pid: proc_info.pid,
            parent_pid: proc_info.parent_pid,
            zellij: None, // populated by the zellij module when a pane match is known
            live,
        });
    }
    Ok(out)
}

#[derive(Debug)]
struct CcProcess {
    pid: u32,
    cwd: PathBuf,
    parent_pid: Option<u32>,
}

fn scan_claude_processes() -> Vec<CcProcess> {
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let name = entry.file_name();
        let Some(pid_str) = name.to_str() else { continue };
        let Ok(pid) = pid_str.parse::<u32>() else { continue };
        if !is_claude(pid) {
            continue;
        }
        let cwd = match std::fs::read_link(format!("/proc/{pid}/cwd")) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let parent_pid = read_ppid(pid);
        out.push(CcProcess { pid, cwd, parent_pid });
    }
    out
}

fn is_claude(pid: u32) -> bool {
    // /proc/<pid>/comm is a single line — the executable basename, truncated.
    let comm = std::fs::read_to_string(format!("/proc/{pid}/comm")).unwrap_or_default();
    let comm = comm.trim();
    comm == "claude" || comm == "claude-code"
}

fn read_ppid(pid: u32) -> Option<u32> {
    // /proc/<pid>/stat: pid (comm) state ppid ...
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The comm field may contain spaces / parens; split on the LAST ')' to be safe.
    let after_comm = s.rsplit_once(')')?.1;
    let mut fields = after_comm.split_whitespace();
    // After splitting we have: state ppid ...
    let _state = fields.next()?;
    let ppid = fields.next()?;
    ppid.parse().ok()
}

fn match_allowlist<'a>(cwd: &Path, allowlist: &'a [ProjectEntry]) -> Option<&'a ProjectEntry> {
    // Longest exact-prefix wins.
    let cwd_s = cwd.to_string_lossy();
    let mut best: Option<(&ProjectEntry, usize)> = None;
    for entry in allowlist {
        let candidates: Vec<String> = std::iter::empty()
            .chain(entry.cwd.as_ref().map(|p| {
                config::expand_path(p).to_string_lossy().into_owned()
            }))
            .chain(entry.cwd_glob.as_ref().map(|s| {
                // Expand ~ in glob patterns too.
                config::expand_path(Path::new(s.as_str())).to_string_lossy().into_owned()
            }))
            .collect();
        for cand in candidates {
            if matches_pattern(&cand, &cwd_s) {
                let score = cand.len();
                if best.map(|(_, l)| score > l).unwrap_or(true) {
                    best = Some((entry, score));
                }
            }
        }
    }
    best.map(|(e, _)| e)
}

/// `pattern` is either an exact prefix or a prefix with a single trailing `*`.
/// Matches the same semantics as the project-routing config doc in po-k.yaml.
fn matches_pattern(pattern: &str, cwd: &str) -> bool {
    if let Some(stripped) = pattern.strip_suffix('*') {
        cwd.starts_with(stripped)
    } else {
        cwd == pattern || cwd.starts_with(&format!("{}/", pattern))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact_and_subpath() {
        assert!(matches_pattern("/workspace", "/workspace"));
        assert!(matches_pattern("/workspace", "/workspace/sub"));
        assert!(!matches_pattern("/workspace", "/workspaceother"));
    }

    #[test]
    fn matches_wildcard() {
        assert!(matches_pattern("/home/me/work/po-k*", "/home/me/work/po-k"));
        assert!(matches_pattern("/home/me/work/po-k*", "/home/me/work/po-k-2"));
        assert!(!matches_pattern("/home/me/work/po-k*", "/home/me/work/other"));
    }
}
