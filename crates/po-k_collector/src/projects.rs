//! Load `~/.config/po-k/projects.toml` and map a Claude Code session's `cwd`
//! to an admin-registered project slug.
//!
//! Example file:
//!
//! ```toml
//! [[project]]
//! id    = "po-k"
//! paths = ["/workspace", "/home/tamas/work/po-k*"]
//!
//! [[project]]
//! id    = "frontend"
//! paths = ["/home/tamas/work/frontend"]
//! ```
//!
//! Patterns are either an exact prefix or a prefix with a single trailing `*`.
//! Anything more elaborate is deliberately out of scope for v1 — we want this
//! file editable by hand without learning a glob dialect.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct ProjectMap {
    entries: Vec<Entry>,
}

#[derive(Debug, Clone)]
struct Entry {
    id: String,
    pattern: String,
    is_wildcard: bool,
}

#[derive(Debug, Deserialize)]
struct File {
    #[serde(default)]
    project: Vec<TomlProject>,
}

#[derive(Debug, Deserialize)]
struct TomlProject {
    id: String,
    #[serde(default)]
    paths: Vec<String>,
}

impl ProjectMap {
    pub fn empty() -> Self {
        Self { entries: Vec::new() }
    }

    /// Load the default `~/.config/po-k/projects.toml`. Missing file → empty map
    /// (logged at info, not an error). Bad TOML → propagated.
    pub fn load_default() -> Result<Self> {
        let Some(path) = default_path() else {
            tracing::info!("no HOME/XDG_CONFIG_HOME; skipping projects.toml");
            return Ok(Self::empty());
        };
        Self::load(&path)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(path = %path.display(), "projects.toml not found; all sessions ship project_id=NULL");
                return Ok(Self::empty());
            }
            Err(e) => return Err(e).context(format!("reading {}", path.display())),
        };
        let text = std::str::from_utf8(&bytes).context("projects.toml is not utf-8")?;
        let parsed: File = toml::from_str(text).context("parsing projects.toml")?;
        let mut entries = Vec::new();
        for p in parsed.project {
            for pat in p.paths {
                let pat = pat.trim().to_string();
                if pat.is_empty() {
                    continue;
                }
                let (pattern, is_wildcard) = if let Some(stripped) = pat.strip_suffix('*') {
                    (stripped.to_string(), true)
                } else {
                    (pat, false)
                };
                entries.push(Entry {
                    id: p.id.clone(),
                    pattern,
                    is_wildcard,
                });
            }
        }
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            "loaded projects.toml"
        );
        Ok(Self { entries })
    }

    /// Resolve a `cwd` to a project slug if any rule matches. Best-effort:
    /// empty cwd or unmatched pattern → None.
    pub fn resolve(&self, cwd: &str) -> Option<&str> {
        if cwd.is_empty() {
            return None;
        }
        // Longest-pattern first so a more specific rule beats a generic one.
        let mut best: Option<(&Entry, usize)> = None;
        for e in &self.entries {
            let matched = if e.is_wildcard {
                cwd.starts_with(&e.pattern)
            } else {
                cwd == e.pattern || cwd.starts_with(&format!("{}/", e.pattern))
            };
            if !matched {
                continue;
            }
            let len = e.pattern.len();
            if best.map(|(_, l)| len > l).unwrap_or(true) {
                best = Some((e, len));
            }
        }
        best.map(|(e, _)| e.id.as_str())
    }
}

fn default_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("po-k").join("projects.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> ProjectMap {
        let parsed: File = toml::from_str(text).unwrap();
        let mut entries = Vec::new();
        for p in parsed.project {
            for pat in p.paths {
                let (pattern, is_wildcard) = if let Some(s) = pat.strip_suffix('*') {
                    (s.to_string(), true)
                } else {
                    (pat, false)
                };
                entries.push(Entry { id: p.id.clone(), pattern, is_wildcard });
            }
        }
        ProjectMap { entries }
    }

    #[test]
    fn exact_prefix_matches_subpath() {
        let m = parse(
            r#"
            [[project]]
            id = "po-k"
            paths = ["/workspace"]
            "#,
        );
        assert_eq!(m.resolve("/workspace"), Some("po-k"));
        assert_eq!(m.resolve("/workspace/sub"), Some("po-k"));
        assert_eq!(m.resolve("/workspace-other"), None);
        assert_eq!(m.resolve("/elsewhere"), None);
    }

    #[test]
    fn wildcard_prefix_matches_anything_after() {
        let m = parse(
            r#"
            [[project]]
            id = "p"
            paths = ["/home/me/work/po-k*"]
            "#,
        );
        assert_eq!(m.resolve("/home/me/work/po-k"), Some("p"));
        assert_eq!(m.resolve("/home/me/work/po-k-2"), Some("p"));
        assert_eq!(m.resolve("/home/me/work/other"), None);
    }

    #[test]
    fn longest_pattern_wins() {
        let m = parse(
            r#"
            [[project]]
            id = "general"
            paths = ["/home/me"]

            [[project]]
            id = "specific"
            paths = ["/home/me/work/foo"]
            "#,
        );
        assert_eq!(m.resolve("/home/me/work/foo/x"), Some("specific"));
        assert_eq!(m.resolve("/home/me/other"), Some("general"));
    }
}
