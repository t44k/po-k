//! YAML config: `~/.config/po-k/po-k.yaml` plus optional per-repo `<repo>/po-k.yaml`
//! overlays. The overlay shape is a strict subset of the main shape; only
//! `topics`, `skills`, and `repos` (nested) merge in.
//!
//! Conventions
//! - Every path field accepts `~` and `$VAR` expansion at load time.
//! - Durations parse simple suffixes: `30s`, `5m`, `2h`.
//! - When the file is missing, `load_or_default()` returns a default `Config` with
//!   no repo configured — useful for the first `po-k --help` before `po-k init`.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const MAIN_CONFIG_REL: &str = ".config/po-k/po-k.yaml";
pub const REPO_OVERLAY_NAME: &str = "po-k.yaml";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// The primary git repo holding `memory/` + `skills/`. None until `po-k init`.
    pub repo: Option<Repo>,
    /// LLM backend used by the distillation loop.
    pub llm: Llm,
    /// Topics the distiller maintains digests for.
    pub topics: Vec<Topic>,
    /// Service / daemon options.
    pub service: Service,
    /// Gateway: project allowlist + zellij session preference.
    pub gateway: Gateway,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Repo {
    pub url: String,
    pub path: PathBuf,
    pub branch: String,
    pub push: bool,
    /// Pull cadence for the background tick.
    #[serde(deserialize_with = "deser_duration", serialize_with = "ser_duration")]
    pub pull_interval: Duration,
    /// Debounce window for batched pushes after distillation writes.
    #[serde(deserialize_with = "deser_duration", serialize_with = "ser_duration")]
    pub push_debounce: Duration,
}

impl Default for Repo {
    fn default() -> Self {
        Self {
            url: String::new(),
            path: PathBuf::from("~/.cache/po-k/repo"),
            branch: "main".to_string(),
            push: true,
            pull_interval: Duration::from_secs(5 * 60),
            push_debounce: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Llm {
    /// `claude-cli` (default) | `anthropic` | `openai`.
    pub backend: String,
    /// Optional model override (e.g. `claude-opus-4-7`).
    pub model: Option<String>,
}

impl Default for Llm {
    fn default() -> Self {
        Self {
            backend: "claude-cli".to_string(),
            model: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topic {
    pub id: String,
    pub question: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_extras: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Service {
    pub socket: PathBuf,
    pub state_db: PathBuf,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            socket: PathBuf::from("~/.config/po-k/service.sock"),
            state_db: PathBuf::from("~/.config/po-k/state.db"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Gateway {
    /// Per-project allowlist for the JSONL bridge.
    pub projects: Vec<ProjectEntry>,
    /// Restrict discovery to this zellij session (default: any).
    pub zellij_session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectEntry {
    /// Short identifier the remote uses to address this project.
    pub slug: String,
    /// Absolute path of the project root. Either `cwd` or `cwd_glob` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    /// Glob pattern matched against the running CC's cwd.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd_glob: Option<String>,
}

/// What a per-repo overlay file is allowed to declare.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Overlay {
    /// Extra nested repos to layer on top (cloned + watched the same way).
    pub repos: Vec<OverlayRepo>,
    /// Additional topics.
    pub topics: Vec<Topic>,
    /// Additional gateway projects.
    #[serde(default)]
    pub gateway: Gateway,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverlayRepo {
    pub url: String,
    pub path: PathBuf,
    #[serde(default = "default_branch")]
    pub branch: String,
}

fn default_branch() -> String {
    "main".into()
}

// ─── load / merge ────────────────────────────────────────────────────────────

pub fn main_config_path() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(MAIN_CONFIG_REL)
}

/// Load the main config (returns Default when the file is missing).
pub fn load_main() -> Result<Config> {
    let path = main_config_path();
    if !path.exists() {
        return Ok(Config::default());
    }
    let bytes = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let cfg: Config = serde_yaml::from_str(&bytes)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(cfg)
}

/// Resolve a path with `~` and env vars expanded. Best-effort: returns the input
/// path unchanged if expansion fails.
pub fn expand_path(p: &Path) -> PathBuf {
    let s = p.to_string_lossy();
    let s = if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return Path::new(&home).join(rest);
        }
        s.into_owned()
    } else if s == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
        s.into_owned()
    } else {
        s.into_owned()
    };
    PathBuf::from(s)
}

/// Load + merge the main config with every overlay it transitively pulls in.
/// `Overlay::repos` declarations are walked in order; cycles are short-circuited
/// by deduping on absolute repo path.
pub fn load_effective() -> Result<Effective> {
    let main = load_main()?;
    let mut eff = Effective::from(main.clone());
    if let Some(repo) = main.repo.as_ref() {
        let mut visited: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        walk_overlays(&expand_path(&repo.path), &mut eff, &mut visited)?;
    }
    Ok(eff)
}

fn walk_overlays(
    repo_path: &Path,
    eff: &mut Effective,
    visited: &mut std::collections::HashSet<PathBuf>,
) -> Result<()> {
    let abs = match std::fs::canonicalize(repo_path) {
        Ok(p) => p,
        // Repo not yet cloned — nothing to walk. Not an error during early bootstrap.
        Err(_) => return Ok(()),
    };
    if !visited.insert(abs.clone()) {
        return Ok(());
    }
    let overlay_path = abs.join(REPO_OVERLAY_NAME);
    if !overlay_path.exists() {
        return Ok(());
    }
    let text = std::fs::read_to_string(&overlay_path)
        .with_context(|| format!("reading overlay {}", overlay_path.display()))?;
    let overlay: Overlay = serde_yaml::from_str(&text)
        .with_context(|| format!("parsing overlay {}", overlay_path.display()))?;
    eff.merge_overlay(&overlay);
    for nested in &overlay.repos {
        walk_overlays(&expand_path(&nested.path), eff, visited)?;
    }
    Ok(())
}

/// The merged config used by every command at runtime.
#[derive(Debug, Clone)]
pub struct Effective {
    pub repo: Option<Repo>,
    pub llm: Llm,
    pub service: Service,
    pub gateway: Gateway,
    pub topics: Vec<Topic>,
    /// Nested repos contributed by overlays. The primary `repo` is not in here.
    pub nested_repos: Vec<OverlayRepo>,
}

impl From<Config> for Effective {
    fn from(c: Config) -> Self {
        Self {
            repo: c.repo,
            llm: c.llm,
            service: c.service,
            gateway: c.gateway,
            topics: c.topics,
            nested_repos: Vec::new(),
        }
    }
}

impl Effective {
    fn merge_overlay(&mut self, ov: &Overlay) {
        // Topics: dedupe by id; overlay wins.
        let new_ids: std::collections::HashSet<&str> =
            ov.topics.iter().map(|t| t.id.as_str()).collect();
        self.topics.retain(|t| !new_ids.contains(t.id.as_str()));
        self.topics.extend(ov.topics.iter().cloned());

        // Gateway projects: dedupe by slug; overlay wins.
        let new_slugs: std::collections::HashSet<&str> =
            ov.gateway.projects.iter().map(|p| p.slug.as_str()).collect();
        self.gateway
            .projects
            .retain(|p| !new_slugs.contains(p.slug.as_str()));
        self.gateway
            .projects
            .extend(ov.gateway.projects.iter().cloned());

        // Nested repos accumulate.
        self.nested_repos.extend(ov.repos.iter().cloned());
    }
}

// ─── duration helpers ────────────────────────────────────────────────────────

fn deser_duration<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

fn ser_duration<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let secs = d.as_secs();
    let out = if secs % 3600 == 0 && secs > 0 {
        format!("{}h", secs / 3600)
    } else if secs % 60 == 0 && secs > 0 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    };
    s.serialize_str(&out)
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('h') {
        n.trim().parse::<u64>().map(|n| Duration::from_secs(n * 3600))
            .map_err(|e| e.to_string())
    } else if let Some(n) = s.strip_suffix('m') {
        n.trim().parse::<u64>().map(|n| Duration::from_secs(n * 60))
            .map_err(|e| e.to_string())
    } else if let Some(n) = s.strip_suffix('s') {
        n.trim().parse::<u64>().map(Duration::from_secs)
            .map_err(|e| e.to_string())
    } else {
        s.parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string())
    }
}

// ─── skeleton + status helpers ───────────────────────────────────────────────

pub const SKELETON_YAML: &str = include_str!("config_skeleton.yaml");

#[derive(Debug, Default, Clone)]
pub struct Status {
    pub config_path: PathBuf,
    pub config_exists: bool,
    pub repo_path: Option<PathBuf>,
    pub repo_present: bool,
    pub last_pull: Option<String>,
    pub topic_count: usize,
    pub skill_count: usize,
}

pub fn status() -> Status {
    let main = load_main().unwrap_or_default();
    let config_path = main_config_path();
    let config_exists = config_path.exists();

    let mut s = Status {
        config_path,
        config_exists,
        ..Default::default()
    };
    if let Some(repo) = main.repo.as_ref() {
        let p = expand_path(&repo.path);
        s.repo_present = p.join(".git").exists();
        s.repo_path = Some(p.clone());
        if s.repo_present {
            // Memory + skill counts: just count *.md under the respective folders.
            s.topic_count = count_md(&p.join("memory"));
            s.skill_count = count_md(&p.join("skills"));
        }
    }
    s
}

fn count_md(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
    rd.flatten()
        .filter(|e| {
            e.file_type().map(|t| t.is_file()).unwrap_or(false)
                && e.file_name().to_string_lossy().ends_with(".md")
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skeleton_parses() {
        let cfg: Config = serde_yaml::from_str(SKELETON_YAML).unwrap();
        assert_eq!(cfg.llm.backend, "claude-cli");
        assert!(cfg.topics.iter().any(|t| t.id == "auth-pattern"));
    }

    #[test]
    fn overlay_merge_dedupes_topics() {
        let base = Config {
            topics: vec![Topic {
                id: "auth-pattern".into(),
                question: "old".into(),
                system_prompt_extras: None,
            }],
            ..Default::default()
        };
        let mut eff = Effective::from(base);
        eff.merge_overlay(&Overlay {
            topics: vec![Topic {
                id: "auth-pattern".into(),
                question: "new".into(),
                system_prompt_extras: None,
            }],
            ..Default::default()
        });
        assert_eq!(eff.topics.len(), 1);
        assert_eq!(eff.topics[0].question, "new");
    }

    #[test]
    fn duration_roundtrip() {
        for s in ["30s", "5m", "2h"] {
            let d = parse_duration(s).unwrap();
            let yaml = serde_yaml::to_string(&DurationWrapper(d)).unwrap();
            let back: DurationWrapper = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(d, back.0);
        }
    }

    // small helper struct so we can reuse the (de)ser fns in tests
    #[derive(Serialize, Deserialize)]
    struct DurationWrapper(
        #[serde(serialize_with = "ser_duration", deserialize_with = "deser_duration")] Duration,
    );
}
