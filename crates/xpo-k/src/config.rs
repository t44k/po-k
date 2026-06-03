//! `xpo-k.yaml` loader (spec §4.2).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "~/.config/xpo-k/xpo-k.yaml";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: Server,
    pub auth: Auth,
    /// Profiles applied to every session unless overridden.
    pub default_profiles: Vec<String>,
    /// Per-project default profiles.
    pub project_defaults: BTreeMap<String, ProjectDefault>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Server {
    pub bind: String,
    pub base_url: String,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".to_string(),
            base_url: "http://127.0.0.1:8080".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Auth {
    pub bearer_token_file: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            bearer_token_file: "~/.config/xpo-k/auth.token".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProjectDefault {
    pub default_profiles: Vec<String>,
}

/// Expand a leading `~/` to `$HOME/`.
pub fn expand_path(p: impl AsRef<str>) -> PathBuf {
    let raw = p.as_ref();
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(raw)
}

pub fn default_config_path() -> PathBuf {
    expand_path(DEFAULT_CONFIG_PATH)
}

pub fn load_from(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    serde_yaml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

pub const SKELETON: &str = r#"# xpo-k configuration
server:
  bind: 0.0.0.0:8080
  base_url: http://127.0.0.1:8080

auth:
  bearer_token_file: ~/.config/xpo-k/auth.token

# Profiles applied to every session unless overridden.
default_profiles: []

# Per-project default profiles.
project_defaults: {}
  # acme-api:
  #   default_profiles: [base-coding, acme-standards]
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_skeleton() {
        let cfg: Config = serde_yaml::from_str(SKELETON).unwrap();
        assert_eq!(cfg.server.bind, "0.0.0.0:8080");
        assert!(cfg.default_profiles.is_empty());
    }

    #[test]
    fn parses_project_defaults() {
        let cfg: Config = serde_yaml::from_str(
            "project_defaults:\n  acme:\n    default_profiles: [base, acme-std]\n",
        )
        .unwrap();
        assert_eq!(
            cfg.project_defaults["acme"].default_profiles,
            vec!["base", "acme-std"]
        );
    }
}
