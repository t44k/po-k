//! YAML config loader for `po-k.yaml`.
//!
//! Layout (see plan):
//!   server: { bind, base_url, reload_on_change }
//!   auth:   { bearer_token_file }
//!   cc:     { model, effort, permission_mode, permission_timeout, disable_slash_commands }
//!   zellij: { session_prefix }
//!   projects: [ { name, cwd, model?, effort?, add_dirs?, zellij_session? } ]

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DEFAULT_CONFIG_PATH: &str = "~/.config/po-k/po-k.yaml";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: Server,
    pub auth: Auth,
    /// Xpo-k connection (M14 Phase 2). Optional until the cutover; once set,
    /// po-k connects to Xpo-k as a WebSocket client.
    pub xpok: Option<Xpok>,
    /// Localhost-only listener that receives CC's hook/permission callbacks.
    pub hooks: Hooks,
    pub cc: CcDefaults,
    pub zellij: Zellij,
    pub projects: Vec<Project>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Xpok {
    /// WebSocket URL of the Xpo-k server, e.g. `ws://xpo-k.host:8080/ws`.
    pub url: String,
    /// Shared secret / bearer presented on connect.
    #[serde(default)]
    pub token: String,
    #[serde(default = "default_reconnect")]
    pub reconnect_interval: HumanDuration,
}

fn default_reconnect() -> HumanDuration {
    HumanDuration(Duration::from_secs(5))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Hooks {
    /// Local-only bind for the hook/permission callback listener.
    pub bind: String,
}

impl Default for Hooks {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7070".to_string(),
        }
    }
}

impl Hooks {
    /// Base URL CC's hook curls + the mcp subprocess post back to.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.bind)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Server {
    pub bind: String,
    pub base_url: String,
    pub reload_on_change: bool,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7070".to_string(),
            base_url: "http://127.0.0.1:7070".to_string(),
            reload_on_change: true,
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
            bearer_token_file: "~/.config/po-k/auth.token".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CcDefaults {
    pub model: String,
    pub effort: String,
    pub permission_mode: String,
    /// Wall-clock budget the MCP `approve` tool waits for the orchestrator's
    /// decision. Times out → deny.
    pub permission_timeout: HumanDuration,
    pub disable_slash_commands: bool,
}

impl Default for CcDefaults {
    fn default() -> Self {
        Self {
            model: "sonnet".to_string(),
            effort: "medium".to_string(),
            permission_mode: "acceptEdits".to_string(),
            permission_timeout: HumanDuration(Duration::from_secs(60)),
            disable_slash_commands: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Zellij {
    pub session_prefix: String,
}

impl Default for Zellij {
    fn default() -> Self {
        Self {
            session_prefix: "po-k-".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add_dirs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zellij_session: Option<String>,
}

impl Project {
    pub fn zellij_session_name(&self, defaults: &Zellij) -> String {
        self.zellij_session
            .clone()
            .unwrap_or_else(|| format!("{}{}", defaults.session_prefix, self.name))
    }

    pub fn model<'a>(&'a self, defaults: &'a CcDefaults) -> &'a str {
        self.model.as_deref().unwrap_or(&defaults.model)
    }

    pub fn effort<'a>(&'a self, defaults: &'a CcDefaults) -> &'a str {
        self.effort.as_deref().unwrap_or(&defaults.effort)
    }
}

/// `"30s"`, `"5m"`, `"2h"` — defaults to seconds if unitless.
#[derive(Debug, Clone, Copy)]
pub struct HumanDuration(pub Duration);

impl Default for HumanDuration {
    fn default() -> Self {
        Self(Duration::from_secs(60))
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}s", self.0.as_secs()))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_duration(&s)
            .map(HumanDuration)
            .map_err(serde::de::Error::custom)
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }
    let (num, unit_secs) = if let Some(n) = s.strip_suffix("ms") {
        (n, 0u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else {
        (s, 1)
    };
    let n: u64 = num.trim().parse().map_err(|e| format!("bad number {num:?}: {e}"))?;
    if unit_secs == 0 {
        Ok(Duration::from_millis(n))
    } else {
        Ok(Duration::from_secs(n.saturating_mul(unit_secs)))
    }
}

/// Expand a leading `~/` to `$HOME/`. Other paths pass through.
pub fn expand_path(p: impl AsRef<str>) -> PathBuf {
    let raw = p.as_ref();
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    if raw == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
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
    let cfg: Config = serde_yaml::from_str(&raw)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(cfg)
}

pub fn load_default() -> Result<Config> {
    load_from(&default_config_path())
}

pub fn skeleton_yaml() -> &'static str {
    include_str!("config_skeleton.yaml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_skeleton() {
        let cfg: Config = serde_yaml::from_str(skeleton_yaml()).unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:7070");
        assert_eq!(cfg.cc.permission_mode, "acceptEdits");
        assert_eq!(cfg.cc.permission_timeout.0, Duration::from_secs(60));
        assert!(cfg.projects.iter().any(|p| p.name == "po-k"));
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("42").unwrap(), Duration::from_secs(42));
    }

    #[test]
    fn project_session_name_uses_prefix() {
        let zellij = Zellij {
            session_prefix: "po-k-".into(),
        };
        let p = Project {
            name: "po-k".into(),
            cwd: "/workspace".into(),
            model: None,
            effort: None,
            add_dirs: vec![],
            zellij_session: None,
        };
        assert_eq!(p.zellij_session_name(&zellij), "po-k-po-k");
    }

    #[test]
    fn project_session_name_honors_override() {
        let zellij = Zellij::default();
        let p = Project {
            name: "foo".into(),
            cwd: "/x".into(),
            model: None,
            effort: None,
            add_dirs: vec![],
            zellij_session: Some("custom".into()),
        };
        assert_eq!(p.zellij_session_name(&zellij), "custom");
    }
}
