//! `po-k.yaml` — two optional keys:
//!
//! ```yaml
//! auth:
//!   bearer_token_file: ~/.config/po-k/auth.token
//! server:
//!   bind: 0.0.0.0:13658
//! ```
//!
//! A missing or empty file means defaults. Unknown keys are tolerated so an
//! old v1 file (`xpok:`, `cc:`, `projects:` …) still loads. Everything about a
//! *session* comes from the create request; fixed fallbacks live in
//! [`crate::defaults`].

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::defaults;

pub const DEFAULT_CONFIG_PATH: &str = "~/.config/po-k/po-k.yaml";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub auth: Auth,
    pub server: Server,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Auth {
    pub bearer_token_file: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            bearer_token_file: defaults::TOKEN_FILE.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Server {
    /// Address the HTTP API listens on. `0.0.0.0` inside a dev box: the box
    /// network is private and every mutating route is bearer-protected.
    pub bind: String,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: defaults::BIND.to_string(),
        }
    }
}

impl Server {
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        SocketAddr::from_str(&self.bind).with_context(|| format!("parsing server.bind {:?}", self.bind))
    }

    /// The URL CC's hook curls and the `cc-mcp` shim call back on. Always the
    /// loopback form of the bind so it works regardless of which interface we
    /// listen on. Baked into each session's hooks.json / mcp.json at spawn.
    pub fn callback_base_url(&self) -> String {
        callback_base_url_for(&self.bind)
    }
}

pub fn callback_base_url_for(bind: &str) -> String {
    match SocketAddr::from_str(bind) {
        Ok(addr) => {
            let host = match addr.ip() {
                ip if ip.is_unspecified() => match ip {
                    IpAddr::V4(_) => "127.0.0.1".to_string(),
                    IpAddr::V6(_) => "[::1]".to_string(),
                },
                IpAddr::V4(v4) => v4.to_string(),
                IpAddr::V6(v6) => format!("[{v6}]"),
            };
            format!("http://{host}:{}", addr.port())
        }
        Err(_) => format!("http://{bind}"),
    }
}

/// Load a config file. Missing or blank → defaults.
pub fn load_from(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(Config::default());
    }
    serde_yaml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

pub fn load_default() -> Result<Config> {
    load_from(&default_config_path())
}

pub fn default_config_path() -> PathBuf {
    std::env::var("POK_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| expand_path(DEFAULT_CONFIG_PATH))
}

/// Expand a leading `~/` using `$HOME`.
pub fn expand_path(p: impl AsRef<str>) -> PathBuf {
    let p = p.as_ref();
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(p)
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
        assert_eq!(cfg.server.bind, "0.0.0.0:13658");
        assert_eq!(cfg.auth.bearer_token_file, "~/.config/po-k/auth.token");
    }

    #[test]
    fn missing_or_blank_file_is_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.yaml");
        let cfg = load_from(&missing).unwrap();
        assert_eq!(cfg.server.bind, defaults::BIND);
        let blank = dir.path().join("blank.yaml");
        std::fs::write(&blank, "\n  \n").unwrap();
        assert_eq!(load_from(&blank).unwrap().server.bind, defaults::BIND);
    }

    #[test]
    fn tolerates_v1_keys() {
        let cfg: Config = serde_yaml::from_str(
            "auth:\n  bearer_token_file: /t\nxpok:\n  url: ws://x\ncc:\n  model: fable\nprojects: []\n",
        )
        .unwrap();
        assert_eq!(cfg.auth.bearer_token_file, "/t");
        assert_eq!(cfg.server.bind, defaults::BIND);
    }

    #[test]
    fn callback_base_url_maps_unspecified_to_loopback() {
        assert_eq!(callback_base_url_for("0.0.0.0:13658"), "http://127.0.0.1:13658");
        assert_eq!(callback_base_url_for("10.0.0.5:8000"), "http://10.0.0.5:8000");
        assert_eq!(callback_base_url_for("[::]:13658"), "http://[::1]:13658");
        assert_eq!(callback_base_url_for("127.0.0.1:7071"), "http://127.0.0.1:7071");
    }

    #[test]
    fn expand_tilde() {
        std::env::set_var("HOME", "/home/x");
        assert_eq!(expand_path("~/a/b"), PathBuf::from("/home/x/a/b"));
        assert_eq!(expand_path("/abs"), PathBuf::from("/abs"));
    }
}
