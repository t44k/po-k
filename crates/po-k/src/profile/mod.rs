//! v1 profiles → CC plugin directories.
//!
//! po-k v2 has no profile store: a session names the plugin directories it
//! wants and CC loads them. This module keeps just enough of the v1 profile
//! system to (a) render one exported profile into a plugin directory
//! (`po-k export-profile`) and (b) serialise an [`McpServer`] into the
//! `.mcp.json` entry shape that both the export and the per-session
//! `mcp.json` use.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub mod types;
pub use types::*;

/// One `.mcp.json` entry from an [`McpServer`]. `redact_env` replaces every
/// env / header value with `${NAME}` so credentials never land on disk in a
/// shared plugin directory (CC expands `${VAR}` from its own environment).
pub fn mcp_server_json(s: &McpServer, redact_env: bool) -> Value {
    let mut entry = serde_json::Map::new();
    entry.insert("type".into(), json!(s.kind));
    if let Some(cmd) = &s.command {
        entry.insert("command".into(), json!(cmd));
    }
    if !s.args.is_empty() {
        entry.insert("args".into(), json!(s.args));
    }
    if let Some(url) = &s.url {
        entry.insert("url".into(), json!(url));
    }
    if !s.env.is_empty() {
        let env: serde_json::Map<String, Value> = s
            .env
            .iter()
            .map(|(k, v)| (k.clone(), json!(if redact_env { format!("${{{k}}}") } else { v.clone() })))
            .collect();
        entry.insert("env".into(), Value::Object(env));
    }
    if !s.headers.is_empty() {
        let headers: serde_json::Map<String, Value> = s
            .headers
            .iter()
            .map(|(k, v)| {
                let key = k.to_uppercase().replace('-', "_");
                (k.clone(), json!(if redact_env { format!("${{{key}}}") } else { v.clone() }))
            })
            .collect();
        entry.insert("headers".into(), Value::Object(headers));
    }
    Value::Object(entry)
}

/// Write a v1 profile as a CC plugin directory at `out` (created). Returns the
/// list of env var names the plugin now expects when `redact_env` is set.
pub fn render_plugin_dir(out: &Path, profile: &Profile, redact_env: bool) -> Result<Vec<String>> {
    std::fs::create_dir_all(out.join(".claude-plugin"))
        .with_context(|| format!("creating {}", out.display()))?;
    let manifest = json!({
        "name": profile.name,
        "description": profile.description.clone().unwrap_or_default(),
        "version": profile.version.clone().unwrap_or_else(|| "1.0.0".into()),
    });
    std::fs::write(
        out.join(".claude-plugin").join("plugin.json"),
        serde_json::to_string_pretty(&manifest).expect("plugin.json serialize"),
    )
    .context("writing plugin.json")?;

    if !profile.agents.is_empty() {
        let dir = out.join("agents");
        std::fs::create_dir_all(&dir).context("creating agents dir")?;
        for (name, agent) in &profile.agents {
            std::fs::write(dir.join(format!("{name}.md")), render_agent_md(name, agent))
                .with_context(|| format!("writing agent {name}"))?;
        }
    }
    for (name, skill) in &profile.skills {
        let sdir = out.join("skills").join(name);
        std::fs::create_dir_all(&sdir).context("creating skill dir")?;
        std::fs::write(sdir.join("SKILL.md"), render_skill_md(name, skill))
            .with_context(|| format!("writing skill {name}"))?;
    }
    if let Some(md) = &profile.claude_md {
        std::fs::write(out.join("CLAUDE.md"), md).context("writing CLAUDE.md")?;
    }

    let mut env_names = Vec::new();
    if !profile.mcp_servers.is_empty() {
        let mut servers = serde_json::Map::new();
        for (name, s) in &profile.mcp_servers {
            if name == "po-k" {
                continue; // reserved for po-k's own permission server
            }
            if redact_env {
                env_names.extend(s.env.keys().cloned());
                env_names.extend(s.headers.keys().map(|k| k.to_uppercase().replace('-', "_")));
            }
            servers.insert(name.clone(), mcp_server_json(s, redact_env));
        }
        std::fs::write(
            out.join(".mcp.json"),
            serde_json::to_string_pretty(&json!({ "mcpServers": Value::Object(servers) }))
                .expect(".mcp.json serialize"),
        )
        .context("writing .mcp.json")?;
    }
    if !profile.hooks.is_empty() {
        let hooks_dir = out.join("hooks");
        std::fs::create_dir_all(&hooks_dir).context("creating hooks dir")?;
        std::fs::write(
            hooks_dir.join("hooks.json"),
            serde_json::to_string_pretty(&json!({ "hooks": profile.hooks })).expect("hooks serialize"),
        )
        .context("writing hooks.json")?;
    }
    // Settings cannot ride in a plugin; keep them next to it as a reference
    // so the operator knows what to pass on `POST /sessions`.
    let settings = serde_json::to_value(&profile.settings).unwrap_or(json!({}));
    if settings.as_object().is_some_and(|o| !o.is_empty()) {
        std::fs::write(
            out.join("profile-settings.json"),
            serde_json::to_string_pretty(&settings).expect("settings serialize"),
        )
        .context("writing profile-settings.json")?;
    }
    Ok(env_names)
}

#[allow(dead_code)]
pub fn plugin_dir_for(out_root: &Path, name: &str) -> PathBuf {
    out_root.join(name)
}

// ---- file renderers -------------------------------------------------------

enum Fm {
    Str(String),
    Bool(bool),
    Int(i64),
    List(Vec<String>),
}

fn frontmatter(pairs: Vec<(&str, Option<Fm>)>) -> String {
    let mut out = String::from("---\n");
    for (key, val) in pairs {
        let Some(val) = val else { continue };
        match val {
            Fm::Str(s) => out.push_str(&format!("{key}: {}\n", yaml_scalar(&s))),
            Fm::Bool(b) => out.push_str(&format!("{key}: {b}\n")),
            Fm::Int(i) => out.push_str(&format!("{key}: {i}\n")),
            Fm::List(items) => {
                if items.is_empty() {
                    continue;
                }
                let joined = items.iter().map(|i| yaml_scalar(i)).collect::<Vec<_>>().join(", ");
                out.push_str(&format!("{key}: [{joined}]\n"));
            }
        }
    }
    out.push_str("---\n");
    out
}

fn yaml_scalar(s: &str) -> String {
    let needs_quote = s.is_empty()
        || s.chars()
            .any(|c| matches!(c, ':' | '#' | '[' | ']' | '{' | '}' | ',' | '"' | '\'' | '\n'))
        || s.starts_with(' ')
        || s.ends_with(' ');
    if needs_quote {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        s.to_string()
    }
}

/// Agent frontmatter. Only an allowlist of keys is emitted — `permissionMode`,
/// `mcpServers` and `hooks` are forbidden in plugin agent frontmatter by CC.
fn render_agent_md(name: &str, a: &Agent) -> String {
    let fm = frontmatter(vec![
        ("name", Some(Fm::Str(name.to_string()))),
        ("description", a.description.clone().map(Fm::Str)),
        ("model", a.model.clone().map(Fm::Str)),
        ("effort", a.effort.clone().map(Fm::Str)),
        ("maxTurns", a.max_turns.map(Fm::Int)),
        ("tools", non_empty_list(&a.tools)),
        ("disallowedTools", non_empty_list(&a.disallowed_tools)),
        ("skills", non_empty_list(&a.skills)),
        ("background", a.background.map(Fm::Bool)),
        ("isolation", a.isolation.clone().map(Fm::Str)),
        ("color", a.color.clone().map(Fm::Str)),
        ("initialPrompt", a.initial_prompt.clone().map(Fm::Str)),
    ]);
    format!("{fm}\n{}\n", a.prompt.clone().unwrap_or_default())
}

fn render_skill_md(name: &str, s: &Skill) -> String {
    let fm = frontmatter(vec![
        ("name", Some(Fm::Str(name.to_string()))),
        ("description", s.description.clone().map(Fm::Str)),
        ("when_to_use", s.when_to_use.clone().map(Fm::Str)),
        ("allowed-tools", non_empty_list(&s.allowed_tools)),
        ("disallowed-tools", non_empty_list(&s.disallowed_tools)),
        ("model", s.model.clone().map(Fm::Str)),
        ("effort", s.effort.clone().map(Fm::Str)),
        ("user-invocable", Some(Fm::Bool(s.user_invocable))),
        ("arguments", non_empty_list(&s.arguments)),
        ("argument-hint", s.argument_hint.clone().map(Fm::Str)),
    ]);
    format!("{fm}\n{}\n", s.content.clone().unwrap_or_default())
}

fn non_empty_list(v: &[String]) -> Option<Fm> {
    if v.is_empty() {
        None
    } else {
        Some(Fm::List(v.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reviewer_profile() -> Profile {
        Profile::from_json(&json!({
            "name": "code-reviewer",
            "description": "Reviews",
            "claude_md": "# Review Protocol\nReview carefully.",
            "agents": {
                "security-reviewer": {
                    "description": "Reviews code for security",
                    "model": "opus",
                    "tools": ["Read", "Bash"],
                    "background": true,
                    "isolation": "worktree",
                    "color": "red",
                    "prompt": "You are a security engineer."
                }
            },
            "skills": {
                "review-checklist": { "description": "Checklist", "user_invocable": true, "content": "## Checklist" }
            },
            "mcp_servers": {
                "database": { "command": "npx", "args": ["-y", "pg"], "env": { "PG_TOKEN": "s3cret" } },
                "po-k": { "command": "evil" }
            },
            "hooks": {
                "PostToolUse": [ { "matcher": "", "hooks": [{ "type": "command", "command": "echo hi" }] } ]
            },
            "settings": { "model": "opus", "effort": "high" }
        }))
        .unwrap()
    }

    #[test]
    fn deserializes_spec_profile() {
        let p = reviewer_profile();
        assert_eq!(p.name, "code-reviewer");
        let a = &p.agents["security-reviewer"];
        assert_eq!(a.model.as_deref(), Some("opus"));
        assert_eq!(p.settings.model.as_deref(), Some("opus"));
        // v1 profiles always had a command; v2 accepts remote servers too.
        let remote: McpServer =
            serde_json::from_value(json!({ "type": "http", "url": "https://x/mcp" })).unwrap();
        assert_eq!(remote.kind, "http");
        assert!(remote.command.is_none());
    }

    #[test]
    fn settings_extra_passthrough() {
        let p = Profile::from_json(&json!({ "name": "x", "settings": { "model": "opus", "customKey": 42 } }))
            .unwrap();
        assert_eq!(p.settings.extra.get("customKey"), Some(&json!(42)));
    }

    #[test]
    fn renders_plugin_dir_with_redacted_env_and_no_pok_server() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("code-reviewer");
        let env = render_plugin_dir(&out, &reviewer_profile(), true).unwrap();
        assert_eq!(env, vec!["PG_TOKEN".to_string()]);

        let manifest = std::fs::read_to_string(out.join(".claude-plugin/plugin.json")).unwrap();
        assert!(manifest.contains("\"code-reviewer\""));
        let agent = std::fs::read_to_string(out.join("agents/security-reviewer.md")).unwrap();
        assert!(agent.contains("model: opus"));
        assert!(agent.contains("isolation: worktree"));
        assert!(!agent.contains("permissionMode"));
        let skill = std::fs::read_to_string(out.join("skills/review-checklist/SKILL.md")).unwrap();
        assert!(skill.contains("user-invocable: true"));
        assert_eq!(
            std::fs::read_to_string(out.join("CLAUDE.md")).unwrap(),
            "# Review Protocol\nReview carefully."
        );
        let mcp: Value = serde_json::from_str(&std::fs::read_to_string(out.join(".mcp.json")).unwrap()).unwrap();
        let servers = mcp["mcpServers"].as_object().unwrap();
        assert!(!servers.contains_key("po-k"), "reserved name must be dropped");
        assert_eq!(servers["database"]["env"]["PG_TOKEN"], "${PG_TOKEN}");
        assert_eq!(servers["database"]["type"], "stdio");
        let hooks: Value = serde_json::from_str(&std::fs::read_to_string(out.join("hooks/hooks.json")).unwrap()).unwrap();
        assert_eq!(hooks["hooks"]["PostToolUse"][0]["hooks"][0]["command"], "echo hi");
        let settings: Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("profile-settings.json")).unwrap()).unwrap();
        assert_eq!(settings["model"], "opus");
    }

    #[test]
    fn keep_env_writes_values_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("p");
        let env = render_plugin_dir(&out, &reviewer_profile(), false).unwrap();
        assert!(env.is_empty());
        let mcp: Value = serde_json::from_str(&std::fs::read_to_string(out.join(".mcp.json")).unwrap()).unwrap();
        assert_eq!(mcp["mcpServers"]["database"]["env"]["PG_TOKEN"], "s3cret");
    }

    #[test]
    fn mcp_server_json_remote_shape() {
        let s: McpServer = serde_json::from_value(json!({
            "type": "http", "url": "https://x/mcp", "headers": { "Authorization": "Bearer t" }
        }))
        .unwrap();
        let v = mcp_server_json(&s, true);
        assert_eq!(v["type"], "http");
        assert_eq!(v["url"], "https://x/mcp");
        assert_eq!(v["headers"]["Authorization"], "${AUTHORIZATION}");
        assert!(v.get("command").is_none());
    }
}
