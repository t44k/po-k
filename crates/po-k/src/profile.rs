//! Profile system (M14): a self-contained description of a CC session's
//! configuration — CLAUDE.md, agents, skills, MCP servers, hooks, settings —
//! and the translation of that description into an on-disk CC plugin directory.
//!
//! Profiles are authored and MERGED centrally on Xpo-k. po-k only ever
//! consumes a single, already-merged [`Profile`] and lays it out on disk under
//! `~/.cache/po-k/sessions/{sid}/plugin/` for `claude --plugin-dir`.
//!
//! The JSON schema mirrors the spec (§3.2): snake_case field names. The plugin
//! files CC reads use camelCase (agent frontmatter) / kebab-case (skill
//! frontmatter), so the generators emit those keys explicitly rather than
//! relying on a single serde rename.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

// The profile schema types are shared with Xpo-k via the `pok-proto` crate.
// po-k owns only the on-disk *generation* of a merged profile (below).
pub use pok_proto::profile::*;

/// The po-k-side values that must be baked into every generated plugin so CC
/// can call back into po-k (hook curls + the permission MCP server). These are
/// NOT part of the profile — they come from po-k's own config/state.
pub struct PokHookContext<'a> {
    pub base_url: &'a str,
    pub token: &'a str,
    pub token_file: &'a Path,
    pub sid: &'a str,
}

/// Paths inside a generated plugin directory that the spawn command and
/// recovery need to reference.
#[derive(Debug, Clone)]
pub struct PluginPaths {
    pub dir: PathBuf,
    pub mcp_json: PathBuf,
    pub hooks_json: PathBuf,
}

/// Write `<session_dir>/plugin/` from a merged profile (spec §3.4 / §8).
/// `session_dir` is `~/.cache/po-k/sessions/{sid}`.
pub fn generate_plugin_dir(
    session_dir: &Path,
    profile: &Profile,
    pok: &PokHookContext,
) -> Result<PluginPaths> {
    let dir = session_dir.join("plugin");
    std::fs::create_dir_all(dir.join(".claude-plugin"))
        .with_context(|| format!("creating {}", dir.display()))?;

    // .claude-plugin/plugin.json
    std::fs::write(
        dir.join(".claude-plugin").join("plugin.json"),
        serde_json::to_string_pretty(&json!({ "name": format!("po-k-session-{}", pok.sid) }))
            .expect("plugin.json serialize"),
    )
    .context("writing plugin.json")?;

    // agents/<name>.md
    if !profile.agents.is_empty() {
        let agents_dir = dir.join("agents");
        std::fs::create_dir_all(&agents_dir).context("creating agents dir")?;
        for (name, agent) in &profile.agents {
            std::fs::write(agents_dir.join(format!("{name}.md")), render_agent_md(name, agent))
                .with_context(|| format!("writing agent {name}"))?;
        }
    }

    // skills/<name>/SKILL.md
    if !profile.skills.is_empty() {
        for (name, skill) in &profile.skills {
            let sdir = dir.join("skills").join(name);
            std::fs::create_dir_all(&sdir).context("creating skill dir")?;
            std::fs::write(sdir.join("SKILL.md"), render_skill_md(name, skill))
                .with_context(|| format!("writing skill {name}"))?;
        }
    }

    // CLAUDE.md (already concatenated by Xpo-k)
    if let Some(md) = &profile.claude_md {
        std::fs::write(dir.join("CLAUDE.md"), md).context("writing CLAUDE.md")?;
    }

    // .mcp.json — profile servers + reserved po-k server (never clobbered).
    let mcp_json = dir.join(".mcp.json");
    std::fs::write(&mcp_json, render_mcp_config(profile, pok)).context("writing .mcp.json")?;

    // hooks/hooks.json — po-k mandatory lifecycle hooks + profile hooks.
    let hooks_dir = dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir).context("creating hooks dir")?;
    let hooks_json = hooks_dir.join("hooks.json");
    std::fs::write(&hooks_json, render_hooks_config(profile, pok)?).context("writing hooks.json")?;

    Ok(PluginPaths {
        dir,
        mcp_json,
        hooks_json,
    })
}

/// Build the merged settings.json passed via `--settings`: po-k's hooks block
/// plus the profile's passthrough settings + optional main agent.
pub fn render_settings_json(
    profile: &Profile,
    pok: &PokHookContext,
    agent: Option<&str>,
) -> Result<String> {
    let hooks: Value = serde_json::from_str(&render_hooks_config(profile, pok)?)?;
    let mut settings = serde_json::Map::new();
    settings.insert("hooks".into(), hooks["hooks"].clone());
    if let Some(a) = agent {
        settings.insert("agent".into(), json!(a));
    }
    if !profile.settings.env.is_empty() {
        settings.insert("env".into(), serde_json::to_value(&profile.settings.env)?);
    }
    for (k, v) in &profile.settings.extra {
        settings.insert(k.clone(), v.clone());
    }
    Ok(serde_json::to_string_pretty(&Value::Object(settings)).expect("settings serialize"))
}

pub fn cleanup_plugin_dir(session_dir: &Path) {
    let _ = std::fs::remove_dir_all(session_dir.join("plugin"));
}

// ---- file renderers -------------------------------------------------------

/// A frontmatter scalar/array value.
enum Fm {
    Str(String),
    Bool(bool),
    Int(i64),
    List(Vec<String>),
}

/// Render a `key: value` line list into YAML frontmatter, skipping empties.
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
                let joined = items
                    .iter()
                    .map(|i| yaml_scalar(i))
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!("{key}: [{joined}]\n"));
            }
        }
    }
    out.push_str("---\n");
    out
}

/// Quote a YAML scalar only when needed (contains special chars or is
/// multi-word with leading/trailing space risk). Keeps simple values clean.
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

/// Agent frontmatter. SECURITY: only an allowlist of keys is ever emitted —
/// `permissionMode`, `mcpServers`, and `hooks` are forbidden in plugin agent
/// frontmatter by CC and must never leak from a profile (spec §8.2).
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

/// `.mcp.json`: profile servers first, then the reserved `po-k` server inserted
/// last so profile content can never override it (spec §3.4 / §8.4).
fn render_mcp_config(profile: &Profile, pok: &PokHookContext) -> String {
    let mut servers = serde_json::Map::new();
    for (name, s) in &profile.mcp_servers {
        if name == "po-k" {
            continue; // reserved; cannot be supplied by a profile
        }
        let mut entry = serde_json::Map::new();
        entry.insert("command".into(), json!(s.command));
        if !s.args.is_empty() {
            entry.insert("args".into(), json!(s.args));
        }
        if !s.env.is_empty() {
            entry.insert("env".into(), serde_json::to_value(&s.env).unwrap_or(Value::Null));
        }
        servers.insert(name.clone(), Value::Object(entry));
    }
    // po-k's own permission MCP server — always present, always last.
    servers.insert(
        "po-k".into(),
        json!({
            "command": "po-k",
            "args": [
                "mcp",
                "--session-id", pok.sid,
                "--base-url", pok.base_url,
                "--token-file", pok.token_file.to_string_lossy().into_owned(),
            ]
        }),
    );
    serde_json::to_string_pretty(&json!({ "mcpServers": Value::Object(servers) }))
        .expect(".mcp.json serialize")
}

/// `hooks/hooks.json`: po-k's mandatory lifecycle hooks (which CC curls back to
/// po-k) plus the profile's hooks appended additively per event. po-k's entries
/// stay first in each array (priority) (spec §3.4 / §8.5).
fn render_hooks_config(profile: &Profile, pok: &PokHookContext) -> Result<String> {
    let base = crate::session::render_hooks_json(pok.base_url, pok.sid, pok.token);
    let mut body: Value = serde_json::from_str(&base).context("parsing base hooks")?;
    let hooks = body["hooks"]
        .as_object_mut()
        .context("base hooks missing 'hooks' object")?;
    for (event, groups) in &profile.hooks {
        let arr = hooks
            .entry(event.clone())
            .or_insert_with(|| Value::Array(vec![]));
        if let Some(a) = arr.as_array_mut() {
            for g in groups {
                a.push(serde_json::to_value(g)?);
            }
        }
    }
    Ok(serde_json::to_string_pretty(&body).expect("hooks.json serialize"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> PokHookContext<'static> {
        PokHookContext {
            base_url: "http://127.0.0.1:7070",
            token: "TOK",
            token_file: Path::new("/home/me/.config/po-k/auth.token"),
            sid: "sid-1",
        }
    }

    fn reviewer_profile() -> Profile {
        let v = json!({
            "name": "code-reviewer",
            "claude_md": "# Review Protocol\nReview carefully.",
            "agents": {
                "security-reviewer": {
                    "description": "Reviews code for security",
                    "model": "opus",
                    "effort": "high",
                    "tools": ["Read", "Bash", "WebSearch"],
                    "background": true,
                    "isolation": "worktree",
                    "color": "red",
                    "prompt": "You are a security engineer."
                }
            },
            "skills": {
                "review-checklist": {
                    "description": "Standard review checklist",
                    "user_invocable": true,
                    "content": "## Checklist\n- [ ] Tests pass"
                }
            },
            "mcp_servers": {
                "database": { "command": "npx", "args": ["-y", "pg"] }
            },
            "hooks": {
                "PostToolUse": [
                    { "matcher": "", "hooks": [{ "type": "command", "command": "echo hi" }] }
                ]
            },
            "settings": { "model": "opus", "effort": "high" }
        });
        Profile::from_json(&v).unwrap()
    }

    #[test]
    fn deserializes_spec_profile() {
        let p = reviewer_profile();
        assert_eq!(p.name, "code-reviewer");
        assert_eq!(p.agents.len(), 1);
        let a = &p.agents["security-reviewer"];
        assert_eq!(a.model.as_deref(), Some("opus"));
        assert_eq!(a.background, Some(true));
        assert_eq!(p.settings.model.as_deref(), Some("opus"));
    }

    #[test]
    fn settings_extra_passthrough() {
        let p = Profile::from_json(&json!({
            "name": "x",
            "settings": { "model": "opus", "customKey": 42 }
        }))
        .unwrap();
        assert_eq!(p.settings.extra.get("customKey"), Some(&json!(42)));
    }

    #[test]
    fn generates_full_plugin_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let p = reviewer_profile();
        let paths = generate_plugin_dir(tmp.path(), &p, &ctx()).unwrap();

        // plugin.json
        let manifest =
            std::fs::read_to_string(paths.dir.join(".claude-plugin/plugin.json")).unwrap();
        assert!(manifest.contains("po-k-session-sid-1"));

        // agent: present fields, body, and NO forbidden keys
        let agent = std::fs::read_to_string(paths.dir.join("agents/security-reviewer.md")).unwrap();
        assert!(agent.contains("model: opus"));
        assert!(agent.contains("color: red"));
        assert!(agent.contains("background: true"));
        assert!(agent.contains("isolation: worktree"));
        assert!(agent.contains("You are a security engineer."));
        assert!(!agent.contains("permissionMode"));
        assert!(!agent.contains("mcpServers"));
        assert!(!agent.contains("\nhooks:"));

        // skill
        let skill =
            std::fs::read_to_string(paths.dir.join("skills/review-checklist/SKILL.md")).unwrap();
        assert!(skill.contains("user-invocable: true"));
        assert!(skill.contains("## Checklist"));

        // CLAUDE.md verbatim
        let claude = std::fs::read_to_string(paths.dir.join("CLAUDE.md")).unwrap();
        assert_eq!(claude, "# Review Protocol\nReview carefully.");
    }

    #[test]
    fn mcp_config_always_includes_pok_and_cannot_be_clobbered() {
        let tmp = tempfile::tempdir().unwrap();
        // A malicious profile that tries to define its own "po-k" server.
        let mut p = reviewer_profile();
        p.mcp_servers.insert(
            "po-k".into(),
            McpServer {
                command: "evil".into(),
                ..Default::default()
            },
        );
        let paths = generate_plugin_dir(tmp.path(), &p, &ctx()).unwrap();
        let mcp: Value =
            serde_json::from_str(&std::fs::read_to_string(paths.mcp_json).unwrap()).unwrap();
        let servers = mcp["mcpServers"].as_object().unwrap();
        assert!(servers.contains_key("database"));
        // po-k server is the real one, not "evil".
        assert_eq!(servers["po-k"]["command"], "po-k");
        assert_eq!(servers["po-k"]["args"][0], "mcp");
    }

    #[test]
    fn hooks_merge_pok_first_then_profile() {
        let tmp = tempfile::tempdir().unwrap();
        let p = reviewer_profile();
        let paths = generate_plugin_dir(tmp.path(), &p, &ctx()).unwrap();
        let hooks: Value =
            serde_json::from_str(&std::fs::read_to_string(paths.hooks_json).unwrap()).unwrap();
        let post = hooks["hooks"]["PostToolUse"].as_array().unwrap();
        // po-k's mandatory curl hook is first, profile's echo hook appended.
        assert!(post.len() >= 2);
        assert!(post[0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("curl"));
        assert!(post
            .iter()
            .any(|g| g["hooks"][0]["command"] == "echo hi"));
        // Untouched events still carry po-k's hook.
        assert!(hooks["hooks"]["Stop"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .contains("curl"));
    }

    #[test]
    fn settings_json_includes_hooks_and_agent() {
        let p = reviewer_profile();
        let s: Value =
            serde_json::from_str(&render_settings_json(&p, &ctx(), Some("security-reviewer")).unwrap())
                .unwrap();
        assert_eq!(s["agent"], "security-reviewer");
        assert!(s["hooks"]["Stop"].is_array());
    }
}
