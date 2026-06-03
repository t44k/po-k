//! `GET /sessions/{id}/capabilities` (spec §6.2): introspect what a running CC
//! session can do by reading the plugin directory po-k generated for it. The
//! plugin dir on disk is the source of truth — it's exactly what CC sees.

use serde_json::{json, Value};
use std::path::Path;

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

pub async fn get(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    // Resolve plugin_dir + profiles from the live registry, falling back to the
    // DB for ended sessions.
    let (plugin_dir, profiles, project): (Option<String>, Vec<String>, String) =
        if let Some(s) = state.sessions.get(sid).await {
            (s.plugin_dir, s.profiles, s.project)
        } else if let Some(row) = crate::events_store::get_session(&state.db, sid)
            .await
            .map_err(internal)?
        {
            let profiles = row
                .profiles
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            (row.plugin_dir, profiles, row.project)
        } else {
            return Err(CoreError::not_found(sid));
        };

    let disable_slash = state.config.read().await.cc.disable_slash_commands;

    let mut agents = Vec::new();
    let mut skills = Vec::new();
    let mut mcp_servers = Vec::new();
    let mut settings = json!({});
    let mut claude_md_summary = String::new();

    if let Some(dir) = plugin_dir.as_deref() {
        let dir = Path::new(dir);
        agents = read_agents(dir);
        skills = read_skills(dir);
        mcp_servers = read_mcp(dir);
        settings = read_settings(dir);
        claude_md_summary = read_claude_summary(dir);
    }
    // po-k's permission server is always configured, even in legacy sessions.
    if !mcp_servers.iter().any(|m| m["name"] == "po-k") {
        mcp_servers.push(json!({ "name": "po-k", "command": "po-k", "status": "configured" }));
    }

    Ok(CoreResponse::ok(json!({
        "session_id": sid,
        "project": project,
        "profiles_applied": profiles,
        "capabilities": {
            "agents": agents,
            "skills": skills,
            "mcp_servers": mcp_servers,
            "settings": settings,
            "claude_md_summary": claude_md_summary,
            "cc_built_in": {
                "modes": ["plan", "autoEdit", "fullAuto"],
                "slash_commands_enabled": !disable_slash,
                "task_tool_available": true,
            }
        }
    })))
}

/// Extract and parse the YAML frontmatter block (between the leading `---` and
/// the next `---`) into a JSON object.
fn parse_frontmatter(content: &str) -> Value {
    let trimmed = content.trim_start();
    let Some(rest) = trimmed.strip_prefix("---") else {
        return json!({});
    };
    let Some(end) = rest.find("\n---") else {
        return json!({});
    };
    let yaml = &rest[..end];
    serde_yaml::from_str::<Value>(yaml).unwrap_or_else(|_| json!({}))
}

fn read_agents(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir.join("agents")) else {
        return out;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let fm = parse_frontmatter(&content);
        out.push(json!({
            "name": fm.get("name").cloned().unwrap_or(Value::Null),
            "description": fm.get("description").cloned().unwrap_or(Value::Null),
            "model": fm.get("model").cloned().unwrap_or(Value::Null),
            "background": fm.get("background").and_then(|v| v.as_bool()).unwrap_or(false),
        }));
    }
    out
}

fn read_skills(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir.join("skills")) else {
        return out;
    };
    for entry in rd.flatten() {
        let skill_md = entry.path().join("SKILL.md");
        let Ok(content) = std::fs::read_to_string(&skill_md) else {
            continue;
        };
        let fm = parse_frontmatter(&content);
        out.push(json!({
            "name": fm.get("name").cloned().unwrap_or(Value::Null),
            "description": fm.get("description").cloned().unwrap_or(Value::Null),
            "user_invocable": fm.get("user-invocable").and_then(|v| v.as_bool()).unwrap_or(true),
        }));
    }
    out
}

fn read_mcp(dir: &Path) -> Vec<Value> {
    let Ok(content) = std::fs::read_to_string(dir.join(".mcp.json")) else {
        return Vec::new();
    };
    let parsed: Value = serde_json::from_str(&content).unwrap_or(json!({}));
    let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    servers
        .iter()
        .map(|(name, cfg)| {
            json!({
                "name": name,
                "command": cfg.get("command").cloned().unwrap_or(Value::Null),
                "status": "configured",
            })
        })
        .collect()
}

fn read_settings(dir: &Path) -> Value {
    let Ok(content) = std::fs::read_to_string(dir.join("settings.json")) else {
        return json!({});
    };
    let parsed: Value = serde_json::from_str(&content).unwrap_or(json!({}));
    json!({
        "model": parsed.get("model").cloned().unwrap_or(Value::Null),
        "effort": parsed.get("effortLevel").or_else(|| parsed.get("effort")).cloned().unwrap_or(Value::Null),
        "permission_mode": parsed.get("permissionMode").cloned().unwrap_or(Value::Null),
    })
}

fn read_claude_summary(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("CLAUDE.md"))
        .map(|s| s.chars().take(500).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_parses_agent() {
        let md = "---\nname: rev\ndescription: Reviews\nmodel: opus\nbackground: true\n---\nbody";
        let fm = parse_frontmatter(md);
        assert_eq!(fm["name"], "rev");
        assert_eq!(fm["background"], true);
    }

    #[test]
    fn reads_back_generated_plugin_dir() {
        use crate::profile::{Profile, PokHookContext};
        let tmp = tempfile::tempdir().unwrap();
        let p = Profile::from_json(&json!({
            "name": "rev",
            "claude_md": "# Hello",
            "agents": { "sec": { "description": "d", "model": "opus", "background": true, "prompt": "x" } },
            "skills": { "chk": { "description": "s", "user_invocable": true, "content": "c" } },
            "mcp_servers": { "db": { "command": "npx" } }
        }))
        .unwrap();
        let pok = PokHookContext {
            base_url: "http://127.0.0.1:7070",
            token: "T",
            token_file: Path::new("/t"),
            sid: "s",
        };
        let paths = crate::profile::generate_plugin_dir(tmp.path(), &p, &pok).unwrap();

        let agents = read_agents(&paths.dir);
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0]["name"], "sec");
        assert_eq!(agents[0]["background"], true);

        let skills = read_skills(&paths.dir);
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0]["name"], "chk");
        assert_eq!(skills[0]["user_invocable"], true);

        let mcp = read_mcp(&paths.dir);
        // profile's "db" + reserved "po-k"
        assert!(mcp.iter().any(|m| m["name"] == "db"));
        assert!(mcp.iter().any(|m| m["name"] == "po-k"));

        assert_eq!(read_claude_summary(&paths.dir), "# Hello");
    }
}
