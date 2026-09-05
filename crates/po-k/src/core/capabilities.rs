//! `GET /sessions/{id}/capabilities`: what a session can do, read from the
//! plugin directories it was created with (exactly what CC sees) plus the
//! MCP servers po-k wrote into its `mcp.json`.

use serde_json::{json, Value};
use std::path::Path;

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::defaults;
use crate::state::AppState;

pub async fn get(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    // Live registry first, DB fallback for ended sessions.
    let (name, plugins, mcp_names, model, effort, permission_mode, agent) =
        if let Some(s) = state.sessions.get(sid).await {
            (s.name, s.plugins, s.mcp_servers, s.model, s.effort, s.permission_mode, s.agent)
        } else if let Some(row) = crate::events_store::get_session(&state.db, sid).await.map_err(internal)? {
            let parse = |s: Option<&str>| -> Vec<String> {
                s.and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default()
            };
            (
                row.name,
                parse(row.plugins.as_deref()),
                parse(row.mcp_servers.as_deref()),
                row.model.unwrap_or_else(|| defaults::MODEL.into()),
                row.effort.unwrap_or_else(|| defaults::EFFORT.into()),
                row.permission_mode.unwrap_or_else(|| defaults::PERMISSION_MODE.into()),
                row.agent,
            )
        } else {
            return Err(CoreError::not_found(sid));
        };

    let plugin_views: Vec<Value> = plugins.iter().map(|p| inspect_plugin(p)).collect();
    let mut mcp_servers: Vec<Value> = mcp_names
        .iter()
        .map(|n| json!({ "name": n, "source": "request", "status": "configured" }))
        .collect();
    mcp_servers.push(json!({ "name": "po-k", "source": "po-k", "status": "configured" }));
    let collisions: Vec<&Value> = plugin_views
        .iter()
        .flat_map(|p| p["mcp_servers"].as_array().into_iter().flatten())
        .filter(|m| m["name"] == "po-k")
        .collect();

    Ok(CoreResponse::ok(json!({
        "session_id": sid,
        "name": name,
        "plugins": plugin_views,
        "capabilities": {
            "mcp_servers": mcp_servers,
            "settings": { "model": model, "effort": effort, "permission_mode": permission_mode, "agent": agent },
            "cc_built_in": {
                "slash_commands_enabled": !defaults::DISABLE_SLASH_COMMANDS,
                "task_tool_available": true,
            }
        },
        "warnings": if collisions.is_empty() {
            Vec::<String>::new()
        } else {
            vec!["a plugin defines an MCP server named \"po-k\", which collides with po-k's permission server".to_string()]
        },
    })))
}

/// Read what a plugin directory exposes. URLs and `.zip` archives are listed
/// but not opened.
pub fn inspect_plugin(source: &str) -> Value {
    let path = Path::new(source);
    if source.starts_with("http://") || source.starts_with("https://") || !path.is_dir() {
        return json!({ "source": source, "inspected": false });
    }
    json!({
        "source": source,
        "inspected": true,
        "agents": read_agents(path),
        "skills": read_skills(path),
        "mcp_servers": read_mcp(path),
        "claude_md_summary": read_claude_summary(path),
    })
}

/// Extract and parse the YAML frontmatter block into a JSON object.
fn parse_frontmatter(content: &str) -> Value {
    let trimmed = content.trim_start();
    let Some(rest) = trimmed.strip_prefix("---") else {
        return json!({});
    };
    let Some(end) = rest.find("\n---") else {
        return json!({});
    };
    serde_yaml::from_str::<Value>(&rest[..end]).unwrap_or_else(|_| json!({}))
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
        let Ok(content) = std::fs::read_to_string(entry.path().join("SKILL.md")) else {
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
                "url": cfg.get("url").cloned().unwrap_or(Value::Null),
                "source": "plugin",
                "status": "configured",
            })
        })
        .collect()
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
    fn inspects_a_plugin_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::create_dir_all(dir.join("agents")).unwrap();
        std::fs::write(dir.join("agents/sec.md"), "---\nname: sec\ndescription: d\nmodel: opus\nbackground: true\n---\nx").unwrap();
        std::fs::create_dir_all(dir.join("skills/chk")).unwrap();
        std::fs::write(dir.join("skills/chk/SKILL.md"), "---\nname: chk\ndescription: s\nuser-invocable: true\n---\nc").unwrap();
        std::fs::write(dir.join(".mcp.json"), r#"{"mcpServers":{"db":{"command":"npx"},"po-k":{"command":"evil"}}}"#).unwrap();
        std::fs::write(dir.join("CLAUDE.md"), "# Hello").unwrap();

        let v = inspect_plugin(&dir.to_string_lossy());
        assert_eq!(v["inspected"], true);
        assert_eq!(v["agents"][0]["name"], "sec");
        assert_eq!(v["agents"][0]["background"], true);
        assert_eq!(v["skills"][0]["name"], "chk");
        let names: Vec<&str> = v["mcp_servers"].as_array().unwrap().iter().map(|m| m["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"db") && names.contains(&"po-k"));
        assert_eq!(v["claude_md_summary"], "# Hello");

        let url = inspect_plugin("https://example.com/p.zip");
        assert_eq!(url["inspected"], false);
    }
}
