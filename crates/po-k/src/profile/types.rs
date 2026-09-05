//! Profile schema (v1 Xpo-k profiles). Kept so `po-k export-profile` can turn
//! the old SQLite profiles into CC plugin directories, and because
//! [`McpServer`] doubles as the `mcp_servers` entry shape on `POST /sessions`.

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_true() -> bool {
    true
}
fn default_stdio() -> String {
    "stdio".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Profile {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_md: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub agents: IndexMap<String, Agent>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub skills: IndexMap<String, Skill>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub mcp_servers: IndexMap<String, McpServer>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub hooks: IndexMap<String, Vec<HookGroup>>,
    #[serde(default)]
    pub settings: ProfileSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Agent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disallowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Skill {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when_to_use: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disallowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default = "default_true")]
    pub user_invocable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// One MCP server for Claude Code, in the shape of a `.mcp.json` entry.
/// Either a stdio server (`command` + `args` + `env`) or a remote one (`url`
/// + `headers`, with `type` = `http` or `sse`).
#[derive(Debug, Clone, Deserialize, Serialize, Default, JsonSchema)]
pub struct McpServer {
    /// Executable for a stdio server (e.g. `npx`, `node`, `/usr/bin/foo`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Environment for a stdio server. `${VAR}` values are expanded by CC.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub env: IndexMap<String, String>,
    /// URL of a remote (`http` / `sse`) server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Extra headers for a remote server (e.g. `Authorization`).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    /// `stdio` (default), `http` or `sse`.
    #[serde(default = "default_stdio", rename = "type")]
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HookGroup {
    #[serde(default)]
    pub matcher: String,
    pub hooks: Vec<HookCmd>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HookCmd {
    #[serde(rename = "type")]
    pub kind: String,
    pub command: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProfileSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_budget_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub env: IndexMap<String, String>,
    /// Any other CC settings.json key, passed through verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl Profile {
    pub fn from_json(v: &Value) -> Result<Self, String> {
        serde_json::from_value(v.clone()).map_err(|e| e.to_string())
    }
}
