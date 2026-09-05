//! `po-k cc-mcp --session-id <sid> --base-url <url> --token-file <path>`
//!
//! The per-session MCP server Claude Code launches (from the generated
//! `mcp.json`). Its single `approve` tool POSTs to
//! `<base_url>/sessions/<sid>/mcp/approve` and blocks until the orchestrator
//! answers (or po-k auto-denies after the permission timeout).

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

use crate::mcp_stdio::{self, McpTools, ToolError};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long)]
    pub session_id: String,
    #[arg(long)]
    pub base_url: String,
    #[arg(long)]
    pub token_file: PathBuf,
}

struct PermissionShim {
    http: reqwest::Client,
    approve_url: String,
    token: String,
}

impl McpTools for PermissionShim {
    fn server_name(&self) -> &str {
        "po-k"
    }

    fn tools(&self) -> Vec<Value> {
        vec![json!({
            "name": "approve",
            "description": "Decide whether Claude Code may run a given tool call. Routed to po-k, which forwards the question to the orchestrator and returns its decision (`allow` or `deny`).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool_name": { "type": "string", "description": "The tool CC wants to run, e.g. `Bash`." },
                    "input": { "type": "object", "description": "Tool-specific arguments (passed through verbatim)." }
                },
                "required": ["tool_name"]
            }
        })]
    }

    async fn call(&self, name: &str, args: Value) -> Result<Value, ToolError> {
        if name != "approve" {
            return Err(ToolError::UnknownTool(name.into()));
        }
        let tool_name = args
            .get("tool_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidParams("missing tool_name".into()))?;
        let input = args.get("input").cloned().unwrap_or(Value::Null);
        let body = json!({ "tool_name": tool_name, "input": input });
        let resp = self.http.post(&self.approve_url).bearer_auth(&self.token).json(&body).send().await;
        let decision: Value = match resp {
            Ok(r) if r.status().is_success() => r.json().await.unwrap_or(json!({ "behavior": "deny", "message": "unparseable decision" })),
            Ok(r) => json!({ "behavior": "deny", "message": format!("po-k returned HTTP {}", r.status().as_u16()) }),
            Err(e) => json!({ "behavior": "deny", "message": format!("po-k unreachable: {e}") }),
        };
        let deny = decision.get("behavior").and_then(Value::as_str) == Some("deny");
        Ok(mcp_stdio::text_result(decision.to_string(), deny, Some(decision)))
    }
}

pub async fn run(args: Args) -> Result<()> {
    let token = std::fs::read_to_string(&args.token_file)
        .with_context(|| format!("reading {}", args.token_file.display()))?
        .trim()
        .to_string();
    if token.is_empty() {
        anyhow::bail!("token file is empty");
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(crate::defaults::PERMISSION_TIMEOUT.as_secs() + 30))
        .build()
        .context("building http client")?;
    let approve_url = format!("{}/sessions/{}/mcp/approve", args.base_url.trim_end_matches('/'), args.session_id);
    mcp_stdio::serve(PermissionShim { http, approve_url, token }).await
}
