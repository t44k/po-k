//! `po-k mcp --session-id <sid> --base-url <url> --token-file <path>`
//!
//! Stdio JSON-RPC 2.0 MCP server. CC spawns this as a child via the per-session
//! `mcp.json` (`{"command":"po-k","args":["mcp","--session-id",…]}`) and uses
//! the single `approve` tool to delegate permission decisions to po-k.
//!
//! The `approve` handler HTTP-POSTs to
//! `<base_url>/sessions/<sid>/mcp/approve {tool_name, input}` and blocks until
//! po-k server responds (which itself blocks until the orchestrator answers,
//! or `cc.permission_timeout` fires → default-deny).

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const PROTOCOL_VERSION: &str = "2025-06-18";
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long)]
    pub session_id: String,
    #[arg(long)]
    pub base_url: String,
    #[arg(long)]
    pub token_file: std::path::PathBuf,
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
        .timeout(HTTP_TIMEOUT)
        .build()
        .context("building http client")?;

    let approve_url = format!(
        "{}/sessions/{}/mcp/approve",
        args.base_url.trim_end_matches('/'),
        args.session_id
    );

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                let body = jsonrpc_error(None, -32700, &format!("parse error: {e}"));
                stdout.write_all(body.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                continue;
            }
        };
        let body = handle(&req, &http, &approve_url, &token).await;
        stdout.write_all(body.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
    }
    Ok(())
}

async fn handle(req: &Value, http: &reqwest::Client, approve_url: &str, token: &str) -> String {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => jsonrpc_ok(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "po-k",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
        ),
        "notifications/initialized" | "ping" => jsonrpc_ok(id, json!({})),
        "tools/list" => jsonrpc_ok(id, json!({ "tools": tool_definitions() })),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(Value::Null);
            match name {
                "approve" => match call_approve(http, approve_url, token, &args).await {
                    Ok(v) => jsonrpc_ok(id, v),
                    Err(e) => jsonrpc_error(id, -32603, &format!("approve failed: {e}")),
                },
                other => jsonrpc_error(id, -32601, &format!("tool not found: {other}")),
            }
        }
        other => jsonrpc_error(id, -32601, &format!("method not found: {other}")),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![json!({
        "name": "approve",
        "description": "Decide whether Claude Code may run a given tool call. Routed to the po-k server, which forwards the question to the orchestrator and returns its decision. The decision is one of `allow` or `deny`; on `deny` you should not invoke the tool.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool_name": { "type": "string", "description": "The tool CC wants to run, e.g. `Bash`." },
                "input":     { "type": "object", "description": "Tool-specific arguments (passed through verbatim)." }
            },
            "required": ["tool_name"]
        }
    })]
}

async fn call_approve(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    args: &Value,
) -> Result<Value> {
    let tool_name = args
        .get("tool_name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing tool_name"))?;
    let input = args.get("input").cloned().unwrap_or(Value::Null);
    let body = json!({ "tool_name": tool_name, "input": input });

    let resp = http
        .post(url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("reading approve response body")?;
    if !status.is_success() {
        anyhow::bail!("server returned {status}: {text}");
    }
    let decision: Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing approve response: {text}"))?;
    Ok(json!({
        "content": [{ "type": "text", "text": serde_json::to_string(&decision).unwrap_or_default() }],
        "isError": decision.get("behavior").and_then(Value::as_str) == Some("deny"),
        "structuredContent": decision
    }))
}

fn jsonrpc_ok(id: Option<Value>, result: Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
    .unwrap_or_default()
}

fn jsonrpc_error(id: Option<Value>, code: i64, message: &str) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    }))
    .unwrap_or_default()
}
