//! Stdio JSON-RPC 2.0 MCP server. Claude Code launches `po-k mcp` as a subprocess
//! via `claude mcp add po-k -- po-k mcp`; it pipes JSON-RPC requests on our stdin
//! and reads replies on our stdout. We expose memory + skills as read-only tools.

use anyhow::Result;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config;

const PROTOCOL_VERSION: &str = "2025-06-18";

pub async fn run() -> Result<()> {
    let cfg = config::load_effective()?;
    let repo_root = cfg
        .repo
        .as_ref()
        .map(|r| config::expand_path(&r.path))
        .unwrap_or_else(|| PathBuf::from("/dev/null"));

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
        let body = handle(&req, &repo_root).await;
        stdout.write_all(body.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
    }
    Ok(())
}

async fn handle(req: &Value, repo_root: &Path) -> String {
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
            match call_tool(name, &args, repo_root) {
                Ok(v) => jsonrpc_ok(id, v),
                Err((code, msg)) => jsonrpc_error(id, code, &msg),
            }
        }
        other => jsonrpc_error(id, -32601, &format!("method not found: {other}")),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "list_topics",
            "description": "List curated topic ids the team maintains living markdown digests for. Each topic has a question and an answer that po-k keeps up to date from Claude Code conversations. Returns id + first non-empty line of the digest as a hint.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "recall_topic",
            "description": "Read the current digest markdown for a topic. Returns the full file content. Use this BEFORE asking the user about a topic that may already have a maintained answer.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "The topic id (filename without .md)." }
                },
                "required": ["id"]
            }
        }),
        json!({
            "name": "list_skills",
            "description": "List skill ids: small procedural how-to documents the team has accumulated (e.g. how to deploy, how to restart a service). Returns id + first non-empty line.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "recall_skill",
            "description": "Read the full markdown for a named skill. Use when you need to perform a procedure the team has documented.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"]
            }
        }),
    ]
}

fn call_tool(name: &str, args: &Value, repo_root: &Path) -> Result<Value, (i64, String)> {
    match name {
        "list_topics" => list_dir(repo_root.join("memory"), "topics"),
        "list_skills" => list_dir(repo_root.join("skills"), "skills"),
        "recall_topic" => {
            let id = args.get("id").and_then(Value::as_str).unwrap_or("");
            recall_file(repo_root.join("memory"), id)
        }
        "recall_skill" => {
            let id = args.get("id").and_then(Value::as_str).unwrap_or("");
            recall_file(repo_root.join("skills"), id)
        }
        other => Err((-32601, format!("tool not found: {other}"))),
    }
}

fn list_dir(dir: PathBuf, kind: &str) -> Result<Value, (i64, String)> {
    let mut items: Vec<Value> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            let Some(id) = name.strip_suffix(".md") else {
                continue;
            };
            let hint = first_meaningful_line(&p).unwrap_or_default();
            items.push(json!({"id": id, "hint": hint}));
        }
    }
    items.sort_by(|a, b| {
        a.get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .cmp(b.get("id").and_then(Value::as_str).unwrap_or(""))
    });
    let text = serde_json::to_string_pretty(&items).unwrap_or_default();
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false,
        "structuredContent": { kind: items }
    }))
}

fn recall_file(dir: PathBuf, id: &str) -> Result<Value, (i64, String)> {
    if id.is_empty() || id.contains('/') || id.contains("..") {
        return Err((-32602, "invalid id".into()));
    }
    let path = dir.join(format!("{id}.md"));
    let text = std::fs::read_to_string(&path)
        .map_err(|e| (-32004, format!("not found: {} ({e})", path.display())))?;
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false,
        "structuredContent": { "id": id, "markdown": text }
    }))
}

fn first_meaningful_line(p: &Path) -> Option<String> {
    let text = std::fs::read_to_string(p).ok()?;
    text.lines()
        .map(|l| l.trim_start_matches('#').trim())
        .find(|l| !l.is_empty())
        .map(|s| {
            if s.chars().count() <= 140 {
                s.to_string()
            } else {
                let truncated: String = s.chars().take(139).collect();
                format!("{truncated}…")
            }
        })
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
