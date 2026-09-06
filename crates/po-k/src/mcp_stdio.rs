//! Minimal MCP server over stdio: newline-delimited JSON-RPC 2.0, the
//! `initialize` / `tools/list` / `tools/call` subset every MCP client uses.
//! Shared by `po-k mcp` (the agent-facing server) and `po-k cc-mcp` (the
//! permission shim Claude Code launches).

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub const PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Debug)]
pub enum ToolError {
    UnknownTool(String),
    InvalidParams(String),
}

#[allow(async_fn_in_trait)]
pub trait McpTools {
    fn server_name(&self) -> &str;
    /// MCP tool descriptors (`{name, description, inputSchema}`).
    fn tools(&self) -> Vec<Value>;
    /// Returns the full `tools/call` result object.
    async fn call(&self, name: &str, args: Value) -> Result<Value, ToolError>;
}

/// A `tools/call` result carrying text (and optionally structured content).
pub fn text_result(text: impl Into<String>, is_error: bool, structured: Option<Value>) -> Value {
    let mut v = json!({ "content": [{ "type": "text", "text": text.into() }], "isError": is_error });
    if let Some(s) = structured {
        v["structuredContent"] = s;
    }
    v
}

pub fn jsonrpc_ok(id: Value, result: Value) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "result": result })).unwrap_or_default()
}

pub fn jsonrpc_error(id: Value, code: i64, message: &str) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }))
        .unwrap_or_default()
}

/// Handle one request. `None` for notifications (no id), which get no reply.
pub async fn handle<T: McpTools>(tools: &T, req: &Value) -> Option<String> {
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let id = req.get("id").cloned().filter(|v| !v.is_null())?;
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    let out = match method {
        "initialize" => jsonrpc_ok(
            id,
            json!({
                "protocolVersion": params.get("protocolVersion").cloned().unwrap_or_else(|| json!(PROTOCOL_VERSION)),
                "capabilities": { "tools": {} },
                "serverInfo": { "name": tools.server_name(), "version": env!("CARGO_PKG_VERSION") }
            }),
        ),
        "ping" => jsonrpc_ok(id, json!({})),
        "tools/list" => jsonrpc_ok(id, json!({ "tools": tools.tools() })),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match tools.call(name, args).await {
                Ok(result) => jsonrpc_ok(id, result),
                Err(ToolError::UnknownTool(n)) => jsonrpc_error(id, -32601, &format!("tool not found: {n}")),
                // Reported as a tool result, not a protocol error: the model
                // reads it and fixes the call.
                Err(ToolError::InvalidParams(m)) => jsonrpc_ok(id, text_result(format!("invalid parameters for `{name}`: {m}"), true, None)),
            }
        }
        other => jsonrpc_error(id, -32601, &format!("method not found: {other}")),
    };
    Some(out)
}

/// Read requests from stdin until EOF, writing one response line each.
pub async fn serve<T: McpTools>(tools: T) -> Result<()> {
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                stdout.write_all(jsonrpc_error(Value::Null, -32700, &format!("parse error: {e}")).as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
                continue;
            }
        };
        if let Some(body) = handle(&tools, &req).await {
            stdout.write_all(body.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl McpTools for Fake {
        fn server_name(&self) -> &str {
            "fake"
        }
        fn tools(&self) -> Vec<Value> {
            vec![json!({ "name": "echo", "inputSchema": { "type": "object" } })]
        }
        async fn call(&self, name: &str, args: Value) -> Result<Value, ToolError> {
            match name {
                "echo" => Ok(text_result(args.to_string(), false, Some(args))),
                "needs" => Err(ToolError::InvalidParams("missing x".into())),
                other => Err(ToolError::UnknownTool(other.into())),
            }
        }
    }

    #[tokio::test]
    async fn initialize_tools_list_and_call_roundtrip() {
        let init = handle(&Fake, &json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": "2024-11-05" } }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&init).unwrap();
        assert_eq!(v["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(v["result"]["serverInfo"]["name"], "fake");
        // Notifications get no reply.
        assert!(handle(&Fake, &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })).await.is_none());
        let list = handle(&Fake, &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" })).await.unwrap();
        let v: Value = serde_json::from_str(&list).unwrap();
        assert_eq!(v["result"]["tools"][0]["name"], "echo");
        let call = handle(&Fake, &json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": { "name": "echo", "arguments": { "a": 1 } } }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&call).unwrap();
        assert_eq!(v["result"]["structuredContent"]["a"], 1);
        assert_eq!(v["result"]["isError"], false);
        let bad = handle(&Fake, &json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": { "name": "nope" } })).await.unwrap();
        let v: Value = serde_json::from_str(&bad).unwrap();
        assert_eq!(v["error"]["code"], -32601);
        let invalid = handle(&Fake, &json!({ "jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": { "name": "needs", "arguments": {} } })).await.unwrap();
        let v: Value = serde_json::from_str(&invalid).unwrap();
        assert_eq!(v["result"]["isError"], true);
        assert!(v["result"]["content"][0]["text"].as_str().unwrap().contains("invalid parameters for `needs`: missing x"));
    }
}
