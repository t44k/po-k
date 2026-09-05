//! `po-k mcp` — the MCP server an agent (Hermes) runs over stdio.
//!
//! It talks only to the **local** `po-k serve` (`POK_URL`, default
//! `http://127.0.0.1:13658`), which proxies to remote boxes and runs the
//! watchers. Every session tool takes a `host` (a connected box, or `local`).
//!
//! Compatibility: CC processes started before v0.12 launch `po-k mcp
//! --session-id … --base-url … --token-file …` as their permission shim; when
//! `--session-id` is present this command delegates to `cc-mcp`.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

use crate::core::sessions::CreateRequest;
use crate::mcp_stdio::{self, text_result, McpTools, ToolError};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Base URL of the local `po-k serve`.
    #[arg(long, env = "POK_URL", default_value = "http://127.0.0.1:13658")]
    pub url: String,
    /// Bearer token file (the fleet key).
    #[arg(long, env = "POK_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
    /// Bearer token value (overrides the file).
    #[arg(long, env = "POK_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// Default webhook URL for `wake`/`watch` when the call has none.
    #[arg(long, env = "POK_WEBHOOK_URL")]
    pub webhook_url: Option<String>,
    /// Env var name (of the `po-k serve` process) holding the webhook secret.
    #[arg(long, env = "POK_WEBHOOK_SECRET_ENV", default_value = "POK_WEBHOOK_SECRET")]
    pub webhook_secret_env: String,
    /// File (readable by `po-k serve`) holding the webhook secret; takes
    /// precedence over the env var name.
    #[arg(long, env = "POK_WEBHOOK_SECRET_FILE")]
    pub webhook_secret_file: Option<PathBuf>,
    /// Default `meta` JSON object attached to connects/watches.
    #[arg(long, env = "POK_META")]
    pub meta: Option<String>,
    /// Legacy permission-shim invocation (pre-0.12 sessions). Hidden.
    #[arg(long, hide = true)]
    pub session_id: Option<String>,
    #[arg(long, hide = true)]
    pub base_url: Option<String>,
}

pub struct AgentTools {
    http: reqwest::Client,
    base: String,
    token: String,
    default_webhook: Option<Value>,
    default_meta: Value,
}

fn s(v: &Value, key: &str) -> Result<String, ToolError> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ToolError::InvalidParams(format!("missing required parameter `{key}`")))
}

fn opt_s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

fn host_prop() -> Value {
    json!({ "type": "string", "description": "A connected box (as passed to `connect`), or `local` for the po-k next to you." })
}

fn sid_prop() -> Value {
    json!({ "type": "string", "description": "Session id (from `create` or `sessions`)." })
}

fn webhook_prop() -> Value {
    json!({ "type": "object", "description": "Override webhook: {url, secret_env | secret_file}. Defaults to POK_WEBHOOK_URL + POK_WEBHOOK_SECRET_ENV.",
        "properties": { "url": { "type": "string" }, "secret_env": { "type": "string" }, "secret_file": { "type": "string" } }, "required": ["url"] })
}

fn meta_prop() -> Value {
    json!({ "type": "object", "description": "Small JSON object echoed in every webhook (e.g. {platform, chat_id, thread_id}) so the wake-up can be routed back to this conversation." })
}

fn obj(props: Value, required: &[&str]) -> Value {
    json!({ "type": "object", "properties": props, "required": required, "additionalProperties": false })
}

fn tool(name: &str, description: &str, schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": schema })
}

impl AgentTools {
    async fn http(&self, method: reqwest::Method, path: &str, body: Option<Value>, timeout: Duration) -> Result<Value, ToolError> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.http.request(method.clone(), &url).bearer_auth(&self.token).timeout(timeout);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(text_result(
                    format!("cannot reach the local po-k at {}: {e}. Is `po-k serve` running on this box?", self.base),
                    true,
                    None,
                ))
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        let parsed: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
        if (200..300).contains(&status) {
            let pretty = serde_json::to_string_pretty(&parsed).unwrap_or_default();
            Ok(text_result(pretty, false, Some(parsed)))
        } else {
            Ok(text_result(
                format!("HTTP {status} from {} {path}: {}", method.as_str(), serde_json::to_string(&parsed).unwrap_or_default()),
                true,
                Some(parsed),
            ))
        }
    }

    fn get(&self, path: &str, timeout: u64) -> impl std::future::Future<Output = Result<Value, ToolError>> + '_ {
        let p = path.to_string();
        async move { self.http(reqwest::Method::GET, &p, None, Duration::from_secs(timeout)).await }
    }

    fn post(&self, path: &str, body: Value, timeout: u64) -> impl std::future::Future<Output = Result<Value, ToolError>> + '_ {
        let p = path.to_string();
        async move { self.http(reqwest::Method::POST, &p, Some(body), Duration::from_secs(timeout)).await }
    }

    fn delete(&self, path: &str) -> impl std::future::Future<Output = Result<Value, ToolError>> + '_ {
        let p = path.to_string();
        async move { self.http(reqwest::Method::DELETE, &p, None, Duration::from_secs(30)).await }
    }

    fn webhook_for(&self, args: &Value) -> Option<Value> {
        args.get("webhook").filter(|w| w.is_object()).cloned().or_else(|| self.default_webhook.clone())
    }

    fn meta_for(&self, args: &Value) -> Value {
        args.get("meta").filter(|m| m.is_object()).cloned().unwrap_or_else(|| self.default_meta.clone())
    }

    fn create_schema(&self) -> Value {
        let mut schema = CreateRequest::json_schema();
        if let Some(props) = schema.get_mut("properties").and_then(Value::as_object_mut) {
            props.insert("host".into(), host_prop());
            props.insert("wake".into(), json!({ "type": "boolean", "description": "Start a watch so a webhook fires when the turn finishes / needs input / ends (default true when a webhook is configured)." }));
            props.insert("webhook".into(), webhook_prop());
            props.insert("meta".into(), meta_prop());
        }
        if let Some(req) = schema.get_mut("required").and_then(Value::as_array_mut) {
            req.insert(0, json!("host"));
        }
        schema.as_object_mut().map(|o| o.remove("$schema"));
        schema
    }
}

const WAIT_TOOL_TIMEOUT: u64 = 660;

impl McpTools for AgentTools {
    fn server_name(&self) -> &str {
        "pok"
    }

    fn tools(&self) -> Vec<Value> {
        vec![
            tool("docs", "Read po-k's API description (routes, the create-session JSON Schema, defaults, cursor rules, webhook contract). Call once before your first `create`.", obj(json!({ "format": { "type": "string", "enum": ["json", "markdown"], "description": "default json" } }), &[])),
            tool("connect", "Connect a dev box so its Claude Code sessions can be driven. Probes it and remembers it. Set a webhook here to get woken when sessions on it finish.", obj(json!({ "host": { "type": "string", "description": "Box name (e.g. `jamail-c1` → jamail-c1.zrz), `host:port`, `http://host:port`, or `local`." }, "webhook": webhook_prop(), "meta": meta_prop() }), &["host"])),
            tool("hosts", "List connected boxes with their last contact and active watch counts.", obj(json!({}), &[])),
            tool("host", "One connected box with a live probe (version, sessions).", obj(json!({ "host": host_prop() }), &["host"])),
            tool("disconnect", "Forget a box and stop its watches.", obj(json!({ "host": host_prop() }), &["host"])),
            tool("create", "Start a Claude Code session on a box: directory, model, plugins, extra MCP servers, agent, system prompt. Returns the session (and a watch when `wake`). One live session per name; 409 tells you the existing session_id.", self.create_schema()),
            tool("sessions", "List running sessions on a box.", obj(json!({ "host": host_prop() }), &["host"])),
            tool("prompt", "Type a prompt into a session. Returns `cursor`: the boundary cursor to pass to `wait`.", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "text": { "type": "string" } }), &["host", "session_id", "text"])),
            tool("status", "Derived status (working | awaiting_input | idle | ended) plus cursor and boundary_cursor.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("wait", "Block until the session reaches a turn boundary newer than `since` (default: its current boundary). Returns status; `timed_out: true` means call again. Prefer a webhook (`wake`) for long tasks.", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "since": { "type": "integer", "description": "boundary cursor from `prompt`/`status`/a webhook; default: current boundary" }, "timeout": { "type": "integer", "description": "seconds, max 600 (default 600)" } }), &["host", "session_id"])),
            tool("events", "Read session events. Default: the latest 10 (offset=-1). Use `transcript_only` for just prompts/replies/tool calls. Keep `size` small — a full transcript can be huge.", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "offset": { "type": "integer", "description": "-1 = tail (default); or a cursor to page forward from" }, "size": { "type": "integer", "description": "default 10, max 1000" }, "wait": { "type": "integer", "description": "long-poll seconds when empty (default 2)" }, "follow": { "type": "boolean", "description": "with offset=-1: wait for NEW events only" }, "transcript_only": { "type": "boolean" } }), &["host", "session_id"])),
            tool("pane", "Raw zellij pane content — ground truth when events look wrong.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("interrupt", "Send ESC to interrupt the current turn.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("clear", "Send /clear to reset CC's context.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("upload", "Drop a file into the session's <cwd>/.po-k-inbox/. Pass content_base64, or file_path (read on this machine).", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "filename": { "type": "string" }, "content_base64": { "type": "string" }, "file_path": { "type": "string" } }), &["host", "session_id", "filename"])),
            tool("cost", "Token and cost totals.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("capabilities", "What the session loaded: plugins (agents, skills, MCP), settings, warnings.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("permission", "Answer a permission_request (see status.deciding_event or a needs_input webhook).", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "request_id": { "type": "string" }, "behavior": { "type": "string", "enum": ["allow", "deny"] }, "message": { "type": "string" } }), &["host", "session_id", "request_id", "behavior"])),
            tool("delete", "Stop a session and remove its zellij session.", obj(json!({ "host": host_prop(), "session_id": sid_prop() }), &["host", "session_id"])),
            tool("watch", "Watch an existing session: webhook on finished / needs_input / ended / connection_lost.", obj(json!({ "host": host_prop(), "session_id": sid_prop(), "webhook": webhook_prop(), "meta": meta_prop() }), &["host", "session_id"])),
            tool("unwatch", "Stop a watch.", obj(json!({ "watch_id": { "type": "string" } }), &["watch_id"])),
            tool("watches", "List watches (optionally by host / state).", obj(json!({ "host": { "type": "string" }, "state": { "type": "string", "enum": ["active", "done", "failed", "stopped"] } }), &[])),
        ]
    }

    async fn call(&self, name: &str, args: Value) -> Result<Value, ToolError> {
        let sp = |host: &str, sid: &str, tail: &str| format!("/hosts/{host}/sessions/{sid}{tail}");
        match name {
            "docs" => {
                if opt_s(&args, "format").as_deref() == Some("markdown") {
                    self.get("/help", 30).await
                } else {
                    self.get("/docs", 30).await
                }
            }
            "connect" => {
                let mut body = json!({ "host": s(&args, "host")? });
                if let Some(w) = self.webhook_for(&args) {
                    body["webhook"] = w;
                }
                let meta = self.meta_for(&args);
                if !meta.is_null() {
                    body["meta"] = meta;
                }
                self.post("/hosts", body, 30).await
            }
            "hosts" => self.get("/hosts", 30).await,
            "host" => self.get(&format!("/hosts/{}", s(&args, "host")?), 30).await,
            "disconnect" => self.delete(&format!("/hosts/{}", s(&args, "host")?)).await,
            "create" => {
                let host = s(&args, "host")?;
                let mut body = args.clone();
                let obj = body.as_object_mut().ok_or_else(|| ToolError::InvalidParams("arguments must be an object".into()))?;
                obj.remove("host");
                let wake = match obj.remove("wake") {
                    Some(Value::Bool(b)) => b,
                    Some(_) => return Err(ToolError::InvalidParams("wake must be a boolean".into())),
                    None => self.default_webhook.is_some() || args.get("webhook").is_some(),
                };
                obj.remove("webhook");
                obj.remove("meta");
                if wake {
                    let Some(w) = self.webhook_for(&args) else {
                        return Ok(text_result("wake requested but no webhook is configured: pass `webhook` or set POK_WEBHOOK_URL for `po-k mcp`. Pass wake=false to create without a watch.", true, None));
                    };
                    obj.insert("wake".into(), json!(true));
                    obj.insert("webhook".into(), w);
                    let meta = self.meta_for(&args);
                    if !meta.is_null() {
                        obj.insert("meta".into(), meta);
                    }
                }
                self.post(&format!("/hosts/{host}/sessions"), body, 90).await
            }
            "sessions" => self.get(&format!("/hosts/{}/sessions", s(&args, "host")?), 30).await,
            "prompt" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                self.post(&sp(&h, &sid, "/messages"), json!({ "text": s(&args, "text")? }), 180).await
            }
            "status" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                self.get(&sp(&h, &sid, "/status"), 30).await
            }
            "wait" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                let timeout = args.get("timeout").and_then(Value::as_u64).unwrap_or(600).min(600);
                let since = match args.get("since").and_then(Value::as_i64) {
                    Some(v) => v,
                    None => {
                        // Resolve the current boundary so a stale stop cannot satisfy the wait.
                        let st = self.get(&sp(&h, &sid, "/status"), 30).await?;
                        if st.get("isError") == Some(&json!(true)) {
                            return Ok(st);
                        }
                        st.get("structuredContent").and_then(|v| v.get("boundary_cursor")).and_then(Value::as_i64).unwrap_or(0)
                    }
                };
                let mut out = self.get(&sp(&h, &sid, &format!("/wait?since={since}&timeout={timeout}")), WAIT_TOOL_TIMEOUT).await?;
                if let Some(sc) = out.get_mut("structuredContent") {
                    sc["since_used"] = json!(since);
                }
                Ok(out)
            }
            "events" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                let offset = args.get("offset").and_then(Value::as_i64).unwrap_or(-1);
                let size = args.get("size").and_then(Value::as_i64).unwrap_or(10);
                let wait = args.get("wait").and_then(Value::as_u64).unwrap_or(2);
                let follow = args.get("follow").and_then(Value::as_bool).unwrap_or(false);
                let path = if args.get("transcript_only").and_then(Value::as_bool).unwrap_or(false) { "/messages" } else { "/events" };
                let q = format!("{path}?offset={offset}&size={size}&wait={wait}&follow={}", if follow { 1 } else { 0 });
                self.get(&sp(&h, &sid, &q), wait + 30).await
            }
            "pane" | "cost" | "capabilities" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                self.get(&sp(&h, &sid, &format!("/{name}")), 30).await
            }
            "interrupt" | "clear" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                self.post(&sp(&h, &sid, &format!("/{name}")), json!({}), 130).await
            }
            "upload" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                let filename = s(&args, "filename")?;
                let content = match (opt_s(&args, "content_base64"), opt_s(&args, "file_path")) {
                    (Some(c), _) => c,
                    (None, Some(p)) => {
                        use base64::Engine;
                        let bytes = std::fs::read(&p).map_err(|e| ToolError::InvalidParams(format!("reading {p}: {e}")))?;
                        base64::engine::general_purpose::STANDARD.encode(bytes)
                    }
                    (None, None) => return Err(ToolError::InvalidParams("pass content_base64 or file_path".into())),
                };
                self.post(&sp(&h, &sid, "/files"), json!({ "filename": filename, "content_base64": content }), 60).await
            }
            "permission" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                let rid = s(&args, "request_id")?;
                let mut body = json!({ "behavior": s(&args, "behavior")? });
                if let Some(m) = opt_s(&args, "message") {
                    body["message"] = json!(m);
                }
                self.post(&sp(&h, &sid, &format!("/permission_requests/{rid}")), body, 30).await
            }
            "delete" => {
                let (h, sid) = (s(&args, "host")?, s(&args, "session_id")?);
                self.delete(&sp(&h, &sid, "")).await
            }
            "watch" => {
                let mut body = json!({ "host": s(&args, "host")?, "session_id": s(&args, "session_id")? });
                if let Some(w) = self.webhook_for(&args) {
                    body["webhook"] = w;
                }
                let meta = self.meta_for(&args);
                if !meta.is_null() {
                    body["meta"] = meta;
                }
                self.post("/watches", body, 30).await
            }
            "unwatch" => self.delete(&format!("/watches/{}", s(&args, "watch_id")?)).await,
            "watches" => {
                let mut q = Vec::new();
                if let Some(h) = opt_s(&args, "host") {
                    q.push(format!("host={h}"));
                }
                if let Some(st) = opt_s(&args, "state") {
                    q.push(format!("state={st}"));
                }
                let qs = if q.is_empty() { String::new() } else { format!("?{}", q.join("&")) };
                self.get(&format!("/watches{qs}"), 30).await
            }
            other => Err(ToolError::UnknownTool(other.into())),
        }
    }
}

fn resolve_token(token: Option<String>, token_file: Option<PathBuf>) -> Result<String> {
    if let Some(t) = token.filter(|t| !t.trim().is_empty()) {
        return Ok(t.trim().to_string());
    }
    let path = token_file.unwrap_or_else(|| crate::config::expand_path(crate::defaults::TOKEN_FILE));
    Ok(crate::auth::Token::from_file(&path)?.raw().to_string())
}

pub async fn run(args: Args) -> Result<()> {
    if let Some(sid) = args.session_id {
        // Legacy permission-shim invocation from a pre-0.12 mcp.json.
        return super::cc_mcp::run(super::cc_mcp::Args {
            session_id: sid,
            base_url: args.base_url.context("--base-url is required with --session-id")?,
            token_file: args.token_file.context("--token-file is required with --session-id")?,
        })
        .await;
    }
    let token = resolve_token(args.token, args.token_file)?;
    let default_webhook = args.webhook_url.filter(|u| !u.is_empty()).map(|url| match &args.webhook_secret_file {
        Some(f) => json!({ "url": url, "secret_file": f.to_string_lossy() }),
        None => json!({ "url": url, "secret_env": args.webhook_secret_env }),
    });
    let default_meta = match args.meta.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
        Some(m) => serde_json::from_str(m).context("POK_META must be a JSON object")?,
        None => Value::Null,
    };
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .build()
        .context("building http client")?;
    let tools = AgentTools {
        http,
        base: args.url.trim_end_matches('/').to_string(),
        token,
        default_webhook,
        default_meta,
    };
    eprintln!("po-k mcp: serving {} tools against {}", tools.tools().len(), tools.base);
    mcp_stdio::serve(tools).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tools() -> AgentTools {
        AgentTools {
            http: reqwest::Client::new(),
            base: "http://127.0.0.1:1".into(),
            token: "t".into(),
            default_webhook: Some(json!({ "url": "http://h/w", "secret_env": "S" })),
            default_meta: json!({ "chat_id": "c" }),
        }
    }

    #[test]
    fn create_schema_merges_hub_fields_into_the_session_schema() {
        let t = tools();
        let schema = t.create_schema();
        let props = schema["properties"].as_object().unwrap();
        for k in ["host", "wake", "webhook", "meta", "cwd", "model", "plugins", "mcp_servers"] {
            assert!(props.contains_key(k), "missing {k}");
        }
        assert_eq!(schema["required"], json!(["host", "cwd"]));
        let all = t.tools();
        let names: Vec<&str> = all.iter().map(|x| x["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"create") && names.contains(&"wait") && names.contains(&"docs"));
        assert_eq!(names.len(), 22);
    }

    #[tokio::test]
    async fn unreachable_local_serve_is_a_tool_error_not_a_crash() {
        let out = tools().call("hosts", json!({})).await.unwrap();
        assert_eq!(out["isError"], true);
        assert!(out["content"][0]["text"].as_str().unwrap().contains("po-k serve"));
        let missing = tools().call("status", json!({ "host": "x" })).await;
        assert!(matches!(missing, Err(ToolError::InvalidParams(m)) if m.contains("session_id")));
        assert!(matches!(tools().call("nope", json!({})).await, Err(ToolError::UnknownTool(_))));
    }
}
