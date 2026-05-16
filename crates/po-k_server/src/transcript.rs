//! Turn the raw event stream into rendered transcript HTML snippets.
//!
//! Each `Turn` knows how to render itself to a small HTML string. The askama
//! templates just iterate those strings and emit them with `|safe`. That keeps
//! the templating layer simple (no recursive includes, no pattern matching in
//! Jinja) and lets us use real Rust for the structural decisions (pairing
//! tool_use with tool_result, splicing subagents in place).

use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;
use std::fmt::Write as _;

#[derive(Debug, Clone)]
pub enum Turn {
    UserText {
        text: String,
        ts: Option<String>,
    },
    AssistantText {
        text: String,
        ts: Option<String>,
    },
    System {
        text: String,
    },
    Tool {
        name: String,
        summary: String,
        input_pretty: String,
        result: Option<ToolResult>,
    },
    Subagent {
        agent_type: String,
        description: String,
        event_count: usize,
        inner: Vec<Turn>,
    },
}

#[derive(Debug, Clone)]
pub struct ToolResult {
    pub text: String,
    pub is_error: bool,
}

#[derive(Debug, Clone, Default)]
struct PendingSubagent {
    fallback_desc: String,
    meta_type: Option<String>,
    meta_desc: Option<String>,
    event_count: usize,
    inner: Vec<Turn>,
}

impl Turn {
    pub fn to_html(&self) -> String {
        let mut buf = String::new();
        self.write_html(&mut buf);
        buf
    }

    fn write_html(&self, buf: &mut String) {
        match self {
            Turn::UserText { text, ts } => {
                let _ = write!(
                    buf,
                    r#"<div class="turn user"><header class="role">user · <span class="muted">{}</span></header>{}</div>"#,
                    html_escape(ts.as_deref().unwrap_or("")),
                    html_escape(text)
                );
            }
            Turn::AssistantText { text, ts } => {
                let _ = write!(
                    buf,
                    r#"<div class="turn assistant"><header class="role">assistant · <span class="muted">{}</span></header>{}</div>"#,
                    html_escape(ts.as_deref().unwrap_or("")),
                    html_escape(text)
                );
            }
            Turn::System { text } => {
                let _ = write!(
                    buf,
                    r#"<div class="turn system"><header class="role">system</header>{}</div>"#,
                    html_escape(text)
                );
            }
            Turn::Tool {
                name,
                summary,
                input_pretty,
                result,
            } => {
                let _ = write!(
                    buf,
                    r#"<details class="tool"><summary><span class="tool-name">{}</span> <span class="muted">— {}</span></summary><pre class="payload">{}</pre>"#,
                    html_escape(name),
                    html_escape(summary),
                    html_escape(input_pretty)
                );
                if let Some(r) = result {
                    let _ = write!(
                        buf,
                        r#"<header class="role" style="margin-top:8px">result{}</header><pre class="payload">{}</pre>"#,
                        if r.is_error {
                            r#" <span style="color:var(--warn)">(error)</span>"#
                        } else {
                            ""
                        },
                        html_escape(&r.text)
                    );
                }
                buf.push_str("</details>");
            }
            Turn::Subagent {
                agent_type,
                description,
                event_count,
                inner,
            } => {
                let _ = write!(
                    buf,
                    r#"<details class="subagent"><summary><span class="tool-name">subagent · {}</span> <span class="muted">— {} · {} events</span></summary>"#,
                    html_escape(agent_type),
                    html_escape(description),
                    event_count
                );
                for t in inner {
                    t.write_html(buf);
                }
                buf.push_str("</details>");
            }
        }
    }
}

/// Build the turn HTML list from raw DB rows.
///
/// * `main_rows`: ordered main-session events (already paginated).
/// * `side_rows`: ALL sidechain events for this session, ordered by (agent_id, line_no).
/// * `meta_rows`: subagent meta (currently unused; reserved for later).
pub fn build_turns_html(
    main_rows: Vec<sqlx::sqlite::SqliteRow>,
    side_rows: Vec<sqlx::sqlite::SqliteRow>,
    meta_rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Vec<String> {
    // Pair tool_results to their tool_use_id via main_rows.
    let tool_results = collect_tool_results(&main_rows);

    // Meta lookup keyed by agent_id (extracted from `agent_file` path basename).
    let mut meta_by_agent_id: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
    for row in &meta_rows {
        let agent_file: String = row.try_get("agent_file").unwrap_or_default();
        let agent_type: Option<String> = row.try_get("agent_type").ok();
        let description: Option<String> = row.try_get("description").ok();
        if let Some(id) = agent_id_from_path(&agent_file) {
            meta_by_agent_id.insert(id, (agent_type, description));
        }
    }

    // Group sidechain rows by agent_id, then turn each group into a subagent block.
    let mut grouped: HashMap<String, Vec<sqlx::sqlite::SqliteRow>> = HashMap::new();
    for row in side_rows {
        let agent_id: String = row.try_get("agent_id").unwrap_or_default();
        grouped.entry(agent_id).or_default().push(row);
    }

    // Index of pending subagent blocks. Keyed by the normalized prompt that the
    // parent Agent tool_use carries — we know the subagent's first user message is
    // that same prompt (verified empirically), so this is a reliable join key.
    // `meta_type` / `meta_desc` come from the meta.json sidecar when available and
    // override the prompt-derived fallback.
    let mut by_prompt_queue: HashMap<String, Vec<PendingSubagent>> = HashMap::new();
    for (agent_id, rows) in grouped.into_iter() {
        let count = rows.len();
        let first_prompt = first_user_text(&rows).unwrap_or_default();
        let prompt_key = normalize_prompt(&first_prompt);
        let fallback_desc = truncate(&first_nonblank_line(&first_prompt), 140);
        let inner = rows
            .iter()
            .filter_map(|r| subagent_row_to_turn(r))
            .collect::<Vec<_>>();
        let (meta_type, meta_desc) = meta_by_agent_id.get(&agent_id).cloned().unwrap_or_default();
        by_prompt_queue.entry(prompt_key).or_default().push(PendingSubagent {
            fallback_desc,
            meta_type,
            meta_desc,
            event_count: count,
            inner,
        });
    }

    let mut out: Vec<String> = Vec::new();
    for row in &main_rows {
        let kind: String = row.try_get("kind").unwrap_or_default();
        let raw: String = row.try_get("raw").unwrap_or_default();
        let ts: Option<String> = row.try_get("timestamp").ok();
        match kind.as_str() {
            "user" => {
                if let Some(t) = render_user_row(&raw, ts) {
                    out.push(t.to_html());
                }
            }
            "assistant" => {
                let Ok(v) = serde_json::from_str::<Value>(&raw) else { continue };
                let Some(items) = v.pointer("/message/content").and_then(Value::as_array) else {
                    continue;
                };
                for item in items {
                    let t = item.get("type").and_then(Value::as_str).unwrap_or_default();
                    if t == "text" {
                        if let Some(text) = item.get("text").and_then(Value::as_str) {
                            if !text.trim().is_empty() {
                                out.push(
                                    Turn::AssistantText {
                                        text: text.to_string(),
                                        ts: ts.clone(),
                                    }
                                    .to_html(),
                                );
                            }
                        }
                    } else if t == "tool_use" {
                        out.push(build_tool_use(item, &tool_results, &mut by_prompt_queue).to_html());
                    }
                }
            }
            "system" => {
                let Ok(v) = serde_json::from_str::<Value>(&raw) else { continue };
                if let Some(content) = v.get("content").and_then(Value::as_str) {
                    out.push(
                        Turn::System {
                            text: content.to_string(),
                        }
                        .to_html(),
                    );
                }
            }
            _ => { /* noise event kinds: kept in db, skipped in transcript v1 */ }
        }
    }
    out
}

fn collect_tool_results(rows: &[sqlx::sqlite::SqliteRow]) -> HashMap<String, ToolResult> {
    let mut out = HashMap::new();
    for row in rows {
        let kind: String = row.try_get("kind").unwrap_or_default();
        if kind != "user" {
            continue;
        }
        let raw: String = row.try_get("raw").unwrap_or_default();
        let Ok(v) = serde_json::from_str::<Value>(&raw) else { continue };
        let Some(items) = v.pointer("/message/content").and_then(Value::as_array) else {
            continue;
        };
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let Some(id) = item.get("tool_use_id").and_then(Value::as_str) else { continue };
            let is_error = item.get("is_error").and_then(Value::as_bool).unwrap_or(false);
            let text = extract_text_content(item.get("content"));
            out.insert(id.to_string(), ToolResult { text, is_error });
        }
    }
    out
}

/// Return the verbatim text of the first user message in a (sub)agent's rows.
/// For subagents this is the prompt that spawned them.
fn first_user_text(rows: &[sqlx::sqlite::SqliteRow]) -> Option<String> {
    for row in rows {
        let kind: String = row.try_get("kind").ok()?;
        if kind != "user" {
            continue;
        }
        let raw: String = row.try_get("raw").ok()?;
        let v: Value = serde_json::from_str(&raw).ok()?;
        let content = v.pointer("/message/content")?;
        let text = match content {
            Value::String(s) => s.clone(),
            Value::Array(items) => {
                let mut buf = String::new();
                for item in items {
                    if item.get("type").and_then(Value::as_str) == Some("text") {
                        if let Some(t) = item.get("text").and_then(Value::as_str) {
                            if !buf.is_empty() {
                                buf.push('\n');
                            }
                            buf.push_str(t);
                        }
                    }
                }
                buf
            }
            _ => String::new(),
        };
        if text.trim().is_empty() {
            continue;
        }
        return Some(text);
    }
    None
}

/// Normalize a prompt to a stable matching key: collapse whitespace, take the first 256 chars.
fn normalize_prompt(s: &str) -> String {
    let collapsed: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(256).collect()
}

/// Extract `<id>` from a path ending in `agent-<id>.jsonl`.
fn agent_id_from_path(path: &str) -> Option<String> {
    let basename = path.rsplit('/').next()?;
    let stem = basename.strip_suffix(".jsonl")?;
    let id = stem.strip_prefix("agent-")?;
    Some(id.to_string())
}

fn render_user_row(raw: &str, ts: Option<String>) -> Option<Turn> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let content = v.pointer("/message/content")?;
    match content {
        Value::String(s) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(Turn::UserText {
                    text: s.clone(),
                    ts,
                })
            }
        }
        Value::Array(items) => {
            let mut buf = String::new();
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        if !buf.is_empty() {
                            buf.push('\n');
                        }
                        buf.push_str(text);
                    }
                }
            }
            if buf.trim().is_empty() {
                None
            } else {
                Some(Turn::UserText { text: buf, ts })
            }
        }
        _ => None,
    }
}

fn subagent_row_to_turn(row: &sqlx::sqlite::SqliteRow) -> Option<Turn> {
    let kind: String = row.try_get("kind").ok()?;
    let raw: String = row.try_get("raw").ok()?;
    let ts: Option<String> = row.try_get("timestamp").ok();
    match kind.as_str() {
        "user" => render_user_row(&raw, ts),
        "assistant" => {
            let v: Value = serde_json::from_str(&raw).ok()?;
            let items = v.pointer("/message/content")?.as_array()?;
            let mut buf = String::new();
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        if !buf.is_empty() {
                            buf.push('\n');
                        }
                        buf.push_str(text);
                    }
                }
            }
            if buf.trim().is_empty() {
                None
            } else {
                Some(Turn::AssistantText { text: buf, ts })
            }
        }
        _ => None,
    }
}

fn build_tool_use(
    item: &Value,
    tool_results: &HashMap<String, ToolResult>,
    by_prompt_queue: &mut HashMap<String, Vec<PendingSubagent>>,
) -> Turn {
    let name = item
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
    let input = item.get("input").cloned().unwrap_or(Value::Null);
    let input_pretty = serde_json::to_string_pretty(&input).unwrap_or_default();
    let summary = one_line_input_summary(&name, &input);
    let result = tool_results.get(id).cloned();

    if name == "Agent" {
        let prompt = input
            .get("prompt")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let key = normalize_prompt(prompt);
        let parent_description = input
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let parent_agent_type = input
            .get("subagent_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Some(q) = by_prompt_queue.get_mut(&key) {
            if let Some(p) = q.pop() {
                // Precedence for the rendered labels: subagent's meta.json (the
                // authoritative server-side sidecar) > parent Agent call's input >
                // prompt-derived fallback.
                let agent_type = p
                    .meta_type
                    .unwrap_or_else(|| {
                        if parent_agent_type.is_empty() {
                            "Agent".to_string()
                        } else {
                            parent_agent_type
                        }
                    });
                let description = p
                    .meta_desc
                    .unwrap_or_else(|| {
                        if parent_description.is_empty() {
                            p.fallback_desc
                        } else {
                            parent_description
                        }
                    });
                return Turn::Subagent {
                    agent_type,
                    description,
                    event_count: p.event_count,
                    inner: p.inner,
                };
            }
        }
        // No matching subagent transcript yet: render as a tool_use so the parent
        // call is still visible. Useful when a subagent is still running.
    }

    Turn::Tool {
        name,
        summary,
        input_pretty,
        result,
    }
}

fn one_line_input_summary(name: &str, input: &Value) -> String {
    let candidate = match name {
        "Bash" => input.get("command"),
        "Read" | "Write" | "Edit" => input.get("file_path"),
        "Grep" | "Glob" => input.get("pattern"),
        "WebFetch" | "WebSearch" => input.get("query").or_else(|| input.get("url")),
        "Agent" => input.get("description"),
        _ => None,
    };
    if let Some(s) = candidate.and_then(Value::as_str) {
        return truncate(s, 140);
    }
    let one_line = first_nonblank_line(&input.to_string());
    truncate(&one_line, 140)
}

fn extract_text_content(v: Option<&Value>) -> String {
    let Some(v) = v else { return String::new() };
    match v {
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut buf = String::new();
            for item in items {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(text);
                }
            }
            buf
        }
        other => other.to_string(),
    }
}

fn first_nonblank_line(s: &str) -> String {
    s.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}
