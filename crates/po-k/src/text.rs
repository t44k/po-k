//! Tiny stopword-aware text helpers used for two things:
//!   - deciding whether a topic's question overlaps with a turn's text (so we
//!     don't bother the LLM with topics that have no plausible evidence);
//!   - extracting the human-readable content out of a JSONL event line.

/// Lowercase a string and split on non-alphanumerics; drop short tokens and
/// English stopwords; return a HashSet for cheap intersection.
pub fn tokens(s: &str) -> std::collections::HashSet<String> {
    s.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
        .map(|w| w.to_lowercase())
        .filter(|w| w.len() >= 4 && !is_stopword(w))
        .collect()
}

/// 0.0–1.0 token-overlap score: |a ∩ b| / |a|.
pub fn overlap(question: &str, text: &str) -> f32 {
    let q = tokens(question);
    if q.is_empty() {
        return 0.0;
    }
    let t = tokens(text);
    let hits = q.iter().filter(|w| t.contains(*w)).count() as f32;
    hits / q.len() as f32
}

pub fn is_stopword(w: &str) -> bool {
    matches!(
        w,
        "what" | "which" | "where" | "when" | "have" | "been" | "from"
            | "with" | "this" | "that" | "these" | "those" | "into" | "about"
            | "your" | "yours" | "ours" | "their" | "them" | "they" | "were"
            | "will" | "would" | "could" | "should" | "must"
            | "does" | "doing" | "done" | "very" | "much" | "more" | "most"
            | "some" | "such" | "also" | "than" | "then" | "just"
            | "only" | "even" | "still" | "back" | "down" | "over" | "under"
            | "above" | "below" | "after" | "before" | "between"
            | "team" | "team's" | "uses" | "used"
    )
}

/// Pull the *interesting* text out of an event's raw JSONL line so the evidence
/// we hand the LLM is human-readable instead of escaped JSON.
pub fn extract_searchable(raw: &str) -> String {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return truncate(raw, 1500);
    };
    let kind = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    let out = match kind {
        "user" => user_text(&v),
        "assistant" => assistant_text(&v),
        "system" => v
            .get("content")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        _ => raw.to_string(),
    };
    if out.trim().is_empty() {
        truncate(raw, 1500)
    } else {
        truncate(&out, 1500)
    }
}

fn user_text(v: &serde_json::Value) -> String {
    let Some(content) = v.pointer("/message/content") else {
        return String::new();
    };
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(items) => {
            let mut buf = String::new();
            for item in items {
                let t = item.get("type").and_then(|x| x.as_str()).unwrap_or("");
                let text = if t == "text" {
                    item.get("text").and_then(|x| x.as_str()).map(str::to_string)
                } else if t == "tool_result" {
                    let inner = item.get("content");
                    Some(match inner {
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(serde_json::Value::Array(arr)) => arr
                            .iter()
                            .filter_map(|x| x.get("text").and_then(|y| y.as_str()))
                            .collect::<Vec<_>>()
                            .join("\n"),
                        _ => String::new(),
                    })
                } else {
                    None
                };
                if let Some(text) = text {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&text);
                }
            }
            buf
        }
        _ => String::new(),
    }
}

fn assistant_text(v: &serde_json::Value) -> String {
    let Some(items) = v.pointer("/message/content").and_then(|x| x.as_array()) else {
        return String::new();
    };
    let mut buf = String::new();
    for item in items {
        if item.get("type").and_then(|x| x.as_str()) == Some("text") {
            if let Some(text) = item.get("text").and_then(|x| x.as_str()) {
                if !buf.is_empty() {
                    buf.push('\n');
                }
                buf.push_str(text);
            }
        }
    }
    buf
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
