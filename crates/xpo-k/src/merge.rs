//! Profile merge engine (spec §3.3). Xpo-k merges N profiles into one before
//! sending the result to po-k — po-k never merges.
//!
//! - claude_md: concatenation with a `## From profile: <name>` section header
//! - agents / skills / mcp_servers / hooks: union by name, later wins
//! - settings: deep merge, later wins on scalar collision
//! - tags: deduplicated union; name / description: last wins

use pok_proto::profile::{Profile, ProfileSettings};
use serde_json::Value;

pub fn merge(profiles: &[Profile]) -> Profile {
    let mut out = Profile::default();
    let mut claude_sections: Vec<String> = Vec::new();

    for p in profiles {
        out.name = p.name.clone();
        if p.description.is_some() {
            out.description = p.description.clone();
        }
        for t in &p.tags {
            if !out.tags.contains(t) {
                out.tags.push(t.clone());
            }
        }
        if let Some(md) = &p.claude_md {
            claude_sections.push(format!("---\n## From profile: {}\n\n{}", p.name, md));
        }
        for (k, v) in &p.agents {
            out.agents.insert(k.clone(), v.clone());
        }
        for (k, v) in &p.skills {
            out.skills.insert(k.clone(), v.clone());
        }
        for (k, v) in &p.mcp_servers {
            out.mcp_servers.insert(k.clone(), v.clone());
        }
        for (k, v) in &p.hooks {
            out.hooks.insert(k.clone(), v.clone());
        }
        out.settings = merge_settings(&out.settings, &p.settings);
    }

    if !claude_sections.is_empty() {
        out.claude_md = Some(claude_sections.join("\n\n"));
    }
    out
}

fn merge_settings(base: &ProfileSettings, next: &ProfileSettings) -> ProfileSettings {
    let mut out = base.clone();
    if next.model.is_some() {
        out.model = next.model.clone();
    }
    if next.effort.is_some() {
        out.effort = next.effort.clone();
    }
    if next.permission_mode.is_some() {
        out.permission_mode = next.permission_mode.clone();
    }
    if next.max_turns.is_some() {
        out.max_turns = next.max_turns;
    }
    if next.max_budget_usd.is_some() {
        out.max_budget_usd = next.max_budget_usd;
    }
    for (k, v) in &next.env {
        out.env.insert(k.clone(), v.clone());
    }
    // Deep-merge the passthrough `extra` JSON objects.
    let mut extra = Value::Object(out.extra.clone());
    deep_merge(&mut extra, &Value::Object(next.extra.clone()));
    if let Value::Object(m) = extra {
        out.extra = m;
    }
    out
}

/// Recursively merge `b` into `a`: objects merge key-by-key, everything else
/// is replaced by `b` (last wins).
fn deep_merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(am), Value::Object(bm)) => {
            for (k, bv) in bm {
                deep_merge(am.entry(k.clone()).or_insert(Value::Null), bv);
            }
        }
        (a, b) => *a = b.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(v: Value) -> Profile {
        Profile::from_json(&v).unwrap()
    }

    #[test]
    fn merges_three_profiles_per_spec_appendix() {
        let base = p(json!({
            "name": "base-coding",
            "claude_md": "# Coding Standards",
            "skills": { "conventional-commits": { "content": "c" } },
            "settings": { "effort": "high" }
        }));
        let reviewer = p(json!({
            "name": "code-reviewer",
            "claude_md": "# Review Protocol",
            "agents": {
                "security-reviewer": { "model": "opus" },
                "perf-reviewer": { "model": "sonnet" }
            },
            "skills": { "review-checklist": { "content": "r" } },
            "settings": { "model": "opus", "effort": "high" }
        }));
        let acme = p(json!({
            "name": "acme-api",
            "claude_md": "# Acme API Project",
            "skills": { "lookup-user-by-email": { "content": "l" } },
            "settings": { "model": "sonnet", "effort": "high", "permission_mode": "bypassPermissions" }
        }));

        let m = merge(&[base, reviewer, acme]);
        // model: acme (last) wins over reviewer's opus
        assert_eq!(m.settings.model.as_deref(), Some("sonnet"));
        assert_eq!(m.settings.effort.as_deref(), Some("high"));
        assert_eq!(m.settings.permission_mode.as_deref(), Some("bypassPermissions"));
        // all 3 skills + both agents survive (unique names)
        assert_eq!(m.skills.len(), 3);
        assert_eq!(m.agents.len(), 2);
        // CLAUDE.md carries all three section headers in order
        let md = m.claude_md.unwrap();
        assert!(md.contains("## From profile: base-coding"));
        assert!(md.contains("## From profile: code-reviewer"));
        assert!(md.contains("## From profile: acme-api"));
        assert!(md.find("base-coding").unwrap() < md.find("acme-api").unwrap());
        // name = last
        assert_eq!(m.name, "acme-api");
    }

    #[test]
    fn later_agent_wins_on_collision() {
        let a = p(json!({ "name": "a", "agents": { "x": { "model": "opus" } } }));
        let b = p(json!({ "name": "b", "agents": { "x": { "model": "sonnet" } } }));
        let m = merge(&[a, b]);
        assert_eq!(m.agents["x"].model.as_deref(), Some("sonnet"));
    }
}
