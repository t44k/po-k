//! Per-turn distillation. Given a just-completed turn, walk the configured
//! topics; for any whose question shares enough token-overlap with the turn's
//! text, ask the LLM to update its markdown digest, write to memory/<id>.md,
//! and git-add + git-commit. The push is debounced by the daemon (M10.4 calls
//! schedule_push() after a successful commit).
//!
//! Salvaged from the M9 distill loop: same prompt shape, simpler evidence
//! window (one turn, not a corpus).

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::config::Topic;
use crate::git;
use crate::llm::Llm;
use crate::text;
use crate::turn::Turn;

/// Minimum overlap ratio to consider a topic "covered" by this turn. Below this
/// we skip the LLM call entirely.
const OVERLAP_THRESHOLD: f32 = 0.10;
/// Hard cap on the per-turn evidence text we hand the LLM.
const EVIDENCE_BUDGET_CHARS: usize = 60_000;

#[derive(Debug, Clone, Default)]
pub struct Outcome {
    pub topics_updated: Vec<String>,
    pub topics_skipped: Vec<(String, String)>,
}

pub async fn distill_turn(
    repo_root: &Path,
    topics: &[Topic],
    turn: &Turn,
    llm: &dyn Llm,
) -> Result<Outcome> {
    let memory_dir = repo_root.join("memory");
    std::fs::create_dir_all(&memory_dir).ok();
    let evidence = trim_to_budget(&turn.searchable, EVIDENCE_BUDGET_CHARS);

    let mut outcome = Outcome::default();
    for topic in topics {
        let score = text::overlap(&topic.question, &evidence);
        if score < OVERLAP_THRESHOLD {
            outcome
                .topics_skipped
                .push((topic.id.clone(), format!("overlap {:.2}", score)));
            continue;
        }
        let target = memory_dir.join(format!("{}.md", topic.id));
        let prior = std::fs::read_to_string(&target).unwrap_or_default();
        let prior_for_prompt = if prior.trim().is_empty() {
            "(no prior digest)".to_string()
        } else {
            prior.clone()
        };
        let system = build_system_prompt(&topic);
        let user = format!(
            "# Topic\n{question}\n\n# Prior digest\n{prior}\n\n# New evidence (one Claude Code turn)\n{evidence}",
            question = topic.question,
            prior = prior_for_prompt,
        );
        let new_digest = llm
            .complete(&system, &user)
            .await
            .with_context(|| format!("llm.complete failed for topic {}", topic.id))?;
        let new_digest = new_digest.trim();
        if new_digest.is_empty() {
            outcome
                .topics_skipped
                .push((topic.id.clone(), "llm returned empty".into()));
            continue;
        }
        if new_digest == prior.trim() {
            outcome
                .topics_skipped
                .push((topic.id.clone(), "no change".into()));
            continue;
        }
        std::fs::write(&target, format!("{new_digest}\n"))
            .with_context(|| format!("writing {}", target.display()))?;
        git_commit_topic(repo_root, &target, &topic.id, &turn.turn_id)?;
        outcome.topics_updated.push(topic.id.clone());
    }
    Ok(outcome)
}

fn build_system_prompt(topic: &Topic) -> String {
    let extras = topic
        .system_prompt_extras
        .as_deref()
        .unwrap_or("")
        .trim();
    let mut s = String::from(
        "You maintain a living markdown digest answering one curated question for a team.\n\
         Read the prior digest and the new evidence below, then output an updated digest \
         (markdown only — no preface, no explanation). Keep it concise (~400 words). \
         If the new evidence adds nothing material, output the prior digest verbatim. \
         If it contradicts the prior digest, prefer the more recent / clearer evidence \
         and add a one-line 'Recent changes' note at the bottom.",
    );
    if !extras.is_empty() {
        s.push_str("\nAdditional guidance: ");
        s.push_str(extras);
    }
    s
}

fn git_commit_topic(repo_root: &Path, target: &PathBuf, topic_id: &str, turn_id: &str) -> Result<()> {
    let pathspec = target
        .strip_prefix(repo_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| target.to_string_lossy().to_string());
    let add = git::add(repo_root, &pathspec).context("git add")?;
    if !add.ok() {
        anyhow::bail!("git add failed: {}", add.stderr.trim());
    }
    let msg = if turn_id.is_empty() {
        format!("po-k: update {topic_id}")
    } else {
        format!("po-k: update {topic_id} after turn {turn_id}")
    };
    let commit = git::commit(repo_root, &msg).context("git commit")?;
    if !commit.ok() {
        // "nothing to commit" is the common case when the file content already
        // matched what's tracked; treat that as success.
        let s = commit.stderr.clone() + &commit.stdout;
        if s.contains("nothing to commit") || s.contains("nothing added to commit") {
            return Ok(());
        }
        anyhow::bail!("git commit failed: {}", s.trim());
    }
    Ok(())
}

// ─── skills auto-extraction ──────────────────────────────────────────────────

/// Heuristic gate: only ask the LLM for skill extraction if the turn looks
/// procedural (mentions a verb like deploy / restart / build / test / run AND
/// references at least one shell command snippet).
fn looks_procedural(turn: &Turn) -> bool {
    let lower = turn.searchable.to_lowercase();
    let has_verb = ["deploy", "restart", "build", "test", "release", "rollback", "install", "configure", "migrate"]
        .iter()
        .any(|v| lower.contains(v));
    let has_command = lower.contains('`')
        || lower.contains("./")
        || lower.contains("$ ")
        || lower.contains("cargo ")
        || lower.contains("make ")
        || lower.contains("npm ");
    has_verb && has_command
}

/// Output of one skill-extraction call.
#[derive(Debug, Clone)]
pub struct SkillProposal {
    pub id: String,
    pub markdown: String,
}

/// Ask the LLM whether this turn introduces a reusable how-to. If yes, return
/// a (skill_id, markdown) pair; if not (or on parse failure), return None.
pub async fn extract_skill(
    repo_root: &Path,
    turn: &Turn,
    llm: &dyn Llm,
) -> Result<Option<SkillProposal>> {
    if !looks_procedural(turn) {
        return Ok(None);
    }
    let skills_dir = repo_root.join("skills");
    std::fs::create_dir_all(&skills_dir).ok();
    let existing_ids = list_md_ids(&skills_dir);
    let existing_list = if existing_ids.is_empty() {
        "(none yet)".to_string()
    } else {
        existing_ids.join(", ")
    };
    let system = "You extract reusable team SKILLS — small how-to procedures (deploy, restart, \
                   migrate, etc.) — from a Claude Code turn.\n\
                   If the turn covers a procedure NOT already in the existing skills list, \
                   reply with EXACTLY this format:\n\n\
                   <kebab-case-id>\n\
                   ---\n\
                   <full markdown body, no preface>\n\n\
                   Otherwise reply with the single word NOSKILL.";
    let user = format!(
        "# Existing skill ids\n{existing_list}\n\n# Turn evidence\n{evidence}",
        evidence = trim_to_budget(&turn.searchable, EVIDENCE_BUDGET_CHARS),
    );
    let raw = llm.complete(system, &user).await?;
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "NOSKILL" || trimmed.starts_with("NOSKILL") {
        return Ok(None);
    }
    let (id_line, body) = match trimmed.split_once("\n---\n") {
        Some((a, b)) => (a.trim(), b.trim()),
        // Fall back: first line = id, rest = body.
        None => match trimmed.split_once('\n') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => return Ok(None),
        },
    };
    let id = id_line.trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_');
    if id.is_empty() || body.is_empty() {
        return Ok(None);
    }
    Ok(Some(SkillProposal {
        id: id.to_string(),
        markdown: body.to_string(),
    }))
}

/// Apply a skill proposal: write the file if it doesn't already exist (we never
/// overwrite existing skills automatically — that's a user-driven concern), git
/// add + commit on a new file.
pub fn apply_skill(
    repo_root: &Path,
    proposal: &SkillProposal,
    turn_id: &str,
) -> Result<bool> {
    let skills_dir = repo_root.join("skills");
    std::fs::create_dir_all(&skills_dir).ok();
    let target = skills_dir.join(format!("{}.md", proposal.id));
    if target.exists() {
        return Ok(false);
    }
    std::fs::write(&target, format!("{}\n", proposal.markdown))
        .with_context(|| format!("writing {}", target.display()))?;
    let pathspec = target
        .strip_prefix(repo_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| target.to_string_lossy().to_string());
    let add = git::add(repo_root, &pathspec)?;
    if !add.ok() {
        anyhow::bail!("git add failed: {}", add.stderr.trim());
    }
    let msg = if turn_id.is_empty() {
        format!("po-k: add skill {}", proposal.id)
    } else {
        format!("po-k: add skill {} after turn {turn_id}", proposal.id)
    };
    let commit = git::commit(repo_root, &msg)?;
    if !commit.ok() {
        let s = commit.stderr.clone() + &commit.stdout;
        if s.contains("nothing to commit") {
            return Ok(true);
        }
        anyhow::bail!("git commit failed: {}", s.trim());
    }
    Ok(true)
}

fn list_md_ids(dir: &Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(dir) else { return Vec::new() };
    rd.flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".md").map(str::to_string)
        })
        .collect()
}

fn trim_to_budget(s: &str, budget: usize) -> String {
    if s.len() <= budget {
        return s.to_string();
    }
    let mut out: String = s.chars().take(budget.saturating_sub(1)).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockLlm {
        reply: String,
    }

    #[async_trait]
    impl crate::llm::Llm for MockLlm {
        async fn complete(&self, _system: &str, _user: &str) -> Result<String> {
            Ok(self.reply.clone())
        }
        fn backend(&self) -> &str {
            "mock"
        }
        fn model(&self) -> &str {
            "mock-1"
        }
    }

    fn tmp_repo() -> PathBuf {
        let tmp = std::env::temp_dir().join(format!("po-k-distill-{}", uuid_like()));
        std::fs::create_dir_all(&tmp).unwrap();
        // Minimal git repo so distill's git add/commit can run.
        std::process::Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(&tmp)
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["-c", "user.email=t@e", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "init"])
            .current_dir(&tmp)
            .status()
            .unwrap();
        tmp
    }

    fn uuid_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            std::process::id()
        )
    }

    #[tokio::test]
    async fn distills_topic_with_overlap_and_commits() {
        let repo = tmp_repo();
        let topic = Topic {
            id: "testing-conventions".into(),
            question: "What testing conventions has this team adopted?".into(),
            system_prompt_extras: None,
        };
        let turn = Turn {
            turn_id: "t1".into(),
            raw_lines: vec![],
            // Token matcher is exact (no stemming); the turn must mention the
            // topic's content words verbatim for the overlap heuristic to fire.
            searchable: "Our testing conventions: pytest-driven, integration over unit, run with --cov against a sqlite fixture.".into(),
        };
        let llm = MockLlm {
            reply: "# Testing conventions\n\n- pytest with --cov".into(),
        };
        let outcome = distill_turn(&repo, &[topic], &turn, &llm).await.unwrap();
        assert_eq!(outcome.topics_updated, vec!["testing-conventions".to_string()]);
        let written = std::fs::read_to_string(repo.join("memory/testing-conventions.md")).unwrap();
        assert!(written.contains("pytest"));
        // git log should show the auto-commit on top of the initial.
        let log = std::process::Command::new("git")
            .args(["log", "--oneline"])
            .current_dir(&repo)
            .output()
            .unwrap();
        let log = String::from_utf8_lossy(&log.stdout);
        assert!(log.contains("update testing-conventions"), "log was:\n{log}");
        std::fs::remove_dir_all(&repo).ok();
    }

    #[tokio::test]
    async fn skill_proposal_written_when_procedural() {
        let repo = tmp_repo();
        let turn = Turn {
            turn_id: "t-skill".into(),
            raw_lines: vec![],
            // Procedural verb + a command snippet trip looks_procedural.
            searchable: "To deploy run `make deploy` after CI is green.".into(),
        };
        let llm = MockLlm {
            reply: "deploy-prod\n---\n# Deploy to prod\n\n- Wait for CI green\n- Run `make deploy`".into(),
        };
        let proposal = extract_skill(&repo, &turn, &llm).await.unwrap().unwrap();
        assert_eq!(proposal.id, "deploy-prod");
        let landed = apply_skill(&repo, &proposal, &turn.turn_id).unwrap();
        assert!(landed);
        let written = std::fs::read_to_string(repo.join("skills/deploy-prod.md")).unwrap();
        assert!(written.contains("make deploy"));
        // Second apply with the same id is a no-op.
        let again = apply_skill(&repo, &proposal, "").unwrap();
        assert!(!again);
        std::fs::remove_dir_all(&repo).ok();
    }

    #[tokio::test]
    async fn skill_skipped_when_not_procedural() {
        let repo = tmp_repo();
        let turn = Turn {
            turn_id: "t-x".into(),
            raw_lines: vec![],
            searchable: "Just a chat about the weather.".into(),
        };
        // LLM should NOT be called; reply would be ignored anyway.
        let llm = MockLlm {
            reply: "should not be used".into(),
        };
        let p = extract_skill(&repo, &turn, &llm).await.unwrap();
        assert!(p.is_none());
        std::fs::remove_dir_all(&repo).ok();
    }

    #[tokio::test]
    async fn skips_topic_without_overlap() {
        let repo = tmp_repo();
        let topic = Topic {
            id: "auth-pattern".into(),
            question: "What auth pattern does this team use?".into(),
            system_prompt_extras: None,
        };
        let turn = Turn {
            turn_id: "t1".into(),
            raw_lines: vec![],
            searchable: "The weather is fine today, no thunderstorm expected.".into(),
        };
        let llm = MockLlm {
            reply: "should not be called".into(),
        };
        let outcome = distill_turn(&repo, &[topic], &turn, &llm).await.unwrap();
        assert!(outcome.topics_updated.is_empty());
        assert_eq!(outcome.topics_skipped.len(), 1);
        assert!(!repo.join("memory/auth-pattern.md").exists());
        std::fs::remove_dir_all(&repo).ok();
    }
}
