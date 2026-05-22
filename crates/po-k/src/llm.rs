//! LLM backends used by the distillation loop. v1 ships only `ClaudeCli` (spawn
//! `claude -p`); the trait shape supports adding `Anthropic` / `OpenAi` HTTP
//! backends later as drop-in replacements.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait Llm: Send + Sync {
    async fn complete(&self, system: &str, user: &str) -> Result<String>;
    fn backend(&self) -> &str;
    fn model(&self) -> &str;
}

pub fn from_config(backend: &str, model: Option<String>) -> Result<Box<dyn Llm>> {
    match backend {
        "claude-cli" | "claude" => Ok(Box::new(ClaudeCli::new(model))),
        other => anyhow::bail!(
            "unknown llm backend '{other}'; supported: claude-cli (default), anthropic / openai (coming later)"
        ),
    }
}

pub struct ClaudeCli {
    model: Option<String>,
}

impl ClaudeCli {
    pub fn new(model: Option<String>) -> Self {
        Self { model }
    }
}

#[async_trait]
impl Llm for ClaudeCli {
    async fn complete(&self, system: &str, user: &str) -> Result<String> {
        let mut cmd = tokio::process::Command::new("claude");
        cmd.arg("-p");
        if let Some(m) = &self.model {
            cmd.args(["--model", m]);
        }
        cmd.args(["--output-format", "text"]);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let combined = if system.trim().is_empty() {
            user.to_string()
        } else {
            format!("<<system>>\n{system}\n<<end-system>>\n\n{user}")
        };
        let mut child = cmd
            .spawn()
            .context("spawning `claude -p` (is Claude Code installed?)")?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(combined.as_bytes())
                .await
                .context("writing prompt to claude stdin")?;
        }
        let out = child.wait_with_output().await.context("waiting on claude")?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::bail!("claude exited with {}: {}", out.status, stderr.trim());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn backend(&self) -> &str {
        "claude-cli"
    }

    fn model(&self) -> &str {
        self.model.as_deref().unwrap_or("default")
    }
}
