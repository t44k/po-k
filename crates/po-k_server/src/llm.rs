//! LLM backends for the distillation loop.
//!
//! The default [`ClaudeCli`] backend spawns `claude -p <prompt>` as a subprocess —
//! zero-config when the operator already has Claude Code installed (it reuses their
//! existing auth, prompt cache, hooks, etc.). The trait is small enough that swapping
//! to an HTTP backend (Anthropic / OpenAI) later is a drop-in replacement.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait Llm: Send + Sync {
    /// Run the LLM once with a system prompt and a user prompt; return the model's text.
    async fn complete(&self, system: &str, user: &str) -> Result<String>;
    fn backend_label(&self) -> &str;
    fn model_label(&self) -> &str;
}

pub fn from_config(backend: &str, model: Option<String>) -> Result<Box<dyn Llm>> {
    match backend {
        "claude-cli" | "claude" => Ok(Box::new(ClaudeCli::new(model.clone()))),
        other => anyhow::bail!(
            "unknown LLM backend '{other}'; supported: claude-cli (default), anthropic*, openai* (*coming soon)"
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
        if let Some(model) = &self.model {
            cmd.args(["--model", model]);
        }
        cmd.args(["--output-format", "text"]);
        // Many CC versions accept `--append-system-prompt`; older ones may not. We pass
        // the system prompt via that flag when set, and otherwise prepend it inline.
        let combined = if system.trim().is_empty() {
            user.to_string()
        } else {
            format!("<<system>>\n{system}\n<<end-system>>\n\n{user}")
        };
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().context("spawning `claude -p` (is Claude Code installed?)")?;

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

    fn backend_label(&self) -> &str {
        "claude-cli"
    }

    fn model_label(&self) -> &str {
        self.model.as_deref().unwrap_or("default")
    }
}
