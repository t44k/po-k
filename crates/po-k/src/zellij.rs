//! Drive zellij sessions via the per-session MCP Unix socket (t44k/zellij
//! `mcp-direct-ipc-refactor` branch). Session lifecycle goes through the CLI;
//! input/output/control goes through the MCP socket. The MCP server only runs
//! *inside* an existing session.
//!
//! Socket path: `~/.cache/zellij/{session_name}.mcp.sock`. Wire format: NDJSON,
//! one `{"operation":"...","args":{...}}` per line.
//!
//! Requires `mcp { enabled true }` in `~/.config/zellij/config.kdl`.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::time::timeout;

const MCP_TIMEOUT: Duration = Duration::from_secs(5);
const SOCKET_WAIT_TOTAL: Duration = Duration::from_secs(5);
const SOCKET_POLL: Duration = Duration::from_millis(50);

// ─────────────────────────────────────────────────────────────────────────────
// Lifecycle (CLI)
// ─────────────────────────────────────────────────────────────────────────────

pub async fn list_sessions() -> Result<Vec<String>> {
    let out = Command::new("zellij")
        .args(["list-sessions", "--short"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("spawn `zellij list-sessions --short`")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("No active") || stderr.contains("No sessions") {
            return Ok(vec![]);
        }
        anyhow::bail!("zellij list-sessions failed: {stderr}");
    }
    Ok(parse_list(&String::from_utf8_lossy(&out.stdout)))
}

fn parse_list(raw: &str) -> Vec<String> {
    raw.lines()
        .map(|l| strip_ansi(l).trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if matches!(chars.peek(), Some('[')) {
                chars.next();
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Ensure a zellij session exists, then wait for its MCP socket to be ready.
///
/// Returns `Ok(true)` if the session pre-existed, `Ok(false)` if we created it.
///
/// **Session creation:** zellij needs a real PTY. The po-k server itself runs
/// inside a `setsid` container with `stdin=/dev/null`, so the tokio process
/// has no TTY. `setsid -f zellij --session NAME` forks a child that inherits
/// `/dev/null` as stdin → zellij's `isatty` check fails → silent death.
///
/// The workaround: `setsid -f script -q -c 'exec zellij --session NAME' /dev/null`.
///
/// - `setsid -f` forks into a new session; the parent process exits immediately.
/// - The forked child runs `script` which allocates a child PTY.
/// - Inside the script PTY, `exec zellij --session NAME` runs.
/// - `.spawn()` (not `.status()`) — we don't wait for it; the grandchild
///   persists in its own process group.
pub async fn ensure_session(name: &str) -> Result<bool> {
    if list_sessions().await?.iter().any(|s| s == name) {
        wait_for_socket(name).await?;
        return Ok(true);
    }
    // Spawn in the background — we'll poll for the socket below.
    Command::new("setsid")
        .args([
            "-f",
            "script",
            "-q",
            "-c",
            &format!("exec zellij --session {name}"),
            "/dev/null",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn `setsid -f script -q -c 'exec zellij --session NAME' /dev/null`")?;

    // The script process exits immediately (setsid -f). The grandchild
    // zellij is starting up in its own PTY session. Poll for both the
    // session listing AND a live MCP socket.
    let deadline = std::time::Instant::now() + SOCKET_WAIT_TOTAL;
    while std::time::Instant::now() < deadline {
        let listed = list_sessions().await?.iter().any(|s| s == name);
        if listed && is_socket_alive(name).await {
            return Ok(false);
        }
        tokio::time::sleep(SOCKET_POLL).await;
    }
    anyhow::bail!(
        "zellij session {name:?} created but MCP socket did not become ready at {}",
        mcp_socket_path(name).display()
    )
}

/// Check if the MCP socket is actually alive (connectable Unix socket).
/// Stale .mcp.sock files survive `zellij kill-session` so we probe, not stat.
async fn is_socket_alive(name: &str) -> bool {
    let path = mcp_socket_path(name);
    if !path.exists() {
        return false;
    }
    match UnixStream::connect(&path).await {
        Ok(_) => true,
        Err(_) => {
            let _ = std::fs::remove_file(&path);
            false
        }
    }
}

async fn wait_for_socket(name: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + SOCKET_WAIT_TOTAL;
    while std::time::Instant::now() < deadline {
        if is_socket_alive(name).await {
            return Ok(());
        }
        tokio::time::sleep(SOCKET_POLL).await;
    }
    anyhow::bail!(
        "MCP socket {} not ready after {:?} (is `mcp {{ enabled true }}` in ~/.config/zellij/config.kdl?)",
        mcp_socket_path(name).display(),
        SOCKET_WAIT_TOTAL,
    )
}

pub async fn kill_session(name: &str) -> Result<()> {
    let out = Command::new("zellij")
        .args(["kill-session", name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("spawn `zellij kill-session NAME`")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("not found") || stderr.contains("No such") {
            return Ok(());
        }
        anyhow::bail!("zellij kill-session {name}: {stderr}");
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// MCP transport
// ─────────────────────────────────────────────────────────────────────────────

fn mcp_socket_path(session: &str) -> PathBuf {
    crate::config::expand_path(&format!("~/.cache/zellij/{session}.mcp.sock"))
}

async fn mcp_call(session: &str, operation: &str, args: Value) -> Result<Value> {
    let req = json!({ "operation": operation, "args": args });
    let line = serde_json::to_string(&req).expect("serialize MCP request");

    let path = mcp_socket_path(session);
    let stream = timeout(MCP_TIMEOUT, UnixStream::connect(&path))
        .await
        .with_context(|| format!("connect to MCP socket {} (timeout)", path.display()))?
        .with_context(|| format!("connect to MCP socket {}", path.display()))?;

    let (read_half, mut write_half) = stream.into_split();
    timeout(MCP_TIMEOUT, async {
        write_half.write_all(line.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        write_half.flush().await?;
        anyhow::Ok(())
    })
    .await
    .context("write MCP request (timeout)")?
    .context("write MCP request")?;

    let mut reader = tokio::io::BufReader::new(read_half);
    let mut resp = String::new();
    timeout(MCP_TIMEOUT, reader.read_line(&mut resp))
        .await
        .context("read MCP response (timeout)")?
        .context("read MCP response")?;

    let v: Value = serde_json::from_str(resp.trim())
        .with_context(|| format!("parse MCP response: {resp:?}"))?;

    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        anyhow::bail!("MCP {operation} failed: {err}");
    }
    if v.get("success").and_then(|s| s.as_bool()) == Some(false) {
        anyhow::bail!("MCP {operation} returned success=false: {v}");
    }
    Ok(v)
}

// ─────────────────────────────────────────────────────────────────────────────
// In-session control (MCP)
// ─────────────────────────────────────────────────────────────────────────────

async fn focused_terminal_pane(session: &str) -> Result<(u32, String)> {
    let resp = mcp_call(session, "list_panes", json!({})).await?;
    let panes = resp
        .get("panes")
        .and_then(|p| p.as_object())
        .context("list_panes: missing `panes` object")?;
    for (tab_key, list) in panes {
        let tab_index: u32 = tab_key
            .strip_prefix("tab_")
            .and_then(|s| s.parse().ok())
            .with_context(|| format!("unexpected tab key {tab_key:?}"))?;
        let arr = list.as_array().context("tab value not an array")?;
        for pane in arr {
            let is_plugin = pane.get("is_plugin").and_then(|b| b.as_bool()).unwrap_or(false);
            let is_focused = pane.get("is_focused").and_then(|b| b.as_bool()).unwrap_or(false);
            if !is_plugin && is_focused {
                let id = pane
                    .get("id")
                    .and_then(|s| s.as_str())
                    .context("pane missing id")?
                    .to_string();
                return Ok((tab_index, id));
            }
        }
    }
    anyhow::bail!("no focused terminal pane in session {session:?}")
}

pub async fn write_to_focused_pane(session: &str, text: &str) -> Result<()> {
    let (tab_index, pane_id) = focused_terminal_pane(session).await?;
    mcp_call(
        session,
        "write_to_pane",
        json!({ "tab_index": tab_index, "pane_id": pane_id, "text": text }),
    )
    .await?;
    Ok(())
}

pub async fn send_escape(session: &str) -> Result<()> {
    let (tab_index, pane_id) = focused_terminal_pane(session).await?;
    mcp_call(
        session,
        "send_keys",
        json!({
            "tab_index": tab_index,
            "pane_id": pane_id,
            "keys": ["escape"],
        }),
    )
    .await?;
    Ok(())
}

/// Read the visible content of the focused terminal pane.
pub async fn read_focused_pane(session: &str) -> Result<String> {
    let (tab_index, pane_id) = focused_terminal_pane(session).await?;
    let resp = mcp_call(
        session,
        "read_pane",
        json!({ "tab_index": tab_index, "pane_id": pane_id }),
    )
    .await?;
    Ok(resp
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string())
}

/// CC's REPL input box renders as a line beginning with `❯` (U+276F). No shell
/// prompt in our setup (`exec claude` replaces a bash/fish shell) emits that
/// glyph at the start of a line, so its presence is a reliable "CC has booted
/// and is ready to accept input" signal.
fn shows_cc_prompt(content: &str) -> bool {
    content.lines().any(|l| l.trim_start().starts_with('❯'))
}

/// Poll the focused pane until CC's `❯` prompt is visible — i.e. the REPL has
/// finished booting and will accept input. Input typed before this point is
/// silently dropped, and CC only writes its transcript JSONL after the first
/// *submitted* prompt, so callers must gate input on this.
pub async fn wait_for_cc_prompt(session: &str, total: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + total;
    loop {
        if let Ok(content) = read_focused_pane(session).await {
            if shows_cc_prompt(&content) {
                return Ok(());
            }
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("CC input prompt (❯) not visible in {session:?} after {total:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_short_listing() {
        let raw = "po-k-po-k\nbeyondbash\n";
        assert_eq!(parse_list(raw), vec!["po-k-po-k", "beyondbash"]);
    }

    #[test]
    fn strips_ansi_then_trims() {
        let raw = "\x1b[33mpo-k-po-k\x1b[0m\n\x1b[33msession-two\x1b[0m";
        assert_eq!(parse_list(raw), vec!["po-k-po-k", "session-two"]);
    }

    #[test]
    fn parses_empty_listing() {
        assert!(parse_list("").is_empty());
        assert!(parse_list("\n\n").is_empty());
    }

    #[test]
    fn detects_cc_prompt_at_line_start() {
        // CC's ready prompt is `❯` + nbsp at column 0.
        assert!(shows_cc_prompt("some output\n❯\u{a0}            \nfooter"));
        // Leading indentation before the glyph still counts.
        assert!(shows_cc_prompt("  ❯ "));
    }

    #[test]
    fn ignores_glyph_mid_line() {
        // Conversation text or a shell prompt mentioning ❯ must not match.
        assert!(!shows_cc_prompt("the ❯ readiness signal explained"));
        assert!(!shows_cc_prompt("me@host /workspace > cd /x && exec claude"));
        assert!(!shows_cc_prompt(""));
    }
}
