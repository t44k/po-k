//! Thin shell-out wrappers around the `zellij` CLI.
//!
//! - `list_sessions()` parses `zellij list-sessions --short`.
//! - `ensure_session(name)` is idempotent: returns Ok if the session exists.
//!   If missing, spawns `setsid -f zellij --session NAME` to create the server
//!   detached from po-k's controlling tty.
//! - `kill_session(name)` is `zellij kill-session NAME` (also idempotent —
//!   missing session exits 0 in modern zellij; we tolerate any error).
//! - `write_chars(session, text)` sends raw bytes to the session's focused
//!   pane via `zellij --session NAME action write-chars ...`.

use anyhow::{Context, Result};
use std::process::Stdio;
use tokio::process::Command;

/// List active zellij session names.
pub async fn list_sessions() -> Result<Vec<String>> {
    let out = Command::new("zellij")
        .args(["list-sessions", "--short"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("spawn `zellij list-sessions --short`")?;
    // zellij exits non-zero if no sessions exist; treat that as "empty list".
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("No active") || stderr.contains("No sessions") {
            return Ok(vec![]);
        }
        anyhow::bail!("zellij list-sessions failed: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    Ok(parse_list(&stdout))
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

/// Returns `Ok(true)` if the session already existed, `Ok(false)` if we just
/// created it. Either way the session exists after this call.
pub async fn ensure_session(name: &str) -> Result<bool> {
    if list_sessions().await?.iter().any(|s| s == name) {
        return Ok(true);
    }
    // setsid -f detaches from the controlling tty and forks; the zellij server
    // stays alive after our child exits.
    let status = Command::new("setsid")
        .args(["-f", "zellij", "--session", name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("spawn `setsid -f zellij --session NAME`")?;
    if !status.success() {
        anyhow::bail!("setsid+zellij returned {status}");
    }
    // Give the server a beat to bind its socket before subsequent commands.
    for _ in 0..20 {
        if list_sessions().await?.iter().any(|s| s == name) {
            return Ok(false);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    anyhow::bail!("zellij session {name:?} did not appear within 1s after spawn")
}

/// Tear down a session. Idempotent: missing session is not an error.
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

/// Write `text` into the focused pane of `session`.
///
/// `zellij action write-chars` treats its positional argument as the literal
/// string to type; we forward bytes verbatim. Caller is responsible for adding
/// a trailing `\n` if they want the line submitted.
pub async fn write_chars(session: &str, text: &str) -> Result<()> {
    let status = Command::new("zellij")
        .args(["--session", session, "action", "write-chars", text])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .status()
        .await
        .context("spawn `zellij action write-chars`")?;
    if !status.success() {
        anyhow::bail!("zellij write-chars returned {status}");
    }
    Ok(())
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
}
