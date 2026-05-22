//! Unix socket IPC between subprocesses and the long-running `po-k service`.
//!
//! Wire format: one JSON object per line, `\n`-terminated. The server reads one
//! request line, writes one reply line, and closes the connection. (For the rare
//! handler that needs to stream — e.g. log tailing later — we'll layer a second
//! protocol on a different path.)

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Quick heartbeat — `pong` echoes the daemon's pid + start time.
    Ping,
    /// Daemon-side status payload (used by bare `po-k`).
    Status,
    /// Forwarded from a CC hook (`po-k hook EVENT`).
    Hook {
        event: String,
        payload: serde_json::Value,
    },
    /// Trigger an immediate `git pull` of the primary + nested repos.
    PullNow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Pong {
        pid: u32,
        started_at: String,
    },
    Status {
        pid: u32,
        started_at: String,
        repo: Option<RepoStatusDto>,
        topic_count: usize,
        skill_count: usize,
    },
    Ok,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatusDto {
    pub path: PathBuf,
    pub last_pull_at: Option<String>,
    pub last_pull_ok: bool,
}

/// Default socket path under `~/.config/po-k/service.sock`.
pub fn default_socket_path() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".config/po-k/service.sock")
}

/// Round-trip one request over a Unix socket; returns the parsed reply.
pub async fn request(socket: &Path, req: &Request) -> Result<Reply> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting {}", socket.display()))?;
    let (rh, mut wh) = stream.into_split();
    let payload = serde_json::to_vec(req)?;
    wh.write_all(&payload).await?;
    wh.write_all(b"\n").await?;
    wh.shutdown().await.ok();
    drop(wh);

    let mut reader = BufReader::new(rh);
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    let reply: Reply = serde_json::from_str(buf.trim_end())
        .with_context(|| format!("parsing reply: {buf:?}"))?;
    Ok(reply)
}
