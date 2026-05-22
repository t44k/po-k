//! `po-k service` — the long-running daemon. Owns the Unix socket, ticks the git
//! pull on a timer, holds per-repo + per-topic + per-jsonl state in a tiny JSON
//! file. v1 handles: ping, status, hook (logged but no distillation yet), pull-now.
//! M10.4 wires hook payloads into the distillation pipeline.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::{self, Effective};
use crate::git;
use crate::ipc::{self, Reply, Request};
use crate::state::{self, State};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Kept for symmetry; the daemon always runs in the foreground today.
    #[arg(long)]
    pub foreground: bool,
}

#[derive(Clone)]
struct Ctx {
    cfg: Effective,
    socket_path: PathBuf,
    state_path: PathBuf,
    state: Arc<Mutex<State>>,
}

pub async fn run(_args: Args) -> Result<()> {
    let cfg = config::load_effective()?;

    let socket_path = config::expand_path(&cfg.service.socket);
    let state_path = config::expand_path(&cfg.service.state_db);
    if state_path.extension().and_then(|s| s.to_str()) == Some("db") {
        // The schema field is named state_db for historical reasons; v1 writes JSON.
        // Reuse the same path with the .db suffix — clearer migration story later.
    }
    let mut state = state::load(&state_path)?;
    state.started_at = Some(state::now_iso());
    state.pid = Some(std::process::id());
    state::save(&state_path, &state)?;
    let state = Arc::new(Mutex::new(state));

    // Remove any stale socket (last daemon was killed before cleanup).
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    info!(socket = %socket_path.display(), pid = std::process::id(), "po-k service listening");

    let ctx = Ctx {
        cfg,
        socket_path: socket_path.clone(),
        state_path: state_path.clone(),
        state: state.clone(),
    };

    // Background tick: periodic git pull.
    {
        let ctx = ctx.clone();
        tokio::spawn(async move { periodic_pull(ctx).await });
    }

    // Cleanly remove the socket on Ctrl-C / SIGTERM.
    let socket_cleanup = socket_path.clone();
    tokio::spawn(async move {
        let mut sig = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = sig.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        info!("shutting down, removing {}", socket_cleanup.display());
        let _ = std::fs::remove_file(&socket_cleanup);
        std::process::exit(0);
    });

    // Accept loop.
    loop {
        let (stream, _addr) = listener.accept().await?;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, ctx).await {
                warn!(error = %e, "connection error");
            }
        });
    }
}

async fn handle_connection(stream: UnixStream, ctx: Ctx) -> Result<()> {
    let (rh, mut wh) = stream.into_split();
    let mut reader = BufReader::new(rh);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let req: Request = match serde_json::from_str(line.trim_end()) {
        Ok(r) => r,
        Err(e) => {
            let reply = Reply::Error {
                message: format!("invalid request: {e}"),
            };
            write_reply(&mut wh, &reply).await?;
            return Ok(());
        }
    };

    let reply = dispatch(req, &ctx).await;
    write_reply(&mut wh, &reply).await?;
    Ok(())
}

async fn write_reply<W: AsyncWriteExt + Unpin>(w: &mut W, reply: &Reply) -> Result<()> {
    let bytes = serde_json::to_vec(reply)?;
    w.write_all(&bytes).await?;
    w.write_all(b"\n").await?;
    w.shutdown().await.ok();
    Ok(())
}

async fn dispatch(req: Request, ctx: &Ctx) -> Reply {
    match req {
        Request::Ping => {
            let s = ctx.state.lock().await;
            Reply::Pong {
                pid: s.pid.unwrap_or(std::process::id()),
                started_at: s.started_at.clone().unwrap_or_default(),
            }
        }
        Request::Status => {
            let s = ctx.state.lock().await;
            let repo_status = ctx.cfg.repo.as_ref().map(|r| {
                let p = config::expand_path(&r.path);
                let rs = s.repos.get(&p);
                ipc::RepoStatusDto {
                    path: p.clone(),
                    last_pull_at: rs.and_then(|r| r.last_pull_at.clone()),
                    last_pull_ok: rs.map(|r| r.last_pull_ok).unwrap_or(false),
                }
            });
            let (topics, skills) = repo_counts(&ctx.cfg);
            Reply::Status {
                pid: s.pid.unwrap_or(std::process::id()),
                started_at: s.started_at.clone().unwrap_or_default(),
                repo: repo_status,
                topic_count: topics,
                skill_count: skills,
            }
        }
        Request::PullNow => {
            do_pull(ctx).await;
            Reply::Ok
        }
        Request::Hook { event, payload } => {
            // M10.4 will route this; for now just log it so the wire round-trip
            // is visible during smoke tests.
            tracing::debug!(event = %event, payload = %payload, "received hook");
            Reply::Ok
        }
    }
}

fn repo_counts(cfg: &Effective) -> (usize, usize) {
    let Some(r) = cfg.repo.as_ref() else { return (0, 0) };
    let p = config::expand_path(&r.path);
    let count_md = |dir: std::path::PathBuf| -> usize {
        let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
        rd.flatten()
            .filter(|e| {
                e.file_type().map(|t| t.is_file()).unwrap_or(false)
                    && e.file_name().to_string_lossy().ends_with(".md")
            })
            .count()
    };
    (count_md(p.join("memory")), count_md(p.join("skills")))
}

// ─── periodic pull ───────────────────────────────────────────────────────────

async fn periodic_pull(ctx: Ctx) {
    let Some(repo) = ctx.cfg.repo.as_ref() else { return };
    let interval = repo.pull_interval;
    // Pull immediately on start so the very first `po-k` status block doesn't say
    // "last pull: never" on a freshly-started daemon.
    do_pull(&ctx).await;
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        do_pull(&ctx).await;
    }
}

async fn do_pull(ctx: &Ctx) {
    let mut all_repos: Vec<(std::path::PathBuf, &'static str)> = Vec::new();
    if let Some(r) = ctx.cfg.repo.as_ref() {
        all_repos.push((config::expand_path(&r.path), "primary"));
    }
    for nested in &ctx.cfg.nested_repos {
        all_repos.push((config::expand_path(&nested.path), "nested"));
    }
    for (path, kind) in all_repos {
        if !path.join(".git").exists() {
            warn!(repo = %path.display(), kind, "repo not cloned yet; skipping pull");
            continue;
        }
        let result = tokio::task::spawn_blocking({
            let p = path.clone();
            move || git::pull(&p)
        })
        .await;
        let outcome = match result {
            Ok(Ok(out)) if out.ok() => {
                tracing::debug!(repo = %path.display(), "pulled");
                Some(true)
            }
            Ok(Ok(out)) => {
                warn!(repo = %path.display(), stderr = %out.stderr.trim(), "git pull failed");
                Some(false)
            }
            Ok(Err(e)) => {
                warn!(repo = %path.display(), error = %e, "git pull spawn failed");
                Some(false)
            }
            Err(e) => {
                warn!(repo = %path.display(), error = %e, "pull task panicked");
                None
            }
        };
        if let Some(ok) = outcome {
            let mut st = ctx.state.lock().await;
            let entry = st.repos.entry(path.clone()).or_default();
            entry.last_pull_at = Some(state::now_iso());
            entry.last_pull_ok = ok;
            // Best-effort persistence; if the disk is wedged, the daemon keeps running.
            if let Err(e) = state::save(&ctx.state_path, &st) {
                warn!(error = %e, "state save failed");
            }
        }
    }
}
