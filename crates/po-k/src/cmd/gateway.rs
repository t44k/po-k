//! `po-k gateway` — stdio JSONL bridge for remote agents driving local Claude
//! Code via zellij.
//!
//! v1 protocol (one JSON object per line, `\n`-terminated):
//!
//!   inbound  (remote → po-k):
//!     {"type":"prompt","project":"po-k","text":"…"[,"attachments":[…]]}
//!     {"type":"command","project":"po-k","verb":"interrupt"|"clear"|"submit"}
//!     {"type":"query","method":"projects.list"|"memory.recall","params":…,"id":"…"}
//!     {"type":"ping"[,"ts":"…"]}
//!
//!   outbound (po-k → remote):
//!     {"type":"hello","version":"po-k/0.10","repo":{…}}
//!     {"type":"result","id":"…","ok":true,"value":…}
//!     {"type":"error","id":"…","message":"…"}
//!     {"type":"event","project":"…","kind":"user_prompt"|"assistant_message"|…,…}
//!     {"type":"pong","ts":"…"}
//!
//! On startup the gateway emits `hello`. Inbound `prompt` frames route to the
//! discovered CC instance for the named project via zellij. Outbound `event`
//! frames come from the daemon's broadcast bus (so the gateway must be able to
//! reach a running `po-k service`).

use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::{config, gateway_proto as gp, ipc, project_discovery, state, zellij};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    pub sub: Option<Sub>,
}

#[derive(Debug, Subcommand)]
pub enum Sub {
    /// Print the resolved project list (discovery + allowlist) and exit.
    Projects,
}

pub async fn run(args: Args) -> Result<()> {
    match args.sub {
        Some(Sub::Projects) => list_projects().await,
        None => bridge().await,
    }
}

async fn list_projects() -> Result<()> {
    let cfg = config::load_effective()?;
    let projects = project_discovery::discover(&cfg)?;
    if projects.is_empty() {
        println!("(no matching projects)");
        println!();
        println!("Checks:");
        println!("  - is `claude` actually running? (process name = `claude`)");
        println!("  - does its cwd match any `gateway.projects[].cwd` in po-k.yaml?");
        println!("  - run `po-k config` to see the merged allowlist.");
        return Ok(());
    }
    println!("{:<14}{:<8}{:<10}{}", "slug", "cc_pid", "live", "cwd");
    for p in &projects {
        println!(
            "{:<14}{:<8}{:<10}{}",
            p.slug,
            p.cc_pid,
            if p.live { "yes" } else { "no" },
            p.cwd.display()
        );
    }
    if let Ok(sessions) = zellij::list_sessions() {
        if !sessions.is_empty() {
            println!();
            println!(
                "zellij sessions: {}",
                sessions.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(", ")
            );
        }
    }
    Ok(())
}

async fn bridge() -> Result<()> {
    let cfg = config::load_effective()?;
    let socket = config::expand_path(&cfg.service.socket);

    // Daemon subscribe stream. Optional — the bridge still serves prompts +
    // queries without it (useful for sandboxed tests). Connection failure is
    // surfaced via an early `error` frame and the gateway proceeds anyway.
    let sub_stream = match UnixStream::connect(&socket).await {
        Ok(s) => Some(s),
        Err(e) => {
            emit_stdout(&gp::Outbound::Error {
                id: None,
                message: format!(
                    "po-k service unreachable at {} ({e}); event stream disabled. Start the daemon with `po-k service --foreground`.",
                    socket.display()
                ),
            })
            .await;
            None
        }
    };

    // Initial hello with the last known pull time.
    let hello_repo = cfg
        .repo
        .as_ref()
        .map(|r| gp::HelloRepo {
            path: Some(r.path.display().to_string()),
            last_pull: read_last_pull(&cfg),
        })
        .unwrap_or(gp::HelloRepo {
            path: None,
            last_pull: None,
        });
    emit_stdout(&gp::Outbound::Hello {
        version: concat!("po-k/", env!("CARGO_PKG_VERSION")),
        repo: hello_repo,
    })
    .await;

    // Set up the streams:
    //   stdin  → inbound frame queue
    //   daemon → outbound event mirror
    let (sub_to_stdout, sub_handle) = if let Some(s) = sub_stream {
        spawn_subscribe(s)
    } else {
        // Spawn a no-op channel so the select! arm types align.
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        drop(tx);
        (rx, None)
    };
    let mut sub_to_stdout = sub_to_stdout;
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut line = String::new();

    loop {
        line.clear();
        tokio::select! {
            // Inbound frame from the remote.
            n = stdin.read_line(&mut line) => {
                let n = n.unwrap_or(0);
                if n == 0 {
                    break; // remote closed
                }
                handle_inbound(line.trim_end(), &cfg).await;
            }
            // Outbound event from the daemon.
            ev = sub_to_stdout.recv() => {
                match ev {
                    Some(frame) => {
                        let mut out = tokio::io::stdout();
                        let _ = out.write_all(frame.as_bytes()).await;
                        let _ = out.write_all(b"\n").await;
                    }
                    None => {
                        // Daemon subscribe channel closed; keep the gateway up
                        // so prompts still work, just without live events.
                    }
                }
            }
        }
    }
    if let Some(h) = sub_handle {
        h.abort();
    }
    Ok(())
}

fn spawn_subscribe(
    stream: UnixStream,
) -> (
    tokio::sync::mpsc::Receiver<String>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    let handle = tokio::spawn(async move {
        let (rh, mut wh) = stream.into_split();
        // Tell the daemon we want the stream.
        let req = ipc::Request::Subscribe;
        if let Ok(b) = serde_json::to_vec(&req) {
            let _ = wh.write_all(&b).await;
            let _ = wh.write_all(b"\n").await;
        }
        let mut r = BufReader::new(rh);
        let mut buf = String::new();
        loop {
            buf.clear();
            match r.read_line(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {
                    if tx.send(buf.trim_end().to_string()).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    (rx, Some(handle))
}

fn read_last_pull(cfg: &config::Effective) -> Option<String> {
    let path = config::expand_path(&cfg.service.state_db);
    let st = state::load(&path).ok()?;
    let repo_path = config::expand_path(&cfg.repo.as_ref()?.path);
    st.repos.get(&repo_path)?.last_pull_at.clone()
}

async fn handle_inbound(line: &str, cfg: &config::Effective) {
    if line.is_empty() {
        return;
    }
    let parsed: Result<gp::Inbound, _> = serde_json::from_str(line);
    match parsed {
        Ok(gp::Inbound::Ping { .. }) => {
            emit_stdout(&gp::Outbound::Pong {
                ts: state::now_iso(),
            })
            .await
        }
        Ok(gp::Inbound::Prompt { project, text, attachments: _ }) => {
            if let Err(e) = route_prompt(&project, &text, cfg).await {
                emit_stdout(&gp::Outbound::Error {
                    id: None,
                    message: format!("prompt routing failed: {e}"),
                })
                .await;
            }
        }
        Ok(gp::Inbound::Command { project, verb }) => {
            if let Err(e) = route_command(&project, &verb, cfg).await {
                emit_stdout(&gp::Outbound::Error {
                    id: None,
                    message: format!("command failed: {e}"),
                })
                .await;
            }
        }
        Ok(gp::Inbound::Query { method, params, id }) => {
            let value = handle_query(&method, &params, cfg).await;
            match value {
                Ok(v) => {
                    emit_stdout(&gp::Outbound::Result {
                        id,
                        ok: true,
                        value: v,
                    })
                    .await
                }
                Err(e) => {
                    emit_stdout(&gp::Outbound::Error {
                        id: Some(id),
                        message: e.to_string(),
                    })
                    .await
                }
            }
        }
        Err(e) => {
            emit_stdout(&gp::Outbound::Error {
                id: None,
                message: format!("bad frame: {e}"),
            })
            .await
        }
    }
}

async fn route_prompt(project: &str, text: &str, cfg: &config::Effective) -> Result<()> {
    let session = find_session_for(project, cfg)?;
    // Write text + Enter so CC actually submits the prompt.
    let payload = format!("{text}\n");
    tokio::task::spawn_blocking(move || zellij::write_chars(&session, &payload))
        .await
        .context("zellij write task")?
        .context("zellij write_chars")?;
    Ok(())
}

async fn route_command(project: &str, verb: &str, cfg: &config::Effective) -> Result<()> {
    let session = find_session_for(project, cfg)?;
    let chars: &str = match verb {
        "interrupt" => "\x1b", // ESC
        "clear" => "/clear\n",
        "submit" => "\n",
        other => anyhow::bail!("unknown command verb '{other}'"),
    };
    let payload = chars.to_string();
    tokio::task::spawn_blocking(move || zellij::write_chars(&session, &payload))
        .await
        .context("zellij write task")?
        .context("zellij write_chars")?;
    Ok(())
}

fn find_session_for(project: &str, cfg: &config::Effective) -> Result<String> {
    let projects = project_discovery::discover(cfg)?;
    let matched = projects
        .iter()
        .find(|p| p.slug == project)
        .with_context(|| format!("no live project matches slug '{project}'"))?;
    let _ = matched; // pid available; M10.7-followup pairs pid → zellij pane via the fork's MCP.

    // For v1 we trust the operator's gateway.zellij_session (if set) or pick
    // the single session that exists.
    if let Some(s) = cfg.gateway.zellij_session.as_ref() {
        return Ok(s.clone());
    }
    let sessions = zellij::list_sessions().context("listing zellij sessions")?;
    match sessions.len() {
        0 => anyhow::bail!("no zellij sessions are running"),
        1 => Ok(sessions[0].name.clone()),
        _ => anyhow::bail!(
            "multiple zellij sessions ({}); set gateway.zellij_session in po-k.yaml",
            sessions.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(", ")
        ),
    }
}

async fn handle_query(
    method: &str,
    params: &serde_json::Value,
    cfg: &config::Effective,
) -> Result<serde_json::Value> {
    match method {
        "projects.list" => {
            let projects = project_discovery::discover(cfg)?;
            Ok(serde_json::json!({ "projects": projects }))
        }
        "memory.recall" => {
            let id = params
                .get("topic_id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing topic_id"))?;
            let repo_path = cfg
                .repo
                .as_ref()
                .map(|r| config::expand_path(&r.path))
                .ok_or_else(|| anyhow::anyhow!("no repo configured"))?;
            let path = repo_path.join("memory").join(format!("{id}.md"));
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            Ok(serde_json::json!({"id": id, "markdown": text}))
        }
        "skill.recall" => {
            let id = params
                .get("skill_id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing skill_id"))?;
            let repo_path = cfg
                .repo
                .as_ref()
                .map(|r| config::expand_path(&r.path))
                .ok_or_else(|| anyhow::anyhow!("no repo configured"))?;
            let path = repo_path.join("skills").join(format!("{id}.md"));
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            Ok(serde_json::json!({"id": id, "markdown": text}))
        }
        other => anyhow::bail!("unknown method '{other}'"),
    }
}

async fn emit_stdout(out: &gp::Outbound) {
    if let Ok(s) = serde_json::to_string(out) {
        let mut sink = tokio::io::stdout();
        let _ = sink.write_all(s.as_bytes()).await;
        let _ = sink.write_all(b"\n").await;
        let _ = sink.flush().await;
    }
}

// Silence the unused-PathBuf import on no-PathBuf-using configurations.
#[allow(dead_code)]
fn _unused() -> PathBuf {
    PathBuf::new()
}
