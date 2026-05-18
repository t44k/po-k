use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use notify_debouncer_full::{
    new_debouncer,
    notify::{EventKind, RecursiveMode},
    DebounceEventResult,
};
use po_k_core::MachineId;
use tokio::sync::mpsc;
use tracing::{info, warn};

mod meta;
mod projects;
mod scan;
mod ship;
mod watermark;

use projects::ProjectMap;
use scan::{discover_jsonl_files, scan_file};
use ship::Shipper;
use watermark::{default_db_path, WatermarkStore};

/// po-k_collector — tails Claude Code's session JSONLs and ships events to po-k_server.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Root to walk. Defaults to ~/.claude/projects.
    #[arg(long, env = "PO_K_PROJECTS_ROOT")]
    projects_root: Option<PathBuf>,

    /// Server base URL (no trailing slash).
    #[arg(long, env = "PO_K_SERVER_URL", default_value = "http://127.0.0.1:8787")]
    server_url: String,

    /// API key sent as `X-Api-Key`.
    #[arg(long, env = "PO_K_API_KEY", default_value = "dev")]
    api_key: String,

    /// Stable machine identifier used in session_key derivation.
    #[arg(long, env = "PO_K_MACHINE_ID", default_value = "dev-machine")]
    machine_id: String,

    /// Local watermark database path.
    #[arg(long, env = "PO_K_COLLECTOR_DB")]
    collector_db: Option<PathBuf>,

    /// Path to projects.toml. Defaults to ~/.config/po-k/projects.toml; missing → empty map.
    #[arg(long, env = "PO_K_PROJECTS_FILE")]
    projects_file: Option<PathBuf>,

    /// Backfill, then exit. Default is backfill + live tail.
    #[arg(long, default_value_t = false)]
    once: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,sqlx=warn")),
        )
        .init();

    let cli = Cli::parse();
    let root = cli
        .projects_root
        .clone()
        .or_else(default_projects_root)
        .context("could not resolve ~/.claude/projects; pass --projects-root")?;
    if !root.exists() {
        anyhow::bail!("projects root {} does not exist", root.display());
    }
    let db_path = cli.collector_db.clone().unwrap_or_else(default_db_path);
    let store = WatermarkStore::open(&db_path)
        .await
        .with_context(|| format!("opening watermark store at {}", db_path.display()))?;
    let machine_id = MachineId::from(cli.machine_id.clone());
    let shipper = Shipper::new(cli.server_url.clone(), cli.api_key.clone())?;
    let projects = match cli.projects_file.as_deref() {
        Some(p) => ProjectMap::load(p).context("loading --projects-file")?,
        None => ProjectMap::load_default().context("loading projects.toml")?,
    };

    info!(root = %root.display(), db = %db_path.display(), once = cli.once, "po-k_collector starting");

    // Always do an initial pass — newly-tracked files start at byte 0.
    let stats = scan_and_ship_all(&root, &store, &shipper, &machine_id, &projects).await?;
    info!(
        sent = stats.requested,
        accepted = stats.accepted,
        duplicates = stats.duplicates,
        "backfill pass complete"
    );

    if cli.once {
        return Ok(());
    }

    run_live(root, store, shipper, machine_id, projects).await
}

fn default_projects_root() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".claude").join("projects"))
}

/// Walk every jsonl file under `root` and ship new events from each.
async fn scan_and_ship_all(
    root: &std::path::Path,
    store: &WatermarkStore,
    shipper: &Shipper,
    machine_id: &MachineId,
    projects: &ProjectMap,
) -> Result<ship::ShipStats> {
    // Ship subagent meta sidecars first so they're available when transcripts render.
    // Idempotent server-side (INSERT OR REPLACE), so re-shipping per scan is cheap.
    match meta::read_meta_rows(root, machine_id).await {
        Ok(rows) if !rows.is_empty() => {
            if let Err(e) = shipper.ship_meta(&rows).await {
                warn!(error = %e, "subagent meta ship failed; will retry next scan");
            }
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "subagent meta discovery failed"),
    }

    let files = discover_jsonl_files(root);
    let mut totals = ship::ShipStats::default();
    for path in files {
        match scan_file(root, &path, store, machine_id, projects).await {
            Ok(Some(result)) => {
                if !result.events.is_empty() {
                    let s = shipper.ship(machine_id, &result.events).await?;
                    totals.requested += s.requested;
                    totals.accepted += s.accepted;
                    totals.duplicates += s.duplicates;
                }
                // Only commit watermark after the batch was acked.
                store.upsert(&result.next_watermark).await?;
            }
            Ok(None) => {
                // Nothing new (or file unreadable / unidentifiable).
            }
            Err(e) => warn!(file = %path.display(), error = %e, "scan failed"),
        }
    }
    Ok(totals)
}

/// Long-running mode: notify-debouncer-full watches the projects root for changes
/// and triggers re-scans. A 30s heartbeat covers any missed event.
async fn run_live(
    root: PathBuf,
    store: WatermarkStore,
    shipper: Shipper,
    machine_id: MachineId,
    projects: ProjectMap,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    let tx_for_watch = tx.clone();

    // notify-debouncer-full hands us a Result<Vec<DebouncedEvent>>; we only care about
    // *something* having changed. We forward a unit signal and let the scanner re-read
    // each file from its watermark — cheap because cursors fast-forward to file size.
    let mut debouncer = new_debouncer(
        Duration::from_millis(500),
        None,
        move |res: DebounceEventResult| match res {
            Ok(events) => {
                let interesting = events.iter().any(|e| {
                    matches!(
                        e.kind,
                        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                    )
                });
                if interesting {
                    let _ = tx_for_watch.send(());
                }
            }
            Err(errors) => {
                for err in errors {
                    warn!(error = %err, "notify watcher error");
                }
            }
        },
    )?;
    debouncer.watch(&root, RecursiveMode::Recursive)?;

    info!(root = %root.display(), "live watching for new events");

    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let _shutdown = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("ctrl-c received, shutting down");
        let _ = tx.send(());
    });

    loop {
        // Wait for either a notify event, the heartbeat, or shutdown.
        tokio::select! {
            _ = heartbeat.tick() => {}
            recv = rx.recv() => {
                if recv.is_none() {
                    break;
                }
                // Drain any rapid bursts before scanning.
                while rx.try_recv().is_ok() {}
            }
        }

        match scan_and_ship_all(&root, &store, &shipper, &machine_id, &projects).await {
            Ok(s) if s.requested > 0 => {
                info!(
                    sent = s.requested,
                    accepted = s.accepted,
                    duplicates = s.duplicates,
                    "live tick shipped"
                );
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "live tick failed; will retry on next event"),
        }
    }
    Ok(())
}
