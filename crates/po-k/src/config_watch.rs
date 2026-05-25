//! Hot-reload `po-k.yaml`. Uses `notify` to watch the file; on change, reload
//! and swap into the shared `AppState`. Logs a diff summary on each reload.
//!
//! Fallback if `notify` fails to install: a 1s mtime poll. Tested by editing
//! the file and watching `GET /projects` reflect the change.

use anyhow::Result;
use notify::{EventKind, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

use crate::config::{load_from, Config};

pub fn spawn(path: PathBuf, current: Arc<RwLock<Config>>) {
    if try_spawn_notify(path.clone(), current.clone()).is_err() {
        tracing::warn!(path = %path.display(), "notify watcher failed to install; falling back to 1s mtime poll");
        tokio::spawn(poll_loop(path, current));
    }
}

fn try_spawn_notify(path: PathBuf, current: Arc<RwLock<Config>>) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    // Watch the parent directory — editors often rename-replace, which loses
    // file-level watches. We filter to events touching our specific file.
    let watch_dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let target = path.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        let Ok(ev) = res else {
            return;
        };
        if !matches!(ev.kind, EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)) {
            return;
        }
        if ev.paths.iter().any(|p| p == &target) {
            let _ = tx.send(());
        }
    })?;
    watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;
    tokio::spawn(async move {
        // Keep the watcher alive for the lifetime of the task.
        let _w = watcher;
        let mut last_reload = SystemTime::UNIX_EPOCH;
        while rx.recv().await.is_some() {
            // Debounce: drain queued change events for 200ms.
            let _ = tokio::time::timeout(Duration::from_millis(200), drain(&mut rx)).await;
            let now = SystemTime::now();
            if now
                .duration_since(last_reload)
                .map(|d| d < Duration::from_millis(100))
                .unwrap_or(false)
            {
                continue;
            }
            last_reload = now;
            reload(&path, &current).await;
        }
    });
    Ok(())
}

async fn drain(rx: &mut tokio::sync::mpsc::UnboundedReceiver<()>) {
    while rx.recv().await.is_some() {
        // keep draining until timeout cuts us off
    }
}

async fn poll_loop(path: PathBuf, current: Arc<RwLock<Config>>) {
    let mut last_mtime = mtime(&path);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = mtime(&path);
        if now != last_mtime {
            last_mtime = now;
            reload(&path, &current).await;
        }
    }
}

fn mtime(path: &PathBuf) -> Option<SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

async fn reload(path: &PathBuf, current: &Arc<RwLock<Config>>) {
    match load_from(path) {
        Ok(new) => {
            let mut guard = current.write().await;
            let diff = diff_projects(&guard, &new);
            *guard = new;
            drop(guard);
            if diff.has_changes() {
                tracing::info!(
                    added = ?diff.added,
                    removed = ?diff.removed,
                    "config reloaded"
                );
            } else {
                tracing::debug!("config reloaded (no project list changes)");
            }
        }
        Err(e) => tracing::warn!(error = %e, path = %path.display(), "config reload failed; keeping previous config"),
    }
}

#[derive(Debug)]
pub struct Diff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

impl Diff {
    pub fn has_changes(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty()
    }
}

pub fn diff_projects(old: &Config, new: &Config) -> Diff {
    let old_names: HashSet<&str> = old.projects.iter().map(|p| p.name.as_str()).collect();
    let new_names: HashSet<&str> = new.projects.iter().map(|p| p.name.as_str()).collect();
    let added = new_names
        .difference(&old_names)
        .map(|s| s.to_string())
        .collect();
    let removed = old_names
        .difference(&new_names)
        .map(|s| s.to_string())
        .collect();
    Diff { added, removed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Project;

    fn p(name: &str) -> Project {
        Project {
            name: name.into(),
            cwd: "/x".into(),
            model: None,
            effort: None,
            add_dirs: vec![],
            zellij_session: None,
        }
    }

    #[test]
    fn diff_detects_add_and_remove() {
        let old = Config {
            projects: vec![p("a"), p("b")],
            ..Default::default()
        };
        let new = Config {
            projects: vec![p("b"), p("c")],
            ..Default::default()
        };
        let d = diff_projects(&old, &new);
        assert_eq!(d.added, vec!["c".to_string()]);
        assert_eq!(d.removed, vec!["a".to_string()]);
        assert!(d.has_changes());
    }

    #[test]
    fn diff_no_change() {
        let old = Config {
            projects: vec![p("a")],
            ..Default::default()
        };
        let d = diff_projects(&old, &old.clone());
        assert!(!d.has_changes());
    }
}
