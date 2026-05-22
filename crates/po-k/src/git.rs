//! Thin wrappers around the `git` CLI. We never use git2 — shelling out keeps the
//! dependency surface tiny and means po-k uses whatever git config the user has.

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl CmdOutput {
    pub fn ok(&self) -> bool {
        self.status.success()
    }
}

fn run(args: &[&str], cwd: Option<&Path>) -> Result<CmdOutput> {
    let mut cmd = Command::new("git");
    cmd.args(args);
    if let Some(d) = cwd {
        cmd.current_dir(d);
    }
    let out = cmd.output().context("spawning git (is git on PATH?)")?;
    Ok(CmdOutput {
        status: out.status,
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

pub fn clone(url: &str, branch: &str, target: &Path) -> Result<CmdOutput> {
    run(&["clone", "--branch", branch, url, target.to_str().unwrap_or(".")], None)
}

pub fn pull(repo: &Path) -> Result<CmdOutput> {
    run(&["pull", "--ff-only"], Some(repo))
}

pub fn push(repo: &Path) -> Result<CmdOutput> {
    run(&["push"], Some(repo))
}

pub fn add(repo: &Path, pathspec: &str) -> Result<CmdOutput> {
    run(&["add", pathspec], Some(repo))
}

pub fn commit(repo: &Path, message: &str) -> Result<CmdOutput> {
    run(&["commit", "-m", message], Some(repo))
}
