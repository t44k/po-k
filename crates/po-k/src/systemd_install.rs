//! Render + install a systemd unit for `po-k serve`.
//!
//! Default target: a user unit at `~/.config/systemd/user/po-k.service`.
//! Use `--system` to write `/etc/systemd/system/po-k.service` instead (root
//! required; we don't try to escalate ourselves).

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn unit_text(exe_path: &Path, args: &[&str]) -> String {
    let exe_quoted = exe_path.display();
    let args_joined = args.join(" ");
    format!(
        "[Unit]\n\
         Description=po-k server — Claude Code orchestrator backend\n\
         After=default.target network.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exe_quoted} {args_joined}\n\
         Restart=on-failure\n\
         RestartSec=3\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
    )
}

pub fn install(user: bool) -> Result<()> {
    let exe = std::env::current_exe().context("resolving current_exe")?;
    let text = unit_text(&exe, &["serve", "--foreground"]);
    let target = if user {
        user_unit_path()?
    } else {
        PathBuf::from("/etc/systemd/system/po-k.service")
    };
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&target, text.as_bytes())
        .with_context(|| format!("writing {}", target.display()))?;
    tracing::info!(path = %target.display(), "wrote systemd unit");

    let (daemon_reload, enable_now) = if user {
        (
            vec!["--user", "daemon-reload"],
            vec!["--user", "enable", "--now", "po-k.service"],
        )
    } else {
        (
            vec!["daemon-reload"],
            vec!["enable", "--now", "po-k.service"],
        )
    };
    run_systemctl(&daemon_reload)?;
    run_systemctl(&enable_now)?;

    let status_cmd = if user {
        "systemctl --user status po-k.service"
    } else {
        "systemctl status po-k.service"
    };
    println!("po-k installed as a systemd {} unit.", if user { "user" } else { "system" });
    println!("  Unit: {}", target.display());
    println!("  Check: {status_cmd}");
    Ok(())
}

fn user_unit_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("HOME unset"))?;
    Ok(PathBuf::from(home).join(".config/systemd/user/po-k.service"))
}

fn run_systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .args(args)
        .status()
        .with_context(|| format!("running `systemctl {}`", args.join(" ")))?;
    if !status.success() {
        anyhow::bail!("`systemctl {}` exited with {status}", args.join(" "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_contains_required_fields() {
        let text = unit_text(Path::new("/usr/local/bin/po-k"), &["serve", "--foreground"]);
        assert!(text.contains("Description=po-k"));
        assert!(text.contains("ExecStart=/usr/local/bin/po-k serve --foreground"));
        assert!(text.contains("Restart=on-failure"));
        assert!(text.contains("WantedBy=default.target"));
    }
}
