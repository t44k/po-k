//! `po-k config` — print the effective config as YAML.

use anyhow::Result;
use clap::Args as ClapArgs;

use crate::config;

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(_args: Args) -> Result<()> {
    let cfg = config::load_default()?;
    let out = serde_yaml::to_string(&cfg)?;
    print!("{out}");
    Ok(())
}
