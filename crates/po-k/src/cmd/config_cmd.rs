//! `po-k config` — dump the effective merged config (main + every layered overlay).

use anyhow::Result;
use clap::Args as ClapArgs;
use serde::Serialize;

use crate::config;

/// Dump the effective merged config as YAML.
#[derive(Debug, ClapArgs)]
pub struct Args {}

#[derive(Serialize)]
struct DumpRepos<'a> {
    primary: Option<&'a config::Repo>,
    nested: &'a [config::OverlayRepo],
}

#[derive(Serialize)]
struct Dump<'a> {
    repos: DumpRepos<'a>,
    llm: &'a config::Llm,
    service: &'a config::Service,
    gateway: &'a config::Gateway,
    topics: &'a [config::Topic],
}

pub async fn run(_args: Args) -> Result<()> {
    let eff = config::load_effective()?;
    let dump = Dump {
        repos: DumpRepos {
            primary: eff.repo.as_ref(),
            nested: &eff.nested_repos,
        },
        llm: &eff.llm,
        service: &eff.service,
        gateway: &eff.gateway,
        topics: &eff.topics,
    };
    print!("{}", serde_yaml::to_string(&dump)?);
    Ok(())
}
