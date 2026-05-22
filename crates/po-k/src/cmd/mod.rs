//! Subcommand entry points. Each module defines an `Args` (clap derive) and an async `run`.

pub mod config_cmd;
pub mod distill_cmd;
pub mod gateway;
pub mod hook;
pub mod init;
pub mod mcp;
pub mod memory;
pub mod service;
pub mod skill;
pub mod status;
