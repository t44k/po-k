//! Canonical event model + session identity for po-k.
//!
//! Every field on `Event` is `#[serde(default)]` and the original JSONL line is preserved
//! verbatim in `raw`, so unknown / future fields are never lost during ingest.

mod event;
mod session_key;

pub use event::{kind, Event, ParseError};
pub use session_key::{MachineId, SessionKey};
