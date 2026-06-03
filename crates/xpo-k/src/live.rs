//! Phase 4: push profile changes to live sessions. Filled in at M4.2.

use crate::state::XState;

pub async fn on_profile_updated(_st: &XState, _name: &str) {
    // M4.2 implements: find live sessions using `name`, re-merge, push
    // `profile_update` to each owning po-k.
}
