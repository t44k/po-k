//! Derived session status for orchestrator control.
//!
//! CC's activity is observable through the ordered per-session event stream
//! (hooks + JSONL transcript + permission tracker + lifecycle). Rather than
//! maintain a separate state machine, we derive a coarse status on demand from
//! the latest seq of each status-relevant event kind. The derivation is pure so
//! it can be unit-tested exhaustively; callers feed it the map produced by
//! [`crate::events_store::latest_status_seqs`].

use std::collections::HashMap;

/// Coarse, orchestrator-facing session status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// CC is actively processing the current turn.
    Working,
    /// CC has paused for the human/orchestrator: an unresolved permission
    /// request, or a notification it's waiting on input.
    AwaitingInput,
    /// CC finished its turn (or never started one) and is ready for input.
    Idle,
    /// The session has ended (CC exited / was killed).
    Ended,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Working => "working",
            Status::AwaitingInput => "awaiting_input",
            Status::Idle => "idle",
            Status::Ended => "ended",
        }
    }

    /// CC is no longer actively producing output for the current turn — i.e.
    /// the orchestrator can resume. Everything except `Working`.
    #[allow(dead_code)]
    pub fn is_stopped(self) -> bool {
        !matches!(self, Status::Working)
    }
}

/// Derive a session's status from the latest seq of each status-relevant event
/// kind (`latest`) plus the `sessions.ended_at` column.
///
/// Returns the status and the seq of the *deciding* boundary event — the event
/// that justifies the status — or `None` when nothing drives it (a fresh
/// session, or one ended purely via `ended_at` with no terminal event).
///
/// Precedence (highest first): ended → awaiting_input → working → idle. See the
/// module/endpoint docs for the rationale; key points:
/// - `ended` is recognised from `session_end`/`cc_exited` events *or* `ended_at`
///   (the kill path appends `cc_exited` before writing `ended_at`).
/// - `subagent_stop` is deliberately excluded from the turn-boundary sets: a
///   subagent finishing does not return the main agent to idle.
/// - CC's post-turn `idle_prompt` notification ("Claude is waiting for your
///   input") is remapped to `idle_notification` at hook ingestion
///   (`core::hooks::ingest`). That kind is not in `latest_status_seqs`'
///   IN-clause, so it never reaches this map and never drives
///   `awaiting_input` — only genuine notifications (e.g. permission prompts)
///   are stored as `notification`.
pub fn derive_status(latest: &HashMap<String, i64>, ended_at: Option<&str>) -> (Status, Option<i64>) {
    let get = |k: &str| latest.get(k).copied();

    // 1. ENDED — terminal lifecycle events, or the ended_at column.
    let terminal = [get("session_end"), get("cc_exited")]
        .into_iter()
        .flatten()
        .max();
    if let Some(seq) = terminal {
        return (Status::Ended, Some(seq));
    }
    if ended_at.is_some() {
        return (Status::Ended, None);
    }

    // Latest "stopped" boundary (turn finished). `subagent_stop` is NOT part of
    // it — a subagent finishing doesn't idle the main agent.
    let stop_seq = [get("stop"), get("turn_end")].into_iter().flatten().max();
    // A turn is *started* by a `user_prompt`. Intra-turn outputs
    // (assistant_message / tool_use / tool_result) are deliberately NOT counted
    // as activity here: the JSONL tailer can flush the turn's final
    // assistant_message a beat AFTER the (fast) Stop hook lands, which would
    // otherwise make a just-finished turn look like new work. Only a fresh
    // `user_prompt` (delivered promptly by the UserPromptSubmit hook) re-opens
    // "working" after a stop.
    let active_seq = get("user_prompt");

    // 2. AWAITING_INPUT — unresolved permission request, or a notification that
    //    is the newest signal.
    let perm_pending = match (get("permission_request"), get("permission_decision")) {
        (Some(req), Some(dec)) => req > dec,
        (Some(_), None) => true,
        _ => false,
    };
    if perm_pending {
        return (Status::AwaitingInput, get("permission_request"));
    }
    if let Some(n) = get("notification") {
        let newest_other = [stop_seq, active_seq].into_iter().flatten().max().unwrap_or(0);
        if n > newest_other {
            return (Status::AwaitingInput, Some(n));
        }
    }

    // 3. WORKING — newest activity is more recent than the last stop boundary.
    match (active_seq, stop_seq) {
        (Some(a), Some(s)) if a > s => return (Status::Working, Some(a)),
        (Some(a), None) => return (Status::Working, Some(a)),
        _ => {}
    }

    // 4. IDLE — last boundary is a stop/turn_end, or no events at all.
    (Status::Idle, stop_seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pairs: &[(&str, i64)]) -> HashMap<String, i64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn fresh_session_is_idle() {
        assert_eq!(derive_status(&m(&[]), None), (Status::Idle, None));
    }

    #[test]
    fn prompt_only_is_working() {
        assert_eq!(derive_status(&m(&[("user_prompt", 5)]), None), (Status::Working, Some(5)));
    }

    #[test]
    fn mid_turn_activity_is_working() {
        // Working is driven by the user_prompt (turn start), not the tool_use.
        let l = m(&[("user_prompt", 5), ("tool_use", 9)]);
        assert_eq!(derive_status(&l, None), (Status::Working, Some(5)));
    }

    #[test]
    fn late_assistant_message_after_stop_is_idle() {
        // The tailer can flush the final assistant_message AFTER the Stop hook
        // (higher seq). That must still read as idle, not working.
        let l = m(&[("user_prompt", 6), ("stop", 10), ("assistant_message", 11)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(10)));
    }

    #[test]
    fn stop_after_prompt_is_idle() {
        let l = m(&[("user_prompt", 5), ("stop", 10)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(10)));
    }

    #[test]
    fn turn_end_is_idle() {
        let l = m(&[("user_prompt", 5), ("turn_end", 10)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(10)));
    }

    #[test]
    fn new_prompt_after_stop_is_working() {
        let l = m(&[("stop", 10), ("user_prompt", 12)]);
        assert_eq!(derive_status(&l, None), (Status::Working, Some(12)));
    }

    #[test]
    fn permission_pending_no_decision_is_awaiting() {
        let l = m(&[("user_prompt", 5), ("permission_request", 8)]);
        assert_eq!(derive_status(&l, None), (Status::AwaitingInput, Some(8)));
    }

    #[test]
    fn permission_request_newer_than_decision_is_awaiting() {
        let l = m(&[("permission_request", 20), ("permission_decision", 14)]);
        assert_eq!(derive_status(&l, None), (Status::AwaitingInput, Some(20)));
    }

    #[test]
    fn permission_resolved_falls_through_to_idle() {
        let l = m(&[("permission_request", 8), ("permission_decision", 9), ("stop", 10)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(10)));
    }

    #[test]
    fn notification_newest_is_awaiting() {
        let l = m(&[("stop", 10), ("notification", 11)]);
        assert_eq!(derive_status(&l, None), (Status::AwaitingInput, Some(11)));
    }

    #[test]
    fn idle_notification_after_stop_stays_idle() {
        // CC's idle_prompt notification is stored as `idle_notification` at
        // hook ingestion and excluded from latest_status_seqs' IN-clause, so
        // it never appears in the map derive_status receives. Even if it did,
        // derive_status never consults the kind — the post-turn sequence
        // stop → idle_notification must read as idle, not awaiting_input.
        let l = m(&[("user_prompt", 5), ("stop", 10), ("idle_notification", 11)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(10)));
    }

    #[test]
    fn notification_then_prompt_is_working() {
        let l = m(&[("notification", 11), ("user_prompt", 13)]);
        assert_eq!(derive_status(&l, None), (Status::Working, Some(13)));
    }

    #[test]
    fn notification_then_stop_is_idle() {
        let l = m(&[("notification", 11), ("stop", 14)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(14)));
    }

    #[test]
    fn subagent_stop_alone_does_not_idle() {
        let l = m(&[("user_prompt", 5), ("subagent_stop", 8)]);
        assert_eq!(derive_status(&l, None), (Status::Working, Some(5)));
    }

    #[test]
    fn subagent_stop_with_real_stop_is_idle() {
        let l = m(&[("user_prompt", 5), ("subagent_stop", 8), ("stop", 9)]);
        assert_eq!(derive_status(&l, None), (Status::Idle, Some(9)));
    }

    #[test]
    fn session_end_is_ended() {
        let l = m(&[("stop", 10), ("session_end", 11)]);
        assert_eq!(derive_status(&l, None), (Status::Ended, Some(11)));
    }

    #[test]
    fn cc_exited_is_ended() {
        assert_eq!(derive_status(&m(&[("cc_exited", 12)]), None), (Status::Ended, Some(12)));
    }

    #[test]
    fn ended_at_without_terminal_event_is_ended() {
        let l = m(&[("stop", 10)]);
        assert_eq!(derive_status(&l, Some("2026-05-27T00:00:00Z")), (Status::Ended, None));
    }

    #[test]
    fn ended_takes_precedence_over_pending_permission() {
        let l = m(&[("permission_request", 8), ("cc_exited", 12)]);
        assert_eq!(derive_status(&l, None), (Status::Ended, Some(12)));
    }
}
