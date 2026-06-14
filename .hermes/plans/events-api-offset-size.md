# Plan: `/events` and `/messages` API — require `offset` + `size` parameters

## Motivation

The `/events` endpoint returns at most `PAGE_LIMIT = 500` events starting from `seq > since` (default 0). For long-lived sessions with 17K+ events, a caller wanting the latest stop event must paginate through ~35 pages. Background watcher scripts that call `/events?wait=2` without a `since` cursor only see events 1–500 and never find the stop event — causing timeouts.

## API change

Replace implicit defaults with explicit, required parameters:

| Parameter | Current | Proposed |
|-----------|---------|----------|
| `since` → `offset` | Optional, default 0 | **Required**. Cursor: return events with `seq > offset`. Special: `offset=-1` returns the **latest** `size` events (tail page). |
| *(hardcoded)* → `size` | Hardcoded `PAGE_LIMIT = 500` | **Required**. Max events to return. Capped at `MAX_SIZE = 1000`. |
| `wait` | Optional, default 30s | Unchanged. Optional, default 30s, max 60s. |

Missing `offset` or `size` → **400 Bad Request** with message: `"offset and size query parameters are required"`.

Applies to both `/events` and `/messages` (they share the `page()` function).

---

## File-by-file changes

### 1. `crates/po-k/src/core/events.rs`

**Current `page()` signature:**
```rust
pub async fn page(
    state: &AppState,
    sid: &str,
    transcript_only: bool,
    since: i64,
    wait: u64,
) -> CoreResult<CoreResponse>
```

**New signature:**
```rust
pub async fn page(
    state: &AppState,
    sid: &str,
    transcript_only: bool,
    offset: i64,
    size: i64,
    wait: u64,
) -> CoreResult<CoreResponse>
```

**Changes:**
- Remove `PAGE_LIMIT` constant (or repurpose as `MAX_SIZE = 1000`).
- Cap `size` at `MAX_SIZE`.
- Add branching logic:
  - `offset >= 0`: existing behavior — call `select_events_since(db, sid, offset, size)`.
  - `offset == -1`: new tail mode — call `select_events_tail(db, sid, size)`.
- Long-poll `wait` behavior:
  - `offset >= 0`: unchanged — subscribe to bus, wait for notification, re-query.
  - `offset == -1` with `wait > 0`: Subscribe to bus. If initial query returns events, return immediately. If empty (brand new session), wait for notification then re-query with tail. This makes `offset=-1&size=10&wait=30` work for watching brand-new sessions.
- `next_cursor` calculation:
  - `offset >= 0`: `rows.last().map(|r| r.seq).unwrap_or(offset)` (same as current).
  - `offset == -1`: `rows.last().map(|r| r.seq).unwrap_or(0)`.

**Impact on `stream_rows()`:** None. SSE streaming is cursor-based (starts at `since`, waits for new events). `offset=-1` doesn't apply to streaming — it's for one-shot tail reads. Leave `stream_rows()` unchanged; it still takes `since: i64`.

**Impact on `cost()`:** `cost()` calls `select_events_since(db, sid, 0, 100_000)` directly — not through `page()`. It reads ALL events to sum up costs. **No change needed** — `cost()` is an internal consumer of the store, not the HTTP API.

### 2. `crates/po-k/src/events_store.rs`

**Add new function `select_events_tail()`:**
```rust
/// Return the latest `limit` events for a session, in ascending seq order.
/// Used when `offset=-1` to get the tail page without knowing the current cursor.
pub async fn select_events_tail(
    db: &Db,
    sid: &str,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM (
             SELECT sid, seq, ts, kind, payload FROM events
             WHERE sid = ?1
             ORDER BY seq DESC LIMIT ?2
           ) sub ORDER BY seq ASC"#,
    )
    .bind(sid)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT events tail")?;
    // ... map to EventRow (same as select_events_since)
}
```

**Add new function `select_messages_tail()`:**
Same pattern but with the `kind IN (...)` filter for transcript-only events:
```rust
pub async fn select_messages_tail(
    db: &Db,
    sid: &str,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM (
             SELECT sid, seq, ts, kind, payload FROM events
             WHERE sid = ?1
               AND kind IN ('user_prompt','assistant_message','tool_use','tool_result','turn_end')
             ORDER BY seq DESC LIMIT ?2
           ) sub ORDER BY seq ASC"#,
    )
    .bind(sid)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT messages tail")?;
    // ... map to EventRow
}
```

**Existing functions `select_events_since()` and `select_messages_since()`:** Unchanged. They still take `since` and `limit` as before. `cost()` and `stream_rows()` continue to use them.

### 3. `crates/po-k/src/ws_dispatcher.rs`

**Replace `poll_params()` with new parsing that requires offset+size:**

```rust
fn page_params(query: &str) -> Result<(i64, i64, u64), CoreError> {
    let offset = qget(query, "offset")
        .and_then(|s| s.parse::<i64>().ok());
    let size = qget(query, "size")
        .and_then(|s| s.parse::<i64>().ok());
    let wait = qget(query, "wait")
        .and_then(|s| s.parse().ok())
        .unwrap_or(core::events::DEFAULT_WAIT);

    match (offset, size) {
        (Some(o), Some(s)) => Ok((o, s, wait)),
        _ => Err(CoreError::bad_request(
            "offset and size query parameters are required"
        )),
    }
}
```

**Update Route handlers for Events and Messages:**
```rust
(\"GET\", Route::Events) => {
    let (offset, size, wait) = page_params(query)?;
    unary(core::events::page(state, &id, false, offset, size, wait).await)
}
(\"GET\", Route::Messages) => {
    let (offset, size, wait) = page_params(query)?;
    unary(core::events::page(state, &id, true, offset, size, wait).await)
}
```

**SSE stream routes:** Leave unchanged — they still parse `since` with a 0 default from the query.

**Note:** `CoreError::bad_request()` may need to be added if it doesn't exist. Check `CoreError` enum and add a `BadRequest(String)` variant that maps to HTTP 400.

### 4. `crates/po-k/src/core/control.rs`

**No changes.** `cost()` calls `events_store::select_events_since()` directly. `/status` and `/wait` don't use the events page API.

### 5. `crates/xpo-k/src/routed.rs`

**No changes.** Xpo-k is a transparent proxy — it forwards the full path+query to po-k verbatim. The new query parameters pass through automatically.

### 6. Hermes plugin (`hermes-plugin/tools.py` and `hermes-plugin/client.py`)

**Update `get_events()` in `client.py`:**
```python
def get_events(self, sid: str, offset: int = 0, size: int = 100, wait: int = 2) -> Dict[str, Any]:
    params = {"offset": offset, "size": size}
    if wait:
        params["wait"] = wait
    return self._get(f"/sessions/{sid}/events", params=params, timeout=wait + 10)
```

**Update `_handle_pok_events()` in `tools.py`:**
- Add `offset` parameter (default 0) and `size` parameter (default 100).
- Update tool schema to include both params with descriptions.

**Update `pok_wait` tool:** No changes — it doesn't call `/events`.

---

## CoreError changes

Check if `CoreError` already has a 400 variant. If not, add:

```rust
// In core/mod.rs or wherever CoreError is defined
pub enum CoreError {
    NotFound(String),
    BadRequest(String),  // NEW
    Internal(String),
}
```

And map it to HTTP 400 in the response conversion.

---

## Tests to update

### Existing tests in `ws_dispatcher.rs`:
- `test poll_params` → rename to `test page_params`, test with offset+size, test missing params returns error
- `wait_returns_when_ended` → uses `/sessions/s2/wait?since=0&timeout=10` — no change needed (wait still uses `since`)
- Any test calling `/events` or `/messages` needs `?offset=0&size=500` added

### New tests needed:

1. **`events_store::select_events_tail`** — insert 20 events, request tail(5), verify you get events 16-20 in ascending order.
2. **`events_store::select_messages_tail`** — same but with kind filtering.
3. **`page()` with offset=-1** — tail mode returns latest events.
4. **`page()` with offset=-1 and wait** — long-poll fires on new event.
5. **400 on missing offset** — `GET /events?size=10` → 400.
6. **400 on missing size** — `GET /events?offset=0` → 400.
7. **size capped at MAX_SIZE** — `GET /events?offset=0&size=9999` → returns at most 1000.
8. **Existing cursor-based pagination still works** — `offset=500&size=100` returns events 501-600.

---

## Migration / backward compatibility

This is a **breaking change** — existing callers that rely on `?since=N` without `size` will get 400 errors. Affected consumers:

1. **Hermes plugin** (`pok_events`, `pok_wait`) — update in the same PR.
2. **Background watcher scripts** — must be updated to use `offset` + `size`.
3. **SSE stream** (`/events/stream`) — NOT affected (still uses `since`).
4. **Any external curl scripts** — breaking.

Since po-k is internal with a small user base, this is acceptable. Document in the commit message and CHANGELOG.

---

## Summary of file touches

| File | Change |
|------|--------|
| `crates/po-k/src/events_store.rs` | Add `select_events_tail()`, `select_messages_tail()` |
| `crates/po-k/src/core/events.rs` | Update `page()` signature + tail branching, remove/rename `PAGE_LIMIT` |
| `crates/po-k/src/core/mod.rs` | Add `CoreError::BadRequest` if missing |
| `crates/po-k/src/ws_dispatcher.rs` | Replace `poll_params()` with `page_params()`, update Event/Message handlers |
| `hermes-plugin/client.py` | Update `get_events()` params |
| `hermes-plugin/tools.py` | Update `pok_events` schema + handler |
| Tests | Update existing, add 8 new |
