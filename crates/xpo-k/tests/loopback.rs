//! Keystone loopback test (M2.9): start Xpo-k in-process, connect a fake po-k
//! over WebSocket, and drive the full HTTP→WS→HTTP round-trip — registration,
//! routed unary calls, and the SSE stream bridge — without any real CC/zellij.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use pok_proto::{ProjectDecl, SessionDecl, WsMsg};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use xpo_k::auth::Token;
use xpo_k::config::Config;
use xpo_k::state::XState;
use xpo_k::store;

async fn start_server() -> (SocketAddr, XState) {
    let dir = tempfile::tempdir().unwrap();
    let db = store::open(&dir.path().join("p.db")).await.unwrap();
    std::mem::forget(dir); // keep the temp dir alive for the test process
    let state = XState::new(Config::default(), Token::new("secret".into()), db);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = xpo_k::app(state.clone());
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    // Give the server a moment to be ready.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, state)
}

type PokSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
type PokStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

/// Connect a fake po-k and register it owning project "demo" + session "s1".
async fn connect_fake_pok(addr: SocketAddr) -> (PokSink, PokStream) {
    connect_fake_pok_as(
        addr,
        "pok-1",
        "host",
        "demo",
        "/demo",
        vec![SessionDecl {
            sid: "s1".into(),
            project: "demo".into(),
            status: "idle".into(),
        }],
    )
    .await
}

/// Connect a fake po-k under an explicit identity, owning one project. Lets
/// tests set up two connected instances that declare a project of the same
/// name — the scenario that exposed the routing-precedence bug (two Zirzen
/// clones of the same repo, each with an identically-named project).
async fn connect_fake_pok_as(
    addr: SocketAddr,
    pok_id: &str,
    hostname: &str,
    project_name: &str,
    project_cwd: &str,
    sessions: Vec<SessionDecl>,
) -> (PokSink, PokStream) {
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let (mut sink, mut stream) = ws.split();
    let reg = WsMsg::Register {
        pok_id: pok_id.into(),
        hostname: hostname.into(),
        version: "0".into(),
        projects: vec![ProjectDecl {
            name: project_name.into(),
            cwd: project_cwd.into(),
        }],
        sessions,
        caps: Default::default(),
    };
    sink.send(Message::Text(serde_json::to_string(&reg).unwrap()))
        .await
        .unwrap();
    // Expect a `registered` ack.
    let ack = next_msg(&mut stream).await;
    assert!(matches!(ack, WsMsg::Registered { .. }));
    (sink, stream)
}

async fn next_msg<S>(stream: &mut S) -> WsMsg
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match stream.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

#[tokio::test]
async fn registry_reflects_connected_pok() {
    let (addr, _state) = start_server().await;
    let (_sink, _stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let v: serde_json::Value = client
        .get(format!("http://{addr}/registry"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v[0]["pok_id"], "pok-1");
    assert_eq!(v[0]["projects"][0], "demo");
}

#[tokio::test]
async fn routed_unary_round_trip() {
    let (addr, _state) = start_server().await;
    let (mut sink, mut stream) = connect_fake_pok(addr).await;

    // The fake po-k answers the first ws_request with a canned response.
    let responder = tokio::spawn(async move {
        if let WsMsg::WsRequest {
            request_id, path, ..
        } = next_msg(&mut stream).await
        {
            assert_eq!(path, "/sessions/s1/status");
            let resp = WsMsg::WsResponse {
                request_id,
                status: 200,
                headers: Default::default(),
                body: r#"{"status":"idle","cursor":0}"#.into(),
            };
            sink.send(Message::Text(serde_json::to_string(&resp).unwrap()))
                .await
                .unwrap();
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/sessions/s1/status"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["status"], "idle");
    responder.await.unwrap();
}

#[tokio::test]
async fn sse_stream_bridge() {
    let (addr, _state) = start_server().await;
    let (mut sink, mut stream) = connect_fake_pok(addr).await;

    let responder = tokio::spawn(async move {
        if let WsMsg::WsRequest {
            request_id,
            stream: true,
            ..
        } = next_msg(&mut stream).await
        {
            for i in 0..2 {
                let chunk = WsMsg::WsStreamChunk {
                    request_id,
                    data: format!("event: message\ndata: {{\"seq\":{i}}}\n\n"),
                };
                sink.send(Message::Text(serde_json::to_string(&chunk).unwrap()))
                    .await
                    .unwrap();
            }
            let end = WsMsg::WsStreamEnd { request_id };
            sink.send(Message::Text(serde_json::to_string(&end).unwrap()))
                .await
                .unwrap();
        }
    });

    let client = reqwest::Client::new();
    let body = client
        .get(format!("http://{addr}/sessions/s1/events/stream"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("event: message"));
    assert!(body.contains("\"seq\":0"));
    assert!(body.contains("\"seq\":1"));
    responder.await.unwrap();
}

// ---------------------------------------------------------------------------
// Regression: explicit `host`/`pok_id` targeting on `POST /sessions` must not
// be silently overridden by project-name lookup. `Registry::project_to_pok`
// is a single fleet-wide map keyed only by name, so two connected po-k
// instances that happen to declare an identically-named project (e.g. two
// clones of the same repo) collapse onto whichever registered last. A caller
// that disambiguates with an explicit `host`/`pok_id` must still land on the
// instance it asked for, not on whoever currently "owns" that project name.
// ---------------------------------------------------------------------------

/// Fake po-k that answers exactly one `POST /sessions` with a session id that
/// reveals which instance actually handled it.
fn spawn_create_responder(
    mut stream: PokStream,
    mut sink: PokSink,
    session_id: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let WsMsg::WsRequest {
            request_id, path, ..
        } = next_msg(&mut stream).await
        {
            assert_eq!(path, "/sessions");
            let resp = WsMsg::WsResponse {
                request_id,
                status: 201,
                headers: Default::default(),
                body: format!(r#"{{"session_id":"{session_id}"}}"#),
            };
            sink.send(Message::Text(serde_json::to_string(&resp).unwrap()))
                .await
                .unwrap();
        }
    })
}

#[tokio::test]
async fn explicit_host_target_wins_over_ambiguous_project_name() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // Two clones of the same repo: identical project name, distinct identity.
    // pok-b registers second, so the fleet-wide project map now points at it
    // — the precondition the old bug relied on.
    let (sink_a, stream_a) =
        connect_fake_pok_as(addr, "pok-a", "host-a", "shared", "/ws", vec![]).await;
    let (_sink_b, _stream_b) =
        connect_fake_pok_as(addr, "pok-b", "host-b", "shared", "/ws", vec![]).await;

    let responder = spawn_create_responder(stream_a, sink_a, "sess-a");

    let created: serde_json::Value = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "host": "host-a"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        created["session_id"], "sess-a",
        "explicit host=host-a must win even though pok-b owns 'shared' in the fleet-wide map"
    );

    tokio::time::timeout(Duration::from_secs(3), responder)
        .await
        .expect("pok-a never received the request")
        .unwrap();
}

/// The same misroute, but by `pok_id` instead of `host`, and checked against
/// both instances to rule out "it just happened to hit the right one".
#[tokio::test]
async fn explicit_pok_id_target_wins_over_ambiguous_project_name() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let (sink_a, stream_a) =
        connect_fake_pok_as(addr, "pok-a", "host-a", "shared", "/ws", vec![]).await;
    let (_sink_b, _stream_b) =
        connect_fake_pok_as(addr, "pok-b", "host-b", "shared", "/ws", vec![]).await;
    // pok-b is last-registered and so owns "shared" in the fleet-wide map —
    // targeting pok-a by id must still reach pok-a, not the project owner.

    let responder = spawn_create_responder(stream_a, sink_a, "sess-a");
    let created: serde_json::Value = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "pok_id": "pok-a"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["session_id"], "sess-a");
    tokio::time::timeout(Duration::from_secs(3), responder)
        .await
        .expect("pok-a never received the request")
        .unwrap();
}

/// Sequential repro of the reported symptom: create → delete → create again,
/// always targeting the same host explicitly. Each attempt must land on the
/// same instance, even though the fleet-wide project map still points
/// elsewhere the whole time.
#[tokio::test]
async fn sequential_creates_keep_routing_to_the_explicitly_targeted_host() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let (sink_a, stream_a) =
        connect_fake_pok_as(addr, "pok-a", "host-a", "shared", "/ws", vec![]).await;
    let (_sink_b, _stream_b) =
        connect_fake_pok_as(addr, "pok-b", "host-b", "shared", "/ws", vec![]).await;

    let responder1 = spawn_create_responder(stream_a, sink_a, "sess-a-1");
    let first: serde_json::Value = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "host": "host-a"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["session_id"], "sess-a-1");
    tokio::time::timeout(Duration::from_secs(3), responder1)
        .await
        .expect("pok-a never received the first request")
        .unwrap();

    // "Delete" is a no-op here (no real session state on the fake po-k) —
    // what matters is that a second, independent create targeting host-a
    // again reaches pok-a, not pok-b.
    let (sink_a2, stream_a2) =
        connect_fake_pok_as(addr, "pok-a", "host-a", "shared", "/ws", vec![]).await;
    let responder2 = spawn_create_responder(stream_a2, sink_a2, "sess-a-2");
    let second: serde_json::Value = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "host": "host-a"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        second["session_id"], "sess-a-2",
        "a second, later create targeting host-a must still reach pok-a"
    );
    tokio::time::timeout(Duration::from_secs(3), responder2)
        .await
        .expect("pok-a never received the second request")
        .unwrap();
}

/// Concurrent repro: two creates fire at once, each targeting a different
/// host explicitly. Neither may cross-route to the other's instance.
#[tokio::test]
async fn concurrent_creates_to_distinct_hosts_do_not_cross_route() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let (sink_a, stream_a) =
        connect_fake_pok_as(addr, "pok-a", "host-a", "shared", "/ws", vec![]).await;
    let (sink_b, stream_b) =
        connect_fake_pok_as(addr, "pok-b", "host-b", "shared", "/ws", vec![]).await;

    let responder_a = spawn_create_responder(stream_a, sink_a, "sess-a");
    let responder_b = spawn_create_responder(stream_b, sink_b, "sess-b");

    let req_a = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "host": "host-a"}))
        .send();
    let req_b = client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"project": "shared", "host": "host-b"}))
        .send();
    let (resp_a, resp_b) = tokio::join!(req_a, req_b);

    let created_a: serde_json::Value = resp_a.unwrap().json().await.unwrap();
    let created_b: serde_json::Value = resp_b.unwrap().json().await.unwrap();
    assert_eq!(
        created_a["session_id"], "sess-a",
        "host-a must never see pok-b's response"
    );
    assert_eq!(
        created_b["session_id"], "sess-b",
        "host-b must never see pok-a's response"
    );

    tokio::time::timeout(Duration::from_secs(3), responder_a)
        .await
        .expect("pok-a never received its request")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), responder_b)
        .await
        .expect("pok-b never received its request")
        .unwrap();
}

#[tokio::test]
async fn profile_update_pushes_to_live_session() {
    let (addr, _state) = start_server().await;
    let (mut sink, mut stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // A profile the session will use.
    client
        .post(format!("{base}/profiles"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "name": "base", "claude_md": "# v1", "agents": { "a": {} } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Fake po-k: answer the create ws_request, then capture the ProfileUpdate.
    let handle = tokio::spawn(async move {
        loop {
            match next_msg(&mut stream).await {
                WsMsg::WsRequest {
                    request_id, path, ..
                } if path == "/sessions" => {
                    let resp = WsMsg::WsResponse {
                        request_id,
                        status: 201,
                        headers: Default::default(),
                        body: r#"{"session_id":"s1"}"#.into(),
                    };
                    sink.send(Message::Text(serde_json::to_string(&resp).unwrap()))
                        .await
                        .unwrap();
                }
                WsMsg::ProfileUpdate {
                    session_id,
                    changed_fields,
                    ..
                } => {
                    break Some((session_id, changed_fields));
                }
                _ => {}
            }
        }
    });

    // Create a session bound to profile "base".
    client
        .post(format!("{base}/sessions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "project": "demo", "profiles": ["base"] }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Update the profile → should push a profile_update to the live session.
    client
        .put(format!("{base}/profiles/base"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "name": "base", "claude_md": "# v2", "agents": { "a": {} } }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let update = tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("timed out waiting for profile_update")
        .unwrap()
        .expect("no profile_update received");
    assert_eq!(update.0, "s1");
    // agents present → structural change flagged.
    assert!(update.1.contains(&"agents".to_string()));
    assert!(update.1.contains(&"claude_md".to_string()));
}

#[tokio::test]
async fn profile_crud_and_merge() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    for (name, model) in [("base", "sonnet"), ("rev", "opus")] {
        client
            .post(format!("{base}/profiles"))
            .bearer_auth("secret")
            .json(&serde_json::json!({
                "name": name,
                "claude_md": format!("# {name}"),
                "settings": { "model": model }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let merged: serde_json::Value = client
        .post(format!("{base}/profiles/merge"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "profiles": ["base", "rev"] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // rev (last) wins on model; both CLAUDE.md sections present.
    assert_eq!(merged["settings"]["model"], "opus");
    let md = merged["claude_md"].as_str().unwrap();
    assert!(md.contains("## From profile: base"));
    assert!(md.contains("## From profile: rev"));
}

// ---------------------------------------------------------------------------
// M15: notification subscriptions end-to-end (HTTP → WS → queue → HTTP)
// ---------------------------------------------------------------------------

/// Subscribe, have the fake po-k push a `stop`, collect it over HTTP, ack it.
#[tokio::test]
async fn subscription_queues_forwarded_stop_and_ack_clears_it() {
    let (addr, _state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // Subscribe with an explicit cursor so no po-k round trip is needed.
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1", "subscriber": "hermes-test", "cursor": 0
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sub["session_id"], "s1");
    assert_eq!(sub["cursor_source"], "explicit");
    let sub_id = sub["subscription_id"].as_str().unwrap().to_string();
    // Defaults are reported back so the agent knows what it will receive.
    assert!(sub["kinds"].as_array().unwrap().iter().any(|k| k == "stop"));

    // po-k forwards the turn's stop (with the seq M15 added to the envelope).
    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::json!({"last_assistant_message": "done"}),
            seq: 12,
            ts: "2026-08-03T10:00:00Z".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    // Long-poll picks it up without any /wait ever having been in flight.
    let pending: serde_json::Value = client
        .get(format!(
            "{base}/notifications?subscriber=hermes-test&wait=5"
        ))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["count"], 1, "got {pending}");
    let n = &pending["notifications"][0];
    assert_eq!(n["kind"], "stop");
    assert_eq!(n["seq"], 12);
    assert_eq!(n["session_id"], "s1");
    assert_eq!(n["subscription_id"].as_str().unwrap(), sub_id);
    assert_eq!(
        n["payload"]["event"]["payload"]["last_assistant_message"],
        "done"
    );
    let ntf_id = n["id"].as_str().unwrap().to_string();

    // Reading doesn't consume.
    let again: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=hermes-test"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again["count"], 1, "unacked notifications stay pending");

    // Ack, then it's gone; re-acking is idempotent.
    let acked: serde_json::Value = client
        .post(format!("{base}/notifications/ack"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "ids": [ntf_id.clone()] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(acked["acked"], 1);
    let re: serde_json::Value = client
        .post(format!("{base}/notifications/ack"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "ids": [ntf_id] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(re["acked"], 0);
    assert_eq!(re["already_acked"], 1);

    let empty: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=hermes-test"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(empty["count"], 0);

    // The subscription is still listed (and its cursor advanced past the ack).
    let list: serde_json::Value = client
        .get(format!("{base}/subscriptions?subscriber=hermes-test"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["count"], 1);
    assert_eq!(list["subscriptions"][0]["cursor"], 12);

    // Unsubscribe.
    let del = client
        .delete(format!(
            "{base}/subscriptions/{}",
            sub["subscription_id"].as_str().unwrap()
        ))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 200);
    let after: serde_json::Value = client
        .get(format!("{base}/subscriptions?subscriber=hermes-test"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after["count"], 0);
}

/// A `status_update` is the level-triggered safety net: it notifies even when
/// the sequenced event that caused the transition never arrived.
#[tokio::test]
async fn status_update_notifies_and_events_for_other_sessions_do_not() {
    let (addr, _state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "session_id": "s1", "subscriber": "sub-a", "cursor": 0 }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // An event for a session nobody subscribed to must not enqueue anything.
    for msg in [
        WsMsg::SessionEvent {
            sid: "other".into(),
            event: pok_proto::EventEnvelope {
                kind: "stop".into(),
                payload: serde_json::Value::Null,
                seq: 3,
                ts: "t".into(),
            },
        },
        WsMsg::StatusUpdate {
            sid: "s1".into(),
            status: "idle".into(),
        },
    ] {
        sink.send(Message::Text(serde_json::to_string(&msg).unwrap()))
            .await
            .unwrap();
    }

    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=sub-a&wait=5"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        pending["count"], 1,
        "only the subscribed session notifies: {pending}"
    );
    assert_eq!(pending["notifications"][0]["kind"], "status");
    assert_eq!(pending["notifications"][0]["status"], "idle");
}

/// On reconnect Xpo-k replays what po-k persisted while the uplink was down,
/// using the existing `/events` page API. Replaying an already-delivered window
/// is idempotent.
#[tokio::test]
async fn reconnect_replays_missed_events_once() {
    let (addr, _state) = start_server().await;
    let (sink, stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "session_id": "s1", "subscriber": "sub-r", "cursor": 0 }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Drop the connection without ever forwarding the stop.
    drop(sink);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect; answer the replay request with the persisted events.
    let (mut sink2, mut stream2) = connect_fake_pok(addr).await;
    let replayed = tokio::spawn(async move {
        loop {
            if let WsMsg::WsRequest {
                request_id, path, ..
            } = next_msg(&mut stream2).await
            {
                if path.starts_with("/sessions/s1/events") {
                    let resp = WsMsg::WsResponse {
                        request_id,
                        status: 200,
                        headers: Default::default(),
                        body: r#"{"events":[{"seq":5,"ts":"t","kind":"stop"}],"next_cursor":5}"#
                            .into(),
                    };
                    sink2
                        .send(Message::Text(serde_json::to_string(&resp).unwrap()))
                        .await
                        .unwrap();
                    break path;
                }
            }
        }
    });
    let path = tokio::time::timeout(Duration::from_secs(5), replayed)
        .await
        .expect("no replay request arrived")
        .unwrap();
    assert!(
        path.contains("offset=0"),
        "replay must resume from the cursor: {path}"
    );

    // Give the matcher a moment, then assert exactly one notification.
    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=sub-r&wait=5"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["count"], 1, "{pending}");
    assert_eq!(pending["notifications"][0]["seq"], 5);

    // A duplicate push of the same seq changes nothing (at-least-once, deduped).
    let dup = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::Value::Null,
            seq: 5,
            ts: "t".into(),
        },
    };
    let (mut sink3, _s3) = connect_fake_pok(addr).await;
    sink3
        .send(Message::Text(serde_json::to_string(&dup).unwrap()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=sub-r"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after["count"], 1, "duplicate suppressed: {after}");
}

/// The new endpoints sit behind the bearer middleware like everything else.
#[tokio::test]
async fn subscription_endpoints_require_auth() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    for (method, path) in [("GET", "/subscriptions"), ("GET", "/notifications")] {
        let r = client
            .request(method.parse().unwrap(), format!("{base}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401, "{method} {path} must require auth");
    }
    let r = client
        .post(format!("{base}/subscriptions"))
        .json(&serde_json::json!({ "session_id": "s1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // …and bad input is rejected with 400, not 500.
    let r = client
        .post(format!("{base}/notifications/ack"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "ids": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

// ---------------------------------------------------------------------------
// M16: webhook push. A stub receiver stands in for Hermes' webhook adapter —
// same contract (HMAC over the exact body, X-Request-ID idempotency, 2xx /
// duplicate responses), no network beyond loopback.
// ---------------------------------------------------------------------------

use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Received {
    /// (signature, request_id, raw_body)
    calls: Vec<(String, String, String)>,
}

/// Behaviour the stub should exhibit for each request, in order.
#[derive(Clone, Copy)]
enum StubReply {
    Accept,
    Duplicate,
    ServerError,
    Unauthorized,
}

async fn start_stub_receiver(replies: Vec<StubReply>) -> (SocketAddr, Arc<Mutex<Received>>) {
    let seen: Arc<Mutex<Received>> = Arc::new(Mutex::new(Received::default()));
    let seen_clone = seen.clone();
    let replies = Arc::new(Mutex::new(
        replies
            .into_iter()
            .collect::<std::collections::VecDeque<_>>(),
    ));
    let app = axum::Router::new().route(
        "/webhooks/pok",
        axum::routing::post(move |headers: axum::http::HeaderMap, body: String| {
            let seen = seen_clone.clone();
            let replies = replies.clone();
            async move {
                let sig = headers
                    .get("x-webhook-signature")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let rid = headers
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                seen.lock().unwrap().calls.push((sig, rid, body));
                let reply = replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(StubReply::Accept);
                match reply {
                    StubReply::Accept => (
                        axum::http::StatusCode::ACCEPTED,
                        axum::Json(serde_json::json!({"status": "accepted"})),
                    ),
                    StubReply::Duplicate => (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({"status": "duplicate"})),
                    ),
                    StubReply::ServerError => (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(serde_json::json!({"error": "boom"})),
                    ),
                    StubReply::Unauthorized => (
                        axum::http::StatusCode::UNAUTHORIZED,
                        axum::Json(serde_json::json!({"error": "bad signature"})),
                    ),
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, seen)
}

/// Expected HMAC-SHA256, computed independently of the implementation under
/// test (same crates, separate code path).
fn expected_sig(secret: &str, body: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

async fn subscribe_with_webhook(
    base: &str,
    client: &reqwest::Client,
    receiver: SocketAddr,
    subscriber: &str,
) -> serde_json::Value {
    client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1",
            "subscriber": subscriber,
            "cursor": 0,
            "deliver": {
                "url": format!("http://{receiver}/webhooks/pok"),
                "secret_env": "POK_TEST_WEBHOOK_SECRET"
            }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn webhook_push_signs_the_exact_body_and_sends_the_notification_id() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, seen) = start_stub_receiver(vec![StubReply::Accept]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let sub = subscribe_with_webhook(&base, &client, recv_addr, "push-a").await;
    assert_eq!(sub["deliver"]["mode"], "webhook");
    assert_eq!(sub["deliver"]["secret_source"], "env");
    assert_eq!(sub["deliver"]["secret_ref"], "POK_TEST_WEBHOOK_SECRET");
    let raw = serde_json::to_string(&sub).unwrap();
    assert!(
        !raw.contains("hmac-test-secret"),
        "secret leaked into the response"
    );

    // po-k forwards a completion, carrying CC prose in the payload.
    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::json!({"last_assistant_message": "TOP SECRET PROSE"}),
            seq: 214,
            ts: "2026-08-03T10:00:00Z".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    // The delivery loop is driven explicitly so the test is deterministic.
    let http = reqwest::Client::new();
    let mut delivered = 0;
    for _ in 0..40 {
        let (_attempted, d) = xpo_k::deliver::run_pass(&state, &http).await;
        delivered += d;
        if delivered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(delivered, 1, "the push must go out");

    let calls = seen.lock().unwrap().calls.clone();
    assert_eq!(calls.len(), 1);
    let (sig, rid, body) = &calls[0];

    // Signature covers the exact bytes the receiver got.
    assert_eq!(
        *sig,
        expected_sig("hmac-test-secret", body),
        "HMAC mismatch"
    );
    // X-Request-ID is the notification id → the adapter can dedupe on it.
    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=push-a"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ntf = &pending["notifications"][0];
    assert_eq!(rid, ntf["id"].as_str().unwrap());
    // Metadata only — no CC prose in the push body.
    assert!(
        !body.contains("TOP SECRET PROSE"),
        "CC prose pushed: {body}"
    );
    assert!(!body.contains("last_assistant_message"));
    let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(parsed["event_type"], "pok_notification");
    assert_eq!(parsed["session_id"], "s1");
    assert_eq!(parsed["seq"], 214);
    assert_eq!(parsed["kind"], "stop");
    assert_eq!(parsed["notification_id"], ntf["id"]);

    // Delivered ≠ handled: still pending until the woken turn acks.
    assert_eq!(pending["count"], 1, "a pushed notification stays pollable");
    assert_eq!(ntf["delivery_state"], "delivered");

    // A second pass must not re-push (idempotent success handling).
    let (attempted, _) = xpo_k::deliver::run_pass(&state, &http).await;
    assert_eq!(attempted, 0, "delivered rows are not retried");
    assert_eq!(seen.lock().unwrap().calls.len(), 1);
}

#[tokio::test]
async fn webhook_duplicate_response_counts_as_delivered() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, seen) = start_stub_receiver(vec![StubReply::Duplicate]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    subscribe_with_webhook(&base, &client, recv_addr, "push-dup").await;

    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::Value::Null,
            seq: 9,
            ts: "t".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let mut delivered = 0;
    for _ in 0..40 {
        let (_a, d) = xpo_k::deliver::run_pass(&state, &http).await;
        delivered += d;
        if delivered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(delivered, 1, "duplicate is success — Hermes already has it");
    assert_eq!(seen.lock().unwrap().calls.len(), 1);
    // No second turn can be created, and no retry storm follows.
    let (attempted, _) = xpo_k::deliver::run_pass(&state, &http).await;
    assert_eq!(attempted, 0);
}

#[tokio::test]
async fn webhook_failure_retries_with_backoff_and_keeps_it_pollable() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, seen) = start_stub_receiver(vec![StubReply::ServerError]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    subscribe_with_webhook(&base, &client, recv_addr, "push-fail").await;

    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::Value::Null,
            seq: 11,
            ts: "t".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let mut attempted_total = 0;
    for _ in 0..40 {
        let (a, _d) = xpo_k::deliver::run_pass(&state, &http).await;
        attempted_total += a;
        if attempted_total > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(attempted_total, 1);
    assert_eq!(seen.lock().unwrap().calls.len(), 1, "one attempt so far");

    // Backoff: not retried immediately…
    let (again, _) = xpo_k::deliver::run_pass(&state, &http).await;
    assert_eq!(again, 0, "must wait for the backoff window");
    assert_eq!(seen.lock().unwrap().calls.len(), 1);

    // …and the notification is untouched from the agent's point of view, so the
    // cron fallback still delivers it.
    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=push-fail"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["count"], 1);
    assert_eq!(pending["notifications"][0]["delivery_state"], "pending");
    assert_eq!(pending["notifications"][0]["delivery_attempts"], 1);

    // The operator can see it in the subscription listing.
    let subs: serde_json::Value = client
        .get(format!("{base}/subscriptions?subscriber=push-fail"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(subs["subscriptions"][0]["delivery"]["delivery_pending"], 1);
    assert_eq!(subs["subscriptions"][0]["delivery"]["unacked"], 1);
    // …and the listing never exposes the secret.
    let raw = serde_json::to_string(&subs).unwrap();
    assert!(!raw.contains("hmac-test-secret"));
    assert_eq!(subs["subscriptions"][0]["deliver"]["mode"], "webhook");
}

#[tokio::test]
async fn a_permanent_rejection_parks_the_delivery_without_acking() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, _seen) = start_stub_receiver(vec![StubReply::Unauthorized]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    subscribe_with_webhook(&base, &client, recv_addr, "push-401").await;

    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::Value::Null,
            seq: 3,
            ts: "t".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    let http = reqwest::Client::new();
    for _ in 0..40 {
        let (a, _d) = xpo_k::deliver::run_pass(&state, &http).await;
        if a > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=push-401"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        pending["count"], 1,
        "still pollable — cron fallback recovers it"
    );
    assert_eq!(pending["notifications"][0]["delivery_state"], "failed");
    // No further attempts are made against a misconfigured route.
    let (attempted, _) = xpo_k::deliver::run_pass(&state, &http).await;
    assert_eq!(attempted, 0);
}

#[tokio::test]
async fn subscription_delivery_target_is_patchable_and_validated() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // Poll-only to start with.
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"session_id": "s1", "subscriber": "patch-me", "cursor": 0}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sub["deliver"]["mode"], "poll");
    let id = sub["subscription_id"].as_str().unwrap().to_string();

    // A target without a secret reference is refused outright.
    let bad = client
        .patch(format!("{base}/subscriptions/{id}"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"deliver": {"url": "http://127.0.0.1:9/webhooks/pok"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    // So is a non-http scheme.
    let bad2 = client
        .patch(format!("{base}/subscriptions/{id}"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "deliver": {"url": "file:///etc/passwd", "secret_env": "X"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad2.status(), 400);

    // A valid target switches the mode…
    let ok: serde_json::Value = client
        .patch(format!("{base}/subscriptions/{id}"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "deliver": {"url": "http://127.0.0.1:9/webhooks/pok", "secret_env": "POK_TEST_WEBHOOK_SECRET"}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ok["deliver"]["mode"], "webhook");

    // …and clear_deliver takes it back to poll-only.
    let cleared: serde_json::Value = client
        .patch(format!("{base}/subscriptions/{id}"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"clear_deliver": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cleared["deliver"]["mode"], "poll");

    // Unknown id → 404, and the endpoint is behind auth.
    let missing = client
        .patch(format!("{base}/subscriptions/sub-nope"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"clear_deliver": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    let unauth = client
        .patch(format!("{base}/subscriptions/{id}"))
        .json(&serde_json::json!({"clear_deliver": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);
}

#[tokio::test]
async fn a_missing_secret_is_a_config_error_not_a_retry_storm() {
    std::env::remove_var("POK_TEST_MISSING_SECRET");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1", "subscriber": "no-secret", "cursor": 0,
            "deliver": {"url": "http://127.0.0.1:9/webhooks/pok",
                        "secret_env": "POK_TEST_MISSING_SECRET"}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::Value::Null,
            seq: 2,
            ts: "t".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    let http = reqwest::Client::new();
    for _ in 0..40 {
        let (a, _d) = xpo_k::deliver::run_pass(&state, &http).await;
        if a > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (attempted, _) = xpo_k::deliver::run_pass(&state, &http).await;
    assert_eq!(attempted, 0, "a missing secret is not retried in a loop");
    let pending: serde_json::Value = client
        .get(format!("{base}/notifications?subscriber=no-secret"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["count"], 1, "and the cron fallback still has it");
    assert_eq!(pending["notifications"][0]["delivery_state"], "failed");
}

// ---------------------------------------------------------------------------
// M17: origin correlation + workflow lifecycle end to end.
// ---------------------------------------------------------------------------

const ORIGIN: fn() -> serde_json::Value = || {
    serde_json::json!({
        "platform": "zulip",
        "chat_id": "stream:eng",
        "chat_name": "eng",
        "chat_type": "channel",
        "thread_id": "deploy-bug",
        "message_id": "9001",
        "user_id": "42",
        "user_name": "Tamas",
        "session_key": "agent:main:zulip:channel:stream:eng:deploy-bug",
        "hint": "asked in #eng > deploy-bug"
    })
};

#[tokio::test]
async fn origin_flows_from_subscribe_into_the_push_envelope() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, seen) = start_stub_receiver(vec![StubReply::Accept]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1",
            "subscriber": "origin-a",
            "cursor": 0,
            "origin": ORIGIN(),
            "max_turns": 3,
            "deliver": {
                "url": format!("http://{recv_addr}/webhooks/pok"),
                "secret_env": "POK_TEST_WEBHOOK_SECRET"
            }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // The subscribe response echoes both correlation halves.
    assert_eq!(sub["origin"]["thread_id"], "deploy-bug");
    let wf_id = sub["workflow"]["workflow_id"].as_str().unwrap().to_string();
    assert!(wf_id.starts_with("wf-"), "{sub}");
    assert_eq!(sub["workflow"]["state"], "active");
    assert_eq!(sub["workflow"]["max_turns"], 3);
    assert_eq!(sub["workflow"]["origin"]["chat_id"], "stream:eng");

    // po-k reports the turn boundary.
    let ev = WsMsg::SessionEvent {
        sid: "s1".into(),
        event: pok_proto::EventEnvelope {
            kind: "stop".into(),
            payload: serde_json::json!({"last_assistant_message": "SECRET PROSE"}),
            seq: 77,
            ts: "2026-08-03T10:00:00Z".into(),
        },
    };
    sink.send(Message::Text(serde_json::to_string(&ev).unwrap()))
        .await
        .unwrap();

    let http = reqwest::Client::new();
    for _ in 0..40 {
        let (_a, d) = xpo_k::deliver::run_pass(&state, &http).await;
        if d > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let calls = seen.lock().unwrap().calls.clone();
    assert_eq!(calls.len(), 1, "one push");
    let body: serde_json::Value = serde_json::from_str(&calls[0].2).unwrap();

    // Everything the woken turn needs to route a reply and resume the task.
    assert_eq!(body["workflow_id"], wf_id);
    assert_eq!(body["session_id"], "s1");
    assert_eq!(body["seq"], 77);
    assert_eq!(body["origin"]["chat_id"], "stream:eng");
    assert_eq!(body["origin"]["thread_id"], "deploy-bug");
    assert_eq!(body["origin"]["user_name"], "Tamas");
    // …and still no CC prose on the trigger path.
    assert!(!calls[0].2.contains("SECRET PROSE"));
}

#[tokio::test]
async fn a_subscription_without_origin_pushes_empty_routing_fields() {
    std::env::set_var("POK_TEST_WEBHOOK_SECRET", "hmac-test-secret");
    let (addr, state) = start_server().await;
    let (mut sink, _stream) = connect_fake_pok(addr).await;
    let (recv_addr, seen) = start_stub_receiver(vec![StubReply::Accept]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // CLI-origin subscription: no origin at all (backward compatible).
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1", "subscriber": "no-origin", "cursor": 0,
            "deliver": {"url": format!("http://{recv_addr}/webhooks/pok"),
                        "secret_env": "POK_TEST_WEBHOOK_SECRET"}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sub["origin"], serde_json::json!({}));
    assert!(
        sub["workflow"]["workflow_id"].as_str().is_some(),
        "still gets a workflow"
    );

    sink.send(Message::Text(
        serde_json::to_string(&WsMsg::SessionEvent {
            sid: "s1".into(),
            event: pok_proto::EventEnvelope {
                kind: "stop".into(),
                payload: serde_json::Value::Null,
                seq: 4,
                ts: "t".into(),
            },
        })
        .unwrap(),
    ))
    .await
    .unwrap();

    let http = reqwest::Client::new();
    for _ in 0..40 {
        let (_a, d) = xpo_k::deliver::run_pass(&state, &http).await;
        if d > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let calls = seen.lock().unwrap().calls.clone();
    let body: serde_json::Value = serde_json::from_str(&calls[0].2).unwrap();
    // Present-but-empty: a Hermes route templating {origin.chat_id} renders ""
    // and falls back to its configured home channel, instead of sending to a
    // channel literally named "{origin.chat_id}".
    assert_eq!(body["origin"]["chat_id"], "");
    assert_eq!(body["origin"]["thread_id"], "");
    assert!(body["origin"].as_object().unwrap().contains_key("chat_id"));
}

#[tokio::test]
async fn oversized_or_malformed_origin_is_rejected_at_subscribe_time() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    for bad in [
        serde_json::json!("a string"),
        serde_json::json!([1, 2, 3]),
        serde_json::json!({"chat_id": {"nested": true}}),
        serde_json::json!({"hint": "x".repeat(400)}),
    ] {
        let r = client
            .post(format!("{base}/subscriptions"))
            .bearer_auth("secret")
            .json(&serde_json::json!({
                "session_id": "s1", "subscriber": "bad-origin", "cursor": 0, "origin": bad
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "origin {bad} must be rejected");
    }
}

#[tokio::test]
async fn workflow_lookup_by_origin_thread_resolves_the_cc_session() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    for (sid, topic) in [("sess-a", "deploy-bug"), ("sess-b", "other-topic")] {
        let mut origin = ORIGIN();
        origin["thread_id"] = serde_json::json!(topic);
        client
            .post(format!("{base}/subscriptions"))
            .bearer_auth("secret")
            .json(&serde_json::json!({
                "session_id": sid, "subscriber": "lookup", "cursor": 0, "origin": origin
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // This is the query a Zulip turn runs when the user replies in a topic.
    let found: serde_json::Value = client
        .get(format!(
            "{base}/workflows?origin_chat_id=stream:eng&origin_thread_id=deploy-bug"
        ))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(found["count"], 1, "{found}");
    assert_eq!(found["workflows"][0]["session_id"], "sess-a");

    // Whole stream → both; unknown topic → none.
    let all: serde_json::Value = client
        .get(format!("{base}/workflows?origin_chat_id=stream:eng"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(all["count"], 2);
    let none: serde_json::Value = client
        .get(format!("{base}/workflows?origin_thread_id=nope"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(none["count"], 0);
}

#[tokio::test]
async fn workflow_claim_release_serialises_autonomous_turns_over_http() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1", "subscriber": "lease", "cursor": 0,
            "origin": ORIGIN(), "max_turns": 2
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let wf = sub["workflow"]["workflow_id"].as_str().unwrap().to_string();

    // Turn A claims.
    let a = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-1", "lease_secs": 120}))
        .send()
        .await
        .unwrap();
    assert_eq!(a.status(), 200);
    let a_body: serde_json::Value = a.json().await.unwrap();
    assert_eq!(a_body["claimed"], true);
    assert_eq!(a_body["workflow"]["turns_remaining"], 2);

    // A duplicate/concurrent turn is refused with actionable guidance.
    let b = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-2"}))
        .send()
        .await
        .unwrap();
    assert_eq!(b.status(), 409);
    let b_body: serde_json::Value = b.json().await.unwrap();
    assert_eq!(b_body["claimed"], false);
    assert_eq!(b_body["reason"], "busy");
    assert_eq!(b_body["detail"]["lease_owner"], "ntf-1");
    assert!(b_body["guidance"]
        .as_str()
        .unwrap()
        .contains("do NOT send pok_prompt"));

    // Wrong owner cannot release.
    let bad = client
        .post(format!("{base}/workflows/{wf}/release"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-2", "outcome": "done"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 409);

    // Turn A continues (a prompt was accepted) → budget consumed.
    let rel: serde_json::Value = client
        .post(format!("{base}/workflows/{wf}/release"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "owner": "ntf-1", "outcome": "continued", "note": "sent follow-up prompt"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rel["workflow"]["turns"], 1);
    assert_eq!(rel["workflow"]["turns_remaining"], 1);
    assert_eq!(rel["workflow"]["state"], "active");

    // Second (final) turn exhausts the budget; the third is refused.
    client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-2"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{base}/workflows/{wf}/release"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-2", "outcome": "continued"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let denied = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-3"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 409);
    let d_body: serde_json::Value = denied.json().await.unwrap();
    assert!(
        d_body["reason"] == "exhausted" || d_body["detail"]["state"] == "exhausted",
        "{d_body}"
    );
    assert_eq!(d_body["workflow"]["terminal"], true);
}

#[tokio::test]
async fn question_flow_waits_for_human_then_resumes_on_reply() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "session_id": "s1", "subscriber": "qa", "cursor": 0, "origin": ORIGIN()
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let wf = sub["workflow"]["workflow_id"].as_str().unwrap().to_string();

    // The woken turn asks a question in the origin topic and stops.
    client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-1"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let waiting: serde_json::Value = client
        .post(format!("{base}/workflows/{wf}/release"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "owner": "ntf-1", "outcome": "waiting_for_human",
            "note": "asked: deploy to prod or staging?"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(waiting["workflow"]["state"], "waiting_for_human");
    assert_eq!(waiting["workflow"]["turns"], 0, "a question is not a turn");
    assert!(waiting["workflow"]["last_note"]
        .as_str()
        .unwrap()
        .contains("prod or staging"));

    // No autonomous turn may proceed while a human owes an answer.
    let blocked = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-2"}))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(
        blocked.json::<serde_json::Value>().await.unwrap()["detail"]["state"],
        "waiting_for_human"
    );

    // The user replies in Zulip: that session looks the workflow up by topic…
    let found: serde_json::Value = client
        .get(format!(
            "{base}/workflows?origin_chat_id=stream:eng&origin_thread_id=deploy-bug&state=waiting_for_human"
        ))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(found["count"], 1);
    assert_eq!(found["workflows"][0]["workflow_id"], wf);
    assert_eq!(found["workflows"][0]["session_id"], "s1");

    // …resumes it, and autonomy is available again.
    let resumed: serde_json::Value = client
        .post(format!("{base}/workflows/{wf}/resume"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"note": "user said staging"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resumed["resumed"], true);
    assert_eq!(resumed["workflow"]["state"], "active");
    let ok = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-3"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    // Resume is a no-op on a finished workflow (cannot revive it).
    client
        .post(format!("{base}/workflows/{wf}/release"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "ntf-3", "outcome": "done"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let stray: serde_json::Value = client
        .post(format!("{base}/workflows/{wf}/resume"))
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stray["resumed"], false);
    assert_eq!(stray["workflow"]["state"], "done");
}

#[tokio::test]
async fn workflow_endpoints_require_auth_and_404_on_unknown_ids() {
    let (addr, _state) = start_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    assert_eq!(
        client
            .get(format!("{base}/workflows"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/workflows/wf-x/claim"))
            .json(&serde_json::json!({"owner": "o"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    for (path, body) in [("wf-nope", serde_json::json!({"owner": "o"}))] {
        let r = client
            .post(format!("{base}/workflows/{path}/claim"))
            .bearer_auth("secret")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404);
    }
    let r = client
        .get(format!("{base}/workflows/wf-nope"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    // Blank owner is a client error.
    let sub: serde_json::Value = client
        .post(format!("{base}/subscriptions"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"session_id": "s1", "subscriber": "auth", "cursor": 0}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let wf = sub["workflow"]["workflow_id"].as_str().unwrap();
    let r = client
        .post(format!("{base}/workflows/{wf}/claim"))
        .bearer_auth("secret")
        .json(&serde_json::json!({"owner": "   "}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}
