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

/// Connect a fake po-k and register it owning project "demo" + session "s1".
async fn connect_fake_pok(
    addr: SocketAddr,
) -> (
    futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
) {
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let (mut sink, mut stream) = ws.split();
    let reg = WsMsg::Register {
        pok_id: "pok-1".into(),
        hostname: "host".into(),
        version: "0".into(),
        projects: vec![ProjectDecl {
            name: "demo".into(),
            cwd: "/demo".into(),
        }],
        sessions: vec![SessionDecl {
            sid: "s1".into(),
            project: "demo".into(),
            status: "idle".into(),
        }],
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
            request_id, stream: true, ..
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
                WsMsg::WsRequest { request_id, path, .. } if path == "/sessions" => {
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
                WsMsg::ProfileUpdate { session_id, changed_fields, .. } => {
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
