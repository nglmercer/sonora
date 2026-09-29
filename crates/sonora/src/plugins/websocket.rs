use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use control::{
    API_VERSION, ControlError, ControlEvent, WsCommand, WsErrorData, WsResultData, WsServerMessage,
};
use state::{Plugin, PluginContext};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, oneshot, watch};

use super::rest;

/// The largest WebSocket message the API reads. Commands are small JSON objects.
const MAX_MESSAGE: usize = 64 * 1024;
/// How many clients one listener serves at once. Past this, upgrades fail closed.
const MAX_CONNECTIONS: usize = 8;
/// How often a live connection is pinged. A failed send ends the connection, so a dead
/// peer holds its slot for at most one interval past its death. Idle-but-live connections
/// stay: a connected remote outlives a paused player.
const HEARTBEAT: Duration = Duration::from_secs(30);

/// The WebSocket control transport: snapshots, commands and events over authenticated
/// loopback sockets, for native clients.
pub struct WsPlugin;

#[async_trait::async_trait]
impl Plugin for WsPlugin {
    fn id(&self) -> &'static str {
        "ws"
    }

    async fn serve(
        &self,
        ctx: PluginContext,
        listener: tokio::net::TcpListener,
        shutdown: oneshot::Receiver<()>,
    ) {
        let (stop_tx, stop_rx) = watch::channel(false);
        let state = WsState {
            ctx,
            stop: stop_rx,
            slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            heartbeat: HEARTBEAT,
        };
        serve_on(listener, state, shutdown, stop_tx).await;
    }
}

/// Serves `listener` until `shutdown` fires. Tests pass a custom state through here.
async fn serve_on(
    listener: tokio::net::TcpListener,
    state: WsState,
    shutdown: oneshot::Receiver<()>,
    stop: watch::Sender<bool>,
) {
    let app = Router::new()
        .route("/v1/ws", get(upgrade))
        .fallback(rest::unknown)
        .layer(middleware::from_fn_with_state(state.clone(), require_auth))
        .with_state(state);
    if let Err(error) = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown.await.ok();
            stop.send(true).ok();
        })
        .await
    {
        log::warn!("websocket: serve ended: {error}");
    }
}

#[derive(Clone)]
struct WsState {
    ctx: PluginContext,
    stop: watch::Receiver<bool>,
    slots: Arc<Semaphore>,
    heartbeat: Duration,
}

async fn require_auth(State(st): State<WsState>, request: Request, next: Next) -> Response {
    match rest::authorized(&st.ctx, request.headers()) {
        true => next.run(request).await,
        false => rest::unauthorized(),
    }
}

async fn upgrade(State(st): State<WsState>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if headers.contains_key(header::ORIGIN) {
        return rest::failure(ControlError::permission_denied(
            "browser clients are not supported",
        ));
    }
    let permit = match st.slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return rest::failure(ControlError::resource_exhausted("too many connections"));
        }
    };
    let ctx = st.ctx.clone();
    let stop = st.stop.clone();
    let heartbeat = st.heartbeat;
    ws.max_message_size(MAX_MESSAGE)
        .on_upgrade(move |socket| connection(ctx, stop, socket, heartbeat, permit))
}

/// Serves one client: a snapshot first, then commands, events and resyncs until either side
/// hangs up. Sends are direct, so a slow client stalls only itself; broadcast lag resyncs
/// it. A ping every `heartbeat` drops dead peers; `permit` frees its connection slot when
/// this returns.
async fn connection(
    ctx: PluginContext,
    mut stop: watch::Receiver<bool>,
    mut socket: WebSocket,
    heartbeat: Duration,
    _permit: OwnedSemaphorePermit,
) {
    if *stop.borrow_and_update() {
        return;
    }
    let mut events = ctx.client.subscribe();
    let snapshot = match ctx.client.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(_) => return,
    };
    if !send(
        &mut socket,
        &WsServerMessage::Snapshot {
            v: API_VERSION,
            data: Box::new(snapshot),
        },
    )
    .await
    {
        return;
    }
    let mut beat = tokio::time::interval(heartbeat);
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires at once; the loop wants the next one.
    beat.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = beat.tick() => {
                if socket
                    .send(Message::Ping(Vec::<u8>::new().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            message = socket.recv() => {
                let Some(message) = message else { break };
                match message {
                    Ok(Message::Text(text)) => {
                        if !command(&ctx, &mut socket, &text).await {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            event = events.recv() => {
                match event {
                    Ok(sequenced) => {
                        let message = WsServerMessage::Event {
                            v: API_VERSION,
                            seq: Some(sequenced.seq),
                            event: sequenced.event,
                        };
                        if !send(&mut socket, &message).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if !resync(&ctx, &mut socket, &mut events).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    socket.send(Message::Close(None)).await.ok();
}

/// Answers one text message. False ends the connection.
async fn command(ctx: &PluginContext, socket: &mut WebSocket, text: &str) -> bool {
    let command: WsCommand = match serde_json::from_str(text) {
        Ok(command) => command,
        Err(_) => {
            return send(
                socket,
                &WsServerMessage::Error {
                    v: API_VERSION,
                    data: WsErrorData {
                        error: ControlError::invalid_argument("invalid message"),
                    },
                },
            )
            .await;
        }
    };
    if command.v != API_VERSION {
        return result(
            socket,
            &command.id,
            WsResultData::Failed {
                error: ControlError::invalid_argument("unsupported version"),
            },
        )
        .await;
    }
    let data = match ctx.client.execute(command.data).await {
        Ok(accepted) => WsResultData::Accepted {
            accepted: accepted.accepted,
            snapshot_revision: accepted.snapshot_revision,
        },
        Err(error) => WsResultData::Failed {
            error: ControlError::from(error),
        },
    };
    result(socket, &command.id, data).await
}

async fn result(socket: &mut WebSocket, id: &str, data: WsResultData) -> bool {
    send(
        socket,
        &WsServerMessage::Result {
            v: API_VERSION,
            id: id.to_owned(),
            data,
        },
    )
    .await
}

/// Resubscribes a lagging client: a seq-less resync marker, then the fresh snapshot it
/// continues from. The receiver is replaced, so nothing stale replays afterwards.
async fn resync(
    ctx: &PluginContext,
    socket: &mut WebSocket,
    events: &mut broadcast::Receiver<control::channel::SequencedEvent>,
) -> bool {
    let Ok(snapshot) = ctx.client.snapshot().await else {
        return false;
    };
    let revision = snapshot.snapshot_revision;
    let ok = send(
        socket,
        &WsServerMessage::Event {
            v: API_VERSION,
            seq: None,
            event: ControlEvent::ResyncRequired {
                snapshot_revision: revision,
            },
        },
    )
    .await
        && send(
            socket,
            &WsServerMessage::Snapshot {
                v: API_VERSION,
                data: Box::new(snapshot),
            },
        )
        .await;
    *events = ctx.client.subscribe();
    ok
}

async fn send(socket: &mut WebSocket, message: &WsServerMessage) -> bool {
    let Ok(text) = serde_json::to_string(message) else {
        return false;
    };
    socket.send(Message::Text(text.into())).await.is_ok()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use control::channel::{CommandResult, Publisher, Request};
    use control::{
        API_VERSION, AppSnapshot, Command, CommandReply, PlaybackSnapshot, PlaybackStatus,
        QueueSnapshot, RepeatMode, SessionSnapshot, SessionStatus, WsCommandKind,
    };
    use futures::{SinkExt as _, StreamExt as _};
    use state::Auth;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::tungstenite::Message as ClientMessage;

    use super::*;

    fn pictured() -> AppSnapshot {
        AppSnapshot {
            snapshot_revision: 3,
            playback: PlaybackSnapshot {
                status: PlaybackStatus::Playing,
                track: None,
                position_ms: 90_000,
                duration_ms: None,
                volume: 0.6,
                repeat: RepeatMode::All,
                shuffle: true,
            },
            queue: QueueSnapshot {
                revision: 17,
                past: Vec::new(),
                current: None,
                upcoming: Vec::new(),
                suggested: Vec::new(),
                manual_count: 0,
                shuffle: true,
            },
            session: SessionSnapshot {
                status: SessionStatus::SignedIn,
                provider: Some("spotify".to_owned()),
            },
        }
    }

    fn accept() -> CommandResult {
        Ok(CommandReply {
            accepted: true,
            snapshot_revision: 9,
        })
    }

    /// A stub host answering snapshots with `pictured` and every command with `verdict`.
    /// Commands are recorded, and `publish` emits events on demand.
    struct Stub {
        ctx: PluginContext,
        publish: Publisher,
        seen: Arc<Mutex<Vec<Command>>>,
    }

    fn stub(verdict: CommandResult) -> Stub {
        let (host, client) = control::channel::pair(pictured());
        let (mut requests, publish) = host.split();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        tokio::spawn(async move {
            while let Some(request) = requests.recv().await {
                match request {
                    Request::GetSnapshot { reply, .. } => {
                        let _ = reply.send(pictured());
                    }
                    Request::Execute { command, reply, .. } => {
                        recorded.lock().unwrap().push(command);
                        let _ = reply.send(verdict.clone());
                    }
                }
            }
        });
        let ctx = PluginContext {
            client,
            auth: Auth::new(),
        };
        ctx.auth.ensure();
        Stub { ctx, publish, seen }
    }

    async fn serve(ctx: PluginContext) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
        serve_with(ctx, MAX_CONNECTIONS, HEARTBEAT).await
    }

    async fn serve_with(
        ctx: PluginContext,
        slots: usize,
        heartbeat: Duration,
    ) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback listener");
        let addr = listener.local_addr().expect("a bound address");
        assert!(addr.ip().is_loopback());
        let (stop_tx, stop_rx) = watch::channel(false);
        let state = WsState {
            ctx,
            stop: stop_rx,
            slots: Arc::new(Semaphore::new(slots)),
            heartbeat,
        };
        let (shutdown, rx) = oneshot::channel();
        let task = tokio::spawn(async move { serve_on(listener, state, rx, stop_tx).await });
        (addr, shutdown, task)
    }

    type Client = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn connect(addr: SocketAddr, headers: &[(&str, &str)]) -> Client {
        let (stream, _) = tokio_tungstenite::connect_async(upgrade_request(addr, headers))
            .await
            .expect("an upgrade");
        stream
    }

    fn upgrade_request(addr: SocketAddr, headers: &[(&str, &str)]) -> axum::http::Request<()> {
        let mut request = axum::http::Request::builder()
            .uri(format!("ws://{addr}/v1/ws"))
            .header("host", addr.to_string())
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).expect("a request")
    }

    /// Reads the next text message as JSON.
    async fn next_json(stream: &mut Client) -> serde_json::Value {
        match stream.next().await {
            Some(Ok(ClientMessage::Text(text))) => serde_json::from_str(&text).expect("json"),
            other => panic!("expected text, saw {other:?}"),
        }
    }

    fn command(id: &str, command: Command) -> String {
        serde_json::to_string(&WsCommand {
            v: API_VERSION,
            id: id.to_owned(),
            kind: WsCommandKind::Command,
            data: command,
        })
        .expect("a command")
    }

    #[tokio::test]
    async fn upgrade_requires_auth_and_native_clients() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;

        let error = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/ws"))
            .await
            .expect_err("no upgrade without credentials");
        assert!(matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status() == axum::http::StatusCode::UNAUTHORIZED
        ));

        let request = upgrade_request(addr, &[("authorization", "Bearer wrong")]);
        let error = tokio_tungstenite::connect_async(request)
            .await
            .expect_err("no upgrade on a wrong credential");
        assert!(matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status() == axum::http::StatusCode::UNAUTHORIZED
        ));

        let bearer = format!("Bearer {token}");
        let request = upgrade_request(
            addr,
            &[("authorization", &bearer), ("origin", "http://example.com")],
        );
        let error = tokio_tungstenite::connect_async(request)
            .await
            .expect_err("no upgrade for browser origins");
        assert!(matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status() == axum::http::StatusCode::FORBIDDEN
        ));
    }

    fn stub_token(stub: &Stub) -> String {
        stub.ctx.auth.token().expect("a minted credential")
    }

    #[tokio::test]
    async fn upgrades_close_past_the_cap() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve_with(stub.ctx, 2, Duration::from_secs(30)).await;
        let auth = format!("Bearer {token}");

        let mut first = connect(addr, &[("authorization", auth.as_str())]).await;
        assert_eq!(next_json(&mut first).await["type"], "snapshot");
        let mut second = connect(addr, &[("authorization", auth.as_str())]).await;
        assert_eq!(next_json(&mut second).await["type"], "snapshot");

        let request = upgrade_request(addr, &[("authorization", auth.as_str())]);
        let error = tokio_tungstenite::connect_async(request)
            .await
            .expect_err("no upgrade past the cap");
        assert!(matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status() == axum::http::StatusCode::TOO_MANY_REQUESTS
        ));

        // A disconnect frees its slot.
        first.close(None).await.ok();
        drop(first);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let request = upgrade_request(addr, &[("authorization", auth.as_str())]);
            match tokio_tungstenite::connect_async(request).await {
                Ok((mut stream, _)) => {
                    assert_eq!(next_json(&mut stream).await["type"], "snapshot");
                    break;
                }
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("no upgrade after a disconnect: {error}"),
            }
        }
    }

    #[tokio::test]
    async fn idle_connections_are_pinged() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve_with(stub.ctx, 8, Duration::from_millis(50)).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;

        let mut snapshot = false;
        let mut pinged = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !snapshot || !pinged {
            let message = tokio::time::timeout_at(deadline, stream.next())
                .await
                .expect("a frame arrives")
                .expect("the connection stays up")
                .expect("a clean frame");
            match message {
                ClientMessage::Text(text) => {
                    let json: serde_json::Value = serde_json::from_str(&text).expect("json");
                    assert_eq!(json["type"], "snapshot");
                    snapshot = true;
                }
                ClientMessage::Ping(_) => pinged = true,
                other => panic!("expected a snapshot or a ping, saw {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn snapshot_first_then_results() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;

        let snapshot = next_json(&mut stream).await;
        assert_eq!(snapshot["type"], "snapshot");
        assert_eq!(snapshot["v"], 1);
        assert_eq!(snapshot["data"]["snapshot_revision"], 3);

        stream
            .send(ClientMessage::text(command("a", Command::Toggle)))
            .await
            .expect("a command goes out");
        let result = next_json(&mut stream).await;
        assert_eq!(result["type"], "result");
        assert_eq!(result["id"], "a");
        assert_eq!(result["data"]["accepted"], true);
        assert_eq!(result["data"]["snapshot_revision"], 9);
        assert_eq!(*stub.seen.lock().unwrap(), [Command::Toggle]);
    }

    #[tokio::test]
    async fn command_errors_mirror_rest() {
        let refused = Err(ControlError::conflict(
            "queue changed; request a new snapshot",
        ));
        let stub = stub(refused);
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        stream
            .send(ClientMessage::text(command("b", Command::Next)))
            .await
            .expect("a command goes out");
        let result = next_json(&mut stream).await;
        assert_eq!(result["id"], "b");
        assert_eq!(result["data"]["error"]["code"], "conflict");
        assert_eq!(
            result["data"]["error"]["message"],
            "queue changed; request a new snapshot"
        );
    }

    #[tokio::test]
    async fn invalid_messages_get_errors_not_disconnects() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        for message in [
            "not json".to_owned(),
            r#"{"v":1,"id":"x","type":"subscribe","data":{}}"#.to_owned(),
        ] {
            stream
                .send(ClientMessage::text(message))
                .await
                .expect("a message goes out");
            let error = next_json(&mut stream).await;
            assert_eq!(error["type"], "error");
            assert_eq!(error["data"]["error"]["code"], "invalid_argument");
        }

        stream
            .send(ClientMessage::text(
                r#"{"v":99,"id":"old","type":"command","data":{"command":"toggle"}}"#,
            ))
            .await
            .expect("a message goes out");
        let result = next_json(&mut stream).await;
        assert_eq!(result["type"], "result");
        assert_eq!(result["id"], "old");
        assert_eq!(result["data"]["error"]["code"], "invalid_argument");
        assert!(stub.seen.lock().unwrap().is_empty());

        stream
            .send(ClientMessage::text(command("c", Command::Toggle)))
            .await
            .expect("a command goes out");
        let result = next_json(&mut stream).await;
        assert_eq!(result["id"], "c");
        assert_eq!(result["data"]["accepted"], true);
    }

    #[tokio::test]
    async fn oversized_messages_close_the_connection() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        stream
            .send(ClientMessage::text(" ".repeat(70 * 1024)))
            .await
            .expect("a message goes out");
        match stream.next().await {
            Some(Ok(ClientMessage::Close(_))) | Some(Err(_)) | None => {}
            other => panic!("expected a close, saw {other:?}"),
        }
    }

    #[tokio::test]
    async fn events_stream_and_lag_resyncs() {
        use control::channel::SequencedEvent;

        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        for seq in 1..=3 {
            stub.publish.emit(SequencedEvent {
                seq,
                event: ControlEvent::PlaybackChanged {
                    snapshot_revision: 3,
                    status: PlaybackStatus::Playing,
                },
            });
        }
        for seq in 1..=3 {
            let event = next_json(&mut stream).await;
            assert_eq!(event["type"], "event");
            assert_eq!(event["seq"], seq);
            assert_eq!(event["event"], "playback.changed");
        }

        for seq in 4..100_004 {
            stub.publish.emit(SequencedEvent {
                seq,
                event: ControlEvent::PositionChanged {
                    snapshot_revision: 3,
                    position_ms: seq,
                },
            });
        }
        let mut resynced = false;
        for _ in 0..1000 {
            let message = next_json(&mut stream).await;
            if message["event"] == "resync.required" {
                assert_eq!(message["type"], "event");
                assert!(message.get("seq").is_none());
                resynced = true;
                break;
            }
        }
        assert!(resynced);
        let snapshot = next_json(&mut stream).await;
        assert_eq!(snapshot["type"], "snapshot");
        assert_eq!(snapshot["data"]["snapshot_revision"], 3);
    }

    #[tokio::test]
    async fn binary_frames_are_ignored() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, _shutdown, _serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        stream
            .send(ClientMessage::binary(vec![0, 1, 2]))
            .await
            .expect("a frame goes out");
        tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .expect_err("no answer to binary");

        stream
            .send(ClientMessage::text(command("d", Command::Toggle)))
            .await
            .expect("a command goes out");
        let result = next_json(&mut stream).await;
        assert_eq!(result["id"], "d");
    }

    #[tokio::test]
    async fn shutdown_closes_connections() {
        let stub = stub(accept());
        let token = stub_token(&stub);
        let (addr, shutdown, serve) = serve(stub.ctx).await;
        let mut stream = connect(addr, &[("authorization", &format!("Bearer {token}"))]).await;
        next_json(&mut stream).await;

        shutdown.send(()).expect("shutdown lands");
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve ends")
            .expect("serve joins");
        match stream.next().await {
            Some(Ok(ClientMessage::Close(_))) | Some(Err(_)) | None => {}
            other => panic!("expected a close, saw {other:?}"),
        }
    }
}
