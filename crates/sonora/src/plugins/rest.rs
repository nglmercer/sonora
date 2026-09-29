use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use control::{
    Command, ControlError, ErrorCode, ErrorReply, PlaybackReply, QueueReply, SnapshotReply,
    SuccessReply,
};
use serde::Serialize;
use state::{Plugin, PluginContext};
use tokio::sync::oneshot;
use tower_http::limit::RequestBodyLimitLayer;

/// The largest command body the API reads. Commands are small JSON objects.
const MAX_BODY: usize = 64 * 1024;

/// The REST control transport: versioned JSON over authenticated loopback HTTP.
pub struct RestPlugin;

#[async_trait::async_trait]
impl Plugin for RestPlugin {
    fn id(&self) -> &'static str {
        "rest"
    }

    async fn serve(
        &self,
        ctx: PluginContext,
        listener: tokio::net::TcpListener,
        shutdown: oneshot::Receiver<()>,
    ) {
        if let Err(error) = axum::serve(listener, router(ctx).into_make_service())
            .with_graceful_shutdown(async move {
                shutdown.await.ok();
            })
            .await
        {
            log::warn!("rest: serve ended: {error}");
        }
    }
}

fn router(ctx: PluginContext) -> Router {
    Router::new()
        .route("/v1/state", get(state))
        .route("/v1/playback", get(playback))
        .route("/v1/queue", get(queue))
        .route("/v1/commands", post(commands))
        .fallback(unknown)
        .layer(RequestBodyLimitLayer::new(MAX_BODY))
        .layer(middleware::from_fn_with_state(ctx.clone(), require_auth))
        .with_state(ctx)
}

async fn require_auth(State(ctx): State<PluginContext>, request: Request, next: Next) -> Response {
    match authorized(&ctx, request.headers()) {
        true => next.run(request).await,
        false => unauthorized(),
    }
}

/// Whether the request headers carry the session credential.
pub(crate) fn authorized(ctx: &PluginContext, headers: &HeaderMap) -> bool {
    let header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    ctx.auth.check(header)
}

/// The 401 answer for missing or wrong credentials.
pub(crate) fn unauthorized() -> Response {
    let mut denied = reply(
        StatusCode::UNAUTHORIZED,
        ErrorReply::new(ControlError::permission_denied(
            "missing or invalid bearer token",
        )),
    );
    denied
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    denied
}

async fn state(State(ctx): State<PluginContext>) -> Response {
    match ctx.client.snapshot().await {
        Ok(snapshot) => reply(StatusCode::OK, SnapshotReply::new(snapshot)),
        Err(error) => failure(error.into()),
    }
}

async fn playback(State(ctx): State<PluginContext>) -> Response {
    match ctx.client.snapshot().await {
        Ok(snapshot) => reply(
            StatusCode::OK,
            PlaybackReply::new(snapshot.snapshot_revision, snapshot.playback),
        ),
        Err(error) => failure(error.into()),
    }
}

async fn queue(State(ctx): State<PluginContext>) -> Response {
    match ctx.client.snapshot().await {
        Ok(snapshot) => reply(
            StatusCode::OK,
            QueueReply::new(snapshot.snapshot_revision, snapshot.queue),
        ),
        Err(error) => failure(error.into()),
    }
}

async fn commands(
    State(ctx): State<PluginContext>,
    command: Result<Json<Command>, JsonRejection>,
) -> Response {
    let command = match command {
        Ok(Json(command)) => command,
        Err(rejection) => return rejected(rejection),
    };
    match ctx.client.execute(command).await {
        Ok(accepted) => reply(StatusCode::OK, SuccessReply::ok(accepted.snapshot_revision)),
        Err(error) => failure(error.into()),
    }
}

pub(crate) async fn unknown() -> Response {
    failure(ControlError::not_found("no such endpoint"))
}

/// Maps a JSON rejection to the versioned error envelope. Unknown commands fail
/// deserialization, so they land here as invalid arguments.
fn rejected(rejection: JsonRejection) -> Response {
    match rejection {
        JsonRejection::MissingJsonContentType(_) => reply(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorReply::new(ControlError::invalid_argument("expected application/json")),
        ),
        _ => reply(
            StatusCode::BAD_REQUEST,
            ErrorReply::new(ControlError::invalid_argument("invalid command body")),
        ),
    }
}

/// Maps a control failure to its status code and the versioned error envelope.
pub(crate) fn failure(error: ControlError) -> Response {
    let status = match error.code {
        ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::Conflict => StatusCode::CONFLICT,
        ErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    reply(status, ErrorReply::new(error))
}

/// A JSON response nothing is allowed to cache.
fn reply(status: StatusCode, body: impl Serialize) -> Response {
    let mut response = (status, Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use control::channel::{CommandResult, Request};
    use control::{
        AppSnapshot, CommandReply, PlaybackSnapshot, PlaybackStatus, QueueSnapshot, RepeatMode,
        SessionSnapshot, SessionStatus,
    };
    use state::Auth;
    use tokio::task::JoinHandle;

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

    /// Spawns a stub host answering snapshots with `pictured`, answering every command with
    /// `verdict`, and recording every command it sees.
    fn stub(verdict: CommandResult) -> (PluginContext, Arc<Mutex<Vec<Command>>>) {
        let (mut host, client) = control::channel::pair(pictured());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        tokio::spawn(async move {
            while let Some(request) = host.recv().await {
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
        (ctx, seen)
    }

    async fn serve(ctx: PluginContext) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback listener");
        let addr = listener.local_addr().expect("a bound address");
        assert!(addr.ip().is_loopback());
        let (shutdown, rx) = oneshot::channel();
        let task = tokio::spawn(async move { RestPlugin.serve(ctx, listener, rx).await });
        (addr, shutdown, task)
    }

    fn send_command(
        client: &reqwest::Client,
        addr: SocketAddr,
        token: &str,
        body: &str,
    ) -> reqwest::RequestBuilder {
        client
            .post(format!("http://{addr}/v1/commands"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(body.to_owned())
    }

    #[tokio::test]
    async fn rejects_unauthenticated_requests() {
        let (ctx, _) = stub(accept());
        let (addr, _shutdown, _serve) = serve(ctx).await;
        let client = reqwest::Client::new();

        let denied = client
            .get(format!("http://{addr}/v1/state"))
            .send()
            .await
            .expect("a response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            denied
                .headers()
                .get("www-authenticate")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer"
        );
        let body: ErrorReply = denied.json().await.expect("an error envelope");
        assert_eq!(body.v, 1);
        assert_eq!(body.error.code, ErrorCode::PermissionDenied);

        let denied = client
            .get(format!("http://{addr}/v1/state"))
            .header("authorization", "Bearer wrong")
            .send()
            .await
            .expect("a response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let denied = client
            .get(format!("http://{addr}/nope"))
            .send()
            .await
            .expect("a response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn serves_snapshots() {
        let (ctx, _) = stub(accept());
        let token = ctx.auth.token().expect("a minted token");
        let (addr, _shutdown, _serve) = serve(ctx).await;
        let client = reqwest::Client::new();
        let get = |path: &str| {
            client
                .get(format!("http://{addr}{path}"))
                .header("authorization", format!("Bearer {token}"))
        };

        let state = get("/v1/state").send().await.expect("a response");
        assert_eq!(state.status(), StatusCode::OK);
        assert_eq!(
            state
                .headers()
                .get("cache-control")
                .unwrap()
                .to_str()
                .unwrap(),
            "no-store"
        );
        let state: SnapshotReply = state.json().await.expect("a snapshot");
        assert_eq!(state.v, 1);
        assert_eq!(state.snapshot.snapshot_revision, 3);
        assert_eq!(state.snapshot.playback.status, PlaybackStatus::Playing);
        assert_eq!(state.snapshot.queue.revision, 17);

        let playback = get("/v1/playback").send().await.expect("a response");
        assert_eq!(playback.status(), StatusCode::OK);
        let playback: PlaybackReply = playback.json().await.expect("a section");
        assert_eq!(playback.snapshot_revision, 3);
        assert_eq!(playback.playback.volume, 0.6);

        let queue = get("/v1/queue").send().await.expect("a response");
        assert_eq!(queue.status(), StatusCode::OK);
        let queue: QueueReply = queue.json().await.expect("a section");
        assert_eq!(queue.snapshot_revision, 3);
        assert_eq!(queue.queue.revision, 17);
    }

    #[tokio::test]
    async fn forwards_commands_once() {
        let (ctx, seen) = stub(accept());
        let token = ctx.auth.token().expect("a minted token");
        let (addr, _shutdown, _serve) = serve(ctx).await;
        let client = reqwest::Client::new();

        let done = send_command(&client, addr, &token, r#"{"command":"toggle"}"#)
            .send()
            .await
            .expect("a response");
        assert_eq!(done.status(), StatusCode::OK);
        let done: SuccessReply = done.json().await.expect("a reply");
        assert!(done.accepted);
        assert_eq!(done.snapshot_revision, 9);

        let done = send_command(
            &client,
            addr,
            &token,
            r#"{"command":"queue_remove_upcoming","index":2,"expected_revision":17}"#,
        )
        .send()
        .await
        .expect("a response");
        assert_eq!(done.status(), StatusCode::OK);

        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            [
                Command::Toggle,
                Command::QueueRemoveUpcoming {
                    index: 2,
                    expected_revision: 17,
                },
            ]
        );
    }

    #[tokio::test]
    async fn maps_errors_to_status() {
        let refused = Err(ControlError::conflict(
            "queue changed; request a new snapshot",
        ));
        let (ctx, _) = stub(refused);
        let token = ctx.auth.token().expect("a minted token");
        let (addr, _shutdown, _serve) = serve(ctx).await;
        let client = reqwest::Client::new();

        let conflicted = send_command(&client, addr, &token, r#"{"command":"next"}"#)
            .send()
            .await
            .expect("a response");
        assert_eq!(conflicted.status(), StatusCode::CONFLICT);
        let body: ErrorReply = conflicted.json().await.expect("an error envelope");
        assert_eq!(body.error.code, ErrorCode::Conflict);

        for body in [r#"{"command":"#, r#"{"command":"launch"}"#] {
            let invalid = send_command(&client, addr, &token, body)
                .send()
                .await
                .expect("a response");
            assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
            let body: ErrorReply = invalid.json().await.expect("an error envelope");
            assert_eq!(body.error.code, ErrorCode::InvalidArgument);
        }

        let typed = client
            .post(format!("http://{addr}/v1/commands"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "text/plain")
            .body(r#"{"command":"toggle"}"#)
            .send()
            .await
            .expect("a response");
        assert_eq!(typed.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let missing = client
            .get(format!("http://{addr}/v1/nothing"))
            .header("authorization", format!("Bearer {token}"))
            .send()
            .await
            .expect("a response");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let body: ErrorReply = missing.json().await.expect("an error envelope");
        assert_eq!(body.error.code, ErrorCode::NotFound);

        let huge = " ".repeat(70 * 1024);
        let limited = send_command(&client, addr, &token, &huge)
            .send()
            .await
            .expect("a response");
        assert_eq!(limited.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn failures_map_to_their_status() {
        for (error, status) in [
            (
                ControlError::invalid_argument("bad"),
                StatusCode::BAD_REQUEST,
            ),
            (ControlError::permission_denied("no"), StatusCode::FORBIDDEN),
            (ControlError::not_found("gone"), StatusCode::NOT_FOUND),
            (ControlError::conflict("moved"), StatusCode::CONFLICT),
            (
                ControlError::resource_exhausted("slow down"),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                ControlError::unavailable("later"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                ControlError::internal("oops"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            assert_eq!(failure(error).status(), status);
        }
    }

    #[tokio::test]
    async fn stops_gracefully() {
        let (ctx, _) = stub(accept());
        let token = ctx.auth.token().expect("a minted token");
        let (addr, shutdown, serve) = serve(ctx).await;
        let client = reqwest::Client::new();

        let state = client
            .get(format!("http://{addr}/v1/state"))
            .header("authorization", format!("Bearer {token}"))
            .send()
            .await
            .expect("a response");
        assert_eq!(state.status(), StatusCode::OK);

        shutdown.send(()).expect("shutdown lands");
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve ends")
            .expect("serve joins");

        assert!(
            client
                .get(format!("http://{addr}/v1/state"))
                .header("authorization", format!("Bearer {token}"))
                .send()
                .await
                .is_err()
        );
    }
}
