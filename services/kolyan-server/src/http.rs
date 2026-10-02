//! Loopback complete-result transport. Detached attempts survive client loss.
mod error;
mod view;
use error::Problem;

use crate::{assembly::App, config::HttpConfig};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State, rejection::JsonRejection},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kolyan_core::TurnRequest;
use kolyan_model::{ContentBlock, Message, MessageRole};
use kolyan_server::{ExecutionRef, ServerError};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Host {
    app: App,
    token: Arc<String>,
    authority: Arc<String>,
    active: Arc<Mutex<HashSet<String>>>,
    capacity: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    session_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    turn_id: String,
    input: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    decision: Choice,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Choice {
    Approve,
    Deny,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

type ResultView = Result<Json<Value>, Problem>;

pub async fn serve(app: App, config: HttpConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !config.listen.ip().is_loopback() || config.max_active_turns == 0 {
        return Err("HTTP requires loopback and a positive active Turn limit".into());
    }
    let count = u32::try_from(config.max_active_turns)?;
    let token = std::env::var(&config.api_token_env).map_err(|_| "missing HTTP API token")?;
    if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err("invalid HTTP API token".into());
    }
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let address = listener.local_addr()?;
    let host = Host {
        app,
        token: Arc::new(token),
        authority: Arc::new(address.to_string()),
        active: Arc::new(Mutex::new(HashSet::new())),
        capacity: Arc::new(Semaphore::new(config.max_active_turns)),
    };
    let router = Router::new()
        .route("/v1/sessions", post(create))
        .route("/v1/sessions/{session_id}", get(session))
        .route("/v1/sessions/{session_id}/turns", post(start))
        .route("/v1/sessions/{session_id}/turns/{turn_id}", get(status))
        .route(
            "/v1/sessions/{session_id}/turns/{turn_id}/approvals/{approval_id}/decision",
            post(decide),
        )
        .route(
            "/v1/sessions/{session_id}/turns/{turn_id}/cancel",
            post(cancel),
        )
        .fallback(|| async { Problem(StatusCode::NOT_FOUND, "not_found") })
        .method_not_allowed_fallback(|| async {
            Problem(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed")
        })
        .layer(DefaultBodyLimit::max(65536))
        .layer(middleware::from_fn_with_state(host.clone(), authorize))
        .with_state(host.clone());
    eprintln!("HTTP listening on {address}");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    // Detached drivers are not owned by connections. Drain them before exit.
    let _drained = host.capacity.acquire_many(count).await?;
    Ok(())
}

async fn authorize(State(host): State<Host>, request: Request, next: Next) -> Response {
    if request.headers().contains_key(header::ORIGIN) {
        return Problem(StatusCode::FORBIDDEN, "origin_forbidden").into_response();
    }
    if request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        != Some(host.authority.as_str())
    {
        return Problem(StatusCode::MISDIRECTED_REQUEST, "misdirected_request").into_response();
    }
    let expected = format!("Bearer {}", host.token);
    if request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(expected.as_str())
    {
        return Problem(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(request).await
}
fn id(value: &str) -> Result<(), Problem> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Problem(StatusCode::BAD_REQUEST, "invalid_request"));
    }
    Ok(())
}
fn body<T>(result: Result<Json<T>, JsonRejection>) -> Result<T, Problem> {
    result
        .map(|Json(value)| value)
        .map_err(|error| match error.status() {
            StatusCode::PAYLOAD_TOO_LARGE => {
                Problem(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large")
            }
            StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                Problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
            }
            _ => Problem(StatusCode::BAD_REQUEST, "invalid_request"),
        })
}
fn key(host: &Host, session: &str, turn: &str) -> Result<ExecutionRef, Problem> {
    id(session)?;
    id(turn)?;
    Ok(view::owned(&host.app, session, turn)?)
}
fn snapshot(host: &Host, key: &ExecutionRef) -> Result<Value, Problem> {
    let active = host
        .active
        .lock()
        .expect("HTTP task set")
        .contains(&key.session_id);
    Ok(view::turn(&host.app, key, active)?)
}
async fn create(
    State(host): State<Host>,
    request: Result<Json<Create>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), Problem> {
    let request = body(request)?;
    id(&request.session_id)?;
    let record = blocking(move || {
        Ok(host
            .app
            .service
            .sessions()
            .create(&request.session_id)
            .map_err(ServerError::from)?)
    })
    .await?;
    Ok((StatusCode::CREATED, Json(view::session(record))))
}
async fn session(State(host): State<Host>, Path(session): Path<String>) -> ResultView {
    id(&session)?;
    blocking(move || {
        Ok(Json(view::session(
            host.app.service.load_reconciled(&session)?,
        )))
    })
    .await
}
async fn status(
    State(host): State<Host>,
    Path((session, turn)): Path<(String, String)>,
) -> ResultView {
    blocking(move || {
        let key = key(&host, &session, &turn)?;
        Ok(Json(snapshot(&host, &key)?))
    })
    .await
}
async fn cancel(
    State(host): State<Host>,
    Path((session, turn)): Path<(String, String)>,
    request: Result<Json<Empty>, JsonRejection>,
) -> ResultView {
    body(request)?;
    blocking(move || {
        let key = key(&host, &session, &turn)?;
        let current = snapshot(&host, &key)?;
        if current["execution_stopped"] == true && current["state"] != "suspended" {
            return Ok(Json(current));
        }
        host.app.service.cancel(&key)?;
        Ok(Json(snapshot(&host, &key)?))
    })
    .await
}
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Problem> + Send + 'static,
) -> Result<T, Problem> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| Problem(StatusCode::INTERNAL_SERVER_ERROR, "internal_error"))?
}
struct Attempt {
    host: Host,
    session: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.host
            .active
            .lock()
            .expect("HTTP task set")
            .remove(&self.session);
    }
}
fn admit(host: &Host, session: &str) -> Result<Attempt, Problem> {
    let mut active = host.active.lock().expect("HTTP task set");
    if active.contains(session) {
        return Err(Problem(StatusCode::CONFLICT, "conflict"));
    }
    let permit = host
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| Problem(StatusCode::TOO_MANY_REQUESTS, "capacity_exceeded"))?;
    active.insert(session.into());
    drop(active);
    Ok(Attempt {
        host: host.clone(),
        session: session.into(),
        _permit: permit,
    })
}
async fn start(
    State(host): State<Host>,
    Path(session): Path<String>,
    request: Result<Json<Start>, JsonRejection>,
) -> ResultView {
    let request = body(request)?;
    id(&session)?;
    id(&request.turn_id)?;
    if request.input.chars().count() > 32768 {
        return Err(Problem(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"));
    }
    if request.input.is_empty() {
        return Err(Problem(StatusCode::BAD_REQUEST, "invalid_request"));
    }
    let attempt = admit(&host, &session)?;
    let existing = host.app.service.load_reconciled(&session)?;
    if existing
        .turns
        .iter()
        .any(|turn| turn.turn_id == request.turn_id)
    {
        return Err(Problem(StatusCode::CONFLICT, "conflict"));
    }
    let key = ExecutionRef {
        session_id: session,
        turn_id: request.turn_id,
        execution_id: String::new(),
    };
    let key = ExecutionRef {
        execution_id: format!(
            "http-{}-{}-{}",
            key.session_id.len(),
            key.session_id,
            key.turn_id
        ),
        ..key
    };
    let task = tokio::spawn(async move {
        let _attempt = attempt;
        let mut model_request = host.app.template.clone();
        model_request.request_id = format!("{}-request", key.execution_id);
        model_request.messages = vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: request.input,
            }],
        }];
        let result = host
            .app
            .service
            .start(
                host.app.executor(),
                TurnRequest {
                    turn_id: key.turn_id.clone(),
                    model_request,
                    config: host.app.budget,
                },
                &key.session_id,
                &key.execution_id,
            )
            .await;
        finish(&host, &key, result.map(|_| ()))
    });
    Ok(Json(task.await.map_err(|_| {
        Problem(StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
    })??))
}
async fn decide(
    State(host): State<Host>,
    Path((session, turn, approval)): Path<(String, String, String)>,
    request: Result<Json<Decision>, JsonRejection>,
) -> ResultView {
    let request = body(request)?;
    let key = key(&host, &session, &turn)?;
    let pending = snapshot(&host, &key)?;
    if pending["state"] != "suspended"
        || !pending["pending_approvals"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["approval_id"] == approval))
    {
        return Err(Problem(StatusCode::CONFLICT, "conflict"));
    }
    let attempt = admit(&host, &session)?;
    let task = tokio::spawn(async move {
        let _attempt = attempt;
        let result = match request.decision {
            Choice::Approve => host
                .app
                .service
                .resume_approval(
                    host.app.executor(),
                    &key.session_id,
                    &key.execution_id,
                    &approval,
                )
                .await
                .map(|_| ()),
            Choice::Deny => host.app.service.deny(host.app.executor(), &key, &approval),
        };
        finish(&host, &key, result)
    });
    Ok(Json(task.await.map_err(|_| {
        Problem(StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
    })??))
}
fn finish(
    host: &Host,
    key: &ExecutionRef,
    result: Result<(), ServerError>,
) -> Result<Value, Problem> {
    match result {
        Ok(()) => snapshot(host, key),
        Err(error @ ServerError::Runtime(_)) => {
            // Model failures with durable terminal facts are queryable results.
            if let Ok(view) = snapshot(host, key)
                && (view["state"] == "failed" || view["state"] == "cancelled")
            {
                return Ok(view);
            }
            Err(error.into())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
