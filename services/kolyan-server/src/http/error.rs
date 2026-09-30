//! Safe HTTP failure translation; underlying exceptions never cross the wire.
use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use kolyan_server::ServerError;
use kolyan_storage::StorageError;
use serde_json::json;

pub(super) struct Problem(pub(super) StatusCode, pub(super) &'static str);
impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let mut response = (self.0, Json(json!({"type":"about:blank","title":self.0.canonical_reason().unwrap_or("Error"),"status":self.0.as_u16(),"code":self.1,"detail":self.1}))).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            "application/problem+json".parse().unwrap(),
        );
        if self.0 == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, "Bearer".parse().unwrap());
        }
        if self.0 == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}
impl From<ServerError> for Problem {
    fn from(error: ServerError) -> Self {
        match error {
            ServerError::Session(StorageError::NotFound(_)) => {
                Self(StatusCode::NOT_FOUND, "not_found")
            }
            ServerError::Session(StorageError::Conflict(_))
            | ServerError::Coordinator(kolyan_server::CoordinatorError::AlreadyActive { .. })
            | ServerError::Coordinator(kolyan_server::CoordinatorError::NotSuspended { .. })
            | ServerError::Coordinator(kolyan_server::CoordinatorError::Terminal { .. }) => {
                Self(StatusCode::CONFLICT, "conflict")
            }
            _ => Self(StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        }
    }
}
