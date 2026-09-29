//! The door on every internal endpoint: only weft's own identity passes.
//!
//! There is no trusted network. Every call to a role's internal endpoints
//! (and to a unit's host agent) carries a bearer token, checked with the
//! platform's `CallerIdentity` before the request reaches the role. The
//! broker checks its own callers (it also serves workers), so it is not
//! behind this door.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use weft_platform_traits::{CallerIdentity, Principal};

#[derive(Clone)]
pub struct CoreOnly {
    pub identity: Arc<dyn CallerIdentity>,
    /// Every base URL callers address this endpoint by.
    pub audiences: Arc<Vec<String>>,
}

impl CoreOnly {
    /// Put `router` behind the door.
    pub fn guard(&self, router: axum::Router) -> axum::Router {
        router.layer(axum::middleware::from_fn_with_state(self.clone(), core_only))
    }
}

async fn core_only(State(door): State<CoreOnly>, req: Request, next: Next) -> Response {
    let bearer = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_string();
    match door.identity.verify(&bearer, &door.audiences).await {
        Ok(Principal::Core) => next.run(req).await,
        Ok(other) => {
            tracing::warn!(target: "weft_runtime::guard", principal = ?other, path = %req.uri().path(), "refused a caller that is not weft");
            StatusCode::FORBIDDEN.into_response()
        }
        Err(e) => {
            tracing::warn!(target: "weft_runtime::guard", error = %e, path = %req.uri().path(), "refused a call");
            StatusCode::UNAUTHORIZED.into_response()
        }
    }
}
