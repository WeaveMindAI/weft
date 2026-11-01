//! `GET /install/cli/{tree}`: a link to the weft CLI built from the
//! source tree `tree`, as the install stores it, with its sha256.
//!
//! The install workflow stores the CLI it ran with for its own source
//! (built there, or downloaded from weft's release) beside the install's
//! files, so a project's deploy workflow downloads it instead of
//! compiling it, which matters for a fork, whose CLI exists nowhere else.
//! The broker owns the bucket and signs the link as the install's own
//! account (`admin_install_cli`); this route only names the tree and
//! passes the answer on. It is asked once per deploy and keeps nothing.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use weft_core::install::{is_tree_id, InstallCli};

use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;

/// `GET /install/cli/{tree}`: 404 when the install stores no CLI for that
/// tree (the caller then looks elsewhere or builds one).
// SYNC: the route <-> scripts/release-cli.sh (from_install), crates/weft-core/src/install.rs (InstallCli)
pub async fn for_tree(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
    Path(tree): Path<String>,
) -> Result<Json<InstallCli>, (StatusCode, String)> {
    if !is_tree_id(&tree) {
        return Err((StatusCode::BAD_REQUEST, format!("'{tree}' is not a git tree id (40 or 64 lowercase hex digits)")));
    }
    let asked = async {
        state
            .broker
            .request(reqwest::Method::GET, &format!("/v1/storage/admin/install-cli/{tree}"))
            .await?
            .timeout(crate::storage::BROKER_STORAGE_CALL_TIMEOUT)
            .send()
            .await
            .map_err(anyhow::Error::from)
    };
    let response = asked.await.map_err(|e| {
        let status = match crate::storage::storage_unreachable(&e) {
            true => StatusCode::BAD_GATEWAY,
            false => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, format!("ask the install's storage for the CLI built from {tree}: {e:#}"))
    })?;
    match response.status() {
        reqwest::StatusCode::OK => response.json::<InstallCli>().await.map(Json).map_err(|e| {
            (StatusCode::BAD_GATEWAY, format!("the install's storage answered the CLI built from {tree} with something unreadable: {e}"))
        }),
        reqwest::StatusCode::NOT_FOUND => Err((
            StatusCode::NOT_FOUND,
            format!("this install stores no weft CLI built from the source tree {tree}; its install workflow stores the one it ran with"),
        )),
        status => {
            let body = response.text().await.unwrap_or_default();
            Err((StatusCode::BAD_GATEWAY, format!("the install's storage answered {status} for the CLI built from {tree}: {body}")))
        }
    }
}
