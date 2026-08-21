//! Stable codes for refusals a user can report back to whoever runs this API.
//!
//! A user who hits one of these sees the code. Grep it here to learn what the
//! refusal means and what clears it. Codes never change meaning: retire one
//! rather than repurpose it.
//!
//! | Code      | Meaning                                        | What clears it |
//! |-----------|------------------------------------------------|----------------|
//! | `INF-C01` | No room for another project, across all users  | Free a slot, or raise `MAX_GLOBAL_INFRA_PROJECTS` and redeploy |
//! | `INF-C02` | No room for another project for this user      | The user stops one of theirs, or raise `MAX_USER_INFRA_PROJECTS` and redeploy |
//! | `INF-C03` | The cluster could not be asked                 | Check the API's own health and its cluster access |
//!
//! Both ceilings are compile-time constants in `weft_core::k8s_provisioner`,
//! so changing one means a redeploy.

use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};

/// No room for another project, across all users.
pub const INFRA_CAPACITY_GLOBAL: &str = "INF-C01";

/// No room for another project for this user.
pub const INFRA_CAPACITY_USER: &str = "INF-C02";

/// The cluster could not be asked whether there is room.
pub const INFRA_CAPACITY_UNKNOWN: &str = "INF-C03";

/// Where to send a user who hits one of these, when the operator has said.
///
/// Read once per refusal from `SUPPORT_CONTACT`. Unset is the ordinary case
/// for anyone running their own instance, and then the code stands on its own.
fn support_contact() -> Option<String> {
    std::env::var("SUPPORT_CONTACT").ok().filter(|s| !s.is_empty())
}

/// A refusal the user can report: what happened, the code, and who to tell.
// SYNC: refusal body <-> weft/dashboard/src/lib/config.ts (failureReason)
pub fn refusal(status: StatusCode, code: &str, what_happened: &str) -> Response {
    let error = match support_contact() {
        Some(contact) => format!("{what_happened} Contact {contact} and quote {code}."),
        None => format!("{what_happened} (Error {code})"),
    };
    (status, Json(serde_json::json!({ "error": error, "code": code }))).into_response()
}

/// The cluster could not be asked, so we do not know whether there is room.
/// A dependency being unreachable is temporary, and retrying is the right
/// move, so it must not read as a conflict with the request itself.
pub fn capacity_unavailable() -> Response {
    refusal(
        StatusCode::SERVICE_UNAVAILABLE,
        INFRA_CAPACITY_UNKNOWN,
        "Could not check whether there is room to start infrastructure.",
    )
}
