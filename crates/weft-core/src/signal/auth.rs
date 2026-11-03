//! Auth policy shared by every public-entry signal kind (any kind served
//! on a public URL: the live-caller routes today). Not
//! tied to one kind, so it lives in its own module rather than on a single
//! signal struct.

use serde::{Deserialize, Serialize};

/// How the worker's door gates a caller.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicEntryAuth {
    /// Anyone with the URL can connect.
    #[default]
    None,
    /// The caller is checked with `verify` (the scheme the trigger's
    /// `auth` names, `crate::access::callers`) against the values a stored
    /// connection holds. The door asks the broker (`/v1/caller/verify`),
    /// which holds the connection, and admits the run only on a yes.
    Connection {
        /// The connection to check against (the trigger's install pick).
        access_id: String,
        /// The service the connection belongs to (the scheme's), so the
        /// lookup refuses a connection of another service instead of
        /// reading its fields blind.
        service: String,
        /// The check itself, boxed: it is most of the enum's size.
        verify: Box<crate::access::events::VerifyKind>,
    },
    /// A gate an older weft stored when the check lived on the
    /// connection's recipe, for a service whose check could not be
    /// carried ([`auth_to_v2`]). Never made today: it keeps the stored
    /// record readable, and the door refuses every caller of it, naming
    /// the fix ([`uncarried_gate`]), so it is never read as open.
    CheckNotKept { access_id: String, service: String },
}

/// The steps a stored `auth` (inside a live-caller signal's config) takes
/// from the shape an older weft wrote; see [`auth_to_v2`].
pub const AUTH_V2: &[crate::stored::Op] = &[crate::stored::Op::Code { at: "auth", run: auth_to_v2 }];

/// A stored `auth` written when the check was the connection's recipe's
/// `verify` block, carried to the shape where the trigger names it. The
/// two access nodes weft shipped for this are known: a connection of
/// `api_key_auth` checked with `api_keys`, one of `hmac_auth` with
/// `hmac`, each the plain scheme (their layout is the plain scheme's).
/// A connection of any other service (a project's own auth access node)
/// carried a check this row never kept, so it cannot be carried: it
/// becomes [`PublicEntryAuth::CheckNotKept`], whose door refuses every
/// caller, naming the fix (set the trigger's `auth`, then `weft resync`).
pub fn auth_to_v2(auth: &mut serde_json::Value) -> Result<(), String> {
    let serde_json::Value::Object(auth) = auth else { return Ok(()) };
    if auth.get("kind").and_then(serde_json::Value::as_str) != Some("connection") || auth.contains_key("verify") {
        return Ok(());
    }
    let service = auth.get("service").and_then(serde_json::Value::as_str).unwrap_or_default();
    // What the two access nodes checked, by the names `auth` gives it now.
    let checked = match service {
        "api_key_auth" => Some("api_keys"),
        "hmac_auth" => Some("hmac_sha256"),
        _ => None,
    };
    if let Some(check) = checked.and_then(crate::access::callers::named_check) {
        let verify = serde_json::to_value(check.verify()).map_err(|e| e.to_string())?;
        auth.insert("verify".into(), verify);
    } else {
        auth.insert("kind".into(), "check_not_kept".into());
    }
    Ok(())
}

/// What the door answers on a route whose stored gate names no check: it
/// was set up by a weft whose check lived on the connection's recipe.
/// Either its service's check could not be carried ([`auth_to_v2`]), or
/// the row is a routing entry that weft never rewrites (the signal's
/// `auth_config`, written when it was set up); a resync sets it up again
/// with the check the Route names.
pub fn uncarried_gate(service: &str) -> String {
    format!(
        "this route was set up by an older weft, which kept its check on the '{service}' connection; \
         set the Route's `auth` to the check its callers use ({}, or a check record) and run `weft resync`",
        crate::access::callers::auth_options()[1..].join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_stored_gate_of_a_known_node_takes_its_scheme() {
        let mut keys = json!({ "kind": "connection", "access_id": "a", "service": "api_key_auth" });
        auth_to_v2(&mut keys).unwrap();
        assert_eq!(keys["verify"], json!({ "kind": "api_keys" }));
        let mut hmac = json!({ "kind": "connection", "access_id": "a", "service": "hmac_auth" });
        auth_to_v2(&mut hmac).unwrap();
        assert_eq!(hmac["verify"]["kind"], "hmac");
        assert_eq!(hmac["verify"]["signature"]["in"], "header:X-Signature");
        let back: PublicEntryAuth = serde_json::from_value(hmac).unwrap();
        assert!(matches!(back, PublicEntryAuth::Connection { ref service, .. } if service == "hmac_auth"));
    }

    #[test]
    fn a_stored_gate_of_another_service_is_kept_as_a_check_not_kept() {
        let mut own = json!({ "kind": "connection", "access_id": "a", "service": "jwt_auth" });
        auth_to_v2(&mut own).unwrap();
        let read: PublicEntryAuth = serde_json::from_value(own).unwrap();
        assert_eq!(read, PublicEntryAuth::CheckNotKept { access_id: "a".into(), service: "jwt_auth".into() });
        let mut open = json!({ "kind": "none" });
        auth_to_v2(&mut open).unwrap();
        assert_eq!(open, json!({ "kind": "none" }));
        assert!(uncarried_gate("jwt_auth").contains("weft resync"));
    }
}
