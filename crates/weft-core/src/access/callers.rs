//! How a trigger checks who calls it (`features.checksCallers`: Route and
//! Socket). Its `auth` setting names a scheme, and its `connection` field
//! holds what the scheme checks against: a connection picked on the
//! install per node place, exactly as an access node's is
//! (`crate::picks`). Everything a check needs is defined here, once: the
//! names `auth` takes and the exact check each one runs ([`NAMED_CHECKS`]),
//! and the recipe of the connection each kind of check reads
//! ([`CallerScheme`]). No catalog node carries these; everything that asks
//! "which recipe does this field connect to" asks [`connection_field`].

use serde_json::Value;

use super::events::{VerifyKind, VerifySide};
use super::spec::AccessSpec;
use crate::node::NodeMetadata;

/// The setting naming how callers are checked.
pub const AUTH_FIELD: &str = "auth";

/// The field holding the connection a scheme checks against.
pub const CONNECTION_FIELD: &str = "connection";

/// The `auth` value that checks nobody (the default).
pub const NO_AUTH: &str = "none";

/// One check `auth` names by a word: exactly one algorithm and one layout.
pub struct NamedCheck {
    /// The word `auth` takes.
    pub name: &'static str,
    /// What the caller sends, in a sentence (the editor's and
    /// `describe-nodes`' explanation of the option).
    pub means: &'static str,
    /// The check, as the record `auth` would spell it.
    record: &'static str,
}

impl NamedCheck {
    /// The check this name runs.
    pub fn verify(&self) -> VerifyKind {
        serde_json::from_str(self.record).expect("a named check is a valid check")
    }
}

/// THE names `auth` takes besides `none`, and the exact check each runs.
/// A recipe weft implements gets a name by one entry here; anything else
/// is spelled as the check record itself. Route's and Socket's metadata
/// list the same names (a test holds them to this table).
pub const NAMED_CHECKS: &[NamedCheck] = &[
    NamedCheck {
        name: "api_keys",
        means: "one of the connection's keys, in `X-Api-Key` or as `Authorization: Bearer <key>`",
        record: r#"{ "kind": "api_keys" }"#,
    },
    NamedCheck {
        name: "hmac_sha256",
        means: "the hex HMAC-SHA256 of `<X-Timestamp>.<body>` under the connection's secret, in `X-Signature`",
        record: r#"{ "kind": "hmac", "signature": { "in": "header:X-Signature" }, "timestamp": { "in": "header:X-Timestamp" },
                    "concat": "{timestamp}.{body}" }"#,
    },
    NamedCheck {
        name: "hmac_sha1",
        means: "the hex HMAC-SHA1 of `<X-Timestamp>.<body>` under the connection's secret, in `X-Signature`",
        record: r#"{ "kind": "hmac", "signature": { "in": "header:X-Signature" }, "timestamp": { "in": "header:X-Timestamp" },
                    "concat": "{timestamp}.{body}", "algorithm": "sha1" }"#,
    },
    NamedCheck {
        name: "hmac_sha512",
        means: "the hex HMAC-SHA512 of `<X-Timestamp>.<body>` under the connection's secret, in `X-Signature`",
        record: r#"{ "kind": "hmac", "signature": { "in": "header:X-Signature" }, "timestamp": { "in": "header:X-Timestamp" },
                    "concat": "{timestamp}.{body}", "algorithm": "sha512" }"#,
    },
    NamedCheck {
        name: "ed25519",
        means: "the hex Ed25519 signature of `<X-Timestamp>.<body>`, in `X-Signature`, checked against the connection's public key",
        record: r#"{ "kind": "signature", "scheme": "ed25519", "signature": { "in": "header:X-Signature" },
                    "timestamp": { "in": "header:X-Timestamp" }, "concat": "{timestamp}.{body}" }"#,
    },
    NamedCheck {
        name: "ecdsa_p256",
        means: "the base64 DER ECDSA P-256 signature of `<X-Timestamp>.<body>`, in `X-Signature`, checked against the connection's public key",
        record: r#"{ "kind": "signature", "scheme": "ecdsa_p256", "signature": { "in": "header:X-Signature" },
                    "timestamp": { "in": "header:X-Timestamp" }, "concat": "{timestamp}.{body}", "encoding": "base64" }"#,
    },
    NamedCheck {
        name: "oidc",
        means: "an identity token in `Authorization: Bearer`, from the connection's issuer and for its audience",
        record: r#"{ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }"#,
    },
];

/// The named check `name`, if `auth` takes that word.
pub fn named_check(name: &str) -> Option<&'static NamedCheck> {
    NAMED_CHECKS.iter().find(|check| check.name == name)
}

/// The kind of connection a check reads, by the kind of check it is
/// (its record's `kind`): one recipe each, which every named check of
/// that kind shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerScheme {
    ApiKeys,
    Hmac,
    Signature,
    Oidc,
}

impl CallerScheme {
    pub const ALL: [CallerScheme; 4] = [Self::ApiKeys, Self::Hmac, Self::Signature, Self::Oidc];

    /// Its check record's `kind`.
    pub fn name(self) -> &'static str {
        match self {
            Self::ApiKeys => "api_keys",
            Self::Hmac => "hmac",
            Self::Signature => "signature",
            Self::Oidc => "oidc",
        }
    }

    /// The service of the connection it reads. `api_key_auth` and
    /// `hmac_auth` are the names the access nodes these schemes replaced
    /// gave their connections, so connections already stored still match.
    pub fn service(self) -> &'static str {
        match self {
            Self::ApiKeys => "api_key_auth",
            Self::Hmac => "hmac_auth",
            Self::Signature => "signature_auth",
            Self::Oidc => "oidc_auth",
        }
    }

    fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.name() == name)
    }

    /// The scheme a check is of; `None` for one that cannot check a
    /// caller (`token_echo`).
    pub fn of(verify: &VerifyKind) -> Option<Self> {
        Self::named(verify.tag())
    }

    /// The scheme whose connections are of `service`.
    pub fn of_service(service: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.service() == service)
    }

    /// The recipe of the connection the scheme reads: a pasted secret (or
    /// for `oidc`, the issuer's addresses and the audience), stored under
    /// the names the check reads (`VerifyKind::required_value_names`).
    pub fn recipe(self) -> AccessSpec {
        let (label, fields) = match self {
            Self::ApiKeys => (
                "API keys",
                serde_json::json!([{
                    "name": super::events::API_KEYS,
                    "label": "Accepted keys",
                    "placeholder": "one or more keys, comma-separated",
                    "secret": true,
                }]),
            ),
            Self::Hmac => (
                "Signing secret",
                serde_json::json!([{
                    "name": super::events::SIGNING_SECRET,
                    "label": "Signing secret",
                    "placeholder": "the secret the caller signs with",
                    "secret": true,
                }]),
            ),
            Self::Signature => (
                "Public key",
                serde_json::json!([{
                    "name": super::events::PUBLIC_KEY,
                    "label": "Public key",
                    "placeholder": "the key the caller's signatures check against: hex for ed25519, base64 DER for ecdsa_p256",
                    "secret": false,
                }]),
            ),
            Self::Oidc => (
                "Identity tokens",
                serde_json::json!([
                    {
                        "name": "issuer",
                        "label": "Issuer",
                        "placeholder": "https://your-tenant.example.com/",
                        "secret": false,
                    },
                    {
                        "name": "jwksUrl",
                        "label": "Signing keys address (JWKS)",
                        "placeholder": "https://your-tenant.example.com/.well-known/jwks.json",
                        "secret": false,
                    },
                    {
                        "name": super::events::AUDIENCE,
                        "label": "Audience",
                        "placeholder": "the audience your tokens are minted for",
                        "secret": false,
                    },
                ]),
            ),
        };
        serde_json::from_value(serde_json::json!({
            "service": self.service(),
            "label": label,
            "acquisition": { "kind": "static", "ways": [{ "fields": fields }] },
            "identity": label,
        }))
        .expect("a scheme's recipe is a valid recipe")
    }
}

/// The words `auth` takes, `none` first, for a message and for a select.
pub fn auth_options() -> Vec<&'static str> {
    std::iter::once(NO_AUTH).chain(NAMED_CHECKS.iter().map(|check| check.name)).collect()
}

/// Read the `auth` setting: `None` when it checks nobody (absent, empty,
/// or `"none"`), else the check it runs. A name is its entry in
/// [`NAMED_CHECKS`]; a record is the check itself, refused when it cannot check a caller or reads a value its
/// scheme's connection does not hold.
pub fn parse_auth(value: Option<&Value>) -> Result<Option<VerifyKind>, String> {
    let words = || auth_options().join(", ");
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.trim().is_empty() || s == NO_AUTH => Ok(None),
        Some(Value::String(s)) => match named_check(s) {
            Some(check) => Ok(Some(check.verify())),
            None => Err(format!("auth must be one of {}, or a scheme written as a record; got '{s}'", words())),
        },
        Some(record @ Value::Object(_)) => {
            let verify: VerifyKind =
                serde_json::from_value(record.clone()).map_err(|e| format!("auth is not a scheme weft knows: {e}"))?;
            let Some(scheme) = CallerScheme::of(&verify) else {
                // The scheme's own refusal says why it cannot check a caller.
                verify.validate(AUTH_FIELD, VerifySide::Caller)?;
                unreachable!("only token_echo is no caller scheme, and it never checks a caller");
            };
            verify.validate(scheme.service(), VerifySide::Caller)?;
            let recipe = scheme.recipe();
            let held = &recipe.chosen_way(None)?.fields;
            for name in verify.required_value_names()? {
                if !held.iter().any(|f| f.name == name) {
                    return Err(format!(
                        "auth reads '{name}' off its connection, which a {} connection does not hold \
                         (it holds {})",
                        scheme.name(),
                        held.iter().map(|f| format!("'{}'", f.name)).collect::<Vec<_>>().join(", ")
                    ));
                }
            }
            Ok(Some(verify))
        }
        Some(other) => Err(format!("auth must be one of {}, or a scheme written as a record; got {other}", words())),
    }
}

/// What a person reads when a node checking its callers has no
/// connection: `place` the node's place, spelled; `verify` the check its
/// `auth` names.
pub fn no_connection(place: &str, verify: &VerifyKind) -> String {
    format!("'{place}' checks callers with {} but has no connection; run `weft connect {place}`", verify.tag())
}

/// The field of a node that takes a connection, and the recipe of what
/// it connects to.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionField {
    pub input: String,
    pub spec: AccessSpec,
}

/// THE answer to "which field of this node takes a connection, and to
/// which recipe", asked by the compiler (stamping, picks), `weft connect`
/// and the editor's mirror of it. An access node: its access widget, to
/// its own `service` recipe. A node that checks its callers: its
/// `connection` field, to the recipe of the scheme `auth` (its written
/// value) names, and nothing while `auth` checks nobody. `None` for any
/// other node.
pub fn connection_field(meta: &NodeMetadata, auth: Option<&Value>) -> Result<Option<ConnectionField>, String> {
    let Some(input) = meta.access_input() else { return Ok(None) };
    if let Some(spec) = &meta.service {
        return Ok(Some(ConnectionField { input: input.name.clone(), spec: spec.clone() }));
    }
    if !meta.features.checks_callers {
        return Ok(None);
    }
    Ok(parse_auth(auth)?.map(|verify| ConnectionField {
        input: input.name.clone(),
        spec: CallerScheme::of(&verify).expect("parse_auth answers caller schemes only").recipe(),
    }))
}

/// The rules a node declaring `checksCallers` is held to at metadata
/// load: an `auth` setting written as a literal, and a `connection` input
/// with the access widget, and no recipe of its own (the scheme brings
/// it).
pub fn check_metadata(meta: &NodeMetadata) -> Result<(), String> {
    if meta.service.is_some() {
        return Err(
            "a node that checks its callers takes the recipe of the scheme its `auth` names, so it \
             declares no `service` recipe of its own"
                .into(),
        );
    }
    if !meta.inputs.iter().any(|i| i.name == AUTH_FIELD) {
        return Err(format!("a node that checks its callers declares the `{AUTH_FIELD}` input its scheme is named in"));
    }
    match meta.access_input() {
        Some(input) if input.name == CONNECTION_FIELD => Ok(()),
        _ => Err(format!(
            "a node that checks its callers declares a `{CONNECTION_FIELD}` input with the `access` widget, \
             where the connection its scheme checks against is picked"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(name: &str) -> VerifyKind {
        named_check(name).unwrap_or_else(|| panic!("no named check '{name}'")).verify()
    }

    /// Every name is a valid check a caller can be checked with, and
    /// reads its connection; `none` checks nobody.
    #[test]
    fn each_name_is_its_check_and_none_checks_nobody() {
        for absent in [None, Some(&Value::Null), Some(&json!("")), Some(&json!("none"))] {
            assert_eq!(parse_auth(absent).unwrap(), None, "{absent:?}");
        }
        for named in NAMED_CHECKS {
            let verify = parse_auth(Some(&json!(named.name))).unwrap().unwrap();
            assert_eq!(verify, named.verify());
            let scheme = CallerScheme::of(&verify).unwrap_or_else(|| panic!("{} checks no caller", named.name));
            verify.validate(scheme.service(), VerifySide::Caller).unwrap_or_else(|e| panic!("{}: {e}", named.name));
        }
        let refused = parse_auth(Some(&json!("hmac"))).unwrap_err();
        assert!(
            refused.contains("none, api_keys, hmac_sha256, hmac_sha1, hmac_sha512, ed25519, ecdsa_p256, oidc")
                && refused.contains("'hmac'"),
            "{refused}"
        );
        assert!(parse_auth(Some(&json!(3))).is_err());
    }

    /// Each name fixes its algorithm; the layout is the one the access
    /// nodes these replaced checked.
    #[test]
    fn each_name_fixes_one_algorithm() {
        use super::super::events::{HmacAlgorithm, SignatureScheme};
        assert_eq!(
            serde_json::to_value(check("hmac_sha256")).unwrap(),
            json!({ "kind": "hmac", "signature": { "in": "header:X-Signature" },
                    "timestamp": { "in": "header:X-Timestamp" }, "concat": "{timestamp}.{body}" })
        );
        for (name, wanted) in [("hmac_sha256", HmacAlgorithm::Sha256), ("hmac_sha1", HmacAlgorithm::Sha1), ("hmac_sha512", HmacAlgorithm::Sha512)] {
            assert!(matches!(check(name), VerifyKind::Hmac { algorithm, .. } if algorithm == wanted), "{name}");
        }
        assert!(matches!(check("ed25519"), VerifyKind::Signature { scheme: SignatureScheme::Ed25519, .. }));
        assert!(matches!(check("ecdsa_p256"), VerifyKind::Signature { scheme: SignatureScheme::EcdsaP256, .. }));
        assert_eq!(serde_json::to_value(check("api_keys")).unwrap(), json!({ "kind": "api_keys" }));
        assert_eq!(check("api_keys").credential_headers(), vec!["X-Api-Key".to_string(), "authorization".to_string()]);
    }

    #[test]
    fn a_record_is_the_check_itself() {
        let record = json!({ "kind": "hmac", "signature": { "in": "header:X-Hub-Signature-256" }, "concat": "{body}" });
        let verify = parse_auth(Some(&record)).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&verify).unwrap(), record);
        let header = json!({ "kind": "api_keys", "header": "X-Team-Key", "bearer": false });
        assert_eq!(parse_auth(Some(&header)).unwrap().unwrap().credential_headers()[0], "X-Team-Key");
    }

    #[test]
    fn a_record_that_cannot_check_a_caller_is_refused() {
        let echo = parse_auth(Some(&json!({ "kind": "token_echo" }))).unwrap_err();
        assert!(echo.contains("token_echo"), "{echo}");
        let unknown = parse_auth(Some(&json!({ "kind": "magic" }))).unwrap_err();
        assert!(unknown.starts_with("auth is not a scheme weft knows"), "{unknown}");
        let stray = parse_auth(Some(&json!({ "kind": "oidc", "issuers": ["{tenant}"], "jwksUrl": "{jwksUrl}" }))).unwrap_err();
        assert!(stray.contains("'tenant'") && stray.contains("'issuer'"), "{stray}");
        // A record meets the rules a push recipe does: signing a
        // {timestamp} with nowhere to read it from is refused here, never
        // checked at the door with no replay check.
        let unsourced = json!({ "kind": "hmac", "signature": { "in": "header:X-Signature" }, "concat": "{timestamp}.{body}" });
        let err = parse_auth(Some(&unsourced)).unwrap_err();
        assert!(err.contains("no timestamp source"), "{err}");
        let literal = json!({ "kind": "oidc", "issuers": ["https://id.example.com/"], "jwksUrl": "https://id.example.com/jwks" });
        assert!(parse_auth(Some(&literal)).is_ok(), "a literal issuer reads only the audience");
    }

    /// Each kind's recipe is a valid recipe of its service, and holds
    /// every value each named check of that kind reads.
    #[test]
    fn each_scheme_has_a_recipe_holding_what_its_checks_read() {
        for scheme in CallerScheme::ALL {
            let recipe = scheme.recipe();
            recipe.validate().unwrap_or_else(|e| panic!("{}: {e}", scheme.name()));
            assert_eq!(recipe.service, scheme.service());
            assert_eq!(CallerScheme::of_service(scheme.service()), Some(scheme));
            assert!(!recipe.connection_optional, "a check with no connection checks nobody");
        }
        for named in NAMED_CHECKS {
            let verify = named.verify();
            let recipe = CallerScheme::of(&verify).unwrap().recipe();
            let held: Vec<String> = recipe.chosen_way(None).unwrap().fields.iter().map(|f| f.name.clone()).collect();
            for name in verify.required_value_names().unwrap() {
                assert!(held.contains(&name), "{}: {name} not in {held:?}", named.name);
            }
        }
        assert_eq!(CallerScheme::ApiKeys.service(), "api_key_auth");
        assert_eq!(CallerScheme::Hmac.service(), "hmac_auth");
    }

    /// Route's and Socket's metadata list `auth`'s options for the editor;
    /// the list is this table's names, `none` first.
    #[test]
    fn route_and_socket_list_exactly_the_named_checks() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../catalog/api");
        for node in ["route", "socket"] {
            let path = root.join(node).join("metadata.json");
            let meta: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let auth = meta["inputs"].as_array().unwrap().iter().find(|i| i["name"] == AUTH_FIELD).expect("an auth input");
            let listed: Vec<&str> = auth["widget"]["options"].as_array().unwrap().iter().map(|o| o.as_str().unwrap()).collect();
            assert_eq!(listed, auth_options(), "{}", path.display());
            assert_eq!(auth["default"], NO_AUTH, "{}", path.display());
        }
    }

    #[test]
    fn the_missing_connection_names_the_place_and_the_scheme() {
        assert_eq!(
            no_connection("secure", &check("api_keys")),
            "'secure' checks callers with api_keys but has no connection; run `weft connect secure`"
        );
    }
}
