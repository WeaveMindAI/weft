//! What an install tells about itself (`GET /install`): the addresses it
//! answers at and, on a cloud, the pieces a project's CI deploys a
//! frontend with. Derived from the install's config, read by `weft login`
//! (to prove a key) and `weft target export` (to hand a project's CI what
//! it needs). Also the domains an install answers at.

use serde::{Deserialize, Serialize};

/// The request header carrying the commit the calling weft CLI was built
/// from, sent on every call when the build knew it.
pub const CLI_COMMIT_HEADER: &str = "x-weft-cli-commit";

/// The response header an install adds when the calling CLI was built
/// from another commit than the weft the install runs: one sentence the
/// CLI prints as a warning. The install never refuses a CLI for it, since
/// it compiles projects itself and a CLI from another commit usually works.
pub const VERSION_NOTE_HEADER: &str = "x-weft-version-note";

/// The response header the install sets on every answer it gives itself,
/// so a caller can tell weft's own answer (a `429` for a limit a caller
/// went past) from one the platform in front of it gave in its place
/// (Cloud Run's `429` when every copy of the service it may start is busy,
/// sent before weft ever saw the request, so nothing ran). The install's
/// counterpart of `weft_platform_traits::WORKER_ANSWER_HEADER`.
// SYNC: INSTALL_ANSWER_HEADER <-> extension-vscode/src/dispatcher.ts INSTALL_ANSWER_HEADER
pub const INSTALL_ANSWER_HEADER: &str = "x-weft-install";

/// Whether an answer the install gave itself (`from_install`: it carries
/// [`INSTALL_ANSWER_HEADER`]) with `status` asks the caller to send the
/// same request again after a wait.
///
/// The rule both ends keep: a `503` from the install means exactly
/// "nothing of this request ran, and the same request goes through
/// later", whatever its method. Today that is its database not taking the
/// call (too busy, or out of reach for now), or a worker or an infra
/// endpoint the call is for still starting. The install answers `503` for
/// nothing else: a failure that may have done part of its work, or that
/// waiting does not lift (its database refusing for good, its storage
/// down), gets another status (`500` for a failure inside the install,
/// `502` for a service behind it that failed) and keeps its words. So a
/// caller sends a `503` again, for as long as it takes, and shows every
/// other answer as it came.
///
/// Two answers carry the install's mark without being its own word on
/// the request, both on the doors a program's outside callers use, which
/// the CLI never calls: what a program's route or infra endpoint answered,
/// passed on as it came, and a call to a trigger that is not on
/// (`crate::arrival::NOT_NOW_FOR_CALLER`, a `503` with a `Retry-After`:
/// nothing ran, and it goes through once the trigger is on).
// SYNC: asks_again <-> crates/weft-dispatcher/src/api/mod.rs (database_did_not_take, unavailable_or_internal, access_store_error), crates/weft-dispatcher/src/api/storage.rs (storage_err), crates/weft-cli/src/client.rs (busy_answer), extension-vscode/src/dispatcher.ts (worthTryingAgain)
pub fn asks_again(status: u16, from_install: bool) -> bool {
    status == 503 && from_install
}

// SYNC: InstallInfo <-> crates/weft-dispatcher/src/app.rs (install_info)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallInfo {
    /// Where people and providers reach the install
    /// (`https://weft.example.com`): its domain once that domain's
    /// certificate is issued, else the platform's own address.
    #[serde(rename = "publicUrl")]
    pub public_url: String,
    /// The platform's own address (a Cloud Run service's `run.app` name),
    /// which keeps answering whatever domain the install has.
    #[serde(rename = "serviceUrl")]
    pub service_url: String,
    /// The cloud the install runs on, `None` on a local one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud: Option<CloudInstall>,
    /// The weft source the install runs. A project's deploy workflow reads
    /// it on every run and deploys with that weft's CLI, and the install
    /// compares it with the commit a CLI sends ([`CLI_COMMIT_HEADER`]).
    /// `None` on a local install, whose CLI is the one beside it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<WeftSource>,
}

/// A commit of a weft repository (upstream, or the fork the install
/// workflow ran in).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeftSource {
    /// `owner/name` on GitHub.
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum CloudInstall {
    Gcp(GcpInstall),
}

/// What a project's CI needs to deploy a frontend beside a GCP install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcpInstall {
    pub project: String,
    pub region: String,
    /// The Artifact Registry repository frontend images are pushed to
    /// (`us-central1-docker.pkg.dev/<project>/<repo>`).
    #[serde(rename = "artifactRegistry")]
    pub artifact_registry: String,
    /// The VPC network and subnet a frontend on Cloud Run reaches the
    /// install's private address through (Direct VPC egress).
    pub network: String,
    pub subnet: String,
    /// The service account a project's CI deploys as.
    #[serde(rename = "deployerServiceAccount")]
    pub deployer_service_account: String,
    /// The service account a frontend runs as on Cloud Run.
    #[serde(rename = "frontendServiceAccount")]
    pub frontend_service_account: String,
    /// The Workload Identity Federation provider GitHub Actions
    /// authenticates through.
    #[serde(rename = "workloadIdentityProvider")]
    pub workload_identity_provider: String,
}

/// A domain the install answers at: its own management and public doors,
/// or one project's frontend or API. Stored by the install (not in its
/// config, since `weft domain add` reaches it over HTTP) and read by its
/// front door, which gets each one a certificate and routes by it.
// SYNC: Domain <-> crates/weft-dispatcher/src/domains.rs (the install_domain
//       canonical DDL and its reads/writes), crates/weft-task-store/migrations/install_domain/origin.sql
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    /// The name, lower case (`app.example.com`).
    pub name: String,
    #[serde(rename = "for")]
    pub serves: DomainServes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DomainServes {
    /// The install itself: everything its own address serves.
    Install,
    /// One project's frontend, which runs at `upstream` (its own service's
    /// address, as its CI deployed it).
    Frontend { project: uuid::Uuid, upstream: String },
    /// One project's API: its route and socket entries, at the root of the
    /// domain.
    Api { project: uuid::Uuid },
}

impl DomainServes {
    /// The project it belongs to, `None` for the install's own.
    pub fn project(&self) -> Option<uuid::Uuid> {
        match self {
            DomainServes::Install => None,
            DomainServes::Frontend { project, .. } | DomainServes::Api { project } => Some(*project),
        }
    }
}

/// Where the certificate the install's door holds for a domain stands.
/// A link on a domain is handed out only once its certificate is issued:
/// before that, whoever opens it meets a TLS error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Certificate {
    /// Issued: the domain answers over https.
    Issued,
    /// Not issued yet, and the authority is still trying; `why` says what
    /// it waits on when it says (usually the DNS record).
    Pending {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        why: Option<String>,
    },
    /// The authority gave up. Removing the domain and adding it again
    /// asks anew.
    Failed { why: String },
}

/// A stored domain and the DNS record that points it at the install:
/// what `GET /install/domains` lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainEntry {
    pub domain: Domain,
    pub record: DnsRecord,
    pub certificate: Certificate,
}

/// What `GET /install/domains` answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainList {
    pub domains: Vec<DomainEntry>,
    /// The address the install hands out in its links now: its domain
    /// once that domain's certificate is issued, else the platform's own.
    #[serde(rename = "liveAddress")]
    pub live_address: String,
    /// Where a provider sends a person back after they sign in
    /// (`<liveAddress>/access/oauth/callback`): what each provider's app
    /// must list once the address moves to a domain.
    #[serde(rename = "callbackUrl")]
    pub callback_url: String,
    /// The services with a sign-in on this install (an app of their own
    /// on a connection, or a shared app in the apps file): the providers
    /// whose app must list [`Self::callback_url`].
    #[serde(rename = "callbackServices")]
    pub callback_services: Vec<String>,
    /// Why the certificates could not be asked after just now, when they
    /// could not: each domain's then says what was last recorded.
    #[serde(default, rename = "certificatesUnread", skip_serializing_if = "Option::is_none")]
    pub certificates_unread: Option<String>,
    /// Why the platform's door refused to follow the stored domains, while
    /// it keeps refusing. The install tries again on its own, less often
    /// each time; what it says is usually something a person has to fix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub door_refused: Option<String>,
}

/// `POST /install/domains`: a domain to answer at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddDomain {
    pub domain: Domain,
    /// Agreement to what the door in front of the install's domains costs
    /// while it stands, which the first domain makes: the install refuses
    /// to make it without, saying what it costs.
    #[serde(default, rename = "acceptCost")]
    pub accept_cost: bool,
}

/// What `POST /install/domains` answers: the stored domain, its record,
/// and what the door in front of the install's domains costs while it
/// stands, when it costs anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainAdded {
    pub domain: Domain,
    pub record: DnsRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<String>,
}

/// The DNS record that points a domain at the install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsRecord {
    /// `A` for an IPv4 address, `AAAA` for an IPv6 one.
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub value: String,
}

impl std::fmt::Display for DnsRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} record, name {}, value {}", self.kind, self.name, self.value)
    }
}

impl Domain {
    /// Check a domain before it is stored, naming what is wrong.
    pub fn validate(&self) -> Result<(), String> {
        check_domain_name(&self.name)?;
        if let DomainServes::Frontend { upstream, .. } = &self.serves {
            if !upstream.starts_with("https://") {
                return Err(format!(
                    "a frontend's address must be https (got '{upstream}'): the install forwards visitors there over the internet"
                ));
            }
        }
        Ok(())
    }
}

/// A domain name as a person types it, lower-cased and checked: labels of
/// letters, digits and hyphens joined by dots, at least two of them, no
/// label over 63 characters and the whole under 254. An IP address is
/// refused (the install already answers at its own).
pub fn normalize_domain_name(raw: &str) -> Result<String, String> {
    let name = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    check_domain_name(&name)?;
    Ok(name)
}

fn check_domain_name(name: &str) -> Result<(), String> {
    if name.parse::<std::net::IpAddr>().is_ok() {
        return Err(format!("'{name}' is an IP address; the install already answers at its own, a domain is a name"));
    }
    if name.is_empty() || name.len() > 253 {
        return Err(format!("'{name}' is not a domain name: it must be 1 to 253 characters"));
    }
    let labels: Vec<&str> = name.split('.').collect();
    if labels.len() < 2 {
        return Err(format!("'{name}' is not a domain name: it needs at least one dot (`app.example.com`)"));
    }
    for label in labels {
        let fine = !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !fine {
            return Err(format!(
                "'{name}' is not a domain name: '{label}' must be 1 to 63 lower-case letters, digits or inner hyphens"
            ));
        }
    }
    Ok(())
}

/// `GET /install/database`: whether the install's database held it back
/// lately: requests that waited for a connection because every one their
/// copy may hold was in use (`weft_task_store::throttle`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseLoad {
    /// One entry per role whose copies waited in the last quarter hour.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held_back: Vec<HeldBack>,
    /// What gives the install more connections, in a person's words, when
    /// it was held back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub more: Option<String>,
}

/// One role's stretch of waits for a database connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldBack {
    pub role: DatabaseHolder,
    pub since_unix: i64,
    pub last_unix: i64,
    /// The waits counted since, every copy of the role together.
    pub waits: u64,
    /// What the database said it takes.
    pub max_connections: u32,
    /// The connections one copy of the role may hold.
    pub share: u32,
}

crate::wire_enum! {
    /// Whose copies waited for a database connection: the processes that
    /// open the database. Stored in `database_throttle.role`
    /// (`weft_task_store::throttle`), which holds no other value.
    // SYNC: DatabaseHolder <-> crates/weft-task-store/src/throttle.rs (database_throttle's CHECK on role)
    pub enum DatabaseHolder {
        /// A local install's one process, which runs every role.
        Machine = "machine",
        /// The dispatcher in a process of its own.
        Dispatcher = "dispatcher",
        /// The broker in a process of its own.
        Broker = "broker",
    }
}

/// The weft CLI the install workflow stored for one source tree, as the
/// install answers it (`GET /install/cli/{tree}`): where to download it,
/// a link the store signs, and the sha256 of its bytes, in hex, which the
/// caller checks what it downloaded against.
// SYNC: InstallCli <-> scripts/release-cli.sh (from_install), crates/weft-dispatcher/src/api/install_cli.rs (the route)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallCli {
    pub url: String,
    pub sha256: String,
}

/// Whether `tree` is a git tree id: 40 hex digits (SHA-1) or 64 (SHA-256),
/// lowercase, as git prints one. What names a stored CLI, so nothing else
/// reaches the store's keys through it.
pub fn is_tree_id(tree: &str) -> bool {
    matches!(tree.len(), 40 | 64) && tree.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Where in the install's file store the CLI built from `tree` lives, and
/// the file beside it holding its sha256, which is written last, so its
/// being there is what says the CLI is whole.
// SYNC: install_cli_keys <-> .github/workflows/install-gcp.yml (store the weft CLI in the install)
pub fn install_cli_keys(tree: &str) -> (String, String) {
    (format!("install/cli/{tree}/weft"), format!("install/cli/{tree}/sha256"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tree_id_is_40_or_64_lowercase_hex_digits() {
        assert!(is_tree_id(&"a".repeat(40)));
        assert!(is_tree_id(&"0123456789abcdef".repeat(4)));
        assert!(!is_tree_id(&"A".repeat(40)), "uppercase");
        assert!(!is_tree_id(&"a".repeat(41)));
        assert!(!is_tree_id(&format!("{}/", "a".repeat(39))), "a path");
        assert!(!is_tree_id(""));
    }

    /// The answer the deploy's lookup reads (`.url`, `.sha256`).
    #[test]
    fn a_stored_cli_answers_the_fields_the_lookup_reads() {
        let answer = serde_json::to_value(InstallCli { url: "https://u".into(), sha256: "s".into() }).unwrap();
        assert_eq!(answer, serde_json::json!({ "url": "https://u", "sha256": "s" }));
        assert_eq!(serde_json::from_value::<InstallCli>(answer).unwrap(), InstallCli { url: "https://u".into(), sha256: "s".into() });
    }

    crate::wire_enum_roundtrip_tests!(DatabaseHolder);

    /// Only a `503` the install gave itself is asked again; the same
    /// status from the platform in front of it, and every other status
    /// the install gives, is the answer.
    #[test]
    fn only_the_installs_own_503_asks_again() {
        assert!(asks_again(503, true));
        assert!(!asks_again(503, false), "the platform's 503");
        for status in [200, 404, 409, 429, 500, 502, 504, 507] {
            assert!(!asks_again(status, true), "{status}");
        }
    }

    #[test]
    fn a_domain_name_is_normalized_and_checked() {
        assert_eq!(normalize_domain_name(" App.Example.COM. ").unwrap(), "app.example.com");
        assert_eq!(normalize_domain_name("xn--bcher-kva.example").unwrap(), "xn--bcher-kva.example");
        for bad in ["", "localhost", "a..b", "-a.b", "a-.b", "a_b.c", "34.1.2.3", "::1", &format!("{}.com", "a".repeat(64))] {
            assert!(normalize_domain_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_frontend_domain_forwards_only_over_https_and_round_trips() {
        let project = uuid::Uuid::nil();
        let front = Domain { name: "app.example.com".into(), serves: DomainServes::Frontend { project, upstream: "http://x.run.app".into() } };
        assert!(front.validate().unwrap_err().contains("https"));
        let front = Domain { name: "app.example.com".into(), serves: DomainServes::Frontend { project, upstream: "https://x.run.app".into() } };
        front.validate().unwrap();
        let v = serde_json::to_value(&front).unwrap();
        assert_eq!(v["for"]["kind"], "frontend");
        assert_eq!(serde_json::from_value::<Domain>(v).unwrap(), front);
        let install = Domain { name: "weft.example.com".into(), serves: DomainServes::Install };
        assert_eq!(serde_json::to_value(&install).unwrap(), serde_json::json!({ "name": "weft.example.com", "for": { "kind": "install" } }));
    }

    #[test]
    fn a_cloud_install_round_trips_and_a_local_one_is_small() {
        let gcp = InstallInfo {
            public_url: "https://weft.example.com".into(),
            service_url: "https://weft-x.a.run.app".into(),
            cloud: Some(CloudInstall::Gcp(GcpInstall {
                project: "p".into(),
                region: "us-central1".into(),
                artifact_registry: "us-central1-docker.pkg.dev/p/weft".into(),
                network: "weft".into(),
                subnet: "weft".into(),
                deployer_service_account: "weft-deployer@p.iam.gserviceaccount.com".into(),
                frontend_service_account: "weft-frontend@p.iam.gserviceaccount.com".into(),
                workload_identity_provider: "projects/1/locations/global/workloadIdentityPools/github/providers/github".into(),
            })),
            source: Some(WeftSource { repository: "me/weft".into(), commit: "abc123".into() }),
        };
        let v = serde_json::to_value(&gcp).unwrap();
        assert_eq!(v["cloud"]["provider"], "gcp");
        assert_eq!(v["cloud"]["subnet"], "weft");
        assert_eq!(v["source"]["repository"], "me/weft");
        assert_eq!(v["serviceUrl"], "https://weft-x.a.run.app");
        assert_eq!(serde_json::from_value::<InstallInfo>(v).unwrap(), gcp);
        let local = InstallInfo { public_url: "http://127.0.0.1:14112".into(), service_url: "http://127.0.0.1:14112".into(), cloud: None, source: None };
        assert_eq!(
            serde_json::to_value(&local).unwrap(),
            serde_json::json!({ "publicUrl": "http://127.0.0.1:14112", "serviceUrl": "http://127.0.0.1:14112" })
        );
        assert!(serde_json::from_value::<InstallInfo>(serde_json::json!({ "publicUrl": "x", "serviceUrl": "x", "typo": 1 })).is_err());
    }
}
