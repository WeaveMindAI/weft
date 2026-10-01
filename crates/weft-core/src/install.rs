//! What an install tells about itself (`GET /install`): the addresses it
//! answers at and, on a cloud, the pieces a project's CI deploys a
//! frontend with. Derived from the install's config, read by `weft login`
//! (to prove a key) and `weft target export` (to hand a project's CI what
//! it needs). Also the domains an install answers at.

use serde::{Deserialize, Serialize};

// SYNC: InstallInfo <-> crates/weft-dispatcher/src/app.rs (install_info)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallInfo {
    /// Where people and providers reach the install
    /// (`https://weft.example.com`).
    #[serde(rename = "publicUrl")]
    pub public_url: String,
    /// Where a frontend running next to the install reaches it privately
    /// (the machine's private address on GCP). `None` on a local install,
    /// whose frontend runs on the same machine.
    #[serde(rename = "internalUrl", default, skip_serializing_if = "Option::is_none")]
    pub internal_url: Option<String>,
    /// The cloud the install runs on, `None` on a local one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud: Option<CloudInstall>,
    /// The weft source the install runs, so a project's CI builds the
    /// same CLI (a project version compiled by another weft is refused).
    /// `None` on a local install, whose CLI is the one beside it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<WeftSource>,
    /// The machine's static public address, which every domain's DNS
    /// record points at. `None` when the install has no front door of its
    /// own (a local install, which the internet reaches through a tunnel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<std::net::IpAddr>,
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

/// A stored domain and the DNS record that points it at the install:
/// what `GET /install/domains` lists and `POST /install/domains` answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainEntry {
    pub domain: Domain,
    pub record: DnsRecord,
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
                    "a frontend's address must be https (got '{upstream}'): the front door forwards visitors there over the internet"
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

#[cfg(test)]
mod tests {
    use super::*;

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
            internal_url: Some("http://10.10.0.5".into()),
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
            address: Some("34.1.2.3".parse().unwrap()),
        };
        let v = serde_json::to_value(&gcp).unwrap();
        assert_eq!(v["cloud"]["provider"], "gcp");
        assert_eq!(v["cloud"]["subnet"], "weft");
        assert_eq!(v["source"]["repository"], "me/weft");
        assert_eq!(v["address"], "34.1.2.3");
        assert_eq!(serde_json::from_value::<InstallInfo>(v).unwrap(), gcp);
        let local = InstallInfo { public_url: "http://127.0.0.1:14112".into(), internal_url: None, cloud: None, source: None, address: None };
        assert_eq!(serde_json::to_value(&local).unwrap(), serde_json::json!({ "publicUrl": "http://127.0.0.1:14112" }));
        assert!(serde_json::from_value::<InstallInfo>(serde_json::json!({ "publicUrl": "x", "typo": 1 })).is_err());
    }
}
