//! A project's frontends: the websites that call the install on behalf of
//! their visitors (`weft frontend add|ls|rm`).
//!
//! A frontend is a named caller of one project: the install keeps a caller
//! token for it, scoped to that project, and nothing else about what it
//! is. Where it runs is a separate choice ([`FrontendHost`]): a service the
//! install makes on its own cloud and lets the frontend's repository
//! deploy to, or somewhere else entirely, which only needs the token and
//! the install's address.

use serde::{Deserialize, Serialize};

/// Where a frontend runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendHost {
    /// A container on the install's cloud (Cloud Run on GCP), made by the
    /// install, which its repository's CI deploys to.
    CloudRun,
    /// Anywhere else (Vercel, a server of your own): the install makes
    /// nothing, and the frontend calls it with its token.
    External,
}

impl FrontendHost {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CloudRun => "cloud_run",
            Self::External => "external",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "cloud_run" => Some(Self::CloudRun),
            "external" => Some(Self::External),
            _ => None,
        }
    }
}

/// One frontend of a project, as the install keeps it.
// SYNC: Frontend <-> crates/weft-dispatcher/src/frontends.rs (project_frontend DDL)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontend {
    pub name: String,
    pub project: uuid::Uuid,
    pub host: FrontendHost,
    /// The GitHub repository whose CI deploys it, for a frontend the
    /// install hosts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<Repository>,
    /// The service it runs as on the install's cloud, for one the install
    /// hosts: what the repository's CI deploys to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Where visitors reach it, once the install knows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The caller token it calls the install with (its id; the value is
    /// shown once, when it is made), once one is in place. A frontend the
    /// install hosts has none until its deploy workflow puts the first in
    /// place: the workflow's is the only token it ever calls with.
    #[serde(default, rename = "tokenId", skip_serializing_if = "Option::is_none")]
    pub token_id: Option<uuid::Uuid>,
    /// New tokens made to replace it and not put in place yet: they all
    /// work until one is (`POST .../token/{id}/done`), so a running site is
    /// never left without a working token.
    #[serde(default, rename = "pendingTokenIds", skip_serializing_if = "Vec::is_empty")]
    pub pending_token_ids: Vec<uuid::Uuid>,
}

/// A GitHub repository: the name a person knows it by, and the id GitHub
/// gave it, which never moves to another repository. Access is granted to
/// the id, so a name deleted and taken again by somebody else gets nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repository {
    /// `owner/name`.
    pub name: String,
    pub id: u64,
}

/// `POST /projects/{id}/frontends`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFrontendRequest {
    pub name: String,
    pub host: FrontendHost,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<Repository>,
}

/// What making a frontend answers: the frontend, and, for one running
/// elsewhere, the token it calls with, shown this once. One the install
/// hosts gets its token from its deploy workflow (`weft target export`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddedFrontend {
    pub frontend: Frontend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// What giving a frontend a new token answers: the frontend and the
/// token, shown this once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendWithToken {
    pub frontend: Frontend,
    pub token: String,
    /// The id of `token`: what puts it in place, or drops it.
    #[serde(rename = "tokenId")]
    pub token_id: uuid::Uuid,
}

impl AddFrontendRequest {
    /// Refuse what no install could make, naming the setting.
    pub fn validate(&self) -> Result<(), String> {
        check_name(&self.name)?;
        match (self.host, &self.repo) {
            (FrontendHost::CloudRun, None) => Err(format!(
                "frontend '{}' runs on the install's cloud, so it needs the GitHub repository that deploys it (--repo owner/name)",
                self.name
            )),
            (FrontendHost::CloudRun, Some(repo)) => check_repo(&repo.name),
            (FrontendHost::External, Some(_)) => Err(format!(
                "frontend '{}' runs elsewhere, so the install deploys nothing for it and takes no repository; leave out --repo",
                self.name
            )),
            (FrontendHost::External, None) => Ok(()),
        }
    }
}

/// A frontend's name: what it is called by in commands and in its
/// service's name, so a short DNS label.
pub fn check_name(name: &str) -> Result<(), String> {
    let fine = !name.is_empty()
        && name.len() <= 20
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && !name.ends_with('-')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if fine {
        Ok(())
    } else {
        Err(format!(
            "'{name}' is not a frontend name: 1 to 20 lower-case letters, digits or inner hyphens, starting with a letter"
        ))
    }
}

/// A GitHub repository as `owner/name`.
pub fn check_repo(repo: &str) -> Result<(), String> {
    let mut parts = repo.split('/');
    let fine = |p: Option<&str>| {
        p.is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')))
    };
    if fine(parts.next()) && fine(parts.next()) && parts.next().is_none() {
        Ok(())
    } else {
        Err(format!("'{repo}' is not a GitHub repository: it is owner/name"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(host: FrontendHost, repo: Option<&str>) -> AddFrontendRequest {
        AddFrontendRequest { name: "shop".into(), host, repo: repo.map(|name| Repository { name: name.into(), id: 7 }) }
    }

    #[test]
    fn a_hosted_frontend_names_its_repository_and_an_outside_one_does_not() {
        assert!(ask(FrontendHost::CloudRun, Some("me/shop")).validate().is_ok());
        assert!(ask(FrontendHost::CloudRun, None).validate().unwrap_err().contains("--repo"));
        assert!(ask(FrontendHost::CloudRun, Some("shop")).validate().unwrap_err().contains("owner/name"));
        assert!(ask(FrontendHost::External, None).validate().is_ok());
        assert!(ask(FrontendHost::External, Some("me/shop")).validate().unwrap_err().contains("leave out --repo"));
    }

    #[test]
    fn a_name_is_a_short_label() {
        assert!(check_name("shop-2").is_ok());
        for bad in ["", "Shop", "2shop", "shop-", "a-name-far-too-long-for-it", "sh op"] {
            assert!(check_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_host_reads_its_one_spelling_and_round_trips() {
        assert_eq!(FrontendHost::parse("cloudrun"), None);
        assert_eq!(FrontendHost::parse(FrontendHost::External.as_str()), Some(FrontendHost::External));
        assert_eq!(serde_json::to_value(FrontendHost::CloudRun).unwrap(), "cloud_run");
    }
}
