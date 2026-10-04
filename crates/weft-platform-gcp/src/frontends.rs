//! A project's frontend on Cloud Run: a service of its own, made empty
//! (a stand-in image until its repository's CI deploys it), public without
//! any IAM grant (`invokerIamDisabled`), running as the frontends' account,
//! which may do nothing.
//!
//! Its repository deploys as the install's one deployer account: weft lets
//! the repository act as that account (Workload Identity Federation, by
//! the repository GitHub vouches for) and lets the account change this
//! service. The account is shared by every frontend, so a repository let
//! in may deploy to any frontend's service, never to anything else: the
//! person adding frontends chose that, and the docs say it.

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{FrontendHosting, FrontendSite, HostedFrontend};

use crate::accounts::{add_binding, remove_binding};
use crate::api::{is_status, Google};

/// What runs on a frontend's service until its repository deploys it.
const STAND_IN_IMAGE: &str = "us-docker.pkg.dev/cloudrun/container/hello";

pub struct CloudRunFrontends {
    google: Google,
    gcp: GcpPlatform,
}

impl CloudRunFrontends {
    pub fn new(google: Google, gcp: GcpPlatform) -> Self {
        Self { google, gcp }
    }

    fn services(&self) -> String {
        format!("https://run.googleapis.com/v2/projects/{}/locations/{}/services", self.gcp.project, self.gcp.region)
    }

    fn deployer(&self) -> String {
        format!("https://iam.googleapis.com/v1/projects/{}/serviceAccounts/{}", self.gcp.project, self.gcp.deployer_service_account)
    }

    /// Every workflow run of `repo`, as Workload Identity Federation names
    /// it: by the id GitHub gave the repository, never its name, which
    /// somebody else can take once it is deleted or renamed.
    // SYNC: attribute.repository_id <-> deploy/terraform/gcp/identity.tf (attribute_mapping)
    fn repo_principal(&self, repo: &weft_core::frontend::Repository) -> anyhow::Result<String> {
        Ok(format!(
            "principalSet://iam.googleapis.com/{}/attribute.repository_id/{}",
            pool_of(&self.gcp.workload_identity_provider)?,
            repo.id
        ))
    }
}

/// The pool a provider belongs to (`projects/<n>/locations/global/workloadIdentityPools/<pool>`).
fn pool_of(provider: &str) -> anyhow::Result<&str> {
    provider
        .split_once("/providers/")
        .map(|(pool, _)| pool)
        .ok_or_else(|| anyhow::anyhow!("workloadIdentityProvider '{provider}' is not .../workloadIdentityPools/<pool>/providers/<id>"))
}

/// The service a project's frontend runs as: at most 49 characters, as
/// Cloud Run allows (16 of the project id, a 20-character name at most).
pub fn frontend_service(project: uuid::Uuid, name: &str) -> String {
    format!("fe-{}-{name}", &project.simple().to_string()[..16])
}

#[async_trait]
impl FrontendHosting for CloudRunFrontends {
    async fn open(&self, site: &FrontendSite) -> anyhow::Result<HostedFrontend> {
        let service = frontend_service(site.project, &site.name);
        let made = self
            .google
            .post(
                &format!("{}?serviceId={service}", self.services()),
                &json!({
                    "ingress": "INGRESS_TRAFFIC_ALL",
                    "invokerIamDisabled": true,
                    "labels": {
                        "weft-project": site.project.simple().to_string(),
                        "weft-frontend": site.name,
                    },
                    "template": {
                        "serviceAccount": self.gcp.frontend_service_account,
                        "containers": [{ "image": STAND_IN_IMAGE }],
                    },
                }),
            )
            .await;
        match made {
            Ok(op) => {
                self.google.wait("https://run.googleapis.com/v2", op).await?;
            }
            // Made before (an add that failed after it, run again): what
            // runs there is the repository's by now, and stays.
            Err(e) if is_status(&e, 409) => {}
            Err(e) => return Err(e.context(format!("make the Cloud Run service of frontend '{}'", site.name))),
        }
        let resource = format!("{}/{service}", self.services());
        add_binding(&self.google, &resource, "roles/run.developer", &format!("serviceAccount:{}", self.gcp.deployer_service_account))
            .await
            .map_err(|e| e.context(format!("let the deployer change {service}")))?;
        add_binding(&self.google, &self.deployer(), "roles/iam.workloadIdentityUser", &self.repo_principal(&site.repo)?)
            .await
            .map_err(|e| e.context(format!("let {} deploy as the install's deployer", site.repo.name)))?;
        let described = self.google.get(&resource).await?;
        let url = stable_address(&described, &self.gcp.region)
            .ok_or_else(|| anyhow::anyhow!("Cloud Run named no `{}.run.app` address for {service}", self.gcp.region))?;
        Ok(HostedFrontend { service, url })
    }

    async fn close(&self, site: &FrontendSite, keep_repo: bool) -> anyhow::Result<()> {
        let service = frontend_service(site.project, &site.name);
        if let Some(op) = self.google.delete(&format!("{}/{service}", self.services())).await? {
            self.google.wait("https://run.googleapis.com/v2", op).await?;
        }
        if !keep_repo {
            remove_binding(&self.google, &self.deployer(), "roles/iam.workloadIdentityUser", &self.repo_principal(&site.repo)?)
                .await
                .map_err(|e| e.context(format!("take back {}'s right to deploy", site.repo.name)))?;
        }
        Ok(())
    }
}

/// The address of the `<service>-<project number>.<region>.run.app` form
/// among those Cloud Run serves a service at. A service answers at two
/// (that one, and an older `-<hash>-<region code>.a.run.app` one Cloud Run
/// calls its main `uri`); the install's own address is of the first form,
/// so a frontend's is too, and a person sees one shape everywhere.
fn stable_address(described: &Value, region: &str) -> Option<String> {
    let suffix = format!(".{region}.run.app");
    described
        .get("urls")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .find(|url| url.ends_with(&suffix))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Of the two addresses Cloud Run serves a service at, the one of the
    /// install's own shape.
    #[test]
    fn a_frontend_is_reached_at_the_address_of_the_install_s_shape() {
        let described = serde_json::json!({
            "uri": "https://weft-front-abc123-uw.a.run.app",
            "urls": ["https://weft-front-123456.us-west1.run.app", "https://weft-front-abc123-uw.a.run.app"],
        });
        assert_eq!(stable_address(&described, "us-west1").as_deref(), Some("https://weft-front-123456.us-west1.run.app"));
        assert_eq!(stable_address(&serde_json::json!({ "uri": "https://x-uw.a.run.app" }), "us-west1"), None);
    }

    #[test]
    fn a_frontend_service_fits_cloud_run_and_names_its_project() {
        let p = uuid::Uuid::from_u128(u128::MAX);
        let s = frontend_service(p, "a-name-of-twenty-chr");
        assert!(s.len() <= 49, "{s}");
        assert!(s.starts_with("fe-ffffffffffffffff-"));
    }

    #[test]
    fn the_pool_is_read_off_its_provider() {
        assert_eq!(
            pool_of("projects/1/locations/global/workloadIdentityPools/weft-github/providers/github").unwrap(),
            "projects/1/locations/global/workloadIdentityPools/weft-github"
        );
        assert!(pool_of("nonsense").is_err());
    }
}
