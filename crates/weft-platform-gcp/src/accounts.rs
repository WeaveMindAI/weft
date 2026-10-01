//! A project's own service account: what its workers and its infra
//! machines run as, so each can prove which project it is and nothing
//! more.

use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;

use crate::api::{is_status, Google};
use crate::names;

/// What a project's account is let into, beyond being itself. Each is
/// one binding on a resource every project shares, and an IAM policy
/// holds at most 1,500 principals, so a project's account gets only what
/// its own side of the platform uses, and `revoke_project_account`
/// takes every one back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// The caller-ticket secret, which a worker's Cloud Run service reads
    /// as its runtime account.
    CallerTokenSecret,
    /// The install's images, which an infra machine pulls as its own
    /// account. Cloud Run pulls a worker's image with its own service
    /// agent, never the runtime account, so workers need no grant here.
    ImageRegistry,
}

impl Access {
    const ALL: [Access; 2] = [Access::CallerTokenSecret, Access::ImageRegistry];

    /// The resource and the role this access is.
    fn binding(self, gcp: &GcpPlatform) -> anyhow::Result<(String, &'static str)> {
        Ok(match self {
            Access::CallerTokenSecret => (
                format!("https://secretmanager.googleapis.com/v1/projects/{}/secrets/{}", gcp.project, gcp.caller_token_secret),
                "roles/secretmanager.secretAccessor",
            ),
            Access::ImageRegistry => (
                format!("https://artifactregistry.googleapis.com/v1/{}", repository_resource(&gcp.artifact_registry)?),
                "roles/artifactregistry.reader",
            ),
        })
    }
}

/// Make sure `project`'s account exists and holds `access`. Answers its
/// email.
pub async fn ensure_project_account(google: &Google, gcp: &GcpPlatform, project: uuid::Uuid, access: Access) -> anyhow::Result<String> {
    let email = names::project_account_email(project, &gcp.project);
    let url = format!("https://iam.googleapis.com/v1/projects/{}/serviceAccounts/{email}", gcp.project);
    if google.get_opt(&url).await?.is_none() {
        let created = google
            .post(
                &format!("https://iam.googleapis.com/v1/projects/{}/serviceAccounts", gcp.project),
                &json!({
                    "accountId": names::project_account_id(project),
                    "serviceAccount": { "displayName": format!("weft project {project}") },
                }),
            )
            .await;
        match created {
            Ok(_) => {}
            Err(e) if is_status(&e, 409) => {}
            Err(e) => return Err(e.context(format!("create the service account of project {project}"))),
        }
    }
    let (resource, role) = access.binding(gcp)?;
    let member = format!("serviceAccount:{email}");
    // A policy refuses an account IAM has not spread yet ("does not exist").
    until_account_is_known(|| add_binding(google, &resource, role, &member)).await?;
    Ok(email)
}

/// Take back every `Access` of `project`'s account and delete it. The
/// bindings go first: a deleted account's bindings stay in a policy (as
/// `deleted:serviceAccount:...`) and keep counting toward its cap.
pub async fn revoke_project_account(google: &Google, gcp: &GcpPlatform, project: uuid::Uuid) -> anyhow::Result<()> {
    let email = names::project_account_email(project, &gcp.project);
    let member = format!("serviceAccount:{email}");
    for access in Access::ALL {
        let (resource, role) = access.binding(gcp)?;
        remove_binding(google, &resource, role, &member).await?;
    }
    google.delete(&format!("https://iam.googleapis.com/v1/projects/{}/serviceAccounts/{email}", gcp.project)).await?;
    Ok(())
}

/// How many times a call is tried while a new account spreads through
/// IAM, and how long between tries (doubling).
const SPREAD_TRIES: u32 = 6;
const SPREAD_FIRST_PAUSE: std::time::Duration = std::time::Duration::from_secs(2);

/// Whether `e` is Google refusing an account it has not heard of yet. A
/// new account, or a new grant to it, takes a while to reach every API,
/// and until then a call naming it fails in one of three known ways: the
/// account "does not exist" (or is "not found"), acting as it is denied
/// "(or it may not exist)", or a new revision's operation fails with
/// "Permission denied on secret" (its account not yet seeing the secret
/// it was just granted). Any other refusal, a 403 included, is real and
/// is not waited out.
fn account_not_spread(e: &anyhow::Error) -> bool {
    says_account_not_spread(&format!("{e:#}"))
}

/// Whether Google's `text` (an error, a failed condition's message) says
/// an account IAM has not spread yet.
pub(crate) fn says_account_not_spread(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    (text.contains("service account") && (text.contains("does not exist") || text.contains("not found")))
        || text.contains("or it may not exist")
        || text.contains("permission denied on secret")
}

/// Run `call` until it stops failing on an account IAM has not spread
/// yet. Bounded: an internal call, and past it the error is real.
pub async fn until_account_is_known<T, F, Fut>(call: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    until_settled(|_| false, call).await
}

/// Run `call` until it stops failing on an account IAM has not spread yet
/// or on what `also` names as settling on its own. One budget for both,
/// so the waits never multiply.
pub async fn until_settled<T, F, Fut>(also: impl Fn(&anyhow::Error) -> bool, mut call: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut pause = SPREAD_FIRST_PAUSE;
    for _ in 1..SPREAD_TRIES {
        match call().await {
            Err(e) if account_not_spread(&e) || also(&e) => {
                tokio::time::sleep(pause).await;
                pause *= 2;
            }
            done => return done,
        }
    }
    call().await
}

/// `projects/<p>/locations/<l>/repositories/<r>` from the registry's
/// address (`<l>-docker.pkg.dev/<p>/<r>`).
fn repository_resource(address: &str) -> anyhow::Result<String> {
    let mut parts = address.trim_end_matches('/').splitn(3, '/');
    let (Some(host), Some(project), Some(repo)) = (parts.next(), parts.next(), parts.next()) else {
        anyhow::bail!("artifactRegistry '{address}' is not <region>-docker.pkg.dev/<project>/<repository>");
    };
    let location = host
        .strip_suffix("-docker.pkg.dev")
        .ok_or_else(|| anyhow::anyhow!("artifactRegistry '{address}' is not an Artifact Registry address"))?;
    Ok(format!("projects/{project}/locations/{location}/repositories/{repo}"))
}

/// How many times a policy change is tried when someone else changed the
/// policy between our read and our write.
const POLICY_TRIES: u32 = 5;

/// Change the IAM policy of `resource` (anything that takes
/// `:getIamPolicy` and `:setIamPolicy`) with `edit`, which answers whether
/// it changed anything. The write carries the read's `etag`, so a policy
/// someone else changed in between is refused (409) instead of lost, and
/// the change is made again on the fresh policy.
async fn change_policy(google: &Google, resource: &str, edit: impl Fn(&mut Vec<Value>) -> anyhow::Result<bool>) -> anyhow::Result<()> {
    let mut tries = 0;
    loop {
        tries += 1;
        let mut policy = google.get(&format!("{resource}:getIamPolicy")).await?;
        let obj = policy.as_object_mut().ok_or_else(|| anyhow::anyhow!("an IAM policy that is not an object"))?;
        let list = obj.entry("bindings").or_insert_with(|| json!([]));
        let list = list.as_array_mut().ok_or_else(|| anyhow::anyhow!("IAM bindings that are not a list"))?;
        if !edit(list)? {
            return Ok(());
        }
        match google.post(&format!("{resource}:setIamPolicy"), &json!({ "policy": policy })).await {
            Err(e) if is_status(&e, 409) && tries < POLICY_TRIES => continue,
            done => return done.map(|_| ()),
        }
    }
}

/// Add `member` to `role` on `resource`, when it is not there yet.
pub async fn add_binding(google: &Google, resource: &str, role: &str, member: &str) -> anyhow::Result<()> {
    change_policy(google, resource, |list| {
        match list.iter_mut().find(|b| b.get("role").and_then(Value::as_str) == Some(role) && b.get("condition").is_none()) {
            Some(b) => {
                let members = b["members"].as_array_mut().ok_or_else(|| anyhow::anyhow!("an IAM binding with no members"))?;
                if members.iter().any(|m| m.as_str() == Some(member)) {
                    return Ok(false);
                }
                members.push(json!(member));
            }
            None => list.push(json!({ "role": role, "members": [member] })),
        }
        Ok(true)
    })
    .await
}

/// Take `member` out of `role` on `resource`, when it is there.
pub async fn remove_binding(google: &Google, resource: &str, role: &str, member: &str) -> anyhow::Result<()> {
    change_policy(google, resource, |list| {
        let mut changed = false;
        for b in list.iter_mut().filter(|b| b.get("role").and_then(Value::as_str) == Some(role)) {
            if let Some(members) = b.get_mut("members").and_then(Value::as_array_mut) {
                let before = members.len();
                members.retain(|m| m.as_str() != Some(member));
                changed |= members.len() != before;
            }
        }
        // A binding left with no members is refused by setIamPolicy.
        list.retain(|b| b.get("members").and_then(Value::as_array).is_some_and(|m| !m.is_empty()));
        Ok(changed)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ApiError;

    fn api(status: u16, body: &str) -> anyhow::Error {
        anyhow::Error::new(ApiError { status, body: body.into() }).context("POST x")
    }

    #[test]
    fn only_an_account_not_known_yet_is_waited_out() {
        assert!(account_not_spread(&api(400, "Service account wp-x@acme.iam.gserviceaccount.com does not exist.")));
        assert!(account_not_spread(&api(403, "Permission 'iam.serviceaccounts.actAs' denied on service account wp-x@acme.iam.gserviceaccount.com (or it may not exist).")));
        assert!(account_not_spread(&anyhow::anyhow!("the operation failed: {{\"message\": \"Permission denied on secret: projects/acme/secrets/s/versions/latest\"}}")));
        assert!(!account_not_spread(&api(403, "The caller does not have permission")));
        assert!(!account_not_spread(&api(403, "Permission 'run.services.update' denied on resource")));
        assert!(!account_not_spread(&api(400, "invalid machine type")));
        assert!(!account_not_spread(&api(409, "already exists")));
    }

    #[test]
    fn a_registry_address_names_its_repository() {
        assert_eq!(
            super::repository_resource("us-central1-docker.pkg.dev/acme/weft").unwrap(),
            "projects/acme/locations/us-central1/repositories/weft"
        );
    }
}
