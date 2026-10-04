//! A project's own service account: what its workers and its infra
//! machines run as, so each can prove which project it is and nothing
//! more.

use std::time::Duration;

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
    /// Writing to Cloud Logging, which an infra machine's guest agent
    /// does as its own account (its boot script's output among it). Only
    /// a project-wide grant carries it; the core may grant this one role
    /// and no other (identity.tf).
    Logging,
}

impl Access {
    const ALL: [Access; 3] = [Access::CallerTokenSecret, Access::ImageRegistry, Access::Logging];

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
            Access::Logging => {
                (format!("https://cloudresourcemanager.googleapis.com/v1/projects/{}", gcp.project), "roles/logging.logWriter")
            }
        })
    }
}

/// Make sure `project`'s account exists and holds every `access`.
/// Answers its email.
pub async fn ensure_project_account(google: &Google, gcp: &GcpPlatform, project: uuid::Uuid, access: &[Access]) -> anyhow::Result<String> {
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
    let member = format!("serviceAccount:{email}");
    for access in access {
        let (resource, role) = access.binding(gcp)?;
        // A policy refuses an account IAM has not spread yet ("does not exist").
        until_account_is_known(|| add_binding(google, &resource, role, &member)).await?;
    }
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

/// How long a call waits for Google to apply an IAM change before its
/// refusal is taken as real: the pause between tries starts at `first`
/// and doubles up to `longest`, and the pauses add up to `budget` at most.
pub struct Patience {
    first: Duration,
    longest: Duration,
    budget: Duration,
}

/// A new account reaching every API: seconds, usually.
const ACCOUNT_SPREAD: Patience =
    Patience { first: Duration::from_secs(2), longest: Duration::from_secs(32), budget: Duration::from_secs(62) };

/// A new grant taking effect. Google applies most within two minutes and
/// some take seven or more.
pub const GRANT_APPLIES: Patience =
    Patience { first: Duration::from_secs(5), longest: Duration::from_secs(30), budget: Duration::from_secs(600) };

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
pub async fn until_settled<T, F, Fut>(also: impl Fn(&anyhow::Error) -> bool, call: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    until_settled_within(&ACCOUNT_SPREAD, also, call).await
}

/// [`until_settled`] with the patience a change needs, for a change that
/// takes longer to apply than a new account does to spread (a new grant:
/// [`GRANT_APPLIES`]). Each wait is logged with the refusal that caused
/// it, so a call held up here says why.
pub async fn until_settled_within<T, F, Fut>(
    patience: &Patience,
    also: impl Fn(&anyhow::Error) -> bool,
    mut call: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let (mut pause, mut waited) = (patience.first, Duration::ZERO);
    while waited + pause <= patience.budget {
        match call().await {
            Err(e) if account_not_spread(&e) || also(&e) => {
                tracing::info!(
                    target: "weft_platform_gcp::accounts",
                    error = format!("{e:#}"),
                    "Google has not applied a recent IAM change yet; asking again in {}s",
                    pause.as_secs()
                );
                tokio::time::sleep(pause).await;
                waited += pause;
                pause = (pause * 2).min(patience.longest);
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
        // Read and written at version 3, the one that keeps a binding's
        // condition: a project's policy carries conditional bindings (the
        // core's own grant to add log writers), and a version-1 write of
        // a policy holding one is refused.
        let mut policy = if reads_policy_with_post(resource) {
            google.post(&format!("{resource}:getIamPolicy"), &json!({ "options": { "requestedPolicyVersion": 3 } })).await?
        } else {
            google.get_query(&format!("{resource}:getIamPolicy"), &[("options.requestedPolicyVersion", "3".into())]).await?
        };
        let obj = policy.as_object_mut().ok_or_else(|| anyhow::anyhow!("an IAM policy that is not an object"))?;
        obj.insert("version".into(), json!(3));
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

/// A project's and a service account's policy are read with a POST, every
/// other resource's with a GET.
fn reads_policy_with_post(resource: &str) -> bool {
    resource.starts_with("https://cloudresourcemanager.googleapis.com/") || resource.starts_with("https://iam.googleapis.com/")
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

    /// A refusal `also` names is asked again until it stops, or until the
    /// pauses would pass the budget, and then the last answer stands; any
    /// other refusal is returned at once.
    #[tokio::test]
    async fn a_settling_refusal_is_asked_again_within_its_budget() {
        let ms = Duration::from_millis;
        let patience = Patience { first: ms(1), longest: ms(2), budget: ms(5) };
        let calls = std::sync::atomic::AtomicU32::new(0);
        let refusing = |until: u32| {
            let calls = &calls;
            move || {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                async move { if n < until { Err(api(403, "denied")) } else { Ok(n) } }
            }
        };
        let settles = |e: &anyhow::Error| is_status(e, 403);

        // Pauses of 1, 2 and 2 ms fit the 5 ms budget: three asks, then a
        // fourth that answers.
        assert_eq!(until_settled_within(&patience, settles, refusing(4)).await.unwrap(), 4);

        // Past the budget the fifth answer, still a refusal, stands.
        calls.store(0, std::sync::atomic::Ordering::Relaxed);
        assert!(is_status(&until_settled_within(&patience, settles, refusing(9)).await.unwrap_err(), 403));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 4);

        // A refusal nobody said settles is final on the first ask.
        calls.store(0, std::sync::atomic::Ordering::Relaxed);
        assert!(until_settled_within(&patience, |_| false, refusing(9)).await.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
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
