//! What weft names things on Google Cloud, and how a name leads back to
//! what it is for. Pure: the same inputs, the same names.

use sha2::{Digest, Sha256};

/// Eight hex characters naming an image (a label value cannot hold the
/// `/` and `:` a reference has).
pub fn image_hash(image: &str) -> String {
    Sha256::digest(image.as_bytes()).iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// The Cloud Run service running `project`'s workers on `image`
/// (at most 49 characters, as Cloud Run allows).
pub fn worker_service(project: uuid::Uuid, image: &str) -> String {
    format!("wk-{}-{}", project.simple(), image_hash(image))
}

/// The Cloud Run job a long run of `project` on `image` executes as.
pub fn worker_job(project: uuid::Uuid, image: &str) -> String {
    format!("wj-{}-{}", project.simple(), image_hash(image))
}

/// How many hex characters of a project id its service account's name
/// holds: an account id is at most 30 characters, three of them the
/// prefix.
const ACCOUNT_PROJECT_CHARS: usize = 27;

/// The id of the service account `project`'s workers run as.
pub fn project_account_id(project: uuid::Uuid) -> String {
    format!("wp-{}", &project.simple().to_string()[..ACCOUNT_PROJECT_CHARS])
}

pub fn project_account_email(project: uuid::Uuid, gcp_project: &str) -> String {
    format!("{}@{gcp_project}.iam.gserviceaccount.com", project_account_id(project))
}

/// The start of the project id a project's service account names (the
/// first 27 hex characters of its id), or `None` for any other account.
pub fn project_prefix_of(email: &str, gcp_project: &str) -> Option<String> {
    let local = email.strip_suffix(&format!("@{gcp_project}.iam.gserviceaccount.com"))?;
    let hex = local.strip_prefix("wp-")?;
    (hex.len() == ACCOUNT_PROJECT_CHARS && hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()))
        .then(|| hex.to_string())
}

/// The Compute Engine machine running `unit` of the copy whose resource
/// base is `base` (at most 63 characters).
pub fn unit_machine(base: &str, unit: &str) -> String {
    format!("{base}-{unit}")
}

/// A persistent disk of the copy whose resource base is `base`.
pub fn unit_disk(base: &str, volume: &str) -> String {
    format!("{base}-d-{volume}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_project_account_leads_back_to_its_project() {
        let p = uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let id = project_account_id(p);
        assert!(id.len() <= 30, "a project account id fits GCP's 30 characters");
        let email = project_account_email(p, "acme");
        let prefix = project_prefix_of(&email, "acme").unwrap();
        assert!(p.simple().to_string().starts_with(&prefix));
        assert_eq!(project_prefix_of("weft-core@acme.iam.gserviceaccount.com", "acme"), None);
        assert_eq!(project_prefix_of(&email, "other"), None);
    }

    #[test]
    fn service_names_fit_cloud_run() {
        let s = worker_service(uuid::Uuid::from_u128(u128::MAX), "us-docker.pkg.dev/p/r/weft-worker:abc");
        assert!(s.len() <= 49, "{s}");
        assert!(s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert_ne!(s, worker_service(uuid::Uuid::from_u128(u128::MAX), "us-docker.pkg.dev/p/r/weft-worker:abd"));
    }
}
