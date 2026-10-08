//! What weft names things on Google Cloud, and how a name leads back to
//! what it is for. Pure: the same inputs, the same names.

use sha2::{Digest, Sha256};

/// Eight hex characters naming an image (a label value cannot hold the
/// `/` and `:` a reference has).
pub fn image_hash(image: &str) -> String {
    Sha256::digest(image.as_bytes()).iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// How many hex characters of a project id its workers' service name
/// holds: short enough that a tagged revision's address
/// (`<tag>---<service>-<project number>.<region>.run.app`) fits one DNS
/// label, long enough that two projects of one install never meet.
const SERVICE_PROJECT_CHARS: usize = 20;

/// The Cloud Run service running every one of `project`'s workers: one
/// revision per program and settings, each reached at its tag's address,
/// and the project's callers sent to its front's.
pub fn worker_service(project: uuid::Uuid) -> String {
    format!("wk-{}", &project.simple().to_string()[..SERVICE_PROJECT_CHARS])
}

/// The tag of the revision running `image` with `settings` (a digest of
/// them, `settings_digest`): what names its revisions and its own address
/// on the project's service. At most 13 characters.
pub fn worker_tag(image: &str, settings_digest: &str) -> String {
    format!("v{}{}", image_hash(image), &settings_digest[..4.min(settings_digest.len())])
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
    fn service_names_and_tags_fit_cloud_run() {
        let s = worker_service(uuid::Uuid::from_u128(u128::MAX));
        let t = worker_tag("us-docker.pkg.dev/p/r/weft-worker:abc", "0123abcd");
        // `<tag>---<service>-<12-digit project number>` is one DNS label.
        assert!(t.len() + 3 + s.len() + 1 + 12 <= 63, "{t}---{s}");
        for name in [&s, &t] {
            assert!(name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'), "{name}");
        }
        assert_ne!(t, worker_tag("us-docker.pkg.dev/p/r/weft-worker:abd", "0123abcd"));
        assert_ne!(t, worker_tag("us-docker.pkg.dev/p/r/weft-worker:abc", "9999abcd"));
    }
}
