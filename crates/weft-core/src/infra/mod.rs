//! Infra: what an infra node asks for, checked and resolved the same way
//! for every platform.
//!
//! - `types`: the spec a node's `provision_infra` returns.
//! - `resolve`: check a spec and resolve its images, pure, one hash per
//!   unit (what an apply compares to decide what to replace).
//! - `status`: the lifecycle state of one infra node copy.
//! - `install`: which install a process belongs to.
//! - `wire`: what the install's infra endpoints answer (doors, logs).
//!
//! The dispatcher never runs infra: it routes lifecycle commands, and the
//! project's supervisor applies them through the platform's `InfraHost`.

mod install;
pub mod resolve;
mod status;
pub mod types;
pub mod wire;

pub use install::{data_dir, Install, INSTALL_ENV, INSTALL_LABEL, MAX_INSTALL_NAME};
pub use resolve::{
    public_path, public_url, resolve, resolve_image, unit_image_refs, NodeRef, ResolveError, ResolvedNode,
    ResolvedUnit,
};
pub use status::InfraNodeStatus;
pub use types::*;

/// A string as a resource-name segment may hold it: lowercase letters and
/// digits, runs of anything else collapsed to one `-`, none at either end.
/// The caller bounds the length.
pub fn name_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for c in s.chars() {
        let lc = c.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            out.push(lc);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_segment_keeps_only_what_a_name_may_hold() {
        assert_eq!(name_segment("Foo_Bar-123"), "foo-bar-123");
        assert_eq!(name_segment("a/b/c"), "a-b-c");
        assert_eq!(name_segment("--leading--"), "leading");
        assert_eq!(name_segment("@src:setup.store"), "src-setup-store");
    }
}
