//! What an infra unit's containers and volumes are called on a Docker
//! daemon, and what happens to a disk named before those names carried
//! the install.
//!
//! On a laptop every install shares the one Docker daemon, so every name
//! carries the install ([`Names::PerInstall`]): two installs running the
//! same project never share a container, a name on the network or a disk.
//! A cloud machine runs one unit ([`Names::Machine`]), and its agent's name
//! is the machine's own, the name the install's other machines reach it by.
//!
//! A disk named before ([`old_disk`]) holds somebody's data, so
//! [`plan_disk`] decides, from what the daemon shows, whether this install
//! carries it over to its own name, starts fresh, or refuses.

use std::collections::BTreeSet;
use std::path::PathBuf;

use weft_core::infra::{Install, NodeRef};

/// Whose names a daemon holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Names {
    /// Several installs share the daemon (a laptop). `claims` is where
    /// `weft infra take-disk` says which install takes an old disk
    /// (`MachineLayout::disk_claims`).
    PerInstall { claims: PathBuf },
    /// The daemon runs one unit of one install (a cloud machine).
    Machine,
}

/// What every name of `node`'s copy starts with.
pub fn base(names: &Names, install: &Install, node: &NodeRef) -> String {
    match names {
        Names::PerInstall { .. } => node.resource_base_within(Some(install.label_value())),
        Names::Machine => node.resource_base(),
    }
}

/// The unit's agent container: also the unit's name on the network.
pub fn agent(base: &str, unit: &str) -> String {
    format!("{base}-{unit}")
}

/// The volume-owning helper's name: beside the agent's, with a separator
/// no container a spec declares can carry.
pub fn own(base: &str, unit: &str) -> String {
    format!("{}.own", agent(base, unit))
}

pub fn container(base: &str, unit: &str, container: &str) -> String {
    format!("{base}-{unit}-{container}")
}

pub fn disk(base: &str, volume: &str) -> String {
    format!("{base}-{volume}")
}

pub fn scratch(base: &str, unit: &str, volume: &str) -> String {
    format!("{base}-{unit}-{volume}")
}

/// The name a laptop gave `node`'s disk `volume` before names carried the
/// install.
pub fn old_disk(node: &NodeRef, volume: &str) -> String {
    disk(&node.resource_base(), volume)
}

/// Whether `name` is one a laptop gave something of `node`'s copy before
/// names carried the install. Every such name starts with the plain base
/// and a `-`; a name carrying an install never does
/// (`NodeRef::resource_base_within`).
pub fn is_old_name(node: &NodeRef, name: &str) -> bool {
    name.starts_with(&format!("{}-", node.resource_base()))
}

/// Whose a volume is: its `weft-install` label, `None` when it has none.
pub type Owner = Option<String>;

/// What the daemon shows about one disk of a copy, for [`plan_disk`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskLook {
    /// This install's label (`Install::label_value`).
    pub install: String,
    /// The disk's name for this install, and whose it is when it exists.
    pub new_name: String,
    pub new: Option<Owner>,
    /// The disk's name from before ([`old_disk`]), and whose it is when it
    /// exists.
    pub old_name: String,
    pub old: Option<Owner>,
    /// Every install (this one included) that still holds something of
    /// this copy under the old names: a container, running or not, or
    /// another volume. Such an install ran the copy on the old disk.
    pub holders: BTreeSet<String>,
    /// The install `weft infra take-disk` named for the old disk.
    pub claimed_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskPlan {
    /// Make the disk under this install's name.
    Fresh,
    /// The disk under this install's name is there and this install's.
    Keep,
    /// Copy the old disk into this install's (emptied first: one already
    /// there is a carry-over cut short, since the old disk goes only
    /// once the copy is whole), then remove the old one.
    CarryOver,
    /// Start nothing, saying why and what to run.
    Refuse(String),
}

/// What to do with one disk before a unit of this install starts.
///
/// The old disk goes to this install only when nothing points anywhere
/// else: the disk's own label, every install still holding something of
/// the copy under the old names, and any `take-disk` claim. When two
/// installs could both claim it, nobody takes it until a person says
/// whose it is.
pub fn plan_disk(look: &DiskLook) -> DiskPlan {
    if let Some(owner) = &look.new {
        if owner.as_deref() != Some(look.install.as_str()) {
            let whose = match owner {
                Some(other) => format!("install '{other}'"),
                None => "no install (it has no weft-install label)".to_string(),
            };
            return DiskPlan::Refuse(format!(
                "the volume {} has this install's name for this disk but belongs to {whose}, and weft never starts \
                 a disk on another install's volume. If nothing needs what it holds, remove it with `docker volume \
                 rm {}` and start the infra again",
                look.new_name, look.new_name
            ));
        }
    }
    let ours_or_fresh = if look.new.is_some() { DiskPlan::Keep } else { DiskPlan::Fresh };
    let Some(old_owner) = &look.old else { return ours_or_fresh };
    match look.claimed_by.as_deref() {
        Some(claimed) if claimed == look.install => return DiskPlan::CarryOver,
        Some(_) => return ours_or_fresh,
        None => {}
    }
    let take = take_disk_command(&look.old_name);
    let Some(old_owner) = old_owner else {
        return DiskPlan::Refuse(format!(
            "the volume {} is this disk's name from before infra names carried the install, and it has no \
             weft-install label, so weft cannot tell whose data it holds. If it is this install's, run `{take}` \
             here and start the infra again; if nothing needs it, remove it with `docker volume rm {}`",
            look.old_name, look.old_name
        ));
    };
    let mut claimants = look.holders.clone();
    claimants.insert(old_owner.clone());
    if !claimants.contains(&look.install) {
        return ours_or_fresh;
    }
    if claimants.len() == 1 {
        return DiskPlan::CarryOver;
    }
    let names: Vec<String> = claimants.iter().map(|c| format!("'{c}'")).collect();
    DiskPlan::Refuse(format!(
        "the volume {} is this disk's name from before infra names carried the install, and installs {} all ran \
         this infra on it, so weft cannot tell whose data it holds. Run `{take}` with WEFT_INSTALL naming the \
         install whose data it is (`default` for the default install), then start the infra again: that install \
         carries it over, the others start this disk empty",
        look.old_name,
        names.join(", ")
    ))
}

/// The command that gives the old disk `volume` to the install it runs
/// under.
pub fn take_disk_command(volume: &str) -> String {
    format!("weft infra take-disk {volume}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(7), node: "db".into(), copy_id: "wn-1".into() }
    }

    fn look() -> DiskLook {
        DiskLook {
            install: "mine".into(),
            new_name: "new".into(),
            new: None,
            old_name: "old".into(),
            old: None,
            holders: BTreeSet::new(),
            claimed_by: None,
        }
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn two_installs_name_one_copy_apart_and_a_machine_keeps_the_plain_name() {
        let claims = Names::PerInstall { claims: "/claims".into() };
        let a = base(&claims, &Install::named("alpha").unwrap(), &node());
        let b = base(&claims, &Install::named("beta").unwrap(), &node());
        let default = base(&claims, &Install::default_install(), &node());
        assert_ne!(a, b);
        assert_ne!(a, default);
        assert_eq!(base(&Names::Machine, &Install::default_install(), &node()), node().resource_base());
        for name in [agent(&a, "main"), own(&a, "main"), container(&a, "main", "pg"), disk(&a, "data"), scratch(&a, "main", "sock")] {
            assert!(!is_old_name(&node(), &name), "{name}");
        }
        let plain = node().resource_base();
        for name in [agent(&plain, "main"), own(&plain, "main"), container(&plain, "main", "pg"), old_disk(&node(), "data")] {
            assert!(is_old_name(&node(), &name), "{name}");
        }
    }

    #[test]
    fn with_no_old_disk_a_disk_is_made_or_kept() {
        assert_eq!(plan_disk(&look()), DiskPlan::Fresh);
        assert_eq!(plan_disk(&DiskLook { new: Some(Some("mine".into())), ..look() }), DiskPlan::Keep);
    }

    #[test]
    fn a_volume_with_this_installs_name_that_is_not_its_own_is_refused() {
        for owner in [Some("other".to_string()), None] {
            let DiskPlan::Refuse(why) = plan_disk(&DiskLook { new: Some(owner), old: Some(Some("mine".into())), ..look() }) else {
                panic!("started on a volume that is not this install's")
            };
            assert!(why.contains("docker volume rm new"), "{why}");
        }
    }

    #[test]
    fn an_old_disk_only_this_install_ran_is_carried_over() {
        assert_eq!(plan_disk(&DiskLook { old: Some(Some("mine".into())), ..look() }), DiskPlan::CarryOver);
        assert_eq!(plan_disk(&DiskLook { old: Some(Some("mine".into())), holders: set(&["mine"]), ..look() }), DiskPlan::CarryOver);
        // Its own name there too: a carry-over cut short, done again.
        assert_eq!(
            plan_disk(&DiskLook { new: Some(Some("mine".into())), old: Some(Some("mine".into())), ..look() }),
            DiskPlan::CarryOver
        );
    }

    #[test]
    fn an_old_disk_this_install_never_ran_is_left_to_its_owner() {
        assert_eq!(plan_disk(&DiskLook { old: Some(Some("other".into())), ..look() }), DiskPlan::Fresh);
        assert_eq!(plan_disk(&DiskLook { old: Some(Some("other".into())), holders: set(&["third"]), ..look() }), DiskPlan::Fresh);
    }

    #[test]
    fn an_old_disk_two_installs_could_claim_is_refused_naming_the_command() {
        for (owner, holders) in [("mine", &["other"][..]), ("other", &["mine"][..]), ("mine", &["mine", "other"][..])] {
            let DiskPlan::Refuse(why) = plan_disk(&DiskLook { old: Some(Some(owner.into())), holders: set(holders), ..look() }) else {
                panic!("took a disk two installs ran ({owner}, {holders:?})")
            };
            assert!(why.contains("weft infra take-disk old") && why.contains("'mine', 'other'"), "{why}");
        }
    }

    #[test]
    fn an_unlabelled_old_disk_is_refused_until_somebody_takes_it() {
        let DiskPlan::Refuse(why) = plan_disk(&DiskLook { old: Some(None), holders: set(&["mine"]), ..look() }) else {
            panic!("took a disk nothing proves is this install's")
        };
        assert!(why.contains("weft infra take-disk old"), "{why}");
        assert_eq!(plan_disk(&DiskLook { old: Some(None), claimed_by: Some("mine".into()), ..look() }), DiskPlan::CarryOver);
    }

    #[test]
    fn a_take_disk_claim_settles_whose_the_old_disk_is() {
        let shared = DiskLook { old: Some(Some("mine".into())), holders: set(&["mine", "other"]), ..look() };
        assert_eq!(plan_disk(&DiskLook { claimed_by: Some("mine".into()), ..shared.clone() }), DiskPlan::CarryOver);
        assert_eq!(plan_disk(&DiskLook { claimed_by: Some("other".into()), ..shared.clone() }), DiskPlan::Fresh);
        assert_eq!(
            plan_disk(&DiskLook { claimed_by: Some("other".into()), new: Some(Some("mine".into())), ..shared }),
            DiskPlan::Keep
        );
    }
}
