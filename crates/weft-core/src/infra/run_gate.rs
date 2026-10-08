//! THE infra gate on a run: which copies of which infra nodes a run reads,
//! and which of them are not running. The dispatcher's run doors and a
//! worker's door both ask it, each over the copies as it holds them.

/// One copy of an infra node, whether it is running, and what it saved
/// for its baked outputs: what the gate and a run's birth read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InfraCopyUp {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    pub running: bool,
    /// Its baked outputs (`crate::infra::bake`), port to value; empty when
    /// it bakes nothing or has saved nothing yet.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub baked: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Which copy of which infra place a run or a trigger reads, the places
/// narrowed to `within` (see [`missing_infra_nodes`]): each place spelled
/// (an infra node inside a file included twice is two places with two
/// rows, and `within` names places the same way), with its copy: the
/// shared one (`Some(None)`), `instance`'s for a per-instance place, or
/// `None` for a per-instance place with no instance named, which has no
/// copy to look at.
pub fn copies_read<'a>(
    project: &crate::ProjectDefinition,
    within: Option<&std::collections::HashSet<String>>,
    instance: Option<&'a crate::instance::InstanceId>,
) -> Vec<(String, Option<Option<&'a crate::instance::InstanceId>>)> {
    let mut wanted = Vec::new();
    for place in crate::project::infra_places(project) {
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        if within.is_some_and(|set| !set.contains(&spelled)) {
            continue;
        }
        let per_instance = crate::project::is_per_instance(project, &place.id);
        // Nobody named, so there is no copy to look at: activate's note
        // over the whole project lands here (it leaves these out), and a
        // run never does ([`require_run_infra`] refuses it first).
        let copy = if per_instance { instance.map(Some) } else { Some(None) };
        wanted.push((spelled, copy));
    }
    wanted
}

/// One infra copy a run or a trigger needs and that is not running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingCopy {
    /// The node, spelled.
    pub place: String,
    /// Whose copy: `None` for the shared one.
    pub instance: Option<crate::instance::InstanceId>,
    /// The node exists once per instance and no instance was named.
    pub needs_instance: bool,
}

impl std::fmt::Display for MissingCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.instance, self.needs_instance) {
            (_, true) => write!(f, "{} (it exists once per instance, and no instance was named)", self.place),
            (Some(instance), _) => write!(f, "{} (instance '{instance}')", self.place),
            (None, _) => f.write_str(&self.place),
        }
    }
}

/// The copies spelled for a message, and how to bring them up: the
/// shared ones with `weft infra start`, an instance's with `--instance` (or the
/// program's own `ctx.infra(..).instance(..).start()`).
pub fn missing_and_fix(missing: &[MissingCopy]) -> (String, String) {
    let listed = missing.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
    let mut fixes: Vec<String> = Vec::new();
    if missing.iter().any(|m| m.instance.is_none()) {
        fixes.push("`weft infra start`".to_string());
    }
    let mut instances: Vec<&crate::instance::InstanceId> = missing.iter().filter_map(|m| m.instance.as_ref()).collect();
    instances.sort();
    instances.dedup();
    for instance in instances {
        fixes.push(format!(
            "`weft infra start --instance {instance}` (or your program's ctx.infra(..).instance(\"{instance}\").start())"
        ));
    }
    (listed, fixes.join(" and "))
}

/// Why a run cannot start while infra it reads is down, and how to bring
/// it up.
pub fn infra_not_running(missing: &[MissingCopy]) -> String {
    let (listed, fix) = missing_and_fix(missing);
    format!("infra not running for: {listed}. Run {fix} first.")
}

/// The copies of `wanted` ([`copies_read`]) that are not running among
/// `copies`: a per-instance place with no instance named is missing too,
/// with nothing to look at.
pub fn missing_copies(
    wanted: &[(String, Option<Option<&crate::instance::InstanceId>>)],
    copies: &[InfraCopyUp],
) -> Vec<MissingCopy> {
    let mut missing = Vec::new();
    for (place, copy) in wanted {
        let Some(copy) = copy else {
            missing.push(MissingCopy { place: place.clone(), instance: None, needs_instance: true });
            continue;
        };
        let running = copies.iter().any(|row| &row.node_id == place && row.instance.as_ref() == *copy && row.running);
        if !running {
            missing.push(MissingCopy { place: place.clone(), instance: copy.cloned(), needs_instance: false });
        }
    }
    missing
}
