//! A program's wiring, indexed once for the walks over it.
//!
//! Every walk over a program (carving a run, finding every place a node
//! runs, a rule asking what a run reaches) asks the same questions many
//! times: which node has this id, which wires go into it, what kind of
//! group this is. Answered by scanning the node and edge lists each time,
//! a walk is quadratic in the program's size. [`ProjectGraph`] answers
//! each in constant time, from maps built in one pass.
//!
//! It borrows the program, so the program cannot change while an index
//! over it exists: build one where the walks start, and let it go with
//! them.

use std::collections::HashMap;

use super::{boundary_in_id, boundary_out_id, Edge, GroupDefinition, GroupKind, NodeDefinition, ProjectDefinition};

pub struct ProjectGraph<'a> {
    project: &'a ProjectDefinition,
    nodes: HashMap<&'a str, &'a NodeDefinition>,
    groups: HashMap<&'a str, &'a GroupDefinition>,
    edges: HashMap<&'a str, &'a Edge>,
    into: HashMap<&'a str, Vec<&'a Edge>>,
    out_of: HashMap<&'a str, Vec<&'a Edge>>,
    /// Per group: its members and its two boundaries, in the program's
    /// node order.
    members: HashMap<&'a str, Vec<&'a NodeDefinition>>,
    /// Per group: the call sites among its members, the group itself
    /// when it is one, in the program's group order.
    sites: HashMap<&'a str, Vec<&'a GroupDefinition>>,
}

impl<'a> ProjectGraph<'a> {
    pub fn new(project: &'a ProjectDefinition) -> Self {
        let mut nodes = HashMap::with_capacity(project.nodes.len());
        for node in &project.nodes {
            nodes.entry(node.id.as_str()).or_insert(node);
        }
        let mut groups = HashMap::with_capacity(project.groups.len());
        for group in &project.groups {
            groups.entry(group.id.as_str()).or_insert(group);
        }
        let mut edges = HashMap::with_capacity(project.edges.len());
        let mut into: HashMap<&str, Vec<&Edge>> = HashMap::new();
        let mut out_of: HashMap<&str, Vec<&Edge>> = HashMap::new();
        for edge in &project.edges {
            edges.entry(edge.id.as_str()).or_insert(edge);
            into.entry(edge.target.as_str()).or_default().push(edge);
            out_of.entry(edge.source.as_str()).or_default().push(edge);
        }
        // Indices first, so a node in a group's scope that is also one of
        // its boundaries is listed once, at its place in the node order.
        let mut member_indices: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, node) in project.nodes.iter().enumerate() {
            for group in &node.scope {
                member_indices.entry(group.as_str()).or_default().push(index);
            }
        }
        let position: HashMap<&str, usize> =
            project.nodes.iter().enumerate().rev().map(|(index, node)| (node.id.as_str(), index)).collect();
        for group in &project.groups {
            for boundary in [boundary_in_id(&group.id), boundary_out_id(&group.id)] {
                if let Some(index) = position.get(boundary.as_str()) {
                    member_indices.entry(group.id.as_str()).or_default().push(*index);
                }
            }
        }
        let members = member_indices
            .into_iter()
            .map(|(group, mut indices)| {
                indices.sort_unstable();
                indices.dedup();
                (group, indices.into_iter().map(|index| &project.nodes[index]).collect())
            })
            .collect();
        let mut site_indices: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, site) in project.groups.iter().enumerate().filter(|(_, g)| matches!(g.kind, GroupKind::Call { .. })) {
            site_indices.entry(site.id.as_str()).or_default().push(index);
            // Every node with the entry's id counts, as a scan would.
            for entry in project.nodes.iter().filter(|n| n.id == boundary_in_id(&site.id)) {
                for group in &entry.scope {
                    site_indices.entry(group.as_str()).or_default().push(index);
                }
            }
        }
        let sites = site_indices
            .into_iter()
            .map(|(group, mut indices)| {
                indices.sort_unstable();
                indices.dedup();
                (group, indices.into_iter().map(|index| &project.groups[index]).collect())
            })
            .collect();
        Self { project, nodes, groups, edges, into, out_of, members, sites }
    }

}

/// The lookups a walk over a program makes. [`ProjectGraph`] answers
/// each from its index; a bare [`ProjectDefinition`] answers by scanning,
/// which is right for a single lookup (building an index would cost more)
/// and wrong for a walk.
pub trait GraphView {
    fn project(&self) -> &ProjectDefinition;
    /// The node with this id (the first, as a scan would find).
    fn node(&self, id: &str) -> Option<&NodeDefinition>;
    /// The group with this id (the first, as a scan would find).
    fn group(&self, id: &str) -> Option<&GroupDefinition>;
    /// The wire with this id.
    fn edge(&self, id: &str) -> Option<&Edge>;
    /// The wires into the node `id`, in the program's order.
    fn edges_into(&self, id: &str) -> impl Iterator<Item = &Edge>;
    /// The wires out of the node `id`, in the program's order.
    fn edges_out_of(&self, id: &str) -> impl Iterator<Item = &Edge>;
    /// A group's members and its two boundaries, in the program's order.
    fn members(&self, group: &str) -> impl Iterator<Item = &NodeDefinition>;
    /// The call sites among a group's members, and the group itself when
    /// it is one, in the program's order.
    fn sites(&self, group: &str) -> impl Iterator<Item = &GroupDefinition>;

    fn is_loop(&self, group: &str) -> bool {
        self.group(group).is_some_and(|g| matches!(g.kind, GroupKind::Loop { .. }))
    }

    fn is_trigger(&self, id: &str) -> bool {
        self.node(id).is_some_and(|n| n.features.is_trigger)
    }

    /// The shared body a call site's group stands for, when `group` is one.
    fn body_of(&self, group: &str) -> Option<&str> {
        self.group(group).and_then(|g| match &g.kind {
            GroupKind::Call { body } => Some(body.as_str()),
            _ => None,
        })
    }

    /// Whether `id` is a boundary of a group, a call site or a body: one
    /// whose ports are walked one at a time.
    fn is_ordinary_boundary(&self, id: &str) -> bool {
        self.node(id).and_then(|n| n.group_boundary.as_ref()).is_some_and(|b| {
            self.group(&b.group_id).is_some_and(|g| matches!(g.kind, GroupKind::Group | GroupKind::Call { .. } | GroupKind::Body))
        })
    }
}

impl GraphView for ProjectGraph<'_> {
    fn project(&self) -> &ProjectDefinition {
        self.project
    }
    fn node(&self, id: &str) -> Option<&NodeDefinition> {
        self.nodes.get(id).copied()
    }
    fn group(&self, id: &str) -> Option<&GroupDefinition> {
        self.groups.get(id).copied()
    }
    fn edge(&self, id: &str) -> Option<&Edge> {
        self.edges.get(id).copied()
    }
    fn edges_into(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.into.get(id).into_iter().flatten().copied()
    }
    fn edges_out_of(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.out_of.get(id).into_iter().flatten().copied()
    }
    fn members(&self, group: &str) -> impl Iterator<Item = &NodeDefinition> {
        self.members.get(group).into_iter().flatten().copied()
    }
    fn sites(&self, group: &str) -> impl Iterator<Item = &GroupDefinition> {
        self.sites.get(group).into_iter().flatten().copied()
    }
}

impl GraphView for ProjectDefinition {
    fn project(&self) -> &ProjectDefinition {
        self
    }
    fn node(&self, id: &str) -> Option<&NodeDefinition> {
        self.nodes.iter().find(|n| n.id == id)
    }
    fn group(&self, id: &str) -> Option<&GroupDefinition> {
        self.groups.iter().find(|g| g.id == id)
    }
    fn edge(&self, id: &str) -> Option<&Edge> {
        self.edges.iter().find(|e| e.id == id)
    }
    fn edges_into(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.target == id)
    }
    fn edges_out_of(&self, id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.source == id)
    }
    fn members(&self, group: &str) -> impl Iterator<Item = &NodeDefinition> {
        let (entry, exit) = (boundary_in_id(group), boundary_out_id(group));
        self.nodes.iter().filter(move |n| n.scope.iter().any(|g| g == group) || n.id == entry || n.id == exit)
    }
    fn sites(&self, group: &str) -> impl Iterator<Item = &GroupDefinition> {
        self.groups.iter().filter(|site| matches!(site.kind, GroupKind::Call { .. })).filter(move |site| {
            site.id == group || self.nodes.iter().any(|n| n.id == boundary_in_id(&site.id) && n.scope.iter().any(|g| g == group))
        })
    }
}
