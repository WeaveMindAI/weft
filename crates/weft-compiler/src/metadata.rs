//! Metadata wires: from the ports whose value is written with the program
//! to the inputs that value fills.
//!
//! A metadata port ([`MetadataPort`]) holds a value fixed when the program
//! is written: a group writes it in its body (`self.tool = { ... }`), a node
//! type in its `metadata.json`. A wire leaving one never runs. Before
//! anything else in enrich, this pass writes the value into the input the
//! wire points at, exactly as if it had been typed there, and moves the
//! wire out of `edges` into `metadata_wires`, which only the editor reads,
//! to say where the value came from.
//! Everything after (the ports a written key creates, the casts, every
//! check on literals) sees a literal and nothing else: a metadata wire is
//! no edge to any graph walk, so pointing backwards makes no cycle.
//!
//! Where the value lands is where the person typed the wire: a node's
//! input, or a group's input seen from outside (`g.x = search.tool`). The
//! joined wire runs on from there to the members that read `self.x`; that
//! remainder is the wire the group's own port sends when it opens, as for
//! any literal written on it.
//!
//! A group's metadata port may also take its value, whole or in part, from
//! other metadata ports: `self.tool = inner.tool`, or `self.tools =
//! [...screen.tools, ...mouse.tools]`. The lowering keeps such a value as
//! written ([`MetadataExpr`], [`parse_written`]); this pass works it out,
//! refusing a cycle of them. The editor reads where each part comes from
//! off the written value itself.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;
use weft_core::node::MetadataCatalog;
use weft_core::project::{
    ConfigFieldSpan, Crossing, CrossingSide, Edge, GroupBoundaryRole, GroupKind, MetadataExpr, MetadataField, MetadataItem,
    MetadataPort, ProjectDefinition, Span,
};
use weft_core::weft_type::WeftType;

use crate::enrich::EnrichError;

/// Whose metadata port: a node's or a group's (a loop's sit on its group),
/// by index into the program's lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Owner {
    Node(usize),
    Group(usize),
}

type PortKey = (Owner, String);

/// What one wire is, read against the metadata ports it meets.
#[derive(Debug)]
struct Walk {
    /// The metadata port the wire leaves, if it leaves one.
    source: Option<PortKey>,
    stop: Stop,
}

#[derive(Debug)]
enum Stop {
    /// The wire meets no metadata port that stops it: an ordinary wire,
    /// or a metadata wire nothing reads past an include's site.
    Through,
    /// The value lands on a node's input (the wire's own target).
    NodeInput { node: usize, port: String },
    /// The value lands on a group's input seen from outside, at crossing
    /// `at`.
    GroupInput { group: usize, port: String, at: usize },
    /// A metadata value reaches an ordinary group output from inside, at
    /// crossing `at`.
    IntoOutput { at: usize },
    /// The wire lands on a metadata port from outside, at crossing `at`
    /// (`via.len()`: its own target).
    AsTarget { at: usize, owner: Owner, port: String },
}

/// The program's ids, by index, and each loop boundary's loop.
struct Index {
    nodes: HashMap<String, usize>,
    groups: HashMap<String, usize>,
    boundary: HashMap<usize, (GroupBoundaryRole, usize)>,
}

impl Index {
    fn new(project: &ProjectDefinition) -> Self {
        let nodes: HashMap<String, usize> = project.nodes.iter().enumerate().map(|(at, n)| (n.id.clone(), at)).collect();
        let groups: HashMap<String, usize> = project.groups.iter().enumerate().map(|(at, g)| (g.id.clone(), at)).collect();
        let boundary = project.nodes.iter().enumerate()
            .filter_map(|(at, n)| {
                let b = n.group_boundary.as_ref()?;
                Some((at, (b.role, *groups.get(&b.group_id)?)))
            })
            .collect();
        Index { nodes, groups, boundary }
    }
}

fn metadata_port<'p>(project: &'p ProjectDefinition, owner: Owner, port: &str) -> Option<&'p MetadataPort> {
    let ports = match owner {
        Owner::Node(at) => &project.nodes[at].metadata_ports,
        Owner::Group(at) => &project.groups[at].metadata_ports,
    };
    ports.iter().find(|m| m.name == port)
}

fn owner_id(project: &ProjectDefinition, owner: Owner) -> String {
    match owner {
        Owner::Node(at) => weft_core::project::plain_id(&project.nodes[at].id),
        Owner::Group(at) => weft_core::project::plain_id(&project.groups[at].id),
    }
}

/// Read one wire against the metadata ports it meets.
fn walk(project: &ProjectDefinition, index: &Index, edge: &Edge) -> Walk {
    let has = |owner: Owner, port: &str| metadata_port(project, owner, port).is_some();
    // Whether the wire lands inside the group that owns the port it reads.
    let lands_inside = |owner: Owner| match owner {
        Owner::Group(at) => edge.target.starts_with(&format!("{}.", project.groups[at].id)),
        Owner::Node(_) => false,
    };
    // The end the wire leaves: a group's own port (a loop's is its
    // `LoopOut`, read from outside), or a node's.
    let (source, start) = if edge.from_group_port() {
        let own = &edge.via[0];
        match index.groups.get(&own.group).copied().map(Owner::Group) {
            // Read from inside (`m.in = self.tool`, or `g.tool` inside
            // `g`), a metadata port is no port at all, like an output read
            // there: the wire stays as written, and validate names the
            // line (`check_port_resolution`).
            Some(owner) if has(owner, &own.port) && own.side == CrossingSide::Out && !lands_inside(owner) => {
                (Some((owner, own.port.clone())), 1)
            }
            _ => (None, 1),
        }
    } else {
        let port = edge.source_port();
        let source = index.nodes.get(&edge.source).and_then(|&node| match index.boundary.get(&node) {
            Some(&(role, group)) => has(Owner::Group(group), port).then_some((role, Owner::Group(group))),
            None => has(Owner::Node(node), port).then_some((GroupBoundaryRole::Out, Owner::Node(node))),
        });
        match source {
            // A loop's metadata port read from inside, as above.
            Some((GroupBoundaryRole::In, _)) | None => (None, 0),
            Some((_, owner)) if lands_inside(owner) => (None, 0),
            Some((_, owner)) => (Some((owner, port.to_string())), 0),
        }
    };
    for (at, crossing) in edge.via.iter().enumerate().skip(start) {
        let Some(&group) = index.groups.get(&crossing.group) else { continue };
        let owner = Owner::Group(group);
        if has(owner, &crossing.port) {
            // Leaving through a metadata port is an included file's value
            // passing out through the site that calls it: the same value.
            // Nothing else leaves through one, since no line but
            // `self.x = ...`, which the lowering keeps apart, lands there.
            if crossing.side == CrossingSide::In {
                return Walk { source, stop: Stop::AsTarget { at, owner, port: crossing.port.clone() } };
            }
        } else if source.is_some() {
            let port = crossing.port.clone();
            return Walk {
                source,
                stop: match crossing.side {
                    CrossingSide::In => Stop::GroupInput { group, port, at },
                    CrossingSide::Out => Stop::IntoOutput { at },
                },
            };
        }
    }
    if edge.to_group_port() {
        return Walk { source, stop: Stop::Through };
    }
    let port = edge.target_port().to_string();
    let end = edge.via.len();
    let Some(&node) = index.nodes.get(&edge.target) else { return Walk { source, stop: Stop::Through } };
    let owner = match index.boundary.get(&node) {
        Some(&(_, group)) => Owner::Group(group),
        None => Owner::Node(node),
    };
    if has(owner, &port) {
        return Walk { source, stop: Stop::AsTarget { at: end, owner, port } };
    }
    match source {
        Some(_) => Walk { source, stop: Stop::NodeInput { node, port } },
        None => Walk { source, stop: Stop::Through },
    }
}

/// The keys a wire reads off its value up to and including crossing `at`
/// (`via.len()`: all of them).
fn keys_through(edge: &Edge, at: usize) -> Vec<String> {
    if at >= edge.via.len() {
        return edge.path.clone();
    }
    edge.via[..=at].iter().flat_map(|crossing| crossing.path.iter().cloned()).collect()
}

/// The line of the leg that ends at crossing `at` (`via.len()`: the
/// wire's last leg), or the nearest line before it a person wrote.
fn line_at(edge: &Edge, at: usize) -> (Span, Option<String>) {
    if at < edge.via.len() {
        if let Some(found) = edge.via[..=at].iter().rev().find(|crossing| crossing.span.is_some()) {
            return (found.span.unwrap_or_default(), found.source_file.clone());
        }
    }
    (edge.span.unwrap_or_default(), edge.source_file.clone())
}

/// The wire cut where it stops at crossing `at`, as the editor draws it:
/// ending on that group's port.
fn cut_at(edge: &Edge, at: usize) -> Edge {
    if at >= edge.via.len() {
        return edge.clone();
    }
    let via: Vec<Crossing> = edge.via[..=at].to_vec();
    let end = &via[at];
    Edge {
        id: crate::wires::wire_id(&edge.source, edge.source_port(), &via, &end.group, &end.port, &keys_through(edge, at)),
        source: edge.source.clone(),
        target: end.group.clone(),
        source_handle: edge.source_handle.clone(),
        target_handle: Some(end.port.clone()),
        path: keys_through(edge, at),
        span: end.span,
        source_file: end.source_file.clone(),
        via,
    }
}

/// The part of the wire past the group port at crossing `at`, sent from
/// that port: an input's when the group opens, as for any value written
/// there, an output's as for any output nothing inside writes. `None`
/// when nothing past the port reads it.
fn sent_from(edge: &Edge, at: usize) -> Option<Edge> {
    let entered = &edge.via[at];
    if at + 1 == edge.via.len() && edge.to_group_port() {
        return None;
    }
    let own = Crossing { group: entered.group.clone(), port: entered.port.clone(), side: entered.side, path: Vec::new(), span: None, source_file: None };
    let via: Vec<Crossing> = std::iter::once(own).chain(edge.via[at + 1..].iter().cloned()).collect();
    Some(Edge {
        id: crate::wires::wire_id(&entered.group, &entered.port, &via, &edge.target, edge.target_port(), &edge.path_after(at)),
        source: entered.group.clone(),
        target: edge.target.clone(),
        source_handle: Some(entered.port.clone()),
        target_handle: edge.target_handle.clone(),
        path: edge.path_after(at),
        via,
        span: edge.span,
        source_file: edge.source_file.clone(),
    })
}

/// A resolved value and the type it is of.
type Resolved = Option<(Value, WeftType)>;

/// Why a port a written value names gives no value: the finding to report
/// (code and message), or `None` when the port's own problem is already
/// reported, or when its value is not known here (an include's opaque
/// node in the editor, which does not open the file).
type Missing = Option<(&'static str, String)>;

struct Resolver<'p> {
    project: &'p ProjectDefinition,
    index: &'p Index,
    catalog: &'p dyn MetadataCatalog,
    memo: HashMap<PortKey, Resolved>,
    resolving: Vec<PortKey>,
    errors: Vec<EnrichError>,
}

impl Resolver<'_> {
    /// The value of a metadata port: written on it, or worked out from what
    /// its group's body writes ([`MetadataPort::written`]). `None` when it
    /// has neither, or when working it out was refused (and said).
    fn value(&mut self, key: &PortKey) -> Resolved {
        if let Some(done) = self.memo.get(key) {
            return done.clone();
        }
        let project = self.project;
        // An include's call site holds its body's value: worked out once,
        // on the body, so what is wrong with it is said once, where the
        // file writes it.
        if let Owner::Group(at) = key.0 {
            if let GroupKind::Call { body, .. } = &project.groups[at].kind {
                let body = *self.index.groups.get(body.as_str())?;
                let resolved = self.value(&(Owner::Group(body), key.1.clone()));
                self.memo.insert(key.clone(), resolved.clone());
                return resolved;
            }
        }
        let port = metadata_port(project, key.0, &key.1)?;
        let resolved = match (&port.value, &port.written) {
            (Some(value), _) => Some((value.clone(), port.port_type.clone())),
            (None, None) => None,
            (None, Some(written)) => {
                if let Some(at) = self.resolving.iter().position(|k| k == key) {
                    let round: Vec<String> = self.resolving[at..].iter()
                        .map(|(owner, port)| format!("'{}.{port}'", owner_id(project, *owner)))
                        .collect();
                    self.refuse(key, "metadata-reference-cycle", format!(
                        "the metadata ports {} take their value from each other, so none of them has one: \
                         write the value out on one of them",
                        round.join(" -> "),
                    ));
                    return None;
                }
                self.resolving.push(key.clone());
                let worked = written.evaluate(
                    &mut |source, name, path| self.named(source, name, path),
                    |item, value| Some((
                        "type-mismatch",
                        format!(
                            "`...` puts a list's items into this one, and {} holds {}, which is no list",
                            describe(item),
                            weft_core::weft_type::WeftType::infer(value),
                        ),
                    )),
                );
                self.resolving.pop();
                match worked {
                    Ok(value) => Some((value, port.port_type.clone())),
                    Err(Some((code, message))) => {
                        self.refuse(key, code, message);
                        None
                    }
                    Err(None) => None,
                }
            }
        };
        self.memo.insert(key.clone(), resolved.clone());
        resolved
    }

    /// The value of port `name` of `source` (a scoped id), named in a
    /// written value, with `path` read off it.
    fn named(&mut self, source: &str, name: &str, path: &[String]) -> Result<Value, Missing> {
        let project = self.project;
        let shown = weft_core::project::plain_id(source);
        let owner = self.index.groups.get(source).copied().map(Owner::Group)
            .or_else(|| self.index.nodes.get(source).copied().map(Owner::Node))
            .ok_or_else(|| Some(("unknown-source-node", format!("'{shown}' is nothing declared here, so it has no metadata port '{name}'"))))?;
        if metadata_port(project, owner, name).is_none() {
            // Before enrich a node's outputs are its type's and its header's.
            let computed = match owner {
                Owner::Node(at) => project.nodes[at].outputs.iter().any(|p| p.name == name)
                    || self.catalog.lookup(&project.nodes[at].node_type).is_some_and(|meta| meta.outputs.iter().any(|o| o.name == name)),
                Owner::Group(at) => project.groups[at].out_ports.iter().any(|p| p.name == name)
                    || project.groups[at].in_ports.iter().any(|p| p.name == name),
            };
            return Err(Some(if computed {
                ("metadata-value-not-literal", format!(
                    "'{shown}.{name}' is computed during the run, and a metadata port's value is fixed when the \
                     program is written: write the value itself, or take it from another metadata port"
                ))
            } else {
                ("unknown-source-port", format!("'{shown}' has no metadata port '{name}'"))
            }));
        }
        let value = self.value(&(owner, name.to_string())).ok_or(None)?;
        project_along(value, path).map(|(value, _)| value).map_err(|why| Some(("deref-path", why)))
    }

    /// A finding about the value written for `key`, on the line that
    /// writes it.
    fn refuse(&mut self, key: &PortKey, code: &'static str, message: String) {
        let project = self.project;
        let (anchor, file, subject) = match key.0 {
            Owner::Node(at) => (project.nodes[at].header_span, project.nodes[at].source_file.clone(), project.nodes[at].id.clone()),
            Owner::Group(at) => (project.groups[at].header_span.or(project.groups[at].span), project.groups[at].source_file.clone(), project.groups[at].id.clone()),
        };
        let span = metadata_port(project, key.0, &key.1).and_then(|port| port.span).or(anchor).unwrap_or_default();
        self.errors.push(EnrichError { code, span, file, subjects: vec![subject], message });
    }
}

/// How a finding names a part of a written value.
fn describe(item: &MetadataExpr) -> String {
    match item {
        MetadataExpr::Port { source, port, path } => {
            let mut name = format!("'{}.{port}", weft_core::project::plain_id(source));
            for key in path {
                name.push('.');
                name.push_str(key);
            }
            name.push('\'');
            name
        }
        _ => "the value written there".to_string(),
    }
}

/// `value` with `keys` read off it, and its type there. A key the value
/// does not hold is an error: nothing would be written.
fn project_along((value, ty): (Value, WeftType), keys: &[String]) -> Result<(Value, WeftType), String> {
    if keys.is_empty() {
        return Ok((value, ty));
    }
    let walked = weft_core::deref::walk_path(&ty, keys)
        .map_err(|why| format!("reading '{}' off a metadata port: {why}", keys.join(".")))?;
    match weft_core::deref::project_value(&value, &ty, keys) {
        Ok(weft_core::deref::Projection::Value(projected)) => Ok((projected, walked)),
        Ok(weft_core::deref::Projection::Closed { key }) => Err(format!(
            "reading '{}' off a metadata port: its value has no '{key}', so there is nothing to write. \
             Read a key the value holds, or write the value with it",
            keys.join("."),
        )),
        Err(why) => Err(format!("reading '{}' off a metadata port: {why}", keys.join("."))),
    }
}

/// A value written in a group's body for one of its metadata ports, as
/// the text after `self.port =` reads: JSON in which a dotted name
/// (`inner.tool`, `act.tools.anthropic`) stands for the value of the
/// metadata port it names, and `...name` inside a list for that list's
/// items. The names are kept as written; the lowering scopes them. A value
/// that names no port comes back as one literal, which the caller reads
/// as any written value instead (a heredoc, a `@file`).
pub(crate) fn parse_written(text: &str) -> Result<MetadataExpr, String> {
    let mut reader = Reader { text, at: 0 };
    let expr = reader.expr()?;
    reader.blank();
    if reader.at < text.len() {
        return Err(reader.unexpected("the end of the value"));
    }
    Ok(expr)
}

struct Reader<'t> {
    text: &'t str,
    at: usize,
}

impl<'t> Reader<'t> {
    fn rest(&self) -> &'t str {
        &self.text[self.at..]
    }

    /// Skip whitespace. Nothing else: a value written out in full is read
    /// as strict JSON (`parse_config_literal`), so one naming ports takes
    /// no comment and no trailing comma either.
    fn blank(&mut self) {
        let rest = self.rest();
        self.at += rest.len() - rest.trim_start().len();
    }

    fn unexpected(&self, wanted: &str) -> String {
        let seen: String = self.rest().chars().take(12).collect();
        if seen.is_empty() {
            format!("expected {wanted}, and the value ended")
        } else {
            format!("expected {wanted} at `{seen}`")
        }
    }

    fn eat(&mut self, token: &str) -> bool {
        self.blank();
        if self.rest().starts_with(token) {
            self.at += token.len();
            true
        } else {
            false
        }
    }

    fn expr(&mut self) -> Result<MetadataExpr, String> {
        self.blank();
        let rest = self.rest();
        if self.eat("{") {
            let mut fields: Vec<MetadataField> = Vec::new();
            if self.eat("}") {
                return Ok(fold(MetadataExpr::Object { fields }));
            }
            loop {
                self.blank();
                let key = self.string()?.ok_or_else(|| self.unexpected("a quoted key"))?;
                if fields.iter().any(|f| f.key == key) {
                    return Err(format!("key \"{key}\" is written twice"));
                }
                if !self.eat(":") {
                    return Err(self.unexpected("`:`"));
                }
                fields.push(MetadataField { key, value: self.expr()? });
                if self.eat("}") {
                    return Ok(fold(MetadataExpr::Object { fields }));
                }
                if !self.eat(",") {
                    return Err(self.unexpected("`,` or `}`"));
                }
            }
        }
        if rest.starts_with('[') {
            self.at += 1;
            let mut items: Vec<MetadataItem> = Vec::new();
            if self.eat("]") {
                return Ok(fold(MetadataExpr::List { items }));
            }
            loop {
                let spread = self.eat("...");
                items.push(MetadataItem { spread, value: self.expr()? });
                if self.eat("]") {
                    return Ok(fold(MetadataExpr::List { items }));
                }
                if !self.eat(",") {
                    return Err(self.unexpected("`,` or `]`"));
                }
            }
        }
        if let Some(text) = self.string()? {
            return Ok(MetadataExpr::Literal { value: Value::String(text) });
        }
        let word_len = rest.find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-' | '+'))).unwrap_or(rest.len());
        let word = &rest[..word_len];
        if word.is_empty() {
            return Err(self.unexpected("a value"));
        }
        self.at += word_len;
        if let Ok(value) = serde_json::from_str::<Value>(word) {
            if !value.is_string() {
                return Ok(MetadataExpr::Literal { value });
            }
        }
        let segments: Vec<&str> = word.split('.').collect();
        let ident = |s: &str| s.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if segments.len() < 2 || !segments.iter().all(|s| ident(s)) {
            return Err(format!(
                "`{word}` is no value: write JSON, or name another metadata port as `node.port`"
            ));
        }
        Ok(MetadataExpr::Port {
            source: segments[0].to_string(),
            port: segments[1].to_string(),
            path: segments[2..].iter().map(|s| s.to_string()).collect(),
        })
    }

    /// A JSON string at the cursor, `None` when there is none.
    fn string(&mut self) -> Result<Option<String>, String> {
        let rest = self.rest();
        if !rest.starts_with('"') {
            return Ok(None);
        }
        let mut escaped = false;
        for (i, c) in rest.char_indices().skip(1) {
            match c {
                '\\' if !escaped => escaped = true,
                '"' if !escaped => {
                    let text = serde_json::from_str::<String>(&rest[..=i]).map_err(|e| format!("a string does not read: {e}"))?;
                    self.at += i + 1;
                    return Ok(Some(text));
                }
                _ => escaped = false,
            }
        }
        Err("a string is not closed".to_string())
    }
}

/// A list or an object that names no port is one literal.
fn fold(expr: MetadataExpr) -> MetadataExpr {
    let literal = |e: &MetadataExpr| match e {
        MetadataExpr::Literal { value } => Some(value.clone()),
        _ => None,
    };
    match &expr {
        MetadataExpr::List { items } if items.iter().all(|i| !i.spread && literal(&i.value).is_some()) => {
            MetadataExpr::Literal { value: Value::Array(items.iter().filter_map(|i| literal(&i.value)).collect()) }
        }
        MetadataExpr::Object { fields } if fields.iter().all(|f| literal(&f.value).is_some()) => MetadataExpr::Literal {
            value: Value::Object(fields.iter().filter_map(|f| Some((f.key.clone(), literal(&f.value)?))).collect()),
        },
        _ => expr,
    }
}

/// Resolve every metadata wire of `project` (see the module docs). Each
/// node first takes its type's metadata ports from `catalog`.
pub(crate) fn resolve(project: &mut ProjectDefinition, catalog: &dyn MetadataCatalog) -> Vec<EnrichError> {
    for node in project.nodes.iter_mut() {
        // An include's opaque node carries its file's, from the lowering.
        if node.include_path.is_some() {
            continue;
        }
        if let Some(meta) = catalog.lookup(&node.node_type) {
            node.metadata_ports = meta.metadata_ports.clone();
        }
    }
    scope_written_names(project);
    let index = Index::new(project);
    let walks: Vec<Walk> = project.edges.iter().map(|edge| walk(project, &index, edge)).collect();

    let mut resolver = Resolver { project, index: &index, catalog, memo: HashMap::new(), resolving: Vec::new(), errors: Vec::new() };

    // Every decision is taken against the program as written, then applied:
    // a refused wire writes nothing and fills nothing, so `metadata_wires`
    // holds exactly the wires whose value an input takes (the editor reads
    // it as such, validate counts it as that input's driver).
    struct Write {
        owner: Owner,
        port: String,
        value: Value,
        ty: WeftType,
        span: Span,
        file: Option<String>,
    }
    let mut writes: Vec<Write> = Vec::new();
    let mut landed: HashMap<PortKey, PortKey> = HashMap::new();
    // Each line met, and whether it was accepted.
    type Line = (String, Vec<String>, Option<(usize, usize)>, Option<String>);
    let line_of = |filled: &Edge| -> Line {
        (filled.id.clone(), filled.path.clone(), filled.span.map(|s| (s.start_line, s.start_column)), filled.source_file.clone())
    };
    let mut lines: HashMap<Line, bool> = HashMap::new();
    let mut kept: Vec<Edge> = Vec::new();
    let mut sent: BTreeMap<String, Edge> = BTreeMap::new();
    let mut fills: BTreeMap<String, Edge> = BTreeMap::new();
    for (edge, walk) in project.edges.iter().zip(&walks) {
        let subjects = vec![edge.source.clone(), edge.target.clone()];
        let refuse = |resolver: &mut Resolver, code: &'static str, at: usize, message: String| {
            let (span, file) = line_at(edge, at);
            resolver.errors.push(EnrichError { code, span, file, subjects: subjects.clone(), message });
        };
        let source_name = |key: &PortKey| format!("{}.{}", owner_id(project, key.0), key.1);
        let Some(source) = &walk.source else {
            // A wire from a port the run computes, or from a metadata port
            // read from inside: it carries nothing a metadata port can take.
            match &walk.stop {
                Stop::AsTarget { at, owner, port } => refuse(&mut resolver, "metadata-port-as-target", *at,
                    crate::weft_compiler::metadata_target_message(&owner_id(project, *owner), port)),
                _ => kept.push(edge.clone()),
            }
            continue;
        };
        // From here on the wire is a metadata wire: it never runs.
        let (landing, at) = match &walk.stop {
            // Nothing past an include's site reads the value: no input takes it.
            Stop::Through => continue,
            Stop::IntoOutput { at } => {
                // The output still sends from its own port, as one nothing
                // inside writes, so what reads it outside is not reported
                // again as having no driver; the line is said once however
                // many read it.
                if let Some(rest) = sent_from(edge, *at) {
                    sent.insert(rest.id.clone(), rest);
                }
                if lines.insert(line_of(&cut_at(edge, *at)), false).is_some() {
                    continue;
                }
                let crossing = &edge.via[*at];
                refuse(&mut resolver, "metadata-into-output", *at, format!(
                    "'self.{port}' is an output of '{group}', which the run fills, and '{}' holds a value fixed when \
                     the program is written. To hand that value on from '{group}', declare the port a metadata port \
                     (`-> (*{port}: Type)`)",
                    source_name(source),
                    port = crossing.port,
                    group = weft_core::project::plain_id(&crossing.group),
                ));
                continue;
            }
            Stop::AsTarget { at, owner, port } => {
                refuse(&mut resolver, "metadata-port-as-target", *at,
                    crate::weft_compiler::metadata_target_message(&owner_id(project, *owner), port));
                continue;
            }
            Stop::NodeInput { node, port } => ((Owner::Node(*node), port.clone()), edge.via.len()),
            Stop::GroupInput { group, port, at } => {
                // A value written on a port the group does not declare is
                // refused by the lowering; the same line as a wire is too.
                let declared = &project.groups[*group];
                if !declared.in_ports.iter().any(|p| p.name == *port) && !weft_core::run::skip::is_gate_port(port) {
                    refuse(&mut resolver, "unknown-target-port", *at, format!(
                        "'{}' has no input port '{port}'. Its ports are the ones in its signature, plus the gate \
                         (`_should_flow` or `_should_not_flow`)",
                        weft_core::project::plain_id(&declared.id),
                    ));
                    continue;
                }
                ((Owner::Group(*group), port.clone()), *at)
            }
        };
        let filled = if at < edge.via.len() { cut_at(edge, at) } else { edge.clone() };
        let rest = (at < edge.via.len()).then(|| sent_from(edge, at)).flatten();
        // The line a person wrote, up to where the value lands. Joining
        // copies a wire landing on a group input once per member reading
        // it: every copy is the same line, decided once (written once,
        // refused once); each only adds what runs on past the input. Two
        // lines differ by where they are written, or by the keys they read.
        let line = line_of(&filled);
        if let Some(accepted) = lines.get(&line) {
            if let (true, Some(rest)) = (*accepted, rest) {
                sent.insert(rest.id.clone(), rest);
            }
            continue;
        }
        let shown = format!("{}.{}", owner_id(project, landing.0), landing.1);
        if let Some(first) = landed.get(&landing) {
            let first = source_name(first);
            refuse(&mut resolver, "double-driven-port", at, format!(
                "'{shown}' takes two metadata wires, from '{first}' and '{}': keep one", source_name(source),
            ));
            lines.insert(line, false);
            continue;
        }
        if written_on(project, &index, &landing) {
            refuse(&mut resolver, "double-driven-port", at, format!(
                "'{shown}' is written in the source and fed by a metadata wire: keep one of the two",
            ));
            lines.insert(line, false);
            continue;
        }
        lines.insert(line, true);
        landed.insert(landing.clone(), source.clone());
        if let Some(rest) = rest {
            sent.insert(rest.id.clone(), rest);
        }
        fills.insert(filled.id.clone(), filled);
        let (span, file) = line_at(edge, at);
        // A value the source cannot give is said where it is written; the
        // input still counts this wire as its driver, so nothing else is
        // reported about it.
        match resolver.value(source).map(|c| project_along(c, &keys_through(edge, at))) {
            Some(Ok((value, ty))) => writes.push(Write { owner: landing.0, port: landing.1, value, ty, span, file }),
            Some(Err(why)) => refuse(&mut resolver, "deref-path", at, why),
            None => {}
        }
    }

    // Every metadata port a person declares has a value in the end. An
    // include's site takes its body's (whose own is checked), and its
    // opaque node in the editor holds what the file writes, which it does
    // not open. A written value is worked out here even when no wire
    // leaves the port, so what is wrong with it is said.
    let mut unwritten = Vec::new();
    for (at, group) in project.groups.iter().enumerate() {
        if matches!(group.kind, GroupKind::Call { .. }) {
            continue;
        }
        for port in &group.metadata_ports {
            let key = (Owner::Group(at), port.name.clone());
            resolver.value(&key);
            if port.value.is_none() && port.written.is_none() {
                unwritten.push(EnrichError {
                    code: "metadata-port-not-written",
                    span: group.header_span.or(group.span).unwrap_or_default(),
                    file: group.source_file.clone(),
                    subjects: vec![group.id.clone()],
                    message: format!(
                        "metadata port '{}' of '{}' has no value: write it in the {}'s body \
                         (`self.{} = {{ ... }}`), or take it from another metadata port",
                        port.name, weft_core::project::plain_id(&group.id), group.kind.noun(), port.name,
                    ),
                });
            }
        }
    }
    let resolved: Vec<(PortKey, Value)> = resolver.memo.iter()
        .filter_map(|(key, value)| Some((key.clone(), value.as_ref()?.0.clone())))
        .collect();
    let mut errors = resolver.errors;
    errors.extend(unwritten);

    // Apply: the values, the wires that run, the wires that fill an input.
    for (key, value) in resolved {
        let ports = match key.0 {
            Owner::Node(at) => &mut project.nodes[at].metadata_ports,
            Owner::Group(at) => &mut project.groups[at].metadata_ports,
        };
        if let Some(port) = ports.iter_mut().find(|m| m.name == key.1) {
            port.value = Some(value);
        }
    }
    for Write { owner, port, value, ty, span, file } in writes {
        let mut field = ConfigFieldSpan::connection(span);
        field.source_file = file;
        match owner {
            Owner::Node(at) => {
                let node = &mut project.nodes[at];
                match node.group_boundary {
                    // A loop's literals sit on its `LoopIn`, which enrich
                    // never moves, and where the editor reads them for the
                    // loop box: one a metadata wire writes lands there too.
                    Some(_) => {
                        node.port_literals.insert(port.clone(), value);
                        node.port_literal_spans.insert(port.clone(), field);
                        node.port_literal_types.insert(port, ty);
                    }
                    None => {
                        let config = node.config.as_object_mut().expect("a node's config is an object");
                        config.insert(port.clone(), value);
                        node.config_spans.insert(port.clone(), field);
                        node.port_literal_types.insert(port, ty);
                    }
                }
            }
            Owner::Group(at) => write_on_group(&mut project.groups[at], port, value, ty, field),
        }
    }
    let kept_ids: HashSet<String> = kept.iter().map(|edge| edge.id.clone()).collect();
    kept.extend(sent.into_values().filter(|edge| !kept_ids.contains(&edge.id)));
    project.edges = kept;
    project.metadata_wires = fills.into_values().collect();
    errors
}

fn write_on_group(group: &mut weft_core::project::GroupDefinition, port: String, value: Value, ty: WeftType, field: ConfigFieldSpan) {
    group.port_literals.insert(port.clone(), value);
    group.port_literal_spans.insert(port.clone(), field);
    group.port_literal_types.insert(port, ty);
}

/// Whether the source already writes a value on the input a metadata wire
/// lands on: a node's config key (before enrich moves it), a loop
/// boundary's or a group's port literal.
fn written_on(project: &ProjectDefinition, index: &Index, (owner, port): &PortKey) -> bool {
    match *owner {
        Owner::Node(at) => {
            let node = &project.nodes[at];
            match index.boundary.get(&at) {
                Some(_) => node.port_literals.contains_key(port),
                None => node.config.as_object().is_some_and(|config| config.contains_key(port)),
            }
        }
        Owner::Group(at) => project.groups[at].port_literals.contains_key(port),
    }
}

/// Scope every name a group's written metadata value holds the way a name
/// reads where it is written: the lowering already scoped a member of the
/// group (`g.inner`); any other name is the nearest enclosing scope's
/// member of that name, else a top-level one. Inside an included file the
/// file is the top: its own group ends the walk, and a name it does not
/// declare stays unresolved rather than reaching into the program that
/// includes it. A call site carries its body's value, already scoped
/// there. The scoped id is written back onto the value, so the editor
/// follows the same id the build resolved.
fn scope_written_names(project: &mut ProjectDefinition) {
    let declared: HashSet<String> = project.nodes.iter().map(|n| n.id.clone())
        .chain(project.groups.iter().map(|g| g.id.clone()))
        .collect();
    let parents: HashMap<String, Option<String>> = project.groups.iter()
        .map(|g| (g.id.clone(), g.parent_group_id.clone()))
        .collect();
    let bodies: HashSet<String> = project.groups.iter()
        .filter(|g| matches!(g.kind, GroupKind::Body))
        .map(|g| g.id.clone())
        .collect();
    for group in project.groups.iter_mut() {
        if matches!(group.kind, GroupKind::Call { .. }) {
            continue;
        }
        // The enclosing scopes, nearest first, ending at the file's top: an
        // included file's body (then nothing past it), or the program's.
        let mut scopes: Vec<String> = Vec::new();
        let mut top: Option<String> = None;
        let mut at = Some(group.id.clone());
        while let Some(id) = at {
            if bodies.contains(&id) {
                top = Some(id.clone());
                if id != group.id {
                    scopes.push(id);
                }
                break;
            }
            if id != group.id {
                scopes.push(id.clone());
            }
            at = parents.get(&id).cloned().flatten();
        }
        let own = format!("{}.", group.id);
        for port in group.metadata_ports.iter_mut() {
            let Some(written) = port.written.as_mut() else { continue };
            written.rescope(&mut |source| {
                if source.starts_with(&own) {
                    return source.to_string();
                }
                scopes.iter()
                    .map(|scope| format!("{scope}.{source}"))
                    .find(|id| declared.contains(id))
                    .unwrap_or_else(|| match &top {
                        // Nothing in the file of that name: scoped to the
                        // file, where it resolves to nothing and is refused.
                        Some(body) => format!("{body}.{source}"),
                        None => source.to_string(),
                    })
            });
        }
    }
    // Each call site takes its body's value as the body scoped it.
    let scoped: HashMap<String, Vec<MetadataPort>> = project.groups.iter()
        .filter(|g| matches!(g.kind, GroupKind::Body))
        .map(|g| (g.id.clone(), g.metadata_ports.clone()))
        .collect();
    for group in project.groups.iter_mut() {
        let GroupKind::Call { body, .. } = &group.kind else { continue };
        let Some(ports) = scoped.get(body) else { continue };
        for port in group.metadata_ports.iter_mut() {
            if let Some(from_body) = ports.iter().find(|m| m.name == port.name) {
                port.written = from_body.written.clone();
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/metadata_tests.rs"]
mod tests;
