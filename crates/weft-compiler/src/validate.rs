//! Graph validation. Runs after enrichment. Emits structured
//! Diagnostic objects (errors + warnings) for the IDE's Problems
//! panel and for the full compile pipeline. Each rule is documented at
//! the helper function that implements it, under the diagnostic code it
//! emits.

use weft_core::rules::{has_incoming_edge, literal_fills};
use weft_core::node::{
    Condition, MetadataCatalog, RuleDiagnostic, RuleSeverity, ValidationLevel, ValidationRule,
};

use weft_core::project::{NodeDefinition, Span};
use weft_core::weft_type::WeftType;
use weft_core::ProjectDefinition;

use crate::weft_compiler::read_loop_port_list_vetted;
use crate::{Diagnostic, Severity};

/// Which validation rules to run. `Structural` checks only editor-
/// time errors (graph shape, required ports wired or satisfied by a
/// literal, config shape). `Runtime` additionally runs rules flagged
/// `level: runtime` (e.g. a provider with no model named), which we deliberately
/// skip during editing so an AI builder or human-in-the-loop can
/// sketch a project without filling every secret.
// SYNC: ValidationMode <-> extension-vscode/src/parseServer.ts ValidationMode
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationMode {
    Structural,
    Runtime,
}

/// Run every validation rule against an enriched project and collect
/// all diagnostics. Returns an empty vector for a clean program.
/// `catalog` provides per-node metadata (including declarative
/// `validate` rules).
pub fn validate(project: &ProjectDefinition, catalog: &dyn MetadataCatalog) -> Vec<Diagnostic> {
    validate_with_mode(project, catalog, ValidationMode::Structural)
}

pub fn validate_with_mode(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
) -> Vec<Diagnostic> {
    // The catalog's type registry is scoped around the WHOLE run, so
    // every rule that resolves a declared name (named-type-conflict's
    // registry half in particular) sees the project's table on every
    // caller, not just the ones that remembered to scope it (the
    // build path once forgot, silently no-op-ing the rule). The scope
    // stack is re-entrant, so already-scoped callers nest harmlessly.
    let registry = catalog.type_registry();
    registry.scoped(|| validate_scoped(project, catalog, mode))
}

fn validate_scoped(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
) -> Vec<Diagnostic> {
    let mut d = Vec::new();
    check_duplicates(project, &mut d);
    check_edge_node_refs(project, &mut d);
    check_scope_reachability(project, &mut d);
    check_port_resolution(project, &mut d);
    check_type_compat(project, &mut d);
    check_port_coverage(project, catalog, &mut d);
    check_config_derived_ports(project, catalog, &mut d);
    check_metadata_ports(project, catalog, &mut d);
    check_pass_through_ports(project, &mut d);
    check_loop_config(project, &mut d);
    check_double_driven_ports(project, &mut d);
    check_two_gates(project, &mut d);
    check_warnings(project, &mut d);
    check_run_decided(project, &mut d);
    check_type_bounds(project, &mut d);
    check_placeholders(project, catalog, &mut d);
    check_name_casing(project, catalog, &mut d);
    check_level_sizes(project, &mut d);
    check_declarative_rules(project, catalog, mode, &mut d);
    check_reserved_names(project, catalog, &mut d);
    check_graph_shape(project, &mut d);
    check_generator_wiring(project, &mut d);
    check_named_type_conflicts(project, &mut d);
    check_route_claims(project, catalog, &mut d);
    check_per_instance(project, catalog, &mut d);
    check_instance_filled(project, &mut d);
    check_run_settings(project, &mut d);
    d
}

/// run-settings: how a trigger's runs are kept, read off the inputs the
/// language gives every trigger (`durable`, `recorded`), with the same
/// reading the ctx makes when the trigger registers. Refused here when the
/// written values cannot be (a durable run that is not recorded, one that
/// may outlive its caller and is not recorded, a switch that is not a
/// boolean); a value arriving on a wire is checked when the trigger
/// registers.
fn check_run_settings(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for node in project.nodes.iter().filter(|n| n.features.is_trigger) {
        // The constants written for the trigger's inputs (`enrich` moved
        // them out of `config`); a wired input is not among them.
        let config: serde_json::Map<String, serde_json::Value> =
            node.port_literals.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        if let Err(refused) = weft_core::run_settings::RunSettings::from_node_fields(&config) {
            cfg_push(d, node, refused.field, Severity::Error, "run-settings", format!("'{}': {refused}", author_name(node)));
        }
    }
}

/// per-instance-ineligible: `@per_instance` marks a node that RUNS something
/// of its own, which the node's metadata says, never its name: an infra
/// node (`requiresInfra`), one container per instance. What an instance
/// provides (its connection, its sheet) is a field written
/// `@instance_filled`, and a node that reads a per-instance value runs per
/// instance without a mark, which the compiler already follows.
fn check_per_instance(project: &ProjectDefinition, catalog: &dyn MetadataCatalog, d: &mut Vec<Diagnostic>) {
    for node in project.nodes.iter().filter(|n| n.per_instance == Some(weft_core::instance::PerInstance::Marked)) {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        if meta.per_instance_eligible() {
            continue;
        }
        let name = author_name(node);
        let instead = match meta.access_input() {
            Some(input) => format!(
                "To have each instance connect its own account, write `{}: @instance_filled` instead",
                input.name
            ),
            None => "Remove the mark: it already runs in an instance's runs whenever it reads a per-instance \
                     value, and a field each instance provides is written `@instance_filled`"
                .to_string(),
        };
        push(d, &[node.part()], node.source_file.as_deref(), node.header_span_or_default(), Severity::Error, "per-instance-ineligible",
            format!(
                "'{name}' is marked `@per_instance`, but only an infra node exists once per instance (one \
                 container each); a {} runs nothing of its own to copy. {instead}",
                node.node_type,
            ));
    }
}

/// The places an `@instance_filled` cannot stand, each refused where it is
/// written:
/// - instance-filled-wired: the field also has a wire into it, so two things
///   would drive one value;
/// - instance-filled-boundary: a group's, loop's or included file's own port
///   (write it on the node inside that reads the value);
/// - instance-filled-not-an-input: a key that is not one of the node's
///   inputs, which no value from outside can reach;
/// - instance-filled-place-types: a field of a node in a file included at
///   places that give it different types, where one value cannot fit all.
fn check_instance_filled(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let boundary_message = |name: &str, field: &str| format!(
        "'{name}.{field}' is the port of a group, a loop or an included file, so it passes a value \
         on rather than using one; write `@instance_filled` on the input of the node inside \
         that reads it");
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        for (field, value) in &group.port_literals {
            if weft_core::instance::as_instance_filled(value).is_none() { continue; }
            let (span, file) = group_literal_anchor(group, field);
            push(d, &[group.id.as_str()], file, span, Severity::Error, "instance-filled-boundary",
                boundary_message(&weft_core::project::plain_id(&group.id), field));
        }
    }
    for node in &project.nodes {
        let name = author_name(node);
        if let Some(config) = node.config.as_object() {
            for (key, value) in config {
                if weft_core::instance::as_instance_filled(value).is_some() {
                    let (file, span) = cfg_anchor(node, key);
                    push(d, &[node.part()], file, span, Severity::Error, "instance-filled-not-an-input",
                        format!("'{name}.{key}' is not an input of '{name}', so no instance's value can reach it; \
                                 only an input can be `@instance_filled`"));
                }
            }
        }
        for (field, _) in weft_core::instance::instance_filled_fields(node) {
            let (file, span) = cfg_anchor(node, field);
            if node.group_boundary.is_some() {
                push(d, &[node.part()], file, span, Severity::Error, "instance-filled-boundary", boundary_message(name, field));
                continue;
            }
            if has_incoming_edge(node, project, field) {
                push(d, &[node.part()], file, span, Severity::Error, "instance-filled-wired",
                    format!("'{name}.{field}' is `@instance_filled` and also wired: each instance provides this \
                             value, so nothing else can drive it. Remove the wire, or the marker"));
            }
            // One value per instance reaches every place the node runs, so
            // the field must take the same type at each of them.
            let mut types = node.place_types.iter().filter_map(|place| place.inputs.get(field));
            if let Some(first) = types.next() {
                if let Some(other) = types.find(|ty| *ty != first) {
                    push(d, &[node.part()], file, span, Severity::Error, "instance-filled-place-types",
                        format!("'{name}.{field}' is `@instance_filled`, and the file it is in is included where \
                                 it takes {first} and where it takes {other}: one instance's value cannot be both. \
                                 Declare the field's type on the node"));
                }
            }
        }
    }
}

/// route-overlap: two places of this program claim public addresses a
/// single call could reach, and neither is the more specific, so which
/// one answers has no answer. A pair where one IS more specific is
/// fine and common (`cards/count` beside `cards/{id}`): the literal
/// takes the call it spells out, the capture takes the rest. A node in
/// a file included twice is at two places, so a path written in that
/// file is claimed twice.
///
/// The same question is asked again at activation, across every project
/// of the account, because only the dispatcher's table knows what the
/// author's OTHER programs already serve. What is answerable here is
/// the half inside one program, and answering it here is worth it: the
/// editor underlines it while you type instead of a deploy failing
/// minutes later on `cards/{id}` against `cards/{slug}`.
///
/// Nothing here knows what a Route is. A node type says which of its
/// config fields hold the address (`claimsRoute`), and route patterns
/// are the language's own vocabulary, so the rule reads the same for
/// Socket or anything else that ever mounts a path.
fn check_route_claims(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    d: &mut Vec<Diagnostic>,
) {
    use weft_core::route::{compare_patterns, methods_overlap, PatternOrder, RoutePattern};

    // Every claim this program makes.
    //
    // A path that is WIRED is skipped: it is computed when the trigger
    // sets up, so there is no address here to read or to compare. A path
    // that is simply not written is NOT skipped, because at run time an
    // absent path is the root of the project's address, which means two
    // triggers that both forgot one both claim it and the second dies at
    // activation. Refused here instead, where the person can see it.
    //
    // A path or a method that IS written and does not parse is reported
    // here, because this is the only place either is read at compile
    // time. Letting them through would ship a program that dies at
    // `weft activate`, and a bad method is worse than late: an empty
    // method list reads as "any method", so a typo would silently widen
    // what the node claims.
    let mut claims: Vec<Claim> = Vec::new();
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let Some(spec) = meta.claims_route.as_ref() else { continue };
        let written = node.written_value(&spec.path_field);
        // A path or method each instance provides is, like a wired one,
        // known only when that instance's trigger sets up; activation
        // compares it.
        let instance_filled =
            |field: &str| node.written_value(field).is_some_and(|value| weft_core::instance::as_instance_filled(value).is_some());
        if instance_filled(&spec.path_field) || spec.method_field.as_deref().is_some_and(instance_filled) {
            continue;
        }
        let raw = match written.map(|value| value.as_str()) {
            Some(Some(raw)) => raw,
            // Written, but not text. The config type rules name that.
            Some(None) => continue,
            None => {
                if has_incoming_edge(node, project, &spec.path_field) {
                    continue;
                }
                cfg_push(
                    d,
                    node,
                    &spec.path_field,
                    Severity::Error,
                    "route-path-missing",
                    format!(
                        "'{}' has no address, so it would answer at the project's own \
                         address and collide with anything else that did. Give it a \
                         `{}` (`hooks/stripe`, `chat/{{room}}`)",
                        author_name(node),
                        spec.path_field
                    ),
                );
                continue;
            }
        };
        if raw.trim().is_empty() {
            cfg_push(
                d,
                node,
                &spec.path_field,
                Severity::Error,
                "route-path-missing",
                format!(
                    "'{}' has an empty address, so it would answer at the project's own \
                     address and collide with anything else that did. Give it a `{}` \
                     (`hooks/stripe`, `chat/{{room}}`)",
                    author_name(node),
                    spec.path_field
                ),
            );
            continue;
        }
        let pattern = match RoutePattern::parse(raw) {
            Ok(pattern) => pattern,
            Err(why) => {
                cfg_push(
                    d,
                    node,
                    &spec.path_field,
                    Severity::Error,
                    "route-path-invalid",
                    format!("'{}' cannot be served: {why}", author_name(node)),
                );
                continue;
            }
        };
        if spec.captures_are_outputs {
            check_capture_outputs(node, meta, &pattern, &spec.path_field, d);
        }
        let written_method = spec
            .method_field
            .as_deref()
            .and_then(|field| node.written_value(field))
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|method| !method.is_empty());
        // A method outside the field's own options is already refused
        // (`literal-not-an-option`), naming the ones there are: one
        // finding for one mistake, and the broken claim stays out of the
        // overlap comparison.
        let off_the_options = spec.method_field.as_deref().is_some_and(|field| {
            let widget = node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref());
            let value = node.written_value(field);
            widget.zip(value).is_some_and(|(widget, value)| widget.has_options() && widget.check_value(value).is_err())
        });
        if off_the_options {
            continue;
        }
        let mut methods = Vec::new();
        if let Some(method) = written_method {
            match weft_core::route::normalize_method(method) {
                Ok(method) => methods.push(method),
                Err(why) => {
                    cfg_push(
                        d,
                        node,
                        spec.method_field.as_deref().unwrap_or(&spec.path_field),
                        Severity::Error,
                        "route-method-unknown",
                        format!("'{}' cannot be served: {why}", author_name(node)),
                    );
                    continue;
                }
            }
        }
        claims.push(Claim { node, field: spec.path_field.as_str(), pattern, methods, places: Vec::new() });
    }

    // A node inside an included file is one definition in the compiled
    // program, but every site that includes the file (through however
    // many files) serves its own copy of it. So a claim is made once per
    // place, and a file with a written path included twice claims that
    // path twice.
    //
    // A node in an included body that no site calls has no place at
    // all: it serves nothing, so it claims nothing and is left out of
    // the comparison. Its path was still checked above for being well
    // formed, because that is source the author wrote.
    let mut claims = placed(project, claims);
    claims.retain(|claim| !claim.places.is_empty());

    // Two patterns a call can reach are fine when one of them is the
    // more specific: that one takes the calls it spells out and the
    // other takes the rest. Only the pair where neither wins leaves a
    // call with no answer.
    let collide = |a: &Claim, b: &Claim| {
        compare_patterns(&a.pattern, &b.pattern) == PatternOrder::Ambiguous && methods_overlap(&a.methods, &b.methods)
    };
    let addresses = |claim: &Claim| -> Vec<String> {
        claim.places.iter().map(|place| weft_core::project::address_of(project, &claim.node.id, &place.path)).collect()
    };

    // One node at several places: the file it is written in is included
    // more than once, so its one written path is served at each, and a
    // claim always collides with itself (same pattern, same methods).
    // One finding for the node, naming every place. The line to change
    // is the include where the second place parts from the first
    // (`two = @include(...)`), not the shared file both read.
    for claim in claims.iter().filter(|claim| claim.places.len() > 1) {
        let (first, second) = (&claim.places[0].path, &claim.places[1].path);
        let parted = second
            .iter()
            .zip(first.iter())
            .position(|(a, b)| a != b)
            .expect("two places of one node part somewhere: a file cannot include itself, so neither path is a prefix of the other");
        let site = project
            .groups
            .iter()
            .find(|group| group.id == second[parted])
            .expect("every site on a place's path is a call site group");
        // Wrong only where both places run: the node and the two sites its
        // places part at.
        push(
            d,
            &[claim.node.part(), first[parted].as_str(), second[parted].as_str()],
            site.source_file.as_deref(),
            site.header_span.or(site.span).unwrap_or_default(),
            Severity::Error,
            "route-overlap",
            format!(
                "{} each serve '{}': they are the same node, in a file included at each of \
                 those places, so a call has no way to pick one. Include the file once, or \
                 wire the node's `{}` from an input of the file so each include gives its own",
                weft_core::comma_list::spell_list(&addresses(claim)),
                claim.pattern.as_str(),
                claim.field,
            ),
        );
    }

    // Each pair of different nodes reported once, on the one written
    // later (by its first place): the other was already there, so the
    // finding sits where the author just typed.
    claims.sort_by(|a, b| a.places[0].order.cmp(&b.places[0].order));
    for (index, claim) in claims.iter().enumerate() {
        for earlier in claims[..index].iter().filter(|earlier| collide(claim, earlier)) {
            // Wrong only in a program serving both.
            let (file, span) = cfg_anchor(claim.node, claim.field);
            push(
                d,
                &[earlier.node.part(), claim.node.part()],
                file,
                span,
                Severity::Error,
                "route-overlap",
                format!(
                    "'{}' is served by {}, and '{}' by {}. A call can reach either, and \
                     neither is the more specific, so there is no answer to give. One of \
                     them has to spell out what the other captures (a literal segment \
                     where the other has '{{name}}'), or they need different paths or \
                     different methods",
                    claim.pattern.as_str(),
                    weft_core::comma_list::spell_list(&addresses(claim)),
                    earlier.pattern.as_str(),
                    weft_core::comma_list::spell_list(&addresses(earlier)),
                ),
            );
        }
    }
}

/// One route claim a node makes: what `check_route_claims` compares.
struct Claim<'a> {
    node: &'a NodeDefinition,
    field: &'a str,
    pattern: weft_core::route::RoutePattern,
    methods: Vec<String>,
    /// Every place the node is at, in source order ([`placed`]).
    places: Vec<ClaimPlace>,
}

/// One place a claiming node is at.
struct ClaimPlace {
    /// The call path: empty outside every included file, else the sites
    /// that reach the node, outermost first.
    path: Vec<String>,
    /// Where the place is written: the start of each site on the path
    /// and then of the node itself, each in the file it is written in.
    /// Two places are equal up to the first site where they part, and
    /// from there on both entries sit in the same file, so comparing
    /// these compares the order the author wrote them in. A program
    /// built without source has no spans, and its places keep the order
    /// the graph lists them in.
    order: Vec<(usize, usize)>,
}

/// route-capture-shadows-output: on such a node, a capture may not share
/// a name with an output the node's metadata declares (`{body}` on a
/// Route), or it could never be read.
/// route-output-not-a-capture: on a node whose added output ports read
/// its path's captures (`claimsRoute.capturesAreOutputs`), an added port
/// the written path does not capture could never fill, so it is refused
/// where the path is written; and a capture is text, so the port takes
/// a `String`.
fn check_capture_outputs(
    node: &NodeDefinition,
    meta: &weft_core::node::NodeMetadata,
    pattern: &weft_core::route::RoutePattern,
    path_field: &str,
    d: &mut Vec<Diagnostic>,
) {
    for output in node.outputs.iter().filter(|output| !meta.outputs.iter().any(|o| o.name == output.name)) {
        if let Some(problem) = pattern.capture_port_problem(&output.name, &output.port_type) {
            let message = format!("'{}' {problem}", author_name(node));
            cfg_push(d, node, path_field, Severity::Error, "route-output-not-a-capture", message);
        }
    }
    // A capture named like one of the node's own outputs could never be
    // read: a port of that name is the node's output, and declaring it
    // only types that output.
    for capture in pattern.captures() {
        if meta.outputs.iter().any(|o| o.name == capture) {
            cfg_push(
                d,
                node,
                path_field,
                Severity::Error,
                "route-capture-shadows-output",
                format!(
                    "'{}' path '{}' captures `{capture}`, which is also the name of one of its own \
                     outputs, so the capture could never be read: rename the capture",
                    author_name(node),
                    pattern.as_str(),
                ),
            );
        }
    }
}

/// `claims` with the places each node is at filled in, each claim's
/// places in source order. A program with no claims never indexes its
/// graph.
fn placed<'p>(project: &'p ProjectDefinition, mut claims: Vec<Claim<'p>>) -> Vec<Claim<'p>> {
    if claims.is_empty() {
        return claims;
    }
    let mut at: std::collections::HashMap<&str, Vec<Vec<String>>> =
        claims.iter().map(|claim| (claim.node.id.as_str(), Vec::new())).collect();
    for place in weft_core::project::places::every_place(project) {
        if let Some(paths) = at.get_mut(place.id.as_str()) {
            paths.push(place.path);
        }
    }
    let groups: std::collections::HashMap<&str, &weft_core::project::GroupDefinition> =
        project.groups.iter().map(|group| (group.id.as_str(), group)).collect();
    let start = |span: Option<Span>| span.map(|s| (s.start_line, s.start_column)).unwrap_or_default();
    for claim in &mut claims {
        let node = claim.node;
        claim.places = at
            .remove(node.id.as_str())
            .unwrap_or_default()
            .into_iter()
            .map(|path| {
                let order = path
                    .iter()
                    .map(|site| {
                        let site = groups.get(site.as_str()).expect("every site on a place's path is a group");
                        start(site.header_span.or(site.span))
                    })
                    .chain(std::iter::once(start(node.header_span.or(node.span))))
                    .collect();
                ClaimPlace { path, order }
            })
            .collect();
        claim.places.sort_by(|a, b| a.order.cmp(&b.order));
    }
    claims
}

/// named-type-conflict: nominal compatibility compares the NAME alone
/// ("one name, one body" is the contract), so every restatement of a
/// named type across the project's ports must carry the same body, and
/// must match the registry's declaration when one is in scope. This is
/// the AUTHORING half of the guarantee; the parser only enforces the
/// registry-independent half (a body must be concrete), because
/// deserialization must stay a pure function of the string (a stored
/// project keeps parsing after its declaration is edited).
fn check_named_type_conflicts(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    fn walk<'t>(ty: &'t WeftType, out: &mut Vec<(&'t str, &'t WeftType)>) {
        match ty {
            WeftType::Named { name, body } => {
                out.push((name, body));
                walk(body, out);
            }
            WeftType::List(inner) | WeftType::Generator(inner) | WeftType::Dict(inner) => walk(inner, out),
            WeftType::Union(members) => members.iter().for_each(|m| walk(m, out)),
            WeftType::Record { fields, .. } => fields.iter().for_each(|f| walk(&f.ty, out)),
            _ => {}
        }
    }

    let registry = weft_core::weft_type::TypeRegistry::current();
    // Per name, its first body, where it was written, and the node there:
    // two bodies clash only in a program holding both nodes, so a cut
    // keeping one of them sees one body.
    let mut seen: std::collections::HashMap<&str, (&WeftType, String, &str)> =
        std::collections::HashMap::new();
    for node in &project.nodes {
        let span = node.header_span_or_default();
        let file = node.source_file.as_deref();
        let ports = node
            .inputs
            .iter()
            .map(|p| (&p.name, &p.port_type))
            .chain(node.outputs.iter().map(|p| (&p.name, &p.port_type)));
        for (port_name, port_type) in ports {
            let mut named = Vec::new();
            walk(port_type, &mut named);
            for (name, body) in named {
                let site = format!("{}.{}", author_name(node), port_name);
                if let Some(weft_core::weft_type::WeftType::Named { body: declared, .. }) =
                    registry.lookup(name)
                {
                    if declared.as_ref() != body {
                        push(d, &[node.part()], file, span, Severity::Error, "named-type-conflict",
                            format!(
                                "port '{site}' restates type `{name}` as `{body}`, but \
                                 it is declared as `{declared}`; a named type has one \
                                 body everywhere"
                            ));
                        continue;
                    }
                }
                match seen.get(name) {
                    None => {
                        seen.insert(name, (body, site, node.part()));
                    }
                    Some((first_body, first_site, first_node)) if *first_body != body => {
                        push(d, &[*first_node, node.part()], file, span, Severity::Error, "named-type-conflict",
                            format!(
                                "type `{name}` appears with two different bodies: \
                                 `{first_body}` at '{first_site}' and `{body}` at \
                                 '{site}'; a named type has one body everywhere"
                            ));
                    }
                    Some(_) => {}
                }
            }
        }
    }
}

/// `Generator[T]` wiring rules. A stream is a typed, one-directional,
/// live edge between exactly two running node bodies, which pins down
/// three things the rest of the type system does not:
///
/// - generator-multiple-consumers: a Generator OUTPUT feeds exactly ONE
///   input. Two consumers on one stream is a question with no defined
///   answer (do both see every item, or do they race?); fanning out is
///   Bus territory. (Many-to-one needs nothing here: the general
///   `duplicate-input-port` rule already forbids two edges into any
///   input.)
/// - generator-through-group: a stream cannot cross a plain Group's
///   port (a group port carries ONE value, not a live item flow) nor a
///   loop's boundary except as the loop's own `over`
///   port (checked by `check_loop_config`). Put the producer and its
///   consumer in the same scope.
/// - generator-in-container: `Generator[T]` is a PORT type only. Nested
///   inside a List/Dict/Record/Union it would make the stream a value
///   to store or copy, which it is not.
fn check_generator_wiring(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    /// Any `Generator` anywhere in `ty` (nominal bodies included).
    fn contains_generator(ty: &WeftType) -> bool {
        match ty {
            WeftType::Generator(_) => true,
            WeftType::List(inner) | WeftType::Dict(inner) => contains_generator(inner),
            WeftType::Union(members) => members.iter().any(contains_generator),
            WeftType::Record { fields, .. } => fields.iter().any(|f| contains_generator(&f.ty)),
            WeftType::Named { body, .. } => contains_generator(body),
            _ => false,
        }
    }

    /// A `Generator` anywhere BELOW the root position. The root itself
    /// may be a stream (directly, or through a nominal alias like
    /// `type Rows = Generator[Number]`, which `as_generator` peels);
    /// any deeper occurrence makes the stream a storable value, which
    /// it is not.
    fn generator_below_root(ty: &WeftType) -> bool {
        match ty.as_generator() {
            Some(element) => contains_generator(element),
            None => contains_generator(ty),
        }
    }

    let by_id: std::collections::HashMap<&str, &NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    for node in &project.nodes {
        let span = node.header_span_or_default();
        let file = node.source_file.as_deref();
        for port in &node.inputs {
            if generator_below_root(&port.port_type) {
                push(d, &[node.part()], file, span, Severity::Error, "generator-in-container",
                    format!(
                        "input '{}.{}: {}': Generator[T] is a port type; a stream cannot \
                         sit inside a list, dict, record, or union",
                        author_name(node), port.name, port.port_type
                    ));
            }
            // An unwired stream has no meaning: a stream cannot be
            // defaulted and has no zero value (the port's runtime
            // value is a live handle the engine installs), so a
            // Generator input must always be wired, which is what
            // `required` without a default guarantees. A LoopIn's
            // `required` mirrors the user's own `over` declaration (a
            // stream `over` port is legal), so it stays under this rule:
            // an optional stream `over` would otherwise compile as an
            // unwirable loop.
            if port.port_type.as_generator().is_some()
                && (!port.required || port.default.is_some())
            {
                push(d, &[node.part()], file, span, Severity::Error, "generator-input-must-be-required",
                    format!(
                        "input '{}.{}': a Generator input must be required and cannot \
                         have a default; a stream has no zero value, so an unwired \
                         stream port could never be satisfied",
                        author_name(node), port.name
                    ));
            }
        }
        for port in &node.outputs {
            if generator_below_root(&port.port_type) {
                push(d, &[node.part()], file, span, Severity::Error, "generator-in-container",
                    format!(
                        "output '{}.{}: {}': Generator[T] is a port type; a stream cannot \
                         sit inside a list, dict, record, or union",
                        author_name(node), port.name, port.port_type
                    ));
            }
            if port.port_type.as_generator().is_some() {
                let consumers = project
                    .edges
                    .iter()
                    .filter(|e| {
                        e.source == node.id && e.source_handle.as_deref() == Some(&port.name)
                    })
                    .count();
                if consumers > 1 {
                    push(d, &[node.part()], file, span, Severity::Error, "generator-multiple-consumers",
                        format!(
                            "stream '{}.{}' feeds {consumers} inputs; a Generator output \
                             connects to exactly ONE consumer (a stream has one taker). \
                             To broadcast, use a Bus",
                            author_name(node), port.name
                        ));
                }
            }
        }
    }

    // A group's port passes one value per run of the group, so a stream's
    // items cannot flow through it. The ban lives on the PORT, not on the
    // wires: a stream-typed port of a group is the crossing itself,
    // whichever side is wired (the project's own interface has no wire
    // from outside at all, so a wire-based rule would let a stream-typed
    // project input compile clean and ship an unsatisfiable graph). One
    // port, one diagnostic, by construction. The project's interface (the
    // root anonymous component's ports) gets its own wording: there IS no
    // outer scope to move the producer into. A loop is not among them: it
    // consumes a stream through its own `over`.
    let root_group_ids: std::collections::HashSet<&str> =
        component_root(project).into_iter().collect();
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        let span = group.header_span.or(group.span).unwrap_or_default();
        let file = group.source_file.as_deref();
        let inputs: Vec<&weft_core::project::PortDefinition> = group.in_ports.iter().map(|p| &p.port).collect();
        let outputs: Vec<&weft_core::project::PortDefinition> = group.out_ports.iter().collect();
        for (side, ports) in [("input", inputs), ("output", outputs)] {
            for port in ports.into_iter().filter(|p| p.port_type.as_generator().is_some()) {
                let port_name = &port.name;
                if root_group_ids.contains(group.id.as_str()) {
                    push(d, &[group.id.as_str()], file, span, Severity::Error, "generator-through-group",
                        format!(
                            "'{port_name}' declares a stream on the project's {side} boundary; a \
                             run's inputs and outputs are values, not live edges. \
                             Produce (or consume) the stream inside the project instead",
                        ));
                } else {
                    push(d, &[group.id.as_str()], file, span, Severity::Error, "generator-through-group",
                        format!(
                            "'{port_name}' carries a stream across the boundary of group \
                             '{}'; a stream is a live edge between two running nodes. Put the \
                             producer and its consumer in the same scope (a loop consumes a \
                             stream only through its own `over`)",
                            weft_core::project::plain_id(&group.id),
                        ));
                }
            }
        }
    }

    for edge in &project.edges {
        let (Some(src), Some(tgt)) =
            (by_id.get(edge.source.as_str()), by_id.get(edge.target.as_str()))
        else {
            continue;
        };
        let src_generator = src
            .outputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref())
            .is_some_and(|p| p.port_type.as_generator().is_some());
        let tgt_generator = tgt
            .inputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref())
            .is_some_and(|p| p.port_type.as_generator().is_some());
        if !src_generator && !tgt_generator {
            continue;
        }
        let span = edge.span.unwrap_or_default();
        let file = edge.source_file.as_deref();
        let wire = [src.part(), tgt.part()];
        // A stream flowing into a generic `T` port would instantiate
        // the variable with a live handle nobody pulls (the consumer
        // receives the raw marker and hangs the producer at its buffer
        // cap); the enrich pass refuses the binding, and this rule
        // names the real problem at the edge.
        if src_generator && !tgt_generator {
            // "Generic" is ANY unresolved leaf, at any depth (`T`,
            // `List[T]`, `Dict[String, T]`): enrich
            // refused the binding for all of them (a stream never
            // instantiates a typevar), and check_type_compat stays
            // quiet on this edge because THIS rule owns it, so the
            // predicates must be one condition, not two that drift.
            let tgt_port = tgt
                .inputs
                .iter()
                .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref());
            let tgt_generic =
                tgt_port.is_some_and(|p| p.port_type.contains_unresolved_leaf());
            if tgt_generic {
                push(d, &wire, file, span, Severity::Error, "generator-into-generic-port",
                    format!(
                        "edge '{}.{} -> {}.{}': the source is a stream, and the target \
                         declares '{}', a generic type that cannot consume one. A stream \
                         needs a port declared Generator[T]; to inspect items, consume \
                         the stream in a node and emit what you want to see",
                        author_name_by_id(project, &edge.source),
                        edge.source_handle.as_deref().unwrap_or("?"),
                        author_name_by_id(project, &edge.target),
                        edge.target_handle.as_deref().unwrap_or("?"),
                        tgt_port.map(|p| p.port_type.to_string()).unwrap_or_default(),
                    ));
                continue;
            }
        }
        // The mirror direction: a generic SOURCE cannot produce the
        // live stream a `Generator` input needs (enrich refuses that
        // binding too), and "connect it to something concrete" would
        // mislead here exactly as it would above.
        if tgt_generator && !src_generator {
            let src_port = src
                .outputs
                .iter()
                .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref());
            let src_generic =
                src_port.is_some_and(|p| p.port_type.contains_unresolved_leaf());
            if src_generic {
                push(d, &wire, file, span, Severity::Error, "generator-into-generic-port",
                    format!(
                        "edge '{}.{} -> {}.{}': the target is a stream port, and the \
                         source declares '{}', a generic type that cannot produce one. \
                         Declare the source output as Generator[T] (or feed the port \
                         from a real stream producer)",
                        author_name_by_id(project, &edge.source),
                        edge.source_handle.as_deref().unwrap_or("?"),
                        author_name_by_id(project, &edge.target),
                        edge.target_handle.as_deref().unwrap_or("?"),
                        src_port.map(|p| p.port_type.to_string()).unwrap_or_default(),
                    ));
                continue;
            }
        }
        // Loop boundaries: a stream INTO LoopOut (a gather/carry) has
        // no meaning, and a stream OUT of a LoopOut cannot exist
        // (nothing inside can produce one across the boundary); the
        // LoopIn side's rules live in `check_loop_config`
        // (`generator-not-iterated`), so it is deliberately NOT
        // re-reported here. A group's ports are refused a stream on the
        // group itself above, not per edge.
        // The target arm needs no generator guard: the early return
        // above already proved one endpoint of this edge is a stream.
        let banned_endpoint =
            tgt.node_type == weft_core::project::boundary_types::LOOP_OUT || (src.node_type == weft_core::project::boundary_types::LOOP_OUT && src_generator);
        if banned_endpoint {
            push(d, &wire, file, span, Severity::Error, "generator-through-group",
                format!(
                    "edge '{}.{} -> {}.{}' carries a stream across a loop boundary; a \
                     stream is a live edge between two running nodes. Put the producer \
                     and its consumer in the same scope (a loop consumes a stream only \
                     through its own `over`)",
                    author_name_by_id(project, &edge.source),
                    edge.source_handle.as_deref().unwrap_or("?"),
                    author_name_by_id(project, &edge.target),
                    edge.target_handle.as_deref().unwrap_or("?"),
                ));
        }
    }
}

// There is deliberately NO compile-time permission check: source holds
// only a connection id, so source alone cannot say what a connection
// actually holds. The check lives where the answer does: live in the
// editor when a connection is picked, at connect time, and at run-time
// resolution (the drift backstop).

/// Structural rules that make the trigger/fire semantics well-defined.
///
/// graph-cycle: the wire graph must be a DAG. Iteration is the Loop
///   container's job and cross-node feedback is the bus's; a wire cycle
///   has no run order and would wedge the engine.
/// trigger-in-loop: a trigger inside a Loop container. Registration
///   happens once per project and fires land outside any iteration, so
///   a per-iteration trigger has no meaning.
/// infra-in-loop: an infra node inside a Loop container. Provisioning
///   happens once per project, before any run; a body that provisions
///   once per item has no meaning either.
/// trigger-into-trigger: a trigger wired (directly or transitively)
///   into another trigger. The downstream trigger's inputs snapshot at
///   setup and its upstream is never walked at fire, so the wiring
///   could never deliver; reject until the semantics are designed.
/// trigger-into-infra: an infra node downstream of a trigger.
///   Provisioning happens before any fire exists, so the value can
///   never be there.
fn check_graph_shape(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use weft_core::frames::Located;
    use weft_core::project::places;

    // The graph as it RUNS, over places: a body is one place per call of
    // it, so two sites including the same file are two copies and never
    // one loop (`two.raw = one.cleaned` with `one` and `two` including the
    // same file is a chain of two calls, not a cycle). A wire onto a
    // group's gate leads to the group, and the group to everything in it:
    // a member feeding its own group's gate waits on itself.
    let graph = weft_core::project::graph::ProjectGraph::new(project);
    let every: BTreeSet<Located> = places::every_place_in(&graph);
    let mut outgoing: BTreeMap<Located, Vec<Located>> = BTreeMap::new();
    let mut scopes: BTreeSet<Located> = BTreeSet::new();
    for place in &every {
        for (_, target) in places::outgoing(&graph, place) {
            if project.groups.iter().any(|g| g.id == target.id) {
                scopes.insert(target.clone());
            }
            outgoing.entry(place.clone()).or_default().push(target);
        }
    }
    for scope in &scopes {
        outgoing.entry(scope.clone()).or_default().extend(places::members_in(&graph, &scope.id, &scope.path));
    }
    let spell = |place: &Located| weft_core::project::group_address(project, &place.id, &place.path);
    let anchor = |place: &Located| -> (Option<&str>, Span) {
        match project.nodes.iter().find(|n| n.id == place.id) {
            Some(node) => (node.source_file.as_deref(), node.header_span_or_default()),
            None => project.groups.iter().find(|g| g.id == place.id)
                .map(|g| (g.source_file.as_deref(), g.header_span.or(g.span).unwrap_or_default()))
                .unwrap_or_default(),
        }
    };

    // graph-cycle: iterative DFS with a three-color marking; report each
    // place where a back edge is found (one diagnostic per cycle entry).
    let mut state: BTreeMap<Located, u8> = BTreeMap::new(); // 0 absent, 1 in-stack, 2 done
    let mut reported: HashSet<String> = HashSet::new();
    for start in every.iter().chain(&scopes) {
        if state.contains_key(start) {
            continue;
        }
        let mut stack: Vec<(Located, usize)> = vec![(start.clone(), 0)];
        state.insert(start.clone(), 1);
        while let Some((place, idx)) = stack.pop() {
            let next = outgoing.get(&place).and_then(|v| v.get(idx)).cloned();
            match next {
                Some(child) => {
                    stack.push((place, idx + 1));
                    match state.get(&child) {
                        Some(1) => {
                            let through = spell(&child);
                            if reported.insert(through.clone()) {
                                let (file, span) = anchor(&child);
                                // The cycle is wrong only with every part on
                                // it kept: the stack from the place the back
                                // edge returns to up to the one it leaves.
                                let from = stack.iter().position(|(on, _)| *on == child)
                                    .expect("an in-stack place is on the stack");
                                let on_cycle: Vec<&str> = stack[from..].iter().map(|(on, _)| part_by_id(project, &on.id)).collect();
                                push(d, &on_cycle, file, span, Severity::Error, "graph-cycle",
                                    format!(
                                        "the wires form a cycle through '{through}'; a wire graph \
                                         must be acyclic (iterate with a Loop, exchange feedback \
                                         over a bus)"
                                    ));
                            }
                        }
                        Some(_) => {}
                        None => {
                            state.insert(child.clone(), 1);
                            stack.push((child, 0));
                        }
                    }
                }
                None => {
                    state.insert(place, 2);
                }
            }
        }
    }

    // trigger-in-loop: a trigger with a loop among the scopes it runs in
    // (around it in its file, or around a site on its call path). The
    // same applies to infra: both are set up once per project, and a
    // setup phase that reached one inside a loop would run the loop and
    // set the node up once per item. A GROUP is fine for both: a trigger
    // in the middle of a group fires there and its run starts from it.
    let is_loop = |id: &str| project.groups.iter().any(|g| g.id == id && matches!(g.kind, weft_core::project::GroupKind::Loop { .. }));
    let mut told: HashSet<&str> = HashSet::new();
    for place in every.iter().filter(|place| project.nodes.iter().any(|n| n.id == place.id && (n.features.is_trigger || n.requires_infra))) {
        let node = project.nodes.iter().find(|n| n.id == place.id).expect("filtered on a node");
        let around = places::scopes_around(&graph, place);
        let Some(in_loop) = around.iter().find(|scope| is_loop(&scope.id)) else { continue };
        if !told.insert(node.id.as_str()) { continue; }
        let what = if node.features.is_trigger { "trigger" } else { "infra node" };
        let why = if node.features.is_trigger {
            "a trigger registers once and fires outside any iteration, so it cannot live in a loop body"
        } else {
            "infra is provisioned once for the project, not once per item, so it cannot live in a loop body"
        };
        let code = if node.features.is_trigger { "trigger-in-loop" } else { "infra-in-loop" };
        // A loop around a site: the node's own file shows no loop, so the
        // message names the site and the loop. A scope listed at a shorter
        // path than the node's sits around the site at that depth.
        let through_a_site = (in_loop.path.len() < place.path.len()).then(|| {
            let depth = in_loop.path.len();
            weft_core::project::address_of(project, &place.path[depth], &place.path[..depth])
        });
        let message = match through_a_site {
            Some(site) => format!(
                "{what} '{}' is in an included file that '{site}' includes from inside the Loop '{}'; {why}",
                author_name(node).rsplit('.').next().unwrap_or_default(),
                in_loop.id,
            ),
            None => format!("{what} '{}' is inside a Loop; {why}", author_name(node)),
        };
        push(d, &[node.part()], node.source_file.as_deref(), node.header_span_or_default(), Severity::Error, code, message);
    }

    // trigger-into-trigger / trigger-into-infra: walk downstream from each
    // trigger along the wires (a gate decides whether a group runs; it
    // delivers nothing to what is inside).
    let is_trigger = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.features.is_trigger);
    let is_infra = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra);
    let mut flagged: HashSet<(String, String, &str)> = HashSet::new();
    for trigger in every.iter().filter(|place| is_trigger(place)) {
        let mut seen: BTreeSet<Located> = BTreeSet::new();
        let mut frontier: Vec<Located> = outgoing.get(trigger).cloned().unwrap_or_default();
        while let Some(place) = frontier.pop() {
            if scopes.contains(&place) || !seen.insert(place.clone()) {
                continue;
            }
            let (file, span) = anchor(&place);
            let (from, to) = (spell(trigger), spell(&place));
            // The two ends: the wires between them are only in a cut that
            // keeps both, and naming those too would need the path.
            let ends = [part_by_id(project, &trigger.id), part_by_id(project, &place.id)];
            if is_trigger(&place) && flagged.insert((from.clone(), to.clone(), "trigger")) {
                push(d, &ends, file, span, Severity::Error, "trigger-into-trigger",
                    format!(
                        "trigger '{from}' is wired into trigger '{to}'; a trigger's \
                         inputs snapshot at setup, so another trigger's output can never \
                         reach it"
                    ));
            }
            if is_infra(&place) && flagged.insert((from.clone(), to.clone(), "infra")) {
                push(d, &ends, file, span, Severity::Error, "trigger-into-infra",
                    format!(
                        "trigger '{from}' is wired into infra node '{to}'; \
                         provisioning happens before any fire exists, so the value can \
                         never be there"
                    ));
            }
            frontier.extend(outgoing.get(&place).cloned().unwrap_or_default());
        }
    }
}

/// A node or group named after a known node type is ambiguous: a reference
/// like `MyName.port` parses as an INLINE node of that type, not a reference to
/// the declaration. Flag it on the declaration line. The catalog is dynamic, so
/// this lives here (where the catalog is in hand) rather than in the
/// catalog-agnostic parser; the structural keywords `Group`/`Loop` are
/// rejected in the parser instead, since they are not catalog entries.
fn check_reserved_names(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    out: &mut Vec<Diagnostic>,
) {
    // Compare the LOCAL name, not the scoped id: a node inside a group has id
    // `grp.Llm`, but the ambiguous reference written in source is the local
    // `Llm.port`, and `grp.Llm` would never match a catalog entry. The local
    // segment is what collides with a type name.
    let mut flag = |id: &str, file: Option<&str>, span: Option<weft_core::project::Span>| {
        let local = id.rsplit('.').next().unwrap_or(id);
        if catalog.lookup(local).is_some() {
            push(
                out,
                &[id],
                file,
                span.unwrap_or_default(),
                Severity::Error,
                "reserved-name",
                format!("'{local}' is a node type name and cannot be used as a node or group name (a reference like '{local}.port' would parse as an inline node)"),
            );
        }
    };
    for node in &project.nodes {
        flag(&node.id, node.source_file.as_deref(), node.header_span);
    }
    for group in &project.groups {
        flag(&group.id, group.source_file.as_deref(), group.header_span);
    }
}

/// Evaluate each node's declarative `validate` rules from its
/// metadata, emit Diagnostics for rules that fire. Safe by
/// construction: the grammar is closed, no user Rust runs here.
fn check_declarative_rules(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    mode: ValidationMode,
    out: &mut Vec<Diagnostic>,
) {
    // A field written `@instance_filled` needs no special case here: the
    // evaluator reads it as present with a value nobody knows yet, so a
    // "no connection picked" or "has no model" rule holds off, and a rule
    // about the value's content waits for the instance's value, when it is
    // asked again with that value in (`weft_core::rules`).
    let rules = weft_core::rules::RuleContext::new(project);
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let custom = weft_core::rules::CustomPorts::of(node, meta);
        for rule in &node_rules(meta, node) {
            // Skip runtime-only rules in structural mode (the build and
            // the Problems panel). Runtime mode runs them: the editor's
            // pre-flight gate before Run/Activate/Resync, and the
            // terminal `weft validate`.
            if matches!(rule.then.level, ValidationLevel::Runtime)
                && mode == ValidationMode::Structural
            {
                continue;
            }
            if weft_core::rules::fires(rule, node, &rules, &custom) {
                emit_rule_diagnostic(node, &rules, &custom, rule, out);
            }
        }
    }
}

/// Every rule a node of this type answers to: the ones its metadata
/// declares, and the one the language adds. Every access node requires a
/// connection BY DEFAULT: declaring a `service` recipe is what makes a
/// node an access node, and running one without a connection picked
/// always fails, so the LANGUAGE synthesizes that runtime rule instead of
/// every node's metadata restating it. Opt-out for the genuine
/// can-run-unauthenticated case: `connectionOptional: true` on the
/// recipe. The ONE list both the compiler's check and an instance's value
/// check (`InstanceRules`, filled at enrich) read.
pub(crate) fn node_rules(meta: &weft_core::node::NodeMetadata, node: &NodeDefinition) -> Vec<ValidationRule> {
    let mut rules = meta.validate.clone();
    rules.extend(implicit_connection_rule(meta, node));
    rules
}

/// The synthesized "no connection picked" rule for an access node, None
/// when the node is not one or its recipe declares the connection
/// optional. Built as an ordinary [`ValidationRule`] so it evaluates and
/// emits through exactly the paths a declared rule uses.
fn implicit_connection_rule(
    meta: &weft_core::node::NodeMetadata,
    node: &NodeDefinition,
) -> Option<ValidationRule> {
    let spec = meta.service.as_ref()?;
    if spec.connection_optional {
        return None;
    }
    let input = meta.access_input()?;
    // A metadata that declares the EXACT rule being synthesized keeps
    // its own copy alone (a project's copied catalog predating the
    // synthesis would otherwise report one mistake twice).
    if meta.validate.iter().any(|rule| is_the_connection_rule(meta, rule)) {
        return None;
    }
    Some(ValidationRule {
        when: Condition::Not {
            of: Box::new(Condition::ConfigNonempty { field: input.name.clone() }),
        },
        then: RuleDiagnostic {
            message: format!(
                "{} '{{id}}' has no {} connection picked; connect one on the node.",
                node.node_type,
                spec.display_label(),
            ),
            level: ValidationLevel::Runtime,
            severity: RuleSeverity::Error,
            port: None,
            field: Some(input.name.clone()),
        },
    })
}

/// Whether `rule` is exactly the "no connection picked" rule on the
/// node's picker. Narrow to that exact shape: an UNRELATED declared rule
/// on the picker field (a scope check, say) is not it.
fn is_the_connection_rule(meta: &weft_core::node::NodeMetadata, rule: &ValidationRule) -> bool {
    let Some(input) = meta.access_input() else { return false };
    rule.then.field.as_deref() == Some(input.name.as_str())
        && matches!(
            &rule.when,
            Condition::Not { of }
                if matches!(of.as_ref(), Condition::ConfigNonempty { field } if field == &input.name)
        )
}

fn emit_rule_diagnostic(
    node: &NodeDefinition,
    rules: &weft_core::rules::RuleContext,
    custom: &weft_core::rules::CustomPorts,
    rule: &ValidationRule,
    out: &mut Vec<Diagnostic>,
) {
    let span = node.header_span_or_default();
    let severity = match rule.then.severity {
        RuleSeverity::Error => Severity::Error,
        RuleSeverity::Warning => Severity::Warning,
        RuleSeverity::Info => Severity::Info,
        RuleSeverity::Hint => Severity::Hint,
    };
    let message = weft_core::rules::message(rule, node, rules, custom);
    let code = match rule.then.level {
        ValidationLevel::Structural => "rule-structural",
        ValidationLevel::Runtime => "rule-runtime",
    };
    push(out, &[node.part()], node.source_file.as_deref(), span, severity, code, message);
}

/// `about` is what the finding is about (`Diagnostic::subjects`), by part
/// id (`NodeDefinition::part`, [`part_by_id`]): the node, the group, or a wire's two
/// ends, every one of which a cut has to keep to hit the problem. Passed
/// at every call, so no finding can forget it; empty only where no part is
/// there to name, which stops every command.
fn push(
    d: &mut Vec<Diagnostic>,
    about: &[&str],
    file: Option<&str>,
    span: Span,
    severity: Severity,
    code: &str,
    message: impl Into<String>,
) {
    // One construction path: a span -> the four Diagnostic position fields. The
    // span is the offending node/edge/field's own range (its full extent), so
    // validate diagnostics get the same ranged underlines the parse layer does.
    // `file` is the anchor's `source_file` (set when an @include spliced it
    // out of another file), so the finding names the file its coordinates
    // live in. A part named twice (a wire between a loop's two boundaries)
    // is one subject.
    let mut subjects: Vec<&str> = Vec::with_capacity(about.len());
    for id in about {
        if !subjects.contains(id) {
            subjects.push(id);
        }
    }
    d.push(Diagnostic::at(span, severity, code, message).in_file(file).about(subjects));
}

/// `NodeDefinition::part` for an id a wire names: a node's part, or the id
/// as it is (a group's, for a wire at its port or gate).
pub(crate) fn part_by_id<'a>(project: &'a ProjectDefinition, id: &'a str) -> &'a str {
    project.nodes.iter().find(|n| n.id == id).map(NodeDefinition::part).unwrap_or(id)
}

/// The anchor for a finding about one CONFIG entry: the entry's own
/// span with the entry's OWN file taken verbatim (a loop's boundary node
/// holds spans from more than one file, and an entry's None means the
/// compiled source even when the node's file differs), falling back to
/// the node's header span + file when no entry exists. Every
/// config-anchored push takes both halves from this ONE call, so no
/// site can pair a span from one object with a file from another.
fn cfg_anchor<'a>(node: &'a NodeDefinition, key: &str) -> (Option<&'a str>, Span) {
    match node.written_span(key) {
        Some(s) => (s.source_file.as_deref(), s.span),
        None => (node.source_file.as_deref(), node.header_span_or_default()),
    }
}

/// [`push`] with both anchor halves taken from [`cfg_anchor`] for `key`.
fn cfg_push(
    d: &mut Vec<Diagnostic>,
    node: &NodeDefinition,
    key: &str,
    severity: Severity,
    code: &str,
    message: impl Into<String>,
) {
    let (file, span) = cfg_anchor(node, key);
    push(d, &[node.part()], file, span, severity, code, message);
}

// ─── group 1: structural integrity ──────────────────────────────────────────

/// duplicate-node-id: two nodes with the same id. After flattening,
/// every node id is required to be globally unique (the parser rejects
/// same-scope dups at parse time, but scope-aware dups may still
/// collide post-flatten due to inline expressions, group renames, or
/// bad manual input).
fn check_duplicates(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let mut seen: std::collections::HashMap<&str, usize> = Default::default();
    for node in &project.nodes {
        let span = node.header_span_or_default();
        let file = node.source_file.as_deref();
        match seen.get(node.id.as_str()) {
            Some(first_line) => push(
                d,
                &[node.id.as_str()],
                file,
                span,
                Severity::Error,
                "duplicate-node-id",
                format!(
                    "duplicate node id '{}' (first declared at line {})",
                    author_name(node), first_line
                ),
            ),
            None => {
                seen.insert(&node.id, span.start_line);
            }
        }
    }
}

/// double-driven-port: an input port may have ONE driver. A port that is
/// wired from upstream AND set in the node body (a `port_literals` entry,
/// per the enrich normalization) has two, and which value wins would be
/// invisible at the call site; reject at compile time. Body values carry
/// no field defaults, so a hit is always an explicit assignment. A
/// same-named config FIELD on a braces-closed port is not a
/// driver (it lives in `config`, not `port_literals`) and stays legal.
fn check_double_driven_ports(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for node in &project.nodes {
        for name in node.port_literals.keys() {
            // A `null` literal drives nothing (`literal_fills`), so a
            // wired port beside `port: null` has ONE driver, not two.
            // `config-null-literal` is what speaks for the null itself.
            if !literal_fills(node, name) {
                continue;
            }
            let wired = project
                .edges
                .iter()
                .any(|e| e.target == node.id && e.target_handle.as_deref() == Some(name.as_str()));
            if wired {
                // The literal's span carries ITS OWN file: a fill written
                // in the INCLUDING file (`alias.port = v`) lands on a
                // loop's boundary node whose other spans live in the included
                // file, so the node's file would name the wrong buffer.
                let (span, file) = match node.port_literal_spans.get(name) {
                    Some(s) => (s.span, s.source_file.as_deref()),
                    None => (node.header_span_or_default(), node.source_file.as_deref()),
                };
                push(
                    d,
                    &[node.part()],
                    file,
                    span,
                    Severity::Error,
                    "double-driven-port",
                    format!(
                        "input '{}' of '{}' has two drivers: it is wired from upstream AND \
                         set in the node body; remove one",
                        name, author_name(node)
                    ),
                );
            }
        }
    }
    // A group's input written (`g.x = 5`, or in the include site's file
    // `alias.x = 5`) and also wired from outside.
    let wired_from_outside: std::collections::HashSet<(String, String)> = lines(project).into_iter()
        .filter_map(|line| match line.to {
            LineEnd::Port { group, port, side: weft_core::project::CrossingSide::In, inner: false } => Some((group, port)),
            _ => None,
        })
        .collect();
    // A loop's literals sit on its `LoopIn` too, where the node rule reads them.
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        for (name, value) in &group.port_literals {
            if value.is_null() { continue; }
            // A gate's wire lands on the group itself, no port of it.
            let wired = if weft_core::run::skip::is_gate_port(name) {
                project.edges.iter().any(|e| e.target == group.id && e.target_port() == name.as_str() && e.is_group_gate(project))
            } else {
                wired_from_outside.contains(&(group.id.clone(), name.clone()))
            };
            if !wired { continue; }
            let (span, file) = group_literal_anchor(group, name);
            push(
                d,
                &[group.id.as_str()],
                file,
                span,
                Severity::Error,
                "double-driven-port",
                format!(
                    "input '{name}' of '{}' has two drivers: it is wired from upstream AND written \
                     on the group; remove one",
                    weft_core::project::plain_id(&group.id)
                ),
            );
        }
    }
}

/// Where a literal on a group's port was written: its own span and file
/// (a fill written in the including file names that file), else the
/// group's header.
fn group_literal_anchor<'a>(group: &'a weft_core::project::GroupDefinition, port: &str) -> (Span, Option<&'a str>) {
    match group.port_literal_spans.get(port) {
        Some(s) => (s.span, s.source_file.as_deref()),
        None => (group.header_span.or(group.span).unwrap_or_default(), group.source_file.as_deref()),
    }
}

/// two-gates: a node has ONE gate. `_should_flow` and
/// `_should_not_flow` are the same decision read opposite ways, so a
/// node carrying both is asking for two answers to one question and
/// there is no sane rule for combining them. Refused loudly instead of
/// quietly picking one.
fn check_two_gates(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use weft_core::run::skip::{SHOULD_FLOW_PORT, SHOULD_NOT_FLOW_PORT};
    for node in &project.nodes {
        // `literal_fills`, not a raw key lookup: a `null` literal is
        // nothing, not a gate. `_should_not_flow: null` beside a real
        // `_should_flow` is ONE gate, and reporting it as two sent the
        // author looking for a second one that was never there (with
        // `config-null-literal` already telling them about the null).
        let driven = |port: &str| {
            literal_fills(node, port)
                || project.edges.iter().any(|e| {
                    e.target == node.id && e.target_handle.as_deref() == Some(port)
                })
        };
        if driven(SHOULD_FLOW_PORT) && driven(SHOULD_NOT_FLOW_PORT) {
            push(
                d,
                &[node.part()],
                node.source_file.as_deref(),
                node.header_span_or_default(),
                Severity::Error,
                "two-gates",
                format!(
                    "'{}' has both `_should_flow` and `_should_not_flow`; a node has one gate, \
                     so keep the one that says what you mean and drop the other",
                    author_name(node)
                ),
            );
        }
    }
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        let driven = |port: &str| {
            group.port_literals.get(port).is_some_and(|v| !v.is_null())
                || project.edges.iter().any(|e| e.target == group.id && e.target_port() == port && !e.to_group_port())
        };
        if driven(SHOULD_FLOW_PORT) && driven(SHOULD_NOT_FLOW_PORT) {
            push(
                d,
                &[group.id.as_str()],
                group.source_file.as_deref(),
                group.header_span.or(group.span).unwrap_or_default(),
                Severity::Error,
                "two-gates",
                format!(
                    "'{}' has both `_should_flow` and `_should_not_flow`; a group has one gate, \
                     so keep the one that says what you mean and drop the other",
                    weft_core::project::plain_id(&group.id)
                ),
            );
        }
    }
}

// ─── the lines a person wrote ───────────────────────────────────────────────

/// One end of a line a person wrote.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LineEnd {
    /// A node's port.
    Node { id: String, port: String },
    /// A group's port: an input (`In`) or an output (`Out`), seen from
    /// inside it (`self.x`) or from outside it (`g.x`).
    Port { group: String, port: String, side: weft_core::project::CrossingSide, inner: bool },
    /// A group's gate.
    Gate { group: String, port: String },
}

impl LineEnd {
    /// Whether this end sits inside `group`: a node or a group nested in
    /// it, or one of its own ports seen from inside.
    fn lies_in(&self, group: &str) -> bool {
        let nested = |id: &str| id.starts_with(&format!("{group}."));
        match self {
            LineEnd::Node { id, .. } => nested(id),
            LineEnd::Port { group: at, inner, .. } => nested(at) || (at == group && *inner),
            LineEnd::Gate { group: at, .. } => nested(at),
        }
    }
}

/// A line a person wrote, read back off the wires that carry it: a wire
/// through a group's ports is one wire and several lines (`g.x = a.out`
/// outside, `m.in = self.x` inside), and a line read by several wires is
/// one line.
#[derive(Debug, Clone)]
struct Line {
    from: LineEnd,
    to: LineEnd,
    span: Span,
    file: Option<String>,
}

/// Every line of `project`, each once, in the order it was written.
fn lines(project: &ProjectDefinition) -> Vec<Line> {
    use weft_core::project::CrossingSide;
    let kind = |id: &str| project.groups.iter().find(|g| g.id == id).map(|g| &g.kind);
    let calls = |site: &str, body: &str| matches!(kind(site), Some(weft_core::project::GroupKind::Call { body: called, .. }) if called == body);
    let entry = |c: &weft_core::project::Crossing| LineEnd::Port { group: c.group.clone(), port: c.port.clone(), side: c.side, inner: c.side == CrossingSide::Out };
    let exit = |c: &weft_core::project::Crossing| LineEnd::Port { group: c.group.clone(), port: c.port.clone(), side: c.side, inner: c.side == CrossingSide::In };
    let mut out: Vec<Line> = Vec::new();
    // A line is one line however many wires carry it: its two ends, and
    // where it was written.
    type Written = (LineEnd, LineEnd, (usize, usize), Option<String>);
    let mut seen: std::collections::HashSet<Written> = Default::default();
    let mut add = |line: Line| {
        let key = (line.from.clone(), line.to.clone(), (line.span.start_line, line.span.start_column), line.file.clone());
        if seen.insert(key) { out.push(line); }
    };
    for edge in &project.edges {
        let (mut from, start) = if edge.from_group_port() {
            (exit(&edge.via[0]), 1)
        } else {
            (node_end(project, &edge.source, edge.source_port(), true), 0)
        };
        for k in start..edge.via.len() {
            let c = &edge.via[k];
            // A site hands its inputs to the body it calls and takes the
            // body's outputs back by itself: nobody writes that line.
            let automatic = k > 0 && {
                let prev = &edge.via[k - 1];
                prev.side == c.side && match c.side {
                    CrossingSide::In => calls(&prev.group, &c.group),
                    CrossingSide::Out => calls(&c.group, &prev.group),
                }
            };
            if !automatic {
                add(Line { from: from.clone(), to: entry(c), span: c.span.unwrap_or_default(), file: c.source_file.clone() });
            }
            from = exit(c);
        }
        if edge.to_group_port() { continue; }
        let to = if project.groups.iter().any(|g| g.id == edge.target) {
            LineEnd::Gate { group: edge.target.clone(), port: edge.target_port().to_string() }
        } else {
            node_end(project, &edge.target, edge.target_port(), false)
        };
        add(Line { from, to, span: edge.span.unwrap_or_default(), file: edge.source_file.clone() });
    }
    // In the order they were written, so the first line into a port is the
    // one a later line is told about.
    out.sort_by(|a, b| (&a.file, a.span.start_line, a.span.start_column).cmp(&(&b.file, b.span.start_line, b.span.start_column)));
    out
}

/// A line's end on node `id`'s `port`, the line leaving it when `leaves`.
/// A loop's two boundary nodes are how its ports compile, and a line read
/// back is the loop's port, the way a group's is: `LoopIn` is its inputs
/// (`self.items` read inside, `each.items` written outside, `each._should_flow`
/// its gate) and `LoopOut` its outputs (`self.results` written inside,
/// `each.results` read outside). So every rule about a line holds for a
/// loop and a group alike.
fn node_end(project: &ProjectDefinition, id: &str, port: &str, leaves: bool) -> LineEnd {
    use weft_core::project::{CrossingSide, GroupBoundaryRole};
    let boundary = project.nodes.iter().find(|n| n.id == id).and_then(|n| n.group_boundary.as_ref());
    let Some(b) = boundary else { return LineEnd::Node { id: id.to_string(), port: port.to_string() } };
    let (group, port) = (b.group_id.clone(), port.to_string());
    match (b.role, leaves) {
        (GroupBoundaryRole::In, false) if weft_core::run::skip::is_gate_port(&port) => LineEnd::Gate { group, port },
        (GroupBoundaryRole::In, inner) => LineEnd::Port { group, port, side: CrossingSide::In, inner },
        (GroupBoundaryRole::Out, leaves) => LineEnd::Port { group, port, side: CrossingSide::Out, inner: !leaves },
    }
}

impl LineEnd {
    /// The part this end is on ([`part_by_id`]): a node's, or the group's
    /// whose port or gate it is.
    fn part<'a>(&'a self, project: &'a ProjectDefinition) -> &'a str {
        match self {
            LineEnd::Node { id, .. } => part_by_id(project, id),
            LineEnd::Port { group, .. } | LineEnd::Gate { group, .. } => group,
        }
    }
}

impl Line {
    /// The parts of both ends: a cut carries the line only when it keeps
    /// both.
    fn parts<'a>(&'a self, project: &'a ProjectDefinition) -> [&'a str; 2] {
        [self.from.part(project), self.to.part(project)]
    }
}

/// The scope a line's end is in: a node's innermost group (a loop's own
/// boundaries sit in the scope around their loop), a group port seen from
/// outside the group's parent's, from inside the group itself, and a gate
/// the scope around the group. `None` at the top.
fn end_scope<'a>(project: &'a ProjectDefinition, end: &'a LineEnd) -> Option<&'a str> {
    match end {
        LineEnd::Node { id, .. } => project.nodes.iter().find(|n| n.id == *id).and_then(|n| n.scope.last()).map(String::as_str),
        LineEnd::Port { group, inner: true, .. } => Some(group.as_str()),
        LineEnd::Port { group, inner: false, .. } | LineEnd::Gate { group, .. } => {
            project.groups.iter().find(|g| g.id == *group).and_then(|g| g.parent_group_id.as_deref())
        }
    }
}

/// How a line's end is named in a message: `node.port`, or the group's
/// own name and the port.
fn end_name(project: &ProjectDefinition, end: &LineEnd) -> String {
    match end {
        LineEnd::Node { id, port } => format!("{}.{port}", author_name_by_id(project, id)),
        LineEnd::Port { group, port, inner: true, .. } => format!("self.{port} (in '{}')", weft_core::project::plain_id(group)),
        LineEnd::Port { group, port, .. } | LineEnd::Gate { group, port } => format!("{}.{port}", weft_core::project::plain_id(group)),
    }
}

/// unknown-source-node / unknown-target-node: edges must reference
/// nodes (or groups, for a wire starting at a group's port or landing on
/// its gate) that actually exist in the project.
fn check_edge_node_refs(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let ids: std::collections::HashSet<&str> =
        project.nodes.iter().map(|n| n.id.as_str()).chain(project.groups.iter().map(|g| g.id.as_str())).collect();
    for edge in &project.edges {
        let span = edge.span.unwrap_or_default();
        let file = edge.source_file.as_deref();
        // About the end that exists, whose wire leads nowhere: an id no part
        // carries is in no cut. A wire with neither end is about no part,
        // so the whole program answers for it.
        let known: Vec<&str> = [edge.source.as_str(), edge.target.as_str()]
            .into_iter()
            .filter(|id| ids.contains(id))
            .map(|id| part_by_id(project, id))
            .collect();
        if !ids.contains(edge.source.as_str()) {
            push(
                d,
                &known,
                file,
                span,
                Severity::Error,
                "unknown-source-node",
                format!("edge references unknown source node '{}'", edge.source),
            );
        }
        if !ids.contains(edge.target.as_str()) {
            push(
                d,
                &known,
                file,
                span,
                Severity::Error,
                "unknown-target-node",
                format!("edge references unknown target node '{}'", edge.target),
            );
        }
    }
}

/// scope-reachability: a line a person wrote joins two ends of ONE scope:
/// two nodes of a group, a node and the group's own port seen from
/// inside (`self.x`), or a node and a child group's port seen from
/// outside (`child.x`). A line from a node straight to a node of another
/// scope reaches through a group's wall without its port. A loop's ports
/// are read the same way (`node_end`).
fn check_scope_reachability(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for line in lines(project) {
        // A container's own output or metadata port, read by its name from
        // inside it (`g.out` in `g`): said the way `self.out` is.
        let read_inside = match &line.from {
            LineEnd::Port { group, port, side: weft_core::project::CrossingSide::Out, inner: false } if line.to.lies_in(group) => {
                read_inside_refusal(project, group, port)
            }
            _ => None,
        };
        if read_inside.is_none() && end_scope(project, &line.from) == end_scope(project, &line.to) {
            continue;
        }
        let (code, message) = read_inside.unwrap_or_else(|| ("scope-reachability", format!(
            "edge '{} -> {}' crosses scope boundaries without a group or loop boundary",
            end_name(project, &line.from),
            end_name(project, &line.to),
        )));
        push(d, &line.parts(project), line.file.as_deref(), line.span, Severity::Error, code, message);
    }
}

// ─── group 2: port resolution ───────────────────────────────────────────────

/// The message for `self.<port>` read inside a group or loop with no such
/// input of its own, when an ENCLOSING scope has one. The rule is right
/// (`self` inside a group is the group's own inputs, never the group
/// around it) and the bare "no input port" reads like the port is
/// missing; what the author has to do is thread the value in as an input
/// of the inner scope, so the message says exactly that, naming both
/// scopes. `None` when no enclosing scope carries the port (then it
/// really is unknown).
fn enclosing_scope_hint(project: &ProjectDefinition, inner: &str, handle: &str) -> Option<String> {
    let group = project.groups.iter().find(|g| g.id == inner)?;
    let mut outer = group.parent_group_id.clone();
    let outer = loop {
        let around = project.groups.iter().find(|g| Some(&g.id) == outer.as_ref())?;
        if around.in_ports.iter().any(|p| p.name == handle) {
            break around;
        }
        outer = around.parent_group_id.clone();
    };
    let inner_kind = if matches!(group.kind, weft_core::project::GroupKind::Loop { .. }) { "loop" } else { "group" };
    let inner_short = inner.rsplit('.').next().unwrap_or(inner);
    Some(format!(
        "'{}' has no input '{handle}': inside the {inner_kind} '{}', `self` is the \
         {inner_kind}'s own inputs, and `{handle}` is an input of the enclosing {} \
         '{}'. Declare it on the {inner_kind} (`{inner_short} = {}({handle}: <type>, ...)`) \
         and wire it in from the group (`{inner_short}.{handle} = self.{handle}`).",
        weft_core::project::plain_id(inner),
        weft_core::project::plain_id(inner),
        outer.kind.noun(),
        weft_core::project::plain_id(&outer.id),
        if inner_kind == "loop" { "Loop" } else { "Group" },
    ))
}

/// What a line reading `self.{port}` inside `group` is told when the port
/// is one of the group's own that only the outside reads: a metadata port,
/// or an output. Both are read from outside, the way a node's output is,
/// and an output is written inside (`self.out = ...`). The code says which.
fn read_inside_refusal(project: &ProjectDefinition, group: &str, port: &str) -> Option<(&'static str, String)> {
    let g = project.groups.iter().find(|g| g.id == group)?;
    let name = weft_core::project::plain_id(group);
    let noun = g.kind.noun();
    if g.metadata_ports.iter().any(|m| m.name == port) {
        return Some(("metadata-port-read-inside", format!(
            "'self.{port}' is a metadata port of '{name}', and like an output it is read from outside the {noun} \
             (`other.input = {name}.{port}`), never from inside it. If a node inside needs the same value, \
             take it from where '{port}' gets it, or name it in another metadata port's value"
        )));
    }
    g.out_ports.iter().any(|p| p.name == port).then(|| ("unknown-source-port", format!(
        "'self.{port}' is an output of '{name}': inside the {noun} it is written (`self.{port} = ...`), \
         and it is read from outside (`other.input = {name}.{port}`)"
    )))
}

/// The root anonymous group of a component file (one used through
/// `@include`), whose boundaries are the file's own interface: there is
/// no outer scope to wire from, and nothing inside consumes its outputs
/// by construction. `None` for a runnable project, including one that
/// puts an anonymous group beside loose nodes: an include refuses that
/// file, so it is a program and the group is one more of its items.
/// The body of a file it includes sits at the top level too, beside the
/// root rather than in it, and is no loose item: it is that file's own
/// page. One answer per file, so every rule reads the same root.
fn component_root(project: &ProjectDefinition) -> Option<&str> {
    let root = project
        .groups
        .iter()
        .find(|g| g.parent_group_id.is_none() && g.anonymous)
        .map(|g| g.id.as_str())?;
    // Alone: every node sits inside a group (the root, or something the
    // root holds), so nothing of the program runs outside the component.
    let alone = project.nodes.iter().all(|n| !n.scope.is_empty());
    alone.then_some(root)
}

/// Does a written value carry a `@file`/`@asset` marker anywhere (at the
/// top, or inside a list of them)? The compiler-read fixed rule refuses
/// those: such a port has to be readable without following a file.
fn holds_a_marker(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => matches!(crate::file_ref::parse_marker(s), Some(Ok(_))),
        serde_json::Value::Array(items) => items.iter().any(holds_a_marker),
        serde_json::Value::Object(map) => map.values().any(holds_a_marker),
        _ => false,
    }
}

/// The name a diagnostic calls a node: its id, or for a loop's boundary
/// node the loop's, since the boundary's own id (`my__in`, `my__out`)
/// appears nowhere in the source.
fn author_name(node: &weft_core::project::NodeDefinition) -> &str {
    node.part()
}

/// `author_name` for a node an edge names by id. An id no node carries
/// is reported as it is: `unknown-source-node` / `unknown-target-node`
/// own that case, and the author wrote that id.
fn author_name_by_id<'a>(project: &'a ProjectDefinition, id: &'a str) -> &'a str {
    project.nodes.iter().find(|n| n.id == id).map(author_name).unwrap_or(id)
}

/// unknown-source-port / unknown-target-port: every line's ends must be
/// real ports, a node's on the enriched node and a group's on the group's
/// signature (a loop's on its `LoopIn` / `LoopOut`). port-typo-suggestion:
/// for each unresolved handle, pick the closest existing port by
/// Levenshtein distance if it's within a threshold.
///
/// duplicate-input-port (1:1 rule): what a line lands on (a node's input,
/// a group's input from outside, a group's output from inside) has at
/// most one line into it.
fn check_port_resolution(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use weft_core::project::CrossingSide;
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    // The ports a line may name on `group`'s `side`, seen from inside or
    // from outside. A loop's are its boundary nodes' (`LoopIn` adds `index`
    // inside, `LoopOut` takes `done` inside), a group's its own.
    let group_ports = |group: &str, side: CrossingSide, inner: bool| -> Option<Vec<&str>> {
        let g = project.groups.iter().find(|g| g.id == group)?;
        let boundary = |role: weft_core::project::GroupBoundaryRole| project.nodes.iter()
            .find(|n| n.group_boundary.as_ref().is_some_and(|b| b.group_id == group && b.role == role));
        let mut ports: Vec<&str> = match (side, inner, &g.kind) {
            (CrossingSide::In, true, weft_core::project::GroupKind::Loop { .. }) => {
                boundary(weft_core::project::GroupBoundaryRole::In)?.outputs.iter().map(|p| p.name.as_str()).collect()
            }
            (CrossingSide::Out, true, weft_core::project::GroupKind::Loop { .. }) => {
                boundary(weft_core::project::GroupBoundaryRole::Out)?.inputs.iter().map(|p| p.name.as_str()).collect()
            }
            (CrossingSide::In, ..) => g.in_ports.iter().map(|p| p.name.as_str()).collect(),
            (CrossingSide::Out, ..) => g.out_ports.iter().map(|p| p.name.as_str()).collect(),
        };
        ports.retain(|p| !weft_core::run::skip::is_gate_port(p));
        // Read from outside, a metadata port is one of the container's
        // outputs (a line reading it from inside is refused below).
        if side == CrossingSide::Out && !inner {
            ports.extend(g.metadata_ports.iter().map(|m| m.name.as_str()));
        }
        Some(ports)
    };
    let unknown_group_port = |group: &str, side: CrossingSide, port: &str, names: &[&str]| {
        let kind = if side == CrossingSide::In { "input" } else { "output" };
        let noun = project.groups.iter().find(|g| g.id == group).map_or("group", |g| g.kind.noun());
        match did_you_mean(port, names) {
            Some(s) => format!("{noun} '{}' has no {kind} '{port}'. Did you mean '{s}'?", weft_core::project::plain_id(group)),
            None => format!("{noun} '{}' has no {kind} '{port}'. Available: [{}]", weft_core::project::plain_id(group), names.join(", ")),
        }
    };
    let lines = lines(project);
    for line in &lines {
        let (file, span) = (line.file.as_deref(), line.span);
        match &line.from {
            LineEnd::Node { id, port } => {
                let Some(src) = by_id.get(id.as_str()) else { continue };
                if !src.outputs.iter().any(|p| p.name == *port) {
                    let names: Vec<&str> = src.outputs.iter().map(|p| p.name.as_str()).collect();
                    let msg = match did_you_mean(port, &names) {
                        Some(s) => format!("node '{}' has no output port '{port}'. Did you mean '{s}'?", author_name(src)),
                        None => format!("node '{}' has no output port '{port}'. Available: [{}]", author_name(src), names.join(", ")),
                    };
                    push(d, &line.parts(project), file, span, Severity::Error, "unknown-source-port", msg);
                }
            }
            LineEnd::Port { group, port, side, inner } => {
                let Some(names) = group_ports(group, *side, *inner) else { continue };
                if !names.contains(&port.as_str()) {
                    // `self.x` read inside: the group's own input; `g.y`
                    // read outside: the group's output.
                    let (code, msg) = inner.then(|| read_inside_refusal(project, group, port)).flatten()
                        .unwrap_or_else(|| ("unknown-source-port", inner.then(|| enclosing_scope_hint(project, group, port)).flatten()
                            .unwrap_or_else(|| unknown_group_port(group, *side, port, &names))));
                    push(d, &line.parts(project), file, span, Severity::Error, code, msg);
                }
            }
            LineEnd::Gate { .. } => {}
        }
        match &line.to {
            LineEnd::Node { id, port } => {
                let Some(tgt) = by_id.get(id.as_str()) else { continue };
                if !tgt.inputs.iter().any(|p| p.name == *port) {
                    let names: Vec<&str> = tgt.inputs.iter().map(|p| p.name.as_str()).collect();
                    let msg = match did_you_mean(port, &names) {
                        Some(s) => format!("node '{}' has no input port '{port}'. Did you mean '{s}'?", author_name(tgt)),
                        None => format!("node '{}' has no input port '{port}'. Available: [{}]", author_name(tgt), names.join(", ")),
                    };
                    push(d, &line.parts(project), file, span, Severity::Error, "unknown-target-port", msg);
                }
            }
            LineEnd::Port { group, port, side, inner } => {
                let Some(names) = group_ports(group, *side, *inner) else { continue };
                if !names.contains(&port.as_str()) {
                    push(d, &line.parts(project), file, span, Severity::Error, "unknown-target-port", unknown_group_port(group, *side, port, &names));
                }
            }
            LineEnd::Gate { .. } => {}
        }
    }

    let mut seen: std::collections::HashMap<&LineEnd, usize> = Default::default();
    for line in &lines {
        if let Some(first) = seen.get(&line.to) {
            push(
                d,
                &[line.to.part(project)],
                line.file.as_deref(),
                line.span,
                Severity::Error,
                "duplicate-input-port",
                format!(
                    "port '{}' already has a driver at line {first}; an input can be fed by exactly one edge",
                    end_name(project, &line.to),
                ),
            );
        } else {
            seen.insert(&line.to, line.span.start_line);
        }
    }
}

/// Return the element of `candidates` with Levenshtein distance <= 2
/// from `input`, if any. Tie-breaks on the first candidate.
fn did_you_mean<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(usize, &str)> = None;
    for c in candidates {
        let d = levenshtein(input, c);
        if d <= 2 && best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, c));
        }
    }
    best.map(|(_, s)| s)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let m = a.len();
    let n = b.len();
    if m == 0 { return n; }
    if n == 0 { return m; }
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0usize; n + 1];
    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = *[
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            ]
            .iter()
            .min()
            .unwrap();
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

// ─── group 3: type checking ─────────────────────────────────────────────────

/// type-mismatch: edge source type must be assignable to target type.
/// unresolved-typevar: a connected port still has a TypeVar after
///   enrichment (couldn't be resolved from context).
fn check_type_compat(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    /// A typed point a wire's type is held to: the type, how a message
    /// names it, and the part it is on.
    type Stop<'p> = (WeftType, String, &'p str);
    let by_id: std::collections::HashMap<&str, &weft_core::project::NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let graph = weft_core::project::graph::ProjectGraph::new(project);
    // The concrete type a group port declares where a wire crosses it (a
    // group inside an included file has the types the site gave it), and
    // how a message names the port, and the group (a stretch's end). `None`
    // for an open type.
    let declared = |crossing: &weft_core::project::Crossing, at: &weft_core::frames::Located| -> Option<Stop> {
        let g = project.groups.iter().find(|g| g.id == crossing.group)?;
        let types = g.types_at(&at.path);
        let port_type = if crossing.side == weft_core::project::CrossingSide::In { types.input(&crossing.port) } else { types.output(&crossing.port) }?;
        (!port_type.contains_unresolved_leaf())
            .then(|| (port_type.clone(), format!("{}.{}", weft_core::project::plain_id(&g.id), crossing.port), g.id.as_str()))
    };

    // Every wire at every place it runs: a wire inside an included file
    // is checked with the types each site gives it. The same finding at
    // two places is reported once.
    let mut reported: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for (edge, source, target) in weft_core::project::places::located_wires(project) {
        let Some(src) = by_id.get(edge.source.as_str()) else { continue };
        let Some(tgt) = by_id.get(edge.target.as_str()) else { continue };
        let (file, span) = (edge.source_file.as_deref(), edge.span.unwrap_or_default());
        let wire = [src.part(), tgt.part()];
        let mut push = |code: &str, message: String| {
            if reported.insert((edge.id.clone(), message.clone())) {
                push(d, &wire, file, span, Severity::Error, code, message);
            }
        };
        let Some(src_port) = src
            .outputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref())
        else { continue };
        let Some(tgt_port) = tgt
            .inputs
            .iter()
            .find(|p| Some(p.name.as_str()) == edge.target_handle.as_deref())
        else { continue };
        let (Some(src_port_type), Some(tgt_port_type)) = (
            src.types_at(&source.path).output(&src_port.name),
            tgt.types_at(&target.path).input(&tgt_port.name),
        ) else { continue };

        // An edge onto a port that takes no wire is illegal as a whole;
        // input-accepts (check_port_coverage) owns that situation, and
        // stacking type-level diagnostics on an edge that cannot exist
        // would bury the real cause.
        if !tgt_port.accepts.wire {
            continue;
        }

        // A wire through a group port that declares a concrete type is
        // held to that type stretch by stretch (below), each on its own
        // line; checked end to end as well, one wrong line would be
        // reported twice, the second time on a line that is fine.
        let crossed = weft_core::project::places::crossing_places(&graph, edge, &source);
        let declared_on_the_way = edge.via.iter().zip(&crossed).any(|(crossing, at)| declared(crossing, at).is_some());

        // What the wire delivers: the source port's type with the
        // edge's path read off it. A path that does not fit the type
        // is its own error, and nothing below can say more.
        let src_label = if edge.path.is_empty() {
            format!("{}.{}", author_name(src), src_port.name)
        } else {
            format!("{}.{}.{}", author_name(src), src_port.name, edge.path.join("."))
        };
        let src_type = match weft_core::deref::walk_path(src_port_type, &edge.path) {
            Ok(t) => t,
            Err(_) if declared_on_the_way => continue,
            Err(e) => {
                push("deref-path", format!("cannot read '{src_label}': {e}"));
                continue;
            }
        };

        // A stream source into an unresolved NON-STREAM target: the
        // `generator-into-generic-port` rule (check_generator_wiring)
        // owns this edge WHOLE; its predicate is the same
        // `contains_unresolved_leaf`, so the two cannot drift. Stacking
        // unresolved-typevar on top would give contradictory advice
        // ("declare a concrete type" vs "declare Generator[T]"). A
        // stream-typed target is NOT that rule's edge (it skips
        // generator-to-generator pairs): enrich binds its element
        // type element-wise, so it falls through here and reports
        // only when the element is genuinely unresolvable (a
        // `Generator[T]` port with nothing concrete upstream).
        if src_type.as_generator().is_some()
            && tgt_port_type.as_generator().is_none()
            && tgt_port_type.contains_unresolved_leaf()
        {
            continue;
        }
        // The mirror direction the same rule owns: a generic source
        // wired into a stream-typed target.
        if tgt_port_type.as_generator().is_some()
            && src_type.as_generator().is_none()
            && src_type.contains_unresolved_leaf()
        {
            continue;
        }

        // A nested unresolved leaf (`A | T`, `List[T]`) is as unusable as a
        // bare one: the runtime gate cannot validate against it.
        if src_type.contains_unresolved_leaf() {
            push("unresolved-typevar",
                format!(
                    "source port '{src_label}' type '{}' unresolved; connect it to something concrete or declare the type",
                    src_type,
                ));
            continue;
        }
        if tgt_port_type.contains_unresolved_leaf() {
            push("unresolved-typevar",
                format!(
                    "target port '{}.{}' type '{}' unresolved; connect it to something concrete or declare the type",
                    author_name(tgt), tgt_port.name, tgt_port_type,
                ));
            continue;
        }

        if !declared_on_the_way && !weft_core::weft_type::WeftType::is_compatible(&src_type, tgt_port_type) {
            // When the pair is refused as a wire but the Cast table
            // accepts it (a JsonDict claiming a declared type, text
            // that would parse), name the door.
            let cast_hint = if weft_core::weft_type::WeftType::cast_allowed(&src_type, tgt_port_type).is_ok() {
                "; wire it through a Cast node to convert and validate the value"
            } else {
                ""
            };
            push("type-mismatch",
                format!(
                    "cannot connect '{src_label}: {}' to '{}.{}: {}'{cast_hint}",
                    src_type,
                    author_name(tgt), tgt_port.name, tgt_port_type,
                ));
        }
    }

    // A wire through a group port that declares a concrete type is held to
    // that type: what reaches the port must fit it, and what the port hands
    // on must fit where it lands. The wire is checked stretch by stretch
    // between the types it meets (its source's, each such port's, its
    // target's), each stretch on the line that lands at its end. A group
    // port of an open type (a type variable the group passes through)
    // declares nothing, so a stretch runs on past it. A line several wires
    // carry is reported once, about the two parts the stretch runs between.
    let mut reported: std::collections::HashSet<(Option<String>, usize, usize, String, String)> = Default::default();
    let mut check = |from: &Stop, path: &[String], to: &Stop, span: Span, file: Option<&str>| {
        let stretch = [from.2, to.2];
        if !reported.insert((file.map(str::to_string), span.start_line, span.start_column, from.1.clone(), to.1.clone())) {
            return;
        }
        let from_type = match weft_core::deref::walk_path(&from.0, path) {
            Ok(t) => t,
            Err(e) => {
                push(d, &stretch, file, span, Severity::Error, "deref-path",
                    format!("cannot read '{}.{}': {e}", from.1, path.join(".")));
                return;
            }
        };
        if from_type.contains_unresolved_leaf() || to.0.contains_unresolved_leaf() {
            return;
        }
        if !weft_core::weft_type::WeftType::is_compatible(&from_type, &to.0) {
            push(d, &stretch, file, span, Severity::Error, "type-mismatch",
                format!("cannot connect '{}: {from_type}' to '{}: {}'", from.1, to.1, to.0));
        }
    };
    // Every wire at every place it runs, with the types each place gives
    // it; a wire that stops at a group port nobody reads too.
    for (edge, source, target) in weft_core::project::places::wires_at_places(project) {
        let crossed = weft_core::project::places::crossing_places(&graph, edge, &source);
        let stops: Vec<(usize, Stop)> = edge.via.iter().zip(&crossed).enumerate()
            .filter_map(|(k, (crossing, at))| declared(crossing, at).map(|typed| (k, typed))).collect();
        // With none, the wire is checked end to end above.
        let Some(&(last, _)) = stops.last() else { continue };
        // Where the stretch being checked starts: the source node's port
        // (`None`), then each declared crossing in turn.
        let mut from: Option<(Option<usize>, Stop)> = if edge.from_group_port() {
            None
        } else {
            by_id.get(edge.source.as_str()).and_then(|src| {
                let port_type = src.types_at(&source.path).output(edge.source_port())?;
                Some((None, (port_type.clone(), format!("{}.{}", author_name(src), edge.source_port()), src.part())))
            })
        };
        for (k, typed) in &stops {
            if let Some((at, start)) = &from {
                let after = at.map_or(0, |i| i + 1);
                let path: Vec<String> = edge.via[after..=*k].iter().flat_map(|c| c.path.iter().cloned()).collect();
                let crossing = &edge.via[*k];
                check(start, &path, typed, crossing.span.unwrap_or_default(), crossing.source_file.as_deref());
            }
            from = Some((Some(*k), typed.clone()));
        }
        let Some(target) = target else { continue };
        let Some((_, start)) = &from else { continue };
        let Some(tgt) = by_id.get(edge.target.as_str()) else { continue };
        let Some(p) = tgt.inputs.iter().find(|p| p.name == edge.target_port()) else { continue };
        // A port that takes no wire: input-accepts owns that.
        if !p.accepts.wire { continue; }
        let Some(port_type) = tgt.types_at(&target.path).input(&p.name) else { continue };
        let to = (port_type.clone(), format!("{}.{}", author_name(tgt), p.name), tgt.part());
        check(start, &edge.path_after(last), &to, edge.span.unwrap_or_default(), edge.source_file.as_deref());
    }

    // config-type-mismatch on a group's own ports: the literal written on
    // an input (`g.x = 5`) is held to the type the group declares there.
    // A loop's sit on its `LoopIn` too, where the node rule below reads
    // them.
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        for (key, value) in &group.port_literals {
            if weft_core::run::skip::is_gate_port(key) { continue; }
            let Some(input) = group.in_ports.iter().find(|p| p.name == *key) else { continue };
            if input.port_type.is_unresolved() { continue; }
            let inferred = crate::file_ref::literal_type(value);
            if !weft_core::weft_type::WeftType::is_compatible(&inferred, &input.port_type) {
                if let Err(why) = input.port_type.cast_value(value) {
                    let (span, file) = group_literal_anchor(group, key);
                    push(d, &[group.id.as_str()], file, span, Severity::Error, "config-type-mismatch",
                        format!(
                            "'{}.{key}: {inferred}' incompatible with input type '{}': {why}",
                            weft_core::project::plain_id(&group.id), input.port_type,
                        ));
                }
            }
        }
    }

    // cast-not-allowed: any node declaring the checked-cast semantic
    // (`features.castPorts`, metadata-declared; the compiler never
    // names a node type) must have a resolved conversion pair the one
    // weft-core table accepts (`WeftType::cast_allowed`). The runtime
    // cast is the same door, so an allowed pair either converts or
    // fails loudly at run time with a field-level message, and a
    // nonsense pair (JsonDict -> Number) dies here instead.
    for node in &project.nodes {
        let Some(cast) = &node.features.cast_ports else { continue };
        let input = node.inputs.iter().find(|p| p.name == cast.input);
        let output = node.outputs.iter().find(|p| p.name == cast.output);
        let (Some(input), Some(output)) = (input, output) else { continue };
        if let Err(e) = weft_core::weft_type::WeftType::cast_allowed(
            &input.port_type,
            &output.port_type,
        ) {
            push(d, &[node.part()], node.source_file.as_deref(), node.header_span_or_default(),
                Severity::Error, "cast-not-allowed", e);
        }
    }

    // config-type-mismatch: walk each node's config fields vs port
    // types. (Port-type-override incompatibility is caught upstream by
    // the connection type check on the overridden port.)
    for node in &project.nodes {
        // Every constant written for a port lives in `port_literals`,
        // whichever spelling wrote it; type-check each against the
        // declared input type. A marker counts as the type it declares
        // (`literal_type`), so `@asset("a.png", Image)` on an Image port
        // is a match and on a Video port a mismatch.
        for (key, value) in &node.port_literals {
            // A connection picked on the install holds no value in the
            // program: the install checks the pick when it is made.
            if weft_core::picks::is_install_picked(value) {
                continue;
            }
            // An `@instance_filled` field is checked through its fallback,
            // the one value of it known now; the instance's own value is
            // checked when it is given (`weft_core::instance`).
            let value = match weft_core::instance::as_instance_filled(value) {
                Some(filled) => match filled.fallback {
                    Some(fallback) => fallback,
                    None => continue,
                },
                None => value,
            };
            let span_entry = node.port_literal_spans.get(key);
            let Some(input) = node.inputs.iter().find(|p| p.name == *key) else { continue };
            // A port whose type is still open (an unbound variable) has
            // nothing to check against yet: `unresolved-typevar` owns it, and
            // `_should_flow`'s constant has its own rule in the coverage
            // pass.
            if input.port_type.is_unresolved() { continue }
            // The culprit is the literal itself; fall back to the node
            // header if the span is missing. The FILE follows the span:
            // an interface-port fill on a loop's boundary node was
            // written in another file than the node's own spans.
            let (span, file) = match span_entry {
                Some(s) => (s.span, s.source_file.as_deref()),
                None => (
                    node.header_span.unwrap_or_default(),
                    node.source_file.as_deref(),
                ),
            };
            // A widget-shaped handle (a remote_select's `{id, label}`
            // pick, an access widget's `{id, identity}` connect handle)
            // is legal on its input by the widget's own contract, and
            // held to that contract here with the rule the runtime
            // applies when it builds the bag. Only the OBJECT form is
            // a handle: a plain value (a pasted raw id string) still
            // type-checks normally.
            if value.is_object()
                && matches!(
                    input.widget,
                    Some(weft_core::node::Widget::RemoteSelect { .. })
                        | Some(weft_core::node::Widget::Access { .. })
                )
            {
                if let Err(why) = input.widget.as_ref().expect("matched above").check_handle_shape(value) {
                    push(d, &[node.part()], file, span, Severity::Error, "config-type-mismatch",
                        format!("config '{}.{}': {why}", author_name(node), key));
                }
                continue;
            }
            let inferred = crate::file_ref::literal_type(value);
            // Enrich already cast every castable literal in place, so a
            // shape still mismatching here is a GENUINE type error iff
            // the cast refuses too; the refusal reason names WHY. A
            // still-castable value only occurs when a caller validates
            // an un-enriched graph, where the value is fine (the
            // compile pass will cast it), so no diagnostic fires.
            if !weft_core::weft_type::WeftType::is_compatible(&inferred, &input.port_type) {
                if let Err(why) = input.port_type.cast_value(value) {
                    push(d, &[node.part()], file, span, Severity::Error, "config-type-mismatch",
                        format!(
                            "config '{}.{}: {}' incompatible with input type '{}': {}",
                            author_name(node), key, inferred, input.port_type, why,
                        ));
                }
            }
        }
    }
}

// ─── group 4: port coverage ─────────────────────────────────────────────────

/// required-port-unmet, for a group, a loop or an include site: an input
/// the outside neither wires nor writes, which inside ends at a required
/// input of a node. A container port is never required for its own sake
/// (whether a group runs is its gate's call alone, and a closed port
/// flows inside like any closure), so a port nothing inside needs may
/// stay unconnected. A loop's iterated and carried ports are required on
/// the loop itself and fall under the node rule instead. A component
/// file's root is skipped: its inputs are the file's interface, and
/// whether they are connected is each include site's question, asked
/// there (`root` is `component_root`).
fn check_container_inputs(project: &ProjectDefinition, root: Option<&str>, d: &mut Vec<Diagnostic>) {
    let fed = |at: &weft_core::frames::Located, port: &str| gets_value(project, &at.id, port);
    // `container` is the group's id, which the finding is about (a loop's
    // too, whose `LoopIn` holds the ports); `group` is how it is named.
    let report = |d: &mut Vec<Diagnostic>, container: &str, group: &str, name: &str, need: weft_core::project::places::Need, file: Option<&str>, span: Span| {
        let (needer, port) = need.needer;
        let why = match &need.one_of {
            None => "a required input".to_string(),
            Some(set) => format!("the only one of @require_one_of({}) that could get a value", set.join(", ")),
        };
        push(
            d,
            &[container],
            file,
            span,
            Severity::Error,
            "required-port-unmet",
            format!(
                "input '{group}.{name}' has nothing connected, and inside it feeds \
                 '{needer}.{port}', {why}, so '{group}.{name}' is required too: \
                 wire or write it",
                needer = weft_core::project::address_of(project, &needer.id, &needer.path),
            ),
        );
    };
    // A group or a call site: what an unconnected input starts is the wire
    // it sends (`crate::wires`), to every member that reads it.
    for group in project.groups.iter().filter(|g| matches!(g.kind, weft_core::project::GroupKind::Group | weft_core::project::GroupKind::Call { .. })) {
        if root == Some(group.id.as_str()) { continue; }
        for input in group.in_ports.iter().filter(|p| !weft_core::run::skip::is_gate_port(&p.name)) {
            if group.port_literals.get(&input.name).is_some_and(|v| !v.is_null()) { continue; }
            if fed_by_metadata_wire(project, &group.id, &input.name) { continue; }
            let need = project.edges.iter()
                .filter(|e| e.source == group.id && e.from_group_port() && e.via[0].port == input.name && !e.to_group_port())
                .filter(|e| project.nodes.iter().any(|n| n.id == e.target))
                .find_map(|e| weft_core::project::places::required_consumer(
                    project, &weft_core::frames::Located::top(&e.target), e.target_port(), &|_, _| true, &|_| false, &fed));
            if let Some(need) = need {
                report(d, &group.id, &weft_core::project::plain_id(&group.id), &input.name, need, group.source_file.as_deref(),
                    group.header_span.or(group.span).unwrap_or_default());
            }
        }
    }
    // A loop: its `LoopIn` takes the ports, and passes what reaches each
    // to the members that read it.
    let driven: std::collections::HashSet<(&str, &str)> = project.edges.iter()
        .filter(|e| !e.to_group_port())
        .chain(&project.metadata_wires)
        .map(|e| (e.target.as_str(), e.target_port())).collect();
    for node in project.nodes.iter().filter(|n| n.node_type == weft_core::project::boundary_types::LOOP_IN) {
        let Some(boundary) = node.group_boundary.as_ref() else { continue };
        if root == Some(boundary.group_id.as_str()) { continue; }
        for input in &node.inputs {
            if input.required
                || weft_core::run::skip::is_gate_port(&input.name)
                || driven.contains(&(node.id.as_str(), input.name.as_str()))
                || literal_fills(node, &input.name)
            {
                continue;
            }
            let at = weft_core::frames::Located::top(&node.id);
            if let Some(need) = weft_core::project::places::required_consumer(project, &at, &input.name, &|_, _| true, &|_| false, &fed) {
                report(d, node.part(), author_name(node), &input.name, need, node.source_file.as_deref(), node.header_span_or_default());
            }
        }
    }
}

/// gate-not-boolean, on a group: a gate written on a group (in its
/// braces, `_should_flow: false`, or outside, `g._should_flow = true`) is
/// a Boolean, as on a node. A loop's sits on its `LoopIn`, where the node
/// rule reads it.
fn check_group_gates(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Loop { .. })) {
        for (key, value) in group.port_literals.iter().filter(|(key, _)| weft_core::run::skip::is_gate_port(key)) {
            if value.is_boolean() || value.is_null() { continue; }
            let (span, file) = group_literal_anchor(group, key);
            push(
                d,
                &[group.id.as_str()],
                file,
                span,
                Severity::Error,
                "gate-not-boolean",
                format!(
                    "'{}.{key}: {}' is not a Boolean; written down, a gate is `true` or \
                     `false` (a wire may carry any value)",
                    weft_core::project::plain_id(&group.id), crate::file_ref::literal_type(value)
                ),
            );
        }
    }
}

/// Whether input `port` of node `id` gets a value: written in the source,
/// a default, or a wire from a node, or from a group's port that is
/// written (`g.x = 5`); a group's port left unconnected sends nothing. A
/// loop's `LoopIn` is followed where its answer is one answer, an input
/// of that name; an output it makes on its own (`index`) counts as a
/// value.
fn gets_value(project: &ProjectDefinition, id: &str, port: &str) -> bool {
    let Some(node) = project.nodes.iter().find(|n| n.id == id) else { return false };
    if literal_fills(node, port)
        || node.inputs.iter().any(|input| input.name == port && input.default.is_some())
        || fed_by_metadata_wire(project, id, port)
    {
        return true;
    }
    project.edges.iter()
        .filter(|e| e.target == id && e.target_port() == port && !e.to_group_port())
        .any(|e| {
            if e.from_group_port() {
                return starts_at_group_constant(project, e);
            }
            let from = e.source_port();
            let followed = project.nodes.iter().find(|n| n.id == e.source).filter(|door| {
                door.node_type == weft_core::project::boundary_types::LOOP_IN && door.inputs.iter().any(|input| input.name == from)
            });
            followed.is_none() || gets_value(project, &e.source, from)
        })
}

/// Whether a metadata wire fills input `port` of `id` (a node, a loop's
/// `LoopIn` or a group): the input is driven, by a value written when the
/// program compiles. When that value could not be worked out, enrich said
/// so where it is written, and the input is not reported again as having
/// no driver.
fn fed_by_metadata_wire(project: &ProjectDefinition, id: &str, port: &str) -> bool {
    project.metadata_wires.iter().any(|wire| wire.target == id && wire.target_port() == port)
}

/// Whether `wire` starts at a constant written on a group's input port
/// (`g.x = "hi"`), which the group sends the moment it opens. A wire from
/// a group's output port of the same name is not one: constants are
/// written on inputs only.
fn starts_at_group_constant(project: &ProjectDefinition, wire: &weft_core::project::Edge) -> bool {
    let Some(own) = wire.via.first().filter(|_| wire.from_group_port()) else { return false };
    own.side == weft_core::project::CrossingSide::In
        && project.groups.iter().any(|g| g.id == wire.source && g.port_literals.get(&own.port).is_some_and(|v| !v.is_null()))
}

/// required-port-unmet: required input with no driver (no edge + no
///   body value).
/// require-one-of-unmet: each @require_one_of group must have at
///   least one port driven.
/// input-accepts: a driver the port does not take (a wire on a
///   literal-only port, a written value on a wire-only port, or a
///   wire or a marker on a compiler-read port).
/// gate-not-boolean: a written gate (`_should_flow` or
///   `_should_not_flow`) that is not a Boolean.
/// undeclared-port-no-custom: node doesn't support canAddInputPorts
///   but config has a key not matching any declared input.
fn check_port_coverage(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    d: &mut Vec<Diagnostic>,
) {
    // Build "is this (node, port) driven by an edge?" lookup once. A
    // metadata wire is no edge here: its value is a literal for every rule
    // (`accepts` above all), and only the missing-driver rule counts it.
    let driven: std::collections::HashSet<(String, String)> = project
        .edges
        .iter()
        .filter_map(|e| Some((e.target.clone(), e.target_handle.clone()?)))
        .collect();

    let root = component_root(project);
    check_container_inputs(project, root, d);
    check_group_gates(project, d);
    for node in &project.nodes {
        let span = node.header_span_or_default();
        let file = node.source_file.as_deref();

        let meta = catalog.lookup(&node.node_type);
        for input in &node.inputs {
            let accepts = input.accepts;
            let has_edge = driven.contains(&(node.id.clone(), input.name.clone()));
            // The constant the source wrote for this port, whichever
            // spelling: enrich homed every one in `port_literals`.
            let literal = node
                .port_literals
                .get(&input.name)
                .filter(|v| weft_core::run::ready::literal_is_data(node, &input.name, v));
            let has_literal = literal.is_some();
            // The file follows the span: an interface-port fill's span
            // lives in the file that wrote it, not the loop boundary's.
            let literal_anchor = || -> (Option<&str>, Span) {
                match node.port_literal_spans.get(&input.name) {
                    Some(s) => (s.source_file.as_deref(), s.span),
                    None => (file, span),
                }
            };
            let edge_anchor = || -> (Vec<&str>, Option<&str>, Span) {
                // Anchor on the offending EDGE where it has a span, and
                // take the edge's own file with it: the wire may be
                // written in a different file than the node it lands on.
                // The finding is about the wire, so it names both ends.
                let wire = project
                    .edges
                    .iter()
                    .find(|e| e.target == node.id && e.target_handle.as_deref() == Some(&input.name));
                let ends = match wire {
                    Some(e) => vec![part_by_id(project, &e.source), node.part()],
                    None => vec![node.part()],
                };
                match wire.filter(|e| e.span.is_some()) {
                    Some(e) => (ends, e.source_file.as_deref(), e.span.unwrap()),
                    None => (ends, file, span),
                }
            };

            // The compiler-read fixed rule: a port the compiler reads to
            // build the node (a `portsFromConfig` list, the access
            // picker) takes an inline typed value only. No wire, no
            // marker: it has to be readable in the source without
            // following anything. Nobody writes this and no `accepts`
            // loosens it.
            let compiler_read = meta.is_some_and(|m| m.is_compiler_read(&input.name));
            if compiler_read {
                let fix = format!(
                    "input '{}.{}' is read by the compiler to build the node; it takes an \
                     inline value only (no wire, no @file, no @asset)",
                    author_name(node), input.name
                );
                if has_edge {
                    let (ends, edge_file, edge_span) = edge_anchor();
                    push(d, &ends, edge_file, edge_span, Severity::Error, "input-accepts", fix.clone());
                }
                // A text marker was already read into its value by the
                // compile; the field's file-ref record is what remains of
                // it, so the record counts as the marker it came from.
                if literal.is_some_and(holds_a_marker) || node.file_refs.contains_key(&input.name) {
                    let (lit_file, lit_span) = literal_anchor();
                    push(d, &[node.part()], lit_file, lit_span, Severity::Error, "input-accepts", fix);
                }
            }

            // input-accepts: the port switched this family off. A port
            // that is BOTH wired and written is one mistake, not two:
            // double-driven-port owns it ("remove one driver" is the
            // accurate advice), so the family check stays silent then.
            if has_edge && !has_literal && !accepts.wire {
                let (ends, edge_file, edge_span) = edge_anchor();
                push(
                    d,
                    &ends,
                    edge_file,
                    edge_span,
                    Severity::Error,
                    "input-accepts",
                    format!(
                        "input '{}.{}' cannot be driven by a wire: `{}` accepts: {}",
                        author_name(node), input.name, input.name, accepts.describe()
                    ),
                );
            }
            if has_literal && !has_edge && !accepts.literal {
                let (lit_file, lit_span) = literal_anchor();
                push(
                    d,
                    &[node.part()],
                    lit_file,
                    lit_span,
                    Severity::Error,
                    "input-accepts",
                    format!(
                        "input '{}.{}' takes no written value: `{}` accepts: {}; wire it from \
                         another node",
                        author_name(node), input.name, input.name, accepts.describe()
                    ),
                );
            }

            // gate-not-boolean: either spelling of the gate is the
            // language's own, and a constant written for one is a
            // Boolean or a type error (a wire may carry anything; the
            // run reads it).
            if weft_core::run::skip::is_gate_port(&input.name) {
                if let Some(v) = literal.filter(|v| !v.is_boolean()) {
                    let (lit_file, lit_span) = literal_anchor();
                    push(
                        d,
                        &[node.part()],
                        lit_file,
                        lit_span,
                        Severity::Error,
                        "gate-not-boolean",
                        format!(
                            "'{}.{}: {}' is not a Boolean; written down, a gate is `true` or \
                             `false` (a wire may carry any value)",
                            author_name(node), input.name, crate::file_ref::literal_type(v)
                        ),
                    );
                }
            }

            // required + default = satisfiable: the runtime supplies the
            // default when nothing else drives the input.
            if input.required && !has_edge && !has_literal && input.default.is_none()
                && !fed_by_metadata_wire(project, &node.id, &input.name)
            {
                push(
                    d,
                    &[node.part()],
                    file,
                    span,
                    Severity::Error,
                    "required-port-unmet",
                    format!(
                        "required input '{}.{}' has no driver (no edge, no body value, no default)",
                        author_name(node), input.name
                    ),
                );
            }

            // The widget-level literal checks read the one home.
            let literal_value = node.port_literals.get(&input.name).and_then(|value| {
                if weft_core::picks::is_install_picked(value) {
                    return None;
                }
                match weft_core::instance::as_instance_filled(value) {
                    Some(filled) => filled.fallback,
                    None => Some(value),
                }
            });
            let literal_span = literal_anchor;
            // literal-out-of-range: the number widget's domain (min,
            // max, step) bounds the literal at compile time;
            // literal-not-an-option: a select's options bound it (each
            // item, for a multiselect). The rule itself lives on the
            // widget (`Widget::check_value`), the same one the runtime
            // holds a WIRED value and an instance's value to, so a source
            // that compiles cannot be refused at run time.
            if let Some(widget) = &input.widget {
                // Range-check the CAST value so a stringified number
                // in an un-enriched graph is bounded consistently
                // with the type check. An uncastable value is not
                // silently passed: config-type-mismatch reports it.
                let cast = literal_value.and_then(|v| input.port_type.cast_value(v).ok());
                if let Some(why) = cast.and_then(|v| widget.check_value(&v).err()) {
                    let (lit_file, lit_span) = literal_span();
                    push(
                        d,
                        &[node.part()],
                        lit_file,
                        lit_span,
                        Severity::Error,
                        if widget.has_options() { "literal-not-an-option" } else { "literal-out-of-range" },
                        format!("input '{}.{}': {why}", author_name(node), input.name),
                    );
                }
            }
        }

        // @require_one_of: every name must be one of the node's inputs
        // (the enriched set, so a created or config-derived port counts),
        // and each group must have at least one satisfied (driven or
        // configured-non-null) input. An unknown name once passed
        // silently, so a typo'd directive guarded nothing.
        for group in &node.features.one_of_required {
            if group.is_empty() {
                continue;
            }
            // Every name has to be a port of this node, on every
            // node type. Where the author can ADD input ports, a name
            // becomes one by being declared in the header, wired, or
            // configured; a name that is none of those is not an
            // alternative the node did not take, it is a port that
            // does not exist, and a guard over a port that does not
            // exist is a typo whichever way it happened.
            let unknown: Vec<&str> = group
                .iter()
                .filter(|name| !node.inputs.iter().any(|p| &p.name == *name))
                .map(String::as_str)
                .collect();
            if !unknown.is_empty() {
                // Every input the check would have accepted, so the
                // list and the test agree (a literal-only input is a
                // legitimate member of a group).
                let available: Vec<&str> = node
                    .inputs
                    .iter()
                    .filter(|p| !weft_core::run::skip::is_gate_port(&p.name))
                    .map(|p| p.name.as_str())
                    .collect();
                push(
                    d,
                    &[node.part()],
                    file,
                    span,
                    Severity::Error,
                    "require-one-of-unknown-port",
                    format!(
                        "node '{}' declares @require_one_of({}) but has no input port {}; declare \
                         it, or drop it from the list. Inputs: [{}]",
                        author_name(node),
                        group.join(", "),
                        unknown.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", "),
                        available.join(", ")
                    ),
                );
                continue;
            }
            let any_met = group.iter().any(|port_name| {
                driven.contains(&(node.id.clone(), port_name.clone())) || literal_fills(node, port_name)
            });
            if !any_met {
                push(
                    d,
                    &[node.part()],
                    file,
                    span,
                    Severity::Error,
                    "require-one-of-unmet",
                    format!(
                        "node '{}' declares @require_one_of({}) but none is driven",
                        author_name(node),
                        group.join(", ")
                    ),
                );
            }
        }

        // undeclared-port-no-custom: if the node can't accept custom
        // inputs (features.can_add_input_ports == false), every
        // config key must name a declared input; a key naming one of
        // the node's outputs and no input is `value-on-output` (an
        // output takes no value, a firing emits on it; a node that
        // accepts custom inputs gets the same refusal from enrich). A
        // node that derives ports from a config list holds that list
        // under a key it DECLARES as a config input (the catalog load
        // refuses `portsFromConfig` naming an undeclared input), so it
        // needs no exemption here. LoopIn/LoopOut carry the loop config
        // (`over`, `parallel`, ...) in their config blob;
        // check_loop_config validates those keys exhaustively against
        // its own known-keys list, so exactly those two types are
        // exempt here (matched by node_type, the same key
        // check_loop_config selects on, so the two checks partition the
        // space with no gap).
        // An unknown node type (a lenient-parse placeholder in the
        // editor; a hard enrich error in a strict compile) has no
        // declared inputs at all, so every config key would
        // false-positive here.
        if !node.features.can_add_input_ports
            && !matches!(node.node_type.as_str(), weft_core::project::boundary_types::LOOP_IN | weft_core::project::boundary_types::LOOP_OUT)
            && catalog.lookup(&node.node_type).is_some()
        {
            let Some(obj) = node.config.as_object() else { continue };
            let known_inputs: std::collections::HashSet<&str> =
                node.inputs.iter().map(|p| p.name.as_str()).collect();
            let known_outputs: std::collections::HashSet<&str> =
                node.outputs.iter().map(|p| p.name.as_str()).collect();
            for key in obj.keys() {
                // Compiler/editor plumbing keys (`_`-reserved,
                // `parentId`) are validated by the parser / merged at
                // flatten time and never need to match a declared
                // input.
                if weft_core::project::is_internal_config_key(key) {
                    continue;
                }
                if known_inputs.contains(key.as_str()) {
                    continue;
                }
                let (key_file, key_span) = cfg_anchor(node, key);
                if known_outputs.contains(key.as_str()) {
                    push(
                        d,
                        &[node.part()],
                        key_file,
                        key_span,
                        Severity::Error,
                        "value-on-output",
                        format!(
                            "node '{}': '{}' is an output port, and an output takes no value: \
                             a firing emits on it. Read it with `{}.{}`",
                            author_name(node), key, author_name(node), key
                        ),
                    );
                    continue;
                }
                push(
                    d,
                    &[node.part()],
                    key_file,
                    key_span,
                    Severity::Error,
                    "undeclared-port-no-custom",
                    format!(
                        "node '{}' does not accept custom inputs; config key '{}' names no declared input",
                        author_name(node), key
                    ),
                );
            }
        }

        // duplicate-port: two ports of the same name on the same side.
        // Config-derived ports (a form's fields, a switch's cases) are
        // where this comes from in practice, since a catalog's own port
        // list is checked at load and the source-vs-catalog merge is
        // by name. Detection runs PER SIDE: a node legitimately has an
        // input AND an output sharing a name (a passthrough's
        // `value`/`value`), so crossing the two would false-positive
        // every one of them.
        // SYNC: duplicate-port <-> packages/weft-graph/src/webview/lib/utils/port-specs.ts entryPortCollisions
        let sides: [Vec<&str>; 2] = [
            node.inputs.iter().map(|p| p.name.as_str()).collect(),
            node.outputs.iter().map(|p| p.name.as_str()).collect(),
        ];
        for names in sides {
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for name in names {
                if !seen.insert(name) {
                    push(
                        d,
                        &[node.part()],
                        file,
                        span,
                        Severity::Error,
                        "duplicate-port",
                        format!("node '{}' has two ports named '{}' on one side", author_name(node), name),
                    );
                }
            }
        }
    }

    // A literal written on an output port needs no check of its own:
    // lowering turns a `node.port = "literal"` line into a config field
    // on the node (not an edge), which the port checks reject when the
    // target port is output-only.
}

/// What a node's or a group's metadata ports have to be once enrich has
/// resolved them (`crate::metadata`):
///  - `duplicate-port`: a metadata port's name is no input's and no
///    output's of the same owner (`self.x` and `g.x` would mean either);
///    on a node this is an input the program added under one of its
///    type's metadata names.
///  - `config-type-mismatch`: a group's value fits the type its header
///    declares (a node type's is checked when the catalog loads).
///  - `unpaired-port`: on a node that pairs its added ports, every added
///    output has the added input of its name, and something reads it: the
///    value its input matches would otherwise be dropped.
// SYNC: duplicate-port (metadata) <-> packages/weft-graph/src/webview/lib/utils/port-specs.ts entryPortCollisions, packages/weft-graph/src/webview/lib/utils/metadata-ports.ts portNameClash
/// optional-pass-through-port: a `?` on a container's input that passes
/// through ([`weft_core::project::InputDefinition::passes_through`]). Such
/// a port holds nothing, so it is never required or optional itself: the
/// nodes inside that read it decide (`check_container_inputs`), and a `?`
/// there would claim a choice that does not exist. An include's site is
/// its file's header, checked on the file's body.
fn check_pass_through_ports(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for group in project.groups.iter().filter(|g| !matches!(g.kind, weft_core::project::GroupKind::Call { .. })) {
        let file = group.source_file.as_deref();
        let span = group.header_span.or(group.span).unwrap_or_default();
        for input in group.in_ports.iter().filter(|p| p.passes_through && !p.required) {
            let noun = group.kind.noun();
            let why = if matches!(group.kind, weft_core::project::GroupKind::Loop { .. }) {
                "a loop needs only the ports it iterates (`over`) or carries (`carry`) to start, and"
            } else {
                "a group needs none of its ports to start, and"
            };
            push(d, &[group.id.as_str()], file, span, Severity::Error, "optional-pass-through-port", format!(
                "'{name}' is written `{port}?` on {noun} '{group_name}', but {why} '{port}' hands what reaches it \
                 straight to the nodes inside, so it is never required or optional itself: whether it must be \
                 connected depends on whether a node inside needs it. Remove the `?`",
                name = input.name,
                port = input.name,
                group_name = weft_core::project::plain_id(&group.id),
            ));
        }
    }
}

fn check_metadata_ports(project: &ProjectDefinition, catalog: &dyn MetadataCatalog, d: &mut Vec<Diagnostic>) {
    let read: std::collections::HashSet<(&str, &str)> = project.edges.iter()
        .filter(|edge| !edge.from_group_port())
        .map(|edge| (edge.source.as_str(), edge.source_port()))
        .collect();
    for node in &project.nodes {
        let file = node.source_file.as_deref();
        let span = node.header_span_or_default();
        for port in &node.metadata_ports {
            if node.inputs.iter().any(|p| p.name == port.name) || node.outputs.iter().any(|p| p.name == port.name) {
                push(d, &[node.part()], file, span, Severity::Error, "duplicate-port", format!(
                    "node '{}' has a port named '{}' besides its metadata port of that name; rename the port you added",
                    author_name(node), port.name,
                ));
            }
        }
        if !node.features.pairs_added_ports {
            continue;
        }
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let added = weft_core::rules::CustomPorts::of(node, meta);
        for input in &added.inputs {
            if meta.outputs.iter().any(|o| o.name == *input) {
                let (key_file, key_span) = cfg_anchor(node, input);
                push(d, &[node.part()], key_file.or(file), key_span, Severity::Error, "unpaired-port", format!(
                    "node '{}': '{input}' is the name of one of its own outputs, so the input you added cannot have \
                     an output of its own. Give it another name",
                    author_name(node),
                ));
            }
        }
        // The language's `_error` output is the metadata's own, so it is
        // never an added output.
        for output in &added.outputs {
            if !added.inputs.contains(output) {
                push(d, &[node.part()], file, span, Severity::Error, "unpaired-port", format!(
                    "node '{}': output '{}' matches no input. Each output of this node is where the input of the same \
                     name sends what it matches: add that input (`{}.{} = ...`), or remove the output",
                    author_name(node), output, author_name(node), output,
                ));
            } else if !read.contains(&(node.id.as_str(), output.as_str())) {
                let (key_file, key_span) = cfg_anchor(node, output);
                push(d, &[node.part()], key_file.or(file), key_span, Severity::Error, "unpaired-port", format!(
                    "node '{}': nothing reads '{}.{}', so a value that input '{}' matches would be dropped. Wire \
                     `{}.{}` to what handles it",
                    author_name(node), author_name(node), output, output, author_name(node), output,
                ));
            }
        }
    }
    for group in &project.groups {
        let file = group.source_file.as_deref();
        let span = group.header_span.or(group.span).unwrap_or_default();
        for port in &group.metadata_ports {
            if group.in_ports.iter().any(|p| p.name == port.name) || group.out_ports.iter().any(|p| p.name == port.name) {
                push(d, &[group.id.as_str()], file, span, Severity::Error, "duplicate-port", format!(
                    "{} '{}' has a port named '{}' besides its metadata port of that name: `self.{}` would mean either",
                    group.kind.noun(), weft_core::project::plain_id(&group.id), port.name, port.name,
                ));
            }
            // An include's site carries its body's value, checked on the body.
            if matches!(group.kind, weft_core::project::GroupKind::Call { .. }) {
                continue;
            }
            let Some(value) = &port.value else { continue };
            if let Err(why) = port.port_type.validate_value(value) {
                push(d, &[group.id.as_str()], file, port.span.unwrap_or(span), Severity::Error, "config-type-mismatch", format!(
                    "the value of metadata port '{}.{}' does not fit its type '{}': {why}",
                    weft_core::project::plain_id(&group.id), port.name, port.port_type,
                ));
            }
        }
    }
}

/// The entries a node derives its ports from (a form's `fields`, a
/// switch's `cases`) have to say what they claim to say, or the ports
/// they were supposed to make just are not there and every wire to them
/// reads as a typo.
///
/// Rules, all about one entry list:
///  - the value under the key is a LIST of objects.
///  - every entry names a `kind` this node offers.
///  - every entry names its port, under the key its spec asks for.
///  - every entry carries only keys its spec declares, so a mistyped
///    test (`euqals`) fails here instead of quietly becoming a case
///    that matches everything.
///  - every test carries the shape it declares: a value the matched
///    input could hold, a list of them, a number, or a regular
///    expression that compiles.
///  - a CATCH-ALL entry (one carrying no test at all) is unique and
///    last, since the entries are matched in order and anything after it
///    could never be reached.
fn check_config_derived_ports(
    project: &ProjectDefinition,
    catalog: &dyn MetadataCatalog,
    d: &mut Vec<Diagnostic>,
) {
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let Some(ports_from_config) = &meta.ports_from_config else { continue };
        let (file, key_span) = cfg_anchor(node, &ports_from_config.field);
        let Some(raw) = node.written_value(&ports_from_config.field) else { continue };
        let Some(entries) = raw.as_array() else {
            push(
                d,
                &[node.part()],
                file,
                key_span,
                Severity::Error,
                "config-ports-not-a-list",
                format!(
                    "node '{}': '{}' holds the list this node's ports come from, so it must be \
                     a list",
                    author_name(node), ports_from_config.field
                ),
            );
            continue;
        };

        let kinds: Vec<&str> = ports_from_config.specs.iter().map(|s| s.kind.as_str()).collect();
        let mut catch_all_at: Option<usize> = None;
        for (index, entry) in entries.iter().enumerate() {
            let Some(obj) = entry.as_object() else {
                push(
                    d,
                    &[node.part()],
                    file,
                    key_span,
                    Severity::Error,
                    "config-entry-not-an-object",
                    format!(
                        "node '{}': {} of '{}' is not an object",
                        author_name(node),
                        entry_label(index, None),
                        ports_from_config.field
                    ),
                );
                continue;
            };
            let kind = obj.get("kind").and_then(|v| v.as_str()).unwrap_or_default();
            let Some(spec) = ports_from_config.spec_for(kind) else {
                push(
                    d,
                    &[node.part()],
                    file,
                    key_span,
                    Severity::Error,
                    "unknown-config-entry-kind",
                    format!(
                        "node '{}': {} of '{}' has kind '{}', which {} does not offer \
                         (it takes: {})",
                        author_name(node),
                        entry_label(index, None),
                        ports_from_config.field,
                        kind,
                        node.node_type,
                        kinds.join(", ")
                    ),
                );
                continue;
            };
            let port_name = obj.get(&spec.key_field).and_then(|v| v.as_str()).unwrap_or_default();
            if port_name.is_empty() {
                push(
                    d,
                    &[node.part()],
                    file,
                    key_span,
                    Severity::Error,
                    "config-entry-without-a-port",
                    format!(
                        "node '{}': {} of '{}' needs a '{}' naming the port it adds",
                        author_name(node),
                        entry_label(index, None),
                        ports_from_config.field,
                        spec.key_field
                    ),
                );
                continue;
            }
            // Every key an entry may carry, so anything else is a
            // mistyped one, null-valued or not: null-is-absent applies
            // to a DECLARED field's value (the loop below), never to
            // the key itself, or a typo'd key with a cleared value
            // would vanish from diagnostics.
            let mut allowed: Vec<&str> = vec!["kind", spec.key_field.as_str()];
            allowed.extend(spec.fields.iter().map(|f| f.key.as_str()));
            let unknown: Vec<&String> =
                obj.keys().filter(|key| !allowed.contains(&key.as_str())).collect();
            for key in &unknown {
                push(
                    d,
                    &[node.part()],
                    file,
                    key_span,
                    Severity::Error,
                    "unknown-config-entry-key",
                    format!(
                        "node '{}': {} of '{}' carries '{}', which a '{}' entry \
                         does not take (it takes: {})",
                        author_name(node),
                        entry_label(index, Some(port_name)),
                        ports_from_config.field,
                        key,
                        spec.kind,
                        allowed.join(", ")
                    ),
                );
            }
            // What the entries are matched against, when they compete.
            // Its type is what holds a test to a value it could see.
            let matched_type = ports_from_config
                .match_input
                .as_ref()
                .and_then(|name| node.inputs.iter().find(|p| p.name == *name))
                .map(|p| p.port_type.clone());
            for field in &spec.fields {
                // A present `null` counts as absent, the same rule record
                // types apply to their optional fields: the editor clears a
                // box by writing null, and the runtime reads null as "not
                // filled in" (a form field's label falls back to its key).
                // SYNC: null-is-absent <->
                //       packages/weft-graph/src/webview/lib/utils/port-specs.ts (hasValue),
                //       catalog/human/form_helpers.rs (build_form_fields)
                match obj.get(field.key.as_str()).filter(|v| !v.is_null()) {
                    Some(value) => {
                        // Where the list IS the choice set, an empty one
                        // on a required field chose nothing: the branch
                        // could never fire (a `value_list` `in`) or the
                        // human would get an empty dropdown. Where a
                        // list is one VALUE being matched, `[]` is a
                        // legitimate value and says nothing here.
                        // SYNC: empty-list-is-nothing-chosen <->
                        //       crates/weft-core/src/node.rs
                        //       (SpecField::empty_list_means_nothing_chosen),
                        //       packages/weft-graph/src/webview/lib/utils/port-specs.ts
                        //       (isEmptyChoiceSet)
                        let empty_choice_set = field.required
                            && field.empty_list_means_nothing_chosen()
                            && value.as_array().is_some_and(|a| a.is_empty());
                        if empty_choice_set {
                            push(
                                d,
                                &[node.part()],
                                file,
                                key_span,
                                Severity::Error,
                                "config-entry-bad-value",
                                format!(
                                    "node '{}': {} of '{}' sets `{}` to an empty list, \
                                     which chooses nothing, and a '{}' needs at least one",
                                    author_name(node),
                                    entry_label(index, Some(port_name)),
                                    ports_from_config.field,
                                    field.key,
                                    spec.kind
                                ),
                            );
                        } else if let Some(problem) =
                            spec_field_problem(field, value, matched_type.as_ref())
                        {
                            push(
                                d,
                                &[node.part()],
                                file,
                                key_span,
                                Severity::Error,
                                "config-entry-bad-value",
                                format!(
                                    "node '{}': {} of '{}' sets `{}`, and {}",
                                    author_name(node),
                                    entry_label(index, Some(port_name)),
                                    ports_from_config.field,
                                    field.key,
                                    problem
                                ),
                            );
                        }
                    }
                    None if field.required => push(
                        d,
                        &[node.part()],
                        file,
                        key_span,
                        Severity::Error,
                        "config-entry-missing-value",
                        format!(
                            "node '{}': {} of '{}' is a '{}', which needs a '{}'",
                            author_name(node),
                            entry_label(index, Some(port_name)),
                            ports_from_config.field,
                            spec.kind,
                            field.key
                        ),
                    ),
                    None => {}
                }
            }
            // A kind that takes anything is the branch reached when no
            // earlier entry matched, so a second one can never fire.
            if spec.catch_all {
                match catch_all_at {
                    Some(first) => push(
                        d,
                        &[node.part()],
                        file,
                        key_span,
                        Severity::Error,
                        "duplicate-catch-all",
                        format!(
                            "node '{}': entries {} and {} of '{}' both match anything; the \
                             second can never be reached",
                            author_name(node),
                            first + 1,
                            index + 1,
                            ports_from_config.field
                        ),
                    ),
                    None => catch_all_at = Some(index),
                }
            }
        }
        if let Some(at) = catch_all_at {
            if at + 1 != entries.len() {
                push(
                    d,
                    &[node.part()],
                    file,
                    key_span,
                    Severity::Error,
                    "catch-all-not-last",
                    format!(
                        "node '{}': entry {} of '{}' matches anything, so the {} after it can \
                         never be reached. Write it last",
                        author_name(node),
                        at + 1,
                        ports_from_config.field,
                        entries.len() - at - 1
                    ),
                );
            }
        }
    }
}

/// How a diagnostic names one entry of a config-derived list: its
/// position, plus the port it names once the entry has gotten that far
/// (two entries can carry the same port name, so the position stays).
fn entry_label(index: usize, port_name: Option<&str>) -> String {
    match port_name {
        // `None`: the diagnostic fires before the entry has named its
        // port, so there is nothing to quote.
        None => format!("entry {}", index + 1),
        Some(name) => format!("entry {} ('{}')", index + 1, name),
    }
}

/// What is wrong with one value a kind asked for, or `None` when it
/// fits. Reads only the SHAPE the spec declares, so the compiler holds
/// it honest without knowing what any node means by the key.
fn spec_field_problem(
    field: &weft_core::node::SpecField,
    value: &serde_json::Value,
    matched_type: Option<&WeftType>,
) -> Option<String> {
    use weft_core::node::SpecShape;
    // Every shape below `Typed` measures the value against the input
    // named by `matchInput`. Manifest validation guarantees that input
    // exists whenever a spec declares such a field, so a missing type
    // here is a broken manifest; report it rather than skipping the
    // check silently.
    fn matched(matched_type: Option<&WeftType>) -> Result<&WeftType, String> {
        matched_type.ok_or_else(|| {
            "its spec matches entries against an input this node does not declare \
             (broken node metadata: `matchInput` names no input)"
                .to_string()
        })
    }
    match field.shape {
        SpecShape::Typed => {
            let declared = field.value_type.as_ref()?;
            (!declared.accepts_runtime_value(value))
                .then(|| format!("{value} is not a {declared}"))
        }
        SpecShape::Value => {
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            (!ty.accepts_runtime_value(value))
                .then(|| format!("{value} is not a {ty}, which is what it is matched against"))
        }
        SpecShape::ValueList => {
            let Some(items) = value.as_array() else {
                return Some(format!("{value} is not a list of values to match against"));
            };
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            items.iter().find(|item| !ty.accepts_runtime_value(item)).map(|item| {
                format!("{item} is not a {ty}, which is what it is matched against")
            })
        }
        SpecShape::Number => {
            if !value.is_number() {
                return Some(format!("{value} is not a number, so there is nothing to compare"));
            }
            // Comparing a number against something that never holds one
            // is a branch that can never be taken.
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            (!ty.accepts_runtime_value(&serde_json::json!(0)))
                .then(|| format!("what it matches against is a {ty}, never a number"))
        }
        SpecShape::Element => {
            let ty = match matched(matched_type) {
                Ok(t) => t,
                Err(p) => return Some(p),
            };
            if ty.is_unresolved() {
                return None;
            }
            match ty.structural() {
                WeftType::List(item) => (!item.accepts_runtime_value(value))
                    .then(|| format!("{value} is not a {item}, so no {ty} could hold it")),
                WeftType::Primitive(weft_core::weft_type::WeftPrimitive::String) => value
                    .as_str()
                    .is_none()
                    .then(|| format!("{value} is not text, so no text could contain it")),
                other => Some(format!(
                    "there is nothing to look inside: what it matches against is a {other}, \
                     not a list or text"
                )),
            }
        }
        SpecShape::Regex => match value.as_str() {
            None => Some(format!("{value} is not a regular expression")),
            Some(pattern) => regex::Regex::new(pattern)
                .err()
                .map(|e| format!("{value} is not a regular expression that compiles: {e}")),
        },
    }
}

// ─── group 5: loop config validation ─────────────────────────────────────────

/// Validate the config block on each `Loop` decl (via its `LoopIn` /
/// `LoopOut` boundary nodes). Rules per the plan:
///  - `parallel` defaults to false (sequential) when omitted.
///  - `over` / `carry` reference declared ports.
///  - `parallel: true` AND `carry` non-empty → `parallel-with-carry`.
///  - `parallel: true` AND `over` empty → `parallel-without-over`.
///  - `parallel: true` AND `self.done = ...` in body → `parallel-with-done`.
///  - Port in both `over` and `carry` → `over-and-carry-overlap`.
///  - Reserved port names `index` (input) / `done` (output) → `reserved-port-name`.
///  - Gather output declared as `List[T]` instead of `List[T | Null]` →
///    `gather-output-must-be-nullable`.
///  - Boundary unpaired → `loop-boundary-unpaired`.
fn check_loop_config(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use std::collections::{HashMap, HashSet};

    // Collect LoopIn / LoopOut node pairs keyed by their shared group id.
    let mut ins: HashMap<&str, &NodeDefinition> = HashMap::new();
    let mut outs: HashMap<&str, &NodeDefinition> = HashMap::new();
    for n in &project.nodes {
        let Some(gb) = &n.group_boundary else { continue };
        match n.node_type.as_str() {
            weft_core::project::boundary_types::LOOP_IN => { ins.insert(gb.group_id.as_str(), n); }
            weft_core::project::boundary_types::LOOP_OUT => { outs.insert(gb.group_id.as_str(), n); }
            _ => {}
        }
    }
    for (gid, in_node) in &ins {
        if !outs.contains_key(gid) {
            push(d, &[*gid], in_node.source_file.as_deref(), in_node.header_span_or_default(),
                Severity::Error, "loop-boundary-unpaired",
                format!("loop '{gid}' has LoopIn but no matching LoopOut"));
        }
    }
    for (gid, out_node) in &outs {
        if !ins.contains_key(gid) {
            push(d, &[*gid], out_node.source_file.as_deref(), out_node.header_span_or_default(),
                Severity::Error, "loop-boundary-unpaired",
                format!("loop '{gid}' has LoopOut but no matching LoopIn"));
        }
    }

    for (gid, in_node) in &ins {
        let Some(out_node) = outs.get(gid).copied() else { continue };
        let cfg = &in_node.config;
        let span = in_node.header_span_or_default();
        let file = in_node.source_file.as_deref();

        // Unknown config keys are rejected loudly: a typo'd knob
        // (`max_itres: 10`) silently running the loop uncapped is
        // exactly the masked-bug class the language forbids.
        // `parentId` is the compiler-internal boundary pointer merged
        // in at flatten time, never a user key (lowering rejects a
        // user-written `parentId` before the merge).
        const KNOWN_LOOP_KEYS: &[&str] =
            &["parentId", "parallel", "over", "carry", "maxIters", "trimOnMismatch"];
        if let Some(obj) = cfg.as_object() {
            for key in obj.keys() {
                if !KNOWN_LOOP_KEYS.contains(&key.as_str()) {
                    cfg_push(d, in_node, key, Severity::Error, "loop-unknown-config-field",
                        format!(
                            "loop '{gid}': unknown config field '{key}' (known: parallel, \
                             over, carry, maxIters, trimOnMismatch)"
                        ));
                }
            }
        }

        // `parallel` defaults to false (sequential, the safer mode:
        // carry / self.done work, no ordering surprises); the flatten
        // step materializes the default, so it is always present
        // here. A wrong-typed value (`parallel: "true"`) is an ERROR,
        // never a coercion: silently running sequential would also
        // silently skip every parallel-interplay rule below.
        let parallel: bool = match cfg.get("parallel") {
            Some(serde_json::Value::Bool(b)) => *b,
            Some(other) => {
                cfg_push(d, in_node, "parallel", Severity::Error, "loop-parallel-not-boolean",
                    format!("loop '{gid}': `parallel` must be a boolean literal (got {other})"));
                continue;
            }
            // Absent means the flatten step did NOT materialize the
            // default into the LoopIn config. The default lives in ONE
            // place (flatten); validate verifies it is there rather than
            // re-defaulting here, because a silent default would also
            // silently skip every parallel-interplay rule below.
            None => {
                cfg_push(d, in_node, "parallel", Severity::Error, "loop-config-missing-parallel",
                    format!("loop '{gid}': internal invariant broken: flatten did not materialize `parallel` into the LoopIn config"));
                continue;
            }
        };

        // `maxIters`: non-negative integer literal when present
        // (`maxIters: 0` is a legal zero-iteration cap).
        if let Some(v) = cfg.get("maxIters") {
            if v.as_u64().is_none() {
                cfg_push(d, in_node, "maxIters", Severity::Error, "loop-max-iters-not-integer",
                    format!("loop '{gid}': `maxIters` must be a non-negative integer literal (got {v})"));
            }
        }
        // `trimOnMismatch`: boolean literal when present.
        if let Some(v) = cfg.get("trimOnMismatch") {
            if !v.is_boolean() {
                cfg_push(d, in_node, "trimOnMismatch", Severity::Error, "loop-trim-not-boolean",
                    format!("loop '{gid}': `trimOnMismatch` must be a boolean literal (got {v})"));
            }
        }

        // A malformed over/carry (a scalar value, or non-string
        // entries) is already rejected at lowering time
        // (`read_loop_port_list` pushes [loop-config-malformed]);
        // validate must not pile a second diagnostic
        // (parallel-without-over, unbounded, ...) onto the same
        // mistake. `lists_ok` gates exactly the rules that READ
        // over/carry; everything else (reserved names, parallel-with-
        // done, type checks) still reports in the same compile.
        let well_formed_list = |key: &str| {
            cfg.get(key).is_none_or(|v| {
                v.as_array().is_some_and(|arr| arr.iter().all(|e| e.is_string()))
            })
        };
        let lists_ok = well_formed_list("over") && well_formed_list("carry");
        let over: Vec<String> = read_loop_port_list_vetted(cfg.as_object(), "over");
        let carry: Vec<String> = read_loop_port_list_vetted(cfg.as_object(), "carry");

        if lists_ok && parallel && !carry.is_empty() {
            cfg_push(d, in_node, "carry", Severity::Error, "parallel-with-carry",
                format!("loop '{gid}': parallel: true forbids carry ports (carry implies sequential)"));
        }
        if lists_ok && parallel && over.is_empty() {
            cfg_push(d, in_node, "parallel", Severity::Error, "parallel-without-over",
                format!("loop '{gid}': parallel: true requires a non-empty 'over' list"));
        }

        // `self.done = ...` writes are detectable here: lowering maps
        // them to an edge whose target is `{loop_id}__out` on port
        // `done`, so a scope-local scan reduces to "any edge whose
        // target is this loop's LoopOut id and target port is `done`".
        // A `self.done` write inside a nested loop's body targets that
        // nested loop's `__out`, never this one, so the check is
        // naturally scope-local.
        let loop_out_id = weft_core::project::loop_out_id(gid);
        let done_wired = project.edges.iter().any(|e| {
            e.target == loop_out_id && e.target_handle.as_deref() == Some("done")
        });

        // parallel-with-done: in parallel mode, any `self.done` write
        // inside THIS loop's body is rejected.
        if parallel && done_wired {
            push(d, &[*gid], file, span, Severity::Error, "parallel-with-done",
                format!("loop '{gid}': parallel: true forbids `self.done = ...` connections in the body"));
        }

        // loop-unbounded-no-termination: a SEQUENTIAL loop with no
        // `over` (nothing to exhaust), no `maxIters` (no cap), and no
        // `self.done` write (no vote to stop) is provably infinite.
        // Reject at compile time. A loop that has ANY of the three is
        // the user's own program: trusted, unbounded by the runtime.
        if lists_ok && !parallel && over.is_empty() && cfg.get("maxIters").is_none() && !done_wired {
            push(d, &[*gid], file, span, Severity::Error, "loop-unbounded-no-termination",
                format!(
                    "loop '{gid}': sequential loop declares no 'over', no 'maxIters', and \
                     never writes `self.done`; it can never terminate. Iterate a list with \
                     `over: [...]`, cap it with `maxIters`, or wire `self.done = ...` in the body."
                ));
        }

        let carry_set: HashSet<&String> = carry.iter().collect();
        if lists_ok {
            for p in &over {
                if carry_set.contains(p) {
                    cfg_push(d, in_node, "over", Severity::Error, "over-and-carry-overlap",
                        format!("loop '{gid}': port '{p}' listed in both 'over' and 'carry'"));
                }
            }
        }

        // Reserved port names: 'index' as user input, 'done' as user output.
        for port in &in_node.inputs {
            if port.name == "index" {
                push(d, &[*gid], file, span, Severity::Error, "reserved-port-name",
                    format!("loop '{gid}': 'index' is reserved (the implicit per-iteration index port)"));
            }
        }
        for port in &out_node.outputs {
            if port.name == "done" {
                push(d, &[*gid], file, span, Severity::Error, "reserved-port-name",
                    format!("loop '{gid}': 'done' is reserved (the implicit done-vote port)"));
            }
        }

        // Gather outputs must be declared `List[T | Null]`. The
        // reserved name `done` already errored above; flagging its
        // nullability too would double-report one mistake. Which
        // outputs are carries is read off the carry list, so a
        // malformed one skips this rule (`lists_ok`).
        for port in &out_node.outputs {
            if !lists_ok || carry_set.contains(&port.name) || port.name == "done" {
                continue;
            }
            if !is_list_of_nullable(&port.port_type) {
                push(d, &[*gid], file, span, Severity::Error, "gather-output-must-be-nullable",
                    format!(
                        "loop '{gid}': gather output '{}' must be declared as List[T | Null] (was {}); per-iteration body failures produce null slots",
                        port.name, port.port_type,
                    ));
            }
        }

        // over ports must exist on LoopIn and be List[T] or
        // Generator[T]. A stream in `over` must be ALONE: zipping a
        // stream against a list (or another stream) has no defined
        // count to trim against, and the runtime pulls exactly one
        // stream per loop.
        let mut over_has_stream = false;
        for p in &over {
            match in_node.inputs.iter().find(|x| &x.name == p) {
                None => {
                    cfg_push(d, in_node, "over", Severity::Error, "loop-over-unknown-port",
                        format!("loop '{gid}': 'over' references unknown input port '{p}'"));
                }
                Some(port) if port.port_type.as_generator().is_some() => {
                    over_has_stream = true;
                }
                Some(port) => match port.port_type.structural() {
                    weft_core::weft_type::WeftType::List(_) => {}
                    other => {
                        cfg_push(d, in_node, "over", Severity::Error, "over-not-a-list",
                            format!(
                                "loop '{gid}': 'over' port '{p}' must be List[T] or \
                                 Generator[T], got {other}"
                            ));
                    }
                },
            }
        }
        if over_has_stream && over.len() > 1 {
            cfg_push(d, in_node, "over", Severity::Error, "over-stream-not-alone",
                format!(
                    "loop '{gid}': a Generator port in 'over' must be the ONLY over port \
                     (a loop iterates one stream at a time; zip upstream if you need more)"
                ));
        }
        // A Generator input on the loop that is NOT the over port has
        // no meaning: LoopIn broadcasts every non-over input verbatim
        // into each iteration, and a stream cannot be copied per
        // iteration (it is a live edge with one taker).
        for port in &in_node.inputs {
            if port.port_type.as_generator().is_some() && !over.contains(&port.name) {
                push(d, &[*gid], file, span, Severity::Error, "generator-not-iterated",
                    format!(
                        "loop '{gid}': Generator input '{}' must be iterated (`over: \
                         [\"{}\"]`); a stream cannot broadcast into the loop body",
                        port.name, port.name
                    ));
            }
        }
        // A stream cannot be carried between iterations either: a
        // carry value round-trips the journal as JSON, and a stream is
        // a live edge.
        for p in &carry {
            let carried_generator = out_node
                .outputs
                .iter()
                .find(|x| &x.name == p)
                .map(|x| x.port_type.as_generator().is_some())
                .unwrap_or(false);
            if carried_generator {
                cfg_push(d, in_node, "carry", Severity::Error, "generator-not-carriable",
                    format!(
                        "loop '{gid}': carry port '{p}' is a Generator; a stream is a live \
                         edge between two running nodes and cannot be carried across \
                         iterations"
                    ));
            }
        }
        // Carry ports: the output side is the source of truth; the
        // input side (synthesized by lowering when not user-declared)
        // must mirror it. The single home for carry semantics:
        // lowering only synthesizes, validate reports.
        for p in &carry {
            let Some(out_port) = out_node.outputs.iter().find(|x| &x.name == p) else {
                cfg_push(d, in_node, "carry", Severity::Error, "loop-carry-unknown-port",
                    format!("loop '{gid}': 'carry' references unknown output port '{p}'"));
                continue;
            };
            let Some(in_port) = in_node.inputs.iter().find(|x| &x.name == p) else {
                // Unreachable when lowering synthesized the input;
                // reachable if the flattened shape drifted.
                cfg_push(d, in_node, "carry", Severity::Error, "loop-carry-unknown-port",
                    format!("loop '{gid}': carry port '{p}' has no matching input on the loop"));
                continue;
            };
            if in_port.port_type != out_port.port_type {
                cfg_push(d, in_node, "carry", Severity::Error, "carry-port-type-mismatch",
                    format!(
                        "loop '{gid}': carry port '{p}' declared with mismatched types \
                         (input: {}, output: {}); both sides of a carry port must be the same type",
                        in_port.port_type, out_port.port_type,
                    ));
            }
        }
    }
}

fn is_list_of_nullable(ty: &weft_core::weft_type::WeftType) -> bool {
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    let WeftType::List(inner) = ty else { return false };
    match inner.as_ref() {
        WeftType::Union(members) => members.iter().any(|m| matches!(m, WeftType::Primitive(WeftPrimitive::Null))),
        WeftType::Primitive(WeftPrimitive::Null) => true,
        _ => false,
    }
}

// ─── group 6: when a node runs, bounds, placeholders ────────────────────────

/// run-undecided: a node whose wired inputs are all optional runs
/// whether or not anything arrives on them (an optional input takes
/// absence and failure alike), so nothing in the program says WHEN it
/// runs. Its gate is where that is said, and the build asks for it.
///
/// Not asked of:
/// - a node with no wired input (a root runs when its scope does);
/// - a node using `@require_one_of` (that group is when it runs);
/// - a join (`features.join`: "the first of these branches" is when it
///   runs, and it closes its output itself when none came);
/// - a trigger (it fires on its event; its inputs are settings);
/// - a loop's boundaries (compiler-made: every loop port is optional
///   there, and each node inside decides for itself);
/// - an `@include` drawn as one opaque node (its ports are the included
///   file's header, checked where the file is built);
/// - a node fed through a group's port when that group's gate is wired
///   (the gate decided the group runs on what that port carries), from a
///   constant written on a group's port (`g.x = "hi"`, sent the moment
///   the group opens), or by a port a loop requires (its `over` item) or
///   makes itself (its `index`), each read through the scopes around it.
/// An input required only once wired counts as required here, which is
/// what it is.
fn check_run_decided(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    let mut wired_ports: std::collections::HashMap<&str, std::collections::BTreeSet<&str>> =
        std::collections::HashMap::new();
    for e in &project.edges {
        if let Some(port) = e.target_handle.as_deref() {
            wired_ports.entry(e.target.as_str()).or_default().insert(port);
        }
    }
    let by_id: std::collections::HashMap<&str, &NodeDefinition> =
        project.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let scope = DecidingScopes { project, wired_ports: &wired_ports, by_id: &by_id };
    for node in &project.nodes {
        // A loop's boundaries are compiler-made: every loop port is optional
        // there, and each node inside decides for itself.
        if node.group_boundary.is_some()
            || node.features.is_trigger
            || node.features.join
            || node.include_path.is_some()
            || !node.features.one_of_required.is_empty()
        {
            continue;
        }
        let Some(wired) = wired_ports.get(node.id.as_str()) else { continue };
        if wired.iter().any(|port| weft_core::run::skip::is_gate_port(port)) {
            continue;
        }
        let data: Vec<&weft_core::project::InputDefinition> = node
            .inputs
            .iter()
            .filter(|p| !weft_core::run::skip::is_gate_port(&p.name) && wired.contains(p.name.as_str()))
            .collect();
        if data.is_empty()
            || data.iter().any(|p| p.is_required(true))
            || data.iter().any(|p| scope.decides(node.id.as_str(), p.name.as_str()))
        {
            continue;
        }
        let name = author_name(node);
        push(
            d,
            &[node.part()],
            node.source_file.as_deref(),
            node.header_span_or_default(),
            Severity::Error,
            "run-undecided",
            format!(
                "when '{name}' runs is undecided: every input wired into it is optional ({}), so \
                 it would run whether or not anything arrives on them. Gate it on what must have \
                 happened first (`{name}._should_flow = <that node>.<port>`), or make the input it \
                 cannot do without required (drop its `?`), or add `@require_one_of(...)` over \
                 the inputs it needs one of",
                data.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", "),
            ),
        );
    }
}

/// What `check_run_decided` reads off the scopes a node sits in: whether
/// what a wire into it carries is there whenever the scope runs.
struct DecidingScopes<'a> {
    project: &'a ProjectDefinition,
    wired_ports: &'a std::collections::HashMap<&'a str, std::collections::BTreeSet<&'a str>>,
    by_id: &'a std::collections::HashMap<&'a str, &'a NodeDefinition>,
}

impl DecidingScopes<'_> {
    /// Whether input `port` of `node` is fed only by wires that each carry
    /// something whenever the scope they enter runs ([`Self::wire_decides`]).
    /// Every wire has to: an included file's member is fed through each
    /// site that includes it, and each site decides for itself.
    fn decides(&self, node: &str, port: &str) -> bool {
        let mut wires = self.project.edges.iter()
            .filter(|e| e.target == node && e.target_port() == port && !e.to_group_port())
            .peekable();
        wires.peek().is_some() && wires.all(|e| self.wire_decides(e))
    }

    /// Whether `wire` carries something whenever the scope it enters runs:
    /// it passes into a group (or a site) whose gate is wired, it starts at
    /// a constant written on a group's port, or it comes out of a loop's
    /// `LoopIn` on a port the loop requires, makes itself, has a constant
    /// on, or is fed that way from the scope around it.
    fn wire_decides(&self, wire: &weft_core::project::Edge) -> bool {
        let gated = |id: &str| {
            self.wired_ports.get(id).is_some_and(|ports| ports.iter().any(|p| weft_core::run::skip::is_gate_port(p)))
        };
        if wire.via.iter().any(|c| c.side == weft_core::project::CrossingSide::In && gated(&c.group)) {
            return true;
        }
        if wire.from_group_port() {
            return starts_at_group_constant(self.project, wire);
        }
        let Some(boundary) = self.by_id.get(wire.source.as_str()).filter(|b| {
            b.group_boundary.as_ref().is_some_and(|g| g.role == weft_core::project::GroupBoundaryRole::In)
        }) else {
            return false;
        };
        let source_port = wire.source_port();
        gated(&boundary.id)
            || match boundary.inputs.iter().find(|i| i.name == source_port) {
                None => true,
                Some(input) => {
                    input.is_required(true)
                        || boundary.port_literals.get(source_port).is_some_and(|value| {
                            weft_core::run::ready::literal_is_data(boundary, source_port, value)
                        })
                        || self.decides(&boundary.id, source_port)
                }
            }
    }
}

/// type-bound: a type variable resolved to something its bound does not
/// take (`KeepFile`'s `T: File` given a `String`, a group's `[T: Media]`
/// fed a `Blob`). A loop's two boundaries share its variables and carry
/// the same bounds, so the loop is reported once, on its In half.
// SYNC: check_type_bounds <-> packages/weft-graph/src/webview/lib/projection/header-ports.ts pinnedBoundProblem
fn check_type_bounds(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for node in &project.nodes {
        if node.group_boundary.as_ref().is_some_and(|b| b.role == weft_core::project::GroupBoundaryRole::Out) {
            continue;
        }
        // At every place the node runs: an included file's node holds its
        // bound to what each site makes the variable.
        let mut reported = std::collections::BTreeSet::new();
        let params = std::iter::once(node.type_params.as_slice())
            .chain(node.place_types.iter().map(|place| place.type_params.as_slice()));
        for param in params.flatten() {
            if let Some(why) = param.violation().filter(|why| reported.insert(why.clone())) {
                push(
                    d,
                    &[node.part()],
                    node.source_file.as_deref(),
                    node.header_span_or_default(),
                    Severity::Error,
                    "type-bound",
                    format!("'{}': {why}", author_name(node)),
                );
            }
        }
    }
    // A group's bounds, the same way at every place it runs.
    for group in &project.groups {
        let mut reported = std::collections::BTreeSet::new();
        let params = std::iter::once(group.type_params.as_slice())
            .chain(group.place_types.iter().map(|place| place.type_params.as_slice()));
        for param in params.flatten() {
            if let Some(why) = param.violation().filter(|why| reported.insert(why.clone())) {
                push(
                    d,
                    &[group.id.as_str()],
                    group.source_file.as_deref(),
                    group.header_span.or(group.span).unwrap_or_default(),
                    Severity::Error,
                    "type-bound",
                    format!("'{}': {why}", weft_core::project::plain_id(&group.id)),
                );
            }
        }
    }
}

/// placeholder-not-an-input / input-not-a-placeholder: a node type that
/// says its text setting's placeholders name its inputs
/// (`placeholdersAreInputs`: a query's `$userId`, a template's
/// `{{name}}`) has the two held to each other here, knowing nothing of
/// what the text is: every placeholder the written text holds must be an
/// input the program added to the node, and every input it added must
/// appear in the text. Only text written in the source can be read (a
/// value in the braces, or a `@file` the parse already read in); a
/// setting arriving on a wire, or an `@asset` read at build, is checked
/// when the node runs.
fn check_placeholders(project: &ProjectDefinition, catalog: &dyn MetadataCatalog, d: &mut Vec<Diagnostic>) {
    for node in &project.nodes {
        let Some(meta) = catalog.lookup(&node.node_type) else { continue };
        let Some(spec) = &meta.placeholders_are_inputs else { continue };
        let Some(text) = node.port_literals.get(&spec.setting).and_then(|v| v.as_str()) else { continue };
        if crate::file_ref::parse_marker(text).is_some() {
            continue;
        }
        let Ok(regex) = spec.regex() else { continue };
        let name = author_name(node);
        let added: Vec<&str> = node
            .inputs
            .iter()
            .map(|p| p.name.as_str())
            .filter(|p| !weft_core::run::skip::is_gate_port(p) && !meta.inputs.iter().any(|i| i.name == *p))
            .collect();
        let used = match spec.names_in(&regex, text) {
            Ok(used) => used,
            Err(why) => {
                cfg_push(d, node, &spec.setting, Severity::Error, "placeholder-not-an-input", why);
                continue;
            }
        };
        for placeholder in &used {
            if added.contains(&placeholder.as_str()) {
                continue;
            }
            let hint = match did_you_mean(placeholder, &added) {
                Some(close) => format!("; did you mean `{close}`?"),
                None if added.is_empty() => format!(
                    ". Declare it as an input: `{name} = {}({placeholder}: String)`",
                    node.node_type
                ),
                None => format!(". Its inputs are {}", added.join(", ")),
            };
            cfg_push(
                d,
                node,
                &spec.setting,
                Severity::Error,
                "placeholder-not-an-input",
                format!(
                    "'{name}.{}' uses the placeholder `{placeholder}`, which is not an input of '{name}'{hint}",
                    spec.setting
                ),
            );
        }
        for input in added.iter().filter(|input| !used.iter().any(|u| u == *input)) {
            cfg_push(
                d,
                node,
                &spec.setting,
                Severity::Error,
                "input-not-a-placeholder",
                format!(
                    "'{name}' takes the input `{input}`, which '{name}.{}' never uses: use it in the \
                     text or drop the input",
                    spec.setting
                ),
            );
        }
    }
}

/// config-null-literal: `key: null` in config is almost always a
/// mistake, since null is the default for absent keys. A group's
/// boundary is skipped: its config is the compiler's own (`parentId`)
/// plus a loop header's keys, which `check_loop_config` validates one
/// by one and refuses a null in.
///
/// There is deliberately no "this node's outputs go nowhere" warning.
/// A leaf is how a program ends: the last node sends the message,
/// writes the row, uploads the file, and its outputs are receipts
/// nobody has to read. Every reached node runs, so the warning would
/// fire on the last node of nearly every real program, which teaches
/// people to ignore warnings.
/// Telling a leaf that acts from a leaf that only computes needs
/// something the language does not have yet; see TODO.md, "A node
/// whose outputs nobody reads".
fn check_warnings(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    for node in &project.nodes {
        if node.group_boundary.is_some() {
            continue;
        }
        let name = author_name(node);

        // config-null-literal: `key: null` in config is almost always
        // a mistake. Null is the default for absent keys.
        if let Some(obj) = node.config.as_object() {
            for (key, v) in obj {
                if v.is_null() {
                    let (key_file, key_span) = cfg_anchor(node, key);
                    push(
                        d,
                        &[node.part()],
                        key_file,
                        key_span,
                        Severity::Warning,
                        "config-null-literal",
                        format!(
                            "config '{name}.{key}: null' is redundant; omit the key to let the default apply"
                        ),
                    );
                }
            }
        }
    }
}

/// not-camel-case: a name the program's author wrote that is not
/// camelCase, the spelling weft uses for node names and ports. A
/// warning: the program runs the same, and the message names the
/// spelling that fits (`weft_core::casing`, the same check the catalog
/// test applies to `metadata.json`).
///
/// Only names the author wrote, each once, where it was written:
/// - a node's name, a group's or loop's, an `@include` alias: the local
///   segment of the id (`g.hitBody` reads `hitBody`). A name holding
///   `__` was minted by the compiler (an inline expression's node), an
///   anonymous group takes its file's name, and an included file's body
///   takes its path, so none of those is checked.
/// - a port the program adds to a node, in its header or by writing a
///   key in its body that the node type does not have (`user_id: 3` on
///   `ExecPython`), unless the node type already has it (a catalog port
///   the header only narrows, which the catalog's own check owns) or
///   names its added ports after outside data
///   (`features.addedPortsAreDataKeys`: a path's captures, a query's
///   parameters, the keys of a JSON object it builds, spelled however
///   that data spells them).
/// - a port a group's or loop's header declares. A call site's ports
///   are its file's, checked once on that file's own header; a loop's
///   carry ghost mirrors a port checked where it was declared.
///
/// The gates (`_should_flow`, `_should_not_flow`) are the language's
/// own and never checked. Boundary nodes are skipped: their ports are
/// their group's, checked on the group.
fn check_name_casing(project: &ProjectDefinition, catalog: &dyn MetadataCatalog, d: &mut Vec<Diagnostic>) {
    use weft_core::casing::Casing;
    use weft_core::run::skip::is_gate_port;

    fn local(id: &str) -> &str {
        id.rsplit('.').next().unwrap_or(id)
    }
    // One finding per name on one owner, so a port declared on both
    // sides of a header (`(text) -> (text)`) warns once.
    let warn = |d: &mut Vec<Diagnostic>, about: &str, file: Option<&str>, span: Span, seen: &mut Vec<String>, what: &str, name: &str| {
        if seen.iter().any(|s| s == name) {
            return;
        }
        if let Err(message) = Casing::Camel.check(what, name) {
            seen.push(name.to_string());
            push(d, &[about], file, span, Severity::Warning, "not-camel-case", message);
        }
    };

    for node in &project.nodes {
        if node.group_boundary.is_some() {
            continue;
        }
        let file = node.source_file.as_deref();
        let span = node.header_span_or_default();
        let mut seen = Vec::new();
        let name = local(&node.id);
        if !name.contains("__") {
            let what = if node.include_path.is_some() { "include" } else { "node" };
            warn(d, node.part(), file, span, &mut seen, what, name);
        }
        let meta = catalog.lookup(&node.node_type);
        if meta.is_some_and(|m| m.features.added_ports_are_data_keys) {
            continue;
        }
        // A port the node type's catalog entry declares is the catalog's
        // to spell (its spelling check names the node's own file), so a
        // header that only narrows one, or a program using a node whose
        // own port is off, adds nothing here.
        let ports = node
            .inputs
            .iter()
            .filter(|p| {
                !p.from_spec
                    && !p.port.synthesized_from_carry
                    && !meta.is_some_and(|m| m.inputs.iter().any(|i| i.name == p.port.name))
            })
            .map(|p| &p.port)
            .chain(node.outputs.iter().filter(|p| {
                p.declared_type.is_some()
                    && !meta.is_some_and(|m| m.outputs.iter().any(|o| o.name == p.name))
            }));
        let mut seen_ports = Vec::new();
        for port in ports {
            if !is_gate_port(&port.name) {
                warn(d, node.part(), file, span, &mut seen_ports, "port", &port.name);
            }
        }
    }

    for group in &project.groups {
        let file = group.source_file.as_deref();
        let span = group.header_span.unwrap_or_default();
        let what = group.kind.noun();
        let mut seen = Vec::new();
        if !group.anonymous && !matches!(group.kind, weft_core::project::GroupKind::Body) {
            warn(d, &group.id, file, span, &mut seen, what, local(&group.id));
        }
        if matches!(group.kind, weft_core::project::GroupKind::Call { .. }) {
            continue;
        }
        let mut seen_ports = Vec::new();
        for port in group.in_ports.iter().map(|p| &p.port).chain(&group.out_ports) {
            if !port.synthesized_from_carry && !is_gate_port(&port.name) {
                warn(d, &group.id, file, span, &mut seen_ports, "port", &port.name);
            }
        }
        for port in &group.metadata_ports {
            warn(d, &group.id, file, span, &mut seen_ports, "metadata port", &port.name);
        }
    }
}

/// level-too-large: a level of the graph holding more than
/// [`LEVEL_WARN_AT`] items, nodes or groups. A weft program is read as a
/// graph, and a level is what the reader scans in one look: about six
/// items reads, past fifteen does not. A warning, never an error: the
/// program still runs, and the advice is about how it reads. The message
/// names the move the language has for it (group the nodes cooperating
/// on one job; let nesting absorb size).
///
/// The file's top level is split only into parts that share no wire at
/// all, each measured on its own: two programs that happen to sit in one
/// file answer for their own width. Anything linked, a database or a
/// key set both branches use included, is one part, so a file whose
/// routes share a supply groups its work instead of growing wide. The
/// inside of a group or loop is one job by construction, so it is
/// measured whole, and so is an included file's page.
fn check_level_sizes(project: &ProjectDefinition, d: &mut Vec<Diagnostic>) {
    use std::collections::{BTreeMap, HashMap, HashSet};

    /// A level holds more than this many items and the warning fires.
    /// Six is the readable size the language asks for; fifteen is the
    /// wall the compiler refuses to stay quiet about.
    const LEVEL_WARN_AT: usize = 15;

    // The file's top level: scope `[]` in a program file. A component
    // file's members all live under its root group, whose boundaries
    // are the file's own interface rather than an item, so THAT scope
    // is the component's top level and is measured the same way.
    // Either way every node's scope starts with `top`, save an included
    // body's members, which sit beside a component's root on their own
    // page (`component_root` reads a file with loose nodes beside its
    // anonymous group as a program).
    let root = component_root(project);
    let top: Vec<String> = root.map(|r| vec![r.to_string()]).unwrap_or_default();

    // Per level (a scope path): each item directly in it, keyed by the id
    // an edge names it by. A plain node is its own item; a group, a call
    // site or a loop is one seat at its parent's table (a loop's two
    // boundary halves both map to the loop's id). Sorted into source
    // order below, so a level's first item is the one the author wrote
    // first.
    let mut items: BTreeMap<Vec<String>, Vec<&str>> = BTreeMap::new();
    let mut seated: HashSet<(Vec<String>, &str)> = HashSet::new();
    // Where a diagnostic about an item lands, and where it sorts: a node's
    // header, a group's header line.
    let mut anchor_of: HashMap<&str, (Option<&str>, Option<Span>)> = HashMap::new();
    // Every node's (and group's) item AT THE TOP LEVEL: itself when it sits
    // there, else the ancestor group holding it that does. A wire reaching
    // into a group from outside names the inner node, and the branch walk
    // has to see that as the group joining.
    let mut top_item_of: HashMap<&str, String> = HashMap::new();

    // An included file's body is its own level (the file's page), not an
    // item on the page of the program that includes it: there the item
    // is the call site.
    let groups_by_id: HashMap<&str, &weft_core::project::GroupDefinition> =
        project.groups.iter().map(|g| (g.id.as_str(), g)).collect();
    let chain_of = |group: &str| -> Vec<String> {
        let mut chain = vec![group.to_string()];
        let mut at = groups_by_id.get(group).and_then(|g| g.parent_group_id.as_deref());
        while let Some(id) = at {
            chain.push(id.to_string());
            at = groups_by_id.get(id).and_then(|g| g.parent_group_id.as_deref());
        }
        chain.reverse();
        chain
    };
    for node in &project.nodes {
        let item = match &node.group_boundary {
            Some(b) if Some(b.group_id.as_str()) == root => continue,
            Some(b) => b.group_id.as_str(),
            None => node.id.as_str(),
        };
        let scope = node.scope.clone();
        if seated.insert((scope.clone(), item)) {
            items.entry(scope.clone()).or_default().push(item);
        }
        anchor_of.entry(item).or_insert((node.source_file.as_deref(), node.header_span.or(node.span)));
        top_item_of.insert(node.id.as_str(), top_item(&top, &scope, item).to_string());
    }
    for group in &project.groups {
        let is_loop = matches!(group.kind, weft_core::project::GroupKind::Loop { .. });
        let parent = match &group.parent_group_id {
            Some(parent) => chain_of(parent),
            None => Vec::new(),
        };
        let item = group.id.as_str();
        anchor_of.entry(item).or_insert((group.source_file.as_deref(), group.header_span.or(group.span)));
        // A component's root and an included body sit at no level; a
        // loop's seat came with its boundaries above.
        let unseated = Some(item) == root || matches!(group.kind, weft_core::project::GroupKind::Body);
        if !unseated && !is_loop && seated.insert((parent.clone(), item)) {
            items.entry(parent.clone()).or_default().push(item);
        }
        top_item_of.insert(item, if unseated { item.to_string() } else { top_item(&top, &parent, item).to_string() });
    }

    // Sort each level into the order the author reads it. An item's
    // position is its header line, or for a node lowered from an
    // inline expression (which has no header of its own) the
    // expression's span, which starts after its host's header. Items
    // spliced in from another file (an `@include`d group keeps its own
    // file and coordinates) come after the level's own, then by file;
    // an item with no position at all sorts last. So the first item is
    // never something the author did not write on this level's page.
    for (scope, level_items) in items.iter_mut() {
        let level_file = scope.last().and_then(|gid| anchor_of.get(gid.as_str())).and_then(|(file, _)| *file);
        level_items.sort_by_key(|item| {
            let (file, span) = anchor_of[item];
            (
                span.is_none(),
                file != level_file,
                file,
                span.map_or((0, 0), |s| (s.start_line, s.start_column)),
            )
        });
    }

    for (scope, level_items) in &items {
        // Each entry is one measured unit: its items in source order.
        // Every sub level is one unit; the top level splits by branch.
        let is_top = *scope == top.as_slice();
        let units: Vec<Vec<&str>> = if is_top {
            parts(level_items, &top_item_of, project)
        } else {
            vec![level_items.clone()]
        };
        for unit in units {
            let count = unit.len();
            if count <= LEVEL_WARN_AT {
                continue;
            }
            // About the group whose inside it is, or for the top level every
            // item of the crowded part.
            let (name, anchor, about) = if is_top {
                // The top level has no header of its own; the part's
                // first item carries the warning.
                let first = unit.first().expect("a crowded branch has a first item");
                ("one connected part of the top level".to_string(), anchor_of[first], unit.clone())
            } else {
                // A sub level is a group's inside, and the group is an
                // item of its parent level: its header carries the warning.
                // An included file's body has no header in any file (its
                // site's header is in the includer's), so its first item
                // carries it, on the file's own page.
                let gid = scope.last().expect("a sub level is a group's scope");
                let body = groups_by_id.get(gid.as_str()).is_some_and(|g| matches!(g.kind, weft_core::project::GroupKind::Body));
                match anchor_of.get(gid.as_str()).filter(|_| !body) {
                    Some(group) => (format!("the inside of '{}'", weft_core::project::plain_id(gid)), *group, vec![gid.as_str()]),
                    None => {
                        let first = unit.first().expect("a crowded level has a first item");
                        ("this included file".to_string(), anchor_of[first], vec![gid.as_str()])
                    }
                }
            };
            push(
                d,
                &about,
                anchor.0,
                anchor.1.unwrap_or_default(),
                Severity::Warning,
                "level-too-large",
                format!(
                    "{name} holds {count} items; a level reads at about six and stops reading \
                     past fifteen: group the nodes cooperating on one job, and nest groups \
                     rather than widen the level"
                ),
            );
        }
    }

    /// The item at the top level for an item seated at `scope`: itself when
    /// `scope` is the top, else the outermost group under the top holding
    /// it (an included body's member, beside a component's root rather
    /// than in it, has the body as its item: its own page).
    fn top_item<'a>(top: &[String], scope: &'a [String], item: &'a str) -> &'a str {
        if scope == top {
            return item;
        }
        match scope.strip_prefix(top) {
            Some([first, ..]) => first.as_str(),
            _ => scope.first().map_or(item, String::as_str),
        }
    }

    /// The top level's items split into the parts no wire links, each
    /// listed in source order. `top_item_of` maps every node and group id
    /// a wire may name to its item at this level, so a wire into a
    /// group's inside joins the group.
    fn parts<'a>(
        level_items: &[&'a str],
        top_item_of: &HashMap<&'a str, String>,
        project: &'a ProjectDefinition,
    ) -> Vec<Vec<&'a str>> {
        // Union-find by position in `level_items`, so parts come out in
        // source order.
        let index: HashMap<&str, usize> =
            level_items.iter().enumerate().map(|(i, id)| (*id, i)).collect();
        let mut parent: Vec<usize> = (0..level_items.len()).collect();
        fn find(parent: &mut [usize], i: usize) -> usize {
            let mut i = i;
            while parent[i] != i {
                parent[i] = parent[parent[i]];
                i = parent[i];
            }
            i
        }
        for edge in &project.edges {
            // The wire links its two ends and every group whose port it
            // passes: `a -> alias -> b` runs `a` to the body's member and
            // that member to `b`, and only the call site `alias` sits on
            // this level between them.
            let passed = std::iter::once(edge.source.as_str())
                .chain(edge.via.iter().map(|crossing| crossing.group.as_str()))
                .chain(std::iter::once(edge.target.as_str()));
            let mut linked: Option<usize> = None;
            for id in passed {
                let Some(&i) = top_item_of.get(id).and_then(|item| index.get(item.as_str())) else { continue };
                if let Some(prev) = linked {
                    let (ra, rb) = (find(&mut parent, prev), find(&mut parent, i));
                    parent[ra.max(rb)] = ra.min(rb);
                }
                linked = Some(i);
            }
        }
        let mut members: BTreeMap<usize, Vec<&str>> = BTreeMap::new();
        for (i, id) in level_items.iter().enumerate() {
            members.entry(find(&mut parent, i)).or_default().push(id);
        }
        members.into_values().collect()
    }
}

#[cfg(test)]
mod validation_mode_wire_tests {
    use super::ValidationMode;

    /// Wire shape: the parse-server request carries the mode as a bare
    /// lowercase string. SYNC partner: extension-vscode/src/parseServer.ts
    /// ValidationMode.
    #[test]
    fn validation_mode_round_trips_as_lowercase_strings() {
        for (mode, wire) in [
            (ValidationMode::Structural, "\"structural\""),
            (ValidationMode::Runtime, "\"runtime\""),
        ] {
            assert_eq!(serde_json::to_string(&mode).unwrap(), wire);
            assert_eq!(serde_json::from_str::<ValidationMode>(wire).unwrap(), mode);
        }
    }
}

