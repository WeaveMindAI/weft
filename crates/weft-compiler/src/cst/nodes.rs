//! The typed view over the untyped rowan tree.
//!
//! rowan nodes are all `SyntaxNode` with a `u16` kind tag. This layer is a set
//! of thin `#[repr(transparent)]` wrappers (`WeftFile`, `Decl`, `Body`, ...)
//! with `cast()` + typed accessors, so edit code reads structurally instead of
//! matching kinds by hand.
//!
//! The view's job is NARROW and deliberate: it lets the edit ops RESOLVE a
//! target by scoped id (`grp.child`) and locate the subtree to mutate. It does
//! NOT reproduce the flatten/`ProjectDefinition` projection: the parse-server
//! re-parses the edited source through the existing `compile`/`flatten` path to
//! render the graph (see `edit_envelope` in `weft-cli`). One flatten, not two.
//!
//! Scoped-id rule (mirrors `flatten`): a decl's scoped id is the dot-joined
//! chain of enclosing GROUP_DECL labels plus the decl's own local id. (The
//! `{group}__in`/`__out` boundary passthroughs are a flatten-side concern; this
//! module works in the as-written `grp.port` form, never the boundary ids.)

use super::kind::{SyntaxKind, SyntaxNode, SyntaxToken};

/// Macro: define a `#[repr(transparent)]` typed wrapper over `SyntaxNode` for a
/// single `SyntaxKind`, with `cast`/`syntax` and a by-kind child finder.
macro_rules! typed_node {
    ($name:ident, $kind:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[repr(transparent)]
        pub struct $name(pub SyntaxNode);

        impl $name {
            pub const KIND: SyntaxKind = SyntaxKind::$kind;

            /// Wrap `node` if its kind matches, else None.
            pub fn cast(node: SyntaxNode) -> Option<Self> {
                if node.kind() == SyntaxKind::$kind {
                    Some(Self(node))
                } else {
                    None
                }
            }

            /// The underlying untyped node.
            pub fn syntax(&self) -> &SyntaxNode {
                &self.0
            }
        }
    };
}

typed_node!(WeftFile, WEFT_FILE);
typed_node!(NodeDecl, NODE_DECL);
typed_node!(GroupDecl, GROUP_DECL);
typed_node!(LoopDecl, LOOP_DECL);
typed_node!(IncludeDecl, INCLUDE_DECL);
typed_node!(Header, HEADER);
typed_node!(Body, BODY);
typed_node!(ConfigField, CONFIG_FIELD);
typed_node!(LabelField, LABEL_FIELD);
typed_node!(Connection, CONNECTION);
typed_node!(Endpoint, ENDPOINT);
typed_node!(GroupDesc, GROUP_DESC);

/// First child node of `parent` with kind `k`, as a raw `SyntaxNode`.
fn child(parent: &SyntaxNode, k: SyntaxKind) -> Option<SyntaxNode> {
    parent.children().find(|n| n.kind() == k)
}

/// The first IDENT token among `parent`'s direct token children.
fn first_ident(parent: &SyntaxNode) -> Option<SyntaxToken> {
    parent
        .children_with_tokens()
        .filter_map(|e| e.into_token())
        .find(|t| t.kind() == SyntaxKind::IDENT)
}

impl WeftFile {
    /// The root WEFT_FILE for `node`'s tree.
    pub fn of(node: &SyntaxNode) -> Option<WeftFile> {
        WeftFile::cast(node.ancestors().last().unwrap_or_else(|| node.clone()))
    }
}

/// Any declaration kind (node, group, include). The shared notion the resolver
/// walks: each carries a local id (the leading IDENT) and may nest a body.
#[derive(Debug, Clone)]
pub enum Decl {
    Node(NodeDecl),
    Group(GroupDecl),
    Loop(LoopDecl),
    Include(IncludeDecl),
}

impl Decl {
    pub fn cast(node: SyntaxNode) -> Option<Decl> {
        match node.kind() {
            SyntaxKind::NODE_DECL => NodeDecl::cast(node).map(Decl::Node),
            SyntaxKind::GROUP_DECL => GroupDecl::cast(node).map(Decl::Group),
            SyntaxKind::LOOP_DECL => LoopDecl::cast(node).map(Decl::Loop),
            SyntaxKind::INCLUDE_DECL => IncludeDecl::cast(node).map(Decl::Include),
            _ => None,
        }
    }

    pub fn syntax(&self) -> &SyntaxNode {
        match self {
            Decl::Node(n) => n.syntax(),
            Decl::Group(g) => g.syntax(),
            Decl::Loop(l) => l.syntax(),
            Decl::Include(i) => i.syntax(),
        }
    }

    pub fn local_id(&self) -> Option<String> {
        match self {
            Decl::Node(n) => n.local_id(),
            Decl::Group(g) => g.local_id(),
            Decl::Loop(l) => l.local_id(),
            Decl::Include(i) => first_ident(i.syntax()).map(|t| t.text().to_string()),
        }
    }

    pub fn body(&self) -> Option<Body> {
        match self {
            Decl::Node(n) => child(n.syntax(), SyntaxKind::BODY).and_then(Body::cast),
            Decl::Group(g) => child(g.syntax(), SyntaxKind::BODY).and_then(Body::cast),
            Decl::Loop(l) => child(l.syntax(), SyntaxKind::BODY).and_then(Body::cast),
            Decl::Include(_) => None,
        }
    }
}

impl LoopDecl {
    pub fn header(&self) -> Option<Header> {
        child(&self.0, SyntaxKind::HEADER).and_then(Header::cast)
    }
    pub fn local_id(&self) -> Option<String> {
        self.header().and_then(|h| first_ident(h.syntax()).map(|t| t.text().to_string()))
    }
    pub fn body(&self) -> Option<Body> {
        child(&self.0, SyntaxKind::BODY).and_then(Body::cast)
    }
    /// The loop's description: the plain `# ...` comment that is the first
    /// body line, if present (same rule as groups).
    pub fn description(&self) -> Option<GroupDesc> {
        self.body()
            .and_then(|b| child(b.syntax(), SyntaxKind::GROUP_DESC))
            .and_then(GroupDesc::cast)
    }
}

impl NodeDecl {
    pub fn header(&self) -> Option<Header> {
        child(&self.0, SyntaxKind::HEADER).and_then(Header::cast)
    }
    pub fn local_id(&self) -> Option<String> {
        self.header().and_then(|h| first_ident(h.syntax()).map(|t| t.text().to_string()))
    }
    pub fn body(&self) -> Option<Body> {
        child(&self.0, SyntaxKind::BODY).and_then(Body::cast)
    }
}

impl GroupDecl {
    pub fn header(&self) -> Option<Header> {
        child(&self.0, SyntaxKind::HEADER).and_then(Header::cast)
    }
    pub fn local_id(&self) -> Option<String> {
        self.header().and_then(|h| first_ident(h.syntax()).map(|t| t.text().to_string()))
    }
    pub fn body(&self) -> Option<Body> {
        child(&self.0, SyntaxKind::BODY).and_then(Body::cast)
    }
    /// The group's description: the plain `# ...` comment that is the first
    /// body line, if present.
    pub fn description(&self) -> Option<GroupDesc> {
        self.body()
            .and_then(|b| child(b.syntax(), SyntaxKind::GROUP_DESC))
            .and_then(GroupDesc::cast)
    }
}

impl GroupDesc {
    /// The description text: the comment body without the leading `# `.
    pub fn text(&self) -> String {
        self.0
            .children_with_tokens()
            .filter_map(|e| e.into_token())
            .find(|t| t.kind() == SyntaxKind::COMMENT)
            .map(|t| {
                t.text()
                    .trim_start()
                    .strip_prefix('#')
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }
}

impl Body {
    /// The direct child declarations of this body (nodes + groups + includes),
    /// in source order.
    pub fn decls(&self) -> impl Iterator<Item = Decl> + '_ {
        self.0.children().filter_map(|n| Decl::cast(n))
    }
    /// The closing `}` token, the splice anchor for inserting a child.
    pub fn close_brace(&self) -> Option<SyntaxToken> {
        self.0
            .children_with_tokens()
            .filter_map(|e| e.into_token())
            .find(|t| t.kind() == SyntaxKind::R_BRACE)
    }
}

impl Endpoint {
    /// The IDENT segments of the endpoint (`a.b` -> `["a","b"]`).
    pub fn segments(&self) -> Vec<String> {
        self.0
            .children_with_tokens()
            .filter_map(|e| e.into_token())
            .filter(|t| t.kind() == SyntaxKind::IDENT)
            .map(|t| t.text().to_string())
            .collect()
    }

    /// `(id, Some(port))` for `id.port`, `(id, None)` for a bare `id`. A
    /// well-formed endpoint has exactly 1 or 2 segments; `segments()` exposes the
    /// raw count so callers can reject a malformed 3+-segment ref (`a.b.c`)
    /// loudly instead of this silently keeping only the first two.
    pub fn parts(&self) -> (Option<String>, Option<String>) {
        let idents = self.segments();
        match idents.len() {
            0 => (None, None),
            1 => (Some(idents[0].clone()), None),
            _ => (Some(idents[0].clone()), Some(idents[1].clone())),
        }
    }
}

/// True iff `conn` (a CONNECTION node) is a config-ORIGIN field
/// (`target.port = <literal>`), as opposed to a wiring construct:
///   - NOT an inline-expr RHS (`= Type{...}.out` synthesizes a node + edge);
///   - exactly ONE endpoint (a second `src.port` endpoint makes it an edge);
///   - the target endpoint is a real node (not `self`) with a non-empty port.
/// When `target_local`/`port` are `Some`, they must match the target endpoint.
///
/// The SINGLE definition of "is this connection a literal config fill", in the
/// typed view so BOTH the lowering (`literal_config_fill`) and the edit ops
/// (`connection_is_config_origin`/`connections_origin_targeting`) call it and
/// cannot drift. Drift here is load-bearing: if the editor misclassifies an
/// inline-expr or a two-endpoint edge as a config field, SetConfig/RemoveConfig
/// silently clobbers the user's wiring.
pub fn connection_is_config_origin(conn: &SyntaxNode, target_local: Option<&str>, port: Option<&str>) -> bool {
    if conn.kind() != SyntaxKind::CONNECTION {
        return false;
    }
    // An inline-expr RHS is a node+edge synthesis, never a literal config field.
    if conn.children().any(|c| c.kind() == SyntaxKind::INLINE_EXPR) {
        return false;
    }
    let endpoints: Vec<SyntaxNode> = conn.children().filter(|c| c.kind() == SyntaxKind::ENDPOINT).collect();
    // A second endpoint means a `src.port` RHS: an edge, not a literal.
    if endpoints.len() != 1 {
        return false;
    }
    let (t_id, t_port) = Endpoint::cast(endpoints[0].clone()).map(|e| e.parts()).unwrap_or((None, None));
    // Target must be a real node (not `self`) with a non-empty port.
    match (&t_id, &t_port) {
        (Some(id), Some(_)) if id != "self" => {
            target_local.map_or(true, |l| id == l) && port.map_or(true, |k| t_port.as_deref() == Some(k))
        }
        _ => false,
    }
}

// ── scoped-id resolution ───────────────────────────────────────────────────

/// The outcome of resolving a scoped id.
pub enum Resolution {
    Found(Decl),
    NotFound,
    Ambiguous,
}

/// A file paired with its source identity: the id an ANONYMOUS top-level group
/// (`Group(){...}` with no `name =`) takes, derived from the filename (e.g.
/// `MyCleaner` from `my-cleaner.weft`, `Untitled` for an unsaved buffer). Every
/// scoped-id query lives here because the anon-group prefix is part of the id
/// scheme: a decl inside an anonymous root is `MyCleaner.child`, NOT `.child`.
/// The SAME id the lowering writes (`source_id` flows to `lower_group`), so the
/// typed view and `flatten` agree on every scoped id with no rename pass.
pub struct FileView<'a> {
    file: &'a WeftFile,
    source_id: &'a str,
}

impl<'a> FileView<'a> {
    pub fn new(file: &'a WeftFile, source_id: &'a str) -> Self {
        Self { file, source_id }
    }

    /// The underlying typed file root, for direct tree mutation by the edit ops.
    pub fn file(&self) -> &'a WeftFile {
        self.file
    }

    /// Resolve a scoped id (`grp.child`, or a bare local) to its declaration,
    /// walking the GROUP_DECL nesting from the file root. Returns the decl whose
    /// scoped path matches. Mirrors `flatten`'s id scheme: scoped id = dot-joined
    /// enclosing group labels + local id.
    ///
    /// An exact scoped match wins. A unique bare-local match is also accepted (the
    /// frontend sometimes sends a bare local when unambiguous), but an ambiguous
    /// bare local returns `Ambiguous` so the caller fails loud rather than guessing
    /// which node was meant.
    pub fn resolve_decl(&self, wanted: &str) -> Resolution {
        let mut exact: Option<Decl> = None;
        let mut local_matches: Vec<Decl> = Vec::new();
        let local_wanted = wanted.rsplit('.').next().unwrap_or(wanted);

        self.walk_decls(&mut |scoped, decl| {
            if scoped == wanted {
                exact = Some(decl.clone());
            }
            if decl.local_id().as_deref() == Some(local_wanted) {
                local_matches.push(decl.clone());
            }
        });

        if let Some(d) = exact {
            return Resolution::Found(d);
        }
        match local_matches.len() {
            0 => Resolution::NotFound,
            1 => Resolution::Found(local_matches.into_iter().next().unwrap()),
            _ => Resolution::Ambiguous,
        }
    }

    /// True iff a decl with EXACTLY this scoped id exists (no bare-local fallback).
    /// For membership checks (can this id be added to this scope?), unlike
    /// `resolve_decl` which is for resolving a user-written ref and tolerates a
    /// unique bare local. Using `resolve_decl` for membership wrongly rejects an id
    /// that is free in the target scope but unique elsewhere in the tree.
    pub fn scoped_id_exists(&self, scoped: &str) -> bool {
        let mut found = false;
        self.walk_decls(&mut |id, _| {
            if id == scoped {
                found = true;
            }
        });
        found
    }

    /// The full scoped id of `decl` (dot-joined enclosing group labels + local
    /// id), or None if it isn't found in the tree.
    pub fn scoped_id_of(&self, decl: &Decl) -> Option<String> {
        let want = decl.syntax().clone();
        let mut found = None;
        self.walk_decls(&mut |scoped, d| {
            if *d.syntax() == want {
                found = Some(scoped.to_string());
            }
        });
        found
    }

    /// Every CONNECTION in the tree that references `target` (by either endpoint),
    /// resolved SCOPE-AWARE: an endpoint id is resolved against its connection's
    /// enclosing group scope the same way `flatten` rescopes, so removing `grp.a`
    /// matches edges that resolve to `grp.a` and NOT a same-named top-level `a`.
    ///
    /// This is the authoritative "what edges touch this node" query, used by
    /// RemoveNode to drop dangling edges in ANY scope (not just the node's own).
    pub fn connections_referencing(&self, target: &Decl) -> Vec<SyntaxNode> {
        let Some(target_scoped) = self.scoped_id_of(target) else { return Vec::new() };
        // The set of every decl's scoped id, so endpoint resolution can tell a
        // local child ref from an outer ref (a ref resolves to `{scope}.x` only if
        // that scoped id actually exists; otherwise it's an outer/top-level ref).
        let mut all_ids = std::collections::HashSet::new();
        self.walk_decls(&mut |scoped, _| {
            all_ids.insert(scoped.to_string());
        });
        let mut out = Vec::new();
        self.walk_connections(&mut |scope, conn| {
            let refs = conn
                .children()
                .filter(|n| n.kind() == SyntaxKind::ENDPOINT)
                .filter_map(|ep| Endpoint::cast(ep))
                .any(|ep| endpoint_resolves_to(&ep, scope, &all_ids).as_deref() == Some(target_scoped.as_str()));
            if refs {
                out.push(conn.clone());
            }
        });
        out
    }

    /// The local id of a decl for scoped-id composition: its own local id, or the
    /// file's `source_id` for an anonymous group (empty local). This is the one
    /// place the anon-group prefix enters; it must match the id the lowering's
    /// `lower_group` writes for the same group.
    fn decl_local(&self, decl: &Decl) -> String {
        match decl.local_id() {
            Some(id) if !id.is_empty() => id,
            _ => self.source_id.to_string(),
        }
    }

    /// Walk every declaration, invoking `f(scoped_id, decl)`.
    fn walk_decls(&self, f: &mut impl FnMut(&str, &Decl)) {
        self.walk_scoped(self.file.syntax(), &mut Vec::new(), &mut |scope, node| {
            if let Some(decl) = Decl::cast(node.clone()) {
                f(&scoped_with(scope, &self.decl_local(&decl)), &decl);
            }
        });
    }

    /// Walk every CONNECTION in the tree depth-first, invoking `f(scope, conn)`
    /// with the enclosing group-label chain. Connections live directly under
    /// WEFT_FILE or a group BODY.
    fn walk_connections(&self, f: &mut impl FnMut(&[String], &SyntaxNode)) {
        self.walk_scoped(self.file.syntax(), &mut Vec::new(), &mut |scope, node| {
            if node.kind() == SyntaxKind::CONNECTION {
                f(scope, node);
            }
        });
    }

    /// The single tree walk: invoke `f(scope, node)` for every CONNECTION and decl
    /// in the tree, with `scope` = the enclosing group-label chain (dot-joinable).
    /// `walk_decls`/`walk_connections` are thin filters over this, so the
    /// scope-prefix rule (including the anon-group `source_id`) lives in ONE place.
    fn walk_scoped(&self, parent: &SyntaxNode, prefix: &mut Vec<String>, f: &mut impl FnMut(&[String], &SyntaxNode)) {
        for node in parent.children() {
            match node.kind() {
                SyntaxKind::NODE_DECL | SyntaxKind::GROUP_DECL | SyntaxKind::LOOP_DECL | SyntaxKind::INCLUDE_DECL => {
                    f(prefix, &node);
                    if let Some(decl) = Decl::cast(node.clone()) {
                        if let Some(body) = decl.body() {
                            if matches!(&decl, Decl::Group(_) | Decl::Loop(_)) {
                                prefix.push(self.decl_local(&decl));
                                self.walk_scoped(body.syntax(), prefix, f);
                                prefix.pop();
                            }
                        }
                    }
                }
                SyntaxKind::CONNECTION => f(prefix, &node),
                _ => {}
            }
        }
    }
}

/// The scoped id of a decl-or-connection node given its enclosing `scope`.
fn scoped_with(prefix: &[String], local: &str) -> String {
    if prefix.is_empty() { local.to_string() } else { format!("{}.{}", prefix.join("."), local) }
}

/// Resolve an endpoint to the NODE id it names, for edge-matching, using Weft's
/// SAME-SCOPE-ONLY rule (no up/down walk): an id that is a LOCAL child of the
/// immediate scope -> `{scope}.id`; otherwise the bare id (a top-level ref, or a
/// cross-scope leak the validator rejects). `self` -> None (a boundary is not a
/// node, so an edge to it references no node).
///
/// This shares ONE load-bearing rule with the lowering's `rescope_endpoint`
/// (the id-rewriter): "an id is local iff it's a direct child of the immediate
/// scope" (`all_ids.contains("{scope}.id")` here == `local_children.contains(id)`
/// there). The two functions are NOT mergeable: this one RESOLVES for matching
/// (self -> None), the other REWRITES for flatten (self -> the `__in`/`__out`
/// boundary). The shared rule is pinned by `endpoint_resolution_matches_flatten`
/// so the two can't silently diverge.
/// SYNC: endpoint_resolves_to <-> crates/weft-compiler/src/edit/ops.rs require_endpoint, crates/weft-compiler/src/weft_compiler.rs rescope_endpoint
fn endpoint_resolves_to(ep: &Endpoint, scope: &[String], all_ids: &std::collections::HashSet<String>) -> Option<String> {
    let (id, _) = ep.parts();
    let id = id?;
    if id == "self" {
        return None;
    }
    let local = if scope.is_empty() { id.clone() } else { format!("{}.{}", scope.join("."), id) };
    if all_ids.contains(&local) {
        Some(local)
    } else {
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cst::parse;
    use crate::cst::kind::SyntaxElement;

    fn file(src: &str) -> WeftFile {
        WeftFile::cast(parse(src)).expect("root is WEFT_FILE")
    }

    /// A `FileView` over `f` with a placeholder source id. These tests use NAMED
    /// top-level groups, so the anon-group `source_id` is never exercised; the
    /// anon-prefix behaviour is covered by `parser_tests` against `flatten`.
    fn view(f: &WeftFile) -> FileView<'_> {
        FileView::new(f, "Untitled")
    }

    /// Pin the load-bearing equivalence between the edit-side resolver
    /// (`endpoint_resolves_to`/`connections_referencing`) and the lowering's
    /// `rescope_endpoint`: an edge the edit layer says references a node must be
    /// an edge the runtime graph (flatten) actually attaches to that node. Uses a
    /// shadowing case (a child named like an outer node) where a wrong "is-local"
    /// rule on either side would disagree. If the two resolvers ever drift, this
    /// fails, rather than a production mis-edit.
    #[test]
    fn endpoint_resolution_matches_flatten() {
        // Top-level `a` AND `g.a`; an intra-`g` edge `b.in = a.out` references the
        // INNER `a` (same-scope rule), so removing `g.a` must match this edge and
        // removing top-level `a` must NOT.
        let src = "a = Text {}\ng = Group() -> () {\n  a = Text {}\n  b = Debug\n  b.data = a.value\n}\n";
        let f = file(src);

        let v = view(&f);
        let inner_a = match v.resolve_decl("g.a") {
            Resolution::Found(d) => d,
            r => panic!("g.a should resolve uniquely, got {:?}", matches!(r, Resolution::Found(_))),
        };
        let top_a = match v.resolve_decl("a") {
            Resolution::Found(d) => d,
            _ => panic!("top-level a should resolve"),
        };

        // The edit layer attributes `b.data = a.value` to the INNER a, not top a.
        assert_eq!(v.connections_referencing(&inner_a).len(), 1, "inner a owns the edge");
        assert_eq!(v.connections_referencing(&top_a).len(), 0, "top-level a owns no edge");

        // Cross-check against the runtime graph: flatten must wire that edge's
        // source to `g.a` (the inner one), confirming both resolvers agree.
        let (proj, _) = crate::weft_compiler::compile_lenient(
            src,
            uuid::Uuid::nil(),
            crate::file_reader::CompileFs::none(),
            crate::weft_compiler::IncludeMode::Interface,
            None,
        );
        let wired = proj.edges.iter().any(|e| e.source == "g.a" && e.target == "g.b");
        assert!(wired, "flatten wires the edge to g.a, matching the edit layer: {:?}", proj.edges);
    }

    #[test]
    fn resolve_top_level_node() {
        let f = file("a = Text {}\nb = Debug\n");
        let v = view(&f);
        assert!(matches!(v.resolve_decl("a"), Resolution::Found(_)));
        assert!(matches!(v.resolve_decl("b"), Resolution::Found(_)));
        assert!(matches!(v.resolve_decl("nope"), Resolution::NotFound));
    }

    #[test]
    fn resolve_scoped_node_in_group() {
        let f = file("g = Group() {\n  child = Text {}\n}\n");
        let v = view(&f);
        assert!(matches!(v.resolve_decl("g.child"), Resolution::Found(_)));
        assert!(matches!(v.resolve_decl("g"), Resolution::Found(_)));
    }

    #[test]
    fn ambiguous_bare_local_is_loud() {
        // two `t` in different groups: a bare `t` must be Ambiguous, not a guess.
        let f = file("g1 = Group() {\n  t = Text {}\n}\ng2 = Group() {\n  t = Text {}\n}\n");
        let v = view(&f);
        assert!(matches!(v.resolve_decl("t"), Resolution::Ambiguous));
        // but the scoped forms resolve fine
        assert!(matches!(v.resolve_decl("g1.t"), Resolution::Found(_)));
        assert!(matches!(v.resolve_decl("g2.t"), Resolution::Found(_)));
    }

    #[test]
    fn group_description_accessor() {
        let f = file("g = Group() {\n  # does things\n  x = Text {}\n}\n");
        let g = match view(&f).resolve_decl("g") {
            Resolution::Found(Decl::Group(g)) => g,
            _ => panic!("g should resolve to a group"),
        };
        let desc = g.description().expect("first-body-line comment is the group desc");
        assert_eq!(desc.text(), "does things");
    }

    #[test]
    fn group_description_is_first_body_line_only() {
        // A comment after the first body item is a plain comment, not a desc.
        let f = file("g = Group() {\n  x = Text {}\n  # not a description\n}\n");
        let g = match view(&f).resolve_decl("g") {
            Resolution::Found(Decl::Group(g)) => g,
            _ => panic!("g should resolve to a group"),
        };
        assert!(g.description().is_none());
    }

    #[test]
    fn group_description_is_single_line() {
        // Only the FIRST comment line is the description; the second stays plain.
        let f = file("g = Group() {\n  # short desc\n  # more prose\n  x = Text {}\n}\n");
        let g = match view(&f).resolve_decl("g") {
            Resolution::Found(Decl::Group(g)) => g,
            _ => panic!("g should resolve to a group"),
        };
        assert_eq!(g.description().unwrap().text(), "short desc");
        // exactly one GROUP_DESC child in the body
        let body = g.body().unwrap();
        let count = body
            .syntax()
            .children()
            .filter(|n| n.kind() == SyntaxKind::GROUP_DESC)
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn spike_mutation_primitives_work() {
        // Prove the rowan mutation path end-to-end before building ops on it:
        // clone_for_update -> splice a freshly-built subtree into a body before
        // its `}` -> serialize round-trips with the insertion.
        use crate::cst::parse;
        let root = parse("g = Group() {\n  a = Text {}\n}\n").clone_for_update();
        let file = WeftFile::cast(root).unwrap();
        let g = match view(&file).resolve_decl("g") {
            Resolution::Found(Decl::Group(g)) => g,
            _ => panic!(),
        };
        let body = g.body().unwrap();
        let brace = body.close_brace().unwrap();
        // Build the element run to insert: "  b = Debug {}\n" parsed, its tokens
        // lifted as mutable elements.
        let snippet = parse("  b = Debug {}\n").clone_for_update();
        let inserted: Vec<SyntaxElement> = snippet.children_with_tokens().collect();
        // splice before the close brace: the brace's index within the BODY.
        let at = brace.index();
        body.syntax().splice_children(at..at, inserted);
        let out = file.syntax().to_string();
        assert!(out.contains("b = Debug {}"), "spliced child present: {out}");
        assert!(out.contains("a = Text {}"), "original child kept: {out}");
        // structurally re-parses and the new child resolves
        let reparsed = WeftFile::cast(parse(&out)).unwrap();
        assert!(matches!(view(&reparsed).resolve_decl("g.b"), Resolution::Found(_)), "{out}");
    }

    #[test]
    fn body_close_brace_is_a_real_token() {
        // The whole point: the closing `}` is a token with a real position, not
        // a text-scanned guess.
        let f = file("g = Group() {\n  x = Text {}\n} -> (out: String)\n");
        let g = match view(&f).resolve_decl("g") {
            Resolution::Found(Decl::Group(g)) => g,
            _ => panic!(),
        };
        let body = g.body().expect("group has a body");
        assert!(body.close_brace().is_some(), "R_BRACE is a real token");
    }
}
