//! Route patterns: how a public-entry path with `{name}` segments is
//! parsed, matched against a called path, and checked for overlap with
//! another pattern. Pure; the dispatcher matches an incoming call with
//! these, the register path refuses two routes that would both claim
//! one call, and the trigger's validation refuses a malformed pattern.
//!
//! A pattern is segments split on `/`, no leading slash, no empty
//! segment. A `{name}` segment captures whatever the caller put there
//! (one segment, never a slash); every other segment must match
//! literally.
//!
//! Two patterns a call could both reach are allowed when one of them is
//! the more specific, which is [`compare_patterns`]: `users/me` and
//! `users/{id}` coexist, and `users/me` serves that exact call while
//! `users/{id}` serves the rest. What the register path refuses is the
//! pair where NEITHER is more specific (`a/{x}/c` against `a/b/{y}`,
//! or two patterns of the same shape), because then a shared call has
//! no answer to give.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One segment of a parsed pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Segment {
    /// Must equal the caller's segment.
    Literal(String),
    /// Captures the caller's segment under this name.
    Param(String),
}

/// A parsed route pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutePattern {
    segments: Vec<Segment>,
}

/// The HTTP methods a route may name. Anything else is refused at
/// validation so a typo (`GTE`) is not a route nobody can call.
// SYNC: HTTP_METHODS <-> deploy/k8s/gateway.yaml (SecurityPolicy weft-live-cors allowMethods)
pub const HTTP_METHODS: &[&str] =
    &["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"];

/// Uppercase and check one method name.
pub fn normalize_method(raw: &str) -> Result<String, String> {
    let upper = raw.trim().to_ascii_uppercase();
    if HTTP_METHODS.contains(&upper.as_str()) {
        Ok(upper)
    } else {
        Err(format!(
            "'{raw}' is not an HTTP method (one of {})",
            HTTP_METHODS.join(", ")
        ))
    }
}

impl RoutePattern {
    /// Parse `chat/{room}` into segments. The empty string is the root
    /// pattern (zero segments), which matches only the empty path.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.starts_with('/') {
            return Err(format!("route '{raw}' must not start with '/'"));
        }
        if raw.is_empty() {
            return Ok(Self { segments: Vec::new() });
        }
        let mut segments = Vec::new();
        let mut names: Vec<&str> = Vec::new();
        for piece in raw.split('/') {
            if piece.is_empty() {
                return Err(format!("route '{raw}' has an empty segment"));
            }
            if let Some(inner) = piece.strip_prefix('{') {
                let Some(name) = inner.strip_suffix('}') else {
                    return Err(format!("route '{raw}': segment '{piece}' opens a parameter but never closes it"));
                };
                if name.is_empty()
                    || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    return Err(format!(
                        "route '{raw}': parameter name '{name}' must be [A-Za-z0-9_]+"
                    ));
                }
                if names.contains(&name) {
                    return Err(format!("route '{raw}' names parameter '{name}' twice"));
                }
                names.push(name);
                segments.push(Segment::Param(name.to_string()));
            } else if piece.contains('{') || piece.contains('}') {
                return Err(format!(
                    "route '{raw}': segment '{piece}' mixes a parameter brace with literal text; \
                     a parameter is a whole segment (`{{name}}`)"
                ));
            } else {
                segments.push(Segment::Literal(piece.to_string()));
            }
        }
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// How many segments capture. Counting is enough to RANK two
    /// patterns that both match one call, because register time has
    /// already refused every pair [`compare_patterns`] calls
    /// ambiguous: of the pairs that survive, the more specific one
    /// holds a literal wherever the other captures, so it always has
    /// the smaller count. Counting is NOT enough to DECIDE which is
    /// more specific (`a/{x}/c` and `a/b/{y}` both count one), which
    /// is why the refusal reads positions and this does not.
    pub fn param_count(&self) -> usize {
        self.segments.iter().filter(|s| matches!(s, Segment::Param(_))).count()
    }

    /// The pattern as written (`chat/{room}`).
    pub fn as_str(&self) -> String {
        self.segments
            .iter()
            .map(|s| match s {
                Segment::Literal(l) => l.clone(),
                Segment::Param(p) => format!("{{{p}}}"),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Match a called path (no leading slash, no tenant prefix). `Some`
    /// carries the captured parameters; `None` when a literal differs
    /// or the segment counts do not agree.
    pub fn match_path(&self, path: &str) -> Option<BTreeMap<String, String>> {
        let called: Vec<&str> = if path.is_empty() { Vec::new() } else { path.split('/').collect() };
        if called.len() != self.segments.len() {
            return None;
        }
        let mut params = BTreeMap::new();
        for (segment, got) in self.segments.iter().zip(called) {
            match segment {
                Segment::Literal(want) if want == got => {}
                Segment::Literal(_) => return None,
                Segment::Param(name) => {
                    params.insert(name.clone(), got.to_string());
                }
            }
        }
        Some(params)
    }
}

/// How two patterns relate. One question, asked by the matcher and by
/// both places that refuse a pair, answered once here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternOrder {
    /// No call can match both.
    Disjoint,
    /// Both match some call, and the first is the more specific of the
    /// two: wherever they differ it holds a literal where the other
    /// captures, never the reverse. So every call they share has one
    /// obvious answer and both may be registered.
    FirstWins,
    /// The mirror of [`PatternOrder::FirstWins`].
    SecondWins,
    /// Both match some call and neither is more specific, so a shared
    /// call has no honest answer. This is the pair that gets refused.
    Ambiguous,
}

/// Compare two patterns. A literal beats a capture at any one position,
/// and a pattern is the more specific one only when it wins somewhere
/// and loses nowhere: `users/me` beats `users/{id}`, while `a/{x}/c`
/// and `a/b/{y}` each win a position, so neither can claim the call
/// `a/b/c` over the other.
pub fn compare_patterns(a: &RoutePattern, b: &RoutePattern) -> PatternOrder {
    if a.segments.len() != b.segments.len() {
        return PatternOrder::Disjoint;
    }
    let mut first_wins = false;
    let mut second_wins = false;
    for (x, y) in a.segments.iter().zip(&b.segments) {
        match (x, y) {
            (Segment::Literal(l), Segment::Literal(r)) if l != r => {
                return PatternOrder::Disjoint;
            }
            (Segment::Literal(_), Segment::Param(_)) => first_wins = true,
            (Segment::Param(_), Segment::Literal(_)) => second_wins = true,
            _ => {}
        }
    }
    match (first_wins, second_wins) {
        (true, false) => PatternOrder::FirstWins,
        (false, true) => PatternOrder::SecondWins,
        _ => PatternOrder::Ambiguous,
    }
}

/// Could one call match both method lists? An empty list is "any
/// method", so it overlaps everything; otherwise they must share one.
pub fn methods_overlap(a: &[String], b: &[String]) -> bool {
    a.is_empty() || b.is_empty() || a.iter().any(|m| b.contains(m))
}

/// A registered route as the matcher sees it: its pattern and the
/// methods it serves (empty = any).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteKey {
    pub pattern: RoutePattern,
    pub methods: Vec<String>,
}

impl RouteKey {
    fn serves_method(&self, method: &str) -> bool {
        self.methods.is_empty() || self.methods.iter().any(|m| m == method)
    }
}

/// What a lookup found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteMatch<T> {
    /// A route serves this call: the candidate's tag and the captured
    /// parameters.
    Found { route: T, params: BTreeMap<String, String> },
    /// A route matches the path but none of them serves the method:
    /// the methods those routes do serve (empty when one of them
    /// serves any method, which cannot happen here: that one would
    /// have matched).
    WrongMethod { allowed: Vec<String> },
    /// No route matches the path at all.
    NotFound,
}

/// Find the route serving `method` on `path` among `candidates`. The
/// most specific matching pattern wins, and [`RoutePattern::param_count`]
/// is how it is picked: every pair register time let through is one
/// where a literal beats a capture everywhere they differ, so within
/// the candidates for a single call, fewer captures IS more specific.
/// A tie means two patterns [`compare_patterns`] calls ambiguous, which
/// register time refuses, so it resolves to the first in the order
/// given rather than guessing. A path match with no method match is
/// [`RouteMatch::WrongMethod`] so the gateway can answer 405 with the
/// allowed list.
pub fn find_route<T>(
    candidates: impl IntoIterator<Item = (RouteKey, T)>,
    method: &str,
    path: &str,
) -> RouteMatch<T> {
    let mut best: Option<(usize, T, BTreeMap<String, String>)> = None;
    let mut allowed: Vec<String> = Vec::new();
    for (key, tag) in candidates {
        let Some(params) = key.pattern.match_path(path) else { continue };
        if !key.serves_method(method) {
            for m in &key.methods {
                if !allowed.contains(m) {
                    allowed.push(m.clone());
                }
            }
            continue;
        }
        let specificity = key.pattern.param_count();
        match &best {
            Some((current, _, _)) if *current <= specificity => {}
            _ => best = Some((specificity, tag, params)),
        }
    }
    match best {
        Some((_, route, params)) => RouteMatch::Found { route, params },
        None if !allowed.is_empty() => {
            allowed.sort();
            RouteMatch::WrongMethod { allowed }
        }
        None => RouteMatch::NotFound,
    }
}

/// Parse a raw query string (`a=1&b=x%20y`) into a map. Repeated keys
/// keep the last value; a key without `=` maps to the empty string.
/// Runtime-only: the URL stack it decodes with is not part of the pure
/// type layer the browser parse build compiles.
#[cfg(feature = "runtime")]
pub fn parse_query(raw: &str) -> BTreeMap<String, String> {
    url::form_urlencoded::parse(raw.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pat(s: &str) -> RoutePattern {
        RoutePattern::parse(s).expect("parses")
    }

    #[test]
    fn parses_literals_and_params() {
        let p = pat("chat/{room}/messages");
        assert_eq!(
            p.segments(),
            &[
                Segment::Literal("chat".into()),
                Segment::Param("room".into()),
                Segment::Literal("messages".into())
            ]
        );
        assert_eq!(p.param_count(), 1);
        assert_eq!(p.as_str(), "chat/{room}/messages");
        assert!(pat("").segments().is_empty(), "the root pattern has no segments");
    }

    #[test]
    fn refuses_malformed_patterns() {
        for bad in ["/chat", "chat//x", "chat/{", "chat/{}", "chat/{a}/{a}", "chat/x{y}", "a/{b-c}"] {
            assert!(RoutePattern::parse(bad).is_err(), "'{bad}' must be refused");
        }
    }

    #[test]
    fn matches_capture_params_and_refuse_wrong_shapes() {
        let p = pat("users/{id}/posts");
        let params = p.match_path("users/42/posts").expect("matches");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
        assert!(p.match_path("users/42").is_none(), "segment count differs");
        assert!(p.match_path("users/42/comments").is_none(), "literal differs");
        assert!(pat("").match_path("").is_some(), "root matches the empty path");
        assert!(pat("").match_path("x").is_none());
        assert!(pat("x").match_path("").is_none());
    }

    #[test]
    fn overlap_is_positional() {
        use PatternOrder::*;
        // Two captures in the same place: a shared call has no answer.
        assert_eq!(compare_patterns(&pat("chat/{room}"), &pat("chat/{x}")), Ambiguous);
        // A literal where the other captures: the literal serves that
        // one call, the capture serves the rest.
        assert_eq!(compare_patterns(&pat("chat/general"), &pat("chat/{room}")), FirstWins);
        assert_eq!(compare_patterns(&pat("chat/{room}"), &pat("chat/general")), SecondWins);
        // Each wins a position, so neither can claim `a/b/c`.
        assert_eq!(compare_patterns(&pat("a/{x}/c"), &pat("a/b/{y}")), Ambiguous);
        // The same pattern twice is the degenerate ambiguous pair.
        assert_eq!(compare_patterns(&pat("chat/general"), &pat("chat/general")), Ambiguous);
        // Nothing to arbitrate: no call reaches both.
        assert_eq!(compare_patterns(&pat("chat/{room}"), &pat("chat/{room}/x")), Disjoint);
        assert_eq!(compare_patterns(&pat("chat/{room}"), &pat("mail/{room}")), Disjoint);
        // A pattern the refusal allows always ranks the way the matcher
        // ranks: more specific means fewer captures.
        assert!(pat("chat/general").param_count() < pat("chat/{room}").param_count());
        assert!(methods_overlap(&[], &["GET".into()]));
        assert!(methods_overlap(&["GET".into(), "POST".into()], &["POST".into()]));
        assert!(!methods_overlap(&["GET".into()], &["POST".into()]));
    }

    #[test]
    fn method_names_are_normalized_and_checked() {
        assert_eq!(normalize_method(" post ").unwrap(), "POST");
        assert!(normalize_method("GTE").is_err());
    }

    fn key(pattern: &str, methods: &[&str]) -> RouteKey {
        RouteKey { pattern: pat(pattern), methods: methods.iter().map(|m| m.to_string()).collect() }
    }

    /// The pair this builds is one the register path lets through
    /// (`compare_patterns` makes `users/me` the more specific), so this
    /// is the ordinary arrangement of any API, not a hypothetical: the
    /// literal answers its own call and the capture answers the rest.
    /// Note the capture is second in the candidate order, which is what
    /// proves the ranking is doing the work rather than the order.
    #[test]
    fn the_most_specific_pattern_wins() {
        let routes = vec![(key("users/{id}", &[]), "by-id"), (key("users/me", &[]), "me")];
        match find_route(routes.clone(), "GET", "users/me") {
            RouteMatch::Found { route, params } => {
                assert_eq!(route, "me");
                assert!(params.is_empty());
            }
            other => panic!("expected the literal route, got {other:?}"),
        }
        match find_route(routes, "GET", "users/7") {
            RouteMatch::Found { route, params } => {
                assert_eq!(route, "by-id");
                assert_eq!(params.get("id").map(String::as_str), Some("7"));
            }
            other => panic!("expected the param route, got {other:?}"),
        }
    }

    #[test]
    fn wrong_method_lists_what_is_allowed_and_no_path_is_not_found() {
        let routes = vec![(key("items", &["POST"]), 1), (key("items", &["DELETE"]), 2)];
        assert_eq!(
            find_route(routes.clone(), "GET", "items"),
            RouteMatch::WrongMethod { allowed: vec!["DELETE".into(), "POST".into()] }
        );
        assert_eq!(find_route(routes, "GET", "nothing"), RouteMatch::NotFound);
    }

    #[test]
    fn any_method_route_serves_every_method() {
        let routes = vec![(key("ping", &[]), ())];
        assert!(matches!(find_route(routes, "DELETE", "ping"), RouteMatch::Found { .. }));
    }

    #[cfg(feature = "runtime")]
    #[test]
    fn query_parses_into_a_map() {
        let q = parse_query("a=1&b=x%20y&flag&a=2");
        assert_eq!(q.get("a").map(String::as_str), Some("2"), "last repeat wins");
        assert_eq!(q.get("b").map(String::as_str), Some("x y"));
        assert_eq!(q.get("flag").map(String::as_str), Some(""));
        assert!(parse_query("").is_empty());
    }
}
