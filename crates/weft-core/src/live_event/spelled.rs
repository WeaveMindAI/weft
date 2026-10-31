//! A run's events the way a person reads them: kept by what they are
//! about ([`EventsFilter`]) and spelled the way the program is written
//! ([`spell_row`]), both read against the program the run ran. `GET
//! /runs/{id}/events` answers `weft events` this way, and `weft run`
//! prints the run it started the same way as it goes.

use serde_json::Value;

use super::DispatcherEvent;
use crate::frames::{call_path, loop_indices, Frame, Located, LoopFrames};
use crate::project::graph::GraphView;
use crate::project::places::{enclosing_loops, scopes_around_group, Named};
use crate::project::{address_of, group_address, placed_address, GroupBoundaryRole};

/// What a read of a run's events keeps ([`EventsFilter::anywhere`],
/// [`EventsFilter::at`]). The default keeps every row.
#[derive(Debug, Default, Clone)]
pub struct EventsFilter {
    /// The node or group the rows must be about, at its place in the
    /// run's program (`places::named` read it): the node's rows, a loop's
    /// with its own two boundaries' and its lifecycle, a group's
    /// decisions, and only those under its call path.
    place: Option<Located>,
    /// A kind, matched exactly or as a substring, so `failed` finds both
    /// `node_failed` and `run_failed`, and `loop` the loop lifecycle.
    kind: Option<String>,
    /// Iterations of the loops around `place`, outermost first
    /// ([`parse_iteration`]): only its rows fired inside them. Empty = any.
    iteration: Vec<u32>,
}

/// Read an iteration filter: iteration numbers from 0, outermost loop
/// first, joined by `.` (`3`, or `3.0` for the first inner iteration
/// inside the fourth outer one).
pub fn parse_iteration(text: &str) -> Result<Vec<u32>, String> {
    text.split('.')
        .map(|part| {
            part.trim().parse::<u32>().map_err(|_| {
                format!(
                    "--iteration takes loop iteration numbers from 0, outermost loop first, joined by '.' \
                     (`3`, or `3.0` for an inner loop's first iteration); got '{text}'"
                )
            })
        })
        .collect()
}

/// THE check of an iteration against the place it counts from, for
/// `weft events --iteration` ([`EventsFilter::at`]) and `weft wake
/// --iteration` alike. An iteration is one of the loops around the place
/// named, counted from the outermost (a loop's own iteration comes after
/// the loops around it). Refused at a place in no loop, and with more
/// numbers than there are loops around the place. Answers how many loops
/// there are.
pub fn check_iteration(g: &impl GraphView, named: &Named, iteration: &[u32]) -> Result<usize, String> {
    let (spelled, loops) = match named {
        Named::Node(place) => (address_of(g, &place.id, &place.path), enclosing_loops(g, place).len()),
        Named::Group(place) => {
            let around = scopes_around_group(g, place).into_iter().filter(|scope| g.is_loop(&scope.id)).count();
            (group_address(g, &place.id, &place.path), around + usize::from(g.is_loop(&place.id)))
        }
    };
    if !iteration.is_empty() && loops == 0 {
        return Err(format!("'{spelled}' is in no loop, so it has no iterations; drop --iteration"));
    }
    if iteration.len() > loops {
        let plural = if loops == 1 { "" } else { "s" };
        return Err(format!(
            "'{spelled}' runs inside {loops} loop{plural}, so --iteration takes at most {loops} number{plural}, outermost loop first"
        ));
    }
    Ok(loops)
}

impl EventsFilter {
    /// The filter keeping the rows of `kind` about any place. An
    /// iteration counts from a place (see [`Self::at`]), so one is refused
    /// here: `3` alone would be the fourth iteration of every loop in the
    /// program at once.
    pub fn anywhere(kind: Option<String>, iteration: Vec<u32>) -> Result<Self, String> {
        if !iteration.is_empty() {
            return Err("--iteration counts the iterations of the loops around the node --node names, so it needs \
                        --node: the loop, or a node inside it"
                .to_string());
        }
        Ok(Self { place: None, kind, iteration })
    }

    /// The filter keeping the rows about `named` (read against the run's
    /// program `g`), of `kind`, inside the loop iterations `iteration`
    /// ([`check_iteration`] refuses one that cannot count from there).
    pub fn at(g: &impl GraphView, named: Named, kind: Option<String>, iteration: Vec<u32>) -> Result<Self, String> {
        check_iteration(g, &named, &iteration)?;
        Ok(Self { place: Some(named.into_place()), kind, iteration })
    }

    /// Whether one row survives the filters. `row` is `event` as it reads
    /// on the wire, still by id: the kind filter reads its spelling, the
    /// place and iteration filters the typed place ([`DispatcherEvent::place`]).
    /// A run-level row (a start, a completion, the run failing) names no
    /// place, so it never passes a place filter.
    pub fn keeps<V>(&self, event: &DispatcherEvent<V>, row: &Value) -> bool {
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let kind_ok = self.kind.as_deref().is_none_or(|k| kind == k || kind.contains(k));
        let Some(place) = &self.place else { return kind_ok };
        let Some((id, frames)) = event.place() else { return false };
        let n = place.id.as_str();
        let named = id == n || crate::project::boundary_loop(id) == Some(n);
        let path_ok = call_path(&frames).into_iter().eq(place.path.iter().map(String::as_str));
        // An inner loop's rows belong to the outer iteration they ran in.
        let iteration_ok = loop_indices(&frames).starts_with(&self.iteration);
        kind_ok && named && path_ok && iteration_ok
    }
}

/// The node a row is about. Most rows name it `node`; the two that come
/// off a pulse rather than a record row (`cost_reported`,
/// `bus_participant`) name it `nodeId`.
pub fn row_node(row: &Value) -> Option<&str> {
    row.get("node").or_else(|| row.get("nodeId")).and_then(|v| v.as_str())
}

/// The row with everything it names spelled the way the program reads: a
/// node of an included file through the site its call frames say
/// (`one.strip`), so two calls of one file read apart; a loop's boundary
/// as the loop (`doubler`), with `boundary=in|out` saying which end; the
/// call frames, a loop's id, a decided scope, a skip's scope, the run's
/// subgraph, the seed's origins and the completion's outputs the same
/// way. The loop frames stay, for the iteration. A node the program does
/// not have keeps its id and says so under `unplaced`: the row was read
/// against a program it does not belong to, and a plausible spelling
/// would hide that.
///
/// `None` for a row that has no line of its own: an included body's copy
/// being decided. Each call of an included file decides two copies, the
/// site's and the body's, and both spell as the site; the site's decision
/// is the call's, so only its row is shown.
///
/// `g` is the program indexed ([`crate::project::graph::ProjectGraph`])
/// by a reader spelling many rows, so each lookup is constant time.
pub fn spell_row(mut row: Value, g: &impl GraphView) -> Option<Value> {
    let boundary_of = |id: &str| g.node(id).and_then(|n| n.group_boundary.as_ref())
        .map(|b| match b.role { GroupBoundaryRole::In => "in", GroupBoundaryRole::Out => "out" });
    let spell_place = |place: &Located| address_of(g, &place.id, &place.path);
    let spell_frames = |frames: &LoopFrames| -> Vec<Value> {
        let mut above: Vec<String> = Vec::new();
        frames.iter().map(|frame| match frame {
            Frame::Loop { index } => serde_json::json!({ "index": index }),
            Frame::Call { site } => {
                let spelled = address_of(g, site, &above);
                above.push(site.clone());
                serde_json::json!({ "site": spelled })
            }
        }).collect()
    };
    let frames: LoopFrames = row.get("frames").and_then(|f| serde_json::from_value(f.clone()).ok()).unwrap_or_default();
    let path: Vec<String> = call_path(&frames).into_iter().map(str::to_string).collect();
    let key = ["node", "nodeId"].into_iter().find(|key| row.get(key).and_then(|v| v.as_str()).is_some());
    if let Some(key) = key {
        let id = row[key].as_str().unwrap_or_default().to_string();
        match placed_address(g, &id, &path) {
            Some(spelled) => row[key] = Value::String(spelled),
            None => row["unplaced"] = Value::String(format!("'{id}' is no node of the program this row was read against")),
        }
        if let Some(role) = boundary_of(&id) { row["boundary"] = Value::String(role.into()); }
    }
    // A decided scope names the group or the site at its frames.
    if row.get("kind").and_then(|k| k.as_str()) == Some("scope_decided") {
        if let Some(scope) = row.get("scope").and_then(|s| s.as_str()).map(str::to_string) {
            if crate::project::places::is_body(g, &scope) {
                return None;
            }
            row["scope"] = Value::String(group_address(g, &scope, &path));
        }
    }
    if !frames.is_empty() {
        row["frames"] = Value::Array(spell_frames(&frames));
    }
    // A loop's lifecycle rows name the loop and the frames around it.
    let parent_frames: LoopFrames = row.get("parentFrames").and_then(|f| serde_json::from_value(f.clone()).ok()).unwrap_or_default();
    if let Some(group) = row.get("groupId").and_then(|g| g.as_str()).map(str::to_string) {
        let parent_path: Vec<String> = call_path(&parent_frames).into_iter().map(str::to_string).collect();
        row["groupId"] = Value::String(group_address(g, &group, &parent_path));
    }
    if !parent_frames.is_empty() {
        row["parentFrames"] = Value::Array(spell_frames(&parent_frames));
    }
    if let Some(scope) = row.get("reason").and_then(|r| r.get("scope")).and_then(|s| s.as_str()).map(str::to_string) {
        // A site is refused in its caller's frames, one above the call
        // frame its body's rows carry.
        let scope_path = if path.last() == Some(&scope) { &path[..path.len() - 1] } else { &path[..] };
        row["reason"]["scope"] = Value::String(group_address(g, &scope, scope_path));
    }
    // A place list or map (`subgraph`, `seed.origins`, `outputs`): each
    // key spelled, boundaries folded into their group.
    let places = |value: &Value| -> Vec<(Located, Value)> {
        match value {
            Value::Array(items) => items.iter().filter_map(|i| serde_json::from_value::<Located>(i.clone()).ok().map(|p| (p, Value::Null))).collect(),
            Value::Object(map) => map.iter().filter_map(|(k, v)| serde_json::from_str::<Located>(&format!("\"{k}\"")).ok().map(|p| (p, v.clone()))).collect(),
            _ => Vec::new(),
        }
    };
    if let Some(subgraph) = row.get("subgraph").cloned() {
        let mut seen = std::collections::BTreeSet::new();
        row["subgraph"] = Value::Array(places(&subgraph).into_iter()
            .filter(|(place, _)| boundary_of(&place.id).is_none())
            .map(|(place, _)| spell_place(&place)).filter(|s| seen.insert(s.clone())).map(Value::String).collect());
    }
    for key in ["outputs", "origins"] {
        let holder = if key == "origins" { row.get_mut("seed") } else { Some(&mut row) };
        let Some(holder) = holder else { continue };
        if let Some(map) = holder.get(key).cloned().filter(|v| v.is_object()) {
            let spelled: serde_json::Map<String, Value> = places(&map).into_iter()
                .filter(|(place, _)| boundary_of(&place.id).is_none())
                .map(|(place, v)| (spell_place(&place), v)).collect();
            holder[key] = Value::Object(spelled);
        }
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use super::{parse_iteration, spell_row, EventsFilter};
    use crate::frames::Located;
    use crate::live_event::DispatcherEvent;
    use serde_json::json;

    /// One replay row, typed and as it reads on the wire, from the fields
    /// that matter to a filter (the rest filled with throwaway values).
    fn replay_row(mut fields: serde_json::Value) -> (DispatcherEvent, serde_json::Value) {
        let row = fields.as_object_mut().unwrap();
        let filler = json!({
            "runId": "00000000-0000-0000-0000-000000000001",
            "projectId": "00000000-0000-0000-0000-000000000002",
            "atUnix": 1u64, "frames": [], "error": "e", "output": null, "outputs": null,
        });
        let kind = row["kind"].as_str().unwrap().to_string();
        for (key, value) in filler.as_object().unwrap() {
            let wanted = match key.as_str() {
                "frames" | "output" => kind.starts_with("node_") && (key != "output" || kind == "node_completed"),
                "error" => kind.ends_with("_failed"),
                "outputs" => kind == "run_completed",
                _ => true,
            };
            if wanted {
                row.entry(key.clone()).or_insert(value.clone());
            }
        }
        let event: DispatcherEvent = serde_json::from_value(fields.clone()).unwrap();
        let wire = serde_json::to_value(&event).unwrap();
        (event, wire)
    }

    fn keeps(filter: &EventsFilter, (event, row): &(DispatcherEvent, serde_json::Value)) -> bool {
        filter.keeps(event, row)
    }

    fn at(id: &str, path: &[&str]) -> Option<Located> {
        Some(Located::new(id.to_string(), path.iter().map(|s| s.to_string()).collect()))
    }

    /// The kind filter matches exactly or by substring, the place filter
    /// exactly, and a run-level row (no node) never passes a place filter.
    #[test]
    fn events_filter_narrows_by_place_and_kind() {
        let failed = replay_row(json!({"kind": "node_failed", "node": "llm"}));
        let done = replay_row(json!({"kind": "node_completed", "node": "reply"}));
        let run_failed = replay_row(json!({"kind": "run_failed"}));
        let all = EventsFilter::default();
        assert!(keeps(&all, &failed) && keeps(&all, &done) && keeps(&all, &run_failed));
        let by_kind = EventsFilter { kind: Some("failed".into()), ..Default::default() };
        assert!(keeps(&by_kind, &failed) && keeps(&by_kind, &run_failed) && !keeps(&by_kind, &done));
        let exact = EventsFilter { kind: Some("node_completed".into()), ..Default::default() };
        assert!(keeps(&exact, &done) && !keeps(&exact, &failed));
        let by_node = EventsFilter { place: at("llm", &[]), ..Default::default() };
        assert!(keeps(&by_node, &failed) && !keeps(&by_node, &done) && !keeps(&by_node, &run_failed));
        // At a place through a call site, only the rows under that call pass.
        let in_call = replay_row(json!({"kind": "node_completed", "node": "Auth.check", "frames": [{"site": "auth"}]}));
        let other_call = replay_row(json!({"kind": "node_completed", "node": "Auth.check", "frames": [{"index": 1}, {"site": "again"}]}));
        let by_call = EventsFilter { place: at("Auth.check", &["auth"]), ..Default::default() };
        assert!(keeps(&by_call, &in_call) && !keeps(&by_call, &other_call));
        // A loop's rows are its own two boundaries' too.
        let launched = replay_row(json!({"kind": "node_completed", "node": "each__in"}));
        assert!(keeps(&EventsFilter { place: at("each", &[]), ..Default::default() }, &launched));
    }

    /// `iteration` keeps the place's rows fired inside those loop
    /// iterations, an inner loop's rows under the outer iteration they ran
    /// in.
    #[test]
    fn events_filter_narrows_by_loop_iteration() {
        let outer_three = replay_row(json!({"kind": "node_completed", "node": "double", "frames": [{"index": 3}]}));
        let inner = replay_row(json!({"kind": "node_completed", "node": "double", "frames": [{"index": 3}, {"index": 0}]}));
        let outer_one = replay_row(json!({"kind": "node_completed", "node": "double", "frames": [{"index": 1}]}));
        let other_node = replay_row(json!({"kind": "node_completed", "node": "other", "frames": [{"index": 3}]}));
        let run_level = replay_row(json!({"kind": "run_completed"}));
        let third = EventsFilter { place: at("double", &[]), iteration: parse_iteration("3").unwrap(), ..Default::default() };
        assert!(keeps(&third, &outer_three) && keeps(&third, &inner));
        assert!(!keeps(&third, &outer_one) && !keeps(&third, &other_node) && !keeps(&third, &run_level));
        let nested = EventsFilter { place: at("double", &[]), iteration: parse_iteration("3.0").unwrap(), ..Default::default() };
        assert!(keeps(&nested, &inner) && !keeps(&nested, &outer_three));
        assert!(parse_iteration("x").is_err() && parse_iteration("3.").is_err());
    }

    /// A loop's lifecycle rows are the loop's own: they pass `--node` at
    /// the loop's place, and an iteration's launch and gather sit inside
    /// that iteration, while the loop opening and ending sit around it.
    #[test]
    fn a_loops_lifecycle_rows_are_kept_at_its_place_and_iteration() {
        let lifecycle = |kind: &str, extra: serde_json::Value| {
            let mut fields = json!({"kind": kind, "groupId": "each", "parentFrames": [{"index": 1}]});
            fields.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            replay_row(fields)
        };
        let opened = lifecycle("loop_instantiated", json!({"iterCap": 4, "parallel": false}));
        let launched = lifecycle("loop_iteration_launched", json!({"index": 2}));
        let gathered = lifecycle("loop_out_fired", json!({"index": 2, "doneVote": null}));
        let ended = lifecycle("loop_terminated", json!({"reason": "over_exhausted"}));
        let by_loop = EventsFilter { place: at("each", &[]), ..Default::default() };
        for row in [&opened, &launched, &gathered, &ended] {
            assert!(keeps(&by_loop, row), "{:?}", row.1);
        }
        assert!(!keeps(&EventsFilter { place: at("each", &["one"]), ..Default::default() }, &launched), "another call's loop");
        let outer = EventsFilter { place: at("each", &[]), iteration: vec![1], ..Default::default() };
        assert!([&opened, &launched, &gathered, &ended].into_iter().all(|row| keeps(&outer, row)));
        let own = EventsFilter { place: at("each", &[]), iteration: vec![1, 2], ..Default::default() };
        assert!(keeps(&own, &launched) && keeps(&own, &gathered) && !keeps(&own, &opened) && !keeps(&own, &ended));
        assert!(!keeps(&EventsFilter { place: at("each", &[]), iteration: vec![1, 0], ..Default::default() }, &launched));
    }

    /// `plain` at the top, the loop `outer` holding `x` and the loop
    /// `outer.inner`.
    fn program_with_loops() -> crate::ProjectDefinition {
        let node = |id: &str, scope: &[&str]| json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false
        });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("plain", &[]), node("outer.x", &["outer"]), node("outer.inner.y", &["outer", "outer.inner"])],
            "edges": [],
            "groups": [
                {"id": "outer", "kind": "loop", "loopConfig": {}, "nodeIds": ["outer.x"]},
                {"id": "outer.inner", "kind": "loop", "loopConfig": {}, "nodeIds": ["outer.inner.y"], "parentGroupId": "outer"}
            ]
        }))
        .unwrap()
    }

    /// An iteration is counted from the loops around the place named, so
    /// it needs a place, one inside a loop, and no more numbers than there
    /// are loops around it (a loop's own counted after the ones around it).
    #[test]
    fn an_iteration_is_counted_from_the_loops_around_the_named_place() {
        use crate::project::places::named;
        let p = program_with_loops();
        let filter = |spelled: Option<&str>, iteration: &[u32]| match spelled {
            Some(spelled) => EventsFilter::at(&p, named(&p, spelled).unwrap(), None, iteration.to_vec()),
            None => EventsFilter::anywhere(None, iteration.to_vec()),
        };
        assert!(filter(None, &[3]).unwrap_err().contains("needs --node"));
        assert!(filter(Some("plain"), &[0]).unwrap_err().contains("is in no loop"));
        assert!(filter(Some("outer.x"), &[3]).is_ok());
        assert!(filter(Some("outer.x"), &[3, 0]).unwrap_err().contains("at most 1 number,"));
        assert!(filter(Some("outer.inner"), &[3, 0]).is_ok());
        assert!(filter(Some("outer.inner.y"), &[3, 0]).is_ok());
        assert!(filter(Some("outer.inner.y"), &[3, 0, 1]).unwrap_err().contains("at most 2 numbers"));
        assert!(filter(Some("outer"), &[3]).is_ok() && filter(None, &[]).is_ok() && filter(Some("plain"), &[]).is_ok());
    }

    /// A scope's decision is a row of the group it decides, so the frame
    /// filters read its frames as they read a firing's: a group at a call
    /// site's place keeps the decisions under that call, and an iteration
    /// keeps the decisions inside it.
    #[test]
    fn events_filter_reads_a_scope_decisions_frames() {
        let decided = |scope: &str, frames: serde_json::Value| {
            replay_row(json!({"kind": "scope_decided", "scope": scope, "frames": frames, "state": "open"}))
        };
        let in_one = decided("@src:lib.g", json!([{"site": "one"}]));
        let in_two = decided("@src:lib.g", json!([{"site": "two"}]));
        let by_call = EventsFilter { place: at("@src:lib.g", &["one"]), ..Default::default() };
        assert!(keeps(&by_call, &in_one) && !keeps(&by_call, &in_two));
        let third = decided("lp.inner", json!([{"index": 3}]));
        let first = decided("lp.inner", json!([{"index": 1}]));
        let by_iteration = EventsFilter { place: at("lp.inner", &[]), iteration: parse_iteration("3").unwrap(), ..Default::default() };
        assert!(keeps(&by_iteration, &third) && !keeps(&by_iteration, &first));
    }

    /// The file `lib` included at two sites, with a group and a node inside it.
    fn program_including_lib() -> crate::ProjectDefinition {
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "@src:lib.strip", "nodeType": "T", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": {}, "scope": ["@src:lib"], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": [
                { "id": "@src:lib", "kind": "body", "nodeIds": ["@src:lib.strip"], "childGroupIds": ["@src:lib.g"] },
                { "id": "@src:lib.g", "kind": "group", "nodeIds": [], "parentGroupId": "@src:lib" },
                { "id": "one", "kind": "call", "body": "@src:lib", "nodeIds": [] },
                { "id": "two", "kind": "call", "body": "@src:lib", "nodeIds": [] },
            ],
        }))
        .expect("the fixture deserializes")
    }

    /// A decision reads as the group or site it decides, through its call
    /// site; an included body's own decision has no row, since its site's
    /// row is the call's and both would read the same.
    #[test]
    fn a_scope_decision_is_spelled_and_a_bodys_copy_dropped() {
        let p = program_including_lib();
        let decided = |scope: &str, frames: serde_json::Value| {
            replay_row(json!({"kind": "scope_decided", "scope": scope, "frames": frames, "state": "open"})).1
        };
        let site = spell_row(decided("one", json!([])), &p).expect("a site's decision is shown");
        assert_eq!(site["scope"], "one");
        assert!(spell_row(decided("@src:lib", json!([{"site": "one"}])), &p).is_none());
        let nested = spell_row(decided("@src:lib.g", json!([{"site": "two"}])), &p).expect("a group's decision is shown");
        assert_eq!(nested["scope"], "two.g");
    }

    /// A node reads through its call site; one the program does not have
    /// keeps its id and says it is not placed, rather than reading as a
    /// plausible name.
    #[test]
    fn a_node_is_spelled_through_its_site_and_an_unknown_one_says_so() {
        let p = program_including_lib();
        let done = |node: &str| replay_row(json!({"kind": "node_completed", "node": node, "frames": [{"site": "two"}]})).1;
        let spelled = spell_row(done("@src:lib.strip"), &p).unwrap();
        assert_eq!(spelled["node"], "two.strip");
        assert!(spelled.get("unplaced").is_none());
        let stranger = spell_row(done("@src:gone.strip"), &p).unwrap();
        assert_eq!(stranger["node"], "@src:gone.strip");
        assert!(stranger["unplaced"].as_str().unwrap().contains("@src:gone.strip"), "{stranger}");
    }
}
