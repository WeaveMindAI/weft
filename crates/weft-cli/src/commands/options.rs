//! `weft options <step> <field>`: print the choices a `remote_select`
//! field offers, the same list the graph editor's dropdown shows when a
//! person searches that field (`ResourceSelect.svelte` over
//! `editor-connect.ts`'s `editorResources`). The connection signing the
//! call is the one the editor traces: the install's pick on the step's
//! own access field, or on the access node wired into its Access input.

use std::collections::{BTreeMap, HashSet};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use uuid::Uuid;
use weft_core::access::lookup::{GrantedQuery, LookupItem, LookupPage, LookupRequest};
use weft_core::node::{Lookup, MetadataCatalog, ResourceSource, Widget};

use crate::commands::connect::{install_picks, list_grants, resolve_step, AccessTarget, Doorway, Pick, Step};
use crate::commands::Ctx;

/// The connection a field's sources are signed with.
struct Signing {
    access_id: Uuid,
    service: String,
    /// The connection's granted permissions, which decide whether a
    /// `list` source that `requires` some is usable.
    scopes: Vec<String>,
}

pub async fn run(ctx: Ctx, step_name: &str, field: &str, search: Option<&str>) -> Result<()> {
    let project = ctx.project()?;
    let catalog = weft_compiler::build::build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    let step = resolve_step(project, &catalog, step_name)?;
    let meta = MetadataCatalog::lookup(&catalog, &step.node_type)
        .with_context(|| format!("'{}' is a {}, a type the catalog does not know", step.spelling, step.node_type))?;
    let input = meta.inputs.iter().find(|i| i.name == field).with_context(|| {
        format!(
            "'{}' ({}) has no input '{field}' (its inputs are: {})",
            step.spelling,
            step.node_type,
            meta.inputs.iter().map(|i| i.name.as_str()).collect::<Vec<_>>().join(", ")
        )
    })?;
    let Widget::RemoteSelect { access, sources, depends_on, .. } = input.effective_widget() else {
        bail!(
            "'{}.{field}' is not a searchable field (its widget is {}); only a \
             remote_select field has a list of choices to print",
            step.spelling,
            input.effective_widget().kind_name()
        );
    };
    let own_widget = meta
        .inputs
        .iter()
        .any(|i| i.name == access && matches!(i.effective_widget(), Widget::Access { .. }));
    let client = ctx.client()?;
    crate::commands::ensure::ensure_project_known(&ctx).await?;
    let picks = install_picks(&client, project.id()).await?;
    let signing = match traced_access(&step, &access, own_widget, &picks)? {
        Some((target, id)) => {
            let grants = list_grants(&client, Doorway::Owner, Some(&target.spec.service)).await?;
            let grant = grants.into_iter().find(|g| g.id == id).with_context(|| {
                format!(
                    "'{}' is picked on a connection that no longer exists ({id}); pick \
                     another with `weft connect --node {} --list`, then `--grant <id>`",
                    target.spelling(),
                    target.spelling()
                )
            })?;
            Some(Signing { access_id: id, service: grant.service, scopes: grant.scopes })
        }
        None => None,
    };
    let node = step
        .program
        .nodes
        .iter()
        .find(|n| n.id == step.node)
        .context("the resolved step is missing from the program")?;
    let parents = parent_values(&depends_on, |name| node.written_value(name));
    let query = search.unwrap_or("").trim();

    let usable = usable_sources(&sources, signing.as_ref());
    let mut pages: Vec<LookupPage> = Vec::new();
    // Granted first (free, off the connection row) when nothing is
    // typed; recorded nothing: fall through to the list, as the editor.
    if query.is_empty() {
        if let (Some(ResourceSource::Granted { from, label, value }), Some(s)) =
            (usable.iter().find(|s| matches!(s, ResourceSource::Granted { .. })), &signing)
        {
            let body = GrantedQuery {
                access_id: s.access_id,
                service: s.service.clone(),
                for_instance: None,
                from: from.clone(),
                label: label.clone(),
                value: value.clone(),
            };
            let items: Vec<LookupItem> = serde_json::from_value(client.post_json("/access/granted", &serde_json::to_value(&body)?).await?)
                .context("parse the recorded choices")?;
            if !items.is_empty() {
                pages.push(LookupPage { items, next_cursor: None });
            }
        }
    }
    if pages.is_empty() {
        let Some(ResourceSource::List { lookup, .. }) =
            usable.iter().find(|s| matches!(s, ResourceSource::List { .. }))
        else {
            bail!("{}", nothing_to_list(&step, field, &access, own_widget, &sources, signing.is_some(), query));
        };
        let mut cursor: Option<String> = None;
        // Every cursor the service has handed out: one coming back is a
        // loop through the same pages, however long its period.
        let mut seen: HashSet<String> = HashSet::new();
        loop {
            let body = lookup_request(lookup, signing.as_ref().map(|s| (s.access_id, s.service.as_str())), query, &parents, cursor.as_deref());
            let page: LookupPage = serde_json::from_value(client.post_json("/access/lookup", &serde_json::to_value(&body)?).await?)
                .context("parse the lookup answer")?;
            let next = page.next_cursor.clone();
            pages.push(LookupPage { items: narrow(lookup, query, page.items), next_cursor: next.clone() });
            match next {
                Some(n) if !n.is_empty() => {
                    if !seen.insert(n.clone()) {
                        bail!(
                            "the service handed back a page cursor it already gave ({n}), so its \
                             pages go round in a loop; the list is cut here rather than read forever"
                        );
                    }
                    cursor = Some(n);
                }
                _ => break,
            }
        }
    }

    if ctx.json_out(&json!({ "step": step.spelling, "field": field, "pages": pages }))? {
        return Ok(());
    }
    let items: Vec<&LookupItem> = pages.iter().flat_map(|p| &p.items).collect();
    if items.is_empty() {
        if query.is_empty() {
            println!("(no choices)");
        } else {
            println!("(no choices match '{query}')");
        }
    }
    for item in items {
        println!("{}  {}", item.id, item.label);
    }
    Ok(())
}

/// The access node and picked connection the field is signed with, the
/// editor's structural trace: the step's own access field, or the
/// access node wired into its Access input (followed through group
/// boundaries, whose passthroughs share one port name on both sides).
/// `None` when nothing is picked. The pick read is the install's at the
/// access node's place in the same call as the step.
fn traced_access<'a>(
    step: &'a Step,
    access: &str,
    own_widget: bool,
    picks: &weft_core::picks::Picks,
) -> Result<Option<(&'a AccessTarget, Uuid)>> {
    let source = if own_widget {
        step.node.clone()
    } else {
        let mut at = (step.node.clone(), access.to_string());
        loop {
            let Some(edge) = step
                .program
                .edges
                .iter()
                .find(|e| e.target == at.0 && e.target_handle.as_deref() == Some(at.1.as_str()))
            else {
                return Ok(None);
            };
            let is_boundary = step
                .program
                .nodes
                .iter()
                .any(|n| n.id == edge.source && n.group_boundary.is_some());
            match (&edge.source_handle, is_boundary) {
                (Some(port), true) => at = (edge.source.clone(), port.clone()),
                _ => break edge.source.clone(),
            }
        }
    };
    let Some(target) = step.targets.iter().find(|t| t.node == source) else {
        return Ok(None);
    };
    match &target.picked {
        Pick::InstanceFilled => bail!(
            "'{}' is connected separately in each instance of the program, so there is no one \
             connection to list choices through",
            target.spelling()
        ),
        Pick::WrittenInSource => bail!("{}", crate::commands::connect::written_in_source(target)),
        Pick::None | Pick::Handle(_) => {
            // The access node's place in the step's call: the step's
            // spelling up to its own name, then the node's.
            let prefix = step.spelling.rsplit_once('.').map(|(call, _)| format!("{call}.")).unwrap_or_default();
            let place = target
                .spellings()
                .iter()
                .find(|s| s.strip_prefix(&prefix).is_some_and(|rest| !rest.contains('.')))
                .cloned()
                .unwrap_or_else(|| target.spelling());
            let id = picks
                .get(&place)
                .and_then(|fields| fields.get(target.input()))
                .and_then(|handle| handle.get("id"))
                .and_then(Value::as_str)
                .and_then(|id| id.parse::<Uuid>().ok());
            Ok(id.map(|id| (target, id)))
        }
    }
}

/// The sources the editor keeps: a connection-backed one only with a
/// connection, a `list` also only when its `requires` are held; a
/// `public` list and a pasted link always.
fn usable_sources<'a>(sources: &'a [ResourceSource], signing: Option<&Signing>) -> Vec<&'a ResourceSource> {
    sources
        .iter()
        .filter(|s| match s {
            ResourceSource::Granted { .. } | ResourceSource::Picker { .. } => signing.is_some(),
            ResourceSource::List { lookup, requires } => {
                lookup.public || signing.is_some_and(|g| requires.iter().all(|r| g.scopes.contains(r)))
            }
            ResourceSource::FromUrl { .. } => true,
        })
        .collect()
}

/// Each `depends_on` parent's picked id, as the step's source holds it;
/// an unset or empty parent is left out.
fn parent_values<'a>(depends_on: &[String], written: impl Fn(&str) -> Option<&'a Value>) -> BTreeMap<String, String> {
    depends_on
        .iter()
        .filter_map(|p| match written(p) {
            Some(Value::String(v)) if !v.is_empty() => Some((p.clone(), v.clone())),
            _ => None,
        })
        .collect()
}

/// The `/access/lookup` body, as the editor posts it: the search text
/// travels only when the URL declares `{query}` (the service filters);
/// otherwise the list comes back whole and [`narrow`] filters it here.
fn lookup_request(
    lookup: &Lookup,
    signing: Option<(Uuid, &str)>,
    query: &str,
    parents: &BTreeMap<String, String>,
    cursor: Option<&str>,
) -> LookupRequest {
    LookupRequest {
        access_id: signing.map(|(id, _)| id),
        for_instance: None,
        service: signing.map(|(_, s)| s.to_string()),
        lookup: lookup.clone(),
        query: if server_filtered(lookup) { query.to_string() } else { String::new() },
        parents: parents.clone(),
        cursor: cursor.map(str::to_string),
    }
}

fn server_filtered(lookup: &Lookup) -> bool {
    lookup.get.contains("{query}")
}

/// The editor's local narrowing when the service does not filter: a
/// case-insensitive substring of the label or the id.
fn narrow(lookup: &Lookup, query: &str, items: Vec<LookupItem>) -> Vec<LookupItem> {
    if server_filtered(lookup) || query.is_empty() {
        return items;
    }
    let q = query.to_lowercase();
    items
        .into_iter()
        .filter(|i| i.label.to_lowercase().contains(&q) || i.id.to_lowercase().contains(&q))
        .collect()
}

/// Why nothing could be listed, naming the fix. Reached when no usable
/// `list` source is left and the granted choices (if any) gave nothing.
fn nothing_to_list(
    step: &Step,
    field: &str,
    access: &str,
    own_widget: bool,
    sources: &[ResourceSource],
    connected: bool,
    query: &str,
) -> String {
    let at = format!("'{}.{field}'", step.spelling);
    let has_list = sources.iter().any(|s| matches!(s, ResourceSource::List { .. }));
    let has_granted = sources.iter().any(|s| matches!(s, ResourceSource::Granted { .. }));
    if !has_list && !has_granted {
        return format!(
            "{at} has no list to print: its choices come from the provider's own chooser \
             or a pasted link, so open it in the graph editor, or write the id in the source"
        );
    }
    if !connected {
        return if own_widget {
            format!(
                "{at} needs a connection first: pick one on this step with `weft connect --node {} \
                 --list`, then `--grant <id>`",
                step.spelling
            )
        } else {
            format!(
                "{at} needs a connection first: wire its `{access}` input to an access node, then \
                 pick that node's connection with `weft connect --node <access node> --list`, then \
                 `--grant <id>`"
            )
        };
    }
    if has_list {
        // Connected, a list declared, and none usable: its `requires`
        // are not all held.
        return format!(
            "{at} can only be listed with permissions the picked connection does not hold; \
             connect one that has them with `weft connect --node <access node>`"
        );
    }
    // Only the choices recorded on the connection when it was made.
    if !query.is_empty() {
        return format!(
            "{at} offers only the choices recorded on the connection, and those cannot be \
             searched; run it without --search to print them all"
        );
    }
    format!(
        "{at} offers only the choices recorded on the connection when it was made, and it \
         recorded none; connect it again with `weft connect --node <access node>` to record them"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(get: &str) -> Lookup {
        Lookup {
            get: get.into(),
            items: "data".into(),
            label: "name".into(),
            value: "id".into(),
            page: None,
            public: false,
        }
    }

    #[test]
    fn request_carries_the_search_only_when_the_url_asks_for_it() {
        let id = Uuid::nil();
        let parents = BTreeMap::from([("repo".to_string(), "r1".to_string())]);
        let filtered = lookup("https://x/items?q={query}&repo={repo}");
        let body = serde_json::to_value(lookup_request(&filtered, Some((id, "github")), "abc", &parents, Some("c2"))).unwrap();
        assert_eq!(
            body,
            json!({
                "access_id": id,
                "service": "github",
                "lookup": { "get": "https://x/items?q={query}&repo={repo}", "items": "data", "label": "name", "value": "id" },
                "query": "abc",
                "parents": { "repo": "r1" },
                "cursor": "c2",
            })
        );
        let whole = lookup("https://x/items");
        let body = serde_json::to_value(lookup_request(&whole, None, "abc", &BTreeMap::new(), None)).unwrap();
        assert_eq!(body["query"], "");
        assert_eq!(body["access_id"], Value::Null);
        assert_eq!(body["service"], Value::Null);
        assert_eq!(body["cursor"], Value::Null);
    }

    #[test]
    fn a_whole_list_is_narrowed_here_by_label_or_id() {
        let items = vec![
            LookupItem { id: "openai/gpt-5".into(), label: "GPT".into() },
            LookupItem { id: "x/y".into(), label: "DeepSeek".into() },
        ];
        let got = narrow(&lookup("https://x"), "deep", items.clone());
        assert_eq!(got.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(), ["x/y"]);
        let got = narrow(&lookup("https://x"), "OPENAI", items.clone());
        assert_eq!(got.len(), 1);
        assert_eq!(narrow(&lookup("https://x?q={query}"), "deep", items).len(), 2);
    }

    #[test]
    fn sources_follow_the_connection_and_its_permissions() {
        let list = |public: bool, requires: &[&str]| ResourceSource::List {
            lookup: Lookup { public, ..lookup("https://x") },
            requires: requires.iter().map(|r| r.to_string()).collect(),
        };
        let sources = vec![list(false, &["read"]), list(true, &[])];
        assert_eq!(usable_sources(&sources, None).len(), 1);
        let held = Signing { access_id: Uuid::nil(), service: "s".into(), scopes: vec!["read".into()] };
        assert_eq!(usable_sources(&sources, Some(&held)).len(), 2);
        let lacking = Signing { scopes: vec![], ..held };
        assert_eq!(usable_sources(&sources, Some(&lacking)).len(), 1);
    }

    #[test]
    fn parents_skip_unset_and_empty_values() {
        let values = BTreeMap::from([
            ("a".to_string(), json!("1")),
            ("b".to_string(), json!("")),
            ("c".to_string(), json!(3)),
        ]);
        let got = parent_values(&["a".into(), "b".into(), "c".into(), "d".into()], |k| values.get(k));
        assert_eq!(got, BTreeMap::from([("a".to_string(), "1".to_string())]));
    }
}
