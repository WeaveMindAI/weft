//! The public door of an infra node's endpoint marked `Expose::Public`:
//! `/infra/<project>/<copy_id>/<path>` on the install's address, passed on
//! to the endpoint with the prefix taken off.
//!
//! The unit's own address is on the install's network only; this door is
//! how the internet reaches the one endpoint its node opened. Which
//! endpoints are open, and where each answers, is what the supervisor
//! wrote on the copy's row when it applied it; nothing else is reachable.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::state::DispatcherState;

/// The prefix every public infra path starts with.
// SYNC: public infra path <-> crates/weft-core/src/infra/resolve.rs (public_path)
pub(crate) const INFRA_PREFIX: &str = "/infra";

/// `(project, copy_id)` of a door path, or `None`.
fn split(path: &str) -> Option<(uuid::Uuid, &str)> {
    let rest = path.strip_prefix(INFRA_PREFIX)?.strip_prefix('/')?;
    let (project, rest) = rest.split_once('/').unwrap_or((rest, ""));
    let copy_id = rest.split('/').next().filter(|i| !i.is_empty())?;
    Some((project.parse().ok()?, copy_id))
}

/// The endpoint whose public path is the longest prefix of `path` (at a
/// segment boundary), and the path the endpoint is called at: what
/// follows its public path, under `/`.
fn route<'a>(public_paths: impl Iterator<Item = (&'a String, &'a String)>, path: &str) -> Option<(&'a str, String)> {
    public_paths
        .filter(|(_, prefix)| {
            path.strip_prefix(prefix.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .max_by_key(|(_, prefix)| prefix.len())
        .map(|(ep, prefix)| {
            let rest = &path[prefix.len()..];
            (ep.as_str(), if rest.is_empty() { "/".to_string() } else { rest.to_string() })
        })
}

/// `ANY /infra/{project}/{copy_id}/...`
pub async fn forward(State(state): State<DispatcherState>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let Some((project, copy_id)) = split(&path) else {
        return (StatusCode::NOT_FOUND, "no infra endpoint at this path").into_response();
    };
    let rows = match crate::infra_node::list_for_project(&state.pg_pool, project).await {
        Ok(rows) => rows,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("read the project's infra: {e:#}")).into_response(),
    };
    let Some(row) = rows.iter().find(|r| r.copy_id == copy_id) else {
        return (StatusCode::NOT_FOUND, "no infra endpoint at this path").into_response();
    };
    let Some((endpoint, under)) = route(row.public_paths.iter(), &path) else {
        return (StatusCode::NOT_FOUND, "no public endpoint at this path").into_response();
    };
    let Some(base_url) = row.install_endpoints.get(endpoint).cloned() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "the endpoint has no address yet; its node is still starting").into_response();
    };
    let path_and_query = match request.uri().query() {
        Some(q) => format!("{under}?{q}"),
        None => under,
    };
    let upstream = crate::proxy::Upstream { what: "the infra endpoint", base_url, auth: None, hold: None };
    crate::proxy::forward(&state.http, upstream, path_and_query, request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const P: &str = "5f0c3d52-6c6b-4c1e-9d4e-0a2b3c4d5e6f";

    #[test]
    fn a_door_path_names_its_copy() {
        let project: uuid::Uuid = P.parse().unwrap();
        assert_eq!(split(&format!("/infra/{P}/wn-1/api/x")), Some((project, "wn-1")));
        assert_eq!(split(&format!("/infra/{P}/wn-1")), Some((project, "wn-1")));
        assert_eq!(split(&format!("/infra/{P}/")), None);
        assert_eq!(split("/infra/nope/wn-1"), None);
    }

    #[test]
    fn the_longest_open_path_wins_at_a_segment_boundary() {
        let mut paths = BTreeMap::new();
        paths.insert("root".to_string(), format!("/infra/{P}/wn-1"));
        paths.insert("api".to_string(), format!("/infra/{P}/wn-1/api"));
        assert_eq!(route(paths.iter(), &format!("/infra/{P}/wn-1/api/users")), Some(("api", "/users".into())));
        assert_eq!(route(paths.iter(), &format!("/infra/{P}/wn-1/api")), Some(("api", "/".into())));
        assert_eq!(route(paths.iter(), &format!("/infra/{P}/wn-1/apix")), Some(("root", "/apix".into())));
        paths.remove("root");
        assert_eq!(route(paths.iter(), &format!("/infra/{P}/wn-1/other")), None);
    }
}
