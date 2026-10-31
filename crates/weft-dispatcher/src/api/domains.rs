//! `GET/POST /install/domains`, `DELETE /install/domains/{name}`: the
//! domains the install answers at (`crate::domains`).
//!
//! Adding one stores it, puts it on the platform's door (which makes the
//! door with the first domain and takes it down with the last, see
//! `weft_platform_traits::DomainHosting`) and answers the DNS record the
//! person has to set, with what the door costs while it stands. A platform
//! with no door (a local install, which the internet reaches through its
//! tunnel) refuses, saying so. Removing one takes it off the door first
//! and forgets it after, so a door that refuses leaves it stored and the
//! removal can be run again.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use weft_core::install::{AddDomain, DnsRecord, DomainAdded, DomainEntry, DomainList};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::domains::ServeError;
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// The record pointing `name` at `address`.
pub fn record_for(name: &str, address: std::net::IpAddr) -> DnsRecord {
    DnsRecord {
        kind: if address.is_ipv4() { "A" } else { "AAAA" }.into(),
        name: name.into(),
        value: address.to_string(),
    }
}

fn internal(e: anyhow::Error) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

/// What the platform answered when asked to serve the domains.
fn door_failed(e: anyhow::Error) -> ApiError {
    (StatusCode::BAD_GATEWAY, format!("{e:#}"))
}

fn served(e: ServeError) -> ApiError {
    match e {
        ServeError::Store(e) => internal(e),
        ServeError::Door(e) => door_failed(e),
    }
}

/// Run `body` while no other change to the domains runs, install-wide.
async fn one_at_a_time<T>(
    state: &DispatcherState,
    body: impl std::future::Future<Output = Result<T, ApiError>>,
) -> Result<T, ApiError> {
    let key = crate::lease::advisory_key(crate::lease::DOMAINS_DOMAIN, "install");
    weft_task_store::locks::with_lock_waiting(&state.lock_pool, key, "another change to the install's domains", || async { anyhow::Ok(body.await) })
        .await
        .map_err(internal)?
}

/// `GET /install/domains`. A person looking is a reason to ask after the
/// certificates still pending, so the answer says where each stands now.
pub async fn list(State(state): State<DispatcherState>, caller: CallerTenant) -> Result<Json<DomainList>, ApiError> {
    let stored = crate::domains::list_stored(&state.pg_pool).await.map_err(|e| crate::api::unavailable_or_internal("read the install's domains", e))?;
    // A certificate that cannot be read is said beside the rest: this is
    // the answer that explains what is wrong, so it never goes missing.
    let (stored, certificates_unread) = match crate::domains::check_certificates(&state.pg_pool, state.domains.as_ref(), stored.clone(), |_| true).await {
        Ok(checked) => (checked, None),
        Err(e) => (stored, Some(format!("{e:#}"))),
    };
    let door_refused = crate::domains::door_state(&state.pg_pool)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("read the domains' door", e))?
        .refused;
    let live_address = state.address.stable_fresh().await.map_err(internal)?;
    let callback_url = crate::api::access::callback_url(&live_address);
    let callback_services = sign_in_services(&state, &caller.0 .0).await?;
    let listed = |domains| DomainList {
        domains,
        door_refused: door_refused.clone(),
        live_address: live_address.clone(),
        callback_url: callback_url.clone(),
        callback_services: callback_services.clone(),
        certificates_unread: certificates_unread.clone(),
    };
    if stored.is_empty() {
        return Ok(Json(listed(Vec::new())));
    }
    let Some(address) = state.domains.address().await.map_err(door_failed)? else {
        let why = door_refused.clone().unwrap_or_else(|| "it is being made".to_string());
        return Err((
            StatusCode::CONFLICT,
            format!(
                "the install stores domains but the door in front of them is not up ({why}); the install keeps \
                 putting it up on its own, and `weft domain rm` drops the ones you no longer want"
            ),
        ));
    };
    let domains = stored
        .into_iter()
        .map(|s| DomainEntry { record: record_for(&s.domain.name, address), domain: s.domain, certificate: s.certificate })
        .collect();
    Ok(Json(listed(domains)))
}

/// The services with a sign-in on this install, by name: the ones
/// `tenant` connected through an app of its own, and the ones the apps
/// file holds a sign-in app for.
async fn sign_in_services(state: &DispatcherState, tenant: &str) -> Result<Vec<String>, ApiError> {
    let mut services = weft_access_store::own_sign_in_services(&state.pg_pool, tenant).await.map_err(crate::api::access_store_error)?;
    let shared: weft_core::access::wire::SignInServices =
        crate::broker_admin::forward_json(state, weft_broker_client::protocol::ADMIN_SIGN_IN_SERVICES, &serde_json::json!({})).await?;
    services.extend(shared.services);
    services.sort();
    services.dedup();
    Ok(services)
}

pub async fn add(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(AddDomain { mut domain, accept_cost }): Json<AddDomain>,
) -> Result<Json<DomainAdded>, ApiError> {
    domain.name = weft_core::install::normalize_domain_name(&domain.name).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    domain.validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if let Some(project) = domain.serves.project() {
        authorize_project(&state, &caller.0, project).await?;
    }
    let address = one_at_a_time(&state, async {
        // The first domain makes the door, which costs money while it
        // stands: only with the person's say-so.
        if let (Some(cost), false) = (state.domains.cost(), accept_cost) {
            if crate::domains::list(&state.pg_pool).await.map_err(internal)?.is_empty() {
                return Err((
                    StatusCode::PRECONDITION_FAILED,
                    format!(
                        "the install answers at its own address for free; a domain needs a door in front of it, \
                         which the first domain makes: {cost}. Add it again with --accept-cost to go ahead"
                    ),
                ));
            }
        }
        let new = match crate::domains::add(&state.pg_pool, &domain, crate::lease::now_unix()).await.map_err(internal)? {
            crate::domains::Added::New => true,
            crate::domains::Added::AlreadyStored => false,
            crate::domains::Added::TakenForSomethingElse => {
                return Err((
                    StatusCode::CONFLICT,
                    format!(
                        "'{}' is already one of this install's domains, for something else; remove it first to point it elsewhere",
                        domain.name
                    ),
                ))
            }
        };
        crate::domains::serve_added(&state.pg_pool, state.domains.as_ref(), &domain.name, new).await.map_err(served)
    })
    .await?;
    Ok(Json(DomainAdded { record: record_for(&domain.name, address), cost: state.domains.cost().map(str::to_string), domain }))
}

pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let name = weft_core::install::normalize_domain_name(&name).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    one_at_a_time(&state, async {
        let stored = crate::domains::list(&state.pg_pool).await.map_err(internal)?;
        let Some(domain) = stored.iter().find(|d| d.name == name) else {
            return Err((StatusCode::NOT_FOUND, format!("'{name}' is not one of this install's domains")));
        };
        if let Some(project) = domain.serves.project() {
            authorize_project(&state, &caller.0, project).await?;
        }
        crate::domains::unserve(&state.pg_pool, state.domains.as_ref(), &stored, &name).await.map_err(served)
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::install::{Domain, DomainServes};

    #[test]
    fn the_record_follows_the_address_family() {
        assert_eq!(
            record_for("app.example.com", "34.1.2.3".parse().unwrap()),
            DnsRecord { kind: "A".into(), name: "app.example.com".into(), value: "34.1.2.3".into() }
        );
        assert_eq!(record_for("app.example.com", "2600::1".parse().unwrap()).kind, "AAAA");
    }

    #[test]
    fn an_entry_carries_the_domain_and_its_record() {
        let entry = DomainEntry {
            domain: Domain { name: "weft.example.com".into(), serves: DomainServes::Install },
            record: record_for("weft.example.com", "34.1.2.3".parse().unwrap()),
            certificate: weft_core::install::Certificate::Pending { why: Some("the DNS record".into()) },
        };
        let v = serde_json::to_value(&entry).unwrap();
        assert_eq!(v["domain"]["name"], "weft.example.com");
        assert_eq!(v["domain"]["for"]["kind"], "install");
        assert_eq!(v["record"]["type"], "A");
        assert_eq!(v["certificate"], serde_json::json!({ "state": "pending", "why": "the DNS record" }));
        let back: DomainEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back, entry);
        let list = DomainList {
            domains: vec![entry],
            door_refused: None,
            live_address: "https://weft.example.com".into(),
            callback_url: "https://weft.example.com/access/oauth/callback".into(),
            callback_services: vec!["google".into()],
            certificates_unread: None,
        };
        let v = serde_json::to_value(&list).unwrap();
        assert_eq!(v["liveAddress"], "https://weft.example.com");
        assert_eq!(v["callbackUrl"], "https://weft.example.com/access/oauth/callback");
        assert_eq!(v["callbackServices"], serde_json::json!(["google"]));
        assert_eq!(serde_json::from_value::<DomainList>(v).unwrap(), list);
    }
}
