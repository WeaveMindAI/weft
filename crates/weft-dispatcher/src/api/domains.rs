//! `GET/POST /install/domains`, `DELETE /install/domains/{name}`: the
//! domains the install answers at (`crate::domains`).
//!
//! Adding one stores it and answers the DNS record the person has to set;
//! the front door gets its certificate once the name points at the
//! machine. An install with no front door of its own (a local one, which
//! the internet reaches through its tunnel) has nothing to point a domain
//! at, so it refuses.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use weft_core::install::{Domain, DomainServes};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// The DNS record that points a domain at the install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsRecord {
    /// `A` for an IPv4 address, `AAAA` for an IPv6 one.
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DomainEntry {
    pub domain: Domain,
    pub record: DnsRecord,
}

/// The record pointing `name` at `address`.
pub fn record_for(name: &str, address: std::net::IpAddr) -> DnsRecord {
    DnsRecord {
        kind: if address.is_ipv4() { "A" } else { "AAAA" }.into(),
        name: name.into(),
        value: address.to_string(),
    }
}

fn address(state: &DispatcherState) -> Result<std::net::IpAddr, ApiError> {
    state.install_info.address.ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "this install has no front door of its own to point a domain at (a local install is \
             reached through its tunnel); domains are for a cloud install"
                .to_string(),
        )
    })
}

fn internal(e: anyhow::Error) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

pub async fn list(State(state): State<DispatcherState>, _caller: CallerTenant) -> Result<Json<Vec<DomainEntry>>, ApiError> {
    let address = address(&state)?;
    let domains = crate::domains::list(&state.pg_pool).await.map_err(internal)?;
    Ok(Json(
        domains.into_iter().map(|domain| DomainEntry { record: record_for(&domain.name, address), domain }).collect(),
    ))
}

pub async fn add(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(mut domain): Json<Domain>,
) -> Result<Json<DomainEntry>, ApiError> {
    let address = address(&state)?;
    domain.name = weft_core::install::normalize_domain_name(&domain.name).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    domain.validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    match &domain.serves {
        DomainServes::Install => {}
        DomainServes::Frontend { project, .. } | DomainServes::Api { project } => {
            authorize_project(&state, &caller.0, *project).await?;
        }
    }
    crate::domains::add(&state.pg_pool, &domain, crate::lease::now_unix())
        .await
        .map_err(|e| (StatusCode::CONFLICT, format!("{e:#}")))?;
    Ok(Json(DomainEntry { record: record_for(&domain.name, address), domain }))
}

pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let name = weft_core::install::normalize_domain_name(&name).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let stored = crate::domains::list(&state.pg_pool).await.map_err(internal)?;
    let Some(domain) = stored.into_iter().find(|d| d.name == name) else {
        return Err((StatusCode::NOT_FOUND, format!("'{name}' is not one of this install's domains")));
    };
    if let DomainServes::Frontend { project, .. } | DomainServes::Api { project } = domain.serves {
        authorize_project(&state, &caller.0, project).await?;
    }
    crate::domains::remove(&state.pg_pool, &name).await.map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let v = serde_json::to_value(&entry).unwrap();
        assert_eq!(v["domain"]["name"], "weft.example.com");
        assert_eq!(v["domain"]["for"]["kind"], "install");
        assert_eq!(v["record"]["type"], "A");
        let back: DomainEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back.domain, entry.domain);
    }
}
