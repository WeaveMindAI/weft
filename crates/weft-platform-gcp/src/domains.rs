//! The door in front of the install's domains: a global external HTTPS
//! load balancer with a Google-managed certificate per domain (Certificate
//! Manager, one map entry per name). A project's API domain is sent
//! straight to the project's own Cloud Run service (a serverless endpoint
//! group and backend of its own, picked by host in the URL map), so its
//! callers reach its workers with nothing of weft's in between; every
//! other request goes to the dispatcher's service.
//!
//! The install answers at its service's own `run.app` address without
//! any of this. A domain needs a load balancer, which Google bills by the
//! hour while its forwarding rule exists, so the door is made with the
//! first domain and taken down with the last. Port 80 is a second rule on
//! the same address that sends every request to HTTPS (both are within
//! the price of the first five rules).
//!
//! A certificate is issued once the name's DNS record points at the
//! door's address (the load balancer answers the authority's challenge),
//! which takes minutes after the record is in place.

use async_trait::async_trait;
use serde_json::{json, Value};
use sha2::Digest as _;
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::DomainHosting;

use crate::api::{is_status, Google};

const COMPUTE: &str = "https://compute.googleapis.com/compute/v1";
const CERTIFICATES: &str = "https://certificatemanager.googleapis.com/v1";

pub struct LoadBalancerDomains {
    google: Google,
    gcp: GcpPlatform,
}

impl LoadBalancerDomains {
    pub fn new(google: Google, gcp: GcpPlatform) -> Self {
        Self { google, gcp }
    }

    /// The name every part of the door goes by.
    fn door(&self) -> String {
        format!("{}-door", self.gcp.name)
    }

    fn global(&self, collection: &str) -> String {
        format!("{COMPUTE}/projects/{}/global/{collection}", self.gcp.project)
    }

    fn regional(&self, collection: &str) -> String {
        format!("{COMPUTE}/projects/{}/regions/{}/{collection}", self.gcp.project, self.gcp.region)
    }

    /// How one resource refers to another.
    fn link(&self, url: &str) -> String {
        url.trim_start_matches(COMPUTE).trim_start_matches('/').to_string()
    }

    fn certificates(&self) -> String {
        format!("{CERTIFICATES}/projects/{}/locations/global", self.gcp.project)
    }

    fn certificate_map(&self) -> String {
        format!("projects/{}/locations/global/certificateMaps/{}", self.gcp.project, self.door())
    }

    /// Make a resource and wait until it is: `false` when one of its name
    /// was there already.
    async fn make(&self, collection_url: &str, body: &Value, operations: &str) -> anyhow::Result<bool> {
        match self.google.post(collection_url, body).await {
            Ok(op) => {
                self.google.wait(operations, op).await?;
                Ok(true)
            }
            Err(e) if is_status(&e, 409) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Make a resource unless it is there already. One already there is
    /// taken as it stands: it is named after this install's door.
    async fn ensure(&self, collection_url: &str, body: Value, operations: &str) -> anyhow::Result<()> {
        self.make(collection_url, &body, operations).await.map(drop)
    }

    /// [`Self::ensure`] for a part whose content decides where the door's
    /// requests go: one already there, read at `url`, must pass `check`.
    async fn ensure_matching(
        &self,
        collection_url: &str,
        url: &str,
        body: Value,
        operations: &str,
        check: impl FnOnce(&Value) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if self.make(collection_url, &body, operations).await? {
            return Ok(());
        }
        check(&self.google.get(url).await?)
    }

    /// Remove a resource if it is there, and wait until it is gone.
    async fn remove(&self, url: &str, operations: &str) -> anyhow::Result<()> {
        if let Some(op) = self.google.delete(url).await? {
            self.google.wait(operations, op).await?;
        }
        Ok(())
    }

    /// The door's parts, made in the order each needs the one before.
    async fn build(&self) -> anyhow::Result<()> {
        let door = self.door();
        let redirect = format!("{door}-redirect");
        self.ensure(&self.global("addresses"), json!({ "name": door }), COMPUTE).await?;
        self.ensure_matching(
            &self.regional("networkEndpointGroups"),
            &format!("{}/{door}", self.regional("networkEndpointGroups")),
            json!({
                "name": door,
                "networkEndpointType": "SERVERLESS",
                "cloudRun": { "service": self.gcp.dispatcher_service },
            }),
            COMPUTE,
            |existing| sends_to(existing, &door, &self.gcp.region, &self.gcp.dispatcher_service),
        )
        .await?;
        self.ensure(
            &self.global("backendServices"),
            json!({
                "name": door,
                "loadBalancingScheme": "EXTERNAL_MANAGED",
                "protocol": "HTTPS",
                "backends": [{ "group": self.link(&format!("{}/{door}", self.regional("networkEndpointGroups"))) }],
            }),
            COMPUTE,
        )
        .await?;
        self.ensure(
            &self.global("urlMaps"),
            json!({ "name": door, "defaultService": self.link(&format!("{}/{door}", self.global("backendServices"))) }),
            COMPUTE,
        )
        .await?;
        self.ensure(
            &self.global("urlMaps"),
            json!({
                "name": redirect,
                "defaultUrlRedirect": { "httpsRedirect": true, "redirectResponseCode": "MOVED_PERMANENTLY_DEFAULT", "stripQuery": false },
            }),
            COMPUTE,
        )
        .await?;
        self.ensure(&format!("{}/certificateMaps?certificateMapId={door}", self.certificates()), json!({}), CERTIFICATES).await?;
        self.ensure(
            &self.global("targetHttpsProxies"),
            json!({
                "name": door,
                "urlMap": self.link(&format!("{}/{door}", self.global("urlMaps"))),
                "certificateMap": format!("//certificatemanager.googleapis.com/{}", self.certificate_map()),
            }),
            COMPUTE,
        )
        .await?;
        self.ensure(
            &self.global("targetHttpProxies"),
            json!({ "name": redirect, "urlMap": self.link(&format!("{}/{redirect}", self.global("urlMaps"))) }),
            COMPUTE,
        )
        .await?;
        let address = self.link(&format!("{}/{door}", self.global("addresses")));
        for (rule, port, target) in [
            (format!("{door}-https"), "443", format!("{}/{door}", self.global("targetHttpsProxies"))),
            (format!("{door}-http"), "80", format!("{}/{redirect}", self.global("targetHttpProxies"))),
        ] {
            self.ensure(
                &self.global("forwardingRules"),
                json!({
                    "name": rule,
                    "IPAddress": address,
                    "IPProtocol": "TCP",
                    "portRange": port,
                    "target": self.link(&target),
                    "loadBalancingScheme": "EXTERNAL_MANAGED",
                }),
                COMPUTE,
            )
            .await?;
        }
        Ok(())
    }

    /// The backend (and its endpoint group) that sends a project's API
    /// domains to the project's own service.
    fn project_backend(&self, project: uuid::Uuid) -> String {
        format!("{}-p-{}", self.door(), &project.simple().to_string()[..12])
    }

    /// The project backends the door holds now, by name.
    async fn project_backends(&self) -> anyhow::Result<Vec<String>> {
        let prefix = format!("{}-p-", self.door());
        let listed = self.google.get(&self.global("backendServices")).await?;
        Ok(listed
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|b| b.get("name").and_then(Value::as_str))
            .filter(|name| name.starts_with(&prefix))
            .map(str::to_string)
            .collect())
    }

    /// Send each project's API domains (`api`, project to names) to the
    /// project's own service, and everything else to the dispatcher: one
    /// backend per project, picked by host in the URL map, and the
    /// backends of projects with no API domain left taken away.
    async fn route_apis(&self, api: &std::collections::BTreeMap<uuid::Uuid, Vec<String>>) -> anyhow::Result<()> {
        let door = self.door();
        for project in api.keys() {
            let name = self.project_backend(*project);
            let service = crate::names::worker_service(*project);
            self.ensure_matching(
                &self.regional("networkEndpointGroups"),
                &format!("{}/{name}", self.regional("networkEndpointGroups")),
                json!({ "name": name, "networkEndpointType": "SERVERLESS", "cloudRun": { "service": service } }),
                COMPUTE,
                |existing| sends_to(existing, &name, &self.gcp.region, &service),
            )
            .await?;
            self.ensure(
                &self.global("backendServices"),
                json!({
                    "name": name,
                    "loadBalancingScheme": "EXTERNAL_MANAGED",
                    "protocol": "HTTPS",
                    "backends": [{ "group": self.link(&format!("{}/{name}", self.regional("networkEndpointGroups"))) }],
                }),
                COMPUTE,
            )
            .await?;
        }
        let url_map = format!("{}/{door}", self.global("urlMaps"));
        let found = self.google.get(&url_map).await?;
        let (host_rules, path_matchers) = url_map_rules(api, |project| self.link(&format!("{}/{}", self.global("backendServices"), self.project_backend(project))));
        let unchanged = found.get("hostRules").cloned().unwrap_or(json!([])) == host_rules
            && found.get("pathMatchers").cloned().unwrap_or(json!([])) == path_matchers;
        if !unchanged {
            let fingerprint = found.get("fingerprint").cloned().unwrap_or(Value::Null);
            let op = self
                .google
                .patch(&url_map, &json!({ "hostRules": host_rules, "pathMatchers": path_matchers, "fingerprint": fingerprint }))
                .await?;
            self.google.wait(COMPUTE, op).await?;
        }
        for name in self.project_backends().await? {
            if !api.keys().any(|project| self.project_backend(*project) == name) {
                self.remove(&format!("{}/{name}", self.global("backendServices")), COMPUTE).await?;
                self.remove(&format!("{}/{name}", self.regional("networkEndpointGroups")), COMPUTE).await?;
            }
        }
        Ok(())
    }

    /// Every part of the door, gone, in the order nothing still uses
    /// what is removed.
    async fn take_down(&self) -> anyhow::Result<()> {
        let door = self.door();
        let redirect = format!("{door}-redirect");
        self.remove(&format!("{}/{door}-https", self.global("forwardingRules")), COMPUTE).await?;
        self.remove(&format!("{}/{door}-http", self.global("forwardingRules")), COMPUTE).await?;
        self.remove(&format!("{}/{door}", self.global("targetHttpsProxies")), COMPUTE).await?;
        self.remove(&format!("{}/{redirect}", self.global("targetHttpProxies")), COMPUTE).await?;
        self.remove(&format!("{}/{door}", self.global("urlMaps")), COMPUTE).await?;
        self.remove(&format!("{}/{redirect}", self.global("urlMaps")), COMPUTE).await?;
        self.remove(&format!("{}/{door}", self.global("backendServices")), COMPUTE).await?;
        self.remove(&format!("{}/{door}", self.regional("networkEndpointGroups")), COMPUTE).await?;
        for name in self.project_backends().await? {
            self.remove(&format!("{}/{name}", self.global("backendServices")), COMPUTE).await?;
            self.remove(&format!("{}/{name}", self.regional("networkEndpointGroups")), COMPUTE).await?;
        }
        for (entry, _) in self.entries().await? {
            self.unserve(&entry).await?;
        }
        self.remove(&format!("{CERTIFICATES}/{}", self.certificate_map()), CERTIFICATES).await?;
        self.remove(&format!("{}/{door}", self.global("addresses")), COMPUTE).await?;
        Ok(())
    }

    /// The names the door holds a certificate for now: each map entry's
    /// id and hostname.
    async fn entries(&self) -> anyhow::Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut query = vec![("pageSize", "500".to_string())];
            if let Some(token) = &page {
                query.push(("pageToken", token.clone()));
            }
            let url = format!("{CERTIFICATES}/{}/certificateMapEntries", self.certificate_map());
            let listed = match self.google.get_query(&url, &query).await {
                Ok(listed) => listed,
                // No map: no names.
                Err(e) if is_status(&e, 404) => return Ok(out),
                Err(e) => return Err(e),
            };
            for entry in listed.get("certificateMapEntries").and_then(Value::as_array).into_iter().flatten() {
                let id = entry.get("name").and_then(Value::as_str).and_then(|n| n.rsplit('/').next()).unwrap_or_default();
                let host = entry.get("hostname").and_then(Value::as_str).unwrap_or_default();
                out.push((id.to_string(), host.to_string()));
            }
            page = listed.get("nextPageToken").and_then(Value::as_str).filter(|t| !t.is_empty()).map(str::to_string);
            if page.is_none() {
                return Ok(out);
            }
        }
    }

    /// Hold a certificate for `name`, and answer it under it.
    async fn serve_name(&self, name: &str) -> anyhow::Result<()> {
        let id = certificate_id(&self.door(), name);
        self.ensure(
            &format!("{}/certificates?certificateId={id}", self.certificates()),
            json!({ "managed": { "domains": [name] } }),
            CERTIFICATES,
        )
        .await
        .map_err(|e| e.context(format!("ask for {name}'s certificate")))?;
        self.ensure(
            &format!("{CERTIFICATES}/{}/certificateMapEntries?certificateMapEntryId={id}", self.certificate_map()),
            json!({
                "hostname": name,
                "certificates": [format!("projects/{}/locations/global/certificates/{id}", self.gcp.project)],
            }),
            CERTIFICATES,
        )
        .await
        .map_err(|e| e.context(format!("answer {name} with its certificate")))
    }

    /// Stop answering the name whose entry (and certificate) is `id`.
    async fn unserve(&self, id: &str) -> anyhow::Result<()> {
        self.remove(&format!("{CERTIFICATES}/{}/certificateMapEntries/{id}", self.certificate_map()), CERTIFICATES).await?;
        self.remove(&format!("{}/certificates/{id}", self.certificates()), CERTIFICATES).await
    }
}

/// The id a name's certificate and map entry go by: the door's name and a
/// digest of the domain, which fits Certificate Manager's ids (lower-case
/// letters, digits and hyphens) whatever the domain is.
fn certificate_id(door: &str, name: &str) -> String {
    let digest: String = sha2::Sha256::digest(name.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    format!("{door}-{}", &digest[..16])
}

/// The URL map's host rules and path matchers that send each project's
/// API domains (`api`) to its backend (`backend` names it): one matcher per
/// project, everything else left to the map's default (the dispatcher).
fn url_map_rules(api: &std::collections::BTreeMap<uuid::Uuid, Vec<String>>, backend: impl Fn(uuid::Uuid) -> String) -> (Value, Value) {
    let matcher = |project: &uuid::Uuid| format!("p-{}", &project.simple().to_string()[..12]);
    let host_rules = api.iter().map(|(project, names)| json!({ "hosts": names, "pathMatcher": matcher(project) })).collect::<Vec<_>>();
    let path_matchers = api.keys().map(|project| json!({ "name": matcher(project), "defaultService": backend(*project) })).collect::<Vec<_>>();
    (Value::Array(host_rules), Value::Array(path_matchers))
}

/// Whether the endpoint group `name`, as Google answers it, sends the
/// door's requests to the Cloud Run service `service`. One that sends
/// them anywhere else is refused, naming both: taking it would put the
/// install's domains in front of another service.
fn sends_to(group: &Value, name: &str, region: &str, service: &str) -> anyhow::Result<()> {
    let found = group.get("cloudRun").and_then(|c| c.get("service")).and_then(Value::as_str);
    anyhow::ensure!(
        found == Some(service),
        "the network endpoint group '{name}' is already there but sends its requests to {}, not to the Cloud Run \
         service '{service}'; delete it (`gcloud compute network-endpoint-groups delete {name} --region {region}`) and add \
         the domain again",
        found.map_or_else(|| "no Cloud Run service".to_string(), |s| format!("the Cloud Run service '{s}'")),
    );
    Ok(())
}

#[async_trait]
impl DomainHosting for LoadBalancerDomains {
    async fn serve(&self, domains: &[weft_core::install::Domain]) -> anyhow::Result<Option<std::net::IpAddr>> {
        if domains.is_empty() {
            self.take_down().await.map_err(|e| e.context("take down the door in front of the install's domains"))?;
            return Ok(None);
        }
        self.build().await.map_err(|e| e.context("make the door in front of the install's domains"))?;
        let mut api: std::collections::BTreeMap<uuid::Uuid, Vec<String>> = Default::default();
        for domain in domains {
            if let weft_core::install::DomainServes::Api { project } = &domain.serves {
                api.entry(*project).or_default().push(domain.name.clone());
            }
        }
        self.route_apis(&api).await.map_err(|e| e.context("send the projects' API domains to their services"))?;
        let names: Vec<String> = domains.iter().map(|d| d.name.clone()).collect();
        let wanted: Vec<String> = names.iter().map(|n| certificate_id(&self.door(), n)).collect();
        for (id, _) in self.entries().await? {
            if !wanted.contains(&id) {
                self.unserve(&id).await?;
            }
        }
        for name in &names {
            self.serve_name(name).await?;
        }
        self.address().await?.ok_or_else(|| anyhow::anyhow!("the door was made but its address is not there"))
            .map(Some)
    }

    async fn address(&self) -> anyhow::Result<Option<std::net::IpAddr>> {
        let Some(address) = self.google.get_opt(&format!("{}/{}", self.global("addresses"), self.door())).await? else {
            return Ok(None);
        };
        let ip = address
            .get("address")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("the door's address names no IP"))?;
        Ok(Some(ip.parse().map_err(|e| anyhow::anyhow!("the door's address '{ip}' is not an IP: {e}"))?))
    }

    fn cost(&self) -> Option<&'static str> {
        Some("a Google Cloud load balancer, about $18 a month while any domain is stored, plus $0.008 per GB through it")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_certificate_id_fits_and_is_the_names_own() {
        let id = certificate_id("weft-door", "api.shop.example.com");
        assert!(id.len() <= 63);
        assert!(id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'));
        assert_eq!(id, certificate_id("weft-door", "api.shop.example.com"));
        assert_ne!(id, certificate_id("weft-door", "app.shop.example.com"));
    }

    #[test]
    fn a_projects_api_domains_go_to_its_own_backend() {
        let (one, two) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let mut api = std::collections::BTreeMap::new();
        api.insert(one, vec!["api.a.example".to_string(), "api2.a.example".to_string()]);
        api.insert(two, vec!["api.b.example".to_string()]);
        let (hosts, matchers) = url_map_rules(&api, |p| format!("backend-{}", p.as_u128()));
        assert_eq!(hosts[0]["hosts"], json!(["api.a.example", "api2.a.example"]));
        assert_eq!(hosts[0]["pathMatcher"], matchers[0]["name"]);
        assert_eq!(matchers[0]["defaultService"], "backend-1");
        assert_eq!(matchers[1]["defaultService"], "backend-2");
        let (none_hosts, none_matchers) = url_map_rules(&Default::default(), |_| String::new());
        assert_eq!((none_hosts, none_matchers), (json!([]), json!([])), "with no API domain, everything goes to the dispatcher");
    }

    #[test]
    fn an_endpoint_group_already_there_must_send_to_the_dispatcher() {
        let ours = json!({ "name": "weft-door", "cloudRun": { "service": "weft-role-dispatcher" } });
        sends_to(&ours, "weft-door", "us-central1", "weft-role-dispatcher").unwrap();
        let other = json!({ "name": "weft-door", "cloudRun": { "service": "someone-else" } });
        let refused = sends_to(&other, "weft-door", "us-central1", "weft-role-dispatcher").unwrap_err().to_string();
        assert!(refused.contains("'someone-else'") && refused.contains("'weft-role-dispatcher'"), "{refused}");
        let bare = json!({ "name": "weft-door" });
        let refused = sends_to(&bare, "weft-door", "us-central1", "weft-role-dispatcher").unwrap_err().to_string();
        assert!(refused.contains("no Cloud Run service"), "{refused}");
    }
}
