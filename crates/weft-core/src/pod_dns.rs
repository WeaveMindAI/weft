//! The DNS settings every pod weft creates carries.
//!
//! A cluster's default resolver config says `ndots:5`: any name with
//! fewer than five dots is tried against every search suffix before it
//! is tried as written. `x.ns.svc.cluster.local` has four, so each
//! lookup first walks `<ns>.svc.cluster.local`, `svc.cluster.local`,
//! `cluster.local` and whatever the node adds (on kind, the host's own
//! domain, which CoreDNS forwards to the host's resolver). That upstream
//! leg alone was measured at 70-95 ms per lookup and stalled for seconds
//! under load. `ndots:1` makes any dotted name go out as written first;
//! the search list is kept, so a bare Service name still resolves, and a
//! dotted name that fails as written still falls back to it.
//!
//! One source for both shapes a pod spec is written in here: the JSON
//! the infra compiler emits and the YAML templates, which embed the same
//! JSON (JSON is valid YAML). The system manifests under `deploy/k8s/`
//! carry the same line by hand; a test below holds them to it.

use serde_json::{json, Value};

/// The pod spec's `dnsConfig` value.
pub fn pod_dns_config() -> Value {
    json!({ "options": [ { "name": "ndots", "value": "1" } ] })
}

/// The `dnsConfig:` line for a YAML pod spec, without indentation or a
/// trailing newline: the caller places it at the pod spec's level.
pub fn pod_dns_config_yaml() -> String {
    format!("dnsConfig: {}", pod_dns_config())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndots_one_and_no_search_override() {
        let v = pod_dns_config();
        assert_eq!(v["options"], json!([{ "name": "ndots", "value": "1" }]));
        assert!(v.get("searches").is_none(), "the search list is kept");
        assert!(v.get("nameservers").is_none());
    }

    #[test]
    fn yaml_line_embeds_the_json() {
        assert_eq!(
            pod_dns_config_yaml(),
            r#"dnsConfig: {"options":[{"name":"ndots","value":"1"}]}"#
        );
    }

    /// Every system manifest with a pod template carries the line, once
    /// per pod template.
    #[test]
    fn system_manifests_carry_it() {
        let line = pod_dns_config_yaml();
        let manifests = [
            ("broker.yaml", include_str!("../../../deploy/k8s/broker.yaml")),
            ("dispatcher.yaml", include_str!("../../../deploy/k8s/dispatcher.yaml")),
            ("postgres.yaml", include_str!("../../../deploy/k8s/postgres.yaml")),
            ("public-tunnel.yaml", include_str!("../../../deploy/k8s/public-tunnel.yaml")),
        ];
        // The gateway's proxy pods get it through a Deployment patch on
        // the EnvoyProxy, which has no `containers:` of its own.
        let gateway = include_str!("../../../deploy/k8s/gateway.yaml");
        assert_eq!(gateway.matches(&line).count(), 1, "gateway.yaml patches the proxy pods");
        for (name, text) in manifests {
            let templates = text.matches("containers:").count();
            assert!(templates > 0, "{name} has a pod template");
            assert_eq!(
                text.matches(&line).count(),
                templates,
                "{name}: every pod template sets `{line}`"
            );
        }
    }
}
