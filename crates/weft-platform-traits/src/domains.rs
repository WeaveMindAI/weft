//! Serving the install's own domains (`weft domain add`): the HTTPS door
//! each domain's DNS record points at.
//!
//! An install answers at its platform's own address with no domain at all
//! (a Cloud Run service's `run.app` name). A domain needs something in
//! front that holds a certificate for it and passes its requests on: a
//! project's API domain straight to the project's own workers, every other
//! one to the install, which routes it by name (the install itself, a
//! project's frontend). That something can cost money while it stands, so
//! it stands only while the install has a domain.

use async_trait::async_trait;

#[async_trait]
pub trait DomainHosting: Send + Sync {
    /// Serve exactly `domains` (names lower case), each with its own
    /// certificate: the door is made when the first one comes and taken
    /// down when the last one goes. Answers the address every name's DNS
    /// record points at, `None` once there is no door. Asking twice for
    /// the same domains changes nothing.
    async fn serve(&self, domains: &[weft_core::install::Domain]) -> anyhow::Result<Option<std::net::IpAddr>>;

    /// The door's address while it stands.
    async fn address(&self) -> anyhow::Result<Option<std::net::IpAddr>>;

    /// What the door costs while it stands, in words a person can weigh
    /// before adding the first domain; `None` when it costs nothing.
    fn cost(&self) -> Option<&'static str>;
}

/// Records every call and puts every door at one made-up address.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    #[derive(Default)]
    pub struct FakeDomainHosting {
        pub served: parking_lot::Mutex<Vec<Vec<String>>>,
    }

    pub const FAKE_DOOR: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(34, 1, 2, 3));

    #[async_trait]
    impl DomainHosting for FakeDomainHosting {
        async fn serve(&self, domains: &[weft_core::install::Domain]) -> anyhow::Result<Option<std::net::IpAddr>> {
            self.served.lock().push(domains.iter().map(|d| d.name.clone()).collect());
            Ok((!domains.is_empty()).then_some(FAKE_DOOR))
        }

        async fn address(&self) -> anyhow::Result<Option<std::net::IpAddr>> {
            Ok(self.served.lock().last().is_some_and(|n| !n.is_empty()).then_some(FAKE_DOOR))
        }

        fn cost(&self) -> Option<&'static str> {
            Some("a made-up price")
        }
    }
}
