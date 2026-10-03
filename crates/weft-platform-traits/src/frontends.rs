//! Hosting a project's frontend on the install's own cloud: the service
//! it runs as, and its repository's right to deploy there.
//!
//! The install makes the service, empty, and its repository's CI deploys
//! the frontend onto it. A platform that hosts no frontend refuses, naming
//! the way that works there: running it anywhere and calling the install
//! with its token.

use async_trait::async_trait;

/// A frontend the install hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendSite {
    pub project: uuid::Uuid,
    pub name: String,
    /// The GitHub repository whose CI deploys it.
    pub repo: weft_core::frontend::Repository,
}

/// Where a hosted frontend runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedFrontend {
    /// The service its repository deploys to.
    pub service: String,
    /// Where visitors reach it.
    pub url: String,
}

#[async_trait]
pub trait FrontendHosting: Send + Sync {
    /// Make the service `site` runs as, and let its repository deploy to
    /// it (and nothing else). Making one that exists makes nothing new.
    async fn open(&self, site: &FrontendSite) -> anyhow::Result<HostedFrontend>;

    /// Remove the service, and the repository's right to deploy unless
    /// `keep_repo` (another frontend still deploys from it). Removing one
    /// that is gone, or a service never made, is not an error: it also
    /// takes back what a failed `open` made.
    async fn close(&self, site: &FrontendSite, keep_repo: bool) -> anyhow::Result<()>;
}

/// Records every call and hosts every frontend at a made-up address.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum FrontendCall {
        Open(FrontendSite),
        Close { site: FrontendSite, keep_repo: bool },
    }

    #[derive(Default)]
    pub struct FakeFrontendHosting {
        calls: parking_lot::Mutex<Vec<FrontendCall>>,
        /// Refuse every open with this, when set.
        refuse: parking_lot::Mutex<Option<String>>,
    }

    impl FakeFrontendHosting {
        pub fn calls(&self) -> Vec<FrontendCall> {
            self.calls.lock().clone()
        }

        pub fn refuse_opens(&self, why: &str) {
            *self.refuse.lock() = Some(why.to_string());
        }
    }

    #[async_trait]
    impl FrontendHosting for FakeFrontendHosting {
        async fn open(&self, site: &FrontendSite) -> anyhow::Result<HostedFrontend> {
            self.calls.lock().push(FrontendCall::Open(site.clone()));
            if let Some(why) = self.refuse.lock().clone() {
                anyhow::bail!(why);
            }
            Ok(HostedFrontend { service: format!("front-{}", site.name), url: format!("https://{}.example", site.name) })
        }

        async fn close(&self, site: &FrontendSite, keep_repo: bool) -> anyhow::Result<()> {
            self.calls.lock().push(FrontendCall::Close { site: site.clone(), keep_repo });
            Ok(())
        }
    }
}
