//! A local install hosts no frontend: the machine it runs on is yours, so
//! a frontend runs there however you run it, and calls the install with
//! its token (`weft frontend add <name>`, without `--repo`).

use async_trait::async_trait;
use weft_platform_traits::{FrontendHosting, FrontendSite, HostedFrontend};

pub struct NoFrontendHosting;

#[async_trait]
impl FrontendHosting for NoFrontendHosting {
    async fn open(&self, site: &FrontendSite) -> anyhow::Result<HostedFrontend> {
        anyhow::bail!(
            "a local install hosts no frontend: run '{}' yourself (it is on your machine already) and add it without \
             `--repo`, which gives it its token",
            site.name
        )
    }

    /// Nothing was ever opened here.
    async fn close(&self, _site: &FrontendSite, _keep_repo: bool) -> anyhow::Result<()> {
        Ok(())
    }
}
