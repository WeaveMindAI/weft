//! A local install's one process holds every held connection itself, so
//! there is no pool to size.

use async_trait::async_trait;
use weft_platform_traits::HolderPool;

pub struct OneProcessHolds;

#[async_trait]
impl HolderPool for OneProcessHolds {
    async fn resize(&self, _copies: u32) -> anyhow::Result<()> {
        Ok(())
    }
}
