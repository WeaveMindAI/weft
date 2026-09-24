//! `weft-e2e-clean`: remove everything e2e runs kept for inspection
//! (`weft_e2e::cleanup`). Run it through `scripts/run-e2e.sh --clean`, which
//! refuses while a suite is running.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_e2e::cleanup::clean_kept().await
}
