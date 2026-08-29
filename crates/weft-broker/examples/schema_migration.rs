//! Write the migration for a change to one of the broker's tables.
//! Driven by `./setup.sh --migration <name>`, which provides the
//! throwaway Postgres this builds schemas in.
//!
//! What the generator does, what a draft is, and what releasing does is
//! documented on `weft_task_store::schema_guard::write_migration`, the
//! shared body every crate's copy of this example calls.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_task_store::schema_guard::write_migration(&[&weft_broker::runtime_store::GROUP]).await
}
