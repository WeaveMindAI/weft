//! The tests that need a real Postgres. They are all in this one binary
//! so `.config/nextest.toml` can tell them apart by its name: the default
//! profile leaves out every binary called `db`, the `db` profile runs only
//! them (`scripts/run-db-tests.sh` runs it against a throwaway Postgres).
//! Each module was once a test file of its own; one binary links the
//! dependency tree once instead of once per file.

mod db_build;
mod db_costs;
mod db_entry_limits;
mod db_install_state;
mod db_lifecycle;
mod db_maintenance;
mod db_project_ports;
mod db_proxy;
mod db_tags;
mod db_tenant_isolation;
mod db_versions;

/// The events of rows a store read, without their places.
pub(crate) fn events(rows: Vec<weft_journal::JournalRow>) -> Vec<weft_journal::RunEvent> {
    rows.into_iter().map(|row| row.event).collect()
}

/// Every event of `run_id`'s record, read a page at a time.
pub(crate) async fn recorded(db: &impl weft_task_store::db::Client, run_id: weft_core::RunId) -> Vec<weft_journal::RunEvent> {
    let mut events = Vec::new();
    let mut after = None;
    loop {
        let page = weft_journal::record::read_page(db, run_id, after).await.unwrap();
        let Some(last) = page.rows.last().map(|row| row.seq) else { return events };
        events.extend(page.events(run_id).unwrap());
        after = Some(last);
    }
}
