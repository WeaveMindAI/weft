//! The tests that need a real Postgres. They are all in this one binary
//! so `.config/nextest.toml` can tell them apart by its name: the default
//! profile leaves out every binary called `db`, the `db` profile runs only
//! them (`scripts/run-db-tests.sh` runs it against a throwaway Postgres).
//! Each module was once a test file of its own; one binary links the
//! dependency tree once instead of once per file.

mod claims;
mod documents;
mod leases;
mod news_outbox;
mod nul;
mod schema_guard;
mod signals;
mod support;
mod terminal_wait;
mod throttle;
mod unanswered;
mod worker_door;
