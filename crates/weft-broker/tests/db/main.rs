//! The tests that need a real Postgres. They are all in this one binary
//! so `.config/nextest.toml` can tell them apart by its name: the default
//! profile leaves out every binary called `db`, the `db` profile runs only
//! them (`scripts/run-db-tests.sh` runs it against a throwaway Postgres).
//! Each module was once a test file of its own; one binary links the
//! dependency tree once instead of once per file.

mod caller_auth;
mod door_let_go;
mod held_signals;
mod held_stop;
mod install_cli;
mod lifecycle_writes;
mod old_shapes;
mod program_tokens;
mod record_batch;
mod run_claim;
mod runtime_store;
