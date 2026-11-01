//! Every integration test of this crate, in one binary: each module was
//! once a test file of its own, and one binary links the dependency tree
//! once instead of once per file. nextest still runs each test in its own
//! process.

mod broken_parts;
mod computer_includes;
mod enrich_tests;
mod fires_with;
mod gated_fire;
mod highlighting_vocabulary;
mod include_cuts;
mod loop_cuts;
mod metadata_ports;
mod infra_image_enum;
mod node_test_sweep;
mod parser_multiline_object;
mod parser_tests;
mod passthrough_include;
mod per_instance;
mod rig;
mod start_feeders;
mod test_crate_emit;
mod type_rules_tests;
mod validate_tests;
mod worker_isolation;
