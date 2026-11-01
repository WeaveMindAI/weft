//! Every e2e test, in one binary: each module was once a test file of its
//! own, and one binary links the dependency tree once instead of once per
//! file. A test is named `<file>::<test>` after the module it is in, which
//! is how `scripts/run-e2e.sh` names and selects it.

mod api_auth;
mod api_reply;
mod api_stream;
mod bus;
mod common;
mod computer;
mod container_link;
mod custom_node;
mod email;
mod event_triggers;
mod fixtures_parse;
mod gcp_install;
mod infra;
mod infra_display;
mod infra_feed;
mod instances;
mod left_out_package;
mod lifecycle_transitions;
mod live_chat;
mod llm_stream_tool;
mod loops;
mod maintenance;
mod metering_edges;
mod node_crash;
mod openrouter;
mod outside_door;
mod postgres;
mod resume;
mod runtime_restart;
mod standard_worker;
mod steer_by_tag;
mod storage_multipart;
mod stored_links;
mod version_tree;
mod working_copies;
