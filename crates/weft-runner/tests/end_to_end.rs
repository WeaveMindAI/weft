//! End-to-end test: compile a simple project, enrich it, run the
//! pulse loop in-process, assert completion.
//!
//! This is the scaffold smoke test for the whole v2 core.

use std::sync::Arc;

use tokio::sync::Notify;

use weft_compiler::enrich::enrich;
use weft_compiler::weft_compiler::compile;
use weft_runner::EntryMode;
use weft_stdlib::StdlibCatalog;

#[tokio::test]
async fn text_then_debug_completes() {
    let source = r#"
# Project: Smoke

greeting = Text { value: "hello world" }
sink = Debug

sink.data = greeting.value
"#;

    let mut project = compile(source, uuid::Uuid::new_v4()).expect("compile");
    enrich(&mut project, &StdlibCatalog).expect("enrich");

    let catalog = Arc::new(StdlibCatalog) as Arc<dyn weft_core::NodeCatalog>;
    let color = uuid::Uuid::new_v4();
    let cancellation = Arc::new(Notify::new());

    // Run in-process. Detached (no dispatcher URL) is fine for this
    // smoke test: Text emits its literal, Debug logs it, done.
    let outcome = weft_runner::loop_driver::run_loop(
        project,
        catalog,
        color,
        Some("greeting"),
        serde_json::Value::Null,
        EntryMode::Fresh,
        None,
        cancellation,
    )
    .await
    .expect("run_loop ok");

    assert!(matches!(outcome, weft_runner::LoopOutcome::Completed { .. }));
}
