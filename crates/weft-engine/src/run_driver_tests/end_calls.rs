    //! Layer 3: a call a node body registers through `ctx.after_node` is
    //! sent once when the node ends, through the real loop, whether the
    //! body returned or failed.

    use super::*;
    use super::engine_test_rig::{catalog, drive, node_json, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::{WeftError, WeftResult};
    use weft_core::node::Node;
    use weft_core::signal::CancelCall;
    use weft_core::{ProjectDefinition, RunContext};
    use weft_journal::RunEvent;
    use std::sync::Mutex as StdMutex;

    /// Makes something at the provider at `base`, registers its clean-up
    /// right after, then returns, or fails when `fails`.
    struct Uploading {
        base: String,
        fails: bool,
    }
    test_manifest!(Uploading, "Uploading");
    #[async_trait]
    impl Node for Uploading {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.after_node(CancelCall::delete(format!("{}/uploads/1", self.base)).always(), None).await?;
            match self.fails {
                true => Err(WeftError::NodeFailed("the job failed".into())),
                false => Ok(()),
            }
        }
    }

    /// A provider answering every call 204, keeping each call's verb and
    /// path.
    async fn provider() -> (String, Arc<StdMutex<Vec<(String, String)>>>) {
        let calls: Arc<StdMutex<Vec<(String, String)>>> = Arc::default();
        let seen = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push((method.to_string(), uri.path().to_string()));
                axum::http::StatusCode::NO_CONTENT
            }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), calls)
    }

    /// Drive a run of one `Uploading` node against a fresh provider: the
    /// run's outcome, its record, and what the provider was sent.
    async fn run_uploading(fails: bool) -> (RunOutcome, Vec<RunEvent>, Vec<(String, String)>) {
        let (base, calls) = provider().await;
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "nodes": [node_json("node", "Uploading", &[], &[])], "edges": [], "groups": []
        }))
        .unwrap();
        let nodes = catalog(vec![("Uploading", Box::new(Uploading { base, fails }) as Box<dyn Node>)]);
        let (outcome, events) = drive(project, nodes, &["node"]).await;
        let sent = calls.lock().unwrap().clone();
        (outcome, events, sent)
    }

    /// The call is owed on the record, sent once, and settled before the
    /// node's ending is written.
    fn assert_sent_once_before_the_end(events: &[RunEvent], sent: &[(String, String)], ending: &str) {
        assert_eq!(sent, [("DELETE".to_string(), "/uploads/1".to_string())], "sent once");
        let kinds: Vec<&str> = events.iter().map(RunEvent::kind_str).collect();
        let at = |kind: &str| kinds.iter().position(|k| *k == kind).unwrap_or_else(|| panic!("no {kind} in {kinds:?}"));
        assert!(at("end_call_owed") < at("end_calls_settled"), "{kinds:?}");
        assert!(at("end_calls_settled") < at(ending), "settled before the node's ending: {kinds:?}");
    }

    #[tokio::test]
    async fn a_registered_call_goes_once_when_the_node_returns() {
        let (outcome, events, sent) = run_uploading(false).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_sent_once_before_the_end(&events, &sent, "node_completed");
    }

    #[tokio::test]
    async fn a_registered_call_goes_once_when_the_node_fails() {
        let (outcome, events, sent) = run_uploading(true).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert_sent_once_before_the_end(&events, &sent, "node_failed");
    }
