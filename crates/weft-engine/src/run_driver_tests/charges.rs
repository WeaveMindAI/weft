    //! Layer 3: a charge an earlier drive of the run opened (a job started
    //! before the run paused or its worker died) is held again by the drive
    //! that picks the run up, and a run that then ends for good books it as
    //! unknown, exactly once, on the node that started the job. The
    //! pause-then-price path is pinned with the charges themselves
    //! (`crate::metering::open_charge_tests`).

    use super::*;
    use super::engine_test_rig::{catalog, drive_journal, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::Node;
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;

    /// Does nothing: the run completes once it ran.
    struct Done;
    test_manifest!(Done, "Done");
    #[async_trait]
    impl Node for Done {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Ok(())
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "node", "nodeType": "Done", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                "inputs": [], "outputs": [],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": []
        }))
        .unwrap()
    }

    /// A run born and kicked, with a charge `node` opened on the meter the
    /// metering tests register (`queued`) and nothing closing it: the
    /// record an earlier drive left when it let go of the run.
    fn rows(project: &ProjectDefinition, run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "node".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                cut_from: None, seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            RunEvent::NodeKicked {
                run_id, node_id: "node".into(), frames: vec![], firing: false,
                payload: None, port_snapshot: None, at_unix: 0,
            },
            RunEvent::ChargeOpened {
                run_id,
                node_id: "node".into(),
                frames: vec![],
                service: "queued".into(),
                charge: "job-1".into(),
                scratch: json!({ "model": "m1" }),
                origin: weft_core::CredentialOwner::Author,
                at_unix: 0,
            },
        ]
    }

    /// The costs on the run's record: (node, amount, the charge closed).
    fn costs(events: &[RunEvent]) -> Vec<(String, Option<f64>, Option<String>)> {
        events
            .iter()
            .filter_map(|event| match event {
                RunEvent::CostReported { node_id, amount_usd, charge, .. } => Some((node_id.clone(), *amount_usd, charge.clone())),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_charge_left_open_on_the_record_is_booked_unknown_once_when_the_run_ends() {
        let project = project();
        let run_id = uuid::Uuid::new_v4();
        let rows = rows(&project, run_id);
        let (outcome, events) =
            drive_journal(project, catalog(vec![("Done", Box::new(Done))]), run_id, rows, CancellationFlag::new_arc()).await;
        assert!(matches!(outcome.expect("the drive runs"), RunOutcome::Completed));
        assert_eq!(costs(&events), vec![("node".to_string(), None, Some("job-1".to_string()))], "{events:?}");
        assert_eq!(weft_journal::ChargeBook::of_record(&events).into_open().count(), 0, "the record closed it");
    }

    /// A run cancelled while it was let go (a person stopping a run that
    /// waits) ends before anything of it runs: its open charge is booked
    /// all the same.
    #[tokio::test]
    async fn a_run_cancelled_before_it_was_picked_up_books_its_open_charge() {
        let project = project();
        let run_id = uuid::Uuid::new_v4();
        let rows = rows(&project, run_id);
        let cancellation = CancellationFlag::new_arc();
        cancellation.cancel_because(weft_core::run::CancelCause::User);
        let (outcome, events) =
            drive_journal(project, catalog(vec![("Done", Box::new(Done))]), run_id, rows, cancellation).await;
        assert!(matches!(outcome.expect("the drive runs"), RunOutcome::Cancelled { .. }));
        assert_eq!(costs(&events), vec![("node".to_string(), None, Some("job-1".to_string()))], "{events:?}");
    }

    /// A run that parked on a wait and was asked to stop before it let go
    /// of its worker is queued and claimed with its cancel waiting: the
    /// drive that claims it ends its waiting firing Cancelled with the
    /// cause, the same row a cancel of a run nobody drives writes, then
    /// the run.
    #[tokio::test]
    async fn a_run_claimed_with_its_cancel_waiting_cancels_the_firing_it_parked_on() {
        let project = project();
        let run_id = uuid::Uuid::new_v4();
        let mut rows = rows(&project, run_id);
        rows.retain(|row| !matches!(row, RunEvent::ChargeOpened { .. }));
        rows.push(RunEvent::NodeStarted { run_id, node_id: "node".into(), frames: vec![], at_unix: 0 });
        rows.push(RunEvent::NodeSuspended { run_id, node_id: "node".into(), frames: vec![], token: Some("form".into()), at_unix: 0 });
        let cause = weft_core::run::CancelCause::Run { by: uuid::Uuid::new_v4(), tag: Some("user_7".into()) };
        let cancellation = CancellationFlag::new_arc();
        cancellation.cancel_because(cause.clone());
        let (outcome, events) =
            drive_journal(project, catalog(vec![("Done", Box::new(Done))]), run_id, rows, cancellation).await;
        assert!(matches!(outcome.expect("the drive runs"), RunOutcome::Cancelled { .. }));
        let cancelled: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                RunEvent::NodeCancelled { node_id, reason, .. } => Some((node_id.clone(), reason.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(cancelled, vec![("node".to_string(), cause.to_string())], "{events:?}");
    }
