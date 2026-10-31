    //! Layer 3: a run's secrets through the real loop and the real writer.
    //! The record holds only their markers, their values ride the run's own
    //! batches, a drive reveals what it made from memory and anything else
    //! from the record, and a run that is not recorded writes none.

    use super::*;
    use super::engine_test_rig::{born, catalog, clients, handle, run_on, test_manifest, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::secret::{Secret, SecretId};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::{JournalClient, RunEvent};

    /// The caller's first credential, revealed, passed on as a secret of
    /// its own.
    struct PassesOn;
    test_manifest!(PassesOn, "Answer");
    #[async_trait]
    impl Node for PassesOn {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let request: weft_core::caller::LiveRequest = serde_json::from_value(ctx.wake.record()?).unwrap();
            let (_, credential) = &request.credentials[0];
            let token: String = ctx.reveal(credential).await?;
            let passed = ctx.secret(&format!("passed on: {token}"))?;
            ctx.pulse_downstream(NodeOutput::new().set("value", passed)).await
        }
    }

    /// Reveals `secret`, which some other drive made, into `found`.
    struct RevealsElsewhere {
        secret: Secret,
        found: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    }
    test_manifest!(RevealsElsewhere, "Answer");
    #[async_trait]
    impl Node for RevealsElsewhere {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let value: String = ctx.reveal(&self.secret).await?;
            *self.found.lock().unwrap() = Some(value);
            ctx.pulse_downstream(NodeOutput::new().set("value", self.secret)).await
        }
    }

    /// Makes a secret, reveals it, and passes the marker on.
    struct MakesAndReveals;
    test_manifest!(MakesAndReveals, "Answer");
    #[async_trait]
    impl Node for MakesAndReveals {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let made = ctx.secret("kept-in-memory")?;
            let back: String = ctx.reveal(&made).await?;
            assert_eq!(back, "kept-in-memory");
            ctx.pulse_downstream(NodeOutput::new().set("value", made)).await
        }
    }

    fn project(recorded: bool) -> (ProjectDefinition, weft_core::run_settings::RunSettings) {
        let project = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "a", "nodeType": "Answer", "position": {"x": 0, "y": 0},
                "outputs": [{"name": "value", "portType": "Secret[String]", "required": false}]
            }]
        }))
        .unwrap();
        (project, weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, recorded).unwrap())
    }

    fn birth(run_id: weft_core::RunId, project: &ProjectDefinition, settings: weft_core::run_settings::RunSettings, payload: Option<serde_json::Value>) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "a".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None,
                source_version: None,
                run_kind: weft_core::run::RunKind::Project,
                cut_from: None,
                seed: None,
                instance: None,
                fired_trigger: None,
                stand_in: None,
                instance_values: Default::default(),
                picks: Default::default(),
                settings,
                at_unix: 0,
            },
            RunEvent::NodeKicked { run_id, node_id: "a".into(), frames: vec![], firing: payload.is_some(), payload, port_snapshot: None, at_unix: 0 },
        ]
    }

    /// The marker `value` emitted, read off the record.
    fn emitted(events: &[RunEvent]) -> Secret {
        events
            .iter()
            .find_map(|event| match event {
                RunEvent::PortEmitted { port, value, .. } if port == "value" => Secret::from_value(value),
                _ => None,
            })
            .expect("a secret was emitted on `value`")
    }

    /// A caller's credential, made a secret at the door, is never in the
    /// run's record: its birth holds the marker, the node reveals the
    /// value, and what it passes on is a secret of its own. Both values
    /// ride the run's batches, the credential with its birth.
    #[tokio::test]
    async fn a_run_writes_down_only_its_secrets_markers() {
        let (project, settings) = project(true);
        let run_id = weft_core::new_run_id();
        let credential = SecretId::mint();
        let request = weft_core::caller::LiveRequest {
            headers: vec![("Accept".into(), "application/json".into())],
            credentials: vec![("Authorization".into(), Secret::new(credential))],
            ..Default::default()
        };
        let birth = birth(run_id, &project, settings, Some(serde_json::to_value(&request).unwrap()));
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        let door = vec![weft_journal::frame::KeptSecret { secret_id: credential, value: json!("Bearer secret-token-123") }];
        let run = handle(&clients, run_id, &birth, 0, door);
        run.record_events(&birth, Some("instance-test")).await.unwrap();
        let drove = run_on(std::sync::Arc::new(project), catalog(vec![("Answer", Box::new(PassesOn))]), &clients, run_id, run, birth, weft_core::cancellation::CancellationFlag::new_arc(), None, None)
            .await
            .expect("the drive ends");
        clients.writer.written().await;
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        let events = journal.events_of(run_id);
        let written = serde_json::to_string(&events).unwrap();
        assert!(!written.contains("secret-token-123"), "{written}");
        let kept = journal.secrets.lock().unwrap().clone();
        assert_eq!(kept.get(&credential), Some(&json!("Bearer secret-token-123")));
        assert_eq!(kept.get(&emitted(&events).id()), Some(&json!("passed on: Bearer secret-token-123")));
    }

    /// A drive that did not make a secret reveals it from the record: what
    /// a run picked up again (paused, handed back, moved) or a run of the
    /// same project holding a marker does, with an empty memory.
    #[tokio::test]
    async fn a_secret_another_drive_made_is_revealed_from_the_record() {
        let (project, settings) = project(true);
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        let made_elsewhere = SecretId::mint();
        journal.secrets.lock().unwrap().insert(made_elsewhere, json!("from another drive"));
        let found = std::sync::Arc::new(std::sync::Mutex::new(None));
        let node = RevealsElsewhere { secret: Secret::new(made_elsewhere), found: found.clone() };
        let run_id = weft_core::new_run_id();
        let birth = birth(run_id, &project, settings, None);
        let run = born(&clients, run_id, &birth).await;
        let drove = run_on(std::sync::Arc::new(project), catalog(vec![("Answer", Box::new(node))]), &clients, run_id, run, birth, weft_core::cancellation::CancellationFlag::new_arc(), None, None)
            .await
            .expect("the drive ends");
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        assert_eq!(found.lock().unwrap().as_deref(), Some("from another drive"));
    }

    /// A secret no run keeps fails the node that reveals it, naming the
    /// secret and why it is gone.
    #[tokio::test]
    async fn a_secret_whose_record_is_gone_fails_by_name() {
        let (project, settings) = project(true);
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        let gone = SecretId::mint();
        let node = RevealsElsewhere { secret: Secret::new(gone), found: Default::default() };
        let run_id = weft_core::new_run_id();
        let birth = birth(run_id, &project, settings, None);
        let run = born(&clients, run_id, &birth).await;
        let drove = run_on(std::sync::Arc::new(project), catalog(vec![("Answer", Box::new(node))]), &clients, run_id, run, birth, weft_core::cancellation::CancellationFlag::new_arc(), None, None)
            .await
            .expect("the drive ends");
        let RunOutcome::Failed { error, .. } = &drove.outcome else { panic!("{:?}", drove.outcome) };
        assert!(error.contains(&gone.to_string()) && error.contains("no run of this project keeps it"), "{error}");
    }

    /// A run that is not recorded reveals what it made from memory, and
    /// no batch it sends carries a secret.
    #[tokio::test]
    async fn an_unrecorded_run_keeps_its_secrets_in_memory() {
        let (project, settings) = project(false);
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        let run_id = weft_core::new_run_id();
        let birth = birth(run_id, &project, settings, None);
        let run = born(&clients, run_id, &birth).await;
        let drove = run_on(std::sync::Arc::new(project), catalog(vec![("Answer", Box::new(MakesAndReveals))]), &clients, run_id, run, birth, weft_core::cancellation::CancellationFlag::new_arc(), None, None)
            .await
            .expect("the drive ends");
        clients.writer.written().await;
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        assert!(journal.secrets.lock().unwrap().is_empty());
    }
