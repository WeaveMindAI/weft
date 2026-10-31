//! The install's database schema: every group of tables it holds, in one
//! list. The boot applies it in one guarded pass, the migration generator
//! (`examples/schema_migration.rs`) writes migrations from it, and the
//! agreement test (`tests/db/schema_agreement.rs`) builds from it, so none
//! of the three can leave a group out. Beside it, every column of JSON
//! documents whose rows outlive an update ([`documents`]), carried to
//! today's shape by the same boot.

use anyhow::Context as _;
use weft_task_store::documents::Encoding;
use weft_task_store::{Document, SchemaGroup};

/// The parts of the list, in dependency order: each role's groups, as
/// that role's crate orders them. The runtime-file plane names no table
/// outside its own group (its two `REFERENCES runtime_file(key)` stay
/// inside it), so it can sit anywhere; it goes last.
static PARTS: &[&[&SchemaGroup]] = &[
    weft_dispatcher::app::DISPATCHER_GROUPS,
    &[&weft_broker::runtime_store::GROUP],
];

/// Every schema group the install's database holds, in dependency order.
pub fn groups() -> Vec<&'static SchemaGroup> {
    PARTS.iter().flat_map(|part| part.iter().copied()).collect()
}

/// Every column of JSON documents whose rows outlive a weft update, with
/// the steps that carry an older weft's rows to today's shape
/// (`weft_task_store::documents`). Each one's samples, one file per
/// version, are under `documents/<table>.<column>/` in this crate, and the
/// `documents` test below checks them against today's types.
///
/// Columns not listed, and why:
/// - Rows that do not outlive an update: task payloads and results, an
///   OAuth connect or picker in flight (`access_connect`,
///   `access_picker`, `access_connect_result`, minutes), worker leases,
///   the announcement outbox.
/// - Columns that are no one type: `signal.kind_state` (each listener
///   kind keeps its own keys there), `alarm.body` (the body of whichever
///   route the wake calls), `infra_event.payload` (a type chosen by the
///   row's `kind`), `signal.auth_config`, `parked_fire.payload` (a
///   trigger's event as it came). Registering one means one document per
///   kind, the day one of those shapes changes.
/// - Plain maps and lists of strings (`access_grant.value_names`,
///   `granted_scopes`, `project_definition.infra_places`), and values a
///   node or a person wrote, which weft never reshapes.
/// - Sealed columns (`access_grant.registration_sealed`, `values_sealed`):
///   only the access store's codec opens them (`crypt.rs`, with the
///   credential key), so carrying one forward needs a step that runs where
///   that key is. None of their types has changed shape.
pub fn documents() -> Vec<&'static Document> {
    DOCUMENTS.iter().collect()
}

static DOCUMENTS: &[Document] = &[
    // A connection's snapshot of its service's recipe, kept as long as the
    // connection. Its events recipes are hashed into `events_recipe_hash`.
    Document::of(
        &weft_core::AccessSpec::STORED,
        "access_grant",
        "spec",
        Encoding::Json,
        weft_access_store::SPEC_FILLS,
        Some(weft_access_store::rehash_grant_recipes),
    ),
    // The events recipes a service declared, kept under their hash.
    Document::of(
        &weft_core::access::events::STORED_EVENTS,
        "service_events_recipe",
        "events_json",
        Encoding::Json,
        &[],
        Some(weft_access_store::rekey_events_recipes),
    ),
    // A run's record, kept as long as the run.
    Document::of(
        &weft_journal::stored::STORED_EVENTS,
        "run_log",
        "events",
        Encoding::Zstd { level: weft_journal::stored::LEVEL },
        &[],
        None,
    ),
    // How a run was asked for: the same type, and the same steps, as a
    // saved spec file.
    Document::of(&weft_core::run_spec::RunSpec::STORED, "run", "spec", Encoding::Json, &[], None),
    // A signal's spec and the program it was registered by.
    Document::of(&weft_core::primitive::SignalSpec::STORED, "signal", "spec_json", Encoding::Json, &[], None),
    Document::of(&weft_core::project::hash::ProgramIdentity::STORED, "signal", "program_json", Encoding::Json, &[], None),
    // A trigger's last bake.
    Document::of(&weft_dispatcher::store::TriggerBake::STORED, "trigger_bake", "bake_json", Encoding::Json, &[], None),
    // Every program a project ran, kept under its hash (which an upgrade
    // keeps: `weft_core::project::hash::compute_definition_hash`), and the
    // project's current one.
    Document::of(
        &weft_core::project::ProjectDefinition::STORED,
        "project_definition",
        "project_json",
        Encoding::Json,
        &[],
        None,
    ),
    Document::of(&weft_core::project::ProjectDefinition::STORED, "project", "project_json", Encoding::Json, &[], None),
    // A project's resync, kept after it ends when a person gave it up (what
    // it left down). Version 2 moved its drive out to `resync_driver`, and
    // the table's check keeps it out.
    Document::of(
        &weft_dispatcher::task_kinds::resync::ResyncPlan::STORED,
        "project",
        "resync",
        Encoding::Json,
        weft_dispatcher::project_store::RESYNC_FILLS,
        None,
    ),
    // What the project's running build was compiled with: with its name
    // and files, the request the maintenance lift builds again.
    Document::of(&weft_core::builds::BuildInputs::STORED, "project", "running_build_inputs", Encoding::Json, &[], None),
    // What a version build waiting on its images was compiled with: it
    // waits across an update, and registers with them once they are built.
    Document::of(&weft_core::builds::BuildInputs::STORED, "version_build", "inputs", Encoding::Json, &[], None),
    // The program a trigger was activated on.
    Document::of(
        &weft_core::project::hash::ProgramIdentity::STORED,
        "trigger_activation",
        "activation_program",
        Encoding::Json,
        &[],
        None,
    ),
    // The health rules an infra node wrote.
    Document::of(&weft_core::infra::health::STORED_RULES, "infra_node", "health_rules_json", Encoding::Json, &[], None),
];

/// The boot's schema work over every group, with every column of
/// documents carried between its two parts
/// (`weft_task_store::schema_guard::apply_schema`): every pending
/// migration across every group runs in one global id order (the order
/// the agreement test replays), and a mismatch anywhere surfaces in ONE
/// error naming every stale group together. Serialization across racing
/// boots is the guard's own job. Runs BEFORE any role's state is built; no
/// role applies schema of its own.
pub async fn apply(pool: &weft_task_store::db::Pool) -> anyhow::Result<()> {
    weft_task_store::apply_schema(pool, &groups(), &documents()).await.context("apply the schema")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two groups of one name would share one stamp and one migration
    /// folder, and a table in two groups would be built twice.
    #[test]
    fn every_group_and_every_table_is_listed_once() {
        let groups = groups();
        let mut names: Vec<&str> = groups.iter().map(|g| g.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "a group is listed twice");
        let mut tables: Vec<&str> = groups.iter().flat_map(|g| g.tables.iter().copied()).collect();
        tables.sort_unstable();
        let before = tables.len();
        tables.dedup();
        assert_eq!(tables.len(), before, "a table belongs to two groups");
        assert!(names.contains(&"runtime_file"), "the broker's runtime-file plane is in the list");
        for document in documents() {
            assert!(tables.contains(&document.table), "document '{}': no schema group owns its table", document.name());
        }
    }

    /// Every registered column's committed samples, one file per version
    /// (`documents/<table>.<column>/v<N>.json`, each a list of documents,
    /// the same documents in every version): today's holds exactly what
    /// today's types read and write back, and every older one, carried
    /// through the steps, lands on today's.
    #[test]
    fn every_document_sample_reads_as_todays_type() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("documents");
        let mut failures = Vec::new();
        for document in documents() {
            let dir = root.join(document.name());
            let current = document.version();
            let sample = |version: weft_core::stored::Version| -> Result<Vec<serde_json::Value>, String> {
                let path = dir.join(format!("v{version}.json"));
                let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
                serde_json::from_str(&text).map_err(|e| format!("{} is not a JSON list: {e}", path.display()))
            };
            let today = match sample(current) {
                Ok(today) => today,
                Err(e) => {
                    failures.push(format!("{}: {e}", document.name()));
                    continue;
                }
            };
            let round_trip = document.read();
            for (at, (doc, read)) in today.iter().map(|doc| (doc, round_trip(doc.clone()))).enumerate() {
                match read {
                    Ok(written) if written == *doc => {}
                    Ok(written) => failures.push(format!(
                        "{} v{current}.json, document {at}: today's type writes it back differently, so its \
                         shape changed. Bump the document's version: add a step for the change next to the \
                         type, and a v{}.json holding these documents in the new shape.\n  stored:  {doc}\n  \
                         written: {written}",
                        document.name(),
                        current + 1
                    )),
                    Err(e) => failures.push(format!(
                        "{} v{current}.json, document {at}: does not read as today's type ({e}), so its shape \
                         changed. Bump the document's version: add a step for the change next to the type, and \
                         a v{}.json holding these documents in the new shape.",
                        document.name(),
                        current + 1
                    )),
                }
            }
            for version in 1..current {
                let older = match sample(version) {
                    Ok(older) => older,
                    Err(e) => {
                        failures.push(format!("{}: {e}", document.name()));
                        continue;
                    }
                };
                if older.len() != today.len() {
                    failures.push(format!("{} v{version}.json holds {} documents and v{current}.json {}: \
                         every version holds the same documents", document.name(), older.len(), today.len()));
                    continue;
                }
                for (at, (mut doc, want)) in older.into_iter().zip(&today).enumerate() {
                    match weft_core::stored::upgrade(document.steps(), version, &mut doc) {
                        Ok(()) if doc == *want => {}
                        Ok(()) => failures.push(format!(
                            "{} v{version}.json, document {at}: carried to v{current} it is not v{current}.json's \
                             document {at}.\n  carried: {doc}\n  today:   {want}",
                            document.name()
                        )),
                        Err(e) => failures.push(format!("{} v{version}.json, document {at}: {e}", document.name())),
                    }
                }
            }
            let stray: Vec<String> = std::fs::read_dir(&dir)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok()?.file_name().into_string().ok())
                        .filter(|name| !(1..=current).any(|v| *name == format!("v{v}.json")))
                        .collect()
                })
                .unwrap_or_default();
            if !stray.is_empty() {
                failures.push(format!("{}: files no version is at: {}", document.name(), stray.join(", ")));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }
}
