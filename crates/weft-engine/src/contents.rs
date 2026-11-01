//! The contents a program names (`weft_core::content`), put back into it
//! when the worker makes it runnable.
//!
//! A program stays as it was stored while the worker reads, cuts and
//! hashes it (a fire's cut is hashed and kept the way the install hashes
//! and keeps its own cuts, and a seed's program is held to the hash it is
//! read by). Only the program a run runs has its contents put back
//! ([`runnable`], from what [`Contents::of`] read): every content it
//! names, read through the tenant's storage, held to its hash, cast to the
//! marker's type, and written where the marker was. Nothing that runs a
//! node (readiness, its inputs, a trigger's setup, an infra node's
//! provisioning, the fold of the record of a run this worker drives) meets
//! a marker.
//!
//! The values are kept by content and type, so one content many programs
//! name (the versions of a project, the cuts of one program) is read once
//! by this worker, even by runs asking for it at the same moment, weighed
//! by their JSON under the bound a worker's programs are kept under
//! (`crate::plan`).

use serde_json::Value;
use weft_core::content::ContentValues;
use weft_core::content_cache::{json_bytes, ContentCache};
use weft_core::ProjectDefinition;

use crate::storage::WorkerStorageOps;

/// What a run runs that no read will get back: a content the store holds
/// no such content for, or holds bytes for that are not what its marker
/// says (the program naming it cannot run until the project is built
/// again, `weft resync`), or a program the install never recorded
/// (`crate::plan::Plans::project`). Unlike a read that failed, which the
/// next one may get past: a run carried on is left queued for a later
/// try then (`weft_core::door_fire::RunAnswer::NotNow`), and ended
/// only for this.
#[derive(Debug)]
pub(crate) struct Gone(pub(crate) String);

impl std::fmt::Display for Gone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Gone {}

/// What one worker keeps of the contents its programs name.
pub(crate) struct Contents {
    /// Whose contents: the tenant the worker's project is.
    tenant: String,
    values: ContentCache<(String, String), Value>,
}

impl Contents {
    pub(crate) fn new(tenant: String) -> Self {
        Self { tenant, values: ContentCache::weighed(crate::plan::KEPT_JSON_BYTES, json_bytes) }
    }

    /// The value of every content `project` (whose definition hash is
    /// `definition_hash`) names, each read through `storage` unless this
    /// worker has it. A content that is [`Gone`] fails with a `Gone`
    /// naming the file and the way back; a read that failed, with the
    /// read's error, and nothing of it is kept.
    pub(crate) async fn of(
        &self,
        storage: &dyn WorkerStorageOps,
        project: &ProjectDefinition,
        definition_hash: &str,
    ) -> anyhow::Result<ContentValues> {
        let mut values = ContentValues::new();
        for content in weft_core::content::refs_in_program(project) {
            let id = content.id();
            let read = async {
                let bytes = storage
                    .content(&format!("{}/{}", self.tenant, content.asset_key()))
                    .await?
                    .ok_or_else(|| Gone("the store holds no such content".into()))?;
                Ok(content.value_of(&bytes).map_err(Gone)?)
            };
            // Named after the program here, not in the load: a caller
            // waiting on another program's read of the same content gets
            // its own program named.
            let value = self.values.get_or_load(id.clone(), || read).await.map_err(|e| {
                let what = format!("program {definition_hash} reads '{}' ({}), stored as {}", content.path(), content.ty(), content.sha256());
                match e.downcast_ref::<Gone>() {
                    Some(gone) => anyhow::Error::new(Gone(format!(
                        "{what}, and it cannot be read: {gone}. Building the project again from its files stores it again (`weft resync`)"
                    ))),
                    None => anyhow::anyhow!("{what}, and reading it failed: {e}"),
                }
            })?;
            values.insert(id, value);
        }
        Ok(values)
    }
}

/// `project` with every content it names put back from `values` (read by
/// [`Contents::of`] for it, or for the program it was cut from).
pub(crate) fn runnable(project: &ProjectDefinition, values: &ContentValues) -> anyhow::Result<ProjectDefinition> {
    let mut runnable = project.clone();
    weft_core::content::resolve_program(&mut runnable, values).map_err(anyhow::Error::msg)?;
    Ok(runnable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use weft_core::weft_type::{WeftPrimitive, WeftType};

    pub(crate) fn program_naming(markers: Vec<Value>) -> ProjectDefinition {
        let mut project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::nil(), "name": "p", "description": null,
            "nodes": [{ "id": "q", "nodeType": "PostgresExecuteQuery", "label": null, "config": {}, "position": { "x": 0.0, "y": 0.0 },
                        "inputs": [], "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }],
            "edges": [], "groups": [],
            "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        project.nodes[0].port_literals.insert("query".into(), markers[0].clone());
        project.nodes[0].port_literals.insert("params".into(), Value::Array(markers[1..].to_vec()));
        project
    }

    /// Every marker a program names comes back as its cast value, wherever
    /// it sits, and each content is read once however many programs and
    /// fields name it. The program read stays as it was.
    #[tokio::test]
    async fn a_program_reads_its_contents_once_each() {
        let storage = crate::storage::FakeWorkerStorage::new();
        let sql = "SELECT 1;".repeat(1000);
        storage.hold_content(sql.as_bytes());
        storage.hold_content(b"7");
        let string = WeftType::Primitive(WeftPrimitive::String);
        let number = WeftType::Primitive(WeftPrimitive::Number);
        let (query, _) = weft_core::content::ContentRef::of_text(&sql, &string, "sql/q.sql").unwrap();
        let (seven, _) = weft_core::content::ContentRef::of_text("7", &number, "seven.txt").unwrap();
        let program = program_naming(vec![query.to_value(), seven.to_value(), seven.to_value()]);
        let contents = Contents::new("t1".into());
        for _ in 0..2 {
            let values = contents.of(storage.as_ref(), &program, "h").await.unwrap();
            let run = runnable(&program, &values).unwrap();
            assert_eq!(run.nodes[0].port_literals["query"], json!(sql));
            assert_eq!(run.nodes[0].port_literals["params"], json!([7.0, 7.0]));
        }
        assert_eq!(program.nodes[0].port_literals["query"], query.to_value());
        let mut reads = storage.content_reads();
        reads.sort();
        let mut each_once = vec![format!("t1/{}", query.asset_key()), format!("t1/{}", seven.asset_key())];
        each_once.sort();
        assert_eq!(reads, each_once);
    }

    /// A content the store no longer holds fails the program naming it as
    /// [`Gone`], with the file, the program and the way back.
    #[tokio::test]
    async fn a_content_that_is_gone_fails_the_program_naming_it() {
        let storage = crate::storage::FakeWorkerStorage::new();
        let (gone, _) =
            weft_core::content::ContentRef::of_text("SELECT 2;", &WeftType::Primitive(WeftPrimitive::String), "sql/gone.sql").unwrap();
        let e = Contents::new("t1".into())
            .of(storage.as_ref(), &program_naming(vec![gone.to_value()]), "h1")
            .await
            .unwrap_err();
        assert!(e.downcast_ref::<Gone>().is_some(), "{e:#}");
        let e = e.to_string();
        assert!(e.contains("program h1 reads 'sql/gone.sql' (String)"), "{e}");
        assert!(e.contains("weft resync"), "{e}");
    }
}
