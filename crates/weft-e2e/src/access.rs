//! Connection helpers the access-driven e2es share: read a service's
//! recipe off real catalog metadata (never a duplicated JSON blob in
//! tests), connect through the dispatcher's store exactly as the
//! editor does, and stamp the resulting handle onto a node.
//!
//! Connections hold REAL credentials, so their lifetime follows the
//! same policy as [`crate::teardown::Teardown`]: a passing test calls
//! [`Connection::finish`] (an awaited delete, loud on failure); a test
//! that ends early keeps the grant for post-mortem and `Drop` warns
//! with the exact recovery command.

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::client::Dispatcher;
use crate::project::Project;

/// The service's AccessSpec, read off the SAME stdlib catalog metadata
/// the editor ships to the store.
pub fn catalog_spec(package: &str, node_dir: &str) -> Result<Value> {
    let path = weft_catalog::stdlib_root()
        .expect("stdlib root")
        .join(package)
        .join(node_dir)
        .join("metadata.json");
    service_spec_of(&path)
}

/// A service's AccessSpec from any node metadata file (a project
/// fixture's own access node).
pub fn service_spec_of(path: &Path) -> Result<Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
    let meta: Value = serde_json::from_str(&raw)?;
    meta.get("service").cloned().context("metadata carries no service recipe")
}

/// A live connection made through the dispatcher's store, owning the
/// grant's lifetime: [`Connection::finish`] deletes it on a passing
/// test; a `Drop` without `finish` keeps it (for post-mortem of the
/// failure that skipped the finish) and warns with the recovery
/// command.
pub struct Connection {
    /// The `{id, identity}` handle the editor would write onto the
    /// access node.
    handle: Value,
    /// The grant id, for the delete + the recovery hint.
    grant_id: String,
    /// The dispatcher the grant lives on (the delete path).
    disp: Dispatcher,
    /// Set by `finish` so `Drop` stays silent after a clean delete.
    finished: bool,
}

impl Connection {
    /// The `{id, identity}` handle, exactly as the editor writes it
    /// onto an access node (what [`set_account`] stamps).
    pub fn handle(&self) -> &Value {
        &self.handle
    }

    /// End-of-test teardown for a PASSING test: delete the grant
    /// through the dispatcher, awaited so a failure surfaces loudly.
    /// Call alongside `project.finish()`. A delete failure returns
    /// early WITHOUT marking done, so `Drop` still keeps + warns.
    pub async fn finish(mut self) -> Result<()> {
        self.disp
            .delete(&format!("/access/grants/{}", self.grant_id))
            .await
            .with_context(|| format!("teardown: delete connection {}", self.grant_id))?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        tracing::warn!(
            "weft-e2e: connection {} NOT finished (test ended early); keeping it for \
             inspection. Recover with: DELETE /access/grants/{}",
            self.grant_id,
            self.grant_id,
        );
    }
}

/// Connect through the dispatcher's store (the direct flow: paste /
/// mint / server-to-server / shared key) and hand back a
/// [`Connection`] owning the resulting grant. `door` is `"own"` or
/// `"shared"`; `values` are the pasted fields (empty for the shared
/// door).
pub async fn connect_direct(
    disp: &Dispatcher,
    spec: Value,
    door: &str,
    values: Value,
) -> Result<Connection> {
    connect_direct_inner(disp, spec, door, values, false).await
}

/// Connect through a consent service's `own_page.paste` section (a
/// ready credential, no app): the store snapshots the spec's static
/// paste variant.
pub async fn connect_paste(disp: &Dispatcher, spec: Value, values: Value) -> Result<Connection> {
    connect_direct_inner(disp, spec, "own", values, true).await
}

async fn connect_direct_inner(
    disp: &Dispatcher,
    spec: Value,
    door: &str,
    values: Value,
    paste: bool,
) -> Result<Connection> {
    let done: Value = disp
        .post_json(
            "/access/connect/direct",
            &json!({ "spec": spec, "door": door, "values": values, "paste": paste,
                     "project_id": null }),
        )
        .await
        .context("connect_direct through the dispatcher")?;
    let grant = done.get("grant").context("connect answered without a grant")?;
    let grant_id =
        grant.get("id").and_then(Value::as_str).context("grant without id")?.to_string();
    let identity =
        grant.get("identity").cloned().context("connect answered a grant without an identity")?;
    Ok(Connection {
        handle: json!({ "id": grant_id, "identity": identity }),
        grant_id,
        disp: disp.clone(),
        finished: false,
    })
}

/// Stamp the connect handle onto the fixture's access node, exactly as
/// the editor writes it.
pub fn set_account(project: &Project, node: &str, input: &str, handle: &Value) -> Result<()> {
    project.set_node_config(node, input, &handle.to_string())
}

/// A grant row a test seeded DIRECTLY in the store's Postgres (the
/// drift-backstop / refresh / events e2es; no HTTP door writes
/// arbitrary grants), owning its DELETE with the same policy as
/// [`Connection`]: [`SeededGrant::finish`] deletes it on a passing
/// test; a `Drop` without `finish` keeps the row and warns with the
/// recovery SQL.
#[cfg(feature = "e2e")]
pub struct SeededGrant {
    /// The seeded row's id.
    grant_id: uuid::Uuid,
    /// The store's Postgres the row was seeded into.
    pool: sqlx::PgPool,
    /// Set by `finish` so `Drop` stays silent after a clean delete.
    finished: bool,
}

#[cfg(feature = "e2e")]
impl SeededGrant {
    /// Adopt a row the test just INSERTed under `grant_id`.
    pub fn new(pool: sqlx::PgPool, grant_id: uuid::Uuid) -> Self {
        Self { grant_id, pool, finished: false }
    }

    /// End-of-test teardown for a PASSING test: delete the seeded row,
    /// awaited so a failure surfaces loudly.
    pub async fn finish(mut self) -> Result<()> {
        sqlx::query("DELETE FROM access_grant WHERE id = $1")
            .bind(self.grant_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("teardown: delete seeded grant {}", self.grant_id))?;
        self.finished = true;
        Ok(())
    }
}

#[cfg(feature = "e2e")]
impl Drop for SeededGrant {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        tracing::warn!(
            "weft-e2e: seeded grant {} NOT finished (test ended early); keeping the row for \
             inspection. Recover with: DELETE FROM access_grant WHERE id = '{}'",
            self.grant_id,
            self.grant_id,
        );
    }
}
