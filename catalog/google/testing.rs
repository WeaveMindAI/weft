//! Shared live-test cleanup for the google package: the one DELETE
//! shape every self-cleaning test uses to take its artifacts back out
//! of the connected account (Drive files, calendar events).
#![cfg(feature = "node-tests")]

use weft::access::OpenedConnection;
use weft::{NodeErrExt, WeftResult};

/// DELETE `url` through the test's own connection, loud on refusal.
/// Google's delete endpoints answer an empty 204, so this checks the
/// status and never parses a body.
pub async fn delete(conn: &OpenedConnection, url: &str, what: &str) -> WeftResult<()> {
    let resp = conn.client().delete(url).send().await.node_err(what)?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        weft::node_bail!(
            "the service answered {status} trying to {what}: {}",
            body.chars().take(500).collect::<String>()
        );
    }
    Ok(())
}

/// Delete a Drive file (docs and folders are Drive files too) by id.
pub async fn drive_delete(conn: &OpenedConnection, id: &str) -> WeftResult<()> {
    delete(
        conn,
        &format!("{}/files/{id}", super::drive::API),
        "delete the test file from Drive",
    )
    .await
}

/// Collects the Drive files a live test creates and deletes every one
/// once the test body finishes, however it finished (returned,
/// `?`-errored, or panicked on an assert). The body registers each file
/// on the returned [`DriveScope`] right after creating it; a failing run
/// then never leaves an artifact in the account. The one self-cleaning
/// shape every Drive-touching google live test uses, so the id-registry
/// and the delete loop live here once, not re-rolled per test.
///
/// Usage: `let scope = DriveScope::new(); with_cleanup(body, ||
/// scope.delete_all(conn)).await`, where `body` calls `scope.track(id)`
/// per create. See any drive live test for the shape.
#[derive(Default)]
pub struct DriveScope {
    // A Mutex (not a RefCell): the live test future runs on a
    // multi-thread runtime, so the scope must be Send.
    created: std::sync::Mutex<Vec<String>>,
}

impl DriveScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a just-created Drive file for deletion.
    pub fn track(&self, id: impl Into<String>) {
        self.created.lock().unwrap().push(id.into());
    }

    /// Extract a Drive create response's `id` AND register it in one
    /// step, so there is no window where the file exists but is not yet
    /// tracked (a bare `resp["id"].expect(...)` could panic between the
    /// create and a later `track`, leaking the file). Loud, not a
    /// panic, if the create answered 200 without an id.
    pub fn track_created(&self, resp: &serde_json::Value, what: &str) -> WeftResult<String> {
        let id = resp["id"].as_str().ok_or_else(|| {
            weft::error::node_error(format!("{what} answered no file id: {resp}"))
        })?;
        self.track(id);
        Ok(id.to_string())
    }

    /// Delete every tracked file, draining the registry (so nothing
    /// double-deletes) and continuing past a single failure so one
    /// undeletable file cannot strand the rest; the first failure is
    /// surfaced.
    pub async fn delete_all(&self, conn: &OpenedConnection) -> WeftResult<()> {
        let ids: Vec<String> = std::mem::take(&mut *self.created.lock().unwrap());
        let mut failure = None;
        // Newest first: a test that moved a file INTO a tracked folder
        // must delete the file before the folder, because deleting a
        // Drive folder deletes its contents with it (the folder-first
        // order would 404 on the already-gone child).
        for id in ids.into_iter().rev() {
            if let Err(e) = drive_delete(conn, &id).await {
                failure.get_or_insert(e);
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
