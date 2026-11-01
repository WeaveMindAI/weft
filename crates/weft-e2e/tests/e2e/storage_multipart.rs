//! In-depth Layer-4 coverage of the runtime-file UPLOAD path, exercised through
//! real node code running in real worker processes against the real broker + real
//! object store (SeaweedFS in the local emulation). This is the one layer that
//! proves the multipart upload machinery inside `ctx.storage` end to end: the
//! per-part presigned URLs (with their SIGNED exact Content-Length) are accepted
//! by a real S3-compatible server and parts assemble in order, for every size
//! shape a payload can take.
//!
//! The test drives the plain `FetchToStorage` node an author writes; all the
//! slicing and per-part reservation lives inside the runtime, so it also
//! proves that machinery is hidden from node authors.
//!
//! The quota-lock unit behavior (a wrong-size part rejected by the signed
//! length, reserve-time charging, resume, idempotent record/complete, the abort
//! sweeps) is pinned deterministically at Layer 3 against a real Postgres +
//! a fake bucket that enforces the signed length
//! (`weft-broker/tests/db/runtime_store.rs`). This Layer-4 test proves the
//! upload path holds against the REAL bucket and through real worker code.
#![cfg(feature = "e2e")]

use weft_e2e::fakes::BytesFake;
use weft_e2e::{ensure, project::Project, run, storage};

/// The fixed multipart part size the runtime slices uploads into (8 MiB). Kept
/// in sync with the runtime's `DEFAULT_PART_SIZE_BYTES`; the size assertions
/// below only need it to be > this for "multi-part" and to straddle a part
/// boundary, so an exact match is not load-bearing, but it documents intent.
const PART_SIZE: usize = 8 * 1024 * 1024;

/// A deterministic payload of `len` bytes with a position-dependent pattern, so
/// any byte landing at the wrong offset (a part out of order, a truncated part,
/// a crossed-over upload) changes the downloaded bytes and fails the assert.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Run the single-fetch `storage_file` fixture against a fake serving `content`,
/// and assert the kept file downloads back byte-exact. Returns the run
/// so a caller can probe platform state for it.
async fn fetch_and_verify(
    disp: &weft_e2e::Dispatcher,
    content: Vec<u8>,
    label: &str,
) -> anyhow::Result<()> {
    let mut project = Project::prepare("storage_file", disp.clone()).await?;
    let fake = BytesFake::start(content.clone()).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &fake.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    let prefix = format!("run/{}/", settled.run_id);
    let key = storage::assert_file_contents(disp, &prefix, &content).await?;
    eprintln!("[{label}] {} bytes round-tripped at {key}", content.len());
    project.finish().await
}

/// Sizes that exercise the whole multipart shape through one fixture:
///   - EMPTY (0 bytes): the one-zero-byte-part edge; the file must still exist
///     and download as empty.
///   - SINGLE PART (small): the common case, one part, no boundary.
///   - EXACT PART BOUNDARY: a payload of exactly one part, so the stream ends on
///     a boundary with no short final part (the "flush nothing extra" edge).
///   - MULTI-PART with a short tail: > 2 full parts plus a remainder, so parts
///     assemble in order against the REAL bucket and the signed per-part
///     Content-Length is accepted by real SeaweedFS.
#[tokio::test]
async fn multipart_uploads_round_trip_across_sizes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;

    fetch_and_verify(&disp, patterned(0), "empty").await?;
    fetch_and_verify(&disp, patterned(11), "tiny").await?;
    fetch_and_verify(&disp, patterned(PART_SIZE), "exact-boundary").await?;
    fetch_and_verify(&disp, patterned(2 * PART_SIZE + 4242), "multi-part").await?;

    Ok(())
}
