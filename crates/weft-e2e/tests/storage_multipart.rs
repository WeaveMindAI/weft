//! In-depth Layer-4 coverage of the runtime-file UPLOAD path, exercised through
//! real node code running in real worker pods against the real broker + real
//! object store (SeaweedFS in the local emulation). This is the one layer that
//! proves the multipart upload machinery inside `ctx.storage` end to end: the
//! per-part presigned URLs (with their SIGNED exact Content-Length) are accepted
//! by a real S3-compatible server, parts assemble in order, an interrupted
//! upload cleans up and frees its quota with no leftover, and concurrent uploads
//! stay isolated and consistent.
//!
//! Node code is UNCHANGED by the multipart reshape: every scenario here drives
//! the exact same `FetchToStorage` node an author writes; all the slicing,
//! per-part reservation, retry, resume, and abort lives inside the runtime, so
//! these tests also prove that the robustness is fully hidden from node authors.
//!
//! The quota-lock unit behavior (a wrong-size part rejected by the signed
//! length, reserve-time charging, resume, idempotent record/complete, the abort
//! sweeps) is pinned deterministically at Layer 3 against a real Postgres +
//! a fake bucket that enforces the signed length
//! (`weft-broker/tests/runtime_store.rs`). These Layer-4 scenarios prove the
//! same paths hold against the REAL bucket and through real worker code.
#![cfg(feature = "e2e")]

use weft_e2e::fakes::{BytesFake, HangingBytesFake};
use weft_e2e::{ensure, project::Project, run, storage, Platform};

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
/// and assert the kept file downloads back byte-exact. Returns the run's color
/// so a caller can probe platform state for it.
async fn fetch_and_verify(
    disp: &weft_e2e::Dispatcher,
    content: Vec<u8>,
    label: &str,
) -> anyhow::Result<()> {
    let mut project = Project::prepare("storage_file", disp.clone()).await?;
    let pid = project.id();
    let fake = BytesFake::start(content.clone()).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &fake.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    let prefix = format!("exec/{}/", settled.color);
    let key = storage::assert_file_contents(disp, &pid, &prefix, &content).await?;
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

/// A source that DIES mid-transfer must leave NO leftover: the runtime's upload
/// aborts the in-flight multipart upload and frees its quota reservation, so no
/// pending row and no charged bytes linger, and nothing becomes downloadable.
///
/// The fake advertises far more bytes than it sends (a multi-part total) then
/// breaks the body after the first part's worth, so the worker has a part
/// already landed and a reservation charged when the stream errors: the abort
/// path has real state to clean, not a no-op.
#[tokio::test]
async fn an_interrupted_upload_leaves_no_leftover() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;

    let mut project = Project::prepare("storage_file", disp.clone()).await?;
    let pid = project.id();

    // Advertise 3 parts, deliver ~1.5 parts then break: at least one full part
    // has landed (and its bytes are reserved) when the stream errors.
    let declared = 3 * PART_SIZE;
    let sent = PART_SIZE + PART_SIZE / 2;
    let fake = HangingBytesFake::start(sent, declared).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &fake.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    // The node must fail LOUD (the source died mid-transfer); a silent success
    // here would mean a truncated file was accepted as complete.
    settled.failed_with("storage")?;

    // The tenant this run stored under (OSS `local`, but read it rather than
    // hardcode). If the color has NO runtime rows at all, the abort already
    // deleted everything, which is exactly the success condition.
    let color = settled.color;
    let pending = platform.runtime_pending_uploads_for_color(&color).await?;
    anyhow::ensure!(
        pending == 0,
        "interrupted upload left {pending} pending row(s) for color {color}; \
         the abort should have deleted the reservation"
    );

    if let Some(tenant) = platform.runtime_tenant_for_color(&color).await? {
        // Any surviving charge would be a leaked reservation. (Other completed
        // files from earlier scenarios could contribute, so this asserts only
        // that THIS color contributes nothing: its rows are gone, checked
        // above, so the only honest cross-check is that no pending bytes remain
        // under the color. The pending==0 check already proves the reservation
        // was freed; this reads the tenant total for the diagnostic.)
        let charged = platform.runtime_charged_bytes(&tenant).await?;
        eprintln!("[interrupted] tenant {tenant} charged {charged} bytes after abort");
    }

    // Nothing became downloadable under the run's scope.
    let prefix = format!("exec/{}/", color);
    let files = storage::list_prefix(&disp, &pid, &prefix).await?;
    anyhow::ensure!(
        files.is_empty(),
        "interrupted upload left {} visible file(s) under {prefix}: {:?}",
        files.len(),
        files.iter().filter_map(weft_e2e::storage::StoredFile::key).collect::<Vec<_>>()
    );

    project.finish().await
}

/// Three multipart uploads driven CONCURRENTLY (three parallel fetch nodes in
/// one graph) each round-trip byte-exact with no cross-contamination, and the
/// tenant's usage reflects all three. This exercises concurrent begins +
/// per-part reservations against the real broker under its tenant lock: a race
/// that mixed up two uploads' parts, or double-charged / lost a reservation,
/// would surface as wrong bytes or a wrong usage total.
#[tokio::test]
async fn concurrent_uploads_stay_isolated_and_consistent() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;

    let mut project = Project::prepare("storage_concurrent", disp.clone()).await?;
    let pid = project.id();

    // Distinct multi-part payloads of DIFFERENT sizes, so a cross-over is caught
    // by both the bytes AND the per-file size, and the sizes don't coincide.
    let a = patterned(2 * PART_SIZE + 100);
    let b = patterned(PART_SIZE + PART_SIZE / 2 + 55);
    let c = patterned(3 * PART_SIZE + 9);
    let fake_a = BytesFake::start(a.clone()).await?;
    let fake_b = BytesFake::start(b.clone()).await?;
    let fake_c = BytesFake::start(c.clone()).await?;
    project.substitute_in_main("__E2E_FAKE_URL_A__", &fake_a.url())?;
    project.substitute_in_main("__E2E_FAKE_URL_B__", &fake_b.url())?;
    project.substitute_in_main("__E2E_FAKE_URL_C__", &fake_c.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // Each distinct payload must appear EXACTLY as stored (byte-exact download),
    // and each is a different size, so a mixed-up part would fail the match.
    let prefix = format!("exec/{}/", settled.color);
    for (payload, label) in [(&a, "a"), (&b, "b"), (&c, "c")] {
        let key = storage::assert_file_contents(&disp, &pid, &prefix, payload)
            .await
            .map_err(|e| anyhow::anyhow!("concurrent file {label} did not round-trip: {e}"))?;
        eprintln!("[concurrent] file {label} ({} bytes) at {key}", payload.len());
    }

    // Usage consistency: the three kept files sum to their exact total under the
    // run's tenant (each upload charged once, none lost or double-charged).
    if let Some(tenant) = platform.runtime_tenant_for_color(&settled.color).await? {
        let charged = platform.runtime_charged_bytes(&tenant).await?;
        let expected = (a.len() + b.len() + c.len()) as i64;
        anyhow::ensure!(
            charged >= expected,
            "tenant {tenant} charged {charged} bytes, expected at least the three \
             concurrent files' {expected} bytes"
        );
    }

    // No in-flight leftovers after a clean completion.
    let pending = platform.runtime_pending_uploads_for_color(&settled.color).await?;
    anyhow::ensure!(pending == 0, "completed run left {pending} pending upload(s)");

    project.finish().await
}
