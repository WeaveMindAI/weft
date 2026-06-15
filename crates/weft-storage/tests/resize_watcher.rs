//! Layer 3: the REAL resize watcher over the real store + fake
//! disks/clock. Grow on fill, shrink only after sustained dwell +
//! cooldown, interrupted-evacuation resume on the watcher path.

use weft_core::storage::bytes_stream;
use weft_storage::config::{SHRINK_COOLDOWN, SHRINK_DWELL};
use weft_storage::disk::PoolCall;
use weft_storage::key::{parse_key, ParsedKey};
use weft_storage::resize::ResizeWatcher;
use weft_storage::testing::StorageTestRig;

const GI: u64 = 1 << 30;
const UNIT: u64 = GI; // test-profile disk unit: 1 GiB

/// Parse a literal key into the `ParsedKey` the store methods take.
fn pk(key: &str) -> ParsedKey {
    parse_key(key).expect("test key literal is well-formed")
}

async fn fill(rig: &StorageTestRig, key: &str, bytes: u64) {
    rig.store
        .put(
            &pk(key),
            "application/octet-stream",
            "fill.bin",
            None,
            bytes_stream(bytes::Bytes::from(vec![0u8; bytes as usize])),
        )
        .await
        .expect("fill put");
}

#[tokio::test]
async fn grows_when_the_pool_fills() {
    let rig = StorageTestRig::with_disks(&[("disk-0", GI)]).await;
    // Leave less than half a unit free.
    fill(&rig, "project/p1/big", GI - UNIT / 4).await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);
    watcher.tick().await.expect("tick");
    assert!(rig.pool.calls().contains(&PoolCall::DiskAddRequested));
}

#[tokio::test]
async fn no_resize_in_the_healthy_band() {
    let rig = StorageTestRig::with_disks(&[("disk-0", GI)]).await;
    fill(&rig, "project/p1/some", GI / 3).await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);
    watcher.tick().await.expect("tick");
    assert!(rig.pool.calls().is_empty(), "{:?}", rig.pool.calls());
}

#[tokio::test]
async fn shrink_waits_for_dwell_then_evacuates_and_releases() {
    let rig = StorageTestRig::with_disks(&[("disk-0", GI), ("disk-1", GI)]).await;
    fill(&rig, "project/p1/small", GI / 10).await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);

    // First tick starts the dwell window; nothing happens.
    watcher.tick().await.expect("tick 1");
    assert!(rig.pool.calls().is_empty());

    // Mid-dwell tick: still nothing.
    rig.advance(SHRINK_DWELL / 2);
    watcher.tick().await.expect("tick 2");
    assert!(rig.pool.calls().is_empty());

    // Past the dwell window: shrink happens (no prior resize, so no
    // cooldown gate).
    rig.advance(SHRINK_DWELL);
    watcher.tick().await.expect("tick 3");
    let calls = rig.pool.calls();
    assert!(
        calls.iter().any(|c| matches!(c, PoolCall::DiskRemoveRequested { .. })),
        "{calls:?}"
    );
    // Data still fully present.
    let (_, stream) = rig.store.get(&pk("project/p1/small"), None).await.expect("get");
    let read = weft_core::storage::collect_stream(stream).await.unwrap();
    assert_eq!(read.len() as u64, GI / 10);
}

#[tokio::test]
async fn dwell_resets_when_the_surplus_dips() {
    let rig = StorageTestRig::with_disks(&[("disk-0", GI), ("disk-1", GI)]).await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);

    watcher.tick().await.expect("tick 1"); // dwell starts
    rig.advance(SHRINK_DWELL / 2);

    // A file lands that erases the droppable-disk surplus (but
    // stays above the grow threshold); the dwell window must reset.
    fill(&rig, "exec/c1/big", (GI / 10) * 7).await;
    watcher.tick().await.expect("tick 2");
    assert!(rig.pool.calls().is_empty());

    // The file is deleted again (the delete-then-redownload thrash
    // case): the window restarts from NOW, so half a dwell later
    // nothing shrinks yet.
    rig.store.delete(&pk("exec/c1/big")).await.unwrap();
    watcher.tick().await.expect("tick 3"); // window restarts
    rig.advance(SHRINK_DWELL / 2);
    watcher.tick().await.expect("tick 4");
    assert!(rig.pool.calls().is_empty(), "thrash must not shrink: {:?}", rig.pool.calls());
}

#[tokio::test]
async fn cooldown_blocks_back_to_back_shrinks_across_the_resize_restart() {
    let mut rig =
        StorageTestRig::with_disks(&[("disk-0", GI), ("disk-1", GI), ("disk-2", GI)]).await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);

    watcher.tick().await.expect("tick");
    rig.advance(SHRINK_DWELL);
    watcher.tick().await.expect("shrink 1");
    let removed = rig
        .pool
        .calls()
        .iter()
        .filter_map(|c| match c {
            PoolCall::DiskRemoveRequested { disk } => Some(disk.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(removed.len(), 1);
    // The dispatcher honors the release (PVC gone) and RESTARTS the
    // pod; the new watcher has fresh RAM but the cooldown stamp
    // lives in boxstate.
    rig.pool.remove_disk(&removed[0]);
    rig.reopen().await;
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);

    // A full dwell later the surplus qualifies again, but the
    // cooldown (persisted across the restart) gates. The pool's call
    // log survives the reopen, so count INCREMENTS past shrink 1.
    let count = || {
        rig.pool
            .calls()
            .iter()
            .filter(|c| matches!(c, PoolCall::DiskRemoveRequested { .. }))
            .count()
    };
    let baseline = count();
    watcher.tick().await.expect("tick post-restart");
    rig.advance(SHRINK_DWELL);
    watcher.tick().await.expect("tick post-shrink");
    assert_eq!(count(), baseline, "cooldown must block the second shrink");

    // After the cooldown the surplus has dwelled continuously, so
    // the next tick shrinks.
    rig.advance(SHRINK_COOLDOWN);
    watcher.tick().await.expect("shrink 2");
    assert_eq!(count(), baseline + 1);
}

#[tokio::test]
async fn watcher_resumes_an_interrupted_evacuation_first() {
    // Crash mid-evacuation, then a NEW watcher (fresh pod) must
    // finish the drain before considering anything else.
    let mut rig = StorageTestRig::with_disks(&[("disk-0", 6 * GI), ("disk-1", 6 * GI)]).await;
    fill(&rig, "project/p1/f", GI / 2).await;
    // All data is on one disk (roomiest); start its evacuation and
    // inject a failure at the first chunk copy (marker write
    // succeeds, copy fails).
    rig.pool.fail_write_after(1);
    let victim = "disk-0";
    assert!(rig.store.evacuate(victim).await.is_err(), "injected interruption");

    rig.reopen().await; // pod restart; scan sees the draining marker
    let mut watcher = ResizeWatcher::new(rig.store.clone(), UNIT);
    watcher.tick().await.expect("resume tick");
    assert!(rig
        .pool
        .calls()
        .contains(&PoolCall::DiskRemoveRequested { disk: victim.to_string() }));
    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get after resume");
    let read = weft_core::storage::collect_stream(stream).await.unwrap();
    assert_eq!(read.len() as u64, GI / 2);
}
