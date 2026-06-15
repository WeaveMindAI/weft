//! Layer 3: the REAL Store over fake disks. Spanning, rebuild by
//! scan, loud mismatch, in-place reclaim, interrupted evacuation.

use std::sync::Arc;

use weft_core::storage::{bytes_stream, collect_stream, ByteRange, KeepTtl};
use weft_storage::disk::{DiskPoolOps, FakeDiskPool, PoolCall};
use weft_storage::key::{parse_key, ParsedKey};
use weft_storage::store::{Store, StoreError};
use weft_storage::testing::StorageTestRig;

const CHUNK: u64 = weft_storage::config::CHUNK_SIZE_BYTES;

/// Parse a literal key into the `ParsedKey` the store methods take.
/// Panics on a malformed literal (a test typo, not a runtime input).
fn pk(key: &str) -> ParsedKey {
    parse_key(key).expect("test key literal is well-formed")
}

fn big_bytes(len: u64) -> bytes::Bytes {
    // Position-dependent pattern so any reordering/corruption breaks
    // equality, not just length.
    bytes::Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

async fn put_bytes(
    store: &Arc<Store>,
    key: &str,
    data: bytes::Bytes,
    keep: Option<KeepTtl>,
) -> weft_core::storage::StoredFileMeta {
    store
        .put(&pk(key), "application/octet-stream", "f.bin", keep, bytes_stream(data))
        .await
        .expect("put")
}

#[tokio::test]
async fn file_larger_than_one_disk_spans_chunks_and_reads_back_byte_identical() {
    // Two disks of 2 chunks each; a 2.5-chunk file MUST span (no
    // single disk can hold all three chunks plus metadata).
    let cap = 2 * CHUNK;
    let rig = StorageTestRig::with_disks(&[("disk-0", cap), ("disk-1", cap)]).await;
    let data = big_bytes(2 * CHUNK + CHUNK / 2);
    let meta = put_bytes(&rig.store, "exec/c1/big", data.clone(), None).await;
    assert_eq!(meta.size_bytes, data.len() as u64);
    // Chunks really live on both disks.
    assert!(rig.pool.file_count("disk-0") > 0, "disk-0 holds pieces");
    assert!(rig.pool.file_count("disk-1") > 0, "disk-1 holds pieces");

    let (_, stream) = rig.store.get(&pk("exec/c1/big"), None).await.expect("get");
    let read = collect_stream(stream).await.expect("collect");
    assert_eq!(read, data, "byte-identical across spans");
}

#[tokio::test]
async fn range_get_returns_exact_sub_ranges_across_chunk_boundaries() {
    let rig = StorageTestRig::with_disks(&[("disk-0", 2 * CHUNK), ("disk-1", 2 * CHUNK)]).await;
    let data = big_bytes(CHUNK * 2 + 100);
    put_bytes(&rig.store, "exec/c1/r", data.clone(), None).await;

    // A window straddling the first chunk boundary.
    let start = CHUNK - 50;
    let end = CHUNK + 75;
    let (_, stream) = rig
        .store
        .get(&pk("exec/c1/r"), Some(ByteRange { start, end: Some(end) }))
        .await
        .expect("range get");
    let read = collect_stream(stream).await.expect("collect");
    assert_eq!(&read[..], &data[start as usize..end as usize]);

    // Open-ended tail.
    let (_, stream) = rig
        .store
        .get(&pk("exec/c1/r"), Some(ByteRange { start: CHUNK * 2, end: None }))
        .await
        .expect("tail get");
    let read = collect_stream(stream).await.expect("collect");
    assert_eq!(&read[..], &data[(CHUNK * 2) as usize..]);
}

#[tokio::test]
async fn delete_frees_the_chunks_in_place() {
    let rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/x", big_bytes(1000), None).await;
    let before = rig.pool.file_count("disk-0");
    rig.store.delete(&pk("exec/c1/x")).await.expect("delete");
    // Chunk + meta both gone; only boxstate remains.
    assert!(rig.pool.file_count("disk-0") < before);
    assert!(matches!(
        rig.store.get(&pk("exec/c1/x"), None).await,
        Err(StoreError::NotFound(_))
    ));
    let usage = rig.store.usage().await.unwrap();
    assert_eq!(usage.stored_bytes, 0);
}

#[tokio::test]
async fn index_rebuild_by_scan_reproduces_the_index_exactly() {
    let mut rig = StorageTestRig::with_disks(&[("disk-0", 2 * CHUNK), ("disk-1", 2 * CHUNK)]).await;
    let data = big_bytes(CHUNK + 333);
    put_bytes(&rig.store, "project/p1/keep-me", data.clone(), None).await;
    rig.store
        .keep(&pk("exec/c1/also"), KeepTtl::Secs { secs: 60 })
        .await
        .err(); // exec/c1/also doesn't exist; ignore
    put_bytes(&rig.store, "exec/c1/scratch", big_bytes(10), None).await;
    let listed_before = rig.store.list_all().await;

    // Pod restart: fresh Store over the same disks.
    rig.reopen().await;
    let listed_after = rig.store.list_all().await;
    assert_eq!(listed_before, listed_after, "scan reproduces the index exactly");

    let (_, stream) = rig.store.get(&pk("project/p1/keep-me"), None).await.expect("get after rebuild");
    assert_eq!(collect_stream(stream).await.unwrap(), data);
}

#[tokio::test]
async fn size_mismatch_fails_the_read_loudly() {
    let mut rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/f", big_bytes(100), None).await;
    // Sabotage: delete the chunk file behind the index's back, then
    // reopen so the scan sees meta(100 bytes) with no chunks.
    rig.pool.remove_file_for_test("disk-0", &weft_storage::index::chunk_path("exec/c1/f", 0));
    rig.reopen().await;
    match rig.store.get(&pk("exec/c1/f"), None).await {
        Err(StoreError::Corrupt(msg)) => assert!(msg.contains("refusing"), "{msg}"),
        Err(other) => panic!("expected Corrupt, got {other}"),
        Ok(_) => panic!("expected Corrupt, got Ok"),
    }
}

#[tokio::test]
async fn evacuation_moves_chunks_then_releases_the_disk() {
    let rig = StorageTestRig::with_disks(&[("disk-0", 2 * CHUNK), ("disk-1", 2 * CHUNK)]).await;
    let data = big_bytes(CHUNK / 2);
    put_bytes(&rig.store, "project/p1/f", data.clone(), None).await;
    // Find which disk got it; evacuate that one.
    let victim = if rig.pool.file_count("disk-0") > 1 { "disk-0" } else { "disk-1" };
    rig.store.evacuate(victim).await.expect("evacuate");
    // Disk asked back; file fully readable from its new home.
    assert!(rig
        .pool
        .calls()
        .contains(&PoolCall::DiskRemoveRequested { disk: victim.to_string() }));
    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get after evacuate");
    assert_eq!(collect_stream(stream).await.unwrap(), data);
}

#[tokio::test]
async fn interrupted_evacuation_resumes_with_no_chunk_lost_or_duplicated() {
    // Asymmetric sizes so BOTH chunks land on disk-0 (always the
    // roomiest), making it a two-chunk evacuation victim.
    let mut rig =
        StorageTestRig::with_disks(&[("disk-0", 6 * CHUNK), ("disk-1", 3 * CHUNK)]).await;
    let data = big_bytes(2 * CHUNK); // 2 chunks
    put_bytes(&rig.store, "project/p1/f", data.clone(), None).await;
    let victim = "disk-0";
    assert!(rig.pool.file_count("disk-0") >= 3, "both chunks + meta on disk-0");

    // Interrupt: writes during evacuate are (draining marker,
    // chunk-0 copy, chunk-1 copy, ...); let two succeed so the
    // SECOND chunk copy fails (first chunk moved + source deleted;
    // second still on the victim).
    rig.pool.fail_write_after(2);
    let err = rig.store.evacuate(victim).await;
    assert!(err.is_err(), "evacuation reports the injected failure");
    // Every byte still readable mid-interruption.
    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get mid-evacuation");
    assert_eq!(collect_stream(stream).await.unwrap(), data);

    // Restart: scan resolves duplicates toward the non-draining
    // disk, the watcher path (here: direct call) resumes and
    // finishes.
    rig.reopen().await;
    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get after restart");
    assert_eq!(collect_stream(stream).await.unwrap(), data);
    rig.store.evacuate(victim).await.expect("resume evacuation");
    assert!(rig
        .pool
        .calls()
        .contains(&PoolCall::DiskRemoveRequested { disk: victim.to_string() }));
    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get after resume");
    assert_eq!(collect_stream(stream).await.unwrap(), data);
}

#[tokio::test]
async fn get_stream_survives_a_concurrent_evacuation_of_its_chunks() {
    use futures::StreamExt;
    // A 2-chunk file all on disk-0 (the roomiest). Open a get
    // stream, pull the FIRST chunk, THEN evacuate disk-0 (moving both
    // chunks to disk-1 and deleting the sources). The stream's later
    // chunk reads must re-resolve to disk-1 and still return the exact
    // bytes: a torn or wrong read would corrupt the download.
    // disk-0 much roomier so it is always the placement target (both
    // chunks + meta land there), making it the evacuation victim.
    let rig = StorageTestRig::with_disks(&[("disk-0", 8 * CHUNK), ("disk-1", 3 * CHUNK)]).await;
    let data = big_bytes(2 * CHUNK);
    put_bytes(&rig.store, "project/p1/f", data.clone(), None).await;
    assert!(rig.pool.file_count("disk-0") >= 3, "both chunks + meta on disk-0");

    let (_, stream) = rig.store.get(&pk("project/p1/f"), None).await.expect("get");
    let mut stream = stream;
    // Pull the first chunk's bytes (one whole CHUNK).
    let mut collected: Vec<u8> = Vec::new();
    while (collected.len() as u64) < CHUNK {
        let piece = stream.next().await.expect("first chunk piece").expect("ok");
        collected.extend_from_slice(&piece);
    }
    assert_eq!(collected.len() as u64, CHUNK);

    // Evacuate disk-0 while the stream is parked between chunks.
    rig.store.evacuate("disk-0").await.expect("evacuate");

    // Drain the rest: the second chunk now lives on disk-1, reached
    // via the re-resolve path.
    while let Some(piece) = stream.next().await {
        collected.extend_from_slice(&piece.expect("ok after evacuation"));
    }
    assert_eq!(&collected[..], &data[..], "byte-identical across the evacuation");
}

#[tokio::test]
async fn failed_put_leaves_nothing_behind() {
    let rig = StorageTestRig::new().await;
    let baseline = rig.pool.file_count("disk-0");
    // A stream that errors mid-way.
    let bad: weft_core::storage::ByteStream = Box::pin(futures::stream::iter(vec![
        Ok(bytes::Bytes::from_static(b"good piece")),
        Err(std::io::Error::other("upstream died")),
    ]));
    let err = rig
        .store
        .put(&pk("exec/c1/broken"), "text/plain", "b.txt", None, bad)
        .await;
    assert!(err.is_err());
    assert_eq!(rig.pool.file_count("disk-0"), baseline, "no junk left");
    assert!(rig.store.meta(&pk("exec/c1/broken")).await.is_none());
}

#[tokio::test]
async fn put_conflicting_key_is_rejected() {
    let rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/dup", big_bytes(5), None).await;
    let err = rig
        .store
        .put(&pk("exec/c1/dup"), "text/plain", "d", None, bytes_stream(big_bytes(5)))
        .await;
    assert!(matches!(err, Err(StoreError::Conflict(_))));
}

#[tokio::test]
async fn terminate_sweep_deletes_unkept_and_preserves_kept() {
    let rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/scratch", big_bytes(10), None).await;
    put_bytes(&rig.store, "exec/c1/survivor", big_bytes(10), Some(KeepTtl::Default)).await;
    put_bytes(&rig.store, "exec/OTHER/elsewhere", big_bytes(10), None).await;
    put_bytes(&rig.store, "project/p1/persistent", big_bytes(10), None).await;

    let swept = rig.store.sweep_exec("c1").await.expect("sweep");
    assert_eq!(swept, 1);
    assert!(rig.store.meta(&pk("exec/c1/scratch")).await.is_none(), "un-kept swept");
    assert!(rig.store.meta(&pk("exec/c1/survivor")).await.is_some(), "kept survives in place");
    assert!(rig.store.meta(&pk("exec/OTHER/elsewhere")).await.is_some(), "other colors untouched");
    assert!(rig.store.meta(&pk("project/p1/persistent")).await.is_some(), "project untouched");
    // Idempotent.
    assert_eq!(rig.store.sweep_exec("c1").await.unwrap(), 0);
}

#[tokio::test]
async fn ttl_sweep_deletes_past_due_and_access_bumps() {
    let rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/touched", big_bytes(5), Some(KeepTtl::Secs { secs: 100 }))
        .await;
    put_bytes(&rig.store, "exec/c1/untouched", big_bytes(5), Some(KeepTtl::Secs { secs: 100 }))
        .await;
    put_bytes(&rig.store, "exec/c1/forever", big_bytes(5), Some(KeepTtl::Never)).await;

    // Touch one at t+60 (bumps to t+160).
    rig.advance(std::time::Duration::from_secs(60));
    let (_, s) = rig.store.get(&pk("exec/c1/touched"), None).await.unwrap();
    collect_stream(s).await.unwrap();

    // At t+120: untouched (expiry t+100) dies, touched survives.
    rig.advance(std::time::Duration::from_secs(60));
    let swept = rig.store.expiry_sweep().await.unwrap();
    assert_eq!(swept, 1);
    assert!(rig.store.meta(&pk("exec/c1/untouched")).await.is_none());
    assert!(rig.store.meta(&pk("exec/c1/touched")).await.is_some());

    // Far future: touched expires too; Never never does.
    rig.advance(std::time::Duration::from_secs(100_000));
    rig.store.expiry_sweep().await.unwrap();
    assert!(rig.store.meta(&pk("exec/c1/touched")).await.is_none());
    assert!(rig.store.meta(&pk("exec/c1/forever")).await.is_some(), "Never never expires");
}

#[tokio::test]
async fn touch_access_bump_survives_restart() {
    // The bump rewrites the meta FILE, not just RAM: prove it by
    // restarting between bump and sweep.
    let mut rig = StorageTestRig::new().await;
    put_bytes(&rig.store, "exec/c1/f", big_bytes(5), Some(KeepTtl::Secs { secs: 100 })).await;
    rig.advance(std::time::Duration::from_secs(60));
    rig.store.touch_access(&pk("exec/c1/f")).await.unwrap(); // expiry now t+160
    rig.reopen().await;
    rig.advance(std::time::Duration::from_secs(60)); // t+120 < 160
    assert_eq!(rig.store.expiry_sweep().await.unwrap(), 0);
    assert!(rig.store.meta(&pk("exec/c1/f")).await.is_some());
}

#[tokio::test]
async fn empty_file_round_trips() {
    let rig = StorageTestRig::new().await;
    let meta = put_bytes(&rig.store, "exec/c1/empty", bytes::Bytes::new(), None).await;
    assert_eq!(meta.size_bytes, 0);
    let (_, stream) = rig.store.get(&pk("exec/c1/empty"), None).await.unwrap();
    assert_eq!(collect_stream(stream).await.unwrap().len(), 0);
}

#[tokio::test]
async fn pool_full_fails_put_loudly_and_cleanly() {
    let rig = StorageTestRig::with_disks(&[("disk-0", CHUNK)]).await;
    let err = rig
        .store
        .put(
            &pk("exec/c1/too-big"),
            "application/octet-stream",
            "big",
            None,
            bytes_stream(big_bytes(CHUNK * 2)),
        )
        .await;
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("full") || msg.contains("free bytes"), "{msg}");
    assert!(rig.store.meta(&pk("exec/c1/too-big")).await.is_none());
}

// A terminate sweep collects its un-kept victims, then deletes them. A
// `keep` that lands AFTER collection but BEFORE the delete must spare
// the file (it is now kept). Before the conditional-delete fix the sweep
// deleted off its stale list and destroyed the just-kept file. Run the
// keep and the sweep CONCURRENTLY many times; whenever the file ends up
// kept it must still exist. Multi-thread + a loop so the race window is
// actually exercised (a single-threaded run would serialize them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_racing_a_terminate_sweep_is_never_destroyed() {
    for i in 0..200 {
        let rig = StorageTestRig::new().await;
        let key = format!("exec/c1/f{i}");
        put_bytes(&rig.store, &key, big_bytes(64), None).await;

        let s1 = rig.store.clone();
        let k = pk(&key);
        let keeper = tokio::spawn(async move { s1.keep(&k, KeepTtl::Default).await });
        let s2 = rig.store.clone();
        let sweeper = tokio::spawn(async move { s2.sweep_exec("c1").await });

        let kept = keeper.await.unwrap();
        let _ = sweeper.await.unwrap();

        // The keep succeeded (the file existed when keep ran), so the
        // file is kept and MUST survive the concurrent sweep.
        if kept.is_ok() {
            assert!(
                rig.store.meta(&pk(&key)).await.is_some(),
                "iter {i}: a kept file was destroyed by a concurrent terminate sweep"
            );
        }
    }
}

// A re-`keep` renews a kept file's TTL (and atomically reports NotFound
// if the file is already gone, under the write lock). An expiry sweep
// collects expired victims, then deletes; a renewal that lands between
// collection and delete must spare the file. The fix re-checks expiry
// under the write lock at delete time. Race a renewing keep against an
// expiry sweep over an already-expired file, many times: whenever the
// keep reports success the renewal DID land in the index, so the file
// was non-expired and must survive the sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewal_racing_an_expiry_sweep_is_never_destroyed() {
    for i in 0..200 {
        let rig = StorageTestRig::new().await;
        let key = format!("exec/c1/f{i}");
        // Kept with a finite TTL, then let it pass its expiry so the
        // sweep sees it as a victim at collection time.
        put_bytes(&rig.store, &key, big_bytes(64), Some(KeepTtl::Secs { secs: 60 })).await;
        rig.advance(std::time::Duration::from_secs(120));

        // The renewal sets expiry to now + ttl (future); race it with the
        // sweep that already considers the file expired. `keep` errors
        // NotFound (atomically) if the sweep already removed the entry,
        // so an Ok return means the renewal genuinely landed.
        let s1 = rig.store.clone();
        let k = pk(&key);
        let renewer = tokio::spawn(async move { s1.keep(&k, KeepTtl::Secs { secs: 60 }).await });
        let s2 = rig.store.clone();
        let sweeper = tokio::spawn(async move { s2.expiry_sweep().await });

        let renewed = renewer.await.unwrap();
        let _ = sweeper.await.unwrap();

        // A successful renewal landed a future expiry in the index, so
        // the file must not have been swept.
        if renewed.is_ok() {
            assert!(
                rig.store.meta(&pk(&key)).await.is_some(),
                "iter {i}: a renewed kept file was destroyed by a concurrent expiry sweep"
            );
        }
    }
}
