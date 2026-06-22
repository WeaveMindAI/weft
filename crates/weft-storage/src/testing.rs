//! Layer-3 rig: the REAL store + service router wired against fake
//! disks, fake auth, and a fake clock. Tests drive HTTP requests
//! through the router with tower's `oneshot`, or call the `Store`
//! directly for placement-level assertions.

use std::sync::Arc;

use weft_platform_traits::{Clock, FakeClock};

use crate::auth::FakeAuth;
use crate::disk::FakeDiskPool;
use crate::key::CallerAuth;
use crate::service::{router, ServiceState};
use crate::store::Store;

pub struct StorageTestRig {
    pub pool: Arc<FakeDiskPool>,
    pub auth: Arc<FakeAuth>,
    pub clock: Arc<FakeClock>,
    pub store: Arc<Store>,
    pub router: axum::Router,
}

impl StorageTestRig {
    /// One 1 GiB disk, a worker token `worker-token` for
    /// (tenant t1, project p1, color c1), a dispatcher token
    /// `dispatcher-token`.
    pub async fn new() -> Self {
        Self::with_disks(&[("disk-0", 1 << 30)]).await
    }

    pub async fn with_disks(disks: &[(&str, u64)]) -> Self {
        let pool = FakeDiskPool::new();
        for (name, cap) in disks {
            pool.add_disk(name, *cap);
        }
        let auth = FakeAuth::new();
        auth.seed(
            "worker-token",
            CallerAuth::Worker {
                tenant: "t1".into(),
                project_id: "p1".into(),
                color: Some("c1".into()),
            },
        );
        auth.seed("dispatcher-token", CallerAuth::ControlPlane);
        let clock = FakeClock::new();
        let store = Arc::new(
            Store::open(pool.clone(), clock.clone())
                .await
                .expect("rig store opens on fresh fakes"),
        );
        let state = Arc::new(ServiceState {
            store: store.clone(),
            auth: auth.clone(),
            box_tenant: "t1".into(),
            public_base_url: "https://t1.example.test/storage".into(),
        });
        Self { pool, auth, clock, store, router: router(state) }
    }

    /// Re-open the store from the SAME fake disks (simulates a pod
    /// restart: index rebuilt by scan, boxstate reloaded).
    pub async fn reopen(&mut self) {
        let store = Arc::new(
            Store::open(self.pool.clone(), self.clock.clone())
                .await
                .expect("rig store reopens from scan"),
        );
        self.store = store.clone();
        let state = Arc::new(ServiceState {
            store,
            auth: self.auth.clone(),
            box_tenant: "t1".into(),
            public_base_url: "https://t1.example.test/storage".into(),
        });
        self.router = router(state);
    }

    pub fn advance(&self, d: std::time::Duration) {
        self.clock.advance(d);
    }

    pub fn now_unix(&self) -> i64 {
        self.clock.now_unix()
    }
}
