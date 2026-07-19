//! What a tenant is allowed to store in the runtime-file plane: the two
//! hard caps the broker enforces at put time, plus the policy seam that
//! resolves a tenant's caps.
//!
//! The caps are pure data + the floor rule deriving the file cap from the
//! disk quota. The VALUES come from the `EntitlementSource` seam: the default is
//! a generous host-bounded `LocalEntitlementSource`; a source that varies caps
//! per tenant can be supplied instead. This module is the shape + the rule + the
//! seam; it has no idea what a plan is.
//!
//! Enforcement lives on the broker because the broker IS the runtime-file
//! data path (it signs every bucket write), so the quota is checked where
//! the bytes flow, against the tenant's live usage in `runtime_file`.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::runtime_store::charged_bytes_for;

/// Take the per-tenant STORAGE advisory lock inside `tx`, held until the tx
/// ends. THE single definition of the account-wide storage budget's lock key:
/// every path that reads a tenant's charged bytes and then writes must take
/// this first, so the check-and-charge is atomic per tenant. The runtime-file
/// plane AND any other plane that charges the same account-wide budget call
/// THIS, so they contend on one key and neither can pass a stale usage read.
///
/// `hashtextextended(<tenant>, 0)` over the bare tenant id: a 64-bit key (the
/// 32-bit `hashtext` collides between two DIFFERENT tenants at birthday-bound
/// odds around tens of thousands of tenants, silently serializing unrelated
/// tenants). The task-store also uses `hashtextextended(_, 0)` but over a
/// different string, so it does NOT contend here (only the hash function is
/// shared, not the key).
pub async fn lock_tenant_storage(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(tenant)
        .execute(&mut **tx)
        .await
        .context("tenant storage lock")?;
    Ok(())
}

/// The two hard caps a tenant's plan grants. Enforced at PUT against the
/// tenant's live usage (`runtime_file` per-tenant sum): the file cap is the
/// billion-files spam defense (it also bounds the metadata a list returns);
/// the disk-bytes cap is the cost ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entitlement {
    /// Max stored bytes across ALL the tenant's runtime files.
    #[serde(rename = "diskBytesCap")]
    pub disk_bytes_cap: u64,
    /// Max number of runtime files. The floor rule (below) keeps this
    /// consistent with the byte cap: a tenant must be able to fill their
    /// quota with 1 MiB files.
    #[serde(rename = "fileCap")]
    pub file_cap: u64,
}

/// 1 MiB: the smallest file size the floor rule budgets for. "Give the
/// tenant N bytes of quota -> they must be able to fill it with 1 MiB
/// files" => `file_cap = disk_bytes_cap / ONE_MIB`.
pub const ONE_MIB: u64 = 1024 * 1024;

impl Entitlement {
    /// Build an entitlement from a disk-bytes cap, deriving the file cap by
    /// the floor rule (`disk_bytes_cap / 1 MiB`, at least 1). This is the ONE
    /// place the "5 GB must hold 5120 1 MB files" rule lives; every
    /// `EntitlementSource` builds through here so the two caps can't drift.
    pub fn from_disk_bytes(disk_bytes_cap: u64) -> Self {
        Self { disk_bytes_cap, file_cap: (disk_bytes_cap / ONE_MIB).max(1) }
    }

    /// Would storing one more file (bringing the tenant to `count+1` files)
    /// exceed the file cap? Pure; the put path checks this BEFORE writing a
    /// byte so a billion-files spam never lands an object.
    pub fn file_count_would_exceed(&self, current_count: u64) -> bool {
        current_count + 1 > self.file_cap
    }

    /// Would the tenant's stored bytes, after adding `incoming`, exceed the
    /// disk-bytes cap? Pure; the put path checks the tenant's already-stored
    /// total against the incoming size up front.
    pub fn disk_bytes_would_exceed(&self, current_bytes: u64, incoming: u64) -> bool {
        current_bytes.saturating_add(incoming) > self.disk_bytes_cap
    }
}

/// The storage-budget policy: the tenant's plan caps AND the ONE definition of
/// their total stored bytes across the WHOLE account. The disk cap is one
/// account-wide number: every byte the tenant stores, in any storage plane,
/// draws from the same budget, so every quota check and every usage readout
/// counts the whole account, the same way. This trait IS that one way; nobody
/// re-sums usage on the side.
///
/// [`Self::account_used_bytes`] reads on a CALLER-SUPPLIED transaction, so a
/// plane enforcing the quota reads the whole-account total UNDER ITS OWN LOCK,
/// on the same connection, at the instant it is about to charge its own bytes.
/// That is the whole point: one fresh combined total, never "my bytes fresh +
/// the other plane's sampled seconds ago" (which would let two planes'
/// concurrent uploads both pass a stale read and overshoot the shared cap).
/// The returned total ALREADY includes the calling plane's own committed bytes,
/// so a check is just `account_used_bytes + incoming > cap`.
///
/// The default `LocalEntitlementSource` has ONE plane (the runtime-file table),
/// so its account total is that plane's charged bytes. A source with more
/// planes sums them all in its impl.
#[async_trait::async_trait]
pub trait EntitlementSource: Send + Sync {
    /// The tenant's plan caps. Async so a plan-driven source can resolve the
    /// tenant's live plan (a lookup, possibly remote); the default local
    /// source answers from memory.
    async fn caps(&self, tenant: &str) -> Result<Entitlement>;

    /// The tenant's TOTAL currently-stored bytes across every storage plane the
    /// deployment has, read on `tx` so it composes into the caller's locked
    /// transaction. THE single account-usage definition.
    async fn account_used_bytes(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
    ) -> anyhow::Result<u64>;
}

/// Default local disk-bytes cap: 2 TiB. A self-hoster's store is bounded by
/// their actual disk, so the cap is a high ceiling
/// that still makes the spam defense + the accounting real (one code path,
/// no `if local`). A self-hoster with a smaller/bigger disk overrides
/// `WEFT_LOCAL_DISK_BYTES_CAP`.
const DEFAULT_LOCAL_DISK_BYTES_CAP: u64 = 2 * (1 << 40); // 2 TiB

/// The default source: every tenant (effectively one, `local`) gets a generous
/// host-bounded entitlement.
pub struct LocalEntitlementSource {
    default: Entitlement,
}

impl LocalEntitlementSource {
    /// `disk_bytes_cap` defaults to `DEFAULT_LOCAL_DISK_BYTES_CAP` (2 TiB)
    /// unless `WEFT_LOCAL_DISK_BYTES_CAP` overrides it. The cap exists so the
    /// accounting + spam-defense path is always exercised, on one code path.
    pub fn from_env() -> Self {
        let disk_bytes_cap = std::env::var("WEFT_LOCAL_DISK_BYTES_CAP")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LOCAL_DISK_BYTES_CAP);
        Self { default: Entitlement::from_disk_bytes(disk_bytes_cap) }
    }
}

#[async_trait::async_trait]
impl EntitlementSource for LocalEntitlementSource {
    async fn caps(&self, _tenant: &str) -> Result<Entitlement> {
        Ok(self.default)
    }

    async fn account_used_bytes(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
    ) -> anyhow::Result<u64> {
        // One plane here: the runtime-file table's charged bytes ARE the account.
        charged_bytes_for(&mut **tx, tenant).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_rule_derives_file_cap_from_bytes() {
        let e = Entitlement::from_disk_bytes(5 * 1024 * 1024 * 1024); // 5 GiB
        assert_eq!(e.file_cap, 5120);
        // At least 1 file even for a sub-MiB cap.
        assert_eq!(Entitlement::from_disk_bytes(1).file_cap, 1);
    }

    #[test]
    fn caps_reject_at_the_boundary() {
        let e = Entitlement { disk_bytes_cap: 100, file_cap: 2 };
        // bytes
        assert!(!e.disk_bytes_would_exceed(40, 60)); // exactly at cap is OK
        assert!(e.disk_bytes_would_exceed(40, 61));
        // saturating add never wraps
        assert!(e.disk_bytes_would_exceed(u64::MAX, 1));
        // file count
        assert!(!e.file_count_would_exceed(1)); // -> 2, at cap
        assert!(e.file_count_would_exceed(2)); // -> 3, over
    }
}
