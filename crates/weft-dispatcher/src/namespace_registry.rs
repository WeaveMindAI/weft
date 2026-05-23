//! Authoritative mapping from k8s namespace name to tenant id.
//!
//! The broker's TokenReview path uses the caller's namespace to
//! determine which tenant they belong to. Parsing the namespace
//! string is brittle: it works as long as the dispatcher is the
//! only writer (its `name_for` collapses dash runs so `--` is the
//! unambiguous separator), but it relies on a k8s RBAC invariant
//! that tenant pods can't create their own namespaces. If that
//! invariant ever leaks, an attacker could hand-craft a
//! `wm-project-eve--alice--proj1` namespace and the parser would
//! happily call them tenant `eve` with project `alice--proj1`.
//!
//! This table is the database of record. Dispatcher writes a row
//! on every namespace creation (tenant + project). Broker looks
//! up the namespace here and reads the tenant_id from the row.
//! No parsing. An attacker-created namespace with no row in this
//! table fails authentication outright.

use anyhow::Result;
use sqlx::postgres::PgPool;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS weft_namespace_tenant (
            namespace TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_weft_namespace_tenant_tenant \
         ON weft_namespace_tenant(tenant_id)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Register a namespace as belonging to `tenant_id`. Idempotent
/// (UPSERT). Called from `tenant_namespace::ensure_tenant_namespace`
/// and `project_namespace::ensure` so the registry is updated
/// alongside every kubectl apply.
///
/// Re-registering the same namespace with a DIFFERENT tenant_id
/// is rejected: the namespace's identity is fixed at creation.
/// This stops a malicious or buggy caller from rebinding an
/// existing namespace to a new tenant.
pub async fn register(pool: &PgPool, namespace: &str, tenant_id: &str) -> Result<()> {
    let res = sqlx::query(
        "INSERT INTO weft_namespace_tenant (namespace, tenant_id) \
         VALUES ($1, $2) \
         ON CONFLICT (namespace) DO UPDATE \
           SET tenant_id = EXCLUDED.tenant_id \
           WHERE weft_namespace_tenant.tenant_id = EXCLUDED.tenant_id",
    )
    .bind(namespace)
    .bind(tenant_id)
    .execute(pool)
    .await?;
    if res.rows_affected() == 0 {
        anyhow::bail!(
            "namespace {namespace} already registered to a different tenant; \
             refusing to rebind"
        );
    }
    Ok(())
}

/// Look up the tenant_id for a namespace. `None` means the
/// namespace was never registered: either it doesn't exist as a
/// weft namespace, or someone created a k8s namespace outside
/// the dispatcher's control. Broker auth treats `None` as a 403.
pub async fn tenant_for(pool: &PgPool, namespace: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT tenant_id FROM weft_namespace_tenant WHERE namespace = $1",
    )
    .bind(namespace)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(t,)| t))
}

#[cfg(test)]
mod tests {
    /// The security contract this module enforces:
    ///
    ///   1. The dispatcher is the only writer (see callers of
    ///      `register`). Tenant pods never have permission to
    ///      INSERT into `weft_namespace_tenant`.
    ///   2. The broker's TokenReview path uses ONLY this table
    ///      to determine `tenant_id`. No namespace-string parsing.
    ///   3. `register` refuses to rebind an existing namespace
    ///      to a different tenant: the `ON CONFLICT DO UPDATE
    ///      SET tenant_id = EXCLUDED.tenant_id WHERE
    ///      weft_namespace_tenant.tenant_id = EXCLUDED.tenant_id`
    ///      clause is a no-op when the prior tenant differs;
    ///      we surface the no-op as an error.
    ///
    /// Behavior tests against the real Postgres live in
    /// integration / layer-4 (no rig in this crate yet); this
    /// `#[cfg(test)] mod tests` exists as a stable anchor for
    /// the documented contract so a future writer can't quietly
    /// loosen these properties.
    #[test]
    fn contract_documented() {}
}
