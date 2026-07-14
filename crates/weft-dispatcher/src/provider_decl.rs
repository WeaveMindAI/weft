//! Per-binary provider declarations: for every worker binary registered
//! here, which paid service each of its node types declared in its source's
//! metadata (`provider: { name, base_url }`, own or inherited from the
//! package root's defaults), keyed by the binary's content hash.
//!
//! Written ONLY at registration, from the build's own walk of the node
//! sources (`BuildPlan::provider_decls`); a workload can never write it. The
//! broker reads it when forwarding a deployment-key request: the calling
//! pod's dispatcher-stamped `worker_pod.binary_hash` names the binary, and
//! this table answers "what URL did THAT node's declared source specify for
//! that provider". Content-addressed on `binary_hash` (the declaration lives
//! in the hashed node source, so same binary = same declarations): a
//! re-register of the same binary is a no-op and two projects sharing a
//! binary share the rows.
// The broker-side reader lives in crates/weft-broker/src/provider_proxy.rs
// (`declared_base_url`); it reads this table by raw SQL over the shared
// Postgres, the same way the deployment-key policies read `worker_pod`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::postgres::PgPool;

use weft_core::node::ProviderDecl;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS provider_declaration (
            binary_hash TEXT NOT NULL,
            node_type TEXT NOT NULL,
            provider TEXT NOT NULL,
            base_url TEXT NOT NULL,
            PRIMARY KEY (binary_hash, node_type)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create provider_declaration")?;
    Ok(())
}

/// Record a build's provider declarations (`node_type -> decl`, from
/// `BuildPlan::provider_decls`). An empty map is a valid no-op: most
/// binaries call no paid provider.
/// All-or-nothing: the whole map is recorded in ONE transaction, so a
/// disagreement on the Nth declaration leaves NOTHING written, never a mix of
/// two decl sets for one binary.
pub async fn record(
    pool: &PgPool,
    binary_hash: &str,
    provider_decls: &BTreeMap<String, ProviderDecl>,
) -> Result<()> {
    let mut tx = pool.begin().await.context("begin provider_declaration tx")?;
    for (node_type, decl) in provider_decls {
        // Content-addressed on `binary_hash`: the same binary carries the same
        // declarations, so a row already present for this
        // `(binary_hash, node_type)` must AGREE. Agreement is a no-op;
        // disagreement means the declarations drifted from the binary they
        // claim, which fails loud rather than silently keeping either row.
        //
        // The no-op `DO UPDATE` (setting the column to itself) makes the
        // statement ALWAYS return a row: the freshly inserted one, or the
        // existing one it conflicted with. That is what `DO NOTHING` cannot do
        // (it returns nothing on conflict, forcing a second read that could
        // race), so one statement decides insert-vs-compare with no window.
        let (provider, base_url): (String, String) = sqlx::query_as(
            "INSERT INTO provider_declaration (binary_hash, node_type, provider, base_url)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (binary_hash, node_type)
                 DO UPDATE SET provider = provider_declaration.provider
             RETURNING provider, base_url",
        )
        .bind(binary_hash)
        .bind(node_type)
        .bind(&decl.name)
        .bind(&decl.base_url)
        .fetch_one(&mut *tx)
        .await
        .context("record provider_declaration")?;
        if provider != decl.name || base_url != decl.base_url {
            anyhow::bail!(
                "provider declaration for node '{node_type}' in binary {binary_hash} \
                 disagrees with the recorded one: recorded ({provider}, {base_url}) vs \
                 ({}, {}); a binary's declarations must match",
                decl.name,
                decl.base_url
            );
        }
    }
    tx.commit().await.context("commit provider_declaration tx")?;
    Ok(())
}
