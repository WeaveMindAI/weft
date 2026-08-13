//! Boot-time schema guard over the `CREATE TABLE IF NOT EXISTS` trap.
//!
//! Every table is created by idempotent DDL run at boot. That shape has a
//! silent failure mode: when a column is added to a CREATE TABLE in source
//! but the database predates it, the CREATE no-ops (the table "exists") and
//! the running service limps against the stale shape, surfacing only as
//! background WARN spam and hung work. This module turns that into a LOUD
//! boot failure.
//!
//! Mechanism: each module's DDL is declared as a [`SchemaGroup`].
//! [`apply_groups`] fingerprints every group's DDL text and compares it to
//! the fingerprint stamped in `weft_schema_stamp` when the group's tables
//! were created. A match (or a fresh database) runs the DDL as before; a
//! mismatch refuses to run that group and fails the boot with ready-to-run
//! SQL that drops exactly the affected tables. In-place schema edits are
//! deliberately unsupported: the schema is edited in its canonical CREATE
//! TABLE, and stale tables are dropped and recreated fresh.

use sha2::{Digest, Sha256};
use sqlx::PgPool;

/// One module's schema: the exact DDL it runs at boot, plus the tables that
/// DDL creates (named in the mismatch error's DROP commands). Declared as a
/// `static` next to the DDL's owning module.
pub struct SchemaGroup {
    /// Short stable slug, usually the main table name. Keys the stored
    /// fingerprint in `weft_schema_stamp`.
    pub name: &'static str,
    /// Every table the group's ddl creates.
    pub tables: &'static [&'static str],
    /// The exact statements the group runs. An entry may hold multiple
    /// semicolon-separated statements (executed via `raw_sql`).
    pub ddl: &'static [&'static str],
}

/// Group and table names are interpolated into the mismatch error's
/// paste-ready reset SQL, so they must be plain SQL identifiers; a stray
/// quote would hand the operator broken SQL at the worst moment. Static
/// declarations, so this fails the first boot of the build that
/// introduced the bad name.
fn validate_identifiers(groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    for group in groups {
        for name in std::iter::once(group.name).chain(group.tables.iter().copied()) {
            anyhow::ensure!(
                !name.is_empty()
                    && name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "schema group '{}' declares a non-identifier name '{name}'; group and \
                 table names must match [a-z_][a-z0-9_]*",
                group.name
            );
        }
    }
    Ok(())
}

/// Hex sha256 over the group's DDL statements, each followed by a newline.
/// Any textual edit to the DDL (a new column, a changed default, a comment)
/// changes the fingerprint; that is the point: the stamp certifies "the
/// database's tables were created by exactly this text".
fn fingerprint(ddl: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for stmt in ddl {
        hasher.update(stmt.as_bytes());
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Run every group's DDL, guarded by the stored fingerprints.
///
/// Per group: no stored fingerprint means a fresh database, so the DDL runs
/// and the fingerprint is stamped. A matching fingerprint re-runs the DDL
/// anyway (idempotent, and it repairs a manually dropped table). A DIFFERENT
/// fingerprint means the tables were created by a different build of this
/// code: the group's DDL is NOT run, and after the loop the call fails with
/// one error naming every mismatched group and the exact SQL to reset it.
///
/// The whole run executes in one transaction under an advisory lock:
/// `IF NOT EXISTS` is idempotent but not concurrency-safe in Postgres
/// (two backends racing the same CREATE on a fresh database both pass the
/// existence check, then one fails on a duplicate catalog key), so
/// concurrent boots serialize and the losers see the tables present. The
/// transaction also makes each stamp atomic with its DDL: a stamp is never
/// written for DDL that did not run.
pub async fn apply_groups(pool: &PgPool, groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    validate_identifiers(groups)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('weft:schema-migrate', 0))")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS weft_schema_stamp (\
             group_name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)",
    )
    .execute(&mut *tx)
    .await?;

    let mut mismatched: Vec<&SchemaGroup> = Vec::new();
    for group in groups {
        let fp = fingerprint(group.ddl);
        let stored: Option<(String,)> =
            sqlx::query_as("SELECT fingerprint FROM weft_schema_stamp WHERE group_name = $1")
                .bind(group.name)
                .fetch_optional(&mut *tx)
                .await?;
        match stored {
            Some((stored_fp,)) if stored_fp != fp => {
                mismatched.push(group);
                continue;
            }
            _ => {}
        }
        for stmt in group.ddl {
            sqlx::raw_sql(stmt).execute(&mut *tx).await.map_err(|e| {
                anyhow::anyhow!("schema group '{}': DDL failed: {e}", group.name)
            })?;
        }
        if stored.is_none() {
            sqlx::query(
                "INSERT INTO weft_schema_stamp (group_name, fingerprint) VALUES ($1, $2) \
                 ON CONFLICT (group_name) DO UPDATE SET fingerprint = EXCLUDED.fingerprint",
            )
            .bind(group.name)
            .bind(&fp)
            .execute(&mut *tx)
            .await?;
        }
    }

    if !mismatched.is_empty() {
        let names: Vec<&str> = mismatched.iter().map(|g| g.name).collect();
        let mut reset_sql = String::new();
        for group in &mismatched {
            for table in group.tables {
                reset_sql.push_str(&format!("DROP TABLE IF EXISTS {table} CASCADE;\n"));
            }
            reset_sql.push_str(&format!(
                "DELETE FROM weft_schema_stamp WHERE group_name = '{}';\n",
                group.name
            ));
        }
        anyhow::bail!(
            "stale database schema for group(s): {}.\n\
             These tables were created by a different build of this code. In-place \
             schema edits are not supported; the schema is edited in its canonical \
             CREATE TABLE and stale tables are dropped and recreated fresh.\n\
             To reset exactly the affected tables, run:\n\n{}\n\
             then restart the service so the tables are recreated fresh.",
            names.join(", "),
            reset_sql
        );
    }

    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // Layer-1 tests for the pure fingerprint function: same DDL yields the
    // same fingerprint, any textual change (edit, reorder, split) changes it.
    use super::fingerprint;

    #[test]
    fn same_ddl_same_fingerprint() {
        let a = fingerprint(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"]);
        let b = fingerprint(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"]);
        assert_eq!(a, b);
        // Hex sha256: 64 lowercase hex chars.
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn changed_ddl_changes_fingerprint() {
        let base = fingerprint(&["CREATE TABLE t (id INT)"]);
        let edited = fingerprint(&["CREATE TABLE t (id INT, name TEXT)"]);
        assert_ne!(base, edited);
    }

    #[test]
    fn a_non_identifier_name_is_refused() {
        let bad = super::SchemaGroup {
            name: "worker'; DROP TABLE task; --",
            tables: &["worker_pod"],
            ddl: &["CREATE TABLE IF NOT EXISTS worker_pod (id INT)"],
        };
        let err = super::validate_identifiers(&[&bad]).expect_err("a quoted name must refuse");
        assert!(err.to_string().contains("non-identifier"), "{err}");
        let good = super::SchemaGroup {
            name: "worker_pod",
            tables: &["worker_pod", "infra_owner2"],
            ddl: &[],
        };
        super::validate_identifiers(&[&good]).expect("plain identifiers pass");
    }

    #[test]
    fn statement_boundaries_matter() {
        // The per-statement newline separator keeps ["ab"] distinct from
        // ["a", "b"]: concatenation without a separator would collide.
        let joined = fingerprint(&["ab"]);
        let split = fingerprint(&["a", "b"]);
        assert_ne!(joined, split);
    }
}
