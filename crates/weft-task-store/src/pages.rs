//! Walking a table's rows in key order a page at a time, each page bounded
//! by the size of what it reads as well as by a row count: the boot's walks
//! over rows holding whole documents (a stored program reaches 19 MB), so
//! that a page of large rows neither exceeds what Postgres takes in one
//! message nor what the process holds at once.
//!
//! A page is measured before any row is read ([`next_page`]): the next
//! rows in key order, each with the size of the rows before it, give the
//! key of the last row the page takes ([`PageCaps`]). The caller then reads
//! (and, where it writes, locks) the rows up to that key with its own
//! query, so the measure needs no lock (a window function cannot run
//! beside `FOR UPDATE`).

use crate::db::{Client, Param};

/// About how many bytes one page holds at the boot's walks: rows are taken
/// while what the page already holds is under it, so the first row always
/// goes (alone, if it is bigger) and a page passes it by at most one row.
/// A page of documents is written back in one statement, and Postgres
/// refuses a message over 1 GB (`invalid message length`): a stored
/// program reaches 19 MB, so a page of 500 of them would never commit, and
/// every boot would try the same page again.
pub const PAGE_BYTES: i64 = 64 * 1024 * 1024;

/// How much one page holds: at most `rows` rows, taken while the rows
/// already in it come to less than `bytes` (the first row always).
#[derive(Debug, Clone, Copy)]
pub struct PageCaps {
    pub rows: i64,
    pub bytes: i64,
}

/// Where a measured page ends: the key of its last row, each part as text,
/// and whether rows are left after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageEnd {
    pub last: Vec<String>,
    pub more: bool,
}

/// Measure the next page of the rows `rows` names (`FROM <table> WHERE
/// ...`, which may name `params` and must leave out the rows before the
/// page), walked in the order of `keys` (the table's key columns), each
/// counted at `octet_length(<size>)`. `size` is the value as the caller
/// reads it, so what is counted is what travels: for TEXT and BYTEA
/// Postgres knows that length without unpacking the value (a JSONB one is
/// rendered to be measured), where `pg_column_size` would give the size
/// stored, compressed, smaller than what travels. `None` when no row is
/// left.
pub async fn next_page(
    db: &impl Client,
    rows: &str,
    keys: &[&str],
    size: &str,
    params: &[Param<'_>],
    caps: PageCaps,
) -> anyhow::Result<Option<PageEnd>> {
    let key_list = keys.join(", ");
    let key_text = keys.iter().map(|key| format!("{key}::text")).collect::<Vec<_>>().join(", ");
    // One row past the row cap says whether any is left after it.
    let measure = format!(
        "SELECT ARRAY[{key_text}] AS key, \
             sum(size) OVER (ORDER BY {key_list} ROWS UNBOUNDED PRECEDING) - size AS before \
         FROM (SELECT {key_list}, octet_length({size}) AS size {rows} ORDER BY {key_list} LIMIT {}) AS candidate \
         ORDER BY {key_list}",
        caps.rows + 1
    );
    let candidates = db.query(&measure, params).await?;
    let before = candidates.iter().map(|row| row.try_get("before")).collect::<Result<Vec<i64>, _>>()?;
    let taken = rows_taken(&before, caps);
    let Some(at) = taken.checked_sub(1) else { return Ok(None) };
    Ok(Some(PageEnd { last: candidates[at].try_get("key")?, more: taken < candidates.len() }))
}

/// How many of the next rows one page takes, given for each, in key order,
/// the size of the rows before it: rows while that is under `caps.bytes`,
/// at most `caps.rows`, and always the first, however big (a page that
/// took none would never move the walk on).
fn rows_taken(before: &[i64], caps: PageCaps) -> usize {
    let fit = before.iter().take(caps.rows.max(1) as usize).take_while(|&&bytes| bytes < caps.bytes).count();
    fit.max(before.len().min(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAPS: PageCaps = PageCaps { rows: 4, bytes: 100 };

    /// The running size before each row, from each row's own size.
    fn before(sizes: &[i64]) -> Vec<i64> {
        sizes.iter().scan(0, |total, size| Some(std::mem::replace(total, *total + size))).collect()
    }

    #[test]
    fn rows_are_taken_while_the_page_is_under_its_bytes() {
        assert_eq!(rows_taken(&before(&[40, 40, 40, 40]), CAPS), 3);
        assert_eq!(rows_taken(&before(&[50, 50, 1]), CAPS), 2);
    }

    #[test]
    fn the_first_row_is_taken_however_big() {
        assert_eq!(rows_taken(&before(&[1_000, 1]), CAPS), 1);
        assert_eq!(rows_taken(&before(&[1_000]), PageCaps { rows: 4, bytes: 0 }), 1);
    }

    #[test]
    fn no_more_rows_than_the_row_cap_are_taken() {
        assert_eq!(rows_taken(&before(&[1; 5]), CAPS), 4);
        assert_eq!(rows_taken(&before(&[1; 3]), CAPS), 3);
    }

    #[test]
    fn no_row_left_takes_none() {
        assert_eq!(rows_taken(&[], CAPS), 0);
    }
}
