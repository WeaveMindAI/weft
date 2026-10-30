//! Running statements: the one way every crate talks to the install's
//! database, whatever it holds (a pool, a connection, a transaction).

use async_trait::async_trait;
use futures::TryStreamExt as _;
use tokio_postgres::types::FromSql;
use tokio_postgres::Row;

use super::text::nul_free;
use super::{Conn, Error, Param, Pool, Result};

/// Something statements run on: a [`Pool`] (each statement on whichever
/// connection it hands out), a [`Conn`] held for several, a
/// [`Transaction`], or a [`super::Session`] of its own. A connection keeps
/// each statement it ran prepared, so running one again costs a single
/// round trip.
///
/// A function that runs statements takes `&impl Client`, so its caller
/// decides whether they run inside a transaction.
#[async_trait]
pub trait Client: Send + Sync {
    /// Every row `sql` returns.
    async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<Row>>;

    /// Run `sql`, for how many rows it touched.
    async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64>;

    /// Run `sql`, which may hold several statements and takes no
    /// parameters (a schema, a script), unprepared.
    async fn batch_execute(&self, sql: &str) -> Result<()>;

    /// Every row `sql` returns, as `T`.
    async fn all<T: FromRow>(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<T>> {
        self.query(sql, params).await?.into_iter().map(T::from_row).collect()
    }

    /// The first row `sql` returns, as `T`; [`Error::NoRow`] when it
    /// returns none.
    async fn one<T: FromRow>(&self, sql: &str, params: &[Param<'_>]) -> Result<T> {
        self.opt(sql, params).await?.ok_or(Error::NoRow)
    }

    /// The first row `sql` returns, as `T`, if it returns one.
    async fn opt<T: FromRow>(&self, sql: &str, params: &[Param<'_>]) -> Result<Option<T>> {
        self.query(sql, params).await?.into_iter().next().map(T::from_row).transpose()
    }

    /// The first column of every row `sql` returns.
    async fn scalars<T>(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<T>>
    where
        T: for<'r> FromSql<'r> + Send,
    {
        self.query(sql, params).await?.iter().map(|row| row.try_get(0).map_err(Error::from)).collect()
    }

    /// The first column of the first row `sql` returns;
    /// [`Error::NoRow`] when it returns none.
    async fn scalar<T>(&self, sql: &str, params: &[Param<'_>]) -> Result<T>
    where
        T: for<'r> FromSql<'r> + Send,
    {
        self.scalar_opt(sql, params).await?.ok_or(Error::NoRow)
    }

    /// The first column of the first row `sql` returns, if it returns one.
    async fn scalar_opt<T>(&self, sql: &str, params: &[Param<'_>]) -> Result<Option<T>>
    where
        T: for<'r> FromSql<'r> + Send,
    {
        self.query(sql, params).await?.first().map(|row| row.try_get(0).map_err(Error::from)).transpose()
    }
}

/// A row read as a value: a whole [`Row`], a tuple of its columns in
/// order, or a type of weft's that names its columns.
pub trait FromRow: Sized + Send {
    fn from_row(row: Row) -> Result<Self>;
}

impl FromRow for Row {
    fn from_row(row: Row) -> Result<Self> {
        Ok(row)
    }
}

macro_rules! tuple_from_row {
    ($($column:tt $t:ident),+) => {
        impl<$($t),+> FromRow for ($($t,)+)
        where
            $($t: for<'r> FromSql<'r> + Send),+
        {
            fn from_row(row: Row) -> Result<Self> {
                Ok(($(row.try_get::<_, $t>($column)?,)+))
            }
        }
    };
}

tuple_from_row!(0 A);
tuple_from_row!(0 A, 1 B);
tuple_from_row!(0 A, 1 B, 2 C);
tuple_from_row!(0 A, 1 B, 2 C, 3 D);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N, 14 O);
tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F, 6 G, 7 H, 8 I, 9 J, 10 K, 11 L, 12 M, 13 N, 14 O, 15 P);

/// A transaction on a [`Conn`] ([`Conn::transaction`]). Dropped without
/// [`Self::commit`], it rolls back.
pub struct Transaction<'a> {
    tx: deadpool_postgres::Transaction<'a>,
}

impl<'a> Transaction<'a> {
    pub(super) fn new(tx: deadpool_postgres::Transaction<'a>) -> Self {
        Self { tx }
    }

    pub async fn commit(self) -> Result<()> {
        Ok(self.tx.commit().await?)
    }

    pub async fn rollback(self) -> Result<()> {
        Ok(self.tx.rollback().await?)
    }
}

#[async_trait]
impl Client for Pool {
    async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<Row>> {
        self.get().await?.query(sql, params).await
    }

    async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        self.get().await?.execute(sql, params).await
    }

    async fn batch_execute(&self, sql: &str) -> Result<()> {
        self.get().await?.batch_execute(sql).await
    }
}

#[async_trait]
impl Client for Conn {
    async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<Row>> {
        let statement = self.object.prepare_cached(sql).await?;
        Ok(self.object.query_raw(&statement, nul_free(params)).await?.try_collect().await?)
    }

    async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        let statement = self.object.prepare_cached(sql).await?;
        Ok(self.object.execute_raw(&statement, nul_free(params)).await?)
    }

    async fn batch_execute(&self, sql: &str) -> Result<()> {
        Ok(self.object.batch_execute(sql).await?)
    }
}

#[async_trait]
impl Client for Transaction<'_> {
    async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<Row>> {
        let statement = self.tx.prepare_cached(sql).await?;
        Ok(self.tx.query_raw(&statement, nul_free(params)).await?.try_collect().await?)
    }

    async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        let statement = self.tx.prepare_cached(sql).await?;
        Ok(self.tx.execute_raw(&statement, nul_free(params)).await?)
    }

    async fn batch_execute(&self, sql: &str) -> Result<()> {
        Ok(self.tx.batch_execute(sql).await?)
    }
}
