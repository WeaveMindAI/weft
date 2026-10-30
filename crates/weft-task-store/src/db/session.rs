//! A connection of its own, outside every pool: the process's one
//! `LISTEN` connection (`crate::pg_signal`), and a one-off question asked
//! before any pool exists (`crate::budget`).

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Notification, Row};

use super::text::nul_free;
use super::{Client, Error, Param, Result};

/// How long opening a session may take, sign-in included.
const CONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);

/// One connection to the database, with what it hears. Dropped, it closes.
pub struct Session {
    client: tokio_postgres::Client,
    /// What the connection hears, in order, until it ends: a notification,
    /// or the error that ended it.
    heard: mpsc::UnboundedReceiver<std::result::Result<Notification, tokio_postgres::Error>>,
    /// Drives the connection: every statement and every notification goes
    /// through it.
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl Session {
    /// Open a connection as `config` says (`super::config`), within
    /// [`CONNECT_WITHIN`]: a server that takes the connection but never
    /// answers its sign-in fails as unreachable instead of holding the
    /// caller for good.
    pub async fn connect(config: &tokio_postgres::Config) -> Result<Self> {
        let Ok(connected) = tokio::time::timeout(CONNECT_WITHIN, config.connect(super::tls::connector())).await else {
            return Err(Error::NoConnection { why: super::NoConnection::Unreachable, refused: None });
        };
        let (client, mut connection) = connected?;
        let (tell, heard) = mpsc::unbounded_channel();
        // tokio-postgres hands a connection's messages to whoever polls it
        // (`Connection::poll_message`), which is also what runs its
        // statements.
        let driver = tokio::spawn(async move {
            let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
            while let Some(message) = messages.next().await {
                match message {
                    Ok(AsyncMessage::Notification(notification)) => {
                        // Nobody reading any more is a session being dropped.
                        let _ = tell.send(Ok(notification));
                    }
                    Ok(AsyncMessage::Notice(notice)) => {
                        tracing::debug!(target: "weft_task_store::db", %notice, "the database said");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tell.send(Err(e));
                        return;
                    }
                }
            }
        });
        Ok(Self { client, heard, driver })
    }

    /// Start hearing what is notified on each of `channels`.
    pub async fn listen<'c>(&self, channels: impl IntoIterator<Item = &'c str>) -> Result<()> {
        let statements: String = channels.into_iter().map(|channel| format!("LISTEN {};", quoted(channel))).collect();
        self.batch_execute(&statements).await
    }

    /// The next notification on a channel this session listens on;
    /// `None` once the connection closed.
    pub async fn notified(&mut self) -> Result<Option<Notification>> {
        match self.heard.recv().await {
            Some(heard) => Ok(Some(heard?)),
            None => Ok(None),
        }
    }
}

/// `name` as a quoted SQL identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[async_trait]
impl Client for Session {
    async fn query(&self, sql: &str, params: &[Param<'_>]) -> Result<Vec<Row>> {
        Ok(self.client.query_raw(sql, nul_free(params)).await?.try_collect().await?)
    }

    async fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        Ok(self.client.execute_raw(sql, nul_free(params)).await?)
    }

    async fn batch_execute(&self, sql: &str) -> Result<()> {
        self.client.batch_execute(sql).await.map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_channel_is_quoted_as_an_identifier() {
        assert_eq!(quoted("weft_task"), "\"weft_task\"");
        assert_eq!(quoted("a\"b"), "\"a\"\"b\"");
    }
}
