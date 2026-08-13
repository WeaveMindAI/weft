//! Reading the email connection's stored fields, in one place.
//!
//! The connection stores `smtp_host`/`smtp_port`, `imap_host`/
//! `imap_port`, `user`, `password`, and the optional `send_as` alias
//! (the recipe in the access metadata names them; the IMAP trigger's
//! declared dialogue interpolates the same names as templates). Both
//! imperative readers live here so the field vocabulary and the port
//! parse exist once.

use weft::access::OpenedConnection;
use weft::{WeftError, WeftResult};

/// The SMTP submission half: server, port, credentials, and the
/// address the message says it is FROM (the connection's `send_as`
/// alias when it set one, else the signing-in account).
pub struct Smtp {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub sender: String,
}

pub fn smtp(conn: &OpenedConnection) -> WeftResult<Smtp> {
    let user = conn.value("user")?.to_string();
    let sender = conn.opt_value("send_as").map(str::trim).unwrap_or("");
    let sender = if sender.is_empty() { user.clone() } else { sender.to_string() };
    Ok(Smtp {
        host: conn.value("smtp_host")?.to_string(),
        port: conn.value("smtp_port")?.trim().parse().map_err(|_| {
            WeftError::Input(
                "the connection's SMTP port is not a number; reconnect it with a numeric port"
                    .to_string(),
            )
        })?,
        user,
        password: conn.value("password")?.to_string(),
        sender,
    })
}

/// The IMAP half: `host:port` dial address plus credentials.
pub struct Imap {
    pub address: String,
    pub user: String,
    pub password: String,
}

pub fn imap(conn: &OpenedConnection) -> WeftResult<Imap> {
    Ok(Imap {
        address: format!("{}:{}", conn.value("imap_host")?, conn.value("imap_port")?),
        user: conn.value("user")?.to_string(),
        password: conn.value("password")?.to_string(),
    })
}
