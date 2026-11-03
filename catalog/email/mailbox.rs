//! Reading the email connection's stored fields, in one place.
//!
//! The connection stores `smtpHost`/`smtpPort`, `imapHost`/
//! `imapPort`, `user`, `password`, and the optional `sendAs` alias
//! (the recipe in the access metadata names them; the IMAP trigger's
//! declared dialogue interpolates the same names as templates). Both
//! imperative readers live here so the field vocabulary and the port
//! parse exist once.

use weft::access::OpenedConnection;
use weft::WeftResult;

/// The SMTP submission half: server, port, credentials, and the
/// address the message says it is FROM (the connection's `sendAs`
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
    let sender = conn.opt_value("sendAs").map(str::trim).unwrap_or("");
    let sender = if sender.is_empty() { user.clone() } else { sender.to_string() };
    let Ok(port) = conn.value("smtpPort")?.trim().parse::<u16>() else {
        weft::input_bail!("the connection's SMTP port is not a number; reconnect it with a numeric port");
    };
    Ok(Smtp {
        host: conn.value("smtpHost")?.to_string(),
        port,
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
        address: format!("{}:{}", conn.value("imapHost")?, conn.value("imapPort")?),
        user: conn.value("user")?.to_string(),
        password: conn.value("password")?.to_string(),
    })
}
