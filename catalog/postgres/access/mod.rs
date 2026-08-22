//! PostgresAccess: emits the connected database's access value. The
//! query and write nodes read the host, credentials and TLS choice off
//! it at use time; nothing secret rides an edge.

weft::access_node!(PostgresAccessNode);
