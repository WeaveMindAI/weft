//! GitHubAccess: emits the connected GitHub account's access value.
//! The runtime already sealed the connect handle + service into the
//! `account` input's bag value (from the widget's stamped metadata),
//! so the body is a pure pass-through: no service name, no secrets.

weft::access_node!(GitHubAccessNode);
