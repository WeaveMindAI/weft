//! DiscordWebhookAccess: the ONE place a Discord channel webhook is
//! connected; the marker passes downstream and consuming nodes open
//! it at call time. The webhook URL is the credential (it carries the
//! channel's token), so it lives sealed in the access store and rides
//! as the connection's base URL, never through node config.

weft::access_node!(DiscordWebhookAccessNode);
