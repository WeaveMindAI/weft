//! OpenRouterAccess: the ONE place the project's OpenRouter connection
//! is picked. The `connection` input's stored `{id, identity}` handle
//! becomes the full `Access` marker when the bag is built (service
//! stamped from metadata); the node just passes it downstream.
//! Consuming nodes open the connection at call time, inside their own
//! firing (which is what keeps a runtime-supplied credential's
//! per-firing lifecycle intact). No service name is written here: it
//! lives once, in metadata.json.

weft::access_node!(OpenRouterAccessNode, "connection");
