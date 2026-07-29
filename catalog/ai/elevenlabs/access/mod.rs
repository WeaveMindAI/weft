//! ElevenLabsAccess: the ONE place the project's ElevenLabs
//! connection is picked; the marker passes downstream and consuming
//! nodes open it at call time. The service name lives once, in
//! metadata.json.

weft::access_node!(ElevenLabsAccessNode, "connection");
