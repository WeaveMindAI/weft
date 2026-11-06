//! CodexAccess: the ONE place a Codex login is picked, a ChatGPT login
//! file or an API key; the marker passes on `access` to the Codex nodes,
//! which hand it to the agent. A ChatGPT login is renewed by weft alone
//! (its way's `renewal`: OpenAI's token endpoint and Codex's own public
//! client id, the request Codex itself sends), never by Codex. The
//! service recipe lives once, in metadata.json.

weft::access_node!(CodexAccessNode);
