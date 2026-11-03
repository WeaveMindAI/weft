//! Shared S3 addressing for every node in this package.
//!
//! The nodes address the stand-in base `https://storage/...`, aimed at
//! the real endpoint by the access's BaseUrl step. S3 keys legally
//! contain spaces, `?`, `#`, `+`, and non-ASCII, so every segment goes
//! through the core URL builder, which encodes each one whole. A name
//! no address can carry (a bucket that is empty, `.` or `..`, a key
//! with a `.` or `..` segment, which URL normalization collapses into a
//! different address) is refused by each node's rules when it is
//! written, and by the builder when it arrives on a wire.

use weft::access::client::{url, Url};
use weft::WeftResult;

/// The stand-in base every S3 node addresses; the connection's
/// BaseUrl step rewrites it to the real endpoint. Private: every node
/// addresses through [`object_url`] / [`bucket_url`], never the raw
/// base.
const STORAGE_BASE: &str = "https://storage";

/// The address of one bucket (the list door), for the caller to add
/// its query to.
pub fn bucket_url(bucket: &str) -> WeftResult<Url> {
    url(STORAGE_BASE, &[bucket])
}

/// The address of one object: base + bucket + the key, each of the
/// key's `/`-separated parts encoded (`/` kept as the separator, so
/// "folder" keys, `photos/` included, keep their shape).
pub fn object_url(bucket: &str, key: &str) -> WeftResult<Url> {
    bucket_url(bucket)?.key_path(key)
}

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block (node self-tests in a
// `tests.rs` belong to nodes; see docs/src/nodes/testing.md).
#[cfg(test)]
mod url_tests {
    use super::*;

    #[test]
    fn an_object_key_keeps_its_folders_and_encodes_each_segment() {
        assert_eq!(object_url("b", "a b/c?d#e").unwrap().as_str(), "https://storage/b/a%20b/c%3Fd%23e");
        assert_eq!(object_url("b", "photos/").unwrap().as_str(), "https://storage/b/photos/");
    }

    #[test]
    fn a_bucket_query_is_encoded() {
        assert_eq!(
            bucket_url("b").unwrap().query("list-type", 2).query("prefix", "a b/c").as_str(),
            "https://storage/b?list-type=2&prefix=a%20b%2Fc"
        );
    }
}
