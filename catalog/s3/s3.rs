//! Shared S3 addressing for every node in this package.
//!
//! One helper, one contract: `object_url` builds the stand-in address
//! (`https://storage/...`, aimed at the real endpoint by the access's
//! BaseUrl step) for one object, percent-encoding each path segment of
//! the key. S3 keys legally contain spaces, `?`, `#`, `+`, and
//! non-ASCII; interpolating them raw would truncate the path at the
//! first reserved character and address a different object.

/// The stand-in base every S3 node addresses; the connection's
/// BaseUrl step rewrites it to the real endpoint. Private: every node
/// addresses through [`object_url`] / [`bucket_url`], never the raw
/// base.
const STORAGE_BASE: &str = "https://storage";

/// The addressable URL of one bucket, with a percent-encoded query
/// (the list door). Owns the query assembly so no node hand-rolls
/// `push_str` + encode pairs.
pub fn bucket_url(bucket: &str, query: &[(&str, &str)]) -> weft::WeftResult<String> {
    let mut url = format!("{STORAGE_BASE}/{}", checked_bucket(bucket)?);
    for (i, (name, value)) in query.iter().enumerate() {
        let sep = if i == 0 { '?' } else { '&' };
        url.push(sep);
        url.push_str(name);
        url.push('=');
        url.push_str(&urlencoding::encode(value));
    }
    Ok(url)
}

/// Refuse a bucket name that would change the path it is spliced into:
/// empty, or carrying a `/`, `?`, `#`, `\` or `..` (S3 bucket names
/// are lowercase letters, digits, dots and hyphens, so none of these is
/// ever a real bucket).
fn checked_bucket(bucket: &str) -> weft::WeftResult<&str> {
    let clean = !bucket.is_empty()
        && !bucket.contains(['/', '?', '#', '\\'])
        && !bucket.contains("..");
    if !clean {
        return Err(weft::WeftError::Input(format!("'{bucket}' is not a usable S3 bucket name")));
    }
    Ok(bucket)
}

/// The addressable URL of one object: base + bucket + the key with
/// every path segment percent-encoded (`/` kept as the separator, so
/// "folder" keys keep their shape). A key with a bare `.` or `..`
/// segment is refused loudly: URL path normalization collapses those
/// segments (percent-encoding the dots does not survive it either),
/// so the request would silently address a DIFFERENT object.
pub fn object_url(bucket: &str, key: &str) -> weft::WeftResult<String> {
    let mut encoded_key: Vec<String> = Vec::new();
    for segment in key.split('/') {
        if segment == "." || segment == ".." {
            return Err(weft::WeftError::Input(format!(
                "the object key '{key}' contains a '{segment}' path segment, which URL \
                 normalization would collapse into a different key; rename the object"
            )));
        }
        encoded_key.push(urlencoding::encode(segment).into_owned());
    }
    Ok(format!("{STORAGE_BASE}/{}/{}", checked_bucket(bucket)?, encoded_key.join("/")))
}

// This file is a package-level SHARED helper, not a node, so its unit
// tests stay an ordinary `#[cfg(test)]` block (node self-tests in a
// `tests.rs` belong to nodes; see docs/src/nodes/testing.md).
#[cfg(test)]
mod url_tests {
    use super::*;

    #[test]
    fn bucket_url_percent_encodes_the_query() {
        assert_eq!(bucket_url("b", &[]).unwrap(), "https://storage/b");
        assert_eq!(
            bucket_url("b", &[("list-type", "2"), ("prefix", "a b/c")]).unwrap(),
            "https://storage/b?list-type=2&prefix=a%20b%2Fc"
        );
    }

    #[test]
    fn a_bucket_that_would_reshape_the_path_is_refused() {
        for bad in ["", "a/b", "a?b", "a#b", "a..b", "..", "a\\b"] {
            assert!(bucket_url(bad, &[]).is_err(), "{bad:?} was accepted");
            assert!(object_url(bad, "k").is_err(), "{bad:?} was accepted");
        }
        assert!(object_url("my.bucket-1", "k").is_ok());
    }
}
