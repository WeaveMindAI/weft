//! Shared Google Docs plumbing: the end-of-body text insert both the
//! create and append nodes write through.

use serde_json::json;

use weft::reqwest_middleware::ClientWithMiddleware;
use weft::WeftResult;

pub const API: &str = "https://docs.googleapis.com/v1";

/// One end-of-body insert through `batchUpdate`.
pub async fn append_text(http: &ClientWithMiddleware,
    doc_id: &str,
    text: &str,
) -> WeftResult<()> {
    weft::access::client::json_call(
        // `:batchUpdate` is a method suffix on the document's own path
        // segment, so it follows the encoded id unencoded.
        http.post(format!("{}:batchUpdate", weft::access::client::url(API, &["documents", doc_id])?))
        .json(&json!({
            "requests": [{
                "insertText": {
                    "endOfSegmentLocation": { "segmentId": "" },
                    "text": text,
                }
            }]
        })),
        "append to the document",
    )
    .await?;
    Ok(())
}
