//! Shared Google Docs plumbing: the end-of-body text insert both the
//! create and append nodes write through.

use serde_json::json;

use weft::reqwest_middleware::ClientWithMiddleware;
use weft::WeftResult;

/// One end-of-body insert through `batchUpdate`.
pub async fn append_text(http: &ClientWithMiddleware,
    doc_id: &str,
    text: &str,
) -> WeftResult<()> {
    weft::access::client::json_call(
        http.post(format!(
            "https://docs.googleapis.com/v1/documents/{doc_id}:batchUpdate"
        ))
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
