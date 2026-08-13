//! MistralParseDocument self-tests: the upload -> signed-url -> OCR
//! dance against canned answers (fake), and one real one-page OCR
//! (live).

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::MistralParseDocumentNode;

/// A minimal VALID one-page PDF (correct xref offsets, startxref,
/// stream length), enough for a real parser to accept and answer.
const TINY_PDF: &[u8] = include_bytes!("fixture.pdf");

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("uploads_signs_and_emits_the_pages_markdown", ocr_dance),
        NodeTest::fake("a_refused_upload_fails_loud", refused_upload),
        NodeTest::live("one_real_page_ocr", "mistral", live_ocr),
    ]
}

async fn ocr_dance(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/files", json!({ "id": "file-1" }));
    rig.respond(
        "GET",
        "/v1/files/file-1/url?expiry=1",
        json!({ "url": "https://signed.example/doc" }),
    );
    rig.respond(
        "POST",
        "/v1/ocr",
        json!({ "pages": [
            { "index": 0, "markdown": "# Page one" },
            { "index": 1, "markdown": "Page two" },
        ]}),
    );
    rig.respond("DELETE", "/v1/files/file-1", json!({ "deleted": true }));
    let file = rig.store_file("doc.pdf", "application/pdf", TINY_PDF.to_vec());
    let outcome = rig
        .run(
            &MistralParseDocumentNode,
            json!({ "account": rig.access("mistral"), "file": file }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("# Page one\n\nPage two"));
    assert_eq!(outcome.outputs["pageCount"], json!(2.0));

    let sent = rig.requests();
    assert_eq!(sent.len(), 4, "upload, sign, ocr, delete-upload");
    assert_eq!(sent[3].method, "DELETE", "the upload never outlives the OCR");
    let ocr_body = sent[2].body.as_ref().expect("ocr body");
    assert_eq!(ocr_body["document"]["document_url"], json!("https://signed.example/doc"));
    Ok(())
}

async fn refused_upload(rig: FakeRig) -> WeftResult<()> {
    // Mistral's real refusal envelope carries `detail`, not `message`.
    rig.respond_status("POST", "/v1/files", 401, json!({ "detail": "bad key" }));
    let file = rig.store_file("doc.pdf", "application/pdf", TINY_PDF.to_vec());
    let outcome = rig
        .run(
            &MistralParseDocumentNode,
            json!({ "account": rig.access("mistral"), "file": file }),
        )
        .await;
    let err = outcome.result.expect_err("a 401 must refuse").to_string();
    assert!(err.contains("bad key"), "{err}");
    Ok(())
}

async fn live_ocr(rig: LiveRig) -> WeftResult<()> {
    let file = rig.store_file("doc.pdf", "application/pdf", TINY_PDF.to_vec()).await?;
    let outcome = rig
        .run(
            &MistralParseDocumentNode,
            json!({ "account": rig.access("mistral"), "file": file }),
        )
        .await
        .ok()?;
    let pages = outcome.output("pageCount")?.as_f64().expect("page count");
    assert_eq!(pages, 1.0, "the tiny pdf has one page");
    Ok(())
}
