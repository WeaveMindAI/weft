//! MistralParseDocument self-tests: the OCR call against canned answers
//! (fake), and one real one-page OCR (live).

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, StoredFile, WeftResult};

use super::MistralParseDocumentNode;

/// A minimal VALID one-page PDF (correct xref offsets, startxref,
/// stream length), enough for a real parser to accept and answer.
const TINY_PDF: &[u8] = include_bytes!("fixture.pdf");

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("reads_the_document_from_its_link_and_emits_the_pages_markdown", ocr),
        NodeTest::fake("an_image_is_sent_as_an_image_url", image_ocr),
        NodeTest::fake("a_document_heavier_than_a_wire_is_stored_whole", heavy_document),
        NodeTest::fake("a_refused_ocr_fails_the_run_when_error_is_unwired", refused_ocr),
        NodeTest::fake("a_refused_ocr_comes_out_on_error_when_it_is_wired", refused_ocr_wired),
        NodeTest::live("one_real_page_ocr", "mistral", live_ocr),
    ]
}

async fn ocr(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/ocr",
        json!({ "pages": [
            { "index": 0, "markdown": "# Page one" },
            { "index": 1, "markdown": "Page two" },
        ]}),
    );
    let file = rig.store_file("doc.pdf", "application/pdf", TINY_PDF.to_vec());
    let outcome = rig
        .run(
            &MistralParseDocumentNode,
            json!({ "account": rig.access("mistral"), "file": file }),
        )
        .await
        .ok()?;
    let markdown = StoredFile::from_value(outcome.output("markdown")?)?;
    assert_eq!((markdown.filename.as_str(), markdown.mime_type.as_str()), ("doc.md", "text/markdown"));
    assert_eq!(&rig.stored_bytes(&markdown.key)?[..], b"# Page one\n\nPage two");
    let pages = StoredFile::from_value(outcome.output("pages")?)?;
    assert_eq!((pages.filename.as_str(), pages.mime_type.as_str()), ("doc.pages.json", "application/json"));
    let listed: serde_json::Value = serde_json::from_slice(&rig.stored_bytes(&pages.key)?).expect("json pages");
    assert_eq!(listed, json!([{ "index": 0, "markdown": "# Page one" }, { "index": 1, "markdown": "Page two" }]));
    assert_eq!(outcome.outputs["pageCount"], json!(2.0));

    let sent = rig.requests();
    assert_eq!(sent.len(), 1, "the OCR call alone: nothing is uploaded to Mistral");
    let document = &sent[0].body.as_ref().expect("ocr body")["document"];
    assert_eq!(document["type"], json!("document_url"));
    let link = document["document_url"].as_str().expect("a link");
    assert!(link.starts_with("data:application/pdf;base64,") || link.starts_with("http"), "{link}");
    Ok(())
}

async fn image_ocr(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/ocr", json!({ "pages": [{ "index": 0, "markdown": "Total: 12" }] }));
    let file = rig.store_file("receipt.png", "image/png", vec![0x89, b'P', b'N', b'G']);
    rig.run(&MistralParseDocumentNode, json!({ "account": rig.access("mistral"), "file": file }))
        .await
        .ok()?;
    let document = &rig.requests()[0].body.clone().expect("ocr body")["document"];
    assert_eq!(document["type"], json!("image_url"));
    assert!(document["image_url"].is_string(), "{document}");
    Ok(())
}

/// Forty pages of 5 KB weigh 200 KB of markdown, twice what a wire
/// carries: every byte of the paid OCR is kept, in the files.
async fn heavy_document(rig: FakeRig) -> WeftResult<()> {
    let pages: Vec<_> = (0..40).map(|i| json!({ "index": i, "markdown": "x".repeat(5 * 1024) })).collect();
    rig.respond("POST", "/v1/ocr", json!({ "pages": pages }));
    let file = rig.store_file("book.pdf", "application/pdf", TINY_PDF.to_vec());
    let outcome = rig
        .run(&MistralParseDocumentNode, json!({ "account": rig.access("mistral"), "file": file }))
        .await
        .ok()?;
    let markdown = StoredFile::from_value(outcome.output("markdown")?)?;
    let stored = rig.stored_bytes(&markdown.key)?;
    assert_eq!(stored.len(), 40 * 5 * 1024 + 39 * 2, "every page, joined by a blank line, uncut");
    assert!(stored.len() > weft::storage::MAX_WIRE_VALUE_BYTES);
    assert_eq!(outcome.outputs["pageCount"], json!(40.0));
    Ok(())
}

async fn refused_ocr(rig: FakeRig) -> WeftResult<()> {
    // Mistral's real refusal envelope carries `detail`, not `message`.
    rig.respond_status("POST", "/v1/ocr", 401, json!({ "detail": "bad key" }));
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

async fn refused_ocr_wired(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status("POST", "/v1/ocr", 401, json!({ "detail": "bad key" }));
    rig.wire_output("_error");
    let file = rig.store_file("doc.pdf", "application/pdf", TINY_PDF.to_vec());
    let outcome = rig
        .run(
            &MistralParseDocumentNode,
            json!({ "account": rig.access("mistral"), "file": file }),
        )
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("bad key"), "{error}");
    for port in ["markdown", "pages", "pageCount"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
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
