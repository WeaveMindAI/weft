//! GoogleDriveDownload self-tests: binary vs Google-native (export)
//! and the no-default-export refusal.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveDownloadNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_binary_file_streams_via_alt_media", binary_download),
        NodeTest::fake("a_google_doc_exports_as_pdf", native_export),
        NodeTest::fake("an_unknown_native_kind_without_format_refuses", no_default_export),
        NodeTest::live("one_real_native_export", "google", live_export).with_fixture(
            fixture_spec(
                "GOOGLE_SHEET_ID",
                "Spreadsheet id",
                "A Google Sheet the test exports as CSV: the id from its URL.",
            ),
        ),
    ]
}

async fn binary_download(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files/f1?fields=name,mimeType&supportsAllDrives=true",
        json!({ "name": "photo.png", "mimeType": "image/png" }),
    );
    rig.respond_raw("GET", "/drive/v3/files/f1?alt=media", 200, "image/png", b"PNGBYTES".to_vec());
    let outcome = rig
        .run(
            &GoogleDriveDownloadNode,
            json!({ "account": rig.access("google"), "fileId": "f1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("photo.png"));
    assert_eq!(outcome.outputs["mimeType"], json!("image/png"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(8));
    Ok(())
}

async fn native_export(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files/d1?fields=name,mimeType&supportsAllDrives=true",
        json!({ "name": "Report", "mimeType": "application/vnd.google-apps.document" }),
    );
    rig.respond_raw(
        "GET",
        "/drive/v3/files/d1/export?mimeType=application%2Fpdf",
        200,
        "application/pdf",
        b"%PDF".to_vec(),
    );
    let outcome = rig
        .run(
            &GoogleDriveDownloadNode,
            json!({ "account": rig.access("google"), "fileId": "d1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("Report.pdf"), "export gets the extension");
    assert_eq!(outcome.outputs["mimeType"], json!("application/pdf"));
    Ok(())
}

async fn no_default_export(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files/x1?fields=name,mimeType&supportsAllDrives=true",
        json!({ "name": "Form", "mimeType": "application/vnd.google-apps.form" }),
    );
    let outcome = rig
        .run(
            &GoogleDriveDownloadNode,
            json!({ "account": rig.access("google"), "fileId": "x1" }),
        )
        .await;
    let err = outcome.result.expect_err("no default export must refuse").to_string();
    assert!(err.contains("no default export"), "{err}");
    Ok(())
}

async fn live_export(rig: LiveRig) -> WeftResult<()> {
    // Read-only: export the dedicated test spreadsheet (a sheet the
    // connected account can read; its first tab holds a header row
    // and at least one data row) through the default CSV export. The
    // download lands in execution storage, which the runner discards.
    let sheet = rig.fixture("GOOGLE_SHEET_ID")?;
    let outcome = rig
        .run(
            &GoogleDriveDownloadNode,
            json!({ "account": rig.access("google"), "fileId": sheet }),
        )
        .await
        .ok()?;
    assert_eq!(
        outcome.output("mimeType")?.as_str().expect("mime"),
        "text/csv",
        "a native sheet exports as CSV by default"
    );
    assert!(
        outcome.output("filename")?.as_str().expect("filename").ends_with(".csv"),
        "the export gets the CSV extension"
    );
    assert!(
        outcome.output("sizeBytes")?.as_f64().expect("size") > 0.0,
        "the export carries bytes"
    );
    Ok(())
}
