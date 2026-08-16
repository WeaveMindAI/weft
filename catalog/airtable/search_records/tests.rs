//! AirtableSearchRecords self-tests: the built query and the paging.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AirtableSearchRecordsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("filters_and_pages_the_records", searches)]
}

async fn searches(rig: FakeRig) -> WeftResult<()> {
    let q = "maxRecords=150&pageSize=100&filterByFormula=%7BStage%7D%20%3D%20%27Lead%27";
    rig.respond(
        "GET",
        &format!("/v0/appB/tblT?{q}"),
        json!({ "records": [{ "id": "rec1", "fields": {} }], "offset": "page2" }),
    );
    rig.respond(
        "GET",
        &format!("/v0/appB/tblT?{q}&offset=page2"),
        json!({ "records": [{ "id": "rec2", "fields": {} }] }),
    );
    let outcome = rig
        .run(
            &AirtableSearchRecordsNode,
            json!({
                "account": rig.access("airtable"),
                "base": "appB",
                "table": "tblT",
                "filterByFormula": "{Stage} = 'Lead'",
                "maxRecords": 150,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0));
    assert_eq!(outcome.outputs["records"][1]["id"], json!("rec2"));
    Ok(())
}
