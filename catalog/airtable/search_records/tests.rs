//! AirtableSearchRecords self-tests: the built query and the paging.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{
    delete_record, seed_record, unique_name, BASE_FIXTURE, BASE_LABEL, TABLE_FIXTURE, TABLE_LABEL,
};

use super::AirtableSearchRecordsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("filters_and_pages_the_records", searches),
        NodeTest::live("one_real_filtered_search", "airtable", live_search)
            .with_fixture(fixture_spec(BASE_FIXTURE, BASE_LABEL.0, BASE_LABEL.1))
            .with_fixture(fixture_spec(TABLE_FIXTURE, TABLE_LABEL.0, TABLE_LABEL.1)),
    ]
}

/// One real filtered search: a record seeded under a run-unique name
/// is the only match for its own formula, then it is deleted so the
/// table stays empty. Airtable bills nothing for this.
async fn live_search(rig: LiveRig) -> WeftResult<()> {
    let base = rig.fixture(BASE_FIXTURE)?;
    let table = rig.fixture(TABLE_FIXTURE)?;
    let conn = rig.connect().await?;
    let name = unique_name("weft search test");
    let record_id = seed_record(&conn, &base, &table, &name).await?;
    let outcome = rig
        .run(
            &AirtableSearchRecordsNode,
            json!({
                "account": rig.access("airtable"),
                "base": base,
                "table": table,
                "filterByFormula": format!("{{Name}} = '{name}'"),
                "maxRecords": 10,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("count")?.as_f64(), Some(1.0), "exactly the seeded row matches");
    assert_eq!(outcome.output("records")?[0]["id"].as_str(), Some(record_id.as_str()));
    delete_record(&conn, &base, &table, &record_id).await
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
