//! BaileyBridge self-tests: the declared infrastructure shape, and the
//! handle it shares with every other WhatsApp node.

use serde_json::json;

use weft::infra::InfraHandle;
use weft::node_test::NODE_UNDER_TEST_ID;
use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyBridgeNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("provisions_one_recreate_deployment_with_auth_volume", provisions),
        NodeTest::fake("shares_its_api_endpoint_as_an_infra_handle", shares_its_endpoint),
    ]
}

async fn provisions(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&BaileyBridgeNode, json!({})).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(spec.units.len(), 1);
    let unit = &spec.units[0];
    assert_eq!(unit.name, "bridge");
    assert_eq!(unit.containers.len(), 1);
    let container = &unit.containers[0];
    assert_eq!(container.ports.len(), 1);
    assert_eq!(container.ports[0].port, 8090);
    assert!(
        container.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry { name, value } if name == "PORT" && value == "8090")),
        "the env PORT matches the container port"
    );
    assert_eq!(spec.volumes.len(), 1, "the session auth volume persists");
    assert!(!spec.endpoints.is_empty(), "the bridge exports its endpoint");
    Ok(())
}

async fn shares_its_endpoint(rig: FakeRig) -> WeftResult<()> {
    rig.declare_endpoint("api", "http://bridge.example:8090");
    rig.answer_endpoint(
        "api",
        EndpointMethod::Get,
        "/outputs",
        json!({ "status": "connected", "jid": "49151@s.whatsapp.net", "bridge": "smuggled" }),
    );
    let outcome = rig.run(&BaileyBridgeNode, json!({})).await.ok()?;
    // The handle names the endpoint, never its address, and the
    // container's own keys cannot shadow it.
    let handle = InfraHandle::from_value(&outcome.outputs["bridge"])?;
    assert_eq!(handle, InfraHandle::new(NODE_UNDER_TEST_ID, "api", None));
    assert_eq!(outcome.outputs["status"], json!("connected"));
    Ok(())
}
