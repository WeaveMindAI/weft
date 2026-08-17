//! BaileyBridge self-tests: the declared infrastructure shape.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::BaileyBridgeNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("provisions_one_recreate_deployment_with_auth_volume", provisions)]
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
            weft::infra::EnvEntry::Literal { name, value } if name == "PORT" && value == "8090")),
        "the env PORT matches the container port"
    );
    assert_eq!(spec.volumes.len(), 1, "the session auth volume persists");
    assert!(!spec.endpoints.is_empty(), "the bridge exports its endpoint");
    Ok(())
}
