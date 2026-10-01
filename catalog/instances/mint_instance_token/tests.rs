//! MintInstanceToken self-tests: the token comes out with its expiry, and
//! a token that never expires is refused.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::MintInstanceTokenNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("mints_for_the_instance", mints),
        NodeTest::fake("a_token_always_expires", never),
        NodeTest::fake("a_replayed_mint_replaces_the_same_token", replayed),
    ]
}

async fn mints(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&MintInstanceTokenNode, json!({ "instance": "ada", "expiresInHours": 2 })).await.ok()?;
    let minted = rig.minted_tokens();
    assert_eq!(minted.len(), 1);
    assert_eq!(minted[0].instance.as_str(), "ada");
    assert_eq!(minted[0].expires_in_secs, 7200);
    assert!(outcome.outputs["token"].as_str().unwrap().starts_with("wft-"));
    Ok(())
}

/// A run replayed past the mint mints again (the value cannot be read
/// back), under the id the first mint journaled: the token is replaced,
/// never doubled.
async fn replayed(rig: FakeRig) -> WeftResult<()> {
    let input = json!({ "instance": "ada", "expiresInHours": 1 });
    rig.run(&MintInstanceTokenNode, input.clone()).await.ok()?;
    rig.run(&MintInstanceTokenNode, input).await.ok()?;
    let minted = rig.minted_tokens();
    assert_eq!(minted.len(), 2);
    assert_eq!(minted[0].id, minted[1].id);
    Ok(())
}

async fn never(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&MintInstanceTokenNode, json!({ "instance": "ada", "expiresInHours": 0 })).await.failure()?;
    assert!(err.contains("always expires"), "{err}");
    assert!(rig.minted_tokens().is_empty());
    Ok(())
}
