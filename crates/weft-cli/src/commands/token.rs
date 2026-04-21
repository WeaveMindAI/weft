//! Extension token management. These are the opaque tokens users
//! paste into the browser extension to grant it access to the
//! dispatcher's pending-task queue.

use super::Ctx;

pub enum TokenAction {
    Mint { name: Option<String> },
    Ls,
    Revoke { token: String },
}

pub async fn run(ctx: Ctx, action: TokenAction) -> anyhow::Result<()> {
    let client = ctx.client();
    match action {
        TokenAction::Mint { name } => {
            let body = serde_json::json!({ "name": name, "metadata": null });
            let resp: serde_json::Value = client.post_json("/ext-tokens", &body).await?;
            let token = resp.get("token").and_then(|v| v.as_str()).unwrap_or("");
            println!("{token}");
            eprintln!("Paste into the browser extension as:");
            eprintln!("  {}/ext/{token}", client.base());
            Ok(())
        }
        TokenAction::Ls => {
            let resp: serde_json::Value = client.get_json("/ext-tokens").await?;
            let arr = resp.as_array().cloned().unwrap_or_default();
            if arr.is_empty() {
                println!("(no tokens)");
                return Ok(());
            }
            for t in arr {
                let token = t.get("token").and_then(|v| v.as_str()).unwrap_or("");
                let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("");
                println!("{token}  {name}");
            }
            Ok(())
        }
        TokenAction::Revoke { token } => {
            client.delete(&format!("/ext-tokens/{token}")).await?;
            println!("revoked");
            Ok(())
        }
    }
}
