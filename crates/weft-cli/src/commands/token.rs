//! Extension token management. These are the opaque tokens users
//! paste into the browser extension to grant it access to the
//! dispatcher's pending-task queue.

use super::Ctx;

pub enum TokenAction {
    Mint { name: Option<String>, hard: bool },
    Ls,
    Revoke { token: String },
}

pub async fn run(ctx: Ctx, action: TokenAction) -> anyhow::Result<()> {
    let client = ctx.client();
    match action {
        TokenAction::Mint { name, hard } => {
            let style = if hard { "hard" } else { "friendly" };
            let body = serde_json::json!({
                "name": name,
                "metadata": null,
                "style": style,
            });
            let resp: serde_json::Value = client.post_json("/ext-tokens", &body).await?;
            let token = resp.get("token").and_then(|v| v.as_str()).unwrap_or("");
            let final_name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
            println!("{token}");
            if !final_name.is_empty() && final_name != token.strip_prefix("wm_tk_").unwrap_or("") {
                eprintln!("Name: {final_name}");
            }
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
            // Three-line layout per entry:
            //   <token>          ← the actual identifier
            //   name: <label>    ← the human label, when set
            //     <paste URL>    ← copy-paste target for the
            //                      browser extension
            // The token comes first because it's the canonical
            // identifier; revoke also accepts the name. The
            // previous layout printed the name as if it were the
            // token, which broke `revoke` for users who copied
            // the displayed string.
            let base = client.base();
            let mut first = true;
            for t in arr {
                let token = t.get("token").and_then(|v| v.as_str()).unwrap_or("");
                let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if !first {
                    println!();
                }
                first = false;
                println!("{token}");
                if !name.is_empty() && name != token.strip_prefix("wm_tk_").unwrap_or("") {
                    println!("  name: {name}");
                }
                println!("  {base}/ext/{token}");
            }
            Ok(())
        }
        TokenAction::Revoke { token } => {
            // The dispatcher accepts either the token string or
            // the human label and returns 404 if nothing matched.
            client.delete(&format!("/ext-tokens/{token}")).await?;
            println!("revoked: {token}");
            Ok(())
        }
    }
}
