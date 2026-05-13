//! API token management. Tokens grant scoped access to the
//! dispatcher's signal enumeration surface (`GET /api-token/{tk}/
//! signals`). Each token carries three independent scope vectors;
//! empty vector = wildcard for that dimension.
//!
//! Tokens are general (not tied to one consumer kind). The browser
//! extension picks the `human_in_the_loop` scope at install time;
//! a future Slack bot would pick its own kind. Same dispatcher
//! surface either way.

use super::Ctx;

pub enum TokenAction {
    Mint {
        name: Option<String>,
        hard: bool,
        kinds: Vec<String>,
        projects: Vec<String>,
        tags: Vec<String>,
    },
    Ls,
    Revoke {
        token: String,
    },
}

pub async fn run(ctx: Ctx, action: TokenAction) -> anyhow::Result<()> {
    let client = ctx.client();
    match action {
        TokenAction::Mint { name, hard, kinds, projects, tags } => {
            let style = if hard { "hard" } else { "friendly" };
            let body = serde_json::json!({
                "name": name,
                "metadata": null,
                "style": style,
                "allowedKinds": kinds,
                "allowedProjects": projects,
                "allowedTags": tags,
            });
            let resp: serde_json::Value = client.post_json("/api-tokens", &body).await?;
            let token = resp.get("token").and_then(|v| v.as_str()).unwrap_or("");
            let final_name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
            println!("{token}");
            if !final_name.is_empty() && final_name != token.strip_prefix("wm_tk_").unwrap_or("") {
                eprintln!("Name: {final_name}");
            }
            print_scope_summary(&resp);
            eprintln!("Use this URL to enumerate this token's signals:");
            eprintln!("  {}/api-token/{token}/signals", client.base());
            Ok(())
        }
        TokenAction::Ls => {
            let resp: serde_json::Value = client.get_json("/api-tokens").await?;
            let arr = resp.as_array().cloned().unwrap_or_default();
            if arr.is_empty() {
                println!("(no tokens)");
                return Ok(());
            }
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
                print_scope_summary(&t);
                println!("  {base}/api-token/{token}/signals");
            }
            Ok(())
        }
        TokenAction::Revoke { token } => {
            client.delete(&format!("/api-tokens/{token}")).await?;
            println!("revoked: {token}");
            Ok(())
        }
    }
}

/// Pretty-print the scope vectors. Empty vector renders as
/// "(any)" so the user can tell a wildcard from a missing field.
fn print_scope_summary(t: &serde_json::Value) {
    let kinds = arr_or_any(t.get("allowedKinds"));
    let projects = arr_or_any(t.get("allowedProjects"));
    let tags = arr_or_any(t.get("allowedTags"));
    eprintln!("  scope:");
    eprintln!("    kinds:    {kinds}");
    eprintln!("    projects: {projects}");
    eprintln!("    tags:     {tags}");
}

fn arr_or_any(v: Option<&serde_json::Value>) -> String {
    let arr = v.and_then(|x| x.as_array());
    match arr {
        Some(a) if a.is_empty() => "(any)".into(),
        Some(a) => {
            let xs: Vec<String> = a
                .iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect();
            xs.join(", ")
        }
        None => "(any)".into(),
    }
}
