//! Signal-token management. A signal token grants scoped access to
//! the dispatcher's signal enumeration + reply surface (`GET
//! /signal-token/signals`, Bearer-authenticated): a client uses it to
//! LISTEN for a project's waiting nodes and REPLY to them. Each token
//! carries two independent scope vectors (allowed_projects,
//! allowed_tags); empty vector = wildcard for that dimension.
//!
//! Show-once: mint prints the full token (as the paste-able connect
//! string `<base>/signal-token/<token>`) exactly once; the server keeps
//! only a hash and can never show it again. `ls` shows metadata + the
//! recognizer prefix; `revoke` addresses the token's id.
//!
//! Tokens are general (not tied to one consumer kind). An external
//! listener picks the `human_in_the_loop` scope at setup time; a
//! future Slack bot would pick its own kind. Same dispatcher surface
//! either way.

use super::Ctx;

pub enum TokenAction {
    Mint {
        name: Option<String>,
        projects: Vec<String>,
        tags: Vec<String>,
    },
    Ls,
    Revoke {
        id: String,
    },
}

pub async fn run(ctx: Ctx, action: TokenAction) -> anyhow::Result<()> {
    let client = ctx.client();
    match action {
        TokenAction::Mint { name, projects, tags } => {
            let body = serde_json::json!({
                "name": name,
                "metadata": null,
                "allowedProjects": projects,
                "allowedTags": tags,
            });
            let resp: serde_json::Value = client.post_json("/signal-tokens", &body).await?;
            let url = resp.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let id = resp.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let final_name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
            // The ONE time the full token is visible. The connect string is
            // what a client (e.g. the browser extension) pastes; it parses out
            // the token and presents it via `Authorization: Bearer`.
            println!("{url}");
            eprintln!("Copy it now: the server stores only a hash and cannot show it again.");
            eprintln!("Id: {id} (use this to revoke)");
            if !final_name.is_empty() {
                eprintln!("Name: {final_name}");
            }
            print_scope_summary(&resp);
            Ok(())
        }
        TokenAction::Ls => {
            let resp: serde_json::Value = client.get_json("/signal-tokens").await?;
            let arr = resp.as_array().cloned().unwrap_or_default();
            if arr.is_empty() {
                println!("(no tokens)");
                return Ok(());
            }
            let mut first = true;
            for t in arr {
                let id = t.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let recognizer = t.get("recognizer").and_then(|v| v.as_str()).unwrap_or("");
                let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if !first {
                    println!();
                }
                first = false;
                println!("{recognizer}  {}", if name.is_empty() { "(unnamed)" } else { name });
                println!("  id: {id}");
                print_scope_summary(&t);
            }
            Ok(())
        }
        TokenAction::Revoke { id } => {
            client.delete(&format!("/signal-tokens/{id}")).await?;
            println!("revoked: {id}");
            Ok(())
        }
    }
}

/// Pretty-print the scope vectors. Empty vector renders as
/// "(any)" so the user can tell a wildcard from a missing field.
fn print_scope_summary(t: &serde_json::Value) {
    let projects = arr_or_any(t.get("allowedProjects"));
    let tags = arr_or_any(t.get("allowedTags"));
    eprintln!("  scope:");
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
