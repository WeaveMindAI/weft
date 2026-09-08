//! Signal-token management. A signal token grants scoped access to
//! the dispatcher's signal enumeration + reply surface (`GET
//! /signal-token/signals`, Bearer-authenticated): a client uses it to
//! LISTEN for a project's waiting nodes and REPLY to them. Each token
//! carries two independent scope vectors (allowed_projects,
//! allowed_tags); empty vector = wildcard for that dimension.
//!
//! Show-once: mint prints the full token exactly once, as the
//! paste-able connect string `<base>/signal-token/<token>` and again
//! bare on a second line for a script; the server keeps only a hash and
//! can never show it again. `ls` shows metadata + the
//! recognizer prefix; `revoke` addresses the token's id.
//!
//! Tokens are general (not tied to one consumer kind). An external
//! listener picks the `human_in_the_loop` scope at setup time; a
//! future Slack bot would pick its own kind. Same dispatcher surface
//! either way.

use serde::Deserialize;

use super::Ctx;

/// The mint response (`POST /signal-tokens`): the one time the full
/// token is visible. Decoded strictly: a field this command cannot
/// find is a token the user can never obtain, so the shape has to
/// fail loudly rather than print a blank.
#[derive(Deserialize)]
struct Minted {
    id: String,
    token: String,
    name: Option<String>,
    url: String,
    #[serde(rename = "allowedProjects")]
    allowed_projects: Vec<String>,
    #[serde(rename = "allowedTags")]
    allowed_tags: Vec<String>,
}

/// One listed token (`GET /signal-tokens`): metadata and the
/// recognizer prefix, never the secret.
#[derive(Deserialize)]
struct Listed {
    id: String,
    recognizer: String,
    name: Option<String>,
    #[serde(rename = "allowedProjects")]
    allowed_projects: Vec<String>,
    #[serde(rename = "allowedTags")]
    allowed_tags: Vec<String>,
}

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
            let minted: Minted = serde_json::from_value(resp.clone())
                .map_err(|e| anyhow::anyhow!("unexpected /signal-tokens mint response shape: {e}"))?;
            if ctx.json_out(&resp)? {
                return Ok(());
            }
            // The ONE time the full token is visible. The connect string is
            // what a client (e.g. the browser extension) pastes; it parses out
            // the token and presents it via `Authorization: Bearer`. The bare
            // token follows on its own line for a script that wants only it.
            println!("{}", minted.url);
            println!("{}", minted.token);
            eprintln!("Copy it now: the server stores only a hash and cannot show it again.");
            eprintln!("Id: {} (use this to revoke)", minted.id);
            if let Some(name) = minted.name.as_deref().filter(|n| !n.is_empty()) {
                eprintln!("Name: {name}");
            }
            print_scope_summary(&minted.allowed_projects, &minted.allowed_tags);
            Ok(())
        }
        TokenAction::Ls => {
            let resp: serde_json::Value = client.get_json("/signal-tokens").await?;
            let listed: Vec<Listed> = serde_json::from_value(resp.clone())
                .map_err(|e| anyhow::anyhow!("unexpected /signal-tokens listing shape: {e}"))?;
            if ctx.json_out(&resp)? {
                return Ok(());
            }
            if listed.is_empty() {
                println!("(no tokens)");
                return Ok(());
            }
            for (i, t) in listed.iter().enumerate() {
                if i > 0 {
                    println!();
                }
                let name = t.name.as_deref().filter(|n| !n.is_empty()).unwrap_or("(unnamed)");
                println!("{}  {name}", t.recognizer);
                println!("  id: {}", t.id);
                print_scope_summary(&t.allowed_projects, &t.allowed_tags);
            }
            Ok(())
        }
        TokenAction::Revoke { id } => {
            client.delete(&format!("/signal-tokens/{id}")).await?;
            if ctx.json_out(&serde_json::json!({ "revoked": id }))? {
                return Ok(());
            }
            println!("revoked: {id}");
            Ok(())
        }
    }
}

/// Pretty-print the scope vectors. An empty vector is a wildcard and
/// renders as "(any)".
fn print_scope_summary(projects: &[String], tags: &[String]) {
    eprintln!("  scope:");
    eprintln!("    projects: {}", scope_or_any(projects));
    eprintln!("    tags:     {}", scope_or_any(tags));
}

fn scope_or_any(xs: &[String]) -> String {
    if xs.is_empty() {
        "(any)".into()
    } else {
        xs.join(", ")
    }
}
