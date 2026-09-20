//! Signal-token management. A signal token grants scoped access to
//! the dispatcher's signal enumeration + reply surface (`GET
//! /signal-token/signals`, Bearer-authenticated): a client uses it to
//! LISTEN for a project's waiting nodes and REPLY to them, and to
//! reach what a node is SHOWING (`GET /signal-token/displays`): read
//! its panel and press the buttons its items carry.
//!
//! Three scope dimensions, and one of them reads differently from the
//! other two. `--projects` and `--tags` are empty-means-any. Displays
//! are not: a display can be a credential (a bridge's QR code pairs
//! the account to whoever scans it), so a token that says nothing
//! about displays reaches none. `--displays` opens every display of
//! the token's projects; `--display <node>` opens one, named the way
//! the user writes it and resolved against the project they are in,
//! because an address is only a name inside one project.
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
    #[serde(rename = "allowedDisplays")]
    allowed_displays: Vec<String>,
    #[serde(rename = "allDisplays")]
    all_displays: bool,
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
    #[serde(rename = "allowedDisplays")]
    allowed_displays: Vec<String>,
    #[serde(rename = "allDisplays")]
    all_displays: bool,
}

pub enum TokenAction {
    Mint {
        name: Option<String>,
        projects: Vec<String>,
        tags: Vec<String>,
        /// The nodes whose display this token may reach (`--display`).
        displays: Vec<String>,
        /// Every display in the token's projects (`--displays`).
        all_displays: bool,
    },
    Ls,
    Revoke {
        id: String,
    },
}

pub async fn run(ctx: Ctx, action: TokenAction) -> anyhow::Result<()> {
    let client = ctx.client();
    match action {
        TokenAction::Mint { name, projects, tags, displays, all_displays } => {
            let displays = grants_for(&ctx, &displays)?;
            let body = serde_json::json!({
                "name": name,
                "allowedProjects": projects,
                "allowedTags": tags,
                "allowedDisplays": displays,
                "allDisplays": all_displays,
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
            print_scope_summary(
                &minted.allowed_projects,
                &minted.allowed_tags,
                &minted.allowed_displays,
                minted.all_displays,
            );
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
                print_scope_summary(
                    &t.allowed_projects,
                    &t.allowed_tags,
                    &t.allowed_displays,
                    t.all_displays,
                );
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

/// Turn the `--display` names into the grants the dispatcher stores:
/// the project the user is standing in, plus the node as they wrote
/// it.
///
/// The project comes from the folder, never from the person: a node's
/// address is only a name inside one project, and asking somebody to
/// paste a uuid to say where they already are is work a tool should
/// do. Outside a project there is nothing to resolve against, so the
/// answer is to go stand in one.
fn grants_for(ctx: &Ctx, displays: &[String]) -> anyhow::Result<Vec<String>> {
    if displays.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(blank) = displays.iter().find(|node| node.trim().is_empty()) {
        anyhow::bail!(
            "--display takes a node, and this one is empty ('{blank}'). Name it the \
             way you write it in the source, like `whatsapp` or `test.whatsapp`."
        );
    }
    let project = ctx.project().map_err(|e| {
        anyhow::anyhow!(
            "--display names a node of the project you are in, and there is none here: \
             {e}. Run this from the project's folder."
        )
    })?;
    let id = project.id();
    Ok(displays.iter().map(|node| grant_for(&id, node)).collect())
}

/// One grant: the project the person is standing in, and the node the
/// way they wrote it. Spelled by the one function that knows the
/// shape, so this side and the door can never drift.
fn grant_for(project_id: &uuid::Uuid, node: &str) -> String {
    weft_core::live::display_grant(project_id, node)
}

/// A stored grant as a person reads it: the node, without the project
/// half the tool filled in. Everything printed back is the spelling
/// they typed.
///
/// A string that is not a grant cannot be minted (the dispatcher
/// refuses it), so one here is a row from somewhere else. It is named
/// as such rather than printed whole, which would put a project id in
/// front of somebody for the first time.
fn grant_node(grant: &str) -> String {
    match weft_core::live::split_display_grant(grant) {
        Some((_, node)) => node.to_string(),
        None => "(unreadable grant)".to_string(),
    }
}

/// Pretty-print the scope. Projects and tags are empty-means-any and
/// render as "(any)"; displays are the other way round and render as
/// "(none)", which is what a token that was never given one reads.
fn print_scope_summary(
    projects: &[String],
    tags: &[String],
    displays: &[String],
    all_displays: bool,
) {
    eprintln!("  scope:");
    eprintln!("    projects: {}", scope_or_any(projects));
    eprintln!("    tags:     {}", scope_or_any(tags));
    eprintln!(
        "    displays: {}",
        if all_displays {
            "(all)".to_string()
        } else if displays.is_empty() {
            "(none)".to_string()
        } else {
            displays.iter().map(|g| grant_node(g)).collect::<Vec<_>>().join(", ")
        }
    );
}

fn scope_or_any(xs: &[String]) -> String {
    if xs.is_empty() {
        "(any)".into()
    } else {
        xs.join(", ")
    }
}

#[cfg(test)]
mod grant_tests {
    use super::{grant_for, grant_node};

    fn project() -> uuid::Uuid {
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("a fixed id")
    }

    #[test]
    fn a_grant_carries_the_project_the_person_is_standing_in() {
        // They type the node; the project is the folder, because an
        // address only means something inside one. Drop the prefix and
        // the dispatcher has no way to tell two projects' nodes apart.
        assert_eq!(
            grant_for(&project(), "test.whatsapp"),
            "11111111-1111-1111-1111-111111111111/test.whatsapp"
        );
    }

    #[test]
    fn what_is_printed_back_is_what_they_typed() {
        // The project half is the tool's bookkeeping and never theirs
        // to read.
        assert_eq!(grant_node(&grant_for(&project(), "test.whatsapp")), "test.whatsapp");
        assert_eq!(grant_node(&grant_for(&project(), "whatsapp")), "whatsapp");
    }

    #[test]
    fn a_blank_node_is_refused_before_a_grant_is_built() {
        // Otherwise the daemon refuses `<uuid>/` and quotes the whole
        // thing back, which is a project id the person never had and a
        // shape they never typed.
        let ctx = crate::commands::Ctx::new(Some("http://localhost:9999".into()), false);
        let why = format!("{:#}", super::grants_for(&ctx, &["  ".to_string()]).unwrap_err());
        assert!(why.contains("--display takes a node"), "{why}");
        assert!(!why.contains('/'), "no grant shape leaks into it: {why}");
    }

    #[test]
    fn a_row_that_is_not_a_grant_is_named_rather_than_printed() {
        // Minting cannot produce one, so it came from somewhere else.
        // Printing it whole would show a person a project id for the
        // first time, which is the one thing this surface avoids.
        assert_eq!(grant_node("whatsapp"), "(unreadable grant)");
        assert_eq!(grant_node("11111111-1111-1111-1111-111111111111/"), "(unreadable grant)");
    }
}
