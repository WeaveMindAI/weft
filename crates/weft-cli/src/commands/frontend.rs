//! `weft frontend add|ls|rm|token`: the project's frontends, each a
//! caller of the install with a token of its own, scoped to the project.
//!
//! With `--repo`, the install hosts it: it makes a Cloud Run service, and
//! lets that repository's CI deploy to it (`weft target export` then hands
//! the repository the service and the token). Without, it runs wherever
//! you run it, and needs only its token and the install's address.
//!
//! A token is shown once, so it goes to a file only you can read, never
//! to the terminal.

use anyhow::{Context, Result};
use weft_core::frontend::{AddFrontendRequest, Frontend, FrontendHost, FrontendWithToken, Repository};

use super::Ctx;

pub enum FrontendAction {
    Add { name: String, repo: Option<String> },
    List,
    Rm { name: String, force: bool },
    Token { name: String, done: Option<uuid::Uuid> },
}

/// The id GitHub gave `repo` (`owner/name`), read with `gh`: access is
/// granted to it, so a name taken again by somebody else gets nothing.
pub(crate) fn repository(repo: &str) -> Result<Repository> {
    weft_core::frontend::check_repo(repo).map_err(anyhow::Error::msg)?;
    let out = std::process::Command::new("gh")
        .args(["api", &format!("repos/{repo}"), "--jq", ".id"])
        .output()
        .context("run gh (the GitHub CLI) to read the repository's id; install it and log in")?;
    anyhow::ensure!(
        out.status.success(),
        "gh cannot read {repo} (not logged in, a typo, or no access): {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    let id = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .with_context(|| format!("gh named no numeric id for {repo}"))?;
    Ok(Repository { name: repo.to_string(), id })
}

pub async fn run(ctx: Ctx, action: FrontendAction) -> Result<()> {
    let (client, id, project) = super::resolve_project(&ctx)?;
    let (install_url, _) = ctx.install_access()?;
    let install_url = install_url.to_string();
    let base = format!("/projects/{id}/frontends");
    match action {
        FrontendAction::Add { name, repo } => {
            let host = if repo.is_some() { FrontendHost::CloudRun } else { FrontendHost::External };
            let repo = repo.as_deref().map(repository).transpose()?;
            let body = AddFrontendRequest { name, host, repo };
            // Refused here, with the flag still in the person's hand.
            body.validate().map_err(anyhow::Error::msg)?;
            // A frontend is often the first thing a project gets on an
            // install, before anything has run there, and it needs only
            // the project to exist, not a build of it.
            super::ensure::ensure_project_known(&ctx).await?;
            let hosted = body.host == FrontendHost::CloudRun;
            let asked = serde_json::to_value(&body)?;
            let made = client.post_json(&base, &asked);
            // A hosted frontend's service takes Cloud Run a minute or two
            // to make, and the call answers only once it is ready.
            let made = if hosted {
                eprintln!("making the frontend's Cloud Run service (this takes a minute or two)");
                crate::progress::while_waiting(made, std::time::Duration::from_secs(15), async |elapsed| {
                    eprintln!("still making the frontend's Cloud Run service ({}s so far)", elapsed.as_secs())
                })
                .await
            } else {
                made.await
            };
            let made: FrontendWithToken =
                serde_json::from_value(made?).context("read the frontend the install made")?;
            hand_over(&ctx, &project, &install_url, made)?;
        }
        FrontendAction::Token { name, done: None } => {
            // A hosted frontend's token is its workflow's: a new one made
            // here would reach no server.
            let frontends: Vec<Frontend> =
                serde_json::from_value(client.get_json(&base).await?).context("read the project's frontends")?;
            if let Some(Frontend { repo: Some(repo), .. }) = frontends.iter().find(|f| f.name == name) {
                anyhow::bail!(
                    "the install hosts frontend '{name}', and its token is the deploy workflow's: in {}, {} gives \
                     the workflow a new one, and its next deploy puts it in place",
                    repo.name,
                    super::target::export_command(ctx.project()?, ctx.on())
                );
            }
            let renewed: FrontendWithToken =
                serde_json::from_value(client.post_json(&format!("{base}/{name}/token"), &serde_json::json!({})).await?)
                    .context("read the frontend's new token")?;
            let id = renewed.token_id;
            hand_over(&ctx, &project, &install_url, renewed)?;
            if !ctx.json() {
                println!(
                    "its old token keeps working until the new one is in place; then `weft frontend token {name} --done {id}` retires it"
                );
            }
        }
        FrontendAction::Token { name, done: Some(id) } => {
            client.post_empty(&format!("{base}/{name}/token/{id}/done")).await?;
            if !ctx.json_out(&serde_json::json!({ "inPlace": id }))? {
                println!("frontend '{name}' calls with token {id}; every other token of it no longer works");
            }
        }
        FrontendAction::List => {
            let frontends: Vec<Frontend> =
                serde_json::from_value(client.get_json(&base).await?).context("read the project's frontends")?;
            if ctx.json_out(&frontends)? {
                return Ok(());
            }
            if frontends.is_empty() {
                println!("{project} has no frontend (`weft frontend add <name>` makes one)");
            }
            for f in &frontends {
                match (&f.repo, &f.url) {
                    (Some(repo), Some(url)) => println!("{:<20} on the install, deployed by {}, at {url}", f.name, repo.name),
                    (Some(repo), None) => println!("{:<20} on the install, deployed by {}", f.name, repo.name),
                    _ => println!("{:<20} runs elsewhere", f.name),
                }
            }
        }
        FrontendAction::Rm { name, force } => {
            let left: Vec<String> = serde_json::from_value(
                client.delete_json(&format!("{base}/{name}{}", if force { "?force=true" } else { "" })).await?,
            )
            .context("read what removing the frontend left")?;
            if !ctx.json_out(&serde_json::json!({ "removed": name, "left": left }))? {
                println!("removed frontend '{name}': its tokens no longer work, and a service the install made for it is gone");
                for line in left {
                    eprintln!("warning: {line}");
                }
            }
        }
    }
    Ok(())
}

/// Put a fresh token where only this person can read it, and say what
/// goes where. A frontend the install hosts gets no file: its service
/// gets a token of its own from the repository's workflow (`weft target
/// export` mints it, the deploy puts it in place and retires every other),
/// so one written here would only sit on disk until that deploy killed it.
fn hand_over(ctx: &Ctx, project: &str, install_url: &str, made: FrontendWithToken) -> Result<()> {
    let f = &made.frontend;
    if let (Some(repo), Some(service)) = (&f.repo, &f.service) {
        if ctx.json_out(&serde_json::json!({ "frontend": f, "tokenId": made.token_id }))? {
            return Ok(());
        }
        println!("frontend '{}': the install made its service {service}, and {} may deploy to it", f.name, repo.name);
        if let Some(url) = &f.url {
            println!("visitors reach it at {url}");
        }
        println!(
            "next, in {}: {} hands its deploy workflow the service and a token",
            repo.name,
            super::target::export_command(ctx.project()?, ctx.on())
        );
        return Ok(());
    }
    // A frontend running elsewhere reaches the install at its public
    // address.
    let env = vec![
        ("WEFT_TOKEN", made.token.clone()),
        ("WEFT_DISPATCHER_URL", install_url.to_string()),
        ("WEFT_PUBLIC_URL", install_url.to_string()),
    ];
    let file = super::target::write_secrets_file(&format!("{project}-frontend-{}", f.name), &env)?;
    if ctx.json_out(&serde_json::json!({ "frontend": f, "tokenId": made.token_id, "tokenFile": file }))? {
        return Ok(());
    }
    println!("frontend '{}': its token is in {} (readable by you only; shown this once)", f.name, file.display());
    println!("put that file's three variables in its server's environment: it calls the install at {install_url} with that token");
    Ok(())
}
