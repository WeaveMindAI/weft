//! `weft target add|list|show|remove` and `weft login|logout`: the installs a
//! project deploys to, and this person's key for each.
//!
//! A target is a name in the project's `weft.toml` (`[targets.prod] url
//! = "..."`), committed, so the whole team calls the same install the
//! same thing. The key that lets a person act there is theirs alone and
//! lives in `~/.config/weft/credentials.toml` (`crate::credentials`).
//!
//! `weft target export` hands a cloud target to the project's GitHub
//! repository, for the workflow `weft ci add` wrote.

use std::path::Path;

use anyhow::{Context, Result};
use weft_compiler::project::LOCAL_TARGET;
use weft_core::signal_token::{MintTokenRequest, MintedToken, TokenKind, TokenSummary};

use super::Ctx;
use crate::credentials;

pub enum TargetAction {
    Add { name: String, url: String },
    List,
    Show { name: String },
    Remove { name: String },
    Export { name: String, github: bool, front_env: Option<std::path::PathBuf>, frontend: Option<String> },
}

pub async fn run(ctx: Ctx, action: TargetAction) -> Result<()> {
    let project = ctx.project()?;
    let manifest = project.root.join("weft.toml");
    match action {
        TargetAction::Add { name, url } => {
            let url = credentials::url_key(&url)?;
            validate_name(&name)?;
            edit_manifest(&manifest, |doc| set_target(doc, &name, &url))?;
            println!("target '{name}' is {url}; act on it with `--on {name}`");
            if name != LOCAL_TARGET {
                println!("next: weft login {name}");
            }
        }
        TargetAction::Remove { name } => {
            let mut removed = false;
            edit_manifest(&manifest, |doc| {
                removed = remove_target(doc, &name);
                Ok(())
            })?;
            anyhow::ensure!(removed, "no target '{name}' in {}", manifest.display());
            println!("target '{name}' removed (any key you stored for it stays until `weft logout`)");
        }
        TargetAction::Export { name, github, front_env, frontend } => {
            export(project, &name, github, front_env.as_deref(), frontend.as_deref()).await?
        }
        TargetAction::Show { name } => {
            let url = project.target_url(&name).map_err(|e| anyhow::anyhow!("{e}"))?;
            let key = credentials::operator_key_for(&url, Some(&name))?;
            let info = crate::client::DispatcherClient::new(url.clone(), key).get_json("/install").await?;
            if ctx.json_out(&info)? {
                return Ok(());
            }
            let info: weft_core::install::InstallInfo = serde_json::from_value(info)
                .with_context(|| format!("{url} answered /install with something that is not an install description"))?;
            for line in describe_install(&name, &info) {
                println!("{line}");
            }
        }
        TargetAction::List => {
            let stored = credentials::load()?;
            let env = std::env::var(credentials::OPERATOR_KEY_ENV).ok();
            let mut rows: Vec<(String, String)> = project
                .manifest
                .targets
                .keys()
                .map(|name| Ok((name.clone(), project.target_url(name).map_err(|e| anyhow::anyhow!("{e}"))?)))
                .collect::<Result<_>>()?;
            if !project.manifest.targets.contains_key(LOCAL_TARGET) {
                rows.insert(0, (LOCAL_TARGET.to_string(), weft_core::ports::local_public_url().map_err(anyhow::Error::msg)?));
            }
            // Whether a command `--on <name>` would carry a key: decided
            // once, by the same rule a request uses, and shown both ways.
            let rows: Vec<(String, String, bool)> = rows
                .into_iter()
                .map(|(name, url)| {
                    let logged_in = credentials::resolve_key(env.clone(), &stored, &url, Some(&name))?.is_some();
                    Ok((name, url, logged_in))
                })
                .collect::<Result<_>>()?;
            if ctx.json_out(&serde_json::json!(rows
                .iter()
                .map(|(name, url, logged_in)| serde_json::json!({
                    "name": name,
                    "url": url,
                    "loggedIn": logged_in,
                }))
                .collect::<Vec<_>>()))?
            {
                return Ok(());
            }
            for (name, url, logged_in) in rows {
                let login = if logged_in {
                    "  (logged in)".to_string()
                } else if name == LOCAL_TARGET {
                    String::new()
                } else {
                    format!("  (not logged in: weft login {name})")
                };
                println!("{name:<12} {url}{login}");
            }
        }
    }
    Ok(())
}

/// `weft target key`: the address and operator key a request with the
/// same `--on` carries, for the editor, which talks to the install itself.
pub fn key(ctx: &Ctx) -> Result<()> {
    let (url, operator_key) = ctx.install_access()?;
    // SYNC: the answer <-> extension-vscode/src/installs.ts InstallAccess
    println!("{}", serde_json::json!({ "url": url, "operatorKey": operator_key }));
    Ok(())
}

/// `weft login <target>`: store an operator key for that install, after
/// proving it works there. The key is read hidden from the terminal, or
/// from stdin with `--key-stdin` for a script.
pub async fn login(ctx: Ctx, name: String, key_stdin: bool) -> Result<()> {
    let project = ctx.project()?;
    let url = project.target_url(&name).map_err(|e| anyhow::anyhow!("{e}"))?;
    let key = if key_stdin {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).context("read the key from stdin")?;
        line.trim().to_string()
    } else {
        anyhow::ensure!(
            crate::prompt::is_interactive(),
            "no terminal to read the key from; pipe it in with --key-stdin"
        );
        rpassword::prompt_password(format!("operator key for {name} ({url}): "))?
            .trim()
            .to_string()
    };
    anyhow::ensure!(!key.is_empty(), "no key given; nothing was stored");
    // Proven before it is stored, so a typo fails here with the install's
    // own answer rather than on the next deploy.
    let info = crate::client::DispatcherClient::new(url.clone(), Some(key.clone()))
        .get_json("/install")
        .await
        .with_context(|| format!("{url} refused this key; nothing was stored"))?;
    let mut stored = credentials::load()?;
    stored.set(&url, key)?;
    credentials::save(&stored)?;
    println!("logged in to {name} ({url}); act on it with `--on {name}`");
    if let Ok(info) = serde_json::from_value::<weft_core::install::InstallInfo>(info) {
        for line in describe_install(&name, &info).into_iter().skip(1) {
            println!("{line}");
        }
    }
    Ok(())
}

/// Where an install lives, a line each: its address, and on a cloud its
/// project and region, which nothing else on this machine records.
fn describe_install(name: &str, info: &weft_core::install::InstallInfo) -> Vec<String> {
    let mut lines = vec![format!("{name}: {}", info.public_url)];
    match &info.cloud {
        Some(weft_core::install::CloudInstall::Gcp(gcp)) => {
            lines.push(format!("  on GCP: project {}, region {}", gcp.project, gcp.region));
        }
        None => lines.push("  on this machine".to_string()),
    }
    if let Some(source) = &info.source {
        lines.push(format!("  runs weft {} at {}", source.repository, source.commit));
    }
    lines
}

pub async fn logout(ctx: Ctx, name: String) -> Result<()> {
    let project = ctx.project()?;
    let url = project.target_url(&name).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut stored = credentials::load()?;
    if stored.remove(&url)? {
        credentials::save(&stored)?;
        println!("forgot your key for {name} ({url})");
    } else {
        println!("no key stored for {name} ({url}); nothing to forget");
    }
    Ok(())
}

/// Write `secrets` as `KEY=value` lines to a file only this user can read,
/// under the install's own folder, and answer its path.
pub(crate) fn write_secrets_file(target: &str, secrets: &[(&'static str, String)]) -> Result<std::path::PathBuf> {
    use std::io::Write;
    let dir = weft_core::infra::Install::from_env().map_err(anyhow::Error::msg)?.dir().join("exports");
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("{target}.secrets.env"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&path).with_context(|| format!("open {}", path.display()))?;
    for (k, v) in secrets {
        writeln!(file, "{k}={v}").with_context(|| format!("write {}", path.display()))?;
    }
    Ok(path)
}

/// What `weft target export` hands the repository: variables anybody
/// with access may read, and secrets nobody can read back.
#[derive(Debug, PartialEq, Eq)]
struct RepositorySettings {
    variables: Vec<(&'static str, String)>,
    secrets: Vec<(&'static str, String)>,
}

/// The credentials the workflow uses: an operator key to deploy the
/// program, and, when the install hosts a frontend for it, where that
/// frontend runs and the token its server calls with; and the frontend's
/// own environment, when one was handed over.
struct CiKeys {
    operator_key: String,
    /// The frontend the install hosts for this repository, when it has one.
    frontend: Option<HandedFrontend>,
    /// A dotenv file's contents (`weft infra env --into` writes them):
    /// the program's database address and credentials, a sign-in secret.
    front_env: Option<String>,
}

// SYNC: these names <-> crates/weft-cli/templates/ci/gcp.yml
fn repository_settings(
    target: &str,
    info: &weft_core::install::InstallInfo,
    keys: CiKeys,
) -> Result<RepositorySettings> {
    let Some(weft_core::install::CloudInstall::Gcp(gcp)) = &info.cloud else {
        anyhow::bail!(
            "{} is not a cloud install, so there is nothing to deploy to from CI",
            info.public_url
        );
    };
    let source = info.source.as_ref().with_context(|| {
        format!(
            "{} does not say which weft it runs; run its install workflow again",
            info.public_url
        )
    })?;
    Ok(RepositorySettings {
        variables: vec![
            ("WEFT_TARGET", target.to_string()),
            ("WEFT_SOURCE_REPOSITORY", source.repository.clone()),
            ("WEFT_SOURCE_COMMIT", source.commit.clone()),
            ("WEFT_PUBLIC_URL", info.public_url.clone()),
            ("GCP_PROJECT_ID", gcp.project.clone()),
            ("GCP_REGION", gcp.region.clone()),
            ("GCP_ARTIFACT_REGISTRY", gcp.artifact_registry.clone()),
            ("GCP_NETWORK", gcp.network.clone()),
            ("GCP_SUBNET", gcp.subnet.clone()),
            ("GCP_DEPLOYER_SERVICE_ACCOUNT", gcp.deployer_service_account.clone()),
            ("GCP_FRONTEND_SERVICE_ACCOUNT", gcp.frontend_service_account.clone()),
            ("GCP_WORKLOAD_IDENTITY_PROVIDER", gcp.workload_identity_provider.clone()),
        ]
        .into_iter()
        .chain(keys.frontend.iter().flat_map(|f| {
            [
                ("WEFT_FRONTEND_NAME", f.name.clone()),
                ("WEFT_FRONTEND_SERVICE", f.service.clone()),
                ("WEFT_FRONTEND_TOKEN_ID", f.token_id.to_string()),
            ]
        }))
        .collect(),
        secrets: [
            Some(("WEFT_OPERATOR_KEY", keys.operator_key)),
            keys.frontend.map(|f| ("WEFT_FRONTEND_TOKEN", f.token)),
            keys.front_env.map(|env| ("WEFT_FRONT_ENV", env)),
        ]
        .into_iter()
        .flatten()
        .collect(),
    })
}

async fn export(
    project: &weft_compiler::project::Project,
    name: &str,
    github: bool,
    front_env: Option<&Path>,
    frontend: Option<&str>,
) -> Result<()> {
    // Read before anything is minted, so a wrong path leaves no stray keys.
    let front_env = front_env
        .map(|path| std::fs::read_to_string(path).with_context(|| format!("read {}", path.display())))
        .transpose()?;
    let url = project.target_url(name).map_err(|e| anyhow::anyhow!("{e}"))?;
    let key = credentials::operator_key_for(&url, Some(name))?
        .with_context(|| format!("you are not logged in to {name}: weft login {name}"))?;
    let client = crate::client::DispatcherClient::new(url.clone(), Some(key));
    let info: weft_core::install::InstallInfo = serde_json::from_value(client.get_json("/install").await?)
        .with_context(|| format!("{url} answered /install with something that is not an install description"))?;
    // Checked before anything is minted, so a missing `gh` leaves no
    // stray keys on the install.
    let repo = if github {
        let out = std::process::Command::new("gh")
            .args(["repo", "view", "--json", "nameWithOwner,defaultBranchRef", "--jq", ".nameWithOwner + \" \" + .defaultBranchRef.name"])
            .current_dir(&project.root)
            .output()
            .context("run gh (the GitHub CLI); install it, or leave out --github to print the settings")?;
        anyhow::ensure!(
            out.status.success(),
            "gh cannot see this project's GitHub repository (not logged in, or no remote yet); \
             fix that, or leave out --github to print the settings"
        );
        let seen = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let (repo_name, branch) = seen.split_once(' ').with_context(|| {
            format!("{seen} has no default branch yet (nothing pushed); push weft.toml, with its `[targets.{name}]`, first")
        })?;
        // The workflow runs from the repository's default branch and reaches
        // the install by the target the `weft.toml` THERE names, so it is
        // that file, as GitHub has it, that must name this target.
        let deployed = default_branch_weft_toml(&project.root, repo_name, branch)?;
        committed_target_matches(&deployed, name, &url)
            .map_err(|e| e.context(format!("checking weft.toml on {repo_name}'s {branch} branch")))?;
        Some(super::frontend::repository(repo_name)?)
    } else {
        None
    };
    // Which frontend this repository's workflow deploys, decided before
    // anything is minted, so a refusal leaves no stray key.
    let frontends: Vec<weft_core::frontend::Frontend> =
        serde_json::from_value(client.get_json(&format!("/projects/{}/frontends", project.id())).await?)
            .context("read the project's frontends")?;
    let frontend = hosted_frontend(&frontends, frontend, repo.as_ref())?;

    let unpicked = unpicked_connections(&client, project).await;
    let labels = ExportLabels::of(project);
    let (keys, minted) = mint_ci_keys(&client, name, project, &labels, front_env, frontend).await?;
    let prepared = async {
        let settings = repository_settings(name, &info, keys)?;
        let stale = stale_exports(&client, &labels, &minted.keys).await?;
        anyhow::Ok((settings, stale))
    }
    .await;
    let (settings, stale) = match prepared {
        Ok(prepared) => prepared,
        Err(e) => return Err(take_back(&client, &minted, e, name).await),
    };
    if !github {
        println!("repository variables:");
        for (k, v) in &settings.variables {
            println!("  {k}={v}");
        }
        // Secrets never go to the terminal, where scrollback and logs keep
        // them: they go to a file only this user can read.
        let file = write_secrets_file(name, &settings.secrets)?;
        println!(
            "repository secrets: written to {} (readable by you only; the install keeps only a hash). \
             Copy each into the repository's secrets, then delete the file.",
            file.display()
        );
        // Nothing here knows when the new values are in place, so the
        // older ones stay usable until the person retires them.
        for id in &stale {
            println!("an earlier export's key {id} still works; once these are in place: weft token revoke {id} --on {name}");
        }
        if let Some((_, frontend, _)) = &minted.frontend {
            println!(
                "frontend '{frontend}' keeps its old token working until the deploy workflow puts the new one in place and retires it"
            );
        }
        print_unpicked(&unpicked, name);
        return Ok(());
    }
    if let Err(e) = set_repository_values(&project.root, &settings) {
        return Err(take_back(&client, &minted, e, name).await);
    }
    println!("set on the repository:");
    for (k, v) in &settings.variables {
        println!("  variable {k}={v}");
    }
    // Only how many: nothing that comes out of a secret reaches the
    // terminal, its name included.
    println!("  secrets  {} set", settings.secrets.len());
    println!("run its deploy workflow from the Actions tab");
    if let Some((_, frontend, _)) = &minted.frontend {
        println!("frontend '{frontend}' keeps its old token working until that run deploys the new one and retires it");
    }
    // The repository now holds the new keys, so an earlier export's are
    // held by nobody.
    revoke_all(&client, &stale).await.with_context(|| {
        format!("revoke an earlier export's keys; revoke them with `weft token revoke <id> --on {name}`")
    })?;
    for id in &stale {
        println!("revoked an earlier export's key {id}");
    }
    print_unpicked(&unpicked, name);
    Ok(())
}

/// What activating this program on `client`'s install would refuse of its
/// connection picks, each gap as the line that says how to fix it: the
/// deploy workflow's activation refuses the program until they are fixed,
/// and that is a late place to find out. The install answers with
/// activation's own check, run on this machine's compile of the program
/// (the install may not have built it yet). A program that does not
/// compile, or an install that cannot answer, come back as one line saying
/// so.
async fn unpicked_connections(client: &crate::client::DispatcherClient, project: &weft_compiler::project::Project) -> Vec<String> {
    let definition = match weft_compiler::hash::load_enriched_project_with_diagnostics(project) {
        Ok((definition, _)) => definition,
        Err(_) => return vec!["the program does not compile here, so its connections were not checked".into()],
    };
    let asked = async {
        let body = serde_json::to_value(&definition)?;
        let answer = client.post_json(&format!("/projects/{}/picks/check", project.id()), &body).await?;
        anyhow::Ok(serde_json::from_value::<weft_core::run_spec::Refusal>(answer)?)
    };
    match asked.await {
        Ok(refusal) => refusal.errors,
        Err(e) => vec![format!("could not check the install's connection picks ({e:#})")],
    }
}

fn print_unpicked(unpicked: &[String], target: &str) {
    if unpicked.is_empty() {
        return;
    }
    println!("the deploy workflow cannot turn the program on until these are fixed on {target} (add `--on {target}`):");
    for line in unpicked {
        println!("  {line}");
    }
}

/// An export that failed after minting: nobody holds the keys it minted,
/// so they are revoked, and the frontend's new token is dropped (it keeps
/// the one it has). The export's own failure stays the message, and what
/// became of its keys goes on a line beneath it.
async fn take_back(client: &crate::client::DispatcherClient, minted: &Minted, e: anyhow::Error, target: &str) -> anyhow::Error {
    let keys = match revoke_all(client, &minted.keys).await {
        Ok(()) => "the keys this export minted were revoked".to_string(),
        Err(revoke) => format!(
            "revoking the keys this export minted failed ({revoke:#}); revoke them with `weft token revoke <id> --on {target}`"
        ),
    };
    let front = match &minted.frontend {
        None => String::new(),
        Some((path, name, token)) => match client.delete(&format!("{path}/token/{token}")).await {
            Ok(()) => format!("; frontend '{name}' keeps the token it had"),
            Err(drop) => format!("; dropping frontend '{name}''s new token failed ({drop:#}), the one it had still works"),
        },
    };
    anyhow::anyhow!("{e:#}\n{keys}{front}")
}

/// The name and kind each key `weft target export` mints carries, so a
/// later export finds the ones an earlier one left: exact labels, with
/// the project's id, so two projects of one name never touch each
/// other's keys. `frontend` is what exports named the frontend's token
/// before each frontend kept a token of its own (`weft frontend`): such a
/// key is retired like any other an export left.
struct ExportLabels {
    id: uuid::Uuid,
    operator: String,
    frontend: String,
    /// The labels exports carried before they held the project's id
    /// (`ci: <name>`, `frontend: <name>`), still matched so the keys
    /// such an export minted get retired like any other.
    legacy_operator: String,
    legacy_frontend: String,
}

impl ExportLabels {
    fn of(project: &weft_compiler::project::Project) -> Self {
        let (name, id) = (&project.manifest.package.name, project.id());
        Self {
            id,
            operator: format!("ci: {name} ({id})"),
            frontend: format!("frontend: {name} ({id})"),
            legacy_operator: format!("ci: {name}"),
            legacy_frontend: format!("frontend: {name}"),
        }
    }

    /// Whether a listed token is one an export of this project minted.
    /// A legacy frontend key is also held to this project's id (it was
    /// minted for exactly that project); a legacy operator key carries
    /// nothing but its name, so one minted by an older export of another
    /// project with the same name matches too.
    fn minted(&self, token: &TokenSummary) -> bool {
        let name = token.name.as_deref();
        match token.kind {
            TokenKind::Operator => name == Some(self.operator.as_str()) || name == Some(self.legacy_operator.as_str()),
            TokenKind::Caller => {
                name == Some(self.frontend.as_str())
                    || (name == Some(self.legacy_frontend.as_str()) && token.allowed_projects == [self.id])
            }
        }
    }
}

/// The frontend the install hosts that this export hands its repository:
/// the one named (`--frontend`), else the one that deploys from this
/// repository, else the only one the install hosts for the project. None
/// when it hosts none: the workflow then deploys the program alone.
fn hosted_frontend<'a>(
    frontends: &'a [weft_core::frontend::Frontend],
    named: Option<&str>,
    repo: Option<&weft_core::frontend::Repository>,
) -> Result<Option<&'a weft_core::frontend::Frontend>> {
    let hosted: Vec<&weft_core::frontend::Frontend> =
        frontends.iter().filter(|f| f.host == weft_core::frontend::FrontendHost::CloudRun).collect();
    if let Some(name) = named {
        return hosted
            .iter()
            .find(|f| f.name == name)
            .copied()
            .map(Some)
            .with_context(|| format!("the install hosts no frontend named '{name}' for this project (`weft frontend ls` lists them)"));
    }
    let candidates: Vec<&weft_core::frontend::Frontend> = match repo {
        // By id: a repository renamed since keeps deploying its frontend.
        Some(repo) => hosted.iter().filter(|f| f.repo.as_ref().is_some_and(|r| r.id == repo.id)).copied().collect(),
        None => hosted,
    };
    match candidates.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one)),
        many => anyhow::bail!(
            "the install hosts several frontends this repository could deploy ({}); name one with --frontend",
            many.iter().map(|f| f.name.as_str()).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Mint the workflow's operator key, and a new token for the frontend it
/// deploys (beside the one it has, which keeps working until the new one is
/// in place), answering what was minted. A failure on the second takes back
/// the first.
async fn mint_ci_keys(
    client: &crate::client::DispatcherClient,
    target: &str,
    project: &weft_compiler::project::Project,
    labels: &ExportLabels,
    front_env: Option<String>,
    frontend: Option<&weft_core::frontend::Frontend>,
) -> Result<(CiKeys, Minted)> {
    let operator = MintTokenRequest { kind: TokenKind::Operator, ..MintTokenRequest::caller(labels.operator.clone()) };
    let minted: MintedToken = serde_json::from_value(client.post_json("/signal-tokens", &serde_json::to_value(&operator)?).await?)
        .context("read the token the install minted")?;
    let mut made = Minted { keys: vec![minted.id.to_string()], frontend: None };
    let operator_key = minted.token;
    let frontend = match frontend {
        None => None,
        Some(f) => {
            let path = format!("/projects/{}/frontends/{}", project.id(), f.name);
            let renewed = async {
                let answer = client.post_json(&format!("{path}/token"), &serde_json::json!({})).await?;
                let renewed: weft_core::frontend::FrontendWithToken =
                    serde_json::from_value(answer).context("read the frontend's new token")?;
                let service = renewed
                    .frontend
                    .service
                    .with_context(|| format!("the install names no service for frontend '{}'", f.name))?;
                anyhow::Ok(HandedFrontend { name: f.name.clone(), service, token: renewed.token, token_id: renewed.token_id })
            }
            .await;
            match renewed {
                Ok(handed) => {
                    made.frontend = Some((path, f.name.clone(), handed.token_id));
                    Some(handed)
                }
                Err(e) => return Err(take_back(client, &made, e, target).await),
            }
        }
    };
    Ok((CiKeys { operator_key, frontend, front_env }, made))
}

/// The frontend an export hands the workflow: its name, its service, and
/// its new token.
struct HandedFrontend {
    name: String,
    service: String,
    token: String,
    /// The token's id: what the workflow names to put it in place.
    token_id: uuid::Uuid,
}

/// What an export minted: its keys' ids, and the frontend it made a new
/// token for (its path on the install, its name, the token's id).
struct Minted {
    keys: Vec<String>,
    frontend: Option<(String, String, uuid::Uuid)>,
}

/// The keys an earlier export of this project left on the install.
async fn stale_exports(client: &crate::client::DispatcherClient, labels: &ExportLabels, fresh: &[String]) -> Result<Vec<String>> {
    let listed: Vec<TokenSummary> = serde_json::from_value(client.get_json("/signal-tokens").await?)
        .context("unexpected /signal-tokens listing shape")?;
    Ok(stale_ids(labels, listed, fresh))
}

fn stale_ids(labels: &ExportLabels, listed: Vec<TokenSummary>, fresh: &[String]) -> Vec<String> {
    listed.into_iter().map(|t| (t.id.to_string(), t)).filter(|(id, t)| labels.minted(t) && !fresh.contains(id)).map(|(id, _)| id).collect()
}

/// Revoke every id, trying all of them; the error names each one that
/// is still live.
async fn revoke_all(client: &crate::client::DispatcherClient, ids: &[String]) -> Result<()> {
    let mut failed = Vec::new();
    for id in ids {
        if let Err(e) = client.delete_idempotent(&format!("/signal-tokens/{id}")).await {
            failed.push(format!("{id} ({e:#})"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "these keys are still live: {}", failed.join(", "));
    Ok(())
}

/// Set every variable and secret on the project's GitHub repository.
fn set_repository_values(root: &Path, settings: &RepositorySettings) -> Result<()> {
    for (kind, entries) in [("variable", &settings.variables), ("secret", &settings.secrets)] {
        for (k, v) in entries {
            // The value goes in on stdin, never as an argument another
            // process on this machine could read.
            let mut child = std::process::Command::new("gh")
                .args([kind, "set", k])
                .current_dir(root)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .with_context(|| format!("run gh {kind} set {k}"))?;
            use std::io::Write;
            child.stdin.take().expect("piped").write_all(v.as_bytes())?;
            let status = child.wait()?;
            anyhow::ensure!(status.success(), "gh {kind} set {k} failed ({status})");
        }
    }
    Ok(())
}

/// This project's `weft.toml` as GitHub holds it on `branch` of `repo`
/// (the project may sit in a subfolder of the repository).
fn default_branch_weft_toml(root: &Path, repo: &str, branch: &str) -> Result<String> {
    let prefix = std::process::Command::new("git")
        .args(["rev-parse", "--show-prefix"])
        .current_dir(root)
        .output()
        .context("run git to find where the project sits in its repository")?;
    anyhow::ensure!(prefix.status.success(), "{} is not inside a git repository", root.display());
    let path = format!("{}weft.toml", String::from_utf8_lossy(&prefix.stdout).trim());
    let read = std::process::Command::new("gh")
        .args([
            "api",
            "-H",
            "Accept: application/vnd.github.raw",
            &format!("repos/{repo}/contents/{}?ref={}", encode_path(&path), encode_path(branch)),
        ])
        .current_dir(root)
        .output()
        .context("run gh to read weft.toml from GitHub")?;
    anyhow::ensure!(
        read.status.success(),
        "{repo} has no {path} on its {branch} branch, which is where its workflow deploys from; commit \
         weft.toml and push it to {branch} (gh: {})",
        String::from_utf8_lossy(&read.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&read.stdout).into_owned())
}

/// `text` escaped for a URL, `/` kept so a path stays a path.
fn encode_path(text: &str) -> String {
    text.split('/')
        .map(|segment| url::form_urlencoded::byte_serialize(segment.as_bytes()).collect::<String>().replace('+', "%20"))
        .collect::<Vec<_>>()
        .join("/")
}

/// The deployed `weft.toml` (`committed`) names target `name` at `url`, the
/// address it has on this machine. The repository's workflow reads the
/// target from what it deploys, never from this machine.
fn committed_target_matches(committed: &str, name: &str, url: &str) -> Result<()> {
    let manifest: toml::Value = toml::from_str(committed).context("parse the committed weft.toml")?;
    let at = manifest.get("targets").and_then(|t| t.get(name)).and_then(|t| t.get("url")).and_then(toml::Value::as_str);
    match at {
        Some(at) if credentials::url_key(at)? == credentials::url_key(url)? => Ok(()),
        Some(at) => anyhow::bail!(
            "weft.toml there names target '{name}' at {at}, and this machine at {url}; commit and push \
             weft.toml as it is now, so the repository's workflow reaches the same install"
        ),
        None => anyhow::bail!(
            "weft.toml there has no `[targets.{name}]`, so the repository's workflow could not find the \
             install; commit and push weft.toml (`weft target add` wrote the target into it)"
        ),
    }
}

/// The `weft target export ... --github` a person runs next, spelled with
/// a target they really have: the one this command was given, else the
/// project's only cloud target, else the choice between them.
pub fn export_command(project: &weft_compiler::project::Project, on: Option<&str>) -> String {
    let name = match on {
        Some(on) => on.to_string(),
        None => {
            let cloud: Vec<&str> = project
                .manifest
                .targets
                .keys()
                .map(String::as_str)
                .filter(|name| *name != weft_compiler::project::LOCAL_TARGET)
                .collect();
            match cloud.as_slice() {
                [one] => one.to_string(),
                [] => return "`weft target add <name> <address>`, then `weft target export <name> --github`".into(),
                many => return format!("`weft target export <{}> --github`", many.join("|")),
            }
        }
    };
    format!("`weft target export {name} --github`")
}

/// A target name is what people type after `--on`, and a TOML key.
fn validate_name(name: &str) -> Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "'{name}' is not a target name: use letters, digits, '-' and '_'"
    );
    Ok(())
}

fn edit_manifest(path: &Path, edit: impl FnOnce(&mut toml_edit::DocumentMut) -> Result<()>) -> Result<()> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut doc: toml_edit::DocumentMut =
        raw.parse().with_context(|| format!("parse {}", path.display()))?;
    edit(&mut doc)?;
    std::fs::write(path, doc.to_string()).with_context(|| format!("write {}", path.display()))
}

fn set_target(doc: &mut toml_edit::DocumentMut, name: &str, url: &str) -> Result<()> {
    let targets = doc
        .entry("targets")
        .or_insert_with(|| {
            let mut table = toml_edit::Table::new();
            table.set_implicit(true);
            toml_edit::Item::Table(table)
        })
        .as_table_mut()
        .context("weft.toml's `targets` is not a table")?;
    let mut entry = toml_edit::Table::new();
    entry.insert("url", toml_edit::value(url));
    targets.insert(name, toml_edit::Item::Table(entry));
    Ok(())
}

fn remove_target(doc: &mut toml_edit::DocumentMut, name: &str) -> bool {
    let Some(targets) = doc.get_mut("targets").and_then(|t| t.as_table_mut()) else {
        return false;
    };
    let removed = targets.remove(name).is_some();
    if targets.is_empty() {
        doc.remove("targets");
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "# ours\n[package]\nname = \"p\" # keep me\nid = \"00000000-0000-0000-0000-000000000000\"\n";

    #[test]
    fn adding_and_removing_a_target_keeps_the_rest_of_the_file() {
        let mut doc: toml_edit::DocumentMut = MANIFEST.parse().unwrap();
        set_target(&mut doc, "prod", "https://weft.example.com").unwrap();
        let written = doc.to_string();
        assert!(written.contains("# keep me"), "{written}");
        assert!(written.contains("[targets.prod]\nurl = \"https://weft.example.com\""), "{written}");
        assert!(remove_target(&mut doc, "prod"));
        assert!(!remove_target(&mut doc, "prod"));
        assert_eq!(doc.to_string(), MANIFEST, "removing the last target leaves no empty table");
    }

    #[test]
    fn a_cloud_install_becomes_the_workflows_settings() {
        use weft_core::install::{CloudInstall, GcpInstall, InstallInfo, WeftSource};
        let keys = || CiKeys {
            operator_key: "op".into(),
            frontend: Some(HandedFrontend { name: "front".into(), service: "fe-svc".into(), token: "fr".into(), token_id: uuid::Uuid::nil() }),
            front_env: None,
        };
        let mut info = InstallInfo {
            public_url: "https://w.example.com".into(),
            cloud: Some(CloudInstall::Gcp(GcpInstall {
                project: "p".into(),
                region: "us-central1".into(),
                artifact_registry: "us-central1-docker.pkg.dev/p/weft".into(),
                network: "weft".into(),
                subnet: "weft-sub".into(),
                deployer_service_account: "d@p".into(),
                frontend_service_account: "f@p".into(),
                workload_identity_provider: "wip".into(),
            })),
            source: Some(WeftSource { repository: "me/weft".into(), commit: "abc".into() }),
        };
        let settings = repository_settings("prod", &info, keys()).unwrap();
        assert!(settings.variables.contains(&("WEFT_TARGET", "prod".into())));
        assert!(settings.variables.contains(&("WEFT_SOURCE_COMMIT", "abc".into())));
        assert!(settings.secrets.contains(&("WEFT_FRONTEND_TOKEN", "fr".into())));
        assert!(settings.variables.contains(&("WEFT_FRONTEND_SERVICE", "fe-svc".into())));
        let alone = repository_settings("prod", &info, CiKeys { frontend: None, ..keys() }).unwrap();
        assert!(!alone.secrets.iter().any(|(k, _)| *k == "WEFT_FRONTEND_TOKEN"), "no hosted frontend, no frontend settings");
        info.source = None;
        assert!(repository_settings("prod", &info, keys()).unwrap_err().to_string().contains("install workflow"));
        info.cloud = None;
        assert!(repository_settings("prod", &info, keys()).is_err());
    }

    /// A folder or branch name with characters a URL would read otherwise
    /// still names the file on GitHub.
    #[test]
    fn a_path_is_escaped_segment_by_segment() {
        assert_eq!(encode_path("apps/my app#1/weft.toml"), "apps/my%20app%231/weft.toml");
        assert_eq!(encode_path("feature/a&b"), "feature/a%26b");
    }

    /// The workflow finds the install in the committed `weft.toml`, so
    /// export refuses a target that only this machine has.
    #[test]
    fn the_committed_weft_toml_must_name_the_target() {
        let committed = "[package]\nname = \"p\"\n\n[targets.prod]\nurl = \"https://weft.example.com\"\n";
        committed_target_matches(committed, "prod", "https://weft.example.com/").unwrap();
        let missing = committed_target_matches(committed, "staging", "https://weft.example.com").unwrap_err();
        assert!(missing.to_string().contains("no `[targets.staging]`"), "{missing}");
        let elsewhere = committed_target_matches(committed, "prod", "https://other.example.com").unwrap_err();
        assert!(elsewhere.to_string().contains("https://other.example.com"), "{elsewhere}");
    }

    /// Every variable and secret the workflow reads is one export sets,
    /// and the other way round.
    #[test]
    fn the_workflow_reads_exactly_what_export_sets() {
        let template = include_str!("../../templates/ci/gcp.yml");
        let mut read: Vec<String> = Vec::new();
        for prefix in ["vars.", "secrets."] {
            for part in template.split(prefix).skip(1) {
                let name: String = part.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
                if !read.contains(&name) {
                    read.push(name);
                }
            }
        }
        read.sort();
        let info = weft_core::install::InstallInfo {
            public_url: "https://w".into(),
            cloud: Some(weft_core::install::CloudInstall::Gcp(weft_core::install::GcpInstall {
                project: String::new(),
                region: String::new(),
                artifact_registry: String::new(),
                network: String::new(),
                subnet: String::new(),
                deployer_service_account: String::new(),
                frontend_service_account: String::new(),
                workload_identity_provider: String::new(),
            })),
            source: Some(weft_core::install::WeftSource { repository: String::new(), commit: String::new() }),
        };
        let settings =
            repository_settings(
                "t",
                &info,
                CiKeys {
                    operator_key: String::new(),
                    frontend: Some(HandedFrontend { name: String::new(), service: String::new(), token: String::new(), token_id: uuid::Uuid::nil() }),
                    front_env: Some(String::new()),
                },
            )
            .unwrap();
        let mut set: Vec<String> =
            settings.variables.iter().chain(&settings.secrets).map(|(k, _)| k.to_string()).collect();
        set.sort();
        assert_eq!(read, set);
    }

    #[test]
    fn the_frontend_an_export_hands_over_is_the_named_one_or_this_repositorys() {
        use weft_core::frontend::{Frontend, FrontendHost};
        let f = |name: &str, host: FrontendHost, repo: Option<&str>| Frontend {
            name: name.into(),
            project: uuid::Uuid::nil(),
            host,
            repo: repo.map(|r| weft_core::frontend::Repository { name: r.into(), id: if r == "me/shop" { 1 } else { 2 } }),
            service: Some(format!("fe-{name}")),
            url: None,
            token_id: uuid::Uuid::nil(),
            pending_token_ids: Vec::new(),
        };
        let all = [
            f("shop", FrontendHost::CloudRun, Some("me/shop")),
            f("admin", FrontendHost::CloudRun, Some("me/admin")),
            f("app", FrontendHost::External, None),
        ];
        let repo = |name: &str, id| weft_core::frontend::Repository { name: name.into(), id };
        assert_eq!(hosted_frontend(&all, None, Some(&repo("me/renamed-shop", 1))).unwrap().unwrap().name, "shop", "by id");
        assert_eq!(hosted_frontend(&all, Some("admin"), Some(&repo("me/shop", 1))).unwrap().unwrap().name, "admin");
        assert!(hosted_frontend(&all, None, Some(&repo("me/other", 9))).unwrap().is_none(), "nothing hosted for this repository");
        assert!(hosted_frontend(&all, None, None).unwrap_err().to_string().contains("--frontend"));
        assert!(hosted_frontend(&all, Some("app"), None).is_err(), "an outside frontend is deployed by nobody here");
    }

    #[test]
    fn only_an_earlier_export_of_this_project_is_stale() {
        let project = weft_compiler::project::Project {
            root: std::path::PathBuf::from("/p"),
            manifest: toml::from_str("[package]\nname = \"p\"\nid = \"00000000-0000-0000-0000-000000000001\"\n").unwrap(),
        };
        let labels = ExportLabels::of(&project);
        let other: uuid::Uuid = "00000000-0000-0000-0000-000000000002".parse().unwrap();
        // Token ids are uuids; each named one here is a uuid whose last
        // digits count up, so the expected list reads by name.
        let names = ["old-op", "old-front", "new-op", "a-person", "wrong-kind", "other-project", "legacy-op", "legacy-front", "legacy-other-front"];
        let id_of = |name: &str| -> uuid::Uuid {
            let n = names.iter().position(|n| *n == name).expect("a named token");
            format!("00000000-0000-0000-0000-{n:012}").parse().expect("a uuid")
        };
        let scoped = |id: &str, kind: TokenKind, name: &str, allowed_projects: Vec<uuid::Uuid>| TokenSummary {
            id: id_of(id),
            kind,
            recognizer: "wft-x-...".into(),
            name: Some(name.into()),
            created_at_unix: 0,
            allowed_projects,
            allowed_tags: vec![],
            allowed_displays: vec![],
            all_displays: false,
            instance: None,
            expires_at_unix: None,
        };
        let token = |id: &str, kind: TokenKind, name: &str| scoped(id, kind, name, vec![]);
        let listed = vec![
            token("old-op", TokenKind::Operator, &labels.operator),
            token("old-front", TokenKind::Caller, &labels.frontend),
            token("new-op", TokenKind::Operator, &labels.operator),
            token("a-person", TokenKind::Operator, "laptop"),
            token("wrong-kind", TokenKind::Caller, &labels.operator),
            token("other-project", TokenKind::Caller, "frontend: p (00000000-0000-0000-0000-000000000002)"),
            token("legacy-op", TokenKind::Operator, "ci: p"),
            scoped("legacy-front", TokenKind::Caller, "frontend: p", vec![project.id()]),
            scoped("legacy-other-front", TokenKind::Caller, "frontend: p", vec![other]),
        ];
        assert_eq!(
            stale_ids(&labels, listed, &[id_of("new-op").to_string()]),
            ["old-op", "old-front", "legacy-op", "legacy-front"].map(|name| id_of(name).to_string())
        );
    }

    #[test]
    fn names_are_checked() {
        assert!(validate_name("prod-eu_1").is_ok());
        assert!(validate_name("pr od").is_err());
        assert!(validate_name("").is_err());
    }
}
