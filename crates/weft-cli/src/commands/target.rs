//! `weft target add|list|remove` and `weft login|logout`: the installs a
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
    Remove { name: String },
    Export { name: String, github: bool, front_env: Option<std::path::PathBuf> },
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
        TargetAction::Export { name, github, front_env } => export(project, &name, github, front_env.as_deref()).await?,
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
    crate::client::DispatcherClient::new(url.clone(), Some(key.clone()))
        .get_json("/install")
        .await
        .with_context(|| format!("{url} refused this key; nothing was stored"))?;
    let mut stored = credentials::load()?;
    stored.set(&url, key)?;
    credentials::save(&stored)?;
    println!("logged in to {name} ({url}); act on it with `--on {name}`");
    Ok(())
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

/// What `weft target export` hands the repository: variables anybody
/// with access may read, and secrets nobody can read back.
#[derive(Debug, PartialEq, Eq)]
struct RepositorySettings {
    variables: Vec<(&'static str, String)>,
    secrets: Vec<(&'static str, String)>,
}

/// The two credentials the workflow uses: an operator key to deploy the
/// program, and a caller token the frontend's server calls with; and the
/// frontend's own environment, when one was handed over.
struct CiKeys {
    operator_key: String,
    frontend_token: String,
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
    let internal = info.internal_url.clone().with_context(|| {
        format!("{} does not say where a frontend beside it reaches it", info.public_url)
    })?;
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
            ("WEFT_INTERNAL_URL", internal),
            ("WEFT_PUBLIC_URL", info.public_url.clone()),
            ("GCP_PROJECT_ID", gcp.project.clone()),
            ("GCP_REGION", gcp.region.clone()),
            ("GCP_ARTIFACT_REGISTRY", gcp.artifact_registry.clone()),
            ("GCP_NETWORK", gcp.network.clone()),
            ("GCP_SUBNET", gcp.subnet.clone()),
            ("GCP_DEPLOYER_SERVICE_ACCOUNT", gcp.deployer_service_account.clone()),
            ("GCP_FRONTEND_SERVICE_ACCOUNT", gcp.frontend_service_account.clone()),
            ("GCP_WORKLOAD_IDENTITY_PROVIDER", gcp.workload_identity_provider.clone()),
        ],
        secrets: [
            Some(("WEFT_OPERATOR_KEY", keys.operator_key)),
            Some(("WEFT_FRONTEND_TOKEN", keys.frontend_token)),
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
    if github {
        let status = std::process::Command::new("gh")
            .args(["repo", "view", "--json", "nameWithOwner"])
            .current_dir(&project.root)
            .stdout(std::process::Stdio::null())
            .status()
            .context("run gh (the GitHub CLI); install it, or leave out --github to print the settings")?;
        anyhow::ensure!(
            status.success(),
            "gh cannot see this project's GitHub repository (not logged in, or no remote yet); \
             fix that, or leave out --github to print the settings"
        );
    }
    let labels = ExportLabels::of(project);
    let (keys, fresh) = mint_ci_keys(&client, name, project, &labels, front_env).await?;
    let prepared = async {
        let settings = repository_settings(name, &info, keys)?;
        let stale = stale_exports(&client, &labels, &fresh).await?;
        anyhow::Ok((settings, stale))
    }
    .await;
    let (settings, stale) = match prepared {
        Ok(prepared) => prepared,
        Err(e) => return Err(take_back(&client, &fresh, e, name).await),
    };
    if !github {
        println!("repository variables:");
        for (k, v) in &settings.variables {
            println!("  {k}={v}");
        }
        println!("repository secrets (shown once; the install keeps only a hash):");
        for (k, v) in &settings.secrets {
            println!("  {k}={v}");
        }
        // Nothing here knows when the new values are in place, so the
        // older ones stay usable until the person retires them.
        for id in &stale {
            println!("an earlier export's key {id} still works; once these are in place: weft token revoke {id} --on {name}");
        }
        return Ok(());
    }
    if let Err(e) = set_repository_values(&project.root, &settings) {
        return Err(take_back(&client, &fresh, e, name).await);
    }
    println!(
        "set {} variables and {} secrets on the repository; run its deploy workflow from the Actions tab",
        settings.variables.len(),
        settings.secrets.len()
    );
    // The repository now holds the new keys, so an earlier export's are
    // held by nobody.
    revoke_all(&client, &stale).await.with_context(|| {
        format!("revoke an earlier export's keys; revoke them with `weft token revoke <id> --on {name}`")
    })?;
    for id in &stale {
        println!("revoked an earlier export's key {id}");
    }
    Ok(())
}

/// An export that failed after minting: nobody holds the keys it minted,
/// so they are revoked. The export's own failure stays the message, and
/// what became of its keys goes on a line beneath it.
async fn take_back(client: &crate::client::DispatcherClient, fresh: &[String], e: anyhow::Error, target: &str) -> anyhow::Error {
    let keys = match revoke_all(client, fresh).await {
        Ok(()) => "the keys this export minted were revoked".to_string(),
        Err(revoke) => format!(
            "revoking the keys this export minted failed ({revoke:#}); revoke them with `weft token revoke <id> --on {target}`"
        ),
    };
    anyhow::anyhow!("{e:#}\n{keys}")
}

/// The name and kind each key `weft target export` mints carries, so a
/// later export finds the ones an earlier one left: exact labels, with
/// the project's id, so two projects of one name never touch each
/// other's keys.
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

/// Mint the workflow's two keys, answering them with their ids. A failure
/// minting the second revokes the first.
async fn mint_ci_keys(
    client: &crate::client::DispatcherClient,
    target: &str,
    project: &weft_compiler::project::Project,
    labels: &ExportLabels,
    front_env: Option<String>,
) -> Result<(CiKeys, Vec<String>)> {
    async fn mint(client: &crate::client::DispatcherClient, body: MintTokenRequest) -> Result<(String, String)> {
        let minted: MintedToken = serde_json::from_value(client.post_json("/signal-tokens", &serde_json::to_value(&body)?).await?)
            .context("read the token the install minted")?;
        Ok((minted.token, minted.id.to_string()))
    }
    let operator = MintTokenRequest { kind: TokenKind::Operator, ..MintTokenRequest::caller(labels.operator.clone()) };
    let (operator_key, operator_id) = mint(client, operator).await?;
    let frontend = MintTokenRequest { allowed_projects: vec![project.id()], ..MintTokenRequest::caller(labels.frontend.clone()) };
    let frontend = mint(client, frontend).await;
    let (frontend_token, frontend_id) = match frontend {
        Ok(minted) => minted,
        Err(e) => return Err(take_back(client, std::slice::from_ref(&operator_id), e, target).await),
    };
    Ok((CiKeys { operator_key, frontend_token, front_env }, vec![operator_id, frontend_id]))
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
        let keys = || CiKeys { operator_key: "op".into(), frontend_token: "fr".into(), front_env: None };
        let mut info = InstallInfo {
            public_url: "https://w.example.com".into(),
            internal_url: Some("http://10.10.0.100".into()),
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
            address: None,
        };
        let settings = repository_settings("prod", &info, keys()).unwrap();
        assert!(settings.variables.contains(&("WEFT_TARGET", "prod".into())));
        assert!(settings.variables.contains(&("WEFT_SOURCE_COMMIT", "abc".into())));
        assert!(settings.secrets.contains(&("WEFT_FRONTEND_TOKEN", "fr".into())));
        info.source = None;
        assert!(repository_settings("prod", &info, keys()).unwrap_err().to_string().contains("install workflow"));
        info.cloud = None;
        assert!(repository_settings("prod", &info, keys()).is_err());
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
            internal_url: Some("http://i".into()),
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
            address: None,
        };
        let settings =
            repository_settings("t", &info, CiKeys { operator_key: String::new(), frontend_token: String::new(), front_env: Some(String::new()) }).unwrap();
        let mut set: Vec<String> =
            settings.variables.iter().chain(&settings.secrets).map(|(k, _)| k.to_string()).collect();
        set.sort();
        assert_eq!(read, set);
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
