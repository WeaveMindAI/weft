//! `weft connect`: manage a node's service connection from the terminal,
//! with the same abilities as the editor's Connect panel: list the stored
//! connections and pick one, connect a new account through the shared or
//! the own door (paste a key, run a browser sign-in, mint an app, use the
//! paste section of a consent service), upgrade an exclusive grant,
//! disconnect the node, or forget a stored connection for good.
//!
//! Every choice has a flag (see [`ConnectOpts`]) so scripts and AI runs
//! never hang on a prompt; interactively the command walks the same
//! decisions as menus. The picked handle is written into the node's own
//! source file (main.weft, or the `@include`d file the node lives in)
//! through the compiler's structural `SetConfig` edit, exactly as a
//! click in the editor writes it; pasted secrets go terminal -> store
//! and never touch the source.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use uuid::Uuid;
use weft_core::access::spec::{
    percent_encode, AccessSpec, AppRegistration, CredentialField, Door, GrantCoexistence,
};
use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, DoorsAnswer, DoorsRequest, DoorsStatus,
    GrantSummary, MintAppRequest, MintAppResponse, SharedAppChoice, SharedDoorPick, StartedOAuth,
};
use weft_core::CredentialOwner;

use crate::client::DispatcherClient;
use crate::commands::Ctx;
use crate::prompt::{confirm, is_interactive, prompt_line};

#[derive(clap::Args, Debug)]
pub struct ConnectOpts {
    /// The access node to manage: its id, or `path/to/file.weft:id`
    /// when the same id exists in more than one included file. Optional
    /// when the project has exactly one access node.
    #[arg(long)]
    pub node: Option<String>,
    /// Only list the node's stored connections, change nothing.
    #[arg(long, group = "action")]
    pub list: bool,
    /// Pick this stored connection (its id, from --list) for the node.
    #[arg(long, group = "action")]
    pub grant: Option<Uuid>,
    /// Clear the node's picked connection (the stored connection stays).
    #[arg(long, group = "action")]
    pub disconnect: bool,
    /// Delete a stored connection for good (its id, from --list). Every
    /// pick in the current project pointing at it is cleared; another
    /// project's node fails loudly at run with a reconnect message.
    #[arg(long, group = "action", conflicts_with = "node")]
    pub forget: Option<Uuid>,
    /// Skip the are-you-sure prompt on --forget.
    #[arg(long, requires = "forget")]
    pub yes: bool,
    /// Connect a new account through this door: `shared` (weft's app or
    /// key, one click) or `own` (bring your own credential).
    #[arg(long, group = "action", value_parser = ["shared", "own"])]
    pub door: Option<String>,
    /// Which registered app to use on the shared door (its label), when
    /// the shared door offers more than one.
    #[arg(long, conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade"])]
    pub shared_app: Option<String>,
    /// A credential field for the own door, `name=value`. Repeatable.
    /// The field names are the ones the connect form asks for (`weft
    /// connect` prints them when a required one is missing). The value
    /// rides the command line, which other processes on this machine
    /// can read; for a secret prefer the prompt or --set-env.
    #[arg(long = "set", value_name = "NAME=VALUE", conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade"])]
    pub set: Vec<String>,
    /// Like --set, but the value is read from the named environment
    /// variable (`name=ENV_VAR`), so a secret never rides the command
    /// line. Repeatable.
    #[arg(long = "set-env", value_name = "NAME=ENV_VAR", conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade"])]
    pub set_env: Vec<String>,
    /// Use the paste-a-credential section of a consent service (a ready
    /// token, no app) instead of the app + sign-in path.
    #[arg(long, conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade", "mint"])]
    pub paste: bool,
    /// Mint the provider app automatically (services that support it)
    /// and connect through the fresh credentials.
    #[arg(long, conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade"])]
    pub mint: bool,
    /// A name for the new connection, shown in the connection list.
    #[arg(long, conflicts_with_all = ["list", "grant", "disconnect", "forget", "upgrade"])]
    pub label: Option<String>,
    /// Permission ids to request on the own door, comma-separated.
    /// Defaults to the service's default set. The shared door's set is
    /// the chosen app's fixed one, so this flag is refused there.
    #[arg(long, value_delimiter = ',', conflicts_with_all = ["list", "grant", "disconnect", "forget"])]
    pub permissions: Option<Vec<String>>,
    /// Re-run the sign-in on this existing connection (its id) to widen
    /// its permissions (services with one grant per account).
    #[arg(long, group = "action")]
    pub upgrade: Option<Uuid>,
}

impl ConnectOpts {
    /// True when any flag shapes a NEW connection (a credential, an app
    /// choice, a name, ...): the intent is "connect", so the
    /// stored-connections menu is skipped; its pick/quit/forget
    /// branches would otherwise silently drop these flags, typed
    /// credentials included.
    fn shapes_a_new_connection(&self) -> bool {
        self.shared_app.is_some()
            || !self.set.is_empty()
            || !self.set_env.is_empty()
            || self.paste
            || self.mint
            || self.label.is_some()
            || self.permissions.is_some()
    }
}

/// One access node of the project: where a connection can be picked.
/// Discovery walks main.weft AND every `@include`d file (recursively),
/// each file visited ONCE: a subgraph included in two places is still
/// one source file, so one pick serves every inclusion.
struct AccessTarget {
    /// The file the node is written in; the pick is written HERE (the
    /// same per-file edit the editor makes when navigated into it).
    file: std::path::PathBuf,
    /// The same file relative to the project root, the name `--node
    /// file:id` qualification and every message use.
    rel_file: String,
    /// The node's id within its file.
    node: String,
    node_type: String,
    input: String,
    spec: AccessSpec,
    /// The project's declared public app for this service, the own
    /// door's fallback when the user pastes no app.
    project_app: Option<AppRegistration>,
    /// What the node's picker field currently holds.
    picked: Pick,
    /// The include aliases this node's file is reachable through, for
    /// display ("in sub.weft, included as c and c2"). Empty for
    /// main.weft's own nodes.
    included_as: Vec<String>,
}

/// The `{id, identity}` handle a node's picker config holds.
struct PickedHandle {
    id: Uuid,
    identity: Option<String>,
}

impl PickedHandle {
    /// Who this pick names, for display.
    fn who(&self) -> String {
        self.identity.clone().unwrap_or_else(|| self.id.to_string())
    }
}

/// What a node's picker field holds. `Malformed` carries the parse
/// error so listings can SAY the field is unreadable; discovery must
/// not die on it, because the write paths (pick, disconnect) overwrite
/// the value, and bailing would brick the very commands that fix it
/// (a `--disconnect` on another node would die on this one's value).
enum Pick {
    None,
    Handle(PickedHandle),
    Malformed(String),
}

impl Pick {
    /// The readable handle, when one is picked.
    fn handle(&self) -> Option<&PickedHandle> {
        match self {
            Pick::Handle(h) => Some(h),
            _ => None,
        }
    }
}

pub async fn run(ctx: Ctx, opts: ConnectOpts) -> Result<()> {
    let client = ctx.client();
    let interactive = is_interactive();
    let json = ctx.json();
    // --json promises one JSON object per line on stdout, which the
    // menu and the connect walkthroughs cannot keep; only the fully
    // flag-driven actions honor it.
    if json && !(opts.list || opts.grant.is_some() || opts.disconnect || opts.forget.is_some()) {
        bail!(
            "--json works with the flag-driven actions (--list, --grant, --disconnect, \
             --forget); the connect walkthrough prints for a person"
        );
    }

    // Forgetting a stored connection is store-wide: it needs no node
    // and works outside any project. When a project IS here, every one
    // of its picks pointing at the deleted row is cleared too, exactly
    // like the interactive menu's forget.
    if let Some(id) = &opts.forget {
        if !opts.yes
            && !confirm(
                &format!(
                    "Forget connection {id} for good? Every project pointing at it will \
                     need a reconnect. [yes/no] "
                ),
                "--yes",
            )?
        {
            if json {
                // Same shape as the accept path, so a consumer can
                // always read `.cleared`.
                println!("{}", serde_json::json!({ "forgot": Value::Null, "cleared": [] }));
            } else {
                println!("kept.");
            }
            return Ok(());
        }
        forget_grant(&client, *id).await?;
        if !json {
            println!("forgot connection {id}.");
        }
        let cleared = clear_picks_pointing_at(&ctx, *id, json);
        if json {
            // `cleared` names the nodes whose source files were just
            // rewritten (their pick pointed at the deleted row); a JSON
            // consumer must hear about disk it did not ask to touch.
            println!("{}", serde_json::json!({ "forgot": id, "cleared": cleared }));
        }
        return Ok(());
    }

    // --list works from ANYWHERE: outside a project (or in one that is
    // broken, or has no access node) there is still a store to list,
    // and other commands print `weft connect --list` as a recovery, so
    // it must not die on project discovery. Inside a healthy project
    // the listing below stays scoped to the chosen node's service and
    // marks its pick.
    if opts.list && !matches!(ctx.project_here(), Ok(Some(_))) {
        return print_all_grants(&client, json).await;
    }

    let project = ctx.project()?;
    let root = project.root.clone();
    let catalog = weft_compiler::build::build_project_catalog(&root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    let targets = access_targets(&root, &project.main_weft(), project.id(), &catalog)?;
    if targets.is_empty() {
        if opts.list {
            // Nothing to scope the listing to; list the whole store.
            return print_all_grants(&client, json).await;
        }
        bail!(
            "this project has no access node (no node whose type declares a service \
             recipe), so there is nothing to connect"
        );
    }
    let target = choose_target(targets, opts.node.as_deref(), json)?;
    let service = target.spec.service.clone();
    let label = target.spec.display_label().to_string();
    let registry = catalog.type_registry();
    let cx = Connecting {
        client: &client,
        target: &target,
        service_label: &label,
        registry: &registry,
        interactive,
        json,
    };

    if opts.disconnect {
        cx.clear_pick()?;
        if json {
            println!(
                "{}",
                serde_json::json!({ "node": target.node, "picked": Value::Null })
            );
        }
        return Ok(());
    }

    let grants = list_grants(&client, Some(&service)).await?;

    if opts.list {
        if json {
            // One JSON OBJECT per line, per the global --json contract.
            println!("{}", serde_json::json!({ "connections": grants }));
        } else {
            print_grants(&target, &label, &grants);
        }
        return Ok(());
    }

    if let Some(id) = &opts.grant {
        let g = grants
            .iter()
            .find(|g| g.id == *id)
            .with_context(|| format!("no stored {label} connection with id {id} (see --list)"))?;
        cx.pick_grant(g)?;
        if json {
            println!(
                "{}",
                serde_json::json!({ "node": target.node, "file": target.rel_file, "picked": g.id })
            );
        }
        return Ok(());
    }

    if let Some(id) = &opts.upgrade {
        // Upgrading re-runs a browser sign-in on the existing grant;
        // a pasted-credential service has no sign-in to re-run.
        if !target.spec.needs_browser_consent() {
            bail!(
                "{label} connections are not browser sign-ins, so there is nothing to \
                 upgrade. To change what a connection holds, connect a new one with \
                 --door own and pick it."
            );
        }
        // The store only rotates a grant in place on a one-grant-per-
        // account service; anywhere else a wider connection is simply a
        // new one. Refuse locally, before any prompting.
        if target.spec.grants != GrantCoexistence::Exclusive {
            bail!(
                "{label} keeps one connection per consent, so there is nothing to \
                 upgrade in place; connect a new account (--door own|shared) and pick it"
            );
        }
        let g = grants
            .iter()
            .find(|g| g.id == *id)
            .with_context(|| format!("no stored {label} connection with id {id} (see --list)"))?;
        // A shared-door upgrade's permission set is the app's declared
        // one (the server replaces whatever a client asks for), so
        // nothing is asked and --permissions is refused.
        let ticked = if g.door == Door::Shared {
            if opts.permissions.is_some() {
                bail!(
                    "--permissions applies to the own door; a shared-door connection \
                     carries the app's declared set"
                );
            }
            Vec::new()
        } else {
            ticked_permissions(&target.spec, &opts, interactive)?
        };
        let shared_app = if g.door == Door::Shared {
            g.label.clone()
        } else {
            None
        };
        let grant = cx
            .consent_flow(g.door, ticked, shared_app, Some(*id), None)
            .await?;
        cx.pick_grant(&grant)?;
        return Ok(());
    }

    // No action flag: connect a new account when --door said so or any
    // flag SHAPES a new connection (--set, --mint, ...; the menu's
    // pick/quit/forget branches would silently drop them, credentials
    // included), else walk the interactive menu over the stored
    // connections.
    if opts.door.is_none() && !opts.shapes_a_new_connection() {
        print_grants(&target, &label, &grants);
        let hint = "--grant <id>, --door shared|own, --disconnect, or --forget <id>";
        let menu = if grants.is_empty() {
            "(c)onnect a new account, (d)isconnect the node, (q)uit: ".to_string()
        } else {
            format!(
                "Pick a connection [1-{}], or (c)onnect a new account, (d)isconnect the \
                 node, (f <N>) forget one, (q)uit: ",
                grants.len()
            )
        };
        let answer = prompt_line(&menu, hint)?;
        let words: Vec<&str> = answer.split_whitespace().collect();
        match words.as_slice() {
            [] | ["q"] => return Ok(()),
            ["d"] => {
                cx.clear_pick()?;
                return Ok(());
            }
            ["c"] => { /* falls through to the connect flow below */ }
            ["f", n] if !grants.is_empty() => {
                let g = grant_by_number(&grants, n)?;
                if confirm(
                    &format!(
                        "Forget \"{}\" for good? Every project pointing at it will need \
                         a reconnect. [yes/no] ",
                        g.identity.clone().unwrap_or_else(|| g.id.to_string())
                    ),
                    "--forget <id> --yes",
                )? {
                    forget_grant(&client, g.id).await?;
                    println!("forgot it.");
                    // The SAME sweep the --forget flag runs: every node
                    // in the project whose pick pointed at the deleted
                    // row is cleared (not just the one being managed),
                    // or a run would fail with a reconnect error the
                    // user did not expect. The catalog in hand is
                    // reused rather than rebuilt from disk.
                    sweep_picks(&ctx, g.id, json, project, &catalog, &registry);
                } else {
                    println!("kept.");
                }
                return Ok(());
            }
            [n] if !grants.is_empty() && n.parse::<usize>().is_ok() => {
                let g = grant_by_number(&grants, n)?;
                cx.pick_grant(g)?;
                return Ok(());
            }
            _ if grants.is_empty() => {
                bail!("'{answer}' is not one of the choices; answer 'c', 'd', or 'q'")
            }
            _ => bail!(
                "'{answer}' is not one of the choices; answer a number, 'c', 'd', \
                 'f <N>', or 'q'"
            ),
        }
    }

    let grant = cx.connect_new(&opts).await?;
    cx.pick_grant(&grant)?;
    Ok(())
}

/// The ambient state of one `weft connect` invocation, threaded once
/// instead of five parameters through every flow.
struct Connecting<'a> {
    client: &'a DispatcherClient,
    target: &'a AccessTarget,
    /// The SERVICE's display label ("Slack"), never a connection's name.
    service_label: &'a str,
    registry: &'a std::sync::Arc<weft_core::weft_type::TypeRegistry>,
    interactive: bool,
    /// Under --json prose stays off stdout (one JSON object per line).
    json: bool,
}

/// Delete a stored connection for good. The ONE path to the delete
/// verb, shared with the menu and `weft test-node`'s ephemeral grants.
pub(crate) async fn forget_grant(client: &DispatcherClient, id: Uuid) -> Result<()> {
    client.delete(&format!("/access/grants/{id}")).await
}

/// After a store-wide forget: clear every pick in the current project
/// that pointed at the deleted row, so no node is left failing every
/// run with a reconnect error the user did not expect. Returns the
/// node ids whose picks were cleared. Outside a project there is
/// nothing to sweep (and nothing to say); a project that is here but
/// broken cannot fail the forget (the grant is already gone), so the
/// dangling picks and their recovery are named on stderr instead of
/// silently skipped.
fn clear_picks_pointing_at(ctx: &Ctx, id: Uuid, json: bool) -> Vec<String> {
    let project = match ctx.project_here() {
        Ok(Some(p)) => p,
        Ok(None) => return Vec::new(),
        Err(e) => {
            sweep_failed(&e);
            return Vec::new();
        }
    };
    match weft_compiler::build::build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))
    {
        Ok(catalog) => {
            let registry = catalog.type_registry();
            sweep_picks(ctx, id, json, project, &catalog, &registry)
        }
        Err(e) => {
            sweep_failed(&e);
            Vec::new()
        }
    }
}

/// The sweep itself, for a caller that already holds the project and
/// catalog (the interactive menu). Failures print the recovery on
/// stderr and never fail the forget; the cleared node ids come back.
fn sweep_picks(
    ctx: &Ctx,
    id: Uuid,
    json: bool,
    project: &weft_compiler::project::Project,
    catalog: &weft_catalog::FsCatalog,
    registry: &std::sync::Arc<weft_core::weft_type::TypeRegistry>,
) -> Vec<String> {
    let mut cleared = Vec::new();
    let client = ctx.client();
    let swept = (|| -> Result<()> {
        let targets = access_targets(&project.root, &project.main_weft(), project.id(), catalog)?;
        for target in &targets {
            if target.picked.handle().is_some_and(|p| p.id == id) {
                Connecting {
                    client: &client,
                    target,
                    service_label: target.spec.display_label(),
                    registry,
                    // The sweep only writes; nothing in clear_pick
                    // prompts, so interactivity is moot here.
                    interactive: false,
                    json,
                }
                .clear_pick()?;
                cleared.push(target.node.clone());
            }
        }
        Ok(())
    })();
    if let Err(e) = swept {
        sweep_failed(&e);
    }
    cleared
}

fn sweep_failed(e: &anyhow::Error) {
    eprintln!(
        "could not sweep this project's picks: {e}; a node still pointing at the \
         deleted connection can be cleared with `weft connect --node <id> --disconnect`"
    );
}

// ── Target discovery ────────────────────────────────────────────────────────

/// Walk main.weft and every `@include`d file (recursively) and pair
/// each access node with its service recipe and its currently picked
/// handle. Each FILE is parsed standalone and visited once: a subgraph
/// included from two places is one source file, so its access node is
/// one target and one pick serves every inclusion.
fn access_targets(
    root: &std::path::Path,
    entry: &std::path::Path,
    project_id: uuid::Uuid,
    catalog: &weft_catalog::FsCatalog,
) -> Result<Vec<AccessTarget>> {
    let mut out = Vec::new();
    let mut visited: std::collections::BTreeMap<std::path::PathBuf, Vec<usize>> =
        Default::default();
    let root = root
        .canonicalize()
        .with_context(|| format!("resolve {}", root.display()))?;
    collect_targets(
        entry,
        &root,
        None,
        project_id,
        catalog,
        &mut out,
        &mut visited,
    )?;
    Ok(out)
}

/// One file's pass: parse it standalone (the same per-file view the
/// editor edits), collect its access nodes, recurse into its includes.
/// `via` is the include alias this file was reached through (None for
/// main.weft); a file reached again only records the extra alias on
/// its existing targets.
fn collect_targets(
    file: &std::path::Path,
    root: &std::path::Path,
    via: Option<&str>,
    project_id: uuid::Uuid,
    catalog: &weft_catalog::FsCatalog,
    out: &mut Vec<AccessTarget>,
    visited: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<usize>>,
) -> Result<()> {
    let canonical = file
        .canonicalize()
        .with_context(|| format!("resolve {}", file.display()))?;
    if let Some(indices) = visited.get(&canonical) {
        // Second inclusion of the same file: same source, same pick;
        // just show the reader both routes to it.
        if let Some(alias) = via {
            for &i in indices {
                out[i].included_as.push(alias.to_string());
            }
        }
        return Ok(());
    }
    let source = std::fs::read_to_string(&canonical)
        .with_context(|| format!("read {}", canonical.display()))?;
    let source_id = weft_compiler::source_name::derive_id(Some(&canonical));
    let base = canonical
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_default();
    let (definition, diagnostics) = weft_compiler::parse_only(
        &source,
        project_id,
        weft_compiler::CompileFs::disk(&base),
        catalog,
        Some(&source_id),
    );
    // The lenient parse answers even on broken source, with a PARTIAL
    // node list; operating on that (and rewriting the file from it)
    // would silently work on half the project. Refuse on PARSE errors
    // only: type-level diagnostics (an unresolved MustOverride in a
    // subgraph viewed standalone, a mis-typed literal) leave the node
    // list complete and the editor edits such files freely.
    if let Some(first) = diagnostics.iter().find(|d| {
        matches!(d.severity, weft_core::node::Severity::Error) && d.code.as_deref() == Some("parse")
    }) {
        bail!(
            "{} does not parse (line {}: {}); fix it before connecting, or the \
             pick would be written into a file the compiler cannot read",
            canonical.display(),
            first.line,
            first.message
        );
    }
    let mut indices = Vec::new();
    let mut includes: Vec<(String, String)> = Vec::new();
    for node in &definition.nodes {
        if let Some(path) = &node.include_path {
            includes.push((node.id.clone(), path.clone()));
            continue;
        }
        let Some(meta) = weft_core::node::MetadataCatalog::lookup(catalog, &node.node_type) else {
            continue;
        };
        let Some(spec) = meta.service.clone() else {
            continue;
        };
        let input = meta
            .access_input()
            .map(|i| i.name.clone())
            .with_context(|| {
                format!(
                    "node type '{}' declares the '{}' service but no input carries the \
                     access widget; metadata load is supposed to refuse that, so the \
                     installed catalog is broken",
                    node.node_type, spec.service
                )
            })?;
        // The stored pick is an `{id, identity}` handle; anything else
        // in the field is unreadable and carried as such (listings say
        // so; picking or disconnecting overwrites it).
        let picked = match node.config.get(&input) {
            None | Some(Value::Null) => Pick::None,
            Some(v) => match parse_picked(v) {
                Ok(h) => Pick::Handle(h),
                Err(e) => Pick::Malformed(format!("{e:#}")),
            },
        };
        indices.push(out.len());
        out.push(AccessTarget {
            file: canonical.clone(),
            rel_file: canonical
                .strip_prefix(root)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| canonical.to_string_lossy().into_owned()),
            node: node.id.clone(),
            node_type: node.node_type.clone(),
            input,
            project_app: meta.access_apps.get(&spec.service).cloned(),
            spec,
            picked,
            included_as: via.map(str::to_string).into_iter().collect(),
        });
    }
    visited.insert(canonical.clone(), indices);
    for (alias, path) in includes {
        collect_targets(
            &base.join(&path),
            root,
            Some(&alias),
            project_id,
            catalog,
            out,
            visited,
        )?;
    }
    Ok(())
}

/// Parse a picker config value into the handle it must be.
fn parse_picked(v: &Value) -> Result<PickedHandle> {
    let obj = v
        .as_object()
        .context("the value is not an {id, identity} object")?;
    let id = obj
        .get("id")
        .and_then(Value::as_str)
        .context("the value has no string `id`")?
        .parse::<Uuid>()
        .context("the `id` is not a UUID")?;
    let identity = obj
        .get("identity")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(PickedHandle { id, identity })
}

/// A target's one-line description: node, type, and (for an included
/// file's node) where it lives and through which aliases.
fn describe_target(t: &AccessTarget) -> String {
    let mut s = format!("{} ({})", t.node, t.node_type);
    if !t.included_as.is_empty() {
        s.push_str(&format!(
            " in {}, included as {}",
            t.rel_file,
            t.included_as.join(" and ")
        ));
    }
    s
}

fn choose_target(
    mut targets: Vec<AccessTarget>,
    node: Option<&str>,
    json: bool,
) -> Result<AccessTarget> {
    if let Some(name) = node {
        // Bare id, or root-relative-file-qualified `nodes/a/sub.weft:node`
        // when the same id exists in two files.
        let matches_name = |t: &AccessTarget| {
            t.node == name
                || name
                    .split_once(':')
                    .is_some_and(|(f, n)| t.node == n && t.rel_file == f)
        };
        let count = targets.iter().filter(|t| matches_name(t)).count();
        if count > 1 {
            let qualified: Vec<String> = targets
                .iter()
                .filter(|t| matches_name(t))
                .map(|t| format!("{}:{}", t.rel_file, t.node))
                .collect();
            bail!(
                "'{name}' names an access node in more than one file; qualify it: {}",
                qualified.join(", ")
            );
        }
        let known = targets
            .iter()
            .map(describe_target)
            .collect::<Vec<_>>()
            .join(", ");
        return targets
            .into_iter()
            .find(|t| matches_name(t))
            .with_context(|| {
                format!("no access node '{name}' in this project (the access nodes are: {known})")
            });
    }
    if targets.len() == 1 {
        let t = targets.remove(0);
        if !json {
            println!("Managing the one access node: {}.", describe_target(&t));
        }
        return Ok(t);
    }
    // Prompt context rides the prompt's stream (stderr), so a --json
    // run's stdout stays one JSON object per line even when the node
    // has to be chosen interactively.
    eprintln!("Access nodes in this project:");
    for (i, t) in targets.iter().enumerate() {
        let status = match &t.picked {
            Pick::Handle(p) => format!("connected as {}", p.who()),
            Pick::None => "NOT connected".to_string(),
            Pick::Malformed(_) => "holds an unreadable value (pick or --disconnect to replace it)".to_string(),
        };
        eprintln!("  [{}] {} - {}", i + 1, describe_target(t), status);
    }
    let choice = prompt_line("Which node? ", "--node <id>")?;
    let idx: usize = choice
        .trim()
        .parse()
        .ok()
        .filter(|i| (1..=targets.len()).contains(i))
        .with_context(|| format!("pick a number between 1 and {}", targets.len()))?;
    Ok(targets.remove(idx - 1))
}

// ── Stored connections ──────────────────────────────────────────────────────

/// The tenant's stored connections for one service, typed. Shared with
/// `weft test-node`'s live tier, so the CLI has ONE path to the grants
/// API.
pub(crate) async fn list_grants(
    client: &DispatcherClient,
    service: Option<&str>,
) -> Result<Vec<GrantSummary>> {
    let path = match service {
        Some(s) => format!("/access/grants?service={}", percent_encode(s)),
        // No service: every stored connection, every service (the
        // store models the filter as optional the same way). The
        // store-wide listing `--list` falls back to when no project
        // scopes it, and what the recovery hints other commands print
        // (`weft connect --list`) rely on, so it works from anywhere.
        None => "/access/grants".to_string(),
    };
    let rows = client.get_json(&path).await?;
    serde_json::from_value(rows).context("parse the connection list")
}

/// Render the store-wide listing (`--list` with no project to scope
/// it): one line per connection with its service, since no node
/// context exists to mark a pick against.
async fn print_all_grants(client: &DispatcherClient, json: bool) -> Result<()> {
    let grants = list_grants(client, None).await?;
    if json {
        println!("{}", serde_json::json!({ "connections": grants }));
        return Ok(());
    }
    if grants.is_empty() {
        println!("No stored connections yet.");
        return Ok(());
    }
    println!("Stored connections (every service):");
    for (i, g) in grants.iter().enumerate() {
        let who = g
            .identity
            .clone()
            .or_else(|| g.label.clone())
            .unwrap_or_else(|| g.id.to_string());
        println!("  [{}] {} - {}  (id {})", i + 1, g.service, who, g.id);
    }
    Ok(())
}

fn grant_by_number<'a>(grants: &'a [GrantSummary], raw: &str) -> Result<&'a GrantSummary> {
    let idx: usize = raw
        .parse()
        .ok()
        .filter(|i| (1..=grants.len()).contains(i))
        .with_context(|| format!("pick a number between 1 and {}", grants.len()))?;
    Ok(&grants[idx - 1])
}

fn print_grants(target: &AccessTarget, label: &str, grants: &[GrantSummary]) {
    match &target.picked {
        Pick::Handle(p) => println!("'{}' is connected as {}.", target.node, p.who()),
        Pick::None => println!("'{}' has no connection picked.", target.node),
        Pick::Malformed(why) => println!(
            "'{}' holds a value `weft connect` cannot read ({why}); pick a connection \
             or --disconnect to replace it.",
            target.node
        ),
    }
    if grants.is_empty() {
        println!("No stored {label} connections yet.");
        return;
    }
    println!("Stored {label} connections:");
    for (i, g) in grants.iter().enumerate() {
        let who = g
            .identity
            .clone()
            .or_else(|| g.label.clone())
            .unwrap_or_else(|| g.id.to_string());
        let can = if g.owner == CredentialOwner::Ours {
            "uses your credits".to_string()
        } else if g.scopes.is_empty() {
            "full access of its credential".to_string()
        } else {
            let joined = g.scopes.join(", ");
            if g.permissions_verified {
                joined
            } else {
                format!("{joined} (claimed)")
            }
        };
        let picked = if target.picked.handle().is_some_and(|p| p.id == g.id) {
            "  <- picked"
        } else {
            ""
        };
        println!("  [{}] {who} - {can}  (id {}){picked}", i + 1, g.id);
    }
}

impl Connecting<'_> {
    /// Point the node at this stored connection and say so. When the
    /// write fails the connection still exists, so the error names the
    /// way to attach it without redoing the sign-in.
    fn pick_grant(&self, g: &GrantSummary) -> Result<()> {
        let handle = match &g.identity {
            Some(identity) => serde_json::json!({ "id": g.id, "identity": identity }),
            None => serde_json::json!({ "id": g.id }),
        };
        self.write_pick(Some(&handle)).with_context(|| {
            format!(
                "the connection is stored as {}; attach it with `weft connect --node {} \
                 --grant {}`",
                g.id, self.target.node, g.id
            )
        })?;
        if !self.json {
            println!(
                "'{}' now uses {}.",
                self.target.node,
                g.identity.clone().unwrap_or_else(|| g.id.to_string())
            );
        }
        Ok(())
    }

    /// Clear the node's pick (the stored connection stays) and say so.
    fn clear_pick(&self) -> Result<()> {
        self.write_pick(None)?;
        if !self.json {
            println!("'{}' now has no connection picked.", self.target.node);
        }
        Ok(())
    }

    /// Write the connection handle onto the node in its own source file
    /// through the compiler's structural edit ops: the same operations
    /// a click in the editor performs, never string surgery on the
    /// source. `None` clears the pick (the editor removes the key
    /// rather than writing null).
    fn write_pick(&self, handle: Option<&Value>) -> Result<()> {
        let file = &self.target.file;
        let node = &self.target.node;
        let input = &self.target.input;
        let source =
            std::fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
        let op = match handle {
            Some(h) => weft_compiler::edit::EditOp::SetConfig {
                node: node.clone(),
                key: input.clone(),
                value: h.to_string(),
                form: None,
            },
            None => weft_compiler::edit::EditOp::RemoveConfig {
                node: node.clone(),
                key: input.clone(),
                form: None,
            },
        };
        // The file's own filename-derived anon-root id, the same identity
        // the editor edits it under when navigated into it.
        let source_id = weft_compiler::source_name::derive_id(Some(file));
        let (edited, _inverse) = weft_compiler::edit::apply_edits(
            &source,
            None,
            &source_id,
            &[op],
            self.registry.clone(),
        )
        .map_err(|e| anyhow::anyhow!("write the pick onto {node}.{input}: {e}"))?;
        std::fs::write(file, edited).with_context(|| format!("write {}", file.display()))?;
        Ok(())
    }
}

// ── Connecting a new account ────────────────────────────────────────────────

impl Connecting<'_> {
    async fn connect_new(&self, opts: &ConnectOpts) -> Result<GrantSummary> {
        let (client, label) = (self.client, self.service_label);
        let spec = &self.target.spec;
        let doors: DoorsStatus = serde_json::from_value(
            client
                .post_json(
                    "/access/doors",
                    &serde_json::to_value(DoorsRequest { spec: spec.clone() })?,
                )
                .await?,
        )
        .context("parse the doors probe")?;
        let is_consent = spec.needs_browser_consent();
        let shared_backed = (!doors.doors.shared_apps.is_empty() || doors.doors.shared_credential)
            && spec.doors.contains(&Door::Shared)
            && !(is_consent && doors.consent_blocked.is_some());
        let own_offered = spec.doors.contains(&Door::Own);

        let door = match opts.door.as_deref() {
            Some("shared") => Door::Shared,
            Some(_) => Door::Own,
            None if shared_backed && own_offered => {
                let shared_desc = if is_consent {
                    "sign in through weft's app, one click"
                } else {
                    "use weft's key, spends your credits"
                };
                let choice = prompt_line(
                    &format!(
                        "How do you want to connect {label}?\n  [1] shared: {shared_desc}\n  [2] own: bring your own credential\nDoor [1/2]: "
                    ),
                    "--door shared|own",
                )?;
                match choice.trim() {
                    "1" => Door::Shared,
                    "2" => Door::Own,
                    other => bail!("'{other}' is not a door; answer 1 or 2 (or pass --door)"),
                }
            }
            None if shared_backed => Door::Shared,
            None if own_offered => Door::Own,
            None => bail!(
                "no connect door is available for {label} right now{}",
                doors
                    .consent_blocked
                    .as_deref()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default()
            ),
        };

        if door == Door::Shared && !shared_backed {
            bail!(
                "the shared door has nothing behind it for {label} right now{}; connect \
                 with --door own instead",
                doors
                    .consent_blocked
                    .as_deref()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default()
            );
        }
        if door == Door::Own && !own_offered {
            bail!("{label} does not offer the own door");
        }
        if door == Door::Own && opts.shared_app.is_some() {
            bail!("--shared-app picks a shared-door app; it does nothing with --door own");
        }
        if door == Door::Shared {
            // The own-door flags would be silently dropped on this path;
            // dropping a pasted credential on the floor is the worst way
            // to ignore a flag.
            if !opts.set.is_empty() || !opts.set_env.is_empty() || opts.paste || opts.mint {
                bail!("--set/--set-env/--paste/--mint fill your own credential; they do nothing with --door shared");
            }
            if opts.label.is_some() {
                bail!("--label names a connection you create yourself; the shared door names it after the app");
            }
            // The shared door's permission set is the chosen app's fixed
            // one (or the runtime credential's full reach); nothing a
            // client asks for widens or narrows it.
            if opts.permissions.is_some() {
                bail!(
                    "--permissions applies to the own door; the shared door's permission \
                     set is the app's declared one (shown beside each app)"
                );
            }
            // Any OAuth2 service signs in through a REGISTERED APP on the
            // shared door (a consent, or a server-to-server exchange), so
            // the app choice keys on is_oauth; only a key service uses
            // the runtime's own credential and has no app to pick.
            let app = choose_shared_app(&doors.doors, opts.shared_app.as_deref(), spec.is_oauth())?;
            if is_consent {
                // The one-time displacement warning the editor shows before
                // a shared-door sign-in; an explicit --door shared already
                // is the acknowledgement.
                if opts.door.is_none() {
                    println!(
                        "Note: with a shared app, someone else in the same workspace using it \
                         too may disconnect this connection. Setting up your own avoids that."
                    );
                }
                return self
                    .consent_flow(
                        Door::Shared,
                        Vec::new(),
                        app.map(|a| a.label.clone()),
                        None,
                        None,
                    )
                    .await;
            }
            // A registered app's exchange lands as the user's own account;
            // only the runtime's own key spends its credit.
            if !spec.is_oauth() {
                println!("One click: calls on this connection spend your credits.");
            }
            return connect_direct(
                client,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Shared,
                    values: BTreeMap::new(),
                    label: None,
                    permissions: Vec::new(),
                    registration: None,
                    paste: false,
                    project_id: None,
                },
                app.map(|a| a.label.clone()),
            )
            .await;
        }

        // The own door: one page. A paste section, a mint button, a guide,
        // and the credential fields, exactly the parts the spec declares.
        let ticked = ticked_permissions(spec, opts, self.interactive)?;
        self.own_flow(opts, ticked, &doors).await
    }
}

fn choose_shared_app<'a>(
    doors: &'a DoorsAnswer,
    flag: Option<&str>,
    uses_app: bool,
) -> Result<Option<&'a SharedAppChoice>> {
    if !uses_app {
        if flag.is_some() {
            bail!(
                "--shared-app picks a registered app; this service's shared door uses \
                 the runtime's own credential and no app applies"
            );
        }
        return Ok(None);
    }
    if let Some(wanted) = flag {
        return doors
            .shared_apps
            .iter()
            .find(|a| a.label == wanted)
            .map(Some)
            .with_context(|| {
                let known: Vec<&str> = doors.shared_apps.iter().map(|a| a.label.as_str()).collect();
                format!(
                    "no shared app '{wanted}' (the offered apps are: {})",
                    known.join(", ")
                )
            });
    }
    match doors.shared_apps.len() {
        0 => Ok(None),
        1 => Ok(doors.shared_apps.first()),
        _ => {
            println!("Shared apps:");
            for (i, a) in doors.shared_apps.iter().enumerate() {
                println!("  [{}] {} ({})", i + 1, a.label, a.covers.join(", "));
            }
            let choice = prompt_line("Which app? ", "--shared-app <label>")?;
            let idx: usize = choice
                .trim()
                .parse()
                .ok()
                .filter(|i| (1..=doors.shared_apps.len()).contains(i))
                .with_context(|| {
                    format!("pick a number between 1 and {}", doors.shared_apps.len())
                })?;
            Ok(Some(&doors.shared_apps[idx - 1]))
        }
    }
}

/// The permission set a new connect asks for: the --permissions flag,
/// else (interactively) the catalogue with the defaults offered, else
/// the spec's default set (the same set the editor starts with).
fn ticked_permissions(
    spec: &AccessSpec,
    opts: &ConnectOpts,
    interactive: bool,
) -> Result<Vec<String>> {
    // Own-account-only entries are capability declarations, never
    // consent asks; they are set up in the provider's account and can
    // never ride a consent URL, so they are not askable here either.
    let tickable: Vec<_> = spec.permissions.iter().filter(|p| !p.own_only).collect();
    let askable = |id: &str| tickable.iter().any(|p| p.id == id);
    if let Some(ids) = &opts.permissions {
        for id in ids {
            if !askable(id) {
                let known: Vec<&str> = tickable.iter().map(|p| p.id.as_str()).collect();
                let why = if spec.declares_permission(id) {
                    format!(
                        "'{id}' is an own-account-only capability: it is set up inside \
                         the connected account, never asked for at connect"
                    )
                } else {
                    format!(
                        "'{id}' is not in the {} permission catalogue",
                        spec.display_label()
                    )
                };
                bail!("{why} (the askable ids are: {})", known.join(", "));
            }
        }
        return Ok(ids.clone());
    }
    let defaults = spec.default_permissions();
    if tickable.is_empty() || !interactive {
        return Ok(defaults);
    }
    println!("Permissions to ask for ([x] = in the default set):");
    for p in &tickable {
        let mark = if defaults.contains(&p.id) { "x" } else { " " };
        println!("  [{mark}] {} - {} ({})", p.id, p.label, p.description);
    }
    if let Some(url) = &spec.all_permissions_url {
        println!(
            "  (a permission missing here can be found at {url} and added to the node's metadata)"
        );
    }
    let answer = prompt_line(
        "Permission ids, comma-separated (Enter keeps the defaults): ",
        "--permissions <id,id>",
    )?;
    if answer.trim().is_empty() {
        return Ok(defaults);
    }
    let ids: Vec<String> = answer.split(',').map(|s| s.trim().to_string()).collect();
    for id in &ids {
        if !askable(id) {
            bail!("'{id}' is not in the permission list above");
        }
    }
    Ok(ids)
}

impl Connecting<'_> {
    async fn own_flow(
        &self,
        opts: &ConnectOpts,
        ticked: Vec<String>,
        doors: &DoorsStatus,
    ) -> Result<GrantSummary> {
        let (client, label, interactive) = (self.client, self.service_label, self.interactive);
        let target = self.target;
        let spec = &target.spec;
        // An OAuth2 service signs in through an app either way; only an
        // authorization-code grant also needs a browser (client_credentials
        // exchanges the app's credentials server-to-server, direct path).
        let uses_app = spec.is_oauth();
        let is_consent = spec.needs_browser_consent();
        let mut set = parse_set_values(&opts.set, &opts.set_env)?;

        // The ready-credential section of a consent service: paste a token,
        // no app, no browser.
        if opts.paste {
            let paste_fields = spec
                .own_page
                .as_ref()
                .and_then(|p| p.paste.as_ref())
                .map(|p| p.fields.clone())
                .with_context(|| format!("{label} has no paste-a-credential section"))?;
            let values = collect_field_values(
                &paste_fields,
                &mut set,
                "--set name=value",
                interactive,
                false,
            )?;
            refuse_strays(&set, &paste_fields)?;
            return connect_direct(
                client,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Own,
                    values,
                    label: connection_name(opts, label, interactive)?,
                    permissions: ticked,
                    registration: None,
                    paste: true,
                    project_id: None,
                },
                None,
            )
            .await;
        }

        // The generated how-to guide, permissions interpolated, exactly as
        // the own page renders it. Terminal-only: in a pipe it is noise a
        // script would have to skip.
        let steps = spec.guide_steps(&ticked);
        if interactive
            && !steps.is_empty()
            && opts.set.is_empty()
            && opts.set_env.is_empty()
            && !opts.mint
        {
            println!("How to create your own {label} credential:");
            if let Some(link) = spec.guide_link(&ticked) {
                println!("  {link}");
            }
            for (i, s) in steps.iter().enumerate() {
                println!("  {}. {s}", i + 1);
            }
            // A consent app needs the provider to know where to send the
            // user back; the editor's own page shows this too.
            if is_consent {
                if let Some(uri) = &doors.redirect_uri {
                    println!("  Register this callback URL on the provider's site:\n    {uri}");
                }
            }
        }

        // "Create it for me": mint the provider app and prefill its
        // credentials, exactly like the editor's mint button. Minted values
        // stay their own map so a stray one is blamed on the MINT, never on
        // a --set the user did not pass.
        let mut minted: BTreeMap<String, String> = BTreeMap::new();
        if opts.mint {
            if spec
                .own_page
                .as_ref()
                .and_then(|p| p.mint.as_ref())
                .is_none()
            {
                bail!("{label} has no create-it-for-me mint");
            }
            let req = MintAppRequest {
                spec: spec.clone(),
                permissions: ticked.clone(),
            };
            let answer: MintAppResponse = serde_json::from_value(
                client
                    .post_json("/access/mint-app", &serde_json::to_value(&req)?)
                    .await?,
            )
            .context("parse the minted app's values")?;
            println!("Minted a fresh {label} app.");
            minted = answer.values;
        }

        let fields = spec.own_fields();
        for f in &fields {
            if let Some(v) = minted.remove(&f.name) {
                set.entry(f.name.clone()).or_insert(v);
            }
        }
        if let Some(stray) = minted.keys().next() {
            let known: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
            bail!(
                "the mint returned a value '{stray}' that {label} declares no field for \
             (the fields are: {})",
                known.join(", ")
            );
        }

        if uses_app {
            if is_consent {
                if let Some(reason) = doors.consent_blocked.as_deref() {
                    bail!(
                        "a browser sign-in cannot run right now ({reason}); if {label} has a \
                     paste-a-credential section, use --paste"
                    );
                }
            }
            // The app, mirroring the editor's own page: the form's fields
            // first (every own field optional at the PROMPT so pressing
            // Enter through all of them is a real answer), then decide.
            // Anything typed means the user's own app, all-or-nothing; an
            // entirely blank form falls back to the project's declared app.
            let typed =
                collect_field_values(&fields, &mut set, "--set name=value", interactive, true)?;
            refuse_strays(&set, &fields)?;
            // The connection's name is asked at most once; the own-app path
            // reuses it for both the app label and the connection label.
            let mut chosen_name: Option<String> = None;
            let registration = if typed.is_empty() {
                // The grant's name IS the app's label on this path, so a
                // user-given --label genuinely cannot apply; refuse it
                // rather than drop it.
                if opts.label.is_some() {
                    bail!(
                        "--label names an app you register yourself; the project's declared \
                     app names the connection. Fill the app fields to use your own."
                    );
                }
                match &target.project_app {
                    Some(app) => {
                        println!("Using the project's declared {label} app.");
                        Some(app.clone())
                    }
                    None => bail!(
                        "{label} needs an app to sign in through and this project declares \
                     none; fill the app fields (or pass --set) to use your own{}",
                        if spec.own_page.as_ref().is_some_and(|p| p.mint.is_some()) {
                            ", or pass --mint to create one"
                        } else {
                            ""
                        }
                    ),
                }
            } else {
                if let Some(missing) = fields
                    .iter()
                    .find(|f| !f.optional && !typed.contains_key(&f.name))
                {
                    bail!(
                        "fill in '{}' to use your own app, or leave every app field blank to \
                     use the project's app",
                        missing
                            .label
                            .clone()
                            .unwrap_or_else(|| missing.name.clone())
                    );
                }
                chosen_name = connection_name(opts, label, interactive)?;
                let mut reg = AppRegistration {
                    label: chosen_name.clone().unwrap_or_else(|| label.to_string()),
                    client_id: typed.get("client_id").cloned().unwrap_or_default(),
                    client_secret: typed.get("client_secret").cloned(),
                    extra: BTreeMap::new(),
                };
                for (k, v) in typed {
                    if k != "client_id" && k != "client_secret" {
                        reg.extra.insert(k, v);
                    }
                }
                Some(reg)
            };
            if is_consent {
                return self
                    .consent_flow(Door::Own, ticked, None, None, registration)
                    .await;
            }
            // client_credentials: the same app, exchanged server-to-server
            // in one request, no browser.
            return connect_direct(
                client,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Own,
                    values: BTreeMap::new(),
                    label: chosen_name,
                    permissions: ticked,
                    registration,
                    paste: false,
                    project_id: None,
                },
                None,
            )
            .await;
        }

        // A pasted-credential service: collect the fields and connect in
        // one request.
        let values =
            collect_field_values(&fields, &mut set, "--set name=value", interactive, false)?;
        refuse_strays(&set, &fields)?;
        connect_direct(
            client,
            ConnectDirect {
                spec: spec.clone(),
                door: Door::Own,
                values,
                label: connection_name(opts, label, interactive)?,
                permissions: ticked,
                registration: None,
                paste: false,
                project_id: None,
            },
            None,
        )
        .await
    }
}

/// The connection's list name: the --label flag, else an interactive
/// prompt (Enter keeps the service's own label), else nothing.
fn connection_name(
    opts: &ConnectOpts,
    service_label: &str,
    interactive: bool,
) -> Result<Option<String>> {
    if let Some(l) = &opts.label {
        return Ok(Some(l.clone()));
    }
    if !interactive {
        return Ok(None);
    }
    let line = prompt_line(
        &format!("A name for this connection (Enter for \"{service_label}\"): "),
        "--label <name>",
    )?;
    Ok(if line.trim().is_empty() {
        None
    } else {
        Some(line.trim().to_string())
    })
}

/// One direct connect (paste / shared key / server-to-server). Shared
/// with `weft test-node`'s live tier.
pub(crate) async fn connect_direct(
    client: &DispatcherClient,
    req: ConnectDirect,
    shared_app: Option<String>,
) -> Result<GrantSummary> {
    let body = serde_json::to_value(&SharedDoorPick {
        shared_app,
        inner: req,
    })?;
    let done = client.post_json("/access/connect/direct", &body).await?;
    grant_of(done)
}

impl Connecting<'_> {
    /// The browser sign-in: begin, open the consent page, poll the parked
    /// outcome until the callback lands (same 2s x 150 window as the
    /// editor). Refused with no terminal: a script cannot finish a browser
    /// consent, and blocking five minutes on a poll nobody watches is the
    /// hang the flag rule exists to prevent.
    async fn consent_flow(
        &self,
        door: Door,
        ticked: Vec<String>,
        shared_app: Option<String>,
        upgrade_grant_id: Option<Uuid>,
        registration: Option<AppRegistration>,
    ) -> Result<GrantSummary> {
        let (client, label, target) = (self.client, self.service_label, self.target);
        if !self.interactive {
            bail!(
                "{label} needs a browser sign-in, which cannot finish without a terminal. \
             Run `weft connect` yourself{}",
                if target
                    .spec
                    .own_page
                    .as_ref()
                    .is_some_and(|p| p.paste.is_some())
                {
                    ", or connect a ready credential with --paste --set name=value"
                } else {
                    ""
                }
            );
        }
        let req = BeginOAuth {
            spec: target.spec.clone(),
            door,
            registration,
            permissions: ticked,
            // The editor sends no project id on a user connect either; the
            // column means "published by a node in this project".
            project_id: None,
            upgrade_grant_id,
            // Filled by the dispatcher (it knows its public host).
            redirect_uri: String::new(),
        };
        let body = serde_json::to_value(&SharedDoorPick {
            shared_app,
            inner: req,
        })?;
        let started: StartedOAuth =
            serde_json::from_value(client.post_json("/access/connect/begin", &body).await?)
                .context("parse the sign-in start")?;
        println!(
            "Finish the sign-in in your browser:\n  {}",
            started.consent_url
        );
        if let Err(e) = open::that_detached(&started.consent_url) {
            println!("(could not open a browser here: {e}; open the address yourself)");
        }
        // The grant is created by the provider's callback hitting the
        // dispatcher, independent of this poll: a Ctrl+C or a network blip
        // here does NOT undo a sign-in that already landed.
        println!(
            "Waiting for the sign-in to land (Ctrl+C to stop waiting; if the sign-in \
         completed anyway, `weft connect` will list the connection)..."
        );
        for _ in 0..150 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let outcome = client
                .get_json(&format!(
                    "/access/connect/status?state={}",
                    percent_encode(&started.state)
                ))
                .await
                // A dropped poll does not undo a sign-in that already
                // landed; the caller needs to know where to look.
                .context(
                    "polling the sign-in outcome failed; if the sign-in completed anyway, \
                 the connection shows in `weft connect --list`",
                )?;
            if outcome.is_null() {
                continue;
            }
            if let Some(err) = outcome.get("error").and_then(Value::as_str) {
                bail!("the sign-in failed: {err}");
            }
            return grant_of(outcome);
        }
        bail!(
            "the sign-in did not land within 5 minutes; run `weft connect` again to retry \
         (if it completed after this gave up, the connection shows in the list)"
        );
    }
}

/// The grant out of a connect answer. Pure: the caller prints.
fn grant_of(answer: Value) -> Result<GrantSummary> {
    let done: CompletedConnect =
        serde_json::from_value(answer).context("parse the connect answer")?;
    Ok(done.grant)
}

// ── Field input ─────────────────────────────────────────────────────────────

fn parse_set_values(set: &[String], set_env: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut put = |k: &str, v: String| -> Result<()> {
        // A repeated key silently last-winning would drop a credential
        // on the floor; the key is safe to name, the values are not.
        if out.insert(k.trim().to_string(), v).is_some() {
            bail!("'{}' was set twice; pass each field once", k.trim());
        }
        Ok(())
    };
    for (i, raw) in set.iter().enumerate() {
        // The error must never echo the value: a user who forgot the
        // `name=` has just typed a SECRET, and printing it puts it in
        // the terminal scrollback and any captured logs.
        let (k, v) = raw
            .split_once('=')
            .with_context(|| format!("--set takes name=value; argument {} has no '='", i + 1))?;
        put(k, v.to_string())?;
    }
    for raw in set_env {
        let (k, var) = raw
            .split_once('=')
            .with_context(|| format!("--set-env takes name=ENV_VAR; '{raw}' has no '='"))?;
        let v = std::env::var(var.trim()).with_context(|| {
            format!(
                "--set-env {raw}: the environment variable '{}' is not set",
                var.trim()
            )
        })?;
        put(k, v)?;
    }
    Ok(out)
}

/// The values for a declared field list: --set wins, then an interactive
/// prompt per missing field (hidden input for secrets). `all_optional`
/// relaxes every field to Enter-to-skip (the own-app form, where an
/// entirely blank form means "use the project's app"); otherwise a
/// missing required field with no terminal bails naming the flag, and
/// optional fields left blank store nothing, same as the editor's form.
pub(crate) fn collect_field_values(
    fields: &[CredentialField],
    set: &mut BTreeMap<String, String>,
    flag_hint: &str,
    interactive: bool,
    all_optional: bool,
) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for f in fields {
        let optional = f.optional || all_optional;
        let label = f.label.clone().unwrap_or_else(|| f.name.clone());
        let (value, supplied_by_flag) = match set.remove(&f.name) {
            Some(v) => (v, true),
            // An optional field with no --set and no terminal simply
            // stores nothing (the editor's form left blank); only a
            // REQUIRED field is worth failing a scripted run over.
            None if !interactive && optional => continue,
            None if !interactive => bail!(
                "'{label}' has no value; pass {flag_hint} (for '{}') to fill it \
                 non-interactively",
                f.name
            ),
            None => {
                let suffix = if optional {
                    " (optional, Enter to skip)"
                } else {
                    ""
                };
                let placeholder = f
                    .placeholder
                    .as_deref()
                    .map(|p| format!(" [{p}]"))
                    .unwrap_or_default();
                let text = format!("{label}{placeholder}{suffix}: ");
                let typed = if f.secret {
                    secret_line(&text)?
                } else {
                    prompt_line(&text, &format!("{flag_hint} (for '{}')", f.name))?
                };
                (typed, false)
            }
        };
        let value = value.trim().to_string();
        if value.is_empty() {
            // An EXPLICIT --set/--set-env with an empty value is a
            // mistake worth stopping on, even on an optional field:
            // treating it as "not supplied" would silently change
            // which credential or app the flow uses.
            if supplied_by_flag {
                bail!("'{label}' was set to an empty value; omit the flag to leave it unset");
            }
            if optional {
                continue;
            }
            bail!("'{label}' cannot be empty");
        }
        out.insert(f.name.clone(), value);
    }
    Ok(out)
}

/// A --set / --set-env key naming no declared field is a typo worth
/// stopping on: the value would silently go nowhere.
fn refuse_strays(set: &BTreeMap<String, String>, fields: &[CredentialField]) -> Result<()> {
    if let Some(stray) = set.keys().next() {
        let known: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        bail!(
            "no field named '{stray}' is declared (--set/--set-env); the fields are: {}",
            known.join(", ")
        );
    }
    Ok(())
}

/// prompt_line for a secret: hidden echo. Only ever reached
/// interactively (collect_field_values gates first).
fn secret_line(prompt: &str) -> Result<String> {
    Ok(rpassword::prompt_password(prompt)?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, optional: bool, secret: bool) -> CredentialField {
        CredentialField {
            name: name.into(),
            label: None,
            optional,
            secret,
            placeholder: None,
        }
    }

    #[test]
    fn set_values_parse_and_never_echo_a_secret() {
        let m = parse_set_values(&["key=sk-or-123".into(), "region=eu=west".into()], &[]).unwrap();
        assert_eq!(m["key"], "sk-or-123");
        // The first '=' splits; the value keeps the rest verbatim.
        assert_eq!(m["region"], "eu=west");
        // A bare word is refused WITHOUT echoing it: it may be a pasted
        // secret missing its `name=`.
        let err = parse_set_values(&["sk-live-oops".into()], &[])
            .unwrap_err()
            .to_string();
        assert!(!err.contains("sk-live-oops"), "{err}");
        assert!(err.contains("argument 1"), "{err}");
        // A repeated key is refused by NAME only, never the values.
        let err = parse_set_values(&["key=a".into(), "key=b".into()], &[])
            .unwrap_err()
            .to_string();
        assert_eq!(err, "'key' was set twice; pass each field once");
    }

    #[test]
    fn set_env_reads_the_environment() {
        std::env::set_var("WEFT_CONNECT_TEST_SECRET", "shh");
        let m = parse_set_values(&[], &["key=WEFT_CONNECT_TEST_SECRET".into()]).unwrap();
        assert_eq!(m["key"], "shh");
        let err = parse_set_values(&[], &["key=WEFT_CONNECT_TEST_UNSET".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("WEFT_CONNECT_TEST_UNSET"), "{err}");
    }

    #[test]
    fn grant_numbers_are_one_based_and_bounded() {
        let grants = vec![GrantSummary {
            id: uuid::Uuid::nil(),
            service: "s".into(),
            project_id: None,
            identity: None,
            label: None,
            scopes: vec![],
            permissions_verified: false,
            value_names: vec![],
            owner: CredentialOwner::TheirOwn,
            door: Door::Own,
            expires_at: None,
        }];
        assert_eq!(grant_by_number(&grants, "1").unwrap().id, uuid::Uuid::nil());
        assert!(grant_by_number(&grants, "0").is_err());
        assert!(grant_by_number(&grants, "2").is_err());
        assert!(grant_by_number(&grants, "x").is_err());
    }

    #[test]
    fn collected_fields_take_set_values_and_refuse_strays() {
        let fields = vec![field("key", false, true), field("region", true, false)];
        // All required values via --set, non-interactive: the optional
        // field left unset stores nothing.
        let mut set = parse_set_values(&["key=k".into()], &[]).unwrap();
        let out = collect_field_values(&fields, &mut set, "--set", false, false).unwrap();
        assert_eq!(out.get("key").map(String::as_str), Some("k"));
        assert!(!out.contains_key("region"));
        refuse_strays(&set, &fields).unwrap();
        // A stray key is refused by name.
        let mut stray = parse_set_values(&["key=k".into(), "nope=v".into()], &[]).unwrap();
        collect_field_values(&fields, &mut stray, "--set", false, false).unwrap();
        let err = refuse_strays(&stray, &fields).unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn required_field_with_no_terminal_names_the_flag() {
        let fields = vec![field("key", false, true)];
        let mut set = BTreeMap::new();
        let err = collect_field_values(&fields, &mut set, "--set", false, false).unwrap_err();
        assert!(err.to_string().contains("--set (for 'key')"), "{err}");
    }

    #[test]
    fn all_optional_relaxes_required_fields_for_the_own_app_form() {
        let fields = vec![
            field("client_id", false, false),
            field("client_secret", false, true),
        ];
        let mut set = BTreeMap::new();
        let out = collect_field_values(&fields, &mut set, "--set", false, true).unwrap();
        assert!(out.is_empty());
    }
}
