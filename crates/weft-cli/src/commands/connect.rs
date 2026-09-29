//! `weft connect`: manage a node's service connection from the terminal,
//! with the same abilities as the editor's Connect panel: list the stored
//! connections and pick one, connect a new account through the shared or
//! the own door (paste a key, run a browser sign-in, mint an app, use the
//! paste section of a consent service), upgrade an exclusive grant,
//! disconnect the node, or forget a stored connection for good.
//!
//! Every choice has a flag (see [`ConnectOpts`]) so scripts and AI runs
//! never hang on a prompt; interactively the command walks the same
//! decisions as menus. The pick is kept by the install, per place of the
//! node (`weft_core::picks`), never written in the source: a connection's
//! id means nothing on another install, so `--on <target>` picks for that
//! one. Pasted secrets go terminal -> store and never touch the source.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use uuid::Uuid;
use weft_core::picks::{ChangePicks, PickInput};
use weft_core::run_spec::MemberFieldRef;
use weft_core::access::spec::{
    percent_encode, AccessSpec, AppRegistration, CredentialField, Door, GrantCoexistence,
};
use weft_core::access::wire::{
    BeginOAuth, CompletedConnect, ConnectDirect, DoorsAnswer, DoorsRequest, DoorsStatus,
    GrantSummary, MintAppRequest, MintAppResponse, SharedAppChoice, SharedDoorPick, StartedOAuth,
};

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
    /// Act as this member of the program, on a node marked `@per_member`:
    /// list, connect and pick THEIR connections, exactly as their connect
    /// page would. For trying a program's member path from the terminal.
    #[arg(long, conflicts_with_all = ["forget", "upgrade", "mint"])]
    pub member: Option<weft_core::member::MemberId>,
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
/// each file visited ONCE: a subgraph included in two places is one
/// node with two places, and the install keeps a pick per place
/// (`weft_core::picks`), so the one this target answers for is the place
/// it is named by ([`AccessTarget::spelling`]).
pub(crate) struct AccessTarget {
    /// The file the node is written in, relative to the project root, the
    /// name `--node file:id` qualification and every message use.
    rel_file: String,
    /// The node's id, as the compiler keys it: for a node inside an
    /// included file that carries the file's path (`@src:sweep.key`).
    /// What a person reads and types is `spellings`.
    pub(crate) node: String,
    node_type: String,
    input: String,
    pub(crate) spec: AccessSpec,
    /// The project's declared public app for this service, the own
    /// door's fallback when the user pastes no app.
    project_app: Option<AppRegistration>,
    /// What the node's connection is at the place it is named by: this
    /// install's pick, or what the source says instead.
    pub(crate) picked: Pick,
    /// Every way a person names this node: one per place it is at in
    /// the program (see [`spellings_of`]). A node of the entry file has
    /// one; a node inside a file included twice has two, and both work.
    spellings: Vec<String>,
}

/// The `{id, identity}` handle a pick is kept as.
pub(crate) struct PickedHandle {
    pub(crate) id: Uuid,
    identity: Option<String>,
}

impl PickedHandle {
    /// Who this pick names, for display.
    fn who(&self) -> String {
        self.identity.clone().unwrap_or_else(|| self.id.to_string())
    }
}

/// What a node's connection is. The install keeps the pick
/// (`weft_core::picks`); the source only says when it is not the
/// install's to keep.
pub(crate) enum Pick {
    /// Nothing picked on this install yet.
    None,
    Handle(PickedHandle),
    /// `@member_filled`: each member of the program picks their own
    /// (`--member`).
    MemberFilled,
    /// A connection written in the source, the old way, which the
    /// compiler refuses: it has to be erased and picked again.
    WrittenInSource,
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
    let client = ctx.client()?;
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
        // The install's picks pointing at it go with it; the ones of the
        // project here are named, so nobody is surprised by a node that
        // no longer has a connection.
        let cleared = picks_pointing_at(&ctx, *id).await?;
        forget_grant(&client, *id).await?;
        if json {
            println!("{}", serde_json::json!({ "forgot": id, "cleared": cleared }));
        } else {
            println!("forgot connection {id}.");
            if !cleared.is_empty() {
                println!("These nodes no longer have a connection picked: {}.", cleared.join(", "));
            }
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
    let mut others: Vec<OtherNode> = Vec::new();
    let (mut targets, program) = discover(project, &catalog, &mut others)?;
    // The picks are the install's: the project has to be known there
    // before one can be read or kept.
    super::ensure::ensure_project_known(&ctx).await?;
    let project_id = project.id();
    let stored = install_picks(&client, project_id).await?;
    with_install_picks(&mut targets, &stored)?;
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
    // `--list` never prompts: with several access nodes and no
    // `--node`, every node's connections are listed in turn.
    if opts.list && opts.node.is_none() && targets.len() > 1 {
        let mut all: Vec<serde_json::Value> = Vec::new();
        for (i, target) in targets.iter().enumerate() {
            let grants = list_grants(&client, Doorway::Owner, Some(&target.spec.service)).await?;
            if json {
                all.push(serde_json::json!({ "node": target.spelling(), "spellings": target.spellings(), "file": target.rel_file, "connections": grants }));
            } else {
                if i > 0 {
                    println!();
                }
                println!("{}:", describe_target(target));
                print_grants(target, target.spec.display_label(), &grants);
            }
        }
        if json {
            println!("{}", serde_json::json!({ "nodes": all }));
        }
        return Ok(());
    }
    let mut target = choose_target(project, &program, targets, &others, opts.node.as_deref(), json)?;
    // Named by one of its places: the pick shown is that place's.
    with_install_picks(std::slice::from_mut(&mut target), &stored)?;
    let service = target.spec.service.clone();
    let label = target.spec.display_label().to_string();
    if let Some(member) = &opts.member {
        return as_member(&ctx, &client, member, &opts, &target, &label).await;
    }
    // A connection written in the source is the old way, refused by the
    // compiler; the line has to go before anything is picked.
    if matches!(target.picked, Pick::WrittenInSource) {
        bail!("{}", written_in_source(&target));
    }
    // A pick here would sit beside the marker that hands the connection to
    // each member, and never be read: say what the field is instead.
    if matches!(target.picked, Pick::MemberFilled) && !opts.list {
        bail!(
            "'{}' is connected by each member of the program (`{}: @member_filled`); manage one \
             member's with --member <id>, or remove the marker to pick one connection for everyone",
            target.spelling(),
            target.input
        );
    }
    let cx = Connecting {
        client: &client,
        project_id,
        target: &target,
        service_label: &label,
        interactive,
        json,
        doorway: Doorway::Owner,
    };

    if opts.disconnect {
        cx.clear_pick().await?;
        if json {
            println!(
                "{}",
                serde_json::json!({ "node": target.spelling(), "picked": Value::Null })
            );
        }
        return Ok(());
    }

    let grants = list_grants(&client, Doorway::Owner, Some(&service)).await?;

    if opts.list {
        if json {
            // One JSON OBJECT per line, per the global --json contract.
            // It names the node it listed for: a caller that passed
            // `--node` still has to see which one answered, spelled the
            // way the program spells it, and every other name the node
            // has (a file included twice gives it two).
            println!(
                "{}",
                serde_json::json!({
                    "node": target.spelling(),
                    "spellings": target.spellings(),
                    "file": target.rel_file,
                    "connections": grants,
                })
            );
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
        let rearmed = cx.pick_grant(g).await?;
        if json {
            println!(
                "{}",
                serde_json::json!({ "node": target.spelling(), "file": target.rel_file, "picked": g.id, "rearmed": rearmed })
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
        cx.pick_grant(&grant).await?;
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
                cx.clear_pick().await?;
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
                    let cleared = picks_pointing_at(&ctx, g.id).await?;
                    forget_grant(&client, g.id).await?;
                    println!("forgot it.");
                    if !cleared.is_empty() {
                        println!("These nodes no longer have a connection picked: {}.", cleared.join(", "));
                    }
                } else {
                    println!("kept.");
                }
                return Ok(());
            }
            [n] if !grants.is_empty() && n.parse::<usize>().is_ok() => {
                let g = grant_by_number(&grants, n)?;
                cx.pick_grant(g).await?;
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
    cx.pick_grant(&grant).await?;
    Ok(())
}

/// `weft connect --member <id>`: the member's side of one member-filled
/// connection field, through the member door (see
/// `member_values::as_member`).
async fn as_member(
    ctx: &Ctx,
    client: &DispatcherClient,
    member: &weft_core::member::MemberId,
    opts: &ConnectOpts,
    target: &AccessTarget,
    label: &str,
) -> Result<()> {
    let json = ctx.json();
    let project_id = ctx.project()?.id();
    super::member_values::as_member(ctx, client, member, |door| async move {
        member_connect(&door, project_id, member, opts, target, label, json).await
    })
    .await
}

async fn member_connect(
    door: &DispatcherClient,
    project_id: Uuid,
    member: &weft_core::member::MemberId,
    opts: &ConnectOpts,
    target: &AccessTarget,
    label: &str,
    json: bool,
) -> Result<()> {
    let step = target.spelling();
    let cx = Connecting {
        client: door,
        project_id,
        target,
        service_label: label,
        interactive: is_interactive(),
        json,
        doorway: Doorway::Member,
    };
    if !matches!(target.picked, Pick::MemberFilled) {
        bail!(
            "'{step}' takes the author's connection, not each member's; write `{}: @member_filled` on it \
             to have each member connect their own",
            target.input
        );
    }
    if opts.disconnect {
        door.put_json("/member/values", &serde_json::json!({ "clear": [{ "step": step, "field": target.input }] }))
            .await?;
        if json {
            println!("{}", serde_json::json!({ "node": step, "member": member, "picked": Value::Null }));
        } else {
            println!("'{step}' now has no connection picked for member '{member}'.");
        }
        return Ok(());
    }
    let grants = list_grants(door, Doorway::Member, Some(&target.spec.service)).await?;
    if opts.list {
        if json {
            println!("{}", serde_json::json!({ "node": step, "member": member, "connections": grants }));
        } else if grants.is_empty() {
            println!("member '{member}' has no {label} connection yet; connect one with --door own (or shared).");
        } else {
            for g in &grants {
                println!("  {}  {}", g.id, g.identity.clone().unwrap_or_default());
            }
        }
        return Ok(());
    }
    let grant = match &opts.grant {
        Some(id) => grants
            .into_iter()
            .find(|g| g.id == *id)
            .with_context(|| format!("member '{member}' has no {label} connection with id {id} (see --list)"))?,
        None => cx.connect_new(opts).await?,
    };
    let changed = door
        .put_json(
            "/member/values",
            &serde_json::json!({ "set": [{ "step": step, "field": target.input, "value": { "id": grant.id } }] }),
        )
        .await
        .with_context(|| {
            format!(
                "the connection is stored as {}; pick it with `weft connect --member {member} --node {step} --grant {}`",
                grant.id, grant.id
            )
        })?;
    let rearmed = serde_json::from_value::<weft_core::member_door::ValuesChanged>(changed)
        .context("read the dispatcher's answer to the change; upgrade the dispatcher or this CLI so the versions match")?
        .rearmed;
    if json {
        println!("{}", serde_json::json!({ "node": step, "member": member, "picked": grant.id, "rearmed": rearmed }));
    } else {
        println!(
            "'{step}' now uses {} for member '{member}'.",
            grant.identity.clone().unwrap_or_else(|| grant.id.to_string())
        );
        if !rearmed.is_empty() {
            println!("Their trigger(s) reading it were set up again: {}.", rearmed.join(", "));
        }
    }
    Ok(())
}

/// The ambient state of one `weft connect` invocation, threaded once
/// instead of five parameters through every flow.
struct Connecting<'a> {
    client: &'a DispatcherClient,
    /// The project the pick is kept for.
    project_id: Uuid,
    target: &'a AccessTarget,
    /// The SERVICE's display label ("Slack"), never a connection's name.
    service_label: &'a str,
    interactive: bool,
    /// Under --json prose stays off stdout (one JSON object per line).
    json: bool,
    /// Whose connection this is: the author's (picked on the install)
    /// or one member's (`--member`, picked at the member door).
    doorway: Doorway,
}

/// Whose connections `weft connect` manages. The author's go through
/// the store's own routes and the pick is kept by the install; a
/// member's go through the member door, exactly as the member's connect
/// page would, with a member token the command mints for the purpose,
/// and the pick is the member's own, stored beside their connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Doorway {
    Owner,
    Member,
}

/// The connect routes both doorways offer, one name each.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Route {
    Grants,
    Doors,
    Direct,
    Begin,
    Status,
}

impl Doorway {
    pub(crate) fn route(self, route: Route) -> &'static str {
        match (self, route) {
            (Doorway::Owner, Route::Grants) => "/access/grants",
            (Doorway::Owner, Route::Doors) => "/access/doors",
            (Doorway::Owner, Route::Direct) => "/access/connect/direct",
            (Doorway::Owner, Route::Begin) => "/access/connect/begin",
            (Doorway::Owner, Route::Status) => "/access/connect/status",
            (Doorway::Member, Route::Grants) => "/member/connections",
            (Doorway::Member, Route::Doors) => "/member/doors",
            (Doorway::Member, Route::Direct) => "/member/connections/direct",
            (Doorway::Member, Route::Begin) => "/member/connections/begin",
            (Doorway::Member, Route::Status) => "/member/connections/status",
        }
    }
}

/// Delete a stored connection for good. The ONE path to the delete
/// verb, shared with the menu and `weft test-node`'s ephemeral grants.
pub(crate) async fn forget_grant(client: &DispatcherClient, id: Uuid) -> Result<()> {
    client.delete(&format!("/access/grants/{id}")).await
}

/// The places of the project here whose pick on this install is the
/// connection `id`, spelled: what a forget of it takes away with it.
/// Outside a project, nobody's. A project here that cannot be read, or
/// an install whose picks cannot be read, is an error: the forget would
/// otherwise report that nothing lost its connection without knowing.
async fn picks_pointing_at(ctx: &Ctx, id: Uuid) -> Result<Vec<String>> {
    let unknowable = "cannot tell which nodes use this connection, so nothing was forgotten";
    let Some(project) = ctx.project_here().context(unknowable)? else { return Ok(Vec::new()) };
    let stored = install_picks(&ctx.client()?,project.id()).await.context(unknowable)?;
    let mut pointing = Vec::new();
    for (step, fields) in stored {
        for (field, handle) in &fields {
            if picked_handle(&step, field, handle)?.id == id {
                pointing.push(step.clone());
                break;
            }
        }
    }
    Ok(pointing)
}

/// The connection an install's pick at `step.field` names. One the CLI
/// cannot read is an error naming the place, never "not connected".
fn picked_handle(step: &str, field: &str, handle: &Value) -> Result<PickedHandle> {
    parse_picked(handle).with_context(|| {
        format!(
            "the install's pick for '{step}.{field}' is not a connection this CLI can read ({handle}); \
             upgrade the CLI or the install so the versions match, or connect '{step}' again"
        )
    })
}

/// Every pick this install keeps for `project_id`, by place and field.
/// A project the install has never heard of has none.
pub(crate) async fn install_picks(client: &DispatcherClient, project_id: Uuid) -> Result<weft_core::picks::Picks> {
    // SYNC: GET /projects/{id}/picks <-> crates/weft-dispatcher/src/api/picks.rs list
    let answer = client.get_json(&format!("/projects/{project_id}/picks")).await?;
    serde_json::from_value(answer).context("read the install's picks")
}

/// Set each target's pick from `stored`, at the place it is named by. A
/// field the source marks for each member, or holds a connection
/// written in the old way, keeps what the source says.
fn with_install_picks(targets: &mut [AccessTarget], stored: &weft_core::picks::Picks) -> Result<()> {
    for target in targets {
        if matches!(target.picked, Pick::MemberFilled | Pick::WrittenInSource) {
            continue;
        }
        let step = target.spelling();
        target.picked = match stored.get(&step).and_then(|fields| fields.get(&target.input)) {
            Some(handle) => Pick::Handle(picked_handle(&step, &target.input, handle)?),
            None => Pick::None,
        };
    }
    Ok(())
}

/// What the source says about a node's connection, off the value its
/// parse holds there: nothing (the install keeps the pick, read after),
/// each member's, or one written the old way. The parse is enriched, so
/// "nothing written" arrives as the compiler's install-picked marker; a
/// member's fallback connection is written in the source like any other.
fn source_pick(written: Option<&Value>) -> Pick {
    match written {
        None | Some(Value::Null) => Pick::None,
        Some(v) if weft_core::picks::is_install_picked(v) => Pick::None,
        Some(v) => match weft_core::member::as_member_filled(v) {
            Some(filled) if filled.fallback.is_none() => Pick::MemberFilled,
            _ => Pick::WrittenInSource,
        },
    }
}

/// What to do about a connection written in the source, the old way:
/// the same words the compiler refuses it with.
pub(crate) fn written_in_source(target: &AccessTarget) -> String {
    format!(
        "'{node}.{field}' holds a connection written in the source ({file}). That was the old way of \
         connecting a node: a connection lives in one install, so its id means nothing on any other. \
         Erase that line, then run `weft connect --node {node}` again; the pick is kept by the \
         install. See {doc}",
        node = target.spelling(),
        field = target.input,
        file = target.rel_file,
        doc = weft_core::picks::PICKS_DOC,
    )
}

// ── Target discovery ────────────────────────────────────────────────────────

/// A node that is NOT connectable, kept only so a refusal can tell
/// "you typed a name that is not here" apart from "that node needs no
/// connection".
struct OtherNode {
    node: String,
    node_type: String,
}

/// Walk main.weft and every `@include`d file (recursively) and pair
/// each access node with its service recipe and what its source says of
/// its connection (the install's pick is read after, `with_install_picks`),
/// plus the leniently flattened program the names came from. Each FILE is
/// parsed standalone and visited once: a subgraph included from two places
/// is one source file, so its access node is one target with two places.
/// What a person calls that node comes from the WHOLE program, flattened
/// once here: a node has one name per place it is at, and the file walk
/// knows nothing about places (a file reached through a nested include is two sites
/// deep, and the alias it was reached by is only the innermost).
///
/// The flattening is the LENIENT one (lex, parse, includes inlined, no
/// enrich and no validation), because connecting is what you do to a
/// program that is not wired up yet: an unmet `MustOverride` or a
/// mis-typed literal leaves the node list whole, and the places come
/// from the node list alone. A parse error is refused the way the file
/// walk refuses one, naming the file and the line.
fn discover(
    project: &weft_compiler::project::Project,
    catalog: &weft_catalog::FsCatalog,
    others: &mut Vec<OtherNode>,
) -> Result<(Vec<AccessTarget>, weft_core::ProjectDefinition)> {
    let main = project.read_main_weft().map_err(|e| anyhow::anyhow!("read {}: {e}", project.main_weft().display()))?;
    // ONE spelling of the root for both halves. The compiler keys an
    // included file by its path under the root it is given, taken from
    // the file's canonical path, and the file walk below keys the same
    // file by its path under the root it was given: hand each a
    // different spelling of one folder (a symlink in the way) and the
    // two ids disagree, so every node of every included file would be
    // "at no place".
    let root = project
        .root
        .canonicalize()
        .with_context(|| format!("resolve {}", project.root.display()))?;
    let (program, parse_errors) = weft_compiler::flatten_lenient(
        &main,
        project.id(),
        weft_compiler::CompileFs::disk(&root).anchored_at(Some(&root.join(weft_compiler::project::SRC_DIR))),
        catalog,
    );
    if let Some(first) = parse_errors.first() {
        bail!(
            "{} does not parse (line {}: {}); fix it before connecting, so the \
             nodes to connect can be read from it",
            first.file.clone().unwrap_or_else(|| project.main_weft().display().to_string()),
            first.span.start_line,
            first.message
        );
    }
    let mut out = Vec::new();
    let mut visited: std::collections::BTreeSet<std::path::PathBuf> = Default::default();
    collect_targets(&project.main_weft(), &root, &program, catalog, &mut out, others, &mut visited)?;
    Ok((out, program))
}

/// One file's pass: parse it standalone (the same per-file view the
/// editor edits), collect its access nodes, recurse into its includes.
/// A file reached again is one file already collected, so nothing is
/// added: its every name came from `program` the first time.
fn collect_targets(
    file: &std::path::Path,
    root: &std::path::Path,
    program: &weft_core::ProjectDefinition,
    catalog: &weft_catalog::FsCatalog,
    out: &mut Vec<AccessTarget>,
    others: &mut Vec<OtherNode>,
    visited: &mut std::collections::BTreeSet<std::path::PathBuf>,
) -> Result<()> {
    let canonical = file
        .canonicalize()
        .with_context(|| format!("resolve {}", file.display()))?;
    if !visited.insert(canonical.clone()) {
        return Ok(());
    }
    let source = std::fs::read_to_string(&canonical)
        .with_context(|| format!("read {}", canonical.display()))?;
    // The ids this parse hands out have to be the ones `program` keys
    // its places by, or `spellings_of` finds nothing. An included file's
    // nodes are keyed under the file's body id on both sides; the entry
    // file's are bare on the program side (the build compiles it with
    // no source id), so the entry is parsed bare here too. A main.weft
    // wrapped in one top-level group would otherwise come out as
    // `@src:main.x` against the program's `Untitled.x`.
    let is_entry = visited.len() == 1;
    let source_id = (!is_entry).then(|| weft_compiler::source_name::body_id(root, &canonical));
    let base = canonical
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_default();
    let (definition, diagnostics) = weft_compiler::parse_only(
        &source,
        program.id,
        weft_compiler::CompileFs::disk(root).anchored_at(Some(&base)),
        catalog,
        source_id.as_deref(),
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
            "{} does not parse (line {}: {}); fix it before connecting, so the \
             nodes to connect can be read from it",
            canonical.display(),
            first.line,
            first.message
        );
    }
    let rel_file = canonical
        .strip_prefix(root)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| canonical.to_string_lossy().into_owned());
    let mut includes: Vec<String> = Vec::new();
    for node in &definition.nodes {
        if let Some(path) = &node.include_path {
            includes.push(path.clone());
            continue;
        }
        let spellings = spellings_of(program, &node.id)?;
        let mut note_other = || {
            others.push(OtherNode { node: node.id.clone(), node_type: node.node_type.clone() })
        };
        let Some(meta) = weft_core::node::MetadataCatalog::lookup(catalog, &node.node_type) else {
            note_other();
            continue;
        };
        let Some(spec) = meta.service.clone() else {
            note_other();
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
        // Through `written_value`, never `config`: a constant written for
        // an INPUT PORT has two homes in source and enrich moves it from
        // one to the other (`normalize_port_literals`).
        let picked = source_pick(node.written_value(&input));
        out.push(AccessTarget {
            rel_file: rel_file.clone(),
            node: node.id.clone(),
            node_type: node.node_type.clone(),
            input,
            project_app: meta.access_apps.get(&spec.service).cloned(),
            spec,
            picked,
            spellings,
        });
    }
    for path in includes {
        collect_targets(&base.join(&path), root, program, catalog, out, others, visited)?;
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

/// The node `id` the way the PROGRAM spells it, which is the only form
/// a person is shown or asked to type. A node in the entry file is its
/// own name. One inside an included file is named through the site that
/// includes the file (`sweep.key`), once per site it is reached
/// through, because the id it is keyed by carries the file's PATH and
/// the language calls that spelling unspellable on purpose.
fn spellings_of(program: &weft_core::ProjectDefinition, id: &str) -> Result<Vec<String>> {
    let spellings: Vec<String> = weft_core::project::selection::every_place(program)
        .into_iter()
        .filter(|place| place.id == id)
        .map(|place| weft_core::project::address_of(program, &place.id, &place.path))
        .collect();
    // Every node the file walk finds is a node of the program, at one
    // place at least; a node with none is a file the walk reached that
    // the program does not, and a name made up here would be one no
    // other command takes.
    anyhow::ensure!(!spellings.is_empty(), "'{}' is at no place in this program", weft_core::project::plain_id(id));
    Ok(spellings)
}

impl AccessTarget {
    /// Every way a person may name this node (see [`spellings_of`]).
    pub(crate) fn spellings(&self) -> &[String] {
        &self.spellings
    }

    /// The connection field.
    pub(crate) fn input(&self) -> &str {
        &self.input
    }

    /// The one spelling to print when only one fits: the first, which
    /// [`Self::answer_as`] makes the one the person asked by.
    pub(crate) fn spelling(&self) -> String {
        self.spellings[0].clone()
    }

    /// Put the spelling the person named this node by first, so every
    /// line printed about it answers in their words: asked for
    /// `two.key`, a node a file included twice is `two.key` in the
    /// answer, never the other call's name.
    fn answer_as(&mut self, asked: &str) {
        if let Some(at) = self.spellings.iter().position(|s| s == asked) {
            self.spellings.swap(0, at);
        }
    }
}

/// A target's one-line description: its names, its type, and (for an
/// included file's node) the file it lives in. The names already say
/// which sites reach it (`nightly.key or manual.key`).
fn describe_target(t: &AccessTarget) -> String {
    let mut s = format!("{} ({})", t.spellings().join(" or "), t.node_type);
    // The compiler keys a node inside an included file by the file's
    // path, and that key starts with `@` (`@src:sweep.key`): the one
    // mark that tells a node not of the entry file apart here.
    if t.node.starts_with('@') {
        s.push_str(&format!(" in {}", t.rel_file));
    }
    s
}

/// Any node of the project, found by the name a person types, the same
/// way `weft connect --node` finds an access node.
pub(crate) struct Step {
    /// The compiled id, the key into `program`.
    pub(crate) node: String,
    /// The name the person asked by (the file qualifier dropped).
    pub(crate) spelling: String,
    pub(crate) node_type: String,
    /// Every access node of the project, to read a traced pick off.
    pub(crate) targets: Vec<AccessTarget>,
    pub(crate) program: weft_core::ProjectDefinition,
}

pub(crate) fn resolve_step(
    project: &weft_compiler::project::Project,
    catalog: &weft_catalog::FsCatalog,
    name: &str,
) -> Result<Step> {
    let (targets, program) = discover(project, catalog, &mut Vec::new())?;
    let (node, _) = super::resolve_node(project, &program, name)?;
    let node_type = program
        .nodes
        .iter()
        .find(|n| n.id == node)
        .map(|n| n.node_type.clone())
        .context("the resolved step is missing from the program")?;
    Ok(Step { node, spelling: super::unqualified(name).to_string(), node_type, targets, program })
}

fn choose_target(
    project: &weft_compiler::project::Project,
    program: &weft_core::ProjectDefinition,
    mut targets: Vec<AccessTarget>,
    others: &[OtherNode],
    node: Option<&str>,
    json: bool,
) -> Result<AccessTarget> {
    if let Some(name) = node {
        let known = targets
            .iter()
            .map(describe_target)
            .collect::<Vec<_>>()
            .join(", ");
        let (id, _) = super::resolve_node(project, program, name)
            .map_err(|e| anyhow::anyhow!("{e:#}; the access nodes are: {known}"))?;
        // A node that IS in the program but takes no connection: say
        // that, rather than listing the connectable ones, which reads
        // as "you typed it wrong" when the real answer is "that one
        // needs nothing from you".
        if let Some(other) = others.iter().find(|o| o.node == id) {
            bail!(
                "'{name}' is a {} and takes no connection: nothing about it is yours to \
                 pick, so there is nothing to connect here. `weft connect --list` shows the \
                 nodes of this project that do take one",
                other.node_type
            );
        }
        let mut found = targets.into_iter().find(|t| t.node == id).with_context(|| {
            format!("'{name}' takes no connection (the access nodes are: {known})")
        })?;
        found.answer_as(super::unqualified(name));
        return Ok(found);
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
            Pick::MemberFilled => "each member connects their own (see --member)".to_string(),
            Pick::WrittenInSource => "written in the source the old way: erase that line and connect again".to_string(),
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
    doorway: Doorway,
    service: Option<&str>,
) -> Result<Vec<GrantSummary>> {
    let path = match service {
        Some(s) => format!("{}?service={}", doorway.route(Route::Grants), percent_encode(s)),
        // No service: every stored connection, every service (the
        // store models the filter as optional the same way). The
        // store-wide listing `--list` falls back to when no project
        // scopes it, and what the recovery hints other commands print
        // (`weft connect --list`) rely on, so it works from anywhere.
        None => doorway.route(Route::Grants).to_string(),
    };
    let rows = client.get_json(&path).await?;
    serde_json::from_value(rows).context("parse the connection list")
}

/// Render the store-wide listing (`--list` with no project to scope
/// it): one line per connection with its service, since no node
/// context exists to mark a pick against.
async fn print_all_grants(client: &DispatcherClient, json: bool) -> Result<()> {
    let grants = list_grants(client, Doorway::Owner, None).await?;
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
        println!("  [{}] {} - {}  (id {}){}", i + 1, g.service, who, g.id, no_key_note(g));
    }
    Ok(())
}

/// The trailing note on a runtime-owned row whose key is gone: the
/// row is not a working connection until the key is back.
fn no_key_note(g: &GrantSummary) -> String {
    if g.has_credential {
        return String::new();
    }
    format!(
        "  NO KEY BEHIND IT: add an api_key entry for '{}' to the shared-credentials file, or connect your own",
        g.service
    )
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
        Pick::Handle(p) => println!("'{}' is connected as {}.", target.spelling(), p.who()),
        Pick::None => println!("'{}' has no connection picked.", target.spelling()),
        Pick::MemberFilled => println!(
            "'{}' is connected by each member of the program; see theirs with --member <id>.",
            target.spelling()
        ),
        Pick::WrittenInSource => println!("{}", written_in_source(target)),
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
        let can = if g.owner.is_platform() && !g.has_credential {
            "runs on the runtime's own key, which is NOT configured".to_string()
        } else if g.owner.is_platform() {
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
        println!("  [{}] {who} - {can}  (id {}){picked}{}", i + 1, g.id, no_key_note(g));
    }
}

impl Connecting<'_> {
    /// Point the node, at the place it is named by, at this stored
    /// connection on this install, and say so. Answers the triggers the
    /// change set up again. When the write fails the connection still
    /// exists, so the error names the way to attach it without redoing
    /// the sign-in.
    async fn pick_grant(&self, g: &GrantSummary) -> Result<Vec<String>> {
        let body = ChangePicks {
            set: vec![PickInput {
                step: self.target.spelling(),
                field: self.target.input.clone(),
                connection: g.id,
                service: self.target.spec.service.clone(),
            }],
            clear: Vec::new(),
        };
        let rearmed = self.write_picks(&body).await.with_context(|| {
            format!(
                "the connection is stored as {}; attach it with `weft connect --node {} \
                 --grant {}`",
                g.id, self.target.spelling(), g.id
            )
        })?;
        if !self.json {
            println!(
                "'{}' now uses {} on this install.",
                self.target.spelling(),
                g.identity.clone().unwrap_or_else(|| g.id.to_string())
            );
            if !rearmed.is_empty() {
                println!("The trigger(s) reading it were set up again: {}.", rearmed.join(", "));
            }
        }
        Ok(rearmed)
    }

    /// Forget the node's pick on this install (the stored connection
    /// stays) and say so.
    async fn clear_pick(&self) -> Result<()> {
        let body = ChangePicks {
            set: Vec::new(),
            clear: vec![MemberFieldRef { step: self.target.spelling(), field: self.target.input.clone() }],
        };
        self.write_picks(&body).await?;
        if !self.json {
            println!("'{}' now has no connection picked on this install.", self.target.spelling());
        }
        Ok(())
    }

    /// Change the install's picks for the project.
    async fn write_picks(&self, body: &ChangePicks) -> Result<Vec<String>> {
        let answer = self
            .client
            .put_json(&format!("/projects/{}/picks", self.project_id), &serde_json::to_value(body)?)
            .await?;
        Ok(serde_json::from_value::<weft_core::member_door::ValuesChanged>(answer)
            .context("read the install's answer to the change; upgrade the dispatcher or this CLI so the versions match")?
            .rearmed)
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
                    self.doorway.route(Route::Doors),
                    &serde_json::to_value(DoorsRequest { spec: spec.clone() })?,
                )
                .await?,
        )
        .context("parse the doors probe")?;
        let is_consent = spec.needs_browser_consent();
        // A member always connects an account of their own: the shared
        // key is the author's and spends the author's credits.
        let shared_backed = self.doorway == Doorway::Owner
            && (!doors.doors.shared_apps.is_empty() || doors.doors.shared_credential)
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
                self.doorway,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Shared,
                    values: BTreeMap::new(),
                    label: None,
                    permissions: Vec::new(),
                    registration: None,
                    paste: false,
                    project_id: None,
                    member: None,
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
                self.doorway,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Own,
                    values,
                    label: connection_name(opts, label, interactive)?,
                    permissions: ticked,
                    registration: None,
                    paste: true,
                    project_id: None,
                    member: None,
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
                self.doorway,
                ConnectDirect {
                    spec: spec.clone(),
                    door: Door::Own,
                    values: BTreeMap::new(),
                    label: chosen_name,
                    permissions: ticked,
                    registration,
                    paste: false,
                    project_id: None,
                    member: None,
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
            self.doorway,
            ConnectDirect {
                spec: spec.clone(),
                door: Door::Own,
                values,
                label: connection_name(opts, label, interactive)?,
                permissions: ticked,
                registration: None,
                paste: false,
                project_id: None,
                member: None,
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
    doorway: Doorway,
    req: ConnectDirect,
    shared_app: Option<String>,
) -> Result<GrantSummary> {
    let body = serde_json::to_value(&SharedDoorPick {
        shared_app,
        inner: req,
    })?;
    let done = client.post_json(doorway.route(Route::Direct), &body).await?;
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
            member: None,
            upgrade_grant_id,
            // Filled by the dispatcher (it knows its public host).
            redirect_uri: String::new(),
        };
        let body = serde_json::to_value(&SharedDoorPick {
            shared_app,
            inner: req,
        })?;
        let started: StartedOAuth =
            serde_json::from_value(client.post_json(self.doorway.route(Route::Begin), &body).await?)
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
                    "{}?state={}",
                    self.doorway.route(Route::Status),
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
            member: None,
            identity: None,
            label: None,
            scopes: vec![],
            permissions_verified: false,
            value_names: vec![],
            owner: weft_core::CredentialOwner::Author,
            door: Door::Own,
            expires_at: None,
            has_credential: true,
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

    /// What `weft connect` reads off a real project, parsed the way it
    /// parses one. The parse is enriched, so a node with nothing written
    /// carries the compiler's install marker, which is no connection
    /// written in the source: reading it as one refused every
    /// `weft connect --grant` on every access node.
    #[test]
    fn the_source_says_what_the_person_wrote_about_a_connection() {
        let dir = tempfile::tempdir().unwrap();
        let project = weft_compiler::project::Project::init(dir.path(), "picks").unwrap();
        let catalog = weft_compiler::build::build_project_catalog(&project.root).unwrap();
        let read = |source: &str| {
            std::fs::write(project.main_weft(), source).unwrap();
            let (targets, _) = discover(&project, &catalog, &mut Vec::new()).unwrap();
            targets.into_iter().next().expect("the access node is a target").picked
        };
        assert!(matches!(read("ws = SlackAccess\n"), Pick::None));
        assert!(matches!(read("ws = SlackAccess { account: @member_filled }\n"), Pick::MemberFilled));
        assert!(matches!(read("ws = SlackAccess { account: {\"id\": \"g-1\"} }\n"), Pick::WrittenInSource));
        assert!(matches!(
            read("ws = SlackAccess { account: @member_filled({\"id\": \"g-1\"}) }\n"),
            Pick::WrittenInSource
        ));
    }

    /// A pick the install keeps that this CLI cannot read is an error
    /// naming its place, never shown as "not connected".
    #[test]
    fn an_unreadable_install_pick_names_its_place() {
        let dir = tempfile::tempdir().unwrap();
        let project = weft_compiler::project::Project::init(dir.path(), "picks").unwrap();
        let catalog = weft_compiler::build::build_project_catalog(&project.root).unwrap();
        std::fs::write(project.main_weft(), "ws = SlackAccess\n").unwrap();
        let (mut targets, _) = discover(&project, &catalog, &mut Vec::new()).unwrap();
        let field = targets[0].input.clone();
        let stored = |handle: Value| -> weft_core::picks::Picks {
            serde_json::from_value(serde_json::json!({ "ws": { field.clone(): handle } })).unwrap()
        };
        let id = Uuid::new_v4();
        with_install_picks(&mut targets, &stored(serde_json::json!({ "id": id.to_string() }))).unwrap();
        assert!(matches!(&targets[0].picked, Pick::Handle(h) if h.id == id));
        let error = with_install_picks(&mut targets, &stored(serde_json::json!({ "id": "not-a-uuid" }))).unwrap_err();
        assert!(error.to_string().contains(&format!("'ws.{field}'")), "{error:#}");
    }

    /// The shape an include compiles to: the file `src/sweep.weft` is
    /// one body, keyed by its path, and each `@include` of it is a call
    /// site; a node inside it is keyed under the file's path.
    fn program_including_sweep(sites: &[&str]) -> weft_core::ProjectDefinition {
        let node = |id: &str, scope: &[&str], boundary: serde_json::Value| {
            serde_json::json!({
                "id": id, "nodeType": "T", "config": {}, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [], "outputs": [], "scope": scope, "groupBoundary": boundary,
            })
        };
        let mut nodes = vec![
            node("db", &[], serde_json::Value::Null),
            node("@src:sweep__in", &[], serde_json::json!({ "groupId": "@src:sweep", "role": "In" })),
            node("@src:sweep.key", &["@src:sweep"], serde_json::Value::Null),
            node("@src:sweep.inner.key", &["@src:sweep", "@src:sweep.inner"], serde_json::Value::Null),
            node("@src:sweep__out", &[], serde_json::json!({ "groupId": "@src:sweep", "role": "Out" })),
        ];
        let mut groups = vec![
            serde_json::json!({ "id": "@src:sweep", "kind": "body", "nodeIds": ["@src:sweep.key"] }),
            serde_json::json!({ "id": "@src:sweep.inner", "kind": "group", "nodeIds": ["@src:sweep.inner.key"] }),
        ];
        for site in sites {
            nodes.push(node(&format!("{site}__in"), &[], serde_json::json!({ "groupId": site, "role": "In" })));
            nodes.push(node(&format!("{site}__out"), &[], serde_json::json!({ "groupId": site, "role": "Out" })));
            groups.push(serde_json::json!({ "id": site, "kind": "call", "body": "@src:sweep", "nodeIds": [] }));
        }
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": nodes, "edges": [], "groups": groups,
        }))
        .expect("the fixture deserializes")
    }

    /// A node inside an included file is keyed by the file's PATH, and
    /// the language calls that spelling unspellable: a person names it
    /// through the site that includes the file. `weft connect` used to
    /// print the key and take nothing else, so the only thing that
    /// worked was `@src:sweep.key`, which no other command accepts.
    #[test]
    fn an_included_files_node_is_named_through_its_site() {
        let p = program_including_sweep(&["sweep"]);
        let spelled = spellings_of(&p, "@src:sweep.key").unwrap();
        assert_eq!(spelled, vec!["sweep.key".to_string()]);
        assert!(!spelled[0].contains('@'), "the compiler's id never reaches a person");
    }

    /// One file included twice is one node at two places, with a name
    /// per place, and both have to work.
    #[test]
    fn a_file_included_twice_answers_to_either_site() {
        let p = program_including_sweep(&["nightly", "manual"]);
        assert_eq!(
            spellings_of(&p, "@src:sweep.key").unwrap(),
            vec!["manual.key".to_string(), "nightly.key".to_string()]
        );
    }

    /// A node nested in a group inside that file keeps the whole tail.
    #[test]
    fn a_nested_node_keeps_the_rest_of_its_address() {
        let p = program_including_sweep(&["sweep"]);
        assert_eq!(spellings_of(&p, "@src:sweep.inner.key").unwrap(), vec!["sweep.inner.key".to_string()]);
    }

    /// A node of the entry file is already written the way it is read.
    #[test]
    fn a_node_of_the_entry_file_is_its_own_name() {
        let p = program_including_sweep(&["sweep"]);
        assert_eq!(spellings_of(&p, "db").unwrap(), vec!["db".to_string()]);
    }

    /// A node at no place in the program is refused, never given a
    /// name no other command would take.
    #[test]
    fn a_node_the_program_does_not_place_is_refused() {
        let p = program_including_sweep(&[]);
        let err = spellings_of(&p, "@src:sweep.key").unwrap_err().to_string();
        assert!(err.contains("'sweep.key' is at no place"), "{err}");
    }
}
