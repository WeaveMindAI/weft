//! A project's settings on the install: its frontends, its worker levers,
//! the connections its own access nodes use (its picks), and the owner's
//! signal tokens (`weft_core::program::settings::SettingsCall`). The verbs
//! of `weft frontend`, `weft workers`, `weft connect`'s picks and
//! `weft token`.
//!
//! What only a person may ask for here is what answers with a credential's
//! value (a new signal token, a frontend's new token): a program call's
//! answer is stored with its task and journaled, and a value kept there is
//! a credential anybody reading the run could take. A program mints an
//! instance token with `ctx.tokens().mint_for_instance`, which goes
//! through the broker and hands the value to the run alone.

use axum::http::StatusCode;
use weft_core::frontend::{AddFrontendRequest, AddedFrontend, Frontend, FrontendHost, FrontendWithToken, VisitorAddressRequest};
use weft_core::picks::{ChangePicks, MovePicks, Picks, PicksMoved};
use weft_core::program::settings::SettingsCall;
use weft_core::program::{ProgramCallOutcome, TokensRevoked};
use weft_core::signal_token::{MintTokenRequest, MintedToken, TokenKind, TokenSummary};
use weft_core::workers::{WorkerOverrides, WorkersResponse};

use super::{answered, failed, operator_only, Actor, Refused};
use crate::state::DispatcherState;
use crate::tenant::TenantId;

const SECRET_IN_ANSWER: &str = "its answer carries a token's value, and a program call's answer is stored with its task and \
     journaled; a program mints an instance token with ctx.tokens().mint_for_instance, which hands the value to the run alone";

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.frontends.list", super::Reach::Everyone),
    ("weft.frontends.add", super::Reach::Everyone),
    ("weft.frontends.remove", super::Reach::Everyone),
    ("weft.frontends.visitor_address", super::Reach::Everyone),
    ("weft.frontends.token_new", super::Reach::Operator(SECRET_IN_ANSWER)),
    ("weft.frontends.token_done", super::Reach::Everyone),
    ("weft.frontends.token_drop", super::Reach::Everyone),
    ("weft.workers.get", super::Reach::Everyone),
    ("weft.workers.put", super::Reach::Everyone),
    ("weft.picks.list", super::Reach::Everyone),
    ("weft.picks.change", super::Reach::Everyone),
    ("weft.picks.move", super::Reach::Everyone),
    (
        "weft.picks.check",
        super::Reach::Operator(
            "it checks a program the caller compiled itself, before the install built it, which a program has no way \
             to make; activating a build refuses the same gaps",
        ),
    ),
    ("weft.signal_tokens.list", super::Reach::Everyone),
    ("weft.signal_tokens.mint", super::Reach::Operator(SECRET_IN_ANSWER)),
    ("weft.signal_tokens.revoke", super::Reach::Everyone),
];

/// Carry out one settings call of `actor` on project `project_id`.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    call: &SettingsCall,
) -> Result<ProgramCallOutcome, Refused> {
    match call {
        SettingsCall::FrontendsList => answered(&frontends_list(state, actor, project_id).await?),
        SettingsCall::FrontendAdd { request } => answered(&frontend_add(state, actor, project_id, request.clone()).await?),
        SettingsCall::FrontendRemove { name, force } => answered(&frontend_remove(state, actor, project_id, name, *force).await?),
        SettingsCall::FrontendVisitorAddress { name, request } => {
            answered(&frontend_visitor_address(state, actor, project_id, name, request).await?)
        }
        SettingsCall::FrontendTokenDone { name, token } => answered(&frontend_token_done(state, actor, project_id, name, *token).await?),
        SettingsCall::FrontendTokenDrop { name, token } => answered(&frontend_token_drop(state, actor, project_id, name, *token).await?),
        SettingsCall::WorkersGet => answered(&workers_get(state, actor, project_id).await?),
        SettingsCall::WorkersPut { levers } => answered(&workers_put(state, actor, project_id, levers).await?),
        SettingsCall::PicksList => answered(&picks_list(state, actor, project_id).await?),
        SettingsCall::PicksChange { request } => answered(&picks_change(state, actor, project_id, request).await?),
        SettingsCall::PicksMove { request } => answered(&picks_move(state, actor, project_id, request).await?),
        SettingsCall::SignalTokensList => answered(&signal_tokens_list(state, actor).await?),
        SettingsCall::SignalTokenRevoke { id } => {
            let revoked = signal_token_revoke(state, actor, *id).await?;
            answered(&TokensRevoked { revoked: u64::from(revoked) })
        }
    }
}

// ---- frontends --------------------------------------------------------

/// Every frontend of `project`, by name.
pub async fn frontends_list(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<Vec<Frontend>, Refused> {
    actor.may_act_on(state, project).await?;
    crate::frontends::list(&state.pg_pool, project).await.map_err(failed("frontends"))
}

/// Make the frontend `request` of `project` (`crate::frontends::make`).
pub async fn frontend_add(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    request: AddFrontendRequest,
) -> Result<AddedFrontend, Refused> {
    actor.may_act_on(state, project).await?;
    request.validate().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    program_may_add(actor, &request)?;
    crate::frontends::make(state, actor.tenant(), project, request).await
}

/// A program makes only a frontend the install hosts: one that runs
/// elsewhere gets its token in the answer, which a program's answers keep.
fn program_may_add(actor: &Actor, request: &AddFrontendRequest) -> Result<(), Refused> {
    match (actor, request.host) {
        (Actor::Program { .. }, FrontendHost::External) => Err((
            StatusCode::FORBIDDEN,
            format!(
                "a program cannot make frontend '{}', which runs elsewhere: its token would be in the answer, and a \
                 program call's answer is stored with its task and journaled. Make it with `weft frontend add {}`, or \
                 have the install host it (a repository that deploys it)",
                request.name, request.name
            ),
        )),
        _ => Ok(()),
    }
}

/// Remove the frontend `name` of `project` (`crate::frontends::remove_one`).
pub async fn frontend_remove(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    name: &str,
    force: bool,
) -> Result<Vec<String>, Refused> {
    actor.may_act_on(state, project).await?;
    crate::frontends::remove_one(state, actor.tenant(), project, name, force).await
}

/// Set or clear the visitor address of the frontend `name` of `project`.
pub async fn frontend_visitor_address(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    name: &str,
    request: &VisitorAddressRequest,
) -> Result<Frontend, Refused> {
    actor.may_act_on(state, project).await?;
    if let Some(address) = &request.visitor_address {
        weft_core::frontend::check_visitor_address(address).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    }
    crate::frontends::set_visitor(state, project, name, request).await
}

/// A new token for the frontend `name` of `project`, shown this once
/// (`crate::frontends::renew_token`).
pub async fn frontend_token_new(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    name: &str,
) -> Result<FrontendWithToken, Refused> {
    operator_only(actor, "weft.frontends.token_new")?;
    actor.may_act_on(state, project).await?;
    crate::frontends::renew_token(state, actor.tenant(), project, name).await
}

/// The new token `token` of the frontend `name` is in place
/// (`crate::frontends::put_token_in_place`).
pub async fn frontend_token_done(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    name: &str,
    token: uuid::Uuid,
) -> Result<(), Refused> {
    actor.may_act_on(state, project).await?;
    crate::frontends::put_token_in_place(state, actor.tenant(), project, name, token).await
}

/// The new token `token` of the frontend `name` will never be put in place
/// (`crate::frontends::drop_new_token`).
pub async fn frontend_token_drop(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    name: &str,
    token: uuid::Uuid,
) -> Result<(), Refused> {
    actor.may_act_on(state, project).await?;
    crate::frontends::drop_new_token(state, actor.tenant(), project, name, token).await
}

// ---- workers ----------------------------------------------------------

/// `project`'s worker levers, the install's, and what its workers run
/// with.
pub async fn workers_get(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<WorkersResponse, Refused> {
    actor.may_act_on(state, project).await?;
    workers_answer(state, project).await
}

async fn workers_answer(state: &DispatcherState, project: uuid::Uuid) -> Result<WorkersResponse, Refused> {
    let levers = state
        .projects
        .worker_overrides(project)
        .await
        .map_err(failed("worker levers"))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?;
    Ok(WorkersResponse { install: state.worker_defaults.clone(), effective: state.worker_defaults.with(&levers), project: levers })
}

/// Replace `project`'s worker levers with `levers` (an unset lever follows
/// the install again). Refused, naming the lever, when the result is a
/// shape no platform could run. A change applies at once: the platform is
/// told to run the project's current image with the new levers (a new
/// Cloud Run revision; a warm local worker started or left to go idle).
pub async fn workers_put(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    levers: &WorkerOverrides,
) -> Result<WorkersResponse, Refused> {
    actor.may_act_on(state, project).await?;
    // A change starts the project's workers (and its front) again, which
    // maintenance holds everything back from.
    crate::maintenance::refuse_while_on(&state.pg_pool).await?;
    state.worker_defaults.with(levers).validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.projects.set_worker_overrides(project, levers).await.map_err(failed("save the worker levers"))?;
    if let Some(program) = state.projects.running_program_identity(project).await.map_err(failed("the running program"))? {
        // Read from the rows just written: this process's held copy of the
        // levers hears the change a moment after it commits.
        let target = crate::delivery::current_worker_target(state, actor.tenant().as_str(), project, &program.binary_hash)
            .await
            .map_err(failed("the worker target"))?;
        state.runner.prepare(&target).await.map_err(|e| {
            (StatusCode::BAD_GATEWAY, format!("the levers are saved, and the platform refused them for the running workers: {e:#}"))
        })?;
        // A project that takes calls has its front started again with the
        // new levers (a container's or a revision's are fixed at its start),
        // which settles the front's program on them. The running program is
        // settled here too: it is another one when the triggers that are on
        // still fire an older build.
        if crate::front::address(state, project).await.map_err(failed("the project's front"))?.is_some() {
            let unserved = |e: &anyhow::Error| {
                (StatusCode::BAD_GATEWAY, format!("the levers are saved, and the project's front could not start with them: {e:#}"))
            };
            let served = crate::front::serve(state, project, None).await.map_err(|e| unserved(&e))?;
            if let Some(e) = &served.failed {
                return Err(unserved(e));
            }
        }
        crate::front::settle(state, actor.tenant().as_str(), project, &program.binary_hash).await;
        // A queued event a failing hand-over stopped may run on the new
        // levers.
        crate::parked_drain::retry_stopped(state, project).await;
    }
    workers_answer(state, project).await
}

// ---- picks ------------------------------------------------------------

/// Every pick this install keeps for `project`, by place and field, each
/// as its `{id, identity}` handle.
pub async fn picks_list(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<Picks, Refused> {
    actor.may_act_on(state, project).await?;
    crate::api::project::stored_picks(state, project).await
}

/// Pick and forget, all or none, re-arming the live triggers that read a
/// changed pick (`crate::install_picks::change`).
pub async fn picks_change(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    request: &ChangePicks,
) -> Result<weft_core::instance_door::ValuesChanged, Refused> {
    actor.may_act_on(state, project).await?;
    let clear: Vec<(String, String)> = request.clear.iter().map(|f| (f.node.clone(), f.field.clone())).collect();
    crate::install_picks::change(state, project, &request.set, &clear).await
}

/// Carry a place's picks and every instance's values to another place,
/// after a node moved in the source (`crate::install_picks::move_picks`).
pub async fn picks_move(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &MovePicks) -> Result<PicksMoved, Refused> {
    actor.may_act_on(state, project).await?;
    crate::install_picks::move_picks(state, project, request).await
}

/// What activating `program` would refuse of this install's picks for
/// `project` (an empty refusal when nothing), asked before a deploy so its
/// activation is not where the gaps are found. `program` is the caller's
/// own compile, since it is asked before the install has built it.
pub async fn picks_kept(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<weft_core::picks::Kept, Refused> {
    operator_only(actor, "weft.picks.kept")?;
    actor.may_act_on(state, project).await?;
    crate::install_picks::kept(state, project).await
}

// ---- signal tokens ----------------------------------------------------

/// Every signal token of the actor's owner, metadata only: no answer ever
/// shows a token's value again.
pub async fn signal_tokens_list(state: &DispatcherState, actor: &Actor) -> Result<Vec<TokenSummary>, Refused> {
    let tokens = state.tokens.list_signal_tokens(actor.tenant().as_str()).await.map_err(failed("tokens"))?;
    Ok(tokens
        .into_iter()
        .map(|t| TokenSummary {
            id: t.id,
            kind: t.kind,
            recognizer: t.recognizer,
            name: t.name,
            created_at_unix: t.created_at,
            allowed_projects: t.allowed_projects,
            allowed_tags: t.allowed_tags,
            allowed_displays: t.allowed_displays,
            all_displays: t.all_displays,
            instance: t.instance,
            expires_at_unix: t.expires_at,
        })
        .collect())
}

/// Revoke the signal token `id` of the actor's owner; `false` when there is
/// none (revoked already, or gone with its project). An operator key is
/// the install's admin, so only a person revokes one.
pub async fn signal_token_revoke(state: &DispatcherState, actor: &Actor, id: uuid::Uuid) -> Result<bool, Refused> {
    if let Actor::Program { .. } = actor {
        let tokens = state.tokens.list_signal_tokens(actor.tenant().as_str()).await.map_err(failed("tokens"))?;
        program_may_revoke(tokens.iter().find(|t| t.id == id).map(|t| t.kind), id)?;
    }
    state.tokens.revoke_signal_token(id, actor.tenant().as_str()).await.map_err(failed("tokens"))
}

/// Whether a program may revoke the token `id` of kind `kind` (`None`:
/// there is no such token, which the revoke itself answers).
fn program_may_revoke(kind: Option<TokenKind>, id: uuid::Uuid) -> Result<(), Refused> {
    match kind {
        Some(TokenKind::Operator) => Err((
            StatusCode::FORBIDDEN,
            format!("{id} is an operator key, which administers the whole install: a program cannot revoke one; `weft token revoke {id}` does"),
        )),
        _ => Ok(()),
    }
}

/// Mint `body` as `actor`: only a person, since the value is in the answer.
pub async fn signal_token_mint(state: &DispatcherState, actor: &Actor, body: MintTokenRequest) -> Result<MintedToken, Refused> {
    operator_only(actor, "weft.signal_tokens.mint")?;
    mint(state, actor.tenant(), body).await
}

/// Why a mint request cannot make the token it asks for. An instance token
/// names exactly one project and an expiry; how long an expiry may be is
/// `weft_core::signal_token::expiry_at`'s, checked at the mint. An
/// operator key is the tenant's admin: a scope or an instance on it would
/// read as a limit it does not have.
fn check_mint_shape(body: &MintTokenRequest) -> Result<(), String> {
    if body.kind == TokenKind::Operator
        && (!body.allowed_projects.is_empty()
            || !body.allowed_tags.is_empty()
            || !body.allowed_displays.is_empty()
            || body.all_displays
            || body.instance.is_some())
    {
        return Err("an operator key administers the whole install and takes no scope and no \
                    instance; mint a caller token for scoped access"
            .into());
    }
    if let Some(instance) = &body.instance {
        if body.allowed_projects.len() != 1 {
            return Err(format!(
                "an instance token acts in exactly one project: name it (--project) for instance '{instance}'"
            ));
        }
        if body.expires_in_secs.is_none() {
            return Err(format!(
                "an instance token lives in a browser, so it needs an expiry (--expires) for instance '{instance}'"
            ));
        }
    }
    Ok(())
}

/// Build a token's paste-able connect string from the dispatcher's public base.
fn token_url(public_base: &str, token: &str) -> String {
    format!("{}/signal-token/{token}", public_base.trim_end_matches('/'))
}

/// Mint `body` for `owner`: the one way a token is made, for the action
/// and for what makes one on a person's behalf (a frontend's). An instance
/// token is made by `weft_access_store::instance_tokens::mint`, the same
/// a program's mint through the broker goes through, which holds its
/// project to `owner`.
pub(crate) async fn mint(state: &DispatcherState, owner: &TenantId, body: MintTokenRequest) -> Result<MintedToken, Refused> {
    use weft_core::signal_token as names;

    let name = body.name.as_deref().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);

    // Validate tag charset on the way in. Same rule as _tags in .weft
    // source so tokens and signals share one vocabulary.
    if let Err(e) = weft_core::tag::validate_tags(&body.allowed_tags) {
        return Err((StatusCode::BAD_REQUEST, format!("allowed_tags: {e}")));
    }
    check_mint_shape(&body).map_err(|why| (StatusCode::BAD_REQUEST, why))?;

    // A display grant that cannot reach anything is refused HERE, with
    // the flag still in the person's hand, rather than minting a token
    // that answers 404 forever. What comes back is what gets stored:
    // the node exactly as the person spelled it, under a project id in
    // one canonical form.
    let allowed_displays = crate::api::display::canonical_display_grants(
        state,
        owner.as_str(),
        &body.allowed_projects,
        &body.allowed_displays,
        body.all_displays,
    )
    .await?;

    // The address is read before the token is stored, so a tunnel that
    // does not answer leaves no token behind that nobody was handed.
    let base = state.external_base_url().await.map_err(|e| (StatusCode::BAD_GATEWAY, format!("{e:#}")))?;
    let id = uuid::Uuid::new_v4();
    let (token, recognizer, expires_at) = match (&body.instance, body.expires_in_secs) {
        (Some(instance), Some(expires_in_secs)) => {
            let minted = weft_access_store::instance_tokens::mint(
                &state.pg_pool,
                &weft_access_store::instance_tokens::InstanceTokenMint {
                    id,
                    tenant: owner.as_str(),
                    project: body.allowed_projects[0],
                    instance,
                    name: name.as_deref(),
                    allowed_tags: &body.allowed_tags,
                    allowed_displays: &allowed_displays,
                    all_displays: body.all_displays,
                    expires_in_secs,
                    now_unix: crate::lease::now_unix() as u64,
                },
            )
            .await
            .map_err(|refused| match refused {
                weft_access_store::instance_tokens::MintRefused::NoProject => {
                    (StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string())
                }
                refused => (StatusCode::from_u16(refused.status()).expect("mint statuses are valid"), refused.to_string()),
            })?;
            (minted.token, minted.recognizer, Some(minted.expires_at))
        }
        (Some(_), None) => unreachable!("check_mint_shape refuses an instance token without an expiry"),
        (None, expires_in_secs) => {
            let now = crate::lease::now_unix() as u64;
            let expires_at = expires_in_secs
                .map(|secs| names::expiry_at(now, secs))
                .transpose()
                .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
            let token = names::generate_token();
            let signal_token = crate::store::SignalToken {
                id,
                kind: body.kind,
                token_hash: names::token_hash(&token),
                recognizer: names::recognizer(&token),
                tenant_id: owner.as_str().to_string(),
                name: name.clone(),
                allowed_projects: body.allowed_projects.clone(),
                allowed_tags: body.allowed_tags.clone(),
                allowed_displays: allowed_displays.clone(),
                all_displays: body.all_displays,
                // Stamp the mint time here (the canonical wall clock) and
                // store it verbatim, so both the postgres and fake token
                // stores agree instead of one stamping now() and the other
                // keeping a placeholder.
                created_at: now,
                instance: None,
                expires_at,
            };
            state.tokens.mint_signal_token(&signal_token).await.map_err(failed("tokens"))?;
            (token, signal_token.recognizer, expires_at)
        }
    };
    let url = token_url(&base, &token);
    Ok(MintedToken {
        id,
        kind: body.kind,
        token,
        recognizer,
        name,
        url,
        allowed_projects: body.allowed_projects,
        allowed_tags: body.allowed_tags,
        allowed_displays,
        all_displays: body.all_displays,
        instance: body.instance,
        expires_at_unix: expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(instance: Option<&str>, projects: usize, expires: Option<u64>) -> MintTokenRequest {
        MintTokenRequest {
            name: None,
            allowed_projects: (0..projects).map(|i| uuid::Uuid::from_u128(i as u128 + 1)).collect(),
            allowed_tags: vec![],
            allowed_displays: vec![],
            all_displays: false,
            instance: instance.map(|m| weft_core::instance::InstanceId::new(m).unwrap()),
            expires_in_secs: expires,
            kind: TokenKind::Caller,
        }
    }

    fn program() -> Actor {
        Actor::Program { tenant: TenantId::local(), project: uuid::Uuid::from_u128(1), run: uuid::Uuid::from_u128(2) }
    }

    #[test]
    fn an_operator_key_takes_no_scope_and_no_instance() {
        let operator = |b: MintTokenRequest| MintTokenRequest { kind: TokenKind::Operator, ..b };
        assert!(check_mint_shape(&operator(body(None, 0, None))).is_ok());
        assert!(check_mint_shape(&operator(body(None, 1, None))).unwrap_err().contains("no scope"));
        assert!(check_mint_shape(&operator(body(Some("ada"), 1, Some(60)))).is_err());
    }

    #[test]
    fn an_instance_token_names_one_project_and_an_expiry() {
        assert!(check_mint_shape(&body(Some("ada"), 1, Some(3600))).is_ok());
        assert!(check_mint_shape(&body(Some("ada"), 0, Some(3600))).unwrap_err().contains("exactly one project"));
        assert!(check_mint_shape(&body(Some("ada"), 2, Some(3600))).unwrap_err().contains("exactly one project"));
        assert!(check_mint_shape(&body(Some("ada"), 1, None)).unwrap_err().contains("needs an expiry"));
        assert!(check_mint_shape(&body(None, 0, None)).is_ok());
    }

    /// What answers with a credential's value is a person's alone; the
    /// rest of the area a program may ask for.
    #[test]
    fn a_program_is_refused_only_what_answers_with_a_secret() {
        let person = Actor::operator(TenantId::local());
        for name in ["weft.frontends.token_new", "weft.signal_tokens.mint", "weft.picks.check"] {
            assert!(operator_only(&program(), name).is_err(), "{name}");
            assert!(operator_only(&person, name).is_ok(), "{name}");
        }
        for name in ["weft.frontends.add", "weft.workers.put", "weft.picks.change", "weft.signal_tokens.revoke"] {
            assert!(operator_only(&program(), name).is_ok(), "{name}");
        }
    }

    #[test]
    fn a_program_makes_only_a_frontend_the_install_hosts() {
        let request = |host| AddFrontendRequest { name: "shop".into(), host, repo: None, visitor_address: None };
        let refused = program_may_add(&program(), &request(FrontendHost::External)).unwrap_err();
        assert_eq!(refused.0, StatusCode::FORBIDDEN);
        assert!(refused.1.contains("weft frontend add shop"), "{}", refused.1);
        assert!(program_may_add(&program(), &request(FrontendHost::CloudRun)).is_ok());
        assert!(program_may_add(&Actor::operator(TenantId::local()), &request(FrontendHost::External)).is_ok());
    }

    #[test]
    fn a_program_never_revokes_an_operator_key() {
        let id = uuid::Uuid::from_u128(9);
        let refused = program_may_revoke(Some(TokenKind::Operator), id).unwrap_err();
        assert!(refused.1.contains("weft token revoke"), "{}", refused.1);
        assert!(program_may_revoke(Some(TokenKind::Caller), id).is_ok());
        assert!(program_may_revoke(None, id).is_ok(), "a token that is not there is the revoke's to answer");
    }
}
