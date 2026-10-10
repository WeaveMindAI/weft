//! The worker's door: where a caller of the project's live routes arrives,
//! straight from outside at the project's own address (or passed on by
//! the install's relay from an address several projects share), and is
//! checked before their run is born here. A trigger's events come in the
//! same door (`crate::worker`, `/_weft/fire`).
//!
//! ```text
//!   caller ─► door: which route, does it take calls now (one that does
//!                   not answers 503 at once), a run permit (waits when the
//!                   worker takes all it may, `permits`), the gate (the
//!                   broker's word on a route with `auth`), whose instance,
//!                   the limits
//!          ─► the run, born and driven in this process, the caller on
//!             the line
//! ```
//!
//! These are the checks the dispatcher ran, with the same strength: the
//! routes are the same rows (held in memory, `broker`), the gate is the
//! broker's same check, and the limits are counted here and shared with
//! the project's other copies once a second while the worker has work
//! under way (`limits`, `ticks`). On an open route nothing on the way to
//! the run leaves this process.
//!
//! A browser cannot put a credential on a socket's opening request, so it
//! asks the route with a plain request and is answered a URL carrying a
//! ticket ([`weft_core::caller_token`]); the socket opened at it is born
//! with what the ticket says, on whichever copy it reaches.

pub(crate) mod broker;
pub(crate) mod limits;
pub(crate) mod permits;
pub(crate) mod ticks;

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use weft_broker_client::protocol::{ArmedEntry, CallerVerifyRequest, DoorCount, DoorTickRequest};
use weft_core::arrival::Arrival;
use weft_core::caller::LiveRequest;
use weft_core::caller_token::{CallerTokenClaims, RequestFingerprint};
use weft_core::instance::InstanceId;
use weft_core::signal::{LiveConnectionConfig, Protocol};

pub use broker::{DoorBroker, HeldDoorBroker, HeldEntry, HeldTrigger, RunFacts, Triggers};
pub use limits::{Going, Limits};
pub use permits::{Busy, RunPermit, RunPermits};
pub use ticks::WorkUnderWay;

/// What the install allows at its public edge, as the worker holds it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Edge {
    /// How many proxies in front of this worker append to
    /// `X-Forwarded-For`: the caller is the entry that many hops from the
    /// right.
    pub trusted_hops: usize,
    /// Refused tokens one address may present per minute before the door
    /// refuses it for the rest of the minute; `None` is no bound.
    pub invalid_tokens_per_minute: Option<u32>,
}

impl Edge {
    /// `WEFT_TRUSTED_HOPS` and `WEFT_INVALID_TOKENS_PER_MINUTE`, both set by
    /// the platform that starts the worker (`none` for no bound).
    pub fn from_env() -> anyhow::Result<Self> {
        let hops = std::env::var("WEFT_TRUSTED_HOPS")
            .map_err(|_| anyhow::anyhow!("WEFT_TRUSTED_HOPS is required: how many proxies in front of this worker append to X-Forwarded-For"))?;
        let trusted_hops = hops.trim().parse().map_err(|e| anyhow::anyhow!("WEFT_TRUSTED_HOPS is not a count: {e}"))?;
        let bound = std::env::var("WEFT_INVALID_TOKENS_PER_MINUTE")
            .map_err(|_| anyhow::anyhow!("WEFT_INVALID_TOKENS_PER_MINUTE is required: a count, or `none`"))?;
        let invalid_tokens_per_minute = match bound.trim() {
            "none" => None,
            n => Some(n.parse().map_err(|e| anyhow::anyhow!("WEFT_INVALID_TOKENS_PER_MINUTE is not a count: {e}"))?),
        };
        Ok(Self { trusted_hops, invalid_tokens_per_minute })
    }
}

/// See the module doc.
pub struct Door {
    pub(crate) project_id: uuid::Uuid,
    pub(crate) tenant_id: String,
    /// The program this worker runs: it serves the routes armed for it.
    pub(crate) binary_hash: String,
    broker: Arc<dyn DoorBroker>,
    limits: Arc<Limits>,
    /// When the door ticks (`ticks`).
    ticking: Arc<ticks::Ticking>,
    permits: Arc<RunPermits>,
    edge: Edge,
    /// The project's secret: socket tickets are signed with a key of its
    /// own.
    secret: weft_core::caller_token::ProjectSecret,
}

/// A call the door let in: what its run is born with.
pub struct Admitted {
    /// The route's trigger, and the version of the held triggers it was
    /// read from (what its run's plan is keyed by, `crate::plan`).
    pub trigger: Arc<HeldTrigger>,
    pub triggers_version: u64,
    pub entry: Arc<ArmedEntry>,
    pub protocol: Protocol,
    pub live_config: LiveConnectionConfig,
    /// The caller's opening request, as the trigger reads it.
    pub opening: LiveRequest,
    pub instance: Option<InstanceId>,
    /// The credentials the caller sent, which the run never writes down.
    pub redaction: weft_core::caller::Redaction,
    /// The run's place at the door while its own work goes.
    pub slot: Slot,
}

/// A run's place at this worker's door while its own work goes: its count
/// among its entry's runs at once (`limits`), and its run permit
/// (`permits`), both given back when it is dropped.
#[derive(Debug)]
pub struct Slot {
    going: Going,
    _permit: RunPermit,
}

impl Slot {
    /// The run failed: counted toward its entry's failed runs this minute.
    pub fn failed(&self, now: i64) {
        self.going.failed(now)
    }

    /// The admitted work did not become a run: it is not counted this
    /// minute, and its room and its permit are free.
    pub fn uncount(self) {
        self.going.uncount()
    }
}

/// The route a call matched, its armed entry, the path's captured
/// parameters, and the version of the held triggers it was matched in.
type RouteMatch = (Arc<HeldTrigger>, Arc<ArmedEntry>, BTreeMap<String, String>, u64);

/// What the door made of a call: a run to start, or the answer the caller
/// gets instead.
pub enum Arrived {
    Run(Box<Admitted>, axum::body::Body),
    Answer(Response),
}

/// The parts of a call the door reads.
pub struct Call<'a> {
    pub method: &'a str,
    /// The path under the project, with its leading slash, still
    /// percent-encoded as the caller sent it.
    pub raw_path: &'a str,
    pub raw_query: &'a str,
    pub headers: &'a HeaderMap,
    /// The address the connection came from, or, for a call weft's own
    /// relay passed on (`relayed`), the caller's address as the relay read
    /// it.
    pub peer: IpAddr,
    pub relayed: bool,
    /// The path the project's routes sit under at the address the caller
    /// used: what weft's relay said (`weft_core::net::relay_hop`), empty
    /// for a call straight to the worker.
    pub route_prefix: &'a str,
}

impl Call<'_> {
    /// Who is calling, by address: the relay's word for a call it passed
    /// on, else the `X-Forwarded-For` entries and the peer, read `hops`
    /// from the right.
    fn address(&self, hops: usize) -> IpAddr {
        if self.relayed {
            return self.peer;
        }
        weft_core::net::caller_address(forwarded_for(self.headers), self.peer, hops)
    }
}

fn answer(status: StatusCode, message: impl Into<String>) -> Arrived {
    Arrived::Answer((status, message.into()).into_response())
}

impl Door {
    pub fn new(
        project_id: uuid::Uuid,
        tenant_id: String,
        binary_hash: String,
        broker: Arc<dyn DoorBroker>,
        edge: Edge,
        secret: weft_core::caller_token::ProjectSecret,
        permits: Arc<RunPermits>,
    ) -> Arc<Self> {
        let ticking = ticks::Ticking::new();
        Arc::new(Self {
            project_id,
            tenant_id,
            binary_hash,
            broker,
            limits: Limits::new(ticking.clone()),
            ticking,
            permits,
            edge,
            secret,
        })
    }

    /// A run permit for an event, waiting for one while the worker takes
    /// all it may (`permits`).
    pub(crate) async fn take_permit(&self) -> Result<RunPermit, Busy> {
        self.permits.take().await
    }

    /// What the door asks the broker, which a run born here asks too.
    pub(crate) fn broker(&self) -> &Arc<dyn DoorBroker> {
        &self.broker
    }

    /// Count a run an event of the entry `token` starts against the entry's
    /// limits, the way a caller's run is counted: its per-minute limit,
    /// its runs at once on this copy, and, for an event somebody outside
    /// sent (`caller`, its key), that caller's per-minute limit. `permit`
    /// rides with the run's slot.
    pub(crate) fn admit_event(
        &self,
        permit: RunPermit,
        token: &str,
        caller: Option<&str>,
        limits: &weft_core::signal::ResolvedLimits,
    ) -> Result<Slot, limits::Refused> {
        let limits = match caller {
            Some(_) => *limits,
            None => weft_core::signal::ResolvedLimits { per_caller_per_minute: None, ..*limits },
        };
        let going = self.limits.admit(token, caller.unwrap_or("event"), &limits, crate::now_unix() as i64)?;
        Ok(Slot { going, _permit: permit })
    }

    /// One call at the door, up to the run it starts (see the module doc).
    pub async fn arrive(&self, call: Call<'_>, body: axum::body::Body) -> Arrived {
        let now = crate::now_unix() as i64;
        let path = percent_decoded(call.raw_path);
        let path = path.trim_start_matches('/');
        // A socket opened at the URL a ticket rode is held to the ticket.
        let socket = is_socket_opening(call.headers);
        let ticketed = weft_core::caller_token::split_ticket(call.raw_query).filter(|_| socket);
        let callers_query = ticketed.map_or(call.raw_query, |(callers, _)| callers);
        let (trigger, entry, params, triggers_version) = match self.route_taking_calls(call.method, path).await {
            Ok(found) => found,
            Err(arrived) => return arrived,
        };
        // Taken before anything of the body is read: a call waiting for
        // one holds its connection and nothing else.
        let Ok(permit) = self.permits.take().await else {
            return Arrived::Answer(crate::busy::busy());
        };
        let (protocol, live_config) = match &trigger.caller {
            Some(Ok((protocol, config))) => (*protocol, config.clone()),
            Some(Err(e)) => return answer(StatusCode::INTERNAL_SERVER_ERROR, format!("the route's settings: {e}")),
            None => return answer(StatusCode::INTERNAL_SERVER_ERROR, "the route is not armed".to_string()),
        };
        let headers_sent = headers_sent(call.headers);
        let header_map: BTreeMap<String, String> = headers_sent.iter().cloned().collect();
        let query = weft_core::route::parse_query(callers_query);
        let address = call.address(self.edge.trusted_hops);

        // Who the caller is and whom the run is for: the ticket's word on a
        // socket it opened, else the gate and the instance checks.
        let (caller, credential_headers, instance, body) = match ticketed {
            Some((_, ticket)) => match self.ticket(ticket, &trigger, call.method, path, callers_query, now) {
                Ok(claims) => (claims.identity, Vec::new(), claims.instance, body),
                Err(arrived) => return arrived,
            },
            None => {
                // The body is read here ONLY when the gate needs it (a
                // signing scheme covers the bytes); otherwise it streams on
                // to the run untouched.
                let (bytes, body) = if entry.auth_kind == "connection" {
                    let limit = live_config.max_inbound_bytes as usize;
                    match axum::body::to_bytes(body, limit).await {
                        Ok(bytes) => (bytes.clone(), axum::body::Body::from(bytes)),
                        Err(_) => return answer(StatusCode::PAYLOAD_TOO_LARGE, format!("request body exceeds {limit} bytes")),
                    }
                } else {
                    (axum::body::Bytes::new(), body)
                };
                let (caller, credential_headers) = match self.gate(&entry, call.method, path, &header_map, &query, &bytes).await {
                    Ok(Some(verified)) => (Some(verified.identity), verified.credential_headers),
                    Ok(None) => (None, Vec::new()),
                    Err(arrived) => return arrived,
                };
                let named = match self.instance(&header_map, entry.auth_kind != "none", &address.to_string(), now).await {
                    Ok(named) => named,
                    Err(arrived) => return arrived,
                };
                // The run is for the instance the call names, else the one
                // whose copy of the route it reached.
                let instance = match (&entry.instance, named) {
                    (Some(own), Some(named)) if *own != named => {
                        return answer(
                            StatusCode::BAD_REQUEST,
                            format!("this route is instance '{own}'s copy, and the call names instance '{named}'"),
                        );
                    }
                    (own, named) => named.or_else(|| own.clone()),
                };
                // A browser asking for its socket is handed the URL to open
                // it at, carrying what was just approved.
                if protocol == Protocol::Websocket && !socket {
                    let approved = (entry.auth_kind != "none").then(|| RequestFingerprint::of(call.method, path, callers_query, &bytes));
                    return self.hand_ticket(&trigger, call, approved, caller, instance, now);
                }
                (caller, credential_headers, instance, body)
            }
        };

        // The limits, as the run is born: the caller is who the gate
        // established when the route has auth, else the instance, else the
        // address. A socket opened with a ticket is counted here like any
        // call, never when its ticket was handed: a ticket opens as many
        // sockets as its life allows, and each is a run.
        let caller_key = match (&caller, &instance) {
            (Some(identity), _) => format!("id:{identity}"),
            (None, Some(instance)) => format!("instance:{instance}"),
            (None, None) => format!("ip:{address}"),
        };
        let going = match self.limits.admit(&trigger.token, &caller_key, &entry.spec.limits.resolve(), now) {
            Ok(going) => going,
            Err(refused) => return Arrived::Answer(too_many(refused)),
        };
        let opening = LiveRequest {
            method: call.method.to_string(),
            path: path.to_string(),
            params,
            query,
            base_url: weft_core::net::request_base_url(call.headers),
            headers: headers_sent,
            caller,
        };
        let redaction = opening.credentials(&credential_headers);
        let slot = Slot { going, _permit: permit };
        Arrived::Run(Box::new(Admitted { trigger, triggers_version, entry, protocol, live_config, opening, instance, redaction, slot }), body)
    }

    /// The live route serving `method` on `path` while its trigger takes
    /// calls: the route, its entry, and the path's captures. A caller is
    /// never held: a route whose trigger is not on (parked, hibernating,
    /// being set up, off) answers `503` with a `Retry-After` at once, in the
    /// caller's terms. Parking keeps the events a trigger picks up; a
    /// caller retries (`weft_core::arrival`).
    async fn route_taking_calls(&self, method: &str, path: &str) -> Result<RouteMatch, Arrived> {
        let found = self.live_route(method, path).await?;
        let standing = found.1.standing;
        match standing.arrival(crate::now_unix() as i64) {
            Arrival::Live => Ok(found),
            Arrival::Wait | Arrival::Refused => {
                tracing::info!(target: "weft_engine::door", node = %found.0.node_id, status = %standing.status.as_str(), "a caller turned away: the route's trigger takes no calls now (`weft status` shows it; `weft activate` switches it back on)");
                Err(Arrived::Answer(not_now()))
            }
        }
    }

    /// The route serving `method` on `path` as it is armed for this
    /// worker's program, whether or not it takes calls, matched against
    /// the routes this worker holds: the most specific pattern wins. One
    /// armed for another program is another worker's to serve.
    async fn live_route(&self, method: &str, path: &str) -> Result<RouteMatch, Arrived> {
        let triggers = self.broker.triggers().await.map_err(|e| {
            tracing::error!(target: "weft_engine::door", error = %format!("{e:#}"), "the project's triggers could not be read");
            answer(StatusCode::SERVICE_UNAVAILABLE, "this worker could not read its routes; try again in a moment")
        })?;
        let (trigger, params) = match triggers.route(method, path) {
            weft_core::route::RouteMatch::Found { route, params } => (route.clone(), params),
            weft_core::route::RouteMatch::WrongMethod { allowed } => {
                return Err(answer(StatusCode::METHOD_NOT_ALLOWED, format!("{method} is not served at this path; allowed: {}", allowed.join(", "))))
            }
            weft_core::route::RouteMatch::NotFound => return Err(answer(StatusCode::NOT_FOUND, "no live endpoint at this path")),
        };
        let entry = match &trigger.entry {
            HeldEntry::Armed(entry) => entry.clone(),
            HeldEntry::Unservable { status, why } => {
                return Err(answer(StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), why.clone()))
            }
        };
        if entry.binary_hash != self.binary_hash {
            // The route still belongs to another version of the program
            // (a version is taking over): the caller tries again, and finds
            // the route here once this version holds it.
            tracing::info!(target: "weft_engine::door", token = %trigger.token, "a caller turned away: the route belongs to another version of the program for now");
            return Err(Arrived::Answer(not_now()));
        }
        Ok((trigger, entry, params, triggers.version))
    }

    /// The gate of `entry`: `None` on an open route, what the broker
    /// established on a gated one (who the caller is, and the headers their
    /// credential rode), the door's `401` when it refused.
    async fn gate(
        &self,
        entry: &ArmedEntry,
        method: &str,
        path: &str,
        headers: &BTreeMap<String, String>,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<Option<weft_broker_client::protocol::CallerVerified>, Arrived> {
        match entry.auth_kind.as_str() {
            "none" => Ok(None),
            "connection" => {
                let field = |name: &str| -> Result<String, Arrived> {
                    entry
                        .auth_config
                        .as_ref()
                        .and_then(|cfg| cfg.get(name))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                        .ok_or_else(|| answer(StatusCode::INTERNAL_SERVER_ERROR, format!("connection auth config has no '{name}'")))
                };
                let request = CallerVerifyRequest {
                    tenant: self.tenant_id.clone(),
                    for_instance: entry
                        .instance
                        .clone()
                        .map(|instance| weft_core::instance::InstanceScope { project_id: self.project_id, instance }),
                    access_id: field("access_id")?,
                    service: field("service")?,
                    method: method.to_string(),
                    path: path.to_string(),
                    headers: headers.clone(),
                    query: query.clone(),
                    body_b64: {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD.encode(body)
                    },
                };
                match self.broker.verify_caller(&request).await {
                    Ok(Some(verified)) => Ok(Some(verified)),
                    // The broker's no IS the answer (flat, the reason in
                    // its log).
                    Ok(None) => Err(answer(StatusCode::UNAUTHORIZED, "refused")),
                    Err(e) => {
                        tracing::error!(target: "weft_engine::door", error = %format!("{e:#}"), "the caller's check could not be made");
                        Err(answer(StatusCode::SERVICE_UNAVAILABLE, "the caller could not be checked; try again in a moment"))
                    }
                }
            }
            other => Err(answer(StatusCode::INTERNAL_SERVER_ERROR, format!("unknown auth_kind: {other}"))),
        }
    }

    /// Who a call is for. An instance token
    /// ([`weft_core::instance::INSTANCE_TOKEN_HEADER`]) names its instance
    /// on any route of its one project; otherwise the instance header names
    /// one, honoured only on a `gated` route. A token and a header that
    /// disagree are refused rather than one silently winning. An address
    /// presenting refused tokens is bounded ([`Edge`]).
    async fn instance(
        &self,
        headers: &BTreeMap<String, String>,
        gated: bool,
        address: &str,
        now: i64,
    ) -> Result<Option<InstanceId>, Arrived> {
        let named = weft_core::instance::instance_from_header(headers, gated).map_err(|why| answer(StatusCode::BAD_REQUEST, why));
        let presented = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(weft_core::instance::INSTANCE_TOKEN_HEADER))
            .map(|(_, value)| value.trim().to_string());
        let Some(presented) = presented else { return named };
        if let Err(refused) = self.limits.address_allowed(address, self.edge.invalid_tokens_per_minute, now) {
            return Err(Arrived::Answer(too_many(refused)));
        }
        let instance = match self.broker.instance_token(&presented).await {
            Ok(Some(instance)) => instance,
            Ok(None) => {
                self.limits.token_refused(address, now);
                return Err(answer(StatusCode::UNAUTHORIZED, "this is not an instance token of this project"));
            }
            Err(e) => {
                tracing::error!(target: "weft_engine::door", error = %format!("{e:#}"), "an instance token could not be checked");
                return Err(answer(StatusCode::SERVICE_UNAVAILABLE, "the instance token could not be checked; try again in a moment"));
            }
        };
        // The header is checked only against the token here: a token is
        // its own proof, so the open-route rule does not apply to it.
        let header_instance = weft_core::instance::instance_from_header(headers, true).map_err(|why| answer(StatusCode::BAD_REQUEST, why))?;
        if header_instance.as_ref().is_some_and(|named| *named != instance) {
            return Err(answer(
                StatusCode::BAD_REQUEST,
                format!(
                    "the {} header names instance '{}', and the instance token is instance '{instance}'s",
                    weft_core::instance::INSTANCE_HEADER,
                    header_instance.expect("checked above"),
                ),
            ));
        }
        Ok(Some(instance))
    }

    /// Answer a browser asking for its socket: the URL to open it at, on
    /// the address it reached this route at, with a ticket holding what
    /// was approved. The socket it opens is counted when it opens.
    #[allow(clippy::too_many_arguments)]
    fn hand_ticket(
        &self,
        trigger: &HeldTrigger,
        call: Call<'_>,
        approved: Option<RequestFingerprint>,
        identity: Option<serde_json::Value>,
        instance: Option<InstanceId>,
        now: i64,
    ) -> Arrived {
        let Some(base) = weft_core::net::request_base_url(call.headers) else {
            return answer(StatusCode::BAD_REQUEST, "the request names no host to open the socket at");
        };
        let claims = CallerTokenClaims {
            project_id: self.project_id,
            route_token: trigger.token.clone(),
            approved,
            identity,
            instance,
            nonce: uuid::Uuid::new_v4(),
            exp: now + weft_core::time_scale::scaled_secs(TICKET_LIFE_SECS),
        };
        let ticket = self.secret.mint_ticket(&claims);
        let url = weft_core::caller_token::socket_url(&format!("{base}{}", call.route_prefix), call.raw_path, call.raw_query, &ticket);
        // SYNC: the ticket answer's shape <-> crates/weft-e2e/src/live.rs (ticket), packages/weft-connect/src/core/socket.ts (socketAddress)
        let body = serde_json::json!({ "url": url, "protocol": "websocket" });
        Arrived::Answer(
            Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .expect("json response builds"),
        )
    }

    /// The claims of the ticket a socket opened with, held to the route and
    /// the request it was handed for.
    fn ticket(
        &self,
        ticket: &str,
        trigger: &HeldTrigger,
        method: &str,
        path: &str,
        callers_query: &str,
        now: i64,
    ) -> Result<CallerTokenClaims, Arrived> {
        let claims = self
            .secret
            .validate_ticket(ticket, now)
            .map_err(|why| answer(StatusCode::UNAUTHORIZED, weft_core::caller_token::refusal(&why)))?;
        if claims.project_id != self.project_id || claims.route_token != trigger.token {
            return Err(answer(StatusCode::FORBIDDEN, "this ticket opens another route"));
        }
        // Held to the request the gate approved, when it checked anything:
        // a socket's opening request has no body.
        if let Some(approved) = &claims.approved {
            if *approved != RequestFingerprint::of(method, path, callers_query, b"") {
                return Err(answer(
                    StatusCode::FORBIDDEN,
                    "this is not the request that was approved: the ticket you were given opens the call you made, not another one",
                ));
            }
        }
        Ok(claims)
    }

    /// While the worker has work under way, every second: state this
    /// worker's counts, hear the other copies and keep the worker's lease
    /// (`/v1/door/tick`); with nothing under way, nothing (`ticks`). A tick
    /// that fails is made again the next second; the limits hold to what
    /// was last heard. `busy` is what the worker holds besides the door's
    /// own runs.
    ///
    /// The first tick is made before this returns, so the worker is known
    /// alive before it takes its first call: a worker that cannot make it
    /// does not start, and says why.
    pub async fn start_ticks(self: &Arc<Self>, busy: WorkUnderWay) -> anyhow::Result<()> {
        let quiet = self.tick().await.map_err(|e| e.context("the worker's first word to the broker (its lease, and its door's counts)"))?;
        let door = Arc::downgrade(self);
        let ticking = self.ticking.clone();
        tokio::spawn(async move {
            let every_second = weft_core::time_scale::scaled(ticks::TICK_EVERY);
            let mut every = tokio::time::interval_at(tokio::time::Instant::now() + every_second, every_second);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The last tick landed and reported nothing in flight.
            let mut quiet = quiet;
            // The loop slept since its last tick: the next one goes at once.
            let mut slept = false;
            loop {
                match door.upgrade() {
                    // The last tick said all there is: sleep until work
                    // comes, then look again (a wake left while the loop
                    // was ticking finds nothing new and sleeps on).
                    Some(strong) if quiet && strong.limits.settled() && !busy() => {
                        // Asleep, this copy hears nothing of the others,
                        // whose leases may lapse meanwhile.
                        strong.limits.asleep();
                        // Not held while asleep: the door goes when its
                        // worker does.
                        drop(strong);
                        ticking.sleep().await;
                        slept = true;
                        continue;
                    }
                    Some(_) => {}
                    None => return,
                }
                if std::mem::take(&mut slept) {
                    every.reset();
                } else {
                    every.tick().await;
                }
                let Some(door) = door.upgrade() else { return };
                quiet = match door.tick().await {
                    Ok(quiet) => quiet,
                    Err(e) => {
                        tracing::warn!(target: "weft_engine::door", error = %format!("{e:#}"), "a tick of the door's counts failed; the next second tries again");
                        false
                    }
                };
            }
        });
        Ok(())
    }

    /// One tick: whether it reported nothing in flight. Its counts are
    /// marked heard once it lands.
    async fn tick(&self) -> anyhow::Result<bool> {
        let now = crate::now_unix() as i64;
        let (window_start, mine, mark) = self.limits.mine(now);
        let in_flight = self.limits.going();
        let quiet = in_flight.is_empty();
        let heard = async {
            let tokens = self.broker.triggers().await?.tokens().cloned().collect();
            let request = DoorTickRequest {
                binary_hash: Some(self.binary_hash.clone()),
                in_flight,
                window_start,
                counts: mine.into_iter().map(|(key, hits)| DoorCount { key, hits }).collect(),
                tokens,
            };
            self.broker.tick(&request).await
        }
        .await?;
        self.limits.reported(mark);
        self.limits.heard(window_start, heard.others.into_iter().map(|count| (count.key, count.hits)), heard.copies);
        Ok(quiet)
    }

    /// A run came onto this worker other than through the door (a claim of
    /// a queued run), once it is counted where the tick looks: the tick
    /// goes out now and renews the lease behind the run, which does not
    /// wait for it (`ticks`).
    pub(crate) fn wake(&self) {
        self.ticking.wake();
    }
}

/// How long a socket ticket is good for: enough for a slow client (a
/// phone, a cold DNS) to open its socket after asking for it.
const TICKET_LIFE_SECS: i64 = 120;

/// The `503` a call gets while what it called cannot take it
/// (`weft_core::arrival::NOT_NOW_FOR_CALLER`).
pub(crate) fn not_now() -> Response {
    use weft_core::arrival::{NOT_NOW_FOR_CALLER, NOT_NOW_RETRY_SECS};
    (StatusCode::SERVICE_UNAVAILABLE, [(axum::http::header::RETRY_AFTER, NOT_NOW_RETRY_SECS.to_string())], NOT_NOW_FOR_CALLER).into_response()
}

/// The `429` a refused call gets: which limit, and when to come back.
fn too_many(refused: limits::Refused) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, refused.retry_after_secs.to_string())],
        refused.answer(),
    )
        .into_response()
}

/// Every header the caller sent, repeats included: what the gate checks,
/// and what the run's trigger reads as the caller's opening request.
fn headers_sent(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| Some((name.as_str().to_string(), value.to_str().ok()?.to_string())))
        .collect()
}

fn forwarded_for(headers: &HeaderMap) -> Option<&str> {
    headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
}

/// Whether `headers` open a WebSocket (`Upgrade: websocket`), which every
/// socket client sends, a browser's included.
pub fn is_socket_opening(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

/// `raw` with its percent escapes decoded, the way a route pattern's
/// segments are matched.
fn percent_decoded(raw: &str) -> String {
    percent_encoding::percent_decode_str(raw).decode_utf8_lossy().into_owned()
}

/// A broker for the door's tests: dumb state a test sets, and a log of
/// what was asked.
#[cfg(test)]
pub(crate) mod fake {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::Value;
    use weft_broker_client::protocol::{CallerVerifyRequest, DoorEntry, DoorRunFacts, DoorTick, DoorTickRequest, DoorTrigger};
    use weft_core::instance::InstanceId;

    use super::*;

    pub(crate) struct FakeDoorBroker {
        /// The project's triggers, as the broker would answer them.
        pub triggers: Mutex<Vec<DoorTrigger>>,
        pub run_facts: Mutex<DoorRunFacts>,
        /// What the gate answers: `None` refuses.
        pub identity: Mutex<Option<Value>>,
        pub verified: Mutex<Vec<CallerVerifyRequest>>,
        pub instance_tokens: Mutex<HashMap<String, InstanceId>>,
        /// Every tick tried, landed or not.
        pub ticks: Mutex<Vec<DoorTickRequest>>,
        /// How long a tick takes to answer, and whether it fails.
        pub tick_takes: Mutex<std::time::Duration>,
        pub tick_fails: std::sync::atomic::AtomicBool,
        /// What a tick hears: the other copies' counts, and the copies alive.
        pub heard: Mutex<(Vec<weft_broker_client::protocol::DoorCount>, u32)>,
        /// The fires parked, in order, with their token.
        pub parked: Mutex<Vec<(String, weft_task_store::parked_fires::Waiting)>>,
    }

    impl FakeDoorBroker {
        pub(crate) fn new(triggers: Vec<DoorTrigger>) -> Arc<Self> {
            Arc::new(Self::bare(triggers))
        }

        /// [`Self::new`], not shared yet, to arm before it is.
        pub(crate) fn bare(triggers: Vec<DoorTrigger>) -> Self {
            Self {
                triggers: Mutex::new(triggers),
                run_facts: Mutex::new(DoorRunFacts::default()),
                identity: Mutex::new(None),
                verified: Mutex::new(Vec::new()),
                instance_tokens: Mutex::new(HashMap::new()),
                ticks: Mutex::new(Vec::new()),
                tick_takes: Mutex::new(std::time::Duration::ZERO),
                tick_fails: std::sync::atomic::AtomicBool::new(false),
                heard: Mutex::new((Vec::new(), 1)),
                parked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl DoorBroker for FakeDoorBroker {
        async fn triggers(&self) -> anyhow::Result<Arc<Triggers>> {
            Ok(Arc::new(Triggers::of(self.triggers.lock().unwrap().clone())))
        }
        async fn fresh_triggers(&self) -> anyhow::Result<Arc<Triggers>> {
            self.triggers().await
        }
        async fn run_facts(&self, _instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>> {
            Ok(Arc::new(RunFacts::of(self.run_facts.lock().unwrap().clone())))
        }
        async fn fresh_run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>> {
            self.run_facts(instance).await
        }
        async fn verify_caller(&self, request: &CallerVerifyRequest) -> anyhow::Result<Option<weft_broker_client::protocol::CallerVerified>> {
            self.verified.lock().unwrap().push(request.clone());
            Ok(self
                .identity
                .lock()
                .unwrap()
                .clone()
                .map(|identity| weft_broker_client::protocol::CallerVerified { identity, credential_headers: vec!["x-api-key".into()] }))
        }
        async fn instance_token(&self, token: &str) -> anyhow::Result<Option<InstanceId>> {
            Ok(self.instance_tokens.lock().unwrap().get(token).cloned())
        }
        async fn tick(&self, request: &DoorTickRequest) -> anyhow::Result<DoorTick> {
            self.ticks.lock().unwrap().push(request.clone());
            let takes = *self.tick_takes.lock().unwrap();
            if !takes.is_zero() {
                tokio::time::sleep(takes).await;
            }
            if self.tick_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("the broker is away");
            }
            let (others, copies) = self.heard.lock().unwrap().clone();
            Ok(DoorTick { others, copies })
        }
        async fn park_fire(
            &self,
            token: &str,
            fire: &weft_task_store::parked_fires::Waiting,
            _held_by: Option<&str>,
        ) -> anyhow::Result<weft_broker_client::protocol::DoorParked> {
            self.parked.lock().unwrap().push((token.to_string(), fire.clone()));
            Ok(weft_broker_client::protocol::DoorParked::Parked)
        }
    }

    pub(crate) const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0x5eed);
    pub(crate) const BINARY: &str = "bin-1";
    pub(crate) const SECRET: &[u8] = b"test-secret-32-bytes-aaaaaaaaaaa";

    /// A trigger of `kind` armed for this worker's program and live: a
    /// route somebody calls at `pattern` when it has one.
    fn trigger(token: &str, kind: &str, pattern: Option<&str>) -> DoorTrigger {
        let spec: weft_core::primitive::SignalSpec =
            serde_json::from_value(serde_json::json!({ "kind": kind, "config": { "path": pattern.unwrap_or_default() } })).unwrap();
        DoorTrigger {
            token: token.into(),
            node_id: "door".into(),
            route: pattern.map(|pattern| weft_broker_client::protocol::DoorMount { pattern: pattern.into(), methods: Vec::new() }),
            entry: DoorEntry::Armed(Box::new(ArmedEntry {
                spec,
                auth_kind: "none".into(),
                auth_config: None,
                port_snapshot: None,
                definition_hash: "def-1".into(),
                binary_hash: BINARY.into(),
                source_version: "v1".into(),
                standing: weft_core::arrival::Standing {
                    status: weft_core::projects::ProjectStatus::Active,
                    accepting_fires: true,
                    fires_deadline_unix: None,
                },
                instance: None,
            })),
            held_by: None,
        }
    }

    /// A live route of `kind` (`route` or `socket`) at `pattern`.
    pub(crate) fn route(token: &str, kind: &str, pattern: &str) -> DoorTrigger {
        trigger(token, kind, Some(pattern))
    }

    /// A trigger nobody calls (`kind` a timer, a poll): its events come in
    /// as fires.
    pub(crate) fn event_trigger(token: &str, kind: &str) -> DoorTrigger {
        trigger(token, kind, None)
    }

    pub(crate) fn armed(trigger: &mut DoorTrigger) -> &mut ArmedEntry {
        match &mut trigger.entry {
            DoorEntry::Armed(entry) => entry,
            DoorEntry::Unservable { .. } => panic!("an armed trigger"),
        }
    }

    pub(crate) fn door(broker: Arc<FakeDoorBroker>) -> Arc<Door> {
        Door::new(
            PROJECT,
            "local".into(),
            BINARY.into(),
            broker,
            Edge { trusted_hops: 0, invalid_tokens_per_minute: Some(2) },
            weft_core::caller_token::ProjectSecret::of(SECRET, PROJECT),
            RunPermits::new(64, std::time::Duration::from_secs(30)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("host", "127.0.0.1:9000".parse().unwrap());
        for (name, value) in pairs {
            headers.insert(axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(), value.parse().unwrap());
        }
        headers
    }

    async fn arrive(door: &Door, method: &str, path: &str, query: &str, headers: &HeaderMap) -> Arrived {
        let call = Call { method, raw_path: path, raw_query: query, headers, peer: "10.0.0.1".parse().unwrap(), relayed: false, route_prefix: "" };
        door.arrive(call, axum::body::Body::empty()).await
    }

    async fn status(arrived: Arrived) -> (StatusCode, String) {
        match arrived {
            Arrived::Run(..) => (StatusCode::OK, "a run".into()),
            Arrived::Answer(answer) => {
                let status = answer.status();
                let body = axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        }
    }

    #[tokio::test]
    async fn a_call_to_a_live_route_is_admitted_with_its_opening_request() {
        let door = door(FakeDoorBroker::new(vec![route("t1", "route", "users/{id}")]));
        let Arrived::Run(admitted, _) = arrive(&door, "GET", "/users/42", "a=1", &headers(&[("x-test", "yes")])).await else {
            panic!("admitted")
        };
        assert_eq!(admitted.trigger.token, "t1");
        assert_eq!(admitted.opening.params.get("id").map(String::as_str), Some("42"));
        assert_eq!(admitted.opening.query.get("a").map(String::as_str), Some("1"));
        assert!(admitted.opening.headers.iter().any(|(n, v)| n == "x-test" && v == "yes"));
        assert_eq!(admitted.opening.base_url.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[tokio::test]
    async fn a_path_no_route_serves_and_a_route_of_another_program_are_refused() {
        let mut other = route("t2", "route", "elsewhere");
        armed(&mut other).binary_hash = "bin-2".into();
        let door = door(FakeDoorBroker::new(vec![route("t1", "route", "here"), other]));
        assert_eq!(status(arrive(&door, "GET", "/nowhere", "", &headers(&[])).await).await.0, StatusCode::NOT_FOUND);
        assert_eq!(status(arrive(&door, "GET", "/elsewhere", "", &headers(&[])).await).await.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn a_route_that_takes_no_work_is_refused_in_the_callers_terms() {
        let mut wiped = route("t1", "route", "x");
        armed(&mut wiped).standing.status = weft_core::projects::ProjectStatus::Inactive;
        armed(&mut wiped).standing.accepting_fires = false;
        let door = door(FakeDoorBroker::new(vec![wiped]));
        let (code, body) = status(arrive(&door, "GET", "/x", "", &headers(&[])).await).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, weft_core::arrival::NOT_NOW_FOR_CALLER);
    }

    /// A parked route turns its caller away at once, telling them when to
    /// try again: no worker holds a caller (it goes away on an update).
    #[tokio::test]
    async fn a_parked_route_turns_the_caller_away_with_a_retry_after() {
        let mut parked = route("t1", "route", "x");
        armed(&mut parked).standing.status = weft_core::projects::ProjectStatus::Inactive;
        let door = door(FakeDoorBroker::new(vec![parked]));
        let Arrived::Answer(answer) = arrive(&door, "GET", "/x", "", &headers(&[])).await else { panic!("turned away") };
        assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(answer.headers().contains_key(axum::http::header::RETRY_AFTER));
    }

    #[tokio::test]
    async fn a_gated_route_asks_the_broker_and_takes_its_word() {
        let mut gated = route("t1", "route", "x");
        armed(&mut gated).auth_kind = "connection".into();
        armed(&mut gated).auth_config = Some(serde_json::json!({ "access_id": "a", "service": "s" }));
        let broker = FakeDoorBroker::new(vec![gated]);
        let door = door(broker.clone());
        assert_eq!(status(arrive(&door, "POST", "/x", "", &headers(&[])).await).await.0, StatusCode::UNAUTHORIZED);
        *broker.identity.lock().unwrap() = Some(serde_json::json!({ "sub": "alice" }));
        let Arrived::Run(admitted, _) = arrive(&door, "POST", "/x", "", &headers(&[])).await else { panic!("admitted") };
        assert_eq!(admitted.opening.caller, Some(serde_json::json!({ "sub": "alice" })));
        assert_eq!(broker.verified.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn an_open_route_refuses_the_instance_header_and_a_token_names_its_instance() {
        let broker = FakeDoorBroker::new(vec![route("t1", "route", "x")]);
        broker.instance_tokens.lock().unwrap().insert("tok-ann".into(), InstanceId::new("ann").unwrap());
        let door = door(broker);
        let header = weft_core::instance::INSTANCE_HEADER;
        let token = weft_core::instance::INSTANCE_TOKEN_HEADER;
        assert_eq!(status(arrive(&door, "GET", "/x", "", &headers(&[(header, "ann")])).await).await.0, StatusCode::BAD_REQUEST);
        let Arrived::Run(admitted, _) = arrive(&door, "GET", "/x", "", &headers(&[(token, "tok-ann")])).await else { panic!("admitted") };
        assert_eq!(admitted.instance, Some(InstanceId::new("ann").unwrap()));
        assert_eq!(
            status(arrive(&door, "GET", "/x", "", &headers(&[(token, "tok-ann"), (header, "bob")])).await).await.0,
            StatusCode::BAD_REQUEST,
            "a header that disagrees with the token"
        );
    }

    #[tokio::test]
    async fn an_address_guessing_instance_tokens_is_refused() {
        let door = door(FakeDoorBroker::new(vec![route("t1", "route", "x")]));
        let token = weft_core::instance::INSTANCE_TOKEN_HEADER;
        for _ in 0..2 {
            assert_eq!(status(arrive(&door, "GET", "/x", "", &headers(&[(token, "nope")])).await).await.0, StatusCode::UNAUTHORIZED);
        }
        assert_eq!(status(arrive(&door, "GET", "/x", "", &headers(&[(token, "nope")])).await).await.0, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn a_caller_over_its_limit_is_told_when_to_come_back() {
        let mut limited = route("t1", "route", "x");
        armed(&mut limited).spec.limits = weft_core::signal::EntryLimits { per_caller_per_minute: Some(1), ..Default::default() };
        let door = door(FakeDoorBroker::new(vec![limited]));
        let _first = arrive(&door, "GET", "/x", "", &headers(&[])).await;
        let Arrived::Answer(answer) = arrive(&door, "GET", "/x", "", &headers(&[])).await else { panic!("refused") };
        assert_eq!(answer.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(answer.headers().contains_key(axum::http::header::RETRY_AFTER));
    }

    /// A browser's plain request for its socket is handed a URL on the
    /// address it used, whose ticket opens that socket, and only that one.
    #[tokio::test]
    async fn a_socket_is_opened_with_the_ticket_its_request_was_handed() {
        let door = door(FakeDoorBroker::new(vec![route("t1", "socket", "feed")]));
        let (code, body) = status(arrive(&door, "GET", "/feed", "v=1", &headers(&[])).await).await;
        assert_eq!(code, StatusCode::OK);
        let url = serde_json::from_str::<serde_json::Value>(&body).unwrap()["url"].as_str().unwrap().to_string();
        assert!(url.starts_with("http://127.0.0.1:9000/feed?v=1&wct="), "{url}");
        let query = url.split_once('?').unwrap().1.to_string();
        let opening = headers(&[("upgrade", "websocket")]);
        let Arrived::Run(admitted, _) = arrive(&door, "GET", "/feed", &query, &opening).await else { panic!("opened") };
        assert_eq!(admitted.opening.query.get("v").map(String::as_str), Some("1"), "the caller's own query, ticket off");
        assert!(!admitted.opening.query.contains_key("wct"));
        // A forged ticket opens nothing.
        let forged = format!("v=1&wct={}", weft_core::caller_token::ProjectSecret::of(b"another-secret-32-bytes-bbbbbbbb", PROJECT).mint_ticket(&weft_core::caller_token::CallerTokenClaims {
            project_id: PROJECT, route_token: "t1".into(), approved: None, identity: None, instance: None, nonce: uuid::Uuid::nil(), exp: i64::MAX,
        }));
        assert_eq!(status(arrive(&door, "GET", "/feed", &forged, &opening).await).await.0, StatusCode::UNAUTHORIZED);
    }

    /// A gated socket's ticket holds the caller to the request the gate
    /// checked, and carries who it said they are.
    #[tokio::test]
    async fn a_gated_sockets_ticket_carries_the_caller_and_holds_them_to_their_request() {
        let mut gated = route("t1", "socket", "feed");
        armed(&mut gated).auth_kind = "connection".into();
        armed(&mut gated).auth_config = Some(serde_json::json!({ "access_id": "a", "service": "s" }));
        let broker = FakeDoorBroker::new(vec![gated]);
        *broker.identity.lock().unwrap() = Some(serde_json::json!({ "sub": "alice" }));
        let door = door(broker.clone());
        let (_, body) = status(arrive(&door, "GET", "/feed", "room=1", &headers(&[])).await).await;
        let url = serde_json::from_str::<serde_json::Value>(&body).unwrap()["url"].as_str().unwrap().to_string();
        let ticket = url.rsplit_once("wct=").unwrap().1.to_string();
        let opening = headers(&[("upgrade", "websocket")]);
        let Arrived::Run(admitted, _) = arrive(&door, "GET", "/feed", &format!("room=1&wct={ticket}"), &opening).await else { panic!("opened") };
        assert_eq!(admitted.opening.caller, Some(serde_json::json!({ "sub": "alice" })));
        assert_eq!(broker.verified.lock().unwrap().len(), 1, "the socket is not gated a second time");
        assert_eq!(
            status(arrive(&door, "GET", "/feed", &format!("room=2&wct={ticket}"), &opening).await).await.0,
            StatusCode::FORBIDDEN,
            "another request than the one approved"
        );
    }
}
