//! What every agent node shares: the agent server it starts and talks to.
//!
//! An agent node (`ClaudeCode`, `Codex`, ...) is an infra node holding ONE
//! conversation. The conversation lives in the agent server
//! (`server/agent_server.py`), which owns the agent's process, its queue of
//! turns, its numbered events and the call it is blocked on, so all of it
//! outlives the short-lived workers that run the nodes:
//!
//! - with no `computer` wired, the server is a process unit on the
//!   machine weft runs on (a local install only), run from the person's own
//!   install of the agent with their own login, in the folder they picked;
//! - with a `computer` wired, the node runs no unit of its own: its `run`
//!   installs the agent and the server inside the computer through the
//!   computer's `exec` action, and its `api` endpoint is the computer's
//!   own, under `/apps/<name>`, which the computer hands on to the server.
//!
//! Either way the node's `agent` output is the handle of its `api`
//! endpoint, so `AskAgent` never knows which. What differs from one agent to
//! the next (how to install it, how a login reaches it, its options) is
//! data: `server/agents.json`, read here and by the server.
//!
//! Each agent has ONE install recipe (`install` in agents.json, a bash
//! script per system, sourcing `server/install_lib.sh`), which puts it into
//! a folder of its own: on the machine, weft runs it as the process's need
//! (`weft::needs::Recipe`) into `~/.local/share/weft/needs/agents/<kind>/` when
//! the agent is not installed there already; inside a computer,
//! [`computer_script`] runs the same recipe into `~/.weft/agents/<kind>/`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use weft::infra::{
    Endpoint, EndpointTarget, EnvEntry, Expose, InfraHandle, InfraSpec, Process, ProcessPort, ProcessReadiness, Unit,
};
use weft::needs::{Need, NeedStage, Os, PackageManager, Recipe, DEFAULT_DISTRO};
use weft::node::NodeOutput;
use weft::{node_bail, EndpointHandle, EndpointMethod, InfraProvisionContext, NodeErrExt, RunContext, ValueBag, WeftResult};

// The computer package's own code (`uses = ["computer"]` in package.toml):
// how a command runs on a computer, and how an app started there is reached.
use computer::actions::END_SESSION;
use computer::apps;
use computer::command::{self, Command, Ran};

// SYNC: the agent server's routes <-> catalog/agents/server/agent_server.py (HEALTH_PATH, CONFIGURE_PATH, TURNS_PATH,
//       EVENTS_PATH, HISTORY_PATH, LOGIN_PATH)
pub const HEALTH_PATH: &str = "/health";
const CONFIGURE_PATH: &str = "/configure";
pub const TURNS_PATH: &str = "/turns";
// The two routes read with a query are path segments, for `client::url`.
pub const EVENTS_PATH: &str = "events";
pub const HISTORY_PATH: &str = "history";
const LOGIN_PATH: &str = "/login";

// SYNC: the event an agent asking for a fresh login raises <-> catalog/agents/server/events.py (LOGIN)
pub const LOGIN_EVENT: &str = "login";

// SYNC: the agent server's environment <-> catalog/agents/server/agent_server.py (WEFT_AGENT_PORT)
const PORT_ENV: &str = "WEFT_AGENT_PORT";

// Where the server and its conversation live inside a computer: the home
// folder, which is the computer's disk, kept across `weft infra stop`. The
// agents' own installs are beside them, `<kind>/`, as on the machine they
// are in weft's needs folder under `NEEDS_GROUP`.
// SYNC: the agents' folder <-> NEEDS_GROUP (`~/.local/share/weft/needs/agents` on the machine)
const COMPUTER_AGENTS_DIR: &str = "$HOME/.weft/agents";

/// The group the agents' installs are in under weft's needs folder
/// (`Recipe::folder` is `agents/<kind>`): every agent's, and the runtimes
/// their recipes share (`need_node`).
// SYNC: NEEDS_GROUP <-> COMPUTER_AGENTS_DIR
const NEEDS_GROUP: &str = "agents";

/// What every recipe sources first: the helpers they share.
const INSTALL_LIB: &str = include_str!("server/install_lib.sh");

// The unit, its port and the endpoint every agent node declares.
const UNIT: &str = "server";
const PORT: &str = "http";
pub const ENDPOINT: &str = "api";

/// How long one live read of a turn's events is held open, in seconds:
/// long enough that a busy turn costs few requests, short enough that the
/// node gets to its pause soon after the agent goes quiet.
pub const LIVE_WAIT_SECS: u64 = 10;

/// The agent server's files, as the process unit writes them into its
/// folder and the computer install writes them into the computer.
// SYNC: the server's files <-> catalog/agents/server/ (every file the server runs on: each .py but the tests,
//       agents.json, and the data files an adapter reads; `server/test_adapters.py` checks the two agree)
pub const SERVER_FILES: &[(&str, &str)] = &[
    ("agent_server.py", include_str!("server/agent_server.py")),
    ("events.py", include_str!("server/events.py")),
    ("adapters.py", include_str!("server/adapters.py")),
    ("program_tools.py", include_str!("server/program_tools.py")),
    ("websocket_client.py", include_str!("server/websocket_client.py")),
    ("tether.py", include_str!("server/tether.py")),
    ("adapter_claude.py", include_str!("server/adapter_claude.py")),
    ("adapter_codex.py", include_str!("server/adapter_codex.py")),
    ("adapter_copilot.py", include_str!("server/adapter_copilot.py")),
    ("adapter_kimi.py", include_str!("server/adapter_kimi.py")),
    ("kimi_bridge.mjs", include_str!("server/kimi_bridge.mjs")),
    ("adapter_antigravity.py", include_str!("server/adapter_antigravity.py")),
    ("antigravity_bridge.py", include_str!("server/antigravity_bridge.py")),
    ("adapter_vibe.py", include_str!("server/adapter_vibe.py")),
    ("adapter_cline.py", include_str!("server/adapter_cline.py")),
    ("cline_bridge.mjs", include_str!("server/cline_bridge.mjs")),
    ("adapter_kilo.py", include_str!("server/adapter_kilo.py")),
    ("kilo_weft.mjs", include_str!("server/kilo_weft.mjs")),
    ("adapter_openclaw.py", include_str!("server/adapter_openclaw.py")),
    ("openclaw_weft.mjs", include_str!("server/openclaw_weft.mjs")),
    ("adapter_muse.py", include_str!("server/adapter_muse.py")),
    ("adapter_nanobot.py", include_str!("server/adapter_nanobot.py")),
    ("plugin_nanobot.py", include_str!("server/plugin_nanobot.py")),
    ("adapter_pi.py", include_str!("server/adapter_pi.py")),
    ("pi_weft.ts", include_str!("server/pi_weft.ts")),
    ("weft_tools.mjs", include_str!("server/weft_tools.mjs")),
    ("js_plugin.py", include_str!("server/js_plugin.py")),
    ("adapter_acp.py", include_str!("server/adapter_acp.py")),
    ("acp_codebuddy.py", include_str!("server/acp_codebuddy.py")),
    ("acp_cursor.py", include_str!("server/acp_cursor.py")),
    ("acp_devin.py", include_str!("server/acp_devin.py")),
    ("acp_dsh.py", include_str!("server/acp_dsh.py")),
    ("dsh_weft.mjs", include_str!("server/dsh_weft.mjs")),
    ("acp_gemini.py", include_str!("server/acp_gemini.py")),
    ("acp_goose.py", include_str!("server/acp_goose.py")),
    ("acp_grok.py", include_str!("server/acp_grok.py")),
    ("acp_hermes.py", include_str!("server/acp_hermes.py")),
    ("plugin_hermes.py", include_str!("server/plugin_hermes.py")),
    ("acp_junie.py", include_str!("server/acp_junie.py")),
    ("acp_kiro.py", include_str!("server/acp_kiro.py")),
    ("acp_minimax.py", include_str!("server/acp_minimax.py")),
    ("acp_opencode.py", include_str!("server/acp_opencode.py")),
    ("opencode_weft.mjs", include_str!("server/opencode_weft.mjs")),
    ("acp_qwen.py", include_str!("server/acp_qwen.py")),
    ("agents.json", include_str!("server/agents.json")),
    // The recipes' helpers: what a computer's install sources
    // ([`computer_script`]), as a machine's does ([`INSTALL_LIB`]).
    ("install_lib.sh", include_str!("server/install_lib.sh")),
];

// ===================================================================
// The agents, as `server/agents.json` describes them
// ===================================================================

/// One agent's facts, the ones the weft side needs.
// SYNC: AgentFacts <-> catalog/agents/server/agents.json (the shape of each entry),
//       catalog/agents/server/adapters.py (AGENTS: reads label, check, permissions, logins)
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentFacts {
    /// Its key in agents.json, which names its folder.
    #[serde(skip)]
    pub kind: String,
    pub label: String,
    /// The command that runs, and exits 0, once the agent is installed.
    pub check: Vec<String>,
    /// The bash recipe installing the agent into its own folder, by
    /// system: run after sourcing `server/install_lib.sh`, as the person
    /// (no sudo), into `$WEFT_NEED_DIR`, with `$WEFT_NEEDS_ROOT` holding
    /// every agent's folder (`weft::needs::Recipe::script`).
    pub install: BTreeMap<Os, String>,
    /// The folders, relative to the agent's own, its programs are in.
    pub bin: Vec<String>,
    /// The system packages the recipe needs and cannot install without
    /// sudo, by package manager.
    #[serde(default)]
    pub system: BTreeMap<PackageManager, Vec<String>>,
    /// How a person installs it on their own machine.
    pub install_hint: String,
    /// Each way to log in, by its id on the connection.
    #[serde(default)]
    pub logins: BTreeMap<String, LoginFacts>,
}

/// How one way to log in reaches the agent. Whether the way is offered on
/// a cloud install is the access node's (`localOnly` on its way), never
/// this: one place per fact. Nothing reaches the agent as a file it could
/// renew on its own: a login whose renewal rotates is renewed by weft
/// alone (the way's `renewal` in its access node's recipe), and an agent
/// takes it over its protocol, asking for a fresh one when it is due.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoginFacts {
    /// Connection value name -> the variable it reaches the agent as.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Connection values the adapter hands the agent over its own
    /// protocol (Codex's API key, or its ChatGPT token and account,
    /// through its login request).
    #[serde(default)]
    pub protocol: Vec<String>,
}

impl LoginFacts {
    /// Every connection value this way hands the agent.
    pub fn values(&self) -> impl Iterator<Item = &String> {
        self.env.keys().chain(self.protocol.iter())
    }
}

/// Every agent the server drives, by kind.
pub fn agents() -> &'static BTreeMap<String, AgentFacts> {
    static AGENTS: OnceLock<BTreeMap<String, AgentFacts>> = OnceLock::new();
    AGENTS.get_or_init(|| {
        let mut agents: BTreeMap<String, AgentFacts> =
            serde_json::from_str(include_str!("server/agents.json")).expect("server/agents.json describes every agent in the shape AgentFacts reads");
        for (kind, facts) in &mut agents {
            facts.kind = kind.clone();
        }
        agents
    })
}

/// The facts of agent `kind`.
pub fn facts(kind: &str) -> WeftResult<&'static AgentFacts> {
    agents().get(kind).node_err(format!("the agent server describes no agent '{kind}'"))
}

// ===================================================================
// Provisioning
// ===================================================================

/// The settings every agent node has, which the node reads itself; every
/// other setting is the agent's own option, handed to its adapter as is.
// SYNC: the common settings <-> every agent node's metadata.json (computer, access, folder, program, model, permission, streamBy)
const COMMON_INPUTS: &[&str] = &["computer", "access", "folder", "program", "model", "permission", "streamBy"];

/// The app name the server answers under inside a computer, from the
/// node's place: letters, digits, `-` and `_` only, as the computer takes it.
pub fn app_name(place: &str) -> String {
    let mut name: String = place.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' }).collect();
    name.insert_str(0, "agent-");
    name
}

/// The infra an agent node asks for: a process unit running the server on
/// this machine, or, with a computer wired, nothing of its own and an
/// endpoint inside the computer.
pub fn provision(kind: &str, ctx: &InfraProvisionContext, input: &ValueBag) -> WeftResult<InfraSpec> {
    let facts = facts(kind)?;
    if let Some(computer) = input.opt::<InfraHandle>("computer")? {
        return Ok(InfraSpec {
            endpoints: vec![Endpoint {
                name: ENDPOINT.into(),
                target: EndpointTarget::Within { infra: computer, path: format!("{}/{}", apps::PREFIX, app_name(&ctx.node)) },
                expose: Expose::Project,
            }],
            ..Default::default()
        });
    }
    let mut process = Process::new(vec!["python3".into(), "agent_server.py".into()])
        .with_ports(vec![ProcessPort::new(PORT, PORT_ENV)])
        .with_readiness(ProcessReadiness::http(PORT, HEALTH_PATH))
        .with_needs(vec![python_need(), agent_need(facts, input.opt::<String>("program")?)]);
    for (path, content) in SERVER_FILES {
        process = process.with_file(*path, *content);
    }
    Ok(InfraSpec {
        units: vec![Unit { name: UNIT.into(), process: Some(process.with_env(Vec::<EnvEntry>::new())), ..Default::default() }],
        endpoints: vec![Endpoint {
            name: ENDPOINT.into(),
            target: EndpointTarget::Process { unit: UNIT.into(), port: PORT.into() },
            expose: Expose::Project,
        }],
        ..Default::default()
    })
}

fn python_need() -> Need {
    serde_json::from_value(json!({
        "name": "python3",
        "check": ["python3", "--version"],
        "install": { "apt": { "default": ["python3"] }, "dnf": { "default": ["python3"] }, "apk": { "default": ["python3"] }, "brew": { "default": ["python"] } }
    }))
    .expect("the python3 need is a valid Need")
}

/// The agent's program, met on the machine before the server starts: the
/// one on PATH, else the one weft installed into its folder, else weft
/// installs it there with its recipe (the system packages the recipe
/// needs first, which weft cannot install without sudo, are its
/// `install`). The person's own `program` in its place is theirs alone:
/// checked, never installed.
pub fn agent_need(facts: &AgentFacts, program: Option<String>) -> Need {
    let how = Some(facts.install_hint.clone());
    let need = Need { name: facts.label.clone(), stage: NeedStage::Run, check: facts.check.clone(), install: Default::default(), how, recipe: None };
    if let Some(program) = program {
        let mut check = facts.check.clone();
        check[0] = program;
        return Need { check, ..need };
    }
    let install = facts.system.iter().map(|(manager, packages)| (*manager, [(DEFAULT_DISTRO.to_string(), packages.clone())].into())).collect();
    let recipe = Recipe {
        folder: format!("{NEEDS_GROUP}/{}", facts.kind),
        script: facts.install.clone(),
        bin: facts.bin.clone(),
        prelude: INSTALL_LIB.to_string(),
    };
    Need { install, recipe: Some(recipe), ..need }
}

// ===================================================================
// The node's run
// ===================================================================

/// What the server is configured with, from the node's settings and the
/// login it was handed. `in_computer` decides the folder's default (the
/// computer's home) and tells an adapter whose own sandbox cannot work
/// inside a container.
pub fn configuration(kind: &str, inputs: &Map<String, Value>, credentials: Option<Value>, in_computer: bool, owner: &str) -> WeftResult<Value> {
    let text = |name: &str| inputs.get(name).and_then(Value::as_str).filter(|s| !s.trim().is_empty()).map(str::to_string);
    let folder = match (text("folder"), in_computer) {
        (Some(folder), _) => folder,
        (None, true) => "~".to_string(),
        // The metadata's rule refuses it at the build; this is the floor.
        (None, false) => node_bail!("this agent works on your machine, so it needs a `folder` to work in"),
    };
    let options: Map<String, Value> = inputs
        .iter()
        .filter(|(name, value)| !COMMON_INPUTS.contains(&name.as_str()) && !value.is_null())
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    Ok(json!({
        "agent": kind,
        "folder": folder,
        "model": text("model"),
        "program": text("program"),
        // Both have their default in the node's metadata, the one place
        // a default lives: a body that sees neither was handed no settings.
        "permission": text("permission").node_err("the agent node was handed no `permission`; its metadata gives every agent node one")?,
        "streamBy": text("streamBy").node_err("the agent node was handed no `streamBy`; its metadata gives every agent node one")?,
        "options": options,
        "credentials": credentials,
        "inComputer": in_computer,
        "owner": owner,
    }))
}

/// The login a connection carries, as the server takes it: the way it was
/// made with and the values that way hands the agent.
async fn credentials(ctx: &RunContext, kind: &str) -> WeftResult<Option<Value>> {
    let Some(access) = ctx.inputs.opt::<weft::Access>("access")? else { return Ok(None) };
    Ok(Some(login_of(kind, &ctx.open(&access).await?)?))
}

/// What `opened`, a connection of agent `kind`, hands the agent:
/// `{way, values}`.
fn login_of(kind: &str, opened: &weft::access::OpenedConnection) -> WeftResult<Value> {
    let facts = facts(kind)?;
    let way = opened.way().node_err(format!("the connection on `access` says no way it was made with; reconnect it for {}", facts.label))?.to_string();
    let login = facts.logins.get(&way).node_err(format!("{} has no login way '{way}'; reconnect `access` with one of {}", facts.label,
        facts.logins.keys().cloned().collect::<Vec<_>>().join(", ")))?;
    let mut values = Map::new();
    for name in login.values() {
        values.insert(name.clone(), json!(opened.value(name)?));
    }
    Ok(json!({ "way": way, "values": values }))
}

/// What an agent node's firing keeps across a pause: inside a computer,
/// the computer's answers to the install it started, so a body run again
/// after waiting on a long install does not start it twice. Emptied once
/// the server answers, so a later run of the body that finds the server
/// gone (the computer restarted while the node waited on its bus) starts
/// it again instead of reading back the old answers.
#[derive(Default, Serialize, Deserialize)]
struct AgentRun {
    install: Command,
}

/// Where the node's own server answers. Inside a computer, the server is
/// installed and started there first when it does not answer.
async fn server(ctx: &RunContext, state: &mut weft::Kept<AgentRun>, kind: &str) -> WeftResult<EndpointHandle> {
    let api = ctx.endpoint(ENDPOINT).await?;
    if let Some(computer) = ctx.inputs.opt::<InfraHandle>("computer")? {
        let computer = ctx.endpoint_of(&computer).await?;
        let app = app_name(api.infra_handle().place());
        if !answers(&api).await {
            start_in_computer(ctx, state, &computer, kind, &app).await?;
            if !answers(&api).await {
                node_bail!("the agent server was started in the computer and does not answer; its log is ~/.weft/agents/{app}/server.log in the computer");
            }
            state.install = Command::default();
            state.save().await?;
        }
    }
    Ok(api)
}

async fn answers(api: &EndpointHandle) -> bool {
    api.call(EndpointMethod::Get, HEALTH_PATH, None).await.is_ok_and(|health| is_agent_server_health(&health))
}

// What tells the agent server's health answer from anything else that
// answers `/health` with a 200: after a crash, its port can be taken by
// another program, which must never pass for it (the server would then
// never be started again). `ok` alone is too common; the conversation's
// `epoch` and whether it is `configured` are the server's own (its `agent`
// is null until configured, so it cannot be the mark). Said twice, here
// and in [`start_script`]'s check, which runs in bash in the computer.
// SYNC: the agent server's health answer <-> catalog/agents/server/agent_server.py (do_GET, HEALTH_PATH)
const HEALTH_CHECK_PY: &str = r#"import json, sys
try:
    health = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
ours = isinstance(health, dict) and health.get("ok") is True and isinstance(health.get("epoch"), str) and isinstance(health.get("configured"), bool)
sys.exit(0 if ours else 1)"#;

/// Whether `health` is the agent server's own answer to `/health`.
pub fn is_agent_server_health(health: &Value) -> bool {
    health["ok"] == true && health["epoch"].is_string() && health["configured"].is_boolean()
}

/// The agent's own folder in a computer.
fn computer_need_dir(facts: &AgentFacts) -> String {
    format!("{COMPUTER_AGENTS_DIR}/{}", facts.kind)
}

/// The folders its install puts its programs in, in a computer, joined
/// as a PATH's start.
fn computer_bins(facts: &AgentFacts) -> String {
    let dir = computer_need_dir(facts);
    facts.bin.iter().map(|b| format!("{dir}/{b}")).collect::<Vec<_>>().join(":")
}

/// The PATH line every script run in the computer for this agent starts
/// with once it is installed: the folders its install puts it in, first.
fn path_line(facts: &AgentFacts) -> String {
    format!("export PATH=\"{}:$PATH\"", computer_bins(facts))
}

/// The line writing `content` into `path` in the computer: through a
/// temporary file and a rename, so a server running from the old file
/// never reads one half written.
fn write_line(path: &str, content: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(content);
    format!("echo '{encoded}' | base64 -d > \"{path}.tmp\" && mv \"{path}.tmp\" \"{path}\"")
}

/// The app's start command, which the computer keeps registered beside
/// the app's port file (`<app>.start`): it starts the agent server unless
/// one already answers. Both the install ([`computer_script`]) and the
/// computer, when it boots, run it holding the app's lock (`<app>.lock`),
/// so two of them at once end with one server.
// SYNC: the app's start script <-> catalog/computer/computer/images/computer/computer.py (start_app)
pub fn start_script(facts: &AgentFacts, app: &str) -> String {
    let dir = format!("{COMPUTER_AGENTS_DIR}/{app}");
    let port = format!("{}/{app}", apps::DIR);
    [
        "#!/bin/bash".to_string(),
        "set -e".to_string(),
        path_line(facts),
        // Up: its port file names a port and the agent server's own health
        // answer comes from there (any other program on that port is not it).
        format!(
            "if [ -s \"{port}\" ] && curl -fsS -m 10 \"http://127.0.0.1:$(cat \"{port}\"){HEALTH_PATH}\" | python3 -c '{HEALTH_CHECK_PY}'; then exit 0; fi"
        ),
        format!("rm -f \"{port}\""),
        format!("cd \"{dir}\""),
        format!("nohup setsid {} >> server.log 2>&1 < /dev/null &", server_command(&format!("\"{dir}/state\""), &format!("\"{port}\""))),
        // Back once the server listens: it writes its port down then.
        format!("for _ in $(seq 100); do [ -s \"{port}\" ] && exit 0; sleep 0.1; done"),
        format!("echo \"the agent server did not start; the end of its log, {dir}/server.log:\""),
        "tail -n 20 server.log".to_string(),
        "exit 1".to_string(),
    ]
    .join("\n")
}

/// The agent server's command line in a computer, its home and port file
/// spelled as the shell is to read them: [`start_script`] runs it, and
/// [`remove_script`] finds the server it started by it.
fn server_command(home: &str, port_file: &str) -> String {
    format!("python3 agent_server.py --port 0 --home {home} --app-file {port_file}")
}

/// How long the removal gives the agent server to stop on its own (it
/// stops its agent first, which gets a grace of its own) before it kills
/// what is left, in tenths of a second.
const REMOVE_GRACE_TENTHS: u32 = 150;

/// The app's remove command, which the computer keeps registered beside
/// its start (`<app>.remove`) and runs, holding the app's lock, when weft
/// terminates the node (`DELETE /apps/<app>`, after it deleted the start,
/// so no restart of the computer brings the server back): it asks the
/// agent server to stop (SIGTERM: the server stops its agent, and what the
/// agent started outside the server's process group, such as a daemon,
/// and its sockets with it), kills what is left of the server's process
/// group once the grace ran out, and deletes the app's folder: the
/// server's files and log, and the conversation it keeps (`state/`: its
/// history, the agent's private home, the files weft wrote for it). The
/// agent's own install, `~/.weft/agents/<kind>/`, is every node's of that
/// kind and stays, as does everything outside the app's folder. Safe to
/// run again: a server gone and a folder gone are no failure.
// SYNC: the app's remove script <-> catalog/computer/computer/images/computer/computer.py (remove_app)
pub fn remove_script(app: &str) -> String {
    let dir = format!("{COMPUTER_AGENTS_DIR}/{app}");
    let port = format!("{}/{app}", apps::DIR);
    // The server's own command line, as `ps` shows it once bash expanded
    // it: only this app's server has this home.
    let server = server_command(&format!("{dir}/state"), &port);
    [
        "#!/bin/bash".to_string(),
        "set -e".to_string(),
        format!("server=\"{server}\""),
        "pids=$(pgrep -f -- \"$server\" || true)".to_string(),
        "for pid in $pids; do kill -TERM \"$pid\" 2>/dev/null || true; done".to_string(),
        format!("for _ in $(seq {REMOVE_GRACE_TENTHS}); do pgrep -f -- \"$server\" >/dev/null || break; sleep 0.1; done"),
        // Its whole group, whether it ended within the grace or not: a
        // program it started can outlive it (as a process unit's stop).
        "for pid in $pids; do kill -KILL -- \"-$pid\" 2>/dev/null || true; done".to_string(),
        format!("rm -rf \"{dir}\""),
    ]
    .join("\n")
}

/// The script that puts the agent and its server into a computer and
/// starts the server, idempotent and safe to run twice at once: it holds
/// the app's lock throughout, an agent already installed is not installed
/// again, and a server already up is left alone. It prints nothing but
/// the end of the log of what failed, so its whole output is the reason.
///
/// The agent is installed the way a machine's setup installs it
/// (`weft::needs::setup`), with the same recipe (its Linux one): the
/// computer's own when the check passes, else the one in
/// `~/.weft/agents/<kind>/` once marked installed, else, holding that
/// agent's lock (two nodes on one computer share the folder), the system
/// packages it needs through `sudo apt-get` (the computer's user has
/// sudo), then the recipe into the emptied folder, then the mark.
// SYNC: the install's steps <-> crates/weft-core/src/needs/setup.rs (Need::meet, INSTALLED_MARK, the folder's lock and log)
pub fn computer_script(facts: &AgentFacts, app: &str) -> WeftResult<String> {
    let dir = format!("{COMPUTER_AGENTS_DIR}/{app}");
    let (apps_dir, start_suffix, lock_suffix) = (apps::DIR, apps::START_SUFFIX, apps::LOCK_SUFFIX);
    let start = format!("{apps_dir}/{app}{start_suffix}");
    let mut lines = vec![
        "set -e".to_string(),
        format!("mkdir -p \"{dir}\" \"{apps_dir}\""),
        // A descriptor bash picks, never a fixed number the shell running
        // the command may already use.
        format!("exec {{lock}}> \"{apps_dir}/{app}{lock_suffix}\""),
        "flock \"$lock\"".to_string(),
        // How to remove it, before anything of it is there: a terminate
        // of the node removes even an install that stopped halfway.
        write_line(&format!("{apps_dir}/{app}{}", apps::REMOVE_SUFFIX), &remove_script(app)),
    ];
    for (path, content) in SERVER_FILES {
        lines.push(write_line(&format!("{dir}/{path}"), content));
    }
    lines.push(install_lines(facts, &format!("{dir}/install_lib.sh"))?);
    lines.push(path_line(facts));
    lines.push(write_line(&start, &start_script(facts, app)));
    // The lock stays with this script, never with the server it starts.
    lines.push(format!("bash \"{start}\" {{lock}}>&-"));
    Ok(lines.join("\n"))
}

/// The lines installing the agent of `facts` in a computer, when it is not
/// there already ([`computer_script`]), with its recipe sourcing `lib`.
fn install_lines(facts: &AgentFacts, lib: &str) -> WeftResult<String> {
    let label = &facts.label;
    let recipe = facts.install.get(&Os::Linux).node_err(format!("agents.json has no Linux recipe for {label}, which a computer installs it with"))?;
    let (need, needs, kind) = (computer_need_dir(facts), COMPUTER_AGENTS_DIR, &facts.kind);
    let (log, script, mark) = (format!("{needs}/{kind}.install.log"), format!("{needs}/{kind}.install.sh"), format!("{need}/{}", weft::needs::setup::INSTALLED_MARK));
    let check = facts.check.iter().map(|w| shell_quote(w)).collect::<Vec<_>>().join(" ");
    let in_folder = format!("PATH=\"{}:$PATH\" {check} >/dev/null 2>&1", computer_bins(facts));
    let failed = |what: &str| format!("{{ echo \"{what}; the end of its log, {log}:\"; tail -n 20 \"{log}\"; exit 1; }}");
    let mut lines = vec![
        format!("if ! {check} >/dev/null 2>&1; then"),
        format!("mkdir -p \"{needs}\""),
        format!("exec {{need_lock}}> \"{needs}/{kind}.lock\""),
        "flock \"$need_lock\"".to_string(),
        format!("if ! {{ [ -e \"{mark}\" ] && {in_folder}; }}; then"),
        format!(": > \"{log}\""),
    ];
    if let Some(packages) = facts.system.get(&PackageManager::Apt).filter(|p| !p.is_empty()) {
        let packages = packages.iter().map(|p| shell_quote(p)).collect::<Vec<_>>().join(" ");
        lines.push(format!(
            "lacking=\"\"; for p in {packages}; do dpkg-query -W -f='${{db:Status-Status}}' \"$p\" 2>/dev/null | grep -qx installed || lacking=\"$lacking $p\"; done"
        ));
        lines.push(format!(
            "if [ -n \"$lacking\" ]; then {{ sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends $lacking; }} >> \"{log}\" 2>&1 || {}; fi",
            failed(&format!("installing the system packages$lacking that {label} needs failed"))
        ));
    }
    lines.extend([
        format!("rm -rf \"{need}\" && mkdir -p \"{need}\""),
        write_line(&script, &format!("set -euo pipefail\nsource \"{lib}\"\n{recipe}\n")),
        format!("( cd \"{need}\" && WEFT_NEED_DIR=\"{need}\" WEFT_NEEDS_ROOT=\"{needs}\" bash \"{script}\" < /dev/null >> \"{log}\" 2>&1 ) || {}", failed(&format!("installing {label} failed"))),
        format!("{in_folder} || {}", failed(&format!("{label} was installed and still does not run"))),
        format!("touch \"{mark}.tmp\" && mv \"{mark}.tmp\" \"{mark}\""),
        "fi".to_string(),
        // The agent's lock is this script's alone, never the server's.
        "exec {need_lock}>&-".to_string(),
        "fi".to_string(),
    ]);
    Ok(lines.join("\n"))
}

fn shell_quote(word: &str) -> String {
    if word.chars().all(|c| c.is_ascii_alphanumeric() || "-_./=:".contains(c)) {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

/// Run [`computer_script`] in the computer: a quick one answers at once, an
/// install that takes longer is waited on through the computer's job route
/// without holding a worker, and for as long as it takes (a person waits
/// on it, so it has no deadline). It runs in a session of its own firing,
/// ended once the script is done with it (it ended, or waiting on it
/// failed): a session runs one command at a time, and two runs starting
/// the same agent at once must not refuse each other.
async fn start_in_computer(ctx: &RunContext, state: &mut weft::Kept<AgentRun>, computer: &EndpointHandle, kind: &str, app: &str) -> WeftResult<()> {
    let facts = facts(kind)?;
    let script = computer_script(facts, app)?;
    let session = format!("weft-{app}-{}", ctx.run_id);
    // Every outcome from here ends the session (an exec the computer
    // refused too: ending a session it never made ends nothing) except the
    // run's own control flow (a pause, a cancel, a wait skipped or given
    // up). Nothing stays open in an idle session (computer.py), but one
    // never ended stays on the card's list of idle sessions. After a pause
    // the script still runs in the session and the body run again waits on
    // that same command; after a skipped or given-up wait it may still run
    // too; ending the session would kill it. After a cancel every ctx call
    // ends with the cancelled error at once, so the end could not be sent.
    let outcome = async {
        let payload = json!({ "command": script, "session": session });
        let finished = match command::run(computer, state, |s| &mut s.install, payload).await? {
            Ran::Finished(finished) => finished,
            Ran::Restarted => node_bail!("the computer only restarted the session '{session}' instead of running the install of {}", facts.label),
        };
        if finished.exit_code != 0 {
            let why = command::output_text(computer, &finished).await?;
            node_bail!("putting {} into the computer failed (exit {}):\n{why}", facts.label, finished.exit_code);
        }
        Ok(())
    }
    .await;
    if matches!(&outcome, Err(e) if e.is_run_control()) {
        return outcome;
    }
    let closed = computer.action(END_SESSION, json!({ "session": session })).await;
    if let Some(warning) = after_session(outcome, closed, &session)? {
        ctx.log(weft::context::LogLevel::Warn, warning).await?;
    }
    Ok(())
}

/// What a command run in a session of its own comes to once the session
/// was ended: its own outcome. A session that could not be ended costs
/// nothing in the computer (nothing stays open between commands, it only
/// stays listed on the card), so it never fails a command that worked:
/// it is the warning answered beside it. A command that failed fails with
/// its own error, the session's failure said after it.
pub fn after_session(outcome: WeftResult<()>, closed: WeftResult<Value>, session: &str) -> WeftResult<Option<String>> {
    match (outcome, closed) {
        (Ok(()), Ok(_)) => Ok(None),
        (Err(failed), Ok(_)) => Err(failed),
        // The run's own control flow landing on the end (a cancel between
        // the two calls) is never folded into a failure or a warning.
        (_, Err(control)) if control.is_run_control() => Err(control),
        (Ok(()), Err(e)) => Ok(Some(format!(
            "ending the session '{session}' in the computer failed ({e}); it stays listed among the computer's idle sessions"
        ))),
        (Err(failed), Err(e)) => {
            let why = match failed {
                weft::WeftError::NodeFailed(why) => why,
                other => other.to_string(),
            };
            node_bail!("{why}\n(ending the session '{session}' in the computer failed as well: {e})")
        }
    }
}

/// The body every agent node runs. In a setup, it makes sure the server is
/// up and configured, and emits its `agent` handle (baked). In a real run
/// it does the same, then opens the run's `stream` bus and reads it until a
/// `CloseBus` ends it, which is when the run is done with the agent. Each
/// time the agent asks for a fresh login there (a `login` event, which
/// AskAgent carries onto the bus with the rest), the node hands it one
/// ([`hand_login`]).
pub async fn run(kind: &str, ctx: RunContext) -> WeftResult<()> {
    let mut state = ctx.keep(AgentRun::default()).await?;
    let api = server(&ctx, &mut state, kind).await?;
    let in_computer = ctx.inputs.opt::<InfraHandle>("computer")?.is_some();
    let login = credentials(&ctx, kind).await?;
    let owner = api.infra_handle().to_string();
    let config = configuration(kind, ctx.inputs.object()?, login, in_computer, &owner)?;
    api.call(EndpointMethod::Post, CONFIGURE_PATH, Some(config)).await?;
    let agent = api.infra_handle().clone();
    if ctx.is_setup() {
        return ctx.pulse_downstream(NodeOutput::new().set("agent", agent)).await;
    }
    ctx.pulse_downstream(NodeOutput::new().set("agent", agent)).await?;
    let bus = ctx.open_bus("stream", weft::bus::BusOptions::default(), "agent").await?;
    // Only the agent's asks for a login are for this node. A pause, a
    // cancellation or a failed bus comes back as the error and goes out
    // as is; the bus's end is the end of the node's part of the run.
    let mut logins = bus.cursor()?.with_filter(|entry| entry.as_json().is_some_and(|(kind, _)| kind == LOGIN_EVENT));
    while logins.next(&mut state).await?.is_some() {
        hand_login(&ctx, &api, kind).await?;
    }
    Ok(())
}

/// What the server answers a login handed to it (`POST /login`).
// SYNC: LoginAnswer <-> catalog/agents/server/agent_server.py (Conversation.take_login)
#[derive(Debug, Deserialize)]
struct LoginAnswer {
    /// The agent took it: either it already held it and wanted nothing,
    /// or it was not what the agent held, and the agent holds it now.
    /// `false`: the agent holds exactly that login and asks for another.
    pub taken: bool,
}

/// The agent asked for a fresh login (the one it holds is about to die,
/// or its service refused it). The connection's login as weft holds it now
/// goes first: another node's renewal, or the lazy one an open makes when
/// it is due, may already have replaced what the agent holds. Only when
/// the agent holds exactly that does the node renew the connection
/// (`ctx.renew`): weft renews it once, however many nodes on it ask, and
/// none of them ever renews a token another one still relies on. A renewal
/// that fails is said to the server too, which fails the agent's turn with
/// it, and fails the node.
pub async fn hand_login(ctx: &RunContext, api: &EndpointHandle, kind: &str) -> WeftResult<()> {
    let Some(access) = ctx.inputs.opt::<weft::Access>("access")? else {
        node_bail!(
            "{} asked for a fresh login, and this node has no `access` to renew: it runs on the login of the machine it \
             is on; log it in again there, or give the node an access node",
            facts(kind)?.label
        )
    };
    let hand = async |credentials: Value| -> WeftResult<bool> {
        let answer = api.call(EndpointMethod::Post, LOGIN_PATH, Some(json!({ "credentials": credentials }))).await?;
        let answer: LoginAnswer = serde_json::from_value(answer.clone())
            .map_err(|e| weft::WeftError::NodeFailed(format!("the agent server answered a login with {answer}: {e}")))?;
        Ok(answer.taken)
    };
    if hand(login_of(kind, &ctx.open(&access).await?)?).await? {
        return Ok(());
    }
    let renewed = match ctx.renew(&access).await {
        Ok(renewed) => renewed,
        Err(e) if e.is_run_control() => return Err(e),
        Err(e) => {
            let why = format!("{} needs a fresh login, and weft could not renew its connection: {e}", facts(kind)?.label);
            api.call(EndpointMethod::Post, LOGIN_PATH, Some(json!({ "error": why }))).await?;
            node_bail!("{why}");
        }
    };
    if !hand(login_of(kind, &renewed)?).await? {
        node_bail!(
            "weft renewed the connection on `access`, and {} still holds the same login and asks for another; connect \
             the account again on the access node",
            facts(kind)?.label
        );
    }
    Ok(())
}
