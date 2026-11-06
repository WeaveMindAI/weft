//! ClaudeCode self-tests, and through it the shared shape of every agent
//! node (`agent.rs`): the process unit it asks for on the person's machine,
//! the endpoint inside a computer it asks for instead, the configuration
//! the server is handed, the script that puts the agent into a computer,
//! and a setup that emits only the baked handle.

use base64::Engine;
use serde_json::{json, Map, Value};

use weft::infra::{EndpointTarget, InfraHandle};
use weft::node_test::NODE_UNDER_TEST_ID;
use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::super::agent::{
    after_session, agent_need, agents, app_name, computer_script, configuration, facts, is_agent_server_health, hand_login, remove_script,
    start_script,
    AgentFacts, SERVER_FILES,
};
use super::ClaudeCodeNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_agent_the_server_drives_reads_as_facts", every_agent_reads),
        NodeTest::basic("every_agent_the_server_drives_has_one_node_and_one_access_that_match_it", one_node_per_agent),
        NodeTest::basic("the_configuration_carries_the_settings_and_the_agents_own_options", configures),
        NodeTest::basic("a_folder_is_required_on_the_machine_and_defaults_to_home_in_a_computer", folder),
        NodeTest::basic("the_computer_script_installs_only_when_missing_and_starts_the_server", script),
        NodeTest::basic("the_start_script_leaves_a_server_that_answers_alone", start),
        NodeTest::basic("on_the_machine_the_agent_is_a_need_weft_installs_with_its_recipe", machine_need),
        NodeTest::basic("in_a_computer_two_installs_at_once_run_the_recipe_once_and_mark_it_done", computer_install),
        NodeTest::basic("only_the_agent_servers_own_health_answer_reads_as_it_up", health),
        NodeTest::basic("a_session_is_ended_on_every_path_and_its_failure_said", session_ended),
        NodeTest::basic("two_installs_at_once_end_with_one_server_and_its_start_brings_it_back", one_server),
        NodeTest::basic("an_apps_removal_stops_its_server_and_deletes_its_folder_and_nothing_else", removed),
        NodeTest::basic("another_program_answering_on_the_servers_old_port_does_not_pass_for_it", foreign_port),
        NodeTest::fake("a_login_ask_is_answered_with_what_the_connection_holds_when_the_agent_holds_older", login_held),
        NodeTest::fake("a_login_ask_renews_the_connection_only_when_the_agent_holds_what_it_holds", login_renewed),
        NodeTest::fake("a_renewal_that_fails_is_said_to_the_server_and_fails_the_node", login_refused),
        NodeTest::fake("a_renewed_login_the_agent_still_asks_past_fails_saying_to_reconnect", login_still_refused),
        NodeTest::fake("on_the_machine_it_runs_the_server_as_a_process_unit", process_unit),
        NodeTest::fake("with_a_computer_its_endpoint_is_inside_the_computer", within_computer),
        NodeTest::fake("a_setup_configures_the_server_and_emits_only_the_handle", setup),
        NodeTest::fake("in_a_computer_a_server_that_does_not_answer_is_started_and_the_start_forgotten", started_in_computer),
        NodeTest::fake("in_a_computer_a_failed_install_fails_with_what_it_printed", failed_in_computer),
        NodeTest::fake("in_a_computer_an_install_the_computer_lost_fails_saying_so", lost_in_computer),
        NodeTest::fake("in_a_computer_a_session_that_does_not_end_is_a_warning_beside_the_install_that_worked", session_refused_in_computer),
        NodeTest::fake("in_a_computer_a_running_install_with_no_id_fails_and_its_session_is_ended", unnamed_in_computer),
    ]
}

fn inputs(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn every_agent_reads() -> WeftResult<()> {
    assert!(agents().contains_key("claude") && agents().contains_key("codex"));
    for (kind, agent) in agents() {
        assert_eq!(&agent.kind, kind);
        assert!(!agent.check.is_empty(), "{kind} names a check");
        assert!(agent.install.get(&weft::needs::Os::Linux).is_some_and(|r| !r.trim().is_empty()), "{kind} has a Linux recipe, which a computer installs it with");
        assert!(!agent.bin.is_empty(), "{kind} says where its programs are");
        let need = agent_need(agent, None);
        assert_eq!(need.recipe.as_ref().and_then(|r| r.refusal()), None, "{kind}'s recipe is one weft runs");
    }
    let claude = facts("claude")?;
    assert_eq!(claude.logins["subscription"].env["token"], "CLAUDE_CODE_OAUTH_TOKEN");
    assert!(facts("nobody").is_err());
    Ok(())
}

/// Every agent node of the package: its kind, its metadata and its access
/// node's. Listed here rather than found, since a node knows its kind only
/// in its code; the test below fails for a kind the server drives that no
/// row names, so a new agent cannot be left out.
const AGENT_NODES: &[(&str, &str, &str)] = &[
    (super::super::claude_code::KIND, include_str!("../claude_code/metadata.json"), include_str!("../claude_code_access/metadata.json")),
    (super::super::cline_cli::KIND, include_str!("../cline_cli/metadata.json"), include_str!("../cline_cli_access/metadata.json")),
    (super::super::code_buddy_code::KIND, include_str!("../code_buddy_code/metadata.json"), include_str!("../code_buddy_code_access/metadata.json")),
    (super::super::codex::KIND, include_str!("../codex/metadata.json"), include_str!("../codex_access/metadata.json")),
    (super::super::copilot_cli::KIND, include_str!("../copilot_cli/metadata.json"), include_str!("../copilot_cli_access/metadata.json")),
    (super::super::cursor_cli::KIND, include_str!("../cursor_cli/metadata.json"), include_str!("../cursor_cli_access/metadata.json")),
    (super::super::deep_seek_harness::KIND, include_str!("../deep_seek_harness/metadata.json"), include_str!("../deep_seek_harness_access/metadata.json")),
    (super::super::devin_cli::KIND, include_str!("../devin_cli/metadata.json"), include_str!("../devin_cli_access/metadata.json")),
    (super::super::gemini_cli::KIND, include_str!("../gemini_cli/metadata.json"), include_str!("../gemini_cli_access/metadata.json")),
    (super::super::google_antigravity::KIND, include_str!("../google_antigravity/metadata.json"), include_str!("../google_antigravity_access/metadata.json")),
    (super::super::goose::KIND, include_str!("../goose/metadata.json"), include_str!("../goose_access/metadata.json")),
    (super::super::grok_build::KIND, include_str!("../grok_build/metadata.json"), include_str!("../grok_build_access/metadata.json")),
    (super::super::hermes_agent::KIND, include_str!("../hermes_agent/metadata.json"), include_str!("../hermes_agent_access/metadata.json")),
    (super::super::junie_cli::KIND, include_str!("../junie_cli/metadata.json"), include_str!("../junie_cli_access/metadata.json")),
    (super::super::kilo_code::KIND, include_str!("../kilo_code/metadata.json"), include_str!("../kilo_code_access/metadata.json")),
    (super::super::kimi_code::KIND, include_str!("../kimi_code/metadata.json"), include_str!("../kimi_code_access/metadata.json")),
    (super::super::kiro_cli::KIND, include_str!("../kiro_cli/metadata.json"), include_str!("../kiro_cli_access/metadata.json")),
    (super::super::mini_max_code::KIND, include_str!("../mini_max_code/metadata.json"), include_str!("../mini_max_code_access/metadata.json")),
    (super::super::mistral_vibe::KIND, include_str!("../mistral_vibe/metadata.json"), include_str!("../mistral_vibe_access/metadata.json")),
    (super::super::muse_code::KIND, include_str!("../muse_code/metadata.json"), include_str!("../muse_code_access/metadata.json")),
    (super::super::nanobot::KIND, include_str!("../nanobot/metadata.json"), include_str!("../nanobot_access/metadata.json")),
    (super::super::open_claw::KIND, include_str!("../open_claw/metadata.json"), include_str!("../open_claw_access/metadata.json")),
    (super::super::open_code::KIND, include_str!("../open_code/metadata.json"), include_str!("../open_code_access/metadata.json")),
    (super::super::pi::KIND, include_str!("../pi/metadata.json"), include_str!("../pi_access/metadata.json")),
    (super::super::qwen_code::KIND, include_str!("../qwen_code/metadata.json"), include_str!("../qwen_code_access/metadata.json")),
];

/// Each agent kind is one node, whose permission choices are the ones the
/// adapter takes, and one access node, whose ways are logins the server
/// knows, each collecting exactly the values that login hands the agent.
fn one_node_per_agent() -> WeftResult<()> {
    let described: Value = serde_json::from_str(include_str!("../server/agents.json")).expect("agents.json is JSON");
    let mut kinds: Vec<&str> = AGENT_NODES.iter().map(|(kind, _, _)| *kind).collect();
    kinds.sort();
    let mut served: Vec<&str> = agents().keys().map(String::as_str).collect();
    served.sort();
    assert_eq!(kinds, served, "one agent node per agent the server drives, and no node for an agent it does not");
    for (kind, node, access) in AGENT_NODES {
        let node: Value = serde_json::from_str(node).expect("a node's metadata is JSON");
        let access: Value = serde_json::from_str(access).expect("an access node's metadata is JSON");
        let permission = node["inputs"].as_array().unwrap().iter().find(|i| i["name"] == "permission").expect("a permission setting");
        assert_eq!(permission["widget"]["options"], described[kind]["permissions"], "{kind}'s permission choices are its adapter's");
        assert!(described[kind]["permissions"].as_array().unwrap().contains(&permission["default"]), "{kind}'s default permission is one of them");
        let logins = &facts(kind)?.logins;
        for way in access["service"]["acquisition"]["ways"].as_array().unwrap() {
            let id = way["id"].as_str().unwrap();
            let login = logins.get(id).unwrap_or_else(|| panic!("{kind}'s access offers '{id}', a login the server does not know"));
            // What a connection made this way stores: its fields, or for a
            // way weft renews, the token and what the paste captured.
            let mut stored: Vec<&str> = match way.get("renewal") {
                Some(renewal) => std::iter::once("token")
                    .chain(renewal["paste"]["captures"].as_array().into_iter().flatten().map(|c| c["name"].as_str().unwrap()))
                    .collect(),
                None => way["fields"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect(),
            };
            let mut values: Vec<&str> = login.values().map(String::as_str).collect();
            stored.sort();
            values.sort();
            assert_eq!(stored, values, "{kind}'s '{id}' stores the values that login hands the agent");
        }
    }
    Ok(())
}

fn configures() -> WeftResult<()> {
    let config = configuration(
        "claude",
        &inputs(json!({ "folder": "/w", "model": "opus", "permission": "plan", "streamBy": "token", "effort": "high",
                        "addDirs": ["/x"], "chrome": false, "computer": null, "access": null, "remoteControl": null })),
        Some(json!({ "way": "apiKey", "values": { "key": "k" } })),
        false,
        "owner",
    )?;
    assert_eq!(config["agent"], "claude");
    assert_eq!((config["folder"].clone(), config["model"].clone()), (json!("/w"), json!("opus")));
    assert_eq!((config["permission"].clone(), config["streamBy"].clone()), (json!("plan"), json!("token")));
    assert_eq!(config["options"], json!({ "effort": "high", "addDirs": ["/x"], "chrome": false }), "the agent's own options, nothing of weft's");
    assert_eq!(config["credentials"]["way"], "apiKey");
    let unset = configuration("claude", &inputs(json!({ "folder": "/w", "permission": "ask", "streamBy": "message" })), None, false, "owner")?;
    assert_eq!(unset["model"], Value::Null, "no model: the agent's own default");
    let error = configuration("claude", &inputs(json!({ "folder": "/w", "streamBy": "message" })), None, false, "owner").expect_err("no permission").to_string();
    assert!(error.contains("handed no `permission`"), "{error}");
    Ok(())
}

fn folder() -> WeftResult<()> {
    let settings = json!({ "permission": "ask", "streamBy": "message" });
    let error = configuration("claude", &inputs(settings.clone()), None, false, "o").expect_err("no folder").to_string();
    assert!(error.contains("needs a `folder`"), "{error}");
    assert_eq!(configuration("claude", &inputs(settings), None, true, "o")?["folder"], "~");
    Ok(())
}

/// What `content` is written into the computer as, by the script.
fn written(path: &str, content: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(content);
    format!("echo '{encoded}' | base64 -d > \"{path}.tmp\" && mv \"{path}.tmp\" \"{path}\"")
}

fn script() -> WeftResult<()> {
    let claude = facts("claude")?;
    let script = computer_script(claude, "agent-claude")?;
    let dir = "$HOME/.weft/agents/agent-claude";
    let bins = claude.bin.iter().map(|b| format!("$HOME/.weft/agents/claude/{b}")).collect::<Vec<_>>().join(":");
    let path = script.find(&format!("export PATH=\"{bins}:$PATH\"")).expect("the agent's folders first on the PATH");
    // The lock first, before anything is written, so two at once take turns.
    let locked = script.find("flock \"$lock\"").expect("takes the app's lock");
    assert!(script.contains("exec {lock}> \"$HOME/.weft/apps/agent-claude.lock\""), "{script}");
    for (path, content) in SERVER_FILES {
        let at = script.find(&written(&format!("{dir}/{path}"), content)).unwrap_or_else(|| panic!("writes {path}"));
        assert!(at > locked, "{path} is written under the lock");
    }
    let recipe = format!("set -euo pipefail\nsource \"{dir}/install_lib.sh\"\n{}\n", claude.install[&weft::needs::Os::Linux]);
    let installed = script.find(&written("$HOME/.weft/agents/claude.install.sh", &recipe)).expect("the machine's own recipe, sourcing the helpers");
    let checked = script.find("if ! claude --version >/dev/null 2>&1; then").expect("installs only when missing");
    let marked = script
        .find(&format!("if ! {{ [ -e \"$HOME/.weft/agents/claude/.weft-installed\" ] && PATH=\"{bins}:$PATH\" claude --version >/dev/null 2>&1; }}; then"))
        .expect("an install marked done is not installed again");
    let turn = script.find("exec {need_lock}> \"$HOME/.weft/agents/claude.lock\"").expect("takes the agent's own lock");
    assert!(locked < checked && checked < turn && turn < marked && marked < installed && installed < path, "{script}");
    assert!(script.contains("bash \"$HOME/.weft/agents/claude.install.sh\" < /dev/null >> \"$HOME/.weft/agents/claude.install.log\" 2>&1"), "{script}");
    assert!(script.contains(&written("$HOME/.weft/apps/agent-claude.start", &start_script(claude, "agent-claude"))), "the start is registered");
    let removal = script.find(&written("$HOME/.weft/apps/agent-claude.remove", &remove_script("agent-claude"))).expect("the removal is registered");
    assert!(locked < removal && removal < script.find(&written(&format!("{dir}/agent_server.py"), SERVER_FILES[0].1)).unwrap(),
        "how to remove it is there under the lock before anything of it");
    assert!(script.ends_with("bash \"$HOME/.weft/apps/agent-claude.start\" {lock}>&-"), "the server never holds the lock: {script}");
    assert_eq!(app_name("one.claude"), "agent-one-claude");
    assert_eq!(app_name("a_b"), "agent-a_b");
    Ok(())
}

fn start() -> WeftResult<()> {
    let start = start_script(facts("claude")?, "agent-claude");
    let port = "$HOME/.weft/apps/agent-claude";
    let up = start
        .find(&format!("if [ -s \"{port}\" ] && curl -fsS -m 10 \"http://127.0.0.1:$(cat \"{port}\")/health\" | python3 -c 'import json, sys"))
        .expect("checks for a server up");
    let launched = start
        .find(&format!("nohup setsid python3 agent_server.py --port 0 --home \"$HOME/.weft/agents/agent-claude/state\" --app-file \"{port}\""))
        .expect("starts the server");
    assert!(up < launched, "a server that answers is left alone");
    assert!(start.starts_with("#!/bin/bash\nset -e\nexport PATH=\"$HOME/.weft/agents/claude/bin:$PATH\""), "{start}");
    Ok(())
}

/// On the machine, the agent is a need weft installs itself: into
/// `agents/<kind>` under weft's data folder, with the agent's recipes, its
/// program folders, the helpers every recipe sources, and the system
/// packages a recipe needs first as the packages it names.
fn machine_need() -> WeftResult<()> {
    let claude = facts("claude")?;
    let need = agent_need(claude, None);
    assert_eq!((need.name.as_str(), need.check.clone()), ("Claude Code", claude.check.clone()));
    let recipe = need.recipe.as_ref().expect("weft installs it");
    assert_eq!(recipe.folder, "agents/claude");
    assert_eq!(recipe.script, claude.install);
    assert_eq!(recipe.bin, claude.bin);
    assert_eq!(recipe.prelude, include_str!("../server/install_lib.sh"));
    assert!(need.by_hand().contains("claude.ai/install.sh"), "the agent says how to get it by hand");
    let mut with_system = claude.clone();
    with_system.system = [(weft::needs::PackageManager::Apt, vec!["git".to_string()])].into();
    let need = agent_need(&with_system, None);
    assert_eq!(need.packages_for(weft::needs::PackageManager::Apt, Some("debian_12")), Some(&["git".to_string()][..]));
    Ok(())
}

/// A stand-in agent whose Linux recipe counts its runs in `count`.
fn counted_agent(count: &std::path::Path) -> AgentFacts {
    let mut agent = facts("claude").unwrap().clone();
    agent.kind = "fake".into();
    agent.label = "Fake".into();
    agent.check = vec!["fakeprog".into()];
    agent.bin = vec!["bin".into()];
    agent.system = Default::default();
    let recipe = format!(
        "echo ran >> '{}'\nmkdir -p \"$WEFT_NEED_DIR/bin\"\nprintf '#!/bin/sh\\nexit 0\\n' > \"$WEFT_NEED_DIR/bin/fakeprog\"\nchmod +x \"$WEFT_NEED_DIR/bin/fakeprog\"\n[ \"$WEFT_NEEDS_ROOT\" = \"$HOME/.weft/agents\" ] && [ \"$(weft_os)\" = linux ]",
        count.display()
    );
    agent.install = [(weft::needs::Os::Linux, recipe)].into();
    agent
}

/// Two computer installs of an agent the computer lacks, at once: one runs
/// the recipe (with the helpers sourced, into `~/.weft/agents/<kind>`)
/// while the other waits on the agent's lock, then finds it marked done;
/// a third installs nothing.
fn computer_install() -> WeftResult<()> {
    let home = Home::new("install");
    let count = home.0.join("count");
    let agent = counted_agent(&count);
    let (first, second) = (computer_script(&agent, "agent-a")?, computer_script(&agent, "agent-b")?);
    let both = std::thread::scope(|scope| {
        let one = scope.spawn(|| home.bash(&first));
        let two = scope.spawn(|| home.bash(&second));
        [one.join().unwrap(), two.join().unwrap()]
    });
    for done in &both {
        assert!(done.status.success(), "{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr));
    }
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "ran\n", "the recipe ran once");
    assert!(home.0.join(".weft/agents/fake").join(weft::needs::setup::INSTALLED_MARK).is_file(), "marked done");
    assert!(home.bash(&first).status.success());
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "ran\n", "never installed again");
    let mut broken = counted_agent(&count);
    broken.kind = "broken".into();
    broken.install = [(weft::needs::Os::Linux, "echo 'the mirror is down'; exit 2".to_string())].into();
    let failed = home.bash(&computer_script(&broken, "agent-c")?);
    let said = String::from_utf8_lossy(&failed.stdout);
    assert!(!failed.status.success() && said.starts_with("installing Fake failed; the end of its log,") && said.contains("the mirror is down"), "{said}");
    assert!(!home.0.join(".weft/agents/broken").join(weft::needs::setup::INSTALLED_MARK).exists());
    Ok(())
}

/// What the agent server answers on `/health`, configured or not.
fn our_health() -> Value {
    json!({ "ok": true, "epoch": "a1b2c3d4", "configured": false, "agent": null })
}

fn health() -> WeftResult<()> {
    assert!(is_agent_server_health(&our_health()));
    assert!(is_agent_server_health(&json!({ "ok": true, "epoch": "a1b2c3d4", "configured": true, "agent": "claude" })));
    assert!(!is_agent_server_health(&json!({ "ok": true })), "another program's health answer");
    assert!(!is_agent_server_health(&json!({ "ok": false, "epoch": "a1b2c3d4", "configured": true })));
    Ok(())
}

fn session_ended() -> WeftResult<()> {
    let refused = || Err(weft::node_error("no session 's'"));
    assert_eq!(after_session(Ok(()), Ok(json!({ "ended": true })), "s")?, None);
    let warning = after_session(Ok(()), refused(), "s")?.expect("a warning for a session left listed");
    assert!(warning.contains("ending the session 's' in the computer failed (node failed: no session 's')"), "{warning}");
    let error = after_session(Err(weft::node_error("exit 1: no network")), Ok(json!({})), "s").expect_err("failed").to_string();
    assert_eq!(error, "node failed: exit 1: no network");
    let error = after_session(Err(weft::node_error("exit 1: no network")), refused(), "s").expect_err("both").to_string();
    assert!(error.starts_with("node failed: exit 1: no network\n(ending the session 's'"), "the install's own failure first: {error}");
    assert!(
        matches!(after_session(Err(weft::node_error("exit 1")), Err(weft::WeftError::Cancelled), "s"), Err(weft::WeftError::Cancelled)),
        "a cancel landing on the end stays a cancel"
    );
    Ok(())
}

/// Hands the agent a fresh login the way the node does when its agent
/// asks ([`hand_login`]), for the tests of what it sends and renews.
struct LoginProbe;

impl weft::node::NodeManifest for LoginProbe {
    fn manifest(&self) -> &'static weft::node::NodeMetadata {
        static MANIFEST: std::sync::OnceLock<weft::node::NodeMetadata> = std::sync::OnceLock::new();
        MANIFEST.get_or_init(|| {
            serde_json::from_value(json!({
                "type": "LoginProbe",
                "label": "Login probe",
                "description": "test-only node",
                "inputs": [{ "name": "access", "type": "Access[codex]", "required": true }],
                "outputs": []
            }))
            .expect("the probe's manifest")
        })
    }
}

#[async_trait::async_trait]
impl weft::Node for LoginProbe {
    async fn run(&self, ctx: weft::RunContext) -> WeftResult<()> {
        let api = ctx.endpoint("api").await?;
        hand_login(&ctx, &api, "codex").await
    }
}

/// A Codex connection made with the ChatGPT login, holding `token`, on `rig`.
fn codex_login(rig: &FakeRig, token: &str) {
    rig.declare_endpoint("api", "http://agent.example:9000");
    rig.connection_way("codex", "subscription");
    rig.connection_value("codex", "token", token);
    rig.connection_value("codex", "accountId", "acct-1");
}

/// What each `POST /login` the probe made carried.
fn logins(rig: &FakeRig) -> Vec<Value> {
    rig.endpoint_calls().into_iter().filter(|c| c.path == "/login").map(|c| c.body.expect("a login carries a body")).collect()
}

fn handed(token: &str) -> Value {
    json!({ "credentials": { "way": "subscription", "values": { "token": token, "accountId": "acct-1" } } })
}

/// The agent holds an older login than the connection (another node
/// renewed it, or an open renewed it lazily): the connection's goes over,
/// the agent takes it, and nothing is renewed.
async fn login_held(rig: FakeRig) -> WeftResult<()> {
    codex_login(&rig, "t1");
    rig.connection_renewal("codex", Ok(&[("token", "t2")]));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": true }));
    rig.run(&LoginProbe, json!({ "access": rig.access("codex") })).await.ok()?;
    assert_eq!(logins(&rig), [handed("t1")]);
    assert_eq!(rig.connection_renewals("codex"), 0, "no renewal");
    Ok(())
}

/// The agent holds exactly what the connection holds and asks past it:
/// weft renews the connection once and the renewed login goes over.
async fn login_renewed(rig: FakeRig) -> WeftResult<()> {
    codex_login(&rig, "t1");
    rig.connection_renewal("codex", Ok(&[("token", "t2")]));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": false }));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": true }));
    rig.run(&LoginProbe, json!({ "access": rig.access("codex") })).await.ok()?;
    assert_eq!(logins(&rig), [handed("t1"), handed("t2")]);
    assert_eq!(rig.connection_renewals("codex"), 1);
    Ok(())
}

/// A renewal the provider refuses goes to the server (which fails the
/// agent's turn with it) and fails the node, saying to reconnect.
async fn login_refused(rig: FakeRig) -> WeftResult<()> {
    codex_login(&rig, "t1");
    rig.connection_renewal("codex", Err("the refresh token was revoked"));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": false }));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({}));
    let error = rig.run(&LoginProbe, json!({ "access": rig.access("codex") })).await.result.expect_err("fails").to_string();
    assert!(error.contains("Codex needs a fresh login, and weft could not renew its connection") && error.contains("needs reconnecting"), "{error}");
    let sent = logins(&rig);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1]["error"].as_str().map(|why| error.contains(why)), Some(true), "the server is told what the node fails with: {}", sent[1]);
    Ok(())
}

/// A renewed login the agent still asks past (it holds the same) fails
/// the node instead of asking again for ever.
async fn login_still_refused(rig: FakeRig) -> WeftResult<()> {
    codex_login(&rig, "t1");
    rig.connection_renewal("codex", Ok(&[("token", "t2")]));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": false }));
    rig.answer_endpoint("api", EndpointMethod::Post, "/login", json!({ "taken": false }));
    let error = rig.run(&LoginProbe, json!({ "access": rig.access("codex") })).await.result.expect_err("fails").to_string();
    assert!(error.contains("still holds the same login") && error.contains("connect the account again"), "{error}");
    Ok(())
}

async fn process_unit(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&ClaudeCodeNode, json!({ "folder": "/w" })).await.ok()?;
    let spec = outcome.infra_spec()?;
    let process = spec.units[0].process.as_ref().expect("a process unit");
    assert_eq!(process.command, vec!["python3".to_string(), "agent_server.py".to_string()]);
    for (path, content) in SERVER_FILES {
        assert_eq!(process.files.get(*path).map(String::as_str), Some(*content), "{path} is written beside it");
    }
    assert_eq!(process.ports[0].env, "WEFT_AGENT_PORT");
    let needs: Vec<&str> = process.needs.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(needs, vec!["python3", "Claude Code"]);
    assert_eq!(process.needs[1], agent_need(facts("claude")?, None), "the agent, which weft installs when it is missing");
    let mine = rig.run_provision_infra(&ClaudeCodeNode, json!({ "folder": "/w", "program": "/opt/my/claude" })).await.ok()?;
    let need = &mine.infra_spec()?.units[0].process.as_ref().expect("a process unit").needs[1];
    assert_eq!((need.check[0].as_str(), need.recipe.is_none()), ("/opt/my/claude", true), "the person's own program is checked, never installed");
    assert!(matches!(&spec.endpoints[0].target, EndpointTarget::Process { unit, .. } if unit == "server"));
    Ok(())
}

async fn within_computer(rig: FakeRig) -> WeftResult<()> {
    let computer = rig.declare_infra("box", "api", "http://computer.example:8000");
    let outcome = rig.run_provision_infra(&ClaudeCodeNode, json!({ "computer": computer })).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert!(spec.units.is_empty(), "no unit of its own");
    match &spec.endpoints[0].target {
        EndpointTarget::Within { infra, path } => {
            assert_eq!(infra, &InfraHandle::new("box", "api", None));
            assert!(path.starts_with("/apps/agent-"), "{path}");
        }
        other => panic!("not inside the computer: {other:?}"),
    }
    Ok(())
}

async fn setup(rig: FakeRig) -> WeftResult<()> {
    rig.declare_endpoint("api", "http://agent.example:9000");
    rig.answer_endpoint("api", EndpointMethod::Post, "/configure", json!({ "ok": true, "changed": true }));
    let outcome = rig.run_infra_setup(&ClaudeCodeNode, json!({ "folder": "/w", "permission": "ask", "streamBy": "message" })).await.ok()?;
    assert_eq!(outcome.outputs["agent"], InfraHandle::new(NODE_UNDER_TEST_ID, "api", None).to_value());
    assert!(outcome.outputs.get("stream").is_none(), "nothing reads a bus in a setup");
    let configured = rig.endpoint_calls().into_iter().find(|c| c.path == "/configure").expect("configured").body.unwrap();
    assert_eq!((configured["agent"].clone(), configured["folder"].clone()), (json!("claude"), json!("/w")));
    assert_eq!(configured["credentials"], Value::Null, "the machine's own login");
    Ok(())
}

/// The removal the computer runs for an app (`DELETE /apps/<app>`, after
/// it deleted the app's start), run by bash as the computer runs it: the
/// app's server stops on its SIGTERM and its folder goes, conversation and
/// all, while another app's server and folder and the agent's own install
/// stay; run again, it finds nothing and still succeeds.
fn removed() -> WeftResult<()> {
    let home = Home::with_claude("remove");
    for app in ["agent-r", "agent-s"] {
        let done = home.bash(&computer_script(facts("claude")?, app)?);
        assert!(done.status.success(), "{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr));
    }
    let server_of = |app: &str| {
        let found = std::process::Command::new("pgrep")
            .args(["-f", "--", &format!("--home {}/.weft/agents/{app}/state", home.0.display())])
            .output()
            .expect("pgrep runs");
        String::from_utf8_lossy(&found.stdout).lines().map(str::to_string).collect::<Vec<_>>()
    };
    assert_eq!((server_of("agent-r").len(), server_of("agent-s").len()), (1, 1), "both servers up");
    let remove = std::fs::read_to_string(home.0.join(".weft/apps/agent-r.remove")).expect("registered by the install");
    assert_eq!(remove, remove_script("agent-r"));
    let done = home.bash(&remove);
    assert!(done.status.success(), "{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr));
    assert!(server_of("agent-r").is_empty(), "its server stopped");
    assert!(!home.0.join(".weft/agents/agent-r").exists(), "its folder is gone");
    assert_eq!(server_of("agent-s").len(), 1, "another app's server stays");
    assert!(home.0.join(".weft/agents/agent-s/state").is_dir(), "another app's conversation stays");
    assert!(home.0.join(".weft/agents/claude").join(weft::needs::setup::INSTALLED_MARK).is_file(), "the agent's own install stays");
    assert!(home.bash(&remove).status.success(), "a removal run again finds nothing and succeeds");
    Ok(())
}

/// A home folder for [`one_server`], with a stand-in `claude` on the
/// agent's PATH; every server started from it is stopped when dropped.
struct Home(std::path::PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::process::Command::new("pkill").args(["-f", "--", &format!("--home {}/", self.0.display())]).status();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Home {
    /// A fresh home.
    fn new(name: &str) -> Home {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let home = Home(std::env::temp_dir().join(format!("weft-agent-{name}-{}-{nanos}", std::process::id())));
        std::fs::create_dir_all(&home.0).unwrap();
        home
    }

    /// A fresh home with a stand-in `claude` installed in the agent's
    /// folder, marked done, as an earlier install left it.
    fn with_claude(name: &str) -> Home {
        let home = Home::new(name);
        let folder = home.0.join(".weft/agents/claude");
        let bin = folder.join(&facts("claude").unwrap().bin[0]);
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("claude"), "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(bin.join("claude"), std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        std::fs::write(folder.join(weft::needs::setup::INSTALLED_MARK), "").unwrap();
        home
    }

    /// `command` run by bash with this home, on its standard input: the
    /// script, with every file in it, is longer than one argument may be.
    fn bash(&self, command: &str) -> std::process::Output {
        let mut bash = std::process::Command::new("bash")
            .env("HOME", &self.0)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("bash runs");
        std::io::Write::write_all(&mut bash.stdin.take().unwrap(), command.as_bytes()).expect("bash reads the script");
        bash.wait_with_output().expect("bash ends")
    }
}

/// The agent servers running from `home`, by pid.
fn servers(home: &Home) -> Vec<String> {
    let found = std::process::Command::new("pgrep").args(["-f", "--", &format!("--home {}/", home.0.display())]).output().expect("pgrep runs");
    String::from_utf8_lossy(&found.stdout).lines().map(str::to_string).collect()
}

/// The real scripts, run by bash as the computer runs them (no install:
/// the stand-in `claude` in the agent's folder answers its check): two at once take turns on
/// the lock and end with one server, a third leaves it alone, and once it
/// is gone, the start the computer runs at boot brings it back.
fn one_server() -> WeftResult<()> {
    let home = Home::with_claude("start");
    let script = computer_script(facts("claude")?, "agent-t")?;
    let run = |command: &str| home.bash(command);
    let both = std::thread::scope(|scope| {
        let first = scope.spawn(|| run(&script));
        let second = scope.spawn(|| run(&script));
        [first.join().unwrap(), second.join().unwrap()]
    });
    for done in &both {
        assert!(done.status.success(), "{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr));
        assert!(done.stdout.is_empty(), "a script that worked prints nothing: {}", String::from_utf8_lossy(&done.stdout));
    }
    let up = servers(&home);
    assert_eq!(up.len(), 1, "two at once start one server: {up:?}");
    assert!(run(&script).status.success());
    assert_eq!(servers(&home), up, "a server that answers is left alone");
    std::process::Command::new("kill").args(&up).status().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !servers(&home).is_empty() {
        assert!(std::time::Instant::now() < deadline, "the server did not stop on SIGTERM");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let start = home.0.join(".weft/apps/agent-t.start");
    let again = run(&format!("bash {}", start.display()));
    assert!(again.status.success(), "{}", String::from_utf8_lossy(&again.stdout));
    assert_eq!(servers(&home).len(), 1, "the registered start brings a gone server back");
    Ok(())
}

/// A program that is not the agent server, answering `/health` with a 200
/// on a loopback port; killed when dropped.
struct Foreign(std::process::Child);

impl Drop for Foreign {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A crash left the port file naming a port another program has since
/// taken, and it answers `/health` with a 200 of its own: the install
/// starts the agent server anyway, on a port of its own.
fn foreign_port() -> WeftResult<()> {
    let home = Home::with_claude("foreign");
    let apps = home.0.join(".weft/apps");
    std::fs::create_dir_all(&apps).unwrap();
    let serve = r#"import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
s = http.server.HTTPServer(("127.0.0.1", 0), H)
print(s.server_address[1], flush=True)
s.serve_forever()"#;
    let mut child = std::process::Command::new("python3").args(["-c", serve]).stdout(std::process::Stdio::piped()).spawn().expect("python3 runs");
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.take().unwrap()), &mut line).expect("the port");
    let _foreign = Foreign(child);
    let taken = line.trim().to_string();
    std::fs::write(apps.join("agent-f"), &taken).unwrap();
    let done = home.bash(&computer_script(facts("claude")?, "agent-f")?);
    assert!(done.status.success(), "{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr));
    assert_eq!(servers(&home).len(), 1, "the agent server was started");
    let port = std::fs::read_to_string(apps.join("agent-f")).unwrap();
    assert_ne!(port.trim(), taken, "on a port of its own, the other program left alone");
    Ok(())
}

const COMPUTER: &str = "http://computer.example:8000";

/// An agent node inside a computer whose server does not answer yet, its
/// settings, and the computer's answer to the install's exec queued.
fn in_computer(rig: &FakeRig, exec: Value) -> Value {
    let computer = rig.declare_infra("box", "api", COMPUTER);
    rig.declare_endpoint("api", &format!("{COMPUTER}/apps/agent-x"));
    rig.refuse_endpoint("api", EndpointMethod::Get, "/health", 502, "{\"error\": \"no app\"}");
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": exec }));
    json!({ "computer": computer, "permission": "ask", "streamBy": "message" })
}

/// The actions the node asked the computer for, as (action, payload).
fn asked(rig: &FakeRig) -> Vec<(String, Value)> {
    rig.endpoint_calls()
        .into_iter()
        .filter(|call| call.path == "/action")
        .map(|call| {
            let body = call.body.expect("an action carries a body");
            (body["action"].as_str().unwrap().to_string(), body["payload"].clone())
        })
        .collect()
}

async fn started_in_computer(rig: FakeRig) -> WeftResult<()> {
    let input = in_computer(&rig, json!({ "status": "done", "id": "j1", "exitCode": 0, "size": 0 }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "ended": true } }));
    rig.answer_endpoint("api", EndpointMethod::Get, "/health", our_health());
    // A real run, stopped right after the start (a run's body would go on
    // to wait on its bus), so what it saved can be read back.
    rig.refuse_endpoint("api", EndpointMethod::Post, "/configure", 500, "{\"error\": \"stop here\"}");
    rig.run(&ClaudeCodeNode, input).await.failure()?;
    let asked = asked(&rig);
    let actions: Vec<&str> = asked.iter().map(|(action, _)| action.as_str()).collect();
    assert_eq!(actions, vec!["exec", "end_session"], "the install, then its session ended");
    let exec = &asked[0].1;
    assert!(exec.get("timeoutSecs").is_none(), "a person waits on the install, so it has no deadline: {exec}");
    let session = exec["session"].as_str().unwrap();
    assert!(session.starts_with("weft-agent-") && session.len() > "weft-agent-".len() + 30, "a session of the firing's own: {session}");
    assert_eq!(asked[1].1, json!({ "session": session }));
    // Forgotten once the server answers: a later run of the body that
    // finds it gone starts it again.
    assert_eq!(rig.saved_state(&ClaudeCodeNode), Some(json!({ "install": { "started": null, "ended": null } })));
    Ok(())
}

async fn failed_in_computer(rig: FakeRig) -> WeftResult<()> {
    let said = "installing Claude Code failed; the end of its log:\nno network\n";
    let input = in_computer(&rig, json!({ "status": "done", "id": "j1", "exitCode": 1, "size": said.len() }));
    let encoded = base64::engine::general_purpose::STANDARD.encode(said);
    let (first, rest) = encoded.split_at(8);
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "data": first, "next": 6, "end": false } }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "data": rest, "next": said.len(), "end": true } }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "ended": true } }));
    let failure = rig.run_infra_setup(&ClaudeCodeNode, input).await.failure()?;
    assert!(failure.contains("failed (exit 1):\ninstalling Claude Code failed; the end of its log:\nno network"), "every piece of what it printed: {failure}");
    let reads: Vec<Value> = asked(&rig).into_iter().filter(|(action, _)| action == "output").map(|(_, p)| p["from"].clone()).collect();
    assert_eq!(reads, vec![json!(0), json!(6)]);
    Ok(())
}

async fn lost_in_computer(rig: FakeRig) -> WeftResult<()> {
    let input = in_computer(&rig, json!({ "status": "running", "id": "j1" }));
    rig.signal(json!({ "status": "lost", "error": "no command 'j1' on this computer: it restarted since the command began" }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "ended": true } }));
    let failure = rig.run_infra_setup(&ClaudeCodeNode, input).await.failure()?;
    assert!(failure.contains("it restarted since the command began"), "{failure}");
    let awaited = rig.awaited_signals();
    assert_eq!(awaited[0].config["url"], json!(format!("{COMPUTER}/jobs/j1")));
    let actions: Vec<String> = asked(&rig).into_iter().map(|(action, _)| action).collect();
    assert_eq!(actions, vec!["exec", "end_session"], "the session is ended on a failure too");
    Ok(())
}

async fn session_refused_in_computer(rig: FakeRig) -> WeftResult<()> {
    let input = in_computer(&rig, json!({ "status": "done", "id": "j1", "exitCode": 0, "size": 0 }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "the computer could not end it" } }));
    rig.answer_endpoint("api", EndpointMethod::Get, "/health", our_health());
    // Stopped right after the start, as in `started_in_computer`.
    rig.refuse_endpoint("api", EndpointMethod::Post, "/configure", 500, "{\"error\": \"stop here\"}");
    let failure = rig.run(&ClaudeCodeNode, input).await.failure()?;
    assert!(failure.contains("stop here"), "the install worked, so the run went on: {failure}");
    let logs = rig.logs();
    assert!(
        logs.iter().any(|(level, line)| *level == weft::context::LogLevel::Warn && line.contains("ending the session 'weft-agent-") && line.contains("the computer could not end it")),
        "{logs:?}"
    );
    Ok(())
}

async fn unnamed_in_computer(rig: FakeRig) -> WeftResult<()> {
    let input = in_computer(&rig, json!({ "status": "running" }));
    rig.answer_infra("box", "api", EndpointMethod::Post, "/action", json!({ "result": { "ended": true } }));
    let failure = rig.run_infra_setup(&ClaudeCodeNode, input).await.failure()?;
    assert!(failure.contains("answered a running command with no id"), "{failure}");
    let actions: Vec<String> = asked(&rig).into_iter().map(|(action, _)| action).collect();
    assert_eq!(actions, vec!["exec", "end_session"], "a failed wait ends the session too");
    assert!(rig.awaited_signals().is_empty(), "nothing to wait on");
    Ok(())
}
