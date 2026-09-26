//! Programs with members: one program serving many people, each with a
//! copy of what is theirs. A node marked `@per_member` exists once per
//! member, every run is for one member or for nobody, and each member
//! reaches their own copies, connections and files through the doors
//! that name them (`--member`, a member token, the `Weft-Member` header
//! on a gated route).
#![cfg(feature = "e2e")]

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};
use uuid::Uuid;
use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, infra, live, poll_until, project::Project, Dispatcher, SettledRun};

/// A copy coming up builds and pulls an image and waits for its
/// readiness probe, the same bound the rig gives any infra node.
const COPY_DEADLINE: Duration = Duration::from_secs(300);
const POLL: Duration = Duration::from_millis(750);

/// The status of `member`'s copy of `node`, `None` when there is none.
async fn copy_status(disp: &Dispatcher, project: Uuid, node: &str, member: &str) -> anyhow::Result<Option<String>> {
    Ok(infra::status(disp, &project)
        .await?
        .into_iter()
        .find(|n| n.node() == Some(node) && n.0.get("member").and_then(Value::as_str) == Some(member))
        .and_then(|n| n.status().map(str::to_string)))
}

/// Wait until `member`'s copy of `node` is `want` (`None`: gone).
async fn wait_copy(disp: &Dispatcher, project: Uuid, node: &str, member: &str, want: Option<&str>) -> anyhow::Result<()> {
    poll_until(&format!("member '{member}''s copy of '{node}' to be {want:?}"), COPY_DEADLINE, POLL, || async {
        let now = copy_status(disp, project, node, member).await?;
        anyhow::ensure!(now.as_deref() != Some("failed"), "member '{member}''s copy of '{node}' failed");
        Ok((now.as_deref() == want).then_some(()))
    })
    .await
}

/// The colors of the project's fired runs for `member` (its infra
/// setups are runs for the member too, and are left out).
async fn runs_for(disp: &Dispatcher, project: Uuid, member: &str) -> anyhow::Result<Vec<String>> {
    let listed: Value = disp.get_json(&format!("/executions?project_id={project}&member={member}&limit=100")).await?;
    Ok(listed["executions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter(|row| row["phase"] == json!("fire"))
        .filter_map(|row| row["color"].as_str().map(str::to_string))
        .collect())
}

/// One call at the member door with `token`, whatever it answers.
async fn member_door(
    disp: &Dispatcher,
    method: Method,
    path: &str,
    token: &str,
    body: Option<&Value>,
) -> anyhow::Result<(reqwest::StatusCode, String)> {
    let mut req = reqwest::Client::new()
        .request(method, format!("{}/member/{path}", disp.base()))
        .bearer_auth(token);
    if let Some(body) = body {
        req = req.json(body);
    }
    let resp = req.send().await?;
    Ok((resp.status(), resp.text().await?))
}

/// The token a `weft token mint --json` printed.
fn minted(stdout: &str) -> anyhow::Result<String> {
    let v: Value = serde_json::from_str(stdout.trim())?;
    v["token"].as_str().map(str::to_string).ok_or_else(|| anyhow::anyhow!("no token in {stdout}"))
}

/// The color a `weft run --json` started.
fn color_of(stdout: &str) -> anyhow::Result<Uuid> {
    for line in stdout.lines() {
        if let Ok(ev) = serde_json::from_str::<Value>(line.trim()) {
            if let Some(color) = ev["detail"]["color"].as_str() {
                return Ok(color.parse()?);
            }
        }
    }
    anyhow::bail!("no color in {stdout}")
}

/// A node marked `@per_member` exists once per member: a run must say
/// whose copy it reads, a member's copy is started and taken down on its
/// own, and a run for a member reads that member's copy. A plain step
/// cannot be marked.
#[tokio::test]
async fn a_per_member_infra_node_runs_one_copy_per_member() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("members", disp.clone()).await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    let pid = project.id();

    project.set_main("svc = MiniService\nout = Debug {\n  @per_member\n}\nout.data = svc.status\n")?;
    let refused = project.weft_refused(&["run"]).await?;
    anyhow::ensure!(refused.contains("is marked `@per_member`, but only an infra node"), "{refused}");

    project.set_main("svc = MiniService {\n  @per_member\n}\nout = Debug\nout.data = svc.status\n")?;
    let refused = project.weft_refused(&["run"]).await?;
    anyhow::ensure!(refused.contains("once per member, so this run needs to know which member it is for"), "{refused}");
    let refused = project.weft_refused(&["run", "--member", "ada"]).await?;
    anyhow::ensure!(refused.contains("svc (member 'ada')") && refused.contains("--member ada"), "{refused}");

    project.weft(&["infra", "start", "--member", "ada"]).await?;
    wait_copy(&disp, pid, "svc", "ada", Some("running")).await?;
    anyhow::ensure!(copy_status(&disp, pid, "svc", "bob").await?.is_none(), "bob has no copy");
    let status = project.weft(&["status"]).await?;
    anyhow::ensure!(status.contains("member copies:") && status.contains("ada"), "{status}");

    let color = color_of(&project.weft(&["run", "--json", "--member", "ada"]).await?)?;
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("ready"))?;
    anyhow::ensure!(runs_for(&disp, pid, "ada").await? == vec![color.to_string()], "the run is ada's");
    anyhow::ensure!(runs_for(&disp, pid, "bob").await?.is_empty());
    let refused = project.weft_refused(&["run", "--member", "bob"]).await?;
    anyhow::ensure!(refused.contains("svc (member 'bob')"), "bob's copy is not ada's: {refused}");

    project.weft(&["clean", "--project", &pid.to_string(), "--member", "ada", "--yes"]).await?;
    anyhow::ensure!(runs_for(&disp, pid, "ada").await?.is_empty(), "clean took ada's runs");

    project.weft(&["infra", "node-terminate", "svc", "--member", "ada"]).await?;
    wait_copy(&disp, pid, "svc", "ada", None).await?;
    project.finish().await
}

/// Each trigger is turned on and off on its own: activating one leaves
/// the others dark, and taking one down leaves the others listening.
#[tokio::test]
async fn one_trigger_is_turned_on_and_off_alone() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("members", disp.clone()).await?;
    project.set_main(
        "a = Route -> (text: String) { path: \"__E2E_PATH__/a\", method: \"POST\" }\n\
         ra = Reply\nra.body = a.text\n\
         b = Route -> (text: String) { path: \"__E2E_PATH__/b\", method: \"POST\" }\n\
         rb = Reply\nrb.body = b.text\n",
    )?;
    let base = project.unique_live_path()?;
    let call = |route: &'static str| {
        let disp = disp.clone();
        let url = format!("{base}/{route}");
        async move { live::http_json(&disp, Method::POST, &url, &[], &json!({ "text": route })).await.map(|(s, _, _)| s) }
    };

    project.weft(&["activate", "--trigger", "a"]).await?;
    anyhow::ensure!(call("a").await? == 200);
    anyhow::ensure!(call("b").await? != 200, "b was never turned on");
    let status = project.weft(&["status"]).await?;
    anyhow::ensure!(status.contains("triggers:"), "{status}");

    project.weft(&["activate", "--trigger", "b"]).await?;
    anyhow::ensure!(call("b").await? == 200);
    project.weft(&["deactivate", "--trigger", "a", "--mode", "wipe", "--running-policy", "cancel"]).await?;
    anyhow::ensure!(call("a").await? != 200, "a is down");
    anyhow::ensure!(call("b").await? == 200, "b kept listening");
    project.finish().await
}

/// The doors that name a member: the `Weft-Member` header on a route
/// gated by a connection, and a member token. A member connects their own
/// account and gives it for a `@member_filled` connection field through
/// the member door, a run for them gets exactly that connection and their
/// own files, and the doors refuse what cannot be trusted.
#[tokio::test]
async fn a_member_picks_their_connection_and_runs_as_themselves() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("members", disp.clone()).await?;
    let base = project.unique_live_path()?;
    let keys = connect_direct(&disp, catalog_spec("api", "api_key_auth")?, "own", json!({ "keys": "k-backend" })).await?;
    set_account(&project, "keys", "account", keys.handle())?;
    project.activate().await?;

    let hook = format!("{base}/hook");
    let call = |headers: Vec<(&'static str, String)>| {
        let disp = disp.clone();
        let hook = hook.clone();
        async move {
            let mut all: Vec<(&str, &str)> = vec![("x-api-key", "k-backend")];
            all.extend(headers.iter().map(|(k, v)| (*k, v.as_str())));
            let (status, _, body) = live::http_json(&disp, Method::POST, &hook, &all, &json!({ "text": "hi" })).await?;
            anyhow::Ok((status, String::from_utf8_lossy(&body).to_string()))
        }
    };

    let (status, body) = call(vec![]).await?;
    anyhow::ensure!(status == 422 && body.contains("once per member, so this run needs to know"), "{status}: {body}");
    let (status, body) = call(vec![("Weft-Member", "ada".into())]).await?;
    anyhow::ensure!(status != 200 && body.contains("member 'ada' has not filled 'theirs.account'"), "{status}: {body}");

    // Ada connects her own key set through the member door, and gives it
    // as her value for `theirs.account`.
    let token = minted(&project.weft(&["token", "mint", "--member", "ada", "--expires", "1h", "--json"]).await?)?;
    let (status, fields) = member_door(&disp, Method::GET, "fields", &token, None).await?;
    anyhow::ensure!(status == 200, "{status}: {fields}");
    let fields: Value = serde_json::from_str(&fields)?;
    anyhow::ensure!(
        fields.as_array().map(|f| f.len()) == Some(1)
            && fields[0]["step"] == json!("theirs")
            && fields[0]["field"] == json!("account")
            && fields[0]["needed"] == json!(true),
        "{fields}"
    );
    let shared = json!({ "spec": fields[0]["spec"], "door": "shared", "values": {} });
    let (status, body) = member_door(&disp, Method::POST, "connections/direct", &token, Some(&shared)).await?;
    anyhow::ensure!(status == 403 && body.contains("the shared key is the program author's"), "{status}: {body}");
    let own = json!({ "spec": fields[0]["spec"], "door": "own", "values": { "keys": "k-ada" } });
    let (status, body) = member_door(&disp, Method::POST, "connections/direct", &token, Some(&own)).await?;
    anyhow::ensure!(status == 200, "{status}: {body}");
    let grant = serde_json::from_str::<Value>(&body)?["grant"]["id"].as_str().map(str::to_string).expect("a grant id");
    let give = json!({ "set": [{ "step": "theirs", "field": "account", "value": { "id": grant } }] });
    let (status, body) = member_door(&disp, Method::PUT, "values", &token, Some(&give)).await?;
    anyhow::ensure!(status.is_success(), "{status}: {body}");

    // The header on the gated route: the run is ada's, with her key set
    // and her own files.
    let (status, body) = call(vec![("Weft-Member", "ada".into())]).await?;
    anyhow::ensure!(status == 200, "{status}: {body}");
    let answer: Value = serde_json::from_str(&body)?;
    anyhow::ensure!(answer["member"] == json!("ada") && answer["key"]["__weft_access__"]["accessId"] == json!(grant) && answer["files"] == json!(1), "{answer}");
    // The token names her too, and her files are still hers.
    let (status, body) = call(vec![("Weft-Member-Token", token.clone())]).await?;
    anyhow::ensure!(status == 200, "{status}: {body}");
    let answer: Value = serde_json::from_str(&body)?;
    anyhow::ensure!(answer["member"] == json!("ada") && answer["files"] == json!(2), "{answer}");
    let (status, body) = call(vec![("Weft-Member-Token", token.clone()), ("Weft-Member", "bob".into())]).await?;
    anyhow::ensure!(status == 400 && body.contains("names member 'bob'"), "{status}: {body}");
    let (status, runs) = member_door(&disp, Method::GET, "runs", &token, None).await?;
    anyhow::ensure!(status == 200 && serde_json::from_str::<Value>(&runs)?["executions"].as_array().map(Vec::len) == Some(2), "{runs}");

    // The open route and the bare address never take a member.
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/open"), &[("Weft-Member", "ada")], &json!({ "text": "x" })).await?;
    let body = String::from_utf8_lossy(&body);
    anyhow::ensure!(status == 400 && body.contains("honoured only on a route gated by a connection"), "{status}: {body}");
    let (status, bare) = reqwest::Client::new()
        .post(format!("{}/{base}/open", disp.base()))
        .header("Weft-Member", "ada")
        .json(&json!({ "text": "x" }))
        .send()
        .await
        .map(|r| (r.status(), r))?;
    let bare = bare.text().await?;
    anyhow::ensure!(status == 400 && bare.contains("a bare fire runs for no member"), "{status}: {bare}");

    // A token that is not a member's never passes the member door.
    let plain = minted(&project.weft(&["token", "mint", "--json"]).await?)?;
    let (status, body) = member_door(&disp, Method::GET, "fields", &plain, None).await?;
    anyhow::ensure!(status == 403 && body.contains("this door takes a member token"), "{status}: {body}");

    // Clearing her value puts the refusal back.
    let clear = json!({ "clear": [{ "step": "theirs", "field": "account" }] });
    let (status, body) = member_door(&disp, Method::PUT, "values", &token, Some(&clear)).await?;
    anyhow::ensure!(status.is_success(), "{status}: {body}");
    let (status, body) = call(vec![("Weft-Member", "ada".into())]).await?;
    anyhow::ensure!(status != 200 && body.contains("has not filled 'theirs.account'"), "{status}: {body}");

    // The terminal does what a member's connect page does: it picks
    // ada's stored key set back, connects bob one of his own, and drops
    // a pick, each for that member alone.
    project.weft(&["connect", "--member", "ada", "--node", "theirs", "--grant", &grant, "--json"]).await?;
    let (status, body) = call(vec![("Weft-Member", "ada".into())]).await?;
    anyhow::ensure!(status == 200, "ada's pick is back: {status}: {body}");
    project.weft(&["connect", "--member", "bob", "--node", "theirs", "--door", "own", "--set", "keys=k-bob"]).await?;
    let (status, body) = call(vec![("Weft-Member", "bob".into())]).await?;
    anyhow::ensure!(status == 200 && serde_json::from_str::<Value>(&body)?["member"] == json!("bob"), "bob runs with his own: {status}: {body}");
    project.weft(&["connect", "--member", "ada", "--node", "theirs", "--disconnect"]).await?;
    let (status, body) = call(vec![("Weft-Member", "ada".into())]).await?;
    anyhow::ensure!(status != 200 && body.contains("has not filled 'theirs.account'"), "{status}: {body}");
    let (status, _) = call(vec![("Weft-Member", "bob".into())]).await?;
    anyhow::ensure!(status == 200, "bob's pick is his own: {status}");

    // The author's own connection as the fallback: a member who connected
    // nothing runs on it, one who did keeps theirs. Only the source can
    // say so; no member can pick the author's connection themselves.
    project.set_node_config("theirs", "account", &format!("@member_filled({})", keys.handle()))?;
    project.weft(&["resync", "--mode", "wipe"]).await?;
    let (status, body) = call(vec![("Weft-Member", "carol".into())]).await?;
    anyhow::ensure!(status == 200, "carol falls back on the author's key set: {status}: {body}");
    let answer: Value = serde_json::from_str(&body)?;
    anyhow::ensure!(answer["key"]["__weft_access__"]["accessId"] == keys.handle()["id"], "{answer}");
    let (status, body) = call(vec![("Weft-Member", "bob".into())]).await?;
    let answer: Value = serde_json::from_str(&body)?;
    anyhow::ensure!(status == 200 && answer["key"]["__weft_access__"]["accessId"] != keys.handle()["id"], "bob keeps his own: {answer}");

    keys.finish().await?;
    project.finish().await
}

/// A program manages its members itself, through the member nodes: it
/// starts a member's copy, watches it come up, mints the member a token,
/// and wipes the member, taking their copy, token and runs.
#[tokio::test]
async fn a_program_manages_its_members_through_the_member_nodes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("members", disp.clone()).await?;
    project.add_node_from_fixture("infra_min", "mini_service")?;
    let pid = project.id();
    project.set_main(
        r#"svc = MiniService {
  @per_member
}
out = Debug
out.data = svc.status

up = Route -> (member: String) { path: "__E2E_PATH__/up", method: "POST" }
start = StartMemberInfra { node: "svc" }
start.member = up.member
upReply = Reply
upReply.body = start.done

look = Route -> (member: String) { path: "__E2E_PATH__/look", method: "POST" }
state = MemberInfraStatus { node: "svc" }
state.member = look.member
lookReply = Reply
lookReply.body = state.status

mint = Route -> (member: String) { path: "__E2E_PATH__/mint", method: "POST" }
token = MintMemberToken { expiresInHours: 1 }
token.member = mint.member
mintReply = Reply
mintReply.body = token.token

wipe = Route -> (member: String) { path: "__E2E_PATH__/wipe", method: "POST" }
gone = WipeMember { infra: ["svc"] }
gone.member = wipe.member
wipeReply = Reply
wipeReply.body = gone.done
"#,
    )?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let ask = |route: &'static str| {
        let disp = disp.clone();
        let url = format!("{base}/{route}");
        async move {
            let (status, _, body) = live::http_json(&disp, Method::POST, &url, &[], &json!({ "member": "ada" })).await?;
            let body = String::from_utf8_lossy(&body).to_string();
            anyhow::ensure!(status == 200, "{route}: {status}: {body}");
            anyhow::Ok(serde_json::from_str::<Value>(&body)?)
        }
    };

    // `StartMemberInfra` answers once the copy runs. Ada's copy is the
    // program's first, so the project's workers move next to it: the run
    // asking is on the worker being replaced, and its setup lands on the
    // new one while it waits, so the wait ends instead of holding the
    // replacement up.
    anyhow::ensure!(ask("up").await? == json!(true));
    anyhow::ensure!(
        copy_status(&disp, pid, "svc", "ada").await?.as_deref() == Some("running"),
        "`done` fired before ada's copy ran"
    );
    // A call pinned to the worker being replaced is told so (503) and
    // asks again, as any caller does.
    let look = format!("{base}/look");
    poll_until("the program to see ada's copy running", COPY_DEADLINE, POLL, || async {
        let (status, _, body) = live::http_json(&disp, Method::POST, &look, &[], &json!({ "member": "ada" })).await?;
        if status == 503 {
            return Ok(None);
        }
        anyhow::ensure!(status == 200, "look: {status}: {}", String::from_utf8_lossy(&body));
        Ok((serde_json::from_slice::<Value>(&body)? == json!("running")).then_some(()))
    })
    .await?;
    let color = color_of(&project.weft(&["run", "--json", "--member", "ada"]).await?)?;
    SettledRun::observe(&disp, color).await?.completed()?.assert_input("out", "data", &json!("ready"))?;

    let token = ask("mint").await?.as_str().map(str::to_string).expect("a token");
    let (status, body) = member_door(&disp, Method::GET, "fields", &token, None).await?;
    anyhow::ensure!(status == 200, "the minted token is ada's: {status}: {body}");

    anyhow::ensure!(ask("wipe").await? == json!(true));
    wait_copy(&disp, pid, "svc", "ada", None).await?;
    let (status, _) = member_door(&disp, Method::GET, "fields", &token, None).await?;
    anyhow::ensure!(status == 401, "ada's token went with her: {status}");
    poll_until("ada's runs to be cleaned", COPY_DEADLINE, POLL, || async {
        Ok(runs_for(&disp, pid, "ada").await?.is_empty().then_some(()))
    })
    .await?;
    project.finish().await
}

/// A value each member gives, beyond a connection: a member's schedule.
/// Their trigger runs on their value (or the written fallback until they
/// give one), a value the node's rules refuse is refused when it is
/// given, and giving a new one while their trigger listens sets the
/// trigger up again on it, with no one re-arming by hand.
#[tokio::test]
async fn a_members_value_reaches_their_live_trigger() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("members", disp.clone()).await?;
    project.set_main("tick = Cron { cron: @member_filled(\"0 0 3 * * *\") }\nlog = Debug\nlog.data = tick.scheduledTime\n")?;
    let pid = project.id();

    // No value given yet: the fallback arms ada's trigger.
    project.weft(&["activate", "--trigger", "tick", "--member", "ada"]).await?;
    let token = minted(&project.weft(&["token", "mint", "--member", "ada", "--expires", "1h", "--display", "tick", "--json"]).await?)?;
    let fires = || async {
        weft_e2e::display::as_token(&disp, &token, &pid, "tick").await?.expect_text("Fires")
    };
    anyhow::ensure!(fires().await?.starts_with("0 0 3 * * *"), "the fallback: {}", fires().await?);

    // A value the Cron node's own rule refuses is refused as it is given,
    // naming the rule, and nothing changes.
    let bad = json!({ "set": [{ "step": "tick", "field": "cron", "value": "0 7 * * *" }] });
    let (status, body) = member_door(&disp, Method::PUT, "values", &token, Some(&bad)).await?;
    anyhow::ensure!(status == 422 && body.contains("needs six fields"), "{status}: {body}");
    anyhow::ensure!(fires().await?.starts_with("0 0 3 * * *"), "a refused value changes nothing");

    // A good one re-arms her live trigger on it before the answer.
    let good = json!({ "set": [{ "step": "tick", "field": "cron", "value": "0 0 7 * * *" }] });
    let (status, body) = member_door(&disp, Method::PUT, "values", &token, Some(&good)).await?;
    anyhow::ensure!(status == 200, "{status}: {body}");
    anyhow::ensure!(serde_json::from_str::<Value>(&body)?["rearmed"] == json!(["tick"]), "{body}");
    anyhow::ensure!(fires().await?.starts_with("0 0 7 * * *"), "her trigger fires on her value now: {}", fires().await?);

    // The field is listed with her value, and bob, who gave none, still
    // gets the fallback.
    let (_, fields) = member_door(&disp, Method::GET, "fields", &token, None).await?;
    let fields: Value = serde_json::from_str(&fields)?;
    anyhow::ensure!(fields[0]["value"] == json!("0 0 7 * * *") && fields[0]["fallback"] == json!("0 0 3 * * *"), "{fields}");
    project.weft(&["member-values", "--member", "bob", "--set", "tick.cron=0 30 9 * * *"]).await?;
    let listed = project.weft(&["member-values", "--member", "bob", "--json"]).await?;
    anyhow::ensure!(listed.contains("0 30 9 * * *"), "{listed}");
    project.finish().await
}
