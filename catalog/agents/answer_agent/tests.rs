//! AnswerAgent: the form built from an agent's question, the answer read
//! back from what the person sent, and the run waiting for the person.

use serde_json::{json, Value};

use weft::signal::{FirstOf, Form};
use weft::{FakeRig, NodeTest, WeftResult};

use super::{answer_from, form_for, AgentQuestion, AnswerAgentNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_question_becomes_its_options_and_a_box_for_another_answer", question_form),
        NodeTest::basic("a_permission_becomes_what_it_runs_its_choices_and_a_note", permission_form),
        NodeTest::basic("a_secret_question_is_refused", secret),
        NodeTest::basic("a_question_the_form_could_not_map_back_is_refused", unmappable),
        NodeTest::basic("an_agent_id_never_becomes_a_field_key", ids_are_not_keys),
        NodeTest::basic("what_the_person_sent_reads_as_the_agents_answer", answers),
        NodeTest::basic("beside_several_picks_an_own_answer_is_sent_with_them", own_beside_picks),
        NodeTest::basic("an_unanswered_question_is_refused_before_it_reaches_the_agent", unanswered),
        NodeTest::fake("the_run_waits_for_the_person_and_hands_the_answer_on", waits),
        NodeTest::fake("a_question_the_agent_dropped_ends_the_form_with_a_dropped_answer", dropped),
    ]
}

fn colour() -> Value {
    json!({ "id": "q1", "kind": "question", "permission": null, "statusUrl": "http://agent.test/question?id=q1", "questions": [
        { "id": "0", "header": "Colour", "question": "Red or blue?", "multiSelect": false, "allowOther": true, "secret": false,
          "options": [{ "label": "Red", "description": "warm" }, { "label": "Blue", "description": "cold" }] },
        { "id": "1", "header": "", "question": "Which days?", "multiSelect": true, "allowOther": false, "secret": false,
          "options": [{ "label": "Mon", "description": "" }, { "label": "Tue", "description": "" }] },
        { "id": "2", "header": "", "question": "Anything else?", "multiSelect": false, "allowOther": true, "secret": false, "options": [] }
    ] })
}

fn bash() -> Value {
    json!({ "id": "p1", "kind": "permission", "questions": [], "statusUrl": "http://agent.test/question?id=p1", "permission": {
        "tool": "Bash", "title": "Bash wants to run", "detail": "Bash: rm -rf build", "input": { "command": "rm -rf build" },
        "choices": [{ "id": "allow", "label": "Allow", "kind": "allow_once" }, { "id": "always", "label": "Always allow", "kind": "allow_always" },
                    { "id": "deny", "label": "Deny", "kind": "reject" }] } })
}

fn read(question: &Value) -> AgentQuestion {
    AgentQuestion::read(question).expect("the question reads")
}

fn build(question: &Value) -> WeftResult<Form> {
    form_for(&read(question), None)
}

fn keys(form: &Form) -> Vec<String> {
    form.schema.fields.iter().map(|f| f.key.clone()).collect()
}

fn question_form() -> WeftResult<()> {
    let history = json!({ "__weft_file__": { "key": "k" } });
    let form = form_for(&read(&colour()), Some(&history))?;
    assert_eq!(keys(&form), vec!["history", "q0", "q0.other", "q1", "q2"]);
    let colour = &form.schema.fields[1];
    assert_eq!(colour.label, "Colour: Red or blue?");
    assert_eq!(
        colour.config["options"],
        json!([{ "value": "Red", "label": "Red", "description": "warm" }, { "value": "Blue", "label": "Blue", "description": "cold" }])
    );
    assert!(!colour.render.multiple);
    assert!(form.schema.fields[3].render.multiple, "a multi select picks several");
    assert_eq!(form.schema.fields[0].value, Some(history), "the conversation beside it");
    // Every question must be answered, and the own box answers for its question.
    let required: Vec<bool> = form.schema.fields.iter().map(|f| f.required).collect();
    assert_eq!(required, vec![false, true, false, true, true]);
    assert_eq!(form.schema.fields[2].answers_for.as_deref(), Some("q0"));
    form.schema.check().map_err(weft::node_error)?;
    Ok(())
}

fn permission_form() -> WeftResult<()> {
    let form = build(&bash())?;
    assert_eq!(form.title.as_deref(), Some("Bash wants to run"));
    assert_eq!(keys(&form), vec!["detail", "choice", "note"]);
    assert_eq!(form.schema.fields[0].value, Some(json!("Bash: rm -rf build")));
    // The person sees each label, and the answer carries the choice's id.
    assert_eq!(
        form.schema.fields[1].config["options"],
        json!([{ "value": "allow", "label": "Allow" }, { "value": "always", "label": "Always allow" }, { "value": "deny", "label": "Deny" }])
    );
    assert!(form.schema.fields[1].required && !form.schema.fields[2].required, "a choice is required, the note is not");
    Ok(())
}

fn secret() -> WeftResult<()> {
    let mut q = colour();
    q["questions"][0]["secret"] = json!(true);
    let error = build(&q).expect_err("secret").to_string();
    assert!(error.contains("kept in the run in plain text"), "{error}");
    Ok(())
}

fn refused(question: &Value) -> String {
    build(question).expect_err("refused").to_string()
}

fn unmappable() -> WeftResult<()> {
    let mut q = bash();
    q["permission"]["choices"][1]["label"] = json!(null);
    let error = AgentQuestion::read(&q).expect_err("no label").to_string();
    assert!(error.contains("label"), "a choice with no label: {error}");
    let mut q = bash();
    q["permission"]["choices"][1]["label"] = json!(" ");
    assert!(refused(&q).contains("no label to show"));
    let mut q = bash();
    q["permission"]["choices"][1]["id"] = json!("allow");
    assert!(refused(&q).contains("repeated id 'allow'"));
    // Two choices with one label are fine: the answer carries the id.
    let mut q = bash();
    q["permission"]["choices"][1]["label"] = json!("Allow");
    let form = build(&q)?;
    let answer = answer_from(&read(&q), &form.schema.answers(&json!({ "choice": "always", "note": "" }))?)?;
    assert_eq!(answer["choice"], json!("always"));

    let mut q = colour();
    q["questions"][0]["options"][1]["label"] = json!("Red");
    assert!(refused(&q).contains("offers the option 'Red' twice"));
    let mut q = colour();
    q["questions"][0]["options"][1]["label"] = json!("");
    assert!(refused(&q).contains("an option with no label"));
    let mut q = colour();
    q["questions"][2]["id"] = json!("0");
    assert!(refused(&q).contains("two questions with the id '0'"));
    Ok(())
}

fn ids_are_not_keys() -> WeftResult<()> {
    // Ids that read like the form's own keys or another field's box.
    let mut q = colour();
    q["questions"][0]["id"] = json!("choice");
    q["questions"][1]["id"] = json!("note");
    q["questions"][2]["id"] = json!("choice.other");
    let form = form_for(&read(&q), Some(&json!({ "url": "https://x/h.json" })))?;
    assert_eq!(keys(&form), vec!["history", "q0", "q0.other", "q1", "q2"]);
    let sent = json!({ "q0": "Red", "q0.other": "", "q1": ["Tue"], "q2": "fine" });
    let answer = answer_from(&read(&q), &form.schema.answers(&sent)?)?;
    assert_eq!(answer["answers"], json!({ "choice": ["Red"], "note": ["Tue"], "choice.other": ["fine"] }));
    Ok(())
}

fn answers() -> WeftResult<()> {
    let question = read(&colour());
    let form = form_for(&question, None)?;
    let sent = json!({ "q0": "Blue", "q0.other": "", "q1": ["Mon", "Tue"], "q2": "nothing" });
    let answer = answer_from(&question, &form.schema.answers(&sent)?)?;
    assert_eq!(answer, json!({ "questionId": "q1", "answers": { "0": ["Blue"], "1": ["Mon", "Tue"], "2": ["nothing"] } }));
    let own = json!({ "q0": "", "q0.other": "green", "q1": ["Mon"], "q2": "x" });
    assert_eq!(answer_from(&question, &form.schema.answers(&own)?)?["answers"]["0"], json!(["green"]), "their own answer stands for a pick");
    let stray = json!({ "q0": "Green", "q0.other": "", "q1": ["Mon"], "q2": "x" });
    let error = answer_from(&question, &form.schema.answers(&stray)?).expect_err("not offered").to_string();
    assert!(error.contains("'Green'") && error.contains("did not offer"), "{error}");

    let permission = read(&bash());
    let form = form_for(&permission, None)?;
    let answer = answer_from(&permission, &form.schema.answers(&json!({ "choice": "deny", "note": "not the build folder" }))?)?;
    assert_eq!(answer, json!({ "questionId": "p1", "choice": "deny", "note": "not the build folder" }));
    let answer = answer_from(&permission, &form.schema.answers(&json!({ "choice": "allow", "note": "" }))?)?;
    assert_eq!(answer, json!({ "questionId": "p1", "choice": "allow" }));
    let error = answer_from(&permission, &form.schema.answers(&json!({ "choice": "Allow" }))?).expect_err("a label is no id").to_string();
    assert!(error.contains("did not offer"), "{error}");
    Ok(())
}

fn own_beside_picks() -> WeftResult<()> {
    let mut q = colour();
    q["questions"][1]["allowOther"] = json!(true);
    let question = read(&q);
    let form = form_for(&question, None)?;
    assert_eq!(keys(&form), vec!["q0", "q0.other", "q1", "q1.other", "q2"]);
    assert_eq!(form.schema.fields[3].config["placeholder"], json!("sent along with the ones picked above"), "the box says it adds to the picks");
    assert_eq!(form.schema.fields[1].config["placeholder"], json!("leave empty to use the one picked above"), "beside one pick it replaces it");
    let sent = |days: Value, own: &str| json!({ "q0": "Red", "q0.other": "", "q1": days, "q1.other": own, "q2": "x" });
    let days = |sent: Value| -> WeftResult<Value> { Ok(answer_from(&question, &form.schema.answers(&sent)?)?["answers"]["1"].clone()) };
    assert_eq!(days(sent(json!(["Mon", "Tue"]), "Fri"))?, json!(["Mon", "Tue", "Fri"]), "the picks first, then their own");
    assert_eq!(days(sent(json!(["Mon"]), "Mon"))?, json!(["Mon"]), "once, when their own is one of the picks");
    assert_eq!(days(sent(json!([]), "Fri"))?, json!(["Fri"]), "their own alone answers it");
    assert_eq!(days(sent(json!(["Tue"]), " "))?, json!(["Tue"]), "a blank box adds nothing");
    let single = answer_from(&question, &form.schema.answers(&json!({ "q0": "Red", "q0.other": "green", "q1": ["Mon"], "q1.other": "", "q2": "x" }))?)?;
    assert_eq!(single["answers"]["0"], json!(["green"]), "beside one pick, their own answer wins");
    Ok(())
}

/// A form sent by an early click, with nothing picked, is refused by the
/// read, so the agent's question waits for a real answer.
fn unanswered() -> WeftResult<()> {
    let form = form_for(&read(&colour()), None)?;
    for sent in [
        json!({ "q0": "", "q0.other": "", "q1": ["Mon"], "q2": "x" }),
        json!({ "q0": "Red", "q0.other": "", "q1": [], "q2": "x" }),
        json!({ "q0": "Red", "q0.other": "", "q1": ["Mon"], "q2": "" }),
    ] {
        let error = form.schema.answers(&sent).expect_err("unanswered").to_string();
        assert!(error.contains("which must be answered"), "{error}");
    }
    let form = build(&bash())?;
    let error = form.schema.answers(&json!({ "choice": "", "note": "" })).expect_err("nothing picked").to_string();
    assert!(error.contains("(choice)"), "{error}");
    Ok(())
}

async fn waits(rig: FakeRig) -> WeftResult<()> {
    rig.signal_paused();
    let paused = rig.run(&AnswerAgentNode, json!({ "question": bash() })).await;
    assert!(paused.paused(), "the run waits for the person: {:?}", paused.result);
    rig.signal(json!({ "branch": 0, "value": { "choice": "always", "note": "" } }));
    let outcome = rig.run(&AnswerAgentNode, json!({ "question": bash() })).await.ok()?;
    assert_eq!(outcome.outputs["answer"], json!({ "questionId": "p1", "choice": "always" }));
    // The form, and beside it the agent server's word on whether the agent
    // still waits, whichever comes first.
    let waited = FirstOf::read(&rig.awaited_signals()[0]).map_err(weft::node_error)?;
    assert_eq!(waited.branches.iter().map(|b| b.kind.as_str()).collect::<Vec<_>>(), ["form", "poll_endpoint"]);
    assert_eq!(waited.branches[1].config["url"], json!("http://agent.test/question?id=p1"));
    assert_eq!(waited.branches[1].match_predicates, vec![weft::signal::Predicate::eq("gone", "true")]);
    Ok(())
}

/// The agent stopped waiting for the answer: the form ends, and the answer
/// that goes on says so, for AskAgent to read the rest of the turn with.
async fn dropped(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "branch": 1, "value": { "why": "the agent stopped waiting for the answer" } }));
    let outcome = rig.run(&AnswerAgentNode, json!({ "question": colour() })).await.ok()?;
    assert_eq!(outcome.outputs["answer"], json!({ "questionId": "q1", "dropped": true }));
    Ok(())
}
