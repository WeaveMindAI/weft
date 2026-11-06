//! AnswerAgent: a coding agent's question, put to a person as a form.
//!
//! The question is data (`AgentQuestion`, whatever the agent and whatever
//! it asks), so the form is built when the question arrives: each of the
//! agent's questions as its options to pick (one, or several), a box to
//! write another answer when the agent takes one, and for a permission
//! request what the agent wants to run with the choices it offered and a
//! note it reads. The run waits for the person without holding a worker,
//! and the answer goes out as the `AgentAnswer` the next `AskAgent` firing
//! carries back to the waiting agent.
//!
//! An agent may stop waiting for the answer on its own (OpenClaw drops a
//! question after an hour) and carry on without it. So the form is not the
//! only thing the run waits on: beside it, the agent server's word on
//! whether its agent still waits (the question's `statusUrl`), and
//! whichever comes first ends the wait (`FirstOf`), taking the other down.
//! Once the agent has dropped the question, the form goes, and the answer
//! that goes out says so (`dropped`). That is still an answer, so a loop
//! like `conversation.weft` carries on with no wiring of its own: the next
//! `AskAgent` firing hands it to the server, which reads it as "go on with
//! the turn" and hands back what the agent did after it gave up. A failure
//! on `_error` would have stopped the loop there instead.

use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::signal::poll_endpoint::DEFAULT_POLL_INTERVAL_SECS;
use weft::signal::{Answers, ChoiceOption, Either, FirstOf, Form, FormField, PollEndpoint, Predicate};
use weft::{node_bail, node_error, Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

#[derive(NodeManifest)]
pub struct AnswerAgentNode;

#[cfg(feature = "node-tests")]
mod tests;

#[derive(Default, Serialize, Deserialize)]
struct Asked {
    /// What ended the wait: the person's submission, or the agent server
    /// saying the agent no longer waits for an answer.
    ended: Option<Either<Value, Dropped>>,
}

/// The agent server's word that nothing waits for the answer any more
/// (`/question` answering `gone`), and why.
// SYNC: the /question answer <-> catalog/agents/server/agent_server.py (Conversation.question)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Dropped {
    why: String,
}

/// An `AgentQuestion`, read typed so a question missing a part the form
/// needs (an id, a label) fails here, naming the part, instead of
/// drawing a blank option the answer could not be mapped back from.
// SYNC: AgentQuestion and its members <-> catalog/agents/metadata.json (types),
//       catalog/agents/server/events.py (question, permission, choice)
#[derive(Debug, Clone, Deserialize)]
pub struct AgentQuestion {
    id: String,
    kind: QuestionKind,
    questions: Vec<AgentQuestionItem>,
    permission: Option<AgentPermission>,
    /// Where the agent server says whether its agent still waits on this
    /// question (AskAgent sets it, `status_url`).
    #[serde(rename = "statusUrl")]
    status_url: String,
}

// SYNC: question kinds <-> catalog/agents/server/events.py (KIND_QUESTION, KIND_PERMISSION)
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QuestionKind {
    Question,
    Permission,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentQuestionItem {
    id: String,
    header: String,
    question: String,
    options: Vec<AgentOption>,
    multi_select: bool,
    allow_other: bool,
    secret: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct AgentOption {
    label: String,
    description: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AgentPermission {
    title: String,
    detail: String,
    choices: Vec<AgentChoice>,
}

#[derive(Debug, Clone, Deserialize)]
struct AgentChoice {
    id: String,
    label: String,
}

impl AgentQuestion {
    /// `value` read as an `AgentQuestion`, failing naming what is wrong.
    pub fn read(value: &Value) -> WeftResult<AgentQuestion> {
        serde_json::from_value(value.clone())
            .map_err(|e| node_error(format!("the agent's question does not read as an AgentQuestion ({e}): {value}")))
    }
}

// The form's own fields have fixed keys, and each of the agent's
// questions gets `q<its position>`: the agent's ids are its own text and
// could be anything (`choice`, or `0.other`), so they never become keys,
// and no agent id can land on a fixed key or on another field's box.
// The answer goes back under the agent's id, found by position.
const HISTORY_KEY: &str = "history";
const DETAIL_KEY: &str = "detail";
const CHOICE_KEY: &str = "choice";
const NOTE_KEY: &str = "note";

/// The key of the agent's question at `index`.
fn question_key(index: usize) -> String {
    format!("q{index}")
}

/// The key of the box beside the question at `index`, for an answer of
/// the person's own.
fn other_key(index: usize) -> String {
    format!("q{index}.other")
}

/// The form for `question`, with the conversation beside it when one is given.
pub fn form_for(question: &AgentQuestion, history: Option<&Value>) -> WeftResult<Form> {
    let mut fields = Vec::new();
    if let Some(file) = history {
        fields.push(FormField::chat_history(HISTORY_KEY, "The conversation so far", file.clone()));
    }
    let title = match question.kind {
        QuestionKind::Question => {
            if question.questions.is_empty() {
                node_bail!("the agent asked a question with nothing in it (question '{}')", question.id);
            }
            let mut ids = BTreeSet::new();
            for (index, q) in question.questions.iter().enumerate() {
                // The answer is a record keyed by these ids: two alike would
                // put one answer where the agent expects two.
                if !ids.insert(q.id.as_str()) {
                    node_bail!("the agent asked two questions with the id '{}' (question '{}')", q.id, question.id);
                }
                fields.extend(question_fields(index, q)?);
            }
            "The agent asks".to_string()
        }
        QuestionKind::Permission => {
            let permission = permission_of(question)?;
            if permission.choices.is_empty() {
                node_bail!("the agent's permission request offers no choice (question '{}')", question.id);
            }
            let mut ids = BTreeSet::new();
            let mut options = Vec::with_capacity(permission.choices.len());
            for c in &permission.choices {
                if c.id.is_empty() || !ids.insert(c.id.as_str()) {
                    node_bail!("the agent's permission request offers a choice with an empty or repeated id '{}' (question '{}')", c.id, question.id);
                }
                if c.label.trim().is_empty() {
                    node_bail!("the agent's permission choice '{}' has no label to show (question '{}')", c.id, question.id);
                }
                // The person sees the label; the answer carries the id, so
                // two choices with one label still map back exactly.
                options.push(ChoiceOption::new(&c.id, &c.label));
            }
            fields.push(FormField::display(DETAIL_KEY, "It wants to run", json!(permission.detail)));
            fields.push(FormField::choice(CHOICE_KEY, "Your answer", options).required());
            fields.push(FormField::long_text(NOTE_KEY, "A note the agent reads (optional)"));
            if permission.title.trim().is_empty() { "The agent asks for permission".to_string() } else { permission.title.clone() }
        }
    };
    Ok(Form::for_person("agent-question", Some(title), None, fields))
}

fn permission_of(question: &AgentQuestion) -> WeftResult<&AgentPermission> {
    question.permission.as_ref().node_err(format!("the agent's permission request carries no permission (question '{}')", question.id))
}

fn question_fields(index: usize, q: &AgentQuestionItem) -> WeftResult<Vec<FormField>> {
    if q.secret {
        // A form's answer is kept in the run's record as it was typed.
        node_bail!("the agent asks for a secret ({:?}), and a form's answer is kept in the run in plain text; put it in a file in the agent's folder and tell the agent where it is", q.question);
    }
    let label = if q.header.is_empty() { q.question.clone() } else { format!("{}: {}", q.header, q.question) };
    let key = question_key(index);
    if q.options.is_empty() {
        return Ok(vec![FormField::long_text(key, label).required()]);
    }
    let mut labels = BTreeSet::new();
    let mut options = Vec::with_capacity(q.options.len());
    for o in &q.options {
        // The agent reads an answer by its option's label, so the label is
        // also the value, and two alike could not be told apart.
        if o.label.trim().is_empty() {
            node_bail!("the agent's question {:?} offers an option with no label", q.question);
        }
        if !labels.insert(o.label.as_str()) {
            node_bail!("the agent's question {:?} offers the option '{}' twice", q.question, o.label);
        }
        options.push(ChoiceOption::text(&o.label).described(&o.description));
    }
    let choice = if q.multi_select { FormField::choices(&key, label, options) } else { FormField::choice(&key, label, options) };
    let mut fields = vec![choice.required()];
    if q.allow_other {
        // What the box does with the picks, said where the person types:
        // beside several picks it adds to them, beside one it replaces it
        // (`answer_from`).
        let (label, placeholder) = if q.multi_select {
            ("Or add your own answer", "sent along with the ones picked above")
        } else {
            ("Or write your own answer", "leave empty to use the one picked above")
        };
        fields.push(FormField::text(other_key(index), label).placeholder(placeholder).answering_for(key));
    }
    Ok(fields)
}

/// What the form waits on beside the person: the agent server saying the
/// agent no longer waits for this answer. Polled by the listener while the
/// form is up and never after (the wait that ends takes it down), so it
/// keeps nothing awake past the question. Slower than AskAgent's live
/// reads: nobody is watching for it, and an agent drops a question after
/// minutes or hours, never seconds.
pub fn dropped_by_agent(question: &AgentQuestion) -> PollEndpoint {
    PollEndpoint {
        url: question.status_url.clone(),
        interval_secs: DEFAULT_POLL_INTERVAL_SECS,
        filters: vec![Predicate::eq("gone", "true")],
        carry: vec!["why".into()],
        ..Default::default()
    }
}

/// The `AgentAnswer` saying the agent dropped `question`: no answer, and
/// the agent server, handed it, reads the turn on from where the agent
/// went on without one.
// SYNC: the `dropped` answer <-> catalog/agents/server/agent_server.py (Conversation.deliver)
pub fn dropped_answer(question: &AgentQuestion) -> Value {
    json!({ "questionId": question.id, "dropped": true })
}

/// The `AgentAnswer` for what the person sent back.
pub fn answer_from(question: &AgentQuestion, answers: &Answers) -> WeftResult<Value> {
    match question.kind {
        QuestionKind::Question => {
            let mut out = Map::new();
            for (index, q) in question.questions.iter().enumerate() {
                // The box for an answer of their own exists only beside options
                // the agent lets them go past (`question_fields`).
                let has_box = q.allow_other && !q.options.is_empty();
                let own: Option<String> =
                    if has_box { answers.value::<String>(&other_key(index))?.filter(|s| !s.trim().is_empty()) } else { None };
                let picks = picked(q, answers, &question_key(index))?;
                let given: Vec<String> = match own {
                    // Several picks and an answer of their own: all of it,
                    // the picks first, the own answer once.
                    Some(text) if q.multi_select => {
                        let mut given = picks;
                        if !given.contains(&text) {
                            given.push(text);
                        }
                        given
                    }
                    // One pick: their own answer is the one they meant.
                    Some(text) => vec![text],
                    None if picks.is_empty() => node_bail!("the form came back with no answer to {:?}", q.question),
                    None => picks,
                };
                out.insert(q.id.clone(), json!(given));
            }
            Ok(json!({ "questionId": question.id, "answers": out }))
        }
        QuestionKind::Permission => {
            let picked: String = answers.value(CHOICE_KEY)?.node_err("the form came back with no choice picked")?;
            let choice = permission_of(question)?
                .choices
                .iter()
                .find(|c| c.id == picked)
                .node_err(format!("the form came back with the choice '{picked}', which the agent did not offer"))?;
            let mut answer = json!({ "questionId": question.id, "choice": choice.id });
            if let Some(note) = answers.value::<String>(NOTE_KEY)?.filter(|n| !n.trim().is_empty()) {
                answer["note"] = json!(note);
            }
            Ok(answer)
        }
    }
}

/// What the person picked or wrote for the question `q` under `key`,
/// nothing when they left it blank (which the form allows only when the
/// box for their own answer answers for it). The form already refused a
/// blank answer otherwise (every one is required), so this reads the
/// value's shape: the text written, or the options picked, each one the
/// agent offered.
fn picked(q: &AgentQuestionItem, answers: &Answers, key: &str) -> WeftResult<Vec<String>> {
    let given: Vec<String> = match answers.value::<Value>(key)? {
        None => Vec::new(),
        Some(Value::String(one)) if one.trim().is_empty() => Vec::new(),
        Some(Value::String(one)) => vec![one],
        Some(Value::Array(many)) => many
            .into_iter()
            .map(|v| v.as_str().map(str::to_string).node_err(format!("the form came back with {v} for {:?}, which is not an option's text", q.question)))
            .collect::<WeftResult<_>>()?,
        Some(other) => node_bail!("the form came back with {other} for {:?}, which is no answer to it", q.question),
    };
    if !q.options.is_empty() {
        if let Some(stray) = given.iter().find(|g| !q.options.iter().any(|o| &o.label == *g)) {
            node_bail!("the form came back with '{stray}' for {:?}, which the agent did not offer", q.question);
        }
    }
    Ok(given)
}

#[async_trait]
impl Node for AnswerAgentNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let question = AgentQuestion::read(&ctx.inputs.get::<Value>("question")?)?;
        let history: Option<Value> = ctx.inputs.opt("history")?;
        let form = form_for(&question, history.as_ref())?;
        let schema = form.schema.clone();
        let mut state = ctx.keep(Asked::default()).await?;
        let answer = match state.wait(|s| &mut s.ended, FirstOf::either(form, dropped_by_agent(&question))).await? {
            Either::First(submission) => answer_from(&question, &schema.answers(&submission)?)?,
            Either::Second(dropped) => {
                ctx.log(LogLevel::Warn, format!("the question was withdrawn before anybody answered it: {}", dropped.why)).await?;
                dropped_answer(&question)
            }
        };
        ctx.pulse_downstream(NodeOutput::new().set("answer", answer)).await
    }
}
