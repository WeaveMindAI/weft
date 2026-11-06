//! Building the form a person sees from data, and reading what they
//! sent back. Any node that asks a person something goes through here:
//! one whose settings list the fields (HumanQuery, HumanTrigger, through
//! [`super::ConfigForm`]) and one that builds its fields at run time from
//! what it was handed (a question an agent asked).
//!
//! A field is drawn by its `render.component`, never by its
//! `field_type`, so the component decides both what the person sees and
//! what shape their answer comes back in: a decision for `buttons`,
//! nothing for a field that only shows something, and the value as sent
//! for everything else.
//!
//! A field marked [`FormField::required`] must come back answered: the
//! consumer keeps its send button off until it is, and
//! [`FormSchema::answers`] refuses a submission where it is not, so a
//! node that needs an answer never has to guess what an early click
//! meant.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use super::{Form, FormField, FormSchema};
use crate::error::WeftResult;
use crate::node::{FormFieldRender, FormFieldSource};

/// The components a consumer knows how to draw.
// SYNC: form components <->
//       extension-browser/src/entrypoints/tasks/App.svelte (the `r.component` chain, SHOWN_ONLY),
//       catalog/human/metadata.json (each spec's `render.component`)
pub mod component {
    /// Shows the field's value as text, asks nothing.
    pub const READONLY: &str = "readonly";
    /// Shows the field's value as a picture, asks nothing.
    pub const IMAGE: &str = "image";
    /// Two buttons, approve and reject: the answer is a decision.
    pub const BUTTONS: &str = "buttons";
    /// Picks one option, or several with `multiple`.
    pub const SELECT: &str = "select";
    /// One line of text.
    pub const TEXT: &str = "text";
    /// Several lines of text.
    pub const TEXTAREA: &str = "textarea";
    /// Shows a conversation (a `ChatHistory` file) as its messages, asks
    /// nothing.
    pub const CHAT_HISTORY: &str = "chat_history";

    /// The components that show a value and ask nothing back.
    pub const SHOWN_ONLY: &[&str] = &[READONLY, IMAGE, CHAT_HISTORY];
}

/// The `config` keys a consumer reads off a field.
// SYNC: form config keys <->
//       extension-browser/src/entrypoints/tasks/App.svelte (`field.config.*`, readOptions),
//       catalog/human/metadata.json (each spec's `fields`)
pub mod config_key {
    /// A select's options: a list of strings, or of [`super::ChoiceOption`]
    /// (`{ value, label, description? }`, drawn as the label with the
    /// description under it). The answer is the picked option's value; a
    /// string option is its own value.
    pub const OPTIONS: &str = "options";
    /// The text an empty text box shows.
    pub const PLACEHOLDER: &str = "placeholder";
    /// The approve button's text (`Approve` when absent).
    pub const APPROVE_LABEL: &str = "approveLabel";
    /// The reject button's text (`Reject` when absent).
    pub const REJECT_LABEL: &str = "rejectLabel";
}

/// The consumer kind a form a person answers is listed under: the
/// browser extension lists the forms tagged with it.
pub const HUMAN_IN_THE_LOOP: &str = "human_in_the_loop";

impl Form {
    /// The form a person answers, listed where they answer forms (the
    /// browser extension). `form_type` is the asking node's own name for
    /// its form (`"human-query"`); nothing draws by it.
    pub fn for_person(
        form_type: impl Into<String>,
        title: Option<String>,
        description: Option<String>,
        fields: Vec<FormField>,
    ) -> Form {
        Form {
            form_type: form_type.into(),
            schema: FormSchema { fields },
            title,
            description,
            consumer_kind: Some(HUMAN_IN_THE_LOOP.into()),
        }
    }
}

/// One option of a [`FormField::choice`]: the person sees `label` (and
/// `description` under it), and the answer carries `value`. The two are
/// apart so a node can offer options whose texts repeat, or read as
/// something other than what it needs back (an id), and still map the
/// answer back exactly.
// SYNC: choice option shape <->
//       extension-browser/src/entrypoints/tasks/App.svelte (readOptions, ChoiceOption)
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ChoiceOption {
    pub value: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ChoiceOption {
    /// An option showing `label` whose answer is `value`.
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> ChoiceOption {
        ChoiceOption { value: value.into(), label: label.into(), description: None }
    }

    /// An option whose answer is the text it shows.
    pub fn text(text: impl Into<String>) -> ChoiceOption {
        let text = text.into();
        ChoiceOption::new(text.clone(), text)
    }

    /// A line drawn under the label.
    pub fn described(mut self, description: impl Into<String>) -> ChoiceOption {
        self.description = Some(description.into());
        self
    }
}

fn options_value(options: Vec<ChoiceOption>) -> Value {
    serde_json::to_value(options).expect("a choice option is plain strings")
}

fn render(component: &str, source: Option<FormFieldSource>, multiple: bool) -> FormFieldRender {
    FormFieldRender { component: component.into(), source, multiple, prefilled: false }
}

impl FormField {
    /// A field drawn by `render`, with no value and no config yet. The
    /// named constructors below cover what consumers draw today; this is
    /// for a component they learn later.
    pub fn new(
        field_type: impl Into<String>,
        key: impl Into<String>,
        label: impl Into<String>,
        render: FormFieldRender,
    ) -> FormField {
        FormField {
            field_type: field_type.into(),
            key: key.into(),
            label: label.into(),
            render,
            value: None,
            config: Map::new(),
            required: false,
            answers_for: None,
        }
    }

    /// Shows `value` to the person as text (a list or a record shows as
    /// JSON). Asks nothing.
    pub fn display(key: impl Into<String>, label: impl Into<String>, value: Value) -> FormField {
        FormField::new("display", key, label, render(component::READONLY, None, false)).with_value(value)
    }

    /// Shows `image` (an image value, stored or a URL) to the person.
    /// Asks nothing.
    pub fn display_image(key: impl Into<String>, label: impl Into<String>, image: Value) -> FormField {
        FormField::new("display_image", key, label, render(component::IMAGE, None, false)).with_value(image)
    }

    /// Shows `conversation` (a `ChatHistory` file) message by message,
    /// for the context of what is asked. Asks nothing.
    pub fn chat_history(key: impl Into<String>, label: impl Into<String>, conversation: Value) -> FormField {
        FormField::new("chat_history", key, label, render(component::CHAT_HISTORY, None, false)).with_value(conversation)
    }

    /// Approve or reject: the answer is a decision
    /// ([`Answers::decision`]). The button texts are
    /// [`Self::approve_label`] and [`Self::reject_label`].
    pub fn decision(key: impl Into<String>, label: impl Into<String>) -> FormField {
        FormField::new("approve_reject", key, label, render(component::BUTTONS, Some(FormFieldSource::Static), false))
    }

    /// Pick exactly one of `options`: the answer is the picked option's
    /// value.
    pub fn choice(key: impl Into<String>, label: impl Into<String>, options: Vec<ChoiceOption>) -> FormField {
        FormField::new("select", key, label, render(component::SELECT, Some(FormFieldSource::Static), false))
            .with_config(config_key::OPTIONS, options_value(options))
    }

    /// Pick any of `options`: the answer is the list of the picked
    /// options' values.
    pub fn choices(key: impl Into<String>, label: impl Into<String>, options: Vec<ChoiceOption>) -> FormField {
        FormField::new("multi_select", key, label, render(component::SELECT, Some(FormFieldSource::Static), true))
            .with_config(config_key::OPTIONS, options_value(options))
    }

    /// One line of text: the answer is what the person typed.
    pub fn text(key: impl Into<String>, label: impl Into<String>) -> FormField {
        FormField::new("text_input", key, label, render(component::TEXT, None, false))
    }

    /// Several lines of text: the answer is what the person typed.
    pub fn long_text(key: impl Into<String>, label: impl Into<String>) -> FormField {
        FormField::new("textarea", key, label, render(component::TEXTAREA, None, false))
    }

    /// The value the field shows (or starts from).
    pub fn with_value(mut self, value: Value) -> FormField {
        self.value = Some(value);
        self
    }

    /// One `config` entry, for a key the field's component reads.
    pub fn with_config(mut self, key: &str, value: Value) -> FormField {
        self.config.insert(key.to_string(), value);
        self
    }

    /// The text an empty text box shows.
    pub fn placeholder(self, text: impl Into<String>) -> FormField {
        self.with_config(config_key::PLACEHOLDER, Value::String(text.into()))
    }

    /// The approve button's text on a [`Self::decision`].
    pub fn approve_label(self, text: impl Into<String>) -> FormField {
        self.with_config(config_key::APPROVE_LABEL, Value::String(text.into()))
    }

    /// The reject button's text on a [`Self::decision`].
    pub fn reject_label(self, text: impl Into<String>) -> FormField {
        self.with_config(config_key::REJECT_LABEL, Value::String(text.into()))
    }

    /// The person must answer this field before the form can be sent
    /// (or answer the field that [`Self::answering_for`] it).
    pub fn required(mut self) -> FormField {
        self.required = true;
        self
    }

    /// This field is a second way of answering the field `key` (a box
    /// for an answer of the person's own beside its options): an
    /// answer here counts as one there for [`Self::required`].
    pub fn answering_for(mut self, key: impl Into<String>) -> FormField {
        self.answers_for = Some(key.into());
        self
    }

    /// Does the person answer this field, or does it only show
    /// something?
    pub fn asks(&self) -> bool {
        !component::SHOWN_ONLY.contains(&self.render.component.as_str())
    }

    /// This field's answer in `submission`, `None` for a field that
    /// asks nothing.
    fn answer(&self, submission: &Map<String, Value>) -> WeftResult<Option<Answer>> {
        if !self.asks() {
            return Ok(None);
        }
        let given = submission.get(&self.key).cloned().unwrap_or(Value::Null);
        if self.render.component != component::BUTTONS {
            return Ok(Some(Answer::Given(given)));
        }
        let approved = match &given {
            Value::Bool(b) => *b,
            Value::String(s) if s == "approve" => true,
            Value::String(s) if s == "reject" => false,
            other => crate::node_bail!(
                "form field '{}' ({}) needs a decision (true or \"approve\", false \
                 or \"reject\"), and the response carried {other}",
                self.key,
                self.field_type
            ),
        };
        Ok(Some(Answer::Decision(approved)))
    }
}

/// What a person answered to one field.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    /// A `buttons` field: approved (`true`) or rejected (`false`).
    Decision(bool),
    /// Any other field that asks: the value as the person sent it, JSON
    /// null when they sent nothing for it.
    Given(Value),
}

/// Every answer of one submission, by field key. Fields that only show
/// something have none.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Answers {
    by_key: BTreeMap<String, Answer>,
}

/// Is this answer no answer at all: nothing sent, a blank text, or an
/// empty pick? The rule a [`FormField::required`] field is held to.
// SYNC: blank answer <-> extension-browser/src/entrypoints/tasks/App.svelte (isBlank)
fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

impl FormSchema {
    /// Refuse a schema no person could answer as meant: two fields with
    /// one key (one answer would land on both), a required field that
    /// asks nothing, and a field answering for one that is not there or
    /// asks nothing. Run when the form is registered
    /// ([`crate::signal::Signal::validate`]) and again when it is read.
    pub fn check(&self) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        for field in &self.fields {
            if !seen.insert(field.key.as_str()) {
                return Err(format!("the form has two fields with the key '{}'", field.key));
            }
            if field.required && !field.asks() {
                return Err(format!(
                    "form field '{}' is required, but its component '{}' only shows something",
                    field.key, field.render.component
                ));
            }
        }
        for field in &self.fields {
            let Some(target) = &field.answers_for else { continue };
            let asks = self.fields.iter().any(|f| &f.key == target && f.key != field.key && f.asks());
            if !asks {
                return Err(format!(
                    "form field '{}' answers for '{target}', which is no other field of the form that asks something",
                    field.key
                ));
            }
        }
        Ok(())
    }

    /// Read a submission against the fields that were asked. A
    /// submission is the record a consumer sends, keyed by field key; a
    /// decision that is not one fails naming its field, and so does a
    /// required field that came back with no answer (nor one in a field
    /// answering for it).
    pub fn answers(&self, submission: &Value) -> WeftResult<Answers> {
        self.check().map_err(crate::error::node_error)?;
        let Some(submission) = submission.as_object() else {
            crate::node_bail!("a form submission is a record keyed by field key, and this one is {submission}");
        };
        let mut by_key = BTreeMap::new();
        for field in &self.fields {
            if let Some(answer) = field.answer(submission)? {
                by_key.insert(field.key.clone(), answer);
            }
        }
        let answered = |key: &str| match by_key.get(key) {
            Some(Answer::Given(value)) => !is_blank(value),
            Some(Answer::Decision(_)) => true,
            None => false,
        };
        for field in self.fields.iter().filter(|f| f.required) {
            let by_another = self.fields.iter().any(|f| f.answers_for.as_deref() == Some(field.key.as_str()) && answered(&f.key));
            if !answered(&field.key) && !by_another {
                crate::node_bail!("the form came back with no answer to '{}' ({}), which must be answered", field.label, field.key);
            }
        }
        Ok(Answers { by_key })
    }
}

impl Answers {
    /// The answer to the field `key`, `None` when no asking field has
    /// that key.
    pub fn get(&self, key: &str) -> Option<&Answer> {
        self.by_key.get(key)
    }

    /// The decision on the `buttons` field `key`.
    pub fn decision(&self, key: &str) -> WeftResult<bool> {
        match self.by_key.get(key) {
            Some(Answer::Decision(approved)) => Ok(*approved),
            Some(Answer::Given(_)) => crate::node_bail!("form field '{key}' is not a decision"),
            None => crate::node_bail!("the form has no field '{key}' that asks something"),
        }
    }

    /// The answer to the field `key`, typed. `None` when the person sent
    /// nothing for it; a value that does not read as `T` fails naming
    /// the field.
    pub fn value<T: DeserializeOwned>(&self, key: &str) -> WeftResult<Option<T>> {
        match self.by_key.get(key) {
            Some(Answer::Given(Value::Null)) => Ok(None),
            Some(Answer::Given(value)) => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(|e| crate::error::node_error(format!("form field '{key}' answered {value}: {e}"))),
            Some(Answer::Decision(_)) => crate::node_bail!("form field '{key}' is a decision; read it with `decision`"),
            None => crate::node_bail!("the form has no field '{key}' that asks something"),
        }
    }

    /// Every answer, by field key.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Answer)> {
        self.by_key.iter().map(|(k, a)| (k.as_str(), a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn question() -> Form {
        Form::for_person(
            "agent-question",
            Some("The agent asks".into()),
            None,
            vec![
                FormField::display("history", "So far", json!("the conversation")),
                FormField::choice("pick", "Which one?", vec![ChoiceOption::text("a"), ChoiceOption::new("id-b", "b").described("the second")]),
                FormField::choices("many", "Which ones?", vec![ChoiceOption::text("x"), ChoiceOption::text("y")]),
                FormField::text("other", "Something else").placeholder("type here"),
                FormField::decision("allow", "Run it?").approve_label("Allow").reject_label("Deny"),
                FormField::long_text("note", "Why"),
            ],
        )
    }

    /// The wire is what the extension draws from, so the built fields
    /// must read exactly like the ones HumanQuery's metadata produces.
    #[test]
    fn built_fields_carry_the_components_and_config_a_consumer_draws() {
        let form = question();
        assert_eq!(form.consumer_kind.as_deref(), Some(HUMAN_IN_THE_LOOP));
        let json = serde_json::to_value(&form.schema).unwrap();
        assert_eq!(
            json,
            json!({ "fields": [
                { "fieldType": "display", "key": "history", "label": "So far",
                  "render": { "component": "readonly" }, "value": "the conversation", "config": {} },
                { "fieldType": "select", "key": "pick", "label": "Which one?",
                  "render": { "component": "select", "source": "static" },
                  "config": { "options": [{ "value": "a", "label": "a" }, { "value": "id-b", "label": "b", "description": "the second" }] } },
                { "fieldType": "multi_select", "key": "many", "label": "Which ones?",
                  "render": { "component": "select", "source": "static", "multiple": true },
                  "config": { "options": [{ "value": "x", "label": "x" }, { "value": "y", "label": "y" }] } },
                { "fieldType": "text_input", "key": "other", "label": "Something else",
                  "render": { "component": "text" }, "config": { "placeholder": "type here" } },
                { "fieldType": "approve_reject", "key": "allow", "label": "Run it?",
                  "render": { "component": "buttons", "source": "static" }, "config": { "approveLabel": "Allow", "rejectLabel": "Deny" } },
                { "fieldType": "textarea", "key": "note", "label": "Why",
                  "render": { "component": "textarea" }, "config": {} }
            ] })
        );
    }

    #[test]
    fn a_submission_reads_as_typed_answers_and_a_shown_field_has_none() {
        let form = question();
        let answers = form
            .schema
            .answers(&json!({ "pick": "id-b", "many": ["x", "y"], "other": "", "allow": "reject", "history": "ignored" }))
            .unwrap();
        assert_eq!(answers.value::<String>("pick").unwrap().as_deref(), Some("id-b"));
        assert_eq!(answers.value::<Vec<String>>("many").unwrap(), Some(vec!["x".to_string(), "y".to_string()]));
        assert_eq!(answers.value::<String>("other").unwrap().as_deref(), Some(""));
        assert!(!answers.decision("allow").unwrap());
        // Nothing sent for the note: it is there, and empty.
        assert_eq!(answers.get("note"), Some(&Answer::Given(Value::Null)));
        assert_eq!(answers.value::<String>("note").unwrap(), None);
        // The shown field asks nothing, so whatever came back for it is no answer.
        assert_eq!(answers.get("history"), None);
        assert_eq!(answers.iter().count(), 5);
    }

    #[test]
    fn a_decision_takes_a_boolean_or_its_word_and_refuses_anything_else() {
        let schema = FormSchema { fields: vec![FormField::decision("ok", "Ok?")] };
        assert!(schema.answers(&json!({ "ok": true })).unwrap().decision("ok").unwrap());
        assert!(schema.answers(&json!({ "ok": "approve" })).unwrap().decision("ok").unwrap());
        assert!(!schema.answers(&json!({ "ok": false })).unwrap().decision("ok").unwrap());
        let err = schema.answers(&json!({ "ok": "maybe" })).unwrap_err().to_string();
        assert!(err.contains("'ok'") && err.contains("needs a decision"), "{err}");
        let err = schema.answers(&json!({})).unwrap_err().to_string();
        assert!(err.contains("needs a decision"), "a missing decision is no decision: {err}");
    }

    /// A required field left blank fails the read naming it, unless a
    /// field answering for it was filled in; a field not required may
    /// stay blank.
    #[test]
    fn a_required_field_needs_an_answer_of_its_own_or_from_the_field_answering_for_it() {
        let schema = FormSchema {
            fields: vec![
                FormField::choice("pick", "Which one?", vec![ChoiceOption::text("a")]).required(),
                FormField::text("own", "Or your own").answering_for("pick"),
                FormField::choices("many", "Which ones?", vec![ChoiceOption::text("x")]).required(),
                FormField::long_text("note", "A note"),
            ],
        };
        schema.check().unwrap();
        // On the wire as the consumer reads them, absent when off.
        let wire = serde_json::to_value(&schema).unwrap();
        assert_eq!(wire["fields"][0]["required"], json!(true));
        assert_eq!(wire["fields"][1]["answersFor"], json!("pick"));
        assert!(wire["fields"][1].get("required").is_none() && wire["fields"][0].get("answersFor").is_none());
        for blank in [json!({ "many": ["x"] }), json!({ "pick": "", "own": "  ", "many": ["x"] })] {
            let err = schema.answers(&blank).unwrap_err().to_string();
            assert!(err.contains("no answer to 'Which one?' (pick)"), "{err}");
        }
        let err = schema.answers(&json!({ "pick": "a", "many": [] })).unwrap_err().to_string();
        assert!(err.contains("(many)"), "an empty pick is no answer: {err}");
        assert!(schema.answers(&json!({ "pick": "a", "many": ["x"] })).is_ok());
        assert!(schema.answers(&json!({ "pick": "", "own": "mine", "many": ["x"] })).is_ok(), "the own box answers for it");
    }

    #[test]
    fn a_schema_nobody_could_answer_as_meant_is_refused() {
        let twice = FormSchema { fields: vec![FormField::text("a", "A"), FormField::long_text("a", "A again")] };
        assert!(twice.check().unwrap_err().contains("two fields with the key 'a'"));
        let shown = FormSchema { fields: vec![FormField::display("a", "A", json!(1)).required()] };
        assert!(shown.check().unwrap_err().contains("only shows something"));
        let dangling = FormSchema { fields: vec![FormField::text("own", "Own").answering_for("gone")] };
        assert!(dangling.check().unwrap_err().contains("answers for 'gone'"));
        let itself = FormSchema { fields: vec![FormField::text("own", "Own").answering_for("own")] };
        assert!(itself.check().is_err(), "a field cannot answer for itself");
        let err = twice.answers(&json!({})).unwrap_err().to_string();
        assert!(err.contains("two fields"), "reading checks the schema too: {err}");
    }

    #[test]
    fn reading_the_wrong_shape_or_an_unknown_field_fails_naming_it() {
        let form = question();
        let answers = form.schema.answers(&json!({ "pick": 3, "allow": true })).unwrap();
        let err = answers.value::<String>("pick").unwrap_err().to_string();
        assert!(err.contains("'pick'"), "{err}");
        assert!(answers.value::<String>("allow").unwrap_err().to_string().contains("is a decision"));
        assert!(answers.decision("pick").unwrap_err().to_string().contains("not a decision"));
        assert!(answers.decision("history").unwrap_err().to_string().contains("no field 'history'"));
        let err = form.schema.answers(&json!(["not", "a", "record"])).unwrap_err().to_string();
        assert!(err.contains("a record keyed by field key"), "{err}");
    }
}
