//! The words a finished run is found by: every string and number its
//! record holds (never a field's name), in the order recorded, each value
//! cut to [`MAX_VALUE`] characters, up to [`MAX_TEXT`] in all. Built behind
//! the writes, by the dispatcher's search loop, from the run's record.

use crate::events::ExecEvent;

/// How much of a run's text its document keeps, in characters, and how
/// much of any one value. A document holds each word once with its
/// positions and Postgres refuses one past a megabyte: at four bytes a
/// character, every word distinct and each hyphenated one also kept in
/// parts, this many characters stays well under it.
pub const MAX_TEXT: usize = 50_000;
pub const MAX_VALUE: usize = 2_000;

/// A run's words as its events are written (see the module doc).
#[derive(Debug, Clone, Default)]
pub struct SearchText {
    text: String,
    /// Characters in `text`.
    chars: usize,
}

impl SearchText {
    /// Add `event`'s values, in order.
    pub fn push(&mut self, event: &ExecEvent) {
        if self.full() {
            return;
        }
        let Ok(value) = serde_json::to_value(event) else { return };
        self.push_value(&value);
    }

    fn full(&self) -> bool {
        self.chars >= MAX_TEXT
    }

    fn push_value(&mut self, value: &serde_json::Value) {
        if self.full() {
            return;
        }
        match value {
            serde_json::Value::String(text) => self.push_word(text),
            serde_json::Value::Number(number) => self.push_word(&number.to_string()),
            serde_json::Value::Array(items) => items.iter().for_each(|item| self.push_value(item)),
            serde_json::Value::Object(fields) => fields.values().for_each(|field| self.push_value(field)),
            serde_json::Value::Bool(_) | serde_json::Value::Null => {}
        }
    }

    fn push_word(&mut self, value: &str) {
        if !self.text.is_empty() {
            self.text.push(' ');
            self.chars += 1;
        }
        let room = MAX_TEXT.saturating_sub(self.chars).min(MAX_VALUE);
        let cut: String = value.chars().take(room).collect();
        self.chars += cut.chars().count();
        self.text.push_str(&cut);
    }

    /// The finished text.
    pub fn into_text(self) -> String {
        self.text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_runs_words_are_its_values_cut_to_the_caps() {
        let execution_id = weft_core::ExecutionId::from_u128(7);
        let mut search = SearchText::default();
        search.push(&ExecEvent::LogLine {
            execution_id,
            node_id: "mailer".into(),
            frames: vec![],
            level: "info".into(),
            message: "sent to ada@example.com".into(),
            at_unix_ms: None,
            seq: Some(3),
            at_unix: 9,
        });
        let text = search.into_text();
        assert!(text.contains("ada@example.com"), "{text}");
        assert!(text.contains("mailer"));
        assert!(!text.contains("message"), "never a field's name: {text}");
        let mut long = SearchText::default();
        for _ in 0..100 {
            long.push(&ExecEvent::LogLine {
                execution_id,
                node_id: "n".into(),
                frames: vec![],
                level: "info".into(),
                message: "x".repeat(MAX_VALUE * 2),
                at_unix_ms: None,
                seq: None,
                at_unix: 0,
            });
        }
        let text = long.into_text();
        assert!(text.chars().count() <= MAX_TEXT, "{}", text.chars().count());
        assert!(!text.split(' ').any(|word| word.chars().count() > MAX_VALUE));
    }
}
