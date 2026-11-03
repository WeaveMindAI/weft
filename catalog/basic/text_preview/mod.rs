//! TextPreview: what a reader (usually a model) should see of a text
//! that may be long. Its start, its end, or both with the middle cut,
//! within a budget of lines and characters, with a line in the cut's
//! place saying how much was cut.
//!
//! A stored file is read as it streams, so a file of gigabytes costs no
//! more memory than the budget: [`Preview`] keeps the text's first
//! `maxChars` characters (the whole text when it fits, the start's share
//! when it does not) and a window over the end it trims as it goes, and
//! counts everything else without holding it.

use async_trait::async_trait;
use futures::StreamExt;

use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Node, NodeManifest, RunContext, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct TextPreviewNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The most `maxChars` takes. A JSON string writes a control character
/// as six bytes (`\u0001`), the most any one character weighs, so the
/// kept text weighs at most 16 000 x 6 = 96 000 bytes, and the marker
/// line, the two line breaks around it and the quotes stay well inside
/// the 6 KB left of the 100 KB (`MAX_WIRE_VALUE_BYTES`) a wire carries.
/// The preview can therefore never be refused for its weight.
pub const MAX_CHARS_LIMIT: usize = 16_000;

/// Which part of the text the reader sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Keep {
    Start,
    End,
    Both,
}

/// An amount of text: so many lines and so many characters, whichever
/// runs out first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub lines: usize,
    pub chars: usize,
}

impl Budget {
    const NONE: Budget = Budget { lines: 0, chars: 0 };
}

/// The node's settings, checked.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Settings {
    pub keep: Keep,
    pub max: Budget,
    pub start_share: f64,
}

impl Settings {
    /// The settings as the node receives them. Their bounds are the
    /// metadata's widgets (whole numbers, `maxChars` up to
    /// [`MAX_CHARS_LIMIT`], `startShare` from 0 to 1), which the compiler
    /// holds a written value to and the runtime a wired one, so a value
    /// here is already within them.
    pub fn new(keep: Keep, max_lines: usize, max_chars: usize, start_share: f64) -> Self {
        Settings { keep, max: Budget { lines: max_lines, chars: max_chars }, start_share }
    }

    /// What the start and the end each get. For `both`, the start gets
    /// `startShare` of each budget, rounded down, and the end the rest.
    pub fn split(&self) -> (Budget, Budget) {
        match self.keep {
            Keep::Start => (self.max, Budget::NONE),
            Keep::End => (Budget::NONE, self.max),
            Keep::Both => {
                let share = |n: usize| ((n as f64 * self.start_share).floor() as usize).min(n);
                let head = Budget { lines: share(self.max.lines), chars: share(self.max.chars) };
                (head, Budget { lines: self.max.lines - head.lines, chars: self.max.chars - head.chars })
            }
        }
    }
}

/// What the node emits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shown {
    pub preview: String,
    pub cut: bool,
    pub lines: usize,
    pub chars: usize,
}

/// A text fed in pieces, finished into what the reader sees. Memory is
/// bounded by the budget whatever the text's length: the first
/// `maxChars` characters, and a window over the end that never holds
/// more than twice the end's share of characters (plus the piece being
/// fed).
pub struct Preview {
    max: Budget,
    head: Budget,
    tail_budget: Budget,
    /// The text's first `max.chars` characters: the whole text when it
    /// fits the budget, and the start's share is cut from it when not.
    prefix: String,
    prefix_chars: usize,
    /// A suffix of the text fed so far, always holding the end's share
    /// of it (the last `tail_budget.lines` lines, at most
    /// `tail_budget.chars` characters of them), trimmed down to exactly
    /// that once it holds twice as many characters. Trimming early never
    /// loses anything the end needs: the end's share of a longer text
    /// starts no earlier than the end's share of its start.
    tail: String,
    tail_chars: usize,
    chars: usize,
    breaks: usize,
    ends_with_break: bool,
}

impl Preview {
    pub fn new(settings: &Settings) -> Self {
        let (head, tail_budget) = settings.split();
        Preview {
            max: settings.max,
            head,
            tail_budget,
            prefix: String::new(),
            prefix_chars: 0,
            tail: String::new(),
            tail_chars: 0,
            chars: 0,
            breaks: 0,
            ends_with_break: false,
        }
    }

    /// Feed the next piece of the text.
    pub fn push(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        let n = text.chars().count();
        self.chars += n;
        self.breaks += text.bytes().filter(|&b| b == b'\n').count();
        self.ends_with_break = text.ends_with('\n');

        let room = self.max.chars - self.prefix_chars;
        if room > 0 {
            self.prefix.push_str(first_chars(text, room));
            self.prefix_chars += n.min(room);
        }

        let window = self.tail_budget.chars;
        if window == 0 || self.tail_budget.lines == 0 {
            return;
        }
        if n >= window {
            // This piece alone holds more than the end can show.
            self.tail.clear();
            self.tail.push_str(last_chars(text, window));
            self.tail_chars = window;
        } else {
            self.tail.push_str(text);
            self.tail_chars += n;
            if self.tail_chars > 2 * window {
                self.trim_tail();
            }
        }
    }

    fn trim_tail(&mut self) {
        let start = end_start(&self.tail, self.tail_budget);
        self.tail_chars -= self.tail[..start].chars().count();
        self.tail.drain(..start);
    }

    /// The preview and the counts of the whole text.
    pub fn finish(mut self) -> Shown {
        let chars = self.chars;
        let lines = self.breaks + usize::from(chars > 0 && !self.ends_with_break);
        if chars == 0 {
            return Shown { preview: "(empty)".into(), cut: false, lines: 0, chars: 0 };
        }
        if lines <= self.max.lines && chars <= self.max.chars {
            return Shown { preview: self.prefix, cut: false, lines, chars };
        }
        self.trim_tail();
        let head = first_part(&self.prefix, self.head);
        let tail = self.tail.as_str();
        let head_chars = head.chars().count();
        // The text does not fit, so its start and its end are apart:
        // over on characters, the two shares add up to fewer than the
        // text holds; over on lines, they hold fewer lines than it has.
        debug_assert!(head_chars + self.tail_chars < chars, "the start and the end overlap");

        // Lines the preview shows any part of, the start's and the end's
        // counted once when both show a piece of the same long line.
        let head_lines = line_count(head);
        let tail_breaks = tail.bytes().filter(|&b| b == b'\n').count();
        let first_tail_line = self.breaks - tail_breaks;
        let shown_lines = match (head.is_empty(), tail.is_empty()) {
            (_, true) => head_lines,
            (true, false) => lines - first_tail_line,
            (false, false) => head_lines + lines - first_tail_line.max(head_lines),
        };
        let from = match (head.is_empty(), tail.is_empty()) {
            (true, _) => "start",
            (_, true) => "end",
            _ => "middle",
        };
        let marker = format!(
            "[... {} of {lines} lines cut ({} of {chars} characters), from the {from} ...]",
            lines - shown_lines,
            chars - head_chars - self.tail_chars,
        );

        let mut preview = String::with_capacity(head.len() + marker.len() + tail.len() + 2);
        if !head.is_empty() {
            preview.push_str(head);
            if !head.ends_with('\n') {
                preview.push('\n');
            }
        }
        preview.push_str(&marker);
        if !tail.is_empty() {
            preview.push('\n');
            preview.push_str(tail);
        }
        Shown { preview, cut: true, lines, chars }
    }
}

/// How many lines `text` holds: one per line break, and the last line
/// counts without one.
fn line_count(text: &str) -> usize {
    text.bytes().filter(|&b| b == b'\n').count() + usize::from(!text.is_empty() && !text.ends_with('\n'))
}

/// The first `n` characters of `text`.
fn first_chars(text: &str, n: usize) -> &str {
    match text.char_indices().nth(n) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}

/// The last `n` characters of `text`.
fn last_chars(text: &str, n: usize) -> &str {
    if n == 0 {
        return "";
    }
    match text.char_indices().rev().nth(n - 1) {
        Some((i, _)) => &text[i..],
        None => text,
    }
}

/// The start of `text` within `budget`: whole lines while they fit,
/// and the line where the characters run out cut where they do.
fn first_part(text: &str, budget: Budget) -> &str {
    let (mut end, mut taken, mut done_lines) = (0, 0, 0);
    for (i, c) in text.char_indices() {
        if taken == budget.chars || done_lines == budget.lines {
            break;
        }
        taken += 1;
        end = i + c.len_utf8();
        if c == '\n' {
            done_lines += 1;
        }
    }
    &text[..end]
}

/// Where the end of `text` within `budget` starts (a byte index): the
/// later of the start of its last `budget.lines` lines and the start of
/// its last `budget.chars` characters. A text that starts mid-line (a
/// window over a longer one) holds fewer lines than asked, and starts
/// at 0.
fn end_start(text: &str, budget: Budget) -> usize {
    if budget.lines == 0 || budget.chars == 0 {
        return text.len();
    }
    let by_chars = text.len() - last_chars(text, budget.chars).len();
    // A line's own break belongs to it, so a final break starts no line.
    let body = text.strip_suffix('\n').unwrap_or(text);
    let by_lines = body.rmatch_indices('\n').nth(budget.lines - 1).map_or(0, |(i, _)| i + 1);
    by_chars.max(by_lines)
}

/// Bytes in, UTF-8 text out, a piece at a time: a character split
/// between two pieces is held until the next one completes it, and a
/// byte that is no UTF-8 reads as U+FFFD, the same as
/// `String::from_utf8_lossy` on the whole.
#[derive(Debug, Default)]
pub struct Utf8Decoder {
    pending: Vec<u8>,
}

impl Utf8Decoder {
    /// The text the bytes so far complete.
    pub fn push(&mut self, bytes: &[u8]) -> String {
        let joined;
        let input: &[u8] = if self.pending.is_empty() {
            bytes
        } else {
            joined = [std::mem::take(&mut self.pending).as_slice(), bytes].concat();
            &joined
        };
        let mut out = String::with_capacity(input.len());
        let mut rest = input;
        loop {
            match std::str::from_utf8(rest) {
                Ok(text) => {
                    out.push_str(text);
                    return out;
                }
                Err(e) => {
                    let (good, bad) = rest.split_at(e.valid_up_to());
                    out.push_str(std::str::from_utf8(good).expect("valid up to here"));
                    match e.error_len() {
                        Some(n) => {
                            out.push(char::REPLACEMENT_CHARACTER);
                            rest = &bad[n..];
                        }
                        // A character the next piece may complete.
                        None => {
                            self.pending = bad.to_vec();
                            return out;
                        }
                    }
                }
            }
        }
    }

    /// What is left once the bytes end: a character they never
    /// completed reads as U+FFFD.
    pub fn finish(self) -> String {
        String::from_utf8_lossy(&self.pending).into_owned()
    }
}

/// Feed a stored file into `preview` as it streams.
async fn read_file(ctx: &RunContext, file: &FileHandle, preview: &mut Preview) -> WeftResult<()> {
    let (_meta, mut stream) = ctx.storage(StorageScope::Run).get(file).await?;
    let mut decoder = Utf8Decoder::default();
    while let Some(chunk) = stream.next().await {
        // A read the run's cancel ended unwinds as cancelled.
        let chunk = chunk.map_err(|e| match weft::cancellation::is_cancel(&e) {
            true => WeftError::Cancelled,
            false => weft::node_error(format!("reading the file: {e}")),
        })?;
        preview.push(&decoder.push(&chunk));
    }
    preview.push(&decoder.finish());
    Ok(())
}

#[async_trait]
impl Node for TextPreviewNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let settings = Settings::new(
            ctx.inputs.get("keep")?,
            ctx.inputs.get("maxLines")?,
            ctx.inputs.get("maxChars")?,
            ctx.inputs.get("startShare")?,
        );
        // Exactly one of the two arrives: `oneOfRequired` skips the node
        // when neither does, and the metadata refuses both.
        let mut preview = Preview::new(&settings);
        match ctx.inputs.opt::<String>("text")? {
            Some(text) => preview.push(&text),
            None => read_file(&ctx, &ctx.inputs.get::<FileHandle>("file")?, &mut preview).await?,
        }
        let shown = preview.finish();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("preview", shown.preview)
                .set("cut", shown.cut)
                .set("lines", shown.lines as u64)
                .set("chars", shown.chars as u64),
        )
        .await
    }
}
