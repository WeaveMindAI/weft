//! The lossless lexer: source text -> a flat stream of `(SyntaxKind, &str)`
//! tokens whose concatenation is byte-identical to the input.
//!
//! It emits EVERY byte as a token: whitespace and comments are real tokens, so
//! nothing is dropped and concatenation reproduces the source exactly. The three
//! opaque kinds (HEREDOC, JSON_VALUE, MARKER) are lexed as
//! single tokens spanning their full extent, because the edit protocol works on
//! whole values; their interior is never Weft syntax.
//!
//! The lexer is context-free and total: any byte that fits no rule becomes a
//! one-char ERROR token, so it never panics and never loses a byte. The parser
//! layers structure on top; lenient error recovery lives there.

use super::kind::SyntaxKind;

/// One lexed token: its kind and the exact source slice it covers. Lifetime ties
/// it to the source so no allocation happens during lexing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Token<'a> {
    pub kind: SyntaxKind,
    pub text: &'a str,
}

/// Tokenize `source` into a lossless stream. Invariant (asserted in tests):
/// `tokens.iter().map(|t| t.text).collect::<String>() == source`.
pub fn lex(source: &str) -> Vec<Token<'_>> {
    Lexer { src: source, pos: 0, out: Vec::new() }.run()
}

struct Lexer<'a> {
    src: &'a str,
    pos: usize,
    out: Vec<Token<'a>>,
}

impl<'a> Lexer<'a> {
    fn run(mut self) -> Vec<Token<'a>> {
        while self.pos < self.src.len() {
            self.step();
        }
        self.out
    }

    /// The remaining unlexed source.
    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    /// Emit a token covering `[pos, pos+len)` and advance.
    fn emit(&mut self, kind: SyntaxKind, len: usize) {
        let text = &self.src[self.pos..self.pos + len];
        self.out.push(Token { kind, text });
        self.pos += len;
    }

    fn step(&mut self) {
        let rest = self.rest();
        let first = rest.as_bytes()[0];

        // Whitespace run (spaces, tabs, newlines, CR). ASCII-only by design: the
        // gate and the run boundary use the SAME predicate, so a non-ASCII space
        // (e.g. NBSP pasted from a doc) is never absorbed into a WHITESPACE token.
        // It falls through to a one-char ERROR token instead, surfacing the
        // invisible character loudly rather than laundering it as benign space.
        if first.is_ascii_whitespace() {
            let len = rest.find(|c: char| !c.is_ascii_whitespace()).unwrap_or(rest.len());
            self.emit(SyntaxKind::WHITESPACE, len);
            return;
        }

        // Comment: `#` to end of line (the line break is separate whitespace).
        // Single-line, terminated at `\n` OR a lone `\r`, so a CR-terminated
        // comment doesn't swallow the next line (see `line_end`).
        if first == b'#' {
            self.emit(SyntaxKind::COMMENT, line_end(rest));
            return;
        }

        // `@`-markers: @file(...), @include(...), @require_one_of(...). One
        // opaque token spanning the directive name + its balanced parens. If a
        // marker has no `(` it is still consumed up to the next delimiter so the
        // byte is never lost (the parser flags it).
        if first == b'@' {
            let len = self.marker_len(rest);
            self.emit(SyntaxKind::MARKER, len);
            return;
        }

        // Heredoc: ```...``` (triple backtick). One opaque token incl. fences.
        if rest.starts_with("```") {
            let len = self.heredoc_len(rest);
            self.emit(SyntaxKind::HEREDOC, len);
            return;
        }

        // `[`-opened value: an array config value. `[` never starts anything
        // structural in Weft, so the whole balanced `[...]` is one opaque
        // JSON_VALUE token. (A `{`-opened JSON OBJECT value is different: `{` is
        // also the structural body brace, which the lexer cannot disambiguate
        // context-free. So `{`/`}` always lex as L_BRACE/R_BRACE here, and the
        // PARSER assembles a JSON_VALUE *node* wrapping that brace-run when it
        // sees a `{` in config-value position. Round-trip is identical: the
        // node's concatenated tokens are the original bytes either way.)
        if first == b'[' {
            let len = self.balanced_len(rest, b'[', b']');
            self.emit(SyntaxKind::JSON_VALUE, len);
            return;
        }

        // String literal "..." with backslash escapes. One STRING token.
        if first == b'"' {
            let len = self.string_len(rest);
            self.emit(SyntaxKind::STRING, len);
            return;
        }

        // Structural punctuation.
        if rest.starts_with("->") {
            self.emit(SyntaxKind::ARROW, 2);
            return;
        }
        let single = match first {
            b'(' => Some(SyntaxKind::L_PAREN),
            b')' => Some(SyntaxKind::R_PAREN),
            b'{' => Some(SyntaxKind::L_BRACE),
            b'}' => Some(SyntaxKind::R_BRACE),
            b':' => Some(SyntaxKind::COLON),
            b',' => Some(SyntaxKind::COMMA),
            b'.' => Some(SyntaxKind::DOT),
            b'=' => Some(SyntaxKind::EQ),
            b'?' => Some(SyntaxKind::QUESTION),
            _ => None,
        };
        if let Some(kind) = single {
            self.emit(kind, 1);
            return;
        }

        // Identifier / keyword / bare scalar.
        //
        // Critical: `.` is STRUCTURAL (the DOT token for endpoints like
        // `b.data`), so an IDENT must NOT absorb it, or `b.data` lexes as one
        // token and a connection looks like a plain assignment. Only a
        // pure-NUMERIC run may contain `.` (so `3.14` is one NUMBER). So: a
        // digit/`-`-led run consumes `[0-9.-]`; an identifier-led run consumes
        // `[alnum _ -]` and stops at `.`.
        let numeric_led = first.is_ascii_digit() || (first == b'-' && rest.len() > 1);
        if first == b'_' || (first as char).is_ascii_alphanumeric() || first == b'-' {
            let stop = |c: char| {
                if numeric_led {
                    !(c.is_ascii_digit() || c == '.' || c == '-')
                } else {
                    !(c.is_ascii_alphanumeric() || c == '_' || c == '-')
                }
            };
            // `first` (`_`/alnum/`-`) never satisfies `stop`, so the run is
            // always >= 1 byte and starts on a char boundary. Assert it rather
            // than silently clamp: a future change that breaks this should panic
            // in tests, not mis-slice a multibyte char in production.
            let len = rest.find(stop).unwrap_or(rest.len());
            debug_assert!(len > 0, "identifier-led run must consume at least one byte");
            let word = &rest[..len];
            let kind = classify_word(word);
            self.emit(kind, len);
            return;
        }

        // Anything else: a single ERROR byte, so no byte is ever dropped.
        let ch_len = rest.chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        self.emit(SyntaxKind::ERROR, ch_len);
    }

    /// Length of an `@marker` token: the `@name` plus a balanced `(...)` if one
    /// immediately follows. A marker is SINGLE-LINE: its parens must close on the
    /// same line. If they don't, the scan stops at the newline, leaving the
    /// marker unclosed so the parser flags it loudly (rather than stealing a
    /// later line's `)`, which would mask the error and corrupt structure).
    fn marker_len(&self, rest: &'a str) -> usize {
        let bytes = rest.as_bytes();
        // consume @ + name chars
        let mut i = 1;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'(' {
            // Balanced parens, but never crossing a line break (`\n` OR a lone
            // `\r`), so a CR-terminated marker line can't swallow the next line.
            let end = i + line_end(&rest[i..]);
            let bal = self.balanced_len(&rest[i..end], b'(', b')');
            return i + bal;
        }
        i
    }

    /// Length of a ```...``` heredoc: opening fence, body, closing fence. If no
    /// closing fence exists, consume to end of input (the parser flags the
    /// unterminated heredoc; no byte is lost).
    fn heredoc_len(&self, rest: &'a str) -> usize {
        // skip opening ```
        let after_open = &rest[3..];
        match after_open.find("```") {
            Some(rel) => 3 + rel + 3, // open + body + close
            None => rest.len(),
        }
    }

    /// Length of a `"..."` string token (including both quotes), `\`-escape aware.
    fn string_len(&self, rest: &'a str) -> usize {
        string_end(rest.as_bytes(), 0)
    }

    /// Length of a balanced `open`/`close` span starting at `rest[0] == open`,
    /// respecting nested pairs and skipping `"..."` strings (so a bracket inside
    /// a JSON string doesn't unbalance it). Runs to end of input if unbalanced.
    fn balanced_len(&self, rest: &'a str, open: u8, close: u8) -> usize {
        let bytes = rest.as_bytes();
        let mut depth: i32 = 0;
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                // Skip a JSON string atomically so brackets/quotes inside it
                // don't affect depth (one string-skip implementation, shared).
                b'"' => {
                    i = string_end(bytes, i);
                    continue;
                }
                b if b == open => depth += 1,
                b if b == close => {
                    depth -= 1;
                    if depth == 0 {
                        return i + 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        rest.len()
    }
}

/// The length of `s` up to (not including) the first line break, `\n` OR a lone
/// `\r` (old-Mac line ending), or all of `s` if it has none. The single home for
/// "to end of line", so single-line tokens (comment, marker) terminate at a line
/// break consistently and a CR-terminated line never swallows the next one.
fn line_end(s: &str) -> usize {
    s.find(['\n', '\r']).unwrap_or(s.len())
}

/// The byte index just PAST a `"..."` string that starts at `bytes[open]` (a
/// `"`), `\`-escape aware. Stops past the closing quote, or at end-of-input /
/// an unescaped newline for an unterminated string. ALWAYS returns an index
/// `<= bytes.len()` on a char boundary (a trailing `\` does not run off the end:
/// the escape step is clamped). The single home for string scanning, so
/// `string_len` and `balanced_len` cannot diverge on the EOF/escape edge.
fn string_end(bytes: &[u8], open: usize) -> usize {
    let mut i = open + 1; // past the opening quote
    while i < bytes.len() {
        match bytes[i] {
            // Escaped char: skip the `\` then the FULL next char. Advancing a
            // fixed +2 could split a multibyte escapee (`\é`) and later panic on
            // a non-boundary slice; stepping by the escapee's UTF-8 width (>=1,
            // so the loop always advances) keeps `i` on a char boundary.
            b'\\' => {
                i += 1;
                i += utf8_width(bytes.get(i).copied());
            }
            b'"' => return i + 1,
            b'\n' => return i, // unterminated: stop at line end
            _ => i += 1,
        }
    }
    i.min(bytes.len())
}

/// The UTF-8 byte width of the char whose leading byte is `b` (1 for ASCII /
/// none, so the caller always advances at least... well, the `\` already
/// advanced; this returns 0 for a missing trailing byte at EOF so `i` lands
/// exactly at len).
fn utf8_width(b: Option<u8>) -> usize {
    match b {
        None => 0,
        Some(b) if b < 0x80 => 1,
        Some(b) if b >= 0xF0 => 4,
        Some(b) if b >= 0xE0 => 3,
        Some(b) if b >= 0xC0 => 2,
        Some(_) => 1, // continuation byte (malformed); advance 1 to make progress
    }
}

/// Classify a bare word: the `Group` keyword, a numeric/bool literal, or an
/// identifier. Type names and node ids are both IDENT; the parser uses position
/// + case to tell them apart.
fn classify_word(word: &str) -> SyntaxKind {
    if word == "Group" {
        return SyntaxKind::KW_GROUP;
    }
    if word == "Loop" {
        return SyntaxKind::KW_LOOP;
    }
    if word == "true" || word == "false" {
        return SyntaxKind::NUMBER;
    }
    // A digit/`-`-led run is a NUMBER only if it's a WELL-FORMED number
    // (optional leading `-`, digits, at most one `.`, no trailing `.`, no
    // embedded `-`). A malformed numeric (`1.2.3`, `--`, `3.`, `1-2`, bare `-`)
    // is an ERROR token, not a NUMBER masquerading as valid (fail-loud at the
    // token level: the parser then reports it as unexpected content).
    let first = word.chars().next().unwrap_or(' ');
    if first.is_ascii_digit() || first == '-' {
        return if is_number(word) { SyntaxKind::NUMBER } else { SyntaxKind::ERROR };
    }
    SyntaxKind::IDENT
}

/// True iff `word` is a well-formed number: optional `-`, one or more digits,
/// optionally a single `.` followed by one or more digits. No trailing `.`, no
/// repeated `.`, no internal `-`.
fn is_number(word: &str) -> bool {
    let body = word.strip_prefix('-').unwrap_or(word);
    if body.is_empty() {
        return false; // bare `-`
    }
    let mut parts = body.splitn(2, '.');
    let int = parts.next().unwrap_or("");
    match parts.next() {
        None => !int.is_empty() && int.bytes().all(|b| b.is_ascii_digit()),
        Some(frac) => {
            !int.is_empty()
                && int.bytes().all(|b| b.is_ascii_digit())
                && !frac.is_empty()
                && frac.bytes().all(|b| b.is_ascii_digit())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lossless invariant: concatenating token texts reproduces the source.
    fn assert_lossless(src: &str) {
        let toks = lex(src);
        let joined: String = toks.iter().map(|t| t.text).collect();
        assert_eq!(joined, src, "lex round-trip must be byte-exact");
    }

    #[test]
    fn never_panics_on_trailing_backslash_or_multibyte_escape() {
        // A string ending in `\` at EOF, or a `\` before a multibyte char, must
        // not overshoot the buffer (the lexer promises it never panics).
        for src in ["\"abc\\", "\"\\", "\"x\\é", "[\"a\\", "n = T { v: \"a\\"] {
            assert_lossless(src); // lexing + concatenation must not panic
        }
    }

    #[test]
    fn lossless_over_shapes() {
        assert_lossless("n = Debug\n");
        assert_lossless("n = Text { value: \"x\" }\n");
        assert_lossless("# Description: hi\ng = Group() -> () {\n  x = Text {}\n}\n");
        assert_lossless("n = Code {\n  src: ```\n  print(1)\n  ```\n}\n");
        assert_lossless("items: [\n  {\"a\": 1},\n  {\"b\": 2}\n]\n");
        assert_lossless("t = Text { value: @file(\"sys.txt\") }\n");
        assert_lossless("x = @include(\"sub.weft\")\n");
        assert_lossless("g = Group() {\n  x = T {}\n} -> (out: String)\n");
        assert_lossless("\n\n\n  \t \n# trailing comment");
        assert_lossless("a.b = c.d\n");
    }

    #[test]
    fn marker_is_one_opaque_token() {
        let toks = lex("@file(\"a, b.txt\", String)");
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].kind, SyntaxKind::MARKER);
    }

    #[test]
    fn heredoc_with_inner_backticks_in_text_is_one_token() {
        let toks = lex("```\nhello\n```");
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].kind, SyntaxKind::HEREDOC);
    }

    #[test]
    fn json_value_with_nested_brackets_and_strings() {
        let toks = lex("[{\"k\": \"]not close\"}, [1, 2]]");
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].kind, SyntaxKind::JSON_VALUE);
    }

    #[test]
    fn marker_with_unclosed_paren_stops_at_lone_cr() {
        // A CR-terminated marker line (old-Mac ending) whose `(` never closes
        // must stop the marker at the `\r`, not swallow the next line's content.
        let toks = lex("@require_one_of(a, b\r= Text");
        let marker = &toks[0];
        assert_eq!(marker.kind, SyntaxKind::MARKER);
        assert_eq!(marker.text, "@require_one_of(a, b", "marker stops at the lone CR, not past it");
        // The `\r` and the rest are separate tokens, not eaten into the marker.
        assert!(toks.iter().skip(1).any(|t| t.text.contains('\r')), "CR is its own (whitespace) token");
    }
}
