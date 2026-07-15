//! Incremental SSE `data:` line extraction for response taps.
//!
//! A streaming tap sees the response as arbitrary byte chunks; SSE events are
//! newline-framed `data: <payload>` lines. This scanner reassembles complete
//! lines across chunk boundaries while buffering ONLY the current partial
//! line (never the whole stream), so a tap stays O(one line) in memory no
//! matter how long the stream runs.

/// Reassembles SSE `data:` payloads from a chunked byte stream.
pub struct DataLineScanner {
    partial: Vec<u8>,
    /// A single line grew past the cap and was dropped (with everything up
    /// to its terminating newline). Visible so a consumer can report the
    /// loss instead of silently mis-measuring.
    overflowed: bool,
    dropping_line: bool,
}

/// One line is more than enough for any usage/cost frame; a line beyond this
/// is a content frame the tap does not need. 1 MiB keeps a hostile or
/// pathological stream from ballooning the tap.
const MAX_LINE_BYTES: usize = 1024 * 1024;

impl DataLineScanner {
    pub fn new() -> Self {
        Self { partial: Vec::new(), overflowed: false, dropping_line: false }
    }

    /// Feed one chunk; `on_data` is called once per completed `data:`
    /// payload (whitespace-trimmed), in stream order.
    pub fn feed(&mut self, chunk: &[u8], mut on_data: impl FnMut(&str)) {
        for &byte in chunk {
            if byte == b'\n' {
                if self.dropping_line {
                    self.dropping_line = false;
                } else {
                    if let Some(payload) = data_payload(&self.partial) {
                        on_data(payload);
                    }
                    self.partial.clear();
                }
                continue;
            }
            if self.dropping_line {
                continue;
            }
            if self.partial.len() >= MAX_LINE_BYTES {
                self.overflowed = true;
                self.dropping_line = true;
                self.partial.clear();
                continue;
            }
            self.partial.push(byte);
        }
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }
}

impl Default for DataLineScanner {
    fn default() -> Self {
        Self::new()
    }
}

/// The payload of an SSE `data:` line, or `None` for any other line
/// (comments, `event:`, blanks). Tolerates the optional space after the
/// colon and a trailing `\r`.
fn data_payload(line: &[u8]) -> Option<&str> {
    let line = std::str::from_utf8(line).ok()?;
    let line = line.strip_suffix('\r').unwrap_or(line);
    let payload = line.strip_prefix("data:")?;
    Some(payload.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(chunks: &[&[u8]]) -> (Vec<String>, bool) {
        let mut scanner = DataLineScanner::new();
        let mut got = Vec::new();
        for chunk in chunks {
            scanner.feed(chunk, |payload| got.push(payload.to_string()));
        }
        (got, scanner.overflowed())
    }

    #[test]
    fn reassembles_data_lines_across_chunk_boundaries() {
        let (got, overflowed) = collect(&[
            b"data: {\"a\":", b" 1}\r\n\r\ndata:", b" [DONE]\n", b"event: x\ndata: {\"b\":2}\n",
        ]);
        assert_eq!(got, vec!["{\"a\": 1}", "[DONE]", "{\"b\":2}"]);
        assert!(!overflowed);
    }

    #[test]
    fn a_pathologically_long_line_is_dropped_and_flagged_without_buffering() {
        let mut scanner = DataLineScanner::new();
        let mut got = Vec::new();
        // Feed a 3 MiB line in pieces, then a normal line.
        let big = vec![b'x'; 1024 * 1024];
        scanner.feed(b"data: ", |p| got.push(p.to_string()));
        for _ in 0..3 {
            scanner.feed(&big, |p| got.push(p.to_string()));
        }
        scanner.feed(b"\ndata: ok\n", |p| got.push(p.to_string()));
        assert_eq!(got, vec!["ok"], "the oversized line never surfaces");
        assert!(scanner.overflowed());
    }
}
