//! The readable record of one change to a stored file: what the
//! inspector shows for an edit, never anything the runtime rebuilds
//! from. A text file gets the changed lines with a few lines of context
//! around each change; any other file gets its old and new size. Either
//! way the record is cut to [`MAX_DIFF_BYTES`], at line boundaries, so
//! what is kept still reads on its own.

use super::MAX_WIRE_VALUE_BYTES;

/// The most one diff weighs, whatever the file: the same cap as any
/// value, since it rides the journal like one.
pub const MAX_DIFF_BYTES: usize = MAX_WIRE_VALUE_BYTES;

/// Unchanged lines shown around each change.
const CONTEXT_LINES: usize = 3;

/// Room kept free at the end for the line saying how much was cut.
const NOTE_ROOM: usize = 64;

/// Whether a file of this type is lines of text a person reads.
pub fn is_text_mime(mime: &str) -> bool {
    let essence = mime.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    essence.starts_with("text/")
        || essence.ends_with("+json")
        || essence.ends_with("+xml")
        || matches!(
            essence.as_str(),
            "application/json"
                | "application/x-ndjson"
                | "application/ndjson"
                | "application/jsonl"
                | "application/xml"
                | "application/csv"
                | "application/yaml"
                | "application/x-yaml"
        )
}

/// What changed between `old` and `new`, the content of a file of type
/// `mime`, as a person reads it, at most [`MAX_DIFF_BYTES`].
pub fn edit_diff(mime: &str, old: &[u8], new: &[u8]) -> String {
    match (is_text_mime(mime), std::str::from_utf8(old), std::str::from_utf8(new)) {
        (true, Ok(old), Ok(new)) => text_diff(old, new, MAX_DIFF_BYTES),
        _ => size_change(old.len() as u64, new.len() as u64),
    }
}

/// The record of a file whose content was written without being read
/// first: only the new size is known.
pub fn overwrite_note(new_size: u64) -> String {
    format!("overwritten without reading it first: now {}", human_size(new_size))
}

fn size_change(old: u64, new: u64) -> String {
    format!("replaced: {} → {}", human_size(old), human_size(new))
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut size = bytes as f64 / 1024.0;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", UNITS[unit])
}

/// One change block: its `@@` header and lines, and how many of those
/// lines are changes (not context).
struct Hunk {
    lines: Vec<String>,
    changed: usize,
}

fn text_diff(old: &str, new: &str, cap: usize) -> String {
    let diff = similar::TextDiff::from_lines(old, new);
    let hunks: Vec<Hunk> = diff
        .grouped_ops(CONTEXT_LINES)
        .iter()
        .map(|ops| {
            let (first, last) = (&ops[0], &ops[ops.len() - 1]);
            let (old_start, old_end) = (first.old_range().start, last.old_range().end);
            let (new_start, new_end) = (first.new_range().start, last.new_range().end);
            let mut lines = vec![format!(
                "@@ -{},{} +{},{} @@",
                old_start + 1,
                old_end - old_start,
                new_start + 1,
                new_end - new_start
            )];
            let mut changed = 0;
            for op in ops {
                for change in diff.iter_changes(op) {
                    let sign = match change.tag() {
                        similar::ChangeTag::Equal => ' ',
                        similar::ChangeTag::Delete => '-',
                        similar::ChangeTag::Insert => '+',
                    };
                    if sign != ' ' {
                        changed += 1;
                    }
                    let text = change.value().trim_end_matches(['\n', '\r']);
                    lines.push(format!("{sign}{text}"));
                }
            }
            Hunk { lines, changed }
        })
        .collect();
    assemble(&hunks, cap)
}

/// Whole blocks from the start while they fit; the first block alone is
/// cut line by line (and a single line too long for what is left, by
/// characters) so something always reads. One closing line counts the
/// changed lines left out.
fn assemble(hunks: &[Hunk], cap: usize) -> String {
    let total: usize = hunks.iter().map(|h| h.changed).sum();
    let budget = cap.saturating_sub(NOTE_ROOM);
    let mut out = String::new();
    let mut shown = 0;
    for hunk in hunks {
        let size: usize = hunk.lines.iter().map(|l| l.len() + 1).sum();
        if out.len() + size <= budget {
            for line in &hunk.lines {
                out.push_str(line);
                out.push('\n');
            }
            shown += hunk.changed;
            continue;
        }
        if out.is_empty() {
            for line in &hunk.lines {
                let room = budget - out.len();
                let is_change = line.starts_with(['+', '-']);
                if line.len() < room {
                    out.push_str(line);
                    out.push('\n');
                } else {
                    out.push_str(&cut_line(line, room));
                    out.push('\n');
                    shown += usize::from(is_change);
                    break;
                }
                shown += usize::from(is_change);
            }
        }
        break;
    }
    let hidden = total - shown;
    if hidden > 0 {
        out.push_str(&format!("... {hidden} more changed lines not shown\n"));
    }
    out
}

/// `line` cut to fit `room` bytes (its newline included), ending on how
/// many characters were left out.
fn cut_line(line: &str, room: usize) -> String {
    const MARKER_ROOM: usize = 40;
    let keep = room.saturating_sub(MARKER_ROOM + 1);
    let end = line.char_indices().map(|(i, _)| i).take_while(|&i| i <= keep).last().unwrap_or(0);
    let left = line[end..].chars().count();
    format!("{}... {left} more characters", &line[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_small_change_shows_its_lines_with_context() {
        let old = "a\nb\nc\nd\ne\nf\ng\n";
        let new = "a\nb\nc\nD\ne\nf\ng\n";
        let diff = edit_diff("text/plain", old.as_bytes(), new.as_bytes());
        assert_eq!(diff, "@@ -1,7 +1,7 @@\n a\n b\n c\n-d\n+D\n e\n f\n g\n");
    }

    #[test]
    fn an_append_to_a_json_lines_file_reads_as_added_lines() {
        let old = "{\"role\":\"user\"}\n";
        let new = "{\"role\":\"user\"}\n{\"role\":\"assistant\"}\n";
        let diff = edit_diff("application/x-ndjson", old.as_bytes(), new.as_bytes());
        assert!(diff.contains("+{\"role\":\"assistant\"}"), "{diff}");
        assert!(diff.contains(" {\"role\":\"user\"}"), "{diff}");
    }

    #[test]
    fn a_large_change_is_cut_at_whole_blocks_and_counts_what_is_left_out() {
        // Changes far apart make separate blocks.
        let old: String = (0..400).map(|i| format!("line {i}\n")).collect();
        let new: String = (0..400)
            .map(|i| if i % 20 == 0 { format!("changed {i}\n") } else { format!("line {i}\n") })
            .collect();
        let full = text_diff(&old, &new, usize::MAX);
        assert!(!full.contains("not shown"));
        let cut = text_diff(&old, &new, 400);
        assert!(cut.len() <= 400, "{}", cut.len());
        assert!(cut.starts_with("@@ "), "{cut}");
        assert!(cut.ends_with("more changed lines not shown\n"), "{cut}");
        // Only whole blocks before the note.
        let body = cut.rsplit_once("... ").unwrap().0;
        assert!(full.starts_with(body), "a kept block is never cut short:\n{cut}");
    }

    #[test]
    fn one_line_longer_than_the_cap_is_cut_with_its_own_marker() {
        let new = format!("{}\n", "x".repeat(10_000));
        let cut = text_diff("", &new, 300);
        assert!(cut.len() <= 300, "{}", cut.len());
        assert!(cut.contains("more characters"), "{cut}");
        assert!(!cut.contains("not shown"), "the one changed line is shown, cut: {cut}");
    }

    #[test]
    fn a_line_is_cut_on_a_character_boundary() {
        let cut = cut_line(&"é".repeat(100), 60);
        assert!(cut.contains("more characters"));
    }

    #[test]
    fn a_binary_file_reads_as_its_sizes() {
        let diff = edit_diff("image/png", &[0u8; 2_200_000], &[0u8; 2_400_000]);
        assert_eq!(diff, "replaced: 2.1 MB → 2.3 MB");
        // Text that is not UTF-8 is read as bytes.
        assert_eq!(edit_diff("text/plain", &[0xff], &[0xfe, 0xff]), "replaced: 1 B → 2 B");
    }

    #[test]
    fn text_types_are_recognized() {
        for mime in ["text/csv", "application/json; charset=utf-8", "application/ld+json", "application/xml"] {
            assert!(is_text_mime(mime), "{mime}");
        }
        assert!(!is_text_mime("application/octet-stream"));
    }
}
