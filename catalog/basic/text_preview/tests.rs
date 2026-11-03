//! TextPreview self-tests: what each mode keeps, which limit wins, the
//! start and the end never overlapping, a single long line, an empty
//! text, an exact fit, a text fed in pieces (characters split across
//! pieces included), the wire bound at the largest `maxChars`, and the
//! node driven with a text and with a stored file. Both inputs given is
//! the metadata's to refuse, and neither the engine's to skip, neither of
//! which the rig runs.

use serde_json::json;

use weft::storage::{wire_weight, MAX_WIRE_VALUE_BYTES};
use weft::{FakeRig, NodeTest, WeftResult};

use super::{Keep, Preview, Settings, Shown, TextPreviewNode, Utf8Decoder, MAX_CHARS_LIMIT};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("each_mode_keeps_its_part_and_says_what_was_cut", modes),
        NodeTest::basic("the_character_limit_wins_when_it_runs_out_first", chars_win),
        NodeTest::basic("many_short_lines_keep_the_start_and_the_end_apart", no_overlap),
        NodeTest::basic("a_single_long_line_is_cut_too", long_line),
        NodeTest::basic("an_empty_text_previews_as_empty", empty),
        NodeTest::basic("a_text_that_fits_exactly_is_shown_whole", exact_fit),
        NodeTest::basic("a_last_line_without_a_break_counts", line_counting),
        NodeTest::basic("a_text_fed_in_pieces_previews_as_fed_whole_in_bounded_memory", pieces),
        NodeTest::basic("utf8_split_across_pieces_decodes_like_the_whole", utf8_pieces),
        NodeTest::basic("the_preview_fits_a_wire_at_the_largest_max_chars", wire_bound),
        NodeTest::basic("the_settings_bounds_are_the_metadatas", bounds_agree),
        NodeTest::fake("the_node_previews_a_wired_text", node_text),
        NodeTest::fake("the_node_previews_a_stored_file", node_file),
    ]
}

/// `keep` as the node reads it off its select.
fn keep_of(keep: &str) -> Keep {
    serde_json::from_value(json!(keep)).expect("a keep option")
}

fn settings(keep: &str, lines: usize, chars: usize, share: f64) -> Settings {
    Settings::new(keep_of(keep), lines, chars, share)
}

fn show(text: &str, s: &Settings) -> Shown {
    let mut preview = Preview::new(s);
    preview.push(text);
    preview.finish()
}

/// `text` fed `size` characters at a time.
fn show_in_pieces(text: &str, s: &Settings, size: usize) -> Shown {
    let mut preview = Preview::new(s);
    let chars: Vec<char> = text.chars().collect();
    for piece in chars.chunks(size) {
        preview.push(&piece.iter().collect::<String>());
    }
    preview.finish()
}

fn numbered(n: usize) -> String {
    (1..=n).map(|i| format!("l{i}\n")).collect()
}

fn modes() -> WeftResult<()> {
    // Ten lines, 31 characters; four lines fit.
    let text = numbered(10);
    assert_eq!(text.chars().count(), 31);
    let both = show(&text, &settings("both", 4, 1000, 0.5));
    assert_eq!(
        both,
        Shown {
            preview: "l1\nl2\n[... 6 of 10 lines cut (18 of 31 characters), from the middle ...]\nl9\nl10\n".into(),
            cut: true,
            lines: 10,
            chars: 31,
        }
    );
    assert_eq!(
        show(&text, &settings("start", 4, 1000, 0.5)).preview,
        "l1\nl2\nl3\nl4\n[... 6 of 10 lines cut (19 of 31 characters), from the end ...]"
    );
    assert_eq!(
        show(&text, &settings("end", 4, 1000, 0.5)).preview,
        "[... 6 of 10 lines cut (18 of 31 characters), from the start ...]\nl7\nl8\nl9\nl10\n"
    );
    // The start's share is rounded down, the end gets the rest.
    assert_eq!(
        show(&text, &settings("both", 4, 1000, 0.25)).preview,
        "l1\n[... 6 of 10 lines cut (18 of 31 characters), from the middle ...]\nl8\nl9\nl10\n"
    );
    // `startShare` at either end leaves `both` showing one side, and
    // the marker says which side was cut.
    assert_eq!(
        show(&text, &settings("both", 4, 1000, 1.0)).preview,
        show(&text, &settings("start", 4, 1000, 0.5)).preview
    );
    assert_eq!(
        show(&text, &settings("both", 4, 1000, 0.0)).preview,
        show(&text, &settings("end", 4, 1000, 0.5)).preview
    );
    // `startShare` means nothing outside `both`.
    assert_eq!(show(&text, &settings("end", 4, 1000, 0.9)), show(&text, &settings("end", 4, 1000, 0.1)));
    Ok(())
}

fn chars_win() -> WeftResult<()> {
    // Three lines of ten characters: the lines would fit, eight
    // characters do not, so each side keeps four.
    let text = "aaaaaaaaa\nbbbbbbbbb\nccccccccc\n";
    let shown = show(text, &settings("both", 256, 8, 0.5));
    assert_eq!(
        shown.preview,
        "aaaa\n[... 1 of 3 lines cut (22 of 30 characters), from the middle ...]\nccc\n"
    );
    assert!(shown.cut);
    assert_eq!(
        show(text, &settings("start", 256, 8, 0.5)).preview,
        "aaaaaaaa\n[... 2 of 3 lines cut (22 of 30 characters), from the end ...]"
    );
    assert_eq!(
        show(text, &settings("end", 256, 8, 0.5)).preview,
        "[... 2 of 3 lines cut (22 of 30 characters), from the start ...]\nccccccc\n"
    );
    // And the line limit wins when it runs out first: two lines, plenty
    // of characters.
    assert_eq!(
        show(text, &settings("start", 2, 10_000, 0.5)).preview,
        "aaaaaaaaa\nbbbbbbbbb\n[... 1 of 3 lines cut (10 of 30 characters), from the end ...]"
    );
    Ok(())
}

/// Lines of one character each: the characters would all fit, so each
/// side is bound by lines alone, and the two must still not meet.
fn no_overlap() -> WeftResult<()> {
    for (count, max_lines) in [(300, 256), (257, 256), (5, 4), (3, 2)] {
        let text = "x\n".repeat(count);
        let shown = show(&text, &settings("both", max_lines, 10_000, 0.5));
        let kept = shown.preview.lines().filter(|l| *l == "x").count();
        assert_eq!(kept, max_lines, "{count} lines, {max_lines} kept: {}", shown.preview);
        let cut = count - max_lines;
        assert!(
            shown.preview.contains(&format!("[... {cut} of {count} lines cut ({} of {} characters)", 2 * cut, 2 * count)),
            "{}",
            shown.preview
        );
        // The lines kept and the lines cut add up to the whole, so no
        // line was shown twice.
        assert_eq!(kept + cut, count);
    }
    // Two characters per side and one-character lines: the start holds
    // `a\n`, the end `e\n`, and the three between are cut.
    let shown = show("a\nb\nc\nd\ne\n", &settings("both", 256, 4, 0.5));
    assert_eq!(shown.preview, "a\n[... 3 of 5 lines cut (6 of 10 characters), from the middle ...]\ne\n");
    Ok(())
}

fn long_line() -> WeftResult<()> {
    let text = format!("{}{}", "a".repeat(100), "b".repeat(100));
    assert_eq!(
        show(&text, &settings("both", 256, 10, 0.5)).preview,
        "aaaaa\n[... 0 of 1 lines cut (190 of 200 characters), from the middle ...]\nbbbbb"
    );
    assert_eq!(
        show(&text, &settings("start", 256, 10, 0.5)).preview,
        "aaaaaaaaaa\n[... 0 of 1 lines cut (190 of 200 characters), from the end ...]"
    );
    assert_eq!(
        show(&text, &settings("end", 256, 10, 0.5)).preview,
        "[... 0 of 1 lines cut (190 of 200 characters), from the start ...]\nbbbbbbbbbb"
    );
    // A long line in the middle of short ones: the start keeps whole
    // lines and then the long line's start, the end the long line's end
    // and then whole lines; the long line is cut, not dropped.
    let text = format!("one\n{}\ntwo\n", "z".repeat(1000));
    assert_eq!(
        show(&text, &settings("both", 256, 14, 0.5)).preview,
        "one\nzzz\n[... 0 of 3 lines cut (995 of 1009 characters), from the middle ...]\nzz\ntwo\n"
    );
    Ok(())
}

fn empty() -> WeftResult<()> {
    let expected = Shown { preview: "(empty)".into(), cut: false, lines: 0, chars: 0 };
    assert_eq!(show("", &settings("both", 256, 10_000, 0.5)), expected);
    let mut preview = Preview::new(&settings("end", 1, 1, 0.5));
    preview.push("");
    preview.push("");
    assert_eq!(preview.finish(), expected);
    Ok(())
}

fn exact_fit() -> WeftResult<()> {
    let text = "ab\ncd\n";
    for keep in ["start", "end", "both"] {
        let shown = show(text, &settings(keep, 2, 6, 0.5));
        assert_eq!(shown, Shown { preview: text.into(), cut: false, lines: 2, chars: 6 }, "{keep}");
        // One character more is a cut.
        assert!(show("ab\ncd\ne", &settings(keep, 3, 6, 0.5)).cut, "{keep}");
        // One line more is a cut.
        assert!(show("ab\ncd\ne", &settings(keep, 2, 7, 0.5)).cut, "{keep}");
    }
    // A text that fits is shown whole even when one side's share could
    // not have held its part: the start's five characters end inside
    // the first line, and the end's two lines start after it.
    let text = "abcdef\nx\ny";
    assert_eq!(show(text, &settings("both", 4, 10, 0.5)).preview, text);
    Ok(())
}

fn line_counting() -> WeftResult<()> {
    let s = settings("both", 256, 10_000, 0.5);
    for (text, lines) in [("a", 1), ("a\n", 1), ("a\nb", 2), ("a\nb\n", 2), ("\n", 1), ("\n\n", 2), ("a\n\nb", 3)] {
        assert_eq!(show(text, &s).lines, lines, "{text:?}");
    }
    assert_eq!(show("héllo\n", &s).chars, 6, "characters, not bytes");
    Ok(())
}

fn pieces() -> WeftResult<()> {
    let texts = [
        numbered(40),
        format!("{}\n{}", "a".repeat(50), "b".repeat(70)),
        "x\n".repeat(30),
        "é🎉\nü\n".repeat(20),
        String::new(),
    ];
    let cases = [
        settings("both", 6, 25, 0.5),
        settings("both", 9, 4, 0.3),
        settings("start", 5, 40, 0.5),
        settings("end", 7, 13, 0.5),
        settings("both", 256, 10_000, 0.5),
    ];
    for text in &texts {
        for s in &cases {
            let whole = show(text, s);
            for size in [1, 2, 3, 5, 64] {
                assert_eq!(show_in_pieces(text, s, size), whole, "{text:?} {s:?} by {size}");
            }
        }
    }

    // A long text fed in pieces holds no more than its budget: the
    // start's characters, and twice the end's characters plus a piece.
    let s = settings("both", 256, MAX_CHARS_LIMIT, 0.5);
    let (_, end) = s.split();
    let mut preview = Preview::new(&s);
    let piece: String = (0..100).map(|i| format!("line {i}\n")).collect();
    for _ in 0..2000 {
        preview.push(&piece);
        assert!(preview.prefix_chars <= s.max.chars);
        assert!(preview.tail_chars <= 2 * end.chars + piece.chars().count(), "{}", preview.tail_chars);
    }
    let shown = preview.finish();
    assert_eq!(shown.lines, 200_000);
    assert!(shown.preview.starts_with("line 0\nline 1\n"), "{}", &shown.preview[..40]);
    assert!(shown.preview.ends_with("line 98\nline 99\n"));
    assert_eq!(shown.preview.lines().count(), 257, "256 lines kept and the marker");
    Ok(())
}

fn utf8_pieces() -> WeftResult<()> {
    let mut bytes = "héllo wörld 🎉 ok\n".as_bytes().to_vec();
    // A byte that starts nothing, a three-byte character cut short by a
    // letter, a whole euro sign, and one cut short by the end.
    bytes.extend_from_slice(&[0xff, b'x', 0xe2, 0x82, b'A']);
    bytes.extend_from_slice("€".as_bytes());
    bytes.extend_from_slice(&[0xe2, 0x82]);
    let expected = String::from_utf8_lossy(&bytes).into_owned();
    assert!(expected.ends_with("A€\u{FFFD}"), "{expected:?}");
    for size in 1..=6 {
        let mut decoder = Utf8Decoder::default();
        let mut out = String::new();
        for piece in bytes.chunks(size) {
            out.push_str(&decoder.push(piece));
        }
        out.push_str(&decoder.finish());
        assert_eq!(out, expected, "pieces of {size} bytes");
    }
    // Every place one split can fall.
    for at in 0..=bytes.len() {
        let mut decoder = Utf8Decoder::default();
        let mut out = decoder.push(&bytes[..at]);
        out.push_str(&decoder.push(&bytes[at..]));
        out.push_str(&decoder.finish());
        assert_eq!(out, expected, "split at {at}");
    }
    Ok(())
}

/// The heaviest text there is: every character one JSON writes as six
/// bytes. At the largest `maxChars`, whatever the mode, the preview fits.
fn wire_bound() -> WeftResult<()> {
    let one_line = "\u{1}".repeat(100_000);
    let many_lines = "\u{1}\u{2}\u{3}\n".repeat(25_000);
    let fits_exactly = "\u{1}".repeat(MAX_CHARS_LIMIT);
    for text in [&one_line, &many_lines, &fits_exactly] {
        for keep in ["start", "end", "both"] {
            let shown = show(text, &settings(keep, 1_000_000, MAX_CHARS_LIMIT, 0.5));
            let weight = wire_weight(shown.preview.as_str());
            assert!(weight <= MAX_WIRE_VALUE_BYTES, "{keep}: {weight} bytes");
        }
    }
    assert!(!show(&fits_exactly, &settings("both", 1, MAX_CHARS_LIMIT, 0.5)).cut);
    Ok(())
}

/// The bounds the node relies on are the ones its metadata's widgets
/// declare, which the compiler and the runtime hold a program to: in
/// particular `maxChars` stops at the most characters a wire carries
/// whatever they are.
fn bounds_agree() -> WeftResult<()> {
    use weft::node::Widget;
    use weft::NodeManifest;
    let widget = |name: &str| {
        TextPreviewNode
            .manifest()
            .inputs
            .iter()
            .find(|i| i.name == name)
            .and_then(|i| i.widget.clone())
            .unwrap_or_else(|| panic!("`{name}` has a widget"))
    };
    match widget("maxChars") {
        Widget::Number { min, max, step } => {
            assert_eq!((min, max, step), (Some(1.0), Some(MAX_CHARS_LIMIT as f64), Some(1.0)));
        }
        other => panic!("maxChars: {other:?}"),
    }
    match widget("maxLines") {
        Widget::Number { min, max, step } => assert_eq!((min, max, step), (Some(1.0), None, Some(1.0))),
        other => panic!("maxLines: {other:?}"),
    }
    match widget("startShare") {
        Widget::Number { min, max, .. } => assert_eq!((min, max), (Some(0.0), Some(1.0))),
        other => panic!("startShare: {other:?}"),
    }
    match widget("keep") {
        Widget::Select { options, free_text } => {
            assert!(!free_text);
            for option in &options {
                keep_of(option);
            }
            assert_eq!(options.len(), 3);
        }
        other => panic!("keep: {other:?}"),
    }
    Ok(())
}

async fn node_text(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&TextPreviewNode, json!({ "text": numbered(10), "maxLines": 4 })).await.ok()?;
    assert_eq!(
        outcome.outputs["preview"],
        json!("l1\nl2\n[... 6 of 10 lines cut (18 of 31 characters), from the middle ...]\nl9\nl10\n")
    );
    assert_eq!(outcome.outputs["cut"], json!(true));
    assert_eq!(outcome.outputs["lines"], json!(10));
    assert_eq!(outcome.outputs["chars"], json!(31));

    // With the defaults, a short text comes back whole.
    let outcome = rig.run(&TextPreviewNode, json!({ "text": "ok\n" })).await.ok()?;
    assert_eq!(outcome.outputs["preview"], json!("ok\n"));
    assert_eq!(outcome.outputs["cut"], json!(false));
    Ok(())
}

async fn node_file(rig: FakeRig) -> WeftResult<()> {
    // 5000 lines, then a byte that is no UTF-8 on a last line with no
    // break.
    let mut bytes: Vec<u8> = (0..5000).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
    bytes.extend_from_slice(&[b'e', b'n', b'd', 0xff]);
    let whole = String::from_utf8_lossy(&bytes).into_owned();
    let file = rig.store_file("out.log", "text/plain", bytes);
    let outcome = rig.run(&TextPreviewNode, json!({ "file": file, "keep": "end", "maxLines": 3 })).await.ok()?;
    assert_eq!(outcome.outputs["lines"], json!(5001));
    assert_eq!(outcome.outputs["chars"], json!(whole.chars().count()));
    assert_eq!(outcome.outputs["cut"], json!(true));
    let preview = outcome.outputs["preview"].as_str().expect("a string");
    assert!(
        preview.starts_with(&format!("[... 4998 of 5001 lines cut ({} of", whole.chars().count() - 24)),
        "{preview}"
    );
    assert!(preview.ends_with("\nline 4998\nline 4999\nend\u{FFFD}"), "{preview}");
    Ok(())
}
