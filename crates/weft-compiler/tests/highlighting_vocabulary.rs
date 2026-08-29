//! Weft is highlighted by two grammars that live outside Rust, one per
//! engine: `weft.tmLanguage.json` for the editor and `highlight-weft.js`
//! for the book. Both spell out the language's own vocabulary, so adding a
//! primitive or a reserved type here goes stale there silently, and the new
//! name renders as an ordinary word.
//!
//! This test is the alarm, in BOTH directions: it fails naming whatever the
//! compiler knows and a grammar does not, and whatever a grammar lists that
//! the compiler no longer reserves (a removed word would otherwise stay
//! painted forever).
//!
//! It compares WORD LISTS, not text: every parenthesized `(a|b|c)`
//! alternation of plain words in a grammar file is one list, so a word in a
//! comment can never satisfy it, and reformatting an alternation cannot
//! break it.
//!
//! SYNC: weft highlighting vocabulary <-> packages/weft-syntax/weft.tmLanguage.json,
//! packages/weft-syntax/highlight-weft.js

use std::collections::BTreeSet;

use weft_compiler::weft_compiler::{
    RESERVED_CONFIG_KEYS, RESERVED_TYPE_KEYWORDS, RESERVED_WORDS,
};
use weft_core::weft_type::{WeftPrimitive, WeftType, CONTAINER_AND_SPECIAL_TYPES};

/// Read one grammar file out of `packages/weft-syntax/`.
fn grammar(file: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../packages/weft-syntax/");
    let path = std::path::Path::new(path).join(file);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the weft grammar at {}: {e}", path.display()))
}

/// Every word both grammars have to list: the reserved type keywords, the
/// reserved config keys, the whole type vocabulary a port signature can
/// mention, and the bare reserved words (`self`, the boolean literals).
fn vocabulary() -> BTreeSet<String> {
    RESERVED_TYPE_KEYWORDS
        .iter()
        .chain(RESERVED_CONFIG_KEYS.iter())
        .chain(RESERVED_WORDS.iter())
        .map(|s| (*s).to_string())
        .chain(WeftPrimitive::ALL_NAMES.iter().map(|s| (*s).to_string()))
        .chain(CONTAINER_AND_SPECIAL_TYPES.iter().map(|s| (*s).to_string()))
        .chain(WeftType::union_alias_names().map(|s| s.to_string()))
        .collect()
}

/// Every word a grammar file LISTS: the union of its `(a|b|c)` word
/// alternations. A span between parentheses counts when, after dropping
/// the join noise a JS string concatenation leaves behind (quotes, `+`,
/// whitespace), only word characters and `|` remain and there are at
/// least two words. Anything else between parentheses (a regex class, a
/// lookahead, code) has other characters and is skipped.
fn listed_words(grammar: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = grammar.as_bytes();
    let mut open = None;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' => open = Some(i + 1),
            b')' => {
                if let Some(start) = open.take() {
                    let span: String = grammar[start..i]
                        .chars()
                        .filter(|c| !matches!(c, '\'' | '"' | '+' ) && !c.is_whitespace())
                        .collect();
                    if !span.is_empty()
                        && span.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '|')
                        && span.contains('|')
                    {
                        out.extend(span.split('|').filter(|w| !w.is_empty()).map(String::from));
                    }
                }
            }
            _ => {}
        }
    }
    out
}

#[test]
fn both_grammars_list_exactly_the_reserved_vocabulary() {
    let expected = vocabulary();
    for file in ["weft.tmLanguage.json", "highlight-weft.js"] {
        let listed = listed_words(&grammar(file));
        let missing: Vec<&String> = expected.difference(&listed).collect();
        let extra: Vec<&String> = listed.difference(&expected).collect();
        assert!(
            missing.is_empty(),
            "packages/weft-syntax/{file} does not list {missing:?}. \
             Add each name to its word list, in both grammar files."
        );
        assert!(
            extra.is_empty(),
            "packages/weft-syntax/{file} lists {extra:?}, which the compiler does not \
             reserve. Remove it from both grammar files (or reserve it in the compiler)."
        );
    }
}

/// A comment is `#` to end of line and nothing else (see the lexer). Both
/// grammars have to agree, or a comment renders as code.
#[test]
fn both_grammars_comment_on_hash() {
    assert!(grammar("weft.tmLanguage.json").contains(r##""match": "#.*$""##));
    assert!(grammar("highlight-weft.js").contains("HASH_COMMENT_MODE"));
}
