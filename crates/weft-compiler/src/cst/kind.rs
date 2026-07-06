//! The Weft syntax-kind set and the `rowan::Language` binding.
//!
//! rowan trees are untyped: every node and token carries a `u16` tag. This
//! module is that tag set (`SyntaxKind`) plus the `Language` impl that teaches
//! rowan to convert between our enum and its raw `u16`. The kinds map 1:1 onto
//! the grammar shapes the parser builds.
//!
//! The cardinal rule of the whole CST: every byte of source is a token. Trivia
//! (whitespace, blank lines, comments) is not skipped, it is a token like any
//! other. Concatenating all token texts in document order reproduces the source
//! byte-for-byte: that is the lossless guarantee the edit engine relies on.

/// Every node and token kind in the Weft CST. `repr(u16)` so rowan can store it
/// as a raw tag. Order matters only in that `ROOT`/the last variant bounds the
/// `from_u16` range check; add new kinds before `__LAST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
#[allow(non_camel_case_types)]
pub enum SyntaxKind {
    // ── tokens (leaves; carry text) ────────────────────────────────────────
    // trivia: whitespace + comments, kept in the tree so round-trip is exact.
    WHITESPACE = 0, // runs of spaces / tabs / newlines
    COMMENT,        // `# ...` to end of line (incl. the `# Description:` convention)

    // punctuation / structure
    L_PAREN,   // (
    R_PAREN,   // )
    L_BRACE,   // {
    R_BRACE,   // }
    ARROW,     // ->
    COLON,     // :
    COMMA,     // ,
    DOT,       // .
    EQ,        // =
    QUESTION,  // ? (optional-port marker)

    // words / literals
    IDENT,      // node/group/port/field names, type names
    STRING,     // "..." (one token; escapes kept verbatim in the text)
    NUMBER,     // 42, 3.14, true, false (bare scalar literals)
    HEREDOC,    // ```...``` incl. fences, ONE opaque token
    JSON_VALUE, // a `[...]`/`{...}` config value, ONE opaque token
    MARKER,     // @file(...) / @include(...) / @require_one_of(...), ONE opaque token
    KW_GROUP,   // the `Group` reserved type keyword
    KW_LOOP,    // the `Loop` reserved type keyword

    ERROR, // an unrecognized byte run (lenient parse; never panics)

    // ── nodes (interior; carry children) ───────────────────────────────────
    WEFT_FILE, // the root

    NODE_DECL,    // id = Type(sig) -> (sig) { body }
    GROUP_DECL,   // label = Group(sig) -> (sig) { body }
    LOOP_DECL,    // label = Loop(sig) -> (sig) { config + body }
    INCLUDE_DECL, // alias = @include("path")

    HEADER,        // `id = Type` + signatures, up to (not incl.) the body `{`
    PORT_SIG_IN,   // (a: T, b: U?) input signature
    PORT_SIG_OUT,  // (out: T) output signature (after `->`)
    PORT_DECL,     // one `name: Type` / `name: Type?` inside a signature

    BODY,         // { ... } the brace-delimited block of a node or group
    CONFIG_FIELD, // key: value (one config entry)
    CONNECTION,   // target.port = source.port (an edge line)
    ENDPOINT,     // ident.port, or bare `self` (one side of a connection)
    INLINE_EXPR,  // key: Type { ... }.port (a node literal used as a value)
    DIRECTIVE,    // standalone @require_one_of(...) line inside a body
    LABEL_FIELD,  // _label: "..." / label: "..." (promoted to node.label)
    GROUP_DESC,   // `# Description:` comment as the FIRST body line of a group

    #[doc(hidden)]
    __LAST,
}

impl SyntaxKind {
    /// True for trivia tokens (whitespace + comments). The typed-view layer
    /// skips these when walking for significant children, but they always stay
    /// in the tree so round-trip is byte-exact.
    pub fn is_trivia(self) -> bool {
        matches!(self, SyntaxKind::WHITESPACE | SyntaxKind::COMMENT)
    }
}

impl From<SyntaxKind> for rowan::SyntaxKind {
    fn from(k: SyntaxKind) -> Self {
        rowan::SyntaxKind(k as u16)
    }
}

/// All `SyntaxKind` variants in discriminant order (0..__LAST). `kind_from_raw`
/// indexes this to recover the enum from rowan's raw `u16` with NO `unsafe` and
/// NO dependence on the discriminants being contiguous: a wrong/missing entry is
/// caught by the `kind_roundtrip_table` test (every variant must map to itself),
/// and an out-of-range raw tag panics loudly here rather than risking UB.
const ALL_KINDS: &[SyntaxKind] = {
    use SyntaxKind::*;
    &[
        WHITESPACE, COMMENT, L_PAREN, R_PAREN, L_BRACE, R_BRACE, ARROW, COLON, COMMA, DOT, EQ,
        QUESTION, IDENT, STRING, NUMBER, HEREDOC, JSON_VALUE, MARKER, KW_GROUP, KW_LOOP, ERROR,
        WEFT_FILE, NODE_DECL, GROUP_DECL, LOOP_DECL, INCLUDE_DECL, HEADER, PORT_SIG_IN, PORT_SIG_OUT,
        PORT_DECL, BODY, CONFIG_FIELD, CONNECTION, ENDPOINT, INLINE_EXPR, DIRECTIVE, LABEL_FIELD,
        GROUP_DESC,
    ]
};

/// The `rowan::Language` binding for Weft: the one spot that converts rowan's
/// raw `u16` back to our enum, via a safe table lookup (no `transmute`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WeftLanguage {}

impl rowan::Language for WeftLanguage {
    type Kind = SyntaxKind;

    fn kind_from_raw(raw: rowan::SyntaxKind) -> Self::Kind {
        *ALL_KINDS
            .get(raw.0 as usize)
            .unwrap_or_else(|| panic!("rowan tag {} out of range", raw.0))
    }

    fn kind_to_raw(kind: Self::Kind) -> rowan::SyntaxKind {
        kind.into()
    }
}

/// Our concrete red-tree handle types, specialized to `WeftLanguage`. The rest
/// of the CST code uses these aliases rather than rowan's generic forms.
pub type SyntaxNode = rowan::SyntaxNode<WeftLanguage>;
pub type SyntaxToken = rowan::SyntaxToken<WeftLanguage>;
pub type SyntaxElement = rowan::SyntaxElement<WeftLanguage>;

#[cfg(test)]
mod tests {
    use super::*;
    use rowan::Language;

    /// `ALL_KINDS` must list every variant exactly once, in discriminant order.
    /// This proves the table is complete and correctly ordered, so the safe
    /// table lookup in `kind_from_raw` is exact: adding/reordering a variant
    /// without updating `ALL_KINDS` fails here loudly.
    #[test]
    fn kind_roundtrip_table() {
        // length matches the discriminant count
        assert_eq!(ALL_KINDS.len(), SyntaxKind::__LAST as usize, "ALL_KINDS is missing or has extra variants");
        // each entry sits at its own discriminant index, and round-trips raw->kind
        for (i, &k) in ALL_KINDS.iter().enumerate() {
            assert_eq!(k as usize, i, "ALL_KINDS[{i}] = {k:?} is out of discriminant order");
            assert_eq!(WeftLanguage::kind_from_raw(rowan::SyntaxKind(i as u16)), k);
            assert_eq!(WeftLanguage::kind_to_raw(k), rowan::SyntaxKind(i as u16));
        }
    }
}
