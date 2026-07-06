//! The CST parser: a lossless token stream -> a rowan green tree whose
//! `to_string()` is byte-identical to the source.
//!
//! Shape (top-down recursive descent feeding `GreenNodeBuilder`):
//!   WEFT_FILE
//!     ( trivia | NODE_DECL | GROUP_DECL | INCLUDE_DECL | CONNECTION | ERROR )*
//!
//! Trivia attachment (model doc §3): leading trivia attaches to the NEXT
//! significant declaration. We implement this by NOT eating leading trivia at
//! file level before a decl; instead each decl-parse begins by attaching the
//! pending leading trivia inside its own node. A trailing same-line comment
//! attaches inside the decl it trails.
//!
//! The parser is total and lenient: any token it cannot place becomes an ERROR
//! child, so a mid-edit-broken buffer still produces a tree (the editor needs
//! the graph to keep rendering). No panics, no lost bytes.

use rowan::GreenNode;

use super::kind::{SyntaxKind, SyntaxNode};
use super::lexer::{lex, Token};

/// Parse source into a lossless CST root (`WEFT_FILE`). Always succeeds: errors
/// are ERROR nodes in the tree, surfaced by a later structural check, not by a
/// parse failure. `SyntaxNode::new_root(green)` gives the navigable red tree.
pub fn parse(source: &str) -> SyntaxNode {
    let tokens = lex(source);
    let mut p = Parser {
        tokens,
        pos: 0,
        builder: rowan::GreenNodeBuilder::new(),
    };
    p.parse_file();
    SyntaxNode::new_root(p.builder.finish())
}

/// Parse and return the green node directly (for callers building subtrees).
pub fn parse_green(source: &str) -> GreenNode {
    let tokens = lex(source);
    let mut p = Parser {
        tokens,
        pos: 0,
        builder: rowan::GreenNodeBuilder::new(),
    };
    p.parse_file();
    p.builder.finish()
}

struct Parser<'a> {
    tokens: Vec<Token<'a>>,
    pos: usize,
    builder: rowan::GreenNodeBuilder<'static>,
}

impl<'a> Parser<'a> {
    // ── token-stream cursor ────────────────────────────────────────────────

    /// The kind at `pos`, or None at end of stream.
    fn cur(&self) -> Option<SyntaxKind> {
        self.tokens.get(self.pos).map(|t| t.kind)
    }

    /// The kind of the next NON-trivia token from `pos`, and its offset.
    fn peek_significant(&self) -> Option<(usize, SyntaxKind)> {
        let mut i = self.pos;
        while let Some(t) = self.tokens.get(i) {
            if !t.kind.is_trivia() {
                return Some((i - self.pos, t.kind));
            }
            i += 1;
        }
        None
    }

    /// True if the next significant token is `->`, reachable across at most ONE
    /// line break (the accepted `T(in)\n-> (out)` layout). A blank line (a
    /// whitespace trivia containing 2+ newlines, or two newline-bearing trivia)
    /// before the arrow means it belongs to a separate statement, not here.
    fn peek_arrow_within_one_newline(&self) -> bool {
        let mut i = self.pos;
        let mut newlines = 0usize;
        while let Some(t) = self.tokens.get(i) {
            if t.kind.is_trivia() {
                if t.kind == SyntaxKind::WHITESPACE {
                    newlines += t.text.matches('\n').count();
                    if newlines > 1 {
                        return false; // blank line: arrow (if any) is not ours
                    }
                }
                i += 1;
                continue;
            }
            return t.kind == SyntaxKind::ARROW;
        }
        false
    }

    /// Emit the current token into the tree as a leaf and advance.
    fn bump(&mut self) {
        let t = self.tokens[self.pos];
        self.builder.token(t.kind.into(), t.text);
        self.pos += 1;
    }

    /// Bump every trivia token at the cursor (whitespace + comments).
    fn bump_trivia(&mut self) {
        while self.cur().map(|k| k.is_trivia()).unwrap_or(false) {
            self.bump();
        }
    }

    /// Bump trivia that stays on the CURRENT line (trailing same-line comment +
    /// its inline whitespace), stopping at the newline. Used to pull a trailing
    /// comment inside the decl it trails before the newline closes the line.
    fn bump_trailing_same_line(&mut self) {
        while let Some(k) = self.cur() {
            if !k.is_trivia() {
                break;
            }
            let text = self.tokens[self.pos].text;
            if k == SyntaxKind::WHITESPACE && text.contains('\n') {
                break; // newline ends the line; leave it as leading trivia for next
            }
            self.bump();
        }
    }

    // ── grammar ────────────────────────────────────────────────────────────

    fn parse_file(&mut self) {
        self.builder.start_node(SyntaxKind::WEFT_FILE.into());
        while self.cur().is_some() {
            // Leading trivia belongs to the next decl: peek past it to decide
            // what comes, then let the decl-parse consume the trivia itself.
            match self.peek_significant() {
                None => {
                    // Only trailing trivia left (end of file): attach to file.
                    self.bump_trivia();
                    break;
                }
                Some((_, _)) => self.parse_item(),
            }
        }
        self.builder.finish_node();
    }

    /// Parse one top-level item, consuming its leading trivia into its node.
    /// Top level accepts decls + connections; a field/directive or a malformed
    /// line is an error here.
    fn parse_item(&mut self) {
        match self.classify() {
            LineShape::Node => self.parse_node_decl(),
            LineShape::Group => self.parse_group_decl(false),
            LineShape::AnonGroup => self.parse_group_decl(true),
            LineShape::Loop => self.parse_loop_decl(),
            LineShape::Include => self.parse_include_decl(),
            LineShape::Connection => self.parse_connection(),
            // Fields/directives are body-only; at top level they're malformed.
            LineShape::Field | LineShape::Directive | LineShape::Unknown => self.parse_error_line(),
        }
    }

    /// Classify the upcoming logical line into exactly one accepted form, or
    /// `Unknown`. STRICT: the LHS of a decl is exactly one IDENT (`id = ...`),
    /// the LHS of a connection is exactly `IDENT . IDENT`, an include's RHS
    /// marker directive is exactly `include`. Anything else (a 3-segment LHS, a
    /// leading `=`, a non-Type/Group/@include RHS, a stray marker) is `Unknown`.
    fn classify(&self) -> LineShape {
        use SyntaxKind as K;
        let sig = self.significant_run();
        let Some(&(_, first)) = sig.first() else { return LineShape::Unknown };

        // A leading `@require_one_of(...)` is a directive (body only; the caller
        // decides whether a directive is legal in its position).
        if first == K::MARKER {
            return LineShape::Directive;
        }

        // A leading `Group` keyword:
        //  - `Group =` is `Group`-used-as-a-name -> a node decl (lowering rejects
        //    the reserved name loudly). Falls through to the IDENT/decl path.
        //  - `Group` then `(` / `->` / `{` / end-of-line is an anonymous group
        //    (an included file's sole top-level group).
        //  - anything else after `Group` (`Group.x`, `Group: v`, ...) is
        //    malformed: NOT a valid anon-group head, so Unknown (one error node),
        //    never a phantom group decl + a stray error sibling.
        if first == K::KW_GROUP {
            return match sig.get(1).map(|(_, k)| *k) {
                Some(K::EQ) => LineShape::Node, // Group-as-a-name; lowering rejects it
                None | Some(K::L_PAREN) | Some(K::ARROW) | Some(K::L_BRACE) => LineShape::AnonGroup,
                _ => LineShape::Unknown,
            };
        }

        // Otherwise the LHS must start with an IDENT. A line that doesn't is
        // malformed (`= foo`, `.x = y`, ...).
        if first != K::IDENT {
            return LineShape::Unknown;
        }

        // Split LHS (before the first top-level operator) from the operator.
        // Accepted LHS shapes: `IDENT` (decl) or `IDENT . IDENT` (connection).
        let op_idx = sig.iter().position(|(_, k)| matches!(k, K::EQ | K::COLON));
        let Some(op_idx) = op_idx else { return LineShape::Unknown };
        let op = sig[op_idx].1;
        let lhs = &sig[..op_idx];

        match op {
            K::COLON => {
                // `key: value` config field: LHS is exactly one IDENT.
                if lhs.len() == 1 && lhs[0].1 == K::IDENT {
                    LineShape::Field
                } else {
                    LineShape::Unknown
                }
            }
            K::EQ => match lhs.len() {
                // `id = <rhs>`: a decl. RHS kind picks node/group/include.
                1 if lhs[0].1 == K::IDENT || lhs[0].1 == K::KW_GROUP => self.decl_shape(&sig, op_idx),
                // `target . port = <rhs>`: a connection. Exactly 3 LHS tokens.
                3 if lhs[0].1 == K::IDENT && lhs[1].1 == K::DOT && lhs[2].1 == K::IDENT => {
                    LineShape::Connection
                }
                // anything else (bare `=`, `a.b.c =`, `a. =`, ...) is malformed.
                _ => LineShape::Unknown,
            },
            _ => LineShape::Unknown,
        }
    }

    /// For a `id = <rhs>` decl, pick Node / Group / Include from the RHS's first
    /// significant token (`op_idx` is the `=` position in `sig`).
    fn decl_shape(&self, sig: &[(usize, SyntaxKind)], op_idx: usize) -> LineShape {
        use SyntaxKind as K;
        match sig.get(op_idx + 1) {
            Some(&(_, K::KW_GROUP)) => LineShape::Group,
            Some(&(_, K::KW_LOOP)) => LineShape::Loop,
            // An `@include("...")` marker (directive name EXACTLY `include`, not
            // a prefix like `@includes_other`). Any other marker RHS (`@file`,
            // `@require_one_of`) is not a valid decl RHS -> Unknown.
            Some(&(off, K::MARKER)) => {
                let directive = self
                    .marker_text_at(self.pos + off)
                    .map(|s| marker_directive(s) == "include")
                    .unwrap_or(false);
                if directive { LineShape::Include } else { LineShape::Unknown }
            }
            // A node type is a bare IDENT. (Inline-expr/body shapes follow it,
            // parsed by parse_node_decl; the classifier only needs the head.)
            Some(&(_, K::IDENT)) => LineShape::Node,
            _ => LineShape::Unknown,
        }
    }

    /// The significant (non-trivia) tokens from the cursor up to and including
    /// the end of the current logical line (first newline-bearing whitespace
    /// after a significant token), as (offset, kind). Used for classification.
    fn significant_run(&self) -> Vec<(usize, SyntaxKind)> {
        let mut out = Vec::new();
        let mut i = self.pos;
        let mut seen_significant = false;
        while let Some(t) = self.tokens.get(i) {
            if t.kind == SyntaxKind::WHITESPACE && t.text.contains('\n') && seen_significant {
                break;
            }
            if !t.kind.is_trivia() {
                out.push((i - self.pos, t.kind));
                seen_significant = true;
            }
            i += 1;
        }
        out
    }

    /// Parse a declaration: `id = Type ...`. Determines node vs group vs include
    /// by the RHS type token (KW_GROUP, MARKER `@include`, else node).
    /// The text of a MARKER token at absolute index `idx`, if present.
    fn marker_text_at(&self, idx: usize) -> Option<&'a str> {
        self.tokens.get(idx).filter(|t| t.kind == SyntaxKind::MARKER).map(|t| t.text)
    }

    fn parse_include_decl(&mut self) {
        self.builder.start_node(SyntaxKind::INCLUDE_DECL.into());
        self.bump_trivia(); // leading trivia for this decl
        self.bump_significant_until_eq(); // IDENT
        self.bump_significant(); // EQ
        self.bump_trivia_inline();
        self.bump_significant(); // MARKER
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    fn parse_node_decl(&mut self) {
        self.builder.start_node(SyntaxKind::NODE_DECL.into());
        self.bump_trivia();
        self.parse_header(false);
        self.maybe_parse_body(false);
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    fn parse_group_decl(&mut self, anon: bool) {
        self.builder.start_node(SyntaxKind::GROUP_DECL.into());
        self.bump_trivia();
        self.parse_header(anon);
        self.maybe_parse_body(true);
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    fn parse_loop_decl(&mut self) {
        self.builder.start_node(SyntaxKind::LOOP_DECL.into());
        self.bump_trivia();
        self.parse_header(false);
        // Loop bodies are MIXED: config fields (`key: value`) AND
        // decls/connections, in any order.
        self.maybe_parse_body(true);
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    /// HEADER = `id = Type` + optional `(in_sig)` + optional `-> (out_sig)`, up
    /// to but not including the body `{`. `anon` is the classifier's decision
    /// that this is an anonymous-group header (`Group` with no id), threaded
    /// down so the anon-vs-named predicate lives in exactly one place
    /// (`classify`).
    fn parse_header(&mut self, anon: bool) {
        self.builder.start_node(SyntaxKind::HEADER.into());
        self.bump_trivia_inline();
        if anon {
            self.bump(); // the `Group` type, no id
        } else {
            // id
            self.bump_significant(); // IDENT
            self.bump_trivia_inline();
            // `=`
            if self.cur() == Some(SyntaxKind::EQ) {
                self.bump();
            }
            self.bump_trivia_inline();
            // Type name (IDENT or KW_GROUP or KW_LOOP)
            if matches!(self.cur(), Some(SyntaxKind::IDENT) | Some(SyntaxKind::KW_GROUP) | Some(SyntaxKind::KW_LOOP)) {
                self.bump();
            }
        }
        // input signature `(...)`
        self.skip_inline_trivia_then(|p| {
            if p.cur() == Some(SyntaxKind::L_PAREN) {
                p.parse_port_sig(SyntaxKind::PORT_SIG_IN);
            }
        });
        // `-> (out)` pre-body output signature. The arrow may sit on the
        // IMMEDIATELY following line (`node = T(in)\n-> (out) {...}`), so we peek
        // across at most ONE newline. A blank line (2+ newlines) terminates the
        // decl: an arrow past a blank line is a separate (malformed) statement,
        // not this decl's output sig, so we must NOT swallow it.
        if self.peek_arrow_within_one_newline() {
            self.bump_trivia(); // consume up to the arrow (the single newline)
            self.builder.start_node(SyntaxKind::PORT_SIG_OUT.into());
            self.bump(); // ARROW
            self.bump_trivia_inline();
            if self.cur() == Some(SyntaxKind::L_PAREN) {
                self.bump_balanced_parens_as_ports();
            }
            self.builder.finish_node();
        }
        self.builder.finish_node(); // HEADER
    }

    /// Parse a `(...)` port signature wrapped in `kind` (PORT_SIG_IN). The
    /// interior is PORT_DECL children + punctuation; we keep all tokens.
    fn parse_port_sig(&mut self, kind: SyntaxKind) {
        self.builder.start_node(kind.into());
        self.bump_balanced_parens_as_ports();
        self.builder.finish_node();
    }

    /// Bump a balanced `(...)` run, wrapping each `name: Type[?]` group in a
    /// PORT_DECL node, keeping commas/whitespace/markers as direct children.
    fn bump_balanced_parens_as_ports(&mut self) {
        // Expect L_PAREN at cursor.
        if self.cur() != Some(SyntaxKind::L_PAREN) {
            return;
        }
        self.bump(); // L_PAREN
        let mut depth = 1;
        while let Some(k) = self.cur() {
            match k {
                SyntaxKind::L_PAREN => {
                    depth += 1;
                    self.bump();
                }
                SyntaxKind::R_PAREN => {
                    depth -= 1;
                    self.bump();
                    if depth == 0 {
                        break;
                    }
                }
                // A port name should be an IDENT, but a malformed one (e.g.
                // `1data`, lexed NUMBER-led) must still be captured as a
                // PORT_DECL so `try_parse_port_decl` rejects it loudly rather
                // than the bad port being silently dropped.
                SyntaxKind::IDENT | SyntaxKind::NUMBER => {
                    // A PORT_DECL: `name [: Type]`. The TYPE can be arbitrarily
                    // complex (`List[Number]`, `Dict[String, String]`,
                    // `String | Number`, with `?`), so after the colon we
                    // consume every token up to the next TOP-LEVEL `,` or `)`
                    // (bracket-aware via the lexer's balanced JSON_VALUE for
                    // `[...]`). The full type text is preserved for the lowering
                    // to re-parse with `WeftType::parse`. The trailing-`?`
                    // optional marker rides along in that run.
                    self.builder.start_node(SyntaxKind::PORT_DECL.into());
                    self.bump(); // name
                    self.bump_trivia_inline();
                    if self.cur() == Some(SyntaxKind::COLON) {
                        self.bump();
                        // consume the type run up to a top-level comma/paren
                        loop {
                            match self.cur() {
                                None => break,
                                Some(SyntaxKind::COMMA) | Some(SyntaxKind::R_PAREN) => break,
                                Some(SyntaxKind::L_PAREN) => break, // nested sig: leave to outer
                                Some(_) => self.bump(),
                            }
                        }
                    } else if self.cur() == Some(SyntaxKind::QUESTION) {
                        // bare optional port `name?` (no type annotation)
                        self.bump();
                    }
                    self.builder.finish_node();
                }
                // markers (@require_one_of), commas, trivia: keep as-is
                _ => self.bump(),
            }
        }
    }

    /// A node/group BODY: `{ ... }`. Children are config fields, connections,
    /// inline directives, nested decls, and trivia. The R_BRACE closes it.
    ///
    /// `is_group`: when true, a `# Description:` comment that is the FIRST body
    /// content (model doc §5: prefix AND first-body-line, both required) is
    /// promoted to a GROUP_DESC node instead of staying plain COMMENT trivia.
    fn maybe_parse_body(&mut self, is_group: bool) {
        // Skip inline trivia to find a `{`.
        let (off, kind) = match self.peek_significant() {
            Some(x) => x,
            None => return,
        };
        if kind != SyntaxKind::L_BRACE {
            return;
        }
        // bump the inline trivia between header and `{`
        for _ in 0..off {
            self.bump();
        }
        self.builder.start_node(SyntaxKind::BODY.into());
        self.bump(); // L_BRACE
        if is_group {
            self.maybe_promote_group_description();
        }
        loop {
            match self.cur() {
                None => break, // unterminated body: lenient, stop
                Some(SyntaxKind::R_BRACE) => {
                    self.bump();
                    break;
                }
                Some(k) if k.is_trivia() => self.bump(),
                // A one-liner body separates fields with commas: `{ a: 1, b: 2 }`.
                // Keep the comma as a body token and move to the next field.
                Some(SyntaxKind::COMMA) => self.bump(),
                Some(_) => self.parse_body_item(),
            }
        }
        self.builder.finish_node();
    }

    /// Right after a group body's `{`, if the first non-whitespace content is a
    /// `# Description:` COMMENT, wrap it (with its leading whitespace) in a
    /// GROUP_DESC node. Only the FIRST body line qualifies; a `# Description:`
    /// anywhere else stays a plain comment. Leading whitespace before the
    /// comment is bumped first so it stays as body trivia, then the comment is
    /// wrapped alone.
    fn maybe_promote_group_description(&mut self) {
        // peek past whitespace (not comments) to the first content token
        let mut i = self.pos;
        while let Some(t) = self.tokens.get(i) {
            if t.kind == SyntaxKind::WHITESPACE {
                i += 1;
            } else {
                break;
            }
        }
        let is_desc = self
            .tokens
            .get(i)
            .map(|t| t.kind == SyntaxKind::COMMENT && is_description_comment(t.text))
            .unwrap_or(false);
        if !is_desc {
            return;
        }
        // bump leading whitespace as body trivia
        while self.pos < i {
            self.bump();
        }
        // wrap the comment token as GROUP_DESC
        self.builder.start_node(SyntaxKind::GROUP_DESC.into());
        self.bump(); // the COMMENT
        self.builder.finish_node();
    }

    /// One item inside a body. Could be a config field (`key: value`), a
    /// connection (`a.b = c.d` or `self.x = ...`), a directive
    /// (`@require_one_of(...)`), a nested decl, or a label.
    fn parse_body_item(&mut self) {
        // A body accepts the same shapes as the top level PLUS fields and
        // directives, all via the one strict classifier (no second scanner).
        match self.classify() {
            LineShape::Field => self.parse_config_field(),
            LineShape::Directive => {
                self.builder.start_node(SyntaxKind::DIRECTIVE.into());
                self.bump_significant(); // MARKER
                self.bump_trailing_same_line();
                self.builder.finish_node();
            }
            LineShape::Connection => self.parse_connection(),
            LineShape::Node => self.parse_node_decl(),
            LineShape::Group => self.parse_group_decl(false),
            LineShape::AnonGroup => self.parse_group_decl(true),
            LineShape::Loop => self.parse_loop_decl(),
            LineShape::Include => self.parse_include_decl(),
            LineShape::Unknown => self.parse_error_line(),
        }
    }

    /// A config field `key: value`. `_label`/`label` keys become LABEL_FIELD;
    /// everything else CONFIG_FIELD. The value may be a STRING, NUMBER, HEREDOC,
    /// JSON_VALUE, MARKER, a `{...}` JSON object (wrapped JSON_VALUE), an inline
    /// expression (`Type {...}.port`), or a dotted ref (port wiring).
    fn parse_config_field(&mut self) {
        let sig = self.significant_run();
        let key_is_label = self
            .tokens
            .get(self.pos + sig[0].0)
            .map(|t| t.text == "label" || t.text == "_label")
            .unwrap_or(false);
        let wrapper = if key_is_label { SyntaxKind::LABEL_FIELD } else { SyntaxKind::CONFIG_FIELD };
        self.builder.start_node(wrapper.into());
        self.bump_significant(); // key IDENT
        self.bump_trivia_inline();
        if self.cur() == Some(SyntaxKind::COLON) {
            self.bump();
        }
        self.bump_trivia_inline();
        self.parse_value();
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    /// Parse a config value at the cursor. Recognizes inline exprs and `{...}`
    /// JSON objects; otherwise bumps the single value token.
    fn parse_value(&mut self) {
        match self.cur() {
            // `{ ... }` JSON object value: wrap the balanced brace-run as one
            // JSON_VALUE node (the lexer can't tag `{` as a value brace).
            Some(SyntaxKind::L_BRACE) => self.parse_json_object_value(),
            // Inline expression: an uppercase Type followed by `(`/`{`/`->`/`.`.
            Some(SyntaxKind::IDENT) | Some(SyntaxKind::KW_GROUP) if self.looks_like_inline_expr() => {
                self.parse_inline_expr();
            }
            // A dotted ref value (`src.port`) is port wiring: keep as ENDPOINT.
            Some(SyntaxKind::IDENT) if self.next_significant_is_dot() => {
                self.parse_endpoint();
            }
            // A single value token. ONLY real value kinds are consumed; a `}` or
            // any structural token means the field has no value (`key:`), so we
            // do NOT bump it as the value (that pulled the brace/newline into the
            // field node and let an editor value-swap collapse the layout).
            Some(k) if is_value_token(k) => self.bump(),
            _ => {}
        }
    }

    /// Wrap a balanced `{...}` run as a single JSON_VALUE node (round-trips as
    /// its concatenated tokens; the editor edits it as one whole value).
    fn parse_json_object_value(&mut self) {
        self.builder.start_node(SyntaxKind::JSON_VALUE.into());
        let mut depth = 0;
        loop {
            match self.cur() {
                None => break,
                Some(SyntaxKind::L_BRACE) => {
                    depth += 1;
                    self.bump();
                }
                Some(SyntaxKind::R_BRACE) => {
                    depth -= 1;
                    self.bump();
                    if depth == 0 {
                        break;
                    }
                }
                Some(_) => self.bump(),
            }
        }
        self.builder.finish_node();
    }

    /// True if the cursor begins an inline expression: `Type` then (after inline
    /// trivia) a `(`, `{`, `->`, or `.`.
    fn looks_like_inline_expr(&self) -> bool {
        // type must be uppercase-leading IDENT or Group keyword
        let is_type = self
            .tokens
            .get(self.pos)
            .map(|t| t.kind == SyntaxKind::KW_GROUP
                || (t.kind == SyntaxKind::IDENT
                    && t.text.chars().next().map(|c| c.is_ascii_uppercase()).unwrap_or(false)))
            .unwrap_or(false);
        if !is_type {
            return false;
        }
        // find next significant after the type token
        let mut i = self.pos + 1;
        while let Some(t) = self.tokens.get(i) {
            if t.kind.is_trivia() {
                i += 1;
                continue;
            }
            return matches!(
                t.kind,
                SyntaxKind::L_PAREN | SyntaxKind::L_BRACE | SyntaxKind::ARROW | SyntaxKind::DOT
            );
        }
        false
    }

    fn next_significant_is_dot(&self) -> bool {
        let mut i = self.pos + 1;
        while let Some(t) = self.tokens.get(i) {
            if t.kind.is_trivia() {
                i += 1;
                continue;
            }
            return t.kind == SyntaxKind::DOT;
        }
        false
    }

    /// Inline expression value: `Type (sig) -> (sig) { body } .port`. We reuse
    /// header/body machinery loosely: bump Type, optional sigs, optional body,
    /// optional `.port`. All tokens preserved under INLINE_EXPR.
    fn parse_inline_expr(&mut self) {
        self.builder.start_node(SyntaxKind::INLINE_EXPR.into());
        self.bump(); // Type
        self.skip_inline_trivia_then(|p| {
            if p.cur() == Some(SyntaxKind::L_PAREN) {
                p.bump_balanced_parens_as_ports();
            }
        });
        self.skip_inline_trivia_then(|p| {
            if p.cur() == Some(SyntaxKind::ARROW) {
                p.bump();
                p.bump_trivia_inline();
                if p.cur() == Some(SyntaxKind::L_PAREN) {
                    p.bump_balanced_parens_as_ports();
                }
            }
        });
        // optional body
        self.skip_inline_trivia_then(|p| {
            if p.peek_significant().map(|(_, k)| k) == Some(SyntaxKind::L_BRACE) {
                p.maybe_parse_body(false);
            }
        });
        // optional `.port`
        self.skip_inline_trivia_then(|p| {
            if p.cur() == Some(SyntaxKind::DOT) {
                p.bump();
                if p.cur() == Some(SyntaxKind::IDENT) {
                    p.bump();
                }
            }
        });
        self.builder.finish_node();
    }

    /// A connection line: `ENDPOINT = ENDPOINT` (or `ENDPOINT = value` for a
    /// connection-origin config field). Both sides are ENDPOINT when dotted.
    fn parse_connection(&mut self) {
        self.builder.start_node(SyntaxKind::CONNECTION.into());
        self.bump_trivia();
        self.parse_endpoint(); // target
        self.bump_trivia_inline();
        if self.cur() == Some(SyntaxKind::EQ) {
            self.bump();
        }
        self.bump_trivia_inline();
        // RHS: an endpoint (dotted) or a plain value (connection-origin config).
        if matches!(self.cur(), Some(SyntaxKind::IDENT)) && self.next_significant_is_dot() {
            self.parse_endpoint();
        } else {
            self.parse_value();
        }
        self.bump_trailing_same_line();
        self.builder.finish_node();
    }

    /// An endpoint: `ident.port`, `self.port`, or a bare `ident`.
    fn parse_endpoint(&mut self) {
        self.builder.start_node(SyntaxKind::ENDPOINT.into());
        if self.cur() == Some(SyntaxKind::IDENT) {
            self.bump();
        }
        // Consume the full dotted chain (`a.b`, and any further `.c.d` of a
        // MALFORMED 3+-segment ref) so it stays inside this ENDPOINT node rather
        // than leaking a stray `.c` to the file loop. A valid endpoint is exactly
        // `id.port`; the lowering's `endpoint_id_port` reads the first two
        // segments and the extra segments make it an unresolved (loud) endpoint.
        while self.cur() == Some(SyntaxKind::DOT) {
            self.bump();
            if self.cur() == Some(SyntaxKind::IDENT) {
                self.bump();
            }
        }
        self.builder.finish_node();
    }

    /// Lenient fallback: consume the rest of the current logical line as an
    /// ERROR node so a malformed line never blocks parsing the rest.
    fn parse_error_line(&mut self) {
        self.builder.start_node(SyntaxKind::ERROR.into());
        self.bump_trivia();
        // consume significant tokens until newline-bearing whitespace
        let mut seen = false;
        while let Some(k) = self.cur() {
            if k == SyntaxKind::WHITESPACE && self.tokens[self.pos].text.contains('\n') && seen {
                break;
            }
            if !k.is_trivia() {
                seen = true;
            }
            self.bump();
        }
        self.builder.finish_node();
    }

    // ── small helpers ──────────────────────────────────────────────────────

    /// Bump the next significant token (skipping leading trivia by emitting it).
    fn bump_significant(&mut self) {
        self.bump_trivia_inline();
        if self.cur().is_some() {
            self.bump();
        }
    }

    /// Bump tokens up to (not including) the next EQ, emitting them. Used for the
    /// include LHS (just the IDENT).
    fn bump_significant_until_eq(&mut self) {
        self.bump_trivia_inline();
        while let Some(k) = self.cur() {
            if k == SyntaxKind::EQ {
                break;
            }
            self.bump();
        }
    }

    /// Bump only inline (non-newline) whitespace + comments, leaving a
    /// line-ending whitespace token in place.
    fn bump_trivia_inline(&mut self) {
        while let Some(k) = self.cur() {
            if !k.is_trivia() {
                break;
            }
            let text = self.tokens[self.pos].text;
            if k == SyntaxKind::WHITESPACE && text.contains('\n') {
                break;
            }
            self.bump();
        }
    }

    /// Bump inline trivia, then run `f`. Keeps signature-chaining readable.
    fn skip_inline_trivia_then(&mut self, f: impl FnOnce(&mut Self)) {
        self.bump_trivia_inline();
        f(self);
    }
}

/// The shape of one logical line, computed ONCE by `classify` from the
/// significant-token run and consumed by every parse site (top-level + body), so
/// there is a single strict definition of what forms are accepted. Anything that
/// fits no variant is `Unknown` and becomes one ERROR node (fail-loud: the CST
/// keeps a byte-covering error span; the lowering drops it and the structural
/// check reports it). The accepted forms are a WHITELIST: `Node`, `Group`,
/// `AnonGroup`, `Include`, `Connection`, `Field`, `Directive`. New syntax is a
/// new variant here, never a permissive fall-through.
enum LineShape {
    /// `id = Type ...` (Type is a non-keyword IDENT).
    Node,
    /// `id = Group ...`.
    Group,
    /// A leading `Group` keyword with no `=` (an included file's sole top-level
    /// group, which has no name).
    AnonGroup,
    /// `id = Loop ...`.
    Loop,
    /// `alias = @include("...")` (the RHS marker's directive is exactly `include`).
    Include,
    /// `target.port = ...` (LHS is exactly `IDENT . IDENT`).
    Connection,
    /// `key: value` (a config field; body only).
    Field,
    /// A leading `@require_one_of(...)` marker (body only).
    Directive,
    /// Fits no accepted form: a malformed line, parsed as one ERROR node.
    Unknown,
}

/// True if a COMMENT's text is a `# Description:` line (the group-description
/// convention). Tolerates leading whitespace inside the comment slice and an
/// optional space after `#`.
fn is_description_comment(text: &str) -> bool {
    let t = text.trim_start();
    let body = t.strip_prefix('#').map(|s| s.trim_start()).unwrap_or("");
    body.starts_with("Description:")
}

/// The token kinds that can stand as a scalar config value (`key: <value>`).
/// A structural token (`}`, `,`, whitespace) is NOT a value: an editor relies on
/// the field node containing only its value, so a `key:` with nothing must parse
/// to an empty value, not absorb the next structural token.
fn is_value_token(k: SyntaxKind) -> bool {
    use SyntaxKind as K;
    matches!(k, K::STRING | K::NUMBER | K::HEREDOC | K::JSON_VALUE | K::MARKER | K::IDENT | K::KW_GROUP)
}

/// The directive name of a marker token: the identifier after `@`, up to the
/// first `(`. `@include("x")` -> `include`, `@includes_other(...)` ->
/// `includes_other` (so an EXACT compare distinguishes them, no prefix match).
fn marker_directive(marker_text: &str) -> &str {
    super::marker::directive(marker_text)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE invariant: parse then serialize == source, byte for byte.
    fn assert_round_trip(src: &str) {
        let tree = parse(src);
        assert_eq!(tree.to_string(), src, "CST must round-trip byte-exact");
    }

    #[test]
    fn round_trip_every_model_shape() {
        // §4.1 bare node
        assert_round_trip("n = Debug\n");
        // §4.2 inline body
        assert_round_trip("n = Text { value: \"x\" }\n");
        // §4.3 multi-line body + heredoc
        assert_round_trip("n = Code {\n  lang: \"python\"\n  src: ```\nprint(1)\n```\n}\n");
        // §4.4 multi-line JSON array value
        assert_round_trip("n = T {\n  items: [\n    {\"a\": 1},\n    {\"b\": 2}\n  ]\n}\n");
        // JSON object value
        assert_round_trip("n = T {\n  cfg: { \"a\": 1, \"b\": [2, 3] }\n}\n");
        // §4.5 connection
        assert_round_trip("b.data = a.value\n");
        // §4.6 connection-origin config field
        assert_round_trip("t.style = \"a\"\n");
        // §4.7 inline empty group
        assert_round_trip("g = Group() -> () {}\n");
        // §4.7 multi-line body bare close
        assert_round_trip("g = Group() {\n  x = Text {}\n}\n");
        // Removed post-body ports syntax: still LOSSLESSLY round-trips
        // (the `-> (...)` after `}` parses as an ERROR node whose text
        // is preserved); acceptance is pinned rejected by
        // `post_body_output_syntax_is_no_longer_accepted`.
        assert_round_trip("g = Group() {\n  x = Text {}\n} -> (out: String)\n");
        assert_round_trip("g = Group() {\n  x = Text {}\n}\n-> (out: String)\n");
        // §4.8 multi-line signature
        assert_round_trip("g = Group(\n  a: String\n) -> () {}\n");
        // §4.9 inline expression as value
        assert_round_trip("greeting = Template { template: Upper { text: \"hi\" }.out }\n");
        // §4.10 include
        assert_round_trip("myThing = @include(\"sub.weft\")\n");
        // description + comments + blank lines
        assert_round_trip("# top comment\n\ng = Group() {\n  # Description: does things\n  x = Text {}  # trailing\n}\n\n");
        // optional port
        assert_round_trip("g = Group(a: String, b: Int?) -> (out: T) {}\n");
        // require_one_of directive
        assert_round_trip("g = Group(a: String) {\n  @require_one_of(a, b)\n}\n");
    }

    #[test]
    fn round_trip_messy_whitespace_and_alignment() {
        assert_round_trip("n   =   Text   {   value:   \"x\"   }\n");
        assert_round_trip("\n\n\nn = Debug\n\n\n");
        assert_round_trip("n = Text {\n\n  value: \"x\"\n\n}\n");
    }

    #[test]
    fn round_trip_empty_and_comment_only() {
        assert_round_trip("");
        assert_round_trip("\n");
        assert_round_trip("# just a comment\n");
        // Top-level `#` lines are plain comments with no special meaning (there is
        // no project header; a group's `# Description:` is semantic only as a
        // group's first BODY line, not at file top). They round-trip verbatim.
        assert_round_trip("# any comment\n# another comment\n");
    }

    /// Soak: a dependency-free LCG drives a generator that assembles random
    /// `.weft`-shaped source from grammar fragments, then asserts the round-trip
    /// invariant on every sample. The point of the whole CST is that NO input
    /// breaks round-trip; this stresses that across thousands of random shapes,
    /// including the gnarly trivia/heredoc/JSON/post-port combinations.
    #[test]
    fn round_trip_soak_random_shapes() {
        let mut rng = 0x2545F4914F6CDD1Du64; // seed
        let mut next = || {
            // xorshift64
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let fragments: &[&str] = &[
            "n = Debug\n",
            "n = Text { value: \"x\" }\n",
            "  k: \"v\"\n",
            "\n",
            "# a comment\n",
            "  # Description: d\n",
            "a.b = c.d\n",
            "t.style = \"a\"\n",
            "g = Group() {\n",
            "} -> (out: String)\n",
            "}\n",
            "  src: ```\ncode line\n```\n",
            "  items: [1, 2, 3]\n",
            "  cfg: { \"a\": 1 }\n",
            "x = @include(\"s.weft\")\n",
            "  @require_one_of(a, b)\n",
            "   \t  \n",
            // adversarial: markers/heredocs/JSON with embedded delimiters,
            // inline-expr values, malformed lines, multi-line sigs, nested groups.
            "  f: @file(\"a, b.txt\")\n",            // marker with a comma in the arg
            "  s: ```\nhas \\``` inside\nmore\n```\n", // escaped fence inside heredoc
            "  j: { \"a\": \"]}\", \"b\": [1] }\n",    // JSON with brackets in strings
            "  v: Upper { text: \"hi\" }.out\n",      // inline-expr value
            "a.b.c = d\n",                            // malformed 3-segment LHS
            "= broken\n",                             // malformed leading =
            "n = T(\n  a: String,\n  b: Number\n)\n", // multi-line signature
            "outer = Group() {\n  inner = Group() {\n",
            "    deep: \"v\"\n",
            "\u{00a0}\n",                              // a lone NBSP (ERROR token)
        ];
        for _ in 0..5000 {
            let n = (next() % 8) as usize + 1;
            let mut src = String::new();
            for _ in 0..n {
                let f = fragments[(next() as usize) % fragments.len()];
                src.push_str(f);
            }
            let tree = parse(&src);
            // Round-trip is the headline invariant (no lost/dup/reordered bytes),
            // and the parse is total (this line panicking IS the no-panic check).
            assert_eq!(
                tree.to_string(),
                src,
                "round-trip broke on generated source:\n{src:?}"
            );
            assert_eq!(tree.kind(), SyntaxKind::WEFT_FILE, "root is always a WEFT_FILE");
        }
    }
}
