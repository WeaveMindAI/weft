//! Weft Compiler: compiles Weft source code into a flat ProjectDefinition.
//!
//! The compiler:
//! 1. Parses Weft syntax (nodes, groups, connections with assignment syntax)
//! 2. Flattens groups by injecting Passthrough nodes at group boundaries
//! 3. Produces a flat ProjectDefinition ready for execution
//!
//! This is a pure function: &str -> Result<ProjectDefinition, Vec<CompileError>>

use uuid::Uuid;

use weft_core::node::NodeFeatures;
use weft_core::project::{
    Edge, GroupBoundary, GroupBoundaryRole, LaneMode, NodeDefinition, PortDefinition, Position,
    ProjectDefinition, Span,
};
use weft_core::weft_type::WeftType;

// ─── Compiler Error ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CompileError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

// ─── Intermediate Representations ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ParsedPort {
    name: String,
    port_type: WeftType,
    required: bool,
    lane_mode: LaneMode,
}

#[derive(Debug, Clone)]
struct ParsedNode {
    id: String,
    node_type: String,
    label: Option<String>,
    config: serde_json::Map<String, serde_json::Value>,
    parent_id: Option<String>,
    in_ports: Vec<ParsedPort>,
    out_ports: Vec<ParsedPort>,
    one_of_required: Vec<Vec<String>>,
    /// Full source range of the declaration (header + config block).
    /// None for synthetic nodes (inline-expression children created
    /// during parsing have a span covering the inline fragment).
    span: Option<Span>,
    /// Source range of the header line (`id = NodeType`), used when
    /// adding a config field to a bare node.
    header_span: Option<Span>,
    /// Span per config field name.
    config_spans: std::collections::BTreeMap<String, Span>,
}

#[derive(Debug, Clone)]
struct ParsedConnection {
    source_id: String,
    source_port: String,
    target_id: String,
    target_port: String,
    /// Source range of the connection line (`target.port = source.port`).
    /// None for synthetic edges produced by inline expressions.
    span: Option<Span>,
}

#[derive(Debug, Clone)]
struct ParsedGroup {
    id: String,
    in_ports: Vec<ParsedPort>,
    out_ports: Vec<ParsedPort>,
    /// @require_one_of groups declared on the group's input port signature.
    /// Each inner Vec is a group of input port names where at least one must
    /// have a non-null value at runtime, otherwise the whole group body is
    /// skipped and all group outputs emit null downstream.
    one_of_required: Vec<Vec<String>>,
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
    child_groups: Vec<ParsedGroup>,
    /// Source range covering the entire `label = Group(...) -> (...) { body }`
    /// block. Used by the webview / surgical layer to delete or rewrite the
    /// whole group region.
    span: Option<Span>,
    /// Source range of the header line (`label = Group(ports) -> (ports) {`).
    /// Used for targeted port-signature rewrites.
    header_span: Option<Span>,
}

struct ParseState {
    name: String,
    description: String,
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
    groups: Vec<ParsedGroup>,
    errors: Vec<CompileError>,
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Compile Weft source code into a flat ProjectDefinition.
///
/// `project_id` must be the real DB project_id. The compiler can't derive
/// it from source (the source has no id field), and downstream consumers
/// (orchestrator ownership guard, billing) trust this id, so making it a
/// required parameter prevents the "forgot to overwrite a random UUID"
/// class of bug.
///
/// Groups are flattened: each group produces two Passthrough nodes
/// ({groupId}__in and {groupId}__out) with edges rewired accordingly.
pub fn compile(source: &str, project_id: Uuid) -> Result<ProjectDefinition, Vec<CompileError>> {
    let state = parse_weft(source);
    if !state.errors.is_empty() {
        return Err(state.errors);
    }
    flatten(state, project_id)
}

// ─── Parser ──────────────────────────────────────────────────────────────────

/// Accumulator for inline-expression children and their connection edges.
/// When a `key: Type { ... }.port` inline is detected inside a config block
/// (or on the RHS of a connection), the parser appends the resulting child
/// node and the synthetic edge to this scope. The caller merges them into
/// its own scope (root project or group body).
///
/// `config_fills` collects `target.port = literal` assignments: the parser
/// emits these here instead of as edges so the caller can apply them to the
/// corresponding node's config map. Later writes override earlier ones for
/// the same (target, port) pair.
#[derive(Default)]
struct InlineScope {
    nodes: Vec<ParsedNode>,
    connections: Vec<ParsedConnection>,
    config_fills: Vec<ConfigFill>,
}

#[derive(Debug, Clone)]
struct ConfigFill {
    target_id: String,
    target_port: String,
    value: serde_json::Value,
}

fn parse_weft(source: &str) -> ParseState {
    let lines: Vec<&str> = source.lines().collect();
    let mut state = ParseState {
        name: "Untitled Project".to_string(),
        description: String::new(),
        nodes: Vec::new(),
        connections: Vec::new(),
        groups: Vec::new(),
        errors: Vec::new(),
    };

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        let line_num = i + 1;

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            // Parse metadata headers
            if let Some(rest) = trimmed.strip_prefix("# Project:") {
                state.name = rest.trim().to_string();
            } else if let Some(rest) = trimmed.strip_prefix("# Description:") {
                state.description = rest.trim().to_string();
            }
            i += 1;
            continue;
        }

        // Node or Group declaration: id = Type(...) -> (...) { ... }
        let mut inline_scope = InlineScope::default();
        if let Some((result, next_i)) = try_parse_declaration(&lines, i, &mut state.errors, &mut inline_scope) {
            match result {
                Declaration::Node(node) => {
                    if state.nodes.iter().any(|n| n.id == node.id) {
                        state.errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}'", node.id) });
                    }
                    state.nodes.push(node);
                }
                Declaration::Group(group) => {
                    if state.groups.iter().any(|g| g.id == group.id) {
                        state.errors.push(CompileError { line: line_num, message: format!("Duplicate group name '{}'", group.id) });
                    }
                    state.groups.push(group);
                }
            }
            // Merge any inline children produced by this declaration.
            for child in inline_scope.nodes {
                if state.nodes.iter().any(|n| n.id == child.id) {
                    state.errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}' (generated from inline expression)", child.id) });
                }
                state.nodes.push(child);
            }
            state.connections.extend(inline_scope.connections);
            i = next_i;
            continue;
        }

        // Connection: target.port = source.port | inline expression | literal config fill
        let mut conn_scope = InlineScope::default();
        match try_parse_connection_with_inline(&lines, i, &mut state.errors, &mut conn_scope) {
            ParseConnectionResult::Edge(conn, next_i) => {
                state.connections.push(conn);
                for child in conn_scope.nodes {
                    if state.nodes.iter().any(|n| n.id == child.id) {
                        state.errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}' (generated from inline expression)", child.id) });
                    }
                    state.nodes.push(child);
                }
                state.connections.extend(conn_scope.connections);
                for fill in conn_scope.config_fills {
                    apply_config_fill(&mut state.nodes, fill);
                }
                i = next_i;
                continue;
            }
            ParseConnectionResult::ConfigFill(next_i) => {
                for fill in conn_scope.config_fills {
                    apply_config_fill(&mut state.nodes, fill);
                }
                i = next_i;
                continue;
            }
            ParseConnectionResult::NotAConnection => {}
        }

        // Unknown line
        state.errors.push(CompileError { line: line_num, message: format!("Unexpected: {}", trimmed) });
        i += 1;
    }

    state
}

// ─── Declaration Parsing ────────────────────────────────────────────────────

enum Declaration {
    Node(ParsedNode),
    Group(ParsedGroup),
}

/// Try to parse a declaration: `id = Type(...)` or `id = Type { ... }` or `id = Type`
/// If Type is "Group", parse as a group (body contains children).
/// Port signatures can span multiple lines: `id = Type(\n  port1,\n  port2\n) -> (\n  out\n) {`
fn try_parse_declaration(
    lines: &[&str],
    start: usize,
    errors: &mut Vec<CompileError>,
    inline_scope: &mut InlineScope,
) -> Option<(Declaration, usize)> {
    let trimmed = lines[start].trim();
    let line_num = start + 1;

    // Match: id = Type  (then optionally ( or { on same line)
    let eq_pos = trimmed.find('=')?;

    // Make sure this isn't a connection (target.port = source.port)
    let left = trimmed[..eq_pos].trim();
    if left.contains('.') {
        return None; // This is a connection, not a declaration
    }

    let right = trimmed[eq_pos + 1..].trim();

    // Validate identifier
    if left.is_empty() { return None; }
    let first = left.chars().next()?;
    if !first.is_alphabetic() && first != '_' { return None; }
    if !left.chars().all(|c| c.is_alphanumeric() || c == '_') { return None; }

    // Check reserved words
    if left == "self" {
        errors.push(CompileError { line: line_num, message: "'self' is a reserved word and cannot be used as an identifier".to_string() });
        return None;
    }

    let id = left.to_string();

    // Extract the type name
    let type_end = right.find(|c: char| c == '(' || c == '{' || c.is_whitespace()).unwrap_or(right.len());
    let node_type = right[..type_end].trim().to_string();

    if node_type.is_empty() { return None; }
    if !node_type.chars().next()?.is_uppercase() { return None; }
    if !node_type.chars().all(|c| c.is_alphanumeric()) { return None; }

    // Passthrough is a compiler-internal node type emitted by the
    // group-flattening pass. Users cannot declare it directly.
    if node_type == "Passthrough" {
        errors.push(CompileError {
            line: line_num,
            message: "'Passthrough' is a compiler-internal node type and cannot be used directly. \
                      Passthrough nodes are emitted automatically when a group is flattened."
                .into(),
        });
        return None;
    }

    let after_type = right[type_end..].trim();

    // Collect the full declaration header across multiple lines. If
    // `after_type` starts with `(` or `->`, find the matching `)` then
    // optionally `-> (...)`, then `{` or end. Use the `_with_body` variant
    // so that one-liner-style headers with multi-line triple-backtick
    // values (like `n = Type { code: ``` \n ... \n ``` }`) are collected
    // fully. Inline expressions use the non-collecting variant elsewhere.
    let (header_text, header_end_line) = if after_type.starts_with('(') || after_type.starts_with("->") {
        collect_declaration_header_with_body(lines, start, after_type)
    } else {
        (after_type.to_string(), start)
    };

    let header_text = header_text.trim();

    // Parse optional port signature from the collected header
    let (in_ports, out_ports, one_of_required, after_ports) = if header_text.starts_with('(') {
        parse_port_signature(header_text, line_num, errors)
    } else if header_text.starts_with("->") {
        // No input ports, just -> (outputs)
        let arrow_rest = header_text.strip_prefix("->").unwrap().trim();
        if arrow_rest.starts_with('(') {
            let mut out_ports = Vec::new();
            let mut one_of_required = Vec::new();
            let (output_content, after_output) = match find_matching_paren(arrow_rest) {
                Some((content, rest)) => (content, rest),
                None => {
                    errors.push(CompileError { line: line_num, message: "Unclosed output port list '('".to_string() });
                    (String::new(), String::new())
                }
            };
            parse_port_list(&output_content, &mut out_ports, &mut one_of_required, "out", line_num, errors);
            (Vec::new(), out_ports, one_of_required, after_output)
        } else {
            (Vec::new(), Vec::new(), Vec::new(), header_text.to_string())
        }
    } else {
        (Vec::new(), Vec::new(), Vec::new(), header_text.to_string())
    };

    let after_ports = after_ports.trim();

    // Detect wrong-order post-config outputs: `-> (pre) -> (post) { config }`.
    // The correct order is `-> (pre) { config } -> (post)`.
    if after_ports.starts_with("->") {
        errors.push(CompileError {
            line: line_num,
            message: "Two arrow clauses before the config block. You wrote: Type -> (out: T) -> (extra: T2) { config }. Fix: merge both port lists into one: Type -> (out: T, extra: T2) { config }. Just add the extra ports to the first arrow clause. Other errors below are likely caused by this, ignore them until this is fixed.".to_string(),
        });
    }

    // The body parsing should start from header_end_line, not start
    let body_start_line = header_end_line;

    // Strip inline comments from after_ports
    let after_ports_clean = if let Some(hash_pos) = after_ports.find('#') {
        if hash_pos > 0 { after_ports[..hash_pos].trim() } else { after_ports }
    } else {
        after_ports
    };

    if node_type == "Group" {
        // The header occupies lines [line_num .. header_end_line+1].
        let header_span = Some(Span {
            start_line: line_num,
            start_col: 0,
            end_line: header_end_line + 1,
            end_col: 0,
        });
        // Parse as group
        let group = if after_ports_clean.starts_with('{') {
            if after_ports_clean == "{}" || (after_ports_clean.starts_with('{') && after_ports_clean.ends_with('}') && after_ports_clean.len() > 1) {
                // One-liner empty group or group with inline body.
                let span = Some(Span {
                    start_line: line_num,
                    start_col: 0,
                    end_line: header_end_line + 1,
                    end_col: 0,
                });
                ParsedGroup {
                    id, in_ports, out_ports,
                    one_of_required,
                    nodes: Vec::new(), connections: Vec::new(), child_groups: Vec::new(),
                    span,
                    header_span,
                }
            } else if after_ports_clean == "{" {
                // Multi-line group body. Note: inline children inside group
                // nodes are added to the group's own `nodes` list by
                // parse_group_body (which maintains its own InlineScope),
                // not to the top-level inline_scope we received.
                let (mut group, next_i) = parse_group_body(lines, body_start_line, &id, in_ports, out_ports, one_of_required, errors);
                group.header_span = header_span;
                group.span = Some(Span {
                    start_line: line_num,
                    start_col: 0,
                    end_line: next_i,
                    end_col: 0,
                });
                return Some((Declaration::Group(group), next_i));
            } else {
                errors.push(CompileError { line: line_num, message: format!("Invalid group syntax: {}", trimmed) });
                return None;
            }
        } else if after_ports_clean.is_empty() {
            // No body: bare group.
            let span = Some(Span {
                start_line: line_num,
                start_col: 0,
                end_line: header_end_line + 1,
                end_col: 0,
            });
            ParsedGroup {
                id, in_ports, out_ports,
                one_of_required,
                nodes: Vec::new(), connections: Vec::new(), child_groups: Vec::new(),
                span,
                header_span,
            }
        } else {
            errors.push(CompileError { line: line_num, message: format!("Unexpected after group declaration: {}", after_ports_clean) });
            return None;
        };
        Some((Declaration::Group(group), body_start_line + 1))
    } else {
        // Parse as node
        //
        // Handle one-liner with post-config outputs on the same line:
        //   LlmInference -> (response: JsonDict) { parseJson: true } -> (summary: String)
        // Split into config part `{ parseJson: true }` and post-config `-> (summary: String)`.
        let (after_ports_for_config, one_liner_post_config): (String, Option<String>) = if after_ports_clean.starts_with('{')
            && !after_ports_clean.ends_with('}')
        {
            // Find the closing brace that ends the config, respecting nesting
            let mut depth = 0i32;
            let mut in_quote = false;
            let mut split_pos = None;
            for (i, c) in after_ports_clean.char_indices() {
                if c == '"' { in_quote = !in_quote; continue; }
                if in_quote { continue; }
                if c == '{' { depth += 1; }
                if c == '}' {
                    depth -= 1;
                    if depth == 0 { split_pos = Some(i); break; }
                }
            }
            if let Some(pos) = split_pos {
                let config_part = &after_ports_clean[..pos + 1];
                let rest = after_ports_clean[pos + 1..].trim();
                if rest.starts_with("->") {
                    (config_part.to_string(), Some(rest.strip_prefix("->").unwrap().trim().to_string()))
                } else {
                    (after_ports_clean.to_string(), None)
                }
            } else {
                (after_ports_clean.to_string(), None)
            }
        } else {
            (after_ports_clean.to_string(), None)
        };

        let (config, label, next_i) = if after_ports_for_config.starts_with('{') {
            if after_ports_for_config.ends_with('}') && after_ports_for_config.len() > 1 {
                // One-liner: { key: val, key: val }. Each pair value may be
                // a literal, a port wiring (dotted ref), or an inline
                // expression (`Type { ... }.port` or bare `Type.port`).
                // The splitter is brace-aware, so inline values with nested
                // braces stay in the same pair.
                let body = &after_ports_for_config[1..after_ports_for_config.len() - 1].trim();
                let mut config = serde_json::Map::new();
                let mut label = None;
                if !body.is_empty() {
                    for pair in split_respecting_quotes(body, ',') {
                        let pair = pair.trim();
                        if pair.is_empty() { continue; }
                        let colon_pos = match pair.find(':') {
                            Some(p) => p,
                            None => {
                                errors.push(CompileError { line: line_num, message: format!("Invalid config pair: '{}'", pair) });
                                continue;
                            }
                        };
                        let key = pair[..colon_pos].trim();
                        let val = pair[colon_pos + 1..].trim();
                        // Inline expression FIRST. This is important for
                        // the bare form `Type.port`, which would otherwise
                        // look like a dotted ref (`Text.value`) and be
                        // consumed by the port-wiring branch below. Type
                        // names start with uppercase, node ids don't, so
                        // looks_like_inline_start and looks_like_dotted_ref
                        // are mutually exclusive on valid input.
                        if is_valid_config_key(key) && looks_like_inline_start(val) {
                            let synth = vec![val];
                            let _ = try_parse_inline_expression(
                                &synth, 0, 0, &id, key, inline_scope, errors,
                            );
                            continue;
                        }
                        // Port wiring: unquoted dotted ref on the RHS emits
                        // an edge from source.port to parent.key. Enrichment
                        // validates the target is a real input port.
                        if is_valid_config_key(key) && looks_like_dotted_ref(val) {
                            if let Some((src_id, src_port)) = parse_dotted(val) {
                                inline_scope.connections.push(ParsedConnection {
                                    source_id: src_id,
                                    source_port: src_port,
                                    target_id: id.clone(),
                                    target_port: key.to_string(),
                                    span: None,
                                });
                                continue;
                            }
                        }
                        // Multi-line triple-backtick literal: `key: ``` ... ``` `
                        // where the value spans newlines in the joined body.
                        // Strip the delimiters, dedent, unescape.
                        if is_valid_config_key(key) && val.starts_with("```") && val.ends_with("```") && val.len() >= 6 {
                            let inner = &val[3..val.len() - 3];
                            // Strip a single leading/trailing newline so
                            // `key: ``` \n content \n ``` ` becomes "content".
                            let inner = inner.strip_prefix('\n').unwrap_or(inner);
                            let inner = inner.strip_suffix('\n').unwrap_or(inner);
                            let dedented = dedent(inner);
                            let unescaped = dedented.replace("\\```", "```").replace("\\`", "`");
                            config.insert(key.to_string(), serde_json::Value::String(unescaped));
                            continue;
                        }
                        if let Some(l) = try_extract_label(pair) {
                            label = Some(l);
                        } else {
                            parse_kv(pair, &mut config, line_num, errors);
                        }
                    }
                }
                (config, label, body_start_line + 1)
            } else if after_ports_for_config == "{" {
                // Multi-line config block. The parser detects inline expressions
                // inside config-field values and emits child nodes + edges into
                // inline_scope. Anon IDs are generated from the parent id + field
                // name (e.g. `llm_config__systemPrompt`).
                let (config, label, extra_in, extra_out, extra_oor, mut end_i) = parse_config_block(lines, body_start_line, line_num, errors, &id, inline_scope);
                let mut all_in = in_ports.clone();
                let mut all_out = out_ports.clone();
                all_in.extend(extra_in);
                all_out.extend(extra_out);
                let mut all_oor = one_of_required.clone();
                all_oor.extend(extra_oor);
                // Check for post-config output ports: } -> (outputs)
                // The `->` can be on its own line, or on the same line as `}` (e.g. `} -> (out: String)`)
                let mut peek_i = end_i;
                // If parse_config_block returned pointing at a `} -> ...` line, extract the arrow part
                let mut arrow_on_brace_line: Option<String> = None;
                if peek_i < lines.len() {
                    let peek_trimmed = lines[peek_i].trim();
                    if peek_trimmed.starts_with('}') {
                        let after_brace = peek_trimmed[1..].trim();
                        if after_brace.starts_with("->") {
                            arrow_on_brace_line = Some(after_brace.strip_prefix("->").unwrap().trim().to_string());
                        }
                        peek_i += 1;
                    }
                }
                while peek_i < lines.len() && lines[peek_i].trim().is_empty() { peek_i += 1; }
                // Determine arrow_rest: either from the `} ->` line or from a standalone `->` line
                let arrow_rest_str: Option<String> = if let Some(ref ar) = arrow_on_brace_line {
                    Some(ar.clone())
                } else if peek_i < lines.len() && lines[peek_i].trim().starts_with("->") {
                    Some(lines[peek_i].trim().strip_prefix("->").unwrap().trim().to_string())
                } else {
                    None
                };
                if let Some(arrow_rest) = arrow_rest_str {
                    let arrow_line = if arrow_on_brace_line.is_some() { end_i } else { peek_i };
                    // Collect multi-line output ports
                    let mut out_sig = arrow_rest.to_string();
                    let mut out_end = arrow_line;
                    let mut paren_depth: i32 = 0;
                    for c in arrow_rest.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                    while paren_depth > 0 && out_end + 1 < lines.len() {
                        out_end += 1;
                        let ol = lines[out_end].trim();
                        out_sig.push(' ');
                        out_sig.push_str(ol);
                        for c in ol.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                    }
                    if out_sig.starts_with('(') {
                        if let Some((content, _rest)) = find_matching_paren(&out_sig) {
                            let existing_names: std::collections::HashSet<String> = all_out.iter().map(|p| p.name.clone()).collect();
                            let mut post_ports = Vec::new();
                            let mut post_oor = Vec::new();
                            parse_port_list(&content, &mut post_ports, &mut post_oor, "out", line_num, errors);
                            for p in post_ports {
                                if existing_names.contains(&p.name) {
                                    errors.push(CompileError { line: line_num, message: format!("Duplicate output port '{}', already declared before the config block", p.name) });
                                } else {
                                    all_out.push(p);
                                }
                            }
                            all_oor.extend(post_oor);
                        }
                    }
                    end_i = out_end + 1;
                }
                let node = ParsedNode {
                    id, node_type: node_type, label, config,
                    parent_id: None, in_ports: all_in, out_ports: all_out, one_of_required: all_oor,
                    span: Some(Span { start_line: line_num, start_col: 0, end_line: end_i, end_col: 0 }),
                    header_span: Some(Span::single_line(line_num, 0, lines[start].len())),
                    config_spans: Default::default(),
                };
                return Some((Declaration::Node(node), end_i));
            } else {
                errors.push(CompileError { line: line_num, message: format!("Invalid node syntax: {}", trimmed) });
                return None;
            }
        } else if after_ports_clean.is_empty() {
            // Bare node, no config
            (serde_json::Map::new(), None, body_start_line + 1)
        } else {
            errors.push(CompileError { line: line_num, message: format!("Unexpected after node declaration: {}", after_ports_clean) });
            return None;
        };

        // Handle post-config outputs from one-liner syntax:
        //   Type -> (response: JsonDict) { parseJson: true } -> (summary: String, score: Number)
        let mut final_out = out_ports;
        let mut final_oor = one_of_required;
        if let Some(ref post_config_str) = one_liner_post_config {
            if post_config_str.starts_with('(') {
                if let Some((content, _rest)) = find_matching_paren(post_config_str) {
                    let existing_names: std::collections::HashSet<String> = final_out.iter().map(|p| p.name.clone()).collect();
                    let mut post_ports = Vec::new();
                    let mut post_oor = Vec::new();
                    parse_port_list(&content, &mut post_ports, &mut post_oor, "out", line_num, errors);
                    for p in post_ports {
                        if existing_names.contains(&p.name) {
                            errors.push(CompileError { line: line_num, message: format!("Duplicate output port '{}', already declared before the config block", p.name) });
                        } else {
                            final_out.push(p);
                        }
                    }
                    final_oor.extend(post_oor);
                }
            }
        }

        let node = ParsedNode {
            id, node_type: node_type, label, config,
            parent_id: None, in_ports: in_ports, out_ports: final_out, one_of_required: final_oor,
            span: Some(Span { start_line: line_num, start_col: 0, end_line: next_i, end_col: 0 }),
            header_span: Some(Span::single_line(line_num, 0, lines[start].len())),
            config_spans: Default::default(),
        };
        Some((Declaration::Node(node), next_i))
    }
}

/// Count structural parens in a line, ignoring parens inside `@require_one_of(...)`.
/// This prevents directive-internal parens from polluting the port-list paren depth.
fn count_structural_parens(line: &str, depth: &mut i32, found_brace: &mut bool) {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip @require_one_of(...) directive, its parens are not structural
        if i + 16 <= bytes.len() && &line[i..i+16] == "@require_one_of(" {
            // Look ahead to find matching close paren
            let mut dir_depth: i32 = 1;
            let mut j = i + 16;
            while j < bytes.len() && dir_depth > 0 {
                if bytes[j] == b'(' { dir_depth += 1; }
                if bytes[j] == b')' { dir_depth -= 1; }
                if dir_depth > 0 { j += 1; }
            }
            // Skip to end of directive (or end of line if unbalanced)
            i = if dir_depth == 0 { j + 1 } else { bytes.len() };
            continue;
        }
        match bytes[i] {
            b'(' => *depth += 1,
            b')' => *depth -= 1,
            b'{' => *found_brace = true,
            _ => {}
        }
        i += 1;
    }
}

/// Collect the full declaration header that may span multiple lines.
/// Starting from `after_type` (which begins with `(`), collects lines until all parens
/// are balanced AND we've seen `{`, `{}`, or end of declaration.
/// Returns (collected_text, last_line_index).
fn collect_declaration_header(lines: &[&str], start: usize, after_type: &str) -> (String, usize) {
    collect_declaration_header_impl(lines, start, after_type, false)
}

/// Variant that also collects multi-line config body when the header has an
/// unclosed `{`. Only called from the top-level declaration path; inline
/// expressions use the non-collecting variant because they rely on matching
/// `}.port` on a later line.
fn collect_declaration_header_with_body(lines: &[&str], start: usize, after_type: &str) -> (String, usize) {
    collect_declaration_header_impl(lines, start, after_type, true)
}

fn collect_declaration_header_impl(
    lines: &[&str],
    start: usize,
    after_type: &str,
    collect_unclosed_body: bool,
) -> (String, usize) {
    // Count paren depth in the initial after_type
    let mut paren_depth: i32 = 0;
    let mut found_open_brace = false;

    count_structural_parens(after_type, &mut paren_depth, &mut found_open_brace);

    // If parens are balanced AND there's an open brace, normally nothing
    // more to collect. When `collect_unclosed_body` is true, we also
    // collect lines if the `{` has content after it (e.g. `{ code: ``` `)
    // that isn't balanced yet, so the entire one-liner-style body is
    // captured. A bare `{` at end of line (like `g = Group() -> (x) {`)
    // is a normal multi-line-block opener and is left alone.
    if paren_depth == 0 && found_open_brace {
        let needs_body_collection = collect_unclosed_body
            && first_brace_has_content_after(after_type)
            && !is_brace_balanced_respecting_quotes_and_backticks(after_type);
        if !needs_body_collection {
            return (after_type.to_string(), start);
        }
    }

    // Collect subsequent lines (or check for -> on next line if parens already balanced)
    let mut collected = after_type.to_string();
    let mut i = start + 1;

    while i < lines.len() && paren_depth > 0 {
        let line = lines[i].trim();
        collected.push('\n');
        collected.push_str(line);

        count_structural_parens(line, &mut paren_depth, &mut found_open_brace);
        i += 1;
    }

    // After parens balanced, check if there's more on this line or next lines
    // (like `-> (...)` or `{`)
    // Keep collecting until we find `{` or reach a line that doesn't look like continuation
    if paren_depth == 0 && !found_open_brace {
        // Check if the collected text ends with `->` or similar continuation
        let tail = collected.trim_end();
        if tail.ends_with("->") || tail.ends_with("-> (") {
            // Need more lines
        } else {
            // Check the remaining text after the last `)` for `->`
            // Actually, we need to peek at what comes after the balanced parens
            // Let's check if the rest of the current state has `->`
            let after_last = collected.rfind(')').map(|p| collected[p + 1..].trim().to_string()).unwrap_or_default();
            if after_last.starts_with("->") {
                let after_arrow = after_last.strip_prefix("->").unwrap().trim();
                if after_arrow.is_empty() || after_arrow == "(" {
                    // Need to collect the output ports too
                    while i < lines.len() {
                        let line = lines[i].trim();
                        collected.push('\n');
                        collected.push_str(line);

                        count_structural_parens(line, &mut paren_depth, &mut found_open_brace);
                        i += 1;

                        if paren_depth == 0 { break; }
                    }
                }
            } else if after_last.is_empty() {
                // Peek at the next line for `->`
                if i < lines.len() {
                    let next_trimmed = lines[i].trim();
                    if next_trimmed.starts_with("->") {
                        // Collect the arrow and output port lines
                        collected.push('\n');
                        collected.push_str(next_trimmed);
                        count_structural_parens(next_trimmed, &mut paren_depth, &mut found_open_brace);
                        i += 1;

                        // If output parens are open, keep collecting
                        while i < lines.len() && paren_depth > 0 {
                            let line = lines[i].trim();
                            collected.push('\n');
                            collected.push_str(line);
                            count_structural_parens(line, &mut paren_depth, &mut found_open_brace);
                            i += 1;
                        }
                    }
                }
            }
        }
    }

    // Now check if there's a `{` on the next line (if we haven't found one yet)
    if !found_open_brace && i < lines.len() {
        let next_trimmed = lines[i].trim();
        if next_trimmed == "{" || next_trimmed == "{}" {
            collected.push('\n');
            collected.push_str(next_trimmed);
            return (collected, i);
        }
    }

    // If the collected header has an opening `{` WITH content after it
    // but the brace isn't closed yet (e.g. `n = Type { code: ``` ` with
    // the value on later lines), keep consuming lines until the matching
    // `}` at brace depth 0, taking triple-backtick into account so that
    // `}` inside a code block doesn't close the outer brace. Bare `{`
    // openers (group body, standard multi-line config) are left for the
    // downstream `parse_config_block` / `parse_group_body` to handle.
    if collect_unclosed_body
        && found_open_brace
        && first_brace_has_content_after(&collected)
        && !is_brace_balanced_respecting_quotes_and_backticks(&collected)
    {
        while i < lines.len() {
            let line = lines[i];
            collected.push('\n');
            collected.push_str(line);
            i += 1;
            if is_brace_balanced_respecting_quotes_and_backticks(&collected) {
                return (collected, i - 1);
            }
        }
    }

    // i - 1 because i has been incremented past the last consumed line
    (collected, if i > start { i - 1 } else { start })
}

/// True if the first `{` in `s` is followed by real config content on the
/// same line (not just whitespace or a trailing `#` comment). Used to
/// distinguish `{ code: ``` ` (one-liner style with content) from a bare
/// `{` opener for a standard multi-line config block. A trailing
/// `{ # comment` is treated as a bare opener.
fn first_brace_has_content_after(s: &str) -> bool {
    let idx = match s.find('{') { Some(i) => i, None => return false };
    let rest = &s[idx + 1..];
    for c in rest.chars() {
        if c == '\n' { return false; }
        if c == '#' { return false; } // Comment continues to end of line.
        if !c.is_whitespace() { return true; }
    }
    false
}

/// True if `s` has matched `{`/`}` at depth 0, respecting quoted strings
/// and triple-backtick code blocks. Used to know when a multi-line `{ ... }`
/// one-liner style header is complete.
fn is_brace_balanced_respecting_quotes_and_backticks(s: &str) -> bool {
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut in_backtick = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Triple-backtick toggle.
        if i + 2 < bytes.len() && bytes[i] == b'`' && bytes[i + 1] == b'`' && bytes[i + 2] == b'`' {
            in_backtick = !in_backtick;
            i += 3;
            continue;
        }
        if in_backtick {
            i += 1;
            continue;
        }
        let c = bytes[i];
        if c == b'\\' && i + 1 < bytes.len() {
            // Escape: skip the next char.
            i += 2;
            continue;
        }
        if c == b'"' {
            in_string = !in_string;
            i += 1;
            continue;
        }
        if !in_string {
            if c == b'{' { depth += 1; }
            if c == b'}' {
                depth -= 1;
                if depth == 0 {
                    // Found matching close. Balanced IFF there's no further
                    // `{` after this point (to catch weirdness), but we
                    // only need to detect the end of the first balanced
                    // block, so return true here.
                    // Check rest of string for any un-escaped `{` in non-
                    // string, non-backtick context: if we find any, it's
                    // another unbalanced block. But for our purposes, we
                    // only care that the FIRST `{` has been closed.
                    return true;
                }
            }
        }
        i += 1;
    }
    depth == 0
}

// ─── Port Signature Parsing ─────────────────────────────────────────────────

/// Parse `(inputs) -> (outputs)` or just `(inputs)` from a string starting with `(`.
/// Returns (in_ports, out_ports, one_of_required, remaining_string).
fn parse_port_signature(
    s: &str,
    line_num: usize,
    errors: &mut Vec<CompileError>,
) -> (Vec<ParsedPort>, Vec<ParsedPort>, Vec<Vec<String>>, String) {
    let mut in_ports = Vec::new();
    let mut out_ports = Vec::new();
    let mut one_of_required: Vec<Vec<String>> = Vec::new();

    // Find matching closing paren for inputs
    let (input_content, after_input) = match find_matching_paren(s) {
        Some((content, rest)) => (content, rest),
        None => {
            errors.push(CompileError { line: line_num, message: "Unclosed input port list '('".to_string() });
            return (in_ports, out_ports, one_of_required, String::new());
        }
    };

    // Parse input ports
    parse_port_list(&input_content, &mut in_ports, &mut one_of_required, "in", line_num, errors);

    let after_input = after_input.trim();

    // Check for -> (outputs)
    if let Some(rest) = after_input.strip_prefix("->") {
        let rest = rest.trim();
        if rest.starts_with('(') {
            let (output_content, after_output) = match find_matching_paren(rest) {
                Some((content, rest)) => (content, rest),
                None => {
                    errors.push(CompileError { line: line_num, message: "Unclosed output port list '('".to_string() });
                    return (in_ports, out_ports, one_of_required, String::new());
                }
            };
            let mut out_oor = Vec::new();
            parse_port_list(&output_content, &mut out_ports, &mut out_oor, "out", line_num, errors);
            // out_oor is unusual but supported
            one_of_required.extend(out_oor);
            return (in_ports, out_ports, one_of_required, after_output);
        } else {
            errors.push(CompileError { line: line_num, message: "Expected '(' after '->'".to_string() });
            return (in_ports, out_ports, one_of_required, rest.to_string());
        }
    }

    (in_ports, out_ports, one_of_required, after_input.to_string())
}

/// Find matching ')' for a string starting with '('. Returns (inner_content, rest_after_paren).
fn find_matching_paren(s: &str) -> Option<(String, String)> {
    if !s.starts_with('(') { return None; }
    let mut depth = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip @require_one_of(...) directive, its parens are not structural.
        // The directive must close on the same line.
        if i + 16 <= bytes.len() && &s[i..i+16] == "@require_one_of(" {
            let mut dir_depth: i32 = 1;
            let mut j = i + 16;
            while j < bytes.len() && bytes[j] != b'\n' && dir_depth > 0 {
                if bytes[j] == b'(' { dir_depth += 1; }
                if bytes[j] == b')' { dir_depth -= 1; }
                if dir_depth > 0 { j += 1; }
            }
            // Skip past directive if balanced within line, otherwise skip to newline
            if dir_depth == 0 {
                i = j + 1;
            } else {
                while i < bytes.len() && bytes[i] != b'\n' { i += 1; }
            }
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((s[1..i].to_string(), s[i + 1..].to_string()));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse a comma/newline-separated port list with optional @require_one_of directives.
fn parse_port_list(
    content: &str,
    ports: &mut Vec<ParsedPort>,
    one_of_required: &mut Vec<Vec<String>>,
    direction: &str,
    line_num: usize,
    errors: &mut Vec<CompileError>,
) {
    // Split on top-level commas and newlines
    for item in split_port_items(content) {
        let item = item.trim();
        if item.is_empty() || item.starts_with('#') { continue; }

        // @require_one_of(a, b), only valid in input port lists
        if let Some(rest) = item.strip_prefix("@require_one_of(") {
            if direction != "in" {
                errors.push(CompileError { line: line_num, message: "@require_one_of is only valid in input port lists".to_string() });
                continue;
            }
            if let Some(body) = rest.strip_suffix(')') {
                let group: Vec<String> = body.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !group.is_empty() {
                    one_of_required.push(group);
                }
            } else {
                errors.push(CompileError { line: line_num, message: "@require_one_of missing closing parenthesis".to_string() });
            }
            continue;
        }

        match try_parse_port_decl(item) {
            Ok(port) => {
                if ports.iter().any(|p| p.name == port.name) {
                    errors.push(CompileError { line: line_num, message: format!("Duplicate {} port \"{}\"", direction, port.name) });
                } else {
                    ports.push(port);
                }
            }
            Err(msg) => {
                errors.push(CompileError { line: line_num, message: msg });
            }
        }
    }
}

/// Split port content on commas and newlines, respecting bracket depth.
fn split_port_items(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' | '(' => depth += 1,
            ']' | ')' => if depth > 0 { depth -= 1; },
            ',' | '\n' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Parse a single port declaration.
/// Port declaration syntax: `name: Type` (required by default) or
/// `name: Type?` (optional). No prefix characters. Lane modes are inferred
/// from the type during enrichment, not declared on the port.
fn try_parse_port_decl(trimmed: &str) -> Result<ParsedPort, String> {
    let s = trimmed.trim();
    let rest = s;
    // v2: no explicit expand/gather prefixes. Lane modes are inferred from types by enrichment.

    let (name, port_type, optional) = if let Some(colon_pos) = rest.find(':') {
        let name = rest[..colon_pos].trim();
        let mut type_str = rest[colon_pos + 1..].trim();

        // Check for `?` suffix (optional marker)
        let optional = type_str.ends_with('?');
        if optional {
            type_str = type_str[..type_str.len() - 1].trim();
        }

        match WeftType::parse(type_str) {
            Some(pt) => (name, pt, optional),
            None => return Err(format!("Invalid port type '{}' on port '{}'", type_str, name)),
        }
    } else {
        // No type annotation
        let name = rest.trim();
        let optional = name.ends_with('?');
        let name = if optional { name[..name.len() - 1].trim() } else { name };
        if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid port name: '{}'", rest.trim()));
        }
        (name, WeftType::default(), optional)
    };

    // Validate port name
    let first = name.chars().next().ok_or_else(|| "Empty port name".to_string())?;
    if !(first.is_alphabetic() || first == '_') {
        return Err(format!("Port name must start with a letter or underscore: '{}'", name));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Port name contains invalid characters: '{}'", name));
    }

    Ok(ParsedPort {
        name: name.to_string(),
        port_type: port_type,
        required: !optional, // v2: required by default, ? makes optional
        lane_mode: LaneMode::Single,
    })
}

// ─── Group Body Parsing ─────────────────────────────────────────────────────

/// Parse a group body: everything between `{` and `}`.
/// Contains child nodes, connections, nested groups, and comments.
/// The first contiguous block of `#` comments is the group description.
/// Pre-scan a group body (starting at the line AFTER the group declaration
/// header) and collect the local-scope identifiers: every `id = Type ...`
/// or `id = Group ...` top-level declaration inside the body, until the
/// matching closing `}`. Brace-depth aware so nested blocks don't
/// contribute false positives.
fn collect_local_child_ids(lines: &[&str], start: usize, _group_id: &str) -> std::collections::HashSet<String> {
    let mut ids = std::collections::HashSet::new();
    let mut depth: i32 = 0;
    // We're already past the group's opening `{`, so depth starts at 0
    // relative to the body's top level. The first line is start + 1
    // (start is the declaration line).
    let mut i = start + 1;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();

        // Closing `}` at depth 0 ends the group body.
        // (Track depth FIRST so we see the close before advancing.)
        if depth == 0 && (trimmed == "}" || trimmed.starts_with("} ")) {
            break;
        }

        // Top-level declaration pattern at depth 0: `id = Type...`.
        if depth == 0 {
            if let Some(eq_pos) = trimmed.find('=') {
                let left = trimmed[..eq_pos].trim();
                let right = trimmed[eq_pos + 1..].trim();
                // Must be a bare identifier on the left and uppercase
                // type name on the right. Dotted left means it's a
                // connection, not a declaration.
                if !left.is_empty()
                    && !left.contains('.')
                    && left.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    && left.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
                    && right.chars().next().map(|c| c.is_ascii_uppercase()).unwrap_or(false)
                {
                    ids.insert(left.to_string());
                }
            }
        }

        // Track brace depth (respecting quotes, but not tracking
        // triple-backtick state here since we only care about top-level
        // identifiers and triple-backtick bodies don't introduce new
        // declarations at the outer level).
        let mut in_string: Option<char> = None;
        let mut escape = false;
        for c in trimmed.chars() {
            if escape { escape = false; continue; }
            if let Some(q) = in_string {
                if c == '\\' { escape = true; }
                else if c == q { in_string = None; }
                continue;
            }
            if c == '"' || c == '\'' { in_string = Some(c); continue; }
            if c == '{' { depth += 1; }
            if c == '}' { depth -= 1; }
        }

        i += 1;
    }
    ids
}

/// Check if a source/target identifier refers to something local to the
/// current group scope. A reference is local if:
///   - it IS one of the declared child ids
///   - OR it's an anon inline child id `{localId}__{field}` where `localId`
///     is a declared child
///   - OR it starts with `self__` (anon generated from `self.field = Type{}.port`
///     where the parent is the group itself)
/// Anything else is an external reference (root or outer scope) and stays
/// unprefixed.
fn is_local_ref(id: &str, local_children: &std::collections::HashSet<String>) -> bool {
    if local_children.contains(id) { return true; }
    if let Some(idx) = id.find("__") {
        let head = &id[..idx];
        if head == "self" { return true; }
        if local_children.contains(head) { return true; }
    }
    false
}

fn parse_group_body(
    lines: &[&str],
    start: usize,
    group_id: &str,
    in_ports: Vec<ParsedPort>,
    out_ports: Vec<ParsedPort>,
    one_of_required: Vec<Vec<String>>,
    errors: &mut Vec<CompileError>,
) -> (ParsedGroup, usize) {
    let mut group = ParsedGroup {
        id: group_id.to_string(),
        in_ports,
        out_ports,
        one_of_required,
        nodes: Vec::new(),
        connections: Vec::new(),
        child_groups: Vec::new(),
        // The caller overrides these once the header/body extents are
        // fully known.
        span: None,
        header_span: None,
    };

    // Pre-scan the group body to collect the set of local child ids. Used
    // when rescoping port-wiring connections emitted from inline bodies:
    // if the source or target of such a connection refers to a local child,
    // we prefix with `{group_id}.`; otherwise the reference is to an outer
    // scope (root, a parent group) and stays unprefixed. Without this
    // pre-scan we'd blindly prefix everything and turn `src.value` (root
    // node) into `grp.src.value` (nonexistent).
    let local_child_ids = collect_local_child_ids(lines, start, group_id);

    let mut i = start + 1; // skip the declaration line

    while i < lines.len() {
        let trimmed = lines[i].trim();
        let line_num = i + 1;

        // Empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            i += 1;
            continue;
        }

        // Closing brace ends the group (optionally followed by -> (post-config outputs))
        if trimmed == "}" || trimmed.starts_with("} ->") {
            if trimmed.starts_with("} ->") {
                // Post-config output ports on the group: } -> (outputs)
                let arrow_rest = trimmed[4..].trim();
                let mut out_sig = arrow_rest.to_string();
                let mut out_end = i;
                let mut paren_depth: i32 = 0;
                for c in out_sig.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                while paren_depth > 0 && out_end + 1 < lines.len() {
                    out_end += 1;
                    let ol = lines[out_end].trim();
                    out_sig.push(' ');
                    out_sig.push_str(ol);
                    for c in ol.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                }
                if out_sig.starts_with('(') {
                    if let Some((content, _rest)) = find_matching_paren(&out_sig) {
                        let existing_names: std::collections::HashSet<String> = group.out_ports.iter().map(|p| p.name.clone()).collect();
                        let mut post_ports = Vec::new();
                        let mut post_oor = Vec::new();
                        parse_port_list(&content, &mut post_ports, &mut post_oor, "out", line_num, errors);
                        for p in post_ports {
                            if existing_names.contains(&p.name) {
                                errors.push(CompileError { line: line_num, message: format!("Duplicate output port '{}' on group '{}'", p.name, group_id) });
                            } else {
                                group.out_ports.push(p);
                            }
                        }
                    }
                }
                return (group, out_end + 1);
            }
            // Check if next non-blank line starts with -> (post-config outputs on separate line)
            let mut peek = i + 1;
            while peek < lines.len() && lines[peek].trim().is_empty() { peek += 1; }
            if peek < lines.len() && lines[peek].trim().starts_with("->") {
                let arrow_rest = lines[peek].trim().strip_prefix("->").unwrap().trim();
                let mut out_sig = arrow_rest.to_string();
                let mut out_end = peek;
                let mut paren_depth: i32 = 0;
                for c in out_sig.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                while paren_depth > 0 && out_end + 1 < lines.len() {
                    out_end += 1;
                    let ol = lines[out_end].trim();
                    out_sig.push(' ');
                    out_sig.push_str(ol);
                    for c in ol.chars() { if c == '(' { paren_depth += 1; } if c == ')' { paren_depth -= 1; } }
                }
                if out_sig.starts_with('(') {
                    if let Some((content, _rest)) = find_matching_paren(&out_sig) {
                        let existing_names: std::collections::HashSet<String> = group.out_ports.iter().map(|p| p.name.clone()).collect();
                        let mut post_ports = Vec::new();
                        let mut post_oor = Vec::new();
                        parse_port_list(&content, &mut post_ports, &mut post_oor, "out", line_num, errors);
                        for p in post_ports {
                            if existing_names.contains(&p.name) {
                                errors.push(CompileError { line: line_num, message: format!("Duplicate output port '{}' on group '{}'", p.name, group_id) });
                            } else {
                                group.out_ports.push(p);
                            }
                        }
                    }
                }
                return (group, out_end + 1);
            }
            return (group, i + 1);
        }

        // Child declaration: node or nested group
        let mut child_inline_scope = InlineScope::default();
        if let Some((result, next_i)) = try_parse_declaration(&lines, i, errors, &mut child_inline_scope) {
            match result {
                Declaration::Node(mut node) => {
                    let local_id = node.id.clone();
                    node.id = format!("{}.{}", group_id, local_id);
                    node.parent_id = Some(group_id.to_string());
                    if group.nodes.iter().any(|n| n.id == node.id) {
                        errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}' in group '{}'", local_id, group_id) });
                    }
                    group.nodes.push(node);
                }
                Declaration::Group(mut child_group) => {
                    let local_id = child_group.id.clone();
                    let scoped_id = format!("{}.{}", group_id, local_id);
                    // Rescope all internal IDs
                    rescope_group(&mut child_group, &local_id, &scoped_id);
                    child_group.id = scoped_id.clone();
                    if group.child_groups.iter().any(|g| g.id == child_group.id) {
                        errors.push(CompileError { line: line_num, message: format!("Duplicate group name '{}' in group '{}'", local_id, group_id) });
                    }
                    group.child_groups.push(child_group);
                }
            }
            // Merge inline children generated by this declaration. The anon
            // IDs are already `{parent_local_id}__{field}` form; we prefix
            // them with the group scope (e.g. `per_lead.email_writer__prompt`)
            // and set parentId so they participate in the group's scope.
            for mut child in child_inline_scope.nodes {
                let local_id = child.id.clone();
                child.id = format!("{}.{}", group_id, local_id);
                child.parent_id = Some(group_id.to_string());
                if group.nodes.iter().any(|n| n.id == child.id) {
                    errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}' in group '{}' (generated from inline expression)", local_id, group_id) });
                }
                group.nodes.push(child);
            }
            // Rescope the inline connections too. Three distinct cases per
            // endpoint:
            //   - "self"               → group In / Out passthrough
            //   - matches a local child id (or is an anon id generated from
            //     inline expressions, which always starts with one) → prefix
            //     with `{group_id}.`
            //   - anything else        → external reference (root / outer
            //     scope), leave unprefixed
            for mut conn in child_inline_scope.connections {
                if conn.source_id == "self" {
                    conn.source_id = format!("{}__in", group_id);
                } else if is_local_ref(&conn.source_id, &local_child_ids) {
                    conn.source_id = format!("{}.{}", group_id, conn.source_id);
                }
                if conn.target_id == "self" {
                    conn.target_id = format!("{}__out", group_id);
                } else if is_local_ref(&conn.target_id, &local_child_ids) {
                    conn.target_id = format!("{}.{}", group_id, conn.target_id);
                }
                group.connections.push(conn);
            }
            i = next_i;
            continue;
        }

        // Connection: target.port = source.port (with self support, inline RHS, and literal config fill)
        let mut conn_scope = InlineScope::default();
        match try_parse_group_connection_with_inline(&lines, i, line_num, group_id, errors, &mut conn_scope) {
            ParseConnectionResult::Edge(conn, next_i) => {
                group.connections.push(conn);
                for mut child in conn_scope.nodes {
                    let local_id = child.id.clone();
                    child.id = format!("{}.{}", group_id, local_id);
                    child.parent_id = Some(group_id.to_string());
                    if group.nodes.iter().any(|n| n.id == child.id) {
                        errors.push(CompileError { line: line_num, message: format!("Duplicate node ID '{}' in group '{}' (generated from inline expression)", local_id, group_id) });
                    }
                    group.nodes.push(child);
                }
                for mut conn in conn_scope.connections {
                    if conn.source_id == "self" {
                        conn.source_id = format!("{}__in", group_id);
                    } else if is_local_ref(&conn.source_id, &local_child_ids) {
                        conn.source_id = format!("{}.{}", group_id, conn.source_id);
                    }
                    if conn.target_id == "self" {
                        conn.target_id = format!("{}__out", group_id);
                    } else if is_local_ref(&conn.target_id, &local_child_ids) {
                        conn.target_id = format!("{}.{}", group_id, conn.target_id);
                    }
                    group.connections.push(conn);
                }
                for fill in conn_scope.config_fills {
                    apply_config_fill(&mut group.nodes, fill);
                }
                i = next_i;
                continue;
            }
            ParseConnectionResult::ConfigFill(next_i) => {
                for fill in conn_scope.config_fills {
                    apply_config_fill(&mut group.nodes, fill);
                }
                i = next_i;
                continue;
            }
            ParseConnectionResult::NotAConnection => {}
        }

        errors.push(CompileError { line: line_num, message: format!("Unexpected in group '{}': {}", group_id, trimmed) });
        i += 1;
    }

    errors.push(CompileError { line: start + 1, message: format!("Unclosed group '{}'", group_id) });
    (group, i)
}

/// Rescope all internal IDs in a group from local_id prefix to scoped_id prefix.
fn rescope_group(group: &mut ParsedGroup, _local_id: &str, scoped_id: &str) {
    // Rescope internal nodes
    for node in &mut group.nodes {
        // Node IDs are already local_id.node_name from the inner parse, need to become scoped_id.node_name
        // Actually, when parse_group_body parsed the child group, it didn't scope yet.
        // The child group's nodes have IDs relative to the child group's local ID.
        // We need to prefix them with the parent scope.
        let old_prefix = format!("{}.", group.id);
        if node.id.starts_with(&old_prefix) {
            node.id = format!("{}.{}", scoped_id, &node.id[old_prefix.len()..]);
        } else {
            node.id = format!("{}.{}", scoped_id, node.id);
        }
        node.parent_id = Some(scoped_id.to_string());
    }
    // Rescope connections
    for conn in &mut group.connections {
        rescope_id(&mut conn.source_id, &group.id, scoped_id);
        rescope_id(&mut conn.target_id, &group.id, scoped_id);
    }
    // Rescope child groups recursively
    for child in &mut group.child_groups {
        let old_child_id = child.id.clone();
        let new_child_id = if old_child_id.starts_with(&format!("{}.", group.id)) {
            format!("{}.{}", scoped_id, &old_child_id[group.id.len() + 1..])
        } else {
            format!("{}.{}", scoped_id, old_child_id)
        };
        rescope_group(child, &old_child_id, &new_child_id);
        child.id = new_child_id;
    }
    // Update the group's own ID (caller does this, but we need connections to reference it)
}

fn rescope_id(id: &mut String, old_prefix: &str, new_prefix: &str) {
    let old_in = format!("{}__in", old_prefix);
    let old_out = format!("{}__out", old_prefix);
    if *id == old_in {
        *id = format!("{}__in", new_prefix);
    } else if *id == old_out {
        *id = format!("{}__out", new_prefix);
    } else if id.starts_with(&format!("{}.", old_prefix)) {
        *id = format!("{}.{}", new_prefix, &id[old_prefix.len() + 1..]);
    }
}

// ─── Config Block Parsing ───────────────────────────────────────────────────

/// Parse a multi-line config block (inside `{ ... }`).
/// Returns (config, label, extra_in_ports, extra_out_ports, one_of_required, next_line_index).
///
/// Shared by regular node declarations and inline expression bodies. All
/// three value forms work in both contexts:
///   - literals (quoted string, number, bool, JSON, triple-backtick)
///   - port wirings: `key: source.port` emits an edge targeting parent.key
///   - inline expressions: `key: Type { ... }.port` or bare `Type.port`
fn parse_config_block(
    lines: &[&str],
    start: usize,
    base_line: usize,
    errors: &mut Vec<CompileError>,
    parent_id: &str,
    inline_scope: &mut InlineScope,
) -> (serde_json::Map<String, serde_json::Value>, Option<String>, Vec<ParsedPort>, Vec<ParsedPort>, Vec<Vec<String>>, usize) {
    let (cfg, lbl, ins, outs, oor, end_i, _close_line) = parse_config_block_inner(lines, start, base_line, errors, parent_id, inline_scope);
    (cfg, lbl, ins, outs, oor, end_i)
}

/// Variant of parse_config_block that also returns the 0-based index of
/// the line containing the closing `}` for the block. Callers that need
/// to disambiguate "close brace standalone on its own line" from "close
/// brace followed by `.port` or `->`" (the inline expression parser) use
/// this to avoid mis-attributing an earlier bare `}` from multi-line JSON
/// inside the body.
fn parse_config_block_with_close(
    lines: &[&str],
    start: usize,
    base_line: usize,
    errors: &mut Vec<CompileError>,
    parent_id: &str,
    inline_scope: &mut InlineScope,
) -> (serde_json::Map<String, serde_json::Value>, Option<String>, Vec<ParsedPort>, Vec<ParsedPort>, Vec<Vec<String>>, usize, usize) {
    parse_config_block_inner(lines, start, base_line, errors, parent_id, inline_scope)
}

fn parse_config_block_inner(
    lines: &[&str],
    start: usize,
    base_line: usize,
    errors: &mut Vec<CompileError>,
    parent_id: &str,
    inline_scope: &mut InlineScope,
) -> (serde_json::Map<String, serde_json::Value>, Option<String>, Vec<ParsedPort>, Vec<ParsedPort>, Vec<Vec<String>>, usize, usize) {
    let mut config = serde_json::Map::new();
    let mut label = None;
    let in_ports = Vec::new();
    let out_ports = Vec::new();
    let mut one_of_required = Vec::new();

    let mut i = start + 1;
    while i < lines.len() {
        let inner = lines[i].trim();
        let line_num = i + 1;

        if inner == "}" || inner.starts_with("} ") || inner.starts_with("}->") || inner.starts_with("}.") {
            // The closing `}` may be followed by `-> (outputs)` or `.port`.
            // Return the close line index so the caller doesn't need to
            // reverse-engineer it from the body contents (which can include
            // multi-line JSON with its own standalone `}` lines).
            let close_line = i;
            if inner == "}" {
                return (config, label, in_ports, out_ports, one_of_required, i + 1, close_line);
            } else {
                return (config, label, in_ports, out_ports, one_of_required, i, close_line);
            }
        }
        if inner.is_empty() || inner.starts_with('#') {
            i += 1;
            continue;
        }

        // @require_one_of inside config block
        if let Some(rest) = inner.strip_prefix("@require_one_of(") {
            if let Some(body) = rest.strip_suffix(')') {
                let group: Vec<String> = body.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !group.is_empty() {
                    one_of_required.push(group);
                }
            } else {
                errors.push(CompileError { line: line_num, message: "@require_one_of missing closing parenthesis".to_string() });
            }
            i += 1;
            continue;
        }


        // Triple-backtick multiline: `key: ``` ... ```` (the space after `:`
        // is optional, and whitespace around the colon is also allowed).
        // We detect by searching for the colon and then the first `` ``` ``
        // after any whitespace, so all of the following forms parse:
        //   key: ```content```
        //   key:```content```
        //   key  :  ```content```
        if let Some(bt_match) = inner.find("```").and_then(|bt| {
            // Find the colon before the backticks.
            let before_bt = &inner[..bt];
            let colon = before_bt.rfind(':')?;
            let key = inner[..colon].trim();
            // Key must be a bare identifier.
            if key.is_empty() { return None; }
            let first = key.chars().next()?;
            if !first.is_ascii_alphabetic() && first != '_' { return None; }
            if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') { return None; }
            // Between colon and `` ``` `` must be only whitespace.
            let between = &inner[colon + 1..bt];
            if !between.chars().all(|c| c.is_whitespace()) { return None; }
            Some((key, bt))
        }) {
            let (key, bt_pos) = bt_match;
            let key = key.to_string();
            let after = &inner[bt_pos + 3..]; // after the opening `` ``` ``

            // Check for inline form: key: ```content```
            if after.ends_with("```") && after.len() > 3 {
                let inline_val = &after[..after.len() - 3];
                store_config_value(&key, inline_val, &mut config, line_num, errors);
                i += 1;
                continue;
            }

            // Multi-line: collect until closing ```.
            let (value, next) = collect_heredoc_value(lines, i + 1, after);
            i = next;

            store_config_value(&key, &value, &mut config, line_num, errors);
            continue;
        }

        // Multi-line JSON: key: [ ... ] or key: { ... } spanning multiple lines
        if let Some(colon_pos) = inner.find(':') {
            let key = inner[..colon_pos].trim();
            let val_start = inner[colon_pos + 1..].trim();
            if let Some((collected, balanced, next)) = collect_multiline_json(lines, i + 1, i, val_start) {
                if !balanced {
                    errors.push(CompileError { line: line_num, message: format!("Broken JSON for '{}': brackets not balanced", key) });
                }
                i = next;
                store_config_value(key, &collected, &mut config, line_num, errors);
                continue;
            }
        }

        // Inline expression: `key: Type ...` where the value is a node
        // literal. Detect by checking if the value part (after `:`) starts
        // with an uppercase identifier followed by `(`, `{`, `->`, or `.`.
        // Must run BEFORE port wiring because the bare form `Type.port`
        // would otherwise look like a dotted ref. Validation that `key` is
        // a configurable input port happens at enrichment time.
        if let Some(colon_pos) = inner.find(':') {
            let key = inner[..colon_pos].trim();
            let val_start = inner[colon_pos + 1..].trim();
            if looks_like_inline_start(val_start) && is_valid_config_key(key) {
                let raw_line = lines[i];
                let raw_colon = raw_line.find(':').unwrap_or(colon_pos);
                let start_col = raw_colon + 1;
                match try_parse_inline_expression(
                    lines, i, start_col, parent_id, key, inline_scope, errors,
                ) {
                    Some(next_i) => {
                        i = next_i;
                        continue;
                    }
                    None => {
                        i += 1;
                        continue;
                    }
                }
            }
        }

        // Port wiring: `key: source.port` where source.port is an unquoted
        // dotted identifier. Emits an edge into inline_scope targeting
        // parent_id.key and skips storing the value in config. Works in
        // BOTH inline-expression bodies and regular node declaration
        // bodies, so `greeting = Template { template: src.value }` wires
        // src.value into greeting.template without a separate connection
        // line. Enrichment validates that `key` is a real input port on
        // the parent node; if not, the edge targets a nonexistent port
        // and enrichment errors.
        if let Some(colon_pos) = inner.find(':') {
            let key = inner[..colon_pos].trim();
            let val_start = inner[colon_pos + 1..].trim();
            if is_valid_config_key(key) && looks_like_dotted_ref(val_start) {
                if let Some((src_id, src_port)) = parse_dotted(val_start) {
                    inline_scope.connections.push(ParsedConnection {
                        source_id: src_id,
                        source_port: src_port,
                        target_id: parent_id.to_string(),
                        target_port: key.to_string(),
                        span: Some(Span::single_line(line_num, 0, lines[i].len())),
                    });
                    i += 1;
                    continue;
                }
            }
        }

        // label: "value"
        if let Some(l) = try_extract_label(inner) {
            label = Some(l);
            i += 1;
            continue;
        }

        // key: value
        parse_kv(inner, &mut config, line_num, errors);
        i += 1;
    }

    errors.push(CompileError { line: base_line, message: "Unclosed config block".to_string() });
    // No matching `}` found; use `i` as a sentinel close_line (out of range).
    (config, label, in_ports, out_ports, one_of_required, i, i)
}

/// Collect a multi-line triple-backtick heredoc value starting after the
/// opening `` ``` ``. `initial_after_bt` is whatever text appeared on the
/// same line after the opening backticks. Lines are read from
/// `lines[start_line..]`. Returns `(unescaped_value, next_line_index)`.
fn collect_heredoc_value(lines: &[&str], start_line: usize, initial_after_bt: &str) -> (String, usize) {
    let mut value = initial_after_bt.to_string();
    let mut i = start_line;
    while i < lines.len() {
        let ml_trimmed = lines[i].trim();
        let closes_bare = ml_trimmed == "```";
        let closes_suffix = !closes_bare
            && ml_trimmed.ends_with("```")
            && !ml_trimmed[..ml_trimmed.len() - 3].ends_with('\\');
        if closes_bare || closes_suffix {
            let before_close = if closes_bare {
                ""
            } else {
                &ml_trimmed[..ml_trimmed.len() - 3]
            };
            if !before_close.is_empty() {
                if !value.is_empty() { value.push('\n'); }
                value.push_str(before_close);
            }
            i += 1;
            break;
        }
        if !value.is_empty() { value.push('\n'); }
        value.push_str(lines[i]);
        i += 1;
    }
    let value = dedent(&value);
    let value = value.replace("\\```", "```").replace("\\`", "`");
    (value, i)
}

/// Collect a multi-line JSON value (object or array) with brace-depth
/// tracking. `initial_value` is the first chunk (e.g. `[` or `{"key":`).
/// Lines are read from `lines[start_line..]`. `origin_line` is the line
/// index where the JSON key lives (used for the 500-line safety limit).
/// Returns `Some((collected_raw_string, is_balanced, next_line_index))`
/// if `initial_value` starts with `[` or `{` and is not already balanced,
/// or `None` otherwise.
fn collect_multiline_json(lines: &[&str], start_line: usize, origin_line: usize, initial_value: &str) -> Option<(String, bool, usize)> {
    if !(initial_value.starts_with('[') || initial_value.starts_with('{')) || is_json_balanced(initial_value) {
        return None;
    }
    let mut depth: i32 = 0;
    let mut collected = initial_value.to_string();
    for c in initial_value.bytes() {
        if c == b'[' || c == b'{' { depth += 1; }
        if c == b']' || c == b'}' { depth -= 1; }
    }
    let mut i = start_line;
    let mut hit_boundary = false;
    while i < lines.len() && depth > 0 {
        let ml = lines[i].trim();
        if i - origin_line > 500 { hit_boundary = true; break; }
        if !looks_like_json(ml) { hit_boundary = true; break; }
        collected.push('\n');
        collected.push_str(ml);
        for c in ml.bytes() {
            if c == b'[' || c == b'{' { depth += 1; }
            if c == b']' || c == b'}' { depth -= 1; }
        }
        if depth <= 0 { i += 1; break; }
        i += 1;
    }
    let balanced = depth <= 0 && !hit_boundary;
    Some((collected, balanced, i))
}

/// Check if a line looks like JSON content (not Weft syntax).
fn looks_like_json(line: &str) -> bool {
    use std::sync::OnceLock;
    static RE_CONN: OnceLock<regex::Regex> = OnceLock::new();
    static RE_DECL: OnceLock<regex::Regex> = OnceLock::new();

    if line.is_empty() { return true; } // blank lines OK inside JSON
    // Connections: x.y = z.w
    if line.contains('.') && line.contains('=') && !line.starts_with('"') {
        let re = RE_CONN.get_or_init(|| regex::Regex::new(r"^[a-zA-Z_]\w*\.\w+\s*=").unwrap());
        if re.is_match(line) { return false; }
    }
    // Declarations: id = Type
    if line.contains('=') && !line.starts_with('"') {
        let re = RE_DECL.get_or_init(|| regex::Regex::new(r"^[a-zA-Z_]\w*\s*=\s*[A-Z]").unwrap());
        if re.is_match(line) { return false; }
    }
    // Comments and directives
    if line.starts_with('#') || line.starts_with('@') { return false; }
    true
}

/// Check if a JSON-like string has balanced brackets/braces.
fn is_json_balanced(s: &str) -> bool {
    let mut depth = 0i32;
    for c in s.bytes() {
        match c {
            b'[' | b'{' => depth += 1,
            b']' | b'}' => depth -= 1,
            _ => {}
        }
    }
    depth == 0
}

fn store_config_value(
    key: &str,
    value: &str,
    config: &mut serde_json::Map<String, serde_json::Value>,
    line_num: usize,
    errors: &mut Vec<CompileError>,
) {
    // Reject removed config keys
    if key == "mock" || key == "mocked" {
        errors.push(CompileError { line: line_num, message: format!("'{}' is not a valid config key. Use test configs for mocking.", key) });
        return;
    }

    // Try to parse as JSON if value looks like JSON (starts with [ or {)
    if value.trim_start().starts_with('[') || value.trim_start().starts_with('{') {
        match serde_json::from_str::<serde_json::Value>(value) {
            Ok(json_val) => { config.insert(key.to_string(), json_val); return; }
            Err(_) => {
                // Fall through to store as string
            }
        }
    }
    // Default: store as string
    config.insert(key.to_string(), serde_json::Value::String(value.to_string()));
}

fn dedent(s: &str) -> String {
    let raw = s.trim_end();
    let min_indent = raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    if min_indent > 0 {
        raw.lines()
            .map(|l| if l.len() >= min_indent { &l[min_indent..] } else { l })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        raw.to_string()
    }
}

fn try_extract_label(s: &str) -> Option<String> {
    let colon = s.find(':')?;
    let key = s[..colon].trim();
    if key != "label" { return None; }
    let val = s[colon + 1..].trim();
    if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
        Some(unescape(&val[1..val.len() - 1]))
    } else {
        Some(val.to_string())
    }
}

// ─── Connection Parsing ─────────────────────────────────────────────────────

/// Parse connections inside a group. Uses `self` instead of `in`/`out`.
/// `child.input = self.port` (child receives from group input)
/// `self.output = child.port` (group output receives from child)
/// `child.port = other_child.port` (internal wiring)
fn try_parse_group_connection(
    trimmed: &str,
    line_num: usize,
    group_id: &str,
    errors: &mut Vec<CompileError>,
) -> Option<ParsedConnection> {
    let eq_pos = trimmed.find('=')?;
    let left = trimmed[..eq_pos].trim();
    let right = trimmed[eq_pos + 1..].trim();

    // Both sides must be dotted
    if !left.contains('.') || !right.contains('.') {
        return None;
    }

    // Parse left side (target: input being set)
    let (target_id, target_port) = if let Some(rest) = left.strip_prefix("self.") {
        // self.port on left = group output being set. The remainder must
        // be a bare identifier (port name), not further dotted.
        if rest.is_empty() || !rest.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
            || !rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            errors.push(CompileError { line: line_num, message: format!("Invalid connection target: '{}'", left) });
            return None;
        }
        (format!("{}__out", group_id), rest.to_string())
    } else if let Some((node, port)) = parse_dotted(left) {
        (format!("{}.{}", group_id, node), port)
    } else {
        errors.push(CompileError { line: line_num, message: format!("Invalid connection target: '{}'", left) });
        return None;
    };

    // Parse right side (source: output providing value)
    let (source_id, source_port) = if let Some(rest) = right.strip_prefix("self.") {
        // self.port on right = group input providing value. Same bare-ident
        // constraint as above.
        if rest.is_empty() || !rest.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
            || !rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            errors.push(CompileError { line: line_num, message: format!("Invalid connection source: '{}'", right) });
            return None;
        }
        (format!("{}__in", group_id), rest.to_string())
    } else if let Some((node, port)) = parse_dotted(right) {
        (format!("{}.{}", group_id, node), port)
    } else {
        errors.push(CompileError { line: line_num, message: format!("Invalid connection source: '{}'", right) });
        return None;
    };

    Some(ParsedConnection {
        source_id: source_id,
        source_port: source_port,
        target_id: target_id,
        target_port: target_port,
        span: Some(Span::single_line(line_num, 0, trimmed.len())),
    })
}

/// Split `node.port` on the single dot. Both parts must be bare identifiers
/// (letters, digits, underscores) with at least one character. Rejects any
/// input with more than one dot so that malformed inputs like
/// `a.b.c = x.y` don't silently produce ports named `b.c`.
fn parse_dotted(s: &str) -> Option<(String, String)> {
    let dot = s.find('.')?;
    let node = s[..dot].trim();
    let port = s[dot + 1..].trim();
    if node.is_empty() || port.is_empty() {
        return None;
    }
    fn is_bare_ident(s: &str) -> bool {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
    if !is_bare_ident(node) || !is_bare_ident(port) {
        return None;
    }
    Some((node.to_string(), port.to_string()))
}

// ─── Config Value Parsing ───────────────────────────────────────────────────

fn parse_kv(
    s: &str,
    config: &mut serde_json::Map<String, serde_json::Value>,
    line: usize,
    errors: &mut Vec<CompileError>,
) {
    let colon_pos = match s.find(':') {
        Some(p) => p,
        None => return,
    };
    let key = s[..colon_pos].trim();
    let raw = s[colon_pos + 1..].trim();

    // Reject removed config keys
    if key == "mock" || key == "mocked" {
        errors.push(CompileError { line, message: format!("'{}' is not a valid config key. Use test configs for mocking.", key) });
        return;
    }

    let value = if raw == "true" {
        serde_json::Value::Bool(true)
    } else if raw == "false" {
        serde_json::Value::Bool(false)
    } else if raw.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') && !raw.is_empty() {
        if !raw.contains('.') {
            if let Ok(n) = raw.parse::<i64>() {
                serde_json::json!(n)
            } else if let Ok(n) = raw.parse::<f64>() {
                serde_json::json!(n)
            } else {
                serde_json::Value::String(unquote(raw))
            }
        } else if let Ok(n) = raw.parse::<f64>() {
            serde_json::json!(n)
        } else {
            serde_json::Value::String(unquote(raw))
        }
    } else if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        serde_json::Value::String(unescape(&raw[1..raw.len() - 1]))
    } else if raw.starts_with('[') || raw.starts_with('{') {
        serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_string()))
    } else {
        serde_json::Value::String(raw.to_string())
    };

    config.insert(key.to_string(), value);
}

fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        unescape(&s[1..s.len() - 1])
    } else {
        s.to_string()
    }
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some(other) => { out.push('\\'); out.push(other); }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Split `s` on `delimiter`, but only at top level: ignore delimiters
/// inside strings, parentheses, square brackets, and curly braces. Used
/// by the one-liner config parser so that values with nested braces
/// (inline expressions) or brackets (JSON) are not split mid-value.
fn split_respecting_quotes(s: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut depth: i32 = 0;
    for c in s.chars() {
        if c == '"' {
            in_quote = !in_quote;
        }
        if !in_quote {
            match c {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                _ => {}
            }
        }
        if c == delimiter && !in_quote && depth == 0 {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

// ─── Flattener ──────────────────────────────────────────────────────────────

fn flatten(state: ParseState, project_id: Uuid) -> Result<ProjectDefinition, Vec<CompileError>> {
    let mut nodes: Vec<NodeDefinition> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();

    let now = chrono::Utc::now();

    // Add top-level nodes
    for pn in &state.nodes {
        nodes.push(parsed_to_node_def(pn));
    }

    // Add top-level connections
    for pc in &state.connections {
        edges.push(parsed_to_edge(pc));
    }

    // Collect the structured group tree BEFORE flattening so the
    // GroupDefinitions capture the pre-flatten shape (which node ids
    // are direct members, which are grandchildren, etc).
    let mut groups: Vec<weft_core::GroupDefinition> = Vec::new();
    for group in &state.groups {
        collect_group_definitions(group, None, &mut groups);
    }

    // Flatten each group (recursively handles nested groups)
    for group in &state.groups {
        flatten_group(group, &mut nodes, &mut edges);
    }

    // Deduplicate edges
    {
        let mut seen = std::collections::HashSet::new();
        edges.retain(|e| {
            let key = (
                e.source.clone(),
                e.source_handle.clone().unwrap_or_default(),
                e.target.clone(),
                e.target_handle.clone().unwrap_or_default(),
            );
            seen.insert(key)
        });
    }

    Ok(ProjectDefinition {
        id: project_id,
        name: state.name,
        description: if state.description.is_empty() { None } else { Some(state.description) },
        nodes,
        edges,
        groups,
        status: Default::default(),
        created_at: now,
        updated_at: now,
    })
}

/// Walk the ParsedGroup tree and emit a GroupDefinition per group.
/// Direct children go into `node_ids`; nested groups go into
/// `child_group_ids` and recurse with their own entry.
fn collect_group_definitions(
    group: &ParsedGroup,
    parent_group_id: Option<String>,
    out: &mut Vec<weft_core::GroupDefinition>,
) {
    let in_ports: Vec<PortDefinition> = group
        .in_ports
        .iter()
        .map(|p| PortDefinition {
            name: p.name.clone(),
            port_type: p.port_type.clone(),
            required: p.required,
            description: None,
            lane_mode: p.lane_mode,
            lane_depth: 1,
            configurable: p.port_type.is_default_configurable(),
        })
        .collect();
    let out_ports: Vec<PortDefinition> = group
        .out_ports
        .iter()
        .map(|p| PortDefinition {
            name: p.name.clone(),
            port_type: p.port_type.clone(),
            required: false,
            description: None,
            lane_mode: LaneMode::Single,
            lane_depth: 1,
            configurable: p.port_type.is_default_configurable(),
        })
        .collect();

    // Direct node members (already scoped to this group's id by the
    // parser's rescope pass).
    let node_ids: Vec<String> = group.nodes.iter().map(|n| n.id.clone()).collect();
    let child_group_ids: Vec<String> = group.child_groups.iter().map(|g| g.id.clone()).collect();

    out.push(weft_core::GroupDefinition {
        id: group.id.clone(),
        label: Some(group.id.clone()),
        in_ports,
        out_ports,
        one_of_required: group.one_of_required.clone(),
        parent_group_id: parent_group_id.clone(),
        child_group_ids,
        node_ids,
        span: group.span.clone(),
        header_span: group.header_span.clone(),
    });

    for child in &group.child_groups {
        collect_group_definitions(child, Some(group.id.clone()), out);
    }
}

/// Build the scope chain for a group ID.
/// "outer.inner" → ["outer", "outer.inner"]
/// "mygroup" → ["mygroup"]
fn build_scope_chain(group_id: &str) -> Vec<String> {
    let mut chain = Vec::new();
    let parts: Vec<&str> = group_id.split('.').collect();
    for i in 0..parts.len() {
        chain.push(parts[..=i].join("."));
    }
    chain
}

fn flatten_group(
    group: &ParsedGroup,
    nodes: &mut Vec<NodeDefinition>,
    edges: &mut Vec<Edge>,
) {
    // Scope for the passthrough nodes: they belong to the group's parent scope.
    // Scope for internal nodes: includes this group.
    let internal_scope = build_scope_chain(&group.id);
    let boundary_scope = if internal_scope.len() > 1 {
        internal_scope[..internal_scope.len() - 1].to_vec()
    } else {
        vec![]
    };

    // 1. Create input passthrough: {groupId}__in
    let in_pt_id = format!("{}__in", group.id);
    let in_pt_inputs: Vec<PortDefinition> = group.in_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        lane_mode: p.lane_mode,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();
    let in_pt_outputs: Vec<PortDefinition> = group.in_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: false,
        description: None,
        lane_mode: LaneMode::Single,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();

    // Copy the group's @require_one_of directives onto the In passthrough's
    // features so the executor can consult them at the group boundary: if
    // any required input is skipped (or the oneOfRequired group is fully
    // skipped), the entire group body is skipped as a unit.
    let mut in_features = NodeFeatures::default();
    in_features.one_of_required = group.one_of_required.clone();
    nodes.push(NodeDefinition {
        id: in_pt_id.clone(),
        node_type: "Passthrough".to_string(),
        label: Some(format!("{} (in)", group.id)),
        config: serde_json::json!({"parentId": group.id}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: in_pt_inputs,
        outputs: in_pt_outputs,
        features: in_features,
        scope: boundary_scope.clone(),
        group_boundary: Some(GroupBoundary { group_id: group.id.clone(), role: GroupBoundaryRole::In }),
        entry_signals: Vec::new(),
        span: None,
        header_span: None,
        config_spans: Default::default(),
    });

    // 2. Create output passthrough: {groupId}__out
    // v2: all lane modes are Single. Expand/gather is inferred from types during enrichment.
    let out_pt_id = format!("{}__out", group.id);
    let out_pt_inputs: Vec<PortDefinition> = group.out_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: false,
        description: None,
        lane_mode: LaneMode::Single,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();
    let out_pt_outputs: Vec<PortDefinition> = group.out_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: false,
        description: None,
        lane_mode: LaneMode::Single,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();

    nodes.push(NodeDefinition {
        id: out_pt_id.clone(),
        node_type: "Passthrough".to_string(),
        label: Some(format!("{} (out)", group.id)),
        config: serde_json::json!({"parentId": group.id}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: out_pt_inputs,
        outputs: out_pt_outputs,
        features: NodeFeatures::default(),
        scope: boundary_scope.clone(),
        group_boundary: Some(GroupBoundary { group_id: group.id.clone(), role: GroupBoundaryRole::Out }),
        entry_signals: Vec::new(),
        span: None,
        header_span: None,
        config_spans: Default::default(),
    });

    // 3. Add internal nodes
    for pn in &group.nodes {
        nodes.push(parsed_to_node_def(pn));
    }

    // 4. Add internal connections
    for pc in &group.connections {
        edges.push(parsed_to_edge(pc));
    }

    // 5. Rewrite edges that reference the group ID directly
    for edge in edges.iter_mut() {
        if edge.target == group.id {
            edge.target = in_pt_id.clone();
        }
        if edge.source == group.id {
            edge.source = out_pt_id.clone();
        }
    }

    // 6. Recursively flatten child groups
    for child in &group.child_groups {
        flatten_group(child, nodes, edges);
    }
}

fn parsed_to_node_def(pn: &ParsedNode) -> NodeDefinition {
    let mut config = serde_json::Value::Object(pn.config.clone());
    if let Some(pid) = &pn.parent_id {
        config.as_object_mut().unwrap().insert("parentId".to_string(), serde_json::Value::String(pid.clone()));
    }
    let inputs = pn.in_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        lane_mode: p.lane_mode,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();
    let outputs = pn.out_ports.iter().map(|p| PortDefinition {
        name: p.name.clone(),
        port_type: p.port_type.clone(),
        required: p.required,
        description: None,
        lane_mode: p.lane_mode,
        lane_depth: 1,
        configurable: p.port_type.is_default_configurable(),
    }).collect();
    let mut features = NodeFeatures::default();
    features.one_of_required = pn.one_of_required.clone();
    let scope = match &pn.parent_id {
        Some(pid) => build_scope_chain(pid),
        None => vec![],
    };
    NodeDefinition {
        id: pn.id.clone(),
        node_type: pn.node_type.clone(),
        label: pn.label.clone(),
        config,
        position: Position { x: 0.0, y: 0.0 },
        inputs,
        outputs,
        features,
        scope,
        group_boundary: None,
        entry_signals: Vec::new(),
        span: pn.span,
        header_span: pn.header_span,
        config_spans: pn.config_spans.clone(),
    }
}

fn parsed_to_edge(pc: &ParsedConnection) -> Edge {
    Edge {
        id: format!("e-{}-{}-{}-{}", pc.source_id, pc.source_port, pc.target_id, pc.target_port),
        source: pc.source_id.clone(),
        target: pc.target_id.clone(),
        source_handle: Some(pc.source_port.clone()),
        target_handle: Some(pc.target_port.clone()),
        span: pc.span,
    }
}

// ─── Inline Expressions ─────────────────────────────────────────────────────
//
// Inline syntax lets the user declare a short-lived child node directly in
// the position where its output would otherwise be wired:
//
//     target.port = Template { template: "hi" }.text
//
//     my_llm = LlmInference {
//       systemPrompt: Template { template: "{{x}}" x: other.value }.text
//     }
//
// The parser recognizes the inline form natively during its main pass and
// emits a ParsedNode (the anon child) plus a ParsedConnection (the edge
// from anon.output to parent.field) into the current scope's InlineScope
// accumulator.
//
// Rules:
//   - Inline expressions are only allowed on the RHS of an edge assignment,
//     or as a config-field value inside a node declaration.
//   - The trailing `.portName` is mandatory.
//   - No post-config outputs: writing `Type { ... } -> (out: X).out` inline
//     is rejected. Declare the node with a name if you need post-config outs.
//   - Anon IDs: `{parent_id}__{field_or_port_name}`. Uniqueness is enforced
//     at the scope merge point (state.nodes / group.nodes).
//   - Nested inlines work naturally via recursion: the inline's body is a
//     config block parsed by the same parse_config_block that handles the
//     outer config, so a nested inline in a nested config field is picked
//     up in the same pass.

/// Check if a string value looks like the start of an inline node expression.
/// Returns true if it starts with an uppercase identifier followed by `(`,
/// `{`, `->`, or whitespace then one of those.
/// True if `s` looks like the start of an inline node expression. Accepted
/// forms (after stripping leading whitespace) all start with a type name
/// (uppercase identifier) followed by one of:
///     Type ( ... ) -> ( ... ) { ... }.port   // full form with ports + config
///     Type { ... }.port                      // config-only form
///     Type ( ... ) -> ( ... ).port           // ports-only form
///     Type.port                              // bare form: default config,
///                                            // no ports wired, just grab
///                                            // the output
fn looks_like_inline_start(s: &str) -> bool {
    let s = s.trim_start();
    if s.is_empty() { return false; }
    let first = match s.chars().next() { Some(c) => c, None => return false };
    if !first.is_ascii_uppercase() { return false; }
    let mut ident_len = 0;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            ident_len += c.len_utf8();
        } else {
            break;
        }
    }
    let rest = s[ident_len..].trim_start();
    if rest.starts_with('(') || rest.starts_with('{') || rest.starts_with("->") {
        return true;
    }
    // Bare form `Type.port`: the dot must be followed by an identifier
    // character so we don't catch things like `Type.` (trailing dot) or
    // an unrelated `Type ` at end of line.
    if let Some(after_dot) = rest.strip_prefix('.') {
        if let Some(c) = after_dot.chars().next() {
            return c.is_ascii_alphabetic() || c == '_';
        }
    }
    false
}

/// A config key must be a bare identifier (no dots, no spaces).
fn is_valid_config_key(s: &str) -> bool {
    if s.is_empty() { return false; }
    let first = match s.chars().next() { Some(c) => c, None => return false };
    if !first.is_ascii_alphabetic() && first != '_' { return false; }
    s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A dotted ref is 2+ identifier segments joined by `.`, no quotes, no
/// whitespace, no digits-only segments (so `3.14` is NOT a dotted ref).
/// Used in inline bodies to distinguish port wirings from literal config
/// values: `name: source.value` is a wiring, `price: 3.14` is a literal.
fn looks_like_dotted_ref(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return false; }
    if s.contains('"') || s.contains('\'') { return false; }
    // Exactly one dot: `node.port`. Multi-dot refs like `a.b.c` are not
    // valid port references and would be silently dropped by parse_dotted.
    let dot_count = s.chars().filter(|&c| c == '.').count();
    if dot_count != 1 { return false; }
    s.split('.').all(|seg| {
        if seg.is_empty() { return false; }
        let first = match seg.chars().next() { Some(c) => c, None => return false };
        if !first.is_ascii_alphabetic() && first != '_' { return false; }
        seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

/// Parse an inline expression starting at lines[start_line][start_col..].
/// Generates an anon child node, emits a connection from the child's output
/// to parent_id.field_key, and appends both to inline_scope. Returns the
/// index of the first line AFTER the inline expression (past `.portName`),
/// or None on parse error.
fn try_parse_inline_expression(
    lines: &[&str],
    start_line: usize,
    start_col: usize,
    parent_id: &str,
    field_key: &str,
    inline_scope: &mut InlineScope,
    errors: &mut Vec<CompileError>,
) -> Option<usize> {
    let first_line = lines[start_line];
    let line_num = start_line + 1;
    let after_start = first_line[start_col..].trim_start();

    // Extract the type name. Stop at the first `(`, `{`, `.`, or whitespace.
    // The `.` stop is required to support the bare form `Type.port` (no
    // config block, no ports, default construction).
    let type_end = after_start
        .find(|c: char| c == '(' || c == '{' || c == '.' || c.is_whitespace())
        .unwrap_or(after_start.len());
    let node_type = after_start[..type_end].trim().to_string();
    if node_type.is_empty() || !node_type.chars().next()?.is_ascii_uppercase() {
        return None;
    }
    if node_type == "Passthrough" {
        errors.push(CompileError {
            line: line_num,
            message: "'Passthrough' is a compiler-internal node type and cannot be used directly."
                .into(),
        });
        return None;
    }
    if !node_type.chars().all(|c| c.is_alphanumeric()) {
        errors.push(CompileError { line: line_num, message: format!("Invalid inline node type '{}'", node_type) });
        return None;
    }
    if node_type == "Group" {
        errors.push(CompileError { line: line_num, message: "Groups cannot be inlined".to_string() });
        return None;
    }

    let after_type = after_start[type_end..].trim_start();

    // Collect header across multiple lines. Reuse the existing helper which
    // understands port signatures and multi-line `-> (...)`.
    let (header_text, header_end_line) = if after_type.starts_with('(') || after_type.starts_with("->") {
        collect_declaration_header(lines, start_line, after_type)
    } else {
        (after_type.to_string(), start_line)
    };
    let header_text = header_text.trim();

    // Parse optional port signature.
    let (in_ports, out_ports, one_of_required, after_ports) = if header_text.starts_with('(') {
        parse_port_signature(header_text, line_num, errors)
    } else if header_text.starts_with("->") {
        let arrow_rest = header_text.strip_prefix("->").unwrap().trim();
        if arrow_rest.starts_with('(') {
            let mut outs = Vec::new();
            let mut oor = Vec::new();
            let (content, after) = match find_matching_paren(arrow_rest) {
                Some(p) => p,
                None => {
                    errors.push(CompileError { line: line_num, message: "Unclosed output port list in inline expression".to_string() });
                    return None;
                }
            };
            parse_port_list(&content, &mut outs, &mut oor, "out", line_num, errors);
            (Vec::new(), outs, oor, after)
        } else {
            (Vec::new(), Vec::new(), Vec::new(), header_text.to_string())
        }
    } else {
        (Vec::new(), Vec::new(), Vec::new(), header_text.to_string())
    };

    let after_ports = after_ports.trim();
    let body_start_line = header_end_line;

    // Anon id for this inline expression. Field/port name separator is `__`.
    // Conflict detection happens at scope merge.
    let anon_id = format!("{}__{}", parent_id, field_key);

    // Body can be:
    //   - a `{ ... }` block (one-liner or multi-line): parsed into config
    //   - completely absent (bare form `Type.port`): no config, just grab
    //     the default node and read one of its outputs. In the bare case
    //     `after_ports` starts with `.portName` directly.
    if after_ports.starts_with('.') {
        // Bare form: no body. Look for `.port` directly in after_ports.
        if let Some(port) = parse_inline_dot_port(after_ports) {
            // Synthesize typed input ports for any port wirings that may
            // have been registered for this anon id by nested inlines.
            // Bare-form anons never have wirings (no body), so this is
            // just an empty-ports node.
            let node = ParsedNode {
                id: anon_id.clone(),
                node_type: node_type,
                label: None,
                config: serde_json::Map::new(),
                parent_id: None,
                in_ports: in_ports,
                out_ports: out_ports,
                one_of_required: one_of_required,
                span: None,
                header_span: None,
                config_spans: Default::default(),
            };
            inline_scope.nodes.push(node);
            inline_scope.connections.push(ParsedConnection {
                source_id: anon_id,
                source_port: port,
                target_id: parent_id.to_string(),
                target_port: field_key.to_string(),
                span: None,
            });
            return Some(start_line + 1);
        } else {
            errors.push(CompileError {
                line: line_num,
                message: format!("Expected '.portName' in bare inline expression, got: '{}'", after_ports),
            });
            return None;
        }
    }

    let (config, label, body_end_line, one_liner_after_close) = if after_ports.starts_with('{') {
        // Find the matching `}` for the opening `{` on this line. If it's
        // there, this is a one-liner. Otherwise it's multi-line.
        if let Some(close_pos) = find_matching_brace_on_line(after_ports) {
            // One-liner: parse the body between positions 1 and close_pos.
            let body = after_ports[1..close_pos].trim();
            let mut config = serde_json::Map::new();
            let mut label = None;
            if !body.is_empty() {
                for pair in split_respecting_quotes(body, ',') {
                    let pair = pair.trim();
                    if pair.is_empty() { continue; }
                    if let Some(l) = try_extract_label(pair) {
                        label = Some(l);
                    } else {
                        parse_kv(pair, &mut config, line_num, errors);
                    }
                }
            }
            // Pass the text after `}` directly from `after_ports` where we
            // know the exact matched brace position, instead of re-discovering
            // it with rfind on the full source line (fragile when the line
            // contains `}` inside quoted strings).
            let after_close = after_ports[close_pos + 1..].trim().to_string();
            (config, label, start_line, Some(after_close))
        } else if after_ports == "{" {
            let (cfg, lbl, _extra_in, _extra_out, _extra_oor, _end_i, close_line) =
                parse_config_block_with_close(lines, body_start_line, line_num, errors, &anon_id, inline_scope);
            if close_line >= lines.len() {
                errors.push(CompileError { line: line_num, message: "Unclosed inline expression".to_string() });
                return None;
            }
            (cfg, lbl, close_line, None)
        } else {
            errors.push(CompileError { line: line_num, message: format!("Invalid inline node syntax: {}", after_ports) });
            return None;
        }
    } else {
        errors.push(CompileError { line: line_num, message: format!("Expected '{{' in inline expression, got: {}", after_ports) });
        return None;
    };

    // After the closing `}` we require `.portName`. For one-liners we have
    // the text after `}` directly from the brace matcher (handles quoted
    // `}` correctly). For multi-line we use rfind on the close line (safe
    // because multi-line close lines are bare `}` or `}.port`).
    let after_brace_owned: String;
    let after_brace: &str = if let Some(ref text) = one_liner_after_close {
        text.as_str()
    } else {
        let close_line_text = lines[body_end_line];
        let close_brace_pos = close_line_text.rfind('}').unwrap_or(0);
        after_brace_owned = close_line_text[close_brace_pos + 1..].to_string();
        after_brace_owned.as_str()
    };
    let after_brace = after_brace.trim();

    // Forbid post-config outputs: `Type { ... } -> (out: X).out`.
    if after_brace.starts_with("->") {
        errors.push(CompileError { line: body_end_line + 1, message: "Inline expressions cannot declare post-config outputs; declare the node with a name instead".to_string() });
        return None;
    }

    let (output_port, next_line) = if let Some(port) = parse_inline_dot_port(after_brace) {
        (port, body_end_line + 1)
    } else if after_brace.is_empty() {
        // Look on the next line for `.portName`.
        if body_end_line + 1 < lines.len() {
            let next = lines[body_end_line + 1].trim();
            if next.starts_with("->") {
                errors.push(CompileError { line: body_end_line + 2, message: "Inline expressions cannot declare post-config outputs".to_string() });
                return None;
            }
            if let Some(port) = parse_inline_dot_port(next) {
                (port, body_end_line + 2)
            } else {
                errors.push(CompileError { line: body_end_line + 1, message: "Inline expression missing required '.portName' after closing '}}'".to_string() });
                return None;
            }
        } else {
            errors.push(CompileError { line: body_end_line + 1, message: "Inline expression missing required '.portName' after closing '}}'".to_string() });
            return None;
        }
    } else {
        errors.push(CompileError { line: body_end_line + 1, message: format!("Expected '.portName' after inline '}}', got: '{}'", after_brace) });
        return None;
    };

    // Build the anon node. Ports come from the explicit signature
    // (`Type(x: String) { ... }`) and from catalog defaults at enrichment.
    // Port-wiring assignments inside the body (`x: src.value`) do NOT
    // synthesize ports here: the rule is "edges require a pre-existing,
    // pre-typed port". Literal assignments (`x: "hi"`) may synthesize a
    // port at enrichment time via WeftType::infer, gated on the catalog
    // node type's `canAddInputPorts` feature.
    let node = ParsedNode {
        id: anon_id.clone(),
        node_type: node_type,
        label,
        config,
        parent_id: None,
        in_ports: in_ports,
        out_ports: out_ports,
        one_of_required: one_of_required,
        span: None,
        header_span: None,
        config_spans: Default::default(),
    };
    inline_scope.nodes.push(node);
    inline_scope.connections.push(ParsedConnection {
        source_id: anon_id,
        source_port: output_port,
        target_id: parent_id.to_string(),
        target_port: field_key.to_string(),
        span: None,
    });

    Some(next_line)
}

/// Find the position of the `}` that matches the opening `{` at the start
/// of `s`. Respects string literals so that `{` or `}` inside quotes don't
/// break matching. Returns None if there's no matching close on this line.
fn find_matching_brace_on_line(s: &str) -> Option<usize> {
    if !s.starts_with('{') { return None; }
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    let mut in_string: Option<u8> = None;
    let mut escape = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if escape { escape = false; i += 1; continue; }
        if let Some(q) = in_string {
            if c == b'\\' { escape = true; }
            else if c == q { in_string = None; }
            i += 1;
            continue;
        }
        match c {
            b'"' | b'\'' => in_string = Some(c),
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 { return Some(i); }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse a `.portName` suffix. Accepts strings like `.text`, ` .text`,
/// ` .text `. Rejects anything with extra characters after the port name.
fn parse_inline_dot_port(s: &str) -> Option<String> {
    let t = s.trim();
    let rest = t.strip_prefix('.')?;
    if rest.is_empty() { return None; }
    let mut end = 0;
    for c in rest.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            end += c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 { return None; }
    let (name, tail) = rest.split_at(end);
    if !tail.trim().is_empty() { return None; }
    Some(name.to_string())
}

/// Top-level connection parser with inline RHS support. Accepts:
///   - `target.port = source.port`          (edge)
///   - `target.port = InlineType { ... }.x` (inline expression + edge)
///   - `target.port = Type.port`            (bare inline + edge)
///   - `target.port = "literal"` / number / bool / JSON  (config fill)
///
/// For literal RHS the parser pushes a `ConfigFill` into `inline_scope`
/// instead of returning a ParsedConnection. The caller applies it to the
/// target node's config. Literals take over from any inline config value
/// for the same `(target, port)` pair because they appear later in source.
fn try_parse_connection_with_inline(
    lines: &[&str],
    start: usize,
    errors: &mut Vec<CompileError>,
    inline_scope: &mut InlineScope,
) -> ParseConnectionResult {
    let line = lines[start];
    let trimmed = line.trim();
    let eq_pos = match trimmed.find('=') { Some(p) => p, None => return ParseConnectionResult::NotAConnection };
    let left = trimmed[..eq_pos].trim();
    let right = trimmed[eq_pos + 1..].trim();
    if !left.contains('.') { return ParseConnectionResult::NotAConnection; }
    let (target_id, target_port) = match parse_dotted(left) {
        Some(pair) => pair,
        None => return ParseConnectionResult::NotAConnection,
    };

    if looks_like_inline_start(right) {
        let eq_byte_pos = match line.find('=') { Some(p) => p, None => return ParseConnectionResult::NotAConnection };
        let start_col = eq_byte_pos + 1;
        let next_i = match try_parse_inline_expression(
            lines, start, start_col, &target_id, &target_port, inline_scope, errors,
        ) {
            Some(n) => n,
            None => return ParseConnectionResult::NotAConnection,
        };
        let conn = match inline_scope.connections.pop() {
            Some(c) => c,
            None => return ParseConnectionResult::NotAConnection,
        };
        return ParseConnectionResult::Edge(conn, next_i);
    }

    // Dotted-ref source: standard edge.
    if let Some((source_id, source_port)) = parse_dotted(right) {
        return ParseConnectionResult::Edge(
            ParsedConnection {
                source_id: source_id,
                source_port: source_port,
                target_id: target_id,
                target_port: target_port,
                span: Some(Span::single_line(start + 1, 0, line.len())),
            },
            start + 1,
        );
    }

    // Single-line literal RHS: config fill.
    if let Some(value) = try_parse_literal(right) {
        inline_scope.config_fills.push(ConfigFill {
            target_id,
            target_port,
            value,
        });
        return ParseConnectionResult::ConfigFill(start + 1);
    }

    // Multi-line triple-backtick RHS: `target.port = ``` ... ``` ` spanning
    // several lines. Collect, dedent, unescape, store as a string.
    if right.starts_with("```") {
        let after_bt = &right[3..];
        // Inline one-liner `target.port = ```content``` ` already fails the
        // check above because `right` starts with ``` and doesn't parse via
        // try_parse_literal. Handle it here too.
        if after_bt.ends_with("```") && after_bt.len() > 3 {
            let inline_val = &after_bt[..after_bt.len() - 3];
            inline_scope.config_fills.push(ConfigFill {
                target_id,
                target_port,
                value: serde_json::Value::String(inline_val.to_string()),
            });
            return ParseConnectionResult::ConfigFill(start + 1);
        }
        // Multi-line: collect until closing ```.
        let (unescaped, next) = collect_heredoc_value(lines, start + 1, after_bt);
        inline_scope.config_fills.push(ConfigFill {
            target_id,
            target_port,
            value: serde_json::Value::String(unescaped),
        });
        return ParseConnectionResult::ConfigFill(next);
    }

    // Multi-line JSON RHS: `target.port = { ... }` or `target.port = [ ... ]`
    // with braces/brackets spread across lines.
    if let Some((collected, balanced, next)) = collect_multiline_json(lines, start + 1, start, right) {
        if !balanced {
            errors.push(CompileError {
                line: start + 1,
                message: format!("Broken JSON for '{}.{}': brackets not balanced", target_id, target_port),
            });
            return ParseConnectionResult::ConfigFill(next);
        }
        let value = serde_json::from_str::<serde_json::Value>(&collected)
            .unwrap_or(serde_json::Value::String(collected));
        inline_scope.config_fills.push(ConfigFill {
            target_id,
            target_port,
            value,
        });
        return ParseConnectionResult::ConfigFill(next);
    }

    ParseConnectionResult::NotAConnection
}

enum ParseConnectionResult {
    NotAConnection,
    Edge(ParsedConnection, usize),
    ConfigFill(usize),
}

/// Write a connection-line literal into the target node's config map.
/// Later writes override earlier ones for the same key. If the target node
/// isn't found, the fill is silently dropped; enrichment's edge validation
/// catches the bad target separately.
fn apply_config_fill(nodes: &mut Vec<ParsedNode>, fill: ConfigFill) {
    if let Some(node) = nodes.iter_mut().find(|n| n.id == fill.target_id) {
        node.config.insert(fill.target_port, fill.value);
    }
}

/// Parse a literal RHS value (quoted string, number, bool, JSON array/object)
/// into a serde JSON value. Returns None if the input doesn't match any
/// literal form (caller falls through to the next interpretation).
fn try_parse_literal(s: &str) -> Option<serde_json::Value> {
    let raw = s.trim();
    if raw.is_empty() { return None; }
    if raw == "true" { return Some(serde_json::Value::Bool(true)); }
    if raw == "false" { return Some(serde_json::Value::Bool(false)); }
    // Numbers (integer or float, negative allowed).
    if raw.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
        if !raw.contains('.') {
            if let Ok(n) = raw.parse::<i64>() {
                return Some(serde_json::json!(n));
            }
        }
        if let Ok(n) = raw.parse::<f64>() {
            return Some(serde_json::json!(n));
        }
    }
    // Quoted string.
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return Some(serde_json::Value::String(unescape(&raw[1..raw.len() - 1])));
    }
    // JSON array or object.
    if raw.starts_with('[') || raw.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
            return Some(v);
        }
    }
    None
}

/// Group-body connection parser with inline RHS support. Mirrors
/// `try_parse_connection_with_inline` but uses `try_parse_group_connection`
/// for the simple path.
fn try_parse_group_connection_with_inline(
    lines: &[&str],
    start: usize,
    line_num: usize,
    group_id: &str,
    errors: &mut Vec<CompileError>,
    inline_scope: &mut InlineScope,
) -> ParseConnectionResult {
    let line = lines[start];
    let trimmed = line.trim();
    let eq_pos = match trimmed.find('=') { Some(p) => p, None => return ParseConnectionResult::NotAConnection };
    let right = trimmed[eq_pos + 1..].trim();

    if looks_like_inline_start(right) {
        let left = trimmed[..eq_pos].trim();
        if !left.contains('.') { return ParseConnectionResult::NotAConnection; }
        let (target_id_local, target_port) = match parse_dotted(left) {
            Some(p) => p,
            None => return ParseConnectionResult::NotAConnection,
        };
        let (parent_id_local, field_key) = if left.starts_with("self.") {
            ("self".to_string(), target_port.clone())
        } else {
            (target_id_local.clone(), target_port.clone())
        };

        let eq_byte_pos = match line.find('=') { Some(p) => p, None => return ParseConnectionResult::NotAConnection };
        let start_col = eq_byte_pos + 1;
        let next_i = match try_parse_inline_expression(
            lines, start, start_col, &parent_id_local, &field_key, inline_scope, errors,
        ) {
            Some(n) => n,
            None => return ParseConnectionResult::NotAConnection,
        };

        let mut conn = match inline_scope.connections.pop() {
            Some(c) => c,
            None => return ParseConnectionResult::NotAConnection,
        };
        let _ = target_id_local;
        conn.source_id = format!("{}.{}", group_id, conn.source_id);
        if left.starts_with("self.") {
            conn.target_id = format!("{}__out", group_id);
        } else {
            conn.target_id = format!("{}.{}", group_id, parent_id_local);
        }
        return ParseConnectionResult::Edge(conn, next_i);
    }

    // Literal RHS config fill. Works for `child.port = literal` inside a
    // group body. `self.port = literal` is nonsensical (self is the group's
    // output, driven by a child, not by a literal), so we skip it here.
    let left = trimmed[..eq_pos].trim();
    if !left.starts_with("self.") {
        if let Some((target_id_local, target_port)) = parse_dotted(left) {
            let scoped_target = format!("{}.{}", group_id, target_id_local);

            // Single-line literal.
            if let Some(value) = try_parse_literal(right) {
                inline_scope.config_fills.push(ConfigFill {
                    target_id: scoped_target,
                    target_port,
                    value,
                });
                return ParseConnectionResult::ConfigFill(start + 1);
            }

            // Multi-line triple-backtick RHS.
            if right.starts_with("```") {
                let after_bt = &right[3..];
                if after_bt.ends_with("```") && after_bt.len() > 3 {
                    let inline_val = &after_bt[..after_bt.len() - 3];
                    inline_scope.config_fills.push(ConfigFill {
                        target_id: scoped_target,
                        target_port,
                        value: serde_json::Value::String(inline_val.to_string()),
                    });
                    return ParseConnectionResult::ConfigFill(start + 1);
                }
                // Multi-line: collect until closing ```.
                let (unescaped, next) = collect_heredoc_value(lines, start + 1, after_bt);
                inline_scope.config_fills.push(ConfigFill {
                    target_id: scoped_target,
                    target_port,
                    value: serde_json::Value::String(unescaped),
                });
                return ParseConnectionResult::ConfigFill(next);
            }

            // Multi-line JSON RHS.
            if let Some((collected, balanced, next)) = collect_multiline_json(lines, start + 1, start, right) {
                if !balanced {
                    errors.push(CompileError {
                        line: line_num,
                        message: format!("Broken JSON for '{}.{}': brackets not balanced", target_id_local, target_port),
                    });
                    return ParseConnectionResult::ConfigFill(next);
                }
                let value = serde_json::from_str::<serde_json::Value>(&collected)
                    .unwrap_or(serde_json::Value::String(collected));
                inline_scope.config_fills.push(ConfigFill {
                    target_id: scoped_target,
                    target_port,
                    value,
                });
                return ParseConnectionResult::ConfigFill(next);
            }
        }
    }

    // Simple case: delegate to the existing group connection parser.
    match try_parse_group_connection(trimmed, line_num, group_id, errors) {
        Some(c) => ParseConnectionResult::Edge(c, start + 1),
        None => ParseConnectionResult::NotAConnection,
    }
}

