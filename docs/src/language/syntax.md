# Syntax

The whole surface, in one page. The language is written mostly by models, so
the surface is small and strict: there is one way to say each thing, and the
compiler refuses everything else.

## Declaring a node

```weft
name = NodeType
name = NodeType { config_field: value, ... }
name = NodeType {}
```

`name` is the node's id and must be unique within its scope. `NodeType` must
exist in the project's `nodes/` catalog.

## Connecting

```weft
target.input_port = source.output_port
```

Read it right to left: the value flows from `source.output_port` into
`target.input_port`, and the types have to be compatible. Every required input
must be wired; an optional one (`port?`) may be left alone.

If you only want one key of the value, keep going with dots:

```weft
speed.wpm = reader.profile.stats.wpm
```

That is still one wire, from `reader.profile`, and it delivers `stats.wpm`
read off the value. For what it needs from the type, go and read
[reading a key off a wire](types.md#reading-a-key-off-a-wire).

## Config values

Config fields are typed JSON-ish literals.

```weft
t     = Text     { value: "a string" }
n     = Range    { to: 10, step: 2 }
flag  = SomeNode { enabled: true }
arr   = SomeNode { items: [1, 2, 3] }
obj   = SomeNode { opts: { "k": "v" } }

multi = SomeNode {
  fields: [
    { "kind": "text_input", "key": "name" }
  ]
}
```

Multi-line arrays and objects are fine, and the comma between two fields is
optional, so a field per line with no commas reads the same to the compiler.

### Wires in the braces

A field whose value is `source.port` is a wire, not a config value. It is the
same edge as the connection line, written inside the node it feeds.

```weft
reply = TelegramSendMedia {
  kind: "photo"
  chatId: ask.chatId          # identical to `reply.chatId = ask.chatId`
  file: picture.image
}
```

Both forms compile to one edge, and the editor can rewrite either. Which one
to write is taste: a node with several wires reads better with them in its
braces, next to its settings, instead of a stack of lines each starting with
the same name.

A `Group` is the exception. Its braces hold its children, so the only field
it reads there is `_should_flow`. Its interface ports are driven from
outside, on their own lines, by a wire or by a value.

#### A key that CREATES a port

On a node type that accepts extra inputs (`ExecPython`, `FirstInOrder`,
`TagRun`, `StopTagged`), a key
naming no declared port creates one. A wire gives it the type of whatever feeds
it, a literal gives it the literal's own type, and a `null` literal is an error
because it says nothing about the type.

```weft
step = ExecPython -> (out: String) {
  code: @file("scripts/step.py")
  text: draft.answer      # a String port, from the wire
  limit: 3                # a Number port, from the literal
  notes?: review.notes    # optional: a closure here does not skip the node
}
```

Created ports keep the order they are written in, which is what
`FirstInOrder` reads: its first input that carried a value is the one it
emits. Reordering two of its lines changes which branch wins, and it is the
only place in weft where the order of lines means anything.

The `?` goes on the key, because it describes the port being created rather
than the wire's source, and it is refused on a key that creates no port (say
so on the port itself instead: `notes?: String` in the signature).

### Literals on a connection line

When the target is a node's own port and the right side is a literal, the line
fills that node's config instead of creating an edge.

```weft
post = SlackSendMessage { channel: "#alerts" }   # in the braces
post.text = "deploy finished"                    # or on its own line
```

Both spellings are the same thing, a constant written for the port, and no
port takes one spelling and refuses the other. What a port can refuse is a
whole family: a value written in the source (`literal`) or a value another
node produces (`wire`). Every port takes both unless its node says otherwise
with `accepts` in its metadata, and getting it wrong is `input-accepts`, with
the message reading the list back ("`params` accepts: wire"). A port the
compiler reads to build the node (a form's `fields`, the access picker) is the
one exception: it takes an inline typed value only, never a wire and never a
`@file` or `@asset`.

The same line works on a `Group`, a `Loop`, or an `@include` alias: its
ports are the ones in its signature, and a value on one reaches everything
inside that reads it.

```weft
escalation.tone = "formal"
```

An output never takes a value: a firing emits on it, and you read it as
`node.port`. Writing one (`step.out = "lit"`, or `out: "lit"` in the braces of
a node whose signature declares `-> (out: String)`) is refused, and so is a
group's own output written from inside (`self.result = "lit"`). Drive it from
a node.

### Multi-line strings

Triple-backtick blocks carry code, templates, anything with newlines in it.

````weft
step = ExecPython() -> (out: Number) {
  code: ```
    return {'out': 42}
  ```
}
````

### Reserved keys

Keys starting with `_` are reserved, and there are exactly three.

| Key | What it does |
|---|---|
| `_label: "..."` | sets the node's display label. A quoted string, settable once, never by wire. |
| `_tags: ["a", "b"]` | attaches tags, used by signal scoping. |
| `_should_flow: <wire or false>` | decides whether this node runs at all. |

<!-- SYNC: reserved keys <-> crates/weft-core/src/exec/skip.rs SHOULD_FLOW_PORT,
     packages/weft-graph/src/protocol.ts SHOULD_FLOW_PORT,
     packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README) -->

Any other leading-underscore key is a compile error, so the namespace stays
available.

`_should_flow` is how a branch turns off. Leave it out and the node runs.
Wire it and the node runs only when what arrives is not `false`; a `false`, or
a closure (whatever decides never spoke), skips the node, which closes its
outputs, which skips everything behind it. Writing `_should_flow: false`
straight into the braces turns one node off without touching anything else.

```weft
reply = SlackSendMessage {
  _should_flow: review.approved
  text: draft.answer
}
```

A `Group` or a `Loop` takes it too, written inside its braces alongside
everything else, or from outside on the container's name
(`escalation._should_flow = false`), the same way you would set any of its
interface ports. A group that does not flow takes everything inside it with
it, however deeply nested.

```weft
escalation = Group(question: String) -> (answer: String) {
  _should_flow: route.needs_a_person

  ...
}
```

The node itself never sees this port: it is the language deciding whether to
call the node, not data the node reads.

## Inline port signatures

Some node types let you declare their ports in the declaration itself, with an
arrow. `ExecPython` is the canonical one.

```weft
calc = ExecPython(a: Number, b: Number) -> (sum: Number, diff: Number) {
  code: "return {'sum': a + b, 'diff': a - b}"
}
```

Inputs arrive in the code as variables named after each port, and the code
returns a dict keyed by output port name. A key set to `None`, or missing
entirely, emits no pulse on that port, which closes it. The compiler
type-checks these ports exactly like declared ones.

You only write the ports the node leaves open. Anything its metadata already
types keeps that type, so a signature can be inputs only, outputs only, or a
single port, and a node whose ports are all pinned needs no signature at all.

```weft
answer = LlmInference -> (response: String)   # the rest of its ports are typed
ok     = Cast -> (value: Boolean)             # the whole point of Cast
```

An empty body is the same as no body, so `Cast -> (value: Boolean) {}` and
`Cast() -> (value: Boolean)` are the line above with more typing.

## Inline expressions

A node literal can appear directly as a value, with a **mandatory** trailing
`.port` naming which of its outputs feeds the target.

```weft
out.data = Text { value: "hi" }.value
```

That synthesizes an anonymous child node (id `{host}__{field}`, here
`out__data`) plus the edge into `out.data`. The same form works as a config
field's value inside a node body, and carries full node syntax including its
own inline signature and nesting. With a signature, the `.port` reads one of
the outputs the signature declares:

```weft
summary.text = ExecPython(m: List[JsonDict]) -> (text: String) {
  code: "return {'text': ' '.join(x['body'] for x in m)}"
}.text
```

Omitting the trailing `.port` is a compile error, because a node with several
outputs would otherwise be silently ambiguous.

## Comments and descriptions

`#` starts a line comment.

One position is special: if the **first line inside a group or loop body** is a
plain comment, that line becomes the group's description and tooling shows it
when the group is collapsed.

```weft
preprocessor = Group(raw: String) -> (result: String) {
  # Cleans and transforms text
  ...
}
```

The same rule applies inside an included file's top-level group body. Comments
outside any group have no special meaning.

The project's name and id live in `weft.toml`, not in the source, so there is
no header comment to keep in sync.

## Directives

`@require_one_of(a, b)` states that at least one of the named inputs must be
satisfied, either wired or set to a non-null literal. It goes on its own line
inside a node body, or inside an inline port signature. A group or loop
refuses it: a group's inputs are all optional at its boundary, so the
directive belongs on the node inside that needs one of them.

```weft
lookup = SlackFindUser {
  @require_one_of(email, phone)
}
```

It is a compile error when unmet (`require-one-of-unmet`), and it also governs
runtime skipping: the node is skipped when every port in the group arrives
closed.

Catalog nodes declare the same thing in their metadata as `oneOfRequired`, so
a node author can build the requirement in rather than relying on every caller
to write the directive.

## Whitespace and formatting

The parser keeps every byte, including whitespace and comments, in a lossless
tree. That is why the editor can rewrite one config field through a GUI
gesture without reformatting your file, and why a round trip through the
compiler is byte-exact when nothing changed.

There is no formatter, and no formatting rules are enforced.

## The full grammar, informally

```
file        := decl*
decl        := IDENT '=' node_expr
             | IDENT '=' '@include' '(' STRING ')'
             | connection
node_expr   := TYPE port_sig? body?
port_sig    := port_sig_in? port_sig_out?
port_sig_in := '(' port_decl,* ')'
port_sig_out:= '->' '(' port_decl,* ')'
port_decl   := IDENT '?'? ':' type     # `?` (inputs only) = may be absent
body        := '{' body_item* '}'
body_item   := IDENT '?'? ':' value     # config field (`?` = the port it
             |                          #   creates is optional)
             | IDENT '?'? ':' path      # a wire into this node's port
             | connection               # inside a group or loop
             | directive
             | COMMENT
connection  := path '=' ( path | value | node_expr '.' IDENT )
path        := IDENT '.' IDENT | 'self' '.' IDENT
```

`Group` and `Loop` are node types with bodies containing connections, covered
in [Groups](groups.md) and [Loops](loops.md).
