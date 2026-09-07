# Types

Every port has a type. Every connection is checked against both ends before
anything runs. This page is the complete list.

## Primitives

| Type | Holds |
|---|---|
| `String` | text |
| `Number` | any number, integer or not |
| `Boolean` | true or false |
| `Null` | the absence of a value, as a value |
| `Image` | a stored image file |
| `Video` | a stored video file |
| `Audio` | a stored audio file |
| `Blob` | any other stored file: a pdf, a zip, a csv |
| `Empty` | the type of a value that cannot exist. An empty list literal is a `List[Empty]`, and it wires into a list of anything. |

The four file types are one idea with four names. What travels a wire is a
small reference to a stored file rather than the bytes, so a conversation
carrying twenty images stays cheap to journal. The type is what tells the
runtime which slots hold files, which is what makes
[media conversion at a provider boundary](../nodes/custom-types.md#media-inside-a-custom-type)
possible without per-node code.

You never write `Empty` yourself. It turns up when the compiler has nothing to
go on, as in `List[Empty]` for `[]`, and it makes unions simplify:
`Number | Empty` is `Number`.

## Union aliases

`Media` is `Image | Video | Audio`, and `File` is `Media | Blob`. Both are just
names for those unions.

## Containers

```
List[Number]
List[List[String]]
Dict[String, String]
```

## Unions

```
String | Number
Number | Null
```

## `JsonDict`

An opaque `Dict[String, *]` whose value types are unchecked. It is compatible
with any `Dict[String, V]` in both directions.

Reach for it if you are holding a raw API response whose shape you do not know,
or do not want to declare. It says "I am not claiming to know what is in
here".

## Records

A dict with known field names and per-field types.

```
{ role: String, name?: String }
```

A `?` on a field marks it optional: the key may be absent, and a present
`null` counts as absent.

Validation is strict. A value carrying a key the record does not declare is
**refused**. A record is a contract, so declare every field the real values
carry.

## Reading a key off a wire

If a node gives you a record and the next node wants one field of it, write
the field on the wire:

```weft
reader = ExecPython() -> (profile: { stats: { wpm: Number, name?: String } }) { ... }
speed  = ExecPython(wpm: Number) -> (out: Number) {
  wpm: reader.profile.stats.wpm
}
```

The compiler walks the keys against the record type: each one has to be a
field of the record at that level, and the wire's type is the last field's
type, so `wpm: Number` above type-checks like any other wire. A type with no
fields to walk, a `JsonDict` or a scalar, is refused with `deref-path` and the
fix in the message: declare the shape on the source port, or `Cast` first.

At run time the value is read right before it lands, once per wire. Five wires
off one port are five separate reads, and none of them changes what the
others get. A `?` key that turns out absent (or `null`) closes that wire
alone, and the port on the other end decides what a closure means to it:
a required port skips the node, an optional one fires with the value missing.
A required key that is absent is a value that broke its declared type, and
that fails the firing rather than turning into a `null`.

In the graph, such a wire is drawn dotted with the path written at its end.
Right-click any wire whose value is a record to pick a key.

## Named custom types

A type gets a name in one of two places. Any node's `metadata.json` can
declare named types, and once declared anywhere in the project the name is
usable in every port type and every inline signature.

```json
"types": {
  "ChatHistory": "List[ChatMessage]",
  "ChatMessage": "{ role: String, content: String | List[Part], name?: String }"
}
```

Or the `.weft` source declares one itself, at the top of a scope:

```weft
type Profile = {
  wpm: Number,          # words per minute
  read_delay: Number
}
type Names = List[String]

typing = ExecPython(p: Profile, who: Names) -> (delay: Number) { ... }
```

The right-hand side is any type the language has, over as many lines as it
needs, and it may name other declared types. Where you write it decides who
sees it: a declaration at file level is visible to every header in the file,
one directly inside a group or loop body is visible in that body and every
body nested in it, and nowhere else. The group's own signature sits outside
its braces, so a type declared inside cannot name the group's ports. Order
within a scope does not matter. A node's braces hold its values, so a `type`
line inside them is refused, and a name that is already visible (from an
outer scope, from a metadata `types` block, or a builtin like `Media`)
cannot be declared again: nothing shadows. An included file sees the
catalog's types and its own declarations, never the including file's, so a
component compiles the same on its own as spliced in.

Named types are **nominal**: the name is the contract, not the shape.

- A `ChatHistory` value wires freely into `JsonDict`, or into its own
  structural shape.
- Nothing unnamed wires into a `ChatHistory` input. A dict that happens to have
  the right shape is not one.

The door between the two worlds is the `Cast` node, below.

Declaring the same name twice is fine when the bodies are structurally
identical, so two packages can ship the same shared type without depending on
each other. Two different bodies under one name is a loud error, which turns
drift between two copies into a build failure.

## Type variables

A bare capitalized name like `T` is a generic that unifies across a node's
ports, so a node with input `T` and output `T` carries whatever concrete type
flows in. Every type variable has to be pinned to something concrete somewhere
in the graph, and one that is not is rejected as `unresolved-typevar`.

## `MustOverride`

A node whose metadata cannot know a port's type declares it `MustOverride`,
and the `.weft` author has to pin it with an inline port signature. A
`MustOverride` port that is wired and still unpinned at compile time is an
error (`must-override-unmet`); one nothing reads or writes is left alone.

`Cast` is the main user of this: its output type is whatever you say it is.

## The `?` marker

`?` after a name means "may be absent", and it goes on the name in every
place it can appear: an input port (`here?: String`) and a record field
(`{ role: String, name?: String }`). On an input port it lets the port accept
a closed pulse; without it, a required input that receives closure skips the
node and cascades that closure downstream. See [How a weft program
runs](mental-model.md#the-closed-pulse). On a config key that creates a port
(`notes?: review.notes`) it marks that created port. See [wires in the
braces](syntax.md#wires-in-the-braces).

`name: String?` is refused, and the message spells the accepted form. An
output port takes no `?` at all: a firing that emits nothing on it closes it,
and there is nothing for a marker to add.

`Number | Null` is a different thing: `null` is a value that arrives, `?` is a
value that does not.

## The wired-only types

Three types never appear as literals in source, because their values are live
runtime handles that only exist while something is running.

### `Bus`

A message channel between nodes that are alive at the same time. A `Bus`
output connects only to a `Bus` input. Message payloads are not type-checked
by the language; the channel carries what its creator declared it carries.

### `Generator[T]`

A typed, one-directional, terminating stream.

The producer's port accepts being emitted into repeatedly, and each emission is
one item checked against `T`. The producer's own state lives in ordinary local
variables across all of them.

The consumer fires **once**, on the first item, and pulls the rest in its own
code. Or a `Loop` names the port in `over` and pulls one item per iteration.
The stream ends when the producer's body returns.

One producer feeds one consumer, a stream cannot leave the group it was made
in, and it takes no literal. Why each of those holds, when to reach for a
stream over a `List[T]`, and what happens when one side stops early:
[Live channels](live-channels.md#streams).

### `Access`

The authorized ability to call a third-party service. One type for every
service, emitted by the node that holds the connection and consumed by the
nodes that make calls.

Because one type covers every service, the compiler never has to know which
services exist. Wiring the wrong service's access into a node fails loudly at
run time.

## The `Cast` node

`Cast` converts a value into the type declared on its output. The inline
signature pins that type, since the metadata ships the output as
`MustOverride`.

```weft
raw = HttpRequest { url: "https://api.example.com/history.json" }
history = Cast() -> (value: ChatHistory)
history.value = raw.body
```

The conversion table is checked at compile time, so an impossible pair like
`JsonDict -> Number` is a compile error rather than a runtime surprise.

What is possible:

- text parses into numbers, booleans, and JSON structures,
- data stringifies,
- `Number` and `Boolean` interconvert as 1 and 0,
- any object shape casts into a record or named type **by validation**: the
  value is held to the declared structure and a mismatch is an error naming
  the exact offending field.

So `Cast` is where you say "this dict really is a `ChatHistory`", and the
runtime checks that before letting it through.

## Compatibility, precisely

A connection from a source type `S` to a target type `T` is allowed when every
value `S` can produce is a value `T` accepts.

- Identical types are compatible.
- A union is compatible with a target when every member is.
- `JsonDict` is compatible with any `Dict[String, V]` in both directions.
- A named type is compatible with `JsonDict` and with its own structural shape,
  but nothing unnamed is compatible with a named target.
- Containers are compatible element-wise.
- A type variable unifies with whatever concrete type reaches it, and stays
  that type for every other port that shares the variable.

Anything else is `type-mismatch`, and the message names both types.
