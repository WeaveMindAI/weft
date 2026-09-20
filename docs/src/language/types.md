# Types

An arrow has to carry something its destination can accept. A `String` output
will not go into a `Number` input, and the compiler says `type-mismatch` and
names both sides. Convert the value, or change the step that produced it.

Types describe single values and whole structures, so a list of messages or a
customer record has a type too. They cannot tell you whether a model's answer
was any good.

## The everyday types

| Type | What it holds |
|---|---|
| `String` | `"hello"` |
| `Number` | `42` or `3.5` |
| `Boolean` | `true` or `false` |
| `Null` | The JSON value `null` |
| `Image`, `Video`, `Audio` | A reference to a stored file of that kind |
| `Blob` | A reference to any other stored file, a PDF or a zip |

A file reference travels through the graph while the bytes stay in storage, so
you can carry an image inside a message record without copying it into every
journal row. For handling files, read [Files at run
time](../running/files.md).

`Media` is short for `Image | Video | Audio`, and `File` is short for those
three plus `Blob`.

## Lists, dictionaries and unions

```text
List[Number]
List[List[String]]
Dict[String, Number]
String | Number
Number | Null
```

`Dict[String, Number]` has string keys and number values. A union like `String
| Number` accepts either. That union cannot then feed a `Number` input,
because it might turn out to be a string.

An empty list `[]` gets the type `List[Empty]`, because there is no item to
learn from. `Empty` means no value is possible, so an empty list goes into a
`List[String]` or a `List[Number]` without complaint. Inside a union, `Number
| Empty` collapses to `Number`.

## When the field names matter: records

Use a record when the field names matter:

```text
{ name: String, age: Number, nickname?: String }
```

`name` and `age` have to be there. `nickname` does not, and a `null` counts as
not there for an optional field. An extra field the record does not declare
fails validation, so the declaration has to list every field your values
really carry.

Here is a step producing a record and another reading one field out of it:

```weft
person = ExecPython -> (profile: { name: String, age: Number }) {
  code: "return {'profile': {'name': 'Mina', 'age': 34}}"
}
show = Debug { data: person.profile.name }
```

### Reading a key off a wire

If you want one field out of a record, add its name to the source port:

```weft
speed.wpm = reader.profile.stats.wpm
```

The compiler follows `stats`, then `wpm`, through the declared record type,
and the arrow ends up with `wpm`'s type. A plain value has no declared fields
to follow, so you get `deref-path` asking you to declare the shape or `Cast`
it first. The same goes for a `JsonDict`, further down this page: it holds an
object without saying what is in it.

In the graph, a path like this draws as a dotted arrow with the field names at
its end. If you would rather not type it, right-click any arrow carrying a
record and pick a field.

If an optional field is missing or `null`, only that arrow closes, and
whatever was waiting on it does what any step does with a closed input. For
those rules, go and read [How a weft program runs](mental-model.md). A missing
required field is bad data and fails the firing.

## Optional or nullable: which one you want

A field that can be absent and a field that can hold `null` are two different
declarations:

```text
count?: Number
count: Number | Null
```

The first says the value can be absent. The second says something has to
arrive, and it is allowed to be `null`. A closed required input skips the
step; a `null` on a nullable input reaches the code.

Put the `?` after the name, including on record fields; `count: Number?` is an
error. Outputs never take `?`. A step is always allowed to finish without
emitting on an output, and weft closes that output for it.

For the `notes?: review.notes` spelling in a step's body, read [extra
inputs](syntax.md#extra-inputs).

## When you do not know the shape: `JsonDict`

`JsonDict` accepts any JSON object without saying what is in it. Use it where
something outside your program hands you an object whose shape you never wrote
down, like an API response.

A `JsonDict` connects to any `Dict[String, V]` and back again. That is the one
unchecked corner of the type system. The compiler only checks that the keys
are strings, and never looks at `V` at all, in either direction. If later
steps depend on particular fields, declare a record and convert into it.

A record can feed a `JsonDict`, which throws away everything the compiler knew
about its fields. A `JsonDict` cannot feed a record directly.

## Giving a type a name

Name a type when two values have the same shape but different meanings:

```weft
type CustomerId = String
type OrderId = String
```

Both hold strings, and a `CustomerId` still will not go into an `OrderId`
input. The compiler compares the name, not the shape.

A named value can go into its underlying shape, so a `CustomerId` feeds a
`String`. Going the other way needs a `Cast`, because a string does not become
a `CustomerId` just by looking plausible.

Names work on records and containers too:

```weft
type Profile = { name: String, age: Number }
type Profiles = List[Profile]
```

`Profile` feeds that record shape or a `JsonDict`. `Profiles` feeds its list
shape, but not `JsonDict`, which wants an object.

A `type` at the top of a file is visible everywhere in that file. Inside a
group or a loop it is visible in that body and the bodies inside it. A group's
or a loop's signature sits outside its own body, so it cannot use a type
declared inside itself.

Declarations can refer to each other in any order. Cycles and unknown names
fail. A name already visible in that scope cannot be declared again: nothing
shadows anything.

A step's metadata can declare types too, which makes them available to every
step in the project. Two packages may both declare the same metadata type as
long as the two definitions agree. If they disagree, the build fails. An
included file sees the catalog's types and its own, but not the types of the
file that included it. For how included files see each other, go and read
[files and reuse](files-and-reuse.md). For declaring types in a step's
metadata, go and read [custom types](../nodes/custom-types.md).

## Converting a value with `Cast`

Pin `Cast`'s output to the type you want:

```weft
type Profile = { name: String, age: Number }
raw = Text { value: "{\"name\":\"Mina\",\"age\":34}" }
profile = Cast -> (value: Profile) {
  value: raw.value
}
show = Debug { data: profile.value.name }
```

That parses the JSON and checks the result against `Profile`, naming whatever
fails. `Cast` also converts between things where the rules allow, such as a
numeric string into a number.

If the source type and the type you pinned can never convert, the build fails
with `cast-not-allowed`. For a pair it allows, whether it works still depends
on the value: `"34"` becomes a number and `"Mina"` does not.

Text goes to a number or a boolean. JSON text goes to a structure. Almost
anything goes to text, and numbers and booleans go both ways. Converting into
a record or a named type checks the shape you asked for. Casting a string into
a `CustomerId` does not mean that customer exists: `Cast` checks the shape,
never the world.

## When the type comes from what you connect

A step's metadata can use `T`, or `T` followed by digits, for a type the
compiler works out from what you connect. Ports on the same step using the
same variable all land on the same type. The same `T` on a different step is
unrelated. If a `T` never gets resolved at all, the build fails with
`unresolved-typevar`.

## When weft asks you for the type

`MustOverride` means the step is asking you to write the type yourself.
`Cast`'s output is one, which is why you pinned it above. Connect a port that
is still `MustOverride` and you get `must-override-unmet`. A `MustOverride`
port you never wire can stay as it is.

If you add a port from the graph, it starts as `MustOverride`, which is why
the editor asks you for a type before the build will go through.

## Channels and access

These carry a capability rather than a value:

| Type | What the receiving step gets |
|---|---|
| `Bus` | A channel shared by steps that are alive at the same time. weft checks the handle type and does not check what you send over it. |
| `Generator[T]` | A stream of `T`, with exactly one producer and one consumer. |
| `Access` | A connection it can make authorised calls with |

A generator's consumer runs once and pulls items as they turn up, and an empty
stream is fine. A loop can take one item per iteration. The stream ends when
the producer finishes or closes it. Two things it cannot do: cross an ordinary
group boundary, and be written down as a literal. The rest is in [live
channels](live-channels.md).

`Access` is the same type for every service, so an `Access` arrow passing the
compiler does not prove a Slack step received Slack access. Which service it
really is gets checked when the handle is opened at run time. For what happens
when it has not, read [connections](../connections/overview.md).

## How the compiler compares two types

- Identical types fit. A union fits when every one of its members fits
something the destination allows.
- Lists and dictionaries compare what is inside them. A destination record has
to declare every field the source might send, and receive every field it
requires.
- A named source can lose its name going into a compatible plain shape. A named
destination insists on that same name.
- Generator element types have to be compatible both ways, and a
`Generator[Number]` is a stream, so it will not feed a plain `Number`.

For the error at one particular connection, read
[diagnostics](diagnostics.md).
