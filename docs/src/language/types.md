# Types

Every port has a type, and the compiler checks every arrow before anything
runs. Same colour on two dots is the quick version of what this page explains.

## What you can write

```text
String
Number
Boolean
Null
List[Number]
Dict[String, Number]
String | Number
{ role: String, name?: String }
{ role: String, .. }
Generator[String]
```

| Type | What it holds |
|---|---|
| `String`, `Number`, `Boolean` | The obvious |
| `Null` | Ordinary data. A port whose type allows it takes it like any value |
| `Image`, `Video`, `Audio`, `Blob` | A stored file. `Blob` is anything that is not the other three: a PDF, a zip |
| `Media` | Short for `Image \| Video \| Audio` |
| `File` | Short for those three plus `Blob` |
| `List[T]` | Any number of `T` |
| `Dict[String, V]` | Text keys, and values all of type `V`. `Dict[Number, String]` is refused, and the message suggests `List[{key: Number, value: String}]` |
| `JsonDict` | An object whose values nobody checked |
| `Json` | Any JSON value: text, a number, a boolean, null, or a list or object of those, at any depth. For a value whose shape you cannot know, like what a device answers to an arbitrary command. A stored file, a stream or a connection is never one |
| `{ a: String, b?: Number }` | A record, with `?` marking a field that may be missing |
| `{ a: String, .. }` | An open record: a record that also takes keys it does not name, and drops them |
| `A \| B` | Either one |
| `Access` | A connection to a service. Go and read [how connections work](../connections/how-they-work.md) |
| `Generator[T]` | A stream of `T`. Go and read [streams and buses](streams-and-buses.md) |
| `Bus` | A live channel. Same page |
| `Secret[T]` | A `T` the run's record never holds. See [secrets](#secrets) below |
| `Empty` | Nothing can be one. See below |
| `T`, or `T` and a number (`T1`, `T2`) | A type variable. The node works with whatever you connect |

A stored file on a wire is never the bytes. It is a small marker saying where
the file is, which is why a `String` holding a URL is not an `Image`.

## What fits into what

| From | Into | Fits? |
|---|---|---|
| `String` | `String` | Yes, and the same for every other primitive |
| `List[A]` | `List[B]` | If `A` fits `B` |
| `List[T]` | `T` | **No.** To go through a list one at a time, put the consumer in a `Loop` |
| `Record` | `JsonDict` | Yes. A record forgets itself |
| `JsonDict` | `Record` | **No.** Use a `Cast`, which checks the shape |
| `Dict[String, V]` | `JsonDict` | Yes, and back |
| Anything made of JSON (text, numbers, booleans, null, `JsonDict`, and lists, dicts and records of those) | `Json` | Yes, at any depth: `List[List[Number]]` fits |
| A file, `Bus`, `Access`, `Infra`, `Generator[T]`, or a type holding one | `Json` | **No** |
| `Json` | anything but `Json` | **No.** Use a `Cast`, which checks the value |
| `Record` | `Record` | Every field you have must be declared on the target (unless the target ends with `..`), every required field it wants must be required on yours, and the types must match |
| `A` | `A \| B` | Yes |
| `A \| B` | `A` | Only if **both** `A` and `B` fit `A` |
| `Generator[A]` | `Generator[B]` | Only if they fit both ways. A stream is invariant |
| `Secret[A]` | `Secret[B]` | If `A` fits `B` |
| `Secret[T]` | `T`, `Json`, or a `JsonDict` | **No.** Wire it to a node whose port takes a `Secret`; that node reveals it |
| A record or a `Dict` holding a `Secret` | `JsonDict` | **No.** A `JsonDict` would carry the secret on to a port that reads it plain |
| `T` | `Secret[T]` | **No.** Only a node makes a secret |
| `Empty` | anything | Yes |
| anything | `Empty` | No |

Records are strict. A field the target does not declare is a mismatch, not
something quietly carried along. If you want a record that takes keys it does
not name, end it with `..`: `{ email: String, .. }` takes any record with an
`email`. At run time, a value checked against it (a request's body, a model's answer,
what a `Cast` makes) loses every key the type does not name, so what flows on
carries only `email`. A missing required field is refused either way.

A field that can be null says so: `extra: JsonDict | Null`. A `JsonDict` alone
refuses a null, so if you declare a query's rows as
`List[{ id: String, extra: JsonDict }]` and a row comes back with `extra`
null, the node fails with `... does not accept: [0].extra: expected JsonDict,
got Null`, and the fix is the `| Null` on that field.

`Empty` is the type of a value that cannot exist. An empty list `[]` is
`List[Empty]`, because there is no item to learn from, and that goes into a
`List[String]` or a `List[Number]` without complaint. Inside a union it
disappears: `Number | Empty` is just `Number`.

A port typed with a type variable (`T`, `T2`) gets its type where the node is
used: from the type your program declares on it, from what is wired to it, or
from a value written into it. `Cast`'s output is the type variable `T2` (its
input is `T`), so it takes the type you declare on it (`-> (value: Boolean)`),
or else the type of the port it feeds. A port you declare in a header with no
type, like `(x)`, gets a type variable of its own, so for a group
`g = Group(x) -> (y: String)`, writing `g.x = 3` makes `x` a `Number`. A type variable nothing pins is the `unresolved-typevar` error.

A type variable can be limited to some types. A group says so right after
the word `Group`, `blur = Group[T: Media](file: T) -> (out: T)`, and so can a
node you declare inline (`ExecPython[T: Media](f: T)`). A node's author does
it in its `metadata.json` (`KeepFile` only takes a file); for how, go and read
[typeParams](../nodes/metadata.md#typeparams). Wire a `String` into that
group's `file` and the build stops with `type-bound`: `` `T` has to fit in
Media, and what is connected makes it String``.

## Saying what a broad output holds

Some outputs carry a shape only your program knows, like the JSON a model
answers with or the body of a request. Their node types them broadly, usually
`JsonDict`, and lets you declare what they hold on the node, like this:

```weft
type Verdict = { label: String, score: Number }
judge = LlmInferenceJson -> (response: Verdict) {
  prompt: "Label this review and score it from 0 to 1"
  provider: model.provider
}
```

From there on the wire carries a `Verdict`, so `judge.response.score` reads
as a `Number`. At run time every value is checked against `Verdict`, and one
that does not fit fails the node, which `_error` catches when it is wired.

You can only do this on an output whose node allows it, and only with a type
that fits inside the widest one the node accepts there. Declaring
`-> (response: Number)` on `LlmInference`, whose answer is always text, is
refused when you build: `... output port 'response' is declared Number, but
LlmInference always gives it String: only an output its node marks narrowable
takes the type the program declares`.

## Giving a type a name

```weft
type CustomerId = String
type Ticket = { id: CustomerId, body: String, priority?: Number }
```

A declaration goes at the top of a scope, meaning the file, or a group's or a
loop's body. Not inside a node's braces. Order does not matter. A name is
visible in its scope and every scope inside it, included files too: for how a
type reaches into an `@include`, go and read
[files and reuse](files-and-reuse.md#types-reach-down-into-an-included-file).

**Names are compared as names.** `CustomerId` and `String` are different types
even though one is the other underneath. That is the point: a customer id
cannot be wired into a port that wanted any old string by accident.

A name flows **into** its underlying shape for free, because the name just
falls away. Going the other direction needs a `Cast`, which checks the value
really has that shape.

**Nothing shadows.** A name already visible cannot be redeclared, even to the
same thing. Two catalog packages shipping an identical type is fine (for two that
disagree, go and read [custom types](../nodes/custom-types.md)). And a port that restates a declared name with a different body is the
`named-type-conflict` error, because a named type has one body everywhere.

## Reading a key off a wire

```weft
speed.wpm = reader.profile.stats.wpm
```

The dotted path goes on the **source** side only. A wire lands on a port, never
on a key inside one.

Each step of the path has to name a field of a record. A `JsonDict` has no
declared fields, so reading a key off one is the `deref-path` error telling you
to declare the type on the source port, or `Cast` it first.

The interesting case is an optional field. If you read `?`-marked key and it is
missing, or it is `null`, **the wire closes**. It does not deliver `null`. So
the node you wired it into applies its own rules: it skips, unless you declared
that input with `?` (`x?: T`), and then it runs without it. For what a closure
does, go and read
[how a program runs](how-a-program-runs.md#how-a-branch-stops-the-nodes-after-it).

A required key that is missing is an error naming the key, never a quiet null.

## Cast

`Cast` is the node for converting a value and checking the result. You declare
the type you want on its output, and the compiler tells you whether that
conversion exists at all.

| From | Into | What happens |
|---|---|---|
| `String` | `Number`, `Boolean` | Parsed |
| `String` | `List`, `Dict`, `JsonDict`, a record, a named type | Parsed as JSON, then held to the target's shape |
| Anything not a file, a live handle or a secret | `String` | Turned into text: a list or an object becomes its JSON |
| `Json` | Anything made of JSON | Checked at run time: a `Json` holding a list casts into `List[Number]` when every item is a number |
| `Number` | `Boolean` | 1 and 0 |
| Any object shape | A record or a named type | Checked against the declared structure at run time |
| Any object shape | A file type | The marker is checked, including that its kind matches |
| `List[A]` | `List[B]` | If `A` casts to `B`. Same for `Dict` values |

If an ordinary arrow is refused and a cast would work, the `type-mismatch`
message says so and tells you to wire it through a `Cast`.

At run time, a value that does not convert fails the node naming the field
that was wrong: `cannot cast the value into Verdict: not a valid Verdict:
field "score": "high" is not a number`. If you want to handle that instead of
failing the run, wire the `Cast`'s `_error`.

## Secrets

`Secret[T]` is a `T` that flows through a run like any value while the run's
record holds only a marker naming it. A caller's `Authorization` header on a
`Route` or a `Socket` comes out this way, on `credentials`, and so does a token
`MintInstanceToken` makes. The record, the inspector, `weft events` and search
show the marker. The value is kept sealed beside the record, and goes when the
record goes.

A secret turns back into its value only inside a node that asks for it, and
only where the type of what you wired in says a secret is. Some of weft's own
do it for you: `HttpRequest` sends a secret in its headers or its body
revealed, `Reply` and `Stream` send one to their caller revealed, and
`ExecPython` hands its script the value of an input you declared as a secret.

If a secret reaches one of them somewhere its input's type does not declare
one (inside a `JsonDict`, or in a `Stream`'s bus message, which carries no
type), the node fails and names the input and where the secret is, rather
than sending the marker. Declare the type, like an object's fields, and it
goes out revealed. If you want an `HttpRequest` body to carry a token, declare
the object you build for it:

```weft
mint = MintInstanceToken { instance: "user-42", expiresInHours: 24 }
handoff = JsonObject -> (object: { instance: String, token: Secret[String] }) {
  instance: "user-42"
  token: mint.token
}
notify = HttpRequest {
  url: "https://partner.example/tokens"
  method: "POST"
  body: handoff.object
}
```

Left as `JsonObject { token: mint.token }`, the same body builds, and the
`HttpRequest` fails when it runs, naming `body` and `/token`.

A body with no secret in it needs no declared type: a plain object, written
or wired, goes out as it always did. For writing a node that makes or
reveals one, go and read [the ctx](../nodes/ctx.md#secrets).

Only a node makes a secret, so nothing written in a `.weft` file can be one.
If you need to make one without writing a node, declare an `ExecPython`
output as `Secret[String]`, and what the script returns there becomes a
secret. The build refuses a value written on a `Secret` port, a secret's
marker written anywhere (`secret-literal`: it would name a secret of another
run), a `Cast` into or out of a secret, and an `@instance_filled` secret
(`instance-filled-secret`). A wire cannot read a key inside one either: if
node `s` has an output `session` typed `Secret[{ token: String }]`,
`s.session.token` is the `deref-path` error, since what flows is the marker.

## What an unwired port starts as

A port nobody wired and nobody wrote a value into has a zero value, which is
what seeds a loop's carry when you did not give it a starting value.

| Type | Starts as |
|---|---|
| `Number` | `0` |
| `String` | `""` |
| `Boolean` | `false` |
| `List[T]` | `[]` |
| `Dict`, `JsonDict` | `{}` |
| A union | The zero of its first variant that is not `Null` |
| Everything else | `null` |
