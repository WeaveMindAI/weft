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
| `Dict[K, V]` | Keys and values, all the same type |
| `JsonDict` | An object whose values nobody checked |
| `{ a: String, b?: Number }` | A record, with `?` marking a field that may be missing |
| `A \| B` | Either one |
| `Access` | A connection to a service. Go and read [how connections work](../connections/how-they-work.md) |
| `Generator[T]` | A stream of `T`. Go and read [streams and buses](streams-and-buses.md) |
| `Bus` | A live channel. Same page |
| `Empty` | Nothing can be one. See below |
| `MustOverride` | Nobody has decided yet, and the build stops until you do |
| `T`, `T1` to `T99` | A type variable. The node works with whatever you connect |

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
| `Record` | `Record` | Every field you have must be declared on the target, every required field it wants must be required on yours, and the types must match |
| `A` | `A \| B` | Yes |
| `A \| B` | `A` | Only if **both** `A` and `B` fit `A` |
| `Generator[A]` | `Generator[B]` | Only if they fit both ways. A stream is invariant |
| `Empty` | anything | Yes |
| anything | `Empty` | No |

Records are strict. A field the target does not declare is a mismatch, not
something quietly carried along. And a field that can be null says so: `extra:
JsonDict | Null`. A `JsonDict` alone refuses a null, so a query whose rows all
have that column empty fails the node with `does not accept (got
List[Dict[String, String | Null]])`, and the fix is the `| Null` on that field.

`Empty` is the type of a value that cannot exist. An empty list `[]` is
`List[Empty]`, because there is no item to learn from, and that goes into a
`List[String]` or a `List[Number]` without complaint. Inside a union it
disappears: `Number | Empty` is just `Number`.

When a type is still `MustOverride`, or still a bare type variable, weft lets
the arrow through and tells you to decide. That is the `must-override-unmet`
and `unresolved-typevar` pair.

## Giving a type a name

```weft
type CustomerId = String
type Ticket = { id: CustomerId, body: String, priority?: Number }
```

A declaration goes at the top of a scope, meaning the file, or a group's or a
loop's body. Not inside a node's braces. Order does not matter.

**Names are compared as names.** `CustomerId` and `String` are different types
even though one is the other underneath. That is the point: a customer id
cannot be wired into a port that wanted any old string by accident.

A name flows **into** its underlying shape for free, because the name just
falls away. Going the other direction needs a `Cast`, which checks the value
really has that shape.

**Nothing shadows.** A name already visible cannot be redeclared, even to the
same thing. Two catalog packages shipping an identical type is fine and they
absorb; two packages shipping the same name with different bodies names both.
And a port that restates a declared name with a different body is the
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
the step you wired it into applies its own rules: a required input skips, an
optional one lets the others decide. For what a closure does, go and read
[how a program runs](how-a-program-runs.md#how-a-branch-stops-the-steps-after-it).

A required key that is missing is an error naming the key, never a quiet null.

## Cast

`Cast` is the node for converting a value and checking the result. You declare
the type you want on its output, and the compiler tells you whether that
conversion exists at all.

| From | Into | What happens |
|---|---|---|
| `String` | `Number`, `Boolean` | Parsed |
| `String` | `List`, `Dict`, `JsonDict`, a record, a named type | Parsed as JSON, then held to the target's shape |
| Anything not a file or a live handle | `String` | Turned into text |
| `Number` | `Boolean` | 1 and 0 |
| Any object shape | A record or a named type | Checked against the declared structure at run time |
| Any object shape | A file type | The marker is checked, including that its kind matches |
| `List[A]` | `List[B]` | If `A` casts to `B`. Same for `Dict` values |

If an ordinary arrow is refused and a cast would work, the `type-mismatch`
message says so and tells you to wire it through a `Cast`.

At run time the check names the exact path that was wrong, like
`messages[2].role: expected String, got Number`, rather than failing with a
shrug.

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
