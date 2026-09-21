# Custom types

Give a shape a name and every node can use it.

```json
"types": {
  "ChatMessage": "{ role: String, content: String }",
  "ChatHistory": "List[ChatMessage]"
}
```

Then in any node's ports, including other people's:

```json
"inputs": [ { "name": "history", "type": "ChatHistory", "required": true } ]
```

## They are global, and compared by name

Every `metadata.json` in the project contributes its `types` to one table
before anything else is parsed, so a name declared by one package is usable by
every node.

The comparison is by **name**, not shape. `CustomerId` declared as `String` is
a different type from `String`, and wiring one into the other is a mismatch.
That is the point: a customer id should not land in a port that wanted any old
string.

A named type flows **into** its shape for free, because the name falls away
when it is not needed. Going the other way, from a plain shape to a named one,
needs a `Cast`, which checks the value really has that shape.

## What travels

The name carries its shape with it. A port typed `ChatMessage` stores as
`ChatMessage={ role: String, content: String }`, so anything reading a saved
project or a journal row understands it with no table to consult, and a run
that started yesterday keeps the shape it started with even if you edit the
declaration today.

## Where to put them

A package root's `metadata.json` can declare the types its nodes share, once,
and every node in it inherits them.

Two packages declaring the same name with the same shape is fine and absorbs.
Two declaring it with different shapes names both, and a port that restates a
declared name with a different shape is the `named-type-conflict` error. A
named type has one body everywhere.

Nothing shadows. A name already visible cannot be redeclared, not even to the
same thing.

## Cast

Any node can declare itself a cast:

```json
"features": { "castPorts": { "input": "value", "output": "value" } }
```

The compiler reads the declaration, never the node's name, so this is a thing
any node can be rather than one built-in.

At compile time, the resolved pair of types has to be a conversion that exists:

| From | Into |
|---|---|
| `String` | A number, a boolean, or any object shape, parsed as JSON and then checked |
| Anything that is not a file or a live handle | `String` |
| `Number` | `Boolean`, and back |
| Any object shape | A record or a named type, checked against the declared shape |

Anything else is refused at compile time:

```text
no cast from JsonDict into Number; a cast parses text, stringifies data, or
validates an object shape against a declared type
```

At run time, the check names the field that was wrong, like
`messages[2].role: expected String, got Number`, rather than failing with a
shrug.

Stored files, buses and connections do not turn into text, so those casts do
not exist.
