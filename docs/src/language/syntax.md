# Syntax

A `.weft` file names some nodes and connects them up. This program puts `HELLO`
in front of you:

```weft
message = Text { value: "hello" }
shout = ExecPython(text: String) -> (text: String) {
  text: message.value
  code: "return {'text': text.upper()}"
}
show = Debug { data: shout.text }
```

`message`, `shout` and `show` are names you choose, unique where they sit.
`Text`, `ExecPython` and `Debug` are node types, from the project's `nodes/`
folder.

## Naming a node

`name = NodeType`, and braces if you have anything to put inside:

```weft
message = Text { value: "hello" }
```

A node with nothing to configure can drop the braces, so `Debug` and `Debug {}`
mean the same. You still have to supply whatever that node requires.

A name the catalog does not know is `unknown-type`. Writing a name does not
bring a node into existence; the declaration has to be there.

## Connecting things

Write `source.output` where a value would go:

```weft
show = Debug { data: message.value }
```

Or put it on its own line, which is the same connection:

```weft
show.data = message.value
```

Values travel right to left, into the input on the left, and the type has to
fit. Write the wire in the braces whenever you can. The line of its own is for
the ports a group, a loop or an `@include` declares, which you set from
outside because their braces hold something else or there are none.

To take one field out of a value, keep going with dots:

```weft
show = Debug { data: reader.profile.stats.wpm }
```

That reads `stats.wpm` off `reader.profile`. For the type rules, go and read
[types](types.md#reading-a-key-off-a-wire).

## Writing values in

Inputs take strings, numbers, `true`, `false`, `null`, lists and objects, as
long as the type allows it. Lists and objects can spread over several lines,
and commas between fields are optional.

```weft
message = Text {
  value: "hello"
}
```

Some inputs only take arrows and some only take written values; the
`input-accepts` diagnostic tells you which. Anything the compiler has to read
to work out the node's shape, such as a form's field list, has to be written
inline: an arrow, `@file` or `@asset` cannot supply it.

For text kept in another file, go and read
[files and reuse](files-and-reuse.md).

Triple backticks hold code, or anything with newlines in it:

````weft
answer = ExecPython -> (value: Number) {
  code: ```
    return {'value': 42}
  ```
}
````

You can write a value into an **input**. **Outputs** are filled by the node
when it runs, so `answer.value = 42` in that example is the `value-on-output`
error. The same holds inside a group: connect a child's output to
`self.result`, and if you want a constant to come out, use something like
`Text` to produce it.

## Declaring ports

Inputs in brackets, outputs after `->`:

```weft
calc = ExecPython(a: Number, b: Number) -> (sum: Number) {
  a: 7
  b: 2
  code: "return {'sum': a + b}"
}
```

`ExecPython` hands your code `a` and `b` and expects a dictionary back, keyed
by output name. A key left out sends nothing on that output, and `return {}`
sends nothing at all. A key set to `None` sends a real null on an output whose
type takes `Null` (`last: JsonDict | Null`), and nothing on any other output.

If you want the program to carry on when the script raises (a request it
makes fails, or it raises on purpose), wire the node's `_error` output: the
exception's type, message and traceback come out there, and the other outputs
send nothing. Left unwired, the exception fails the run. A mistake in the
program fails the run either way: code that does not compile, a script that
ends without returning a dictionary, a key that is not one of its outputs, or a
value of the wrong type for its output. `_error` counts as a key it may not
return, since weft fills it; to fail on purpose, raise. `_error` is not only
for scripts: every node has one. For which failures reach it, go and read
[when a node fails](how-a-program-runs.md#when-a-node-fails).

You can declare any of the node's own inputs, with its type or a narrower
one, and add new inputs on a node that takes them. An output you can declare only where the node leaves it open: one it lets
you add, one typed with a type variable (a stand-in like `T` that takes its
type where the node is used), or one it lets you narrow. Declaring an output
looks like this:

```weft
answer = LlmInferenceJson -> (response: { label: String })
convert = Cast -> (value: Boolean)
```

Those are fragments: the nodes still need their other required inputs before
they can run. For narrowing an output like `response`, go and read [saying
what a broad output holds](types.md#saying-what-a-broad-output-holds).

A group or a loop can also list a metadata port among its outputs, with a `*`
before the name: `-> (results: String, *tool: LlmTool)`. Its value is written
in the program instead of made by a run. For what that is for, go and read
[metadata ports](groups.md#metadata-ports).

### Optional and required inputs

A node's metadata says which of its inputs it can run without. Where you use
the node, you can declare the input yourself: without `?` it is required, and
with `?` the node runs without it. If the metadata requires that input, the `?`
is a compile error.

| What the node's metadata says | What the program writes | Result |
|---|---|---|
| required | nothing | always required |
| required | `x?: T` | compile error naming the port: the node's code needs it |
| optional | nothing | unwired: runs without it. Wired: waits for it, and skips if the wire closes |
| optional | `x: T` (no `?`) | always required |
| optional | `x?: T` | runs without it even when wired and the wire closed, whether nothing arrived or the source failed |

A wire closes when its source finished without sending on that port, or
failed. A node that skips closes its own outputs in turn. A `?` lets a node run
without a failed source's value, but the failure still fails the run unless
that source's `_error` is wired (go and read [when a node
fails](how-a-program-runs.md#when-a-node-fails)).

One exception to the table: an input in one of the node's `oneOfRequired`
sets (or in a `@require_one_of` you wrote on it) is decided by its set. The
node skips only when every member of the set closed, unless you declared that
input without `?`.

If you want the build to refuse a node until something fills an input,
declare that input without `?`. `SlackSendMessage` takes `text` or `blocks`, so neither is
required on its own, but this message must have text:

```weft
send = SlackSendMessage(text: String) {
  account: slack.access
  channel: "#support"
  text: draft.answer
}
```

Leave `text` out and `weft validate` refuses `send`: `required input
'send.text' has no driver (no edge, no body value, no default)`.

If you want a node to run when a wire into it brings nothing, write the `?`.
Here `ask` is a `SlackReceiveMessage`, whose `thread` sends nothing when the
message was not in a thread. With `threadTs?`, such a message gets its answer
at the top of the channel. Without the `?`, `answer` waits for `threadTs` and
skips:

```weft
answer = SlackSendMessage(threadTs?: String) {
  account: slack.access
  channel: "#support"
  text: draft.answer
  threadTs: ask.thread
}
```

### Extra inputs

Some nodes, `ExecPython` and `FirstInOrder` among them, let you invent input
names. Writing a key in the body creates that input and works out its type from
what you put there:

```weft
summary = ExecPython -> (out: String) {
  text: draft.answer
  limit: 3
  notes?: review.notes
  code: @file("assets/scripts/summary.py")
}
```

`limit` becomes a `Number`. `text` takes whatever type `draft.answer` has. A
bare `null` gives it nothing to work from, so that fails.

The `?` on `notes?` works like `x?: T` in the table above: the node runs
without `notes` even when its wire closes. Without the `?`, a port you create
this way is required. That spelling only works when you are creating the port;
for a port the node already has, write the `?` in the signature, like
`(threadTs?: String)`.

Extra inputs stay in the order you wrote them, which matters for
`FirstInOrder`, since it takes the first one that supplied a value.

## The four underscore keys

| Key | What it does |
|---|---|
| `_label: "Review the draft"` | Puts a label on the box. A string literal only; it cannot come over a wire |
| `_tags: ["support"]` | Tags the node, including for deciding which runs a stop reaches |
| `_should_flow: decision.approved` | Decides whether the node runs. Takes true, false, or an arrow |
| `_should_not_flow: decision.sent` | The same decision inverted: the node runs when the thing wired here did NOT happen |

<!-- SYNC: reserved keys <-> crates/weft-core/src/run/skip.rs SHOULD_FLOW_PORT
     and SHOULD_NOT_FLOW_PORT, packages/weft-graph/src/protocol.ts
     SHOULD_FLOW_PORT and SHOULD_NOT_FLOW_PORT,
     packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README) -->

Any other key starting with `_` is an error.

```weft
send = SlackSendMessage {
  _should_flow: review.approved
  channel: "#support"
  text: draft.answer
}
```

A node has one gate, and wiring both spellings is the `two-gates` error. For
what each one does to the nodes after it, go and read
[how a program runs](how-a-program-runs.md#choosing-a-branch-and-joining-back-up).

## Groups and loops

A group's braces hold its children, and its signature says what the outside can
see:

```weft
shout = Group(text: String) -> (result: String) {
  # Uppercase the message
  convert = ExecPython(text: String) -> (text: String) {
    text: self.text
    code: "return {'text': text.upper()}"
  }
  self.result = convert.text
}
shout.text = "hello"
```

Set a group's inputs from outside, as `shout.text` does. `self` means this
group's own interface. `_should_flow` can go inside the braces too, and when it
skips a group it skips everything in it. The only literal you can write on
`self` is a metadata port's value: `self.tool = { "name": "search" }`.

Loops work the same way with iteration settings of their own. Go and read
[groups](groups.md) and [loops](loops.md).

## Declaring a node where you use it

You can write a node straight into the place its value is wanted, ending with
`.port` to say which output you mean:

```weft
show = Debug {
  data: Text { value: "hello" }.value
}
```

That makes the node and the arrow together, exactly as a named declaration
would. The `.port` on the end is required even when there is only one output.

## Needing one of several inputs

`@require_one_of` says a node needs at least one out of a list:

```weft
lookup = SlackFindUser {
  @require_one_of(email, id)
  account: slack.access
  email: "person@example.com"
}
```

Every name in there has to be a real input. Supply none of them and the
compiler says `require-one-of-unmet`. At run time, the node skips if they all
close without a value.

It goes in a node's body or its inline signature. A node's author can bake the
same requirement into its metadata as `oneOfRequired`. Groups and loops reject
it: put it on the child that needs the value.

## One copy per instance, one value per instance

`@per_instance` on its own line in an infra node's body gives each instance of
the program its own copy of that node (its own container):

```weft
bridge = BaileyBridge {
  @per_instance
}
```

It takes no arguments.

`@instance_filled` goes where a field's value would, and makes the value each
instance's own: `@instance_filled` alone, or `@instance_filled(<value>)` with
the value an instance that was given none gets. A connection is a value too:

```weft
google = GoogleAccess { account: @instance_filled }
digest = Cron { cron: @instance_filled("0 0 8 * * *") }
```

The fallback can also be a file, read the way a written one is:
`personality: @instance_filled(@file("prompts/default.md"))`.

For what both do, go and read [programs with instances](../running/instances.md).

## Spelling a name

Every name you choose in a program is camelCase: what you call a node, a
group, a loop or an `@include` (`sendReply`), and every port you declare on a
node, a group or a loop, or in an included file's header
(`maxRetries: Number`). A key you write in a node's body that creates a port
counts too (`userId: 3` on `ExecPython`). A node type is spelled exactly as
the catalog spells it, which is PascalCase (`ExecPython`).

If you spell a name you chose any other way, the program still runs, and the
compiler warns `not-camel-case`. When it can respell the name, the message says how:

```text
node 'send_reply' is not camelCase: write 'sendReply'
```

The gates, `_should_flow` and `_should_not_flow`, are the language's own names,
so this warning skips them.

If the ports you add are named after data from outside weft, you can keep that
data's own spelling on a node built for it: `body = JsonObject { user_id:
ask.id }` draws no `not-camel-case` warning for `user_id`, though the name you
give the node, `body`, is still checked. A few nodes that ship with weft let you do this (`Route` for its path's captures, `JsonObject` for the keys
of the object it builds, among others), because each one sets
`addedPortsAreDataKeys` in its `metadata.json`. For the full list, go and read
[the `addedPortsAreDataKeys` row in the features table](../nodes/metadata.md#features).

## Comments

`#` starts a comment. A plain comment as the first thing inside a group or loop
body becomes its description, which is what the graph shows when it is folded
shut, as in the `shout` example above. A comment anywhere else is just a
comment.

The parser keeps your whitespace and comments, so the editor can change one
field without reformatting everything around it. There is no enforced
formatting style.
