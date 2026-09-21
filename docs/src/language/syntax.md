# Syntax

A `.weft` file names some steps and connects them up. This program puts `HELLO`
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
`Text`, `ExecPython` and `Debug` are kinds of step, from the project's `nodes/`
folder.

## Naming a step

`name = NodeType`, and braces if you have anything to put inside:

```weft
message = Text { value: "hello" }
```

A step with nothing to configure can drop the braces, so `Debug` and `Debug {}`
mean the same. You still have to supply whatever that step requires.

A name the catalog does not know is `unknown-type`. Writing a name does not
bring a step into existence; the declaration has to be there.

## Connecting things

Write `source.output` where a value would go:

```weft
show = Debug { data: message.value }
```

Or put it on its own line, which is the same connection:

```weft
show = Debug
show.data = message.value
```

Values travel right to left, into the input on the left, and the type has to
fit.

To take one field out of a value, keep going with dots:

```weft
speed.wpm = reader.profile.stats.wpm
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
to work out the step's shape, such as a form's field list, has to be written
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

You can write a value into an **input**. **Outputs** are filled by the step
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
by output name.

You can only declare the ports a step leaves open for you. Ports whose types
are already fixed keep them. Declare inputs, outputs, or both:

```weft
answer = LlmInference -> (response: String)
convert = Cast -> (value: Boolean)
```

Those are fragments: the steps still need their other required inputs before
they can run.

### Extra inputs

Some steps, `ExecPython` and `FirstInOrder` among them, let you invent input
names. Writing a key in the body creates that input and works out its type from
what you put there:

```weft
step = ExecPython -> (out: String) {
  text: draft.answer
  limit: 3
  notes?: review.notes
  code: @file("assets/scripts/step.py")
}
```

`limit` becomes a `Number`. `text` takes whatever type `draft.answer` has. A
bare `null` gives it nothing to work from, so that fails.

The `?` on `notes?` makes it optional, so a closed arrow there does not skip
the step. That spelling only works when you are creating the port; to make a
port optional in a full signature, write `notes?: String` there instead.

Extra inputs stay in the order you wrote them, which matters for
`FirstInOrder`, since it takes the first one that supplied a value.

## The four underscore keys

| Key | What it does |
|---|---|
| `_label: "Review the draft"` | Puts a label on the box. A string literal only; it cannot come over a wire |
| `_tags: ["support"]` | Tags the step, including for deciding which runs a stop reaches |
| `_should_flow: decision.approved` | Decides whether the step runs. Takes true, false, or an arrow |
| `_should_not_flow: decision.sent` | The same decision inverted: the step runs when the thing wired here did NOT happen |

<!-- SYNC: reserved keys <-> crates/weft-core/src/exec/skip.rs SHOULD_FLOW_PORT
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

A step has one gate, and wiring both spellings is the `two-gates` error. For
what each one does to the steps after it, go and read
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
skips a group it skips everything in it.

Loops work the same way with iteration settings of their own. Go and read
[groups](groups.md) and [loops](loops.md).

## Declaring a step where you use it

You can write a step straight into the place its value is wanted, ending with
`.port` to say which output you mean:

```weft
show = Debug {
  data: Text { value: "hello" }.value
}
```

That makes the step and the arrow together, exactly as a named declaration
would. The `.port` on the end is required even when there is only one output.

## Needing one of several inputs

`@require_one_of` says a step needs at least one out of a list:

```weft
lookup = SlackFindUser {
  @require_one_of(email, id)
  account: slack.access
  email: "person@example.com"
}
```

Every name in there has to be a real input. Supply none of them and the
compiler says `require-one-of-unmet`. At run time, the step skips if they all
close without a value.

It goes in a step's body or its inline signature. A step's author can bake the
same requirement into its metadata as `oneOfRequired`. Groups and loops reject
it: put it on the child that needs the value.

## Comments

`#` starts a comment. A plain comment as the first thing inside a group or loop
body becomes its description, which is what the graph shows when it is folded
shut, as in the `shout` example above. A comment anywhere else is just a
comment.

The parser keeps your whitespace and comments, so the editor can change one
field without reformatting everything around it. There is no enforced
formatting style.
