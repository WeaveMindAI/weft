# Groups

Put the steps that do one job in a group. The graph can then show that job as
a single box, with inputs and outputs you can look at before opening it.

```weft
message = Text { value: "  hello  " }

clean_text = Group(raw: String) -> (result: String) {
  # Remove whitespace around the message
  trim = ExecPython(text: String) -> (out: String) {
    text: self.raw
    code: "return {'out': text.strip()}"
  }
  self.result = trim.out
}

clean_text.raw = message.value
show = Debug { data: clean_text.result }
```

From outside, `clean_text` takes a string and gives back a string. Inside,
`trim` does the work. You can add more steps in there later without the
outside changing at all.

## The interface

The signature says what the rest of the graph can see:

```weft
clean_text = Group(raw: String) -> (result: String) {
  ...
}
```

Inside the body, `self.raw` is the group's input. `self.result = trim.out`
sends a child's output back out, right to left like any other connection.

Children can wire to each other and to `self`. They cannot reach straight into
another group's insides, or past `self` to anything outside. The compiler
checks that. If a child needs something from outside, expose it as a port so
the connection is visible from out there.

Set a group's inputs on their own lines, from an arrow or a written value:

```weft
clean_text.raw = "  hello  "
```

That value reaches every child wired to `self.raw`. The braces hold the
children, so ordinary input values do not go in there.

## Names and nesting

Groups can hold groups. Each body has its own scope, so two groups can both
have a child called `trim` without arguing.

Name a group after the job the rest of the graph wants done. A support program
might have `understand_request`, `find_answer` and `review_reply`. Somebody
reading the top level can see what `find_answer` gave back without first
reading how the search works.

## The readable size

A level of the graph is what somebody takes in at one look: the file itself,
or the inside of one group. Keep about six items on it. Past fifteen you get a
`level-too-large` warning. It is only a warning: the program still builds and
runs.

When a level grows, nest instead of spreading out. Find the steps cooperating
on one job, put them in a group, and the level is back to one item where it
had six. Groups nest, so a group holding nothing but two more groups is a
perfectly normal thing to have.

## Which children start on their own

When a group starts, weft also starts any child with nothing wired into it. So
a group can contain a `Text` or anything else that needs no upstream value.
Children that do have arrows follow the usual [readiness
rules](mental-model.md).

A child belongs to its group: it runs when the group runs, and stops when the
group is skipped. Inside a loop, the same group and its children run once per
iteration.

A group is not a function call. It runs once, in place, just as the steps
inside it would have. If you want repeated work, go and read
[Loops](loops.md).

## Turning a whole group off

`_should_flow` decides whether any work inside a group starts at all:

```weft
escalation = Group(question: String) -> (answer: String) {
  _should_flow: route.needs_a_person
  ...
}
```

You can write the same gate from outside as `escalation._should_flow =
route.needs_a_person`, or just give it true or false.

A false or closed gate skips every child, including nested groups, and closes
the group's outputs. The inspector records the skipped scope as the reason.
Downstream, those closures behave like any other closure, so a step with
another input to fall back on can still run.

If one of the group's ordinary inputs closes, the group does not shut down. If
`question` closes, that closure reaches the children wired to `self.question`,
and a child that requires it skips while another child may carry on. The
boundary does not insist on all its inputs before it lets any child work. If
you want all or nothing, gate it with `_should_flow`.

`@require_one_of` goes on a child, never on the group, and the compiler says
so if you try. For what the directive does, go and read [needing one of
several inputs](syntax.md#needing-one-of-several-inputs).

## The description line

A plain comment at the very start of a body becomes the group's description,
with only whitespace allowed before it. The graph shows that text when the
group is folded shut.

In the example above it is "Remove whitespace around the message". Describe
the result the caller wants, and leave the details to the steps inside.

## Finding the first wrong value

Say a support reply quotes the wrong price. Look at the outputs of
`find_answer` and `review_reply`. If the price is already wrong coming out of
`find_answer`, open that group and look at its children. If it was right
there, the damage happened later.

Keep going into nested groups until you reach the step that introduced it. The
boundaries give you intermediate values to check, even in a program you did
not write. For the editor controls, read [reading and building the
graph](../start/reading-the-graph.md).

## What actually runs

Groups do not exist at run time. For what the compiler turns them into, go and
read [How a weft program runs](mental-model.md).

## Reusing a group from another file

A file you want to reuse is written as one group with no name wrapped around
the whole thing. Pull it in under a local name:

```weft
clean_text = @include("clean-text.weft")
clean_text.raw = message.value
```

Its ports become `clean_text`'s ports. For the file format and the path rules,
read [files and reuse](files-and-reuse.md).
