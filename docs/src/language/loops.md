# Loops

A loop runs what is inside it once per item of a list, or until the body says
stop.

```weft
grade = Loop(cards: List[JsonDict], rubric: String, provider: LlmProvider) -> (scores: List[Number | Null]) {
  over: ["cards"]

  ask = Format(card: JsonDict, rubric: String) {
    template: "{{rubric}}\n\nThe card: {{card}}\n\nAnswer with one number."
    card: self.cards
    rubric: self.rubric
  }
  mark = LlmInference {
    prompt: ask.text
    provider: self.provider
  }
  self.scores = Cast { value: mark.response }.value
}
grade.cards = pull.rows
grade.rubric = "Score this flashcard from 0 to 10 for clarity."
grade.provider = model.provider
```

Unlike a group, a loop becomes nodes: the compiler turns it into a `LoopIn` and
a `LoopOut`, and the runtime drives the iterations between them.

## The five settings

They go in the body as ordinary lines. Anything else starting with a letter is
a node, and anything else that looks like a setting is a hard error, because a
typo like `maxItres: 10` silently running your loop uncapped is exactly what
the language refuses to allow.

| Setting | Default | What it does |
|---|---|---|
| `over: ["cards"]` | `[]` | Which inputs to iterate. Each has to be a `List[T]` or a `Generator[T]` |
| `carry: ["total"]` | `[]` | Values threaded from one iteration to the next |
| `parallel: true` | `false` | Launch every iteration at once, rather than each once the one before it reaches the loop's edge |
| `maxIters: 50` | no cap | A hard limit. `0` is legal and means no iterations |
| `trimOnMismatch: false` | `true` | With several `over` lists of different lengths: trim to the shortest, or fail |

## The four kinds of port

Which kind a port is depends on where it is and whether you named it.

| Kind | Outside the loop | Inside the loop |
|---|---|---|
| **Iterated**, named in `over` | `List[T]` or `Generator[T]` | One `T`, a different one each iteration |
| **Broadcast**, any other input | `T` | The same `T`, every iteration |
| **Gathered**, any output not in `carry` | `List[T \| Null]`, one slot per iteration | You write one `T` per iteration |
| **Carried**, named in `carry` | The seed goes in, the final value comes out | The running value in, the new one out |

A gathered output **has** to allow null, because an iteration that sends
nothing on it (it failed, or a branch inside it was not taken) leaves a null
in its slot. If you declare it without `| Null`, the build stops with
`gather-output-must-be-nullable`.

The list that comes out is built for every gathered port, even one that no
iteration ever wrote, because a port that produces nothing at all would leave
whatever is downstream waiting forever. A port nothing inside the loop writes
at all is refused when you build (`loop-output-unwritten`): it could only
gather nulls.

A carried value that an iteration does not write keeps what it had, as long as
nothing broke: a branch not taken or a gate that said no is fine.

A carried value needs a seed from outside, wired or written
(`myLoop.total = 0` for a loop named `myLoop`); without one, the build fails
with `required input 'myLoop.total' has no driver`. The one case it can go
without: you declare it among the loop's inputs with a `?` (`total?: Number`)
and nothing inside wires it into an input written without `?`. Then it
starts at the zero for its type: `0` for a number, `""` for a string, `false`
for a boolean, `[]` for a list, `{}` for a dict, and null for a type with no
empty value, such as a `File` or an `Image`. A seed that is wired but arrives
as null starts at the zero too, and for a type like `Number | Null` that zero
is `0`, not null.

A loop can also declare a [metadata port](groups.md#metadata-ports)
(`*tool: LlmTool`), which is none of the four: it holds one value written
in the program, never a list.

## Two implicit ports

`self.index` is a `Number`, readable inside, and it is which iteration you are
in, counting from zero.

`self.done` is a `Boolean` you can write inside, and it is how the body votes
to stop.

Both only appear if you did not declare a port of that name yourself. And
`index` as an input name, or `done` as an output name, is the
`reserved-port-name` error.

## One at a time, or all at once

Sequential is the default. The next iteration launches once this one's values
reach the loop's edge: its carried values, what it gathers, and its
`self.done`. That is what makes `carry` possible: the next iteration reads the
value the last one wrote. Nothing else holds the next iteration back, so a
loop with no outputs launches its iterations back to back. If one item's work
has to finish before the next item starts (a database write the next item
reads), carry a value out of that work into the next iteration.

A loop launches its iterations and does not own them. Work inside an
iteration that does not feed the loop's edge keeps running after the loop has
moved on, and after the loop has ended. That is what lets a loop start twenty
agents, each holding a bus, gather the buses and exit, so the nodes after the
loop can talk to every agent while they all keep running.

`parallel: true` launches every iteration up front. Three things it refuses:

- **with `carry`**, because carrying means each turn feeds the next, and that
  is sequential by definition
- **without `over`**, because there is nothing to spread out
- **with `self.done`**, because a vote to stop means nothing when everything
  already started

## How a loop ends

- It runs out of items in `over`.
- It hits `maxIters`.
- The body writes `self.done = true`, on a sequential loop.
- A carried value is lost to a failure, and then the loop fails (see below).

A sequential loop with none of those is the
`loop-unbounded-no-termination` error, refused at compile time rather than
left to run forever.

With several `over` lists of different lengths, the default trims to the
shortest. Set `trimOnMismatch: false` and a mismatch fails the run instead,
naming the lengths.

## When an iteration fails

If you only gather, the run carries on. That iteration's slot in every
gathered list is `null`, so a loop over a hundred rows where three fail gives
you a hundred slots with three nulls in them. Whatever reads that list decides
what a null means.

If the failure means a carried value never got written, the loop stops there
and fails. The next iteration would start from the same value and meet the
same failure: an agent loop whose model call broke would otherwise pay for
the same doomed call up to `maxIters` times, then hand on a stale history as
if all went well. The loop fails with

```
loop 'agent' stopped at iteration 4: its carried value 'history' could not be updated because 'ask' failed: <the error>
```

where `ask` is the node that actually broke, even when other nodes between it
and the loop's end skipped because of it. Everything reading the loop's outputs
sees a failure, the same as for any failed node, and `ask` shows as failed in
that iteration too.

A carried value that closed with no failure behind it, because a branch was not
taken or a gate said no, still keeps what it had.

## What cannot go in one

A trigger and an infra node are both refused inside a loop, with
`trigger-in-loop` and `infra-in-loop`. A trigger registers once for the
project, and infrastructure is provisioned once for the project, so a
per-iteration one is not a thing.

## Running part of its body

If you want to prove one stage of a loop's body without running the loop over
every item, run that stage on its own. Every way of cutting a run (`--group`,
`--from`, `--emit`, `--target`, `--before`, `--feed`) works inside a body,
loops inside loops included. Say the loop `clips` runs a group `edit` on each
item, reading the item and `self.index`, then `caption` and `upper` after it:

```bash
weft run --from 'clips.edit={"clip": "intro.mp4", "at": 0}' --target clips.upper
```

The part you pick runs once, as if it were the first time round: inside
iteration `0` of each loop around it, so `weft runs`, `weft events` and the
graph show its nodes at `#0`. Nothing of those loops runs: no other
iteration, and nothing reaches the loop's outputs. A loop that sits wholly
inside the part you picked runs normally, every iteration of it.

What the loop would have handed the part (the item, `self.index`, a carried
value) is a value from outside the run like any other: you hand it at the
start, as `edit`'s `clip` and `at` above, or the run is refused before it
starts, naming the input that would get nothing.

What is refused is cutting a loop in half. A loop has two ends, its
start (where its inputs go in) and its end (where its outputs come out), and
a run holds both or neither. Ending the run inside the body with nothing
started there reaches back to the loop's start for the item:

```text
the run holds loop 'clips''s start but not its end: include both to run the loop, or neither to run part of its body
```

Starting inside the body with no end there reaches the loop's outputs, which
runs the loop whole, every iteration, around a start that cannot apply to
all of them:

```text
'clips.edit' is inside loop 'clips', and the run holds both of the loop's ends, so it runs the loop
whole: a run cannot start, stand in or stop inside a loop it runs whole. To run part of the loop's
body, end the run inside the body too (--target a node in it), so the run holds neither end; to run
the loop, start or end at the loop
```

In both cases you either start and end inside the body, or run the whole
loop.

A loop in a file you include at several places is one loop at all of them,
so one run takes it the same way everywhere: whole at every place it holds
it, or part of its body at every place. A run that holds all of it at one
place and part of its body at another is refused, naming both; run them in
two runs.
