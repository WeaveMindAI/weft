# Loops

A loop runs what is inside it once per item of a list, or until the body says
stop.

```weft
grade = Loop(cards: List[JsonDict], rubric: String) -> (scores: List[Number | Null]) {
  over: ["cards"]

  mark = LlmInference -> (response: String) {
    prompt: self.rubric
    ...
  }
  self.scores = Cast { value: mark.response }.value
}
grade.cards = pull.rows
```

Like a group, a loop is gone before anything runs. The compiler turns it into a
pair of boundary nodes and the runtime drives the iterations.

## The five settings

They go in the body as ordinary lines. Anything else starting with a letter is
a node, and anything else that looks like a setting is a hard error, because a
typo like `max_itres: 10` silently running your loop uncapped is exactly what
the language refuses to allow.

| Setting | Default | What it does |
|---|---|---|
| `over: ["cards"]` | `[]` | Which inputs to iterate. Each has to be a `List[T]` or a `Generator[T]` |
| `carry: ["total"]` | `[]` | Values threaded from one iteration to the next |
| `parallel: true` | `false` | Launch every iteration at once, rather than one at a time |
| `max_iters: 50` | no cap | A hard limit. `0` is legal and means no iterations |
| `trim_on_mismatch: false` | `true` | With several `over` lists of different lengths: trim to the shortest, or fail |

## The four kinds of port

Which kind a port is depends on where it is and whether you named it.

| Kind | Outside the loop | Inside the loop |
|---|---|---|
| **Iterated**, named in `over` | `List[T]` or `Generator[T]` | One `T`, a different one each iteration |
| **Broadcast**, any other input | `T` | The same `T`, every iteration |
| **Gathered**, any output not in `carry` | `List[T \| Null]`, one slot per iteration | You write one `T` per iteration |
| **Carried**, named in `carry` | The seed goes in, the final value comes out | The running value in, the new one out |

A gathered output **has** to allow null, because an iteration that failed
leaves an empty slot. That is the `gather-output-must-be-nullable` error.

The list that comes out is built for every gathered port, even one that no
iteration ever wrote, because a port that produces nothing at all would leave
whatever is downstream waiting forever.

A carried value that an iteration does not write keeps what it had. If you do
not wire a seed, it starts at the zero for its type: `0` for a number, `""` for
a string, `[]` for a list.

## Two implicit ports

`self.index` is a `Number`, readable inside, and it is which iteration you are
in, counting from zero.

`self.done` is a `Boolean` you can write inside, and it is how the body votes
to stop.

Both only appear if you did not declare a port of that name yourself. And
`index` as an input name, or `done` as an output name, is the
`reserved-port-name` error.

## One at a time, or all at once

Sequential is the default. Each iteration finishes before the next launches,
which is what makes `carry` possible: the next iteration reads the value the
last one wrote.

`parallel: true` launches every iteration up front. Three things it refuses:

- **with `carry`**, because carrying means each turn feeds the next, and that
  is sequential by definition
- **without `over`**, because there is nothing to spread out
- **with `self.done`**, because a vote to stop means nothing when everything
  already started

## How a loop ends

- It runs out of items in `over`.
- It hits `max_iters`.
- The body writes `self.done = true`, on a sequential loop.

A sequential loop with none of those is the
`loop-unbounded-no-termination` error, refused at compile time rather than
left to run forever.

With several `over` lists of different lengths, the default trims to the
shortest. Set `trim_on_mismatch: false` and a mismatch fails the run instead,
naming the lengths.

## When an iteration fails

The run carries on. That iteration's slot in every gathered list is `null`, and
a carried value keeps what it had.

So a loop over a hundred rows where three fail gives you a hundred slots with
three nulls in them, rather than nothing at all. Whatever reads that list
decides what a null means.

## What cannot go in one

A trigger and an infra node are both refused inside a loop, with
`trigger-in-loop` and `infra-in-loop`. A trigger registers once for the
project, and infrastructure is provisioned once for the project, so a
per-iteration one is not a thing.

## Cutting a run inside one

You cannot. `weft run --target` stops at a loop's edge and brings the whole
loop along, because half an iteration is not a state the runtime has.
