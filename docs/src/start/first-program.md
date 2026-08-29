# Your first program

```bash
weft new hello
cd hello
weft run
```

`weft new` scaffolds a project. `weft run` compiles it, registers it with the
daemon, fires one execution, and streams the events back until it finishes.

## What got created

```
hello/
  weft.toml      the project's name and its permanent id
  main.weft      the program
  nodes/         every node this project can use
  .weft/         build output and caches (already gitignored for you)
```

`nodes/` is the surprising one, because it changes where your nodes come
from. When you run `weft new`, the entire standard library is **copied into
your project** under `nodes/base_catalog/`, so the build never reaches back
into the weft installation and upgrading weft cannot change what your program
does. If you want the newer standard library later, `weft catalog update` re-syncs
that mirror.

Your own nodes go anywhere else under `nodes/`, never inside `base_catalog/`,
because `weft catalog update` wipes and recopies that folder and anything you
edited in there goes with it.

## The program

`main.weft` is three lines:

```weft
greeting = Text { value: "hello world" }
out = Debug

out.data = greeting.value
```

Two node declarations and one connection.

The first line says: make a node called `greeting`, of type `Text`, configured
with the string `"hello world"`. The second makes a `Debug` node called `out`.
The third wires them.

Read the connection right to left, the way an assignment reads: the value
flows **from** `greeting.value` **into** `out.data`.

```
  greeting (Text)                 out (Debug)
  ┌──────────────────┐            ┌──────────────┐
  │ value: "hello…"  │            │              │
  │            value ●───────────▶● data         │
  └──────────────────┘            └──────────────┘
```

Before anything ran, the compiler checked that connection. Both ports exist,
`Text.value` emits a `String`, `Debug.data` accepts one, and nothing required
was left unwired.

## What `weft run` printed

One line per node event, in order: the execution started, `greeting` ran and
emitted, `out` ran, the execution completed. All of it is written to the
journal as it happens, and you can
read them back later with `weft events <color>`.

A **color** is one execution. Running the same project again mints a new one,
so whenever anything in weft says "per color", it means per execution.

## Change something

Edit `main.weft`:

```weft
greeting = Text { value: "hello world" }
shout = ExecPython(text: String) -> (out: String) {
  code: "return {'out': text.upper() + '!'}"
}
out = Debug

shout.text = greeting.value
out.data = shout.out
```

`weft run` again. The chain is three nodes now.

`ExecPython` is worth noticing because of the arrow. Most nodes have fixed
ports declared by their author; this one lets you declare them inline.
`(text: String)` is its input, `-> (out: String)` is its output, and the Python
body gets `text` as a variable and returns a dict keyed by output port name.
The compiler type-checks those ports like any others.

## The mental model

A node fires when all of its required inputs have arrived. When it fires it
runs its code and emits values on its output ports, and each emission travels
along a wire to exactly one input port and waits there. A node with no upstream
fires immediately, and the execution ends when nothing is left in flight and
nothing is waiting.

Everything else in the language, groups and loops and streams and human pauses,
is built out of that one rule, including the wrinkle where branching comes
from: what happens when a node produces **nothing**. That is
[How a weft program runs](../language/mental-model.md), the chapter to read
once you want to build something real.

Next: [reading the graph](reading-the-graph.md).
