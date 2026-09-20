# Your first program

Make a project and run it:

```bash
weft new hello --assistant claude-code
cd hello
weft run
```

`--assistant claude-code` installs Tangle: instructions that teach your AI
assistant how weft works. Once it is in place, you can ask for changes in plain
English and it will make them. If you use Kilo Code, write `--assistant
kilo-code` instead. If you use neither, run `weft new hello --assistant none`
and skip [Ask Tangle](#ask-tangle); this page shows you two other ways to make
the same change. Repeat the flag to install for several assistants at once.
Whichever you pick is remembered, so your next `weft new` installs the same
one with no flag at all, until you pass `--assistant <name>` to change it or
`--assistant none` to stop.

If `weft run` says it cannot reach the runtime, start it with
`weft daemon start` and try again.

The terminal prints `registered hello` and the project id, then `started color`
and a long id, then a line or two for each step as it starts and finishes. That
long id is a **color**, and the last line, `✓ completed color=`, repeats its
first eight characters. A color is how you point at one particular run later.

## See the program as a graph

Open the `hello` folder in VS Code and open `src/main.weft`. The text opens on the
left and the graph opens beside it: two boxes joined by an arrow. This is the
same file both ways:

```weft
greeting = Text { value: "hello world" }
out = Debug

out.data = greeting.value
```

`greeting` and `out` are names, and you can change them to anything.
`Text` and `Debug` say what kind of step each one is. The last line is the
arrow: `greeting.value` flows into `out.data`.

## Change it

Now suppose you want it to greet a person by name.

The step for that is `Format`. It fills in a template: you write `{{name}}`
wherever a value should go, and you give the box an input with the same name so
the value has somewhere to land. Every placeholder needs an input of the same
name, or the step fails and tells you which is missing.

There are three ways to make this change. Pick whichever you like.

### Ask Tangle

Open the project in your assistant and tell it:

> Greet a person by name, with the name as its own input so I can change it
> without touching the text. Run it and show me the result.

Tangle makes the change and runs it for you.

![Tangle building the greeting, with the finished graph and its result beside it](../img/first_program.png)

You end up with three boxes, and the last one shows `"data": "Hello, Ada!"`.
The name sits in its own box. Tangle picked its own names for the boxes here, so
yours may read differently.

### Build it in the graph

Press `Ctrl+P`, type `Format`, and pick it. Double-click the new box's title and
rename it to `sentence`.

A new `Format` box arrives with one input, `template`. The value you want to
drop into the sentence needs an input of its own, so make that first. Underneath
the box's inputs there is a small **+ input** button. Click it, type `name`, and
press Enter.

Now right-click that new `name` dot. A menu opens with a row reading
`✎ Type: MustOverride`. Click it and the row turns into a text box with
`MustOverride` already selected, so type `String` over it and press Enter.
`MustOverride` is weft's way of saying nobody has decided this type yet, and the
build stops until you do.

Open the box. Its body has a **Template** field: click that and type
`Hello, {{name}}!`.

Now connect the boxes. Drag from the `value` dot on the right of `greeting` to
the `name` dot you just made, then from the `text` dot on the right of `sentence`
to `data` on `out`.

One thing left: `greeting` still holds `hello world`. Open it and change that to
`Ada`. Then click empty canvas so the box is no longer being typed into, and
press `Ctrl+Enter` to run.

### Write it

Go to the text (if you closed it, the code button at the top right of the
graph brings it back) and replace the file with this:

```weft
greeting = Text { value: "Ada" }

sentence = Format(name: String) {
  template: "Hello, {{name}}!"
  name: greeting.value
}

out = Debug
out.data = sentence.text
```

Then press `Ctrl+Enter` to run it.

## See what each box received

Once the program has run, a small magnifying glass appears in the top right of
every box. Click the one on `sentence` and a panel shows what went in, the
template along with `"name": "Ada"`, and what came out: `"Hello, Ada!"`.

Change `Ada` to another name and run again.

## What is in the folder

| File or directory | What it is for |
|---|---|
| `src/main.weft` | The program |
| `weft.toml` | The project's name and its permanent id |
| `nodes/` | The code for every kind of step it can use |
| `CLAUDE.md`, `.claude/` (or `kilo.json`, `.kilo/`) | Tangle's instructions, for whichever assistant you picked. They are copied in, so they get committed with the rest and somebody who clones your project gets Tangle without a weft checkout. After updating weft, `weft tangle update` re-copies them. |
| `layouts/` | Where you dragged the boxes. It turns up the first time you move a box. |
| `.weft/` | Build files, generated |

`weft new` also starts a git repository and writes a `.gitignore` for you.

If you write a step of your own, put it anywhere under `nodes/` except
`base_catalog/`. That folder holds the standard steps, and `weft catalog update`
wipes it and copies fresh ones in, deleting anything you left there.

If you want a step that does something new, check whether one already exists
before you write it. `weft describe-nodes --list` prints every step in the
project, one line each. Once a name looks promising,
`weft describe-nodes --node Text --compact` prints what that one takes in and
gives back.

## Where to go next

If you already have something you want to build, ask for it now. Ask for one
small piece at a time, run it against a real input, then say what is wrong with
it before you ask for the next. For more on working that way, read
[Sequential Diffusion Programming](../thinking/sdp.md).

If you would rather keep reading, go and read
[Reading and building the graph](reading-the-graph.md) next.
