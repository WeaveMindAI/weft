# Giving a model a computer

A model can work a computer the way you do: look at the screen, click, type,
run commands, look again. It never touches the machine itself. It writes a
tool call, something carries the call out, and a line of text comes back
saying what happened; when it wants to see the result, it asks for a
screenshot. In weft that something is a handful of ordinary nodes, and the
machine is an infra node of its own.

If you only want an agent working a computer, this is the whole program:

```weft
box = Computer
anthropic = AnthropicAccess
model = AnthropicModel { access: anthropic.access, model: "claude-sonnet-5" }

agent = @include("__catalog/base_catalog/computer/agent.weft")
agent.task = "Find the weather in Paris tomorrow and write it to ~/weather.txt"
agent.computer = box.computer
agent.model = model.provider

out = Debug { data: agent.answer }
```

`AnthropicAccess` needs your Anthropic key (`weft connect`, or the node in
the graph). A path starting with `__catalog/` points into your project's
`nodes/` folder, where this package lives under `base_catalog/computer/`. If
your project is older than these nodes, run `weft catalog update` first to
bring them in.

No run starts infrastructure for you, so start the machine first:

```bash
weft infra start
weft run
```

In the graph, the `Computer` node shows its screen, refreshed every few
seconds, so you can watch the model work.

## The machine

`Computer` runs one container: a screen at `1280x800` by default, a window
manager, a terminal and Firefox, `git`, `curl`, Python 3, and a bash shell
for commands. Its home folder sits on a disk that
`weft infra stop` keeps and `weft infra terminate` deletes. Anything outside
the home folder, a package installed with `apt-get` included, is gone after a
stop.

| Set it when | Input |
|---|---|
| a task needs a bigger or smaller screen | `screen`, as `WIDTHxHEIGHT`, the size of every screenshot. Above `1280x800`, a vendor may shrink the image, and coordinates read off it stop matching the screen |
| a task needs software the machine lacks | `packages`, Debian package names separated by spaces, installed again at every start |
| the work fills more than the default `10Gi` | `disk`, a number followed by Gi, Mi, G or M (`20Gi`) |
| on a cloud install, a browser crawls or runs out of memory | `cpu` and `memory`, `1` and `2Gi` by default. A local install does not cap the container, so they change nothing there |
| you, a person, or a builder like Tangle wants to get into it | `sshKeys`, public keys one per line. They open SSH to the install's own network (your machine on a local install, the install's private network on a cloud one, never the internet); `weft infra list-doors` prints the address for `ssh -p <port> user@<host>` |
| on a cloud install, `cpu` and `memory` cannot describe the machine you need | `machineType`, the cloud's own name for it (`n2-standard-4` on Compute Engine), used as is in place of the machine `cpu` and `memory` would pick. A local install ignores it |

There is no credential inside unless your program puts one there. If a task
should stay away from some site, say so in the task and do not give the model
an account there: the container can reach anywhere on the internet, and weft
cannot narrow that.

For anything interactive (looking around, checking a build, letting a person
work in it), SSH is the way in, rather than streaming commands through
`RunCommand`.

One computer has one screen, one mouse and one keyboard, so agents working at
the same time each want a computer of their own: a `Computer` marked
`@per_instance` gives each instance one without writing the node twice.
Computers share no disk; agents on them coordinate through a git repository
they can all reach.

If the screen freezes, the shell gets stuck or SSH goes down, buttons on the
`Computer` node's card get you out without restarting anything else.
**Restart SSH** starts the SSH server again (the card shows it only when the
node has `sshKeys`, and says when the server is down). **Restart the desktop**
closes every window and starts the screen again, and **Restart the shell**
ends every shell session: it stops whatever they run, and whatever their
commands left running, and each starts over in the home folder. Files on the disk
survive both.

## One node per action

Every action is its own node, and you can use any of them without a model.
If you want to script a few actions yourself, wire them in a row. With the
`box` from the program above, this one takes a screenshot, clicks at (300, 200), then types `hello` and presses
Return:

```weft
look = Screenshot { computer: box.computer }
focus = Click {
  computer: box.computer
  args: { "x": 300, "y": 200 }
  _should_flow: look.result
}
typing = TypeText {
  computer: box.computer
  args: { "text": "hello\n" }
  _should_flow: focus.result
}
```

Each node runs as soon as its inputs arrive, so without the `_should_flow`
gates all three would act at once. Each gate waits for the action before it.

In the table, `?` marks a field you can leave out. A point read off a full
screenshot is where the click lands; a point read off a zoom is not, so work
it back through the zoom's `region` first.

| Node | What it does | `args` |
|---|---|---|
| `Screenshot` | the whole screen, on `image` | none |
| `Zoom` | one region, enlarged to the screen's size, on `image` | `{ region: [x0, y0, x1, y1] }` |
| `Click` | a click, or a double or triple click with `count` | `{ x?, y?, button?, count?, modifiers? }` |
| `Drag` | a press, a move along a path, a release | `{ path: [{ x, y }, ...], button?, modifiers? }` |
| `MoveMouse` | a move with no click | `{ x, y, modifiers? }` |
| `MouseButton` | a button pressed down or let up | `{ action: "down" or "up", button? }` |
| `CursorPosition` | where the mouse is | none |
| `Scroll` | the wheel | `{ x?, y?, direction, amount?, modifiers? }` |
| `TypeText` | types `text`; a newline in it presses Return | `{ text }` |
| `PressKey` | a key or a combination, like `Return`, `ctrl+s` or `alt+Tab` | `{ keys, repeat?, holdSecs? }` |
| `RunCommand` | a command in a shell session; its whole output comes out as a file, `output`; `restart: true` ends the session first | `{ command?, restart?, timeoutSecs? }` |
| `EndSession` | ends a shell session | none (it takes `session`) |

For the values each field takes (`button` is left, right, middle, back or
forward, for any node that takes one; `modifiers` and combinations are keys joined by `+`; key names are
xdotool's, so `Return` rather than `Enter`), read the node's description in
the graph or run `weft describe-nodes --node Click --compact`.

Each action node's `result` is a sentence a model can read back
(`Clicked.`). When the computer cannot do what was asked, the node fails
with the computer's message after its name for the action (`click`, `move`,
`type`, `key`, `exec` for a command), like
`click: (300, 900) is off the 1280x800 screen`, and `_error` catches it like
any other failure.

`RunCommand` hands back the command's whole output as a stored text file,
however long, with its exit code on `exitCode`; a command that exits
non-zero still succeeds. If a model is to read the output, pass the file
through `TextPreview`, which keeps its start and end within a budget you set
and says how much it cut, so a long output never floods the model.

Commands run in a session, named by `RunCommand`'s `session` (left out, the
computer's default one; a name used for the first time starts in the home
folder). A session keeps the folder a command ended in and its exported
variables for its next command, so a `cd` or an `export` carries over; a
plain variable, a readonly one or a function does not. A session runs one
command at a time (one sent while another is still going fails, naming it),
and different sessions run side by side. Nothing stays open between
commands, so a session costs nothing while it waits. `EndSession` stops what
a session's commands left running and forgets it, and the name starts over.

A command can run as long as it needs: past ten seconds, the run waits for it
without holding a worker, and checks on it every five seconds. If you start
a server or a watcher, put `&` after it, or it holds its session and every
later command there fails. If you want a command stopped after a while, give
`timeoutSecs`: it stops the command and everything the command started, and
nothing an earlier command left running.

## How a model's call reaches a node

Each action node describes the tools a model calls it through on its `tools`
metadata port, an `ActionTools` record holding two lists of `LlmTool`. They
copy each vendor's own computer tool, name for name and field for field,
because that is what the models were trained on: `anthropic` holds Claude's
names (`left_click`, `key`, `zoom`, ...), `openai` GPT's (`click`,
`keypress`, ...), and `RunCommand` adds `bash` for the shell to both. For a
model that is neither Claude nor GPT, either list works. They are ordinary
function tools, so they go through any provider, OpenRouter included, which
refuses the vendors' built-in computer tools. Zoom, mouse down and up,
cursor position, triple clicks and holding a key down exist only in
Anthropic's list; the back and forward buttons only in OpenAI's.

If you want to hand a model the actions you wire, gather their `tools` the
way `perform.weft` does, in one metadata port of your own
(`self.tools = { "anthropic": [...click.tools.anthropic, ...], ... }`), and
wire one list of it into the LLM node's `tools`. An action you leave out is
one the model is never offered.

`RouteComputerCall` takes one call at a time, so it sits inside a Loop over
the LLM node's `toolCalls`. It recognizes the call in either style and puts
its arguments, translated to the one shape the action nodes take, on that
action's port. Every other port closes, so the one action node wired to the
port that fired is the one that runs. A call to any other tool comes out on
`other`, untouched, so you can send it on to your own tools from there
with a `Dispatch`.

## The two premade files

If you would rather not wire `RouteComputerCall`, the eleven action nodes and
a `Wait` for the model's wait yourself, `perform.weft` has it done, and
`agent.weft` builds a whole agent on it.

`__catalog/base_catalog/computer/perform.weft` does one tool call on a
computer. It takes `call`, `computer` and the shell `session` to use, and
gives back three things. `result` is the text the model reads: the action's
result, or why it failed; for a command, its output through `TextPreview`
(its first and last lines, 10,000 characters at most) and its exit code.
`image` is the screenshot or zoom, when the action took one. `error` is
closed unless something went wrong, a call to a tool that is not a computer
action included.

`perform.weft` also hands out, on its `tools` metadata port, every action it
performs, described in both vendors' names.

`__catalog/base_catalog/computer/agent.weft` takes `task`, `computer`,
`model`, and an optional `agentId`, and gives back
`answer`. It offers the model the actions in Anthropic's names; if your
model is a GPT, change `calls.tools.anthropic` to `calls.tools.openai` in
your copy of the file. Its commands run in a session of its own, named after the run's
id, or the run's id and `agentId` when that is set, so several agents in one
run keep apart; it ends its session when it answers. A run that fails or
is stopped before that leaves the session up, with whatever its commands
left running, until an `EndSession` on it or Restart the shell on the
computer's card. If an agent you call
again in a later run should find its shell where it left it, key the session
on `agentId` alone and leave out the end: the file says where. Inside, a Loop
carries the conversation's `historyFile` from turn to turn: each turn the
model reads it, every action it asked for runs through `perform.weft`, and
the results go back into it. Screenshots go in one message after the turn's
results, for every provider, because OpenAI's chat format takes no image
inside a tool result. When an action fails, the rest of that turn's actions
are answered without running, so the model sees the failure before it acts
again; a command that exits non-zero is not a failure here, it only ends its
result with the exit code. The loop ends when the model answers without calling a tool. It allows
100 turns, and on the last one the model is not allowed to call a tool, so it
always answers, saying where the task stands if it could not finish.

The default prompt asks the model not to pay, buy, send a message or an
email, accept terms or delete something it did not create unless the task
clearly asks for it, and to stop and answer if the task cannot go on without
one; that is a request to the model, and nothing enforces it. If you want a
person to approve those actions, or a check on certain others, write your own
agent around `perform.weft`. Copy
`agent.weft` out of `nodes/base_catalog/computer/` into your own code,
because `weft catalog update` replaces that folder; it already includes
`perform.weft` by its `__catalog/` path, so the copy works as it is. If you
want your own system prompt, copy `agent_prompt.md` too and point the
`@file` at it. For the approval step itself, `HumanQuery` asks a person
and parks the run until they answer.
