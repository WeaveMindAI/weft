# Driving a coding agent

If you already use Claude Code, Codex or another coding agent, a weft program
can drive it. The agent works in a folder you pick, and when it needs a person
it asks through a form and waits as long as that takes. While it waits on
somebody, or works quietly on a long step, the run lets go of its worker. The
agent's model requests are billed to the account it logs in with: the one you
connect on `access`, or, for most agents on your machine with `access` left
empty, the login it already has (see [logging in](#logging-in)).

Here is a whole program:

```weft
claude = ClaudeCode {
  folder: "/home/you/projects/app"
}

talk = @include("__catalog/base_catalog/agents/conversation.weft")
talk.agent = claude.agent
talk.stream = claude.stream
talk.task = "Add a --verbose flag to the CLI and update the README."

out = Debug { data: talk.reply }
```

`__catalog/` points into your project's `nodes/` folder, where this loop lives
under `base_catalog/agents/`. If your project is older than these nodes, run
`weft catalog update` first.

`ClaudeCode` is an infra node: it starts a small server beside Claude Code,
which holds the conversation and the agent's process. That server is why a run
can let go of its worker and still find the conversation where it left it. So
start the infrastructure first, then run the program:

```bash
weft infra start
weft run
```

Claude Code starts on the task. When it wants to run a command or change a file
it has not been allowed to, or asks you something with its own question tool,
a form appears in the browser extension (to set the extension up, go and read
[putting a person in the loop](a-person-in-the-loop.md)). Your answer goes back
to it and it carries on. When it replies, a form shows you the reply and asks
for your next message. Leave that empty and the conversation ends, along with
the run.

Swap `ClaudeCode` for `Codex` and the same program drives Codex. For every
other agent weft drives, go to [the list at the end of this
page](#every-agent-weft-drives).

## One node is one conversation

Two agent nodes are two conversations, even with the same agent and the same
folder. If you run the program again, the agent still remembers the earlier
runs, because every run talks to the same conversation. Two runs at once take
turns: the second run's message waits until the first run's turn ends.

If you want the agent to forget the conversation, press **Start over** on its
node's card in the graph and confirm. The same card shows whether the agent is
working, waiting on an answer or on a tool call, and how many messages are
queued. If the agent says it is retrying a request its provider refused
(Mistral Vibe does, for minutes against a rate limit), the card shows that
too, until the agent does something else. If you start over in the middle of
a turn, that turn stops, and the
run waiting on it fails with `the conversation was started over from the
agent's card while this turn ran; send the message again`. When the turn was
waiting on a question or one of your tools, the run fails once the answer comes
back, with `nothing waits for question '<id>': its turn ended first:` (or
`tool_call '<id>'` for a tool) and that same reason. In your own loop,
`startOver: true` together with an `AskAgent`'s `message` starts a new
conversation with that message, once any turn queued before it is done.

## On your machine, or inside a computer

With nothing on `computer`, the agent runs on your machine. If you already
have it installed, weft uses yours, and for most agents, leaving `access`
empty means it uses the login it already has there (five never do, see
[logging in](#logging-in)). If you keep it somewhere off your `PATH`, give its
path in `program`, and weft uses that one.

If you do not have it, weft installs it the first time the infra starts, into
`~/.local/share/weft/needs/agents/<agent>/` (Claude Code goes into
`~/.local/share/weft/needs/agents/claude/`). It installs nothing outside that folder
and never asks for `sudo`. Every program and every weft install on your
machine then uses that copy, so it is installed once. While it installs, the
infra's progress says what it waits on:
`server: installing Claude Code (its log: /home/you/.local/share/weft/needs/agents/claude.install.log)`.

A few agents also need a system tool that only `sudo` can install. weft
does not install those itself: the start fails, naming the command that
installs them, and `weft deps-install`, run in the project's folder, installs
them (asking for your password) and then the agent.

The agent works in `folder`, which has to exist; if it does not, the node
fails with `the folder <path> does not exist; create it, or pick one that
does`.

If you want the agent kept off your machine, wire a
[Computer](https://github.com/WeaveMindAI/weft/blob/main/catalog/computer/README.md)
(a Linux machine in a container that weft starts for you) into `computer`, and
the agent runs inside it. Weft installs the agent there when it is missing and
starts its server whenever it finds it down, and `folder`, when you give one,
is a path inside the computer (its home folder otherwise). Several agents can
share one computer. On a cloud install there is no machine of yours for the
agent to run on, so you need a computer, and building without one says:

```text
ClaudeCode 'claude' runs on the machine weft runs on, which a cloud install does not have; wire a Computer into `computer`.
```

## Stop keeps the conversation, terminate deletes it

If you `weft infra stop` an agent node, everything it keeps stays where it
is: the conversation, the agent's own home with its login and sessions, the
server's log. On your machine its server stops, after stopping the agent;
inside a computer the server keeps running until the computer stops. The
next `weft infra start` picks the conversation up where it was.

If you `weft infra terminate` it, nothing of that conversation is left. On
your machine, weft stops the server and deletes its folder,
`~/.local/share/weft/infra-processes/<copy>/` (under the branch's own install
folder on a branch), which holds all of it. Inside a computer, weft asks the
computer to remove the agent: the computer stops its server and deletes
`~/.weft/agents/agent-<node>/`, which holds the server, its log and the
conversation, and the agent's files in `~/.weft/apps/`. Other agent nodes in
the same computer are left alone, and so is everything else in it. If the
computer is stopped, nothing can be removed from it, so the terminate fails,
says so, and leaves the agent node as it was: start the computer and
terminate the agent node again, or terminate the computer too.

A few things stay after a terminate, because they are not this
conversation's:

- the agent's own install, `~/.local/share/weft/needs/agents/<agent>/` on your
  machine and `~/.weft/agents/<agent>/` in a computer, which every node of
  that agent uses;
- what the agent writes into the home it runs with when that home is not one
  weft made for this node: on your machine with `access` empty, that is your
  own home (Claude Code's sessions in `~/.claude`, Codex's in `~/.codex`, and
  so on), which weft never deletes. Claude Code and Muse Code write there even
  with a connection, since weft does not give them a home of their own; in a
  computer that home is the computer's, and it goes when you terminate the
  computer.

## Logging in

If you leave `access` empty on your machine, the agent uses the login it
already has. For Google Antigravity, that is whatever its SDK finds in your
environment, such as `GEMINI_API_KEY` or your Vertex AI settings. If Codex has
no login there, the turn fails saying so:

```text
Codex is not logged in with this machine's own login (`codex login`); log in again, or give the node a Codex connection
```

Devin CLI, Hermes Agent, nanobot, OpenClaw and Pi never use the login on your
machine, so even there they need their access node (`DevinCliAccess`,
`PiAccess`, and so on) on `access`, and all but Devin CLI also need `model`,
since a provider's key names no model. Each runs in a home of its own, because its install on your machine would bring along what it keeps
there: Devin CLI would import your Claude Code, Windsurf, Copilot, OpenCode
and Zed settings, Hermes Agent may borrow your Claude Code and Codex logins,
nanobot's config would start your own channels and jobs, and OpenClaw's
Gateway holds its own logins, channels and scheduled jobs. Pi keeps its
settings and sessions in that home too, and never loads the folder's `.pi/`
settings, extensions or MCP servers.

Inside a computer there is no login waiting, so you need an access node, which
every agent has: `ClaudeCodeAccess` for `ClaudeCode`, `CodexAccess` for
`Codex`, and so on.

```weft
box = Computer {}
login = ClaudeCodeAccess {}

claude = ClaudeCode {
  computer: box.computer
  access: login.access
}
```

Connect the access node by its name, here `weft connect login` (`login` is the
node), or press the **Connect** button on the node in the graph (for the details, go and read [connecting an
account](connections.md)). Each access node offers the ways its agent logs in,
with a guide for each:

| Agent | Your subscription | An API key |
|---|---|---|
| Claude Code | The token `claude setup-token` prints, valid a year | An Anthropic API key |
| Codex | The content of `~/.codex/auth.json` after `codex login` | An OpenAI API key |

A subscription login works on a local install only, so the token stays on your
own machine; a cloud install refuses a connection made with one. For why, go
and read [how connections work](../connections/how-they-work.md#a-way-that-stays-on-your-machine).

If you connected Codex with your ChatGPT login, one connection serves as many
Codex nodes as you like, on your machine or in computers. weft keeps the login
alive itself. Codex is handed only the short-lived token, never the refresh
token, so it never renews the login on its own. When the token is about to
die, or OpenAI refuses it, the agent node asks weft for a fresh one: weft
renews the connection once, and every other Codex node on it is handed that
same token the next time it needs one.
That ask reaches the agent node on `stream`, so wire `stream` into every
`AskAgent` of a Codex node.

From the moment you paste it, the login belongs to weft. On the machine you
copied `auth.json` from, run `codex logout`, or `codex login` again to give
that machine a login of its own: each time weft renews the login, the copy on
that machine stops being able to renew. An API key has none of this.

## Your own loop

If you want a different loop (tools of your own, a check in place of a person
writing the next message), copy `nodes/base_catalog/agents/conversation.weft`
into your `src/`, change the copy, and include it from there
(`@include("conversation.weft")` in `src/main.weft`): an edit in
`base_catalog/` itself is overwritten by the next `weft catalog update`. It is
built around these nodes:

- `AskAgent` gives the agent one thing per firing: a `message`, the `answer` to
  its question, or the `toolResult` of a call it made. Each firing ends on the
  first of three outputs: `reply` with `history` (the whole conversation as a
  ChatHistory file, the format `LlmInference` reads), `question` when the agent
  asks something or asks permission, or `toolCall` when it calls one of your
  tools.
- `AnswerAgent` turns a `question` into a form for a person and gives back the
  `answer`. Wire `history` into it and the person sees the conversation beside
  the question.
- `CloseBus` on the agent node's `stream` ends the agent node's part of the
  run. Wire its `_should_flow` to an output that arrives only once the loop is
  over, as `conversation.weft` does with its loop's last `message`.

Building refuses an `AskAgent` whose `question` or `answer` is not wired,
because an agent asking would then wait for an answer nobody can give.

If you want the agent to call tools of yours, give `AskAgent` a list of
`LlmTool` on `tools` (an `LlmTool` node, or a group's `*tool` [metadata
port](../language/groups.md#metadata-ports)). A call comes out on `toolCall` as
`{ id, name, arguments }`, with `arguments` as a JSON string, and the agent
waits. If you want each call to reach the right tool, a `Dispatch` node sends
it to the branch whose tool has the call's `name`. For how those branches are
wired, go and read [a model that calls tools](../language/groups.md#a-model-that-calls-tools),
but leave out its `over` and its `ChatHistoryAppend`: `AskAgent` hands out one
call per firing, and the answer goes back as a record. Send the result back as
the next firing's `toolResult`: `{ callId, output, isError? }`, where `callId`
is the call's `id` and `output` is text. In your copy of `conversation.weft`,
that means giving the `turn` loop a `toolResult` input, adding it to `carry`
the way `answer` is, wiring `ask.toolCall` into the `Dispatch` and
`self.toolResult` from your tool's result. Building refuses an `AskAgent`
that has `tools` but nothing wired to `toolCall`, and one whose `toolCall` is
wired but whose `toolResult` is not, because a call would then wait for a
result nobody sends. The agent's own tools (its shell, its file edits) never
come out of `AskAgent`: they run inside the agent and show up on `stream`.

If you call your tool `search`, `toolCall` gives `search` back whichever agent
you drive, so your `Dispatch` branches always match it. What the agent's model
sees can differ: 16 agents show it as `search`, and 9 show it some other way
(Gemini CLI shows `mcp_weft_search`). So if your message tells the agent which
tool to use, check what its model calls it in [the
table](#every-agent-weft-drives).

If your tool has the same name as one of the agent's own tools, some agents
let yours take that tool's place, some refuse the name, and some do a bit of
each, keeping certain names for themselves; the table's last column says
which. The names an agent keeps for itself
are listed under `reservedToolNames` in its node's `metadata.json`
(`nodes/base_catalog/agents/claude_code/metadata.json` for Claude Code), and
an agent that refuses whole shapes of name lists them as regexes under
`reservedToolNamePatterns` (Codex refuses a name starting `mcp__`). When
your tools arrive on a wire, as they usually do (a `List` gathering your tool
nodes), the `AskAgent` firing that gets one of those names fails with this
message. When you write the list straight on `tools`, the build refuses it with
the same message:

```text
ClaudeCode 'claude' cannot give up its built-in tool 'ToolSearch', so AskAgent 'ask' cannot hand it a tool of that name on `tools`; rename your tool.
```

An agent asks in two ways, and they take different paths. When it asks with its
own question tool, or asks permission, its turn is still running, so the
question comes out on `question` and the answer goes back into the same turn.
When it ends its turn with a question in its text, that is a `reply`, and the
next `message` answers it.

Weft waits for the person as long as they take, but some agents stop waiting
on their own: OpenClaw gives up on a question after an hour, and on a command
it asked to run after 30 minutes, both limits written into its own code. Its
model then carries on without the answer. When that happens, `AnswerAgent`
takes its form down, so nobody answers a question nothing reads any more, and
its `answer` comes out as `{ "questionId": ..., "dropped": true }`. Wire that
into `AskAgent` like any answer (`conversation.weft` already does): its next
firing comes back with what the agent did after it gave up, its reply or its
next question. If you want to know how long your agent waits, check the agent
you drive.

The form goes within 30 seconds of the agent giving up: while it is up,
`AnswerAgent` asks the agent server every 30 seconds whether the agent still
waits, and stops asking once the form is answered or taken down.

## Watching it work

Everything the agent does goes out on the agent node's `stream` bus: each
`AskAgent` wired to it sends the events of its turn, each one once, even when
the run paused in between. That is its text, the tools it uses and what they
return, its reasoning when it shows it, its calls and questions, and the end
of each turn, or the error it failed with. Some agents, Codex among them, also say which files they
changed. If you want to watch it, wire `stream` into a `Debug`, which logs each
event as it lands.

With the default, `streamBy: "message"`, each message goes out once it is
whole, so while the agent runs a five-minute command the run sleeps. If you
want text to show up as it is written, set `streamBy: "token"` on the agent
node; the run then stays awake while the agent writes.

## When something goes wrong

**A run with no `CloseBus`** never finishes cleanly: the agent node waits for
its bus to end, so once nothing else is left to happen, weft ends the bus as
failed, and the run fails with:

```text
the bus ended failed: the run was stuck: every node on this bus waited on another, and nothing could end the wait
```

If you see this, add a `CloseBus` after your loop, as in [your own
loop](#your-own-loop).

**The agent's server restarted mid-turn** (your machine rebooted, the computer
was restarted). The turn is lost, and the run fails with:

```text
the agent server restarted in the middle of this turn, so the turn is lost; send the message again (the conversation itself is kept)
```

If the agent was waiting on a question or one of your tools at the time, the
run fails once the answer comes back, with `nothing waits for question '<id>':
the agent server restarted while the agent waited on it, so its turn is lost`
(`tool_call '<id>'` for a tool).
The conversation is kept, so if you run the program again, the agent gets the
program's first message (in `conversation.weft`, `task`) on top of everything
it already remembers.

**The agent itself crashed mid-turn.** The run fails with the last lines the
agent printed. Run the program again and the agent starts up again on the same
conversation:

```text
Claude Code ended in the middle of the turn (exit 1): <what it printed>
```

**The agent's account will not run the turn.** When Cursor CLI's account
has to do something first (sign in, upgrade its plan, add a payment method),
Cursor says so as an ordinary message, and weft fails the turn with Cursor's
words:

```text
Cursor refused the turn: "Upgrade your plan to continue" (its plan does not cover this turn: upgrade it, or pick a `model` the plan includes), then send the message again
```

GitHub Copilot CLI on a Free plan offers only the model it picks itself, so
if you set `model` to another one, the turn stops before the model runs and
fails naming the models the plan offers. Set `model` to one of those, to
`auto`, or leave it empty.

**The agent asks for a secret** in a question. A form's answer is kept in the
run in plain text, so `AnswerAgent` fails with `the agent asks for a secret
(<the question>), and a form's answer is kept in the run in plain text; put it
in a file in the agent's folder and tell the agent where it is`.

**A tool of yours keeps Codex waiting.** While one of your tools has not
answered, Codex asks its model again about every half minute, and each of those
is a model request. In a test on codex-cli 0.166.0 the call came back failed
after 21 minutes, which `stream` shows as a failed `tool_result`. A result
sent after that never reaches Codex: the `AskAgent` firing that carries it
reads whatever is left of the turn instead, and ends on its reply, its next
call or its next question. If the agent's server restarted in between, the rest
of that turn is gone, and the firing fails saying so; send the message again.
If a person has to decide something, tell the agent in your message to ask
with its own question tool: that question comes out on `question`, and
`AnswerAgent` puts it to the person.

## Every agent weft drives

For what each permission choice does, read the node's `permission` setting.
Its description in the graph lists which of the agent's own options you can
set from weft.

| Node | Agent | Ways to log in | Your tool `search`, as its model sees it | Your tool named like one of the agent's own |
|---|---|---|---|---|
| `ClaudeCode` | Claude Code | Claude subscription, local install only; API key | `search` | replaces it, except `ToolSearch` |
| `ClineCli` | Cline CLI | Cline API key; Another provider | `search` | replaces it |
| `CodeBuddyCode` | CodeBuddy Code | API key | `search` | replaces it |
| `Codex` | Codex | ChatGPT login, local install only; API key | `search` | replaces `web_search`, `view_image`, `update_plan` and `image_generation`, which Codex can turn off one at a time; the others are refused, and so is a name starting `mcp__` or holding a character other than letters, digits, `_` and `-` |
| `CopilotCli` | GitHub Copilot CLI | GitHub token; Your own model provider | `search` | replaces it, except `tool_search_tool` |
| `CursorCli` | Cursor CLI | API key | a tool of the `weft` server, called through `CallMcpTool` | refused |
| `DeepSeekHarness` | DeepSeek Harness | DeepSeek API key | `search` | replaces it, except `run_code` |
| `DevinCli` | Devin CLI | API key | `mcp__weft__search` | refused |
| `GeminiCli` | Gemini CLI | Gemini API key; Vertex AI key | `mcp_weft_search` | refused |
| `GoogleAntigravity` | Google Antigravity | Gemini API key; Vertex AI key | `search` | replaces it, except its skill tools |
| `Goose` | goose | Anthropic API key; OpenAI API key; Gemini API key; OpenRouter API key; OpenAI-compatible server | `weft__search` | refused |
| `GrokBuild` | Grok Build | API key | `weft__search`, found with `search_tool` and called through `use_tool` | refused |
| `HermesAgent` | Hermes Agent | OpenRouter API key; Anthropic API key; OpenAI API key; Gemini API key; OpenAI-compatible server | `search` | replaces it, except eight that Hermes always runs itself |
| `JunieCli` | Junie CLI | Junie token; Anthropic, OpenAI, Gemini, xAI, Meta or OpenRouter API key; LiteLLM proxy. All of them are local install only, because JetBrains' terms forbid offering Junie to anyone else, so JunieCli does not run on a cloud install, even inside a Computer | a tool of its `weft` MCP server; before each turn Junie spends a model call on a filter that can leave your tools out, and its model switches them back on with `enable_mcp` | refused |
| `KiloCode` | Kilo Code | Kilo API key; Anthropic API key; OpenAI API key; OpenRouter API key; Gemini API key | `search` | replaces it, except its three MCP resource tools |
| `KimiCode` | Kimi Code | Moonshot API key | `search` | replaces it, except `ReadMediaFile` |
| `KiroCli` | Kiro CLI | API key | `weft::search`, loaded with `tool_load` when it needs it | refused |
| `MiniMaxCode` | MiniMax Code | MiniMax API key; Your own model provider | `mcp__weft__search` | refused |
| `MistralVibe` | Mistral Vibe | API key | `search` | replaces `ask_user_question`, `todo`, `cron` and `unified_harness_scratchpad`; its other eight are refused |
| `MuseCode` | Muse Code | Meta Model API key | `mcp__weft__search` | refused |
| `Nanobot` | nanobot | Anthropic API key; OpenAI API key; OpenRouter API key; Gemini API key; DeepSeek API key; Mistral API key; OpenAI-compatible server | `search` | replaces `run_cli_app`, `my` and `generate_image`, which nanobot can turn off one at a time; the others are refused |
| `OpenClaw` | OpenClaw | Anthropic API key; OpenAI API key; Gemini API key; OpenRouter API key; OpenAI-compatible server | `search` | refused |
| `OpenCode` | OpenCode | Anthropic API key; OpenAI API key; OpenRouter API key; Gemini API key; OpenCode Zen key | `search` | replaces it, except `execute` |
| `Pi` | Pi | Anthropic API key; OpenAI API key; Gemini API key; OpenRouter API key; DeepSeek API key; Mistral API key; xAI API key; Groq API key; OpenAI-compatible server | `search` | replaces it |
| `QwenCode` | Qwen Code | OpenAI-compatible key; Anthropic API key | `search` | replaces it |
