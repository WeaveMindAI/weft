---
name: weft-connections
description: "Connecting a weft project to outside services and putting people in the loop. Read when a user asks about connections, API keys, sign-ins, permissions, the browser extension, human tasks, or a public URL: the connect flow door by door, what the banners mean, tokens and scoping, and how HumanQuery tasks reach people."
---

# Connections and people

## What a connection is

A **connection** is an account somebody hooked up to an outside service. It
lives in the runtime's access store and holds everything secret; the
project's source holds only a bare id, so a credential can never end up in
git history. Every kind of credential is the same concept: an OAuth
sign-in, a pasted API key, a mail server login. What flows through the
graph is an `Access` value, a sealed handle that resolves to the signed-in
client when a node fires; it is not a secret.

An **access node** (`TelegramAccess`, `SlackAccess`, `OpenRouterProvider`,
`PostgresDatabase`, and so on) is where a connection is picked: expand the
node in the graph and click its "Connect <Service>..." field.

## The connect flow, door by door

The widget first lists existing connections ("identity / what it can do",
with Change and Disconnect), then "+ Add a connection" with up to two
doors. A door the service does not offer is hidden, never greyed out.

- **shared**: a credential this weft holds. Either "Sign in with <Service>"
  through a registered app (one click, the provider's consent page opens
  in the browser, the panel waits and updates on its own), or "Use ours
  (uses your credits)" for key-based services: calls on that connection
  spend the runtime's credits, and it says so.
- **own**: the user brings or creates their own. One page: the permissions
  to ask for (checkboxes), an optional "Create it for me" mint button, a
  generated step-by-step guide with the callback URL to register, the app
  fields or a paste-a-credential section, then "Sign in" or "Connect".

After connecting, the field shows "Connected as <identity>". Some services
(Google class) allow many grants; others (Slack bot, GitHub App) allow one
grant per account, and the widget says so when a second connect would
displace the first ("Upgrade this connection...").

An access node with a required, unpicked connection pins open in the graph
(expanded, collapse disabled, "Pick a connection first") until one is
picked; a `connection_optional` node never pins.

## The same flow in the terminal: `weft connect`

Everything the Connect panel does exists as a CLI verb, so a user without
VS Code is never stuck. `weft connect` with no arguments opens interactive
menus: the project's access nodes (including nodes inside `@include`d
files; each file is one target, and `--node` takes a bare id or
`file.weft:node` when the id is ambiguous), then the doors: paste a key
(hidden input), browser sign-in (URL printed, polled), the one-click
shared app, `--mint` for a created own app. A pick is written into the
`.weft` source through the compiler's structural edit, exactly as the
editor writes it, so it survives and diffs like any edit.

The scripted surface, which is yours:

- `--list` works from anywhere: inside a project it lists that project's
  access nodes with their stored connections (the picked one marked);
  outside a project, or in one with no access node, it lists every stored
  connection across all services.
- `--node <id> --grant <grant>` picks a stored connection (no secrets
  travel; this one is yours to run). It edits the `.weft` source and prints
  the edit as an edit tool would (`main.weft:15`, then `- old` and `+ new`
  lines), so read that block and update your picture of the file: the
  node's braces now carry the pick, and your next edit of that node builds
  on the printed line. `--disconnect` prints the same block for the removal.
- `--set name=value` fills an acquisition field, and `--set-env
  NAME=ENV_VAR` reads the value from an environment variable so a secret
  never rides the command line. An explicitly empty `--set` or `--set-env`
  value is an error, not a silent skip. Any flag that shapes a new
  connection (`--set`, `--set-env`, `--paste`, `--mint`, `--label`,
  `--permissions`, `--shared-app`) skips the stored-connections menu and
  goes straight to connecting.
- `--forget <id>` deletes a stored connection and also clears every node
  in the current project that pointed at it (under `--json` it reports
  `{"forgot": ..., "cleared": [...]}`; declining reports
  `{"forgot": null, "cleared": []}`). `--upgrade` and `--disconnect`
  manage the rest.
- `--json` works with the flag-driven actions only; the interactive
  walkthrough refuses it. Prompts print to stderr, and any non-interactive
  run (any of stdin, stdout, stderr piped) with a prompt pending fails
  immediately, naming the flag to pass, so a scripted run never hangs.

The split for you: `--list` and picking a stored connection are yours.
Entering a new credential is the user's to run in their own terminal,
interactive by design, or with `--set-env` reading from their environment;
you hand them the exact command instead of ever seeing the key.

## What the banners mean

When a connection is picked, the consuming nodes check it live. A red line
on a node body like "'<input>' needs permission <scope>; the picked
connection does not hold it. Reconnect or upgrade it on the access node."
means exactly what it says. When the provider verified which permissions the
connection holds, a missing one is an error. When the provider only claims
them, a missing one passes with a warning, because refusing would block
every pasted key on every service that reports nothing.

A revoked or expired credential surfaces as a loud "needs reconnecting"
error naming the fix, never a silent retry.

## People in the loop

A `HumanQuery` node suspends the run and asks a person a form; a
`HumanTrigger` is a form a person submits to start a run. The question
reaches people through the **weft browser extension**:

1. Build it once: `./setup.sh --browser --no-sign` in the weft checkout
   (needs Node 20+ and pnpm; the default install skips it because signing
   is slow).
2. Load it: Chrome-family, "Load unpacked" from `chrome://extensions`
   picking the folder under `extension-browser/build/`; Firefox, a
   temporary add-on from `about:debugging`, or the signed `.xpi` when
   signing was left on.
3. Connect it: `weft token mint --name "my laptop"` prints a connect URL
   exactly once (the server stores only a hash; lost means mint another
   and `weft token revoke` the old one). Paste it into the extension's
   popup.

A token with no scope sees every task of the tenant. Handing one to
somebody else: `weft token mint --name "reviewer" --projects <id> --tags
approvals` narrows it to projects and task tags. `weft token ls` and
`weft token revoke <id>` manage them. The extension is one client of the
token's doors, which list and fire any signal kind that renders for
consumers; for building your own (a website, a bot, another extension),
read the `weft-consumers` skill.

While a question waits, the node sits in its cyan waiting state in the
graph, the worker has exited, and the wait costs one row in a table. The
answer resumes the run from exactly where it stopped, seconds or weeks
later.

## A public address

Webhook-style triggers (an `ApiEndpoint`, a Drive watch, a Slack app
installed in other workspaces) need the runtime reachable from the
internet: `weft daemon start --public-url` tunnels a public base and the
trigger surfaces get real URLs (shown in the trigger node's live feed in
the graph). Without it, polling triggers (Telegram, email, sheets, RSS,
cron) and everything local still work. With a public address, the `url` on
a file marker is a link under it, and a fetch answered `403` with the text
`error code: 1010` is Cloudflare's Browser Integrity Check refusing the
client (Python's `urllib` is one it refuses), never weft: the fix is a
Cloudflare configuration rule on the user's side, and the book's public
address page walks through it.
