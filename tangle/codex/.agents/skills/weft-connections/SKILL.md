---
name: weft-connections
description: "Read when the user asks about connections, API keys, sign-ins or permissions: your part and the user's, the connect flow in the terminal and door by door in the editor, and what each banner means."
---

# Connections

A [connection] is an account hooked up to an outside service: an OAuth
sign-in, a pasted API key, a mail server login, all one concept. The
runtime's access store holds the secret, and the install keeps which
connection each [access node] uses (its pick); the project's source holds
neither, so a credential never reaches git history and each install (this
machine, a cloud one) keeps picks of its own. What flows through
the graph is an `Access` value, a sealed handle that resolves to the
signed-in client when a node fires, never a secret.

An [access node] is any node whose metadata carries a `service` block: that
block is what makes it the place a [connection] is picked (`TelegramAccess`
is one). `--compact` strips the block, so you read it in the node's
`metadata.json`. Do not go by the `access` tag in the listing, which several
access nodes do not carry: `CustomProvider`, for one, holds a connection
exactly the same way but is tagged `provider` and `model`. One with
a required, unpicked [connection] pins open in the graph (expanded,
collapse disabled, "Pick a connection first") until one is picked; a
`connectionOptional` node never pins. A [door] is one way of adding a
[connection]: **shared** (a credential this weft holds) or **own** (the
user's own); the editor flow below has both in full.

## Your part and the user's part

You list stored connections and pick them. The user enters a new
credential in their own terminal, and you hand them the exact command. If
you catch yourself asking the user to paste a key into the chat, or typing
one yourself, stop and write: "Wait. The key never passes through me."
Then hand them the `weft connect` command below, with `--set-env` when the
value is in their environment.

For a key from another service, the user always runs `weft connect
<node>` (plus `--label`, `--on <target>` when they apply) and types
the key at its hidden prompt. You never offer them the form that puts the
key on the command line (`--set`), or any other place to keep it (a secrets
script of their own, an env file).

## `weft connect`, the terminal flow

Every Connect panel action exists as a CLI verb, so a user without VS
Code is never stuck. `weft connect` with no arguments opens
interactive menus: first the project's [access node]s (including nodes
inside `@include`d files; each file is one target, and the node is a
bare id or `file.weft:node` when the id is ambiguous), then the [door]s: paste a key (hidden input), browser
sign-in (URL printed, polled), the one-click shared app, `--mint` for a
created own app. A pick is kept by the install, per place of the node (a
file included twice gives its node two places, each picked on its own),
never written into the `.weft` source: a connection's id means nothing on
another install, so `--on <target>` picks for that one. A connection
written in the source (`account: {"id": ...}`, an older weft's way) is
refused by the build and by `weft connect`: the line is erased and the node
connected again.

The flags you run:

- `--list` works from anywhere: inside a project it lists that project's
  [access node]s with their stored connections (the picked one marked);
  outside a project, or in one with no [access node], every stored
  [connection] across all services.
- `<node> --grant <grant>` picks a stored [connection] on this
  install; no secrets travel and no file changes. A live trigger that
  reads the node is set up again with it, and the answer names it.
  `--disconnect` forgets the pick.
- Each listed [connection] shows its account and its name. When a service
  has several, pick the one whose name matches this project; if none
  clearly does, ask the user which one in one plain sentence that gives
  the names.
- `--forget <id>` deletes a stored [connection] and every pick of it
  goes with it (under `--json` it reports the current project's places
  that lose their pick: `{"forgot": ..., "cleared": [...]}`; declining reports
  `{"forgot": null, "cleared": []}`). `--upgrade` manages the rest.
- `--json` works with the flag-driven actions only; the interactive
  walkthrough refuses it. Prompts print to stderr, and any non-interactive
  run (stdin, stdout or stderr piped) with a prompt pending fails at once,
  naming the flag to pass.

The flags the user runs, in the command you hand them:

- `--set name=value` fills an acquisition field; `--set-env NAME=ENV_VAR`
  reads the value from an environment variable so a secret never rides
  the command line. A key from another service goes in at the hidden
  prompt instead, never through `--set`. An explicitly empty `--set` or `--set-env` value is an
  error, not a silent skip.
- `--way <id>` picks which way to sign in when a service offers several (a
  subscription's token or an API key, say); without it the terminal asks. A
  way marked local only (a subscription login) is offered on this machine's
  install and refused on a cloud install.
- `--label <name>` names the new [connection]; without it, it is named
  after the service (or the app it signs in through). Suggest a name with
  the project in it (`acme-scraper`) for a key only this project should
  use; a key meant for every project, like a model provider's, keeps the
  service's name.
- Any flag that shapes a new [connection] (`--set`, `--set-env`, `--paste`,
  `--mint`, `--label`, `--permissions`, `--shared-app`) skips the
  stored-connections menu and goes straight to connecting.

## On a cloud install

A connection this machine already holds is copied to a cloud install with
`weft migrate connections`, never made again (the weft-cloud-install skill,
"Moving an app from this machine").

Once a cloud install's own domain is live, its sign-in callback moves to
`https://<domain>/access/oauth/callback`. Each provider app only sends people
back to the addresses it lists, so for each service in `callbackServices`
(from `weft domain list --json`, which the deployer reports) you give the
user one line: "In your <service> app's dashboard, add
`https://<domain>/access/oauth/callback` to the allowed redirect URLs." A
connection already made keeps working, since a refresh needs no callback;
only a new sign-in goes through the new one.

## The editor flow, door by door

The user expands the [access node] in the graph and clicks its
"Connect <Service>..." field. The widget lists existing connections first
("account / name / what it can do", with Change and Disconnect), then
"+ Add a connection" with up to two [door]s. A [door] the service does not offer is
hidden, never greyed out.

- **shared**: a credential this weft holds. Either "Sign in with <Service>"
  through a registered app (one click, the provider's consent page opens
  in the browser, the panel waits and updates on its own), or "Use ours
  (uses your credits)" for key-based services: calls on that [connection]
  spend the runtime's credits, and it says so.
- **own**: the user brings or creates their own. One page: the permissions
  to ask for (checkboxes), an optional "Create it for me" mint button, a
  generated step-by-step guide with the callback URL to register, the app
  fields or a paste-a-credential section, then "Sign in" or "Connect".

After connecting, the field shows "Connected as <identity>". Some services
(Google class) allow many grants; others (Slack bot, GitHub App) allow one
grant per account, and the widget says so when a second connect would
displace the first ("Upgrade this connection...").

## What the banners mean

Once a [connection] is picked, the consuming nodes check it live. A red
line on a node body like "'<input>' needs permission <scope>; the picked
connection does not hold it. Reconnect or upgrade it on the access node."
is literal. When the provider verified which permissions
the [connection] holds, a missing one is an error. When the provider only
claims them, a missing one passes with a warning, because refusing would
block every pasted key on every service that reports nothing.

A revoked or expired credential surfaces as a loud "needs reconnecting"
error naming the fix, never a silent retry. A provider that is busy or
down while weft refreshes a sign-in (it answers 408, 429 or a 5xx, or
does not answer) is not a dead credential: the node fails with the
provider's own answer (`token endpoint answered <status>: ...` or `token
endpoint unreachable: ...`), never with "needs reconnecting", and the refresh is tried
again the next time the connection is opened.
