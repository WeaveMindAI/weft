---
name: weft-connections
description: "Read when the user asks about connections, API keys, sign-ins, permissions, the browser extension, human tasks, or a public URL: the connect flow door by door, what each banner means, tokens and scoping, and how HumanQuery tasks reach people. Also read it when activation fails with \"needs your weft to be reachable from the internet\", which this skill diagnoses and walks through."
---

# Connections and people

A [connection] is an account hooked up to an outside service: an OAuth
sign-in, a pasted API key, a mail server login, all one concept. The
runtime's access store holds the secret; the project's source holds only a
bare id, so a credential never reaches git history. What flows through
the graph is an `Access` value, a sealed handle that resolves to the
signed-in client when a node fires, never a secret.

An [access node] is any node whose metadata carries a `service` block: that
block is what makes it the place a [connection] is picked (`TelegramAccess`
is one). `--compact` strips the block, so you read it in the node's
`metadata.json`. Do not go by the `access` tag in the listing, which several
access nodes do not carry: a model provider holds a connection exactly the
same way and is tagged by its service instead. One with
a required, unpicked [connection] pins open in the graph (expanded,
collapse disabled, "Pick a connection first") until one is picked; a
`connection_optional` node never pins. A [door] is one way of adding a
[connection]: **shared** (a credential this weft holds) or **own** (the
user's own); the editor flow below has both in full.

## Your part and the user's part

You list stored connections and pick them. The user enters a new
credential in their own terminal, and you hand them the exact command. If
you catch yourself asking the user to paste a key into the chat, or typing
one yourself, stop and write: "Wait. The key never passes through me."
Then hand them the `weft connect` command below, with `--set-env` when the
value is in their environment.

## `weft connect`, the terminal flow

Every Connect panel action exists as a CLI verb, so a user without VS
Code is never stuck. `weft connect` with no arguments opens
interactive menus: first the project's [access node]s (including nodes
inside `@include`d files; each file is one target, and `--node` takes a
bare id or `file.weft:node` when the id is ambiguous), then the [door]s: paste a key (hidden input), browser
sign-in (URL printed, polled), the one-click shared app, `--mint` for a
created own app. A pick is written into the `.weft` source through the
compiler's structural edit, exactly as the editor writes it, so it
survives and diffs like any edit.

The flags you run:

- `--list` works from anywhere: inside a project it lists that project's
  [access node]s with their stored connections (the picked one marked);
  outside a project, or in one with no [access node], every stored
  [connection] across all services.
- `--node <id> --grant <grant>` picks a stored [connection]; no secrets
  travel. It edits the `.weft` source and prints the edit as an edit tool
  would (`main.weft:15`, then `- old` and `+ new` lines). You read that
  block and update your picture of the file: the node's braces now carry
  the pick, and your next edit of that node builds on the printed line.
  `--disconnect` prints the same block for the removal.
- `--forget <id>` deletes a stored [connection] and clears every node in
  the current project that pointed at it (under `--json` it reports
  `{"forgot": ..., "cleared": [...]}`; declining reports
  `{"forgot": null, "cleared": []}`). `--upgrade` manages the rest.
- `--json` works with the flag-driven actions only; the interactive
  walkthrough refuses it. Prompts print to stderr, and any non-interactive
  run (stdin, stdout or stderr piped) with a prompt pending fails at once,
  naming the flag to pass.

The flags the user runs, in the command you hand them:

- `--set name=value` fills an acquisition field; `--set-env NAME=ENV_VAR`
  reads the value from an environment variable so a secret never rides
  the command line. An explicitly empty `--set` or `--set-env` value is an
  error, not a silent skip.
- Any flag that shapes a new [connection] (`--set`, `--set-env`, `--paste`,
  `--mint`, `--label`, `--permissions`, `--shared-app`) skips the
  stored-connections menu and goes straight to connecting.

## The editor flow, door by door

The user expands the [access node] in the graph and clicks its
"Connect <Service>..." field. The widget lists existing connections first
("identity / what it can do", with Change and Disconnect), then
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
error naming the fix, never a silent retry.

## People in the loop

A node parks its run on a person's answer by registering a question and
waiting for it, and any node can do that for its own service. `HumanQuery`
is the general form node that asks one; `HumanTrigger` is a form a person
submits to start a run instead. A parked question reaches people through the
weft browser extension:

1. Build it once: `./setup.sh --browser --no-sign` in the weft checkout
   (needs Node 20+ and pnpm; the default install skips it because signing
   is slow).
2. Load it: Chrome-family, "Load unpacked" from `chrome://extensions`
   picking the folder under `extension-browser/build/`; Firefox, a
   temporary add-on from `about:debugging`, or the signed `.xpi` when
   signing was left on.
3. Connect it: `weft token mint --name "my laptop"` prints a connect URL
   exactly once, and the bare [token] on the line after it (the server
   stores only a hash; lost means mint another and `weft token revoke`
   the old one). Paste the URL into the extension's popup.

A [token] with no scope sees every task of the tenant. To hand one to
somebody else, `weft token mint --name "reviewer" --projects <id> --tags
approvals` narrows it to projects and task tags. `weft token ls` and
`weft token revoke <id>` manage them. The extension is one client of the
HTTP doors a [token] opens, which list and fire any signal kind that
renders for consumers; to build your own client (a website, a bot,
another extension), read the `weft-consumers` skill.

While a question waits, the node sits in its cyan waiting state in the
graph, the worker has exited, and the wait costs one row in a table. The
answer resumes the run from where it stopped, seconds or weeks later.

## A public address

Webhook-style triggers (an `ApiEndpoint`, a Drive watch, a Slack app
installed in other workspaces) need the runtime reachable from the
internet. Polling triggers (Telegram, email, sheets, RSS, cron) and
everything local work without one.

**The error, verbatim.** When a project with a dial-in trigger is
activated on a weft the internet cannot reach, activation refuses with:

> this trigger needs '<service>' to deliver events TO your weft, which
> requires your weft to be reachable from the internet, and it is not. If
> you accept making its public trigger surface reachable, rerun
> `./setup.sh --public-url` and re-activate; or use a connection that
> supports dialing out (your own provider app), where the service offers
> one.

If you see that, or a user pastes it, this is the cause and nothing else.
It is not a broken credential, a wrong path, or a bad subscription. Say so
in one sentence before doing anything else, because the wording sends
people hunting through their provider settings.

**Walking a user through it.** The command is `./setup.sh --public-url`,
always. Never `weft daemon start --public-url`: it exists, but every
message weft prints names setup.sh, and the user should be typing the same
thing the tool told them. Steps:

1. Say what it will do: open a Cloudflare quick tunnel so the provider can
   reach their machine, and expose only the event, signal, file-link and
   OAuth-callback routes. The management API stays unreachable. The
   book's [public address page](https://weavemindai.github.io/weft/connections/public-address.html)
   lists the exact routes; offer it, do not paste it.
2. Get their consent before running it, because it makes a surface on
   their machine reachable from the internet. This is one of the
   `ask`-first moves.
3. Run `./setup.sh --public-url`, then `weft daemon status` to read back
   the hostname it minted.
4. Re-activate the project. If the same error comes back, the transport
   was never the problem; go and read the trigger's own metadata for
   which transports its connection actually supports.

**The alternative, and prefer it when it exists.** Several services can
dial out instead. For Slack, pasting an app-level token with
`connections:write` on the connection switches it to Socket Mode and drops
the public-address requirement entirely. Offer that first: it needs no
tunnel and nothing on the user's machine becomes reachable.

**A quick tunnel's hostname changes** whenever the tunnel restarts, so any
URL the user registered by hand at a provider goes stale. If they are
registering webhook URLs anywhere, they want the named-tunnel setup
(`WEFT_PUBLIC_TUNNEL_TOKEN` plus `WEFT_PUBLIC_TUNNEL_HOSTNAME`, both
required, hostname HTTPS with no port or path), which is on the same book
page.

**Google Drive has a second requirement** on top of the address: it must
be on a domain verified in the app's Google console, or Google refuses to
create the watch. That refusal comes from Google, not weft, and no amount
of tunnel work fixes it.

**A `403` carrying `error code: 1010`** on a file link is Cloudflare's
Browser Integrity Check refusing the client (Python's `urllib` is one it
refuses), never weft. The fix is a Cloudflare configuration rule on the
user's side, and the book's public address page walks through it.
