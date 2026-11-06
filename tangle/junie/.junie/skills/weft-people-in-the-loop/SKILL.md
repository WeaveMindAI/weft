---
name: weft-people-in-the-loop
description: "Read when a program asks a person something or a person starts a run (HumanQuery, HumanTrigger), or the user asks about the browser extension or a task token: how a waiting question reaches people, building, loading and connecting the extension, minting and scoping a token, and what a waiting run costs."
---

# People in the loop

A node parks its run on a person's answer by registering a question and
waiting for it, and any node can do that for its own service. `HumanQuery`
is the general form node that asks one; `HumanTrigger` is a form a person
submits to start a run instead. `AnswerAgent` turns a coding agent's question (from `AskAgent`) into
a form, with the conversation beside it when `history` is wired, and keeps
Submit off until every question has an answer. A parked question reaches people through the
weft browser extension:

1. Build it once: `./setup.sh --browser --no-sign` in the weft checkout
   (needs Node 22.12+ and pnpm; the default install skips it because signing
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

A run waiting on its caller's next message (a socket, or a held route whose
caller is connected and says nothing) stays on its worker. On a cloud
install it costs the database nothing while the caller is silent, though
the worker holding the caller stays up: the worker stops reporting to weft,
so the database can sleep, and it tells weft the run is alive until the
run's hour or the trigger's `maxSessionSecs` runs out, whichever comes
first. A silent caller is never cut for being silent. Cloud Run also cuts a
worker's connection to weft an hour after that connection opened. A worker
whose runs all only wait (on a silent caller, or on a person while the run
cannot pause) opens no new connection, since that would read the database.
A minute before the cut it stops: the runs that can pause let go of the
worker and keep waiting, and the others end naming the limit (a caller
still connected gets `504`, a socket closes `1011`). On your machine there is no hour and no cut: a
run waiting on a silent caller keeps its worker reporting every second,
unless the route or socket sets `maxSessionSecs`, in which case the worker
goes quiet until that runs out.
