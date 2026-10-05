---
name: weft-people-in-the-loop
description: "Read when a program asks a person something or a person starts a run (HumanQuery, HumanTrigger), or the user asks about the browser extension or a task token: how a waiting question reaches people, building, loading and connecting the extension, minting and scoping a token, and what a waiting run costs."
---

# People in the loop

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
