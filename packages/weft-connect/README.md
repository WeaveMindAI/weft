# @weft/connect

The connect page for one service: the list of connections somebody may pick
from, the doors a service offers (the shared one, or your own), a paste or a
browser sign-in, and forgetting a connection. Beside it, the control that
fills a list field (a spreadsheet, a channel, a model) from the service. The
weft editor's connection picker and list fields are built on them, and so is
an instance's own settings page.

It is two halves. `src/core` is plain TypeScript: the recipe readers (what a
service's recipe says about its fields, its guide and its permissions), the
browser sign-in wait, and the `ConnectTransport` interface a host implements to
answer the page's questions (and `ResourceTransport` for a list field's).
`src/svelte` has the components, and `src/server` the pass-through a
website's server mounts so its pages reach weft through it.

## Where it talks

A page asks weft only a few things (which connections, which doors, connect,
forget, how a sign-in went), so a host plugs in by answering them:

- the editor answers through its host bridge, as the program's author
  (`packages/weft-graph/.../editor-connect.ts`);
- an instance's page answers with `InstanceDoor`, which calls the
  dispatcher's instance door with that instance's token.

On a website, the visitor's browser never calls the dispatcher itself: it
calls its own site at `/weft/...`, and the site's server passes the call on.
The dispatcher's address is often one the browser cannot reach (a loopback
port on another machine, as with a Windows browser in front of a WSL
install, or a dispatcher that is not public at all), and the site's server
always can. The pass-through ships in `src/server`.

In a weft project's frontend, `weft connect-lib` copies the library into
`front/src/lib/weft-connect` (or the folder `--into` names), and you import
it from `$lib/weft-connect`, `$lib/weft-connect/svelte` and
`$lib/weft-connect/server` instead of the package names below.

If you are mounting it in SvelteKit, the pass-through is one route, and the
dispatcher's address lives only in the server's environment
(`WEFT_DISPATCHER_URL=http://127.0.0.1:14111` for a local install):

```ts
// front/src/routes/weft/[...path]/+server.ts
import { env } from '$env/dynamic/private';
import { weftPassThrough } from '@weft/connect/server';
import type { RequestHandler } from './$types';

const pass = weftPassThrough({ dispatcher: () => env.WEFT_DISPATCHER_URL });

export const fallback: RequestHandler = ({ request, params }) => pass(request, params.path);
```

It passes on the instance door (`/instance/...`), the signal doors
(`/signal/...`), the token's listings, displays and files
(`/signal-token/...`), the program's own routes
(`/connect/<tenant>/<path>`), the picture links in their answers
(`/public/files/<token>`) and a field's file chooser
(`/access/picker/<state>` and the result it posts back), and answers 404 for
anything else. It carries the caller's `Authorization`, `Weft-Instance-Token`,
body and content type, and hands back the dispatcher's status. It also tells
the dispatcher which address the browser used (your site's host and the
`/weft` mount), so a link the dispatcher hands back, like a chooser page,
points at your site and not at the address your server reaches weft on.

Every door streams its body through as it arrives, and hands a redirect
back to the browser as it is.

## A socket that comes back

`openLiveSocket` (in the core) opens a socket to one of the program's
`Socket` routes and reconnects whenever it closes, which on a cloud install
happens at least once an hour. It keeps a session id and sends it as the
`session` query parameter on every connection, so the program can read its
conversation back from its own storage; each connection is a run of its own.
It stops when the page calls `close()`, when the program closes the socket
with code `4000` (a run that simply ends closes with `1000`, which only ends
that connection), or when the route refuses the `headers` it was given with
a 4xx. Give it `headers` (an instance token) on a route that checks callers:
a browser cannot put them on a socket's opening request, so it asks for the
socket's address with a plain `GET` carrying them first. A socket goes to
the install's public address (`WEFT_PUBLIC_URL`), not through the
pass-through, which carries no sockets.

The site's cookies stay on the site, and so does `Weft-Instance`, which only
the site's server may send. If `WEFT_DISPATCHER_URL` is unset it answers 500
naming it; if the dispatcher does not answer, 502 naming the address it tried.

If your site sends everyone who is not signed in to its login page (a check
in `hooks.server.ts`, say), let `/weft/...` past that check. Each of those
calls carries an instance's own weft token, and weft checks it. Behind a login check, a call from
a page without a site session would get the login page back instead of
its answer.

A page then builds its door with the instance's token alone, and it calls
`/weft/instance/...` on its own site:

```ts
import { InstanceDoor } from '@weft/connect';
import { InstanceSettings } from '@weft/connect/svelte';

const door = new InstanceDoor(instanceToken);
// <InstanceSettings {door} />
```

If the page reaches the dispatcher some other way (the browser extension
does, with the address the token was minted at), pass it as
`new InstanceDoor(token, { base: dispatcherUrl })`.

`InstanceSettings` lists every field the program gives each instance its own
value for (the ones it writes `@instance_filled`), grouped by step, each with
its own control: the connection picker for a connection, the searchable list
and the provider's chooser for a list field, a plain input for anything else.
It stores what is given for that instance, and when a value it changed is one
a live trigger of that instance reads, the trigger is set up again before the
save answers and the page says so. An instance's page never sees the shared
key: it spends the program author's credit.

Each step is titled with the program's own label and each field with the
node's own name for it (a text step's field reads "Value"), both written for
the program's author. Name what your visitors read in your own words with
`labels`, keyed by a step's id for its title and by `step.field` for a field;
anything you leave out keeps the program's label:

```svelte
<InstanceSettings {door} labels={{ greet: 'Your greeting', 'greet.value': 'How the bot greets you' }} />
```

If you build your own page instead, `door.fields()` lists the fields,
`door.setValues(set, clear)` stores values, and `ResourceSelect` with
`door.resources(step, field)` draws one list field.

## How it looks

The components use no CSS framework. Set these on any ancestor to match your
page: `--wc-font-size`, `--wc-fg`, `--wc-bg`, `--wc-muted-fg`, `--wc-muted-bg`,
`--wc-border`, `--wc-radius`, `--wc-accent`, `--wc-accent-hover`,
`--wc-accent-fg`, `--wc-danger`, `--wc-warn-fg`, `--wc-warn-bg`, `--wc-ok-fg`,
`--wc-ok-bg`, `--wc-ok-border`, `--wc-guide-bg`, `--wc-go`.

## Tests

`pnpm -C packages/weft-connect test`. Like the graph package, this one has no
install of its own: `setup.sh` points its `node_modules` at the VS Code
extension's.
