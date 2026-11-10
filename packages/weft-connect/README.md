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
(`/connect/<tenant>/<project id>/<path>`), a field's file chooser
(`/access/picker/<state>` and the result it posts back), and the files door:
links to stored files (`/files/runtime/...`) and the steps of a visitor's
upload (`/files/upload/<step>`). It answers 404 for anything else. It
carries the caller's `Authorization`, `Weft-Instance-Token`, body and
content type, plus the `Range` and `If-*` headers a file read sends, and
hands back the dispatcher's status, the `Weft-Run-Id` header a route's
answer carries, and the headers a file read needs (`Content-Range`, `ETag`
and the like). It also tells the dispatcher
which address the browser used (your site's host and the `/weft` mount), so a
link the dispatcher hands back, like a chooser page, points at your site and
not at the address your server reaches weft on.

Every door streams its body through as it arrives, except a part of an
upload: its length is signed into its link, so the pass-through reads it
whole and sends it with that exact length. A redirect goes back to the
browser as it is.

If your project's visitor address is your site (`https://shop.example/weft`),
the links to its files point at your site too, and come back through this
route. The signature in a link is what lets it in, so it needs no token.

## Uploading a visitor's file

If you want a visitor to send a file, however big, have the program hand
the page an upload slot (a string starting `weft-upload:`), then send the
file through it:

```ts
import { uploadFile } from '@weft/connect';

const sent = await uploadFile(slot, file, (done, total) => (progress = done / total));
// Send `sent` to your route in the field it declares as a file.
```

It hashes the file, sends it in parts through `/weft/files/upload/...`, and
answers the slot, which the route then receives as the stored file. If the
install already holds the same content, no byte is sent. A part that does
not land is sent again after asking the install what is missing, a few
times, and then the upload fails naming what went wrong. If the install
refuses the file (too big, or a type the slot does not take), the error's
message is its own sentence, ready to show. Pass `{ base }` if the page
reaches weft somewhere other than `/weft`.

When a route's run fails, your page gets this, and never the reason, which is
yours to read:

```json
{"error": "Something went wrong", "runId": "..."}
```

If you want to show a visitor which run it was, or log it, read the `runId`,
or the `Weft-Run-Id` header that every answer carries, failed or not.
`@weft/connect/server` exports that header's name as `RUN_ID_HEADER` and the body's shape as
`PlainFailure`, and you open the run itself with `weft logs <run id>`.

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
import { DoorSettings } from '@weft/connect/svelte';

const door = new InstanceDoor(instanceToken);
// <DoorSettings {door} />
```

If the page reaches the dispatcher some other way (the browser extension
does, with the address the token was minted at), pass it as
`new InstanceDoor(token, { base: dispatcherUrl })`.

`DoorSettings` lists every field the program gives each instance its own
value for (the ones it writes `@instance_filled`), grouped by node, each with
its own control: the connection picker for a connection, the searchable list
and the provider's chooser for a list field, a plain input for anything else.
It stores what is given for that instance, and when a value it changed is one
a live trigger of that instance reads, the trigger is set up again before the
save answers and the page says so. An instance's page never sees the shared
key: it spends the program author's credit.

Each node is titled with the program's own label and each field with the
node's own name for it (a text node's field reads "Value"), both written for
the program's author. Name what your visitors read in your own words with
`labels`, keyed by a node's id for its title and by `node.field` for a field;
anything you leave out keeps the program's label:

```svelte
<DoorSettings {door} labels={{ greet: 'Your greeting', 'greet.value': 'How the bot greets you' }} />
```

If you build your own page instead, `door.fields()` lists the fields,
`door.setValues(set, clear)` stores values, and `ResourceSelect` with
`door.resources(node, field)` draws one list field.

## An admin page for the program's own keys

If your site's admin should paste the keys the program itself uses (the ones
you would otherwise set with `weft connect`, like the key of a `VapiAccess`
node), give `DoorSettings` a `ProgramDoor` and it becomes that admin page.
It lists every key the program uses on the install. The admin sets one by
pasting it, and weft stores it for the program.

The page needs a connections key: mint it with `weft token mint
--connections` and keep it in the server's environment as
`WEFT_CONNECTIONS_KEY`. Then mount a second pass-through on its own route.
Every call to it takes your own answer to "is this caller my admin?", because
weft cannot tell your admin from your visitors:

```ts
// front/src/routes/weft-admin/[...path]/+server.ts
import { env } from '$env/dynamic/private';
import { weftProgramPassThrough } from '@weft/connect/server';
import type { RequestHandler } from './$types';

const pass = weftProgramPassThrough({
	dispatcher: () => env.WEFT_DISPATCHER_URL,
	connectionsKey: () => env.WEFT_CONNECTIONS_KEY,
});

export const fallback: RequestHandler = ({ request, params, locals }) =>
	pass(request, params.path, { admin: locals.user?.isAdmin === true }); // your own check
```

If `admin` is anything but `true`, it answers 403 saying your admin check
refused the call, and weft never sees it. It passes on only calls under
`/program/` (anything else answers 404), and sends your connections key in
place of any `Authorization` the page sent. If `WEFT_CONNECTIONS_KEY` is not
set, it answers 500 naming it. The admin page
then calls `/weft-admin/program/...` on your site with no key of its own:

```ts
import { ProgramDoor } from '@weft/connect';
import { DoorSettings } from '@weft/connect/svelte';

const door = new ProgramDoor();
// <DoorSettings {door} />
```

If you mount the route somewhere other than `/weft-admin`, pass that path:
`new ProgramDoor({ base: '/your-path' })`.

Pasting a key there stores it and picks it in one go, and if weft refuses the
pick, it deletes the key it just stored and the page shows why. If you build
your own admin page instead, that is the whole of picking: the connect of
`door.connectTransport(field)` answers `picked` with what the pick changed,
and `door.clear(field)` clears it. A `ProgramDoor` has no `save`, since there
is nothing to pick on it but a key you just pasted. Replacing or
clearing a key deletes the old one from weft once nothing else on the install
uses it. If a service signs in through its own page in the browser
instead of taking a pasted key, this page cannot connect it: do that from the
editor's Connect button or with `weft connect`.

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
