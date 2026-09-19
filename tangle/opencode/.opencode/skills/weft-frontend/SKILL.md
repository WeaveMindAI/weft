---
name: weft-frontend
description: "Read when the user wants a page, app or site for the program, and before dispatching the frontend-builder: the verified scaffold commands, the default stack (pnpm, SvelteKit, PostgreSQL, BetterAuth, shadcn-svelte), calling the program's own routes and signal doors, one shared PostgreSQL, where the api token lives, pictures as links, and the build."
---


# The frontend

[the frontend] is the pages where a person starts a run, answers a parked
question, or reads what came out. It lives in `front/`, with its own
toolchain, as a client of [the program], never a second backend.
[the program] is the weft graph in `src/`, with its `Route` nodes (the
`weft-api` skill) and its signals (the `weft-consumers` skill). The `frontend-builder` specialist builds [the frontend] and reads
this skill; Tangle reads it whenever the request touches [the frontend].

## The default stack

If the user names a stack, that stack wins: you never override a technology
the user asked for. If they name nothing, you build with these and ask no
question about them:

- **pnpm**, the package manager, and the only one you use.
- **SvelteKit**, the framework (Svelte).
- **PostgreSQL**, the database.
- **BetterAuth**, the authentication.
- **shadcn-svelte**, the components.

You add a piece only when the request needs it.

## Starting the project

Every command below runs without a prompt; this exact chain has been run and
builds in under three seconds. You run it from the project root, one line at
a time, each with a timeout of at most 60 seconds:

```bash
pnpm dlx sv@0.17.0 create front --template minimal --types ts --no-add-ons --no-install
pnpm dlx sv@0.17.0 add tailwindcss="plugins:none" --no-install --cwd front --no-git-check --no-download-check
cd front && pnpm add clsx tailwind-merge && pnpm install
```

The Tailwind add-on writes `src/routes/layout.css`. `shadcn-svelte init`
cannot be made quiet (it asks for a preset whatever flags it gets), so you
never run it: you write the two files it would have written, then add
components one at a time with `--yes`:

`front/components.json`:

```json
{
  "$schema": "https://shadcn-svelte.com/schema.json",
  "tailwind": { "css": "src/routes/layout.css", "baseColor": "zinc" },
  "aliases": { "components": "$lib/components", "utils": "$lib/utils", "ui": "$lib/components/ui", "hooks": "$lib/hooks", "lib": "$lib" },
  "typescript": true,
  "registry": "https://shadcn-svelte.com/registry"
}
```

`front/src/lib/utils.ts`:

```ts
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
	return twMerge(clsx(inputs));
}

export type WithoutChild<T> = T extends { child?: unknown } ? Omit<T, 'child'> : T;
export type WithoutChildren<T> = T extends { children?: unknown } ? Omit<T, 'children'> : T;
export type WithoutChildrenOrChild<T> = WithoutChildren<WithoutChild<T>>;
export type WithElementRef<T, U extends HTMLElement = HTMLElement> = T & { ref?: U | null };
```

Then, still in `front/`:

```bash
pnpm dlx shadcn-svelte@1.6.1 add button card input --yes --no-deps-install && pnpm install
pnpm run build
```

If any command asks a question anyway, you kill it and report what it asked;
you never answer it. A build takes seconds: you give it 30, and if it
overruns, you kill it and report the output.

## Reaching the program's own infrastructure

[the program] sometimes runs infrastructure of its own: a database, a cache, a
broker, whatever its author gave it. When [the frontend] needs to speak to one
of those directly, in that thing's own protocol, it goes through [the door].

[the door] is a piece of [the program]'s infrastructure that its node declared
reachable from this machine. You find them yourself:

````
weft infra list-doors
````

Each line is a node, one of its endpoints, and the address it answers on
(`db.sql  127.0.0.1:30080`). Nothing is listed unless the node that runs it
declared it reachable, so the listing answers "what is reachable from here".
Empty means nothing is, and that is not a thing you can change from here.

**A door is an address, not a key.** Being reachable and being allowed in are
different questions, and the listing only answers the first: nearly everything
worth reaching still asks you to prove who you are. So a listed door with no
credential is not something to work around, it is the second half of the job,
and you ask the orchestrator for it rather than hunting for it yourself.

Where the credential comes from is the node's business, and the node says so
on its card in the graph: the readouts there are how a piece of infrastructure
hands out or resets what it needs, and the node's own description in the
catalog names what it offers. A secret it is showing once renders masked, with
a button to reveal and a button to copy. That is the path you ask for, and
the orchestrator walks the user through it and hands you the result. You put
it in [the frontend]'s server environment, never anywhere a browser can read.

**If you need a door that is not listed, you report it and stop on that part.**
Whether a piece of infrastructure is reachable is part of what [the program]
is, written in the node that runs it, so it is the orchestrator's to change and
never yours. Say which infrastructure you need and why, and the orchestrator
either makes it reachable or builds what was missing and dispatches you again.

You never go round it. If you catch yourself running `kubectl`, reading a
node's source for a password, or opening a shell in a container, stop and write
verbatim "Wait. A door is listed or it does not exist." Then report and work on
something else meanwhile.

**You never stand up your own copy of something [the program] runs**, and you
never tell the user the two halves cannot share it. A second database beside
the program's, a second cache, a second broker: each is two sources of truth
where the user asked for one, and it is the orchestrator's call, never yours.

### The database, which is the case you hit most

If [the program] runs a database of its own (an infra node that hands out an
access to it, which most programs carry as `PostgresDatabase`), [the
frontend] puts its own tables in that database, under their own names,
through [the door]. A row written by one half is visible to the other.

BetterAuth's users are the people who log in to the pages, separate from the
program's connection store. You give BetterAuth [the door], and you put it in
[the frontend]'s server environment, never anywhere a browser can read it.

A page that SHOWS program data still reads it through [the program]'s routes
or signal doors, never out of the program's rows directly. The door is for
[the frontend]'s own tables; the program's data has an interface, and that
interface is the routes.

## The frontend talks to the program as its API

[the program] is the backend. [the frontend] reaches it two ways, both the
program's own surface, never its internals:

- **The program's HTTP routes.** If [the program] answers calls at an
  address (any node that claims one, `Route` being the plain case, plus the
  group behind it), you call them by URL with `fetch` like any REST
  service. You reach for them first when a page needs data the program
  already produces.
- **The program's signals.** A parked question a person answers, a trigger
  with no HTTP route, a stored file: those come through the signal doors of
  the `weft-consumers` skill. You list what the api token may see, draw each
  entry by its `kind`, fire it with the signal token from the listing under
  the field `key` (a parked question disappears once answered; a trigger
  stays listed), and ask the files door for every stored file as you render
  it. On any failure you show the store's message text, never a broken image
  and never a silent no-op.

The api token lives server side: you keep it in SvelteKit's server-only
environment, call the doors from a `+page.server.ts` or a `+server.ts`, and
never ship it to the browser. A program route that needs the token is called
server side too.

A page never calls a service [the program] does not. If a page needs a
capability [the program] exposes as neither a route nor a signal, that is a
boundary to widen in the weft graph: you say it back to Tangle. If you catch
yourself faking it in frontend code, writing a stand-in server, or routing
around [the program] to a side service, stop and write: "Wait. The program
is the backend." Then report the missing route or signal.

## Holding the token is not the same as being allowed to use it

Keeping the api token server side answers one question: can this server call
[the program]? It says nothing about the other one: is the person who just hit
this route allowed to make that call? A `+server.ts` holding the token calls
[the program] for whoever reaches it, a stranger included, so an unchecked
server route is a public button on a privileged action.

So before a server route uses the token, it checks who is asking, and it checks
the visitor's session. Never whether a key is configured, never whether the
token exists: those are facts about the server, and a page that reads them as
identity lets anybody act as the owner.

The check goes on anything that deletes, sends, spends, moderates, approves,
cancels, or writes on somebody else's behalf, and on anything that starts a run
that costs money. Reading data the page shows everyone does not need it. When
you cannot tell, the check goes on.

With BetterAuth on the shared Postgres, that is a session read at the top of
the handler, with nothing below it running for a caller who did not pass:

```ts
// front/src/routes/api/moderate/+server.ts
import { error, json } from '@sveltejs/kit';
import { auth } from '$lib/server/auth';

export async function POST({ request }) {
	const session = await auth.api.getSession({ headers: request.headers });
	if (!session) error(401, 'Sign in first');
	const item = await loadItem(await request.json());
	if (item.ownerId !== session.user.id) error(403, 'Not yours');
	// only here does the server reach for the api token
}
```

If you catch yourself working out who the caller is from something the server
holds anyway (a configured key, an environment variable, whether the token is
set), stop and write: "Wait. That is the credential, not the person." Then
read the session.

## House rules

- `front/` is the frontend's whole world: its `package.json`, its
  `node_modules`, its build output. You never put frontend files in the weft
  source tree, and never point a weft `@file` marker into `front/`.
- The build is `pnpm install` then `pnpm run build`; the dev server is
  `pnpm run dev`. You hand the user those exact commands in the report.
- A picture [the program] answers with arrives as `{ url, mimeType,
  filename, sizeBytes }`: you put the `url` in an `<img>`. A picture the page
  sends goes in the JSON body as a `data:` URL on the key the route declares
  as `Image`; never as multipart, never as a separate upload.
- Seed data and demo accounts are fine; the report says they are seed data,
  and no page depends on one to look alive.
- Plain: shadcn-svelte components, little custom CSS, no design system the
  request did not ask for.

## Proving the frontend is proving the frontend

[the proof] is that the page builds and works as a client of [the program];
whether a node returns the right value is the orchestrator's and the
`node-smith`'s to establish before this work starts. A wrong value from
[the program] is a finding in the report, never something to test around or
fix in the page. For what the proof has to contain, and what a stand-in
costs you, go and read the `frontend-builder` agent file.

[the frontend] is built from [the brief] alone and never waits on
[the program]: you never poll a file, loop until a marker appears, or read
another agent's transcript.

## Stop when the frontend is done

A page that starts a run and reads its result is often the whole job. You
build that, prove it against a real run, and stop. You do not grow
[the frontend] past what the person using it needs.
