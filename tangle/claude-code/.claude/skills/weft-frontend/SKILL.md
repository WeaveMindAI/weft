---
name: weft-frontend
description: "Read when the user wants a page, app or site for the program, and before dispatching the frontend-builder: the verified scaffold commands, the default stack (pnpm, SvelteKit, PostgreSQL, BetterAuth, shadcn-svelte), calling the program's own routes and signal doors, the three variables that say where the program is, one shared PostgreSQL, where the api token lives, pictures as links, the build and its Dockerfile, and the shape for a site whose people each bring their own accounts or settings (members: the manager routes, the member header and token, their settings page)."
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
cd front && pnpm add clsx tailwind-merge && pnpm add -D tw-animate-css shadcn-svelte@1.6.1 && pnpm install
```

The Tailwind add-on writes `src/routes/layout.css`. `shadcn-svelte init`
cannot be made quiet (it asks before it touches the stylesheet whatever flags
it gets), so you never run it: you write the three files it would have
written, then add components with `--yes --overwrite` (without
`--overwrite`, `add` still asks "overwrite all existing files?" as soon as one
of its files is there):

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

`front/src/routes/layout.css`, replacing the one line the Tailwind add-on
wrote. These are the zinc colours `init` writes; the components' classes
(`bg-destructive`, `border-input`, `ring-ring`) name them, so without this
block a destructive button renders with no colour at all:

```css
@import 'tailwindcss';
@import "tw-animate-css";
@import "shadcn-svelte/tailwind.css";

@custom-variant dark (&:is(.dark *));

:root {
	--background: oklch(1 0 0);
	--foreground: oklch(0.141 0.005 285.823);
	--card: oklch(1 0 0);
	--card-foreground: oklch(0.141 0.005 285.823);
	--popover: oklch(1 0 0);
	--popover-foreground: oklch(0.141 0.005 285.823);
	--primary: oklch(0.21 0.006 285.885);
	--primary-foreground: oklch(0.985 0 0);
	--secondary: oklch(0.967 0.001 286.375);
	--secondary-foreground: oklch(0.21 0.006 285.885);
	--muted: oklch(0.967 0.001 286.375);
	--muted-foreground: oklch(0.552 0.016 285.938);
	--accent: oklch(0.967 0.001 286.375);
	--accent-foreground: oklch(0.21 0.006 285.885);
	--destructive: oklch(0.577 0.245 27.325);
	--border: oklch(0.92 0.004 286.32);
	--input: oklch(0.92 0.004 286.32);
	--ring: oklch(0.705 0.015 286.067);
	--chart-1: oklch(0.87 0 0);
	--chart-2: oklch(0.556 0 0);
	--chart-3: oklch(0.439 0 0);
	--chart-4: oklch(0.371 0 0);
	--chart-5: oklch(0.269 0 0);
	--radius: 0.625rem;
	--sidebar: oklch(0.985 0 0);
	--sidebar-foreground: oklch(0.141 0.005 285.823);
	--sidebar-primary: oklch(0.21 0.006 285.885);
	--sidebar-primary-foreground: oklch(0.985 0 0);
	--sidebar-accent: oklch(0.967 0.001 286.375);
	--sidebar-accent-foreground: oklch(0.21 0.006 285.885);
	--sidebar-border: oklch(0.92 0.004 286.32);
	--sidebar-ring: oklch(0.705 0.015 286.067);
}

.dark {
	--background: oklch(0.141 0.005 285.823);
	--foreground: oklch(0.985 0 0);
	--card: oklch(0.21 0.006 285.885);
	--card-foreground: oklch(0.985 0 0);
	--popover: oklch(0.21 0.006 285.885);
	--popover-foreground: oklch(0.985 0 0);
	--primary: oklch(0.92 0.004 286.32);
	--primary-foreground: oklch(0.21 0.006 285.885);
	--secondary: oklch(0.274 0.006 286.033);
	--secondary-foreground: oklch(0.985 0 0);
	--muted: oklch(0.274 0.006 286.033);
	--muted-foreground: oklch(0.705 0.015 286.067);
	--accent: oklch(0.274 0.006 286.033);
	--accent-foreground: oklch(0.985 0 0);
	--destructive: oklch(0.704 0.191 22.216);
	--border: oklch(1 0 0 / 10%);
	--input: oklch(1 0 0 / 15%);
	--ring: oklch(0.552 0.016 285.938);
	--chart-1: oklch(0.87 0 0);
	--chart-2: oklch(0.556 0 0);
	--chart-3: oklch(0.439 0 0);
	--chart-4: oklch(0.371 0 0);
	--chart-5: oklch(0.269 0 0);
	--sidebar: oklch(0.21 0.006 285.885);
	--sidebar-foreground: oklch(0.985 0 0);
	--sidebar-primary: oklch(0.488 0.243 264.376);
	--sidebar-primary-foreground: oklch(0.985 0 0);
	--sidebar-accent: oklch(0.274 0.006 286.033);
	--sidebar-accent-foreground: oklch(0.985 0 0);
	--sidebar-border: oklch(1 0 0 / 10%);
	--sidebar-ring: oklch(0.552 0.016 285.938);
}

@theme inline {
	--color-sidebar-ring: var(--sidebar-ring);
	--color-sidebar-border: var(--sidebar-border);
	--color-sidebar-accent-foreground: var(--sidebar-accent-foreground);
	--color-sidebar-accent: var(--sidebar-accent);
	--color-sidebar-primary-foreground: var(--sidebar-primary-foreground);
	--color-sidebar-primary: var(--sidebar-primary);
	--color-sidebar-foreground: var(--sidebar-foreground);
	--color-sidebar: var(--sidebar);
	--color-chart-5: var(--chart-5);
	--color-chart-4: var(--chart-4);
	--color-chart-3: var(--chart-3);
	--color-chart-2: var(--chart-2);
	--color-chart-1: var(--chart-1);
	--color-ring: var(--ring);
	--color-input: var(--input);
	--color-border: var(--border);
	--color-destructive: var(--destructive);
	--color-accent-foreground: var(--accent-foreground);
	--color-accent: var(--accent);
	--color-muted-foreground: var(--muted-foreground);
	--color-muted: var(--muted);
	--color-secondary-foreground: var(--secondary-foreground);
	--color-secondary: var(--secondary);
	--color-primary-foreground: var(--primary-foreground);
	--color-primary: var(--primary);
	--color-popover-foreground: var(--popover-foreground);
	--color-popover: var(--popover);
	--color-card-foreground: var(--card-foreground);
	--color-card: var(--card);
	--color-foreground: var(--foreground);
	--color-background: var(--background);
	--radius-sm: calc(var(--radius) * 0.6);
	--radius-md: calc(var(--radius) * 0.8);
	--radius-lg: var(--radius);
	--radius-xl: calc(var(--radius) * 1.4);
	--radius-2xl: calc(var(--radius) * 1.8);
	--radius-3xl: calc(var(--radius) * 2.2);
	--radius-4xl: calc(var(--radius) * 2.6);
}

@layer base {
	* {
		@apply border-border outline-ring/50;
	}
	body {
		@apply bg-background text-foreground;
	}
}
```

Then, still in `front/`:

```bash
pnpm dlx shadcn-svelte@1.6.1 add button card input --yes --overwrite --no-deps-install && pnpm install
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
A door that is listed answers only once its infrastructure is running,
and nothing starts a piece the program itself never touches (a database only
the site's sign-in uses): if yours does not answer, ask the orchestrator to
run `weft infra start`.

**A door is an address, not a key.** Being reachable and being allowed in are
different questions, and the listing only answers the first: nearly everything
worth reaching still asks you to prove who you are. So a listed door with no
credential is not something to work around, it is the second half of the job,
and you ask the orchestrator for it rather than hunting for it yourself.

Where the credential comes from is the node's business, and the node says so
on its card in the graph: the readouts there are how a piece of infrastructure
hands out or resets what it needs, and the node's own description in the
catalog names what it offers. If you want values the node shows on its card (a
database's user, or a masked `secret` line like its password) in [the
frontend]'s server environment, one command writes them all, `weft infra env <node> --into <server env file> --set
DATABASE_USER=User --set DATABASE_PASSWORD=Password --set DATABASE_NAME=Database`, each `NAME=Label` taking
the card item under that label (`weft infra show <node>` lists the labels, and
`--as <NAME>` is the short form for the card's only secret). It prints the
plain values it wrote and never a secret's, so a secret never passes through
you. Plain values it cannot write (the door's host and port, from `weft infra
list-doors`) you add to the same file yourself. The file is server-side only,
never anywhere a browser can read.

A secret the node hands out once (a database's password) has usually been
taken by the time anyone looks, so expect `env` to say it was handed over
already. The flow is the same every time: read the card (`weft infra show
<node>` lists its items and its buttons), press the button that issues a new
one if the secret is gone (`weft infra press <node> <action>`), then write the
values with `weft infra env`. The button's warning (`weft infra show` prints it) says what a new secret
cuts off; if something already uses the old one, cutting it off is the user's
call, so you ask the orchestrator before pressing.

Run each of these as one plain command from the project folder, exactly as
written above: no `cd` in front, no `&&`, no pipe, and `--into front/.env`
for the path. The permission checker approves `weft infra ...` only when the
command is written that way; wrapped in anything else, it goes to an
automatic check that refuses writing a password into a file. If one is
refused anyway, you are a subagent: put the exact command in your report and
go on with the rest, and the orchestrator runs it and tells you when the file
is written. Nobody goes round a refusal by reading the secret or copying it
onto a command line; if the orchestrator is refused too, it hands the user
the exact command.

**If you need a door that is not listed, you report it and stop on that part.**
Whether a piece of infrastructure is reachable is part of what [the program]
is, written in the node that runs it, so it is the orchestrator's to change and
never yours. Say which infrastructure you need and why, and the orchestrator
either makes it reachable or builds what was missing and dispatches you again.

You never go round it. If you catch yourself running `docker`, reading a
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

BetterAuth keeps its people in four tables of its own (`user`, `session`,
`account`, `verification`), and they have to exist before the site starts:
without them it logs "Database schema mismatch ... Missing tables" and every
call to it fails. You never write that SQL or startup code yourself, because
BetterAuth's own CLI (the `auth` package; the older `@better-auth/cli` is
deprecated) creates them. Once the database's values are in the server env
file, from `front/`:

```bash
pnpm add better-auth@1.7.6 pg && pnpm add -D auth@1.7.6 @types/pg
pnpm exec auth migrate --yes
```

`migrate` finds the config at `src/lib/server/auth.ts`, reads `front/.env` the
way the server does, creates only what is missing and asks nothing with
`--yes`. On a database that has them already it prints "No migrations needed",
so you run it once after setup, and again whenever you add a BetterAuth plugin
that brings tables of its own. The config it reads, with the names you wrote
with `weft infra env`, plus `BETTER_AUTH_SECRET`, a random value you generate
yourself (`openssl rand -base64 32`) and write into `front/.env`. A build does
not load that file, and BetterAuth refuses to start without a secret, so the
config hands it an obviously fake one while building and the real one
otherwise:

```ts
// front/src/lib/server/auth.ts
import { betterAuth } from 'better-auth';
import { sveltekitCookies } from 'better-auth/svelte-kit';
import { getRequestEvent } from '$app/server';
import { building } from '$app/environment';
import { env } from '$env/dynamic/private';
import pg from 'pg';

export const auth = betterAuth({
	secret: building ? 'build-only-not-a-secret' : env.BETTER_AUTH_SECRET,
	database: new pg.Pool({
		host: env.DATABASE_HOST,
		port: Number(env.DATABASE_PORT),
		database: env.DATABASE_NAME,
		user: env.DATABASE_USER,
		password: env.DATABASE_PASSWORD
	}),
	emailAndPassword: { enabled: true },
	plugins: [sveltekitCookies(getRequestEvent)]
});
```

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

The server finds [the program] in three environment variables, and never in
an address written into the code, because the same frontend runs on this
machine and on a cloud install:

| Variable | What it is | On this machine |
|---|---|---|
| `WEFT_DISPATCHER_URL` | where the server calls routes and doors | `http://127.0.0.1:14111` |
| `WEFT_TOKEN` | the api token | a token from `weft token mint` |
| `WEFT_PUBLIC_URL` | the start of any link a browser follows | `http://127.0.0.1:14111` |

On this machine, 14111 is the default port; if the install was started on
another one, `~/.local/share/weft/ports.json` has it (`public`).

On this machine they live in `front/.env`; on a cloud install the deploy
workflow sets them (the `weft-deploying` skill). You read them with
`$env/dynamic/private`, so a deployed build picks them up when it starts.

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

## When the people using the pages bring their own accounts

Some requests are about other people: "each customer connects their own
WhatsApp", "users log in and use their own OpenAI key", "every client gets a
bot on their own Slack". That is a program with members (the `weft-members`
skill), and the frontend is where those members live. You never build one
project per person, and you never ask each person for a key in a form field.

The shape, unless the user asks for another:

- **The frontend owns the accounts.** BetterAuth on the shared Postgres signs
  people in, and a person's member id is their BetterAuth user id. weft keeps
  no list of members: an id becomes a member the first time the program
  starts something for it.
- **The program has manager routes**, gated by an `ApiKeyAuth` key set whose
  key lives only in the frontend's server environment. They are the only way
  anything starts, stops or removes a member's things: one that brings a
  member on (starts their copy, waits until it runs, turns on their
  triggers), one that mints them a member token, one that reads where they
  stand, and one that wipes them. A copy can take minutes to come up, so the
  bring-on route answers first and keeps working after its reply.
- **Every page shows what is happening, never a stale "not started".** After
  a person presses a button that starts something, the page reads where it
  stands and says so: the member token's display listing
  (`GET /signal-token/displays`) gives each infra display a `status`
  (`provisioning` is "starting…", `running` shows the display, `stopped` or
  `absent` offers the start button, `failed` shows the error), and the page
  reads it again every few seconds until it settles. A display read that
  answers 404 while the listing says `provisioning` is "starting", never "not
  started". The `members` catalog nodes do each of
  those. A `+server.ts` calls them only after reading the session, and it
  takes the member id from the session, never from the request body.
- **A signed-in person's own runs** go through the program's routes gated
  the same way, called from the frontend's server with
  `Weft-Member: <session user id>`. The header is honoured only on a gated
  route, and only the server may send it.
- **What the browser does as the member** carries a member token the program
  minted for that person (short-lived, handed over once, on their login):
  their settings page, their own displays (a bridge's QR code), a route they
  call from the page with `Weft-Member-Token`. A member token acts as that
  person in this one program and nothing else.
- **The browser only ever talks to its own site.** The dispatcher's address
  is often one the browser cannot reach (a loopback port on the machine
  running weft, which a Windows browser in front of a WSL install cannot
  see, or a dispatcher that is not public at all), and the site's server
  always can. So the site mounts the pass-through the connect library ships,
  and every member door, signal door and display call a page makes goes to
  `/weft/...` on the site itself:

  ```ts
  // front/src/routes/weft/[...path]/+server.ts
  import { env } from '$env/dynamic/private';
  import { weftPassThrough } from '$lib/weft-connect/server';
  import type { RequestHandler } from './$types';

  const pass = weftPassThrough({ dispatcher: () => env.WEFT_DISPATCHER_URL });

  export const fallback: RequestHandler = ({ request, params }) => pass(request, params.path);
  ```

  `WEFT_DISPATCHER_URL` (the address `weft token mint` printed before
  `/signal-token/`, `http://127.0.0.1:14111` on a local install) goes in the
  server's environment and nowhere a page can read it. The pass-through
  forwards only `/member/`, `/signal/`, `/signal-token/` and the program's
  own routes (`/connect/<tenant>/<path>`, following the route's redirect to
  the worker itself), carries the caller's token and body, and keeps the
  site's cookies and the `Weft-Member` header back. A page fetches
  `/weft/signal-token/displays` with the member token as bearer, calls a
  program route as `/weft/connect/<tenant>/<path>` with the member token in
  `Weft-Member-Token`, and `new MemberDoor(memberToken)` already calls
  `/weft/member/...`. If you catch yourself putting the dispatcher's
  address in a page, stop and write: "Wait. The browser talks to its own
  site." Then call `/weft/...`. If the site sends everyone not signed in to
  its login page (a check in `hooks.server.ts`, say), let `/weft/...` past
  that check: each of those calls carries the member's own weft token, and
  weft checks it.
- **Each person fills in what the program asks of them** (every field it
  writes `@member_filled`: their accounts, their spreadsheet, their model,
  their schedule) on a settings page you mount with `MemberSettings` and a
  `MemberDoor` from the connect library: the same pickers and list fields the
  weft editor shows, restyled to the site, and never the author's shared key.
  A save that changes a value one of their live triggers reads re-arms it
  before it answers, and the page says so; nothing else to call. The page
  titles each step with the program's own label and each field with the
  node's own name for it ("Value"), both written for the program's author,
  so you name every field a member sees in your own words:
  `<MemberSettings {door} labels={{ 'greet': 'Your greeting', 'greet.value': 'How the bot greets you' }} />`,
  a step's id for its title and `step.field` for a field (`door.fields()`
  lists both). If the page
  must look different, build it from `door.fields()`, `door.setValues(..)` and
  `ResourceSelect` over `door.resources(step, field)` instead. The library is not a package you install: you run
  `weft connect-lib` from the project root, which copies it into
  `front/src/lib/weft-connect` (`--into <dir>` for another folder), then
  import it from `$lib/weft-connect` and `$lib/weft-connect/svelte` (and
  the pass-through from `$lib/weft-connect/server`); the README it copies
  along has the lines that mount both. Run it again
  after weft is updated, and never edit inside that folder, because the next
  run replaces it.
- **A cron in the program tidies up**: members who left, copies idle too
  long. It is the program's, not the frontend's.
- **If the site bills its users**, a manager route reads `ctx.costs()` for a
  member (whose credential paid each call) and the frontend shows or charges
  it; weft only records the cost.

[the brief] for such a frontend names every manager route with its body and
reply, says each one is callable by the frontend's server alone after a
session check, and names which pages carry a member token and which calls
carry the header.

## House rules

- `front/` is the frontend's whole world: its `package.json`, its
  `node_modules`, its build output. You never put frontend files in the weft
  source tree, and never point a weft `@file` marker into `front/`.
- The build is `pnpm install` then `pnpm run build`; the dev server is
  `pnpm run dev`. You hand the user those exact commands in the report.
- `front/Dockerfile` builds the frontend into a server that listens on
  `$PORT`: that is what the deploy workflow runs on the cloud. For SvelteKit
  that means `@sveltejs/adapter-node` in `svelte.config.js`, and the image
  runs `node build`.
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
