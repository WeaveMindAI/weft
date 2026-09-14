---
name: weft-frontend
description: "Building the project's own frontend under front/. Read when the user wants a page, app, or site for the program: the default stack (pnpm, SvelteKit, PostgreSQL, BetterAuth, shadcn-svelte), how the frontend reaches the program through the program's own HTTP routes and signal doors and never its internals, where the api token lives, how the frontend shares the program's database without standing up a second one, and the build and run commands."
---

# The frontend

A program people use has a frontend: the pages where a person starts a run,
answers a parked question, or reads what came out. It lives in `front/`, its
own toolchain, and it is a client of the program, never a second backend. The
`frontend-builder` specialist builds it; this skill is what it reads, and what
Tangle reads when the user's request touches the frontend at all.

## The default stack

When the user names a stack, that stack wins: never override a technology the
user asked for. When the user names nothing, build with these defaults and
spend no question on them:

- **pnpm**, the package manager, and the only one you use.
- **SvelteKit**, the framework (Svelte).
- **PostgreSQL**, the database.
- **BetterAuth**, the authentication.
- **shadcn-svelte**, the components.

Add a piece only when the request needs it. These are defaults, not a
shopping list: the frontend that does the job with less is the better one.

## One PostgreSQL, not two

When the program already carries a `PostgresDatabase` infra node, the
frontend uses that same database: one Postgres for the project, not one the
frontend brings beside it. Point the frontend's connection at the database
the runtime runs, and put the frontend's tables (its auth tables, its own
data) in it under their own names. The failure to avoid is the split the user
cannot see: a frontend database and a program database that were supposed to
be one, so a row written by one half is invisible to the other. Never stand
up a second database when the program has one, and never tell the user the
two halves cannot share it: sharing it is the design.

When the program has no database of its own, the frontend brings its own
Postgres under `front/` (its `docker-compose` or the local install) and says
so in the report.

## The frontend talks to the program as its API

The program is the backend. The frontend reaches it two ways, and both are
the program's own surface, never its internals:

- **The program's HTTP routes.** When the program exposes routes (the
  `ApiEndpoint` node, and any group behind it), those routes are the
  frontend's API: call them by URL with `fetch`, exactly as you would call
  any REST service. Reach for them first when a page needs data the program
  already produces.
- **The program's signals.** A parked question a person answers, a trigger
  with no HTTP route, a stored file: those come through the signal doors
  (the `weft-consumers` skill). List what the token may see, draw each entry
  by its `kind`, fire it with the per-signal token, and ask the files door
  for every stored file as you render it.

The api token for the signal doors lives server side: keep it in SvelteKit's
server-only environment, call the doors from a `+page.server.ts` or a
`+server.ts`, and never ship it to the browser. A program route that needs
the token is called server side too.

- A parked question is answered under the field `key`, with the per-task
  token from the listing. A trigger is fired the same way and stays listed.
- Show the store's message text on any failure, never a broken image and
  never a silent no-op.

A page never calls a service the program does not. A capability the page
needs is a route or a signal the program exposes: if the program does not
expose it, that is a boundary to widen in the weft graph, said back to
Tangle, never faked in frontend code and never routed around the program to
a side service.

## Auth and the frontend's own data

BetterAuth is the frontend's concern: its users are the people who log in to
the pages, separate from the program's connection store. Give BetterAuth the
project's Postgres (the shared one above). A page that needs to show
program data reads it through the program's own routes or signal doors, not
through BetterAuth's tables or the program's rows directly.

## House rules

- `front/` is the frontend's whole world: its `package.json`, its
  `node_modules`, its build output. Never put frontend files in the weft
  source tree, and never point a weft `@file` marker into `front/`.
- The build is `pnpm install` then `pnpm run build`; the dev server is
  `pnpm run dev`. Hand the user those exact commands in the report.
- Seed data and demo accounts are useful; they are seed data, so say so, and
  never let a page depend on one to look alive.
- Keep it plain and legible, the same as the graph: shadcn-svelte components,
  little custom CSS, no design system the request did not ask for.

## Proving the frontend is proving the frontend

The proof is that the page builds and works as a client of the program, not
that the program is correct. Whether a node returns the right value is the
orchestrator's and the `node-smith`'s to establish before this work starts.
The frontend proves its own side: `pnpm run build` passes, the page drives the
one task end to end against the program (or against a recorded response when
the program is out of reach, said so plainly), and a failure the program sends
back renders as a readable message. A wrong value that comes from the program
is a finding to report, never something to test around or fix in the page.

## Stop when the frontend is done

A page that starts a run and reads its result is often the whole job. Build
that, prove it against a real run, and stop. Do not grow a frontend past what
the person using it needs.
