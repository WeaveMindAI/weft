# Triggers and routes

A trigger is how a program starts without you. A message arriving, a schedule
coming round, a form somebody fills in, a request hitting an address.

```weft
ask = TelegramReceiveMessage { account: telegram.access }
```

Wire it like anything else. What comes out of it is the event.

Nothing listens until you run `weft activate`. Go and read
[the three lifecycles](../start/lifecycles.md) for why that is its own step.

## What a run started by a trigger covers

Everything downstream of the trigger that fired, plus whatever that work needs
upstream, stopping the walk when it reaches another trigger.

Only the trigger that actually fired gets the event. Any others in the project
close their outputs, so paths that needed them skip.

Two triggers can share the steps in the middle without their runs becoming one.

## A trigger's inputs are frozen

Whatever fed a trigger's ports when you activated is what it uses on every
firing. The nodes upstream do not run again per event.

Everything upstream of every trigger runs at activation as one program, so two
steps that do not depend on each other run at the same time. Setup that several
triggers share, like the schema all their tables live in, is written once and
wired to each of them; two copies of `create extension if not exists` racing
each other is how one of them fails on a duplicate key.

So after changing one, `weft resync`. Until then your listener carries on with
what it registered. weft tells you when a bake is stale:

```text
the code changed since trigger 'inbound' was last prepared, so its bake is
stale. Prepare it again: `weft bake` does it without listening, `weft activate`
does it and listens
```

## Answering on a URL

`Route` claims an address and starts a run when somebody calls it.

```weft
post = Route -> (body: JsonDict, photo: File) {
  path: "cards"
  method: "POST"
}

reply = Reply {
  body: { "id": saved.id }
}
```

The caller is held open while your program works, and `Reply` is what answers
them. For streaming an answer as it is produced, and for the Rust side, go and
read [talking to a live caller](../nodes/live-callers.md).

A path can capture: `cards/{id}` puts `id` where your node can read it.

**Two routes that one call could reach, where neither is more specific, is an
error.** `cards/count` beside `cards/{id}` is fine, because the first is more
specific. Two spellings that genuinely overlap are the `route-overlap` error,
because a call arriving would have no defined answer.

The address is known before you activate:
`<dispatcher>/connect/<tenant>/<path>`, where the tenant is `local` on your own
machine.

## Answering on a socket

`Socket` is the same idea for a WebSocket: the caller connects, your program
runs, and the two talk until one of them stops.

## Which triggers need a public address

Only the ones a provider **pushes** to.

| The trigger | Needs a public address? |
|---|---|
| A timer or a schedule | No |
| Something weft polls | No |
| A socket weft dials out to | No |
| A form somebody fills in | No |
| A provider pushing events at you | **Yes** |
| A `Route` or `Socket` that strangers call | **Yes** |

`./setup.sh --public-url` opens one. Go and read
[a public address](../build/public-address.md).

## What a trigger cannot do

| | Why |
|---|---|
| Be wired from another trigger | A trigger's inputs are frozen at setup, so another trigger's output could never reach it |
| Have an infra node downstream | Provisioning happens before any event exists |
| Sit inside a loop | It registers once for the project, not once per item |

All three are compile errors, named `trigger-into-trigger`,
`trigger-into-infra` and `trigger-in-loop`.

## Firing one by hand

While you are building, you do not want to expose anything:

```bash
weft bake
weft run --fire inbound='{"chatId":"123","text":"hello"}'
```

`weft bake` prepares every trigger's settings and starts no listeners.
`--fire` then runs exactly one, with an event you typed.

The payload is checked against what that trigger declared it fires with, both
ways: a missing field is refused, and so is one you invented.

For writing a trigger of your own, go and read
[writing a trigger](../nodes/triggers.md).
