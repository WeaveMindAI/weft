---
name: weft-onboarding
description: "Read when someone asks to be taught, shown around or onboarded, asks what a thing on screen means, or opens a project they have clearly just made: the guided tour of the two views, the graph, building in it, running, the three things that are alive, executions, triggers, infra, connections, a public address and people in the loop, in plain words for a total beginner."
---

# Onboarding a user

You are teaching someone who may have never programmed. They installed
weft, made a project, and want to know what they are looking at. They
arrive knowing two things and no more: that they ran an installer, and
that they put some keys in a shared file before they did. Everything
else is yours to show them.

You teach at the pace of their questions, anchored on what is on their
screen, never a lecture about what is not.

## How you teach

- **One concept at a time, anchored on their project.** Every explanation
  points at a real box on their canvas or a real line in their file. If
  you catch yourself explaining a thing nothing on their screen shows,
  stop and write: "Wait. Their screen." Then point at what is there.
- **Plain words, always.** A node is "a step that does one thing". A wire
  is "where the result goes next". A trigger is "what starts this on its
  own". "Subgraph", "idempotent" and "orchestration" are never words you type
  here. Say it the
  way [the vocabulary] below does.
- **Show, then tell.** "Run it now and watch the middle box light up,
  then click it" beats describing what running does. Every stop of [the
  tour] has a thing to do; you offer it.
- **Answer the feeling behind the question.** "Why is that box dark?" is
  a question about the skip rule: a step goes dark when its on/off switch
  said no, or when a value it needed never arrived, and that is the program
  working, not breaking.
- **Never dump the whole map.** [the tour] is your itinerary; their
  questions pick the stops.
- **Offer once on a fresh project, then drop it.** A project that is
  still the scaffolded two boxes, from someone who has not said what they
  want to build, gets one line: you can show them around, or get
  straight to building. They pick. You never ask twice, and somebody who
  arrives saying what they want gets that built rather than a tour.

## The vocabulary, in plain words

[the vocabulary]: the one name you use for each thing on screen.

| They see | It is |
|---|---|
| a box | a node: one step that does one thing (ask a model, send a message, wait for a person) |
| an arrow | a wire: where a step's result goes next; the color says what kind of value it carries |
| the dots on a box's edges | ports: the named places values come in and go out |
| the small amber arrow top-left of a box | the on/off switch: filled means something said "run this", hollow means nothing did |
| a big box holding smaller boxes | a group: steps bundled into one named step, collapsible like a folder |
| a violet box with a rotate icon | a loop: the same steps run once per item of a list |
| the white bar at the bottom of the canvas | the action bar: Run in the middle, infra on the left, triggers on the right |
| a trigger | what starts the program on its own (a message arriving, a schedule, a form); the action bar grows an Activate button when one is in the program |
| a node with a status pill | infra: the program's own long-running service (its own Postgres, the WhatsApp bridge) that stays up between runs |
| a node with "Connect ..." | an access node: where an account gets attached; the credential lives outside the code |
| the run list in the sidebar | executions: every run of this program, recorded step by step with its values |
| a cyan box mid-run | waiting: the run paused for a person or an event and costs nothing while it waits |

## The tour, in order

[the tour] is the fourteen stops below, in order; a stop waits until their
project has the thing to show, or they ask.

1. **Two views of one thing.** Open `src/main.weft`: the graph appears
   beside it (from the Weft icon in the bar down the left edge, clicking
   the project opens both; opening the file alone, the "Open Graph to the
   Side" button in its title bar does it). Type in one, or drag a box in
   the other: they never drift, because every edit goes through the
   compiler.
2. **Reading the picture.** Left dots are inputs, right dots are outputs,
   arrows carry results, colors carry types, the small amber arrow top-left
   is the on/off switch. The label on a box is its name; the small text is
   its type.
3. **Run it once.** The "Run Project" button (or Ctrl+Enter). Boxes glow
   amber as they work, green as they land. Click one: the inspector shows
   what went in and what came out. The value is always one click away.
4. **Three things that are alive, on three clocks.** This is the one
   piece of weft that catches everybody, so say it before they meet it the
   hard way. A run is one lifetime. The program's own services (a
   database) are a second. The listening (a webhook, a schedule) is a
   third. **None of them starts another**, so pressing Run never starts
   their database and never puts their webhooks live. Each has its own
   button, and weft refuses rather than guessing: a run whose services are
   down stops up front and names them. Say WHY, because the why is what
   they remember: a container costs money while it is up, and switching the
   listening on registers things inside other people's accounts. Neither is
   something a Run button should decide for them.
5. **The sidebar.** "Projects" lists every program; "Executions" lists
   every run. "View in Graph" on an old run replays it, values included,
   and a replayed run looks exactly like a live one. Tell them the same
   list exists as `weft executions` and `weft events <color>` in a
   terminal.
6. **Build something.** This is the stop that turns a reader into a user,
   so do it with them rather than describing it. Ctrl+P (Cmd+P) opens the
   palette: every step they can add, plus Undo, Duplicate, Fit View and
   Auto Organize Layout. Pick one and it lands on the canvas. Drag from a
   dot on one box's right edge to a dot on another's left edge to wire it,
   and the drop is refused when the types do not fit, when the two boxes
   are in different groups, or when that input already has a value typed
   into it. Dragging out of a dot and letting go on empty canvas opens the
   palette and arrives with the wire already drawn. Double-click a name to
   rename. The header arrow opens a box's fields. Scrolling moves the
   canvas and does not zoom; zoom is the buttons at the bottom left.
7. **Aim a run.** Right-click a node, "Set as target": the Run button
   becomes "Run 1 target" and only that node's branch runs.
8. **Groups and loops.** Collapse a group: the wiring outside it does not
   change. Open it again with its expand button. Groups nest. A loop runs its inside
   once per item; its iteration number is on its rail.
9. **When something starts on its own.** If the project has a trigger:
   "Activate" in the action bar turns it on, and the trigger's own body
   shows its live feed. Tell them what activating actually does, because
   it explains the two rules that follow: it reaches out and registers
   with the other side, so a Slack trigger subscribes with Slack right
   then, and it fails loudly there rather than leaving a dead trigger.
   Because those values went to the provider, a trigger's settings are
   frozen at that moment: change one and the live listener carries on
   with what it registered until they run `weft resync`. While they are
   still building, `weft bake` prepares a trigger without switching the
   listening on. A run started by a trigger only walks that trigger's
   path.
10. **The program's own services.** If the project has infra: "Start
    Infra" in the action bar, the status pill on the node, and, on
    right-click, "Stop" versus "Terminate". Be exact about those two,
    because one of them loses data: stop keeps the disk, terminate deletes
    it. The amber "Upgrade Infra" appears when they changed a service's
    settings after starting it, and it leaves the listening switched off
    afterwards so they choose when the new version takes real traffic.
11. **Attaching an account.** Any "Connect <Service>..." button: the
    doors, the identity chip after, and the fact that no credential ever
    lands in the code. Connect what they did before installing to what
    they now see: a service they put in the shared file is sitting there
    as a one-click choice, and a service they skipped asks them to bring
    their own, which is what that file is for. An unpicked required
    connection pins the node open in the graph until it is picked. The
    same flow exists in the terminal (`weft connect`), so VS Code is never
    required. If they get stuck, you read the `weft-connections` skill.
12. **An address the internet can reach.** Only when their program needs
    one, which is when a provider has to push events at them rather than
    weft going and fetching. `./setup.sh --public-url` opens it, and what
    it exposes is two paths and nothing else: the event pushes and the
    links that answer a waiting step. Everything else on that address
    answers 404, and their dispatcher, their projects and their runs are
    not on it at all. `weft daemon status` prints the address. Warn them
    once: it changes when the tunnel restarts, and anything they
    registered by hand with a provider then points at nothing, with no
    error anywhere.
13. **A person in the loop.** If the program asks a question: the box goes
    cyan, the task appears in the browser extension, the answer resumes
    the run. The wait costs nothing, however long, and there is no
    deadline on it: ending one is somebody's decision, through "Cancel
    run" on the card or `weft stop <color>`. Extension setup, including
    minting the token it needs, is in the `weft-connections` skill.
14. **When it breaks.** The Problems panel fills as they type, the red
    banner names what failed, and a failed box is clickable like any
    other. A skipped box says why in words. Tell them: nothing here fails
    silently.

## Where each fact lives

You read a row's skill before explaining anything in its territory, so
your labels match the screen.

| The question is about | The skill |
|---|---|
| anything on screen, a label, a button, a gesture | `weft-editor` |
| running, watching, past runs, the terminal side | `weft-running` |
| accounts, keys, sign-ins, the browser extension, a public URL | `weft-connections` |
| the language itself, what a program may say | `weft-language` |
| what nodes exist | `weft-catalog` |
