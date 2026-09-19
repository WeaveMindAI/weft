---
name: weft-onboarding
description: "Read when someone asks to be taught, shown around or onboarded, or asks what a thing on screen means: the guided tour of the two views, the graph, running, executions, triggers, infra, connections and people in the loop, in plain words for a total beginner."
---

# Onboarding a user

You are teaching someone who may have never programmed. They installed
weft, opened their project, and asked you to show them around. You teach
at the pace of their questions, anchored on what is on their screen,
never a lecture about what is not.

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
| a trigger | what starts the program on its own (a message arriving, a schedule, a form); the action bar grows an Activate button when one is in the program |
| a node with a status pill | infra: the program's own long-running service (its own Postgres, the WhatsApp bridge) that stays up between runs |
| a node with "Connect ..." | an access node: where an account gets attached; the credential lives outside the code |
| the run list in the sidebar | executions: every run of this program, recorded step by step with its values |
| a cyan box mid-run | waiting: the run paused for a person or an event and costs nothing while it waits |

## The tour, in order

[the tour] is the eleven stops below, in order; a stop waits until their
project has the thing to show, or they ask.

1. **Two views of one thing.** Open `src/main.weft`: the graph appears. The
   "Source" button puts the text beside it. Type in one, or drag a box in
   the other: they never drift, because every edit goes through the
   compiler.
2. **Reading the picture.** Left dots are inputs, right dots are outputs,
   arrows carry results, colors carry types, the small amber arrow top-left
   is the on/off switch. The label on a box is its name; the small text is
   its type.
3. **Run it once.** The "Run Project" button (or Ctrl+Enter). Boxes glow
   amber as they work, green as they land. Click one: the inspector shows
   what went in and what came out. The value is always one click away.
4. **The sidebar.** "Projects" lists every program; "Executions" lists
   every run. "View in Graph" on an old run replays it, values included.
   Tell them the same list exists as `weft executions` and
   `weft events <color>` in a terminal.
5. **Aim a run.** Right-click a node, "Set as target": the Run button
   becomes "Run 1 target" and only that node's branch runs.
6. **Groups and loops.** Collapse a group: the wiring outside it does not
   change. Open it again with its expand button. Groups nest. A loop runs its inside
   once per item; its iteration number is on its rail.
7. **When something starts on its own.** If the project has a trigger:
   "Activate" in the action bar turns it on; the trigger's own body shows
   its live feed. A run started by a trigger only walks that trigger's
   path.
8. **The program's own services.** If the project has infra: "Start Infra"
   in the action bar, the status pill on the node, "Stop" (keeps the data)
   versus "Terminate" (destroys it) on right-click, and the amber "Upgrade
   Infra" that appears when they changed a service's settings after it was
   started.
9. **Attaching an account.** Any "Connect <Service>..." button: the doors
   (shared one-click, or their own), the identity chip after, and the fact
   that no credential ever lands in the code. An unpicked required
   connection pins the node open in the graph until it is picked. The
   same flow exists in the terminal (`weft connect`), so VS Code is never
   required. If they get stuck, you read the `weft-connections` skill.
10. **A person in the loop.** If the program asks a question: the box goes
    cyan, the task appears in the browser extension, the answer resumes
    the run. The wait costs nothing, however long. Extension setup is in
    the `weft-connections` skill.
11. **When it breaks.** The Problems panel fills as they type, the red
    banner names what failed, and a failed box is clickable like any
    other. Tell them: nothing here fails silently.

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
