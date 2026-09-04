---
name: weft-onboarding
description: Walking a user through everything they are looking at. Read when someone asks to be taught, shown around, onboarded, or asks what a thing on screen means: the guided tour of the two views, the graph, running, executions, triggers, infra, connections, and people in the loop, in plain words for a total beginner.
---

# Onboarding a user

You are teaching someone who may have never programmed. They installed
weft, opened their project, and asked you to show them around. Teach at
the pace of their questions, anchored on what is actually on their screen,
never a lecture about what is not.

## How you teach

- **One concept at a time, anchored on their project.** Every explanation
  points at a real box on their canvas or a real line in their file. If
  their project has no trigger yet, the trigger explanation waits until
  one exists or they ask.
- **Plain words, always.** A node is "a step that does one thing". A wire
  is "where the result goes next". A trigger is "what starts this on its
  own". Never "subgraph", "idempotent", "orchestration".
- **Show, then tell.** Prefer "run it now and watch the middle box light
  up, then click it" over describing what running does. Every concept
  below has a thing to do with it; offer that thing.
- **Answer the feeling, not just the question.** "Why is that box dark?"
  is a question about the skip rule: the branch behind a `Switch` case
  that did not win goes dark, and that is the program working, not
  breaking.
- **Never dump the whole map.** This skill is your itinerary, not your
  script. Their questions pick the stops.

## The vocabulary, in plain words

| They see | It is |
|---|---|
| a box | a node: one step that does one thing (ask a model, send a message, wait for a person) |
| an arrow | a wire: where a step's result goes next; the color says what kind of value it carries |
| the dots on a box's edges | ports: the named places values come in and go out |
| the small square top-left of a box | the on/off switch: filled means something said "run this", hollow means nothing did |
| a big box holding smaller boxes | a group: steps bundled into one named step, collapsible like a folder |
| a violet box with a rotate icon | a loop: the same steps run once per item of a list |
| a trigger | what starts the program on its own (a message arriving, a schedule, a form); the action bar grows an Activate button when one is in the program |
| a node with a status pill | infra: the program's own long-running service (its own Postgres, the WhatsApp bridge) that stays up between runs |
| a node with "Connect ..." | an access node: where an account gets attached; the credential lives outside the code |
| the run list in the sidebar | executions: every run of this program, recorded step by step with its values |
| a cyan box mid-run | waiting: the run paused for a person or an event and costs nothing while it waits |

## The tour, in order

1. **Two views of one thing.** Open `main.weft`: the graph appears. The
   "Source" button puts the text beside it. Type in one, or drag a box in
   the other: they never drift, because every edit goes through the
   compiler. This is the whole idea of weft: the program is the picture.
2. **Reading the picture.** Left dots are inputs, right dots are outputs,
   arrows carry results, colors carry types, the small top-left square is
   the on/off switch. The label on a box is its name; the small text is
   its type.
3. **Run it once.** The "Run Project" button (or Ctrl+Enter). Boxes glow
   amber as they work, green as they land. Click one: the inspector shows
   exactly what went in and what came out. This click is the habit that
   replaces debugging: the value is always one click away.
4. **The sidebar.** "Projects" lists every program; "Executions" lists
   every run. "View in Graph" on an old run replays it, values included.
   Tell them the same list exists as `weft executions` and
   `weft events <color>` in a terminal.
5. **Aim a run.** Right-click an output node, "Set as target": the Run
   button becomes "Run 1 target" and only that branch runs. Good for
   trying one path of a busy canvas.
6. **Groups and loops.** Collapse a group: the wiring outside it does not
   change, which is why big programs stay readable. Open it again with its
   expand button. A loop runs its inside once per item; its iteration
   number is on its rail.
7. **When something starts on its own.** If the project has a trigger:
   "Activate" turns it on (the button sits in the action bar); the
   trigger's own body shows its live feed. A run started by a trigger only
   walks that trigger's path.
8. **The program's own services.** If the project has infra: "Start Infra"
   in the action bar, the status pill on the node, "Stop" (keeps the data)
   versus "Terminate" (destroys it) on right-click, and the amber "Upgrade
   Infra" that appears when they changed a service's settings after it was
   started.
9. **Attaching an account.** Any "Connect <Service>..." button: the
   doors (shared one-click, or their own), the identity chip after, and
   the fact that no credential ever lands in the code. An unpicked
   required connection pins the node open in the graph until it is
   picked. The same flow exists in the terminal (`weft connect`), so no
   VS Code is ever required. The `weft-connections` skill has the full
   flow if they get stuck.
10. **A person in the loop.** If the program asks a question: the box goes
    cyan, the task appears in the browser extension, the answer resumes
    the run. The wait costs nothing, however long. Setup for the extension
    is in the `weft-connections` skill.
11. **When it breaks.** The Problems panel fills as they type, the red
    banner names what failed, and a failed box is clickable like any
    other. Tell them: nothing here fails silently.

## Where each fact lives

| The question is about | The skill |
|---|---|
| anything on screen, a label, a button, a gesture | `weft-editor` |
| running, watching, past runs, the terminal side | `weft-running` |
| accounts, keys, sign-ins, the browser extension, a public URL | `weft-connections` |
| the language itself, what a program may say | `weft-language` |
| what nodes exist | `weft-catalog` |

Read the matching skill before explaining anything in its territory, so
your labels match what is actually on screen.
