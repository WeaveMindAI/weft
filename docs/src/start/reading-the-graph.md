# Reading and building the graph

Open `main.weft` in VS Code and you get the graph, not the text. That is on
purpose. One thing to know: if you already had that file open as text, that
tab closes.

![The greeting graph on the left and its source on the right](../img/graph-split-view.png)

<!-- IMAGE: graph-split-view.png. The three-node name/message/out example
after clicking Source. Graph left, source right. No inspector open. Do not
reverse these panes: source always opens on the right. -->

If you want the code, the **Source** button is in the small toolbar at the top
left of the canvas. It opens the text beside the graph, on the right, and
clicking it again just brings that tab back to the front. Type in either one
and the other updates. You never have to save the graph: every change you make
writes the file straight away.

Everything below you can do with the mouse.

## What you are looking at

A box is one step of the program. weft calls it a **node**, and so does every
command and every error message. The dots on the left edge are what goes in, and the dots on the right edge are what
come out. Each dot takes a colour from the kind of value it carries, so two
dots of the same colour fit together. Weft call those **ports**

If you want to know which inputs a step cannot run without, look at how the
dot is filled in:

| The dot on the left | What it means |
|---|---|
| Solid | The step needs this one |
| Hollow | Optional, the step runs without it |
| Half filled | It needs at least one out of that little group |
| Dotted outline | A value is already written into the step, so nothing has to arrive here |

Every dot on the right is solid, so the fill only tells you something on the
left side.

An arrow carries a result from one box to the next, and it takes the colour of
the dot it started from.

The small amber triangle at the top left of a box is where you plug in the
answer to "should this step run at all?". It is hollow while nothing answers
it, and a step nobody has switched off just runs.

A big box with other boxes inside it is a **group**. It takes several steps,
gives them one name, and lets you fold them shut like a folder. A violet box
with a rotate icon is a **loop**, which runs what is inside it once per item
of a list.

Once a program has run, every box shows a small symbol for how it went: a tick
for done, a filled circle while it is working, a ringed dot while it waits for
an answer, a cross for failed, a slashed circle for skipped, and a filled
square for a run you stopped. The whole box glows too, so you can see it from
across the canvas: amber while it works, cyan while it waits, green when it
finished, red when it failed.

## Get around the canvas

Scroll to move around. Hold Ctrl (Cmd on a Mac) and scroll to zoom. If you
have dragged a box off into the distance and lost it, **Fit View** brings
everything back on screen; it is in the palette below, and in the small
controls at the bottom left.

## Add a step

Press **Ctrl+P** (Cmd+P on a Mac). That opens the graph's own palette, "Search
nodes and actions...", with every kind of step you can add. Pick one and it
appears on the canvas. Right-clicking empty canvas and choosing **Add
Node...** does the same thing.

If you wanted VS Code's own file switcher, click outside the graph first:
inside the graph, Ctrl+P belongs to the graph.

The palette also holds the plain actions: Undo, Redo, Duplicate Selected,
Delete Selected, Select All Nodes, Fit View, and Auto Organize Layout, which
tidies the whole canvas for you.

## Connect two steps

Drag from a dot on one box's right edge to a dot on another box's left edge.

If the two dots carry different kinds of value, the graph will not let you
drop the arrow there. Colour is the quick check: same colour fits. The full
story is in [Types](../language/types.md).

You also cannot wire a box inside a group to a box outside it. If a value has
to cross that line, it goes through the group's own dots on its edge.

If you already know what should come next, drag from a dot and let go over
empty canvas. The palette opens, and the step you pick arrives with the arrow
already drawn. That whole thing counts as one action, so one Ctrl+Z takes away
the new step and its arrow together.

If an input already has a value written into it, that dot will not take an
arrow. You get a message saying the port is driven by a config assignment,
which means: clear the value, then draw the arrow.

To get rid of an arrow, grab it near either end and drop it on empty canvas.

## Change what a step does

Every box has a small arrow in its header that opens it up. Open one and you
can edit its fields right there: text boxes, dropdowns, tickboxes, file
pickers, code editors.

A step that talks to an outside service has a **Connect** button in its body,
named after the service. Click it to sign in or paste a key. Until you pick an
account, the step stays open and you cannot fold it shut: where the fold arrow
normally sits, it says **Pick a connection first**.

Double-click a box's name to rename it. Right-click a box for **Duplicate**,
**Delete**, **Tags…**, and **Set as target**. Right-click one of its dots to
make that input optional or required, change its type, or remove it.

If a step hands you a whole bundle of values and you only want one piece,
right-click the arrow. You get the names inside it, and clicking one narrows
the arrow to that piece. Click again to go deeper. **Up one level** goes back,
and **Read the whole value** puts it back to the whole bundle.

## Run it and see what happened

If you want to run the whole thing, **Run Project** sits in the bar at the
bottom of the canvas, or press **Ctrl+Enter**. Boxes light up as they fire,
and while a run is going that same button becomes **Stop Execution**.

Then click the magnifying glass on any box that ran, **Inspect execution**,
and you get exactly what went in and what came out, plus how long it took and
what it cost.

![The message inspector showing the name that went in and the greeting that came out](../img/graph-inspector.png)

<!-- IMAGE: graph-inspector.png. One node selected, inspector open on that
firing's inputs and outputs, plus status and timing. Pick a node whose value
is interesting. -->

If a box failed, the same magnifying glass shows the error, and the bar at the
bottom keeps the failure on screen until you dismiss it.

If a box was skipped, the inspector says why in plain words, such as "its
`_should_flow` said no", "the required input 'x' closed", or "the scope 'x' it
lives in did not run". Follow the arrows backwards from there and you find the
step that made the decision.

If a box ran more than once, which happens inside a loop, a small `‹ 2/5 ›`
appears in its header. Click the arrows to step through the runs one by one.

## Look at an older run

If you want a run from yesterday, click the **Weft** icon in the strip down
the side of VS Code. It opens two lists. **Projects** lists every program in
the folder. **Executions** lists every run of whichever program you last
opened, newest first, and each row has **View in Graph**, which puts that run
back on the canvas with its values in place.

The button in the top left toolbar says which run you are looking at, with its
id beside it: **Live** while it follows the newest one, **Pinned** while it
stays on the one you chose. Click it to switch. If new runs happen while you
are pinned, an amber button appears saying how many, with **Catch up** on the
end.

## Run only part of it

Right-click the box whose result you want and choose **Set as target**. The
run button changes to **Run 1 target**, or **Run 2 targets**, and so on. Only
that box and the boxes it needs will run. You can set as many as you like, and
right-clicking a target again offers **Unset target**.

The same thing from a terminal:

```bash
weft run --target daily_report
```

The run covers the target and everything feeding into it. A trigger it reaches
is included, but the walk does not carry on past it. If you target a box
inside a group or a loop, the whole of that group or loop comes with it. For
the exact rules, read
[What actually runs](../language/mental-model.md#what-happens-when-you-hit-run).

## Fold a big program down

Every group has a small button in its header, **Collapse group** when it is
open and **Expand group** when it is shut. The wiring outside does not change
either way.

![One group collapsed, then expanded with the same outside connections](../img/graph-groups.png)

<!-- IMAGE: graph-groups.png. Left: a group collapsed to one box with its
name, description line and boundary ports. Right: the same graph expanded,
children visible. The outside wiring must be identical in both. -->

Once a program gets big, start with the groups shut. If one group's answer is
wrong, open it up, check the boxes inside, and keep going until you find the
one that got it wrong. For writing those boundaries yourself, read
[Groups](../language/groups.md).

If you want to show your program to somebody, there is a **Simplified view**
switch at the top right. Every box becomes a plain square with one dot on each
side, which is much easier to read and no use at all for building. While it is
on you cannot add, delete or rewire anything, and trying tells you to switch
back to the builder view. You can still move boxes around and fold groups open
and shut.

## Moving boxes around

Where you drag a box is saved next to the program, in `layouts/`, not in
`main.weft`. Dragging a box somewhere else does not change your program at
all.

Moving one *into* or *out of* a group does change it, because it changes what
that step can reach.

Next, [put a person in the loop](a-person-in-the-loop.md).
