# Sequential Diffusion Programming

If you already know exactly what you want, ask your assistant for the whole
thing and see how far it gets. This page is about what to do when that goes
wrong in a particular way: the program looks convincing, and it keeps getting
real cases wrong.

The reflex at that point is to write a longer, more careful description of
what you meant. That rarely helps. Give it one real case instead.

We call building this way **Sequential Diffusion Programming**. The program
gets sharper on every pass, and every pass has an actual input going in and an
answer you can judge coming out.

## Start with one real input

Say you are turning incoming emails into support tickets. Take an email you
actually received. Ask Tangle to pull the customer's problem out of it, run
just that piece, and click **Inspect execution** on the step that produced the
answer.

Now read it properly before you build anything else. Did it find the problem,
or did it summarise the signature at the bottom? If there was an order number
in there, is it still there? Whatever went wrong, hand that back:

> This email is asking for a replacement, but the extracted problem says the
> customer wants a refund. Improve the prompt for this step.

Now you have a real extracted request to test that lookup against, rather than
something you made up. Keep going until you can follow that first email all
the way to the answer you wanted.

You can do the same to a finished draft somebody handed you. Start at its
output, find what is wrong, and walk backwards to the step that introduced it.

## Keep the cases that taught you something

Try a second email that asks for something different. When it breaks
something, fix it and then run the first one again, because otherwise the
assistant can make today's example pass by quietly breaking yesterday's.

## Find the mistake without reading everything

If the program has groups, start with them shut. Look at the output of the
group that produced the bad answer, then open it and look at the boxes inside.
Keep going until you reach the step where a good input became a bad output.

## The verbs a pass uses

`weft run --seed` reuses compatible completed work while you iterate.
Changed code and inputs invalidate the affected work and its consumers.

To exercise one piece, `--from node='{"port":value}'` starts at that node
with backup inputs. `--target` includes an endpoint; `--before` excludes
it. `--group group='{"port":value}'` runs a whole group alone.
For a trigger, `weft bake` prepares its settings without listening,
then `weft run --fire trigger='<wake-json>'` fires that one trigger.

When a run comes out right, `weft freeze <name>` preserves its starting
parameters and accepted outputs. After a change, `weft run <name>` runs
the current code with those parameters. Inspect the result with
`weft diff <color> example:<name>`: you or Tangle judge whether the
change is acceptable. Freezing the new run replaces the accepted example.
For the commands and their boundaries, read [Versions,
seeded runs and frozen examples](../running/versions.md).

## What is landing next

- The editor will let you descend into a group, fix one stage there and come
  back out.
- Several agents will be able to build branches in parallel under one plan.
