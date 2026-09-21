# Sequential Diffusion Programming

If you already know exactly what you want, ask your assistant for the whole
thing and see how far it gets. Sometimes that is the end of it.

This page is for when it goes wrong in one particular way: the program looks
convincing, and it keeps getting real cases wrong.

The reflex then is to write a longer, more careful description of what you
meant. That rarely helps. Give it one real case instead.

We call building this way **Sequential Diffusion Programming**. The program gets
sharper on every pass, and every pass has an actual input going in and an answer
you can judge coming out.

## Start with one real input

Say you are turning incoming emails into support tickets. Take an email you
actually received. Ask Tangle to pull the customer's problem out of it, run just
that piece, and click **Inspect execution** on the step that produced the
answer.

Now read it properly, before you build anything else. Did it find the problem,
or did it summarise the signature at the bottom? If there was an order number in
there, is it still there?

Whatever went wrong, hand that back:

> This email is asking for a replacement, but the extracted problem says the
> customer wants a refund. Improve the prompt for this step.

Now you have a real extracted request to test the next step against, rather than
something you made up. Keep going until you can follow that first email all the
way to the answer you wanted.

You can do the same to a finished draft somebody handed you. Start at its
output, find what is wrong, and walk backwards to the step that introduced it.

## Keep the cases that taught you something

Try a second email that asks for something different. When it breaks something,
fix it, then run the first one again. Otherwise the assistant can make today's
example pass by quietly breaking yesterday's.

## Find the mistake without reading everything

If the program has groups, start with them shut. Look at the output of the group
that produced the bad answer, then open it and look at the boxes inside. Keep
going until you reach the step where a good input became a bad output.

## The verbs a pass uses

| If you want to | Run |
|---|---|
| Re-run without redoing what did not change | `weft run --seed`. Changed code and inputs invalidate the affected work and everything downstream of it |
| Exercise one step with values you supply | `weft run --from node='{"port":value}'` |
| Run up to a step, including it | `weft run --target node` |
| Run up to a step, excluding it | `weft run --before node` |
| Run one group on its own | `weft run --group group='{"port":value}'` |
| Try a trigger without switching the listening on | `weft bake`, then `weft run --fire trigger='<json>'` |

When a run comes out right, `weft freeze <name>` keeps its starting parameters
and its accepted outputs. After a change, `weft run <name>` runs the current code
with those parameters, and `weft diff <color> example:<name>` shows you what
moved, for you or Tangle to judge. Freezing the new run replaces the accepted
example.

For the full rules and every refusal, go and read
[versions, seeds and frozen examples](../running/versions.md). What is landing
next is on [the roadmap](../appendix/roadmap.md).
