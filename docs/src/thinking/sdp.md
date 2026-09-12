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
