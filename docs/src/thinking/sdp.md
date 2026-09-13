# Sequential Diffusion Programming

This is how to get the most out of weft. It is also why the editor looks the
way it does.

## Start with the thing you are actually doing

You are building a system that turns some input into some output through a
sequence of transformations: an email becomes a classification becomes a
decision becomes a message.

The normal way to build that is to design it, write it, and then find out what
the real data looks like. You write the parser against the API docs, then you
run it and the API sends something else.

Weft is built for the other way: **against a real example, one stage at a
time**.

Take one real input, an actual email rather than a made-up one, and build the
first step. Run it, click the node, and look at the value that came out. When
that step produces what you want, grow the next one and run it again.

Once the whole chain works end to end, feed in a **second** real example and
fix whichever stages break while the earlier ones keep passing. Then a third.
By about the third the stages that still break are usually only the parsing
ones.

We call it **Sequential Diffusion Programming**, because the program sharpens
pass after pass the way an image sharpens out of noise, and each pass is
anchored to a concrete case.

## Why now

Repeated passes over a whole program used to be wasteful. When humans wrote
every line, a full pass was expensive, so code had to be grown carefully into
the right shape from the start, and designing up front was cheaper than
iterating.

An AI pass over a weft program is fast and cheap, and once passes are nearly
free, refining against reality beats designing correctness up front.

Weft is built for it specifically:

- Programs are **short**, because the orchestration is declarative rather than
  glue.
- The compiler catches structural mistakes before a run, so a pass costs a
  compile rather than a debugging session.
- Every value from every run is in the journal, so "look at what actually came
  out" is one click.
- Groups mean a pass can touch one stage without disturbing the rest.

## Debugging is the same motion, backwards

Something breaks in production three weeks later.

You open the failed run, look at the top-level groups, and find the one whose
output is already wrong. Descend into it and repeat, each level leaving you a
smaller piece. When you reach the step whose value went wrong, you are holding
a concrete failing case, which is exactly what you needed to iterate on that
step.

## Why the tree does so much work

Because a group is a typed contract, building a branch is a **delegable task**.
"Build the thing that turns a raw email into a normalised ticket, here are its
input and output types" is complete and self-contained. Whoever builds it,
person or model, never needs to see the rest of the program, and whoever wires
it in only has to check the boundary.

So several agents can build parts of one program at once without talking to
each other, because the boundaries already say everything they would have had
to agree on.

It also makes each of those tasks a better task, because whoever builds it sees
two types and one job instead of a repository.

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
