# Things people say to me

## "Why a new language? Just make it a library"

For the actual work, weft is a Rust framework. You write a step's logic in Rust
with the APIs weft gives you, and I am not going to invent a new syntax for
parsing a response or adding up a total.

The language has one job: saying how the pieces fit together. For that job the
syntax is the whole point.

Think about where the architecture of a normal codebase is written down.
Nowhere. It is something you reconstruct by reading implementations and working
upwards, and that is fine while the whole thing fits in your head.

It stops being fine for a coding assistant, which is working in a window. It
goes down into one detail, does a good job on the piece in front of it, and has
no idea what shape it was supposed to plug into. So it writes a second version
of something that already exists, or a path around the infrastructure it should
have used. And if you do not guide it, it writes with no structure at the top at
all, because nothing in the language asks for one.

In weft the structure is the source. Scoping a part of the program nests it, so
the nesting is free, and a group declares its inputs and outputs before you open
it. You read from the top down: collapse everything, see the whole shape, open
the one part you are working on. The compiler holds those boundaries, so a wire
cannot quietly reach into another group's insides. It has to appear in the
interface, where somebody can see it.

That is what you hand an assistant: the outer structure from the contracts, then
one group, one level deeper, with edges around the job. See
[groups](../language/groups.md) and
[the commandments of plumbing](plumbing.md).

## "Won't better models make this unnecessary?"

A model getting better at untangling a codebase is not a reason to keep handing
it a tangled one.

Even if tomorrow's model held your whole project in its head and never missed a
connection, you would still be paying it to read all of it. If a tenth of the
context gives it everything the job needs, why buy the other nine tenths? Being
smarter does not make those tokens free, or faster to produce.

And you do not have to hand the project to one assistant at all. Agree the
contracts, let one arrange the groups, have others work inside them at the same
time, and a group can divide again. Weft is built for that kind of parallel
work.

## "Visual programming always turns into spaghetti"

It absolutely can. Two hundred boxes on a canvas do not become understandable
because you can zoom out far enough to see them all.

The difference is that the nesting is not an editor feature, it is in the
language. A group is a real boundary with declared inputs and outputs, so
folding it away actually removes something from what you have to think about.

You still have to pick useful boundaries, and no editor rescues a design where
everything needs to know about everything else. What the compiler does is tell
you when a level is getting crowded: past fifteen items on one level it says so,
by name.

The graph also has a text form. The `.weft` file you review in git **is** the
source, and the editor edits that file rather than a separate model of it.

## "The compiler checks the wiring, not whether the program is right"

Correct. A perfectly well typed program can send a beautifully formatted wrong
answer to the wrong person.

What the checks catch today is incompatible port types, missing required inputs
and cycles in the wiring. That is feedback about how the pieces got connected,
which is exactly the kind of mistake an assistant makes and can fix without you.
It says nothing about whether a step does the right work. That is still your
judgement, and the step's own tests.

Where I want to take it is proving things about how the program behaves, at
compilation time: letting a node carry labels about what it is and what it does,
then having the compiler check claims across the whole graph, so that an
autonomous step cannot reach a high-stakes action without a strong enough
verification in front of it. The full argument, and why I think it matters, is
in [how I think about AI safety](safety.md).

---

If something here looks wrong, or you have other arguments, come and tell me on
[Discord](https://discord.com/invite/FGwNu6mDkU).
