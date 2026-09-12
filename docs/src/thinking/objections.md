# Objections we sometimes get and what we think about them

## "Why a new language? Just make it a library"

For the actual work, weft is a rust framework. You write a step's logic in Rust with the APIs weft gives you, and I'm not going to invent a new syntax for parsing a response or adding up a number. The language has one job: saying how the pieces fit together. And for that job the syntax is the whole point.

Here's the thing I actually care about: in a normal codebase the architecture isn't written down anywhere, it's something you reconstruct by reading the implementations and working upwards. That's fine while everything fits in context. It stops being fine the moment it doesn't, and that's exactly what happens to a coding assistant: it goes down into one detail, does a good job on the piece in front of it, and doesn't know the shape it was supposed to plug into. So it writes a second version of something that already exists, or a path around the infrastructure it should have used. And by default, if you don't guide it, it writes with no structure at the top at all, because nothing in the language asks for one. It works right up until it doesn't.

In weft the structure is the source. Scoping a part of the program nests it, so you get the nesting for free, and a group declares its inputs and outputs before you open it. You read from the top down: collapse everything, see the whole shape, open the one part you're working on. The compiler checks those boundaries, so a wire can't quietly reach into another group's insides, it has to show up in the interface where someone can see it. That's what you hand an assistant: the outer structure from the contracts, then one group, one level deeper, with edges around the job. See [Groups](../language/groups.md) and [The commandments of plumbing](plumbing.md).

## "Won't better models make this unnecessary?"

A model getting better at untangling a codebase isn't a reason to keep handing it a tangled one. Even if tomorrow's model held your whole project in its head and never missed a connection, you'd still be paying it to read all of it. If a tenth of the context gives it everything the job needs, why buy the other nine tenths? Being smarter doesn't make those tokens free or faster to produce.

And you don't have to hand the project to one assistant at all. Agree the contracts, let one arrange the groups, have others work inside them at the same time, and a group can divide again. Weft natively enables high level of parallelization.

## "Visual programming always turns into spaghetti"

It absolutely can. Two hundred boxes on a canvas don't become understandable because you can zoom out far enough to see them all.

The difference is that the nesting isn't an editor feature, it's in the language. A group is a real boundary with declared inputs and outputs, so folding it away actually removes something from what you have to think about. You still have to pick useful boundaries, and no editor rescues a design where everything needs to know about everything else, but the compiler warns you when a level of the graph is getting crowded and Tangle is told to group things before it gets there. The graph also has a text form: the `.weft` file you review in git *is* the source, and the editor edits that file, not a separate model of it.

## "The compiler checks the wiring, not whether the program is right"

Correct. A perfectly well typed program can send a beautifully formatted wrong answer to the wrong person. What the checks catch today is incompatible port types, missing required inputs and cycles in the wiring. That's feedback about how the pieces got connected, which is exactly the kind of mistake an assistant makes and can fix without you. It says nothing about whether a step does the right work. Your judgement and the step's own tests still have jobs.

Where I want to take it is proving things about how the program behaves, at compilation time. The idea is to let a node carry labels about what it is and what it does: a node that takes an action in the world says so, and how big that action is; a node that's an autonomous agent says so; a node that verifies something says what strength of verification it gives you. Then the compiler checks claims across the whole graph. Every path from an autonomous step to a high-stakes action goes through a verification of at least this strength, or a critical action has three separate human approvals in front of it. If the program doesn't hold up, it doesn't build. You could even have metadata that store the recorded probability of a model to hallucinate and be able to tell you after chaining multiple AI the probability of a step containing an hallucination. You could then add recovery steps to reduce the probability of hallucination (like having a double verification with a web search)

It doesn't exist yet but it's high in the roadmap. For the whole argument about why any of this matters, read [How I think about AI safety](safety.md).

---

If something here looks wrong, or if you have other arguments, come tell me on [Discord](https://discord.com/invite/FGwNu6mDkU) this could help shape weft toward something even more useful.