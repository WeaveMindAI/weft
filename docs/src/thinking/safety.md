# How I think about AI safety

There's something fundamental about LLMs that most people get wrong: the thing you talk to isn't the model. The model predicts probabilities for the next token, a sampling algorithm picks which one actually happens, that token goes back into the context, and the model predicts again. Nobody wrote the assistant down anywhere. The model offers the possible continuations, the sampler picks one, and after enough picks you're talking to a character.

So if you want a different assistant, you edit some text and start again, and what emerges is different. The character lives mostly in the context, and you can open the context and read it.

That still works, but it's getting harder, because more and more of the assumptions about what's being predicted get pulled out of the context and into the weights. Getting a model to actually be the character you handed it takes real effort now, and more of it every year. Those models are still fundamentally simulators though, even if their internal dynamics have changed. For what I think about training, go read [Three properties for alignment](https://weavemind.ai/blog/three-properties-for-alignment).

## What I'm worried about

I worry about an AI that stays self-coherent over long periods of time. That's the future I'm trying to make less probable.

There are two ways I see us getting there.

The first is the one most labs are probably aiming at: a single model with either an infinite context or a very compressed memory, where everything that defines its behaviour has moved inside the weights, or the memory, and is no longer in the context. Models are drifting that way. I'd argue that as long as next-token prediction is still the base training and a major chunk of the training, it stays possible to make another persona emerge by tweaking the context hard enough and we wouldn't be in this scenario.

The second is a mind emerging at a meta level, through high-bandwidth coordination and high context adherence. Labs would train AIs to be extremely good at coordinating with each other, and at storing and retrieving information through a well organised shared memory they can edit. With the current paradigm, I think this path could arrive earlier than the first one.

## Yes, you could use weft for the second one

weft is powerful enough to build that level of coordination. But:

1. You'd still need AIs trained to act as a singular mind through context alone. weft doesn't get you there, and I'd argue that's the hardest part.
2. weft doesn't make it much easier anyway. You still have to shape the memory layer properly, and weft helps with that no more than any other language.

## What weft actually changes

1. **The model doesn't choose the control flow.** In an agent, the model
   decides what happens next, so every property you want is a property you're
   hoping it maintains. In weft the graph decides, and the graph was written
   down and checked before anything ran. The model fills in values at the
   steps you gave it.
2. **A part reaches exactly what you wired to it.** Not just what it can see,
   what it can do. The agent that investigates a case cannot also decide
   whether its own answer needs approving, and that's not a convention someone
   followed, it's a fact about the program. Scoping what information
   circulates is the same mechanism.
3. **People are in the graph, not bolted onto it.** A human wait is durable
   and costs nothing while it waits, so oversight doesn't get dropped the
   first time someone looks at the compute bill. Making the careful path the
   cheap one is the whole bet, and this is it at the level of one design
   decision.
4. **The more coordination you pull out of the model, the more interpretable the system gets.**
   Everything a run did is in the journal, and you read it
   against a graph you already have in front of you. One agent node doesn't get more intepretable, but each time you take a
   piece of what the model was deciding and make it explicit in the graph, a
   piece of what used to be an unreadable trajectory becomes something you can
   actually inspect.
   
You could build a CoEm with it, or weird shapes that include people and API services.

## Make the safe path the cheaper one, and make it arrive earlier

If you pull the coordination out of the AI's job and write it down, you get something explicit, controllable, interpretable and cheaper to run, with the same economic benefit as the autonomous version.

My bet is that it's what makes the scary future less likely. The autonomous version costs more and gets harder to justify building, without anyone having to ban it. And policy people get something they can point to, which is a much easier job than arguing for a full stop.

It's also why weft can't be a tool for safety people only. A tool nobody else picks up doesn't change what gets built, so it has to be worth using simply because it's a good way to build things.

## Where I want to take the compiler

Today the compiler checks how the pieces connect: types, required inputs, graph shape. What I want next is for it to prove things about how a program behaves at compilation time, and I think that's reachable through richer metadata.

The idea is to let a node carry labels about what it is and what it does. A node that takes an action in the world says so, and says how big that action is. A node that's an autonomous agent says so. A node that verifies something says what strength of verification it gives you. Then the compiler can check claims across the whole graph: every path from an autonomous step to a high-stakes action goes through a verification of at least this strength, or a critical action has three separate human approvals in front of it. If the program doesn't hold up, it doesn't build.

None of that works if the labels are decoration. So the other half is vetted sets of nodes, checked and signed off by whoever is doing the regulating, with safety-critical programs required to build out of those. Then the labels mean something, and so does everything the compiler proves on top of them.

That's the piece that turns all of the above from a way of working into a property of the program. It doesn't exist yet but it's high in the roadmap.

None of it is necessary for the rest of the safety vision to hold, though. If the language only makes those two shapes less likely on its own, I'd already call it a win. Where it does become useful is if anyone wants to regulate AI properly, because I think regulation as compilation properties is one of the better shapes AI regulation could take.