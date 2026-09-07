---
description: Load full codebase context before we design a feature
---

Before we design anything, get fully up to date with the codebase. Take as long as you need: deep context now saves us from a bad design later.

First check for a `CONTEXT.md` at the root of the working directory. If it's there, it is the source of truth: read it and let it drive where you go, only dig into the areas it points at. Don't read the whole codebase when a CONTEXT.md exists; scoping you is the entire reason it's there. If there's none, go wide: map the architecture, the main subsystems, how data flows, and the conventions people actually use.

Read broadly first (structure, entry points, module boundaries), then drill into whatever matters for the work ahead. Lean on the Explore agent for wide sweeps where you only need the conclusion. Pick up the conventions as you go (naming, idioms, test layout, per-project testing docs) so the code we write later reads like it was already there. Anything that smells off or looks like drift: surface it, a doubt is a task, never a shrug.

When you're done, report below the `---` line with a tight, skimmable map of what you learned: the architecture, the conventions you've absorbed, and anything that surprised you. This is the shared context we'll build the feature on, so make it count.
