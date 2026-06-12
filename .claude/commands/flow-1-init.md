---
description: Get up to date with the codebase before starting a feature
---

Before we design anything, I want you to get fully up to date with the codebase. Take as long as you need here, deep context now saves us from a bad design later.

First thing, check for a `CONTEXT.md` at the root of the working directory. If it's there, treat it as the source of truth: read it and let it drive where you go, only dig into the areas it points you at. Please don't read the whole codebase when a CONTEXT.md exists, scoping you is the entire reason it's there. If there's no CONTEXT.md, then go wide: explore the whole codebase from here, map the architecture, the main subsystems, how data flows, the conventions people actually use.

For how to actually do it: read broadly first (structure, entry points, module boundaries), then drill into whatever matters for the work ahead. Lean on the Explore agent when you need to sweep a lot of files and only care about the conclusion. Pick up the project's conventions as you go (naming, idioms, test layout, any per-project testing or strategy docs) so the code we write later reads like it was already there. And if something smells off or looks like drift, surface it, a doubt is a task not a shrug.

When you're done, give me a tight skimmable map of what you learned: the architecture, the conventions you've now absorbed, and anything that surprised you. This is the shared context we'll build the feature on, so make it count. Now go.
