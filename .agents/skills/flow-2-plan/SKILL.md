---
name: flow-2-plan
description: "Write the agreed design to a plan file in ~/.codex/plans/"
---

We've babbled and landed on a design. Now write it down as a plan. Do not use a plan or approval mode and never present the plan through an approval flow. Stay in whatever mode you are in, write the plan file directly, then talk to me below the `---` line: tell me the path and summarize the shape in plain words. I'll tell you in chat whether to implement.

Write the plan ONLY to `~/.codex/plans/`, with a descriptive kebab-case filename. Never drop it into the codebase or the working directory; plans live there and nowhere else (otherwise you have a recurring habit of leaving plan files in the project root).

Write the design we actually agreed on during the babble, not a fresh one. If anything we settled on feels ambiguous now, ask me inline before writing it down.

Keep it as one clean delivery per the [decision framework]: no shippable phases, no backwards-compat shims between steps, no half-migrated states, no "phase N will fix this". Steps can have ordering (X before Y when Y depends on X), but every step is part of the same final shape.

Before you write the steps, run the extension analysis from the [decision framework] ("Before adding a new concept, look for an existing one to extend") over everything the plan introduces, and write it into the plan's shape section, in the plan's own voice: what gets extended, what is genuinely new because it survives the substitution test, and what requires building the shared parent Z first (build Z, reshape the existing concept onto it, then build the new thing on it). State which of the three each new thing is, so the implementer never has to guess. Reshaping existing code to make room for the clean shape is expected work, not scope creep.

You are not the one who will execute this plan. Your entire job here is to load a fresh session with enough context to implement it end to end, cold, with zero memory of our conversation. Over-explain; don't assume the reader was in the room. Structure it so that fresh session can execute cold:
- **Goal**: what we're building and why, a few lines.
- **Shape**: the architecture and data flow we agreed on, drawn out. The most important part: the implementer has to understand the intended shape, not just a task list.
- **Steps**: ordered, each concrete enough to act on, referencing real files and symbols where you know them.
- **Tests**: what to test and at which layer of the testing pyramid (load the `mode-code` skill when writing this section).
- **Decisions and open questions**: what we explicitly settled so it doesn't get re-litigated, plus anything genuinely still open.

Write the flexibility clause into the plan itself, in the plan's own voice to its future implementer: deviation during implementation is expected and fine, there are always surprises once you touch real code. Keep going without stopping as long as the change stays toward the same designed shape and [the fork rule] resolves the call; stop and surface only if a genuine big change of plan is needed, or one of the parts of the intended shape turns out unachievable (or looks like it isn't), so I can think about whether there's a workaround you can't see.

Keep it dense and honest. The implementer will trust this file completely, so it has to be right.
