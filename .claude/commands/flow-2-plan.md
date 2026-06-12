---
description: Write the agreed design to a plan file in .claude/plans/
---

We've babbled and landed on a design. Now I want you to write it down as a plan. Flip yourself into plan mode (EnterPlanMode) before you write anything.

Write the plan ONLY to `/home/quent/.claude/plans/`, with a descriptive kebab-case filename. Never drop it into the codebase or the working directory, this is absolute: plans live in `.claude/plans/` and nowhere else. You have a recurring habit of leaving plan files in the project root, please don't do that here.

Write the design we actually agreed on during the babble, not a fresh one. If anything we settled on feels ambiguous to you now, ask me inline before you write it down.

Keep it as one clean delivery, not a stack of shippable phases. Per the Decision Framework: no backwards-compat shims between steps, no half-migrated states, no "phase N will fix this." Steps can have ordering (do X before Y when Y depends on X), but every step is part of the same final shape.

Structure it so future-me, or a fresh session with zero memory of this conversation, can execute it cold: the goal (what we're building and why, a few lines), the shape (the architecture and data flow we agreed on, drawn out, this is the most important part, the implementer has to understand the intended shape not just a task list), the steps (ordered, each concrete enough to act on, referencing real files and symbols where you know them), the tests (what to test and at which layer, per the pyramid), and the decisions and open questions (what we explicitly settled so it doesn't get re-litigated, plus anything genuinely still open).

Keep it dense and honest, the implementer will trust this file completely so it has to be right. When it's written, tell me the path and give me a short summary so I can sanity-check the shape before we implement.
