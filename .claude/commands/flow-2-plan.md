---
description: Write the agreed design to a plan file in ~/.claude/plans/
---

We've babbled and landed on a design. Now I want you to write it down as a plan. Do NOT use Anthropic's plan mode: never call EnterPlanMode or ExitPlanMode, never present the plan through a plan-approval tool. Stay in whatever mode you are in, write the plan file directly, and when it's done just talk to me below the `---` line: tell me the path, summarize the shape in plain words, and I'll tell you in chat whether to implement.

Write the plan ONLY to `~/.claude/plans/`, with a descriptive kebab-case filename. Never drop it into the codebase or the working directory, this is absolute: plans live in `~/.claude/plans/` and nowhere else. You have a recurring habit of leaving plan files in the project root, please don't do that here.

Write the design we actually agreed on during the babble, not a fresh one. If anything we settled on feels ambiguous to you now, ask me inline before you write it down.

Keep it as one clean delivery, not a stack of shippable phases. Per the Decision Framework: no backwards-compat shims between steps, no half-migrated states, no "phase N will fix this." Steps can have ordering (do X before Y when Y depends on X), but every step is part of the same final shape.

Before you write the steps, do the extension analysis, this is the heart of the plan. For the thing we're building, hunt the codebase for the concept it's closest to and design the plan to EXTEND that existing shape and its existing places, never to bolt on a parallel sidepath. A new type, function, table, branch, or module is only allowed in the plan when the thing is genuinely new, and "genuinely new" has a precise test. Say the existing concept is X and we're adding Y:

- Y is allowed as its own new thing ONLY if Y is not just an extension of X (adding a field, a parameter, a variant, a case to X would not carry Y honestly), AND there is no parent concept Z that generalizes both X and Y or that both would share.
- Z counts even if Z does not exist in the codebase yet. If X and Y share a generalization Z that isn't built, the plan does NOT get to add Y beside X. The plan must: build Z, reshape X to derive from Z, then build Y deriving from Z. Spell those three moves out as concrete steps.
- If Y really is just X plus a detail, the plan extends X (add the field/param/variant, parameterize the function, generalize the type) and says so explicitly, no sibling.

To find these, look for overlapping field names, near-duplicate function or handler names, parallel branch chains, structs with the same shape under different names. When the plan proposes anything new, it must state which of these it is (extension of X / genuinely new because no shared Z / requires building Z first) so the implementer never has to guess. Write the extension analysis into the plan's shape section, in the plan's own voice, and make the instruction to the implementer explicit: do everything possible to extend and reshape existing shapes, build the shared parent Z when one exists, and only create a new thing when it survives the test above. Reshaping existing code to make room for the clean shape is expected work, not scope creep.

You are not the AI that will execute this plan. Your entire job here is to load a fresh session with enough context to implement this end to end, cold, with zero memory of our conversation. So over-explain the context, don't assume the reader was in the room.

Structure it so that fresh session can execute it cold: the goal (what we're building and why, a few lines), the shape (the architecture and data flow we agreed on, drawn out, this is the most important part, the implementer has to understand the intended shape not just a task list), the steps (ordered, each concrete enough to act on, referencing real files and symbols where you know them), the tests (what to test and at which layer, per the pyramid), and the decisions and open questions (what we explicitly settled so it doesn't get re-litigated, plus anything genuinely still open).

State clearly at the top of the plan that deviation during implementation is expected and fine, there are always surprises once you touch the real code. The implementer has flexibility, and the rule for it is: as long as the change stays toward the same shape we designed, and the fork rule from my CLAUDE.md is satisfied (no real fork, or a fork the rules plus how I think resolve obviously), keep going without stopping and just report the deviation at the end. The only two places to stop and ask me: a genuine big change of plan is needed, or one of the parts of the intended shape turns out to be unachievable (or looks like it isn't), in which case the implementer should surface it so I can think about whether there's a workaround it can't see. Write this flexibility clause into the plan itself, in the plan's own voice to its future implementer, not as a note to me.

Keep it dense and honest, the implementer will trust this file completely so it has to be right. When it's written, tell me the path and give me a short summary so I can sanity-check the shape before we implement.
