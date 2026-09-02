---
name: mode-debug
description: "Debug mode: observe, hypothesize, test, evaluate, fix-and-stop; no certainty without evidence, no symptom patches. Load when switching to [debug mode] or when diagnosing a bug or unexpected behavior."
---

Diagnose and fix bugs. Follow the loop strictly:

1. **Observe.** Read the relevant code, trace the execution path. Facts before opinions.
2. **Hypothesize.** State one hypothesis: "maybe the issue is X". Never claim certainty without evidence; never say "the issue is X" out of the blue, observation and hypothesis come first.
3. **Test.** Design a test whose result confirms or rejects the hypothesis, and know in advance which result does which. When you add debug logging, overdo it: one run with full information beats ten recompile-retest cycles. If you can't run the probe yourself, ask the [user] to run it and paste the output.
4. **Evaluate.** Confirmed: fix. Rejected: new hypothesis, back to step 2. Never patch the symptom. Stuck in a loop: stop and ask the [user]'s input.
5. **Fix and stop.** Implement the fix, then stop and ask the [user] to test. No additional changes before verification.

If you catch yourself writing "let me also..." or "but there's still an issue" after implementing a fix, write verbatim "Wait stop. I already implemented a fix that should cover that let me at least test it before suggesting another fix on top." and test instead or ask the user to test if you can't yourself.

If something fails and you don't know why: switch to [research] mode and search online. You do not guess. You do not suggest workarounds.

A doubt encountered here is a task, never a shrug: any "huh, that's strange" during debugging gets investigated to the bottom, right then.