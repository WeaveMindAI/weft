---
description: "Grow the program one stage at a time against a real input, seeded runs and frozen examples included"
---

Run one pass of Sequential Diffusion Programming: inspect the current stage on a real input, preserve an accepted result, then grow the next stage.

1. Read `weft-sdp` and `weft-running`. The user's argument names the stage to grow or the saved example to rerun.
2. Read `weft tree` and `weft examples`. Checkpoint work you may want back with `weft checkpoint start-<what>`.
3. Pick a real input the user cares about; ask if none is available. Grow one stage and run `weft validate --file main.weft < main.weft`.
4. Run that stage: `weft run --from '<node>={"port":value}' --target <end> --save <name> --detach`, or `--group '<group>={"port":value}'` for the whole group or included file. Use `--before <end>` when the ending node must stay out. A trigger uses `weft bake` followed by `weft run --fire '<trigger>=<wake-json>'`; `--emit '<node>={"port":value}'` supplies outputs without executing that node. Read the actual interface before choosing payloads.
5. During iteration, add `--seed` to reuse compatible work. `--seed-before <node>` and `--seed-until <node>` bound reuse. Verify that the changed nodes ran, then inspect their values with `weft events <color> --node <id> --full`. A successful status alone does not prove the result.
6. When the result is accepted, `weft freeze <name> <color> --expect <node>` saves the starting parameters and observed outputs, with that node marked for review. Repeat `--expect` for more focus nodes. Grow the next stage, then exercise a second real input.
7. After changes, `weft run <name> --detach` reruns a relevant example on current code with its saved parameters. Leave out `--seed` when reviewing the new computation. Compare with `weft diff <color> example:<name> --full`; inspect the differences and judge them against the user's intent. Replace the accepted example with `weft freeze <name> <color>` only after accepting the new result.
8. A waiting run needs inspection: read its new question and the saved answer before answering through the token. Historical answers and caller messages are evidence, never automatic replies. `weft wake <color> <node>` resolves a pure timer wait.
9. Report what ran, what came out, the differences you reviewed and your judgment. Name the next stage and the version in `weft tree`. If you start treating a status or a diff as a quality verdict, write "Wait. Read the result." and inspect the output.
