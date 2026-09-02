---
description: Build and run the weft program, then report what came out
argument-hint: [optional: output node ids to target]
---

Run this project's program and report what actually came out.

1. Read the `weft-running` skill if you have not this session. `$ARGUMENTS`, if present, is a list of output node ids to target.
2. Pre-flight, before the heavy build: `weft validate --file main.weft < main.weft`. Fix any structural error first. Then read the `rule-runtime` findings: an unpicked connection does not fail the build, it fails the run at execution, so catch it here, in seconds. For each finding, either fix it yourself (`weft connect --list`, then `weft connect --node <id> --grant <grant>` when a stored connection fits; the pick is written into the source by the compiler) or name the node for the user (its Connect button in the graph, or `weft connect` in their own terminal for entering a new key). Wait for the picks, then continue.
3. `weft build`; fix any compile error before running.
4. Run: `weft run` with `--target <id>` for each id in `$ARGUMENTS` if any were given. If the dispatcher is unreachable, say so, run `weft daemon start`, and try once more. If the run refuses because infra it touches is not running, say so and run `weft infra start`.
5. When the run settles: `weft events <color>` (and `weft logs <color>` if it failed). Report, in plain words: the status (completed, suspended, failed), the value each output node produced, and, if suspended, where it is waiting and who needs to answer. If it failed, name the node, the error, and the values that reached it, then propose the fix.
6. Remind the user of the one stage this run just proved, and what the next stage to grow is.
