---
description: Build and run the weft program, then report what came out
---

Run this project's program and report what actually came out.

1. Read the `weft-running` skill if you have not this session. If the user named any node ids, those are the targets; if they named none, every root fires.
2. Pre-flight, before the heavy build: `weft validate --file main.weft < main.weft`. Fix any structural error first. Then read the `rule-runtime` findings: an unpicked connection does not fail the build, it fails the run at execution, so catch it here, in seconds. For each finding, either fix it yourself (`weft connect --list`, then `weft connect --node <id> --grant <grant>` when a stored connection fits; the pick is written into the source by the compiler) or name the node for the user (its Connect button in the graph, or `weft connect` in their own terminal for entering a new key). Wait for the picks, then continue.
3. `weft build` first, so a compile error surfaces before the run's own build starts; fix any error before running.
4. Run: `weft run` with `--target <id>` for each id the user named, if any were given (a plain `weft run` fires every root in the file, so aim it when you mean one chain). If the dispatcher is unreachable, say so, run `weft daemon start`, and try once more. If the run refuses because infra it touches is not running, say so and run `weft infra start`.
5. When the run settles: `weft logs <color>` if it failed (it names the node and the error), and `weft events <color> --node <id>` for the values that came out. Report, in plain words: the status (completed, suspended, failed), what the run produced (the value of each target when you aimed it, the outcomes the user asked for when you did not), and, if suspended, where it is waiting and who needs to answer. If it failed, name the node, the error, and the values that reached it, then propose the fix.
6. Remind the user of the one [stage] this run just proved, and the next [stage] to grow.
