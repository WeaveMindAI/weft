---
description: "Inspect the last execution and chase the failure to one node"
agent: agent
argument-hint: [optional: an execution color]
---
Debug this project's latest run (or `${input:arguments}`, a specific execution color). Read the `weft-running` skill if you have not this session.

1. `weft executions --limit 5 --phase fire` and pick the color: the one in `${input:arguments}`, else the latest.
2. `weft logs <color>` first: it names the failing node and quotes the error, and for most failures that is the whole diagnosis. Then narrow the events instead of reading them all: `weft events <color> --kind failed` for the failures, `--node <id>` for what reached one node, `--kind node_skipped` for what did not run and why, `--full` on the one line whose value you need whole.
3. Chase it to one node:
   - A failed node: name it, quote its error, list the values that reached its inputs (its `node_started` line), and read the relevant source.
   - A wrong value: the motion backwards. Look at the top-level groups in `main.weft`, find the first whose output is already wrong in the events, descend into it, repeat, until you hold the single step whose value went wrong and the concrete input that broke it.
   - Suspended: say where it waits and for whom; if nobody should have been asked, the bug is the `_should_flow` that routed there.
   - Cancelled: read the reason on its `execution_cancelled` line. `Stopped by execution <color> (tag <tag>)` means a sibling run's `StopTagged` did it; look at that run, and if no sibling was meant to, the bug is the tagging.
   - Nothing ran: nothing reached the branch; walk from the trigger (or the root) down to the first node whose `_should_flow` or required input closed.
4. Name one fix, in two or three sentences: what you will change and why that fixes the observed value, not just the symptom. Apply it, validate, and run the same case again. Ask first only when the fix changes what the program does for the user, or when it would spend money or destroy state.

Report, in plain words: the color and status, the node and value at fault, the fix and whether it ran again green or waits on the user's yes. No hedging, no blaming the environment.
