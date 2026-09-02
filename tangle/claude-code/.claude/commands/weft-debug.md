---
description: Inspect the last execution and chase the failure to one node
argument-hint: [optional: an execution color]
---

Debug this project's latest run (or `$ARGUMENTS`, a specific execution color). Read the `weft-running` skill if you have not this session.

1. `weft executions --limit 5` and pick the color: the one in `$ARGUMENTS`, else the latest.
2. `weft events <color>`, then `weft logs <color>`.
3. Chase it to one node:
   - A failed node: name it, quote its error, list the values that reached its inputs (from the events), and read the relevant source.
   - A wrong value: the motion backwards. Look at the top-level groups in `main.weft`, find the first whose output is already wrong in the events, descend into it, repeat, until you hold the single step whose value went wrong and the concrete input that broke it.
   - Suspended: say where it waits and for whom; if nobody should have been asked, the bug is the `_should_flow` that routed there.
   - Nothing ran: a branch wired to no output; check `_is_output`.
4. Propose one fix, in two or three sentences: what you will change and why that fixes the observed value, not just the symptom. Wait for agreement, then apply it, validate, and offer to run the same case again.

Report, in plain words: the color and status, the node and value at fault, the fix you propose. No hedging, no blaming the environment.
