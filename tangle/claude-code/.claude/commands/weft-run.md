---
description: Build and run the weft program, then report what came out
argument-hint: [optional: node ids to target]
---

Run this project's program and report what actually came out.

1. Read `weft-running` and `weft-sdp`. A saved example names starting parameters to run again. Node targets name where execution stops, including those nodes; `--before` stops before them. With no selection, run the ordinary graph roots; triggers require an explicit fire or emitted outputs.
2. Validate before building: `weft validate --file main.weft < main.weft`. Fix structural errors and inspect runtime findings. Pick an existing connection with `weft connect --node <id> --grant <grant>`; when credentials are missing, tell the user which node needs its Connect button or `weft connect` in their terminal.
3. Run `weft run --detach`, adding the requested selection, or `weft run <example> --detach` for saved parameters. Run builds automatically. For isolated inputs use `--from '<node>={"port":value}'`, or `--group '<group>={"port":value}'` for a whole group. To execute one trigger, prepare with `weft bake`, then use `--fire '<trigger>=<wake-json>'`. Read the interface before choosing payloads.
4. Inspect status with `weft executions`. Read `weft logs <color>` for failures and `weft events <color> --node <id> --full` for values. If suspended, inspect the question or timer and report what it needs. Historical answers are evidence for deciding a new answer.
5. When rerunning a frozen example, leave out `--seed` to review current computation, then `weft diff <color> example:<name> --full`. Judge the result against the user's intent. A difference is neither an automatic failure nor permission to replace the accepted example. Freeze again only after accepting the new result.
6. Report status, the requested outcomes and any failure with its node and inputs. Name the stage this run exercised and the next stage to grow. If you start calling a completed run proof of quality, write "Wait. Read the result." and inspect the values.
