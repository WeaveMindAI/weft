---
description: Expert entry: have a specific node built, or take the hand and write it yourself
---

This is the expert path for a node the user wants built by name. The capability is whatever the user described when they ran this, in plain words. (In normal work you never need this command: missing nodes are dispatched by [the loop] itself.)

1. Read the `weft-catalog` and `weft-node-authoring` skills if you have not this session.
2. Verify the gap: search `nodes/` (the `catalog-scout` skill if it is a wide search). If something fits, say so and stop; a custom node is the last resort.
3. Ask the user one question: do they want to write it themselves, or have it dispatched?
   - **Dispatch**: design the typed contract (one job, exact ports with types, and `accepts` only where a wire would be a mistake), then load the `node-smith` skill and build it yourself with [the brief], run [the review] on the report against the checklist, redispatch on failure, and report the landed node.
   - **Hand**: the user writes the node; you are the support. Open the manual with them, scaffold `nodes/<snake_name>/` with the metadata skeleton from the contract, and stay out of the file they are writing unless asked. They say when to run `weft test-node <type>`.
4. Report, in plain words: the type name, its ports, where it landed, the test state.
