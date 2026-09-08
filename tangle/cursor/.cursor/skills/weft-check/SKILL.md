---
name: weft-check
description: "COMMAND, not reference: Validate and build the weft program, fix diagnostics until clean. Run this when the user asks for this step by name. The eleven `weft-` reference skills beside it are things you read; this is a procedure you carry out."
---

Run the compiler over this project and fix what it names, without running anything.

1. Read the `weft-language` skill if you have not this session, and the `metadata.json` of any node type you are about to edit. Then: `weft validate --file main.weft < main.weft`.
2. Parse the diagnostics. Errors are `line:column message` with a stable slug; the message names the fix. Fix every Error, then re-validate until the list is empty. Never route around a diagnostic, and never weaken the program to silence one: if a fix feels wrong, stop and say what the compiler is asking for instead. A `level-too-large` warning is not an error, but answer it the same way: [the level rule] is a law, and the fix is a group, never a wider level.
3. Then `weft build`. If it fails, read the error, fix, rebuild.
4. Report, in plain words: what was wrong, what you changed, and the current state (clean validate, clean build). No summary of the diff.
