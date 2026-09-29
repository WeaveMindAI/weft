---
description: Deploy the program to a cloud install, through the deployer
---

Act on the cloud install named in the arguments: a deploy, unless the arguments ask for a rollback or for setting up CI.

1. Read `weft-deploying`.
2. The target is the first argument. If there is none, list the targets with `weft target list` and ask the user which one; never pick one yourself.
3. Load the `deployer` skill and do its job with the target and what to do, holding to its scope.
4. Relay its report: the version active on the target, its route addresses, and anything the user must do by hand, as the exact commands.
