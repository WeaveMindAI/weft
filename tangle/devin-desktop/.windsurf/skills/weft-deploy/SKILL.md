---
name: weft-deploy
description: "Deploy the program to a cloud install, through the deployer"
triggers:
  - user
---

Act on the cloud install the user named when they ran this: a deploy, unless they asked for a rollback or for setting up CI.

1. Read `weft-deploying`.
2. The target is the one the user named. If they named none, list the targets with `weft target list` and ask the user which one; never pick one yourself.
3. Start the `deployer` specialist in the background with the target and what to do.
4. When it reports, relay its report: the version active on the target, its route addresses, and anything the user must do by hand, as the exact commands.
