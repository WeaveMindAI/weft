---
description: Walk the user through setting up and running a node's live test
argument-hint: <node type or package name>
---

Run the live tier of a node's self-tests, with the user's informed consent. `$ARGUMENTS` is the node type or package. The local tiers (basic, fake) already passed when the node was built; the live tier calls the real service with a real credential and can spend money, which is why it is run here, by the user's choice, and not by the node specialist.

Walk it in this order:

1. **Say what it costs.** One live test is one short-lived pod in the cluster making a real call on the named service, on the user's own credential or their credits. Get a plain "yes" before anything else.
2. **Find what the tests need.** Read `nodes/<type>/tests.rs` (or the package's members) for the `NodeTest::live` entries: the service each names, the credential fields, and any fixtures the test cannot self-provision (a chat id to message, a file to touch).
3. **Check the ground.** `weft daemon status`; start it with the user if it is down. The live tier also needs the project registered with the dispatcher: if the run refuses with an unknown-project error, register with an ordinary `weft run` (registration is part of it) or follow the error's own hint.
4. **Set up the credential, never through the chat.** Two ways, the user picks:
   - a key: the user puts `WEFT_NODE_TEST_<SERVICE>_<FIELD>=...` lines in the project's `.env` (auto-loaded) or their shell themselves. Never ask them to paste a secret into the conversation, and never echo one back. Fixtures (non-secret values like a chat id) go the same route.
   - a connection they already made in the editor: `--connection <service>=<grant id>`.
5. **Run it.** `weft test-node <type> --tier live` (add `--parallel` if they want speed and several tests exist). The permission prompt and the CLI's own money confirmation are both expected; the user answers them.
6. **Report.** Each test line, pass or fail, in plain words; a failure carries the service's own error text. On green, note that `weft node-test-hash <type>` prints the hash that lets a scripted sweep skip this package until its sources change, and that the node is now proven end to end.
