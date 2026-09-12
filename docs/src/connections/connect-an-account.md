# Connect an account

A program that calls a model or sends a Telegram message needs an account with
that service. You connect it once, then pick it on the program's access step.

In the graph, click **Connect** on the access step, or **Change** if it already
has one. Pick an existing connection, or one of the options under **+ Add a
connection**.

If you would rather work in the terminal, run this from the project folder:

```bash
weft connect
```

Choose the step you want to set up, then follow the prompts to sign in or paste
whatever the service asks for. Repeat for each service the program needs.

Never paste a key into an ordinary step input, or into a conversation with your
assistant. Step inputs are written to the journal in the clear and show up in
the run view, where anyone who can open the project can read them. For what
weft stores instead and how it is sealed, read
[how connections work](overview.md).

## Changing or removing a connection

Run `weft connect` again and pick a different one, or name the step directly:

```bash
weft connect --node account
```

If you only want to see what is already stored, `weft connect --list` prints it
and changes nothing. It works from anywhere, including outside a project.

`--disconnect` clears the step's choice and leaves the stored connection alone.
`--forget <id>` deletes the stored connection for good, and every other
project's step that was using it then fails at run time telling you to
reconnect. Plain `weft connect` offers both from its menu.

If it says this project has no access node, the program has not got one yet.
Ask Tangle for the capability you want, or find the step yourself with
`weft describe-nodes --list`.

If signing in needs an OAuth application nobody has configured, read
[the apps file](the-apps-file.md). For a service that sends events to your
program, read [events from a service](events.md).
