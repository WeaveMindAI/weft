# A bigger example

The [Telegram image bot](https://github.com/WeaveMindAI/weft/tree/mvp/examples/telegram-image-bot)
turns a chat message into a picture. Before it calls the model it takes one
credit off the sender's balance, kept in the program's own database. No
account or no credits, and they get a polite refusal instead.

It is worth opening because it is the first program here with real parts: an
outside service, a database of its own, a model call, and a form for you to
fill in. Open `examples/telegram-image-bot` from your weft checkout as a
project in VS Code, and keep the whole folder, because the program reads its
SQL, its Python and its prompts from the files sitting next to `main.weft`.

## Get it running

It needs three accounts: Telegram, fal and OpenRouter. Click **Connect** on
each access step in the graph, or run `weft connect` in the project folder and
work through the prompts. Either way the credential goes into weft's own
store, and the program only ever holds a reference to it.

Model calls and image generation spend real money on those accounts. The bot's
own credits are a separate thing: they are just rows in its database.

Then bring up the database, which the program declares for itself:

```bash
weft infra start
```

Now give yourself an account. Right-click the `enroll` step, choose **Set as
target**, and run. That runs the enrollment path on its own, which creates the
table and then waits on the **Add a user** form. Fill it in from the browser
extension with your numeric Telegram user id and a starting balance, leaving
the balance empty for five. Wait for that run to finish.

Turn the bot on with `weft activate`, then send it a picture request in
Telegram. Open the run in the graph and watch the credit check, the prompt
being written, and the reply going back.

## Read it with the groups shut

There are two groups. `credit` decides whether this person may proceed, and
`brief` turns their message into a prompt for the image model. One line
between them decides everything:

```weft
brief._should_flow = credit.paid
```

If `paid` is false, the whole `brief` group is skipped and no image is ever
requested. The refusal goes to `sorry`, which sends it to Telegram. If the
credit was taken, `credit.refusal` closes, so `sorry` has nothing to send and
skips itself. Exactly one of the two branches speaks.

The picture travels as a reference to a stored file, not as bytes on the wire.
The Telegram step reads the file when it sends the message.

## Open the credit group

It takes a database connection and a Telegram user id, and its children read
those as `self.db` and `self.telegramUser`.

`sql/spend_credit.sql` takes one credit off, but only if the balance is above
zero, and reports why it did not when it did not. Then `scripts/outcome.py`
turns that row into the group's two outputs:

```python
row = rows[0]
if row.get("refusal"):
    return {"paid": False, "refusal": row["refusal"]}
return {"paid": True}
```

Notice that the success case never mentions `refusal`. That omission is what
closes the output, and the closed output is what keeps the apology quiet. It
is worth reading [the closed pulse](../language/mental-model.md#how-a-branch-stops-the-steps-after-it)
if that feels like sleight of hand, because it is the rule the whole language
branches on.

## Two front doors, one program

`ask` receives Telegram messages. `join` shows the enrollment form. They start
different runs and share the same database, and neither can trigger the other.
That is why adding a user never draws a picture, and why a Telegram message
never opens the enrollment form.

It is also why the table gets created inside the enrollment path rather than
somewhere separate: the first enrollment builds it, so the door looks after
itself.

## What it does not do

The credit comes off before the picture is made. If generation or delivery
fails, nobody gives it back, and nothing stops the same request being charged
twice. That is fine for an example and not fine for anything real, so if you
grow this into something that bills people, those are the two things you have
to design on purpose.

This is a good program to start changing. Ask Tangle for a different image
prompt, or a human approval before the model runs, or another way to top up
credits. Change one thing, run it, look at what came out.

For more of the language, carry on to
[how a program runs](../language/mental-model.md). If a run did not do what
you expected, [when something goes wrong](troubleshooting.md).
