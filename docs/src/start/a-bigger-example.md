# A bigger example

Everything in the book so far has been one idea at a time. This is a whole
program: a Telegram bot that draws pictures, and charges a credit for each one.

```weft
telegram = TelegramAccess
db = PostgresAccess
fal = FalAccess

ask = TelegramReceiveMessage { account: telegram.access }

credit = Group(db: Access, telegramUser: String) -> (paid: Boolean, refusal: String?) {
  # Take one credit off this telegram account, or say why we cannot.
  # The query reads the sender's id as `$telegram_id`.
  debit = PostgresExecuteQuery(telegram_id: String) {
    query: @file("sql/spend_credit.sql")
    account: self.db
    telegram_id: self.telegramUser
  }

  # `refusal` says nothing when the credit was taken
  read = ExecPython(rows: List[JsonDict]) -> (paid: Boolean, refusal: String?) {
    code: @file("scripts/outcome.py")
    rows: debit.rows
  }

  self.paid = read.paid
  self.refusal = read.refusal
}
credit.db = db.access
credit.telegramUser = ask.user

sorry = TelegramSendMessage {
  _is_output: true
  account: telegram.access
  chatId: ask.chatId
  text: credit.refusal
}

brief = Group(request: String) -> (prompt: String) {
  # Turn what they typed into an image prompt
  write = LlmInference -> (response: String) {
    prompt: self.request
    provider: OpenRouterProvider { model: "anthropic/claude-sonnet-4.5" }.provider
    params: LlmParams { systemPrompt: @file("prompts/image_brief.md") }.params
  }
  self.prompt = write.response
}
brief.request = ask.text
brief._should_flow = credit.paid

picture = FalGenerateImage {
  model: "fal-ai/flux/dev"
  imageSize: "square_hd"
  account: fal.access
  prompt: brief.prompt
}

reply = TelegramSendMedia {
  _is_output: true
  kind: "photo"
  account: telegram.access
  chatId: ask.chatId
  file: picture.image
  caption: brief.prompt
}
```

Somebody messages the bot. One credit comes off their balance if they have
one, a model turns their message into an image brief, fal draws it, and the
picture goes back to the chat. No credits, or no account at all, and they get
told so instead.

Three files sit beside it, none of them weft:

| File | What it holds |
|---|---|
| `sql/spend_credit.sql` | one statement that takes a credit and reports what happened |
| `scripts/outcome.py` | turns the query's row into `paid` and, when it failed, a sentence |
| `prompts/image_brief.md` | the system prompt that turns a message into an image brief |

`@file` pulls each one in as that field's value, so the SQL lives in a real
`.sql` file your editor can highlight while still being the node's config.
The query's parameter is a port the node declares for itself,
`PostgresExecuteQuery(telegram_id: String)`, and the SQL reads it as
`$telegram_id`. For what
else `@file` can do, and its read-only sibling, go and read
[Files and reuse](../language/files-and-reuse.md).

## The credit group

```weft
credit = Group(db: Access, telegramUser: String) -> (paid: Boolean, refusal: String?)
```

A group is a node with a graph inside it. Its children can only reach each
other and `self`, which is why the database connection comes in as a port:
`db` is out in the file where the children cannot see it, so the group asks for
it. In return you can fold the whole thing shut and read the program without
it. For what else the boundary buys you, go and read
[Groups](../language/groups.md).

## Two endings from one query

The whole check is one statement, so the read and the debit cannot drift apart
between two queries:

```sql
with account as (
  select id from users where telegram_id = $1
),
spent as (
  update users set credits = credits - 1
  where telegram_id = $1 and credits > 0
  returning id
)
select
  (select id from spent) as user_id,
  case
    when not exists (select 1 from account) then 'this Telegram account is not linked to an account'
    when not exists (select 1 from spent) then 'you have no credits left'
  end as refusal
```

The two endings are driven by one value. When the credit goes through,
`outcome.py` returns no `refusal` key at all, so that port closes. `sorry`
needs that text to run, so it is skipped and nobody gets an apology. The other
way round, `paid` is false, the `brief` group is told not to flow, and
everything inside and behind it (the model, fal, the reply) is skipped
instead.

For the whole rule, go and read
[How a weft program runs](../language/mental-model.md#the-closed-pulse).

## Why both endings say `_is_output`

The picture and the apology are the two things this program is for, so both say
`_is_output: true`. A run walks back from every output node and executes what
feeds it. Take it off `sorry` and nothing asks for the apology any more, so it
never runs.

For how a run picks its nodes, and how a trigger fire narrows that, go and read
[How a weft program runs](../language/mental-model.md).

## Where to go next

For the syntax used here, wires written inside a node's braces and port
signatures given inline, go and read [Syntax](../language/syntax.md).

Next: [when something goes wrong](troubleshooting.md).
