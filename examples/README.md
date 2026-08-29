# Examples

Each folder is one complete weft project: the `main.weft`, every prompt,
script and SQL file it references, and a `nodes/base_catalog` that is a
symlink to this repo's `catalog/`, so the examples always compile against
the catalog you have checked out.

Open a folder in VS Code with the weft extension (or `cd` into it and use
the CLI) and it works as is; the only thing left to you is picking your
own connections on the access nodes.

| Project | What it is |
|---|---|
| [`whatsapp-support-bot/`](whatsapp-support-bot/) | The README's program: a WhatsApp support bot that answers on its own and asks a person before sending anything sensitive. |
| [`telegram-image-bot/`](telegram-image-bot/) | A Telegram bot that draws pictures with fal, charges a credit per image against its own Postgres, and refuses politely when the credits run out. It is walked through in [a bigger example](https://weavemindai.github.io/weft/start/a-bigger-example.html). |

If you copy an example out of this repo to build on it, run
`weft catalog update` in the copy: it replaces the symlink with a real
copy of the catalog, and the project is self-contained.
