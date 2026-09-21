# The Weft Book

Read it at **<https://weavemindai.github.io/weft/>**.

Or start at [`src/introduction.md`](src/introduction.md) and follow
[`src/SUMMARY.md`](src/SUMMARY.md). The pages are plain markdown and read fine
on GitHub.

## Build it locally

```bash
cargo install mdbook mdbook-mermaid
cd docs
mdbook-mermaid install .
mdbook serve --open
```

That watches the files and reloads as you write. Python 3 is also needed, for
the Copy Markdown button on each page. `mdbook-mermaid` renders the diagram
fences, and GitHub renders them natively, so the same source works in both
places.

## Layout

```text
docs/
  book.toml                mdBook configuration
  theme/weft.css           colours, typography, responsive layout
  theme/header.hbs         the sidebar wordmark
  theme/fonts/             local fonts and their licences
  theme/highlight-weft.js  a symlink to the shared weft highlighter
  src/
    SUMMARY.md             the table of contents. A page not listed here is not built
    introduction.md
    start/                 install through a running program. Ten minutes
    build/                 the graph, connections, a public address, a person in the loop. Ten more
    language/             the language reference
    nodes/                 writing nodes
    connections/           talking to outside services
    running/               the CLI, the runtime, the journal, versions
    thinking/              why it is shaped this way
    appendix/              glossary, roadmap
    img/                   pictures
```

## Writing for this book

**Every claim is grounded.** Read it in the source, run it, or be told it by
someone who did. Never from an older version of these docs: they have been
wrong before. Where something is a direction rather than a shipped feature, say
so on the page.

**Length is a defect.** If cutting a sentence loses nothing, cut it.

**Start where the reader is**, including the wrong belief they arrived with.
Taking that apart is usually the first job, before any syntax.

**Reference pages list things.** A person looking up what `ctx.presign`
returns wants the signature and one line, not an essay. The pages somebody
reads front to back are the ones that walk.

**One page owns each fact.** Everywhere else links. To check, pull every
sentence over about 55 characters out of every page and look for any that
appears twice.

**Pictures only on pages people read front to back.** A reference page's reader
wants the signature and one line, not a screenshot. Never link a picture that is
not in `src/img/` yet: a broken image is worse than none.

**No em dashes.** Comma, colon, parentheses, or two sentences. Models reach for
them far more than people do, so an em dash reads as a line nobody reviewed.

If you add a page, add it to `SUMMARY.md`. `create-missing` is off, so a link
to a file that does not exist fails the build rather than shipping a dead link.
