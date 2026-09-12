# The Weft Book

Read it at **<https://weavemindai.github.io/weft/>**.

Or start at [`src/introduction.md`](src/introduction.md) and follow
[`src/SUMMARY.md`](src/SUMMARY.md). The pages are plain markdown and read fine
on GitHub.

## Build it locally

Python 3 is also required for the page's Copy Markdown button.

```bash
cargo install mdbook mdbook-mermaid
cd docs
mdbook-mermaid install .
mdbook serve --open
```

That watches the files and reloads as you write. `mdbook-mermaid` renders the
diagram fences, and GitHub renders them natively, so the same source works in
both places.

## Layout

```
docs/
  book.toml           mdBook configuration
  theme/weft.css      book colors, typography and responsive layout
  theme/header.hbs    sidebar wordmark
  theme/fonts/       local fonts and their licenses
  theme/highlight-weft.js  link to the shared Weft syntax highlighter
  src/
    SUMMARY.md        the table of contents. A page not listed here is not built.
    introduction.md
    start/            install through first program with a human step
    language/         the language reference
    nodes/            writing nodes
    connections/      talking to outside services
    running/          the CLI, the runtime, the journal
    thinking/         why it is shaped this way
    appendix/         glossary, roadmap
    img/              screenshots and diagrams, with README.md as the checklist
```

## Writing for this book

Four rules that keep it consistent.

**Every claim is grounded.** Read it in the source, run it, or be told it by
someone who did. Where something is a direction rather than a shipped feature,
say so on the page.

**Length is a defect.** If cutting a sentence loses nothing, cut it.

**Start where the reader is**, including the wrong belief they arrived with.
Taking that apart is usually the first job, before any syntax.

**No em dashes.** Comma, colon, parentheses, or two sentences. Why we hunt them
is [its own page](src/thinking/em-dashes.md).

If you add a page, add it to `SUMMARY.md`. `create-missing` is off, so a link
to a file that does not exist fails the build rather than shipping a dead link.
