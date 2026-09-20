# Weft syntax highlighting

Colours for weft source, as TextMate and highlight.js grammars.

- `weft.tmLanguage.json` is a TextMate grammar. That is what VS Code speaks,
  and what most editors speak through it.
- `weft-markdown-injection.json` is the same grammar again, aimed at a
  ` ```weft ` fence inside a markdown file.
- `highlight-weft.js` is a highlight.js language, for a page that renders
  weft rather than edits it.

Nothing here has dependencies, so any host can take the file it needs.

## Who uses which

If you want colours while editing a `.weft` file, the VS Code extension
already has them: `extension-vscode/syntaxes/` symlinks both grammars and
`extension-vscode/package.json` points the `weft` language at the first and
injects the second into markdown.

If you want colours in VS Code's markdown preview, the extension ships its own
highlight.js there. The preview renders fenced code back in the extension
host, using a copy of highlight.js that only knows the languages it was built
with, and nothing can add to it. So `markdown-preview/hljs.js` (built by
`pnpm run bundle:markdown-preview`) puts a highlight.js in the preview itself
and `markdown-preview/highlight-weft.js` teaches it weft, both listed under
`markdown.previewScripts`. The preview rewrites its whole body on every
keystroke, so the repaint runs again on each `vscode.markdown.updateContent`.

If you want colours in the book, `docs/theme/highlight-weft.js` is a symlink
to the file here, and `docs/book.toml` loads it through `additional-js`. It
installs itself on the page's highlight.js and repaints every ` ```weft `
block, because highlight.js has already walked the page by then.

The one place none of this reaches is this repo's pages on GitHub. GitHub
highlights through Linguist, which has no way to take a grammar from the repo
it is rendering, so a ` ```weft ` fence there stays plain until weft is
submitted to Linguist (which wants hundreds of public repos using the
extension first).

## Keeping them true

Every file here describes one lexer,
[`crates/weft-compiler/src/cst/lexer.rs`](../../crates/weft-compiler/src/cst/lexer.rs).
Change a token rule there and the grammars here need the same change. The
markdown injection is the one exception: it only says where a weft block
starts and ends, then hands the body to `weft.tmLanguage.json`.

The vocabulary half of that is checked:
`crates/weft-compiler/tests/highlighting_vocabulary.rs` fails, naming the
word, when the compiler knows a reserved type or a primitive that a grammar
does not. What each piece of source actually turns into is checked here, in
`highlight-weft.test.js` (`pnpm test`).

## What gets a colour

| The thing | In the editor | In the book |
|---|---|---|
| `# a comment` | `comment.line.number-sign.weft` | `hljs-comment` |
| `"a string"`, and a ` ``` ` block | `string.quoted.*.weft` | `hljs-string` |
| `@include(...)`, `@file(...)`, `@require_one_of(...)` | `keyword.control.directive.weft` | `hljs-meta` |
| `Group`, `Loop`, `self` | `keyword.control.weft` | `hljs-keyword` |
| `_label`, `_tags`, `_should_flow` | `keyword.other.reserved.weft` | `hljs-keyword` |
| `String`, `List`, `Access`, every other type | `support.type.weft` | `hljs-type` |
| A node type, `LlmInference` | `entity.name.type.weft` | `hljs-title` |
| A config key, left of its colon | `variable.parameter.weft` | `hljs-attr` |
| The port in `ticket.body` | `variable.other.member.weft` | `hljs-symbol` |

Node ids stay uncoloured on purpose: they are your names, and leaving them
plain is what makes the types stand out.

A type is told from a node id by its capital letter, which is a convention
rather than a rule the compiler enforces. Name a node `Ticket` and it will be
painted as a type.

<!-- SYNC: the reserved-key rule in weft.tmLanguage.json (JSON takes no
     comments, so this marker stands in for it) <->
     crates/weft-core/src/exec/skip.rs SHOULD_FLOW_PORT,
     packages/weft-graph/src/protocol.ts SHOULD_FLOW_PORT,
     docs/src/language/syntax.md (reserved keys) -->

## Tests

The package declares no dependencies of its own: `node_modules` here is a
symlink into `extension-vscode`'s (setup.sh and CI both wire it), so the
`highlight.js` and `vitest` the tests run against are the extension's.
Run them with `pnpm test`.
