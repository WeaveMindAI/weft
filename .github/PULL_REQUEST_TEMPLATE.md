<!--
Thanks for sending this.

For anything medium or large, an issue first saves you the afternoon where we
change something underneath you. CONTRIBUTING.md has the rest.
-->

## What and why

<!-- One or two sentences. The diff says what changed, so say why. -->

## How

<!-- The approach, briefly. Skip it if the diff is obvious. -->

## Linked issue

<!-- Closes #123 / Refs #456 -->

## What kind of change

- [ ] Bug fix
- [ ] New node
- [ ] New service (an access recipe)
- [ ] Language change: parser, type system, validator, codegen
- [ ] Runtime change: engine, dispatcher, listener, supervisor, broker
- [ ] The graph renderer
- [ ] The VS Code extension
- [ ] Docs
- [ ] Refactor
- [ ] Other:

## Checklist

- [ ] `cargo test` and `cargo clippy` pass.
- [ ] If you touched the graph renderer, `pnpm -C packages/weft-graph test` passes.
- [ ] If you touched the VS Code extension, `pnpm -C extension-vscode test` passes.
- [ ] New code has tests, at [the right
      layer](https://weavemindai.github.io/weft/running/architecture.html#testing-in-four-layers).
- [ ] A bug fix has a regression test that fails before the fix.
- [ ] Docs updated, if this changes behavior the docs describe.
- [ ] If you changed SQL, the `db-tests` suites pass against a real Postgres.
- [ ] A new node holds up against [the review
      list](https://github.com/WeaveMindAI/weft/blob/main/CONTRIBUTING.md#writing-a-node-for-the-standard-catalogue).
- [ ] If you built this with an AI assistant, `/flow-4-review` and
      `/flow-5-review-check` ran as a pair until a pair turned up nothing.

## Anything reviewers should look at twice

<!--
The tricky part, an alternative you rejected, a tradeoff you are unsure about.
Saying it here turns a review comment into a conversation you already started.
-->
