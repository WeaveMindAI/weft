---
name: releasing
description: "Shipping weft: merging into mvp through a PR, what the release workflow publishes, the record tags that decide which stores publish, and the store rules that have actually bitten us. Load before merging to mvp, bumping an extension version, or debugging a red publish job."
---

How a change gets from the working tree to the stores, and the traps
that cost a round trip the last time each one was hit.

## The path

`mvp` is the branch that ships. `main` carries the old proof of
concept and nothing merges there. Work lands on a feature branch, goes
into `mvp` through a PR, and the merge is what publishes.

1. Commit on the feature branch (only when the [user] says so).
2. `gh pr create --base mvp --head <branch>`.
3. `gh pr merge <n> --auto --merge`, which lands it the moment CI
   passes. Earlier PRs into `mvp` are merge commits, so match that.
4. The push to `mvp` runs `release.yml`, which builds the images, the
   CLI binaries, the `.vsix` and the browser zips, updates the rolling
   `mvp-latest` release, and then publishes to each store.

CI runs on `pull_request` into `mvp` only. Four checks: `build`,
`cargo test + clippy`, `cargo test --features db-tests`, and
`graph + editor`.

## Run the checks CI runs, not the ones you touched

`cargo clippy --workspace --all-targets --locked -- -D warnings` is
the CI line. Running clippy on the crates you edited is not the same
thing and has already sent a PR red on a lint in a crate the change
only touched indirectly.

`scripts/validate-examples.sh` compiles every example. It fails on
ERRORS only, so a warning is advice an example may carry. If an
example starts failing after a language change, the question is
whether the example is wrong or the diagnostic is, and the answer has
been "the diagnostic" as often as not.

## The record tags decide what publishes

Each successful publish is recorded as a git tag,
`<prefix>-<store>-v<version>`: `vscode-marketplace-v0.2.239`,
`browser-firefox-v1.0.62`. Before publishing, the workflow asks
`git ls-remote origin refs/tags/<tag>` per store; a store whose tag
exists gets no job at all.

So **a store publishes exactly when its tag is missing**, and the
version in the workspace's `package.json` is what the tag names.
Without a version bump, a release republishes nothing.

Two things follow, and both have bitten:

**Bumping is the release gesture.** `extension-vscode/package.json`
and `extension-browser/package.json` carry their own versions.
`./setup.sh --bump` (with `--vscode` and/or `--browser`) moves them;
so does `pnpm version patch --no-git-tag-version` in the workspace.
Push a bump and that store publishes.

**A tag that is missing when the store already has the version is a
stuck release.** The tag is written after the publish succeeds, so a
successful publish whose tag push failed leaves the repo believing the
store still owes one. The next run submits a duplicate, the store
refuses, and only a hand-made tag ever clears it. That is why
`.github/actions/publish-and-record` treats a refusal saying the
version is already there as published and records the tag. Every other
failure still fails the job. If you ever widen that pattern, the
failure mode you are risking is a tag written for a publish that never
happened, which skips that store forever.

To check the state before a release, or to work out why a job ran:

```bash
git ls-remote --tags origin | grep -E "vscode|browser"
```

And to record something a store already has (a manual upload, a
publish whose tag was lost), tag the commit whose content is live:

```bash
git tag vscode-marketplace-v0.2.239 <commit> && git push origin vscode-marketplace-v0.2.239
```

## The stores

**VS Code Marketplace.** Publishes with `vsce publish --azure-credential`
from the `marketplace-publish` environment. A brand-new publisher gets
blocked with "Your extension has suspicious content. Please fix your
extension metadata, or contact support if you need assistance", which
names nothing and is not documented anywhere. It survived removing an
ad-carrying dependency, trimming the package and fixing every link.
What cleared it was uploading the `.vsix` by hand at
`marketplace.visualstudio.com/manage/publishers/weavemind`
("New extension"). Once the publisher has an accepted extension, CI
should work; the next bump is what proves it. If it is blocked again,
the question for `vsmarketplace@microsoft.com` is sharp: the package is
byte-identical to one they accepted by hand, so the automated path is
the only difference. Every documented case of this ends at support.

**Open VSX** (`ovsx`) is the store VSCodium, Cursor and Windsurf read.
It has never refused anything. Its namespace must exist before the
first publish and creating it twice is refused, which the workflow
handles.

**Chrome, Firefox, Edge** go through `wxt submit` from
`extension-browser`. AMO refuses a duplicate version with a 409 naming
the version, which is the case the self-heal was written for. Firefox
also needs a sources zip (`zip:firefox` writes it) because AMO reviews
the source behind a bundle.

## Marketplace package rules worth knowing before packaging

Checked against the published rules, and all of these are enforced:

- The icon may not be an SVG and must be at least 128x128.
- Images in the README must resolve to `https` URLs and may not be
  SVGs. Relative links are rewritten by `vsce` using the repository
  field, and it defaults to the `main` branch, which on this repo is
  the old POC. Prefer absolute links to the docs site.
- Badges are only allowed from approved providers.
- `categories` must come from the fixed list; `keywords` caps at 30.
- `displayName` must be unique Marketplace-wide.
- A licence file must ship. `release.yml` copies the repo `LICENSE`
  into `extension-vscode/` at package time.

`pnpm dlx @vscode/vsce@3.9.2 ls --no-dependencies` lists exactly what
would ship, which is how to catch things like compiled tests riding
along because `out/` was never cleaned.

## Deployments show red forever

The repo's Deployments panel lists the `marketplace-publish`
environment with a red mark from a failed run. GitHub keeps the last
status per deployment and never revises it, and a job that is SKIPPED
creates no deployment at all, so a later green run does not clear it.
It is a record of a past attempt, not the current state. The truth is
the run's own conclusion and the store itself:

```bash
curl -s -X POST 'https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json;api-version=7.2-preview.1' \
  -d '{"filters":[{"criteria":[{"filterType":7,"value":"weavemind.weft-vscode"}],"pageSize":5,"pageNumber":1}],"flags":914}'
```

## What ships even when a store fails

Each store publish is its own job, so one refusal never blocks another,
and the rolling `mvp-latest` release goes out as long as the `.vsix`
packaged and the images stitched. That release is what a fresh
`./setup.sh` pulls prebuilts from, so a failed store publish costs the
listing on that store and nothing else. Say so plainly rather than
reporting a release as broken.

## Never run these yourself

`./setup.sh` (any flag) and the e2e redeploy the daemon and rebuild the
extension while the [user] works, and they take a long time. Ask them
to run those. Packaging the `.vsix`
(`pnpm run vscode:prepublish && pnpm dlx @vscode/vsce@3.9.2 package --no-dependencies`)
is yours: it touches nothing outside `extension-vscode/`.
