---
name: mode-code
description: "Code mode: rules for writing and refactoring code. Style (DRY, SYNC markers, naming), no fallbacks fail loudly, tooling, testing pyramid. Load when switching to [code mode], before writing or editing code, or when fixing review findings."
---

Write or refactor code. Clean, not just simple: no dev-only hacks, no temporary workarounds, no "for now". The [decision framework] in `.codex/config.toml` (perfectionist not patcher, extend before adding, no shippable phases, info on the object) is how you decide each line; this skill carries the mechanical rules that follow from it.

## The working tree is the review

You never `git commit` and never `git add` on your own: the unstaged diff is what the [user] reviews, and a commit or a stage collapses it. Only a "commit" in the [user]'s current message authorizes one, for exactly the changes they were looking at. When you feel done, you stop and report; you do not tidy up into commits.

## Style

1. **Imports at top only.** Never mid-function.
2. **No legacy, no backward compat.** Delete old dead code completely: no remnants, no fallback formats, no pre-refactor paths. When something looks like dead legacy, remove it without hesitation; if the [user] wanted it preserved, they'd say so. The modern path is the only path.
3. **DRY.** If two functions can merge, merge them. Check the codebase before duplicating anything.
4. **Cross-boundary SYNC markers.** When the same concept must be defined in two places because a language or runtime boundary forces it (a backend enum mirrored by a frontend union, a wire shape restated across services), define it once per language and link the sites on every definition:

   ```
   // SYNC: <local-name> <-> <fully-qualified-other-site-1>, <fully-qualified-other-site-2>
   ```

   Every site lists every other site, so opening any one file shows the full chain; when changing one side, grep `SYNC:` for the peers. No code generation, no shared schemas. Same-language duplicates are not sanctioned by this rule; they fall under "extend the existing concept" in the [decision framework].
5. **Fix the root cause.** One line at the source beats five lines of workaround downstream. Don't over-engineer the fix either.
6. **Always write tests.** Non-trivial changes come with tests written or updated, at the right layer of the pyramid below. Never delete or weaken existing tests without explicit approval.
7. **Name by contract, not mechanism.** The name states what the caller asks for and receives, from the caller's side; how you fulfill it (caching, pooling, retries) belongs in the doc comment. Past incident: a function whose contract was "hand me the shared GeneratorInfo" was named `pricing_generator` because it happened to carry a price cache; the caller doesn't ask for pricing. Test: read the call site alone; does the name say what the caller receives? A name that reads like a recipe (`connect_db_and_login_and_hand_connection`) gets renamed to the thing it hands back (`db_connection`). When you see a misnaming in existing code, surface it right away: rename on the spot if obvious, mention it immediately if it's a genuine fork.

## No fallbacks, fail loudly

Never implement fallbacks, legacy patches, or silent error recovery. The only two acceptable outcomes: the correct implementation works, or it fails loudly with a clear error visible to the user (node failure in the UI) or clear logs in the backend. Fallbacks hide real bugs and become hidden tech debt; previous assistants added many, and the [user] is still digging them out.

Failing loud is not enough; the aftermath is designed too. For each piece of state the failed operation created or held:
- **Can the user act on it** (resume, retry, inspect, delete via a documented action)? If not, clean it up: untouchable junk is forbidden.
- **Does it have value a recovery would need** (in-flight work, expensive artifacts, debugging context)? If yes and the failure is recoverable, preserve it; if not, clean up.

Build the recovery path when it's worth it (high value, plausible failure) and skip it when it isn't; the recovery path is part of the failure's design, not a follow-up. Never ship a half-built recovery that works for the easy case and strands the user on the real one. If the value-vs-effort call isn't obvious, ask the [user]; that is a design decision, not a coding one. Then write the error message: what broke, the named recovery action if anything was preserved, how to prevent recurrence. Hard floor: nothing remains that the user can neither act on nor delete and doesn't know about.

**Retry logic that spends money** (LLM calls, paid APIs) requires explicit approval. A silent retry on a paid call doubles spend and hides the underlying problem.

## Tooling

**Wait on conditions, never timers.** To wait for a long task (build, deploy, rollout), block on the actual condition returning the instant it flips: `until <check>; do sleep 5; done`, in the foreground with a generous timeout. Never `sleep <N>` then check, never hand-rolled re-polls.

**pnpm, not npm.** The projects are pnpm-based.

**Never mass-edit code with sed/awk/python.** Past incident: a sed command and a python "fix comments" script stripped `//` from code lines, commented out parameters and return statements, broke the parser; an hour of manual repair. Regex mass edits can't tell comments from code. Use exact-match edits (apply_patch), one change at a time, one change at a time, and verify compilation after each batch.

## Python function pattern

```python
def method_name(self,  # self on the same line: it adds no info, no new line for it
    param1: Type,
    param2: Type,
) -> ReturnType:
    """Docstring on one line. No ultra long docstring"""
    # Code here
```

## Testing pyramid

Four layers; pick the right one and call it by its name.

- **Layer 1: pure-function unit tests.** Values in, values out, no I/O. Fast, next to the function. This is where 80% of the test count lives.
- **Layer 2: wire-shape tests.** Round-trip every cross-process type through its serialization format, next to the type. Catches "renamed a field, broke the contract".
- **Layer 3: contract tests with fakes.** A subsystem's real code against hand-rolled in-memory fakes of its I/O, in the subsystem's test directory. Catches orchestration bugs the pure functions can't. This is the layer most projects skip.
- **Layer 4: end-to-end integration tests.** Real binaries, real network, real backing services. Slow, few of them, CI nightly or pre-release, not every save.

Rules when adding code:
1. **No I/O inside subsystem code.** HTTP clients, clocks, subprocesses, DB drivers, file I/O, env vars go through a trait with a production impl and a fake impl.
2. **Extract pure functions aggressively.** Decision logic becomes a pure function with explicit state input; the I/O becomes a thin wrapper that gathers, calls, dispatches.
3. **Fakes are dumb.** An append-only call log, plain state maps, no business logic. Replicating production behavior in a fake means the fake is wrong.
4. **No mock libraries.** Hand-rolled fakes beat `mockall` / `jest.mock` / `unittest.mock`: the DSL overhead isn't worth it and macros hide what's tested.
5. **Each subsystem owns its rig.** No central testing crate; the rig lives alongside the subsystem behind a test-only feature flag.
6. **Tests at the right layer.** New pure function: layer-1 test. New orchestration path: layer-3 test. Never layer-4 tests for layer-1 bugs, or the reverse.

**Run only the [test scope]** (defined in `.codex/config.toml`): the crate you
edited plus each crate that depends on what you changed, one test by name
while iterating (`cargo test -p <crate> --test <file> <name>`), then the
crate. Runners take the same narrowing: `scripts/run-node-tests.sh <package>`,
`scripts/run-db-tests.sh <crate>`, `scripts/run-e2e.sh <name>`.

**Flakes are bugs, never noise.** A test that fails 1-in-N is a bug; "just flaky" frames it as the test's fault and trains the eye to ignore it. Reject the frame. Reproduce deterministically first: loop it 20-50 times locally, under parallel load (generate contention if the plain loop doesn't trigger it), widening the load until it triggers. Find the root cause: the usual suspects are notification fired before a waiter is armed (use `notify_one`'s permit semantics), arm-then-check windows, order-dependent assertions under multi-thread, relaxed atomics that should be acquire/release. Fix cleanly in the code under test: never retries, sleeps, longer timeouts, `#[ignore]`, or "try N times" wrappers; those tolerate the race instead of fixing it. And never use flakiness as a permission slip: a failed test failed. Re-running to green is evidence the race is intermittent, not that the failure was spurious; investigate every failure on first observation.

**Stress-loop timing-sensitive tests by construction.** Tests touching timing-sensitive primitives run many times by design, via a `stress_test!`-style macro generating N named variants so `cargo test` parallelizes them and any race surfaces loudly. Apply it to every test using a multi-thread runtime, `tokio::sync::Notify`, channels, or broadcast; depending on a firing order between tasks; asserting a stuck-detection deadline; or coordinating spawned tasks through shared state. A 10-second harness timeout that "succeeds" in 10s is a hung test the harness rescued: treat it as a failure.