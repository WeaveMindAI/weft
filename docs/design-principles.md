# Design principles

What weft is FOR, stated as the properties every design decision is
measured against. When a proposal is hard to judge, judge it here.

## 1. The vocabulary is dynamic

Weft is not a catalog of integrations. The nodes that ship are
examples and conveniences; the language's job is to be expressive
enough that whatever is missing gets built on demand, correctly, by
whoever needs it.

The test of a new capability is therefore never "did we add the node
for X". It is: **can the next X be expressed without touching the
engine?** Slack's held socket, Google's expiring watch channels and
email's held IMAP pipe are three genuinely different event
topologies, and all three are declared data over one engine. That is
the bar. A feature that only works because weft learned about a
specific provider has failed it.

Consequences, in force everywhere:

- Per-service knowledge lives in declared data (a service recipe, a
  node's metadata), never in engine, listener, broker or dispatcher
  code.
- The escape hatch, when a mechanism genuinely cannot be data (a
  signature algorithm, a browser picker handshake), is a CLOSED typed
  variant added deliberately and shared by every service, never a
  per-service hook.
- Breadth of the shipped catalog is not a goal. Expressive coverage
  is.

## 2. The author is an AI with a prompt, not a human with a manual

Weft's surfaces are written by a model working from a prompt and the
docs. That inverts the usual design pressure:

**Clarity is the bar; simplicity is not.** A surface may be large and
detailed if it is legible and steers toward the correct shape. "Would
a newcomer find this easy" is the wrong test. The right test is:
**given this vocabulary and these docs, does the model build the right
thing?**

So: name concepts for what they mean, state invariants where they are
enforced, make the doc comment say why a rule exists, and prefer one
honest complex declaration over several simple ones that must be kept
in agreement by hand. Ceremony that exists only to look simple (a
builder that hides a required decision, a default that silently picks
a policy) costs more than it saves, because the model fills it in
wrong and nothing complains.

## 3. The right shape is the easy shape; wrong shapes are refused

Everything an author can get wrong falls in one of three buckets, and
each has an intended fate:

1. **Structurally impossible.** Best. The vocabulary cannot express
   the mistake.
2. **Refused loudly, naming the fix.** At parse, at compile, or at
   registration; before anything runs. This is where most rules land.
3. **Runs and silently misbehaves.** Forbidden. If a mistake can reach
   this bucket, that is a design defect to fix, not a documentation
   problem.

The failure mode this exists to kill is the trigger that registers
fine and never fires, the connection that resolves fine and never
authenticates, the filter that matches nothing forever. Those are
worse than crashes, because nobody learns anything. Every declared
surface therefore validates its own internal consistency at the moment
it enters the system, and says what is missing in the words of the fix:

- a subscription that renews but records no expiry,
- a recipe that routes by an id it never mints,
- a check that compares against a secret nobody sends,
- a filter whose pattern does not compile,
- a service offering a door its own auth cannot serve.

All refused where someone is watching, none of them discoverable at
3am from a trigger that quietly stopped.

**Corollary: no fallbacks.** A path that "works anyway" hides the
defect and denies the author the refusal. Either it works correctly or
it fails with a message that names the recovery.

## 4. The system supplies the recipes, so bodies stay logic

The declared surface exists to make correct construction the default.
A node body contains its own logic and nothing else: no transport
choice, no credential handling, no acknowledgement protocol, no
subscription lifecycle, no retry bookkeeping. Those are the language's,
implemented once.

The practical rule when adding to the node-facing surface: **if three
calls are always implied by one intent, expose the one call.** Chained
ceremony is where models improvise, and improvisation is where the
wrong shape enters.

The reverse rule is just as load-bearing: a node embodies ONE user
expectation. When one capability serves two genuinely different
expectations, build two nodes, even when the machinery beneath is
identical. A node that answers a different question depending on what
was wired into it cannot be reasoned about by anyone, human or model.
